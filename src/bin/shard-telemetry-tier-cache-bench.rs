use std::fs;
use std::hint::black_box;
use std::ops::Range;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use shard_stream_core::{LogicalPartitionId, ShardId, TopicPartition};
use shard_telemetry::{
    CorrelationBlockFilter, LocalObjectStore, METRICS_TOPIC_ID, ObjectTierConfig, SsdCacheConfig,
    SsdObjectCache, TelemetryObjectStore, TelemetryObjectTier, TelemetrySignal, TierArtifactKind,
    TierArtifactSource, TierBlockEntry, TierCheckpoint, TierGroupSource, TierQueryRange, TraceId,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut object_bytes = 64 * 1024 * 1024usize;
    let mut chunk_bytes = 4 * 1024 * 1024u64;
    let mut range_bytes = 64 * 1024u64;
    let mut ranges = 64usize;
    let mut iterations = 100usize;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--object-bytes" => object_bytes = parse(args.next(), "--object-bytes")?,
            "--chunk-bytes" => chunk_bytes = parse(args.next(), "--chunk-bytes")?,
            "--range-bytes" => range_bytes = parse(args.next(), "--range-bytes")?,
            "--ranges" => ranges = parse(args.next(), "--ranges")?,
            "--iterations" => iterations = parse(args.next(), "--iterations")?,
            _ => return Err(format!("unknown argument {argument}").into()),
        }
    }
    if object_bytes == 0 || chunk_bytes == 0 || range_bytes == 0 || ranges == 0 || iterations == 0 {
        return Err("all benchmark bounds must be nonzero".into());
    }
    let selected_bytes = range_bytes
        .checked_mul(u64::try_from(ranges)?)
        .ok_or("selected byte count overflow")?;
    if selected_bytes > chunk_bytes || chunk_bytes > u64::try_from(object_bytes)? {
        return Err("selected ranges must fit one cache chunk and object".into());
    }

    let root = benchmark_directory();
    fs::create_dir_all(&root)?;
    let store = LocalObjectStore::open(root.join("objects"))?;
    let payload = (0..object_bytes)
        .map(|index| (index as u64).wrapping_mul(131).wrapping_add(17) as u8)
        .collect::<Vec<_>>();
    let metadata = store.put_bytes_if_absent("payload/pack", &payload)?;
    let selected = (0..ranges)
        .map(|index| {
            let start = u64::try_from(index).expect("range index fits") * range_bytes;
            start..start + range_bytes
        })
        .collect::<Vec<Range<u64>>>();
    let config = SsdCacheConfig {
        max_bytes: 2 * (chunk_bytes + 64),
        chunk_bytes,
        max_read_bytes: chunk_bytes,
        memory_bytes: 2 * chunk_bytes,
        parsed_memory_bytes: 0,
    };
    let legacy = SsdObjectCache::open(root.join("legacy-cache"), config)?;
    let batched = SsdObjectCache::open(root.join("batched-cache"), config)?;

    let (legacy_cold_elapsed, legacy_cold) = time(|| {
        selected
            .iter()
            .map(|range| {
                legacy.read_range_with_metadata(&store, "payload/pack", &metadata, range.clone())
            })
            .collect::<Result<Vec<_>, _>>()
    });
    let legacy_cold = legacy_cold?;
    let (batched_cold_elapsed, batched_cold) = time(|| {
        batched.read_shared_ranges_with_metadata(&store, "payload/pack", &metadata, &selected)
    });
    let batched_cold = batched_cold?;
    let legacy_expected = legacy_cold.iter().flatten().copied().collect::<Vec<_>>();
    let batched_expected = batched_cold
        .iter()
        .flat_map(|range| range.as_ref().iter().copied())
        .collect::<Vec<_>>();
    if legacy_expected != batched_expected || legacy_expected != payload[..selected_bytes as usize]
    {
        return Err("batched and independent range reads disagree".into());
    }

    let legacy_start = Instant::now();
    for _ in 0..iterations {
        for range in &selected {
            black_box(legacy.read_range_with_metadata(
                &store,
                "payload/pack",
                &metadata,
                range.clone(),
            )?);
        }
    }
    let legacy_warm = legacy_start.elapsed();
    let batched_start = Instant::now();
    for _ in 0..iterations {
        black_box(batched.read_shared_ranges_with_metadata(
            &store,
            "payload/pack",
            &metadata,
            &selected,
        )?);
    }
    let batched_warm = batched_start.elapsed();
    let legacy_stats = legacy.stats();
    let batched_stats = batched.stats();
    let control = benchmark_control_cache(&root, iterations.saturating_mul(100).max(1))?;

    println!("ShardTelemetry tier-cache benchmark");
    println!(
        "object_bytes={object_bytes} chunk_bytes={chunk_bytes} range_bytes={range_bytes} ranges={ranges} iterations={iterations}"
    );
    println!(
        "cold_us legacy={:.3} batched={:.3} speedup={:.2}x",
        legacy_cold_elapsed.as_secs_f64() * 1e6,
        batched_cold_elapsed.as_secs_f64() * 1e6,
        legacy_cold_elapsed.as_secs_f64() / batched_cold_elapsed.as_secs_f64(),
    );
    println!(
        "warm_us_per_query legacy={:.3} batched={:.3} speedup={:.2}x",
        legacy_warm.as_secs_f64() * 1e6 / iterations as f64,
        batched_warm.as_secs_f64() * 1e6 / iterations as f64,
        legacy_warm.as_secs_f64() / batched_warm.as_secs_f64(),
    );
    println!(
        "cache legacy_hits={} legacy_memory_hits={} legacy_misses={} legacy_source_bytes={} batched_hits={} batched_memory_hits={} batched_misses={} batched_source_bytes={}",
        legacy_stats.hits,
        legacy_stats.memory_hits,
        legacy_stats.misses,
        legacy_stats.source_bytes,
        batched_stats.hits,
        batched_stats.memory_hits,
        batched_stats.misses,
        batched_stats.source_bytes,
    );
    println!(
        "control_us_per_query raw_decode={:.3} parsed={:.3} speedup={:.2}x iterations={} parsed_hits={}",
        control.raw_decode_us,
        control.parsed_us,
        control.raw_decode_us / control.parsed_us,
        control.iterations,
        control.parsed_hits,
    );
    println!(
        "correlation_catalog_prune_us_per_query={:.3} speedup_vs_parsed_control={:.2}x",
        control.correlation_pruned_us,
        control.parsed_us / control.correlation_pruned_us,
    );
    println!("evidence_directory={}", root.display());
    Ok(())
}

struct ControlResult {
    raw_decode_us: f64,
    parsed_us: f64,
    correlation_pruned_us: f64,
    iterations: usize,
    parsed_hits: u64,
}

fn benchmark_control_cache(
    root: &std::path::Path,
    iterations: usize,
) -> Result<ControlResult, Box<dyn std::error::Error>> {
    let source_directory = root.join("control-sources");
    fs::create_dir_all(&source_directory)?;
    let payload_path = source_directory.join("payload");
    let index_path = source_directory.join("index");
    fs::write(&payload_path, b"compressed metric chunk")?;
    fs::write(&index_path, b"metric recovery index")?;
    let store = LocalObjectStore::open(root.join("control-objects"))?;
    let shard_id = ShardId::new(7);
    let partition = TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(3));
    let payload = fs::read(&payload_path)?;
    let block = TierBlockEntry::for_signal_payload(
        TelemetrySignal::Metrics,
        1,
        42,
        42,
        0,
        0,
        1,
        100,
        100,
        0,
        u64::try_from(payload.len())?,
        blake3::hash(&payload).to_hex().to_string(),
        CorrelationBlockFilter::default(),
    )?;
    let mut publisher = TelemetryObjectTier::open(
        store.clone(),
        shard_id,
        partition,
        ObjectTierConfig::default(),
    )?;
    publisher.publish_group(TierGroupSource {
        group_sequence: 0,
        checkpoint: TierCheckpoint {
            next_placement_sequence: 1,
            next_offset: 1,
        },
        blocks: vec![block],
        artifacts: vec![
            TierArtifactSource {
                kind: TierArtifactKind::PayloadPack,
                name: "signal.payload".into(),
                path: payload_path,
            },
            TierArtifactSource {
                kind: TierArtifactKind::QueryIndex,
                name: "signal.index".into(),
                path: index_path,
            },
        ],
    })?;

    let raw_tier = TelemetryObjectTier::open(
        store.clone(),
        shard_id,
        partition,
        ObjectTierConfig::default(),
    )?;
    let parsed_tier =
        TelemetryObjectTier::open(store, shard_id, partition, ObjectTierConfig::default())?;
    let raw_cache = SsdObjectCache::open(
        root.join("raw-control-cache"),
        SsdCacheConfig {
            max_bytes: 1024 * 1024,
            chunk_bytes: 64 * 1024,
            max_read_bytes: 1024 * 1024,
            memory_bytes: 1024 * 1024,
            parsed_memory_bytes: 0,
        },
    )?;
    let parsed_cache = SsdObjectCache::open(
        root.join("parsed-control-cache"),
        SsdCacheConfig {
            max_bytes: 1024 * 1024,
            chunk_bytes: 64 * 1024,
            max_read_bytes: 1024 * 1024,
            memory_bytes: 1024 * 1024,
            parsed_memory_bytes: 1024 * 1024,
        },
    )?;
    query_control(&raw_tier, &raw_cache)?;
    query_control(&parsed_tier, &parsed_cache)?;

    let raw_start = Instant::now();
    for _ in 0..iterations {
        black_box(query_control(&raw_tier, &raw_cache)?);
    }
    let raw_elapsed = raw_start.elapsed();
    let parsed_start = Instant::now();
    for _ in 0..iterations {
        black_box(query_control(&parsed_tier, &parsed_cache)?);
    }
    let parsed_elapsed = parsed_start.elapsed();
    let absent_query = shard_telemetry::CorrelationQuery::new("tenant-a")
        .with_trace_id(TraceId::from_bytes([9; 16]).expect("benchmark trace ID is valid"));
    let correlation_start = Instant::now();
    for _ in 0..iterations {
        let groups = parsed_tier.candidate_groups_cached_for_correlation(
            TierQueryRange::default(),
            &parsed_cache,
            &absent_query,
            TelemetrySignal::Metrics,
        )?;
        if !groups.is_empty() {
            return Err("absent correlation was not pruned by the catalog".into());
        }
        black_box(groups);
    }
    let correlation_elapsed = correlation_start.elapsed();
    Ok(ControlResult {
        raw_decode_us: raw_elapsed.as_secs_f64() * 1e6 / iterations as f64,
        parsed_us: parsed_elapsed.as_secs_f64() * 1e6 / iterations as f64,
        correlation_pruned_us: correlation_elapsed.as_secs_f64() * 1e6 / iterations as f64,
        iterations,
        parsed_hits: parsed_cache.stats().parsed_hits,
    })
}

fn query_control(
    tier: &TelemetryObjectTier<LocalObjectStore>,
    cache: &SsdObjectCache,
) -> Result<u64, Box<dyn std::error::Error>> {
    let groups = tier.candidate_groups_cached(TierQueryRange::default(), cache)?;
    let group = groups.first().ok_or("published group is missing")?;
    let manifest = tier.load_group_cached(group, cache)?;
    Ok(manifest.group_sequence)
}

fn time<T>(operation: impl FnOnce() -> T) -> (std::time::Duration, T) {
    let start = Instant::now();
    let result = operation();
    (start.elapsed(), result)
}

fn parse<T: std::str::FromStr>(value: Option<String>, name: &str) -> Result<T, String> {
    value
        .ok_or_else(|| format!("missing value for {name}"))?
        .parse()
        .map_err(|_| format!("invalid value for {name}"))
}

fn benchmark_directory() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shard-telemetry-tier-cache-bench-{}-{nanos}",
        std::process::id()
    ))
}
