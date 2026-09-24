use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shard_telemetry::{
    DurableTelemetryConfig, DurableTelemetryStore, LokiEntry, LokiStore, StripeConfig,
};

const DEFAULT_RECORDS: usize = 100_000;
const DEFAULT_ITERATIONS: usize = 100;
const BATCH_SIZE: usize = 1_000;

fn setting(name: &str, default: usize) -> Result<usize, Box<dyn Error>> {
    Ok(std::env::var(name)
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(default))
}

fn percentile(samples: &mut [Duration], numerator: usize, denominator: usize) -> Duration {
    samples.sort_unstable();
    let index = samples
        .len()
        .saturating_mul(numerator)
        .div_ceil(denominator)
        .saturating_sub(1)
        .min(samples.len().saturating_sub(1));
    samples[index]
}

fn run_query(
    store: &DurableTelemetryStore,
    expression: &str,
    records: usize,
    iterations: usize,
) -> Result<(), Box<dyn Error>> {
    let end = i64::try_from(records)?.saturating_add(1);
    let mut samples = Vec::with_capacity(iterations);
    let mut total_lines = 0usize;
    let mut total_bytes = 0usize;
    let mut returned = 0usize;
    for _ in 0..iterations {
        let started = Instant::now();
        let result = store.query_range("benchmark", expression, 1, end, 100, true)?;
        samples.push(started.elapsed());
        total_lines = total_lines.saturating_add(result.lines_processed);
        total_bytes = total_bytes.saturating_add(result.bytes_processed);
        returned = result.entries.len();
    }
    let p50 = percentile(&mut samples, 50, 100);
    let p95 = percentile(&mut samples, 95, 100);
    let p99 = percentile(&mut samples, 99, 100);
    let total_seconds = samples.iter().map(Duration::as_secs_f64).sum::<f64>();
    let queries_per_second = if total_seconds > 0.0 {
        iterations as f64 / total_seconds
    } else {
        0.0
    };
    println!(
        "loki_query expression={expression:?} records={records} iterations={iterations} returned={returned} lines_per_query={:.1} bytes_per_query={:.1} qps={queries_per_second:.1} p50_us={:.3} p95_us={:.3} p99_us={:.3}",
        total_lines as f64 / iterations.max(1) as f64,
        total_bytes as f64 / iterations.max(1) as f64,
        p50.as_secs_f64() * 1_000_000.0,
        p95.as_secs_f64() * 1_000_000.0,
        p99.as_secs_f64() * 1_000_000.0,
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let records = setting("LOKI_BENCH_RECORDS", DEFAULT_RECORDS)?;
    let iterations = setting("LOKI_BENCH_ITERATIONS", DEFAULT_ITERATIONS)?;
    if records == 0 || iterations == 0 {
        return Err("LOKI_BENCH_RECORDS and LOKI_BENCH_ITERATIONS must be nonzero".into());
    }
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-loki-query-bench-{}-{nonce}",
        std::process::id()
    ));
    let store = DurableTelemetryStore::open(DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: false,
        retention: None,
        shard_count: 4,
        tenant_partitions: 8,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    })?;

    let build_started = Instant::now();
    for start in (0..records).step_by(BATCH_SIZE) {
        let end = records.min(start.saturating_add(BATCH_SIZE));
        let entries = (start..end)
            .map(|index| LokiEntry {
                timestamp_unix_nanos: i64::try_from(index.saturating_add(1))
                    .expect("benchmark timestamp fits u64"),
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                line: if index % 1_000 == 0 {
                    format!("needle request_id={index}")
                } else {
                    format!("noise request_id={index}")
                },
                structured_metadata: BTreeMap::new(),
            })
            .collect();
        store.push("benchmark", entries)?;
    }
    println!(
        "loki_query_build records={records} seconds={:.6}",
        build_started.elapsed().as_secs_f64()
    );
    run_query(&store, r#"{app="api"}"#, records, iterations)?;
    run_query(&store, r#"{app="api"} |= "needle""#, records, iterations)?;
    drop(store);
    fs::remove_dir_all(directory)?;
    Ok(())
}
