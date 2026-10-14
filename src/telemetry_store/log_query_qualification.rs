//! Real cold log groups exercise native ordering, exact identities and refusal.

use super::*;
use crate::{
    CorrelationQuery, EmbeddedTelemetryConfig, LogQuery, NativeTelemetryAppendAck, OtlpLogEvent,
    TelemetryAttribute, TelemetryRecordRef, TelemetrySignal, TelemetryValue, TraceId,
};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

fn now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("time")
}

fn fixture(name: &str) -> EmbeddedTelemetryConfig {
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-{name}-{}-{}",
        std::process::id(),
        now()
    ));
    let mut config = EmbeddedTelemetryConfig::bounded_local(&directory, Duration::from_secs(3_600));
    config.store.shard_count = 1;
    config.store.tenant_partitions = 1;
    config.store.append_linger = Duration::ZERO;
    config = config.with_max_ram_bytes(64 * 1024 * 1024);
    config.object_tier.target_group_payload_bytes = 1;
    let work = config
        .local_limits
        .native_query_work
        .as_mut()
        .expect("work limits");
    work.max_winner_records = 2;
    work.max_winner_bytes = 32 * 1024;
    config
}

fn open(config: &EmbeddedTelemetryConfig) -> DurableTelemetryStore {
    DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
        config.store.clone(),
        config.object_tier,
        config.local_limits.clone(),
    )
    .expect("actual store")
}

fn event(time: u64, trace: TraceId, value: TelemetryValue) -> OtlpLogEvent {
    OtlpLogEvent {
        timestamp_unix_nanos: time,
        message: Arc::from("typed log query"),
        trace_id: Some(trace),
        attributes: Arc::new(vec![TelemetryAttribute::new("typed", value)]),
        ..OtlpLogEvent::default()
    }
}

fn refs(ack: &NativeTelemetryAppendAck) -> Vec<TelemetryRecordRef> {
    ack.partitions
        .iter()
        .flat_map(|part| {
            (part.first_offset..=part.last_offset).map(|offset| {
                TelemetryRecordRef::for_signal(
                    TelemetrySignal::Logs,
                    part.topic_partition,
                    LogicalOffset::new(offset),
                )
            })
        })
        .collect()
}

fn query_index_lengths(directory: &Path) -> Vec<(usize, usize)> {
    let mut lengths = Vec::new();
    for entry in fs::read_dir(directory).expect("owned directory") {
        let entry = entry.expect("owned entry");
        let path = entry.path();
        if entry.file_type().expect("owned file type").is_dir() {
            lengths.extend(query_index_lengths(&path));
            continue;
        }
        let mut file = fs::File::open(path).expect("owned file");
        let mut header = [0; 16];
        if file.read_exact(&mut header).is_ok() && &header[..8] == b"SLTQIX1\0" {
            lengths.push((
                usize::try_from(file.metadata().expect("index size").len()).expect("size"),
                usize::try_from(u64::from_le_bytes(header[8..].try_into().expect("length")))
                    .expect("decoded size"),
            ));
        }
    }
    lengths
}

#[test]
fn bounded_logs_scan_unsorted_groups_and_exact_typed_cursor_pages_after_reopen() {
    let config = fixture("bounded-log-pages");
    let timestamp = now() - 120_000_000_000;
    let trace = TraceId::from_bytes([27; 16]).expect("trace");
    let attribute = TelemetryAttribute::new("typed", TelemetryValue::Integer(42));
    let store = open(&config);
    let mut all = BTreeMap::<&str, Vec<TelemetryRecordRef>>::new();
    let mut expected = BTreeMap::<&str, Vec<TelemetryRecordRef>>::new();
    let mut chronological = BTreeMap::<&str, Vec<(u64, TelemetryRecordRef)>>::new();
    for ordinal in 0..88 {
        let time = timestamp + ((ordinal * 17) % 89) * 1_000_000;
        for tenant in ["tenant-a", "tenant-b"] {
            let value = if ordinal < 72 {
                TelemetryValue::String(Arc::from("42"))
            } else {
                TelemetryValue::Integer(42)
            };
            let appended = refs(
                &store
                    .append_log_events(tenant, vec![event(time, trace, value)], true)
                    .expect("log append"),
            );
            all.entry(tenant)
                .or_default()
                .extend(appended.iter().copied());
            chronological
                .entry(tenant)
                .or_default()
                .extend(appended.iter().map(|record| (time, *record)));
            if ordinal >= 72 {
                expected.entry(tenant).or_default().extend(appended);
            }
        }
        store
            .flush(Duration::from_secs(30))
            .expect("independent cold groups");
    }
    let verify = |store: &DurableTelemetryStore| {
        for tenant in ["tenant-a", "tenant-b"] {
            let partition = all[tenant][0].topic_partition;
            let offset_query = LogQuery::new(partition)
                .with_field("resource.loki.tenant", tenant)
                .with_limit(2);
            let offset = store
                .service
                .query_all(&offset_query)
                .expect("bounded offset page");
            assert_eq!(
                offset
                    .iter()
                    .map(|matched| matched.record.record_ref)
                    .collect::<Vec<_>>(),
                all[tenant][..2]
            );
            let error = store
                .service
                .query_all(&offset_query.clone().with_limit(usize::MAX))
                .expect_err("requested limit cannot bypass winner admission");
            assert!(error.to_string().contains("native query capacity"));
            assert!(
                store
                    .service
                    .query_all(&offset_query.clone().with_term("absent-token"))
                    .expect("nonmatching terms do not consume the page")
                    .is_empty()
            );
            let newest = store
                .service
                .query_all(&offset_query.clone().newest_first())
                .expect("bounded newest offset page");
            assert_eq!(
                newest
                    .iter()
                    .map(|matched| matched.record.record_ref)
                    .collect::<Vec<_>>(),
                all[tenant]
                    .iter()
                    .rev()
                    .take(2)
                    .copied()
                    .collect::<Vec<_>>()
            );
            let mut timed = chronological[tenant].clone();
            timed.sort_unstable();
            let chronological_page = store
                .service
                .query_all(&offset_query.clone().sort_by_timestamp())
                .expect("bounded timestamp page");
            assert_eq!(
                chronological_page
                    .iter()
                    .map(|matched| matched.record.record_ref)
                    .collect::<Vec<_>>(),
                timed
                    .iter()
                    .take(2)
                    .map(|(_, record)| *record)
                    .collect::<Vec<_>>()
            );
            let newest = store
                .service
                .query_all(&offset_query.clone().sort_by_timestamp().newest_first())
                .expect("bounded newest timestamp page");
            assert_eq!(
                newest
                    .iter()
                    .map(|matched| matched.record.record_ref)
                    .collect::<Vec<_>>(),
                timed
                    .iter()
                    .rev()
                    .take(2)
                    .map(|(_, record)| *record)
                    .collect::<Vec<_>>()
            );
            let query = CorrelationQuery::new(tenant)
                .with_trace_id(trace)
                .for_signal(TelemetrySignal::Logs)
                .with_attribute(&attribute)
                .with_limit(1);
            let mut page = query.clone();
            let mut actual = Vec::new();
            loop {
                let results = store.query_correlations(&page).expect("typed cursor page");
                let Some(last) = results.last().copied() else {
                    break;
                };
                assert_eq!(results.len(), 1);
                page = query.clone().after(last);
                actual.extend(results);
            }
            assert_eq!(
                actual, expected[tenant],
                "string 42 and foreign-tenant records cannot consume the limit"
            );
        }
        assert!(
            store
                .query_correlations(
                    &CorrelationQuery::new("absent")
                        .with_trace_id(trace)
                        .for_signal(TelemetrySignal::Logs)
                        .with_attribute(&attribute)
                        .with_limit(1)
                )
                .expect("absent tenant")
                .is_empty()
        );
    };
    verify(&store);
    drop(store);
    let store = open(&config);
    assert_eq!(
        store
            .service
            .retained_payload_bytes()
            .expect("resident payloads"),
        0
    );
    let before = store
        .service
        .object_tier_cache_stats()
        .expect("cache stats");
    verify(&store);
    let after = store
        .service
        .object_tier_cache_stats()
        .expect("queried stats");
    assert!(
        after.payload.hits + after.payload.misses > before.payload.hits + before.payload.misses
    );
    drop(store);
    fs::remove_dir_all(config.store.data_directory).expect("remove owned fixture");
}

#[test]
fn limit_one_log_query_refuses_encoded_typed_expansion_and_winner_bytes_after_cold_reopen() {
    let mut config = fixture("bounded-log-refusal");
    config.local_limits.native_query_work = None;
    let trace = TraceId::from_bytes([28; 16]).expect("trace");
    let attribute = TelemetryAttribute::new("typed", TelemetryValue::Integer(42));
    let mut row = event(now() - 120_000_000_000, trace, TelemetryValue::Integer(42));
    row.body = Some(TelemetryValue::String(Arc::from("x".repeat(16 * 1024))));
    let store = open(&config);
    store
        .append_log_events("tenant-a", vec![row], true)
        .expect("legacy valid large typed body");
    store
        .flush(Duration::from_secs(30))
        .expect("cold publication");
    drop(store);
    let query = CorrelationQuery::new("tenant-a")
        .with_trace_id(trace)
        .for_signal(TelemetrySignal::Logs)
        .with_attribute(&attribute)
        .with_limit(1);
    let baseline = open(&config);
    assert_eq!(
        baseline
            .query_correlations(&query)
            .expect("unbudgeted control")
            .len(),
        1
    );
    drop(baseline);
    let work = crate::NativeQueryWorkLimits::for_ram_budget(64 * 1024 * 1024, 1);
    for boundary in ["encoded", "typed", "winner", "header"] {
        let mut limit = work;
        match boundary {
            "encoded" => limit.max_encoded_block_bytes = 1,
            "typed" => limit.max_decoded_sidecar_bytes = 4 * 1024,
            "winner" => limit.max_winner_bytes = 4 * 1024,
            "header" => limit.max_winner_bytes = 1,
            _ => unreachable!("four boundaries"),
        }
        config.local_limits.native_query_work = Some(limit);
        let store = open(&config);
        let before = store
            .service
            .object_tier_cache_stats()
            .expect("cold cache stats");
        let error = store
            .query_correlations(&query)
            .expect_err("limit1 cannot bypass work admission");
        assert!(
            error.to_string().contains("native query capacity"),
            "{boundary}: {error}"
        );
        let compatibility_query = CorrelationQuery::new("tenant-a")
            .with_trace_id(trace)
            .for_signal(TelemetrySignal::Logs)
            .with_limit(1);
        if boundary == "winner" {
            assert_eq!(
                store
                    .query_correlations(&compatibility_query)
                    .expect("reference projection does not retain the typed body")
                    .len(),
                1
            );
        } else {
            assert!(
                store
                    .query_correlations(&compatibility_query)
                    .expect_err("checked compatibility-field fallback preserves refusal")
                    .to_string()
                    .contains("native query capacity")
            );
        }
        if boundary == "encoded" {
            let after = store
                .service
                .object_tier_cache_stats()
                .expect("refused cache stats");
            assert_eq!(
                before.payload.hits + before.payload.misses,
                after.payload.hits + after.payload.misses,
                "encoded refusal precedes payload prefetch"
            );
        }
        drop(store);
    }
    fs::remove_dir_all(config.store.data_directory).expect("remove owned fixture");
}

#[test]
fn cold_log_query_refuses_compressed_control_expansion_before_payload_read() {
    let mut config = fixture("bounded-log-control-expansion");
    config.local_limits.native_query_work = None;
    let trace = TraceId::from_bytes([29; 16]).expect("trace");
    let time = now() - 120_000_000_000;
    let rows = (0..128)
        .map(|ordinal| {
            let mut row = event(time, trace, TelemetryValue::Integer(42));
            row.fields = Arc::new(vec![crate::MetadataField::new(
                "indexed",
                format!("{}-{ordinal}", "compressible".repeat(256)),
            )]);
            row
        })
        .collect();
    let store = open(&config);
    let acknowledgement = store
        .append_log_events("tenant-a", rows, true)
        .expect("compressible indexed fields");
    let partition = refs(&acknowledgement)[0].topic_partition;
    store.flush(Duration::from_secs(30)).expect("cold group");
    drop(store);
    let (encoded, decoded) = query_index_lengths(&config.store.data_directory)
        .into_iter()
        .max_by_key(|(_, decoded)| *decoded)
        .expect("actual immutable query index");
    assert!(
        encoded < decoded - 1,
        "fixture must compress the query index"
    );
    let query = LogQuery::new(partition)
        .with_field("resource.loki.tenant", "tenant-a")
        .with_limit(1);
    let baseline = open(&config);
    assert_eq!(
        baseline
            .service
            .query_all(&query)
            .expect("unbudgeted control")
            .len(),
        1
    );
    drop(baseline);
    let mut work = crate::NativeQueryWorkLimits::for_ram_budget(64 * 1024 * 1024, 1);
    work.max_control_object_bytes = decoded - 1;
    config.local_limits.native_query_work = Some(work);
    let store = open(&config);
    let before = store
        .service
        .object_tier_cache_stats()
        .expect("cold cache stats");
    let error = store
        .service
        .query_all(&query)
        .expect_err("declared control expansion");
    assert!(
        error.to_string().contains(&format!(
            "control object bytes: {decoded} > {}",
            decoded - 1
        )),
        "{error}"
    );
    let after = store
        .service
        .object_tier_cache_stats()
        .expect("refused cache stats");
    assert_eq!(
        before.payload.hits + before.payload.misses,
        after.payload.hits + after.payload.misses
    );
    drop(store);
    let mut work = crate::NativeQueryWorkLimits::for_ram_budget(64 * 1024 * 1024, 1);
    work.max_decoded_records = 1;
    config.local_limits.native_query_work = Some(work);
    let store = open(&config);
    let error = store
        .service
        .query_all(&query)
        .expect_err("limit1 still admits decoded rows");
    assert!(
        error.to_string().contains("decoded block rows: 128 > 1"),
        "{error}"
    );
    drop(store);
    fs::remove_dir_all(config.store.data_directory).expect("remove owned fixture");
}

#[test]
fn native_log_pages_discover_lazily_and_recheck_capacity_after_cold_reopen() {
    let mut config = fixture("bounded-native-log-discovery");
    config
        .local_limits
        .native_query_work
        .as_mut()
        .expect("work limits")
        .max_decoded_records = 2;
    let time = now() - 120_000_000_000;
    let trace = TraceId::from_bytes([30; 16]).expect("trace");
    let store = open(&config);
    let mut expected = Vec::new();
    let mut partition = None;
    for ordinal in 0..48 {
        let timestamp = time + ((ordinal * 17) % 53) * 1_000_000;
        for tenant in ["tenant-a", "tenant-b"] {
            let message = format!("{tenant}-{ordinal}");
            let mut row = event(timestamp, trace, TelemetryValue::Integer(42));
            row.message = Arc::from(message.as_str());
            let acknowledgement = store
                .append_log_events(tenant, vec![row], true)
                .expect("shared partition append");
            partition = Some(refs(&acknowledgement)[0].topic_partition);
            if tenant == "tenant-a" {
                expected.push((timestamp, message));
            }
        }
        store
            .flush(Duration::from_secs(30))
            .expect("many cold groups");
    }
    expected.sort_unstable();
    let request = crate::NativeLogPageQuery {
        query: crate::NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::new(),
            terms: Vec::new(),
            start_timestamp_unix_nanos: Some(time),
            end_timestamp_unix_nanos: Some(time + 1_000_000_000),
            limit: 1,
            direction: crate::NativeQueryDirection::OldestFirst,
        },
        max_bytes: 4 * 1024,
        cursor: None,
    };
    let verify = |store: &DurableTelemetryStore| {
        let mut page = request.clone();
        let mut actual = Vec::new();
        loop {
            let result = store
                .query_native_page(&page)
                .expect("end-to-end native page");
            assert!(result.entries.len() <= 1);
            actual.extend(result.entries.into_iter().map(|entry| entry.line));
            let Some(cursor) = result.next_cursor else {
                break;
            };
            page.cursor = Some(cursor);
        }
        assert_eq!(
            actual,
            expected
                .iter()
                .map(|(_, message)| message.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            store
                .service
                .count_log_records(
                    Arc::from("tenant-a"),
                    vec![partition.expect("shared partition")]
                )
                .expect("aggregate tenant count"),
            48,
            "discovery counts remain exact beyond the page/retained-winner limit"
        );
    };
    verify(&store);
    drop(store);
    let store = open(&config);
    assert_eq!(
        store
            .service
            .retained_payload_bytes()
            .expect("cold payloads"),
        0
    );
    verify(&store);

    // This unrelated tenant's frame is outside the requested time window.
    // Tenant discovery reads it even though the match traversal would skip
    // it, demonstrating admission before producing a native response page.
    let rows = (0..3)
        .map(|_| event(time + 2_000_000_000, trace, TelemetryValue::Integer(42)))
        .collect();
    store
        .append_log_events("tenant-c", rows, true)
        .expect("valid ingestion beyond query work");
    store
        .flush(Duration::from_secs(30))
        .expect("new independent cold group");
    let assert_refusal = |store: &DurableTelemetryStore| {
        let before = store
            .service
            .object_tier_cache_stats()
            .expect("cache stats");
        let error = store
            .query_native_page(&request)
            .expect_err("discovery refuses new work");
        assert!(
            error.to_string().contains("decoded block rows: 3 > 2"),
            "{error}"
        );
        let after = store
            .service
            .object_tier_cache_stats()
            .expect("refused stats");
        assert_eq!(
            before.payload.hits + before.payload.misses,
            after.payload.hits + after.payload.misses,
            "discovery refuses before payload reads"
        );
    };
    assert_refusal(&store);
    drop(store);
    let store = open(&config);
    assert_refusal(&store);
    drop(store);
    let mut work = crate::NativeQueryWorkLimits::for_ram_budget(64 * 1024 * 1024, 1);
    work.max_control_object_bytes = query_index_lengths(&config.store.data_directory)
        .into_iter()
        .map(|(encoded, decoded)| encoded.max(decoded))
        .max()
        .expect("actual query-index sizes");
    config.local_limits.native_query_work = Some(work);
    let store = open(&config);
    let mut absent = request.clone();
    absent.query.tenant = "absent".to_owned();
    let error = store
        .query_native_page(&absent)
        .expect_err("catalog work refusal cannot become a successful absent-tenant page");
    assert!(
        error.to_string().contains("control object bytes"),
        "{error}"
    );
    drop(store);
    fs::remove_dir_all(config.store.data_directory).expect("remove owned fixture");
}
