//! Unequal signal-window and recovery regressions for the durable store.

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::{
    collector::{metrics::v1::ExportMetricsServiceRequest, trace::v1::ExportTraceServiceRequest},
    common::v1::{AnyValue, KeyValue, any_value},
    metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;
use shard_stream_core::ShardId;

use super::*;

#[test]
fn unequal_signal_retention_survives_recovery_and_maintenance() {
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("timestamp");
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-signal-windows-{}-{now}",
        std::process::id()
    ));
    let config = DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: Some(directory.join("objects")),
        s3_object_store: None,
        recovery_journal: true,
        retention: None,
        shard_count: 1,
        tenant_partitions: 1,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    };
    let mut limits = DurableTelemetryLimits::default();
    limits.signals.logs.retention = Some(Duration::from_secs(60));
    limits.signals.traces.retention = Some(Duration::from_secs(120));
    limits.signals.metrics.retention = Some(Duration::from_secs(180));
    let tier = ObjectTierConfig {
        target_group_payload_bytes: 1,
        ..ObjectTierConfig::default()
    };
    let store = DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
        config.clone(),
        tier,
        limits.clone(),
    )
    .expect("store");
    let timestamps = [240, 150, 90, 10].map(|seconds| now - seconds * 1_000_000_000);
    for (index, timestamp) in timestamps.into_iter().enumerate() {
        store
            .push(
                "tenant-a",
                vec![LokiEntry {
                    timestamp_unix_nanos: i64::try_from(timestamp).expect("Loki timestamp"),
                    labels: BTreeMap::new(),
                    line: format!("retention {index}"),
                    structured_metadata: BTreeMap::new(),
                }],
            )
            .expect("log append");
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_owned(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("retention".to_owned())),
                        }),
                        ..KeyValue::default()
                    }],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![u8::try_from(index + 1).expect("trace ID"); 16],
                        span_id: vec![1; 8],
                        name: "retention".to_owned(),
                        start_time_unix_nano: if index == 2 {
                            timestamp - 60_000_000_000
                        } else {
                            timestamp
                        },
                        end_time_unix_nano: timestamp + 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        store
            .append_trace_events(
                crate::OtlpTelemetryDecoder
                    .decode_traces("tenant-a", &request.encode_to_vec())
                    .expect("traces"),
                true,
            )
            .expect("trace append");
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "retention".to_owned(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: timestamp,
                                value: Some(number_data_point::Value::AsInt(1)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let points = crate::OtlpTelemetryDecoder
            .decode_metrics("tenant-a", &request.encode_to_vec())
            .expect("metrics")
            .into_iter()
            .map(|event| {
                event.into_durable(
                    ShardId::new(0),
                    TopicPartition::new(crate::METRICS_TOPIC_ID, LogicalPartitionId::new(0)),
                    LogicalOffset::new(0),
                )
            })
            .collect();
        store
            .append_metric_points(points, true)
            .expect("metric append");
        // Physical retention removes whole published groups. Keep each age
        // cohort separate so expired objects can retire without removing a
        // group containing a retained span or point.
        store
            .flush(Duration::from_secs(30))
            .expect("age cohort flush");
    }
    store.flush(Duration::from_secs(30)).expect("flush");
    let historical_crossing_query = crate::TraceQuery {
        tenant: Arc::from("tenant-a"),
        end_time_unix_nanos: Some(now - 130_000_000_000),
        limit: 10,
        ..Default::default()
    };
    assert_eq!(
        store
            .query_traces(&historical_crossing_query)
            .expect("crossing span before cutoff")
            .len(),
        1
    );
    assert_signal_windows(&store);
    let report = store.compact_retention().expect("maintenance");
    assert!(
        report.retired_object_groups > 0 || report.advanced_offsets > 0,
        "expired groups are reclaimed"
    );
    assert_signal_windows(&store);
    drop(store);
    let recovered = DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
        config.clone(),
        tier,
        limits.clone(),
    )
    .expect("recover");
    assert_signal_windows(&recovered);
    assert_eq!(
        recovered
            .query_traces(&historical_crossing_query)
            .expect("recovered crossing span")
            .len(),
        1
    );
    drop(recovered);
    let mut global = config;
    global.retention = Some(Duration::from_secs(60));
    let overridden =
        DurableTelemetryStore::open_with_object_tier_config_and_local_limits(global, tier, limits)
            .expect("explicit global override");
    assert_eq!(
        overridden.signal_retention,
        [Some(Duration::from_secs(60)); 3]
    );
    assert_eq!(
        overridden
            .query_metrics(&crate::MetricQuery {
                tenant: Arc::from("tenant-a"),
                limit: 10,
                ..Default::default()
            })
            .expect("global metrics")
            .len(),
        1
    );
    drop(overridden);
    fs::remove_dir_all(directory).expect("remove fixture");
}

fn assert_signal_windows(store: &DurableTelemetryStore) {
    assert_eq!(
        store
            .query_native(&NativeQuery {
                tenant: "tenant-a".to_owned(),
                labels: BTreeMap::new(),
                terms: Vec::new(),
                start_timestamp_unix_nanos: None,
                end_timestamp_unix_nanos: None,
                limit: 10,
                direction: NativeQueryDirection::OldestFirst,
            })
            .expect("logs")
            .len(),
        1
    );
    assert_eq!(
        store
            .query_traces(&crate::TraceQuery {
                tenant: Arc::from("tenant-a"),
                limit: 10,
                ..Default::default()
            })
            .expect("traces")
            .len(),
        2
    );
    assert_eq!(
        store
            .query_metrics(&crate::MetricQuery {
                tenant: Arc::from("tenant-a"),
                limit: 10,
                ..Default::default()
            })
            .expect("metrics")
            .len(),
        3
    );
    assert!(
        store
            .query_metrics(&crate::MetricQuery {
                tenant: Arc::from("tenant-b"),
                limit: 10,
                ..Default::default()
            })
            .expect("tenant isolation")
            .is_empty()
    );
    for (relation, expected) in [
        (AnalyticsRelation::Logs, 1),
        (AnalyticsRelation::Spans, 2),
        (AnalyticsRelation::MetricPoints, 3),
    ] {
        let mut request = AnalyticsScanRequest::for_relation("tenant-a", relation);
        request.limit = Some(10);
        assert_eq!(
            analytics_rows(store, &request),
            expected,
            "unfiltered {relation:?}"
        );
    }
    let mut projected = AnalyticsScanRequest::for_relation("tenant-a", AnalyticsRelation::Spans);
    projected.columns = vec![
        crate::AnalyticsColumn::Timestamp,
        crate::AnalyticsColumn::Name,
    ];
    projected
        .resource_attributes
        .push(crate::MetadataField::new("service.name", "retention"));
    projected.limit = Some(10);
    assert_eq!(
        analytics_rows(store, &projected),
        2,
        "resource projection retains crossing span"
    );
    projected.trace_id = Some(crate::TraceId::from_bytes([1; 16]).expect("expired trace ID"));
    assert_eq!(analytics_rows(store, &projected), 0, "expired exact trace");
    projected.trace_id = Some(crate::TraceId::from_bytes([3; 16]).expect("crossing trace ID"));
    assert_eq!(analytics_rows(store, &projected), 1, "retained exact trace");
    let points = store
        .query_metrics(&crate::MetricQuery {
            tenant: Arc::from("tenant-a"),
            limit: 10,
            ..Default::default()
        })
        .expect("metric series fixture");
    let mut exact_metric =
        AnalyticsScanRequest::for_relation("tenant-a", AnalyticsRelation::MetricPoints);
    exact_metric.series_id = Some(points[0].series_fingerprint());
    exact_metric.columns = vec![
        crate::AnalyticsColumn::Timestamp,
        crate::AnalyticsColumn::ScalarInteger,
    ];
    exact_metric.limit = Some(10);
    assert_eq!(
        analytics_rows(store, &exact_metric),
        3,
        "exact-series optimized path"
    );
}

#[test]
fn object_reclamation_keeps_metrics_appended_after_the_rollup_checkpoint() {
    for existing_checkpoint in [true, false] {
        assert_metric_reclamation_after_checkpoint(true, existing_checkpoint);
    }
}

#[test]
fn physical_retention_keeps_metrics_appended_after_the_rollup_checkpoint() {
    for existing_checkpoint in [true, false] {
        assert_metric_reclamation_after_checkpoint(false, existing_checkpoint);
    }
}

fn assert_metric_reclamation_after_checkpoint(object_tier: bool, existing_checkpoint: bool) {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock for unique fixture path")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-rollup-reclamation-{}-{nonce}-{object_tier}-{existing_checkpoint}",
        std::process::id(),
    ));
    let config = DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: object_tier.then(|| directory.join("objects")),
        s3_object_store: None,
        recovery_journal: true,
        retention: None,
        shard_count: 1,
        tenant_partitions: 1,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    };
    let limits = DurableTelemetryLimits {
        max_lifetime_rollup_series: Some(8),
        max_lifetime_rollup_bytes: 1024 * 1024,
        ..DurableTelemetryLimits::default()
    };
    let tier = ObjectTierConfig {
        target_group_payload_bytes: 1,
        ..ObjectTierConfig::default()
    };
    let open = || {
        DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
            config.clone(),
            tier,
            limits.clone(),
        )
        .expect("real journal-backed store")
    };
    let store = open();
    if existing_checkpoint {
        store
            .append_metric_point(reclamation_metric_point(1), true)
            .expect("initial indexed metric");
    }
    store.flush(Duration::from_secs(30)).expect("initial flush");
    drop(store);

    // Reopen the actual coordinator/index journals and persisted rollup catalog
    // before placing a new complete batch after the last rollup snapshot.
    let store = open();
    let partition = TopicPartition::new(crate::METRICS_TOPIC_ID, LogicalPartitionId::new(0));
    let expected_checkpoint = existing_checkpoint.then_some(LogicalOffset::new(1));
    assert_eq!(
        store
            .lifetime_rollups
            .as_ref()
            .expect("enabled rollups")
            .lock()
            .expect("catalog")
            .checkpoint(partition),
        expected_checkpoint
    );
    let original_start = store.engine.watermarks(partition).expect("WAL").log_start;
    let ack = store
        .append_metric_points(
            vec![reclamation_metric_point(2), reclamation_metric_point(3)],
            true,
        )
        .expect("new complete indexed metric batch");
    let ack = ack.partitions.first().expect("metric acknowledgement");
    assert_eq!(ack.topic_partition, partition);
    assert_eq!(ack.first_offset, if existing_checkpoint { 1 } else { 0 });
    assert_eq!(ack.last_offset - ack.first_offset, 1);
    let indexed_end = LogicalOffset::new(ack.last_offset + 1);
    assert_eq!(
        store
            .engine
            .durable_sink_checkpoint(partition)
            .expect("index checkpoint")
            .expect("indexed batch")
            .next_offset,
        indexed_end
    );

    // These are the actual production stages following a flush's rollup
    // snapshot. Ordering the new append here reproduces the race without a
    // sleep, retry, fake checkpoint, or production-only synchronization hook.
    if object_tier {
        store
            .service
            .flush_object_tier()
            .expect("object publication");
        store.reclaim_source_packs().expect("source reclamation");
    } else {
        store
            .compact_retention_after_flush([0, 0, 500])
            .expect("post-flush expired-metric scan");
    }
    let start_after_maintenance = store.engine.watermarks(partition).expect("WAL").log_start;
    store
        .checkpoint_lifetime_rollups()
        .expect("new metric batch must remain available after source maintenance");
    let initial_points = if existing_checkpoint { 1 } else { 0 };
    let initial_total = if existing_checkpoint { 1 } else { 0 };
    assert_reclamation_rollup(&store, initial_points + 2, initial_total + 5);
    assert_eq!(
        start_after_maintenance,
        expected_checkpoint.unwrap_or(original_start)
    );

    // Once the complete batch is really persisted in the rollup catalog, both
    // paths must release it at its actual batch boundary rather than pin forever.
    finish_reclamation(&store, object_tier);
    assert_eq!(
        store.engine.watermarks(partition).expect("WAL").log_start,
        indexed_end
    );
    drop(store);
    let store = open();
    assert_reclamation_rollup(&store, initial_points + 2, initial_total + 5);
    let ack = store
        .append_metric_point(reclamation_metric_point(4), true)
        .expect("new metric after reclaimed-WAL restart");
    assert_eq!(
        ack.partitions
            .first()
            .expect("restarted acknowledgement")
            .first_offset,
        indexed_end.get()
    );
    finish_reclamation(&store, object_tier);
    assert_eq!(
        store.engine.watermarks(partition).expect("WAL").log_start,
        LogicalOffset::new(indexed_end.get() + 1)
    );
    assert_reclamation_rollup(&store, initial_points + 3, initial_total + 9);
    drop(store);
    let store = open();
    assert_reclamation_rollup(&store, initial_points + 3, initial_total + 9);
    drop(store);
    fs::remove_dir_all(directory).expect("remove fixture");
}

fn finish_reclamation(store: &DurableTelemetryStore, object_tier: bool) {
    if object_tier {
        store
            .flush(Duration::from_secs(30))
            .expect("checkpointed reclamation");
    } else {
        store
            .compact_retention_before(500)
            .expect("checkpointed retention");
    }
}

fn assert_reclamation_rollup(store: &DurableTelemetryStore, points: u64, total: i64) {
    let rollups = store
        .query_lifetime_metric_rollups("tenant-a", Some("reclamation_total"))
        .expect("complete lifetime outcome");
    assert_eq!(rollups.len(), 1);
    assert_eq!(rollups[0].observed_points, points);
    assert_eq!(rollups[0].reset_count, 0);
    assert_eq!(
        rollups[0].outcome,
        crate::MetricValue::Sum(crate::NumberValue::Integer(total))
    );
    assert!(
        store
            .query_lifetime_metric_rollups("tenant-b", Some("reclamation_total"))
            .expect("other tenant")
            .is_empty()
    );
}

fn reclamation_metric_point(value: i64) -> crate::DurableMetricPoint {
    crate::DurableMetricPoint {
        stream_shard_id: ShardId::new(0),
        record_ref: crate::TelemetryRecordRef::for_signal(
            crate::TelemetrySignal::Metrics,
            TopicPartition::new(crate::METRICS_TOPIC_ID, LogicalPartitionId::new(0)),
            LogicalOffset::new(0),
        ),
        identity: Arc::new(crate::MetricIdentity {
            tenant: Arc::from("tenant-a"),
            resource: Arc::new(crate::ResourceContext::default()),
            scope: Arc::new(crate::ScopeContext::default()),
            name: Arc::from("reclamation_total"),
            unit: Arc::from("1"),
            kind: crate::MetricKind::Sum {
                temporality: 1,
                monotonic: true,
            },
            point_attributes: Arc::new(Vec::new()),
        }),
        description: Arc::from("synthetic retention regression"),
        metadata: Arc::new(Vec::new()),
        start_time_unix_nanos: 0,
        timestamp_unix_nanos: u64::try_from(value).expect("positive fixture timestamp"),
        flags: 0,
        value: crate::MetricValue::Sum(crate::NumberValue::Integer(value)),
        exemplars: Arc::new(Vec::new()),
    }
}

fn analytics_rows(store: &DurableTelemetryStore, request: &AnalyticsScanRequest) -> usize {
    let mut rows = 0;
    store
        .scan_analytics(request, &mut |batch| {
            rows += batch.len();
            Ok(())
        })
        .expect("analytical retention query");
    rows
}

#[test]
fn effective_signal_retention_requires_recoverable_storage_and_nonzero_window() {
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-retention-preflight-{}",
        std::process::id()
    ));
    let config = DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: false,
        retention: None,
        shard_count: 1,
        tenant_partitions: 1,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    };
    let mut limits = DurableTelemetryLimits::default();
    limits.signals.traces.retention = Some(Duration::from_secs(120));
    assert!(DurableTelemetryStore::open_with_local_limits(config.clone(), limits.clone()).is_err());
    assert!(!directory.exists(), "preflight failure creates no store");
    let mut recoverable = config;
    recoverable.recovery_journal = true;
    limits.signals.traces.retention = Some(Duration::ZERO);
    assert!(DurableTelemetryStore::open_with_local_limits(recoverable, limits).is_err());
    assert!(!directory.exists(), "zero-window failure creates no store");
}
