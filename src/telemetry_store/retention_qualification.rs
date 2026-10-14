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
    let store = DurableTelemetryStore::open_with_local_limits(config.clone(), limits.clone())
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
    let recovered = DurableTelemetryStore::open_with_local_limits(config.clone(), limits.clone())
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
    let overridden = DurableTelemetryStore::open_with_local_limits(global, limits)
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
