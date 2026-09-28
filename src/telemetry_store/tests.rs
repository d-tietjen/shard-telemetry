use std::fs;
use std::num::NonZeroU16;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::{
    collector::{metrics::v1::ExportMetricsServiceRequest, trace::v1::ExportTraceServiceRequest},
    metrics::v1::{
        Exemplar, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, exemplar, metric,
        number_data_point,
    },
    trace::v1::{ResourceSpans, ScopeSpans, Span, span},
};
use prost::Message;
use shard_stream_core::ShardId;
use shard_stream_protocol::{FetchMode, FetchRequest};

use super::*;
use crate::ingest_pack::{decode_ingest_pack, validate_ingest_pack};

#[test]
fn append_submission_pool_preserves_grouping_concurrency() {
    assert_eq!(
        build_append_submission_pool(1, None)
            .expect("single stripe pool")
            .current_num_threads(),
        MIN_APPEND_SUBMISSION_THREADS
    );
    assert_eq!(
        build_append_submission_pool(16, None)
            .expect("multi-stripe pool")
            .current_num_threads(),
        16
    );
    assert_eq!(
        build_append_submission_pool(256, None)
            .expect("bounded pool")
            .current_num_threads(),
        MAX_APPEND_SUBMISSION_THREADS
    );
    assert_eq!(
        build_append_submission_pool(1, Some(1))
            .expect("embedded single-worker pool")
            .current_num_threads(),
        1
    );
    assert_eq!(
        build_append_submission_pool(1, Some(64))
            .expect("explicit multi-core pool")
            .current_num_threads(),
        64
    );
    assert!(build_append_submission_pool(1, Some(65)).is_err());
}

#[test]
fn durable_sink_worker_count_is_independent_from_physical_shards() {
    assert_eq!(durable_sink_worker_count(1, None), 1);
    assert_eq!(durable_sink_worker_count(16, None), 16);
    assert_eq!(durable_sink_worker_count(256, None), 256);
    assert_eq!(durable_sink_worker_count(512, None), 256);
    assert_eq!(durable_sink_worker_count(16, Some(4)), 4);
}

#[test]
fn object_tier_catalogs_cover_every_signal_partition() {
    let partitions = object_tier_partitions(3);
    assert_eq!(partitions.len(), 9);
    for signal in [
        crate::TelemetrySignal::Logs,
        crate::TelemetrySignal::Traces,
        crate::TelemetrySignal::Metrics,
    ] {
        assert_eq!(
            partitions
                .iter()
                .filter(|partition| partition.topic_id == signal.topic_id())
                .count(),
            3
        );
    }
    assert!(partitions.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn append_receipt_catalog_migrates_v1_without_losing_retry_identity() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-receipt-migration-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&directory).expect("directory");
    let request_id = 7_u128;
    let digest = "a".repeat(64);
    let legacy = PersistedAppendReceipts {
        version: APPEND_RECEIPTS_VERSION,
        receipts: vec![PersistedAppendReceipt {
            request_id: request_key(request_id),
            payload_digest: digest.clone(),
            recorded_at_unix_nanos: unix_nanos_now(),
            acknowledgement: crate::NativeTelemetryAppendAck {
                partitions: Vec::new(),
            },
        }],
    };
    fs::write(
        directory.join("native-append-receipts-v1.json"),
        serde_json::to_vec(&legacy).expect("encode legacy"),
    )
    .expect("write legacy");
    let catalog = AppendReceiptCatalog::open(&directory).expect("migrate catalog");
    assert!(matches!(
        catalog.reserve(request_id, &digest).expect("lookup"),
        AppendReceiptReservation::Existing(crate::NativeTelemetryAppendAck { ref partitions })
            if partitions.is_empty()
    ));
    assert!(
        directory
            .join("native-append-receipts-v2")
            .join(format!("{}.json", request_key(request_id)))
            .exists()
    );
    drop(catalog);
    fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn direct_metric_append_groups_multiple_series_before_serial_partition_append() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-direct-metrics-{}-{nonce}",
        std::process::id()
    ));
    let store = DurableTelemetryStore::open(DurableTelemetryConfig {
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
    })
    .expect("store opens");
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![
                    Metric {
                        name: "requests_total".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 10,
                                value: Some(number_data_point::Value::AsInt(7)),
                                ..NumberDataPoint::default()
                            }],
                        })),
                        ..Metric::default()
                    },
                    Metric {
                        name: "in_flight".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 11,
                                value: Some(number_data_point::Value::AsInt(3)),
                                ..NumberDataPoint::default()
                            }],
                        })),
                        ..Metric::default()
                    },
                ],
                ..ScopeMetrics::default()
            }],
            ..ResourceMetrics::default()
        }],
    };
    let points = crate::OtlpTelemetryDecoder
        .decode_metrics("tenant-a", &request.encode_to_vec())
        .expect("decode")
        .into_iter()
        .map(|event| {
            event.into_durable(
                shard_stream_core::ShardId::new(0),
                TopicPartition::new(crate::METRICS_TOPIC_ID, LogicalPartitionId::new(0)),
                LogicalOffset::new(0),
            )
        })
        .collect::<Vec<_>>();
    let mut singleton = points[0].clone();
    singleton.timestamp_unix_nanos = singleton.timestamp_unix_nanos.saturating_sub(1);
    let singleton_acknowledgement = store
        .append_metric_point(singleton, true)
        .expect("singleton direct append");
    assert_eq!(singleton_acknowledgement.partitions.len(), 1);
    let acknowledgement = store
        .append_metric_points(points, true)
        .expect("direct append");
    assert_eq!(acknowledgement.partitions.len(), 2);
    let points = store
        .query_metrics(&crate::MetricQuery {
            tenant: Arc::from("tenant-a"),
            limit: 10,
            ..crate::MetricQuery::default()
        })
        .expect("query");
    assert_eq!(points.len(), 3);
    assert_eq!(
        points
            .iter()
            .filter(|point| point.identity.name.as_ref() == "requests_total")
            .count(),
        2
    );
    assert_eq!(
        points
            .iter()
            .filter(|point| point.identity.name.as_ref() == "in_flight")
            .count(),
        1
    );
    drop(store);
    fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn shared_durable_sink_indexes_trace_and_metric_partition_envelopes() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-signals-store-{}-{nonce}",
        std::process::id()
    ));
    let store = DurableTelemetryStore::open(DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: true,
        retention: None,
        shard_count: 2,
        tenant_partitions: 8,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    })
    .expect("store opens");
    let decoder = crate::OtlpTelemetryDecoder;
    let router = crate::TelemetryRouter::new(NonZeroU16::new(8).unwrap());

    let trace_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    name: "checkout".into(),
                    start_time_unix_nano: 10,
                    end_time_unix_nano: 20,
                    events: vec![span::Event {
                        time_unix_nano: 15,
                        name: "charged".into(),
                        ..span::Event::default()
                    }],
                    links: vec![span::Link {
                        trace_id: vec![3; 16],
                        span_id: vec![4; 8],
                        ..span::Link::default()
                    }],
                    ..Span::default()
                }],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    };
    let mut trace_partitions = decoder.partition_traces(
        &router,
        decoder
            .decode_traces("tenant-a", &trace_request.encode_to_vec())
            .unwrap(),
    );
    let (trace_partition, trace_events) = trace_partitions.pop_first().unwrap();

    let metric_request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "requests".into(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 30,
                            value: Some(number_data_point::Value::AsInt(7)),
                            exemplars: vec![Exemplar {
                                time_unix_nano: 30,
                                value: Some(exemplar::Value::AsInt(7)),
                                trace_id: vec![1; 16],
                                span_id: vec![2; 8],
                                ..Exemplar::default()
                            }],
                            ..NumberDataPoint::default()
                        }],
                    })),
                    ..Metric::default()
                }],
                ..ScopeMetrics::default()
            }],
            ..ResourceMetrics::default()
        }],
    };
    let mut metric_partitions = decoder.partition_metrics(
        &router,
        decoder
            .decode_metrics("tenant-a", &metric_request.encode_to_vec())
            .unwrap(),
    );
    let (metric_partition, metric_events) = metric_partitions.pop_first().unwrap();
    let trace_id = crate::TraceId::from_bytes([1; 16]).unwrap();
    let log_partition = router.log("tenant-a", Some(trace_id), &[]);
    let log_resource = Arc::new(crate::ResourceContext {
        attributes: Arc::new(vec![crate::TelemetryAttribute::new(
            "service.name",
            crate::TelemetryValue::String(Arc::from("checkout-api")),
        )]),
        ..crate::ResourceContext::default()
    });
    let mut log_events = vec![crate::OtlpLogEvent {
        timestamp_unix_nanos: 25,
        body: Some(crate::TelemetryValue::String(Arc::from(
            "checkout request complete",
        ))),
        message: Arc::from("checkout request complete"),
        fields: Arc::new(vec![
            crate::MetadataField::new("otel.trace_id", trace_id.to_string()),
            crate::MetadataField::new("otel.severity_number", "17"),
            crate::MetadataField::new("service.version", "v1"),
            crate::MetadataField::new("resource.loki.label.service", "checkout"),
            crate::MetadataField::new("resource.service.name", "checkout-api"),
        ]),
        attributes: Arc::new(vec![crate::TelemetryAttribute::new(
            "service.version",
            crate::TelemetryValue::String(Arc::from("v1")),
        )]),
        severity_number: 17,
        trace_id: Some(trace_id),
        span_id: Some(crate::SpanId::from_bytes([2; 8]).unwrap()),
        resource: Arc::clone(&log_resource),
        compression_cohort: crate::CompressionCohortId::new(1),
        ..crate::OtlpLogEvent::default()
    }];
    log_events.push(crate::OtlpLogEvent {
        timestamp_unix_nanos: 26,
        body: Some(crate::TelemetryValue::String(Arc::from(
            "payment request complete",
        ))),
        message: Arc::from("payment request complete"),
        fields: Arc::new(vec![
            crate::MetadataField::new("otel.trace_id", trace_id.to_string()),
            crate::MetadataField::new("resource.loki.label.service", "payment"),
            crate::MetadataField::new("resource.loki.label.service_name", "payment"),
            crate::MetadataField::new("resource.service.name", "payment-api"),
        ]),
        attributes: Arc::new(Vec::new()),
        trace_id: Some(trace_id),
        span_id: None,
        resource: Arc::new(crate::ResourceContext {
            attributes: Arc::new(vec![crate::TelemetryAttribute::new(
                "service.name",
                crate::TelemetryValue::String(Arc::from("payment-api")),
            )]),
            ..crate::ResourceContext::default()
        }),
        compression_cohort: crate::CompressionCohortId::new(1),
        ..crate::OtlpLogEvent::default()
    });

    let batch = crate::NativeTelemetryBatch {
        partitions: vec![
            crate::NativePartitionAppend {
                topic_partition: log_partition,
                envelope: crate::prepare_log_envelope("tenant-a", &log_events).unwrap(),
                transient_context: None,
            },
            crate::NativePartitionAppend {
                topic_partition: trace_partition,
                envelope: crate::prepare_trace_envelope(trace_partition, trace_events).unwrap(),
                transient_context: None,
            },
            crate::NativePartitionAppend {
                topic_partition: metric_partition,
                envelope: crate::prepare_metric_envelope(metric_partition, metric_events).unwrap(),
                transient_context: None,
            },
        ],
    };
    let acknowledgement = store.append_telemetry_batch(&batch, true).unwrap();
    assert_eq!(acknowledgement.partitions.len(), 3);
    let mut joined_trace_count = crate::AnalyticsScanRequest::new("tenant-a");
    joined_trace_count.columns = vec![crate::AnalyticsColumn::Offset];
    joined_trace_count.cardinality_only = true;
    joined_trace_count
        .labels
        .push(crate::MetadataField::new("service", "checkout"));
    joined_trace_count.trace_join_service = Some(Arc::from("payment"));
    let mut joined_count = 0_u64;
    store
        .scan_analytics_cardinality(&joined_trace_count, &mut |batch| {
            joined_count += batch;
            Ok(())
        })
        .unwrap();
    assert_eq!(joined_count, 1);
    joined_trace_count.trace_join_service = Some(Arc::from("payment-api"));
    joined_count = 0;
    store
        .scan_analytics_cardinality(&joined_trace_count, &mut |batch| {
            joined_count += batch;
            Ok(())
        })
        .unwrap();
    assert_eq!(joined_count, 1);
    joined_trace_count.labels.clear();
    joined_trace_count
        .resource_attributes
        .push(crate::MetadataField::new("service.name", "checkout-api"));
    joined_count = 0;
    store
        .scan_analytics_cardinality(&joined_trace_count, &mut |batch| {
            joined_count += batch;
            Ok(())
        })
        .unwrap();
    assert_eq!(joined_count, 1);
    let spans = store
        .query_traces(&crate::TraceQuery {
            tenant: Arc::from("tenant-a"),
            trace_id: Some(crate::TraceId::from_bytes([1; 16]).unwrap()),
            limit: 10,
            ..crate::TraceQuery::default()
        })
        .unwrap();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].name.as_ref(), "checkout");
    assert_eq!(
        spans[0].stream_shard_id,
        ShardId::new(trace_partition.partition_id.get() % 2)
    );
    let points = store
        .query_metrics(&crate::MetricQuery {
            tenant: Arc::from("tenant-a"),
            name: Some(Arc::from("requests")),
            limit: 10,
            ..crate::MetricQuery::default()
        })
        .unwrap();
    assert_eq!(points.len(), 1);
    assert_eq!(
        points[0].stream_shard_id,
        ShardId::new(metric_partition.partition_id.get() % 2)
    );
    assert_eq!(
        points[0].value,
        crate::MetricValue::Gauge(crate::NumberValue::Integer(7))
    );
    let correlated = store
        .query_correlations(
            &crate::CorrelationQuery::new("tenant-a")
                .with_trace_id(trace_id)
                .with_limit(10),
        )
        .unwrap();
    assert_eq!(correlated.len(), 4);
    assert!(
        [
            crate::TelemetrySignal::Logs,
            crate::TelemetrySignal::Traces,
            crate::TelemetrySignal::Metrics,
        ]
        .into_iter()
        .all(|signal| correlated.iter().any(|record| record.signal == signal))
    );
    for (relation, expected) in [
        (crate::AnalyticsRelation::Logs, 2),
        (crate::AnalyticsRelation::Spans, 1),
        (crate::AnalyticsRelation::SpanEvents, 1),
        (crate::AnalyticsRelation::SpanLinks, 1),
        (crate::AnalyticsRelation::MetricPoints, 1),
        (crate::AnalyticsRelation::MetricExemplars, 1),
    ] {
        let request = crate::AnalyticsScanRequest::for_relation("tenant-a", relation);
        let mut rows = Vec::new();
        store
            .scan_analytics(&request, &mut |batch| {
                rows.extend_from_slice(batch);
                Ok(())
            })
            .unwrap();
        assert_eq!(rows.len(), expected, "{relation:?}");
        assert_eq!(
            rows.first().map(|row| row.signal.as_ref()),
            Some(relation.signal())
        );
    }
    let mut exact_log =
        crate::AnalyticsScanRequest::for_relation("tenant-a", crate::AnalyticsRelation::Logs);
    exact_log.trace_id = Some(trace_id);
    exact_log.span_id = Some(crate::SpanId::from_bytes([2; 8]).unwrap());
    exact_log.limit = Some(1);
    exact_log.columns = vec![
        crate::AnalyticsColumn::Timestamp,
        crate::AnalyticsColumn::Message,
    ];
    let mut exact_log_rows = Vec::new();
    store
        .scan_analytics(&exact_log, &mut |batch| {
            exact_log_rows.extend_from_slice(batch);
            Ok(())
        })
        .unwrap();
    assert_eq!(exact_log_rows.len(), 1);

    let mut filtered_log =
        crate::AnalyticsScanRequest::for_relation("tenant-a", crate::AnalyticsRelation::Logs);
    filtered_log
        .labels
        .push(crate::MetadataField::new("service", "checkout"));
    filtered_log.limit = Some(1);
    filtered_log.columns = vec![
        crate::AnalyticsColumn::Timestamp,
        crate::AnalyticsColumn::Message,
    ];
    let mut filtered_log_rows = Vec::new();
    store
        .scan_analytics(&filtered_log, &mut |batch| {
            filtered_log_rows.extend_from_slice(batch);
            Ok(())
        })
        .unwrap();
    assert_eq!(filtered_log_rows.len(), 1);
    assert_eq!(
        filtered_log_rows[0].message.as_deref(),
        Some("checkout request complete")
    );

    let mut resource_filtered_log =
        crate::AnalyticsScanRequest::for_relation("tenant-a", crate::AnalyticsRelation::Logs);
    resource_filtered_log
        .resource_attributes
        .push(crate::MetadataField::new("service.name", "checkout-api"));
    resource_filtered_log.limit = Some(1);
    resource_filtered_log.columns = vec![
        crate::AnalyticsColumn::Timestamp,
        crate::AnalyticsColumn::Message,
    ];
    let mut resource_filtered_log_rows = Vec::new();
    store
        .scan_analytics(&resource_filtered_log, &mut |batch| {
            resource_filtered_log_rows.extend_from_slice(batch);
            Ok(())
        })
        .unwrap();
    assert_eq!(resource_filtered_log_rows.len(), 1);
    assert_eq!(
        resource_filtered_log_rows[0].message.as_deref(),
        Some("checkout request complete")
    );
    resource_filtered_log.limit = Some(1);
    resource_filtered_log.order = Some(AnalyticsScanOrder::RelevanceDescending);
    resource_filtered_log_rows.clear();
    store
        .scan_analytics(&resource_filtered_log, &mut |batch| {
            resource_filtered_log_rows.extend_from_slice(batch);
            Ok(())
        })
        .expect("relevance scan with resource filter");
    assert_eq!(resource_filtered_log_rows.len(), 1);
    assert_eq!(
        resource_filtered_log_rows[0].message.as_deref(),
        Some("checkout request complete")
    );
    let mut severity_scan = AnalyticsScanRequest::new("tenant-a");
    severity_scan.predicate = LogPredicate::field_numeric(
        "otel.severity_number",
        crate::NumericComparison::GreaterThanOrEqual,
        13,
    );
    severity_scan.limit = Some(1);
    severity_scan.order = Some(AnalyticsScanOrder::TimestampDescending);
    let mut severity_rows = Vec::new();
    store
        .scan_analytics(&severity_scan, &mut |batch| {
            severity_rows.extend_from_slice(batch);
            Ok(())
        })
        .expect("numeric severity scan");
    assert_eq!(severity_rows.len(), 1);
    assert_eq!(
        severity_rows[0].message.as_deref(),
        Some("checkout request complete")
    );
    resource_filtered_log.limit = None;
    resource_filtered_log.order = None;
    resource_filtered_log_rows.clear();
    store
        .scan_analytics(&resource_filtered_log, &mut |batch| {
            resource_filtered_log_rows.extend_from_slice(batch);
            Ok(())
        })
        .unwrap();
    assert_eq!(resource_filtered_log_rows.len(), 1);

    let mut mixed_filtered_log =
        crate::AnalyticsScanRequest::for_relation("tenant-a", crate::AnalyticsRelation::Logs);
    mixed_filtered_log
        .labels
        .push(crate::MetadataField::new("service", "checkout"));
    mixed_filtered_log
        .attributes
        .push(crate::MetadataField::new("service.version", "v1"));
    mixed_filtered_log.limit = Some(1);
    mixed_filtered_log.columns = vec![
        crate::AnalyticsColumn::Timestamp,
        crate::AnalyticsColumn::Message,
    ];
    let mut mixed_filtered_log_rows = Vec::new();
    store
        .scan_analytics(&mixed_filtered_log, &mut |batch| {
            mixed_filtered_log_rows.extend_from_slice(batch);
            Ok(())
        })
        .unwrap();
    assert_eq!(mixed_filtered_log_rows.len(), 1);
    assert_eq!(
        mixed_filtered_log_rows[0].message.as_deref(),
        Some("checkout request complete")
    );

    let mut exact_trace =
        crate::AnalyticsScanRequest::for_relation("tenant-a", crate::AnalyticsRelation::Spans);
    exact_trace.trace_id = Some(trace_id);
    exact_trace.span_id = Some(crate::SpanId::from_bytes([2; 8]).unwrap());
    exact_trace.name = Some(Arc::from("checkout"));
    let mut exact_trace_rows = Vec::new();
    store
        .scan_analytics(&exact_trace, &mut |batch| {
            exact_trace_rows.extend_from_slice(batch);
            Ok(())
        })
        .unwrap();
    assert_eq!(exact_trace_rows.len(), 1);

    exact_trace.columns = vec![
        crate::AnalyticsColumn::Timestamp,
        crate::AnalyticsColumn::Name,
    ];
    exact_trace_rows.clear();
    store
        .scan_analytics(&exact_trace, &mut |batch| {
            exact_trace_rows.extend_from_slice(batch);
            Ok(())
        })
        .unwrap();
    assert_eq!(exact_trace_rows.len(), 1);
    assert_eq!(exact_trace_rows[0].name.as_deref(), Some("checkout"));
    assert!(exact_trace_rows[0].resource_attributes.is_empty());
    assert!(exact_trace_rows[0].attributes_json.is_none());
    assert!(exact_trace_rows[0].events_json.is_none());

    let mut exact_metric = crate::AnalyticsScanRequest::for_relation(
        "tenant-a",
        crate::AnalyticsRelation::MetricPoints,
    );
    exact_metric.series_id = Some(points[0].series_fingerprint());
    exact_metric.name = Some(Arc::from("requests"));
    exact_metric.limit = Some(10);
    let mut exact_metric_rows = Vec::new();
    store
        .scan_analytics(&exact_metric, &mut |batch| {
            exact_metric_rows.extend_from_slice(batch);
            Ok(())
        })
        .unwrap();
    assert_eq!(exact_metric_rows.len(), 1);

    exact_metric.columns = vec![
        crate::AnalyticsColumn::Timestamp,
        crate::AnalyticsColumn::ScalarInteger,
    ];
    exact_metric_rows.clear();
    store
        .scan_analytics(&exact_metric, &mut |batch| {
            exact_metric_rows.extend_from_slice(batch);
            Ok(())
        })
        .unwrap();
    assert_eq!(exact_metric_rows.len(), 1);
    assert_eq!(exact_metric_rows[0].scalar_integer, Some(7));
    assert!(exact_metric_rows[0].labels.is_empty());
    assert!(exact_metric_rows[0].value_json.is_none());

    for (relation, start, end) in [
        (crate::AnalyticsRelation::SpanEvents, 15, 16),
        (crate::AnalyticsRelation::MetricExemplars, 30, 31),
    ] {
        let mut request = crate::AnalyticsScanRequest::for_relation("tenant-a", relation);
        request.start_timestamp_unix_nanos = Some(start);
        request.end_timestamp_unix_nanos = Some(end);
        let mut rows = Vec::new();
        store
            .scan_analytics(&request, &mut |batch| {
                rows.extend_from_slice(batch);
                Ok(())
            })
            .unwrap();
        assert_eq!(rows.len(), 1, "nested timestamp filter: {relation:?}");

        request.start_timestamp_unix_nanos = Some(end);
        request.end_timestamp_unix_nanos = Some(end + 1);
        rows.clear();
        store
            .scan_analytics(&request, &mut |batch| {
                rows.extend_from_slice(batch);
                Ok(())
            })
            .unwrap();
        assert!(rows.is_empty(), "nested timestamp residual: {relation:?}");
    }
    drop(store);
    fs::remove_dir_all(directory).expect("remove test store");
}

#[test]
fn durable_store_acknowledges_and_queries_the_same_stripe_owned_record() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-loki-store-{}-{nonce}",
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
        append_linger: Duration::from_micros(250),
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    })
    .expect("store opens");
    for timestamp in 100..103 {
        store
            .push(
                "tenant-a",
                vec![LokiEntry {
                    timestamp_unix_nanos: timestamp,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: format!("durable message {timestamp}"),
                    structured_metadata: BTreeMap::from([(
                        "trace_id".to_owned(),
                        format!("abc-{timestamp}"),
                    )]),
                }],
            )
            .expect("push is durable");
    }
    let entries = store.entries("tenant-a").expect("query");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].line, "durable message 100");
    assert_eq!(entries[0].labels["app"], "api");
    assert_eq!(entries[0].structured_metadata["trace_id"], "abc-100");
    let ranged = store
        .query_range("tenant-a", r#"{app="api"}"#, 100, 102, 2, true)
        .expect("indexed Loki range query");
    assert_eq!(
        ranged
            .entries
            .iter()
            .map(|entry| entry.line.as_str())
            .collect::<Vec<_>>(),
        ["durable message 102", "durable message 101"]
    );
    let pipelined = store
        .query_range("tenant-a", r#"{app="api"} |= "101""#, 100, 102, 2, true)
        .expect("indexed Loki pipeline query");
    assert_eq!(pipelined.entries.len(), 1);
    assert_eq!(pipelined.entries[0].line, "durable message 101");
    for index in 0..4 {
        let timestamp = 1_000 + index * 2;
        store
            .push(
                "tenant-a",
                vec![
                    LokiEntry {
                        timestamp_unix_nanos: timestamp,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: format!("keep {timestamp}"),
                        structured_metadata: BTreeMap::new(),
                    },
                    LokiEntry {
                        timestamp_unix_nanos: timestamp + 1,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: format!("noise {timestamp}"),
                        structured_metadata: BTreeMap::new(),
                    },
                ],
            )
            .expect("push residual-filter test data");
    }
    let residual = store
        .query_range(
            "tenant-a",
            r#"{app="api"} |= "keep""#,
            1_000,
            2_000,
            4,
            true,
        )
        .expect("indexed Loki residual query");
    assert_eq!(
        residual
            .entries
            .iter()
            .map(|entry| entry.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        [1_006, 1_004, 1_002, 1_000]
    );
    store
        .push(
            "tenant-a",
            vec![
                LokiEntry {
                    timestamp_unix_nanos: 2_000,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: "needle".to_owned(),
                    structured_metadata: BTreeMap::new(),
                },
                LokiEntry {
                    timestamp_unix_nanos: 2_001,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: "needlex".to_owned(),
                    structured_metadata: BTreeMap::new(),
                },
            ],
        )
        .expect("push substring test data");
    let substring = store
        .query_range(
            "tenant-a",
            r#"{app="api"} |= "needle""#,
            2_000,
            2_001,
            10,
            true,
        )
        .expect("indexed Loki substring query");
    assert_eq!(
        substring
            .entries
            .iter()
            .map(|entry| entry.line.as_str())
            .collect::<Vec<_>>(),
        ["needlex", "needle"]
    );
    drop(store);
    let recovered = DurableTelemetryStore::open(DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: false,
        retention: None,
        shard_count: 4,
        tenant_partitions: 8,
        append_linger: Duration::from_micros(250),
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    })
    .expect("store recovers");
    let entries = recovered.entries("tenant-a").expect("recovered query");
    assert_eq!(entries.len(), 13);
    assert_eq!(entries[2].line, "durable message 102");
    assert!(!directory.join("index-journal").exists());
    drop(recovered);
    fs::remove_dir_all(directory).expect("remove test store");
}

#[test]
fn object_tier_flushes_queries_cold_and_recovers_without_source_replay() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-cold-recovery-{}-{nonce}",
        std::process::id()
    ));
    let object_directory = directory.join("objects");
    let config = DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: Some(object_directory.clone()),
        s3_object_store: None,
        recovery_journal: false,
        retention: None,
        shard_count: 1,
        tenant_partitions: 1,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    };
    let store = DurableTelemetryStore::open(config.clone()).expect("store opens");
    store
        .push(
            "tenant-a",
            vec![
                LokiEntry {
                    timestamp_unix_nanos: 100,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: "cold request completed".to_owned(),
                    structured_metadata: BTreeMap::new(),
                },
                LokiEntry {
                    timestamp_unix_nanos: 200,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: "cold request failed".to_owned(),
                    structured_metadata: BTreeMap::from([("code".to_owned(), "500".to_owned())]),
                },
            ],
        )
        .expect("push");
    LokiStore::flush(&store, Duration::from_secs(30)).expect("object tier flushes");
    assert_eq!(store.operational_metrics().retained_payload_bytes, Some(0));
    assert_eq!(store.operational_metrics().source_reclaimed_offsets, 2);
    let log_partition = TopicPartition::new(LOKI_TOPIC_ID, LogicalPartitionId::new(0));
    assert_eq!(
        store
            .engine
            .watermarks(log_partition)
            .expect("log watermarks")
            .log_start,
        LogicalOffset::new(2)
    );
    let cold = store
        .query_native(&NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            terms: vec!["failed".to_owned()],
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            limit: 10,
            direction: NativeQueryDirection::OldestFirst,
        })
        .expect("cold query");
    assert_eq!(cold.len(), 1);
    assert_eq!(cold[0].line, "cold request failed");
    let mut count_request = AnalyticsScanRequest::new("tenant-a");
    count_request.columns = vec![crate::AnalyticsColumn::Offset];
    count_request.cardinality_only = true;
    let mut count = 0_u64;
    store
        .scan_analytics_cardinality(&count_request, &mut |batch_count| {
            count += batch_count;
            Ok(())
        })
        .expect("cold cardinality scan");
    assert_eq!(count, 2);
    let mut filtered_count_request = AnalyticsScanRequest::new("tenant-a");
    filtered_count_request.columns = vec![crate::AnalyticsColumn::Offset];
    filtered_count_request.cardinality_only = true;
    filtered_count_request
        .case_insensitive_message_tokens
        .push(Arc::from("failed"));
    filtered_count_request
        .labels
        .push(crate::MetadataField::new("app", "api"));
    let mut filtered_count = 0_u64;
    store
        .scan_analytics_cardinality(&filtered_count_request, &mut |batch_count| {
            filtered_count += batch_count;
            Ok(())
        })
        .expect("cold filtered cardinality scan");
    assert_eq!(filtered_count, 1);
    assert!(object_directory.exists());
    drop(store);

    let recovered = DurableTelemetryStore::open(config).expect("store recovers from tier root");
    let entries = recovered.entries("tenant-a").expect("recovered cold query");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].line, "cold request completed");
    assert_eq!(entries[1].structured_metadata["code"], "500");
    let mut recovered_count = 0_u64;
    recovered
        .scan_analytics_cardinality(&count_request, &mut |batch_count| {
            recovered_count += batch_count;
            Ok(())
        })
        .expect("recovered cold cardinality scan");
    assert_eq!(recovered_count, 2);
    let mut recovered_filtered_count = 0_u64;
    recovered
        .scan_analytics_cardinality(&filtered_count_request, &mut |batch_count| {
            recovered_filtered_count += batch_count;
            Ok(())
        })
        .expect("recovered cold filtered cardinality scan");
    assert_eq!(recovered_filtered_count, 1);
    drop(recovered);
    fs::remove_dir_all(directory).expect("remove test store");
}

#[test]
fn object_retention_removes_complete_signal_groups_without_scanning_storage() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-object-retention-{}-{nonce}",
        std::process::id()
    ));
    let config = DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: Some(directory.join("objects")),
        s3_object_store: None,
        recovery_journal: false,
        retention: None,
        shard_count: 1,
        tenant_partitions: 1,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    };
    let store = DurableTelemetryStore::open(config.clone()).expect("store opens");
    for (timestamp, line) in [(100, "expired"), (200, "retained")] {
        store
            .push(
                "tenant-a",
                vec![LokiEntry {
                    timestamp_unix_nanos: timestamp,
                    labels: BTreeMap::new(),
                    line: line.into(),
                    structured_metadata: BTreeMap::new(),
                }],
            )
            .expect("push");
        LokiStore::flush(&store, Duration::from_secs(30)).expect("group flushes");
    }
    let report = store
        .compact_retention_before(150)
        .expect("object retention publishes");
    assert_eq!(report.retired_object_groups, 1);
    assert!(report.retired_object_payload_bytes > 0);
    assert!(report.retired_object_keys >= 4);
    let matches = store
        .query_native(&NativeQuery {
            tenant: "tenant-a".into(),
            labels: BTreeMap::new(),
            terms: Vec::new(),
            start_timestamp_unix_nanos: Some(150),
            end_timestamp_unix_nanos: None,
            limit: 10,
            direction: NativeQueryDirection::OldestFirst,
        })
        .expect("retained query");
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].line, "retained");
    drop(store);

    let recovered = DurableTelemetryStore::open(config).expect("retained store reopens");
    let matches = recovered
        .query_native(&NativeQuery {
            tenant: "tenant-a".into(),
            labels: BTreeMap::new(),
            terms: Vec::new(),
            start_timestamp_unix_nanos: Some(150),
            end_timestamp_unix_nanos: None,
            limit: 10,
            direction: NativeQueryDirection::OldestFirst,
        })
        .expect("recovered retained query");
    assert_eq!(matches.len(), 1);
    drop(recovered);
    fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn logical_deletes_survive_restart_and_filter_native_and_analytical_reads() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-delete-store-{}-{nonce}",
        std::process::id()
    ));
    let config = DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: false,
        retention: None,
        shard_count: 2,
        tenant_partitions: 8,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    };
    let store = DurableTelemetryStore::open(config.clone()).expect("store");
    store
        .push(
            "tenant-a",
            (100..103)
                .map(|timestamp| LokiEntry {
                    timestamp_unix_nanos: timestamp,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: format!("durable message {timestamp}"),
                    structured_metadata: BTreeMap::new(),
                })
                .collect(),
        )
        .expect("push");
    let request_id = store
        .create_delete(
            "tenant-a",
            1,
            200,
            "{app=\"api\"} |= \"101\"".to_owned(),
            1_000,
        )
        .expect("create delete");
    assert_eq!(request_id, "0000000000000001");
    assert_eq!(
        store
            .entries("tenant-a")
            .expect("Loki entries")
            .into_iter()
            .map(|entry| entry.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        vec![100, 102]
    );
    assert_eq!(
        store
            .query_range("tenant-a", r#"{app="api"}"#, 1, 200, 10, false)
            .expect("Loki range entries")
            .entries
            .into_iter()
            .map(|entry| entry.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        vec![100, 102]
    );

    let native = store
        .query_native(&NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            terms: vec!["message".to_owned()],
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            limit: 10,
            direction: NativeQueryDirection::OldestFirst,
        })
        .expect("native query");
    assert_eq!(native.len(), 2);
    assert!(native.iter().all(|entry| !entry.line.ends_with("101")));
    let bounded = store
        .query_native_page(&NativeLogPageQuery {
            query: NativeQuery {
                tenant: "tenant-a".to_owned(),
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                terms: vec!["message".to_owned()],
                start_timestamp_unix_nanos: None,
                end_timestamp_unix_nanos: None,
                limit: 10,
                direction: NativeQueryDirection::OldestFirst,
            },
            max_bytes: 1_024,
            cursor: None,
        })
        .expect("bounded native query respects deletes");
    assert_eq!(bounded.entries, native);


    let mut rows = Vec::new();
    store
        .scan_analytics(&AnalyticsScanRequest::new("tenant-a"), &mut |batch| {
            rows.extend_from_slice(batch);
            Ok(())
        })
        .expect("analytics scan");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| {
        row.message
            .as_deref()
            .is_none_or(|message| !message.ends_with("101"))
    }));
    drop(store);

    let recovered = DurableTelemetryStore::open(config).expect("recovered store");
    assert_eq!(recovered.delete_requests("tenant-a").unwrap().len(), 1);
    assert_eq!(recovered.entries("tenant-a").unwrap().len(), 2);
    assert!(recovered.cancel_delete("tenant-a", &request_id).unwrap());
    assert_eq!(recovered.entries("tenant-a").unwrap().len(), 3);
    drop(recovered);
    fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn native_log_pages_bound_bytes_and_keep_equal_timestamp_cursor_order() {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    let directory = std::env::temp_dir().join(format!("shard-telemetry-native-pages-{}-{nonce}", std::process::id()));
    let store = DurableTelemetryStore::open(DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: false,
        retention: None,
        shard_count: 2,
        tenant_partitions: 2,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    })
    .expect("store opens");
    for line in ["first", "second", "third"] {
        store
            .push(
                "tenant-a",
                vec![LokiEntry {
                    timestamp_unix_nanos: 100,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: line.to_owned(),
                    structured_metadata: BTreeMap::new(),
                }],
            )
            .expect("push");
    }
    let mut request = NativeLogPageQuery {
        query: NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            terms: Vec::new(),
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            limit: 10,
            direction: NativeQueryDirection::OldestFirst,
        },
        max_bytes: 128,
        cursor: None,
    };
    assert_eq!(store.tenant_partitions("tenant-a").expect("partitions").len(), 2);
    let full = store.query_native_page(&request).expect("complete page");
    assert_eq!(full.entries.len(), 3);
    request.max_bytes = 12;
    let mut paged = Vec::new();
    let mut first_cursor = None;
    for _ in 0..4 {
        let page = store.query_native_page(&request).expect("bounded page");
        assert!(page.entries.iter().map(crate::native_log_entry_bytes).sum::<usize>() <= 12);
        paged.extend(page.entries);
        match page.next_cursor {
            Some(cursor) => {
                if first_cursor.is_none() {
                    first_cursor = Some(cursor.clone());
                }
                request.cursor = Some(cursor);
            }
            None => break,
        }
    }
    assert_eq!(paged, full.entries, "equal timestamps cross partitions without gaps or duplicates");
    let cursor = first_cursor.expect("first continuation");

    request.cursor = None;
    request.query.direction = NativeQueryDirection::NewestFirst;
    request.max_bytes = 128;
    let newest = store.query_native_page(&request).expect("newest page");
    assert_eq!(newest.entries.iter().rev().collect::<Vec<_>>(), full.entries.iter().collect::<Vec<_>>());
    request.query.direction = NativeQueryDirection::OldestFirst;

    request.cursor = Some(cursor);
    request.query.tenant = "tenant-b".to_owned();
    assert!(store.query_native_page(&request).is_err(), "cursor cannot cross tenants");
    request.query.tenant = "tenant-a".to_owned();
    request.query.labels.insert("app".to_owned(), "other".to_owned());
    assert!(store.query_native_page(&request).is_err(), "cursor cannot cross filters");
    request.cursor = None;
    request.query.labels.insert("app".to_owned(), "api".to_owned());
    request.max_bytes = 1;
    assert!(store.query_native_page(&request).is_err(), "oversized first row fails explicitly");
    drop(store);
    fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn native_query_merges_owner_local_top_k_across_partitions() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-native-top-k-{}-{nonce}",
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
    })
    .expect("store opens");

    for timestamp in 0..8 {
        store
            .push(
                "tenant-a",
                vec![LokiEntry {
                    timestamp_unix_nanos: timestamp,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: format!("request {timestamp}"),
                    structured_metadata: BTreeMap::new(),
                }],
            )
            .expect("push");
    }

    let oldest = store
        .query_native(&NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            terms: Vec::new(),
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            limit: 3,
            direction: NativeQueryDirection::OldestFirst,
        })
        .expect("oldest query");
    assert_eq!(
        oldest
            .iter()
            .map(|entry| entry.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );

    let newest = store
        .query_native(&NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            terms: Vec::new(),
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            limit: 3,
            direction: NativeQueryDirection::NewestFirst,
        })
        .expect("newest query");
    assert_eq!(
        newest
            .iter()
            .map(|entry| entry.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        [7, 6, 5]
    );

    drop(store);
    fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn retention_cutoff_is_enforced_by_loki_native_and_analytical_reads() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-retention-store-{}-{nonce}",
        std::process::id()
    ));
    let now = i64::try_from(nonce).expect("current timestamp fits i64");
    let config = DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: true,
        retention: Some(Duration::from_secs(60)),
        shard_count: 1,
        tenant_partitions: 1,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    };
    let store = DurableTelemetryStore::open(config.clone()).expect("store");
    store
        .push(
            "tenant-a",
            vec![LokiEntry {
                timestamp_unix_nanos: now - 120_000_000_000,
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                line: "expired message".to_owned(),
                structured_metadata: BTreeMap::new(),
            }],
        )
        .expect("push expired batch");
    store
        .push(
            "tenant-a",
            vec![LokiEntry {
                timestamp_unix_nanos: now,
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                line: "retained message".to_owned(),
                structured_metadata: BTreeMap::new(),
            }],
        )
        .expect("push retained batch");
    let entries = store.entries("tenant-a").expect("Loki query");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].line, "retained message");

    let native = store
        .query_native(&NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::new(),
            terms: vec!["message".to_owned()],
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            limit: 10,
            direction: NativeQueryDirection::OldestFirst,
        })
        .expect("native query");
    assert_eq!(native.len(), 1);
    assert_eq!(native[0].line, "retained message");

    let mut rows = Vec::new();
    store
        .scan_analytics(&AnalyticsScanRequest::new("tenant-a"), &mut |batch| {
            rows.extend_from_slice(batch);
            Ok(())
        })
        .expect("analytics scan");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].message.as_deref(), Some("retained message"));
    let report = store.compact_retention().expect("retention compaction");
    assert_eq!(report.advanced_partitions, 1);
    assert_eq!(report.advanced_offsets, 1);
    assert_eq!(store.operational_metrics().retention_runs, 1);
    drop(store);
    let recovered = DurableTelemetryStore::open(config).expect("restart after compaction");
    assert_eq!(recovered.entries("tenant-a").unwrap().len(), 1);
    drop(recovered);
    fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn standalone_store_makes_the_stel_envelope_authoritative() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-ingest-pack-{}-{nonce}",
        std::process::id()
    ));
    let store = DurableTelemetryStore::open(DurableTelemetryConfig {
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
    })
    .expect("store opens");
    let entry = LokiEntry {
        timestamp_unix_nanos: 123,
        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
        line: "request completed".to_owned(),
        structured_metadata: BTreeMap::from([("trace_id".to_owned(), "abc".to_owned())]),
    };
    store
        .push("tenant-a", vec![entry])
        .expect("Loki push succeeds");
    let batches = store
        .engine
        .fetch(FetchRequest {
            request_id: 1,
            topic_id: LOKI_TOPIC_ID,
            partition_id: LogicalPartitionId::new(0),
            start_offset: LogicalOffset::new(0),
            max_bytes: 1024 * 1024,
            mode: FetchMode::Ordered,
        })
        .expect("authoritative batch fetches");
    assert_eq!(batches.len(), 1);
    let envelope = crate::TelemetryEnvelope::decode(&batches[0].payload)
        .expect("stored STEL envelope validates");
    assert_eq!(envelope.signal, crate::TelemetrySignal::Logs);
    validate_ingest_pack(&envelope.payload, 1).expect("stored pack validates");
    let decoded = decode_ingest_pack(&envelope.payload).expect("stored pack decodes");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].timestamp_unix_nanos, 123);
    assert_eq!(decoded[0].message.as_ref(), "request completed");
    assert!(decoded[0].fields.iter().any(|field| {
        field.key.as_ref() == "resource.loki.tenant" && field.value.as_ref() == "tenant-a"
    }));
    drop(store);
    fs::remove_dir_all(directory).expect("remove test store");
}

#[test]
fn durable_analytics_scan_pushes_indexable_constraints_into_stripes() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-analytics-store-{}-{nonce}",
        std::process::id()
    ));
    let store = DurableTelemetryStore::open(DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: false,
        retention: None,
        shard_count: 2,
        tenant_partitions: 8,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    })
    .expect("store opens");
    store
        .push(
            "tenant-a",
            vec![
                LokiEntry {
                    timestamp_unix_nanos: 100,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: "request completed".to_owned(),
                    structured_metadata: BTreeMap::from([("code".to_owned(), "200".to_owned())]),
                },
                LokiEntry {
                    timestamp_unix_nanos: 200,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: "request ERROR".to_owned(),
                    structured_metadata: BTreeMap::from([("code".to_owned(), "500".to_owned())]),
                },
            ],
        )
        .expect("push");
    store
        .push(
            "tenant-a",
            vec![LokiEntry {
                timestamp_unix_nanos: 300,
                labels: BTreeMap::from([("app".to_owned(), "worker".to_owned())]),
                line: "newest request".to_owned(),
                structured_metadata: BTreeMap::new(),
            }],
        )
        .expect("second partition push");
    let mut request = AnalyticsScanRequest::new("tenant-a");
    request.start_timestamp_unix_nanos = Some(150);
    request.end_timestamp_unix_nanos = Some(250);
    request.terms.push(Arc::from("error"));
    request.labels.push(crate::MetadataField::new("app", "api"));
    request
        .metadata
        .push(crate::MetadataField::new("code", "500"));
    let mut rows = Vec::new();
    store
        .scan_analytics(&request, &mut |batch| {
            rows.extend_from_slice(batch);
            Ok(())
        })
        .expect("scan");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].timestamp_unix_nanos, 200);
    assert_eq!(rows[0].message.as_deref(), Some("request ERROR"));
    assert_eq!(rows[0].labels["app"], "api");
    assert_eq!(rows[0].metadata["code"], "500");
    let mut filtered_count_request = request.clone();
    filtered_count_request.columns = vec![crate::AnalyticsColumn::Offset];
    filtered_count_request.cardinality_only = true;
    let mut filtered_count = 0_u64;
    store
        .scan_analytics_cardinality(&filtered_count_request, &mut |batch_count| {
            filtered_count += batch_count;
            Ok(())
        })
        .expect("filtered cardinality scan");
    assert_eq!(filtered_count, 1);
    let mut newest = AnalyticsScanRequest::new("tenant-a");
    newest.limit = Some(1);
    newest.order = Some(AnalyticsScanOrder::TimestampDescending);
    let mut newest_rows = Vec::new();
    store
        .scan_analytics(&newest, &mut |batch| {
            newest_rows.extend_from_slice(batch);
            Ok(())
        })
        .expect("newest scan");
    assert_eq!(newest_rows.len(), 1);
    assert_eq!(newest_rows[0].timestamp_unix_nanos, 300);
    assert_eq!(newest_rows[0].message.as_deref(), Some("newest request"));
    newest.columns = vec![
        crate::AnalyticsColumn::Timestamp,
        crate::AnalyticsColumn::Message,
    ];
    newest_rows.clear();
    store
        .scan_analytics(&newest, &mut |batch| {
            newest_rows.extend_from_slice(batch);
            Ok(())
        })
        .expect("projected newest scan");
    assert_eq!(newest_rows.len(), 1);
    assert_eq!(newest_rows[0].message.as_deref(), Some("newest request"));
    assert!(newest_rows[0].metadata.is_empty());
    assert!(newest_rows[0].body_json.is_none());
    let mut newest_typed = newest.clone();
    newest_typed.columns = vec![
        crate::AnalyticsColumn::Timestamp,
        crate::AnalyticsColumn::Message,
        crate::AnalyticsColumn::BodyJson,
    ];
    newest_rows.clear();
    store
        .scan_analytics(&newest_typed, &mut |batch| {
            newest_rows.extend_from_slice(batch);
            Ok(())
        })
        .expect("typed projected newest scan");
    assert_eq!(newest_rows.len(), 1);
    assert!(newest_rows[0].body_json.is_some());
    let mut newest_exact_token = AnalyticsScanRequest::new("tenant-a");
    newest_exact_token.limit = Some(1);
    newest_exact_token.order = Some(AnalyticsScanOrder::TimestampDescending);
    newest_exact_token.message_tokens.push(Arc::from("ERROR"));
    let mut exact_rows = Vec::new();
    store
        .scan_analytics(&newest_exact_token, &mut |batch| {
            exact_rows.extend_from_slice(batch);
            Ok(())
        })
        .expect("newest exact-token scan");
    assert_eq!(exact_rows.len(), 1);
    assert_eq!(exact_rows[0].timestamp_unix_nanos, 200);
    assert_eq!(exact_rows[0].message.as_deref(), Some("request ERROR"));
    let mut newest_folded_token = AnalyticsScanRequest::new("tenant-a");
    newest_folded_token.limit = Some(1);
    newest_folded_token.order = Some(AnalyticsScanOrder::TimestampDescending);
    newest_folded_token
        .case_insensitive_message_tokens
        .push(Arc::from("error"));
    let mut folded_rows = Vec::new();
    store
        .scan_analytics(&newest_folded_token, &mut |batch| {
            folded_rows.extend_from_slice(batch);
            Ok(())
        })
        .expect("newest case-insensitive token scan");
    assert_eq!(folded_rows.len(), 1);
    assert_eq!(folded_rows[0].timestamp_unix_nanos, 200);
    drop(store);
    fs::remove_dir_all(directory).expect("remove test store");
}

#[test]
fn durable_analytics_ordered_scan_with_resource_filter_ranks_across_partitions() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-ordered-resource-scan-{}-{nonce}",
        std::process::id()
    ));
    let store = DurableTelemetryStore::open(DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: false,
        retention: None,
        shard_count: 2,
        tenant_partitions: 2,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    })
    .expect("store opens");
    let resource = Arc::new(crate::ResourceContext {
        attributes: Arc::new(vec![crate::TelemetryAttribute::new(
            "service.name",
            crate::TelemetryValue::String(Arc::from("checkout")),
        )]),
        ..crate::ResourceContext::default()
    });
    let partitions = [
        TopicPartition::new(LOKI_TOPIC_ID, LogicalPartitionId::new(0)),
        TopicPartition::new(LOKI_TOPIC_ID, LogicalPartitionId::new(1)),
    ];
    let events = [(100, "early"), (300, "late")]
        .into_iter()
        .map(|(timestamp, message)| crate::OtlpLogEvent {
            timestamp_unix_nanos: timestamp,
            body: Some(crate::TelemetryValue::String(Arc::from(message))),
            message: Arc::from(message),
            fields: Arc::new(vec![crate::MetadataField::new(
                "resource.service.name",
                "checkout",
            )]),
            resource: Arc::clone(&resource),
            ..crate::OtlpLogEvent::default()
        })
        .collect::<Vec<_>>();
    let batch = crate::NativeTelemetryBatch {
        partitions: partitions
            .into_iter()
            .zip(events)
            .map(|(topic_partition, event)| crate::NativePartitionAppend {
                topic_partition,
                envelope: crate::prepare_log_envelope("tenant-a", &[event]).expect("envelope"),
                transient_context: None,
            })
            .collect(),
    };
    store.append_telemetry_batch(&batch, true).expect("append");

    let mut request = AnalyticsScanRequest::new("tenant-a");
    request
        .resource_attributes
        .push(crate::MetadataField::new("service.name", "checkout"));
    request.columns = vec![
        crate::AnalyticsColumn::Timestamp,
        crate::AnalyticsColumn::Message,
    ];
    request.limit = Some(1);
    for (order, expected) in [
        (AnalyticsScanOrder::TimestampDescending, "late"),
        (AnalyticsScanOrder::TimestampAscending, "early"),
    ] {
        request.order = Some(order);
        let mut rows = Vec::new();
        store
            .scan_analytics(&request, &mut |batch| {
                rows.extend_from_slice(batch);
                Ok(())
            })
            .expect("ordered scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].message.as_deref(), Some(expected));
    }
    request.limit = Some(2);
    request.order = Some(AnalyticsScanOrder::TimestampDescending);
    let mut rows = Vec::new();
    store
        .scan_analytics(&request, &mut |batch| {
            rows.extend_from_slice(batch);
            Ok(())
        })
        .expect("ordered scan below limit");
    assert_eq!(
        rows.iter()
            .map(|row| row.message.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("late"), Some("early")]
    );
    drop(store);
    fs::remove_dir_all(directory).expect("cleanup");
}
