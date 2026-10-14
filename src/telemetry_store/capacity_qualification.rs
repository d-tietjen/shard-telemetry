//! Real-store capacity regressions for exact fallback navigation and admission.

use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, span::Link},
};
use prost::Message;
use shard_stream_core::ShardId;

use super::*;
use crate::{
    CorrelationConfig, CorrelationQuery, DurableMetricPoint, EmbeddedTelemetryConfig,
    MetadataField, MetricExemplar, MetricIdentity, MetricKind, MetricValue, NumberValue,
    OtlpLogEvent, OtlpTelemetryDecoder, ResourceContext, ScopeContext, TelemetryAttribute,
    TelemetryRecordRef, TelemetrySignal, TelemetryValue, TraceId,
};

#[test]
fn disabled_navigation_preserves_typed_correlations_through_retention_and_cold_restart() {
    let now = unix_nanos_now();
    let trace = TraceId::from_bytes([1; 16]).expect("trace ID");
    let integer = TelemetryAttribute::new("typed", TelemetryValue::Integer(42));
    let string = TelemetryAttribute::new("typed", TelemetryValue::String(Arc::from("42")));
    let scope_attribute =
        TelemetryAttribute::new("scope.tag", TelemetryValue::String(Arc::from("scoped")));
    let resource = Arc::new(ResourceContext {
        attributes: Arc::new(vec![TelemetryAttribute::new(
            "application",
            TelemetryValue::String(Arc::from("shared")),
        )]),
        ..ResourceContext::default()
    });
    let scope = Arc::new(ScopeContext {
        name: Arc::from("qualification"),
        version: Arc::from("1"),
        attributes: Arc::new(vec![scope_attribute.clone()]),
        ..ScopeContext::default()
    });
    for ram in [8 * 1024 * 1024, 64 * 1024 * 1024] {
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-disabled-navigation-{}-{now}-{ram}",
            std::process::id(),
        ));
        let mut bounded =
            EmbeddedTelemetryConfig::bounded_local(directory.clone(), Duration::from_secs(3_600))
                .with_max_ram_bytes(ram);
        bounded.store.append_linger = Duration::ZERO;
        bounded.object_tier.target_group_payload_bytes = 1;
        assert_eq!(
            bounded.local_limits.correlations,
            CorrelationConfig::disabled()
        );
        let config = bounded.store;
        let tier = bounded.object_tier;
        let limits = bounded.local_limits;
        let store = DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
            config.clone(),
            tier,
            limits.clone(),
        )
        .expect("store");
        assert_eq!(store.telemetry_partition_count(), 1);
        for tenant in ["tenant-a", "tenant-b"] {
            store
                .append_log_events(
                    tenant,
                    vec![navigation_log(
                        now - 7_200_000_000_000,
                        trace,
                        &integer,
                        &resource,
                        &scope,
                    )],
                    true,
                )
                .expect("expired log");
            append_navigation_spans(&store, tenant, trace, now, true);
            store
                .append_metric_point(
                    navigation_metric(
                        tenant,
                        now - 7_200_000_000_000,
                        trace,
                        &integer,
                        &resource,
                        &scope,
                    ),
                    true,
                )
                .expect("expired metric");
        }
        // Expired records own separate published groups and cannot consume a
        // result limit or prevent later physical retention of their objects.
        store.flush(Duration::from_secs(30)).expect("expired flush");
        let mut expected = BTreeMap::<&str, Vec<TelemetryRecordRef>>::new();
        let mut string_refs = BTreeMap::<&str, Vec<TelemetryRecordRef>>::new();
        for tenant in ["tenant-a", "tenant-b"] {
            let distractors = (0..70)
                .map(|_| navigation_log(now - 10_000_000_000, trace, &string, &resource, &scope))
                .collect();
            let ack = store
                .append_log_events(tenant, distractors, true)
                .expect("string logs");
            string_refs.insert(tenant, acknowledgement_refs(&ack));
            let ack = store
                .append_log_events(
                    tenant,
                    vec![
                        navigation_log(now - 10_000_000_000, trace, &integer, &resource, &scope,);
                        2
                    ],
                    true,
                )
                .expect("integer logs");
            let mut retained = acknowledgement_refs(&ack);
            retained.extend(append_navigation_spans(&store, tenant, trace, now, false));
            let ack = store
                .append_metric_point(
                    navigation_metric(
                        tenant,
                        now - 10_000_000_000,
                        trace,
                        &integer,
                        &resource,
                        &scope,
                    ),
                    true,
                )
                .expect("retained metric");
            retained.extend(acknowledgement_refs(&ack));
            retained.sort_unstable();
            expected.insert(tenant, retained);
        }
        let queries = [
            CorrelationQuery::new("tenant-a").with_attribute(&integer),
            CorrelationQuery::new("tenant-a")
                .with_trace_id(trace)
                .with_attribute(&integer),
            CorrelationQuery::new("tenant-a")
                .with_resource_id(resource.id())
                .with_attribute(&integer),
            CorrelationQuery::new("tenant-a")
                .with_scope_id(scope.id())
                .with_attribute(&integer),
            CorrelationQuery::new("tenant-a")
                .with_trace_id(trace)
                .with_label("application", "shared")
                .with_attribute(&integer),
            CorrelationQuery::new("tenant-a")
                .with_attribute(&scope_attribute)
                .with_attribute(&integer),
        ];
        assert_navigation_results(&store, &queries, &expected, &string, &string_refs, now);
        store
            .flush(Duration::from_secs(30))
            .expect("retained flush");
        assert_navigation_results(&store, &queries, &expected, &string, &string_refs, now);
        drop(store);
        let recovered = DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
            config.clone(),
            tier,
            limits.clone(),
        )
        .expect("cold restart");
        assert_eq!(
            recovered
                .service
                .retained_payload_bytes()
                .expect("resident payload"),
            0
        );
        let before = recovered
            .service
            .object_tier_cache_stats()
            .expect("cold cache");
        assert_navigation_results(&recovered, &queries, &expected, &string, &string_refs, now);
        let after = recovered
            .service
            .object_tier_cache_stats()
            .expect("queried cold cache");
        assert!(
            after.payload.hits + after.payload.misses > before.payload.hits + before.payload.misses
        );
        let report = recovered.compact_retention().expect("retention");
        assert!(report.retired_object_groups > 0);
        assert_navigation_results(&recovered, &queries, &expected, &string, &string_refs, now);
        drop(recovered);
        let maintained = DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
            config, tier, limits,
        )
        .expect("restart after retention");
        assert_navigation_results(&maintained, &queries, &expected, &string, &string_refs, now);
        drop(maintained);
        fs::remove_dir_all(directory).expect("remove fixture");
    }
}

fn navigation_log(
    timestamp: u64,
    trace: TraceId,
    attribute: &TelemetryAttribute,
    resource: &Arc<ResourceContext>,
    scope: &Arc<ScopeContext>,
) -> OtlpLogEvent {
    OtlpLogEvent {
        timestamp_unix_nanos: timestamp,
        message: Arc::from("shared typed correlation"),
        attributes: Arc::new(vec![attribute.clone()]),
        resource: Arc::clone(resource),
        scope: Arc::clone(scope),
        trace_id: Some(trace),
        fields: Arc::new(vec![MetadataField::new(
            "resource.loki.label.application",
            "shared",
        )]),
        ..OtlpLogEvent::default()
    }
}

fn append_navigation_spans(
    store: &DurableTelemetryStore,
    tenant: &str,
    trace: TraceId,
    now: u64,
    expired: bool,
) -> Vec<TelemetryRecordRef> {
    let integer = KeyValue {
        key: "typed".to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(42)),
        }),
        ..KeyValue::default()
    };
    let spans = if expired {
        vec![Span {
            trace_id: trace.as_bytes().to_vec(),
            span_id: vec![1; 8],
            name: "expired".to_owned(),
            start_time_unix_nano: now - 7_200_000_000_000,
            end_time_unix_nano: now - 7_199_000_000_000,
            attributes: vec![integer],
            ..Span::default()
        }]
    } else {
        vec![
            Span {
                trace_id: trace.as_bytes().to_vec(),
                span_id: vec![2; 8],
                name: "crossing".to_owned(),
                start_time_unix_nano: now - 5_400_000_000_000,
                end_time_unix_nano: now - 10_000_000_000,
                attributes: vec![integer.clone()],
                ..Span::default()
            },
            Span {
                trace_id: vec![9; 16],
                span_id: vec![3; 8],
                name: "linked".to_owned(),
                start_time_unix_nano: now - 10_000_000_000,
                end_time_unix_nano: now - 9_000_000_000,
                links: vec![Link {
                    trace_id: trace.as_bytes().to_vec(),
                    span_id: vec![2; 8],
                    attributes: vec![integer],
                    ..Link::default()
                }],
                ..Span::default()
            },
        ]
    };
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "application".to_owned(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("shared".to_owned())),
                    }),
                    ..KeyValue::default()
                }],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "qualification".to_owned(),
                    version: "1".to_owned(),
                    attributes: vec![KeyValue {
                        key: "scope.tag".to_owned(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("scoped".to_owned())),
                        }),
                        ..KeyValue::default()
                    }],
                    ..InstrumentationScope::default()
                }),
                spans,
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    };
    let events = OtlpTelemetryDecoder
        .decode_traces(tenant, &request.encode_to_vec())
        .expect("decode spans");
    acknowledgement_refs(
        &store
            .append_trace_events(events, true)
            .expect("append spans"),
    )
}

fn navigation_metric(
    tenant: &str,
    timestamp: u64,
    trace: TraceId,
    attribute: &TelemetryAttribute,
    resource: &Arc<ResourceContext>,
    scope: &Arc<ScopeContext>,
) -> DurableMetricPoint {
    DurableMetricPoint {
        stream_shard_id: ShardId::new(0),
        record_ref: TelemetryRecordRef::for_signal(
            TelemetrySignal::Metrics,
            TopicPartition::new(crate::METRICS_TOPIC_ID, LogicalPartitionId::new(0)),
            LogicalOffset::new(0),
        ),
        identity: Arc::new(MetricIdentity {
            tenant: Arc::from(tenant),
            resource: Arc::clone(resource),
            scope: Arc::clone(scope),
            name: Arc::from("navigation"),
            unit: Arc::from("1"),
            kind: MetricKind::Gauge,
            point_attributes: Arc::new(Vec::new()),
        }),
        description: Arc::from(""),
        metadata: Arc::new(Vec::new()),
        start_time_unix_nanos: 0,
        timestamp_unix_nanos: timestamp,
        flags: 0,
        value: MetricValue::Gauge(NumberValue::Integer(1)),
        exemplars: Arc::new(vec![MetricExemplar {
            filtered_attributes: Arc::new(vec![attribute.clone()]),
            timestamp_unix_nanos: timestamp,
            value: NumberValue::Integer(1),
            span_id: None,
            trace_id: Some(trace),
        }]),
    }
}

fn acknowledgement_refs(ack: &crate::NativeTelemetryAppendAck) -> Vec<TelemetryRecordRef> {
    ack.partitions
        .iter()
        .flat_map(|partition| {
            (partition.first_offset..=partition.last_offset).map(|offset| {
                TelemetryRecordRef::for_signal(
                    [
                        TelemetrySignal::Logs,
                        TelemetrySignal::Traces,
                        TelemetrySignal::Metrics,
                    ]
                    .into_iter()
                    .find(|signal| signal.topic_id() == partition.topic_partition.topic_id)
                    .expect("signal"),
                    partition.topic_partition,
                    LogicalOffset::new(offset),
                )
            })
        })
        .collect()
}

fn assert_navigation_results(
    store: &DurableTelemetryStore,
    queries: &[CorrelationQuery],
    expected: &BTreeMap<&str, Vec<TelemetryRecordRef>>,
    string: &TelemetryAttribute,
    string_refs: &BTreeMap<&str, Vec<TelemetryRecordRef>>,
    now: u64,
) {
    for tenant in ["tenant-a", "tenant-b"] {
        let mut all = expected[tenant].clone();
        all.extend_from_slice(&string_refs[tenant]);
        all.sort_unstable();
        for original in queries.iter().skip(1) {
            let mut query = CorrelationQuery {
                tenant: Arc::from(tenant),
                ..original.clone()
            };
            // Remove only the integer constraint; trace, context and label
            // lookups must preserve the complete mixed-type result as well.
            query.attributes = Arc::new(
                query
                    .attributes
                    .iter()
                    .copied()
                    .filter(|fingerprint| *fingerprint != queries[0].attributes[0])
                    .collect(),
            );
            assert_eq!(
                store
                    .query_correlations(&query)
                    .expect("complete navigation"),
                all
            );
            let first = query.with_limit(2);
            assert_eq!(
                store
                    .query_correlations(&first)
                    .expect("label or context first page"),
                all[..2]
            );
            assert_eq!(
                store
                    .query_correlations(&first.after(all[1]))
                    .expect("label or context second page"),
                all[2..4]
            );
        }
        for original in queries {
            let query = CorrelationQuery {
                tenant: Arc::from(tenant),
                ..original.clone()
            };
            let records = &expected[tenant];
            assert_eq!(
                store
                    .query_correlations(&query)
                    .expect("exact typed result"),
                *records
            );
            let mut page = query.clone().with_limit(1);
            for record in records {
                assert_eq!(
                    store.query_correlations(&page).expect("cursor page"),
                    vec![*record]
                );
                page = page.after(*record);
            }
            assert!(
                store
                    .query_correlations(&page)
                    .expect("cursor end")
                    .is_empty()
            );
            for signal in [
                TelemetrySignal::Logs,
                TelemetrySignal::Traces,
                TelemetrySignal::Metrics,
            ] {
                let selected = records
                    .iter()
                    .copied()
                    .filter(|record| record.signal == signal)
                    .collect::<Vec<_>>();
                assert_eq!(
                    store
                        .query_correlations(&query.clone().for_signal(signal))
                        .expect("signal restriction"),
                    selected
                );
            }
            let mut historical = query.clone().for_signal(TelemetrySignal::Traces);
            historical.end_time_unix_nanos = Some(now - 4_800_000_000_000);
            assert_eq!(
                store
                    .query_correlations(&historical)
                    .expect("retained crossing span"),
                vec![
                    *records
                        .iter()
                        .find(|record| record.signal == TelemetrySignal::Traces)
                        .expect("crossing ref")
                ]
            );
            for absent in ["tenant-absent", ""] {
                assert!(
                    store
                        .query_correlations(&CorrelationQuery {
                            tenant: Arc::from(absent),
                            ..query.clone()
                        })
                        .expect("foreign tenant")
                        .is_empty()
                );
            }
        }
        assert_eq!(
            store
                .query_correlations(
                    &CorrelationQuery::new(tenant)
                        .with_attribute(string)
                        .for_signal(TelemetrySignal::Logs)
                )
                .expect("string identity"),
            string_refs[tenant]
        );
    }
}

#[test]
fn field_only_log_correlations_preserve_inclusive_time_bounds_through_cold_restart() {
    let now = unix_nanos_now();
    let timestamp = now - 10_000_000_000;
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-field-boundary-{}-{now}",
        std::process::id(),
    ));
    let mut bounded =
        EmbeddedTelemetryConfig::bounded_local(directory.clone(), Duration::from_secs(3_600))
            .with_max_ram_bytes(8 * 1024 * 1024);
    bounded.store.append_linger = Duration::ZERO;
    bounded.object_tier.target_group_payload_bytes = 1;
    let config = bounded.store;
    let tier = bounded.object_tier;
    let limits = bounded.local_limits;
    let store = DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
        config.clone(),
        tier,
        limits.clone(),
    )
    .expect("store");
    let events = [timestamp, timestamp + 1]
        .into_iter()
        .map(|timestamp_unix_nanos| OtlpLogEvent {
            timestamp_unix_nanos,
            message: Arc::from("field-only boundary"),
            fields: Arc::new(vec![MetadataField::new("legacy.label", "value")]),
            ..OtlpLogEvent::default()
        })
        .collect();
    let ack = store
        .append_log_events("tenant-a", events, true)
        .expect("field-only logs");
    let records = acknowledgement_refs(&ack);
    assert_eq!(records.len(), 2);
    let mut boundary = CorrelationQuery::new("tenant-a")
        .with_label("legacy.label", "value")
        .for_signal(TelemetrySignal::Logs);
    boundary.start_time_unix_nanos = Some(timestamp);
    boundary.end_time_unix_nanos = Some(timestamp);
    let verify = |store: &DurableTelemetryStore| {
        assert_eq!(
            store
                .query_correlations(&boundary)
                .expect("inclusive boundary"),
            vec![records[0]]
        );
        assert!(
            store
                .query_correlations(&boundary.clone().after(records[0]))
                .expect("boundary cursor end")
                .is_empty()
        );
        let mut maximum = boundary.clone();
        maximum.end_time_unix_nanos = Some(u64::MAX);
        assert_eq!(
            store
                .query_correlations(&maximum)
                .expect("maximum upper bound"),
            records
        );
    };
    verify(&store);
    store
        .flush(Duration::from_secs(30))
        .expect("publication flush");
    verify(&store);
    drop(store);
    let recovered =
        DurableTelemetryStore::open_with_object_tier_config_and_local_limits(config, tier, limits)
            .expect("cold restart");
    assert_eq!(
        recovered
            .service
            .retained_payload_bytes()
            .expect("resident payload"),
        0
    );
    verify(&recovered);
    drop(recovered);
    fs::remove_dir_all(directory).expect("remove fixture");
}

#[test]
fn oversized_envelopes_and_calls_are_bad_requests_before_any_partition_mutates() {
    let nonce = unix_nanos_now();
    let valid = OtlpLogEvent {
        timestamp_unix_nanos: nonce,
        message: Arc::from("valid"),
        ..OtlpLogEvent::default()
    };
    let large = OtlpLogEvent {
        message: Arc::from(
            (0..16_384)
                .map(|index| format!("{index:08x}"))
                .collect::<String>(),
        ),
        ..valid.clone()
    };
    let small = crate::prepare_log_envelope("tenant-a", std::slice::from_ref(&valid))
        .expect("small envelope");
    let big = crate::prepare_log_envelope("tenant-a", std::slice::from_ref(&large))
        .expect("large envelope");
    let small_bytes = small.encode().expect("small wire").len();
    let big_bytes = big.encode().expect("large wire").len();
    assert!(big_bytes > small_bytes * 2);
    let partitions = [0, 1].map(|partition| {
        TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(partition))
    });
    for engine_limit in [true, false] {
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-append-cap-{}-{nonce}-{engine_limit}",
            std::process::id(),
        ));
        let mut bounded = EmbeddedTelemetryConfig::new(DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 1,
            tenant_partitions: 2,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        });
        bounded.local_limits.max_batch_bytes = if engine_limit { small_bytes } else { big_bytes };
        if !engine_limit {
            bounded.local_limits.signals.max_otlp_request_bytes = small_bytes * 2 - 1;
        }
        let store =
            DurableTelemetryStore::open_with_local_limits(bounded.store, bounded.local_limits)
                .expect("store");
        assert_eq!(
            store.maximum_append_bytes(),
            if engine_limit {
                small_bytes
            } else {
                small_bytes * 2 - 1
            }
        );
        let batch = crate::NativeTelemetryBatch {
            partitions: vec![
                crate::NativePartitionAppend {
                    topic_partition: partitions[0],
                    envelope: small.clone(),
                    transient_context: None,
                },
                crate::NativePartitionAppend {
                    topic_partition: partitions[1],
                    envelope: if engine_limit {
                        big.clone()
                    } else {
                        small.clone()
                    },
                    transient_context: None,
                },
            ],
        };
        assert!(
            store
                .append_telemetry_batch(&batch, true)
                .expect_err("oversized grouped call")
                .is_bad_request()
        );
        assert!(
            store
                .append_log_events("tenant-a", vec![large.clone()], true)
                .expect_err("oversized direct log")
                .is_bad_request()
        );
        for partition in partitions {
            assert!(
                store
                    .fetch_telemetry_batches(partition, LogicalOffset::new(0), 1024 * 1024)
                    .expect("unmutated WAL")
                    .is_empty()
            );
        }
        let rejected_retry = crate::NativeTelemetryBatch {
            partitions: vec![crate::NativePartitionAppend {
                topic_partition: partitions[0],
                envelope: big.clone(),
                transient_context: None,
            }],
        };
        assert!(
            store
                .append_telemetry_batch_with_retry_id(&rejected_retry, true, 7)
                .expect_err("oversized retry")
                .is_bad_request()
        );
        let accepted = crate::NativeTelemetryBatch {
            partitions: vec![crate::NativePartitionAppend {
                topic_partition: partitions[0],
                envelope: small.clone(),
                transient_context: None,
            }],
        };
        let ack = store
            .append_telemetry_batch_with_retry_id(&accepted, true, 7)
            .expect("valid reuse of unreserved retry ID");
        assert_eq!(ack.partitions[0].first_offset, 0);
        assert_eq!(ack.partitions[0].last_offset, 0);
        let ack = store
            .append_log_events("tenant-a", vec![valid.clone()], true)
            .expect("valid direct append");
        assert_eq!(acknowledgement_refs(&ack).len(), 1);
        drop(store);
        fs::remove_dir_all(directory).expect("remove fixture");
    }
}
