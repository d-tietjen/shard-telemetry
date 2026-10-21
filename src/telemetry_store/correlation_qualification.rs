//! Tenant-isolation regressions for shared-partition correlation fallbacks.

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;
use crate::{CorrelationQuery, ResourceContext, ScopeContext, TelemetryRecordRef, TelemetrySignal};

#[test]
fn log_correlations_isolate_colliding_tenants_after_cold_recovery_and_retention() {
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("timestamp");
    let trace_id = crate::TraceId::from_bytes([1; 16]).expect("trace ID");
    let resource = ResourceContext {
        attributes: Arc::new(vec![crate::TelemetryAttribute::new(
            "application",
            crate::TelemetryValue::String(Arc::from("shared")),
        )]),
        ..ResourceContext::default()
    };
    for recovery_journal in [false, true] {
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-correlation-tenants-{}-{now}-{recovery_journal}",
            std::process::id()
        ));
        let config = DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: Some(directory.join("objects")),
            s3_object_store: None,
            recovery_journal,
            retention: Some(Duration::from_secs(3_600)),
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        };
        let tier = ObjectTierConfig {
            target_group_payload_bytes: 1,
            ..ObjectTierConfig::default()
        };
        let limits = DurableTelemetryLimits {
            append_submission_threads: Some(1),
            durable_sink_threads: Some(1),
            ..DurableTelemetryLimits::default()
        };
        let store = DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
            config.clone(),
            tier,
            limits.clone(),
        )
        .expect("store");
        assert_eq!(store.telemetry_partition_count(), 1);
        for (tenant, age, line) in [
            ("tenant-a", 7_200, "expired a"),
            ("tenant-b", 10, "retained b first"),
            ("tenant-a", 10, "retained a first"),
            ("tenant-b", 10, "retained b second"),
            ("tenant-a", 10, "retained a second"),
        ] {
            store
                .push(
                    tenant,
                    vec![LokiEntry {
                        timestamp_unix_nanos: i64::try_from(now - age * 1_000_000_000)
                            .expect("Loki timestamp"),
                        labels: BTreeMap::from([("application".to_owned(), "shared".to_owned())]),
                        line: line.to_owned(),
                        structured_metadata: BTreeMap::from([(
                            "trace_id".to_owned(),
                            trace_id.to_string(),
                        )]),
                    }],
                )
                .expect("log append");
            // Publish each append as its own group so maintenance can reclaim
            // the expired record without also removing either tenant's data.
            store.flush(Duration::from_secs(30)).expect("group flush");
        }
        let queries = [
            CorrelationQuery::new("tenant-a").with_trace_id(trace_id),
            CorrelationQuery::new("tenant-a")
                .with_trace_id(trace_id)
                .for_signal(TelemetrySignal::Logs),
            CorrelationQuery::new("tenant-a").with_label("application", "shared"),
            CorrelationQuery::new("tenant-a")
                .with_trace_id(trace_id)
                .with_label("application", "shared"),
            CorrelationQuery::new("tenant-a").with_resource_id(resource.id()),
            CorrelationQuery::new("tenant-a").with_scope_id(ScopeContext::default().id()),
        ];
        assert_isolated_log_correlations(&store, &queries);
        drop(store);

        // Published checkpoints suppress WAL and journal replay of these
        // groups. This reopen exercises log refs from the cold catalog.
        let recovered = DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
            config.clone(),
            tier,
            limits.clone(),
        )
        .expect("cold recovery");
        assert_eq!(
            recovered
                .service
                .retained_payload_bytes()
                .expect("resident payload"),
            0,
            "published logs are not recovered into hot payloads"
        );
        let before = recovered
            .service
            .object_tier_cache_stats()
            .expect("cold cache stats");
        assert_isolated_log_correlations(&recovered, &queries);
        let after = recovered
            .service
            .object_tier_cache_stats()
            .expect("queried cache stats");
        assert!(
            after.payload.hits + after.payload.misses > before.payload.hits + before.payload.misses,
            "correlation reads exercised the cold object tier"
        );
        let report = recovered
            .compact_retention()
            .expect("retention maintenance");
        assert_eq!(report.retired_object_groups, 1);
        assert_isolated_log_correlations(&recovered, &queries);
        drop(recovered);

        let maintained = DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
            config, tier, limits,
        )
        .expect("recovery after retention");
        assert_isolated_log_correlations(&maintained, &queries);
        drop(maintained);
        fs::remove_dir_all(directory).expect("remove fixture");
    }
}

fn assert_isolated_log_correlations(store: &DurableTelemetryStore, queries: &[CorrelationQuery]) {
    let partition = TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(0));
    for original in queries {
        for (tenant, offsets) in [("tenant-a", [2, 4]), ("tenant-b", [1, 3])] {
            let expected = offsets
                .map(|offset| TelemetryRecordRef::new(partition, LogicalOffset::new(offset)));
            let query = CorrelationQuery {
                tenant: Arc::from(tenant),
                limit: 10,
                ..original.clone()
            };
            assert_eq!(
                store
                    .query_correlations(&query)
                    .expect("complete tenant result"),
                expected,
                "shared trace, labels and routing must remain tenant scoped"
            );
            let first = query.clone().with_limit(1);
            assert_eq!(
                store
                    .query_correlations(&first)
                    .expect("bounded first page"),
                vec![expected[0]],
                "expired and foreign records cannot consume the page limit"
            );
            let second = first.clone().after(expected[0]);
            assert_eq!(
                store
                    .query_correlations(&second)
                    .expect("bounded second page"),
                vec![expected[1]]
            );
            assert!(
                store
                    .query_correlations(&first.after(expected[1]))
                    .expect("end of tenant cursor")
                    .is_empty()
            );
        }
        for tenant in ["tenant-absent", ""] {
            let absent = CorrelationQuery {
                tenant: Arc::from(tenant),
                ..original.clone()
            };
            assert!(
                store
                    .query_correlations(&absent)
                    .expect("absent tenant")
                    .is_empty()
            );
        }
    }
}

#[test]
fn durable_trace_correlations_require_the_third_identity_before_limit_and_after_restart() {
    use opentelemetry_proto::tonic::{
        collector::trace::v1::ExportTraceServiceRequest,
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };
    use prost::Message;

    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-three-key-correlations-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let config = DurableTelemetryConfig {
        data_directory: directory.clone(),
        object_store_directory: None,
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
        append_submission_threads: Some(1),
        durable_sink_threads: Some(1),
        ..DurableTelemetryLimits::default()
    };
    let store = DurableTelemetryStore::open_with_local_limits(config.clone(), limits.clone())
        .expect("actual store");
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: ["wrong", "wanted", "wanted"]
                .into_iter()
                .enumerate()
                .map(|(index, scope)| ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: scope.to_owned(),
                        ..Default::default()
                    }),
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![u8::try_from(index + 1).expect("span ID"); 8],
                        name: format!("span-{index}"),
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 20,
                        attributes: vec![KeyValue {
                            key: "required".to_owned(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("yes".to_owned())),
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
    };
    store
        .append_trace_events(
            crate::OtlpTelemetryDecoder
                .decode_traces("tenant-a", &request.encode_to_vec())
                .expect("validated trace events"),
            true,
        )
        .expect("actual indexed append");
    let spans = store
        .query_traces(&crate::TraceQuery {
            tenant: Arc::from("tenant-a"),
            limit: 10,
            ..Default::default()
        })
        .expect("actual span coordinates");
    assert_eq!(spans.len(), 3);
    let wanted = spans
        .iter()
        .find(|span| span.name.as_ref() == "span-1")
        .expect("wanted span");
    let absent_scope = ScopeContext {
        name: Arc::from("absent"),
        ..Default::default()
    };
    let required =
        crate::TelemetryAttribute::new("required", crate::TelemetryValue::String(Arc::from("yes")));
    let missing = crate::TelemetryAttribute::new(
        "required",
        crate::TelemetryValue::String(Arc::from("missing")),
    );
    let two = CorrelationQuery::new("tenant-a")
        .with_trace_id(wanted.trace_id)
        .with_resource_id(wanted.resource_id())
        .for_signal(TelemetrySignal::Traces);
    let three = two.clone().with_scope_id(wanted.scope_id());
    let mut expected = spans
        .iter()
        .filter(|span| span.scope_id() == wanted.scope_id())
        .map(|span| span.record_ref)
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(expected.len(), 2);
    let assert_conjunction = |store: &DurableTelemetryStore| {
        for query in [three.clone(), three.clone().with_attribute(&required)] {
            assert_eq!(
                store.query_correlations(&query).expect("all matches"),
                expected
            );
            let first = query.clone().with_limit(1);
            assert_eq!(
                store.query_correlations(&first).expect("first page"),
                vec![expected[0]]
            );
            assert_eq!(
                store
                    .query_correlations(&first.after(expected[0]))
                    .expect("second page"),
                vec![expected[1]]
            );
            assert!(
                store
                    .query_correlations(&query.with_scope_id(absent_scope.id()))
                    .expect("missing third identity")
                    .is_empty()
            );
        }
        assert!(
            store
                .query_correlations(&two.clone().with_attribute(&missing))
                .expect("missing third attribute")
                .is_empty()
        );
        assert!(
            store
                .query_correlations(&three.clone().with_attribute(&missing))
                .expect("missing fourth attribute")
                .is_empty()
        );
    };
    assert_conjunction(&store);
    drop(store);
    let recovered = DurableTelemetryStore::open_with_local_limits(config, limits)
        .expect("actual journal-backed restart");
    assert_conjunction(&recovered);
    drop(recovered);
    fs::remove_dir_all(directory).expect("remove fixture");
}
