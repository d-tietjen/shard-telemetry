use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;

use super::*;
use crate::{
    NativeQuery, NativeQueryDirection, OtlpTelemetryDecoder, ResourceContext, StripeConfig,
    TelemetryValue, TraceId, TraceQuery,
};

#[test]
fn embedded_budget_builder_bounds_local_and_s3_managed_storage() {
    let local = EmbeddedTelemetryConfig::bounded_local(
        std::env::temp_dir().join("shard-telemetry-unused-budget-config"),
        Duration::from_secs(5 * 60),
    )
    .with_storage_budgets(256 * 1024 * 1024, 1024 * 1024 * 1024);
    local.validate().expect("local budgets validate");
    assert_eq!(local.local_limits.append_submission_threads, Some(1));
    assert_eq!(local.local_limits.durable_sink_threads, Some(1));
    let mut parallel_store = local.store.clone();
    parallel_store.shard_count = 4;
    let parallel = EmbeddedTelemetryConfig::new(parallel_store);
    assert_eq!(parallel.local_limits.append_submission_threads, Some(4));
    assert_eq!(parallel.local_limits.durable_sink_threads, Some(4));
    assert!(local.local_limits.queue_bytes_per_shard < 128 * 1024 * 1024);
    assert!(
        local
            .local_limits
            .max_object_payload_bytes_per_partition
            .is_some()
    );
    assert_eq!(local.object_tier.retirement_grace, Duration::ZERO);

    let archived = EmbeddedTelemetryConfig::bounded(
        std::env::temp_dir().join("shard-telemetry-unused-s3-budget-config"),
        Duration::from_secs(60 * 60),
        EmbeddedEvictionPolicy::OffloadToS3(S3ObjectStoreConfig {
            bucket: "telemetry-archive".into(),
            prefix: "device-a".into(),
            region: Some("us-east-1".into()),
            endpoint: None,
            allow_http: false,
            virtual_hosted_style: false,
        }),
    )
    .with_storage_budgets(256 * 1024 * 1024, 1024 * 1024 * 1024);
    archived.validate().expect("S3 budgets validate");
    assert!(archived.store.object_store_directory.is_none());
    assert!(archived.store.s3_object_store.is_some());
    assert!(
        archived
            .local_limits
            .max_object_payload_bytes_per_partition
            .is_none()
    );
    assert_eq!(
        archived.local_limits.control_cache.max_bytes
            + archived.local_limits.payload_cache.max_bytes
            + archived.local_limits.max_lifetime_rollup_bytes,
        1024 * 1024 * 1024
    );

    let mut multi_shard = EmbeddedTelemetryConfig::bounded_local(
        std::env::temp_dir().join("shard-telemetry-unused-multi-shard-config"),
        Duration::from_secs(5 * 60),
    );
    multi_shard.store.shard_count = 4;
    multi_shard.store.tenant_partitions = 1;
    let multi_shard = multi_shard.with_max_ram_bytes(64 * 1024 * 1024);
    multi_shard
        .validate()
        .expect("multi-shard budget validates");
    assert!(
        u64::try_from(multi_shard.local_limits.queue_bytes_per_shard)
            .expect("queue bytes")
            .saturating_mul(u64::from(multi_shard.store.shard_count))
            <= 64 * 1024 * 1024 / 16
    );
}

#[test]
fn direct_embedded_log_path_is_lifecycle_checked_and_avoids_the_native_listener() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-embedded-direct-{}-{nonce}",
        std::process::id()
    ));
    let runtime =
        EmbeddedTelemetryRuntime::open(EmbeddedTelemetryConfig::new(DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::from_micros(250),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        }))
        .expect("runtime opens");
    let event = OtlpLogEvent {
        timestamp_unix_nanos: 42,
        message: Arc::from("embedded fast path"),
        body: Some(TelemetryValue::String(Arc::from("embedded fast path"))),
        resource: Arc::new(ResourceContext::default()),
        ..OtlpLogEvent::default()
    };

    assert!(
        runtime
            .append_log_events("tenant-a", vec![event.clone()], true)
            .is_err()
    );
    runtime.mark_ready().expect("runtime ready");
    let acknowledgement = runtime
        .append_log_events("tenant-a", vec![event.clone()], true)
        .expect("direct append");
    assert_eq!(acknowledgement.partitions.len(), 1);
    let logs = runtime
        .query_native(&NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::new(),
            terms: vec!["fast".to_owned()],
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            limit: 10,
            direction: NativeQueryDirection::OldestFirst,
        })
        .expect("query");
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].line, "embedded fast path");

    let trace_id = TraceId::from_bytes([7; 16]).expect("trace ID");
    let trace_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: trace_id.as_bytes().to_vec(),
                    span_id: vec![8; 8],
                    name: "embedded direct span".into(),
                    start_time_unix_nano: 50,
                    end_time_unix_nano: 60,
                    ..Span::default()
                }],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    };
    let spans = OtlpTelemetryDecoder
        .decode_traces("tenant-a", &trace_request.encode_to_vec())
        .expect("spans decode");
    let acknowledgement = runtime
        .append_trace_events(spans, true)
        .expect("direct trace append");
    assert_eq!(acknowledgement.partitions.len(), 1);
    let traces = runtime
        .query_traces(&TraceQuery {
            tenant: Arc::from("tenant-a"),
            trace_id: Some(trace_id),
            limit: 10,
            ..TraceQuery::default()
        })
        .expect("trace query");
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].name.as_ref(), "embedded direct span");

    runtime.drain().expect("drain");
    assert!(
        runtime
            .append_log_events("tenant-a", vec![event], true)
            .is_err()
    );
    drop(runtime);
    std::fs::remove_dir_all(directory).expect("cleanup");
}

#[test]
fn built_in_worker_runs_retention_and_reports_health() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-embedded-maintenance-{}-{nonce}",
        std::process::id()
    ));
    let runtime = Arc::new(
        EmbeddedTelemetryRuntime::open(EmbeddedTelemetryConfig::bounded_local(
            directory.clone(),
            Duration::from_secs(60),
        ))
        .expect("runtime opens"),
    );
    runtime.mark_ready().expect("runtime ready");
    let worker = runtime
        .spawn_maintenance(Duration::from_millis(5))
        .expect("maintenance starts");
    assert!(runtime.spawn_maintenance(Duration::from_millis(5)).is_err());
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        if runtime.storage_health().expect("health").retention_runs > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "maintenance did not run before the deadline"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let health = runtime.storage_health().expect("health");
    assert!(health.last_successful_maintenance_unix_seconds.is_some());
    worker.shutdown().expect("maintenance stops");
    runtime
        .spawn_maintenance(Duration::from_millis(5))
        .expect("maintenance can restart")
        .shutdown()
        .expect("restarted maintenance stops");
    runtime.drain().expect("runtime drains");
    drop(runtime);
    std::fs::remove_dir_all(directory).expect("cleanup");
}
