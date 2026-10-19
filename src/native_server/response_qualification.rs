//! Real SDK dispatch/store/codec completion tests; fixture gates are not private-engine proofs.

use super::*;
use opentelemetry_proto::tonic::{
    collector::{metrics::v1::ExportMetricsServiceRequest, trace::v1::ExportTraceServiceRequest},
    metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    },
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicPartition};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const REFUSAL: &str = "fixture query dependency invalidated";
type EncodedObserver = Box<dyn FnOnce(NativeOpcode, &[u8])>;
thread_local! {
    static ENCODED: RefCell<Option<EncodedObserver>> = const { RefCell::new(None) };
}

pub(super) fn encoded_response(opcode: NativeOpcode, payload: &[u8]) {
    let observer = ENCODED.with(|slot| slot.borrow_mut().take());
    if let Some(observer) = observer {
        observer(opcode, payload);
    }
}

struct ObserveEncoding;
impl ObserveEncoding {
    fn new(observer: impl FnOnce(NativeOpcode, &[u8]) + 'static) -> Self {
        ENCODED.with(|slot| {
            assert!(slot.borrow().is_none());
            *slot.borrow_mut() = Some(Box::new(observer));
        });
        Self
    }
}
impl Drop for ObserveEncoding {
    fn drop(&mut self) {
        ENCODED.with(|slot| slot.borrow_mut().take());
    }
}

#[derive(Debug)]
struct Gate {
    valid: AtomicBool,
    query_checks: AtomicUsize,
    append_only: bool,
}
impl Gate {
    fn new(valid: bool, append_only: bool) -> Self {
        Self {
            valid: AtomicBool::new(valid),
            query_checks: AtomicUsize::new(0),
            append_only,
        }
    }
}
impl NativeRequestGate for Gate {
    fn check(&self) -> Result<(), String> {
        Ok(())
    }
    fn check_query(&self) -> Result<(), String> {
        self.query_checks.fetch_add(1, Ordering::AcqRel);
        self.valid
            .load(Ordering::Acquire)
            .then_some(())
            .ok_or_else(|| REFUSAL.into())
    }
    fn capabilities(&self, mut capabilities: NativeCapabilities) -> NativeCapabilities {
        if self.append_only {
            capabilities.query_logs = false;
            capabilities.query_metrics = false;
            capabilities.query_traces = false;
            capabilities.query_series = false;
            capabilities.append_queryable = false;
        }
        capabilities
    }
}

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).expect("fixture cleanup");
        }
    }
}
struct Fixture {
    // Drop the actual store before removing its directory.
    store: Arc<DurableTelemetryStore>,
    _directory: Directory,
}
impl Fixture {
    fn new() -> Self {
        let directory = Directory(std::env::temp_dir().join(format!(
            "shard-telemetry-query-completion-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos()
        )));
        let store = Arc::new(
            DurableTelemetryStore::open(crate::DurableTelemetryConfig {
                data_directory: directory.0.clone(),
                object_store_directory: None,
                s3_object_store: None,
                recovery_journal: true,
                retention: None,
                shard_count: 1,
                tenant_partitions: 1,
                append_linger: Duration::ZERO,
                stripe: crate::StripeConfig::default(),
                indexed_ack_timeout: Duration::from_secs(10),
            })
            .expect("actual SDK store"),
        );
        store
            .append_telemetry_batch(&log_batch(), true)
            .expect("real indexed log");
        let trace = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "completion-span".into(),
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 20,
                        ..Span::default()
                    }],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        };
        let events = crate::OtlpTelemetryDecoder
            .decode_traces("tenant-a", &trace.encode_to_vec())
            .expect("real trace decoder");
        store
            .append_trace_events(events, true)
            .expect("real indexed span");
        let metrics = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "completion-gauge".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 30,
                                value: Some(number_data_point::Value::AsInt(7)),
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
        let points = crate::OtlpTelemetryDecoder
            .decode_metrics("tenant-a", &metrics.encode_to_vec())
            .expect("real metric decoder")
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
            .expect("real indexed metric");
        Self {
            store,
            _directory: directory,
        }
    }
}

fn log_batch() -> crate::NativeTelemetryBatch {
    crate::NativeTelemetryBatch {
        partitions: vec![crate::NativePartitionAppend {
            topic_partition: TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(0)),
            envelope: crate::prepare_loki_log_envelope(
                "tenant-a",
                vec![crate::LokiEntry {
                    timestamp_unix_nanos: 5,
                    labels: BTreeMap::new(),
                    line: "completion-log".into(),
                    structured_metadata: BTreeMap::new(),
                }],
            )
            .expect("real log envelope"),
            transient_context: None,
        }],
    }
}
fn server_config(gate: &Arc<Gate>) -> NativeServerConfig {
    let lifecycle = Arc::new(crate::ServiceLifecycle::new());
    lifecycle.mark_ready();
    let runtime = ProductionRuntime::new(
        crate::SingleTenantConfig {
            tenant: Arc::from("tenant-a"),
            bearer_token: Arc::from("synthetic-completion-token"),
            max_http_in_flight: 2,
            max_ingest_in_flight: 1,
            max_query_in_flight: 1,
            ingest_bytes_per_second: 0,
            ingest_burst_bytes: 0,
            max_tail_subscribers: 1,
            max_native_connections: 2,
            query_timeout: Duration::from_secs(10),
            native_auth_timeout: Duration::from_secs(1),
        },
        lifecycle,
    )
    .expect("actual production controls");
    NativeServerConfig {
        production: Some(Arc::new(runtime)),
        request_gate: Some(gate.clone()),
        ..NativeServerConfig::default()
    }
}
fn query() -> crate::NativeQuery {
    crate::NativeQuery {
        tenant: "tenant-a".into(),
        labels: BTreeMap::new(),
        terms: Vec::new(),
        start_timestamp_unix_nanos: None,
        end_timestamp_unix_nanos: None,
        limit: 10,
        direction: crate::NativeQueryDirection::OldestFirst,
    }
}
fn request(opcode: NativeOpcode) -> Vec<u8> {
    match opcode {
        NativeOpcode::Query => crate::encode_native_query(&query()).expect("log request"),
        NativeOpcode::QueryLogsPage => {
            crate::encode_native_log_page_query(&crate::NativeLogPageQuery {
                query: query(),
                max_bytes: 1024,
                cursor: None,
            })
            .expect("page request")
        }
        NativeOpcode::QueryMetrics => crate::encode_native_metric_query(&crate::MetricQuery {
            tenant: Arc::from("tenant-a"),
            limit: 10,
            ..crate::MetricQuery::default()
        })
        .expect("metric request"),
        NativeOpcode::QueryTraces => crate::encode_native_trace_query(&crate::TraceQuery {
            tenant: Arc::from("tenant-a"),
            limit: 10,
            ..crate::TraceQuery::default()
        })
        .expect("trace request"),
        NativeOpcode::ProviderStatus => {
            crate::encode_native_provider_status_request(&crate::ProviderStatusRequest {
                tenant: "tenant-a".into(),
                organization_uuid: None,
                telemetry_plane: None,
                start_timestamp_unix_nanos: None,
                end_timestamp_unix_nanos: None,
                partitions: vec![crate::ProviderStatusPartition {
                    signal: crate::TelemetrySignal::Logs,
                    partition_id: 0,
                }],
            })
            .expect("status request")
        }
        _ => Vec::new(),
    }
}
async fn dispatch(
    opcode: NativeOpcode,
    payload: Vec<u8>,
    fixture: &Fixture,
    config: &NativeServerConfig,
) -> NativeFrame {
    let frame = NativeFrame::request(opcode, 73, payload).expect("actual request frame");
    let hash = blake3::hash(&frame.payload);
    super::dispatch::dispatch(
        frame.header,
        frame.payload,
        hash,
        Arc::clone(&fixture.store),
        config,
    )
    .await
}
fn assert_actual_result(opcode: NativeOpcode, payload: &[u8]) {
    match opcode {
        NativeOpcode::Query => {
            let result =
                crate::decode_native_log_query_result(payload).expect("real encoded log result");
            assert_eq!(result.tenant, "tenant-a");
            assert_eq!(result.entries.len(), 1);
            assert_eq!(result.entries[0].line, "completion-log");
        }
        NativeOpcode::QueryLogsPage => {
            let result = crate::decode_native_log_query_page(payload).expect("real encoded page");
            assert_eq!(result.entries.len(), 1);
            assert_eq!(result.entries[0].line, "completion-log");
        }
        NativeOpcode::QueryMetrics => {
            let points =
                crate::decode_native_metric_query_result(payload).expect("real encoded metric");
            assert_eq!(points.len(), 1);
            assert_eq!(points[0].identity.name.as_ref(), "completion-gauge");
            assert_eq!(
                points[0].value,
                crate::MetricValue::Gauge(crate::NumberValue::Integer(7))
            );
        }
        NativeOpcode::QueryTraces => {
            let spans =
                crate::decode_native_trace_query_result(payload).expect("real encoded span");
            assert_eq!(spans.len(), 1);
            assert_eq!(spans[0].name.as_ref(), "completion-span");
        }
        NativeOpcode::ProviderStatus => {
            let status = crate::decode_native_provider_status_response(payload)
                .expect("real encoded status");
            assert_eq!(status.partitions.len(), 1);
            assert_eq!(
                status.partitions[0]
                    .progress
                    .expect("actual checkpoint")
                    .indexed_end,
                Some(1)
            );
        }
        NativeOpcode::Describe => {
            let caps =
                crate::decode_native_capabilities(payload).expect("actual encoded capabilities");
            assert!(
                caps.query_logs && caps.query_metrics && caps.query_traces && caps.append_queryable
            );
        }
        _ => panic!("only actual query-bearing responses are observed"),
    }
}
const QUERIES: [NativeOpcode; 5] = [
    NativeOpcode::Query,
    NativeOpcode::QueryLogsPage,
    NativeOpcode::QueryMetrics,
    NativeOpcode::QueryTraces,
    NativeOpcode::ProviderStatus,
];

#[tokio::test]
async fn query_completion_rechecks_after_real_store_results_are_encoded() {
    let fixture = Fixture::new();
    for opcode in QUERIES {
        let gate = Arc::new(Gate::new(true, false));
        let config = server_config(&gate);
        let accepted = dispatch(opcode, request(opcode), &fixture, &config).await;
        assert_eq!(accepted.header.status, NativeStatus::Ok);
        assert_actual_result(opcode, &accepted.payload);
        assert_eq!(gate.query_checks.load(Ordering::Acquire), 2);
        let observed = Arc::new(AtomicBool::new(false));
        let actual_observed = Arc::clone(&observed);
        let actual_gate = Arc::clone(&gate);
        let _observer = ObserveEncoding::new(move |actual_opcode, payload| {
            assert_eq!(actual_opcode, opcode);
            assert_actual_result(actual_opcode, payload);
            actual_observed.store(true, Ordering::Release);
            actual_gate.valid.store(false, Ordering::Release);
        });
        let refused = dispatch(opcode, request(opcode), &fixture, &config).await;
        assert!(observed.load(Ordering::Acquire));
        assert_eq!(refused.header.status, NativeStatus::Unavailable);
        assert_eq!(refused.header.opcode, opcode);
        assert_eq!(refused.header.request_id, 73);
        assert_eq!(refused.payload, REFUSAL.as_bytes());
        assert_eq!(gate.query_checks.load(Ordering::Acquire), 4);
    }
}

#[tokio::test]
async fn describe_completion_preserves_append_only_discovery_and_nonquery_requests() {
    let fixture = Fixture::new();
    let gate = Arc::new(Gate::new(true, false));
    let config = server_config(&gate);
    let encoded = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&encoded);
    let held = Arc::clone(&gate);
    let observer = ObserveEncoding::new(move |opcode, payload| {
        assert_actual_result(opcode, payload);
        seen.store(true, Ordering::Release);
        held.valid.store(false, Ordering::Release);
    });
    let refused = dispatch(NativeOpcode::Describe, Vec::new(), &fixture, &config).await;
    assert!(encoded.load(Ordering::Acquire));
    assert_eq!(refused.header.status, NativeStatus::Unavailable);
    assert_eq!(refused.payload, REFUSAL.as_bytes());
    assert_eq!(gate.query_checks.load(Ordering::Acquire), 1);
    drop(observer);
    let gate = Arc::new(Gate::new(false, true));
    let config = server_config(&gate);
    let encoded = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&encoded);
    let _observer = ObserveEncoding::new(move |_, _| {
        seen.store(true, Ordering::Release);
    });
    let described = dispatch(NativeOpcode::Describe, Vec::new(), &fixture, &config).await;
    assert_eq!(described.header.status, NativeStatus::Ok);
    let caps =
        crate::decode_native_capabilities(&described.payload).expect("append-only capabilities");
    assert!(caps.append_v1);
    assert!(!caps.query_logs && !caps.query_metrics && !caps.query_traces);
    assert!(!caps.query_series && !caps.append_queryable);
    let ping = dispatch(
        NativeOpcode::Ping,
        b"completion-ping".to_vec(),
        &fixture,
        &config,
    )
    .await;
    assert_eq!(ping.header.status, NativeStatus::Ok);
    assert_eq!(ping.payload, b"completion-ping");
    let appended = dispatch(
        NativeOpcode::Append,
        log_batch().encode().expect("actual append"),
        &fixture,
        &config,
    )
    .await;
    assert_eq!(appended.header.status, NativeStatus::Ok);
    let ack = crate::NativeTelemetryAppendAck::decode(&appended.payload)
        .expect("actual append acknowledgement");
    assert_eq!(ack.partitions[0].first_offset, 1);
    assert!(!encoded.load(Ordering::Acquire));
    assert_eq!(gate.query_checks.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn query_completion_does_not_replace_existing_decode_errors() {
    let fixture = Fixture::new();
    for opcode in QUERIES {
        let gate = Arc::new(Gate::new(true, false));
        let config = server_config(&gate);
        let encoded = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&encoded);
        let _observer = ObserveEncoding::new(move |_, _| {
            seen.store(true, Ordering::Release);
        });
        let error = dispatch(opcode, Vec::new(), &fixture, &config).await;
        assert_eq!(error.header.status, NativeStatus::BadRequest);
        assert!(!encoded.load(Ordering::Acquire));
        assert_eq!(gate.query_checks.load(Ordering::Acquire), 1);
    }
}
