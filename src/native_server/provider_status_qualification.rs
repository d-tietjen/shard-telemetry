//! Exercise real authentication, dispatch, and physical checkpoint ownership without sockets.

use super::*;
use crate::{
    DurableTelemetryConfig, LocalPartitionPopulation, ProviderCoverage, ProviderStatusPartition,
    ProviderStatusRequest, ServiceLifecycle, SingleTenantConfig, StripeConfig, TelemetrySignal,
};
use shard_stream_core::ShardId;
use shard_stream_engine::{
    DurableAppendSink, DurableAppendSinkFactory, DurableSinkCheckpoint, DurableSinkConfig,
    DurableSinkOptions, EngineConfig, EngineError, EngineResult, StreamEngine,
};
use std::num::{NonZeroU16, NonZeroU32};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Directory(PathBuf);
impl Directory {
    fn new(name: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "shard-telemetry-native-status-{name}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos()
        )))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).expect("fixture cleanup");
        }
    }
}

fn request() -> ProviderStatusRequest {
    ProviderStatusRequest {
        tenant: "tenant-a".into(),
        organization_uuid: Some([7; 16]),
        telemetry_plane: Some("private".into()),
        start_timestamp_unix_nanos: Some(1),
        end_timestamp_unix_nanos: Some(100),
        partitions: vec![ProviderStatusPartition {
            signal: TelemetrySignal::Logs,
            partition_id: 0,
        }],
    }
}

fn runtime(lifecycle: Arc<ServiceLifecycle>, timeout: Duration) -> Arc<ProductionRuntime> {
    Arc::new(
        ProductionRuntime::new(
            SingleTenantConfig {
                tenant: Arc::from("tenant-a"),
                bearer_token: Arc::from("synthetic-status-token"),
                max_http_in_flight: 2,
                max_ingest_in_flight: 1,
                max_query_in_flight: 1,
                ingest_bytes_per_second: 0,
                ingest_burst_bytes: 0,
                max_tail_subscribers: 1,
                max_native_connections: 4,
                query_timeout: timeout,
                native_auth_timeout: Duration::from_secs(1),
            },
            lifecycle,
        )
        .expect("actual production controls"),
    )
}

async fn status(
    request: &ProviderStatusRequest,
    store: &Arc<DurableTelemetryStore>,
    config: &NativeServerConfig,
) -> NativeFrame {
    let payload =
        crate::encode_native_provider_status_request(request).expect("valid status request");
    let frame =
        NativeFrame::request(NativeOpcode::ProviderStatus, 41, payload).expect("actual frame");
    let digest = blake3::hash(&frame.payload);
    super::dispatch::dispatch(
        frame.header,
        frame.payload,
        digest,
        Arc::clone(store),
        config,
    )
    .await
}

#[derive(Debug)]
struct QueryUnavailable;
impl NativeRequestGate for QueryUnavailable {
    fn check(&self) -> Result<(), String> {
        Ok(())
    }
    fn check_query(&self) -> Result<(), String> {
        Err("synthetic incomplete query replica".into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_status_authentication_tenant_query_gate_and_admission_are_enforced() {
    let directory = Directory::new("auth");
    let store = Arc::new(
        DurableTelemetryStore::open(DurableTelemetryConfig {
            data_directory: directory.0.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: true,
            retention: None,
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(10),
        })
        .expect("actual store"),
    );
    let lifecycle = Arc::new(ServiceLifecycle::new());
    let runtime = runtime(Arc::clone(&lifecycle), Duration::from_secs(10));
    let config = NativeServerConfig {
        production: Some(Arc::clone(&runtime)),
        ..NativeServerConfig::default()
    };
    let request = request();
    let anonymous = status(&request, &store, &NativeServerConfig::default()).await;
    assert_eq!(
        anonymous.header.status,
        NativeStatus::Unavailable,
        "the new network status contract refuses an anonymous default listener"
    );
    assert_eq!(
        store
            .local_provider_status(&request)
            .expect("trusted direct metadata remains usable")
            .partitions[0]
            .population,
        LocalPartitionPopulation::VerifiedEmpty
    );
    let payload = crate::encode_native_provider_status_request(&request).expect("selection");
    let status_header = NativeFrameHeader::request(NativeOpcode::ProviderStatus, 1, &payload)
        .expect("frame header");
    assert_eq!(
        super::server::authenticate_frame(status_header, &payload, &config)
            .header
            .status,
        NativeStatus::Unauthorized,
        "the actual connection auth handler refuses status as its first frame"
    );
    let wrong = NativeFrame::request(
        NativeOpcode::Authenticate,
        2,
        b"incorrect-status-token".to_vec(),
    )
    .expect("bad credential frame");
    assert_eq!(
        super::server::authenticate_frame(wrong.header, &wrong.payload, &config)
            .header
            .status,
        NativeStatus::Unauthorized
    );
    let credential = NativeFrame::request(
        NativeOpcode::Authenticate,
        3,
        b"synthetic-status-token".to_vec(),
    )
    .expect("credential frame");
    assert_eq!(
        super::server::authenticate_frame(credential.header, &credential.payload, &config)
            .header
            .status,
        NativeStatus::Ok
    );
    assert_eq!(
        status(&request, &store, &config).await.header.status,
        NativeStatus::Unavailable,
        "starting blocks query work"
    );
    lifecycle.mark_ready();
    let mut foreign = request.clone();
    foreign.tenant = "tenant-b".into();
    assert_eq!(
        status(&foreign, &store, &config).await.header.status,
        NativeStatus::Unauthorized
    );
    let denied = NativeServerConfig {
        request_gate: Some(Arc::new(QueryUnavailable)),
        ..config.clone()
    };
    assert_eq!(
        status(&request, &store, &denied).await.header.status,
        NativeStatus::Unavailable
    );
    let occupied = runtime.try_query().expect("occupy shared query admission");
    assert_eq!(
        status(&request, &store, &config).await.header.status,
        NativeStatus::TooManyRequests
    );
    drop(occupied);
    let accepted = status(&request, &store, &config).await;
    assert_eq!(accepted.header.status, NativeStatus::Ok);
    assert_eq!(accepted.header.request_id, 41);
    let observation = crate::decode_native_provider_status_response(&accepted.payload)
        .expect("real response codec");
    assert_eq!(observation.request, request);
    assert_eq!(
        observation.partitions[0].population,
        LocalPartitionPopulation::VerifiedEmpty
    );
    assert_eq!(
        observation.source_interval_coverage,
        ProviderCoverage::Unknown
    );
    let mut invalid = crate::encode_native_provider_status_request(&request).expect("selection");
    invalid[10..12].copy_from_slice(&u16::MAX.to_le_bytes());
    let header = NativeFrameHeader::request(NativeOpcode::ProviderStatus, 42, &invalid)
        .expect("malformed body frame");
    let digest = blake3::hash(&invalid);
    let refused =
        super::dispatch::dispatch(header, invalid, digest, Arc::clone(&store), &config).await;
    assert_eq!(refused.header.status, NativeStatus::BadRequest);
    assert!(
        runtime.try_query().is_some(),
        "malformed status does not consume query admission"
    );
    lifecycle.request_shutdown();
    assert_eq!(
        status(&request, &store, &config).await.header.status,
        NativeStatus::Unavailable
    );
    drop(store);
}

#[derive(Debug)]
struct CheckpointBlock {
    blocked: Mutex<bool>,
    changed: Condvar,
    entered: AtomicBool,
}

struct ReleaseCheckpoint(Arc<CheckpointBlock>);
impl Drop for ReleaseCheckpoint {
    fn drop(&mut self) {
        if let Ok(mut blocked) = self.0.blocked.lock() {
            *blocked = false;
        }
        self.0.changed.notify_all();
    }
}

#[derive(Debug)]
struct BlockingFactory {
    inner: crate::TelemetrySinkFactory,
    block: Arc<CheckpointBlock>,
}

impl DurableAppendSinkFactory for BlockingFactory {
    fn validate_append(&self, payload: &[u8], count: NonZeroU32) -> EngineResult<()> {
        self.inner.validate_append(payload, count)
    }
    fn open_shard(&self, shard: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
        self.inner.open_shard(shard)
    }
    fn load_checkpoint(
        &self,
        partition: shard_stream_core::TopicPartition,
    ) -> EngineResult<Option<DurableSinkCheckpoint>> {
        let mut blocked = self
            .block
            .blocked
            .lock()
            .map_err(|_| EngineError::DurableSinkUnavailable("fixture lock poisoned".into()))?;
        self.block.entered.store(true, Ordering::Release);
        while *blocked {
            blocked =
                self.block.changed.wait(blocked).map_err(|_| {
                    EngineError::DurableSinkUnavailable("fixture lock poisoned".into())
                })?;
        }
        drop(blocked);
        self.inner.load_checkpoint(partition)
    }
}

fn blocked_store(
    directory: &Directory,
    block: &Arc<CheckpointBlock>,
) -> (Arc<DurableTelemetryStore>, Arc<StreamEngine>) {
    let mut sink_config = crate::OtlpSinkConfig::default();
    let one = NonZeroU16::new(1).expect("one owner");
    for signal in [
        &mut sink_config.signals.logs,
        &mut sink_config.signals.traces,
        &mut sink_config.signals.metrics,
    ] {
        signal.logical_partitions = one;
        signal.physical_stripes = one;
    }
    let inner = crate::TelemetrySinkFactory::new([ShardId::new(0)], sink_config)
        .expect("actual signal factory");
    let service = inner.service();
    let factory = Arc::new(BlockingFactory {
        inner,
        block: Arc::clone(block),
    });
    let engine = Arc::new(
        StreamEngine::open_with_durable_sink(
            EngineConfig {
                data_dir: directory.0.join("stream"),
                object_store_dir: None,
                shard_count: 1,
                virtual_lane_count: 1,
                replication_factor: 1,
                min_in_sync_replicas: 1,
                queue_slots_per_shard: 4,
                queue_bytes_per_shard: 1024 * 1024,
                target_pack_bytes: 1024 * 1024,
                max_pack_age: Duration::from_secs(1),
                max_batch_bytes: 64 * 1024,
                max_fetch_bytes: 64 * 1024,
                append_linger: Duration::ZERO,
            },
            DurableSinkConfig::new(factory).with_options(DurableSinkOptions {
                worker_count: 1,
                recovery_timeout: Duration::from_secs(2),
                ..DurableSinkOptions::default()
            }),
        )
        .expect("actual engine with intercepted real checkpoint load"),
    );
    let store = Arc::new(
        DurableTelemetryStore::attach(
            directory.0.join("metadata"),
            Arc::clone(&engine),
            service,
            1,
            1,
            Duration::from_secs(2),
            None,
        )
        .expect("real attached store"),
    );
    (store, engine)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_status_timeout_owns_checkpoint_query_permit() {
    let directory = Directory::new("physical-timeout");
    let block = Arc::new(CheckpointBlock {
        blocked: Mutex::new(true),
        changed: Condvar::new(),
        entered: AtomicBool::new(false),
    });
    let (store, _engine) = blocked_store(&directory, &block);
    let lifecycle = Arc::new(ServiceLifecycle::new());
    lifecycle.mark_ready();
    let runtime = runtime(lifecycle, Duration::from_millis(50));
    let config = NativeServerConfig {
        production: Some(Arc::clone(&runtime)),
        ..NativeServerConfig::default()
    };
    let request = request();
    let _release = ReleaseCheckpoint(Arc::clone(&block));
    let task_request = request.clone();
    let task_store = Arc::clone(&store);
    let task_config = config.clone();
    let task = tokio::spawn(async move { status(&task_request, &task_store, &task_config).await });
    let started = Instant::now();
    while !block.entered.load(Ordering::Acquire) {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "actual sink checkpoint load must start"
        );
        tokio::task::yield_now().await;
    }
    let outcome = task.await.expect("logical request joins");
    assert!(
        block.entered.load(Ordering::Acquire),
        "actual sink factory checkpoint read started"
    );
    assert_eq!(outcome.header.status, NativeStatus::Timeout);
    assert!(
        runtime.try_query().is_none(),
        "logical deadline cannot release physical work admission"
    );
    assert_eq!(
        status(&request, &store, &config).await.header.status,
        NativeStatus::TooManyRequests
    );
    *block.blocked.lock().expect("release gate") = false;
    block.changed.notify_all();
    let started = Instant::now();
    loop {
        if let Some(permit) = runtime.try_query() {
            drop(permit);
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "physical checkpoint job must finish and release its permit"
        );
        tokio::task::yield_now().await;
    }
    let recovered = status(&request, &store, &config).await;
    assert_eq!(recovered.header.status, NativeStatus::Ok);
    let observation = crate::decode_native_provider_status_response(&recovered.payload)
        .expect("actual settled response");
    assert_eq!(
        observation.partitions[0]
            .progress
            .expect("loaded real initial checkpoint")
            .indexed_end,
        Some(0)
    );
    assert_eq!(
        observation.partitions[0].population,
        LocalPartitionPopulation::Unknown,
        "host attachment cannot prove SDK local ownership"
    );
    drop(store);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_status_concurrent_wal_change_makes_the_checkpoint_observation_unknown() {
    let directory = Directory::new("coherence");
    let block = Arc::new(CheckpointBlock {
        blocked: Mutex::new(true),
        changed: Condvar::new(),
        entered: AtomicBool::new(false),
    });
    let (store, engine) = blocked_store(&directory, &block);
    let lifecycle = Arc::new(ServiceLifecycle::new());
    lifecycle.mark_ready();
    let config = NativeServerConfig {
        production: Some(runtime(lifecycle, Duration::from_secs(2))),
        ..NativeServerConfig::default()
    };
    let request = request();
    let _release = ReleaseCheckpoint(Arc::clone(&block));
    let task_request = request.clone();
    let task_store = Arc::clone(&store);
    let task_config = config.clone();
    let task = tokio::spawn(async move { status(&task_request, &task_store, &task_config).await });
    let started = Instant::now();
    while !block.entered.load(Ordering::Acquire) {
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "actual checkpoint read starts after the first watermark sample"
        );
        tokio::task::yield_now().await;
    }
    let partition = request.partitions[0].topic_partition();
    let batch = crate::NativeTelemetryBatch {
        partitions: vec![crate::NativePartitionAppend {
            topic_partition: partition,
            envelope: crate::prepare_loki_log_envelope(
                "tenant-a",
                vec![crate::LokiEntry {
                    timestamp_unix_nanos: 5,
                    labels: std::collections::BTreeMap::new(),
                    line: "concurrent native metadata change".into(),
                    structured_metadata: std::collections::BTreeMap::new(),
                }],
            )
            .expect("real log envelope"),
            transient_context: None,
        }],
    };
    let append_store = Arc::clone(&store);
    let ack =
        tokio::task::spawn_blocking(move || append_store.append_telemetry_batch(&batch, false))
            .await
            .expect("actual WAL worker joins")
            .expect("WAL changes while checkpoint read is held");
    assert_eq!(ack.partitions[0].first_offset, 0);
    *block.blocked.lock().expect("release checkpoint") = false;
    block.changed.notify_all();
    let response = task.await.expect("status query joins");
    assert_eq!(response.header.status, NativeStatus::Ok);
    let observation = crate::decode_native_provider_status_response(&response.payload)
        .expect("coherence response");
    assert!(
        observation.partitions[0].progress.is_none(),
        "watermarks changed around the genuine checkpoint read"
    );
    assert_eq!(
        observation.partitions[0].population,
        LocalPartitionPopulation::Unknown
    );
    engine
        .wait_for_durable_sink_checkpoint(DurableSinkCheckpoint {
            topic_partition: partition,
            next_placement_sequence: shard_stream_core::PlacementSequence::new(1),
            next_offset: shard_stream_core::LogicalOffset::new(1),
        })
        .expect("actual sink settles");
    let settled = status(&request, &store, &config).await;
    assert_eq!(settled.header.status, NativeStatus::Ok);
    assert_eq!(
        crate::decode_native_provider_status_response(&settled.payload)
            .expect("settled observation")
            .partitions[0]
            .progress
            .expect("coherent applied metadata")
            .indexed_end,
        Some(1)
    );
    drop(store);
    drop(engine);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_status_native_client_authenticates_and_reader_rejects_oversized_header() {
    let directory = Directory::new("tcp");
    let store = Arc::new(
        DurableTelemetryStore::open(DurableTelemetryConfig {
            data_directory: directory.0.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: true,
            retention: None,
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(10),
        })
        .expect("actual store"),
    );
    let lifecycle = Arc::new(ServiceLifecycle::new());
    lifecycle.mark_ready();
    let config = NativeServerConfig {
        production: Some(runtime(lifecycle, Duration::from_secs(10))),
        ..NativeServerConfig::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("actual native listener");
    let address = listener.local_addr().expect("listener address");
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server_store = Arc::clone(&store);
    let server = tokio::spawn(async move {
        serve_native(listener, server_store, config, async {
            let _ = stopped.await;
        })
        .await
    });
    let client = crate::ShardTelemetryClient::new(
        crate::NativeClientConfig::new(address).with_auth_token("synthetic-status-token"),
    )
    .expect("actual authenticated native client");
    let request = request();
    let response = client
        .provider_status(&request)
        .await
        .expect("real authenticated status exchange");
    assert_eq!(response.request, request);
    assert_eq!(
        response.partitions[0].population,
        LocalPartitionPopulation::VerifiedEmpty
    );
    assert_eq!(response.source_interval_coverage, ProviderCoverage::Unknown);
    let capped = super::server::bounded_response(
        NativeFrame {
            header: NativeFrameHeader::response(
                NativeOpcode::ProviderStatus,
                90,
                NativeStatus::Ok,
                &crate::encode_native_provider_status_response(&response)
                    .expect("bounded response"),
            )
            .expect("response header"),
            payload: crate::encode_native_provider_status_response(&response)
                .expect("bounded response"),
        },
        128,
    );
    assert_eq!(
        capped.header.status,
        NativeStatus::TooManyRequests,
        "existing outbound control refuses a response, never a successful partial proof"
    );
    let mut foreign = request.clone();
    foreign.tenant = "tenant-b".into();
    assert_eq!(
        client
            .provider_status(&foreign)
            .await
            .expect_err("wrong tenant")
            .status,
        Some(NativeStatus::Unauthorized)
    );
    assert!(
        client
            .describe()
            .await
            .expect("existing Describe after rejected request reconnect")
            .append_v1
    );
    let mut raw = TcpStream::connect(address)
        .await
        .expect("actual raw native client");
    let auth = NativeFrame::request(
        NativeOpcode::Authenticate,
        3,
        b"synthetic-status-token".to_vec(),
    )
    .expect("authenticate frame");
    raw.write_all(&auth.encode())
        .await
        .expect("write authentication");
    let mut header = [0; NATIVE_FRAME_HEADER_BYTES];
    raw.read_exact(&mut header)
        .await
        .expect("authentication response");
    let header = NativeFrameHeader::decode(&header).expect("authentication response header");
    assert_eq!(header.status, NativeStatus::Ok);
    assert_eq!(header.payload_len, 0);
    // Send only an oversized status header: the real reader must close without
    // allocating or waiting for this deliberately absent body.
    let oversized = NativeFrameHeader::request(
        NativeOpcode::ProviderStatus,
        4,
        &vec![0; crate::MAX_PROVIDER_STATUS_REQUEST_BYTES + 1],
    )
    .expect("oversized declared status frame");
    raw.write_all(&oversized.encode())
        .await
        .expect("write oversized header only");
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), raw.read(&mut byte))
            .await
            .expect("fixed header cap closes promptly")
            .expect("read closed connection"),
        0
    );
    drop(raw);
    drop(client);
    stop.send(()).expect("stop listener");
    server
        .await
        .expect("listener joins")
        .expect("listener succeeds");
    drop(store);
}
