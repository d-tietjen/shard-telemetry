use std::future::Future;
use std::io;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::loki_api::LokiApiError;
use crate::{
    DurableTelemetryStore, MAX_NATIVE_FRAME_BYTES, NATIVE_FRAME_HEADER_BYTES, NativeCapabilities,
    NativeFrame, NativeFrameHeader, NativeOpcode, NativeStatus, NativeTelemetryBatch,
    ProductionRuntime, ServiceState, decode_native_metric_query, decode_native_query,
    decode_native_trace_query, encode_native_capabilities, encode_native_log_query_result,
    encode_native_metric_query_result, encode_native_trace_query_result, is_native_telemetry_batch,
};

/// Product-owned admission check evaluated for every native append and query.
///
/// HA distributions use this to fence direct native traffic on followers while
/// allowing an existing connection to follow leadership changes safely.
pub trait NativeRequestGate: Send + Sync + std::fmt::Debug + 'static {
    /// Returns `Ok` only when this process may execute the request.
    fn check(&self) -> Result<(), String>;

    /// Returns `Ok` only when this process can execute a complete native
    /// query. The default keeps coordinator-only gates compatible. HA
    /// products override this when a node hosts only a subset of signal
    /// partitions, preventing a local query from being mistaken for a
    /// cluster-wide result.
    fn check_query(&self) -> Result<(), String> {
        self.check()
    }

    /// Applies product-specific capability constraints before `Describe`
    /// responds. A partitioned HA node can therefore advertise append support
    /// while explicitly withholding query support until it is a complete
    /// query replica, rather than returning silently partial data.
    fn capabilities(&self, capabilities: NativeCapabilities) -> NativeCapabilities {
        capabilities
    }

    /// Returns `Ok` only when this process may append every routed partition.
    ///
    /// The default preserves coordinator-only gates. HA products override this
    /// method to fence each signal partition after the complete STB1 request is
    /// decoded and before any partition append starts.
    fn check_partitions(&self, _partitions: &[crate::NativePartitionAppend]) -> Result<(), String> {
        self.check()
    }
}

/// Runtime limits for the native TCP listener.
#[derive(Debug, Clone)]
pub struct NativeServerConfig {
    /// Largest accepted payload, bounded by [`MAX_NATIVE_FRAME_BYTES`].
    pub max_frame_bytes: usize,
    /// Maximum requests executing concurrently on one connection.
    pub max_in_flight_per_connection: usize,
    /// Maximum response bytes queued or being written for one connection.
    pub max_outbound_bytes_per_connection: usize,
    /// Maximum response bytes reserved across every native connection.
    pub max_outbound_bytes_total: usize,
    /// Maximum request payload bytes buffered across every native connection.
    ///
    /// A reservation is acquired before allocating a frame body and retained
    /// until request dispatch completes. This prevents many valid, large
    /// frames from turning into unbounded process memory use.
    pub max_inbound_bytes_total: usize,
    /// Deadline for one complete native response write.
    pub write_timeout: std::time::Duration,
    /// Wait for exact-query visibility before acknowledging native appends.
    ///
    /// When false, acknowledgement means the authoritative checksummed
    /// STEL envelope is durable and indexing continues under bounded
    /// shard-stream backpressure.
    pub wait_for_index: bool,
    /// Shared authentication, tenant, lifecycle, and admission controls.
    ///
    /// `None` is intended only for tests and explicit development mode.
    pub production: Option<Arc<ProductionRuntime>>,
    /// Optional product-owned fencing check for append and query operations.
    pub request_gate: Option<Arc<dyn NativeRequestGate>>,
    /// Explicitly permits raw native TCP from non-loopback peers in production.
    ///
    /// Keep this disabled unless a local TLS/service-mesh ingress is the
    /// actual encryption boundary for the accepted connection.
    pub allow_insecure_remote: bool,
}

impl Default for NativeServerConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_NATIVE_FRAME_BYTES,
            max_in_flight_per_connection: 64,
            max_outbound_bytes_per_connection: 8 * 1024 * 1024,
            max_outbound_bytes_total: 64 * 1024 * 1024,
            max_inbound_bytes_total: MAX_NATIVE_FRAME_BYTES,
            write_timeout: std::time::Duration::from_secs(15),
            wait_for_index: true,
            production: None,
            request_gate: None,
            allow_insecure_remote: false,
        }
    }
}

impl NativeServerConfig {
    /// Validates listener memory, concurrency, and response-write bounds.
    ///
    /// Product constructors call this during preflight so a deployment cannot
    /// report a healthy configuration and then fail while its listener starts.
    pub fn validate(self) -> io::Result<Self> {
        if self.max_frame_bytes == 0 || self.max_frame_bytes > MAX_NATIVE_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("native max frame bytes must be in 1..={MAX_NATIVE_FRAME_BYTES}"),
            ));
        }
        if self.max_in_flight_per_connection == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native max in-flight requests must be nonzero",
            ));
        }
        if self.max_outbound_bytes_per_connection == 0
            || self.max_outbound_bytes_per_connection > u32::MAX as usize
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native per-connection outbound bytes must be in 1..=u32::MAX",
            ));
        }
        if self.max_outbound_bytes_total < self.max_outbound_bytes_per_connection
            || self.max_outbound_bytes_total > u32::MAX as usize
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native total outbound bytes must be at least one connection and fit u32",
            ));
        }
        if self.max_inbound_bytes_total < self.max_frame_bytes
            || self.max_inbound_bytes_total > u32::MAX as usize
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native total inbound bytes must cover one frame and fit u32",
            ));
        }
        if self.write_timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native write timeout must be nonzero",
            ));
        }
        Ok(self)
    }
}

/// A response whose byte reservations remain held until the writer finishes
/// the socket write. This makes both queued and actively blocked responses
/// count against the same limits.
struct QueuedNativeResponse {
    frame: NativeFrame,
    _connection_bytes: OwnedSemaphorePermit,
    _global_bytes: OwnedSemaphorePermit,
}

async fn enqueue_response(
    sender: &mpsc::Sender<QueuedNativeResponse>,
    response: NativeFrame,
    connection_budget: Arc<Semaphore>,
    global_budget: Arc<Semaphore>,
    reserved_global: Option<OwnedSemaphorePermit>,
    maximum_connection_bytes: usize,
) -> io::Result<()> {
    let response = bounded_response(response, maximum_connection_bytes);
    let bytes = response
        .payload
        .len()
        .saturating_add(NATIVE_FRAME_HEADER_BYTES);
    let bytes = u32::try_from(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "native response exceeds outbound byte budget",
        )
    })?;
    let connection_bytes = connection_budget
        .acquire_many_owned(bytes)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native server stopped"))?;
    let global_bytes = match reserved_global {
        Some(permit) => permit,
        None => global_budget
            .acquire_many_owned(bytes)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native server stopped"))?,
    };
    sender
        .send(QueuedNativeResponse {
            frame: response,
            _connection_bytes: connection_bytes,
            _global_bytes: global_bytes,
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native writer stopped"))
}

fn bounded_response(response: NativeFrame, maximum_connection_bytes: usize) -> NativeFrame {
    if response
        .payload
        .len()
        .saturating_add(NATIVE_FRAME_HEADER_BYTES)
        <= maximum_connection_bytes
    {
        return response;
    }
    error_frame(
        response.header,
        NativeStatus::TooManyRequests,
        "native response exceeds the configured outbound byte budget",
    )
}

/// Serves multiplexed native connections until `shutdown` resolves.
///
/// Responses may complete out of order and are correlated by the request ID
/// copied from each request frame.
pub async fn serve_native<F>(
    listener: TcpListener,
    store: Arc<DurableTelemetryStore>,
    config: NativeServerConfig,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()>,
{
    let config = config.validate()?;
    let outbound_budget = Arc::new(Semaphore::new(config.max_outbound_bytes_total));
    let inbound_budget = Arc::new(Semaphore::new(config.max_inbound_bytes_total));
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (socket, peer) = accepted?;
                if config.production.is_some()
                    && !config.allow_insecure_remote
                    && !peer.ip().is_loopback()
                {
                    continue;
                }
                socket.set_nodelay(true)?;
                let store = Arc::clone(&store);
                let config = config.clone();
                let outbound_budget = Arc::clone(&outbound_budget);
                let inbound_budget = Arc::clone(&inbound_budget);
                let connection_permit = match &config.production {
                    Some(runtime) => match runtime.try_native_connection() {
                        Some(permit) => Some(permit),
                        None => continue,
                    },
                    None => None,
                };
                tokio::spawn(async move {
                    let _connection_permit = connection_permit;
                    if let Err(error) = serve_connection(
                        socket,
                        store,
                        config,
                        outbound_budget,
                        inbound_budget,
                    ).await
                        && error.kind() != io::ErrorKind::UnexpectedEof
                        && error.kind() != io::ErrorKind::ConnectionReset
                    {
                        eprintln!("native ShardTelemetry connection failed: {error}");
                    }
                });
            }
        }
    }
}

async fn serve_connection(
    socket: TcpStream,
    store: Arc<DurableTelemetryStore>,
    config: NativeServerConfig,
    outbound_budget: Arc<Semaphore>,
    inbound_budget: Arc<Semaphore>,
) -> io::Result<()> {
    let (mut reader, mut writer) = socket.into_split();
    let (responses, mut response_receiver) =
        mpsc::channel::<QueuedNativeResponse>(config.max_in_flight_per_connection);
    let write_timeout = config.write_timeout;
    let writer_task = tokio::spawn(async move {
        while let Some(response) = response_receiver.recv().await {
            let write = async {
                writer.write_all(&response.frame.header.encode()).await?;
                writer.write_all(&response.frame.payload).await
            };
            tokio::time::timeout(write_timeout, write)
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "native response write timed out")
                })??;
        }
        writer.shutdown().await
    });
    let permits = Arc::new(Semaphore::new(config.max_in_flight_per_connection));
    let connection_outbound = Arc::new(Semaphore::new(config.max_outbound_bytes_per_connection));
    let mut authenticated = config.production.is_none();
    let mut shutdown = config
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().subscribe_shutdown());
    let authentication_deadline = config
        .production
        .as_ref()
        .map(|runtime| tokio::time::Instant::now() + runtime.native_auth_timeout());

    loop {
        let mut encoded_header = [0; NATIVE_FRAME_HEADER_BYTES];
        if shutdown.as_ref().is_some_and(|shutdown| *shutdown.borrow()) {
            break;
        }
        let read = reader.read_exact(&mut encoded_header);
        let read_result = match (shutdown.as_mut(), authenticated) {
            (Some(shutdown), false) => tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep_until(authentication_deadline.expect("production deadline")) => break,
                result = read => result,
            },
            (Some(shutdown), true) => tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                result = read => result,
            },
            (None, _) => read.await,
        };
        match read_result {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
        let header = NativeFrameHeader::decode(&encoded_header).map_err(invalid_data)?;
        if header.payload_len as usize > config.max_frame_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native frame exceeds the configured connection limit",
            ));
        }
        let inbound_permit = if header.payload_len == 0 {
            None
        } else {
            Some(
                Arc::clone(&inbound_budget)
                    .try_acquire_many_owned(header.payload_len)
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "native inbound byte budget is exhausted",
                        )
                    })?,
            )
        };
        let mut payload = vec![0; header.payload_len as usize];
        if !authenticated {
            match shutdown.as_mut() {
                Some(shutdown) => tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    _ = tokio::time::sleep_until(authentication_deadline.expect("production deadline")) => break,
                    result = reader.read_exact(&mut payload) => { result?; }
                },
                None => reader.read_exact(&mut payload).await.map(|_| ())?,
            }
        } else {
            reader.read_exact(&mut payload).await?;
        }
        header.verify_payload(&payload).map_err(invalid_data)?;
        if !authenticated {
            let response = authenticate_frame(header, &payload, &config);
            authenticated = response.header.status == NativeStatus::Ok;
            enqueue_response(
                &responses,
                response,
                Arc::clone(&connection_outbound),
                Arc::clone(&outbound_budget),
                None,
                config.max_outbound_bytes_per_connection,
            )
            .await?;
            if !authenticated {
                break;
            }
            continue;
        }
        let permit = Arc::clone(&permits)
            .acquire_owned()
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native server stopped"))?;
        let responses = responses.clone();
        let store = Arc::clone(&store);
        let request_config = config.clone();
        let connection_outbound = Arc::clone(&connection_outbound);
        let outbound_budget = Arc::clone(&outbound_budget);
        tokio::spawn(async move {
            let _inbound_permit = inbound_permit;
            // Reserve a whole per-connection response window before executing
            // a query. This bounds work whose exact encoded result size is not
            // known until after stripe execution completes.
            let query_outbound = if matches!(
                header.opcode,
                NativeOpcode::Query | NativeOpcode::QueryMetrics | NativeOpcode::QueryTraces
            ) {
                match outbound_budget
                    .clone()
                    .acquire_many_owned(request_config.max_outbound_bytes_per_connection as u32)
                    .await
                {
                    Ok(permit) => Some(permit),
                    Err(_) => return,
                }
            } else {
                None
            };
            let response = if header.is_response || header.status != NativeStatus::Ok {
                error_frame(
                    header,
                    NativeStatus::BadRequest,
                    "clients must send request frames with status OK",
                )
            } else {
                dispatch(header, payload, store, &request_config).await
            };
            let _ = enqueue_response(
                &responses,
                response,
                connection_outbound,
                outbound_budget,
                query_outbound,
                request_config.max_outbound_bytes_per_connection,
            )
            .await;
            drop(permit);
        });
    }

    drop(responses);
    writer_task
        .await
        .map_err(|error| io::Error::other(format!("native writer task failed: {error}")))?
}

fn authenticate_frame(
    header: NativeFrameHeader,
    payload: &[u8],
    config: &NativeServerConfig,
) -> NativeFrame {
    if header.is_response || header.status != NativeStatus::Ok {
        return error_frame(
            header,
            NativeStatus::BadRequest,
            "authentication must be a request frame with status OK",
        );
    }
    if header.opcode != NativeOpcode::Authenticate {
        if let Some(runtime) = &config.production {
            runtime.record_authentication_failure();
        }
        return error_frame(
            header,
            NativeStatus::Unauthorized,
            "authenticate before sending native operations",
        );
    }
    let Some(runtime) = &config.production else {
        return ok_frame(header, Vec::new());
    };
    let Ok(token) = std::str::from_utf8(payload) else {
        runtime.record_authentication_failure();
        return error_frame(
            header,
            NativeStatus::Unauthorized,
            "native credential is not valid UTF-8",
        );
    };
    if runtime.authenticates(token) {
        ok_frame(header, Vec::new())
    } else {
        error_frame(
            header,
            NativeStatus::Unauthorized,
            "invalid native production credential",
        )
    }
}

async fn dispatch(
    header: NativeFrameHeader,
    payload: Vec<u8>,
    store: Arc<DurableTelemetryStore>,
    config: &NativeServerConfig,
) -> NativeFrame {
    if matches!(
        header.opcode,
        NativeOpcode::Query | NativeOpcode::QueryMetrics | NativeOpcode::QueryTraces
    ) && let Some(gate) = &config.request_gate
        && let Err(error) = gate.check_query()
    {
        return error_frame(header, NativeStatus::Unavailable, &error);
    }
    match header.opcode {
        NativeOpcode::Ping => ok_frame(header, payload),
        NativeOpcode::Describe => {
            if let Some(gate) = &config.request_gate
                && let Err(error) = gate.check()
            {
                return error_frame(header, NativeStatus::Unavailable, &error);
            }
            let mut capabilities = NativeCapabilities {
                protocol_version: 1,
                append_v1: true,
                logical_partitions: [
                    u16::try_from(store.telemetry_partition_count())
                        .expect("DurableTelemetryStore validates the v1 partition space"),
                    u16::try_from(store.telemetry_partition_count())
                        .expect("DurableTelemetryStore validates the v1 partition space"),
                    u16::try_from(store.telemetry_partition_count())
                        .expect("DurableTelemetryStore validates the v1 partition space"),
                ],
                query_logs: true,
                query_metrics: true,
                query_traces: true,
                query_series: false,
                append_queryable: config.wait_for_index,
            };
            if let Some(gate) = &config.request_gate {
                capabilities = gate.capabilities(capabilities);
            }
            match encode_native_capabilities(&capabilities) {
                Ok(encoded) => ok_frame(header, encoded),
                Err(error) => error_frame(header, NativeStatus::Internal, &error.to_string()),
            }
        }
        NativeOpcode::Authenticate => error_frame(
            header,
            NativeStatus::BadRequest,
            "connection is already authenticated",
        ),
        NativeOpcode::Append => {
            let runtime = config.production.clone();
            let ingest_permit = match runtime.as_ref() {
                Some(runtime) => match runtime.try_ingest(payload.len()) {
                    Some(permit) => Some(permit),
                    None if runtime.lifecycle().state() != ServiceState::Ready => {
                        return error_frame(
                            header,
                            NativeStatus::Unavailable,
                            "native ingestion is draining or unavailable",
                        );
                    }
                    None => {
                        return error_frame(
                            header,
                            NativeStatus::TooManyRequests,
                            "native ingest concurrency or rate limit exceeded",
                        );
                    }
                },
                None => None,
            };
            let source_bytes = payload.len();
            if !is_native_telemetry_batch(&payload) {
                return error_frame(
                    header,
                    NativeStatus::BadRequest,
                    "native append requires the signal-aware STB1 payload",
                );
            }
            let telemetry_batch = match NativeTelemetryBatch::decode_native_append(&payload) {
                Ok(batch) => batch,
                Err(error) => {
                    return error_frame(header, NativeStatus::BadRequest, &error.to_string());
                }
            };
            if let Some(gate) = &config.request_gate
                && let Err(error) = gate.check_partitions(&telemetry_batch.partitions)
            {
                return error_frame(header, NativeStatus::Unavailable, &error);
            }
            if let Some(runtime) = &runtime
                && telemetry_batch
                    .partitions
                    .iter()
                    .any(|partition| partition.envelope.tenant.as_ref() != runtime.tenant())
            {
                return error_frame(
                    header,
                    NativeStatus::Unauthorized,
                    "native telemetry tenant does not match the authenticated tenant",
                );
            }
            let wait_for_index = config.wait_for_index;
            let payload_digest = blake3::hash(&payload).to_hex().to_string();
            let retry_id = header.request_id;
            let append = move || {
                let records = telemetry_batch
                    .partitions
                    .iter()
                    .fold(0_u32, |total, partition| {
                        total.saturating_add(partition.envelope.item_count)
                    });
                let result = store
                    .append_validated_telemetry_batch_with_retry_id(
                        &telemetry_batch,
                        wait_for_index,
                        retry_id,
                        payload_digest,
                    )
                    .and_then(|ack| {
                        ack.encode()
                            .map(|encoded| (encoded, records))
                            .map_err(|error| LokiApiError::internal(error.to_string()))
                    });
                drop(ingest_permit);
                result
            };
            match tokio::task::spawn_blocking(append).await {
                Ok(Ok((ack, records))) => {
                    if let Some(runtime) = &config.production {
                        runtime.record_ingest(source_bytes, records as usize);
                    }
                    ok_frame(header, ack)
                }
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(
                    header,
                    NativeStatus::Internal,
                    &format!("native append worker failed: {error}"),
                ),
            }
        }
        NativeOpcode::Query => {
            let query_permit = match config.production.as_ref() {
                Some(runtime) => match runtime.try_query() {
                    Some(permit) => {
                        runtime.record_query();
                        Some(permit)
                    }
                    None if matches!(
                        runtime.lifecycle().state(),
                        ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
                    ) =>
                    {
                        return error_frame(
                            header,
                            NativeStatus::Unavailable,
                            "native query service is unavailable",
                        );
                    }
                    None => {
                        return error_frame(
                            header,
                            NativeStatus::TooManyRequests,
                            "native query concurrency limit exceeded",
                        );
                    }
                },
                None => None,
            };
            let query = match decode_native_query(&payload) {
                Ok(query) => query,
                Err(error) => {
                    return error_frame(header, NativeStatus::BadRequest, &error.to_string());
                }
            };
            if config
                .production
                .as_ref()
                .is_some_and(|runtime| query.tenant != runtime.tenant())
            {
                return error_frame(
                    header,
                    NativeStatus::Unauthorized,
                    "native query tenant does not match the authenticated tenant",
                );
            }
            let tenant = query.tenant.clone();
            let query_timeout = config
                .production
                .as_ref()
                .map(|runtime| runtime.query_timeout());
            let worker = tokio::task::spawn_blocking(move || {
                let result = store.query_native(&query);
                drop(query_permit);
                result
            });
            let result = match query_timeout {
                Some(timeout) => match tokio::time::timeout(timeout, worker).await {
                    Ok(result) => result,
                    Err(_) => {
                        return error_frame(
                            header,
                            NativeStatus::Timeout,
                            "native query deadline exceeded",
                        );
                    }
                },
                None => worker.await,
            };
            match result {
                Ok(Ok(entries)) => match encode_native_log_query_result(&tenant, entries) {
                    Ok(encoded) => ok_frame(header, encoded),
                    Err(error) => error_frame(header, NativeStatus::Internal, &error.to_string()),
                },
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(
                    header,
                    NativeStatus::Internal,
                    &format!("native query worker failed: {error}"),
                ),
            }
        }
        NativeOpcode::QueryMetrics => {
            let query_permit = match config.production.as_ref() {
                Some(runtime) => match runtime.try_query() {
                    Some(permit) => {
                        runtime.record_query();
                        Some(permit)
                    }
                    None if matches!(
                        runtime.lifecycle().state(),
                        ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
                    ) =>
                    {
                        return error_frame(
                            header,
                            NativeStatus::Unavailable,
                            "native query service is unavailable",
                        );
                    }
                    None => {
                        return error_frame(
                            header,
                            NativeStatus::TooManyRequests,
                            "native query concurrency limit exceeded",
                        );
                    }
                },
                None => None,
            };
            let query = match decode_native_metric_query(&payload) {
                Ok(query) => query,
                Err(error) => {
                    return error_frame(header, NativeStatus::BadRequest, &error.to_string());
                }
            };
            if config
                .production
                .as_ref()
                .is_some_and(|runtime| query.tenant.as_ref() != runtime.tenant())
            {
                return error_frame(
                    header,
                    NativeStatus::Unauthorized,
                    "native metric query tenant does not match the authenticated tenant",
                );
            }
            let query_timeout = config
                .production
                .as_ref()
                .map(|runtime| runtime.query_timeout());
            let worker = tokio::task::spawn_blocking(move || {
                let result = store.query_metrics(&query);
                drop(query_permit);
                result
            });
            let result = match query_timeout {
                Some(timeout) => match tokio::time::timeout(timeout, worker).await {
                    Ok(result) => result,
                    Err(_) => {
                        return error_frame(
                            header,
                            NativeStatus::Timeout,
                            "native metric query deadline exceeded",
                        );
                    }
                },
                None => worker.await,
            };
            match result {
                Ok(Ok(points)) => match encode_native_metric_query_result(&points) {
                    Ok(encoded) => ok_frame(header, encoded),
                    Err(error) => error_frame(header, NativeStatus::Internal, &error.to_string()),
                },
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(
                    header,
                    NativeStatus::Internal,
                    &format!("native metric query worker failed: {error}"),
                ),
            }
        }
        NativeOpcode::QueryTraces => {
            let query_permit = match config.production.as_ref() {
                Some(runtime) => match runtime.try_query() {
                    Some(permit) => {
                        runtime.record_query();
                        Some(permit)
                    }
                    None if matches!(
                        runtime.lifecycle().state(),
                        ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
                    ) =>
                    {
                        return error_frame(
                            header,
                            NativeStatus::Unavailable,
                            "native query service is unavailable",
                        );
                    }
                    None => {
                        return error_frame(
                            header,
                            NativeStatus::TooManyRequests,
                            "native query concurrency limit exceeded",
                        );
                    }
                },
                None => None,
            };
            let query = match decode_native_trace_query(&payload) {
                Ok(query) => query,
                Err(error) => {
                    return error_frame(header, NativeStatus::BadRequest, &error.to_string());
                }
            };
            if config
                .production
                .as_ref()
                .is_some_and(|runtime| query.tenant.as_ref() != runtime.tenant())
            {
                return error_frame(
                    header,
                    NativeStatus::Unauthorized,
                    "native trace query tenant does not match the authenticated tenant",
                );
            }
            let query_timeout = config
                .production
                .as_ref()
                .map(|runtime| runtime.query_timeout());
            let worker = tokio::task::spawn_blocking(move || {
                let result = store.query_traces(&query);
                drop(query_permit);
                result
            });
            let result = match query_timeout {
                Some(timeout) => match tokio::time::timeout(timeout, worker).await {
                    Ok(result) => result,
                    Err(_) => {
                        return error_frame(
                            header,
                            NativeStatus::Timeout,
                            "native trace query deadline exceeded",
                        );
                    }
                },
                None => worker.await,
            };
            match result {
                Ok(Ok(spans)) => match encode_native_trace_query_result(&spans) {
                    Ok(encoded) => ok_frame(header, encoded),
                    Err(error) => error_frame(header, NativeStatus::Internal, &error.to_string()),
                },
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(
                    header,
                    NativeStatus::Internal,
                    &format!("native trace query worker failed: {error}"),
                ),
            }
        }
    }
}

fn ok_frame(header: NativeFrameHeader, payload: Vec<u8>) -> NativeFrame {
    NativeFrame::response(header.opcode, header.request_id, NativeStatus::Ok, payload)
        .expect("bounded request produced a bounded response")
}

fn store_error_frame(header: NativeFrameHeader, error: LokiApiError) -> NativeFrame {
    let status = match error.status() {
        axum::http::StatusCode::BAD_REQUEST => NativeStatus::BadRequest,
        axum::http::StatusCode::UNAUTHORIZED | axum::http::StatusCode::FORBIDDEN => {
            NativeStatus::Unauthorized
        }
        axum::http::StatusCode::SERVICE_UNAVAILABLE => NativeStatus::Unavailable,
        axum::http::StatusCode::TOO_MANY_REQUESTS => NativeStatus::TooManyRequests,
        _ => NativeStatus::Internal,
    };
    error_frame(header, status, &error.to_string())
}

fn error_frame(header: NativeFrameHeader, status: NativeStatus, message: &str) -> NativeFrame {
    let mut payload = message.as_bytes();
    if payload.len() > 4_096 {
        payload = &payload[..4_096];
    }
    NativeFrame::response(header.opcode, header.request_id, status, payload.to_vec())
        .expect("error payload is bounded")
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use shard_stream_core::{LogicalPartitionId, TopicPartition};
    use tokio::net::TcpStream;

    use super::*;
    use crate::{
        DurableTelemetryConfig, LokiEntry, NativeLogQueryResult, NativePartitionAppend,
        NativeQuery, NativeQueryDirection, NativeTelemetryAppendAck, NativeTelemetryBatch,
        ServiceLifecycle, SingleTenantConfig, StripeConfig, decode_native_log_query_result,
        encode_native_query, prepare_loki_log_envelope,
    };

    #[derive(Debug)]
    struct DenyGate;

    impl NativeRequestGate for DenyGate {
        fn check(&self) -> Result<(), String> {
            Err("not the current leader".into())
        }
    }

    #[test]
    fn native_server_rejects_an_inbound_budget_smaller_than_one_frame() {
        let config = NativeServerConfig {
            max_frame_bytes: 1024,
            max_inbound_bytes_total: 1023,
            ..NativeServerConfig::default()
        };
        let error = config.validate().expect_err("invalid inbound budget");
        assert!(
            error
                .to_string()
                .contains("total inbound bytes must cover one frame")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_server_reserves_global_input_before_reading_a_frame_body() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-native-input-budget-{}-{nonce}",
            std::process::id()
        ));
        let store = Arc::new(
            DurableTelemetryStore::open(DurableTelemetryConfig {
                data_directory: directory.clone(),
                object_store_directory: None,
                s3_object_store: None,
                recovery_journal: false,
                retention: None,
                shard_count: 1,
                tenant_partitions: 1,
                append_linger: std::time::Duration::from_micros(250),
                stripe: StripeConfig::default(),
                indexed_ack_timeout: std::time::Duration::from_secs(30),
            })
            .expect("store"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server_store = Arc::clone(&store);
        let server = tokio::spawn(async move {
            serve_native(
                listener,
                server_store,
                NativeServerConfig {
                    max_frame_bytes: 16,
                    max_inbound_bytes_total: 16,
                    ..NativeServerConfig::default()
                },
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });

        let header = NativeFrameHeader::request(NativeOpcode::Ping, 1, &[0; 16])
            .expect("header")
            .encode();
        let mut first = TcpStream::connect(address).await.expect("first connect");
        first.write_all(&header).await.expect("first header");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        let mut second = TcpStream::connect(address).await.expect("second connect");
        second.write_all(&header).await.expect("second header");
        let mut closed = [0_u8; 1];
        let read =
            tokio::time::timeout(std::time::Duration::from_secs(1), second.read(&mut closed))
                .await
                .expect("inbound budget closes excess connection")
                .expect("read");
        assert_eq!(read, 0);

        drop(first);
        stop.send(()).expect("stop");
        server
            .await
            .expect("server joins")
            .expect("server succeeds");
        drop(store);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_protocol_pings_appends_and_queries_with_request_ids() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-native-server-{}-{nonce}",
            std::process::id()
        ));
        let store = Arc::new(
            DurableTelemetryStore::open(DurableTelemetryConfig {
                data_directory: directory.clone(),
                object_store_directory: None,
                s3_object_store: None,
                recovery_journal: false,
                retention: None,
                shard_count: 2,
                tenant_partitions: 8,
                append_linger: std::time::Duration::from_micros(250),
                stripe: StripeConfig::default(),
                indexed_ack_timeout: std::time::Duration::from_secs(30),
            })
            .expect("store"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server_store = Arc::clone(&store);
        let server = tokio::spawn(async move {
            serve_native(
                listener,
                server_store,
                NativeServerConfig::default(),
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });
        let mut client = TcpStream::connect(address).await.expect("connect");

        let ping = NativeFrame::request(NativeOpcode::Ping, 7, b"hello".to_vec()).expect("ping");
        write_frame(&mut client, &ping).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 7);
        assert_eq!(response.header.status, NativeStatus::Ok);
        assert_eq!(response.payload, b"hello");

        let entry = LokiEntry {
            timestamp_unix_nanos: 100,
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            line: "native timeout".to_owned(),
            structured_metadata: BTreeMap::from([("trace".to_owned(), "abc".to_owned())]),
        };
        let topic_partition = TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(0));
        let batch = NativeTelemetryBatch {
            partitions: vec![NativePartitionAppend {
                topic_partition,
                envelope: prepare_loki_log_envelope("tenant-a", vec![entry.clone()])
                    .expect("log envelope"),
                transient_context: None,
            }],
        }
        .encode()
        .expect("batch");
        let append = NativeFrame::request(NativeOpcode::Append, 8, batch).expect("append");
        write_frame(&mut client, &append).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 8);
        assert_eq!(response.header.status, NativeStatus::Ok);
        assert_eq!(
            NativeTelemetryAppendAck::decode(&response.payload)
                .expect("ack")
                .partitions[0]
                .first_offset,
            0
        );

        // A retry after a lost acknowledgement uses the same native request
        // ID. It must return the original receipt rather than append a second
        // copy of the log event.
        write_frame(&mut client, &append).await;
        let retry = read_frame(&mut client).await;
        assert_eq!(retry.header.status, NativeStatus::Ok);
        assert_eq!(retry.payload, response.payload);

        let query = NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            terms: vec!["timeout".to_owned()],
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            limit: 10,
            direction: NativeQueryDirection::OldestFirst,
        };
        let query = NativeFrame::request(
            NativeOpcode::Query,
            9,
            encode_native_query(&query).expect("query"),
        )
        .expect("query frame");
        write_frame(&mut client, &query).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 9);
        assert_eq!(response.header.status, NativeStatus::Ok);
        let NativeLogQueryResult { tenant, entries } =
            decode_native_log_query_result(&response.payload).expect("results");
        assert_eq!(tenant, "tenant-a");
        assert_eq!(entries, vec![entry]);

        drop(client);
        stop.send(()).expect("stop");
        server
            .await
            .expect("server joins")
            .expect("server succeeds");
        drop(store);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn production_native_protocol_requires_authentication_before_operations() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-native-auth-{}-{nonce}",
            std::process::id()
        ));
        let store = Arc::new(
            DurableTelemetryStore::open(DurableTelemetryConfig {
                data_directory: directory.clone(),
                object_store_directory: None,
                s3_object_store: None,
                recovery_journal: false,
                retention: None,
                shard_count: 1,
                tenant_partitions: 1,
                append_linger: std::time::Duration::from_micros(250),
                stripe: StripeConfig::default(),
                indexed_ack_timeout: std::time::Duration::from_secs(30),
            })
            .expect("store"),
        );
        let lifecycle = Arc::new(ServiceLifecycle::new());
        lifecycle.mark_ready();
        let runtime = Arc::new(
            ProductionRuntime::new(
                SingleTenantConfig {
                    tenant: Arc::from("tenant-a"),
                    bearer_token: Arc::from("0123456789abcdef"),
                    max_http_in_flight: 4,
                    max_ingest_in_flight: 2,
                    max_query_in_flight: 2,
                    ingest_bytes_per_second: 0,
                    ingest_burst_bytes: 0,
                    max_tail_subscribers: 1,
                    max_native_connections: 4,
                    query_timeout: std::time::Duration::from_secs(30),
                    native_auth_timeout: std::time::Duration::from_millis(50),
                },
                lifecycle,
            )
            .expect("runtime"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server_store = Arc::clone(&store);
        let server = tokio::spawn(async move {
            serve_native(
                listener,
                server_store,
                NativeServerConfig {
                    production: Some(runtime),
                    request_gate: Some(Arc::new(DenyGate)),
                    ..NativeServerConfig::default()
                },
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });

        let mut idle = TcpStream::connect(address).await.expect("idle connect");
        let mut closed = [0_u8; 1];
        let read = tokio::time::timeout(std::time::Duration::from_secs(1), idle.read(&mut closed))
            .await
            .expect("authentication deadline closes idle connection")
            .expect("idle read");
        assert_eq!(read, 0);

        let mut unauthenticated = TcpStream::connect(address).await.expect("connect");
        let ping = NativeFrame::request(NativeOpcode::Ping, 1, b"hello".to_vec()).expect("ping");
        write_frame(&mut unauthenticated, &ping).await;
        let response = read_frame(&mut unauthenticated).await;
        assert_eq!(response.header.status, NativeStatus::Unauthorized);

        let mut client = TcpStream::connect(address).await.expect("connect");
        let authenticate =
            NativeFrame::request(NativeOpcode::Authenticate, 2, b"0123456789abcdef".to_vec())
                .expect("authenticate");
        write_frame(&mut client, &authenticate).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 2);
        assert_eq!(response.header.status, NativeStatus::Ok);

        let ping = NativeFrame::request(NativeOpcode::Ping, 3, b"ready".to_vec()).expect("ping");
        write_frame(&mut client, &ping).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 3);
        assert_eq!(response.header.status, NativeStatus::Ok);
        assert_eq!(response.payload, b"ready");

        let query = NativeFrame::request(NativeOpcode::Query, 4, Vec::new()).expect("query");
        write_frame(&mut client, &query).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 4);
        assert_eq!(response.header.status, NativeStatus::Unavailable);
        assert_eq!(response.payload, b"not the current leader");

        drop(unauthenticated);
        drop(client);
        stop.send(()).expect("stop");
        server
            .await
            .expect("server joins")
            .expect("server succeeds");
        drop(store);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    async fn write_frame(stream: &mut TcpStream, frame: &NativeFrame) {
        stream
            .write_all(&frame.header.encode())
            .await
            .expect("header");
        stream.write_all(&frame.payload).await.expect("payload");
    }

    async fn read_frame(stream: &mut TcpStream) -> NativeFrame {
        let mut header = [0; NATIVE_FRAME_HEADER_BYTES];
        stream.read_exact(&mut header).await.expect("header");
        let header = NativeFrameHeader::decode(&header).expect("decode header");
        let mut payload = vec![0; header.payload_len as usize];
        stream.read_exact(&mut payload).await.expect("payload");
        header.verify_payload(&payload).expect("checksum");
        NativeFrame { header, payload }
    }
}
