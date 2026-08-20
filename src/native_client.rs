use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, MutexGuard};

use crate::{
    MAX_NATIVE_FRAME_BYTES, NATIVE_FRAME_HEADER_BYTES, NativeCapabilities, NativeFrame,
    NativeFrameHeader, NativeLogQueryResult, NativeOpcode, NativeProtocolError, NativeQuery,
    NativeStatus, NativeTelemetryAppendAck, NativeTelemetryBatch, decode_native_capabilities,
    decode_native_log_query_result, decode_native_metric_query_result,
    decode_native_trace_query_result, encode_native_metric_query, encode_native_query,
    encode_native_trace_query,
};

/// Connection and bounded-request settings for [`ShardTelemetryClient`].
#[derive(Debug, Clone)]
pub struct NativeClientConfig {
    /// Native TCP address of the ShardTelemetry server.
    pub endpoint: SocketAddr,
    /// Bearer token sent as the first native frame when configured.
    pub auth_token: Option<Arc<str>>,
    /// Largest accepted response payload.
    pub max_frame_bytes: usize,
    /// Deadline for one authenticate, append, query, or health exchange.
    pub request_timeout: Duration,
    /// Maximum persistent native connections to this endpoint.
    ///
    /// Each connection carries one exchange at a time. A pool larger than one
    /// lets independent partition appends make progress concurrently while
    /// bounding sockets, authentication handshakes, and server admission.
    pub max_connections: usize,
    /// Explicitly permits raw TCP to a non-loopback endpoint.
    ///
    /// Production callers should leave this false and use a local mTLS tunnel
    /// or a TLS-capable transport endpoint. Native bearer authentication is a
    /// protocol frame and must never traverse an untrusted network in clear
    /// text.
    pub allow_insecure_remote: bool,
}

impl NativeClientConfig {
    /// Creates a client configuration with production frame and timeout defaults.
    #[must_use]
    pub fn new(endpoint: SocketAddr) -> Self {
        Self {
            endpoint,
            auth_token: None,
            max_frame_bytes: MAX_NATIVE_FRAME_BYTES,
            request_timeout: Duration::from_secs(30),
            max_connections: 1,
            allow_insecure_remote: false,
        }
    }

    /// Configures the bearer token required by production servers.
    #[must_use]
    pub fn with_auth_token(mut self, auth_token: impl Into<Arc<str>>) -> Self {
        self.auth_token = Some(auth_token.into());
        self
    }

    /// Changes the response-frame bound for this client.
    #[must_use]
    pub const fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    /// Changes the deadline for one native exchange.
    #[must_use]
    pub const fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// Changes the maximum number of reusable connections to this endpoint.
    #[must_use]
    pub const fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Explicitly permits plaintext native TCP to a non-loopback endpoint.
    ///
    /// This exists for a locally administered TLS/service-mesh tunnel whose
    /// remote encryption boundary is outside this client. Direct internet or
    /// LAN use is unsafe; prefer the default fail-closed setting.
    #[must_use]
    pub const fn with_insecure_remote(mut self) -> Self {
        self.allow_insecure_remote = true;
        self
    }

    fn validate(&self) -> Result<(), NativeClientError> {
        if self.max_frame_bytes == 0 || self.max_frame_bytes > MAX_NATIVE_FRAME_BYTES {
            return Err(NativeClientError::new(format!(
                "native client max_frame_bytes must be in 1..={MAX_NATIVE_FRAME_BYTES}"
            )));
        }
        if self.request_timeout.is_zero() {
            return Err(NativeClientError::new(
                "native client request_timeout must be nonzero",
            ));
        }
        if self.max_connections == 0 {
            return Err(NativeClientError::new(
                "native client max_connections must be nonzero",
            ));
        }
        if !self.allow_insecure_remote && !self.endpoint.ip().is_loopback() {
            return Err(NativeClientError::new(
                "plaintext native transport is restricted to loopback; use a TLS tunnel or explicitly opt in to insecure remote transport",
            ));
        }
        if self
            .auth_token
            .as_ref()
            .is_some_and(|token| token.is_empty())
        {
            return Err(NativeClientError::new(
                "native client authentication token must not be empty",
            ));
        }
        Ok(())
    }
}

/// Error returned by the reusable native client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeClientError {
    message: Arc<str>,
    /// Native status returned by the server, when a response was received.
    pub status: Option<NativeStatus>,
}

impl NativeClientError {
    fn new(message: impl Into<Arc<str>>) -> Self {
        Self {
            message: message.into(),
            status: None,
        }
    }

    fn server(status: NativeStatus, message: impl Into<Arc<str>>) -> Self {
        Self {
            message: message.into(),
            status: Some(status),
        }
    }
}

impl fmt::Display for NativeClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for NativeClientError {}

impl From<NativeProtocolError> for NativeClientError {
    fn from(error: NativeProtocolError) -> Self {
        Self::new(error.to_string())
    }
}

/// Persistent, bounded native-v1 client for ShardTelemetry.
///
/// One client uses a bounded pool of reusable TCP connections. Each connection
/// serializes its exchanges; different callers may use different connections
/// concurrently. A failed or timed-out exchange drops only its connection, so
/// its next request starts with a clean authentication handshake instead of
/// consuming a stale frame.
#[derive(Debug)]
pub struct ShardTelemetryClient {
    config: NativeClientConfig,
    connections: Box<[Mutex<Option<TcpStream>>]>,
    request_id_prefix: u64,
    next_request_id: AtomicU64,
    next_connection: AtomicUsize,
}

/// A checked-out pool connection.
///
/// An incomplete native exchange makes framing state unknowable: cancellation
/// may happen after bytes are sent but before the response is fully read. Drop
/// such a connection instead of letting the next request consume that response.
struct ConnectionLease<'a> {
    connection: MutexGuard<'a, Option<TcpStream>>,
    completed: bool,
}

impl<'a> ConnectionLease<'a> {
    fn new(connection: MutexGuard<'a, Option<TcpStream>>) -> Self {
        Self {
            connection,
            completed: false,
        }
    }

    fn stream_mut(&mut self) -> &mut TcpStream {
        self.connection
            .as_mut()
            .expect("native connection was opened before exchange")
    }

    fn preserve(&mut self) {
        self.completed = true;
    }
}

impl Drop for ConnectionLease<'_> {
    fn drop(&mut self) {
        if !self.completed {
            *self.connection = None;
        }
    }
}

impl ShardTelemetryClient {
    /// Builds a client. Connections are established lazily by the first request.
    pub fn new(config: NativeClientConfig) -> Result<Self, NativeClientError> {
        config.validate()?;
        let connections: Box<[Mutex<Option<TcpStream>>]> = (0..config.max_connections)
            .map(|_| Mutex::new(None))
            .collect();
        Ok(Self {
            request_id_prefix: request_id_prefix(&config),
            config,
            connections,
            next_request_id: AtomicU64::new(1),
            next_connection: AtomicUsize::new(0),
        })
    }

    /// Appends one or more signal-aware routed envelopes.
    pub async fn append(
        &self,
        batch: &NativeTelemetryBatch,
    ) -> Result<NativeTelemetryAppendAck, NativeClientError> {
        self.append_with_request_id(batch, self.next_request_id())
            .await
    }

    /// Appends logs with a client-generated request ID.
    ///
    /// Use [`Self::append_with_request_id`] when the caller may retry after an
    /// indeterminate result and therefore needs a durable retry identity.
    pub async fn append_logs(
        &self,
        batch: &NativeTelemetryBatch,
    ) -> Result<NativeTelemetryAppendAck, NativeClientError> {
        self.append(batch).await
    }

    /// Appends traces with a client-generated request ID.
    ///
    /// Use [`Self::append_with_request_id`] for durable retry identity.
    pub async fn append_traces(
        &self,
        batch: &NativeTelemetryBatch,
    ) -> Result<NativeTelemetryAppendAck, NativeClientError> {
        self.append(batch).await
    }

    /// Appends metrics with a client-generated request ID.
    ///
    /// Use [`Self::append_with_request_id`] for durable retry identity.
    pub async fn append_metrics(
        &self,
        batch: &NativeTelemetryBatch,
    ) -> Result<NativeTelemetryAppendAck, NativeClientError> {
        self.append(batch).await
    }

    /// Appends a batch using `request_id` as its durable idempotency identity.
    pub async fn append_with_request_id(
        &self,
        batch: &NativeTelemetryBatch,
        request_id: u128,
    ) -> Result<NativeTelemetryAppendAck, NativeClientError> {
        let payload = batch.encode_native_append()?;
        let response = self
            .request(NativeOpcode::Append, request_id, payload)
            .await?;
        NativeTelemetryAppendAck::decode(&response).map_err(Into::into)
    }

    /// Executes the existing bounded native indexed-log query.
    pub async fn query_logs(
        &self,
        query: &NativeQuery,
    ) -> Result<NativeLogQueryResult, NativeClientError> {
        let payload = encode_native_query(query)?;
        let response = self
            .request(NativeOpcode::Query, self.next_request_id(), payload)
            .await?;
        decode_native_log_query_result(&response).map_err(Into::into)
    }

    /// Executes a bounded native metric query.
    pub async fn query_metrics(
        &self,
        query: &crate::MetricQuery,
    ) -> Result<Vec<crate::DurableMetricPoint>, NativeClientError> {
        let payload = encode_native_metric_query(query)?;
        let response = self
            .request(NativeOpcode::QueryMetrics, self.next_request_id(), payload)
            .await?;
        decode_native_metric_query_result(&response).map_err(Into::into)
    }

    /// Executes a bounded native trace query.
    pub async fn query_traces(
        &self,
        query: &crate::TraceQuery,
    ) -> Result<Vec<crate::DurableSpan>, NativeClientError> {
        let payload = encode_native_trace_query(query)?;
        let response = self
            .request(NativeOpcode::QueryTraces, self.next_request_id(), payload)
            .await?;
        decode_native_trace_query_result(&response).map_err(Into::into)
    }

    /// Returns the authenticated server's native protocol and signal support.
    pub async fn describe(&self) -> Result<NativeCapabilities, NativeClientError> {
        let response = self
            .request(NativeOpcode::Describe, self.next_request_id(), Vec::new())
            .await?;
        decode_native_capabilities(&response).map_err(Into::into)
    }

    /// Checks native listener health and verifies a reusable round trip.
    pub async fn health(&self) -> Result<(), NativeClientError> {
        let payload = b"health".to_vec();
        let response = self
            .request(NativeOpcode::Ping, self.next_request_id(), payload.clone())
            .await?;
        if response != payload {
            return Err(NativeClientError::new(
                "native health response did not echo the request payload",
            ));
        }
        Ok(())
    }

    fn next_request_id(&self) -> u128 {
        let sequence = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        (u128::from(self.request_id_prefix) << 64) | u128::from(sequence)
    }

    async fn request(
        &self,
        opcode: NativeOpcode,
        request_id: u128,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, NativeClientError> {
        let request = NativeFrame::request(opcode, request_id, payload)?;
        let connection_index =
            self.next_connection.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        let mut connection = ConnectionLease::new(self.connections[connection_index].lock().await);
        if connection.connection.is_none() {
            *connection.connection = Some(self.open_connection().await?);
        }
        let exchange = exchange(
            connection.stream_mut(),
            request,
            self.config.max_frame_bytes,
        );
        match tokio::time::timeout(self.config.request_timeout, exchange).await {
            Ok(Ok(response)) => {
                connection.preserve();
                Ok(response)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(NativeClientError::new("native request deadline exceeded")),
        }
    }

    async fn open_connection(&self) -> Result<TcpStream, NativeClientError> {
        let mut stream = tokio::time::timeout(
            self.config.request_timeout,
            TcpStream::connect(self.config.endpoint),
        )
        .await
        .map_err(|_| NativeClientError::new("native connection deadline exceeded"))?
        .map_err(|error| NativeClientError::new(format!("native connection failed: {error}")))?;
        stream.set_nodelay(true).map_err(|error| {
            NativeClientError::new(format!("native TCP_NODELAY failed: {error}"))
        })?;
        if let Some(token) = &self.config.auth_token {
            let request = NativeFrame::request(
                NativeOpcode::Authenticate,
                self.next_request_id(),
                token.as_bytes().to_vec(),
            )?;
            let response = tokio::time::timeout(
                self.config.request_timeout,
                exchange(&mut stream, request, self.config.max_frame_bytes),
            )
            .await
            .map_err(|_| NativeClientError::new("native authentication deadline exceeded"))??;
            if !response.is_empty() {
                return Err(NativeClientError::new(
                    "native authentication returned an unexpected payload",
                ));
            }
        }
        Ok(stream)
    }
}

/// Produces a per-client namespace for one-shot request IDs. Retry-capable
/// callers must still choose their own stable ID with `append_with_request_id`.
/// The time, process, endpoint, and object-address inputs prevent independent
/// client lifetimes from accidentally reusing the small monotonically
/// increasing counter portion while preserving a lock-free hot path.
fn request_id_prefix(config: &NativeClientConfig) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"shard-telemetry-native-client-request-id-v1\0");
    hasher.update(config.endpoint.to_string().as_bytes());
    hasher.update(&std::process::id().to_le_bytes());
    hasher.update(
        &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes(),
    );
    hasher.update(&(config as *const NativeClientConfig as usize).to_le_bytes());
    u64::from_le_bytes(
        hasher.finalize().as_bytes()[..8]
            .try_into()
            .expect("BLAKE3 digest contains eight bytes"),
    )
}

async fn exchange(
    stream: &mut TcpStream,
    request: NativeFrame,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, NativeClientError> {
    stream
        .write_all(&request.header.encode())
        .await
        .map_err(|error| {
            NativeClientError::new(format!("native request header write failed: {error}"))
        })?;
    stream.write_all(&request.payload).await.map_err(|error| {
        NativeClientError::new(format!("native request payload write failed: {error}"))
    })?;

    let mut encoded_header = [0_u8; NATIVE_FRAME_HEADER_BYTES];
    stream
        .read_exact(&mut encoded_header)
        .await
        .map_err(|error| {
            NativeClientError::new(format!("native response header read failed: {error}"))
        })?;
    let header = NativeFrameHeader::decode(&encoded_header)?;
    if !header.is_response
        || header.opcode != request.header.opcode
        || header.request_id != request.header.request_id
    {
        return Err(NativeClientError::new(
            "native response does not match the active request",
        ));
    }
    if header.payload_len as usize > max_frame_bytes {
        return Err(NativeClientError::new(
            "native response exceeds the configured client frame limit",
        ));
    }
    let mut payload = vec![0; header.payload_len as usize];
    stream.read_exact(&mut payload).await.map_err(|error| {
        NativeClientError::new(format!("native response payload read failed: {error}"))
    })?;
    header.verify_payload(&payload)?;
    if header.status != NativeStatus::Ok {
        let message = std::str::from_utf8(&payload)
            .map(str::to_owned)
            .unwrap_or_else(|_| "native server returned a non-UTF-8 error payload".to_string());
        return Err(NativeClientError::server(header.status, message));
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use shard_stream_core::{LogicalPartitionId, TopicPartition};
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        DurableTelemetryConfig, DurableTelemetryStore, LokiEntry, NativePartitionAppend,
        NativeQueryDirection, NativeRequestGate, NativeServerConfig, StripeConfig,
        prepare_loki_log_envelope, serve_native,
    };

    #[derive(Debug)]
    struct ParallelAppendGate {
        state: std::sync::Mutex<ParallelAppendGateState>,
        arrivals: std::sync::Condvar,
    }

    #[derive(Debug, Default)]
    struct ParallelAppendGateState {
        active: usize,
        maximum_active: usize,
    }

    impl NativeRequestGate for ParallelAppendGate {
        fn check(&self) -> Result<(), String> {
            Ok(())
        }

        fn check_partitions(
            &self,
            _partitions: &[crate::NativePartitionAppend],
        ) -> Result<(), String> {
            let mut state = self.state.lock().map_err(|_| "test gate poisoned")?;
            state.active = state.active.saturating_add(1);
            state.maximum_active = state.maximum_active.max(state.active);
            if state.active == 1 {
                let (next, _) = self
                    .arrivals
                    .wait_timeout(state, Duration::from_secs(2))
                    .map_err(|_| "test gate poisoned")?;
                state = next;
            } else {
                self.arrivals.notify_one();
            }
            state.active = state.active.saturating_sub(1);
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_reuses_a_connection_for_append_query_and_health() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-native-client-{}-{nonce}",
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
                append_linger: Duration::from_micros(250),
                stripe: StripeConfig::default(),
                indexed_ack_timeout: Duration::from_secs(30),
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

        let client = ShardTelemetryClient::new(NativeClientConfig::new(address)).expect("client");
        client.health().await.expect("health");
        let capabilities = client.describe().await.expect("capabilities");
        assert!(capabilities.append_v1);
        assert!(capabilities.query_metrics);
        assert!(capabilities.query_traces);
        assert_eq!(capabilities.logical_partitions, [1, 1, 1]);
        assert!(
            client
                .query_metrics(&crate::MetricQuery {
                    tenant: Arc::from("tenant-a"),
                    limit: 10,
                    ..crate::MetricQuery::default()
                })
                .await
                .expect("empty metric query")
                .is_empty()
        );
        assert!(
            client
                .query_traces(&crate::TraceQuery {
                    tenant: Arc::from("tenant-a"),
                    limit: 10,
                    ..crate::TraceQuery::default()
                })
                .await
                .expect("empty trace query")
                .is_empty()
        );
        let entry = LokiEntry {
            timestamp_unix_nanos: 100,
            labels: BTreeMap::from([("service".to_owned(), "api".to_owned())]),
            line: "native client event".to_owned(),
            structured_metadata: BTreeMap::new(),
        };
        let batch = NativeTelemetryBatch {
            partitions: vec![NativePartitionAppend {
                topic_partition: TopicPartition::new(
                    crate::LOGS_TOPIC_ID,
                    LogicalPartitionId::new(0),
                ),
                envelope: prepare_loki_log_envelope("tenant-a", vec![entry.clone()])
                    .expect("envelope"),
                transient_context: None,
            }],
        };
        let first = client
            .append_with_request_id(&batch, 42)
            .await
            .expect("append");
        let replay = client
            .append_with_request_id(&batch, 42)
            .await
            .expect("replay");
        assert_eq!(first, replay);
        let queried = client
            .query_logs(&NativeQuery {
                tenant: "tenant-a".to_owned(),
                labels: BTreeMap::from([("service".to_owned(), "api".to_owned())]),
                terms: vec!["client".to_owned()],
                start_timestamp_unix_nanos: None,
                end_timestamp_unix_nanos: None,
                limit: 10,
                direction: NativeQueryDirection::OldestFirst,
            })
            .await
            .expect("query");
        assert_eq!(queried.entries, vec![entry]);

        drop(client);
        stop.send(()).expect("stop");
        server
            .await
            .expect("server joins")
            .expect("server succeeds");
        drop(store);
        std::fs::remove_dir_all(directory).expect("cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn configured_connection_pool_executes_independent_appends_concurrently() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-native-client-pool-{}-{nonce}",
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
                tenant_partitions: 2,
                append_linger: Duration::from_micros(250),
                stripe: StripeConfig::default(),
                indexed_ack_timeout: Duration::from_secs(30),
            })
            .expect("store"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let gate = Arc::new(ParallelAppendGate {
            state: std::sync::Mutex::new(ParallelAppendGateState::default()),
            arrivals: std::sync::Condvar::new(),
        });
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server_store = Arc::clone(&store);
        let server_gate: Arc<dyn NativeRequestGate> = gate.clone();
        let server = tokio::spawn(async move {
            serve_native(
                listener,
                server_store,
                NativeServerConfig {
                    request_gate: Some(server_gate),
                    ..NativeServerConfig::default()
                },
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });
        let client = Arc::new(
            ShardTelemetryClient::new(NativeClientConfig::new(address).with_max_connections(2))
                .expect("client"),
        );
        assert_eq!(client.connections.len(), 2);
        let first = NativeTelemetryBatch {
            partitions: vec![NativePartitionAppend {
                topic_partition: TopicPartition::new(
                    crate::LOGS_TOPIC_ID,
                    LogicalPartitionId::new(0),
                ),
                envelope: prepare_loki_log_envelope(
                    "tenant-a",
                    vec![LokiEntry {
                        timestamp_unix_nanos: 100,
                        labels: BTreeMap::new(),
                        line: "first pooled append".to_owned(),
                        structured_metadata: BTreeMap::new(),
                    }],
                )
                .expect("first envelope"),
                transient_context: None,
            }],
        };
        let second = NativeTelemetryBatch {
            partitions: vec![NativePartitionAppend {
                topic_partition: TopicPartition::new(
                    crate::LOGS_TOPIC_ID,
                    LogicalPartitionId::new(1),
                ),
                envelope: prepare_loki_log_envelope(
                    "tenant-a",
                    vec![LokiEntry {
                        timestamp_unix_nanos: 101,
                        labels: BTreeMap::new(),
                        line: "second pooled append".to_owned(),
                        structured_metadata: BTreeMap::new(),
                    }],
                )
                .expect("second envelope"),
                transient_context: None,
            }],
        };
        let start = Arc::new(tokio::sync::Barrier::new(3));
        let first_client = Arc::clone(&client);
        let first_start = Arc::clone(&start);
        let first_append = tokio::spawn(async move {
            first_start.wait().await;
            first_client.append_with_request_id(&first, 100).await
        });
        let second_client = Arc::clone(&client);
        let second_start = Arc::clone(&start);
        let second_append = tokio::spawn(async move {
            second_start.wait().await;
            second_client.append_with_request_id(&second, 101).await
        });
        start.wait().await;
        first_append
            .await
            .expect("first append task")
            .expect("first append");
        second_append
            .await
            .expect("second append task")
            .expect("second append");
        assert_eq!(
            gate.state.lock().expect("test gate state").maximum_active,
            2
        );

        drop(client);
        stop.send(()).expect("stop");
        server
            .await
            .expect("server joins")
            .expect("server succeeds");
        drop(store);
        std::fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn client_rejects_an_empty_connection_pool() {
        let endpoint = "127.0.0.1:3101".parse().expect("endpoint");
        let error =
            ShardTelemetryClient::new(NativeClientConfig::new(endpoint).with_max_connections(0))
                .expect_err("empty pool must fail");
        assert_eq!(
            error.to_string(),
            "native client max_connections must be nonzero"
        );
    }

    #[test]
    fn client_rejects_plaintext_remote_transport_by_default() {
        let endpoint = "192.0.2.10:3101".parse().expect("address");
        assert!(ShardTelemetryClient::new(NativeClientConfig::new(endpoint)).is_err());
        assert!(
            ShardTelemetryClient::new(NativeClientConfig::new(endpoint).with_insecure_remote())
                .is_ok()
        );
    }

    #[tokio::test]
    async fn incomplete_connection_lease_discards_the_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let client = TcpStream::connect(address).await.expect("connect");
        let (server, _) = listener.accept().await.expect("accept");
        let connection = Mutex::new(Some(client));
        let lease = ConnectionLease::new(connection.lock().await);
        drop(lease);
        assert!(connection.lock().await.is_none());
        drop(server);
    }
}
