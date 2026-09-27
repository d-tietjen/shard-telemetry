//! Native server ownership: connection handling and request dispatch live in `native_server/`.
mod server;
pub use server::serve_native;
mod dispatch;
use dispatch::*;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::io;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::loki_api::LokiApiError;
use crate::native_protocol::{NativeEncodedPartitionAppend, encode_native_log_query_matches};
use crate::{
    DurableTelemetryStore, MAX_NATIVE_FRAME_BYTES, NATIVE_FRAME_HEADER_BYTES, NativeCapabilities,
    NativeFrame, NativeFrameHeader, NativeOpcode, NativeStatus, NativeTelemetryBatch,
    ProductionRuntime, ServiceState, decode_native_metric_query, decode_native_query,
    decode_native_trace_query, encode_native_capabilities, encode_native_log_query_result,
    encode_native_metric_query_result, encode_native_trace_query_result, is_native_telemetry_batch,
};

enum ValidatedNativeAppend {
    Owned {
        batch: NativeTelemetryBatch,
        wire_ranges: Vec<(Range<usize>, Option<Range<usize>>)>,
    },
    Borrowed {
        topic_partition: shard_stream_core::TopicPartition,
        tenant: Arc<str>,
        item_count: u32,
        envelope_range: Range<usize>,
        transient_range: Option<Range<usize>>,
    },
    BorrowedMany {
        partitions: Vec<NativeEncodedPartitionAppend>,
    },
}

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
