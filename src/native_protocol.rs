//! Native protocol ownership: frame and batch models, codecs, query messages, and validation.
//! `native_protocol/` keeps wire operations separate from the stable root types and exports.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use foldhash::{HashMap, HashMapExt};
use shard_stream_core::{LogicalPartitionId, TopicId, TopicPartition};

use crate::{LokiEntry, TelemetryEnvelope};

mod batch;
mod frame;
pub use batch::is_native_telemetry_batch;
mod log_query;
pub(crate) use log_query::encode_native_log_query_matches;
pub use log_query::{decode_native_log_query_result, encode_native_log_query_result};
mod query_codec;
pub use query_codec::{
    decode_native_capabilities, decode_native_metric_query, decode_native_metric_query_result,
    decode_native_query, decode_native_trace_query, decode_native_trace_query_result,
    encode_native_capabilities, encode_native_metric_query, encode_native_metric_query_result,
    encode_native_query, encode_native_trace_query, encode_native_trace_query_result,
};
mod wire;
use wire::*;
#[cfg(test)]
mod tests;

/// Fixed number of bytes in every native protocol frame header.
pub const NATIVE_FRAME_HEADER_BYTES: usize = 32;
/// Production maximum for one native request or response payload.
pub const MAX_NATIVE_FRAME_BYTES: usize = 64 * 1024 * 1024;

const FRAME_MAGIC: [u8; 4] = *b"STNP";
const FRAME_VERSION: u8 = 1;
const FRAME_FLAG_RESPONSE: u8 = 1;
const LOG_QUERY_RESULT_MAGIC: [u8; 4] = *b"STR1";
const TELEMETRY_BATCH_MAGIC: [u8; 4] = *b"STB1";
const TELEMETRY_ACK_MAGIC: [u8; 4] = *b"STM1";
const QUERY_MAGIC: [u8; 4] = *b"STQ1";
const METRIC_QUERY_MAGIC: [u8; 4] = *b"STQ2";
const TRACE_QUERY_MAGIC: [u8; 4] = *b"STQ3";
const METRIC_QUERY_RESULT_MAGIC: [u8; 4] = *b"STR2";
const TRACE_QUERY_RESULT_MAGIC: [u8; 4] = *b"STR3";
const CAPABILITIES_MAGIC: [u8; 4] = *b"STC1";
const LOG_QUERY_RESULT_HEADER_BYTES: usize = 16;
const QUERY_HEADER_BYTES: usize = 32;
const MAX_TENANT_BYTES: usize = 1_024;
const MAX_STREAMS: usize = 65_535;
const MAX_LABELS_PER_STREAM: usize = 256;
const MAX_METADATA_PER_ENTRY: usize = 256;
const MAX_QUERY_TERMS: usize = 256;
const MAX_QUERY_LIMIT: u32 = 1_000_000;
const NATIVE_LABEL_PREFIX: &str = "resource.loki.label.";
const NATIVE_METADATA_PREFIX: &str = "attr.loki.metadata.";

type EnvelopeWireRange = (Range<usize>, Option<Range<usize>>);
type EnvelopeWireRanges = Vec<EnvelopeWireRange>;
type NativeAppendViewRanges<'a> = (
    NativePartitionAppendView<'a>,
    Range<usize>,
    Option<Range<usize>>,
);
type NativeAppendViewsRanges<'a> = (Vec<NativePartitionAppendView<'a>>, EnvelopeWireRanges);

/// Native operation carried by a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NativeOpcode {
    /// Append one grouped log batch.
    Append = 1,
    /// Execute an indexed exact-label and term query.
    Query = 2,
    /// Verify the connection and echo the request payload.
    Ping = 3,
    /// Authenticates a connection before any tenant operation is accepted.
    Authenticate = 4,
    /// Executes a bounded signal-native metric query.
    QueryMetrics = 5,
    /// Executes a bounded signal-native trace query.
    QueryTraces = 6,
    /// Returns negotiated protocol, signal, and query capabilities.
    Describe = 7,
    /// Append one grouped log batch without a durable idempotency receipt.
    ///
    /// Callers must use [`NativeOpcode::Append`] when they need to replay an
    /// indeterminate request safely after a connection failure.
    AppendUntracked = 8,
}

impl NativeOpcode {
    fn from_byte(value: u8) -> Result<Self, NativeProtocolError> {
        match value {
            1 => Ok(Self::Append),
            2 => Ok(Self::Query),
            3 => Ok(Self::Ping),
            4 => Ok(Self::Authenticate),
            5 => Ok(Self::QueryMetrics),
            6 => Ok(Self::QueryTraces),
            7 => Ok(Self::Describe),
            8 => Ok(Self::AppendUntracked),
            _ => Err(NativeProtocolError::new(format!(
                "unsupported native opcode {value}"
            ))),
        }
    }
}

/// Status returned in a native response frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NativeStatus {
    /// The operation completed successfully.
    Ok = 0,
    /// The request frame or payload was invalid.
    BadRequest = 1,
    /// The operation failed after validation.
    Internal = 2,
    /// The requested version or operation is unsupported.
    Unsupported = 3,
    /// The connection did not supply the configured production credential.
    Unauthorized = 4,
    /// The service is draining or temporarily unavailable.
    Unavailable = 5,
    /// A bounded admission or rate limit rejected the request.
    TooManyRequests = 6,
    /// Query execution exceeded the configured response deadline.
    Timeout = 7,
}

impl NativeStatus {
    fn from_byte(value: u8) -> Result<Self, NativeProtocolError> {
        match value {
            0 => Ok(Self::Ok),
            1 => Ok(Self::BadRequest),
            2 => Ok(Self::Internal),
            3 => Ok(Self::Unsupported),
            4 => Ok(Self::Unauthorized),
            5 => Ok(Self::Unavailable),
            6 => Ok(Self::TooManyRequests),
            7 => Ok(Self::Timeout),
            _ => Err(NativeProtocolError::new(format!(
                "unsupported native status {value}"
            ))),
        }
    }
}

/// Validated fixed header for one multiplexed native frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeFrameHeader {
    /// Operation associated with the frame.
    pub opcode: NativeOpcode,
    /// Caller-selected ID copied into the response.
    pub request_id: u128,
    /// Response status; requests must use [`NativeStatus::Ok`].
    pub status: NativeStatus,
    /// Whether this frame is a server response.
    pub is_response: bool,
    /// Number of payload bytes following the header.
    pub payload_len: u32,
    payload_checksum: u32,
}

/// One complete native wire frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeFrame {
    /// Validated frame header.
    pub header: NativeFrameHeader,
    /// Operation-specific payload.
    pub payload: Vec<u8>,
}

/// Decoded stream-grouped native log query result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeLogQueryResult {
    /// Tenant whose records are returned by the query.
    pub tenant: String,
    /// Flattened records; stream labels are encoded once per stream on the wire.
    pub entries: Vec<LokiEntry>,
}

/// One routed STEL envelope in a signal-aware native append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePartitionAppend {
    /// Exact logical topic and partition selected before append.
    pub topic_partition: TopicPartition,
    /// Checksummed signal envelope for that partition.
    pub envelope: TelemetryEnvelope,
    /// Optional process-local index context forwarded to the durable sink.
    ///
    /// This is never part of the authoritative STEL payload and is discarded
    /// after the owner stripe publishes its index. Recovery reconstructs the
    /// same index from the durable envelope.
    pub transient_context: Option<Arc<[u8]>>,
}

/// Borrowed metadata for one validated wire append.
///
/// The native server uses this view when no partition-aware request gate is
/// installed. Large envelope sections remain in the input `Bytes` allocation;
/// only the small routing metadata needed to cross the blocking boundary is
/// retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NativePartitionAppendView<'a> {
    pub(crate) topic_partition: TopicPartition,
    pub(crate) signal: crate::TelemetrySignal,
    pub(crate) tenant: &'a str,
    pub(crate) item_count: u32,
    pub(crate) transient_context: Option<&'a [u8]>,
}

/// Validated metadata and wire ranges for a multi-partition append.
///
/// The server keeps the large envelope and transient-context sections in the
/// request `Bytes` allocation. Only routing metadata and the tenant identity
/// cross into the blocking append worker.
#[derive(Debug, Clone)]
pub(crate) struct NativeEncodedPartitionAppend {
    pub(crate) topic_partition: TopicPartition,
    pub(crate) tenant: Arc<str>,
    pub(crate) item_count: u32,
    pub(crate) envelope_range: Range<usize>,
    pub(crate) transient_range: Option<Range<usize>>,
}

/// Native protocol v1 append containing one envelope per resulting partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeTelemetryBatch {
    /// Routed partition appends. Duplicate partitions are rejected.
    pub partitions: Vec<NativePartitionAppend>,
}

/// Per-partition acknowledgement returned by native protocol v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NativePartitionAck {
    /// Appended topic and partition.
    pub topic_partition: TopicPartition,
    /// First assigned durable offset.
    pub first_offset: u64,
    /// Last assigned durable offset.
    pub last_offset: u64,
}

/// Atomic native v1 response containing one acknowledgement per partition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NativeTelemetryAppendAck {
    /// Partition acknowledgements in request order.
    pub partitions: Vec<NativePartitionAck>,
}

/// Sort direction for an indexed native query.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NativeQueryDirection {
    /// Lowest timestamps first.
    #[default]
    OldestFirst,
    /// Highest timestamps first.
    NewestFirst,
}

/// Bounded native indexed-query request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeQuery {
    /// Tenant to search.
    pub tenant: String,
    /// Exact stream labels combined with AND semantics.
    pub labels: BTreeMap<String, String>,
    /// Case-insensitive exact message tokens combined with AND semantics.
    pub terms: Vec<String>,
    /// Inclusive lower timestamp bound, or no lower bound.
    pub start_timestamp_unix_nanos: Option<u64>,
    /// Exclusive upper timestamp bound, or no upper bound.
    pub end_timestamp_unix_nanos: Option<u64>,
    /// Maximum records to return.
    pub limit: u32,
    /// Timestamp result order.
    pub direction: NativeQueryDirection,
}

/// Capabilities negotiated before a remote producer is allowed to mark its
/// telemetry integration ready.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NativeCapabilities {
    /// Maximum native protocol version understood by this peer.
    pub protocol_version: u8,
    /// Whether native v1 accepts signal-aware append envelopes.
    pub append_v1: bool,
    /// Logical partition counts for logs, traces, and metrics respectively.
    pub logical_partitions: [u16; 3],
    /// Whether indexed log queries are available.
    pub query_logs: bool,
    /// Whether signal-native metric queries are available.
    pub query_metrics: bool,
    /// Whether signal-native trace queries are available.
    pub query_traces: bool,
    /// Whether the bucketed normalized series contract is available.
    pub query_series: bool,
    /// Whether append acknowledgements wait for local query visibility.
    pub append_queryable: bool,
}

/// Protocol validation error suitable for a native bad-request response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeProtocolError {
    message: String,
}

impl NativeProtocolError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for NativeProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for NativeProtocolError {}
