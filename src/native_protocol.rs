use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use foldhash::{HashMap, HashMapExt};
use shard_stream_core::{LogicalPartitionId, TopicId, TopicPartition};

use crate::{LokiEntry, TelemetryEnvelope};

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

impl NativeFrameHeader {
    /// Creates a request header for `payload`.
    pub fn request(
        opcode: NativeOpcode,
        request_id: u128,
        payload: &[u8],
    ) -> Result<Self, NativeProtocolError> {
        Self::new(opcode, request_id, NativeStatus::Ok, false, payload)
    }

    /// Creates a response header for `payload`.
    pub fn response(
        opcode: NativeOpcode,
        request_id: u128,
        status: NativeStatus,
        payload: &[u8],
    ) -> Result<Self, NativeProtocolError> {
        Self::new(opcode, request_id, status, true, payload)
    }

    fn new(
        opcode: NativeOpcode,
        request_id: u128,
        status: NativeStatus,
        is_response: bool,
        payload: &[u8],
    ) -> Result<Self, NativeProtocolError> {
        if payload.len() > MAX_NATIVE_FRAME_BYTES {
            return Err(NativeProtocolError::new(format!(
                "native frame payload is {} bytes, exceeding {MAX_NATIVE_FRAME_BYTES}",
                payload.len()
            )));
        }
        Ok(Self {
            opcode,
            request_id,
            status,
            is_response,
            payload_len: u32::try_from(payload.len())
                .map_err(|_| NativeProtocolError::new("native frame payload exceeds u32"))?,
            payload_checksum: payload_checksum(payload),
        })
    }

    /// Encodes this header into its fixed-width wire representation.
    #[must_use]
    pub fn encode(self) -> [u8; NATIVE_FRAME_HEADER_BYTES] {
        let mut bytes = [0; NATIVE_FRAME_HEADER_BYTES];
        bytes[0..4].copy_from_slice(&FRAME_MAGIC);
        bytes[4] = FRAME_VERSION;
        bytes[5] = self.opcode as u8;
        bytes[6] = u8::from(self.is_response) * FRAME_FLAG_RESPONSE;
        bytes[7] = self.status as u8;
        bytes[8..24].copy_from_slice(&self.request_id.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.payload_len.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.payload_checksum.to_le_bytes());
        bytes
    }

    /// Decodes and validates a fixed-width wire header.
    pub fn decode(bytes: &[u8; NATIVE_FRAME_HEADER_BYTES]) -> Result<Self, NativeProtocolError> {
        if bytes[0..4] != FRAME_MAGIC {
            return Err(NativeProtocolError::new("invalid native frame magic"));
        }
        if bytes[4] != FRAME_VERSION {
            return Err(NativeProtocolError::new(format!(
                "unsupported native frame version {}",
                bytes[4]
            )));
        }
        if bytes[6] & !FRAME_FLAG_RESPONSE != 0 {
            return Err(NativeProtocolError::new(
                "native frame contains unknown flags",
            ));
        }
        let payload_len = u32::from_le_bytes(bytes[24..28].try_into().expect("fixed range"));
        if payload_len as usize > MAX_NATIVE_FRAME_BYTES {
            return Err(NativeProtocolError::new(format!(
                "native frame payload is {payload_len} bytes, exceeding {MAX_NATIVE_FRAME_BYTES}"
            )));
        }
        Ok(Self {
            opcode: NativeOpcode::from_byte(bytes[5])?,
            request_id: u128::from_le_bytes(bytes[8..24].try_into().expect("fixed range")),
            status: NativeStatus::from_byte(bytes[7])?,
            is_response: bytes[6] == FRAME_FLAG_RESPONSE,
            payload_len,
            payload_checksum: u32::from_le_bytes(bytes[28..32].try_into().expect("fixed range")),
        })
    }

    /// Verifies that `payload` has the declared length and BLAKE3 checksum.
    pub fn verify_payload(self, payload: &[u8]) -> Result<(), NativeProtocolError> {
        self.verify_payload_and_hash(payload).map(|_| ())
    }

    /// Verifies `payload` and returns the full BLAKE3 hash computed for the
    /// frame checksum. Callers that need a payload identity can reuse this
    /// value instead of hashing the complete payload a second time.
    pub fn verify_payload_and_hash(
        self,
        payload: &[u8],
    ) -> Result<blake3::Hash, NativeProtocolError> {
        if payload.len() != self.payload_len as usize {
            return Err(NativeProtocolError::new(
                "native frame payload length disagrees with its header",
            ));
        }
        let hash = blake3::hash(payload);
        if u32::from_le_bytes(hash.as_bytes()[0..4].try_into().expect("fixed range"))
            != self.payload_checksum
        {
            return Err(NativeProtocolError::new(
                "native frame payload checksum mismatch",
            ));
        }
        Ok(hash)
    }
}

/// One complete native wire frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeFrame {
    /// Validated frame header.
    pub header: NativeFrameHeader,
    /// Operation-specific payload.
    pub payload: Vec<u8>,
}

impl NativeFrame {
    /// Creates a request frame.
    pub fn request(
        opcode: NativeOpcode,
        request_id: u128,
        payload: Vec<u8>,
    ) -> Result<Self, NativeProtocolError> {
        Ok(Self {
            header: NativeFrameHeader::request(opcode, request_id, &payload)?,
            payload,
        })
    }

    /// Creates a response frame.
    pub fn response(
        opcode: NativeOpcode,
        request_id: u128,
        status: NativeStatus,
        payload: Vec<u8>,
    ) -> Result<Self, NativeProtocolError> {
        Ok(Self {
            header: NativeFrameHeader::response(opcode, request_id, status, &payload)?,
            payload,
        })
    }

    /// Encodes the complete frame.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(NATIVE_FRAME_HEADER_BYTES + self.payload.len());
        encoded.extend_from_slice(&self.header.encode());
        encoded.extend_from_slice(&self.payload);
        encoded
    }
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

impl NativeTelemetryBatch {
    /// Encodes a retryable native v1 append.
    ///
    /// Native v1 deliberately accepts one partition per request. A single
    /// durable retry identity cannot make a parallel multi-partition append
    /// atomic when a later partition fails, so callers fan out one request per
    /// routed partition instead. In-process store APIs retain their grouped
    /// fast path and do not use this wire-level constraint.
    pub fn encode_native_append(&self) -> Result<Vec<u8>, NativeProtocolError> {
        if self.partitions.len() != 1 {
            return Err(NativeProtocolError::new(
                "retryable native v1 append requires exactly one partition",
            ));
        }
        self.encode()
    }

    /// Encodes the bounded signal-aware native v1 payload.
    pub fn encode(&self) -> Result<Vec<u8>, NativeProtocolError> {
        if self.partitions.is_empty() || self.partitions.len() > 256 {
            return Err(NativeProtocolError::new(
                "native telemetry batch requires 1..=256 partitions",
            ));
        }
        let mut seen = (self.partitions.len() > 1).then(BTreeSet::new);
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&TELEMETRY_BATCH_MAGIC);
        encoded.extend_from_slice(
            &u16::try_from(self.partitions.len())
                .expect("partition count was bounded")
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&[0; 2]);
        for partition in &self.partitions {
            if let Some(seen) = &mut seen
                && !seen.insert(partition.topic_partition)
            {
                return Err(NativeProtocolError::new(
                    "native telemetry batch contains a duplicate partition",
                ));
            }
            if partition.topic_partition.topic_id != partition.envelope.signal.topic_id() {
                return Err(NativeProtocolError::new(
                    "native telemetry partition topic disagrees with its signal",
                ));
            }
            let envelope = partition
                .envelope
                .encode()
                .map_err(|error| NativeProtocolError::new(error.to_string()))?;
            encoded.extend_from_slice(&partition.topic_partition.topic_id.get().to_le_bytes());
            encoded.extend_from_slice(&partition.topic_partition.partition_id.get().to_le_bytes());
            encoded.extend_from_slice(
                &u32::try_from(envelope.len())
                    .map_err(|_| NativeProtocolError::new("STEL envelope exceeds u32"))?
                    .to_le_bytes(),
            );
            encoded.extend_from_slice(&envelope);
            let transient_context = partition.transient_context.as_deref().unwrap_or_default();
            encoded.extend_from_slice(
                &u32::try_from(transient_context.len())
                    .map_err(|_| NativeProtocolError::new("transient context exceeds u32"))?
                    .to_le_bytes(),
            );
            encoded.extend_from_slice(transient_context);
        }
        if encoded.len() > MAX_NATIVE_FRAME_BYTES {
            return Err(NativeProtocolError::new(
                "native telemetry batch exceeds the frame limit",
            ));
        }
        Ok(encoded)
    }

    /// Decodes and verifies every STEL envelope before returning any partition.
    pub fn decode(payload: &[u8]) -> Result<Self, NativeProtocolError> {
        Self::decode_impl(payload, false).map(|(batch, _)| batch)
    }

    /// Decodes and validates a batch while retaining the exact wire ranges for
    /// each envelope and optional transient context.
    pub(crate) fn decode_with_envelope_ranges(
        payload: &[u8],
    ) -> Result<(Self, EnvelopeWireRanges), NativeProtocolError> {
        let (batch, ranges) = Self::decode_impl(payload, true)?;
        Ok((
            batch,
            ranges.expect("range capture was enabled for this decode"),
        ))
    }

    fn decode_impl(
        payload: &[u8],
        capture_ranges: bool,
    ) -> Result<(Self, Option<EnvelopeWireRanges>), NativeProtocolError> {
        if payload.len() < 8 || payload[..4] != TELEMETRY_BATCH_MAGIC {
            return Err(NativeProtocolError::new(
                "invalid native telemetry batch header",
            ));
        }
        let count = usize::from(u16::from_le_bytes(
            payload[4..6].try_into().expect("fixed range"),
        ));
        if count == 0 || count > 256 || payload[6..8] != [0, 0] {
            return Err(NativeProtocolError::new(
                "invalid native telemetry partition count or flags",
            ));
        }
        let mut cursor = Cursor::at(payload, 8);
        let mut partitions = Vec::with_capacity(count);
        let mut wire_ranges = capture_ranges.then(|| Vec::with_capacity(count));
        let mut seen = (count > 1).then(BTreeSet::new);
        for _ in 0..count {
            let topic_partition = TopicPartition::new(
                TopicId::new(cursor.u128("telemetry topic ID")?),
                LogicalPartitionId::new(cursor.u32("telemetry partition ID")?),
            );
            let envelope_len = cursor.u32("telemetry envelope length")? as usize;
            let envelope_start = cursor.offset;
            let envelope =
                TelemetryEnvelope::decode(cursor.bytes(envelope_len, "telemetry envelope")?)
                    .map_err(|error| NativeProtocolError::new(error.to_string()))?;
            let envelope_range = envelope_start..cursor.offset;
            let transient_len = cursor.u32("transient context length")? as usize;
            let transient_start = cursor.offset;
            let transient_context = if transient_len == 0 {
                None
            } else {
                Some(Arc::<[u8]>::from(
                    cursor.bytes(transient_len, "transient context")?,
                ))
            };
            let transient_range = (transient_len != 0).then_some(transient_start..cursor.offset);
            if topic_partition.topic_id != envelope.signal.topic_id() {
                return Err(NativeProtocolError::new(
                    "native telemetry partition topic disagrees with its signal",
                ));
            }
            if let Some(seen) = &mut seen
                && !seen.insert(topic_partition)
            {
                return Err(NativeProtocolError::new(
                    "native telemetry batch contains a duplicate partition",
                ));
            }
            if let Some(wire_ranges) = wire_ranges.as_mut() {
                wire_ranges.push((envelope_range, transient_range));
            }
            partitions.push(NativePartitionAppend {
                topic_partition,
                envelope,
                transient_context,
            });
        }
        cursor.finish()?;
        Ok((Self { partitions }, wire_ranges))
    }

    /// Decodes a retryable native v1 append after enforcing its single
    /// partition atomicity boundary.
    pub fn decode_native_append(payload: &[u8]) -> Result<Self, NativeProtocolError> {
        let batch = Self::decode(payload)?;
        if batch.partitions.len() != 1 {
            return Err(NativeProtocolError::new(
                "retryable native v1 append requires exactly one partition",
            ));
        }
        Ok(batch)
    }

    /// Decodes one native append and returns the exact verified STEL byte
    /// range from the wire payload. The native server forwards that range to
    /// storage so it does not re-encode an already checksummed envelope.
    pub(crate) fn decode_native_append_with_envelope_range(
        payload: &[u8],
    ) -> Result<(Self, std::ops::Range<usize>), NativeProtocolError> {
        let (batch, mut ranges) = Self::decode_with_envelope_ranges(payload)?;
        if batch.partitions.len() != 1 {
            return Err(NativeProtocolError::new(
                "retryable native v1 append requires exactly one partition",
            ));
        }
        let (envelope_range, _) = ranges
            .pop()
            .expect("validated single append has one envelope range");
        Ok((batch, envelope_range))
    }

    /// Decodes one native append while retaining the large envelope sections
    /// as borrowed wire slices. The returned ranges can be converted to
    /// `Bytes` without copying once the input buffer is owned by the caller.
    pub(crate) fn decode_native_append_view_with_ranges(
        payload: &[u8],
    ) -> Result<NativeAppendViewRanges<'_>, NativeProtocolError> {
        if payload.len() < 8 || payload[..4] != TELEMETRY_BATCH_MAGIC {
            return Err(NativeProtocolError::new(
                "invalid native telemetry batch header",
            ));
        }
        let count = usize::from(u16::from_le_bytes(
            payload[4..6].try_into().expect("fixed range"),
        ));
        if count != 1 || payload[6..8] != [0, 0] {
            return Err(NativeProtocolError::new(
                "retryable native v1 append requires exactly one partition",
            ));
        }
        let mut cursor = Cursor::at(payload, 8);
        let topic_partition = TopicPartition::new(
            TopicId::new(cursor.u128("telemetry topic ID")?),
            LogicalPartitionId::new(cursor.u32("telemetry partition ID")?),
        );
        let envelope_len = cursor.u32("telemetry envelope length")? as usize;
        let envelope_start = cursor.offset;
        let envelope_bytes = cursor.bytes(envelope_len, "telemetry envelope")?;
        let envelope = crate::envelope::TelemetryEnvelope::decode_view(envelope_bytes)
            .map_err(|error| NativeProtocolError::new(error.to_string()))?;
        let transient_len = cursor.u32("transient context length")? as usize;
        let transient_start = cursor.offset;
        let transient_context = if transient_len == 0 {
            None
        } else {
            Some(cursor.bytes(transient_len, "transient context")?)
        };
        let transient_range = (transient_len != 0).then_some(transient_start..cursor.offset);
        if topic_partition.topic_id != envelope.signal.topic_id() {
            return Err(NativeProtocolError::new(
                "native telemetry partition topic disagrees with its signal",
            ));
        }
        cursor.finish()?;
        Ok((
            NativePartitionAppendView {
                topic_partition,
                signal: envelope.signal,
                tenant: envelope.tenant,
                item_count: envelope.item_count,
                transient_context,
            },
            envelope_start..envelope_start + envelope_bytes.len(),
            transient_range,
        ))
    }

    /// Decodes and validates every partition while retaining large sections as
    /// borrowed wire slices. This is the untracked multi-partition fast path;
    /// retryable appends continue to use the single-partition decoder above.
    pub(crate) fn decode_native_append_views_with_ranges(
        payload: &[u8],
    ) -> Result<NativeAppendViewsRanges<'_>, NativeProtocolError> {
        if payload.len() < 8 || payload[..4] != TELEMETRY_BATCH_MAGIC {
            return Err(NativeProtocolError::new(
                "invalid native telemetry batch header",
            ));
        }
        let count = usize::from(u16::from_le_bytes(
            payload[4..6].try_into().expect("fixed range"),
        ));
        if count == 0 || count > 256 || payload[6..8] != [0, 0] {
            return Err(NativeProtocolError::new(
                "invalid native telemetry partition count or flags",
            ));
        }
        let mut cursor = Cursor::at(payload, 8);
        let mut views = Vec::with_capacity(count);
        let mut wire_ranges = Vec::with_capacity(count);
        let mut seen = (count > 1).then(BTreeSet::new);
        for _ in 0..count {
            let topic_partition = TopicPartition::new(
                TopicId::new(cursor.u128("telemetry topic ID")?),
                LogicalPartitionId::new(cursor.u32("telemetry partition ID")?),
            );
            let envelope_len = cursor.u32("telemetry envelope length")? as usize;
            let envelope_start = cursor.offset;
            let envelope_bytes = cursor.bytes(envelope_len, "telemetry envelope")?;
            let envelope = crate::envelope::TelemetryEnvelope::decode_view(envelope_bytes)
                .map_err(|error| NativeProtocolError::new(error.to_string()))?;
            let transient_len = cursor.u32("transient context length")? as usize;
            let transient_start = cursor.offset;
            let transient_context = if transient_len == 0 {
                None
            } else {
                Some(cursor.bytes(transient_len, "transient context")?)
            };
            let transient_range = (transient_len != 0).then_some(transient_start..cursor.offset);
            if topic_partition.topic_id != envelope.signal.topic_id() {
                return Err(NativeProtocolError::new(
                    "native telemetry partition topic disagrees with its signal",
                ));
            }
            if let Some(seen) = &mut seen
                && !seen.insert(topic_partition)
            {
                return Err(NativeProtocolError::new(
                    "native telemetry batch contains a duplicate partition",
                ));
            }
            views.push(NativePartitionAppendView {
                topic_partition,
                signal: envelope.signal,
                tenant: envelope.tenant,
                item_count: envelope.item_count,
                transient_context,
            });
            wire_ranges.push((
                envelope_start..envelope_start + envelope_bytes.len(),
                transient_range,
            ));
        }
        cursor.finish()?;
        Ok((views, wire_ranges))
    }
}

/// Returns true when a native append payload uses the signal-aware v1 batch format.
#[must_use]
pub fn is_native_telemetry_batch(payload: &[u8]) -> bool {
    payload.starts_with(&TELEMETRY_BATCH_MAGIC)
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

impl NativeTelemetryAppendAck {
    /// Encodes a native v1 multi-partition acknowledgement.
    pub fn encode(&self) -> Result<Vec<u8>, NativeProtocolError> {
        if self.partitions.len() > 256 {
            return Err(NativeProtocolError::new(
                "native acknowledgement exceeds 256 partitions",
            ));
        }
        let mut encoded = Vec::with_capacity(8 + self.partitions.len() * 36);
        encoded.extend_from_slice(&TELEMETRY_ACK_MAGIC);
        encoded.extend_from_slice(
            &u16::try_from(self.partitions.len())
                .expect("ack partition count was bounded")
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&[0; 2]);
        for partition in &self.partitions {
            encoded.extend_from_slice(&partition.topic_partition.topic_id.get().to_le_bytes());
            encoded.extend_from_slice(&partition.topic_partition.partition_id.get().to_le_bytes());
            encoded.extend_from_slice(&partition.first_offset.to_le_bytes());
            encoded.extend_from_slice(&partition.last_offset.to_le_bytes());
        }
        Ok(encoded)
    }

    /// Decodes a native v1 multi-partition acknowledgement.
    pub fn decode(payload: &[u8]) -> Result<Self, NativeProtocolError> {
        if payload.len() < 8 || payload[..4] != TELEMETRY_ACK_MAGIC {
            return Err(NativeProtocolError::new(
                "invalid native telemetry acknowledgement",
            ));
        }
        let count = usize::from(u16::from_le_bytes(
            payload[4..6].try_into().expect("fixed range"),
        ));
        if count > 256 || payload[6..8] != [0, 0] || payload.len() != 8 + count * 36 {
            return Err(NativeProtocolError::new(
                "invalid native telemetry acknowledgement length",
            ));
        }
        let mut cursor = Cursor::at(payload, 8);
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            partitions.push(NativePartitionAck {
                topic_partition: TopicPartition::new(
                    TopicId::new(cursor.u128("ack topic ID")?),
                    LogicalPartitionId::new(cursor.u32("ack partition ID")?),
                ),
                first_offset: cursor.u64("ack first offset")?,
                last_offset: cursor.u64("ack last offset")?,
            });
        }
        cursor.finish()?;
        Ok(Self { partitions })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NativeLogQueryResultInfo<'a> {
    tenant: &'a str,
    record_count: u32,
}

fn inspect_native_log_query_result(
    payload: &[u8],
) -> Result<NativeLogQueryResultInfo<'_>, NativeProtocolError> {
    if payload.len() < LOG_QUERY_RESULT_HEADER_BYTES {
        return Err(NativeProtocolError::new(
            "native log query result is shorter than its header",
        ));
    }
    if payload[0..4] != LOG_QUERY_RESULT_MAGIC {
        return Err(NativeProtocolError::new(
            "invalid native log query result magic",
        ));
    }
    if payload[12..16] != [0; 4] {
        return Err(NativeProtocolError::new(
            "native log query result reserved bytes must be zero",
        ));
    }
    let tenant_len = usize::from(u16::from_le_bytes(
        payload[4..6].try_into().expect("fixed range"),
    ));
    if tenant_len > MAX_TENANT_BYTES {
        return Err(NativeProtocolError::new(
            "native log query result tenant exceeds its limit",
        ));
    }
    let end = LOG_QUERY_RESULT_HEADER_BYTES
        .checked_add(tenant_len)
        .filter(|end| *end <= payload.len())
        .ok_or_else(|| NativeProtocolError::new("native log query result tenant is truncated"))?;
    let tenant = std::str::from_utf8(&payload[LOG_QUERY_RESULT_HEADER_BYTES..end])
        .map_err(|_| NativeProtocolError::new("native log query result tenant is not UTF-8"))?;
    if tenant.is_empty() {
        return Err(NativeProtocolError::new(
            "native log query result tenant must not be empty",
        ));
    }
    Ok(NativeLogQueryResultInfo {
        tenant,
        record_count: u32::from_le_bytes(payload[8..12].try_into().expect("fixed range")),
    })
}

/// Encodes records with labels stored once per stream.
pub fn encode_native_log_query_result(
    tenant: &str,
    entries: Vec<LokiEntry>,
) -> Result<Vec<u8>, NativeProtocolError> {
    validate_tenant(tenant)?;
    let record_count = u32::try_from(entries.len())
        .map_err(|_| NativeProtocolError::new("native batch contains more than u32 records"))?;
    // Native indexed queries usually return one stream. Avoid hashing and
    // comparing a BTreeMap for every record in that case; labels are still
    // checked before taking ownership, so the fast path preserves exact
    // stream grouping and the same negative-timestamp validation as the
    // general path.
    let single_stream = entries.first().is_some_and(|first| {
        first.timestamp_unix_nanos >= 0
            && entries[1..]
                .iter()
                .all(|entry| entry.timestamp_unix_nanos >= 0 && entry.labels == first.labels)
    });
    let streams = if single_stream {
        let mut entries = entries.into_iter();
        let mut first = entries.next().expect("single-stream result is nonempty");
        let labels = std::mem::take(&mut first.labels);
        let mut stream_entries = Vec::with_capacity(record_count as usize);
        stream_entries.push(first);
        for mut entry in entries {
            drop(std::mem::take(&mut entry.labels));
            stream_entries.push(entry);
        }
        vec![(labels, stream_entries)]
    } else {
        let mut streams = HashMap::<BTreeMap<String, String>, Vec<LokiEntry>>::with_capacity(
            entries.len().min(MAX_STREAMS),
        );
        for mut entry in entries {
            if entry.timestamp_unix_nanos < 0 {
                return Err(NativeProtocolError::new(
                    "negative native log timestamps are unsupported",
                ));
            }
            // Move the labels into the grouping key. The old implementation
            // cloned every map before insertion, although only one copy per
            // stream is emitted on the wire.
            let labels = std::mem::take(&mut entry.labels);
            streams.entry(labels).or_default().push(entry);
        }
        streams.into_iter().collect::<Vec<_>>()
    };
    if streams.len() > MAX_STREAMS {
        return Err(NativeProtocolError::new(
            "native batch contains too many streams",
        ));
    }
    let mut streams = streams.into_iter().collect::<Vec<_>>();
    // Keep the wire representation deterministic while using hash lookup for
    // the common case where many entries share one stream label map.
    streams.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut estimated_bytes = LOG_QUERY_RESULT_HEADER_BYTES.saturating_add(tenant.len());
    for (labels, entries) in &streams {
        estimated_bytes = estimated_bytes.saturating_add(8);
        for (key, value) in labels {
            estimated_bytes = estimated_bytes
                .saturating_add(4)
                .saturating_add(key.len())
                .saturating_add(value.len());
        }
        for entry in entries {
            estimated_bytes = estimated_bytes
                .saturating_add(16)
                .saturating_add(entry.line.len());
            for (key, value) in &entry.structured_metadata {
                estimated_bytes = estimated_bytes
                    .saturating_add(4)
                    .saturating_add(key.len())
                    .saturating_add(value.len());
            }
        }
    }
    let mut encoded = Vec::with_capacity(estimated_bytes.min(MAX_NATIVE_FRAME_BYTES));
    encoded.extend_from_slice(&LOG_QUERY_RESULT_MAGIC);
    put_u16(&mut encoded, tenant.len(), "tenant")?;
    put_u16(&mut encoded, streams.len(), "stream count")?;
    encoded.extend_from_slice(&record_count.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    encoded.extend_from_slice(tenant.as_bytes());

    for (labels, entries) in streams {
        if labels.len() > MAX_LABELS_PER_STREAM {
            return Err(NativeProtocolError::new(
                "native stream contains too many labels",
            ));
        }
        put_u16(&mut encoded, labels.len(), "label count")?;
        encoded.extend_from_slice(&0_u16.to_le_bytes());
        let entry_count = u32::try_from(entries.len())
            .map_err(|_| NativeProtocolError::new("native stream contains too many entries"))?;
        encoded.extend_from_slice(&entry_count.to_le_bytes());
        for (key, value) in labels {
            put_string16(&mut encoded, &key, "label key")?;
            put_string16(&mut encoded, &value, "label value")?;
        }
        for entry in entries {
            encoded.extend_from_slice(&(entry.timestamp_unix_nanos as u64).to_le_bytes());
            put_u32(&mut encoded, entry.line.len(), "log line")?;
            if entry.structured_metadata.len() > MAX_METADATA_PER_ENTRY {
                return Err(NativeProtocolError::new(
                    "native entry contains too much structured metadata",
                ));
            }
            put_u16(
                &mut encoded,
                entry.structured_metadata.len(),
                "metadata count",
            )?;
            encoded.extend_from_slice(&0_u16.to_le_bytes());
            encoded.extend_from_slice(entry.line.as_bytes());
            for (key, value) in entry.structured_metadata {
                put_string16(&mut encoded, &key, "metadata key")?;
                put_string16(&mut encoded, &value, "metadata value")?;
            }
        }
    }
    if encoded.len() > MAX_NATIVE_FRAME_BYTES {
        return Err(NativeProtocolError::new(format!(
            "native batch is {} bytes, exceeding {MAX_NATIVE_FRAME_BYTES}",
            encoded.len()
        )));
    }
    Ok(encoded)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct NativeStreamKey(Vec<(Arc<str>, Arc<str>)>);

struct NativeProjectedLog {
    timestamp_unix_nanos: u64,
    message: Arc<str>,
    metadata: Vec<(Arc<str>, Arc<str>)>,
}

fn normalize_projected_fields(
    mut fields: Vec<(Arc<str>, Arc<str>, usize)>,
) -> Vec<(Arc<str>, Arc<str>)> {
    fields.sort_unstable_by(|left, right| left.0.cmp(&right.0).then_with(|| left.2.cmp(&right.2)));
    let mut normalized = Vec::with_capacity(fields.len());
    for (key, value, _) in fields {
        if let Some((existing_key, existing_value)) = normalized.last_mut()
            && *existing_key == key
        {
            *existing_value = value;
        } else {
            normalized.push((key, value));
        }
    }
    normalized
}

/// Encodes projected indexed records without materializing one Loki label and
/// metadata map per result. The native server uses this for the common query
/// path; delete-filtered queries continue through the public Loki entry path.
pub(crate) fn encode_native_log_query_matches(
    tenant: &str,
    matches: Vec<crate::LogMatch>,
) -> Result<Vec<u8>, NativeProtocolError> {
    validate_tenant(tenant)?;
    let record_count = u32::try_from(matches.len())
        .map_err(|_| NativeProtocolError::new("native batch contains more than u32 records"))?;
    let mut streams = HashMap::<NativeStreamKey, Vec<NativeProjectedLog>>::with_capacity(
        matches.len().min(MAX_STREAMS),
    );
    for matched in matches {
        let record = matched.record;
        let mut labels = Vec::new();
        let mut metadata = Vec::new();
        for (index, field) in record.fields.iter().enumerate() {
            if field.key.as_ref().starts_with(NATIVE_LABEL_PREFIX) {
                labels.push((Arc::clone(&field.key), Arc::clone(&field.value), index));
            } else if field.key.as_ref().starts_with(NATIVE_METADATA_PREFIX) {
                metadata.push((Arc::clone(&field.key), Arc::clone(&field.value), index));
            }
        }
        streams
            .entry(NativeStreamKey(normalize_projected_fields(labels)))
            .or_default()
            .push(NativeProjectedLog {
                timestamp_unix_nanos: record.timestamp_unix_nanos,
                message: record.message,
                metadata: normalize_projected_fields(metadata),
            });
    }
    if streams.len() > MAX_STREAMS {
        return Err(NativeProtocolError::new(
            "native batch contains too many streams",
        ));
    }
    let streams = streams.into_iter().collect::<Vec<_>>();

    let mut estimated_bytes = LOG_QUERY_RESULT_HEADER_BYTES.saturating_add(tenant.len());
    for (NativeStreamKey(labels), entries) in &streams {
        estimated_bytes = estimated_bytes.saturating_add(8);
        for (key, value) in labels {
            let key = key
                .as_ref()
                .strip_prefix(NATIVE_LABEL_PREFIX)
                .expect("native projected label prefix");
            estimated_bytes = estimated_bytes
                .saturating_add(4)
                .saturating_add(key.len())
                .saturating_add(value.len());
        }
        for entry in entries {
            estimated_bytes = estimated_bytes
                .saturating_add(16)
                .saturating_add(entry.message.len());
            for (key, value) in &entry.metadata {
                let key = key
                    .as_ref()
                    .strip_prefix(NATIVE_METADATA_PREFIX)
                    .expect("native projected metadata prefix");
                estimated_bytes = estimated_bytes
                    .saturating_add(4)
                    .saturating_add(key.len())
                    .saturating_add(value.len());
            }
        }
    }

    let mut encoded = Vec::with_capacity(estimated_bytes.min(MAX_NATIVE_FRAME_BYTES));
    encoded.extend_from_slice(&LOG_QUERY_RESULT_MAGIC);
    put_u16(&mut encoded, tenant.len(), "tenant")?;
    put_u16(&mut encoded, streams.len(), "stream count")?;
    encoded.extend_from_slice(&record_count.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    encoded.extend_from_slice(tenant.as_bytes());

    for (NativeStreamKey(labels), entries) in streams {
        if labels.len() > MAX_LABELS_PER_STREAM {
            return Err(NativeProtocolError::new(
                "native stream contains too many labels",
            ));
        }
        put_u16(&mut encoded, labels.len(), "label count")?;
        encoded.extend_from_slice(&0_u16.to_le_bytes());
        let entry_count = u32::try_from(entries.len())
            .map_err(|_| NativeProtocolError::new("native stream contains too many entries"))?;
        encoded.extend_from_slice(&entry_count.to_le_bytes());
        for (key, value) in labels {
            let key = key
                .as_ref()
                .strip_prefix(NATIVE_LABEL_PREFIX)
                .expect("native projected label prefix");
            put_string16(&mut encoded, key, "label key")?;
            put_string16(&mut encoded, &value, "label value")?;
        }
        for entry in entries {
            encoded.extend_from_slice(&entry.timestamp_unix_nanos.to_le_bytes());
            put_u32(&mut encoded, entry.message.len(), "log line")?;
            if entry.metadata.len() > MAX_METADATA_PER_ENTRY {
                return Err(NativeProtocolError::new(
                    "native entry contains too much structured metadata",
                ));
            }
            encoded.extend_from_slice(&(entry.metadata.len() as u16).to_le_bytes());
            encoded.extend_from_slice(&0_u16.to_le_bytes());
            encoded.extend_from_slice(entry.message.as_bytes());
            for (key, value) in entry.metadata {
                let key = key
                    .as_ref()
                    .strip_prefix(NATIVE_METADATA_PREFIX)
                    .expect("native projected metadata prefix");
                put_string16(&mut encoded, key, "metadata key")?;
                put_string16(&mut encoded, &value, "metadata value")?;
            }
        }
    }
    if encoded.len() > MAX_NATIVE_FRAME_BYTES {
        return Err(NativeProtocolError::new(format!(
            "native batch is {} bytes, exceeding {MAX_NATIVE_FRAME_BYTES}",
            encoded.len()
        )));
    }
    Ok(encoded)
}

/// Decodes and fully validates a grouped native log batch.
pub fn decode_native_log_query_result(
    payload: &[u8],
) -> Result<NativeLogQueryResult, NativeProtocolError> {
    let info = inspect_native_log_query_result(payload)?;
    let stream_count = usize::from(u16::from_le_bytes(
        payload[6..8].try_into().expect("fixed range"),
    ));
    let mut cursor = Cursor::at(payload, LOG_QUERY_RESULT_HEADER_BYTES + info.tenant.len());
    let mut entries = Vec::with_capacity(info.record_count as usize);
    for _ in 0..stream_count {
        let label_count = usize::from(cursor.u16("label count")?);
        if label_count > MAX_LABELS_PER_STREAM {
            return Err(NativeProtocolError::new(
                "native stream contains too many labels",
            ));
        }
        if cursor.u16("stream reserved bytes")? != 0 {
            return Err(NativeProtocolError::new(
                "native stream reserved bytes must be zero",
            ));
        }
        let entry_count = cursor.u32("entry count")? as usize;
        let mut labels = BTreeMap::new();
        for _ in 0..label_count {
            let key = cursor.string16("label key")?.to_owned();
            let value = cursor.string16("label value")?.to_owned();
            if key.is_empty() || labels.insert(key, value).is_some() {
                return Err(NativeProtocolError::new(
                    "native stream contains an empty or duplicate label",
                ));
            }
        }
        entries
            .len()
            .checked_add(entry_count)
            .filter(|count| *count <= info.record_count as usize)
            .ok_or_else(|| {
                NativeProtocolError::new("native stream counts exceed declared record count")
            })?;
        for _ in 0..entry_count {
            let timestamp = cursor.u64("timestamp")?;
            let line_len = cursor.u32("line length")? as usize;
            let metadata_count = usize::from(cursor.u16("metadata count")?);
            if metadata_count > MAX_METADATA_PER_ENTRY {
                return Err(NativeProtocolError::new(
                    "native entry contains too much structured metadata",
                ));
            }
            if cursor.u16("entry reserved bytes")? != 0 {
                return Err(NativeProtocolError::new(
                    "native entry reserved bytes must be zero",
                ));
            }
            let line = cursor.string(line_len, "log line")?.to_owned();
            let mut structured_metadata = BTreeMap::new();
            for _ in 0..metadata_count {
                let key = cursor.string16("metadata key")?.to_owned();
                let value = cursor.string16("metadata value")?.to_owned();
                if key.is_empty() || structured_metadata.insert(key, value).is_some() {
                    return Err(NativeProtocolError::new(
                        "native entry contains empty or duplicate metadata",
                    ));
                }
            }
            let timestamp_unix_nanos = i64::try_from(timestamp).map_err(|_| {
                NativeProtocolError::new("native log timestamp exceeds the signed i64 range")
            })?;
            entries.push(LokiEntry {
                timestamp_unix_nanos,
                labels: labels.clone(),
                line,
                structured_metadata,
            });
        }
    }
    if entries.len() != info.record_count as usize {
        return Err(NativeProtocolError::new(format!(
            "native batch decoded {} records, expected {}",
            entries.len(),
            info.record_count
        )));
    }
    cursor.finish()?;
    Ok(NativeLogQueryResult {
        tenant: info.tenant.to_owned(),
        entries,
    })
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

/// Encodes one bounded native metric query.
pub fn encode_native_metric_query(
    query: &crate::MetricQuery,
) -> Result<Vec<u8>, NativeProtocolError> {
    encode_messagepack(METRIC_QUERY_MAGIC, query, "metric query")
}

/// Decodes one bounded native metric query.
pub fn decode_native_metric_query(
    payload: &[u8],
) -> Result<crate::MetricQuery, NativeProtocolError> {
    decode_messagepack(METRIC_QUERY_MAGIC, payload, "metric query")
}

/// Encodes native metric query results.
pub fn encode_native_metric_query_result(
    points: &[crate::DurableMetricPoint],
) -> Result<Vec<u8>, NativeProtocolError> {
    encode_messagepack(METRIC_QUERY_RESULT_MAGIC, points, "metric query result")
}

/// Decodes native metric query results.
pub fn decode_native_metric_query_result(
    payload: &[u8],
) -> Result<Vec<crate::DurableMetricPoint>, NativeProtocolError> {
    decode_messagepack(METRIC_QUERY_RESULT_MAGIC, payload, "metric query result")
}

/// Encodes one bounded native trace query.
pub fn encode_native_trace_query(
    query: &crate::TraceQuery,
) -> Result<Vec<u8>, NativeProtocolError> {
    encode_messagepack(TRACE_QUERY_MAGIC, query, "trace query")
}

/// Decodes one bounded native trace query.
pub fn decode_native_trace_query(payload: &[u8]) -> Result<crate::TraceQuery, NativeProtocolError> {
    decode_messagepack(TRACE_QUERY_MAGIC, payload, "trace query")
}

/// Encodes native trace query results.
pub fn encode_native_trace_query_result(
    spans: &[crate::DurableSpan],
) -> Result<Vec<u8>, NativeProtocolError> {
    encode_messagepack(TRACE_QUERY_RESULT_MAGIC, spans, "trace query result")
}

/// Decodes native trace query results.
pub fn decode_native_trace_query_result(
    payload: &[u8],
) -> Result<Vec<crate::DurableSpan>, NativeProtocolError> {
    decode_messagepack(TRACE_QUERY_RESULT_MAGIC, payload, "trace query result")
}

/// Encodes native server capabilities.
pub fn encode_native_capabilities(
    capabilities: &NativeCapabilities,
) -> Result<Vec<u8>, NativeProtocolError> {
    encode_messagepack(CAPABILITIES_MAGIC, capabilities, "capabilities")
}

/// Decodes native server capabilities.
pub fn decode_native_capabilities(
    payload: &[u8],
) -> Result<NativeCapabilities, NativeProtocolError> {
    decode_messagepack(CAPABILITIES_MAGIC, payload, "capabilities")
}

fn encode_messagepack<T: serde::Serialize + ?Sized>(
    magic: [u8; 4],
    value: &T,
    kind: &str,
) -> Result<Vec<u8>, NativeProtocolError> {
    // Write the discriminator and MessagePack value into one buffer. Using
    // `to_vec` first would allocate a temporary payload and copy it again
    // after prepending the native type tag on every query response.
    let mut payload = Vec::with_capacity(magic.len() + 128);
    payload.extend_from_slice(&magic);
    rmp_serde::encode::write(&mut payload, value).map_err(|error| {
        NativeProtocolError::new(format!("native {kind} encoding failed: {error}"))
    })?;
    if payload.len() > MAX_NATIVE_FRAME_BYTES {
        return Err(NativeProtocolError::new(format!(
            "native {kind} exceeds the frame limit"
        )));
    }
    Ok(payload)
}

fn decode_messagepack<T: serde::de::DeserializeOwned>(
    magic: [u8; 4],
    payload: &[u8],
    kind: &str,
) -> Result<T, NativeProtocolError> {
    let Some(encoded) = payload.strip_prefix(&magic) else {
        return Err(NativeProtocolError::new(format!(
            "invalid native {kind} magic"
        )));
    };
    rmp_serde::from_slice(encoded)
        .map_err(|error| NativeProtocolError::new(format!("invalid native {kind}: {error}")))
}

/// Encodes an indexed native query.
pub fn encode_native_query(query: &NativeQuery) -> Result<Vec<u8>, NativeProtocolError> {
    validate_query(query)?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&QUERY_MAGIC);
    put_u16(&mut encoded, query.tenant.len(), "query tenant")?;
    put_u16(&mut encoded, query.labels.len(), "query label count")?;
    put_u16(&mut encoded, query.terms.len(), "query term count")?;
    encoded.push(match query.direction {
        NativeQueryDirection::OldestFirst => 0,
        NativeQueryDirection::NewestFirst => 1,
    });
    encoded.push(0);
    encoded.extend_from_slice(&query.limit.to_le_bytes());
    encoded.extend_from_slice(
        &query
            .start_timestamp_unix_nanos
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    encoded.extend_from_slice(
        &query
            .end_timestamp_unix_nanos
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    encoded.extend_from_slice(query.tenant.as_bytes());
    for (key, value) in &query.labels {
        put_string16(&mut encoded, key, "query label key")?;
        put_string16(&mut encoded, value, "query label value")?;
    }
    for term in &query.terms {
        put_string16(&mut encoded, term, "query term")?;
    }
    Ok(encoded)
}

/// Decodes and validates an indexed native query.
pub fn decode_native_query(payload: &[u8]) -> Result<NativeQuery, NativeProtocolError> {
    if payload.len() < QUERY_HEADER_BYTES || payload[0..4] != QUERY_MAGIC {
        return Err(NativeProtocolError::new("invalid native query header"));
    }
    let tenant_len = usize::from(u16::from_le_bytes(
        payload[4..6].try_into().expect("fixed range"),
    ));
    let label_count = usize::from(u16::from_le_bytes(
        payload[6..8].try_into().expect("fixed range"),
    ));
    let term_count = usize::from(u16::from_le_bytes(
        payload[8..10].try_into().expect("fixed range"),
    ));
    let direction = match payload[10] {
        0 => NativeQueryDirection::OldestFirst,
        1 => NativeQueryDirection::NewestFirst,
        value => {
            return Err(NativeProtocolError::new(format!(
                "unsupported native query direction {value}"
            )));
        }
    };
    if payload[11] != 0 {
        return Err(NativeProtocolError::new(
            "native query reserved byte must be zero",
        ));
    }
    let limit = u32::from_le_bytes(payload[12..16].try_into().expect("fixed range"));
    let start = u64::from_le_bytes(payload[16..24].try_into().expect("fixed range"));
    let end = u64::from_le_bytes(payload[24..32].try_into().expect("fixed range"));
    let mut cursor = Cursor::at(payload, QUERY_HEADER_BYTES);
    let tenant = cursor.string(tenant_len, "query tenant")?.to_owned();
    let mut labels = BTreeMap::new();
    for _ in 0..label_count {
        let key = cursor.string16("query label key")?.to_owned();
        let value = cursor.string16("query label value")?.to_owned();
        if key.is_empty() || labels.insert(key, value).is_some() {
            return Err(NativeProtocolError::new(
                "native query contains an empty or duplicate label",
            ));
        }
    }
    let mut terms = Vec::with_capacity(term_count);
    for _ in 0..term_count {
        let term = cursor.string16("query term")?.to_owned();
        if term.is_empty() {
            return Err(NativeProtocolError::new(
                "native query terms must not be empty",
            ));
        }
        terms.push(term);
    }
    cursor.finish()?;
    let query = NativeQuery {
        tenant,
        labels,
        terms,
        start_timestamp_unix_nanos: (start != u64::MAX).then_some(start),
        end_timestamp_unix_nanos: (end != u64::MAX).then_some(end),
        limit,
        direction,
    };
    validate_query(&query)?;
    Ok(query)
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

fn validate_tenant(tenant: &str) -> Result<(), NativeProtocolError> {
    if tenant.is_empty() {
        return Err(NativeProtocolError::new("native tenant must not be empty"));
    }
    if tenant.len() > MAX_TENANT_BYTES || tenant.len() > usize::from(u16::MAX) {
        return Err(NativeProtocolError::new(
            "native tenant exceeds its length limit",
        ));
    }
    Ok(())
}

fn validate_query(query: &NativeQuery) -> Result<(), NativeProtocolError> {
    validate_tenant(&query.tenant)?;
    if query.labels.len() > MAX_LABELS_PER_STREAM {
        return Err(NativeProtocolError::new(
            "native query contains too many labels",
        ));
    }
    if query.terms.len() > MAX_QUERY_TERMS {
        return Err(NativeProtocolError::new(
            "native query contains too many terms",
        ));
    }
    if query.limit == 0 || query.limit > MAX_QUERY_LIMIT {
        return Err(NativeProtocolError::new(format!(
            "native query limit must be in 1..={MAX_QUERY_LIMIT}"
        )));
    }
    if let (Some(start), Some(end)) = (
        query.start_timestamp_unix_nanos,
        query.end_timestamp_unix_nanos,
    ) && start >= end
    {
        return Err(NativeProtocolError::new(
            "native query timestamp range must be nonempty",
        ));
    }
    for (key, value) in &query.labels {
        if key.is_empty() {
            return Err(NativeProtocolError::new(
                "native query label keys must not be empty",
            ));
        }
        validate_string16(key, "query label key")?;
        validate_string16(value, "query label value")?;
    }
    for term in &query.terms {
        if term.is_empty() {
            return Err(NativeProtocolError::new(
                "native query terms must not be empty",
            ));
        }
        validate_string16(term, "query term")?;
    }
    Ok(())
}

fn payload_checksum(payload: &[u8]) -> u32 {
    u32::from_le_bytes(
        blake3::hash(payload).as_bytes()[0..4]
            .try_into()
            .expect("fixed range"),
    )
}

fn put_u16(
    encoded: &mut Vec<u8>,
    value: usize,
    field: &'static str,
) -> Result<(), NativeProtocolError> {
    let value = u16::try_from(value)
        .map_err(|_| NativeProtocolError::new(format!("{field} exceeds u16")))?;
    encoded.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn put_u32(
    encoded: &mut Vec<u8>,
    value: usize,
    field: &'static str,
) -> Result<(), NativeProtocolError> {
    let value = u32::try_from(value)
        .map_err(|_| NativeProtocolError::new(format!("{field} exceeds u32")))?;
    encoded.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn validate_string16(value: &str, field: &'static str) -> Result<(), NativeProtocolError> {
    u16::try_from(value.len())
        .map(|_| ())
        .map_err(|_| NativeProtocolError::new(format!("{field} exceeds u16")))
}

fn put_string16(
    encoded: &mut Vec<u8>,
    value: &str,
    field: &'static str,
) -> Result<(), NativeProtocolError> {
    put_u16(encoded, value.len(), field)?;
    encoded.extend_from_slice(value.as_bytes());
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn at(bytes: &'a [u8], offset: usize) -> Self {
        Self { bytes, offset }
    }

    fn bytes(&mut self, len: usize, field: &'static str) -> Result<&'a [u8], NativeProtocolError> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| NativeProtocolError::new(format!("native {field} is truncated")))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, NativeProtocolError> {
        Ok(u16::from_le_bytes(
            self.bytes(2, field)?.try_into().expect("fixed range"),
        ))
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, NativeProtocolError> {
        Ok(u32::from_le_bytes(
            self.bytes(4, field)?.try_into().expect("fixed range"),
        ))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, NativeProtocolError> {
        Ok(u64::from_le_bytes(
            self.bytes(8, field)?.try_into().expect("fixed range"),
        ))
    }

    fn u128(&mut self, field: &'static str) -> Result<u128, NativeProtocolError> {
        Ok(u128::from_le_bytes(
            self.bytes(16, field)?.try_into().expect("fixed range"),
        ))
    }

    fn string(&mut self, len: usize, field: &'static str) -> Result<&'a str, NativeProtocolError> {
        std::str::from_utf8(self.bytes(len, field)?)
            .map_err(|_| NativeProtocolError::new(format!("native {field} is not UTF-8")))
    }

    fn string16(&mut self, field: &'static str) -> Result<&'a str, NativeProtocolError> {
        let len = usize::from(self.u16(field)?);
        self.string(len, field)
    }

    fn finish(self) -> Result<(), NativeProtocolError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(NativeProtocolError::new(
                "native payload contains trailing bytes",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_aware_batch_and_partition_ack_round_trip() {
        let topic_partition =
            TopicPartition::new(crate::TRACES_TOPIC_ID, LogicalPartitionId::new(7));
        let batch = NativeTelemetryBatch {
            partitions: vec![NativePartitionAppend {
                topic_partition,
                envelope: TelemetryEnvelope::new(
                    crate::TelemetrySignal::Traces,
                    "tenant-a",
                    2,
                    &b"route"[..],
                    &b"payload"[..],
                )
                .unwrap(),
                transient_context: Some(Arc::<[u8]>::from(&b"context"[..])),
            }],
        };
        let encoded = batch.encode().unwrap();
        assert_eq!(NativeTelemetryBatch::decode(&encoded).unwrap(), batch);
        let (ranged, wire_ranges) = NativeTelemetryBatch::decode_with_envelope_ranges(&encoded)
            .expect("all append wire ranges");
        assert_eq!(ranged, batch);
        assert_eq!(wire_ranges.len(), 1);
        assert_eq!(
            &encoded[wire_ranges[0].0.clone()],
            batch.partitions[0].envelope.encode().unwrap()
        );
        assert_eq!(
            wire_ranges[0]
                .1
                .as_ref()
                .map(|range| &encoded[range.clone()]),
            Some(&b"context"[..])
        );
        let (decoded, envelope_range) =
            NativeTelemetryBatch::decode_native_append_with_envelope_range(&encoded)
                .expect("single append range");
        assert_eq!(decoded, batch);
        assert_eq!(
            &encoded[envelope_range.clone()],
            batch.partitions[0].envelope.encode().unwrap().as_slice()
        );
        let (view, borrowed_envelope_range, transient_range) =
            NativeTelemetryBatch::decode_native_append_view_with_ranges(&encoded)
                .expect("borrowed single append view");
        assert_eq!(view.topic_partition, topic_partition);
        assert_eq!(view.signal, crate::TelemetrySignal::Traces);
        assert_eq!(view.tenant, "tenant-a");
        assert_eq!(view.item_count, 2);
        assert_eq!(borrowed_envelope_range, envelope_range);
        assert_eq!(
            transient_range.map(|range| &encoded[range]),
            Some(&b"context"[..])
        );

        let acknowledgement = NativeTelemetryAppendAck {
            partitions: vec![NativePartitionAck {
                topic_partition,
                first_offset: 10,
                last_offset: 11,
            }],
        };
        assert_eq!(
            NativeTelemetryAppendAck::decode(&acknowledgement.encode().unwrap()).unwrap(),
            acknowledgement
        );
    }

    fn entries() -> Vec<LokiEntry> {
        vec![
            LokiEntry {
                timestamp_unix_nanos: 10,
                labels: BTreeMap::from([
                    ("app".to_owned(), "api".to_owned()),
                    ("region".to_owned(), "東京".to_owned()),
                ]),
                line: "request café".to_owned(),
                structured_metadata: BTreeMap::from([("trace_id".to_owned(), "abc".to_owned())]),
            },
            LokiEntry {
                timestamp_unix_nanos: 11,
                labels: BTreeMap::from([
                    ("app".to_owned(), "api".to_owned()),
                    ("region".to_owned(), "東京".to_owned()),
                ]),
                line: "request complete".to_owned(),
                structured_metadata: BTreeMap::new(),
            },
        ]
    }

    #[test]
    fn log_query_result_sorts_streams_after_hash_grouping() {
        let entries = vec![
            LokiEntry {
                timestamp_unix_nanos: 10,
                labels: BTreeMap::from([("app".to_owned(), "worker".to_owned())]),
                line: "worker".to_owned(),
                structured_metadata: BTreeMap::new(),
            },
            LokiEntry {
                timestamp_unix_nanos: 11,
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                line: "api".to_owned(),
                structured_metadata: BTreeMap::new(),
            },
        ];
        let mut reversed = entries.clone();
        reversed.reverse();
        assert_eq!(
            encode_native_log_query_result("tenant-a", entries).expect("entries encode"),
            encode_native_log_query_result("tenant-a", reversed).expect("reversed entries encode")
        );
    }

    #[test]
    fn log_query_result_round_trips_byte_exact_text_and_metadata() {
        let expected = entries();
        let encoded = encode_native_log_query_result("tenant-a", expected.clone())
            .expect("query result encodes");
        let decoded = decode_native_log_query_result(&encoded).expect("query result decodes");
        assert_eq!(decoded.tenant, "tenant-a");
        assert_eq!(decoded.entries, expected);
    }

    #[test]
    fn projected_log_query_result_matches_materialized_wire_encoding() {
        let topic_partition = TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(0));
        let labels = Arc::new(vec![
            crate::MetadataField::new("resource.loki.label.app", "api"),
            crate::MetadataField::new("resource.loki.label.region", "東京"),
            crate::MetadataField::new("attr.loki.metadata.trace_id", "abc"),
        ]);
        let mut first = crate::DurableLog::new(
            shard_stream_core::ShardId::new(0),
            topic_partition,
            shard_stream_core::LogicalOffset::new(0),
            10,
            "request café",
            crate::CompressionCohortId::new(0),
        );
        first.fields = Arc::clone(&labels);
        let mut second = crate::DurableLog::new(
            shard_stream_core::ShardId::new(0),
            topic_partition,
            shard_stream_core::LogicalOffset::new(1),
            11,
            "request complete",
            crate::CompressionCohortId::new(0),
        );
        second.fields = Arc::new(
            labels
                .iter()
                .filter(|field| field.key.as_ref() != "attr.loki.metadata.trace_id")
                .cloned()
                .collect(),
        );
        let projected = encode_native_log_query_matches(
            "tenant-a",
            vec![
                crate::LogMatch { record: first },
                crate::LogMatch { record: second },
            ],
        )
        .expect("projected result encodes");
        assert_eq!(
            projected,
            encode_native_log_query_result("tenant-a", entries()).unwrap()
        );
    }

    #[test]
    fn log_query_result_rejects_truncation_count_mismatch_and_trailing_bytes() {
        let encoded = encode_native_log_query_result("tenant-a", entries()).expect("query result");
        for end in 0..encoded.len() {
            assert!(
                decode_native_log_query_result(&encoded[..end]).is_err(),
                "{end}"
            );
        }
        let mut count = encoded.clone();
        count[8..12].copy_from_slice(&3_u32.to_le_bytes());
        assert!(decode_native_log_query_result(&count).is_err());
        let mut reserved = encoded.clone();
        reserved[12] = 1;
        assert!(decode_native_log_query_result(&reserved).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_native_log_query_result(&trailing).is_err());
    }

    #[test]
    fn frame_header_checks_magic_version_length_and_payload_checksum() {
        let frame = NativeFrame::request(NativeOpcode::Append, 42, vec![1, 2, 3]).expect("frame");
        let encoded = frame.encode();
        let header: [u8; NATIVE_FRAME_HEADER_BYTES] = encoded[..NATIVE_FRAME_HEADER_BYTES]
            .try_into()
            .expect("header");
        let decoded = NativeFrameHeader::decode(&header).expect("decode");
        assert_eq!(decoded.request_id, 42);
        decoded
            .verify_payload(&encoded[NATIVE_FRAME_HEADER_BYTES..])
            .expect("checksum");
        assert_eq!(
            decoded
                .verify_payload_and_hash(&encoded[NATIVE_FRAME_HEADER_BYTES..])
                .expect("checksum hash"),
            blake3::hash(&[1, 2, 3])
        );
        assert!(decoded.verify_payload(&[1, 2, 4]).is_err());

        let untracked =
            NativeFrame::request(NativeOpcode::AppendUntracked, 43, vec![1, 2, 3]).expect("frame");
        let untracked_header: [u8; NATIVE_FRAME_HEADER_BYTES] = untracked.encode()
            [..NATIVE_FRAME_HEADER_BYTES]
            .try_into()
            .expect("header");
        assert_eq!(
            NativeFrameHeader::decode(&untracked_header)
                .expect("decode")
                .opcode,
            NativeOpcode::AppendUntracked
        );

        let mut invalid = header;
        invalid[4] = 3;
        assert!(NativeFrameHeader::decode(&invalid).is_err());
    }

    #[test]
    fn indexed_query_round_trips_all_bounds_and_constraints() {
        let query = NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            terms: vec!["timeout".to_owned(), "error".to_owned()],
            start_timestamp_unix_nanos: Some(10),
            end_timestamp_unix_nanos: Some(20),
            limit: 100,
            direction: NativeQueryDirection::NewestFirst,
        };
        assert_eq!(
            decode_native_query(&encode_native_query(&query).expect("encode")).expect("decode"),
            query
        );
    }

    #[test]
    fn signal_native_queries_and_capabilities_round_trip() {
        let metric = crate::MetricQuery {
            tenant: std::sync::Arc::from("tenant-a"),
            name: Some(std::sync::Arc::from("requests_total")),
            limit: 10,
            ..crate::MetricQuery::default()
        };
        let mut expected_metric = METRIC_QUERY_MAGIC.to_vec();
        expected_metric.extend(rmp_serde::to_vec(&metric).expect("reference metric encoding"));
        assert_eq!(
            encode_native_metric_query(&metric).expect("encode"),
            expected_metric
        );
        assert_eq!(
            decode_native_metric_query(&encode_native_metric_query(&metric).expect("encode"))
                .expect("decode"),
            metric
        );
        let trace = crate::TraceQuery {
            tenant: std::sync::Arc::from("tenant-a"),
            name: Some(std::sync::Arc::from("checkout")),
            limit: 10,
            ..crate::TraceQuery::default()
        };
        assert_eq!(
            decode_native_trace_query(&encode_native_trace_query(&trace).expect("encode"))
                .expect("decode"),
            trace
        );
        let capabilities = NativeCapabilities {
            protocol_version: 1,
            append_v1: true,
            logical_partitions: [8, 8, 8],
            query_logs: true,
            query_metrics: true,
            query_traces: true,
            query_series: false,
            append_queryable: true,
        };
        assert_eq!(
            decode_native_capabilities(&encode_native_capabilities(&capabilities).expect("encode"))
                .expect("decode"),
            capabilities
        );
    }
}
