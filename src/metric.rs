//! Metric ownership: stable point and query models, chunk codec, stripe state, and query matching.
//! `metric/` holds codecs and operations; this root keeps the public-facing types and exports.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hash};
use std::mem::size_of;
use std::sync::Arc;

use foldhash::{HashMap, HashMapExt, HashSet};
use pco::ChunkConfig;
use pco::standalone::{simple_compress, simple_decompress_into};
use serde::{Deserialize, Serialize};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

use crate::{
    CorrelationBlockFilter, ResourceContext, ScopeContext, SeriesFingerprint, SignalTierPayload,
    SpanId, TelemetryAttribute, TelemetryError, TelemetryRecordRef, TelemetryResult,
    TelemetrySignal, TraceId, estimated_arc_str_bytes, estimated_arc_vec_storage,
    estimated_resource_context_bytes, estimated_scope_context_bytes,
    estimated_telemetry_attribute_bytes,
};

mod codec;
mod query_helpers;
mod stripe;
#[cfg(test)]
mod tests;
use codec::*;
pub use codec::{decode_metric_chunk, encode_metric_chunk};
pub use query_helpers::prometheus_string_labels;
use query_helpers::*;
pub(crate) use query_helpers::{metric_exact_series_point_matches, metric_query_matches};

const METRIC_CHUNK_MAGIC: [u8; 4] = *b"STMP";
const METRIC_CHUNK_VERSION: u8 = 1;
const METRIC_PCO_LEVEL: usize = 8;
const METRIC_SIDECAR_ZSTD_LEVEL: i32 = 1;
const DEFAULT_OUT_OF_ORDER_NANOS: u64 = 10 * 60 * 1_000_000_000;
const DEFAULT_CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_CHUNK_POINTS: usize = 4_096;
const DEFAULT_CHUNK_NANOS: u64 = 2 * 60 * 60 * 1_000_000_000;
const SERIES_ID_CACHE_ENTRIES: usize = 1_024;
// Resident chunk IDs are allocated across all series in a stripe.  A small
// fixed-entry cache keyed by resident ID avoids interleaved series evicting
// one another when the working set for one queried series is small.  Keep the
// lazy cache bounded by both entries and bytes.
const DECODED_METRIC_CACHE_ENTRIES: usize = 2_048;
const MAX_DECODED_METRIC_CACHE_BYTES: usize = 64 * 1024 * 1024;

thread_local! {
    static METRIC_COMPRESSOR: RefCell<zstd::bulk::Compressor<'static>> =
        RefCell::new(zstd::bulk::Compressor::new(METRIC_SIDECAR_ZSTD_LEVEL)
            .expect("metric zstd level is valid"));
    static METRIC_DECOMPRESSOR: RefCell<zstd::bulk::Decompressor<'static>> =
        RefCell::new(zstd::bulk::Decompressor::new()
            .expect("metric zstd decompressor initializes"));
}

/// Exact scalar number used by gauges, sums, and exemplars.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NumberValue {
    /// Signed integer sample.
    Integer(i64),
    /// Exact IEEE-754 double bits.
    DoubleBits(u64),
}

/// Exact integer or floating-point histogram count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistogramCount {
    /// Integer count used by OTLP and integer Prometheus native histograms.
    Integer(u64),
    /// Exact IEEE-754 count bits used by Prometheus float histograms.
    DoubleBits(u64),
}

impl NumberValue {
    /// Creates a bit-exact floating-point sample.
    #[must_use]
    pub const fn from_f64(value: f64) -> Self {
        Self::DoubleBits(value.to_bits())
    }
}

/// One metric exemplar nested under a point.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MetricExemplar {
    /// Filtered attributes in wire order.
    pub filtered_attributes: Arc<Vec<TelemetryAttribute>>,
    /// Exemplar timestamp.
    pub timestamp_unix_nanos: u64,
    /// Exact scalar value.
    pub value: NumberValue,
    /// Optional binary span ID.
    pub span_id: Option<SpanId>,
    /// Optional binary trace ID.
    pub trace_id: Option<TraceId>,
}

/// Explicit histogram point payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplicitHistogramValue {
    /// Number of observations.
    pub count: HistogramCount,
    /// Exact optional sum bits.
    pub sum_bits: Option<u64>,
    /// Bucket counts.
    pub bucket_counts: Arc<Vec<HistogramCount>>,
    /// Exact explicit-bound bits.
    pub explicit_bounds_bits: Arc<Vec<u64>>,
    /// Exact optional minimum bits.
    pub min_bits: Option<u64>,
    /// Exact optional maximum bits.
    pub max_bits: Option<u64>,
    /// Prometheus reset hint; zero for OTLP explicit histograms.
    pub reset_hint: i32,
}

/// Positive or negative exponential-histogram buckets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExponentialHistogramBuckets {
    /// Sparse bucket spans in wire order.
    pub spans: Arc<Vec<HistogramBucketSpan>>,
    /// Bucket counts corresponding to the concatenated spans.
    pub bucket_counts: Arc<Vec<HistogramCount>>,
}

/// One sparse native-histogram bucket span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistogramBucketSpan {
    /// Gap from the prior span, or starting bucket for the first span.
    pub offset: i32,
    /// Number of consecutive buckets.
    pub length: u32,
}

/// Exponential histogram point payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExponentialHistogramValue {
    /// Number of observations.
    pub count: HistogramCount,
    /// Exact optional sum bits.
    pub sum_bits: Option<u64>,
    /// Base-2 scale.
    pub scale: i32,
    /// Count of exact zero values.
    pub zero_count: HistogramCount,
    /// Positive buckets.
    pub positive: Option<ExponentialHistogramBuckets>,
    /// Negative buckets.
    pub negative: Option<ExponentialHistogramBuckets>,
    /// Exact optional minimum bits.
    pub min_bits: Option<u64>,
    /// Exact optional maximum bits.
    pub max_bits: Option<u64>,
    /// Exact zero-threshold bits.
    pub zero_threshold_bits: u64,
    /// Prometheus reset hint; zero for OTLP exponential histograms.
    pub reset_hint: i32,
}

/// One legacy summary quantile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryQuantileValue {
    /// Exact quantile bits.
    pub quantile_bits: u64,
    /// Exact value bits.
    pub value_bits: u64,
}

/// Legacy summary point payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryValue {
    /// Number of observations.
    pub count: u64,
    /// Exact sum bits.
    pub sum_bits: u64,
    /// Quantile values in wire order.
    pub quantiles: Arc<Vec<SummaryQuantileValue>>,
}

/// Signal-native metric point payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricValue {
    /// Gauge sample.
    Gauge(NumberValue),
    /// Sum sample. Temporality and monotonicity are part of series identity.
    Sum(NumberValue),
    /// Explicit histogram sample.
    ExplicitHistogram(ExplicitHistogramValue),
    /// Exponential histogram sample.
    ExponentialHistogram(ExponentialHistogramValue),
    /// Legacy summary sample.
    Summary(SummaryValue),
}

/// Metric instrument identity fields that are common to a series.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetricKind {
    /// Gauge.
    Gauge,
    /// Sum with raw OTLP temporality and monotonicity.
    Sum {
        /// OTLP aggregation temporality enum value.
        temporality: i32,
        /// Whether the sum is monotonic.
        monotonic: bool,
    },
    /// Explicit histogram with raw OTLP temporality.
    ExplicitHistogram {
        /// OTLP aggregation temporality enum value.
        temporality: i32,
    },
    /// Exponential histogram with raw OTLP temporality.
    ExponentialHistogram {
        /// OTLP aggregation temporality enum value.
        temporality: i32,
    },
    /// Legacy cumulative summary.
    Summary,
}

/// Canonical identity of one metric series.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MetricIdentity {
    /// Authenticated tenant.
    pub tenant: Arc<str>,
    /// Resource context.
    pub resource: Arc<ResourceContext>,
    /// Instrumentation scope context.
    pub scope: Arc<ScopeContext>,
    /// Metric name.
    pub name: Arc<str>,
    /// Metric unit.
    pub unit: Arc<str>,
    /// Metric kind, temporality, and monotonicity.
    pub kind: MetricKind,
    /// Exact point attributes. Their canonical sorted representation defines identity.
    pub point_attributes: Arc<Vec<TelemetryAttribute>>,
}

impl MetricIdentity {
    /// Returns the cross-signal identity of this series' resource context.
    #[must_use]
    pub fn resource_id(&self) -> crate::ResourceContextId {
        self.resource.id()
    }

    /// Returns the cross-signal identity of this series' instrumentation scope.
    #[must_use]
    pub fn scope_id(&self) -> crate::ScopeContextId {
        self.scope.id()
    }

    /// Computes the process-independent canonical series fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> SeriesFingerprint {
        let mut canonical = Vec::new();
        append_bytes(&mut canonical, self.tenant.as_bytes());
        self.resource.append_identity(&mut canonical);
        self.scope.append_identity(&mut canonical);
        append_bytes(&mut canonical, self.name.as_bytes());
        append_bytes(&mut canonical, self.unit.as_bytes());
        let kind = rmp_serde::to_vec(&self.kind).expect("in-memory metric kind serializes");
        append_bytes(&mut canonical, &kind);
        let mut attributes = self
            .point_attributes
            .iter()
            .map(|attribute| {
                let mut bytes = Vec::new();
                attribute.append_canonical(&mut bytes);
                bytes
            })
            .collect::<Vec<_>>();
        attributes.sort_unstable();
        for attribute in attributes {
            append_bytes(&mut canonical, &attribute);
        }
        SeriesFingerprint::from_canonical(&canonical)
    }
}

/// One durable raw metric point. Exactly one point consumes one logical offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableMetricPoint {
    /// Physical shard-stream owner stripe.
    pub stream_shard_id: ShardId,
    /// Durable signal-aware address.
    pub record_ref: TelemetryRecordRef,
    /// Canonical series identity.
    pub identity: Arc<MetricIdentity>,
    /// Description metadata, deliberately excluded from series identity.
    pub description: Arc<str>,
    /// Non-identifying OTLP metric metadata.
    pub metadata: Arc<Vec<TelemetryAttribute>>,
    /// Optional start timestamp.
    pub start_time_unix_nanos: u64,
    /// Required sample timestamp.
    pub timestamp_unix_nanos: u64,
    /// Point flags, including stale-marker flags and unknown future bits.
    pub flags: u32,
    /// Exact raw point payload.
    pub value: MetricValue,
    /// Nested exemplars.
    pub exemplars: Arc<Vec<MetricExemplar>>,
}

impl DurableMetricPoint {
    /// Returns this point's canonical series fingerprint.
    #[must_use]
    pub fn series_fingerprint(&self) -> SeriesFingerprint {
        self.identity.fingerprint()
    }

    /// Estimates resident head storage without serializing the point.
    ///
    /// This is deliberately conservative because it bounds memory admission;
    /// it is not a wire-size calculation.
    fn estimated_head_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(estimated_metric_identity_bytes(&self.identity))
            .saturating_add(estimated_arc_str_bytes(&self.description))
            .saturating_add(estimated_arc_attributes_bytes(&self.metadata))
            .saturating_add(estimated_metric_value_bytes(&self.value))
            .saturating_add(estimated_arc_exemplars_bytes(&self.exemplars))
    }
}

/// Ingestion semantics used for same-timestamp sample conflicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricIngestProtocol {
    /// OTLP overlaps use highest durable offset with diagnostics.
    Otlp,
    /// Prometheus Remote Write rejects conflicting same-timestamp values.
    RemoteWrite,
}

impl MetricIngestProtocol {
    pub(crate) const fn to_wire(self) -> u8 {
        match self {
            Self::Otlp => 1,
            Self::RemoteWrite => 2,
        }
    }

    pub(crate) const fn from_wire(value: u8) -> TelemetryResult<Self> {
        match value {
            1 => Ok(Self::Otlp),
            2 => Ok(Self::RemoteWrite),
            _ => Err(TelemetryError::InvalidTelemetryEnvelope(
                "unknown metric ingestion protocol",
            )),
        }
    }
}

/// Result of applying one raw metric point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricApplyOutcome {
    /// New point inserted.
    Inserted,
    /// Byte-identical retry ignored.
    Duplicate,
    /// OTLP conflict replaced by a higher durable offset.
    Replaced,
    /// Older OTLP conflict ignored.
    Obsolete,
    /// Accepted into the out-of-order delta region.
    OutOfOrder,
}

/// Checkpointed delta-to-cumulative state for PromQL views.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesAccumulatorCheckpoint {
    /// Canonical series fingerprint.
    pub series: SeriesFingerprint,
    /// Logical partition that owns the complete series.
    pub topic_partition: TopicPartition,
    /// Reset generation.
    pub reset_generation: u64,
    /// Latest raw timestamp.
    pub latest_timestamp_unix_nanos: u64,
    /// Exact cumulative value when the series is numeric.
    pub cumulative: Option<NumberValue>,
}

#[derive(Debug)]
struct SeriesHead {
    topic_partition: TopicPartition,
    identity: Arc<MetricIdentity>,
    points: BTreeMap<(u64, LogicalOffset), DurableMetricPoint>,
    latest_timestamp: u64,
    bytes: usize,
    reset_generation: u64,
    cumulative: Option<NumberValue>,
    conflicts: u64,
}

/// Bounded single-writer metric head and immutable raw chunk collection.
#[derive(Debug)]
pub struct MetricStripe {
    head_budget_bytes: usize,
    head_bytes: usize,
    out_of_order_nanos: u64,
    chunk_bytes: usize,
    chunk_points: usize,
    chunk_nanos: u64,
    series: HashMap<SeriesFingerprint, SeriesHead>,
    chunks: HashMap<SeriesFingerprint, Vec<SealedMetricChunk>>,
    pending_chunks: Vec<SignalTierPayload>,
    next_chunk_id: u64,
    recovered_accumulators: HashMap<SeriesFingerprint, SeriesAccumulatorCheckpoint>,
    name_index: HashMap<Arc<str>, HashSet<SeriesFingerprint>>,
    label_index: HashMap<(Arc<str>, Arc<str>), HashSet<SeriesFingerprint>>,
    identity_fingerprints: Vec<Option<CachedSeriesIdentity>>,
    decoded_chunks: RefCell<DecodedMetricCache>,
}

#[derive(Debug)]
struct CachedSeriesIdentity {
    hash: u64,
    identity: Arc<MetricIdentity>,
    fingerprint: SeriesFingerprint,
}

#[derive(Debug)]
struct SealedMetricChunk {
    resident_id: u64,
    min_timestamp_unix_nanos: u64,
    max_timestamp_unix_nanos: u64,
    payload: Arc<[u8]>,
}

#[derive(Debug)]
struct CachedDecodedMetricChunk {
    estimated_bytes: usize,
    points: Arc<[DurableMetricPoint]>,
}

#[derive(Debug)]
struct DecodedMetricCache {
    chunks: HashMap<u64, CachedDecodedMetricChunk>,
    max_bytes: usize,
    used_bytes: usize,
    hits: u64,
    misses: u64,
}

impl DecodedMetricCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            chunks: HashMap::new(),
            max_bytes,
            used_bytes: 0,
            hits: 0,
            misses: 0,
        }
    }

    fn get(&mut self, resident_id: u64) -> Option<Arc<[DurableMetricPoint]>> {
        if let Some(cached) = self.chunks.get(&resident_id) {
            self.hits = self.hits.saturating_add(1);
            return Some(Arc::clone(&cached.points));
        }
        self.misses = self.misses.saturating_add(1);
        None
    }

    fn insert(
        &mut self,
        resident_id: u64,
        estimated_bytes: usize,
        points: Arc<[DurableMetricPoint]>,
    ) {
        if estimated_bytes > self.max_bytes {
            return;
        }
        if let Some(previous) = self.chunks.remove(&resident_id) {
            self.used_bytes = self.used_bytes.saturating_sub(previous.estimated_bytes);
        }
        while self.used_bytes.saturating_add(estimated_bytes) > self.max_bytes
            || self.chunks.len() >= DECODED_METRIC_CACHE_ENTRIES
        {
            let Some(evicted_id) = self.chunks.keys().next().copied() else {
                break;
            };
            let previous = self
                .chunks
                .remove(&evicted_id)
                .expect("selected metric cache entry was present");
            self.used_bytes = self.used_bytes.saturating_sub(previous.estimated_bytes);
        }
        self.used_bytes = self.used_bytes.saturating_add(estimated_bytes);
        self.chunks.insert(
            resident_id,
            CachedDecodedMetricChunk {
                estimated_bytes,
                points,
            },
        );
    }

    fn remove(&mut self, resident_ids: &[u64]) {
        for resident_id in resident_ids {
            if let Some(removed) = self.chunks.remove(resident_id) {
                self.used_bytes = self.used_bytes.saturating_sub(removed.estimated_bytes);
            }
        }
    }
}

enum ExactMetricSource<'a> {
    Head(&'a SeriesHead),
    Chunk(&'a SealedMetricChunk),
}

impl ExactMetricSource<'_> {
    fn min_timestamp_unix_nanos(&self) -> u64 {
        match self {
            Self::Head(head) => head
                .points
                .first_key_value()
                .map_or(u64::MAX, |((timestamp, _), _)| *timestamp),
            Self::Chunk(chunk) => chunk.min_timestamp_unix_nanos,
        }
    }

    fn max_timestamp_unix_nanos(&self) -> u64 {
        match self {
            Self::Head(head) => head
                .points
                .last_key_value()
                .map_or(0, |((timestamp, _), _)| *timestamp),
            Self::Chunk(chunk) => chunk.max_timestamp_unix_nanos,
        }
    }
}

/// Native metric selector used by PromQL storage scans and direct APIs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricQuery {
    /// Required tenant.
    pub tenant: Arc<str>,
    /// Optional logical partition for bounded analytical scans.
    pub partition: Option<TopicPartition>,
    /// Optional inclusive durable-offset cursor. Applied after same-timestamp
    /// conflict resolution so pagination cannot expose an obsolete point.
    pub start_offset: Option<LogicalOffset>,
    /// Optional exact series fingerprint.
    pub series: Option<SeriesFingerprint>,
    /// Optional exact metric name.
    pub name: Option<Arc<str>>,
    /// Exact Prometheus labels pushed into the stripe-local inverted index.
    pub exact_labels: Arc<Vec<(Arc<str>, Arc<str>)>>,
    /// Inclusive start time.
    pub start_time_unix_nanos: Option<u64>,
    /// Inclusive end time.
    pub end_time_unix_nanos: Option<u64>,
    /// Maximum raw points to return.
    pub limit: usize,
}
