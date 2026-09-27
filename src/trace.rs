//! Trace ownership: span and query models, block codec, stripe state, and query matching.
//! `trace/` holds codecs and operations; this root keeps the public-facing types and exports.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::hash::Hash;
use std::mem::size_of;
use std::sync::Arc;

use foldhash::{HashMap, HashMapExt};
use pco::ChunkConfig;
use pco::standalone::{simple_compress, simple_decompress_into};
use serde::{Deserialize, Serialize};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

use crate::{
    CorrelationBlockFilter, ResourceContext, ResourceContextId, ScopeContext, SignalTierPayload,
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
pub(crate) use codec::{TraceProjection, decode_trace_block_matching};
pub use codec::{decode_trace_block, encode_trace_block};
pub(crate) use query_helpers::trace_query_matches;
use query_helpers::*;

const TRACE_BLOCK_MAGIC: [u8; 4] = *b"STSP";
const TRACE_BLOCK_VERSION: u8 = 1;
const TRACE_PCO_LEVEL: usize = 8;
const TRACE_SIDECAR_ZSTD_LEVEL: i32 = 1;
const TARGET_TRACE_BLOCK_SOURCE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_TRACE_IDLE_NANOS: u64 = 30_000_000_000;
const DEFAULT_LATE_TRACE_NANOS: u64 = 15 * 60 * 1_000_000_000;
const RESOURCE_ID_CACHE_ENTRIES: usize = 256;

thread_local! {
    static TRACE_COMPRESSOR: RefCell<zstd::bulk::Compressor<'static>> =
        RefCell::new(zstd::bulk::Compressor::new(TRACE_SIDECAR_ZSTD_LEVEL)
            .expect("trace zstd level is valid"));
    static TRACE_DECOMPRESSOR: RefCell<zstd::bulk::Decompressor<'static>> =
        RefCell::new(zstd::bulk::Decompressor::new()
            .expect("trace zstd decompressor initializes"));
}

/// Final OpenTelemetry span status.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanStatus {
    /// Status message.
    pub message: Arc<str>,
    /// OTLP status enum value, including unknown future values.
    pub code: i32,
}

/// One nested span event. It does not consume a shard-stream offset.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanEvent {
    /// Event timestamp in Unix nanoseconds.
    pub timestamp_unix_nanos: u64,
    /// Event name.
    pub name: Arc<str>,
    /// Exact typed event attributes.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Attributes dropped before export.
    pub dropped_attributes_count: u32,
}

/// One nested link to another span. It does not consume a shard-stream offset.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanLink {
    /// Linked trace ID.
    pub trace_id: TraceId,
    /// Linked span ID.
    pub span_id: SpanId,
    /// W3C trace state.
    pub trace_state: Arc<str>,
    /// Exact typed link attributes.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Attributes dropped before export.
    pub dropped_attributes_count: u32,
    /// OTLP link flags, including unknown future bits.
    pub flags: u32,
}

/// One durable OTLP span. Exactly one instance consumes one logical offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableSpan {
    /// Physical shard-stream owner stripe.
    pub stream_shard_id: ShardId,
    /// Durable signal-aware address.
    pub record_ref: TelemetryRecordRef,
    /// Authenticated tenant.
    pub tenant: Arc<str>,
    /// Exact resource context.
    pub resource: Arc<ResourceContext>,
    /// Exact instrumentation scope context.
    pub scope: Arc<ScopeContext>,
    /// Trace ID.
    pub trace_id: TraceId,
    /// Span ID.
    pub span_id: SpanId,
    /// Parent span ID, absent for a root.
    pub parent_span_id: Option<SpanId>,
    /// W3C trace state.
    pub trace_state: Arc<str>,
    /// OTLP span flags, including unknown future bits.
    pub flags: u32,
    /// Span operation name.
    pub name: Arc<str>,
    /// OTLP span kind enum value, including unknown future values.
    pub kind: i32,
    /// Start timestamp in Unix nanoseconds.
    pub start_time_unix_nanos: u64,
    /// Exact nonnegative duration in nanoseconds.
    pub duration_nanos: u64,
    /// Exact typed span attributes.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Attributes dropped before export.
    pub dropped_attributes_count: u32,
    /// Nested span events.
    pub events: Arc<Vec<SpanEvent>>,
    /// Events dropped before export.
    pub dropped_events_count: u32,
    /// Nested links.
    pub links: Arc<Vec<SpanLink>>,
    /// Links dropped before export.
    pub dropped_links_count: u32,
    /// Final status. `None` is distinct from an explicitly unset status.
    pub status: Option<SpanStatus>,
}

impl DurableSpan {
    /// Returns the cross-signal identity of this span's resource context.
    #[must_use]
    pub fn resource_id(&self) -> crate::ResourceContextId {
        self.resource.id()
    }

    /// Returns the cross-signal identity of this span's instrumentation scope.
    #[must_use]
    pub fn scope_id(&self) -> crate::ScopeContextId {
        self.scope.id()
    }

    /// Returns the exact end timestamp after checked duration reconstruction.
    #[must_use]
    pub fn end_time_unix_nanos(&self) -> Option<u64> {
        self.start_time_unix_nanos.checked_add(self.duration_nanos)
    }

    /// Estimates resident head storage without serializing the span.
    ///
    /// This is deliberately conservative because it bounds memory admission;
    /// it is not a wire-size calculation.
    fn estimated_head_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(estimated_arc_str_bytes(&self.tenant))
            .saturating_add(estimated_resource_context_bytes(&self.resource))
            .saturating_add(estimated_scope_context_bytes(&self.scope))
            .saturating_add(estimated_arc_str_bytes(&self.trace_state))
            .saturating_add(estimated_arc_str_bytes(&self.name))
            .saturating_add(estimated_span_attributes_bytes(&self.attributes))
            .saturating_add(estimated_span_events_bytes(&self.events))
            .saturating_add(estimated_span_links_bytes(&self.links))
            .saturating_add(
                self.status
                    .as_ref()
                    .map_or(0, |status| estimated_arc_str_bytes(&status.message)),
            )
    }
}

/// Immutable directory entry for one trace's block fragments and summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSummary {
    /// Trace ID.
    pub trace_id: TraceId,
    /// Tenant.
    pub tenant: Arc<str>,
    /// Earliest span start.
    pub start_time_unix_nanos: u64,
    /// Latest span end.
    pub end_time_unix_nanos: u64,
    /// Maximum observed span duration.
    pub max_duration_nanos: u64,
    /// Number of current winning spans.
    pub span_count: u32,
    /// Number of spans with error status.
    pub error_count: u32,
    /// Root span name when known.
    pub root_name: Option<Arc<str>>,
    /// Immutable block IDs containing current or late fragments.
    pub block_fragments: Arc<Vec<u64>>,
}

/// Trace-by-ID query and bounded search constraints.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceQuery {
    /// Required tenant.
    pub tenant: Arc<str>,
    /// Optional logical partition for bounded analytical scans.
    pub partition: Option<TopicPartition>,
    /// Optional inclusive durable-offset cursor. Applied after conflict
    /// resolution so pagination never resurrects an obsolete span version.
    pub start_offset: Option<LogicalOffset>,
    /// Exact trace ID for direct lookup.
    pub trace_id: Option<TraceId>,
    /// Exact span ID for indexed analytical filtering.
    pub span_id: Option<SpanId>,
    /// Exact span operation name.
    pub name: Option<Arc<str>>,
    /// Exact rendered span attributes.
    pub exact_attributes: Arc<Vec<(Arc<str>, Arc<str>)>>,
    /// Exact rendered resource attributes.
    pub exact_resource_attributes: Arc<Vec<(Arc<str>, Arc<str>)>>,
    /// Exact rendered scope attributes.
    pub exact_scope_attributes: Arc<Vec<(Arc<str>, Arc<str>)>>,
    /// Inclusive lower start-time bound.
    pub start_time_unix_nanos: Option<u64>,
    /// Exclusive upper start-time bound.
    pub end_time_unix_nanos: Option<u64>,
    /// Minimum span or trace duration.
    pub min_duration_nanos: Option<u64>,
    /// Maximum number of trace summaries returned.
    pub limit: usize,
}

/// In-memory view of the immutable trace directory.
#[derive(Debug, Default, Clone)]
pub struct TraceDirectory {
    entries: BTreeMap<(Arc<str>, TraceId), TraceSummary>,
}

impl TraceDirectory {
    /// Publishes one immutable fragment and merges it into the trace summary.
    ///
    /// Late fragments must extend the directory entry rather than replacing
    /// the earlier block list; otherwise a direct trace lookup can lose the
    /// only directory reference to spans that were already sealed.
    pub fn publish(&mut self, summary: TraceSummary) {
        let key = (Arc::clone(&summary.tenant), summary.trace_id);
        let Some(current) = self.entries.get_mut(&key) else {
            self.entries.insert(key, summary);
            return;
        };
        current.start_time_unix_nanos = current
            .start_time_unix_nanos
            .min(summary.start_time_unix_nanos);
        current.end_time_unix_nanos = current.end_time_unix_nanos.max(summary.end_time_unix_nanos);
        current.max_duration_nanos = current.max_duration_nanos.max(summary.max_duration_nanos);
        current.span_count = current.span_count.saturating_add(summary.span_count);
        current.error_count = current.error_count.saturating_add(summary.error_count);
        if current.root_name.is_none() {
            current.root_name = summary.root_name;
        }
        let mut fragments = current.block_fragments.as_ref().clone();
        fragments.extend(summary.block_fragments.iter().copied());
        fragments.sort_unstable();
        fragments.dedup();
        current.block_fragments = Arc::new(fragments);
    }

    fn publish_current(&mut self, mut summary: TraceSummary) {
        let key = (Arc::clone(&summary.tenant), summary.trace_id);
        if let Some(current) = self.entries.get(&key) {
            let mut fragments = current.block_fragments.as_ref().clone();
            fragments.extend(summary.block_fragments.iter().copied());
            fragments.sort_unstable();
            fragments.dedup();
            summary.block_fragments = Arc::new(fragments);
        }
        self.entries.insert(key, summary);
    }

    /// Executes direct-ID or bounded summary search without span materialization.
    #[must_use]
    pub fn query(&self, query: &TraceQuery) -> Vec<TraceSummary> {
        let limit = query.limit.max(1);
        self.entries
            .values()
            .filter(|entry| entry.tenant == query.tenant)
            .filter(|entry| query.trace_id.is_none_or(|value| value == entry.trace_id))
            .filter(|entry| {
                query
                    .start_time_unix_nanos
                    .is_none_or(|value| entry.end_time_unix_nanos >= value)
            })
            .filter(|entry| {
                query
                    .end_time_unix_nanos
                    .is_none_or(|value| entry.start_time_unix_nanos < value)
            })
            .filter(|entry| {
                query
                    .min_duration_nanos
                    .is_none_or(|value| entry.max_duration_nanos >= value)
            })
            .take(limit)
            .cloned()
            .collect()
    }
}

#[derive(Debug)]
struct HotTrace {
    spans: BTreeMap<SpanId, DurableSpan>,
    bytes: usize,
    last_append_nanos: u64,
    first_sealed_nanos: Option<u64>,
    conflicts: u64,
    retries: u64,
}

#[derive(Debug)]
struct RecentlySealedTrace {
    spans: BTreeMap<SpanId, DurableSpan>,
    bytes: usize,
    first_sealed_nanos: u64,
    last_sealed_nanos: u64,
    conflicts: u64,
    retries: u64,
}

type AnalyticalResourceKey = (Arc<str>, crate::ResourceContextId);
type AnalyticalResourceAttributeKey = (Arc<str>, Arc<str>, Arc<str>);

#[derive(Debug)]
struct AnalyticalResourceBucket {
    resource: Arc<ResourceContext>,
    spans: Vec<DurableSpan>,
}

type AnalyticalResourceRows = HashMap<AnalyticalResourceKey, Vec<AnalyticalResourceBucket>>;
type AnalyticalResourcePostings =
    HashMap<AnalyticalResourceAttributeKey, Vec<AnalyticalResourceKey>>;
type AnalyticalWinnerRows = HashMap<Arc<str>, HashMap<(TraceId, SpanId), LogicalOffset>>;

#[derive(Debug)]
struct CachedResourceIdentity {
    resource: Arc<ResourceContext>,
    id: ResourceContextId,
}

/// Result of applying one span to a stripe-local trace head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceApplyOutcome {
    /// A new span identity was inserted.
    Inserted,
    /// A byte-identical retransmission was ignored.
    Duplicate,
    /// A conflicting version replaced an older durable offset.
    Replaced,
    /// An older conflicting version was ignored.
    Obsolete,
}

/// Bounded, single-writer trace state owned by one physical stripe.
#[derive(Debug)]
pub struct TraceStripe {
    head_budget_bytes: usize,
    head_bytes: usize,
    idle_nanos: u64,
    late_grace_nanos: u64,
    traces: HashMap<(Arc<str>, TraceId), HotTrace>,
    recently_sealed: HashMap<(Arc<str>, TraceId), RecentlySealedTrace>,
    recent_order: VecDeque<(u64, (Arc<str>, TraceId))>,
    recently_sealed_bytes: usize,
    sealed_blocks: HashMap<u64, Arc<[u8]>>,
    pending_blocks: Vec<SignalTierPayload>,
    directory: TraceDirectory,
    next_block_id: u64,
    analytical_resources: RefCell<AnalyticalResourceRows>,
    analytical_resource_postings: AnalyticalResourcePostings,
    analytical_winners: AnalyticalWinnerRows,
    analytical_index_dirty: Cell<bool>,
    analytical_index_bytes: usize,
    analytical_index_budget_bytes: usize,
    analytical_index_complete: bool,
    resource_id_cache: Vec<Option<CachedResourceIdentity>>,
}

struct PreparedTrace {
    spans: Vec<DurableSpan>,
    summary_spans: Vec<DurableSpan>,
    replaces_summary: bool,
    source_bytes: usize,
}
