//! `correlation/` owns block filters, query matching, bounded postings, and tests.
//! Bounded stripe-local links between logs, traces, and metrics.

mod filter;
mod query;
use query::*;
pub(crate) use query::{metric_matches_correlation, span_matches_correlation};
mod index;
#[cfg(test)]
mod tests;

use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use foldhash::{HashMap, HashMapExt, HashSet, HashSetExt};
use serde::{Deserialize, Serialize};

use crate::{
    AttributeFingerprint, DurableLog, DurableMetricPoint, DurableSpan, ResourceContext,
    ResourceContextId, ScopeContext, ScopeContextId, TelemetryAttribute, TelemetryRecordRef,
    TelemetrySignal, TraceId,
};

const CONTEXT_ID_CACHE_ENTRIES: usize = 64;
const ATTRIBUTE_ID_CACHE_ENTRIES: usize = 1_024;

/// Hard bounds for one owner stripe's cross-signal postings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorrelationConfig {
    /// Maximum distinct trace, resource, scope, and attribute keys combined.
    pub max_keys: usize,
    /// Maximum record references retained for one correlation key.
    pub max_refs_per_key: usize,
    /// Maximum references retained across all postings.
    pub max_total_refs: usize,
}

impl Default for CorrelationConfig {
    fn default() -> Self {
        Self {
            max_keys: 65_536,
            max_refs_per_key: 4_096,
            max_total_refs: 1_048_576,
        }
    }
}

/// Snapshot of bounded correlation-index state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CorrelationStats {
    /// Distinct admitted correlation keys.
    pub keys: usize,
    /// Record references retained across all postings.
    pub refs: usize,
    /// Postings omitted because a configured bound was reached.
    pub dropped_postings: u64,
}

/// Cross-signal lookup constraints combined with AND semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelationQuery {
    /// Authenticated tenant.
    pub tenant: Arc<str>,
    /// Optional trace link shared by spans, logs, and metric exemplars.
    pub trace_id: Option<TraceId>,
    /// Optional exact resource context.
    pub resource_id: Option<ResourceContextId>,
    /// Optional exact instrumentation scope.
    pub scope_id: Option<ScopeContextId>,
    /// Exact typed metadata key/value identities.
    pub attributes: Arc<Vec<AttributeFingerprint>>,
    /// String labels rendered by compatibility APIs and matched across signal scopes.
    pub labels: Arc<Vec<(Arc<str>, Arc<str>)>>,
    /// Optional signal restriction.
    pub signal: Option<TelemetrySignal>,
    /// Inclusive lower event-time bound.
    pub start_time_unix_nanos: Option<u64>,
    /// Inclusive upper event-time bound.
    pub end_time_unix_nanos: Option<u64>,
    /// Exclusive stable continuation point.
    pub after: Option<TelemetryRecordRef>,
    /// Maximum record references returned.
    pub limit: usize,
}

impl CorrelationQuery {
    /// Creates an unconstrained, bounded query for one tenant.
    #[must_use]
    pub fn new(tenant: impl Into<Arc<str>>) -> Self {
        Self {
            tenant: tenant.into(),
            trace_id: None,
            resource_id: None,
            scope_id: None,
            attributes: Arc::new(Vec::new()),
            labels: Arc::new(Vec::new()),
            signal: None,
            start_time_unix_nanos: None,
            end_time_unix_nanos: None,
            after: None,
            limit: 1_000,
        }
    }

    /// Requires an exact trace link.
    #[must_use]
    pub const fn with_trace_id(mut self, trace_id: TraceId) -> Self {
        self.trace_id = Some(trace_id);
        self
    }

    /// Requires an exact resource context.
    #[must_use]
    pub const fn with_resource_id(mut self, resource_id: ResourceContextId) -> Self {
        self.resource_id = Some(resource_id);
        self
    }

    /// Requires an exact instrumentation scope.
    #[must_use]
    pub const fn with_scope_id(mut self, scope_id: ScopeContextId) -> Self {
        self.scope_id = Some(scope_id);
        self
    }

    /// Requires one exact typed metadata key/value.
    #[must_use]
    pub fn with_attribute(mut self, attribute: &TelemetryAttribute) -> Self {
        Arc::make_mut(&mut self.attributes).push(attribute.fingerprint());
        self
    }

    /// Requires one exact string label wherever that key/value is attached.
    #[must_use]
    pub fn with_label(mut self, key: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Self {
        let key = key.into();
        let value = value.into();
        Arc::make_mut(&mut self.attributes).push(
            TelemetryAttribute::new(
                Arc::clone(&key),
                crate::TelemetryValue::String(Arc::clone(&value)),
            )
            .fingerprint(),
        );
        Arc::make_mut(&mut self.labels).push((key, value));
        self
    }

    /// Restricts results to one signal.
    #[must_use]
    pub const fn for_signal(mut self, signal: TelemetrySignal) -> Self {
        self.signal = Some(signal);
        self
    }

    /// Continues strictly after a record reference returned by a prior page.
    #[must_use]
    pub const fn after(mut self, record_ref: TelemetryRecordRef) -> Self {
        self.after = Some(record_ref);
        self
    }

    /// Sets the bounded result count.
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CorrelationKey {
    Trace(TraceId),
    Resource(ResourceContextId),
    Scope(ScopeContextId),
    Attribute(AttributeFingerprint),
}

const CORRELATION_FILTER_WORDS: usize = 16;
const CORRELATION_FILTER_HASHES: usize = 4;

/// Compact immutable filter used to prune cold signal blocks by shared
/// telemetry identity. False positives are possible; false negatives are not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrelationBlockFilter {
    bits: [u64; CORRELATION_FILTER_WORDS],
}

impl Default for CorrelationBlockFilter {
    fn default() -> Self {
        Self {
            bits: [0; CORRELATION_FILTER_WORDS],
        }
    }
}

/// Preallocated, single-writer correlation postings owned by one stripe.
///
/// Reaching a bound drops only an optional navigation posting; durable signal
/// storage and each signal's exact native indexes remain authoritative.
#[derive(Debug)]
pub struct CorrelationIndex {
    config: CorrelationConfig,
    tenants: HashMap<Arc<str>, u32>,
    postings: HashMap<(u32, CorrelationKey), Vec<CorrelationPosting>>,
    resource_pointer_ids: Vec<Option<CachedResourceId>>,
    resource_ids: Vec<Option<CachedResourceId>>,
    scope_pointer_ids: Vec<Option<CachedScopeId>>,
    scope_ids: Vec<Option<CachedScopeId>>,
    attribute_pointer_ids: Vec<Option<CachedAttributeIds>>,
    attribute_ids: Vec<Option<CachedAttributeIds>>,
    refs: usize,
    dropped_postings: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CorrelationPosting {
    record_ref: TelemetryRecordRef,
    timestamp_unix_nanos: u64,
}

#[derive(Debug)]
struct CachedResourceId {
    hash: u64,
    context: Arc<ResourceContext>,
    id: ResourceContextId,
}

#[derive(Debug)]
struct CachedScopeId {
    hash: u64,
    context: Arc<ScopeContext>,
    id: ScopeContextId,
}

#[derive(Debug)]
struct CachedAttributeIds {
    hash: u64,
    attributes: Arc<Vec<TelemetryAttribute>>,
    ids: Arc<[AttributeFingerprint]>,
}
