//! Loki API ownership: stable store contract and models, routing, ingestion, LogQL, and handlers.
//! `loki_api/` separates HTTP routes, parsing, evaluation, and operational endpoints.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, RawQuery, Request, State, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use prost::Message;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::analytics::AnalyticsGroupRow;
use crate::deletion::DeleteCatalog;
use crate::{
    AnalyticsRow, AnalyticsScanRequest, CaseSensitivity, DeleteRequest, LogPredicate,
    TextMatchKind, TextMatcher,
};
use crate::{ProductionRuntime, ServiceState};

mod router;
pub use router::{
    loki_router, loki_router_with_clickhouse, single_tenant_loki_api_router,
    single_tenant_loki_router,
};
mod ingest;
use ingest::*;
mod query_http;
use query_http::*;
mod metric_eval;
use metric_eval::*;
mod logql;
pub(crate) use logql::parse_log_query;
use logql::*;
mod discovery;
use discovery::*;
mod tail_deletes;
use tail_deletes::*;
mod operations;
use operations::*;
#[cfg(test)]
mod tests;

const DEFAULT_TENANT: &str = "fake";
const DEFAULT_QUERY_LIMIT: usize = 100;
const MAX_QUERY_LIMIT: usize = 5_000;

/// Configuration for the Loki-compatible HTTP boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LokiApiConfig {
    /// Tenant used when Loki multi-tenancy headers are absent.
    pub default_tenant: Arc<str>,
    /// Largest materialized result accepted by query APIs.
    pub max_query_limit: usize,
    /// Maximum request body accepted by the Loki and Prometheus-compatible
    /// ingestion routes.
    pub max_request_bytes: usize,
}

impl Default for LokiApiConfig {
    fn default() -> Self {
        Self {
            default_tenant: Arc::from(DEFAULT_TENANT),
            max_query_limit: MAX_QUERY_LIMIT,
            max_request_bytes: 16 * 1024 * 1024,
        }
    }
}

/// One normalized Loki entry accepted by the compatibility boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LokiEntry {
    /// Nanosecond Unix timestamp.
    pub timestamp_unix_nanos: i64,
    /// Stream labels used by LogQL selectors.
    pub labels: BTreeMap<String, String>,
    /// Original log line.
    pub line: String,
    /// Structured metadata attached to the entry.
    pub structured_metadata: BTreeMap<String, String>,
}

/// Health snapshot supplied by a Loki storage backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreHealth {
    /// Whether durable writes and indexed reads can be served safely.
    pub ready: bool,
    /// Bounded operator-facing explanation when readiness is false.
    pub detail: Arc<str>,
}

impl Default for StoreHealth {
    fn default() -> Self {
        Self {
            ready: true,
            detail: Arc::from("ready"),
        }
    }
}

/// Storage counters rendered with protocol counters by `/metrics`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreMetrics {
    /// Durable sink work waiting to be indexed.
    pub pending_items: u64,
    /// Bytes represented by pending durable sink work.
    pub pending_bytes: u64,
    /// Age in milliseconds of the oldest pending checkpoint.
    pub checkpoint_age_ms: u64,
    /// Durable appends applied to the log index.
    pub applied_appends: u64,
    /// Durable sink retries.
    pub retry_attempts: u64,
    /// Durable sink failures.
    pub failed_attempts: u64,
    /// Partitions requiring explicit recovery.
    pub dirty_partitions: u64,
    /// Source payload bytes still retained in shard-stream.
    pub retained_payload_bytes: Option<u64>,
    /// Completed batch-aligned retention passes.
    pub retention_runs: u64,
    /// Logical offsets made eligible for pack reclamation.
    pub retention_advanced_offsets: u64,
    /// Failed retention passes.
    pub retention_failures: u64,
    /// Object-tier requests, bytes, exact-key deletions, and failures.
    pub object_store: Option<crate::ObjectStoreStats>,
    /// Raw shard-stream offsets reclaimed after compressed object publication.
    pub source_reclaimed_offsets: u64,
    /// Compressed groups retired by physical retention.
    pub retired_object_groups: u64,
    /// Compressed payload bytes retired by physical retention.
    pub retired_object_payload_bytes: u64,
    /// Exact object keys transferred to reclamation ownership.
    pub retired_object_keys: u64,
}

#[derive(Debug, Default)]
struct TenantStore {
    entries: Vec<LokiEntry>,
}

/// Result of one Loki range query, including the work counters reported by
/// the protocol response.
#[derive(Debug, Default)]
pub struct LokiQueryResult {
    /// Entries that survived storage pruning, selector stages, and deletes.
    pub entries: Vec<LokiEntry>,
    /// Candidate lines inspected by the storage implementation.
    pub lines_processed: usize,
    /// Candidate line bytes inspected by the storage implementation.
    pub bytes_processed: usize,
}

/// Storage contract used by the Loki-compatible protocol boundary.
pub trait LokiStore: Send + Sync + std::fmt::Debug {
    /// Atomically accepts one normalized push for a tenant.
    fn push(&self, tenant: &str, entries: Vec<LokiEntry>) -> Result<(), LokiApiError>;

    /// Returns entries for a tenant. Exact query filtering is performed by the
    /// protocol evaluator after storage-level pruning.
    fn entries(&self, tenant: &str) -> Result<Vec<LokiEntry>, LokiApiError>;

    /// Executes a bounded LogQL range query with storage-level pruning when
    /// the backend supports it. The default reference implementation retains
    /// exact behavior by filtering a tenant snapshot.
    fn query_range(
        &self,
        tenant: &str,
        expression: &str,
        start_timestamp_unix_nanos: i64,
        end_timestamp_unix_nanos: i64,
        limit: usize,
        newest_first: bool,
    ) -> Result<LokiQueryResult, LokiApiError> {
        let selector = parse_log_query(expression)?;
        let mut entries = self.entries(tenant)?;
        let lines_processed = entries.len();
        let bytes_processed = entries.iter().map(|entry| entry.line.len()).sum();
        entries.retain(|entry| {
            entry.timestamp_unix_nanos >= start_timestamp_unix_nanos
                && entry.timestamp_unix_nanos <= end_timestamp_unix_nanos
        });
        let mut entries = entries
            .into_iter()
            .filter_map(|entry| selector.process(entry))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|entry| entry.timestamp_unix_nanos);
        if newest_first {
            entries.reverse();
        }
        entries.truncate(limit);
        Ok(LokiQueryResult {
            entries,
            lines_processed,
            bytes_processed,
        })
    }

    /// Streams bounded batches through the analytical columnar boundary.
    ///
    /// Durable stores override this method to push constraints into their
    /// stripe indexes. Reference stores retain exact behavior through this
    /// entry-based implementation.
    fn scan_analytics(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        crate::analytics::scan_entries(self.entries(&request.tenant)?, request, emit)
    }

    /// Streams a relevance-ordered bounded log scan. Durable stores may
    /// override this to rank indexed candidates before row materialization.
    fn scan_analytics_relevance(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        self.scan_analytics(request, emit)
    }

    /// Counts distinct non-empty log trace IDs, optionally requiring that a
    /// second service has a record for each trace.
    fn scan_analytics_distinct_trace_cardinality(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(u64) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        scan_distinct_trace_cardinality_by_rows(self, request, emit)
    }

    /// Emits an already-columnar Arrow batch when the storage engine can
    /// project a narrow indexed query without first constructing normalized
    /// row objects. Returning `false` asks the protocol boundary to use
    /// [`Self::scan_analytics`].
    fn scan_analytics_arrow(
        &self,
        _request: &AnalyticsScanRequest,
        _schema: &SchemaRef,
        _emit: &mut dyn FnMut(&RecordBatch) -> Result<(), LokiApiError>,
    ) -> Result<bool, LokiApiError> {
        Ok(false)
    }

    /// Writes a projected query directly as ClickHouse RowBinary when the
    /// storage engine can avoid normalized row materialization. Returning
    /// `false` asks the boundary to encode [`Self::scan_analytics`] output.
    fn scan_analytics_rowbinary(
        &self,
        _request: &AnalyticsScanRequest,
        _writer: &mut dyn std::io::Write,
    ) -> Result<bool, LokiApiError> {
        Ok(false)
    }

    /// Emits exact row counts for analytical scans that do not materialize a
    /// physical column. Durable stores may answer this from append metadata;
    /// reference stores retain exact behavior through the ordinary scan.
    fn scan_analytics_cardinality(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(u64) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        self.scan_analytics(request, &mut |rows| {
            emit(u64::try_from(rows.len()).unwrap_or(u64::MAX))
        })
    }

    /// Emits grouped analytical results. Reference stores use the exact row
    /// scan implementation; durable stores may replace it with indexed
    /// aggregation while preserving the same result contract.
    fn scan_analytics_grouped(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsGroupRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        crate::analytics::group_analytics_rows(self, request, emit)
    }

    /// Returns a bounded health snapshot without scanning stored records.
    fn health(&self) -> Result<StoreHealth, LokiApiError> {
        Ok(StoreHealth::default())
    }

    /// Synchronizes accepted durable writes and their query-visible indexes.
    fn flush(&self, _timeout: Duration) -> Result<(), LokiApiError> {
        Ok(())
    }

    /// Returns a bounded lock-free storage counter snapshot.
    fn operational_metrics(&self) -> StoreMetrics {
        StoreMetrics::default()
    }

    /// Durably records one validated logical deletion request.
    fn create_delete(
        &self,
        _tenant: &str,
        _start_time: i64,
        _end_time: i64,
        _query: String,
        _created_at: i64,
    ) -> Result<String, LokiApiError> {
        Err(LokiApiError::configuration(
            "the configured store does not support deletion",
        ))
    }

    /// Returns active logical deletion requests for one tenant.
    fn delete_requests(&self, _tenant: &str) -> Result<Vec<DeleteRequest>, LokiApiError> {
        Ok(Vec::new())
    }

    /// Durably cancels an active logical deletion request.
    fn cancel_delete(&self, _tenant: &str, _request_id: &str) -> Result<bool, LokiApiError> {
        Ok(false)
    }
}

pub(crate) fn scan_distinct_trace_cardinality_by_rows<S: LokiStore + ?Sized>(
    store: &S,
    request: &AnalyticsScanRequest,
    emit: &mut dyn FnMut(u64) -> Result<(), LokiApiError>,
) -> Result<(), LokiApiError> {
    let mut rows_request = request.clone();
    let join_service = rows_request.trace_join_service.take();
    rows_request.cardinality_only = false;
    rows_request.distinct_trace_id = false;
    rows_request.columns = vec![crate::AnalyticsColumn::TraceId];
    let mut outer = HashSet::<Arc<str>>::new();
    store.scan_analytics(&rows_request, &mut |rows| {
        for row in rows {
            if let Some(trace_id) = row.trace_id.as_ref().filter(|value| !value.is_empty()) {
                outer.insert(Arc::clone(trace_id));
            }
        }
        Ok(())
    })?;
    if let Some(service) = join_service {
        let mut inner_request = rows_request;
        inner_request.predicate = crate::LogPredicate::MatchAll;
        inner_request.predicate_any = false;
        inner_request.terms.clear();
        inner_request.message_tokens.clear();
        inner_request.case_insensitive_message_tokens.clear();
        inner_request.labels = vec![crate::MetadataField::new("service_name", service)];
        let mut inner = HashSet::<Arc<str>>::new();
        store.scan_analytics(&inner_request, &mut |rows| {
            for row in rows {
                if let Some(trace_id) = row.trace_id.as_ref().filter(|value| !value.is_empty()) {
                    inner.insert(Arc::clone(trace_id));
                }
            }
            Ok(())
        })?;
        outer.retain(|trace_id| inner.contains(trace_id));
    }
    emit(u64::try_from(outer.len()).unwrap_or(u64::MAX))
}

/// Thread-safe in-memory reference backend used by differential API tests.
#[derive(Debug, Default)]
pub struct LokiApiStore {
    tenants: RwLock<HashMap<String, TenantStore>>,
    deletes: DeleteCatalog,
}

impl LokiApiStore {
    /// Appends normalized entries to one tenant.
    fn append(&self, tenant: &str, entries: Vec<LokiEntry>) -> Result<(), LokiApiError> {
        let mut tenants = self
            .tenants
            .write()
            .map_err(|_| LokiApiError::internal("tenant store lock is poisoned"))?;
        let store = tenants.entry(tenant.to_owned()).or_default();
        store.entries.extend(entries);
        store
            .entries
            .sort_unstable_by_key(|entry| entry.timestamp_unix_nanos);
        Ok(())
    }

    fn snapshot(&self, tenant: &str) -> Result<Vec<LokiEntry>, LokiApiError> {
        let tenants = self
            .tenants
            .read()
            .map_err(|_| LokiApiError::internal("tenant store lock is poisoned"))?;
        Ok(tenants
            .get(tenant)
            .map(|store| store.entries.clone())
            .unwrap_or_default())
    }
}

impl LokiStore for LokiApiStore {
    fn push(&self, tenant: &str, entries: Vec<LokiEntry>) -> Result<(), LokiApiError> {
        self.append(tenant, entries)
    }

    fn entries(&self, tenant: &str) -> Result<Vec<LokiEntry>, LokiApiError> {
        let mut entries = self.snapshot(tenant)?;
        apply_logical_deletes(&mut entries, &self.deletes.list(tenant)?)?;
        Ok(entries)
    }

    fn create_delete(
        &self,
        tenant: &str,
        start_time: i64,
        end_time: i64,
        query: String,
        created_at: i64,
    ) -> Result<String, LokiApiError> {
        self.deletes
            .create(tenant, start_time, end_time, query, created_at)
    }

    fn delete_requests(&self, tenant: &str) -> Result<Vec<DeleteRequest>, LokiApiError> {
        self.deletes.list(tenant)
    }

    fn cancel_delete(&self, tenant: &str, request_id: &str) -> Result<bool, LokiApiError> {
        self.deletes.cancel(tenant, request_id)
    }
}

#[derive(Clone)]
struct ApiState {
    store: Arc<dyn LokiStore>,
    config: LokiApiConfig,
    live: broadcast::Sender<LivePush>,
    analytics_bearer_token: Option<Arc<str>>,
    production: Option<Arc<ProductionRuntime>>,
    flush_timeout: Duration,
}

#[derive(Debug, Clone)]
struct LivePush {
    tenant: String,
    entries: Vec<LokiEntry>,
}

/// HTTP-boundary failure with a stable response status and safe message.
#[derive(Debug)]
pub struct LokiApiError {
    status: StatusCode,
    message: String,
}

impl std::fmt::Display for LokiApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for LokiApiError {}

impl LokiApiError {
    /// Creates a retryable storage/control-plane availability error for an
    /// embedded backend implementation.
    pub fn backend_unavailable(message: impl Into<String>) -> Self {
        Self::unavailable(message)
    }

    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    fn too_many_requests(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: message.into(),
        }
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }

    fn timeout(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    pub(crate) fn configuration(message: impl Into<String>) -> Self {
        Self::internal(message)
    }

    pub(crate) const fn status(&self) -> StatusCode {
        self.status
    }
}

impl IntoResponse for LokiApiError {
    fn into_response(self) -> Response {
        let error_type = match self.status {
            StatusCode::BAD_REQUEST => "bad_data",
            StatusCode::UNAUTHORIZED => "unauthorized",
            StatusCode::FORBIDDEN => "forbidden",
            StatusCode::TOO_MANY_REQUESTS => "rate_limited",
            StatusCode::SERVICE_UNAVAILABLE => "unavailable",
            StatusCode::GATEWAY_TIMEOUT => "timeout",
            _ => "internal",
        };
        (
            self.status,
            Json(json!({
                "status": "error",
                "errorType": error_type,
                "error": self.message,
            })),
        )
            .into_response()
    }
}

#[derive(Debug, Default, Deserialize)]
struct QueryParams {
    query: Option<String>,
    start: Option<String>,
    end: Option<String>,
    time: Option<String>,
    since: Option<String>,
    limit: Option<usize>,
    direction: Option<String>,
    step: Option<String>,
    line_limit: Option<usize>,
    field_limit: Option<usize>,
}

#[derive(Debug, Clone)]
enum MetricExpression {
    Scalar(f64),
    Range {
        operation: RangeOperation,
        selector: LogSelector,
        window_nanos: i64,
        parameter: Option<f64>,
    },
    Aggregate {
        operation: AggregateOperation,
        grouping: Option<MetricGrouping>,
        parameter: Option<usize>,
        expression: Box<Self>,
    },
    Binary {
        operation: BinaryOperation,
        bool_mode: bool,
        matching: VectorMatching,
        left: Box<Self>,
        right: Box<Self>,
    },
}

#[derive(Debug, Clone, Copy)]
enum RangeOperation {
    Count,
    Rate,
    Bytes,
    BytesRate,
    Absent,
    Sum,
    Average,
    Minimum,
    Maximum,
    Stddev,
    Stdvar,
    Quantile,
    First,
    Last,
    RateCounter,
}

#[derive(Debug, Clone, Copy)]
enum AggregateOperation {
    Sum,
    Average,
    Minimum,
    Maximum,
    Count,
    Stddev,
    Stdvar,
    TopK,
    BottomK,
    Sort,
    SortDescending,
}

#[derive(Debug, Clone)]
enum MetricGrouping {
    By(Vec<String>),
    Without(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryOperation {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    And,
    Or,
    Unless,
}

#[derive(Debug, Clone, Default)]
struct VectorMatching {
    on: Option<Vec<String>>,
    ignoring: Vec<String>,
    cardinality: VectorCardinality,
    include: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum VectorCardinality {
    #[default]
    OneToOne,
    ManyToOne,
    OneToMany,
}

#[derive(Debug, Clone)]
struct MetricSample {
    labels: BTreeMap<String, String>,
    value: f64,
}

#[derive(Debug, Clone)]
enum MetricValue {
    Scalar(f64),
    Vector(Vec<MetricSample>),
}

#[derive(Debug, Clone)]
pub(crate) struct LogSelector {
    matchers: Vec<LabelMatcher>,
    stages: Vec<PipelineStage>,
}

impl LogSelector {
    fn matches(&self, entry: &LokiEntry) -> bool {
        self.process(entry.clone()).is_some()
    }

    pub(crate) fn process(&self, mut entry: LokiEntry) -> Option<LokiEntry> {
        if !self
            .matchers
            .iter()
            .all(|matcher| matcher.matches(&entry.labels))
        {
            return None;
        }
        for stage in &self.stages {
            if !stage.apply(&mut entry) {
                return None;
            }
        }
        Some(entry)
    }

    pub(crate) fn exact_label_matchers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.matchers.iter().filter_map(|matcher| {
            matches!(matcher.operation, MatchOperation::Equal)
                .then_some((matcher.name.as_str(), matcher.value.as_str()))
        })
    }

    pub(crate) fn is_exact_label_only(&self) -> bool {
        self.stages.is_empty()
            && self
                .matchers
                .iter()
                .all(|matcher| matcher.operation == MatchOperation::Equal)
    }

    pub(crate) fn indexed_line_predicate(&self) -> Option<LogPredicate> {
        let predicates = self
            .stages
            .iter()
            .filter_map(|stage| match stage {
                PipelineStage::Line(filter)
                    if filter.operation == MatchOperation::Equal && !filter.value.is_empty() =>
                {
                    Some(LogPredicate::message(TextMatcher::new(
                        filter.value.clone(),
                        TextMatchKind::Contains,
                        CaseSensitivity::Sensitive,
                    )))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        (!predicates.is_empty()).then(|| LogPredicate::and(predicates))
    }
}

pub(crate) struct LogicalDeleteFilter {
    compiled: Vec<(i64, i64, LogSelector)>,
}

impl LogicalDeleteFilter {
    pub(crate) fn compile(requests: &[DeleteRequest]) -> Result<Self, LokiApiError> {
        let compiled = requests
            .iter()
            .filter(|request| request.status == "received")
            .map(|request| {
                Ok((
                    request.start_time,
                    request.end_time,
                    parse_log_query(&request.query)?,
                ))
            })
            .collect::<Result<Vec<_>, LokiApiError>>()?;
        Ok(Self { compiled })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.compiled.is_empty()
    }

    pub(crate) fn matches(&self, entry: &LokiEntry) -> bool {
        self.compiled.iter().any(|(start, end, selector)| {
            entry.timestamp_unix_nanos >= *start
                && entry.timestamp_unix_nanos <= *end
                && selector.matches(entry)
        })
    }
}

pub(crate) fn apply_logical_deletes(
    entries: &mut Vec<LokiEntry>,
    requests: &[DeleteRequest],
) -> Result<(), LokiApiError> {
    let filter = LogicalDeleteFilter::compile(requests)?;
    entries.retain(|entry| !filter.matches(entry));
    Ok(())
}

#[derive(Debug, Clone)]
struct LabelMatcher {
    name: String,
    operation: MatchOperation,
    value: String,
    regex: Option<Regex>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchOperation {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
}

impl LabelMatcher {
    fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        let observed = labels.get(&self.name).map(String::as_str).unwrap_or("");
        match self.operation {
            MatchOperation::Equal => observed == self.value,
            MatchOperation::NotEqual => observed != self.value,
            MatchOperation::Regex => self
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(observed)),
            MatchOperation::NotRegex => self
                .regex
                .as_ref()
                .is_some_and(|regex| !regex.is_match(observed)),
        }
    }
}

#[derive(Debug, Clone)]
struct LineFilter {
    operation: MatchOperation,
    value: String,
    regex: Option<Regex>,
}

impl LineFilter {
    fn matches(&self, line: &str) -> bool {
        match self.operation {
            MatchOperation::Equal => line.contains(&self.value),
            MatchOperation::NotEqual => !line.contains(&self.value),
            MatchOperation::Regex => self
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(line)),
            MatchOperation::NotRegex => self
                .regex
                .as_ref()
                .is_some_and(|regex| !regex.is_match(line)),
        }
    }
}

#[derive(Debug, Clone)]
enum PipelineStage {
    Line(LineFilter),
    Json(Vec<(String, Vec<String>)>),
    Logfmt,
    Regexp(Regex),
    Pattern(Regex),
    LabelFilter(LabelFilter),
    LineFormat(String),
    LabelFormat(Vec<(String, LabelFormatValue)>),
    Drop(Vec<String>),
    Keep(Vec<String>),
    Decolorize,
    Unpack,
    Unwrap(String),
}

#[derive(Debug, Clone)]
struct LabelFilter {
    name: String,
    operation: LabelFilterOperation,
    value: String,
    regex: Option<Regex>,
}

#[derive(Debug, Clone, Copy)]
enum LabelFilterOperation {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

#[derive(Debug, Clone)]
enum LabelFormatValue {
    Rename(String),
    Template(String),
}

impl PipelineStage {
    fn apply(&self, entry: &mut LokiEntry) -> bool {
        match self {
            Self::Line(filter) => filter.matches(&entry.line),
            Self::Json(expressions) => {
                match serde_json::from_str::<Value>(&entry.line) {
                    Ok(Value::Object(object)) => {
                        if expressions.is_empty() {
                            flatten_json_object("", &object, &mut entry.labels);
                        } else {
                            for (label, path) in expressions {
                                if let Some(value) = json_path(&Value::Object(object.clone()), path)
                                    && let Some(value) = scalar_label_value(value)
                                {
                                    insert_extracted_label(&mut entry.labels, label, value);
                                }
                            }
                        }
                    }
                    Ok(_) | Err(_) => set_parser_error(entry, "JSONParserErr"),
                }
                true
            }
            Self::Logfmt => {
                let parsed = parse_logfmt_labels(&entry.line);
                if parsed.is_empty() {
                    set_parser_error(entry, "LogfmtParserErr");
                } else {
                    for (name, value) in parsed {
                        insert_extracted_label(&mut entry.labels, &name, value);
                    }
                }
                true
            }
            Self::Regexp(regex) | Self::Pattern(regex) => {
                let Some(captures) = regex.captures(&entry.line) else {
                    set_parser_error(entry, "RegexpParserErr");
                    return true;
                };
                for name in regex.capture_names().flatten() {
                    if let Some(value) = captures.name(name) {
                        insert_extracted_label(&mut entry.labels, name, value.as_str().to_owned());
                    }
                }
                true
            }
            Self::LabelFilter(filter) => filter.matches(entry),
            Self::LineFormat(template) => {
                entry.line = render_logql_template(template, entry);
                true
            }
            Self::LabelFormat(assignments) => {
                for (target, value) in assignments {
                    let rendered = match value {
                        LabelFormatValue::Rename(source) => {
                            entry.labels.remove(source).unwrap_or_default()
                        }
                        LabelFormatValue::Template(template) => {
                            render_logql_template(template, entry)
                        }
                    };
                    entry.labels.insert(target.clone(), rendered);
                }
                true
            }
            Self::Drop(names) => {
                for name in names {
                    entry.labels.remove(name);
                }
                true
            }
            Self::Keep(names) => {
                entry
                    .labels
                    .retain(|name, _| names.iter().any(|candidate| candidate == name));
                true
            }
            Self::Decolorize => {
                entry.line = strip_ansi(&entry.line);
                true
            }
            Self::Unpack => {
                match serde_json::from_str::<Value>(&entry.line) {
                    Ok(Value::Object(mut object)) => {
                        let unpacked = object
                            .remove("_entry")
                            .and_then(|value| value.as_str().map(str::to_owned));
                        for (name, value) in object {
                            if let Some(value) = scalar_label_value(&value) {
                                insert_extracted_label(&mut entry.labels, &name, value);
                            }
                        }
                        if let Some(unpacked) = unpacked {
                            entry.line = unpacked;
                        } else {
                            set_parser_error(entry, "JSONParserErr");
                        }
                    }
                    Ok(_) | Err(_) => set_parser_error(entry, "JSONParserErr"),
                }
                true
            }
            Self::Unwrap(label) => {
                let _ = label;
                true
            }
        }
    }
}

impl LabelFilter {
    fn matches(&self, entry: &LokiEntry) -> bool {
        let observed = entry
            .labels
            .get(&self.name)
            .or_else(|| entry.structured_metadata.get(&self.name))
            .map(String::as_str)
            .unwrap_or("");
        match self.operation {
            LabelFilterOperation::Equal => observed == self.value,
            LabelFilterOperation::NotEqual => observed != self.value,
            LabelFilterOperation::Regex => self
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(observed)),
            LabelFilterOperation::NotRegex => self
                .regex
                .as_ref()
                .is_some_and(|regex| !regex.is_match(observed)),
            operation => {
                compare_typed_label(observed, &self.value).is_some_and(|ordering| match operation {
                    LabelFilterOperation::Greater => ordering.is_gt(),
                    LabelFilterOperation::GreaterEqual => ordering.is_ge(),
                    LabelFilterOperation::Less => ordering.is_lt(),
                    LabelFilterOperation::LessEqual => ordering.is_le(),
                    _ => false,
                })
            }
        }
    }
}
