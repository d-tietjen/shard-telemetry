//! Analytical query ownership: schema models, request parsing, row projection, and wire output.
//! `analytics/` separates parsing, matching, grouping, and Arrow/RowBinary responses.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};
use std::mem::size_of;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, MapBuilder, StringBuilder,
    TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, Int32Array, RecordBatch, UInt32Array, UInt64Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use axum::body::{Body, Bytes};
use axum::http::{HeaderName, HeaderValue, header};
use axum::response::Response;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::loki_api::LokiApiError;
use crate::query::{message_has_clickhouse_token, message_has_term};
use crate::trace::TraceProjection;
use crate::{
    CaseSensitivity, DurableLog, DurableMetricPoint, DurableSpan, LogPredicate, LokiStore,
    MetadataField, MetricKind, MetricValue, NumberValue, NumericComparison, SeriesFingerprint,
    SpanId, TelemetryAttribute, TelemetryValue, TextMatchKind, TextMatcher, TraceId,
};

mod parse;
pub(crate) use parse::parse_scan_request;
mod log_rows;
pub(crate) use log_rows::{
    log_columns_need_structural_fields, log_columns_need_typed_metadata, log_row,
    projected_log_row, scan_entries,
};
mod relevance;
pub(crate) use relevance::RelevanceScorer;
mod signal_rows;
use signal_rows::*;
pub(crate) use signal_rows::{
    metric_rows, projected_metric_row, projected_span_row, projected_trace_row, span_rows,
};
mod filter;
pub(crate) use filter::row_matches;
use filter::*;
mod response;
pub(crate) use response::analytics_stream_response;
mod grouping;
use grouping::*;
pub(crate) use grouping::{decoded_group_value, durable_group_value, group_analytics_rows};
mod arrow;
use arrow::*;
pub(crate) use arrow::{
    can_direct_metric_projection, can_direct_span_projection, direct_metric_record_batch,
    direct_span_record_batch, write_direct_metric_rowbinary, write_direct_span_rowbinary,
};
#[cfg(test)]
mod tests;

/// Pinned ClickHouse release whose evaluator defines ShardTelemetry SQL semantics.
pub const CLICKHOUSE_COMPATIBILITY_TARGET: &str = "26.3.17.56-lts";

/// The only pre-release analytical schema and protocol version.
pub const ANALYTICS_SCHEMA_VERSION: u16 = 1;

pub(crate) const DEFAULT_SCAN_BATCH_ROWS: usize = 8_192;
const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// Exact timestamp order that the analytical storage layer may apply before
/// a bounded log scan is returned to ClickHouse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyticsScanOrder {
    /// Oldest timestamp first, with durable offset as the stable tie-breaker.
    TimestampAscending,
    /// Newest timestamp first, with durable offset as the stable tie-breaker.
    TimestampDescending,
    /// Highest relevance score first, with newest timestamp and durable offset
    /// as stable tie-breakers.
    RelevanceDescending,
}

/// Supported analytical grouping keys for log scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyticsGroupKey {
    /// OpenTelemetry severity text.
    SeverityText,
    /// Scope name stored in the normalized metadata fields.
    ScopeName,
    /// Event timestamp truncated to a minute.
    Minute,
}

impl AnalyticsGroupKey {
    /// Parses a stable query parameter name.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "severity_text" | "SeverityText" => Some(Self::SeverityText),
            "scope_name" | "ScopeName" => Some(Self::ScopeName),
            "minute" => Some(Self::Minute),
            _ => None,
        }
    }
}

/// Ordering applied to grouped analytical results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyticsGroupOrder {
    /// Order groups by descending count, then key.
    CountDescending,
    /// Order groups lexicographically by their keys.
    KeyAscending,
}

/// One grouped analytical result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AnalyticsGroupRow {
    /// Group key values in the request order.
    pub keys: Vec<Option<Arc<str>>>,
    /// Number of matching records in the group.
    pub count: u64,
}

/// Encoding used by the authenticated analytical scan boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AnalyticsWireFormat {
    /// Standard Arrow IPC streaming format.
    #[default]
    ArrowStream,
    /// ClickHouse RowBinary using the requested relation projection.
    RowBinary,
    /// Newline-delimited JSON objects using the requested relation projection.
    ///
    /// This is intended for bounded analytical interchange, including DuckDB's
    /// built-in JSON reader. It is not an ingest protocol or a replacement for
    /// the typed Arrow stream on high-throughput analytical paths.
    JsonLines,
}

/// A stable telemetry relation exposed to ClickHouse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AnalyticsRelation {
    /// One row per log record.
    Logs,
    /// One row per winning span version.
    Spans,
    /// One row per nested span event.
    SpanEvents,
    /// One row per nested span link.
    SpanLinks,
    /// One row per winning raw metric point.
    MetricPoints,
    /// One row per nested metric exemplar.
    MetricExemplars,
}

impl AnalyticsRelation {
    /// Stable v1 wire name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::Spans => "spans",
            Self::SpanEvents => "span_events",
            Self::SpanLinks => "span_links",
            Self::MetricPoints => "metric_points",
            Self::MetricExemplars => "metric_exemplars",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "logs" => Some(Self::Logs),
            "spans" => Some(Self::Spans),
            "span_events" => Some(Self::SpanEvents),
            "span_links" => Some(Self::SpanLinks),
            "metric_points" | "metrics" => Some(Self::MetricPoints),
            "metric_exemplars" => Some(Self::MetricExemplars),
            _ => None,
        }
    }

    /// Columns available from this relation in stable wire order.
    #[must_use]
    pub const fn columns(self) -> &'static [AnalyticsColumn] {
        match self {
            Self::Logs => &LOG_COLUMNS,
            Self::Spans => &SPAN_COLUMNS,
            Self::SpanEvents => &SPAN_EVENT_COLUMNS,
            Self::SpanLinks => &SPAN_LINK_COLUMNS,
            Self::MetricPoints => &METRIC_COLUMNS,
            Self::MetricExemplars => &METRIC_EXEMPLAR_COLUMNS,
        }
    }

    #[must_use]
    /// Parent telemetry signal containing this relation.
    pub const fn signal(self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::Spans | Self::SpanEvents | Self::SpanLinks => "traces",
            Self::MetricPoints | Self::MetricExemplars => "metrics",
        }
    }
}

/// One stable column available from at least one analytical relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(missing_docs)]
pub enum AnalyticsColumn {
    Tenant,
    Signal,
    Timestamp,
    ParentTimestamp,
    ObservedTimestamp,
    StartTimestamp,
    EndTimestamp,
    Partition,
    Offset,
    Ordinal,
    ResourceId,
    ScopeId,
    TraceId,
    SpanId,
    ParentSpanId,
    LinkedTraceId,
    LinkedSpanId,
    SeriesId,
    Message,
    Score,
    BodyJson,
    Name,
    EventName,
    SeverityNumber,
    SeverityText,
    Kind,
    DurationNanos,
    StatusCode,
    StatusMessage,
    TraceState,
    Flags,
    DroppedAttributesCount,
    DroppedEventsCount,
    DroppedLinksCount,
    Labels,
    Metadata,
    Attributes,
    ResourceAttributes,
    ScopeAttributes,
    AttributeIds,
    ResourceAttributeIds,
    ScopeAttributeIds,
    AttributesJson,
    ResourceAttributesJson,
    ScopeAttributesJson,
    EventsJson,
    LinksJson,
    Description,
    Unit,
    MetricKind,
    Temporality,
    Monotonic,
    ValueType,
    ScalarInteger,
    ScalarDoubleBits,
    ValueJson,
    ExemplarsJson,
}

impl AnalyticsColumn {
    /// Stable Arrow, JSON, and ClickHouse column name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::Signal => "signal",
            Self::Timestamp => "timestamp",
            Self::ParentTimestamp => "parent_timestamp",
            Self::ObservedTimestamp => "observed_timestamp",
            Self::StartTimestamp => "start_timestamp",
            Self::EndTimestamp => "end_timestamp",
            Self::Partition => "partition",
            Self::Offset => "offset",
            Self::Ordinal => "ordinal",
            Self::ResourceId => "resource_id",
            Self::ScopeId => "scope_id",
            Self::TraceId => "trace_id",
            Self::SpanId => "span_id",
            Self::ParentSpanId => "parent_span_id",
            Self::LinkedTraceId => "linked_trace_id",
            Self::LinkedSpanId => "linked_span_id",
            Self::SeriesId => "series_id",
            Self::Message => "message",
            Self::Score => "score",
            Self::BodyJson => "body_json",
            Self::Name => "name",
            Self::EventName => "event_name",
            Self::SeverityNumber => "severity_number",
            Self::SeverityText => "severity_text",
            Self::Kind => "kind",
            Self::DurationNanos => "duration_nanos",
            Self::StatusCode => "status_code",
            Self::StatusMessage => "status_message",
            Self::TraceState => "trace_state",
            Self::Flags => "flags",
            Self::DroppedAttributesCount => "dropped_attributes_count",
            Self::DroppedEventsCount => "dropped_events_count",
            Self::DroppedLinksCount => "dropped_links_count",
            Self::Labels => "labels",
            Self::Metadata => "metadata",
            Self::Attributes => "attributes",
            Self::ResourceAttributes => "resource_attributes",
            Self::ScopeAttributes => "scope_attributes",
            Self::AttributeIds => "attribute_ids",
            Self::ResourceAttributeIds => "resource_attribute_ids",
            Self::ScopeAttributeIds => "scope_attribute_ids",
            Self::AttributesJson => "attributes_json",
            Self::ResourceAttributesJson => "resource_attributes_json",
            Self::ScopeAttributesJson => "scope_attributes_json",
            Self::EventsJson => "events_json",
            Self::LinksJson => "links_json",
            Self::Description => "description",
            Self::Unit => "unit",
            Self::MetricKind => "metric_kind",
            Self::Temporality => "temporality",
            Self::Monotonic => "monotonic",
            Self::ValueType => "value_type",
            Self::ScalarInteger => "scalar_integer",
            Self::ScalarDoubleBits => "scalar_double_bits",
            Self::ValueJson => "value_json",
            Self::ExemplarsJson => "exemplars_json",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        ALL_COLUMNS
            .iter()
            .copied()
            .find(|column| column.name() == value)
    }

    fn nullable(self) -> bool {
        !matches!(
            self,
            Self::Tenant
                | Self::Signal
                | Self::Timestamp
                | Self::Partition
                | Self::Offset
                | Self::Labels
                | Self::Metadata
                | Self::Attributes
                | Self::ResourceAttributes
                | Self::ScopeAttributes
                | Self::AttributeIds
                | Self::ResourceAttributeIds
                | Self::ScopeAttributeIds
        )
    }

    fn field(self) -> Field {
        let data_type = match self {
            Self::Timestamp
            | Self::ParentTimestamp
            | Self::ObservedTimestamp
            | Self::StartTimestamp
            | Self::EndTimestamp => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::<str>::from("UTC")))
            }
            Self::Partition
            | Self::Ordinal
            | Self::Flags
            | Self::DroppedAttributesCount
            | Self::DroppedEventsCount
            | Self::DroppedLinksCount => DataType::UInt32,
            Self::Offset | Self::DurationNanos | Self::ScalarDoubleBits => DataType::UInt64,
            Self::Score => DataType::Float64,
            Self::SeverityNumber | Self::Kind | Self::StatusCode | Self::Temporality => {
                DataType::Int32
            }
            Self::ScalarInteger => DataType::Int64,
            Self::Monotonic => DataType::Boolean,
            Self::Labels
            | Self::Metadata
            | Self::Attributes
            | Self::ResourceAttributes
            | Self::ScopeAttributes
            | Self::AttributeIds
            | Self::ResourceAttributeIds
            | Self::ScopeAttributeIds => string_map_data_type(),
            _ => DataType::Utf8,
        };
        Field::new(self.name(), data_type, self.nullable())
    }
}

const ALL_COLUMNS: [AnalyticsColumn; 57] = [
    AnalyticsColumn::Tenant,
    AnalyticsColumn::Signal,
    AnalyticsColumn::Timestamp,
    AnalyticsColumn::ParentTimestamp,
    AnalyticsColumn::ObservedTimestamp,
    AnalyticsColumn::StartTimestamp,
    AnalyticsColumn::EndTimestamp,
    AnalyticsColumn::Partition,
    AnalyticsColumn::Offset,
    AnalyticsColumn::Ordinal,
    AnalyticsColumn::ResourceId,
    AnalyticsColumn::ScopeId,
    AnalyticsColumn::TraceId,
    AnalyticsColumn::SpanId,
    AnalyticsColumn::ParentSpanId,
    AnalyticsColumn::LinkedTraceId,
    AnalyticsColumn::LinkedSpanId,
    AnalyticsColumn::SeriesId,
    AnalyticsColumn::Message,
    AnalyticsColumn::Score,
    AnalyticsColumn::BodyJson,
    AnalyticsColumn::Name,
    AnalyticsColumn::EventName,
    AnalyticsColumn::SeverityNumber,
    AnalyticsColumn::SeverityText,
    AnalyticsColumn::Kind,
    AnalyticsColumn::DurationNanos,
    AnalyticsColumn::StatusCode,
    AnalyticsColumn::StatusMessage,
    AnalyticsColumn::TraceState,
    AnalyticsColumn::Flags,
    AnalyticsColumn::DroppedAttributesCount,
    AnalyticsColumn::DroppedEventsCount,
    AnalyticsColumn::DroppedLinksCount,
    AnalyticsColumn::Labels,
    AnalyticsColumn::Metadata,
    AnalyticsColumn::Attributes,
    AnalyticsColumn::ResourceAttributes,
    AnalyticsColumn::ScopeAttributes,
    AnalyticsColumn::AttributeIds,
    AnalyticsColumn::ResourceAttributeIds,
    AnalyticsColumn::ScopeAttributeIds,
    AnalyticsColumn::AttributesJson,
    AnalyticsColumn::ResourceAttributesJson,
    AnalyticsColumn::ScopeAttributesJson,
    AnalyticsColumn::EventsJson,
    AnalyticsColumn::LinksJson,
    AnalyticsColumn::Description,
    AnalyticsColumn::Unit,
    AnalyticsColumn::MetricKind,
    AnalyticsColumn::Temporality,
    AnalyticsColumn::Monotonic,
    AnalyticsColumn::ValueType,
    AnalyticsColumn::ScalarInteger,
    AnalyticsColumn::ScalarDoubleBits,
    AnalyticsColumn::ValueJson,
    AnalyticsColumn::ExemplarsJson,
];

const BASE: [AnalyticsColumn; 9] = [
    AnalyticsColumn::Tenant,
    AnalyticsColumn::Signal,
    AnalyticsColumn::Timestamp,
    AnalyticsColumn::Partition,
    AnalyticsColumn::Offset,
    AnalyticsColumn::ResourceId,
    AnalyticsColumn::ScopeId,
    AnalyticsColumn::TraceId,
    AnalyticsColumn::SpanId,
];

const LOG_COLUMNS: [AnalyticsColumn; 29] = [
    BASE[0],
    BASE[1],
    BASE[2],
    AnalyticsColumn::ObservedTimestamp,
    BASE[3],
    BASE[4],
    BASE[5],
    BASE[6],
    BASE[7],
    BASE[8],
    AnalyticsColumn::Message,
    AnalyticsColumn::Score,
    AnalyticsColumn::BodyJson,
    AnalyticsColumn::SeverityNumber,
    AnalyticsColumn::SeverityText,
    AnalyticsColumn::EventName,
    AnalyticsColumn::Flags,
    AnalyticsColumn::DroppedAttributesCount,
    AnalyticsColumn::Labels,
    AnalyticsColumn::Metadata,
    AnalyticsColumn::Attributes,
    AnalyticsColumn::ResourceAttributes,
    AnalyticsColumn::ScopeAttributes,
    AnalyticsColumn::AttributeIds,
    AnalyticsColumn::ResourceAttributeIds,
    AnalyticsColumn::ScopeAttributeIds,
    AnalyticsColumn::AttributesJson,
    AnalyticsColumn::ResourceAttributesJson,
    AnalyticsColumn::ScopeAttributesJson,
];

const SPAN_COLUMNS: [AnalyticsColumn; 32] = [
    BASE[0],
    BASE[1],
    BASE[2],
    AnalyticsColumn::EndTimestamp,
    BASE[3],
    BASE[4],
    BASE[5],
    BASE[6],
    BASE[7],
    BASE[8],
    AnalyticsColumn::ParentSpanId,
    AnalyticsColumn::Name,
    AnalyticsColumn::Kind,
    AnalyticsColumn::DurationNanos,
    AnalyticsColumn::StatusCode,
    AnalyticsColumn::StatusMessage,
    AnalyticsColumn::TraceState,
    AnalyticsColumn::Flags,
    AnalyticsColumn::DroppedAttributesCount,
    AnalyticsColumn::DroppedEventsCount,
    AnalyticsColumn::DroppedLinksCount,
    AnalyticsColumn::Attributes,
    AnalyticsColumn::ResourceAttributes,
    AnalyticsColumn::ScopeAttributes,
    AnalyticsColumn::AttributeIds,
    AnalyticsColumn::ResourceAttributeIds,
    AnalyticsColumn::ScopeAttributeIds,
    AnalyticsColumn::AttributesJson,
    AnalyticsColumn::ResourceAttributesJson,
    AnalyticsColumn::ScopeAttributesJson,
    AnalyticsColumn::EventsJson,
    AnalyticsColumn::LinksJson,
];

const SPAN_EVENT_COLUMNS: [AnalyticsColumn; 20] = [
    BASE[0],
    BASE[1],
    BASE[2],
    AnalyticsColumn::ParentTimestamp,
    BASE[3],
    BASE[4],
    BASE[5],
    BASE[6],
    BASE[7],
    BASE[8],
    AnalyticsColumn::Ordinal,
    AnalyticsColumn::Name,
    AnalyticsColumn::DroppedAttributesCount,
    AnalyticsColumn::Attributes,
    AnalyticsColumn::ResourceAttributes,
    AnalyticsColumn::ScopeAttributes,
    AnalyticsColumn::AttributeIds,
    AnalyticsColumn::AttributesJson,
    AnalyticsColumn::ResourceAttributesJson,
    AnalyticsColumn::ScopeAttributesJson,
];

const SPAN_LINK_COLUMNS: [AnalyticsColumn; 22] = [
    BASE[0],
    BASE[1],
    BASE[2],
    BASE[3],
    BASE[4],
    BASE[5],
    BASE[6],
    BASE[7],
    BASE[8],
    AnalyticsColumn::Ordinal,
    AnalyticsColumn::LinkedTraceId,
    AnalyticsColumn::LinkedSpanId,
    AnalyticsColumn::TraceState,
    AnalyticsColumn::Flags,
    AnalyticsColumn::DroppedAttributesCount,
    AnalyticsColumn::Attributes,
    AnalyticsColumn::ResourceAttributes,
    AnalyticsColumn::ScopeAttributes,
    AnalyticsColumn::AttributeIds,
    AnalyticsColumn::AttributesJson,
    AnalyticsColumn::ResourceAttributesJson,
    AnalyticsColumn::ScopeAttributesJson,
];

const METRIC_COLUMNS: [AnalyticsColumn; 32] = [
    BASE[0],
    BASE[1],
    BASE[2],
    AnalyticsColumn::StartTimestamp,
    BASE[3],
    BASE[4],
    BASE[5],
    BASE[6],
    AnalyticsColumn::SeriesId,
    AnalyticsColumn::Name,
    AnalyticsColumn::Description,
    AnalyticsColumn::Unit,
    AnalyticsColumn::MetricKind,
    AnalyticsColumn::Temporality,
    AnalyticsColumn::Monotonic,
    AnalyticsColumn::Flags,
    AnalyticsColumn::ValueType,
    AnalyticsColumn::ScalarInteger,
    AnalyticsColumn::ScalarDoubleBits,
    AnalyticsColumn::ValueJson,
    AnalyticsColumn::Labels,
    AnalyticsColumn::Metadata,
    AnalyticsColumn::Attributes,
    AnalyticsColumn::ResourceAttributes,
    AnalyticsColumn::ScopeAttributes,
    AnalyticsColumn::AttributeIds,
    AnalyticsColumn::ResourceAttributeIds,
    AnalyticsColumn::ScopeAttributeIds,
    AnalyticsColumn::AttributesJson,
    AnalyticsColumn::ResourceAttributesJson,
    AnalyticsColumn::ScopeAttributesJson,
    AnalyticsColumn::ExemplarsJson,
];

const METRIC_EXEMPLAR_COLUMNS: [AnalyticsColumn; 21] = [
    BASE[0],
    BASE[1],
    BASE[2],
    AnalyticsColumn::ParentTimestamp,
    BASE[3],
    BASE[4],
    BASE[5],
    BASE[6],
    AnalyticsColumn::TraceId,
    AnalyticsColumn::SpanId,
    AnalyticsColumn::SeriesId,
    AnalyticsColumn::Ordinal,
    AnalyticsColumn::Name,
    AnalyticsColumn::ValueType,
    AnalyticsColumn::ScalarInteger,
    AnalyticsColumn::ScalarDoubleBits,
    AnalyticsColumn::Attributes,
    AnalyticsColumn::AttributeIds,
    AnalyticsColumn::AttributesJson,
    AnalyticsColumn::Labels,
    AnalyticsColumn::Metadata,
];

/// Bounded storage-level scan requested by ClickHouse.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs)]
pub struct AnalyticsScanRequest {
    pub tenant: Arc<str>,
    pub relation: AnalyticsRelation,
    pub start_timestamp_unix_nanos: Option<u64>,
    pub end_timestamp_unix_nanos: Option<u64>,
    pub terms: Vec<Arc<str>>,
    pub message_tokens: Vec<Arc<str>>,
    pub case_insensitive_message_tokens: Vec<Arc<str>>,
    /// Additional Boolean log predicate. It is pushed into the indexed log
    /// query path and is combined with the legacy fields above using AND.
    pub predicate: LogPredicate,
    /// Combines the explicitly supplied predicate parts with OR when set.
    /// Legacy field filters remain independent AND constraints.
    pub predicate_any: bool,
    pub labels: Vec<MetadataField>,
    pub metadata: Vec<MetadataField>,
    pub attributes: Vec<MetadataField>,
    pub resource_attributes: Vec<MetadataField>,
    pub scope_attributes: Vec<MetadataField>,
    pub trace_id: Option<TraceId>,
    /// Return the number of distinct non-empty trace IDs that also have a
    /// record matching this service name.
    pub trace_join_service: Option<Arc<str>>,
    /// Deduplicate log results by trace ID for a cardinality query.
    pub distinct_trace_id: bool,
    pub span_id: Option<SpanId>,
    pub series_id: Option<SeriesFingerprint>,
    pub name: Option<Arc<str>>,
    /// Grouping keys for grouped log scans.
    pub group_by: Vec<AnalyticsGroupKey>,
    /// Optional maximum number of grouped results.
    pub group_limit: Option<usize>,
    /// Ordering for grouped results.
    pub group_order: AnalyticsGroupOrder,
    pub columns: Vec<AnalyticsColumn>,
    pub limit: Option<usize>,
    pub cardinality_only: bool,
    pub order: Option<AnalyticsScanOrder>,
    pub wire_format: AnalyticsWireFormat,
}

impl AnalyticsScanRequest {
    /// Creates a full-column log scan for one tenant.
    #[must_use]
    pub fn new(tenant: impl Into<Arc<str>>) -> Self {
        Self::for_relation(tenant, AnalyticsRelation::Logs)
    }

    /// Creates a full-column scan for one relation.
    #[must_use]
    pub fn for_relation(tenant: impl Into<Arc<str>>, relation: AnalyticsRelation) -> Self {
        Self {
            tenant: tenant.into(),
            relation,
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            terms: Vec::new(),
            message_tokens: Vec::new(),
            case_insensitive_message_tokens: Vec::new(),
            predicate: LogPredicate::MatchAll,
            predicate_any: false,
            labels: Vec::new(),
            metadata: Vec::new(),
            attributes: Vec::new(),
            resource_attributes: Vec::new(),
            scope_attributes: Vec::new(),
            trace_id: None,
            trace_join_service: None,
            distinct_trace_id: false,
            span_id: None,
            series_id: None,
            name: None,
            group_by: Vec::new(),
            group_limit: None,
            group_order: AnalyticsGroupOrder::KeyAscending,
            columns: relation.columns().to_vec(),
            limit: None,
            cardinality_only: false,
            order: None,
            wire_format: AnalyticsWireFormat::ArrowStream,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), LokiApiError> {
        if self.tenant.is_empty() {
            return Err(LokiApiError::bad_request(
                "analytics tenant must not be empty",
            ));
        }
        if self.columns.is_empty() {
            return Err(LokiApiError::bad_request(
                "analytics columns must not be empty",
            ));
        }
        let allowed = self
            .relation
            .columns()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut observed = BTreeSet::new();
        if self
            .columns
            .iter()
            .any(|column| !allowed.contains(column) || !observed.insert(*column))
        {
            return Err(LokiApiError::bad_request(
                "analytics columns contain a duplicate or relation-incompatible column",
            ));
        }
        if self.cardinality_only
            && (self.columns.len() != 1
                || (self.wire_format == AnalyticsWireFormat::ArrowStream
                    && self.columns != [AnalyticsColumn::Offset])
                || self.wire_format == AnalyticsWireFormat::JsonLines)
        {
            return Err(LokiApiError::bad_request(
                "cardinality_only requires one RowBinary column or the Arrow offset column",
            ));
        }
        if self.order.is_some() && self.relation != AnalyticsRelation::Logs {
            return Err(LokiApiError::bad_request(
                "ordered analytical scans are currently log-only",
            ));
        }
        if !self.group_by.is_empty() && self.relation != AnalyticsRelation::Logs {
            return Err(LokiApiError::bad_request(
                "grouping is currently available only on the logs relation",
            ));
        }
        if !self.group_by.is_empty() && self.wire_format != AnalyticsWireFormat::JsonLines {
            return Err(LokiApiError::bad_request(
                "grouped analytical scans require JSON Lines output",
            ));
        }
        if self.order.is_some() && self.limit.is_none() {
            return Err(LokiApiError::bad_request(
                "ordered analytical scans require a bounded limit",
            ));
        }
        if self
            .start_timestamp_unix_nanos
            .zip(self.end_timestamp_unix_nanos)
            .is_some_and(|(start, end)| start >= end)
        {
            return Err(LokiApiError::bad_request(
                "analytics timestamp range must be non-empty",
            ));
        }
        if self.relation != AnalyticsRelation::Logs
            && (!self.terms.is_empty()
                || !self.message_tokens.is_empty()
                || !self.case_insensitive_message_tokens.is_empty()
                || self.predicate != LogPredicate::MatchAll)
        {
            return Err(LokiApiError::bad_request(
                "term filtering is available only on the logs relation",
            ));
        }
        if (self.trace_join_service.is_some() || self.distinct_trace_id)
            && (self.relation != AnalyticsRelation::Logs
                || !self.cardinality_only
                || self.order.is_some()
                || self.limit.is_some()
                || !self.group_by.is_empty())
        {
            return Err(LokiApiError::bad_request(
                "trace joins and distinct trace IDs require an unbounded log cardinality scan",
            ));
        }
        Ok(())
    }
}

/// One normalized row shared by the relation-specific Arrow writers.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs)]
pub struct AnalyticsRow {
    pub tenant: Arc<str>,
    pub signal: Arc<str>,
    pub timestamp_unix_nanos: i64,
    pub parent_timestamp_unix_nanos: Option<i64>,
    pub observed_timestamp_unix_nanos: Option<i64>,
    pub start_timestamp_unix_nanos: Option<i64>,
    pub end_timestamp_unix_nanos: Option<i64>,
    pub partition: u32,
    pub offset: u64,
    pub ordinal: Option<u32>,
    pub resource_id: Option<Arc<str>>,
    pub scope_id: Option<Arc<str>>,
    pub trace_id: Option<Arc<str>>,
    pub span_id: Option<Arc<str>>,
    pub parent_span_id: Option<Arc<str>>,
    pub linked_trace_id: Option<Arc<str>>,
    pub linked_span_id: Option<Arc<str>>,
    pub series_id: Option<Arc<str>>,
    pub message: Option<Arc<str>>,
    pub score: Option<f64>,
    pub body_json: Option<Arc<str>>,
    pub name: Option<Arc<str>>,
    pub event_name: Option<Arc<str>>,
    pub severity_number: Option<i32>,
    pub severity_text: Option<Arc<str>>,
    pub kind: Option<i32>,
    pub duration_nanos: Option<u64>,
    pub status_code: Option<i32>,
    pub status_message: Option<Arc<str>>,
    pub trace_state: Option<Arc<str>>,
    pub flags: Option<u32>,
    pub dropped_attributes_count: Option<u32>,
    pub dropped_events_count: Option<u32>,
    pub dropped_links_count: Option<u32>,
    pub labels: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, String>,
    pub attributes: BTreeMap<String, String>,
    pub resource_attributes: BTreeMap<String, String>,
    pub scope_attributes: BTreeMap<String, String>,
    pub attribute_ids: BTreeMap<String, String>,
    pub resource_attribute_ids: BTreeMap<String, String>,
    pub scope_attribute_ids: BTreeMap<String, String>,
    pub attributes_json: Option<Arc<str>>,
    pub resource_attributes_json: Option<Arc<str>>,
    pub scope_attributes_json: Option<Arc<str>>,
    pub events_json: Option<Arc<str>>,
    pub links_json: Option<Arc<str>>,
    pub description: Option<Arc<str>>,
    pub unit: Option<Arc<str>>,
    pub metric_kind: Option<Arc<str>>,
    pub temporality: Option<i32>,
    pub monotonic: Option<bool>,
    pub value_type: Option<Arc<str>>,
    pub scalar_integer: Option<i64>,
    pub scalar_double_bits: Option<u64>,
    pub value_json: Option<Arc<str>>,
    pub exemplars_json: Option<Arc<str>>,
}

impl AnalyticsRow {
    pub(crate) fn empty(
        tenant: Arc<str>,
        signal: &'static str,
        timestamp_unix_nanos: u64,
        partition: u32,
        offset: u64,
    ) -> Result<Self, LokiApiError> {
        Ok(Self {
            tenant,
            signal: interned_signal(signal),
            timestamp_unix_nanos: timestamp_i64(timestamp_unix_nanos)?,
            parent_timestamp_unix_nanos: None,
            observed_timestamp_unix_nanos: None,
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            partition,
            offset,
            ordinal: None,
            resource_id: None,
            scope_id: None,
            trace_id: None,
            span_id: None,
            parent_span_id: None,
            linked_trace_id: None,
            linked_span_id: None,
            series_id: None,
            message: None,
            score: None,
            body_json: None,
            name: None,
            event_name: None,
            severity_number: None,
            severity_text: None,
            kind: None,
            duration_nanos: None,
            status_code: None,
            status_message: None,
            trace_state: None,
            flags: None,
            dropped_attributes_count: None,
            dropped_events_count: None,
            dropped_links_count: None,
            labels: BTreeMap::new(),
            metadata: BTreeMap::new(),
            attributes: BTreeMap::new(),
            resource_attributes: BTreeMap::new(),
            scope_attributes: BTreeMap::new(),
            attribute_ids: BTreeMap::new(),
            resource_attribute_ids: BTreeMap::new(),
            scope_attribute_ids: BTreeMap::new(),
            attributes_json: None,
            resource_attributes_json: None,
            scope_attributes_json: None,
            events_json: None,
            links_json: None,
            description: None,
            unit: None,
            metric_kind: None,
            temporality: None,
            monotonic: None,
            value_type: None,
            scalar_integer: None,
            scalar_double_bits: None,
            value_json: None,
            exemplars_json: None,
        })
    }
}

#[inline]
fn interned_signal(signal: &'static str) -> Arc<str> {
    static LOGS: OnceLock<Arc<str>> = OnceLock::new();
    static TRACES: OnceLock<Arc<str>> = OnceLock::new();
    static METRICS: OnceLock<Arc<str>> = OnceLock::new();

    match signal {
        "logs" => LOGS.get_or_init(|| Arc::from("logs")).clone(),
        "traces" => TRACES.get_or_init(|| Arc::from("traces")).clone(),
        "metrics" => METRICS.get_or_init(|| Arc::from("metrics")).clone(),
        _ => Arc::from(signal),
    }
}
