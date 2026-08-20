use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};
use std::mem::size_of;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::{
    BooleanBuilder, Int32Builder, Int64Builder, MapBuilder, StringBuilder,
    TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, Int32Array, RecordBatch, UInt32Array, UInt64Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use axum::body::{Body, Bytes};
use axum::http::{HeaderName, HeaderValue, header};
use axum::response::Response;
use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::loki_api::LokiApiError;
use crate::query::{message_has_clickhouse_token, message_has_term};
use crate::{
    CaseSensitivity, DurableLog, DurableMetricPoint, DurableSpan, LokiStore, MetadataField,
    MetricKind, MetricValue, NumberValue, SeriesFingerprint, SpanId, TelemetryAttribute,
    TelemetryValue, TraceId,
};

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

const ALL_COLUMNS: [AnalyticsColumn; 56] = [
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

const LOG_COLUMNS: [AnalyticsColumn; 28] = [
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
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct AnalyticsScanRequest {
    pub tenant: Arc<str>,
    pub relation: AnalyticsRelation,
    pub start_timestamp_unix_nanos: Option<u64>,
    pub end_timestamp_unix_nanos: Option<u64>,
    pub terms: Vec<Arc<str>>,
    pub message_tokens: Vec<Arc<str>>,
    pub case_insensitive_message_tokens: Vec<Arc<str>>,
    pub labels: Vec<MetadataField>,
    pub metadata: Vec<MetadataField>,
    pub attributes: Vec<MetadataField>,
    pub resource_attributes: Vec<MetadataField>,
    pub scope_attributes: Vec<MetadataField>,
    pub trace_id: Option<TraceId>,
    pub span_id: Option<SpanId>,
    pub series_id: Option<SeriesFingerprint>,
    pub name: Option<Arc<str>>,
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
            labels: Vec::new(),
            metadata: Vec::new(),
            attributes: Vec::new(),
            resource_attributes: Vec::new(),
            scope_attributes: Vec::new(),
            trace_id: None,
            span_id: None,
            series_id: None,
            name: None,
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
                || !self.case_insensitive_message_tokens.is_empty())
        {
            return Err(LokiApiError::bad_request(
                "term filtering is available only on the logs relation",
            ));
        }
        Ok(())
    }
}

/// One normalized row shared by the relation-specific Arrow writers.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    fn empty(
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

pub(crate) fn parse_scan_request(
    tenant: String,
    raw_query: Option<&str>,
) -> Result<AnalyticsScanRequest, LokiApiError> {
    let pairs = form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let relations = pairs
        .iter()
        .filter(|(key, _)| key == "relation")
        .map(|(_, value)| value.as_str())
        .collect::<Vec<_>>();
    if relations.len() > 1 {
        return Err(LokiApiError::bad_request(
            "relation may be specified only once",
        ));
    }
    let relation = relations
        .first()
        .map_or(Some(AnalyticsRelation::Logs), |value| {
            AnalyticsRelation::parse(value)
        })
        .ok_or_else(|| LokiApiError::bad_request("unknown analytics relation"))?;
    let mut request = AnalyticsScanRequest::for_relation(tenant, relation);
    let mut columns_seen = false;
    let mut cardinality_seen = false;
    let mut order_seen = false;
    let mut wire_seen = false;
    for (key, value) in pairs {
        match key.as_str() {
            "relation" => {}
            "start_ns" => request.start_timestamp_unix_nanos = Some(parse_u64("start_ns", &value)?),
            "end_ns" => request.end_timestamp_unix_nanos = Some(parse_u64("end_ns", &value)?),
            "limit" => {
                request.limit = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| LokiApiError::bad_request("limit is not a usize"))?,
                );
            }
            "cardinality_only" => {
                if cardinality_seen {
                    return Err(LokiApiError::bad_request(
                        "cardinality_only may be specified only once",
                    ));
                }
                cardinality_seen = true;
                request.cardinality_only = match value.as_str() {
                    "1" | "true" => true,
                    "0" | "false" => false,
                    _ => {
                        return Err(LokiApiError::bad_request(
                            "cardinality_only is not a boolean",
                        ));
                    }
                };
            }
            "order" => {
                if order_seen {
                    return Err(LokiApiError::bad_request(
                        "order may be specified only once",
                    ));
                }
                order_seen = true;
                request.order = Some(match value.as_str() {
                    "timestamp_asc" => AnalyticsScanOrder::TimestampAscending,
                    "timestamp_desc" => AnalyticsScanOrder::TimestampDescending,
                    _ => return Err(LokiApiError::bad_request("unknown analytics order")),
                });
            }
            "wire" => {
                if wire_seen {
                    return Err(LokiApiError::bad_request("wire may be specified only once"));
                }
                wire_seen = true;
                request.wire_format = match value.as_str() {
                    "arrow" | "arrow_stream" => AnalyticsWireFormat::ArrowStream,
                    "rowbinary" => AnalyticsWireFormat::RowBinary,
                    "json" | "jsonl" | "ndjson" => AnalyticsWireFormat::JsonLines,
                    _ => return Err(LokiApiError::bad_request("unknown analytics wire format")),
                };
            }
            "term" => request.terms.push(Arc::from(value)),
            "message_token" => request.message_tokens.push(Arc::from(value)),
            "message_token_ci" => request
                .case_insensitive_message_tokens
                .push(Arc::from(value)),
            "trace_id" => request.trace_id = Some(parse_trace_id(&value)?),
            "span_id" => request.span_id = Some(parse_span_id(&value)?),
            "series_id" => request.series_id = Some(parse_series_id(&value)?),
            "name" => request.name = Some(Arc::from(value)),
            "columns" => {
                if columns_seen {
                    return Err(LokiApiError::bad_request(
                        "columns may be specified only once",
                    ));
                }
                columns_seen = true;
                request.columns = value
                    .split(',')
                    .map(|name| {
                        AnalyticsColumn::parse(name).ok_or_else(|| {
                            LokiApiError::bad_request(format!("unknown analytics column {name:?}"))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            }
            key if key.starts_with("label.") => {
                push_field(&mut request.labels, key, "label.", &value)?;
            }
            key if key.starts_with("metadata.") => {
                push_field(&mut request.metadata, key, "metadata.", &value)?;
            }
            key if key.starts_with("attribute.") => {
                push_field(&mut request.attributes, key, "attribute.", &value)?;
            }
            key if key.starts_with("resource.") => {
                push_field(&mut request.resource_attributes, key, "resource.", &value)?;
            }
            key if key.starts_with("scope.") => {
                push_field(&mut request.scope_attributes, key, "scope.", &value)?;
            }
            unknown => {
                return Err(LokiApiError::bad_request(format!(
                    "unknown analytics parameter {unknown:?}"
                )));
            }
        }
    }
    request.validate()?;
    Ok(request)
}

fn push_field(
    fields: &mut Vec<MetadataField>,
    key: &str,
    prefix: &str,
    value: &str,
) -> Result<(), LokiApiError> {
    let name = &key[prefix.len()..];
    if name.is_empty() {
        return Err(LokiApiError::bad_request(
            "attribute name must not be empty",
        ));
    }
    fields.push(MetadataField::new(name, value));
    Ok(())
}

fn parse_u64(name: &str, value: &str) -> Result<u64, LokiApiError> {
    value
        .parse::<u64>()
        .map_err(|_| LokiApiError::bad_request(format!("{name} is not a u64")))
}

fn parse_trace_id(value: &str) -> Result<TraceId, LokiApiError> {
    TraceId::from_bytes(decode_hex(value)?)
        .map_err(|_| LokiApiError::bad_request("trace_id is not a valid nonzero 128-bit hex ID"))
}

fn parse_span_id(value: &str) -> Result<SpanId, LokiApiError> {
    SpanId::from_bytes(decode_hex(value)?)
        .map_err(|_| LokiApiError::bad_request("span_id is not a valid nonzero 64-bit hex ID"))
}

fn parse_series_id(value: &str) -> Result<SeriesFingerprint, LokiApiError> {
    let bytes: [u8; 16] = decode_hex(value)?;
    Ok(SeriesFingerprint::from_raw(u128::from_be_bytes(bytes)))
}

fn decode_hex<const N: usize>(value: &str) -> Result<[u8; N], LokiApiError> {
    if value.len() != N * 2 {
        return Err(LokiApiError::bad_request(
            "hex identifier has the wrong length",
        ));
    }
    let mut output = [0; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        output[index] = (high << 4) | low;
    }
    Ok(output)
}

fn hex_nibble(value: u8) -> Result<u8, LokiApiError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(LokiApiError::bad_request(
            "identifier contains non-hex bytes",
        )),
    }
}

pub(crate) fn scan_entries(
    entries: Vec<crate::LokiEntry>,
    request: &AnalyticsScanRequest,
    emit: &mut dyn FnMut(&[AnalyticsRow]) -> Result<(), LokiApiError>,
) -> Result<(), LokiApiError> {
    request.validate()?;
    if request.relation != AnalyticsRelation::Logs {
        return Err(LokiApiError::bad_request(
            "the in-memory Loki store exposes only the logs analytical relation",
        ));
    }
    let limit = request.limit.unwrap_or(usize::MAX);
    if let Some(order) = request.order {
        let mut rows = entries
            .into_iter()
            .enumerate()
            .map(|(ordinal, entry)| {
                let mut row = AnalyticsRow::empty(
                    Arc::clone(&request.tenant),
                    "logs",
                    u64::try_from(entry.timestamp_unix_nanos)
                        .map_err(|_| LokiApiError::internal("pre-epoch log timestamp"))?,
                    0,
                    u64::try_from(ordinal).unwrap_or(u64::MAX),
                )?;
                row.message = Some(Arc::from(entry.line));
                row.labels = entry.labels;
                row.metadata = entry.structured_metadata;
                Ok(row)
            })
            .collect::<Result<Vec<_>, LokiApiError>>()?;
        rows.retain(|row| row_matches(row, request));
        rows.sort_unstable_by_key(|row| (row.timestamp_unix_nanos, row.offset));
        if order == AnalyticsScanOrder::TimestampDescending {
            rows.reverse();
        }
        rows.truncate(limit);
        for batch in rows.chunks(DEFAULT_SCAN_BATCH_ROWS) {
            emit(batch)?;
        }
        return Ok(());
    }
    let mut rows = Vec::with_capacity(DEFAULT_SCAN_BATCH_ROWS.min(limit));
    let mut emitted = 0usize;
    for (ordinal, entry) in entries.into_iter().enumerate() {
        if emitted == limit {
            break;
        }
        let mut row = AnalyticsRow::empty(
            Arc::clone(&request.tenant),
            "logs",
            u64::try_from(entry.timestamp_unix_nanos)
                .map_err(|_| LokiApiError::internal("pre-epoch log timestamp"))?,
            0,
            u64::try_from(ordinal).unwrap_or(u64::MAX),
        )?;
        row.message = Some(Arc::from(entry.line));
        row.labels = entry.labels;
        row.metadata = entry.structured_metadata;
        if !row_matches(&row, request) {
            continue;
        }
        rows.push(row);
        emitted += 1;
        if rows.len() == DEFAULT_SCAN_BATCH_ROWS {
            emit(&rows)?;
            rows.clear();
        }
    }
    if !rows.is_empty() {
        emit(&rows)?;
    }
    Ok(())
}

pub(crate) fn log_row(
    tenant: &Arc<str>,
    record: &DurableLog,
    labels: BTreeMap<String, String>,
    metadata: BTreeMap<String, String>,
) -> Result<AnalyticsRow, LokiApiError> {
    let mut row = AnalyticsRow::empty(
        Arc::clone(tenant),
        "logs",
        record.timestamp_unix_nanos,
        record.record_ref.topic_partition.partition_id.get(),
        record.record_ref.offset.get(),
    )?;
    row.observed_timestamp_unix_nanos = nonzero_timestamp(record.observed_timestamp_unix_nanos)?;
    row.resource_id = Some(Arc::from(record.resource_id().to_string()));
    row.scope_id = Some(Arc::from(record.scope_id().to_string()));
    row.trace_id = record.trace_id.map(|value| Arc::from(value.to_string()));
    row.span_id = record.span_id.map(|value| Arc::from(value.to_string()));
    row.message = Some(Arc::clone(&record.message));
    row.body_json = json(&record.body)?;
    row.event_name = Some(Arc::clone(&record.event_name));
    row.severity_number = Some(record.severity_number);
    row.severity_text = Some(Arc::clone(&record.severity_text));
    row.flags = Some(record.flags);
    row.dropped_attributes_count = Some(record.dropped_attributes_count);
    row.labels = labels;
    row.metadata = metadata;
    populate_attributes(
        &mut row,
        &record.attributes,
        &record.resource.attributes,
        &record.scope.attributes,
    )?;
    Ok(row)
}

/// Builds only the columns requested by the analytical boundary. Storage
/// pushdown has already applied the predicate before this function is used,
/// so omitted filter-only columns do not need to be materialized a second
/// time. This keeps narrow ClickHouse scans from serializing every OTLP map
/// and JSON sidecar for each selected record.
pub(crate) fn projected_log_row(
    tenant: &Arc<str>,
    record: &DurableLog,
    columns: &[AnalyticsColumn],
) -> Result<AnalyticsRow, LokiApiError> {
    let mut row = AnalyticsRow::empty(
        Arc::clone(tenant),
        "logs",
        record.timestamp_unix_nanos,
        record.record_ref.topic_partition.partition_id.get(),
        record.record_ref.offset.get(),
    )?;
    if wants(columns, AnalyticsColumn::ObservedTimestamp) {
        row.observed_timestamp_unix_nanos =
            nonzero_timestamp(record.observed_timestamp_unix_nanos)?;
    }
    if wants(columns, AnalyticsColumn::ResourceId) {
        row.resource_id = Some(Arc::from(record.resource_id().to_string()));
    }
    if wants(columns, AnalyticsColumn::ScopeId) {
        row.scope_id = Some(Arc::from(record.scope_id().to_string()));
    }
    if wants(columns, AnalyticsColumn::TraceId) {
        row.trace_id = record.trace_id.map(|value| Arc::from(value.to_string()));
    }
    if wants(columns, AnalyticsColumn::SpanId) {
        row.span_id = record.span_id.map(|value| Arc::from(value.to_string()));
    }
    if wants(columns, AnalyticsColumn::Message) {
        row.message = Some(Arc::clone(&record.message));
    }
    if wants(columns, AnalyticsColumn::BodyJson) {
        row.body_json = json(&record.body)?;
    }
    if wants(columns, AnalyticsColumn::EventName) {
        row.event_name = Some(Arc::clone(&record.event_name));
    }
    if wants(columns, AnalyticsColumn::SeverityNumber) {
        row.severity_number = Some(record.severity_number);
    }
    if wants(columns, AnalyticsColumn::SeverityText) {
        row.severity_text = Some(Arc::clone(&record.severity_text));
    }
    if wants(columns, AnalyticsColumn::Flags) {
        row.flags = Some(record.flags);
    }
    if wants(columns, AnalyticsColumn::DroppedAttributesCount) {
        row.dropped_attributes_count = Some(record.dropped_attributes_count);
    }
    if wants(columns, AnalyticsColumn::Labels) || wants(columns, AnalyticsColumn::Metadata) {
        for field in record.fields.iter() {
            if wants(columns, AnalyticsColumn::Labels)
                && let Some(name) = field.key.as_ref().strip_prefix("resource.loki.label.")
            {
                row.labels.insert(name.to_owned(), field.value.to_string());
            } else if wants(columns, AnalyticsColumn::Metadata)
                && let Some(name) = field.key.as_ref().strip_prefix("attr.loki.metadata.")
            {
                row.metadata
                    .insert(name.to_owned(), field.value.to_string());
            }
        }
    }
    populate_projected_attributes(
        &mut row,
        columns,
        &record.attributes,
        &record.resource.attributes,
        &record.scope.attributes,
    )?;
    Ok(row)
}

pub(crate) fn span_rows(
    span: &DurableSpan,
    relation: AnalyticsRelation,
) -> Result<Vec<AnalyticsRow>, LokiApiError> {
    match relation {
        AnalyticsRelation::Spans => Ok(vec![span_row(span)?]),
        AnalyticsRelation::SpanEvents => span
            .events
            .iter()
            .enumerate()
            .map(|(ordinal, event)| {
                let mut row = span_base_row(span, event.timestamp_unix_nanos)?;
                row.parent_timestamp_unix_nanos = Some(timestamp_i64(span.start_time_unix_nanos)?);
                row.ordinal = Some(u32::try_from(ordinal).unwrap_or(u32::MAX));
                row.name = Some(Arc::clone(&event.name));
                row.dropped_attributes_count = Some(event.dropped_attributes_count);
                set_record_attributes(&mut row, &event.attributes)?;
                Ok(row)
            })
            .collect(),
        AnalyticsRelation::SpanLinks => span
            .links
            .iter()
            .enumerate()
            .map(|(ordinal, link)| {
                let mut row = span_base_row(span, span.start_time_unix_nanos)?;
                row.ordinal = Some(u32::try_from(ordinal).unwrap_or(u32::MAX));
                row.linked_trace_id = Some(Arc::from(link.trace_id.to_string()));
                row.linked_span_id = Some(Arc::from(link.span_id.to_string()));
                row.trace_state = Some(Arc::clone(&link.trace_state));
                row.flags = Some(link.flags);
                row.dropped_attributes_count = Some(link.dropped_attributes_count);
                set_record_attributes(&mut row, &link.attributes)?;
                Ok(row)
            })
            .collect(),
        _ => Err(LokiApiError::internal(
            "span scanner received a non-trace relation",
        )),
    }
}

pub(crate) fn projected_span_row(
    span: &DurableSpan,
    columns: &[AnalyticsColumn],
) -> Result<AnalyticsRow, LokiApiError> {
    let mut row = AnalyticsRow::empty(
        Arc::clone(&span.tenant),
        "traces",
        span.start_time_unix_nanos,
        span.record_ref.topic_partition.partition_id.get(),
        span.record_ref.offset.get(),
    )?;
    populate_projected_context(&mut row, columns, span)?;
    if wants(columns, AnalyticsColumn::EndTimestamp) {
        row.end_timestamp_unix_nanos = span.end_time_unix_nanos().map(timestamp_i64).transpose()?;
    }
    if wants(columns, AnalyticsColumn::ParentSpanId) {
        row.parent_span_id = span
            .parent_span_id
            .map(|value| Arc::from(value.to_string()));
    }
    if wants(columns, AnalyticsColumn::Name) {
        row.name = Some(Arc::clone(&span.name));
    }
    if wants(columns, AnalyticsColumn::Kind) {
        row.kind = Some(span.kind);
    }
    if wants(columns, AnalyticsColumn::DurationNanos) {
        row.duration_nanos = Some(span.duration_nanos);
    }
    if wants(columns, AnalyticsColumn::StatusCode) {
        row.status_code = span.status.as_ref().map(|status| status.code);
    }
    if wants(columns, AnalyticsColumn::StatusMessage) {
        row.status_message = span
            .status
            .as_ref()
            .map(|status| Arc::clone(&status.message));
    }
    if wants(columns, AnalyticsColumn::TraceState) {
        row.trace_state = Some(Arc::clone(&span.trace_state));
    }
    if wants(columns, AnalyticsColumn::Flags) {
        row.flags = Some(span.flags);
    }
    if wants(columns, AnalyticsColumn::DroppedAttributesCount) {
        row.dropped_attributes_count = Some(span.dropped_attributes_count);
    }
    if wants(columns, AnalyticsColumn::DroppedEventsCount) {
        row.dropped_events_count = Some(span.dropped_events_count);
    }
    if wants(columns, AnalyticsColumn::DroppedLinksCount) {
        row.dropped_links_count = Some(span.dropped_links_count);
    }
    populate_projected_attributes(
        &mut row,
        columns,
        &span.attributes,
        &span.resource.attributes,
        &span.scope.attributes,
    )?;
    if wants(columns, AnalyticsColumn::EventsJson) {
        row.events_json = json(span.events.as_ref())?;
    }
    if wants(columns, AnalyticsColumn::LinksJson) {
        row.links_json = json(span.links.as_ref())?;
    }
    Ok(row)
}

fn span_base_row(span: &DurableSpan, timestamp: u64) -> Result<AnalyticsRow, LokiApiError> {
    let mut row = AnalyticsRow::empty(
        Arc::clone(&span.tenant),
        "traces",
        timestamp,
        span.record_ref.topic_partition.partition_id.get(),
        span.record_ref.offset.get(),
    )?;
    row.resource_id = Some(Arc::from(span.resource_id().to_string()));
    row.scope_id = Some(Arc::from(span.scope_id().to_string()));
    row.trace_id = Some(Arc::from(span.trace_id.to_string()));
    row.span_id = Some(Arc::from(span.span_id.to_string()));
    row.resource_attributes = attribute_map(&span.resource.attributes);
    row.scope_attributes = attribute_map(&span.scope.attributes);
    row.resource_attribute_ids = attribute_ids(&span.resource.attributes);
    row.scope_attribute_ids = attribute_ids(&span.scope.attributes);
    row.resource_attributes_json = json(span.resource.attributes.as_ref())?;
    row.scope_attributes_json = json(span.scope.attributes.as_ref())?;
    Ok(row)
}

fn span_row(span: &DurableSpan) -> Result<AnalyticsRow, LokiApiError> {
    let mut row = span_base_row(span, span.start_time_unix_nanos)?;
    row.end_timestamp_unix_nanos = span.end_time_unix_nanos().map(timestamp_i64).transpose()?;
    row.parent_span_id = span
        .parent_span_id
        .map(|value| Arc::from(value.to_string()));
    row.name = Some(Arc::clone(&span.name));
    row.kind = Some(span.kind);
    row.duration_nanos = Some(span.duration_nanos);
    row.status_code = span.status.as_ref().map(|status| status.code);
    row.status_message = span
        .status
        .as_ref()
        .map(|status| Arc::clone(&status.message));
    row.trace_state = Some(Arc::clone(&span.trace_state));
    row.flags = Some(span.flags);
    row.dropped_attributes_count = Some(span.dropped_attributes_count);
    row.dropped_events_count = Some(span.dropped_events_count);
    row.dropped_links_count = Some(span.dropped_links_count);
    set_record_attributes(&mut row, &span.attributes)?;
    row.events_json = json(span.events.as_ref())?;
    row.links_json = json(span.links.as_ref())?;
    Ok(row)
}

pub(crate) fn metric_rows(
    point: &DurableMetricPoint,
    relation: AnalyticsRelation,
) -> Result<Vec<AnalyticsRow>, LokiApiError> {
    match relation {
        AnalyticsRelation::MetricPoints => Ok(vec![metric_row(point)?]),
        AnalyticsRelation::MetricExemplars => point
            .exemplars
            .iter()
            .enumerate()
            .map(|(ordinal, exemplar)| {
                let mut row = metric_base_row(point, exemplar.timestamp_unix_nanos)?;
                row.parent_timestamp_unix_nanos = Some(timestamp_i64(point.timestamp_unix_nanos)?);
                row.ordinal = Some(u32::try_from(ordinal).unwrap_or(u32::MAX));
                row.trace_id = exemplar.trace_id.map(|value| Arc::from(value.to_string()));
                row.span_id = exemplar.span_id.map(|value| Arc::from(value.to_string()));
                set_number(&mut row, exemplar.value);
                set_record_attributes(&mut row, &exemplar.filtered_attributes)?;
                Ok(row)
            })
            .collect(),
        _ => Err(LokiApiError::internal(
            "metric scanner received a non-metric relation",
        )),
    }
}

pub(crate) fn projected_metric_row(
    point: &DurableMetricPoint,
    columns: &[AnalyticsColumn],
) -> Result<AnalyticsRow, LokiApiError> {
    let identity = &point.identity;
    let mut row = AnalyticsRow::empty(
        Arc::clone(&identity.tenant),
        "metrics",
        point.timestamp_unix_nanos,
        point.record_ref.topic_partition.partition_id.get(),
        point.record_ref.offset.get(),
    )?;
    if wants(columns, AnalyticsColumn::ResourceId) {
        row.resource_id = Some(Arc::from(identity.resource_id().to_string()));
    }
    if wants(columns, AnalyticsColumn::ScopeId) {
        row.scope_id = Some(Arc::from(identity.scope_id().to_string()));
    }
    if wants(columns, AnalyticsColumn::SeriesId) {
        row.series_id = Some(Arc::from(format!(
            "{:032x}",
            point.series_fingerprint().get()
        )));
    }
    if wants(columns, AnalyticsColumn::Name) {
        row.name = Some(Arc::clone(&identity.name));
    }
    if wants(columns, AnalyticsColumn::Labels) {
        row.labels = crate::prometheus_string_labels(identity)
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
    }
    populate_projected_attributes(
        &mut row,
        columns,
        &identity.point_attributes,
        &identity.resource.attributes,
        &identity.scope.attributes,
    )?;
    if wants(columns, AnalyticsColumn::StartTimestamp) {
        row.start_timestamp_unix_nanos = nonzero_timestamp(point.start_time_unix_nanos)?;
    }
    if wants(columns, AnalyticsColumn::Description) {
        row.description = Some(Arc::clone(&point.description));
    }
    if wants(columns, AnalyticsColumn::Unit) {
        row.unit = Some(Arc::clone(&identity.unit));
    }
    if wants(columns, AnalyticsColumn::MetricKind)
        || wants(columns, AnalyticsColumn::Temporality)
        || wants(columns, AnalyticsColumn::Monotonic)
    {
        let (kind, temporality, monotonic) = match identity.kind {
            MetricKind::Gauge => ("gauge", None, None),
            MetricKind::Sum {
                temporality,
                monotonic,
            } => ("sum", Some(temporality), Some(monotonic)),
            MetricKind::ExplicitHistogram { temporality } => {
                ("explicit_histogram", Some(temporality), None)
            }
            MetricKind::ExponentialHistogram { temporality } => {
                ("exponential_histogram", Some(temporality), None)
            }
            MetricKind::Summary => ("summary", None, None),
        };
        if wants(columns, AnalyticsColumn::MetricKind) {
            row.metric_kind = Some(Arc::from(kind));
        }
        if wants(columns, AnalyticsColumn::Temporality) {
            row.temporality = temporality;
        }
        if wants(columns, AnalyticsColumn::Monotonic) {
            row.monotonic = monotonic;
        }
    }
    if wants(columns, AnalyticsColumn::Flags) {
        row.flags = Some(point.flags);
    }
    if wants(columns, AnalyticsColumn::Metadata) {
        row.metadata = attribute_map(&point.metadata);
    }
    if wants(columns, AnalyticsColumn::ValueType)
        || wants(columns, AnalyticsColumn::ScalarInteger)
        || wants(columns, AnalyticsColumn::ScalarDoubleBits)
    {
        match &point.value {
            MetricValue::Gauge(value) | MetricValue::Sum(value) => {
                set_projected_number(&mut row, columns, *value);
            }
            MetricValue::ExplicitHistogram(_) => {
                row.value_type = Some(Arc::from("explicit_histogram"));
            }
            MetricValue::ExponentialHistogram(_) => {
                row.value_type = Some(Arc::from("exponential_histogram"));
            }
            MetricValue::Summary(_) => row.value_type = Some(Arc::from("summary")),
        }
    }
    if wants(columns, AnalyticsColumn::ValueJson) {
        row.value_json = json(&point.value)?;
    }
    if wants(columns, AnalyticsColumn::ExemplarsJson) {
        row.exemplars_json = json(point.exemplars.as_ref())?;
    }
    Ok(row)
}

#[inline]
fn set_projected_number(row: &mut AnalyticsRow, columns: &[AnalyticsColumn], value: NumberValue) {
    match value {
        NumberValue::Integer(value) => {
            if wants(columns, AnalyticsColumn::ValueType) {
                row.value_type = Some(Arc::from("integer"));
            }
            if wants(columns, AnalyticsColumn::ScalarInteger) {
                row.scalar_integer = Some(value);
            }
        }
        NumberValue::DoubleBits(bits) => {
            if wants(columns, AnalyticsColumn::ValueType) {
                row.value_type = Some(Arc::from("double"));
            }
            if wants(columns, AnalyticsColumn::ScalarDoubleBits) {
                row.scalar_double_bits = Some(bits);
            }
        }
    }
}

fn populate_projected_context(
    row: &mut AnalyticsRow,
    columns: &[AnalyticsColumn],
    span: &DurableSpan,
) -> Result<(), LokiApiError> {
    if wants(columns, AnalyticsColumn::ResourceId) {
        row.resource_id = Some(Arc::from(span.resource_id().to_string()));
    }
    if wants(columns, AnalyticsColumn::ScopeId) {
        row.scope_id = Some(Arc::from(span.scope_id().to_string()));
    }
    if wants(columns, AnalyticsColumn::TraceId) {
        row.trace_id = Some(Arc::from(span.trace_id.to_string()));
    }
    if wants(columns, AnalyticsColumn::SpanId) {
        row.span_id = Some(Arc::from(span.span_id.to_string()));
    }
    Ok(())
}

fn populate_projected_attributes(
    row: &mut AnalyticsRow,
    columns: &[AnalyticsColumn],
    attributes: &[TelemetryAttribute],
    resource: &[TelemetryAttribute],
    scope: &[TelemetryAttribute],
) -> Result<(), LokiApiError> {
    if wants(columns, AnalyticsColumn::Attributes) {
        row.attributes = attribute_map(attributes);
    }
    if wants(columns, AnalyticsColumn::ResourceAttributes) {
        row.resource_attributes = attribute_map(resource);
    }
    if wants(columns, AnalyticsColumn::ScopeAttributes) {
        row.scope_attributes = attribute_map(scope);
    }
    if wants(columns, AnalyticsColumn::AttributeIds) {
        row.attribute_ids = attribute_ids(attributes);
    }
    if wants(columns, AnalyticsColumn::ResourceAttributeIds) {
        row.resource_attribute_ids = attribute_ids(resource);
    }
    if wants(columns, AnalyticsColumn::ScopeAttributeIds) {
        row.scope_attribute_ids = attribute_ids(scope);
    }
    if wants(columns, AnalyticsColumn::AttributesJson) {
        row.attributes_json = json(attributes)?;
    }
    if wants(columns, AnalyticsColumn::ResourceAttributesJson) {
        row.resource_attributes_json = json(resource)?;
    }
    if wants(columns, AnalyticsColumn::ScopeAttributesJson) {
        row.scope_attributes_json = json(scope)?;
    }
    Ok(())
}

fn wants(columns: &[AnalyticsColumn], column: AnalyticsColumn) -> bool {
    columns.contains(&column)
}

fn metric_base_row(
    point: &DurableMetricPoint,
    timestamp: u64,
) -> Result<AnalyticsRow, LokiApiError> {
    let identity = &point.identity;
    let mut row = AnalyticsRow::empty(
        Arc::clone(&identity.tenant),
        "metrics",
        timestamp,
        point.record_ref.topic_partition.partition_id.get(),
        point.record_ref.offset.get(),
    )?;
    row.resource_id = Some(Arc::from(identity.resource_id().to_string()));
    row.scope_id = Some(Arc::from(identity.scope_id().to_string()));
    row.series_id = Some(Arc::from(format!(
        "{:032x}",
        point.series_fingerprint().get()
    )));
    row.name = Some(Arc::clone(&identity.name));
    row.labels = crate::prometheus_string_labels(identity)
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    row.resource_attributes = attribute_map(&identity.resource.attributes);
    row.scope_attributes = attribute_map(&identity.scope.attributes);
    row.resource_attribute_ids = attribute_ids(&identity.resource.attributes);
    row.scope_attribute_ids = attribute_ids(&identity.scope.attributes);
    row.resource_attributes_json = json(identity.resource.attributes.as_ref())?;
    row.scope_attributes_json = json(identity.scope.attributes.as_ref())?;
    Ok(row)
}

fn metric_row(point: &DurableMetricPoint) -> Result<AnalyticsRow, LokiApiError> {
    let identity = &point.identity;
    let mut row = metric_base_row(point, point.timestamp_unix_nanos)?;
    row.start_timestamp_unix_nanos = nonzero_timestamp(point.start_time_unix_nanos)?;
    row.description = Some(Arc::clone(&point.description));
    row.unit = Some(Arc::clone(&identity.unit));
    let (kind, temporality, monotonic) = match identity.kind {
        MetricKind::Gauge => ("gauge", None, None),
        MetricKind::Sum {
            temporality,
            monotonic,
        } => ("sum", Some(temporality), Some(monotonic)),
        MetricKind::ExplicitHistogram { temporality } => {
            ("explicit_histogram", Some(temporality), None)
        }
        MetricKind::ExponentialHistogram { temporality } => {
            ("exponential_histogram", Some(temporality), None)
        }
        MetricKind::Summary => ("summary", None, None),
    };
    row.metric_kind = Some(Arc::from(kind));
    row.temporality = temporality;
    row.monotonic = monotonic;
    row.flags = Some(point.flags);
    row.metadata = attribute_map(&point.metadata);
    set_record_attributes(&mut row, &identity.point_attributes)?;
    match &point.value {
        MetricValue::Gauge(value) | MetricValue::Sum(value) => set_number(&mut row, *value),
        MetricValue::ExplicitHistogram(_) => row.value_type = Some(Arc::from("explicit_histogram")),
        MetricValue::ExponentialHistogram(_) => {
            row.value_type = Some(Arc::from("exponential_histogram"));
        }
        MetricValue::Summary(_) => row.value_type = Some(Arc::from("summary")),
    }
    row.value_json = json(&point.value)?;
    row.exemplars_json = json(point.exemplars.as_ref())?;
    Ok(row)
}

fn set_number(row: &mut AnalyticsRow, value: NumberValue) {
    match value {
        NumberValue::Integer(value) => {
            row.value_type = Some(Arc::from("integer"));
            row.scalar_integer = Some(value);
        }
        NumberValue::DoubleBits(bits) => {
            row.value_type = Some(Arc::from("double"));
            row.scalar_double_bits = Some(bits);
        }
    }
}

fn populate_attributes(
    row: &mut AnalyticsRow,
    attributes: &[TelemetryAttribute],
    resource: &[TelemetryAttribute],
    scope: &[TelemetryAttribute],
) -> Result<(), LokiApiError> {
    set_record_attributes(row, attributes)?;
    row.resource_attributes = attribute_map(resource);
    row.scope_attributes = attribute_map(scope);
    row.resource_attribute_ids = attribute_ids(resource);
    row.scope_attribute_ids = attribute_ids(scope);
    row.resource_attributes_json = json(resource)?;
    row.scope_attributes_json = json(scope)?;
    Ok(())
}

fn set_record_attributes(
    row: &mut AnalyticsRow,
    attributes: &[TelemetryAttribute],
) -> Result<(), LokiApiError> {
    row.attributes = attribute_map(attributes);
    row.attribute_ids = attribute_ids(attributes);
    row.attributes_json = json(attributes)?;
    Ok(())
}

fn attribute_map(attributes: &[TelemetryAttribute]) -> BTreeMap<String, String> {
    attributes
        .iter()
        .filter_map(|attribute| {
            attribute
                .value
                .as_ref()
                .map(|value| (attribute.key.to_string(), render_value(value)))
        })
        .collect()
}

fn attribute_ids(attributes: &[TelemetryAttribute]) -> BTreeMap<String, String> {
    attributes
        .iter()
        .map(|attribute| {
            (
                attribute.key.to_string(),
                attribute.fingerprint().to_string(),
            )
        })
        .collect()
}

fn render_value(value: &TelemetryValue) -> String {
    match value {
        TelemetryValue::Empty => String::new(),
        TelemetryValue::String(value) => value.to_string(),
        TelemetryValue::Boolean(value) => value.to_string(),
        TelemetryValue::Integer(value) => value.to_string(),
        TelemetryValue::DoubleBits(bits) => f64::from_bits(*bits).to_string(),
        TelemetryValue::Bytes(value) => value.iter().map(|byte| format!("{byte:02x}")).collect(),
        TelemetryValue::StringTableIndex(value) => value.to_string(),
        TelemetryValue::Array(_) | TelemetryValue::Map(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned())
        }
    }
}

fn json<T: serde::Serialize + ?Sized>(value: &T) -> Result<Option<Arc<str>>, LokiApiError> {
    serde_json::to_string(value)
        .map(|value| Some(Arc::from(value)))
        .map_err(|error| LokiApiError::internal(error.to_string()))
}

fn timestamp_i64(value: u64) -> Result<i64, LokiApiError> {
    i64::try_from(value)
        .map_err(|_| LokiApiError::internal("timestamp exceeds ClickHouse i64 range"))
}

fn nonzero_timestamp(value: u64) -> Result<Option<i64>, LokiApiError> {
    (value != 0).then(|| timestamp_i64(value)).transpose()
}

pub(crate) fn row_matches(row: &AnalyticsRow, request: &AnalyticsScanRequest) -> bool {
    let timestamp = u64::try_from(row.timestamp_unix_nanos).ok();
    if request
        .start_timestamp_unix_nanos
        .is_some_and(|start| timestamp.is_none_or(|value| value < start))
        || request
            .end_timestamp_unix_nanos
            .is_some_and(|end| timestamp.is_none_or(|value| value >= end))
        || request
            .trace_id
            .is_some_and(|value| row.trace_id.as_deref() != Some(value.to_string().as_str()))
        || request
            .span_id
            .is_some_and(|value| row.span_id.as_deref() != Some(value.to_string().as_str()))
        || request.series_id.is_some_and(|value| {
            row.series_id.as_deref() != Some(format!("{:032x}", value.get()).as_str())
        })
        || request
            .name
            .as_ref()
            .is_some_and(|value| row.name.as_deref() != Some(value.as_ref()))
    {
        return false;
    }
    fields_match(&row.labels, &request.labels)
        && fields_match(&row.metadata, &request.metadata)
        && fields_match(&row.attributes, &request.attributes)
        && fields_match(&row.resource_attributes, &request.resource_attributes)
        && fields_match(&row.scope_attributes, &request.scope_attributes)
        && request.terms.iter().all(|term| {
            row.message
                .as_deref()
                .is_some_and(|message| message_has_term(message, term))
        })
        && request.message_tokens.iter().all(|term| {
            row.message.as_deref().is_some_and(|message| {
                message_has_clickhouse_token(message, term, CaseSensitivity::Sensitive)
            })
        })
        && request.case_insensitive_message_tokens.iter().all(|term| {
            row.message.as_deref().is_some_and(|message| {
                message_has_clickhouse_token(message, term, CaseSensitivity::Insensitive)
            })
        })
}

fn fields_match(values: &BTreeMap<String, String>, expected: &[MetadataField]) -> bool {
    expected.iter().all(|field| {
        values.get(field.key.as_ref()).map(String::as_str) == Some(field.value.as_ref())
    })
}

pub(crate) fn analytics_stream_response(
    store: Arc<dyn LokiStore>,
    request: AnalyticsScanRequest,
) -> Response {
    match request.wire_format {
        AnalyticsWireFormat::ArrowStream => arrow_stream_response(store, request),
        AnalyticsWireFormat::RowBinary => rowbinary_stream_response(store, request),
        AnalyticsWireFormat::JsonLines => jsonlines_stream_response(store, request),
    }
}

fn arrow_stream_response(store: Arc<dyn LokiStore>, request: AnalyticsScanRequest) -> Response {
    let relation = request.relation.name();
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::task::spawn_blocking(move || {
        if let Err(error) = write_arrow_stream(store, &request, sender.clone()) {
            let _ = sender.blocking_send(Err(io::Error::other(error.to_string())));
        }
    });
    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apache.arrow.stream"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-schema-version"),
        HeaderValue::from_static("1"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-relation"),
        HeaderValue::from_static(relation),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-clickhouse-target"),
        HeaderValue::from_static(CLICKHOUSE_COMPATIBILITY_TARGET),
    );
    response
}

fn rowbinary_stream_response(store: Arc<dyn LokiStore>, request: AnalyticsScanRequest) -> Response {
    let relation = request.relation.name();
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::task::spawn_blocking(move || {
        let mut sink = ChannelWriter::new(sender.clone(), STREAM_CHUNK_BYTES);
        let result = write_rowbinary_stream(store, &request, &mut sink).and_then(|()| {
            sink.finish()
                .map_err(|error| LokiApiError::internal(error.to_string()))
        });
        if let Err(error) = result {
            let _ = sender.blocking_send(Err(io::Error::other(error.to_string())));
        }
    });
    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-schema-version"),
        HeaderValue::from_static("1"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-relation"),
        HeaderValue::from_static(relation),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-clickhouse-target"),
        HeaderValue::from_static(CLICKHOUSE_COMPATIBILITY_TARGET),
    );
    response
}

fn jsonlines_stream_response(store: Arc<dyn LokiStore>, request: AnalyticsScanRequest) -> Response {
    let relation = request.relation.name();
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::task::spawn_blocking(move || {
        let mut sink = ChannelWriter::new(sender.clone(), STREAM_CHUNK_BYTES);
        let result = write_jsonlines_stream(store, &request, &mut sink).and_then(|()| {
            sink.finish()
                .map_err(|error| LokiApiError::internal(error.to_string()))
        });
        if let Err(error) = result {
            let _ = sender.blocking_send(Err(io::Error::other(error.to_string())));
        }
    });
    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-schema-version"),
        HeaderValue::from_static("1"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-relation"),
        HeaderValue::from_static(relation),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-clickhouse-target"),
        HeaderValue::from_static(CLICKHOUSE_COMPATIBILITY_TARGET),
    );
    response
}

fn write_rowbinary_stream(
    store: Arc<dyn LokiStore>,
    request: &AnalyticsScanRequest,
    writer: &mut dyn Write,
) -> Result<(), LokiApiError> {
    if request.cardinality_only {
        let row = rowbinary_default_value(request.columns[0]);
        let mut batch = Vec::with_capacity(row.len() * DEFAULT_SCAN_BATCH_ROWS);
        for _ in 0..DEFAULT_SCAN_BATCH_ROWS {
            batch.extend_from_slice(&row);
        }
        store.scan_analytics_cardinality(request, &mut |count| {
            let mut remaining = count;
            while remaining > 0 {
                let rows = remaining.min(DEFAULT_SCAN_BATCH_ROWS as u64) as usize;
                writer
                    .write_all(&batch[..rows * row.len()])
                    .map_err(rowbinary_error)?;
                remaining -= rows as u64;
            }
            Ok(())
        })?;
        return Ok(());
    }
    if store.scan_analytics_rowbinary(request, writer)? {
        return Ok(());
    }
    store.scan_analytics(request, &mut |rows| {
        for row in rows {
            write_rowbinary_row(writer, row, &request.columns)?;
        }
        Ok(())
    })
}

fn write_jsonlines_stream(
    store: Arc<dyn LokiStore>,
    request: &AnalyticsScanRequest,
    writer: &mut dyn Write,
) -> Result<(), LokiApiError> {
    debug_assert!(!request.cardinality_only);
    store.scan_analytics(request, &mut |rows| {
        for row in rows {
            write_jsonlines_row(writer, row, &request.columns)?;
        }
        Ok(())
    })
}

fn write_jsonlines_row(
    writer: &mut dyn Write,
    row: &AnalyticsRow,
    columns: &[AnalyticsColumn],
) -> Result<(), LokiApiError> {
    let mut object = JsonMap::with_capacity(columns.len());
    for column in columns {
        object.insert(column.name().to_owned(), json_column_value(row, *column));
    }
    serde_json::to_writer(&mut *writer, &JsonValue::Object(object))
        .map_err(|error| LokiApiError::internal(error.to_string()))?;
    writer.write_all(b"\n").map_err(rowbinary_error)
}

fn json_column_value(row: &AnalyticsRow, column: AnalyticsColumn) -> JsonValue {
    match column.field().data_type() {
        DataType::Utf8 => string_value(row, column)
            .map(|value| JsonValue::String(value.to_owned()))
            .unwrap_or(JsonValue::Null),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => timestamp_value(row, column)
            .map(JsonValue::from)
            .unwrap_or(JsonValue::Null),
        DataType::UInt32 => u32_value(row, column)
            .map(JsonValue::from)
            .unwrap_or(JsonValue::Null),
        DataType::UInt64 => u64_value(row, column)
            .map(JsonValue::from)
            .unwrap_or(JsonValue::Null),
        DataType::Int32 => i32_value(row, column)
            .map(JsonValue::from)
            .unwrap_or(JsonValue::Null),
        DataType::Int64 => row
            .scalar_integer
            .map(JsonValue::from)
            .unwrap_or(JsonValue::Null),
        DataType::Boolean => row
            .monotonic
            .map(JsonValue::from)
            .unwrap_or(JsonValue::Null),
        DataType::Map(_, _) => JsonValue::Object(
            map_value(row, column)
                .iter()
                .map(|(key, value)| (key.clone(), JsonValue::String(value.clone())))
                .collect(),
        ),
        _ => unreachable!("public analytical columns use supported JSON types"),
    }
}

fn rowbinary_default_value(column: AnalyticsColumn) -> Vec<u8> {
    let field = column.field();
    if field.is_nullable() {
        return vec![1];
    }
    match field.data_type() {
        DataType::Utf8 | DataType::Boolean | DataType::Map(_, _) => vec![0],
        DataType::UInt32 | DataType::Int32 => vec![0; size_of::<u32>()],
        DataType::Timestamp(TimeUnit::Nanosecond, _) | DataType::UInt64 | DataType::Int64 => {
            vec![0; size_of::<u64>()]
        }
        _ => unreachable!("public analytical columns use supported RowBinary types"),
    }
}

fn write_rowbinary_row(
    writer: &mut dyn Write,
    row: &AnalyticsRow,
    columns: &[AnalyticsColumn],
) -> Result<(), LokiApiError> {
    for column in columns {
        let field = column.field();
        match field.data_type() {
            DataType::Utf8 => {
                let value = string_value(row, *column);
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    write_rowbinary_bytes(writer, value.expect("presence was checked").as_bytes())?;
                }
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let value = timestamp_value(row, *column);
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&value.expect("presence was checked").to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::UInt32 => {
                let value = u32_value(row, *column);
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&value.expect("presence was checked").to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::UInt64 => {
                let value = u64_value(row, *column);
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&value.expect("presence was checked").to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::Int32 => {
                let value = i32_value(row, *column);
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&value.expect("presence was checked").to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::Int64 => {
                let value = row.scalar_integer;
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&value.expect("presence was checked").to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::Boolean => {
                let value = row.monotonic;
                if write_rowbinary_presence(writer, value.is_some(), field.is_nullable())? {
                    writer
                        .write_all(&[u8::from(value.expect("presence was checked"))])
                        .map_err(rowbinary_error)?;
                }
            }
            DataType::Map(_, _) => {
                let values = map_value(row, *column);
                write_rowbinary_varuint(writer, values.len())?;
                for (key, value) in values {
                    write_rowbinary_bytes(writer, key.as_bytes())?;
                    write_rowbinary_bytes(writer, value.as_bytes())?;
                }
            }
            _ => {
                return Err(LokiApiError::internal(
                    "unsupported RowBinary analytics type",
                ));
            }
        }
    }
    Ok(())
}

fn write_rowbinary_presence(
    writer: &mut dyn Write,
    present: bool,
    nullable: bool,
) -> Result<bool, LokiApiError> {
    if nullable {
        writer
            .write_all(&[u8::from(!present)])
            .map_err(rowbinary_error)?;
        Ok(present)
    } else if present {
        Ok(true)
    } else {
        Err(LokiApiError::internal(
            "non-nullable RowBinary column has no value",
        ))
    }
}

fn write_rowbinary_bytes(writer: &mut dyn Write, value: &[u8]) -> Result<(), LokiApiError> {
    write_rowbinary_varuint(writer, value.len())?;
    writer.write_all(value).map_err(rowbinary_error)
}

fn write_rowbinary_varuint(writer: &mut dyn Write, mut value: usize) -> Result<(), LokiApiError> {
    while value >= 0x80 {
        writer
            .write_all(&[((value as u8) & 0x7f) | 0x80])
            .map_err(rowbinary_error)?;
        value >>= 7;
    }
    writer.write_all(&[value as u8]).map_err(rowbinary_error)
}

fn rowbinary_error(error: io::Error) -> LokiApiError {
    LokiApiError::internal(error.to_string())
}

fn write_arrow_stream(
    store: Arc<dyn LokiStore>,
    request: &AnalyticsScanRequest,
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
) -> Result<(), LokiApiError> {
    let schema = projection_schema(&request.columns);
    let mut sink = ChannelWriter::new(sender, STREAM_CHUNK_BYTES);
    {
        let mut writer = StreamWriter::try_new(&mut sink, &schema)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        if request.cardinality_only {
            store.scan_analytics_cardinality(request, &mut |count| {
                let mut remaining = count;
                while remaining > 0 {
                    let rows = remaining.min(DEFAULT_SCAN_BATCH_ROWS as u64) as usize;
                    let batch = RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(UInt64Array::from(vec![0_u64; rows])) as ArrayRef],
                    )
                    .map_err(|error| LokiApiError::internal(error.to_string()))?;
                    writer
                        .write(&batch)
                        .map_err(|error| LokiApiError::internal(error.to_string()))?;
                    remaining -= rows as u64;
                }
                Ok(())
            })?;
        } else if !store.scan_analytics_arrow(request, &schema, &mut |batch| {
            writer
                .write(batch)
                .map_err(|error| LokiApiError::internal(error.to_string()))
        })? {
            store.scan_analytics(request, &mut |rows| {
                let batch = record_batch(rows, &request.columns, Arc::clone(&schema))?;
                writer
                    .write(&batch)
                    .map_err(|error| LokiApiError::internal(error.to_string()))
            })?;
        }
        writer
            .finish()
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
    }
    sink.finish()
        .map_err(|error| LokiApiError::internal(error.to_string()))
}

fn projection_schema(columns: &[AnalyticsColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .copied()
            .map(AnalyticsColumn::field)
            .collect::<Vec<_>>(),
    ))
}

fn record_batch(
    rows: &[AnalyticsRow],
    columns: &[AnalyticsColumn],
    schema: SchemaRef,
) -> Result<RecordBatch, LokiApiError> {
    let arrays = columns
        .iter()
        .copied()
        .map(|column| column_array(rows, column))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, arrays).map_err(|error| LokiApiError::internal(error.to_string()))
}

pub(crate) fn direct_metric_record_batch(
    points: &[DurableMetricPoint],
    columns: &[AnalyticsColumn],
    schema: SchemaRef,
) -> Result<Option<RecordBatch>, LokiApiError> {
    if !can_direct_metric_projection(columns) {
        return Ok(None);
    }
    let arrays = columns
        .iter()
        .map(|column| -> Result<ArrayRef, LokiApiError> {
            Ok(match column {
                AnalyticsColumn::Timestamp => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(points.len());
                    for point in points {
                        builder.append_value(timestamp_i64(point.timestamp_unix_nanos)?);
                    }
                    Arc::new(builder.finish().with_timezone("UTC"))
                }
                AnalyticsColumn::StartTimestamp => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(points.len());
                    for point in points {
                        if let Some(value) = nonzero_timestamp(point.start_time_unix_nanos)? {
                            builder.append_value(value);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish().with_timezone("UTC"))
                }
                AnalyticsColumn::Partition => {
                    Arc::new(UInt32Array::from_iter_values(points.iter().map(|point| {
                        point.record_ref.topic_partition.partition_id.get()
                    })))
                }
                AnalyticsColumn::Offset => Arc::new(UInt64Array::from_iter_values(
                    points.iter().map(|point| point.record_ref.offset.get()),
                )),
                AnalyticsColumn::Name => {
                    let mut builder = StringBuilder::new();
                    for point in points {
                        builder.append_value(point.identity.name.as_ref());
                    }
                    Arc::new(builder.finish())
                }
                AnalyticsColumn::ScalarInteger => {
                    let mut builder = Int64Builder::with_capacity(points.len());
                    for point in points {
                        match point.value {
                            MetricValue::Gauge(NumberValue::Integer(value))
                            | MetricValue::Sum(NumberValue::Integer(value)) => {
                                builder.append_value(value);
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                AnalyticsColumn::ScalarDoubleBits => {
                    let mut builder = UInt64Builder::with_capacity(points.len());
                    for point in points {
                        match point.value {
                            MetricValue::Gauge(NumberValue::DoubleBits(value))
                            | MetricValue::Sum(NumberValue::DoubleBits(value)) => {
                                builder.append_value(value);
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => unreachable!("direct metric projection was validated"),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, arrays)
        .map(Some)
        .map_err(|error| LokiApiError::internal(error.to_string()))
}

pub(crate) fn can_direct_metric_projection(columns: &[AnalyticsColumn]) -> bool {
    columns.iter().all(|column| {
        matches!(
            column,
            AnalyticsColumn::Timestamp
                | AnalyticsColumn::StartTimestamp
                | AnalyticsColumn::Partition
                | AnalyticsColumn::Offset
                | AnalyticsColumn::Name
                | AnalyticsColumn::ScalarInteger
                | AnalyticsColumn::ScalarDoubleBits
        )
    })
}

pub(crate) fn direct_span_record_batch(
    spans: &[DurableSpan],
    columns: &[AnalyticsColumn],
    schema: SchemaRef,
) -> Result<Option<RecordBatch>, LokiApiError> {
    if !can_direct_span_projection(columns) {
        return Ok(None);
    }
    let arrays = columns
        .iter()
        .map(|column| -> Result<ArrayRef, LokiApiError> {
            Ok(match column {
                AnalyticsColumn::Timestamp => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(spans.len());
                    for span in spans {
                        builder.append_value(timestamp_i64(span.start_time_unix_nanos)?);
                    }
                    Arc::new(builder.finish().with_timezone("UTC"))
                }
                AnalyticsColumn::EndTimestamp => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(spans.len());
                    for span in spans {
                        if let Some(value) = span.end_time_unix_nanos() {
                            builder.append_value(timestamp_i64(value)?);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish().with_timezone("UTC"))
                }
                AnalyticsColumn::Partition => Arc::new(UInt32Array::from_iter_values(
                    spans
                        .iter()
                        .map(|span| span.record_ref.topic_partition.partition_id.get()),
                )),
                AnalyticsColumn::Offset => Arc::new(UInt64Array::from_iter_values(
                    spans.iter().map(|span| span.record_ref.offset.get()),
                )),
                AnalyticsColumn::Name => {
                    let mut builder = StringBuilder::new();
                    for span in spans {
                        builder.append_value(span.name.as_ref());
                    }
                    Arc::new(builder.finish())
                }
                AnalyticsColumn::Kind => Arc::new(Int32Array::from_iter_values(
                    spans.iter().map(|span| span.kind),
                )),
                AnalyticsColumn::DurationNanos => Arc::new(UInt64Array::from_iter_values(
                    spans.iter().map(|span| span.duration_nanos),
                )),
                AnalyticsColumn::StatusCode => Arc::new(Int32Array::from_iter(
                    spans
                        .iter()
                        .map(|span| span.status.as_ref().map(|status| status.code)),
                )),
                _ => unreachable!("direct span projection was validated"),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, arrays)
        .map(Some)
        .map_err(|error| LokiApiError::internal(error.to_string()))
}

pub(crate) fn can_direct_span_projection(columns: &[AnalyticsColumn]) -> bool {
    columns.iter().all(|column| {
        matches!(
            column,
            AnalyticsColumn::Timestamp
                | AnalyticsColumn::EndTimestamp
                | AnalyticsColumn::Partition
                | AnalyticsColumn::Offset
                | AnalyticsColumn::Name
                | AnalyticsColumn::Kind
                | AnalyticsColumn::DurationNanos
                | AnalyticsColumn::StatusCode
        )
    })
}

pub(crate) fn write_direct_metric_rowbinary(
    points: &[DurableMetricPoint],
    columns: &[AnalyticsColumn],
    writer: &mut dyn Write,
) -> Result<bool, LokiApiError> {
    if !can_direct_metric_projection(columns) {
        return Ok(false);
    }
    for point in points {
        for column in columns {
            match column {
                AnalyticsColumn::Timestamp => writer
                    .write_all(&timestamp_i64(point.timestamp_unix_nanos)?.to_le_bytes())
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::StartTimestamp => {
                    let value = nonzero_timestamp(point.start_time_unix_nanos)?;
                    if write_rowbinary_presence(writer, value.is_some(), true)? {
                        writer
                            .write_all(&value.expect("presence was checked").to_le_bytes())
                            .map_err(rowbinary_error)?;
                    }
                }
                AnalyticsColumn::Partition => writer
                    .write_all(
                        &point
                            .record_ref
                            .topic_partition
                            .partition_id
                            .get()
                            .to_le_bytes(),
                    )
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::Offset => writer
                    .write_all(&point.record_ref.offset.get().to_le_bytes())
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::Name => {
                    write_rowbinary_presence(writer, true, true)?;
                    write_rowbinary_bytes(writer, point.identity.name.as_bytes())?;
                }
                AnalyticsColumn::ScalarInteger => {
                    let value = match point.value {
                        MetricValue::Gauge(NumberValue::Integer(value))
                        | MetricValue::Sum(NumberValue::Integer(value)) => Some(value),
                        _ => None,
                    };
                    if write_rowbinary_presence(writer, value.is_some(), true)? {
                        writer
                            .write_all(&value.expect("presence was checked").to_le_bytes())
                            .map_err(rowbinary_error)?;
                    }
                }
                AnalyticsColumn::ScalarDoubleBits => {
                    let value = match point.value {
                        MetricValue::Gauge(NumberValue::DoubleBits(value))
                        | MetricValue::Sum(NumberValue::DoubleBits(value)) => Some(value),
                        _ => None,
                    };
                    if write_rowbinary_presence(writer, value.is_some(), true)? {
                        writer
                            .write_all(&value.expect("presence was checked").to_le_bytes())
                            .map_err(rowbinary_error)?;
                    }
                }
                _ => unreachable!("direct metric RowBinary projection was validated"),
            }
        }
    }
    Ok(true)
}

pub(crate) fn write_direct_span_rowbinary(
    spans: &[DurableSpan],
    columns: &[AnalyticsColumn],
    writer: &mut dyn Write,
) -> Result<bool, LokiApiError> {
    if !can_direct_span_projection(columns) {
        return Ok(false);
    }
    for span in spans {
        for column in columns {
            match column {
                AnalyticsColumn::Timestamp => writer
                    .write_all(&timestamp_i64(span.start_time_unix_nanos)?.to_le_bytes())
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::EndTimestamp => {
                    let value = span.end_time_unix_nanos().map(timestamp_i64).transpose()?;
                    if write_rowbinary_presence(writer, value.is_some(), true)? {
                        writer
                            .write_all(&value.expect("presence was checked").to_le_bytes())
                            .map_err(rowbinary_error)?;
                    }
                }
                AnalyticsColumn::Partition => writer
                    .write_all(
                        &span
                            .record_ref
                            .topic_partition
                            .partition_id
                            .get()
                            .to_le_bytes(),
                    )
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::Offset => writer
                    .write_all(&span.record_ref.offset.get().to_le_bytes())
                    .map_err(rowbinary_error)?,
                AnalyticsColumn::Name => {
                    write_rowbinary_presence(writer, true, true)?;
                    write_rowbinary_bytes(writer, span.name.as_bytes())?;
                }
                AnalyticsColumn::Kind => {
                    write_rowbinary_presence(writer, true, true)?;
                    writer
                        .write_all(&span.kind.to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
                AnalyticsColumn::DurationNanos => {
                    write_rowbinary_presence(writer, true, true)?;
                    writer
                        .write_all(&span.duration_nanos.to_le_bytes())
                        .map_err(rowbinary_error)?;
                }
                AnalyticsColumn::StatusCode => {
                    let value = span.status.as_ref().map(|status| status.code);
                    if write_rowbinary_presence(writer, value.is_some(), true)? {
                        writer
                            .write_all(&value.expect("presence was checked").to_le_bytes())
                            .map_err(rowbinary_error)?;
                    }
                }
                _ => unreachable!("direct span RowBinary projection was validated"),
            }
        }
    }
    Ok(true)
}

fn column_array(rows: &[AnalyticsRow], column: AnalyticsColumn) -> Result<ArrayRef, LokiApiError> {
    let array: ArrayRef = match column.field().data_type() {
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in rows {
                if let Some(value) = string_value(row, column) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let mut builder = TimestampNanosecondBuilder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = timestamp_value(row, column) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish().with_timezone("UTC"))
        }
        DataType::UInt32 => {
            let mut builder = UInt32Builder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = u32_value(row, column) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = u64_value(row, column) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = i32_value(row, column) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = row.scalar_integer {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                if let Some(value) = row.monotonic {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Map(_, _) => Arc::new(string_map_array(
            rows.iter().map(|row| map_value(row, column)),
        )?),
        _ => return Err(LokiApiError::internal("unsupported analytics Arrow type")),
    };
    Ok(array)
}

fn string_value(row: &AnalyticsRow, column: AnalyticsColumn) -> Option<&str> {
    match column {
        AnalyticsColumn::Tenant => Some(&row.tenant),
        AnalyticsColumn::Signal => Some(&row.signal),
        AnalyticsColumn::ResourceId => row.resource_id.as_deref(),
        AnalyticsColumn::ScopeId => row.scope_id.as_deref(),
        AnalyticsColumn::TraceId => row.trace_id.as_deref(),
        AnalyticsColumn::SpanId => row.span_id.as_deref(),
        AnalyticsColumn::ParentSpanId => row.parent_span_id.as_deref(),
        AnalyticsColumn::LinkedTraceId => row.linked_trace_id.as_deref(),
        AnalyticsColumn::LinkedSpanId => row.linked_span_id.as_deref(),
        AnalyticsColumn::SeriesId => row.series_id.as_deref(),
        AnalyticsColumn::Message => row.message.as_deref(),
        AnalyticsColumn::BodyJson => row.body_json.as_deref(),
        AnalyticsColumn::Name => row.name.as_deref(),
        AnalyticsColumn::EventName => row.event_name.as_deref(),
        AnalyticsColumn::SeverityText => row.severity_text.as_deref(),
        AnalyticsColumn::StatusMessage => row.status_message.as_deref(),
        AnalyticsColumn::TraceState => row.trace_state.as_deref(),
        AnalyticsColumn::AttributesJson => row.attributes_json.as_deref(),
        AnalyticsColumn::ResourceAttributesJson => row.resource_attributes_json.as_deref(),
        AnalyticsColumn::ScopeAttributesJson => row.scope_attributes_json.as_deref(),
        AnalyticsColumn::EventsJson => row.events_json.as_deref(),
        AnalyticsColumn::LinksJson => row.links_json.as_deref(),
        AnalyticsColumn::Description => row.description.as_deref(),
        AnalyticsColumn::Unit => row.unit.as_deref(),
        AnalyticsColumn::MetricKind => row.metric_kind.as_deref(),
        AnalyticsColumn::ValueType => row.value_type.as_deref(),
        AnalyticsColumn::ValueJson => row.value_json.as_deref(),
        AnalyticsColumn::ExemplarsJson => row.exemplars_json.as_deref(),
        _ => None,
    }
}

fn timestamp_value(row: &AnalyticsRow, column: AnalyticsColumn) -> Option<i64> {
    match column {
        AnalyticsColumn::Timestamp => Some(row.timestamp_unix_nanos),
        AnalyticsColumn::ParentTimestamp => row.parent_timestamp_unix_nanos,
        AnalyticsColumn::ObservedTimestamp => row.observed_timestamp_unix_nanos,
        AnalyticsColumn::StartTimestamp => row.start_timestamp_unix_nanos,
        AnalyticsColumn::EndTimestamp => row.end_timestamp_unix_nanos,
        _ => None,
    }
}

fn u32_value(row: &AnalyticsRow, column: AnalyticsColumn) -> Option<u32> {
    match column {
        AnalyticsColumn::Partition => Some(row.partition),
        AnalyticsColumn::Ordinal => row.ordinal,
        AnalyticsColumn::Flags => row.flags,
        AnalyticsColumn::DroppedAttributesCount => row.dropped_attributes_count,
        AnalyticsColumn::DroppedEventsCount => row.dropped_events_count,
        AnalyticsColumn::DroppedLinksCount => row.dropped_links_count,
        _ => None,
    }
}

fn u64_value(row: &AnalyticsRow, column: AnalyticsColumn) -> Option<u64> {
    match column {
        AnalyticsColumn::Offset => Some(row.offset),
        AnalyticsColumn::DurationNanos => row.duration_nanos,
        AnalyticsColumn::ScalarDoubleBits => row.scalar_double_bits,
        _ => None,
    }
}

fn i32_value(row: &AnalyticsRow, column: AnalyticsColumn) -> Option<i32> {
    match column {
        AnalyticsColumn::SeverityNumber => row.severity_number,
        AnalyticsColumn::Kind => row.kind,
        AnalyticsColumn::StatusCode => row.status_code,
        AnalyticsColumn::Temporality => row.temporality,
        _ => None,
    }
}

fn map_value(row: &AnalyticsRow, column: AnalyticsColumn) -> &BTreeMap<String, String> {
    match column {
        AnalyticsColumn::Labels => &row.labels,
        AnalyticsColumn::Metadata => &row.metadata,
        AnalyticsColumn::Attributes => &row.attributes,
        AnalyticsColumn::ResourceAttributes => &row.resource_attributes,
        AnalyticsColumn::ScopeAttributes => &row.scope_attributes,
        AnalyticsColumn::AttributeIds => &row.attribute_ids,
        AnalyticsColumn::ResourceAttributeIds => &row.resource_attribute_ids,
        AnalyticsColumn::ScopeAttributeIds => &row.scope_attribute_ids,
        _ => unreachable!("column type checked before map lookup"),
    }
}

fn string_map_data_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

fn string_map_array<'a>(
    rows: impl Iterator<Item = &'a BTreeMap<String, String>>,
) -> Result<arrow_array::MapArray, LokiApiError> {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for values in rows {
        for (key, value) in values {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder
            .append(true)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
    }
    Ok(builder.finish())
}

struct ChannelWriter {
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
    bytes: Vec<u8>,
    chunk_bytes: usize,
}

impl ChannelWriter {
    fn new(sender: mpsc::Sender<Result<Bytes, io::Error>>, chunk_bytes: usize) -> Self {
        Self {
            sender,
            bytes: Vec::with_capacity(chunk_bytes),
            chunk_bytes,
        }
    }

    fn emit(&mut self) -> io::Result<()> {
        if self.bytes.is_empty() {
            return Ok(());
        }
        let bytes = Bytes::from(std::mem::take(&mut self.bytes));
        self.bytes = Vec::with_capacity(self.chunk_bytes);
        self.sender
            .blocking_send(Ok(bytes))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "analytics client disconnected"))
    }

    fn finish(&mut self) -> io::Result<()> {
        self.emit()
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(buffer);
        if self.bytes.len() >= self.chunk_bytes {
            self.emit()?;
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // Arrow's stream writer flushes after the schema and every IPC message.
        // Emitting each flush as its own HTTP body chunk creates a small schema
        // packet followed by a data packet, which can hit the TCP delayed-ACK
        // timer. The size threshold and explicit `finish` retain bounded
        // streaming while coalescing adjacent IPC messages.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow_array::{MapArray, StringArray, TimestampNanosecondArray};
    use arrow_ipc::reader::StreamReader;

    use super::*;
    use crate::LokiEntry;

    #[test]
    fn channel_writer_coalesces_arrow_flushes_until_size_or_finish() {
        let (sender, mut receiver) = mpsc::channel(8);
        let mut writer = ChannelWriter::new(sender, 64);
        writer.write_all(b"schema").unwrap();
        writer.flush().unwrap();
        assert!(receiver.try_recv().is_err());
        writer.write_all(b"batch").unwrap();
        writer.finish().unwrap();
        assert_eq!(
            receiver.try_recv().unwrap().unwrap().as_ref(),
            b"schemabatch"
        );
    }

    #[test]
    fn request_parses_relation_columns_ids_and_pushdown_constraints() {
        let request = parse_scan_request(
            "tenant-a".to_owned(),
            Some("relation=spans&start_ns=10&end_ns=20&trace_id=01010101010101010101010101010101&resource.service.name=api&columns=timestamp,trace_id,name&limit=7"),
        )
        .expect("valid scan");
        assert_eq!(request.relation, AnalyticsRelation::Spans);
        assert_eq!(request.start_timestamp_unix_nanos, Some(10));
        assert_eq!(request.end_timestamp_unix_nanos, Some(20));
        assert_eq!(
            request.trace_id.expect("trace").to_string(),
            "01010101010101010101010101010101"
        );
        assert_eq!(
            request.resource_attributes[0],
            MetadataField::new("service.name", "api")
        );
        assert_eq!(
            request.columns,
            vec![
                AnalyticsColumn::Timestamp,
                AnalyticsColumn::TraceId,
                AnalyticsColumn::Name
            ]
        );
        assert_eq!(request.limit, Some(7));
    }

    #[test]
    fn request_rejects_ambiguous_or_relation_incompatible_inputs() {
        for query in [
            "unknown=value",
            "relation=logs&relation=spans",
            "relation=unknown",
            "relation=spans&columns=message",
            "columns=timestamp&columns=message",
            "columns=timestamp,timestamp",
            "columns=",
            "start_ns=20&end_ns=20",
            "start_ns=-1",
            "limit=-1",
            "label.=value",
            "relation=spans&term=error",
            "relation=spans&message_token=error",
            "relation=spans&message_token_ci=error",
            "trace_id=00",
            "columns=offset&cardinality_only=maybe",
            "columns=offset&cardinality_only=1&cardinality_only=1",
            "columns=message&cardinality_only=1",
            "columns=partition,offset&cardinality_only=1&wire=rowbinary",
            "order=timestamp_desc",
            "limit=1&order=unknown",
            "limit=1&order=timestamp_desc&order=timestamp_asc",
            "relation=spans&limit=1&order=timestamp_desc",
        ] {
            assert!(
                parse_scan_request("tenant-a".to_owned(), Some(query)).is_err(),
                "query must fail closed: {query}"
            );
        }
    }

    #[test]
    fn cardinality_request_is_explicit_and_uses_one_fixed_width_lane() {
        let request = parse_scan_request(
            "tenant-a".to_owned(),
            Some("columns=offset&cardinality_only=1"),
        )
        .expect("cardinality request");
        assert!(request.cardinality_only);
        assert_eq!(request.columns, [AnalyticsColumn::Offset]);

        let rowbinary = parse_scan_request(
            "tenant-a".to_owned(),
            Some("columns=partition&cardinality_only=1&wire=rowbinary"),
        )
        .expect("RowBinary cardinality request");
        assert!(rowbinary.cardinality_only);
        assert_eq!(rowbinary.columns, [AnalyticsColumn::Partition]);

        assert!(
            parse_scan_request(
                "tenant-a".to_owned(),
                Some("columns=offset&cardinality_only=1&wire=jsonl"),
            )
            .is_err()
        );
    }

    #[test]
    fn bounded_timestamp_order_is_parsed_explicitly() {
        let request = parse_scan_request(
            "tenant-a".to_owned(),
            Some("columns=timestamp,message&limit=100&order=timestamp_desc"),
        )
        .expect("ordered request");
        assert_eq!(request.order, Some(AnalyticsScanOrder::TimestampDescending));
        assert_eq!(request.limit, Some(100));
    }

    #[test]
    fn exact_message_token_is_parsed_for_log_scans() {
        let request = parse_scan_request(
            "tenant-a".to_owned(),
            Some("message_token=Cannot&message_token_ci=cannot&limit=10&order=timestamp_desc"),
        )
        .expect("exact token scan");
        assert_eq!(request.message_tokens, [Arc::<str>::from("Cannot")]);
        assert_eq!(
            request.case_insensitive_message_tokens,
            [Arc::<str>::from("cannot")]
        );
    }

    #[test]
    fn zero_limit_is_a_valid_empty_scan() {
        let entries = vec![LokiEntry {
            timestamp_unix_nanos: 11,
            labels: BTreeMap::new(),
            line: "must not be emitted".to_owned(),
            structured_metadata: BTreeMap::new(),
        }];
        let request = parse_scan_request("tenant-a".to_owned(), Some("limit=0"))
            .expect("zero limit is valid");
        let mut called = false;
        scan_entries(entries, &request, &mut |_| {
            called = true;
            Ok(())
        })
        .expect("empty scan");
        assert!(!called);
    }

    #[test]
    fn projected_arrow_batch_preserves_timestamp_message_and_maps() {
        let mut row = AnalyticsRow::empty(Arc::from("tenant-a"), "logs", 123, 4, 9).expect("row");
        row.message = Some(Arc::from("request failed"));
        row.labels.insert("app".to_owned(), "api".to_owned());
        row.metadata.insert("code".to_owned(), "500".to_owned());
        let columns = vec![
            AnalyticsColumn::Timestamp,
            AnalyticsColumn::Message,
            AnalyticsColumn::Labels,
        ];
        let schema = projection_schema(&columns);
        let batch = record_batch(&[row], &columns, Arc::clone(&schema)).expect("batch");
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).expect("writer");
            writer.write(&batch).expect("write");
            writer.finish().expect("finish");
        }
        let mut reader = StreamReader::try_new(Cursor::new(bytes), None).expect("reader");
        let decoded = reader.next().expect("one batch").expect("valid batch");
        assert_eq!(
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0),
            123
        );
        assert_eq!(
            decoded
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "request failed"
        );
        assert_eq!(
            decoded
                .column(2)
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap()
                .value_length(0),
            1
        );
    }

    #[test]
    fn jsonlines_projection_preserves_exact_numeric_and_map_values() {
        let mut row = AnalyticsRow::empty(
            Arc::from("tenant-a"),
            "logs",
            1_800_000_000_000_000_001,
            4,
            9,
        )
        .expect("row");
        row.message = Some(Arc::from("request failed"));
        row.labels.insert("app".to_owned(), "api".to_owned());
        row.metadata.insert("code".to_owned(), "500".to_owned());
        let columns = vec![
            AnalyticsColumn::Timestamp,
            AnalyticsColumn::Offset,
            AnalyticsColumn::Message,
            AnalyticsColumn::Labels,
            AnalyticsColumn::Metadata,
        ];
        let mut output = Vec::new();
        write_jsonlines_row(&mut output, &row, &columns).expect("JSON lines row");
        assert!(output.ends_with(b"\n"));
        let value: JsonValue = serde_json::from_slice(&output).expect("valid JSON line");
        assert_eq!(value["timestamp"], 1_800_000_000_000_000_001_i64);
        assert_eq!(value["offset"], 9_u64);
        assert_eq!(value["message"], "request failed");
        assert_eq!(value["labels"]["app"], "api");
        assert_eq!(value["metadata"]["code"], "500");
    }

    #[test]
    fn every_relation_builds_its_complete_declared_arrow_schema() {
        for relation in [
            AnalyticsRelation::Logs,
            AnalyticsRelation::Spans,
            AnalyticsRelation::SpanEvents,
            AnalyticsRelation::SpanLinks,
            AnalyticsRelation::MetricPoints,
            AnalyticsRelation::MetricExemplars,
        ] {
            let row = AnalyticsRow::empty(Arc::from("tenant-a"), relation.signal(), 123, 4, 9)
                .expect("row");
            let schema = projection_schema(relation.columns());
            let batch = record_batch(&[row], relation.columns(), Arc::clone(&schema))
                .expect("relation batch");
            assert_eq!(batch.num_rows(), 1, "{relation:?}");
            assert_eq!(
                batch.num_columns(),
                relation.columns().len(),
                "{relation:?}"
            );
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                relation
                    .columns()
                    .iter()
                    .map(|column| column.name())
                    .collect::<Vec<_>>(),
                "{relation:?}"
            );
        }
    }

    #[test]
    fn relation_columns_are_unique_and_drawn_from_the_public_catalog() {
        let catalog = ALL_COLUMNS.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(catalog.len(), ALL_COLUMNS.len());
        for relation in [
            AnalyticsRelation::Logs,
            AnalyticsRelation::Spans,
            AnalyticsRelation::SpanEvents,
            AnalyticsRelation::SpanLinks,
            AnalyticsRelation::MetricPoints,
            AnalyticsRelation::MetricExemplars,
        ] {
            let columns = relation.columns().iter().copied().collect::<BTreeSet<_>>();
            assert_eq!(columns.len(), relation.columns().len(), "{relation:?}");
            assert!(columns.is_subset(&catalog), "{relation:?}");
        }
    }

    #[test]
    fn typed_attribute_fingerprints_disambiguate_equal_renderings() {
        let string = TelemetryAttribute::new("code", TelemetryValue::String(Arc::from("1")));
        let integer = TelemetryAttribute::new("code", TelemetryValue::Integer(1));
        assert_eq!(
            attribute_map(std::slice::from_ref(&string)),
            attribute_map(std::slice::from_ref(&integer))
        );
        assert_ne!(attribute_ids(&[string]), attribute_ids(&[integer]));
    }

    #[test]
    fn in_memory_scan_applies_log_constraints() {
        let entries = vec![
            LokiEntry {
                timestamp_unix_nanos: 11,
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                line: "request completed".to_owned(),
                structured_metadata: BTreeMap::from([("code".to_owned(), "200".to_owned())]),
            },
            LokiEntry {
                timestamp_unix_nanos: 12,
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                line: "request ERROR".to_owned(),
                structured_metadata: BTreeMap::from([("code".to_owned(), "500".to_owned())]),
            },
        ];
        let mut request = AnalyticsScanRequest::new("tenant-a");
        request.start_timestamp_unix_nanos = Some(10);
        request.end_timestamp_unix_nanos = Some(20);
        request.terms.push(Arc::from("error"));
        request.message_tokens.push(Arc::from("ERROR"));
        request.labels.push(MetadataField::new("app", "api"));
        request.metadata.push(MetadataField::new("code", "500"));
        let mut observed = Vec::new();
        scan_entries(entries, &request, &mut |rows| {
            observed.extend_from_slice(rows);
            Ok(())
        })
        .expect("scan");
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].message.as_deref(), Some("request ERROR"));
    }
}
