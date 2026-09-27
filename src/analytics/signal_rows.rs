use super::*;

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
    if has_only_columns(columns, AnalyticsColumn::Timestamp, AnalyticsColumn::Name) {
        let mut row = AnalyticsRow::empty(
            Arc::clone(&span.tenant),
            "traces",
            span.start_time_unix_nanos,
            span.record_ref.topic_partition.partition_id.get(),
            span.record_ref.offset.get(),
        )?;
        row.name = Some(Arc::clone(&span.name));
        return Ok(row);
    }
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

pub(crate) fn projected_trace_row(
    tenant: &Arc<str>,
    span: &TraceProjection,
    columns: &[AnalyticsColumn],
) -> Result<AnalyticsRow, LokiApiError> {
    let mut row = AnalyticsRow::empty(
        Arc::clone(tenant),
        "traces",
        span.start_time_unix_nanos,
        span.record_ref.topic_partition.partition_id.get(),
        span.record_ref.offset.get(),
    )?;
    if wants(columns, AnalyticsColumn::EndTimestamp) {
        row.end_timestamp_unix_nanos = span
            .start_time_unix_nanos
            .checked_add(span.duration_nanos)
            .map(timestamp_i64)
            .transpose()?;
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
        row.status_code = span.status_code;
    }
    Ok(row)
}

pub(super) fn span_base_row(
    span: &DurableSpan,
    timestamp: u64,
) -> Result<AnalyticsRow, LokiApiError> {
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

pub(super) fn span_row(span: &DurableSpan) -> Result<AnalyticsRow, LokiApiError> {
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
    if has_only_columns(
        columns,
        AnalyticsColumn::Timestamp,
        AnalyticsColumn::ScalarDoubleBits,
    ) {
        let mut row = AnalyticsRow::empty(
            Arc::clone(&identity.tenant),
            "metrics",
            point.timestamp_unix_nanos,
            point.record_ref.topic_partition.partition_id.get(),
            point.record_ref.offset.get(),
        )?;
        if let MetricValue::Gauge(NumberValue::DoubleBits(bits))
        | MetricValue::Sum(NumberValue::DoubleBits(bits)) = &point.value
        {
            row.scalar_double_bits = Some(*bits);
        }
        return Ok(row);
    }
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
pub(super) fn set_projected_number(
    row: &mut AnalyticsRow,
    columns: &[AnalyticsColumn],
    value: NumberValue,
) {
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

pub(super) fn populate_projected_context(
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

pub(super) fn populate_projected_attributes(
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

pub(super) fn wants(columns: &[AnalyticsColumn], column: AnalyticsColumn) -> bool {
    columns.contains(&column)
}

#[inline]
pub(super) fn has_only_columns(
    columns: &[AnalyticsColumn],
    first: AnalyticsColumn,
    second: AnalyticsColumn,
) -> bool {
    columns.len() == 2 && columns.contains(&first) && columns.contains(&second)
}

pub(super) fn metric_base_row(
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

pub(super) fn metric_row(point: &DurableMetricPoint) -> Result<AnalyticsRow, LokiApiError> {
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

pub(super) fn set_number(row: &mut AnalyticsRow, value: NumberValue) {
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

pub(super) fn populate_attributes(
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

pub(super) fn set_record_attributes(
    row: &mut AnalyticsRow,
    attributes: &[TelemetryAttribute],
) -> Result<(), LokiApiError> {
    row.attributes = attribute_map(attributes);
    row.attribute_ids = attribute_ids(attributes);
    row.attributes_json = json(attributes)?;
    Ok(())
}

pub(super) fn attribute_map(attributes: &[TelemetryAttribute]) -> BTreeMap<String, String> {
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

pub(super) fn attribute_ids(attributes: &[TelemetryAttribute]) -> BTreeMap<String, String> {
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

pub(super) fn render_value(value: &TelemetryValue) -> String {
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

pub(super) fn json<T: serde::Serialize + ?Sized>(
    value: &T,
) -> Result<Option<Arc<str>>, LokiApiError> {
    serde_json::to_string(value)
        .map(|value| Some(Arc::from(value)))
        .map_err(|error| LokiApiError::internal(error.to_string()))
}

pub(super) fn timestamp_i64(value: u64) -> Result<i64, LokiApiError> {
    i64::try_from(value)
        .map_err(|_| LokiApiError::internal("timestamp exceeds ClickHouse i64 range"))
}

pub(super) fn nonzero_timestamp(value: u64) -> Result<Option<i64>, LokiApiError> {
    (value != 0).then(|| timestamp_i64(value)).transpose()
}
