use super::*;

pub(crate) fn trace_query_matches(query: &TraceQuery, span: &DurableSpan) -> bool {
    trace_query_matches_with_resource(query, span, true)
}

pub(super) fn trace_query_matches_with_resource(
    query: &TraceQuery,
    span: &DurableSpan,
    match_resource: bool,
) -> bool {
    span.tenant == query.tenant
        && query
            .partition
            .is_none_or(|partition| partition == span.record_ref.topic_partition)
        && query.trace_id.is_none_or(|value| value == span.trace_id)
        && query.span_id.is_none_or(|value| value == span.span_id)
        && query
            .name
            .as_ref()
            .is_none_or(|value| value.as_ref() == span.name.as_ref())
        && rendered_attributes_match(&span.attributes, &query.exact_attributes)
        && (!match_resource
            || rendered_attributes_match(
                &span.resource.attributes,
                &query.exact_resource_attributes,
            ))
        && rendered_attributes_match(&span.scope.attributes, &query.exact_scope_attributes)
        && query
            .start_time_unix_nanos
            .is_none_or(|start| span.end_time_unix_nanos().unwrap_or(u64::MAX) >= start)
        && query
            .end_time_unix_nanos
            .is_none_or(|end| span.start_time_unix_nanos < end)
        && query
            .min_duration_nanos
            .is_none_or(|duration| span.duration_nanos >= duration)
}

/// Matches the non-resource predicates after the caller has selected the
/// tenant bucket and ruled out partition and trace-ID queries.
#[inline]
pub(super) fn trace_query_matches_without_tenant_resource(
    query: &TraceQuery,
    span: &DurableSpan,
) -> bool {
    query.span_id.is_none_or(|value| value == span.span_id)
        && query
            .name
            .as_ref()
            .is_none_or(|value| value.as_ref() == span.name.as_ref())
        && rendered_attributes_match(&span.attributes, &query.exact_attributes)
        && rendered_attributes_match(&span.scope.attributes, &query.exact_scope_attributes)
        && query
            .start_time_unix_nanos
            .is_none_or(|start| span.end_time_unix_nanos().unwrap_or(u64::MAX) >= start)
        && query
            .end_time_unix_nanos
            .is_none_or(|end| span.start_time_unix_nanos < end)
        && query
            .min_duration_nanos
            .is_none_or(|duration| span.duration_nanos >= duration)
}

#[inline]
pub(super) fn trace_projected_span_filter_is_match_all(query: &TraceQuery) -> bool {
    query.start_offset.is_none()
        && query.span_id.is_none()
        && query.name.is_none()
        && query.exact_attributes.is_empty()
        && query.exact_scope_attributes.is_empty()
        && query.start_time_unix_nanos.is_none()
        && query.end_time_unix_nanos.is_none()
        && query.min_duration_nanos.is_none()
}

pub(super) fn rendered_attributes_match(
    attributes: &[TelemetryAttribute],
    expected: &[(Arc<str>, Arc<str>)],
) -> bool {
    expected.iter().all(|(key, expected_value)| {
        attributes.iter().any(|attribute| {
            attribute.key.as_ref() == key.as_ref()
                && attribute.value.as_ref().is_some_and(|value| {
                    telemetry_value_matches_rendered(value, expected_value.as_ref())
                })
        })
    })
}

pub(super) fn render_resource_attribute_value(
    value: Option<&crate::TelemetryValue>,
) -> Option<Arc<str>> {
    let value = value?;
    match value {
        crate::TelemetryValue::Empty => Some(Arc::from("")),
        crate::TelemetryValue::String(value) => Some(Arc::clone(value)),
        crate::TelemetryValue::Boolean(value) => {
            Some(Arc::from(if *value { "true" } else { "false" }))
        }
        crate::TelemetryValue::Integer(value) => Some(Arc::from(value.to_string())),
        crate::TelemetryValue::DoubleBits(bits) => {
            Some(Arc::from(f64::from_bits(*bits).to_string()))
        }
        crate::TelemetryValue::Bytes(value) => {
            let mut rendered = String::with_capacity(value.len().saturating_mul(2));
            for byte in value.iter() {
                rendered.push_str(&format!("{byte:02x}"));
            }
            Some(Arc::from(rendered))
        }
        crate::TelemetryValue::StringTableIndex(value) => Some(Arc::from(value.to_string())),
        crate::TelemetryValue::Array(_) | crate::TelemetryValue::Map(_) => {
            serde_json::to_string(value).ok().map(Arc::from)
        }
    }
}

pub(super) fn telemetry_value_matches_rendered(
    value: &crate::TelemetryValue,
    expected: &str,
) -> bool {
    match value {
        crate::TelemetryValue::Empty => expected.is_empty(),
        crate::TelemetryValue::String(value) => value.as_ref() == expected,
        crate::TelemetryValue::Boolean(value) => {
            (*value && expected == "true") || (!*value && expected == "false")
        }
        crate::TelemetryValue::Integer(value) => expected.parse::<i64>() == Ok(*value),
        crate::TelemetryValue::DoubleBits(bits) => f64::from_bits(*bits).to_string() == expected,
        crate::TelemetryValue::Bytes(value) => {
            value.len().saturating_mul(2) == expected.len()
                && value
                    .iter()
                    .zip(expected.as_bytes().chunks_exact(2))
                    .all(|(byte, pair)| {
                        u8::from_str_radix(std::str::from_utf8(pair).unwrap_or(""), 16) == Ok(*byte)
                    })
        }
        crate::TelemetryValue::StringTableIndex(value) => expected.parse::<i32>() == Ok(*value),
        crate::TelemetryValue::Array(_) | crate::TelemetryValue::Map(_) => {
            serde_json::to_string(value).is_ok_and(|rendered| rendered == expected)
        }
    }
}

pub(super) fn trace_query_cursor_matches(query: &TraceQuery, span: &DurableSpan) -> bool {
    query
        .start_offset
        .is_none_or(|offset| span.record_ref.offset >= offset)
}

pub(super) fn same_span_payload(left: &DurableSpan, right: &DurableSpan) -> bool {
    let mut normalized = right.clone();
    normalized.record_ref.offset = left.record_ref.offset;
    left == &normalized
}

pub(super) fn retain_newest_span(
    winners: &mut HashMap<(TraceId, SpanId), DurableSpan>,
    span: DurableSpan,
) {
    let key = (span.trace_id, span.span_id);
    if winners
        .get(&key)
        .is_none_or(|existing| existing.record_ref.offset < span.record_ref.offset)
    {
        winners.insert(key, span);
    }
}

pub(super) fn retain_exact_trace_span(
    winners: &mut BTreeMap<SpanId, DurableSpan>,
    span: DurableSpan,
) {
    if winners
        .get(&span.span_id)
        .is_none_or(|existing| existing.record_ref.offset < span.record_ref.offset)
    {
        winners.insert(span.span_id, span);
    }
}

pub(super) fn summarize_trace(
    spans: &[DurableSpan],
    block_id: u64,
) -> TelemetryResult<TraceSummary> {
    let first = spans.first().ok_or(TelemetryError::InvalidBlockEncoding(
        "cannot summarize an empty trace",
    ))?;
    let mut start = u64::MAX;
    let mut end = 0;
    let mut max_duration = 0;
    let mut errors = 0u32;
    let mut root_name = None;
    for span in spans {
        start = start.min(span.start_time_unix_nanos);
        end = end.max(span.end_time_unix_nanos().unwrap_or(u64::MAX));
        max_duration = max_duration.max(span.duration_nanos);
        if span.status.as_ref().is_some_and(|status| status.code == 2) {
            errors = errors.saturating_add(1);
        }
        if span.parent_span_id.is_none() && root_name.is_none() {
            root_name = Some(Arc::clone(&span.name));
        }
    }
    Ok(TraceSummary {
        trace_id: first.trace_id,
        tenant: Arc::clone(&first.tenant),
        start_time_unix_nanos: start,
        end_time_unix_nanos: end,
        max_duration_nanos: max_duration,
        span_count: u32::try_from(spans.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        error_count: errors,
        root_name,
        block_fragments: Arc::new(vec![block_id]),
    })
}
