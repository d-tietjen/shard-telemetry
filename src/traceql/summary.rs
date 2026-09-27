use super::*;

pub(super) fn summarize(
    trace_id: TraceId,
    spans: Vec<DurableSpan>,
    selected_fields: Arc<Vec<String>>,
) -> TraceqlTrace {
    let start_time_unix_nanos = spans
        .iter()
        .map(|span| span.start_time_unix_nanos)
        .min()
        .unwrap_or(0);
    let end_time_unix_nanos = spans
        .iter()
        .filter_map(DurableSpan::end_time_unix_nanos)
        .max()
        .unwrap_or(start_time_unix_nanos);
    let root = spans.iter().find(|span| span.parent_span_id.is_none());
    let root_name = root.map(|span| Arc::clone(&span.name));
    let root_service_name = root
        .and_then(|span| attribute(&span.resource.attributes, "service.name"))
        .and_then(value_string)
        .map(Arc::from);
    let error_count = spans
        .iter()
        .filter(|span| span.status.as_ref().is_some_and(|status| status.code == 2))
        .count() as u32;
    TraceqlTrace {
        trace_id,
        spans,
        start_time_unix_nanos,
        end_time_unix_nanos,
        root_name,
        root_service_name,
        error_count,
        selected_fields,
    }
}
