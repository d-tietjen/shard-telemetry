use super::*;

pub(super) async fn query_range(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    execute_query(state, headers, params, QueryMode::Range).await
}

pub(super) async fn query_instant(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    execute_query(state, headers, params, QueryMode::Instant).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QueryMode {
    Instant,
    Range,
}

pub(super) async fn execute_query(
    state: ApiState,
    headers: HeaderMap,
    params: QueryParams,
    mode: QueryMode,
) -> Result<Json<Value>, LokiApiError> {
    let expression = params
        .query
        .as_deref()
        .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?;
    if expression.trim_start().starts_with('{') {
        execute_stream_query(state, headers, params).await
    } else {
        execute_metric_query(state, headers, params, mode).await
    }
}

pub(super) fn merge_form_query_params(
    headers: &HeaderMap,
    mut url: QueryParams,
    body: &[u8],
) -> Result<QueryParams, LokiApiError> {
    if body.is_empty() {
        return Ok(url);
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type
        .split(';')
        .next()
        .is_none_or(|value| value.trim() != "application/x-www-form-urlencoded")
    {
        return Err(LokiApiError::bad_request(
            "POST query bodies must use application/x-www-form-urlencoded",
        ));
    }
    let form: QueryParams = serde_urlencoded::from_bytes(body)
        .map_err(|error| LokiApiError::bad_request(format!("invalid form body: {error}")))?;
    macro_rules! overlay {
        ($($field:ident),+ $(,)?) => {
            $(if form.$field.is_some() { url.$field = form.$field; })+
        };
    }
    overlay!(
        query,
        start,
        end,
        time,
        since,
        limit,
        direction,
        step,
        line_limit,
        field_limit,
    );
    Ok(url)
}

pub(super) async fn execute_stream_query(
    state: ApiState,
    headers: HeaderMap,
    params: QueryParams,
) -> Result<Json<Value>, LokiApiError> {
    let started = std::time::Instant::now();
    let expression = params
        .query
        .as_deref()
        .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?;
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let limit = params
        .limit
        .unwrap_or(DEFAULT_QUERY_LIMIT)
        .min(state.config.max_query_limit);
    let backward = params.direction.as_deref().unwrap_or("backward") != "forward";
    let store = Arc::clone(&state.store);
    let tenant_for_query = tenant.clone();
    let expression = expression.to_owned();
    let result = tokio::task::spawn_blocking(move || {
        store.query_range(&tenant_for_query, &expression, start, end, limit, backward)
    })
    .await
    .map_err(|error| LokiApiError::internal(format!("query worker failed: {error}")))??;
    let total_lines_processed = result.lines_processed;
    let total_bytes_processed = result.bytes_processed;
    let entries = result.entries;
    let total_entries_returned = entries.len();

    let mut streams: BTreeMap<BTreeMap<String, String>, Vec<Value>> = BTreeMap::new();
    for entry in entries {
        let mut response_labels = entry.labels;
        response_labels.extend(entry.structured_metadata);
        let level = detected_level(&response_labels, &entry.line);
        response_labels
            .entry("detected_level".to_owned())
            .or_insert(level);
        let value = vec![
            Value::String(entry.timestamp_unix_nanos.to_string()),
            Value::String(entry.line),
        ];
        streams
            .entry(response_labels)
            .or_default()
            .push(Value::Array(value));
    }
    let result = streams
        .into_iter()
        .map(|(stream, values)| json!({ "stream": stream, "values": values }))
        .collect::<Vec<_>>();
    let elapsed = started.elapsed().as_secs_f64();
    Ok(Json(success(
        "streams",
        result,
        query_stats(
            total_bytes_processed,
            total_lines_processed,
            total_entries_returned,
            elapsed,
        ),
    )))
}
