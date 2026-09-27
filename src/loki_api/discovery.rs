use super::*;

pub(super) async fn labels(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let mut names = BTreeSet::new();
    for entry in entries_for(&state, &tenant).await? {
        if entry.timestamp_unix_nanos >= start && entry.timestamp_unix_nanos <= end {
            names.extend(entry.labels.into_keys());
        }
    }
    Ok(Json(json!({"status": "success", "data": names})))
}

pub(super) async fn label_values(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    validate_label_name(&name)?;
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let values = entries_for(&state, &tenant)
        .await?
        .into_iter()
        .filter(|entry| entry.timestamp_unix_nanos >= start && entry.timestamp_unix_nanos <= end)
        .filter_map(|entry| entry.labels.get(&name).cloned())
        .collect::<BTreeSet<_>>();
    Ok(Json(json!({"status": "success", "data": values})))
}

pub(super) async fn series(
    State(state): State<ApiState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let mut pairs = form_urlencoded::parse(raw_query.as_deref().unwrap_or_default().as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    if !body.is_empty() {
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
                "POST series bodies must use application/x-www-form-urlencoded",
            ));
        }
        pairs.extend(
            form_urlencoded::parse(&body)
                .map(|(key, value)| (key.into_owned(), value.into_owned())),
        );
    }
    let params = QueryParams {
        start: pairs
            .iter()
            .rev()
            .find_map(|(key, value)| (key == "start").then(|| value.clone())),
        end: pairs
            .iter()
            .rev()
            .find_map(|(key, value)| (key == "end").then(|| value.clone())),
        ..QueryParams::default()
    };
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let mut selector_values = pairs
        .iter()
        .filter(|(key, _)| key == "match[]")
        .map(|(_, value)| value.clone())
        .collect::<Vec<_>>();
    if selector_values.is_empty() {
        selector_values.push("{}".to_owned());
    }
    let selectors = selector_values
        .into_iter()
        .map(|selector| parse_log_query(&selector))
        .collect::<Result<Vec<_>, _>>()?;
    let streams = entries_for(&state, &tenant)
        .await?
        .into_iter()
        .filter(|entry| {
            entry.timestamp_unix_nanos >= start
                && entry.timestamp_unix_nanos <= end
                && selectors.iter().any(|selector| selector.matches(entry))
        })
        .map(|entry| entry.labels)
        .collect::<BTreeSet<_>>();
    Ok(Json(json!({"status": "success", "data": streams})))
}

pub(super) async fn index_stats(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let tenant = tenant(&headers, &state.config);
    let selector = parse_log_query(params.query.as_deref().unwrap_or("{}"))?;
    let (start, end) = query_range_bounds(&params)?;
    let entries = entries_for(&state, &tenant)
        .await?
        .into_iter()
        .filter(|entry| {
            entry.timestamp_unix_nanos >= start
                && entry.timestamp_unix_nanos <= end
                && selector.matches(entry)
        })
        .collect::<Vec<_>>();
    let streams = entries
        .iter()
        .map(|entry| &entry.labels)
        .collect::<BTreeSet<_>>()
        .len();
    let bytes = entries.iter().map(|entry| entry.line.len()).sum::<usize>();
    Ok(Json(json!({
        "streams": streams,
        "chunks": streams,
        "entries": entries.len(),
        "bytes": bytes
    })))
}

pub(super) async fn index_volume(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    volume_response(state, headers, params).await
}

pub(super) async fn index_volume_range(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    volume_response(state, headers, params).await
}

pub(super) async fn volume_response(
    state: ApiState,
    headers: HeaderMap,
    params: QueryParams,
) -> Result<Json<Value>, LokiApiError> {
    let tenant = tenant(&headers, &state.config);
    let selector = parse_log_query(params.query.as_deref().unwrap_or("{}"))?;
    let (start, end) = query_range_bounds(&params)?;
    let mut volumes: BTreeMap<String, usize> = BTreeMap::new();
    for entry in entries_for(&state, &tenant).await? {
        if entry.timestamp_unix_nanos >= start
            && entry.timestamp_unix_nanos <= end
            && selector.matches(&entry)
        {
            *volumes.entry(format_label_set(&entry.labels)).or_default() += entry.line.len();
        }
    }
    let volumes = volumes
        .into_iter()
        .map(|(name, volume)| json!({"name": name, "volume": volume}))
        .collect::<Vec<_>>();
    Ok(Json(
        json!({"status": "success", "data": {"resultType": "vector", "result": volumes}}),
    ))
}

pub(super) async fn patterns(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let selector = parse_log_query(
        params
            .query
            .as_deref()
            .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?,
    )?;
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let step = pattern_step_nanos(params.step.as_deref())?;
    let mut patterns = BTreeMap::<String, BTreeMap<i64, u64>>::new();
    let mut matched = 0usize;
    for entry in entries_for(&state, &tenant).await? {
        if entry.timestamp_unix_nanos < start
            || entry.timestamp_unix_nanos > end
            || !selector.matches(&entry)
        {
            continue;
        }
        let bucket = start
            .saturating_add(
                entry
                    .timestamp_unix_nanos
                    .saturating_sub(start)
                    .div_euclid(step)
                    .saturating_mul(step),
            )
            .div_euclid(1_000_000_000);
        *patterns
            .entry(crate::message_pattern(&entry.line))
            .or_default()
            .entry(bucket)
            .or_default() += 1;
        matched += 1;
        if matched == state.config.max_query_limit {
            break;
        }
    }
    let data = patterns
        .into_iter()
        .map(|(pattern, samples)| {
            json!({
                "pattern": pattern,
                "samples": samples.into_iter().map(|(timestamp, count)| {
                    json!([timestamp, count])
                }).collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"status": "success", "data": data})))
}

pub(super) fn pattern_step_nanos(value: Option<&str>) -> Result<i64, LokiApiError> {
    let step = match value {
        None => 10_000_000_000,
        Some(value) => parse_duration_nanos(value)
            .or_else(|| {
                value
                    .parse::<f64>()
                    .ok()
                    .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
                    .map(|seconds| (seconds * 1_000_000_000.0) as i64)
            })
            .ok_or_else(|| LokiApiError::bad_request("step must be a positive duration"))?,
    };
    if step <= 0 {
        return Err(LokiApiError::bad_request(
            "step must be a positive duration",
        ));
    }
    Ok(step)
}

pub(super) async fn detected_fields(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let tenant = tenant(&headers, &state.config);
    let selector = parse_log_query(params.query.as_deref().unwrap_or("{}"))?;
    let (start, end) = query_range_bounds(&params)?;
    let line_limit = params
        .line_limit
        .unwrap_or(100)
        .min(state.config.max_query_limit);
    let field_limit = params
        .field_limit
        .or(params.limit)
        .unwrap_or(1_000)
        .min(state.config.max_query_limit);
    let mut fields = BTreeMap::<String, DetectedField>::new();
    let mut scanned = 0usize;
    for entry in entries_for(&state, &tenant).await? {
        if entry.timestamp_unix_nanos < start
            || entry.timestamp_unix_nanos > end
            || !selector.matches(&entry)
        {
            continue;
        }
        for (name, value) in &entry.structured_metadata {
            fields
                .entry(name.clone())
                .or_default()
                .values
                .insert(value.clone());
        }
        for (name, value, parser) in detect_line_fields(&entry.line) {
            let field = fields.entry(name).or_default();
            field.values.insert(value);
            field.parsers.insert(parser);
        }
        scanned += 1;
        if scanned == line_limit {
            break;
        }
    }
    let fields = fields
        .into_iter()
        .take(field_limit)
        .map(|(label, field)| {
            json!({
                "label": label,
                "type": inferred_field_type(&field.values),
                "cardinality": field.values.len(),
                "parsers": field.parsers,
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"fields": fields, "limit": field_limit})))
}

pub(super) async fn detected_field_values(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let tenant = tenant(&headers, &state.config);
    let selector = parse_log_query(params.query.as_deref().unwrap_or("{}"))?;
    let (start, end) = query_range_bounds(&params)?;
    let line_limit = params
        .line_limit
        .unwrap_or(100)
        .min(state.config.max_query_limit);
    let value_limit = params
        .field_limit
        .or(params.limit)
        .unwrap_or(1_000)
        .min(state.config.max_query_limit);
    let mut values = BTreeSet::new();
    let mut scanned = 0usize;
    for entry in entries_for(&state, &tenant).await? {
        if entry.timestamp_unix_nanos < start
            || entry.timestamp_unix_nanos > end
            || !selector.matches(&entry)
        {
            continue;
        }
        if let Some(value) = entry.structured_metadata.get(&name) {
            values.insert(value.clone());
        }
        for (field, value, _) in detect_line_fields(&entry.line) {
            if field == name {
                values.insert(value);
            }
        }
        scanned += 1;
        if scanned == line_limit || values.len() == value_limit {
            break;
        }
    }
    Ok(Json(json!({"values": values, "limit": value_limit})))
}

#[derive(Debug, Default)]
pub(super) struct DetectedField {
    values: BTreeSet<String>,
    parsers: BTreeSet<&'static str>,
}

pub(super) fn detect_line_fields(line: &str) -> Vec<(String, String, &'static str)> {
    if let Ok(Value::Object(object)) = serde_json::from_str::<Value>(line) {
        return object
            .into_iter()
            .filter_map(|(name, value)| match value {
                Value::String(value) => Some((name, value, "json")),
                Value::Number(value) => Some((name, value.to_string(), "json")),
                Value::Bool(value) => Some((name, value.to_string(), "json")),
                _ => None,
            })
            .collect();
    }
    line.split_ascii_whitespace()
        .filter_map(|term| {
            let (name, value) = term.split_once('=')?;
            validate_label_name(name).ok()?;
            let value = value.trim_matches(|character| matches!(character, '"' | '\'' | ','));
            (!value.is_empty()).then(|| (name.to_owned(), value.to_owned(), "logfmt"))
        })
        .collect()
}

pub(super) fn inferred_field_type(values: &BTreeSet<String>) -> &'static str {
    if values.iter().all(|value| value.parse::<bool>().is_ok()) {
        "boolean"
    } else if values.iter().all(|value| value.parse::<i128>().is_ok()) {
        "int"
    } else if values.iter().all(|value| value.parse::<f64>().is_ok()) {
        "float"
    } else if values
        .iter()
        .all(|value| parse_duration_nanos(value).is_some())
    {
        "duration"
    } else if values
        .iter()
        .all(|value| parse_byte_quantity(value).is_some())
    {
        "bytes"
    } else {
        "string"
    }
}

pub(super) fn parse_byte_quantity(value: &str) -> Option<u64> {
    for (suffix, scale) in [
        ("KiB", 1_u64 << 10),
        ("MiB", 1_u64 << 20),
        ("GiB", 1_u64 << 30),
        ("KB", 1_000),
        ("MB", 1_000_000),
        ("GB", 1_000_000_000),
        ("B", 1),
    ] {
        if let Some(number) = value.strip_suffix(suffix) {
            return number
                .parse::<u64>()
                .ok()
                .and_then(|number| number.checked_mul(scale));
        }
    }
    None
}
