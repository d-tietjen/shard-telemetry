use super::*;

pub(super) async fn query_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<InstantQueryParameters>,
) -> Response {
    execute_instant_query(service, headers, parameters).await
}

pub(super) async fn query_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Form(parameters): Form<InstantQueryParameters>,
) -> Response {
    execute_instant_query(service, headers, parameters).await
}

pub(super) async fn execute_instant_query(
    service: PrometheusService,
    headers: HeaderMap,
    parameters: InstantQueryParameters,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let time_ms = match parameters
        .time
        .as_deref()
        .map(parse_prometheus_time)
        .transpose()
    {
        Ok(Some(value)) => value,
        Ok(None) => current_time_ms(),
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let engine = PromqlEngine::new(
        Arc::clone(&service.store),
        Arc::clone(&service.config.tenant),
        PromqlLimits::default(),
    );
    let expression = parameters.query;
    match tokio::task::spawn_blocking(move || engine.query(&expression, time_ms)).await {
        Ok(Ok(value)) => query_success(value),
        Ok(Err(error)) => query_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "execution",
            &error.to_string(),
        ),
        Err(error) => query_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &format!("PromQL worker failed: {error}"),
        ),
    }
}

pub(super) async fn query_range_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<RangeQueryParameters>,
) -> Response {
    execute_range_query(service, headers, parameters).await
}

pub(super) async fn query_range_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Form(parameters): Form<RangeQueryParameters>,
) -> Response {
    execute_range_query(service, headers, parameters).await
}

pub(super) async fn execute_range_query(
    service: PrometheusService,
    headers: HeaderMap,
    parameters: RangeQueryParameters,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let start_ms = match parse_prometheus_time(&parameters.start) {
        Ok(value) => value,
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let end_ms = match parse_prometheus_time(&parameters.end) {
        Ok(value) => value,
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let step_ms = match parse_prometheus_duration(&parameters.step) {
        Ok(value) => value,
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let engine = PromqlEngine::new(
        Arc::clone(&service.store),
        Arc::clone(&service.config.tenant),
        PromqlLimits::default(),
    );
    let expression = parameters.query;
    match tokio::task::spawn_blocking(move || {
        engine.query_range(&expression, start_ms, end_ms, step_ms)
    })
    .await
    {
        Ok(Ok(value)) => query_success(value),
        Ok(Err(error)) => query_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "execution",
            &error.to_string(),
        ),
        Err(error) => query_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &format!("PromQL worker failed: {error}"),
        ),
    }
}

pub(super) async fn series_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<DiscoveryParameters>,
) -> Response {
    execute_series(service, headers, parameters).await
}

pub(super) async fn series_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Form(parameters): Form<DiscoveryParameters>,
) -> Response {
    execute_series(service, headers, parameters).await
}

pub(super) async fn execute_series(
    service: PrometheusService,
    headers: HeaderMap,
    parameters: DiscoveryParameters,
) -> Response {
    let points = match discovery_points(&service, &headers, &parameters).await {
        Ok(points) => points,
        Err(response) => return response,
    };
    let series = points
        .iter()
        .map(metric_labels)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    api_success(json!(series))
}

pub(super) async fn labels_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<DiscoveryParameters>,
) -> Response {
    execute_labels(service, headers, parameters).await
}

pub(super) async fn labels_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Form(parameters): Form<DiscoveryParameters>,
) -> Response {
    execute_labels(service, headers, parameters).await
}

pub(super) async fn execute_labels(
    service: PrometheusService,
    headers: HeaderMap,
    parameters: DiscoveryParameters,
) -> Response {
    let points = match discovery_points(&service, &headers, &parameters).await {
        Ok(points) => points,
        Err(response) => return response,
    };
    let mut labels = BTreeSet::new();
    for point in &points {
        labels.extend(metric_labels(point).into_keys());
    }
    api_success(json!(labels))
}

pub(super) async fn label_values_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(parameters): Query<DiscoveryParameters>,
) -> Response {
    execute_label_values(service, headers, name, parameters).await
}

pub(super) async fn label_values_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Form(parameters): Form<DiscoveryParameters>,
) -> Response {
    execute_label_values(service, headers, name, parameters).await
}

pub(super) async fn execute_label_values(
    service: PrometheusService,
    headers: HeaderMap,
    name: String,
    parameters: DiscoveryParameters,
) -> Response {
    if name.is_empty() {
        return query_error(StatusCode::BAD_REQUEST, "bad_data", "label name is empty");
    }
    let points = match discovery_points(&service, &headers, &parameters).await {
        Ok(points) => points,
        Err(response) => return response,
    };
    let values = points
        .iter()
        .filter_map(|point| metric_labels(point).remove(&name))
        .collect::<BTreeSet<_>>();
    api_success(json!(values))
}

pub(super) async fn metadata_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<MetadataParameters>,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let engine = PromqlEngine::new(
        Arc::clone(&service.store),
        Arc::clone(&service.config.tenant),
        PromqlLimits::default(),
    );
    let selectors = parameters
        .metric
        .as_ref()
        .map(|metric| vec![metric.clone()])
        .unwrap_or_default();
    let points = match tokio::task::spawn_blocking(move || {
        engine.raw_points(&selectors, 0, current_time_ms())
    })
    .await
    {
        Ok(Ok(points)) => points,
        Ok(Err(error)) => {
            return query_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "execution",
                &error.to_string(),
            );
        }
        Err(error) => {
            return query_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &format!("metadata worker failed: {error}"),
            );
        }
    };
    let limit = parameters.limit.unwrap_or(usize::MAX);
    let mut metadata = BTreeMap::<String, BTreeSet<(String, String, String)>>::new();
    for point in points {
        if metadata.len() >= limit && !metadata.contains_key(point.identity.name.as_ref()) {
            continue;
        }
        metadata
            .entry(point.identity.name.to_string())
            .or_default()
            .insert((
                prometheus_metric_type(&point.identity.kind).into(),
                point.description.to_string(),
                point.identity.unit.to_string(),
            ));
    }
    let data = metadata
        .into_iter()
        .map(|(name, entries)| {
            let entries = entries
                .into_iter()
                .map(|(metric_type, help, unit)| {
                    json!({"type": metric_type, "help": help, "unit": unit})
                })
                .collect::<Vec<_>>();
            (name, Value::Array(entries))
        })
        .collect::<serde_json::Map<_, _>>();
    api_success(Value::Object(data))
}

pub(super) async fn exemplars_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<ExemplarParameters>,
) -> Response {
    execute_exemplars(service, headers, parameters).await
}

pub(super) async fn exemplars_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Form(parameters): Form<ExemplarParameters>,
) -> Response {
    execute_exemplars(service, headers, parameters).await
}

pub(super) async fn execute_exemplars(
    service: PrometheusService,
    headers: HeaderMap,
    parameters: ExemplarParameters,
) -> Response {
    let start_ms = match parse_prometheus_time(&parameters.start) {
        Ok(value) => value,
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let end_ms = match parse_prometheus_time(&parameters.end) {
        Ok(value) => value,
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let discovery = DiscoveryParameters {
        selectors: vec![parameters.query],
        start: Some((start_ms as f64 / 1_000.0).to_string()),
        end: Some((end_ms as f64 / 1_000.0).to_string()),
    };
    let points = match discovery_points(&service, &headers, &discovery).await {
        Ok(points) => points,
        Err(response) => return response,
    };
    let mut series = BTreeMap::<BTreeMap<String, String>, Vec<Value>>::new();
    for point in points {
        let labels = metric_labels(&point);
        for exemplar in point.exemplars.iter() {
            let exemplar_labels = exemplar
                .filtered_attributes
                .iter()
                .filter_map(|attribute| {
                    attribute
                        .value
                        .as_ref()
                        .map(|value| (attribute.key.to_string(), render_telemetry_value(value)))
                })
                .collect::<BTreeMap<_, _>>();
            series.entry(labels.clone()).or_default().push(json!({
                "labels": exemplar_labels,
                "value": format_number(exemplar.value),
                "timestamp": timestamp_seconds(
                    i64::try_from(exemplar.timestamp_unix_nanos / 1_000_000).unwrap_or(i64::MAX)
                ),
                "traceID": exemplar.trace_id.map(|id| id.to_string()).unwrap_or_default(),
                "spanID": exemplar.span_id.map(|id| id.to_string()).unwrap_or_default()
            }));
        }
    }
    let data = series
        .into_iter()
        .map(|(series_labels, exemplars)| {
            json!({"seriesLabels": series_labels, "exemplars": exemplars})
        })
        .collect::<Vec<_>>();
    api_success(json!(data))
}

#[allow(clippy::result_large_err)]
pub(super) async fn discovery_points(
    service: &PrometheusService,
    headers: &HeaderMap,
    parameters: &DiscoveryParameters,
) -> Result<Vec<crate::DurableMetricPoint>, Response> {
    let _permit = service.authorize(headers)?;
    let start_ms = parameters
        .start
        .as_deref()
        .map(parse_prometheus_time)
        .transpose()
        .map_err(|error| query_error(StatusCode::BAD_REQUEST, "bad_data", &error))?
        .unwrap_or(0);
    let end_ms = parameters
        .end
        .as_deref()
        .map(parse_prometheus_time)
        .transpose()
        .map_err(|error| query_error(StatusCode::BAD_REQUEST, "bad_data", &error))?
        .unwrap_or_else(current_time_ms);
    let engine = PromqlEngine::new(
        Arc::clone(&service.store),
        Arc::clone(&service.config.tenant),
        PromqlLimits::default(),
    );
    let selectors = parameters.selectors.clone();
    match tokio::task::spawn_blocking(move || engine.raw_points(&selectors, start_ms, end_ms)).await
    {
        Ok(Ok(points)) => Ok(points),
        Ok(Err(error)) => Err(query_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "execution",
            &error.to_string(),
        )),
        Err(error) => Err(query_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &format!("discovery worker failed: {error}"),
        )),
    }
}
