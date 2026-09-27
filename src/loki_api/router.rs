use super::*;

/// Builds the stable Loki 3.7-compatible HTTP route surface.
pub fn loki_router(store: Arc<dyn LokiStore>, api_config: LokiApiConfig) -> Router {
    build_loki_router(store, api_config, None, None, Duration::from_secs(30), true)
}

/// Builds the Loki surface plus the authenticated ClickHouse Arrow scan route.
///
/// The analytical route is deliberately absent from [`loki_router`]. Servers
/// must opt in with a non-empty bearer token loaded from a protected source.
pub fn loki_router_with_clickhouse(
    store: Arc<dyn LokiStore>,
    api_config: LokiApiConfig,
    bearer_token: Arc<str>,
) -> Result<Router, LokiApiError> {
    if bearer_token.is_empty() {
        return Err(LokiApiError::configuration(
            "ClickHouse bearer token must not be empty",
        ));
    }
    Ok(build_loki_router(
        store,
        api_config,
        Some(bearer_token),
        None,
        Duration::from_secs(30),
        true,
    ))
}

/// Builds the fail-closed single-tenant Loki surface used by the standalone server.
pub fn single_tenant_loki_router(
    store: Arc<dyn LokiStore>,
    api_config: LokiApiConfig,
    runtime: Arc<ProductionRuntime>,
    analytics_bearer_token: Option<Arc<str>>,
    flush_timeout: Duration,
) -> Result<Router, LokiApiError> {
    if flush_timeout.is_zero() {
        return Err(LokiApiError::configuration(
            "production flush timeout must be nonzero",
        ));
    }
    if let Some(token) = analytics_bearer_token.as_deref()
        && token.is_empty()
    {
        return Err(LokiApiError::configuration(
            "ClickHouse bearer token must not be empty",
        ));
    }
    Ok(build_loki_router(
        store,
        api_config,
        analytics_bearer_token,
        Some(runtime),
        flush_timeout,
        true,
    ))
}

/// Builds the authenticated Loki/OTLP compatibility routes for embedding in a
/// host which already owns health, readiness, metrics, and shutdown routes.
pub fn single_tenant_loki_api_router(
    store: Arc<dyn LokiStore>,
    api_config: LokiApiConfig,
    runtime: Arc<ProductionRuntime>,
    analytics_bearer_token: Option<Arc<str>>,
    flush_timeout: Duration,
) -> Result<Router, LokiApiError> {
    if flush_timeout.is_zero() {
        return Err(LokiApiError::configuration(
            "production flush timeout must be nonzero",
        ));
    }
    Ok(build_loki_router(
        store,
        api_config,
        analytics_bearer_token,
        Some(runtime),
        flush_timeout,
        false,
    ))
}

pub(super) fn build_loki_router(
    store: Arc<dyn LokiStore>,
    api_config: LokiApiConfig,
    analytics_bearer_token: Option<Arc<str>>,
    production: Option<Arc<ProductionRuntime>>,
    flush_timeout: Duration,
    include_operational_routes: bool,
) -> Router {
    let (live, _) = broadcast::channel(1_024);
    let analytics_enabled = analytics_bearer_token.is_some();
    let state = ApiState {
        store,
        config: api_config,
        live,
        analytics_bearer_token,
        production,
        flush_timeout,
    };
    let router = Router::new()
        .route("/loki/api/v1/status/buildinfo", get(build_info))
        .route("/loki/api/v1/push", post(push_logs))
        .route("/otlp/v1/logs", post(push_otlp))
        .route("/loki/api/v1/query", get(query_instant).post(query_instant))
        .route(
            "/loki/api/v1/query_range",
            get(query_range).post(query_range),
        )
        .route("/loki/api/v1/labels", get(labels).post(labels))
        .route(
            "/loki/api/v1/label/{name}/values",
            get(label_values).post(label_values),
        )
        .route("/loki/api/v1/series", get(series).post(series))
        .route(
            "/loki/api/v1/index/stats",
            get(index_stats).post(index_stats),
        )
        .route(
            "/loki/api/v1/index/volume",
            get(index_volume).post(index_volume),
        )
        .route(
            "/loki/api/v1/index/volume_range",
            get(index_volume_range).post(index_volume_range),
        )
        .route("/loki/api/v1/patterns", get(patterns).post(patterns))
        .route(
            "/loki/api/v1/detected_fields",
            get(detected_fields).post(detected_fields),
        )
        .route(
            "/loki/api/v1/detected_field/{name}/values",
            get(detected_field_values).post(detected_field_values),
        )
        .route("/loki/api/v1/tail", get(tail))
        .route(
            "/loki/api/v1/delete",
            get(list_deletes)
                .post(create_delete)
                .put(create_delete)
                .delete(cancel_delete),
        )
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query),
        )
        .route("/api/prom/push", post(push_logs))
        .route("/api/prom/query", get(query_range))
        .route("/api/prom/label", get(labels))
        .route("/api/prom/label/{name}/values", get(label_values))
        .route("/api/prom/series", get(series))
        .route("/api/prom/tail", get(tail));
    let router = if include_operational_routes {
        router
            .route("/ready", get(ready))
            .route("/metrics", get(metrics))
            .route("/config", get(current_config))
            .route("/services", get(services))
            .route("/log_level", get(log_level).post(log_level))
            .route("/flush", post(flush))
            .route("/ingester/prepare_shutdown", post(prepare_shutdown))
            .route("/ingester/shutdown", post(shutdown))
    } else {
        router
    };
    let router = if analytics_enabled {
        router.route(
            "/shardtelemetry/api/v1/clickhouse/scan",
            get(clickhouse_scan),
        )
    } else {
        router
    };
    router
        .layer(middleware::from_fn_with_state(
            state.clone(),
            production_gate,
        ))
        .layer(DefaultBodyLimit::max(state.config.max_request_bytes))
        .with_state(state)
}

pub(super) async fn production_gate(
    State(state): State<ApiState>,
    request: Request,
    next: Next,
) -> Response {
    let Some(runtime) = state.production.as_ref() else {
        return next.run(request).await;
    };
    let path = request.uri().path();
    if matches!(path, "/ready" | "/metrics") {
        return next.run(request).await;
    }
    let analytics = path == "/shardtelemetry/api/v1/clickhouse/scan";
    if !analytics {
        let observed = bearer_token(request.headers());
        let authenticated = observed.is_some_and(|observed| runtime.authenticates(observed));
        if !authenticated {
            if observed.is_none() {
                runtime.record_authentication_failure();
            }
            return LokiApiError::unauthorized("valid production bearer token is required")
                .into_response();
        }
    }
    if request
        .headers()
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|tenant| tenant != runtime.tenant())
    {
        return LokiApiError::forbidden("tenant header does not match the configured tenant")
            .into_response();
    }
    let Some(_http_permit) = runtime.try_http() else {
        return LokiApiError::too_many_requests("HTTP concurrency limit exceeded").into_response();
    };
    let query_path = analytics
        || path.starts_with("/loki/api/v1/query")
        || path.starts_with("/loki/api/v1/label")
        || path.starts_with("/loki/api/v1/series")
        || path.starts_with("/loki/api/v1/index")
        || path.starts_with("/loki/api/v1/patterns")
        || path.starts_with("/loki/api/v1/detected")
        || path.starts_with("/api/prom/query")
        || path.starts_with("/api/prom/label")
        || path.starts_with("/api/prom/series");
    let _query_permit = if query_path {
        let Some(permit) = runtime.try_query() else {
            if matches!(
                runtime.lifecycle().state(),
                ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
            ) {
                return LokiApiError::unavailable("query service is unavailable").into_response();
            }
            return LokiApiError::too_many_requests("query concurrency limit exceeded")
                .into_response();
        };
        runtime.record_query();
        Some(permit)
    } else {
        None
    };
    if query_path {
        match tokio::time::timeout(runtime.query_timeout(), next.run(request)).await {
            Ok(response) => response,
            Err(_) => LokiApiError::timeout("query deadline exceeded").into_response(),
        }
    } else {
        next.run(request).await
    }
}

pub(super) fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

pub(super) async fn clickhouse_scan(
    State(state): State<ApiState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<Response, LokiApiError> {
    let expected = state
        .analytics_bearer_token
        .as_deref()
        .ok_or_else(|| LokiApiError::not_found("analytical scan route is disabled"))?;
    if !authorized_bearer(&headers, expected) {
        return Err(LokiApiError::unauthorized(
            "valid ClickHouse bearer token is required",
        ));
    }
    let request = crate::analytics::parse_scan_request(
        tenant(&headers, &state.config),
        raw_query.as_deref(),
    )?;
    Ok(crate::analytics::analytics_stream_response(
        state.store,
        request,
    ))
}

pub(super) fn authorized_bearer(headers: &HeaderMap, expected: &str) -> bool {
    let Some(observed) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return false;
    };
    let observed = blake3::hash(observed.as_bytes());
    let expected = blake3::hash(expected.as_bytes());
    observed
        .as_bytes()
        .iter()
        .zip(expected.as_bytes())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}
