use super::*;

pub(super) async fn ready(State(state): State<ApiState>) -> Response {
    let lifecycle = state
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().state())
        .unwrap_or(ServiceState::Ready);
    let health = match state.store.health() {
        Ok(health) => health,
        Err(error) => {
            return (StatusCode::SERVICE_UNAVAILABLE, format!("{error}\n")).into_response();
        }
    };
    if lifecycle == ServiceState::Ready && health.ready {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("{}: {}\n", lifecycle.as_str(), health.detail),
        )
            .into_response()
    }
}

pub(super) async fn metrics(State(state): State<ApiState>) -> Response {
    let store = state.store.operational_metrics();
    let lifecycle = state
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().state())
        .unwrap_or(ServiceState::Ready);
    let ready = state
        .store
        .health()
        .is_ok_and(|health| health.ready && lifecycle == ServiceState::Ready);
    let mut output = format!(
        "# HELP shard_telemetry_ready Whether ShardTelemetry is ready.\n\
         # TYPE shard_telemetry_ready gauge\n\
         shard_telemetry_ready {}\n\
         # HELP shard_telemetry_durable_sink_pending_items Durable sink items waiting for indexing.\n\
         # TYPE shard_telemetry_durable_sink_pending_items gauge\n\
         shard_telemetry_durable_sink_pending_items {}\n\
         shard_telemetry_durable_sink_pending_bytes {}\n\
         shard_telemetry_durable_sink_checkpoint_age_milliseconds {}\n\
         shard_telemetry_durable_sink_applied_appends_total {}\n\
         shard_telemetry_durable_sink_retries_total {}\n\
         shard_telemetry_durable_sink_failures_total {}\n\
         shard_telemetry_durable_sink_dirty_partitions {}\n\
         shard_telemetry_retention_runs_total {}\n\
             shard_telemetry_retention_advanced_offsets_total {}\n\
             shard_telemetry_retention_failures_total {}\n\
             shard_telemetry_source_reclaimed_offsets_total {}\n",
        u8::from(ready),
        store.pending_items,
        store.pending_bytes,
        store.checkpoint_age_ms,
        store.applied_appends,
        store.retry_attempts,
        store.failed_attempts,
        store.dirty_partitions,
        store.retention_runs,
        store.retention_advanced_offsets,
        store.retention_failures,
        store.source_reclaimed_offsets,
    );
    output.push_str(&format!(
        "shard_telemetry_retired_object_groups_total {}\n\
         shard_telemetry_retired_object_payload_bytes_total {}\n\
         shard_telemetry_retired_object_keys_total {}\n",
        store.retired_object_groups, store.retired_object_payload_bytes, store.retired_object_keys,
    ));
    if let Some(retained_payload_bytes) = store.retained_payload_bytes {
        output.push_str(&format!(
            "shard_telemetry_retained_payload_bytes {retained_payload_bytes}\n"
        ));
    }
    if let Some(object) = store.object_store {
        output.push_str(&format!(
            "shard_telemetry_object_store_put_requests_total {}\n\
             shard_telemetry_object_store_put_bytes_total {}\n\
             shard_telemetry_object_store_get_requests_total {}\n\
             shard_telemetry_object_store_get_bytes_total {}\n\
             shard_telemetry_object_store_range_requests_total {}\n\
             shard_telemetry_object_store_range_bytes_total {}\n\
             shard_telemetry_object_store_head_requests_total {}\n\
             shard_telemetry_object_store_compare_and_swaps_total {}\n\
             shard_telemetry_object_store_exact_deletes_total {}\n\
             shard_telemetry_object_store_failures_total {}\n",
            object.put_requests,
            object.put_bytes,
            object.get_requests,
            object.get_bytes,
            object.range_requests,
            object.range_bytes,
            object.head_requests,
            object.compare_and_swaps,
            object.delete_requests,
            object.failures,
        ));
    }
    if let Some(runtime) = &state.production {
        let protocol = runtime.metrics();
        let (http, ingest, query, tail, native) = runtime.admission_in_flight();
        output.push_str(&format!(
            "shard_telemetry_http_requests_total {}\n\
             shard_telemetry_authentication_failures_total {}\n\
             shard_telemetry_rejected_requests_total {}\n\
             shard_telemetry_ingest_requests_total {}\n\
             shard_telemetry_ingest_bytes_total {}\n\
             shard_telemetry_ingest_records_total {}\n\
             shard_telemetry_query_requests_total {}\n\
             shard_telemetry_native_connections_total {}\n\
             shard_telemetry_tail_subscriptions_total {}\n\
             shard_telemetry_http_in_flight {}\n\
             shard_telemetry_ingest_in_flight {}\n\
             shard_telemetry_query_in_flight {}\n\
             shard_telemetry_tail_in_flight {}\n\
             shard_telemetry_native_connections_in_flight {}\n",
            protocol.http_requests,
            protocol.authentication_failures,
            protocol.rejected_requests,
            protocol.ingest_requests,
            protocol.ingest_bytes,
            protocol.ingest_records,
            protocol.query_requests,
            protocol.native_connections,
            protocol.tail_subscriptions,
            http,
            ingest,
            query,
            tail,
            native,
        ));
    }
    (StatusCode::OK, output).into_response()
}

pub(super) async fn current_config(State(state): State<ApiState>) -> Json<Value> {
    Json(json!({
        "target": "all",
        "auth_enabled": state.production.is_some(),
        "single_tenant": state.production.is_some()
    }))
}

pub(super) async fn services(State(state): State<ApiState>) -> Json<Value> {
    let status = state
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().state().as_str())
        .unwrap_or("ready");
    Json(json!({"services": [{"service": "shard-telemetry", "status": status}]}))
}

pub(super) async fn log_level() -> Json<Value> {
    Json(json!({"status": "success", "message": "current log level is info"}))
}

pub(super) async fn flush(State(state): State<ApiState>) -> Result<StatusCode, LokiApiError> {
    flush_store(&state).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn prepare_shutdown(
    State(state): State<ApiState>,
) -> Result<StatusCode, LokiApiError> {
    if let Some(runtime) = &state.production {
        runtime.lifecycle().begin_draining();
    }
    if let Err(error) = flush_store(&state).await {
        if let Some(runtime) = &state.production {
            runtime.lifecycle().mark_failed(error.to_string());
        }
        return Err(error);
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn shutdown(State(state): State<ApiState>) -> Result<StatusCode, LokiApiError> {
    prepare_shutdown(State(state.clone())).await?;
    if let Some(runtime) = &state.production {
        runtime.lifecycle().request_shutdown();
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn flush_store(state: &ApiState) -> Result<(), LokiApiError> {
    let store = Arc::clone(&state.store);
    let timeout = state.flush_timeout;
    tokio::task::spawn_blocking(move || store.flush(timeout))
        .await
        .map_err(|error| LokiApiError::internal(format!("flush worker failed: {error}")))?
}

pub(super) async fn build_info() -> Json<Value> {
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": option_env!("SHARD_TELEMETRY_GIT_REVISION").unwrap_or("unknown"),
        "branch": "unknown",
        "buildUser": "cargo",
        "buildDate": option_env!("SHARD_TELEMETRY_BUILD_DATE").unwrap_or("unknown"),
        "goVersion": "",
    }))
}
