use super::*;

pub(super) async fn tail(
    websocket: WebSocketUpgrade,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Response, LokiApiError> {
    let tail_permit = state
        .production
        .as_ref()
        .map(|runtime| {
            runtime
                .try_tail()
                .ok_or_else(|| LokiApiError::too_many_requests("tail subscriber limit exceeded"))
        })
        .transpose()?;
    let tenant_name = tenant(&headers, &state.config);
    let selector = parse_log_query(
        params
            .query
            .as_deref()
            .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?,
    )?;
    let mut live = state.live.subscribe();
    let store = Arc::clone(&state.store);
    let mut service_shutdown = state
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().subscribe_shutdown());
    let result = execute_stream_query(state, headers, params).await?.0;
    Ok(websocket
        .on_upgrade(move |mut socket| async move {
            let _tail_permit = tail_permit;
            let payload = result
                .get("data")
                .and_then(|data| data.get("result"))
                .cloned()
                .unwrap_or_else(|| json!([]));
            let _ = socket
                .send(axum::extract::ws::Message::Text(
                    json!({"streams": payload, "dropped_entries": []})
                        .to_string()
                        .into(),
                ))
                .await;
            loop {
                let shutdown = async {
                    match service_shutdown.as_mut() {
                        Some(shutdown) => {
                            let _ = shutdown.changed().await;
                        }
                        None => std::future::pending::<()>().await,
                    }
                };
                let received = tokio::select! {
                    biased;
                    () = shutdown => break,
                    received = live.recv() => received,
                };
                match received {
                    Ok(push) if push.tenant == tenant_name => {
                        let Ok(delete_filter) = store
                            .delete_requests(&tenant_name)
                            .and_then(|requests| LogicalDeleteFilter::compile(&requests))
                        else {
                            break;
                        };
                        let entries = push
                            .entries
                            .into_iter()
                            .filter_map(|entry| {
                                if delete_filter.matches(&entry) {
                                    None
                                } else {
                                    selector.process(entry)
                                }
                            })
                            .collect::<Vec<_>>();
                        if entries.is_empty() {
                            continue;
                        }
                        let payload = tail_streams(entries);
                        if socket
                            .send(axum::extract::ws::Message::Text(
                                json!({"streams": payload, "dropped_entries": []})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(dropped)) => {
                        if socket
                            .send(axum::extract::ws::Message::Text(
                                json!({
                                    "streams": [],
                                    "dropped_entries": [{
                                        "labels": {},
                                        "timestamp": now_nanos().to_string(),
                                        "dropped": dropped
                                    }]
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
        .into_response())
}

pub(super) fn tail_streams(entries: Vec<LokiEntry>) -> Vec<Value> {
    let mut streams = BTreeMap::<BTreeMap<String, String>, Vec<Value>>::new();
    for entry in entries {
        let mut labels = entry.labels;
        labels.extend(entry.structured_metadata);
        let level = detected_level(&labels, &entry.line);
        labels.entry("detected_level".to_owned()).or_insert(level);
        streams
            .entry(labels)
            .or_default()
            .push(json!([entry.timestamp_unix_nanos.to_string(), entry.line]));
    }
    streams
        .into_iter()
        .map(|(stream, values)| json!({"stream": stream, "values": values}))
        .collect()
}

#[derive(Debug, Deserialize)]
pub(super) struct DeleteParams {
    query: Option<String>,
    start: Option<String>,
    end: Option<String>,
    request_id: Option<String>,
}

pub(super) async fn create_delete(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<DeleteParams>,
) -> Result<StatusCode, LokiApiError> {
    let query = params
        .query
        .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?;
    parse_log_query(&query)?;
    let start = parse_delete_timestamp(
        params
            .start
            .as_deref()
            .ok_or_else(|| LokiApiError::bad_request("start parameter is required"))?,
    )?;
    let end = params
        .end
        .as_deref()
        .map(parse_delete_timestamp)
        .transpose()?
        .unwrap_or_else(now_nanos);
    let tenant = tenant(&headers, &state.config);
    let store = Arc::clone(&state.store);
    tokio::task::spawn_blocking(move || {
        store.create_delete(&tenant, start, end, query, now_nanos())
    })
    .await
    .map_err(|error| LokiApiError::internal(format!("delete worker failed: {error}")))??;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn list_deletes(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<DeleteParams>,
) -> Result<Json<Value>, LokiApiError> {
    let tenant = tenant(&headers, &state.config);
    let range = match (params.start.as_deref(), params.end.as_deref()) {
        (None, None) => None,
        (Some(start), Some(end)) => {
            Some((parse_delete_timestamp(start)?, parse_delete_timestamp(end)?))
        }
        _ => {
            return Err(LokiApiError::bad_request(
                "delete list start and end must be provided together",
            ));
        }
    };
    let deletes = state
        .store
        .delete_requests(&tenant)?
        .into_iter()
        .filter(|request| {
            range.is_none_or(|(start, end)| request.start_time <= end && request.end_time >= start)
        })
        .collect::<Vec<_>>();
    Ok(Json(json!(deletes)))
}

pub(super) async fn cancel_delete(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<DeleteParams>,
) -> Result<StatusCode, LokiApiError> {
    let request_id = params
        .request_id
        .ok_or_else(|| LokiApiError::bad_request("request_id parameter is required"))?;
    let tenant = tenant(&headers, &state.config);
    let store = Arc::clone(&state.store);
    tokio::task::spawn_blocking(move || store.cancel_delete(&tenant, &request_id))
        .await
        .map_err(|error| LokiApiError::internal(format!("delete worker failed: {error}")))??;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn format_query(
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let query = params
        .query
        .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?;
    parse_log_query(&query)?;
    Ok(Json(json!({"status": "success", "data": query})))
}

pub(super) fn tenant(headers: &HeaderMap, config: &LokiApiConfig) -> String {
    headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or(&config.default_tenant)
        .to_owned()
}

pub(super) async fn entries_for(
    state: &ApiState,
    tenant: &str,
) -> Result<Vec<LokiEntry>, LokiApiError> {
    let store = Arc::clone(&state.store);
    let tenant = tenant.to_owned();
    tokio::task::spawn_blocking(move || store.entries(&tenant))
        .await
        .map_err(|error| LokiApiError::internal(format!("query worker failed: {error}")))?
}

pub(super) fn format_label_set(labels: &BTreeMap<String, String>) -> String {
    let contents = labels
        .iter()
        .map(|(name, value)| {
            format!(
                "{name}={}",
                serde_json::to_string(value).expect("string serialization cannot fail")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{contents}}}")
}

pub(super) fn success(result_type: &str, result: Vec<Value>, stats: Value) -> Value {
    json!({
        "status": "success",
        "data": {
            "resultType": result_type,
            "result": result,
            "stats": stats,
        }
    })
}

pub(super) fn query_stats(bytes: usize, lines: usize, returned: usize, elapsed: f64) -> Value {
    let denominator = elapsed.max(f64::EPSILON);
    json!({
        "ingester": {
            "compressedBytes": 0,
            "decompressedBytes": bytes,
            "decompressedLines": lines,
            "headChunkBytes": bytes,
        },
        "summary": {
            "bytesProcessedPerSecond": (bytes as f64 / denominator) as u64,
            "linesProcessedPerSecond": (lines as f64 / denominator) as u64,
            "totalBytesProcessed": bytes,
            "totalLinesProcessed": lines,
            "execTime": elapsed,
            "queueTime": 0,
            "subqueries": 0,
            "totalEntriesReturned": returned,
            "splits": 0,
            "shards": 0
        }
    })
}
