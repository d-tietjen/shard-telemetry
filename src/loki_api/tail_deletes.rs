use super::*;
use axum::extract::ws::Message;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use std::pin::Pin;
use std::task::Poll;

pub(super) async fn tail(
    websocket: WebSocketUpgrade,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    query_gate: Option<axum::Extension<Arc<dyn crate::NativeRequestGate>>>,
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
    let live = state.live.subscribe();
    let store = Arc::clone(&state.store);
    let service_shutdown = state
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().subscribe_shutdown());
    let result = execute_stream_query(state, headers, params).await?.0;
    let query_gate = query_gate.map(|axum::Extension(gate)| gate);
    if let Some(gate) = &query_gate {
        gate.check_query()
            .map_err(LokiApiError::backend_unavailable)?;
    }
    let session = TailSession {
        initial: result,
        tenant: tenant_name,
        selector,
        store,
        live,
        shutdown: service_shutdown,
        _permit: tail_permit,
        gate: query_gate,
    };
    Ok(websocket
        .on_upgrade(move |socket| session.run(socket))
        .into_response())
}

// The actual production websocket consumes this same session. No new queue,
// policy, authority or provider is created; an optional existing gate stays
// owned alongside the original subscription permit for the entire lifetime.
struct TailSession {
    initial: Value,
    tenant: String,
    selector: LogSelector,
    store: Arc<dyn LokiStore>,
    live: broadcast::Receiver<LivePush>,
    shutdown: Option<tokio::sync::watch::Receiver<bool>>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
    gate: Option<Arc<dyn crate::NativeRequestGate>>,
}
impl TailSession {
    async fn run<S, E>(mut self, mut socket: S)
    where
        S: Sink<Message> + Stream<Item = Result<Message, E>> + Unpin,
    {
        let gated = self.gate.is_some();
        let payload = self
            .initial
            .get("data")
            .and_then(|data| data.get("result"))
            .cloned()
            .unwrap_or_else(|| json!([]));
        let initial = Message::Text(
            json!({"streams": payload, "dropped_entries": []})
                .to_string()
                .into(),
        );
        let sent =
            send_tail_message(&mut socket, initial, self.gate.as_ref(), &mut self.shutdown).await;
        // Preserve the original no-extension initial-send behavior.
        if gated && !sent {
            return;
        }
        loop {
            let incoming = async {
                if gated {
                    socket.next().await
                } else {
                    std::future::pending().await
                }
            };
            let received = tokio::select! {
                biased;
                () = tail_shutdown(&mut self.shutdown, gated) => break,
                incoming = incoming => {
                    match incoming {
                        None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                        Some(Ok(_)) => continue,
                    }
                }
                received = self.live.recv() => received,
            };
            let message = match received {
                Ok(push) if push.tenant == self.tenant => {
                    let Ok(delete_filter) = self
                        .store
                        .delete_requests(&self.tenant)
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
                                self.selector.process(entry)
                            }
                        })
                        .collect::<Vec<_>>();
                    if entries.is_empty() {
                        continue;
                    }
                    Message::Text(
                        json!({"streams": tail_streams(entries), "dropped_entries": []})
                            .to_string()
                            .into(),
                    )
                }
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(dropped)) => Message::Text(
                    json!({
                        "streams": [], "dropped_entries": [{"labels": {},
                        "timestamp": now_nanos().to_string(), "dropped": dropped}]
                    })
                    .to_string()
                    .into(),
                ),
                Err(broadcast::error::RecvError::Closed) => break,
            };
            if !send_tail_message(&mut socket, message, self.gate.as_ref(), &mut self.shutdown)
                .await
            {
                break;
            }
        }
    }
}

async fn tail_shutdown(shutdown: &mut Option<tokio::sync::watch::Receiver<bool>>, gated: bool) {
    match shutdown.as_mut() {
        Some(shutdown) => {
            if gated && *shutdown.borrow() {
                return;
            }
            let _ = shutdown.changed().await;
        }
        None => std::future::pending::<()>().await,
    }
}

async fn send_tail_message<S, E>(
    socket: &mut S,
    message: Message,
    gate: Option<&Arc<dyn crate::NativeRequestGate>>,
    shutdown: &mut Option<tokio::sync::watch::Receiver<bool>>,
) -> bool
where
    S: Sink<Message> + Stream<Item = Result<Message, E>> + Unpin,
{
    #[cfg(test)]
    tests::encoded(&message);
    let Some(gate) = gate else {
        return socket.send(message).await.is_ok();
    };
    // Keep the real encoded message while readiness is pending. The SAME gate
    // is checked after readiness, immediately before the actual start_send.
    // That check can linearize before later transport writes; it is not a
    // permission held atomically through network publication or flush.
    let mut message = Some(message);
    let send = futures_util::future::poll_fn(|cx| {
        // One peer item per poll, including while readiness OR flush is pending.
        // This socket has one poll owner; no split or concurrent read half is
        // created. A buffered terminal event after a nonterminal item must not
        // depend on a future sink wake, so explicitly schedule its next poll.
        match Pin::new(&mut *socket).poll_next(cx) {
            Poll::Ready(None | Some(Err(_)) | Some(Ok(Message::Close(_)))) => {
                return Poll::Ready(Err(()));
            }
            Poll::Ready(Some(Ok(_))) => cx.waker().wake_by_ref(),
            Poll::Pending => {}
        }
        if message.is_some() {
            match Pin::new(&mut *socket).poll_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Ready(Err(())),
                Poll::Ready(Ok(())) => {}
            }
            if gate.check_query().is_err() {
                return Poll::Ready(Err(()));
            }
            let Some(message) = message.take() else {
                return Poll::Ready(Err(()));
            };
            if Pin::new(&mut *socket).start_send(message).is_err() {
                return Poll::Ready(Err(()));
            }
        }
        Pin::new(&mut *socket)
            .poll_flush(cx)
            .map(|actual| actual.map_err(|_| ()))
    });
    tokio::select! {
        biased;
        () = tail_shutdown(shutdown, true) => false,
        actual = send => actual.is_ok(),
    }
}

#[cfg(test)]
mod tests;

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
