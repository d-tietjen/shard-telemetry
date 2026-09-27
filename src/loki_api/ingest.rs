use super::*;

#[derive(Debug, Deserialize)]
pub(super) struct JsonPushRequest {
    pub(super) streams: Vec<JsonPushStream>,
}

#[derive(Debug, Deserialize)]
pub(super) struct JsonPushStream {
    stream: BTreeMap<String, String>,
    values: Vec<Vec<Value>>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ProtoPushRequest {
    #[prost(message, repeated, tag = "1")]
    pub(super) streams: Vec<ProtoStream>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ProtoStream {
    #[prost(string, tag = "1")]
    pub(super) labels: String,
    #[prost(message, repeated, tag = "2")]
    pub(super) entries: Vec<ProtoEntry>,
    #[prost(uint64, tag = "3")]
    pub(super) hash: u64,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ProtoEntry {
    #[prost(message, optional, tag = "1")]
    pub(super) timestamp: Option<prost_types::Timestamp>,
    #[prost(string, tag = "2")]
    pub(super) line: String,
    #[prost(message, repeated, tag = "3")]
    pub(super) structured_metadata: Vec<ProtoLabelPair>,
    #[prost(message, repeated, tag = "4")]
    pub(super) parsed: Vec<ProtoLabelPair>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ProtoLabelPair {
    #[prost(string, tag = "1")]
    pub(super) name: String,
    #[prost(string, tag = "2")]
    pub(super) value: String,
}

pub(super) async fn push_logs(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, LokiApiError> {
    let source_bytes = body.len();
    let _ingest_permit = production_ingest_permit(&state, source_bytes)?;
    let tenant = tenant(&headers, &state.config);
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let json = content_type.starts_with("application/json");
    let store = Arc::clone(&state.store);
    let durable_tenant = tenant.clone();
    let entries = tokio::task::spawn_blocking(move || {
        let entries = if json {
            decode_json_push(&body)?
        } else {
            decode_protobuf_push(&body)?
        };
        store.push(&durable_tenant, entries.clone())?;
        Ok::<_, LokiApiError>(entries)
    })
    .await
    .map_err(|error| LokiApiError::internal(format!("ingest worker failed: {error}")))??;
    if let Some(runtime) = &state.production {
        runtime.record_ingest(source_bytes, entries.len());
    }
    let _ = state.live.send(LivePush { tenant, entries });
    Ok(StatusCode::NO_CONTENT)
}

pub(super) fn decode_json_push(body: &[u8]) -> Result<Vec<LokiEntry>, LokiApiError> {
    let request: JsonPushRequest = serde_json::from_slice(body)
        .map_err(|error| LokiApiError::bad_request(format!("invalid push JSON: {error}")))?;
    let mut entries = Vec::new();
    for stream in request.streams {
        validate_labels(&stream.stream)?;
        let labels = normalize_stream_labels(stream.stream);
        for value in stream.values {
            if !(2..=3).contains(&value.len()) {
                return Err(LokiApiError::bad_request(
                    "push value must contain timestamp, line, and optional metadata",
                ));
            }
            let timestamp = value[0]
                .as_str()
                .ok_or_else(|| LokiApiError::bad_request("push timestamp must be a string"))?
                .parse::<i64>()
                .map_err(|_| LokiApiError::bad_request("push timestamp is not an integer"))?;
            let line = value[1]
                .as_str()
                .ok_or_else(|| LokiApiError::bad_request("push line must be a string"))?
                .to_owned();
            let metadata = value
                .get(2)
                .map(parse_metadata)
                .transpose()?
                .unwrap_or_default();
            entries.push(LokiEntry {
                timestamp_unix_nanos: timestamp,
                labels: labels.clone(),
                line,
                structured_metadata: metadata,
            });
        }
    }
    Ok(entries)
}

pub(super) fn decode_protobuf_push(body: &[u8]) -> Result<Vec<LokiEntry>, LokiApiError> {
    let decoded = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|error| LokiApiError::bad_request(format!("invalid Snappy payload: {error}")))?;
    let request = ProtoPushRequest::decode(decoded.as_slice())
        .map_err(|error| LokiApiError::bad_request(format!("invalid push protobuf: {error}")))?;
    let mut entries = Vec::new();
    for stream in request.streams {
        let labels = normalize_stream_labels(parse_label_set(&stream.labels)?);
        for entry in stream.entries {
            let timestamp = entry
                .timestamp
                .ok_or_else(|| LokiApiError::bad_request("push entry has no timestamp"))?;
            let nanos = timestamp
                .seconds
                .checked_mul(1_000_000_000)
                .and_then(|seconds| seconds.checked_add(i64::from(timestamp.nanos)))
                .ok_or_else(|| LokiApiError::bad_request("push timestamp is out of range"))?;
            entries.push(LokiEntry {
                timestamp_unix_nanos: nanos,
                labels: labels.clone(),
                line: entry.line,
                structured_metadata: entry
                    .structured_metadata
                    .into_iter()
                    .map(|pair| (pair.name, pair.value))
                    .collect(),
            });
        }
    }
    Ok(entries)
}

pub(super) fn parse_metadata(value: &Value) -> Result<BTreeMap<String, String>, LokiApiError> {
    let object = value
        .as_object()
        .ok_or_else(|| LokiApiError::bad_request("structured metadata must be an object"))?;
    object
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|value| (key.clone(), value.to_owned()))
                .ok_or_else(|| {
                    LokiApiError::bad_request("structured metadata values must be strings")
                })
        })
        .collect()
}

pub(super) async fn push_otlp(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, LokiApiError> {
    let source_bytes = body.len();
    let _ingest_permit = production_ingest_permit(&state, source_bytes)?;
    let tenant = tenant(&headers, &state.config);
    let store = Arc::clone(&state.store);
    let durable_tenant = tenant.clone();
    let entries = tokio::task::spawn_blocking(move || {
        let events = crate::OtlpLogDecoder
            .decode(&body)
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let entries = events
            .into_iter()
            .map(|event| {
                let mut labels = BTreeMap::new();
                let mut structured_metadata = BTreeMap::new();
                for field in event.fields.iter() {
                    let key = field.key.to_string();
                    let value = field.value.to_string();
                    if key == "service.name" || key == "resource.service.name" {
                        labels.insert("service_name".to_owned(), value);
                    } else {
                        structured_metadata.insert(normalize_otlp_name(&key), value);
                    }
                }
                LokiEntry {
                    timestamp_unix_nanos: event.timestamp_unix_nanos.min(i64::MAX as u64) as i64,
                    labels: normalize_stream_labels(labels),
                    line: event.message.to_string(),
                    structured_metadata,
                }
            })
            .collect::<Vec<_>>();
        store.push(&durable_tenant, entries.clone())?;
        Ok::<_, LokiApiError>(entries)
    })
    .await
    .map_err(|error| LokiApiError::internal(format!("OTLP ingest worker failed: {error}")))??;
    if let Some(runtime) = &state.production {
        runtime.record_ingest(source_bytes, entries.len());
    }
    let _ = state.live.send(LivePush { tenant, entries });
    Ok(StatusCode::OK)
}

pub(super) fn production_ingest_permit(
    state: &ApiState,
    source_bytes: usize,
) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, LokiApiError> {
    let Some(runtime) = &state.production else {
        return Ok(None);
    };
    runtime.try_ingest(source_bytes).map(Some).ok_or_else(|| {
        if runtime.lifecycle().state() != ServiceState::Ready {
            LokiApiError::unavailable("ingestion is draining or unavailable")
        } else {
            LokiApiError::too_many_requests("ingest concurrency or rate limit exceeded")
        }
    })
}

pub(super) fn normalize_otlp_name(name: &str) -> String {
    name.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect()
}
