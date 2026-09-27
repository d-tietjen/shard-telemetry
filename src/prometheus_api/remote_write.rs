use super::*;

pub(super) async fn remote_write(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let _http_permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    if body.len() > service.config.max_request_bytes {
        return write_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Remote Write body is too large",
        );
    }
    if !headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("snappy"))
    {
        return write_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Remote Write requires Content-Encoding: snappy",
        );
    }
    let content_type = match headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) => value,
        None => {
            return write_error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Remote Write requires a protobuf Content-Type",
            );
        }
    };
    let version = match RemoteWriteDecoder::version_from_content_type(content_type) {
        Ok(version) => version,
        Err(error) => return write_error(StatusCode::UNSUPPORTED_MEDIA_TYPE, &error.to_string()),
    };
    if let Err(response) = validate_version_header(&headers, version) {
        return response;
    }
    let decompressed_len = match snap::raw::decompress_len(&body) {
        Ok(length) if length <= service.config.max_request_bytes => length,
        Ok(_) => {
            return write_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Remote Write Snappy payload expands beyond 64 MiB",
            );
        }
        Err(error) => return write_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let tenant = Arc::clone(&service.config.tenant);
    let router = service.router;
    let decoded = match tokio::task::spawn_blocking(move || {
        let mut protobuf = vec![0_u8; decompressed_len];
        snap::raw::Decoder::new()
            .decompress(&body, &mut protobuf)
            .map_err(|error| error.to_string())?;
        let decoded = RemoteWriteDecoder
            .decode(&tenant, version, &protobuf)
            .map_err(|error| error.to_string())?;
        let stats = decoded.stats;
        let batch = decoded
            .into_native_batch(&router)
            .map_err(|error| error.to_string())?;
        Ok::<_, String>((batch, stats))
    })
    .await
    {
        Ok(Ok(decoded)) => decoded,
        Ok(Err(error)) => return write_error(StatusCode::BAD_REQUEST, &error),
        Err(error) => {
            return write_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Remote Write decode worker failed: {error}"),
            );
        }
    };
    let (batch, stats) = decoded;
    if batch.partitions.is_empty() {
        return write_success(stats);
    }
    let _ingest_permit = match &service.production {
        Some(runtime) => match runtime.try_ingest(decompressed_len) {
            Some(permit) => Some(permit),
            None if runtime.lifecycle().state() == ServiceState::Ready => {
                return write_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "Remote Write concurrency or rate limit exceeded",
                );
            }
            None => {
                return write_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Remote Write ingestion is unavailable",
                );
            }
        },
        None => None,
    };
    let store = Arc::clone(&service.store);
    let result = tokio::task::spawn_blocking(move || store.append_remote_write_batch(&batch)).await;
    match result {
        Ok(Ok(_)) => {
            if let Some(runtime) = &service.production {
                let records = stats.samples.saturating_add(stats.histograms);
                runtime.record_ingest(decompressed_len, records as usize);
            }
            write_success(stats)
        }
        Ok(Err(error)) => write_error(error.status(), &error.to_string()),
        Err(error) => write_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Remote Write worker failed: {error}"),
        ),
    }
}
