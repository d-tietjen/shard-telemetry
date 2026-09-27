use super::*;

pub(crate) fn analytics_stream_response(
    store: Arc<dyn LokiStore>,
    request: AnalyticsScanRequest,
) -> Response {
    match request.wire_format {
        AnalyticsWireFormat::ArrowStream => arrow_stream_response(store, request),
        AnalyticsWireFormat::RowBinary => rowbinary_stream_response(store, request),
        AnalyticsWireFormat::JsonLines => jsonlines_stream_response(store, request),
    }
}

pub(super) fn arrow_stream_response(
    store: Arc<dyn LokiStore>,
    request: AnalyticsScanRequest,
) -> Response {
    let relation = request.relation.name();
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::task::spawn_blocking(move || {
        if let Err(error) = write_arrow_stream(store, &request, sender.clone()) {
            let _ = sender.blocking_send(Err(io::Error::other(error.to_string())));
        }
    });
    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apache.arrow.stream"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-schema-version"),
        HeaderValue::from_static("1"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-relation"),
        HeaderValue::from_static(relation),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-clickhouse-target"),
        HeaderValue::from_static(CLICKHOUSE_COMPATIBILITY_TARGET),
    );
    response
}

pub(super) fn rowbinary_stream_response(
    store: Arc<dyn LokiStore>,
    request: AnalyticsScanRequest,
) -> Response {
    let relation = request.relation.name();
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::task::spawn_blocking(move || {
        let mut sink = ChannelWriter::new(sender.clone(), STREAM_CHUNK_BYTES);
        let result = write_rowbinary_stream(store, &request, &mut sink).and_then(|()| {
            sink.finish()
                .map_err(|error| LokiApiError::internal(error.to_string()))
        });
        if let Err(error) = result {
            let _ = sender.blocking_send(Err(io::Error::other(error.to_string())));
        }
    });
    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-schema-version"),
        HeaderValue::from_static("1"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-relation"),
        HeaderValue::from_static(relation),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-clickhouse-target"),
        HeaderValue::from_static(CLICKHOUSE_COMPATIBILITY_TARGET),
    );
    response
}

pub(super) fn jsonlines_stream_response(
    store: Arc<dyn LokiStore>,
    request: AnalyticsScanRequest,
) -> Response {
    let relation = request.relation.name();
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::task::spawn_blocking(move || {
        let mut sink = ChannelWriter::new(sender.clone(), STREAM_CHUNK_BYTES);
        let result = write_jsonlines_stream(store, &request, &mut sink).and_then(|()| {
            sink.finish()
                .map_err(|error| LokiApiError::internal(error.to_string()))
        });
        if let Err(error) = result {
            let _ = sender.blocking_send(Err(io::Error::other(error.to_string())));
        }
    });
    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-schema-version"),
        HeaderValue::from_static("1"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-relation"),
        HeaderValue::from_static(relation),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-shardtelemetry-clickhouse-target"),
        HeaderValue::from_static(CLICKHOUSE_COMPATIBILITY_TARGET),
    );
    response
}

pub(super) fn write_rowbinary_stream(
    store: Arc<dyn LokiStore>,
    request: &AnalyticsScanRequest,
    writer: &mut dyn Write,
) -> Result<(), LokiApiError> {
    if request.cardinality_only {
        let row = rowbinary_default_value(request.columns[0]);
        let mut batch = Vec::with_capacity(row.len() * DEFAULT_SCAN_BATCH_ROWS);
        for _ in 0..DEFAULT_SCAN_BATCH_ROWS {
            batch.extend_from_slice(&row);
        }
        store.scan_analytics_cardinality(request, &mut |count| {
            let mut remaining = count;
            while remaining > 0 {
                let rows = remaining.min(DEFAULT_SCAN_BATCH_ROWS as u64) as usize;
                writer
                    .write_all(&batch[..rows * row.len()])
                    .map_err(rowbinary_error)?;
                remaining -= rows as u64;
            }
            Ok(())
        })?;
        return Ok(());
    }
    if store.scan_analytics_rowbinary(request, writer)? {
        return Ok(());
    }
    store.scan_analytics(request, &mut |rows| {
        for row in rows {
            write_rowbinary_row(writer, row, &request.columns)?;
        }
        Ok(())
    })
}

pub(super) fn write_jsonlines_stream(
    store: Arc<dyn LokiStore>,
    request: &AnalyticsScanRequest,
    writer: &mut dyn Write,
) -> Result<(), LokiApiError> {
    debug_assert!(!request.cardinality_only);
    if !request.group_by.is_empty() {
        return store.scan_analytics_grouped(request, &mut |groups| {
            for group in groups {
                serde_json::to_writer(&mut *writer, group)
                    .map_err(|error| LokiApiError::internal(error.to_string()))?;
                writer.write_all(b"\n").map_err(rowbinary_error)?;
            }
            Ok(())
        });
    }
    const JSON_WRITE_BATCH_BYTES: usize = 64 * 1024;
    let mut encoded_row = Vec::with_capacity(1024);
    let mut encoded_batch = Vec::with_capacity(JSON_WRITE_BATCH_BYTES);
    store.scan_analytics(request, &mut |rows| {
        for row in rows {
            encoded_row.clear();
            write_jsonlines_row(&mut encoded_row, row, &request.columns)?;
            if !encoded_batch.is_empty()
                && encoded_batch.len().saturating_add(encoded_row.len()) > JSON_WRITE_BATCH_BYTES
            {
                writer.write_all(&encoded_batch).map_err(rowbinary_error)?;
                encoded_batch.clear();
            }
            encoded_batch.extend_from_slice(&encoded_row);
        }
        Ok(())
    })?;
    if !encoded_batch.is_empty() {
        writer.write_all(&encoded_batch).map_err(rowbinary_error)?;
    }
    Ok(())
}
