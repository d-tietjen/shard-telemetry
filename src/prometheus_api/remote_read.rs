use super::*;

pub(super) async fn remote_read(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    if body.len() > service.config.max_request_bytes {
        return write_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Remote Read body is too large",
        );
    }
    if !headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("snappy"))
    {
        return write_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Remote Read requires Content-Encoding: snappy",
        );
    }
    let decompressed_len = match snap::raw::decompress_len(&body) {
        Ok(length) if length <= service.config.max_request_bytes => length,
        Ok(_) => {
            return write_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Remote Read Snappy payload expands beyond 64 MiB",
            );
        }
        Err(error) => return write_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let request = match tokio::task::spawn_blocking(move || {
        let mut protobuf = vec![0_u8; decompressed_len];
        snap::raw::Decoder::new()
            .decompress(&body, &mut protobuf)
            .map_err(|error| error.to_string())?;
        prometheus_v1::ReadRequest::decode(protobuf.as_slice()).map_err(|error| error.to_string())
    })
    .await
    {
        Ok(Ok(request)) => request,
        Ok(Err(error)) => return write_error(StatusCode::BAD_REQUEST, &error),
        Err(error) => {
            return write_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Remote Read decode worker failed: {error}"),
            );
        }
    };
    let response_type = if request.accepted_response_types.is_empty() {
        Some(prometheus_v1::ReadRequestResponseType::Samples)
    } else {
        request
            .accepted_response_types
            .iter()
            .find_map(|value| prometheus_v1::ReadRequestResponseType::try_from(*value).ok())
    };
    let Some(response_type) = response_type else {
        return write_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Remote Read requested no supported response type",
        );
    };
    if response_type == prometheus_v1::ReadRequestResponseType::StreamedXorChunks {
        return remote_read_streamed(service, request).await;
    }
    let engine = PromqlEngine::new(
        Arc::clone(&service.store),
        Arc::clone(&service.config.tenant),
        PromqlLimits::default(),
    );
    let result = tokio::task::spawn_blocking(move || {
        let mut results = Vec::with_capacity(request.queries.len());
        for query in request.queries {
            let selector = remote_read_selector(&query.matchers)?;
            let points = engine.raw_points(
                &[selector],
                query.start_timestamp_ms,
                query.end_timestamp_ms,
            )?;
            let mut series = BTreeMap::<
                BTreeMap<String, String>,
                BTreeMap<i64, (shard_stream_core::LogicalOffset, RemoteReadValue)>,
            >::new();
            for point in points {
                let timestamp =
                    i64::try_from(point.timestamp_unix_nanos / 1_000_000).unwrap_or(i64::MAX);
                let Some(value) = remote_read_value(&point, timestamp) else {
                    continue;
                };
                let values = series.entry(metric_labels(&point)).or_default();
                if values
                    .get(&timestamp)
                    .is_none_or(|(offset, _)| *offset < point.record_ref.offset)
                {
                    values.insert(timestamp, (point.record_ref.offset, value));
                }
            }
            let timeseries = series
                .into_iter()
                .map(|(labels, values)| {
                    let mut samples = Vec::new();
                    let mut histograms = Vec::new();
                    for (_, (_, value)) in values {
                        match value {
                            RemoteReadValue::Sample(sample) => samples.push(sample),
                            RemoteReadValue::Histogram(histogram) => histograms.push(*histogram),
                        }
                    }
                    prometheus_v1::TimeSeries {
                        labels: labels
                            .into_iter()
                            .map(|(name, value)| prometheus_v1::Label { name, value })
                            .collect(),
                        samples,
                        exemplars: Vec::new(),
                        histograms,
                    }
                })
                .collect();
            results.push(prometheus_v1::QueryResult { timeseries });
        }
        Ok::<_, crate::PromqlError>(prometheus_v1::ReadResponse { results })
    })
    .await;
    let response = match result {
        Ok(Ok(response)) => response,
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
                &format!("Remote Read worker failed: {error}"),
            );
        }
    };
    let compressed = match tokio::task::spawn_blocking(move || {
        let protobuf = response.encode_to_vec();
        snap::raw::Encoder::new()
            .compress_vec(&protobuf)
            .map_err(|error| error.to_string())
    })
    .await
    {
        Ok(Ok(compressed)) => compressed,
        Ok(Err(error)) => return write_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
        Err(error) => {
            return write_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Remote Read encode worker failed: {error}"),
            );
        }
    };
    (
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            ),
            (header::CONTENT_ENCODING, HeaderValue::from_static("snappy")),
        ],
        compressed,
    )
        .into_response()
}

pub(super) async fn remote_read_streamed(
    service: PrometheusService,
    request: prometheus_v1::ReadRequest,
) -> Response {
    const XOR_SAMPLES_PER_CHUNK: usize = 120;
    const MAX_FRAME_BYTES: usize = 1024 * 1024;

    let engine = PromqlEngine::new(
        Arc::clone(&service.store),
        Arc::clone(&service.config.tenant),
        PromqlLimits::default(),
    );
    let result = tokio::task::spawn_blocking(move || {
        let mut stream = Vec::new();
        for (query_index, query) in request.queries.into_iter().enumerate() {
            let selector = remote_read_selector(&query.matchers)?;
            let points = engine.raw_points(
                &[selector],
                query.start_timestamp_ms,
                query.end_timestamp_ms,
            )?;
            let mut series = BTreeMap::<
                BTreeMap<String, String>,
                BTreeMap<i64, (shard_stream_core::LogicalOffset, f64)>,
            >::new();
            for point in points {
                let Some(value) = remote_read_float(&point.value) else {
                    continue;
                };
                let timestamp =
                    i64::try_from(point.timestamp_unix_nanos / 1_000_000).unwrap_or(i64::MAX);
                let samples = series.entry(metric_labels(&point)).or_default();
                if samples
                    .get(&timestamp)
                    .is_none_or(|(offset, _)| *offset < point.record_ref.offset)
                {
                    samples.insert(timestamp, (point.record_ref.offset, value));
                }
            }
            for (labels, samples) in series {
                let labels = labels
                    .into_iter()
                    .map(|(name, value)| prometheus_v1::Label { name, value })
                    .collect::<Vec<_>>();
                let samples = samples
                    .into_iter()
                    .map(|(timestamp, (_, value))| (timestamp, value))
                    .collect::<Vec<_>>();
                let mut chunks = Vec::new();
                for samples in samples.chunks(XOR_SAMPLES_PER_CHUNK) {
                    chunks.push(prometheus_v1::Chunk {
                        min_time_ms: samples.first().expect("chunk is nonempty").0,
                        max_time_ms: samples.last().expect("chunk is nonempty").0,
                        r#type: prometheus_v1::ChunkEncoding::Xor as i32,
                        data: encode_xor_chunk(samples)
                            .map_err(|error| crate::PromqlError::new(error.to_string()))?,
                    });
                }
                append_chunked_series_frames(
                    &mut stream,
                    i64::try_from(query_index).unwrap_or(i64::MAX),
                    &labels,
                    chunks,
                    MAX_FRAME_BYTES,
                )?;
            }
        }
        Ok::<_, crate::PromqlError>(stream)
    })
    .await;
    match result {
        Ok(Ok(stream)) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static(
                    "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse",
                ),
            )],
            stream,
        )
            .into_response(),
        Ok(Err(error)) => query_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "execution",
            &error.to_string(),
        ),
        Err(error) => query_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &format!("streamed Remote Read worker failed: {error}"),
        ),
    }
}

pub(super) fn append_chunked_series_frames(
    stream: &mut Vec<u8>,
    query_index: i64,
    labels: &[prometheus_v1::Label],
    chunks: Vec<prometheus_v1::Chunk>,
    max_frame_bytes: usize,
) -> Result<(), crate::PromqlError> {
    const CHUNKS_PER_FRAME: usize = 256;
    for frame_chunks in chunks.chunks(CHUNKS_PER_FRAME) {
        let protobuf = prometheus_v1::ChunkedReadResponse {
            chunked_series: vec![prometheus_v1::ChunkedSeries {
                labels: labels.to_vec(),
                chunks: frame_chunks.to_vec(),
            }],
            query_index,
        }
        .encode_to_vec();
        if protobuf.len() > max_frame_bytes {
            return Err(crate::PromqlError::new(
                "one streamed Remote Read series frame exceeds 1 MiB",
            ));
        }
        append_stream_frame(stream, &protobuf)?;
    }
    Ok(())
}

pub(super) fn append_stream_frame(
    stream: &mut Vec<u8>,
    protobuf: &[u8],
) -> Result<(), crate::PromqlError> {
    if protobuf.len() > 1024 * 1024 {
        return Err(crate::PromqlError::new(
            "one streamed Remote Read frame exceeds 1 MiB",
        ));
    }
    append_uvarint(
        stream,
        u64::try_from(protobuf.len())
            .map_err(|_| crate::PromqlError::new("Remote Read frame is too large"))?,
    );
    stream.extend_from_slice(&crc32c::crc32c(protobuf).to_be_bytes());
    stream.extend_from_slice(protobuf);
    Ok(())
}

pub(super) fn append_uvarint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

#[allow(clippy::result_large_err)]
pub(super) fn validate_version_header(
    headers: &HeaderMap,
    version: RemoteWriteVersion,
) -> Result<(), Response> {
    let Some(observed) = headers
        .get("x-prometheus-remote-write-version")
        .and_then(|value| value.to_str().ok())
    else {
        return Err(write_error(
            StatusCode::BAD_REQUEST,
            "missing X-Prometheus-Remote-Write-Version",
        ));
    };
    let valid = match version {
        RemoteWriteVersion::V1 => observed == "0.1.0" || observed.starts_with("1."),
        RemoteWriteVersion::V2 => observed.starts_with("2."),
    };
    if valid {
        Ok(())
    } else {
        Err(write_error(
            StatusCode::BAD_REQUEST,
            "Remote Write version header conflicts with Content-Type",
        ))
    }
}
