use super::*;

pub(super) async fn dispatch(
    header: NativeFrameHeader,
    payload: Vec<u8>,
    payload_hash: blake3::Hash,
    store: Arc<DurableTelemetryStore>,
    config: &NativeServerConfig,
) -> NativeFrame {
    if matches!(
        header.opcode,
        NativeOpcode::Query | NativeOpcode::QueryLogsPage | NativeOpcode::QueryMetrics | NativeOpcode::QueryTraces
    ) && let Some(gate) = &config.request_gate
        && let Err(error) = gate.check_query()
    {
        return error_frame(header, NativeStatus::Unavailable, &error);
    }
    match header.opcode {
        NativeOpcode::Ping => ok_frame(header, payload),
        NativeOpcode::Describe => {
            if let Some(gate) = &config.request_gate
                && let Err(error) = gate.check()
            {
                return error_frame(header, NativeStatus::Unavailable, &error);
            }
            let mut capabilities = NativeCapabilities {
                protocol_version: 1,
                append_v1: true,
                logical_partitions: [
                    u16::try_from(store.telemetry_partition_count())
                        .expect("DurableTelemetryStore validates the v1 partition space"),
                    u16::try_from(store.telemetry_partition_count())
                        .expect("DurableTelemetryStore validates the v1 partition space"),
                    u16::try_from(store.telemetry_partition_count())
                        .expect("DurableTelemetryStore validates the v1 partition space"),
                ],
                query_logs: true,
                query_metrics: true,
                query_traces: true,
                query_series: false,
                append_queryable: config.wait_for_index,
            };
            if let Some(gate) = &config.request_gate {
                capabilities = gate.capabilities(capabilities);
            }
            match encode_native_capabilities(&capabilities) {
                Ok(encoded) => ok_frame(header, encoded),
                Err(error) => error_frame(header, NativeStatus::Internal, &error.to_string()),
            }
        }
        NativeOpcode::Authenticate => error_frame(
            header,
            NativeStatus::BadRequest,
            "connection is already authenticated",
        ),
        NativeOpcode::Append | NativeOpcode::AppendUntracked => {
            let runtime = config.production.clone();
            let ingest_permit = match runtime.as_ref() {
                Some(runtime) => match runtime.try_ingest(payload.len()) {
                    Some(permit) => Some(permit),
                    None if runtime.lifecycle().state() != ServiceState::Ready => {
                        return error_frame(
                            header,
                            NativeStatus::Unavailable,
                            "native ingestion is draining or unavailable",
                        );
                    }
                    None => {
                        return error_frame(
                            header,
                            NativeStatus::TooManyRequests,
                            "native ingest concurrency or rate limit exceeded",
                        );
                    }
                },
                None => None,
            };
            let source_bytes = payload.len();
            if !is_native_telemetry_batch(&payload) {
                return error_frame(
                    header,
                    NativeStatus::BadRequest,
                    "native append requires the signal-aware STB1 payload",
                );
            }
            let retryable = header.opcode == NativeOpcode::Append;
            let multi_partition_untracked = !retryable
                && payload.len() >= 6
                && u16::from_le_bytes([payload[4], payload[5]]) > 1;
            let validated = if config.request_gate.is_some() {
                let (telemetry_batch, wire_ranges) = if retryable {
                    let (batch, envelope_range) =
                        match NativeTelemetryBatch::decode_native_append_with_envelope_range(
                            &payload,
                        ) {
                            Ok(batch) => batch,
                            Err(error) => {
                                return error_frame(
                                    header,
                                    NativeStatus::BadRequest,
                                    &error.to_string(),
                                );
                            }
                        };
                    (batch, vec![(envelope_range, None)])
                } else {
                    match NativeTelemetryBatch::decode_with_envelope_ranges(&payload) {
                        Ok(batch) => batch,
                        Err(error) => {
                            return error_frame(
                                header,
                                NativeStatus::BadRequest,
                                &error.to_string(),
                            );
                        }
                    }
                };
                if let Some(gate) = &config.request_gate
                    && let Err(error) = gate.check_partitions(&telemetry_batch.partitions)
                {
                    return error_frame(header, NativeStatus::Unavailable, &error);
                }
                if telemetry_batch.partitions.windows(2).any(|partitions| {
                    partitions[0].envelope.tenant != partitions[1].envelope.tenant
                }) {
                    return error_frame(
                        header,
                        NativeStatus::BadRequest,
                        "native telemetry batch must contain one tenant",
                    );
                }
                ValidatedNativeAppend::Owned {
                    batch: telemetry_batch,
                    wire_ranges,
                }
            } else if multi_partition_untracked {
                let (views, wire_ranges) =
                    match NativeTelemetryBatch::decode_native_append_views_with_ranges(&payload) {
                        Ok(views) => views,
                        Err(error) => {
                            return error_frame(
                                header,
                                NativeStatus::BadRequest,
                                &error.to_string(),
                            );
                        }
                    };
                if views.is_empty() {
                    return error_frame(
                        header,
                        NativeStatus::BadRequest,
                        "native telemetry batch requires at least one partition",
                    );
                }
                if views
                    .windows(2)
                    .any(|partitions| partitions[0].tenant != partitions[1].tenant)
                {
                    return error_frame(
                        header,
                        NativeStatus::BadRequest,
                        "native telemetry batch must contain one tenant",
                    );
                }
                let partitions: Vec<NativeEncodedPartitionAppend> = views
                    .into_iter()
                    .zip(wire_ranges)
                    .map(
                        |(view, (envelope_range, transient_range))| NativeEncodedPartitionAppend {
                            topic_partition: view.topic_partition,
                            tenant: Arc::from(view.tenant),
                            item_count: view.item_count,
                            envelope_range,
                            transient_range,
                        },
                    )
                    .collect();
                ValidatedNativeAppend::BorrowedMany { partitions }
            } else {
                let (view, envelope_range, transient_range) =
                    match NativeTelemetryBatch::decode_native_append_view_with_ranges(&payload) {
                        Ok(view) => view,
                        Err(error) => {
                            return error_frame(
                                header,
                                NativeStatus::BadRequest,
                                &error.to_string(),
                            );
                        }
                    };
                ValidatedNativeAppend::Borrowed {
                    topic_partition: view.topic_partition,
                    tenant: Arc::from(view.tenant),
                    item_count: view.item_count,
                    envelope_range,
                    transient_range,
                }
            };
            let tenant = match &validated {
                ValidatedNativeAppend::Owned { batch, .. } => {
                    Arc::clone(&batch.partitions[0].envelope.tenant)
                }
                ValidatedNativeAppend::Borrowed { tenant, .. } => Arc::clone(tenant),
                ValidatedNativeAppend::BorrowedMany { partitions } => {
                    Arc::clone(&partitions[0].tenant)
                }
            };
            if let Some(runtime) = &runtime
                && tenant.as_ref() != runtime.tenant()
            {
                return error_frame(
                    header,
                    NativeStatus::Unauthorized,
                    "native telemetry tenant does not match the authenticated tenant",
                );
            }
            let wait_for_index = config.wait_for_index;
            let payload_digest = retryable.then(|| payload_hash.to_hex().to_string());
            let wire = Bytes::from(payload);
            let retry_id = header.request_id;
            let append_result = match validated {
                ValidatedNativeAppend::Owned { batch, wire_ranges } => {
                    let records = batch
                        .partitions
                        .iter()
                        .map(|partition| partition.envelope.item_count as usize)
                        .sum::<usize>();
                    tokio::task::spawn_blocking(move || {
                        let result = if retryable {
                            let encoded_envelope = wire.slice(
                                wire_ranges
                                    .first()
                                    .expect("retryable native append has one range")
                                    .0
                                    .clone(),
                            );
                            store
                                .append_validated_telemetry_batch_with_retry_id_and_encoded_envelope(
                                    &batch,
                                    encoded_envelope,
                                    wait_for_index,
                                    retry_id,
                                    payload_digest.expect("retryable native append has a digest"),
                                )
                        } else {
                            store.append_validated_telemetry_batch_with_encoded_envelopes(
                                &batch,
                                wire,
                                &wire_ranges,
                                wait_for_index,
                            )
                        }
                            .and_then(|ack| {
                                ack.encode()
                                    .map(|encoded| (encoded, records))
                                    .map_err(|error| LokiApiError::internal(error.to_string()))
                            });
                        drop(ingest_permit);
                        result
                    })
                    .await
                }
                ValidatedNativeAppend::Borrowed {
                    topic_partition,
                    item_count,
                    envelope_range,
                    transient_range,
                    ..
                } => {
                    let encoded_envelope = wire.slice(envelope_range);
                    let transient_context = transient_range.map(|range| wire.slice(range));
                    tokio::task::spawn_blocking(move || {
                        let result = if retryable {
                            store
                                .append_validated_native_metadata_with_retry_id_and_encoded_envelope(
                                    topic_partition,
                                    item_count,
                                    encoded_envelope,
                                    transient_context,
                                    wait_for_index,
                                    retry_id,
                                    payload_digest.expect("retryable native append has a digest"),
                                )
                        } else {
                            store.append_validated_native_metadata_with_encoded_envelope(
                                topic_partition,
                                item_count,
                                encoded_envelope,
                                transient_context,
                                wait_for_index,
                            )
                        }
                            .and_then(|ack| {
                                ack.encode()
                                    .map(|encoded| (encoded, item_count as usize))
                                    .map_err(|error| LokiApiError::internal(error.to_string()))
                            });
                        drop(ingest_permit);
                        result
                    })
                    .await
                }
                ValidatedNativeAppend::BorrowedMany { partitions } => {
                    let records = partitions
                        .iter()
                        .map(|partition| partition.item_count as usize)
                        .sum::<usize>();
                    tokio::task::spawn_blocking(move || {
                        let result = store
                            .append_validated_native_metadata_with_encoded_envelopes(
                                &partitions,
                                wire,
                                wait_for_index,
                            )
                            .and_then(|ack| {
                                ack.encode()
                                    .map(|encoded| (encoded, records))
                                    .map_err(|error| LokiApiError::internal(error.to_string()))
                            });
                        drop(ingest_permit);
                        result
                    })
                    .await
                }
            };
            match append_result {
                Ok(Ok((ack, records))) => {
                    if let Some(runtime) = &config.production {
                        runtime.record_ingest(source_bytes, records);
                    }
                    ok_frame(header, ack)
                }
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(
                    header,
                    NativeStatus::Internal,
                    &format!("native append worker failed: {error}"),
                ),
            }
        }
        NativeOpcode::Query => {
            let query_permit = match config.production.as_ref() {
                Some(runtime) => match runtime.try_query() {
                    Some(permit) => {
                        runtime.record_query();
                        Some(permit)
                    }
                    None if matches!(
                        runtime.lifecycle().state(),
                        ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
                    ) =>
                    {
                        return error_frame(
                            header,
                            NativeStatus::Unavailable,
                            "native query service is unavailable",
                        );
                    }
                    None => {
                        return error_frame(
                            header,
                            NativeStatus::TooManyRequests,
                            "native query concurrency limit exceeded",
                        );
                    }
                },
                None => None,
            };
            let query = match decode_native_query(&payload) {
                Ok(query) => query,
                Err(error) => {
                    return error_frame(header, NativeStatus::BadRequest, &error.to_string());
                }
            };
            if config
                .production
                .as_ref()
                .is_some_and(|runtime| query.tenant != runtime.tenant())
            {
                return error_frame(
                    header,
                    NativeStatus::Unauthorized,
                    "native query tenant does not match the authenticated tenant",
                );
            }
            let tenant = query.tenant.clone();
            let query_timeout = config
                .production
                .as_ref()
                .map(|runtime| runtime.query_timeout());
            let worker = tokio::task::spawn_blocking(move || {
                let result = (|| match store.query_native_projected(&query)? {
                    Some(matches) => encode_native_log_query_matches(&tenant, matches)
                        .map_err(|error| LokiApiError::internal(error.to_string())),
                    None => store.query_native(&query).and_then(|entries| {
                        encode_native_log_query_result(&tenant, entries)
                            .map_err(|error| LokiApiError::internal(error.to_string()))
                    }),
                })();
                drop(query_permit);
                result
            });
            let result = match query_timeout {
                Some(timeout) => match tokio::time::timeout(timeout, worker).await {
                    Ok(result) => result,
                    Err(_) => {
                        return error_frame(
                            header,
                            NativeStatus::Timeout,
                            "native query deadline exceeded",
                        );
                    }
                },
                None => worker.await,
            };
            match result {
                Ok(Ok(encoded)) => ok_frame(header, encoded),
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(
                    header,
                    NativeStatus::Internal,
                    &format!("native query worker failed: {error}"),
                ),
            }
        }
        NativeOpcode::QueryLogsPage => {
            let query_permit = match config.production.as_ref() {
                Some(runtime) => match runtime.try_query() {
                    Some(permit) => {
                        runtime.record_query();
                        Some(permit)
                    }
                    None if matches!(
                        runtime.lifecycle().state(),
                        ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
                    ) =>
                    {
                        return error_frame(header, NativeStatus::Unavailable, "native query service is unavailable");
                    }
                    None => {
                        return error_frame(header, NativeStatus::TooManyRequests, "native query concurrency limit exceeded");
                    }
                },
                None => None,
            };
            let query = match crate::decode_native_log_page_query(&payload) {
                Ok(query) => query,
                Err(error) => return error_frame(header, NativeStatus::BadRequest, &error.to_string()),
            };
            if config.production.as_ref().is_some_and(|runtime| query.query.tenant != runtime.tenant()) {
                return error_frame(header, NativeStatus::Unauthorized, "native query tenant does not match the authenticated tenant");
            }
            let timeout = config.production.as_ref().map(|runtime| runtime.query_timeout());
            let worker = tokio::task::spawn_blocking(move || {
                let result = store
                    .query_native_page(&query)
                    .and_then(|page| crate::encode_native_log_query_page(page).map_err(|error| LokiApiError::internal(error.to_string())));
                drop(query_permit);
                result
            });
            let result = match timeout {
                Some(timeout) => match tokio::time::timeout(timeout, worker).await {
                    Ok(result) => result,
                    Err(_) => return error_frame(header, NativeStatus::Timeout, "native query deadline exceeded"),
                },
                None => worker.await,
            };
            match result {
                Ok(Ok(encoded)) => ok_frame(header, encoded),
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(header, NativeStatus::Internal, &format!("native query worker failed: {error}")),
            }
        }
        NativeOpcode::QueryMetrics => {
            let query_permit = match config.production.as_ref() {
                Some(runtime) => match runtime.try_query() {
                    Some(permit) => {
                        runtime.record_query();
                        Some(permit)
                    }
                    None if matches!(
                        runtime.lifecycle().state(),
                        ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
                    ) =>
                    {
                        return error_frame(
                            header,
                            NativeStatus::Unavailable,
                            "native query service is unavailable",
                        );
                    }
                    None => {
                        return error_frame(
                            header,
                            NativeStatus::TooManyRequests,
                            "native query concurrency limit exceeded",
                        );
                    }
                },
                None => None,
            };
            let query = match decode_native_metric_query(&payload) {
                Ok(query) => query,
                Err(error) => {
                    return error_frame(header, NativeStatus::BadRequest, &error.to_string());
                }
            };
            if config
                .production
                .as_ref()
                .is_some_and(|runtime| query.tenant.as_ref() != runtime.tenant())
            {
                return error_frame(
                    header,
                    NativeStatus::Unauthorized,
                    "native metric query tenant does not match the authenticated tenant",
                );
            }
            let query_timeout = config
                .production
                .as_ref()
                .map(|runtime| runtime.query_timeout());
            let worker = tokio::task::spawn_blocking(move || {
                let result = store.query_metrics(&query);
                drop(query_permit);
                result
            });
            let result = match query_timeout {
                Some(timeout) => match tokio::time::timeout(timeout, worker).await {
                    Ok(result) => result,
                    Err(_) => {
                        return error_frame(
                            header,
                            NativeStatus::Timeout,
                            "native metric query deadline exceeded",
                        );
                    }
                },
                None => worker.await,
            };
            match result {
                Ok(Ok(points)) => match encode_native_metric_query_result(&points) {
                    Ok(encoded) => ok_frame(header, encoded),
                    Err(error) => error_frame(header, NativeStatus::Internal, &error.to_string()),
                },
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(
                    header,
                    NativeStatus::Internal,
                    &format!("native metric query worker failed: {error}"),
                ),
            }
        }
        NativeOpcode::QueryTraces => {
            let query_permit = match config.production.as_ref() {
                Some(runtime) => match runtime.try_query() {
                    Some(permit) => {
                        runtime.record_query();
                        Some(permit)
                    }
                    None if matches!(
                        runtime.lifecycle().state(),
                        ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
                    ) =>
                    {
                        return error_frame(
                            header,
                            NativeStatus::Unavailable,
                            "native query service is unavailable",
                        );
                    }
                    None => {
                        return error_frame(
                            header,
                            NativeStatus::TooManyRequests,
                            "native query concurrency limit exceeded",
                        );
                    }
                },
                None => None,
            };
            let query = match decode_native_trace_query(&payload) {
                Ok(query) => query,
                Err(error) => {
                    return error_frame(header, NativeStatus::BadRequest, &error.to_string());
                }
            };
            if config
                .production
                .as_ref()
                .is_some_and(|runtime| query.tenant.as_ref() != runtime.tenant())
            {
                return error_frame(
                    header,
                    NativeStatus::Unauthorized,
                    "native trace query tenant does not match the authenticated tenant",
                );
            }
            let query_timeout = config
                .production
                .as_ref()
                .map(|runtime| runtime.query_timeout());
            let worker = tokio::task::spawn_blocking(move || {
                let result = store.query_traces(&query);
                drop(query_permit);
                result
            });
            let result = match query_timeout {
                Some(timeout) => match tokio::time::timeout(timeout, worker).await {
                    Ok(result) => result,
                    Err(_) => {
                        return error_frame(
                            header,
                            NativeStatus::Timeout,
                            "native trace query deadline exceeded",
                        );
                    }
                },
                None => worker.await,
            };
            match result {
                Ok(Ok(spans)) => match encode_native_trace_query_result(&spans) {
                    Ok(encoded) => ok_frame(header, encoded),
                    Err(error) => error_frame(header, NativeStatus::Internal, &error.to_string()),
                },
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(
                    header,
                    NativeStatus::Internal,
                    &format!("native trace query worker failed: {error}"),
                ),
            }
        }
    }
}

pub(super) fn ok_frame(header: NativeFrameHeader, payload: Vec<u8>) -> NativeFrame {
    NativeFrame::response(header.opcode, header.request_id, NativeStatus::Ok, payload)
        .expect("bounded request produced a bounded response")
}

pub(super) fn store_error_frame(header: NativeFrameHeader, error: LokiApiError) -> NativeFrame {
    let status = match error.status() {
        axum::http::StatusCode::BAD_REQUEST => NativeStatus::BadRequest,
        axum::http::StatusCode::UNAUTHORIZED | axum::http::StatusCode::FORBIDDEN => {
            NativeStatus::Unauthorized
        }
        axum::http::StatusCode::SERVICE_UNAVAILABLE => NativeStatus::Unavailable,
        axum::http::StatusCode::TOO_MANY_REQUESTS => NativeStatus::TooManyRequests,
        _ => NativeStatus::Internal,
    };
    error_frame(header, status, &error.to_string())
}

pub(super) fn error_frame(
    header: NativeFrameHeader,
    status: NativeStatus,
    message: &str,
) -> NativeFrame {
    let mut payload = message.as_bytes();
    if payload.len() > 4_096 {
        payload = &payload[..4_096];
    }
    NativeFrame::response(header.opcode, header.request_id, status, payload.to_vec())
        .expect("error payload is bounded")
}

pub(super) fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
