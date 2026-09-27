use super::*;

pub(super) fn apply_durable_appends(
    stripe: &mut TelemetryStripeState,
    checkpoints: &Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>,
    journal: Option<&SinkJournal>,
    validated_signal_cache: &ValidatedSignalCache,
    expected: DurableSinkCheckpoint,
    appends: &[DurableAppend],
    next: DurableSinkCheckpoint,
) -> EngineResult<DurableSinkApply> {
    if expected.topic_partition != next.topic_partition {
        return Err(EngineError::DurableSinkCheckpoint(
            "expected and next checkpoints refer to different partitions".into(),
        ));
    }
    if appends
        .iter()
        .any(|append| append.topic_partition() != expected.topic_partition)
    {
        return Err(EngineError::DurableSinkCheckpoint(
            "sink transaction contains appends from another partition".into(),
        ));
    }

    let actual = checkpoints
        .lock()
        .map_err(|_| {
            EngineError::DurableSinkUnavailable("shard-telemetry checkpoint lock poisoned".into())
        })?
        .get(&expected.topic_partition)
        .copied()
        .unwrap_or_else(|| DurableSinkCheckpoint::initial(expected.topic_partition));
    if !checkpoint_allows_lane_gap(actual, expected) {
        return Ok(DurableSinkApply::CheckpointConflict(actual));
    }

    if let Some(journal) = journal {
        journal
            .append(expected, appends, next)
            .map_err(log_error_to_engine)?;
    }
    // StreamEngine calls the sink factory's validate_append before delivering
    // live commands. The Bytes payload is then forwarded unchanged, so the
    // indexer can retain its slice without repeating the envelope checksum.
    index_durable_appends(
        stripe,
        appends,
        validated_signal_cache,
        true,
        expected,
        next,
    )
    .map_err(log_error_to_engine)?;
    stripe
        .logs
        .offload_indexed_groups(false)
        .map_err(log_error_to_engine)?;
    if matches!(
        next.topic_partition.topic_id,
        crate::TRACES_TOPIC_ID | crate::METRICS_TOPIC_ID
    ) {
        offload_signal_partition(stripe, next.topic_partition, next, false)
            .map_err(log_error_to_engine)?;
    }
    checkpoints
        .lock()
        .map_err(|_| {
            EngineError::DurableSinkUnavailable("shard-telemetry checkpoint lock poisoned".into())
        })?
        .insert(next.topic_partition, next);
    Ok(DurableSinkApply::Applied)
}

pub(super) fn index_durable_appends(
    stripe: &mut TelemetryStripeState,
    appends: &[DurableAppend],
    validated_signal_cache: &ValidatedSignalCache,
    already_validated: bool,
    expected: DurableSinkCheckpoint,
    next: DurableSinkCheckpoint,
) -> TelemetryResult<()> {
    for append in appends {
        if append.physical_shard_id != stripe.stream_shard_id {
            return Err(TelemetryError::WrongStripe {
                expected: stripe.stream_shard_id,
                observed: append.physical_shard_id,
            });
        }
        let topic_partition =
            TopicPartition::new(append.reservation.topic_id, append.reservation.partition_id);
        index_payload(
            stripe,
            topic_partition,
            append.reservation.first_offset,
            Some(append.reservation.record_count.get()),
            &append.payload,
            append.transient_context.as_deref(),
            Some(validated_signal_cache),
            already_validated,
            (expected, next),
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn index_payload(
    stripe: &mut TelemetryStripeState,
    topic_partition: TopicPartition,
    first_offset: shard_stream_core::LogicalOffset,
    expected_count: Option<u32>,
    payload: &Bytes,
    transient_context: Option<&[u8]>,
    validated_signal_cache: Option<&ValidatedSignalCache>,
    already_validated: bool,
    checkpoints: (DurableSinkCheckpoint, DurableSinkCheckpoint),
) -> TelemetryResult<()> {
    if !TelemetryEnvelope::is_encoded(payload) {
        return Err(TelemetryError::InvalidTelemetryEnvelope(
            "durable telemetry append is not a STEL envelope",
        ));
    }
    let envelope = if already_validated {
        crate::envelope::TelemetryEnvelope::decode_view_after_validation(payload)?
    } else {
        crate::envelope::TelemetryEnvelope::decode_view(payload)?
    };
    if envelope.signal.topic_id() != topic_partition.topic_id {
        return Err(TelemetryError::InvalidTelemetryEnvelope(
            "signal does not match its shard-stream topic",
        ));
    }
    if expected_count.is_some_and(|count| count != envelope.item_count) {
        return Err(TelemetryError::InvalidTelemetryEnvelope(
            "durable reservation count disagrees with envelope",
        ));
    }
    let cached_signal = match envelope.signal {
        TelemetrySignal::Logs => None,
        TelemetrySignal::Traces | TelemetrySignal::Metrics => {
            validated_signal_cache.and_then(|cache| cache.take(envelope.checksum))
        }
    };
    match envelope.signal {
        TelemetrySignal::Logs => {
            // `decode_indexed_ingest_frames` validates every compressed group
            // before publishing its frame metadata. Repeating that checksum
            // scan here only burns CPU on the live path; the same validation
            // remains active during recovery through the stripe apply path.
            let payload_start = payload.len().checked_sub(envelope.payload.len()).ok_or(
                TelemetryError::InvalidTelemetryEnvelope("log payload is outside its envelope"),
            )?;
            stripe.logs.apply_checkpointed_ingest_pack(
                Arc::from(envelope.tenant),
                topic_partition,
                first_offset,
                envelope.item_count,
                payload.slice(payload_start..),
                transient_context,
                already_validated,
                checkpoints,
            )?;
        }
        TelemetrySignal::Traces => {
            let records = match cached_signal {
                Some(ValidatedSignalPayload::Traces(records)) => records,
                _ => decode_trace_block(envelope.payload)?,
            };
            validate_relative_offsets(
                records.iter().map(|record| record.record_ref.offset),
                envelope.item_count,
            )?;
            for mut record in records {
                record.stream_shard_id = stripe.stream_shard_id;
                record.record_ref = crate::TelemetryRecordRef::for_signal(
                    TelemetrySignal::Traces,
                    topic_partition,
                    absolute_offset(topic_partition, first_offset, record.record_ref.offset)?,
                );
                let append_time = record.end_time_unix_nanos().unwrap_or(u64::MAX);
                let outcome = stripe.traces.apply_ref(&record, append_time)?;
                if matches!(
                    outcome,
                    TraceApplyOutcome::Inserted | TraceApplyOutcome::Replaced
                ) {
                    stripe.correlations.index_span(&record);
                }
            }
        }
        TelemetrySignal::Metrics => {
            if envelope.routing_metadata.len() != 5 {
                return Err(TelemetryError::InvalidTelemetryEnvelope(
                    "metric routing metadata must contain partition and protocol",
                ));
            }
            let routed_partition = u32::from_le_bytes(
                envelope.routing_metadata[..4]
                    .try_into()
                    .expect("fixed metric partition bytes"),
            );
            if routed_partition != topic_partition.partition_id.get() {
                return Err(TelemetryError::InvalidTelemetryEnvelope(
                    "metric routing metadata partition mismatch",
                ));
            }
            let protocol = MetricIngestProtocol::from_wire(envelope.routing_metadata[4])?;
            let records = match cached_signal {
                Some(ValidatedSignalPayload::Metrics(records)) => records,
                _ => decode_metric_chunk(envelope.payload)?,
            };
            validate_relative_offsets(
                records.iter().map(|record| record.record_ref.offset),
                envelope.item_count,
            )?;
            for mut record in records {
                record.stream_shard_id = stripe.stream_shard_id;
                record.record_ref = crate::TelemetryRecordRef::for_signal(
                    TelemetrySignal::Metrics,
                    topic_partition,
                    absolute_offset(topic_partition, first_offset, record.record_ref.offset)?,
                );
                let outcome = stripe.metrics.apply_ref(&record, protocol)?;
                if matches!(
                    outcome,
                    MetricApplyOutcome::Inserted
                        | MetricApplyOutcome::Replaced
                        | MetricApplyOutcome::OutOfOrder
                ) {
                    stripe.correlations.index_metric(&record);
                }
            }
        }
    }
    Ok(())
}
