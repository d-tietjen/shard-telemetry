use super::*;

pub(super) fn offload_signal_partition(
    stripe: &mut TelemetryStripeState,
    partition: TopicPartition,
    checkpoint: DurableSinkCheckpoint,
    force: bool,
) -> TelemetryResult<usize> {
    let signal = if partition.topic_id == crate::TRACES_TOPIC_ID {
        TelemetrySignal::Traces
    } else if partition.topic_id == crate::METRICS_TOPIC_ID {
        TelemetrySignal::Metrics
    } else {
        return Ok(0);
    };
    let Some(state) = stripe.signal_tiers.get(&signal) else {
        return Ok(0);
    };
    if !state.tiers.contains_key(&partition) {
        return Ok(0);
    }
    let pending = match signal {
        TelemetrySignal::Traces => stripe.traces.pending_partition(partition),
        TelemetrySignal::Metrics => stripe.metrics.pending_partition(partition),
        TelemetrySignal::Logs => unreachable!("signal was selected above"),
    };
    let pending_bytes = pending.iter().try_fold(0u64, |total, payload| {
        total
            .checked_add(
                u64::try_from(payload.payload.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            )
            .ok_or(TelemetryError::RecordTooLarge)
    })?;
    let block_trigger = state.config.max_blocks_per_group.saturating_div(2).max(1);
    if !force
        && pending_bytes < state.config.target_group_payload_bytes
        && pending.len() < block_trigger
    {
        return Ok(0);
    }

    match signal {
        TelemetrySignal::Traces => {
            let now_nanos = pending
                .iter()
                .map(|payload| payload.max_timestamp_unix_nanos)
                .max()
                .unwrap_or(0);
            stripe.traces.seal_partition(partition, now_nanos)?;
        }
        TelemetrySignal::Metrics => stripe.metrics.seal_partition(partition)?,
        TelemetrySignal::Logs => unreachable!("signal was selected above"),
    }
    let mut payloads = match signal {
        TelemetrySignal::Traces => stripe.traces.pending_partition(partition),
        TelemetrySignal::Metrics => stripe.metrics.pending_partition(partition),
        TelemetrySignal::Logs => unreachable!("signal was selected above"),
    };
    if payloads.is_empty() {
        return Ok(0);
    }
    payloads.sort_unstable_by_key(|payload| payload.resident_id);
    let payload_bytes = payloads.iter().try_fold(0u64, |total, payload| {
        total
            .checked_add(
                u64::try_from(payload.payload.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            )
            .ok_or(TelemetryError::RecordTooLarge)
    })?;
    let recovery_state = match signal {
        TelemetrySignal::Metrics => stripe
            .metrics
            .accumulator_checkpoints_for_partition(partition)?,
        TelemetrySignal::Traces => Vec::new(),
        TelemetrySignal::Logs => unreachable!("signal was selected above"),
    };
    let state = stripe
        .signal_tiers
        .get_mut(&signal)
        .expect("signal tier was checked above");
    if payloads.len() > state.config.max_blocks_per_group
        || payload_bytes > state.config.max_group_payload_bytes
    {
        return Err(TelemetryError::ObjectStore(format!(
            "complete {signal:?} partition boundary needs {} blocks and {payload_bytes} bytes, exceeding the configured object group bound",
            payloads.len()
        )));
    }
    let tier = state
        .tiers
        .get_mut(&partition)
        .expect("signal partition tier was checked above");
    let group_sequence = tier
        .root()
        .pages
        .last()
        .map_or(0, |page| page.last_group_sequence.saturating_add(1));
    let first_block_id = tier.root().next_block_id;
    let group_directory = state.spool_directory.join(format!(
        "topic-{:032x}-partition-{}/group-{group_sequence:020}",
        partition.topic_id.get(),
        partition.partition_id.get()
    ));
    let source = stage_signal_group(
        &group_directory,
        "signal",
        signal,
        group_sequence,
        first_block_id,
        TierCheckpoint {
            next_placement_sequence: checkpoint.next_placement_sequence.get(),
            next_offset: checkpoint.next_offset.get(),
        },
        &payloads,
        &recovery_state,
    )?;
    let staged_paths = source
        .artifacts
        .iter()
        .map(|artifact| artifact.path.clone())
        .collect::<Vec<_>>();
    let manifest = tier.publish_group(source)?;
    if state.warm_local_cache_on_publish {
        let entry = tier
            .latest_group_cached(&state.control_cache)?
            .ok_or_else(|| {
                TelemetryError::CorruptTier(
                    "published signal group is missing from its catalog".into(),
                )
            })?;
        let _ = tier.load_group_cached(&entry, &state.control_cache)?;
        for (artifact, source_path) in manifest.artifacts.iter().zip(&staged_paths) {
            match artifact.kind {
                TierArtifactKind::PayloadPack => {
                    state.payload_cache.admit_file(artifact, source_path)?;
                }
                TierArtifactKind::QueryIndex => {
                    state.control_cache.admit_file(artifact, source_path)?;
                }
                TierArtifactKind::Dictionary | TierArtifactKind::DictionaryCatalog => {}
            }
        }
    }
    for path in staged_paths {
        if let Err(error) = fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!(
                "shard-telemetry retained published signal spool {} after cleanup failed: {error}",
                path.display()
            );
        }
    }
    let _ = fs::remove_dir(&group_directory);
    let resident_ids = payloads
        .iter()
        .map(|payload| payload.resident_id)
        .collect::<Vec<_>>();
    match signal {
        TelemetrySignal::Traces => stripe.traces.release_published_blocks(&resident_ids),
        TelemetrySignal::Metrics => stripe.metrics.release_published_chunks(&resident_ids),
        TelemetrySignal::Logs => unreachable!("signal was selected above"),
    }
    Ok(1)
}

pub(super) fn flush_object_tiers(
    stripe: &mut TelemetryStripeState,
    checkpoints: &Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>,
) -> TelemetryResult<usize> {
    let mut published = stripe.logs.offload_indexed_groups(true)?;
    let checkpoints = checkpoints
        .lock()
        .map_err(|_| TelemetryError::StorageIo("sink checkpoint lock is poisoned".into()))?
        .clone();
    let mut partitions = stripe
        .signal_tiers
        .values()
        .flat_map(|state| state.tiers.keys().copied())
        .collect::<Vec<_>>();
    partitions.sort_unstable();
    for partition in partitions {
        let Some(checkpoint) = checkpoints.get(&partition).copied() else {
            continue;
        };
        published = published.saturating_add(offload_signal_partition(
            stripe, partition, checkpoint, true,
        )?);
    }
    stripe.logs.reclaim_retired_object_generations()?;
    for state in stripe.signal_tiers.values_mut() {
        for tier in state.tiers.values_mut() {
            tier.reclaim_retired_objects()?;
        }
    }
    Ok(published)
}

pub(super) fn retain_object_tiers(
    stripe: &mut TelemetryStripeState,
    cutoff_timestamp_unix_nanos: u64,
    max_payload_bytes_per_partition: Option<u64>,
) -> TelemetryResult<TierRetentionReport> {
    stripe
        .correlations
        .retain_since_timestamp(cutoff_timestamp_unix_nanos);
    let mut total = stripe
        .logs
        .retain_object_tier_since(cutoff_timestamp_unix_nanos)?;
    if let Some(max_payload_bytes) = max_payload_bytes_per_partition {
        add_retention_report(
            &mut total,
            stripe
                .logs
                .retain_object_tier_to_payload_bytes(max_payload_bytes)?,
        );
    }
    for state in stripe.signal_tiers.values_mut() {
        for tier in state.tiers.values_mut() {
            let report = tier.retain_since_timestamp(cutoff_timestamp_unix_nanos)?;
            add_retention_report(&mut total, report);
            if let Some(max_payload_bytes) = max_payload_bytes_per_partition {
                let report = tier.retain_to_payload_bytes(max_payload_bytes)?;
                add_retention_report(&mut total, report);
            }
        }
    }
    Ok(total)
}

pub(super) fn add_retention_report(total: &mut TierRetentionReport, report: TierRetentionReport) {
    total.retired_groups = total.retired_groups.saturating_add(report.retired_groups);
    total.retired_payload_bytes = total
        .retired_payload_bytes
        .saturating_add(report.retired_payload_bytes);
    total.retired_objects = total.retired_objects.saturating_add(report.retired_objects);
}

pub(super) fn read_signal_tier_payloads(
    state: &SignalTierState,
    partitions: &[TopicPartition],
    range: TierQueryRange,
    correlation: Option<&CorrelationQuery>,
) -> TelemetryResult<Vec<CachedObjectRange>> {
    let mut payloads = Vec::new();
    let expected_codec = match state.signal {
        TelemetrySignal::Traces => "trace-native",
        TelemetrySignal::Metrics => "metric-native",
        TelemetrySignal::Logs => return Ok(payloads),
    };
    for partition in partitions {
        let Some(tier) = state.tiers.get(partition) else {
            continue;
        };
        let groups = match correlation {
            Some(query) => tier.candidate_groups_cached_for_correlation(
                range,
                &state.control_cache,
                query,
                state.signal,
            )?,
            None => tier.candidate_groups_cached(range, &state.control_cache)?,
        };
        for group in groups {
            let manifest = tier.load_group_cached(&group, &state.control_cache)?;
            let artifact = manifest
                .artifact(TierArtifactKind::PayloadPack)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no payload pack".into()))?;
            let metadata = ObjectMetadata {
                bytes: artifact.bytes,
                version_token: artifact.checksum.clone(),
                content_digest: artifact.checksum.clone(),
            };
            let blocks = manifest
                .blocks
                .iter()
                .filter(|block| {
                    block.compression_codec == expected_codec
                        && range.signal_identity.is_none_or(|identity| {
                            block
                                .min_signal_identity
                                .zip(block.max_signal_identity)
                                .is_some_and(|(minimum, maximum)| {
                                    identity >= minimum && identity <= maximum
                                })
                        })
                        && range
                            .min_timestamp_unix_nanos
                            .is_none_or(|minimum| block.max_timestamp_unix_nanos >= minimum)
                        && range
                            .max_timestamp_unix_nanos
                            .is_none_or(|maximum| block.min_timestamp_unix_nanos <= maximum)
                        && correlation.is_none_or(|query| {
                            block.correlation_filter.as_ref().is_some_and(|filter| {
                                if state.signal == TelemetrySignal::Traces {
                                    block
                                        .min_signal_identity
                                        .zip(block.max_signal_identity)
                                        .is_some_and(|(minimum, maximum)| {
                                            filter.may_match_trace_block(query, minimum, maximum)
                                        })
                                } else {
                                    filter.may_match(query)
                                }
                            })
                        })
                })
                .collect::<Vec<_>>();
            let ranges = blocks
                .iter()
                .map(|block| {
                    let end = block
                        .payload_offset
                        .checked_add(block.payload_bytes)
                        .ok_or(TelemetryError::RecordTooLarge)?;
                    Ok(block.payload_offset..end)
                })
                .collect::<TelemetryResult<Vec<_>>>()?;
            let encoded = state.payload_cache.read_shared_ranges_with_metadata(
                tier.object_store(),
                &artifact.object_key,
                &metadata,
                &ranges,
            )?;
            for (block, payload) in blocks.into_iter().zip(encoded) {
                if blake3::hash(payload.as_ref()).to_hex().as_str() != block.payload_checksum {
                    return Err(TelemetryError::CorruptTier(format!(
                        "signal block {} payload checksum failed",
                        block.block_id
                    )));
                }
                payloads.push(payload);
            }
        }
    }
    Ok(payloads)
}
