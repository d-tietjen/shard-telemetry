use super::*;

impl TelemetryService {
    /// Forces every owner stripe to publish complete pending append boundaries.
    pub fn flush_object_tier(&self) -> TelemetryResult<usize> {
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender.send(SinkCommand::Flush { response }).map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped before accepting a flush"
                ))
            })?;
            responses.push((shard_id, receiver));
        }
        responses
            .into_iter()
            .try_fold(0usize, |total, (shard_id, receiver)| {
                let published = receiver.recv().map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped while flushing"
                    ))
                })??;
                Ok(total.saturating_add(published))
            })
    }

    /// Applies bounded physical retention to every signal catalog in parallel.
    pub fn retain_object_tier_since(
        &self,
        cutoff_timestamp_unix_nanos: u64,
    ) -> TelemetryResult<TierRetentionReport> {
        self.retain_object_tier(cutoff_timestamp_unix_nanos, None)
    }

    /// Applies time and optional per-partition payload-cap retention to every
    /// signal catalog in parallel.
    pub fn retain_object_tier(
        &self,
        cutoff_timestamp_unix_nanos: u64,
        max_payload_bytes_per_partition: Option<u64>,
    ) -> TelemetryResult<TierRetentionReport> {
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::RetainObjectTier {
                    cutoff_timestamp_unix_nanos,
                    max_payload_bytes_per_partition,
                    response,
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting object retention"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        responses.into_iter().try_fold(
            TierRetentionReport::default(),
            |mut total, (shard_id, receiver)| {
                let report = receiver.recv().map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped during object retention"
                    ))
                })??;
                total.retired_groups = total.retired_groups.saturating_add(report.retired_groups);
                total.retired_payload_bytes = total
                    .retired_payload_bytes
                    .saturating_add(report.retired_payload_bytes);
                total.retired_objects =
                    total.retired_objects.saturating_add(report.retired_objects);
                Ok(total)
            },
        )
    }

    /// Returns compressed bytes still resident while awaiting a complete group.
    pub fn retained_payload_bytes(&self) -> TelemetryResult<u64> {
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::RetainedPayloadBytes { response })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before reporting resident bytes"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        responses
            .into_iter()
            .try_fold(0u64, |total, (shard_id, receiver)| {
                let bytes = receiver.recv().map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped while reporting resident bytes"
                    ))
                })?;
                Ok(total.saturating_add(bytes))
            })
    }
}
