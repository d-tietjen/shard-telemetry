//! Checkpoint observations use real engine metadata, never counts or maximum returned offsets.

use super::*;
use crate::provider_status::{
    LocalPartitionPopulation, LocalPartitionProgress, LocalPartitionStatus, ProviderCoverage,
    ProviderStatusRequest, ProviderStatusResponse,
};

impl DurableTelemetryStore {
    /// Observes explicitly selected local partitions using real WAL and applied-sink frontiers.
    ///
    /// The tenant/organization/plane/window is caller context; progress describes whole
    /// partitions. This API does not authenticate direct Rust callers or prove source,
    /// query-retention, loss, HA epoch, or indexed-quorum coverage. Native production
    /// dispatch authenticates the tenant and applies its existing query controls.
    /// Errors or concurrent watermark changes yield unknown for that partition. One
    /// overall sampling budget stops further reads; an already blocking engine metadata
    /// operation can outlive it, so callers must retain physical admission until return.
    pub fn local_provider_status(
        &self,
        request: &ProviderStatusRequest,
    ) -> Result<ProviderStatusResponse, LokiApiError> {
        self.local_provider_status_with_budget(request, self.indexed_ack_timeout)
    }

    pub(crate) fn local_provider_status_with_budget(
        &self,
        request: &ProviderStatusRequest,
        budget: Duration,
    ) -> Result<ProviderStatusResponse, LokiApiError> {
        request
            .validate()
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        if request
            .partitions
            .iter()
            .any(|partition| partition.partition_id >= self.tenant_partitions)
        {
            return Err(LokiApiError::bad_request(
                "provider status partition is outside this store's routing space",
            ));
        }
        let started = Instant::now();
        let mut partitions = Vec::with_capacity(request.partitions.len());
        for &partition in &request.partitions {
            let unknown = || LocalPartitionStatus::unknown(partition);
            if started.elapsed() >= budget {
                partitions.push(unknown());
                continue;
            }
            let topic_partition = partition.topic_partition();
            let observation = (|| {
                let before = self.engine.watermarks(topic_partition).ok()?;
                if started.elapsed() >= budget {
                    return None;
                }
                let checkpoint = self.engine.durable_sink_checkpoint(topic_partition).ok()?;
                if started.elapsed() >= budget {
                    return None;
                }
                let after = self.engine.watermarks(topic_partition).ok()?;
                if started.elapsed() >= budget
                    || before != after
                    || after.log_start > after.last_stable_offset
                    || after.last_stable_offset > after.contiguous_log_end
                    || after.replicated_high_watermark > after.contiguous_log_end
                    || after.contiguous_log_end > after.allocated_end
                {
                    return None;
                }
                let indexed_end = match checkpoint {
                    Some(checkpoint)
                        if checkpoint.topic_partition == topic_partition
                        && checkpoint.next_offset >= after.log_start
                        && checkpoint.next_offset <= after.last_stable_offset
                        // The pinned per-TP sequencer adds one placement and a
                        // nonzero record count per reservation. Keep both coordinates;
                        // this is only an impossible-state guard, not a conversion.
                        && checkpoint.next_placement_sequence.get() <= checkpoint.next_offset.get() =>
                    {
                        Some(checkpoint.next_offset.get())
                    }
                    Some(_) => return None,
                    None => None,
                };
                let population = if after.allocated_end.get() > 0 {
                    LocalPartitionPopulation::Allocated
                } else if self.physical_shard_count.is_some()
                    && checkpoint.is_some_and(|checkpoint| {
                        checkpoint == DurableSinkCheckpoint::initial(topic_partition)
                    })
                    && after.log_start.get() == 0
                    && after.contiguous_log_end.get() == 0
                    && after.replicated_high_watermark.get() == 0
                    && after.last_stable_offset.get() == 0
                {
                    LocalPartitionPopulation::VerifiedEmpty
                } else {
                    LocalPartitionPopulation::Unknown
                };
                Some(LocalPartitionStatus {
                    partition,
                    population,
                    progress: Some(LocalPartitionProgress {
                        wal_retained_start: after.log_start.get(),
                        allocated_end: after.allocated_end.get(),
                        contiguous_log_end: after.contiguous_log_end.get(),
                        last_stable_offset: after.last_stable_offset.get(),
                        replicated_high_watermark: after.replicated_high_watermark.get(),
                        indexed_end,
                        indexed_next_placement_sequence: checkpoint
                            .map(|checkpoint| checkpoint.next_placement_sequence.get()),
                    }),
                })
            })();
            partitions.push(observation.unwrap_or_else(unknown));
        }
        Ok(ProviderStatusResponse {
            request: request.clone(),
            partitions,
            query_retention_coverage: ProviderCoverage::Unknown,
            source_interval_coverage: ProviderCoverage::Unknown,
            scoped_loss_coverage: ProviderCoverage::Unknown,
            ha_epoch: ProviderCoverage::Unknown,
            indexed_quorum: ProviderCoverage::Unknown,
        })
    }
}
