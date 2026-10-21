//! Bounded observations of local partition checkpoints, separate from source coverage.

use std::collections::BTreeSet;

use shard_stream_core::{LogicalPartitionId, TopicPartition};

use crate::{NativeProtocolError, TelemetrySignal};

mod codec;
pub use codec::{
    decode_native_provider_status_request, decode_native_provider_status_response,
    encode_native_provider_status_request, encode_native_provider_status_response,
};
#[cfg(test)]
mod tests;

/// Maximum selected partitions in one local or native status observation.
pub const MAX_PROVIDER_STATUS_PARTITIONS: usize = 768;
/// Maximum encoded status request; counts are checked before allocation.
pub const MAX_PROVIDER_STATUS_REQUEST_BYTES: usize = 8 * 1024;
/// Maximum encoded status response, independently of native connection budgets.
pub const MAX_PROVIDER_STATUS_RESPONSE_BYTES: usize = 64 * 1024;

/// One explicitly selected signal partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProviderStatusPartition {
    /// Signal whose real sink checkpoint is observed.
    pub signal: TelemetrySignal,
    /// Logical partition within that signal.
    pub partition_id: u32,
}

impl ProviderStatusPartition {
    pub(crate) fn topic_partition(self) -> TopicPartition {
        TopicPartition::new(
            self.signal.topic_id(),
            LogicalPartitionId::new(self.partition_id),
        )
    }
}

/// Selection context for partition-wide status, not a scoped completeness claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderStatusRequest {
    /// Tenant checked against native connection authentication in production.
    pub tenant: String,
    /// Optional raw UUID echoed for caller correlation; it does not authorize a tenant.
    pub organization_uuid: Option<[u8; 16]>,
    /// Optional deployment plane echoed for caller correlation.
    pub telemetry_plane: Option<String>,
    /// Optional inclusive requested source interval start, whose coverage stays unknown.
    pub start_timestamp_unix_nanos: Option<u64>,
    /// Optional exclusive requested source interval end, whose coverage stays unknown.
    pub end_timestamp_unix_nanos: Option<u64>,
    /// Explicit, nonduplicate selection; response preserves this order.
    pub partitions: Vec<ProviderStatusPartition>,
}

impl ProviderStatusRequest {
    /// Validates fixed wire bounds before checkpoint reads or codec allocation.
    pub fn validate(&self) -> Result<(), NativeProtocolError> {
        if self.tenant.is_empty() || self.tenant.len() > 1_024 {
            return Err(NativeProtocolError::new(
                "provider status tenant must be in 1..=1024 bytes",
            ));
        }
        if self
            .telemetry_plane
            .as_ref()
            .is_some_and(|plane| plane.is_empty() || plane.len() > 64)
        {
            return Err(NativeProtocolError::new(
                "provider status plane must be in 1..=64 bytes when present",
            ));
        }
        if self
            .start_timestamp_unix_nanos
            .zip(self.end_timestamp_unix_nanos)
            .is_some_and(|(start, end)| start >= end)
        {
            return Err(NativeProtocolError::new(
                "provider status source interval must be nonempty",
            ));
        }
        if self.partitions.is_empty() || self.partitions.len() > MAX_PROVIDER_STATUS_PARTITIONS {
            return Err(NativeProtocolError::new(
                "provider status requires 1..=768 selected partitions",
            ));
        }
        let mut selected = BTreeSet::new();
        if self
            .partitions
            .iter()
            .any(|partition| !selected.insert(*partition))
        {
            return Err(NativeProtocolError::new(
                "provider status has duplicate selected partitions",
            ));
        }
        Ok(())
    }
}

/// Coverage not established by local partition checkpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderCoverage {
    /// No authoritative proof is available for this dimension.
    Unknown,
}

/// Observed allocation state of a whole authoritative native partition.
/// This is not the state of query-retained rows, an organization, or a source interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalPartitionPopulation {
    /// A coherent observation, local ownership, or checkpoint could not be established.
    Unknown,
    /// An SDK-owned standalone partition has a loaded initial checkpoint and all zero frontiers.
    VerifiedEmpty,
    /// At least one logical offset was allocated; this does not assert retained query rows.
    Allocated,
}

/// Coherent exclusive offset frontiers from the engine and its actual durable sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalPartitionProgress {
    /// Earliest retained WAL offset, not the earliest queryable object-tier offset.
    pub wal_retained_start: u64,
    /// Exclusive logical allocation end; offset zero is a real first record.
    pub allocated_end: u64,
    /// Exclusive engine contiguous/resolved log frontier, including aborted slots.
    /// It does not prove every allocated slot contains a durable emitted record.
    pub contiguous_log_end: u64,
    /// Exclusive stable/resolved WAL frontier, including aborted slots.
    /// This is independent of replica acknowledgement and applied indexing.
    pub last_stable_offset: u64,
    /// Exclusive engine replica-acknowledged WAL frontier, not an indexed replica quorum.
    pub replicated_high_watermark: u64,
    /// Exclusive previously applied sink checkpoint, bounded by the sampled stable WAL frontier.
    /// This is partition-wide and does not prove query-retention or source-interval coverage.
    pub indexed_end: Option<u64>,
    /// Exact next placement coordinate from that same applied checkpoint, independent of offsets.
    pub indexed_next_placement_sequence: Option<u64>,
}

/// One local partition observation. Unknown never becomes an offset-zero watermark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPartitionStatus {
    /// Selected signal and partition.
    pub partition: ProviderStatusPartition,
    /// Coherent real progress, or unknown after errors, concurrent changes, or deadline exhaustion.
    pub progress: Option<LocalPartitionProgress>,
    /// Allocation state, independent of requested source interval and scoped losses.
    pub population: LocalPartitionPopulation,
}

impl LocalPartitionStatus {
    pub(crate) fn unknown(partition: ProviderStatusPartition) -> Self {
        Self {
            partition,
            progress: None,
            population: LocalPartitionPopulation::Unknown,
        }
    }
}

/// Local checkpoint evidence with unsupported completeness dimensions explicit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderStatusResponse {
    /// Exact caller context; organization/plane/time fields are echoes, not authorization or proof.
    pub request: ProviderStatusRequest,
    /// Selected partition observations in request order, not a cross-partition atomic snapshot.
    pub partitions: Vec<LocalPartitionStatus>,
    /// Query retention coverage cannot be inferred from the WAL retained floor.
    pub query_retention_coverage: ProviderCoverage,
    /// Missing or delayed producer emissions are not proven by applied checkpoints.
    pub source_interval_coverage: ProviderCoverage,
    /// Scoped loss intervals require their own durable producer contract.
    pub scoped_loss_coverage: ProviderCoverage,
    /// Local WAL and sink progress do not establish an HA assignment epoch.
    pub ha_epoch: ProviderCoverage,
    /// Local applied checkpoints do not establish an indexed replica quorum.
    pub indexed_quorum: ProviderCoverage,
}

impl ProviderStatusResponse {
    pub(crate) fn validate(&self) -> Result<(), NativeProtocolError> {
        self.request.validate()?;
        if self.partitions.len() != self.request.partitions.len() {
            return Err(NativeProtocolError::new(
                "provider status response partition count mismatch",
            ));
        }
        for (selection, status) in self.request.partitions.iter().zip(&self.partitions) {
            if *selection != status.partition {
                return Err(NativeProtocolError::new(
                    "provider status response selection mismatch",
                ));
            }
            match status.progress {
                None if status.population != LocalPartitionPopulation::Unknown => {
                    return Err(NativeProtocolError::new(
                        "provider status population requires observed progress",
                    ));
                }
                Some(progress) => {
                    if progress.indexed_end.is_some()
                        != progress.indexed_next_placement_sequence.is_some()
                        || progress
                            .indexed_end
                            .zip(progress.indexed_next_placement_sequence)
                            .is_some_and(|(offset, sequence)| sequence > offset)
                        || progress.wal_retained_start > progress.last_stable_offset
                        || progress.last_stable_offset > progress.contiguous_log_end
                        || progress.replicated_high_watermark > progress.contiguous_log_end
                        || progress.contiguous_log_end > progress.allocated_end
                        || progress.indexed_end.is_some_and(|end| {
                            end < progress.wal_retained_start || end > progress.last_stable_offset
                        })
                        || (status.population == LocalPartitionPopulation::VerifiedEmpty
                            && (progress.allocated_end != 0
                                || progress.indexed_end != Some(0)
                                || progress.indexed_next_placement_sequence != Some(0)))
                        || (status.population == LocalPartitionPopulation::Allocated
                            && progress.allocated_end == 0)
                    {
                        return Err(NativeProtocolError::new(
                            "provider status has inconsistent exclusive frontiers",
                        ));
                    }
                }
                None => {}
            }
        }
        Ok(())
    }
}
