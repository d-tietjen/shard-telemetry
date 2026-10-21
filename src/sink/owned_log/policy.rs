//! Public finite configuration translated to the existing real child policies.
use super::*;
use crate::ingest_pack::{FrameDecompressionLimits, IndexedFramePreparationLimits};
use crate::sink_journal::JournalAppendLimits;
use crate::stripe::LogIndexPublicationLimits;
use crate::structural::EmbeddedIndexDecodeLimits;

/// Explicit native streaming reconstruction policy for durable log frames.
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
pub struct OwnedLogDecompressionLimits {
    /// Cumulative accepted compressed input ceiling per child.
    pub max_input_bytes: usize,
    /// Largest actual decoded structural frame accepted.
    pub max_output_bytes: usize,
    /// Fixed native input slice ceiling per attempt.
    pub input_chunk_bytes: usize,
    /// Fixed native output slice ceiling per attempt.
    pub output_chunk_bytes: usize,
    /// Cumulative native stream-call ceiling per child.
    pub max_stream_calls: usize,
    /// Selected zstd buffered-window policy; not a frame-header promise.
    pub max_window_log: u32,
    /// Prepaid actual native context request; measured excess refuses.
    pub max_decoder_bytes: usize,
    /// Child declared-state ceiling, additional to shared native work.
    pub max_declared_bytes: usize,
    /// Child element-slot ceiling, additional to shared native work.
    pub max_entries: usize,
    /// Child work ceiling, additional to shared native work.
    pub max_work_bytes: usize,
}
impl From<OwnedLogDecompressionLimits> for FrameDecompressionLimits {
    fn from(p: OwnedLogDecompressionLimits) -> Self {
        Self {
            max_input_bytes: p.max_input_bytes,
            max_output_bytes: p.max_output_bytes,
            input_chunk_bytes: p.input_chunk_bytes,
            output_chunk_bytes: p.output_chunk_bytes,
            max_stream_calls: p.max_stream_calls,
            max_window_log: p.max_window_log,
            max_decoder_bytes: p.max_decoder_bytes,
            max_declared_bytes: p.max_declared_bytes,
            max_entries: p.max_entries,
            max_work_bytes: p.max_work_bytes,
        }
    }
}

/// Finite configured ceilings for one genuine single-log command.
///
/// Child policies use these ceilings as additional local bounds; every child
/// still spends the same nonresettable native ledger and original expiry.
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
pub struct OwnedLogApplyLimits {
    /// Largest STEL payload or present live-context byte length.
    pub max_input_bytes: usize,
    /// Largest input reservation/frame record count.
    pub max_records: usize,
    /// Largest grouped-frame count.
    pub max_groups: usize,
    /// Largest query partition-table capacity.
    pub max_partitions: usize,
    /// Largest append-vector capacity per query partition.
    pub max_appends_per_partition: usize,
    /// Per-kind dictionary/term/field/locator/run decode ceiling.
    pub max_index_items: usize,
    /// Largest structural frame and decoded-index declared output.
    pub max_output_bytes: usize,
    /// Actual shared application-checkpoint table capacity ceiling.
    pub max_checkpoint_slots: usize,
    /// Actual shared active-partition cache capacity ceiling.
    pub max_cache_slots: usize,
    /// Sum of retained cache-key bytes.
    pub max_cache_name_bytes: usize,
    /// Sum of actual cache partition-vector capacities.
    pub max_cache_partition_slots: usize,
    /// Declared-state ceiling for each composing producer.
    pub max_declared_bytes: usize,
    /// Element-slot ceiling for each composing producer.
    pub max_entries: usize,
    /// Work ceiling for each composing producer.
    pub max_work_bytes: usize,
    /// Native stat/seek/write attempts plus prepaid settlement sync.
    pub max_journal_io_attempts: usize,
    /// Sum of requested native write bytes.
    pub max_journal_write_bytes: usize,
    /// Native write slice ceiling, at most 16KiB.
    pub max_journal_write_chunk_bytes: usize,
    /// `None` requires real live context; `Some` enables real durable recovery.
    pub decompression: Option<OwnedLogDecompressionLimits>,
}
impl OwnedLogApplyLimits {
    pub(super) fn native(self) -> Result<bounded_apply::LogApplyLimits, Failure> {
        let p = self;
        if [
            p.max_input_bytes,
            p.max_records,
            p.max_groups,
            p.max_partitions,
            p.max_appends_per_partition,
            p.max_index_items,
            p.max_output_bytes,
            p.max_checkpoint_slots,
            p.max_cache_slots,
            p.max_cache_name_bytes,
            p.max_cache_partition_slots,
            p.max_declared_bytes,
            p.max_entries,
            p.max_work_bytes,
            p.max_journal_io_attempts,
            p.max_journal_write_bytes,
            p.max_journal_write_chunk_bytes,
        ]
        .into_iter()
        .any(|n| n == 0 || n > isize::MAX as usize)
            || p.max_journal_write_chunk_bytes > 16 * 1024
        {
            return Err(Failure::Limits);
        }
        Ok(bounded_apply::LogApplyLimits {
            publication: LogIndexPublicationLimits {
                max_envelope_bytes: p.max_input_bytes,
                max_context_bytes: p.max_input_bytes,
                max_partitions: p.max_partitions,
                max_appends_per_partition: p.max_appends_per_partition,
                max_declared_bytes: p.max_declared_bytes,
                max_entries: p.max_entries,
                max_work_bytes: p.max_work_bytes,
                frames: IndexedFramePreparationLimits {
                    max_payload_bytes: p.max_input_bytes,
                    max_context_bytes: p.max_input_bytes,
                    max_records: p.max_records,
                    max_groups: p.max_groups,
                    max_structural_bytes_per_group: p.max_output_bytes,
                    max_checksum_bytes: p.max_input_bytes,
                    max_declared_bytes: p.max_declared_bytes,
                    max_entries: p.max_entries,
                    max_work_bytes: p.max_work_bytes,
                    decompression: p.decompression.map(Into::into),
                    index: EmbeddedIndexDecodeLimits {
                        max_input_bytes: p.max_output_bytes,
                        max_records: p.max_records,
                        max_dictionary_entries: p.max_index_items,
                        max_terms: p.max_index_items,
                        max_fields: p.max_index_items,
                        max_locator_ids: p.max_index_items,
                        max_runs: p.max_index_items,
                        max_output_bytes: p.max_output_bytes,
                        max_allocation_slots: p.max_entries,
                        max_work_bytes: p.max_work_bytes,
                    },
                },
            },
            journal: JournalAppendLimits {
                max_appends: 1,
                max_input_slots: 1,
                max_payload_bytes: p.max_input_bytes,
                max_input_bytes: mul(p.max_input_bytes, 2)?,
                max_frame_bytes: add(p.max_input_bytes, 128)?,
                max_io_attempts: p.max_journal_io_attempts,
                max_write_request_bytes: p.max_journal_write_bytes,
                max_write_chunk_bytes: p.max_journal_write_chunk_bytes,
            },
            max_checkpoint_slots: p.max_checkpoint_slots,
            max_cache_slots: p.max_cache_slots,
            max_cache_name_bytes: p.max_cache_name_bytes,
            max_cache_partition_slots: p.max_cache_partition_slots,
            max_declared_bytes: p.max_declared_bytes,
            max_entries: p.max_entries,
            max_work_bytes: p.max_work_bytes,
        })
    }
}

/// Explicit lifetime setup and one-slot worker policy.
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
pub struct OwnedLogFactoryLimits {
    /// Physical workers, limited to 64.
    pub max_shards: usize,
    /// Actual original source-vector capacity ceiling.
    pub max_source_slots: usize,
    /// Exactly one pending command per shard in this producer.
    pub max_pending_commands: usize,
    /// Exact requested native stack bytes per worker.
    pub thread_stack_bytes: usize,
    /// Largest actual ASCII worker name.
    pub max_thread_name_bytes: usize,
    /// Additional setup declared-value ceiling.
    pub max_retained_bytes: usize,
    /// Additional setup work ceiling.
    pub max_work_bytes: usize,
    /// Genuine runtime child ceilings; no runtime work owner is constructed.
    pub apply: OwnedLogApplyLimits,
}
impl OwnedLogFactoryLimits {
    /// Full configured setup declaration for outer pre-admission. Actual setup
    /// additionally spends the same recovery ledger and refuses any excess;
    /// this is no allocator/RSS or native-call-duration promise.
    pub fn setup_cost(&self) -> Result<TelemetryRecoveryUsage, TelemetryRecoveryError> {
        if self.max_shards == 0
            || self.max_shards > 64
            || self.max_source_slots == 0
            || self.max_pending_commands != 1
            || self.thread_stack_bytes == 0
            || self.max_thread_name_bytes == 0
            || self.max_retained_bytes == 0
            || self.max_work_bytes == 0
        {
            return Err(TelemetryRecoveryError::Limits);
        }
        self.apply
            .native()
            .map_err(|_| TelemetryRecoveryError::Limits)?;
        let strings = self
            .max_shards
            .checked_mul(self.max_thread_name_bytes)
            .filter(|n| *n <= self.max_retained_bytes)
            .ok_or(TelemetryRecoveryError::Limits)?;
        Ok(TelemetryRecoveryUsage {
            retained_bytes: self.max_retained_bytes,
            string_bytes: strings,
            work_bytes: self.max_work_bytes,
            ..Default::default()
        })
    }
}
