//! Declared native payload and append materialization policy.

use crate::{NativeQueryWorkLimits, TelemetryError, TelemetryResult};

/// Finite resident trace/metric payload policy for bounded embedded operation.
/// These encoded/materialization ceilings are separate from allocator/RSS,
/// startup metadata, and transport concurrency measurements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeResidentLimits {
    /// Maximum encoded sealed payload retained for one owned signal partition.
    pub max_payload_bytes_per_partition: u64,
    /// Input, decompression, record-count and serialized-record append ceilings.
    /// They are checked before shard-stream mutates its WAL.
    pub append_work: NativeQueryWorkLimits,
}

impl NativeResidentLimits {
    pub(crate) fn clamp_partition_bytes(&mut self, maximum: u64) {
        self.max_payload_bytes_per_partition = self.max_payload_bytes_per_partition.min(maximum);
        self.append_work.max_encoded_block_bytes = self.append_work.max_encoded_block_bytes.min(
            usize::try_from(self.publication_target_bytes())
                .unwrap_or(usize::MAX)
                .max(1),
        );
        self.append_work.max_control_object_bytes = self
            .append_work
            .max_control_object_bytes
            .min(self.append_work.max_encoded_block_bytes);
    }

    pub(crate) fn charge_records<T: serde::Serialize>(
        self,
        records: &[T],
        encode: impl Fn(&T) -> TelemetryResult<Vec<u8>>,
    ) -> TelemetryResult<usize> {
        self.append_work.rows(records.len())?;
        self.append_work.winners(records.len())?;
        let mut budget = crate::query_work::QueryWorkBudget::new(Some(self.append_work));
        let mut encoded = 0_u64;
        for record in records {
            budget.replace(None, record, 0)?;
            // A singleton includes all codec columns and context. Reserve
            // further per-row framing for regrouping and absolute offsets.
            encoded = encoded
                .saturating_add(u64::try_from(encode(record)?.len()).unwrap_or(u64::MAX))
                .saturating_add(1024);
            if encoded
                > self
                    .max_payload_bytes_per_partition
                    .saturating_sub(self.publication_target_bytes())
            {
                return Err(TelemetryError::InvalidConfig(
                    "native append sealed representation exceeds its partition allowance",
                ));
            }
        }
        Ok(budget.used_bytes())
    }
    /// Reserve one quarter of the declared RAM allowance across the sum of
    /// logical trace and metric partitions. Each partition has one engine
    /// owner; physical stripe count does not duplicate this payload allowance.
    #[must_use]
    pub fn for_ram_budget(
        bytes: u64,
        physical_owners: u64,
        trace_partitions: u16,
        metric_partitions: u16,
    ) -> Self {
        let partitions = u64::from(trace_partitions)
            .saturating_add(u64::from(metric_partitions))
            .max(1);
        let group = (bytes / 4 / partitions).max(1);
        let encoded = usize::try_from(group / 4).unwrap_or(usize::MAX).max(1);
        // Reserve framing and repeated JSON context expansion independently
        // of compact serialized records; highly expanded inputs fail admission.
        let decoded = usize::try_from(bytes / 16 / physical_owners.max(1))
            .unwrap_or(usize::MAX)
            .max(1);
        let row = std::mem::size_of::<crate::DurableSpan>()
            .max(std::mem::size_of::<crate::DurableMetricPoint>())
            .max(1);
        let records = (decoded / row).max(1);
        Self {
            max_payload_bytes_per_partition: group,
            append_work: NativeQueryWorkLimits {
                max_encoded_block_bytes: encoded,
                max_decoded_sidecar_bytes: decoded,
                max_decoded_records: records,
                max_winner_records: records,
                max_winner_bytes: decoded,
                max_control_object_bytes: encoded,
            },
        }
    }

    /// Publication threshold before the next native boundary can be admitted.
    #[must_use]
    pub const fn publication_target_bytes(self) -> u64 {
        self.max_payload_bytes_per_partition / 4
    }

    /// Aggregate sealed allowance for the declared uniquely owned partitions.
    #[must_use]
    pub fn reserved_bytes(self, trace_partitions: u16, metric_partitions: u16) -> u64 {
        self.max_payload_bytes_per_partition.saturating_mul(
            u64::from(trace_partitions).saturating_add(u64::from(metric_partitions)),
        )
    }

    /// Global decoded validation-cache allowance, shared by physical owners.
    #[must_use]
    pub fn cache_bytes(self, physical_owners: u64) -> usize {
        self.append_work
            .max_winner_bytes
            .saturating_mul(usize::try_from(physical_owners).unwrap_or(usize::MAX))
    }

    /// Reserve separate preflight, owner decoding and decoded-cache charges.
    #[must_use]
    pub fn materialization_reserve_bytes(self, physical_owners: u64) -> u64 {
        u64::try_from(self.cache_bytes(physical_owners))
            .unwrap_or(u64::MAX)
            .saturating_mul(3)
    }

    pub(crate) fn validate(self) -> TelemetryResult<()> {
        self.append_work.validate()?;
        let row_bytes = std::mem::size_of::<crate::DurableSpan>()
            .max(std::mem::size_of::<crate::DurableMetricPoint>());
        if self.publication_target_bytes() == 0
            || u64::try_from(self.append_work.max_encoded_block_bytes).unwrap_or(u64::MAX)
                > self.publication_target_bytes()
            || self.append_work.max_decoded_sidecar_bytes > self.append_work.max_winner_bytes
            || self
                .append_work
                .max_decoded_records
                .saturating_mul(row_bytes)
                > self.append_work.max_winner_bytes
            || self
                .append_work
                .max_winner_records
                .saturating_mul(row_bytes)
                > self.append_work.max_winner_bytes
        {
            return Err(TelemetryError::InvalidConfig(
                "native resident append ceilings exceed partition or materialization allowance",
            ));
        }
        Ok(())
    }
}
