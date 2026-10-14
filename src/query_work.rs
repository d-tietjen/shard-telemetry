//! Declared working-set ceilings for native queries, independent of corpus size.

use crate::{TelemetryError, TelemetryResult};

/// Work limits checked before native payload reads, decoding and winner growth.
///
/// These bound the declared inputs and row collections of each owner query.
/// They are not a quota for allocator overhead or whole-process resident memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeQueryWorkLimits {
    /// Maximum encoded bytes read for one signal block.
    pub max_encoded_block_bytes: usize,
    /// Maximum decompressed bytes per sidecar or variable metric-value lane.
    pub max_decoded_sidecar_bytes: usize,
    /// Maximum rows decoded from one signal block.
    pub max_decoded_records: usize,
    /// Maximum distinct logical winners retained by one owner query.
    pub max_winner_records: usize,
    /// Maximum cumulative serialized winner bytes plus row/key headers.
    /// This deliberately charges shared payloads again; it is a conservative
    /// materialization proxy, not an allocator measurement.
    pub max_winner_bytes: usize,
    /// Maximum encoded catalog-page or manifest bytes read by one query.
    pub max_control_object_bytes: usize,
}

impl NativeQueryWorkLimits {
    pub(crate) fn for_ram_budget(bytes: u64, stripes: u64) -> Self {
        let fragment = usize::try_from(bytes / 16 / stripes.max(1))
            .unwrap_or(usize::MAX)
            .max(1);
        let row_bytes = std::mem::size_of::<crate::DurableSpan>()
            .max(std::mem::size_of::<crate::DurableMetricPoint>());
        let rows = (fragment / row_bytes.max(1)).max(1);
        Self {
            max_encoded_block_bytes: fragment,
            max_decoded_sidecar_bytes: fragment,
            max_decoded_records: rows,
            max_winner_records: rows,
            max_winner_bytes: fragment,
            max_control_object_bytes: fragment,
        }
    }

    pub(crate) fn validate(self) -> TelemetryResult<()> {
        if [
            self.max_encoded_block_bytes,
            self.max_decoded_sidecar_bytes,
            self.max_decoded_records,
            self.max_winner_records,
            self.max_winner_bytes,
            self.max_control_object_bytes,
        ]
        .contains(&0)
        {
            return Err(TelemetryError::InvalidConfig(
                "native query work ceilings must be nonzero",
            ));
        }
        Ok(())
    }

    pub(crate) fn encoded(self, bytes: usize) -> TelemetryResult<()> {
        Self::check("encoded block bytes", bytes, self.max_encoded_block_bytes)
    }

    pub(crate) fn rows(self, rows: usize) -> TelemetryResult<()> {
        Self::check("decoded block rows", rows, self.max_decoded_records)
    }

    pub(crate) fn winners(self, rows: usize) -> TelemetryResult<()> {
        Self::check("distinct winner rows", rows, self.max_winner_records)
    }

    pub(crate) fn control(self, bytes: u64) -> TelemetryResult<()> {
        Self::check(
            "control object bytes",
            usize::try_from(bytes).unwrap_or(usize::MAX),
            self.max_control_object_bytes,
        )
    }

    pub(crate) fn sidecar(self, encoded: &[u8], format_limit: usize) -> TelemetryResult<usize> {
        let bytes = zstd::zstd_safe::get_frame_content_size(encoded)
            .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid native sidecar frame"))?
            .ok_or_else(|| {
                TelemetryError::InvalidQuery(
                    "native query capacity cannot verify sidecar frame content size".into(),
                )
            })?;
        Self::check(
            "decoded sidecar bytes",
            usize::try_from(bytes).unwrap_or(usize::MAX),
            self.max_decoded_sidecar_bytes,
        )?;
        Ok(self.max_decoded_sidecar_bytes.min(format_limit))
    }

    pub(crate) fn metric_values(self, bytes: usize) -> TelemetryResult<()> {
        Self::check(
            "decoded metric value bytes",
            bytes,
            self.max_decoded_sidecar_bytes,
        )
    }

    fn check(resource: &str, actual: usize, maximum: usize) -> TelemetryResult<()> {
        if actual > maximum {
            return Err(TelemetryError::InvalidQuery(format!(
                "native query capacity exceeded for {resource}: {actual} > {maximum}"
            )));
        }
        Ok(())
    }
}

/// Tracks a conservative serialized payload/header charge before retaining a winner.
pub(crate) struct QueryWorkBudget {
    limits: Option<NativeQueryWorkLimits>,
    bytes: usize,
}

impl QueryWorkBudget {
    pub(crate) fn new(limits: Option<NativeQueryWorkLimits>) -> Self {
        Self { limits, bytes: 0 }
    }

    pub(crate) fn replace<T: serde::Serialize>(
        &mut self,
        previous: Option<&T>,
        incoming: &T,
        key_bytes: usize,
    ) -> TelemetryResult<()> {
        let Some(limits) = self.limits else {
            return Ok(());
        };
        let charge = |value: &T| -> TelemetryResult<usize> {
            let mut writer = WorkCounter {
                bytes: 0,
                maximum: limits.max_winner_bytes,
            };
            rmp_serde::encode::write(&mut writer, value).map_err(|error| {
                TelemetryError::InvalidQuery(format!(
                    "native query capacity exceeded while charging winner: {error}"
                ))
            })?;
            Ok(writer
                .bytes
                .saturating_add(std::mem::size_of::<T>())
                .saturating_add(key_bytes))
        };
        let old = match previous {
            Some(previous) => charge(previous)?,
            None => 0,
        };
        let next = self
            .bytes
            .saturating_sub(old)
            .saturating_add(charge(incoming)?);
        NativeQueryWorkLimits::check(
            "serialized winner/header bytes",
            next,
            limits.max_winner_bytes,
        )?;
        self.bytes = next;
        Ok(())
    }
}

struct WorkCounter {
    bytes: usize,
    maximum: usize,
}

impl std::io::Write for WorkCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("native winner charge overflow"))?;
        if next > self.maximum {
            return Err(std::io::Error::other(
                "native query serialized winner capacity exceeded",
            ));
        }
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
