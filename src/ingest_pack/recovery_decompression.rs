//! Retained structural-frame reconstruction on the actual original setup ledger.
use super::bounded_decompression::{
    FrameWork, PreparedDecompressedFrame, RetainedFrameDecompressionError,
    decompress_frame_with_work,
};
use crate::{
    OwnedLogDecompressionLimits, TelemetryRecoveryBudget, TelemetryRecoveryError,
    TelemetryRecoveryUsage,
};
use bytes::Bytes;
use std::error::Error;
use std::fmt;
use std::time::Instant;

pub(super) struct RecoveryFrameWork {
    budget: TelemetryRecoveryBudget,
}
impl FrameWork for RecoveryFrameWork {
    type Refusal = TelemetryRecoveryError;

    fn check(&mut self) -> Result<(), Self::Refusal> {
        self.budget.check()
    }
    fn pay(&mut self, bytes: usize, _entries: usize, work: usize) -> Result<(), Self::Refusal> {
        // Attempt admits its explicit local decoder-entry ceiling before this
        // call. Resident decoder entries are not JSON records or file reads.
        self.budget.charge(TelemetryRecoveryUsage {
            retained_bytes: bytes,
            work_bytes: work,
            ..Default::default()
        })
    }
    fn original_expiry(&self) -> Instant {
        self.budget.original_expiry()
    }
}

/// A complete structural frame and its actual decoder/setup-work owners.
///
/// This is decoder substrate, not a query, application checkpoint, authenticated
/// owner, serving permission or proof that an entire source scope was visited.
/// No mutable decoder or recovery ledger can be extracted.
pub struct PreparedRecoveredLogFrame {
    inner: PreparedDecompressedFrame<RecoveryFrameWork>,
}
impl PreparedRecoveredLogFrame {
    /// Actual complete decompressed structural bytes, without a cache copy.
    pub fn structural_bytes(&self) -> Option<&[u8]> {
        self.inner.structural().map(Bytes::as_ref)
    }

    /// The same absolute expiry supplied to the original setup ledger.
    pub fn original_expiry(&self) -> Instant {
        self.inner.work().original_expiry()
    }

    /// Cumulative original setup charges, including all earlier frames.
    pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
        self.inner.work().budget.usage()
    }

    /// Actual completed native streaming calls for this frame.
    pub fn stream_calls(&self) -> usize {
        self.inner.stream_calls()
    }

    /// Actual admitted decoder work for this frame, separate from prior setup.
    pub fn decoder_work_bytes(&self) -> usize {
        self.inner.work_bytes()
    }

    /// Release this completed frame and decode another on the SAME spent ledger.
    ///
    /// The continuation consumes the previous successful owner. It never returns
    /// a mutable work/native owner, refreshes expiry or resets cumulative fees.
    /// A failure retains the entered next frame and has no retry continuation.
    #[allow(
        clippy::result_large_err,
        reason = "retain the original ledger, native context and outcome without a refusal allocation"
    )]
    pub fn decompress_next_until(
        self,
        input: Bytes,
        expected_bytes: usize,
        expected_records: u32,
        limits: OwnedLogDecompressionLimits,
    ) -> Result<Self, RetainedRecoveredLogFrameError> {
        finish(
            self.inner
                .next_until(input, expected_bytes, expected_records, limits.into()),
        )
    }
}
impl fmt::Debug for PreparedRecoveredLogFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedRecoveredLogFrame")
            .field("decoder", &self.inner)
            .field("recovery_usage", &self.recovery_usage())
            .finish_non_exhaustive()
    }
}

/// Terminal owner of actual decoder results, output prefix and original ledger.
///
/// There is no retry, native-context extractor or budget-renewal conversion.
pub struct RetainedRecoveredLogFrameError {
    inner: RetainedFrameDecompressionError<RecoveryFrameWork>,
}
impl RetainedRecoveredLogFrameError {
    /// The same saved setup deadline, including after a late native result.
    pub fn original_expiry(&self) -> Instant {
        self.inner.work().original_expiry()
    }

    /// Actual cumulative setup/decoder fees already spent before refusal.
    pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
        self.inner.work().budget.usage()
    }

    /// Actual initialized output prefix retained before any post-entry refusal.
    pub fn output_prefix(&self) -> &[u8] {
        self.inner.output_prefix()
    }

    /// Actual completed streaming calls, including one that returned an error.
    pub fn stream_calls(&self) -> usize {
        self.inner.stream_calls()
    }

    /// Whether the actual native decoder context remains in this owner.
    pub fn has_native_context(&self) -> bool {
        self.inner.has_native_context()
    }

    /// The actual native initialization/parameter/stream error, if observed.
    pub fn native_error(&self) -> Option<usize> {
        self.inner.native_error()
    }

    /// Whether an actual caught decoder/observer unwind remains retained.
    pub fn has_panic(&self) -> bool {
        self.inner.has_panic()
    }
}
impl fmt::Debug for RetainedRecoveredLogFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedRecoveredLogFrameError")
            .field("decoder", &self.inner)
            .field("recovery_usage", &self.recovery_usage())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedRecoveredLogFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for RetainedRecoveredLogFrameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.inner)
    }
}

#[allow(
    clippy::result_large_err,
    reason = "retain the original ledger, native context and outcome without a refusal allocation"
)]
fn finish(
    result: Result<
        PreparedDecompressedFrame<RecoveryFrameWork>,
        RetainedFrameDecompressionError<RecoveryFrameWork>,
    >,
) -> Result<PreparedRecoveredLogFrame, RetainedRecoveredLogFrameError> {
    result
        .map(|inner| PreparedRecoveredLogFrame { inner })
        .map_err(|inner| RetainedRecoveredLogFrameError { inner })
}

/// Reconstruct one real structural frame on its original already-spent setup work.
///
/// Consumes the non-Clone ledger before checking its E or entering native code.
/// The existing finite streaming/window/context and retained-outcome producer
/// performs every codec check and charge. Only documented recovery retained/work
/// axes are charged; local decoder entries remain bounded by `limits`.
///
/// A successful owner can consume itself to decode a subsequent frame with the
/// same cumulative ledger. An error retains that ledger and actual partial
/// native state. This does not decode typed query rows or confer query readiness.
#[allow(
    clippy::result_large_err,
    reason = "retain the original ledger, native context and outcome without a refusal allocation"
)]
pub fn decompress_recovered_log_frame_until(
    input: Bytes,
    expected_bytes: usize,
    expected_records: u32,
    limits: OwnedLogDecompressionLimits,
    original_setup_work: TelemetryRecoveryBudget,
) -> Result<PreparedRecoveredLogFrame, RetainedRecoveredLogFrameError> {
    finish(decompress_frame_with_work(
        input,
        expected_bytes,
        expected_records,
        limits.into(),
        RecoveryFrameWork {
            budget: original_setup_work,
        },
    ))
}
