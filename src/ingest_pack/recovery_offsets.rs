//! Actual bounded logical-offset decoding over complete retained section facts.
use super::bounded_decompression::{FrameContinuation, FrameWork, PreparedDecompressedFrame};
use super::recovery_decompression::RecoveryFrameWork;
use super::recovery_sections::PreparedRecoveredStructuralSections;
use crate::{TelemetryError, TelemetryRecoveryError, TelemetryRecoveryUsage};
use bytes::Bytes;
use shard_stream_core::LogicalOffset;
use std::collections::TryReserveError;
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::ops::Range;
use std::time::Instant;

/// Additional finite offset-lane policy on the same original recovery ledger.
#[derive(Clone, Copy, Debug)]
pub struct RecoveredStructuralOffsetLimits {
    /// Largest actual encoded offset lane.
    pub max_input_bytes: usize,
    /// Largest actual number of offsets.
    pub max_records: usize,
    /// Concrete continuation state and observed offset-vector capacity.
    pub max_declared_bytes: usize,
    /// Scalar-byte, allocation and output-entry admission ceiling.
    pub max_entries: usize,
    /// Additional admitted child work.
    pub max_work_bytes: usize,
}

/// Actual offset-child charges and partial progress, separate from its parent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveredStructuralOffsetUsage {
    /// Concrete continuation state and admitted vector capacity.
    pub declared_bytes: usize,
    /// Actual admitted scalar/allocation/output entries.
    pub entries: usize,
    /// Actual admitted child work.
    pub work_bytes: usize,
    /// Actual inspected encoded scalar bytes.
    pub inspected_bytes: usize,
    /// Actual completed logical-offset rows.
    pub decoded_records: usize,
}

#[derive(Debug)]
enum Failure {
    Limits,
    Recovery(TelemetryRecoveryError),
    Codec(TelemetryError),
    Allocation,
    State,
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for Failure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Recovery(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::Limits | Self::Allocation | Self::State => None,
        }
    }
}
type Result<T> = std::result::Result<T, Failure>;
fn invalid(message: &'static str) -> Failure {
    Failure::Codec(TelemetryError::InvalidBlockEncoding(message))
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .filter(|value| *value <= isize::MAX as usize)
        .ok_or(Failure::Limits)
}
struct Offsets {
    limits: RecoveredStructuralOffsetLimits,
    usage: RecoveredStructuralOffsetUsage,
    range: Option<Range<usize>>,
    record_count: u32,
    cursor: usize,
    offsets: Vec<LogicalOffset>,
    reservation: Option<std::result::Result<(), TryReserveError>>,
}
impl Offsets {
    fn pay(
        &mut self,
        owner: &mut RecoveryFrameWork,
        bytes: usize,
        entries: usize,
        work: usize,
    ) -> Result<()> {
        let declared = add(self.usage.declared_bytes, bytes)?;
        let elements = add(self.usage.entries, entries)?;
        let effort = add(self.usage.work_bytes, work)?;
        if declared > self.limits.max_declared_bytes
            || elements > self.limits.max_entries
            || effort > self.limits.max_work_bytes
        {
            return Err(Failure::Limits);
        }
        owner.pay(bytes, entries, work).map_err(Failure::Recovery)?;
        self.usage.declared_bytes = declared;
        self.usage.entries = elements;
        self.usage.work_bytes = effort;
        Ok(())
    }
    fn scalar(&mut self, input: &[u8], owner: &mut RecoveryFrameWork) -> Result<u64> {
        let mut value = 0u64;
        for index in 0..10 {
            self.pay(owner, 0, 1, 1)?;
            let byte = input
                .get(self.cursor)
                .copied()
                .ok_or_else(|| invalid("truncated offset varint"))?;
            self.cursor = add(self.cursor, 1)?;
            self.usage.inspected_bytes = add(self.usage.inspected_bytes, 1)?;
            let payload = u64::from(byte & 0x7f);
            if index == 9 && payload > 1 {
                return Err(invalid("offset varint overflow"));
            }
            value |= payload << (index * 7);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(invalid("offset varint is too long"))
    }
    fn observed(&self, owner: &RecoveryFrameWork, phase: &'static str) {
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        super::bounded_frames::observe_decompression(owner.original_expiry(), phase);
        #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
        let _ = (owner, phase);
    }
    fn run(
        &mut self,
        structural: &Bytes,
        owner: &mut RecoveryFrameWork,
        fixed: Option<usize>,
    ) -> Result<()> {
        let input = structural
            .get(self.range.clone().ok_or(Failure::State)?)
            .ok_or(Failure::State)?;
        let count = usize::try_from(self.record_count).map_err(|_| Failure::Limits)?;
        if [
            self.limits.max_input_bytes,
            self.limits.max_records,
            self.limits.max_declared_bytes,
            self.limits.max_entries,
            self.limits.max_work_bytes,
        ]
        .into_iter()
        .any(|limit| limit == 0 || limit > isize::MAX as usize)
            || input.len() > self.limits.max_input_bytes
            || count > self.limits.max_records
        {
            return Err(Failure::Limits);
        }
        let fixed = fixed.ok_or(Failure::Limits)?;
        self.pay(owner, fixed, 1, fixed)?;
        if count == 0 {
            return if input.is_empty() {
                Ok(())
            } else {
                Err(invalid("offsets for empty block"))
            };
        }
        if count > input.len() {
            return Err(invalid("offset count exceeds encoded lane"));
        }
        let capacity = count
            .checked_mul(size_of::<LogicalOffset>())
            .filter(|bytes| *bytes <= isize::MAX as usize)
            .ok_or(Failure::Limits)?;
        self.pay(owner, capacity, 1, capacity)?;
        self.reservation = Some(self.offsets.try_reserve_exact(count));
        // Preserve the actual allocator Result before any observer/E check.
        self.observed(owner, "offset-allocation");
        if self
            .reservation
            .as_ref()
            .is_some_and(|result| result.is_err())
        {
            return Err(Failure::Allocation);
        }
        let actual_capacity = self
            .offsets
            .capacity()
            .checked_mul(size_of::<LogicalOffset>())
            .filter(|bytes| *bytes <= isize::MAX as usize)
            .ok_or(Failure::Limits)?;
        self.pay(
            owner,
            actual_capacity
                .checked_sub(capacity)
                .ok_or(Failure::State)?,
            0,
            0,
        )?;
        let mut previous = 0u64;
        for ordinal in 0..count {
            let scalar = self.scalar(input, owner)?;
            let value = if ordinal == 0 {
                scalar
            } else {
                previous
                    .checked_add(scalar)
                    .ok_or_else(|| invalid("offset delta overflow"))?
            };
            self.pay(owner, 0, 1, size_of::<LogicalOffset>())?;
            self.offsets.push(LogicalOffset::new(value));
            previous = value;
            self.usage.decoded_records = add(self.usage.decoded_records, 1)?;
            self.observed(owner, "offset-row");
        }
        if self.cursor != input.len() {
            return Err(invalid("trailing offset bytes"));
        }
        Ok(())
    }
}
type Continued = FrameContinuation<
    PreparedRecoveredStructuralSections,
    Offsets,
    Failure,
    TelemetryRecoveryError,
>;

/// Actual decoded logical offsets and the complete successful section parent.
///
/// Pco timestamps, bodies, fields and typed metadata remain encoded. These
/// offsets do not constitute reconstructed log rows or query/HA ownership.
pub struct PreparedRecoveredStructuralOffsets {
    held: Continued,
}
impl PreparedRecoveredStructuralOffsets {
    /// Every actual logical offset in encoded row order, including equal values.
    pub fn offsets(&self) -> &[LogicalOffset] {
        &self.held.child.offsets
    }
    /// Borrow the complete successful section owner, without extracting work.
    pub fn parent_sections(&self) -> &PreparedRecoveredStructuralSections {
        &self.held.parent
    }
    /// Actual additional offset-child charges.
    pub fn usage(&self) -> RecoveredStructuralOffsetUsage {
        self.held.child.usage
    }
    /// Original setup deadline, unchanged through both consuming children.
    pub fn original_expiry(&self) -> Instant {
        self.held.parent.original_expiry()
    }
    /// Cumulative original setup/native/section/offset charges.
    pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
        self.held.parent.recovery_usage()
    }
    /// Consume completed offsets/sections before the next SAME-ledger frame.
    #[allow(
        clippy::result_large_err,
        reason = "retain original work and actual next-frame outcomes inline"
    )]
    pub fn decompress_next_until(
        self,
        input: Bytes,
        expected_bytes: usize,
        expected_records: u32,
        limits: crate::OwnedLogDecompressionLimits,
    ) -> std::result::Result<crate::PreparedRecoveredLogFrame, crate::RetainedRecoveredLogFrameError>
    {
        let FrameContinuation {
            child,
            initial,
            actual,
            post,
            panic,
            parent,
            ..
        } = self.held;
        drop((child, initial, actual, post, panic));
        parent.decompress_next_until(input, expected_bytes, expected_records, limits)
    }
}
impl fmt::Debug for PreparedRecoveredStructuralOffsets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedRecoveredStructuralOffsets")
            .field("usage", &self.usage())
            .finish_non_exhaustive()
    }
}

/// Terminal complete sections/native parent and actual partial offset outcome.
/// No retry, mutable work or native-owner extractor is provided.
pub struct RetainedRecoveredStructuralOffsetError {
    held: Continued,
}
impl RetainedRecoveredStructuralOffsetError {
    /// Actual captured complete child Result, separate from late E/unwind.
    pub fn child_succeeded(&self) -> bool {
        self.held
            .actual
            .as_ref()
            .is_some_and(|result| result.is_ok())
    }
    /// Whether actual child entry followed a successful original-work check.
    pub fn child_entered(&self) -> bool {
        self.held.entered
    }
    /// Whether an actual caught unwind remains retained.
    pub fn has_panic(&self) -> bool {
        self.held.panic.is_some()
    }
    /// Actual allocator call was entered and its Result captured.
    pub fn allocation_entered(&self) -> bool {
        self.held.child.reservation.is_some()
    }
    /// Actual retained allocator Result succeeded before any later refusal.
    pub fn allocation_succeeded(&self) -> bool {
        self.held
            .child
            .reservation
            .as_ref()
            .is_some_and(|result| result.is_ok())
    }
    /// Actual completed row prefix, retained on malformed input or refusal.
    pub fn offsets(&self) -> &[LogicalOffset] {
        &self.held.child.offsets
    }
    /// Complete successful sections and outer native decoder remain owned.
    pub fn parent_sections(&self) -> &PreparedRecoveredStructuralSections {
        &self.held.parent
    }
    /// Actual child charges and partial progress.
    pub fn usage(&self) -> RecoveredStructuralOffsetUsage {
        self.held.child.usage
    }
    /// Same original setup deadline with no continuation or renewal path.
    pub fn original_expiry(&self) -> Instant {
        self.held.parent.original_expiry()
    }
    /// Cumulative original work including this actual partial child.
    pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
        self.held.parent.recovery_usage()
    }
    /// Actual post-work check, separate from the saved child/allocator Results.
    pub fn post_error(&self) -> Option<&TelemetryRecoveryError> {
        self.held.post.as_ref()?.as_ref().err()
    }
}
impl fmt::Debug for RetainedRecoveredStructuralOffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedRecoveredStructuralOffsetError")
            .field("usage", &self.usage())
            .field("child_entered", &self.child_entered())
            .field("child_succeeded", &self.child_succeeded())
            .field("panic", &self.has_panic())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedRecoveredStructuralOffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for RetainedRecoveredStructuralOffsetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(Err(Failure::Allocation)) = &self.held.actual {
            return self
                .held
                .child
                .reservation
                .as_ref()?
                .as_ref()
                .err()
                .map(|error| error as &dyn Error);
        }
        if let Some(Err(error)) = &self.held.actual {
            return Some(error);
        }
        if let Some(Err(error)) = &self.held.initial {
            return Some(error);
        }
        self.post_error().map(|error| error as &dyn Error)
    }
}

#[allow(
    clippy::result_large_err,
    reason = "retain complete parent, actual child/allocator Results and original work inline"
)]
pub(super) fn prepare_offsets_until(
    parent: PreparedRecoveredStructuralSections,
    range: Option<Range<usize>>,
    limits: RecoveredStructuralOffsetLimits,
) -> std::result::Result<PreparedRecoveredStructuralOffsets, RetainedRecoveredStructuralOffsetError>
{
    let child = Offsets {
        limits,
        usage: RecoveredStructuralOffsetUsage::default(),
        range,
        record_count: parent.record_count(),
        cursor: 0,
        offsets: Vec::new(),
        reservation: None,
    };
    let held = PreparedDecompressedFrame::<RecoveryFrameWork>::continue_owned_until(
        parent,
        child,
        |input, work, child, fixed| child.run(input, work, fixed),
    );
    if held.accepted() {
        Ok(PreparedRecoveredStructuralOffsets { held })
    } else {
        Err(RetainedRecoveredStructuralOffsetError { held })
    }
}
