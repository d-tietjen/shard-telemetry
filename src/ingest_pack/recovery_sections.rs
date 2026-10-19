//! Charged section facts over a retained complete structural-frame decoder.
use super::bounded_decompression::{FrameContinuation, FrameWork, PreparedDecompressedFrame};
use super::recovery_decompression::RecoveryFrameWork;
use crate::structural::STRUCTURAL_BLOCK_MAGIC;
use crate::{TelemetryError, TelemetryRecoveryError, TelemetryRecoveryUsage};
use bytes::Bytes;
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::ops::Range;
use std::time::Instant;

/// One actual section in the existing structural wire layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveredStructuralSection {
    /// Delta-coded logical offsets.
    Offsets,
    /// Pco-encoded timestamps.
    Timestamps,
    /// Structural message templates.
    Templates,
    /// Seekable message-body lane.
    Bodies,
    /// Attribute key/value dictionaries.
    Attributes,
    /// Seekable field lane.
    Fields,
    /// Compressed exact typed metadata.
    TypedMetadata,
    /// Embedded compressed-domain index.
    EmbeddedIndex,
}
impl RecoveredStructuralSection {
    fn index(self) -> usize {
        match self {
            Self::Offsets => 0,
            Self::Timestamps => 1,
            Self::Templates => 2,
            Self::Bodies => 3,
            Self::Attributes => 4,
            Self::Fields => 5,
            Self::TypedMetadata => 6,
            Self::EmbeddedIndex => 7,
        }
    }
}

/// Additional finite section-envelope bounds on the original setup work.
#[derive(Clone, Copy, Debug)]
pub struct RecoveredStructuralSectionLimits {
    /// Largest complete decompressed structural input.
    pub max_input_bytes: usize,
    /// Largest actual declared record count.
    pub max_records: usize,
    /// Largest individual section extent; no section payload is decoded here.
    pub max_section_bytes: usize,
    /// Declared continuation/child storage, additional to cumulative setup work.
    pub max_declared_bytes: usize,
    /// Header scalar-byte and section-entry ceiling for this child.
    pub max_entries: usize,
    /// Actual child admission work, additional to cumulative setup work.
    pub max_work_bytes: usize,
}

/// Actual section-child charges, separate from the already-spent parent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveredStructuralSectionUsage {
    /// Declared continuation and child state.
    pub declared_bytes: usize,
    /// Admitted scalar-byte/section entries.
    pub entries: usize,
    /// Admitted child work.
    pub work_bytes: usize,
    /// Actual inspected varint bytes.
    pub header_bytes: usize,
    /// Fully captured section extents.
    pub sections: usize,
}
#[derive(Debug)]
enum Failure {
    Limits,
    Recovery(TelemetryRecoveryError),
    Codec(TelemetryError),
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
            Self::Limits | Self::State => None,
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
struct Sections {
    limits: RecoveredStructuralSectionLimits,
    usage: RecoveredStructuralSectionUsage,
    cursor: usize,
    record_count: u32,
    expected_records: u32,
    expected_index: Option<Range<usize>>,
    ranges: [Option<Range<usize>>; 8],
}
impl Sections {
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

    fn byte(&mut self, input: &[u8], owner: &mut RecoveryFrameWork) -> Result<u8> {
        self.pay(owner, 0, 1, 1)?;
        let value = input
            .get(self.cursor)
            .copied()
            .ok_or_else(|| invalid("truncated structural header scalar"))?;
        self.cursor = add(self.cursor, 1)?;
        self.usage.header_bytes = add(self.usage.header_bytes, 1)?;
        Ok(value)
    }

    fn scalar(&mut self, input: &[u8], owner: &mut RecoveryFrameWork) -> Result<usize> {
        let mut value = 0u64;
        for index in 0..10 {
            let byte = self.byte(input, owner)?;
            let payload = u64::from(byte & 0x7f);
            if index == 9 && payload > 1 {
                return Err(invalid("structural header varint overflow"));
            }
            value |= payload << (index * 7);
            if byte & 0x80 == 0 {
                return usize::try_from(value)
                    .map_err(|_| invalid("structural length does not fit usize"));
            }
        }
        Err(invalid("structural header varint is too long"))
    }

    fn observed(&self, owner: &RecoveryFrameWork, phase: &'static str) {
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        super::bounded_frames::observe_decompression(owner.original_expiry(), phase);
        #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
        let _ = (owner, phase);
    }

    fn run(
        &mut self,
        input: &Bytes,
        owner: &mut RecoveryFrameWork,
        fixed: Option<usize>,
    ) -> Result<()> {
        let limits = self.limits;
        if [
            limits.max_input_bytes,
            limits.max_records,
            limits.max_section_bytes,
            limits.max_declared_bytes,
            limits.max_entries,
            limits.max_work_bytes,
        ]
        .into_iter()
        .any(|limit| limit == 0 || limit > isize::MAX as usize)
            || input.len() > limits.max_input_bytes
        {
            return Err(Failure::Limits);
        }
        self.pay(
            owner,
            fixed.ok_or(Failure::Limits)?,
            1,
            fixed.ok_or(Failure::Limits)?,
        )?;
        self.pay(owner, 0, 1, 2 * STRUCTURAL_BLOCK_MAGIC.len())?;
        if input.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
            return Err(invalid("missing structural block magic"));
        }
        self.cursor = STRUCTURAL_BLOCK_MAGIC.len();
        let count = self.scalar(input, owner)?;
        self.record_count = u32::try_from(count)
            .map_err(|_| invalid("structural record count does not fit u32"))?;
        self.observed(owner, "structural-record-count");
        if self.record_count != self.expected_records {
            return Err(invalid("structural count differs from retained decoder"));
        }
        if count > limits.max_records {
            return Err(Failure::Limits);
        }
        if count > input.len().saturating_sub(self.cursor) {
            return Err(invalid("structural record count exceeds remaining bytes"));
        }
        for index in 0..8 {
            let bytes = self.scalar(input, owner)?;
            if bytes > limits.max_section_bytes {
                return Err(Failure::Limits);
            }
            self.pay(owner, 0, 1, 2 * size_of::<Range<usize>>())?;
            let end = self
                .cursor
                .checked_add(bytes)
                .ok_or_else(|| invalid("structural section extent overflow"))?;
            if end > input.len() {
                return Err(invalid("truncated structural section"));
            }
            self.ranges[index] = Some(self.cursor..end);
            self.cursor = end;
            self.usage.sections = add(self.usage.sections, 1)?;
            self.observed(owner, "structural-section");
        }
        if self.cursor != input.len() {
            return Err(invalid("trailing structural bytes"));
        }
        if self.ranges[7] != self.expected_index {
            return Err(Failure::State);
        }
        Ok(())
    }
}
type Continued = FrameContinuation<RecoveryFrameWork, Sections, Failure>;

fn section(held: &Continued, kind: RecoveredStructuralSection) -> Option<&[u8]> {
    held.parent
        .structural()?
        .get(held.child.ranges[kind.index()].as_ref()?.clone())
}

/// Actual charged section views and their complete outer decoder/work owner.
///
/// Section payloads remain encoded. These facts do not constitute decoded typed
/// rows, a complete owner query, application progress or serving/HA permission.
pub struct PreparedRecoveredStructuralSections {
    held: Continued,
}
impl PreparedRecoveredStructuralSections {
    /// Actual record count, checked against the original decoder's count.
    pub fn record_count(&self) -> u32 {
        self.held.child.record_count
    }
    /// Borrow one actual encoded section without copying or decoding its payload.
    pub fn section_bytes(&self, kind: RecoveredStructuralSection) -> Option<&[u8]> {
        section(&self.held, kind)
    }
    /// The exact original complete structural bytes, retained with all views.
    pub fn structural_bytes(&self) -> Option<&[u8]> {
        self.held.parent.structural().map(Bytes::as_ref)
    }
    /// The unchanged original setup deadline.
    pub fn original_expiry(&self) -> Instant {
        self.held.parent.work().original_expiry()
    }
    /// Cumulative original setup/decoder/section charges.
    pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
        self.held.parent.work().usage()
    }
    /// Actual additional section-child charges and progress.
    pub fn usage(&self) -> RecoveredStructuralSectionUsage {
        self.held.child.usage
    }
    /// Consume completed section facts and decode the next frame on SAME work/E.
    ///
    /// Completed child/results are released before the previous native parent;
    /// no ledger or mutable native owner is returned to the caller.
    #[allow(
        clippy::result_large_err,
        reason = "retain original work and actual next-frame outcomes without refusal allocation"
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
        super::recovery_decompression::finish(parent.next_until(
            input,
            expected_bytes,
            expected_records,
            limits.into(),
        ))
    }

    /// Whether the full original native decoder context remains owned.
    pub fn parent_has_native_context(&self) -> bool {
        self.held.parent.has_native_context()
    }
    /// Actual completed streaming calls of the original decoder.
    pub fn parent_stream_calls(&self) -> usize {
        self.held.parent.stream_calls()
    }
}
impl fmt::Debug for PreparedRecoveredStructuralSections {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedRecoveredStructuralSections")
            .field("record_count", &self.record_count())
            .field("usage", &self.usage())
            .finish_non_exhaustive()
    }
}

/// Terminal full parent plus actual partial/completed section-child outcome.
///
/// No retry, mutable ledger or native/context extractor is provided.
pub struct RetainedRecoveredStructuralSectionError {
    held: Continued,
}
impl RetainedRecoveredStructuralSectionError {
    /// Actual captured child Result was successful, even if a later check failed.
    pub fn child_succeeded(&self) -> bool {
        self.held
            .actual
            .as_ref()
            .is_some_and(|result| result.is_ok())
    }
    /// Whether the child producer was entered after the original work check.
    pub fn child_entered(&self) -> bool {
        self.held.entered
    }
    /// Whether an actual caught child/observer unwind remains owned.
    pub fn has_panic(&self) -> bool {
        self.held.panic.is_some()
    }
    /// Captured completed section facts, even after later refusal.
    pub fn section_bytes(&self, kind: RecoveredStructuralSection) -> Option<&[u8]> {
        section(&self.held, kind)
    }
    /// Complete original parent output; no failed child discards the decoder.
    pub fn structural_bytes(&self) -> Option<&[u8]> {
        self.held.parent.structural().map(Bytes::as_ref)
    }
    /// Same original expiry, without a continuation or renewal path.
    pub fn original_expiry(&self) -> Instant {
        self.held.parent.work().original_expiry()
    }
    /// Actual cumulative original ledger fees, including the completed parent.
    pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
        self.held.parent.work().usage()
    }
    /// Actual child admission and partial section progress.
    pub fn usage(&self) -> RecoveredStructuralSectionUsage {
        self.held.child.usage
    }
    /// Whether the complete original native context remains owned.
    pub fn parent_has_native_context(&self) -> bool {
        self.held.parent.has_native_context()
    }
    /// Actual streaming calls of the completed original decoder.
    pub fn parent_stream_calls(&self) -> usize {
        self.held.parent.stream_calls()
    }
    /// Actual original-work postcheck, separate from the retained child Result.
    pub fn post_error(&self) -> Option<&TelemetryRecoveryError> {
        self.held.post.as_ref()?.as_ref().err()
    }
}
impl fmt::Debug for RetainedRecoveredStructuralSectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedRecoveredStructuralSectionError")
            .field("child_entered", &self.child_entered())
            .field("child_succeeded", &self.child_succeeded())
            .field("usage", &self.usage())
            .field("panic", &self.has_panic())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedRecoveredStructuralSectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for RetainedRecoveredStructuralSectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
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
    reason = "retain the complete parent, actual child Result and original ledger without refusal allocation"
)]
pub(super) fn prepare_sections_until(
    parent: PreparedDecompressedFrame<RecoveryFrameWork>,
    limits: RecoveredStructuralSectionLimits,
) -> std::result::Result<PreparedRecoveredStructuralSections, RetainedRecoveredStructuralSectionError>
{
    let child = Sections {
        limits,
        usage: RecoveredStructuralSectionUsage::default(),
        cursor: 0,
        record_count: 0,
        expected_records: parent.expected_records(),
        expected_index: parent.index_range(),
        ranges: std::array::from_fn(|_| None),
    };
    let held = parent.continue_until(child, |input, work, child, fixed| {
        child.run(input, work, fixed)
    });
    if held.accepted() {
        Ok(PreparedRecoveredStructuralSections { held })
    } else {
        Err(RetainedRecoveredStructuralSectionError { held })
    }
}
