//! Actual bounded embedded-index decoding on the complete recovery/Pco parent.
use super::bounded_decompression::{
    FrameContinuation, FrameParent, FrameWork, PreparedDecompressedFrame,
};
use super::recovery_decompression::RecoveryFrameWork;
use super::recovery_sections::RecoveredStructuralSection;
use super::recovery_timestamps::PreparedRecoveredStructuralTimestamps;
use crate::structural::{EmbeddedIndexDecodeLimits, EmbeddedIndexFailure, EmbeddedIndexState};
use crate::{EmbeddedFrameIndex, TelemetryError, TelemetryRecoveryError, TelemetryRecoveryUsage};
use bytes::Bytes;
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::ops::Range;
use std::time::Instant;

/// Finite parser ceilings on the original setup ledger; no new work owner.
/// Fixed continuation/parser framing is charged separately to the same ledger.
#[derive(Clone, Copy, Debug)]
pub struct RecoveredStructuralIndexLimits {
    /// Largest original encoded embedded-index section.
    pub max_input_bytes: usize,
    /// Largest authoritative outer structural record count.
    pub max_records: usize,
    /// Sum of layout/field-set dictionary entries per parser pass.
    pub max_dictionary_entries: usize,
    /// Largest actual term-locator count.
    pub max_terms: usize,
    /// Largest actual field-locator count.
    pub max_fields: usize,
    /// Sum of all locator IDs per parser pass.
    pub max_locator_ids: usize,
    /// Sum of both columns' declared runs per parser pass.
    pub max_runs: usize,
    /// Cumulative actual output requests, including observed capacity excess.
    pub max_output_bytes: usize,
    /// Cumulative actual output slots, including observed capacity excess.
    pub max_allocation_slots: usize,
    /// Both parser passes, copies, comparisons and bit operations.
    pub max_work_bytes: usize,
}
impl From<RecoveredStructuralIndexLimits> for EmbeddedIndexDecodeLimits {
    fn from(value: RecoveredStructuralIndexLimits) -> Self {
        Self {
            max_input_bytes: value.max_input_bytes,
            max_records: value.max_records,
            max_dictionary_entries: value.max_dictionary_entries,
            max_terms: value.max_terms,
            max_fields: value.max_fields,
            max_locator_ids: value.max_locator_ids,
            max_runs: value.max_runs,
            max_output_bytes: value.max_output_bytes,
            max_allocation_slots: value.max_allocation_slots,
            max_work_bytes: value.max_work_bytes,
        }
    }
}
/// Actual local output fees and progress; cumulative original fees remain separate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveredStructuralIndexUsage {
    /// Admitted continuation and Bytes-sharing metadata, outside parser output fees.
    pub continuation_bytes: usize,
    /// Actual cumulative parser output allocation requests, including capacity excess.
    pub output_bytes: usize,
    /// Actual cumulative parser allocation slots.
    pub allocation_slots: usize,
    /// Actual cumulative explicit parser work; no JSON/record entry surcharge.
    pub work_bytes: usize,
    /// Actual parser cursor, reset between its census and output passes.
    pub cursor: usize,
    /// Actual currently retained packed-column bytes; completed columns move to index.
    pub partial_column_bytes: usize,
    /// Actual currently retained uncommitted locator IDs.
    pub partial_locator_ids: usize,
    /// Actual retained incomplete-parser term locators.
    pub partial_terms: usize,
    /// Actual retained incomplete-parser field locators.
    pub partial_fields: usize,
    /// Actual retained incomplete-parser membership filters.
    pub partial_memberships: usize,
}
#[derive(Debug)]
enum Failure {
    Index(EmbeddedIndexFailure<TelemetryRecoveryError>),
    Recovery(TelemetryRecoveryError),
    Codec(TelemetryError),
    Limits,
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for Failure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Index(EmbeddedIndexFailure::Refused(error)) | Self::Recovery(error) => {
                Some(error)
            }
            Self::Index(EmbeddedIndexFailure::Codec(error)) | Self::Codec(error) => Some(error),
            Self::Index(EmbeddedIndexFailure::Allocation(error)) => Some(error),
            _ => None,
        }
    }
}
struct Index {
    range: Option<Range<usize>>,
    record_count: u32,
    limits: RecoveredStructuralIndexLimits,
    continuation_bytes: usize,
    state: Option<EmbeddedIndexState>,
}
impl Index {
    fn run(
        &mut self,
        structural: &Bytes,
        owner: &mut RecoveryFrameWork,
        fixed: Option<usize>,
    ) -> Result<(), Failure> {
        let range = self
            .range
            .clone()
            .filter(|range| range.start <= range.end && range.end <= structural.len())
            .ok_or(Failure::Codec(TelemetryError::InvalidBlockEncoding(
                "missing embedded index section",
            )))?;
        // A first Bytes slice may promote its Vec owner to shared metadata.
        // Prepay that possible allocation before constructing the retained view.
        let bytes = fixed
            .and_then(|fixed| fixed.checked_add(3 * size_of::<usize>()))
            .filter(|bytes| *bytes <= isize::MAX as usize)
            .ok_or(Failure::Limits)?;
        owner.pay(bytes, 1, bytes).map_err(Failure::Recovery)?;
        self.continuation_bytes = bytes;
        self.state = Some(EmbeddedIndexState::new(
            structural.slice(range),
            self.record_count,
            self.limits.into(),
        ));
        self.state
            .as_mut()
            .ok_or(Failure::Limits)?
            .run(owner, size_of::<EmbeddedIndexState>())
            .map_err(Failure::Index)
    }
    fn usage(&self) -> RecoveredStructuralIndexUsage {
        let mut usage = RecoveredStructuralIndexUsage {
            continuation_bytes: self.continuation_bytes,
            ..Default::default()
        };
        if let Some(state) = &self.state {
            (usage.output_bytes, usage.allocation_slots, usage.work_bytes) = state.usage();
            usage.cursor = state.cursor();
            (
                usage.partial_column_bytes,
                usage.partial_locator_ids,
                usage.partial_terms,
                usage.partial_fields,
                usage.partial_memberships,
            ) = state.partial_counts();
        }
        usage
    }
}
type Continued = FrameContinuation<
    PreparedRecoveredStructuralTimestamps,
    Index,
    Failure,
    TelemetryRecoveryError,
>;
/// Completed index with the actual complete Pco/offset/section/native parent.
/// Bodies/fields/typed metadata remain encoded; this is no query permission.
pub struct PreparedRecoveredStructuralIndex {
    held: Continued,
}
impl FrameParent<RecoveryFrameWork> for PreparedRecoveredStructuralIndex {
    fn with_frame<T>(
        &mut self,
        operation: impl FnOnce(&mut PreparedDecompressedFrame<RecoveryFrameWork>) -> T,
    ) -> T {
        self.held.parent.with_frame(operation)
    }
}
/// Terminal index refusal retaining all real partial output and original owners.
pub struct RetainedRecoveredStructuralIndexError {
    held: Continued,
}
macro_rules! views {
    () => {
        /// Actual completed index, including one produced before a late refusal.
        /// Presence alone does not mean the child result/postcheck was accepted.
        pub fn index(&self) -> Option<&EmbeddedFrameIndex> {
            self.held.child.state.as_ref()?.index()
        }
        /// Original exact encoded index view; no mutable source/owner is returned.
        pub fn retained_input(&self) -> Option<&[u8]> {
            Some(self.held.child.state.as_ref()?.encoded())
        }
        /// Original index range inside the complete retained structural bytes.
        pub fn input_range(&self) -> Option<Range<usize>> {
            self.held.child.range.clone()
        }
        /// Actual local parser requests/progress, separate from original cumulative fees.
        pub fn usage(&self) -> RecoveredStructuralIndexUsage {
            self.held.child.usage()
        }
        /// Complete actual Pco/offset/section/native parent, immutably borrowed.
        pub fn parent_timestamps(&self) -> &PreparedRecoveredStructuralTimestamps {
            &self.held.parent
        }
        /// Exact original setup expiry, never extended by the index child.
        pub fn original_expiry(&self) -> Instant {
            self.held.parent.original_expiry()
        }
        /// Original cumulative ledger, including prior frames and entered index work.
        pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
            self.held.parent.recovery_usage()
        }
        /// The real child operation returned Ok before the original-E postcheck.
        pub fn child_succeeded(&self) -> bool {
            self.held
                .actual
                .as_ref()
                .is_some_and(|result| result.is_ok())
        }
        /// Actual caught unwind; process-abort configurations cannot return one.
        pub fn has_panic(&self) -> bool {
            self.held.panic.is_some()
        }
    };
}
impl PreparedRecoveredStructuralIndex {
    views!();
    /// Borrow only this actual child index and the disjoint original frame.
    pub(super) fn with_index_frame<T>(
        &mut self,
        framing: &std::cell::Cell<Option<usize>>,
        operation: impl FnOnce(
            Option<&EmbeddedFrameIndex>,
            &mut PreparedDecompressedFrame<RecoveryFrameWork>,
        ) -> T,
    ) -> T {
        let operation_bytes = std::mem::size_of_val(&operation);
        let index = self
            .held
            .child
            .state
            .as_ref()
            .and_then(EmbeddedIndexState::index);
        let scoped =
            |frame: &mut PreparedDecompressedFrame<RecoveryFrameWork>| operation(index, frame);
        let parts = [
            operation_bytes,
            size_of::<usize>(), // actual operation_bytes control
            std::mem::size_of_val(&scoped),
            size_of::<Option<&EmbeddedFrameIndex>>(),
            size_of::<&mut Self>(),
            size_of::<&std::cell::Cell<Option<usize>>>(),
            size_of::<[usize; 7]>(),
        ];
        framing.set(parts.into_iter().fold(framing.get(), |sum, n| {
            sum.and_then(|sum| sum.checked_add(n))
                .filter(|value| *value <= isize::MAX as usize)
        }));
        self.held.parent.with_frame(scoped)
    }
    /// Consume this accepted index into absent metadata or exact native bytes.
    /// The same original work/E and complete parent remain retained; MessagePack
    /// and typed rows are not decoded or accepted by this byte producer.
    /// Absent metadata does not validate unused native decoder limits.
    #[allow(
        clippy::result_large_err,
        reason = "retain complete parent and actual metadata native outcome inline"
    )]
    pub fn prepare_metadata_bytes_until(
        self,
        limits: crate::OwnedLogDecompressionLimits,
    ) -> Result<
        super::recovery_metadata::PreparedRecoveredStructuralMetadataBytes,
        super::recovery_metadata::RetainedRecoveredStructuralMetadataByteError,
    > {
        super::recovery_metadata::prepare_metadata_bytes_until(self, limits)
    }
    /// Consume completed index/parents and decode the next frame on SAME spent work/E.
    #[allow(
        clippy::result_large_err,
        reason = "retain original work and actual native outcome inline"
    )]
    pub fn decompress_next_until(
        self,
        input: Bytes,
        expected_bytes: usize,
        expected_records: u32,
        limits: crate::OwnedLogDecompressionLimits,
    ) -> Result<crate::PreparedRecoveredLogFrame, crate::RetainedRecoveredLogFrameError> {
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
impl RetainedRecoveredStructuralIndexError {
    views!();
}
impl fmt::Debug for PreparedRecoveredStructuralIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedRecoveredStructuralIndex")
            .field("usage", &self.usage())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for RetainedRecoveredStructuralIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedRecoveredStructuralIndexError")
            .field("usage", &self.usage())
            .field("initial", &self.held.initial)
            .field("actual", &self.held.actual)
            .field("post", &self.held.post)
            .field("panicked", &self.has_panic())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedRecoveredStructuralIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for RetainedRecoveredStructuralIndexError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(Err(error)) = &self.held.actual {
            return Some(error);
        }
        if let Some(Err(error)) = &self.held.post {
            return Some(error);
        }
        self.held
            .initial
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(|error| error as &(dyn Error + 'static))
    }
}
#[allow(
    clippy::result_large_err,
    reason = "retain complete parent and actual parser partial state on every result"
)]
pub(super) fn prepare_index_until(
    parent: PreparedRecoveredStructuralTimestamps,
    limits: RecoveredStructuralIndexLimits,
) -> Result<PreparedRecoveredStructuralIndex, RetainedRecoveredStructuralIndexError> {
    let sections = parent.parent_offsets().parent_sections();
    let child = Index {
        range: sections.section_range(RecoveredStructuralSection::EmbeddedIndex),
        record_count: sections.record_count(),
        limits,
        continuation_bytes: 0,
        state: None,
    };
    let held = PreparedDecompressedFrame::continue_owned_until(
        parent,
        child,
        |input, work, child, fixed| child.run(input, work, fixed),
    );
    if held.accepted() {
        Ok(PreparedRecoveredStructuralIndex { held })
    } else {
        Err(RetainedRecoveredStructuralIndexError { held })
    }
}
