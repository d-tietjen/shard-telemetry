//! Exact metadata-lane native bytes on the original retained recovery work.
use super::bounded_decompression::{
    Failure as StreamFailure, FrameContinuation, FrameParent, FrameWork, PreparedDecompressedFrame,
    ScopedFrameStream,
};
use super::recovery_decompression::RecoveryFrameWork;
use super::recovery_index::PreparedRecoveredStructuralIndex;
use super::recovery_sections::RecoveredStructuralSection;
use crate::{
    OwnedLogDecompressionLimits, TelemetryError, TelemetryRecoveryError, TelemetryRecoveryUsage,
};
use bytes::Bytes;
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::ops::Range;
use std::time::Instant;

const RAW_METADATA_SAFETY_BYTES: usize = 64 * 1024 * 1024;

/// Actual metadata-envelope and native-stream charges on the original ledger.
/// Envelope framing is separate from the additional native decoder ceilings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveredStructuralMetadataByteUsage {
    /// Actual complete continuation/return/closure carrier framing.
    pub continuation_bytes: usize,
    /// Prepaid possible Bytes owner-sharing metadata; no source copy is made.
    pub sharing_bytes: usize,
    /// Actual admitted four-byte metadata length inspection.
    pub header_bytes: usize,
    /// Original stream's admitted state/output/context allocation requests.
    pub decoder_declared_bytes: usize,
    /// Original stream's local scalar/slot entries, never JSON records.
    pub decoder_entries: usize,
    /// Original stream's explicit native/copy work.
    pub decoder_work_bytes: usize,
    /// Actual stream calls whose complete results remain retained.
    pub stream_calls: usize,
}

#[derive(Debug)]
enum Failure {
    Recovery(TelemetryRecoveryError),
    Codec(TelemetryError),
    Stream(StreamFailure<TelemetryRecoveryError>),
    UnsupportedZeroRawLength,
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
            Self::Recovery(error) | Self::Stream(StreamFailure::Refused(error)) => Some(error),
            Self::Codec(error) | Self::Stream(StreamFailure::Codec(error)) => Some(error),
            Self::Stream(StreamFailure::Allocation(error)) => Some(error),
            _ => None,
        }
    }
}
type Result<T> = std::result::Result<T, Failure>;
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .filter(|bytes| *bytes <= isize::MAX as usize)
        .ok_or(Failure::Limits)
}
fn invalid(message: &'static str) -> Failure {
    Failure::Codec(TelemetryError::InvalidBlockEncoding(message))
}

struct MetadataBytes {
    range: Option<Range<usize>>,
    record_count: u32,
    limits: OwnedLogDecompressionLimits,
    continuation_bytes: usize,
    sharing_bytes: usize,
    header_bytes: usize,
    header: Option<[u8; 4]>,
    declared_raw_bytes: Option<u32>,
    absent: bool,
    // Its state/results precede its actual second context. No work is stored here.
    stream: Option<ScopedFrameStream>,
}
impl MetadataBytes {
    fn run(
        &mut self,
        structural: &Bytes,
        owner: &mut RecoveryFrameWork,
        fixed: Option<usize>,
    ) -> Result<()> {
        // Preserve the existing three-complete-carrier/closure framing. Also
        // cover actual wrapper padding without assuming equal Rust layouts.
        let supplied_carriers = size_of::<Continued>()
            .checked_mul(3)
            .ok_or(Failure::Limits)?;
        // Explicit live/constructed/returned carriers. The two Result slots
        // are the helper return and public forwarding/move boundary; their
        // actual enum tags/alignment are not inferred from either wrapper.
        let values = [
            size_of::<Continued>(),
            size_of::<PreparedRecoveredStructuralMetadataBytes>(),
            size_of::<RetainedRecoveredStructuralMetadataByteError>(),
            size_of::<MetadataResult>(),
            size_of::<MetadataResult>(),
            size_of::<std::result::Result<(), Box<dyn std::any::Any + Send>>>(),
            size_of::<Result<()>>(),
            size_of::<[usize; 8]>(),
        ];
        let actual_carriers = values.into_iter().try_fold(0, add)?;
        let framing = add(
            fixed.ok_or(Failure::Limits)?,
            actual_carriers.saturating_sub(supplied_carriers),
        )?;
        owner
            .pay(framing, values.len(), framing)
            .map_err(Failure::Recovery)?;
        self.continuation_bytes = framing;
        let range = self
            .range
            .clone()
            .filter(|range| range.start <= range.end && range.end <= structural.len())
            .ok_or_else(|| invalid("missing typed log metadata section"))?;
        if range.is_empty() {
            // The ordinary absent lane enters neither zstd nor MessagePack.
            // Unused native policy is deliberately not validated on this branch.
            self.absent = true;
            return Ok(());
        }
        owner.pay(0, 1, 4).map_err(Failure::Recovery)?;
        self.header_bytes = 4;
        let header_end = add(range.start, 4)?;
        let bytes = structural
            .get(range.start..header_end)
            .filter(|_| header_end <= range.end)
            .ok_or_else(|| invalid("truncated typed log metadata lane"))?;
        let header = [bytes[0], bytes[1], bytes[2], bytes[3]];
        self.header = Some(header);
        let raw = u32::from_le_bytes(header);
        self.declared_raw_bytes = Some(raw);
        let expected = usize::try_from(raw).map_err(|_| Failure::Limits)?;
        if expected > RAW_METADATA_SAFETY_BYTES {
            return Err(invalid("typed log metadata lane exceeds safety limit"));
        }
        if expected == 0 {
            // This is not an absent lane. Do not forge an rmp/native outcome or
            // change the existing stream's positive expected-output contract.
            return Err(Failure::UnsupportedZeroRawLength);
        }
        let sharing = size_of::<usize>().checked_mul(3).ok_or(Failure::Limits)?;
        owner.pay(sharing, 1, sharing).map_err(Failure::Recovery)?;
        self.sharing_bytes = sharing;
        self.stream = Some(ScopedFrameStream::new(
            structural.slice(header_end..range.end),
            expected,
            self.record_count,
            self.limits.into(),
        ));
        // The complete actual parent/child/error/return framing was paid above.
        // Preserve all original stream fees and its three local framing entries.
        self.stream
            .as_mut()
            .ok_or(Failure::Limits)?
            .run(owner, Some(0))
            .map_err(Failure::Stream)
    }
    fn usage(&self) -> RecoveredStructuralMetadataByteUsage {
        let (declared, entries, work) = self
            .stream
            .as_ref()
            .map_or((0, 0, 0), ScopedFrameStream::usage);
        RecoveredStructuralMetadataByteUsage {
            continuation_bytes: self.continuation_bytes,
            sharing_bytes: self.sharing_bytes,
            header_bytes: self.header_bytes,
            decoder_declared_bytes: declared,
            decoder_entries: entries,
            decoder_work_bytes: work,
            stream_calls: self
                .stream
                .as_ref()
                .map_or(0, ScopedFrameStream::stream_calls),
        }
    }
}
type Continued = FrameContinuation<
    PreparedRecoveredStructuralIndex,
    MetadataBytes,
    Failure,
    TelemetryRecoveryError,
>;

type MetadataResult = std::result::Result<
    PreparedRecoveredStructuralMetadataBytes,
    RetainedRecoveredStructuralMetadataByteError,
>;

/// Absent metadata or exact native bytes, with the complete actual index parent.
/// This does not parse MessagePack, construct typed rows or grant query permission.
pub struct PreparedRecoveredStructuralMetadataBytes {
    held: Continued,
}
impl FrameParent<RecoveryFrameWork> for PreparedRecoveredStructuralMetadataBytes {
    fn with_frame<T>(
        &mut self,
        operation: impl FnOnce(&mut PreparedDecompressedFrame<RecoveryFrameWork>) -> T,
    ) -> T {
        self.held.parent.with_frame(operation)
    }
}
/// Terminal metadata-byte outcome retaining actual partial native state and parent.
/// No retry, mutable decoder or original-work extractor is provided.
pub struct RetainedRecoveredStructuralMetadataByteError {
    held: Continued,
}
macro_rules! views {
    () => {
        /// The real absent branch was reached; presence alone is not acceptance.
        pub fn is_absent(&self) -> bool {
            self.held.child.absent
        }
        /// Exact captured native length prefix, distinct from an absent lane.
        pub fn captured_header(&self) -> Option<[u8; 4]> {
            self.held.child.header
        }
        /// Actual captured u32 raw length, distinct from an absent lane.
        pub fn declared_raw_bytes(&self) -> Option<u32> {
            self.held.child.declared_raw_bytes
        }
        /// Original exact typed metadata range in the retained structural source.
        pub fn input_range(&self) -> Option<Range<usize>> {
            self.held.child.range.clone()
        }
        /// Exact compressed remainder owned by the actual entered stream state.
        pub fn retained_compressed_input(&self) -> Option<&[u8]> {
            self.held
                .child
                .stream
                .as_ref()
                .map(ScopedFrameStream::input)
        }
        /// Actual produced prefix, possibly partial or unaccepted on a refusal.
        pub fn output_prefix(&self) -> &[u8] {
            self.held
                .child
                .stream
                .as_ref()
                .map_or(&[], ScopedFrameStream::output_prefix)
        }
        /// Entire initialized actual destination, including its sentinel byte.
        pub fn initialized_output(&self) -> &[u8] {
            self.held
                .child
                .stream
                .as_ref()
                .map_or(&[], ScopedFrameStream::initialized_output)
        }
        /// Whether the real second native context constructor was entered.
        pub fn creation_entered(&self) -> bool {
            self.held
                .child
                .stream
                .as_ref()
                .is_some_and(ScopedFrameStream::creation_entered)
        }
        /// Whether the actual second native context remains owned.
        pub fn has_native_context(&self) -> bool {
            self.held
                .child
                .stream
                .as_ref()
                .is_some_and(ScopedFrameStream::has_native_context)
        }
        /// Actual native initialization result, never inferred from output.
        pub fn init_result(&self) -> Option<&std::result::Result<usize, usize>> {
            self.held.child.stream.as_ref()?.init_result()
        }
        /// Actual native window-parameter result.
        pub fn parameter_result(&self) -> Option<&std::result::Result<usize, usize>> {
            self.held.child.stream.as_ref()?.parameter_result()
        }
        /// Last actual native stream result, captured before original-E checks.
        pub fn last_stream_result(&self) -> Option<&std::result::Result<usize, usize>> {
            self.held.child.stream.as_ref()?.last_stream_result()
        }
        /// Actual local envelope and unchanged stream charges/progress.
        pub fn usage(&self) -> RecoveredStructuralMetadataByteUsage {
            self.held.child.usage()
        }
        /// Complete original index/Pco/outer decoder parent, immutably borrowed.
        pub fn parent_index(&self) -> &PreparedRecoveredStructuralIndex {
            &self.held.parent
        }
        /// Original absolute expiry, never renewed for this child.
        pub fn original_expiry(&self) -> Instant {
            self.held.parent.original_expiry()
        }
        /// The one original cumulative setup ledger, including earlier frames.
        pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
            self.held.parent.recovery_usage()
        }
        /// Actual child returned Ok before the separate original-E postcheck.
        pub fn child_succeeded(&self) -> bool {
            self.held
                .actual
                .as_ref()
                .is_some_and(|result| result.is_ok())
        }
        /// Whether an actual caught unwind remains retained.
        pub fn has_panic(&self) -> bool {
            self.held.panic.is_some()
        }
    };
}
impl PreparedRecoveredStructuralMetadataBytes {
    views!();
    /// Complete exact native bytes; None denotes the ordinary absent lane.
    /// These bytes have not been parsed or accepted as PackedLogMetadata.
    pub fn raw_bytes(&self) -> Option<&[u8]> {
        self.held
            .child
            .stream
            .as_ref()
            .map(ScopedFrameStream::output_prefix)
    }
    /// Consume child/outcomes first and decode another frame on SAME spent work/E.
    #[allow(
        clippy::result_large_err,
        reason = "retain original work and actual native outcome inline"
    )]
    pub fn decompress_next_until(
        self,
        input: Bytes,
        expected_bytes: usize,
        expected_records: u32,
        limits: OwnedLogDecompressionLimits,
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
impl RetainedRecoveredStructuralMetadataByteError {
    views!();
}
impl fmt::Debug for PreparedRecoveredStructuralMetadataBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedRecoveredStructuralMetadataBytes")
            .field("usage", &self.usage())
            .field("absent", &self.is_absent())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for RetainedRecoveredStructuralMetadataByteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedRecoveredStructuralMetadataByteError")
            .field("usage", &self.usage())
            .field("initial", &self.held.initial)
            .field("actual", &self.held.actual)
            .field("post", &self.held.post)
            .field("panicked", &self.has_panic())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedRecoveredStructuralMetadataByteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for RetainedRecoveredStructuralMetadataByteError {
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
    reason = "retain complete parent and actual partial metadata native state on every result"
)]
pub(super) fn prepare_metadata_bytes_until(
    parent: PreparedRecoveredStructuralIndex,
    limits: OwnedLogDecompressionLimits,
) -> std::result::Result<
    PreparedRecoveredStructuralMetadataBytes,
    RetainedRecoveredStructuralMetadataByteError,
> {
    let sections = parent
        .parent_timestamps()
        .parent_offsets()
        .parent_sections();
    let child = MetadataBytes {
        range: sections.section_range(RecoveredStructuralSection::TypedMetadata),
        record_count: sections.record_count(),
        limits,
        continuation_bytes: 0,
        sharing_bytes: 0,
        header_bytes: 0,
        header: None,
        declared_raw_bytes: None,
        absent: false,
        stream: None,
    };
    let held = PreparedDecompressedFrame::continue_owned_until(
        parent,
        child,
        |input, work, child, fixed| child.run(input, work, fixed),
    );
    if held.accepted() {
        Ok(PreparedRecoveredStructuralMetadataBytes { held })
    } else {
        Err(RetainedRecoveredStructuralMetadataByteError { held })
    }
}
