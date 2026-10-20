//! Owned lower-level Pco timestamps on the complete same-ledger offset parent.
mod cost;
mod wire;
use super::bounded_decompression::{
    FrameContinuation, FrameParent, FrameWork, PreparedDecompressedFrame,
};
use super::recovery_decompression::RecoveryFrameWork;
use super::recovery_offsets::PreparedRecoveredStructuralOffsets;
use crate::{TelemetryError, TelemetryRecoveryError, TelemetryRecoveryUsage};
use better_io::BetterBufRead;
use bytes::Bytes;
use pco::Progress;
use pco::errors::{PcoError, PcoResult};
use pco::standalone::{ChunkDecompressor, DecompressorItem, FileDecompressor};
use std::alloc::Layout;
use std::collections::TryReserveError;
use std::error::Error;
use std::fmt;
use std::mem::{size_of, take};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Additional finite timestamp policy; the original setup ledger and E continue.
#[derive(Clone, Copy, Debug)]
pub struct RecoveredStructuralTimestampLimits {
    /// Largest actual encoded timestamp lane, independent of Pco's n_hint.
    pub max_input_bytes: usize,
    /// Authoritative outer structural record count ceiling.
    pub max_records: usize,
    /// Largest number of real chunks, each with a positive count.
    pub max_chunks: usize,
    /// File/chunk/read calls, including the real terminating EndOfData call.
    pub max_native_calls: usize,
    /// Maximum actual bins per chunk; the pinned codec permits at most16384.
    pub max_bins: usize,
    /// Maximum actual ANS table logarithm, at most14 (zero is valid).
    pub max_ans_log: usize,
    /// Concrete retained state and all possible requested native allocations.
    pub max_declared_bytes: usize,
    /// Local scalar/operation entries, separate from JSON/filesystem counters.
    pub max_entries: usize,
    /// Additional declared source work, charged to the same original ledger.
    pub max_work_bytes: usize,
}
/// Actual admitted source work and native progress; no native RSS/CPU claim.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveredStructuralTimestampUsage {
    /// Concrete storage plus cumulative possible native heap requests.
    pub declared_bytes: usize,
    /// Local paid scalar and native-call entries.
    pub entries: usize,
    /// Cumulative admitted source work.
    pub work_bytes: usize,
    /// Current actual preflight bit position (not a native error bit cursor).
    pub preflight_bits: usize,
    /// Actual native file/chunk/read calls entered.
    pub native_calls: usize,
    /// Actual successful chunk constructors transitioned into live owners.
    pub chunks: usize,
    /// Actually finished chunks consumed through canonical into_src.
    pub completed_chunks: usize,
    /// Successful read progress accepted before the next original-E check.
    pub confirmed_records: usize,
    /// Actual read calls whose complete Result has been captured.
    pub reads: usize,
    /// Actual EndOfData was captured and accepted, independently of a type peek.
    pub reached_end: bool,
}
#[derive(Debug)]
enum Failure {
    Limits,
    Unsupported,
    Recovery(TelemetryRecoveryError),
    Codec(TelemetryError),
    Allocation,
    Native,
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
            Self::Recovery(e) => Some(e),
            Self::Codec(e) => Some(e),
            _ => None,
        }
    }
}
type Result<T> = std::result::Result<T, Failure>;
fn invalid(message: &'static str) -> Failure {
    Failure::Codec(TelemetryError::InvalidBlockEncoding(message))
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or(Failure::Limits)
}
struct ReaderState {
    input: Vec<u8>,
    cursor: AtomicUsize,
    fills: AtomicUsize,
    consumes: AtomicUsize,
}
// NonClone wrapper: one actual R is consumed between native owners. The child
// Arc is only a custody/observation escrow, never a second cursor writer.
struct OwnedReader {
    shared: Arc<ReaderState>,
}
fn increment(value: &AtomicUsize, amount: usize) {
    // Sole active OwnedReader is the only writer. Avoid a CAS retry loop.
    let Some(after) = value.load(Ordering::Relaxed).checked_add(amount) else {
        panic!("bounded Pco source observation overflow");
    };
    value.store(after, Ordering::Relaxed);
}

impl BetterBufRead for OwnedReader {
    fn fill_or_eof(&mut self, _requested: usize) -> std::io::Result<&[u8]> {
        increment(&self.shared.fills, 1);
        // Immutable finite in-memory input: no physical IO or discarded IO error.
        Ok(&self.shared.input[self.shared.cursor.load(Ordering::Relaxed)..])
    }
    fn consume(&mut self, bytes: usize) {
        let before = self.shared.cursor.load(Ordering::Relaxed);
        let Some(after) = before
            .checked_add(bytes)
            .filter(|after| *after <= self.shared.input.len())
        else {
            panic!("Pco consumed beyond its actual admitted source");
        };
        self.shared.cursor.store(after, Ordering::Relaxed);
        increment(&self.shared.consumes, 1);
    }
    fn capacity(&self) -> Option<usize> {
        None
    }
    fn resize_capacity(&mut self, _desired: usize) {}
}
struct ReadCall {
    window: Range<usize>,
    actual: Option<PcoResult<Progress>>,
}
struct Timestamps {
    limits: RecoveredStructuralTimestampLimits,
    usage: RecoveredStructuralTimestampUsage,
    range: Option<Range<usize>>,
    expected: usize,
    input: Vec<u8>,
    output: Vec<u64>,
    reservations: [Option<std::result::Result<(), TryReserveError>>; 3],
    shared: Option<Arc<ReaderState>>,
    source: Option<OwnedReader>,
    file_result: Option<PcoResult<(FileDecompressor, OwnedReader)>>,
    file: Option<FileDecompressor>,
    chunk_result: Option<PcoResult<DecompressorItem<u64, OwnedReader>>>,
    chunk: Option<ChunkDecompressor<u64, OwnedReader>>,
    shape: Option<wire::Shape>,
    calls: Vec<ReadCall>,
    pending_window: Option<Range<usize>>,
}
impl Timestamps {
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
    fn observed(&self, owner: &RecoveryFrameWork, phase: &'static str) {
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        super::bounded_frames::observe_decompression(owner.original_expiry(), phase);
        #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
        let _ = (owner, phase);
    }
    fn check(&mut self, owner: &mut RecoveryFrameWork) -> Result<()> {
        owner.check().map_err(Failure::Recovery)
    }
    fn last_cursor(&self) -> Option<usize> {
        self.shared
            .as_ref()
            .map(|s| s.cursor.load(Ordering::Relaxed))
    }
    fn native_error(&self) -> Option<&PcoError> {
        self.calls
            .last()
            .and_then(|r| r.actual.as_ref())
            .and_then(|r| r.as_ref().err())
            .or_else(|| self.chunk_result.as_ref().and_then(|r| r.as_ref().err()))
            .or_else(|| self.file_result.as_ref().and_then(|r| r.as_ref().err()))
    }
    fn observe_message_excess(&mut self, owner: &mut RecoveryFrameWork) -> Result<()> {
        let capacity = self.native_error().map_or(0, |e| e.message.capacity());
        if capacity > cost::MAX_MESSAGE {
            self.pay(
                owner,
                capacity - cost::MAX_MESSAGE,
                0,
                capacity - cost::MAX_MESSAGE,
            )?;
        }
        Ok(())
    }
    fn admit_native<T>(
        &mut self,
        owner: &mut RecoveryFrameWork,
        fees: (usize, usize),
    ) -> Result<()> {
        if self.usage.native_calls >= self.limits.max_native_calls {
            return Err(Failure::Limits);
        }
        // Real return, capture and move slots, in addition to outer continuation.
        let fixed = cost::mul(3, size_of::<PcoResult<T>>())?;
        self.pay(owner, add(fees.0, fixed)?, 1, add(fees.1, fixed)?)?;
        Ok(())
    }
    fn allocation_result(
        &mut self,
        owner: &mut RecoveryFrameWork,
        slot: usize,
        requested: usize,
        actual: usize,
    ) -> Result<()> {
        self.observed(owner, "timestamp-allocation");
        self.check(owner)?;
        if self.reservations[slot].as_ref().is_some_and(|r| r.is_err()) {
            return Err(Failure::Allocation);
        }
        self.pay(
            owner,
            actual.checked_sub(requested).ok_or(Failure::State)?,
            0,
            0,
        )
    }
    fn allocate(&mut self, input: &[u8], owner: &mut RecoveryFrameWork) -> Result<()> {
        self.pay(owner, input.len(), 1, cost::mul(2, input.len())?)?;
        self.reservations[0] = Some(self.input.try_reserve_exact(input.len()));
        self.allocation_result(owner, 0, input.len(), self.input.capacity())?;
        self.input.extend_from_slice(input);
        // Rust1.93 ArcInner is repr(C,align(2)): two AtomicUsize then data.
        // AtomicUsize alignment already exceeds2 on the selected target.
        let (arc, _) = Layout::new::<[AtomicUsize; 2]>()
            .extend(Layout::new::<ReaderState>())
            .map_err(|_| Failure::Limits)?;
        let arc = arc.pad_to_align().size();
        // Actual ReaderState constructor/move carriers, in addition to Arc heap.
        let carriers = cost::mul(2, size_of::<ReaderState>())?;
        self.pay(owner, add(arc, carriers)?, 1, add(arc, carriers)?)?;
        self.shared = Some(Arc::new(ReaderState {
            input: take(&mut self.input),
            cursor: AtomicUsize::new(0),
            fills: AtomicUsize::new(0),
            consumes: AtomicUsize::new(0),
        }));
        self.source = Some(OwnedReader {
            shared: Arc::clone(self.shared.as_ref().ok_or(Failure::State)?),
        });
        self.observed(owner, "timestamp-reader");
        self.check(owner)?;
        let bytes = cost::mul(self.expected, size_of::<u64>())?;
        self.pay(owner, bytes, 1, bytes)?;
        self.reservations[1] = Some(self.output.try_reserve_exact(self.expected));
        self.allocation_result(
            owner,
            1,
            bytes,
            cost::mul(self.output.capacity(), size_of::<u64>())?,
        )?;
        self.output.resize(self.expected, 0); // retain the ENTIRE initialized destination on Err.
        // At most one positive read per record; n_hint never drives this bound.
        let slots = self.expected;
        let bytes = cost::mul(slots, size_of::<ReadCall>())?;
        self.pay(owner, bytes, 1, bytes)?;
        self.reservations[2] = Some(self.calls.try_reserve_exact(slots));
        self.allocation_result(
            owner,
            2,
            bytes,
            cost::mul(self.calls.capacity(), size_of::<ReadCall>())?,
        )
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
        if [
            self.limits.max_input_bytes,
            self.limits.max_records,
            self.limits.max_chunks,
            self.limits.max_native_calls,
            self.limits.max_bins,
            self.limits.max_declared_bytes,
            self.limits.max_entries,
            self.limits.max_work_bytes,
        ]
        .into_iter()
        .any(|n| n == 0 || n > isize::MAX as usize)
            || self.limits.max_ans_log > 14
            || self.limits.max_bins > 16384
            || input.len() > self.limits.max_input_bytes
            || self.expected > self.limits.max_records
        {
            return Err(Failure::Limits);
        }
        self.pay(
            owner,
            fixed.ok_or(Failure::Limits)?,
            1,
            fixed.ok_or(Failure::Limits)?,
        )?;
        if !cfg!(telemetry_bounded_pco_layout) {
            return Err(Failure::Unsupported);
        }
        let header_end = self.preflight_file(input, owner)?;
        self.allocate(input, owner)?;
        self.admit_native::<(FileDecompressor, OwnedReader)>(owner, cost::file()?)?;
        let calls = add(self.usage.native_calls, 1)?;
        let source = self.source.take().ok_or(Failure::State)?;
        self.usage.native_calls = calls;
        self.file_result = Some(FileDecompressor::new(source));
        self.observed(owner, "timestamp-file-result");
        self.observe_message_excess(owner)?;
        self.check(owner)?;
        if self.file_result.as_ref().is_some_and(|r| r.is_err()) {
            return Err(Failure::Native);
        }
        if self.last_cursor() != Some(header_end) {
            return Err(Failure::State);
        }
        let (file, source) = self
            .file_result
            .take()
            .ok_or(Failure::State)?
            .map_err(|_| Failure::State)?;
        self.file = Some(file);
        self.source = Some(source);
        loop {
            self.check(owner)?;
            let start = self.last_cursor().ok_or(Failure::State)?;
            self.shape = self.preflight_chunk(input, owner, start)?;
            let fees = if let Some(shape) = self.shape {
                cost::chunk(
                    shape.bins,
                    shape.ans_log,
                    shape.delta,
                    shape.end.checked_sub(shape.start).ok_or(Failure::State)?,
                )?
            } else {
                // Actual zero termination: with_reader(1), possible2-byte EOF buffer.
                (
                    add(2, cost::error_storage()?)?,
                    add(
                        add(add(4, cost::error_storage()?)?, cost::reader_work(1)?)?,
                        cost::error_extra_work()?,
                    )?,
                )
            };
            self.admit_native::<DecompressorItem<u64, OwnedReader>>(owner, fees)?;
            let calls = add(self.usage.native_calls, 1)?;
            let file = self.file.as_ref().ok_or(Failure::State)?;
            let source = self.source.take().ok_or(Failure::State)?;
            self.usage.native_calls = calls;
            self.chunk_result = Some(file.chunk_decompressor::<u64, _>(source));
            self.observed(owner, "timestamp-chunk-result");
            if self.shape.is_none() {
                self.observed(owner, "timestamp-eof-result");
            }
            self.observe_message_excess(owner)?;
            self.check(owner)?;
            if self.chunk_result.as_ref().is_some_and(|r| r.is_err()) {
                return Err(Failure::Native);
            }
            // A successful actual constructor is moved only after original-E check.
            match self
                .chunk_result
                .take()
                .ok_or(Failure::State)?
                .map_err(|_| Failure::State)?
            {
                DecompressorItem::EndOfData(source) => {
                    self.source = Some(source);
                    self.usage.reached_end = true;
                    if self.shape.is_some() || self.usage.confirmed_records != self.expected {
                        return Err(invalid("Pco ended before authoritative count"));
                    }
                    // Preserve ordinary first-EndOfData semantics: unread tail remains.
                    return Ok(());
                }
                DecompressorItem::Chunk(chunk) => {
                    self.chunk = Some(chunk);
                    let shape = self.shape.ok_or(Failure::State)?;
                    if self.chunk.as_ref().ok_or(Failure::State)?.n() != shape.n
                        || self.last_cursor() != Some(shape.end)
                    {
                        return Err(Failure::State);
                    }
                    self.usage.chunks = add(self.usage.chunks, 1)?;
                    let mut remaining = shape.n;
                    while remaining > 0 {
                        let n = remaining.min(256); // ONE native batch, including final partial.
                        let begin = self.usage.confirmed_records;
                        let end = add(begin, n)?;
                        self.pending_window = Some(begin..end);
                        self.admit_native::<Progress>(owner, cost::read(n)?)?;
                        self.observed(owner, "timestamp-read-admission");
                        let capture = cost::mul(3, size_of::<ReadCall>())?;
                        self.pay(owner, capture, 1, capture)?;
                        if self.calls.len() == self.calls.capacity() {
                            return Err(Failure::State);
                        }
                        self.calls.push(ReadCall {
                            window: begin..end,
                            actual: None,
                        });
                        let calls = add(self.usage.native_calls, 1)?;
                        let captured = self.calls.last_mut().ok_or(Failure::State)?;
                        let chunk = self.chunk.as_mut().ok_or(Failure::State)?;
                        let output = self.output.get_mut(begin..end).ok_or(Failure::State)?;
                        // Admission and every fallible capture/argument lookup
                        // precede this actual-entry count. A native unwind still
                        // counts its entered call without fabricating a Result.
                        self.usage.native_calls = calls;
                        captured.actual = Some(chunk.read(output));
                        self.usage.reads = add(self.usage.reads, 1)?;
                        self.observed(owner, "timestamp-read-result");
                        self.observe_message_excess(owner)?;
                        self.check(owner)?;
                        let actual = self
                            .calls
                            .last()
                            .and_then(|r| r.actual.as_ref())
                            .ok_or(Failure::State)?;
                        let progress = actual.as_ref().map_err(|_| Failure::Native)?;
                        if progress.n_processed != n || progress.finished != (n == remaining) {
                            return Err(invalid("Pco progress contradicts actual finite window"));
                        }
                        // No-progress false and any contradictory progress refuse once;
                        // there is no retry/no-op loop. Real Result remains in calls.
                        self.usage.confirmed_records = end;
                        remaining -= n;
                        self.pending_window = None;
                    }
                    self.check(owner)?;
                    // Only a real finished read checked above allows this transition.
                    self.source = Some(self.chunk.take().ok_or(Failure::State)?.into_src());
                    self.usage.completed_chunks = add(self.usage.completed_chunks, 1)?;
                    self.observed(owner, "timestamp-finished-source");
                    self.check(owner)?;
                }
            }
        }
    }
}
type Continued = FrameContinuation<
    PreparedRecoveredStructuralOffsets,
    Timestamps,
    Failure,
    TelemetryRecoveryError,
>;

/// Actual timestamps and the complete successful offset/section/native parent.
/// Bodies/fields/typed metadata stay encoded; this is not a typed row or query.
pub struct PreparedRecoveredStructuralTimestamps {
    held: Continued,
}
impl FrameParent<RecoveryFrameWork> for PreparedRecoveredStructuralTimestamps {
    fn with_frame<T>(
        &mut self,
        operation: impl FnOnce(&mut PreparedDecompressedFrame<RecoveryFrameWork>) -> T,
    ) -> T {
        self.held.parent.with_frame(operation)
    }
}
impl PreparedRecoveredStructuralTimestamps {
    // Observe actual completed parent custody in the recovery-index regressions.
    #[cfg(test)]
    pub(super) fn has_native_file(&self) -> bool {
        self.held.child.file.is_some()
            || self
                .held
                .child
                .file_result
                .as_ref()
                .is_some_and(|result| result.is_ok())
    }
    // The real EndOfData Result has been consumed into its returned reader.
    #[cfg(test)]
    pub(super) fn has_end_source(&self) -> bool {
        self.held.child.usage.reached_end && self.held.child.source.is_some()
    }
    /// Decode the actual embedded index on the same original work and expiry.
    /// The complete Pco/offset/section/native parent remains retained on refusal.
    #[allow(
        clippy::result_large_err,
        reason = "retain complete original parents and real partial index state inline"
    )]
    pub fn prepare_index_until(
        self,
        limits: super::recovery_index::RecoveredStructuralIndexLimits,
    ) -> std::result::Result<
        super::recovery_index::PreparedRecoveredStructuralIndex,
        super::recovery_index::RetainedRecoveredStructuralIndexError,
    > {
        super::recovery_index::prepare_index_until(self, limits)
    }
    /// Every actual decoded u64 timestamp in authoritative outer row order.
    pub fn timestamps(&self) -> &[u64] {
        &self.held.child.output
    }
    /// Complete offset/section/native owner, with no mutable work extractor.
    pub fn parent_offsets(&self) -> &PreparedRecoveredStructuralOffsets {
        &self.held.parent
    }
    /// Actual additional timestamp charges/progress.
    pub fn usage(&self) -> RecoveredStructuralTimestampUsage {
        self.held.child.usage
    }
    /// Same original absolute setup deadline.
    pub fn original_expiry(&self) -> Instant {
        self.held.parent.original_expiry()
    }
    /// Cumulative original setup, all prior decoders, sections and lanes.
    pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
        self.held.parent.recovery_usage()
    }
    /// Unread bytes after real EndOfData, accepted just as ordinary Pco does.
    pub fn unread_tail(&self) -> &[u8] {
        self.held
            .child
            .shared
            .as_ref()
            .map_or(&[], |s| &s.input[s.cursor.load(Ordering::Relaxed)..])
    }
    /// Consume completed children before a new frame on SAME original work/E.
    #[allow(
        clippy::result_large_err,
        reason = "retain actual next-frame state and original work inline"
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
impl fmt::Debug for PreparedRecoveredStructuralTimestamps {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedRecoveredStructuralTimestamps")
            .field("usage", &self.usage())
            .finish_non_exhaustive()
    }
}
/// Terminal actual native results, initialized output and complete parent.
/// No work/native extractor or retry continuation is provided.
pub struct RetainedRecoveredStructuralTimestampError {
    held: Continued,
}
impl RetainedRecoveredStructuralTimestampError {
    /// Complete offset/section/native owner, including all original wire bytes.
    pub fn parent_offsets(&self) -> &PreparedRecoveredStructuralOffsets {
        &self.held.parent
    }
    /// Whole initialized destination; current window may have been changed on Err.
    pub fn initialized_timestamps(&self) -> &[u64] {
        &self.held.child.output
    }
    /// Successful progress accepted before the next native call/original-E check.
    pub fn confirmed_timestamps(&self) -> &[u64] {
        &self.held.child.output[..self.held.child.usage.confirmed_records]
    }
    /// Actual possibly-modified window, including on Err/late E/unwind.
    pub fn pending_window(&self) -> Option<Range<usize>> {
        self.held.child.pending_window.clone()
    }
    /// Actual last read Result, never reconstructed from initialized values.
    pub fn last_read_result(&self) -> Option<&PcoResult<Progress>> {
        self.held.child.calls.last()?.actual.as_ref()
    }
    /// Actual last read's destination extent, even if a caught panic returned no Result.
    pub fn last_read_window(&self) -> Option<Range<usize>> {
        self.held.child.calls.last().map(|call| call.window.clone())
    }
    /// Actual native error, distinct from any original-work/postcheck refusal.
    pub fn native_error(&self) -> Option<&PcoError> {
        self.held.child.native_error()
    }
    /// Actual native File owner exists after a successful constructor transition.
    pub fn has_native_file(&self) -> bool {
        self.held.child.file.is_some()
            || self
                .held
                .child
                .file_result
                .as_ref()
                .is_some_and(|r| r.is_ok())
    }
    /// Actual live Chunk survives read refusal/error/late E/unwind.
    pub fn has_native_chunk(&self) -> bool {
        self.held.child.chunk.is_some()
            || self
                .held
                .child
                .chunk_result
                .as_ref()
                .is_some_and(|r| matches!(r, Ok(DecompressorItem::Chunk(_))))
    }
    /// Whether the actual file constructor returned a captured Result.
    pub fn file_result_captured(&self) -> bool {
        self.held.child.file_result.is_some() || self.held.child.file.is_some()
    }
    /// Whether the current actual chunk constructor Result remains captured.
    pub fn chunk_result_captured(&self) -> bool {
        self.held.child.chunk_result.is_some() || self.held.child.chunk.is_some()
    }
    /// Actual EndOfData Result remains retained before any late acceptance check.
    pub fn end_result_captured(&self) -> bool {
        self.held
            .child
            .chunk_result
            .as_ref()
            .is_some_and(|r| matches!(r, Ok(DecompressorItem::EndOfData(_))))
    }
    /// Last committed native byte consume, not inspected/error bit position.
    pub fn last_committed_source_cursor(&self) -> Option<usize> {
        self.held.child.last_cursor()
    }
    /// Actual fill/consume observations; the escrow never reconstructs a reader.
    pub fn source_observations(&self) -> Option<(usize, usize)> {
        self.held.child.shared.as_ref().map(|s| {
            (
                s.fills.load(Ordering::Relaxed),
                s.consumes.load(Ordering::Relaxed),
            )
        })
    }
    /// Full actual immutable admitted input survives constructors dropping R on Err.
    pub fn retained_input(&self) -> Option<&[u8]> {
        self.held.child.shared.as_ref().map(|s| s.input.as_slice())
    }
    /// Actual additional source charges and progress.
    pub fn usage(&self) -> RecoveredStructuralTimestampUsage {
        self.held.child.usage
    }
    /// Actual child completion Result, separate from late E/unwind.
    pub fn child_succeeded(&self) -> bool {
        self.held.actual.as_ref().is_some_and(|r| r.is_ok())
    }
    /// An actual unwind was captured; abort builds cannot return from a panic.
    pub fn has_panic(&self) -> bool {
        self.held.panic.is_some()
    }
    /// Same original expiry, including when native physical completion was late.
    pub fn original_expiry(&self) -> Instant {
        self.held.parent.original_expiry()
    }
    /// Cumulative original setup and partial codec charges.
    pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
        self.held.parent.recovery_usage()
    }
    /// Actual final original-work refusal retained separately from native results.
    pub fn post_error(&self) -> Option<&TelemetryRecoveryError> {
        self.held.post.as_ref()?.as_ref().err()
    }
}
impl fmt::Debug for RetainedRecoveredStructuralTimestampError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedRecoveredStructuralTimestampError")
            .field("usage", &self.usage())
            .field("file", &self.has_native_file())
            .field("chunk", &self.has_native_chunk())
            .field("panic", &self.has_panic())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedRecoveredStructuralTimestampError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for RetainedRecoveredStructuralTimestampError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(Err(Failure::Native)) = &self.held.actual {
            return self.native_error().map(|e| e as &dyn Error);
        }
        if let Some(Err(Failure::Allocation)) = &self.held.actual {
            return self
                .held
                .child
                .reservations
                .iter()
                .flatten()
                .find_map(|r| r.as_ref().err())
                .map(|e| e as &dyn Error);
        }
        if let Some(Err(e)) = &self.held.actual {
            return Some(e);
        }
        if let Some(Err(e)) = &self.held.initial {
            return Some(e);
        }
        self.post_error().map(|e| e as &dyn Error)
    }
}
#[allow(
    clippy::result_large_err,
    reason = "retain original work, complete parent and actual native Results inline"
)]
pub(super) fn prepare_timestamps_until(
    parent: PreparedRecoveredStructuralOffsets,
    range: Option<Range<usize>>,
    limits: RecoveredStructuralTimestampLimits,
) -> std::result::Result<
    PreparedRecoveredStructuralTimestamps,
    RetainedRecoveredStructuralTimestampError,
> {
    let expected = parent.offsets().len();
    let child = Timestamps {
        limits,
        usage: RecoveredStructuralTimestampUsage::default(),
        range,
        expected,
        input: Vec::new(),
        output: Vec::new(),
        reservations: std::array::from_fn(|_| None),
        shared: None,
        source: None,
        file_result: None,
        file: None,
        chunk_result: None,
        chunk: None,
        shape: None,
        calls: Vec::new(),
        pending_window: None,
    };
    let held = PreparedDecompressedFrame::<RecoveryFrameWork>::continue_owned_until(
        parent,
        child,
        |input, work, child, fixed| child.run(input, work, fixed),
    );
    if held.accepted() {
        Ok(PreparedRecoveredStructuralTimestamps { held })
    } else {
        Err(RetainedRecoveredStructuralTimestampError { held })
    }
}
