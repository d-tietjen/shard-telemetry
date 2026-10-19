//! Consuming SLW1/SLT1 preparation with real retained embedded-index producers.
//!
//! Explicit recovery policy permits retained streaming decompression; stripe
//! mutation, publication, journal writes and worker entry remain separate.

use super::bounded_decompression::{
    PreparedDecompressedFrame, RetainedFrameDecompressionError, decompress_structural_frame_until,
};
use super::*;
use crate::structural::{
    EmbeddedIndexDecodeLimits, PreparedEmbeddedFrameIndex, RetainedEmbeddedIndexError,
};
use shard_stream_engine::{NativeResumeRefusal, NativeResumeWork};
use std::any::Any;
use std::collections::TryReserveError;
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};

const HASH_CHUNK: usize = 16 * 1024;
// Selected bytes 1.12.1 Shared: pointer, usize, AtomicUsize, word-aligned.
// slice clones may promote a Vec-backed owner, including a losing CAS attempt.
const BYTES_PROMOTION_BYTES: usize = 3 * size_of::<usize>();

/// Finite outer framing/hash/value limits; every child also spends SAME work.
#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexedFramePreparationLimits {
    pub(crate) max_payload_bytes: usize,
    pub(crate) max_context_bytes: usize,
    /// Expected total AND actual cumulative group records, before child output.
    pub(crate) max_records: usize,
    pub(crate) max_groups: usize,
    pub(crate) max_structural_bytes_per_group: usize,
    pub(crate) max_checksum_bytes: usize,
    /// Outer input/value/vector/promotion declarations, excluding child outputs.
    pub(crate) max_declared_bytes: usize,
    pub(crate) max_entries: usize,
    /// Outer parsing/hash/movement work, excluding separately charged children.
    pub(crate) max_work_bytes: usize,
    /// Per-child policy, additionally subject to the SAME cumulative native work.
    pub(crate) index: EmbeddedIndexDecodeLimits,
    /// Contextless recovery only; None preserves live-context-only admission.
    pub(crate) decompression: Option<FrameDecompressionLimits>,
}

#[derive(Debug)]
pub(crate) enum IndexedFramePreparationFailure {
    Refused(NativeResumeRefusal),
    Limits,
    Codec(TelemetryError),
    /// The ordinary route needs a real bounded decompressor at this point.
    DecompressionUnsupported,
    Decompression,
    Allocation(TryReserveError),
    Index,
    Panic,
    State,
}
type Failure = IndexedFramePreparationFailure;
type Result<T> = std::result::Result<T, Failure>;
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or(Failure::Limits)
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or(Failure::Limits)
}
fn codec(message: &'static str) -> Failure {
    Failure::Codec(TelemetryError::InvalidBlockEncoding(message))
}

/// Actual outer prepayments; child output/codec fees remain in shared work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct IndexedFramePreparationUsage {
    pub(crate) declared_bytes: usize,
    pub(crate) entries: usize,
    pub(crate) work_bytes: usize,
    pub(crate) checksum_bytes: usize,
}

/// Exact durable group facts plus the actual consuming child's retained result.
pub(crate) struct PreparedIngestFrame {
    pub(crate) cohort: CompressionCohortId,
    pub(crate) record_count: u32,
    pub(crate) structural_bytes: usize,
    pub(crate) min_timestamp_unix_nanos: u64,
    pub(crate) max_timestamp_unix_nanos: u64,
    compressed: Range<usize>,
    expected_checksum: u32,
    checksum: Option<u32>,
    embedded: Option<Range<usize>>,
    index: Option<std::result::Result<PreparedEmbeddedFrameIndex, RetainedEmbeddedIndexError>>,
    decompressed:
        Option<std::result::Result<PreparedDecompressedFrame, RetainedFrameDecompressionError>>,
}
impl PreparedIngestFrame {
    pub(crate) fn embedded_index(&self) -> Option<&EmbeddedFrameIndex> {
        self.index.as_ref()?.as_ref().ok()?.index()
    }
    pub(crate) fn index_failure(&self) -> Option<&RetainedEmbeddedIndexError> {
        self.index.as_ref()?.as_ref().err()
    }
}

struct Attempt {
    // Original complete inputs enter custody BEFORE any E/limit/admission check.
    payload: Bytes,
    context: Option<Bytes>,
    expected_records: u32,
    cursor: usize,
    context_cursor: usize,
    groups: usize,
    observed_records: u64,
    frames: Vec<PreparedIngestFrame>,
    current: Option<PreparedIngestFrame>,
    index_input: Option<Bytes>,
    decompression_input: Option<Bytes>,
    hasher: Option<blake3::Hasher>,
    hashed_through: usize,
    allocation_error: Option<TryReserveError>,
    panic: Option<Box<dyn Any + Send>>,
    complete: bool,
    usage: IndexedFramePreparationUsage,
    limits: IndexedFramePreparationLimits,
    work: NativeResumeWork,
}

/// Group preparation only; no application checkpoint or serving permission.
pub(crate) struct PreparedIndexedIngestFrames {
    retained: Attempt,
}
/// By-value failure, retaining original inputs and all actual produced prefixes.
pub(crate) struct RetainedIndexedFrameError {
    pub(crate) cause: Failure,
    retained: Attempt,
}
impl PreparedIndexedIngestFrames {
    pub(crate) fn payload(&self) -> &Bytes {
        &self.retained.payload
    }
    #[allow(
        dead_code,
        reason = "preserve read-only original frame custody diagnostics without changing retained inputs"
    )]
    pub(crate) fn context(&self) -> Option<&Bytes> {
        self.retained.context.as_ref()
    }
    pub(crate) fn frames(&self) -> &[PreparedIngestFrame] {
        &self.retained.frames
    }
    #[allow(
        dead_code,
        reason = "preserve read-only original frame custody diagnostics without changing retained inputs"
    )]
    pub(crate) fn compressed(&self, frame: usize) -> Option<&[u8]> {
        self.retained
            .payload
            .get(self.retained.frames.get(frame)?.compressed.clone())
    }
    #[allow(
        dead_code,
        reason = "preserve read-only original frame custody diagnostics without changing retained inputs"
    )]
    pub(crate) fn usage(&self) -> IndexedFramePreparationUsage {
        self.retained.usage
    }
    /// Transfer a successful child's actual completed value into external
    /// staging, retaining this complete original attempt and every child owner.
    pub(crate) fn transfer_index_to(
        &mut self,
        frame: usize,
        destination: &mut Option<EmbeddedFrameIndex>,
    ) -> bool {
        let Some(Ok(child)) = self
            .retained
            .frames
            .get_mut(frame)
            .and_then(|f| f.index.as_mut())
        else {
            return false;
        };
        child.transfer_index_to(destination)
    }
    /// Borrow the original payload range; producing a Bytes slice is charged
    /// by the caller, independently of the preparation's earlier slice fees.
    pub(crate) fn compressed_range(&self, frame: usize) -> Option<Range<usize>> {
        Some(self.retained.frames.get(frame)?.compressed.clone())
    }
}
impl RetainedIndexedFrameError {
    #[allow(
        dead_code,
        reason = "preserve read-only original frame custody diagnostics without changing retained inputs"
    )]
    pub(crate) fn payload(&self) -> &Bytes {
        &self.retained.payload
    }
    #[allow(
        dead_code,
        reason = "preserve read-only original frame custody diagnostics without changing retained inputs"
    )]
    pub(crate) fn context(&self) -> Option<&Bytes> {
        self.retained.context.as_ref()
    }
    #[allow(
        dead_code,
        reason = "preserve read-only original frame custody diagnostics without changing retained inputs"
    )]
    pub(crate) fn completed_frames(&self) -> &[PreparedIngestFrame] {
        &self.retained.frames
    }
    #[allow(
        dead_code,
        reason = "preserve read-only original frame custody diagnostics without changing retained inputs"
    )]
    pub(crate) fn current_frame(&self) -> Option<&PreparedIngestFrame> {
        self.retained.current.as_ref()
    }
    #[allow(
        dead_code,
        reason = "preserve read-only original frame custody diagnostics without changing retained inputs"
    )]
    pub(crate) fn usage(&self) -> IndexedFramePreparationUsage {
        self.retained.usage
    }
}
impl fmt::Debug for RetainedIndexedFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedIndexedFrameError")
            .field("cause", &self.cause)
            .field("frames", &self.retained.frames.len())
            .field("hashed_through", &self.retained.hashed_through)
            .field("usage", &self.retained.usage)
            .field("complete", &self.retained.complete)
            .field("panicked", &self.retained.panic.is_some())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for PreparedIndexedIngestFrames {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedIndexedIngestFrames")
            .field("frames", &self.retained.frames.len())
            .field("usage", &self.retained.usage)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedIndexedFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for RetainedIndexedFrameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause {
            Failure::Refused(e) => Some(e),
            Failure::Codec(e) => Some(e),
            Failure::Allocation(e) => Some(e),
            Failure::Index => self
                .retained
                .current
                .as_ref()?
                .index_failure()
                .map(|e| e as &(dyn Error + 'static)),
            Failure::Decompression => self
                .retained
                .current
                .as_ref()?
                .decompressed
                .as_ref()?
                .as_ref()
                .err()
                .map(|e| e as &(dyn Error + 'static)),
            _ => None,
        }
    }
}

/// Prepare real grouped live indexes from moved original SLW1 and SLT1 owners.
///
/// Preserves ordinary wire semantics when the explicit limits fit. Missing or
/// unrecognized context uses the explicit optional bounded recovery policy;
/// without that policy it refuses. No sidecar is fabricated. Both variants retain SAME work/E
/// and inputs. There is no allocation of an emergency error box on refusal.
#[allow(
    clippy::result_large_err,
    reason = "retain complete original owners and native outcomes inline without a refusal allocation"
)]
pub(crate) fn prepare_indexed_ingest_frames_until(
    payload: Bytes,
    context: Option<Bytes>,
    expected_records: u32,
    limits: IndexedFramePreparationLimits,
    work: &NativeResumeWork,
) -> std::result::Result<PreparedIndexedIngestFrames, RetainedIndexedFrameError> {
    let mut retained = Attempt {
        payload,
        context,
        expected_records,
        cursor: 0,
        context_cursor: 0,
        groups: 0,
        observed_records: 0,
        frames: Vec::new(),
        current: None,
        index_input: None,
        decompression_input: None,
        hasher: None,
        hashed_through: 0,
        allocation_error: None,
        panic: None,
        complete: false,
        usage: IndexedFramePreparationUsage::default(),
        limits,
        work: work.clone(),
    };
    let actual = catch_unwind(AssertUnwindSafe(|| retained.run()));
    let cause = match actual {
        Ok(Ok(())) => return Ok(PreparedIndexedIngestFrames { retained }),
        Ok(Err(cause)) => cause,
        Err(payload) => {
            retained.panic = Some(payload);
            Failure::Panic
        }
    };
    Err(RetainedIndexedFrameError { cause, retained })
}

impl Attempt {
    fn check(&self) -> Result<()> {
        self.work.check().map_err(Failure::Refused)
    }
    fn pay(&mut self, bytes: usize, entries: usize, work: usize) -> Result<()> {
        let next = IndexedFramePreparationUsage {
            declared_bytes: add(self.usage.declared_bytes, bytes)?,
            entries: add(self.usage.entries, entries)?,
            work_bytes: add(self.usage.work_bytes, work)?,
            ..self.usage
        };
        if next.declared_bytes > self.limits.max_declared_bytes
            || next.entries > self.limits.max_entries
            || next.work_bytes > self.limits.max_work_bytes
        {
            return Err(Failure::Limits);
        }
        self.work
            .reserve_values(bytes, 0, entries, work)
            .map_err(Failure::Refused)?;
        self.usage = next;
        Ok(())
    }
    fn observed(&self, phase: &'static str) -> Result<()> {
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        tests::after_value(self.work.absolute_expiry(), phase, self.frames.len());
        #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
        let _ = phase;
        self.check()
    }
    fn run(&mut self) -> Result<()> {
        self.check()?;
        let l = self.limits;
        if [
            l.max_payload_bytes,
            l.max_context_bytes,
            l.max_records,
            l.max_groups,
            l.max_structural_bytes_per_group,
            l.max_checksum_bytes,
            l.max_declared_bytes,
            l.max_entries,
            l.max_work_bytes,
        ]
        .into_iter()
        .any(|n| n == 0 || n > isize::MAX as usize)
            || self.payload.len() > l.max_payload_bytes
            || self
                .context
                .as_ref()
                .is_some_and(|c| c.len() > l.max_context_bytes)
            || self.expected_records as usize > l.max_records
        {
            return Err(Failure::Limits);
        }
        let visible = add(
            self.payload.len(),
            self.context.as_ref().map_or(0, Bytes::len),
        )?;
        let fixed = add(size_of::<Self>(), size_of::<RetainedIndexedFrameError>())?;
        self.pay(add(fixed, visible)?, 2, fixed)?;
        self.pay(0, 0, INGEST_PACK_HEADER_BYTES)?;
        if self.payload.len() < INGEST_PACK_HEADER_BYTES
            || self.payload.get(..4) != Some(INGEST_PACK_MAGIC)
            || self.payload[10..12] != [0; 2]
        {
            return Err(codec("invalid compressed ingest pack header"));
        }
        if u32_at(&self.payload, 4)? != self.expected_records {
            return Err(codec("compressed ingest record count mismatch"));
        }
        self.groups = usize::from(u16_at(&self.payload, 8)?);
        self.cursor = INGEST_PACK_HEADER_BYTES;
        self.observed("header")?;
        if self.groups > l.max_groups {
            return Err(Failure::Limits);
        }
        let live_context = self
            .context
            .as_ref()
            .is_some_and(|c| c.starts_with(TRANSIENT_PACK_MAGIC));
        if !live_context {
            // The ordinary zero-group path never enters a decompressor. It
            // also ignores unrecognized context; preserve that empty result.
            if self.groups == 0 {
                if self.cursor != self.payload.len() || self.expected_records != 0 {
                    return Err(codec("compressed ingest pack totals are invalid"));
                }
                self.complete = true;
                return self.observed("complete");
            }
            if l.decompression.is_none() {
                return Err(Failure::DecompressionUnsupported);
            }
        }
        if live_context {
            self.pay(0, 0, TRANSIENT_PACK_HEADER_BYTES)?;
            let context = self.context.as_ref().ok_or(Failure::State)?;
            if context.len() < TRANSIENT_PACK_HEADER_BYTES || context[6..8] != [0; 2] {
                return Err(codec("invalid transient ingest pack header"));
            }
            if usize::from(u16_at(context, 4)?) != self.groups {
                return Err(codec("transient ingest group count mismatch"));
            }
            self.context_cursor = TRANSIENT_PACK_HEADER_BYTES;
        }
        let bytes = mul(self.groups, size_of::<PreparedIngestFrame>())?;
        self.pay(bytes, self.groups, bytes)?;
        self.allocation_error = self.frames.try_reserve_exact(self.groups).err();
        if let Some(error) = self.allocation_error.take() {
            return Err(Failure::Allocation(error));
        }
        let excess = self.frames.capacity().saturating_sub(self.groups);
        let extra = mul(excess, size_of::<PreparedIngestFrame>())?;
        self.pay(extra, excess, extra)?;
        self.observed("slots")?;
        for _ in 0..self.groups {
            self.next_frame()?;
        }
        if self.cursor != self.payload.len()
            || self.observed_records != u64::from(self.expected_records)
            || self.expected_records > 0 && self.groups == 0
        {
            return Err(codec("compressed ingest pack totals are invalid"));
        }
        if live_context && self.context_cursor != self.context.as_ref().ok_or(Failure::State)?.len()
        {
            return Err(codec("transient ingest pack totals are invalid"));
        }
        self.complete = true;
        self.observed("complete")
    }
    fn next_frame(&mut self) -> Result<()> {
        self.pay(0, 0, INGEST_GROUP_HEADER_BYTES)?;
        let end = add(self.cursor, INGEST_GROUP_HEADER_BYTES)?;
        let header = self
            .payload
            .get(self.cursor..end)
            .ok_or_else(|| codec("compressed ingest group header is truncated"))?;
        let records = u32_at(header, 8)?;
        let structural_bytes = u32_at(header, 12)? as usize;
        let payload_end = end
            .checked_add(u32_at(header, 16)? as usize)
            .filter(|n| *n <= self.payload.len())
            .ok_or_else(|| codec("compressed ingest group payload is truncated"))?;
        self.current = Some(PreparedIngestFrame {
            cohort: CompressionCohortId::new(u64_at(header, 0)?),
            record_count: records,
            structural_bytes,
            min_timestamp_unix_nanos: u64_at(header, 24)?,
            max_timestamp_unix_nanos: u64_at(header, 32)?,
            compressed: end..payload_end,
            expected_checksum: u32_at(header, 20)?,
            checksum: None,
            embedded: None,
            index: None,
            decompressed: None,
        });
        self.cursor = payload_end;
        self.observed_records = self
            .observed_records
            .checked_add(u64::from(records))
            .ok_or(Failure::Codec(TelemetryError::RecordTooLarge))?;
        self.observed("group")?;
        let frame = self.current.as_ref().ok_or(Failure::State)?;
        if records == 0
            || structural_bytes == 0
            || structural_bytes > MAX_INGEST_STRUCTURAL_BYTES
            || frame.min_timestamp_unix_nanos > frame.max_timestamp_unix_nanos
        {
            return Err(codec("invalid compressed ingest group"));
        }
        if records as usize > self.limits.max_records
            || self.observed_records > self.limits.max_records as u64
            || structural_bytes > self.limits.max_structural_bytes_per_group
        {
            return Err(Failure::Limits);
        }
        self.checksum()?;
        self.prepare_index_input()?;
        let records = self.current.as_ref().ok_or(Failure::State)?.record_count;
        self.observed("index_input")?;
        let input = self.index_input.take().ok_or(Failure::State)?;
        let actual =
            EmbeddedFrameIndex::decode_until(input, records, self.limits.index, &self.work);
        self.current.as_mut().ok_or(Failure::State)?.index = Some(actual);
        self.observed("index_result")?;
        if self
            .current
            .as_ref()
            .ok_or(Failure::State)?
            .index_failure()
            .is_some()
        {
            return Err(Failure::Index);
        }
        if self
            .current
            .as_ref()
            .and_then(PreparedIngestFrame::embedded_index)
            .is_none_or(|i| i.record_count() != records)
        {
            return Err(codec("compressed ingest frame index record count mismatch"));
        }
        self.pay(0, 0, size_of::<PreparedIngestFrame>())?;
        self.frames.push(self.current.take().ok_or(Failure::State)?);
        self.observed("frame")
    }
    fn prepare_index_input(&mut self) -> Result<()> {
        let live_context = self
            .context
            .as_ref()
            .is_some_and(|c| c.starts_with(TRANSIENT_PACK_MAGIC));
        if live_context {
            self.context_group()?;
        }
        let copy = add(size_of::<Bytes>(), BYTES_PROMOTION_BYTES)?;
        self.pay(
            copy,
            2,
            add(copy, mul(4, size_of::<PreparedIngestFrame>())?)?,
        )?;
        if live_context {
            let range = self
                .current
                .as_ref()
                .ok_or(Failure::State)?
                .embedded
                .clone()
                .ok_or(Failure::State)?;
            self.index_input = Some(self.context.as_ref().ok_or(Failure::State)?.slice(range));
            return Ok(());
        }
        let frame = self.current.as_ref().ok_or(Failure::State)?;
        let (range, bytes, records) = (
            frame.compressed.clone(),
            frame.structural_bytes,
            frame.record_count,
        );
        self.decompression_input = Some(self.payload.slice(range));
        self.observed("decompression-input")?;
        let actual = decompress_structural_frame_until(
            self.decompression_input.take().ok_or(Failure::State)?,
            bytes,
            records,
            self.limits
                .decompression
                .ok_or(Failure::DecompressionUnsupported)?,
            &self.work,
        );
        self.current.as_mut().ok_or(Failure::State)?.decompressed = Some(actual);
        self.observed("decompressed")?;
        self.pay(copy, 2, copy)?;
        let child = self
            .current
            .as_ref()
            .ok_or(Failure::State)?
            .decompressed
            .as_ref()
            .and_then(|r| r.as_ref().ok())
            .ok_or(Failure::Decompression)?;
        self.index_input = Some(
            child
                .structural()
                .ok_or(Failure::State)?
                .slice(child.index_range().ok_or(Failure::State)?),
        );
        Ok(())
    }
    fn checksum(&mut self) -> Result<()> {
        let range = self
            .current
            .as_ref()
            .ok_or(Failure::State)?
            .compressed
            .clone();
        let total = add(self.usage.checksum_bytes, range.len())?;
        if total > self.limits.max_checksum_bytes {
            return Err(Failure::Limits);
        }
        self.pay(0, 0, size_of::<blake3::Hasher>())?;
        self.hasher = Some(blake3::Hasher::new());
        self.hashed_through = range.start;
        while self.hashed_through < range.end {
            let end = self
                .hashed_through
                .saturating_add(HASH_CHUNK)
                .min(range.end);
            let n = end - self.hashed_through;
            self.pay(0, 0, n)?;
            self.hasher
                .as_mut()
                .ok_or(Failure::State)?
                .update(&self.payload[self.hashed_through..end]);
            self.hashed_through = end;
            self.usage.checksum_bytes = add(self.usage.checksum_bytes, n)?;
            self.observed("checksum_chunk")?;
        }
        self.pay(0, 0, add(size_of::<blake3::Hasher>(), 64)?)?;
        let digest = self.hasher.as_ref().ok_or(Failure::State)?.finalize();
        let checksum = u32::from_le_bytes(
            digest.as_bytes()[..4]
                .try_into()
                .map_err(|_| Failure::State)?,
        );
        self.current.as_mut().ok_or(Failure::State)?.checksum = Some(checksum);
        self.observed("checksum")?;
        if checksum
            != self
                .current
                .as_ref()
                .ok_or(Failure::State)?
                .expected_checksum
        {
            return Err(codec("invalid compressed ingest group"));
        }
        Ok(())
    }
    fn context_group(&mut self) -> Result<()> {
        self.pay(0, 0, TRANSIENT_GROUP_HEADER_BYTES)?;
        let context = self.context.as_ref().ok_or(Failure::State)?;
        let end = add(self.context_cursor, TRANSIENT_GROUP_HEADER_BYTES)?;
        let header = context
            .get(self.context_cursor..end)
            .ok_or_else(|| codec("transient ingest group header is truncated"))?;
        let payload_end = end
            .checked_add(u32_at(header, 16)? as usize)
            .filter(|end| *end <= context.len())
            .ok_or_else(|| codec("transient ingest index payload is truncated"))?;
        let frame = self.current.as_mut().ok_or(Failure::State)?;
        frame.embedded = Some(end..payload_end);
        self.context_cursor = payload_end;
        if u64_at(header, 0)? != frame.cohort.get()
            || u32_at(header, 8)? != frame.record_count
            || u32_at(header, 12)? as usize != frame.structural_bytes
        {
            return Err(codec("transient ingest group disagrees with durable group"));
        }
        self.observed("context")
    }
}
fn u16_at(bytes: &[u8], at: usize) -> Result<u16> {
    Ok(u16::from_le_bytes(
        bytes
            .get(at..at + 2)
            .ok_or(Failure::State)?
            .try_into()
            .map_err(|_| Failure::State)?,
    ))
}
fn u32_at(bytes: &[u8], at: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(at..at + 4)
            .ok_or(Failure::State)?
            .try_into()
            .map_err(|_| Failure::State)?,
    ))
}
fn u64_at(bytes: &[u8], at: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(at..at + 8)
            .ok_or(Failure::State)?
            .try_into()
            .map_err(|_| Failure::State)?,
    ))
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
pub(super) fn observe_decompression(expiry: std::time::Instant, phase: &'static str) {
    tests::after_value(expiry, phase, 0);
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests;
