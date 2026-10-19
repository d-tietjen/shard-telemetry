//! Retained streaming decompression of one actual durable structural frame.
use crate::structural::structural_embedded_index_range;
use crate::{TelemetryError, TelemetryResult};
use bytes::Bytes;
use shard_stream_engine::{NativeResumeRefusal, NativeResumeWork};
use std::any::Any;
use std::collections::TryReserveError;
use std::error::Error;
use std::fmt;
#[cfg(unix)]
use std::mem::size_of_val;
use std::mem::{size_of, take};
use std::ops::Range;
use std::panic::{AssertUnwindSafe, catch_unwind};
use zstd::zstd_safe::{DCtx, DParameter, InBuffer, OutBuffer};

/// Additional recovery policy, sharing the original native work and deadline.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FrameDecompressionLimits {
    pub(crate) max_input_bytes: usize,
    pub(crate) max_output_bytes: usize,
    pub(crate) input_chunk_bytes: usize,
    pub(crate) output_chunk_bytes: usize,
    pub(crate) max_stream_calls: usize,
    /// Native buffered-window policy, set before compressed input. A native
    /// whole-frame shortcut can use the fixed destination without that buffer;
    /// this is not a maximum accepted frame-header window value.
    pub(crate) max_window_log: u32,
    /// Prepaid native context reservation; measured excess admission is
    /// attempted before further work. Refusal retains its actual size/context.
    /// Neither this reservation nor sizeof measures allocator RSS.
    pub(crate) max_decoder_bytes: usize,
    pub(crate) max_declared_bytes: usize,
    pub(crate) max_entries: usize,
    pub(crate) max_work_bytes: usize,
}
#[derive(Debug)]
pub(super) enum Failure<R = NativeResumeRefusal> {
    Refused(R),
    Limits,
    Allocation(TryReserveError),
    Native,
    Codec(TelemetryError),
    Panic,
    State,
}
type Result<T, R = NativeResumeRefusal> = std::result::Result<T, Failure<R>>;

/// Mandatory admission at the existing native codec boundaries. A concrete
/// owner is monomorphized into the attempt; the native path gains no wrapper,
/// trait object, storage field or alternative reservation policy.
pub(super) trait FrameWork {
    type Refusal: Error + 'static;
    fn check(&mut self) -> std::result::Result<(), Self::Refusal>;
    fn pay(
        &mut self,
        bytes: usize,
        entries: usize,
        work: usize,
    ) -> std::result::Result<(), Self::Refusal>;
    #[cfg(unix)]
    fn original_expiry(&self) -> std::time::Instant;
}
impl FrameWork for NativeResumeWork {
    type Refusal = NativeResumeRefusal;
    fn check(&mut self) -> std::result::Result<(), Self::Refusal> {
        NativeResumeWork::check(self)
    }
    fn pay(
        &mut self,
        bytes: usize,
        entries: usize,
        work: usize,
    ) -> std::result::Result<(), Self::Refusal> {
        self.reserve_values(bytes, 0, entries, work)
    }
    #[cfg(unix)]
    fn original_expiry(&self) -> std::time::Instant {
        self.absolute_expiry()
    }
}
fn add<R>(a: usize, b: usize) -> Result<usize, R> {
    a.checked_add(b).ok_or(Failure::Limits)
}
fn mul<R>(a: usize, b: usize) -> Result<usize, R> {
    a.checked_mul(b).ok_or(Failure::Limits)
}
fn codec<R>(message: &'static str) -> Failure<R> {
    Failure::Codec(TelemetryError::InvalidBlockEncoding(message))
}
struct Step {
    result: std::result::Result<usize, usize>,
    read: usize,
    written: usize,
}
struct Attempt<W: FrameWork = NativeResumeWork> {
    input: Bytes,
    expected_bytes: usize,
    expected_records: u32,
    output: Vec<u8>,
    structural: Option<Bytes>,
    section: Option<TelemetryResult<(usize, Range<usize>)>>,
    steps: Vec<Step>,
    consumed: usize,
    produced: usize,
    frame_start: bool,
    frame_magic: Option<u32>,
    creation_entered: bool,
    init: Option<std::result::Result<usize, usize>>,
    parameter: Option<std::result::Result<usize, usize>>,
    observed_decoder_bytes: usize,
    prepaid_decoder_bytes: usize,
    declared: usize,
    entries: usize,
    work_bytes: usize,
    panic: Option<Box<dyn Any + Send>>,
    limits: FrameDecompressionLimits,
    work: W,
    // Actual native state outlives its results and all produced bytes.
    context: Option<DCtx<'static>>,
}
/// Complete parent and actual child outcomes, with one original work owner.
/// Child state contains no reference back to this owner or any native task.
#[cfg(unix)]
pub(super) struct FrameContinuation<W: FrameWork, C, E> {
    pub(super) child: C,
    pub(super) initial: Option<std::result::Result<(), W::Refusal>>,
    pub(super) actual: Option<std::result::Result<(), E>>,
    pub(super) post: Option<std::result::Result<(), W::Refusal>>,
    pub(super) panic: Option<Box<dyn Any + Send>>,
    pub(super) entered: bool,
    // Drop the complete parent/native context after every child/result owner.
    pub(super) parent: PreparedDecompressedFrame<W>,
}
#[cfg(unix)]
impl<W: FrameWork, C, E> FrameContinuation<W, C, E> {
    pub(super) fn accepted(&self) -> bool {
        self.entered
            && self.panic.is_none()
            && self.initial.as_ref().is_some_and(|result| result.is_ok())
            && self.actual.as_ref().is_some_and(|result| result.is_ok())
            && self.post.as_ref().is_some_and(|result| result.is_ok())
    }
}
pub(super) struct PreparedDecompressedFrame<W: FrameWork = NativeResumeWork> {
    retained: Attempt<W>,
}
pub(super) struct RetainedFrameDecompressionError<W: FrameWork = NativeResumeWork> {
    pub(super) cause: Failure<W::Refusal>,
    retained: Attempt<W>,
}
impl<W: FrameWork> PreparedDecompressedFrame<W> {
    #[cfg(unix)]
    pub(super) fn expected_records(&self) -> u32 {
        self.retained.expected_records
    }
    #[cfg(unix)]
    pub(super) fn has_native_context(&self) -> bool {
        self.retained.context.is_some()
    }
    #[cfg(unix)]
    pub(super) fn continue_until<C, E>(
        self,
        child: C,
        operation: impl FnOnce(&Bytes, &mut W, &mut C, Option<usize>) -> std::result::Result<(), E>,
    ) -> FrameContinuation<W, C, E> {
        let fixed = size_of::<FrameContinuation<W, C, E>>()
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(size_of_val(&operation)))
            .filter(|bytes| *bytes <= isize::MAX as usize);
        let mut continued = FrameContinuation {
            parent: self,
            child,
            initial: None,
            actual: None,
            post: None,
            panic: None,
            entered: false,
        };
        let actual = catch_unwind(AssertUnwindSafe(|| {
            continued.initial = Some(continued.parent.retained.work.check());
            if continued
                .initial
                .as_ref()
                .is_some_and(|result| result.is_ok())
                && let Some(input) = &continued.parent.retained.structural
            {
                continued.entered = true;
                continued.actual = Some(operation(
                    input,
                    &mut continued.parent.retained.work,
                    &mut continued.child,
                    fixed,
                ));
                // The real child Result and complete parent already belong to
                // continued before any observer or original-E postcheck.
                #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
                super::bounded_frames::observe_decompression(
                    continued.parent.retained.work.original_expiry(),
                    "structural-child-result",
                );
            }
        }));
        if let Err(panic) = actual {
            continued.panic = Some(panic);
        }
        continued.post = Some(continued.parent.retained.work.check());
        continued
    }
    #[cfg(unix)]
    pub(super) fn work(&self) -> &W {
        &self.retained.work
    }
    #[cfg(unix)]
    #[allow(
        clippy::result_large_err,
        reason = "retain the original work and native outcomes inline without a refusal allocation"
    )]
    pub(super) fn next_until(
        self,
        input: Bytes,
        expected_bytes: usize,
        expected_records: u32,
        limits: FrameDecompressionLimits,
    ) -> std::result::Result<Self, RetainedFrameDecompressionError<W>> {
        // Only a successful, physically completed frame can continue. Release
        // its completed native/data owners before entering the next frame;
        // move the exact cumulative work owner without an extractor or reset.
        let Attempt {
            work,
            context,
            input: previous,
            output,
            structural,
            section,
            steps,
            panic,
            ..
        } = self.retained;
        drop((previous, output, structural, section, steps, panic, context));
        decompress_frame_with_work(input, expected_bytes, expected_records, limits, work)
    }
    pub(super) fn structural(&self) -> Option<&Bytes> {
        self.retained.structural.as_ref()
    }
    pub(super) fn index_range(&self) -> Option<Range<usize>> {
        self.retained
            .section
            .as_ref()?
            .as_ref()
            .ok()
            .map(|(_, r)| r.clone())
    }
    #[allow(
        dead_code,
        reason = "preserve read-only decoder custody diagnostics without a mutable owner extractor"
    )]
    pub(super) fn stream_calls(&self) -> usize {
        self.retained.steps.len()
    }
    #[allow(
        dead_code,
        reason = "preserve read-only decoder custody diagnostics without a mutable owner extractor"
    )]
    pub(super) fn work_bytes(&self) -> usize {
        self.retained.work_bytes
    }
}
impl<W: FrameWork> fmt::Debug for PreparedDecompressedFrame<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedDecompressedFrame")
            .field("produced", &self.retained.produced)
            .field("stream_calls", &self.retained.steps.len())
            .finish_non_exhaustive()
    }
}
impl<W: FrameWork> RetainedFrameDecompressionError<W> {
    #[cfg(unix)]
    pub(super) fn work(&self) -> &W {
        &self.retained.work
    }
    #[allow(
        dead_code,
        reason = "preserve read-only decoder custody diagnostics without a mutable owner extractor"
    )]
    pub(super) fn output_prefix(&self) -> &[u8] {
        let output = self
            .retained
            .structural
            .as_deref()
            .unwrap_or(&self.retained.output);
        &output[..self.retained.produced.min(output.len())]
    }
    #[allow(
        dead_code,
        reason = "preserve read-only decoder custody diagnostics without a mutable owner extractor"
    )]
    pub(super) fn stream_calls(&self) -> usize {
        self.retained.steps.len()
    }
    #[allow(
        dead_code,
        reason = "preserve read-only decoder custody diagnostics without a mutable owner extractor"
    )]
    pub(super) fn has_native_context(&self) -> bool {
        self.retained.context.is_some()
    }
    #[allow(
        dead_code,
        reason = "preserve read-only decoder custody diagnostics without a mutable owner extractor"
    )]
    pub(super) fn native_error(&self) -> Option<usize> {
        self.retained
            .init
            .as_ref()
            .and_then(|r| r.as_ref().err().copied())
            .or_else(|| {
                self.retained
                    .parameter
                    .as_ref()
                    .and_then(|r| r.as_ref().err().copied())
            })
            .or_else(|| {
                self.retained
                    .steps
                    .iter()
                    .find_map(|s| s.result.as_ref().err().copied())
            })
    }
    #[allow(
        dead_code,
        reason = "preserve read-only decoder custody diagnostics without a mutable owner extractor"
    )]
    pub(super) fn has_panic(&self) -> bool {
        self.retained.panic.is_some()
    }
}
impl<W: FrameWork> fmt::Debug for RetainedFrameDecompressionError<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedFrameDecompressionError")
            .field("cause", &self.cause)
            .field("consumed", &self.retained.consumed)
            .field("produced", &self.retained.produced)
            .field("stream_calls", &self.retained.steps.len())
            .field("creation_entered", &self.retained.creation_entered)
            .field("decoder_bytes", &self.retained.observed_decoder_bytes)
            .finish_non_exhaustive()
    }
}
impl<W: FrameWork> fmt::Display for RetainedFrameDecompressionError<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl<W: FrameWork> Error for RetainedFrameDecompressionError<W> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause {
            Failure::Refused(e) => Some(e),
            Failure::Allocation(e) => Some(e),
            Failure::Codec(e) => Some(e),
            _ => None,
        }
    }
}

/// Consume exact compressed bytes before checking E or entering native code.
/// Complete native Results, decoder state and output prefixes remain retained.
#[allow(
    clippy::result_large_err,
    reason = "retain complete original owners and native outcomes inline without a refusal allocation"
)]
pub(super) fn decompress_structural_frame_until(
    input: Bytes,
    expected_bytes: usize,
    expected_records: u32,
    limits: FrameDecompressionLimits,
    work: &NativeResumeWork,
) -> std::result::Result<PreparedDecompressedFrame, RetainedFrameDecompressionError> {
    decompress_frame_with_work(
        input,
        expected_bytes,
        expected_records,
        limits,
        work.clone(),
    )
}

#[allow(
    clippy::result_large_err,
    reason = "retain complete original owners and native outcomes inline without a refusal allocation"
)]
pub(super) fn decompress_frame_with_work<W: FrameWork>(
    input: Bytes,
    expected_bytes: usize,
    expected_records: u32,
    limits: FrameDecompressionLimits,
    work: W,
) -> std::result::Result<PreparedDecompressedFrame<W>, RetainedFrameDecompressionError<W>> {
    let mut retained = Attempt {
        input,
        expected_bytes,
        expected_records,
        output: Vec::new(),
        structural: None,
        section: None,
        steps: Vec::new(),
        consumed: 0,
        produced: 0,
        frame_start: true,
        frame_magic: None,
        creation_entered: false,
        init: None,
        parameter: None,
        observed_decoder_bytes: 0,
        prepaid_decoder_bytes: 0,
        declared: 0,
        entries: 0,
        work_bytes: 0,
        panic: None,
        limits,
        work,
        context: None,
    };
    let actual = catch_unwind(AssertUnwindSafe(|| retained.run()));
    let cause = match actual {
        Ok(Ok(())) => return Ok(PreparedDecompressedFrame { retained }),
        Ok(Err(cause)) => cause,
        Err(payload) => {
            retained.panic = Some(payload);
            Failure::Panic
        }
    };
    Err(RetainedFrameDecompressionError { cause, retained })
}
impl<W: FrameWork> Attempt<W> {
    fn pay(&mut self, bytes: usize, entries: usize, work: usize) -> Result<(), W::Refusal> {
        let d = add(self.declared, bytes)?;
        let e = add(self.entries, entries)?;
        let w = add(self.work_bytes, work)?;
        if d > self.limits.max_declared_bytes
            || e > self.limits.max_entries
            || w > self.limits.max_work_bytes
        {
            return Err(Failure::Limits);
        }
        self.work
            .pay(bytes, entries, work)
            .map_err(Failure::Refused)?;
        self.declared = d;
        self.entries = e;
        self.work_bytes = w;
        Ok(())
    }
    fn observed(&mut self, phase: &'static str) -> Result<(), W::Refusal> {
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        super::bounded_frames::observe_decompression(self.work.original_expiry(), phase);
        #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
        let _ = phase;
        self.work.check().map_err(Failure::Refused)
    }
    fn capture_context_size(&mut self) -> Result<(), W::Refusal> {
        self.observed_decoder_bytes = self.context.as_ref().ok_or(Failure::State)?.sizeof();
        Ok(())
    }
    fn admit_context_size(&mut self) -> Result<(), W::Refusal> {
        let extra = self
            .observed_decoder_bytes
            .saturating_sub(self.prepaid_decoder_bytes);
        if extra > 0 {
            // Native memory already exists; retain it even if charging excess
            // fails. Never infer absence of an effect from that refusal.
            self.pay(extra, 0, extra)?;
            self.prepaid_decoder_bytes = self.observed_decoder_bytes;
            return Err(Failure::Limits);
        }
        Ok(())
    }
    fn run(&mut self) -> Result<(), W::Refusal> {
        self.work.check().map_err(Failure::Refused)?;
        let l = self.limits;
        if [
            l.max_input_bytes,
            l.max_output_bytes,
            l.input_chunk_bytes,
            l.output_chunk_bytes,
            l.max_stream_calls,
            l.max_decoder_bytes,
            l.max_declared_bytes,
            l.max_entries,
            l.max_work_bytes,
        ]
        .into_iter()
        .any(|n| n == 0 || n > isize::MAX as usize)
            || l.max_stream_calls > 65536
            || l.input_chunk_bytes > 1 << 20
            || l.output_chunk_bytes > 1 << 20
            || !(10..=31).contains(&l.max_window_log)
            || l.max_window_log >= usize::BITS
            || self.input.is_empty()
            || self.input.len() > l.max_input_bytes
            || self.expected_bytes == 0
            || self.expected_bytes > l.max_output_bytes
            || self.expected_records == 0
        {
            return Err(Failure::Limits);
        }
        // The extra initialized byte detects an understated structural length
        // without letting native code grow its destination.
        let output = add(self.expected_bytes, 1)?;
        let steps = mul(l.max_stream_calls, size_of::<Step>())?;
        let fixed = add(
            size_of::<Self>(),
            add(
                size_of::<PreparedDecompressedFrame<W>>(),
                size_of::<RetainedFrameDecompressionError<W>>(),
            )?,
        )?;
        self.pay(add(self.input.len(), fixed)?, 3, fixed)?;
        self.pay(
            add(output, steps)?,
            add(output, l.max_stream_calls)?,
            add(output, steps)?,
        )?;
        self.output
            .try_reserve_exact(output)
            .map_err(Failure::Allocation)?;
        self.pay(
            self.output.capacity().saturating_sub(output),
            self.output.capacity().saturating_sub(output),
            0,
        )?;
        self.output.resize(output, 0);
        self.observed("decompression-output")?;
        self.steps
            .try_reserve_exact(l.max_stream_calls)
            .map_err(Failure::Allocation)?;
        let extra = self.steps.capacity().saturating_sub(l.max_stream_calls);
        self.pay(mul(extra, size_of::<Step>())?, extra, 0)?;
        self.pay(l.max_decoder_bytes, 1, l.max_decoder_bytes)?;
        self.prepaid_decoder_bytes = l.max_decoder_bytes;
        self.creation_entered = true;
        self.context = DCtx::try_create();
        if self.context.is_none() {
            return Err(Failure::Native);
        }
        self.capture_context_size()?;
        self.observed("decompression-context")?;
        self.admit_context_size()?;
        self.pay(0, 0, size_of::<DCtx<'static>>())?;
        self.init = Some(self.context.as_mut().ok_or(Failure::Native)?.init());
        self.observed("decompression-init")?;
        if self.init.as_ref().is_some_and(|r| r.is_err()) {
            return Err(Failure::Native);
        }
        self.pay(0, 0, size_of::<DParameter>())?;
        self.parameter = Some(
            self.context
                .as_mut()
                .ok_or(Failure::State)?
                .set_parameter(DParameter::WindowLogMax(l.max_window_log)),
        );
        self.observed("decompression-parameter")?;
        if self.parameter.as_ref().is_some_and(|r| r.is_err()) {
            return Err(Failure::Native);
        }
        loop {
            if self.steps.len() >= l.max_stream_calls {
                return Err(Failure::Limits);
            }
            if self.frame_start {
                self.pay(0, 0, 4)?;
                let end = add(self.consumed, 4)?;
                self.frame_magic = Some(u32::from_le_bytes(
                    self.input
                        .get(self.consumed..end)
                        .ok_or_else(|| codec("truncated zstd frame magic"))?
                        .try_into()
                        .map_err(|_| Failure::State)?,
                ));
                let magic = self.frame_magic.ok_or(Failure::State)?;
                // The selected legacy decoder bypasses WindowLogMax and its
                // allocation is absent from DCtx::sizeof. Never enter it.
                if magic != 0xFD2F_B528 && magic & 0xFFFF_FFF0 != 0x184D_2A50 {
                    return Err(codec(
                        "bounded recovery requires modern or skippable zstd frames",
                    ));
                }
                self.frame_start = false;
            }
            let input_end = add(self.consumed, l.input_chunk_bytes)?.min(self.input.len());
            let output_end = add(self.produced, l.output_chunk_bytes)?.min(self.output.len());
            let read_limit = input_end - self.consumed;
            let write_limit = output_end - self.produced;
            self.pay(
                0,
                0,
                add(
                    mul(add(read_limit, write_limit)?, 4)?,
                    add(size_of::<Step>(), size_of::<DCtx<'static>>())?,
                )?,
            )?;
            let mut input = InBuffer::around(&self.input[self.consumed..input_end]);
            let mut output = OutBuffer::around(&mut self.output[self.produced..output_end]);
            let actual = self
                .context
                .as_mut()
                .ok_or(Failure::State)?
                .decompress_stream(&mut output, &mut input);
            let read = input.pos();
            let written = output.pos();
            self.steps.push(Step {
                result: actual,
                read,
                written,
            });
            self.consumed = add(self.consumed, read)?;
            self.produced = add(self.produced, written)?;
            self.capture_context_size()?;
            self.observed("decompression-step")?;
            self.admit_context_size()?;
            let step = self.steps.last().ok_or(Failure::State)?;
            let hint = step.result.as_ref().copied().map_err(|_| Failure::Native)?;
            if self.produced > self.expected_bytes {
                return Err(codec("compressed ingest structural length mismatch"));
            }
            if hint == 0 {
                if self.consumed == self.input.len() {
                    if self.produced != self.expected_bytes {
                        return Err(codec("compressed ingest structural extent mismatch"));
                    }
                    break;
                }
                if step.read == 0 && step.written == 0 {
                    return Err(codec("compressed ingest structural extent mismatch"));
                }
                // Like the ordinary bulk decoder, accept concatenated frames
                // within the same finite input/output extent and call ceiling.
                self.frame_start = true;
                continue;
            }
            if step.read == 0 && step.written == 0 {
                return Err(codec("compressed ingest frame made no progress"));
            }
        }
        self.pay(3 * size_of::<usize>(), 1, size_of::<Bytes>())?;
        self.output.truncate(self.produced);
        self.structural = Some(Bytes::from(take(&mut self.output)));
        self.observed("decompression-bytes")?;
        self.pay(size_of::<TelemetryResult<(usize, Range<usize>)>>(), 1, 128)?;
        self.section = Some(structural_embedded_index_range(
            self.structural.as_ref().ok_or(Failure::State)?,
        ));
        self.observed("decompression-section")?;
        let (records, _) = self
            .section
            .as_ref()
            .and_then(|r| r.as_ref().ok())
            .ok_or_else(|| codec("invalid decompressed structural sections"))?;
        if *records != self.expected_records as usize {
            return Err(codec("compressed ingest structural record count mismatch"));
        }
        Ok(())
    }
}
