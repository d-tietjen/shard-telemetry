//! Full-scope message bytes on the same complete packed/native recovery owner.
use super::bounded_decompression::{
    FrameContinuation, FrameParent, FrameWork, PreparedDecompressedFrame,
};
use super::recovery_decompression::RecoveryFrameWork;
use super::recovery_packed_metadata::PreparedRecoveredPackedMetadata;
use super::recovery_sections::RecoveredStructuralSection;
use crate::structural::{
    BodyInput, BodyRecord, BodyState, CoreFailure, FragmentHooks, HelperResult, LaneHooks,
    LaneState, TemplateHooks, TemplateState, WireHooks, WireSource, WireState, replay_body,
    scan_body_record, visit_lane, visit_templates,
};
use crate::{EmbeddedFrameIndex, TelemetryError, TelemetryRecoveryError, TelemetryRecoveryUsage};
use bytes::Bytes;
use std::collections::TryReserveError;
use std::error::Error;
use std::fmt;
use std::mem::{size_of, size_of_val};
use std::ops::Range;
use std::str::Utf8Error;
use std::time::Instant;

/// Finite full-frame template/body limits on the already-spent recovery work.
#[derive(Clone, Copy, Debug)]
pub struct RecoveredMessageLimits {
    /// Sum of actual encoded template and body section lengths.
    pub max_input_bytes: usize,
    /// Authoritative structural row count, never a wire allocation hint.
    pub max_records: usize,
    /// Actual declared templates.
    pub max_templates: usize,
    /// Sum of all literal entries.
    pub max_literals: usize,
    /// Actual checkpoint directory entries; any positive interval is supported.
    pub max_checkpoints: usize,
    /// Cumulative complete message bytes presented to UTF8 validation.
    pub max_message_bytes: usize,
    /// Cumulative requested output/carrier storage, including observed excess.
    pub max_output_bytes: usize,
    /// Cumulative admitted allocation/framing/entry slots, not JSON records.
    pub max_slots: usize,
    /// Explicit helper/scalar/scan/copy work, also spent on the original work.
    pub max_work_bytes: usize,
}
/// Source of an actual complete or in-progress message-byte range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveredMessageBytes {
    /// Borrowed literal in the original template section.
    Template(Range<usize>),
    /// Borrowed raw bytes in the original body payload.
    Body(Range<usize>),
    /// Initialized bytes in this child's retained reconstruction buffer.
    Reconstructed(Range<usize>),
}
/// Actual additional declared charges and produced progress for this child.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveredMessageUsage {
    /// Complete actual continuation/return/closure framing.
    pub continuation_bytes: usize,
    /// Cumulative output requests, including carrier and capacity excess.
    pub output_bytes: usize,
    /// Cumulative admitted local slots.
    pub slots: usize,
    /// Cumulative declared helper/scan/copy work.
    pub work_bytes: usize,
    /// Actual text bytes entered into UTF8 validation.
    pub message_bytes: usize,
    /// Actual completed UTF8-valid rows retained in order.
    pub rows: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    Limits,
    Recovery,
    Allocation,
    Utf8,
    State,
}
#[derive(Default)]
struct Charges {
    usage: RecoveredMessageUsage,
    recovery: Option<TelemetryRecoveryError>,
    reservation: Option<Result<(), TryReserveError>>,
    reservation_entered: bool,
    reservation_request: Option<usize>,
    reservation_old_capacity: Option<usize>,
    reservation_capacity: Option<usize>,
    failure: Option<Failure>,
}
impl Charges {
    fn fail(&mut self, why: Failure) -> CoreFailure {
        if self.failure.is_none() {
            self.failure = Some(why);
        }
        CoreFailure::Refused
    }
}
#[derive(Default)]
struct Data {
    templates: Vec<Range<usize>>,
    literals: Vec<Range<usize>>,
    current_template: usize,
    messages: Vec<RecoveredMessageBytes>,
    reconstructed: Vec<u8>,
    current: Option<RecoveredMessageBytes>,
    delivered_ordinal: Option<usize>,
    utf8: Option<Result<(), Utf8Error>>,
    dynamic_template: Option<usize>,
    output_start: usize,
}
struct State {
    limits: RecoveredMessageLimits,
    record_count: usize,
    setup_bytes: Option<usize>,
    template_range: Option<Range<usize>>,
    body_range: Option<Range<usize>>,
    templates: TemplateState,
    lane: LaneState,
    body: BodyState,
    wire: WireState,
    data: Data,
    charges: Charges,
}
struct Run<'a> {
    limits: RecoveredMessageLimits,
    ordinal: usize,
    owner: &'a mut RecoveryFrameWork,
    charges: &'a mut Charges,
    data: &'a mut Data,
    template_input: &'a [u8],
    body_input: &'a [u8],
}
impl Run<'_> {
    fn pay(&mut self, bytes: usize, entries: usize, work: usize) -> Result<(), CoreFailure> {
        pay(self.owner, self.charges, self.limits, bytes, entries, work)
    }
    fn observed(&mut self, phase: &'static str) -> Result<(), CoreFailure> {
        observed(self.owner, self.charges, phase)
    }
    fn deliver(&mut self, value: RecoveredMessageBytes) -> Result<(), CoreFailure> {
        self.data.current = Some(value);
        self.data.delivered_ordinal = Some(self.ordinal);
        self.data.utf8 = None;
        self.observed("message-delivered")
    }
    fn copy(&mut self, bytes: &[u8]) -> Result<(), CoreFailure> {
        let frames = [
            size_of::<std::slice::Chunks<'_, u8>>(),
            size_of::<&[u8]>(),
            size_of::<usize>(),
            size_of::<RecoveredMessageBytes>(),
            size_of::<Option<RecoveredMessageBytes>>(),
            size_of::<[usize; 6]>(),
        ];
        let framing = frames
            .into_iter()
            .try_fold(0usize, usize::checked_add)
            .ok_or_else(|| self.charges.fail(Failure::Limits))?;
        self.pay(0, frames.len(), framing)?;
        for part in bytes.chunks(16 * 1024) {
            let desired = self
                .data
                .reconstructed
                .len()
                .checked_add(part.len())
                .ok_or_else(|| self.charges.fail(Failure::Limits))?;
            reserve(
                self.owner,
                self.charges,
                self.limits,
                &mut self.data.reconstructed,
                desired,
            )?;
            let work = part
                .len()
                .checked_add(size_of::<RecoveredMessageBytes>())
                .and_then(|n| n.checked_add(size_of::<Option<RecoveredMessageBytes>>()))
                .ok_or_else(|| self.charges.fail(Failure::Limits))?;
            self.pay(0, 1, work)?;
            self.data.reconstructed.extend_from_slice(part);
            self.data.current = Some(RecoveredMessageBytes::Reconstructed(
                self.data.output_start..self.data.reconstructed.len(),
            ));
            self.data.delivered_ordinal = Some(self.ordinal);
            self.observed("message-copy")?;
        }
        Ok(())
    }
    fn literal_range(&self, template: usize, literal: usize) -> Option<Range<usize>> {
        let row = self.data.templates.get(template)?;
        let index = row.start.checked_add(literal).filter(|i| *i < row.end)?;
        self.data.literals.get(index).cloned()
    }
    fn begin_dynamic(&mut self, template: usize) -> Result<(), CoreFailure> {
        self.data.dynamic_template = Some(template);
        self.data.output_start = self.data.reconstructed.len();
        self.deliver(RecoveredMessageBytes::Reconstructed(
            self.data.output_start..self.data.output_start,
        ))?;
        let range = self
            .literal_range(template, 0)
            .ok_or_else(|| self.charges.fail(Failure::State))?;
        let bytes = self
            .template_input
            .get(range)
            .ok_or_else(|| self.charges.fail(Failure::State))?;
        self.copy(bytes)
    }
    fn finish_row(&mut self) -> Result<(), CoreFailure> {
        let span = self
            .data
            .current
            .as_ref()
            .ok_or_else(|| self.charges.fail(Failure::State))?;
        let bytes = match span {
            RecoveredMessageBytes::Template(range) => self.template_input.get(range.clone()),
            RecoveredMessageBytes::Body(range) => self.body_input.get(range.clone()),
            RecoveredMessageBytes::Reconstructed(range) => {
                self.data.reconstructed.get(range.clone())
            }
        }
        .ok_or_else(|| self.charges.fail(Failure::State))?;
        let length = bytes.len();
        let total = self
            .charges
            .usage
            .message_bytes
            .checked_add(length)
            .ok_or_else(|| self.charges.fail(Failure::Limits))?;
        if total > self.limits.max_message_bytes {
            return Err(self.charges.fail(Failure::Limits));
        }
        // The complete source/destination already lives outside this call.
        // Charge the actual validation input and concrete returned result first.
        let map_utf8 = |_: &str| ();
        let utf8_frames = [
            size_of::<Result<&str, Utf8Error>>(),
            size_of::<Result<(), Utf8Error>>(),
            size_of_val(&map_utf8),
            size_of::<[usize; 4]>(),
        ];
        let effort = utf8_frames
            .into_iter()
            .try_fold(length, usize::checked_add)
            .ok_or_else(|| self.charges.fail(Failure::Limits))?;
        self.pay(0, utf8_frames.len(), effort)?;
        let bytes = match self.data.current.as_ref() {
            Some(RecoveredMessageBytes::Template(range)) => self.template_input.get(range.clone()),
            Some(RecoveredMessageBytes::Body(range)) => self.body_input.get(range.clone()),
            Some(RecoveredMessageBytes::Reconstructed(range)) => {
                self.data.reconstructed.get(range.clone())
            }
            None => None,
        }
        .ok_or_else(|| self.charges.fail(Failure::State))?;
        self.data.utf8 = Some(std::str::from_utf8(bytes).map(map_utf8));
        self.charges.usage.message_bytes = total;
        self.observed("message-utf8")?;
        if self.data.utf8.as_ref().is_some_and(|v| v.is_err()) {
            return Err(self.charges.fail(Failure::Utf8));
        }
        self.pay(
            0,
            1,
            size_of::<RecoveredMessageBytes>() + size_of::<Option<RecoveredMessageBytes>>(),
        )?;
        let span = self
            .data
            .current
            .clone()
            .ok_or_else(|| self.charges.fail(Failure::State))?;
        self.data.messages.push(span);
        self.charges.usage.rows = self.data.messages.len();
        self.observed("message-row")
    }
}
fn reserve<T>(
    owner: &mut RecoveryFrameWork,
    charges: &mut Charges,
    limits: RecoveredMessageLimits,
    output: &mut Vec<T>,
    desired: usize,
) -> Result<(), CoreFailure> {
    let frames = [
        size_of::<&mut RecoveryFrameWork>(),
        size_of::<&mut Charges>(),
        size_of::<RecoveredMessageLimits>(),
        size_of::<&mut Vec<T>>(),
        size_of::<usize>(),
        size_of::<Result<(), TryReserveError>>(),
        size_of::<[usize; 7]>(),
    ];
    let framing = frames
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .ok_or_else(|| charges.fail(Failure::Limits))?;
    pay(owner, charges, limits, 0, frames.len(), framing)?;
    if desired <= output.capacity() {
        return Ok(());
    }
    // New request state is explicit; no previous Result impersonates an entry.
    charges.reservation = None;
    charges.reservation_entered = false;
    charges.reservation_request = Some(desired);
    charges.reservation_old_capacity = Some(output.capacity());
    charges.reservation_capacity = None;
    let bytes = desired
        .checked_mul(size_of::<T>())
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or_else(|| charges.fail(Failure::Limits))?;
    let movement = output
        .capacity()
        .checked_mul(size_of::<T>())
        .and_then(|n| n.checked_add(bytes))
        .ok_or_else(|| charges.fail(Failure::Limits))?;
    pay(owner, charges, limits, bytes, desired, movement)?;
    let additional = desired
        .checked_sub(output.len())
        .ok_or_else(|| charges.fail(Failure::State))?;
    charges.reservation_entered = true;
    charges.reservation = Some(output.try_reserve_exact(additional));
    charges.reservation_capacity = Some(output.capacity());
    observed(owner, charges, "message-reservation")?;
    if charges.reservation.as_ref().is_some_and(|v| v.is_err()) {
        return Err(charges.fail(Failure::Allocation));
    }
    let excess = output.capacity().saturating_sub(desired);
    let bytes = excess
        .checked_mul(size_of::<T>())
        .ok_or_else(|| charges.fail(Failure::Limits))?;
    pay(owner, charges, limits, bytes, excess, bytes)
}
fn pay(
    owner: &mut RecoveryFrameWork,
    charges: &mut Charges,
    limits: RecoveredMessageLimits,
    bytes: usize,
    entries: usize,
    work: usize,
) -> Result<(), CoreFailure> {
    let next = charges
        .usage
        .output_bytes
        .checked_add(bytes)
        .zip(charges.usage.slots.checked_add(entries))
        .zip(charges.usage.work_bytes.checked_add(work));
    let Some(((output, slots), effort)) = next else {
        return Err(charges.fail(Failure::Limits));
    };
    if output > limits.max_output_bytes
        || slots > limits.max_slots
        || effort > limits.max_work_bytes
    {
        return Err(charges.fail(Failure::Limits));
    }
    if let Err(error) = owner.pay(bytes, entries, work) {
        if charges.recovery.is_none() {
            charges.recovery = Some(error);
        }
        return Err(charges.fail(Failure::Recovery));
    }
    charges.usage.output_bytes = output;
    charges.usage.slots = slots;
    charges.usage.work_bytes = effort;
    Ok(())
}
fn observed(
    owner: &mut RecoveryFrameWork,
    charges: &mut Charges,
    phase: &'static str,
) -> Result<(), CoreFailure> {
    #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
    super::bounded_frames::observe_decompression(owner.original_expiry(), phase);
    #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
    let _ = phase;
    if let Err(error) = owner.check() {
        if charges.recovery.is_none() {
            charges.recovery = Some(error);
        }
        return Err(charges.fail(Failure::Recovery));
    }
    Ok(())
}

struct Check<'a> {
    limits: RecoveredMessageLimits,
    owner: &'a mut RecoveryFrameWork,
    charges: &'a mut Charges,
}
impl WireHooks for Check<'_> {
    fn admit(&mut self, bytes: usize, entries: usize, work: usize) -> Result<(), CoreFailure> {
        pay(self.owner, self.charges, self.limits, bytes, entries, work)
    }
    fn after(&mut self, phase: &'static str) -> Result<(), CoreFailure> {
        observed(self.owner, self.charges, phase)
    }
}
impl WireHooks for Run<'_> {
    fn admit(&mut self, bytes: usize, entries: usize, work: usize) -> Result<(), CoreFailure> {
        self.pay(bytes, entries, work)
    }
    fn after(&mut self, phase: &'static str) -> Result<(), CoreFailure> {
        self.observed(phase)
    }
}
impl TemplateHooks for Run<'_> {
    fn templates(&mut self, count: usize) -> Result<(), CoreFailure> {
        if count > self.limits.max_templates {
            return Err(self.charges.fail(Failure::Limits));
        }
        reserve(
            self.owner,
            self.charges,
            self.limits,
            &mut self.data.templates,
            count,
        )
    }
    fn begin_template(&mut self, count: usize) -> Result<(), CoreFailure> {
        self.data.current_template = self.data.literals.len();
        let desired = self
            .data
            .literals
            .len()
            .checked_add(count)
            .ok_or_else(|| self.charges.fail(Failure::Limits))?;
        if desired > self.limits.max_literals {
            return Err(self.charges.fail(Failure::Limits));
        }
        reserve(
            self.owner,
            self.charges,
            self.limits,
            &mut self.data.literals,
            desired,
        )
    }
    fn literal(&mut self, range: Range<usize>) -> Result<(), CoreFailure> {
        self.pay(0, 1, size_of::<Range<usize>>())?;
        self.data.literals.push(range);
        self.observed("message-literal")
    }
    fn end_template(&mut self) -> Result<(), CoreFailure> {
        self.pay(0, 1, size_of::<Range<usize>>())?;
        self.data
            .templates
            .push(self.data.current_template..self.data.literals.len());
        self.observed("message-template")
    }
}
impl LaneHooks for Run<'_> {
    fn reserve_checkpoints(
        &mut self,
        output: &mut Vec<usize>,
        count: usize,
    ) -> Result<(), CoreFailure> {
        if count > self.limits.max_checkpoints {
            return Err(self.charges.fail(Failure::Limits));
        }
        reserve(self.owner, self.charges, self.limits, output, count)
    }
}
impl FragmentHooks for Run<'_> {
    fn body_fragment(&mut self, range: Range<usize>) -> Result<(), CoreFailure> {
        let bytes = self
            .body_input
            .get(range)
            .ok_or_else(|| self.charges.fail(Failure::State))?;
        self.copy(bytes)
    }
    fn template_fragment(&mut self, literal: usize) -> Result<(), CoreFailure> {
        let template = self
            .data
            .dynamic_template
            .ok_or_else(|| self.charges.fail(Failure::State))?;
        let range = self
            .literal_range(template, literal)
            .ok_or_else(|| self.charges.fail(Failure::State))?;
        let bytes = self
            .template_input
            .get(range)
            .ok_or_else(|| self.charges.fail(Failure::State))?;
        self.copy(bytes)
    }
}

impl State {
    fn run(
        &mut self,
        input: Option<&Bytes>,
        index: Option<&EmbeddedFrameIndex>,
        owner: &mut RecoveryFrameWork,
        fixed: Option<usize>,
    ) -> Result<(), CoreFailure> {
        let tables = [
            size_of::<Continued>(),
            size_of::<PreparedRecoveredMessages>(),
            size_of::<RetainedRecoveredMessageError>(),
            size_of::<MessageResult>(),
            size_of::<MessageResult>(),
            size_of::<Result<(), Box<dyn std::any::Any + Send>>>(),
            size_of::<Result<(), CoreFailure>>(),
            size_of::<Run<'_>>(),
            size_of::<Check<'_>>(),
            size_of::<BodyInput<'_>>(),
            size_of::<[usize; 9]>(), // the separate policy table below
            size_of::<Option<&Bytes>>(),
            size_of::<Option<&EmbeddedFrameIndex>>(),
            size_of::<&[u8]>(), // template_input binding
            size_of::<&[u8]>(), // body_input binding
            size_of::<&[u8]>(), // payload binding
            size_of::<[usize; 17]>(),
        ];
        let all = tables
            .into_iter()
            .try_fold(0usize, usize::checked_add)
            .ok_or_else(|| self.charges.fail(Failure::Limits))?;
        let supplied = size_of::<Continued>()
            .checked_mul(3)
            .ok_or_else(|| self.charges.fail(Failure::Limits))?;
        let framing = fixed
            .and_then(|n| self.setup_bytes.and_then(|setup| n.checked_add(setup)))
            .and_then(|n| n.checked_add(all.saturating_sub(supplied)))
            .filter(|n| *n <= isize::MAX as usize)
            .ok_or_else(|| self.charges.fail(Failure::Limits))?;
        pay(
            owner,
            &mut self.charges,
            self.limits,
            framing,
            tables.len(),
            framing,
        )?;
        self.charges.usage.continuation_bytes = framing;
        let policy = [
            self.limits.max_input_bytes,
            self.limits.max_records,
            self.limits.max_templates,
            self.limits.max_literals,
            self.limits.max_checkpoints,
            self.limits.max_message_bytes,
            self.limits.max_output_bytes,
            self.limits.max_slots,
            self.limits.max_work_bytes,
        ];
        if policy.iter().any(|n| *n == 0 || *n > isize::MAX as usize)
            || self.record_count > self.limits.max_records
        {
            return Err(self.charges.fail(Failure::Limits));
        }
        let input = input.ok_or_else(|| self.charges.fail(Failure::State))?;
        let index = index.ok_or_else(|| self.charges.fail(Failure::State))?;
        let template_input = self
            .template_range
            .as_ref()
            .and_then(|r| input.get(r.clone()))
            .ok_or_else(|| self.charges.fail(Failure::State))?;
        let body_input = self
            .body_range
            .as_ref()
            .and_then(|r| input.get(r.clone()))
            .ok_or_else(|| self.charges.fail(Failure::State))?;
        if template_input
            .len()
            .checked_add(body_input.len())
            .is_none_or(|n| n > self.limits.max_input_bytes)
        {
            return Err(self.charges.fail(Failure::Limits));
        }
        let mut run = Run {
            limits: self.limits,
            ordinal: 0,
            owner,
            charges: &mut self.charges,
            data: &mut self.data,
            template_input,
            body_input,
        };
        visit_templates(
            template_input,
            &mut self.templates,
            &mut self.wire,
            &mut run,
        )?;
        // Exact existing body-layout validation precedes lane directory parsing.
        self.wire.unit(
            || {
                crate::structural::validate_message_body_layout(
                    index,
                    self.templates.count,
                    self.record_count,
                )
            },
            &mut run,
        )?;
        visit_lane(
            body_input,
            self.record_count,
            &mut self.lane,
            &mut self.wire,
            &mut run,
        )?;
        let payload = &body_input[..self.lane.directory_start];
        reserve(
            run.owner,
            run.charges,
            run.limits,
            &mut run.data.messages,
            self.record_count,
        )?;
        for ordinal in 0..self.record_count {
            self.body.ordinal = ordinal;
            self.body.template = None;
            self.body.actual = None;
            run.ordinal = ordinal;
            // Prepay this arrival's argument/destination/association before
            // native wire entry can deliver the next body source.
            let capture = [
                size_of::<RecoveredMessageBytes>(),
                size_of::<Option<RecoveredMessageBytes>>(),
                size_of::<Option<usize>>(),
                size_of::<[usize; 4]>(),
            ];
            let capture_bytes = capture
                .into_iter()
                .try_fold(0usize, usize::checked_add)
                .ok_or_else(|| run.charges.fail(Failure::Limits))?;
            run.pay(0, capture.len(), capture_bytes)?;
            let counts = &run.data.templates;
            let mut check = Check {
                limits: run.limits,
                owner: &mut *run.owner,
                charges: &mut *run.charges,
            };
            scan_body_record(
                BodyInput {
                    payload,
                    index,
                    templates: self.templates.count,
                    interval: self.lane.interval,
                    checkpoints: &self.lane.checkpoints,
                },
                |id| counts[id].len(),
                &mut self.body,
                &mut self.wire,
                &mut check,
            )?;
            match self
                .body
                .actual
                .clone()
                .ok_or_else(|| run.charges.fail(Failure::State))?
            {
                BodyRecord::Raw(range) => run.deliver(RecoveredMessageBytes::Body(range))?,
                BodyRecord::Exact(template) => {
                    let range = run
                        .literal_range(template, 0)
                        .ok_or_else(|| run.charges.fail(Failure::State))?;
                    run.deliver(RecoveredMessageBytes::Template(range))?;
                }
                BodyRecord::Dynamic { template, encoded } => {
                    run.begin_dynamic(template)?;
                    let count = run.data.templates[template].len();
                    replay_body(
                        payload,
                        count,
                        encoded.start,
                        &mut self.body,
                        &mut self.wire,
                        &mut run,
                    )?;
                }
            }
            run.finish_row()?;
        }
        self.wire.unit(
            || crate::structural::require_message_payload_consumed(payload, self.body.cursor),
            &mut run,
        )
    }
}
type Continued =
    FrameContinuation<PreparedRecoveredPackedMetadata, State, CoreFailure, TelemetryRecoveryError>;
type MessageResult = Result<PreparedRecoveredMessages, RetainedRecoveredMessageError>;
/// Exact validated message bytes in full structural row order, with whole parents.
/// Fields, final models and query/serving ownership remain separate operations.
pub struct PreparedRecoveredMessages {
    held: Continued,
}
/// Terminal actual partial message/helper/UTF8 outcomes on original recovery work.
pub struct RetainedRecoveredMessageError {
    held: Continued,
}
impl FrameParent<RecoveryFrameWork> for PreparedRecoveredMessages {
    fn with_frame<T>(
        &mut self,
        operation: impl FnOnce(&mut PreparedDecompressedFrame<RecoveryFrameWork>) -> T,
    ) -> T {
        self.held.parent.with_frame(operation)
    }
}
macro_rules! views {
    () => {
        /// Actual completed row-byte descriptors; errors may retain a valid prefix.
        pub fn messages(&self) -> &[RecoveredMessageBytes] {
            &self.held.child.data.messages
        }
        /// Actual borrowed/reconstructed byte view, with no new scan or allocation.
        pub fn source_bytes(&self, value: &RecoveredMessageBytes) -> Option<&[u8]> {
            match value {
                RecoveredMessageBytes::Template(range) => self
                    .held
                    .parent
                    .parent_metadata()
                    .parent_index()
                    .parent_timestamps()
                    .parent_offsets()
                    .parent_sections()
                    .section_bytes(RecoveredStructuralSection::Templates)?
                    .get(range.clone()),
                RecoveredMessageBytes::Body(range) => self
                    .held
                    .parent
                    .parent_metadata()
                    .parent_index()
                    .parent_timestamps()
                    .parent_offsets()
                    .parent_sections()
                    .section_bytes(RecoveredStructuralSection::Bodies)?
                    .get(range.clone()),
                RecoveredMessageBytes::Reconstructed(range) => {
                    self.held.child.data.reconstructed.get(range.clone())
                }
            }
        }
        /// Actual initialized current message range, not an acceptance claim.
        pub fn current_message(&self) -> Option<&RecoveredMessageBytes> {
            self.held.child.data.current.as_ref()
        }
        /// Row associated with the retained delivered source and UTF8 result.
        /// A later attempted row can refuse before delivering any new source.
        pub fn current_message_ordinal(&self) -> Option<usize> {
            self.held.child.data.delivered_ordinal
        }
        /// Actual last complete-message UTF8 Result before a post-check/unwind.
        pub fn utf8_result(&self) -> Option<&Result<(), Utf8Error>> {
            self.held.child.data.utf8.as_ref()
        }
        /// Real last allocation Result retained before capacity/E observations.
        pub fn reservation_result(&self) -> Option<&Result<(), TryReserveError>> {
            self.held.child.charges.reservation.as_ref()
        }
        /// True only when the last tracked allocation request actually entered.
        pub fn reservation_entered(&self) -> bool {
            self.held.child.charges.reservation_entered
        }
        /// Declared target capacity of the last tracked allocation attempt.
        pub fn reservation_request(&self) -> Option<usize> {
            self.held.child.charges.reservation_request
        }
        /// Actual old allocation capacity recorded before a possible growth.
        pub fn reservation_old_capacity(&self) -> Option<usize> {
            self.held.child.charges.reservation_old_capacity
        }
        /// Actual returned capacity, saved before the allocator-result hook.
        pub fn reservation_capacity(&self) -> Option<usize> {
            self.held.child.charges.reservation_capacity
        }
        /// Actual last native scalar helper result; None identifies another helper.
        pub fn scalar_result(&self) -> Option<&crate::TelemetryResult<usize>> {
            match self.held.child.wire.actual.as_ref() {
                Some(HelperResult::Scalar(r)) => Some(r),
                _ => None,
            }
        }
        /// Actual last borrowed-byte extent helper result, including its real error.
        pub fn range_result(&self) -> Option<&crate::TelemetryResult<Range<usize>>> {
            match self.held.child.wire.actual.as_ref() {
                Some(HelperResult::Bytes(r)) => Some(r),
                _ => None,
            }
        }
        /// Actual last native unit check Result, before any later observation.
        pub fn helper_result(&self) -> Option<&crate::TelemetryResult<()>> {
            match self.held.child.wire.actual.as_ref() {
                Some(HelperResult::Unit(r)) => Some(r),
                _ => None,
            }
        }
        /// Actual last real index layout lookup result, not a reconstructed index.
        pub fn template_id_result(&self) -> Option<&crate::TelemetryResult<Option<usize>>> {
            match self.held.child.wire.actual.as_ref() {
                Some(HelperResult::Template(r)) => Some(r),
                _ => None,
            }
        }
        /// Original section of the last entered scalar/range helper.
        pub fn helper_section(&self) -> Option<RecoveredStructuralSection> {
            match self.held.child.wire.source? {
                WireSource::Templates => Some(RecoveredStructuralSection::Templates),
                WireSource::Directory | WireSource::Bodies => {
                    Some(RecoveredStructuralSection::Bodies)
                }
            }
        }
        /// True means the actual scalar cursor is directory-relative.
        pub fn helper_is_directory(&self) -> bool {
            self.held.child.wire.source == Some(WireSource::Directory)
        }
        /// Original helper error, kept separately from later original-work refusal.
        pub fn helper_error(&self) -> Option<&TelemetryError> {
            self.held.child.wire.native_error()
        }
        /// Last byte-reading helper cursor; non-position helpers leave it intact.
        /// Native length-prefix failure never rolls it back.
        pub fn helper_cursor(&self) -> usize {
            self.held.child.wire.cursor
        }
        /// Actual copied literal-range prefix into the retained template bytes.
        pub fn literal_ranges(&self) -> &[Range<usize>] {
            &self.held.child.data.literals
        }
        /// Current attempted body ordinal, not a completed/delivered-row count.
        /// See current_message_ordinal for the last delivered source's identity.
        pub fn current_ordinal(&self) -> usize {
            self.held.child.body.ordinal
        }
        /// Actual index-selected template for the current body attempt.
        pub fn current_template_id(&self) -> Option<usize> {
            self.held.child.body.template
        }
        /// Actual complete raw/dynamic body-record extent before cache/output work.
        pub fn current_body_range(&self) -> Option<Range<usize>> {
            match self.held.child.body.actual.as_ref()? {
                BodyRecord::Raw(r) | BodyRecord::Dynamic { encoded: r, .. } => Some(r.clone()),
                BodyRecord::Exact(_) => None,
            }
        }
        /// Actual checkpoint prefix, retained even during directory refusal.
        pub fn checkpoints(&self) -> &[usize] {
            &self.held.child.lane.checkpoints
        }
        /// Actual initialized reconstruction bytes, including an unaccepted row.
        pub fn reconstructed_bytes(&self) -> &[u8] {
            &self.held.child.data.reconstructed
        }
        /// Complete original packed/index/Pco/native parent, borrowed immutably.
        pub fn parent_packed(&self) -> &PreparedRecoveredPackedMetadata {
            &self.held.parent
        }
        /// Same absolute setup deadline; no continuation renews it.
        pub fn original_expiry(&self) -> Instant {
            self.held.parent.original_expiry()
        }
        /// Same original cumulative ledger, including all prior setup/frame work.
        pub fn recovery_usage(&self) -> TelemetryRecoveryUsage {
            self.held.parent.recovery_usage()
        }
        /// Exact additional source-declared message charges and progress.
        pub fn usage(&self) -> RecoveredMessageUsage {
            self.held.child.charges.usage
        }
        /// Actual first original-work error rather than a formatted substitute.
        pub fn recovery_refusal(&self) -> Option<&TelemetryRecoveryError> {
            self.held.child.charges.recovery.as_ref()
        }
        /// Whether the physical child returned Ok before a separate postcheck.
        pub fn child_succeeded(&self) -> bool {
            self.held.actual.as_ref().is_some_and(|r| r.is_ok())
        }
        /// Actual captured unwind; process-abort builds preserve abort semantics.
        pub fn has_panic(&self) -> bool {
            self.held.panic.is_some()
        }
    };
}
impl PreparedRecoveredMessages {
    views!();
    /// Release completed children before the next frame on SAME spent work/E.
    #[allow(
        clippy::result_large_err,
        reason = "retain original work and complete native outcomes inline"
    )]
    pub fn decompress_next_until(
        self,
        input: Bytes,
        bytes: usize,
        records: u32,
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
        parent.decompress_next_until(input, bytes, records, limits)
    }
}
impl RetainedRecoveredMessageError {
    views!();
}
impl fmt::Debug for PreparedRecoveredMessages {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedRecoveredMessages")
            .field("usage", &self.usage())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for RetainedRecoveredMessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedRecoveredMessageError")
            .field("usage", &self.usage())
            .field("failure", &self.held.child.charges.failure)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedRecoveredMessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for RetainedRecoveredMessageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(e) = self.recovery_refusal() {
            return Some(e);
        }
        if let Some(e) = self.helper_error() {
            return Some(e);
        }
        if let Some(Err(e)) = self.utf8_result() {
            return Some(e);
        }
        if let Some(Err(e)) = self.reservation_result() {
            return Some(e);
        }
        for value in [&self.held.initial, &self.held.post] {
            if let Some(Err(e)) = value {
                return Some(e);
            }
        }
        None
    }
}
#[allow(
    clippy::result_large_err,
    reason = "retain complete packed/native parents and actual message child inline"
)]
pub(super) fn prepare(
    parent: PreparedRecoveredPackedMetadata,
    limits: RecoveredMessageLimits,
) -> MessageResult {
    let sections = parent
        .parent_metadata()
        .parent_index()
        .parent_timestamps()
        .parent_offsets()
        .parent_sections();
    // These are the already-accepted structural extents. No slicing, pointer
    // reconstruction or second wire decoder runs before the child admission.
    let setup = [
        size_of::<&super::recovery_sections::PreparedRecoveredStructuralSections>(),
        size_of::<Option<Range<usize>>>(),
        size_of::<Option<Range<usize>>>(),
        size_of::<[usize; 4]>(),
    ];
    let setup_bytes = setup
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .filter(|n| *n <= isize::MAX as usize);
    let child = State {
        limits,
        record_count: sections.record_count() as usize,
        setup_bytes,
        template_range: sections.section_range(RecoveredStructuralSection::Templates),
        body_range: sections.section_range(RecoveredStructuralSection::Bodies),
        templates: TemplateState::default(),
        lane: LaneState::default(),
        body: BodyState::default(),
        wire: WireState::default(),
        data: Data::default(),
        charges: Charges::default(),
    };
    let held = parent.continue_messages_until(child, |input, index, work, child, fixed| {
        child.run(input, index, work, fixed)
    });
    if held.accepted() {
        Ok(PreparedRecoveredMessages { held })
    } else {
        Err(RetainedRecoveredMessageError { held })
    }
}
