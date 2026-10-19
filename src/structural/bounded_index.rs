//! Consuming pure-Rust embedded-index decoder; no decompressor or publication.

use super::*;
use bytes::Bytes;
use shard_stream_engine::{NativeResumeRefusal, NativeResumeWork};
use std::any::Any;
use std::collections::TryReserveError;
use std::error::Error;
use std::fmt;
use std::mem::take;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Positive finite ceilings for the real embedded-index producer.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EmbeddedIndexDecodeLimits {
    pub(crate) max_input_bytes: usize,
    pub(crate) max_records: usize,
    /// Sum of layout and field-set dictionary counts in either parser pass.
    pub(crate) max_dictionary_entries: usize,
    pub(crate) max_terms: usize,
    pub(crate) max_fields: usize,
    /// All residual/layout/field-set locator IDs in either pass.
    pub(crate) max_locator_ids: usize,
    /// Both columns' total declared runs in either parser pass.
    pub(crate) max_runs: usize,
    /// Cumulative actual output allocation requests, including capacity excess.
    pub(crate) max_output_bytes: usize,
    pub(crate) max_allocation_slots: usize,
    /// Both parser passes, byte copies, comparisons and bit operations.
    pub(crate) max_work_bytes: usize,
}

#[derive(Debug)]
pub(crate) enum EmbeddedIndexFailure {
    Refused(NativeResumeRefusal),
    Limits,
    Codec(TelemetryError),
    Allocation(TryReserveError),
    Panic,
    State,
}
type Failure = EmbeddedIndexFailure;
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

#[derive(Debug, Default)]
struct AllocationUsage {
    bytes: usize,
    slots: usize,
    work: usize,
}
impl AllocationUsage {
    fn pay(
        &mut self,
        bytes: usize,
        slots: usize,
        work: usize,
        limits: EmbeddedIndexDecodeLimits,
        owner: &NativeResumeWork,
    ) -> Result<()> {
        let next_bytes = add(self.bytes, bytes)?;
        let next_slots = add(self.slots, slots)?;
        let next_work = add(self.work, work)?;
        if next_bytes > limits.max_output_bytes
            || next_slots > limits.max_allocation_slots
            || next_work > limits.max_work_bytes
        {
            return Err(Failure::Limits);
        }
        owner
            .reserve_values(bytes, 0, slots, work)
            .map_err(Failure::Refused)?;
        self.bytes = next_bytes;
        self.slots = next_slots;
        self.work = next_work;
        Ok(())
    }
}
fn reserve<T>(
    values: &mut Vec<T>,
    count: usize,
    usage: &mut AllocationUsage,
    limits: EmbeddedIndexDecodeLimits,
    work: &NativeResumeWork,
) -> Result<()> {
    let bytes = mul(count, size_of::<T>())?;
    usage.pay(
        bytes,
        count,
        add(bytes, mul(values.len(), size_of::<T>())?)?,
        limits,
        work,
    )?;
    if let Err(error) = values.try_reserve_exact(count.saturating_sub(values.len())) {
        return Err(Failure::Allocation(error));
    }
    let excess = values.capacity().saturating_sub(count);
    usage.pay(
        mul(excess, size_of::<T>())?,
        excess,
        mul(excess, size_of::<T>())?,
        limits,
        work,
    )
}

#[derive(Default)]
struct Counts {
    dictionaries: usize,
    ids: usize,
    runs: usize,
}
struct Column {
    dictionary: u32,
    bits: u8,
    ordered: bool,
    values: Vec<u8>,
}
impl Column {
    fn empty() -> Self {
        Self {
            dictionary: 0,
            bits: 0,
            ordered: false,
            values: Vec::new(),
        }
    }
}

struct Attempt {
    input: Bytes,
    record_count: u32,
    cursor: usize,
    counts: Counts,
    usage: AllocationUsage,
    last_scalar: u64,
    last_range: Option<Range<usize>>,
    layout: Column,
    field_set: Column,
    residual: Vec<u32>,
    membership_words: [u64; EMBEDDED_MEMBERSHIP_FILTER_WORDS],
    term_membership: Option<MembershipFilter>,
    field_membership: Option<MembershipFilter>,
    terms: Vec<EmbeddedTermLocator>,
    fields: Vec<EmbeddedFieldLocator>,
    current_ids: Vec<u32>,
    current_fingerprint: Option<u32>,
    packed_through: usize,
    completed: Option<EmbeddedFrameIndex>,
    panic: Option<Box<dyn Any + Send>>,
    limits: EmbeddedIndexDecodeLimits,
    work: NativeResumeWork,
}

/// Actual completed index together with its original encoded input and work.
pub(crate) struct PreparedEmbeddedFrameIndex {
    retained: Attempt,
}
impl PreparedEmbeddedFrameIndex {
    pub(crate) fn index(&self) -> Option<&EmbeddedFrameIndex> {
        self.retained.completed.as_ref()
    }
    #[allow(
        dead_code,
        reason = "preserve read-only original encoded index custody without an owner extractor"
    )]
    pub(crate) fn encoded(&self) -> &Bytes {
        &self.retained.input
    }
    /// Move only the completed value into an already-owned staging slot.
    /// The original attempt, encoded input and SAME work remain here. An
    /// occupied destination or previously transferred value is unchanged.
    pub(crate) fn transfer_index_to(
        &mut self,
        destination: &mut Option<EmbeddedFrameIndex>,
    ) -> bool {
        if destination.is_some() || self.retained.completed.is_none() {
            return false;
        }
        *destination = self.retained.completed.take();
        true
    }
}
pub(crate) struct RetainedEmbeddedIndexError {
    pub(crate) cause: Failure,
    retained: Attempt,
}
impl fmt::Debug for RetainedEmbeddedIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedEmbeddedIndexError")
            .field("cause", &self.cause)
            .field("cursor", &self.retained.cursor)
            .field("allocation", &self.retained.usage)
            .field("terms", &self.retained.terms.len())
            .field("fields", &self.retained.fields.len())
            .field("current_ids", &self.retained.current_ids.len())
            .field("completed", &self.retained.completed.is_some())
            .field("panicked", &self.retained.panic.is_some())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for PreparedEmbeddedFrameIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedEmbeddedFrameIndex")
            .field("input_bytes", &self.retained.input.len())
            .field("allocation", &self.retained.usage)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedEmbeddedIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for RetainedEmbeddedIndexError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause {
            Failure::Refused(e) => Some(e),
            Failure::Codec(e) => Some(e),
            Failure::Allocation(e) => Some(e),
            _ => None,
        }
    }
}

impl EmbeddedFrameIndex {
    /// Decode the original embedded wire directly with SAME work/original E.
    ///
    /// Consumes the actual input before initial refusal. Both result variants
    /// retain it, all real partial output and the supplied work. This performs
    /// no decompression, stripe mutation, application or serving publication.
    #[allow(
        clippy::result_large_err,
        reason = "retain complete original owners and native outcomes inline without a refusal allocation"
    )]
    pub(crate) fn decode_until(
        input: Bytes,
        record_count: u32,
        limits: EmbeddedIndexDecodeLimits,
        work: &NativeResumeWork,
    ) -> std::result::Result<PreparedEmbeddedFrameIndex, RetainedEmbeddedIndexError> {
        let mut retained = Attempt {
            input,
            record_count,
            cursor: 0,
            counts: Counts::default(),
            usage: AllocationUsage::default(),
            last_scalar: 0,
            last_range: None,
            layout: Column::empty(),
            field_set: Column::empty(),
            residual: Vec::new(),
            membership_words: [0; EMBEDDED_MEMBERSHIP_FILTER_WORDS],
            term_membership: None,
            field_membership: None,
            terms: Vec::new(),
            fields: Vec::new(),
            current_ids: Vec::new(),
            current_fingerprint: None,
            packed_through: 0,
            completed: None,
            panic: None,
            limits,
            work: work.clone(),
        };
        let actual = catch_unwind(AssertUnwindSafe(|| retained.run()));
        let cause = match actual {
            Ok(Ok(())) => return Ok(PreparedEmbeddedFrameIndex { retained }),
            Ok(Err(error)) => error,
            Err(payload) => {
                retained.panic = Some(payload);
                Failure::Panic
            }
        };
        Err(RetainedEmbeddedIndexError { cause, retained })
    }
}
impl Attempt {
    fn check(&self) -> Result<()> {
        self.work.check().map_err(Failure::Refused)
    }
    fn effort(&mut self, work: usize) -> Result<()> {
        self.usage.pay(0, 0, work, self.limits, &self.work)
    }
    fn byte(&mut self) -> Result<u8> {
        self.effort(1)?;
        let value = *self
            .input
            .get(self.cursor)
            .ok_or_else(|| codec("truncated block"))?;
        self.cursor = self
            .cursor
            .checked_add(1)
            .ok_or_else(|| codec("cursor overflow"))?;
        self.last_scalar = u64::from(value);
        self.check()?;
        Ok(value)
    }
    fn varint(&mut self) -> Result<u64> {
        let mut value = 0_u64;
        let mut shift = 0_u32;
        loop {
            let byte = self.byte()?;
            let payload = u64::from(byte & 0x7f);
            self.effort(1)?;
            if shift > 63 || (shift == 63 && payload > 1) {
                return Err(codec("varint overflow"));
            }
            value |= payload << shift;
            self.last_scalar = value;
            self.check()?;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift = shift.saturating_add(7);
            if shift > 63 {
                return Err(codec("varint is too long"));
            }
        }
    }
    fn usize(&mut self) -> Result<usize> {
        usize::try_from(self.varint()?).map_err(|_| codec("length does not fit usize"))
    }
    fn u32(&mut self) -> Result<u32> {
        u32::try_from(self.varint()?).map_err(|_| codec("value does not fit u32"))
    }
    fn range(&mut self, len: usize, message: &'static str) -> Result<Range<usize>> {
        self.effort(1)?;
        let end = self
            .cursor
            .checked_add(len)
            .ok_or_else(|| codec("section length overflow"))?;
        if end > self.input.len() {
            return Err(codec(message));
        }
        let range = self.cursor..end;
        self.cursor = end;
        self.last_range = Some(range.clone());
        self.check()?;
        Ok(range)
    }
    fn run(&mut self) -> Result<()> {
        self.check()?;
        let l = self.limits;
        let records = usize::try_from(self.record_count)
            .map_err(|_| Failure::Codec(TelemetryError::RecordTooLarge))?;
        if [
            l.max_input_bytes,
            l.max_records,
            l.max_dictionary_entries,
            l.max_terms,
            l.max_fields,
            l.max_locator_ids,
            l.max_runs,
            l.max_output_bytes,
            l.max_allocation_slots,
            l.max_work_bytes,
        ]
        .into_iter()
        .any(|n| n == 0 || n > isize::MAX as usize)
            || self.input.len() > l.max_input_bytes
            || records > l.max_records
        {
            return Err(Failure::Limits);
        }
        self.work
            .reserve_values(add(size_of::<Self>(), self.input.len())?, 0, 1, 0)
            .map_err(Failure::Refused)?;
        self.effort(self.input.len())?;
        self.pass(false)?;
        self.cursor = 0;
        self.counts = Counts::default();
        self.current_fingerprint = None;
        self.pass(true)?;
        if self.term_membership.is_none() || self.field_membership.is_none() {
            return Err(Failure::State);
        }
        let members = (self.term_membership.take(), self.field_membership.take());
        match members {
            (Some(term_membership), Some(field_membership)) => {
                self.completed = Some(EmbeddedFrameIndex {
                    record_count: self.record_count,
                    timestamp_offset_ordinal_ordered: self.layout.ordered,
                    layout_count: self.layout.dictionary,
                    layout_ids: PackedIdColumn {
                        bits_per_id: self.layout.bits,
                        values: take(&mut self.layout.values),
                    },
                    residual_layout_ids: take(&mut self.residual),
                    term_membership,
                    terms: take(&mut self.terms),
                    field_set_count: self.field_set.dictionary,
                    field_set_ids: PackedIdColumn {
                        bits_per_id: self.field_set.bits,
                        values: take(&mut self.field_set.values),
                    },
                    field_membership,
                    fields: take(&mut self.fields),
                });
            }
            (term, field) => {
                self.term_membership = term;
                self.field_membership = field;
                return Err(Failure::State);
            }
        }
        self.after_value("completed")
    }
    fn pass(&mut self, decode: bool) -> Result<()> {
        self.column(false, decode)?;
        self.ids(self.layout.dictionary, true, decode)?;
        if decode {
            self.residual = take(&mut self.current_ids);
        }
        let range = self.range(
            mul(EMBEDDED_MEMBERSHIP_FILTER_WORDS, size_of::<u64>())?,
            "truncated membership filter",
        )?;
        self.effort(range.len())?;
        for (index, word) in self.input[range].chunks_exact(size_of::<u64>()).enumerate() {
            let mut raw = [0; 8];
            raw.copy_from_slice(word);
            self.membership_words[index] = u64::from_le_bytes(raw);
        }
        if decode {
            self.membership(false)?;
        }
        let terms = self.usize()?;
        if terms > self.input.len().saturating_sub(self.cursor) {
            return Err(codec("embedded term count"));
        }
        if terms > self.limits.max_terms {
            return Err(Failure::Limits);
        }
        if decode {
            reserve(
                &mut self.terms,
                terms,
                &mut self.usage,
                self.limits,
                &self.work,
            )?;
        }
        let mut previous = None;
        for _ in 0..terms {
            self.fingerprint()?;
            let fingerprint = self.current_fingerprint.ok_or(Failure::State)?;
            self.ids(self.layout.dictionary, false, decode)?;
            self.effort(1)?;
            if previous.is_some_and(|v| v >= fingerprint) {
                return Err(codec("embedded terms are not ordered"));
            }
            previous = Some(fingerprint);
            if decode {
                self.terms.push(EmbeddedTermLocator {
                    fingerprint,
                    layout_ids: take(&mut self.current_ids),
                });
                self.after_value("term")?;
            }
        }
        self.column(true, decode)?;
        if decode {
            self.membership(true)?;
        }
        let fields = self.usize()?;
        if fields > self.input.len().saturating_sub(self.cursor) {
            return Err(codec("embedded field count"));
        }
        if fields > self.limits.max_fields {
            return Err(Failure::Limits);
        }
        if decode {
            reserve(
                &mut self.fields,
                fields,
                &mut self.usage,
                self.limits,
                &self.work,
            )?;
        }
        previous = None;
        for _ in 0..fields {
            self.fingerprint()?;
            let fingerprint = self.current_fingerprint.ok_or(Failure::State)?;
            self.ids(self.field_set.dictionary, false, decode)?;
            self.effort(1)?;
            if previous.is_some_and(|v| v >= fingerprint) {
                return Err(codec("embedded fields are not ordered"));
            }
            previous = Some(fingerprint);
            if decode {
                self.fields.push(EmbeddedFieldLocator {
                    fingerprint,
                    field_set_ids: take(&mut self.current_ids),
                });
                self.after_value("field")?;
            }
        }
        if self.cursor != self.input.len() {
            return Err(codec("trailing component bytes"));
        }
        self.check()
    }
    fn membership(&mut self, field: bool) -> Result<()> {
        let bytes = size_of::<[u64; EMBEDDED_MEMBERSHIP_FILTER_WORDS]>();
        self.usage.pay(
            bytes,
            EMBEDDED_MEMBERSHIP_FILTER_WORDS,
            bytes,
            self.limits,
            &self.work,
        )?;
        let words = Box::new(self.membership_words);
        let member = MembershipFilter { words };
        if field {
            self.field_membership = Some(member);
        } else {
            self.term_membership = Some(member);
        }
        self.after_value("membership")
    }
    fn fingerprint(&mut self) -> Result<()> {
        let end = self
            .cursor
            .checked_add(3)
            .ok_or_else(|| codec("fingerprint cursor overflow"))?;
        self.effort(3)?;
        let bytes = self
            .input
            .get(self.cursor..end)
            .ok_or_else(|| codec("fingerprint is truncated"))?;
        let value = u32::from(bytes[0]) | u32::from(bytes[1]) << 8 | u32::from(bytes[2]) << 16;
        self.cursor = end;
        self.current_fingerprint = Some(value);
        self.last_scalar = u64::from(value);
        self.check()
    }
    fn ids(&mut self, upper: u32, optional: bool, decode: bool) -> Result<()> {
        let count = self.usize()?;
        if optional && count == 0 {
            return Ok(());
        }
        if count == 0 || count > self.input.len().saturating_sub(self.cursor) {
            return Err(codec("invalid embedded locator count"));
        }
        self.counts.ids = add(self.counts.ids, count)?;
        if self.counts.ids > self.limits.max_locator_ids {
            return Err(Failure::Limits);
        }
        if decode {
            reserve(
                &mut self.current_ids,
                count,
                &mut self.usage,
                self.limits,
                &self.work,
            )?;
        }
        let mut previous = self.u32()?;
        self.effort(1)?;
        if previous >= upper {
            return Err(codec("embedded locator ID is out of range"));
        }
        if decode {
            self.current_ids.push(previous);
            self.after_value("id")?;
        }
        for _ in 1..count {
            let delta = self.u32()?;
            self.effort(2)?;
            if delta == 0 {
                return Err(codec("embedded locator IDs are not ordered"));
            }
            previous = previous
                .checked_add(delta)
                .filter(|v| *v < upper)
                .ok_or_else(|| codec("embedded locator ID is out of range"))?;
            self.last_scalar = u64::from(previous);
            if decode {
                self.current_ids.push(previous);
                self.after_value("id")?;
            }
        }
        Ok(())
    }
    fn column(&mut self, field: bool, decode: bool) -> Result<()> {
        let records = usize::try_from(self.record_count)
            .map_err(|_| Failure::Codec(TelemetryError::RecordTooLarge))?;
        let dictionary = self.u32()?;
        if (self.record_count == 0 && dictionary != 0)
            || (self.record_count != 0 && (dictionary == 0 || dictionary > self.record_count))
        {
            return Err(codec("invalid embedded dictionary count"));
        }
        self.counts.dictionaries = add(self.counts.dictionaries, dictionary as usize)?;
        if self.counts.dictionaries > self.limits.max_dictionary_entries {
            return Err(Failure::Limits);
        }
        self.effort(1)?;
        let bits = bits_for_dictionary(dictionary);
        let bit_count = records
            .checked_mul(usize::from(bits))
            .ok_or(Failure::Codec(TelemetryError::RecordTooLarge))?;
        let expected = bit_count.div_ceil(8);
        let kind = self.byte()?;
        let ordered = kind & PACKED_IDS_POSITION_ORDERED != 0;
        if field && ordered {
            return Err(codec(
                "packed ID column has an unexpected position-order flag",
            ));
        }
        let target = if field {
            &mut self.field_set
        } else {
            &mut self.layout
        };
        target.dictionary = dictionary;
        target.bits = bits;
        target.ordered = ordered;
        match kind & !PACKED_IDS_POSITION_ORDERED {
            PACKED_IDS_BITPACKED => {
                let len = self.usize()?;
                let range = self.range(len, "truncated section")?;
                if len != expected {
                    return Err(codec("packed ID column length mismatch"));
                }
                if decode {
                    let target = if field {
                        &mut self.field_set
                    } else {
                        &mut self.layout
                    };
                    reserve(
                        &mut target.values,
                        expected,
                        &mut self.usage,
                        self.limits,
                        &self.work,
                    )?;
                    self.effort(expected)?;
                    let mut at = range.start;
                    while at < range.end {
                        self.check()?;
                        let end = at.saturating_add(16 * 1024).min(range.end);
                        let target = if field {
                            &mut self.field_set
                        } else {
                            &mut self.layout
                        };
                        target.values.extend_from_slice(&self.input[at..end]);
                        self.after_value("column")?;
                        at = end;
                    }
                }
                for ordinal in 0..self.record_count {
                    self.effort(add(usize::from(bits), 8)?)?;
                    let actual = packed(&self.input[range.clone()], bits, ordinal);
                    self.last_scalar = u64::from(actual);
                    self.packed_through = ordinal as usize + 1;
                    if actual >= dictionary {
                        return Err(codec("packed ID exceeds its dictionary"));
                    }
                }
                if expected > 0 {
                    let used = bit_count % 8;
                    self.effort(1)?;
                    if used != 0 && self.input[range.end - 1] >> used != 0 {
                        return Err(codec("packed ID padding is nonzero"));
                    }
                }
            }
            PACKED_IDS_RUN_LENGTH => {
                let runs = self.usize()?;
                if runs > self.input.len().saturating_sub(self.cursor) {
                    return Err(codec("packed ID run count"));
                }
                self.counts.runs = add(self.counts.runs, runs)?;
                if self.counts.runs > self.limits.max_runs {
                    return Err(Failure::Limits);
                }
                if decode {
                    let target = if field {
                        &mut self.field_set
                    } else {
                        &mut self.layout
                    };
                    reserve(
                        &mut target.values,
                        expected,
                        &mut self.usage,
                        self.limits,
                        &self.work,
                    )?;
                    self.effort(expected)?;
                    let target = if field {
                        &mut self.field_set
                    } else {
                        &mut self.layout
                    };
                    target.values.resize(expected, 0);
                    self.after_value("column")?;
                }
                let mut ordinal = 0_usize;
                for _ in 0..runs {
                    let id = self.u32()?;
                    let length = self.usize()?;
                    self.effort(2)?;
                    if id >= dictionary || length == 0 {
                        return Err(codec("packed ID run is invalid"));
                    }
                    let next = ordinal
                        .checked_add(length)
                        .filter(|n| *n <= records)
                        .ok_or_else(|| codec("packed ID runs exceed record count"))?;
                    // Charge bounded expansion and every produced bit BEFORE
                    // the actual run changes its retained packed destination.
                    self.effort(mul(length, add(usize::from(bits), 1)?)?)?;
                    if decode {
                        let mut at = ordinal;
                        while at < next {
                            self.check()?;
                            let end = at.saturating_add(256).min(next);
                            let target = if field {
                                &mut self.field_set
                            } else {
                                &mut self.layout
                            };
                            for index in at..end {
                                put_id(&mut target.values, bits, index, id);
                            }
                            self.packed_through = end;
                            self.after_value("run")?;
                            at = end;
                        }
                    }
                    ordinal = next;
                }
                if ordinal != records {
                    return Err(codec("packed ID runs do not cover record count"));
                }
            }
            _ => return Err(codec("unknown packed ID column encoding")),
        }
        self.check()
    }
    fn after_value(&self, phase: &'static str) -> Result<()> {
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        tests::after_value(self.work.absolute_expiry(), phase);
        #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
        let _ = phase;
        self.check()
    }
}
fn packed(values: &[u8], bits: u8, ordinal: u32) -> u32 {
    if bits == 0 {
        return 0;
    }
    let bit = ordinal as usize * usize::from(bits);
    let byte = bit / 8;
    let shift = bit % 8;
    let mut window = 0_u64;
    for index in 0..5 {
        if let Some(value) = values.get(byte + index) {
            window |= u64::from(*value) << (index * 8);
        }
    }
    ((window >> shift) & ((1_u64 << bits) - 1)) as u32
}
fn put_id(values: &mut [u8], bits: u8, ordinal: usize, id: u32) {
    let first = ordinal * usize::from(bits);
    for bit in 0..usize::from(bits) {
        if id & (1_u32 << bit) != 0 {
            let at = first + bit;
            values[at / 8] |= 1 << (at % 8);
        }
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests;
