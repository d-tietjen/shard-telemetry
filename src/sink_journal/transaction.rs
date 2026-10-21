//! Actual bounded transaction writes on the journal's retained file.
//!
//! The consuming entry retains the caller's original owned Vec/Bytes and SAME
//! NativeResumeWork. Its error is returned by value: exhausted counters never
//! cause an emergency Box allocation. A later strict callback adapter must
//! prepay/own its actual error carrier and input queue before entry; this
//! producer does not allocate that carrier or claim index/factory completion.

use super::*;
use shard_stream_engine::{NativeResumeRefusal, NativeResumeWork};
use std::any::Any;
use std::fmt;
use std::io;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

const HEADER: usize = 2 * (16 + 4 + 8 + 8) + 4;
const APPEND_HEADER: usize = 4 + 8 + 4;
const CHUNK: usize = 16 * 1024;

/// Whole genuine owned input, including transient context and shared payloads.
#[derive(Debug)]
pub(crate) struct JournalTransaction {
    pub(crate) expected: DurableSinkCheckpoint,
    pub(crate) appends: Vec<DurableAppend>,
    pub(crate) next: DurableSinkCheckpoint,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct JournalAppendLimits {
    pub(crate) max_appends: usize,
    pub(crate) max_input_slots: usize,
    pub(crate) max_payload_bytes: usize,
    pub(crate) max_input_bytes: usize,
    pub(crate) max_frame_bytes: usize,
    /// Includes stat, seek, write attempts and the prepaid settlement sync.
    pub(crate) max_io_attempts: usize,
    pub(crate) max_write_request_bytes: usize,
    pub(crate) max_write_chunk_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JournalAppendKind {
    /// The actual durable journal checkpoint already covers this boundary.
    AlreadyCovered(DurableSinkCheckpoint),
    /// Actual complete frame, successful file sync and published local cache.
    Committed(DurableSinkCheckpoint),
}

pub(crate) struct JournalAppendOutcome {
    pub(crate) kind: JournalAppendKind,
    #[allow(
        dead_code,
        reason = "retain the complete original input and native append outcomes through result Drop"
    )]
    retained: Attempt,
}
pub(crate) struct RetainedJournalAppendError {
    pub(crate) cause: JournalAppendFailure,
    retained: Attempt,
}
#[derive(Debug)]
pub(crate) enum JournalAppendFailure {
    Limits,
    Refused(NativeResumeRefusal),
    Busy,
    Terminal,
    InvalidInput,
    Changed,
    Allocation(std::collections::TryReserveError),
    Io(io::Error),
    Panic,
    Settlement,
}
type Result<T> = std::result::Result<T, JournalAppendFailure>;
type Failure = JournalAppendFailure;

struct Attempt {
    input: JournalTransaction,
    frame: Vec<u8>,
    checkpoint: Option<DurableSinkCheckpoint>,
    stat: Option<rustix::fs::Stat>,
    seek: Option<u64>,
    confirmed_written: usize,
    io_attempts: usize,
    write_requests: usize,
    entered: bool,
    frame_complete: bool,
    bytes_published: bool,
    checkpoint_published: bool,
    primary: Option<Failure>,
    panic: Option<Box<dyn Any + Send>>,
    sync: Option<io::Result<()>>,
    sync_panic: Option<Box<dyn Any + Send>>,
    cache_panic: Option<Box<dyn Any + Send>>,
    interrupted: Option<io::Error>,
    kind: Option<JournalAppendKind>,
    limits: JournalAppendLimits,
    work: NativeResumeWork,
    // Actual file/mutex/checkpoint owner drops after all input/effect witnesses.
    journal: Arc<SinkJournal>,
}
impl fmt::Debug for RetainedJournalAppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedJournalAppendError")
            .field("cause", &self.cause)
            .field("confirmed_written", &self.retained.confirmed_written)
            .field("frame_complete", &self.retained.frame_complete)
            .field("checkpoint_published", &self.retained.checkpoint_published)
            .field(
                "sync",
                &self
                    .retained
                    .sync
                    .as_ref()
                    .map(|r| r.as_ref().map_err(io::Error::kind)),
            )
            .field("sync_panicked", &self.retained.sync_panic.is_some())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedJournalAppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bounded journal transaction refused: {:?}", self.cause)
    }
}
impl std::error::Error for RetainedJournalAppendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            Failure::Io(error) => Some(error),
            Failure::Allocation(error) => Some(error),
            Failure::Refused(error) => Some(error),
            Failure::Settlement => self
                .retained
                .sync
                .as_ref()
                .and_then(|r| r.as_ref().err())
                .map(|e| e as &(dyn std::error::Error + 'static)),
            _ => None,
        }
    }
}
impl fmt::Debug for JournalAppendOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournalAppendOutcome")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl JournalAppendOutcome {
    /// Borrow the exact owned input retained across the actual journal write.
    /// This supplies no indexed/application completion or retry extractor.
    #[allow(
        dead_code,
        reason = "preserve original transaction custody through the actual native append result"
    )]
    pub(crate) fn transaction(&self) -> &JournalTransaction {
        &self.retained.input
    }
}

impl SinkJournal {
    /// Consume one actual transaction using the supplied original-E work.
    /// No ordinary append call, detached task or input clone occurs here.
    ///
    /// Input and physical outcomes stay in the by-value result on every path.
    /// Uncertain/partial writes latch the same journal terminal for bounded AND
    /// legacy append. Only a complete synced frame with published cache clears
    /// that latch, even when its final original-clock observation is late.
    /// A later callback adapter must independently prepay/own its error carrier.
    #[allow(
        clippy::result_large_err,
        reason = "retain complete original owners and native outcomes inline without a refusal allocation"
    )]
    pub(crate) fn append_until(
        self: &Arc<Self>,
        input: JournalTransaction,
        limits: JournalAppendLimits,
        work: &NativeResumeWork,
    ) -> std::result::Result<JournalAppendOutcome, RetainedJournalAppendError> {
        self.append_entry_until(input, limits, work, None)
    }

    #[allow(
        clippy::result_large_err,
        reason = "retain original transaction/native outcomes inline on every path"
    )]
    pub(crate) fn append_application_until(
        self: &Arc<Self>,
        input: JournalTransaction,
        limits: JournalAppendLimits,
        edge: &crate::sink::ApplicationJournalEdge<'_>,
    ) -> std::result::Result<JournalAppendOutcome, RetainedJournalAppendError> {
        self.append_entry_until(input, limits, edge.work(), Some(edge))
    }

    #[allow(
        clippy::result_large_err,
        reason = "retain original transaction/native outcomes inline on every path"
    )]
    fn append_entry_until(
        self: &Arc<Self>,
        input: JournalTransaction,
        limits: JournalAppendLimits,
        work: &NativeResumeWork,
        edge: Option<&crate::sink::ApplicationJournalEdge<'_>>,
    ) -> std::result::Result<JournalAppendOutcome, RetainedJournalAppendError> {
        let mut attempt = Attempt {
            input,
            frame: Vec::new(),
            checkpoint: None,
            stat: None,
            seek: None,
            confirmed_written: 0,
            io_attempts: 0,
            write_requests: 0,
            entered: false,
            frame_complete: false,
            bytes_published: false,
            checkpoint_published: false,
            primary: None,
            panic: None,
            sync: None,
            sync_panic: None,
            cache_panic: None,
            interrupted: None,
            kind: None,
            limits,
            work: work.clone(),
            journal: Arc::clone(self),
        };
        let actual = catch_unwind(AssertUnwindSafe(|| attempt.run(edge)));
        let actual = match actual {
            Ok(result) => result,
            Err(payload) => {
                attempt.panic = Some(payload);
                Err(Failure::Panic)
            }
        };
        if let Err(cause) = actual {
            return Err(RetainedJournalAppendError {
                cause,
                retained: attempt,
            });
        }
        match attempt.kind {
            Some(kind) => Ok(JournalAppendOutcome {
                kind,
                retained: attempt,
            }),
            None => Err(RetainedJournalAppendError {
                cause: Failure::Changed,
                retained: attempt,
            }),
        }
    }
}

fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or(Failure::Limits)
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or(Failure::Limits)
}
fn pay(work: &NativeResumeWork, bytes: usize, entries: usize, effort: usize) -> Result<()> {
    work.reserve_values(bytes, 0, entries, effort)
        .map_err(Failure::Refused)
}
impl Attempt {
    fn check(&self) -> Result<()> {
        self.work.check().map_err(Failure::Refused)
    }
    fn io(&mut self, bytes: usize) -> Result<()> {
        let count = add(self.io_attempts, 1)?;
        let requests = add(self.write_requests, bytes)?;
        if count > self.limits.max_io_attempts || requests > self.limits.max_write_request_bytes {
            return Err(Failure::Limits);
        }
        pay(&self.work, 0, 1, add(bytes, 1)?)?;
        self.io_attempts = count;
        self.write_requests = requests;
        Ok(())
    }
    fn census(&self) -> Result<usize> {
        self.check()?;
        if [
            self.limits.max_appends,
            self.limits.max_input_slots,
            self.limits.max_payload_bytes,
            self.limits.max_input_bytes,
            self.limits.max_frame_bytes,
            self.limits.max_io_attempts,
            self.limits.max_write_request_bytes,
            self.limits.max_write_chunk_bytes,
        ]
        .into_iter()
        .any(|v| v == 0 || v > isize::MAX as usize)
            || self.limits.max_write_chunk_bytes > CHUNK
            || self.input.appends.len() > self.limits.max_appends
            || self.input.appends.capacity() > self.limits.max_input_slots
        {
            return Err(Failure::Limits);
        }
        let values = add(
            size_of::<Self>(),
            mul(self.input.appends.capacity(), size_of::<DurableAppend>())?,
        )?;
        pay(
            &self.work,
            values,
            add(self.input.appends.capacity(), 1)?,
            values,
        )?;
        let expected = self.input.expected;
        let next = self.input.next;
        if expected.topic_partition != next.topic_partition
            || expected.next_offset > next.next_offset
            || expected.next_placement_sequence > next.next_placement_sequence
        {
            return Err(Failure::InvalidInput);
        }
        let mut length = HEADER;
        let mut payloads = 0;
        let mut retained_input = 0;
        let mut prior = None;
        let mut prior_sequence = None;
        for append in &self.input.appends {
            self.check()?;
            if append.topic_partition() != expected.topic_partition
                || append.physical_shard_id != self.journal.shard_id
                || append.reservation.first_offset < expected.next_offset
                || append.reservation.last_offset >= next.next_offset
                || append
                    .reservation
                    .first_offset
                    .get()
                    .checked_add(u64::from(append.reservation.record_count.get()) - 1)
                    != Some(append.reservation.last_offset.get())
                || prior.is_some_and(|offset| offset >= append.reservation.first_offset)
                || append.reservation.placement.sequence < expected.next_placement_sequence
                || append.reservation.placement.sequence >= next.next_placement_sequence
                || prior_sequence
                    .is_some_and(|prior| prior >= append.reservation.placement.sequence)
            {
                return Err(Failure::InvalidInput);
            }
            prior = Some(append.reservation.last_offset);
            prior_sequence = Some(append.reservation.placement.sequence);
            payloads = add(payloads, append.payload.len())?;
            let context = append.transient_context.as_ref().map_or(0, Bytes::len);
            let actual_input = add(append.payload.len(), context)?;
            retained_input = add(retained_input, actual_input)?;
            if retained_input > self.limits.max_input_bytes {
                return Err(Failure::Limits);
            }
            pay(&self.work, actual_input, 1, actual_input)?;
            length = add(length, add(APPEND_HEADER, append.payload.len())?)?;
            if payloads > self.limits.max_payload_bytes || length > MAX_FRAME_BYTES {
                return Err(Failure::Limits);
            }
        }
        let length = add(length, 4 + CHECKSUM_BYTES)?;
        if length > self.limits.max_frame_bytes {
            return Err(Failure::Limits);
        }
        Ok(length)
    }
    fn encode(&mut self, length: usize) -> Result<()> {
        pay(&self.work, length, 1, mul(length, 3)?)?;
        if let Err(error) = self.frame.try_reserve_exact(length) {
            return Err(Failure::Allocation(error));
        }
        pay(
            &self.work,
            self.frame.capacity().saturating_sub(length),
            0,
            0,
        )?;
        self.frame.resize(length, 0);
        let mut at = 0;
        put(
            &mut self.frame,
            &mut at,
            &u32::try_from(length - 4 - CHECKSUM_BYTES)
                .map_err(|_| Failure::Limits)?
                .to_le_bytes(),
        )?;
        put_checkpoint(&mut self.frame, &mut at, self.input.expected)?;
        put_checkpoint(&mut self.frame, &mut at, self.input.next)?;
        put(
            &mut self.frame,
            &mut at,
            &u32::try_from(self.input.appends.len())
                .map_err(|_| Failure::Limits)?
                .to_le_bytes(),
        )?;
        for append in &self.input.appends {
            self.check()?;
            put(
                &mut self.frame,
                &mut at,
                &append.physical_shard_id.get().to_le_bytes(),
            )?;
            put(
                &mut self.frame,
                &mut at,
                &append.reservation.first_offset.get().to_le_bytes(),
            )?;
            put(
                &mut self.frame,
                &mut at,
                &u32::try_from(append.payload.len())
                    .map_err(|_| Failure::Limits)?
                    .to_le_bytes(),
            )?;
            put(&mut self.frame, &mut at, &append.payload)?;
        }
        if at != length - CHECKSUM_BYTES {
            return Err(Failure::InvalidInput);
        }
        let checksum = *blake3::hash(&self.frame[4..at]).as_bytes();
        put(&mut self.frame, &mut at, &checksum)?;
        self.check()
    }
    fn run(&mut self, edge: Option<&crate::sink::ApplicationJournalEdge<'_>>) -> Result<()> {
        let length = self.census()?;
        // The new persisted domain has no unsealed writer. Legacy calls retain
        // the exact old comparison below; no permissive flag crosses the API.
        let application = match (self.journal.format, edge) {
            (JournalFormat::Legacy, None) => false,
            (JournalFormat::Application, Some(edge))
                if edge.permits(&self.journal, self.input.expected, self.input.next) =>
            {
                true
            }
            _ => return Err(Failure::InvalidInput),
        };
        // A separate Arc local permits mutation of this retained attempt while
        // the genuine journal's try-lock guard is held through all settlement.
        let journal = Arc::clone(&self.journal);
        let mut state = journal.state.try_lock().map_err(|_| Failure::Busy)?;
        self.check()?;
        if state.terminal_write_refused {
            return Err(Failure::Terminal);
        }
        probe(&state.checkpoints, &self.work)?;
        let actual = state
            .checkpoints
            .get(&self.input.expected.topic_partition)
            .copied()
            .unwrap_or_else(|| DurableSinkCheckpoint::initial(self.input.expected.topic_partition));
        self.checkpoint = Some(actual);
        if checkpoint_covers(actual, self.input.next) {
            self.kind = Some(JournalAppendKind::AlreadyCovered(actual));
            return self.check();
        }
        if if application {
            // Missing placements belong to other actual physical anchors. The
            // borrowed seal proves the exact global edge under its held guard.
            actual.topic_partition != self.input.expected.topic_partition
                || actual.next_placement_sequence > self.input.expected.next_placement_sequence
                || actual.next_offset > self.input.expected.next_offset
        } else {
            !checkpoint_allows_lane_gap(actual, self.input.expected)
        } {
            return Err(Failure::InvalidInput);
        }
        let next_bytes = state
            .bytes
            .checked_add(length as u64)
            .ok_or(Failure::Limits)?;
        if next_bytes > journal.max_bytes {
            return Err(Failure::Limits);
        }
        self.encode(length)?;
        probe(&state.checkpoints, &self.work)?;
        if !state
            .checkpoints
            .contains_key(&self.input.next.topic_partition)
        {
            reserve_checkpoint(&mut state.checkpoints, &self.work)?;
        }
        // Reserve the actual eventual insertion BEFORE physical mutation. No
        // clock or allocation can suppress truthful publication after commit.
        probe(&state.checkpoints, &self.work)?;
        self.io(0)?;
        self.stat = Some(rustix::fs::fstat(&state.file).map_err(|e| Failure::Io(e.into()))?);
        self.check()?;
        let stat = self.stat.as_ref().ok_or(Failure::Changed)?;
        if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::RegularFile
            || stat.st_nlink != 1
            || u64::try_from(stat.st_size).ok() != Some(state.bytes)
        {
            state.terminal_write_refused = true;
            return Err(Failure::Changed);
        }
        self.io(0)?; // Prepaid physical fsync, even on later refusal/unwind.
        let actual = catch_unwind(AssertUnwindSafe(|| {
            self.io(0)?;
            state.terminal_write_refused = true;
            self.entered = true;
            self.seek = Some(
                rustix::fs::seek(&state.file, rustix::fs::SeekFrom::Start(state.bytes))
                    .map_err(|e| Failure::Io(e.into()))?,
            );
            if self.seek != Some(state.bytes) {
                return Err(Failure::Changed);
            }
            self.check()?;
            while self.confirmed_written < self.frame.len() {
                let request = (self.frame.len() - self.confirmed_written)
                    .min(self.limits.max_write_chunk_bytes);
                self.io(request)?;
                let actual = rustix::io::write(
                    &state.file,
                    &self.frame[self.confirmed_written..self.confirmed_written + request],
                )
                .map_err(io::Error::from);
                match actual {
                    Ok(0) => {
                        return Err(Failure::Io(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "journal append made no progress",
                        )));
                    }
                    Ok(written) => {
                        self.confirmed_written += written;
                        self.frame_complete = self.confirmed_written == self.frame.len();
                        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
                        tests::after_write(self.work.absolute_expiry());
                        self.check()?;
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                        self.interrupted = Some(error);
                        self.check()?;
                    }
                    Err(error) => return Err(Failure::Io(error)),
                }
            }
            Ok(())
        }));
        self.primary = match actual {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(payload) => {
                self.panic = Some(payload);
                Some(Failure::Panic)
            }
        };
        if self.entered {
            let actual = catch_unwind(AssertUnwindSafe(|| {
                self.sync = Some(rustix::fs::fsync(&state.file).map_err(io::Error::from));
                #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
                tests::after_sync();
            }));
            if let Err(payload) = actual {
                self.sync_panic = Some(payload);
            }
        }
        if self.frame_complete && self.sync.as_ref().is_some_and(std::result::Result::is_ok) {
            let actual = catch_unwind(AssertUnwindSafe(|| {
                state.bytes = next_bytes;
                self.bytes_published = true;
                state.unsynced_bytes = 0;
                state
                    .checkpoints
                    .insert(self.input.next.topic_partition, self.input.next);
                self.checkpoint_published = true;
                state.terminal_write_refused = false;
                self.kind = Some(JournalAppendKind::Committed(self.input.next));
            }));
            if let Err(payload) = actual {
                self.cache_panic = Some(payload);
            }
        }
        if let Some(error) = self.primary.take() {
            return Err(error);
        }
        if self.sync_panic.is_some() || self.cache_panic.is_some() {
            return Err(Failure::Panic);
        }
        if self.entered && !self.sync.as_ref().is_some_and(std::result::Result::is_ok) {
            return Err(Failure::Settlement);
        }
        if self.kind.is_none() {
            return Err(Failure::Changed);
        }
        self.check()
    }
}
fn probe(
    map: &HashMap<TopicPartition, DurableSinkCheckpoint>,
    work: &NativeResumeWork,
) -> Result<()> {
    pay(
        work,
        0,
        0,
        mul(
            mul(add(map.capacity(), 1)?, 2)?,
            size_of::<(TopicPartition, DurableSinkCheckpoint)>(),
        )?,
    )
}
fn reserve_checkpoint(
    map: &mut HashMap<TopicPartition, DurableSinkCheckpoint>,
    work: &NativeResumeWork,
) -> Result<()> {
    if map.len() < map.capacity() {
        return work.check().map_err(Failure::Refused);
    }
    let requested = add(map.len(), 1)?;
    let slot = size_of::<(TopicPartition, DurableSinkCheckpoint)>();
    pay(
        work,
        mul(requested, slot)?,
        requested,
        mul(add(requested, map.capacity())?, slot)?,
    )?;
    if let Err(error) = map.try_reserve(1) {
        return Err(Failure::Allocation(error));
    }
    let extra = map.capacity().saturating_sub(requested);
    pay(work, mul(extra, slot)?, extra, mul(extra, slot)?)
}
fn put(frame: &mut [u8], at: &mut usize, bytes: &[u8]) -> Result<()> {
    let end = add(*at, bytes.len())?;
    let output = frame.get_mut(*at..end).ok_or(Failure::InvalidInput)?;
    output.copy_from_slice(bytes);
    *at = end;
    Ok(())
}
fn put_checkpoint(
    frame: &mut [u8],
    at: &mut usize,
    checkpoint: DurableSinkCheckpoint,
) -> Result<()> {
    put(
        frame,
        at,
        &checkpoint.topic_partition.topic_id.get().to_le_bytes(),
    )?;
    put(
        frame,
        at,
        &checkpoint.topic_partition.partition_id.get().to_le_bytes(),
    )?;
    put(
        frame,
        at,
        &checkpoint.next_placement_sequence.get().to_le_bytes(),
    )?;
    put(frame, at, &checkpoint.next_offset.get().to_le_bytes())
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests;
