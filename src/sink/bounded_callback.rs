//! Synchronous callback prerequisites; no ordinary index/worker is entered.
//!
//! The real factory checkpoint is an indexed/application checkpoint. The
//! journal bridge has a different result type and cannot return Applied.

use super::*;
use crate::sink_journal::{
    JournalAppendLimits, JournalAppendOutcome, JournalTransaction, RetainedJournalAppendError,
};
use shard_stream_engine::{
    NativeResumeCallbackError, NativeResumeCallbackResult, NativeResumeRefusal, NativeResumeWork,
};
use std::any::Any;
use std::collections::TryReserveError;
use std::error::Error;
use std::mem::{size_of, take};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::TryLockError;
use std::sync::atomic::AtomicUsize;

type Checkpoints = Arc<Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>>;

// Selected bytes 1.12.1: bytes.rs::shallow_clone_vec may allocate one Shared
// before CAS, even for a losing promoter. Its three equally aligned word
// fields are (*mut u8, usize, AtomicUsize). Other clone vtables do not allocate.
// This is a source-derived header request, not allocator/RSS measurement.
const BYTES_PROMOTION_BYTES: usize = size_of::<(*mut u8, usize, AtomicUsize)>();

#[derive(Debug)]
enum Failure {
    Refused(NativeResumeRefusal),
    Busy,
    Poisoned,
    Limits,
    Allocation(TryReserveError),
    Journal,
    Panic,
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bounded telemetry callback refused: {self:?}")
    }
}
fn add(a: usize, b: usize) -> Result<usize, Failure> {
    a.checked_add(b).ok_or(Failure::Limits)
}
fn mul(a: usize, b: usize) -> Result<usize, Failure> {
    a.checked_mul(b).ok_or(Failure::Limits)
}
fn pay(
    work: &NativeResumeWork,
    bytes: usize,
    entries: usize,
    effort: usize,
) -> Result<(), Failure> {
    work.reserve_values(bytes, 0, entries, effort)
        .map_err(Failure::Refused)
}
fn check(work: &NativeResumeWork) -> Result<(), Failure> {
    work.check().map_err(Failure::Refused)
}

struct CheckpointCarrier {
    partition: TopicPartition,
    observed: Option<Option<DurableSinkCheckpoint>>,
    observed_capacity: Option<usize>,
    failure: Option<Failure>,
    panic: Option<Box<dyn Any + Send>>,
    work: NativeResumeWork,
    // Retain the actual map and its poison state, after scalar observations.
    owner: Checkpoints,
}
impl fmt::Debug for CheckpointCarrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedCheckpointCallback")
            .field("failure", &self.failure)
            .field("observed", &self.observed)
            .field("capacity", &self.observed_capacity)
            .field("panicked", &self.panic.is_some())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for CheckpointCarrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for CheckpointCarrier {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.failure {
            Some(Failure::Refused(error)) => Some(error),
            _ => None,
        }
    }
}
impl CheckpointCarrier {
    fn read(&mut self) -> Result<Option<DurableSinkCheckpoint>, Failure> {
        check(&self.work)?;
        let map = match self.owner.try_lock() {
            Ok(map) => map,
            Err(TryLockError::WouldBlock) => return Err(Failure::Busy),
            Err(TryLockError::Poisoned(error)) => {
                // A borrowed MutexGuard cannot enter a Send+'static error.
                // Release that guard; retain the SAME map and real poison
                // state, and report the actual native error discriminant.
                drop(error.into_inner());
                return Err(Failure::Poisoned);
            }
        };
        let capacity = map.capacity();
        self.observed_capacity = Some(capacity);
        pay(
            &self.work,
            0,
            0,
            mul(
                add(capacity, 1)?,
                size_of::<(TopicPartition, DurableSinkCheckpoint)>(),
            )?,
        )?;
        check(&self.work)?;
        self.observed = Some(map.get(&self.partition).copied());
        drop(map);
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        tests::after_checkpoint(self.work.absolute_expiry());
        check(&self.work)?;
        self.observed.ok_or(Failure::Limits)
    }
}

/// Read only the real application checkpoint, never the journal checkpoint.
pub(super) fn load_checkpoint_until(
    owner: &Checkpoints,
    partition: TopicPartition,
    work: &NativeResumeWork,
) -> NativeResumeCallbackResult<Option<DurableSinkCheckpoint>> {
    // No owned input/native phase exists on initial admission refusal. The
    // original typed work refusal fits the engine's inline error variant.
    work.reserve_values(
        size_of::<CheckpointCarrier>(),
        0,
        1,
        size_of::<CheckpointCarrier>(),
    )
    .map_err(NativeResumeCallbackError::refused)?;
    let mut carrier = Box::new(CheckpointCarrier {
        partition,
        observed: None,
        observed_capacity: None,
        failure: None,
        panic: None,
        work: work.clone(),
        owner: Arc::clone(owner),
    });
    let actual = catch_unwind(AssertUnwindSafe(|| carrier.read()));
    let failure = match actual {
        Ok(Ok(value)) => return Ok(value),
        Ok(Err(error)) => error,
        Err(payload) => {
            carrier.panic = Some(payload);
            Failure::Panic
        }
    };
    carrier.failure = Some(failure);
    // Coercion reuses the already-admitted actual allocation.
    Err(NativeResumeCallbackError::from_external(carrier))
}

struct JournalCarrier {
    expected: DurableSinkCheckpoint,
    next: DurableSinkCheckpoint,
    source_len: usize,
    copied: Vec<DurableAppend>,
    outcome: Option<Result<JournalAppendOutcome, RetainedJournalAppendError>>,
    failure: Option<Failure>,
    panic: Option<Box<dyn Any + Send>>,
    work: NativeResumeWork,
    // The exact journal owner outlives the carrier's copy/result observations.
    journal: Arc<SinkJournal>,
}
impl fmt::Debug for JournalCarrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedJournalCallback")
            .field("failure", &self.failure)
            .field("source_len", &self.source_len)
            .field("copied_len", &self.copied.len())
            .field("copied_capacity", &self.copied.capacity())
            .field("outcome", &self.outcome)
            .field("panicked", &self.panic.is_some())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for JournalCarrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Error for JournalCarrier {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.failure {
            Some(Failure::Refused(error)) => Some(error),
            Some(Failure::Allocation(error)) => Some(error),
            Some(Failure::Journal) => self
                .outcome
                .as_ref()
                .and_then(|result| result.as_ref().err())
                .map(|error| error as &(dyn Error + 'static)),
            _ => None,
        }
    }
}

/// A journal-only observation. This cannot be converted into Applied here.
#[derive(Debug)]
pub(crate) struct JournalCallbackReceipt {
    retained: Box<JournalCarrier>,
}
impl JournalCallbackReceipt {
    /// Borrow actual journal input/effects for a future bounded index producer.
    pub(crate) fn journal_outcome(&self) -> Option<&JournalAppendOutcome> {
        self.retained.outcome.as_ref()?.as_ref().ok()
    }
}
impl JournalCarrier {
    fn run(
        &mut self,
        appends: &[DurableAppend],
        limits: JournalAppendLimits,
        edge: Option<&super::ApplicationJournalEdge<'_>>,
    ) -> Result<(), Failure> {
        check(&self.work)?;
        if [
            limits.max_appends,
            limits.max_input_slots,
            limits.max_payload_bytes,
            limits.max_input_bytes,
        ]
        .into_iter()
        .any(|value| value == 0 || value > isize::MAX as usize)
            || appends.len() > limits.max_appends
            || appends.len() > limits.max_input_slots
        {
            return Err(Failure::Limits);
        }
        let slots = mul(appends.len(), size_of::<DurableAppend>())?;
        // Prepay the whole borrowed descriptor census before inspecting it.
        pay(&self.work, 0, 0, slots)?;
        let mut visible = 0;
        let mut payloads = 0;
        let mut clones = 0;
        for append in appends {
            check(&self.work)?;
            payloads = add(payloads, append.payload.len())?;
            visible = add(visible, append.payload.len())?;
            clones = add(clones, 1)?;
            if let Some(context) = &append.transient_context {
                visible = add(visible, context.len())?;
                clones = add(clones, 1)?;
            }
            if visible > limits.max_input_bytes || payloads > limits.max_payload_bytes {
                return Err(Failure::Limits);
            }
        }
        let promotions = mul(clones, BYTES_PROMOTION_BYTES)?;
        let declared = add(add(slots, visible)?, promotions)?;
        // All visible source retention, possible clone headers and actual copy
        // work are admitted BEFORE any borrowed descriptor is cloned.
        pay(&self.work, declared, add(appends.len(), clones)?, declared)?;
        if let Err(error) = self.copied.try_reserve_exact(appends.len()) {
            return Err(Failure::Allocation(error));
        }
        let excess = self.copied.capacity().saturating_sub(appends.len());
        pay(
            &self.work,
            mul(excess, size_of::<DurableAppend>())?,
            excess,
            mul(excess, size_of::<DurableAppend>())?,
        )?;
        if self.copied.capacity() > limits.max_input_slots {
            return Err(Failure::Limits);
        }
        check(&self.work)?;
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        tests::before_copy(appends, &self.work);
        for append in appends {
            check(&self.work)?;
            // This explicit clone can promote payload/context ownership; its
            // worst-case actual Shared requests were independently prepaid.
            self.copied.push(append.clone());
            #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
            tests::after_copy(self.work.absolute_expiry());
            check(&self.work)?;
        }
        let input = JournalTransaction {
            expected: self.expected,
            appends: take(&mut self.copied),
            next: self.next,
        };
        self.outcome = Some(match edge {
            Some(edge) => self.journal.append_application_until(input, limits, edge),
            None => self.journal.append_until(input, limits, &self.work),
        });
        // Store the real entire native result BEFORE later clocks/hooks. No
        // new allocation is needed to carry either journal result variant.
        if self.outcome.as_ref().is_some_and(Result::is_err) {
            return Err(Failure::Journal);
        }
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        tests::after_journal(self.work.absolute_expiry());
        check(&self.work)
    }
}

/// Necessary synchronous bridge for a later genuine bounded apply adapter.
///
/// The caller retains the original borrowed source through this call. The
/// actual native engine keeps current_append on callback refusal; this helper
/// retains every successfully copied prefix and the whole input after journal
/// entry. It cannot own unprocessed borrowed elements on a copy refusal.
/// No worker/channel, application checkpoint update or Applied result occurs.
pub(crate) fn append_journal_until(
    journal: &Arc<SinkJournal>,
    expected: DurableSinkCheckpoint,
    appends: &[DurableAppend],
    next: DurableSinkCheckpoint,
    limits: JournalAppendLimits,
    work: &NativeResumeWork,
) -> NativeResumeCallbackResult<JournalCallbackReceipt> {
    append_journal_entry_until(journal, expected, appends, next, limits, work, None)
}

/// Closed application-edge bridge: every coordinate and the original work are
/// borrowed from the actual held global-map proof, never supplied separately.
pub(crate) fn append_application_journal_until(
    journal: &Arc<SinkJournal>,
    edge: &super::ApplicationJournalEdge<'_>,
    appends: &[DurableAppend],
    limits: JournalAppendLimits,
) -> NativeResumeCallbackResult<JournalCallbackReceipt> {
    append_journal_entry_until(
        journal,
        edge.expected(),
        appends,
        edge.next(),
        limits,
        edge.work(),
        Some(edge),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "preserve original callback source and closed application-edge proof"
)]
fn append_journal_entry_until(
    journal: &Arc<SinkJournal>,
    expected: DurableSinkCheckpoint,
    appends: &[DurableAppend],
    next: DurableSinkCheckpoint,
    limits: JournalAppendLimits,
    work: &NativeResumeWork,
    edge: Option<&super::ApplicationJournalEdge<'_>>,
) -> NativeResumeCallbackResult<JournalCallbackReceipt> {
    work.reserve_values(
        size_of::<JournalCarrier>(),
        0,
        1,
        size_of::<JournalCarrier>(),
    )
    .map_err(NativeResumeCallbackError::refused)?;
    let mut carrier = Box::new(JournalCarrier {
        expected,
        next,
        source_len: appends.len(),
        copied: Vec::new(),
        outcome: None,
        failure: None,
        panic: None,
        work: work.clone(),
        journal: Arc::clone(journal),
    });
    let actual = catch_unwind(AssertUnwindSafe(|| carrier.run(appends, limits, edge)));
    let failure = match actual {
        Ok(Ok(())) => return Ok(JournalCallbackReceipt { retained: carrier }),
        Ok(Err(error)) => error,
        Err(payload) => {
            carrier.panic = Some(payload);
            Failure::Panic
        }
    };
    carrier.failure = Some(failure);
    Err(NativeResumeCallbackError::from_external(carrier))
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests;
