//! Consuming journal/index/application-checkpoint transaction for real log input.
//!
//! This synchronous component is not a factory, worker adapter or HA grant.
//! Only the actual strict dispatcher's single Publish/non-atomic log is joined.

use super::*;
use crate::sink::bounded_callback::JournalCallbackReceipt;
use crate::sink::owned_log::replay::AdoptedAppend;
use crate::sink_journal::{
    AnchoredSinkJournal, JournalAppendKind, JournalAppendLimits, JournalFormat, JournalReplayInput,
};
use crate::stripe::{
    LogIndexPublicationLimits, LogIndexPublicationOutcome, LogPublicationInput,
    PreparedLogIndexPublication, PublishedLogIndex, RetainedLogIndexPublicationError,
};
use shard_stream_core::{LogicalOffset, PlacementSequence};
use shard_stream_engine::{
    DurableAppendDelivery, NativeResumeCallbackError, NativeResumeRefusal, NativeResumeWork,
};
use std::any::Any;
use std::collections::TryReserveError;
use std::error::Error;
use std::mem::{replace, size_of};
use std::ops::{Deref, DerefMut};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::MutexGuard;
use std::sync::TryLockError;

type ApplicationCheckpoints = Arc<Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>>;
type ActivePartitions = Arc<Mutex<HashMap<Arc<str>, Vec<TopicPartition>>>>;
type Cache = HashMap<Arc<str>, Vec<TopicPartition>>;
type CheckpointEntry = (TopicPartition, DurableSinkCheckpoint);

/// Borrowed provenance for one actual global application transaction. Only this
/// module constructs it, while its real checkpoint mutex guard remains held.
/// It carries original native coordinates/work, not a synthetic reservation.
/// The map borrow prevents publication/release until physical append settles.
pub(crate) struct ApplicationJournalEdge<'a> {
    _held_checkpoints: &'a HashMap<TopicPartition, DurableSinkCheckpoint>,
    observed: DurableSinkCheckpoint,
    expected: DurableSinkCheckpoint,
    next: DurableSinkCheckpoint,
    work: &'a NativeResumeWork,
    anchor: &'a AnchoredSinkJournal,
}
impl ApplicationJournalEdge<'_> {
    pub(crate) fn expected(&self) -> DurableSinkCheckpoint {
        self.expected
    }
    pub(crate) fn next(&self) -> DurableSinkCheckpoint {
        self.next
    }
    pub(crate) fn work(&self) -> &NativeResumeWork {
        self.work
    }
    pub(crate) fn matches_anchor(&self, anchor: &AnchoredSinkJournal) -> bool {
        std::ptr::eq(self.anchor, anchor)
    }
    pub(crate) fn permits(
        &self,
        journal: &crate::sink_journal::SinkJournal,
        expected: DurableSinkCheckpoint,
        next: DurableSinkCheckpoint,
    ) -> bool {
        self.anchor.owns_journal(journal)
            && !self.anchor.is_denied()
            && self.anchor.format() == JournalFormat::Application
            && self.expected == expected
            && self.next == next
            && checkpoint_allows_lane_gap(self.observed, self.expected)
    }
}

struct CapturedGuard<'a, T> {
    inner: MutexGuard<'a, T>,
    anchor: Arc<AnchoredSinkJournal>,
    accepted: bool,
}
impl<T> Deref for CapturedGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}
impl<T> DerefMut for CapturedGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}
impl<T> Drop for CapturedGuard<'_, T> {
    fn drop(&mut self) {
        // The real negative dependency is closed before the inner native
        // mutex guard drops, including an unwind or post-physical deadline.
        if !self.accepted {
            self.anchor.deny();
        }
    }
}

/// Finite outer transaction fees; real child producers spend SAME work as well.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LogApplyLimits {
    pub(crate) publication: LogIndexPublicationLimits,
    pub(crate) journal: JournalAppendLimits,
    pub(crate) max_checkpoint_slots: usize,
    pub(crate) max_cache_slots: usize,
    pub(crate) max_cache_name_bytes: usize,
    pub(crate) max_cache_partition_slots: usize,
    pub(crate) max_declared_bytes: usize,
    pub(crate) max_entries: usize,
    pub(crate) max_work_bytes: usize,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct LogApplyUsage {
    pub(crate) declared_bytes: usize,
    pub(crate) entries: usize,
    pub(crate) work_bytes: usize,
}
#[derive(Debug)]
pub(crate) enum LogApplyFailure {
    Refused(NativeResumeRefusal),
    Limits,
    Busy,
    Poisoned,
    Terminal,
    #[allow(
        dead_code,
        reason = "retain the exact static eligibility diagnostic without a refusal allocation"
    )]
    Eligibility(&'static str),
    InvalidEdge,
    Allocation(TryReserveError),
    Preparation,
    Journal,
    Publication,
    Panic,
    State,
}
type Failure = LogApplyFailure;
type Result<T> = std::result::Result<T, Failure>;
type Prepared<I> =
    std::result::Result<PreparedLogIndexPublication<I>, RetainedLogIndexPublicationError<I>>;
type Published<I> = std::result::Result<PublishedLogIndex<I>, RetainedLogIndexPublicationError<I>>;

struct Attempt<I> {
    input: Option<I>,
    expected: DurableSinkCheckpoint,
    next: DurableSinkCheckpoint,
    index_edge: Option<(DurableSinkCheckpoint, DurableSinkCheckpoint)>,
    prepared: Option<Prepared<I>>,
    journal: Option<std::result::Result<JournalCallbackReceipt, NativeResumeCallbackError>>,
    published: Option<Published<I>>,
    observed_checkpoint: Option<DurableSinkCheckpoint>,
    published_checkpoint: Option<DurableSinkCheckpoint>,
    effect: Option<DurableSinkApply>,
    checkpoint_capacity: Option<usize>,
    empty_cache: Option<Cache>,
    retired_cache: Option<Cache>,
    panic: Option<Box<dyn Any + Send>>,
    limits: LogApplyLimits,
    usage: LogApplyUsage,
    work: NativeResumeWork,
    checkpoints: ApplicationCheckpoints,
    active_partitions: ActivePartitions,
    stripe: Option<LogStripe>,
    // LAST: the genuine anchored file/directory and every replay row survive
    // all source/staged/index/checkpoint observations and refused native work.
    anchor: Arc<AnchoredSinkJournal>,
}

/// Real completed transaction or real application-checkpoint conflict.
pub(crate) struct AppliedLogTransaction<I = DurableAppend> {
    retained: Attempt<I>,
}
/// Terminal whole-owner failure, including late durable/published effects.
pub(crate) struct RetainedLogApplyError<I = DurableAppend> {
    pub(crate) cause: Failure,
    retained: Attempt<I>,
}
impl<I: LogPublicationInput> fmt::Debug for AppliedLogTransaction<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppliedLogTransaction")
            .field("effect", &self.retained.effect)
            .field("checkpoint", &self.retained.published_checkpoint)
            .finish_non_exhaustive()
    }
}
impl<I: LogPublicationInput> fmt::Debug for RetainedLogApplyError<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedLogApplyError")
            .field("cause", &self.cause)
            .field("effect", &self.retained.effect)
            .field("observed_checkpoint", &self.retained.observed_checkpoint)
            .field("published_checkpoint", &self.retained.published_checkpoint)
            .field("usage", &self.retained.usage)
            .field("panic", &self.retained.panic.is_some())
            .finish_non_exhaustive()
    }
}
impl<I: LogPublicationInput> fmt::Display for RetainedLogApplyError<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl<I: LogPublicationInput> Error for RetainedLogApplyError<I> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause {
            Failure::Refused(e) => Some(e),
            Failure::Allocation(e) => Some(e),
            Failure::Preparation => self
                .retained
                .prepared
                .as_ref()?
                .as_ref()
                .err()
                .map(|e| e as &dyn Error),
            Failure::Journal => self
                .retained
                .journal
                .as_ref()?
                .as_ref()
                .err()
                .map(|e| e as &dyn Error),
            Failure::Publication => self
                .retained
                .published
                .as_ref()?
                .as_ref()
                .err()
                .map(|e| e as &dyn Error),
            _ => None,
        }
    }
}
impl<I: LogPublicationInput> AppliedLogTransaction<I> {
    pub(crate) fn apply_result(&self) -> Option<DurableSinkApply> {
        self.retained.effect
    }
    #[allow(
        dead_code,
        reason = "preserve actual retained transaction diagnostics without extracting native owners"
    )]
    pub(crate) fn publication(&self) -> Option<&PublishedLogIndex<I>> {
        self.retained.published.as_ref()?.as_ref().ok()
    }
    #[allow(
        dead_code,
        reason = "preserve actual retained transaction diagnostics without extracting native owners"
    )]
    pub(crate) fn journal_outcome(&self) -> Option<&crate::sink_journal::JournalAppendOutcome> {
        self.retained
            .journal
            .as_ref()?
            .as_ref()
            .ok()?
            .journal_outcome()
    }
    /// Only a successful whole transaction may restore ordinary stripe custody.
    /// An unexpected invariant keeps the successful whole owner in Err.
    #[allow(
        clippy::result_large_err,
        reason = "retain complete original owners and native outcomes inline without a refusal allocation"
    )]
    pub(crate) fn into_stripe(mut self) -> std::result::Result<LogStripe, Self> {
        if let Some(stripe) = self.retained.stripe.take() {
            return Ok(stripe);
        }
        if self.retained.published.as_ref().is_some_and(|r| r.is_ok())
            && let Some(Ok(published)) = self.retained.published.take()
        {
            return Ok(published.into_stripe());
        }
        Err(self)
    }
}
impl<I: LogPublicationInput> RetainedLogApplyError<I> {
    #[allow(
        dead_code,
        reason = "preserve actual retained transaction diagnostics without extracting native owners"
    )]
    pub(crate) fn input(&self) -> Option<&I> {
        if let Some(input) = &self.retained.input {
            return Some(input);
        }
        if let Some(prepared) = &self.retained.prepared {
            return Some(match prepared {
                Ok(p) => p.input(),
                Err(e) => e.input(),
            });
        }
        self.retained.published.as_ref().map(|p| match p {
            Ok(p) => p.input(),
            Err(e) => e.input(),
        })
    }
    #[allow(
        dead_code,
        reason = "preserve actual retained transaction diagnostics without extracting native owners"
    )]
    pub(crate) fn journal_error(&self) -> Option<&NativeResumeCallbackError> {
        self.retained.journal.as_ref()?.as_ref().err()
    }
    #[allow(
        dead_code,
        reason = "preserve the original native journal outcome diagnostic without an ownership extractor"
    )]
    pub(crate) fn journal_outcome(&self) -> Option<&crate::sink_journal::JournalAppendOutcome> {
        self.retained
            .journal
            .as_ref()?
            .as_ref()
            .ok()?
            .journal_outcome()
    }
    #[allow(
        dead_code,
        reason = "preserve actual retained transaction diagnostics without extracting native owners"
    )]
    pub(crate) fn application_effect(&self) -> Option<DurableSinkApply> {
        self.retained.effect
    }
    #[allow(
        dead_code,
        reason = "preserve actual retained transaction diagnostics without extracting native owners"
    )]
    pub(crate) fn published_checkpoint(&self) -> Option<DurableSinkCheckpoint> {
        self.retained.published_checkpoint
    }
    #[allow(
        dead_code,
        reason = "preserve actual retained transaction diagnostics without extracting native owners"
    )]
    pub(crate) fn publication_outcome(&self) -> Option<LogIndexPublicationOutcome> {
        match self.retained.published.as_ref()? {
            Ok(p) => p.outcome(),
            Err(e) => e.outcome(),
        }
    }
}

fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or(Failure::Limits)
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or(Failure::Limits)
}

impl LogStripe {
    /// Consume the actual log owner and real supplied native append through
    /// preparation, anchored durable journal, query publication and application
    /// checkpoint. The SAME real namespace/cache owners and work are retained.
    ///
    /// This component does not implement or advertise a sink/factory callback.
    /// The real caller must own its admission/queue/error carrier and preserve
    /// current control until every synchronous phase settles. Setup expiry is
    /// not reused as live permission. Ordinary methods are unchanged.
    #[allow(
        clippy::too_many_arguments,
        reason = "retain exact original owners and ledgers"
    )]
    #[allow(
        clippy::result_large_err,
        reason = "retain complete original owners and native outcomes inline without a refusal allocation"
    )]
    pub(crate) fn apply_durable_log_until(
        self,
        input: DurableAppend,
        expected: DurableSinkCheckpoint,
        next: DurableSinkCheckpoint,
        anchor: Arc<AnchoredSinkJournal>,
        checkpoints: ApplicationCheckpoints,
        active_partitions: ActivePartitions,
        limits: LogApplyLimits,
        work: &NativeResumeWork,
    ) -> std::result::Result<AppliedLogTransaction, RetainedLogApplyError> {
        self.apply_log_input_until(
            input,
            expected,
            next,
            anchor,
            checkpoints,
            active_partitions,
            limits,
            work,
        )
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "retain the exact real source, namespace, policy and original work owners"
    )]
    #[allow(
        clippy::result_large_err,
        reason = "retain original inputs and actual native/published outcomes inline"
    )]
    pub(crate) fn apply_log_input_until<I: LogPublicationInput>(
        self,
        input: I,
        expected: DurableSinkCheckpoint,
        next: DurableSinkCheckpoint,
        anchor: Arc<AnchoredSinkJournal>,
        checkpoints: ApplicationCheckpoints,
        active_partitions: ActivePartitions,
        limits: LogApplyLimits,
        work: &NativeResumeWork,
    ) -> std::result::Result<AppliedLogTransaction<I>, RetainedLogApplyError<I>> {
        let mut retained = Attempt {
            input: Some(input),
            expected,
            next,
            index_edge: None,
            prepared: None,
            journal: None,
            published: None,
            observed_checkpoint: None,
            published_checkpoint: None,
            effect: None,
            checkpoint_capacity: None,
            empty_cache: None,
            retired_cache: None,
            panic: None,
            limits,
            usage: LogApplyUsage::default(),
            work: work.clone(),
            checkpoints,
            active_partitions,
            stripe: Some(self),
            anchor,
        };
        let actual = catch_unwind(AssertUnwindSafe(|| retained.run()));
        let cause = match actual {
            Ok(Ok(())) => return Ok(AppliedLogTransaction { retained }),
            Ok(Err(cause)) => cause,
            Err(payload) => {
                retained.panic = Some(payload);
                Failure::Panic
            }
        };
        // Close this SAME anchored owner before any external guard can release.
        // This is only negative admission, never a retry/permission factory.
        retained.anchor.deny();
        Err(RetainedLogApplyError { cause, retained })
    }
}
impl<I: LogPublicationInput> Attempt<I> {
    fn check(&self) -> Result<()> {
        self.work.check().map_err(Failure::Refused)?;
        if self.anchor.is_denied() {
            return Err(Failure::Terminal);
        }
        Ok(())
    }
    fn pay(&mut self, declared_bytes: usize, entries: usize, work_bytes: usize) -> Result<()> {
        let usage = LogApplyUsage {
            declared_bytes: add(self.usage.declared_bytes, declared_bytes)?,
            entries: add(self.usage.entries, entries)?,
            work_bytes: add(self.usage.work_bytes, work_bytes)?,
        };
        if usage.declared_bytes > self.limits.max_declared_bytes
            || usage.entries > self.limits.max_entries
            || usage.work_bytes > self.limits.max_work_bytes
        {
            return Err(Failure::Limits);
        }
        self.work
            .reserve_values(declared_bytes, 0, entries, work_bytes)
            .map_err(Failure::Refused)?;
        self.usage = usage;
        Ok(())
    }
    fn observed(&self, phase: &'static str) -> Result<()> {
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        tests::after_phase(self.work.absolute_expiry(), phase);
        #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
        let _ = phase;
        self.check()
    }
    fn probe(&mut self, capacity: usize) -> Result<()> {
        self.pay(0, 0, mul(add(capacity, 1)?, size_of::<CheckpointEntry>())?)
    }
    fn edge(&mut self) -> Result<()> {
        let input = self.input.as_ref().ok_or(Failure::State)?;
        if self.anchor.is_denied() {
            return Err(Failure::Terminal);
        }
        if let Some(journal) = input.journal() {
            if self
                .stripe
                .as_ref()
                .ok_or(Failure::State)?
                .requires_log_object_publication()
            {
                return Err(Failure::Eligibility(
                    "actual object tier needs its bounded producer",
                ));
            }
            if !Arc::ptr_eq(journal.anchor(), &self.anchor)
                || journal.expected() != self.expected
                || journal.next() != self.next
            {
                return Err(Failure::InvalidEdge);
            }
            let mut expected = self.expected;
            expected.next_offset = journal.first_offset();
            self.index_edge = Some((expected, self.next));
            return Ok(());
        }
        if !self.anchor.recovered().is_empty()
            && !input.adopted_for(&self.anchor, &self.checkpoints)
        {
            return Err(Failure::Eligibility(
                "actual journal replay has not been adopted",
            ));
        }
        let input = input.live().ok_or(Failure::State)?;
        if input.delivery != DurableAppendDelivery::Publish
            || input.atomic_group.is_some()
            || input.topic_partition().topic_id != TelemetrySignal::Logs.topic_id()
        {
            return Err(Failure::Eligibility(
                "requires actual Publish/non-atomic log input",
            ));
        }
        if self
            .stripe
            .as_ref()
            .ok_or(Failure::State)?
            .requires_log_object_publication()
        {
            return Err(Failure::Eligibility(
                "actual object tier needs its bounded producer",
            ));
        }
        let r = input.reservation;
        let tp = input.topic_partition();
        if input.physical_shard_id != self.anchor.shard_id()
            || self.expected.topic_partition != tp
            || self.next.topic_partition != tp
            || self.expected.next_placement_sequence != r.placement.sequence
            || self.expected.next_offset > r.first_offset
            || r.first_offset
                .get()
                .checked_add(u64::from(r.record_count.get()) - 1)
                != Some(r.last_offset.get())
            || r.last_offset.get().checked_add(1) != Some(self.next.next_offset.get())
            || r.placement.sequence.get().checked_add(1)
                != Some(self.next.next_placement_sequence.get())
        {
            return Err(Failure::InvalidEdge);
        }
        // Only the data-index edge is derived from the actual reservation.
        // Original application expected/next and the lane-gap rule stay intact.
        self.index_edge = Some((
            DurableSinkCheckpoint {
                topic_partition: tp,
                next_offset: r.first_offset,
                next_placement_sequence: r.placement.sequence,
            },
            DurableSinkCheckpoint {
                topic_partition: tp,
                next_offset: LogicalOffset::new(r.last_offset.get() + 1),
                next_placement_sequence: PlacementSequence::new(r.placement.sequence.get() + 1),
            },
        ));
        Ok(())
    }
    fn reserve_checkpoint(
        &mut self,
        map: &mut HashMap<TopicPartition, DurableSinkCheckpoint>,
    ) -> Result<()> {
        let capacity = map.capacity();
        self.checkpoint_capacity = Some(capacity);
        if capacity > self.limits.max_checkpoint_slots {
            return Err(Failure::Limits);
        }
        self.probe(capacity)?;
        if !map.contains_key(&self.next.topic_partition) && map.len() == capacity {
            let n = add(map.len(), 1)?;
            if n > self.limits.max_checkpoint_slots {
                return Err(Failure::Limits);
            }
            let slots = self.limits.max_checkpoint_slots;
            // Prepay the finite declared table ceiling and conservative repeated
            // fixed-key rehash/probe visits before growing actual scalar state.
            let bytes = mul(slots, size_of::<CheckpointEntry>())?;
            let work = mul(
                mul(n, add(add(slots, capacity)?, 1)?)?,
                size_of::<CheckpointEntry>(),
            )?;
            self.pay(bytes, slots, add(bytes, work)?)?;
            map.try_reserve(1).map_err(Failure::Allocation)?;
            self.checkpoint_capacity = Some(map.capacity());
            if map.capacity() > slots {
                let extra = map.capacity() - slots;
                self.pay(
                    mul(extra, size_of::<CheckpointEntry>())?,
                    extra,
                    mul(extra, size_of::<CheckpointEntry>())?,
                )?;
                return Err(Failure::Limits);
            }
            self.observed("checkpoint-capacity")?;
        }
        self.probe(map.capacity())
    }
    fn prepare_cache(&mut self, cache: &Cache) -> Result<()> {
        if cache.capacity() > self.limits.max_cache_slots {
            return Err(Failure::Limits);
        }
        self.pay(
            0,
            0,
            mul(
                add(cache.capacity(), 1)?,
                size_of::<(Arc<str>, Vec<TopicPartition>)>(),
            )?,
        )?;
        let mut names = 0;
        let mut slots = 0;
        for (name, partitions) in cache {
            self.check()?;
            names = add(names, name.len())?;
            slots = add(slots, partitions.capacity())?;
            if names > self.limits.max_cache_name_bytes
                || slots > self.limits.max_cache_partition_slots
            {
                return Err(Failure::Limits);
            }
        }
        let bytes = add(names, mul(slots, size_of::<TopicPartition>())?)?;
        self.pay(
            add(bytes, size_of::<Cache>())?,
            add(slots, 1)?,
            add(bytes, size_of::<Cache>())?,
        )?;
        self.empty_cache = Some(HashMap::with_hasher(cache.hasher().clone()));
        self.check()
    }
    fn run(&mut self) -> Result<()> {
        self.check()?;
        if [
            self.limits.max_checkpoint_slots,
            self.limits.max_cache_slots,
            self.limits.max_cache_name_bytes,
            self.limits.max_cache_partition_slots,
            self.limits.max_declared_bytes,
            self.limits.max_entries,
            self.limits.max_work_bytes,
        ]
        .contains(&0)
        {
            return Err(Failure::Limits);
        }
        let fixed = add(
            add(size_of::<Self>(), size_of::<RetainedLogApplyError<I>>())?,
            add(
                size_of::<CapturedGuard<'_, HashMap<TopicPartition, DurableSinkCheckpoint>>>(),
                size_of::<CapturedGuard<'_, Cache>>(),
            )?,
        )?;
        self.pay(fixed, 4, fixed)?;
        self.edge()?;
        let checkpoints_owner = Arc::clone(&self.checkpoints);
        let checkpoint_guard = match checkpoints_owner.try_lock() {
            Ok(g) => g,
            Err(TryLockError::WouldBlock) => return Err(Failure::Busy),
            Err(TryLockError::Poisoned(e)) => {
                self.anchor.deny();
                drop(e.into_inner());
                return Err(Failure::Poisoned);
            }
        };
        let mut checkpoints = CapturedGuard {
            inner: checkpoint_guard,
            anchor: Arc::clone(&self.anchor),
            accepted: false,
        };
        let actual = catch_unwind(AssertUnwindSafe(|| self.run_checkpoint(&mut checkpoints)));
        let result = self.capture(actual).and_then(|()| self.check());
        if result.is_ok() {
            checkpoints.accepted = true;
        }
        result
    }
    fn capture(&mut self, actual: std::thread::Result<Result<()>>) -> Result<()> {
        match actual {
            Ok(result) => result,
            Err(payload) => {
                self.panic = Some(payload);
                Err(Failure::Panic)
            }
        }
    }
    fn run_checkpoint(
        &mut self,
        checkpoints: &mut CapturedGuard<'_, HashMap<TopicPartition, DurableSinkCheckpoint>>,
    ) -> Result<()> {
        self.probe(checkpoints.capacity())?;
        let actual = checkpoints
            .get(&self.expected.topic_partition)
            .copied()
            .unwrap_or_else(|| DurableSinkCheckpoint::initial(self.expected.topic_partition));
        self.observed_checkpoint = Some(actual);
        self.observed("application-observed")?;
        if !checkpoint_allows_lane_gap(actual, self.expected) {
            self.effect = Some(DurableSinkApply::CheckpointConflict(actual));
            self.observed("checkpoint-conflict")?;
            return Ok(());
        }
        self.reserve_checkpoint(checkpoints)?;
        let cache_owner = Arc::clone(&self.active_partitions);
        let cache_guard = match cache_owner.try_lock() {
            Ok(g) => g,
            Err(TryLockError::WouldBlock) => return Err(Failure::Busy),
            Err(TryLockError::Poisoned(e)) => {
                self.anchor.deny();
                drop(e.into_inner());
                return Err(Failure::Poisoned);
            }
        };
        let mut cache = CapturedGuard {
            inner: cache_guard,
            anchor: Arc::clone(&self.anchor),
            accepted: false,
        };
        let actual = catch_unwind(AssertUnwindSafe(|| {
            self.run_locked(checkpoints, &mut cache)
        }));
        let result = self.capture(actual).and_then(|()| self.check());
        if result.is_ok() {
            cache.accepted = true;
        }
        result
    }
    fn run_locked(
        &mut self,
        checkpoints: &mut HashMap<TopicPartition, DurableSinkCheckpoint>,
        cache: &mut Cache,
    ) -> Result<()> {
        if self.anchor.format() == JournalFormat::Application {
            let seal = add(
                size_of::<ApplicationJournalEdge<'_>>(),
                add(
                    size_of::<Option<&ApplicationJournalEdge<'_>>>(),
                    add(size_of::<JournalFormat>(), size_of::<bool>())?,
                )?,
            )?;
            self.pay(seal, 4, seal)?;
        }
        self.prepare_cache(cache)?;
        let (expected, next) = self.index_edge.ok_or(Failure::State)?;
        self.prepared = Some(
            self.stripe
                .take()
                .ok_or(Failure::State)?
                .prepare_log_input_until(
                    self.input.take().ok_or(Failure::State)?,
                    expected,
                    next,
                    self.limits.publication,
                    &self.work,
                ),
        );
        self.observed("index-prepared")?;
        let prepared = self
            .prepared
            .as_ref()
            .and_then(|p| p.as_ref().ok())
            .ok_or(Failure::Preparation)?;
        if prepared.outcome().is_some() {
            return Err(Failure::Eligibility(
                "watermark coverage is not an application/content proof",
            ));
        }
        // These final scalar/map/cache movements are paid BEFORE native I/O.
        self.pay(
            0,
            0,
            size_of::<CheckpointEntry>() + 2 * size_of::<Cache>() + size_of::<DurableSinkApply>(),
        )?;
        let prepared = self
            .prepared
            .as_ref()
            .and_then(|p| p.as_ref().ok())
            .ok_or(Failure::State)?;
        self.check()?;
        if let Some(input) = prepared.input().live() {
            self.journal = Some(if self.anchor.format() == JournalFormat::Application {
                // This charged seal borrows the actual held map and SAME work.
                // The global comparison occurred under this unchanged guard;
                // neither journal/index settlement can release or advance it.
                let edge = ApplicationJournalEdge {
                    _held_checkpoints: checkpoints,
                    observed: self.observed_checkpoint.ok_or(Failure::State)?,
                    expected: self.expected,
                    next: self.next,
                    work: &self.work,
                    anchor: &self.anchor,
                };
                self.anchor.append_application_journal_until(
                    &edge,
                    std::slice::from_ref(input),
                    self.limits.journal,
                )
            } else {
                self.anchor.append_journal_until(
                    self.expected,
                    std::slice::from_ref(input),
                    self.next,
                    self.limits.journal,
                    &self.work,
                )
            });
            self.observed("journal-settled")?;
            if self.journal.as_ref().is_none_or(|j| j.is_err()) {
                return Err(Failure::Journal);
            }
            if !matches!(
                self.journal.as_ref().and_then(|j| j.as_ref().ok())
                    .and_then(|j| j.journal_outcome()).map(|j| j.kind),
                Some(JournalAppendKind::Committed(checkpoint)) if checkpoint == self.next
            ) {
                return Err(Failure::Eligibility(
                    "journal watermark coverage is not this input's durable identity",
                ));
            }
        }
        // Both actual result variants are saved before observing post-native E.
        match self.prepared.take() {
            Some(Ok(prepared)) => self.published = Some(prepared.commit_until()),
            other => {
                self.prepared = other;
                return Err(Failure::State);
            }
        }
        self.observed("index-settled")?;
        let published = self
            .published
            .as_ref()
            .and_then(|p| p.as_ref().ok())
            .ok_or(Failure::Publication)?;
        if !matches!(
            published.outcome(),
            Some(LogIndexPublicationOutcome::Published { .. })
        ) {
            return Err(Failure::State);
        }
        self.check()?;
        let empty = self.empty_cache.take().ok_or(Failure::State)?;
        // Admitted insertion, no allocation/user callback/E check within the
        // actual cache/checkpoint/effect publication. Capture truth first.
        self.retired_cache = Some(replace(cache, empty));
        checkpoints.insert(self.next.topic_partition, self.next);
        self.published_checkpoint = Some(self.next);
        if self
            .published
            .as_ref()
            .and_then(|p| p.as_ref().ok())
            .is_some_and(|p| p.input().live().is_some())
        {
            self.effect = Some(DurableSinkApply::Applied);
        }
        self.observed("application-published")?;
        Ok(())
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests;

impl AppliedLogTransaction<JournalReplayInput> {
    pub(crate) fn replayed_checkpoint(&self) -> Option<DurableSinkCheckpoint> {
        self.retained.published_checkpoint
    }
}
pub(crate) type ReplayedLogTransaction = AppliedLogTransaction<JournalReplayInput>;
pub(crate) type AdoptedLogTransaction = AppliedLogTransaction<AdoptedAppend>;
