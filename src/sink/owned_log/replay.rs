//! Actual anchored-journal adoption on the first original-work checkpoint turn.
use super::*;
use crate::sink_journal::RecoveredTransaction;
use std::time::Instant;

/// Additional finite census and replay ceilings, within SAME native work.
#[derive(Clone, Copy, Debug)]
pub struct OwnedLogReplayLimits {
    /// Largest complete configured logical partition count.
    pub max_partitions: usize,
    /// Largest supplied configured partition vector capacity.
    pub max_partition_slots: usize,
    /// Largest cumulative transaction count over all actual anchors.
    pub max_transactions: usize,
    /// Largest sum of original recovered transaction vector capacities.
    pub max_transaction_slots: usize,
    /// Largest cumulative actual source payload bytes.
    pub max_payload_bytes: usize,
    /// Additional declared replay state ceiling within original work.
    pub max_declared_bytes: usize,
    /// Additional cumulative replay element/visit ceiling.
    pub max_entries: usize,
    /// Additional cumulative replay comparison/hash/copy work ceiling.
    pub max_work_bytes: usize,
}
impl OwnedLogReplayLimits {
    fn validate(self) -> Result<(), Failure> {
        if [
            self.max_partitions,
            self.max_partition_slots,
            self.max_transactions,
            self.max_transaction_slots,
            self.max_payload_bytes,
            self.max_declared_bytes,
            self.max_entries,
            self.max_work_bytes,
        ]
        .into_iter()
        .any(|n| n == 0 || n > isize::MAX as usize)
        {
            return Err(Failure::Limits);
        }
        Ok(())
    }
}
/// Complete configured logical log scope. These ordinary source names confer
/// neither current control nor HA/query ownership. The host supplies its actual
/// complete scope; every recovered row must belong to it.
#[derive(Debug)]
pub struct OwnedLogReplayPlan {
    /// Actual complete configured log namespace, with no duplicate partitions.
    pub partitions: Vec<TopicPartition>,
    /// Positive finite local ceilings; original native work remains cumulative.
    pub limits: OwnedLogReplayLimits,
}

/// Closed historical adoption receipt for one exact anchor and application map.
/// The shared completion bit is set only after this factory's entire plan.
pub(crate) struct JournalAdoption {
    anchor: Arc<AnchoredSinkJournal>,
    checkpoints: Checkpoints,
    complete: Arc<AtomicBool>,
}
impl JournalAdoption {
    pub(super) fn new(
        anchor: Arc<AnchoredSinkJournal>,
        checkpoints: Checkpoints,
        complete: Arc<AtomicBool>,
    ) -> Self {
        Self {
            anchor,
            checkpoints,
            complete,
        }
    }
    pub(super) fn complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
    pub(crate) fn permits(
        &self,
        anchor: &Arc<AnchoredSinkJournal>,
        checkpoints: &Checkpoints,
    ) -> bool {
        self.complete()
            && !self.anchor.is_denied()
            && Arc::ptr_eq(anchor, &self.anchor)
            && Arc::ptr_eq(checkpoints, &self.checkpoints)
    }
}
/// A real live append plus the exact successful adoption receipt. It preserves
/// every original reservation, delivery, epoch, producer and payload field.
pub(crate) struct AdoptedAppend {
    input: DurableAppend,
    adoption: Arc<JournalAdoption>,
}
impl AdoptedAppend {
    pub(super) fn new(input: DurableAppend, adoption: Arc<JournalAdoption>) -> Self {
        Self { input, adoption }
    }
    pub(crate) fn input(&self) -> &DurableAppend {
        &self.input
    }
    pub(crate) fn adoption(&self) -> &JournalAdoption {
        &self.adoption
    }
}

pub(super) struct Replay {
    plan: OwnedLogReplayPlan,
    // Outside Namespace/Shared: a retained attempt owns workers but never this
    // gate/factory, so factory -> attempt -> worker -> Namespace has no cycle.
    retained: Mutex<Option<Arc<Mutex<Attempt>>>>,
}
impl Replay {
    pub(super) fn new(plan: OwnedLogReplayPlan) -> Self {
        Self {
            plan,
            retained: Mutex::new(None),
        }
    }
    pub(super) fn adopt(
        &self,
        factory: &OwnedLogSinkFactory,
        partition: TopicPartition,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<()> {
        let slots = self.plan.partitions.capacity();
        work.reserve_values(
            0,
            0,
            self.plan.partitions.len(),
            mul(
                add(slots, 1).map_err(callback)?,
                2 * size_of::<TopicPartition>(),
            )
            .map_err(callback)?,
        )
        .map_err(NativeResumeCallbackError::refused)?;
        if !self.plan.partitions.contains(&partition) {
            factory.begin_draining();
            return Err(NativeResumeCallbackError::unsupported(
                "checkpoint outside configured replay scope",
            ));
        }
        if factory
            .namespace
            .adoption
            .as_ref()
            .is_some_and(|a| a.load(Ordering::Acquire))
        {
            return work.check().map_err(NativeResumeCallbackError::refused);
        }
        if work.absolute_expiry() > factory.setup_expiry {
            factory.begin_draining();
            return Err(NativeResumeCallbackError::unsupported(
                "replay work exceeds original source expiry",
            ));
        }
        // Error/attempt and complete roots are prepaid before any gate/worker
        // callback. The same attempt remains outside every physical mutation.
        let fixed = add(
            size_of::<ReplayError>(),
            arc_bytes::<Mutex<Attempt>>().map_err(callback)?,
        )
        .map_err(callback)?;
        work.reserve_values(fixed, 0, 2, fixed)
            .map_err(NativeResumeCallbackError::refused)?;
        let held = Arc::new(Mutex::new(Attempt::new(
            work.clone(),
            self.plan.limits,
            factory.setup_expiry,
        )));
        let error = Box::new(ReplayError {
            retained: Arc::clone(&held),
        });
        let mut attempt = held
            .lock()
            .map_err(|_| NativeResumeCallbackError::unsupported("fresh replay attempt poisoned"))?;
        let prepare = catch_unwind(AssertUnwindSafe(|| attempt.prepare(factory, &self.plan)));
        if !attempt.capture(prepare) {
            factory.begin_draining();
            drop(attempt);
            return Err(NativeResumeCallbackError::from_external(error));
        }
        let mut gate = match self.retained.try_lock() {
            Ok(gate) => gate,
            Err(TryLockError::WouldBlock) => {
                attempt.failure = Some(Failure::Busy);
                factory.begin_draining();
                drop(attempt);
                return Err(NativeResumeCallbackError::from_external(error));
            }
            Err(TryLockError::Poisoned(poison)) => {
                drop(poison.into_inner());
                attempt.failure = Some(Failure::Poisoned);
                factory.begin_draining();
                drop(attempt);
                return Err(NativeResumeCallbackError::from_external(error));
            }
        };
        if gate.is_some() || factory.namespace.denied.load(Ordering::Acquire) {
            attempt.failure = Some(Failure::Closed);
            factory.begin_draining();
            drop(attempt);
            return Err(NativeResumeCallbackError::from_external(error));
        }
        *gate = Some(Arc::clone(&held));
        let actual = catch_unwind(AssertUnwindSafe(|| attempt.run()));
        if !attempt.capture(actual) {
            factory.begin_draining();
            if let Some(current) = &mut attempt.current {
                current.settle_error();
            }
            drop(attempt);
            return Err(NativeResumeCallbackError::from_external(error));
        }
        // No fallible operation or callback lies between the final actual check
        // and whole-plan completion. A later outer refusal still denies serving.
        let complete =
            factory.namespace.adoption.as_ref().ok_or_else(|| {
                NativeResumeCallbackError::unsupported("replay namespace missing")
            })?;
        complete.store(true, Ordering::Release);
        attempt.accepted = true;
        Ok(())
    }
}
fn callback(failure: Failure) -> NativeResumeCallbackError {
    match failure {
        Failure::Refused(value) => NativeResumeCallbackError::refused(value),
        _ => NativeResumeCallbackError::unsupported("bounded journal replay refused"),
    }
}
pub(super) struct ReplayError {
    pub(super) retained: Arc<Mutex<Attempt>>,
}
impl fmt::Debug for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A Busy observer must never wait for another physical callback.
        if let Ok(attempt) = self.retained.try_lock() {
            return f
                .debug_struct("RetainedJournalReplay")
                .field("completed", &attempt.completed)
                .field("accepted", &attempt.accepted)
                .field("failure", &attempt.failure)
                .field("panic", &attempt.panic.is_some())
                .finish_non_exhaustive();
        }
        f.write_str("actual journal replay refused; source and physical outcomes retained")
    }
}
impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for ReplayError {}

#[derive(Clone, Copy)]
struct Descriptor {
    worker: usize,
    row: usize,
    expected: DurableSinkCheckpoint,
    next: DurableSinkCheckpoint,
}
impl Descriptor {
    fn key(self) -> (u128, u32, u64) {
        (
            self.expected.topic_partition.topic_id.get(),
            self.expected.topic_partition.partition_id.get(),
            self.expected.next_placement_sequence.get(),
        )
    }
}
pub(super) struct Attempt {
    limits: OwnedLogReplayLimits,
    usage: (usize, usize, usize),
    original_expiry: Instant,
    work: NativeResumeWork,
    roots: Vec<Arc<OwnedLogWorker>>,
    namespace: Option<Arc<Namespace>>,
    partitions: Vec<TopicPartition>,
    rows: Vec<Descriptor>,
    pub(super) current: Option<Box<Carrier>>,
    pub(super) completed: usize,
    pub(super) accepted: bool,
    pub(super) failure: Option<Failure>,
    panic: Option<Box<dyn Any + Send>>,
    hasher: Option<blake3::Hasher>,
}
impl Attempt {
    fn new(work: NativeResumeWork, limits: OwnedLogReplayLimits, original_expiry: Instant) -> Self {
        Self {
            limits,
            usage: (0, 0, 0),
            original_expiry,
            work,
            roots: Vec::new(),
            namespace: None,
            partitions: Vec::new(),
            rows: Vec::new(),
            current: None,
            completed: 0,
            accepted: false,
            failure: None,
            panic: None,
            hasher: None,
        }
    }
    fn check(&self) -> Result<(), Failure> {
        self.work.check().map_err(Failure::Refused)?;
        if Instant::now() >= self.original_expiry
            || self
                .namespace
                .as_ref()
                .is_some_and(|namespace| namespace.denied.load(Ordering::Acquire))
        {
            return Err(Failure::Closed);
        }
        Ok(())
    }
    fn pay(&mut self, bytes: usize, entries: usize, work: usize) -> Result<(), Failure> {
        let next = (
            add(self.usage.0, bytes)?,
            add(self.usage.1, entries)?,
            add(self.usage.2, work)?,
        );
        if next.0 > self.limits.max_declared_bytes
            || next.1 > self.limits.max_entries
            || next.2 > self.limits.max_work_bytes
        {
            return Err(Failure::Limits);
        }
        self.work
            .reserve_values(bytes, 0, entries, work)
            .map_err(Failure::Refused)?;
        self.usage = next;
        self.check()
    }
    fn capture(&mut self, actual: std::thread::Result<Result<(), Failure>>) -> bool {
        match actual {
            Ok(Ok(())) => true,
            Ok(Err(e)) => {
                self.failure = Some(e);
                false
            }
            Err(p) => {
                self.panic = Some(p);
                self.failure = Some(Failure::Panic);
                false
            }
        }
    }
    fn prepare(
        &mut self,
        factory: &OwnedLogSinkFactory,
        plan: &OwnedLogReplayPlan,
    ) -> Result<(), Failure> {
        self.limits.validate()?;
        let n = factory.sinks.len();
        let p = plan.partitions.len();
        self.pay(size_of::<Arc<Namespace>>(), 1, size_of::<Arc<Namespace>>())?;
        self.namespace = Some(Arc::clone(&factory.namespace));
        self.pay(
            mul(n, size_of::<Arc<OwnedLogWorker>>())?,
            n,
            mul(
                factory.sinks.capacity(),
                size_of::<Arc<ShardTelemetryStripeSink>>(),
            )?,
        )?;
        self.roots
            .try_reserve_exact(n)
            .map_err(Failure::Allocation)?;
        let roots_excess = self.roots.capacity().saturating_sub(n);
        self.pay(
            mul(roots_excess, size_of::<Arc<OwnedLogWorker>>())?,
            roots_excess,
            mul(roots_excess, size_of::<Arc<OwnedLogWorker>>())?,
        )?;
        for sink in &factory.sinks {
            let ShardSinkOwner::Owned(worker) = &sink.state else {
                return Err(Failure::InvalidSource);
            };
            self.roots.push(Arc::clone(worker));
        }
        self.pay(
            mul(p, size_of::<TopicPartition>())?,
            p,
            mul(plan.partitions.capacity(), size_of::<TopicPartition>())?,
        )?;
        self.partitions
            .try_reserve_exact(p)
            .map_err(Failure::Allocation)?;
        let partitions_excess = self.partitions.capacity().saturating_sub(p);
        self.pay(
            mul(partitions_excess, size_of::<TopicPartition>())?,
            partitions_excess,
            mul(partitions_excess, size_of::<TopicPartition>())?,
        )?;
        self.partitions.extend_from_slice(&plan.partitions);
        self.check()
    }
    fn run(&mut self) -> Result<(), Failure> {
        self.census()?;
        // Insertion sort has explicit paid comparisons and movement; no opaque
        // comparator can hide input-dependent work from the original ledger.
        for index in 1..self.rows.len() {
            let mut at = index;
            while at > 0 {
                self.pay(0, 1, 2 * size_of::<Descriptor>())?;
                if self.rows[at - 1].key() <= self.rows[at].key() {
                    break;
                }
                self.pay(0, 0, 2 * size_of::<Descriptor>())?;
                self.rows.swap(at - 1, at);
                at -= 1;
            }
        }
        let mut checkpoint = None;
        // Complete global chain, including all foreign/duplicate/overlap checks,
        // is proved before the first worker receives a mutation command.
        for index in 0..self.rows.len() {
            self.pay(0, 1, 2 * size_of::<Descriptor>())?;
            let row = self.rows[index];
            let actual = checkpoint
                .filter(|c: &DurableSinkCheckpoint| {
                    c.topic_partition == row.expected.topic_partition
                })
                .unwrap_or_else(|| DurableSinkCheckpoint::initial(row.expected.topic_partition));
            if !checkpoint_allows_lane_gap(actual, row.expected) {
                return Err(Failure::InvalidSource);
            }
            checkpoint = Some(row.next);
        }
        for index in 0..self.rows.len() {
            let row = self.rows[index];
            let bytes = add(
                size_of::<Carrier>(),
                add(size_of::<Command>(), size_of::<JournalReplayInput>())?,
            )?;
            self.pay(bytes, 3, bytes)?;
            let worker = Arc::clone(&self.roots[row.worker]);
            self.current = Some(Box::new(Carrier {
                command: Some(Box::new(Command {
                    input: None,
                    adopted_input: None,
                    adopted_result: None,
                    replay_input: None,
                    replay_result: None,
                    expected: row.expected,
                    next: row.next,
                    result: None,
                    effect: None,
                    panic: None,
                    settled: false,
                    work: self.work.clone(),
                })),
                failure: None,
                panic: None,
                worker,
            }));
            let current = self.current.as_mut().ok_or(Failure::State)?;
            let input = current
                .worker
                .shared
                .anchor
                .replay_input(row.row)
                .ok_or(Failure::InvalidSource)?;
            current.command.as_mut().ok_or(Failure::State)?.replay_input = Some(input);
            current
                .worker
                .shared
                .observed(current.command.as_ref().ok_or(Failure::State)?, "copied");
            let value = current.run_command(&self.work)?;
            if !matches!(value, CommandValue::Replay(actual) if actual == row.next) {
                return Err(Failure::State);
            }
            self.completed += 1;
            // Successful prefix lives in the real worker stripe and application
            // map. Its retained source rows stay rooted; no journal bytes move.
            self.current = None;
            self.check()?;
        }
        self.check()
    }
    fn census(&mut self) -> Result<(), Failure> {
        let mut rows = 0usize;
        let mut slots = 0usize;
        for index in 0..self.roots.len() {
            self.pay(0, 1, size_of::<Arc<OwnedLogWorker>>())?;
            let anchor = &self.roots[index].shared.anchor;
            rows = add(rows, anchor.recovered().len())?;
            slots = add(slots, anchor.replay_capacity())?;
        }
        if rows > self.limits.max_transactions || slots > self.limits.max_transaction_slots {
            return Err(Failure::Limits);
        }
        self.pay(
            mul(rows, size_of::<Descriptor>())?,
            rows,
            mul(slots, size_of::<RecoveredTransaction>())?,
        )?;
        self.rows
            .try_reserve_exact(rows)
            .map_err(Failure::Allocation)?;
        let rows_excess = self.rows.capacity().saturating_sub(rows);
        self.pay(
            mul(rows_excess, size_of::<Descriptor>())?,
            rows_excess,
            mul(rows_excess, size_of::<Descriptor>())?,
        )?;
        let mut payload_bytes = 0usize;
        for worker in 0..self.roots.len() {
            let n = self.roots[worker].shared.anchor.recovered().len();
            for index in 0..n {
                self.pay(0, 1, size_of::<RecoveredTransaction>())?;
                let row = &self.roots[worker].shared.anchor.recovered()[index];
                let capacity = row.appends.capacity();
                self.pay(
                    0,
                    capacity,
                    mul(capacity, size_of::<crate::sink_journal::RecoveredAppend>())?,
                )?;
                let row = &self.roots[worker].shared.anchor.recovered()[index];
                if row.appends.len() != 1 {
                    return Err(Failure::InvalidSource);
                }
                let expected = row.expected;
                let next = row.next;
                let append = &row.appends[0];
                let first = append.first_offset;
                let topic = append.topic_partition;
                let bytes = append.payload.len();
                payload_bytes = add(payload_bytes, bytes)?;
                if payload_bytes > self.limits.max_payload_bytes
                    || expected.topic_partition != topic
                    || next.topic_partition != topic
                    || topic.topic_id != TelemetrySignal::Logs.topic_id()
                    || first < expected.next_offset
                    || expected.next_placement_sequence.get().checked_add(1)
                        != Some(next.next_placement_sequence.get())
                {
                    return Err(Failure::InvalidSource);
                }
                self.pay(
                    0,
                    self.partitions.len(),
                    mul(self.partitions.capacity(), 2 * size_of::<TopicPartition>())?,
                )?;
                if !self.partitions.contains(&topic) {
                    return Err(Failure::InvalidSource);
                }
                let count = u32::try_from(
                    next.next_offset
                        .get()
                        .checked_sub(first.get())
                        .ok_or(Failure::InvalidSource)?,
                )
                .map_err(|_| Failure::Limits)?;
                if count == 0 {
                    return Err(Failure::InvalidSource);
                }
                self.envelope(worker, index, count)?;
                self.rows.push(Descriptor {
                    worker,
                    row: index,
                    expected,
                    next,
                });
            }
        }
        self.check()
    }
    fn envelope(&mut self, worker: usize, row: usize, count: u32) -> Result<(), Failure> {
        let bytes = self.roots[worker].shared.anchor.recovered()[row].appends[0]
            .payload
            .len();
        self.pay(size_of::<blake3::Hasher>(), 1, 64)?;
        let payload = &self.roots[worker].shared.anchor.recovered()[row].appends[0].payload;
        if !(64..=crate::MAX_TELEMETRY_ENVELOPE_BYTES).contains(&bytes)
            || &payload[..4] != b"STEL"
            || payload[4] != 1
            || payload[6..8] != [0, 0]
            || payload[28..32] != [0; 4]
        {
            return Err(Failure::InvalidSource);
        }
        self.hasher = Some(blake3::Hasher::new());
        let mut at = 0;
        while at < bytes {
            let end = if at < 32 {
                32
            } else if at == 32 {
                64
            } else {
                add(at, 16 * 1024)?.min(bytes)
            };
            self.pay(0, 0, mul(end - at, 4)?)?;
            let hasher = self.hasher.as_mut().ok_or(Failure::State)?;
            if at == 32 {
                hasher.update(&[0; 32]);
            } else {
                hasher.update(
                    &self.roots[worker].shared.anchor.recovered()[row].appends[0].payload[at..end],
                );
            }
            at = end;
            self.check()?;
        }
        self.pay(0, 0, add(size_of::<blake3::Hasher>(), add(64, bytes)?)?)?;
        let payload = &self.roots[worker].shared.anchor.recovered()[row].appends[0].payload;
        if self
            .hasher
            .as_ref()
            .ok_or(Failure::State)?
            .finalize()
            .as_bytes()
            .as_slice()
            != &payload[32..64]
        {
            return Err(Failure::InvalidSource);
        }
        let view = TelemetryEnvelope::decode_view_after_validation(payload)
            .map_err(|_| Failure::InvalidSource)?;
        if view.signal != TelemetrySignal::Logs || view.item_count != count {
            return Err(Failure::InvalidSource);
        }
        self.hasher = None;
        self.check()
    }
}

/// Original setup source census precedes population or registered worker entry.
pub(super) fn census_setup(
    setup: &mut Setup,
    budget: &mut TelemetryRecoveryBudget,
) -> Result<(), Failure> {
    let plan = setup.replay_plan.as_ref().ok_or(Failure::State)?;
    plan.limits.validate()?;
    let limits = plan.limits;
    let n = plan.partitions.len();
    let slots = plan.partitions.capacity();
    if n == 0 || n > limits.max_partitions || slots > limits.max_partition_slots {
        return Err(Failure::Limits);
    }
    setup.pay(
        budget,
        add(
            size_of::<Replay>(),
            mul(slots, size_of::<TopicPartition>())?,
        )?,
        mul(add(mul(n, n)?, 1)?, 2 * size_of::<TopicPartition>())?,
    )?;
    for i in 0..n {
        setup.check(budget)?;
        let topic = setup.replay_plan.as_ref().ok_or(Failure::State)?.partitions[i];
        if topic.topic_id != TelemetrySignal::Logs.topic_id() {
            return Err(Failure::InvalidSource);
        }
        for j in 0..i {
            if setup.replay_plan.as_ref().ok_or(Failure::State)?.partitions[j] == topic {
                return Err(Failure::InvalidSource);
            }
        }
    }
    let mut rows = 0usize;
    let mut row_slots = 0usize;
    let mut payload = 0usize;
    for source in 0..setup.sources.len() {
        setup.check(budget)?;
        let len = setup.sources[source].journal.recovered().len();
        let capacity = setup.sources[source].journal.replay_capacity();
        rows = add(rows, len)?;
        row_slots = add(row_slots, capacity)?;
        if rows > limits.max_transactions || row_slots > limits.max_transaction_slots {
            return Err(Failure::Limits);
        }
        setup.pay(budget, 0, mul(capacity, size_of::<RecoveredTransaction>())?)?;
        for index in 0..len {
            setup.check(budget)?;
            let row = &setup.sources[source].journal.recovered()[index];
            let cap = row.appends.capacity();
            setup.pay(
                budget,
                0,
                mul(cap, size_of::<crate::sink_journal::RecoveredAppend>())?,
            )?;
            let row = &setup.sources[source].journal.recovered()[index];
            if row.appends.len() != 1 {
                return Err(Failure::InvalidSource);
            }
            payload = add(payload, row.appends[0].payload.len())?;
            if payload > limits.max_payload_bytes {
                return Err(Failure::Limits);
            }
        }
    }
    setup.check(budget)
}
