//! Actual host-registered strict log workers and their genuine factory adapter.
use super::*;
use crate::sink_journal::AnchoredSinkJournal;
use crate::sink_journal::JournalReplayInput;
use crate::{
    PreparedLogStripe, TelemetryRecoveryBudget, TelemetryRecoveryError, TelemetryRecoveryUsage,
    TelemetryTaskRegistry,
};
use bounded_apply::{AppliedLogTransaction, RetainedLogApplyError};
use replay::{AdoptedAppend, JournalAdoption, Replay};
use shard_stream_engine::{
    NativeResumeCallbackError, NativeResumeCallbackResult, NativeResumeRefusal, NativeResumeWork,
};
use std::any::Any;
use std::io;
use std::mem::{size_of, size_of_val};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, MutexGuard, OnceLock, TryLockError};

mod policy;
pub(crate) mod replay;
pub use policy::{OwnedLogApplyLimits, OwnedLogDecompressionLimits, OwnedLogFactoryLimits};
pub use replay::{OwnedLogReplayLimits, OwnedLogReplayPlan};
#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests;

/// Complete real source pair. It cannot be created from an ordinary journal or
/// an ordinary stripe: both original bounded producers must succeed.
pub struct OwnedLogShard {
    stripe: Option<PreparedLogStripe>,
    journal: Arc<AnchoredSinkJournal>,
}
impl OwnedLogShard {
    /// Couple the two original owners. Legacy setup rejects nonempty replay;
    /// opt-in setup validates and quarantines these exact sources for adoption.
    pub fn new(stripe: PreparedLogStripe, journal: Arc<AnchoredSinkJournal>) -> Self {
        Self {
            stripe: Some(stripe),
            journal,
        }
    }
}

#[derive(Debug)]
enum Failure {
    Limits,
    State,
    Busy,
    Poisoned,
    Closed,
    InvalidSource,
    Recovery(TelemetryRecoveryError),
    Registration,
    Allocation(std::collections::TryReserveError),
    Refused(NativeResumeRefusal),
    Transaction,
    Panic,
}
fn add(a: usize, b: usize) -> Result<usize, Failure> {
    a.checked_add(b).ok_or(Failure::Limits)
}
fn mul(a: usize, b: usize) -> Result<usize, Failure> {
    a.checked_mul(b).ok_or(Failure::Limits)
}
fn arc_bytes<T>() -> Result<usize, Failure> {
    add(
        add(2 * size_of::<usize>(), std::mem::align_of::<T>() - 1)?,
        size_of::<T>(),
    )
}
type Checkpoints = Arc<Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>>;
type ActiveCache = Arc<Mutex<HashMap<Arc<str>, Vec<TopicPartition>>>>;
struct Namespace {
    checkpoints: Checkpoints,
    cache: ActiveCache,
    denied: AtomicBool,
    adoption: Option<Arc<AtomicBool>>,
}
type Outcome = Result<AppliedLogTransaction, RetainedLogApplyError>;
struct Command {
    input: Option<DurableAppend>,
    replay_input: Option<JournalReplayInput>,
    replay_result: Option<
        Result<bounded_apply::ReplayedLogTransaction, RetainedLogApplyError<JournalReplayInput>>,
    >,
    adopted_input: Option<AdoptedAppend>,
    adopted_result:
        Option<Result<bounded_apply::AdoptedLogTransaction, RetainedLogApplyError<AdoptedAppend>>>,
    expected: DurableSinkCheckpoint,
    next: DurableSinkCheckpoint,
    result: Option<Outcome>,
    effect: Option<DurableSinkApply>,
    panic: Option<Box<dyn Any + Send>>,
    settled: bool,
    work: NativeResumeWork,
}
#[derive(Clone, Copy)]
enum CommandValue {
    Live(DurableSinkApply),
    Replay(DurableSinkCheckpoint),
}
impl Command {
    fn has_input(&self) -> bool {
        self.input.is_some() || self.adopted_input.is_some() || self.replay_input.is_some()
    }
    fn value(&self) -> Option<CommandValue> {
        if let Some(Ok(done)) = &self.result {
            return done.apply_result().map(CommandValue::Live);
        }
        if let Some(Ok(done)) = &self.adopted_result {
            return done.apply_result().map(CommandValue::Live);
        }
        self.replay_result
            .as_ref()?
            .as_ref()
            .ok()?
            .replayed_checkpoint()
            .map(CommandValue::Replay)
    }
    fn failed(&self) -> bool {
        self.value().is_none()
    }
    fn error(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Some(Err(error)) = &self.result {
            return Some(error);
        }
        if let Some(Err(error)) = &self.adopted_result {
            return Some(error);
        }
        if let Some(Err(error)) = &self.replay_result {
            return Some(error);
        }
        None
    }
    fn restore_stripe(&mut self) -> Result<LogStripe, Failure> {
        if let Some(result) = self.result.take() {
            match result {
                Ok(done) => match done.into_stripe() {
                    Ok(stripe) => return Ok(stripe),
                    Err(done) => {
                        self.result = Some(Ok(done));
                        return Err(Failure::State);
                    }
                },
                Err(error) => {
                    self.result = Some(Err(error));
                    return Err(Failure::Transaction);
                }
            }
        }
        if let Some(result) = self.adopted_result.take() {
            match result {
                Ok(done) => match done.into_stripe() {
                    Ok(stripe) => return Ok(stripe),
                    Err(done) => {
                        self.adopted_result = Some(Ok(done));
                        return Err(Failure::State);
                    }
                },
                Err(error) => {
                    self.adopted_result = Some(Err(error));
                    return Err(Failure::Transaction);
                }
            }
        }
        if let Some(result) = self.replay_result.take() {
            match result {
                Ok(done) => match done.into_stripe() {
                    Ok(stripe) => return Ok(stripe),
                    Err(done) => {
                        self.replay_result = Some(Ok(done));
                        return Err(Failure::State);
                    }
                },
                Err(error) => {
                    self.replay_result = Some(Err(error));
                    return Err(Failure::Transaction);
                }
            }
        }
        Err(Failure::State)
    }
}
struct Control {
    stripe: Option<LogStripe>,
    pending: Option<Box<Command>>,
    stop: bool,
    #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
    started: bool,
    exited: bool,
}
#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
type CommandProbe = Arc<dyn Fn(&Command, &'static str) + Send + Sync>;

pub(super) struct Shared {
    control: Mutex<Control>,
    changed: Condvar,
    wake_thread: OnceLock<std::thread::Thread>,
    limits: bounded_apply::LogApplyLimits,
    namespace: Arc<Namespace>,
    anchor: Arc<AnchoredSinkJournal>,
    adoption: Option<Arc<JournalAdoption>>,
    #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
    probe: Mutex<Option<CommandProbe>>,
}
impl Shared {
    fn wake(&self) {
        self.changed.notify_all();
        if let Some(thread) = self.wake_thread.get() {
            thread.unpark();
        }
    }
    fn observed(&self, command: &Command, phase: &'static str) {
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        {
            let p = self.probe.lock().unwrap_or_else(|e| e.into_inner()).clone();
            if let Some(p) = p {
                p(command, phase);
            }
        }
        #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
        let _ = (command, phase);
    }
    fn deny(&self) {
        self.anchor.deny();
        self.namespace.denied.store(true, Ordering::Release);
        self.wake();
    }
    fn denied(&self) -> bool {
        self.anchor.is_denied() || self.namespace.denied.load(Ordering::Acquire)
    }
    fn lock(&self) -> MutexGuard<'_, Control> {
        match self.control.lock() {
            Ok(g) => g,
            Err(e) => {
                self.deny();
                e.into_inner()
            }
        }
    }
    fn wait<'a>(&self, g: MutexGuard<'a, Control>) -> MutexGuard<'a, Control> {
        match self.changed.wait(g) {
            Ok(g) => g,
            Err(e) => {
                self.deny();
                e.into_inner()
            }
        }
    }
    fn close(&self) {
        self.deny();
        // Negative wakeup does not acquire the command mutex: shutdown may be
        // requested while physical work owns it, including reentrant refusal.
        self.wake();
    }
    fn wait_exit(&self) {
        let mut c = self.lock();
        while !c.exited {
            c = self.wait(c);
        }
    }
}
pub(super) struct OwnedLogWorker {
    shared: Arc<Shared>,
    shard: ShardId,
    opened: AtomicBool,
    registered: AtomicBool,
    // Host owns actual native handles/results. The thread only captures Shared,
    // so no registry/worker strong-reference cycle is introduced.
    _registry: Arc<dyn TelemetryTaskRegistry>,
}
impl Drop for OwnedLogWorker {
    fn drop(&mut self) {
        self.shared.close();
        if self.registered.load(Ordering::Acquire) {
            self.shared.wait_exit();
        }
    }
}
impl OwnedLogWorker {
    pub(super) fn shard_id(&self) -> ShardId {
        self.shard
    }
}

/// Real factory whose registered workers own bounded prepared log sources.
/// Ordinary signal/object/validation APIs are explicitly outside this route;
/// legacy factories and handlers retain their ordinary behavior.
pub struct OwnedLogSinkFactory {
    sinks: Vec<Arc<ShardTelemetryStripeSink>>,
    namespace: Arc<Namespace>,
    setup_expiry: std::time::Instant,
    open_calls: AtomicUsize,
    max_open_calls: usize,
    replay: Option<Replay>,
}
impl fmt::Debug for OwnedLogSinkFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedLogSinkFactory")
            .field("workers", &self.sinks.len())
            .field("denied", &self.namespace.denied.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
impl Drop for OwnedLogSinkFactory {
    fn drop(&mut self) {
        // Request exit from every actual worker before waiting on any one.
        self.begin_draining();
        for s in &self.sinks {
            if let ShardSinkOwner::Owned(w) = &s.state
                && w.registered.load(Ordering::Acquire)
            {
                w.shared.wait_exit();
            }
        }
    }
}
struct Setup {
    sources: Vec<OwnedLogShard>,
    replay_plan: Option<OwnedLogReplayPlan>,
    sinks: Vec<Arc<ShardTelemetryStripeSink>>,
    checkpoints: Option<Checkpoints>,
    cache: Option<ActiveCache>,
    namespace: Option<Arc<Namespace>>,
    current: Option<Arc<OwnedLogWorker>>,
    current_shared: Option<Arc<Shared>>,
    name: Option<String>,
    registrations: Vec<io::Result<()>>,
    panic: Option<Box<dyn Any + Send>>,
    registry: Arc<dyn TelemetryTaskRegistry>,
    limits: OwnedLogFactoryLimits,
    declared: usize,
    work: usize,
    expiry: std::time::Instant,
}
/// Whole terminal setup owner: original sources, actual workers/registrations,
/// namespace and partial native results remain retained. No retry extractor.
pub struct RetainedOwnedLogFactoryError {
    cause: Failure,
    retained: Setup,
}
impl fmt::Debug for RetainedOwnedLogFactoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedOwnedLogFactoryError")
            .field("cause", &self.cause)
            .field("workers", &self.retained.sinks.len())
            .field("registrations", &self.retained.registrations.len())
            .field("panic", &self.retained.panic.is_some())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedOwnedLogFactoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for RetainedOwnedLogFactoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            Failure::Recovery(e) => Some(e),
            Failure::Registration => self
                .retained
                .registrations
                .iter()
                .find_map(|r| r.as_ref().err())
                .map(|e| e as &dyn std::error::Error),
            Failure::Allocation(e) => Some(e),
            _ => None,
        }
    }
}
impl Setup {
    fn check(&self, budget: &mut TelemetryRecoveryBudget) -> Result<(), Failure> {
        budget.check().map_err(Failure::Recovery)?;
        if std::time::Instant::now() >= self.expiry {
            return Err(Failure::Recovery(TelemetryRecoveryError::Expired));
        }
        Ok(())
    }
    fn pay(
        &mut self,
        budget: &mut TelemetryRecoveryBudget,
        bytes: usize,
        work: usize,
    ) -> Result<(), Failure> {
        self.check(budget)?;
        let d = add(self.declared, bytes)?;
        let w = add(self.work, work)?;
        if d > self.limits.max_retained_bytes || w > self.limits.max_work_bytes {
            return Err(Failure::Limits);
        }
        budget
            .charge(TelemetryRecoveryUsage {
                retained_bytes: bytes,
                work_bytes: work,
                ..Default::default()
            })
            .map_err(Failure::Recovery)?;
        self.declared = d;
        self.work = w;
        Ok(())
    }
    fn close(&self) {
        if let Some(n) = &self.namespace {
            n.denied.store(true, Ordering::Release);
        }
        for s in &self.sources {
            s.journal.deny();
        }
        if let Some(w) = &self.current {
            w.shared.close();
        }
        if let Some(shared) = &self.current_shared {
            shared.close();
        }
        for s in &self.sinks {
            if let ShardSinkOwner::Owned(w) = &s.state {
                w.shared.close();
            }
        }
    }
    fn run(&mut self, budget: &mut TelemetryRecoveryBudget) -> Result<(), Failure> {
        self.check(budget)?;
        let n = self.sources.len();
        let l = self.limits;
        if n == 0
            || n > l.max_shards
            || l.max_shards > 64
            || self.sources.capacity() > l.max_source_slots
            || l.max_pending_commands != 1
            || l.thread_stack_bytes == 0
            || l.max_thread_name_bytes == 0
        {
            return Err(Failure::Limits);
        }
        let native = l.apply.native()?;
        let fixed = add(size_of::<Self>(), size_of::<RetainedOwnedLogFactoryError>())?;
        let input = mul(self.sources.capacity(), size_of::<OwnedLogShard>())?;
        self.pay(
            budget,
            add(fixed, input)?,
            add(
                add(fixed, input)?,
                mul(mul(n, n)?, 4 * size_of::<ShardId>())?,
            )?,
        )?;
        // Complete duplicate/source census precedes population/native entry.
        for i in 0..n {
            self.check(budget)?;
            let s = &self.sources[i];
            let stripe = s.stripe.as_ref().ok_or(Failure::State)?;
            self.expiry = self
                .expiry
                .min(stripe.original_expiry())
                .min(s.journal.original_expiry());
            self.check(budget)?;
            if stripe.stream_shard_id() != s.journal.shard_id()
                || s.journal.is_denied()
                || (self.replay_plan.is_none()
                    && (!s.journal.recovered().is_empty()
                        || s.journal.format() != crate::sink_journal::JournalFormat::Legacy))
                || self.sources[0].journal.format() != s.journal.format()
            {
                return Err(Failure::InvalidSource);
            }
            for prior in &self.sources[..i] {
                if prior.journal.shard_id() == s.journal.shard_id() {
                    return Err(Failure::InvalidSource);
                }
            }
        }
        if self.replay_plan.is_some() {
            replay::census_setup(self, budget)?;
            self.pay(budget, arc_bytes::<AtomicBool>()?, size_of::<AtomicBool>())?;
        }
        self.pay(
            budget,
            add(
                arc_bytes::<Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>>()?,
                add(
                    arc_bytes::<Mutex<HashMap<Arc<str>, Vec<TopicPartition>>>>()?,
                    arc_bytes::<Namespace>()?,
                )?,
            )?,
            size_of::<Namespace>(),
        )?;
        self.checkpoints = Some(Arc::new(Mutex::new(HashMap::new())));
        self.check(budget)?;
        self.cache = Some(Arc::new(Mutex::new(HashMap::new())));
        self.check(budget)?;
        self.namespace = Some(Arc::new(Namespace {
            checkpoints: Arc::clone(self.checkpoints.as_ref().ok_or(Failure::State)?),
            cache: Arc::clone(self.cache.as_ref().ok_or(Failure::State)?),
            denied: AtomicBool::new(false),
            adoption: self
                .replay_plan
                .as_ref()
                .map(|_| Arc::new(AtomicBool::new(false))),
        }));
        self.pay(
            budget,
            mul(
                n,
                add(
                    size_of::<Arc<ShardTelemetryStripeSink>>(),
                    size_of::<io::Result<()>>(),
                )?,
            )?,
            mul(n, size_of::<usize>())?,
        )?;
        self.sinks
            .try_reserve_exact(n)
            .map_err(Failure::Allocation)?;
        let extra = mul(
            self.sinks.capacity().saturating_sub(n),
            size_of::<Arc<ShardTelemetryStripeSink>>(),
        )?;
        self.pay(budget, extra, extra)?;
        self.registrations
            .try_reserve_exact(n)
            .map_err(Failure::Allocation)?;
        let extra = mul(
            self.registrations.capacity().saturating_sub(n),
            size_of::<io::Result<()>>(),
        )?;
        self.pay(budget, extra, extra)?;
        for i in 0..n {
            let fixed = add(
                add(arc_bytes::<OwnedLogWorker>()?, arc_bytes::<Shared>()?)?,
                arc_bytes::<ShardTelemetryStripeSink>()?,
            )?;
            self.pay(budget, add(fixed, l.thread_stack_bytes)?, fixed)?;
            let shard = self.sources[i].journal.shard_id();
            self.name = Some(String::new());
            let name_len = add(b"shard-telemetry-owned-log-".len(), 20)?;
            if name_len > l.max_thread_name_bytes {
                return Err(Failure::Limits);
            }
            self.pay(budget, name_len, name_len)?;
            budget
                .charge(TelemetryRecoveryUsage {
                    string_bytes: name_len,
                    ..Default::default()
                })
                .map_err(Failure::Recovery)?;
            self.name
                .as_mut()
                .ok_or(Failure::State)?
                .try_reserve_exact(name_len)
                .map_err(Failure::Allocation)?;
            let name_capacity = self.name.as_ref().ok_or(Failure::State)?.capacity();
            self.pay(
                budget,
                name_capacity.saturating_sub(name_len),
                name_capacity.saturating_sub(name_len),
            )?;
            use std::fmt::Write;
            write!(
                self.name.as_mut().ok_or(Failure::State)?,
                "shard-telemetry-owned-log-{}",
                shard.get()
            )
            .map_err(|_| Failure::State)?;
            self.check(budget)?;
            let adoption = if self
                .namespace
                .as_ref()
                .ok_or(Failure::State)?
                .adoption
                .is_some()
            {
                self.pay(
                    budget,
                    arc_bytes::<JournalAdoption>()?,
                    size_of::<JournalAdoption>(),
                )?;
                let complete = self
                    .namespace
                    .as_ref()
                    .ok_or(Failure::State)?
                    .adoption
                    .as_ref()
                    .ok_or(Failure::State)?;
                Some(Arc::new(JournalAdoption::new(
                    Arc::clone(&self.sources[i].journal),
                    Arc::clone(self.checkpoints.as_ref().ok_or(Failure::State)?),
                    Arc::clone(complete),
                )))
            } else {
                None
            };
            self.current_shared = Some(Arc::new(Shared {
                control: Mutex::new(Control {
                    stripe: Some(
                        self.sources[i]
                            .stripe
                            .take()
                            .ok_or(Failure::State)?
                            .into_stripe(),
                    ),
                    pending: None,
                    stop: false,
                    #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
                    started: false,
                    exited: false,
                }),
                changed: Condvar::new(),
                wake_thread: OnceLock::new(),
                limits: native,
                namespace: Arc::clone(self.namespace.as_ref().ok_or(Failure::State)?),
                anchor: Arc::clone(&self.sources[i].journal),
                adoption,
                #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
                probe: Mutex::new(None),
            }));
            self.check(budget)?;
            self.current = Some(Arc::new(OwnedLogWorker {
                shared: Arc::clone(self.current_shared.as_ref().ok_or(Failure::State)?),
                shard,
                opened: AtomicBool::new(false),
                registered: AtomicBool::new(false),
                _registry: Arc::clone(&self.registry),
            }));
            self.check(budget)?;
            let shared = Arc::clone(self.current_shared.as_ref().ok_or(Failure::State)?);
            let worker = move || super::worker::run_owned_sink_worker(shared);
            self.pay(budget, size_of_val(&worker), size_of_val(&worker))?;
            let actual = self.registry.spawn_until(
                self.name.as_ref().ok_or(Failure::State)?,
                l.thread_stack_bytes,
                self.expiry,
                Box::new(worker),
            );
            self.registrations.push(actual);
            if let Some(Err(_)) = self.registrations.last() {
                return Err(Failure::Registration);
            }
            self.current
                .as_ref()
                .ok_or(Failure::State)?
                .registered
                .store(true, Ordering::Release);
            // Capture original result and actual task owner BEFORE post-entry E.
            self.check(budget)?;
            self.sinks.push(Arc::new(ShardTelemetryStripeSink {
                state: ShardSinkOwner::Owned(self.current.take().ok_or(Failure::State)?),
            }));
            self.check(budget)?;
        }
        Ok(())
    }
}
impl OwnedLogSinkFactory {
    /// Close the SAME actual workers while factory/sink/engine Arcs remain
    /// retained. This negative-only request wakes idle loops and refuses later
    /// phases; accepted commands still physically settle with their outcomes.
    ///
    /// Call this before joining the host task registry. A worker-body exit is
    /// not native/TLS join: the host must join its real handles before releasing
    /// engine/directory custody. This method grants no permission or readiness.
    pub fn begin_draining(&self) {
        self.namespace.denied.store(true, Ordering::Release);
        for s in &self.sinks {
            if let ShardSinkOwner::Owned(w) = &s.state {
                w.shared.close();
            }
        }
    }
    /// Consume all actual bounded sources on the supplied original setup ledger
    /// and register their genuine index threads with the real host lifetime.
    /// No journal replay is promoted to an application checkpoint.
    #[allow(
        clippy::result_large_err,
        reason = "keep the complete entered setup owner inline without an emergency allocation"
    )]
    pub fn new_until(
        sources: Vec<OwnedLogShard>,
        limits: OwnedLogFactoryLimits,
        registry: Arc<dyn TelemetryTaskRegistry>,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<Self, RetainedOwnedLogFactoryError> {
        Self::new_inner(sources, limits, None, registry, budget)
    }
    /// Opt in to actual anchored journal adoption in the first genuine native
    /// checkpoint callback. Historical scope is not HA or serving permission.
    #[allow(
        clippy::result_large_err,
        reason = "retain the complete entered setup and native worker owners inline"
    )]
    pub fn new_with_replay_until(
        sources: Vec<OwnedLogShard>,
        limits: OwnedLogFactoryLimits,
        plan: OwnedLogReplayPlan,
        registry: Arc<dyn TelemetryTaskRegistry>,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<Self, RetainedOwnedLogFactoryError> {
        Self::new_inner(sources, limits, Some(plan), registry, budget)
    }
    #[allow(
        clippy::result_large_err,
        reason = "retain the complete entered setup and native worker owners inline"
    )]
    fn new_inner(
        sources: Vec<OwnedLogShard>,
        limits: OwnedLogFactoryLimits,
        replay_plan: Option<OwnedLogReplayPlan>,
        registry: Arc<dyn TelemetryTaskRegistry>,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<Self, RetainedOwnedLogFactoryError> {
        let mut retained = Setup {
            sources,
            replay_plan,
            sinks: Vec::new(),
            checkpoints: None,
            cache: None,
            namespace: None,
            current: None,
            current_shared: None,
            name: None,
            registrations: Vec::new(),
            panic: None,
            registry,
            limits,
            declared: 0,
            work: 0,
            expiry: budget.original_expiry(),
        };
        let actual = catch_unwind(AssertUnwindSafe(|| retained.run(budget)));
        let cause = match actual {
            Ok(Ok(())) => {
                if let Some(namespace) = retained.namespace.take() {
                    return Ok(Self {
                        sinks: std::mem::take(&mut retained.sinks),
                        namespace,
                        setup_expiry: retained.expiry,
                        open_calls: AtomicUsize::new(0),
                        max_open_calls: retained.sources.len(),
                        replay: retained.replay_plan.take().map(Replay::new),
                    });
                }
                Failure::State
            }
            Ok(Err(e)) => e,
            Err(p) => {
                retained.panic = Some(p);
                Failure::Panic
            }
        };
        retained.close();
        Err(RetainedOwnedLogFactoryError { cause, retained })
    }
}

struct Carrier {
    command: Option<Box<Command>>,
    failure: Option<Failure>,
    panic: Option<Box<dyn Any + Send>>,
    worker: Arc<OwnedLogWorker>,
}
impl fmt::Debug for Carrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedLogCallback")
            .field("failure", &self.failure)
            .field("effect", &self.command.as_ref().and_then(|c| c.effect))
            .finish_non_exhaustive()
    }
}
impl fmt::Display for Carrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for Carrier {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.failure {
            Some(Failure::Refused(e)) => Some(e),
            Some(Failure::Transaction) => self.command.as_ref()?.error(),
            _ => None,
        }
    }
}
impl OwnedLogWorker {
    pub(super) fn apply_until(
        self: &Arc<Self>,
        expected: DurableSinkCheckpoint,
        appends: &[DurableAppend],
        next: DurableSinkCheckpoint,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<DurableSinkApply> {
        if self.shared.adoption.as_ref().is_some_and(|a| !a.complete()) {
            self.shared.deny();
            return Err(NativeResumeCallbackError::unsupported(
                "owned journal adoption is incomplete",
            ));
        }
        if appends.len() != 1 {
            self.shared.deny();
            return Err(NativeResumeCallbackError::unsupported(
                "owned log worker requires one real append",
            ));
        }
        let visible = appends[0]
            .payload
            .len()
            .checked_add(appends[0].transient_context.as_ref().map_or(0, Bytes::len))
            .ok_or_else(|| NativeResumeCallbackError::refused(NativeResumeRefusal::Capacity))?;
        let buffers = 1 + usize::from(appends[0].transient_context.is_some());
        let bytes = visible
            .checked_add(
                size_of::<Carrier>() + size_of::<Command>() + buffers * 3 * size_of::<usize>(),
            )
            .ok_or_else(|| NativeResumeCallbackError::refused(NativeResumeRefusal::Capacity))?;
        work.reserve_values(bytes, 0, 2 + buffers, bytes)
            .map_err(NativeResumeCallbackError::refused)?;
        let mut carrier = Box::new(Carrier {
            command: Some(Box::new(Command {
                input: None,
                replay_input: None,
                replay_result: None,
                adopted_input: None,
                adopted_result: None,
                expected,
                next,
                result: None,
                effect: None,
                panic: None,
                settled: false,
                work: work.clone(),
            })),
            failure: None,
            panic: None,
            worker: Arc::clone(self),
        });
        let actual = catch_unwind(AssertUnwindSafe(|| {
            carrier.command.as_mut().ok_or(Failure::State)?.input = Some(appends[0].clone());
            if let Some(adoption) = &self.shared.adoption {
                work.reserve_values(size_of::<AdoptedAppend>(), 0, 1, size_of::<AdoptedAppend>())
                    .map_err(Failure::Refused)?;
                let command = carrier.command.as_mut().ok_or(Failure::State)?;
                command.adopted_input = Some(AdoptedAppend::new(
                    command.input.take().ok_or(Failure::State)?,
                    Arc::clone(adoption),
                ));
            }
            self.shared
                .observed(carrier.command.as_ref().ok_or(Failure::State)?, "copied");
            carrier.run(work)
        }));
        let failure = match actual {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(e)) => e,
            Err(p) => {
                carrier.panic = Some(p);
                Failure::Panic
            }
        };
        self.shared.deny();
        carrier.failure = Some(failure);
        carrier.settle_error();
        Err(NativeResumeCallbackError::from_external(carrier))
    }
}
impl Carrier {
    fn settle_error(&mut self) {
        let shared = Arc::clone(&self.worker.shared);
        if self.command.as_ref().is_some_and(|c| !c.settled) {
            // This caller never queued its command. Do not synchronously wait
            // for somebody else's native work (including a reentrant Busy
            // refusal). That command's own caller retains settlement custody.
            if let Ok(mut c) = shared.control.try_lock() {
                c.stop = true;
                shared.wake();
            }
            return;
        }
        // An unwind after queue publication never abandons accepted work. The
        // original pending slot remains the one owner until actual settlement.
        let mut c = shared.lock();
        shared.deny();
        c.stop = true;
        shared.wake();
        if self.command.is_none() {
            while !c.pending.as_ref().is_some_and(|p| p.settled) && !c.exited {
                c = shared.wait(c);
            }
            self.command = c.pending.take();
        }
        drop(c);
        if self.worker.registered.load(Ordering::Acquire) {
            shared.wait_exit();
        }
    }
    fn run(&mut self, work: &NativeResumeWork) -> Result<DurableSinkApply, Failure> {
        match self.run_command(work)? {
            CommandValue::Live(value) => Ok(value),
            CommandValue::Replay(_) => Err(Failure::State),
        }
    }
    fn run_command(&mut self, work: &NativeResumeWork) -> Result<CommandValue, Failure> {
        work.check().map_err(Failure::Refused)?;
        let shared = Arc::clone(&self.worker.shared);
        if shared.denied() {
            return Err(Failure::Closed);
        }
        let mut control = match shared.control.try_lock() {
            Ok(g) => g,
            Err(TryLockError::WouldBlock) => return Err(Failure::Busy),
            Err(TryLockError::Poisoned(e)) => {
                shared.deny();
                drop(e.into_inner());
                return Err(Failure::Poisoned);
            }
        };
        if control.stop || control.pending.is_some() {
            shared.deny();
            return Err(Failure::Busy);
        }
        work.reserve_job().map_err(Failure::Refused)?;
        control.pending = self.command.take();
        shared.wake();
        shared.observed(control.pending.as_ref().ok_or(Failure::State)?, "queued");
        while !control.pending.as_ref().is_some_and(|c| c.settled) {
            control = shared.wait(control);
        }
        self.command = control.pending.take();
        shared.observed(self.command.as_ref().ok_or(Failure::State)?, "settled");
        let result = (|| {
            work.check().map_err(Failure::Refused)?;
            let c = self.command.as_mut().ok_or(Failure::State)?;
            if c.panic.is_some() {
                return Err(Failure::Panic);
            }
            let value = c.value().ok_or(Failure::Transaction)?;
            c.effect = match value {
                CommandValue::Live(value) => Some(value),
                CommandValue::Replay(_) => None,
            };
            if shared.denied() {
                return Err(Failure::Closed);
            }
            // Last observation precedes restoring the exact successful stripe.
            control.stripe = Some(c.restore_stripe()?);
            Ok(value)
        })();
        if result.is_err() {
            shared.deny();
            control.stop = true;
        }
        shared.wake();
        drop(control);
        if result.is_err() {
            shared.wait_exit();
        }
        result
    }
}

pub(super) fn run(shared: Arc<Shared>) {
    let _ = shared.wake_thread.set(std::thread::current());
    let mut control = shared.lock();
    #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
    {
        control.started = true;
    }
    shared.wake();
    loop {
        while !control.stop
            && !shared.denied()
            && control.pending.as_ref().is_none_or(|c| c.settled)
        {
            // A retained unpark token closes the check/wait race even when
            // shutdown cannot take this mutex during entered physical work.
            drop(control);
            std::thread::park();
            control = shared.lock();
        }
        if (control.stop || shared.denied()) && control.pending.as_ref().is_none_or(|c| c.settled) {
            break;
        }
        if control.pending.as_ref().is_none_or(|c| !c.has_input()) || control.stripe.is_none() {
            shared.deny();
            control.stop = true;
            if let Some(c) = control.pending.as_mut() {
                c.settled = true;
            }
            shared.wake();
            continue;
        }
        let stripe = control.stripe.take();
        let command = control.pending.as_mut();
        if let (Some(stripe), Some(command)) = (stripe, command) {
            let actual = catch_unwind(AssertUnwindSafe(|| {
                if let Some(input) = command.replay_input.take() {
                    command.replay_result = Some(stripe.apply_log_input_until(
                        input,
                        command.expected,
                        command.next,
                        Arc::clone(&shared.anchor),
                        Arc::clone(&shared.namespace.checkpoints),
                        Arc::clone(&shared.namespace.cache),
                        shared.limits,
                        &command.work,
                    ));
                    shared.observed(command, "worker-settled");
                } else if let Some(input) = command.adopted_input.take() {
                    command.adopted_result = Some(stripe.apply_log_input_until(
                        input,
                        command.expected,
                        command.next,
                        Arc::clone(&shared.anchor),
                        Arc::clone(&shared.namespace.checkpoints),
                        Arc::clone(&shared.namespace.cache),
                        shared.limits,
                        &command.work,
                    ));
                    command.effect = command.value().and_then(|value| match value {
                        CommandValue::Live(effect) => Some(effect),
                        CommandValue::Replay(_) => None,
                    });
                    shared.observed(command, "worker-settled");
                } else if let Some(input) = command.input.take() {
                    command.result = Some(stripe.apply_durable_log_until(
                        input,
                        command.expected,
                        command.next,
                        Arc::clone(&shared.anchor),
                        Arc::clone(&shared.namespace.checkpoints),
                        Arc::clone(&shared.namespace.cache),
                        shared.limits,
                        &command.work,
                    ));
                    command.effect = command
                        .result
                        .as_ref()
                        .and_then(|r| r.as_ref().ok())
                        .and_then(|r| r.apply_result());
                    shared.observed(command, "worker-settled");
                }
            }));
            if let Err(p) = actual {
                command.panic = Some(p);
            }
            command.settled = true;
            if command.failed() || command.panic.is_some() {
                shared.deny();
                control.stop = true;
            }
        } else {
            shared.deny();
            control.stop = true;
            if let Some(c) = control.pending.as_mut() {
                c.settled = true;
            }
        }
        shared.wake();
    }
    control.exited = true;
    shared.wake();
}

impl DurableAppendSinkFactory for OwnedLogSinkFactory {
    fn validate_append(&self, _: &[u8], _: NonZeroU32) -> EngineResult<()> {
        Err(EngineError::UnsupportedDurability(
            "owned strict log preflight requires its genuine request work",
        ))
    }
    fn load_checkpoint(&self, _: TopicPartition) -> EngineResult<Option<DurableSinkCheckpoint>> {
        Err(EngineError::UnsupportedDurability(
            "owned log checkpoint requires original native work",
        ))
    }
    fn load_checkpoint_until(
        &self,
        tp: TopicPartition,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<Option<DurableSinkCheckpoint>> {
        if self.namespace.denied.load(Ordering::Acquire) {
            return Err(NativeResumeCallbackError::unsupported(
                "owned log factory is terminal",
            ));
        }
        if let Some(replay) = &self.replay
            && let Err(error) = replay.adopt(self, tp, work)
        {
            self.begin_draining();
            return Err(error);
        }
        let observed =
            bounded_callback::load_checkpoint_until(&self.namespace.checkpoints, tp, work);
        if self.replay.is_some() && observed.is_err() {
            self.begin_draining();
        }
        observed
    }
    fn open_shard(&self, id: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
        if self.namespace.denied.load(Ordering::Acquire)
            || std::time::Instant::now() >= self.setup_expiry
            || self
                .open_calls
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    n.checked_add(1).filter(|next| *next <= self.max_open_calls)
                })
                .is_err()
        {
            return Err(EngineError::UnsupportedDurability(
                "owned log shard setup is closed",
            ));
        }
        for s in &self.sinks {
            if let ShardSinkOwner::Owned(w) = &s.state
                && w.shard == id
                && !w.opened.swap(true, Ordering::AcqRel)
            {
                if std::time::Instant::now() >= self.setup_expiry {
                    self.namespace.denied.store(true, Ordering::Release);
                    return Err(EngineError::UnsupportedDurability(
                        "owned log shard setup expired",
                    ));
                }
                return Ok(Arc::clone(s) as Arc<dyn DurableAppendSink>);
            }
        }
        Err(EngineError::UnknownShard(id))
    }
}
