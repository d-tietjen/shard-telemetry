use super::*;
use crate::TelemetryRecoveryLimits;
use rayon::prelude::*;
use shard_stream_engine::{EngineConfig, StreamEngine};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let clock = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("fixture clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "telemetry-registered-pool-{}-{clock}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).expect("real unique fixture");
        Self(path)
    }

    fn config(&self) -> EngineConfig {
        EngineConfig {
            data_dir: self.0.join("stream"),
            object_store_dir: None,
            shard_count: 1,
            virtual_lane_count: 1,
            replication_factor: 1,
            min_in_sync_replicas: 1,
            queue_slots_per_shard: 64,
            queue_bytes_per_shard: 2 << 20,
            target_pack_bytes: 64 << 10,
            max_pack_age: Duration::from_secs(1),
            max_batch_bytes: 1 << 20,
            max_fetch_bytes: 1 << 20,
            append_linger: Duration::ZERO,
        }
    }

    fn handle(&self) -> EngineHandle {
        Arc::new(StreamEngine::open(self.config()).expect("actual native engine")).into()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("cleanup after physical worker joins");
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Normal,
    RefuseSecond,
    LateFirst,
}

struct Registry {
    mode: Mode,
    calls: AtomicUsize,
    entered: AtomicUsize,
    held: Arc<(Mutex<bool>, Condvar)>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    facts: Mutex<Vec<(String, usize, Instant)>>,
}
impl Registry {
    fn new(mode: Mode, held: bool) -> Arc<Self> {
        Arc::new(Self {
            mode,
            calls: AtomicUsize::new(0),
            entered: AtomicUsize::new(0),
            held: Arc::new((Mutex::new(held), Condvar::new())),
            tasks: Mutex::new(Vec::new()),
            facts: Mutex::new(Vec::new()),
        })
    }

    fn release_and_join(&self) {
        let (held, changed) = &*self.held;
        *held.lock().expect("fixture gate") = false;
        changed.notify_all();
        let tasks = std::mem::take(&mut *self.tasks.lock().expect("actual task owner"));
        for task in tasks {
            task.join().expect("actual native worker outcome");
        }
    }
}
impl Drop for Registry {
    fn drop(&mut self) {
        self.release_and_join();
    }
}
impl TelemetryTaskRegistry for Registry {
    fn spawn_until(
        &self,
        name: &str,
        stack_bytes: usize,
        original_expiry: Instant,
        worker: Box<dyn FnOnce() + Send>,
    ) -> io::Result<()> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        if matches!(self.mode, Mode::RefuseSecond) && call == 1 {
            return Err(io::Error::other("actual second registration refused"));
        }
        if call >= 8 || name.len() > 64 || stack_bytes != 1 << 20 {
            return Err(io::Error::other("fixture registry bounds"));
        }
        if Instant::now() >= original_expiry {
            return Err(io::Error::other("fixture registry deadline"));
        }
        self.facts.lock().expect("actual facts").push((
            name.to_owned(),
            stack_bytes,
            original_expiry,
        ));
        let held = Arc::clone(&self.held);
        let thread = std::thread::Builder::new()
            .name(name.to_owned())
            .stack_size(stack_bytes)
            .spawn(move || {
                let (lock, changed) = &*held;
                let mut held = lock.lock().expect("actual worker gate");
                while *held {
                    held = changed.wait(held).expect("actual worker wait");
                }
                drop(held);
                worker();
            })?;
        // The actual JoinHandle is retained before any late refusal.
        self.tasks.lock().expect("actual task owner").push(thread);
        self.entered.fetch_add(1, Ordering::AcqRel);
        if matches!(self.mode, Mode::LateFirst) && call == 0 {
            while Instant::now() < original_expiry {
                std::thread::park_timeout(Duration::from_millis(1));
            }
            return Err(io::Error::other("late actual registration outcome"));
        }
        Ok(())
    }
}

fn limits(workers: usize) -> TelemetryAppendPoolLimits {
    TelemetryAppendPoolLimits {
        workers,
        thread_stack_bytes: 1 << 20,
        max_thread_name_bytes: 64,
    }
}

fn budget(expiry: Instant) -> TelemetryRecoveryBudget {
    TelemetryRecoveryBudget::new(
        TelemetryRecoveryLimits {
            max_path_bytes: 8192,
            max_file_bytes: 1 << 20,
            max_read_bytes: 8 << 20,
            max_directory_entries: 4096,
            max_records: 4096,
            max_json_values: 4096,
            max_string_bytes: 1 << 20,
            max_retained_bytes: 32 << 20,
            max_work_bytes: 64 << 20,
        },
        expiry,
    )
    .expect("finite cumulative setup ledger")
}

#[test]
fn actual_pool_registers_every_worker_with_same_expiry_and_runs_parallel_work() {
    let directory = Directory::new();
    let engine = directory.handle();
    let expiry = Instant::now() + Duration::from_secs(10);
    let registry = Registry::new(Mode::Normal, false);
    let mut budget = budget(expiry);
    budget
        .charge(TelemetryRecoveryUsage {
            work_bytes: 7,
            ..TelemetryRecoveryUsage::default()
        })
        .expect("earlier actual setup work");
    let pool =
        TelemetryAppendPool::build_until(engine.clone(), registry.clone(), limits(2), &mut budget)
            .expect("actual registered pool");
    assert!(pool.retains_engine(&engine));
    assert_eq!(pool.worker_count(), 2);
    assert_eq!(
        pool.install(|| (0..100_usize).into_par_iter().sum::<usize>()),
        4950
    );
    assert_eq!(budget.original_expiry(), expiry);
    assert_eq!(budget.usage().records, 2);
    assert!(budget.usage().retained_bytes >= 2 << 20);
    let facts = registry.facts.lock().expect("actual worker facts");
    assert_eq!(facts.len(), 2);
    assert_eq!(facts[0].0, "shard-telemetry-append-0");
    assert_eq!(facts[1].0, "shard-telemetry-append-1");
    assert!(
        facts
            .iter()
            .all(|(_, stack, saved)| *stack == 1 << 20 && *saved == expiry)
    );
    drop(facts);
    drop(pool);
    registry.release_and_join();
}

#[test]
fn cumulative_refusal_and_invalid_worker_limits_enter_no_actual_task() {
    let directory = Directory::new();
    let registry = Registry::new(Mode::Normal, false);
    let mut budget = budget(Instant::now() + Duration::from_secs(10));
    budget
        .charge(TelemetryRecoveryUsage {
            records: 4095,
            ..TelemetryRecoveryUsage::default()
        })
        .expect("earlier catalog records remain spent");
    let error = TelemetryAppendPool::build_until(
        directory.handle(),
        registry.clone(),
        limits(2),
        &mut budget,
    )
    .expect_err("same cumulative ledger refuses before constructor");
    assert!(matches!(
        error.cause(),
        TelemetryAppendPoolSetupFailure::Recovery(TelemetryRecoveryError::Limits)
    ));
    assert_eq!(budget.usage().records, 4095);
    assert_eq!(registry.calls.load(Ordering::Acquire), 0);
    for workers in [0, 65, usize::MAX] {
        let mut budget = self::budget(Instant::now() + Duration::from_secs(10));
        assert!(
            TelemetryAppendPool::build_until(
                directory.handle(),
                registry.clone(),
                limits(workers),
                &mut budget
            )
            .is_err()
        );
    }
    assert_eq!(registry.calls.load(Ordering::Acquire), 0);
}

#[test]
fn expired_original_budget_refuses_before_native_registration() {
    let directory = Directory::new();
    let engine = directory.handle();
    let registry = Registry::new(Mode::Normal, false);
    let expiry = Instant::now() + Duration::from_millis(10);
    let mut budget = budget(expiry);
    while Instant::now() < expiry {
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let error = TelemetryAppendPool::build_until(engine, registry.clone(), limits(1), &mut budget)
        .expect_err("original expired setup is not refreshed");
    assert!(matches!(
        error.cause(),
        TelemetryAppendPoolSetupFailure::Recovery(TelemetryRecoveryError::Expired)
    ));
    assert_eq!(registry.calls.load(Ordering::Acquire), 0);
    assert_eq!(budget.usage(), TelemetryRecoveryUsage::default());
}

#[test]
fn partial_registration_failure_keeps_engine_until_actual_entered_worker_joins() {
    let directory = Directory::new();
    let engine = directory.handle();
    let weak = engine.downgrade();
    let registry = Registry::new(Mode::RefuseSecond, true);
    let error = TelemetryAppendPool::build_until(
        engine,
        registry.clone(),
        limits(2),
        &mut budget(Instant::now() + Duration::from_secs(10)),
    )
    .expect_err("second actual registration refuses");
    assert!(matches!(
        error.cause(),
        TelemetryAppendPoolSetupFailure::Build(_)
    ));
    assert!(
        error
            .to_string()
            .contains("actual second registration refused")
    );
    assert_eq!(registry.entered.load(Ordering::Acquire), 1);
    drop(error);
    assert!(
        weak.upgrade().is_some(),
        "entered native closure still holds complete engine"
    );
    registry.release_and_join();
    assert!(
        weak.upgrade().is_none(),
        "only physical completion releases engine"
    );
}

#[test]
fn genuine_late_registration_retains_actual_error_and_native_worker_custody() {
    let directory = Directory::new();
    let engine = directory.handle();
    let weak = engine.downgrade();
    let registry = Registry::new(Mode::LateFirst, true);
    let expiry = Instant::now() + Duration::from_millis(50);
    let mut budget = budget(expiry);
    let error = TelemetryAppendPool::build_until(engine, registry.clone(), limits(1), &mut budget)
        .expect_err("real entered registration settles after original expiry");
    assert!(matches!(
        error.cause(),
        TelemetryAppendPoolSetupFailure::Build(_)
    ));
    assert!(
        error
            .to_string()
            .contains("late actual registration outcome")
    );
    assert_eq!(registry.facts.lock().expect("actual facts")[0].2, expiry);
    assert_eq!(registry.entered.load(Ordering::Acquire), 1);
    assert!(Instant::now() >= expiry);
    assert!(weak.upgrade().is_some());
    drop(error);
    registry.release_and_join();
    assert!(weak.upgrade().is_none());
}

#[cfg(unix)]
#[test]
fn dropped_pool_keeps_genuine_recovered_root_until_registered_workers_physically_join() {
    use shard_stream_core::DirectoryOwnership;
    use shard_stream_engine::{NativeRecoveryCensus, NativeRecoveryLimits};

    let directory = Directory::new();
    let config = directory.config();
    drop(StreamEngine::open(config.clone()).expect("produce actual native files"));
    let ownership = DirectoryOwnership::acquire(&config.data_dir).expect("real exclusive root");
    let census = NativeRecoveryCensus::read_for_engine(
        config,
        ownership,
        None,
        NativeRecoveryLimits {
            max_files: 256,
            max_entries: 1_000_000,
            max_read_bytes: 128 << 20,
            max_declared_bytes: 128 << 20,
            max_work_bytes: 1 << 30,
            max_frame_bytes: 1 << 20,
            max_manifest_bytes: 1 << 20,
            max_partial_tail_bytes: 1 << 20,
            max_path_bytes: 8192,
        },
        Instant::now() + Duration::from_secs(10),
    )
    .expect("real native recovery census");
    let storage = census.restore_storage().expect("real native restore");
    let recovered = Arc::new(
        StreamEngine::open_recovered_storage(storage, None, None, None, None)
            .expect("actual complete recovered engine owner"),
    );
    let root_weak = Arc::downgrade(recovered.directory_ownership());
    let engine_weak = Arc::downgrade(&recovered);
    let handle = EngineHandle::from_recovered(recovered.clone());
    let registry = Registry::new(Mode::Normal, true);
    let pool = TelemetryAppendPool::build_until(
        handle,
        registry.clone(),
        limits(2),
        &mut budget(Instant::now() + Duration::from_secs(10)),
    )
    .expect("actual owned append worker setup");
    drop(recovered);
    drop(pool);
    assert!(engine_weak.upgrade().is_some());
    assert!(
        root_weak.upgrade().is_some(),
        "actual blocked registered worker holds original root"
    );
    assert!(DirectoryOwnership::acquire(&directory.config().data_dir).is_err());
    registry.release_and_join();
    assert!(engine_weak.upgrade().is_none());
    assert!(root_weak.upgrade().is_none());
    drop(
        DirectoryOwnership::acquire(&directory.config().data_dir)
            .expect("released after physical join"),
    );
}
