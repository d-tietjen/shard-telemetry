//! Component journal tests using SAME work from a genuine recovered callback.
//! The harness deliberately refuses the rest of that native resume; it is not
//! an implemented telemetry factory or a successful owned-host fixture.
use super::*;
use crate::{TelemetryRecoveryBudget, TelemetryRecoveryLimits};
use shard_stream_core::{
    DirectoryOwnership, NativeDirectoryBudget, NativeDirectoryLimits, RecordId, RingEpoch,
    TopicSequencer, VirtualLaneId,
};
use shard_stream_engine::{
    DurableAppendDelivery, DurableAppendSink, DurableAppendSinkFactory, DurableSinkApply,
    DurableSinkConfig, DurableSinkOptions, DurableSinkRecoveryMode, EngineConfig, EngineError,
    EngineHandle, EngineResult, NativeFetchLimits, NativePrimaryRecovery, NativeRecoveryCensus,
    NativeRecoveryLimits, NativeResumeCallbackError, NativeResumeCallbackResult,
    NativeResumeGcPolicy, NativeResumeLimits, PinnedShardDirectoryLimits, PinnedShardGcChecks,
    PinnedShardGcLimits, PinnedStoreOpenChecks, PinnedStoreOpenLimits, ShardDirectoryCheckFailure,
    ShardDirectoryChecks, ShardGuardAcquireLimits, StreamEngine, TopicConfig,
};
use shard_stream_protocol::{AppendRequest, Durability};
use std::cell::Cell;
use std::fs;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("fixture clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "telemetry-journal-transaction-{}-{now}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual unique root");
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("remove after actual owners drop");
    }
}
fn partition() -> TopicPartition {
    TopicPartition::new(TopicId::new(23), LogicalPartitionId::new(0))
}
fn config(path: &Path) -> EngineConfig {
    EngineConfig {
        data_dir: path.join("data"),
        object_store_dir: None,
        shard_count: 1,
        virtual_lane_count: 1,
        replication_factor: 1,
        min_in_sync_replicas: 1,
        queue_slots_per_shard: 16,
        queue_bytes_per_shard: 2 << 20,
        target_pack_bytes: 4096,
        max_pack_age: Duration::from_secs(1),
        max_batch_bytes: 1 << 19,
        max_fetch_bytes: 1 << 20,
        append_linger: Duration::ZERO,
    }
}
struct Checks(AtomicBool);
impl ShardDirectoryChecks for Checks {
    fn check(&self) -> std::result::Result<(), ShardDirectoryCheckFailure> {
        if self.0.load(Ordering::Acquire) {
            Err(ShardDirectoryCheckFailure::Refused)
        } else {
            Ok(())
        }
    }
}
impl PinnedStoreOpenChecks for Checks {
    fn deny(&self) {
        self.0.store(true, Ordering::Release);
    }
}
impl PinnedShardGcChecks for Checks {
    fn deny(&self) {
        self.0.store(true, Ordering::Release);
    }
}
type Job = Box<dyn FnOnce(&NativeResumeWork) + Send>;
struct Harness {
    job: Mutex<Option<Job>>,
    completed: AtomicBool,
    ordinary: AtomicUsize,
}
#[derive(Debug)]
struct RefusingSink;
impl DurableAppendSink for RefusingSink {
    fn apply(
        &self,
        _: DurableSinkCheckpoint,
        _: &[DurableAppend],
        _: DurableSinkCheckpoint,
    ) -> EngineResult<DurableSinkApply> {
        Err(EngineError::UnsupportedDurability(
            "component harness has no ordinary index producer",
        ))
    }
}
impl fmt::Debug for Harness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("actual recovered callback harness")
    }
}
impl DurableAppendSinkFactory for Harness {
    fn validate_append(&self, _: &[u8], _: NonZeroU32) -> EngineResult<()> {
        Ok(())
    }
    fn load_checkpoint(&self, _: TopicPartition) -> EngineResult<Option<DurableSinkCheckpoint>> {
        self.ordinary.fetch_add(1, Ordering::AcqRel);
        Ok(None)
    }
    fn open_shard(&self, _: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
        Ok(Arc::new(RefusingSink))
    }
    fn load_checkpoint_until(
        &self,
        _: TopicPartition,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<Option<DurableSinkCheckpoint>> {
        let job = self
            .job
            .try_lock()
            .expect("actual callback job")
            .take()
            .expect("one original callback");
        job(work);
        self.completed.store(true, Ordering::Release);
        Err(NativeResumeCallbackError::unsupported(
            "component assertions complete; no full sink/factory producer",
        ))
    }
}

fn with_work(duration: Duration, job: impl FnOnce(&NativeResumeWork) + Send + 'static) {
    let root = Directory::new();
    let cfg = config(&root.0);
    {
        let producer = StreamEngine::open(cfg.clone()).expect("actual source engine");
        producer
            .create_topic(TopicConfig {
                topic_id: partition().topic_id,
                partitions: 1,
                shards: None,
            })
            .expect("actual native topic");
        producer
            .append(AppendRequest {
                request_id: 1,
                topic_id: partition().topic_id,
                partition_id: partition().partition_id,
                record_count: 1,
                payload: Bytes::from_static(b"actual source batch"),
                durability: Durability::Leader,
                producer: None,
                atomic_group: None,
                leader_epoch: None,
                extension_context: None,
            })
            .expect("actual source WAL");
    }
    let expiry = Instant::now() + Duration::from_secs(60);
    let budget = Arc::new(
        NativeDirectoryBudget::new(
            NativeDirectoryLimits {
                max_depth: 8,
                max_component_bytes: 1024,
                max_path_bytes: 8192,
                max_operations: 100_000,
                max_calls: 1_000_000,
                max_entries: 10_000_000,
                max_declared_bytes: 1 << 30,
                max_work_bytes: 8usize << 30,
            },
            expiry,
        )
        .expect("original real directory budget"),
    );
    let checks = Arc::new(Checks(AtomicBool::new(false)));
    let gc = PinnedShardGcLimits {
        max_path_bytes: 8192,
        max_name_bytes: 1024,
        max_packs: 16,
        max_batch_slots: 256,
        max_log_start_slots: 256,
        max_topic_slots: 256,
        max_location_slots: 256,
        max_plan_packs: 16,
        max_io_calls: 128,
        max_work_bytes: 1 << 25,
    };
    let primary = NativePrimaryRecovery {
        budget: budget.clone(),
        guard_limits: ShardGuardAcquireLimits {
            max_path_bytes: 8192,
            max_io_calls: 5,
        },
        pin_limits: PinnedShardDirectoryLimits {
            max_path_bytes: 8192,
            max_io_calls: 4,
        },
        open_limits: PinnedStoreOpenLimits {
            max_directory_visits: 128,
            directory_buffer_bytes: 8192,
            max_name_bytes: 1024,
            max_path_bytes: 8192,
            max_packs: 16,
            max_frames: 64,
            max_frame_bytes: 8192,
            max_read_attempts: 256,
            max_read_bytes: 1 << 22,
            max_index_entries: 256,
            max_repair_calls: 64,
            max_io_calls: 512,
            max_outcomes: 1024,
            max_entries: 1 << 16,
            max_declared_bytes: 1 << 20,
            max_work_bytes: 1 << 24,
        },
        open_checks: checks.clone(),
        gc_checks: checks.clone(),
        gc_limits: gc,
    };
    let owner = DirectoryOwnership::acquire(&cfg.data_dir).expect("actual exclusive root");
    let storage = NativeRecoveryCensus::read_for_engine_with_primary(
        cfg,
        owner,
        None,
        NativeRecoveryLimits {
            max_files: 128,
            max_entries: 1_000_000,
            max_read_bytes: 128 << 20,
            max_declared_bytes: 128 << 20,
            max_work_bytes: 1 << 30,
            max_frame_bytes: 1 << 20,
            max_manifest_bytes: 1 << 20,
            max_partial_tail_bytes: 1 << 20,
            max_path_bytes: 8192,
        },
        expiry,
        primary,
    )
    .expect("actual primary native census")
    .restore_storage()
    .expect("actual consuming storage");
    let harness = Arc::new(Harness {
        job: Mutex::new(Some(Box::new(job))),
        completed: AtomicBool::new(false),
        ordinary: AtomicUsize::new(0),
    });
    let sink = DurableSinkConfig::new(harness.clone()).with_options(DurableSinkOptions {
        recovery_mode: DurableSinkRecoveryMode::Blocking,
        worker_count: 1,
        max_pending_items: 32,
        max_pending_bytes: Some(2 << 20),
        ..DurableSinkOptions::default()
    });
    let engine = EngineHandle::from_recovered(Arc::new(
        StreamEngine::open_recovered_storage(storage, None, None, None, Some(sink))
            .expect("actual recovered engine"),
    ));
    let limits = NativeResumeLimits {
        max_partitions: 16,
        max_fetch_passes: 32,
        max_batches: 32,
        max_atomic_groups: 32,
        max_callbacks: 64,
        max_jobs: 128,
        max_steps: 256,
        max_entries: 1_000_000,
        max_declared_bytes: 1 << 26,
        max_work_bytes: 1 << 30,
        fetch: NativeFetchLimits {
            max_plan_batches_per_shard: 128,
            max_selected_batches: 1,
            max_candidate_window: 1024,
            max_entries: 1_000_000,
            max_path_bytes: 8192,
            max_read_attempts: 4096,
            max_read_bytes: 1 << 24,
            max_declared_bytes: 1 << 26,
            max_work_bytes: 1 << 30,
        },
    };
    let actual = engine.resume_durable_sink_until(
        &[partition()],
        NativeResumeGcPolicy {
            budget,
            limits: gc,
            checks,
        },
        limits,
        Instant::now() + duration,
    );
    assert!(
        actual.is_err(),
        "harness refuses after the component callback; no invented sink success"
    );
    assert!(
        harness.completed.load(Ordering::Acquire),
        "real callback assertions must finish rather than vanish in retained engine panic"
    );
    assert_eq!(harness.ordinary.load(Ordering::Acquire), 0);
    drop(actual);
    drop(engine);
    drop(harness);
}

fn setup() -> (Directory, Arc<SinkJournal>) {
    let directory = Directory::new();
    let journal = reopen(&directory.0).0;
    (directory, Arc::new(journal))
}
fn reopen(path: &Path) -> (SinkJournal, Vec<RecoveredTransaction>) {
    let mut budget = TelemetryRecoveryBudget::new(
        TelemetryRecoveryLimits {
            max_path_bytes: 8192,
            max_file_bytes: 16 << 20,
            max_read_bytes: 32 << 20,
            max_directory_entries: 4096,
            max_records: 4096,
            max_json_values: 4096,
            max_string_bytes: 1 << 20,
            max_retained_bytes: 64 << 20,
            max_work_bytes: 128 << 20,
        },
        Instant::now() + Duration::from_secs(10),
    )
    .expect("actual setup/restart ledger");
    SinkJournal::open_until(
        &File::open(path).expect("actual retained parent"),
        ShardId::new(0),
        16 << 20,
        false,
        SinkJournalRecoveryLimits {
            max_file_bytes: 16 << 20,
            max_frame_bytes: 1 << 20,
            max_transactions: 128,
            max_appends: 4096,
            max_payload_bytes: 8 << 20,
            max_io_attempts: 4096,
            max_io_bytes: 64 << 20,
        },
        &mut budget,
    )
    .expect("genuine bounded journal recovery")
}
fn input(offset: u64) -> JournalTransaction {
    // A real neutral sequencer makes plain component reservation coordinates;
    // these values are not a current-writer/finality capability or host claim.
    let mut sequencer = TopicSequencer::new(
        partition().topic_id,
        partition().partition_id,
        RingEpoch::new(1),
        vec![VirtualLaneId::new(0)],
    )
    .expect("actual sequencer");
    let mut reservation = sequencer
        .reserve(NonZeroU32::new(1).expect("one record"))
        .expect("actual reservation");
    for _ in 0..offset {
        reservation = sequencer
            .reserve(NonZeroU32::new(1).expect("one record"))
            .expect("next actual reservation");
    }
    JournalTransaction {
        expected: DurableSinkCheckpoint {
            topic_partition: partition(),
            next_placement_sequence: PlacementSequence::new(offset),
            next_offset: LogicalOffset::new(offset),
        },
        next: DurableSinkCheckpoint {
            topic_partition: partition(),
            next_placement_sequence: PlacementSequence::new(offset + 1),
            next_offset: LogicalOffset::new(offset + 1),
        },
        appends: vec![DurableAppend {
            event_id: RecordId::new(offset as u128),
            physical_shard_id: ShardId::new(0),
            reservation,
            producer_event_id: None,
            atomic_group: None,
            delivery: DurableAppendDelivery::Publish,
            payload: Bytes::from_static(b"actual component transaction body"),
            transient_context: Some(Bytes::from_static(b"original transient context")),
        }],
    }
}
fn limits() -> JournalAppendLimits {
    JournalAppendLimits {
        max_appends: 128,
        max_input_slots: 128,
        max_payload_bytes: 1 << 20,
        max_input_bytes: 2 << 20,
        max_frame_bytes: 2 << 20,
        max_io_attempts: 4096,
        max_write_request_bytes: 8 << 20,
        max_write_chunk_bytes: CHUNK,
    }
}
thread_local! { static EXPIRE: Cell<bool> = const { Cell::new(false) }; static WRITE_PANIC: Cell<bool> = const { Cell::new(false) }; static SYNC_PANIC: Cell<bool> = const { Cell::new(false) }; }
pub(super) fn after_write(expiry: Instant) {
    if WRITE_PANIC.get() {
        panic!("actual write completed before primary panic");
    }
    if EXPIRE.get() {
        while Instant::now() < expiry {
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
}
pub(super) fn after_sync() {
    if SYNC_PANIC.get() {
        panic!("actual fsync returned before independent panic");
    }
}
struct Hooks;
impl Drop for Hooks {
    fn drop(&mut self) {
        EXPIRE.set(false);
        WRITE_PANIC.set(false);
        SYNC_PANIC.set(false);
    }
}

#[test]
fn genuine_callback_work_commits_two_actual_frames_and_restarts_with_exact_checkpoint() {
    let (directory, journal) = setup();
    let first = input(0);
    let second = input(1);
    with_work(Duration::from_secs(10), move |work| {
        let initial = work.usage().expect("actual initial counters");
        let one = journal
            .append_until(first, limits(), work)
            .expect("real first commit");
        assert_eq!(
            one.kind,
            JournalAppendKind::Committed(one.retained.input.next)
        );
        assert!(matches!(one.retained.sync, Some(Ok(()))));
        let usage = work.usage().expect("same cumulative counters");
        assert!(
            usage.work_bytes > initial.work_bytes && usage.declared_bytes > initial.declared_bytes
        );
        let two = journal
            .append_until(second, limits(), work)
            .expect("real second commit");
        assert!(work.usage().expect("same counters").work_bytes > usage.work_bytes);
        let duplicate = journal
            .append_until(input(1), limits(), work)
            .expect("actual covered checkpoint");
        assert_eq!(
            duplicate.kind,
            JournalAppendKind::AlreadyCovered(two.retained.input.next)
        );
        drop(duplicate);
        drop(two);
        drop(one);
        drop(journal);
        let (journal, replay) = reopen(&directory.0);
        assert_eq!(replay.len(), 2);
        assert_eq!(
            &replay[0].appends[0].payload[..],
            b"actual component transaction body"
        );
        assert_eq!(replay[1].next.next_offset, LogicalOffset::new(2));
        drop(journal);
    });
}

#[test]
fn genuine_one_byte_write_finite_refusal_latches_both_apis_and_recovery_repairs_tail() {
    let (directory, journal) = setup();
    with_work(Duration::from_secs(10), move |work| {
        let mut local = limits();
        local.max_write_chunk_bytes = 1;
        local.max_io_attempts = 4;
        let error = journal
            .append_until(input(0), local, work)
            .expect_err("real one-byte partial frame");
        assert!(matches!(error.cause, Failure::Limits));
        assert_eq!(error.retained.confirmed_written, 1);
        assert!(matches!(error.retained.sync, Some(Ok(()))));
        assert!(!error.retained.frame_complete && !error.retained.checkpoint_published);
        assert!(
            journal
                .state
                .lock()
                .expect("actual state")
                .terminal_write_refused
        );
        let before = fs::read(directory.0.join("shard-0.journal")).expect("actual partial frame");
        assert_eq!(before.len(), MAGIC.len() + 1);
        assert!(
            journal
                .append(input(0).expected, &[], input(0).next)
                .is_err()
        );
        let again = journal
            .append_until(input(0), limits(), work)
            .expect_err("sticky native terminal latch");
        assert!(matches!(again.cause, Failure::Terminal));
        assert_eq!(
            fs::read(directory.0.join("shard-0.journal")).expect("no unsafe later append"),
            before
        );
        drop(again);
        drop(error);
        drop(journal);
        let (journal, replay) = reopen(&directory.0);
        assert!(replay.is_empty());
        assert_eq!(
            fs::read(directory.0.join("shard-0.journal")).expect("real recovered tail"),
            MAGIC
        );
        drop(journal);
    });
}

#[test]
fn complete_synced_but_late_frame_keeps_true_cache_and_returns_retained_refusal() {
    let (directory, journal) = setup();
    let transaction = input(0);
    let original = transaction.appends[0].payload.as_ptr();
    // Store only a scalar pointer address in this Send fixture; no dereference.
    let original = original as usize;
    with_work(Duration::from_millis(200), move |work| {
        EXPIRE.set(true);
        let hooks = Hooks;
        let error = journal
            .append_until(transaction, limits(), work)
            .expect_err("actual full write settles after original E");
        drop(hooks);
        assert!(matches!(
            error.cause,
            Failure::Refused(NativeResumeRefusal::Expired)
        ));
        assert!(error.retained.frame_complete && error.retained.checkpoint_published);
        assert!(matches!(error.retained.sync, Some(Ok(()))));
        assert_eq!(
            error.retained.input.appends[0].payload.as_ptr() as usize,
            original
        );
        let state = journal.state.lock().expect("actual native cache");
        assert!(!state.terminal_write_refused);
        assert_eq!(state.checkpoints[&partition()], error.retained.input.next);
        drop(state);
        drop(error);
        drop(journal);
        let (journal, replay) = reopen(&directory.0);
        assert_eq!(replay.len(), 1);
        drop(journal);
    });
}

#[test]
fn real_write_and_sync_panics_are_independent_and_keep_partial_source_terminal() {
    let (directory, journal) = setup();
    with_work(Duration::from_secs(10), move |work| {
        WRITE_PANIC.set(true);
        SYNC_PANIC.set(true);
        let hooks = Hooks;
        let mut local = limits();
        local.max_write_chunk_bytes = 1;
        let error = journal
            .append_until(input(0), local, work)
            .expect_err("actual one byte plus independent cleanup panic");
        drop(hooks);
        assert!(matches!(error.cause, Failure::Panic));
        assert!(error.retained.panic.is_some() && error.retained.sync_panic.is_some());
        assert!(matches!(error.retained.sync, Some(Ok(()))));
        assert!(
            journal
                .state
                .lock()
                .expect("actual state")
                .terminal_write_refused
        );
        drop(error);
        drop(journal);
        let (journal, replay) = reopen(&directory.0);
        assert!(replay.is_empty());
        drop(journal);
    });
}

#[test]
fn same_work_cumulative_exhaustion_retains_owned_input_without_wire_or_io() {
    let (directory, journal) = setup();
    with_work(Duration::from_secs(10), move |work| {
        let one = journal
            .append_until(input(0), limits(), work)
            .expect("actual first transaction");
        let remaining = work.limits().max_declared_bytes
            - work.usage().expect("actual spent counters").declared_bytes;
        work.reserve_values(remaining, 0, 0, 0)
            .expect("same work fully spent, never reset");
        let transaction = input(1);
        let capacity = transaction.appends.capacity();
        let error = journal
            .append_until(transaction, limits(), work)
            .expect_err("no emergency allocation on refused native work");
        assert!(matches!(
            error.cause,
            Failure::Refused(NativeResumeRefusal::Capacity)
        ));
        assert_eq!(error.retained.input.appends.capacity(), capacity);
        assert_eq!(error.retained.frame.capacity(), 0);
        assert_eq!(error.retained.io_attempts, 0);
        assert!(
            !journal
                .state
                .lock()
                .expect("actual state")
                .terminal_write_refused
        );
        drop(error);
        drop(one);
        drop(journal);
        let (journal, replay) = reopen(&directory.0);
        assert_eq!(replay.len(), 1);
        drop(journal);
    });
}

#[test]
fn try_lock_and_actual_input_capacity_refuse_before_native_write() {
    let (directory, journal) = setup();
    with_work(Duration::from_secs(10), move |work| {
        let held = journal.state.lock().expect("actual held mutex");
        let busy = journal
            .append_until(input(0), limits(), work)
            .expect_err("actual nonblocking mutex refusal");
        assert!(matches!(busy.cause, Failure::Busy));
        assert_eq!(busy.retained.io_attempts, 0);
        drop(busy);
        drop(held);
        let mut transaction = input(0);
        transaction.appends.reserve_exact(1024);
        let actual_capacity = transaction.appends.capacity();
        let error = journal
            .append_until(transaction, limits(), work)
            .expect_err("actual retained Vec spare capacity exceeds cap");
        assert!(matches!(error.cause, Failure::Limits));
        assert_eq!(error.retained.input.appends.capacity(), actual_capacity);
        assert_eq!(error.retained.frame.capacity(), 0);
        assert_eq!(
            fs::read(directory.0.join("shard-0.journal")).expect("actual original file"),
            MAGIC
        );
    });
}

#[test]
fn actual_read_only_same_inode_write_error_keeps_primary_input_and_independent_sync_panic() {
    let (directory, journal) = setup();
    with_work(Duration::from_secs(10), move |work| {
        // Deliberately replace only descriptor access mode in this component
        // fixture. The real inode, length and original exclusive flock remain
        // owned here; no production constructor or fake I/O result is used.
        let original = {
            let mut state = journal.state.lock().expect("actual state");
            std::mem::replace(
                &mut state.file,
                File::open(directory.0.join("shard-0.journal"))
                    .expect("actual same-inode read-only descriptor"),
            )
        };
        SYNC_PANIC.set(true);
        let hooks = Hooks;
        let transaction = input(0);
        let pointer = transaction.appends[0].payload.as_ptr();
        let error = journal
            .append_until(transaction, limits(), work)
            .expect_err("actual write syscall refuses read-only descriptor");
        drop(hooks);
        let Failure::Io(native) = &error.cause else {
            panic!("original native write error must survive cleanup panic");
        };
        assert_eq!(
            native.raw_os_error(),
            Some(rustix::io::Errno::BADF.raw_os_error())
        );
        assert_eq!(error.retained.confirmed_written, 0);
        assert!(error.retained.entered && error.retained.sync.is_some());
        assert!(error.retained.sync_panic.is_some());
        assert_eq!(error.retained.input.appends[0].payload.as_ptr(), pointer);
        assert!(
            journal
                .state
                .lock()
                .expect("actual state")
                .terminal_write_refused
        );
        assert_eq!(
            fs::read(directory.0.join("shard-0.journal")).expect("real file"),
            MAGIC
        );
        drop(error);
        drop(journal);
        drop(original);
        let (journal, replay) = reopen(&directory.0);
        assert!(replay.is_empty());
        drop(journal);
    });
}

#[test]
fn actual_same_inode_path_descriptor_retains_independent_seek_and_sync_errors() {
    let (directory, journal) = setup();
    with_work(Duration::from_secs(10), move |work| {
        // O_PATH really names the same regular inode, but the kernel refuses
        // both seek and fsync on it. Retain the genuine locked descriptor too.
        let path_only = File::from(
            rustix::fs::open(
                directory.0.join("shard-0.journal"),
                rustix::fs::OFlags::PATH | rustix::fs::OFlags::NOFOLLOW,
                rustix::fs::Mode::empty(),
            )
            .expect("actual same-inode O_PATH descriptor"),
        );
        let original = {
            let mut state = journal.state.lock().expect("actual state");
            std::mem::replace(&mut state.file, path_only)
        };
        let error = journal
            .append_until(input(0), limits(), work)
            .expect_err("actual seek and independent cleanup fsync both refuse");
        let Failure::Io(primary) = &error.cause else {
            panic!("the original actual seek error is retained");
        };
        assert_eq!(
            primary.raw_os_error(),
            Some(rustix::io::Errno::BADF.raw_os_error())
        );
        let Some(Err(settlement)) = &error.retained.sync else {
            panic!("the separate native fsync error is retained too");
        };
        assert_eq!(
            settlement.raw_os_error(),
            Some(rustix::io::Errno::BADF.raw_os_error())
        );
        assert_eq!(error.retained.confirmed_written, 0);
        assert!(
            journal
                .state
                .lock()
                .expect("actual state")
                .terminal_write_refused
        );
        drop(error);
        drop(journal);
        drop(original);
        let (journal, replay) = reopen(&directory.0);
        assert!(replay.is_empty());
        drop(journal);
    });
}

#[test]
fn retained_partial_result_owns_real_journal_and_lock_until_genuine_recovery() {
    let (directory, journal) = setup();
    with_work(Duration::from_secs(10), move |work| {
        let weak = Arc::downgrade(&journal);
        let mut local = limits();
        local.max_write_chunk_bytes = 1;
        local.max_io_attempts = 4;
        let error = journal
            .append_until(input(0), local, work)
            .expect_err("actual partial frame owns its file");
        drop(journal);
        assert!(
            weak.upgrade().is_some(),
            "actual result retains the same journal Arc"
        );
        let path = directory.0.join("shard-0.journal");
        let contender = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("real contender descriptor");
        assert!(
            fs2::FileExt::try_lock_exclusive(&contender).is_err(),
            "the retained actual flock prevents competing recovery"
        );
        drop(contender);
        drop(error);
        assert!(
            weak.upgrade().is_none(),
            "no hidden background owner survives result drop"
        );
        let (journal, replay) = reopen(&directory.0);
        assert!(replay.is_empty());
        drop(journal);
    });
}

#[test]
fn actual_shard_and_offset_census_refuse_before_frame_allocation_or_io() {
    let (directory, journal) = setup();
    with_work(Duration::from_secs(10), move |work| {
        let mut wrong_shard = input(0);
        wrong_shard.appends[0].physical_shard_id = ShardId::new(1);
        let error = journal
            .append_until(wrong_shard, limits(), work)
            .expect_err("actual constructor shard cannot be relabeled by caller");
        assert!(matches!(error.cause, Failure::InvalidInput));
        assert_eq!(error.retained.frame.capacity(), 0);
        assert_eq!(error.retained.io_attempts, 0);
        let mut wrong_offset = input(0);
        wrong_offset.next.next_offset = LogicalOffset::new(0);
        let offset = journal
            .append_until(wrong_offset, limits(), work)
            .expect_err("actual append lies outside claimed durable checkpoint");
        assert!(matches!(offset.cause, Failure::InvalidInput));
        assert_eq!(offset.retained.frame.capacity(), 0);
        assert_eq!(offset.retained.io_attempts, 0);
        assert_eq!(
            fs::read(directory.0.join("shard-0.journal")).expect("actual unmodified source"),
            MAGIC
        );
    });
}
