//! Callback prerequisite tests using work from a genuine recovered callback.
//! The harness deliberately refuses the rest of that native resume; it is not
//! an implemented telemetry factory or a successful owned-host fixture.
use super::*;
use crate::sink_journal::{RecoveredTransaction, SinkJournalRecoveryLimits};
use crate::{TelemetryRecoveryBudget, TelemetryRecoveryLimits};
use shard_stream_core::{
    DirectoryOwnership, LogicalOffset, LogicalPartitionId, NativeDirectoryBudget,
    NativeDirectoryLimits, PlacementSequence, RecordId, RingEpoch, TopicId, TopicSequencer,
    VirtualLaneId,
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
use std::fs::File;
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
            "telemetry-bounded-callback-{}-{now}-{}",
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
        max_write_chunk_bytes: 16 * 1024,
    }
}

thread_local! {
    static CHECKPOINT_LATE: Cell<bool> = const { Cell::new(false) };
    static COPY_LATE: Cell<bool> = const { Cell::new(false) };
    static COPY_PANIC: Cell<bool> = const { Cell::new(false) };
    static JOURNAL_LATE: Cell<bool> = const { Cell::new(false) };
    static JOURNAL_PANIC: Cell<bool> = const { Cell::new(false) };
    static JOURNAL_HOOK_ENTRIES: Cell<usize> = const { Cell::new(0) };
    static PROMOTION_PAID: Cell<Option<usize>> = const { Cell::new(None) };
}
pub(super) fn before_copy(source: &[DurableAppend], work: &NativeResumeWork) {
    if let Some(minimum) = PROMOTION_PAID.get() {
        assert!(
            work.usage()
                .expect("actual before-clone counters")
                .declared_bytes
                >= minimum,
            "all declared copy/promotion requests must be prepaid before entry"
        );
        assert!(
            source[0].payload.is_unique(),
            "payload has not been cloned yet"
        );
        assert!(
            source[0]
                .transient_context
                .as_ref()
                .expect("real context")
                .is_unique(),
            "context has not been cloned yet"
        );
    }
}
fn wait_for(expiry: Instant) {
    while Instant::now() < expiry {
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
pub(super) fn after_checkpoint(expiry: Instant) {
    if CHECKPOINT_LATE.get() {
        wait_for(expiry);
    }
}
pub(super) fn after_copy(expiry: Instant) {
    if COPY_PANIC.get() {
        panic!("actual copied prefix retained before unwind");
    }
    if COPY_LATE.get() {
        wait_for(expiry);
    }
}
pub(super) fn after_journal(expiry: Instant) {
    JOURNAL_HOOK_ENTRIES.set(JOURNAL_HOOK_ENTRIES.get() + 1);
    if JOURNAL_PANIC.get() {
        panic!("actual journal result retained before unwind");
    }
    if JOURNAL_LATE.get() {
        wait_for(expiry);
    }
}
struct Hooks;
impl Drop for Hooks {
    fn drop(&mut self) {
        CHECKPOINT_LATE.set(false);
        COPY_LATE.set(false);
        COPY_PANIC.set(false);
        JOURNAL_LATE.set(false);
        JOURNAL_PANIC.set(false);
        JOURNAL_HOOK_ENTRIES.set(0);
        PROMOTION_PAID.set(None);
    }
}
fn checkpoint_error(error: &NativeResumeCallbackError) -> &CheckpointCarrier {
    error
        .external_error()
        .expect("actual prepaid checkpoint carrier")
        .downcast_ref::<CheckpointCarrier>()
        .expect("original typed carrier")
}
fn journal_error(error: &NativeResumeCallbackError) -> &JournalCarrier {
    error
        .external_error()
        .expect("actual prepaid journal carrier")
        .downcast_ref::<JournalCarrier>()
        .expect("original typed carrier")
}
fn log_input() -> JournalTransaction {
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{AnyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    };
    use prost::Message;
    let encoded = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1,
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("actual indexed log".into())),
                    }),
                    ..LogRecord::default()
                }],
                ..ScopeLogs::default()
            }],
            ..ResourceLogs::default()
        }],
    }
    .encode_to_vec();
    let events = crate::OtlpLogDecoder
        .decode(&encoded)
        .expect("actual log decoder");
    let envelope = crate::prepare_log_envelope("tenant", &events)
        .expect("actual ingest pack")
        .encode()
        .expect("actual STEL envelope");
    let p = TopicPartition::new(
        crate::LOGS_TOPIC_ID,
        shard_stream_core::LogicalPartitionId::new(0),
    );
    let (append, expected, next) = crate::sink::tests::durable_append(p, envelope, 41);
    JournalTransaction {
        expected,
        appends: vec![append],
        next,
    }
}
fn indexed_factory() -> (TelemetrySinkFactory, DurableSinkCheckpoint) {
    let factory = TelemetrySinkFactory::new([ShardId::new(0)], OtlpSinkConfig::default())
        .expect("ordinary setup is fixture only, no bounded setup claim");
    let source = log_input();
    let sink = factory
        .open_shard(ShardId::new(0))
        .expect("ordinary fixture worker");
    assert_eq!(
        sink.apply(source.expected, &source.appends, source.next)
            .expect("actual ordinary indexing"),
        DurableSinkApply::Applied
    );
    drop(sink); // Physically joins the ordinary fixture worker before the callback test.
    (factory, source.next)
}

#[test]
fn bounded_factory_reads_actual_indexed_checkpoint_and_preserves_absence() {
    let (factory, actual) = indexed_factory();
    with_work(Duration::from_secs(10), move |work| {
        assert_eq!(
            factory
                .load_checkpoint_until(actual.topic_partition, work)
                .expect("actual bounded factory callback"),
            Some(actual)
        );
        assert_eq!(
            factory
                .load_checkpoint_until(partition(), work)
                .expect("ordinary absent partition remains absent"),
            None
        );
        assert_eq!(
            factory
                .load_checkpoint(actual.topic_partition)
                .expect("ordinary behavior preserved"),
            Some(actual)
        );
    });
}

#[test]
fn journal_commit_cannot_advance_factory_application_checkpoint_or_claim_applied() {
    let directory = Directory::new();
    let cfg = OtlpSinkConfig {
        state_directory: Some(directory.0.clone()),
        ..OtlpSinkConfig::default()
    };
    let factory = TelemetrySinkFactory::new([ShardId::new(0)], cfg.clone())
        .expect("actual ordinary constructor as component fixture");
    let journal = Arc::clone(
        factory
            .journals
            .lock()
            .expect("actual journal namespace")
            .get(&ShardId::new(0))
            .expect("actual same factory journal"),
    );
    let source = log_input();
    with_work(Duration::from_secs(10), move |work| {
        let pointer = source.appends[0].payload.as_ptr();
        let before = work.usage().expect("same original counters");
        let receipt = append_journal_until(
            &journal,
            source.expected,
            &source.appends,
            source.next,
            limits(),
            work,
        )
        .expect("actual journal-only callback bridge");
        let actual = receipt.journal_outcome().expect("actual complete result");
        assert_eq!(
            actual.kind,
            crate::sink_journal::JournalAppendKind::Committed(source.next)
        );
        assert_eq!(actual.transaction().appends[0].payload.as_ptr(), pointer);
        assert!(
            work.usage().expect("same cumulative owner").declared_bytes > before.declared_bytes
        );
        assert_eq!(
            factory
                .load_checkpoint_until(source.next.topic_partition, work)
                .expect("actual application source"),
            None,
            "journal durability does not mean the index/publication succeeded"
        );
        let sink = factory
            .open_shard(ShardId::new(0))
            .expect("ordinary fixture handle");
        let refused = sink
            .apply_until(source.expected, &source.appends, source.next, work)
            .expect_err("full bounded index producer still absent");
        assert!(
            refused.external_error().is_none(),
            "default unsupported refusal enters no ordinary indexing"
        );
        assert_eq!(
            factory
                .load_checkpoint(source.next.topic_partition)
                .expect("still not applied"),
            None
        );
        drop(sink);
        drop(receipt);
        drop(journal);
        drop(factory);
        let recovered = TelemetrySinkFactory::new([ShardId::new(0)], cfg)
            .expect("genuine existing journal recovery actually indexes its real STEL input");
        assert_eq!(
            recovered
                .load_checkpoint(source.next.topic_partition)
                .expect("real recovered index"),
            Some(source.next)
        );
        drop(recovered);
        drop(directory);
    });
}

#[test]
fn checkpoint_try_lock_poison_and_capacity_refusals_retain_actual_map() {
    let (factory, actual) = indexed_factory();
    with_work(Duration::from_secs(10), move |work| {
        let held = factory.checkpoints.lock().expect("actual contended map");
        let busy = factory
            .load_checkpoint_until(actual.topic_partition, work)
            .expect_err("no mutex wait");
        assert!(matches!(
            checkpoint_error(&busy).failure,
            Some(Failure::Busy)
        ));
        assert!(Arc::ptr_eq(
            &checkpoint_error(&busy).owner,
            &factory.checkpoints
        ));
        drop(busy);
        drop(held);
        let owner = Arc::clone(&factory.checkpoints);
        std::thread::spawn(move || {
            let _held = owner.lock().expect("real poison guard");
            panic!("actual mutex poison");
        })
        .join()
        .expect_err("actual fixture panic joined");
        let poison = factory
            .load_checkpoint_until(actual.topic_partition, work)
            .expect_err("native poison discriminant");
        assert!(matches!(
            checkpoint_error(&poison).failure,
            Some(Failure::Poisoned)
        ));
        assert!(checkpoint_error(&poison).owner.is_poisoned());
    });
    let (factory, actual) = indexed_factory();
    factory
        .checkpoints
        .lock()
        .expect("actual spare slots")
        .reserve(4096);
    with_work(Duration::from_secs(10), move |work| {
        let remaining =
            work.limits().max_work_bytes - work.usage().expect("real counters").work_bytes;
        work.reserve_values(0, 0, 0, remaining - 16_384)
            .expect("same finite work nearly spent");
        let error = factory
            .load_checkpoint_until(actual.topic_partition, work)
            .expect_err("actual spare capacity probe refuses");
        let carrier = checkpoint_error(&error);
        assert!(matches!(
            carrier.failure,
            Some(Failure::Refused(NativeResumeRefusal::Capacity))
        ));
        assert!(carrier.observed_capacity.expect("actual capacity saved") >= 4096);
        assert!(carrier.observed.is_none(), "refusal precedes the real get");
    });
}

#[test]
fn actual_checkpoint_late_result_keeps_value_and_real_owner_until_error_drop() {
    let (factory, actual) = indexed_factory();
    let weak = Arc::downgrade(&factory.checkpoints);
    with_work(Duration::from_millis(200), move |work| {
        CHECKPOINT_LATE.set(true);
        let hooks = Hooks;
        let error = factory
            .load_checkpoint_until(actual.topic_partition, work)
            .expect_err("actual get precedes original E");
        drop(hooks);
        let carrier = checkpoint_error(&error);
        assert_eq!(carrier.observed, Some(Some(actual)));
        assert!(matches!(
            carrier.failure,
            Some(Failure::Refused(NativeResumeRefusal::Expired))
        ));
        drop(factory);
        assert!(weak.upgrade().is_some(), "the actual map remains owned");
        drop(error);
        assert!(weak.upgrade().is_none(), "no detached owner remains");
    });
}

#[test]
fn copied_prefix_survives_actual_expiry_or_unwind_with_borrowed_source_still_owned() {
    for panic in [false, true] {
        let (directory, journal) = setup();
        let source = [input(0).appends.remove(0), input(1).appends.remove(0)];
        with_work(Duration::from_millis(200), move |work| {
            COPY_LATE.set(!panic);
            COPY_PANIC.set(panic);
            let hooks = Hooks;
            let error = append_journal_until(
                &journal,
                input(0).expected,
                &source,
                input(1).next,
                limits(),
                work,
            )
            .expect_err("real copied prefix retained before original E/unwind");
            drop(hooks);
            let carrier = journal_error(&error);
            assert_eq!(carrier.source_len, 2);
            assert_eq!(carrier.copied.len(), 1);
            assert_eq!(
                carrier.copied[0].payload.as_ptr(),
                source[0].payload.as_ptr()
            );
            assert_eq!(
                carrier.copied[0].transient_context,
                source[0].transient_context
            );
            assert!(carrier.outcome.is_none(), "journal was never entered");
            assert_eq!(
                source.len(),
                2,
                "actual caller still owns every original borrowed element"
            );
            assert_eq!(
                fs::read(directory.0.join("shard-0.journal")).expect("physical original file"),
                b"STELSNK1"
            );
        });
    }
}

#[test]
fn genuine_partial_journal_error_is_moved_into_prepaid_callback_carrier_and_keeps_lock() {
    let (directory, journal) = setup();
    with_work(Duration::from_secs(10), move |work| {
        let weak = Arc::downgrade(&journal);
        let source = input(0);
        let mut local = limits();
        local.max_write_chunk_bytes = 1;
        local.max_io_attempts = 4;
        let error = append_journal_until(
            &journal,
            source.expected,
            &source.appends,
            source.next,
            local,
            work,
        )
        .expect_err("actual inner partial frame refusal");
        let carrier = journal_error(&error);
        assert!(matches!(carrier.failure, Some(Failure::Journal)));
        assert!(matches!(carrier.outcome, Some(Err(_))));
        assert!(
            carrier.copied.is_empty(),
            "input moved into the real retained journal result"
        );
        assert_eq!(
            fs::metadata(directory.0.join("shard-0.journal"))
                .expect("physical partial frame")
                .len(),
            9
        );
        assert!(
            journal
                .append(source.expected, &source.appends, source.next)
                .is_err(),
            "same terminal latch guards ordinary append"
        );
        drop(journal);
        assert!(weak.upgrade().is_some());
        let contender = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.0.join("shard-0.journal"))
            .expect("actual competitor");
        assert!(fs2::FileExt::try_lock_exclusive(&contender).is_err());
        drop(contender);
        drop(error);
        assert!(weak.upgrade().is_none());
        let (recovered, replay) = reopen(&directory.0);
        assert!(
            replay.is_empty(),
            "real recovery repairs actual one-byte tail"
        );
        drop(recovered);
    });
}

#[test]
fn actual_complete_journal_outcome_survives_late_or_panicking_callback_bridge() {
    for panic in [false, true] {
        let (directory, journal) = setup();
        with_work(Duration::from_millis(200), move |work| {
            let callback_observed_at = Instant::now();
            JOURNAL_LATE.set(!panic);
            JOURNAL_PANIC.set(panic);
            let hooks = Hooks;
            let source = input(0);
            let append_started_at = Instant::now();
            let error = append_journal_until(
                &journal,
                source.expected,
                &source.appends,
                source.next,
                limits(),
                work,
            )
            .expect_err("actual complete inner result precedes callback refusal");
            let observed_at = Instant::now();
            let journal_hook_entries = JOURNAL_HOOK_ENTRIES.get();
            drop(hooks);
            let carrier = journal_error(&error);
            let Some(Ok(actual)) = &carrier.outcome else {
                panic!(
                    "original complete journal outcome retained; expected_panic={panic}; \
                     completed_result_hook_entries={journal_hook_entries}; \
                     callback_observed_at={callback_observed_at:?}; \
                     append_started_at={append_started_at:?}; observed_at={observed_at:?}; \
                     original_expiry={:?}; usage={:?}; carrier={carrier:?}",
                    work.absolute_expiry(),
                    work.usage()
                );
            };
            assert_eq!(
                journal_hook_entries, 1,
                "completed-result bridge hook entered"
            );
            assert_eq!(
                actual.transaction().appends[0].payload.as_ptr(),
                source.appends[0].payload.as_ptr()
            );
            assert_eq!(
                actual.kind,
                crate::sink_journal::JournalAppendKind::Committed(source.next)
            );
            assert_eq!(carrier.panic.is_some(), panic);
            drop(error);
            drop(journal);
            let (recovered, replay) = reopen(&directory.0);
            assert_eq!(replay.len(), 1);
            drop(recovered);
        });
    }
}

#[test]
fn same_work_initial_carrier_refusal_allocates_no_external_error_and_leaves_input_owned() {
    let (directory, journal) = setup();
    with_work(Duration::from_secs(10), move |work| {
        let source = input(0);
        let remaining = work.limits().max_declared_bytes
            - work
                .usage()
                .expect("actual cumulative counters")
                .declared_bytes;
        work.reserve_values(remaining, 0, 0, 0)
            .expect("same counters fully spent");
        let error = append_journal_until(
            &journal,
            source.expected,
            &source.appends,
            source.next,
            limits(),
            work,
        )
        .expect_err("carrier refused before allocation/copy/native entry");
        assert!(
            error.external_error().is_none(),
            "no emergency Box on exhausted work"
        );
        assert_eq!(
            source.appends.len(),
            1,
            "the original caller input remains owned"
        );
        assert_eq!(
            fs::read(directory.0.join("shard-0.journal")).expect("actual unchanged file"),
            b"STELSNK1"
        );
    });
}

#[test]
fn actual_vec_backed_payload_and_context_promotions_are_prepaid_before_first_clone() {
    let (directory, journal) = setup();
    with_work(Duration::from_secs(10), move |work| {
        let mut source = input(0);
        source.appends[0].payload = Bytes::from(vec![3_u8; 128].into_boxed_slice());
        source.appends[0].transient_context = Some(Bytes::from(vec![4_u8; 96].into_boxed_slice()));
        assert!(source.appends[0].payload.is_unique());
        assert!(
            source.appends[0]
                .transient_context
                .as_ref()
                .expect("context")
                .is_unique()
        );
        let before = work
            .usage()
            .expect("actual original counters")
            .declared_bytes;
        let minimum = before
            + size_of::<JournalCarrier>()
            + size_of::<DurableAppend>()
            + 128
            + 96
            + 2 * BYTES_PROMOTION_BYTES;
        PROMOTION_PAID.set(Some(minimum));
        let hooks = Hooks;
        let receipt = append_journal_until(
            &journal,
            source.expected,
            &source.appends,
            source.next,
            limits(),
            work,
        )
        .expect("actual source promotion and journal commit");
        drop(hooks);
        assert!(
            !source.appends[0].payload.is_unique(),
            "the real first clone promoted shared ownership"
        );
        assert!(
            !source.appends[0]
                .transient_context
                .as_ref()
                .expect("context")
                .is_unique()
        );
        let actual = receipt
            .journal_outcome()
            .expect("actual complete journal outcome");
        assert_eq!(
            actual.transaction().appends[0].payload.as_ptr(),
            source.appends[0].payload.as_ptr()
        );
        drop(receipt);
        drop(journal);
        let (recovered, replay) = reopen(&directory.0);
        assert_eq!(replay.len(), 1);
        assert_eq!(&replay[0].appends[0].payload[..], &[3_u8; 128]);
        drop(recovered);
    });
}
