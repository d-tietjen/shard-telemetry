//! Genuine anchored journal + native callback work + consuming log transaction.
//! Component callbacks deliberately refuse the outer resume; no installed
//! factory/host or reconstructed journal reservation is fabricated.
use super::*;
use crate::CompressionCohortId;
use crate::ingest_pack::FrameDecompressionLimits;
use crate::ingest_pack::{IndexedFramePreparationLimits, PreparedIngestPack, prepare_ingest_pack};
use crate::sink_journal::{RetainedAnchoredJournalError, SinkJournalRecoveryLimits};
use crate::structural::EmbeddedIndexDecodeLimits;
use crate::{TelemetryRecoveryBudget, TelemetryRecoveryLimits};
use shard_stream_core::{
    DirectoryOwnership, LogicalOffset, LogicalPartitionId, NativeDirectoryBudget,
    NativeDirectoryLimits, ShardId, TopicPartition,
};
use shard_stream_engine::{
    DurableAppend, DurableAppendDelivery, DurableAppendSink, DurableAppendSinkFactory,
    DurableSinkApply, DurableSinkCheckpoint, DurableSinkConfig, DurableSinkOptions,
    DurableSinkRecoveryMode, EngineConfig, EngineError, EngineHandle, EngineResult,
    NativeFetchLimits, NativePrimaryRecovery, NativeRecoveryCensus, NativeRecoveryLimits,
    NativeResumeCallbackError, NativeResumeCallbackResult, NativeResumeGcPolicy,
    NativeResumeLimits, PinnedShardDirectoryLimits, PinnedShardGcChecks, PinnedShardGcLimits,
    PinnedStoreOpenChecks, PinnedStoreOpenLimits, ShardDirectoryCheckFailure, ShardDirectoryChecks,
    ShardGuardAcquireLimits, StreamEngine, TopicConfig,
};
use shard_stream_protocol::{AppendRequest, Durability};
use std::cell::{Cell, RefCell};
use std::fs::File;
const BYTES_PROMOTION: usize = 3 * size_of::<usize>();
use std::fs;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
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
            "telemetry-consuming-log-apply-{}-{now}-{}",
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
    TopicPartition::new(TelemetrySignal::Logs.topic_id(), LogicalPartitionId::new(0))
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
type Job = Box<
    dyn FnOnce(&NativeResumeWork, DurableAppend, DurableSinkCheckpoint, DurableSinkCheckpoint)
        + Send,
>;
struct Shared {
    job: Mutex<Option<Job>>,
    completed: AtomicBool,
    ordinary: AtomicUsize,
}
struct Harness {
    shared: Arc<Shared>,
}
impl fmt::Debug for Harness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("real WAL callback harness")
    }
}
impl DurableAppendSink for Harness {
    fn apply(
        &self,
        _: DurableSinkCheckpoint,
        _: &[DurableAppend],
        _: DurableSinkCheckpoint,
    ) -> EngineResult<DurableSinkApply> {
        self.shared.ordinary.fetch_add(1, Ordering::AcqRel);
        Err(EngineError::UnsupportedDurability(
            "no ordinary component apply",
        ))
    }
    fn apply_until(
        &self,
        expected: DurableSinkCheckpoint,
        appends: &[DurableAppend],
        next: DurableSinkCheckpoint,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<DurableSinkApply> {
        assert_eq!(appends.len(), 1, "actual single-edge native replay");
        work.reserve_values(
            size_of::<DurableAppend>() + BYTES_PROMOTION,
            0,
            2,
            size_of::<DurableAppend>(),
        )
        .expect("prepaid real borrowed-to-owned fixture clone");
        let input = appends[0].clone();
        let job = self
            .shared
            .job
            .try_lock()
            .expect("actual job lock")
            .take()
            .expect("one actual append");
        job(work, input, expected, next);
        self.shared.completed.store(true, Ordering::Release);
        Err(NativeResumeCallbackError::unsupported(
            "component assertions complete; installed factory/worker continuation remains open",
        ))
    }
}
impl DurableAppendSinkFactory for Harness {
    fn validate_append(&self, _: &[u8], _: NonZeroU32) -> EngineResult<()> {
        Ok(())
    }
    fn load_checkpoint(&self, _: TopicPartition) -> EngineResult<Option<DurableSinkCheckpoint>> {
        self.shared.ordinary.fetch_add(1, Ordering::AcqRel);
        Ok(None)
    }
    fn open_shard(&self, _: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
        Ok(Arc::new(Harness {
            shared: self.shared.clone(),
        }))
    }
    fn load_checkpoint_until(
        &self,
        _: TopicPartition,
        work: &NativeResumeWork,
    ) -> NativeResumeCallbackResult<Option<DurableSinkCheckpoint>> {
        work.check().map_err(NativeResumeCallbackError::refused)?;
        Ok(None)
    }
}

fn with_work(
    sample: PreparedIngestPack,
    duration: Duration,
    job: impl FnOnce(&NativeResumeWork, DurableAppend, DurableSinkCheckpoint, DurableSinkCheckpoint)
    + Send
    + 'static,
) {
    let records = u32::from_le_bytes(sample.payload[4..8].try_into().expect("ordinary count"));
    let wire = Bytes::from(
        TelemetryEnvelope::new(
            TelemetrySignal::Logs,
            "tenant-a",
            records,
            &b"route"[..],
            sample.payload,
        )
        .expect("actual STEL")
        .encode()
        .expect("ordinary checksum"),
    );
    drop(sample.transient_context); // actual WAL never persists this live context
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
                record_count: records,
                payload: wire,
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
    let shared = Arc::new(Shared {
        job: Mutex::new(Some(Box::new(move |work, input, expected, next| {
            assert!(
                input.transient_context.is_none(),
                "real contextless WAL replay"
            );
            job(work, input, expected, next);
        }))),
        completed: AtomicBool::new(false),
        ordinary: AtomicUsize::new(0),
    });
    let harness = Arc::new(Harness {
        shared: shared.clone(),
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
        shared.completed.load(Ordering::Acquire),
        "real callback assertions must finish rather than vanish in retained engine panic"
    );
    assert_eq!(shared.ordinary.load(Ordering::Acquire), 0);
    drop(actual);
    drop(engine);
    drop(harness);
    drop(shared);
}

fn frame_policy() -> IndexedFramePreparationLimits {
    IndexedFramePreparationLimits {
        max_payload_bytes: 1 << 22,
        max_context_bytes: 1 << 22,
        max_records: 1 << 16,
        max_groups: 64,
        max_structural_bytes_per_group: 1 << 24,
        max_checksum_bytes: 1 << 22,
        max_declared_bytes: 1 << 24,
        max_entries: 1 << 20,
        max_work_bytes: 1 << 27,
        decompression: Some(decompression_policy()),
        index: EmbeddedIndexDecodeLimits {
            max_input_bytes: 1 << 20,
            max_records: 1 << 16,
            max_dictionary_entries: 1 << 17,
            max_terms: 1 << 16,
            max_fields: 1 << 16,
            max_locator_ids: 1 << 20,
            max_runs: 1 << 18,
            max_output_bytes: 1 << 24,
            max_allocation_slots: 1 << 22,
            max_work_bytes: 1 << 27,
        },
    }
}
fn original_pack(count: usize, cohorts: u64) -> PreparedIngestPack {
    let events = (0..count)
        .map(|i| crate::OtlpLogEvent {
            timestamp_unix_nanos: 10_000 + i as u64,
            message: Arc::from(if i % 2 == 0 {
                "actual ordinary alpha log"
            } else {
                "worker retry count 42"
            }),
            compression_cohort: CompressionCohortId::new(i as u64 % cohorts),
            ..crate::OtlpLogEvent::default()
        })
        .collect::<Vec<_>>();
    prepare_ingest_pack(&events).expect("actual ordinary SLW1 compressed data and SLT1 index")
}
fn publication_policy() -> LogIndexPublicationLimits {
    LogIndexPublicationLimits {
        max_envelope_bytes: 1 << 22,
        max_context_bytes: 1 << 22,
        max_partitions: 128,
        max_appends_per_partition: 128,
        max_declared_bytes: 1 << 26,
        max_entries: 1 << 20,
        max_work_bytes: 1 << 28,
        frames: frame_policy(),
    }
}
fn stripe() -> LogStripe {
    LogStripe::new(ShardId::new(0), StripeConfig::default())
        .expect("actual ordinary stripe setup outside producer")
}
fn duplicate(input: &DurableAppend, work: &NativeResumeWork) -> DurableAppend {
    let buffers = 1 + usize::from(input.transient_context.is_some());
    work.reserve_values(
        size_of::<DurableAppend>() + buffers * BYTES_PROMOTION,
        0,
        buffers + 1,
        size_of::<DurableAppend>(),
    )
    .expect("same-work fixture clone prepayment");
    input.clone()
}
fn query(stripe: &LogStripe) -> Vec<(u64, String)> {
    stripe
        .query_checked(&LogQuery::new(partition()).with_field("resource.loki.tenant", "tenant-a"))
        .expect("real unchanged ordinary query after publication")
        .iter()
        .map(|m| {
            (
                m.record.record_ref.offset.get(),
                m.record.message.to_string(),
            )
        })
        .collect()
}

fn decompression_policy() -> FrameDecompressionLimits {
    FrameDecompressionLimits {
        max_input_bytes: 1 << 20,
        max_output_bytes: 1 << 20,
        input_chunk_bytes: 97,
        output_chunk_bytes: 83,
        max_stream_calls: 512,
        max_window_log: 20,
        max_decoder_bytes: 2 << 20,
        max_declared_bytes: 4 << 20,
        max_entries: 2 << 20,
        max_work_bytes: 32 << 20,
    }
}
fn journal_policy() -> JournalAppendLimits {
    JournalAppendLimits {
        max_appends: 16,
        max_input_slots: 16,
        max_payload_bytes: 1 << 22,
        max_input_bytes: 1 << 23,
        max_frame_bytes: 1 << 23,
        max_io_attempts: 4096,
        max_write_request_bytes: 1 << 25,
        max_write_chunk_bytes: 16 * 1024,
    }
}
fn policy() -> LogApplyLimits {
    LogApplyLimits {
        publication: publication_policy(),
        journal: journal_policy(),
        max_checkpoint_slots: 128,
        max_cache_slots: 128,
        max_cache_name_bytes: 1 << 20,
        max_cache_partition_slots: 1 << 16,
        max_declared_bytes: 1 << 26,
        max_entries: 1 << 20,
        max_work_bytes: 1 << 28,
    }
}
fn recovery_limits() -> TelemetryRecoveryLimits {
    TelemetryRecoveryLimits {
        max_path_bytes: 8192,
        max_file_bytes: 16 << 20,
        max_read_bytes: 128 << 20,
        max_directory_entries: 4096,
        max_records: 100_000,
        max_json_values: 100_000,
        max_string_bytes: 1 << 20,
        max_retained_bytes: 128 << 20,
        max_work_bytes: 256 << 20,
    }
}
#[allow(
    clippy::result_large_err,
    reason = "retain complete original owners and native outcomes inline without a refusal allocation"
)]
fn open_anchor(
    root: &Directory,
    expiry: Instant,
) -> std::result::Result<Arc<AnchoredSinkJournal>, RetainedAnchoredJournalError> {
    let mut budget = TelemetryRecoveryBudget::new(recovery_limits(), expiry)
        .expect("actual original finite recovery budget");
    SinkJournal::open_anchored_until(
        File::open(&root.0).expect("actual supplied directory descriptor"),
        ShardId::new(0),
        16 << 20,
        false,
        SinkJournalRecoveryLimits {
            max_file_bytes: 16 << 20,
            max_frame_bytes: 1 << 23,
            max_transactions: 128,
            max_appends: 4096,
            max_payload_bytes: 8 << 20,
            max_io_attempts: 4096,
            max_io_bytes: 64 << 20,
        },
        &mut budget,
    )
}
fn setup() -> (Directory, Arc<AnchoredSinkJournal>) {
    let root = Directory::new();
    let anchor = open_anchor(&root, Instant::now() + Duration::from_secs(30))
        .expect("actual anchored native open, no ordinary journal conversion");
    assert!(anchor.recovered().is_empty());
    (root, anchor)
}
fn maps() -> (ApplicationCheckpoints, ActivePartitions) {
    (
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(Mutex::new(HashMap::new())),
    )
}
fn journal_bytes(root: &Directory) -> u64 {
    fs::metadata(root.0.join("shard-0.journal"))
        .expect("actual file stat")
        .len()
}
fn committed(result: &AppliedLogTransaction, next: DurableSinkCheckpoint) {
    assert_eq!(result.apply_result(), Some(DurableSinkApply::Applied));
    assert!(matches!(
        result.retained.journal.as_ref().expect("retained actual result")
            .as_ref().expect("successful bounded bridge")
            .journal_outcome().expect("actual native outcome").kind,
        JournalAppendKind::Committed(actual) if actual == next
    ));
    assert!(matches!(
        result
            .publication()
            .expect("actual published index")
            .outcome(),
        Some(LogIndexPublicationOutcome::Published { .. })
    ));
}

type PhaseAction = (&'static str, Box<dyn FnOnce()>);

thread_local! {
    static TARGET: Cell<Option<(&'static str, bool)>> = const { Cell::new(None) };
    static OBSERVED: Cell<bool> = const { Cell::new(false) };
    static ACTION: RefCell<Option<PhaseAction>> = const { RefCell::new(None) };
}
pub(super) fn after_phase(expiry: Instant, phase: &'static str) {
    let action = ACTION.with(|a| {
        let mut a = a.borrow_mut();
        if a.as_ref().is_some_and(|(target, _)| *target == phase) {
            a.take()
        } else {
            None
        }
    });
    if let Some((_, action)) = action {
        action();
    }
    if TARGET.get().is_some_and(|(target, _)| target == phase) {
        let (_, panic) = TARGET.take().expect("matched actual phase");
        OBSERVED.set(true);
        if panic {
            panic!("observer after actual captured transaction phase");
        }
        while Instant::now() < expiry {
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
}
struct Hooks;
impl Drop for Hooks {
    fn drop(&mut self) {
        TARGET.set(None);
        OBSERVED.set(false);
        ACTION.with(|a| a.borrow_mut().take());
        crate::sink_journal::set_anchored_probe(None);
    }
}

#[test]
fn genuine_contextless_wal_commits_journal_query_and_application_checkpoint() {
    with_work(
        original_pack(12, 3),
        Duration::from_secs(30),
        |work, input, expected, next| {
            let (root, anchor) = setup();
            let (checkpoints, cache) = maps();
            let name: Arc<str> = Arc::from("actual cached tenant");
            let weak = Arc::downgrade(&name);
            cache
                .lock()
                .expect("real cache")
                .insert(name, vec![partition()]);
            let pointer = input.payload.as_ptr();
            let view = TelemetryEnvelope::decode_view(&input.payload).expect("real WAL STEL");
            let mut ordinary = stripe();
            ordinary
                .apply_checkpointed_ingest_pack(
                    Arc::from(view.tenant),
                    input.topic_partition(),
                    input.reservation.first_offset,
                    input.reservation.record_count.get(),
                    Bytes::copy_from_slice(view.payload),
                    None,
                    false,
                    (expected, next),
                )
                .expect("ordinary contextless native decompression reference");
            let before = work.usage().expect("same original native work");
            let result = stripe()
                .apply_durable_log_until(
                    input,
                    expected,
                    next,
                    anchor.clone(),
                    checkpoints.clone(),
                    cache.clone(),
                    policy(),
                    work,
                )
                .expect("actual complete consuming log transaction");
            committed(&result, next);
            assert_eq!(
                result
                    .publication()
                    .expect("published")
                    .input()
                    .payload
                    .as_ptr(),
                pointer
            );
            assert_eq!(
                query(result.publication().expect("published").stripe()),
                query(&ordinary)
            );
            assert_eq!(
                query(result.publication().expect("published").stripe()).len(),
                12
            );
            assert_eq!(
                checkpoints.lock().expect("actual application map")[&partition()],
                next
            );
            assert!(cache.lock().expect("same actual cache").is_empty());
            assert!(
                weak.upgrade().is_some(),
                "old cache remains in complete result"
            );
            assert!(!anchor.is_denied());
            assert!(journal_bytes(&root) > 8);
            assert!(work.usage().expect("actual cumulative fees").work_bytes > before.work_bytes);
            drop(result);
            assert!(
                weak.upgrade().is_none(),
                "old cache releases only with its actual result"
            );
            drop(anchor);
            let reopened = open_anchor(&root, Instant::now() + Duration::from_secs(30))
                .expect("actual second anchored constructor");
            assert_eq!(reopened.recovered().len(), 1);
            assert_eq!(reopened.recovered()[0].expected, expected);
            assert_eq!(reopened.recovered()[0].next, next);
            assert_eq!(reopened.recovered()[0].appends.len(), 1);
        },
    );
}

#[test]
fn real_application_conflict_does_not_duplicate_journal_or_claim_applied() {
    with_work(
        original_pack(6, 2),
        Duration::from_secs(30),
        |work, input, expected, next| {
            let (root, anchor) = setup();
            let (checkpoints, cache) = maps();
            let duplicate = duplicate(&input, work);
            let first = stripe()
                .apply_durable_log_until(
                    input,
                    expected,
                    next,
                    anchor.clone(),
                    checkpoints.clone(),
                    cache.clone(),
                    policy(),
                    work,
                )
                .expect("real first application");
            committed(&first, next);
            let stripe = first
                .into_stripe()
                .expect("only successful stripe custody returns");
            let bytes = journal_bytes(&root);
            let rows = query(&stripe);
            let conflict = stripe
                .apply_durable_log_until(
                    duplicate,
                    expected,
                    next,
                    anchor.clone(),
                    checkpoints.clone(),
                    cache,
                    policy(),
                    work,
                )
                .expect("actual application checkpoint conflict is a scalar result");
            assert_eq!(
                conflict.apply_result(),
                Some(DurableSinkApply::CheckpointConflict(next))
            );
            assert!(conflict.retained.journal.is_none() && conflict.retained.prepared.is_none());
            assert_eq!(journal_bytes(&root), bytes);
            assert_eq!(
                query(&conflict.into_stripe().expect("no failed stripe extraction")),
                rows
            );
            assert!(!anchor.is_denied());
        },
    );
}

#[test]
fn precovered_different_journal_payload_never_upgrades_to_this_input_applied() {
    with_work(
        original_pack(6, 2),
        Duration::from_secs(30),
        |work, input, expected, next| {
            let (root, anchor) = setup();
            let (checkpoints, cache) = maps();
            let pointer = input.payload.as_ptr();
            let mut different = duplicate(&input, work);
            // Deliberately adversarial component input, preserving the actual WAL
            // identity/edge while proving the journal primitive does not compare data.
            different.payload = Bytes::from_static(b"different actual journal content");
            let seeded = anchor
                .append_journal_until(expected, &[different], next, journal_policy(), work)
                .expect("real bounded write establishes a durable covered watermark");
            assert_eq!(
                seeded.journal_outcome().expect("native outcome").kind,
                JournalAppendKind::Committed(next)
            );
            let bytes = journal_bytes(&root);
            let error = stripe()
                .apply_durable_log_until(
                    input,
                    expected,
                    next,
                    anchor.clone(),
                    checkpoints.clone(),
                    cache,
                    policy(),
                    work,
                )
                .expect_err("coverage has no exact content proof");
            assert!(matches!(error.cause, Failure::Eligibility(_)));
            assert_eq!(
                error.input().expect("same original input").payload.as_ptr(),
                pointer
            );
            assert_eq!(
                error
                    .retained
                    .journal
                    .as_ref()
                    .expect("complete actual coverage result")
                    .as_ref()
                    .expect("bridge succeeded")
                    .journal_outcome()
                    .expect("native coverage")
                    .kind,
                JournalAppendKind::AlreadyCovered(next)
            );
            assert!(error.retained.published.is_none());
            assert_eq!(error.application_effect(), None);
            assert!(checkpoints.lock().expect("same actual map").is_empty());
            assert_eq!(journal_bytes(&root), bytes);
            assert!(anchor.is_denied());
            drop(error);
            drop(seeded);
        },
    );
}

#[test]
fn real_checkpoint_cache_contention_poison_and_capacity_refuse_before_journal_entry() {
    for mode in 0..5 {
        with_work(
            original_pack(6, 2),
            Duration::from_secs(30),
            move |work, input, expected, next| {
                let (root, anchor) = setup();
                let (checkpoints, cache) = maps();
                let bytes = journal_bytes(&root);
                let mut limits = policy();
                let cp_guard = (mode == 0).then(|| checkpoints.lock().expect("real contention"));
                let cache_guard = (mode == 1).then(|| cache.lock().expect("real contention"));
                if mode == 2 {
                    let cloned = cache.clone();
                    assert!(
                        std::thread::spawn(move || {
                            let _guard = cloned.lock().expect("real lock");
                            panic!("actual native mutex poison");
                        })
                        .join()
                        .is_err()
                    );
                }
                if mode == 3 {
                    checkpoints.lock().expect("actual table").reserve(64);
                    limits.max_checkpoint_slots = 1;
                }
                if mode == 4 {
                    cache.lock().expect("actual cache").reserve(64);
                    limits.max_cache_slots = 1;
                }
                let error = stripe()
                    .apply_durable_log_until(
                        input,
                        expected,
                        next,
                        anchor.clone(),
                        checkpoints.clone(),
                        cache.clone(),
                        limits,
                        work,
                    )
                    .expect_err("actual bounded refusal");
                assert!(matches!(
                    error.cause,
                    Failure::Busy | Failure::Poisoned | Failure::Limits
                ));
                assert!(error.retained.journal.is_none());
                assert_eq!(journal_bytes(&root), bytes);
                assert!(anchor.is_denied());
                assert_eq!(error.application_effect(), None);
                drop(cp_guard);
                drop(cache_guard);
            },
        );
    }
}

#[test]
fn current_owner_denial_is_observed_after_captured_physical_phases_before_guard_release() {
    for phase in ["index-prepared", "journal-settled", "index-settled"] {
        with_work(
            original_pack(6, 2),
            Duration::from_secs(30),
            move |work, input, expected, next| {
                let _hooks = Hooks;
                let (root, anchor) = setup();
                let (checkpoints, cache) = maps();
                let second = duplicate(&input, work);
                let actual_work = work.clone();
                let second_anchor = anchor.clone();
                let second_checkpoints = checkpoints.clone();
                let second_cache = cache.clone();
                ACTION.with(|a| {
                    *a.borrow_mut() = Some((
                        phase,
                        Box::new(move || {
                            assert!(
                                second_checkpoints.try_lock().is_err(),
                                "real guard spans physical phase"
                            );
                            let error = stripe()
                                .apply_durable_log_until(
                                    second,
                                    expected,
                                    next,
                                    second_anchor.clone(),
                                    second_checkpoints,
                                    second_cache,
                                    policy(),
                                    &actual_work,
                                )
                                .expect_err("same actual namespace is Busy");
                            assert!(matches!(error.cause, Failure::Busy));
                            assert!(
                                second_anchor.is_denied(),
                                "negative refusal precedes return"
                            );
                        }),
                    ))
                });
                let error = stripe()
                    .apply_durable_log_until(
                        input,
                        expected,
                        next,
                        anchor.clone(),
                        checkpoints.clone(),
                        cache.clone(),
                        policy(),
                        work,
                    )
                    .expect_err("in-flight transaction observes actual sticky denial");
                assert!(matches!(error.cause, Failure::Terminal));
                assert!(
                    checkpoints
                        .try_lock()
                        .expect("guard released only after terminal capture")
                        .is_empty()
                );
                assert!(cache.try_lock().is_ok());
                assert_eq!(error.application_effect(), None);
                if phase != "index-prepared" {
                    assert_eq!(
                        error
                            .retained
                            .journal
                            .as_ref()
                            .expect("actual durable result retained")
                            .as_ref()
                            .expect("native bridge result")
                            .journal_outcome()
                            .expect("native outcome")
                            .kind,
                        JournalAppendKind::Committed(next)
                    );
                    assert!(journal_bytes(&root) > 8);
                }
                assert_eq!(error.retained.published.is_some(), phase == "index-settled");
            },
        );
    }
}

#[test]
fn actual_durable_index_and_application_effects_survive_late_expiry_and_observer_panic() {
    for phase in [
        "index-prepared",
        "journal-settled",
        "index-settled",
        "application-published",
    ] {
        for panic in [false, true] {
            with_work(
                original_pack(6, 2),
                Duration::from_secs(3),
                move |work, input, expected, next| {
                    let _hooks = Hooks;
                    let (root, anchor) = setup();
                    let (checkpoints, cache) = maps();
                    let pointer = input.payload.as_ptr();
                    TARGET.set(Some((phase, panic)));
                    let error = stripe()
                        .apply_durable_log_until(
                            input,
                            expected,
                            next,
                            anchor.clone(),
                            checkpoints.clone(),
                            cache,
                            policy(),
                            work,
                        )
                        .expect_err("real result captured before original-clock refusal or unwind");
                    assert!(OBSERVED.get(), "target is after the actual phase");
                    assert!(matches!(
                        error.cause,
                        Failure::Panic | Failure::Refused(NativeResumeRefusal::Expired)
                    ));
                    assert_eq!(error.retained.panic.is_some(), panic);
                    assert_eq!(
                        error
                            .input()
                            .expect("source owner retained")
                            .payload
                            .as_ptr(),
                        pointer
                    );
                    assert!(anchor.is_denied());
                    if phase == "index-prepared" {
                        assert!(error.retained.journal.is_none());
                        assert_eq!(journal_bytes(&root), 8);
                    } else {
                        assert_eq!(
                            error
                                .retained
                                .journal
                                .as_ref()
                                .expect("whole physical result")
                                .as_ref()
                                .expect("actual successful bridge")
                                .journal_outcome()
                                .expect("actual native success")
                                .kind,
                            JournalAppendKind::Committed(next)
                        );
                    }
                    if phase == "application-published" {
                        assert_eq!(error.application_effect(), Some(DurableSinkApply::Applied));
                        assert_eq!(error.published_checkpoint(), Some(next));
                        assert_eq!(
                            checkpoints.lock().expect("same actual map")[&partition()],
                            next
                        );
                        assert!(matches!(
                            error.publication_outcome(),
                            Some(LogIndexPublicationOutcome::Published { .. })
                        ));
                    } else {
                        assert_eq!(error.application_effect(), None);
                        assert!(checkpoints.lock().expect("same actual map").is_empty());
                    }
                },
            );
        }
    }
}

#[test]
fn real_partial_native_write_keeps_prepared_input_terminal_owner_and_advisory_lock() {
    with_work(
        original_pack(6, 2),
        Duration::from_secs(30),
        |work, input, expected, next| {
            let (root, anchor) = setup();
            let (checkpoints, cache) = maps();
            let mut limits = policy();
            // stat + mandatory settlement sync + seek + one real one-byte write;
            // the second actual write attempt cannot be admitted.
            limits.journal.max_io_attempts = 4;
            limits.journal.max_write_chunk_bytes = 1;
            let pointer = input.payload.as_ptr();
            let error = stripe()
                .apply_durable_log_until(
                    input,
                    expected,
                    next,
                    anchor.clone(),
                    checkpoints.clone(),
                    cache,
                    limits,
                    work,
                )
                .expect_err("real partial journal write refuses");
            assert!(matches!(error.cause, Failure::Journal));
            assert!(error.journal_error().is_some());
            assert_eq!(
                error
                    .input()
                    .expect("actual source retained")
                    .payload
                    .as_ptr(),
                pointer
            );
            assert_eq!(
                journal_bytes(&root),
                9,
                "one actual byte after the eight-byte magic"
            );
            assert!(error.retained.published.is_none());
            assert!(checkpoints.lock().expect("same app map").is_empty());
            drop(anchor);
            assert!(
                open_anchor(&root, Instant::now() + Duration::from_secs(30)).is_err(),
                "terminal result retains the one actual native advisory lock"
            );
            drop(error);
            let repaired = open_anchor(&root, Instant::now() + Duration::from_secs(30))
                .expect("same actual file repairs only after terminal owner drops");
            assert!(repaired.recovered().is_empty());
            assert_eq!(journal_bytes(&root), 8);
        },
    );
}

#[test]
fn recovered_real_rows_remain_owned_and_never_masquerade_as_application_replay() {
    with_work(
        original_pack(6, 2),
        Duration::from_secs(30),
        |work, input, expected, next| {
            let (root, anchor) = setup();
            let repeat = duplicate(&input, work);
            let (checkpoints, cache) = maps();
            let success = stripe()
                .apply_durable_log_until(
                    input,
                    expected,
                    next,
                    anchor.clone(),
                    checkpoints,
                    cache,
                    policy(),
                    work,
                )
                .expect("actual original transaction");
            committed(&success, next);
            drop(success);
            drop(anchor);
            let recovered = open_anchor(&root, Instant::now() + Duration::from_secs(30))
                .expect("real descriptor replay rows");
            assert_eq!(recovered.recovered().len(), 1);
            let recovered_pointer = recovered.recovered()[0].appends[0].payload.as_ptr();
            let bytes = journal_bytes(&root);
            let (fresh_checkpoints, fresh_cache) = maps();
            let error = stripe()
                .apply_durable_log_until(
                    repeat,
                    expected,
                    next,
                    recovered.clone(),
                    fresh_checkpoints.clone(),
                    fresh_cache,
                    policy(),
                    work,
                )
                .expect_err("no application checkpoint invented from durable journal coverage");
            assert!(matches!(error.cause, Failure::Eligibility(_)));
            assert!(
                fresh_checkpoints
                    .lock()
                    .expect("unadopted app state")
                    .is_empty()
            );
            assert_eq!(
                recovered.recovered()[0].appends[0].payload.as_ptr(),
                recovered_pointer
            );
            assert_eq!(journal_bytes(&root), bytes);
            assert_eq!(error.application_effect(), None);
        },
    );
}

#[test]
fn original_shared_work_exhaustion_retains_input_before_native_journal_entry() {
    with_work(
        original_pack(6, 2),
        Duration::from_secs(30),
        |work, input, expected, next| {
            let (root, anchor) = setup();
            let (checkpoints, cache) = maps();
            let pointer = input.payload.as_ptr();
            let spent = work.usage().expect("actual recovered callback ledger");
            work.reserve_values(
                work.limits().max_declared_bytes - spent.declared_bytes,
                0,
                0,
                0,
            )
            .expect("other composing work consumes the SAME allowance without reset");
            let error = stripe()
                .apply_durable_log_until(
                    input,
                    expected,
                    next,
                    anchor.clone(),
                    checkpoints,
                    cache,
                    policy(),
                    work,
                )
                .expect_err("real exhausted original work refuses");
            assert!(matches!(
                error.cause,
                Failure::Refused(NativeResumeRefusal::Capacity)
            ));
            assert_eq!(
                error
                    .input()
                    .expect("input entered before fee admission")
                    .payload
                    .as_ptr(),
                pointer
            );
            assert!(error.retained.prepared.is_none() && error.retained.journal.is_none());
            assert_eq!(journal_bytes(&root), 8);
        },
    );
}

#[test]
fn actual_unsupported_mode_identity_codec_and_child_policy_refuse_before_io() {
    for mode in 0..6 {
        with_work(
            original_pack(6, 2),
            Duration::from_secs(30),
            move |work, mut input, expected, next| {
                let (root, anchor) = setup();
                let object_root = Directory::new();
                let (checkpoints, cache) = maps();
                let mut actual_stripe = stripe();
                let mut limits = policy();
                let mut supplied_next = next;
                match mode {
                    0 => input.delivery = DurableAppendDelivery::Stage,
                    1 => input.physical_shard_id = ShardId::new(1),
                    2 => supplied_next.next_offset = LogicalOffset::new(next.next_offset.get() + 1),
                    3 => input.payload = Bytes::from_static(b"not the actual supported STEL wire"),
                    4 => limits.publication.frames.max_records = 1,
                    5 => {
                        let store = crate::LocalObjectStore::open(object_root.0.join("objects"))
                            .expect("real ordinary object-store setup outside producer scope");
                        let control = Arc::new(
                            SsdObjectCache::open(
                                object_root.0.join("control"),
                                SsdCacheConfig::default(),
                            )
                            .expect("actual cache"),
                        );
                        let payload = Arc::new(
                            SsdObjectCache::open(
                                object_root.0.join("payload"),
                                SsdCacheConfig::default(),
                            )
                            .expect("actual cache"),
                        );
                        actual_stripe
                            .attach_object_tier(
                                SharedTelemetryObjectStore::from(store),
                                object_root.0.join("spool"),
                                (control, payload),
                                [partition()],
                                ObjectTierConfig::default(),
                                false,
                            )
                            .expect("actual attached object tier");
                    }
                    _ => unreachable!(),
                }
                let pointer = input.payload.as_ptr();
                let bytes = journal_bytes(&root);
                let error = actual_stripe
                    .apply_durable_log_until(
                        input,
                        expected,
                        supplied_next,
                        anchor.clone(),
                        checkpoints.clone(),
                        cache,
                        limits,
                        work,
                    )
                    .expect_err("real unsupported input/policy refuses without ordinary fallback");
                assert!(matches!(
                    error.cause,
                    Failure::Eligibility(_) | Failure::InvalidEdge | Failure::Preparation
                ));
                assert_eq!(
                    error
                        .input()
                        .expect("whole actual supplied input")
                        .payload
                        .as_ptr(),
                    pointer
                );
                assert!(error.retained.journal.is_none());
                assert_eq!(error.application_effect(), None);
                assert!(
                    checkpoints
                        .lock()
                        .expect("actual application map")
                        .is_empty()
                );
                assert_eq!(journal_bytes(&root), bytes);
                assert!(anchor.is_denied());
            },
        );
    }
}

#[test]
fn anchored_wrapper_admission_is_paid_before_native_open_and_retains_original_error() {
    let root = Directory::new();
    let fee =
        crate::sink_journal::anchored_journal_cost().expect("checked fixed source-derived fee");
    let mut limits = recovery_limits();
    limits.max_retained_bytes = fee.retained_bytes - 1;
    let mut budget = TelemetryRecoveryBudget::new(limits, Instant::now() + Duration::from_secs(30))
        .expect("real one-less original setup allowance");
    let error = SinkJournal::open_anchored_until(
        File::open(&root.0).expect("actual original fd"),
        ShardId::new(0),
        16 << 20,
        false,
        SinkJournalRecoveryLimits {
            max_file_bytes: 16 << 20,
            max_frame_bytes: 1 << 23,
            max_transactions: 128,
            max_appends: 4096,
            max_payload_bytes: 8 << 20,
            max_io_attempts: 4096,
            max_io_bytes: 64 << 20,
        },
        &mut budget,
    )
    .expect_err("wrapper admission refuses before native open");
    assert!(!error.native_completed() && error.native_error().is_none());
    assert!(error.completed_owner().is_none() && error.panic_payload().is_none());
    assert!(!root.0.join("shard-0.journal").exists());
    assert_eq!(budget.usage().retained_bytes, 0);
    drop(error);
    fs::write(root.0.join("shard-0.journal"), b"invalid!").expect("actual malformed native file");
    let native = open_anchor(&root, Instant::now() + Duration::from_secs(30))
        .expect_err("actual underlying native error remains intact");
    assert!(!native.native_completed());
    assert!(native.native_error().is_some());
}

#[test]
fn anchored_wrapper_preserves_native_success_and_lock_after_late_or_panicking_setup() {
    for phase in ["native-opened", "journal-arc", "anchored-owner"] {
        for panic in [false, true] {
            let _hooks = Hooks;
            let root = Directory::new();
            crate::sink_journal::set_anchored_probe(Some(Box::new(move |expiry, actual| {
                if phase != actual {
                    return;
                }
                if panic {
                    panic!("after actual anchored setup result capture");
                }
                while Instant::now() < expiry {
                    std::thread::park_timeout(Duration::from_millis(1));
                }
            })));
            let error = open_anchor(&root, Instant::now() + Duration::from_secs(1))
                .expect_err("completed actual native owner cannot escape its original setup E");
            assert!(error.native_completed());
            assert_eq!(error.panic_payload().is_some(), panic);
            assert_eq!(error.completed_owner().is_some(), phase == "anchored-owner");
            if let Some(owner) = error.completed_owner() {
                assert!(
                    owner.is_denied(),
                    "terminal setup cannot re-enter live append"
                );
            }
            crate::sink_journal::set_anchored_probe(None);
            assert!(
                open_anchor(&root, Instant::now() + Duration::from_secs(30)).is_err(),
                "late/panicking wrapper retains original native lock"
            );
            drop(error);
            let owner = open_anchor(&root, Instant::now() + Duration::from_secs(30))
                .expect("native lock releases only with whole terminal setup owner");
            assert!(owner.recovered().is_empty());
        }
    }
}
