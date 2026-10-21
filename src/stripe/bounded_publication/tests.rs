//! Actual SLW1/SLT1 fixtures using a genuine recovered callback work owner.
//! The harness deliberately refuses the rest of that native resume; it is not
//! an implemented telemetry factory or a successful owned-host fixture.
use super::*;
use crate::ingest_pack::{PreparedIngestPack, prepare_ingest_pack};
use crate::structural::EmbeddedIndexDecodeLimits;
use shard_stream_core::{
    DirectoryOwnership, LogicalOffset, LogicalPartitionId, NativeDirectoryBudget,
    NativeDirectoryLimits, PlacementSequence, RingEpoch, ShardId, TopicPartition, TopicSequencer,
    VirtualLaneId,
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
use std::cell::Cell;
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
            "telemetry-bounded-log-publication-{}-{now}-{}",
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
            "component assertions complete; full application transaction remains open",
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
    let context = Bytes::from(sample.transient_context);
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
        job: Mutex::new(Some(Box::new(move |work, mut input, expected, next| {
            assert!(
                input.transient_context.is_none(),
                "WAL does not invent live context"
            );
            // The exact ordinary producer's process-local sidecar is captured,
            // not synthesized by replay. Missing-context refusal is tested too.
            work.reserve_values(context.len(), 0, 1, size_of::<Bytes>())
                .expect("real captured context move");
            input.transient_context = Some(context);
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
        decompression: None,
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
fn policy() -> LogIndexPublicationLimits {
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
fn next_input(
    input: &DurableAppend,
    work: &NativeResumeWork,
) -> (DurableAppend, DurableSinkCheckpoint, DurableSinkCheckpoint) {
    // Subsequent component identities come from the real sequencer, not an
    // invented constructor for work/authority. Only the first is the WAL input.
    let mut sequencer = TopicSequencer::new(
        partition().topic_id,
        partition().partition_id,
        RingEpoch::new(1),
        vec![VirtualLaneId::new(0)],
    )
    .expect("real sequencer");
    sequencer
        .reserve(input.reservation.record_count)
        .expect("first actual reservation");
    let reservation = sequencer
        .reserve(input.reservation.record_count)
        .expect("second contiguous reservation");
    let mut copy = duplicate(input, work);
    copy.reservation = reservation;
    let expected = DurableSinkCheckpoint {
        topic_partition: partition(),
        next_offset: reservation.first_offset,
        next_placement_sequence: reservation.placement.sequence,
    };
    let next = DurableSinkCheckpoint {
        topic_partition: partition(),
        next_offset: LogicalOffset::new(reservation.last_offset.get() + 1),
        next_placement_sequence: PlacementSequence::new(reservation.placement.sequence.get() + 1),
    };
    (copy, expected, next)
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

thread_local! {
    static TARGET: Cell<Option<(&'static str, bool)>> = const { Cell::new(None) };
    static OBSERVED: Cell<bool> = const { Cell::new(false) };
}
pub(super) fn after_value(expiry: Instant, phase: &'static str) {
    let Some((target, panic)) = TARGET.get() else {
        return;
    };
    if target != phase {
        return;
    }
    TARGET.set(None);
    OBSERVED.set(true);
    if panic {
        panic!("actual publication observer unwind");
    }
    while Instant::now() < expiry {
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
struct Hook;
impl Drop for Hook {
    fn drop(&mut self) {
        TARGET.set(None);
        OBSERVED.set(false);
    }
}

#[test]
fn real_wal_stel_to_normal_query_frames_matches_ordinary_publication() {
    with_work(
        original_pack(12, 3),
        Duration::from_secs(30),
        |work, input, expected, next| {
            let payload_pointer = input.payload.as_ptr();
            let context_pointer = input
                .transient_context
                .as_ref()
                .expect("actual captured live sidecar")
                .as_ptr();
            let view = TelemetryEnvelope::decode_view(&input.payload).expect("real checksum");
            let mut ordinary = stripe();
            ordinary
                .apply_checkpointed_ingest_pack(
                    Arc::from(view.tenant),
                    input.topic_partition(),
                    input.reservation.first_offset,
                    input.reservation.record_count.get(),
                    Bytes::copy_from_slice(view.payload),
                    input.transient_context.as_deref(),
                    false,
                    (expected, next),
                )
                .expect("ordinary route");
            let before = work.usage().expect("same ledger");
            let result = stripe()
                .publish_durable_log_until(input, expected, next, policy(), work)
                .expect("actual query publication");
            assert_eq!(result.input().payload.as_ptr(), payload_pointer);
            assert_eq!(
                result
                    .input()
                    .transient_context
                    .as_ref()
                    .expect("retained context")
                    .as_ptr(),
                context_pointer
            );
            assert_eq!(
                result.outcome(),
                Some(LogIndexPublicationOutcome::Published {
                    frames: 3,
                    indexed_through: LogicalOffset::new(11)
                })
            );
            assert_eq!(query(result.stripe()), query(&ordinary));
            assert_eq!(query(result.stripe()).len(), 12);
            let actual = &result.stripe().indexed_frame_partitions[&partition()].appends[0];
            let old = &ordinary.indexed_frame_partitions[&partition()].appends[0];
            assert_eq!(actual.next_checkpoint, Some(next));
            for (a, b) in actual.frames.iter().zip(&old.frames) {
                assert_eq!(a.index, b.index);
                assert_eq!(a.frame_id, b.frame_id);
                assert_eq!(a.compressed, b.compressed);
            }
            assert!(
                work.usage().expect("same native ledger").declared_bytes > before.declared_bytes
            );
        },
    );
}

#[test]
fn repeated_ranges_partial_overlap_and_second_edge_retain_real_ordering() {
    with_work(
        original_pack(6, 2),
        Duration::from_secs(30),
        |work, input, expected, next| {
            let duplicate_input = duplicate(&input, work);
            let (second, second_expected, second_next) = next_input(&input, work);
            let result = stripe()
                .publish_durable_log_until(input, expected, next, policy(), work)
                .expect("first publication");
            let result = result
                .into_stripe()
                .publish_durable_log_until(duplicate_input, expected, next, policy(), work)
                .expect("actual covered range");
            assert_eq!(
                result.outcome(),
                Some(LogIndexPublicationOutcome::AlreadyCovered {
                    indexed_through: LogicalOffset::new(5)
                })
            );
            assert_eq!(result.stripe().next_frame_id, 2);
            let result = result
                .into_stripe()
                .publish_durable_log_until(second, second_expected, second_next, policy(), work)
                .expect("second contiguous edge");
            assert_eq!(query(result.stripe()).len(), 12);
            assert_eq!(result.stripe().next_frame_id, 4);
            assert_eq!(
                result.stripe().indexed_through(partition()),
                Some(LogicalOffset::new(11))
            );
            let mut partial = duplicate(result.input(), work);
            partial.reservation.first_offset = LogicalOffset::new(9);
            partial.reservation.last_offset = LogicalOffset::new(14);
            let mut edge = second_expected;
            edge.next_offset = partial.reservation.first_offset;
            let mut end = second_next;
            end.next_offset = LogicalOffset::new(15);
            let error = result
                .into_stripe()
                .publish_durable_log_until(partial, edge, end, policy(), work)
                .expect_err("partial overlap");
            assert!(matches!(
                error.cause,
                Failure::Codec(TelemetryError::OffsetOutOfOrder { .. })
            ));
            assert_eq!(
                error.retained.stripe.indexed_through(partition()),
                Some(LogicalOffset::new(11))
            );
            assert_eq!(query(&error.retained.stripe).len(), 12);
            assert!(error.retained.prepared.is_none());
        },
    );
}

#[test]
fn reservation_shard_visibility_and_stel_identity_refuse_without_index_mutation() {
    with_work(
        original_pack(4, 2),
        Duration::from_secs(30),
        |work, input, expected, next| {
            for kind in [
                "shard",
                "stage",
                "checkpoint",
                "range",
                "signal",
                "count",
                "checksum",
            ] {
                let mut copy = duplicate(&input, work);
                let mut expected_copy = expected;
                match kind {
                    "shard" => copy.physical_shard_id = ShardId::new(7),
                    "stage" => copy.delivery = DurableAppendDelivery::Stage,
                    "checkpoint" => {
                        expected_copy.next_placement_sequence = PlacementSequence::new(99)
                    }
                    "range" => copy.reservation.last_offset = LogicalOffset::new(99),
                    "signal" | "count" => {
                        let view =
                            TelemetryEnvelope::decode_view(&copy.payload).expect("original STEL");
                        copy.payload = Bytes::from(
                            TelemetryEnvelope::new(
                                if kind == "signal" {
                                    TelemetrySignal::Traces
                                } else {
                                    TelemetrySignal::Logs
                                },
                                "tenant-a",
                                if kind == "count" { 3 } else { 4 },
                                view.routing_metadata,
                                view.payload,
                            )
                            .expect("changed real wire")
                            .encode()
                            .expect("actual rechecksum"),
                        );
                    }
                    _ => {
                        let mut bytes = copy.payload.to_vec();
                        *bytes.last_mut().expect("real body") ^= 1;
                        copy.payload = Bytes::from(bytes);
                    }
                }
                let pointer = copy.payload.as_ptr();
                let error = stripe()
                    .publish_durable_log_until(copy, expected_copy, next, policy(), work)
                    .expect_err(kind);
                assert_eq!(error.input().payload.as_ptr(), pointer);
                assert!(error.retained.stripe.indexed_frame_partitions.is_empty());
                assert_eq!(error.retained.stripe.next_frame_id, 0);
                assert!(error.retained.prepared.is_none());
                if kind == "checksum" {
                    assert!(matches!(
                        error.cause,
                        Failure::Codec(TelemetryError::InvalidTelemetryEnvelope(
                            "envelope checksum mismatch"
                        ))
                    ));
                }
            }
        },
    );
}

#[test]
fn original_child_refusal_and_finite_caps_keep_all_actual_inputs_and_prefixes() {
    with_work(
        original_pack(12, 3),
        Duration::from_secs(30),
        |work, input, expected, next| {
            let mut missing = duplicate(&input, work);
            missing.transient_context = None;
            let error = stripe()
                .publish_durable_log_until(missing, expected, next, policy(), work)
                .expect_err("no unbounded decompression fallback");
            assert!(matches!(error.cause, Failure::Frames));
            assert!(
                error
                    .retained
                    .prepared
                    .as_ref()
                    .expect("captured original child Result")
                    .is_err()
            );
            assert!(error.retained.stripe.indexed_frame_partitions.is_empty());
            for kind in ["input", "records", "groups", "bytes", "append"] {
                let mut limits = policy();
                match kind {
                    "input" => limits.max_envelope_bytes = 1,
                    "records" => limits.frames.max_records = 1,
                    "groups" => limits.frames.max_groups = 1,
                    "bytes" => limits.max_declared_bytes = 1,
                    _ => limits.max_appends_per_partition = 0,
                }
                let copy = duplicate(&input, work);
                let pointer = copy.payload.as_ptr();
                let error = stripe()
                    .publish_durable_log_until(copy, expected, next, limits, work)
                    .expect_err(kind);
                assert_eq!(error.input().payload.as_ptr(), pointer);
                assert!(error.outcome().is_none());
                assert!(error.retained.stripe.indexed_frame_partitions.is_empty());
            }
        },
    );
}

#[test]
fn actual_staged_index_map_and_publication_survive_original_expiry_and_unwind() {
    for phase in [
        "checksum-prefix",
        "prepared",
        "transferred-index",
        "index-arc",
        "converted-frame",
        "old-map",
        "map-entry",
        "staged-append",
        "before-publication",
        "published",
    ] {
        for panic in [false, true] {
            with_work(
                original_pack(6, 2),
                Duration::from_secs(if panic { 30 } else { 1 }),
                move |work, input, expected, next| {
                    let _hook = Hook;
                    TARGET.set(Some((phase, panic)));
                    let pointer = input.payload.as_ptr();
                    let error = stripe()
                        .publish_durable_log_until(input, expected, next, policy(), work)
                        .expect_err("entered phase then original E/unwind");
                    assert!(OBSERVED.get(), "real requested phase reached: {phase}");
                    assert_eq!(error.input().payload.as_ptr(), pointer);
                    if panic {
                        assert!(matches!(error.cause, Failure::Panic));
                        assert!(error.retained.panic.is_some());
                    } else {
                        assert!(matches!(
                            error.cause,
                            Failure::Refused(NativeResumeRefusal::Expired)
                        ));
                    }
                    if phase == "published" {
                        assert_eq!(
                            error.outcome(),
                            Some(LogIndexPublicationOutcome::Published {
                                frames: 2,
                                indexed_through: LogicalOffset::new(5)
                            })
                        );
                        assert_eq!(query(&error.retained.stripe).len(), 6);
                        assert!(
                            error
                                .retained
                                .prepared
                                .as_ref()
                                .expect("original complete child owner")
                                .is_ok()
                        );
                    } else {
                        assert!(error.outcome().is_none());
                    }
                    if phase == "transferred-index" {
                        assert!(error.retained.index.is_some());
                    }
                    if phase == "index-arc" {
                        assert!(error.retained.index_arc.is_some());
                    }
                    if phase == "converted-frame" {
                        assert_eq!(error.retained.frames.len(), 1);
                    }
                    if phase == "staged-append" || phase == "before-publication" {
                        assert_eq!(
                            error
                                .retained
                                .new_append
                                .as_ref()
                                .expect("actual staged frame append")
                                .frames
                                .len(),
                            2
                        );
                        assert_eq!(error.retained.stripe.next_frame_id, 0);
                    }
                },
            );
        }
    }
}

#[test]
fn existing_query_owners_and_active_cache_are_moved_without_deep_clones() {
    with_work(
        original_pack(6, 2),
        Duration::from_secs(30),
        |work, input, expected, next| {
            let (second, second_expected, second_next) = next_input(&input, work);
            let result = stripe()
                .publish_durable_log_until(input, expected, next, policy(), work)
                .expect("first native publication");
            let old_index = Arc::as_ptr(
                &result.stripe().indexed_frame_partitions[&partition()].appends[0].frames[0].index,
            );
            let mut stripe = result.into_stripe();
            stripe
                .active_partition_cache
                .insert(Arc::from("resident tenant cache"), vec![partition()]);
            let cache_pointer = stripe
                .active_partition_cache
                .values()
                .next()
                .expect("actual cache")
                .as_ptr();
            // A second, different partition exercises actual table enumeration and
            // insertion rather than only removing/reinserting the target key.
            let other = TopicPartition::new(partition().topic_id, LogicalPartitionId::new(1));
            stripe
                .indexed_frame_partitions
                .insert(other, IndexedFramePartition::default());
            let result = stripe
                .publish_durable_log_until(second, second_expected, second_next, policy(), work)
                .expect("real migrated query state");
            assert_eq!(result.stripe().indexed_frame_partitions.len(), 2);
            assert_eq!(
                Arc::as_ptr(
                    &result.stripe().indexed_frame_partitions[&partition()].appends[0].frames[0]
                        .index
                ),
                old_index
            );
            assert!(result.stripe().active_partition_cache.is_empty());
            assert_eq!(
                result
                    .retained
                    .retired_cache
                    .as_ref()
                    .expect("retained old cache")
                    .values()
                    .next()
                    .expect("same vector")
                    .as_ptr(),
                cache_pointer
            );
            assert_eq!(query(result.stripe()).len(), 12);
            assert!(
                result.retained.usage.declared_bytes
                    >= result.stripe().indexed_frame_partitions.capacity() * size_of::<MapEntry>()
            );
        },
    );
    for phase in ["old-map", "moved-partition"] {
        with_work(
            original_pack(6, 2),
            Duration::from_secs(30),
            move |work, input, expected, next| {
                let (second, second_expected, second_next) = next_input(&input, work);
                let first = stripe()
                    .publish_durable_log_until(input, expected, next, policy(), work)
                    .expect("actual resident query index before interrupted migration");
                let weak = Arc::downgrade(
                    &first.stripe().indexed_frame_partitions[&partition()].appends[0].frames[0]
                        .index,
                );
                assert_eq!(weak.strong_count(), 1, "one physical resident index owner");
                let mut stripe = first.into_stripe();
                stripe
                    .active_partition_cache
                    .insert(Arc::from("retained occupied cache"), vec![partition()]);
                let cached = stripe
                    .active_partition_cache
                    .values()
                    .next()
                    .expect("actual resident vector")
                    .as_ptr();
                let other = TopicPartition::new(partition().topic_id, LogicalPartitionId::new(1));
                stripe
                    .indexed_frame_partitions
                    .insert(other, IndexedFramePartition::default());
                let _hook = Hook;
                TARGET.set(Some((phase, true)));
                let error = stripe
                    .publish_durable_log_until(second, second_expected, second_next, policy(), work)
                    .expect_err("actual occupied migration observer panic");
                assert!(OBSERVED.get());
                assert!(matches!(error.cause, Failure::Panic));
                assert!(error.retained.old_map.is_some());
                assert!(error.retained.stripe.indexed_frame_partitions.is_empty());
                assert!(error.outcome().is_none());
                assert_eq!(
                    error
                        .retained
                        .stripe
                        .active_partition_cache
                        .values()
                        .next()
                        .expect("same retained cache")
                        .as_ptr(),
                    cached
                );
                // Randomized table order can place this actual index in the
                // retained iterator, target or staged table. It stays alive
                // exactly once in all cases; no deep copy/extractor is needed.
                assert_eq!(weak.strong_count(), 1);
                drop(error);
                assert_eq!(weak.strong_count(), 0, "release only with terminal custody");
            },
        );
    }
}

#[test]
fn exact_and_one_less_original_shared_work_retain_actual_publication_and_staging() {
    for short in [0, 1] {
        with_work(
            original_pack(6, 2),
            Duration::from_secs(30),
            move |work, input, expected, next| {
                // The two equivalent component publications use the SAME real
                // callback work owner. Neither a replacement budget nor an
                // ordinary/fabricated callback supplies the boundary.
                let second = duplicate(&input, work);
                let pointer = second.payload.as_ptr();
                let before = work.usage().expect("actual cumulative ledger");
                let first = stripe()
                    .publish_durable_log_until(input, expected, next, policy(), work)
                    .expect("measure an actual complete publication");
                let after = work.usage().expect("actual complete native fees");
                let paid = after.work_bytes - before.work_bytes;
                assert!(paid > short);
                let remaining = work.limits().max_work_bytes - after.work_bytes;
                let keep = paid - short;
                work.reserve_values(
                    0,
                    0,
                    0,
                    remaining
                        .checked_sub(keep)
                        .expect("finite original ceiling"),
                )
                .expect("prepay other composing work on the original ledger");
                let actual =
                    stripe().publish_durable_log_until(second, expected, next, policy(), work);
                if short == 0 {
                    let published = actual.expect("the exact measured remainder suffices");
                    assert_eq!(published.input().payload.as_ptr(), pointer);
                    assert_eq!(published.outcome(), first.outcome());
                    assert_eq!(query(published.stripe()), query(first.stripe()));
                    assert_eq!(
                        work.usage()
                            .expect("same exact cumulative debit")
                            .work_bytes,
                        work.limits().max_work_bytes
                    );
                } else {
                    let error = actual.expect_err("one fewer original work byte refuses");
                    assert!(matches!(
                        error.cause,
                        Failure::Refused(NativeResumeRefusal::Capacity)
                    ));
                    assert_eq!(error.input().payload.as_ptr(), pointer);
                    assert!(error.outcome().is_none());
                    assert!(error.retained.new_append.is_some());
                    assert!(error.retained.prepared.as_ref().is_some_and(|r| r.is_ok()));
                    assert_eq!(error.retained.stripe.next_frame_id, 0);
                    assert!(error.retained.stripe.indexed_frame_partitions.is_empty());
                }
            },
        );
    }
}
