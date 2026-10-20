//! Actual embedded-index fixtures using a genuine recovered callback work owner.
//! The harness deliberately refuses the rest of that native resume; it is not
//! an implemented telemetry factory or a successful owned-host fixture.
use super::*;
use shard_stream_core::{
    DirectoryOwnership, LogicalOffset, LogicalPartitionId, NativeDirectoryBudget,
    NativeDirectoryLimits, ShardId, TopicId, TopicPartition,
};
use shard_stream_engine::{
    DurableAppend, DurableAppendSink, DurableAppendSinkFactory, DurableSinkApply,
    DurableSinkCheckpoint, DurableSinkConfig, DurableSinkOptions, DurableSinkRecoveryMode,
    EngineConfig, EngineError, EngineHandle, EngineResult, NativeFetchLimits,
    NativePrimaryRecovery, NativeRecoveryCensus, NativeRecoveryLimits, NativeResumeCallbackError,
    NativeResumeCallbackResult, NativeResumeGcPolicy, NativeResumeLimits,
    PinnedShardDirectoryLimits, PinnedShardGcChecks, PinnedShardGcLimits, PinnedStoreOpenChecks,
    PinnedStoreOpenLimits, ShardDirectoryCheckFailure, ShardDirectoryChecks,
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
            "telemetry-bounded-embedded-index-{}-{now}-{}",
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

thread_local! { static LATE: Cell<bool> = const {Cell::new(false)}; static PANIC: Cell<bool> = const {Cell::new(false)}; static TARGET: Cell<Option<&'static str>>=const{Cell::new(None)}; }
pub(super) fn after_value(expiry: Instant, phase: &'static str) {
    if TARGET.get().is_some_and(|target| target != phase) {
        return;
    }
    if PANIC.get() {
        panic!("actual owned partial index precedes unwind");
    }
    if LATE.get() {
        while Instant::now() < expiry {
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
}
struct Hooks;
impl Drop for Hooks {
    fn drop(&mut self) {
        LATE.set(false);
        PANIC.set(false);
        TARGET.set(None);
    }
}
fn limits() -> EmbeddedIndexDecodeLimits {
    EmbeddedIndexDecodeLimits {
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
    }
}
fn ordinary_records() -> Vec<DurableLog> {
    (0..96)
        .map(|offset| {
            DurableLog::new(
                ShardId::new(7),
                partition(),
                LogicalOffset::new(offset),
                10_000 + offset,
                if offset % 2 == 0 {
                    "ordinary alpha message"
                } else {
                    "worker failed after retry count 42 from shard"
                },
                crate::CompressionCohortId::new(4),
            )
            .with_field("service", "billing")
            .with_field("severity", "INFO")
        })
        .collect()
}
fn native_index(records: u32, rle: bool) -> Vec<u8> {
    let ids = (0..records)
        .map(|v| {
            if rle {
                u32::from(v >= records / 2)
            } else {
                v % 2
            }
        })
        .collect::<Vec<_>>();
    let dictionary = if records == 0 {
        0
    } else if records == 1 {
        1
    } else {
        2
    };
    let ids = if records == 1 { vec![0] } else { ids };
    let column = pack_ids(&ids, dictionary).expect("genuine ordinary packer");
    let mut out = Vec::new();
    encode_packed_column(&column, dictionary, records, true, &mut out)
        .expect("genuine column producer");
    encode_optional_sorted_ids(
        if dictionary == 0 { &[] } else { &[0] },
        dictionary,
        &mut out,
    )
    .expect("actual residual locator");
    encode_membership_filter(&MembershipFilter::new(), &mut out);
    write_varint(0, &mut out);
    encode_packed_column(&column, dictionary, records, false, &mut out)
        .expect("genuine field column");
    write_varint(0, &mut out);
    out
}

#[test]
fn actual_ordinary_stlg_and_slt1_sections_decode_to_identical_index() {
    let records = ordinary_records();
    let structural =
        encode_indexed_structural_records(&records).expect("actual ordinary STLG producer");
    let (_, section) =
        structural_sections(&structural.structural).expect("actual durable STLG section");
    assert_eq!(section, structural.embedded_index);
    let events = (0..12)
        .map(|ordinal| crate::OtlpLogEvent {
            timestamp_unix_nanos: ordinal,
            message: Arc::from("actual transient indexed message"),
            compression_cohort: crate::CompressionCohortId::new(7),
            ..crate::OtlpLogEvent::default()
        })
        .collect::<Vec<_>>();
    let prepared = crate::ingest_pack::prepare_ingest_pack(&events)
        .expect("actual ordinary SLW1/SLT1 producer");
    // Genuine SLT1 single-cohort header is8, group header20; bytes16..20 of
    // that group declare the retained embedded section used by live indexing.
    let transient = &prepared.transient_context;
    assert_eq!(&transient[..4], b"SLT1");
    let count = u32::from_le_bytes(transient[16..20].try_into().expect("actual group records"));
    let len = u32::from_le_bytes(
        transient[24..28]
            .try_into()
            .expect("actual group index length"),
    ) as usize;
    let transient_section = transient[28..28 + len].to_vec();
    let cases = [(structural.embedded_index, 96), (transient_section, count)];
    with_work(Duration::from_secs(10), move |work| {
        for (encoded, count) in cases {
            let legacy = decode_embedded_frame_index_section(&encoded, count as usize)
                .expect("genuine legacy result");
            let input = Bytes::from(encoded);
            let pointer = input.as_ptr();
            let actual = EmbeddedFrameIndex::decode_until(input, count, limits(), work)
                .expect("bounded actual decoder");
            assert_eq!(actual.index(), Some(&legacy));
            assert_eq!(
                actual.retained.usage.bytes,
                actual
                    .index()
                    .expect("actual completed value")
                    .cache_bytes()
                    - size_of::<EmbeddedFrameIndex>(),
                "every actual output Vec capacity and both membership boxes are charged"
            );
            assert_eq!(
                actual.encoded().as_ptr(),
                pointer,
                "original input owner retained without clone"
            );
        }
    });
}

#[test]
fn genuine_bitpacked_rle_zero_and_single_dictionary_columns_match_legacy() {
    let cases = [(0, false), (1, false), (33, false), (512, true)]
        .map(|(count, rle)| (native_index(count, rle), count));
    with_work(Duration::from_secs(10), move |work| {
        for (wire, count) in cases {
            let legacy = decode_embedded_frame_index_section(&wire, count as usize)
                .expect("ordinary accepted wire");
            let actual = EmbeddedFrameIndex::decode_until(Bytes::from(wire), count, limits(), work)
                .expect("bounded exact same result");
            assert_eq!(actual.index(), Some(&legacy));
        }
    });
}

#[test]
fn truncated_mutated_and_trailing_wire_preserve_actual_legacy_error_semantics() {
    let produced =
        encode_indexed_structural_records(&ordinary_records()).expect("actual nested producer");
    let mut cases = Vec::new();
    for (valid, count) in [
        (native_index(33, false), 33),
        (native_index(512, true), 512),
        (produced.embedded_index, 96),
    ] {
        for n in 0..valid.len() {
            cases.push((valid[..n].to_vec(), count));
        }
        let mut trailing = valid.clone();
        trailing.push(0);
        cases.push((trailing, count));
        for at in 0..valid.len() {
            for byte in [0, 1, 0x7f, 0x80, 0xff] {
                let mut wire = valid.clone();
                wire[at] = byte;
                cases.push((wire, count));
            }
        }
    }
    cases.push((vec![0x80; 10], 33));
    cases.push((vec![0xff; 10], 33));
    with_work(Duration::from_secs(10), move |work| {
        for (wire, count) in cases {
            let legacy = decode_embedded_frame_index_section(&wire, count as usize);
            let actual = EmbeddedFrameIndex::decode_until(Bytes::from(wire), count, limits(), work);
            match (legacy, actual) {
                (Ok(expected), Ok(actual)) => assert_eq!(actual.index(), Some(&expected)),
                (Err(expected), Err(actual)) => match &actual.cause {
                    Failure::Codec(native) => assert_eq!(native.to_string(), expected.to_string()),
                    _ => panic!(
                        "fitting finite corpus must preserve the real codec cause: {actual:?}"
                    ),
                },
                (left, right) => panic!("wire acceptance diverged: {left:?} {right:?}"),
            }
        }
    });
}

#[test]
fn all_nested_limits_refuse_before_output_allocations_and_keep_actual_input() {
    let encoded = encode_indexed_structural_records(&ordinary_records())
        .expect("actual producer")
        .embedded_index;
    with_work(Duration::from_secs(10), move |work| {
        for category in 0..6 {
            let mut bound = limits();
            match category {
                0 => bound.max_records = 1,
                1 => bound.max_dictionary_entries = 1,
                2 => bound.max_locator_ids = 1,
                3 => bound.max_terms = 1,
                4 => bound.max_fields = 1,
                _ => bound.max_input_bytes = 1,
            }
            let input = Bytes::from(encoded.clone());
            let pointer = input.as_ptr();
            let error = EmbeddedFrameIndex::decode_until(input, 96, bound, work)
                .expect_err("real finite census refusal");
            assert!(matches!(error.cause, Failure::Limits));
            assert_eq!(error.retained.input.as_ptr(), pointer);
            assert_eq!(
                error.retained.usage.bytes, 0,
                "count refusal precedes output allocation"
            );
        }
    });
}

#[test]
fn actual_partial_values_survive_original_expiry_and_unwind() {
    for (panic, phase) in [false, true].into_iter().flat_map(|panic| {
        [
            "column",
            "run",
            "id",
            "membership",
            "term",
            "field",
            "completed",
        ]
        .into_iter()
        .map(move |phase| (panic, phase))
    }) {
        let (wire, count) = if phase == "run" {
            (native_index(512, true), 512)
        } else {
            (
                encode_indexed_structural_records(&ordinary_records())
                    .expect("actual nested index")
                    .embedded_index,
                96,
            )
        };
        with_work(Duration::from_millis(200), move |work| {
            LATE.set(!panic);
            PANIC.set(panic);
            TARGET.set(Some(phase));
            let hooks = Hooks;
            let input = Bytes::from(wire);
            let pointer = input.as_ptr();
            let error = EmbeddedFrameIndex::decode_until(input, count, limits(), work)
                .expect_err("real allocated column before refusal");
            drop(hooks);
            assert_eq!(error.retained.input.as_ptr(), pointer);
            match phase {
                "column" => assert!(!error.retained.layout.values.is_empty()),
                "run" => assert!(
                    error.retained.packed_through > 0 && !error.retained.layout.values.is_empty()
                ),
                "id" => assert!(!error.retained.current_ids.is_empty()),
                "membership" => assert!(error.retained.term_membership.is_some()),
                "term" => assert!(!error.retained.terms.is_empty()),
                "field" => assert!(!error.retained.fields.is_empty()),
                _ => assert!(error.retained.completed.is_some()),
            }
            assert_eq!(error.retained.panic.is_some(), panic);
            assert_eq!(error.retained.completed.is_some(), phase == "completed");
        });
    }
}

#[test]
fn real_output_capacity_one_less_and_same_work_exhaustion_are_retained() {
    let wire = native_index(33, false);
    with_work(Duration::from_secs(10), move |work| {
        let first = EmbeddedFrameIndex::decode_until(Bytes::from(wire.clone()), 33, limits(), work)
            .expect("actual output capacity measured");
        let mut smaller = limits();
        smaller.max_output_bytes = first.retained.usage.bytes - 1;
        let error = EmbeddedFrameIndex::decode_until(Bytes::from(wire.clone()), 33, smaller, work)
            .expect_err("actual output ceiling one less");
        assert!(matches!(error.cause, Failure::Limits));
        assert!(!error.retained.layout.values.is_empty());
        let remaining = work.limits().max_declared_bytes
            - work.usage().expect("actual spent work").declared_bytes;
        work.reserve_values(remaining, 0, 0, 0)
            .expect("same cumulative allowance spent");
        let input = Bytes::from(wire);
        let pointer = input.as_ptr();
        let initial = EmbeddedFrameIndex::decode_until(input, 33, limits(), work)
            .expect_err("no emergency carrier allocation on exhausted work");
        assert!(matches!(
            initial.cause,
            Failure::Refused(NativeResumeRefusal::Capacity)
        ));
        assert_eq!(initial.retained.input.as_ptr(), pointer);
        assert_eq!(initial.retained.usage.bytes, 0);
    });
}

#[test]
fn actual_run_slot_and_cumulative_work_limits_precede_their_real_phase() {
    let wire = native_index(512, true);
    assert_eq!(
        wire[1] & !PACKED_IDS_POSITION_ORDERED,
        PACKED_IDS_RUN_LENGTH,
        "the ordinary encoder really selected RLE"
    );
    with_work(Duration::from_secs(10), move |work| {
        let mut local = limits();
        local.max_runs = 1;
        let error = EmbeddedFrameIndex::decode_until(Bytes::from(wire), 512, local, work)
            .expect_err("actual declared runs exceed finite census ceiling");
        assert!(matches!(error.cause, Failure::Limits));
        assert_eq!(error.retained.usage.bytes, 0);
        let wire = native_index(33, false);
        let actual =
            EmbeddedFrameIndex::decode_until(Bytes::from(wire.clone()), 33, limits(), work)
                .expect("genuine actual phase costs");
        for slots in [false, true] {
            let mut local = limits();
            if slots {
                local.max_allocation_slots = actual.retained.usage.slots - 1;
            } else {
                local.max_work_bytes = actual.retained.usage.work - 1;
            }
            let error =
                EmbeddedFrameIndex::decode_until(Bytes::from(wire.clone()), 33, local, work)
                    .expect_err("one-less actual retained slots/work refuses");
            assert!(matches!(error.cause, Failure::Limits));
            assert!(
                !error.retained.layout.values.is_empty(),
                "real earlier output remains owned"
            );
        }
    });
}

// Reuse the existing actual phase observer for the new consuming recovery chain.
pub(super) fn with_recovery_phase<T>(
    phase: &'static str,
    panic: bool,
    operation: impl FnOnce() -> T,
) -> T {
    LATE.set(!panic);
    PANIC.set(panic);
    TARGET.set(Some(phase));
    let _hooks = Hooks;
    operation()
}
