//! Actual SLW1/SLT1 fixtures using a genuine recovered callback work owner.
//! The harness deliberately refuses the rest of that native resume; it is not
//! an implemented telemetry factory or a successful owned-host fixture.
use super::*;
use shard_stream_core::{
    DirectoryOwnership, LogicalPartitionId, NativeDirectoryBudget, NativeDirectoryLimits, ShardId,
    TopicId, TopicPartition,
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
            "telemetry-bounded-ingest-frames-{}-{now}-{}",
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

fn policy() -> IndexedFramePreparationLimits {
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
        max_work_bytes: 1 << 25,
    }
}

#[test]
fn contextless_recovery_rebuilds_actual_indexes_without_a_transient_sidecar() {
    with_work(Duration::from_secs(30), |work| {
        for context in [
            None,
            Some(Bytes::from_static(b"unrecognized legacy context")),
        ] {
            let sample = original_pack(12, 3);
            let expected =
                decode_indexed_ingest_frames(Bytes::from(sample.payload.clone()), None, 12)
                    .expect("actual ordinary durable decompression");
            let original = Bytes::from(sample.payload);
            let pointer = original.as_ptr();
            let mut limits = policy();
            limits.decompression = Some(decompression_policy());
            let before = work.usage().expect("same original native ledger");
            let result = prepare_indexed_ingest_frames_until(original, context, 12, limits, work)
                .expect("real bounded durable decompression and index decode");
            assert_eq!(result.payload().as_ptr(), pointer);
            compare(&result, &expected);
            assert!(result.frames().iter().all(|frame| {
                frame
                    .decompressed
                    .as_ref()
                    .and_then(|r| r.as_ref().ok())
                    .is_some_and(|d| d.stream_calls() > 1)
            }));
            assert!(
                work.usage().expect("actual cumulative fees").declared_bytes
                    > before.declared_bytes
            );
        }
    });
}

#[test]
fn real_native_decompression_errors_and_finite_limits_retain_partial_state() {
    with_work(Duration::from_secs(30), |work| {
        let sample = original_pack(12, 1);
        let mut cursor = IngestPackCursor::new(&sample.payload, 12).expect("actual wire cursor");
        let group = cursor.next_group().expect("real compressed group");
        let input = Bytes::copy_from_slice(group.compressed);
        let expected = group.structural_bytes;
        let good = decompress_structural_frame_until(
            input.clone(),
            expected,
            12,
            decompression_policy(),
            work,
        )
        .expect("actual streaming result");
        assert!(good.stream_calls() > 1);
        let mut exact = decompression_policy();
        exact.max_stream_calls = good.stream_calls();
        decompress_structural_frame_until(input.clone(), expected, 12, exact, work)
            .expect("exact actual call ceiling");
        exact.max_stream_calls -= 1;
        let short = decompress_structural_frame_until(input.clone(), expected, 12, exact, work)
            .expect_err("one fewer streaming call");
        assert_eq!(short.stream_calls(), exact.max_stream_calls);
        assert!(short.has_native_context());
        assert!(!short.output_prefix().is_empty());

        let mut invalid = input.to_vec();
        invalid[4] |= 0x08; // Reserved descriptor bit, keeping the real complete header.
        let bad = decompress_structural_frame_until(
            Bytes::from(invalid),
            expected,
            12,
            decompression_policy(),
            work,
        )
        .expect_err("genuine native error");
        assert!(bad.native_error().is_some());
        assert!(bad.has_native_context());
        let unknown = decompress_structural_frame_until(
            Bytes::from_static(b"not a zstd frame"),
            expected,
            12,
            decompression_policy(),
            work,
        )
        .expect_err("unknown magic");
        assert_eq!(
            unknown.stream_calls(),
            0,
            "no unknown compressed bytes entered native decode"
        );
        assert!(unknown.native_error().is_none());
        let larger = original_pack(512, 1);
        let mut larger_cursor =
            IngestPackCursor::new(&larger.payload, 512).expect("actual larger modern group");
        let larger_group = larger_cursor
            .next_group()
            .expect("real native window input");
        assert!(larger_group.structural_bytes > 1024);
        let mut small_window = decompression_policy();
        small_window.max_window_log = 10;
        let window = decompress_structural_frame_until(
            Bytes::copy_from_slice(larger_group.compressed),
            larger_group.structural_bytes,
            512,
            small_window,
            work,
        )
        .expect_err("native modern frame exceeds actual configured window");
        assert!(window.native_error().is_some());
        assert!(window.has_native_context());
        // The selected native whole-frame shortcut uses our fixed destination
        // without allocating a buffered window. WindowLogMax is not a strict
        // frame-header admission rule on that path.
        let mut full_output = small_window;
        full_output.input_chunk_bytes = 1 << 20;
        full_output.output_chunk_bytes = 1 << 20;
        let direct = decompress_structural_frame_until(
            Bytes::copy_from_slice(larger_group.compressed),
            larger_group.structural_bytes,
            512,
            full_output,
            work,
        )
        .expect("actual native fixed-destination shortcut");
        assert_eq!(direct.stream_calls(), 1);
        for (bytes, length) in [
            (input.slice(..input.len() - 1), expected),
            (input.clone(), expected - 1),
            (input.clone(), expected + 1),
        ] {
            let error =
                decompress_structural_frame_until(bytes, length, 12, decompression_policy(), work)
                    .expect_err("actual truncated/extent refusal");
            assert!(error.has_native_context());
            assert!(error.stream_calls() > 0);
        }
        let mut low = decompression_policy();
        low.max_decoder_bytes = 1;
        let error = decompress_structural_frame_until(input.clone(), expected, 12, low, work)
            .expect_err("actual context exceeds prepaid native reservation");
        assert!(error.has_native_context());
        assert_eq!(error.stream_calls(), 0);
        low = decompression_policy();
        low.max_output_bytes = expected - 1;
        let error = decompress_structural_frame_until(input, expected, 12, low, work)
            .expect_err("output ceiling before native entry");
        assert!(!error.has_native_context());
        assert_eq!(error.stream_calls(), 0);
    });
}

#[test]
fn actual_decoder_results_and_output_owners_survive_expiry_and_unwind() {
    for phase in [
        "decompression-output",
        "decompression-context",
        "decompression-init",
        "decompression-parameter",
        "decompression-step",
        "decompression-bytes",
        "decompression-section",
    ] {
        for panic in [false, true] {
            with_work(
                Duration::from_secs(if panic { 30 } else { 1 }),
                move |work| {
                    let sample = original_pack(12, 1);
                    let original = Bytes::from(sample.payload);
                    let pointer = original.as_ptr();
                    let mut limits = policy();
                    limits.decompression = Some(decompression_policy());
                    let _hook = Hook;
                    TARGET.set(Some((phase, 0, panic)));
                    let error =
                        prepare_indexed_ingest_frames_until(original, None, 12, limits, work)
                            .expect_err("actual entered native phase then original E/unwind");
                    assert!(OBSERVED.get(), "actual phase reached: {phase}");
                    assert_eq!(error.payload().as_ptr(), pointer);
                    let child = error
                        .retained
                        .current
                        .as_ref()
                        .expect("actual current frame")
                        .decompressed
                        .as_ref()
                        .expect("complete child Result retained")
                        .as_ref()
                        .expect_err("actual failed native child");
                    assert_eq!(child.has_panic(), panic);
                    assert_eq!(child.has_native_context(), phase != "decompression-output");
                    if [
                        "decompression-step",
                        "decompression-bytes",
                        "decompression-section",
                    ]
                    .contains(&phase)
                    {
                        assert!(child.stream_calls() > 0);
                    }
                    if ["decompression-bytes", "decompression-section"].contains(&phase) {
                        assert!(!child.output_prefix().is_empty());
                    }
                },
            );
        }
    }
}

#[test]
fn decompression_exact_and_one_less_work_share_original_native_counters() {
    with_work(Duration::from_secs(30), |work| {
        let sample = original_pack(12, 1);
        let mut cursor = IngestPackCursor::new(&sample.payload, 12).expect("actual wire cursor");
        let group = cursor.next_group().expect("real compressed group");
        let input = Bytes::copy_from_slice(group.compressed);
        let first = decompress_structural_frame_until(
            input.clone(),
            group.structural_bytes,
            12,
            decompression_policy(),
            work,
        )
        .expect("measure actual native fees");
        for short in [0, 1] {
            let before = work.usage().expect("original cumulative counters");
            let mut limits = decompression_policy();
            limits.max_work_bytes = first.work_bytes() - short;
            let actual = decompress_structural_frame_until(
                input.clone(),
                group.structural_bytes,
                12,
                limits,
                work,
            );
            if short == 0 {
                let result = actual.expect("exact measured source-visible work");
                assert_eq!(result.structural(), first.structural());
            } else {
                let error = actual.expect_err("one less source-visible work");
                assert!(error.has_native_context());
                assert!(!error.output_prefix().is_empty());
            }
            assert!(
                work.usage().expect("same ledger after attempt").work_bytes > before.work_bytes
            );
        }
    });
}

#[test]
fn modern_concatenation_and_skippable_frames_keep_native_parity_and_legacy_stays_out() {
    with_work(Duration::from_secs(30), |work| {
        let sample = original_pack(12, 1);
        let mut cursor = IngestPackCursor::new(&sample.payload, 12).expect("actual wire cursor");
        let group = cursor.next_group().expect("real compressed group");
        let original = zstd::bulk::decompress(group.compressed, group.structural_bytes)
            .expect("ordinary native structural output");
        let mid = original.len() / 2;
        let mut concatenated =
            zstd::bulk::compress(&original[..mid], 1).expect("actual first modern frame");
        concatenated
            .extend(zstd::bulk::compress(&original[mid..], 1).expect("actual second modern frame"));
        assert_eq!(
            zstd::bulk::decompress(&concatenated, group.structural_bytes)
                .expect("ordinary concatenated native output"),
            original
        );
        let decoded = decompress_structural_frame_until(
            Bytes::from(concatenated),
            group.structural_bytes,
            12,
            decompression_policy(),
            work,
        )
        .expect("bounded concatenated modern output");
        assert_eq!(
            decoded
                .structural()
                .expect("actual retained bytes")
                .as_ref(),
            original
        );

        let mut skipped = vec![0x50, 0x2A, 0x4D, 0x18, 3, 0, 0, 0, 1, 2, 3];
        skipped.extend_from_slice(group.compressed);
        let decoded = decompress_structural_frame_until(
            Bytes::from(skipped),
            group.structural_bytes,
            12,
            decompression_policy(),
            work,
        )
        .expect("actual native skippable prefix");
        assert_eq!(
            decoded
                .structural()
                .expect("same actual structural bytes")
                .as_ref(),
            original
        );

        let mut legacy_tail = group.compressed.to_vec();
        legacy_tail.extend_from_slice(&[0x27, 0xB5, 0x2F, 0xFD]);
        let refused = decompress_structural_frame_until(
            Bytes::from(legacy_tail),
            group.structural_bytes,
            12,
            decompression_policy(),
            work,
        )
        .expect_err("legacy next frame refused before any legacy native input");
        assert!(matches!(
            refused.cause,
            super::super::bounded_decompression::Failure::Codec(_)
        ));
        assert_eq!(refused.output_prefix(), original);
        assert!(refused.native_error().is_none());
    });
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
fn compare(actual: &PreparedIndexedIngestFrames, expected: &[IndexedIngestFrame]) {
    assert_eq!(actual.frames().len(), expected.len());
    for (i, (frame, original)) in actual.frames().iter().zip(expected).enumerate() {
        assert_eq!(frame.cohort, original.cohort);
        assert_eq!(frame.record_count, original.record_count);
        assert_eq!(frame.structural_bytes, original.structural_bytes);
        assert_eq!(
            frame.min_timestamp_unix_nanos,
            original.min_timestamp_unix_nanos
        );
        assert_eq!(
            frame.max_timestamp_unix_nanos,
            original.max_timestamp_unix_nanos
        );
        assert_eq!(frame.embedded_index(), Some(original.index.as_ref()));
        assert_eq!(actual.compressed(i), Some(original.compressed.as_ref()));
        assert_eq!(frame.checksum, Some(frame.expected_checksum));
    }
}
fn codec_error(error: &RetainedIndexedFrameError) -> Option<String> {
    match &error.cause {
        Failure::Codec(e) => Some(e.to_string()),
        Failure::Index => error
            .current_frame()?
            .index_failure()?
            .source()
            .and_then(|e| e.downcast_ref::<TelemetryError>())
            .map(ToString::to_string),
        _ => None,
    }
}

#[test]
fn ordinary_generated_groups_and_empty_pack_match_exact_typed_native_indexes() {
    let samples = [
        original_pack(0, 1),
        original_pack(1, 1),
        original_pack(96, 3),
    ];
    with_work(Duration::from_secs(30), move |work| {
        for sample in samples {
            let input = Bytes::from(sample.payload);
            let context = Bytes::from(sample.transient_context);
            let pointer = input.as_ptr();
            let context_pointer = context.as_ptr();
            let records = u32_at(&input, 4).expect("actual encoded count");
            let legacy = decode_indexed_ingest_frames(input.clone(), Some(&context), records)
                .expect("actual legacy typed output");
            let before = work.usage().expect("same original ledger");
            let prepared =
                prepare_indexed_ingest_frames_until(input, Some(context), records, policy(), work)
                    .expect("actual bounded group preparation");
            compare(&prepared, &legacy);
            assert_eq!(prepared.payload().as_ptr(), pointer);
            assert_eq!(
                prepared.context().expect("same context").as_ptr(),
                context_pointer
            );
            assert!(
                work.usage()
                    .expect("same ledger after children")
                    .declared_bytes
                    > before.declared_bytes
            );
            let slots = prepared.retained.frames.capacity() * size_of::<PreparedIngestFrame>();
            assert!(prepared.usage().declared_bytes >= slots + prepared.payload().len());
        }
    });
}

#[test]
fn actual_wire_corruptions_preserve_original_group_and_child_codec_failures() {
    let sample = original_pack(12, 3);
    with_work(Duration::from_secs(30), move |work| {
        let payload = sample.payload;
        let context = sample.transient_context;
        let mut cases = Vec::new();
        for at in [0, 4, 8, 10, 12, 20, 24, 28, 32, 36, payload.len() - 1] {
            let mut changed = payload.clone();
            changed[at] ^= 0x80;
            cases.push((changed, context.clone()));
        }
        for at in [4, 6, 8, 16, 20, 24, 28, context.len() - 1] {
            let mut changed = context.clone();
            changed[at] ^= 0x80;
            cases.push((payload.clone(), changed));
        }
        for end in [0, 4, 11, 12, 33, payload.len() - 1] {
            cases.push((payload[..end].to_vec(), context.clone()));
        }
        // Keep SLT1 magic so the ordinary route must use this same actual sidecar.
        for end in [4, 6, 7, 8, 19, context.len() - 1] {
            cases.push((payload.clone(), context[..end].to_vec()));
        }
        let mut trailing = payload.clone();
        trailing.push(0);
        cases.push((trailing, context.clone()));
        let mut trailing = context.clone();
        trailing.push(0);
        cases.push((payload.clone(), trailing));
        for (payload, context) in cases {
            let legacy =
                decode_indexed_ingest_frames(Bytes::from(payload.clone()), Some(&context), 12);
            let mut l = policy();
            l.max_groups = u16::MAX as usize;
            let actual = prepare_indexed_ingest_frames_until(
                Bytes::from(payload),
                Some(Bytes::from(context)),
                12,
                l,
                work,
            );
            match (legacy, actual) {
                (Ok(old), Ok(new)) => compare(&new, &old),
                (Err(old), Err(new)) => {
                    assert_eq!(codec_error(&new), Some(old.to_string()), "{new:?}")
                }
                (old, new) => panic!("ordinary and bounded acceptance differ: {old:?} / {new:?}"),
            }
        }
    });
}

#[test]
fn missing_context_refuses_actual_decompression_but_zero_groups_need_none() {
    let sample = original_pack(4, 2);
    let empty = original_pack(0, 1);
    with_work(Duration::from_secs(30), move |work| {
        for context in [None, Some(Bytes::from_static(b"not SLT1"))] {
            let input = Bytes::from(sample.payload.clone());
            let pointer = input.as_ptr();
            let context_pointer = context.as_ref().map(|bytes| bytes.as_ptr());
            let error = prepare_indexed_ingest_frames_until(input, context, 4, policy(), work)
                .expect_err("no opaque or invented decompression fallback");
            assert!(matches!(error.cause, Failure::DecompressionUnsupported));
            assert_eq!(error.payload().as_ptr(), pointer);
            assert_eq!(error.context().map(|bytes| bytes.as_ptr()), context_pointer);
            assert_eq!(error.retained.frames.capacity(), 0);
            assert!(error.retained.hasher.is_none());
            let zero = prepare_indexed_ingest_frames_until(
                Bytes::from(empty.payload.clone()),
                error.context().cloned(),
                0,
                policy(),
                work,
            )
            .expect("zero groups never enter decompression");
            assert!(zero.frames().is_empty());
        }
    });
}

#[test]
fn finite_count_hash_and_actual_value_capacities_refuse_before_production() {
    let sample = original_pack(12, 3);
    with_work(Duration::from_secs(30), move |work| {
        for kind in [
            "payload", "context", "records", "groups", "checksum", "values",
        ] {
            let mut l = policy();
            match kind {
                "payload" => l.max_payload_bytes = 1,
                "context" => l.max_context_bytes = 1,
                "records" => l.max_records = 1,
                "groups" => l.max_groups = 1,
                "checksum" => l.max_checksum_bytes = 1,
                _ => l.max_declared_bytes = 1,
            }
            let payload = Bytes::from(sample.payload.clone());
            let p = payload.as_ptr();
            let context = Bytes::from(sample.transient_context.clone());
            let c = context.as_ptr();
            let error = prepare_indexed_ingest_frames_until(payload, Some(context), 12, l, work)
                .expect_err("actual finite local ceiling");
            assert!(matches!(error.cause, Failure::Limits));
            assert_eq!(error.payload().as_ptr(), p);
            assert_eq!(error.context().expect("whole context").as_ptr(), c);
            assert!(error.completed_frames().is_empty());
            if kind == "checksum" {
                assert!(error.current_frame().is_some());
                assert_eq!(error.usage().checksum_bytes, 0);
                assert!(error.retained.hasher.is_none());
            } else {
                assert_eq!(error.retained.frames.capacity(), 0);
            }
        }
        // Native generated groups each contain four records. A changed total
        // header still fits the explicit cap and each individual group, while
        // the third actual group crosses the CUMULATIVE record allowance.
        let mut changed_total = sample.payload.clone();
        changed_total[4..8].copy_from_slice(&8_u32.to_le_bytes());
        let mut cumulative = policy();
        cumulative.max_records = 8;
        let crossed = prepare_indexed_ingest_frames_until(
            Bytes::from(changed_total.clone()),
            Some(Bytes::from(sample.transient_context.clone())),
            8,
            cumulative,
            work,
        )
        .expect_err("actual cumulative records refuse before third child output");
        assert!(matches!(crossed.cause, Failure::Limits));
        assert_eq!(crossed.completed_frames().len(), 2);
        assert_eq!(crossed.retained.observed_records, 12);
        assert!(
            crossed
                .current_frame()
                .expect("third actual group retained")
                .index
                .is_none()
        );
        let fitting = prepare_indexed_ingest_frames_until(
            Bytes::from(changed_total),
            Some(Bytes::from(sample.transient_context.clone())),
            8,
            policy(),
            work,
        )
        .expect_err("fitting explicit cap preserves original trailing total error");
        assert_eq!(
            codec_error(&fitting),
            Some(
                TelemetryError::InvalidBlockEncoding("compressed ingest pack totals are invalid")
                    .to_string()
            )
        );
        let baseline = prepare_indexed_ingest_frames_until(
            Bytes::from(sample.payload.clone()),
            Some(Bytes::from(sample.transient_context.clone())),
            12,
            policy(),
            work,
        )
        .expect("measure actual source values");
        let measured = baseline.usage();
        drop(baseline);
        for axis in ["values", "entries", "work"] {
            let mut l = policy();
            match axis {
                "values" => l.max_declared_bytes = measured.declared_bytes - 1,
                "entries" => l.max_entries = measured.entries - 1,
                _ => l.max_work_bytes = measured.work_bytes - 1,
            }
            let error = prepare_indexed_ingest_frames_until(
                Bytes::from(sample.payload.clone()),
                Some(Bytes::from(sample.transient_context.clone())),
                12,
                l,
                work,
            )
            .expect_err("one less actual outer ceiling");
            assert!(matches!(error.cause, Failure::Limits));
            assert!(!error.retained.complete);
            assert!(!error.payload().is_empty());
        }
    });
}

thread_local! {
    static TARGET: Cell<Option<(&'static str, usize, bool)>> = const {Cell::new(None)};
    static OBSERVED: Cell<bool> = const {Cell::new(false)};
}
pub(super) fn after_value(expiry: Instant, phase: &'static str, prefix: usize) {
    let Some((target, completed, panic)) = TARGET.get() else {
        return;
    };
    if phase != target || completed != prefix {
        return;
    }
    TARGET.set(None);
    OBSERVED.set(true);
    if panic {
        panic!("actual grouped-frame observer unwind");
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
fn real_partial_index_results_and_complete_prefixes_survive_expiry_and_unwind() {
    for phase in ["group", "index_input", "index_result", "frame", "complete"] {
        for panic in [false, true] {
            let sample = original_pack(12, 3);
            with_work(
                Duration::from_secs(if panic { 30 } else { 1 }),
                move |work| {
                    let _hook = Hook;
                    let prefix = if phase == "frame" {
                        1
                    } else if phase == "complete" {
                        3
                    } else {
                        1
                    };
                    TARGET.set(Some((phase, prefix, panic)));
                    let payload = Bytes::from(sample.payload);
                    let pointer = payload.as_ptr();
                    let context = Bytes::from(sample.transient_context);
                    let c = context.as_ptr();
                    let error = prepare_indexed_ingest_frames_until(
                        payload,
                        Some(context),
                        12,
                        policy(),
                        work,
                    )
                    .expect_err("no late/panicked group preparation success");
                    assert!(OBSERVED.get(), "actual requested phase was reached");
                    assert_eq!(error.payload().as_ptr(), pointer);
                    assert_eq!(error.context().expect("same context").as_ptr(), c);
                    assert_eq!(error.completed_frames().len(), prefix);
                    if phase == "index_result" {
                        assert!(
                            error
                                .current_frame()
                                .expect("actual current child")
                                .embedded_index()
                                .is_some()
                        );
                    }
                    if phase == "index_input" {
                        assert!(error.retained.index_input.is_some());
                    }
                    if phase == "complete" {
                        assert!(error.retained.complete);
                    }
                    if panic {
                        assert!(matches!(error.cause, Failure::Panic));
                        assert!(error.retained.panic.is_some());
                    } else {
                        assert!(matches!(
                            error.cause,
                            Failure::Refused(NativeResumeRefusal::Expired)
                        ));
                    }
                },
            );
        }
    }
}

#[test]
fn real_hash_prefix_and_original_inputs_survive_expiry_after_one_finite_chunk() {
    let mut state = 0x1234abcd_u64;
    let message = (0..262144)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'!' + (state % 90) as u8)
        })
        .collect::<String>();
    let sample = prepare_ingest_pack(&[crate::OtlpLogEvent {
        message: Arc::from(message),
        timestamp_unix_nanos: 7,
        ..crate::OtlpLogEvent::default()
    }])
    .expect("actual ordinary incompressible group");
    let mut cursor = IngestPackCursor::new(&sample.payload, 1).expect("actual normal header");
    assert!(
        cursor
            .next_group()
            .expect("actual compressed data")
            .compressed
            .len()
            > HASH_CHUNK
    );
    with_work(Duration::from_secs(1), move |work| {
        let _hook = Hook;
        TARGET.set(Some(("checksum_chunk", 0, false)));
        let error = prepare_indexed_ingest_frames_until(
            Bytes::from(sample.payload),
            Some(Bytes::from(sample.transient_context)),
            1,
            policy(),
            work,
        )
        .expect_err("actual finite chunk then original E");
        assert!(OBSERVED.get());
        assert!(matches!(
            error.cause,
            Failure::Refused(NativeResumeRefusal::Expired)
        ));
        assert_eq!(error.usage().checksum_bytes, HASH_CHUNK);
        assert!(error.retained.hasher.is_some());
        assert!(
            error
                .current_frame()
                .expect("actual group facts")
                .checksum
                .is_none()
        );
        assert!(error.completed_frames().is_empty());
    });
}

#[test]
fn actual_child_failure_and_same_cumulative_work_refusal_keep_whole_sources() {
    let sample = original_pack(12, 3);
    with_work(Duration::from_secs(30), move |work| {
        let mut context = sample.transient_context.clone();
        // First actual sidecar index begins after its two native headers.
        context[TRANSIENT_PACK_HEADER_BYTES + TRANSIENT_GROUP_HEADER_BYTES] = 0xff;
        let error = prepare_indexed_ingest_frames_until(
            Bytes::from(sample.payload.clone()),
            Some(Bytes::from(context)),
            12,
            policy(),
            work,
        )
        .expect_err("actual child wire refusal retained");
        assert!(matches!(error.cause, Failure::Index));
        assert!(
            error
                .current_frame()
                .expect("actual child scope")
                .index_failure()
                .is_some()
        );
        assert!(codec_error(&error).is_some());
        drop(error);
        let before = work
            .usage()
            .expect("same ledger before explicit exhaustion");
        // This is actual work admission on the genuine callback, not a new budget.
        work.reserve_values(0, 0, 0, (1 << 30) - before.work_bytes - 1)
            .expect("actual remaining configured callback work");
        let input = Bytes::from(sample.payload);
        let pointer = input.as_ptr();
        let context = Bytes::from(sample.transient_context);
        let c = context.as_ptr();
        let error = prepare_indexed_ingest_frames_until(input, Some(context), 12, policy(), work)
            .expect_err("same exhausted original native allowance");
        assert!(matches!(
            error.cause,
            Failure::Refused(NativeResumeRefusal::Capacity)
        ));
        assert_eq!(error.payload().as_ptr(), pointer);
        assert_eq!(error.context().expect("whole input").as_ptr(), c);
        assert_eq!(error.retained.frames.capacity(), 0);
        assert_eq!(error.usage(), IndexedFramePreparationUsage::default());
        assert!(work.check().is_err());
    });
}
