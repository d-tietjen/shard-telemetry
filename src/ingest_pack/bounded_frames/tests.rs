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

fn recovery_decompression_policy() -> crate::OwnedLogDecompressionLimits {
    let p = decompression_policy();
    crate::OwnedLogDecompressionLimits {
        max_input_bytes: p.max_input_bytes,
        max_output_bytes: p.max_output_bytes,
        input_chunk_bytes: p.input_chunk_bytes,
        output_chunk_bytes: p.output_chunk_bytes,
        max_stream_calls: p.max_stream_calls,
        max_window_log: p.max_window_log,
        max_decoder_bytes: p.max_decoder_bytes,
        max_declared_bytes: p.max_declared_bytes,
        max_entries: p.max_entries,
        max_work_bytes: p.max_work_bytes,
    }
}

fn recovery_decompression_budget_limits() -> crate::TelemetryRecoveryLimits {
    crate::TelemetryRecoveryLimits {
        max_path_bytes: 8192,
        max_file_bytes: 1 << 20,
        max_read_bytes: 1 << 20,
        max_directory_entries: 1024,
        max_records: 1024,
        max_json_values: 4096,
        max_string_bytes: 1 << 20,
        max_retained_bytes: 32 << 20,
        max_work_bytes: 128 << 20,
    }
}

fn persisted_frame_setup_work(
    compressed: &[u8],
    expiry: Instant,
) -> (
    Bytes,
    crate::TelemetryRecoveryBudget,
    crate::TelemetryRecoveryUsage,
) {
    let directory = Directory::new();
    let path = directory.0.join("actual-structural-frame.zst");
    fs::write(&path, compressed).expect("persist actual producer frame");
    let mut original =
        crate::TelemetryRecoveryBudget::new(recovery_decompression_budget_limits(), expiry)
            .expect("one original finite setup ledger before recovery");
    let input = Bytes::from(original.read_file(&path).expect("real admitted frame read"));
    let before = original.usage();
    assert!(before.read_bytes > 0 && before.retained_bytes > 0 && before.work_bytes > 0);
    (input, original, before)
}

#[test]
fn recovery_decoder_continues_on_spent_setup_ledger_with_native_parity() {
    with_work(Duration::from_secs(30), |native_work| {
        let sample = original_pack(12, 1);
        let mut cursor = IngestPackCursor::new(&sample.payload, 12).expect("actual wire cursor");
        let group = cursor.next_group().expect("real original structural frame");
        let native = decompress_structural_frame_until(
            Bytes::copy_from_slice(group.compressed),
            group.structural_bytes,
            12,
            decompression_policy(),
            native_work,
        )
        .expect("original real-native adapter");
        let expiry = Instant::now() + Duration::from_secs(30);
        let (input, original, before) = persisted_frame_setup_work(group.compressed, expiry);
        let first = crate::decompress_recovered_log_frame_until(
            input.clone(),
            group.structural_bytes,
            12,
            recovery_decompression_policy(),
            original,
        )
        .expect("actual already-spent recovery ledger");
        assert_eq!(first.original_expiry(), expiry);
        assert_eq!(
            first.structural_bytes(),
            native.structural().map(Bytes::as_ref)
        );
        assert_eq!(first.stream_calls(), native.stream_calls());
        assert!(first.stream_calls() > 1);
        let used = first.recovery_usage();
        assert!(used.retained_bytes > before.retained_bytes && used.work_bytes > before.work_bytes);
        assert_eq!(used.read_bytes, before.read_bytes);
        assert_eq!(used.records, before.records);
        assert_eq!(used.json_values, before.json_values);
        let mut exact = recovery_decompression_policy();
        exact.max_work_bytes = first.decoder_work_bytes();
        let second = first
            .decompress_next_until(input.clone(), group.structural_bytes, 12, exact)
            .expect("next actual frame uses the SAME spent ledger at exact measured child work");
        assert_eq!(second.original_expiry(), expiry);
        assert_eq!(
            second.structural_bytes(),
            native.structural().map(Bytes::as_ref)
        );
        assert!(second.recovery_usage().work_bytes > used.work_bytes);
        let before_short = second.recovery_usage();
        exact.max_work_bytes -= 1;
        let short = second
            .decompress_next_until(input, group.structural_bytes, 12, exact)
            .expect_err("one less actual child work after two frames on the same ledger");
        assert_eq!(short.original_expiry(), expiry);
        assert!(short.has_native_context());
        assert!(!short.output_prefix().is_empty());
        assert!(short.recovery_usage().work_bytes > before_short.work_bytes);
        assert_eq!(short.recovery_usage().read_bytes, before.read_bytes);
        assert_eq!(short.recovery_usage().records, before.records);
        assert_eq!(short.recovery_usage().json_values, before.json_values);

        // Observe the parent cap from two real decodes, then create each
        // independent original ledger before identical file-read setup. One
        // fixed path prevents path-length drift from changing actual fees.
        let directory = Directory::new();
        let path = directory.0.join("same-cumulative-frame.zst");
        fs::write(&path, group.compressed).expect("persist one fixed actual source");
        let run_two = |max_work_bytes| {
            let mut limits = recovery_decompression_budget_limits();
            limits.max_work_bytes = max_work_bytes;
            let mut original = crate::TelemetryRecoveryBudget::new(limits, expiry)
                .expect("independent original parent ledger before identical recovery");
            let input = Bytes::from(original.read_file(&path).expect("same actual setup read"));
            let setup_usage = original.usage();
            let first = crate::decompress_recovered_log_frame_until(
                input.clone(),
                group.structural_bytes,
                12,
                recovery_decompression_policy(),
                original,
            )
            .expect("first real frame fits each original parent envelope");
            let first_usage = first.recovery_usage();
            let second = first.decompress_next_until(
                input,
                group.structural_bytes,
                12,
                recovery_decompression_policy(),
            );
            (setup_usage, first_usage, second)
        };
        let (observed_setup, observed_first, observed) =
            run_two(recovery_decompression_budget_limits().max_work_bytes);
        let observed = observed.expect("actual two-frame observation with ample child limits");
        let observed_two = observed.recovery_usage();
        assert!(observed_setup.work_bytes > 0);
        assert!(observed_first.work_bytes > observed_setup.work_bytes);
        assert!(observed_two.work_bytes > observed_first.work_bytes);
        let exact_parent = observed_two.work_bytes;
        drop(observed);
        let (exact_setup, exact_first, exact) = run_two(exact_parent);
        let exact = exact.expect("exact observed cumulative parent work succeeds");
        assert_eq!(exact_setup, observed_setup);
        assert_eq!(exact_first, observed_first);
        assert_eq!(exact.recovery_usage(), observed_two);
        assert_eq!(exact.original_expiry(), expiry);
        assert_eq!(
            exact.structural_bytes(),
            native.structural().map(Bytes::as_ref)
        );
        drop(exact);
        let (short_setup, short_first, short) = run_two(exact_parent - 1);
        let short = short.expect_err("one less ORIGINAL parent work refuses the second frame");
        assert_eq!(short_setup, observed_setup);
        assert_eq!(short_first, observed_first);
        assert_eq!(short.original_expiry(), expiry);
        assert!(short.has_native_context());
        assert!(!short.output_prefix().is_empty());
        assert!(short.stream_calls() > 0);
        assert!(short.recovery_usage().work_bytes > short_first.work_bytes);
        assert!(short.recovery_usage().work_bytes < exact_parent);
        assert_eq!(short.recovery_usage().read_bytes, observed_setup.read_bytes);
        assert_eq!(short.recovery_usage().records, observed_setup.records);
        assert_eq!(
            short.recovery_usage().json_values,
            observed_setup.json_values
        );
        let mut source = std::error::Error::source(&short);
        let mut parent_limits = false;
        while let Some(error) = source {
            parent_limits |= matches!(
                error.downcast_ref::<crate::TelemetryRecoveryError>(),
                Some(crate::TelemetryRecoveryError::Limits)
            );
            source = error.source();
        }
        assert!(
            parent_limits,
            "actual original parent Limits remains in the error chain"
        );
    });
}

#[test]
fn recovery_decoder_limits_and_native_errors_retain_original_ledger() {
    let sample = original_pack(12, 1);
    let mut cursor = IngestPackCursor::new(&sample.payload, 12).expect("actual wire cursor");
    let group = cursor.next_group().expect("actual source frame");
    for case in 0..3 {
        let expiry = Instant::now() + Duration::from_secs(30);
        let (input, original, before) = persisted_frame_setup_work(group.compressed, expiry);
        let mut limits = recovery_decompression_policy();
        let input = match case {
            0 => {
                let mut corrupted = input.to_vec();
                corrupted[4..].fill(0xFF);
                Bytes::from(corrupted)
            }
            1 => {
                limits.max_decoder_bytes = 1;
                input
            }
            _ => {
                limits.max_output_bytes = 1;
                input
            }
        };
        let error = crate::decompress_recovered_log_frame_until(
            input,
            group.structural_bytes,
            12,
            limits,
            original,
        )
        .expect_err("genuine native error or exact finite child refusal");
        assert_eq!(error.original_expiry(), expiry);
        assert_eq!(error.recovery_usage().read_bytes, before.read_bytes);
        assert_eq!(error.recovery_usage().records, before.records);
        if case == 0 {
            assert!(error.native_error().is_some());
            assert!(error.stream_calls() > 0 && error.has_native_context());
        } else if case == 1 {
            assert!(error.has_native_context());
            assert_eq!(error.stream_calls(), 0);
            assert!(error.recovery_usage().work_bytes > before.work_bytes);
        } else {
            assert!(!error.has_native_context());
            assert_eq!(error.stream_calls(), 0);
            assert_eq!(error.recovery_usage(), before);
        }
    }
}

#[test]
fn recovery_decoder_late_and_panic_keep_actual_native_outcomes() {
    let sample = original_pack(12, 1);
    let mut cursor = IngestPackCursor::new(&sample.payload, 12).expect("actual wire cursor");
    let group = cursor.next_group().expect("actual producer frame");
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
            let expiry = Instant::now() + Duration::from_secs(if panic { 30 } else { 1 });
            let (input, original, before) = persisted_frame_setup_work(group.compressed, expiry);
            let _hook = Hook;
            TARGET.set(Some((phase, 0, panic)));
            let error = crate::decompress_recovered_log_frame_until(
                input,
                group.structural_bytes,
                12,
                recovery_decompression_policy(),
                original,
            )
            .expect_err("actual entered codec result followed by original E or unwind");
            assert!(OBSERVED.get(), "actual phase reached: {phase}");
            assert_eq!(error.original_expiry(), expiry);
            assert_eq!(error.has_panic(), panic);
            assert_eq!(error.has_native_context(), phase != "decompression-output");
            assert!(error.recovery_usage().work_bytes > before.work_bytes);
            assert_eq!(error.recovery_usage().records, before.records);
            assert_eq!(error.recovery_usage().json_values, before.json_values);
            if [
                "decompression-step",
                "decompression-bytes",
                "decompression-section",
            ]
            .contains(&phase)
            {
                assert!(error.stream_calls() > 0);
            }
            if ["decompression-bytes", "decompression-section"].contains(&phase) {
                assert!(!error.output_prefix().is_empty());
            }
        }
    }
}

fn structural_section_limits() -> crate::RecoveredStructuralSectionLimits {
    crate::RecoveredStructuralSectionLimits {
        max_input_bytes: 1 << 20,
        max_records: 1 << 16,
        max_section_bytes: 1 << 20,
        max_declared_bytes: 1 << 20,
        max_entries: 1024,
        max_work_bytes: 1 << 22,
    }
}

const STRUCTURAL_SECTION_KINDS: [crate::RecoveredStructuralSection; 8] = [
    crate::RecoveredStructuralSection::Offsets,
    crate::RecoveredStructuralSection::Timestamps,
    crate::RecoveredStructuralSection::Templates,
    crate::RecoveredStructuralSection::Bodies,
    crate::RecoveredStructuralSection::Attributes,
    crate::RecoveredStructuralSection::Fields,
    crate::RecoveredStructuralSection::TypedMetadata,
    crate::RecoveredStructuralSection::EmbeddedIndex,
];

fn append_section_extent(output: &mut Vec<u8>, mut value: usize) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        output.push(byte | if value == 0 { 0 } else { 0x80 });
        if value == 0 {
            break;
        }
    }
}

#[test]
fn recovered_sections_match_exact_wire_views_and_keep_completed_native_parent() {
    let typed = crate::OtlpLogEvent {
        timestamp_unix_nanos: 19,
        observed_timestamp_unix_nanos: 27,
        fields: Arc::new(vec![crate::MetadataField::new("tenant", "a")]),
        message: Arc::from("original full typed record"),
        body: Some(crate::TelemetryValue::Array(Arc::new(vec![
            crate::TelemetryValue::Integer(-7),
            crate::TelemetryValue::DoubleBits(0x7ff8_0000_0000_0042),
        ]))),
        attributes: Arc::new(vec![crate::TelemetryAttribute::new(
            "nested",
            crate::TelemetryValue::Map(Arc::new(vec![crate::TelemetryAttribute::new(
                "preserved",
                crate::TelemetryValue::Boolean(true),
            )])),
        )]),
        resource: Arc::new(crate::ResourceContext {
            schema_url: Arc::from("https://example.test/resource"),
            ..crate::ResourceContext::default()
        }),
        scope: Arc::new(crate::ScopeContext {
            name: Arc::from("actual.section.fixture"),
            version: Arc::from("1.0"),
            ..crate::ScopeContext::default()
        }),
        severity_number: 17,
        severity_text: Arc::from("ERROR"),
        dropped_attributes_count: 3,
        flags: 1,
        trace_id: Some(crate::TraceId::from_bytes([1; 16]).expect("real trace ID")),
        span_id: Some(crate::SpanId::from_bytes([2; 8]).expect("real span ID")),
        event_name: Arc::from("original.event"),
        ..crate::OtlpLogEvent::default()
    };
    let events = vec![crate::OtlpLogEvent::default(), typed];
    let sample = prepare_ingest_pack(&events).expect("actual ordinary typed producer");
    let mut cursor = IngestPackCursor::new(&sample.payload, 2).expect("actual wire cursor");
    let group = cursor.next_group().expect("actual typed structural frame");
    let record_count = u32::try_from(group.record_count).expect("actual frame count fits u32");
    let expiry = Instant::now() + Duration::from_secs(30);
    let (input, original, before) = persisted_frame_setup_work(group.compressed, expiry);
    let parent = crate::decompress_recovered_log_frame_until(
        input,
        group.structural_bytes,
        record_count,
        recovery_decompression_policy(),
        original,
    )
    .expect("actual complete native decoder");
    let bytes = parent
        .structural_bytes()
        .expect("actual structural bytes")
        .to_vec();
    let pointer = parent.structural_bytes().expect("same allocation").as_ptr();
    let expected = crate::decode_structural_block(&bytes).expect("ordinary full typed oracle");
    assert_eq!(expected[1].body, events[1].body);
    assert_eq!(expected[1].attributes, events[1].attributes);
    assert_eq!(expected[1].resource, events[1].resource);
    assert_eq!(expected[1].scope, events[1].scope);
    assert_eq!(expected[1].trace_id, events[1].trace_id);
    assert_eq!(expected[1].span_id, events[1].span_id);
    let parent_usage = parent.recovery_usage();
    let parent_calls = parent.stream_calls();
    let sections = parent
        .prepare_sections_until(structural_section_limits())
        .expect("actual charged section continuation");
    assert_eq!(sections.record_count(), record_count);
    assert_eq!(sections.original_expiry(), expiry);
    assert_eq!(
        sections
            .structural_bytes()
            .expect("retained exact output")
            .as_ptr(),
        pointer
    );
    assert!(sections.parent_has_native_context());
    assert_eq!(sections.parent_stream_calls(), parent_calls);
    assert_eq!(sections.usage().sections, 8);
    assert!(sections.usage().header_bytes >= 9);
    assert!(sections.recovery_usage().work_bytes > parent_usage.work_bytes);
    assert_eq!(sections.recovery_usage().read_bytes, before.read_bytes);
    assert_eq!(sections.recovery_usage().records, before.records);
    assert_eq!(sections.recovery_usage().json_values, before.json_values);
    let mut reconstructed = crate::structural::STRUCTURAL_BLOCK_MAGIC.to_vec();
    append_section_extent(&mut reconstructed, sections.record_count() as usize);
    for kind in STRUCTURAL_SECTION_KINDS {
        let view = sections
            .section_bytes(kind)
            .expect("actual complete section fact");
        assert!(view.as_ptr() >= pointer);
        assert!((view.as_ptr() as usize) + view.len() <= (pointer as usize) + bytes.len());
        append_section_extent(&mut reconstructed, view.len());
        reconstructed.extend_from_slice(view);
    }
    assert!(
        !sections
            .section_bytes(crate::RecoveredStructuralSection::TypedMetadata)
            .expect("full typed metadata remains encoded")
            .is_empty()
    );
    assert_eq!(
        reconstructed, bytes,
        "views reconstruct the exact original canonical wire"
    );
    assert_eq!(
        crate::decode_structural_block(&reconstructed).expect("ordinary typed decode from views"),
        expected,
        "every typed value and field survives the retained views without projection"
    );
    let after_sections = sections.recovery_usage();
    let next = sections
        .decompress_next_until(
            Bytes::copy_from_slice(group.compressed),
            group.structural_bytes,
            record_count,
            recovery_decompression_policy(),
        )
        .expect("completed sections continue to another actual frame on same original work");
    assert_eq!(next.original_expiry(), expiry);
    assert!(next.recovery_usage().work_bytes > after_sections.work_bytes);
    assert_eq!(next.recovery_usage().read_bytes, before.read_bytes);
    assert_eq!(next.structural_bytes(), Some(bytes.as_slice()));
}

#[test]
fn recovered_sections_limits_and_cumulative_parent_work_retain_real_child() {
    let sample = original_pack(12, 1);
    let mut cursor = IngestPackCursor::new(&sample.payload, 12).expect("actual wire cursor");
    let group = cursor.next_group().expect("actual structural source");
    let directory = Directory::new();
    let path = directory.0.join("same-section-parent-source.zst");
    fs::write(&path, group.compressed).expect("one fixed original source");
    let expiry = Instant::now() + Duration::from_secs(30);
    let run = |parent_work, child_limits| {
        let mut limits = recovery_decompression_budget_limits();
        limits.max_work_bytes = parent_work;
        let mut original = crate::TelemetryRecoveryBudget::new(limits, expiry)
            .expect("independent original ledger before the same setup");
        let input = Bytes::from(original.read_file(&path).expect("same physical setup"));
        let setup_usage = original.usage();
        let parent = crate::decompress_recovered_log_frame_until(
            input,
            group.structural_bytes,
            12,
            recovery_decompression_policy(),
            original,
        )
        .expect("the actual parent fits every initial envelope");
        let parent_usage = parent.recovery_usage();
        (
            setup_usage,
            parent_usage,
            parent.prepare_sections_until(child_limits),
        )
    };
    let ample_parent = recovery_decompression_budget_limits().max_work_bytes;
    let (setup, parent, observed) = run(ample_parent, structural_section_limits());
    let observed = observed.expect("actual complete section observation");
    let observed_usage = observed.recovery_usage();
    let child_work = observed.usage().work_bytes;
    let largest = STRUCTURAL_SECTION_KINDS
        .iter()
        .map(|kind| {
            observed
                .section_bytes(*kind)
                .expect("actual observed section")
                .len()
        })
        .max()
        .expect("eight real sections");
    drop(observed);
    let mut exact_child = structural_section_limits();
    exact_child.max_work_bytes = child_work;
    exact_child.max_section_bytes = largest;
    exact_child.max_records = 12;
    let (exact_setup, exact_parent, exact) = run(ample_parent, exact_child);
    let exact = exact.expect("actual exact child work/section/count ceilings");
    assert_eq!(exact_setup, setup);
    assert_eq!(exact_parent, parent);
    assert_eq!(exact.recovery_usage(), observed_usage);
    drop(exact);
    for case in 0..3 {
        let mut limits = exact_child;
        match case {
            0 => limits.max_work_bytes -= 1,
            1 => limits.max_section_bytes -= 1,
            _ => limits.max_records -= 1,
        }
        let (_, same_parent, refused) = run(ample_parent, limits);
        let refused = refused.expect_err("one less actual child bound");
        assert_eq!(same_parent, parent);
        assert!(refused.parent_has_native_context());
        assert!(refused.structural_bytes().is_some());
        assert!(refused.child_entered());
        assert!(!refused.child_succeeded());
    }
    let (exact_setup, exact_parent, exact) =
        run(observed_usage.work_bytes, structural_section_limits());
    let exact =
        exact.expect("exact cumulative ORIGINAL work includes setup/native parent/sections");
    assert_eq!(exact_setup, setup);
    assert_eq!(exact_parent, parent);
    assert_eq!(exact.recovery_usage(), observed_usage);
    drop(exact);
    let (short_setup, short_parent, short) =
        run(observed_usage.work_bytes - 1, structural_section_limits());
    let short = short.expect_err("one less cumulative ORIGINAL work refuses actual section child");
    assert_eq!(short_setup, setup);
    assert_eq!(short_parent, parent);
    assert_eq!(short.original_expiry(), expiry);
    assert!(short.parent_has_native_context());
    assert!(
        !short
            .structural_bytes()
            .expect("complete original output")
            .is_empty()
    );
    assert!(short.usage().sections > 0 && short.usage().sections < 8);
    assert!(short.recovery_usage().work_bytes > parent.work_bytes);
    let mut source = std::error::Error::source(&short);
    let mut actual_limits = false;
    while let Some(error) = source {
        actual_limits |= matches!(
            error.downcast_ref::<crate::TelemetryRecoveryError>(),
            Some(crate::TelemetryRecoveryError::Limits)
        );
        source = error.source();
    }
    assert!(
        actual_limits,
        "actual parent budget refusal remains in the child source chain"
    );
}

#[test]
fn recovered_sections_expiry_and_unwind_keep_parent_and_actual_child_result() {
    let sample = original_pack(12, 1);
    let mut cursor = IngestPackCursor::new(&sample.payload, 12).expect("actual wire cursor");
    let group = cursor.next_group().expect("actual structural source");
    for phase in [
        "structural-record-count",
        "structural-section",
        "structural-child-result",
    ] {
        for panic in [false, true] {
            let expiry = Instant::now() + Duration::from_secs(if panic { 30 } else { 1 });
            let (input, original, _) = persisted_frame_setup_work(group.compressed, expiry);
            let parent = crate::decompress_recovered_log_frame_until(
                input,
                group.structural_bytes,
                12,
                recovery_decompression_policy(),
                original,
            )
            .expect("complete actual outer decoder before child entry");
            let pointer = parent.structural_bytes().expect("actual output").as_ptr();
            let before = parent.recovery_usage();
            let _hook = Hook;
            TARGET.set(Some((phase, 0, panic)));
            let refused = parent
                .prepare_sections_until(structural_section_limits())
                .expect_err("actual captured child phase before original E or unwind");
            assert!(OBSERVED.get());
            assert_eq!(refused.has_panic(), panic);
            assert_eq!(refused.original_expiry(), expiry);
            assert_eq!(
                refused
                    .structural_bytes()
                    .expect("retained complete parent")
                    .as_ptr(),
                pointer
            );
            assert!(refused.parent_has_native_context());
            assert!(refused.parent_stream_calls() > 0);
            assert!(refused.recovery_usage().work_bytes > before.work_bytes);
            assert_eq!(
                refused.child_succeeded(),
                phase == "structural-child-result"
            );
            if phase == "structural-child-result" {
                assert_eq!(refused.usage().sections, 8);
                assert!(
                    refused
                        .section_bytes(crate::RecoveredStructuralSection::EmbeddedIndex)
                        .is_some()
                );
            }
            if !panic {
                assert!(matches!(
                    refused.post_error(),
                    Some(crate::TelemetryRecoveryError::Expired)
                ));
            }
        }
    }
    let expiry = Instant::now() + Duration::from_secs(1);
    let (input, original, _) = persisted_frame_setup_work(group.compressed, expiry);
    let parent = crate::decompress_recovered_log_frame_until(
        input,
        group.structural_bytes,
        12,
        recovery_decompression_policy(),
        original,
    )
    .expect("complete actual parent before E");
    let before = parent.recovery_usage();
    while Instant::now() < expiry {
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let refused = parent
        .prepare_sections_until(structural_section_limits())
        .expect_err("pre-entry original E does not start a replacement child");
    assert!(!refused.child_entered());
    assert_eq!(refused.recovery_usage(), before);
    assert_eq!(refused.usage().sections, 0);
    assert!(refused.parent_has_native_context());
}

#[test]
fn malformed_structural_wire_stays_a_real_parent_decoder_refusal() {
    let sample = original_pack(12, 1);
    let mut cursor = IngestPackCursor::new(&sample.payload, 12).expect("actual wire cursor");
    let group = cursor.next_group().expect("actual structural source");
    let raw = zstd::bulk::decompress(group.compressed, group.structural_bytes)
        .expect("independent ordinary original frame oracle");
    for case in 0..3 {
        let mut malformed = raw.clone();
        match case {
            0 => malformed[0] ^= 0x80,
            1 => malformed[4] = 13,
            _ => {
                malformed.pop();
            }
        }
        let compressed =
            zstd::bulk::compress(&malformed, 1).expect("actual malformed frame encoding");
        let expiry = Instant::now() + Duration::from_secs(30);
        let (input, original, before) = persisted_frame_setup_work(&compressed, expiry);
        let parent_error = crate::decompress_recovered_log_frame_until(
            input,
            malformed.len(),
            12,
            recovery_decompression_policy(),
            original,
        )
        .expect_err("real outer framing/count/section refusal; no fabricated successful parent");
        assert!(parent_error.has_native_context());
        assert!(parent_error.stream_calls() > 0);
        assert_eq!(parent_error.output_prefix(), malformed);
        assert_eq!(parent_error.original_expiry(), expiry);
        assert!(parent_error.recovery_usage().work_bytes > before.work_bytes);
        assert!(parent_error.native_error().is_none());
    }
}

fn structural_offset_limits() -> crate::RecoveredStructuralOffsetLimits {
    crate::RecoveredStructuralOffsetLimits {
        max_input_bytes: 1 << 20,
        max_records: 1 << 16,
        max_declared_bytes: 1 << 20,
        max_entries: 1 << 20,
        max_work_bytes: 1 << 22,
    }
}

fn actual_offset_frame(offsets: &[u64]) -> (Vec<u8>, Vec<u8>, u32) {
    let records = offsets
        .iter()
        .enumerate()
        .map(|(ordinal, offset)| {
            let mut record = crate::DurableLog::new(
                ShardId::new(7),
                partition(),
                shard_stream_core::LogicalOffset::new(*offset),
                19 + ordinal as u64,
                "actual retained offset fixture",
                CompressionCohortId::new(4),
            )
            .with_field("original", "preserved");
            record.body = Some(crate::TelemetryValue::Array(Arc::new(vec![
                crate::TelemetryValue::Integer(-7),
                crate::TelemetryValue::DoubleBits(0x7ff8_0000_0000_0042),
            ])));
            record.event_name = Arc::from("actual.offset.typed.metadata");
            record
        })
        .collect::<Vec<_>>();
    let raw = crate::encode_structural_block(&records)
        .expect("actual canonical offset/Pco/metadata producer");
    let compressed = zstd::bulk::compress(&raw, 1).expect("actual outer frame producer");
    (
        raw,
        compressed,
        u32::try_from(records.len()).expect("actual finite record count"),
    )
}

fn actual_offset_sections(
    compressed: &[u8],
    expected_bytes: usize,
    expected_records: u32,
    expiry: Instant,
) -> crate::PreparedRecoveredStructuralSections {
    let (input, original, _) = persisted_frame_setup_work(compressed, expiry);
    crate::decompress_recovered_log_frame_until(
        input,
        expected_bytes,
        expected_records,
        recovery_decompression_policy(),
        original,
    )
    .expect("actual completed native parent")
    .prepare_sections_until(structural_section_limits())
    .expect("actual completed charged section parent")
}

#[test]
fn recovered_offsets_match_ordinary_rows_and_keep_complete_section_parent() {
    for values in [vec![0, 0, 127, 128, 16_384, u64::MAX], vec![u64::MAX]] {
        let (raw, compressed, count) = actual_offset_frame(&values);
        let ordinary =
            crate::decode_structural_block(&raw).expect("actual full ordinary typed-row oracle");
        let expected = ordinary.iter().map(|row| row.offset).collect::<Vec<_>>();
        assert_eq!(
            expected
                .iter()
                .map(|offset| offset.get())
                .collect::<Vec<_>>(),
            values
        );
        let expiry = Instant::now() + Duration::from_secs(30);
        let sections = actual_offset_sections(&compressed, raw.len(), count, expiry);
        let pointer = sections
            .structural_bytes()
            .expect("complete original output")
            .as_ptr();
        let before = sections.recovery_usage();
        let section_usage = sections.usage();
        let calls = sections.parent_stream_calls();
        let offsets = sections
            .prepare_offsets_until(structural_offset_limits())
            .expect("actual typed offset continuation");
        assert_eq!(offsets.offsets(), expected);
        assert_eq!(offsets.usage().decoded_records, values.len());
        assert_eq!(offsets.parent_sections().usage(), section_usage);
        assert!(
            !offsets
                .parent_sections()
                .section_bytes(crate::RecoveredStructuralSection::Timestamps)
                .expect("actual Pco timestamp payload remains encoded")
                .is_empty()
        );
        assert!(
            !offsets
                .parent_sections()
                .section_bytes(crate::RecoveredStructuralSection::TypedMetadata)
                .expect("actual compressed typed metadata remains encoded")
                .is_empty()
        );
        assert_eq!(
            crate::decode_structural_block(
                offsets
                    .parent_sections()
                    .structural_bytes()
                    .expect("retained complete typed wire")
            )
            .expect("ordinary complete typed differential oracle"),
            ordinary
        );

        assert_eq!(
            offsets.parent_sections().structural_bytes(),
            Some(raw.as_slice())
        );
        assert_eq!(
            offsets
                .parent_sections()
                .structural_bytes()
                .expect("same original allocation")
                .as_ptr(),
            pointer
        );
        assert!(offsets.parent_sections().parent_has_native_context());
        assert_eq!(offsets.parent_sections().parent_stream_calls(), calls);
        assert_eq!(offsets.original_expiry(), expiry);
        assert!(offsets.recovery_usage().work_bytes > before.work_bytes);
        assert_eq!(offsets.recovery_usage().read_bytes, before.read_bytes);
        assert_eq!(offsets.recovery_usage().json_values, before.json_values);
        assert_eq!(offsets.recovery_usage().records, before.records);
        let after = offsets.recovery_usage();
        let next = offsets
            .decompress_next_until(
                Bytes::from(compressed),
                raw.len(),
                count,
                recovery_decompression_policy(),
            )
            .expect("same original spent ledger continues after both completed children");
        assert_eq!(next.original_expiry(), expiry);
        assert!(next.recovery_usage().work_bytes > after.work_bytes);
        assert_eq!(next.recovery_usage().read_bytes, before.read_bytes);
        assert_eq!(next.structural_bytes(), Some(raw.as_slice()));
    }
}

#[test]
fn recovered_offsets_limits_and_original_work_keep_actual_partial_rows() {
    let values = (0..12).map(|index| 127 + index * 129).collect::<Vec<_>>();
    let (raw, compressed, count) = actual_offset_frame(&values);
    let directory = Directory::new();
    let path = directory.0.join("same-offset-parent-source.zst");
    fs::write(&path, &compressed).expect("one fixed actual source before independent ledgers");
    let expiry = Instant::now() + Duration::from_secs(30);
    let run = |parent_work, child_limits| {
        let mut limits = recovery_decompression_budget_limits();
        limits.max_work_bytes = parent_work;
        let mut original = crate::TelemetryRecoveryBudget::new(limits, expiry)
            .expect("independent ORIGINAL ledger before identical real setup");
        let input = Bytes::from(
            original
                .read_file(&path)
                .expect("same admitted physical read"),
        );
        let setup = original.usage();
        let sections = crate::decompress_recovered_log_frame_until(
            input,
            raw.len(),
            count,
            recovery_decompression_policy(),
            original,
        )
        .expect("actual native parent under all independent original envelopes")
        .prepare_sections_until(structural_section_limits())
        .expect("actual complete same-ledger section parent");
        let parent = sections.recovery_usage();
        let section_usage = sections.usage();
        (
            setup,
            parent,
            section_usage,
            sections.prepare_offsets_until(child_limits),
        )
    };
    let ample = recovery_decompression_budget_limits().max_work_bytes;
    let (setup, parent, section_usage, observed) = run(ample, structural_offset_limits());
    let observed = observed.expect("actual observed complete child");
    let usage = observed.usage();
    let cumulative = observed.recovery_usage();
    let lane_bytes = observed
        .parent_sections()
        .section_bytes(crate::RecoveredStructuralSection::Offsets)
        .expect("actual offset lane")
        .len();
    drop(observed);
    let exact_child = crate::RecoveredStructuralOffsetLimits {
        max_input_bytes: lane_bytes,
        max_records: values.len(),
        max_declared_bytes: usage.declared_bytes,
        max_entries: usage.entries,
        max_work_bytes: usage.work_bytes,
    };
    let (same_setup, same_parent, _, exact) = run(ample, exact_child);
    let exact = exact.expect("all actual observed child boundaries fit exactly");
    assert_eq!(same_setup, setup);
    assert_eq!(same_parent, parent);
    assert_eq!(exact.usage(), usage);
    assert_eq!(exact.recovery_usage(), cumulative);
    drop(exact);
    for axis in 0..5 {
        let mut limits = exact_child;
        match axis {
            0 => limits.max_input_bytes -= 1,
            1 => limits.max_records -= 1,
            2 => limits.max_declared_bytes -= 1,
            3 => limits.max_entries -= 1,
            _ => limits.max_work_bytes -= 1,
        }
        let (same_setup, same_parent, _, refused) = run(ample, limits);
        let refused = refused.expect_err("one less actual child axis");
        assert_eq!(same_setup, setup);
        assert_eq!(same_parent, parent);
        assert_eq!(refused.parent_sections().usage(), section_usage);
        assert_eq!(
            refused.parent_sections().structural_bytes(),
            Some(raw.as_slice())
        );
        assert!(refused.parent_sections().parent_has_native_context());
        assert!(!refused.child_succeeded());
    }
    let (same_setup, same_parent, _, exact) =
        run(cumulative.work_bytes, structural_offset_limits());
    let exact = exact.expect("exact observed cumulative ORIGINAL work");
    assert_eq!(same_setup, setup);
    assert_eq!(same_parent, parent);
    assert_eq!(exact.recovery_usage(), cumulative);
    drop(exact);
    let (same_setup, same_parent, _, refused) =
        run(cumulative.work_bytes - 1, structural_offset_limits());
    let refused =
        refused.expect_err("one less ORIGINAL cumulative work refuses the actual final row");
    assert_eq!(same_setup, setup);
    assert_eq!(same_parent, parent);
    assert_eq!(refused.parent_sections().usage(), section_usage);
    assert_eq!(refused.original_expiry(), expiry);
    assert_eq!(refused.usage().decoded_records, values.len() - 1);
    assert_eq!(
        refused
            .offsets()
            .iter()
            .map(|offset| offset.get())
            .collect::<Vec<_>>(),
        values[..values.len() - 1]
    );
    assert!(refused.allocation_succeeded());
    assert!(refused.recovery_usage().work_bytes > parent.work_bytes);
    assert_eq!(
        refused.parent_sections().structural_bytes(),
        Some(raw.as_slice())
    );
    let mut source = std::error::Error::source(&refused);
    let mut original_limits = false;
    while let Some(error) = source {
        original_limits |= matches!(
            error.downcast_ref::<crate::TelemetryRecoveryError>(),
            Some(crate::TelemetryRecoveryError::Limits)
        );
        source = error.source();
    }
    assert!(
        original_limits,
        "actual original budget refusal remains in the child source chain"
    );
}

#[test]
fn recovered_offsets_expiry_and_unwind_retain_allocation_and_actual_child_result() {
    let (raw, compressed, count) = actual_offset_frame(&[1, 2, 130]);
    for phase in ["offset-allocation", "offset-row", "structural-child-result"] {
        for panic in [false, true] {
            let expiry = Instant::now() + Duration::from_secs(if panic { 30 } else { 1 });
            let sections = actual_offset_sections(&compressed, raw.len(), count, expiry);
            let before = sections.recovery_usage();
            let section_usage = sections.usage();
            let pointer = sections.structural_bytes().expect("actual output").as_ptr();
            let _hook = Hook;
            TARGET.set(Some((phase, 0, panic)));
            let refused = sections
                .prepare_offsets_until(structural_offset_limits())
                .expect_err("actual child/allocator outcome before original E or unwind");
            assert!(OBSERVED.get());
            assert!(refused.child_entered());
            assert!(refused.allocation_entered());
            assert!(refused.allocation_succeeded());
            assert_eq!(refused.has_panic(), panic);
            assert_eq!(
                refused.child_succeeded(),
                phase == "structural-child-result"
            );
            assert_eq!(
                refused.usage().decoded_records,
                match phase {
                    "offset-allocation" => 0,
                    "offset-row" => 1,
                    _ => 3,
                }
            );
            assert_eq!(refused.parent_sections().usage(), section_usage);
            assert_eq!(
                refused
                    .parent_sections()
                    .structural_bytes()
                    .expect("same full output")
                    .as_ptr(),
                pointer
            );
            assert!(refused.parent_sections().parent_has_native_context());
            assert!(refused.parent_sections().parent_stream_calls() > 0);
            assert_eq!(refused.original_expiry(), expiry);
            assert!(refused.recovery_usage().work_bytes > before.work_bytes);
            if !panic {
                assert!(matches!(
                    refused.post_error(),
                    Some(crate::TelemetryRecoveryError::Expired)
                ));
            }
        }
    }
    let expiry = Instant::now() + Duration::from_secs(1);
    let sections = actual_offset_sections(&compressed, raw.len(), count, expiry);
    let before = sections.recovery_usage();
    let section_usage = sections.usage();
    while Instant::now() < expiry {
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let refused = sections
        .prepare_offsets_until(structural_offset_limits())
        .expect_err("expired original work never enters the new child");
    assert!(!refused.child_entered());
    assert!(!refused.allocation_entered());
    assert_eq!(refused.recovery_usage(), before);
    assert_eq!(
        refused.usage(),
        crate::RecoveredStructuralOffsetUsage::default()
    );
    assert_eq!(refused.parent_sections().usage(), section_usage);
    assert_eq!(
        refused.parent_sections().structural_bytes(),
        Some(raw.as_slice())
    );
}

#[test]
fn malformed_offset_lanes_keep_real_successful_outer_decoder() {
    let (raw, compressed, count) = actual_offset_frame(&[0, 1, 2]);
    let expiry = Instant::now() + Duration::from_secs(30);
    let original = actual_offset_sections(&compressed, raw.len(), count, expiry);
    let mut lanes = STRUCTURAL_SECTION_KINDS
        .iter()
        .map(|kind| {
            original
                .section_bytes(*kind)
                .expect("actual original encoded lane")
                .to_vec()
        })
        .collect::<Vec<_>>();
    let canonical_offsets = lanes[0].clone();
    drop(original);
    let mut maximum = vec![0xff; 9];
    maximum.push(1);
    maximum.extend([1, 0]);
    let mut overflow = vec![0xff; 9];
    overflow.extend([2, 0, 0]);
    let mut trailing = canonical_offsets;
    trailing.push(0);
    for (lane, prefix) in [
        (vec![0, 1, 0x80], vec![0, 1]),
        (maximum, vec![u64::MAX]),
        (vec![0x80; 11], vec![]),
        (overflow, vec![]),
        (trailing, vec![0, 1, 2]),
    ] {
        lanes[0] = lane;
        let mut malformed = crate::structural::STRUCTURAL_BLOCK_MAGIC.to_vec();
        append_section_extent(
            &mut malformed,
            usize::try_from(count).expect("finite count"),
        );
        for lane in &lanes {
            append_section_extent(&mut malformed, lane.len());
            malformed.extend_from_slice(lane);
        }
        assert!(
            crate::decode_structural_block(&malformed).is_err(),
            "ordinary real typed decoder rejects this lane"
        );
        let compressed =
            zstd::bulk::compress(&malformed, 1).expect("real malformed offset lane frame");
        let sections = actual_offset_sections(&compressed, malformed.len(), count, expiry);
        let before = sections.recovery_usage();
        let section_usage = sections.usage();
        let refused = sections
            .prepare_offsets_until(structural_offset_limits())
            .expect_err("genuine successful outer decoder, actual malformed offset child");
        assert_eq!(refused.parent_sections().usage(), section_usage);
        assert_eq!(
            refused.parent_sections().structural_bytes(),
            Some(malformed.as_slice())
        );
        assert!(refused.parent_sections().parent_has_native_context());
        assert!(refused.parent_sections().parent_stream_calls() > 0);
        assert!(refused.allocation_succeeded());
        assert!(!refused.child_succeeded());
        assert_eq!(
            refused
                .offsets()
                .iter()
                .map(|offset| offset.get())
                .collect::<Vec<_>>(),
            prefix
        );
        assert_eq!(refused.original_expiry(), expiry);
        assert!(refused.recovery_usage().work_bytes > before.work_bytes);
    }
}

fn structural_timestamp_limits() -> crate::RecoveredStructuralTimestampLimits {
    crate::RecoveredStructuralTimestampLimits {
        max_input_bytes: 1 << 20,
        max_records: 1 << 16,
        max_chunks: 1024,
        max_native_calls: 1 << 17,
        max_bins: 16384,
        max_ans_log: 14,
        max_declared_bytes: 16 << 20,
        max_entries: 16 << 20,
        max_work_bytes: 64 << 20,
    }
}

fn actual_timestamp_file(chunks: &[&[u64]], hint: usize, uniform: bool) -> Vec<u8> {
    let file = pco::standalone::FileCompressor::default()
        .with_n_hint(hint)
        .with_uniform_type(uniform.then_some(pco::data_types::NumberType::U64));
    let config = pco::ChunkConfig::default()
        .with_compression_level(8)
        .with_mode_spec(pco::ModeSpec::Classic)
        .with_delta_spec(pco::DeltaSpec::TryConsecutive(1));
    let mut lane = Vec::new();
    file.write_header(&mut lane)
        .expect("actual canonical Pco file header");
    for values in chunks {
        file.chunk_compressor(values, &config)
            .expect("genuine configured Classic/consecutive/fallback compressor")
            .write(&mut lane)
            .expect("actual native compressed chunk");
    }
    file.write_footer(&mut lane)
        .expect("genuine actual EndOfData marker");
    lane
}

fn actual_timestamp_records(values: &[u64]) -> Vec<crate::DurableLog> {
    values
        .iter()
        .enumerate()
        .map(|(ordinal, timestamp)| {
            let mut record = crate::DurableLog::new(
                ShardId::new(7),
                partition(),
                shard_stream_core::LogicalOffset::new(
                    u64::try_from(ordinal).expect("finite ordinal"),
                ),
                *timestamp,
                "actual timestamp typed body",
                CompressionCohortId::new(4),
            )
            .with_field("original", "retained");
            record.body = Some(crate::TelemetryValue::Array(Arc::new(vec![
                crate::TelemetryValue::Integer(-7),
                crate::TelemetryValue::DoubleBits(0x7ff8_0000_0000_0042),
            ])));
            record.event_name = Arc::from("actual.timestamps.typed.metadata");
            record
        })
        .collect()
}

fn timestamp_frame_with_lane(values: &[u64], lane: &[u8]) -> (Vec<u8>, Vec<u8>, u32) {
    let records = actual_timestamp_records(values);
    let count = u32::try_from(records.len()).expect("finite original typed record count");
    let raw =
        crate::encode_structural_block(&records).expect("actual canonical complete typed producer");
    let compressed = zstd::bulk::compress(&raw, 1).expect("real canonical outer encoding");
    let sections = actual_offset_sections(
        &compressed,
        raw.len(),
        count,
        Instant::now() + Duration::from_secs(30),
    );
    let mut wire = crate::structural::STRUCTURAL_BLOCK_MAGIC.to_vec();
    append_section_extent(&mut wire, records.len());
    for kind in STRUCTURAL_SECTION_KINDS {
        let actual = if kind == crate::RecoveredStructuralSection::Timestamps {
            lane
        } else {
            sections
                .section_bytes(kind)
                .expect("all original actual other encoded lanes")
        };
        append_section_extent(&mut wire, actual.len());
        wire.extend_from_slice(actual);
    }
    let compressed = zstd::bulk::compress(&wire, 1).expect("real outer structural frame");
    (wire, compressed, count)
}

fn actual_timestamp_parent(
    raw: &[u8],
    compressed: &[u8],
    count: u32,
    expiry: Instant,
) -> crate::PreparedRecoveredStructuralOffsets {
    actual_offset_sections(compressed, raw.len(), count, expiry)
        .prepare_offsets_until(structural_offset_limits())
        .expect("actual complete native/section/offset parent before timestamps")
}

fn actual_timestamp_random(n: usize) -> Vec<u64> {
    let mut state = 0x38e9_83df_a762_4021u64;
    (0..n)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        })
        .collect()
}

#[test]
#[allow(
    clippy::assertions_on_constants,
    reason = "this runtime regression must verify the actual selected compiler configuration"
)]
fn recovered_timestamps_match_real_modes_chunks_and_ordinary_tail() {
    assert!(
        cfg!(telemetry_bounded_pco_layout),
        "normal reviewed compiler/config selects this new stage"
    );
    let consecutive = (0..600u64).map(|i| 1_000_000 + 71 * i).collect::<Vec<_>>();
    let random = actual_timestamp_random(600);
    let config = pco::ChunkConfig::default()
        .with_compression_level(8)
        .with_mode_spec(pco::ModeSpec::Classic)
        .with_delta_spec(pco::DeltaSpec::TryConsecutive(1));
    let file = pco::standalone::FileCompressor::default();
    assert!(matches!(
        file.chunk_compressor(&consecutive, &config)
            .expect("actual consecutive producer")
            .meta()
            .delta_encoding,
        pco::metadata::DeltaEncoding::Consecutive {
            order: 1,
            secondary_uses_delta: false
        }
    ));
    assert!(matches!(
        file.chunk_compressor(&random, &config)
            .expect("actual incompressible producer fallback")
            .meta()
            .delta_encoding,
        pco::metadata::DeltaEncoding::NoOp
    ));
    for (values, uniform) in [(&consecutive, false), (&random, true)] {
        // Two true chunks, >256 in each, oversized untrusted hint and real tail.
        let mut lane =
            actual_timestamp_file(&[&values[..300], &values[300..]], usize::MAX, uniform);
        let tail = b"retained ordinary first-file tail";
        lane.extend_from_slice(tail);
        assert_eq!(
            pco::standalone::simple_decompress::<u64>(&lane)
                .expect("ordinary native value/tail oracle"),
            *values
        );
        let (raw, compressed, count) = timestamp_frame_with_lane(values, &lane);
        let ordinary =
            crate::decode_structural_block(&raw).expect("ordinary complete exact typed-row oracle");
        let canonical = crate::encode_structural_block(&actual_timestamp_records(values))
            .expect("actual canonical complete typed producer");
        assert_eq!(
            ordinary,
            crate::decode_structural_block(&canonical)
                .expect("full typed differential: unchanged non-Pco lanes")
        );
        assert_eq!(
            ordinary
                .iter()
                .map(|row| row.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            *values
        );
        let expiry = Instant::now() + Duration::from_secs(30);
        let parent = actual_timestamp_parent(&raw, &compressed, count, expiry);
        let pointer = parent
            .parent_sections()
            .structural_bytes()
            .expect("complete output")
            .as_ptr();
        let prior = parent.recovery_usage();
        let result = parent
            .prepare_timestamps_until(structural_timestamp_limits())
            .expect("actual owned lower-level Pco chain");
        assert_eq!(result.timestamps(), values.as_slice());
        assert_eq!(result.usage().chunks, 2);
        assert_eq!(result.usage().completed_chunks, 2);
        assert_eq!(result.usage().reads, 4);
        assert_eq!(result.usage().native_calls, 8); // file +2 chunks +4 reads +EOF.
        assert_eq!(result.usage().confirmed_records, values.len());
        assert!(result.usage().reached_end);
        assert_eq!(result.unread_tail(), tail);
        assert_eq!(result.original_expiry(), expiry);
        assert_eq!(
            result
                .parent_offsets()
                .parent_sections()
                .structural_bytes()
                .expect("same complete parent allocation")
                .as_ptr(),
            pointer
        );
        assert_eq!(
            result.parent_offsets().parent_sections().structural_bytes(),
            Some(raw.as_slice())
        );
        assert!(
            result
                .parent_offsets()
                .parent_sections()
                .parent_has_native_context()
        );
        assert_eq!(result.recovery_usage().read_bytes, prior.read_bytes);
        assert_eq!(result.recovery_usage().json_values, prior.json_values);
        assert!(result.recovery_usage().work_bytes > prior.work_bytes);
        assert_eq!(
            crate::decode_structural_block(
                result
                    .parent_offsets()
                    .parent_sections()
                    .structural_bytes()
                    .expect("all typed lanes retained")
            )
            .expect("same full typed oracle"),
            ordinary
        );
    }
}

#[test]
fn recovered_timestamps_native_read_error_retains_untrusted_window_and_source() {
    let values = actual_timestamp_random(600);
    let mut lane = actual_timestamp_file(&[&values], values.len(), false);
    // Remove the real footer and one genuine body byte. Header/metadata remain
    // genuinely valid; the entered third native read sees actual short input.
    lane.truncate(lane.len() - 2);
    assert!(pco::standalone::simple_decompress::<u64>(&lane).is_err());
    let (raw, compressed, count) = timestamp_frame_with_lane(&values, &lane);
    let expiry = Instant::now() + Duration::from_secs(30);
    let parent = actual_timestamp_parent(&raw, &compressed, count, expiry);
    let prior = parent.recovery_usage();
    let refused = parent
        .prepare_timestamps_until(structural_timestamp_limits())
        .expect_err("real native short-body refusal");
    assert!(refused.has_native_file() && refused.has_native_chunk());
    assert!(refused.native_error().is_some());
    assert!(
        refused
            .last_read_result()
            .expect("actual native read returned a Result")
            .is_err()
    );
    assert_eq!(refused.initialized_timestamps().len(), values.len());
    assert_eq!(
        refused.confirmed_timestamps(),
        &values[..refused.usage().confirmed_records]
    );
    assert!(refused.usage().confirmed_records >= 256);
    let window = refused
        .pending_window()
        .expect("actual possibly-modified window stays owned");
    assert_eq!(refused.last_read_window(), Some(window.clone()));
    assert!(!window.is_empty() && window.len() <= 256);
    assert_eq!(window.start, refused.confirmed_timestamps().len());
    assert_eq!(refused.retained_input(), Some(lane.as_slice()));
    assert!(
        refused
            .last_committed_source_cursor()
            .is_some_and(|cursor| cursor <= lane.len())
    );
    let (fills, consumes) = refused
        .source_observations()
        .expect("real reader escrow counters");
    assert!(fills > 0 && consumes > 0 && fills >= consumes);
    assert_eq!(refused.usage().completed_chunks, 0);
    assert!(!refused.usage().reached_end);
    assert_eq!(
        refused
            .parent_offsets()
            .parent_sections()
            .structural_bytes(),
        Some(raw.as_slice())
    );
    assert!(
        refused
            .parent_offsets()
            .parent_sections()
            .parent_has_native_context()
    );
    assert_eq!(refused.original_expiry(), expiry);
    assert!(refused.recovery_usage().work_bytes > prior.work_bytes);
}

#[test]
fn recovered_timestamps_limits_and_original_cumulative_work_keep_parent() {
    let values = (0..300u64).map(|n| 190 + 17 * n).collect::<Vec<_>>();
    let lane = actual_timestamp_file(&[&values], values.len(), false);
    let (raw, compressed, count) = timestamp_frame_with_lane(&values, &lane);
    let directory = Directory::new();
    let path = directory.0.join("same-real-timestamp-source.zst");
    fs::write(&path, &compressed)
        .expect("one fixed real source before independent original ledgers");
    let expiry = Instant::now() + Duration::from_secs(30);
    let run = |parent_work, child_limits| {
        let mut limits = recovery_decompression_budget_limits();
        limits.max_work_bytes = parent_work;
        let mut original = crate::TelemetryRecoveryBudget::new(limits, expiry)
            .expect("one independent ORIGINAL finite ledger before identical real setup");
        let input = Bytes::from(original.read_file(&path).expect("same actual source read"));
        let setup = original.usage();
        let parent = crate::decompress_recovered_log_frame_until(
            input,
            raw.len(),
            count,
            recovery_decompression_policy(),
            original,
        )
        .expect("real retained outer decoder under original envelope")
        .prepare_sections_until(structural_section_limits())
        .expect("same genuine sections")
        .prepare_offsets_until(structural_offset_limits())
        .expect("same actual offsets");
        let prior = parent.recovery_usage();
        (setup, prior, parent.prepare_timestamps_until(child_limits))
    };
    let ample = recovery_decompression_budget_limits().max_work_bytes;
    let (setup, prior, observed) = run(ample, structural_timestamp_limits());
    let observed = observed.expect("actual complete observation, no copied formula");
    let usage = observed.usage();
    let cumulative = observed.recovery_usage();
    drop(observed);
    let exact = crate::RecoveredStructuralTimestampLimits {
        max_input_bytes: lane.len(),
        max_records: values.len(),
        max_chunks: usage.chunks,
        max_native_calls: usage.native_calls,
        max_declared_bytes: usage.declared_bytes,
        max_entries: usage.entries,
        max_work_bytes: usage.work_bytes,
        ..structural_timestamp_limits()
    };
    let (same_setup, same_prior, accepted) = run(ample, exact);
    let accepted = accepted.expect("actual observed child boundaries fit exactly");
    assert_eq!(accepted.usage(), usage);
    assert_eq!(accepted.recovery_usage(), cumulative);
    assert_eq!(same_setup, setup);
    assert_eq!(same_prior, prior);
    assert_eq!(accepted.timestamps(), values);
    drop(accepted);
    for axis in 0..7 {
        let mut limits = exact;
        match axis {
            0 => limits.max_input_bytes -= 1,
            1 => limits.max_records -= 1,
            2 => limits.max_chunks -= 1,
            3 => limits.max_native_calls -= 1,
            4 => limits.max_declared_bytes -= 1,
            5 => limits.max_entries -= 1,
            _ => limits.max_work_bytes -= 1,
        }
        let (same_setup, same_prior, refused) = run(ample, limits);
        let refused = refused.expect_err("one less real child boundary, never renew work");
        assert_eq!(same_setup, setup);
        assert_eq!(same_prior, prior);
        assert_eq!(
            refused
                .parent_offsets()
                .parent_sections()
                .structural_bytes(),
            Some(raw.as_slice())
        );
        assert!(
            refused
                .parent_offsets()
                .parent_sections()
                .parent_has_native_context()
        );
        assert_eq!(refused.original_expiry(), expiry);
    }
    let (same_setup, same_prior, accepted) =
        run(cumulative.work_bytes, structural_timestamp_limits());
    let accepted =
        accepted.expect("exact OBSERVED cumulative ORIGINAL parent work across all stages");
    assert_eq!(accepted.recovery_usage(), cumulative);
    assert_eq!(same_setup, setup);
    assert_eq!(same_prior, prior);
    drop(accepted);
    let (same_setup, same_prior, refused) =
        run(cumulative.work_bytes - 1, structural_timestamp_limits());
    let refused =
        refused.expect_err("one less ORIGINAL cumulative work before final physical EOF entry");
    assert_eq!(same_setup, setup);
    assert_eq!(same_prior, prior);
    assert_eq!(refused.confirmed_timestamps(), values);
    assert!(!refused.usage().reached_end);
    assert_eq!(refused.usage().completed_chunks, 1);
    assert!(refused.has_native_file());
    assert_eq!(refused.original_expiry(), expiry);
    assert!(refused.recovery_usage().work_bytes > prior.work_bytes);
    let mut source = std::error::Error::source(&refused);
    let mut original_limits = false;
    while let Some(error) = source {
        original_limits |= matches!(
            error.downcast_ref::<crate::TelemetryRecoveryError>(),
            Some(crate::TelemetryRecoveryError::Limits)
        );
        source = error.source();
    }
    assert!(
        original_limits,
        "actual original parent Limits remains in source chain"
    );
}

#[test]
fn recovered_timestamps_expiry_and_unwind_keep_actual_native_owners() {
    let values = (0..300u64).collect::<Vec<_>>();
    let lane = actual_timestamp_file(&[&values], values.len(), false);
    let (raw, compressed, count) = timestamp_frame_with_lane(&values, &lane);
    for phase in [
        "timestamp-allocation",
        "timestamp-reader",
        "timestamp-file-result",
        "timestamp-chunk-result",
        "timestamp-read-result",
        "timestamp-finished-source",
        "timestamp-eof-result",
        "structural-child-result",
    ] {
        for panic in [false, true] {
            if panic && !cfg!(panic = "unwind") {
                continue;
            } // abort cannot retain a returned panic.
            let expiry = Instant::now() + Duration::from_secs(if panic { 30 } else { 1 });
            let parent = actual_timestamp_parent(&raw, &compressed, count, expiry);
            let before = parent.recovery_usage();
            let pointer = parent
                .parent_sections()
                .structural_bytes()
                .expect("real complete output")
                .as_ptr();
            let _hook = Hook;
            TARGET.set(Some((phase, 0, panic)));
            let refused = parent
                .prepare_timestamps_until(structural_timestamp_limits())
                .expect_err("actual saved outcome before original E/unwind");
            assert!(OBSERVED.get(), "actual phase reached: {phase}");
            assert_eq!(refused.has_panic(), panic);
            assert_eq!(
                refused.child_succeeded(),
                phase == "structural-child-result"
            );
            assert_eq!(
                refused
                    .parent_offsets()
                    .parent_sections()
                    .structural_bytes()
                    .expect("same native allocation")
                    .as_ptr(),
                pointer
            );
            assert!(
                refused
                    .parent_offsets()
                    .parent_sections()
                    .parent_has_native_context()
            );
            assert_eq!(refused.original_expiry(), expiry);
            assert!(refused.recovery_usage().work_bytes > before.work_bytes);
            if !panic {
                assert!(matches!(
                    refused.post_error(),
                    Some(crate::TelemetryRecoveryError::Expired)
                ));
            }
            if matches!(
                phase,
                "timestamp-file-result"
                    | "timestamp-chunk-result"
                    | "timestamp-read-result"
                    | "timestamp-finished-source"
                    | "timestamp-eof-result"
                    | "structural-child-result"
            ) {
                assert!(refused.file_result_captured());
                assert!(refused.has_native_file());
                assert_eq!(refused.retained_input(), Some(lane.as_slice()));
            }
            if matches!(phase, "timestamp-chunk-result" | "timestamp-read-result") {
                assert!(refused.chunk_result_captured());
                assert!(refused.has_native_chunk());
            }
            if phase == "timestamp-read-result" {
                let actual = refused
                    .last_read_result()
                    .expect("actual native Result saved before observer")
                    .as_ref()
                    .expect("genuine successful physical read");
                assert_eq!(actual.n_processed, 256);
                assert!(!actual.finished);
                assert_eq!(refused.pending_window(), Some(0..256));
                assert_eq!(&refused.initialized_timestamps()[..256], &values[..256]);
                assert!(refused.confirmed_timestamps().is_empty());
            }
            if phase == "timestamp-eof-result" {
                assert!(refused.end_result_captured());
                assert!(!refused.has_native_chunk());
                assert_eq!(refused.confirmed_timestamps(), values);
                assert_eq!(refused.usage().completed_chunks, 1);
                assert!(!refused.usage().reached_end);
            }
            if phase == "timestamp-finished-source" {
                assert!(!refused.has_native_chunk());
                assert_eq!(refused.usage().completed_chunks, 1);
                assert_eq!(refused.confirmed_timestamps(), values);
                assert!(!refused.usage().reached_end);
            }
        }
    }
    let expiry = Instant::now() + Duration::from_secs(1);
    let parent = actual_timestamp_parent(&raw, &compressed, count, expiry);
    let prior = parent.recovery_usage();
    while Instant::now() < expiry {
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let refused = parent
        .prepare_timestamps_until(structural_timestamp_limits())
        .expect_err("expired original owner does not start a new codec");
    assert_eq!(refused.recovery_usage(), prior);
    assert_eq!(
        refused.usage(),
        crate::RecoveredStructuralTimestampUsage::default()
    );
    assert!(!refused.file_result_captured());
    assert!(refused.retained_input().is_none());
    assert_eq!(
        refused
            .parent_offsets()
            .parent_sections()
            .structural_bytes(),
        Some(raw.as_slice())
    );
}

#[test]
fn recovered_timestamps_untrusted_metadata_refuses_before_native_chunk() {
    let values = (0..12u64).map(|n| 9 + n * 81).collect::<Vec<_>>();
    let canonical = actual_timestamp_file(&[&values], usize::MAX, false);
    let (_, rest) = pco::standalone::FileDecompressor::new(canonical.as_slice())
        .expect("actual header location oracle");
    let meta = canonical.len() - rest.len() + 4; // actual type byte +24-bit n_minus_one.
    let mut dict = canonical.clone();
    dict[meta] = (dict[meta] & 0xf0) | 4;
    let mut lookback = canonical.clone();
    lookback[meta] = (lookback[meta] & 0x0f) | 0x20;
    let mut too_many = canonical.clone();
    // Actual three-byte little-endian n_minus_one now declares13 against12.
    let preamble = canonical.len() - rest.len();
    too_many[preamble + 1..preamble + 4].copy_from_slice(&[12, 0, 0]);
    let mut early = canonical[..preamble].to_vec();
    early.push(0);
    for (lane, actual_eof) in [
        (dict, false),
        (lookback, false),
        (too_many, false),
        (early, true),
    ] {
        let (raw, compressed, count) = timestamp_frame_with_lane(&values, &lane);
        let expiry = Instant::now() + Duration::from_secs(30);
        let parent = actual_timestamp_parent(&raw, &compressed, count, expiry);
        let refused = parent
            .prepare_timestamps_until(structural_timestamp_limits())
            .expect_err("real valid outer frame, unsupported/malformed inner timestamp lane");
        assert!(refused.has_native_file());
        assert!(!refused.has_native_chunk());
        assert!(refused.native_error().is_none());
        assert_eq!(refused.usage().native_calls, if actual_eof { 2 } else { 1 });
        assert_eq!(refused.usage().reached_end, actual_eof);
        assert_eq!(refused.retained_input(), Some(lane.as_slice()));
        assert_eq!(refused.initialized_timestamps().len(), values.len());
        assert!(refused.confirmed_timestamps().is_empty());
        assert_eq!(refused.original_expiry(), expiry);
        assert_eq!(
            refused
                .parent_offsets()
                .parent_sections()
                .structural_bytes(),
            Some(raw.as_slice())
        );
        assert!(
            refused
                .parent_offsets()
                .parent_sections()
                .parent_has_native_context()
        );
    }
    let mut future_header = canonical;
    future_header[4] = 4; // real unsupported standalone version, no fake native result.
    let (raw, compressed, count) = timestamp_frame_with_lane(&values, &future_header);
    let expiry = Instant::now() + Duration::from_secs(30);
    let parent = actual_timestamp_parent(&raw, &compressed, count, expiry);
    let refused = parent
        .prepare_timestamps_until(structural_timestamp_limits())
        .expect_err("unsupported file domain refuses before reader/native entry");
    assert_eq!(refused.usage().native_calls, 0);
    assert!(!refused.file_result_captured());
    assert!(refused.retained_input().is_none());
    assert_eq!(refused.original_expiry(), expiry);
    assert_eq!(
        refused
            .parent_offsets()
            .parent_sections()
            .structural_bytes(),
        Some(raw.as_slice())
    );
}

#[test]
fn recovered_timestamps_same_ledger_continues_after_finished_sources() {
    let values = (0..300u64).collect::<Vec<_>>();
    let lane = actual_timestamp_file(&[&values], 0, false);
    let (raw, compressed, count) = timestamp_frame_with_lane(&values, &lane);
    let expiry = Instant::now() + Duration::from_secs(30);
    let parent = actual_timestamp_parent(&raw, &compressed, count, expiry);
    let before = parent.recovery_usage();
    let prepared = parent
        .prepare_timestamps_until(structural_timestamp_limits())
        .expect("genuine finished native timestamp stage");
    assert_eq!(prepared.timestamps(), values);
    assert!(prepared.usage().reached_end);
    let after = prepared.recovery_usage();
    let next = prepared
        .decompress_next_until(
            Bytes::from(compressed),
            raw.len(),
            count,
            recovery_decompression_policy(),
        )
        .expect("same original already-spent work through next actual native frame");
    assert_eq!(next.original_expiry(), expiry);
    assert!(
        next.recovery_usage().work_bytes > after.work_bytes && after.work_bytes > before.work_bytes
    );
    assert_eq!(next.recovery_usage().read_bytes, before.read_bytes);
    assert_eq!(next.structural_bytes(), Some(raw.as_slice()));
}

#[cfg(panic = "unwind")]
#[test]
fn recovered_timestamps_refused_capture_counts_no_unentered_native_read() {
    let values = (100..400u64).collect::<Vec<_>>();
    let lane = actual_timestamp_file(&[&values], values.len(), false);
    let (raw, compressed, count) = timestamp_frame_with_lane(&values, &lane);
    let directory = Directory::new();
    let path = directory
        .0
        .join("same-real-native-read-admission-source.zst");
    fs::write(&path, &compressed).expect("one fixed genuine source before original ledgers");
    let expiry = Instant::now() + Duration::from_secs(30);
    let run = |parent_work| {
        let mut limits = recovery_decompression_budget_limits();
        limits.max_work_bytes = parent_work;
        let mut original = crate::TelemetryRecoveryBudget::new(limits, expiry)
            .expect("independent ORIGINAL ledger before identical actual read setup");
        let input = Bytes::from(original.read_file(&path).expect("same real source read"));
        let setup = original.usage();
        let parent = crate::decompress_recovered_log_frame_until(
            input,
            raw.len(),
            count,
            recovery_decompression_policy(),
            original,
        )
        .expect("actual native parent")
        .prepare_sections_until(structural_section_limits())
        .expect("actual sections")
        .prepare_offsets_until(structural_offset_limits())
        .expect("actual offsets");
        let prior = parent.recovery_usage();
        (
            setup,
            prior,
            parent.prepare_timestamps_until(structural_timestamp_limits()),
        )
    };
    let ample = recovery_decompression_budget_limits().max_work_bytes;
    let (setup, prior, observed) = {
        let _hook = Hook;
        TARGET.set(Some(("timestamp-read-admission", 0, true)));
        let (setup, prior, actual) = run(ample);
        assert!(OBSERVED.get(), "the actual native read fee was admitted");
        let actual = actual.expect_err("observe original work before actual capture/read entry");
        assert!(actual.has_panic());
        (setup, prior, actual)
    };
    assert!(observed.has_native_file() && observed.has_native_chunk());
    assert_eq!(
        observed.usage().native_calls,
        2,
        "only the actual File and Chunk constructors entered"
    );
    assert_eq!(observed.usage().reads, 0);
    assert!(observed.last_read_result().is_none());
    assert!(observed.last_read_window().is_none());
    let cutoff = observed.recovery_usage().work_bytes;
    let observed_usage = observed.usage();
    let source_cursor = observed.last_committed_source_cursor();
    let source_operations = observed.source_observations();
    assert!(source_operations.is_some_and(|(fills, consumes)| fills > 0 && consumes > 0));
    assert_eq!(observed.pending_window(), Some(0..256));
    assert_eq!(observed.retained_input(), Some(lane.as_slice()));
    assert!(
        observed
            .initialized_timestamps()
            .iter()
            .all(|value| *value == 0)
    );
    drop(observed);

    // Derive this original cap from the real observation, never a fee formula.
    // It fits the native fee exactly; the next actual capture charge must refuse.
    let (same_setup, same_prior, limited) = run(cutoff);
    let limited = limited.expect_err("capture storage cannot enter an unadmitted read");
    assert_eq!(same_setup, setup);
    assert_eq!(same_prior, prior);
    assert!(!limited.has_panic());
    assert_eq!(limited.recovery_usage().work_bytes, cutoff);
    assert_eq!(limited.usage(), observed_usage);
    assert_eq!(limited.usage().native_calls, 2);
    assert_eq!(limited.usage().reads, 0);
    assert!(limited.last_read_result().is_none());
    assert!(limited.last_read_window().is_none());
    assert_eq!(limited.pending_window(), Some(0..256));
    assert_eq!(limited.last_committed_source_cursor(), source_cursor);
    assert_eq!(
        limited.source_observations(),
        source_operations,
        "the actual native reader has performed no read after its original constructor work"
    );
    assert!(limited.has_native_file() && limited.has_native_chunk());
    assert!(limited.native_error().is_none());
    assert_eq!(limited.initialized_timestamps().len(), values.len());
    assert!(
        limited
            .initialized_timestamps()
            .iter()
            .all(|value| *value == 0)
    );
    assert!(limited.confirmed_timestamps().is_empty());
    assert_eq!(limited.retained_input(), Some(lane.as_slice()));
    assert_eq!(limited.original_expiry(), expiry);
    assert_eq!(
        limited
            .parent_offsets()
            .parent_sections()
            .structural_bytes(),
        Some(raw.as_slice())
    );
    assert!(
        limited
            .parent_offsets()
            .parent_sections()
            .parent_has_native_context()
    );
    let mut source = std::error::Error::source(&limited);
    let mut original_limits = false;
    while let Some(error) = source {
        original_limits |= matches!(
            error.downcast_ref::<crate::TelemetryRecoveryError>(),
            Some(crate::TelemetryRecoveryError::Limits)
        );
        source = error.source();
    }
    assert!(
        original_limits,
        "the actual original capture-admission refusal remains retained"
    );
    drop(limited);
    let (same_setup, same_prior, complete) = run(ample);
    let complete =
        complete.expect("the unchanged original ample ledger genuinely enters both reads and EOF");
    assert_eq!(same_setup, setup);
    assert_eq!(same_prior, prior);
    assert_eq!(complete.timestamps(), values);
    assert_eq!(complete.usage().reads, 2);
    assert_eq!(complete.usage().native_calls, 5);
    assert!(complete.usage().reached_end);
}

fn structural_index_limits() -> crate::RecoveredStructuralIndexLimits {
    crate::RecoveredStructuralIndexLimits {
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
fn actual_recovery_index_frame() -> crate::IndexedStructuralBlock {
    let mut records = actual_timestamp_records(&(0..96).map(|i| 10_000 + i).collect::<Vec<_>>());
    for (ordinal, record) in records.iter_mut().enumerate() {
        record.message = Arc::from(if ordinal < 48 {
            "ordinary alpha message"
        } else {
            "worker failed after retry count 42 from shard"
        });
        record.fields = Arc::new(vec![
            crate::MetadataField::new("service", "billing"),
            crate::MetadataField::new("severity", "INFO"),
        ]);
    }
    crate::encode_indexed_structural_records(&records)
        .expect("real complete structural producer including exact typed lane and index")
}
fn actual_recovery_index_parent(
    raw: &[u8],
    compressed: &[u8],
    count: u32,
    expiry: Instant,
) -> crate::PreparedRecoveredStructuralTimestamps {
    actual_timestamp_parent(raw, compressed, count, expiry)
        .prepare_timestamps_until(structural_timestamp_limits())
        .expect("actual native outer/sections/offsets/Pco parent before index")
}
#[test]
fn recovered_index_matches_real_emitter_and_continues_same_original_ledger() {
    let frame = actual_recovery_index_frame();
    let compressed = zstd::bulk::compress(&frame.structural, 1).expect("actual outer codec");
    let expiry = Instant::now() + Duration::from_secs(30);
    let parent = actual_recovery_index_parent(&frame.structural, &compressed, 96, expiry);
    let prior = parent.recovery_usage();
    let pointer = parent
        .parent_offsets()
        .parent_sections()
        .structural_bytes()
        .expect("actual retained complete source")
        .as_ptr();
    assert!(parent.has_native_file() && parent.has_end_source());
    let index = parent
        .prepare_index_until(structural_index_limits())
        .expect("actual same-parser recovery child");
    let ordinary = crate::decode_embedded_frame_index(&frame.structural)
        .expect("actual ordinary index oracle");
    assert_eq!(index.index(), Some(&ordinary));
    assert_eq!(index.index(), Some(&frame.index));
    assert_eq!(
        index.retained_input(),
        Some(frame.embedded_index.as_slice())
    );
    let range = index.input_range().expect("original actual section range");
    assert_eq!(
        index.retained_input().expect("exact shared input").as_ptr(),
        pointer.wrapping_add(range.start)
    );
    assert!(index.child_succeeded() && !index.has_panic());
    assert_eq!(index.original_expiry(), expiry);
    let usage = index.usage();
    assert_eq!(
        usage.output_bytes,
        ordinary.cache_bytes() - size_of::<crate::EmbeddedFrameIndex>()
    );
    let spent = index.recovery_usage();
    assert_eq!(spent.read_bytes, prior.read_bytes);
    assert_eq!(spent.records, prior.records);
    assert_eq!(spent.json_values, prior.json_values);
    // Fixed state+input admission is outside local output requests; local slots
    // do not become an extra work surcharge on the recovery ledger.
    assert_eq!(
        spent.work_bytes - prior.work_bytes,
        usage.continuation_bytes + usage.work_bytes
    );
    assert!(
        spent.retained_bytes - prior.retained_bytes > usage.continuation_bytes + usage.output_bytes
    );
    let second = index
        .decompress_next_until(
            Bytes::from(compressed),
            frame.structural.len(),
            96,
            recovery_decompression_policy(),
        )
        .expect("same spent original ledger after all completed children")
        .prepare_sections_until(structural_section_limits())
        .expect("second actual sections")
        .prepare_offsets_until(structural_offset_limits())
        .expect("second actual offsets")
        .prepare_timestamps_until(structural_timestamp_limits())
        .expect("second actual native Pco")
        .prepare_index_until(structural_index_limits())
        .expect("second actual same-parser index");
    assert_eq!(second.index(), Some(&ordinary));
    assert_eq!(second.original_expiry(), expiry);
    assert!(second.recovery_usage().work_bytes > spent.work_bytes);
    assert!(second.recovery_usage().retained_bytes > spent.retained_bytes);
    assert_eq!(second.recovery_usage().read_bytes, prior.read_bytes);
}
#[test]
fn recovered_index_exact_and_one_less_original_caps_retain_partial_output() {
    let frame = actual_recovery_index_frame();
    let compressed = zstd::bulk::compress(&frame.structural, 1).expect("actual finite frame");
    let directory = Directory::new();
    let path = directory.0.join("same-index-original-source.zst");
    fs::write(&path, &compressed).expect("one real source before independent original ledgers");
    let expiry = Instant::now() + Duration::from_secs(30);
    let run = |bytes, work, child| {
        let mut limits = recovery_decompression_budget_limits();
        limits.max_retained_bytes = bytes;
        limits.max_work_bytes = work;
        let mut original = crate::TelemetryRecoveryBudget::new(limits, expiry)
            .expect("independent original budget before identical setup");
        let input = Bytes::from(
            original
                .read_file(&path)
                .expect("same actual physical read"),
        );
        let parent = crate::decompress_recovered_log_frame_until(
            input,
            frame.structural.len(),
            96,
            recovery_decompression_policy(),
            original,
        )
        .expect("actual entered outer stream")
        .prepare_sections_until(structural_section_limits())
        .expect("actual section parent")
        .prepare_offsets_until(structural_offset_limits())
        .expect("actual offsets")
        .prepare_timestamps_until(structural_timestamp_limits())
        .expect("actual native Pco before index boundary");
        let prior = parent.recovery_usage();
        (prior, parent.prepare_index_until(child))
    };
    let ample = recovery_decompression_budget_limits();
    let (_, observed) = run(
        ample.max_retained_bytes,
        ample.max_work_bytes,
        structural_index_limits(),
    );
    let observed = observed
        .expect("real complete observation before constructing independent original exact ledgers");
    let cumulative = observed.recovery_usage();
    let local = observed.usage();
    drop(observed);
    let (_, exact) = run(
        cumulative.retained_bytes,
        cumulative.work_bytes,
        structural_index_limits(),
    );
    let exact = exact.expect("exact original caps fit actual complete chain");
    assert_eq!(exact.recovery_usage(), cumulative);
    assert_eq!(exact.index(), Some(&frame.index));
    drop(exact);
    for (bytes, work) in [
        (cumulative.retained_bytes - 1, cumulative.work_bytes),
        (cumulative.retained_bytes, cumulative.work_bytes - 1),
    ] {
        let (prior, refused) = run(bytes, work, structural_index_limits());
        let refused =
            refused.expect_err("actual original ledger refuses before complete index acceptance");
        assert!(refused.usage().output_bytes > 0);
        assert_eq!(
            refused.retained_input(),
            Some(frame.embedded_index.as_slice())
        );
        assert!(refused.parent_timestamps().has_native_file());
        assert_eq!(refused.original_expiry(), expiry);
        assert!(refused.recovery_usage().work_bytes > prior.work_bytes);
        assert_eq!(refused.recovery_usage().records, prior.records);
        assert_eq!(refused.recovery_usage().json_values, prior.json_values);
        let mut cause = std::error::Error::source(&refused);
        let mut original_refusal = false;
        while let Some(error) = cause {
            original_refusal |= matches!(
                error.downcast_ref::<crate::TelemetryRecoveryError>(),
                Some(crate::TelemetryRecoveryError::Limits)
            );
            cause = error.source();
        }
        assert!(
            original_refusal,
            "retain the actual original ledger refusal in source chain"
        );
    }
    let mut smaller = structural_index_limits();
    smaller.max_output_bytes = local.output_bytes - 1;
    let (_, refused) = run(ample.max_retained_bytes, ample.max_work_bytes, smaller);
    let refused =
        refused.expect_err("actual local allocation admission preserves existing partial vectors");
    assert!(refused.usage().output_bytes > 0);
    assert!(refused.usage().partial_column_bytes > 0);
    assert_eq!(
        refused.retained_input(),
        Some(frame.embedded_index.as_slice())
    );
}
#[test]
fn recovered_index_late_and_unwind_keep_real_partial_and_completed_state() {
    let frame = actual_recovery_index_frame();
    let compressed = zstd::bulk::compress(&frame.structural, 1).expect("actual outer frame");
    for phase in [
        "column",
        "run",
        "id",
        "membership",
        "term",
        "field",
        "completed",
    ] {
        for panic in [false, true] {
            let expiry = Instant::now() + Duration::from_millis(200);
            let parent = actual_recovery_index_parent(&frame.structural, &compressed, 96, expiry);
            let before = parent.recovery_usage();
            let pointer = parent
                .parent_offsets()
                .parent_sections()
                .structural_bytes()
                .expect("real complete source")
                .as_ptr();
            let refused = crate::structural::with_recovery_phase(phase, panic, || {
                parent.prepare_index_until(structural_index_limits())
            })
            .expect_err("actual parser mutation precedes original expiry or unwind");
            assert_eq!(refused.has_panic(), panic);
            assert_eq!(refused.original_expiry(), expiry);
            assert_eq!(
                refused
                    .parent_timestamps()
                    .parent_offsets()
                    .parent_sections()
                    .structural_bytes()
                    .expect("original parent allocation")
                    .as_ptr(),
                pointer
            );
            assert!(
                refused.parent_timestamps().has_native_file()
                    && refused.parent_timestamps().has_end_source()
            );
            assert_eq!(
                refused.retained_input(),
                Some(frame.embedded_index.as_slice())
            );
            let actual = refused.usage();
            match phase {
                "column" | "run" => assert!(actual.partial_column_bytes > 0),
                "id" => assert!(actual.partial_locator_ids > 0),
                "membership" => assert!(actual.partial_memberships > 0),
                "term" => assert!(actual.partial_terms > 0),
                "field" => assert!(actual.partial_fields > 0),
                _ => assert_eq!(refused.index(), Some(&frame.index)),
            }
            assert_eq!(refused.index().is_some(), phase == "completed");
            assert!(!refused.child_succeeded());
            assert!(refused.recovery_usage().work_bytes > before.work_bytes);
        }
    }
    for panic in [false, true] {
        let expiry = Instant::now() + Duration::from_millis(200);
        let parent = actual_recovery_index_parent(&frame.structural, &compressed, 96, expiry);
        let _hook = Hook;
        TARGET.set(Some(("structural-child-result", 0, panic)));
        let refused = parent
            .prepare_index_until(structural_index_limits())
            .expect_err(
                "actual complete child Result is captured before generic continuation postcheck",
            );
        assert!(OBSERVED.get());
        assert_eq!(refused.has_panic(), panic);
        assert!(refused.child_succeeded());
        assert_eq!(refused.index(), Some(&frame.index));
        assert_eq!(refused.original_expiry(), expiry);
        assert!(refused.parent_timestamps().has_end_source());
    }
}
#[test]
fn recovered_index_malformed_wire_retains_genuine_complete_parent_and_codec_error() {
    let frame = actual_recovery_index_frame();
    let compressed = zstd::bulk::compress(&frame.structural, 1).expect("ordinary original frame");
    let sections = actual_offset_sections(
        &compressed,
        frame.structural.len(),
        96,
        Instant::now() + Duration::from_secs(30),
    );
    let mut wire = crate::structural::STRUCTURAL_BLOCK_MAGIC.to_vec();
    append_section_extent(&mut wire, 96);
    let malformed = &frame.embedded_index[..frame.embedded_index.len() - 1];
    for kind in STRUCTURAL_SECTION_KINDS {
        let lane = if kind == crate::RecoveredStructuralSection::EmbeddedIndex {
            malformed
        } else {
            sections
                .section_bytes(kind)
                .expect("all original other lanes")
        };
        append_section_extent(&mut wire, lane.len());
        wire.extend_from_slice(lane);
    }
    drop(sections);
    let ordinary =
        crate::decode_embedded_frame_index(&wire).expect_err("actual legacy codec failure oracle");
    let compressed =
        zstd::bulk::compress(&wire, 1).expect("actual outer compression of malformed child only");
    let expiry = Instant::now() + Duration::from_secs(30);
    let parent = actual_recovery_index_parent(&wire, &compressed, 96, expiry);
    let before = parent.recovery_usage();
    assert!(parent.has_native_file() && parent.has_end_source());
    let refused = parent
        .prepare_index_until(structural_index_limits())
        .expect_err("real same parser preserves codec refusal");
    assert_eq!(refused.retained_input(), Some(malformed));
    assert_eq!(refused.original_expiry(), expiry);
    assert!(refused.parent_timestamps().has_native_file());
    let mut source = std::error::Error::source(&refused);
    let mut actual = None;
    while let Some(error) = source {
        if let Some(error) = error.downcast_ref::<crate::TelemetryError>() {
            actual = Some(error);
            break;
        }
        source = error.source();
    }
    assert_eq!(
        actual
            .expect("actual original codec error preserved")
            .to_string(),
        ordinary.to_string()
    );
    assert!(refused.recovery_usage().work_bytes > before.work_bytes);
    assert!(refused.index().is_none() && !refused.child_succeeded());
}
