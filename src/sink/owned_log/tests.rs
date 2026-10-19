//! Actual bounded factory/registered worker through full native WAL resume.
//! The task registry uses real native threads; no Prepared host, control grant
//! or serving qualification is fabricated.
#[path = "replay/tests.rs"]
mod journal_replay;
use super::*;
use crate::CompressionCohortId;
use crate::ingest_pack::{PreparedIngestPack, prepare_ingest_pack};
use crate::{AnchoredJournalLimits, LogStripeSetupLimits};
use crate::{TelemetryRecoveryBudget, TelemetryRecoveryLimits};
use shard_stream_core::{
    DirectoryOwnership, LogicalPartitionId, NativeDirectoryBudget, NativeDirectoryLimits, ShardId,
    TopicPartition,
};
use shard_stream_engine::{
    DurableAppendSink, DurableAppendSinkFactory, DurableSinkApply, DurableSinkConfig,
    DurableSinkOptions, DurableSinkRecoveryMode, EngineConfig, EngineHandle, NativeFetchLimits,
    NativePrimaryRecovery, NativeRecoveryCensus, NativeRecoveryLimits, NativeResumeGcPolicy,
    NativeResumeLimits, PinnedShardDirectoryLimits, PinnedShardGcChecks, PinnedShardGcLimits,
    PinnedStoreOpenChecks, PinnedStoreOpenLimits, ShardDirectoryCheckFailure, ShardDirectoryChecks,
    ShardGuardAcquireLimits, StreamEngine, TopicConfig,
};
use shard_stream_engine::{NativeResumeOutcome, NativeResumeReport, RetainedNativeResumeError};
use shard_stream_protocol::{AppendRequest, Durability};
use std::fs;
use std::fs::File;
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

#[derive(Default)]
struct Tasks {
    handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
    entered: AtomicUsize,
    exited: Arc<AtomicUsize>,
}
impl TelemetryTaskRegistry for Tasks {
    fn spawn_until(
        &self,
        name: &str,
        stack_bytes: usize,
        expiry: Instant,
        job: Box<dyn FnOnce() + Send>,
    ) -> io::Result<()> {
        if Instant::now() >= expiry {
            return Err(io::Error::other("actual initiating task expiry"));
        }
        let exited = self.exited.clone();
        let handle = std::thread::Builder::new()
            .name(name.to_owned())
            .stack_size(stack_bytes)
            .spawn(move || {
                job();
                exited.fetch_add(1, Ordering::AcqRel);
            })?;
        self.handles
            .lock()
            .expect("actual task custody")
            .push(handle);
        self.entered.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}
impl Tasks {
    fn join_all(&self) {
        let handles =
            std::mem::take(&mut *self.handles.lock().expect("take actual native handles"));
        for h in handles {
            h.join().expect("actual native/TLS join");
        }
        assert_eq!(
            self.entered.load(Ordering::Acquire),
            self.exited.load(Ordering::Acquire)
        );
    }
}
impl Drop for Tasks {
    fn drop(&mut self) {
        self.join_all();
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
        max_work_bytes: 512 << 20,
    }
}
fn stripe_limits() -> LogStripeSetupLimits {
    LogStripeSetupLimits {
        max_dictionary_cache_bytes: 32 << 20,
        max_locality_slots: 32,
        max_cache_slots: 4096,
        max_native_context_bytes: 256 << 10,
        max_retained_bytes: 4 << 20,
        max_work_bytes: 16 << 20,
    }
}
fn journal_limits() -> AnchoredJournalLimits {
    AnchoredJournalLimits {
        max_file_bytes: 16 << 20,
        max_frame_bytes: 8 << 20,
        max_transactions: 128,
        max_appends: 4096,
        max_payload_bytes: 8 << 20,
        max_io_attempts: 4096,
        max_io_bytes: 64 << 20,
    }
}
fn policy() -> OwnedLogFactoryLimits {
    OwnedLogFactoryLimits {
        max_shards: 4,
        max_source_slots: 4,
        max_pending_commands: 1,
        thread_stack_bytes: 2 << 20,
        max_thread_name_bytes: 64,
        max_retained_bytes: 16 << 20,
        max_work_bytes: 64 << 20,
        apply: OwnedLogApplyLimits {
            max_input_bytes: 4 << 20,
            max_records: 1 << 16,
            max_groups: 64,
            max_partitions: 128,
            max_appends_per_partition: 128,
            max_index_items: 1 << 20,
            max_output_bytes: 16 << 20,
            max_checkpoint_slots: 128,
            max_cache_slots: 128,
            max_cache_name_bytes: 1 << 20,
            max_cache_partition_slots: 1 << 16,
            max_declared_bytes: 64 << 20,
            max_entries: 1 << 20,
            max_work_bytes: 256 << 20,
            max_journal_io_attempts: 4096,
            max_journal_write_bytes: 32 << 20,
            max_journal_write_chunk_bytes: 16 * 1024,
            decompression: Some(OwnedLogDecompressionLimits {
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
            }),
        },
    }
}
fn worker(factory: &OwnedLogSinkFactory) -> Arc<OwnedLogWorker> {
    match &factory.sinks[0].state {
        ShardSinkOwner::Owned(w) => w.clone(),
        _ => panic!("actual configured owned sink"),
    }
}
fn with_engine(
    duration: Duration,
    configure: impl FnOnce(&Arc<OwnedLogSinkFactory>),
    inspect: impl FnOnce(
        &Arc<OwnedLogSinkFactory>,
        Result<NativeResumeReport, RetainedNativeResumeError>,
    ),
) {
    let sample = original_pack(12, 3);
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
    let journal_root = Directory::new();
    with_open_engine(
        cfg,
        &[(ShardId::new(0), &journal_root.0)],
        None,
        duration,
        configure,
        |factory, _engine, actual| inspect(factory, actual),
    );
}

fn with_open_engine(
    cfg: EngineConfig,
    journals: &[(ShardId, &Path)],
    replay: Option<OwnedLogReplayPlan>,
    duration: Duration,
    configure: impl FnOnce(&Arc<OwnedLogSinkFactory>),
    inspect: impl FnOnce(
        &Arc<OwnedLogSinkFactory>,
        &EngineHandle,
        Result<NativeResumeReport, RetainedNativeResumeError>,
    ),
) {
    with_open_engine_format(
        cfg,
        journals,
        replay,
        crate::sink_journal::JournalFormat::Legacy,
        duration,
        configure,
        inspect,
    );
}

fn with_open_engine_format(
    cfg: EngineConfig,
    journals: &[(ShardId, &Path)],
    replay: Option<OwnedLogReplayPlan>,
    format: crate::sink_journal::JournalFormat,
    duration: Duration,
    configure: impl FnOnce(&Arc<OwnedLogSinkFactory>),
    inspect: impl FnOnce(
        &Arc<OwnedLogSinkFactory>,
        &EngineHandle,
        Result<NativeResumeReport, RetainedNativeResumeError>,
    ),
) {
    let configured_physical_shards = cfg.shard_count;
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
    let census = NativeRecoveryCensus::read_for_engine_with_primary(
        cfg,
        owner,
        None,
        NativeRecoveryLimits {
            // Preserve the original 128-file envelope per declared physical
            // owner. Native restore and final engine admission reserve for
            // that topology, independently of later native resume work.
            max_files: usize::try_from(configured_physical_shards)
                .expect("finite fixture physical shard count")
                .checked_mul(128)
                .expect("finite per-physical-shard fixture file allowance"),
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
    .expect("actual primary native census");
    // Immutable counts from actual retained facts; no path/payload clones or
    // replacement work/limits enter the real producer.
    let source_counts = census.sources().fold([0usize; 3], |mut counts, source| {
        counts[0] += 1;
        match source.kind {
            shard_stream_engine::RecoverySourceKind::Coordinator => counts[1] += 1,
            shard_stream_engine::RecoverySourceKind::PrimaryPack => counts[2] += 1,
            _ => {}
        }
        counts
    });
    let storage = census.restore_storage().expect("actual consuming storage");

    let tasks = Arc::new(Tasks::default());
    let mut startup = TelemetryRecoveryBudget::new(recovery_limits(), expiry)
        .expect("one genuine original setup ledger");
    let mut sources = Vec::new();
    for &(shard, journal_root) in journals {
        let prepared = LogStripe::prepare_until(
            shard,
            StripeConfig::default(),
            stripe_limits(),
            &mut startup,
        )
        .expect("actual bounded native stripe setup");
        let journal = match format {
            crate::sink_journal::JournalFormat::Legacy => AnchoredSinkJournal::open_until(
                File::open(journal_root).expect("actual owned directory descriptor"),
                shard,
                16 << 20,
                false,
                journal_limits(),
                &mut startup,
            ),
            crate::sink_journal::JournalFormat::Application => {
                AnchoredSinkJournal::open_application_until(
                    File::open(journal_root)
                        .expect("actual owned application directory descriptor"),
                    shard,
                    16 << 20,
                    false,
                    journal_limits(),
                    &mut startup,
                )
            }
        }
        .expect("actual explicitly selected anchored journal reopen");
        sources.push(OwnedLogShard::new(prepared, journal));
    }
    let factory = Arc::new(
        match replay {
            Some(plan) => OwnedLogSinkFactory::new_with_replay_until(
                sources,
                policy(),
                plan,
                tasks.clone(),
                &mut startup,
            ),
            None => OwnedLogSinkFactory::new_until(sources, policy(), tasks.clone(), &mut startup),
        }
        .expect("actual same-ledger registered factory"),
    );
    configure(&factory);
    let sink = DurableSinkConfig::new(factory.clone()).with_options(DurableSinkOptions {
        recovery_mode: DurableSinkRecoveryMode::Blocking,
        worker_count: 1,
        max_pending_items: 32,
        max_pending_bytes: Some(2 << 20),
        ..DurableSinkOptions::default()
    });
    let before_engine_usage = storage.usage();
    let original_engine_expiry = storage.absolute_expiry();
    let before_directory_usage = budget.usage();
    let engine = EngineHandle::from_recovered(Arc::new(
        StreamEngine::open_recovered_storage(storage, None, None, None, Some(sink)).unwrap_or_else(
            |error| {
                panic!(
                    "actual recovered engine consumes actual factory: {error:?}; \
                     configured_physical_shards={configured_physical_shards}, \
                     source_counts[all,coordinator,primary_pack]={source_counts:?}; \
                     native_recovery_before={before_engine_usage:?}, refused={:?}; \
                     directory_before={before_directory_usage:?}, after={:?}; \
                     worker_start={:?}, worker_budget={:?}, worker_checkpoint={:?}; \
                     gc_outcomes={}, gc_allocation={:?}, gc_receive={:?}; \
                     factory_open_calls={}, original_expiry_unchanged={}",
                    error.usage(),
                    budget.usage(),
                    error.worker_start_error(),
                    error.worker_start_budget_error(),
                    error.worker_start_checkpoint_error(),
                    error.gc_outcomes().len(),
                    error.gc_allocation_error(),
                    error.gc_receive_error(),
                    factory.open_calls.load(Ordering::Acquire),
                    error.absolute_expiry() == original_engine_expiry,
                );
            },
        ),
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
        engine.recovered_partition_sources(),
        NativeResumeGcPolicy {
            budget,
            limits: gc,
            checks,
        },
        limits,
        Instant::now() + duration,
    );
    inspect(&factory, &engine, actual);
    for sink in &factory.sinks {
        if let ShardSinkOwner::Owned(worker) = &sink.state {
            worker
                .shared
                .probe
                .lock()
                .expect("clear component observer")
                .take();
        }
    }
    // Keep all actual factory/engine/source roots while registered native/TLS
    // joins complete. Worker-body exit alone is not the host join contract.
    factory.begin_draining();
    tasks.join_all();
    drop(engine);
    drop(factory);
}

fn carrier(error: &RetainedNativeResumeError) -> &Carrier {
    error
        .outcomes()
        .iter()
        .find_map(|o| match o {
            NativeResumeOutcome::Sink { observed, .. } => observed
                .callback_error()?
                .external_error()?
                .downcast_ref::<Carrier>(),
            _ => None,
        })
        .expect("actual retained external callback owner")
}
#[test]
fn actual_factory_native_dispatcher_and_registered_worker_complete_full_log_resume() {
    with_engine(
        Duration::from_secs(30),
        |_| {},
        |factory, actual| {
            let report = actual.expect("genuine full native resume, no fixture callback refusal");
            assert!(report.outcomes().iter().any(|o|matches!(o,NativeResumeOutcome::Sink{observed,..} if observed.apply_result()==Some(DurableSinkApply::Applied))));
            assert!(
                report.usage().jobs >= 2,
                "actual native dispatch plus genuine provider queue"
            );
            let w = worker(factory);
            let c = w.shared.lock();
            assert!(c.started && !c.exited && !c.stop && c.pending.is_none());
            let stripe = c
                .stripe
                .as_ref()
                .expect("actual mutable stripe returned only after acceptance");
            let rows = stripe
                .query_checked(
                    &LogQuery::new(partition()).with_field("resource.loki.tenant", "tenant-a"),
                )
                .expect("actual unchanged query after native dispatcher applied");
            assert_eq!(rows.len(), 12);
            let cp = w
                .shared
                .namespace
                .checkpoints
                .lock()
                .expect("same application map")[&partition()];
            assert_eq!(cp.next_offset.get(), 12);
            assert!(!w.shared.denied());
            assert!(
                factory.load_checkpoint(partition()).is_err(),
                "ordinary route is not used as a strict fallback"
            );
        },
    );
}
#[test]
fn entered_worker_and_callback_late_or_panic_keep_exact_complete_actual_outcomes() {
    for phase in ["copied", "queued", "settled", "worker-settled"] {
        for panic in [false, true] {
            with_engine(
                Duration::from_secs(3),
                move |factory| {
                    *worker(factory)
                        .shared
                        .probe
                        .lock()
                        .expect("actual scoped observer") =
                        Some(Arc::new(move |command, actual| {
                            if actual != phase {
                                return;
                            }
                            if panic {
                                panic!("after actual owned callback/worker capture");
                            }
                            while Instant::now() < command.work.absolute_expiry() {
                                std::thread::park_timeout(Duration::from_millis(1));
                            }
                        }));
                },
                move |factory, actual| {
                    let error = actual.expect_err("real entered work observes original E/unwind");
                    let callback = carrier(&error);
                    let command = callback
                        .command
                        .as_ref()
                        .expect("same original command retained");
                    let w = worker(factory);
                    assert!(w.shared.denied());
                    if phase == "settled" || phase == "worker-settled" {
                        assert!(command.settled);
                        let done = command
                            .result
                            .as_ref()
                            .expect("complete actual native transaction result")
                            .as_ref()
                            .expect("actual journal/index/app success preceded observation");
                        assert_eq!(done.apply_result(), Some(DurableSinkApply::Applied));
                        assert!(matches!(
                            done.journal_outcome()
                                .expect("real durable native result")
                                .kind,
                            crate::sink_journal::JournalAppendKind::Committed(_)
                        ));
                        assert_eq!(command.effect, Some(DurableSinkApply::Applied));
                        assert!(
                            w.shared.lock().exited,
                            "entered command worker body settles before error returns"
                        );
                    } else if phase == "queued" {
                        assert!(
                            command.settled && command.result.as_ref().is_some_and(|r| r.is_err()),
                            "accepted queue was physically processed/refused, never abandoned"
                        );
                        assert!(w.shared.lock().exited);
                    } else {
                        assert!(
                            command.input.is_some() && command.result.is_none(),
                            "prequeue clone remains original retained owned input"
                        );
                    }
                    assert_eq!(callback.panic.is_some() || command.panic.is_some(), panic);
                },
            );
        }
    }
}
#[test]
fn actual_queue_busy_denial_never_reenters_or_waits_on_its_own_worker_mutex() {
    with_engine(
        Duration::from_secs(30),
        |factory| {
            let weak = Arc::downgrade(&factory.sinks[0]);
            *worker(factory)
                .shared
                .probe
                .lock()
                .expect("actual observer") = Some(Arc::new(move |command, phase| {
                if phase != "worker-settled" {
                    return;
                }
                let sink = weak.upgrade().expect("original live sink");
                let ShardSinkOwner::Owned(w) = &sink.state else {
                    panic!("owned sink");
                };
                let input = command
                    .result
                    .as_ref()
                    .expect("same actual completed result")
                    .as_ref()
                    .expect("actual transaction success")
                    .publication()
                    .expect("actual published native append")
                    .input();
                assert!(w.shared.control.try_lock().is_err());
                let error = sink
                    .apply_until(
                        command.expected,
                        std::slice::from_ref(input),
                        command.next,
                        &command.work,
                    )
                    .expect_err("actual reentrant slot contention must refuse finitely");
                let retained = error
                    .external_error()
                    .expect("prepaid actual callback carrier")
                    .downcast_ref::<Carrier>()
                    .expect("actual owned callback");
                assert!(matches!(retained.failure, Some(Failure::Busy)));
                assert_eq!(
                    retained
                        .command
                        .as_ref()
                        .expect("same unqueued copy")
                        .input
                        .as_ref()
                        .expect("actual copied native source")
                        .payload
                        .as_ptr(),
                    input.payload.as_ptr()
                );
            }));
        },
        |factory, actual| {
            let error = actual.expect_err("same actual source denial after native result");
            let c = carrier(&error);
            assert!(
                c.panic.is_none(),
                "a caught callback assertion must fail this fixture"
            );
            assert!(
                c.command.as_ref().expect("real command").panic.is_none(),
                "a caught Busy probe assertion must fail this fixture"
            );
            assert_eq!(
                c.command.as_ref().expect("real command").effect,
                Some(DurableSinkApply::Applied)
            );
            assert!(worker(factory).shared.lock().exited);
        },
    );
}

fn setup_sources(
    count: usize,
    budget: &mut TelemetryRecoveryBudget,
) -> (Vec<Directory>, Vec<OwnedLogShard>) {
    let mut roots = Vec::new();
    let mut sources = Vec::new();
    for i in 0..count {
        let root = Directory::new();
        let shard = ShardId::new(i as u32);
        let prepared =
            LogStripe::prepare_until(shard, StripeConfig::default(), stripe_limits(), budget)
                .expect("actual bounded stripe");
        let journal = AnchoredSinkJournal::open_until(
            File::open(&root.0).expect("actual descriptor"),
            shard,
            16 << 20,
            false,
            journal_limits(),
            budget,
        )
        .expect("actual bounded anchored journal");
        roots.push(root);
        sources.push(OwnedLogShard::new(prepared, journal));
    }
    (roots, sources)
}
#[test]
fn setup_rejects_actual_source_mismatch_and_capacity_without_starting_tasks() {
    for mode in 0..2 {
        let mut budget = TelemetryRecoveryBudget::new(
            recovery_limits(),
            Instant::now() + Duration::from_secs(30),
        )
        .expect("actual original setup ledger");
        let (roots, mut sources) = setup_sources(2, &mut budget);
        let tasks = Arc::new(Tasks::default());
        let mut limits = policy();
        if mode == 0 {
            limits.max_source_slots = 1;
        } else {
            // Original independent actual producer owners remain complete; mismatching
            // their genuine IDs is refused, without forging a prepared stripe.
            let second = sources.pop().expect("second real owner");
            sources[0].journal = second.journal;
        }
        let error = OwnedLogSinkFactory::new_until(sources, limits, tasks.clone(), &mut budget)
            .expect_err("actual source eligibility/capacity");
        assert_eq!(tasks.entered.load(Ordering::Acquire), 0);
        assert!(error.retained.sources.iter().all(|s| s.journal.is_denied()));
        drop(error);
        drop(roots);
    }
}
struct RejectTasks;
impl TelemetryTaskRegistry for RejectTasks {
    fn spawn_until(
        &self,
        _: &str,
        _: usize,
        _: Instant,
        _: Box<dyn FnOnce() + Send>,
    ) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "explicit component registry admission policy",
        ))
    }
}
#[test]
fn actual_registration_refusal_retains_sources_namespace_and_unentered_thread_evidence() {
    let mut budget =
        TelemetryRecoveryBudget::new(recovery_limits(), Instant::now() + Duration::from_secs(30))
            .expect("original setup ledger");
    let (roots, sources) = setup_sources(1, &mut budget);
    let error =
        OwnedLogSinkFactory::new_until(sources, policy(), Arc::new(RejectTasks), &mut budget)
            .expect_err("real registry policy refusal");
    assert!(matches!(error.cause, Failure::Registration));
    assert_eq!(
        error.retained.registrations[0]
            .as_ref()
            .expect_err("actual registry result")
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    let w = error
        .retained
        .current
        .as_ref()
        .expect("original physical source owner");
    assert!(Arc::ptr_eq(
        error
            .retained
            .current_shared
            .as_ref()
            .expect("staged real shared owner"),
        &w.shared
    ));
    assert!(
        error
            .retained
            .name
            .as_ref()
            .expect("actual staged worker name")
            .starts_with("shard-telemetry-owned-log-")
    );
    assert!(!w.registered.load(Ordering::Acquire) && !w.shared.lock().started);
    assert!(w.shared.anchor.is_denied() && w.shared.lock().stripe.is_some());
    drop(error);
    drop(roots);
}
struct LateTasks(Tasks);
impl TelemetryTaskRegistry for LateTasks {
    fn spawn_until(
        &self,
        name: &str,
        stack: usize,
        expiry: Instant,
        job: Box<dyn FnOnce() + Send>,
    ) -> io::Result<()> {
        self.0.spawn_until(name, stack, expiry, job)?;
        while Instant::now() < expiry {
            std::thread::park_timeout(Duration::from_millis(1));
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "actual post-entry original registry deadline",
        ))
    }
}
#[test]
fn late_registered_native_task_survives_actual_error_until_closed_and_physically_joined() {
    let expiry = Instant::now() + Duration::from_secs(2);
    let mut budget =
        TelemetryRecoveryBudget::new(recovery_limits(), expiry).expect("real original E");
    let (roots, sources) = setup_sources(1, &mut budget);
    let tasks = Arc::new(LateTasks(Tasks::default()));
    let error = OwnedLogSinkFactory::new_until(sources, policy(), tasks.clone(), &mut budget)
        .expect_err("actual native entry then late registry result");
    assert!(matches!(error.cause, Failure::Registration));
    assert_eq!(tasks.0.entered.load(Ordering::Acquire), 1);
    assert_eq!(
        error.retained.registrations[0]
            .as_ref()
            .expect_err("actual late returned result")
            .kind(),
        io::ErrorKind::TimedOut
    );
    assert!(
        error
            .retained
            .current
            .as_ref()
            .expect("retained actual source owner")
            .shared
            .anchor
            .is_denied()
    );
    drop(error);
    tasks.0.join_all();
    drop(tasks);
    drop(roots);
}
#[test]
fn original_setup_ledger_exhaustion_preserves_every_source_without_reset_or_task_entry() {
    let mut budget =
        TelemetryRecoveryBudget::new(recovery_limits(), Instant::now() + Duration::from_secs(30))
            .expect("original setup ledger");
    let (roots, sources) = setup_sources(1, &mut budget);
    let tasks = Arc::new(Tasks::default());
    let spent = budget.usage();
    budget
        .charge(TelemetryRecoveryUsage {
            retained_bytes: recovery_limits().max_retained_bytes - spent.retained_bytes,
            ..Default::default()
        })
        .expect("other composing work consumes SAME allowance");
    let error = OwnedLogSinkFactory::new_until(sources, policy(), tasks.clone(), &mut budget)
        .expect_err("no reset after prior genuine producers");
    assert!(matches!(
        error.cause,
        Failure::Recovery(TelemetryRecoveryError::Limits)
    ));
    assert_eq!(tasks.entered.load(Ordering::Acquire), 0);
    assert!(error.retained.sources[0].stripe.is_some());
    drop(error);
    drop(roots);
}

#[test]
fn explicit_drain_wakes_and_native_joins_workers_while_factory_remains_owned() {
    let mut budget =
        TelemetryRecoveryBudget::new(recovery_limits(), Instant::now() + Duration::from_secs(30))
            .expect("same original producer ledger");
    let (roots, sources) = setup_sources(2, &mut budget);
    let tasks = Arc::new(Tasks::default());
    let factory = Arc::new(
        OwnedLogSinkFactory::new_until(sources, policy(), tasks.clone(), &mut budget)
            .expect("actual prepared registered factory"),
    );
    factory.begin_draining();
    tasks.join_all();
    assert_eq!(tasks.exited.load(Ordering::Acquire), 2);
    for s in &factory.sinks {
        let ShardSinkOwner::Owned(w) = &s.state else {
            panic!("owned worker")
        };
        assert!(w.shared.lock().exited && w.shared.anchor.is_denied());
        assert!(
            w.shared.lock().stripe.is_some(),
            "same physical source still held after native join"
        );
    }
    assert!(factory.open_shard(ShardId::new(0)).is_err());
    drop(factory);
    drop(roots);
}
