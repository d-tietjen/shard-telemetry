//! True filesystem/native-WAL reopen, replay, query and physical settlement.
use super::*;
use crate::sink::owned_log::replay::ReplayError;

fn plan() -> OwnedLogReplayPlan {
    OwnedLogReplayPlan {
        partitions: vec![partition()],
        limits: OwnedLogReplayLimits {
            max_partitions: 16,
            max_partition_slots: 32,
            max_transactions: 128,
            max_transaction_slots: 256,
            max_payload_bytes: 16 << 20,
            max_declared_bytes: 64 << 20,
            max_entries: 1 << 20,
            max_work_bytes: 256 << 20,
        },
    }
}
fn request(records: usize) -> AppendRequest {
    let sample = original_pack(records, 3);
    let count = u32::from_le_bytes(sample.payload[4..8].try_into().expect("actual pack count"));
    let payload = Bytes::from(
        TelemetryEnvelope::new(
            TelemetrySignal::Logs,
            "tenant-a",
            count,
            &b"route"[..],
            sample.payload,
        )
        .expect("real envelope")
        .encode()
        .expect("real STEL checksum"),
    );
    drop(sample.transient_context);
    AppendRequest {
        request_id: 1,
        topic_id: partition().topic_id,
        partition_id: partition().partition_id,
        record_count: count,
        payload,
        durability: Durability::Leader,
        producer: None,
        atomic_group: None,
        leader_epoch: None,
        extension_context: None,
    }
}
fn seed(config: &EngineConfig, shard: ShardId) {
    let engine = StreamEngine::open(config.clone()).expect("real source native engine");
    engine
        .create_topic(TopicConfig {
            topic_id: partition().topic_id,
            partitions: 1,
            shards: Some(vec![shard]),
        })
        .expect("real selected physical shard");
    engine
        .append(request(12))
        .expect("actual durable source WAL");
}
fn query(factory: &OwnedLogSinkFactory) -> Vec<crate::LogMatch> {
    let worker = worker(factory);
    let control = worker.shared.lock();
    let stripe = control
        .stripe
        .as_ref()
        .expect("accepted real stripe restored");
    let rows = stripe
        .query_checked(&LogQuery::new(partition()).with_field("resource.loki.tenant", "tenant-a"))
        .expect("genuine indexed query");
    assert!(
        stripe
            .query_checked(
                &LogQuery::new(partition()).with_field("resource.loki.tenant", "other-tenant")
            )
            .expect("real different tenant query")
            .is_empty()
    );
    rows
}
fn replay_error(error: &RetainedNativeResumeError) -> &ReplayError {
    error
        .outcomes()
        .iter()
        .find_map(|outcome| match outcome {
            NativeResumeOutcome::Sink { observed, .. } => observed
                .callback_error()?
                .external_error()?
                .downcast_ref::<ReplayError>(),
            _ => None,
        })
        .expect("actual retained first-checkpoint replay error")
}

#[test]
fn actual_nonempty_journal_reopen_adopts_exact_rows_then_replays_genuine_new_wal_suffix() {
    let root = Directory::new();
    let journal = Directory::new();
    let cfg = config(&root.0);
    seed(&cfg, ShardId::new(0));
    let path = journal.0.join("shard-0.journal");
    let mut expected: Option<Vec<crate::LogMatch>> = None;
    let mut expected_ids: Option<Vec<u64>> = None;
    for round in 0..4 {
        if round == 2 {
            let source = StreamEngine::open(cfg.clone())
                .expect("genuine later source engine after owned drain/join");
            source
                .append(request(1))
                .expect("genuine new WAL suffix, no sink-work constructor or validation bypass");
        }
        let before = fs::read(&path).ok();
        with_open_engine(
            cfg.clone(),
            &[(ShardId::new(0), &journal.0)],
            (round > 0).then(plan),
            Duration::from_secs(30),
            |_| {},
            |factory, _engine, actual| {
                actual.expect("actual original-work resume including journal adoption");
                if round != 2
                    && let Some(before) = &before
                {
                    assert_eq!(
                        &fs::read(&path).expect("same journal"),
                        before,
                        "adoption writes no journal bytes"
                    );
                }
                let rows = query(factory);
                let ids = worker(factory)
                    .shared
                    .lock()
                    .stripe
                    .as_ref()
                    .expect("real stripe")
                    .replay_frame_ids(partition());
                if let Some(expected) = &expected {
                    if round == 2 {
                        let old_rows = rows
                            .iter()
                            .filter(|row| row.record.record_ref.offset.get() < 12)
                            .cloned()
                            .collect::<Vec<_>>();
                        assert_eq!(
                            &old_rows, expected,
                            "old exact typed rows survive the genuine new WAL suffix"
                        );
                        assert_eq!(
                            rows.iter()
                                .filter(|row| row.record.record_ref.offset.get() == 12)
                                .count(),
                            1
                        );
                    } else {
                        assert_eq!(
                            &rows, expected,
                            "all typed rows and durable addresses survive true reopen"
                        );
                    }
                }
                if let Some(expected_ids) = &expected_ids {
                    if round == 2 {
                        assert_eq!(
                            &ids[..expected_ids.len()],
                            expected_ids.as_slice(),
                            "real prior frame IDs remain stable"
                        );
                    } else {
                        assert_eq!(
                            &ids, expected_ids,
                            "actual frame IDs are stable without duplicate indexing"
                        );
                    }
                }
                assert_eq!(rows.len(), if round < 2 { 12 } else { 13 });
                if round > 0 {
                    assert!(
                        worker(factory)
                            .shared
                            .adoption
                            .as_ref()
                            .expect("actual opaque receipt")
                            .complete()
                    );
                }
                expected = Some(query(factory));
                expected_ids = Some(
                    worker(factory)
                        .shared
                        .lock()
                        .stripe
                        .as_ref()
                        .expect("real stripe")
                        .replay_frame_ids(partition()),
                );
            },
        );
        // with_open_engine drops actual engine/factory, requests drain and joins
        // its genuine native handles before the next descriptor acquisition.
    }
}

#[test]
fn actual_replay_late_settlement_and_panic_retain_published_checkpoint_without_applied_ack() {
    for panic in [false, true] {
        let root = Directory::new();
        let journal = Directory::new();
        let cfg = config(&root.0);
        seed(&cfg, ShardId::new(0));
        with_open_engine(
            cfg.clone(),
            &[(ShardId::new(0), &journal.0)],
            None,
            Duration::from_secs(30),
            |_| {},
            |_, _, actual| {
                actual.expect("real initial WAL to journal transaction");
            },
        );
        let path = journal.0.join("shard-0.journal");
        let before = fs::read(&path).expect("actual durable rows");
        let entered = Arc::new(AtomicBool::new(false));
        let observed = entered.clone();
        with_open_engine(
            cfg,
            &[(ShardId::new(0), &journal.0)],
            Some(plan()),
            Duration::from_millis(200),
            move |factory| {
                *worker(factory)
                    .shared
                    .probe
                    .lock()
                    .expect("actual worker observer") = Some(Arc::new(move |command, phase| {
                    if phase != "worker-settled" || command.replay_result.is_none() {
                        return;
                    }
                    observed.store(true, Ordering::Release);
                    if panic {
                        panic!(
                            "actual replay worker observation after committed index/application"
                        );
                    }
                    while Instant::now() < command.work.absolute_expiry() {
                        std::thread::park_timeout(Duration::from_millis(1));
                    }
                }));
            },
            |factory, _, actual| {
                let error = actual.expect_err("real late/panicked replay must refuse");
                let retained = replay_error(&error)
                    .retained
                    .lock()
                    .expect("physically settled retained attempt");
                assert!(!retained.accepted);
                let command = retained
                    .current
                    .as_ref()
                    .expect("same retained outside carrier")
                    .command
                    .as_ref()
                    .expect("original physical command");
                assert!(command.settled);
                let done = command
                    .replay_result
                    .as_ref()
                    .expect("saved actual replay outcome")
                    .as_ref()
                    .expect("index/application completed before observation");
                assert_eq!(
                    done.replayed_checkpoint()
                        .expect("actual checkpoint")
                        .next_offset
                        .get(),
                    12
                );
                assert_eq!(
                    done.apply_result(),
                    None,
                    "journal adoption never emits live Applied ACK"
                );
                assert_eq!(command.effect, None);
                assert_eq!(
                    command.panic.is_some(),
                    panic,
                    "actual worker unwind must match the exercised branch"
                );
                let actual_rows = done
                    .publication()
                    .expect("actual retained published owner")
                    .stripe()
                    .query_checked(
                        &LogQuery::new(partition()).with_field("resource.loki.tenant", "tenant-a"),
                    )
                    .expect("query actual retained partial outcome after denial");
                assert_eq!(
                    actual_rows.len(),
                    12,
                    "real query rows survive late failure, not only checkpoint metadata"
                );
                assert!(factory.namespace.denied.load(Ordering::Acquire));
                assert!(
                    worker(factory).shared.lock().exited,
                    "queued physical worker settled before callback refusal"
                );
                assert_eq!(fs::read(&path).expect("retained journal"), before);
            },
        );
        assert!(
            entered.load(Ordering::Acquire),
            "fixture must reach actual replay physical completion"
        );
    }
}

#[test]
fn genuine_cross_shard_duplicate_edges_refuse_before_any_index_or_application_mutation() {
    let roots = [Directory::new(), Directory::new()];
    let journals = [Directory::new(), Directory::new()];
    let mut cfg = config(&roots[0].0);
    cfg.shard_count = 2;
    cfg.virtual_lane_count = 2;
    for index in 0..2 {
        let mut source = cfg.clone();
        source.data_dir = roots[index].0.join("data");
        seed(&source, ShardId::new(index as u32));
        with_open_engine(
            source,
            &[
                (ShardId::new(0), &journals[index].0),
                (ShardId::new(1), &journals[index].0),
            ],
            None,
            Duration::from_secs(30),
            |_| {},
            |_, _, actual| {
                actual.expect("actual source WAL/journal on selected physical shard");
            },
        );
    }
    let paths = [
        journals[0].0.join("shard-0.journal"),
        journals[1].0.join("shard-1.journal"),
    ];
    let before = paths
        .iter()
        .map(|path| fs::read(path).expect("actual independently produced journal"))
        .collect::<Vec<_>>();
    with_open_engine(
        cfg,
        &[
            (ShardId::new(0), &journals[0].0),
            (ShardId::new(1), &journals[1].0),
        ],
        Some(plan()),
        Duration::from_secs(30),
        |_| {},
        |factory, _, actual| {
            let error =
                actual.expect_err("both real journals contain the same logical placement edge");
            let retained = replay_error(&error)
                .retained
                .lock()
                .expect("actual retained validation attempt");
            assert!(matches!(retained.failure, Some(Failure::InvalidSource)));
            assert_eq!(retained.completed, 0);
            assert!(retained.current.is_none());
            assert!(
                factory
                    .namespace
                    .checkpoints
                    .lock()
                    .expect("actual namespace")
                    .is_empty()
            );
            for sink in &factory.sinks {
                let ShardSinkOwner::Owned(worker) = &sink.state else {
                    panic!("real owned source");
                };
                let control = worker.shared.lock();
                let stripe = control
                    .stripe
                    .as_ref()
                    .expect("no stripe was moved into mutation");
                assert!(
                    stripe
                        .query_checked(&LogQuery::new(partition()))
                        .expect("actual unmutated query")
                        .is_empty()
                );
            }
            assert_eq!(
                paths
                    .iter()
                    .map(|path| fs::read(path).expect("original bytes survive"))
                    .collect::<Vec<_>>(),
                before
            );
        },
    );
}

#[test]
fn actual_interleaved_native_placements_merge_across_reversed_physical_sources() {
    let root = Directory::new();
    let journal = Directory::new();
    let mut cfg = config(&root.0);
    cfg.shard_count = 2;
    cfg.virtual_lane_count = 2;
    {
        let source = StreamEngine::open(cfg.clone()).expect("actual two-lane source");
        source
            .create_topic(TopicConfig {
                topic_id: partition().topic_id,
                partitions: 1,
                shards: Some(vec![ShardId::new(0), ShardId::new(1)]),
            })
            .expect("actual rotating native ring");
        let first = source
            .append(request(12))
            .expect("first actual native placement");
        let second = source
            .append(request(12))
            .expect("second actual native placement");
        assert_ne!(
            first.placement.virtual_lane_id, second.placement.virtual_lane_id,
            "the fixture must produce actual distinct physical owners"
        );
    }
    let journals = [
        (ShardId::new(1), journal.0.as_path()),
        (ShardId::new(0), journal.0.as_path()),
    ];
    let paths = [
        journal.0.join("shard-0.journal"),
        journal.0.join("shard-1.journal"),
    ];
    let mut expected: Option<Vec<crate::LogMatch>> = None;
    let mut expected_ids: Option<Vec<(ShardId, Vec<u64>)>> = None;
    for round in 0..4 {
        if round == 2 {
            let source = StreamEngine::open(cfg.clone()).expect("actual later native source");
            let suffix = source
                .append(request(1))
                .expect("actual new global placement suffix");
            assert_eq!(
                suffix.placement.sequence.get(),
                2,
                "the suffix returns to the first physical lane with a genuine sequence gap"
            );
        }
        let before = if round == 0 {
            None
        } else {
            Some(
                paths
                    .iter()
                    .map(|p| fs::read(p).expect("genuine journal bytes"))
                    .collect::<Vec<_>>(),
            )
        };
        with_open_engine_format(
            cfg.clone(),
            &journals,
            Some(plan()),
            crate::sink_journal::JournalFormat::Application,
            Duration::from_secs(30),
            |_| {},
            |factory, _, actual| {
                let report = actual.expect("actual globally ordered cross-shard adoption");
                assert!(
                    report.usage().jobs >= 2,
                    "genuine dispatcher and provider jobs spend SAME work"
                );
                let mut rows = Vec::new();
                let mut ids = Vec::new();
                for sink in &factory.sinks {
                    let ShardSinkOwner::Owned(worker) = &sink.state else {
                        panic!("actual owned worker");
                    };
                    if round > 0 {
                        assert!(
                            !worker.shared.anchor.recovered().is_empty(),
                            "both actual journals contribute"
                        );
                    }
                    let control = worker.shared.lock();
                    ids.push((
                        worker.shard,
                        control
                            .stripe
                            .as_ref()
                            .expect("actual restored stripe")
                            .replay_frame_ids(partition()),
                    ));
                    rows.extend(
                        control
                            .stripe
                            .as_ref()
                            .expect("accepted actual stripe")
                            .query_checked(
                                &LogQuery::new(partition())
                                    .with_field("resource.loki.tenant", "tenant-a"),
                            )
                            .expect("actual indexed rows"),
                    );
                }
                rows.sort_by_key(|row| row.record.record_ref.offset);
                assert_eq!(rows.len(), if round < 2 { 24 } else { 25 });
                if let Some(expected) = &expected {
                    if round == 2 {
                        let original = rows
                            .iter()
                            .filter(|row| row.record.record_ref.offset.get() < 24)
                            .cloned()
                            .collect::<Vec<_>>();
                        assert_eq!(
                            &original, expected,
                            "all real prior rows survive the native suffix"
                        );
                        assert_eq!(
                            rows.iter()
                                .filter(|row| row.record.record_ref.offset.get() == 24)
                                .count(),
                            1
                        );
                    } else {
                        assert_eq!(
                            &rows, expected,
                            "full exact cross-shard rows survive another true reopen"
                        );
                    }
                }
                if let Some(prior_ids) = &expected_ids {
                    for ((shard, ids), (prior_shard, prior)) in ids.iter().zip(prior_ids) {
                        assert_eq!(shard, prior_shard);
                        if round == 2 {
                            assert_eq!(
                                &ids[..prior.len()],
                                prior.as_slice(),
                                "all real prior frame IDs survive"
                            );
                        } else {
                            assert_eq!(ids, prior, "no duplicate frame indexing after true reopen");
                        }
                    }
                }
                expected_ids = Some(ids);
                expected = Some(rows);
                if round != 2
                    && let Some(before) = &before
                {
                    assert_eq!(
                        &paths
                            .iter()
                            .map(|p| fs::read(p).expect("original journal bytes"))
                            .collect::<Vec<_>>(),
                        before
                    );
                }
                for path in &paths {
                    assert!(
                        fs::read(path)
                            .expect("actual application journal")
                            .starts_with(b"STELAPP2")
                    );
                }
            },
        );
    }
}

#[test]
fn application_journal_refuses_unsealed_append_and_both_format_mismatches_without_mutation() {
    let root = Directory::new();
    let journal = Directory::new();
    let legacy = Directory::new();
    let cfg = config(&root.0);
    seed(&cfg, ShardId::new(0));
    let path = journal.0.join("shard-0.journal");
    let reached = Arc::new(AtomicBool::new(false));
    let completed = reached.clone();
    with_open_engine_format(
        cfg.clone(),
        &[(ShardId::new(0), journal.0.as_path())],
        Some(plan()),
        crate::sink_journal::JournalFormat::Application,
        Duration::from_secs(30),
        |factory| {
            let actual = worker(factory);
            let anchor = Arc::downgrade(&actual.shared.anchor);
            let path = path.clone();
            *actual.shared.probe.lock().expect("real phase observer") =
                Some(Arc::new(move |command, phase| {
                    if phase != "worker-settled" {
                        return;
                    }
                    use crate::stripe::LogPublicationInput;
                    let input = command
                        .adopted_result
                        .as_ref()
                        .expect("actual adopted live result")
                        .as_ref()
                        .expect("actual real transaction success")
                        .publication()
                        .expect("actual native input retained")
                        .input()
                        .live()
                        .expect("the real DurableAppend, never reconstructed");
                    let anchor = anchor.upgrade().expect("actual retained physical owner");
                    let before = fs::read(&path).expect("actual physical journal");
                    let error = anchor
                        .append_journal_until(
                            command.expected,
                            std::slice::from_ref(input),
                            command.next,
                            policy()
                                .apply
                                .native()
                                .expect("original bounded policy")
                                .journal,
                            &command.work,
                        )
                        .expect_err("the ordinary native writer has no held-map seal");
                    assert!(
                        error.external_error().is_some(),
                        "the real native refusal remains retained"
                    );
                    assert_eq!(fs::read(&path).expect("actual unchanged file"), before);
                    reached.store(true, Ordering::Release);
                }));
        },
        |factory, _, actual| {
            actual.expect("the genuine sealed global transaction succeeds");
            assert!(completed.load(Ordering::Acquire));
            assert_eq!(query(factory).len(), 12);
        },
    );
    let application_bytes = fs::read(&path).expect("actual application file");
    let mut budget =
        TelemetryRecoveryBudget::new(recovery_limits(), Instant::now() + Duration::from_secs(30))
            .expect("new genuine source-open budget");
    let mismatch = AnchoredSinkJournal::open_until(
        File::open(&journal.0).expect("real parent"),
        ShardId::new(0),
        16 << 20,
        false,
        journal_limits(),
        &mut budget,
    )
    .expect_err("legacy recovery must reject application-format bytes");
    assert!(mismatch.native_error().is_some());
    assert_eq!(
        fs::read(&path).expect("no repair or rewrite"),
        application_bytes
    );
    drop(mismatch); // genuine failed source owner keeps its exclusive descriptor until Drop
    with_open_engine(
        cfg,
        &[(ShardId::new(0), legacy.0.as_path())],
        None,
        Duration::from_secs(30),
        |_| {},
        |factory, _, actual| {
            actual.expect("genuine unchanged v1 native writer");
            assert_eq!(query(factory).len(), 12);
        },
    );
    let legacy_path = legacy.0.join("shard-0.journal");
    let legacy_bytes = fs::read(&legacy_path).expect("actual v1 file");
    let mut budget =
        TelemetryRecoveryBudget::new(recovery_limits(), Instant::now() + Duration::from_secs(30))
            .expect("new genuine source-open budget");
    let mismatch = AnchoredSinkJournal::open_application_until(
        File::open(&legacy.0).expect("real parent"),
        ShardId::new(0),
        16 << 20,
        false,
        journal_limits(),
        &mut budget,
    )
    .expect_err("application recovery must reject legacy-format bytes");
    assert!(mismatch.native_error().is_some());
    assert_eq!(
        fs::read(&legacy_path).expect("no repair or rewrite"),
        legacy_bytes
    );
}

#[test]
fn mixed_journal_domains_refuse_before_worker_registration_or_index_mutation() {
    let legacy = Directory::new();
    let application = Directory::new();
    let expiry = Instant::now() + Duration::from_secs(30);
    let mut budget = TelemetryRecoveryBudget::new(recovery_limits(), expiry)
        .expect("one original source/setup ledger");
    let tasks = Arc::new(Tasks::default());
    let mut sources = Vec::new();
    for (shard, directory, format) in [
        (
            ShardId::new(0),
            legacy.0.as_path(),
            crate::sink_journal::JournalFormat::Legacy,
        ),
        (
            ShardId::new(1),
            application.0.as_path(),
            crate::sink_journal::JournalFormat::Application,
        ),
    ] {
        let stripe =
            LogStripe::prepare_until(shard, StripeConfig::default(), stripe_limits(), &mut budget)
                .expect("genuine bounded stripe producer");
        let journal = match format {
            crate::sink_journal::JournalFormat::Legacy => AnchoredSinkJournal::open_until(
                File::open(directory).expect("real parent"),
                shard,
                16 << 20,
                false,
                journal_limits(),
                &mut budget,
            ),
            crate::sink_journal::JournalFormat::Application => {
                AnchoredSinkJournal::open_application_until(
                    File::open(directory).expect("real parent"),
                    shard,
                    16 << 20,
                    false,
                    journal_limits(),
                    &mut budget,
                )
            }
        }
        .expect("real physical source with its original identity");
        sources.push(OwnedLogShard::new(stripe, journal));
    }
    let paths = [
        legacy.0.join("shard-0.journal"),
        application.0.join("shard-1.journal"),
    ];
    let before = paths
        .iter()
        .map(|p| fs::read(p).expect("actual source file"))
        .collect::<Vec<_>>();
    let error = OwnedLogSinkFactory::new_with_replay_until(
        sources,
        policy(),
        plan(),
        tasks.clone(),
        &mut budget,
    )
    .expect_err("mixed producer domains have no global adoption contract");
    assert!(matches!(error.cause, Failure::InvalidSource));
    assert_eq!(tasks.entered.load(Ordering::Acquire), 0);
    assert!(error.retained.sinks.is_empty());
    assert!(error.retained.checkpoints.is_none());
    assert_eq!(
        paths
            .iter()
            .map(|p| fs::read(p).expect("retained unchanged source"))
            .collect::<Vec<_>>(),
        before
    );
}
