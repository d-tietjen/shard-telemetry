use super::*;
use std::time::Duration;

fn test_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shard-telemetry-usage-{name}-{}-{nonce}.ledger",
        std::process::id(),
    ))
}

fn timestamp(year: i32, month: u32) -> u64 {
    use chrono::TimeZone;
    u64::try_from(
        Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
            .single()
            .expect("valid test month")
            .timestamp(),
    )
    .expect("positive timestamp")
}

#[test]
fn one_hundred_features_and_twelve_months_stay_far_below_one_mib() {
    let path = test_path("annual");
    let features = (0..100)
        .map(|index| format!("feature-{index:03}"))
        .collect::<Vec<_>>();
    let ledger = EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(
        &path,
        features.iter().map(String::as_str),
    ))
    .expect("ledger opens");
    for month in 1..=12 {
        ledger
            .record_batch_at(
                timestamp(2026, month),
                3_600,
                features.iter().map(|feature| (feature.as_str(), 1_u64)),
            )
            .expect("month checkpoints");
    }
    let health = ledger.health().expect("health");
    assert!(health.file_bytes < 64 * 1024, "{}", health.file_bytes);
    assert!(health.file_bytes <= health.max_file_bytes);
    assert!(health.allocated_file_bytes >= health.file_bytes);
    assert!(health.allocated_file_bytes <= health.max_file_bytes);
    assert_eq!(health.monthly_bucket_count, 12);
    assert!(health.compressed);
    let snapshot = ledger.snapshot().expect("snapshot");
    assert_eq!(snapshot.active_seconds, 12 * 3_600);
    assert!(snapshot.features.iter().all(|feature| feature.count == 12));
    drop(ledger);

    let recovered = EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(
        &path,
        features.iter().map(String::as_str),
    ))
    .expect("ledger recovers");
    assert_eq!(recovered.snapshot().expect("snapshot"), snapshot);
    drop(recovered);
    std::fs::remove_file(path).expect("cleanup");
}

#[test]
fn quota_and_registry_are_hard_construction_time_bounds() {
    let quota_path = test_path("quota");
    let features = (0..600)
        .map(|index| format!("feature-{index:03}"))
        .collect::<Vec<_>>();
    let result = EmbeddedUsageLedger::open(
        EmbeddedUsageLedgerConfig::new(&quota_path, features.iter().map(String::as_str))
            .with_max_file_bytes(32 * 1024),
    );
    assert!(result.is_err());
    assert!(!quota_path.exists(), "quota failure must not create a file");

    let path = test_path("registry");
    let ledger =
        EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search", "export"]))
            .expect("ledger opens");
    assert!(ledger.record_feature("unknown", 1).is_err());
    assert_eq!(
        ledger
            .snapshot()
            .expect("snapshot")
            .features
            .iter()
            .map(|feature| feature.count)
            .sum::<u64>(),
        0,
    );
    assert_eq!(
        ledger.health().expect("health").rejected_unknown_features,
        1,
    );
    let oversized_unknown = "x".repeat(MAX_FEATURE_ID_BYTES + 1);
    let error = ledger
        .record_feature(&oversized_unknown, 1)
        .expect_err("oversized unknown ID is rejected");
    assert!(!error.to_string().contains(&oversized_unknown));
    drop(ledger);
    std::fs::remove_file(path).expect("cleanup");
}

#[test]
fn overflow_policy_and_rolling_months_are_deterministic() {
    let path = test_path("overflow");
    let ledger = EmbeddedUsageLedger::open(
        EmbeddedUsageLedgerConfig::new(&path, ["search"])
            .with_monthly_buckets(2)
            .with_unknown_feature_policy(UnknownFeaturePolicy::AccumulateOverflow),
    )
    .expect("ledger opens");
    for month in 1..=3 {
        ledger
            .record_batch_at(timestamp(2026, month), 10, [("future-feature", 2)])
            .expect("usage checkpoints");
    }
    ledger
        .record_feature_at("search", 5, timestamp(2026, 1))
        .expect("old lifetime usage is retained");
    let snapshot = ledger.snapshot().expect("snapshot");
    assert_eq!(snapshot.active_seconds, 30);
    assert_eq!(snapshot.overflow_feature_events, 6);
    assert_eq!(snapshot.features[0].count, 5);
    assert_eq!(
        snapshot
            .months
            .iter()
            .map(|month| (month.year, month.month))
            .collect::<Vec<_>>(),
        vec![(2026, 2), (2026, 3)],
    );
    assert_eq!(
        ledger
            .health()
            .expect("health")
            .monthly_updates_outside_window,
        1,
    );
    drop(ledger);
    std::fs::remove_file(path).expect("cleanup");
}

#[test]
fn recovery_uses_previous_generation_when_newest_slot_is_torn() {
    let path = test_path("torn");
    let ledger = EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search"]))
        .expect("ledger opens");
    ledger.record_feature("search", 2).expect("first update");
    let previous = ledger.snapshot().expect("previous snapshot");
    ledger.record_feature("search", 3).expect("second update");
    let (slot_bytes, active_slot) = {
        let inner = ledger.inner.lock().expect("lock");
        (ledger.slot_bytes, inner.active_slot)
    };
    drop(ledger);

    let mut file = OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open for corruption");
    let offset = slot_bytes
        .saturating_mul(u64::try_from(active_slot).expect("slot"))
        .saturating_add(64);
    file.seek(SeekFrom::Start(offset)).expect("seek");
    file.write_all(&[0, 0, 0, 0]).expect("tear header CRC");
    file.sync_all().expect("sync corruption");
    drop(file);

    let recovered = EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search"]))
        .expect("previous generation recovers");
    assert_eq!(recovered.snapshot().expect("snapshot"), previous);
    drop(recovered);
    std::fs::remove_file(path).expect("cleanup");
}

#[test]
fn recovery_rejects_checksum_valid_oversized_collection_headers() {
    let path = test_path("bounded-decode");
    let ledger = EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search"]))
        .expect("ledger opens");
    ledger.record_feature("search", 2).expect("first update");
    let previous = ledger.snapshot().expect("previous snapshot");
    ledger.record_feature("search", 3).expect("second update");
    let (slot_bytes, active_slot, generation, fingerprint) = {
        let inner = ledger.inner.lock().expect("lock");
        (
            ledger.slot_bytes,
            inner.active_slot,
            inner.generation,
            ledger.config_fingerprint,
        )
    };
    drop(ledger);

    // A valid v1 header and payload checksum must not let MessagePack's
    // untrusted collection length drive an unbounded allocation.
    let malicious = [0x95, 0xdd, 0xff, 0xff, 0xff, 0xff];
    let header = encode_header(
        CODEC_RAW,
        generation + 1,
        malicious.len(),
        malicious.len(),
        &fingerprint,
        crc32c::crc32c(&malicious),
    )
    .expect("header");
    let slot_offset = slot_bytes.saturating_mul(u64::try_from(active_slot).expect("slot"));
    let mut file = OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open for corruption");
    file.seek(SeekFrom::Start(
        slot_offset + u64::try_from(HEADER_BYTES).expect("header bytes"),
    ))
    .expect("seek payload");
    file.write_all(&malicious).expect("write payload");
    file.seek(SeekFrom::Start(slot_offset))
        .expect("seek header");
    file.write_all(&header).expect("write header");
    file.sync_all().expect("sync malicious slot");
    drop(file);

    let recovered = EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search"]))
        .expect("bounded decoder falls back to previous generation");
    assert_eq!(recovered.snapshot().expect("snapshot"), previous);
    drop(recovered);
    std::fs::remove_file(path).expect("cleanup");
}

#[test]
fn closed_file_backup_restores_the_exact_snapshot() {
    let path = test_path("backup-source");
    let backup = test_path("backup-restored");
    let config = EmbeddedUsageLedgerConfig::new(&path, ["search", "export"]);
    let ledger = EmbeddedUsageLedger::open(config).expect("ledger opens");
    ledger
        .record_batch_at(timestamp(2026, 8), 600, [("search", 7), ("export", 3)])
        .expect("usage checkpoints");
    let expected = ledger.snapshot().expect("snapshot");
    drop(ledger);

    std::fs::copy(&path, &backup).expect("closed ledger backup copies");
    let restored = EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(
        &backup,
        ["search", "export"],
    ))
    .expect("backup restores");
    assert_eq!(restored.snapshot().expect("restored snapshot"), expected);
    drop(restored);
    std::fs::remove_file(path).expect("source cleanup");
    std::fs::remove_file(backup).expect("backup cleanup");
}

#[test]
fn file_lock_and_configuration_fingerprint_prevent_unsafe_reuse() {
    let path = test_path("exclusive");
    let ledger =
        EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search", "export"]))
            .expect("ledger opens");
    assert!(
        EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search", "export"],))
            .is_err()
    );
    drop(ledger);
    assert!(
        EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search", "changed"],))
            .is_err()
    );
    std::fs::remove_file(path).expect("cleanup");
}

#[cfg(unix)]
#[test]
fn ledger_path_must_not_be_a_symbolic_link() {
    use std::os::unix::fs::symlink;

    let target = test_path("symlink-target");
    let link = test_path("symlink-link");
    std::fs::write(&target, b"host-owned-data").expect("target");
    symlink(&target, &link).expect("symlink");

    assert!(EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&link, ["search"])).is_err());
    assert_eq!(
        std::fs::read(&target).expect("target remains readable"),
        b"host-owned-data"
    );
    std::fs::remove_file(link).expect("link cleanup");
    std::fs::remove_file(target).expect("target cleanup");
}

#[test]
fn zero_updates_do_not_rewrite_or_advance_generation() {
    let path = test_path("zero");
    let ledger = EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search"]))
        .expect("ledger opens");
    let generation = ledger.health().expect("health").generation;
    ledger.record_feature("search", 0).expect("zero is a no-op");
    assert_eq!(ledger.health().expect("health").generation, generation);
    drop(ledger);
    std::fs::remove_file(path).expect("cleanup");
}

#[test]
fn lifetime_only_ledgers_do_not_interpret_event_timestamps() {
    let path = test_path("lifetime-only");
    let ledger = EmbeddedUsageLedger::open(
        EmbeddedUsageLedgerConfig::new(&path, ["search"]).with_monthly_buckets(0),
    )
    .expect("ledger opens");
    ledger
        .record_feature_at("search", 1, u64::MAX)
        .expect("lifetime update does not require a calendar timestamp");
    let snapshot = ledger.snapshot().expect("snapshot");
    assert_eq!(snapshot.features[0].count, 1);
    assert!(snapshot.months.is_empty());
    drop(ledger);
    std::fs::remove_file(path).expect("cleanup");
}

#[test]
fn twelve_month_acceleration_does_not_depend_on_wall_clock_delays() {
    let path = test_path("accelerated");
    let ledger = EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search"]))
        .expect("ledger opens");
    let started = std::time::Instant::now();
    for month in 1..=12 {
        ledger
            .record_feature_at("search", 1, timestamp(2025, month))
            .expect("month");
    }
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(ledger.snapshot().expect("snapshot").months.len(), 12);
    drop(ledger);
    std::fs::remove_file(path).expect("cleanup");
}

#[test]
fn one_month_acceleration_preserves_every_daily_increment() {
    let path = test_path("one-month");
    let ledger =
        EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search", "export"]))
            .expect("ledger opens");
    let january = timestamp(2026, 1);
    for day in 0..31_u64 {
        ledger
            .record_batch_at(
                january + day * 24 * 60 * 60,
                60,
                [("search", 2), ("export", 1)],
            )
            .expect("daily usage");
    }
    let snapshot = ledger.snapshot().expect("snapshot");
    assert_eq!(snapshot.months.len(), 1);
    assert_eq!(snapshot.active_seconds, 31 * 60);
    assert_eq!(snapshot.features[0].feature_id.as_ref(), "export");
    assert_eq!(snapshot.features[0].count, 31);
    assert_eq!(snapshot.features[1].feature_id.as_ref(), "search");
    assert_eq!(snapshot.features[1].count, 62);
    drop(ledger);
    std::fs::remove_file(path).expect("cleanup");
}
