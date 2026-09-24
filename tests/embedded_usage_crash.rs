use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use shard_telemetry::{EmbeddedUsageLedger, EmbeddedUsageLedgerConfig};

const CRASH_PATH_ENV: &str = "SHARD_TELEMETRY_USAGE_CRASH_TEST_PATH";

fn ledger(path: &Path) -> EmbeddedUsageLedger {
    EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(path, ["export", "search"]))
        .expect("usage ledger opens")
}

fn test_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shard-telemetry-usage-crash-{}-{nonce}.ledger",
        std::process::id(),
    ))
}

#[test]
fn acknowledged_usage_survives_abrupt_process_exit() {
    let path = test_path();
    let initial = ledger(&path);
    initial.record_feature("search", 2).expect("initial update");
    drop(initial);

    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("embedded_usage_crash_worker")
        .arg("--nocapture")
        .env(CRASH_PATH_ENV, &path)
        .status()
        .expect("crash worker starts");
    assert_eq!(status.code(), Some(91), "worker must exit without dropping");

    let recovered = ledger(&path);
    let snapshot = recovered.snapshot().expect("recovered snapshot");
    let search = snapshot
        .features
        .iter()
        .find(|feature| feature.feature_id.as_ref() == "search")
        .expect("search feature");
    assert_eq!(search.count, 5);
    assert_eq!(snapshot.active_seconds, 60);
    drop(recovered);
    std::fs::remove_file(path).expect("cleanup");
}

#[test]
fn embedded_usage_crash_worker() {
    let Some(path) = std::env::var_os(CRASH_PATH_ENV).map(PathBuf::from) else {
        return;
    };
    let ledger = ledger(&path);
    ledger
        .record_batch_at(1_767_225_600, 60, [("search", 3)])
        .expect("acknowledged update");

    // Deliberately bypass Rust destructors. record_batch_at has already
    // synchronized the inactive payload and its committed header.
    std::process::exit(91);
}
