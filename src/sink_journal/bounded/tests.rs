use super::*;
use crate::TelemetryRecoveryLimits;
use std::cell::Cell;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{MetadataExt, symlink};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("fixture clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "telemetry-bounded-journal-{}-{now}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("actual unique directory");
        Self(path)
    }
    fn file(&self) -> File {
        File::open(&self.0).expect("actual retained directory FD")
    }
    fn journal(&self) -> PathBuf {
        self.0.join("shard-0.journal")
    }
    fn write(&self, bytes: &[u8]) {
        fs::write(self.journal(), bytes).expect("actual journal fixture");
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("remove actual fixture after owners drop");
    }
}

fn limits() -> SinkJournalRecoveryLimits {
    SinkJournalRecoveryLimits {
        max_file_bytes: 16 << 20,
        max_frame_bytes: 1 << 20,
        max_transactions: 128,
        max_appends: 4096,
        max_payload_bytes: 8 << 20,
        max_io_attempts: 4096,
        max_io_bytes: 64 << 20,
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
fn budget() -> TelemetryRecoveryBudget {
    TelemetryRecoveryBudget::new(recovery_limits(), Instant::now() + Duration::from_secs(10))
        .expect("same finite original ledger")
}
#[allow(
    clippy::result_large_err,
    reason = "retain complete original owners and native outcomes inline without a refusal allocation"
)]
fn open(
    directory: &Directory,
    limits: SinkJournalRecoveryLimits,
    budget: &mut TelemetryRecoveryBudget,
) -> std::result::Result<(SinkJournal, Vec<RecoveredTransaction>), RetainedSinkJournalError> {
    SinkJournal::open_until(
        &directory.file(),
        ShardId::new(0),
        16 << 20,
        false,
        limits,
        budget,
    )
}
fn checkpoint(sequence: u64, offset: u64) -> DurableSinkCheckpoint {
    DurableSinkCheckpoint {
        topic_partition: TopicPartition::new(TopicId::new(17), LogicalPartitionId::new(0)),
        next_placement_sequence: PlacementSequence::new(sequence),
        next_offset: LogicalOffset::new(offset),
    }
}
fn body(
    expected: DurableSinkCheckpoint,
    next: DurableSinkCheckpoint,
    appends: &[(u32, u64, &[u8])],
) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_checkpoint(&mut bytes, expected);
    encode_checkpoint(&mut bytes, next);
    bytes.extend_from_slice(&(appends.len() as u32).to_le_bytes());
    for (shard, offset, payload) in appends {
        bytes.extend_from_slice(&shard.to_le_bytes());
        bytes.extend_from_slice(&offset.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(payload);
    }
    bytes
}
fn framed(body: &[u8]) -> Vec<u8> {
    let mut frame = (body.len() as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(body);
    frame.extend_from_slice(blake3::hash(body).as_bytes());
    frame
}
fn one() -> Vec<u8> {
    framed(&body(
        checkpoint(0, 0),
        checkpoint(1, 1),
        &[(0, 0, b"first body")],
    ))
}
fn wire(frames: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = MAGIC.to_vec();
    for frame in frames {
        bytes.extend_from_slice(frame);
    }
    bytes
}

#[test]
fn genuine_legacy_wire_replays_and_returned_journal_appends_then_restarts() {
    let directory = Directory::new();
    let (legacy, recovered) = SinkJournal::open(&directory.0, ShardId::new(0), 16 << 20, false)
        .expect("unchanged legacy header producer");
    assert!(recovered.is_empty());
    drop(legacy);
    let second = framed(&body(
        checkpoint(1, 1),
        checkpoint(2, 2),
        &[(0, 1, b"second body")],
    ));
    fs::OpenOptions::new()
        .append(true)
        .open(directory.journal())
        .expect("actual legacy journal")
        .write_all(&[one(), second].concat())
        .expect("actual committed frames");
    let (journal, recovered) =
        open(&directory, limits(), &mut budget()).expect("real bounded replay");
    assert_eq!(recovered.len(), 2);
    assert_eq!(&recovered[0].appends[0].payload[..], b"first body");
    assert_eq!(&recovered[1].appends[0].payload[..], b"second body");
    assert_eq!(
        journal.state.lock().expect("actual state").checkpoints[&checkpoint(0, 0).topic_partition],
        checkpoint(2, 2)
    );
    journal
        .append(checkpoint(2, 2), &[], checkpoint(3, 3))
        .expect("returned actual journal remains appendable");
    drop(journal);
    drop(recovered);
    let (journal, replay) =
        open(&directory, limits(), &mut budget()).expect("restart actual appended journal");
    assert_eq!(replay.len(), 3);
    assert_eq!(replay[2].next, checkpoint(3, 3));
    assert!(replay[2].appends.is_empty());
    drop(journal);
    let (legacy, ordinary) = SinkJournal::open(&directory.0, ShardId::new(0), 16 << 20, false)
        .expect("ordinary constructor unchanged");
    assert_eq!(ordinary.len(), 3);
    drop(legacy);
}

#[test]
fn retained_directory_fd_anchors_creation_after_ancestor_name_replacement() {
    let root = Directory::new();
    let supplied = root.0.join("supplied");
    let moved = root.0.join("moved");
    fs::create_dir(&supplied).expect("actual initial directory");
    let parent = File::open(&supplied).expect("retain actual source FD");
    fs::rename(&supplied, &moved).expect("replace ancestor namespace");
    fs::create_dir(&supplied).expect("replacement directory");
    fs::write(
        supplied.join("shard-0.journal"),
        b"replacement remains intact",
    )
    .expect("replacement sentinel");
    let (journal, replay) = SinkJournal::open_until(
        &parent,
        ShardId::new(0),
        16 << 20,
        false,
        limits(),
        &mut budget(),
    )
    .expect("descriptor-relative real producer");
    assert!(replay.is_empty());
    assert_eq!(
        fs::read(moved.join("shard-0.journal")).expect("original FD target"),
        MAGIC
    );
    assert_eq!(
        fs::read(supplied.join("shard-0.journal")).expect("replacement sentinel"),
        b"replacement remains intact"
    );
    let actual = journal
        .state
        .lock()
        .expect("real state")
        .file
        .metadata()
        .expect("real inode");
    assert_eq!(
        actual.ino(),
        fs::metadata(moved.join("shard-0.journal"))
            .expect("original inode")
            .ino()
    );
}

#[test]
fn nofollow_nonblocking_and_single_link_checks_refuse_real_special_sources() {
    use rustix::fs::{CWD, Mode, makedev, mknodat};
    let directory = Directory::new();
    let target = directory.0.join("target");
    fs::write(&target, MAGIC).expect("actual target");
    symlink(&target, directory.journal()).expect("actual final symlink");
    let error =
        open(&directory, limits(), &mut budget()).expect_err("nofollow refuses actual symlink");
    assert!(matches!(error.cause, Failure::Io(_)));
    assert_eq!(fs::read(&target).expect("target intact"), MAGIC);
    drop(error);
    fs::remove_file(directory.journal()).expect("remove fixture symlink");
    mknodat(
        CWD,
        directory.journal(),
        FileType::Fifo,
        Mode::RUSR | Mode::WUSR,
        makedev(0, 0),
    )
    .expect("actual FIFO");
    let start = Instant::now();
    let error =
        open(&directory, limits(), &mut budget()).expect_err("nonblocking opens then rejects FIFO");
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(matches!(error.cause, Failure::Changed));
    assert!(error.attempt.file.is_some());
    drop(error);
    fs::remove_file(directory.journal()).expect("remove FIFO");
    fs::hard_link(&target, directory.journal()).expect("actual second link");
    let error = open(&directory, limits(), &mut budget())
        .expect_err("shared inode refuses before mutation");
    assert!(matches!(error.cause, Failure::Changed));
    assert!(!error.attempt.locked);
}

#[test]
fn corrupt_frame_census_refuses_before_untrusted_append_allocation() {
    let directory = Directory::new();
    let mut huge = body(checkpoint(0, 0), checkpoint(1, 1), &[]);
    huge[2 * CHECKPOINT_BYTES..TRANSACTION_HEADER_BYTES].copy_from_slice(&u32::MAX.to_le_bytes());
    directory.write(&wire(&[framed(&huge)]));
    let mut ledger = budget();
    let error = open(&directory, limits(), &mut ledger)
        .expect_err("impossible count has no append allocation");
    assert!(matches!(
        error.cause,
        Failure::Corrupt("append count exceeds actual frame layout")
    ));
    assert!(error.attempt.current.is_none());
    assert!(error.attempt.transactions.is_empty());
    assert_eq!(ledger.usage().records, 0);
    assert_eq!(error.attempt.frame.len(), TRANSACTION_HEADER_BYTES);
    drop(error);
    let invalid = [
        body(
            checkpoint(0, 0),
            checkpoint(1, 1),
            &[(9, 0, b"wrong shard")],
        ),
        body(
            checkpoint(0, 0),
            checkpoint(1, 1),
            &[(0, 1, b"offset outside next")],
        ),
        body(checkpoint(1, 1), checkpoint(0, 0), &[]),
        body(
            checkpoint(0, 0),
            checkpoint(2, 2),
            &[(0, 1, b"first"), (0, 0, b"regression")],
        ),
        [body(checkpoint(0, 0), checkpoint(1, 1), &[]), vec![99]].concat(),
    ];
    for bytes in invalid {
        directory.write(&wire(&[framed(&bytes)]));
        let error = open(&directory, limits(), &mut budget())
            .expect_err("actual checksummed invalid layout");
        assert!(matches!(error.cause, Failure::Corrupt(_)));
        assert!(error.attempt.current.is_none());
    }
    directory.write(&wire(&[one(), one()]));
    let error = open(&directory, limits(), &mut budget()).expect_err("checkpoint chain regresses");
    assert!(matches!(
        error.cause,
        Failure::Corrupt("checkpoint chain regressed")
    ));
    assert_eq!(error.attempt.transactions.len(), 1);
}

#[test]
fn original_shared_record_and_local_cumulative_payload_limits_precede_typed_copy() {
    let directory = Directory::new();
    let two = body(
        checkpoint(0, 0),
        checkpoint(2, 2),
        &[(0, 0, b"abc"), (0, 1, b"def")],
    );
    directory.write(&wire(&[framed(&two)]));
    let mut policy = recovery_limits();
    policy.max_records = 3;
    let mut ledger = TelemetryRecoveryBudget::new(policy, Instant::now() + Duration::from_secs(10))
        .expect("original finite ledger");
    ledger
        .charge(TelemetryRecoveryUsage {
            records: 1,
            ..Default::default()
        })
        .expect("earlier catalog record stays spent");
    let error = open(&directory, limits(), &mut ledger)
        .expect_err("one shared ledger refuses before append Vec");
    assert!(matches!(
        error.cause,
        Failure::Recovery(TelemetryRecoveryError::Limits)
    ));
    assert!(error.attempt.current.is_none());
    assert_eq!(ledger.usage().records, 1);
    drop(error);
    let mut local = limits();
    local.max_appends = 1;
    let error =
        open(&directory, local, &mut budget()).expect_err("explicit cumulative append count");
    assert!(error.attempt.current.is_none());
    drop(error);
    let first = framed(&body(checkpoint(0, 0), checkpoint(1, 1), &[(0, 0, b"abc")]));
    let second = framed(&body(checkpoint(1, 1), checkpoint(2, 2), &[(0, 1, b"def")]));
    directory.write(&wire(&[first, second]));
    local = limits();
    local.max_payload_bytes = 5;
    let error = open(&directory, local, &mut budget())
        .expect_err("payload allowance is not refreshed per frame");
    assert_eq!(error.attempt.transactions.len(), 1);
    assert!(error.attempt.current.is_none());
    assert_eq!(
        &error.attempt.transactions[0].appends[0].payload[..],
        b"abc"
    );
}

#[test]
fn actual_partial_headers_and_frames_repair_once_and_restart_in_order() {
    let second = framed(&body(
        checkpoint(1, 1),
        checkpoint(2, 2),
        &[(0, 1, b"partial body")],
    ));
    for tail in [
        vec![4],
        vec![4, 0],
        vec![4, 0, 0],
        second[..second.len() - 1].to_vec(),
    ] {
        let directory = Directory::new();
        let complete = wire(&[one()]);
        directory.write(&[complete.clone(), tail].concat());
        let (journal, replay) =
            open(&directory, limits(), &mut budget()).expect("real tail repair");
        assert_eq!(replay.len(), 1);
        assert_eq!(
            fs::read(directory.journal()).expect("actual synced repaired file"),
            complete
        );
        drop(journal);
        let (journal, replay) =
            open(&directory, limits(), &mut budget()).expect("actual repaired restart");
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].next, checkpoint(1, 1));
        assert_eq!(&replay[0].appends[0].payload[..], b"first body");
        drop(journal);
    }
}

#[test]
fn corrupt_checksum_retains_original_fd_partial_replay_and_exclusive_lock_until_drop() {
    let directory = Directory::new();
    let mut bad = one();
    *bad.last_mut().expect("actual checksum byte") ^= 1;
    directory.write(&wire(&[one(), bad]));
    let original = fs::metadata(directory.journal()).expect("actual source metadata");
    let error = open(&directory, limits(), &mut budget()).expect_err("actual CRC/hash failure");
    assert!(matches!(
        error.cause,
        Failure::Corrupt("frame checksum is invalid")
    ));
    assert_eq!(error.attempt.transactions.len(), 1);
    assert_eq!(
        &error.attempt.transactions[0].appends[0].payload[..],
        b"first body"
    );
    assert_eq!(
        error
            .attempt
            .file
            .as_ref()
            .expect("actual retained FD")
            .metadata()
            .expect("actual metadata")
            .ino(),
        original.ino()
    );
    let held =
        open(&directory, limits(), &mut budget()).expect_err("actual retained advisory owner");
    assert!(matches!(held.cause, Failure::Io(ref io) if io.kind() == io::ErrorKind::WouldBlock));
    drop(held);
    drop(error);
    let error = open(&directory, limits(), &mut budget())
        .expect_err("real lock released, original corruption still observed");
    assert!(matches!(
        error.cause,
        Failure::Corrupt("frame checksum is invalid")
    ));
}

#[test]
fn real_truncation_settles_before_following_finite_io_refusal_and_can_restart() {
    let complete = wire(&[one()]);
    let input = [complete.clone(), vec![1, 2, 3]].concat();
    let mut witnessed = false;
    for attempts in 1..64 {
        let directory = Directory::new();
        directory.write(&input);
        let mut local = limits();
        local.max_io_attempts = attempts;
        let actual = open(&directory, local, &mut budget());
        if let Err(error) = actual
            && error.attempt.repaired_to.is_some()
        {
            assert!(matches!(error.cause, Failure::Limits));
            assert!(error.attempt.file_synced);
            assert!(matches!(error.attempt.file_settlement, Some(Ok(()))));
            assert_eq!(error.attempt.repaired_to, Some(complete.len() as u64));
            assert_eq!(
                fs::read(directory.journal()).expect("actual truncation already occurred"),
                complete
            );
            assert!(error.attempt.file.is_some() && error.attempt.locked);
            assert_eq!(error.attempt.transactions.len(), 1);
            drop(error);
            let (journal, replay) = open(&directory, limits(), &mut budget())
                .expect("restart after real refused repair");
            assert_eq!(replay.len(), 1);
            drop(journal);
            witnessed = true;
            break;
        }
    }
    assert!(
        witnessed,
        "finite ceiling must reach actual post-repair/post-settlement refusal"
    );
}

thread_local! {
    static EXPIRE_AFTER_REPAIR: Cell<bool> = const { Cell::new(false) };
    static PANIC_AFTER_REPAIR: Cell<bool> = const { Cell::new(false) };
    static EXPIRE_AFTER_IO_ERROR: Cell<bool> = const { Cell::new(false) };
    static EXPIRE_AFTER_INITIAL_WRITE: Cell<bool> = const { Cell::new(false) };
    static PANIC_AFTER_INITIAL_WRITE: Cell<bool> = const { Cell::new(false) };
    static PANIC_AFTER_FILE_SETTLEMENT: Cell<bool> = const { Cell::new(false) };
    static PANIC_AFTER_DIRECTORY_SETTLEMENT: Cell<bool> = const { Cell::new(false) };
}
pub(super) fn after_tail_repair(expiry: Instant) {
    if PANIC_AFTER_REPAIR.get() {
        panic!("actual journal repair completed before setup unwind");
    }
    if EXPIRE_AFTER_REPAIR.get() {
        while Instant::now() < expiry {
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
}
pub(super) fn before_error_observation(expiry: Instant, failed: bool) {
    if failed && EXPIRE_AFTER_IO_ERROR.get() {
        while Instant::now() < expiry {
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
}
pub(super) fn after_initial_write(expiry: Instant) {
    if PANIC_AFTER_INITIAL_WRITE.get() {
        panic!("actual initialization write completed before unwind");
    }
    if EXPIRE_AFTER_INITIAL_WRITE.get() {
        while Instant::now() < expiry {
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
}
pub(super) fn after_file_settlement() {
    if PANIC_AFTER_FILE_SETTLEMENT.get() {
        panic!("actual file sync completed before independent cleanup panic");
    }
}
pub(super) fn after_directory_settlement() {
    if PANIC_AFTER_DIRECTORY_SETTLEMENT.get() {
        panic!("actual directory sync completed before independent cleanup panic");
    }
}
struct ExpireAfterRepair;
impl Drop for ExpireAfterRepair {
    fn drop(&mut self) {
        EXPIRE_AFTER_REPAIR.set(false);
        PANIC_AFTER_REPAIR.set(false);
        EXPIRE_AFTER_IO_ERROR.set(false);
        EXPIRE_AFTER_INITIAL_WRITE.set(false);
        PANIC_AFTER_INITIAL_WRITE.set(false);
        PANIC_AFTER_FILE_SETTLEMENT.set(false);
        PANIC_AFTER_DIRECTORY_SETTLEMENT.set(false);
    }
}

#[test]
fn genuine_completed_tail_repair_and_file_owner_survive_original_expiry() {
    let directory = Directory::new();
    let complete = wire(&[one()]);
    directory.write(&[complete.clone(), vec![1, 2]].concat());
    let expiry = Instant::now() + Duration::from_millis(100);
    let mut ledger =
        TelemetryRecoveryBudget::new(recovery_limits(), expiry).expect("original actual deadline");
    EXPIRE_AFTER_REPAIR.set(true);
    let guard = ExpireAfterRepair;
    let error = open(&directory, limits(), &mut ledger)
        .expect_err("real truncate completed before original E refusal");
    drop(guard);
    assert!(matches!(
        error.cause,
        Failure::Recovery(TelemetryRecoveryError::Expired)
    ));
    assert_eq!(error.attempt.repaired_to, Some(complete.len() as u64));
    assert_eq!(
        fs::read(directory.journal()).expect("actual post-expiry file"),
        complete
    );
    assert_eq!(error.attempt.transactions.len(), 1);
    assert!(error.attempt.file.is_some() && error.attempt.locked);
    assert!(error.attempt.file_synced);
    assert!(matches!(error.attempt.file_settlement, Some(Ok(()))));
    assert!(Instant::now() >= expiry);
    drop(error);
    let (journal, replay) = open(&directory, limits(), &mut budget())
        .expect("fresh later restart of retained physical outcome");
    assert_eq!(replay.len(), 1);
    drop(journal);
}

#[test]
fn actual_post_truncate_unwind_retains_original_file_and_partial_replay() {
    let directory = Directory::new();
    let complete = wire(&[one()]);
    directory.write(&[complete.clone(), vec![1, 2]].concat());
    PANIC_AFTER_REPAIR.set(true);
    let guard = ExpireAfterRepair;
    let error = open(&directory, limits(), &mut budget())
        .expect_err("actual completed truncate then unwind");
    drop(guard);
    assert!(matches!(error.cause, Failure::Panic));
    assert!(error.attempt.panic.is_some());
    assert_eq!(error.attempt.repaired_to, Some(complete.len() as u64));
    assert_eq!(error.attempt.transactions.len(), 1);
    assert!(error.attempt.file.is_some() && error.attempt.locked);
    assert!(error.attempt.file_synced);
    assert!(matches!(error.attempt.file_settlement, Some(Ok(()))));
    assert_eq!(
        fs::read(directory.journal()).expect("actual retained repaired file"),
        complete
    );
    drop(error);
    let (journal, replay) = open(&directory, limits(), &mut budget())
        .expect("restart after actual refused constructor unwind");
    assert_eq!(replay.len(), 1);
    drop(journal);
}

#[test]
fn actual_open_error_is_retained_before_a_later_original_clock_refusal() {
    let directory = Directory::new();
    let target = directory.0.join("target");
    fs::write(&target, MAGIC).expect("actual target");
    symlink(&target, directory.journal()).expect("actual symlink open refusal");
    let expiry = Instant::now() + Duration::from_millis(100);
    let mut ledger =
        TelemetryRecoveryBudget::new(recovery_limits(), expiry).expect("original actual expiry");
    EXPIRE_AFTER_IO_ERROR.set(true);
    let guard = ExpireAfterRepair;
    let error = open(&directory, limits(), &mut ledger)
        .expect_err("actual ELOOP result retained across real elapsed E");
    drop(guard);
    assert!(matches!(error.cause, Failure::Io(ref error) if error.raw_os_error().is_some()));
    assert!(error.attempt.late_io);
    assert!(error.attempt.directory.is_some());
    assert!(error.attempt.file.is_none());
    assert!(Instant::now() >= expiry);
    assert_eq!(fs::read(&target).expect("actual target unchanged"), MAGIC);
}

#[test]
fn finite_refusals_after_real_create_always_settle_file_and_parent() {
    let mut observed_created_refusal = false;
    for attempts in 1..48 {
        let directory = Directory::new();
        let mut local = limits();
        local.max_io_attempts = attempts;
        if let Err(error) = open(&directory, local, &mut budget())
            && error.attempt.created
        {
            assert!(matches!(error.cause, Failure::Limits));
            assert!(error.attempt.file_synced && error.attempt.directory_synced);
            assert!(matches!(error.attempt.file_settlement, Some(Ok(()))));
            assert!(matches!(error.attempt.directory_settlement, Some(Ok(()))));
            assert!(error.attempt.file.is_some() && error.attempt.directory.is_some());
            drop(error);
            let (journal, replay) = open(&directory, limits(), &mut budget())
                .expect("settled empty/initialized creation restarts");
            assert!(replay.is_empty());
            drop(journal);
            observed_created_refusal = true;
        }
    }
    assert!(
        observed_created_refusal,
        "real exclusive creation must be reached by finite policies"
    );
}

#[test]
fn actual_initial_write_settles_file_and_parent_despite_elapsed_original_expiry() {
    let directory = Directory::new();
    let expiry = Instant::now() + Duration::from_millis(100);
    let mut ledger =
        TelemetryRecoveryBudget::new(recovery_limits(), expiry).expect("original actual expiry");
    EXPIRE_AFTER_INITIAL_WRITE.set(true);
    let guard = ExpireAfterRepair;
    let error = open(&directory, limits(), &mut ledger)
        .expect_err("actual initialization write then real original E");
    drop(guard);
    assert!(matches!(
        error.cause,
        Failure::Recovery(TelemetryRecoveryError::Expired)
    ));
    assert!(error.attempt.created && error.attempt.initialized);
    assert!(error.attempt.file_synced && error.attempt.directory_synced);
    assert!(matches!(error.attempt.file_settlement, Some(Ok(()))));
    assert!(matches!(error.attempt.directory_settlement, Some(Ok(()))));
    assert_eq!(
        fs::read(directory.journal()).expect("actual settled initialized bytes"),
        MAGIC
    );
    drop(error);
    let (journal, replay) =
        open(&directory, limits(), &mut budget()).expect("restart initialized native source");
    assert!(replay.is_empty());
    drop(journal);
}

#[test]
fn actual_initialization_and_independent_cleanup_panics_retain_both_completed_syncs() {
    let directory = Directory::new();
    PANIC_AFTER_INITIAL_WRITE.set(true);
    PANIC_AFTER_FILE_SETTLEMENT.set(true);
    PANIC_AFTER_DIRECTORY_SETTLEMENT.set(true);
    let guard = ExpireAfterRepair;
    let error = open(&directory, limits(), &mut budget())
        .expect_err("three actual separate mutation/cleanup outcomes");
    drop(guard);
    assert!(matches!(error.cause, Failure::Panic));
    assert!(error.attempt.panic.is_some());
    assert!(error.attempt.file_settlement_panic.is_some());
    assert!(error.attempt.directory_settlement_panic.is_some());
    assert!(error.attempt.file_synced && error.attempt.directory_synced);
    assert!(matches!(error.attempt.file_settlement, Some(Ok(()))));
    assert!(matches!(error.attempt.directory_settlement, Some(Ok(()))));
    assert_eq!(
        fs::read(directory.journal()).expect("actual completed initialized state"),
        MAGIC
    );
    drop(error);
    let (journal, replay) =
        open(&directory, limits(), &mut budget()).expect("restart after actual settled unwind");
    assert!(replay.is_empty());
    drop(journal);
}

#[test]
fn real_checkpoint_table_probes_and_growth_spend_cumulative_work_before_use() {
    let directory = Directory::new();
    let frames = (0..512)
        .map(|index| {
            let mut expected = checkpoint(0, 0);
            expected.topic_partition.topic_id = TopicId::new(17 + index);
            let mut next = checkpoint(1, 1);
            next.topic_partition = expected.topic_partition;
            framed(&body(expected, next, &[]))
        })
        .collect::<Vec<_>>();
    directory.write(&wire(&frames));
    let mut local = limits();
    local.max_transactions = 1024;
    let mut policy = recovery_limits();
    policy.max_work_bytes = 20 << 20;
    let mut ledger = TelemetryRecoveryBudget::new(policy, Instant::now() + Duration::from_secs(10))
        .expect("genuine cumulative finite work ceiling");
    let error = open(&directory, local, &mut ledger)
        .expect_err("actual growing sparse checkpoint table consumes probe work");
    assert!(matches!(
        error.cause,
        Failure::Recovery(TelemetryRecoveryError::Limits)
    ));
    assert!(
        !error.attempt.transactions.is_empty() && error.attempt.transactions.len() < frames.len()
    );
    assert!(error.attempt.checkpoints.capacity() >= error.attempt.checkpoints.len());
    assert!(error.attempt.file.is_some() && error.attempt.locked);
    assert_eq!(
        fs::read(directory.journal()).expect("actual original source remains intact"),
        wire(&frames)
    );
}

#[test]
fn finite_file_frame_read_and_expired_limits_leave_real_sources_intact() {
    let directory = Directory::new();
    let bytes = wire(&[one()]);
    directory.write(&bytes);
    let mut local = limits();
    local.max_file_bytes = MAGIC.len();
    let error = open(&directory, local, &mut budget())
        .expect_err("real file ceiling before replay allocation");
    assert!(matches!(error.cause, Failure::Limits));
    assert!(error.attempt.frame.is_empty());
    drop(error);
    local = limits();
    local.max_frame_bytes = TRANSACTION_HEADER_BYTES;
    let error = open(&directory, local, &mut budget())
        .expect_err("actual header length checked before frame allocation");
    assert!(error.attempt.frame.is_empty());
    drop(error);
    local = limits();
    local.max_io_bytes = 1;
    let error = open(&directory, local, &mut budget()).expect_err("actual request byte ceiling");
    assert_eq!(error.attempt.actual_read_bytes, 0);
    drop(error);
    let expiry = Instant::now() + Duration::from_millis(10);
    let mut ledger =
        TelemetryRecoveryBudget::new(recovery_limits(), expiry).expect("original finite expiry");
    while Instant::now() < expiry {
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let error =
        open(&directory, limits(), &mut ledger).expect_err("expired E never starts physical work");
    assert_eq!(error.attempt.io_attempts, 0);
    assert!(error.attempt.file.is_none());
    assert_eq!(
        fs::read(directory.journal()).expect("unchanged source"),
        bytes
    );
}
