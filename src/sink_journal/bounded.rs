//! Descriptor-anchored synchronous journal setup and replay.
//!
//! This additive producer leaves `SinkJournal::open` unchanged. The caller
//! supplies its actual retained directory descriptor, original expiry and
//! cumulative recovery ledger. A duplicate directory descriptor, journal file,
//! partial frames/replay and actual refusal/error are retained on failure.
//! Every read/write/truncate/sync attempt spends finite admission before entry.
//!
//! All journal operations use the same opened inode; no ancestor path is
//! rewalked. An advisory file lock coordinates bounded constructors. Callers
//! must coordinate ordinary writers and namespace changes with their genuine
//! directory owner: this is not a distributed fence or atomic namespace lease.
//! Name/inode checks detect substitutions; this producer never repairs a
//! replacement tree reached through an ancestor pathname. Declared collection
//! capacities exclude allocator/Bytes/HashMap implementation overhead and RSS.
//! Factory stripes/object recovery, replay indexing and workers remain separate.

use super::*;
use crate::{TelemetryRecoveryBudget, TelemetryRecoveryError, TelemetryRecoveryUsage};
use rustix::fs::{AtFlags, FileType, FlockOperation, Mode, OFlags, Stat};
use std::any::Any;
use std::ffi::CStr;
use std::fmt;
use std::io;
use std::mem::{size_of, take};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

const CHECKPOINT_BYTES: usize = 16 + 4 + 8 + 8;
const TRANSACTION_HEADER_BYTES: usize = 2 * CHECKPOINT_BYTES + 4;
const APPEND_HEADER_BYTES: usize = 4 + 8 + 4;
const IO_CHUNK_BYTES: usize = 16 * 1024;

/// Finite local journal bounds, additional to the shared recovery ledger.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SinkJournalRecoveryLimits {
    pub(crate) max_file_bytes: usize,
    pub(crate) max_frame_bytes: usize,
    pub(crate) max_transactions: usize,
    /// Cumulative replayed appends, not a refreshed per-frame allowance.
    pub(crate) max_appends: usize,
    pub(crate) max_payload_bytes: usize,
    /// Admitted attempts, including prepaid cleanup that may remain unused.
    pub(crate) max_io_attempts: usize,
    /// Sum of requested read/write bytes and admitted truncated tail bytes.
    pub(crate) max_io_bytes: usize,
}

#[derive(Debug)]
pub(crate) enum JournalRecoveryFailure {
    Limits,
    Changed,
    Corrupt(&'static str),
    Panic,
    Settlement,
    Recovery(TelemetryRecoveryError),
    Io(io::Error),
}

/// Actual partial setup/replay owner. No file, replay or error extractor exists.
pub(crate) struct RetainedSinkJournalError {
    pub(crate) cause: JournalRecoveryFailure,
    attempt: Attempt,
}
impl fmt::Debug for RetainedSinkJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedSinkJournalError")
            .field("cause", &self.cause)
            .field("file_retained", &self.attempt.file.is_some())
            .field("completed_retained", &self.attempt.completed.is_some())
            .field("transactions", &self.attempt.transactions.len())
            .field("admitted_io_attempts", &self.attempt.io_attempts)
            .field("read_progress", &self.attempt.read_progress)
            .field("repaired_to", &self.attempt.repaired_to)
            .field(
                "file_sync",
                &self
                    .attempt
                    .file_settlement
                    .as_ref()
                    .map(|result| result.as_ref().map_err(io::Error::kind)),
            )
            .field(
                "directory_sync",
                &self
                    .attempt
                    .directory_settlement
                    .as_ref()
                    .map(|result| result.as_ref().map_err(io::Error::kind)),
            )
            .field(
                "file_sync_panicked",
                &self.attempt.file_settlement_panic.is_some(),
            )
            .field(
                "directory_sync_panicked",
                &self.attempt.directory_settlement_panic.is_some(),
            )
            .field("late_io", &self.attempt.late_io)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedSinkJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            JournalRecoveryFailure::Limits => formatter.write_str("journal recovery limit refused"),
            JournalRecoveryFailure::Changed => {
                formatter.write_str("journal identity or size changed")
            }
            JournalRecoveryFailure::Corrupt(reason) => {
                write!(formatter, "invalid journal: {reason}")
            }
            JournalRecoveryFailure::Panic => {
                formatter.write_str("journal recovery panicked; actual owner retained")
            }
            JournalRecoveryFailure::Settlement => formatter
                .write_str("journal durability settlement failed; actual outcomes retained"),
            JournalRecoveryFailure::Recovery(error) => {
                write!(formatter, "journal recovery refused: {error}")
            }
            JournalRecoveryFailure::Io(error) => {
                write!(formatter, "journal recovery I/O failed: {error}")
            }
        }
    }
}
impl std::error::Error for RetainedSinkJournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            JournalRecoveryFailure::Recovery(error) => Some(error),
            JournalRecoveryFailure::Io(error) => Some(error),
            JournalRecoveryFailure::Settlement => self
                .attempt
                .file_settlement
                .as_ref()
                .and_then(|outcome| outcome.as_ref().err())
                .or_else(|| {
                    self.attempt
                        .directory_settlement
                        .as_ref()
                        .and_then(|outcome| outcome.as_ref().err())
                })
                .map(|error| error as &(dyn std::error::Error + 'static)),
            _ => None,
        }
    }
}

type Failure = JournalRecoveryFailure;
type Result<T> = std::result::Result<T, Failure>;

struct Attempt {
    completed: Option<(SinkJournal, Vec<RecoveredTransaction>)>,
    transactions: Vec<RecoveredTransaction>,
    current: Option<RecoveredTransaction>,
    checkpoints: HashMap<TopicPartition, DurableSinkCheckpoint>,
    frame: Vec<u8>,
    decoded_frame: Bytes,
    magic: [u8; MAGIC.len()],
    header: [u8; 4],
    checksum: [u8; CHECKSUM_BYTES],
    growth_probe: [u8; 1],
    observed_checksum: Option<[u8; CHECKSUM_BYTES]>,
    interrupted: Option<io::Error>,
    panic: Option<Box<dyn Any + Send>>,
    mutation_failure: Option<Failure>,
    file_settlement: Option<io::Result<()>>,
    directory_settlement: Option<io::Result<()>>,
    file_settlement_panic: Option<Box<dyn Any + Send>>,
    directory_settlement_panic: Option<Box<dyn Any + Send>>,
    open_absence: Option<io::Error>,
    original_stat: Option<Stat>,
    observed_stat: Option<Stat>,
    file_bytes: u64,
    position: u64,
    appends: usize,
    payload_bytes: usize,
    io_attempts: usize,
    io_bytes: usize,
    actual_read_bytes: usize,
    actual_write_bytes: usize,
    read_progress: usize,
    repaired_to: Option<u64>,
    initialized: bool,
    locked: bool,
    created: bool,
    mutation_entered: bool,
    settlement_prepaid: bool,
    file_synced: bool,
    directory_synced: bool,
    late_io: bool,
    expiry: Instant,
    limits: SinkJournalRecoveryLimits,
    format: JournalFormat,
    // Actual descriptors drop after every derived frame/replay/diagnostic.
    file: Option<File>,
    directory: Option<File>,
}

impl SinkJournal {
    /// Open and replay an actual journal relative to the supplied directory FD.
    ///
    /// Uses the SAME caller ledger/E. No path opener, legacy constructor, task
    /// or reconstructed budget is used. A genuine refusal retains descriptors,
    /// partial typed replay and any completed initialization/tail repair. The
    /// returned journal keeps the actual exclusive advisory lock while its
    /// ordinary append API operates on that same file. The caller still owns
    /// directory/namespace and current-write authority and bounded replay
    /// indexing. This producer is not a complete sink factory or attachment.
    #[allow(
        clippy::result_large_err,
        reason = "retain complete original owners and native outcomes inline without a refusal allocation"
    )]
    pub(crate) fn open_until(
        directory: &File,
        shard_id: ShardId,
        runtime_max_bytes: u64,
        sync_each_append: bool,
        limits: SinkJournalRecoveryLimits,
        budget: &mut TelemetryRecoveryBudget,
    ) -> std::result::Result<(Self, Vec<RecoveredTransaction>), RetainedSinkJournalError> {
        Self::open_format_until(
            directory,
            shard_id,
            runtime_max_bytes,
            sync_each_append,
            limits,
            JournalFormat::Legacy,
            budget,
        )
    }

    /// Same actual descriptor/repair producer in an explicit persisted domain.
    #[allow(
        clippy::too_many_arguments,
        clippy::result_large_err,
        reason = "retain the original native inputs, finite policy and format without a new owner"
    )]
    pub(super) fn open_format_until(
        directory: &File,
        shard_id: ShardId,
        runtime_max_bytes: u64,
        sync_each_append: bool,
        limits: SinkJournalRecoveryLimits,
        format: JournalFormat,
        budget: &mut TelemetryRecoveryBudget,
    ) -> std::result::Result<(Self, Vec<RecoveredTransaction>), RetainedSinkJournalError> {
        let mut attempt = Attempt {
            completed: None,
            transactions: Vec::new(),
            current: None,
            checkpoints: HashMap::new(),
            frame: Vec::new(),
            decoded_frame: Bytes::new(),
            magic: [0; MAGIC.len()],
            header: [0; 4],
            checksum: [0; CHECKSUM_BYTES],
            growth_probe: [0],
            observed_checksum: None,
            interrupted: None,
            panic: None,
            mutation_failure: None,
            file_settlement: None,
            directory_settlement: None,
            file_settlement_panic: None,
            directory_settlement_panic: None,
            open_absence: None,
            original_stat: None,
            observed_stat: None,
            file_bytes: 0,
            position: 0,
            appends: 0,
            payload_bytes: 0,
            io_attempts: 0,
            io_bytes: 0,
            actual_read_bytes: 0,
            actual_write_bytes: 0,
            read_progress: 0,
            repaired_to: None,
            initialized: false,
            locked: false,
            created: false,
            mutation_entered: false,
            settlement_prepaid: false,
            file_synced: false,
            directory_synced: false,
            late_io: false,
            expiry: budget.original_expiry(),
            limits,
            format,
            file: None,
            directory: None,
        };
        let actual = catch_unwind(AssertUnwindSafe(|| {
            attempt.run(
                directory,
                shard_id,
                runtime_max_bytes,
                sync_each_append,
                budget,
            )
        }));
        let actual = match actual {
            Ok(actual) => actual,
            Err(payload) => {
                attempt.panic = Some(payload);
                Err(Failure::Panic)
            }
        };
        if let Err(cause) = actual {
            // Preserve the actual cause; the shared ledger becomes terminal.
            let _ = budget.invalid::<()>();
            return Err(RetainedSinkJournalError { cause, attempt });
        }
        match attempt.completed.take() {
            Some(value) => Ok(value),
            None => {
                let _ = budget.invalid::<()>();
                Err(RetainedSinkJournalError {
                    cause: Failure::Changed,
                    attempt,
                })
            }
        }
    }
}

impl Attempt {
    fn check(&self, budget: &mut TelemetryRecoveryBudget) -> Result<()> {
        budget.check().map_err(Failure::Recovery)
    }

    fn enter_io(
        &mut self,
        budget: &mut TelemetryRecoveryBudget,
        bytes: usize,
        read: bool,
    ) -> Result<()> {
        let attempts = self.io_attempts.checked_add(1).ok_or(Failure::Limits)?;
        let requested = self.io_bytes.checked_add(bytes).ok_or(Failure::Limits)?;
        if attempts > self.limits.max_io_attempts || requested > self.limits.max_io_bytes {
            return Err(Failure::Limits);
        }
        budget
            .charge(TelemetryRecoveryUsage {
                read_bytes: if read { bytes } else { 0 },
                work_bytes: bytes.checked_add(1).ok_or(Failure::Limits)?,
                ..Default::default()
            })
            .map_err(Failure::Recovery)?;
        self.io_attempts = attempts;
        self.io_bytes = requested;
        Ok(())
    }

    // Successful native owners/effects are installed by the caller before its
    // next check. Actual physical errors always win over a later clock check.
    fn observed<T>(&mut self, actual: io::Result<T>) -> Result<T> {
        #[cfg(test)]
        tests::before_error_observation(self.expiry, actual.is_err());
        self.late_io |= Instant::now() >= self.expiry;
        actual.map_err(Failure::Io)
    }

    fn run(
        &mut self,
        directory: &File,
        shard_id: ShardId,
        runtime_max_bytes: u64,
        sync_each_append: bool,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<()> {
        self.check(budget)?;
        if [
            self.limits.max_file_bytes,
            self.limits.max_frame_bytes,
            self.limits.max_transactions,
            self.limits.max_appends,
            self.limits.max_payload_bytes,
            self.limits.max_io_attempts,
            self.limits.max_io_bytes,
        ]
        .into_iter()
        .any(|n| n == 0 || n > isize::MAX as usize)
            || self.limits.max_file_bytes < MAGIC.len()
            || !(TRANSACTION_HEADER_BYTES..=MAX_FRAME_BYTES).contains(&self.limits.max_frame_bytes)
            || runtime_max_bytes < MAGIC.len() as u64
        {
            return Err(Failure::Limits);
        }
        budget
            .charge(TelemetryRecoveryUsage {
                retained_bytes: size_of::<Self>()
                    + size_of::<RetainedSinkJournalError>()
                    + size_of::<SinkJournal>(),
                work_bytes: size_of::<Self>(),
                ..Default::default()
            })
            .map_err(Failure::Recovery)?;
        self.enter_io(budget, 0, false)?;
        let actual = rustix::io::fcntl_dupfd_cloexec(directory, 0).map_err(io::Error::from);
        self.directory = Some(File::from(self.observed(actual)?));
        self.check(budget)?;
        self.enter_io(budget, 0, false)?;
        let actual = rustix::fs::fstat(self.directory.as_ref().ok_or(Failure::Changed)?)
            .map_err(io::Error::from);
        self.observed_stat = Some(self.observed(actual)?);
        self.check(budget)?;
        if !self
            .observed_stat
            .as_ref()
            .is_some_and(|stat| FileType::from_raw_mode(stat.st_mode) == FileType::Directory)
        {
            return Err(Failure::Changed);
        }
        // Largest u32 name is fixed and stack-backed; no unbounded formatter.
        let mut name = [0_u8; 6 + 10 + 8 + 1];
        let name = journal_name(shard_id, &mut name)?;
        self.open_journal(name, runtime_max_bytes, budget)?;
        self.seek(0, budget)?;
        let mut magic = self.magic;
        let actual = self.read_into(&mut magic, budget);
        self.magic = magic;
        if actual? != MAGIC.len() || &self.magic != self.format.magic() {
            return Err(Failure::Corrupt("magic is invalid or truncated"));
        }
        self.position = MAGIC.len() as u64;
        loop {
            self.check(budget)?;
            if self.position == self.file_bytes {
                let mut probe = self.growth_probe;
                let actual = self.read_into(&mut probe, budget);
                self.growth_probe = probe;
                if actual? != 0 {
                    return Err(Failure::Changed);
                }
                break;
            }
            let frame_start = self.position;
            let mut header = self.header;
            let actual = self.read_into(&mut header, budget);
            self.header = header;
            if actual? < 4 {
                self.repair_tail(name, frame_start, budget)?;
                break;
            }
            let length = u32::from_le_bytes(self.header) as usize;
            if !(TRANSACTION_HEADER_BYTES..=MAX_FRAME_BYTES).contains(&length) {
                return Err(Failure::Corrupt("frame length is invalid"));
            }
            if length > self.limits.max_frame_bytes {
                return Err(Failure::Limits);
            }
            if self.transactions.len() >= self.limits.max_transactions {
                return Err(Failure::Limits);
            }
            let frame_end = frame_start
                .checked_add(4)
                .and_then(|n| n.checked_add(length as u64))
                .and_then(|n| n.checked_add(CHECKSUM_BYTES as u64))
                .ok_or(Failure::Limits)?;
            if frame_end > self.file_bytes {
                self.repair_tail(name, frame_start, budget)?;
                break;
            }
            self.frame = Vec::new();
            reserve_vec(&mut self.frame, length, budget)?;
            budget
                .charge(TelemetryRecoveryUsage {
                    work_bytes: length,
                    ..Default::default()
                })
                .map_err(Failure::Recovery)?;
            self.frame.resize(length, 0);
            let mut frame = take(&mut self.frame);
            let actual = self.read_into(&mut frame, budget);
            self.frame = frame;
            if actual? != length {
                return Err(Failure::Changed);
            }
            let mut checksum = self.checksum;
            let actual = self.read_into(&mut checksum, budget);
            self.checksum = checksum;
            if actual? != CHECKSUM_BYTES {
                return Err(Failure::Changed);
            }
            budget
                .charge(TelemetryRecoveryUsage {
                    work_bytes: length,
                    ..Default::default()
                })
                .map_err(Failure::Recovery)?;
            self.observed_checksum = Some(*blake3::hash(&self.frame).as_bytes());
            self.check(budget)?;
            if self.observed_checksum != Some(self.checksum) {
                return Err(Failure::Corrupt("frame checksum is invalid"));
            }
            self.decode_frame(shard_id, budget)?;
            self.position = frame_end;
        }
        self.validate_name(name, budget)?;
        self.seek(self.file_bytes, budget)?;
        self.completed = Some((
            SinkJournal {
                format: self.format,
                shard_id,
                max_bytes: runtime_max_bytes,
                sync_each_append,
                state: Mutex::new(JournalState {
                    file: self.file.take().ok_or(Failure::Changed)?,
                    bytes: self.file_bytes,
                    unsynced_bytes: 0,
                    checkpoints: take(&mut self.checkpoints),
                    terminal_write_refused: false,
                }),
            },
            take(&mut self.transactions),
        ));
        self.check(budget)
    }

    fn open_journal(
        &mut self,
        name: &CStr,
        runtime_max_bytes: u64,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<()> {
        self.enter_io(budget, 0, false)?;
        let actual = rustix::fs::openat(
            self.directory.as_ref().ok_or(Failure::Changed)?,
            name,
            OFlags::RDWR | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from);
        match self.observed(actual) {
            Ok(file) => self.file = Some(File::from(file)),
            Err(Failure::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                self.open_absence = Some(error);
                self.check(budget)?;
                // Admission precedes CREATE itself, not merely the later body
                // write. Even a late empty-file creation must settle the inode
                // and its real parent directory before returning/refusing.
                self.prepay_settlement(true, budget)?;
                let actual = catch_unwind(AssertUnwindSafe(|| {
                    self.enter_io(budget, 0, false)?;
                    let actual = rustix::fs::openat(
                        self.directory.as_ref().ok_or(Failure::Changed)?,
                        name,
                        OFlags::RDWR
                            | OFlags::CREATE
                            | OFlags::EXCL
                            | OFlags::NOFOLLOW
                            | OFlags::NONBLOCK
                            | OFlags::CLOEXEC,
                        Mode::RUSR | Mode::WUSR,
                    )
                    .map_err(io::Error::from);
                    self.file = Some(File::from(self.observed(actual)?));
                    self.created = true;
                    self.mutation_entered = true;
                    self.check(budget)?;
                    self.inspect_and_lock(name, runtime_max_bytes, budget)?;
                    self.write_magic(budget)
                }));
                return self.settle_mutation(actual, true, budget);
            }
            Err(error) => return Err(error),
        }
        self.check(budget)?;
        self.inspect_and_lock(name, runtime_max_bytes, budget)?;
        if self.file_bytes != 0 {
            return Ok(());
        }
        self.prepay_settlement(true, budget)?;
        let actual = catch_unwind(AssertUnwindSafe(|| self.write_magic(budget)));
        self.settle_mutation(actual, true, budget)
    }

    fn inspect_and_lock(
        &mut self,
        name: &CStr,
        runtime_max_bytes: u64,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<()> {
        self.enter_io(budget, 0, false)?;
        let actual =
            rustix::fs::fstat(self.file.as_ref().ok_or(Failure::Changed)?).map_err(io::Error::from);
        self.original_stat = Some(self.observed(actual)?);
        self.check(budget)?;
        let stat = self.original_stat.as_ref().ok_or(Failure::Changed)?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile || stat.st_nlink != 1 {
            return Err(Failure::Changed);
        }
        self.file_bytes = u64::try_from(stat.st_size).map_err(|_| Failure::Changed)?;
        if self.file_bytes > self.limits.max_file_bytes as u64
            || self.file_bytes > runtime_max_bytes
        {
            return Err(Failure::Limits);
        }
        self.enter_io(budget, 0, false)?;
        let actual = rustix::fs::flock(
            self.file.as_ref().ok_or(Failure::Changed)?,
            FlockOperation::NonBlockingLockExclusive,
        )
        .map_err(io::Error::from);
        self.observed(actual)?;
        self.locked = true;
        self.check(budget)?;
        self.validate_name(name, budget)
    }

    fn validate_name(&mut self, name: &CStr, budget: &mut TelemetryRecoveryBudget) -> Result<()> {
        self.enter_io(budget, 0, false)?;
        let actual = rustix::fs::statat(
            self.directory.as_ref().ok_or(Failure::Changed)?,
            name,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(io::Error::from);
        self.observed_stat = Some(self.observed(actual)?);
        self.check(budget)?;
        let original = self.original_stat.as_ref().ok_or(Failure::Changed)?;
        let observed = self.observed_stat.as_ref().ok_or(Failure::Changed)?;
        if observed.st_dev != original.st_dev
            || observed.st_ino != original.st_ino
            || observed.st_nlink != 1
            || FileType::from_raw_mode(observed.st_mode) != FileType::RegularFile
            || u64::try_from(observed.st_size).ok() != Some(self.file_bytes)
        {
            return Err(Failure::Changed);
        }
        Ok(())
    }

    fn seek(&mut self, position: u64, budget: &mut TelemetryRecoveryBudget) -> Result<()> {
        self.enter_io(budget, 0, false)?;
        let actual = rustix::fs::seek(
            self.file.as_ref().ok_or(Failure::Changed)?,
            rustix::fs::SeekFrom::Start(position),
        )
        .map_err(io::Error::from);
        let observed = self.observed(actual)?;
        self.position = observed;
        self.check(budget)?;
        if observed != position {
            return Err(Failure::Changed);
        }
        Ok(())
    }

    fn read_into(
        &mut self,
        bytes: &mut [u8],
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<usize> {
        let mut initialized = 0;
        self.read_progress = 0;
        while initialized < bytes.len() {
            let request = (bytes.len() - initialized).min(IO_CHUNK_BYTES);
            self.enter_io(budget, request, true)?;
            let actual = rustix::io::read(
                self.file.as_ref().ok_or(Failure::Changed)?,
                &mut bytes[initialized..initialized + request],
            )
            .map_err(io::Error::from);
            match self.observed(actual) {
                Ok(count) => {
                    initialized += count;
                    self.read_progress = initialized;
                    self.actual_read_bytes += count;
                    self.check(budget)?;
                    if count == 0 {
                        break;
                    }
                }
                Err(Failure::Io(error)) if error.kind() == io::ErrorKind::Interrupted => {
                    self.interrupted = Some(error);
                    self.check(budget)?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(initialized)
    }

    fn write_magic(&mut self, budget: &mut TelemetryRecoveryBudget) -> Result<()> {
        let mut written = 0;
        while written < MAGIC.len() {
            let request = MAGIC.len() - written;
            self.enter_io(budget, request, false)?;
            self.mutation_entered = true;
            let actual = rustix::io::write(
                self.file.as_ref().ok_or(Failure::Changed)?,
                &self.format.magic()[written..],
            )
            .map_err(io::Error::from);
            match self.observed(actual) {
                Ok(0) => {
                    return Err(Failure::Io(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "journal initialization made no progress",
                    )));
                }
                Ok(count) => {
                    written += count;
                    self.actual_write_bytes += count;
                    self.file_bytes = written as u64;
                    self.initialized = written == MAGIC.len();
                    #[cfg(test)]
                    tests::after_initial_write(self.expiry);
                    self.check(budget)?;
                }
                Err(Failure::Io(error)) if error.kind() == io::ErrorKind::Interrupted => {
                    self.interrupted = Some(error);
                    self.check(budget)?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn prepay_settlement(
        &mut self,
        directory: bool,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<()> {
        // These are consumed by settle_mutation without a fresh budget/E check.
        // Reserve every cleanup attempt BEFORE any physical mutation, including
        // CREATE. Unused reservations remain spent when entry itself refuses.
        self.enter_io(budget, 0, false)?;
        if directory {
            self.enter_io(budget, 0, false)?;
        }
        self.mutation_entered = false;
        self.settlement_prepaid = true;
        self.file_synced = false;
        self.directory_synced = false;
        Ok(())
    }

    fn settle_mutation(
        &mut self,
        actual: std::thread::Result<Result<()>>,
        directory: bool,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<()> {
        // Save the primary error/panic BEFORE either independently fallible
        // cleanup, so a later cleanup panic cannot discard the original result.
        self.mutation_failure = match actual {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(payload) => {
                self.panic = Some(payload);
                Some(Failure::Panic)
            }
        };
        if !self.settlement_prepaid {
            return Err(Failure::Changed);
        }
        self.settlement_prepaid = false;
        if self.mutation_entered {
            let actual = catch_unwind(AssertUnwindSafe(|| {
                let actual = self
                    .file
                    .as_ref()
                    .ok_or_else(|| io::Error::other("missing journal settlement file"))
                    .and_then(|file| rustix::fs::fsync(file).map_err(io::Error::from));
                self.file_synced = actual.is_ok();
                self.file_settlement = Some(actual);
                self.late_io |= Instant::now() >= self.expiry;
                #[cfg(test)]
                tests::after_file_settlement();
            }));
            if let Err(payload) = actual {
                self.file_settlement_panic = Some(payload);
            }
            // A file-sync failure/panic never suppresses the separately prepaid
            // real parent sync after file creation/initialization.
            if directory {
                let actual = catch_unwind(AssertUnwindSafe(|| {
                    let actual = self
                        .directory
                        .as_ref()
                        .ok_or_else(|| io::Error::other("missing journal settlement directory"))
                        .and_then(|directory| {
                            rustix::fs::fsync(directory).map_err(io::Error::from)
                        });
                    self.directory_synced = actual.is_ok();
                    self.directory_settlement = Some(actual);
                    self.late_io |= Instant::now() >= self.expiry;
                    #[cfg(test)]
                    tests::after_directory_settlement();
                }));
                if let Err(payload) = actual {
                    self.directory_settlement_panic = Some(payload);
                }
            }
        }
        if let Some(error) = self.mutation_failure.take() {
            return Err(error);
        }
        if self.mutation_entered
            && (!self.file_synced
                || self.file_settlement_panic.is_some()
                || (directory
                    && (!self.directory_synced || self.directory_settlement_panic.is_some())))
        {
            return Err(Failure::Settlement);
        }
        self.check(budget)
    }

    fn repair_tail(
        &mut self,
        name: &CStr,
        frame_start: u64,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<()> {
        self.validate_name(name, budget)?;
        let removed = usize::try_from(
            self.file_bytes
                .checked_sub(frame_start)
                .ok_or(Failure::Changed)?,
        )
        .map_err(|_| Failure::Limits)?;
        self.prepay_settlement(false, budget)?;
        let actual = catch_unwind(AssertUnwindSafe(|| {
            self.enter_io(budget, removed, false)?;
            self.mutation_entered = true;
            let actual =
                rustix::fs::ftruncate(self.file.as_ref().ok_or(Failure::Changed)?, frame_start)
                    .map_err(io::Error::from);
            self.observed(actual)?;
            self.repaired_to = Some(frame_start);
            self.file_bytes = frame_start;
            self.position = frame_start;
            #[cfg(test)]
            tests::after_tail_repair(self.expiry);
            self.check(budget)
        }));
        self.settle_mutation(actual, false, budget)
    }

    fn decode_frame(
        &mut self,
        shard_id: ShardId,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<()> {
        budget
            .charge(TelemetryRecoveryUsage {
                work_bytes: self.frame.len().checked_mul(2).ok_or(Failure::Limits)?,
                ..Default::default()
            })
            .map_err(Failure::Recovery)?;
        let mut cursor = Cursor {
            bytes: &self.frame,
            at: 0,
        };
        let expected = cursor.checkpoint()?;
        let next = cursor.checkpoint()?;
        let count = cursor.u32()? as usize;
        // Layout census: impossible counts cannot allocate an append vector.
        if count > cursor.remaining() / APPEND_HEADER_BYTES {
            return Err(Failure::Corrupt("append count exceeds actual frame layout"));
        }
        let appends = self.appends.checked_add(count).ok_or(Failure::Limits)?;
        if appends > self.limits.max_appends {
            return Err(Failure::Limits);
        }
        budget
            .charge(TelemetryRecoveryUsage {
                records: count.checked_add(1).ok_or(Failure::Limits)?,
                ..Default::default()
            })
            .map_err(Failure::Recovery)?;
        if expected.topic_partition != next.topic_partition
            || next.next_placement_sequence < expected.next_placement_sequence
            || next.next_offset < expected.next_offset
            || (count > 0
                && (next.next_placement_sequence == expected.next_placement_sequence
                    || next.next_offset == expected.next_offset))
        {
            return Err(Failure::Corrupt("transaction checkpoints are invalid"));
        }
        let mut payload_bytes = self.payload_bytes;
        let mut prior_offset = None;
        for _ in 0..count {
            budget.check().map_err(Failure::Recovery)?;
            let shard = ShardId::new(cursor.u32()?);
            let offset = LogicalOffset::new(cursor.u64()?);
            let length = cursor.u32()? as usize;
            cursor.skip(length)?;
            if shard != shard_id
                || offset < expected.next_offset
                || offset >= next.next_offset
                || prior_offset.is_some_and(|prior| prior >= offset)
            {
                return Err(Failure::Corrupt("append shard or offset is invalid"));
            }
            prior_offset = Some(offset);
            payload_bytes = payload_bytes.checked_add(length).ok_or(Failure::Limits)?;
            if payload_bytes > self.limits.max_payload_bytes {
                return Err(Failure::Limits);
            }
        }
        if cursor.remaining() != 0 {
            return Err(Failure::Corrupt("transaction has trailing bytes"));
        }
        map_probe_work(&self.checkpoints, budget)?;
        if let Some(actual) = self.checkpoints.get(&expected.topic_partition)
            && (actual.next_placement_sequence > expected.next_placement_sequence
                || actual.next_offset > expected.next_offset)
        {
            return Err(Failure::Corrupt("checkpoint chain regressed"));
        }
        self.check(budget)?;
        self.current = Some(RecoveredTransaction {
            expected,
            next,
            appends: Vec::new(),
        });
        reserve_vec(
            &mut self.current.as_mut().ok_or(Failure::Changed)?.appends,
            count,
            budget,
        )?;
        // Typed payloads share the real already-measured frame allocation.
        self.decoded_frame = Bytes::from(take(&mut self.frame));
        let mut cursor = Cursor {
            bytes: &self.decoded_frame,
            at: TRANSACTION_HEADER_BYTES,
        };
        for _ in 0..count {
            budget.check().map_err(Failure::Recovery)?;
            cursor.u32()?;
            let first_offset = LogicalOffset::new(cursor.u64()?);
            let length = cursor.u32()? as usize;
            let start = cursor.at;
            cursor.skip(length)?;
            self.current
                .as_mut()
                .ok_or(Failure::Changed)?
                .appends
                .push(RecoveredAppend {
                    topic_partition: expected.topic_partition,
                    first_offset,
                    payload: self.decoded_frame.slice(start..cursor.at),
                });
        }
        self.check(budget)?;
        reserve_vec(&mut self.transactions, 1, budget)?;
        map_probe_work(&self.checkpoints, budget)?;
        if !self.checkpoints.contains_key(&next.topic_partition) {
            reserve_map(&mut self.checkpoints, budget)?;
        }
        map_probe_work(&self.checkpoints, budget)?;
        self.checkpoints.insert(next.topic_partition, next);
        self.transactions
            .push(self.current.take().ok_or(Failure::Changed)?);
        self.appends = appends;
        self.payload_bytes = payload_bytes;
        self.check(budget)
    }
}

fn reserve_vec<T>(
    values: &mut Vec<T>,
    additional: usize,
    budget: &mut TelemetryRecoveryBudget,
) -> Result<()> {
    let capacity = values
        .len()
        .checked_add(additional)
        .ok_or(Failure::Limits)?;
    if capacity <= values.capacity() {
        return budget.check().map_err(Failure::Recovery);
    }
    let bytes = capacity
        .checked_mul(size_of::<T>())
        .ok_or(Failure::Limits)?;
    budget
        .charge(TelemetryRecoveryUsage {
            retained_bytes: bytes,
            work_bytes: bytes,
            ..Default::default()
        })
        .map_err(Failure::Recovery)?;
    let actual = values.try_reserve_exact(additional);
    if let Err(error) = actual {
        return Err(Failure::Recovery(TelemetryRecoveryError::Allocation(error)));
    }
    let excess = values
        .capacity()
        .checked_sub(capacity)
        .and_then(|n| n.checked_mul(size_of::<T>()))
        .ok_or(Failure::Limits)?;
    budget
        .charge(TelemetryRecoveryUsage {
            retained_bytes: excess,
            ..Default::default()
        })
        .map_err(Failure::Recovery)
}

fn reserve_map(
    values: &mut HashMap<TopicPartition, DurableSinkCheckpoint>,
    budget: &mut TelemetryRecoveryBudget,
) -> Result<()> {
    let capacity = values.len().checked_add(1).ok_or(Failure::Limits)?;
    if capacity <= values.capacity() {
        return budget.check().map_err(Failure::Recovery);
    }
    let slot = size_of::<(TopicPartition, DurableSinkCheckpoint)>();
    let bytes = capacity.checked_mul(slot).ok_or(Failure::Limits)?;
    let movement = values.capacity().checked_mul(slot).ok_or(Failure::Limits)?;
    budget
        .charge(TelemetryRecoveryUsage {
            retained_bytes: bytes,
            work_bytes: bytes.checked_add(movement).ok_or(Failure::Limits)?,
            ..Default::default()
        })
        .map_err(Failure::Recovery)?;
    if let Err(error) = values.try_reserve(1) {
        return Err(Failure::Recovery(TelemetryRecoveryError::Allocation(error)));
    }
    let excess = values
        .capacity()
        .checked_sub(capacity)
        .and_then(|n| n.checked_mul(slot))
        .ok_or(Failure::Limits)?;
    budget
        .charge(TelemetryRecoveryUsage {
            retained_bytes: excess,
            work_bytes: excess,
            ..Default::default()
        })
        .map_err(Failure::Recovery)
}

fn map_probe_work(
    values: &HashMap<TopicPartition, DurableSinkCheckpoint>,
    budget: &mut TelemetryRecoveryBudget,
) -> Result<()> {
    // Capacity, rather than len: retained spare slots still participate in
    // probe work. The conservative doubled public-slot charge also covers
    // table slack; this is declared work, not a CPU-cycle measurement.
    let slots = values
        .capacity()
        .checked_add(1)
        .and_then(|n| n.checked_mul(2))
        .ok_or(Failure::Limits)?;
    let bytes = slots
        .checked_mul(size_of::<(TopicPartition, DurableSinkCheckpoint)>())
        .ok_or(Failure::Limits)?;
    budget
        .charge(TelemetryRecoveryUsage {
            work_bytes: bytes,
            ..Default::default()
        })
        .map_err(Failure::Recovery)
}

fn journal_name(shard: ShardId, buffer: &mut [u8; 25]) -> Result<&CStr> {
    buffer[..6].copy_from_slice(b"shard-");
    let mut digits = [0_u8; 10];
    let mut value = shard.get();
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    let count = digits.len() - start;
    buffer[6..6 + count].copy_from_slice(&digits[start..]);
    buffer[6 + count..6 + count + 8].copy_from_slice(b".journal");
    CStr::from_bytes_with_nul(&buffer[..6 + count + 8 + 1]).map_err(|_| Failure::Changed)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl Cursor<'_> {
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.at)
    }
    fn skip(&mut self, count: usize) -> Result<()> {
        self.at = self
            .at
            .checked_add(count)
            .filter(|at| *at <= self.bytes.len())
            .ok_or(Failure::Corrupt("frame is truncated"))?;
        Ok(())
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let start = self.at;
        self.skip(N)?;
        self.bytes[start..self.at]
            .try_into()
            .map_err(|_| Failure::Corrupt("frame is truncated"))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    fn checkpoint(&mut self) -> Result<DurableSinkCheckpoint> {
        Ok(DurableSinkCheckpoint {
            topic_partition: TopicPartition::new(
                TopicId::new(u128::from_le_bytes(self.array()?)),
                LogicalPartitionId::new(self.u32()?),
            ),
            next_placement_sequence: PlacementSequence::new(self.u64()?),
            next_offset: LogicalOffset::new(self.u64()?),
        })
    }
}

#[cfg(test)]
mod tests;
