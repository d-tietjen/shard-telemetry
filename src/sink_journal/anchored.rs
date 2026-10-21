//! Opaque successful ownership from the actual descriptor-anchored producer.
//! This records source provenance, never namespace/currentness/HA permission.

use super::*;
use crate::{TelemetryRecoveryBudget, TelemetryRecoveryError, TelemetryRecoveryUsage};
use std::any::Any;
use std::fmt;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// Genuine anchored journal and every original replay row. No constructor from
/// an ordinary journal, mutable owner extraction, reset or Clone is provided.
pub struct AnchoredSinkJournal {
    journal: Arc<SinkJournal>,
    replay: Vec<RecoveredTransaction>,
    original_expiry: Instant,
    #[allow(
        dead_code,
        reason = "retain the original successful setup ledger for diagnostics without live authority"
    )]
    setup_usage: TelemetryRecoveryUsage,
    denied: AtomicBool,
    // LAST: original supplied directory descriptor outlives journal/replay.
    #[allow(
        dead_code,
        reason = "retain the original directory descriptor LAST through all journal and replay cleanup"
    )]
    directory: File,
}
impl fmt::Debug for AnchoredSinkJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnchoredSinkJournal")
            .field("recovered_transactions", &self.replay.len())
            .field("denied", &self.is_denied())
            .finish_non_exhaustive()
    }
}
impl AnchoredSinkJournal {
    /// Consume the supplied actual directory descriptor through the real
    /// anchored native producer. This is provenance, not current authority.
    #[allow(
        clippy::result_large_err,
        reason = "retain the complete actual anchored setup owner inline on refusal"
    )]
    pub fn open_until(
        directory: File,
        shard_id: ShardId,
        runtime_max_bytes: u64,
        sync_each_append: bool,
        limits: AnchoredJournalLimits,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<Arc<Self>, RetainedAnchoredJournalError> {
        SinkJournal::open_anchored_until(
            directory,
            shard_id,
            runtime_max_bytes,
            sync_each_append,
            SinkJournalRecoveryLimits {
                max_file_bytes: limits.max_file_bytes,
                max_frame_bytes: limits.max_frame_bytes,
                max_transactions: limits.max_transactions,
                max_appends: limits.max_appends,
                max_payload_bytes: limits.max_payload_bytes,
                max_io_attempts: limits.max_io_attempts,
                max_io_bytes: limits.max_io_bytes,
            },
            budget,
        )
    }
    /// Open an application-edge journal with a distinct persisted header.
    /// Only a genuine held global application-map transaction can append it.
    /// Its sparse physical-shard rows still require complete cross-anchor
    /// adoption before query/application publication; this grants no authority.
    #[allow(
        clippy::result_large_err,
        reason = "retain every actual descriptor and native setup result inline"
    )]
    pub fn open_application_until(
        directory: File,
        shard_id: ShardId,
        runtime_max_bytes: u64,
        sync_each_append: bool,
        limits: AnchoredJournalLimits,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<Arc<Self>, RetainedAnchoredJournalError> {
        SinkJournal::open_anchored_format_until(
            directory,
            shard_id,
            runtime_max_bytes,
            sync_each_append,
            SinkJournalRecoveryLimits {
                max_file_bytes: limits.max_file_bytes,
                max_frame_bytes: limits.max_frame_bytes,
                max_transactions: limits.max_transactions,
                max_appends: limits.max_appends,
                max_payload_bytes: limits.max_payload_bytes,
                max_io_attempts: limits.max_io_attempts,
                max_io_bytes: limits.max_io_bytes,
            },
            JournalFormat::Application,
            budget,
        )
    }
    pub(crate) fn format(&self) -> JournalFormat {
        self.journal.format
    }
    pub(crate) fn owns_journal(&self, journal: &SinkJournal) -> bool {
        std::ptr::eq(self.journal.as_ref(), journal)
    }
    /// Distinct entry: the private seal borrows the actual held application map
    /// and original work for the entire synchronous physical settlement.
    pub(crate) fn append_application_journal_until(
        &self,
        edge: &crate::sink::ApplicationJournalEdge<'_>,
        appends: &[DurableAppend],
        limits: JournalAppendLimits,
    ) -> shard_stream_engine::NativeResumeCallbackResult<crate::sink::JournalCallbackReceipt> {
        if self.is_denied()
            || !edge.matches_anchor(self)
            || self.format() != JournalFormat::Application
        {
            return Err(shard_stream_engine::NativeResumeCallbackError::unsupported(
                "application journal requires its actual held global-map edge",
            ));
        }
        crate::sink::append_application_journal_until(&self.journal, edge, appends, limits)
    }
    /// Enter only the real bounded append bridge, retaining its complete actual
    /// result in the caller. No ordinary mutable journal handle escapes. The
    /// caller must save this result before its post-physical negative/E check.
    pub(crate) fn append_journal_until(
        &self,
        expected: DurableSinkCheckpoint,
        appends: &[DurableAppend],
        next: DurableSinkCheckpoint,
        limits: JournalAppendLimits,
        work: &shard_stream_engine::NativeResumeWork,
    ) -> shard_stream_engine::NativeResumeCallbackResult<crate::sink::JournalCallbackReceipt> {
        if self.is_denied() {
            return Err(shard_stream_engine::NativeResumeCallbackError::unsupported(
                "anchored journal is terminal",
            ));
        }
        crate::sink::append_journal_until(&self.journal, expected, appends, next, limits, work)
    }
    /// Exact recovered inputs, not evidence of application/index completion.
    pub(crate) fn recovered(&self) -> &[RecoveredTransaction] {
        &self.replay
    }
    pub(super) fn recovered_capacity(&self) -> usize {
        self.replay.capacity()
    }
    pub(crate) fn shard_id(&self) -> ShardId {
        self.journal.shard_id
    }
    /// Original setup observation; live commands require their own genuine E.
    pub(crate) fn original_expiry(&self) -> Instant {
        self.original_expiry
    }
    /// Actual cumulative recovery fees at successful ownership publication.
    #[allow(
        dead_code,
        reason = "retain the original setup usage diagnostic; live commands require separate admission"
    )]
    pub(crate) fn setup_usage(&self) -> TelemetryRecoveryUsage {
        self.setup_usage
    }
    /// Sticky negative-only transaction refusal; no permission is produced.
    pub(crate) fn deny(&self) {
        self.denied.store(true, Ordering::Release);
    }
    pub(crate) fn is_denied(&self) -> bool {
        self.denied.load(Ordering::Acquire)
    }
}

struct Attempt {
    native: Option<
        std::result::Result<(SinkJournal, Vec<RecoveredTransaction>), RetainedSinkJournalError>,
    >,
    journal: Option<Arc<SinkJournal>>,
    replay: Option<Vec<RecoveredTransaction>>,
    completed: Option<Arc<AnchoredSinkJournal>>,
    native_completed: bool,
    panic: Option<Box<dyn Any + Send>>,
    expiry: Instant,
    usage: TelemetryRecoveryUsage,
    // Original owned input enters custody before every admission/check.
    directory: Option<File>,
}
#[derive(Debug)]
pub(crate) enum AnchoredJournalFailure {
    Recovery(TelemetryRecoveryError),
    Native,
    Panic,
    State,
}
/// Terminal source-preserving setup error, including an actual late success.
pub struct RetainedAnchoredJournalError {
    pub(crate) cause: AnchoredJournalFailure,
    retained: Attempt,
}

/// Finite native anchored journal replay/repair ceilings, spent on the supplied
/// same recovery ledger. These do not grant namespace or writer permission.
#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
pub struct AnchoredJournalLimits {
    /// Largest regular journal accepted before reading.
    pub max_file_bytes: usize,
    /// Largest actual frame accepted.
    pub max_frame_bytes: usize,
    /// Cumulative transaction count.
    pub max_transactions: usize,
    /// Cumulative append count across all transactions.
    pub max_appends: usize,
    /// Cumulative actual retained payload bytes.
    pub max_payload_bytes: usize,
    /// Native read/repair/sync attempts including prepaid settlement.
    pub max_io_attempts: usize,
    /// Total admitted native byte requests and truncated-tail bytes.
    pub max_io_bytes: usize,
}
impl fmt::Debug for RetainedAnchoredJournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedAnchoredJournalError")
            .field("cause", &self.cause)
            .field("native_completed", &self.retained.native_completed)
            .field("completed_owner", &self.retained.completed.is_some())
            .field("panic", &self.retained.panic.is_some())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedAnchoredJournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for RetainedAnchoredJournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            AnchoredJournalFailure::Recovery(e) => Some(e),
            AnchoredJournalFailure::Native => self
                .retained
                .native
                .as_ref()?
                .as_ref()
                .err()
                .map(|e| e as &(dyn std::error::Error + 'static)),
            _ => None,
        }
    }
}
impl RetainedAnchoredJournalError {
    /// Whether the unchanged native constructor actually returned success.
    #[allow(
        dead_code,
        reason = "preserve original native setup outcomes without a descriptor or partial-owner extractor"
    )]
    pub(crate) fn native_completed(&self) -> bool {
        self.retained.native_completed
    }
    /// A completed opaque owner retained after a later wrapper refusal.
    #[allow(
        dead_code,
        reason = "preserve original native setup outcomes without a descriptor or partial-owner extractor"
    )]
    pub(crate) fn completed_owner(&self) -> Option<&AnchoredSinkJournal> {
        self.retained.completed.as_deref()
    }
    /// Actual native error, without a descriptor or partial-owner extractor.
    #[allow(
        dead_code,
        reason = "preserve original native setup outcomes without a descriptor or partial-owner extractor"
    )]
    pub(crate) fn native_error(&self) -> Option<&RetainedSinkJournalError> {
        self.retained.native.as_ref()?.as_ref().err()
    }
    #[allow(
        dead_code,
        reason = "preserve original native setup outcomes without a descriptor or partial-owner extractor"
    )]
    pub(crate) fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.retained.panic.as_deref()
    }
}

fn arc_bytes<T>() -> Option<usize> {
    (2 * size_of::<usize>())
        .checked_add(std::mem::align_of::<T>().checked_sub(1)?)?
        .checked_add(size_of::<T>())
}
/// Fixed by-value wrapper/result and two actual Arc allocation requests. The
/// existing native producer's limits/fees are additional on the SAME ledger.
pub(crate) fn anchored_journal_cost() -> Option<TelemetryRecoveryUsage> {
    let n = size_of::<Attempt>()
        .checked_add(size_of::<RetainedAnchoredJournalError>())?
        .checked_add(arc_bytes::<SinkJournal>()?)?
        .checked_add(arc_bytes::<AnchoredSinkJournal>()?)?;
    Some(TelemetryRecoveryUsage {
        retained_bytes: n,
        work_bytes: n.checked_mul(3)?,
        ..Default::default()
    })
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
type AnchoredProbe = Box<dyn FnMut(Instant, &'static str)>;

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
thread_local! {
    static PROBE: std::cell::RefCell<Option<AnchoredProbe>> =
        const { std::cell::RefCell::new(None) };
}
#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
pub(crate) fn set_anchored_probe(probe: Option<Box<dyn FnMut(Instant, &'static str)>>) {
    PROBE.with(|p| *p.borrow_mut() = probe);
}
fn observed(expiry: Instant, phase: &'static str) {
    #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
    PROBE.with(|p| {
        if let Some(p) = &mut *p.borrow_mut() {
            p(expiry, phase);
        }
    });
    #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
    let _ = (expiry, phase);
}

impl SinkJournal {
    /// Consume an actual directory descriptor through the unchanged real
    /// `open_until`, retaining all replay and actual results. Wrapper/Arc/result
    /// fees are paid on the original recovery ledger before native entry.
    /// The opaque success cannot be constructed from an ordinary journal.
    /// Its setup expiry never grants a later live command permission.
    #[allow(
        clippy::result_large_err,
        reason = "retain complete original owners and native outcomes inline without a refusal allocation"
    )]
    pub(crate) fn open_anchored_until(
        directory: File,
        shard_id: ShardId,
        runtime_max_bytes: u64,
        sync_each_append: bool,
        limits: SinkJournalRecoveryLimits,
        budget: &mut TelemetryRecoveryBudget,
    ) -> std::result::Result<Arc<AnchoredSinkJournal>, RetainedAnchoredJournalError> {
        Self::open_anchored_format_until(
            directory,
            shard_id,
            runtime_max_bytes,
            sync_each_append,
            limits,
            JournalFormat::Legacy,
            budget,
        )
    }
    #[allow(
        clippy::too_many_arguments,
        clippy::result_large_err,
        reason = "keep the actual native setup inputs and explicit persisted producer domain"
    )]
    pub(super) fn open_anchored_format_until(
        directory: File,
        shard_id: ShardId,
        runtime_max_bytes: u64,
        sync_each_append: bool,
        limits: SinkJournalRecoveryLimits,
        format: JournalFormat,
        budget: &mut TelemetryRecoveryBudget,
    ) -> std::result::Result<Arc<AnchoredSinkJournal>, RetainedAnchoredJournalError> {
        let mut retained = Attempt {
            native: None,
            journal: None,
            replay: None,
            completed: None,
            native_completed: false,
            panic: None,
            expiry: budget.original_expiry(),
            usage: budget.usage(),
            directory: Some(directory),
        };
        let actual = catch_unwind(AssertUnwindSafe(|| {
            budget
                .charge(
                    anchored_journal_cost().ok_or(AnchoredJournalFailure::Recovery(
                        TelemetryRecoveryError::Limits,
                    ))?,
                )
                .map_err(AnchoredJournalFailure::Recovery)?;
            let directory = retained
                .directory
                .as_ref()
                .ok_or(AnchoredJournalFailure::State)?;
            retained.native = Some(match format {
                JournalFormat::Legacy => SinkJournal::open_until(
                    directory,
                    shard_id,
                    runtime_max_bytes,
                    sync_each_append,
                    limits,
                    budget,
                ),
                JournalFormat::Application => SinkJournal::open_format_until(
                    directory,
                    shard_id,
                    runtime_max_bytes,
                    sync_each_append,
                    limits,
                    format,
                    budget,
                ),
            });
            retained.native_completed = retained.native.as_ref().is_some_and(|r| r.is_ok());
            retained.usage = budget.usage();
            observed(retained.expiry, "native-opened");
            if !retained.native_completed {
                return Err(AnchoredJournalFailure::Native);
            }
            budget.check().map_err(AnchoredJournalFailure::Recovery)?;
            let (journal, rows) = match retained.native.take() {
                Some(Ok(value)) => value,
                other => {
                    retained.native = other;
                    return Err(AnchoredJournalFailure::State);
                }
            };
            retained.replay = Some(rows);
            retained.journal = Some(Arc::new(journal));
            observed(retained.expiry, "journal-arc");
            budget.check().map_err(AnchoredJournalFailure::Recovery)?;
            retained.completed = Some(Arc::new(AnchoredSinkJournal {
                journal: retained
                    .journal
                    .take()
                    .ok_or(AnchoredJournalFailure::State)?,
                replay: retained
                    .replay
                    .take()
                    .ok_or(AnchoredJournalFailure::State)?,
                original_expiry: retained.expiry,
                setup_usage: retained.usage,
                denied: AtomicBool::new(false),
                directory: retained
                    .directory
                    .take()
                    .ok_or(AnchoredJournalFailure::State)?,
            }));
            observed(retained.expiry, "anchored-owner");
            budget.check().map_err(AnchoredJournalFailure::Recovery)
        }));
        retained.usage = budget.usage();
        let cause = match actual {
            Ok(Ok(())) => {
                if let Some(owner) = retained.completed.take() {
                    return Ok(owner);
                }
                AnchoredJournalFailure::State
            }
            Ok(Err(cause)) => cause,
            Err(payload) => {
                retained.panic = Some(payload);
                AnchoredJournalFailure::Panic
            }
        };
        if let Some(owner) = &retained.completed {
            owner.deny();
        }
        Err(RetainedAnchoredJournalError { cause, retained })
    }
}
