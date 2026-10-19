//! Actual append workers registered with the consuming host's lifetime owner.

use std::fmt;
use std::io;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;
use std::time::Instant;

use rayon::{ThreadPool, ThreadPoolBuildError, ThreadPoolBuilder};
use shard_stream_engine::EngineHandle;

use crate::{TelemetryRecoveryBudget, TelemetryRecoveryError, TelemetryRecoveryUsage};

const MAX_WORKERS: usize = 64;
const NAME_PREFIX: &[u8] = b"shard-telemetry-append-";

#[cfg(test)]
mod tests;

/// A host-owned registry of actual native tasks, rather than detached callbacks.
///
/// Implementations must enforce their own cumulative task/name/stack limits and
/// the supplied original expiry before spawning. Each entered native thread and
/// its actual result/panic must be retained by the host before returning, even
/// when a post-spawn clock check returns an error. The host's non-clone owner
/// must physically join those threads before releasing engine/directory custody.
///
/// `stack_bytes` is the explicit requested stack size; the registry must use it
/// or refuse before spawning. A registry configured with one stack size can
/// require exact equality. OS stack rounding and allocator overhead are not
/// represented by this declared size.
pub trait TelemetryTaskRegistry: Send + Sync {
    /// Registers one actual native thread using this same initiating deadline.
    ///
    /// Once entered, `worker` must run to completion even if setup has expired
    /// or shutdown was requested. In particular, skipping a Rayon worker loop
    /// can strand already-entered pool work. Drop the pool to request its exit,
    /// then join the registered tasks. Dropping a registration client must not
    /// detach or discard actual entered task outcomes.
    fn spawn_until(
        &self,
        name: &str,
        stack_bytes: usize,
        original_expiry: Instant,
        worker: Box<dyn FnOnce() + Send>,
    ) -> io::Result<()>;
}

/// Explicit finite bounds for the actual append worker producer.
#[derive(Clone, Copy, Debug)]
pub struct TelemetryAppendPoolLimits {
    /// Exact actual worker count, in `1..=64`; no environment-derived default.
    pub workers: usize,
    /// Positive explicit requested native stack bytes per worker.
    pub thread_stack_bytes: usize,
    /// Largest worker name accepted by the host registry.
    pub max_thread_name_bytes: usize,
}

/// Complete engine custody and the actual pool using host-registered workers.
///
/// Pool setup uses the supplied recovery ledger and its saved original expiry;
/// callers must pass the same ledger through other setup/recovery producers.
/// Workers retain the complete [`EngineHandle`] until their actual Rayon loop
/// returns, including during partial construction, panic and late refusal.
///
/// This producer bounds actual worker count, requested stacks, names and known
/// callback state. Rayon privately allocates bounded per-worker scheduler state
/// using its ordinary allocator; those inaccessible allocations and allocator
/// overhead are not a measured byte/RSS guarantee. This API neither recovers
/// catalogs/journals/object tiers nor qualifies a complete owned store setup.
pub struct TelemetryAppendPool {
    // Declaration order matters: request worker termination before releasing
    // the registration client or this owner's complete engine handle.
    pool: ThreadPool,
    _registry: Arc<dyn TelemetryTaskRegistry>,
    engine: EngineHandle,
}

impl TelemetryAppendPool {
    /// Builds a real pool whose every worker enters the actual host registry.
    ///
    /// Spends all known worker/stack/callback charges before entering Rayon's
    /// constructor, and rechecks the saved expiry around every registration.
    /// An actual pool-build error takes precedence over a later clock check.
    /// A successfully built pool refused by the final check is retained in the
    /// returned error. Drop that pool/error before asking the registry to join.
    pub fn build_until(
        engine: EngineHandle,
        registry: Arc<dyn TelemetryTaskRegistry>,
        limits: TelemetryAppendPoolLimits,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<Self, TelemetryAppendPoolSetupError> {
        let failure = |cause| TelemetryAppendPoolSetupError {
            cause,
            retained_pool: None,
        };
        budget
            .check()
            .map_err(|error| failure(TelemetryAppendPoolSetupFailure::Recovery(error)))?;
        let longest_name = NAME_PREFIX.len() + if limits.workers > 10 { 2 } else { 1 };
        if !(1..=MAX_WORKERS).contains(&limits.workers)
            || limits.thread_stack_bytes == 0
            || limits.thread_stack_bytes > isize::MAX as usize
            || limits.max_thread_name_bytes < longest_name
            || limits.max_thread_name_bytes > isize::MAX as usize
        {
            return Err(failure(TelemetryAppendPoolSetupFailure::Recovery(
                TelemetryRecoveryError::Limits,
            )));
        }
        let per_worker = limits
            .thread_stack_bytes
            .checked_add(size_of::<(rayon::ThreadBuilder, EngineHandle)>())
            .and_then(|value| value.checked_add(size_of::<Box<dyn FnOnce() + Send>>()));
        let declared = per_worker
            .and_then(|bytes| bytes.checked_mul(limits.workers))
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()));
        let names = (0..limits.workers).try_fold(0_usize, |bytes, index| {
            bytes.checked_add(NAME_PREFIX.len() + if index >= 10 { 2 } else { 1 })
        });
        let (Some(declared), Some(names)) = (declared, names) else {
            return Err(failure(TelemetryAppendPoolSetupFailure::Recovery(
                TelemetryRecoveryError::Limits,
            )));
        };
        budget
            .charge(TelemetryRecoveryUsage {
                records: limits.workers,
                string_bytes: names,
                retained_bytes: declared,
                work_bytes: declared,
                ..TelemetryRecoveryUsage::default()
            })
            .map_err(|error| failure(TelemetryAppendPoolSetupFailure::Recovery(error)))?;
        let original_expiry = budget.original_expiry();
        let pool = ThreadPoolBuilder::new()
            .num_threads(limits.workers)
            .stack_size(limits.thread_stack_bytes)
            .spawn_handler(|thread| {
                budget.check().map_err(io::Error::other)?;
                if thread.index() >= limits.workers
                    || thread.stack_size() != Some(limits.thread_stack_bytes)
                {
                    return Err(io::Error::other("unexpected append worker configuration"));
                }
                // Names use a fixed stack buffer; the host owns any copied name
                // allocation under its separately enforced registry allowance.
                let mut name = [0_u8; NAME_PREFIX.len() + 2];
                name[..NAME_PREFIX.len()].copy_from_slice(NAME_PREFIX);
                let index = thread.index();
                let used = if index >= 10 {
                    name[NAME_PREFIX.len()] = b'0' + (index / 10) as u8;
                    name[NAME_PREFIX.len() + 1] = b'0' + (index % 10) as u8;
                    NAME_PREFIX.len() + 2
                } else {
                    name[NAME_PREFIX.len()] = b'0' + index as u8;
                    NAME_PREFIX.len() + 1
                };
                let name = std::str::from_utf8(&name[..used]).map_err(io::Error::other)?;
                budget
                    .charge(TelemetryRecoveryUsage {
                        work_bytes: used + 1,
                        ..TelemetryRecoveryUsage::default()
                    })
                    .map_err(io::Error::other)?;
                let retained_engine = engine.clone();
                let worker = move || {
                    thread.run();
                    drop(retained_engine);
                };
                // The compiler chooses closure layout. Admit any measured
                // excess over the prepaid tuple before its actual Box allocation.
                let extra = size_of_val(&worker)
                    .saturating_sub(size_of::<(rayon::ThreadBuilder, EngineHandle)>());
                budget
                    .charge(TelemetryRecoveryUsage {
                        retained_bytes: extra,
                        work_bytes: extra,
                        ..TelemetryRecoveryUsage::default()
                    })
                    .map_err(io::Error::other)?;
                registry.spawn_until(
                    name,
                    limits.thread_stack_bytes,
                    original_expiry,
                    Box::new(worker),
                )?;
                budget.check().map_err(io::Error::other)
            })
            .build()
            .map_err(|error| failure(TelemetryAppendPoolSetupFailure::Build(error)))?;
        let owner = Self {
            pool,
            _registry: registry,
            engine,
        };
        if let Err(error) = budget.check() {
            return Err(TelemetryAppendPoolSetupError {
                cause: TelemetryAppendPoolSetupFailure::Recovery(error),
                retained_pool: Some(owner),
            });
        }
        Ok(owner)
    }

    /// Executes real partition-parallel append preparation on this pool.
    ///
    /// This is ordinary live work, not an installation deadline extension or a
    /// runtime command budget. The consuming operation enforces its own limits.
    pub fn install<OP, R>(&self, operation: OP) -> R
    where
        OP: FnOnce() -> R + Send,
        R: Send,
    {
        self.pool.install(operation)
    }

    /// Number of actual Rayon worker slots in this successfully built pool.
    pub fn worker_count(&self) -> usize {
        self.pool.current_num_threads()
    }

    /// Tests whether this pool retains the exact supplied complete engine owner.
    pub fn retains_engine(&self, engine: &EngineHandle) -> bool {
        self.engine.ptr_eq(engine)
    }
}

impl fmt::Debug for TelemetryAppendPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelemetryAppendPool")
            .field("workers", &self.worker_count())
            .finish_non_exhaustive()
    }
}

/// Actual builder/registration failure or the shared cumulative refusal.
#[derive(Debug)]
pub enum TelemetryAppendPoolSetupFailure {
    /// Genuine shared recovery ledger/deadline refusal.
    Recovery(TelemetryRecoveryError),
    /// Actual Rayon error, including its underlying registration I/O error.
    Build(ThreadPoolBuildError),
}

/// Retains a successfully built native pool if its final admission is late.
#[derive(Debug)]
pub struct TelemetryAppendPoolSetupError {
    cause: TelemetryAppendPoolSetupFailure,
    retained_pool: Option<TelemetryAppendPool>,
}

impl TelemetryAppendPoolSetupError {
    /// Actual failure observed without replacing it with a later timeout.
    pub fn cause(&self) -> &TelemetryAppendPoolSetupFailure {
        &self.cause
    }

    /// Whether this error owns a complete successfully built but refused pool.
    pub fn has_retained_pool(&self) -> bool {
        self.retained_pool.is_some()
    }
}

impl fmt::Display for TelemetryAppendPoolSetupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            TelemetryAppendPoolSetupFailure::Recovery(error) => {
                write!(formatter, "append worker setup refused: {error}")
            }
            TelemetryAppendPoolSetupFailure::Build(error) => {
                write!(formatter, "append worker setup failed: {error}")
            }
        }
    }
}

impl std::error::Error for TelemetryAppendPoolSetupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match &self.cause {
            TelemetryAppendPoolSetupFailure::Recovery(error) => error,
            TelemetryAppendPoolSetupFailure::Build(error) => error,
        })
    }
}
