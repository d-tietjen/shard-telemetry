//! Genuine admitted empty log-stripe construction for the owned sink factory.

use super::*;
use crate::{TelemetryRecoveryBudget, TelemetryRecoveryError, TelemetryRecoveryUsage};
use std::any::Any;
use std::collections::TryReserveError;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

/// Finite source values for constructing one actual empty log stripe.
#[derive(Clone, Copy, Debug)]
pub struct LogStripeSetupLimits {
    /// Maximum configured future dictionary-cache capacity; no payload is allocated now.
    pub max_dictionary_cache_bytes: usize,
    /// Maximum actual allocated persistent locality slots.
    pub max_locality_slots: usize,
    /// Maximum total capacity of the two ordinary fixed lookup-cache vectors.
    pub max_cache_slots: usize,
    /// Prepaid native context reservation, checked against actual native sizeof.
    /// This does not prevent a larger native allocation or bound allocator/RSS.
    pub max_native_context_bytes: usize,
    /// Maximum cumulative declared state, including measured capacity excess.
    pub max_retained_bytes: usize,
    /// Maximum cumulative source work for this producer.
    pub max_work_bytes: usize,
}

/// Actual retained setup observations, including results before later refusal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogStripeSetupObservation {
    /// Actual message lookup-cache vector capacity.
    pub message_cache_capacity: usize,
    /// Actual field lookup-cache vector capacity.
    pub field_cache_capacity: usize,
    /// Actual locality profile vector capacity.
    pub locality_capacity: usize,
    /// The actual bulk Default/CCtx native constructor was entered.
    pub native_creation_entered: bool,
    /// Native sizeof immediately after creation, parameter setup and dictionary load.
    pub native_context_bytes: [Option<usize>; 3],
    /// Original native compression-level setter Result, without error-string conversion.
    pub level_result: Option<Result<usize, usize>>,
    /// Original native empty-dictionary load Result.
    pub dictionary_result: Option<Result<usize, usize>>,
    /// A complete actual stripe was assembled, including on late refusal.
    pub assembled: bool,
}

/// Setup refusal; complete inputs and partial native/Rust state remain owned.
#[derive(Debug)]
pub enum LogStripeSetupFailure {
    /// Original shared recovery ledger or expiry refused.
    Recovery(TelemetryRecoveryError),
    /// A finite local policy or checked arithmetic refused.
    Limits,
    /// The ordinary configuration validation returned its actual error.
    Configuration(TelemetryError),
    /// A real vector reservation returned an allocation error.
    Allocation,
    /// An entered native parameter/dictionary operation returned an error.
    Native,
    /// An entered constructor or observer unwound.
    Panic,
    /// A consuming setup invariant refused.
    State,
}
type Failure = LogStripeSetupFailure;
type SetupResult<T> = Result<T, Failure>;

struct Attempt {
    shard: ShardId,
    config: Option<StripeConfig>,
    limits: LogStripeSetupLimits,
    expiry: Instant,
    usage: TelemetryRecoveryUsage,
    local_usage: TelemetryRecoveryUsage,
    observation: LogStripeSetupObservation,
    allocation: [Option<Result<(), TryReserveError>>; 3],
    message_cache: Vec<Option<CachedMessageTerms>>,
    field_cache: Vec<Option<CachedFields>>,
    collator: Option<CompressionBlockCollator>,
    dictionary_cache: Option<DictionaryCache>,
    native: Option<zstd::bulk::Compressor<'static>>,
    panic: Option<Box<dyn Any + Send>>,
    // The actual completed owner survives any final check/observer refusal.
    stripe: Option<LogStripe>,
}

/// Opaque success from actual admitted construction, consumed by the owned factory.
/// No conversion from an ordinary stripe or mutable native owner is provided.
pub struct PreparedLogStripe {
    retained: Attempt,
    stripe: LogStripe,
}
/// Terminal whole-owner error. It exposes observations, never a retryable stripe.
pub struct RetainedLogStripeSetupError {
    /// Actual setup refusal classification.
    pub cause: LogStripeSetupFailure,
    retained: Attempt,
}
impl std::fmt::Debug for PreparedLogStripe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedLogStripe")
            .field("shard", &self.stripe.stream_shard_id)
            .field("observation", &self.retained.observation)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for RetainedLogStripeSetupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedLogStripeSetupError")
            .field("cause", &self.cause)
            .field("shard", &self.retained.shard)
            .field("observation", &self.retained.observation)
            .field("panic", &self.retained.panic.is_some())
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for RetainedLogStripeSetupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("admitted log stripe setup refused; complete state retained")
    }
}
impl std::error::Error for RetainedLogStripeSetupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            Failure::Recovery(e) => Some(e),
            Failure::Configuration(e) => Some(e),
            Failure::Allocation => self.retained.allocation.iter().find_map(|r| {
                r.as_ref()?
                    .as_ref()
                    .err()
                    .map(|e| e as &dyn std::error::Error)
            }),
            _ => None,
        }
    }
}
impl PreparedLogStripe {
    /// Original setup expiry, for a composing factory's minimum startup cap.
    /// This historical observation supplies no live command permission.
    pub fn original_expiry(&self) -> Instant {
        self.retained.expiry
    }

    /// Physical shard of the actual completed stripe.
    pub fn stream_shard_id(&self) -> ShardId {
        self.stripe.stream_shard_id
    }
    /// Retained actual native and capacity observations from setup.
    pub fn observation(&self) -> LogStripeSetupObservation {
        self.retained.observation
    }
    /// Original shared-ledger cumulative observation at successful setup.
    pub fn setup_usage(&self) -> TelemetryRecoveryUsage {
        self.retained.usage
    }
    pub(crate) fn into_stripe(self) -> LogStripe {
        self.stripe
    }
}
impl RetainedLogStripeSetupError {
    /// Actual native/capacity observations before failure.
    pub fn observation(&self) -> LogStripeSetupObservation {
        self.retained.observation
    }
    /// Actual original configuration, including after late complete assembly.
    pub fn config(&self) -> Option<&StripeConfig> {
        self.retained
            .config
            .as_ref()
            .or_else(|| self.retained.stripe.as_ref().map(|stripe| &stripe.config))
    }
    /// Actual cumulative original-ledger observation at terminal return.
    pub fn setup_usage(&self) -> TelemetryRecoveryUsage {
        self.retained.usage
    }
}
fn add(a: usize, b: usize) -> SetupResult<usize> {
    a.checked_add(b)
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or(Failure::Limits)
}
fn mul(a: usize, b: usize) -> SetupResult<usize> {
    a.checked_mul(b)
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or(Failure::Limits)
}
impl LogStripeSetupLimits {
    /// Plain checked source sizing. Native allocation, files and tasks are not entered.
    /// Actual producer charges this on its original ledger before configuration/native work.
    pub fn setup_cost(&self, config: &StripeConfig) -> SetupResult<TelemetryRecoveryUsage> {
        if [
            self.max_dictionary_cache_bytes,
            self.max_locality_slots,
            self.max_cache_slots,
            self.max_native_context_bytes,
            self.max_retained_bytes,
            self.max_work_bytes,
        ]
        .into_iter()
        .any(|n| n == 0 || n > isize::MAX as usize)
            || config.dictionary_cache_bytes > self.max_dictionary_cache_bytes
            || config.compression_locality.max_compression_shards > self.max_locality_slots
            || add(MESSAGE_TERM_CACHE_ENTRIES, FIELD_CACHE_ENTRIES)? > self.max_cache_slots
        {
            return Err(Failure::Limits);
        }
        let cache = add(
            mul(
                MESSAGE_TERM_CACHE_ENTRIES,
                size_of::<Option<CachedMessageTerms>>(),
            )?,
            mul(FIELD_CACHE_ENTRIES, size_of::<Option<CachedFields>>())?,
        )?;
        let locality = mul(
            config.compression_locality.max_compression_shards,
            CompressionBlockCollator::stripe_setup_slot_bytes(),
        )?;
        let fixed = add(
            size_of::<Attempt>(),
            add(
                size_of::<PreparedLogStripe>(),
                add(
                    size_of::<RetainedLogStripeSetupError>(),
                    add(size_of::<LogStripe>(), 2 * size_of::<StripeConfig>())?,
                )?,
            )?,
        )?;
        let bytes = add(
            fixed,
            add(cache, add(locality, self.max_native_context_bytes)?)?,
        )?;
        // Covers fixed/config validation (including the two native level-bound
        // queries), vector initialization, create/set/load and three sizeof
        // observations. This is admitted source work, not a native-cycle bound.
        let work = mul(bytes, 3)?;
        if bytes > self.max_retained_bytes || work > self.max_work_bytes {
            return Err(Failure::Limits);
        }
        Ok(TelemetryRecoveryUsage {
            retained_bytes: bytes,
            work_bytes: work,
            ..Default::default()
        })
    }
}
impl LogStripe {
    /// Construct the real empty stripe under the SAME recovery ledger and E.
    /// Partial vectors, native Results/context and late success remain in errors.
    /// The native context ceiling is a reservation/sizeof policy, not an allocator,
    /// RSS or native-call latency bound. Later ingest/query/Drop is outside setup.
    #[allow(
        clippy::result_large_err,
        reason = "retain original config and complete partial/native stripe owners inline"
    )]
    pub fn prepare_until(
        stream_shard_id: ShardId,
        config: StripeConfig,
        limits: LogStripeSetupLimits,
        budget: &mut TelemetryRecoveryBudget,
    ) -> Result<PreparedLogStripe, RetainedLogStripeSetupError> {
        let mut retained = Attempt {
            shard: stream_shard_id,
            config: Some(config),
            limits,
            expiry: budget.original_expiry(),
            usage: budget.usage(),
            local_usage: TelemetryRecoveryUsage::default(),
            observation: Default::default(),
            allocation: [None, None, None],
            message_cache: Vec::new(),
            field_cache: Vec::new(),
            collator: None,
            dictionary_cache: None,
            native: None,
            panic: None,
            stripe: None,
        };
        let actual = catch_unwind(AssertUnwindSafe(|| retained.run(budget)));
        retained.usage = budget.usage();
        let cause = match actual {
            Ok(Ok(())) => match retained.stripe.take() {
                Some(stripe) => return Ok(PreparedLogStripe { retained, stripe }),
                None => Failure::State,
            },
            Ok(Err(cause)) => cause,
            Err(payload) => {
                retained.panic = Some(payload);
                Failure::Panic
            }
        };
        // Terminal shared allowance as well as terminal retained setup state.
        let _ = budget.invalid::<()>();
        Err(RetainedLogStripeSetupError { cause, retained })
    }
}
impl Attempt {
    fn observed(
        &mut self,
        budget: &mut TelemetryRecoveryBudget,
        phase: &'static str,
    ) -> SetupResult<()> {
        self.usage = budget.usage();
        #[cfg(test)]
        tests::after_phase(self.expiry, phase);
        #[cfg(not(test))]
        let _ = phase;
        budget.check().map_err(Failure::Recovery)
    }
    fn extra(&mut self, budget: &mut TelemetryRecoveryBudget, bytes: usize) -> SetupResult<()> {
        let next = add(self.local_usage.retained_bytes, bytes)?;
        let work = add(self.local_usage.work_bytes, mul(bytes, 3)?)?;
        // Local totals include only this producer's deltas; the original ledger
        // additionally retains every earlier composing producer's expenditure.
        if next > self.limits.max_retained_bytes || work > self.limits.max_work_bytes {
            return Err(Failure::Limits);
        }
        budget
            .charge(TelemetryRecoveryUsage {
                retained_bytes: bytes,
                work_bytes: mul(bytes, 3)?,
                ..Default::default()
            })
            .map_err(Failure::Recovery)?;
        self.local_usage.retained_bytes = next;
        self.local_usage.work_bytes = work;
        self.usage = budget.usage();
        Ok(())
    }
    fn native_size(
        &mut self,
        budget: &mut TelemetryRecoveryBudget,
        slot: usize,
    ) -> SetupResult<()> {
        let actual = self
            .native
            .as_mut()
            .ok_or(Failure::State)?
            .context_mut()
            .sizeof();
        self.observation.native_context_bytes[slot] = Some(actual);
        if actual > self.limits.max_native_context_bytes {
            self.extra(budget, actual - self.limits.max_native_context_bytes)?;
            return Err(Failure::Limits);
        }
        Ok(())
    }
    fn run(&mut self, budget: &mut TelemetryRecoveryBudget) -> SetupResult<()> {
        budget.check().map_err(Failure::Recovery)?;
        let config = self.config.as_ref().ok_or(Failure::State)?;
        let charge = self.limits.setup_cost(config)?;
        budget.charge(charge).map_err(Failure::Recovery)?;
        self.local_usage = charge;
        self.usage = budget.usage();
        config.validate().map_err(Failure::Configuration)?;
        self.observed(budget, "configuration")?;
        let config = self.config.as_ref().ok_or(Failure::State)?;
        self.dictionary_cache = Some(
            DictionaryCache::new(config.dictionary_cache_bytes).map_err(Failure::Configuration)?,
        );
        self.collator = Some(
            CompressionBlockCollator::empty_for_stripe_setup(
                config.compression_locality.clone(),
                config.target_block_bytes,
            )
            .map_err(Failure::Configuration)?,
        );
        self.allocation[0] = Some(
            self.collator
                .as_mut()
                .ok_or(Failure::State)?
                .reserve_for_stripe_setup(),
        );
        self.observation.locality_capacity = self
            .collator
            .as_ref()
            .ok_or(Failure::State)?
            .stripe_setup_capacity();
        let requested = self
            .config
            .as_ref()
            .ok_or(Failure::State)?
            .compression_locality
            .max_compression_shards;
        let excess = self.observation.locality_capacity.saturating_sub(requested);
        self.extra(
            budget,
            mul(excess, CompressionBlockCollator::stripe_setup_slot_bytes())?,
        )?;
        self.observed(budget, "locality-reserved")?;
        if self.allocation[0].as_ref().is_none_or(|r| r.is_err()) {
            return Err(Failure::Allocation);
        }
        if self.observation.locality_capacity > self.limits.max_locality_slots {
            return Err(Failure::Limits);
        }

        self.allocation[1] = Some(
            self.message_cache
                .try_reserve_exact(MESSAGE_TERM_CACHE_ENTRIES),
        );
        self.observation.message_cache_capacity = self.message_cache.capacity();
        self.extra(
            budget,
            mul(
                self.message_cache
                    .capacity()
                    .saturating_sub(MESSAGE_TERM_CACHE_ENTRIES),
                size_of::<Option<CachedMessageTerms>>(),
            )?,
        )?;
        self.observed(budget, "message-reserved")?;
        if self.allocation[1].as_ref().is_none_or(|r| r.is_err()) {
            return Err(Failure::Allocation);
        }
        if self.message_cache.capacity() > self.limits.max_cache_slots {
            return Err(Failure::Limits);
        }
        self.message_cache
            .resize_with(MESSAGE_TERM_CACHE_ENTRIES, || None);
        self.observed(budget, "message-initialized")?;
        self.allocation[2] = Some(self.field_cache.try_reserve_exact(FIELD_CACHE_ENTRIES));
        self.observation.field_cache_capacity = self.field_cache.capacity();
        self.extra(
            budget,
            mul(
                self.field_cache
                    .capacity()
                    .saturating_sub(FIELD_CACHE_ENTRIES),
                size_of::<Option<CachedFields>>(),
            )?,
        )?;
        self.observed(budget, "field-reserved")?;
        if self.allocation[2].as_ref().is_none_or(|r| r.is_err()) {
            return Err(Failure::Allocation);
        }
        if add(self.message_cache.capacity(), self.field_cache.capacity())?
            > self.limits.max_cache_slots
        {
            return Err(Failure::Limits);
        }
        self.field_cache.resize_with(FIELD_CACHE_ENTRIES, || None);
        self.observed(budget, "field-initialized")?;

        // Selected zstd0.14 bulk Default wraps safe8 CCtx::create: its sole
        // native call is ZSTD_createCCtx, with a NULL-only panic. Success is
        // stored before sizeof/hooks; the outer catch retains actual panic.
        self.observation.native_creation_entered = true;
        self.native = Some(zstd::bulk::Compressor::default());
        self.native_size(budget, 0)?;
        self.observed(budget, "native-created")?;
        let level = self
            .config
            .as_ref()
            .ok_or(Failure::State)?
            .compression_level;
        self.observation.level_result = Some(
            self.native
                .as_mut()
                .ok_or(Failure::State)?
                .context_mut()
                .set_parameter(zstd::zstd_safe::CParameter::CompressionLevel(level)),
        );
        self.native_size(budget, 1)?;
        self.observed(budget, "native-level")?;
        if self.observation.level_result.is_none_or(|r| r.is_err()) {
            return Err(Failure::Native);
        }
        self.observation.dictionary_result = Some(
            self.native
                .as_mut()
                .ok_or(Failure::State)?
                .context_mut()
                .load_dictionary(&[]),
        );
        self.native_size(budget, 2)?;
        self.observed(budget, "native-dictionary")?;
        if self
            .observation
            .dictionary_result
            .is_none_or(|r| r.is_err())
        {
            return Err(Failure::Native);
        }
        let compression_level = self
            .config
            .as_ref()
            .ok_or(Failure::State)?
            .compression_level;
        self.stripe = Some(LogStripe {
            stream_shard_id: self.shard,
            config: self.config.take().ok_or(Failure::State)?,
            partitions: HashMap::new(),
            indexed_frame_partitions: HashMap::new(),
            indexed_frame_query_cache: Mutex::new(IndexedFrameQueryCache::default()),
            exact_posting_cache: Mutex::new(ExactPostingCache::default()),
            active_partition_cache: HashMap::new(),
            message_term_cache: std::mem::take(&mut self.message_cache),
            field_cache: std::mem::take(&mut self.field_cache),
            active_blocks: HashMap::new(),
            catalog: BlockCatalog::default(),
            placement_dictionaries: HashMap::new(),
            dictionary_cache: self.dictionary_cache.take().ok_or(Failure::State)?,
            dictionary_catalog: None,
            dictionary_snapshot: None,
            dictionary_generation: 0,
            realtime_dictionary: None,
            block_collator: self.collator.take().ok_or(Failure::State)?,
            compressor: StripeCompressor {
                zstd_level: compression_level,
                active_dictionary: None,
                zstd: self.native.take().ok_or(Failure::State)?,
            },
            tier: None,
            next_frame_id: 0,
            native_query_work: None,
        });
        self.observation.assembled = true;
        self.observed(budget, "assembled")
    }
}

#[cfg(test)]
mod tests;
