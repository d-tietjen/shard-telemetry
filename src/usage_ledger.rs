//! Quota-enforced embedded product-usage accounting.
//!
//! This module intentionally does not use the general telemetry WAL or metric
//! rollup catalog. A ledger has a fixed feature registry and stores only
//! monotonic totals plus an optional bounded calendar-month window. Its single
//! file contains two checksummed generations, so crash recovery never needs a
//! WAL or a temporary rewrite file.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Datelike, Utc};
use fs2::FileExt;
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::LokiApiError;

const LEDGER_MAGIC: &[u8; 8] = b"STUSAGE1";
const LEDGER_VERSION: u16 = 1;
const HEADER_BYTES: usize = 68;
const SLOT_COUNT: u64 = 2;
const CODEC_RAW: u8 = 0;
const CODEC_ZSTD: u8 = 1;
const ZSTD_LEVEL: i32 = 1;
const DEFAULT_MAX_FILE_BYTES: u64 = 1024 * 1024;
const DEFAULT_MONTHLY_BUCKETS: usize = 12;
const MAX_FEATURES: usize = 4_096;
const MAX_FEATURE_UPDATES_PER_BATCH: usize = 4_096;
const MAX_FEATURE_ID_BYTES: usize = 128;
const MAX_MONTHLY_BUCKETS: usize = 120;

/// Deterministic treatment of feature identifiers outside the fixed registry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum UnknownFeaturePolicy {
    /// Reject the complete update without changing durable totals.
    #[default]
    Reject,
    /// Add unknown feature increments to one fixed overflow counter.
    AccumulateOverflow,
}

/// Configuration for one quota-enforced embedded usage ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedUsageLedgerConfig {
    /// Exclusive file containing both crash-safe ledger generations.
    pub path: PathBuf,
    /// Fixed, unique product feature identifiers.
    pub feature_ids: Vec<Arc<str>>,
    /// Number of latest calendar-month buckets retained in addition to totals.
    pub monthly_buckets: usize,
    /// Policy applied when an update names a feature outside `feature_ids`.
    pub unknown_feature_policy: UnknownFeaturePolicy,
    /// Hard maximum for the complete ledger file, including both generations,
    /// checksums, codec metadata, and unused slot capacity.
    pub max_file_bytes: u64,
}

impl EmbeddedUsageLedgerConfig {
    /// Creates a ledger with a one-MiB hard file limit and twelve month buckets.
    pub fn new<I, S>(path: impl Into<PathBuf>, feature_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        Self {
            path: path.into(),
            feature_ids: feature_ids.into_iter().map(Into::into).collect(),
            monthly_buckets: DEFAULT_MONTHLY_BUCKETS,
            unknown_feature_policy: UnknownFeaturePolicy::Reject,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
        }
    }

    /// Sets the fixed number of latest calendar months retained; zero disables
    /// monthly accounting while preserving lifetime totals.
    #[must_use]
    pub const fn with_monthly_buckets(mut self, monthly_buckets: usize) -> Self {
        self.monthly_buckets = monthly_buckets;
        self
    }

    /// Sets the deterministic policy for identifiers outside the registry.
    #[must_use]
    pub const fn with_unknown_feature_policy(mut self, policy: UnknownFeaturePolicy) -> Self {
        self.unknown_feature_policy = policy;
        self
    }

    /// Sets the hard limit for the complete ledger file.
    #[must_use]
    pub const fn with_max_file_bytes(mut self, max_file_bytes: u64) -> Self {
        self.max_file_bytes = max_file_bytes;
        self
    }

    fn normalized(mut self) -> Result<Self, LokiApiError> {
        if self.feature_ids.is_empty() {
            return Err(LokiApiError::configuration(
                "embedded usage ledger requires at least one feature ID",
            ));
        }
        if self.feature_ids.len() > MAX_FEATURES {
            return Err(LokiApiError::configuration(format!(
                "embedded usage ledger supports at most {MAX_FEATURES} feature IDs",
            )));
        }
        if self.monthly_buckets > MAX_MONTHLY_BUCKETS {
            return Err(LokiApiError::configuration(format!(
                "embedded usage ledger supports at most {MAX_MONTHLY_BUCKETS} monthly buckets",
            )));
        }
        if self.max_file_bytes == 0 {
            return Err(LokiApiError::configuration(
                "embedded usage ledger max_file_bytes must be nonzero",
            ));
        }
        for feature_id in &self.feature_ids {
            if feature_id.is_empty() || feature_id.len() > MAX_FEATURE_ID_BYTES {
                return Err(LokiApiError::configuration(format!(
                    "embedded usage feature IDs must contain 1..={MAX_FEATURE_ID_BYTES} UTF-8 bytes",
                )));
            }
        }
        self.feature_ids.sort_unstable();
        if self.feature_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(LokiApiError::configuration(
                "embedded usage feature IDs must be unique",
            ));
        }
        Ok(self)
    }
}

/// One feature's exact monotonic lifetime total.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUsage {
    /// Allowlisted product feature identifier.
    pub feature_id: Arc<str>,
    /// Accepted events accumulated for this feature.
    pub count: u64,
}

/// Exact usage totals retained for one calendar month.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonthlyUsage {
    /// Gregorian calendar year.
    pub year: i32,
    /// Gregorian calendar month in the inclusive range 1..=12.
    pub month: u8,
    /// Fixed-registry feature counts in lexicographic feature-ID order.
    pub features: Vec<FeatureUsage>,
    /// Active seconds attributed to this month.
    pub active_seconds: u64,
    /// Unknown-feature events accumulated under the overflow policy.
    pub overflow_feature_events: u64,
}

/// Consistent in-memory view of one recovered usage generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedUsageSnapshot {
    /// Exact fixed-registry lifetime feature totals.
    pub features: Vec<FeatureUsage>,
    /// Exact lifetime active seconds.
    pub active_seconds: u64,
    /// Lifetime unknown-feature events accumulated under the overflow policy.
    pub overflow_feature_events: u64,
    /// Oldest-to-newest fixed monthly window.
    pub months: Vec<MonthlyUsage>,
    /// Durable generation represented by this snapshot.
    pub generation: u64,
    /// Wall-clock second of the last successful durable checkpoint.
    pub last_successful_checkpoint_unix_seconds: u64,
}

/// Bounded operational snapshot for quota and checkpoint monitoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedUsageHealth {
    /// Bytes in the complete fixed-capacity ledger file.
    pub file_bytes: u64,
    /// Physical filesystem bytes reserved for the ledger file.
    pub allocated_file_bytes: u64,
    /// Configured hard file quota.
    pub max_file_bytes: u64,
    /// Bytes of quota not assigned to the ledger file.
    pub quota_headroom_bytes: u64,
    /// Encoded bytes in the current generation before fixed slot padding.
    pub current_generation_bytes: u64,
    /// Number of allowlisted feature series.
    pub feature_count: usize,
    /// Calendar-month buckets currently populated.
    pub monthly_bucket_count: usize,
    /// Current durable generation.
    pub generation: u64,
    /// Wall-clock second of the last successful durable checkpoint.
    pub last_successful_checkpoint_unix_seconds: u64,
    /// Checkpoint attempts that failed after update validation since open.
    pub checkpoint_failures: u64,
    /// Complete updates rejected since open by validation, overflow, or quota checks.
    pub rejected_updates: u64,
    /// Unknown identifiers rejected by policy since open.
    pub rejected_unknown_features: u64,
    /// Unknown feature events accepted into the fixed overflow counter since open.
    pub overflowed_unknown_events: u64,
    /// Monthly updates too old for the configured rolling window since open.
    pub monthly_updates_outside_window: u64,
    /// Accepted updates waiting for persistence. Synchronous checkpoints keep
    /// this at zero; the field makes that contract observable.
    pub pending_updates: u64,
    /// Whether the current payload is compressed with Zstandard.
    pub compressed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedMonth {
    month_index: u32,
    #[serde(deserialize_with = "deserialize_feature_counts")]
    feature_counts: Vec<u64>,
    active_seconds: u64,
    overflow_feature_events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedState {
    #[serde(deserialize_with = "deserialize_feature_counts")]
    feature_counts: Vec<u64>,
    active_seconds: u64,
    overflow_feature_events: u64,
    #[serde(deserialize_with = "deserialize_months")]
    months: Vec<PersistedMonth>,
    last_successful_checkpoint_unix_seconds: u64,
}

fn deserialize_feature_counts<'de, D>(deserializer: D) -> Result<Vec<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(BoundedVecVisitor::<u64, MAX_FEATURES>::new(
        "at most 4096 feature counters",
    ))
}

fn deserialize_months<'de, D>(deserializer: D) -> Result<Vec<PersistedMonth>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(
        BoundedVecVisitor::<PersistedMonth, MAX_MONTHLY_BUCKETS>::new(
            "at most 120 monthly buckets",
        ),
    )
}

struct BoundedVecVisitor<T, const MAX: usize> {
    expected: &'static str,
    marker: std::marker::PhantomData<T>,
}

impl<T, const MAX: usize> BoundedVecVisitor<T, MAX> {
    const fn new(expected: &'static str) -> Self {
        Self {
            expected,
            marker: std::marker::PhantomData,
        }
    }
}

impl<'de, T, const MAX: usize> Visitor<'de> for BoundedVecVisitor<T, MAX>
where
    T: Deserialize<'de>,
{
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.expected)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if sequence.size_hint().is_some_and(|length| length > MAX) {
            return Err(de::Error::custom(self.expected));
        }
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX));
        while let Some(value) = sequence.next_element()? {
            if values.len() == MAX {
                return Err(de::Error::custom(self.expected));
            }
            values.push(value);
        }
        Ok(values)
    }
}

impl PersistedState {
    fn empty(feature_count: usize) -> Self {
        Self {
            feature_counts: vec![0; feature_count],
            active_seconds: 0,
            overflow_feature_events: 0,
            months: Vec::new(),
            last_successful_checkpoint_unix_seconds: 0,
        }
    }

    fn maximum_sized(feature_count: usize, monthly_buckets: usize) -> Self {
        Self {
            feature_counts: vec![u64::MAX; feature_count],
            active_seconds: u64::MAX,
            overflow_feature_events: u64::MAX,
            months: (0..monthly_buckets)
                .map(|_| PersistedMonth {
                    month_index: u32::MAX,
                    feature_counts: vec![u64::MAX; feature_count],
                    active_seconds: u64::MAX,
                    overflow_feature_events: u64::MAX,
                })
                .collect(),
            last_successful_checkpoint_unix_seconds: u64::MAX,
        }
    }
}

#[derive(Debug)]
struct LedgerInner {
    file: File,
    state: PersistedState,
    active_slot: usize,
    generation: u64,
    current_generation_bytes: u64,
    compressed: bool,
}

/// Crash-safe, quota-enforced embedded product-usage ledger.
///
/// Each successful recording call synchronously commits a complete generation.
/// On failure, the in-memory and durable snapshots remain unchanged. Opening
/// the same path with a different feature registry or month policy is rejected.
pub struct EmbeddedUsageLedger {
    path: PathBuf,
    feature_ids: Arc<Vec<Arc<str>>>,
    feature_indexes: BTreeMap<Arc<str>, usize>,
    monthly_buckets: usize,
    unknown_feature_policy: UnknownFeaturePolicy,
    max_file_bytes: u64,
    file_bytes: u64,
    allocated_file_bytes: u64,
    slot_bytes: u64,
    max_raw_payload_bytes: usize,
    config_fingerprint: [u8; 32],
    inner: Mutex<LedgerInner>,
    checkpoint_failures: AtomicU64,
    rejected_updates: AtomicU64,
    rejected_unknown_features: AtomicU64,
    overflowed_unknown_events: AtomicU64,
    monthly_updates_outside_window: AtomicU64,
}

impl std::fmt::Debug for EmbeddedUsageLedger {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddedUsageLedger")
            .field("path", &self.path)
            .field("feature_count", &self.feature_ids.len())
            .field("monthly_buckets", &self.monthly_buckets)
            .field("max_file_bytes", &self.max_file_bytes)
            .field("file_bytes", &self.file_bytes)
            .field("allocated_file_bytes", &self.allocated_file_bytes)
            .finish_non_exhaustive()
    }
}

impl EmbeddedUsageLedger {
    /// Opens or creates a ledger and exclusively locks its single data file.
    ///
    /// The maximum possible serialized state is calculated before the file is
    /// opened. Construction fails without creating the file if two crash-safe
    /// slots cannot fit within `max_file_bytes`.
    pub fn open(config: EmbeddedUsageLedgerConfig) -> Result<Self, LokiApiError> {
        let config = config.normalized()?;
        let maximum_state =
            PersistedState::maximum_sized(config.feature_ids.len(), config.monthly_buckets);
        let maximum_raw = rmp_serde::to_vec(&maximum_state).map_err(|error| {
            LokiApiError::internal(format!(
                "embedded usage ledger sizing serialization failed: {error}",
            ))
        })?;
        let max_raw_payload_bytes = maximum_raw.len();
        let slot_bytes = u64::try_from(HEADER_BYTES)
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(max_raw_payload_bytes).unwrap_or(u64::MAX));
        let file_bytes = slot_bytes.saturating_mul(SLOT_COUNT);
        if file_bytes > config.max_file_bytes {
            return Err(LokiApiError::configuration(format!(
                "embedded usage ledger requires {file_bytes} bytes for {} features and {} monthly buckets, exceeding the {}-byte hard quota",
                config.feature_ids.len(),
                config.monthly_buckets,
                config.max_file_bytes,
            )));
        }

        if let Some(parent) = config.path.parent() {
            std::fs::create_dir_all(parent).map_err(ledger_io)?;
        }
        let existing_metadata = match config.path.symlink_metadata() {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(ledger_io(error)),
        };
        if existing_metadata
            .as_ref()
            .is_some_and(|metadata| !metadata.is_file())
        {
            return Err(LokiApiError::configuration(format!(
                "embedded usage ledger path {} must be a regular file and not a symbolic link",
                config.path.display(),
            )));
        }
        let path_existed = existing_metadata.is_some();
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
        }
        if path_existed {
            options.create(false);
        } else {
            options.create_new(true);
        }
        let mut file = options.open(&config.path).map_err(ledger_io)?;
        if let Err(error) = file.try_lock_exclusive() {
            let error = LokiApiError::configuration(format!(
                "embedded usage ledger {} is already in use or cannot be locked: {error}",
                config.path.display(),
            ));
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                error,
            ));
        }
        let observed_bytes = match file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                return Err(clean_up_failed_open(
                    &config.path,
                    path_existed,
                    file,
                    ledger_io(error),
                ));
            }
        };
        let is_new = observed_bytes == 0;
        if !is_new && observed_bytes != file_bytes {
            let error = LokiApiError::configuration(format!(
                "embedded usage ledger {} has {observed_bytes} bytes but this configuration requires {file_bytes}",
                config.path.display(),
            ));
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                error,
            ));
        }
        if let Err(error) = file.allocate(file_bytes) {
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                ledger_io(error),
            ));
        }
        if is_new && let Err(error) = file.sync_all() {
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                ledger_io(error),
            ));
        }
        let initialize = if is_new {
            true
        } else {
            match slots_are_pristine(&mut file, slot_bytes) {
                Ok(pristine) => pristine,
                Err(error) => {
                    return Err(clean_up_failed_open(
                        &config.path,
                        path_existed,
                        file,
                        error,
                    ));
                }
            }
        };
        let allocated_file_bytes = match file.allocated_size() {
            Ok(bytes) => bytes,
            Err(error) => {
                return Err(clean_up_failed_open(
                    &config.path,
                    path_existed,
                    file,
                    ledger_io(error),
                ));
            }
        };
        if allocated_file_bytes < file_bytes {
            let error = LokiApiError::configuration(format!(
                "embedded usage ledger filesystem reserved only {allocated_file_bytes} of {file_bytes} required bytes",
            ));
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                error,
            ));
        }
        if allocated_file_bytes > config.max_file_bytes {
            let error = LokiApiError::configuration(format!(
                "embedded usage ledger requires {allocated_file_bytes} allocated filesystem bytes, exceeding the {}-byte hard quota",
                config.max_file_bytes,
            ));
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                error,
            ));
        }

        let cleanup_path = config.path.clone();
        let feature_ids = Arc::new(config.feature_ids);
        let feature_indexes = feature_ids
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, feature_id)| (feature_id, index))
            .collect();
        let config_fingerprint = configuration_fingerprint(
            feature_ids.as_slice(),
            config.monthly_buckets,
            config.unknown_feature_policy,
        );
        let ledger = Self {
            path: config.path,
            feature_ids,
            feature_indexes,
            monthly_buckets: config.monthly_buckets,
            unknown_feature_policy: config.unknown_feature_policy,
            max_file_bytes: config.max_file_bytes,
            file_bytes,
            allocated_file_bytes,
            slot_bytes,
            max_raw_payload_bytes,
            config_fingerprint,
            inner: Mutex::new(LedgerInner {
                file,
                state: PersistedState::empty(0),
                active_slot: 1,
                generation: 0,
                current_generation_bytes: 0,
                compressed: false,
            }),
            checkpoint_failures: AtomicU64::new(0),
            rejected_updates: AtomicU64::new(0),
            rejected_unknown_features: AtomicU64::new(0),
            overflowed_unknown_events: AtomicU64::new(0),
            monthly_updates_outside_window: AtomicU64::new(0),
        };

        let ready = if initialize {
            ledger
                .inner
                .lock()
                .map_err(|_| ledger_lock_error())
                .and_then(|mut inner| {
                    inner.state = PersistedState::empty(ledger.feature_ids.len());
                    let mut initial = inner.state.clone();
                    initial.last_successful_checkpoint_unix_seconds = unix_seconds_now();
                    ledger.persist_locked(&mut inner, initial)
                })
        } else {
            ledger.recover().and_then(|recovered| {
                let mut inner = ledger.inner.lock().map_err(|_| ledger_lock_error())?;
                inner.state = recovered.state;
                inner.active_slot = recovered.slot;
                inner.generation = recovered.generation;
                inner.current_generation_bytes = recovered.payload_bytes;
                inner.compressed = recovered.compressed;
                Ok(())
            })
        };
        if let Err(error) = ready {
            drop(ledger);
            if !path_existed {
                let _ = std::fs::remove_file(cleanup_path);
            }
            return Err(error);
        }
        if !path_existed && let Err(error) = sync_parent_directory(&cleanup_path) {
            drop(ledger);
            let _ = std::fs::remove_file(&cleanup_path);
            let _ = sync_parent_directory(&cleanup_path);
            return Err(error);
        }
        Ok(ledger)
    }

    /// Adds one monotonic feature count at the current wall-clock time.
    pub fn record_feature(&self, feature_id: &str, increment: u64) -> Result<(), LokiApiError> {
        self.record_feature_at(feature_id, increment, unix_seconds_now())
    }

    /// Adds one monotonic feature count at an explicit Unix second.
    pub fn record_feature_at(
        &self,
        feature_id: &str,
        increment: u64,
        timestamp_unix_seconds: u64,
    ) -> Result<(), LokiApiError> {
        self.record_batch_at(timestamp_unix_seconds, 0, [(feature_id, increment)])
    }

    /// Adds monotonic active time at the current wall-clock time.
    pub fn record_active_seconds(&self, seconds: u64) -> Result<(), LokiApiError> {
        self.record_active_seconds_at(seconds, unix_seconds_now())
    }

    /// Adds monotonic active time at an explicit Unix second.
    pub fn record_active_seconds_at(
        &self,
        seconds: u64,
        timestamp_unix_seconds: u64,
    ) -> Result<(), LokiApiError> {
        self.record_batch_at(
            timestamp_unix_seconds,
            seconds,
            std::iter::empty::<(&str, u64)>(),
        )
    }

    /// Atomically records active seconds and feature increments at the current
    /// wall-clock time.
    pub fn record_batch<I, S>(
        &self,
        active_seconds: u64,
        feature_increments: I,
    ) -> Result<(), LokiApiError>
    where
        I: IntoIterator<Item = (S, u64)>,
        S: AsRef<str>,
    {
        self.record_batch_at(unix_seconds_now(), active_seconds, feature_increments)
    }

    /// Atomically records active seconds and any number of feature increments.
    ///
    /// Duplicate feature IDs in the iterator are added in order. Any rejected
    /// identifier or arithmetic overflow rejects the complete batch.
    pub fn record_batch_at<I, S>(
        &self,
        timestamp_unix_seconds: u64,
        active_seconds: u64,
        feature_increments: I,
    ) -> Result<(), LokiApiError>
    where
        I: IntoIterator<Item = (S, u64)>,
        S: AsRef<str>,
    {
        let mut increments = vec![0_u64; self.feature_ids.len()];
        let mut overflowed = 0_u64;
        let mut update_count = 0_usize;
        for (feature_id, increment) in feature_increments {
            update_count = update_count.saturating_add(1);
            if update_count > MAX_FEATURE_UPDATES_PER_BATCH {
                self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                return Err(LokiApiError::bad_request(format!(
                    "embedded usage batches support at most {MAX_FEATURE_UPDATES_PER_BATCH} feature updates",
                )));
            }
            if increment == 0 {
                continue;
            }
            let feature_id = feature_id.as_ref();
            if feature_id.len() > MAX_FEATURE_ID_BYTES {
                match self.unknown_feature_policy {
                    UnknownFeaturePolicy::Reject => {
                        self.rejected_unknown_features
                            .fetch_add(1, Ordering::Relaxed);
                        self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                        return Err(LokiApiError::bad_request(format!(
                            "unknown embedded usage feature IDs must contain at most {MAX_FEATURE_ID_BYTES} UTF-8 bytes",
                        )));
                    }
                    UnknownFeaturePolicy::AccumulateOverflow => {
                        overflowed = checked_add(overflowed, increment).inspect_err(|_| {
                            self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                        })?;
                        continue;
                    }
                }
            }
            if let Some(index) = self.feature_indexes.get(feature_id).copied() {
                increments[index] =
                    checked_add(increments[index], increment).inspect_err(|_| {
                        self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                    })?;
                continue;
            }
            match self.unknown_feature_policy {
                UnknownFeaturePolicy::Reject => {
                    self.rejected_unknown_features
                        .fetch_add(1, Ordering::Relaxed);
                    self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                    return Err(LokiApiError::bad_request(
                        "feature ID is not in the embedded usage registry",
                    ));
                }
                UnknownFeaturePolicy::AccumulateOverflow => {
                    overflowed = checked_add(overflowed, increment).inspect_err(|_| {
                        self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                    })?;
                }
            }
        }
        if active_seconds == 0 && overflowed == 0 && increments.iter().all(|value| *value == 0) {
            return Ok(());
        }
        let month_index = if self.monthly_buckets == 0 {
            0
        } else {
            calendar_month_index(timestamp_unix_seconds).inspect_err(|_| {
                self.rejected_updates.fetch_add(1, Ordering::Relaxed);
            })?
        };
        let mut inner = self.inner.lock().map_err(|_| ledger_lock_error())?;
        let mut staged = inner.state.clone();
        let month_position = self.prepare_month(&mut staged, month_index);

        staged.active_seconds =
            checked_add(staged.active_seconds, active_seconds).inspect_err(|_| {
                self.rejected_updates.fetch_add(1, Ordering::Relaxed);
            })?;
        if let Some(position) = month_position {
            staged.months[position].active_seconds =
                checked_add(staged.months[position].active_seconds, active_seconds).inspect_err(
                    |_| {
                        self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                    },
                )?;
        }

        for (index, increment) in increments.into_iter().enumerate() {
            if increment == 0 {
                continue;
            }
            staged.feature_counts[index] = checked_add(staged.feature_counts[index], increment)
                .inspect_err(|_| {
                    self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                })?;
            if let Some(position) = month_position {
                staged.months[position].feature_counts[index] =
                    checked_add(staged.months[position].feature_counts[index], increment)
                        .inspect_err(|_| {
                            self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                        })?;
            }
        }
        staged.overflow_feature_events = checked_add(staged.overflow_feature_events, overflowed)
            .inspect_err(|_| {
                self.rejected_updates.fetch_add(1, Ordering::Relaxed);
            })?;
        if let Some(position) = month_position {
            staged.months[position].overflow_feature_events =
                checked_add(staged.months[position].overflow_feature_events, overflowed)
                    .inspect_err(|_| {
                        self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                    })?;
        }

        staged.last_successful_checkpoint_unix_seconds =
            unix_seconds_now().max(staged.last_successful_checkpoint_unix_seconds);
        if let Err(error) = self.persist_locked(&mut inner, staged) {
            self.checkpoint_failures.fetch_add(1, Ordering::Relaxed);
            self.rejected_updates.fetch_add(1, Ordering::Relaxed);
            return Err(error);
        }
        self.overflowed_unknown_events
            .fetch_add(overflowed, Ordering::Relaxed);
        Ok(())
    }

    /// Returns a consistent snapshot without accessing the data file.
    pub fn snapshot(&self) -> Result<EmbeddedUsageSnapshot, LokiApiError> {
        let inner = self.inner.lock().map_err(|_| ledger_lock_error())?;
        Ok(self.public_snapshot(&inner))
    }

    /// Returns quota, cardinality, durability, and rejection health counters.
    pub fn health(&self) -> Result<EmbeddedUsageHealth, LokiApiError> {
        let inner = self.inner.lock().map_err(|_| ledger_lock_error())?;
        Ok(EmbeddedUsageHealth {
            file_bytes: self.file_bytes,
            allocated_file_bytes: self.allocated_file_bytes,
            max_file_bytes: self.max_file_bytes,
            quota_headroom_bytes: self
                .max_file_bytes
                .saturating_sub(self.file_bytes.max(self.allocated_file_bytes)),
            current_generation_bytes: inner.current_generation_bytes,
            feature_count: self.feature_ids.len(),
            monthly_bucket_count: inner.state.months.len(),
            generation: inner.generation,
            last_successful_checkpoint_unix_seconds: inner
                .state
                .last_successful_checkpoint_unix_seconds,
            checkpoint_failures: self.checkpoint_failures.load(Ordering::Relaxed),
            rejected_updates: self.rejected_updates.load(Ordering::Relaxed),
            rejected_unknown_features: self.rejected_unknown_features.load(Ordering::Relaxed),
            overflowed_unknown_events: self.overflowed_unknown_events.load(Ordering::Relaxed),
            monthly_updates_outside_window: self
                .monthly_updates_outside_window
                .load(Ordering::Relaxed),
            pending_updates: 0,
            compressed: inner.compressed,
        })
    }

    fn prepare_month(&self, state: &mut PersistedState, month_index: u32) -> Option<usize> {
        if self.monthly_buckets == 0 {
            return None;
        }
        match state
            .months
            .binary_search_by_key(&month_index, |month| month.month_index)
        {
            Ok(position) => Some(position),
            Err(position) if state.months.len() < self.monthly_buckets => {
                state.months.insert(
                    position,
                    PersistedMonth {
                        month_index,
                        feature_counts: vec![0; self.feature_ids.len()],
                        active_seconds: 0,
                        overflow_feature_events: 0,
                    },
                );
                Some(position)
            }
            Err(0) => {
                self.monthly_updates_outside_window
                    .fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(position) => {
                state.months.remove(0);
                let adjusted = position - 1;
                state.months.insert(
                    adjusted,
                    PersistedMonth {
                        month_index,
                        feature_counts: vec![0; self.feature_ids.len()],
                        active_seconds: 0,
                        overflow_feature_events: 0,
                    },
                );
                Some(adjusted)
            }
        }
    }

    fn public_snapshot(&self, inner: &LedgerInner) -> EmbeddedUsageSnapshot {
        EmbeddedUsageSnapshot {
            features: self
                .feature_ids
                .iter()
                .cloned()
                .zip(inner.state.feature_counts.iter().copied())
                .map(|(feature_id, count)| FeatureUsage { feature_id, count })
                .collect(),
            active_seconds: inner.state.active_seconds,
            overflow_feature_events: inner.state.overflow_feature_events,
            months: inner
                .state
                .months
                .iter()
                .map(|month| {
                    let (year, calendar_month) = calendar_month(month.month_index);
                    MonthlyUsage {
                        year,
                        month: calendar_month,
                        features: self
                            .feature_ids
                            .iter()
                            .cloned()
                            .zip(month.feature_counts.iter().copied())
                            .map(|(feature_id, count)| FeatureUsage { feature_id, count })
                            .collect(),
                        active_seconds: month.active_seconds,
                        overflow_feature_events: month.overflow_feature_events,
                    }
                })
                .collect(),
            generation: inner.generation,
            last_successful_checkpoint_unix_seconds: inner
                .state
                .last_successful_checkpoint_unix_seconds,
        }
    }

    fn persist_locked(
        &self,
        inner: &mut LedgerInner,
        state: PersistedState,
    ) -> Result<(), LokiApiError> {
        validate_state(&state, self.feature_ids.len(), self.monthly_buckets)?;
        let raw = rmp_serde::to_vec(&state).map_err(|error| {
            LokiApiError::internal(format!(
                "embedded usage ledger serialization failed: {error}",
            ))
        })?;
        if raw.len() > self.max_raw_payload_bytes {
            return Err(LokiApiError::configuration(
                "embedded usage ledger state exceeds its precomputed hard quota",
            ));
        }
        let compressed = zstd::bulk::compress(&raw, ZSTD_LEVEL).map_err(|error| {
            LokiApiError::internal(format!("embedded usage ledger compression failed: {error}",))
        })?;
        let (codec, payload) = if compressed.len() < raw.len() {
            (CODEC_ZSTD, compressed.as_slice())
        } else {
            (CODEC_RAW, raw.as_slice())
        };
        if payload.len() > self.max_raw_payload_bytes {
            return Err(LokiApiError::configuration(
                "embedded usage ledger encoded state exceeds its fixed slot",
            ));
        }
        let generation = inner
            .generation
            .checked_add(1)
            .ok_or_else(|| LokiApiError::internal("embedded usage ledger generation exhausted"))?;
        let target_slot = (inner.active_slot + 1) % usize::try_from(SLOT_COUNT).unwrap_or(2);
        let slot_offset = self
            .slot_bytes
            .saturating_mul(u64::try_from(target_slot).unwrap_or(u64::MAX));
        inner
            .file
            .seek(SeekFrom::Start(slot_offset.saturating_add(
                u64::try_from(HEADER_BYTES).unwrap_or(u64::MAX),
            )))
            .and_then(|_| inner.file.write_all(payload))
            .and_then(|_| inner.file.sync_data())
            .map_err(ledger_io)?;
        let header = encode_header(
            codec,
            generation,
            payload.len(),
            raw.len(),
            &self.config_fingerprint,
            crc32c::crc32c(payload),
        )?;
        inner
            .file
            .seek(SeekFrom::Start(slot_offset))
            .and_then(|_| inner.file.write_all(&header))
            .and_then(|_| inner.file.sync_all())
            .map_err(ledger_io)?;
        inner.state = state;
        inner.active_slot = target_slot;
        inner.generation = generation;
        inner.current_generation_bytes =
            u64::try_from(HEADER_BYTES + payload.len()).unwrap_or(u64::MAX);
        inner.compressed = codec == CODEC_ZSTD;
        Ok(())
    }

    fn recover(&self) -> Result<RecoveredSlot, LokiApiError> {
        let mut inner = self.inner.lock().map_err(|_| ledger_lock_error())?;
        let mut recovered = Vec::new();
        let mut observed_other_configuration = false;
        for slot in 0..usize::try_from(SLOT_COUNT).unwrap_or(2) {
            match read_slot(
                &mut inner.file,
                slot,
                self.slot_bytes,
                self.max_raw_payload_bytes,
                &self.config_fingerprint,
                self.feature_ids.len(),
                self.monthly_buckets,
            )? {
                SlotRead::Valid(slot) => recovered.push(slot),
                SlotRead::OtherConfiguration => observed_other_configuration = true,
                SlotRead::Invalid => {}
            }
        }
        recovered
            .into_iter()
            .max_by_key(|slot| slot.generation)
            .ok_or_else(|| {
                if observed_other_configuration {
                    LokiApiError::configuration(
                        "embedded usage ledger feature registry or monthly policy changed",
                    )
                } else {
                    LokiApiError::internal(
                        "embedded usage ledger contains no valid checksummed generation",
                    )
                }
            })
    }
}

#[derive(Debug)]
struct RecoveredSlot {
    state: PersistedState,
    slot: usize,
    generation: u64,
    payload_bytes: u64,
    compressed: bool,
}

enum SlotRead {
    Valid(RecoveredSlot),
    OtherConfiguration,
    Invalid,
}

fn read_slot(
    file: &mut File,
    slot: usize,
    slot_bytes: u64,
    max_raw_payload_bytes: usize,
    expected_fingerprint: &[u8; 32],
    feature_count: usize,
    monthly_buckets: usize,
) -> Result<SlotRead, LokiApiError> {
    let slot_offset = slot_bytes.saturating_mul(u64::try_from(slot).unwrap_or(u64::MAX));
    let mut header = [0_u8; HEADER_BYTES];
    file.seek(SeekFrom::Start(slot_offset))
        .and_then(|_| file.read_exact(&mut header))
        .map_err(ledger_io)?;
    if &header[..8] != LEDGER_MAGIC {
        return Ok(SlotRead::Invalid);
    }
    let expected_header_crc = read_u32(&header[64..68]);
    if crc32c::crc32c(&header[..64]) != expected_header_crc {
        return Ok(SlotRead::Invalid);
    }
    if read_u16(&header[8..10]) != LEDGER_VERSION {
        return Ok(SlotRead::Invalid);
    }
    if &header[28..60] != expected_fingerprint {
        return Ok(SlotRead::OtherConfiguration);
    }
    let codec = header[10];
    if !matches!(codec, CODEC_RAW | CODEC_ZSTD) {
        return Ok(SlotRead::Invalid);
    }
    let generation = read_u64(&header[12..20]);
    let payload_len = usize::try_from(read_u32(&header[20..24])).unwrap_or(usize::MAX);
    let raw_len = usize::try_from(read_u32(&header[24..28])).unwrap_or(usize::MAX);
    if payload_len > max_raw_payload_bytes || raw_len > max_raw_payload_bytes {
        return Ok(SlotRead::Invalid);
    }
    let mut payload = vec![0_u8; payload_len];
    file.seek(SeekFrom::Start(
        slot_offset.saturating_add(u64::try_from(HEADER_BYTES).unwrap_or(u64::MAX)),
    ))
    .and_then(|_| file.read_exact(&mut payload))
    .map_err(ledger_io)?;
    if crc32c::crc32c(&payload) != read_u32(&header[60..64]) {
        return Ok(SlotRead::Invalid);
    }
    let raw = if codec == CODEC_ZSTD {
        match zstd::bulk::decompress(&payload, raw_len) {
            Ok(raw) if raw.len() == raw_len => raw,
            Ok(_) | Err(_) => return Ok(SlotRead::Invalid),
        }
    } else {
        if payload_len != raw_len {
            return Ok(SlotRead::Invalid);
        }
        payload
    };
    let state: PersistedState = match rmp_serde::from_slice(&raw) {
        Ok(state) => state,
        Err(_) => return Ok(SlotRead::Invalid),
    };
    if validate_state(&state, feature_count, monthly_buckets).is_err() {
        return Ok(SlotRead::Invalid);
    }
    Ok(SlotRead::Valid(RecoveredSlot {
        state,
        slot,
        generation,
        payload_bytes: u64::try_from(HEADER_BYTES + payload_len).unwrap_or(u64::MAX),
        compressed: codec == CODEC_ZSTD,
    }))
}

fn slots_are_pristine(file: &mut File, slot_bytes: u64) -> Result<bool, LokiApiError> {
    for slot in 0..SLOT_COUNT {
        let mut magic = [0_u8; LEDGER_MAGIC.len()];
        file.seek(SeekFrom::Start(slot_bytes.saturating_mul(slot)))
            .and_then(|_| file.read_exact(&mut magic))
            .map_err(ledger_io)?;
        if magic.iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_state(
    state: &PersistedState,
    feature_count: usize,
    monthly_buckets: usize,
) -> Result<(), LokiApiError> {
    if state.feature_counts.len() != feature_count
        || state.months.len() > monthly_buckets
        || state
            .months
            .iter()
            .any(|month| month.feature_counts.len() != feature_count)
        || state
            .months
            .windows(2)
            .any(|months| months[0].month_index >= months[1].month_index)
    {
        return Err(LokiApiError::internal(
            "embedded usage ledger state violates fixed-schema bounds",
        ));
    }
    Ok(())
}

fn encode_header(
    codec: u8,
    generation: u64,
    payload_len: usize,
    raw_len: usize,
    config_fingerprint: &[u8; 32],
    payload_crc: u32,
) -> Result<[u8; HEADER_BYTES], LokiApiError> {
    let payload_len = u32::try_from(payload_len)
        .map_err(|_| LokiApiError::configuration("embedded usage payload exceeds v1 format"))?;
    let raw_len = u32::try_from(raw_len)
        .map_err(|_| LokiApiError::configuration("embedded usage payload exceeds v1 format"))?;
    let mut header = [0_u8; HEADER_BYTES];
    header[..8].copy_from_slice(LEDGER_MAGIC);
    header[8..10].copy_from_slice(&LEDGER_VERSION.to_le_bytes());
    header[10] = codec;
    header[12..20].copy_from_slice(&generation.to_le_bytes());
    header[20..24].copy_from_slice(&payload_len.to_le_bytes());
    header[24..28].copy_from_slice(&raw_len.to_le_bytes());
    header[28..60].copy_from_slice(config_fingerprint);
    header[60..64].copy_from_slice(&payload_crc.to_le_bytes());
    let header_crc = crc32c::crc32c(&header[..64]);
    header[64..68].copy_from_slice(&header_crc.to_le_bytes());
    Ok(header)
}

fn configuration_fingerprint(
    feature_ids: &[Arc<str>],
    monthly_buckets: usize,
    unknown_feature_policy: UnknownFeaturePolicy,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"shard-telemetry-embedded-usage-v1\0");
    hasher.update(
        &u64::try_from(monthly_buckets)
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    hasher.update(&[match unknown_feature_policy {
        UnknownFeaturePolicy::Reject => 0,
        UnknownFeaturePolicy::AccumulateOverflow => 1,
    }]);
    for feature_id in feature_ids {
        hasher.update(
            &u64::try_from(feature_id.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(feature_id.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn calendar_month_index(timestamp_unix_seconds: u64) -> Result<u32, LokiApiError> {
    let timestamp = i64::try_from(timestamp_unix_seconds).map_err(|_| {
        LokiApiError::bad_request("embedded usage timestamp is outside the supported calendar")
    })?;
    let date = DateTime::<Utc>::from_timestamp(timestamp, 0).ok_or_else(|| {
        LokiApiError::bad_request("embedded usage timestamp is outside the supported calendar")
    })?;
    let year = u32::try_from(date.year()).map_err(|_| {
        LokiApiError::bad_request("embedded usage timestamps before year zero are unsupported")
    })?;
    Ok(year
        .saturating_mul(12)
        .saturating_add(date.month().saturating_sub(1)))
}

fn calendar_month(month_index: u32) -> (i32, u8) {
    let year = i32::try_from(month_index / 12).unwrap_or(i32::MAX);
    let month = u8::try_from(month_index % 12 + 1).unwrap_or(12);
    (year, month)
}

fn checked_add(left: u64, right: u64) -> Result<u64, LokiApiError> {
    left.checked_add(right)
        .ok_or_else(|| LokiApiError::bad_request("embedded usage counter overflow"))
}

fn read_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("fixed u16 field"))
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("fixed u32 field"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("fixed u64 field"))
}

fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn ledger_io(error: std::io::Error) -> LokiApiError {
    LokiApiError::internal(format!("embedded usage ledger I/O failed: {error}"))
}

fn ledger_lock_error() -> LokiApiError {
    LokiApiError::internal("embedded usage ledger lock poisoned")
}

fn clean_up_failed_open(
    path: &Path,
    path_existed: bool,
    file: File,
    error: LokiApiError,
) -> LokiApiError {
    drop(file);
    if !path_existed {
        let _ = std::fs::remove_file(path);
    }
    error
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), LokiApiError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(ledger_io)
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<(), LokiApiError> {
    Ok(())
}

#[cfg(test)]
mod tests {
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

        let recovered =
            EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search"]))
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

        let recovered =
            EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&path, ["search"]))
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
            EmbeddedUsageLedger::open(
                EmbeddedUsageLedgerConfig::new(&path, ["search", "changed"],)
            )
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

        assert!(
            EmbeddedUsageLedger::open(EmbeddedUsageLedgerConfig::new(&link, ["search"])).is_err()
        );
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
}
