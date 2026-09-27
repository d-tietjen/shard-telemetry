//! `usage_ledger/` owns ledger transactions, crash recovery, and their tests.
//! Quota-enforced embedded product-usage accounting.
//!
//! This module intentionally does not use the general telemetry WAL or metric
//! rollup catalog. A ledger has a fixed feature registry and stores only
//! monotonic totals plus an optional bounded calendar-month window. Its single
//! file contains two checksummed generations, so crash recovery never needs a
//! WAL or a temporary rewrite file.

mod ledger;
mod recovery;
use recovery::*;
#[cfg(test)]
mod tests;

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
