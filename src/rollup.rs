//! Durable local lifetime rollups for metric outcomes.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use shard_stream_core::{LogicalOffset, TopicPartition};

use crate::{
    DurableMetricPoint, ExplicitHistogramValue, ExponentialHistogramBuckets,
    ExponentialHistogramValue, HistogramCount, MetricIdentity, MetricKind, MetricValue,
    NumberValue, SeriesFingerprint, SummaryValue, TelemetryError, TelemetryResult,
};

const ROLLUP_VERSION: u8 = 1;

/// Queryable outcome retained locally after raw metric points are removed or
/// archived.
///
/// Monotonic cumulative sums and cumulative histograms are converted into a
/// reset-aware lifetime total; delta values are added directly. Gauges and
/// non-monotonic cumulative sums retain their latest value, and gauges also
/// retain numeric lifetime minimum and maximum. `observed_points` counts
/// accepted raw snapshots, not underlying application events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifetimeMetricRollup {
    /// Canonical series fingerprint.
    pub series: SeriesFingerprint,
    /// Complete series identity required for local selection and labels.
    pub identity: Arc<MetricIdentity>,
    /// Latest metric description.
    pub description: Arc<str>,
    /// Earliest raw snapshot incorporated into this rollup.
    pub first_timestamp_unix_nanos: u64,
    /// Latest raw snapshot incorporated into this rollup.
    pub last_timestamp_unix_nanos: u64,
    /// Number of raw metric points incorporated.
    pub observed_points: u64,
    /// Cumulative-instrument resets observed while deriving lifetime totals.
    pub reset_count: u64,
    /// Lifetime outcome for sums and histograms, or the latest gauge value.
    pub outcome: MetricValue,
    /// Minimum numeric gauge value observed, when applicable.
    pub numeric_min: Option<NumberValue>,
    /// Maximum numeric gauge value observed, when applicable.
    pub numeric_max: Option<NumberValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RollupState {
    rollup: LifetimeMetricRollup,
    last_raw_value: MetricValue,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedRollups {
    version: u8,
    checkpoints: Vec<(TopicPartition, LogicalOffset)>,
    rollups: Vec<RollupState>,
}

/// Crash-safe local rollup catalog advanced before source payload reclamation.
#[derive(Debug, Clone)]
pub(crate) struct MetricRollupCatalog {
    path: PathBuf,
    max_series: usize,
    max_bytes: u64,
    checkpoints: BTreeMap<TopicPartition, LogicalOffset>,
    rollups: BTreeMap<SeriesFingerprint, RollupState>,
    incorporated_points: u64,
}

impl MetricRollupCatalog {
    pub(crate) fn open(path: PathBuf, max_series: usize, max_bytes: u64) -> TelemetryResult<Self> {
        if max_series == 0 || max_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "lifetime metric rollup series and byte limits must be nonzero",
            ));
        }
        let persisted = match fs::read(&path) {
            Ok(bytes) => {
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_bytes {
                    return Err(TelemetryError::InvalidConfiguration(format!(
                        "recovered lifetime metric rollup catalog exceeds configured byte limit {max_bytes}"
                    )));
                }
                Some(
                    rmp_serde::from_slice::<PersistedRollups>(&bytes).map_err(|error| {
                        TelemetryError::StorageIo(format!(
                            "lifetime metric rollup catalog is invalid: {error}"
                        ))
                    })?,
                )
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(storage_io("read lifetime metric rollups", error)),
        };
        let mut catalog = Self {
            path,
            max_series,
            max_bytes,
            checkpoints: BTreeMap::new(),
            rollups: BTreeMap::new(),
            incorporated_points: 0,
        };
        if let Some(persisted) = persisted {
            if persisted.version != ROLLUP_VERSION {
                return Err(TelemetryError::StorageIo(format!(
                    "unsupported lifetime metric rollup version {}",
                    persisted.version
                )));
            }
            for (partition, checkpoint) in persisted.checkpoints {
                if catalog.checkpoints.insert(partition, checkpoint).is_some() {
                    return Err(TelemetryError::StorageIo(
                        "duplicate lifetime metric rollup checkpoint".into(),
                    ));
                }
            }
            for state in persisted.rollups {
                let series = state.rollup.series;
                if state.rollup.identity.fingerprint() != series
                    || catalog.rollups.insert(series, state).is_some()
                {
                    return Err(TelemetryError::StorageIo(
                        "invalid or duplicate lifetime metric rollup series".into(),
                    ));
                }
            }
            if catalog.rollups.len() > max_series {
                return Err(TelemetryError::InvalidConfiguration(format!(
                    "recovered lifetime rollups contain {} series, exceeding configured limit {max_series}",
                    catalog.rollups.len()
                )));
            }
        }
        Ok(catalog)
    }

    pub(crate) fn checkpoint(&self, partition: TopicPartition) -> Option<LogicalOffset> {
        self.checkpoints.get(&partition).copied()
    }

    pub(crate) fn apply_batch(
        &mut self,
        partition: TopicPartition,
        first_offset: LogicalOffset,
        points: Vec<DurableMetricPoint>,
    ) -> TelemetryResult<()> {
        let checkpoint = self.checkpoint(partition);
        let expected = checkpoint.unwrap_or(first_offset);
        if checkpoint.is_some_and(|expected| first_offset > expected) {
            return Err(TelemetryError::InvalidMetricSample(
                "lifetime metric rollup WAL checkpoint has a gap".into(),
            ));
        }
        let mut next_offset = first_offset;
        for (index, mut point) in points.into_iter().enumerate() {
            let offset = first_offset
                .get()
                .checked_add(u64::try_from(index).map_err(|_| TelemetryError::RecordTooLarge)?)
                .map(LogicalOffset::new)
                .ok_or(TelemetryError::OffsetExhausted(partition))?;
            next_offset = LogicalOffset::new(
                offset
                    .get()
                    .checked_add(1)
                    .ok_or(TelemetryError::OffsetExhausted(partition))?,
            );
            if offset < expected {
                continue;
            }
            point.record_ref.topic_partition = partition;
            point.record_ref.offset = offset;
            self.apply_point(point)?;
            self.incorporated_points = self.incorporated_points.saturating_add(1);
        }
        let current = self.checkpoint(partition).unwrap_or(expected);
        self.checkpoints.insert(partition, current.max(next_offset));
        Ok(())
    }

    fn apply_point(&mut self, point: DurableMetricPoint) -> TelemetryResult<()> {
        let series = point.series_fingerprint();
        if let Some(state) = self.rollups.get_mut(&series) {
            update_state(state, &point)?;
            return Ok(());
        }
        if self.rollups.len() >= self.max_series {
            return Err(TelemetryError::InvalidConfiguration(format!(
                "lifetime metric rollup series limit {} exhausted",
                self.max_series
            )));
        }
        let value = point.value.clone();
        let (numeric_min, numeric_max) = match value {
            MetricValue::Gauge(value) => (Some(value), Some(value)),
            _ => (None, None),
        };
        self.rollups.insert(
            series,
            RollupState {
                rollup: LifetimeMetricRollup {
                    series,
                    identity: Arc::clone(&point.identity),
                    description: Arc::clone(&point.description),
                    first_timestamp_unix_nanos: point.timestamp_unix_nanos,
                    last_timestamp_unix_nanos: point.timestamp_unix_nanos,
                    observed_points: 1,
                    reset_count: 0,
                    outcome: value.clone(),
                    numeric_min,
                    numeric_max,
                },
                last_raw_value: value,
            },
        );
        Ok(())
    }

    pub(crate) fn persist(&mut self) -> TelemetryResult<u64> {
        if self.incorporated_points == 0 {
            return Ok(0);
        }
        let persisted = PersistedRollups {
            version: ROLLUP_VERSION,
            checkpoints: self
                .checkpoints
                .iter()
                .map(|(partition, checkpoint)| (*partition, *checkpoint))
                .collect(),
            rollups: self.rollups.values().cloned().collect(),
        };
        let bytes = rmp_serde::to_vec(&persisted).map_err(|error| {
            TelemetryError::StorageIo(format!("serialize lifetime metric rollup catalog: {error}"))
        })?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > self.max_bytes {
            return Err(TelemetryError::InvalidConfiguration(format!(
                "lifetime metric rollup catalog exceeds configured byte limit {}",
                self.max_bytes
            )));
        }
        write_atomically(&self.path, &bytes)?;
        let points = self.incorporated_points;
        self.incorporated_points = 0;
        Ok(points)
    }

    pub(crate) fn clear_pending_report(&mut self) {
        self.incorporated_points = 0;
    }

    pub(crate) fn query(&self, tenant: &str, name: Option<&str>) -> Vec<LifetimeMetricRollup> {
        self.rollups
            .values()
            .filter(|state| {
                state.rollup.identity.tenant.as_ref() == tenant
                    && name.is_none_or(|name| state.rollup.identity.name.as_ref() == name)
            })
            .map(|state| state.rollup.clone())
            .collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.rollups.len()
    }
}

fn update_state(state: &mut RollupState, point: &DurableMetricPoint) -> TelemetryResult<()> {
    if state.rollup.identity.as_ref() != point.identity.as_ref() {
        return Err(TelemetryError::InvalidMetricSample(
            "lifetime rollup series identity changed".into(),
        ));
    }
    let (outcome, reset) = accumulate(
        &point.identity.kind,
        &state.rollup.outcome,
        &state.last_raw_value,
        &point.value,
    )?;
    state.rollup.outcome = outcome;
    state.last_raw_value = point.value.clone();
    state.rollup.description = Arc::clone(&point.description);
    state.rollup.first_timestamp_unix_nanos = state
        .rollup
        .first_timestamp_unix_nanos
        .min(point.timestamp_unix_nanos);
    state.rollup.last_timestamp_unix_nanos = state
        .rollup
        .last_timestamp_unix_nanos
        .max(point.timestamp_unix_nanos);
    state.rollup.observed_points = state.rollup.observed_points.saturating_add(1);
    state.rollup.reset_count = state.rollup.reset_count.saturating_add(u64::from(reset));
    if let MetricValue::Gauge(value) = point.value {
        state.rollup.numeric_min = Some(match state.rollup.numeric_min {
            Some(current) => number_min(current, value),
            None => value,
        });
        state.rollup.numeric_max = Some(match state.rollup.numeric_max {
            Some(current) => number_max(current, value),
            None => value,
        });
    }
    Ok(())
}

fn accumulate(
    kind: &MetricKind,
    lifetime: &MetricValue,
    previous: &MetricValue,
    current: &MetricValue,
) -> TelemetryResult<(MetricValue, bool)> {
    match (kind, lifetime, previous, current) {
        (MetricKind::Gauge, _, _, MetricValue::Gauge(value)) => {
            Ok((MetricValue::Gauge(*value), false))
        }
        (
            MetricKind::Sum {
                temporality,
                monotonic,
            },
            MetricValue::Sum(total),
            MetricValue::Sum(previous),
            MetricValue::Sum(current),
        ) => {
            if *temporality != 1 && !monotonic {
                return Ok((MetricValue::Sum(*current), false));
            }
            let (delta, reset) = if *temporality == 1 {
                (*current, false)
            } else {
                number_delta(*previous, *current)
            };
            Ok((MetricValue::Sum(number_add(*total, delta)?), reset))
        }
        (
            MetricKind::ExplicitHistogram { temporality },
            MetricValue::ExplicitHistogram(total),
            MetricValue::ExplicitHistogram(previous),
            MetricValue::ExplicitHistogram(current),
        ) => accumulate_explicit_histogram(*temporality, total, previous, current),
        (
            MetricKind::ExponentialHistogram { temporality },
            MetricValue::ExponentialHistogram(total),
            MetricValue::ExponentialHistogram(previous),
            MetricValue::ExponentialHistogram(current),
        ) => accumulate_exponential_histogram(*temporality, total, previous, current),
        (
            MetricKind::Summary,
            MetricValue::Summary(total),
            MetricValue::Summary(previous),
            MetricValue::Summary(current),
        ) => accumulate_summary(total, previous, current),
        _ => Err(TelemetryError::InvalidMetricSample(
            "metric lifetime rollup kind or value changed".into(),
        )),
    }
}

fn number_delta(previous: NumberValue, current: NumberValue) -> (NumberValue, bool) {
    match (previous, current) {
        (NumberValue::Integer(previous), NumberValue::Integer(current)) if current >= previous => {
            (NumberValue::Integer(current - previous), false)
        }
        (NumberValue::DoubleBits(previous), NumberValue::DoubleBits(current))
            if f64::from_bits(current) >= f64::from_bits(previous) =>
        {
            (
                NumberValue::from_f64(f64::from_bits(current) - f64::from_bits(previous)),
                false,
            )
        }
        (_, current) => (current, true),
    }
}

fn number_add(left: NumberValue, right: NumberValue) -> TelemetryResult<NumberValue> {
    match (left, right) {
        (NumberValue::Integer(left), NumberValue::Integer(right)) => left
            .checked_add(right)
            .map(NumberValue::Integer)
            .ok_or_else(|| TelemetryError::InvalidMetricSample("rollup integer overflow".into())),
        (NumberValue::DoubleBits(left), NumberValue::DoubleBits(right)) => Ok(
            NumberValue::from_f64(f64::from_bits(left) + f64::from_bits(right)),
        ),
        _ => Err(TelemetryError::InvalidMetricSample(
            "rollup numeric representation changed".into(),
        )),
    }
}

fn number_min(left: NumberValue, right: NumberValue) -> NumberValue {
    match (left, right) {
        (NumberValue::Integer(left), NumberValue::Integer(right)) => {
            NumberValue::Integer(left.min(right))
        }
        (NumberValue::DoubleBits(left), NumberValue::DoubleBits(right)) => {
            NumberValue::from_f64(f64::from_bits(left).min(f64::from_bits(right)))
        }
        (_, right) => right,
    }
}

fn number_max(left: NumberValue, right: NumberValue) -> NumberValue {
    match (left, right) {
        (NumberValue::Integer(left), NumberValue::Integer(right)) => {
            NumberValue::Integer(left.max(right))
        }
        (NumberValue::DoubleBits(left), NumberValue::DoubleBits(right)) => {
            NumberValue::from_f64(f64::from_bits(left).max(f64::from_bits(right)))
        }
        (_, right) => right,
    }
}

fn count_add(left: HistogramCount, right: HistogramCount) -> TelemetryResult<HistogramCount> {
    match (left, right) {
        (HistogramCount::Integer(left), HistogramCount::Integer(right)) => left
            .checked_add(right)
            .map(HistogramCount::Integer)
            .ok_or_else(|| TelemetryError::InvalidMetricSample("rollup count overflow".into())),
        (HistogramCount::DoubleBits(left), HistogramCount::DoubleBits(right)) => Ok(
            HistogramCount::DoubleBits((f64::from_bits(left) + f64::from_bits(right)).to_bits()),
        ),
        _ => Err(TelemetryError::InvalidMetricSample(
            "rollup histogram count representation changed".into(),
        )),
    }
}

fn count_delta(previous: HistogramCount, current: HistogramCount) -> Option<HistogramCount> {
    match (previous, current) {
        (HistogramCount::Integer(previous), HistogramCount::Integer(current)) => {
            current.checked_sub(previous).map(HistogramCount::Integer)
        }
        (HistogramCount::DoubleBits(previous), HistogramCount::DoubleBits(current)) => {
            let previous = f64::from_bits(previous);
            let current = f64::from_bits(current);
            (current >= previous)
                .then(|| HistogramCount::DoubleBits((current - previous).to_bits()))
        }
        _ => None,
    }
}

fn sum_bits_add(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    Some((f64::from_bits(left?) + f64::from_bits(right?)).to_bits())
}

fn sum_bits_delta(previous: Option<u64>, current: Option<u64>, reset: bool) -> Option<u64> {
    let current = current?;
    if reset {
        return Some(current);
    }
    Some((f64::from_bits(current) - f64::from_bits(previous?)).to_bits())
}

fn accumulate_explicit_histogram(
    temporality: i32,
    total: &ExplicitHistogramValue,
    previous: &ExplicitHistogramValue,
    current: &ExplicitHistogramValue,
) -> TelemetryResult<(MetricValue, bool)> {
    if total.explicit_bounds_bits != current.explicit_bounds_bits
        || previous.explicit_bounds_bits != current.explicit_bounds_bits
        || total.bucket_counts.len() != current.bucket_counts.len()
        || previous.bucket_counts.len() != current.bucket_counts.len()
    {
        return Err(TelemetryError::InvalidMetricSample(
            "explicit histogram rollup boundaries changed".into(),
        ));
    }
    let reset = temporality != 1 && count_delta(previous.count, current.count).is_none();
    let delta_count = if temporality == 1 || reset {
        current.count
    } else {
        count_delta(previous.count, current.count).expect("reset was checked")
    };
    let mut delta_buckets = Vec::with_capacity(current.bucket_counts.len());
    for (index, current) in current.bucket_counts.iter().copied().enumerate() {
        let delta = if temporality == 1 || reset {
            current
        } else {
            count_delta(previous.bucket_counts[index], current).ok_or_else(|| {
                TelemetryError::InvalidMetricSample(
                    "explicit histogram bucket regressed without a count reset".into(),
                )
            })?
        };
        delta_buckets.push(count_add(total.bucket_counts[index], delta)?);
    }
    Ok((
        MetricValue::ExplicitHistogram(ExplicitHistogramValue {
            count: count_add(total.count, delta_count)?,
            sum_bits: sum_bits_add(
                total.sum_bits,
                if temporality == 1 {
                    current.sum_bits
                } else {
                    sum_bits_delta(previous.sum_bits, current.sum_bits, reset)
                },
            ),
            bucket_counts: Arc::new(delta_buckets),
            explicit_bounds_bits: Arc::clone(&current.explicit_bounds_bits),
            min_bits: numeric_bits_min(total.min_bits, current.min_bits),
            max_bits: numeric_bits_max(total.max_bits, current.max_bits),
            reset_hint: current.reset_hint,
        }),
        reset,
    ))
}

fn accumulate_exponential_histogram(
    temporality: i32,
    total: &ExponentialHistogramValue,
    previous: &ExponentialHistogramValue,
    current: &ExponentialHistogramValue,
) -> TelemetryResult<(MetricValue, bool)> {
    if total.scale != current.scale || previous.scale != current.scale {
        return Err(TelemetryError::InvalidMetricSample(
            "exponential histogram rollup scale changed".into(),
        ));
    }
    let reset = temporality != 1 && count_delta(previous.count, current.count).is_none();
    let delta_count = if temporality == 1 || reset {
        current.count
    } else {
        count_delta(previous.count, current.count).expect("reset was checked")
    };
    let delta_zero = if temporality == 1 || reset {
        current.zero_count
    } else {
        count_delta(previous.zero_count, current.zero_count).ok_or_else(|| {
            TelemetryError::InvalidMetricSample(
                "exponential histogram zero count regressed without a reset".into(),
            )
        })?
    };
    Ok((
        MetricValue::ExponentialHistogram(ExponentialHistogramValue {
            count: count_add(total.count, delta_count)?,
            sum_bits: sum_bits_add(
                total.sum_bits,
                if temporality == 1 {
                    current.sum_bits
                } else {
                    sum_bits_delta(previous.sum_bits, current.sum_bits, reset)
                },
            ),
            scale: current.scale,
            zero_count: count_add(total.zero_count, delta_zero)?,
            positive: add_buckets(
                total.positive.as_ref(),
                previous.positive.as_ref(),
                current.positive.as_ref(),
                temporality == 1 || reset,
            )?,
            negative: add_buckets(
                total.negative.as_ref(),
                previous.negative.as_ref(),
                current.negative.as_ref(),
                temporality == 1 || reset,
            )?,
            min_bits: numeric_bits_min(total.min_bits, current.min_bits),
            max_bits: numeric_bits_max(total.max_bits, current.max_bits),
            zero_threshold_bits: current.zero_threshold_bits,
            reset_hint: current.reset_hint,
        }),
        reset,
    ))
}

fn add_buckets(
    total: Option<&ExponentialHistogramBuckets>,
    previous: Option<&ExponentialHistogramBuckets>,
    current: Option<&ExponentialHistogramBuckets>,
    reset: bool,
) -> TelemetryResult<Option<ExponentialHistogramBuckets>> {
    let total = expand_buckets(total)?;
    let previous = expand_buckets(previous)?;
    let current = expand_buckets(current)?;
    if total.is_empty() && current.is_empty() {
        return Ok(None);
    }
    let count_type = total
        .values()
        .chain(previous.values())
        .chain(current.values())
        .next()
        .copied()
        .unwrap_or(HistogramCount::Integer(0));
    let zero = match count_type {
        HistogramCount::Integer(_) => HistogramCount::Integer(0),
        HistogramCount::DoubleBits(_) => HistogramCount::DoubleBits(0.0_f64.to_bits()),
    };
    let first = total
        .keys()
        .chain(current.keys())
        .copied()
        .min()
        .expect("a nonempty bucket map has a first index");
    let last = total
        .keys()
        .chain(current.keys())
        .copied()
        .max()
        .expect("a nonempty bucket map has a final index");
    let length = i64::from(last)
        .checked_sub(i64::from(first))
        .and_then(|length| length.checked_add(1))
        .and_then(|length| usize::try_from(length).ok())
        .ok_or_else(|| {
            TelemetryError::InvalidMetricSample(
                "exponential histogram rollup bucket range is too large".into(),
            )
        })?;
    if length > 1_000_000 {
        return Err(TelemetryError::InvalidMetricSample(
            "exponential histogram rollup bucket range exceeds one million buckets".into(),
        ));
    }
    let mut counts = Vec::with_capacity(length);
    for index in first..=last {
        let total = total.get(&index).copied().unwrap_or(zero);
        let current = current.get(&index).copied().unwrap_or(zero);
        let delta = if reset {
            current
        } else {
            let previous = previous.get(&index).copied().unwrap_or(zero);
            count_delta(previous, current).ok_or_else(|| {
                TelemetryError::InvalidMetricSample(
                    "exponential histogram bucket regressed without a reset".into(),
                )
            })?
        };
        counts.push(count_add(total, delta)?);
    }
    Ok(Some(ExponentialHistogramBuckets {
        spans: Arc::new(vec![crate::HistogramBucketSpan {
            offset: first,
            length: u32::try_from(length).map_err(|_| {
                TelemetryError::InvalidMetricSample(
                    "exponential histogram rollup bucket range is too large".into(),
                )
            })?,
        }]),
        bucket_counts: Arc::new(counts),
    }))
}

fn expand_buckets(
    buckets: Option<&ExponentialHistogramBuckets>,
) -> TelemetryResult<BTreeMap<i32, HistogramCount>> {
    let Some(buckets) = buckets else {
        return Ok(BTreeMap::new());
    };
    let expected = buckets.spans.iter().try_fold(0_usize, |total, span| {
        total
            .checked_add(usize::try_from(span.length).map_err(|_| {
                TelemetryError::InvalidMetricSample(
                    "exponential histogram span length cannot fit in memory".into(),
                )
            })?)
            .ok_or_else(|| {
                TelemetryError::InvalidMetricSample(
                    "exponential histogram bucket count overflow".into(),
                )
            })
    })?;
    if expected != buckets.bucket_counts.len() {
        return Err(TelemetryError::InvalidMetricSample(
            "exponential histogram spans disagree with bucket counts".into(),
        ));
    }
    let mut output = BTreeMap::new();
    let mut count_index = 0_usize;
    let mut prior_end = 0_i64;
    for (span_index, span) in buckets.spans.iter().enumerate() {
        let start = if span_index == 0 {
            i64::from(span.offset)
        } else {
            prior_end
                .checked_add(i64::from(span.offset))
                .ok_or_else(|| {
                    TelemetryError::InvalidMetricSample(
                        "exponential histogram span offset overflow".into(),
                    )
                })?
        };
        let end = start.checked_add(i64::from(span.length)).ok_or_else(|| {
            TelemetryError::InvalidMetricSample("exponential histogram span length overflow".into())
        })?;
        for index in start..end {
            let index = i32::try_from(index).map_err(|_| {
                TelemetryError::InvalidMetricSample(
                    "exponential histogram bucket index is out of range".into(),
                )
            })?;
            if output
                .insert(index, buckets.bucket_counts[count_index])
                .is_some()
            {
                return Err(TelemetryError::InvalidMetricSample(
                    "exponential histogram spans overlap".into(),
                ));
            }
            count_index += 1;
        }
        prior_end = end;
    }
    Ok(output)
}

fn accumulate_summary(
    total: &SummaryValue,
    previous: &SummaryValue,
    current: &SummaryValue,
) -> TelemetryResult<(MetricValue, bool)> {
    let same_quantiles = |left: &SummaryValue, right: &SummaryValue| {
        left.quantiles.len() == right.quantiles.len()
            && left
                .quantiles
                .iter()
                .zip(right.quantiles.iter())
                .all(|(left, right)| left.quantile_bits == right.quantile_bits)
    };
    if !same_quantiles(total, current) || !same_quantiles(previous, current) {
        return Err(TelemetryError::InvalidMetricSample(
            "summary rollup quantiles changed".into(),
        ));
    }
    let reset = current.count < previous.count;
    let delta_count = if reset {
        current.count
    } else {
        current.count - previous.count
    };
    Ok((
        MetricValue::Summary(SummaryValue {
            count: total.count.checked_add(delta_count).ok_or_else(|| {
                TelemetryError::InvalidMetricSample("summary rollup count overflow".into())
            })?,
            sum_bits: (f64::from_bits(total.sum_bits)
                + if reset {
                    f64::from_bits(current.sum_bits)
                } else {
                    f64::from_bits(current.sum_bits) - f64::from_bits(previous.sum_bits)
                })
            .to_bits(),
            quantiles: Arc::clone(&current.quantiles),
        }),
        reset,
    ))
}

fn numeric_bits_min(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => {
            Some(f64::from_bits(left).min(f64::from_bits(right)).to_bits())
        }
        (left, right) => left.or(right),
    }
}

fn numeric_bits_max(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => {
            Some(f64::from_bits(left).max(f64::from_bits(right)).to_bits())
        }
        (left, right) => left.or(right),
    }
}

fn write_atomically(path: &Path, bytes: &[u8]) -> TelemetryResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| TelemetryError::StorageIo("lifetime rollup path has no parent".into()))?;
    fs::create_dir_all(parent).map_err(|error| storage_io("create rollup directory", error))?;
    let temporary = temporary_path(path);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| storage_io("open rollup temporary", error))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| storage_io("write rollup temporary", error))?;
    fs::rename(&temporary, path).map_err(|error| storage_io("publish rollup catalog", error))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| storage_io("sync rollup directory", error))?;
    Ok(())
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

fn storage_io(operation: &str, error: std::io::Error) -> TelemetryError {
    TelemetryError::StorageIo(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        METRICS_TOPIC_ID, ResourceContext, ScopeContext, TelemetryRecordRef, TelemetrySignal,
    };
    use shard_stream_core::{LogicalPartitionId, ShardId};

    fn sum_point(offset: u64, timestamp: u64, value: i64) -> DurableMetricPoint {
        let partition = TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(0));
        DurableMetricPoint {
            stream_shard_id: ShardId::new(0),
            record_ref: TelemetryRecordRef::for_signal(
                TelemetrySignal::Metrics,
                partition,
                LogicalOffset::new(offset),
            ),
            identity: Arc::new(MetricIdentity {
                tenant: Arc::from("tenant-a"),
                resource: Arc::new(ResourceContext::default()),
                scope: Arc::new(ScopeContext::default()),
                name: Arc::from("requests_total"),
                unit: Arc::from("1"),
                kind: MetricKind::Sum {
                    temporality: 2,
                    monotonic: true,
                },
                point_attributes: Arc::new(Vec::new()),
            }),
            description: Arc::from("requests"),
            metadata: Arc::new(Vec::new()),
            start_time_unix_nanos: 0,
            timestamp_unix_nanos: timestamp,
            flags: 0,
            value: MetricValue::Sum(NumberValue::Integer(value)),
            exemplars: Arc::new(Vec::new()),
        }
    }

    #[test]
    fn cumulative_sum_rollup_survives_resets_and_recovery() {
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-rollup-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let path = directory.join("rollups.msgpack");
        let partition = TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(0));
        let mut catalog = MetricRollupCatalog::open(path.clone(), 8, 1024 * 1024).expect("catalog");
        catalog
            .apply_batch(
                partition,
                LogicalOffset::new(0),
                vec![sum_point(0, 1, 4), sum_point(1, 2, 9), sum_point(2, 3, 2)],
            )
            .expect("apply");
        assert_eq!(catalog.persist().expect("persist"), 3);
        let recovered = MetricRollupCatalog::open(path, 8, 1024 * 1024).expect("recover");
        let rollups = recovered.query("tenant-a", Some("requests_total"));
        assert_eq!(rollups.len(), 1);
        assert_eq!(rollups[0].reset_count, 1);
        assert!(matches!(
            rollups[0].outcome,
            MetricValue::Sum(NumberValue::Integer(11))
        ));
        fs::remove_dir_all(directory).expect("cleanup");
    }
}
