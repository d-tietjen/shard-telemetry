//! Direct metric snapshots from `fast-telemetry` into an embedded store.
//!
//! Metric recording remains entirely inside `fast-telemetry`. A host-owned
//! background task calls [`FastTelemetryExporter::export_once`] at a bounded
//! interval; only that snapshot path allocates storage-native points or waits
//! for the local durable WAL.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ::fast_telemetry::{
    DistributionSnapshot, HistogramSnapshot, MetricLabels, MetricMeta, MetricVisitor, Runtime,
};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicPartition};

use crate::{
    DurableMetricPoint, EmbeddedTelemetryRuntime, ExplicitHistogramValue,
    ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramBucketSpan, HistogramCount,
    LokiApiError, METRICS_TOPIC_ID, MetricIdentity, MetricKind, MetricValue,
    NativeTelemetryAppendAck, NumberValue, ResourceContext, ScopeContext, TelemetryAttribute,
    TelemetryRecordRef, TelemetrySignal, TelemetryValue,
};

const CUMULATIVE_TEMPORALITY: i32 = 2;
const DEFAULT_EXPORT_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_MAX_POINTS_PER_EXPORT: usize = 65_536;

/// Bounded configuration for a periodic `fast-telemetry` snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FastTelemetryConfig {
    /// Tenant attached to every exported metric series.
    pub tenant: Arc<str>,
    /// Resource attributes attached to every exported metric series.
    pub resource_attributes: Arc<Vec<TelemetryAttribute>>,
    /// Version attached to each registered `fast-telemetry` scope.
    pub scope_version: Arc<str>,
    /// Recommended interval for the host-owned export timer.
    pub interval: Duration,
    /// Maximum points emitted by one runtime snapshot.
    pub max_points_per_export: usize,
    /// Whether zero counters and empty histograms/distributions are skipped.
    pub skip_empty_cumulative: bool,
    /// Whether an export waits until the local query index has applied its WAL
    /// append.
    pub wait_for_index: bool,
}

impl FastTelemetryConfig {
    /// Creates a conventional configuration with one `service.name` resource
    /// attribute.
    #[must_use]
    pub fn new(tenant: impl Into<Arc<str>>, service_name: impl Into<Arc<str>>) -> Self {
        Self {
            tenant: tenant.into(),
            resource_attributes: Arc::new(vec![TelemetryAttribute::new(
                "service.name",
                TelemetryValue::String(service_name.into()),
            )]),
            scope_version: Arc::from("fast-telemetry"),
            interval: DEFAULT_EXPORT_INTERVAL,
            max_points_per_export: DEFAULT_MAX_POINTS_PER_EXPORT,
            skip_empty_cumulative: true,
            wait_for_index: true,
        }
    }

    /// Adds one typed resource attribute to every exported series.
    #[must_use]
    pub fn with_resource_attribute(mut self, attribute: TelemetryAttribute) -> Self {
        Arc::make_mut(&mut self.resource_attributes).push(attribute);
        self
    }

    /// Sets the recommended host-owned export interval.
    #[must_use]
    pub const fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Sets the maximum points accepted from one snapshot.
    #[must_use]
    pub const fn with_max_points_per_export(mut self, max_points: usize) -> Self {
        self.max_points_per_export = max_points;
        self
    }

    /// Sets whether empty cumulative instruments are omitted.
    #[must_use]
    pub const fn with_skip_empty_cumulative(mut self, skip: bool) -> Self {
        self.skip_empty_cumulative = skip;
        self
    }

    /// Chooses between query-visible and WAL-only local acknowledgement.
    #[must_use]
    pub const fn with_wait_for_index(mut self, wait_for_index: bool) -> Self {
        self.wait_for_index = wait_for_index;
        self
    }

    fn validate(&self) -> Result<(), LokiApiError> {
        if self.tenant.is_empty() {
            return Err(LokiApiError::configuration(
                "fast-telemetry tenant must not be empty",
            ));
        }
        if self.interval.is_zero() {
            return Err(LokiApiError::configuration(
                "fast-telemetry export interval must be nonzero",
            ));
        }
        if self.max_points_per_export == 0 {
            return Err(LokiApiError::configuration(
                "fast-telemetry export point limit must be nonzero",
            ));
        }
        Ok(())
    }
}

/// Counters returned by one embedded snapshot export.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FastTelemetryExportReport {
    /// Points observed after empty-cumulative filtering and before the cap.
    pub observed_points: usize,
    /// Points omitted because the configured snapshot cap was reached.
    pub dropped_points: usize,
    /// Points acknowledged by the embedded store.
    pub appended_points: usize,
    /// Per-series durable envelopes appended by the store.
    pub append_batches: usize,
    /// Whether another export was already active.
    pub skipped_busy: bool,
}

/// Background-driven bridge from a `fast-telemetry` runtime to local durable
/// storage.
pub struct FastTelemetryExporter {
    runtime: Arc<Runtime>,
    embedded: Arc<EmbeddedTelemetryRuntime>,
    config: FastTelemetryConfig,
    exporting: AtomicBool,
}

impl FastTelemetryExporter {
    /// Creates an exporter for one runtime and embedded store.
    ///
    /// Construct this through [`EmbeddedTelemetryRuntime::attach`] before
    /// marking the embedded runtime ready. Call [`Self::export_once`] only
    /// from a background worker, never from an application metric update.
    pub fn new(
        runtime: Arc<Runtime>,
        embedded: Arc<EmbeddedTelemetryRuntime>,
        config: FastTelemetryConfig,
    ) -> Result<Self, LokiApiError> {
        config.validate()?;
        Ok(Self {
            runtime,
            embedded,
            config,
            exporting: AtomicBool::new(false),
        })
    }

    /// Returns the recommended interval for a host-owned periodic task.
    #[must_use]
    pub const fn interval(&self) -> Duration {
        self.config.interval
    }

    /// Snapshots registered metric groups at the current wall-clock time.
    pub fn export_once(&self) -> Result<FastTelemetryExportReport, LokiApiError> {
        self.export_once_at(unix_nanos_now())
    }

    /// Snapshots registered metric groups at an explicit timestamp.
    pub fn export_once_at(
        &self,
        timestamp_unix_nanos: u64,
    ) -> Result<FastTelemetryExportReport, LokiApiError> {
        if self
            .exporting
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Ok(FastTelemetryExportReport {
                skipped_busy: true,
                ..FastTelemetryExportReport::default()
            });
        }
        let _guard = ExportGuard {
            exporting: &self.exporting,
        };
        let mut export = FastMetricExport::new(&self.embedded, &self.config, timestamp_unix_nanos);

        // Runtime registration is a construction-time operation. Deduplicating
        // scopes avoids revisiting all groups when several groups intentionally
        // share one logical scope.
        let mut scopes = self.runtime.scopes();
        scopes.sort_unstable();
        scopes.dedup();
        for scope in scopes {
            export.set_scope(scope.name());
            self.runtime.visit_metrics_for_scope(&scope, &mut export);
        }

        let observed_points = export.observed_points;
        let dropped_points = export.dropped_points;
        let (appended_points, acknowledgement) = export.finish()?;
        Ok(FastTelemetryExportReport {
            observed_points,
            dropped_points,
            appended_points,
            append_batches: acknowledgement.partitions.len(),
            skipped_busy: false,
        })
    }
}

struct ExportGuard<'a> {
    exporting: &'a AtomicBool,
}

impl Drop for ExportGuard<'_> {
    fn drop(&mut self) {
        self.exporting.store(false, Ordering::Release);
    }
}

struct FastMetricExport<'a> {
    embedded: &'a EmbeddedTelemetryRuntime,
    tenant: Arc<str>,
    resource: Arc<ResourceContext>,
    scope_version: Arc<str>,
    scope: Arc<ScopeContext>,
    timestamp_unix_nanos: u64,
    max_points_per_export: usize,
    skip_empty_cumulative: bool,
    wait_for_index: bool,
    points: Vec<DurableMetricPoint>,
    tail: Option<DurableMetricPoint>,
    observed_points: usize,
    dropped_points: usize,
}

impl<'a> FastMetricExport<'a> {
    fn new(
        embedded: &'a EmbeddedTelemetryRuntime,
        config: &FastTelemetryConfig,
        timestamp_unix_nanos: u64,
    ) -> Self {
        Self {
            embedded,
            tenant: Arc::clone(&config.tenant),
            resource: Arc::new(ResourceContext {
                attributes: Arc::clone(&config.resource_attributes),
                ..ResourceContext::default()
            }),
            scope_version: Arc::clone(&config.scope_version),
            scope: Arc::new(ScopeContext::default()),
            timestamp_unix_nanos,
            max_points_per_export: config.max_points_per_export,
            skip_empty_cumulative: config.skip_empty_cumulative,
            wait_for_index: config.wait_for_index,
            points: Vec::new(),
            tail: None,
            observed_points: 0,
            dropped_points: 0,
        }
    }

    fn set_scope(&mut self, scope_name: &str) {
        self.scope = Arc::new(ScopeContext {
            name: Arc::from(scope_name),
            version: Arc::clone(&self.scope_version),
            ..ScopeContext::default()
        });
    }

    fn finish(mut self) -> Result<(usize, NativeTelemetryAppendAck), LokiApiError> {
        let Some(tail) = self.tail.take() else {
            return Ok((
                0,
                NativeTelemetryAppendAck {
                    partitions: Vec::new(),
                },
            ));
        };
        let point_count = self.points.len().saturating_add(1);
        let acknowledgement = if self.points.is_empty() {
            self.embedded
                .append_metric_point(tail, self.wait_for_index)?
        } else {
            self.points.push(tail);
            self.embedded
                .append_metric_points(self.points, self.wait_for_index)?
        };
        Ok((point_count, acknowledgement))
    }

    fn push(
        &mut self,
        meta: MetricMeta<'_>,
        labels: MetricLabels<'_>,
        kind: MetricKind,
        value: MetricValue,
    ) {
        self.observed_points = self.observed_points.saturating_add(1);
        if self
            .points
            .len()
            .saturating_add(usize::from(self.tail.is_some()))
            >= self.max_points_per_export
        {
            self.dropped_points = self.dropped_points.saturating_add(1);
            return;
        }
        let point_attributes = labels
            .iter()
            .map(|label| {
                TelemetryAttribute::new(
                    Arc::<str>::from(label.name),
                    TelemetryValue::String(Arc::from(label.value)),
                )
            })
            .collect();
        let point = DurableMetricPoint {
            stream_shard_id: ShardId::new(0),
            record_ref: TelemetryRecordRef::for_signal(
                TelemetrySignal::Metrics,
                TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(0)),
                LogicalOffset::new(0),
            ),
            identity: Arc::new(MetricIdentity {
                tenant: Arc::clone(&self.tenant),
                resource: Arc::clone(&self.resource),
                scope: Arc::clone(&self.scope),
                name: Arc::from(meta.name),
                unit: Arc::from(meta.unit.unwrap_or("")),
                kind,
                point_attributes: Arc::new(point_attributes),
            }),
            description: Arc::from(meta.help),
            metadata: Arc::new(Vec::new()),
            start_time_unix_nanos: 0,
            timestamp_unix_nanos: self.timestamp_unix_nanos,
            flags: 0,
            value,
            exemplars: Arc::new(Vec::new()),
        };
        if let Some(previous) = self.tail.replace(point) {
            self.points.push(previous);
        }
    }
}

impl MetricVisitor for FastMetricExport<'_> {
    fn counter(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: i64) {
        if self.skip_empty_cumulative && value == 0 {
            return;
        }
        self.push(
            meta,
            labels,
            MetricKind::Sum {
                temporality: CUMULATIVE_TEMPORALITY,
                monotonic: true,
            },
            MetricValue::Sum(NumberValue::Integer(value)),
        );
    }

    fn gauge_i64(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: i64) {
        self.push(
            meta,
            labels,
            MetricKind::Gauge,
            MetricValue::Gauge(NumberValue::Integer(value)),
        );
    }

    fn gauge_f64(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: f64) {
        self.push(
            meta,
            labels,
            MetricKind::Gauge,
            MetricValue::Gauge(NumberValue::from_f64(value)),
        );
    }

    fn histogram(
        &mut self,
        meta: MetricMeta<'_>,
        labels: MetricLabels<'_>,
        histogram: &dyn HistogramSnapshot,
    ) {
        if self.skip_empty_cumulative && histogram.count() == 0 {
            return;
        }
        let mut explicit_bounds_bits = Vec::new();
        let mut bucket_counts = Vec::new();
        let mut previous = 0_u64;
        histogram.visit_buckets(&mut |bound, cumulative_count| {
            explicit_bounds_bits.push((bound as f64).to_bits());
            bucket_counts.push(HistogramCount::Integer(
                cumulative_count.saturating_sub(previous),
            ));
            previous = cumulative_count;
        });
        bucket_counts.push(HistogramCount::Integer(
            histogram.count().saturating_sub(previous),
        ));
        self.push(
            meta,
            labels,
            MetricKind::ExplicitHistogram {
                temporality: CUMULATIVE_TEMPORALITY,
            },
            MetricValue::ExplicitHistogram(ExplicitHistogramValue {
                count: HistogramCount::Integer(histogram.count()),
                sum_bits: Some((histogram.sum() as f64).to_bits()),
                bucket_counts: Arc::new(bucket_counts),
                explicit_bounds_bits: Arc::new(explicit_bounds_bits),
                min_bits: None,
                max_bits: None,
                reset_hint: 0,
            }),
        );
    }

    fn distribution(
        &mut self,
        meta: MetricMeta<'_>,
        labels: MetricLabels<'_>,
        distribution: &dyn DistributionSnapshot,
    ) {
        if self.skip_empty_cumulative && distribution.count() == 0 {
            return;
        }
        let mut sparse = Vec::new();
        distribution.visit_positive_buckets(&mut |index, count| sparse.push((index, count)));
        sparse.sort_unstable_by_key(|(index, _)| *index);
        let positive = (!sparse.is_empty()).then(|| {
            let mut spans = Vec::<HistogramBucketSpan>::new();
            let mut counts = Vec::with_capacity(sparse.len());
            let mut prior_end = 0_i64;
            for (index, count) in sparse {
                let index = i64::from(index);
                if let Some(span) = spans.last_mut()
                    && index == prior_end
                {
                    span.length = span.length.saturating_add(1);
                } else {
                    let offset = if spans.is_empty() {
                        index
                    } else {
                        index.saturating_sub(prior_end)
                    };
                    spans.push(HistogramBucketSpan {
                        offset: i32::try_from(offset).unwrap_or(i32::MAX),
                        length: 1,
                    });
                }
                counts.push(HistogramCount::Integer(count));
                prior_end = index.saturating_add(1);
            }
            ExponentialHistogramBuckets {
                spans: Arc::new(spans),
                bucket_counts: Arc::new(counts),
            }
        });
        self.push(
            meta,
            labels,
            MetricKind::ExponentialHistogram {
                temporality: CUMULATIVE_TEMPORALITY,
            },
            MetricValue::ExponentialHistogram(ExponentialHistogramValue {
                count: HistogramCount::Integer(distribution.count()),
                sum_bits: Some((distribution.sum() as f64).to_bits()),
                scale: 0,
                zero_count: HistogramCount::Integer(distribution.zero_count()),
                positive,
                negative: None,
                min_bits: distribution.min().map(|value| (value as f64).to_bits()),
                max_bits: distribution.max().map(|value| (value as f64).to_bits()),
                zero_threshold_bits: 0.0_f64.to_bits(),
                reset_hint: 0,
            }),
        );
    }
}

fn unix_nanos_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use ::fast_telemetry::{
        Counter, Distribution, ExportMetrics, Gauge, Histogram, MetricKind as FastMetricKind,
        MetricLabel, MetricLabels, MetricMeta, MetricScope, RuntimeConfig,
    };

    use super::*;
    use crate::{EmbeddedTelemetryConfig, MetricQuery};

    struct TestMetrics {
        requests: Counter,
        in_flight: Gauge,
        latency: Histogram,
        payload: Distribution,
    }

    impl TestMetrics {
        fn new() -> Self {
            Self {
                requests: Counter::new(4),
                in_flight: Gauge::new(),
                latency: Histogram::new(&[10, 100], 4),
                payload: Distribution::new(4),
            }
        }
    }

    impl ExportMetrics for TestMetrics {
        fn visit_metrics<V: MetricVisitor + ?Sized>(&self, visitor: &mut V) {
            let route = [MetricLabel {
                name: "route",
                value: "/health",
            }];
            visitor.counter(
                MetricMeta {
                    name: "requests_total",
                    help: "Total requests.",
                    kind: FastMetricKind::Counter,
                    unit: Some("1"),
                },
                MetricLabels::slice(&route),
                self.requests.sum() as i64,
            );
            visitor.gauge_i64(
                MetricMeta {
                    name: "in_flight",
                    help: "In-flight requests.",
                    kind: FastMetricKind::Gauge,
                    unit: Some("1"),
                },
                MetricLabels::none(),
                self.in_flight.get(),
            );
            visitor.histogram(
                MetricMeta {
                    name: "latency_us",
                    help: "Request latency.",
                    kind: FastMetricKind::Histogram,
                    unit: Some("us"),
                },
                MetricLabels::none(),
                &self.latency,
            );
            visitor.distribution(
                MetricMeta {
                    name: "payload_bytes",
                    help: "Payload size.",
                    kind: FastMetricKind::Distribution,
                    unit: Some("By"),
                },
                MetricLabels::none(),
                &self.payload,
            );
        }
    }

    #[test]
    fn runtime_snapshot_is_queryable_from_the_embedded_store() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-fast-runtime-{}-{nonce}",
            std::process::id()
        ));
        let embedded = Arc::new(
            EmbeddedTelemetryRuntime::open(EmbeddedTelemetryConfig::bounded_local(
                directory.clone(),
                Duration::from_secs(60),
            ))
            .expect("embedded runtime"),
        );
        let runtime = Runtime::new(RuntimeConfig::default());
        let registered = runtime.register_metrics(MetricScope::new("api"), TestMetrics::new());
        registered.requests.add(3);
        registered.in_flight.set(7);
        registered.latency.record(5);
        registered.latency.record(50);
        registered.payload.record(0);
        registered.payload.record(16);

        let exporter = embedded
            .attach(|| {
                FastTelemetryExporter::new(
                    runtime,
                    Arc::clone(&embedded),
                    FastTelemetryConfig::new("tenant-a", "embedded-test"),
                )
            })
            .expect("exporter attachment");
        embedded.mark_ready().expect("ready");
        let report = exporter.export_once().expect("snapshot export");
        assert_eq!(report.observed_points, 4);
        assert_eq!(report.dropped_points, 0);
        assert_eq!(report.appended_points, 4);
        assert!(!report.skipped_busy);

        let points = embedded
            .query_metrics(&MetricQuery {
                tenant: Arc::from("tenant-a"),
                limit: 10,
                ..MetricQuery::default()
            })
            .expect("local metric query");
        assert_eq!(points.len(), 4);
        let requests = points
            .iter()
            .find(|point| point.identity.name.as_ref() == "requests_total")
            .expect("counter");
        assert_eq!(requests.identity.scope.name.as_ref(), "api");
        assert_eq!(requests.identity.resource.attributes.len(), 1);
        assert_eq!(requests.identity.point_attributes.len(), 1);
        assert!(matches!(
            requests.value,
            MetricValue::Sum(NumberValue::Integer(3))
        ));
        let distribution = points
            .iter()
            .find(|point| point.identity.name.as_ref() == "payload_bytes")
            .expect("distribution");
        assert!(matches!(
            distribution.value,
            MetricValue::ExponentialHistogram(ExponentialHistogramValue {
                scale: 0,
                zero_count: HistogramCount::Integer(1),
                ..
            })
        ));

        registered.requests.add(2);
        registered.payload.record(4_096);
        let second = exporter.export_once().expect("second snapshot export");
        assert_eq!(second.appended_points, 4);
        let rollups = embedded
            .query_lifetime_metric_rollups("tenant-a", None)
            .expect("lifetime outcomes");
        assert_eq!(rollups.len(), 4);
        let requests = rollups
            .iter()
            .find(|rollup| rollup.identity.name.as_ref() == "requests_total")
            .expect("request lifetime outcome");
        assert_eq!(requests.observed_points, 2);
        assert!(matches!(
            requests.outcome,
            MetricValue::Sum(NumberValue::Integer(5))
        ));

        embedded.compact_retention().expect("retention pass");
        embedded.drain().expect("drain");
        drop(exporter);
        drop(embedded);
        std::fs::remove_dir_all(directory).expect("cleanup");
    }
}
