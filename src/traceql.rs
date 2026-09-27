//! TraceQL ownership: engine and result models, parser, spanset evaluation, and metric stages.
//! `traceql/` holds parsing and evaluation details; this root keeps the public engine.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use regex::Regex;

use crate::{
    DurableSpan, DurableTelemetryStore, TelemetryAttribute, TelemetryValue, TraceId, TraceQuery,
};

mod summary;
use summary::*;
mod parse;
use parse::*;
mod metric;
use metric::*;
mod spanset;
use spanset::*;
mod filter;
use filter::*;
mod lex;
use lex::*;
#[cfg(test)]
mod tests;

/// Bounded TraceQL execution limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceqlLimits {
    /// Maximum spans materialized before trace grouping.
    pub max_spans: usize,
    /// Maximum traces returned by one search.
    pub max_traces: usize,
}

impl Default for TraceqlLimits {
    fn default() -> Self {
        Self {
            max_spans: 1_000_000,
            max_traces: 1_000,
        }
    }
}

/// One trace selected by the clean-room TraceQL evaluator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceqlTrace {
    /// Trace ID.
    pub trace_id: TraceId,
    /// Winning spans ordered by start time and durable offset.
    pub spans: Vec<DurableSpan>,
    /// Earliest span start.
    pub start_time_unix_nanos: u64,
    /// Latest span end.
    pub end_time_unix_nanos: u64,
    /// Root span name when present.
    pub root_name: Option<Arc<str>>,
    /// Root resource service name when present.
    pub root_service_name: Option<Arc<str>>,
    /// Number of error spans.
    pub error_count: u32,
    /// Fields requested by the final `select(...)` pipeline stage.
    pub selected_fields: Arc<Vec<String>>,
}

/// One trace-linked exemplar emitted by a TraceQL metrics query.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceqlMetricExemplar {
    /// Trace that contributed the sample.
    pub trace_id: TraceId,
    /// Span that contributed the sample.
    pub span_id: crate::SpanId,
    /// Bucket timestamp in Unix milliseconds.
    pub timestamp_ms: u64,
    /// Aggregate value for the bucket.
    pub value: f64,
}

/// One TraceQL metrics sample.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceqlMetricSample {
    /// Bucket timestamp in Unix milliseconds.
    pub timestamp_ms: u64,
    /// Aggregate value.
    pub value: f64,
}

/// One Prometheus-like time series derived directly from matching spans.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceqlMetricSeries {
    /// TraceQL grouping labels.
    pub labels: BTreeMap<String, String>,
    /// Samples ordered by time.
    pub samples: Vec<TraceqlMetricSample>,
    /// Bounded trace-linked exemplars.
    pub exemplars: Vec<TraceqlMetricExemplar>,
}

/// TraceQL parse or execution error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceqlError(String);

impl TraceqlError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for TraceqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for TraceqlError {}

/// Clean-room Rust TraceQL evaluator backed by trace-owner stripes.
#[derive(Clone)]
pub struct TraceqlEngine {
    store: Arc<DurableTelemetryStore>,
    tenant: Arc<str>,
    limits: TraceqlLimits,
}

impl fmt::Debug for TraceqlEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TraceqlEngine")
            .field("tenant", &self.tenant)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl TraceqlEngine {
    /// Creates a bounded single-tenant evaluator.
    #[must_use]
    pub fn new(store: Arc<DurableTelemetryStore>, tenant: Arc<str>, limits: TraceqlLimits) -> Self {
        Self {
            store,
            tenant,
            limits,
        }
    }

    /// Executes a TraceQL spanset filter over a bounded trace/time window.
    pub fn search(
        &self,
        expression: &str,
        start_time_unix_nanos: Option<u64>,
        end_time_unix_nanos: Option<u64>,
        limit: usize,
    ) -> Result<Vec<TraceqlTrace>, TraceqlError> {
        let query = TraceqlQuery::parse(expression)?;
        let pushed_trace_id = query.exact_trace_id();
        let spans = self
            .store
            .query_traces(&TraceQuery {
                tenant: Arc::clone(&self.tenant),
                trace_id: pushed_trace_id,
                start_time_unix_nanos,
                end_time_unix_nanos,
                limit: self.limits.max_spans,
                ..TraceQuery::default()
            })
            .map_err(|error| TraceqlError::new(error.to_string()))?;
        let mut traces = BTreeMap::<TraceId, Vec<DurableSpan>>::new();
        for span in spans {
            traces.entry(span.trace_id).or_default().push(span);
        }
        let result_limit = limit.max(1).min(self.limits.max_traces);
        let selected_fields = query.selected_fields();
        let mut results = traces
            .into_iter()
            .filter_map(|(trace_id, mut spans)| {
                spans.sort_unstable_by_key(|span| {
                    (span.start_time_unix_nanos, span.record_ref.offset)
                });
                let selected = query.evaluate(&spans);
                (!selected.is_empty()).then(|| {
                    let spans = selected
                        .into_iter()
                        .map(|index| spans[index].clone())
                        .collect();
                    summarize(trace_id, spans, Arc::clone(&selected_fields))
                })
            })
            .collect::<Vec<_>>();
        results.sort_unstable_by_key(|trace| {
            (
                std::cmp::Reverse(trace.start_time_unix_nanos),
                trace.trace_id,
            )
        });
        results.truncate(result_limit);
        Ok(results)
    }

    /// Performs an indexed trace-ID lookup without parsing TraceQL.
    pub fn trace_by_id(&self, trace_id: TraceId) -> Result<Option<TraceqlTrace>, TraceqlError> {
        let spans = self
            .store
            .query_traces(&TraceQuery {
                tenant: Arc::clone(&self.tenant),
                trace_id: Some(trace_id),
                limit: self.limits.max_spans,
                ..TraceQuery::default()
            })
            .map_err(|error| TraceqlError::new(error.to_string()))?;
        Ok((!spans.is_empty()).then(|| summarize(trace_id, spans, Arc::default())))
    }

    /// Evaluates a bounded TraceQL metrics expression over an inclusive time range.
    pub fn query_metrics(
        &self,
        expression: &str,
        start_time_unix_nanos: u64,
        end_time_unix_nanos: u64,
        step_nanos: u64,
        instant: bool,
        max_exemplars: usize,
    ) -> Result<Vec<TraceqlMetricSeries>, TraceqlError> {
        if start_time_unix_nanos > end_time_unix_nanos || step_nanos == 0 {
            return Err(TraceqlError::new("invalid TraceQL metrics time range"));
        }
        let metric = TraceMetricQuery::parse(expression)?;
        let spans = self
            .store
            .query_traces(&TraceQuery {
                tenant: Arc::clone(&self.tenant),
                trace_id: metric.spanset.exact_trace_id(),
                start_time_unix_nanos: Some(start_time_unix_nanos),
                end_time_unix_nanos: end_time_unix_nanos.checked_add(1),
                limit: self.limits.max_spans,
                ..TraceQuery::default()
            })
            .map_err(|error| TraceqlError::new(error.to_string()))?;
        let mut traces = BTreeMap::<TraceId, Vec<DurableSpan>>::new();
        for span in spans {
            traces.entry(span.trace_id).or_default().push(span);
        }
        let mut buckets = BTreeMap::<MetricGroup, BTreeMap<u64, MetricBucket>>::new();
        for (trace_id, mut spans) in traces {
            spans.sort_unstable_by_key(|span| (span.start_time_unix_nanos, span.record_ref.offset));
            for index in metric.spanset.evaluate(&spans) {
                let span = &spans[index];
                let Some(labels) = metric.labels(span, &spans) else {
                    continue;
                };
                let Some(value) = metric.observed_value(span, &spans) else {
                    continue;
                };
                let bucket_timestamp = if instant {
                    end_time_unix_nanos
                } else {
                    let ordinal = span
                        .start_time_unix_nanos
                        .saturating_sub(start_time_unix_nanos)
                        / step_nanos;
                    start_time_unix_nanos
                        .saturating_add(ordinal.saturating_add(1).saturating_mul(step_nanos))
                        .min(end_time_unix_nanos)
                };
                let bucket = buckets
                    .entry(MetricGroup(labels))
                    .or_default()
                    .entry(bucket_timestamp)
                    .or_default();
                bucket.values.push(value);
                if bucket.exemplar.is_none() && max_exemplars > 0 {
                    bucket.exemplar = Some((trace_id, span.span_id));
                }
            }
        }

        let denominator_seconds = if instant {
            end_time_unix_nanos
                .saturating_sub(start_time_unix_nanos)
                .max(1) as f64
                / 1_000_000_000.0
        } else {
            step_nanos as f64 / 1_000_000_000.0
        };
        let mut remaining_exemplars = max_exemplars;
        let mut series = Vec::with_capacity(buckets.len());
        for (MetricGroup(labels), samples) in buckets {
            let mut output_samples = Vec::with_capacity(samples.len());
            let mut exemplars = Vec::new();
            for (timestamp_nanos, bucket) in samples {
                let value = metric.aggregate(&bucket.values, denominator_seconds);
                if !metric.passes(value) {
                    continue;
                }
                let timestamp_ms = timestamp_nanos / 1_000_000;
                output_samples.push(TraceqlMetricSample {
                    timestamp_ms,
                    value,
                });
                if remaining_exemplars > 0
                    && let Some((trace_id, span_id)) = bucket.exemplar
                {
                    exemplars.push(TraceqlMetricExemplar {
                        trace_id,
                        span_id,
                        timestamp_ms,
                        value,
                    });
                    remaining_exemplars -= 1;
                }
            }
            if !output_samples.is_empty() {
                series.push(TraceqlMetricSeries {
                    labels,
                    samples: output_samples,
                    exemplars,
                });
            }
        }
        metric.limit_series(series)
    }
}
