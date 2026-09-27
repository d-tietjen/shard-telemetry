//! PromQL ownership: evaluator and expression helpers live in `promql/`.
mod engine;
mod eval;
use eval::*;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::{
    AggregateExpr, AtModifier, BinaryExpr, Call, Expr, LabelModifier, MatrixSelector, Offset,
    SubqueryExpr, VectorMatchCardinality, VectorSelector, parse,
};

use crate::{
    DurableMetricPoint, DurableTelemetryStore, MetricQuery, MetricValue, NumberValue,
    prometheus_string_labels,
};

const DEFAULT_LOOKBACK: Duration = Duration::from_secs(5 * 60);
const PROMETHEUS_STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Limits for one native PromQL evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlLimits {
    /// Maximum raw points materialized from storage.
    pub max_points: usize,
    /// Maximum output series.
    pub max_series: usize,
    /// Maximum range-query steps.
    pub max_steps: usize,
    /// Selector lookback used when a query does not specify a range.
    pub lookback: Duration,
}

impl Default for PromqlLimits {
    fn default() -> Self {
        Self {
            max_points: 1_000_000,
            max_series: 100_000,
            max_steps: 11_000,
            lookback: DEFAULT_LOOKBACK,
        }
    }
}

/// One Prometheus float sample with its complete label set.
#[derive(Debug, Clone, PartialEq)]
pub struct PromqlSample {
    /// Prometheus-visible labels, including `__name__` when retained.
    pub labels: BTreeMap<String, String>,
    /// Evaluation timestamp in milliseconds since the Unix epoch.
    pub timestamp_ms: i64,
    /// Floating-point sample value.
    pub value: f64,
}

/// One matrix series returned by a range query.
#[derive(Debug, Clone, PartialEq)]
pub struct PromqlSeries {
    /// Prometheus-visible labels.
    pub labels: BTreeMap<String, String>,
    /// Timestamp/value pairs in evaluation order.
    pub samples: Vec<(i64, f64)>,
}

/// Native PromQL result value.
#[derive(Debug, Clone, PartialEq)]
pub enum PromqlValue {
    /// Scalar at an evaluation timestamp.
    Scalar {
        /// Evaluation timestamp.
        timestamp_ms: i64,
        /// Scalar value.
        value: f64,
    },
    /// String at an evaluation timestamp.
    String {
        /// Evaluation timestamp.
        timestamp_ms: i64,
        /// String value.
        value: String,
    },
    /// Instant vector.
    Vector(Vec<PromqlSample>),
    /// Range vector or range-query output.
    Matrix(Vec<PromqlSeries>),
}

/// Parse or evaluation error returned through the Prometheus API envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlError(String);

impl PromqlError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for PromqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for PromqlError {}

/// Rust PromQL evaluator backed by ShardTelemetry metric stripes.
#[derive(Clone)]
pub struct PromqlEngine {
    store: Arc<DurableTelemetryStore>,
    tenant: Arc<str>,
    limits: PromqlLimits,
}

impl fmt::Debug for PromqlEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PromqlEngine")
            .field("tenant", &self.tenant)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
