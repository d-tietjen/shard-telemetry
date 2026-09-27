use super::*;

impl PromqlEngine {
    /// Creates a bounded single-tenant evaluator.
    #[must_use]
    pub fn new(store: Arc<DurableTelemetryStore>, tenant: Arc<str>, limits: PromqlLimits) -> Self {
        Self {
            store,
            tenant,
            limits,
        }
    }

    /// Evaluates one PromQL expression at one instant.
    pub fn query(&self, expression: &str, time_ms: i64) -> Result<PromqlValue, PromqlError> {
        let expr = parse(expression).map_err(PromqlError::new)?;
        let context = EvalContext {
            eval_ms: time_ms,
            start_ms: time_ms,
            end_ms: time_ms,
            lookback: self.limits.lookback,
        };
        self.eval(&expr, &context)
    }

    /// Evaluates one PromQL expression over an inclusive stepped range.
    pub fn query_range(
        &self,
        expression: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<PromqlValue, PromqlError> {
        if step_ms <= 0 || start_ms > end_ms {
            return Err(PromqlError::new("invalid PromQL query range"));
        }
        let steps = ((end_ms - start_ms) / step_ms) as usize + 1;
        if steps > self.limits.max_steps {
            return Err(PromqlError::new("PromQL range exceeds the step limit"));
        }
        let expr = parse(expression).map_err(PromqlError::new)?;
        let mut series = BTreeMap::<BTreeMap<String, String>, Vec<(i64, f64)>>::new();
        for ordinal in 0..steps {
            let eval_ms = start_ms + i64::try_from(ordinal).unwrap_or(i64::MAX) * step_ms;
            let value = self.eval(
                &expr,
                &EvalContext {
                    eval_ms,
                    start_ms,
                    end_ms,
                    lookback: self.limits.lookback,
                },
            )?;
            match value {
                PromqlValue::Scalar { value, .. } => {
                    series
                        .entry(BTreeMap::new())
                        .or_default()
                        .push((eval_ms, value));
                }
                PromqlValue::Vector(samples) => {
                    for sample in samples {
                        series
                            .entry(sample.labels)
                            .or_default()
                            .push((eval_ms, sample.value));
                    }
                }
                PromqlValue::Matrix(_) | PromqlValue::String { .. } => {
                    return Err(PromqlError::new(
                        "range query expression must return a scalar or instant vector",
                    ));
                }
            }
        }
        Ok(PromqlValue::Matrix(
            series
                .into_iter()
                .map(|(labels, samples)| PromqlSeries { labels, samples })
                .collect(),
        ))
    }

    /// Selects exact raw points for Prometheus discovery, metadata, and exemplar APIs.
    pub(crate) fn raw_points(
        &self,
        selectors: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<DurableMetricPoint>, PromqlError> {
        if start_ms > end_ms {
            return Err(PromqlError::new("invalid Prometheus discovery range"));
        }
        if selectors.is_empty() {
            return self
                .store
                .query_metrics(&MetricQuery {
                    tenant: Arc::clone(&self.tenant),
                    start_time_unix_nanos: Some(millis_to_nanos(start_ms)?),
                    end_time_unix_nanos: Some(millis_to_nanos(end_ms)?),
                    limit: self.limits.max_points,
                    ..MetricQuery::default()
                })
                .map_err(|error| PromqlError::new(error.to_string()));
        }
        let mut points = BTreeMap::new();
        for expression in selectors {
            let selector = match parse(expression).map_err(PromqlError::new)? {
                Expr::VectorSelector(selector) => selector,
                _ => {
                    return Err(PromqlError::new(
                        "series matchers must be instant vector selectors",
                    ));
                }
            };
            for point in self.scan_selector(&selector, start_ms, end_ms)? {
                let labels = point_labels(&point);
                if selector_matches(&selector, &labels) {
                    points.insert(
                        (point.record_ref.topic_partition, point.record_ref.offset),
                        point,
                    );
                }
            }
        }
        Ok(points.into_values().collect())
    }

    fn eval(&self, expr: &Expr, context: &EvalContext) -> Result<PromqlValue, PromqlError> {
        match expr {
            Expr::NumberLiteral(value) => Ok(PromqlValue::Scalar {
                timestamp_ms: context.eval_ms,
                value: value.val,
            }),
            Expr::StringLiteral(value) => Ok(PromqlValue::String {
                timestamp_ms: context.eval_ms,
                value: value.val.clone(),
            }),
            Expr::VectorSelector(selector) => self.select_vector(selector, context),
            Expr::MatrixSelector(selector) => self.select_matrix(selector, context),
            Expr::Paren(paren) => self.eval(&paren.expr, context),
            Expr::Unary(unary) => negate(self.eval(&unary.expr, context)?),
            Expr::Aggregate(aggregate) => self.aggregate(aggregate, context),
            Expr::Binary(binary) => self.binary(binary, context),
            Expr::Call(call) => self.call(call, context),
            Expr::Subquery(subquery) => self.eval_subquery(subquery, context),
            Expr::Extension(_) => Err(PromqlError::new("unsupported PromQL extension node")),
        }
    }

    fn select_vector(
        &self,
        selector: &VectorSelector,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let eval_ms = selector_time(selector, context)?;
        let lookback_ms = i64::try_from(context.lookback.as_millis()).unwrap_or(i64::MAX);
        let points = self.scan_selector(selector, eval_ms.saturating_sub(lookback_ms), eval_ms)?;
        let mut latest = BTreeMap::<BTreeMap<String, String>, DurableMetricPoint>::new();
        for point in points {
            let labels = point_labels(&point);
            if !selector_matches(selector, &labels) {
                continue;
            }
            let replace = latest.get(&labels).is_none_or(|prior| {
                (point.timestamp_unix_nanos, point.record_ref.offset)
                    > (prior.timestamp_unix_nanos, prior.record_ref.offset)
            });
            if replace {
                latest.insert(labels, point);
            }
        }
        let mut samples = Vec::with_capacity(latest.len());
        for (labels, point) in latest {
            if let Some(value) = point_float(&point) {
                if value.to_bits() == PROMETHEUS_STALE_NAN_BITS {
                    continue;
                }
                samples.push(PromqlSample {
                    labels,
                    timestamp_ms: eval_ms,
                    value,
                });
            }
        }
        self.bound_vector(samples).map(PromqlValue::Vector)
    }

    fn select_matrix(
        &self,
        selector: &MatrixSelector,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let eval_ms = selector_time(&selector.vs, context)?;
        let range_ms = i64::try_from(selector.range.as_millis()).unwrap_or(i64::MAX);
        let start_ms = eval_ms.saturating_sub(range_ms);
        let points = self.scan_selector(&selector.vs, start_ms, eval_ms)?;
        let mut series = BTreeMap::<BTreeMap<String, String>, Vec<(i64, f64)>>::new();
        for point in points {
            let labels = point_labels(&point);
            let timestamp_ms = nanos_to_millis(point.timestamp_unix_nanos)?;
            if timestamp_ms <= start_ms || !selector_matches(&selector.vs, &labels) {
                continue;
            }
            if let Some(value) = point_float(&point)
                && value.to_bits() != PROMETHEUS_STALE_NAN_BITS
            {
                series
                    .entry(labels)
                    .or_default()
                    .push((timestamp_ms, value));
            }
        }
        if series.len() > self.limits.max_series {
            return Err(PromqlError::new("PromQL series limit exceeded"));
        }
        Ok(PromqlValue::Matrix(
            series
                .into_iter()
                .map(|(labels, mut samples)| {
                    samples.sort_unstable_by_key(|(timestamp, _)| *timestamp);
                    PromqlSeries { labels, samples }
                })
                .collect(),
        ))
    }

    fn eval_subquery(
        &self,
        subquery: &SubqueryExpr,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let end_ms = subquery_time(subquery, context)?;
        let range_ms = i64::try_from(subquery.range.as_millis()).unwrap_or(i64::MAX);
        let start_ms = end_ms.saturating_sub(range_ms);
        let step = subquery.step.unwrap_or(Duration::from_secs(60));
        let step_ms = i64::try_from(step.as_millis()).unwrap_or(i64::MAX);
        if step_ms <= 0 {
            return Err(PromqlError::new("PromQL subquery step must be positive"));
        }
        let first_ms = start_ms
            .div_euclid(step_ms)
            .saturating_add(1)
            .saturating_mul(step_ms);
        let step_count = if first_ms > end_ms {
            0
        } else {
            usize::try_from((end_ms - first_ms) / step_ms)
                .unwrap_or(usize::MAX)
                .saturating_add(1)
        };
        if step_count > self.limits.max_steps {
            return Err(PromqlError::new("PromQL subquery exceeds the step limit"));
        }

        let mut series = BTreeMap::<BTreeMap<String, String>, Vec<(i64, f64)>>::new();
        for ordinal in 0..step_count {
            let eval_ms = first_ms.saturating_add(
                i64::try_from(ordinal)
                    .unwrap_or(i64::MAX)
                    .saturating_mul(step_ms),
            );
            match self.eval(
                &subquery.expr,
                &EvalContext {
                    eval_ms,
                    start_ms: context.start_ms,
                    end_ms: context.end_ms,
                    lookback: context.lookback,
                },
            )? {
                PromqlValue::Vector(samples) => {
                    for sample in samples {
                        if sample.value.to_bits() != PROMETHEUS_STALE_NAN_BITS {
                            series
                                .entry(sample.labels)
                                .or_default()
                                .push((eval_ms, sample.value));
                        }
                    }
                }
                PromqlValue::Scalar { value, .. } => {
                    series
                        .entry(BTreeMap::new())
                        .or_default()
                        .push((eval_ms, value));
                }
                PromqlValue::Matrix(_) | PromqlValue::String { .. } => {
                    return Err(PromqlError::new(
                        "PromQL subquery expression must return an instant vector or scalar",
                    ));
                }
            }
            if series.len() > self.limits.max_series {
                return Err(PromqlError::new("PromQL series limit exceeded"));
            }
        }
        Ok(PromqlValue::Matrix(
            series
                .into_iter()
                .map(|(labels, samples)| PromqlSeries { labels, samples })
                .collect(),
        ))
    }

    fn scan_selector(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<DurableMetricPoint>, PromqlError> {
        if start_ms < 0 || end_ms < 0 {
            return Err(PromqlError::new(
                "pre-epoch metric timestamps are outside the current storage epoch",
            ));
        }
        let name = selector_name(selector);
        let exact_labels = selector
            .matchers
            .matchers
            .iter()
            .filter_map(|matcher| match matcher.op {
                MatchOp::Equal if matcher.name != "__name__" => Some((
                    Arc::<str>::from(matcher.name.as_str()),
                    Arc::<str>::from(matcher.value.as_str()),
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        self.store
            .query_metrics(&MetricQuery {
                tenant: Arc::clone(&self.tenant),
                partition: None,
                start_offset: None,
                series: None,
                name: name.map(Arc::from),
                exact_labels: Arc::new(exact_labels),
                start_time_unix_nanos: Some(millis_to_nanos(start_ms)?),
                end_time_unix_nanos: Some(millis_to_nanos(end_ms)?),
                limit: self.limits.max_points,
            })
            .map_err(|error| PromqlError::new(error.to_string()))
    }

    fn aggregate(
        &self,
        aggregate: &AggregateExpr,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let PromqlValue::Vector(samples) = self.eval(&aggregate.expr, context)? else {
            return Err(PromqlError::new("aggregation requires an instant vector"));
        };
        let mut groups = BTreeMap::<BTreeMap<String, String>, Vec<f64>>::new();
        for sample in samples {
            let labels = grouped_labels(sample.labels, aggregate.modifier.as_ref());
            groups.entry(labels).or_default().push(sample.value);
        }
        let operation = aggregate.op.to_string();
        let mut output = Vec::with_capacity(groups.len());
        for (labels, values) in groups {
            let value = match operation.as_str() {
                "sum" => values.iter().sum(),
                "avg" => values.iter().sum::<f64>() / values.len() as f64,
                "count" => values.len() as f64,
                "group" => 1.0,
                "min" => values.iter().copied().reduce(f64::min).unwrap_or(f64::NAN),
                "max" => values.iter().copied().reduce(f64::max).unwrap_or(f64::NAN),
                "stddev" => variance(&values).sqrt(),
                "stdvar" => variance(&values),
                _ => {
                    return Err(PromqlError::new(format!(
                        "PromQL aggregator {operation} is not enabled"
                    )));
                }
            };
            output.push(PromqlSample {
                labels,
                timestamp_ms: context.eval_ms,
                value,
            });
        }
        self.bound_vector(output).map(PromqlValue::Vector)
    }

    fn binary(
        &self,
        binary: &BinaryExpr,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let left = self.eval(&binary.lhs, context)?;
        let right = self.eval(&binary.rhs, context)?;
        let op = binary.op.to_string();
        match (left, right) {
            (
                PromqlValue::Scalar {
                    timestamp_ms,
                    value: left,
                },
                PromqlValue::Scalar { value: right, .. },
            ) => Ok(PromqlValue::Scalar {
                timestamp_ms,
                value: binary_float(&op, left, right, binary.return_bool())?.unwrap_or(f64::NAN),
            }),
            (PromqlValue::Vector(samples), PromqlValue::Scalar { value, .. }) => self
                .bound_vector(binary_vector_scalar(
                    samples,
                    value,
                    &op,
                    false,
                    binary.return_bool(),
                )?)
                .map(PromqlValue::Vector),
            (PromqlValue::Scalar { value, .. }, PromqlValue::Vector(samples)) => self
                .bound_vector(binary_vector_scalar(
                    samples,
                    value,
                    &op,
                    true,
                    binary.return_bool(),
                )?)
                .map(PromqlValue::Vector),
            (PromqlValue::Vector(left), PromqlValue::Vector(right)) => self
                .bound_vector(binary_vectors(left, right, binary, &op)?)
                .map(PromqlValue::Vector),
            _ => Err(PromqlError::new("unsupported PromQL binary operand types")),
        }
    }

    fn call(&self, call: &Call, context: &EvalContext) -> Result<PromqlValue, PromqlError> {
        match call.func.name {
            "time" => Ok(PromqlValue::Scalar {
                timestamp_ms: context.eval_ms,
                value: context.eval_ms as f64 / 1_000.0,
            }),
            "vector" => {
                let value = self.eval(call_arg(call, 0)?, context)?;
                let PromqlValue::Scalar { value, .. } = value else {
                    return Err(PromqlError::new("vector() requires a scalar"));
                };
                Ok(PromqlValue::Vector(vec![PromqlSample {
                    labels: BTreeMap::new(),
                    timestamp_ms: context.eval_ms,
                    value,
                }]))
            }
            "scalar" => {
                let value = self.eval(call_arg(call, 0)?, context)?;
                let PromqlValue::Vector(samples) = value else {
                    return Err(PromqlError::new("scalar() requires an instant vector"));
                };
                Ok(PromqlValue::Scalar {
                    timestamp_ms: context.eval_ms,
                    value: if samples.len() == 1 {
                        samples[0].value
                    } else {
                        f64::NAN
                    },
                })
            }
            "rate" | "irate" | "increase" | "delta" | "idelta" | "changes" | "resets"
            | "sum_over_time" | "avg_over_time" | "min_over_time" | "max_over_time"
            | "count_over_time" | "last_over_time" | "present_over_time" => {
                let value = self.eval(call_arg(call, 0)?, context)?;
                let PromqlValue::Matrix(series) = value else {
                    return Err(PromqlError::new(format!(
                        "{}() requires a range vector",
                        call.func.name
                    )));
                };
                self.range_function(call.func.name, series, context)
            }
            name if is_unary_math(name) => {
                let value = self.eval(call_arg(call, 0)?, context)?;
                map_vector(value, context.eval_ms, |value| unary_math(name, value))
            }
            name => Err(PromqlError::new(format!(
                "PromQL function {name} is not enabled"
            ))),
        }
    }

    fn range_function(
        &self,
        function: &str,
        series: Vec<PromqlSeries>,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let mut output = Vec::new();
        for series in series {
            let values = series
                .samples
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
            let value = match function {
                "sum_over_time" => values.iter().sum(),
                "avg_over_time" => values.iter().sum::<f64>() / values.len() as f64,
                "min_over_time" => values.iter().copied().reduce(f64::min).unwrap_or(f64::NAN),
                "max_over_time" => values.iter().copied().reduce(f64::max).unwrap_or(f64::NAN),
                "count_over_time" => values.len() as f64,
                "last_over_time" => *values.last().unwrap_or(&f64::NAN),
                "present_over_time" => f64::from(!values.is_empty()),
                "changes" => values.windows(2).filter(|pair| pair[0] != pair[1]).count() as f64,
                "resets" => values.windows(2).filter(|pair| pair[1] < pair[0]).count() as f64,
                "delta" => delta(&series.samples, false),
                "idelta" => delta(&series.samples, true),
                "rate" => counter_rate(&series.samples, false),
                "irate" => counter_rate(&series.samples, true),
                "increase" => {
                    let duration = series
                        .samples
                        .last()
                        .zip(series.samples.first())
                        .map_or(0.0, |(last, first)| (last.0 - first.0) as f64 / 1_000.0);
                    counter_rate(&series.samples, false) * duration
                }
                _ => unreachable!("range function was matched by caller"),
            };
            output.push(PromqlSample {
                labels: series.labels,
                timestamp_ms: context.eval_ms,
                value,
            });
        }
        self.bound_vector(output).map(PromqlValue::Vector)
    }

    fn bound_vector(&self, samples: Vec<PromqlSample>) -> Result<Vec<PromqlSample>, PromqlError> {
        if samples.len() > self.limits.max_series {
            Err(PromqlError::new("PromQL series limit exceeded"))
        } else {
            Ok(samples)
        }
    }
}
