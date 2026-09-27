use super::*;

pub(super) async fn execute_metric_query(
    state: ApiState,
    headers: HeaderMap,
    params: QueryParams,
    mode: QueryMode,
) -> Result<Json<Value>, LokiApiError> {
    let started = std::time::Instant::now();
    let expression = parse_metric_expression(
        params
            .query
            .as_deref()
            .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?,
    )?;
    let tenant = tenant(&headers, &state.config);
    let entries = entries_for(&state, &tenant).await?;
    let total_lines_processed = entries.len();
    let total_bytes_processed = entries.iter().map(|entry| entry.line.len()).sum::<usize>();
    let (_, end) = query_range_bounds(&params)?;
    let evaluation_times = match mode {
        QueryMode::Instant => vec![end],
        QueryMode::Range => metric_evaluation_times(&params, state.config.max_query_limit)?,
    };
    let limit = params
        .limit
        .unwrap_or(state.config.max_query_limit)
        .min(state.config.max_query_limit);
    let mut series = BTreeMap::<BTreeMap<String, String>, Vec<Value>>::new();
    let mut scalar = None;
    for timestamp in evaluation_times {
        match evaluate_metric_expression(&expression, &entries, timestamp)? {
            MetricValue::Scalar(value) => scalar = Some((timestamp, value)),
            MetricValue::Vector(mut values) => {
                values.sort_by(|left, right| left.labels.cmp(&right.labels));
                values.truncate(limit);
                for sample in values {
                    series.entry(sample.labels).or_default().push(json!([
                        timestamp as f64 / 1_000_000_000.0,
                        format_metric_value(sample.value)
                    ]));
                }
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let returned = series.values().map(Vec::len).sum();
    let stats = query_stats(
        total_bytes_processed,
        total_lines_processed,
        returned,
        elapsed,
    );
    if let Some((timestamp, value)) = scalar {
        return Ok(Json(json!({
            "status": "success",
            "data": {
                "resultType": "scalar",
                "result": [
                    timestamp as f64 / 1_000_000_000.0,
                    format_metric_value(value)
                ],
                "stats": stats
            }
        })));
    }
    let result_type = if mode == QueryMode::Instant {
        "vector"
    } else {
        "matrix"
    };
    let result = series
        .into_iter()
        .map(|(metric, mut values)| {
            if mode == QueryMode::Instant {
                json!({"metric": metric, "value": values.pop().unwrap_or(Value::Null)})
            } else {
                json!({"metric": metric, "values": values})
            }
        })
        .collect::<Vec<_>>();
    Ok(Json(success(result_type, result, stats)))
}

pub(super) fn metric_evaluation_times(
    params: &QueryParams,
    maximum_points: usize,
) -> Result<Vec<i64>, LokiApiError> {
    let (start, end) = query_range_bounds(params)?;
    let span = end.saturating_sub(start);
    let default_step = (span / 250).max(1_000_000_000);
    let step = match params.step.as_deref() {
        Some(value) => parse_positive_step(value)?,
        None => default_step,
    };
    let point_count = usize::try_from(span.div_euclid(step).saturating_add(1))
        .map_err(|_| LokiApiError::bad_request("metric query has too many evaluation points"))?;
    if point_count > maximum_points {
        return Err(LokiApiError::bad_request(format!(
            "metric query exceeds the {maximum_points}-point limit"
        )));
    }
    Ok((0..point_count)
        .map(|index| {
            start.saturating_add(step.saturating_mul(i64::try_from(index).unwrap_or(i64::MAX)))
        })
        .collect())
}

pub(super) fn parse_positive_step(value: &str) -> Result<i64, LokiApiError> {
    let step = parse_duration_nanos(value)
        .or_else(|| {
            value
                .parse::<f64>()
                .ok()
                .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
                .map(|seconds| (seconds * 1_000_000_000.0) as i64)
        })
        .filter(|step| *step > 0)
        .ok_or_else(|| LokiApiError::bad_request("step must be a positive duration"))?;
    Ok(step)
}

pub(super) fn parse_metric_expression(input: &str) -> Result<MetricExpression, LokiApiError> {
    let input = strip_outer_parentheses(input.trim());
    if let Some((index, operator)) = find_top_level_binary_operator(input) {
        let left = parse_metric_expression(&input[..index])?;
        let (bool_mode, matching, right) = parse_vector_matching(
            &input[index + operator.len()..],
            binary_operation(operator)?,
        )?;
        return Ok(MetricExpression::Binary {
            operation: binary_operation(operator)?,
            bool_mode,
            matching,
            left: Box::new(left),
            right: Box::new(parse_metric_expression(right)?),
        });
    }
    if let Some(expression) = parse_aggregate_expression(input)? {
        return Ok(expression);
    }
    if let Some(expression) = parse_range_expression(input)? {
        return Ok(expression);
    }
    if let Ok(value) = input.parse::<f64>()
        && value.is_finite()
    {
        return Ok(MetricExpression::Scalar(value));
    }
    Err(LokiApiError::bad_request(
        "unsupported or invalid metric LogQL expression",
    ))
}

pub(super) fn parse_aggregate_expression(
    input: &str,
) -> Result<Option<MetricExpression>, LokiApiError> {
    const OPERATIONS: [(&str, AggregateOperation); 13] = [
        ("sort_desc", AggregateOperation::SortDescending),
        ("bottomk", AggregateOperation::BottomK),
        ("stddev", AggregateOperation::Stddev),
        ("stdvar", AggregateOperation::Stdvar),
        ("topk", AggregateOperation::TopK),
        ("count", AggregateOperation::Count),
        ("average", AggregateOperation::Average),
        ("avg", AggregateOperation::Average),
        ("maximum", AggregateOperation::Maximum),
        ("max", AggregateOperation::Maximum),
        ("minimum", AggregateOperation::Minimum),
        ("min", AggregateOperation::Minimum),
        ("sum", AggregateOperation::Sum),
    ];
    for (name, operation) in OPERATIONS {
        let Some(mut rest) = input.strip_prefix(name) else {
            continue;
        };
        if rest
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            continue;
        }
        rest = rest.trim_start();
        let grouping = if let Some(after) = rest.strip_prefix("by") {
            let (labels, remaining) = parse_grouping_clause(after)?;
            rest = remaining;
            Some(MetricGrouping::By(labels))
        } else if let Some(after) = rest.strip_prefix("without") {
            let (labels, remaining) = parse_grouping_clause(after)?;
            rest = remaining;
            Some(MetricGrouping::Without(labels))
        } else {
            None
        };
        let (arguments, trailing) = extract_parenthesized(rest.trim_start())?;
        if !trailing.trim().is_empty() {
            return Err(LokiApiError::bad_request(
                "aggregation expression has trailing input",
            ));
        }
        let (parameter, expression) = if matches!(
            operation,
            AggregateOperation::TopK | AggregateOperation::BottomK
        ) {
            let (parameter, expression) = split_top_level_once(arguments, ',')
                .ok_or_else(|| LokiApiError::bad_request("topk/bottomk requires k and a vector"))?;
            let parameter = parameter
                .trim()
                .parse::<usize>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| LokiApiError::bad_request("topk/bottomk k must be positive"))?;
            (Some(parameter), expression)
        } else {
            (None, arguments)
        };
        return Ok(Some(MetricExpression::Aggregate {
            operation,
            grouping,
            parameter,
            expression: Box::new(parse_metric_expression(expression)?),
        }));
    }
    if let Some(arguments) = function_arguments(input, "sort")? {
        return Ok(Some(MetricExpression::Aggregate {
            operation: AggregateOperation::Sort,
            grouping: None,
            parameter: None,
            expression: Box::new(parse_metric_expression(arguments)?),
        }));
    }
    Ok(None)
}

pub(super) fn parse_range_expression(
    input: &str,
) -> Result<Option<MetricExpression>, LokiApiError> {
    const OPERATIONS: [(&str, RangeOperation); 13] = [
        ("count_over_time", RangeOperation::Count),
        ("bytes_over_time", RangeOperation::Bytes),
        ("bytes_rate", RangeOperation::BytesRate),
        ("absent_over_time", RangeOperation::Absent),
        ("sum_over_time", RangeOperation::Sum),
        ("avg_over_time", RangeOperation::Average),
        ("min_over_time", RangeOperation::Minimum),
        ("max_over_time", RangeOperation::Maximum),
        ("stddev_over_time", RangeOperation::Stddev),
        ("stdvar_over_time", RangeOperation::Stdvar),
        ("first_over_time", RangeOperation::First),
        ("last_over_time", RangeOperation::Last),
        ("rate_counter", RangeOperation::RateCounter),
    ];
    if let Some(arguments) = function_arguments(input, "rate")? {
        return parse_range_source(arguments, RangeOperation::Rate, None).map(Some);
    }
    if let Some(arguments) = function_arguments(input, "quantile_over_time")? {
        let (quantile, source) = split_top_level_once(arguments, ',').ok_or_else(|| {
            LokiApiError::bad_request("quantile_over_time requires q and a range")
        })?;
        let quantile = quantile
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .ok_or_else(|| LokiApiError::bad_request("quantile must be between zero and one"))?;
        return parse_range_source(source, RangeOperation::Quantile, Some(quantile)).map(Some);
    }
    for (name, operation) in OPERATIONS {
        if let Some(arguments) = function_arguments(input, name)? {
            return parse_range_source(arguments, operation, None).map(Some);
        }
    }
    Ok(None)
}

pub(super) fn parse_range_source(
    input: &str,
    operation: RangeOperation,
    parameter: Option<f64>,
) -> Result<MetricExpression, LokiApiError> {
    let input = input.trim();
    let open = find_range_open(input)
        .ok_or_else(|| LokiApiError::bad_request("range function requires [duration]"))?;
    if !input.ends_with(']') {
        return Err(LokiApiError::bad_request("range duration is unterminated"));
    }
    let window_nanos = parse_duration_nanos(input[open + 1..input.len() - 1].trim())
        .filter(|duration| *duration > 0)
        .ok_or_else(|| LokiApiError::bad_request("range duration must be positive"))?;
    let selector = parse_log_query(input[..open].trim())?;
    let unwrap = selector
        .stages
        .iter()
        .any(|stage| matches!(stage, PipelineStage::Unwrap(_)));
    if matches!(
        operation,
        RangeOperation::Sum
            | RangeOperation::Average
            | RangeOperation::Minimum
            | RangeOperation::Maximum
            | RangeOperation::Stddev
            | RangeOperation::Stdvar
            | RangeOperation::Quantile
            | RangeOperation::First
            | RangeOperation::Last
            | RangeOperation::RateCounter
    ) && !unwrap
    {
        return Err(LokiApiError::bad_request(
            "unwrapped range function requires an unwrap stage",
        ));
    }
    Ok(MetricExpression::Range {
        operation,
        selector,
        window_nanos,
        parameter,
    })
}

pub(super) fn evaluate_metric_expression(
    expression: &MetricExpression,
    entries: &[LokiEntry],
    timestamp: i64,
) -> Result<MetricValue, LokiApiError> {
    match expression {
        MetricExpression::Scalar(value) => Ok(MetricValue::Scalar(*value)),
        MetricExpression::Range {
            operation,
            selector,
            window_nanos,
            parameter,
        } => evaluate_range_metric(
            *operation,
            selector,
            *window_nanos,
            *parameter,
            entries,
            timestamp,
        ),
        MetricExpression::Aggregate {
            operation,
            grouping,
            parameter,
            expression,
        } => evaluate_aggregate(
            *operation,
            grouping.as_ref(),
            *parameter,
            evaluate_metric_expression(expression, entries, timestamp)?,
        ),
        MetricExpression::Binary {
            operation,
            bool_mode,
            matching,
            left,
            right,
        } => evaluate_binary(
            *operation,
            *bool_mode,
            matching,
            evaluate_metric_expression(left, entries, timestamp)?,
            evaluate_metric_expression(right, entries, timestamp)?,
        ),
    }
}

pub(super) fn evaluate_range_metric(
    operation: RangeOperation,
    selector: &LogSelector,
    window_nanos: i64,
    parameter: Option<f64>,
    entries: &[LokiEntry],
    timestamp: i64,
) -> Result<MetricValue, LokiApiError> {
    let start = timestamp.saturating_sub(window_nanos);
    let unwrap_label = selector.stages.iter().find_map(|stage| match stage {
        PipelineStage::Unwrap(label) => Some(label.as_str()),
        _ => None,
    });
    let mut groups = BTreeMap::<BTreeMap<String, String>, Vec<(i64, f64)>>::new();
    for entry in entries {
        if entry.timestamp_unix_nanos <= start || entry.timestamp_unix_nanos > timestamp {
            continue;
        }
        let Some(processed) = selector.process(entry.clone()) else {
            continue;
        };
        let value = match operation {
            RangeOperation::Count | RangeOperation::Rate | RangeOperation::Absent => 1.0,
            RangeOperation::Bytes | RangeOperation::BytesRate => processed.line.len() as f64,
            _ => {
                let label = unwrap_label.expect("validated unwrapped operation");
                let Some(raw) = processed
                    .labels
                    .get(label)
                    .or_else(|| processed.structured_metadata.get(label))
                else {
                    continue;
                };
                let Some(value) = parse_unwrapped_value(raw) else {
                    continue;
                };
                value
            }
        };
        groups
            .entry(processed.labels)
            .or_default()
            .push((processed.timestamp_unix_nanos, value));
    }
    if matches!(operation, RangeOperation::Absent) {
        return Ok(MetricValue::Vector(if groups.is_empty() {
            vec![MetricSample {
                labels: BTreeMap::new(),
                value: 1.0,
            }]
        } else {
            Vec::new()
        }));
    }
    let seconds = window_nanos as f64 / 1_000_000_000.0;
    let samples = groups
        .into_iter()
        .filter_map(|(labels, mut values)| {
            values.sort_unstable_by_key(|(timestamp, _)| *timestamp);
            let numeric = values.iter().map(|(_, value)| *value).collect::<Vec<_>>();
            let value = match operation {
                RangeOperation::Count => numeric.len() as f64,
                RangeOperation::Rate => numeric.len() as f64 / seconds,
                RangeOperation::Bytes => numeric.iter().sum(),
                RangeOperation::BytesRate => numeric.iter().sum::<f64>() / seconds,
                RangeOperation::Sum => numeric.iter().sum(),
                RangeOperation::Average => mean(&numeric)?,
                RangeOperation::Minimum => numeric.iter().copied().reduce(f64::min)?,
                RangeOperation::Maximum => numeric.iter().copied().reduce(f64::max)?,
                RangeOperation::Stddev => variance(&numeric)?.sqrt(),
                RangeOperation::Stdvar => variance(&numeric)?,
                RangeOperation::Quantile => quantile(&numeric, parameter.unwrap_or(0.5))?,
                RangeOperation::First => values.first()?.1,
                RangeOperation::Last => values.last()?.1,
                RangeOperation::RateCounter => counter_increase(&numeric) / seconds,
                RangeOperation::Absent => unreachable!(),
            };
            Some(MetricSample { labels, value })
        })
        .collect();
    Ok(MetricValue::Vector(samples))
}

pub(super) fn evaluate_aggregate(
    operation: AggregateOperation,
    grouping: Option<&MetricGrouping>,
    parameter: Option<usize>,
    value: MetricValue,
) -> Result<MetricValue, LokiApiError> {
    let MetricValue::Vector(samples) = value else {
        return Err(LokiApiError::bad_request(
            "vector aggregation cannot consume a scalar",
        ));
    };
    if matches!(
        operation,
        AggregateOperation::Sort | AggregateOperation::SortDescending
    ) {
        let mut samples = samples;
        samples.sort_by(|left, right| left.value.total_cmp(&right.value));
        if matches!(operation, AggregateOperation::SortDescending) {
            samples.reverse();
        }
        return Ok(MetricValue::Vector(samples));
    }
    if matches!(
        operation,
        AggregateOperation::TopK | AggregateOperation::BottomK
    ) {
        let mut groups = BTreeMap::<BTreeMap<String, String>, Vec<MetricSample>>::new();
        for sample in samples {
            groups
                .entry(group_metric_labels(&sample.labels, grouping))
                .or_default()
                .push(sample);
        }
        let mut output = Vec::new();
        for mut samples in groups.into_values() {
            samples.sort_by(|left, right| right.value.total_cmp(&left.value));
            if matches!(operation, AggregateOperation::BottomK) {
                samples.reverse();
            }
            samples.truncate(parameter.unwrap_or(1));
            output.extend(samples);
        }
        return Ok(MetricValue::Vector(output));
    }
    let mut groups = BTreeMap::<BTreeMap<String, String>, Vec<f64>>::new();
    for sample in samples {
        groups
            .entry(group_metric_labels(&sample.labels, grouping))
            .or_default()
            .push(sample.value);
    }
    Ok(MetricValue::Vector(
        groups
            .into_iter()
            .filter_map(|(labels, values)| {
                let value = match operation {
                    AggregateOperation::Sum => values.iter().sum(),
                    AggregateOperation::Average => mean(&values)?,
                    AggregateOperation::Minimum => values.iter().copied().reduce(f64::min)?,
                    AggregateOperation::Maximum => values.iter().copied().reduce(f64::max)?,
                    AggregateOperation::Count => values.len() as f64,
                    AggregateOperation::Stddev => variance(&values)?.sqrt(),
                    AggregateOperation::Stdvar => variance(&values)?,
                    AggregateOperation::TopK
                    | AggregateOperation::BottomK
                    | AggregateOperation::Sort
                    | AggregateOperation::SortDescending => unreachable!(),
                };
                Some(MetricSample { labels, value })
            })
            .collect(),
    ))
}

pub(super) fn evaluate_binary(
    operation: BinaryOperation,
    bool_mode: bool,
    matching: &VectorMatching,
    left: MetricValue,
    right: MetricValue,
) -> Result<MetricValue, LokiApiError> {
    match (left, right) {
        (MetricValue::Scalar(left), MetricValue::Scalar(right)) => Ok(MetricValue::Scalar(
            apply_binary_number(operation, bool_mode, left, right).unwrap_or(f64::NAN),
        )),
        (MetricValue::Vector(samples), MetricValue::Scalar(scalar)) => Ok(MetricValue::Vector(
            samples
                .into_iter()
                .filter_map(|sample| {
                    apply_binary_number(operation, bool_mode, sample.value, scalar).map(|value| {
                        MetricSample {
                            labels: sample.labels,
                            value,
                        }
                    })
                })
                .collect(),
        )),
        (MetricValue::Scalar(scalar), MetricValue::Vector(samples)) => Ok(MetricValue::Vector(
            samples
                .into_iter()
                .filter_map(|sample| {
                    apply_binary_number(operation, bool_mode, scalar, sample.value).map(|value| {
                        MetricSample {
                            labels: sample.labels,
                            value,
                        }
                    })
                })
                .collect(),
        )),
        (MetricValue::Vector(left), MetricValue::Vector(right)) => {
            evaluate_vector_binary(operation, bool_mode, matching, left, right)
        }
    }
}

pub(super) fn evaluate_vector_binary(
    operation: BinaryOperation,
    bool_mode: bool,
    matching: &VectorMatching,
    left: Vec<MetricSample>,
    right: Vec<MetricSample>,
) -> Result<MetricValue, LokiApiError> {
    if matches!(
        operation,
        BinaryOperation::And | BinaryOperation::Or | BinaryOperation::Unless
    ) {
        let left_keys = left
            .iter()
            .map(|sample| metric_match_key(&sample.labels, matching))
            .collect::<BTreeSet<_>>();
        let right_keys = right
            .iter()
            .map(|sample| metric_match_key(&sample.labels, matching))
            .collect::<BTreeSet<_>>();
        let mut output = match operation {
            BinaryOperation::And => left
                .into_iter()
                .filter(|sample| right_keys.contains(&metric_match_key(&sample.labels, matching)))
                .collect::<Vec<_>>(),
            BinaryOperation::Unless => left
                .into_iter()
                .filter(|sample| !right_keys.contains(&metric_match_key(&sample.labels, matching)))
                .collect::<Vec<_>>(),
            BinaryOperation::Or => left,
            _ => unreachable!(),
        };
        if operation == BinaryOperation::Or {
            output.extend(
                right.into_iter().filter(|sample| {
                    !left_keys.contains(&metric_match_key(&sample.labels, matching))
                }),
            );
        }
        return Ok(MetricValue::Vector(output));
    }
    if matching.cardinality == VectorCardinality::OneToMany {
        return evaluate_one_to_many_binary(operation, bool_mode, matching, left, right);
    }
    let mut right_by_key = BTreeMap::<BTreeMap<String, String>, MetricSample>::new();
    for sample in right {
        let key = metric_match_key(&sample.labels, matching);
        if right_by_key.insert(key, sample).is_some() {
            return Err(LokiApiError::bad_request(
                "many-to-many vector matching is not allowed",
            ));
        }
    }
    let mut output = Vec::new();
    let mut left_keys = BTreeSet::new();
    for sample in left {
        let key = metric_match_key(&sample.labels, matching);
        if matching.cardinality == VectorCardinality::OneToOne && !left_keys.insert(key.clone()) {
            return Err(LokiApiError::bad_request(
                "many-to-many vector matching is not allowed",
            ));
        }
        let other = right_by_key.get(&key);
        if let Some(other) = other
            && let Some(value) =
                apply_binary_number(operation, bool_mode, sample.value, other.value)
        {
            let mut labels = sample.labels;
            for name in &matching.include {
                if let Some(included) = other.labels.get(name) {
                    labels.insert(name.clone(), included.clone());
                }
            }
            output.push(MetricSample { labels, value });
        }
    }
    Ok(MetricValue::Vector(output))
}

pub(super) fn evaluate_one_to_many_binary(
    operation: BinaryOperation,
    bool_mode: bool,
    matching: &VectorMatching,
    left: Vec<MetricSample>,
    right: Vec<MetricSample>,
) -> Result<MetricValue, LokiApiError> {
    if matches!(
        operation,
        BinaryOperation::And | BinaryOperation::Or | BinaryOperation::Unless
    ) {
        return Err(LokiApiError::bad_request(
            "group_left/group_right cannot be used with set operators",
        ));
    }
    let mut left_by_key = BTreeMap::<BTreeMap<String, String>, MetricSample>::new();
    for sample in left {
        let key = metric_match_key(&sample.labels, matching);
        if left_by_key.insert(key, sample).is_some() {
            return Err(LokiApiError::bad_request(
                "many-to-many vector matching is not allowed",
            ));
        }
    }
    let mut output = Vec::new();
    for sample in right {
        let key = metric_match_key(&sample.labels, matching);
        let Some(other) = left_by_key.get(&key) else {
            continue;
        };
        let Some(value) = apply_binary_number(operation, bool_mode, other.value, sample.value)
        else {
            continue;
        };
        let mut labels = sample.labels;
        for name in &matching.include {
            if let Some(included) = other.labels.get(name) {
                labels.insert(name.clone(), included.clone());
            }
        }
        output.push(MetricSample { labels, value });
    }
    Ok(MetricValue::Vector(output))
}

pub(super) fn apply_binary_number(
    operation: BinaryOperation,
    bool_mode: bool,
    left: f64,
    right: f64,
) -> Option<f64> {
    let comparison = match operation {
        BinaryOperation::Equal => Some(left == right),
        BinaryOperation::NotEqual => Some(left != right),
        BinaryOperation::Greater => Some(left > right),
        BinaryOperation::GreaterEqual => Some(left >= right),
        BinaryOperation::Less => Some(left < right),
        BinaryOperation::LessEqual => Some(left <= right),
        _ => None,
    };
    if let Some(matches) = comparison {
        return if bool_mode {
            Some(f64::from(u8::from(matches)))
        } else {
            matches.then_some(left)
        };
    }
    match operation {
        BinaryOperation::Add => Some(left + right),
        BinaryOperation::Subtract => Some(left - right),
        BinaryOperation::Multiply => Some(left * right),
        BinaryOperation::Divide => Some(left / right),
        BinaryOperation::Modulo => Some(left % right),
        BinaryOperation::Power => Some(left.powf(right)),
        BinaryOperation::And | BinaryOperation::Or | BinaryOperation::Unless => None,
        _ => unreachable!(),
    }
}

pub(super) fn group_metric_labels(
    labels: &BTreeMap<String, String>,
    grouping: Option<&MetricGrouping>,
) -> BTreeMap<String, String> {
    match grouping {
        Some(MetricGrouping::By(names)) => labels
            .iter()
            .filter(|(name, _)| names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        Some(MetricGrouping::Without(names)) => labels
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        None => BTreeMap::new(),
    }
}

pub(super) fn metric_match_key(
    labels: &BTreeMap<String, String>,
    matching: &VectorMatching,
) -> BTreeMap<String, String> {
    if let Some(on) = &matching.on {
        labels
            .iter()
            .filter(|(name, _)| on.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    } else {
        labels
            .iter()
            .filter(|(name, _)| !matching.ignoring.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }
}

pub(super) fn parse_unwrapped_value(value: &str) -> Option<f64> {
    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .or_else(|| parse_duration_nanos(value).map(|value| value as f64 / 1_000_000_000.0))
        .or_else(|| parse_byte_quantity(value).map(|value| value as f64))
}

pub(super) fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

pub(super) fn variance(values: &[f64]) -> Option<f64> {
    let mean = mean(values)?;
    Some(
        values
            .iter()
            .map(|value| {
                let deviation = value - mean;
                deviation * deviation
            })
            .sum::<f64>()
            / values.len() as f64,
    )
}

pub(super) fn quantile(values: &[f64], quantile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    let rank = quantile * (values.len().saturating_sub(1)) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let fraction = rank - lower as f64;
    Some(values[lower] + (values[upper] - values[lower]) * fraction)
}

pub(super) fn counter_increase(values: &[f64]) -> f64 {
    values.windows(2).fold(0.0, |increase, pair| {
        if pair[1] >= pair[0] {
            increase + pair[1] - pair[0]
        } else {
            increase + pair[1].max(0.0)
        }
    })
}

pub(super) fn format_metric_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "+Inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_owned()
    } else {
        value.to_string()
    }
}

pub(super) fn parse_grouping_clause(input: &str) -> Result<(Vec<String>, &str), LokiApiError> {
    let (labels, remaining) = extract_parenthesized(input.trim_start())?;
    let labels = labels
        .split(',')
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    labels
        .iter()
        .try_for_each(|label| validate_label_name(label))?;
    Ok((labels, remaining))
}

pub(super) fn function_arguments<'a>(
    input: &'a str,
    name: &str,
) -> Result<Option<&'a str>, LokiApiError> {
    let Some(rest) = input.strip_prefix(name) else {
        return Ok(None);
    };
    if rest
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Ok(None);
    }
    let (arguments, trailing) = extract_parenthesized(rest.trim_start())?;
    if !trailing.trim().is_empty() {
        return Err(LokiApiError::bad_request(format!(
            "{name} has trailing input"
        )));
    }
    Ok(Some(arguments))
}

pub(super) fn extract_parenthesized(input: &str) -> Result<(&str, &str), LokiApiError> {
    if !input.starts_with('(') {
        return Err(LokiApiError::bad_request(
            "expected parenthesized expression",
        ));
    }
    let mut quoted = false;
    let mut escaped = false;
    let mut depth = 0usize;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if !quoted && character == '(' {
            depth += 1;
        } else if !quoted && character == ')' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Ok((&input[1..index], &input[index + 1..]));
            }
        }
    }
    Err(LokiApiError::bad_request(
        "unterminated parenthesized expression",
    ))
}

pub(super) fn strip_outer_parentheses(mut input: &str) -> &str {
    loop {
        let Ok((inner, trailing)) = extract_parenthesized(input) else {
            return input;
        };
        if !trailing.trim().is_empty() {
            return input;
        }
        input = inner.trim();
    }
}

pub(super) fn split_top_level_once(input: &str, separator: char) -> Option<(&str, &str)> {
    let mut quoted = false;
    let mut escaped = false;
    let mut parentheses = 0usize;
    let mut braces = 0usize;
    let mut brackets = 0usize;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
            continue;
        }
        if character == '"' {
            quoted = !quoted;
            continue;
        }
        if quoted {
            continue;
        }
        match character {
            '(' => parentheses += 1,
            ')' => parentheses = parentheses.saturating_sub(1),
            '{' => braces += 1,
            '}' => braces = braces.saturating_sub(1),
            '[' => brackets += 1,
            ']' => brackets = brackets.saturating_sub(1),
            _ if character == separator && parentheses == 0 && braces == 0 && brackets == 0 => {
                return Some((&input[..index], &input[index + character.len_utf8()..]));
            }
            _ => {}
        }
    }
    None
}

pub(super) fn find_range_open(input: &str) -> Option<usize> {
    let mut quoted = false;
    let mut escaped = false;
    let mut candidate = None;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if character == '[' && !quoted {
            candidate = Some(index);
        }
    }
    candidate
}

pub(super) fn find_top_level_binary_operator(input: &str) -> Option<(usize, &'static str)> {
    const PRECEDENCE: [&[&str]; 6] = [
        &[" or ", " unless ", " and "],
        &["==", "!=", ">=", "<=", ">", "<"],
        &["+", "-"],
        &["*", "/", "%"],
        &["^"],
        &[],
    ];
    for operators in PRECEDENCE {
        let mut found = None;
        walk_top_level(input, |index| {
            for operator in operators {
                if input[index..].starts_with(operator)
                    && !(matches!(*operator, "+" | "-") && input[..index].trim().is_empty())
                {
                    found = Some((index, *operator));
                    break;
                }
            }
        });
        if found.is_some() {
            return found;
        }
    }
    None
}

pub(super) fn walk_top_level(mut input: &str, mut visit: impl FnMut(usize)) {
    let original_length = input.len();
    let mut quoted = false;
    let mut escaped = false;
    let mut parentheses = 0usize;
    let mut braces = 0usize;
    let mut brackets = 0usize;
    while !input.is_empty() {
        let index = original_length - input.len();
        let character = input.chars().next().expect("nonempty input");
        if escaped {
            escaped = false;
        } else if character == '\\' && quoted {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if !quoted {
            match character {
                '(' => parentheses += 1,
                ')' => parentheses = parentheses.saturating_sub(1),
                '{' => braces += 1,
                '}' => braces = braces.saturating_sub(1),
                '[' => brackets += 1,
                ']' => brackets = brackets.saturating_sub(1),
                _ => {}
            }
            if parentheses == 0 && braces == 0 && brackets == 0 {
                visit(index);
            }
        }
        input = &input[character.len_utf8()..];
    }
}

pub(super) fn binary_operation(operator: &str) -> Result<BinaryOperation, LokiApiError> {
    match operator.trim() {
        "+" => Ok(BinaryOperation::Add),
        "-" => Ok(BinaryOperation::Subtract),
        "*" => Ok(BinaryOperation::Multiply),
        "/" => Ok(BinaryOperation::Divide),
        "%" => Ok(BinaryOperation::Modulo),
        "^" => Ok(BinaryOperation::Power),
        "==" => Ok(BinaryOperation::Equal),
        "!=" => Ok(BinaryOperation::NotEqual),
        ">" => Ok(BinaryOperation::Greater),
        ">=" => Ok(BinaryOperation::GreaterEqual),
        "<" => Ok(BinaryOperation::Less),
        "<=" => Ok(BinaryOperation::LessEqual),
        "and" => Ok(BinaryOperation::And),
        "or" => Ok(BinaryOperation::Or),
        "unless" => Ok(BinaryOperation::Unless),
        _ => Err(LokiApiError::bad_request("invalid binary operator")),
    }
}

pub(super) fn parse_vector_matching(
    input: &str,
    operation: BinaryOperation,
) -> Result<(bool, VectorMatching, &str), LokiApiError> {
    let mut input = input.trim_start();
    let mut bool_mode = false;
    let mut matching = VectorMatching::default();
    loop {
        if let Some(rest) = input.strip_prefix("bool")
            && rest.chars().next().is_none_or(char::is_whitespace)
        {
            if !matches!(
                operation,
                BinaryOperation::Equal
                    | BinaryOperation::NotEqual
                    | BinaryOperation::Greater
                    | BinaryOperation::GreaterEqual
                    | BinaryOperation::Less
                    | BinaryOperation::LessEqual
            ) {
                return Err(LokiApiError::bad_request(
                    "bool is valid only for comparison operators",
                ));
            }
            bool_mode = true;
            input = rest.trim_start();
            continue;
        }
        if let Some(rest) = input.strip_prefix("on") {
            let (labels, trailing) = parse_grouping_clause(rest)?;
            matching.on = Some(labels);
            input = trailing.trim_start();
            continue;
        }
        if let Some(rest) = input.strip_prefix("ignoring") {
            let (labels, trailing) = parse_grouping_clause(rest)?;
            matching.ignoring = labels;
            input = trailing.trim_start();
            continue;
        }
        if let Some(rest) = input.strip_prefix("group_left") {
            matching.cardinality = VectorCardinality::ManyToOne;
            let (include, rest) = parse_optional_grouping_clause(rest)?;
            matching.include = include;
            input = rest.trim_start();
            continue;
        }
        if let Some(rest) = input.strip_prefix("group_right") {
            matching.cardinality = VectorCardinality::OneToMany;
            let (include, rest) = parse_optional_grouping_clause(rest)?;
            matching.include = include;
            input = rest.trim_start();
            continue;
        }
        break;
    }
    Ok((bool_mode, matching, input))
}

pub(super) fn parse_optional_grouping_clause(
    input: &str,
) -> Result<(Vec<String>, &str), LokiApiError> {
    let input = input.trim_start();
    if input.starts_with('(') {
        parse_grouping_clause(input)
    } else {
        Ok((Vec::new(), input))
    }
}

pub(super) fn normalize_stream_labels(
    mut labels: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    if !labels.contains_key("service_name") {
        const SERVICE_CANDIDATES: [&str; 11] = [
            "service",
            "app",
            "application",
            "name",
            "app_kubernetes_io_name",
            "container",
            "container_name",
            "component",
            "workload",
            "job",
            "service.name",
        ];
        if let Some(service_name) = SERVICE_CANDIDATES
            .into_iter()
            .find_map(|name| labels.get(name).cloned())
        {
            labels.insert("service_name".to_owned(), service_name);
        }
    }
    labels
}

pub(super) fn detected_level(labels: &BTreeMap<String, String>, line: &str) -> String {
    const LEVEL_LABELS: [&str; 5] = ["level", "severity", "severity_text", "lvl", "log_level"];
    if let Some(level) = LEVEL_LABELS.into_iter().find_map(|name| labels.get(name)) {
        return level.to_ascii_lowercase();
    }
    let lowercase = line.to_ascii_lowercase();
    ["trace", "debug", "info", "warn", "error", "fatal"]
        .into_iter()
        .find(|level| {
            lowercase
                .split(|character: char| !character.is_ascii_alphanumeric())
                .any(|term| term == *level)
        })
        .unwrap_or("unknown")
        .to_owned()
}
