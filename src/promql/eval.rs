use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct EvalContext {
    pub(super) eval_ms: i64,
    pub(super) start_ms: i64,
    pub(super) end_ms: i64,
    pub(super) lookback: Duration,
}

pub(super) fn selector_name(selector: &VectorSelector) -> Option<&str> {
    selector.name.as_deref().or_else(|| {
        selector
            .matchers
            .matchers
            .iter()
            .find(|matcher| matcher.name == "__name__" && matches!(matcher.op, MatchOp::Equal))
            .map(|matcher| matcher.value.as_str())
    })
}

pub(super) fn selector_matches(
    selector: &VectorSelector,
    labels: &BTreeMap<String, String>,
) -> bool {
    let base = matcher_group_matches(&selector.matchers.matchers, labels);
    if selector.matchers.or_matchers.is_empty() {
        base
    } else {
        base && selector
            .matchers
            .or_matchers
            .iter()
            .any(|group| matcher_group_matches(group, labels))
    }
}

pub(super) fn matcher_group_matches(
    matchers: &[Matcher],
    labels: &BTreeMap<String, String>,
) -> bool {
    matchers
        .iter()
        .all(|matcher| matcher.is_match(labels.get(&matcher.name).map_or("", String::as_str)))
}

pub(super) fn selector_time(
    selector: &VectorSelector,
    context: &EvalContext,
) -> Result<i64, PromqlError> {
    let at = match selector.at.as_ref() {
        None => context.eval_ms,
        Some(AtModifier::Start) => context.start_ms,
        Some(AtModifier::End) => context.end_ms,
        Some(AtModifier::At(time)) => system_time_millis(*time)?,
    };
    let offset = match selector.offset.as_ref() {
        None => 0,
        Some(Offset::Pos(duration)) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Some(Offset::Neg(duration)) => -i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
    };
    at.checked_sub(offset)
        .ok_or_else(|| PromqlError::new("PromQL selector timestamp overflow"))
}

pub(super) fn subquery_time(
    subquery: &SubqueryExpr,
    context: &EvalContext,
) -> Result<i64, PromqlError> {
    let at = match subquery.at.as_ref() {
        None => context.eval_ms,
        Some(AtModifier::Start) => context.start_ms,
        Some(AtModifier::End) => context.end_ms,
        Some(AtModifier::At(time)) => system_time_millis(*time)?,
    };
    let offset = match subquery.offset.as_ref() {
        None => 0,
        Some(Offset::Pos(duration)) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Some(Offset::Neg(duration)) => -i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
    };
    at.checked_sub(offset)
        .ok_or_else(|| PromqlError::new("PromQL subquery timestamp overflow"))
}

pub(super) fn system_time_millis(time: SystemTime) -> Result<i64, PromqlError> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis())
            .map_err(|_| PromqlError::new("PromQL @ timestamp exceeds i64")),
        Err(error) => i64::try_from(error.duration().as_millis())
            .map(|value| -value)
            .map_err(|_| PromqlError::new("PromQL @ timestamp precedes i64")),
    }
}

pub(super) fn point_labels(point: &DurableMetricPoint) -> BTreeMap<String, String> {
    let mut labels = prometheus_string_labels(&point.identity)
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect::<BTreeMap<_, _>>();
    labels.insert("__name__".into(), point.identity.name.to_string());
    labels
}

pub(super) fn point_float(point: &DurableMetricPoint) -> Option<f64> {
    let value = match point.value {
        MetricValue::Gauge(value) | MetricValue::Sum(value) => value,
        MetricValue::ExplicitHistogram(_)
        | MetricValue::ExponentialHistogram(_)
        | MetricValue::Summary(_) => return None,
    };
    Some(match value {
        NumberValue::Integer(value) => value as f64,
        NumberValue::DoubleBits(bits) => f64::from_bits(bits),
    })
}

pub(super) fn grouped_labels(
    mut labels: BTreeMap<String, String>,
    modifier: Option<&LabelModifier>,
) -> BTreeMap<String, String> {
    labels.remove("__name__");
    match modifier {
        None => BTreeMap::new(),
        Some(LabelModifier::Include(included)) => labels
            .into_iter()
            .filter(|(name, _)| included.labels.contains(name))
            .collect(),
        Some(LabelModifier::Exclude(excluded)) => labels
            .into_iter()
            .filter(|(name, _)| !excluded.labels.contains(name))
            .collect(),
    }
}

pub(super) fn match_key(
    labels: &BTreeMap<String, String>,
    binary: &BinaryExpr,
) -> Vec<(String, String)> {
    let mut labels = labels
        .iter()
        .filter(|(name, _)| name.as_str() != "__name__")
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    if let Some(modifier) = &binary.modifier
        && let Some(matching) = &modifier.matching
    {
        match matching {
            LabelModifier::Include(included) => {
                labels.retain(|(name, _)| included.labels.contains(name));
            }
            LabelModifier::Exclude(excluded) => {
                labels.retain(|(name, _)| !excluded.labels.contains(name));
            }
        }
    }
    labels
}

pub(super) fn binary_vectors(
    left: Vec<PromqlSample>,
    right: Vec<PromqlSample>,
    binary: &BinaryExpr,
    operation: &str,
) -> Result<Vec<PromqlSample>, PromqlError> {
    let mut left_by_key = HashMap::<Vec<(String, String)>, Vec<usize>>::new();
    let mut right_by_key = HashMap::<Vec<(String, String)>, Vec<usize>>::new();
    for (index, sample) in left.iter().enumerate() {
        left_by_key
            .entry(match_key(&sample.labels, binary))
            .or_default()
            .push(index);
    }
    for (index, sample) in right.iter().enumerate() {
        right_by_key
            .entry(match_key(&sample.labels, binary))
            .or_default()
            .push(index);
    }

    if binary.op.is_set_operator() {
        return match operation {
            "and" => Ok(left
                .into_iter()
                .filter(|sample| right_by_key.contains_key(&match_key(&sample.labels, binary)))
                .collect()),
            "unless" => Ok(left
                .into_iter()
                .filter(|sample| !right_by_key.contains_key(&match_key(&sample.labels, binary)))
                .collect()),
            "or" => {
                let mut output = left;
                output.extend(right.into_iter().filter(|sample| {
                    !left_by_key.contains_key(&match_key(&sample.labels, binary))
                }));
                Ok(output)
            }
            _ => Err(PromqlError::new(format!(
                "unsupported PromQL set operator {operation}"
            ))),
        };
    }

    let cardinality = binary
        .modifier
        .as_ref()
        .map_or(VectorMatchCardinality::OneToOne, |modifier| {
            modifier.card.clone()
        });
    match &cardinality {
        VectorMatchCardinality::OneToOne => {
            reject_duplicate_match_groups(&left_by_key, "left")?;
            reject_duplicate_match_groups(&right_by_key, "right")?;
            let mut output = Vec::new();
            for mut sample in left {
                let key = match_key(&sample.labels, binary);
                let Some(right_index) = right_by_key.get(&key).and_then(|group| group.first())
                else {
                    continue;
                };
                if let Some(value) = binary_float(
                    operation,
                    sample.value,
                    right[*right_index].value,
                    binary.return_bool(),
                )? {
                    sample.value = value;
                    normalize_binary_labels(&mut sample.labels, binary, None);
                    output.push(sample);
                }
            }
            Ok(output)
        }
        VectorMatchCardinality::ManyToOne(included) => {
            reject_duplicate_match_groups(&right_by_key, "right")?;
            let mut output = Vec::new();
            for mut sample in left {
                let key = match_key(&sample.labels, binary);
                let Some(right_index) = right_by_key.get(&key).and_then(|group| group.first())
                else {
                    continue;
                };
                let one = &right[*right_index];
                if let Some(value) =
                    binary_float(operation, sample.value, one.value, binary.return_bool())?
                {
                    sample.value = value;
                    normalize_binary_labels(
                        &mut sample.labels,
                        binary,
                        Some((&one.labels, included)),
                    );
                    output.push(sample);
                }
            }
            Ok(output)
        }
        VectorMatchCardinality::OneToMany(included) => {
            reject_duplicate_match_groups(&left_by_key, "left")?;
            let mut output = Vec::new();
            for mut sample in right {
                let key = match_key(&sample.labels, binary);
                let Some(left_index) = left_by_key.get(&key).and_then(|group| group.first()) else {
                    continue;
                };
                let one = &left[*left_index];
                if let Some(value) =
                    binary_float(operation, one.value, sample.value, binary.return_bool())?
                {
                    sample.value = value;
                    normalize_binary_labels(
                        &mut sample.labels,
                        binary,
                        Some((&one.labels, included)),
                    );
                    output.push(sample);
                }
            }
            Ok(output)
        }
        VectorMatchCardinality::ManyToMany => Err(PromqlError::new(
            "many-to-many matching is only valid for PromQL set operators",
        )),
    }
}

pub(super) fn reject_duplicate_match_groups(
    groups: &HashMap<Vec<(String, String)>, Vec<usize>>,
    side: &str,
) -> Result<(), PromqlError> {
    if groups.values().any(|group| group.len() > 1) {
        Err(PromqlError::new(format!(
            "many-to-many matching: duplicate series on the {side} side"
        )))
    } else {
        Ok(())
    }
}

pub(super) fn normalize_binary_labels(
    labels: &mut BTreeMap<String, String>,
    binary: &BinaryExpr,
    include_from_one: Option<(&BTreeMap<String, String>, &promql_parser::label::Labels)>,
) {
    if !binary.op.is_comparison_operator() || binary.return_bool() || binary.is_matching_on() {
        labels.remove("__name__");
    }
    if let Some((one, included)) = include_from_one {
        for name in &included.labels {
            if let Some(value) = one.get(name) {
                labels.insert(name.clone(), value.clone());
            } else {
                labels.remove(name);
            }
        }
    }
}

pub(super) fn binary_vector_scalar(
    samples: Vec<PromqlSample>,
    scalar: f64,
    operation: &str,
    scalar_left: bool,
    return_bool: bool,
) -> Result<Vec<PromqlSample>, PromqlError> {
    let mut output = Vec::new();
    for mut sample in samples {
        let (left, right) = if scalar_left {
            (scalar, sample.value)
        } else {
            (sample.value, scalar)
        };
        if let Some(value) = binary_float(operation, left, right, return_bool)? {
            sample.value = value;
            output.push(sample);
        }
    }
    Ok(output)
}

pub(super) fn binary_float(
    operation: &str,
    left: f64,
    right: f64,
    return_bool: bool,
) -> Result<Option<f64>, PromqlError> {
    let comparison = match operation {
        "==" => Some(left == right),
        "!=" => Some(left != right),
        ">" => Some(left > right),
        ">=" => Some(left >= right),
        "<" => Some(left < right),
        "<=" => Some(left <= right),
        _ => None,
    };
    if let Some(matches) = comparison {
        return Ok(if return_bool {
            Some(f64::from(matches))
        } else if matches {
            Some(left)
        } else {
            None
        });
    }
    Ok(Some(match operation {
        "+" => left + right,
        "-" => left - right,
        "*" => left * right,
        "/" => left / right,
        "%" => left % right,
        "^" => left.powf(right),
        "atan2" => left.atan2(right),
        _ => {
            return Err(PromqlError::new(format!(
                "unsupported binary operator {operation}"
            )));
        }
    }))
}

pub(super) fn negate(value: PromqlValue) -> Result<PromqlValue, PromqlError> {
    map_vector(value, 0, |value| -value)
}

pub(super) fn map_vector(
    value: PromqlValue,
    timestamp_ms: i64,
    function: impl Fn(f64) -> f64,
) -> Result<PromqlValue, PromqlError> {
    match value {
        PromqlValue::Scalar {
            timestamp_ms: observed,
            value,
        } => Ok(PromqlValue::Scalar {
            timestamp_ms: observed,
            value: function(value),
        }),
        PromqlValue::Vector(mut samples) => {
            for sample in &mut samples {
                sample.timestamp_ms = if timestamp_ms == 0 {
                    sample.timestamp_ms
                } else {
                    timestamp_ms
                };
                sample.value = function(sample.value);
            }
            Ok(PromqlValue::Vector(samples))
        }
        _ => Err(PromqlError::new(
            "function requires a scalar or instant vector",
        )),
    }
}

pub(super) fn is_unary_math(name: &str) -> bool {
    matches!(
        name,
        "abs" | "ceil" | "floor" | "exp" | "ln" | "log2" | "log10" | "sqrt" | "sgn"
    )
}

pub(super) fn unary_math(name: &str, value: f64) -> f64 {
    match name {
        "abs" => value.abs(),
        "ceil" => value.ceil(),
        "floor" => value.floor(),
        "exp" => value.exp(),
        "ln" => value.ln(),
        "log2" => value.log2(),
        "log10" => value.log10(),
        "sqrt" => value.sqrt(),
        "sgn" => value.signum(),
        _ => unreachable!("name was checked by is_unary_math"),
    }
}

pub(super) fn call_arg(call: &Call, index: usize) -> Result<&Expr, PromqlError> {
    call.args.args.get(index).map(Box::as_ref).ok_or_else(|| {
        PromqlError::new(format!("{}() is missing argument {index}", call.func.name))
    })
}

pub(super) fn variance(values: &[f64]) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64
}

pub(super) fn delta(samples: &[(i64, f64)], instant: bool) -> f64 {
    let samples = if instant && samples.len() > 2 {
        &samples[samples.len() - 2..]
    } else {
        samples
    };
    samples
        .last()
        .zip(samples.first())
        .map_or(f64::NAN, |(last, first)| last.1 - first.1)
}

pub(super) fn counter_rate(samples: &[(i64, f64)], instant: bool) -> f64 {
    let samples = if instant && samples.len() > 2 {
        &samples[samples.len() - 2..]
    } else {
        samples
    };
    if samples.len() < 2 {
        return f64::NAN;
    }
    let mut increase = 0.0;
    for pair in samples.windows(2) {
        increase += if pair[1].1 < pair[0].1 {
            pair[1].1
        } else {
            pair[1].1 - pair[0].1
        };
    }
    let seconds = (samples.last().unwrap().0 - samples.first().unwrap().0) as f64 / 1_000.0;
    increase / seconds
}

pub(super) fn millis_to_nanos(milliseconds: i64) -> Result<u64, PromqlError> {
    u64::try_from(milliseconds)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .ok_or_else(|| PromqlError::new("PromQL timestamp is outside the storage range"))
}

pub(super) fn nanos_to_millis(nanoseconds: u64) -> Result<i64, PromqlError> {
    i64::try_from(nanoseconds / 1_000_000)
        .map_err(|_| PromqlError::new("metric timestamp exceeds PromQL range"))
}
