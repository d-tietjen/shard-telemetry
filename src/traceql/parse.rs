use super::*;

#[derive(Debug, Clone)]
pub(super) struct TraceqlQuery {
    spanset: SpansetExpr,
    pipeline: Vec<PipelineStage>,
}

impl TraceqlQuery {
    pub(super) fn parse(input: &str) -> Result<Self, TraceqlError> {
        let parts = split_traceql_pipeline(input);
        let Some((spanset, pipeline)) = parts.split_first() else {
            return Ok(Self {
                spanset: SpansetExpr::Selector(TraceFilter::True),
                pipeline: Vec::new(),
            });
        };
        Ok(Self {
            spanset: SpansetExpr::parse(spanset)?,
            pipeline: pipeline
                .iter()
                .map(|stage| PipelineStage::parse(stage))
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    pub(super) fn evaluate(&self, spans: &[DurableSpan]) -> Vec<usize> {
        let selected = self.spanset.evaluate(spans);
        if selected.is_empty() {
            return selected;
        }
        let mut groups = vec![selected];
        for stage in &self.pipeline {
            groups = stage.apply(groups, spans);
            if groups.is_empty() {
                break;
            }
        }
        sorted_unique(groups.into_iter().flatten().collect())
    }

    pub(super) fn exact_trace_id(&self) -> Option<TraceId> {
        self.spanset.exact_trace_id()
    }

    pub(super) fn selected_fields(&self) -> Arc<Vec<String>> {
        Arc::new(
            self.pipeline
                .iter()
                .rev()
                .find_map(|stage| match stage {
                    PipelineStage::Select(fields) => Some(fields.clone()),
                    PipelineStage::By(_) | PipelineStage::Aggregate { .. } => None,
                })
                .unwrap_or_default(),
        )
    }
}

#[derive(Debug, Clone)]
pub(super) enum PipelineStage {
    By(String),
    Select(Vec<String>),
    Aggregate {
        operation: TraceAggregate,
        field: Option<String>,
        comparison: Comparison,
        expected: Literal,
    },
}

impl PipelineStage {
    pub(super) fn parse(input: &str) -> Result<Self, TraceqlError> {
        let input = input.trim();
        if let Some(field) = input
            .strip_prefix("by(")
            .and_then(|value| value.strip_suffix(')'))
        {
            let field = field.trim().trim_start_matches('.');
            if field.is_empty() {
                return Err(TraceqlError::new("TraceQL by() has an empty field"));
            }
            return Ok(Self::By(field.to_owned()));
        }
        if let Some(fields) = input
            .strip_prefix("select(")
            .and_then(|value| value.strip_suffix(')'))
        {
            let fields = split_quoted(fields, ",")
                .into_iter()
                .map(|field| field.trim().trim_start_matches('.').to_owned())
                .filter(|field| !field.is_empty())
                .collect::<Vec<_>>();
            if fields.is_empty() {
                return Err(TraceqlError::new("TraceQL select() has no fields"));
            }
            return Ok(Self::Select(fields));
        }
        for (token, comparison) in comparison_tokens() {
            if let Some(index) = find_unquoted(input, token) {
                let aggregate = input[..index].trim();
                let open = aggregate
                    .find('(')
                    .ok_or_else(|| TraceqlError::new("TraceQL aggregate is missing '('"))?;
                let field = aggregate[open + 1..]
                    .strip_suffix(')')
                    .ok_or_else(|| TraceqlError::new("TraceQL aggregate is missing ')'"))?
                    .trim()
                    .trim_start_matches('.');
                let operation = TraceAggregate::parse(aggregate[..open].trim())?;
                if operation != TraceAggregate::Count && field.is_empty() {
                    return Err(TraceqlError::new(
                        "TraceQL numeric aggregate requires a field",
                    ));
                }
                if operation == TraceAggregate::Count && !field.is_empty() {
                    return Err(TraceqlError::new("TraceQL count() takes no field"));
                }
                return Ok(Self::Aggregate {
                    operation,
                    field: (!field.is_empty()).then(|| field.to_owned()),
                    comparison,
                    expected: Literal::parse(input[index + token.len()..].trim())?,
                });
            }
        }
        Err(TraceqlError::new(format!(
            "unsupported TraceQL pipeline stage {input:?}"
        )))
    }

    pub(super) fn apply(&self, groups: Vec<Vec<usize>>, spans: &[DurableSpan]) -> Vec<Vec<usize>> {
        match self {
            Self::By(field) => groups
                .into_iter()
                .flat_map(|group| group_by_field(group, spans, field))
                .collect(),
            Self::Select(fields) => {
                let _ = fields;
                groups
            }
            Self::Aggregate {
                operation,
                field,
                comparison,
                expected,
            } => groups
                .into_iter()
                .filter(|group| {
                    aggregate_group(*operation, field.as_deref(), group, spans).is_some_and(
                        |value| compare(&[ObservedValue::Float(value)], expected, *comparison),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TraceAggregate {
    Count,
    Sum,
    Average,
    Minimum,
    Maximum,
}

impl TraceAggregate {
    pub(super) fn parse(input: &str) -> Result<Self, TraceqlError> {
        match input {
            "count" => Ok(Self::Count),
            "sum" => Ok(Self::Sum),
            "avg" => Ok(Self::Average),
            "min" => Ok(Self::Minimum),
            "max" => Ok(Self::Maximum),
            _ => Err(TraceqlError::new(format!(
                "unsupported TraceQL aggregate {input:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct TraceMetricQuery {
    pub(super) spanset: TraceqlQuery,
    pub(super) function: TraceMetricFunction,
    pub(super) field: Option<String>,
    pub(super) quantile: Option<f64>,
    pub(super) group_by: Vec<String>,
    pub(super) threshold: Option<(Comparison, Literal)>,
    pub(super) series_limit: Option<(bool, usize)>,
}

impl TraceMetricQuery {
    pub(super) fn parse(input: &str) -> Result<Self, TraceqlError> {
        let parts = split_traceql_pipeline(input);
        let metric_index = parts
            .iter()
            .position(|part| TraceMetricFunction::recognizes(part))
            .ok_or_else(|| TraceqlError::new("TraceQL metrics query has no metrics function"))?;
        if metric_index == 0 {
            return Err(TraceqlError::new(
                "TraceQL metrics query requires a spanset before the function",
            ));
        }
        let spanset = TraceqlQuery::parse(&parts[..metric_index].join(" | "))?;
        let (function, field, quantile, group_by, threshold) =
            parse_trace_metric_stage(parts[metric_index])?;
        let mut series_limit = None;
        for stage in &parts[metric_index + 1..] {
            let stage = stage.trim();
            let (descending, value) = if let Some(value) = stage
                .strip_prefix("topk(")
                .and_then(|value| value.strip_suffix(')'))
            {
                (true, value)
            } else if let Some(value) = stage
                .strip_prefix("bottomk(")
                .and_then(|value| value.strip_suffix(')'))
            {
                (false, value)
            } else {
                return Err(TraceqlError::new(format!(
                    "unsupported TraceQL metrics pipeline stage {stage:?}"
                )));
            };
            let limit = value
                .trim()
                .parse::<usize>()
                .ok()
                .filter(|limit| *limit > 0)
                .ok_or_else(|| TraceqlError::new("TraceQL topk/bottomk requires positive k"))?;
            if series_limit.replace((descending, limit)).is_some() {
                return Err(TraceqlError::new(
                    "TraceQL metrics query has multiple series limits",
                ));
            }
        }
        Ok(Self {
            spanset,
            function,
            field,
            quantile,
            group_by,
            threshold,
            series_limit,
        })
    }

    pub(super) fn labels(
        &self,
        span: &DurableSpan,
        trace: &[DurableSpan],
    ) -> Option<BTreeMap<String, String>> {
        let mut labels = BTreeMap::new();
        for field in &self.group_by {
            let value = field_values(span, trace, field).into_iter().next()?;
            labels.insert(field.clone(), observed_string(&value));
        }
        Some(labels)
    }

    pub(super) fn observed_value(&self, span: &DurableSpan, trace: &[DurableSpan]) -> Option<f64> {
        match self.function {
            TraceMetricFunction::Rate | TraceMetricFunction::Count => Some(1.0),
            TraceMetricFunction::Sum
            | TraceMetricFunction::Minimum
            | TraceMetricFunction::Maximum
            | TraceMetricFunction::Average
            | TraceMetricFunction::Quantile => self
                .field
                .as_deref()
                .and_then(|field| field_values(span, trace, field).into_iter().next())
                .and_then(|value| numeric_observed(&value)),
        }
    }

    pub(super) fn aggregate(&self, values: &[f64], denominator_seconds: f64) -> f64 {
        match self.function {
            TraceMetricFunction::Rate => values.len() as f64 / denominator_seconds,
            TraceMetricFunction::Count => values.len() as f64,
            TraceMetricFunction::Sum => values.iter().sum(),
            TraceMetricFunction::Minimum => {
                values.iter().copied().reduce(f64::min).unwrap_or(f64::NAN)
            }
            TraceMetricFunction::Maximum => {
                values.iter().copied().reduce(f64::max).unwrap_or(f64::NAN)
            }
            TraceMetricFunction::Average => values.iter().sum::<f64>() / values.len().max(1) as f64,
            TraceMetricFunction::Quantile => trace_quantile(values, self.quantile.unwrap_or(0.5)),
        }
    }

    pub(super) fn passes(&self, value: f64) -> bool {
        self.threshold
            .as_ref()
            .is_none_or(|(comparison, expected)| {
                compare(&[ObservedValue::Float(value)], expected, *comparison)
            })
    }

    pub(super) fn limit_series(
        &self,
        mut series: Vec<TraceqlMetricSeries>,
    ) -> Result<Vec<TraceqlMetricSeries>, TraceqlError> {
        if let Some((descending, limit)) = self.series_limit {
            series.sort_by(|left, right| {
                let left = left.samples.last().map_or(f64::NAN, |sample| sample.value);
                let right = right.samples.last().map_or(f64::NAN, |sample| sample.value);
                let order = left.total_cmp(&right);
                if descending { order.reverse() } else { order }
            });
            series.truncate(limit);
        }
        Ok(series)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TraceMetricFunction {
    Rate,
    Count,
    Sum,
    Minimum,
    Maximum,
    Average,
    Quantile,
}

impl TraceMetricFunction {
    pub(super) fn recognizes(input: &str) -> bool {
        let input = input.trim();
        [
            "rate(",
            "count_over_time(",
            "sum_over_time(",
            "min_over_time(",
            "max_over_time(",
            "avg_over_time(",
            "quantile_over_time(",
        ]
        .iter()
        .any(|prefix| input.starts_with(prefix))
    }

    pub(super) fn parse(input: &str) -> Result<Self, TraceqlError> {
        match input {
            "rate" => Ok(Self::Rate),
            "count_over_time" => Ok(Self::Count),
            "sum_over_time" => Ok(Self::Sum),
            "min_over_time" => Ok(Self::Minimum),
            "max_over_time" => Ok(Self::Maximum),
            "avg_over_time" => Ok(Self::Average),
            "quantile_over_time" => Ok(Self::Quantile),
            _ => Err(TraceqlError::new(format!(
                "unsupported TraceQL metrics function {input:?}"
            ))),
        }
    }
}

type ParsedTraceMetricStage = (
    TraceMetricFunction,
    Option<String>,
    Option<f64>,
    Vec<String>,
    Option<(Comparison, Literal)>,
);

pub(super) fn parse_trace_metric_stage(
    input: &str,
) -> Result<ParsedTraceMetricStage, TraceqlError> {
    let input = input.split(" with (").next().unwrap_or(input).trim();
    let open = input
        .find('(')
        .ok_or_else(|| TraceqlError::new("TraceQL metrics function is missing '('"))?;
    let close = find_closing_parenthesis_at(input, open)
        .ok_or_else(|| TraceqlError::new("TraceQL metrics function is missing ')'"))?;
    let function = TraceMetricFunction::parse(input[..open].trim())?;
    let arguments = split_quoted(&input[open + 1..close], ",");
    let (field, quantile) = match function {
        TraceMetricFunction::Rate | TraceMetricFunction::Count => {
            if arguments.len() != 1 || !arguments[0].is_empty() {
                return Err(TraceqlError::new(
                    "TraceQL rate/count_over_time takes no field",
                ));
            }
            (None, None)
        }
        TraceMetricFunction::Sum
        | TraceMetricFunction::Minimum
        | TraceMetricFunction::Maximum
        | TraceMetricFunction::Average => {
            if arguments.len() != 1 || arguments[0].is_empty() {
                return Err(TraceqlError::new(
                    "TraceQL metrics function requires one field",
                ));
            }
            (Some(arguments[0].trim_start_matches('.').to_owned()), None)
        }
        TraceMetricFunction::Quantile => {
            if arguments.len() != 2 || arguments[0].is_empty() {
                return Err(TraceqlError::new(
                    "TraceQL quantile_over_time requires field and quantile",
                ));
            }
            let quantile = arguments[1]
                .parse::<f64>()
                .ok()
                .filter(|value| (0.0..=1.0).contains(value))
                .ok_or_else(|| {
                    TraceqlError::new("TraceQL quantile must be between zero and one")
                })?;
            (
                Some(arguments[0].trim_start_matches('.').to_owned()),
                Some(quantile),
            )
        }
    };

    let mut suffix = input[close + 1..].trim();
    let mut group_by = Vec::new();
    if let Some(group) = suffix.strip_prefix("by") {
        let group = group.trim_start();
        let group_open = group
            .strip_prefix('(')
            .ok_or_else(|| TraceqlError::new("TraceQL metrics by is missing '('"))?;
        let group_close = group_open
            .find(')')
            .ok_or_else(|| TraceqlError::new("TraceQL metrics by is missing ')'"))?;
        group_by = split_quoted(&group_open[..group_close], ",")
            .into_iter()
            .map(|field| field.trim().trim_start_matches('.').to_owned())
            .filter(|field| !field.is_empty())
            .collect();
        suffix = group_open[group_close + 1..].trim();
    }
    let threshold = if suffix.is_empty() {
        None
    } else {
        comparison_tokens()
            .into_iter()
            .find_map(|(token, comparison)| {
                suffix.strip_prefix(token).map(|expected| {
                    Literal::parse(expected.trim()).map(|expected| (comparison, expected))
                })
            })
            .transpose()?
            .ok_or_else(|| TraceqlError::new("invalid TraceQL metrics comparison"))?
            .into()
    };
    Ok((function, field, quantile, group_by, threshold))
}

pub(super) fn find_closing_parenthesis_at(input: &str, open: usize) -> Option<usize> {
    let mut depth = 0_u32;
    for (index, character) in input.char_indices().skip_while(|(index, _)| *index < open) {
        match character {
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

pub(super) fn trace_quantile(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    let mut values = values.to_vec();
    values.sort_by(|left, right| left.total_cmp(right));
    if values.len() == 1 {
        return values[0];
    }
    let rank = quantile * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    values[lower] + (values[upper] - values[lower]) * (rank - lower as f64)
}
