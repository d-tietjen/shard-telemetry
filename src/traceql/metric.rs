use super::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct MetricGroup(pub(super) BTreeMap<String, String>);

#[derive(Debug, Default)]
pub(super) struct MetricBucket {
    pub(super) values: Vec<f64>,
    pub(super) exemplar: Option<(TraceId, crate::SpanId)>,
}

pub(super) fn group_by_field(
    group: Vec<usize>,
    spans: &[DurableSpan],
    field: &str,
) -> Vec<Vec<usize>> {
    let mut grouped = BTreeMap::<String, Vec<usize>>::new();
    for index in group {
        let values = field_values(&spans[index], spans, field);
        for value in values {
            grouped
                .entry(observed_string(&value))
                .or_default()
                .push(index);
        }
    }
    grouped
        .into_values()
        .map(sorted_unique)
        .filter(|group| !group.is_empty())
        .collect()
}

pub(super) fn aggregate_group(
    operation: TraceAggregate,
    field: Option<&str>,
    group: &[usize],
    spans: &[DurableSpan],
) -> Option<f64> {
    if operation == TraceAggregate::Count {
        return Some(group.len() as f64);
    }
    let field = field?;
    let values = group
        .iter()
        .flat_map(|index| field_values(&spans[*index], spans, field))
        .filter_map(|value| numeric_observed(&value))
        .collect::<Vec<_>>();
    match operation {
        TraceAggregate::Count => unreachable!("count returned before field collection"),
        TraceAggregate::Sum => Some(values.iter().sum()),
        TraceAggregate::Average => {
            (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
        }
        TraceAggregate::Minimum => values.into_iter().reduce(f64::min),
        TraceAggregate::Maximum => values.into_iter().reduce(f64::max),
    }
}

pub(super) fn split_traceql_pipeline(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    let mut braces = 0_u32;
    let mut parentheses = 0_u32;
    let bytes = input.as_bytes();
    for (index, byte) in bytes.iter().copied().enumerate() {
        if escaped {
            escaped = false;
        } else if quoted && byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if !quoted {
            match byte {
                b'{' => braces = braces.saturating_add(1),
                b'}' => braces = braces.saturating_sub(1),
                b'(' => parentheses = parentheses.saturating_add(1),
                b')' => parentheses = parentheses.saturating_sub(1),
                b'|' if braces == 0
                    && parentheses == 0
                    && bytes.get(index.wrapping_sub(1)) != Some(&b'|')
                    && bytes.get(index + 1) != Some(&b'|') =>
                {
                    parts.push(input[start..index].trim());
                    start = index + 1;
                }
                _ => {}
            }
        }
    }
    parts.push(input[start..].trim());
    parts
}

pub(super) fn comparison_tokens() -> [(&'static str, Comparison); 8] {
    [
        ("=~", Comparison::Regex),
        ("!~", Comparison::NotRegex),
        (">=", Comparison::GreaterOrEqual),
        ("<=", Comparison::LessOrEqual),
        ("!=", Comparison::NotEqual),
        ("=", Comparison::Equal),
        (">", Comparison::Greater),
        ("<", Comparison::Less),
    ]
}
