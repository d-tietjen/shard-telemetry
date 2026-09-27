use super::*;

#[derive(Debug, Clone)]
pub(super) enum TraceFilter {
    True,
    Condition(Condition),
    And(Vec<TraceFilter>),
    Or(Vec<TraceFilter>),
}

impl TraceFilter {
    pub(super) fn parse(input: &str) -> Result<Self, TraceqlError> {
        let input = input.trim();
        if input.is_empty() || input == "{}" {
            return Ok(Self::True);
        }
        let body = input
            .strip_prefix('{')
            .and_then(|value| value.strip_suffix('}'))
            .ok_or_else(|| TraceqlError::new("TraceQL filter must be enclosed in braces"))?
            .trim();
        if body.is_empty() {
            return Ok(Self::True);
        }
        let or_parts = split_quoted(body, "||");
        if or_parts.len() > 1 {
            return or_parts
                .into_iter()
                .map(Self::parse_body)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Or);
        }
        Self::parse_body(body)
    }

    pub(super) fn parse_body(body: &str) -> Result<Self, TraceqlError> {
        let parts = split_quoted(body, "&&");
        if parts.len() > 1 {
            return parts
                .into_iter()
                .map(|part| Condition::parse(part).map(Self::Condition))
                .collect::<Result<Vec<_>, _>>()
                .map(Self::And);
        }
        Condition::parse(body).map(Self::Condition)
    }

    pub(super) fn matches(&self, span: &DurableSpan, trace: &[DurableSpan]) -> bool {
        match self {
            Self::True => true,
            Self::Condition(condition) => condition.matches(span, trace),
            Self::And(filters) => filters.iter().all(|filter| filter.matches(span, trace)),
            Self::Or(filters) => filters.iter().any(|filter| filter.matches(span, trace)),
        }
    }

    pub(super) fn exact_trace_id(&self) -> Option<TraceId> {
        match self {
            Self::Condition(condition) => condition.exact_trace_id(),
            Self::And(filters) => filters.iter().find_map(Self::exact_trace_id),
            Self::True | Self::Or(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Condition {
    field: String,
    operation: Comparison,
    value: Literal,
}

impl Condition {
    pub(super) fn parse(input: &str) -> Result<Self, TraceqlError> {
        for (token, operation) in [
            ("=~", Comparison::Regex),
            ("!~", Comparison::NotRegex),
            (">=", Comparison::GreaterOrEqual),
            ("<=", Comparison::LessOrEqual),
            ("!=", Comparison::NotEqual),
            ("=", Comparison::Equal),
            (">", Comparison::Greater),
            ("<", Comparison::Less),
        ] {
            if let Some(index) = find_unquoted(input, token) {
                let field = input[..index].trim().trim_start_matches('.').to_owned();
                if field.is_empty() {
                    return Err(TraceqlError::new("TraceQL condition has an empty field"));
                }
                let value = Literal::parse(input[index + token.len()..].trim())?;
                return Ok(Self {
                    field,
                    operation,
                    value,
                });
            }
        }
        Err(TraceqlError::new(format!(
            "invalid TraceQL condition {input:?}"
        )))
    }

    pub(super) fn matches(&self, span: &DurableSpan, trace: &[DurableSpan]) -> bool {
        let observed = field_values(span, trace, &self.field);
        compare(&observed, &self.value, self.operation)
    }

    pub(super) fn exact_trace_id(&self) -> Option<TraceId> {
        (self.field == "trace:id" && self.operation == Comparison::Equal)
            .then(|| match &self.value {
                Literal::String(value) => parse_trace_id(value),
                _ => None,
            })
            .flatten()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Comparison {
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
    Regex,
    NotRegex,
}

#[derive(Debug, Clone)]
pub(super) enum Literal {
    Nil,
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Duration(u64),
}

impl Literal {
    pub(super) fn parse(input: &str) -> Result<Self, TraceqlError> {
        if input == "nil" {
            return Ok(Self::Nil);
        }
        if input.starts_with('"') {
            return serde_json::from_str::<String>(input)
                .map(Self::String)
                .map_err(|error| TraceqlError::new(error.to_string()));
        }
        if let Some(duration) = parse_duration_nanos(input) {
            return Ok(Self::Duration(duration));
        }
        if input == "true" || input == "false" {
            return Ok(Self::Boolean(input == "true"));
        }
        if let Ok(value) = input.parse::<i64>() {
            return Ok(Self::Integer(value));
        }
        if let Ok(value) = input.parse::<f64>() {
            return Ok(Self::Float(value));
        }
        Ok(Self::String(input.to_owned()))
    }
}

#[derive(Debug, Clone)]
pub(super) enum ObservedValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Duration(u64),
}

pub(super) fn field_values(
    span: &DurableSpan,
    trace: &[DurableSpan],
    field: &str,
) -> Vec<ObservedValue> {
    let singleton = match field {
        "name" | "span:name" => Some(ObservedValue::String(span.name.to_string())),
        "duration" | "span:duration" => Some(ObservedValue::Duration(span.duration_nanos)),
        "traceDuration" | "trace:duration" => trace_duration(trace).map(ObservedValue::Duration),
        "rootName" | "trace:rootName" => trace
            .iter()
            .find(|candidate| candidate.parent_span_id.is_none())
            .map(|root| ObservedValue::String(root.name.to_string())),
        "rootServiceName" | "trace:rootServiceName" => trace
            .iter()
            .find(|candidate| candidate.parent_span_id.is_none())
            .and_then(|root| attribute(&root.resource.attributes, "service.name"))
            .and_then(observed_telemetry_value),
        "kind" | "span:kind" => Some(ObservedValue::Integer(i64::from(span.kind))),
        "status" | "span:status" => Some(ObservedValue::Integer(i64::from(
            span.status.as_ref().map_or(0, |status| status.code),
        ))),
        "statusMessage" | "span:statusMessage" => span
            .status
            .as_ref()
            .map(|status| ObservedValue::String(status.message.to_string())),
        "trace:id" => Some(ObservedValue::String(span.trace_id.to_string())),
        "span:id" => Some(ObservedValue::String(span.span_id.to_string())),
        "parent" | "span:parent" => span
            .parent_span_id
            .map(|id| ObservedValue::String(id.to_string())),
        "instrumentation:name" | "scope:name" => {
            Some(ObservedValue::String(span.scope.name.to_string()))
        }
        "instrumentation:version" | "scope:version" => {
            Some(ObservedValue::String(span.scope.version.to_string()))
        }
        _ => None,
    };
    if let Some(value) = singleton {
        return vec![value];
    }

    let mut observed = Vec::new();
    match field {
        "event:name" => observed.extend(
            span.events
                .iter()
                .map(|event| ObservedValue::String(event.name.to_string())),
        ),
        "link:traceID" | "link:traceId" => observed.extend(
            span.links
                .iter()
                .map(|link| ObservedValue::String(link.trace_id.to_string())),
        ),
        "link:spanID" | "link:spanId" => observed.extend(
            span.links
                .iter()
                .map(|link| ObservedValue::String(link.span_id.to_string())),
        ),
        _ => {
            let value = field
                .strip_prefix("resource.")
                .and_then(|key| attribute(&span.resource.attributes, key))
                .or_else(|| {
                    field
                        .strip_prefix("span.")
                        .and_then(|key| attribute(&span.attributes, key))
                })
                .or_else(|| {
                    field
                        .strip_prefix("instrumentation.")
                        .and_then(|key| attribute(&span.scope.attributes, key))
                })
                .or_else(|| attribute(&span.attributes, field));
            if let Some(value) = value {
                append_observed_values(value, &mut observed);
            } else if let Some(key) = field.strip_prefix("event.") {
                for event in span.events.iter() {
                    if let Some(value) = attribute(&event.attributes, key) {
                        append_observed_values(value, &mut observed);
                    }
                }
            } else if let Some(key) = field.strip_prefix("link.") {
                for link in span.links.iter() {
                    if let Some(value) = attribute(&link.attributes, key) {
                        append_observed_values(value, &mut observed);
                    }
                }
            }
        }
    }
    observed
}

pub(super) fn trace_duration(trace: &[DurableSpan]) -> Option<u64> {
    let start = trace.iter().map(|span| span.start_time_unix_nanos).min()?;
    let end = trace
        .iter()
        .filter_map(DurableSpan::end_time_unix_nanos)
        .max()?;
    end.checked_sub(start)
}

pub(super) fn attribute<'a>(
    attributes: &'a [TelemetryAttribute],
    key: &str,
) -> Option<&'a TelemetryValue> {
    attributes
        .iter()
        .rev()
        .find(|attribute| attribute.key.as_ref() == key)
        .and_then(|attribute| attribute.value.as_ref())
}

pub(super) fn observed_telemetry_value(value: &TelemetryValue) -> Option<ObservedValue> {
    match value {
        TelemetryValue::String(value) => Some(ObservedValue::String(value.to_string())),
        TelemetryValue::Boolean(value) => Some(ObservedValue::Boolean(*value)),
        TelemetryValue::Integer(value) => Some(ObservedValue::Integer(*value)),
        TelemetryValue::DoubleBits(bits) => Some(ObservedValue::Float(f64::from_bits(*bits))),
        TelemetryValue::StringTableIndex(value) => Some(ObservedValue::Integer(i64::from(*value))),
        TelemetryValue::Empty
        | TelemetryValue::Bytes(_)
        | TelemetryValue::Array(_)
        | TelemetryValue::Map(_) => None,
    }
}

pub(super) fn append_observed_values(value: &TelemetryValue, output: &mut Vec<ObservedValue>) {
    match value {
        TelemetryValue::Array(values) => {
            for value in values.iter() {
                append_observed_values(value, output);
            }
        }
        _ => output.extend(observed_telemetry_value(value)),
    }
}

pub(super) fn value_string(value: &TelemetryValue) -> Option<&str> {
    match value {
        TelemetryValue::String(value) => Some(value),
        _ => None,
    }
}

pub(super) fn compare(
    observed: &[ObservedValue],
    expected: &Literal,
    operation: Comparison,
) -> bool {
    if matches!(expected, Literal::Nil) {
        return match operation {
            Comparison::Equal => observed.is_empty(),
            Comparison::NotEqual => !observed.is_empty(),
            _ => false,
        };
    }
    if observed.is_empty() {
        return false;
    }
    match operation {
        Comparison::NotEqual => observed
            .iter()
            .all(|value| !compare_one(value, expected, Comparison::Equal)),
        Comparison::NotRegex => observed
            .iter()
            .all(|value| !compare_one(value, expected, Comparison::Regex)),
        _ => observed
            .iter()
            .any(|value| compare_one(value, expected, operation)),
    }
}

pub(super) fn compare_one(
    observed: &ObservedValue,
    expected: &Literal,
    operation: Comparison,
) -> bool {
    if operation == Comparison::Regex {
        let observed = observed_string(observed);
        let expected = literal_string(expected);
        return Regex::new(&format!("^(?:{expected})$"))
            .is_ok_and(|regex| regex.is_match(&observed));
    }
    match (numeric_observed(observed), numeric_literal(expected)) {
        (Some(left), Some(right)) => compare_order(left, right, operation),
        _ => {
            let equal = observed_string(observed) == literal_string(expected);
            match operation {
                Comparison::Equal => equal,
                Comparison::NotEqual => !equal,
                Comparison::Greater
                | Comparison::GreaterOrEqual
                | Comparison::Less
                | Comparison::LessOrEqual
                | Comparison::Regex
                | Comparison::NotRegex => false,
            }
        }
    }
}

pub(super) fn compare_order(left: f64, right: f64, operation: Comparison) -> bool {
    match operation {
        Comparison::Equal => left == right,
        Comparison::NotEqual => left != right,
        Comparison::Greater => left > right,
        Comparison::GreaterOrEqual => left >= right,
        Comparison::Less => left < right,
        Comparison::LessOrEqual => left <= right,
        Comparison::Regex | Comparison::NotRegex => false,
    }
}

pub(super) fn numeric_observed(value: &ObservedValue) -> Option<f64> {
    match value {
        ObservedValue::Integer(value) => Some(*value as f64),
        ObservedValue::Float(value) => Some(*value),
        ObservedValue::Duration(value) => Some(*value as f64),
        ObservedValue::String(_) | ObservedValue::Boolean(_) => None,
    }
}

pub(super) fn numeric_literal(value: &Literal) -> Option<f64> {
    match value {
        Literal::Nil => None,
        Literal::Integer(value) => Some(*value as f64),
        Literal::Float(value) => Some(*value),
        Literal::Duration(value) => Some(*value as f64),
        Literal::String(_) | Literal::Boolean(_) => None,
    }
}

pub(super) fn observed_string(value: &ObservedValue) -> String {
    match value {
        ObservedValue::String(value) => value.clone(),
        ObservedValue::Integer(value) => value.to_string(),
        ObservedValue::Float(value) => value.to_string(),
        ObservedValue::Boolean(value) => value.to_string(),
        ObservedValue::Duration(value) => value.to_string(),
    }
}

pub(super) fn literal_string(value: &Literal) -> String {
    match value {
        Literal::Nil => "nil".to_owned(),
        Literal::String(value) => value.clone(),
        Literal::Integer(value) => value.to_string(),
        Literal::Float(value) => value.to_string(),
        Literal::Boolean(value) => value.to_string(),
        Literal::Duration(value) => value.to_string(),
    }
}
