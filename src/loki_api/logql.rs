use super::*;

pub(crate) fn parse_log_query(expression: &str) -> Result<LogSelector, LokiApiError> {
    let end = matching_brace(expression)
        .ok_or_else(|| LokiApiError::bad_request("LogQL query requires a stream selector"))?;
    let matchers = parse_selector_matchers(&expression[..=end])?;
    let stages = parse_pipeline_stages(&expression[end + 1..])?;
    Ok(LogSelector { matchers, stages })
}

pub(super) fn parse_selector_matchers(input: &str) -> Result<Vec<LabelMatcher>, LokiApiError> {
    let input = input.trim();
    if !input.starts_with('{') || !input.ends_with('}') {
        return Err(LokiApiError::bad_request("invalid stream selector"));
    }
    let mut matchers = Vec::new();
    let mut remaining = &input[1..input.len() - 1];
    loop {
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            break;
        }
        let name_end = remaining
            .find(|character: char| {
                character == '=' || character == '!' || character.is_whitespace()
            })
            .ok_or_else(|| LokiApiError::bad_request("invalid label matcher"))?;
        let name = remaining[..name_end].trim();
        validate_label_name(name)?;
        remaining = remaining[name_end..].trim_start();
        let (operation, operator) = if remaining.starts_with("=~") {
            (MatchOperation::Regex, "=~")
        } else if remaining.starts_with("!~") {
            (MatchOperation::NotRegex, "!~")
        } else if remaining.starts_with("!=") {
            (MatchOperation::NotEqual, "!=")
        } else if remaining.starts_with('=') {
            (MatchOperation::Equal, "=")
        } else {
            return Err(LokiApiError::bad_request("invalid label matcher operation"));
        };
        remaining = remaining[operator.len()..].trim_start();
        let (value, rest) = parse_quoted(remaining)?;
        let regex = matches!(operation, MatchOperation::Regex | MatchOperation::NotRegex)
            .then(|| Regex::new(&format!("^(?:{value})$")))
            .transpose()
            .map_err(|error| LokiApiError::bad_request(format!("invalid label regex: {error}")))?;
        matchers.push(LabelMatcher {
            name: name.to_owned(),
            operation,
            value,
            regex,
        });
        remaining = rest.trim_start();
        if let Some(rest) = remaining.strip_prefix(',') {
            remaining = rest;
        } else if !remaining.is_empty() {
            return Err(LokiApiError::bad_request(
                "expected comma between label matchers",
            ));
        }
    }
    Ok(matchers)
}

pub(super) fn parse_pipeline_stages(mut input: &str) -> Result<Vec<PipelineStage>, LokiApiError> {
    let mut stages = Vec::new();
    loop {
        input = input.trim_start();
        if input.is_empty() {
            return Ok(stages);
        }
        let (operation, rest) = if let Some(rest) = input.strip_prefix("|=") {
            (Some(MatchOperation::Equal), rest)
        } else if let Some(rest) = input.strip_prefix("!=") {
            (Some(MatchOperation::NotEqual), rest)
        } else if let Some(rest) = input.strip_prefix("|~") {
            (Some(MatchOperation::Regex), rest)
        } else if let Some(rest) = input.strip_prefix("!~") {
            (Some(MatchOperation::NotRegex), rest)
        } else {
            (None, input)
        };
        if let Some(operation) = operation {
            let (value, rest) = parse_quoted(rest.trim_start())?;
            let regex = matches!(operation, MatchOperation::Regex | MatchOperation::NotRegex)
                .then(|| Regex::new(&value))
                .transpose()
                .map_err(|error| LokiApiError::bad_request(format!("invalid regex: {error}")))?;
            stages.push(PipelineStage::Line(LineFilter {
                operation,
                value,
                regex,
            }));
            input = rest;
            continue;
        }
        let Some(rest) = input.strip_prefix('|') else {
            return Err(LokiApiError::bad_request(
                "unsupported or invalid LogQL pipeline stage",
            ));
        };
        let (segment, remaining) = take_pipeline_segment(rest.trim_start());
        let (name, arguments) = segment
            .trim()
            .split_once(char::is_whitespace)
            .map_or((segment.trim(), ""), |(name, arguments)| {
                (name, arguments.trim())
            });
        match name {
            "json" => stages.push(PipelineStage::Json(parse_json_expressions(arguments)?)),
            "logfmt" => stages.push(PipelineStage::Logfmt),
            "regexp" => {
                let (expression, trailing) = parse_quoted(arguments)?;
                if !trailing.trim().is_empty() {
                    return Err(LokiApiError::bad_request("regexp stage has trailing input"));
                }
                stages.push(PipelineStage::Regexp(Regex::new(&expression).map_err(
                    |error| LokiApiError::bad_request(format!("invalid regexp stage: {error}")),
                )?));
            }
            "pattern" => {
                let (expression, trailing) = parse_quoted(arguments)?;
                if !trailing.trim().is_empty() {
                    return Err(LokiApiError::bad_request(
                        "pattern stage has trailing input",
                    ));
                }
                stages.push(PipelineStage::Pattern(compile_pattern_parser(&expression)?));
            }
            "line_format" => {
                let (template, trailing) = parse_quoted(arguments)?;
                if !trailing.trim().is_empty() {
                    return Err(LokiApiError::bad_request(
                        "line_format stage has trailing input",
                    ));
                }
                stages.push(PipelineStage::LineFormat(template));
            }
            "label_format" => stages.push(PipelineStage::LabelFormat(
                parse_label_format_assignments(arguments)?,
            )),
            "drop" => stages.push(PipelineStage::Drop(parse_label_list(arguments)?)),
            "keep" => stages.push(PipelineStage::Keep(parse_label_list(arguments)?)),
            "decolorize" if arguments.is_empty() => stages.push(PipelineStage::Decolorize),
            "unpack" if arguments.is_empty() => stages.push(PipelineStage::Unpack),
            "unwrap" => {
                let label = arguments
                    .split_ascii_whitespace()
                    .next()
                    .filter(|label| validate_label_name(label).is_ok())
                    .ok_or_else(|| LokiApiError::bad_request("unwrap requires a label name"))?;
                stages.push(PipelineStage::Unwrap(label.to_owned()));
            }
            _ => {
                stages.extend(parse_label_filter_expression(segment.trim())?);
            }
        }
        input = remaining;
    }
}

pub(super) fn take_pipeline_segment(input: &str) -> (&str, &str) {
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if character == '|' && !quoted {
            return (&input[..index], &input[index..]);
        }
    }
    (input, "")
}

pub(super) fn parse_json_expressions(
    input: &str,
) -> Result<Vec<(String, Vec<String>)>, LokiApiError> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    split_unquoted(input, ',')
        .into_iter()
        .map(|assignment| {
            let (label, path) = assignment
                .split_once('=')
                .ok_or_else(|| LokiApiError::bad_request("invalid json extraction expression"))?;
            let label = label.trim();
            validate_label_name(label)?;
            let (path, trailing) = parse_quoted(path.trim())?;
            if !trailing.trim().is_empty() || path.is_empty() {
                return Err(LokiApiError::bad_request("invalid json extraction path"));
            }
            Ok((
                label.to_owned(),
                path.trim_start_matches('.')
                    .split('.')
                    .map(str::to_owned)
                    .collect(),
            ))
        })
        .collect()
}

pub(super) fn parse_label_format_assignments(
    input: &str,
) -> Result<Vec<(String, LabelFormatValue)>, LokiApiError> {
    let assignments = split_unquoted(input, ',');
    if assignments.is_empty() {
        return Err(LokiApiError::bad_request(
            "label_format requires at least one assignment",
        ));
    }
    assignments
        .into_iter()
        .map(|assignment| {
            let (target, value) = assignment
                .split_once('=')
                .ok_or_else(|| LokiApiError::bad_request("invalid label_format assignment"))?;
            let target = target.trim();
            validate_label_name(target)?;
            let value = value.trim();
            let value = if value.starts_with('"') {
                let (template, trailing) = parse_quoted(value)?;
                if !trailing.trim().is_empty() {
                    return Err(LokiApiError::bad_request(
                        "label_format template has trailing input",
                    ));
                }
                LabelFormatValue::Template(template)
            } else {
                validate_label_name(value)?;
                LabelFormatValue::Rename(value.to_owned())
            };
            Ok((target.to_owned(), value))
        })
        .collect()
}

pub(super) fn parse_label_list(input: &str) -> Result<Vec<String>, LokiApiError> {
    let labels = input
        .split([',', ' '])
        .filter(|label| !label.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if labels.is_empty() {
        return Err(LokiApiError::bad_request(
            "label list stage requires at least one label",
        ));
    }
    labels
        .iter()
        .try_for_each(|label| validate_label_name(label))?;
    Ok(labels)
}

pub(super) fn parse_label_filter_expression(
    input: &str,
) -> Result<Vec<PipelineStage>, LokiApiError> {
    split_keyword_unquoted(input, "and")
        .into_iter()
        .map(|condition| parse_label_filter(condition).map(PipelineStage::LabelFilter))
        .collect()
}

pub(super) fn parse_label_filter(input: &str) -> Result<LabelFilter, LokiApiError> {
    let (index, operator) = ["=~", "!~", ">=", "<=", "!=", "=", ">", "<"]
        .into_iter()
        .filter_map(|operator| find_unquoted(input, operator).map(|index| (index, operator)))
        .min_by_key(|(index, _)| *index)
        .ok_or_else(|| LokiApiError::bad_request("invalid label filter"))?;
    let name = input[..index].trim();
    validate_label_name(name)?;
    let raw_value = input[index + operator.len()..].trim();
    let value = if raw_value.starts_with('"') {
        let (value, trailing) = parse_quoted(raw_value)?;
        if !trailing.trim().is_empty() {
            return Err(LokiApiError::bad_request("label filter has trailing input"));
        }
        value
    } else if !raw_value.is_empty() {
        raw_value.to_owned()
    } else {
        return Err(LokiApiError::bad_request("label filter value is missing"));
    };
    let operation = match operator {
        "=" => LabelFilterOperation::Equal,
        "!=" => LabelFilterOperation::NotEqual,
        "=~" => LabelFilterOperation::Regex,
        "!~" => LabelFilterOperation::NotRegex,
        ">" => LabelFilterOperation::Greater,
        ">=" => LabelFilterOperation::GreaterEqual,
        "<" => LabelFilterOperation::Less,
        "<=" => LabelFilterOperation::LessEqual,
        _ => unreachable!(),
    };
    let regex = matches!(
        operation,
        LabelFilterOperation::Regex | LabelFilterOperation::NotRegex
    )
    .then(|| Regex::new(&format!("^(?:{value})$")))
    .transpose()
    .map_err(|error| LokiApiError::bad_request(format!("invalid label filter regex: {error}")))?;
    Ok(LabelFilter {
        name: name.to_owned(),
        operation,
        value,
        regex,
    })
}

pub(super) fn split_unquoted(input: &str, separator: char) -> Vec<&str> {
    let mut output = Vec::new();
    let mut start = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if character == separator && !quoted {
            let value = input[start..index].trim();
            if !value.is_empty() {
                output.push(value);
            }
            start = index + character.len_utf8();
        }
    }
    let value = input[start..].trim();
    if !value.is_empty() {
        output.push(value);
    }
    output
}

pub(super) fn split_keyword_unquoted<'a>(input: &'a str, keyword: &str) -> Vec<&'a str> {
    let needle = format!(" {keyword} ");
    let mut output = Vec::new();
    let mut start = 0usize;
    while let Some(relative) = find_unquoted(&input[start..], &needle) {
        let index = start + relative;
        let value = input[start..index].trim();
        if !value.is_empty() {
            output.push(value);
        }
        start = index + needle.len();
    }
    let value = input[start..].trim();
    if !value.is_empty() {
        output.push(value);
    }
    output
}

pub(super) fn find_unquoted(input: &str, needle: &str) -> Option<usize> {
    let mut quoted = false;
    let mut escaped = false;
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
        if !quoted && input[index..].starts_with(needle) {
            return Some(index);
        }
    }
    None
}

pub(super) fn flatten_json_object(
    prefix: &str,
    object: &serde_json::Map<String, Value>,
    labels: &mut BTreeMap<String, String>,
) {
    for (name, value) in object {
        let normalized = normalize_extracted_label_name(name);
        let path = if prefix.is_empty() {
            normalized
        } else {
            format!("{prefix}_{normalized}")
        };
        match value {
            Value::Object(nested) => flatten_json_object(&path, nested, labels),
            value => {
                if let Some(value) = scalar_label_value(value) {
                    insert_extracted_label(labels, &path, value);
                }
            }
        }
    }
}

pub(super) fn json_path<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    path.iter().try_fold(value, |value, component| match value {
        Value::Object(object) => object.get(component),
        Value::Array(values) => component
            .parse::<usize>()
            .ok()
            .and_then(|index| values.get(index)),
        _ => None,
    })
}

pub(super) fn scalar_label_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null => Some(String::new()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

pub(super) fn normalize_extracted_label_name(name: &str) -> String {
    let mut normalized = name
        .chars()
        .enumerate()
        .map(|(index, character)| {
            if character == '_'
                || character.is_ascii_alphabetic()
                || (index > 0 && character.is_ascii_digit())
            {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if normalized
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_digit)
    {
        normalized.insert(0, '_');
    }
    normalized
}

pub(super) fn insert_extracted_label(
    labels: &mut BTreeMap<String, String>,
    requested: &str,
    value: String,
) {
    let mut name = normalize_extracted_label_name(requested);
    while labels.contains_key(&name) {
        name.push_str("_extracted");
    }
    labels.insert(name, value);
}

pub(super) fn set_parser_error(entry: &mut LokiEntry, error: &str) {
    entry
        .labels
        .entry("__error__".to_owned())
        .or_insert_with(|| error.to_owned());
}

pub(super) fn parse_logfmt_labels(line: &str) -> Vec<(String, String)> {
    let bytes = line.as_bytes();
    let mut output = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        let key_start = cursor;
        while cursor < bytes.len() && !bytes[cursor].is_ascii_whitespace() && bytes[cursor] != b'='
        {
            cursor += 1;
        }
        if cursor == key_start || bytes.get(cursor) != Some(&b'=') {
            while cursor < bytes.len() && !bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            continue;
        }
        let key = &line[key_start..cursor];
        cursor += 1;
        let value = if bytes.get(cursor) == Some(&b'"') {
            let value_start = cursor;
            cursor += 1;
            let mut escaped = false;
            while cursor < bytes.len() {
                if escaped {
                    escaped = false;
                } else if bytes[cursor] == b'\\' {
                    escaped = true;
                } else if bytes[cursor] == b'"' {
                    cursor += 1;
                    break;
                }
                cursor += 1;
            }
            serde_json::from_str::<String>(&line[value_start..cursor]).unwrap_or_default()
        } else {
            let value_start = cursor;
            while cursor < bytes.len() && !bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            line[value_start..cursor].to_owned()
        };
        if validate_label_name(key).is_ok() {
            output.push((key.to_owned(), value));
        }
    }
    output
}

pub(super) fn compile_pattern_parser(pattern: &str) -> Result<Regex, LokiApiError> {
    let mut regex = String::from("^");
    let mut remaining = pattern;
    let mut capture_count = 0usize;
    while let Some(start) = remaining.find('<') {
        regex.push_str(&regex::escape(&remaining[..start]));
        let after = &remaining[start + 1..];
        let end = after
            .find('>')
            .ok_or_else(|| LokiApiError::bad_request("unterminated pattern capture"))?;
        let capture = &after[..end];
        if capture == "_" {
            regex.push_str("(?s:.*?)");
        } else {
            validate_label_name(capture)?;
            regex.push_str("(?P<");
            regex.push_str(capture);
            regex.push_str(">(?s:.*?))");
            capture_count += 1;
        }
        remaining = &after[end + 1..];
    }
    regex.push_str(&regex::escape(remaining));
    regex.push('$');
    if capture_count == 0 {
        return Err(LokiApiError::bad_request(
            "pattern parser requires at least one named capture",
        ));
    }
    Regex::new(&regex)
        .map_err(|error| LokiApiError::bad_request(format!("invalid pattern parser: {error}")))
}

pub(super) fn render_logql_template(template: &str, entry: &LokiEntry) -> String {
    let mut output = String::new();
    let mut remaining = template;
    while let Some(start) = remaining.find("{{") {
        output.push_str(&remaining[..start]);
        let after = &remaining[start + 2..];
        let Some(end) = after.find("}}") else {
            output.push_str(&remaining[start..]);
            return output;
        };
        let expression = after[..end].trim();
        let value = if expression == "__line__" {
            entry.line.as_str()
        } else if expression == "__timestamp__" {
            output.push_str(&entry.timestamp_unix_nanos.to_string());
            ""
        } else if let Some(name) = expression.strip_prefix('.') {
            entry
                .labels
                .get(name)
                .or_else(|| entry.structured_metadata.get(name))
                .map(String::as_str)
                .unwrap_or("")
        } else {
            ""
        };
        output.push_str(value);
        remaining = &after[end + 2..];
    }
    output.push_str(remaining);
    output
}

pub(super) fn strip_ansi(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0usize;
    let mut literal_start = 0usize;
    while cursor + 1 < bytes.len() {
        if bytes[cursor] == 0x1b && bytes[cursor + 1] == b'[' {
            output.push_str(&input[literal_start..cursor]);
            cursor += 2;
            while cursor < bytes.len() {
                let byte = bytes[cursor];
                cursor += 1;
                if (0x40..=0x7e).contains(&byte) {
                    break;
                }
            }
            literal_start = cursor;
        } else {
            cursor += 1;
        }
    }
    output.push_str(&input[literal_start..]);
    output
}

pub(super) fn compare_typed_label(left: &str, right: &str) -> Option<std::cmp::Ordering> {
    if let (Some(left), Some(right)) = (parse_duration_nanos(left), parse_duration_nanos(right)) {
        return Some(left.cmp(&right));
    }
    if let (Some(left), Some(right)) = (parse_byte_quantity(left), parse_byte_quantity(right)) {
        return Some(left.cmp(&right));
    }
    let left = left.parse::<f64>().ok()?;
    let right = right.parse::<f64>().ok()?;
    left.partial_cmp(&right)
}

pub(super) fn parse_label_set(input: &str) -> Result<BTreeMap<String, String>, LokiApiError> {
    let input = input.trim();
    if !input.starts_with('{') || !input.ends_with('}') {
        return Err(LokiApiError::bad_request("invalid label set"));
    }
    let mut labels = BTreeMap::new();
    let mut remaining = &input[1..input.len() - 1];
    loop {
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            break;
        }
        let name_end = remaining
            .find(|character: char| character == '=' || character.is_whitespace())
            .ok_or_else(|| LokiApiError::bad_request("invalid label matcher"))?;
        let name = remaining[..name_end].trim();
        validate_label_name(name)?;
        remaining = remaining[name_end..].trim_start();
        let operation = ["=~", "!~", "!=", "="]
            .into_iter()
            .find(|operation| remaining.starts_with(operation))
            .ok_or_else(|| LokiApiError::bad_request("invalid label matcher operation"))?;
        remaining = remaining[operation.len()..].trim_start();
        let (value, rest) = parse_quoted(remaining)?;
        if operation != "=" {
            return Err(LokiApiError::bad_request(
                "non-equality matchers are not valid in pushed label sets",
            ));
        }
        if labels.insert(name.to_owned(), value).is_some() {
            return Err(LokiApiError::bad_request("duplicate label name"));
        }
        remaining = rest.trim_start();
        if let Some(rest) = remaining.strip_prefix(',') {
            remaining = rest;
        } else if !remaining.is_empty() {
            return Err(LokiApiError::bad_request("expected comma between labels"));
        }
    }
    validate_labels(&labels)?;
    Ok(labels)
}

pub(super) fn parse_quoted(input: &str) -> Result<(String, &str), LokiApiError> {
    if !input.starts_with('"') {
        return Err(LokiApiError::bad_request("expected quoted string"));
    }
    let bytes = input.as_bytes();
    let mut escaped = false;
    for index in 1..bytes.len() {
        if escaped {
            escaped = false;
            continue;
        }
        match bytes[index] {
            b'\\' => escaped = true,
            b'"' => {
                let encoded = &input[..=index];
                let value: String = serde_json::from_str(encoded)
                    .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
                return Ok((value, &input[index + 1..]));
            }
            _ => {}
        }
    }
    Err(LokiApiError::bad_request("unterminated quoted string"))
}

pub(super) fn matching_brace(input: &str) -> Option<usize> {
    input
        .char_indices()
        .find_map(|(index, character)| (character == '}').then_some(index))
}

pub(super) fn validate_labels(labels: &BTreeMap<String, String>) -> Result<(), LokiApiError> {
    if labels.is_empty() {
        return Err(LokiApiError::bad_request(
            "at least one stream label is required",
        ));
    }
    labels.keys().try_for_each(|name| validate_label_name(name))
}

pub(super) fn validate_label_name(name: &str) -> Result<(), LokiApiError> {
    let valid = !name.is_empty()
        && name.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphabetic() || (index > 0 && byte.is_ascii_digit())
        });
    if valid {
        Ok(())
    } else {
        Err(LokiApiError::bad_request(format!(
            "invalid label name {name:?}"
        )))
    }
}

pub(super) fn query_range_bounds(params: &QueryParams) -> Result<(i64, i64), LokiApiError> {
    let now = now_nanos();
    let end = params
        .end
        .as_deref()
        .or(params.time.as_deref())
        .map(parse_timestamp)
        .transpose()?
        .unwrap_or(now);
    let start = params
        .start
        .as_deref()
        .map(parse_timestamp)
        .transpose()?
        .unwrap_or_else(|| {
            params
                .since
                .as_deref()
                .and_then(parse_duration_nanos)
                .and_then(|duration| end.checked_sub(duration))
                .unwrap_or(0)
        });
    if start > end {
        return Err(LokiApiError::bad_request("start is after end"));
    }
    Ok((start, end))
}

pub(super) fn parse_duration_nanos(value: &str) -> Option<i64> {
    let (number, scale) = if let Some(number) = value.strip_suffix("ns") {
        (number, 1)
    } else if let Some(number) = value.strip_suffix("us") {
        (number, 1_000)
    } else if let Some(number) = value.strip_suffix("ms") {
        (number, 1_000_000)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000_000_000)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60 * 1_000_000_000)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 60 * 60 * 1_000_000_000)
    } else {
        return None;
    };
    number
        .parse::<i64>()
        .ok()
        .and_then(|number| number.checked_mul(scale))
}

pub(super) fn parse_timestamp(value: &str) -> Result<i64, LokiApiError> {
    if let Ok(integer) = value.parse::<i64>() {
        return Ok(integer);
    }
    if let Ok(float) = value.parse::<f64>()
        && float.is_finite()
    {
        return Ok((float * 1_000_000_000.0) as i64);
    }
    if let Some(timestamp) = parse_rfc3339_nanos(value) {
        return Ok(timestamp);
    }
    Err(LokiApiError::bad_request(format!(
        "invalid timestamp {value:?}"
    )))
}

pub(super) fn parse_delete_timestamp(value: &str) -> Result<i64, LokiApiError> {
    if let Ok(seconds) = value.parse::<i64>() {
        return seconds
            .checked_mul(1_000_000_000)
            .ok_or_else(|| LokiApiError::bad_request("delete timestamp is out of range"));
    }
    parse_timestamp(value)
}

pub(super) fn parse_rfc3339_nanos(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return None;
    }
    let year = i64::try_from(parse_decimal(&bytes[0..4])?).ok()?;
    let month = u32::try_from(parse_decimal(&bytes[5..7])?).ok()?;
    let day = u32::try_from(parse_decimal(&bytes[8..10])?).ok()?;
    let hour = parse_decimal(&bytes[11..13])?;
    let minute = parse_decimal(&bytes[14..16])?;
    let second = parse_decimal(&bytes[17..19])?;
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour >= 24
        || minute >= 60
        || second >= 60
    {
        return None;
    }
    let timezone_start = bytes[19..]
        .iter()
        .position(|byte| matches!(byte, b'Z' | b'+' | b'-'))?
        + 19;
    let fraction = match &bytes[19..timezone_start] {
        [] => 0,
        [b'.', digits @ ..] if !digits.is_empty() && digits.len() <= 9 => {
            parse_decimal(digits)?.checked_mul(10_u64.pow(u32::try_from(9 - digits.len()).ok()?))?
        }
        _ => return None,
    };
    let offset_seconds = match &bytes[timezone_start..] {
        [b'Z'] => 0_i64,
        [
            sign @ (b'+' | b'-'),
            hour_tens,
            hour_ones,
            b':',
            minute_tens,
            minute_ones,
        ] => {
            let offset_hours = i64::try_from(parse_decimal(&[*hour_tens, *hour_ones])?).ok()?;
            let offset_minutes =
                i64::try_from(parse_decimal(&[*minute_tens, *minute_ones])?).ok()?;
            if offset_hours >= 24 || offset_minutes >= 60 {
                return None;
            }
            let magnitude = offset_hours
                .checked_mul(3_600)?
                .checked_add(offset_minutes.checked_mul(60)?)?;
            if *sign == b'-' { -magnitude } else { magnitude }
        }
        _ => return None,
    };
    let days = days_from_civil(year, month, day);
    let local_seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::try_from(hour.checked_mul(3_600)?).ok()?)?
        .checked_add(i64::try_from(minute.checked_mul(60)?).ok()?)?
        .checked_add(i64::try_from(second).ok()?)?;
    local_seconds
        .checked_sub(offset_seconds)?
        .checked_mul(1_000_000_000)?
        .checked_add(i64::try_from(fraction).ok()?)
}

pub(super) fn parse_decimal(bytes: &[u8]) -> Option<u64> {
    bytes.iter().try_fold(0_u64, |value, byte| {
        byte.is_ascii_digit().then_some(())?;
        value.checked_mul(10)?.checked_add(u64::from(byte - b'0'))
    })
}

pub(super) fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 0,
    }
}

pub(super) fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

pub(super) fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}
