use super::*;

pub(crate) fn row_matches(row: &AnalyticsRow, request: &AnalyticsScanRequest) -> bool {
    let timestamp = u64::try_from(row.timestamp_unix_nanos).ok();
    if request
        .start_timestamp_unix_nanos
        .is_some_and(|start| timestamp.is_none_or(|value| value < start))
        || request
            .end_timestamp_unix_nanos
            .is_some_and(|end| timestamp.is_none_or(|value| value >= end))
        || request
            .trace_id
            .is_some_and(|value| row.trace_id.as_deref() != Some(value.to_string().as_str()))
        || request
            .span_id
            .is_some_and(|value| row.span_id.as_deref() != Some(value.to_string().as_str()))
        || request.series_id.is_some_and(|value| {
            row.series_id.as_deref() != Some(format!("{:032x}", value.get()).as_str())
        })
        || request
            .name
            .as_ref()
            .is_some_and(|value| row.name.as_deref() != Some(value.as_ref()))
    {
        return false;
    }
    fields_match(&row.labels, &request.labels)
        && fields_match(&row.metadata, &request.metadata)
        && fields_match(&row.attributes, &request.attributes)
        && fields_match(&row.resource_attributes, &request.resource_attributes)
        && fields_match(&row.scope_attributes, &request.scope_attributes)
        && request.terms.iter().all(|term| {
            row.message
                .as_deref()
                .is_some_and(|message| message_has_term(message, term))
        })
        && request.message_tokens.iter().all(|term| {
            row.message.as_deref().is_some_and(|message| {
                message_has_clickhouse_token(message, term, CaseSensitivity::Sensitive)
            })
        })
        && request.case_insensitive_message_tokens.iter().all(|term| {
            row.message.as_deref().is_some_and(|message| {
                message_has_clickhouse_token(message, term, CaseSensitivity::Insensitive)
            })
        })
        && predicate_matches_row(&request.predicate, row)
}

pub(super) fn fields_match(values: &BTreeMap<String, String>, expected: &[MetadataField]) -> bool {
    expected.iter().all(|field| {
        values.get(field.key.as_ref()).map(String::as_str) == Some(field.value.as_ref())
    })
}

pub(super) fn predicate_matches_row(predicate: &LogPredicate, row: &AnalyticsRow) -> bool {
    let message = row.message.as_deref().unwrap_or_default();
    match predicate {
        LogPredicate::MatchAll => true,
        LogPredicate::MatchNone => false,
        LogPredicate::Term(term) => message_has_term(message, term),
        LogPredicate::MessageToken {
            value,
            case_sensitivity,
        } => message_has_clickhouse_token(message, value, *case_sensitivity),
        LogPredicate::Message(matcher) => text_matches_row(message, matcher),
        LogPredicate::MessageRegex(regex) => regex.is_match(message),
        LogPredicate::MessageTokenRegex(regex) => {
            crate::query::message_has_token_regex(message, regex)
        }
        LogPredicate::MessageTokenPrefix {
            value,
            case_sensitivity,
        } => crate::query::message_has_token_prefix(message, value, *case_sensitivity),
        LogPredicate::MessagePhrase {
            terms,
            max_gap,
            case_sensitivity,
        } => crate::query::message_has_phrase(message, terms, *max_gap, *case_sensitivity),
        LogPredicate::MessageFuzzy {
            value,
            max_distance,
        } => crate::query::message_has_fuzzy_token(message, value, *max_distance),
        LogPredicate::FieldExists(key) => row_has_field(row, key, |_, _| true),
        LogPredicate::Field { key, matcher } => {
            row_has_field(row, key, |_, value| text_matches_row(value, matcher))
        }
        LogPredicate::FieldIn { key, values } => row_has_field(row, key, |_, value| {
            values.iter().any(|expected| expected.as_ref() == value)
        }),
        LogPredicate::FieldRegex { key, regex } => {
            row_has_field(row, key, |_, value| regex.is_match(value))
        }
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => row_has_field(row, key, |_, observed| {
            observed
                .parse::<i128>()
                .is_ok_and(|observed| numeric_matches_row(observed, *comparison, *value))
        }),
        LogPredicate::And(predicates) => predicates
            .iter()
            .all(|predicate| predicate_matches_row(predicate, row)),
        LogPredicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| predicate_matches_row(predicate, row)),
        LogPredicate::Not(predicate) => !predicate_matches_row(predicate, row),
    }
}

pub(super) fn wildcard_pattern_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    for character in pattern.chars() {
        match character {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => regex.push_str("\\\\"),
            character if ".^$*+?()[]{}|".contains(character) => {
                regex.push('\\');
                regex.push(character);
            }
            character => regex.push(character),
        }
    }
    regex.push('$');
    regex
}

pub(super) fn row_has_field(
    row: &AnalyticsRow,
    key: &str,
    mut predicate: impl FnMut(&str, &str) -> bool,
) -> bool {
    if let Some(name) = key.strip_prefix("resource.loki.label.") {
        return row
            .labels
            .get(name)
            .is_some_and(|value| predicate(key, value));
    }
    if let Some(name) = key.strip_prefix("attr.loki.metadata.") {
        return row
            .metadata
            .get(name)
            .is_some_and(|value| predicate(key, value));
    }
    if let Some(name) = key.strip_prefix("resource.") {
        return row
            .resource_attributes
            .get(name)
            .is_some_and(|value| predicate(key, value));
    }
    if let Some(name) = key.strip_prefix("scope.") {
        return row
            .scope_attributes
            .get(name)
            .is_some_and(|value| predicate(key, value));
    }
    if key == "otel.severity_number" {
        return row
            .severity_number
            .map(|value| value.to_string())
            .is_some_and(|value| predicate(key, &value));
    }
    if key == "otel.severity_text" {
        return row
            .severity_text
            .as_deref()
            .is_some_and(|value| predicate(key, value));
    }
    row.attributes
        .get(key)
        .is_some_and(|value| predicate(key, value))
}

pub(super) fn text_matches_row(observed: &str, matcher: &TextMatcher) -> bool {
    let equal = |left: &str, right: &str| match matcher.case_sensitivity {
        CaseSensitivity::Sensitive => left == right,
        CaseSensitivity::Insensitive => left.eq_ignore_ascii_case(right),
    };
    match matcher.kind {
        TextMatchKind::Exact => equal(observed, &matcher.value),
        TextMatchKind::Contains => match matcher.case_sensitivity {
            CaseSensitivity::Sensitive => observed.contains(&*matcher.value),
            CaseSensitivity::Insensitive => observed
                .to_ascii_lowercase()
                .contains(&matcher.value.to_ascii_lowercase()),
        },
        TextMatchKind::Prefix => match matcher.case_sensitivity {
            CaseSensitivity::Sensitive => observed.starts_with(&*matcher.value),
            CaseSensitivity::Insensitive => observed
                .get(..matcher.value.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(&matcher.value)),
        },
        TextMatchKind::Suffix => match matcher.case_sensitivity {
            CaseSensitivity::Sensitive => observed.ends_with(&*matcher.value),
            CaseSensitivity::Insensitive => observed
                .get(observed.len().saturating_sub(matcher.value.len())..)
                .is_some_and(|suffix| suffix.eq_ignore_ascii_case(&matcher.value)),
        },
    }
}

pub(super) fn numeric_matches_row(
    observed: i128,
    comparison: NumericComparison,
    expected: i128,
) -> bool {
    match comparison {
        NumericComparison::Equal => observed == expected,
        NumericComparison::NotEqual => observed != expected,
        NumericComparison::LessThan => observed < expected,
        NumericComparison::LessThanOrEqual => observed <= expected,
        NumericComparison::GreaterThan => observed > expected,
        NumericComparison::GreaterThanOrEqual => observed >= expected,
    }
}
