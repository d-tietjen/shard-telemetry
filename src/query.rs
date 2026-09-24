use std::cmp::Ordering;

use crate::{
    CaseSensitivity, LogPredicate, LogQuery, MetadataField, NumericComparison, QueryCursor,
    QueryOrder, QuerySort, StructuralRecordView, TextMatchKind, TextMatcher, analyze_message,
};

pub(crate) struct RequiredIndexConstraints<'a> {
    pub(crate) terms: Vec<&'a str>,
    pub(crate) fields: Vec<(&'a str, &'a str)>,
    pub(crate) field_exists: Vec<&'a str>,
    pub(crate) field_in: Vec<(&'a str, Vec<&'a str>)>,
    pub(crate) field_text: Vec<(&'a str, &'a TextMatcher)>,
    pub(crate) field_regex: Vec<(&'a str, &'a crate::LogRegex)>,
    pub(crate) field_numeric: Vec<(&'a str, NumericComparison, i128)>,
    pub(crate) message_literals: Vec<&'a str>,
    pub(crate) case_sensitive_message_literals: Vec<&'a str>,
    pub(crate) impossible: bool,
}

impl LogQuery {
    /// Returns whether a normalized structural record satisfies every lookup
    /// range, legacy exact constraint, Boolean predicate, and page cursor.
    #[must_use]
    pub fn matches<R: StructuralRecordView>(&self, record: &R) -> bool {
        self.offset_matches(record.structural_offset())
            && self.timestamp_matches(record.structural_timestamp_unix_nanos())
            && self.cursor_matches(record)
            && self.legacy_terms_match(record)
            && self.legacy_fields_match(record)
            && predicate_matches(&self.predicate, record)
    }

    /// Compares two matching records in the query's deterministic result order.
    #[must_use]
    pub fn compare<R: StructuralRecordView>(&self, left: &R, right: &R) -> Ordering {
        let ordering = match self.sort {
            QuerySort::Offset => left.structural_offset().cmp(&right.structural_offset()),
            QuerySort::Timestamp => (
                left.structural_timestamp_unix_nanos(),
                left.structural_offset(),
            )
                .cmp(&(
                    right.structural_timestamp_unix_nanos(),
                    right.structural_offset(),
                )),
        };
        if self.order == QueryOrder::NewestFirst {
            ordering.reverse()
        } else {
            ordering
        }
    }

    /// Creates the stable continuation point represented by one result.
    #[must_use]
    pub fn cursor_for<R: StructuralRecordView>(&self, record: &R) -> QueryCursor {
        QueryCursor::new(
            record.structural_timestamp_unix_nanos(),
            record.structural_offset(),
        )
    }

    /// Applies exact residual filtering, deterministic ordering, and the result
    /// limit to decoded candidate records from a sealed-block lookup.
    #[must_use]
    pub fn select<R: StructuralRecordView>(&self, records: impl IntoIterator<Item = R>) -> Vec<R> {
        let mut selected = records
            .into_iter()
            .filter(|record| self.matches(record))
            .collect::<Vec<_>>();
        selected.sort_unstable_by(|left, right| self.compare(left, right));
        if let Some(limit) = self.limit {
            selected.truncate(limit);
        }
        selected
    }

    /// Returns whether sealed candidates require record decoding before the
    /// query can be limited safely.
    ///
    /// Callers serving sealed blocks should stream these queries block by
    /// block instead of materializing the complete candidate set.
    #[must_use]
    pub fn requires_post_decode(&self) -> bool {
        self.start_offset.is_some()
            || self.end_offset.is_some()
            || self.start_timestamp_unix_nanos.is_some()
            || self.end_timestamp_unix_nanos.is_some()
            || self.after.is_some()
            || self.sort == QuerySort::Timestamp
            || !predicate_is_index_only_conjunction(&self.predicate)
    }

    pub(crate) fn required_index_constraints(&self) -> RequiredIndexConstraints<'_> {
        self.required_index_constraints_with_message_phrases(false)
    }

    pub(crate) fn required_index_constraints_with_message_phrases(
        &self,
        include_message_phrases: bool,
    ) -> RequiredIndexConstraints<'_> {
        let mut constraints = RequiredIndexConstraints {
            terms: self.terms.iter().map(AsRef::as_ref).collect(),
            fields: self
                .exact_fields
                .iter()
                .map(|field| (field.key.as_ref(), field.value.as_ref()))
                .collect(),
            field_exists: Vec::new(),
            field_in: Vec::new(),
            field_text: Vec::new(),
            field_regex: Vec::new(),
            field_numeric: Vec::new(),
            message_literals: Vec::new(),
            case_sensitive_message_literals: Vec::new(),
            impossible: false,
        };
        collect_required_constraints(&self.predicate, &mut constraints, include_message_phrases);
        constraints
    }

    pub(crate) fn can_apply_index_limit(&self) -> bool {
        self.start_offset.is_none()
            && self.end_offset.is_none()
            && self.start_timestamp_unix_nanos.is_none()
            && self.end_timestamp_unix_nanos.is_none()
            && self.after.is_none()
            && self.sort == QuerySort::Offset
            && predicate_is_index_only_conjunction(&self.predicate)
    }

    pub(crate) fn needs_record_filter(&self) -> bool {
        self.start_timestamp_unix_nanos.is_some()
            || self.end_timestamp_unix_nanos.is_some()
            || self.after.is_some()
            || self.sort == QuerySort::Timestamp
            || !predicate_is_index_only_conjunction(&self.predicate)
    }

    #[allow(dead_code)]
    pub(crate) fn can_use_message_only_filter(&self) -> bool {
        self.can_use_indexed_message_filter()
            && self.start_timestamp_unix_nanos.is_none()
            && self.end_timestamp_unix_nanos.is_none()
    }

    pub(crate) fn can_use_indexed_message_filter(&self) -> bool {
        self.start_offset.is_none()
            && self.end_offset.is_none()
            && self.after.is_none()
            && self.sort == QuerySort::Offset
            && self.message_candidate_matches("").is_some()
    }

    /// Returns a safe exact-token conjunction that can be answered from the
    /// frame's lazily-built ClickHouse-compatible postings.
    ///
    /// The caller still performs the normal record matcher after using these
    /// postings. They are therefore only a necessary-candidate optimization;
    /// this method deliberately rejects legacy terms, offset bounds, cursors,
    /// and every predicate shape other than AND of exact message tokens.
    pub(crate) fn exact_message_token_conjunction(&self) -> Option<Vec<(&str, CaseSensitivity)>> {
        if !self.terms.is_empty()
            || self.start_offset.is_some()
            || self.end_offset.is_some()
            || self.after.is_some()
        {
            return None;
        }
        fn collect<'a>(
            predicate: &'a LogPredicate,
            tokens: &mut Vec<(&'a str, CaseSensitivity)>,
        ) -> bool {
            match predicate {
                LogPredicate::MatchAll => true,
                LogPredicate::MessageToken {
                    value,
                    case_sensitivity,
                } if clickhouse_token_is_index_safe(value) => {
                    tokens.push((value, *case_sensitivity));
                    true
                }
                LogPredicate::And(predicates) => predicates
                    .iter()
                    .all(|predicate| collect(predicate, tokens)),
                _ => false,
            }
        }
        let mut tokens = Vec::new();
        collect(&self.predicate, &mut tokens).then_some(tokens)
    }

    /// Returns a safe exact-token disjunction suitable for posting-list
    /// union. This is intentionally limited to a single OR node so callers
    /// can answer count queries directly without evaluating residual logic.
    pub(crate) fn exact_message_token_disjunction(&self) -> Option<Vec<(&str, CaseSensitivity)>> {
        if !self.terms.is_empty()
            || self.start_offset.is_some()
            || self.end_offset.is_some()
            || self.after.is_some()
        {
            return None;
        }
        let predicate = match &self.predicate {
            LogPredicate::Or(predicates) => predicates,
            LogPredicate::And(predicates) if predicates.len() == 1 => {
                let LogPredicate::Or(predicates) = &predicates[0] else {
                    return None;
                };
                predicates
            }
            _ => return None,
        };
        if predicate.is_empty() {
            return Some(Vec::new());
        }
        predicate
            .iter()
            .map(|predicate| match predicate {
                LogPredicate::MessageToken {
                    value,
                    case_sensitivity,
                } if clickhouse_token_is_index_safe(value) => {
                    Some((value.as_ref(), *case_sensitivity))
                }
                _ => None,
            })
            .collect()
    }

    pub(crate) fn has_residual_predicate(&self) -> bool {
        !predicate_is_index_only_conjunction(&self.predicate)
    }

    pub(crate) fn message_candidate_matches(&self, message: &str) -> Option<bool> {
        if !self
            .terms
            .iter()
            .all(|expected| message_has_term(message, expected))
        {
            return Some(false);
        }
        message_only_predicate_matches(&self.predicate, message)
    }

    pub(crate) fn matches_index_candidate<R: StructuralRecordView>(&self, record: &R) -> bool {
        self.matches_index_bounds(record) && predicate_matches(&self.predicate, record)
    }

    pub(crate) fn matches_index_bounds<R: StructuralRecordView>(&self, record: &R) -> bool {
        self.offset_matches(record.structural_offset())
            && self.timestamp_matches(record.structural_timestamp_unix_nanos())
            && self.cursor_matches(record)
    }

    pub(crate) fn has_invalid_range(&self) -> bool {
        self.start_offset
            .zip(self.end_offset)
            .is_some_and(|(start, end)| start >= end)
            || self
                .start_timestamp_unix_nanos
                .zip(self.end_timestamp_unix_nanos)
                .is_some_and(|(start, end)| start >= end)
    }

    fn offset_matches(&self, offset: shard_stream_core::LogicalOffset) -> bool {
        self.start_offset.is_none_or(|start| offset >= start)
            && self.end_offset.is_none_or(|end| offset < end)
    }

    pub(crate) fn timestamp_matches(&self, timestamp_unix_nanos: u64) -> bool {
        self.start_timestamp_unix_nanos
            .is_none_or(|start| timestamp_unix_nanos >= start)
            && self
                .end_timestamp_unix_nanos
                .is_none_or(|end| timestamp_unix_nanos < end)
    }

    fn cursor_matches<R: StructuralRecordView>(&self, record: &R) -> bool {
        let Some(cursor) = self.after else {
            return true;
        };
        let ordering = match self.sort {
            QuerySort::Offset => record.structural_offset().cmp(&cursor.offset),
            QuerySort::Timestamp => (
                record.structural_timestamp_unix_nanos(),
                record.structural_offset(),
            )
                .cmp(&(cursor.timestamp_unix_nanos, cursor.offset)),
        };
        if self.order == QueryOrder::NewestFirst {
            ordering == Ordering::Less
        } else {
            ordering == Ordering::Greater
        }
    }

    fn legacy_terms_match<R: StructuralRecordView>(&self, record: &R) -> bool {
        self.terms
            .iter()
            .all(|expected| message_has_term(record.structural_message(), expected))
    }

    fn legacy_fields_match<R: StructuralRecordView>(&self, record: &R) -> bool {
        self.exact_fields
            .iter()
            .all(|expected| field_equals(record, expected))
    }
}

fn collect_required_constraints<'a>(
    predicate: &'a LogPredicate,
    constraints: &mut RequiredIndexConstraints<'a>,
    include_message_phrases: bool,
) {
    match predicate {
        LogPredicate::MatchAll => {}
        LogPredicate::MatchNone => constraints.impossible = true,
        LogPredicate::Term(term) => constraints.terms.push(term),
        LogPredicate::MessageToken { value, .. } if clickhouse_token_is_index_safe(value) => {
            constraints.terms.push(value);
        }
        LogPredicate::Field { key, matcher }
            if matcher.kind == TextMatchKind::Exact
                && matcher.case_sensitivity == CaseSensitivity::Sensitive =>
        {
            constraints.fields.push((key, &matcher.value));
        }
        LogPredicate::Field { key, matcher } => constraints.field_text.push((key, matcher)),
        LogPredicate::FieldExists(key) => constraints.field_exists.push(key),
        LogPredicate::FieldIn { key, values } => constraints
            .field_in
            .push((key, values.iter().map(AsRef::as_ref).collect())),
        LogPredicate::FieldRegex { key, regex } => constraints.field_regex.push((key, regex)),
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => constraints.field_numeric.push((key, *comparison, *value)),
        LogPredicate::And(predicates) => {
            for predicate in predicates {
                collect_required_constraints(predicate, constraints, include_message_phrases);
            }
        }
        LogPredicate::Message(matcher) => {
            constraints.message_literals.push(matcher.value.as_ref());
            if matcher.case_sensitivity == CaseSensitivity::Sensitive {
                constraints
                    .case_sensitive_message_literals
                    .push(matcher.value.as_ref());
            }
        }
        LogPredicate::MessagePhrase { terms, .. } if include_message_phrases => {
            constraints.terms.extend(
                terms.iter().filter_map(|term| {
                    clickhouse_token_is_index_safe(term).then_some(term.as_ref())
                }),
            );
        }
        LogPredicate::MessagePhrase { .. } => {}
        LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::Or(_)
        | LogPredicate::Not(_) => {}
        LogPredicate::MessageRegex(regex) => {
            if let Some(literals) = regex_required_literals(regex.pattern()) {
                constraints
                    .message_literals
                    .extend(literals.iter().copied());
                if regex.case_sensitivity() == CaseSensitivity::Sensitive {
                    constraints.case_sensitive_message_literals.extend(literals);
                }
            }
        }
        LogPredicate::MessageTokenRegex(regex) => {
            if let Some(literals) = regex_required_literals(regex.pattern()) {
                constraints
                    .message_literals
                    .extend(literals.iter().copied());
                if regex.case_sensitivity() == CaseSensitivity::Sensitive {
                    constraints.case_sensitive_message_literals.extend(literals);
                }
            }
        }
    }
}

pub(crate) fn regex_required_literals(pattern: &str) -> Option<Vec<&str>> {
    let mut literals = Vec::new();
    let mut start = None;
    let mut escaped = false;
    let mut unsafe_pattern = false;
    for (index, byte) in pattern.bytes().enumerate() {
        if escaped {
            escaped = false;
            start = None;
            continue;
        }
        if byte == b'\\' {
            start = None;
            escaped = true;
            continue;
        }
        if byte.is_ascii_alphanumeric() {
            start.get_or_insert(index);
            continue;
        }
        let was_literal = start.is_some();
        if let Some(begin) = start.take() {
            literals.push(&pattern[begin..index]);
        }
        if matches!(byte, b'|' | b'(' | b')' | b'[' | b']' | b'{' | b'}')
            || (matches!(byte, b'?' | b'+' | b'*') && was_literal)
        {
            unsafe_pattern = true;
        }
    }
    if let Some(begin) = start {
        literals.push(&pattern[begin..]);
    }
    (!unsafe_pattern && !literals.is_empty()).then_some(literals)
}

fn predicate_is_index_only_conjunction(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MatchAll
        | LogPredicate::MatchNone
        | LogPredicate::Term(_)
        | LogPredicate::Field {
            matcher:
                TextMatcher {
                    kind: TextMatchKind::Exact,
                    case_sensitivity: CaseSensitivity::Sensitive,
                    ..
                },
            ..
        } => true,
        LogPredicate::And(predicates) => predicates.iter().all(predicate_is_index_only_conjunction),
        LogPredicate::MessageToken {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        } => true,
        LogPredicate::Not(predicate) => predicate_is_index_only_conjunction(predicate),
        LogPredicate::MessageToken { .. }
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. }
        | LogPredicate::Or(_) => false,
    }
}

fn predicate_matches<R: StructuralRecordView>(predicate: &LogPredicate, record: &R) -> bool {
    match predicate {
        LogPredicate::MatchAll => true,
        LogPredicate::MatchNone => false,
        LogPredicate::Term(term) => message_has_term(record.structural_message(), term),
        LogPredicate::MessageToken {
            value,
            case_sensitivity,
        } => message_has_clickhouse_token(record.structural_message(), value, *case_sensitivity),
        LogPredicate::Message(matcher) => text_matches(record.structural_message(), matcher),
        LogPredicate::MessageRegex(regex) => regex.is_match(record.structural_message()),
        LogPredicate::MessageTokenRegex(regex) => {
            message_has_token_regex(record.structural_message(), regex)
        }
        LogPredicate::MessageTokenPrefix {
            value,
            case_sensitivity,
        } => message_has_token_prefix(record.structural_message(), value, *case_sensitivity),
        LogPredicate::MessagePhrase {
            terms,
            max_gap,
            case_sensitivity,
        } => message_has_phrase(
            record.structural_message(),
            terms,
            *max_gap,
            *case_sensitivity,
        ),
        LogPredicate::MessageFuzzy {
            value,
            max_distance,
        } => message_has_fuzzy_token(record.structural_message(), value, *max_distance),
        LogPredicate::FieldExists(key) => {
            fields(record).any(|(observed, _)| observed == key.as_ref())
        }
        LogPredicate::Field { key, matcher } => fields(record).any(|(observed_key, value)| {
            observed_key == key.as_ref() && text_matches(value, matcher)
        }),
        LogPredicate::FieldIn { key, values } => fields(record).any(|(observed_key, value)| {
            observed_key == key.as_ref() && values.iter().any(|expected| value == expected.as_ref())
        }),
        LogPredicate::FieldRegex { key, regex } => fields(record)
            .any(|(observed_key, value)| observed_key == key.as_ref() && regex.is_match(value)),
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => fields(record).any(|(observed_key, observed)| {
            observed_key == key.as_ref()
                && observed
                    .parse::<i128>()
                    .is_ok_and(|observed| compare_numeric(observed, *comparison, *value))
        }),
        LogPredicate::And(predicates) => predicates
            .iter()
            .all(|predicate| predicate_matches(predicate, record)),
        LogPredicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| predicate_matches(predicate, record)),
        LogPredicate::Not(predicate) => !predicate_matches(predicate, record),
    }
}

pub(crate) fn predicate_fields_match(predicate: &LogPredicate, fields: &[MetadataField]) -> bool {
    match predicate {
        LogPredicate::MatchAll
        | LogPredicate::Term(_)
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. } => true,
        LogPredicate::MatchNone => false,
        LogPredicate::FieldExists(key) => fields
            .iter()
            .any(|field| field.key.as_ref() == key.as_ref()),
        LogPredicate::Field { key, matcher } => fields
            .iter()
            .any(|field| field.key.as_ref() == key.as_ref() && text_matches(&field.value, matcher)),
        LogPredicate::FieldIn { key, values } => fields.iter().any(|field| {
            field.key.as_ref() == key.as_ref()
                && values
                    .iter()
                    .any(|expected| field.value.as_ref() == expected.as_ref())
        }),
        LogPredicate::FieldRegex { key, regex } => fields
            .iter()
            .any(|field| field.key.as_ref() == key.as_ref() && regex.is_match(&field.value)),
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => fields.iter().any(|field| {
            field.key.as_ref() == key.as_ref()
                && field
                    .value
                    .parse::<i128>()
                    .is_ok_and(|observed| compare_numeric(observed, *comparison, *value))
        }),
        LogPredicate::And(predicates) => predicates
            .iter()
            .all(|predicate| predicate_fields_match(predicate, fields)),
        LogPredicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| predicate_fields_match(predicate, fields)),
        LogPredicate::Not(predicate) => !predicate_fields_match(predicate, fields),
    }
}

fn message_only_predicate_matches(predicate: &LogPredicate, message: &str) -> Option<bool> {
    match predicate {
        LogPredicate::MatchAll => Some(true),
        LogPredicate::MatchNone => Some(false),
        LogPredicate::Term(term) => Some(message_has_term(message, term)),
        LogPredicate::MessageToken {
            value,
            case_sensitivity,
        } => Some(message_has_clickhouse_token(
            message,
            value,
            *case_sensitivity,
        )),
        LogPredicate::Message(matcher) => Some(text_matches(message, matcher)),
        LogPredicate::MessageRegex(regex) => Some(regex.is_match(message)),
        LogPredicate::MessageTokenRegex(regex) => Some(message_has_token_regex(message, regex)),
        LogPredicate::MessageTokenPrefix {
            value,
            case_sensitivity,
        } => Some(message_has_token_prefix(message, value, *case_sensitivity)),
        LogPredicate::MessagePhrase {
            terms,
            max_gap,
            case_sensitivity,
        } => Some(message_has_phrase(
            message,
            terms,
            *max_gap,
            *case_sensitivity,
        )),
        LogPredicate::MessageFuzzy {
            value,
            max_distance,
        } => Some(message_has_fuzzy_token(message, value, *max_distance)),
        LogPredicate::And(predicates) => {
            let mut matched = true;
            for predicate in predicates {
                matched &= message_only_predicate_matches(predicate, message)?;
            }
            Some(matched)
        }
        LogPredicate::Or(predicates) => {
            let mut matched = false;
            for predicate in predicates {
                matched |= message_only_predicate_matches(predicate, message)?;
            }
            Some(matched)
        }
        LogPredicate::Not(predicate) => {
            message_only_predicate_matches(predicate, message).map(|value| !value)
        }
        LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. } => None,
    }
}

fn fields<R: StructuralRecordView>(record: &R) -> impl Iterator<Item = (&str, &str)> {
    (0..record.structural_field_count()).filter_map(|index| record.structural_field(index))
}

fn field_equals<R: StructuralRecordView>(record: &R, expected: &MetadataField) -> bool {
    fields(record)
        .any(|(key, value)| key == expected.key.as_ref() && value == expected.value.as_ref())
}

pub(crate) fn message_has_term(message: &str, expected: &str) -> bool {
    let mut matched = false;
    let _ = analyze_message(message, &[], |term| {
        matched |= text_equal(term, expected, CaseSensitivity::Insensitive);
    });
    matched
}

#[derive(Clone, Copy)]
struct ClickhouseTokenIter<'a> {
    message: &'a str,
    offset: usize,
}

impl<'a> ClickhouseTokenIter<'a> {
    fn new(message: &'a str) -> Self {
        Self { message, offset: 0 }
    }
}

impl<'a> Iterator for ClickhouseTokenIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.message.as_bytes();
        while self.offset < bytes.len() && clickhouse_token_separator(bytes[self.offset]) {
            self.offset += 1;
        }
        let start = self.offset;
        while self.offset < bytes.len() && !clickhouse_token_separator(bytes[self.offset]) {
            self.offset += 1;
        }
        (start < self.offset).then(|| &self.message[start..self.offset])
    }
}

pub(crate) fn scan_clickhouse_tokens(message: &str, mut on_token: impl FnMut(&str)) {
    for token in ClickhouseTokenIter::new(message) {
        on_token(token);
    }
}

pub(crate) fn message_has_clickhouse_token(
    message: &str,
    expected: &str,
    case_sensitivity: CaseSensitivity,
) -> bool {
    let expected = expected.as_bytes();
    if expected.is_empty() || expected.iter().copied().any(clickhouse_token_separator) {
        return false;
    }
    let message = message.as_bytes();
    if expected.len() > message.len() {
        return false;
    }
    message
        .windows(expected.len())
        .enumerate()
        .any(|(start, candidate)| {
            let bytes_match = match case_sensitivity {
                CaseSensitivity::Sensitive => candidate == expected,
                CaseSensitivity::Insensitive => candidate.eq_ignore_ascii_case(expected),
            };
            bytes_match
                && (start == 0 || clickhouse_token_separator(message[start - 1]))
                && (start + expected.len() == message.len()
                    || clickhouse_token_separator(message[start + expected.len()]))
        })
}

pub(crate) fn message_has_token_prefix(
    message: &str,
    prefix: &str,
    case_sensitivity: CaseSensitivity,
) -> bool {
    if prefix.is_empty() || prefix.bytes().any(clickhouse_token_separator) {
        return false;
    }
    let mut matched = false;
    scan_clickhouse_tokens(message, |token| {
        matched |= match case_sensitivity {
            CaseSensitivity::Sensitive => token.starts_with(prefix),
            CaseSensitivity::Insensitive => token
                .get(..prefix.len())
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix)),
        };
    });
    matched
}

pub(crate) fn message_has_token_regex(message: &str, regex: &crate::LogRegex) -> bool {
    let mut matched = false;
    scan_clickhouse_tokens(message, |token| matched |= regex.is_match(token));
    matched
}

pub(crate) fn message_has_phrase(
    message: &str,
    terms: &[std::sync::Arc<str>],
    max_gap: usize,
    case_sensitivity: CaseSensitivity,
) -> bool {
    if terms.is_empty() {
        return true;
    }
    let matches_term = |token: &str, term: &str| match case_sensitivity {
        CaseSensitivity::Sensitive => token == term,
        CaseSensitivity::Insensitive => token.eq_ignore_ascii_case(term),
    };
    if max_gap == 0 {
        let first = terms[0].as_ref();
        let mut next = 0usize;
        for token in ClickhouseTokenIter::new(message) {
            if matches_term(token, &terms[next]) {
                next += 1;
                if next == terms.len() {
                    return true;
                }
            } else {
                next = usize::from(matches_term(token, first));
            }
        }
        return false;
    }
    let mut starts = ClickhouseTokenIter::new(message);
    while let Some(first) = starts.next() {
        if !matches_term(first, &terms[0]) {
            continue;
        }
        let mut remaining = starts;
        let mut matched = true;
        for term in &terms[1..] {
            let mut found = false;
            for _ in 0..=max_gap {
                let Some(candidate) = remaining.next() else {
                    break;
                };
                if matches_term(candidate, term) {
                    found = true;
                    break;
                }
            }
            if !found {
                matched = false;
                break;
            }
        }
        if matched {
            return true;
        }
    }
    false
}

pub(crate) fn message_has_fuzzy_token(message: &str, value: &str, max_distance: u8) -> bool {
    if value.is_empty() {
        return false;
    }
    let mut matched = false;
    scan_clickhouse_tokens(message, |token| {
        matched |= bounded_levenshtein(token, value, usize::from(max_distance));
    });
    matched
}

pub(crate) fn bounded_levenshtein(left: &str, right: &str, max_distance: usize) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len().abs_diff(right.len()) > max_distance {
        return false;
    }
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0; right.len() + 1];
    for (left_index, left_byte) in left.iter().copied().enumerate() {
        current[0] = left_index + 1;
        let mut row_min = current[0];
        for (right_index, right_byte) in right.iter().copied().enumerate() {
            let substitution = if left_byte.eq_ignore_ascii_case(&right_byte) {
                previous[right_index]
            } else {
                previous[right_index] + 1
            };
            current[right_index + 1] =
                (substitution.min(previous[right_index + 1] + 1)).min(current[right_index] + 1);
            row_min = row_min.min(current[right_index + 1]);
        }
        if row_min > max_distance {
            return false;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[right.len()] <= max_distance
}

fn clickhouse_token_is_index_safe(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

pub(crate) const fn clickhouse_token_separator(byte: u8) -> bool {
    byte.is_ascii() && !(byte.is_ascii_alphanumeric() || byte == b'_')
}

pub(crate) fn text_matches(observed: &str, matcher: &TextMatcher) -> bool {
    match matcher.kind {
        TextMatchKind::Exact => text_equal(observed, &matcher.value, matcher.case_sensitivity),
        TextMatchKind::Contains => {
            text_contains(observed, &matcher.value, matcher.case_sensitivity)
        }
        TextMatchKind::Prefix => text_prefix(observed, &matcher.value, matcher.case_sensitivity),
        TextMatchKind::Suffix => text_suffix(observed, &matcher.value, matcher.case_sensitivity),
    }
}

fn text_equal(left: &str, right: &str, case_sensitivity: CaseSensitivity) -> bool {
    match case_sensitivity {
        CaseSensitivity::Sensitive => left == right,
        CaseSensitivity::Insensitive if left.is_ascii() && right.is_ascii() => {
            left.eq_ignore_ascii_case(right)
        }
        CaseSensitivity::Insensitive => left.to_lowercase() == right.to_lowercase(),
    }
}

fn text_contains(haystack: &str, needle: &str, case_sensitivity: CaseSensitivity) -> bool {
    if needle.is_empty() {
        return true;
    }
    match case_sensitivity {
        CaseSensitivity::Sensitive => haystack.contains(needle),
        CaseSensitivity::Insensitive if haystack.is_ascii() && needle.is_ascii() => haystack
            .as_bytes()
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle.as_bytes())),
        CaseSensitivity::Insensitive => haystack.to_lowercase().contains(&needle.to_lowercase()),
    }
}

fn text_prefix(observed: &str, prefix: &str, case_sensitivity: CaseSensitivity) -> bool {
    match case_sensitivity {
        CaseSensitivity::Sensitive => observed.starts_with(prefix),
        CaseSensitivity::Insensitive if observed.is_ascii() && prefix.is_ascii() => observed
            .get(..prefix.len())
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix)),
        CaseSensitivity::Insensitive => observed.to_lowercase().starts_with(&prefix.to_lowercase()),
    }
}

fn text_suffix(observed: &str, suffix: &str, case_sensitivity: CaseSensitivity) -> bool {
    match case_sensitivity {
        CaseSensitivity::Sensitive => observed.ends_with(suffix),
        CaseSensitivity::Insensitive if observed.is_ascii() && suffix.is_ascii() => observed
            .len()
            .checked_sub(suffix.len())
            .and_then(|start| observed.get(start..))
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(suffix)),
        CaseSensitivity::Insensitive => observed.to_lowercase().ends_with(&suffix.to_lowercase()),
    }
}

const fn compare_numeric(observed: i128, comparison: NumericComparison, expected: i128) -> bool {
    match comparison {
        NumericComparison::Equal => observed == expected,
        NumericComparison::NotEqual => observed != expected,
        NumericComparison::LessThan => observed < expected,
        NumericComparison::LessThanOrEqual => observed <= expected,
        NumericComparison::GreaterThan => observed > expected,
        NumericComparison::GreaterThanOrEqual => observed >= expected,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

    use super::*;
    use crate::{CompressionCohortId, DurableLog, LogRegex};

    fn record(message: &str) -> DurableLog {
        DurableLog::new(
            ShardId::new(1),
            TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(2)),
            LogicalOffset::new(7),
            99,
            message,
            CompressionCohortId::new(1),
        )
        .with_field("service", "Paiements")
        .with_field("status", "503")
    }

    #[test]
    fn literal_regex_numeric_and_boolean_predicates_are_exact() {
        let record = record("ÉCHEC checkout timed out");
        let predicate = LogPredicate::and(vec![
            LogPredicate::message_contains("échec checkout"),
            LogPredicate::message_regex("timed\\s+out$", CaseSensitivity::Insensitive)
                .expect("regex compiles"),
            LogPredicate::field(
                "service",
                TextMatcher::new("paie", TextMatchKind::Prefix, CaseSensitivity::Insensitive),
            ),
            LogPredicate::field_numeric("status", NumericComparison::GreaterThanOrEqual, 500),
            LogPredicate::negate(LogPredicate::field_equals("service", "worker")),
        ]);
        assert!(predicate_matches(&predicate, &record));
        assert!(!predicate_matches(
            &LogPredicate::message_contains("success"),
            &record
        ));
        assert!(predicate_matches(
            &LogPredicate::field_numeric("status", NumericComparison::Equal, 503),
            &record
        ));
        assert!(predicate_matches(
            &LogPredicate::field_numeric("status", NumericComparison::NotEqual, 200),
            &record
        ));
    }

    #[test]
    fn invalid_regular_expressions_are_rejected_at_query_construction() {
        let error =
            LogRegex::new("(", CaseSensitivity::Sensitive).expect_err("invalid regex is rejected");
        assert!(matches!(error, crate::TelemetryError::InvalidQuery(_)));
    }

    #[test]
    fn empty_literal_contains_matches_without_panicking() {
        assert!(text_contains("anything", "", CaseSensitivity::Insensitive));
    }

    #[test]
    fn clickhouse_tokens_preserve_case_and_ascii_boundaries() {
        assert!(message_has_clickhouse_token(
            "prefix Cannot suffix",
            "Cannot",
            CaseSensitivity::Sensitive
        ));
        assert!(!message_has_clickhouse_token(
            "prefix Cannot suffix",
            "cannot",
            CaseSensitivity::Sensitive
        ));
        assert!(message_has_clickhouse_token(
            "prefix Cannot suffix",
            "cannot",
            CaseSensitivity::Insensitive
        ));
        assert!(!message_has_clickhouse_token(
            "prefix_cannot suffix",
            "cannot",
            CaseSensitivity::Sensitive
        ));
        assert!(!message_has_clickhouse_token(
            "prefix écannot suffix",
            "cannot",
            CaseSensitivity::Sensitive
        ));
    }

    #[test]
    fn clickhouse_token_uses_the_case_folded_index_only_as_a_candidate_filter() {
        let query = LogQuery::new(record("unused").record_ref.topic_partition).where_predicate(
            LogPredicate::message_token("Cannot", CaseSensitivity::Sensitive),
        );
        let constraints = query.required_index_constraints();
        assert_eq!(constraints.terms, ["Cannot"]);
        assert!(query.requires_post_decode());
    }

    #[test]
    fn case_insensitive_ascii_tokens_can_use_message_only_filter() {
        let query = LogQuery::new(record("unused").record_ref.topic_partition).where_predicate(
            LogPredicate::message_token("Cannot", CaseSensitivity::Insensitive),
        );
        assert!(query.can_use_message_only_filter());
        assert!(!query.requires_post_decode());
    }

    #[test]
    fn exact_frame_token_fast_path_accepts_both_case_modes_and_timestamp_sort() {
        let query = LogQuery::new(record("unused").record_ref.topic_partition)
            .where_predicate(LogPredicate::and(vec![
                LogPredicate::message_token("Cannot", CaseSensitivity::Sensitive),
                LogPredicate::message_token("checkout", CaseSensitivity::Insensitive),
            ]))
            .sort_by_timestamp();
        assert_eq!(
            query.exact_message_token_conjunction(),
            Some(vec![
                ("Cannot", CaseSensitivity::Sensitive),
                ("checkout", CaseSensitivity::Insensitive),
            ])
        );
        assert!(
            LogQuery::new(record("unused").record_ref.topic_partition)
                .with_term("Cannot")
                .exact_message_token_conjunction()
                .is_none()
        );
        assert!(
            LogQuery::new(record("unused").record_ref.topic_partition)
                .where_predicate(LogPredicate::message_token(
                    "two words",
                    CaseSensitivity::Sensitive,
                ))
                .exact_message_token_conjunction()
                .is_none()
        );
    }

    #[test]
    fn token_phrase_prefix_regex_and_fuzzy_predicates_match_message_tokens() {
        let message = "failed to place order connection";
        assert!(message_has_token_prefix(
            message,
            "conn",
            CaseSensitivity::Insensitive
        ));
        let regex = LogRegex::new("charg.*", CaseSensitivity::Insensitive).unwrap();
        assert!(message_has_token_regex("charge failed", &regex));
        assert!(message_has_phrase(
            message,
            &[Arc::from("failed"), Arc::from("order")],
            2,
            CaseSensitivity::Insensitive
        ));
        assert!(!message_has_phrase(
            message,
            &[Arc::from("failed"), Arc::from("order")],
            1,
            CaseSensitivity::Insensitive
        ));
        assert!(message_has_fuzzy_token(
            "connection refused",
            "conection",
            1
        ));
        assert!(!message_has_fuzzy_token(
            "connection refused",
            "conection",
            0
        ));
    }
}
