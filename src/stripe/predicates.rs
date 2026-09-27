use super::*;

pub(super) fn hot_predicate_candidates(
    predicate: &LogPredicate,
    partition: &PartitionIndex,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    if let Some((tokens, minimum)) = message_token_min_match_shape(predicate) {
        return Some(hot_message_token_min_match_candidates(
            partition, &tokens, minimum, start, end,
        ));
    }
    match predicate {
        LogPredicate::MatchAll => Some((start..end).collect()),
        LogPredicate::MatchNone => Some(Vec::new()),
        LogPredicate::Term(term) => {
            let normalized = normalize_term(term);
            let Some(term_id) = partition.term_ids.get(normalized.as_ref()) else {
                return Some(Vec::new());
            };
            let Some(postings) = partition.term_postings.get(*term_id) else {
                return Some(Vec::new());
            };
            Some(postings.collect_in(start, end, QueryOrder::OldestFirst, None))
        }
        LogPredicate::MessageToken { value, .. } => {
            if value.is_empty() || value.bytes().any(|byte| !byte.is_ascii_alphanumeric()) {
                return Some(Vec::new());
            }
            let normalized = normalize_term(value);
            let Some(term_id) = partition.term_ids.get(normalized.as_ref()) else {
                return Some(Vec::new());
            };
            let Some(postings) = partition.term_postings.get(*term_id) else {
                return Some(Vec::new());
            };
            Some(postings.collect_in(start, end, QueryOrder::OldestFirst, None))
        }
        LogPredicate::MessageTokenPrefix { value, .. } => {
            if value.is_empty() || value.bytes().any(|byte| !byte.is_ascii_alphanumeric()) {
                return Some(Vec::new());
            }
            let prefix = normalize_term(value);
            hot_message_token_candidates(partition, start, end, |token| {
                token.starts_with(prefix.as_ref())
            })
        }
        LogPredicate::MessageTokenRegex(regex) => {
            if regex.case_sensitivity() == CaseSensitivity::Sensitive
                && regex
                    .pattern()
                    .bytes()
                    .any(|byte| byte.is_ascii_uppercase())
            {
                return Some((start..end).collect());
            }
            hot_message_token_candidates(partition, start, end, |token| regex.is_match(token))
        }
        LogPredicate::MessagePhrase { terms, .. } => {
            hot_message_phrase_candidates(partition, terms, start, end)
        }
        LogPredicate::MessageFuzzy {
            value,
            max_distance,
        } => {
            if value.is_empty() || value.bytes().any(|byte| !byte.is_ascii_alphanumeric()) {
                return Some(Vec::new());
            }
            let value = normalize_term(value);
            hot_message_token_candidates(partition, start, end, |token| {
                bounded_levenshtein(token, value.as_ref(), usize::from(*max_distance))
            })
        }
        LogPredicate::FieldExists(key) => Some(
            partition
                .field_presence_postings
                .get(key)
                .map(|postings| postings.collect_in(start, end, QueryOrder::OldestFirst, None))
                .unwrap_or_default(),
        ),
        LogPredicate::Field { key, matcher } => {
            hot_field_text_candidates(partition, key, matcher, start, end)
        }
        LogPredicate::FieldIn { key, values } => {
            // Merge the value postings directly. Materializing one ordinal
            // vector per requested value only to union them doubles the
            // allocation and copy work for common two-value filters.
            let Some(value_ids) = partition.field_ids.get(key) else {
                return Some(Vec::new());
            };
            let mut field_postings = Vec::with_capacity(values.len());
            for value in values {
                if let Some(field_id) = value_ids.get(value.as_ref())
                    && let Some(posting) = partition.field_postings.get(*field_id)
                {
                    field_postings.push(posting);
                }
            }
            Some(collect_hot_posting_union(&field_postings, start, end, None))
        }
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => hot_numeric_field_candidates(partition, key, *comparison, *value, start, end),
        LogPredicate::And(predicates) => {
            let mut current = None;
            for predicate in predicates {
                if matches!(predicate, LogPredicate::MatchAll) {
                    continue;
                }
                let candidates = hot_predicate_candidates(predicate, partition, start, end)?;
                intersect_frame_candidate_slice(&mut current, &candidates);
                if current.as_ref().is_some_and(Vec::is_empty) {
                    return Some(Vec::new());
                }
            }
            Some(current.unwrap_or_else(|| (start..end).collect()))
        }
        LogPredicate::Or(predicates) => {
            let mut candidates = Vec::new();
            for predicate in predicates {
                if matches!(predicate, LogPredicate::MatchNone) {
                    continue;
                }
                if matches!(predicate, LogPredicate::MatchAll) {
                    return Some((start..end).collect());
                }
                union_sorted_ordinals(
                    &mut candidates,
                    hot_predicate_candidates(predicate, partition, start, end)?,
                );
            }
            Some(candidates)
        }
        LogPredicate::Not(predicate) if hot_predicate_candidates_are_exact(predicate) => {
            let excluded = hot_predicate_candidates(predicate, partition, start, end)?;
            let mut candidates = Vec::with_capacity(
                (end.saturating_sub(start) as usize).saturating_sub(excluded.len()),
            );
            let mut next = start;
            for ordinal in excluded {
                if ordinal < next || ordinal >= end {
                    continue;
                }
                candidates.extend(next..ordinal);
                next = ordinal.saturating_add(1);
            }
            if next < end {
                candidates.extend(next..end);
            }
            Some(candidates)
        }
        LogPredicate::Message(matcher) => {
            hot_message_literal_candidates(partition, matcher, start, end)
        }
        LogPredicate::MessageRegex(regex) => {
            hot_message_regex_token_candidates(partition, regex, start, end)
        }
        LogPredicate::Not(_) => None,
        LogPredicate::FieldRegex { key, regex } => hot_field_predicate_candidates(
            partition,
            key,
            |value| regex.is_match(value),
            start,
            end,
        ),
    }
}

pub(super) fn hot_message_literal_candidates(
    partition: &PartitionIndex,
    matcher: &crate::TextMatcher,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    if matcher.value.is_empty() {
        return None;
    }
    if matcher.case_sensitivity == CaseSensitivity::Insensitive
        && !partition.message_trigram_ascii_only
    {
        // The resident token/trigram indexes only implement ASCII folding.
        // Once a partition contains Unicode, retain the exact Unicode scan
        // for every insensitive literal rather than risking a false negative.
        return None;
    }
    if !matcher.value.is_ascii() {
        // Token and resident-trigram indexes use ASCII boundary/folding
        // rules. Unicode literals stay on the exact residual matcher.
        return None;
    }
    if matcher.kind == crate::TextMatchKind::Contains
        && matcher.value.len() >= 3
        && (matcher.case_sensitivity == CaseSensitivity::Sensitive
            || partition.message_trigram_ascii_only)
        && let Some(candidates) =
            hot_message_trigram_candidates(partition, &matcher.value, start, end)
    {
        return Some(candidates);
    }
    let bytes = matcher.value.as_bytes();
    let mut runs = Vec::new();
    let mut run_start = None;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if byte.is_ascii_alphanumeric() {
            run_start.get_or_insert(index);
        } else if let Some(begin) = run_start.take() {
            runs.push((begin, index));
        }
    }
    if let Some(begin) = run_start {
        runs.push((begin, bytes.len()));
    }
    if runs.len() != 1 {
        return None;
    }
    let (begin, finish) = runs[0];
    let leading_boundary = begin > 0
        && bytes[..begin]
            .iter()
            .all(|byte| !byte.is_ascii_alphanumeric());
    let trailing_boundary = finish < bytes.len()
        && bytes[finish..]
            .iter()
            .all(|byte| !byte.is_ascii_alphanumeric());
    let term = normalize_term(&matcher.value[begin..finish]);
    match matcher.kind {
        crate::TextMatchKind::Contains if leading_boundary && trailing_boundary => {
            hot_predicate_candidates(
                &LogPredicate::Term(Arc::from(term.as_ref())),
                partition,
                start,
                end,
            )
        }
        crate::TextMatchKind::Contains if leading_boundary => {
            hot_message_token_candidates(partition, start, end, |token| {
                token.starts_with(term.as_ref())
            })
        }
        crate::TextMatchKind::Contains if trailing_boundary => {
            hot_message_token_candidates(partition, start, end, |token| {
                token.ends_with(term.as_ref())
            })
        }
        crate::TextMatchKind::Contains => {
            // The resident directory folds ASCII bytes only. Unicode
            // case-insensitive matching uses full lowercase expansion, so it
            // must retain the verified scan unless the caller is sensitive.
            (matcher.case_sensitivity == CaseSensitivity::Sensitive
                || partition.message_trigram_ascii_only)
                .then(|| hot_message_trigram_candidates(partition, &matcher.value, start, end))
                .flatten()
        }
        crate::TextMatchKind::Prefix => {
            hot_message_token_candidates(partition, start, end, |token| {
                token.starts_with(term.as_ref())
            })
        }
        crate::TextMatchKind::Suffix => {
            hot_message_token_candidates(partition, start, end, |token| {
                token.ends_with(term.as_ref())
            })
        }
        crate::TextMatchKind::Exact => None,
    }
}

pub(super) fn regex_boundary_safe_literal(pattern: &str) -> Option<&str> {
    let bytes = pattern.as_bytes();
    let mut run_start = None;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if byte == b'b' && index > 0 && bytes[index - 1] == b'\\' {
            continue;
        }
        if byte.is_ascii_alphanumeric() {
            run_start.get_or_insert(index);
            continue;
        }
        let Some(begin) = run_start.take() else {
            continue;
        };
        let left_boundary = begin == 0
            || bytes[begin - 1] == b'^'
            || (begin >= 2 && bytes[begin - 2..begin] == *b"\\b");
        let right_boundary = index == bytes.len()
            || bytes[index] == b'$'
            || (index + 2 <= bytes.len() && bytes[index..index + 2] == *b"\\b");
        if left_boundary && right_boundary {
            return Some(&pattern[begin..index]);
        }
    }
    let begin = run_start?;
    let left_boundary = begin == 0
        || bytes[begin - 1] == b'^'
        || (begin >= 2 && bytes[begin - 2..begin] == *b"\\b");
    if left_boundary {
        return Some(&pattern[begin..]);
    }
    None
}

pub(super) fn hot_message_regex_token_candidates(
    partition: &PartitionIndex,
    regex: &crate::LogRegex,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    let literal = regex_boundary_safe_literal(regex.pattern())?;
    let term_id = partition.term_ids.get(normalize_term(literal).as_ref())?;
    let postings = partition.term_postings.get(*term_id)?;
    Some(postings.collect_in(start, end, QueryOrder::OldestFirst, None))
}

pub(super) fn cached_message_predicate_is_exact(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MatchAll | LogPredicate::MatchNone => true,
        LogPredicate::MessageToken {
            case_sensitivity, ..
        } => *case_sensitivity == CaseSensitivity::Insensitive,
        LogPredicate::MessageTokenRegex(regex) => {
            regex.case_sensitivity() == CaseSensitivity::Insensitive
        }
        LogPredicate::MessageTokenPrefix {
            case_sensitivity, ..
        } => *case_sensitivity == CaseSensitivity::Insensitive,
        LogPredicate::MessagePhrase { .. } => true,
        LogPredicate::MessageFuzzy { .. } => true,
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            !predicates.is_empty() && predicates.iter().all(cached_message_predicate_is_exact)
        }
        LogPredicate::Not(predicate) => cached_message_predicate_is_exact(predicate),
        _ => false,
    }
}

pub(super) fn message_predicate_is_message_only(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MatchAll
        | LogPredicate::MatchNone
        | LogPredicate::Term(_)
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. } => true,
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            predicates.iter().all(message_predicate_is_message_only)
        }
        LogPredicate::Not(predicate) => message_predicate_is_message_only(predicate),
        LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. } => false,
    }
}

pub(super) fn cached_message_predicate_candidates_are_cheap(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MessageToken {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        } => true,
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            !predicates.is_empty()
                && predicates
                    .iter()
                    .all(cached_message_predicate_candidates_are_cheap)
        }
        LogPredicate::Not(predicate) => cached_message_predicate_candidates_are_cheap(predicate),
        _ => false,
    }
}

pub(super) fn message_cache_can_supply_frame_base(query: &LogQuery, tenant: &str) -> bool {
    query.terms.is_empty()
        && query.exact_fields.iter().all(|field| {
            field.key.as_ref() == "resource.loki.tenant" && field.value.as_ref() == tenant
        })
        && cached_message_predicate_candidates_are_cheap(&query.predicate)
}

pub(super) fn predicate_is_indexed_conjunction(predicate: &LogPredicate) -> bool {
    fn indexed_message_atom(predicate: &LogPredicate) -> bool {
        match predicate {
            LogPredicate::Term(_) => true,
            LogPredicate::MessageToken {
                case_sensitivity: CaseSensitivity::Insensitive,
                ..
            }
            | LogPredicate::MessageTokenPrefix {
                case_sensitivity: CaseSensitivity::Insensitive,
                ..
            } => true,
            LogPredicate::MessageTokenRegex(regex)
                if regex.case_sensitivity() == CaseSensitivity::Insensitive =>
            {
                true
            }
            _ => false,
        }
    }

    match predicate {
        LogPredicate::MatchAll
        | LogPredicate::Term(_)
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. } => true,
        LogPredicate::MessageToken {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        }
        | LogPredicate::MessageTokenPrefix {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        } => true,
        LogPredicate::MessageTokenRegex(regex)
            if regex.case_sensitivity() == CaseSensitivity::Insensitive =>
        {
            true
        }
        LogPredicate::Or(predicates) => {
            !predicates.is_empty() && predicates.iter().all(indexed_message_atom)
        }
        LogPredicate::And(predicates) => predicates.iter().all(predicate_is_indexed_conjunction),
        _ => false,
    }
}

pub(super) fn hot_message_token_candidates(
    partition: &PartitionIndex,
    start: u32,
    end: u32,
    mut matches_token: impl FnMut(&str) -> bool,
) -> Option<Vec<u32>> {
    let mut postings = Vec::new();
    for (token, term_id) in &partition.term_ids {
        if matches_token(token)
            && let Some(posting) = partition.term_postings.get(*term_id)
        {
            postings.push(posting);
        }
    }
    Some(collect_hot_posting_union(&postings, start, end, None))
}

pub(super) fn hot_message_trigram_candidates(
    partition: &PartitionIndex,
    literal: &str,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    if !partition.message_trigram_index_complete {
        return None;
    }
    let keys = collect_message_trigram_keys(literal);
    if keys.is_empty() {
        return None;
    }
    let mut postings = Vec::with_capacity(keys.len());
    for key in keys {
        let Some(posting) = partition.message_trigram_postings.get(&key) else {
            return Some(Vec::new());
        };
        postings.push(posting);
    }
    Some(collect_hot_posting_intersection(
        &postings,
        start,
        end,
        QueryOrder::OldestFirst,
        None,
    ))
}

pub(super) fn hot_message_phrase_candidates(
    partition: &PartitionIndex,
    terms: &[Arc<str>],
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    if terms.is_empty() {
        return Some((start..end).collect());
    }
    let mut current = None;
    for term in terms {
        let normalized = normalize_term(term);
        let Some(term_id) = partition.term_ids.get(normalized.as_ref()) else {
            return Some(Vec::new());
        };
        let Some(postings) = partition.term_postings.get(*term_id) else {
            return Some(Vec::new());
        };
        let candidates = postings.collect_in(start, end, QueryOrder::OldestFirst, None);
        intersect_frame_candidate_slice(&mut current, &candidates);
        if current.as_ref().is_some_and(Vec::is_empty) {
            return Some(Vec::new());
        }
    }
    Some(current.unwrap_or_default())
}

pub(super) fn hot_predicate_postings<'a>(
    predicate: &LogPredicate,
    partition: &'a PartitionIndex,
) -> Option<Vec<&'a HotPostingList>> {
    match predicate {
        LogPredicate::MatchNone => Some(Vec::new()),
        LogPredicate::Term(term) => Some(
            partition
                .term_ids
                .get(normalize_term(term).as_ref())
                .and_then(|term_id| partition.term_postings.get(*term_id))
                .into_iter()
                .collect(),
        ),
        LogPredicate::MessageToken {
            value,
            case_sensitivity: CaseSensitivity::Insensitive,
        } => Some(
            partition
                .term_ids
                .get(normalize_term(value).as_ref())
                .and_then(|term_id| partition.term_postings.get(*term_id))
                .into_iter()
                .collect(),
        ),
        LogPredicate::FieldExists(key) => Some(
            partition
                .field_presence_postings
                .get(key)
                .into_iter()
                .collect(),
        ),
        LogPredicate::Field { key, matcher } => Some(
            partition
                .field_ids
                .get(key)
                .into_iter()
                .flat_map(|values| values.iter())
                .filter(|(value, _)| text_matches(value, matcher))
                .filter_map(|(_, field_id)| partition.field_postings.get(*field_id))
                .collect(),
        ),
        LogPredicate::FieldIn { key, values } => Some(
            partition
                .field_ids
                .get(key)
                .into_iter()
                .flat_map(|field_ids| {
                    values
                        .iter()
                        .filter_map(|value| field_ids.get(value.as_ref()))
                })
                .filter_map(|field_id| partition.field_postings.get(*field_id))
                .collect(),
        ),
        LogPredicate::FieldRegex { key, regex } => Some(
            partition
                .field_ids
                .get(key)
                .into_iter()
                .flat_map(|values| values.iter())
                .filter(|(value, _)| regex.is_match(value))
                .filter_map(|(_, field_id)| partition.field_postings.get(*field_id))
                .collect(),
        ),
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => Some(
            partition
                .numeric_field_values
                .get(key)
                .into_iter()
                .flat_map(|values| values.iter())
                .filter(|(observed, _)| numeric_comparison_matches(*comparison, *observed, *value))
                .filter_map(|(_, field_id)| partition.field_postings.get(*field_id))
                .collect(),
        ),
        LogPredicate::Or(predicates) => {
            let mut postings = Vec::new();
            for predicate in predicates {
                postings.extend(hot_predicate_postings(predicate, partition)?);
            }
            Some(postings)
        }
        _ => None,
    }
}

pub(super) fn visit_hot_predicate_candidates(
    predicate: &LogPredicate,
    partition: &PartitionIndex,
    start: u32,
    end: u32,
    visit: impl FnMut(u32) -> bool,
) -> Option<bool> {
    let postings = hot_predicate_postings(predicate, partition)?;
    Some(visit_hot_posting_union(&postings, start, end, visit))
}

pub(super) fn optimized_hot_predicate_candidates(
    predicate: &LogPredicate,
    partition: &PartitionIndex,
    start: u32,
    end: u32,
    limit: Option<usize>,
) -> Option<Vec<u32>> {
    if limit.is_none() {
        return hot_predicate_candidates(predicate, partition, start, end);
    }
    if !hot_predicate_candidates_are_exact(predicate) {
        return hot_predicate_candidates(predicate, partition, start, end);
    }
    if let LogPredicate::And(predicates) = predicate
        && predicates.len() >= 2
    {
        let driver_index = predicates
            .iter()
            .enumerate()
            .filter(|(_, child)| hot_predicate_postings(child, partition).is_some())
            .min_by_key(|(_, child)| hot_predicate_cardinality(child, partition, start, end))
            .map(|(index, _)| index);
        if let Some(driver_index) = driver_index {
            let take = limit.unwrap_or(usize::MAX);
            let mut selected = Vec::with_capacity(take.min(hot_predicate_cardinality(
                &predicates[driver_index],
                partition,
                start,
                end,
            )));
            if visit_hot_predicate_candidates(
                &predicates[driver_index],
                partition,
                start,
                end,
                |ordinal| {
                    if predicates.iter().enumerate().all(|(index, child)| {
                        index == driver_index
                            || hot_predicate_matches_ordinal(child, partition, ordinal)
                    }) {
                        selected.push(ordinal);
                    }
                    selected.len() < take
                },
            )
            .is_some()
            {
                return Some(selected);
            }
        }
    }
    if let Some(driver) = hot_predicate_driver_posting(predicate, partition, start, end) {
        let take = limit.unwrap_or(usize::MAX);
        let mut selected = Vec::with_capacity(take.min(driver.cardinality_in(start, end)));
        driver.visit_in(start, end, QueryOrder::OldestFirst, |ordinal| {
            if hot_predicate_matches_ordinal(predicate, partition, ordinal) {
                selected.push(ordinal);
            }
            selected.len() < take
        });
        return Some(selected);
    }
    let LogPredicate::And(predicates) = predicate else {
        let mut candidates = hot_predicate_candidates(predicate, partition, start, end)?;
        if let Some(limit) = limit {
            candidates.truncate(limit);
        }
        return Some(candidates);
    };
    if predicates.len() < 2 {
        let mut candidates = hot_predicate_candidates(predicate, partition, start, end)?;
        if let Some(limit) = limit {
            candidates.truncate(limit);
        }
        return Some(candidates);
    }

    let driver_index = predicates
        .iter()
        .enumerate()
        .min_by_key(|(_, child)| hot_predicate_cardinality(child, partition, start, end))
        .map(|(index, _)| index)?;
    let take = limit.unwrap_or(usize::MAX);
    let mut selected = Vec::new();
    if visit_hot_predicate_candidates(
        &predicates[driver_index],
        partition,
        start,
        end,
        |ordinal| {
            if predicates.iter().enumerate().all(|(index, child)| {
                index == driver_index || hot_predicate_matches_ordinal(child, partition, ordinal)
            }) {
                selected.push(ordinal);
            }
            selected.len() < take
        },
    )
    .is_some()
    {
        return Some(selected);
    }

    let mut driver = hot_predicate_candidates(&predicates[driver_index], partition, start, end)?;
    selected.reserve(take.min(driver.len()));
    for ordinal in driver.drain(..) {
        if predicates.iter().enumerate().all(|(index, child)| {
            index == driver_index || hot_predicate_matches_ordinal(child, partition, ordinal)
        }) {
            selected.push(ordinal);
            if selected.len() == take {
                break;
            }
        }
    }
    Some(selected)
}

pub(super) fn hot_predicate_driver_posting<'a>(
    predicate: &LogPredicate,
    partition: &'a PartitionIndex,
    start: u32,
    end: u32,
) -> Option<&'a HotPostingList> {
    match predicate {
        LogPredicate::Term(term) => partition
            .term_ids
            .get(normalize_term(term).as_ref())
            .and_then(|term_id| partition.term_postings.get(*term_id)),
        LogPredicate::FieldExists(key) => partition.field_presence_postings.get(key),
        LogPredicate::Field { key, matcher } => {
            let mut driver = None;
            for (value, field_id) in partition.field_ids.get(key)? {
                if text_matches(value, matcher) {
                    let postings = partition.field_postings.get(*field_id)?;
                    if driver.is_some() {
                        return None;
                    }
                    driver = Some(postings);
                }
            }
            driver
        }
        LogPredicate::FieldIn { key, values } => {
            let mut driver = None;
            let field_ids = partition.field_ids.get(key)?;
            for value in values {
                if let Some(field_id) = field_ids.get(value.as_ref()) {
                    let postings = partition.field_postings.get(*field_id)?;
                    if driver.is_some() {
                        return None;
                    }
                    driver = Some(postings);
                }
            }
            driver
        }
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => {
            let mut driver = None;
            for (observed, field_id) in partition.numeric_field_values.get(key)? {
                if numeric_comparison_matches(*comparison, *observed, *value) {
                    let postings = partition.field_postings.get(*field_id)?;
                    if driver.is_some() {
                        return None;
                    }
                    driver = Some(postings);
                }
            }
            driver
        }
        LogPredicate::FieldRegex { key, regex } => {
            let mut driver = None;
            for (value, field_id) in partition.field_ids.get(key)? {
                if regex.is_match(value) {
                    let postings = partition.field_postings.get(*field_id)?;
                    if driver.is_some() {
                        return None;
                    }
                    driver = Some(postings);
                }
            }
            driver
        }
        LogPredicate::And(predicates) => predicates
            .iter()
            .filter_map(|predicate| hot_predicate_driver_posting(predicate, partition, start, end))
            .min_by_key(|postings| postings.cardinality_in(start, end)),
        LogPredicate::MatchAll
        | LogPredicate::MatchNone
        | LogPredicate::Or(_)
        | LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::Not(_) => None,
    }
}

pub(super) fn hot_single_posting_predicate(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::Term(_)
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. } => true,
        LogPredicate::FieldIn { values, .. } => values.len() == 1,
        _ => false,
    }
}

pub(super) fn hot_predicate_cardinality(
    predicate: &LogPredicate,
    partition: &PartitionIndex,
    start: u32,
    end: u32,
) -> usize {
    match predicate {
        LogPredicate::MatchAll => end.saturating_sub(start) as usize,
        LogPredicate::MatchNone => 0,
        LogPredicate::Term(term) => partition
            .term_ids
            .get(normalize_term(term).as_ref())
            .and_then(|term_id| partition.term_postings.get(*term_id))
            .map_or(0, |postings| postings.cardinality_in(start, end)),
        LogPredicate::FieldExists(key) => partition
            .field_presence_postings
            .get(key)
            .map_or(0, |postings| postings.cardinality_in(start, end)),
        LogPredicate::Field { key, matcher } => partition
            .field_ids
            .get(key)
            .into_iter()
            .flat_map(|values| values.iter())
            .filter(|(value, _)| text_matches(value, matcher))
            .filter_map(|(_, field_id)| partition.field_postings.get(*field_id))
            .map(|postings| postings.cardinality_in(start, end))
            .sum(),
        LogPredicate::FieldIn { key, values } => values
            .iter()
            .filter_map(|value| {
                partition
                    .field_ids
                    .get(key)
                    .and_then(|ids| ids.get(value.as_ref()))
                    .and_then(|field_id| partition.field_postings.get(*field_id))
            })
            .map(|postings| postings.cardinality_in(start, end))
            .sum(),
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => partition
            .numeric_field_values
            .get(key)
            .into_iter()
            .flat_map(|values| values.iter())
            .filter(|(observed, _)| numeric_comparison_matches(*comparison, *observed, *value))
            .filter_map(|(_, field_id)| partition.field_postings.get(*field_id))
            .map(|postings| postings.cardinality_in(start, end))
            .sum(),
        LogPredicate::And(predicates) => predicates
            .iter()
            .map(|predicate| hot_predicate_cardinality(predicate, partition, start, end))
            .min()
            .unwrap_or_else(|| end.saturating_sub(start) as usize),
        LogPredicate::Or(predicates) => predicates
            .iter()
            .map(|predicate| hot_predicate_cardinality(predicate, partition, start, end))
            .fold(0usize, usize::saturating_add)
            .min(end.saturating_sub(start) as usize),
        LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::Not(_)
        | LogPredicate::FieldRegex { .. } => end.saturating_sub(start) as usize,
    }
}

pub(super) fn hot_predicate_matches_ordinal(
    predicate: &LogPredicate,
    partition: &PartitionIndex,
    ordinal: u32,
) -> bool {
    match predicate {
        LogPredicate::MatchAll => true,
        LogPredicate::MatchNone => false,
        LogPredicate::Term(term) => partition
            .term_ids
            .get(normalize_term(term).as_ref())
            .and_then(|term_id| partition.term_postings.get(*term_id))
            .is_some_and(|postings| postings.contains(ordinal)),
        LogPredicate::FieldExists(key) => partition
            .field_presence_postings
            .get(key)
            .is_some_and(|postings| postings.contains(ordinal)),
        LogPredicate::Field { key, matcher } => partition
            .field_ids
            .get(key)
            .into_iter()
            .flat_map(|values| values.iter())
            .any(|(value, field_id)| {
                text_matches(value, matcher)
                    && partition
                        .field_postings
                        .get(*field_id)
                        .is_some_and(|postings| postings.contains(ordinal))
            }),
        LogPredicate::FieldIn { key, values } => values.iter().any(|value| {
            partition
                .field_ids
                .get(key)
                .and_then(|ids| ids.get(value.as_ref()))
                .and_then(|field_id| partition.field_postings.get(*field_id))
                .is_some_and(|postings| postings.contains(ordinal))
        }),
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => partition
            .numeric_field_values
            .get(key)
            .into_iter()
            .flat_map(|values| values.iter())
            .any(|(observed, field_id)| {
                numeric_comparison_matches(*comparison, *observed, *value)
                    && partition
                        .field_postings
                        .get(*field_id)
                        .is_some_and(|postings| postings.contains(ordinal))
            }),
        LogPredicate::And(predicates) => predicates
            .iter()
            .all(|predicate| hot_predicate_matches_ordinal(predicate, partition, ordinal)),
        LogPredicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| hot_predicate_matches_ordinal(predicate, partition, ordinal)),
        LogPredicate::FieldRegex { key, regex } => partition
            .field_ids
            .get(key)
            .into_iter()
            .flat_map(|values| values.iter())
            .any(|(value, field_id)| {
                regex.is_match(value)
                    && partition
                        .field_postings
                        .get(*field_id)
                        .is_some_and(|postings| postings.contains(ordinal))
            }),
        LogPredicate::MessageToken {
            value,
            case_sensitivity: CaseSensitivity::Insensitive,
        } => partition
            .term_ids
            .get(normalize_term(value).as_ref())
            .and_then(|term_id| partition.term_postings.get(*term_id))
            .is_some_and(|postings| postings.contains(ordinal)),
        LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_) => false,
        LogPredicate::Not(predicate) => {
            hot_predicate_candidates_are_exact(predicate)
                && !hot_predicate_matches_ordinal(predicate, partition, ordinal)
        }
    }
}

pub(super) fn hot_predicate_candidates_are_exact(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MatchAll
        | LogPredicate::MatchNone
        | LogPredicate::Term(_)
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. } => true,
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            predicates.iter().all(hot_predicate_candidates_are_exact)
        }
        LogPredicate::MessageToken {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        } => true,
        LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_) => false,
        LogPredicate::Not(predicate) => hot_predicate_candidates_are_exact(predicate),
    }
}

pub(super) fn hot_field_text_candidates(
    partition: &PartitionIndex,
    key: &str,
    matcher: &crate::TextMatcher,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    hot_field_predicate_candidates(
        partition,
        key,
        |value| text_matches(value, matcher),
        start,
        end,
    )
}

pub(super) fn hot_field_predicate_candidates(
    partition: &PartitionIndex,
    key: &str,
    mut matches_value: impl FnMut(&str) -> bool,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    let Some(value_ids) = partition.field_ids.get(key) else {
        return Some(Vec::new());
    };
    let mut field_postings = Vec::new();
    for (value, field_id) in value_ids {
        if matches_value(value)
            && let Some(posting) = partition.field_postings.get(*field_id)
        {
            field_postings.push(posting);
        }
    }
    Some(collect_hot_posting_union(&field_postings, start, end, None))
}

pub(super) fn numeric_comparison_matches(
    comparison: NumericComparison,
    observed: i128,
    target: i128,
) -> bool {
    match comparison {
        NumericComparison::Equal => observed == target,
        NumericComparison::NotEqual => observed != target,
        NumericComparison::LessThan => observed < target,
        NumericComparison::LessThanOrEqual => observed <= target,
        NumericComparison::GreaterThan => observed > target,
        NumericComparison::GreaterThanOrEqual => observed >= target,
    }
}

pub(super) fn hot_numeric_field_candidates(
    partition: &PartitionIndex,
    key: &str,
    comparison: NumericComparison,
    target: i128,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    let Some(value_ids) = partition.numeric_field_values.get(key) else {
        return Some(Vec::new());
    };
    let mut field_postings = Vec::new();
    for (observed, field_id) in value_ids {
        let matches = match comparison {
            NumericComparison::Equal => *observed == target,
            NumericComparison::NotEqual => *observed != target,
            NumericComparison::LessThan => *observed < target,
            NumericComparison::LessThanOrEqual => *observed <= target,
            NumericComparison::GreaterThan => *observed > target,
            NumericComparison::GreaterThanOrEqual => *observed >= target,
        };
        if matches && let Some(posting) = partition.field_postings.get(*field_id) {
            field_postings.push(posting);
        }
    }
    Some(collect_hot_posting_union(&field_postings, start, end, None))
}
