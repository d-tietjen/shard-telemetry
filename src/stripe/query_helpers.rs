use super::*;

pub(super) fn normalize_term(term: &str) -> Cow<'_, str> {
    if term.chars().any(char::is_uppercase) {
        Cow::Owned(term.to_lowercase())
    } else {
        Cow::Borrowed(term)
    }
}

pub(super) fn exact_message_posting_key(
    frame_id: u64,
    token: &str,
    case_sensitivity: CaseSensitivity,
) -> ExactPostingKey {
    ExactPostingKey::Message(
        frame_id,
        match case_sensitivity {
            CaseSensitivity::Sensitive => Arc::from(token),
            CaseSensitivity::Insensitive => Arc::from(token.to_ascii_lowercase()),
        },
        case_sensitivity,
    )
}

pub(super) fn validate_batch_offset_range(
    topic_partition: TopicPartition,
    first_offset: LogicalOffset,
    record_count: usize,
) -> TelemetryResult<()> {
    let Some(last_index) = record_count.checked_sub(1) else {
        return Ok(());
    };
    batch_offset(topic_partition, first_offset, last_index).map(|_| ())
}

pub(super) fn batch_offset(
    topic_partition: TopicPartition,
    first_offset: LogicalOffset,
    index: usize,
) -> TelemetryResult<LogicalOffset> {
    let relative_offset =
        u64::try_from(index).map_err(|_| TelemetryError::OffsetExhausted(topic_partition))?;
    first_offset
        .get()
        .checked_add(relative_offset)
        .map(LogicalOffset::new)
        .ok_or(TelemetryError::OffsetExhausted(topic_partition))
}

#[inline]
pub(super) fn same_message(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && (std::ptr::eq(left.as_ptr(), right.as_ptr()) || left.as_bytes() == right.as_bytes())
}

#[inline]
pub(super) fn message_term_cache_slot(topic_partition: TopicPartition, message: &[u8]) -> usize {
    let mut hash = message.len() as u64
        ^ u64::from(topic_partition.partition_id.get()).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    if message.len() >= 16 {
        let first = u64::from_le_bytes(
            message[..8]
                .try_into()
                .expect("eight-byte prefix is present"),
        );
        let last = u64::from_le_bytes(
            message[message.len() - 8..]
                .try_into()
                .expect("eight-byte suffix is present"),
        );
        hash ^= first.rotate_left(17) ^ last.rotate_left(41);
    } else {
        for byte in message {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash as usize & (MESSAGE_TERM_CACHE_ENTRIES - 1)
}

pub(super) fn collect_message_trigram_keys(message: &str) -> Vec<u32> {
    let bytes = message.as_bytes();
    if bytes.len() < 3 {
        return Vec::new();
    }
    let mut keys = bytes
        .windows(3)
        .map(|window| {
            u32::from(window[0].to_ascii_lowercase())
                | (u32::from(window[1].to_ascii_lowercase()) << 8)
                | (u32::from(window[2].to_ascii_lowercase()) << 16)
        })
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    keys
}

#[inline]
pub(super) fn field_cache_slot(
    topic_partition: TopicPartition,
    fields: &Arc<Vec<crate::MetadataField>>,
) -> usize {
    let pointer = Arc::as_ptr(fields) as usize;
    let partition = topic_partition.partition_id.get() as usize;
    (pointer.rotate_left(17) ^ partition.wrapping_mul(0x9e37_79b9)) & (FIELD_CACHE_ENTRIES - 1)
}

#[inline]
pub(super) fn same_fields(
    left: &Arc<Vec<crate::MetadataField>>,
    right: &Arc<Vec<crate::MetadataField>>,
) -> bool {
    Arc::ptr_eq(left, right) || left.as_slice() == right.as_slice()
}

pub(super) fn ordinal_record_window(
    records: &[IndexedRecord],
    start: Option<LogicalOffset>,
    end: Option<LogicalOffset>,
) -> std::ops::Range<usize> {
    let start_index = start.map_or(0, |start| {
        records.partition_point(|record| record.record.record_ref.offset < start)
    });
    let end_index = end.map_or(records.len(), |end| {
        records.partition_point(|record| record.record.record_ref.offset < end)
    });
    start_index.min(end_index)..end_index
}

pub(super) fn collect_ordered_range(
    ordinals: std::ops::Range<usize>,
    order: QueryOrder,
    limit: Option<usize>,
) -> Vec<u32> {
    let take = limit.unwrap_or(ordinals.len()).min(ordinals.len());
    match order {
        QueryOrder::OldestFirst => ordinals
            .take(take)
            .map(|ordinal| u32::try_from(ordinal).expect("record ordinal was bounded"))
            .collect(),
        QueryOrder::NewestFirst => ordinals
            .rev()
            .take(take)
            .map(|ordinal| u32::try_from(ordinal).expect("record ordinal was bounded"))
            .collect(),
    }
}

pub(super) fn same_query_across_partition(left: &LogQuery, right: &LogQuery) -> bool {
    let mut normalized = left.clone();
    normalized.topic_partition = right.topic_partition;
    normalized == *right
}

pub(super) fn append_matches_query_bounds(query: &LogQuery, append: &IndexedFrameAppend) -> bool {
    query.end_offset.is_none_or(|end| end > append.first_offset)
        && query
            .start_offset
            .is_none_or(|start| start <= append.last_offset)
}

pub(super) fn frame_matches_query_bounds(query: &LogQuery, frame: &IndexedIngestFrame) -> bool {
    timestamp_bounds_overlap(
        query,
        frame.min_timestamp_unix_nanos,
        frame.max_timestamp_unix_nanos,
    )
}

pub(super) fn timestamp_bounds_overlap(query: &LogQuery, minimum: u64, maximum: u64) -> bool {
    query
        .end_timestamp_unix_nanos
        .is_none_or(|end| end > minimum)
        && query
            .start_timestamp_unix_nanos
            .is_none_or(|start| start <= maximum)
}

pub(super) fn indexed_frame_candidates_for_append(
    query: &LogQuery,
    index: &EmbeddedFrameIndex,
    record_count: u32,
    tenant: &str,
) -> Vec<u32> {
    indexed_frame_candidates_for_append_with_phrase_mode(query, index, record_count, tenant, false)
}

pub(super) fn indexed_frame_candidates_for_append_with_phrase_mode(
    query: &LogQuery,
    index: &EmbeddedFrameIndex,
    record_count: u32,
    tenant: &str,
    include_message_phrases: bool,
) -> Vec<u32> {
    let mut constraints = query.required_index_constraints();
    if include_message_phrases {
        constraints = query.required_index_constraints_with_message_phrases(true);
    }
    if query
        .exact_fields
        .iter()
        .any(|field| field.key.as_ref() == "resource.loki.tenant" && field.value.as_ref() != tenant)
    {
        return Vec::new();
    }
    constraints
        .fields
        .retain(|(key, _)| *key != "resource.loki.tenant");
    indexed_frame_candidates_from_constraints(query, index, record_count, constraints)
}

pub(super) fn indexed_frame_candidates_from_constraints(
    query: &LogQuery,
    index: &EmbeddedFrameIndex,
    record_count: u32,
    constraints: crate::query::RequiredIndexConstraints<'_>,
) -> Vec<u32> {
    if constraints.impossible {
        return Vec::new();
    }
    let mut candidates = None::<Vec<u32>>;
    for term in &constraints.terms {
        intersect_frame_candidates(&mut candidates, index.term_candidate_ordinals(term));
        if candidates.as_ref().is_some_and(Vec::is_empty) {
            return Vec::new();
        }
    }
    for (key, value) in &constraints.fields {
        intersect_frame_candidates(&mut candidates, index.field_candidate_ordinals(key, value));
        if candidates.as_ref().is_some_and(Vec::is_empty) {
            return Vec::new();
        }
    }
    if let Some(predicate_candidates) =
        embedded_message_predicate_candidates(&query.predicate, index)
    {
        intersect_frame_candidate_slice(&mut candidates, &predicate_candidates);
        if candidates.as_ref().is_some_and(Vec::is_empty) {
            return Vec::new();
        }
    }
    candidates.unwrap_or_else(|| (0..record_count).collect())
}

pub(super) fn normalize_structural_candidate_ordinals(candidates: &mut Vec<u32>) {
    if candidates.windows(2).all(|pair| pair[0] < pair[1]) {
        return;
    }
    candidates.sort_unstable();
    candidates.dedup();
}

pub(super) fn trace_predicate_candidates_are_exact(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MatchAll | LogPredicate::MatchNone => true,
        LogPredicate::MessageToken {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        }
        | LogPredicate::MessageTokenPrefix {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        }
        | LogPredicate::MessagePhrase { .. } => true,
        LogPredicate::MessageTokenRegex(regex)
            if regex.case_sensitivity() == CaseSensitivity::Insensitive =>
        {
            true
        }
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            predicates.iter().all(trace_predicate_candidates_are_exact)
        }
        LogPredicate::Term(_)
        | LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. }
        | LogPredicate::Not(_) => false,
    }
}

pub(super) fn embedded_message_predicate_candidates(
    predicate: &LogPredicate,
    index: &EmbeddedFrameIndex,
) -> Option<Vec<u32>> {
    if let Some((tokens, minimum)) = message_token_min_match_shape(predicate) {
        let mut counts = vec![0_u8; index.record_count() as usize];
        for (token, _) in tokens {
            for ordinal in index.term_candidate_ordinals(token.as_ref()) {
                if let Some(count) = counts.get_mut(ordinal as usize) {
                    *count = count.saturating_add(1);
                }
            }
        }
        return Some(
            counts
                .into_iter()
                .enumerate()
                .filter_map(|(ordinal, count)| {
                    (usize::from(count) >= minimum).then_some(ordinal as u32)
                })
                .collect(),
        );
    }
    if let Some(terms) = simple_message_token_or_terms(predicate) {
        return Some(index.term_candidate_ordinals_union(&terms));
    }
    match predicate {
        LogPredicate::MatchAll => Some((0..index.record_count()).collect()),
        LogPredicate::MatchNone => Some(Vec::new()),
        LogPredicate::Term(term) | LogPredicate::MessageToken { value: term, .. } => {
            if term.is_empty() || term.bytes().any(|byte| !byte.is_ascii_alphanumeric()) {
                Some(Vec::new())
            } else {
                Some(index.term_candidate_ordinals(term))
            }
        }
        LogPredicate::And(predicates) => {
            let mut candidates = None;
            for predicate in predicates {
                if let Some(child) = embedded_message_predicate_candidates(predicate, index) {
                    intersect_frame_candidate_slice(&mut candidates, &child);
                    if candidates.as_ref().is_some_and(Vec::is_empty) {
                        return Some(Vec::new());
                    }
                }
            }
            candidates
        }
        LogPredicate::Or(predicates) => {
            let mut candidates = Vec::new();
            for predicate in predicates {
                union_sorted_ordinals(
                    &mut candidates,
                    embedded_message_predicate_candidates(predicate, index)?,
                );
            }
            Some(candidates)
        }
        LogPredicate::Message(_)
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
        | LogPredicate::Not(_) => None,
    }
}

pub(super) fn simple_message_token_or_terms(predicate: &LogPredicate) -> Option<Vec<&str>> {
    let LogPredicate::Or(predicates) = predicate else {
        return None;
    };
    if predicates.is_empty() {
        return None;
    }
    predicates
        .iter()
        .map(|predicate| match predicate {
            LogPredicate::Term(term) | LogPredicate::MessageToken { value: term, .. }
                if !term.is_empty() && term.bytes().all(|byte| byte.is_ascii_alphanumeric()) =>
            {
                Some(term.as_ref())
            }
            _ => None,
        })
        .collect()
}

pub(super) fn remove_published_spool_file(path: &std::path::Path) {
    if let Err(error) = fs::remove_file(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        eprintln!(
            "shard-telemetry retained published tier spool {} after cleanup failed: {error}",
            path.display()
        );
    }
}

#[allow(clippy::type_complexity)]
pub(super) fn message_token_min_match_shape(
    predicate: &LogPredicate,
) -> Option<(Vec<(Arc<str>, CaseSensitivity)>, usize)> {
    let LogPredicate::Or(predicates) = predicate else {
        return None;
    };
    if predicates.len() < 2 {
        return None;
    }
    let mut tokens = Vec::<(Arc<str>, CaseSensitivity)>::new();
    let mut subsets = Vec::<Vec<usize>>::with_capacity(predicates.len());
    let mut minimum = None;
    for predicate in predicates {
        let LogPredicate::And(children) = predicate else {
            return None;
        };
        if children.is_empty() {
            return None;
        }
        let mut subset = Vec::with_capacity(children.len());
        for child in children {
            let LogPredicate::MessageToken {
                value,
                case_sensitivity,
            } = child
            else {
                return None;
            };
            if value.is_empty() || value.bytes().any(|byte| !byte.is_ascii_alphanumeric()) {
                return None;
            }
            let index = tokens
                .iter()
                .position(|(known, known_case)| {
                    known.as_ref() == value.as_ref() && *known_case == *case_sensitivity
                })
                .unwrap_or_else(|| {
                    tokens.push((Arc::clone(value), *case_sensitivity));
                    tokens.len() - 1
                });
            if subset.contains(&index) {
                return None;
            }
            subset.push(index);
        }
        subset.sort_unstable();
        if minimum.is_some_and(|known| known != subset.len()) {
            return None;
        }
        minimum = Some(subset.len());
        subsets.push(subset);
    }
    let minimum = minimum?;
    if minimum == 0 || minimum > tokens.len() {
        return None;
    }
    subsets.sort_unstable();
    subsets.dedup();
    let expected = bounded_combination_count(tokens.len(), minimum)?;
    (expected == subsets.len()).then_some((tokens, minimum))
}

pub(super) fn bounded_combination_count(n: usize, k: usize) -> Option<usize> {
    let k = k.min(n.saturating_sub(k));
    let mut count = 1usize;
    for index in 1..=k {
        count = count.checked_mul(n - k + index)?.checked_div(index)?;
    }
    Some(count)
}

pub(super) fn cached_message_token_min_match_candidates(
    postings: &MessageTokenPostings,
    record_count: u32,
    tokens: &[(Arc<str>, CaseSensitivity)],
    minimum: usize,
) -> Vec<u32> {
    let mut counts = vec![0u16; record_count as usize];
    for (token, _) in tokens {
        let Some(posting) = postings.get(normalize_term(token).as_ref()) else {
            continue;
        };
        for ordinal in posting.ordinals.iter().copied() {
            if let Some(count) = counts.get_mut(ordinal as usize) {
                *count = count.saturating_add(1);
            }
        }
    }
    counts
        .into_iter()
        .enumerate()
        .filter_map(|(ordinal, count)| {
            (usize::from(count) >= minimum).then_some(
                u32::try_from(ordinal)
                    .expect("indexed frame record count is bounded by a u32 ordinal"),
            )
        })
        .collect()
}

pub(super) fn exact_message_token_min_match_candidates(
    postings: &[Option<Arc<[u32]>>],
    record_count: u32,
    minimum: usize,
) -> Vec<u32> {
    let mut counts = vec![0u16; record_count as usize];
    for posting in postings.iter().flatten() {
        for ordinal in posting.iter().copied() {
            if let Some(count) = counts.get_mut(ordinal as usize) {
                *count = count.saturating_add(1);
            }
        }
    }
    counts
        .into_iter()
        .enumerate()
        .filter_map(|(ordinal, count)| {
            (usize::from(count) >= minimum).then_some(
                u32::try_from(ordinal)
                    .expect("indexed frame record count is bounded by a u32 ordinal"),
            )
        })
        .collect()
}

pub(super) fn hot_message_token_min_match_candidates(
    partition: &PartitionIndex,
    tokens: &[(Arc<str>, CaseSensitivity)],
    minimum: usize,
    start: u32,
    end: u32,
) -> Vec<u32> {
    if start >= end {
        return Vec::new();
    }
    let mut counts = vec![0u16; (end - start) as usize];
    for (token, _) in tokens {
        let Some(term_id) = partition.term_ids.get(normalize_term(token).as_ref()) else {
            continue;
        };
        let Some(postings) = partition.term_postings.get(*term_id) else {
            continue;
        };
        for run in &postings.runs {
            if run.last < start {
                continue;
            }
            if run.first >= end {
                break;
            }
            let first = run.first.max(start);
            let last = run.last.min(end - 1);
            for ordinal in first..=last {
                let index = (ordinal - start) as usize;
                counts[index] = counts[index].saturating_add(1);
            }
        }
    }
    counts
        .into_iter()
        .enumerate()
        .filter_map(|(index, count)| {
            (usize::from(count) >= minimum).then_some(start + index as u32)
        })
        .collect()
}
