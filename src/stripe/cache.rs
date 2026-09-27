use super::*;

impl CachedMessageTokenStats {
    pub(super) fn candidate_ordinals_for_predicate(
        &self,
        predicate: &LogPredicate,
    ) -> Option<Vec<u32>> {
        if !self.messages.iter().all(|message| message.is_ascii()) {
            return None;
        }
        match predicate {
            LogPredicate::MessageTokenRegex(regex)
                if regex.case_sensitivity() == CaseSensitivity::Insensitive
                    && regex.pattern().is_ascii() =>
            {
                let mut candidates = Vec::new();
                for (token, posting) in &self.postings {
                    if regex.is_match(token) {
                        union_sorted_ordinals(&mut candidates, posting.ordinals.to_vec());
                    }
                }
                Some(candidates)
            }
            LogPredicate::MessageTokenPrefix {
                value,
                case_sensitivity: CaseSensitivity::Insensitive,
            } if value.is_ascii() => {
                let prefix = normalize_term(value);
                let mut candidates = Vec::new();
                for (token, posting) in &self.postings {
                    if token.starts_with(prefix.as_ref()) {
                        union_sorted_ordinals(&mut candidates, posting.ordinals.to_vec());
                    }
                }
                Some(candidates)
            }
            LogPredicate::MessageFuzzy {
                value,
                max_distance,
            } if value.is_ascii() => {
                let value = normalize_term(value);
                let mut candidates = Vec::new();
                for (token, posting) in &self.postings {
                    if bounded_levenshtein(token, value.as_ref(), usize::from(*max_distance)) {
                        union_sorted_ordinals(&mut candidates, posting.ordinals.to_vec());
                    }
                }
                Some(candidates)
            }
            LogPredicate::And(predicates) if !predicates.is_empty() => {
                let mut candidates = None;
                for predicate in predicates {
                    let posting_candidates = self.candidate_ordinals_for_predicate(predicate)?;
                    intersect_frame_candidate_slice(&mut candidates, &posting_candidates);
                    if candidates.as_ref().is_some_and(Vec::is_empty) {
                        break;
                    }
                }
                Some(candidates.unwrap_or_default())
            }
            _ => None,
        }
    }

    pub(super) fn phrase_candidate_ordinals(
        &self,
        candidates: &[u32],
        terms: &[Arc<str>],
        max_gap: usize,
        case_sensitivity: CaseSensitivity,
    ) -> Option<Vec<u32>> {
        if case_sensitivity != CaseSensitivity::Insensitive
            || !terms.iter().all(|term| term.is_ascii())
            || !self.messages.iter().all(|message| message.is_ascii())
        {
            return None;
        }
        let term_ids = terms
            .iter()
            .map(|term| {
                self.token_ids_by_term
                    .get(normalize_term(term).as_ref())
                    .copied()
            })
            .collect::<Option<Vec<_>>>();
        let Some(term_ids) = term_ids else {
            return Some(Vec::new());
        };
        Some(
            candidates
                .iter()
                .copied()
                .filter(|ordinal| {
                    let Some(&start) = self.token_offsets.get(*ordinal as usize) else {
                        return false;
                    };
                    let Some(&end) = self.token_offsets.get(*ordinal as usize + 1) else {
                        return false;
                    };
                    let Ok(start) = usize::try_from(start) else {
                        return false;
                    };
                    let Ok(end) = usize::try_from(end) else {
                        return false;
                    };
                    let Some(tokens) = self.token_sequence.get(start..end) else {
                        return false;
                    };
                    message_token_ids_have_phrase(tokens, &term_ids, max_gap)
                })
                .collect(),
        )
    }

    pub(super) fn cache_bytes(&self) -> usize {
        self.document_lengths
            .len()
            .saturating_mul(size_of::<u32>())
            .saturating_add(self.token_sequence.len().saturating_mul(size_of::<u32>()))
            .saturating_add(self.token_offsets.len().saturating_mul(size_of::<u32>()))
            .saturating_add(
                self.messages
                    .iter()
                    .map(|message| size_of::<Arc<str>>().saturating_add(message.len()))
                    .sum::<usize>(),
            )
            .saturating_add(
                self.token_ids_by_term
                    .keys()
                    .map(|token| token.len().saturating_add(size_of::<u32>()))
                    .sum::<usize>(),
            )
            .saturating_add(
                self.postings
                    .iter()
                    .map(|(token, posting)| {
                        token
                            .len()
                            .saturating_add(posting.ordinals.len().saturating_mul(size_of::<u32>()))
                            .saturating_add(
                                posting.frequencies.len().saturating_mul(size_of::<u32>()),
                            )
                    })
                    .sum::<usize>(),
            )
    }

    pub(super) fn score_batch<E>(
        &self,
        scorer: &crate::analytics::RelevanceScorer,
        ordinals: impl Iterator<Item = u32>,
        mut emit: impl FnMut(f64) -> Result<(), E>,
    ) -> Result<(), E> {
        let postings = scorer
            .terms()
            .iter()
            .map(|term| self.postings.get(term).map(Arc::as_ref))
            .collect::<Vec<_>>();
        let mut posting_positions = vec![0usize; postings.len()];
        let mut previous_ordinal = None;
        for ordinal in ordinals {
            // Frame scans yield ascending ordinals, but owner-local top-k
            // heaps are returned in arbitrary order before the coordinator
            // scores them. Reset the posting cursors after a backward jump.
            if previous_ordinal.is_some_and(|previous| ordinal < previous) {
                for (position, posting) in posting_positions.iter_mut().zip(&postings) {
                    *position = posting.map_or(0, |posting| {
                        posting
                            .ordinals
                            .partition_point(|candidate| *candidate < ordinal)
                    });
                }
            }
            previous_ordinal = Some(ordinal);
            let document_length = self
                .document_lengths
                .get(ordinal as usize)
                .copied()
                .unwrap_or_default();
            let score = scorer.score_indexed_by_index(document_length, |index| {
                let Some(Some(posting)) = postings.get(index) else {
                    return 0;
                };
                let position = &mut posting_positions[index];
                while *position < posting.ordinals.len() && posting.ordinals[*position] < ordinal {
                    *position += 1;
                }
                if *position >= posting.ordinals.len() || posting.ordinals[*position] != ordinal {
                    return 0;
                }
                posting
                    .frequencies
                    .get(*position)
                    .copied()
                    .unwrap_or_default()
            });
            emit(score)?;
        }
        Ok(())
    }
}

pub(super) fn message_token_ids_have_phrase(tokens: &[u32], terms: &[u32], max_gap: usize) -> bool {
    let Some(&first_term) = terms.first() else {
        return true;
    };
    if max_gap == 0 {
        let mut next = 0usize;
        for &token in tokens {
            if token == terms[next] {
                next += 1;
                if next == terms.len() {
                    return true;
                }
            } else {
                next = usize::from(token == first_term);
            }
        }
        return false;
    }
    for start in 0..tokens.len() {
        if tokens[start] != first_term {
            continue;
        }
        let mut cursor = start + 1;
        let mut matched = true;
        for &term in &terms[1..] {
            let search_end = cursor.saturating_add(max_gap + 1).min(tokens.len());
            let Some(relative) = tokens
                .get(cursor..search_end)
                .and_then(|window| window.iter().position(|token| *token == term))
            else {
                matched = false;
                break;
            };
            cursor = cursor.saturating_add(relative + 1);
        }
        if matched {
            return true;
        }
    }
    false
}

pub(crate) fn for_each_message_match_score<E>(
    matches: &[LogMessageMatch],
    scorer: &crate::analytics::RelevanceScorer,
    emit: &mut dyn FnMut(&LogMessageMatch, f64) -> Result<(), E>,
) -> Result<(), E> {
    let mut start = 0usize;
    while start < matches.len() {
        let Some(relevance) = matches[start].relevance.as_ref() else {
            let matched = &matches[start];
            let message = matched
                .message
                .as_deref()
                .expect("hot message matches retain their message body");
            emit(matched, scorer.score(message))?;
            start += 1;
            continue;
        };
        let stats = Arc::clone(&relevance.stats);
        let mut end = start + 1;
        while end < matches.len()
            && matches[end]
                .relevance
                .as_ref()
                .is_some_and(|next| Arc::ptr_eq(&stats, &next.stats))
        {
            end += 1;
        }
        let run = &matches[start..end];
        let mut match_iter = run.iter();
        stats.score_batch(
            scorer,
            run.iter().map(|matched| {
                matched
                    .relevance
                    .as_ref()
                    .expect("indexed relevance run has indexed matches")
                    .ordinal
            }),
            |score| {
                let matched = match_iter
                    .next()
                    .expect("indexed relevance score has a matching row");
                emit(matched, score)
            },
        )?;
        start = end;
    }
    Ok(())
}

impl CachedFieldPostings {
    pub(super) fn cache_bytes(&self) -> usize {
        self.presence
            .len()
            .saturating_mul(size_of::<u32>())
            .saturating_add(
                self.ordinal_value_ids
                    .len()
                    .saturating_mul(size_of::<u32>()),
            )
            .saturating_add(self.value_table.len().saturating_mul(size_of::<Arc<str>>()))
            .saturating_add(
                self.values
                    .iter()
                    .map(|(value, posting)| {
                        value
                            .len()
                            .saturating_add(posting.len().saturating_mul(size_of::<u32>()))
                    })
                    .sum::<usize>(),
            )
    }
}

impl CachedIndexedFrame {
    pub(super) fn cache_bytes(&self) -> usize {
        let field_posting_bytes = self
            .field_postings
            .lock()
            .expect("indexed frame field postings lock is not poisoned")
            .values()
            .map(|postings| postings.cache_bytes())
            .sum::<usize>();
        let message_token_bytes = self
            .message_token_stats
            .lock()
            .expect("indexed frame message token cache lock is not poisoned")
            .as_ref()
            .map_or(0, |stats| stats.cache_bytes());
        let typed_metadata_bytes = self
            .typed_metadata
            .lock()
            .expect("indexed frame typed metadata cache lock is not poisoned")
            .as_ref()
            .map_or(0, |metadata| metadata.cache_bytes);
        let message_predicate_bytes = self
            .message_predicate_candidates
            .lock()
            .expect("indexed frame message predicate cache lock is not poisoned")
            .iter()
            .map(|(key, posting)| {
                key.len()
                    .saturating_add(posting.len().saturating_mul(size_of::<u32>()))
            })
            .sum::<usize>();
        let message_body_bytes = self
            .message_bodies
            .lock()
            .expect("indexed frame message body cache lock is not poisoned")
            .values
            .values()
            .map(|message| {
                size_of::<u32>()
                    .saturating_add(size_of::<Arc<str>>())
                    .saturating_add(message.len())
            })
            .sum::<usize>();
        let message_body_last_bytes = self
            .message_bodies
            .lock()
            .expect("indexed frame message body cache lock is not poisoned")
            .last
            .as_ref()
            .map_or(0, |(ordinals, messages)| {
                ordinals
                    .capacity()
                    .saturating_mul(size_of::<u32>())
                    .saturating_add(messages.len().saturating_mul(size_of::<Arc<str>>()))
            });
        let metadata_field_cache = self
            .metadata_fields
            .lock()
            .expect("indexed frame metadata field cache lock is not poisoned");
        let metadata_field_bytes = metadata_field_cache
            .values
            .values()
            .map(cached_field_bytes)
            .sum::<usize>();
        let metadata_field_last_bytes =
            metadata_field_cache
                .last
                .as_ref()
                .map_or(0, |(ordinals, fields)| {
                    ordinals
                        .capacity()
                        .saturating_mul(size_of::<u32>())
                        .saturating_add(
                            fields
                                .len()
                                .saturating_mul(size_of::<Arc<Vec<crate::MetadataField>>>()),
                        )
                });
        self.structural
            .len()
            .saturating_add(self.embedded_index.cache_bytes())
            .saturating_add(
                self.attribute_tables
                    .0
                    .iter()
                    .map(|key| key.len().saturating_add(size_of::<Arc<str>>()))
                    .sum::<usize>(),
            )
            .saturating_add(
                self.attribute_tables
                    .1
                    .iter()
                    .map(|values| {
                        values
                            .iter()
                            .map(|value| value.len().saturating_add(size_of::<Arc<str>>()))
                            .sum::<usize>()
                    })
                    .sum::<usize>(),
            )
            .saturating_add(
                self.templates
                    .iter()
                    .map(|literals| {
                        size_of::<Vec<Vec<u8>>>().saturating_add(
                            literals
                                .iter()
                                .map(|literal| {
                                    size_of::<Vec<u8>>().saturating_add(literal.capacity())
                                })
                                .sum::<usize>(),
                        )
                    })
                    .sum::<usize>(),
            )
            .saturating_add(
                self.offsets
                    .len()
                    .saturating_mul(size_of::<LogicalOffset>()),
            )
            .saturating_add(self.timestamps.len().saturating_mul(size_of::<u64>()))
            .saturating_add(
                self.trace_ids
                    .lock()
                    .expect("indexed frame trace ID cache lock is not poisoned")
                    .as_ref()
                    .map(|trace_ids| trace_ids.len().saturating_mul(size_of::<Option<TraceId>>()))
                    .unwrap_or_default(),
            )
            .saturating_add(typed_metadata_bytes)
            .saturating_add(message_token_bytes)
            .saturating_add(message_predicate_bytes)
            .saturating_add(message_body_bytes)
            .saturating_add(message_body_last_bytes)
            .saturating_add(metadata_field_bytes)
            .saturating_add(metadata_field_last_bytes)
            .saturating_add(field_posting_bytes)
    }

    pub(super) fn cached_messages(&self, ordinals: &[u32]) -> Option<Arc<[Arc<str>]>> {
        if ordinals.len() > MAX_CACHED_FRAME_MESSAGES {
            return None;
        }
        let cache = self
            .message_bodies
            .lock()
            .expect("indexed frame message body cache lock is not poisoned");
        if let Some((cached_ordinals, messages)) = &cache.last
            && cached_ordinals.as_slice() == ordinals
        {
            return Some(Arc::clone(messages));
        }
        let messages = ordinals
            .iter()
            .map(|ordinal| cache.values.get(ordinal).cloned())
            .collect::<Option<Vec<_>>>()?;
        if messages.iter().map(|message| message.len()).sum::<usize>()
            > MAX_CACHED_FRAME_MESSAGE_BYTES
        {
            return None;
        }
        let messages = Arc::<[Arc<str>]>::from(messages);
        drop(cache);
        let mut cache = self
            .message_bodies
            .lock()
            .expect("indexed frame message body cache lock is not poisoned");
        cache.last = Some((ordinals.to_vec(), Arc::clone(&messages)));
        Some(messages)
    }

    pub(super) fn cache_messages(
        &self,
        ordinals: &[u32],
        messages: &[crate::DecodedStructuralRecord],
    ) {
        if ordinals.len() != messages.len() {
            return;
        }
        let mut cache = self
            .message_bodies
            .lock()
            .expect("indexed frame message body cache lock is not poisoned");
        for (ordinal, record) in ordinals.iter().zip(messages) {
            if cache.values.contains_key(ordinal) {
                continue;
            }
            let bytes = record.message.len();
            if cache.values.len() >= MAX_CACHED_FRAME_MESSAGES
                || cache.bytes.saturating_add(bytes) > MAX_CACHED_FRAME_MESSAGE_BYTES
            {
                break;
            }
            cache.bytes = cache.bytes.saturating_add(bytes);
            cache.values.insert(*ordinal, Arc::clone(&record.message));
        }
        let batch_bytes = messages
            .iter()
            .map(|record| record.message.len())
            .sum::<usize>();
        cache.last = (ordinals.len() <= MAX_CACHED_FRAME_MESSAGES
            && batch_bytes <= MAX_CACHED_FRAME_MESSAGE_BYTES)
            .then(|| {
                (
                    ordinals.to_vec(),
                    Arc::from(
                        messages
                            .iter()
                            .map(|record| Arc::clone(&record.message))
                            .collect::<Vec<_>>(),
                    ),
                )
            });
    }

    pub(super) fn cached_fields(
        &self,
        ordinals: &[u32],
    ) -> Option<Arc<[Arc<Vec<crate::MetadataField>>]>> {
        if ordinals.len() > MAX_CACHED_FRAME_FIELDS {
            return None;
        }
        let cache = self
            .metadata_fields
            .lock()
            .expect("indexed frame metadata field cache lock is not poisoned");
        if let Some((cached_ordinals, fields)) = &cache.last
            && cached_ordinals.as_slice() == ordinals
        {
            return Some(Arc::clone(fields));
        }
        let fields = ordinals
            .iter()
            .map(|ordinal| cache.values.get(ordinal).cloned())
            .collect::<Option<Vec<_>>>()?;
        if fields.iter().map(cached_field_bytes).sum::<usize>() > MAX_CACHED_FRAME_FIELD_BYTES {
            return None;
        }
        let fields = Arc::<[Arc<Vec<crate::MetadataField>>]>::from(fields);
        drop(cache);
        let mut cache = self
            .metadata_fields
            .lock()
            .expect("indexed frame metadata field cache lock is not poisoned");
        cache.last = Some((ordinals.to_vec(), Arc::clone(&fields)));
        Some(fields)
    }

    pub(super) fn cache_fields(
        &self,
        ordinals: &[u32],
        records: &[crate::DecodedStructuralRecord],
    ) {
        if ordinals.len() != records.len() {
            return;
        }
        let mut cache = self
            .metadata_fields
            .lock()
            .expect("indexed frame metadata field cache lock is not poisoned");
        for (ordinal, record) in ordinals.iter().zip(records) {
            if cache.values.contains_key(ordinal) {
                continue;
            }
            let bytes = cached_field_bytes(&record.fields);
            if cache.values.len() >= MAX_CACHED_FRAME_FIELDS
                || cache.bytes.saturating_add(bytes) > MAX_CACHED_FRAME_FIELD_BYTES
            {
                break;
            }
            cache.bytes = cache.bytes.saturating_add(bytes);
            cache.values.insert(*ordinal, Arc::clone(&record.fields));
        }
        let batch_bytes = records
            .iter()
            .map(|record| cached_field_bytes(&record.fields))
            .sum::<usize>();
        cache.last = (ordinals.len() <= MAX_CACHED_FRAME_FIELDS
            && batch_bytes <= MAX_CACHED_FRAME_FIELD_BYTES)
            .then(|| {
                (
                    ordinals.to_vec(),
                    Arc::from(
                        records
                            .iter()
                            .map(|record| Arc::clone(&record.fields))
                            .collect::<Vec<_>>(),
                    ),
                )
            });
    }
}

pub(super) fn retain_cached_timestamp_candidates(
    query: &LogQuery,
    cached: &CachedIndexedFrame,
    candidates: &mut Vec<u32>,
) {
    if query.start_timestamp_unix_nanos.is_none() && query.end_timestamp_unix_nanos.is_none() {
        return;
    }
    if cached.embedded_index.timestamp_offset_ordinal_ordered() {
        // Posting intersections preserve ordinal order, so an ordered frame's
        // timestamp window can trim the candidate vector without probing each
        // timestamp individually.
        let start = query.start_timestamp_unix_nanos.map_or(0, |timestamp| {
            cached
                .timestamps
                .partition_point(|candidate| *candidate < timestamp)
        });
        let end = query
            .end_timestamp_unix_nanos
            .map_or(cached.timestamps.len(), |timestamp| {
                cached
                    .timestamps
                    .partition_point(|candidate| *candidate < timestamp)
            });
        if start >= end {
            candidates.clear();
            return;
        }
        let first = candidates
            .partition_point(|ordinal| usize::try_from(*ordinal).is_ok_and(|index| index < start));
        let last = candidates
            .partition_point(|ordinal| usize::try_from(*ordinal).is_ok_and(|index| index < end));
        candidates.truncate(last);
        candidates.drain(..first);
    } else {
        candidates.retain(|ordinal| {
            usize::try_from(*ordinal)
                .ok()
                .and_then(|index| cached.timestamps.get(index))
                .is_some_and(|timestamp| query.timestamp_matches(*timestamp))
        });
    }
}

pub(super) fn count_cached_timestamp_candidates(
    query: &LogQuery,
    cached: &CachedIndexedFrame,
    candidates: &[u32],
) -> usize {
    if query.start_timestamp_unix_nanos.is_none() && query.end_timestamp_unix_nanos.is_none() {
        return candidates.len();
    }
    if cached.embedded_index.timestamp_offset_ordinal_ordered() {
        let start = query.start_timestamp_unix_nanos.map_or(0, |timestamp| {
            cached
                .timestamps
                .partition_point(|candidate| *candidate < timestamp)
        });
        let end = query
            .end_timestamp_unix_nanos
            .map_or(cached.timestamps.len(), |timestamp| {
                cached
                    .timestamps
                    .partition_point(|candidate| *candidate < timestamp)
            });
        if start >= end {
            return 0;
        }
        let first = candidates
            .partition_point(|ordinal| usize::try_from(*ordinal).is_ok_and(|index| index < start));
        let last = candidates
            .partition_point(|ordinal| usize::try_from(*ordinal).is_ok_and(|index| index < end));
        return last.saturating_sub(first);
    }
    candidates
        .iter()
        .filter_map(|ordinal| {
            usize::try_from(*ordinal)
                .ok()
                .and_then(|index| cached.timestamps.get(index))
                .copied()
        })
        .filter(|timestamp| query.timestamp_matches(*timestamp))
        .count()
}

impl IndexedFrameQueryCache {
    pub(super) fn get(&mut self, frame_id: u64) -> Option<Arc<CachedIndexedFrame>> {
        // Hits do not update the eviction order. Maintaining an exact LRU here
        // would make every frame hit scan the order queue; insertion-order
        // eviction keeps the bounded cache out of the query hot path.
        self.entries.get(&frame_id).cloned()
    }

    pub(super) fn insert(&mut self, frame_id: u64, cached: Arc<CachedIndexedFrame>) {
        if cached.cache_bytes() > MAX_INDEXED_FRAME_QUERY_CACHE_BYTES {
            return;
        }
        self.entries.remove(&frame_id);
        self.remove_from_eviction_order(frame_id);
        self.entries.insert(frame_id, cached);
        self.eviction_order.push_back(frame_id);
        self.enforce_budget();
    }

    pub(super) fn remove_from_eviction_order(&mut self, frame_id: u64) {
        if let Some(position) = self
            .eviction_order
            .iter()
            .position(|cached| *cached == frame_id)
        {
            self.eviction_order.remove(position);
        }
    }

    pub(super) fn enforce_budget(&mut self) {
        self.bytes = self
            .entries
            .values()
            .map(|cached| cached.cache_bytes())
            .fold(0usize, usize::saturating_add);
        while self.bytes > MAX_INDEXED_FRAME_QUERY_CACHE_BYTES {
            let Some(evicted_id) = self.eviction_order.pop_front() else {
                break;
            };
            self.entries.remove(&evicted_id);
            self.bytes = self
                .entries
                .values()
                .map(|cached| cached.cache_bytes())
                .fold(0usize, usize::saturating_add);
        }
    }
}

impl ExactPostingCache {
    pub(super) fn get(&self, key: &ExactPostingKey) -> Option<Arc<[u32]>> {
        self.entries.get(key).cloned()
    }

    pub(super) fn insert(&mut self, key: ExactPostingKey, posting: Arc<[u32]>) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.bytes = self
            .bytes
            .saturating_add(posting.len().saturating_mul(size_of::<u32>()));
        self.eviction_order.push_back(key.clone());
        self.entries.insert(key, posting);
        while self.bytes > MAX_EXACT_FRAME_POSTING_CACHE_BYTES {
            let Some(evicted) = self.eviction_order.pop_front() else {
                break;
            };
            if let Some(posting) = self.entries.remove(&evicted) {
                self.bytes = self
                    .bytes
                    .saturating_sub(posting.len().saturating_mul(size_of::<u32>()));
            }
        }
    }
}
