use super::*;

impl LogStripe {
    pub(super) fn exact_boolean_message_candidate_count(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Option<u64>> {
        if !query.terms.is_empty()
            || query.start_offset.is_some()
            || query.end_offset.is_some()
            || query.after.is_some()
        {
            return Ok(None);
        }
        let Some(cache_key) = message_predicate_key else {
            return Ok(None);
        };
        let cached = self.cached_indexed_frame(frame)?;
        if let Some(candidates) = cached
            .message_predicate_candidates
            .lock()
            .expect("indexed frame message predicate cache lock is not poisoned")
            .get(cache_key)
            .cloned()
        {
            return Ok(Some(u64::try_from(candidates.len()).unwrap_or(u64::MAX)));
        }
        let candidates =
            self.exact_boolean_message_candidates(query, frame, message_predicate_key)?;
        Ok(candidates.map(|candidates| u64::try_from(candidates.len()).unwrap_or(u64::MAX)))
    }

    pub(super) fn exact_boolean_message_candidates(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        if !query.terms.is_empty()
            || query.start_offset.is_some()
            || query.end_offset.is_some()
            || query.after.is_some()
        {
            return Ok(None);
        }
        let cached = self.cached_indexed_frame(frame)?;
        if let Some(key) = message_predicate_key
            && let Some(candidates) = cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .get(key)
                .cloned()
        {
            return Ok(Some(candidates.to_vec()));
        }
        if let Some((tokens, minimum)) = message_token_min_match_shape(&query.predicate) {
            let requested = tokens
                .iter()
                .map(|(value, sensitivity)| (value.as_ref(), *sensitivity))
                .collect::<Vec<_>>();
            let postings = self.cached_exact_message_terms(&cached, &requested)?;
            let candidates =
                exact_message_token_min_match_candidates(&postings, frame.record_count, minimum);
            if let Some(key) = message_predicate_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        fn collect_tokens(
            predicate: &LogPredicate,
            tokens: &mut Vec<(Arc<str>, CaseSensitivity)>,
        ) -> bool {
            match predicate {
                LogPredicate::MatchAll | LogPredicate::MatchNone => true,
                LogPredicate::MessageToken {
                    value,
                    case_sensitivity,
                } if !value.is_empty()
                    && value.bytes().all(|byte| byte.is_ascii_alphanumeric()) =>
                {
                    if !tokens.iter().any(|(known, sensitivity)| {
                        known == value && sensitivity == case_sensitivity
                    }) {
                        tokens.push((Arc::clone(value), *case_sensitivity));
                    }
                    true
                }
                LogPredicate::And(predicates) | LogPredicate::Or(predicates) => predicates
                    .iter()
                    .all(|predicate| collect_tokens(predicate, tokens)),
                LogPredicate::Not(predicate) => collect_tokens(predicate, tokens),
                _ => false,
            }
        }
        fn evaluate(
            predicate: &LogPredicate,
            postings: &HashMap<(Arc<str>, CaseSensitivity), Vec<u32>>,
            record_count: u32,
        ) -> Option<Vec<u32>> {
            match predicate {
                LogPredicate::MatchAll => Some((0..record_count).collect()),
                LogPredicate::MatchNone => Some(Vec::new()),
                LogPredicate::MessageToken {
                    value,
                    case_sensitivity,
                } => Some(
                    postings
                        .get(&(Arc::clone(value), *case_sensitivity))
                        .cloned()
                        .unwrap_or_default(),
                ),
                LogPredicate::And(predicates) => {
                    let mut current = None;
                    for predicate in predicates {
                        let child = evaluate(predicate, postings, record_count)?;
                        intersect_frame_candidate_slice(&mut current, &child);
                    }
                    Some(current.unwrap_or_else(|| (0..record_count).collect()))
                }
                LogPredicate::Or(predicates) => {
                    let mut current = Vec::new();
                    for predicate in predicates {
                        union_sorted_ordinals(
                            &mut current,
                            evaluate(predicate, postings, record_count)?,
                        );
                    }
                    Some(current)
                }
                LogPredicate::Not(predicate) => {
                    let excluded = evaluate(predicate, postings, record_count)?;
                    let mut selected = Vec::new();
                    let mut excluded_index = 0;
                    for ordinal in 0..record_count {
                        if excluded.get(excluded_index).copied() == Some(ordinal) {
                            excluded_index += 1;
                        } else {
                            selected.push(ordinal);
                        }
                    }
                    Some(selected)
                }
                _ => None,
            }
        }

        let mut tokens = Vec::new();
        if !collect_tokens(&query.predicate, &mut tokens) || tokens.is_empty() {
            return Ok(None);
        }
        let requested = tokens
            .iter()
            .map(|(value, sensitivity)| (value.as_ref(), *sensitivity))
            .collect::<Vec<_>>();
        let postings = self.cached_exact_message_terms(&cached, &requested)?;
        let postings = tokens
            .into_iter()
            .zip(postings)
            .map(|((value, sensitivity), posting)| {
                (
                    (value, sensitivity),
                    posting.map(|posting| posting.to_vec()).unwrap_or_default(),
                )
            })
            .collect::<HashMap<_, _>>();
        let candidates = evaluate(&query.predicate, &postings, frame.record_count);
        if let (Some(key), Some(candidates)) = (message_predicate_key, candidates.as_ref()) {
            cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .insert(Arc::clone(key), Arc::from(candidates.clone()));
        }
        Ok(candidates)
    }

    pub(super) fn count_cached_exact_candidates(
        &self,
        query: &LogQuery,
        frame_id: u64,
        candidates: &[u32],
    ) -> Option<u64> {
        if query.start_timestamp_unix_nanos.is_none() && query.end_timestamp_unix_nanos.is_none() {
            return Some(u64::try_from(candidates.len()).unwrap_or(u64::MAX));
        }
        let cached = self.cached_indexed_frame_if_present(frame_id)?;
        let count = count_cached_timestamp_candidates(query, &cached, candidates);
        Some(u64::try_from(count).unwrap_or(u64::MAX))
    }

    pub(super) fn count_cached_message_predicate_candidates(
        &self,
        query: &LogQuery,
        frame_id: u64,
        candidates: &[u32],
    ) -> Option<u64> {
        if query.start_timestamp_unix_nanos.is_none() && query.end_timestamp_unix_nanos.is_none() {
            return Some(u64::try_from(candidates.len()).unwrap_or(u64::MAX));
        }
        let cached = self.cached_indexed_frame_if_present(frame_id)?;
        let count = count_cached_timestamp_candidates(query, &cached, candidates);
        Some(u64::try_from(count).unwrap_or(u64::MAX))
    }

    pub(super) fn cached_indexed_frame(
        &self,
        frame: &IndexedIngestFrame,
    ) -> TelemetryResult<Arc<CachedIndexedFrame>> {
        {
            let mut cache = self
                .indexed_frame_query_cache
                .lock()
                .expect("indexed frame query cache lock is not poisoned");
            if let Some(cached) = cache.get(frame.frame_id) {
                return Ok(cached);
            }
        }

        let structural = Arc::<[u8]>::from(decompress_indexed_ingest_frame(frame)?);
        let embedded_index = Arc::new(crate::structural::decode_embedded_frame_index(&structural)?);
        let templates = Arc::<[Vec<Vec<u8>>]>::from(decode_structural_templates(&structural)?);
        let attribute_tables = Arc::new(decode_structural_attribute_tables(&structural)?);
        let (offsets, timestamps) = decode_structural_positions(&structural)?;
        let cached = Arc::new(CachedIndexedFrame {
            structural,
            embedded_index,
            templates,
            attribute_tables,
            offsets: Arc::from(offsets),
            timestamps: Arc::from(timestamps),
            message_bodies: Mutex::new(CachedFrameMessages::default()),
            metadata_fields: Mutex::new(CachedFrameFields::default()),
            trace_ids: Mutex::new(None),
            typed_metadata: Mutex::new(None),
            exact_message_terms: Mutex::new(HashMap::new()),
            message_token_stats: Mutex::new(None),
            message_predicate_candidates: Mutex::new(HashMap::new()),
            exact_fields: Mutex::new(HashMap::new()),
            field_postings: Mutex::new(HashMap::new()),
        });
        let mut cache = self
            .indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned");
        if let Some(existing) = cache.get(frame.frame_id) {
            return Ok(existing);
        }
        cache.insert(frame.frame_id, Arc::clone(&cached));
        Ok(cached)
    }

    pub(super) fn cached_typed_metadata(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        record_count: u32,
    ) -> TelemetryResult<Arc<CachedTypedMetadata>> {
        {
            let typed_metadata = cached
                .typed_metadata
                .lock()
                .expect("indexed frame typed metadata cache lock is not poisoned");
            if let Some(typed_metadata) = typed_metadata.as_ref() {
                return Ok(Arc::clone(typed_metadata));
            }
        }
        let record_count =
            usize::try_from(record_count).map_err(|_| TelemetryError::RecordTooLarge)?;
        let (packed, raw_bytes) =
            decode_structural_typed_metadata(&cached.structural, record_count)?;
        let computed = Arc::new(CachedTypedMetadata {
            packed: Arc::new(packed),
            // The decompressed representation bounds the serialized values;
            // include a small allowance for Vec/Arc bookkeeping in the cache
            // budget so a typed lane cannot consume the whole query cache.
            cache_bytes: raw_bytes.saturating_mul(2),
        });
        let mut typed_metadata = cached
            .typed_metadata
            .lock()
            .expect("indexed frame typed metadata cache lock is not poisoned");
        if typed_metadata.is_none() {
            *typed_metadata = Some(Arc::clone(&computed));
        }
        let result = typed_metadata
            .as_ref()
            .expect("typed metadata was inserted")
            .clone();
        drop(typed_metadata);
        self.indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned")
            .enforce_budget();
        Ok(result)
    }

    pub(super) fn cached_trace_ids(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        record_count: u32,
    ) -> TelemetryResult<Arc<[Option<TraceId>]>> {
        {
            let trace_ids = cached
                .trace_ids
                .lock()
                .expect("indexed frame trace ID cache lock is not poisoned");
            if let Some(trace_ids) = trace_ids.as_ref() {
                return Ok(Arc::clone(trace_ids));
            }
        }
        let ordinals = (0..record_count).collect::<Vec<_>>();
        let decoded = decode_structural_trace_ids(&cached.structural, &ordinals)?;
        let decoded = Arc::<[Option<TraceId>]>::from(decoded);
        let mut trace_ids = cached
            .trace_ids
            .lock()
            .expect("indexed frame trace ID cache lock is not poisoned");
        if trace_ids.is_none() {
            *trace_ids = Some(Arc::clone(&decoded));
        }
        drop(trace_ids);
        self.indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned")
            .enforce_budget();
        Ok(cached
            .trace_ids
            .lock()
            .expect("indexed frame trace ID cache lock is not poisoned")
            .as_ref()
            .cloned()
            .unwrap_or(decoded))
    }

    pub(super) fn cached_indexed_frame_if_present(
        &self,
        frame_id: u64,
    ) -> Option<Arc<CachedIndexedFrame>> {
        self.indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned")
            .get(frame_id)
    }

    pub(super) fn cached_message_predicate_candidates_if_present(
        &self,
        frame_id: u64,
        predicate: &LogPredicate,
    ) -> Option<Vec<u32>> {
        let key = Self::cached_message_predicate_key(predicate)?;
        self.cached_message_predicate_candidates_arc_if_present(frame_id, Some(&key))
            .map(|candidates| candidates.to_vec())
    }

    pub(super) fn cached_message_predicate_candidates_arc_if_present(
        &self,
        frame_id: u64,
        cache_key: Option<&Arc<str>>,
    ) -> Option<Arc<[u32]>> {
        let key = cache_key?;
        let cached = self.cached_indexed_frame_if_present(frame_id)?;
        cached
            .message_predicate_candidates
            .lock()
            .expect("indexed frame message predicate cache lock is not poisoned")
            .get(key)
            .cloned()
    }

    pub(super) fn cached_exact_posting(&self, key: &ExactPostingKey) -> Option<Arc<[u32]>> {
        self.exact_posting_cache
            .lock()
            .expect("exact posting cache lock is not poisoned")
            .get(key)
    }

    pub(super) fn cache_exact_posting(&self, key: ExactPostingKey, posting: Arc<[u32]>) {
        self.exact_posting_cache
            .lock()
            .expect("exact posting cache lock is not poisoned")
            .insert(key, posting);
    }

    pub(super) fn cached_exact_frame_candidates(
        &self,
        frame_id: u64,
        exact_tokens: &[(&str, CaseSensitivity)],
        exact_fields: &[(Arc<str>, Arc<str>)],
    ) -> Option<Vec<u32>> {
        if exact_tokens.is_empty() && exact_fields.is_empty() {
            return None;
        }
        let mut candidates = None;
        for (token, case_sensitivity) in exact_tokens {
            let key = exact_message_posting_key(frame_id, token, *case_sensitivity);
            let posting = self.cached_exact_posting(&key)?;
            intersect_frame_candidate_slice(&mut candidates, &posting);
            if candidates.as_ref().is_some_and(Vec::is_empty) {
                return Some(Vec::new());
            }
        }
        for (key, value) in exact_fields {
            let posting = self.cached_exact_posting(&ExactPostingKey::Field(
                frame_id,
                Arc::clone(key),
                Arc::clone(value),
            ))?;
            intersect_frame_candidate_slice(&mut candidates, &posting);
            if candidates.as_ref().is_some_and(Vec::is_empty) {
                return Some(Vec::new());
            }
        }
        candidates
    }

    pub(super) fn cached_exact_message_terms(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        requested: &[(&str, CaseSensitivity)],
    ) -> TelemetryResult<Vec<Option<Arc<[u32]>>>> {
        let requested = requested
            .iter()
            .map(|(token, case_sensitivity)| {
                let normalized = match case_sensitivity {
                    CaseSensitivity::Sensitive => Arc::<str>::from(*token),
                    CaseSensitivity::Insensitive => Arc::<str>::from(token.to_ascii_lowercase()),
                };
                (normalized, *case_sensitivity)
            })
            .collect::<Vec<_>>();
        let missing = {
            let cached_terms = cached
                .exact_message_terms
                .lock()
                .expect("exact frame term cache lock is not poisoned");
            let missing = requested
                .iter()
                .filter(|term| !cached_terms.contains_key(*term))
                .cloned()
                .collect::<Vec<_>>();
            if missing.is_empty() {
                return Ok(requested
                    .iter()
                    .map(|term| cached_terms.get(term).cloned())
                    .collect());
            }
            missing
        };
        if !missing.is_empty() {
            let mut postings = missing
                .iter()
                .map(|term| (term.clone(), Vec::new()))
                .collect::<Vec<_>>();
            let mut verify_ordinals = Vec::new();
            for (missing_index, (term, case_sensitivity)) in missing.iter().enumerate() {
                if !cached.embedded_index.term_might_contain(term) {
                    continue;
                }
                let static_layouts = cached.embedded_index.term_layout_ids(term);
                let guaranteed_layouts = static_layouts
                    .iter()
                    .copied()
                    .filter(|layout_id| {
                        cached
                            .templates
                            .get(*layout_id as usize)
                            .is_some_and(|literals| {
                                Self::template_literals_contain_term(
                                    literals,
                                    term,
                                    *case_sensitivity,
                                )
                            })
                    })
                    .collect::<Vec<_>>();
                let mut guaranteed_layouts = guaranteed_layouts;
                guaranteed_layouts.sort_unstable();
                guaranteed_layouts.dedup();
                postings[missing_index].1.extend(
                    cached
                        .embedded_index
                        .record_ordinals_for_layout_ids(&guaranteed_layouts),
                );
                let verify_layouts = static_layouts
                    .into_iter()
                    .filter(|layout_id| guaranteed_layouts.binary_search(layout_id).is_err())
                    .chain(
                        cached
                            .embedded_index
                            .residual_layout_ids()
                            .iter()
                            .copied()
                            .filter(|layout_id| {
                                guaranteed_layouts.binary_search(layout_id).is_err()
                            }),
                    )
                    .collect::<Vec<_>>();
                let mut verify_layouts = verify_layouts;
                verify_layouts.sort_unstable();
                verify_layouts.dedup();
                verify_ordinals.extend(
                    cached
                        .embedded_index
                        .record_ordinals_for_layout_ids(&verify_layouts),
                );
            }
            verify_ordinals.sort_unstable();
            verify_ordinals.dedup();
            // Exact cardinality queries only need postings for the requested
            // terms. Building the full relevance statistics table here also
            // allocates a posting list for every token in the frame, which
            // made the first SearchBench token query pay the cost of an
            // unrelated relevance index. The embedded token index narrows the
            // verification decode before those requested terms are cached.
            // Keep the broader relevance cache lazy for top-k and phrase
            // queries.
            let messages = decode_structural_messages_with_embedded_index_and_templates(
                &cached.structural,
                &verify_ordinals,
                &cached.embedded_index,
                &cached.templates,
            )?;
            for (ordinal, message) in verify_ordinals.into_iter().zip(messages) {
                let mut seen = Vec::<usize>::new();
                scan_clickhouse_tokens(&message, |token| {
                    for (index, expected) in missing.iter().enumerate() {
                        if seen.contains(&index) {
                            continue;
                        }
                        let matched = match expected.1 {
                            CaseSensitivity::Sensitive => token == expected.0.as_ref(),
                            CaseSensitivity::Insensitive => {
                                token.eq_ignore_ascii_case(expected.0.as_ref())
                            }
                        };
                        if matched {
                            seen.push(index);
                            postings[index].1.push(ordinal);
                        }
                    }
                });
            }
            for (_, ordinals) in &mut postings {
                ordinals.sort_unstable();
                ordinals.dedup();
            }
            let computed = postings
                .into_iter()
                .map(|(term, ordinals)| (term, Arc::<[u32]>::from(ordinals)))
                .collect::<HashMap<_, _>>();
            let mut cached_terms = cached
                .exact_message_terms
                .lock()
                .expect("exact frame term cache lock is not poisoned");
            for (term, posting) in &computed {
                if cached_terms.contains_key(term)
                    || cached_terms.len() < MAX_EXACT_FRAME_QUERY_TERMS
                {
                    cached_terms.insert(term.clone(), Arc::clone(posting));
                }
            }
            return Ok(requested
                .iter()
                .map(|term| {
                    cached_terms
                        .get(term)
                        .cloned()
                        .or_else(|| computed.get(term).cloned())
                })
                .collect());
        }
        let cached_terms = cached
            .exact_message_terms
            .lock()
            .expect("exact frame term cache lock is not poisoned");
        Ok(requested
            .iter()
            .map(|term| cached_terms.get(term).cloned())
            .collect())
    }

    pub(super) fn cached_message_token_stats(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        record_count: u32,
    ) -> TelemetryResult<Arc<CachedMessageTokenStats>> {
        {
            let postings = cached
                .message_token_stats
                .lock()
                .expect("indexed frame message token cache lock is not poisoned");
            if let Some(postings) = postings.as_ref() {
                return Ok(Arc::clone(postings));
            }
        }
        let ordinals = (0..record_count).collect::<Vec<_>>();
        let messages = decode_structural_messages_with_embedded_index_and_templates(
            &cached.structural,
            &ordinals,
            &cached.embedded_index,
            &cached.templates,
        )?;
        let messages = Arc::<[Arc<str>]>::from(messages);
        let mut postings = HashMap::<Arc<str>, Vec<(u32, u32)>>::new();
        let mut document_lengths = Vec::with_capacity(messages.len());
        let mut token_ids_by_term = HashMap::<Arc<str>, u32>::new();
        let mut token_sequence = Vec::new();
        let mut token_offsets = Vec::with_capacity(messages.len().saturating_add(1));
        token_offsets.push(0_u32);
        for (ordinal, message) in ordinals.into_iter().zip(messages.iter()) {
            let mut counts = HashMap::<Arc<str>, u32>::new();
            let mut document_length = 0_u32;
            scan_clickhouse_tokens(message, |token| {
                document_length = document_length.saturating_add(1);
                let normalized = Arc::<str>::from(normalize_term(token).as_ref());
                let token_id = match token_ids_by_term.get(&normalized) {
                    Some(token_id) => *token_id,
                    None => {
                        let token_id = u32::try_from(token_ids_by_term.len()).unwrap_or(u32::MAX);
                        token_ids_by_term.insert(Arc::clone(&normalized), token_id);
                        token_id
                    }
                };
                token_sequence.push(token_id);
                let frequency = counts.entry(normalized).or_default();
                *frequency = frequency.saturating_add(1);
            });
            document_lengths.push(document_length);
            token_offsets.push(
                u32::try_from(token_sequence.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            );
            for (token, frequency) in counts {
                postings
                    .entry(token)
                    .or_default()
                    .push((ordinal, frequency));
            }
        }
        let postings = postings
            .into_iter()
            .map(|(token, entries)| {
                let (ordinals, frequencies): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
                (
                    token,
                    Arc::new(MessageTokenPosting {
                        ordinals: Arc::from(ordinals),
                        frequencies: Arc::from(frequencies),
                    }),
                )
            })
            .collect::<HashMap<_, _>>();
        let computed = Arc::new(CachedMessageTokenStats {
            postings,
            document_lengths: Arc::from(document_lengths),
            messages,
            token_ids_by_term,
            token_sequence: Arc::from(token_sequence),
            token_offsets: Arc::from(token_offsets),
        });
        let mut cached_postings = cached
            .message_token_stats
            .lock()
            .expect("indexed frame message token cache lock is not poisoned");
        if cached_postings.is_none() {
            *cached_postings = Some(Arc::clone(&computed));
        }
        let result = cached_postings
            .as_ref()
            .expect("message token postings were inserted")
            .clone();
        drop(cached_postings);
        self.indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned")
            .enforce_budget();
        Ok(result)
    }

    pub(super) fn template_literals_contain_term(
        literals: &[Vec<u8>],
        term: &str,
        case_sensitivity: CaseSensitivity,
    ) -> bool {
        literals.iter().any(|literal| {
            let Ok(literal) = std::str::from_utf8(literal) else {
                return false;
            };
            let mut found = false;
            scan_clickhouse_tokens(literal, |token| {
                found |= match case_sensitivity {
                    CaseSensitivity::Sensitive => token == term,
                    CaseSensitivity::Insensitive => token.eq_ignore_ascii_case(term),
                };
            });
            found
        })
    }

    pub(super) fn template_literals_contain_phrase(
        literals: &[Vec<u8>],
        terms: &[Arc<str>],
        max_gap: usize,
        case_sensitivity: CaseSensitivity,
    ) -> bool {
        literals.iter().any(|literal| {
            let Ok(literal) = std::str::from_utf8(literal) else {
                return false;
            };
            crate::query::message_has_phrase(literal, terms, max_gap, case_sensitivity)
        })
    }

    pub(super) fn cached_message_predicate_key(predicate: &LogPredicate) -> Option<Arc<str>> {
        let sensitivity = |case_sensitivity: CaseSensitivity| match case_sensitivity {
            CaseSensitivity::Sensitive => 's',
            CaseSensitivity::Insensitive => 'i',
        };
        if let Some((tokens, minimum)) = message_token_min_match_shape(predicate) {
            return Some(Arc::from(format!(
                "min-match:{}:{}",
                minimum,
                tokens
                    .iter()
                    .map(|(value, case_sensitivity)| {
                        format!("{}:{}", sensitivity(*case_sensitivity), value)
                    })
                    .collect::<Vec<_>>()
                    .join("\u{1f}")
            )));
        }
        match predicate {
            LogPredicate::Term(term) => Some(Arc::from(format!("term:{term}"))),
            LogPredicate::MessageToken {
                value,
                case_sensitivity,
            } => Some(Arc::from(format!(
                "token:{}:{}",
                sensitivity(*case_sensitivity),
                value
            ))),
            LogPredicate::MessageTokenRegex(regex) => Some(Arc::from(format!(
                "token-regex:{}:{}",
                sensitivity(regex.case_sensitivity()),
                regex.pattern()
            ))),
            LogPredicate::MessageTokenPrefix {
                value,
                case_sensitivity,
            } => Some(Arc::from(format!(
                "token-prefix:{}:{}",
                sensitivity(*case_sensitivity),
                value
            ))),
            LogPredicate::MessageFuzzy {
                value,
                max_distance,
            } => Some(Arc::from(format!("token-fuzzy:{max_distance}:{value}"))),
            LogPredicate::MessagePhrase {
                terms,
                max_gap,
                case_sensitivity,
            } => Some(Arc::from(format!(
                "phrase:{}:{}:{}",
                sensitivity(*case_sensitivity),
                max_gap,
                terms
                    .iter()
                    .map(AsRef::as_ref)
                    .collect::<Vec<&str>>()
                    .join("\u{1f}")
            ))),
            LogPredicate::MessageRegex(regex) => Some(Arc::from(format!(
                "message-regex:{}:{}",
                sensitivity(regex.case_sensitivity()),
                regex.pattern()
            ))),
            LogPredicate::Message(matcher)
                if matcher.kind != crate::TextMatchKind::Exact
                    && !matcher.value.is_empty()
                    && matcher
                        .value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric()) =>
            {
                let kind = match matcher.kind {
                    crate::TextMatchKind::Contains => 'c',
                    crate::TextMatchKind::Prefix => 'p',
                    crate::TextMatchKind::Suffix => 's',
                    crate::TextMatchKind::Exact => unreachable!(),
                };
                Some(Arc::from(format!(
                    "message-literal:{kind}:{}:{}",
                    sensitivity(matcher.case_sensitivity),
                    matcher.value
                )))
            }
            LogPredicate::And(predicates) => {
                let parts = predicates
                    .iter()
                    .filter_map(Self::cached_message_predicate_key)
                    .collect::<Vec<_>>();
                (!parts.is_empty()).then(|| {
                    Arc::from(format!(
                        "and:{}",
                        parts
                            .iter()
                            .map(AsRef::as_ref)
                            .collect::<Vec<&str>>()
                            .join("\u{1f}")
                    ))
                })
            }
            LogPredicate::Or(predicates) => {
                let parts = predicates
                    .iter()
                    .map(Self::cached_message_predicate_key)
                    .collect::<Option<Vec<_>>>()?;
                Some(Arc::from(format!(
                    "or:{}",
                    parts
                        .iter()
                        .map(AsRef::as_ref)
                        .collect::<Vec<&str>>()
                        .join("\u{1f}")
                )))
            }
            LogPredicate::Not(predicate) => Self::cached_message_predicate_key(predicate)
                .map(|part| Arc::from(format!("not:{part}"))),
            _ => None,
        }
    }

    pub(super) fn cached_message_predicate_candidates(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        let cache_key = Self::cached_message_predicate_key(&query.predicate);
        self.cached_message_predicate_candidates_with_key(query, frame, cache_key.as_ref())
    }

    pub(super) fn cached_message_predicate_candidates_with_key(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        cache_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        self.cached_message_predicate_candidates_with_key_mode(query, frame, cache_key, true)
    }

    pub(super) fn cached_message_predicate_candidates_for_relevance(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        cache_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        self.cached_message_predicate_candidates_with_key_mode(query, frame, cache_key, false)
    }

    pub(super) fn cached_message_predicate_candidates_with_key_mode(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        cache_key: Option<&Arc<str>>,
        allow_structural_message_fast_path: bool,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        let cached = self.cached_indexed_frame(frame)?;
        if let Some(key) = cache_key
            && let Some(candidates) = cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .get(key)
                .cloned()
        {
            return Ok(Some(candidates.to_vec()));
        }
        if let Some(tokens) = query
            .exact_message_token_conjunction()
            .filter(|tokens| !tokens.is_empty())
        {
            let postings = self.cached_exact_message_terms(&cached, &tokens)?;
            let mut candidates = None;
            for posting in postings {
                let Some(posting) = posting else {
                    candidates = Some(Vec::new());
                    break;
                };
                intersect_frame_candidate_slice(&mut candidates, &posting);
                if candidates.as_ref().is_some_and(Vec::is_empty) {
                    break;
                }
            }
            let candidates = candidates.unwrap_or_default();
            if let Some(key) = cache_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        if let Some(tokens) = query
            .exact_message_token_disjunction()
            .filter(|tokens| !tokens.is_empty())
        {
            let postings = self.cached_exact_message_terms(&cached, &tokens)?;
            let mut candidates = Vec::new();
            for posting in postings.into_iter().flatten() {
                union_sorted_ordinals(&mut candidates, posting.to_vec());
            }
            if let Some(key) = cache_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        // A Boolean token predicate can include NOT. The embedded index can
        // narrow its positive terms, but that is only a superset; caching it
        // under an exact predicate key lets later relevance scans admit rows
        // that the negated term excludes.
        if cached_message_predicate_is_exact(&query.predicate)
            && let Some(candidates) =
                self.exact_boolean_message_candidates(query, frame, cache_key)?
        {
            return Ok(Some(candidates));
        }
        // A field-only predicate has no message candidate source. Returning
        // early avoids constructing the full token-statistics cache just to
        // discover that it cannot narrow the frame. For a mixed AND, the
        // embedded index can still provide a safe token superset while the
        // normal residual field matcher verifies the complete predicate.
        if cache_key.is_none() {
            return Ok(None);
        }
        if let LogPredicate::MessagePhrase {
            terms,
            max_gap,
            case_sensitivity,
        } = &query.predicate
            && !terms.is_empty()
        {
            let requested = terms
                .iter()
                .map(|term| (term.as_ref(), *case_sensitivity))
                .collect::<Vec<_>>();
            let postings = self.cached_exact_message_terms(&cached, &requested)?;
            let mut ordinals = None;
            for posting in postings {
                let Some(posting) = posting else {
                    ordinals = Some(Vec::new());
                    break;
                };
                intersect_frame_candidate_slice(&mut ordinals, &posting);
                if ordinals.as_ref().is_some_and(Vec::is_empty) {
                    break;
                }
            }
            let ordinals = ordinals.unwrap_or_default();
            let mut phrase_layouts = None;
            for term in terms {
                let mut term_layouts = cached.embedded_index.term_layout_ids(term);
                term_layouts.sort_unstable();
                term_layouts.dedup();
                intersect_frame_candidate_slice(&mut phrase_layouts, &term_layouts);
            }
            let static_layouts = phrase_layouts
                .unwrap_or_default()
                .into_iter()
                .filter(|layout_id| {
                    cached
                        .templates
                        .get(*layout_id as usize)
                        .is_some_and(|literals| {
                            Self::template_literals_contain_phrase(
                                literals,
                                terms,
                                *max_gap,
                                *case_sensitivity,
                            )
                        })
                })
                .collect::<Vec<_>>();
            let static_matches = cached
                .embedded_index
                .record_ordinals_for_layout_ids(&static_layouts);
            let mut verify_ordinals =
                Vec::with_capacity(ordinals.len().saturating_sub(static_matches.len()));
            let mut static_index = 0;
            for ordinal in ordinals {
                while static_index < static_matches.len() && static_matches[static_index] < ordinal
                {
                    static_index += 1;
                }
                if static_index >= static_matches.len() || static_matches[static_index] != ordinal {
                    verify_ordinals.push(ordinal);
                }
            }
            let messages = decode_structural_messages_with_embedded_index_and_templates(
                &cached.structural,
                &verify_ordinals,
                &cached.embedded_index,
                &cached.templates,
            )?;
            let mut candidates = static_matches;
            candidates.extend(verify_ordinals.into_iter().zip(messages).filter_map(
                |(ordinal, message)| {
                    crate::query::message_has_phrase(&message, terms, *max_gap, *case_sensitivity)
                        .then_some(ordinal)
                },
            ));
            candidates.sort_unstable();
            cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .insert(
                    Arc::clone(cache_key.expect("message predicate cache key is present")),
                    Arc::from(candidates.clone()),
                );
            return Ok(Some(candidates));
        }
        if let Some(mut candidates) =
            embedded_message_predicate_candidates(&query.predicate, &cached.embedded_index)
        {
            if cached_message_predicate_is_exact(&query.predicate)
                && message_predicate_is_message_only(&query.predicate)
            {
                // AND may combine an indexed token with a phrase or NOT
                // clause. The embedded result then narrows the scan but does
                // not prove the full predicate, so verify before caching it
                // under a key consumed by exact count and relevance paths.
                let messages = decode_structural_messages_with_embedded_index_and_templates(
                    &cached.structural,
                    &candidates,
                    &cached.embedded_index,
                    &cached.templates,
                )?;
                candidates = candidates
                    .into_iter()
                    .zip(messages)
                    .filter_map(|(ordinal, message)| {
                        query
                            .message_candidate_matches(&message)?
                            .then_some(ordinal)
                    })
                    .collect();
            }
            cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .insert(
                    Arc::clone(cache_key.expect("message predicate cache key is present")),
                    Arc::from(candidates.clone()),
                );
            return Ok(Some(candidates));
        }
        if let LogPredicate::MessagePhrase {
            terms,
            max_gap,
            case_sensitivity,
        } = &query.predicate
        {
            let stats = self.cached_message_token_stats(&cached, frame.record_count)?;
            let postings = &stats.postings;
            let mut ordinals = None;
            for term in terms {
                let candidates = postings
                    .get(normalize_term(term).as_ref())
                    .map(|posting| posting.ordinals.as_ref())
                    .unwrap_or(&[]);
                intersect_frame_candidate_slice(&mut ordinals, candidates);
                if ordinals.as_ref().is_some_and(Vec::is_empty) {
                    break;
                }
            }
            let ordinals = ordinals.unwrap_or_default();
            let candidates = if let Some(candidates) =
                stats.phrase_candidate_ordinals(&ordinals, terms, *max_gap, *case_sensitivity)
            {
                candidates
            } else {
                ordinals
                    .into_iter()
                    .filter_map(|ordinal| {
                        let message = stats.messages.get(ordinal as usize)?;
                        crate::query::message_has_phrase(
                            message,
                            terms,
                            *max_gap,
                            *case_sensitivity,
                        )
                        .then_some(ordinal)
                    })
                    .collect::<Vec<_>>()
            };
            if let Some(key) = cache_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        let token_stats_candidate = match &query.predicate {
            LogPredicate::MessageTokenRegex(_) | LogPredicate::MessageTokenPrefix { .. } => {
                let stats = self.cached_message_token_stats(&cached, frame.record_count)?;
                stats.candidate_ordinals_for_predicate(&query.predicate)
            }
            _ => None,
        };
        if let Some(candidates) = token_stats_candidate {
            if let Some(key) = cache_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        if allow_structural_message_fast_path
            && query.limit.is_none()
            && query.terms.is_empty()
            && message_predicate_is_message_only(&query.predicate)
        {
            let ordinals = (0..frame.record_count).collect::<Vec<_>>();
            let messages = decode_structural_messages_with_embedded_index_and_templates(
                &cached.structural,
                &ordinals,
                &cached.embedded_index,
                &cached.templates,
            )?;
            let candidates = ordinals
                .into_iter()
                .zip(messages)
                .filter_map(|(ordinal, message)| {
                    query
                        .message_candidate_matches(&message)
                        .unwrap_or(false)
                        .then_some(ordinal)
                })
                .collect::<Vec<_>>();
            cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .insert(
                    Arc::clone(cache_key.expect("message predicate cache key is present")),
                    Arc::from(candidates.clone()),
                );
            return Ok(Some(candidates));
        }
        let stats = self.cached_message_token_stats(&cached, frame.record_count)?;
        let postings = &stats.postings;
        if let Some((tokens, minimum)) = message_token_min_match_shape(&query.predicate) {
            let candidates = cached_message_token_min_match_candidates(
                postings,
                frame.record_count,
                &tokens,
                minimum,
            );
            if let Some(key) = cache_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        fn token_posting_candidates(
            postings: &MessageTokenPostings,
            mut matches_token: impl FnMut(&str) -> bool,
        ) -> Vec<u32> {
            let mut candidates = Vec::new();
            for (token, posting) in postings {
                if matches_token(token) {
                    union_sorted_ordinals(&mut candidates, posting.ordinals.to_vec());
                }
            }
            candidates
        }
        fn exact_token_candidates(postings: &MessageTokenPostings, token: &str) -> Vec<u32> {
            postings
                .get(normalize_term(token).as_ref())
                .map(|posting| posting.ordinals.to_vec())
                .unwrap_or_default()
        }
        fn phrase_candidates(
            postings: &MessageTokenPostings,
            terms: &[Arc<str>],
            record_count: u32,
        ) -> Vec<u32> {
            if terms.is_empty() {
                return (0..record_count).collect();
            }
            let mut current = None;
            for term in terms {
                let candidates = exact_token_candidates(postings, term);
                intersect_frame_candidate_slice(&mut current, &candidates);
                if current.as_ref().is_some_and(Vec::is_empty) {
                    return Vec::new();
                }
            }
            current.unwrap_or_default()
        }
        fn candidates_for(
            predicate: &LogPredicate,
            postings: &MessageTokenPostings,
            record_count: u32,
        ) -> Option<Vec<u32>> {
            match predicate {
                LogPredicate::MatchAll => Some((0..record_count).collect()),
                LogPredicate::MatchNone => Some(Vec::new()),
                LogPredicate::Term(term) | LogPredicate::MessageToken { value: term, .. } => {
                    Some(exact_token_candidates(postings, term))
                }
                LogPredicate::MessageTokenPrefix { value, .. } => {
                    let prefix = normalize_term(value);
                    Some(token_posting_candidates(postings, |token| {
                        token.starts_with(prefix.as_ref())
                    }))
                }
                LogPredicate::MessageTokenRegex(regex) => {
                    if regex.case_sensitivity() == CaseSensitivity::Sensitive
                        && regex
                            .pattern()
                            .bytes()
                            .any(|byte| byte.is_ascii_uppercase())
                    {
                        return Some((0..record_count).collect());
                    }
                    Some(token_posting_candidates(postings, |token| {
                        regex.is_match(token)
                    }))
                }
                LogPredicate::MessageFuzzy {
                    value,
                    max_distance,
                } => {
                    let value = normalize_term(value);
                    Some(token_posting_candidates(postings, |token| {
                        bounded_levenshtein(token, value.as_ref(), usize::from(*max_distance))
                    }))
                }
                LogPredicate::MessagePhrase { terms, .. } => {
                    Some(phrase_candidates(postings, terms, record_count))
                }
                LogPredicate::MessageRegex(regex) => {
                    let literals = crate::query::regex_required_literals(regex.pattern())?;
                    if let Some(literal) = regex_boundary_safe_literal(regex.pattern()) {
                        return Some(exact_token_candidates(postings, literal));
                    }
                    Some(token_posting_candidates(postings, |token| {
                        literals.iter().any(|literal| {
                            normalize_term(token).contains(normalize_term(literal).as_ref())
                        })
                    }))
                }
                LogPredicate::Message(matcher)
                    if matcher.kind != crate::TextMatchKind::Exact
                        && !matcher.value.is_empty()
                        && matcher
                            .value
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric()) =>
                {
                    let literal = normalize_term(&matcher.value);
                    Some(token_posting_candidates(postings, |token| {
                        token.contains(literal.as_ref())
                    }))
                }
                LogPredicate::Message(_) => None,
                LogPredicate::And(predicates) => {
                    let mut current = None;
                    for predicate in predicates {
                        let Some(candidates) = candidates_for(predicate, postings, record_count)
                        else {
                            continue;
                        };
                        intersect_frame_candidate_slice(&mut current, &candidates);
                        if current.as_ref().is_some_and(Vec::is_empty) {
                            return Some(Vec::new());
                        }
                    }
                    current
                }
                LogPredicate::Or(predicates) => {
                    let mut current = Vec::new();
                    for predicate in predicates {
                        let candidates = candidates_for(predicate, postings, record_count)?;
                        union_sorted_ordinals(&mut current, candidates);
                    }
                    Some(current)
                }
                LogPredicate::Not(predicate) => {
                    let excluded = candidates_for(predicate, postings, record_count)?;
                    let mut candidates =
                        Vec::with_capacity((record_count as usize).saturating_sub(excluded.len()));
                    let mut next = 0_u32;
                    for ordinal in excluded {
                        if ordinal < next || ordinal >= record_count {
                            continue;
                        }
                        candidates.extend(next..ordinal);
                        next = ordinal.saturating_add(1);
                    }
                    if next < record_count {
                        candidates.extend(next..record_count);
                    }
                    Some(candidates)
                }
                LogPredicate::FieldExists(_)
                | LogPredicate::Field { .. }
                | LogPredicate::FieldIn { .. }
                | LogPredicate::FieldRegex { .. }
                | LogPredicate::FieldNumeric { .. } => None,
            }
        }
        let candidates = candidates_for(&query.predicate, postings, frame.record_count);
        if let (Some(key), Some(candidates)) = (cache_key, candidates.as_ref()) {
            cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .insert(Arc::clone(key), Arc::from(candidates.clone()));
        }
        Ok(candidates)
    }

    pub(super) fn cached_exact_fields(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        requested: &[(Arc<str>, Arc<str>)],
    ) -> TelemetryResult<Vec<Option<Arc<[u32]>>>> {
        let missing = {
            let cached_fields = cached
                .exact_fields
                .lock()
                .expect("exact frame field cache lock is not poisoned");
            let missing = requested
                .iter()
                .filter(|field| !cached_fields.contains_key(*field))
                .cloned()
                .collect::<Vec<_>>();
            if missing.is_empty() {
                return Ok(requested
                    .iter()
                    .map(|field| cached_fields.get(field).cloned())
                    .collect());
            }
            missing
        };
        if !missing.is_empty() {
            let mut ordinals = Vec::new();
            for (key, value) in &missing {
                union_sorted_ordinals(
                    &mut ordinals,
                    cached.embedded_index.field_candidate_ordinals(key, value),
                );
            }
            let mut wanted_keys = missing
                .iter()
                .map(|(key, _)| key.as_ref())
                .collect::<Vec<_>>();
            wanted_keys.sort_unstable();
            wanted_keys.dedup();
            let fields = crate::structural::decode_structural_fields_for_keys(
                &cached.structural,
                &ordinals,
                &wanted_keys,
            )?;
            let mut postings = missing
                .iter()
                .map(|field| (field.clone(), Vec::new()))
                .collect::<Vec<_>>();
            for (ordinal, fields) in ordinals.into_iter().zip(fields) {
                let mut seen = Vec::<usize>::new();
                for field in fields.iter() {
                    let Some(index) = missing
                        .iter()
                        .position(|expected| expected.0 == field.key && expected.1 == field.value)
                    else {
                        continue;
                    };
                    if seen.contains(&index) {
                        continue;
                    }
                    seen.push(index);
                    postings[index].1.push(ordinal);
                }
            }
            let computed = postings
                .into_iter()
                .map(|(field, ordinals)| (field, Arc::<[u32]>::from(ordinals)))
                .collect::<HashMap<_, _>>();
            let mut cached_fields = cached
                .exact_fields
                .lock()
                .expect("exact frame field cache lock is not poisoned");
            for (field, posting) in &computed {
                if cached_fields.contains_key(field)
                    || cached_fields.len() < MAX_EXACT_FRAME_QUERY_FIELDS
                {
                    cached_fields.insert(field.clone(), Arc::clone(posting));
                }
            }
            return Ok(requested
                .iter()
                .map(|field| {
                    cached_fields
                        .get(field)
                        .cloned()
                        .or_else(|| computed.get(field).cloned())
                })
                .collect());
        }
        let cached_fields = cached
            .exact_fields
            .lock()
            .expect("exact frame field cache lock is not poisoned");
        Ok(requested
            .iter()
            .map(|field| cached_fields.get(field).cloned())
            .collect())
    }

    pub(super) fn cached_field_postings(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        record_count: u32,
        key: &str,
    ) -> TelemetryResult<Option<Arc<CachedFieldPostings>>> {
        let key = Arc::<str>::from(key);
        {
            let field_postings = cached
                .field_postings
                .lock()
                .expect("indexed frame field postings lock is not poisoned");
            if let Some(postings) = field_postings.get(&key) {
                return Ok(Some(Arc::clone(postings)));
            }
        }

        let ordinals = (0..record_count).collect::<Vec<_>>();
        let fields = crate::structural::decode_structural_fields_for_keys(
            &cached.structural,
            &ordinals,
            &[key.as_ref()],
        )?;
        let mut values = HashMap::<Arc<str>, Vec<u32>>::new();
        let mut presence = Vec::new();
        let mut value_ids = HashMap::<Arc<str>, u32>::new();
        let mut value_table = Vec::<Arc<str>>::new();
        let mut ordinal_value_ids =
            vec![u32::MAX; usize::try_from(record_count).unwrap_or_default()];
        for (ordinal, fields) in ordinals.into_iter().zip(fields) {
            for field in fields.iter().filter(|field| field.key == key) {
                let value_id = if let Some(value_id) = value_ids.get(&field.value) {
                    *value_id
                } else {
                    if values.len() >= MAX_INDEXED_FRAME_FIELD_VALUES {
                        return Ok(None);
                    }
                    let value_id =
                        u32::try_from(value_table.len()).expect("indexed field values fit in u32");
                    value_ids.insert(Arc::clone(&field.value), value_id);
                    value_table.push(Arc::clone(&field.value));
                    value_id
                };
                if let Some(slot) = ordinal_value_ids.get_mut(ordinal as usize)
                    && *slot == u32::MAX
                {
                    *slot = value_id;
                }
                let posting = values.entry(Arc::clone(&field.value)).or_default();
                if posting.last().copied() != Some(ordinal) {
                    posting.push(ordinal);
                }
                if presence.last().copied() != Some(ordinal) {
                    presence.push(ordinal);
                }
            }
        }
        let postings = Arc::new(CachedFieldPostings {
            values: values
                .into_iter()
                .map(|(value, ordinals)| (value, Arc::<[u32]>::from(ordinals)))
                .collect(),
            presence: Arc::from(presence),
            ordinal_value_ids: Arc::from(ordinal_value_ids),
            value_table: Arc::from(value_table),
        });
        let mut field_postings = cached
            .field_postings
            .lock()
            .expect("indexed frame field postings lock is not poisoned");
        if let Some(existing) = field_postings.get(&key) {
            return Ok(Some(Arc::clone(existing)));
        }
        if field_postings.len() < MAX_INDEXED_FRAME_FIELD_KEYS {
            field_postings.insert(Arc::clone(&key), Arc::clone(&postings));
        }
        drop(field_postings);
        self.indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned")
            .enforce_budget();
        Ok(Some(postings))
    }

    pub(super) fn cached_field_value_candidates(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        record_count: u32,
        key: &str,
        mut matches_value: impl FnMut(&str) -> bool,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        let Some(postings) = self.cached_field_postings(cached, record_count, key)? else {
            return Ok(None);
        };
        let mut candidates = Vec::new();
        for (value, posting) in &postings.values {
            if matches_value(value) {
                candidates.extend(posting.iter().copied());
            }
        }
        candidates.sort_unstable();
        candidates.dedup();
        Ok(Some(candidates))
    }

    pub(super) fn indexed_frame_field_predicate_candidates(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        candidates: &[u32],
    ) -> TelemetryResult<Option<Vec<u32>>> {
        let required = query.required_index_constraints();
        let has_field_constraints = !required.field_exists.is_empty()
            || !required.field_in.is_empty()
            || !required.field_text.is_empty()
            || !required.field_regex.is_empty()
            || !required.field_numeric.is_empty();
        if !has_field_constraints {
            return Ok(None);
        }
        Ok(Some(self.indexed_frame_field_predicate_candidates_owned(
            query,
            frame,
            candidates.to_vec(),
        )?))
    }

    pub(super) fn indexed_frame_field_predicate_candidates_owned(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        candidates: Vec<u32>,
    ) -> TelemetryResult<Vec<u32>> {
        let required = query.required_index_constraints();
        let has_field_constraints = !required.field_exists.is_empty()
            || !required.field_in.is_empty()
            || !required.field_text.is_empty()
            || !required.field_regex.is_empty()
            || !required.field_numeric.is_empty();
        if !has_field_constraints {
            return Ok(candidates);
        }
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let mut current = Some(candidates);
        for key in &required.field_exists {
            let Some(field_postings) =
                self.cached_field_postings(&cached, frame.record_count, key)?
            else {
                let current_candidates = current.as_deref().unwrap_or(&[]);
                return self.scan_indexed_frame_field_predicates(
                    &cached,
                    current_candidates,
                    &required,
                );
            };
            let postings = field_postings.presence.as_ref();
            intersect_frame_candidate_slice(&mut current, postings);
        }
        for (key, values) in &required.field_in {
            let mut postings = Vec::new();
            for value in values {
                union_sorted_ordinals(
                    &mut postings,
                    cached.embedded_index.field_candidate_ordinals(key, value),
                );
            }
            intersect_frame_candidate_slice(&mut current, &postings);
            let current_candidates = current.as_deref().unwrap_or(&[]);
            if current_candidates.is_empty() {
                continue;
            }
            let fields = crate::structural::decode_structural_fields_for_keys(
                &cached.structural,
                current_candidates,
                &[key],
            )?;
            let verified = current_candidates
                .iter()
                .copied()
                .zip(fields)
                .filter_map(|(ordinal, fields)| {
                    fields
                        .iter()
                        .any(|field| {
                            field.key.as_ref() == *key
                                && values
                                    .iter()
                                    .any(|expected| field.value.as_ref() == *expected)
                        })
                        .then_some(ordinal)
                })
                .collect::<Vec<_>>();
            current = Some(verified);
        }
        for (key, matcher) in &required.field_text {
            let Some(postings) =
                self.cached_field_value_candidates(&cached, frame.record_count, key, |value| {
                    text_matches(value, matcher)
                })?
            else {
                let current_candidates = current.as_deref().unwrap_or(&[]);
                return self.scan_indexed_frame_field_predicates(
                    &cached,
                    current_candidates,
                    &required,
                );
            };
            intersect_frame_candidate_slice(&mut current, &postings);
        }
        for (key, regex) in &required.field_regex {
            let Some(postings) =
                self.cached_field_value_candidates(&cached, frame.record_count, key, |value| {
                    regex.is_match(value)
                })?
            else {
                let current_candidates = current.as_deref().unwrap_or(&[]);
                return self.scan_indexed_frame_field_predicates(
                    &cached,
                    current_candidates,
                    &required,
                );
            };
            intersect_frame_candidate_slice(&mut current, &postings);
        }
        for (key, comparison, target) in &required.field_numeric {
            let Some(postings) =
                self.cached_field_value_candidates(&cached, frame.record_count, key, |value| {
                    value
                        .parse::<i128>()
                        .is_ok_and(|observed| match *comparison {
                            NumericComparison::Equal => observed == *target,
                            NumericComparison::NotEqual => observed != *target,
                            NumericComparison::LessThan => observed < *target,
                            NumericComparison::LessThanOrEqual => observed <= *target,
                            NumericComparison::GreaterThan => observed > *target,
                            NumericComparison::GreaterThanOrEqual => observed >= *target,
                        })
                })?
            else {
                let current_candidates = current.as_deref().unwrap_or(&[]);
                return self.scan_indexed_frame_field_predicates(
                    &cached,
                    current_candidates,
                    &required,
                );
            };
            intersect_frame_candidate_slice(&mut current, &postings);
        }
        Ok(current.unwrap_or_default())
    }

    pub(super) fn scan_indexed_frame_field_predicates(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        candidates: &[u32],
        required: &crate::query::RequiredIndexConstraints<'_>,
    ) -> TelemetryResult<Vec<u32>> {
        let fields = decode_structural_fields(&cached.structural, candidates)?;
        let mut matches = Vec::with_capacity(candidates.len());
        for (ordinal, fields) in candidates.iter().copied().zip(fields) {
            let exists = required
                .field_exists
                .iter()
                .all(|key| fields.iter().any(|field| field.key.as_ref() == *key));
            let in_values = required.field_in.iter().all(|(key, values)| {
                fields.iter().any(|field| {
                    field.key.as_ref() == *key
                        && values
                            .iter()
                            .any(|expected| field.value.as_ref() == *expected)
                })
            });
            let text = required.field_text.iter().all(|(key, matcher)| {
                fields
                    .iter()
                    .any(|field| field.key.as_ref() == *key && text_matches(&field.value, matcher))
            });
            let regex = required.field_regex.iter().all(|(key, regex)| {
                fields
                    .iter()
                    .any(|field| field.key.as_ref() == *key && regex.is_match(&field.value))
            });
            let numeric = required
                .field_numeric
                .iter()
                .all(|(key, comparison, target)| {
                    fields.iter().any(|field| {
                        field.key.as_ref() == *key
                            && field
                                .value
                                .parse::<i128>()
                                .is_ok_and(|observed| match *comparison {
                                    NumericComparison::Equal => observed == *target,
                                    NumericComparison::NotEqual => observed != *target,
                                    NumericComparison::LessThan => observed < *target,
                                    NumericComparison::LessThanOrEqual => observed <= *target,
                                    NumericComparison::GreaterThan => observed > *target,
                                    NumericComparison::GreaterThanOrEqual => observed >= *target,
                                })
                    })
                });
            if exists && in_values && text && regex && numeric {
                matches.push(ordinal);
            }
        }
        Ok(matches)
    }
}
