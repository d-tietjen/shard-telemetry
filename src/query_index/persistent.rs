use super::*;

impl PersistentQueryIndex {
    /// Builds one immutable directory from block-local exact postings.
    pub fn from_blocks(
        mut blocks: Vec<(QueryBlockMetadata, BlockQueryIndex)>,
    ) -> TelemetryResult<Self> {
        blocks.sort_unstable_by_key(|(metadata, _)| metadata.block_ordinal);
        if blocks
            .windows(2)
            .any(|pair| pair[0].0.block_ordinal == pair[1].0.block_ordinal)
        {
            return Err(TelemetryError::InvalidBlockEncoding(
                "duplicate query block ordinal",
            ));
        }
        let mut metadata_entries = Vec::with_capacity(blocks.len());
        let mut message_trigram_words =
            Vec::with_capacity(blocks.len().saturating_mul(MESSAGE_TRIGRAM_FILTER_WORDS));
        let mut case_sensitive_message_trigram_words = Vec::with_capacity(
            blocks
                .len()
                .saturating_mul(CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS),
        );
        let mut partition_blocks = HashMap::<TopicPartition, Vec<u32>>::new();
        let mut term_postings =
            HashMap::<TopicPartition, HashMap<Arc<str>, Vec<BlockPosting>>>::new();
        let mut field_postings = HashMap::<
            TopicPartition,
            HashMap<Arc<str>, HashMap<Arc<str>, Vec<BlockPosting>>>,
        >::new();
        for (metadata, index) in blocks {
            if metadata.record_count != index.record_count
                || metadata.first_offset > metadata.last_offset
                || metadata.min_timestamp_unix_nanos > metadata.max_timestamp_unix_nanos
            {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "invalid query block metadata",
                ));
            }
            partition_blocks
                .entry(metadata.topic_partition)
                .or_default()
                .push(metadata.block_ordinal);
            let partition_terms = term_postings.entry(metadata.topic_partition).or_default();
            for (term, record_ordinals) in index.term_postings {
                partition_terms.entry(term).or_default().push(BlockPosting {
                    block_ordinal: metadata.block_ordinal,
                    record_ordinals,
                });
            }
            let partition_fields = field_postings.entry(metadata.topic_partition).or_default();
            for (key, values) in index.field_postings {
                let indexed_values = partition_fields.entry(key).or_default();
                for (value, record_ordinals) in values {
                    indexed_values.entry(value).or_default().push(BlockPosting {
                        block_ordinal: metadata.block_ordinal,
                        record_ordinals,
                    });
                }
            }
            message_trigram_words.extend_from_slice(&index.message_trigrams.words);
            case_sensitive_message_trigram_words
                .extend_from_slice(&index.case_sensitive_message_trigrams.words);
            metadata_entries.push(metadata);
        }
        let (message_trigram_union, message_trigram_intersection) =
            aggregate_message_trigrams(&message_trigram_words);
        let (case_sensitive_message_trigram_union, case_sensitive_message_trigram_intersection) =
            aggregate_case_sensitive_message_trigrams(&case_sensitive_message_trigram_words);
        Ok(Self {
            blocks: metadata_entries,
            message_trigram_words: message_trigram_words.into_boxed_slice(),
            case_sensitive_message_trigram_words: case_sensitive_message_trigram_words
                .into_boxed_slice(),
            message_trigram_union,
            message_trigram_intersection,
            case_sensitive_message_trigram_union,
            case_sensitive_message_trigram_intersection,
            partition_blocks,
            term_postings,
            field_postings,
        })
    }

    /// Returns the immutable block directory.
    #[must_use]
    pub fn blocks(&self) -> &[QueryBlockMetadata] {
        &self.blocks
    }

    /// Heap bytes occupied by record-ordinal arrays and run tables.
    ///
    /// This excludes hash-table buckets and interned term/field strings.
    #[must_use]
    pub fn posting_storage_bytes(&self) -> usize {
        self.term_postings
            .values()
            .flat_map(HashMap::values)
            .flatten()
            .chain(
                self.field_postings
                    .values()
                    .flat_map(HashMap::values)
                    .flat_map(HashMap::values)
                    .flatten(),
            )
            .map(|posting| posting.record_ordinals.storage_bytes())
            .sum()
    }

    /// Heap bytes occupied by block-level message trigram rejection filters.
    #[must_use]
    pub fn message_trigram_filter_bytes(&self) -> usize {
        self.message_trigram_words
            .len()
            .saturating_add(self.case_sensitive_message_trigram_words.len())
            .saturating_mul(size_of::<u64>())
    }

    /// Logical number of record ordinals represented by all postings.
    #[must_use]
    pub fn posting_cardinality(&self) -> usize {
        self.term_postings
            .values()
            .flat_map(HashMap::values)
            .flatten()
            .chain(
                self.field_postings
                    .values()
                    .flat_map(HashMap::values)
                    .flat_map(HashMap::values)
                    .flatten(),
            )
            .map(|posting| posting.record_ordinals.cardinality())
            .sum()
    }

    /// Plans safe term/field candidates before payload reads and decoding.
    ///
    /// Posting-only queries may return exact hits. Queries containing residual
    /// predicates return a superset. Always pass decoded records through
    /// [`LogQuery::select`] before returning them to a caller. Offset and
    /// timestamp ranges prune whole blocks here, while boundary records are
    /// checked after selective decoding. A limit is applied during planning
    /// only when the complete query can be answered safely by postings.
    #[must_use]
    pub fn candidate_hits(&self, query: &LogQuery) -> Vec<QueryHit> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Vec::new();
        }
        let required = query.required_index_constraints();
        if required.impossible {
            return Vec::new();
        }
        let Some(required_message_slots) =
            self.prepare_required_message_trigrams(&required.message_literals)
        else {
            return Vec::new();
        };
        let Some(required_case_sensitive_message_slots) =
            self.prepare_case_sensitive_message_trigrams(&required.case_sensitive_message_literals)
        else {
            return Vec::new();
        };
        let mut constraints = Vec::<&[BlockPosting]>::with_capacity(
            required.terms.len().saturating_add(required.fields.len()),
        );
        let partition_terms = self.term_postings.get(&query.topic_partition);
        for &term in &required.terms {
            let normalized = normalize_term(term);
            let Some(postings) = partition_terms.and_then(|terms| terms.get(normalized.as_ref()))
            else {
                return Vec::new();
            };
            constraints.push(postings);
        }
        let partition_fields = self.field_postings.get(&query.topic_partition);
        for &(key, value) in &required.fields {
            let Some(postings) = partition_fields
                .and_then(|keys| keys.get(key))
                .and_then(|values| values.get(value))
            else {
                return Vec::new();
            };
            constraints.push(postings);
        }

        let mut candidate_blocks = if constraints.is_empty() {
            self.partition_blocks
                .get(&query.topic_partition)
                .cloned()
                .unwrap_or_default()
        } else {
            constraints.sort_unstable_by_key(|postings| postings.len());
            let mut candidates = constraints[0]
                .iter()
                .map(|posting| posting.block_ordinal)
                .collect::<Vec<_>>();
            for postings in &constraints[1..] {
                intersect_block_ordinals(&mut candidates, postings);
                if candidates.is_empty() {
                    return Vec::new();
                }
            }
            candidates
        };
        for field_blocks in self.required_field_block_constraints(query.topic_partition, &required)
        {
            intersect_u32(&mut candidate_blocks, &field_blocks);
            if candidate_blocks.is_empty() {
                return Vec::new();
            }
        }
        candidate_blocks.retain(|ordinal| {
            self.block(*ordinal)
                .is_some_and(|metadata| block_overlaps(metadata, query))
                && self.message_might_match(*ordinal, &required_message_slots)
                && self.case_sensitive_message_might_match(
                    *ordinal,
                    &required_case_sensitive_message_slots,
                )
        });
        if query.order == QueryOrder::NewestFirst {
            candidate_blocks.reverse();
        }

        let safe_limit = query
            .can_apply_index_limit()
            .then_some(query.limit)
            .flatten()
            .unwrap_or(usize::MAX);
        let mut hits = Vec::new();
        for block_ordinal in candidate_blocks {
            let Some(metadata) = self.block(block_ordinal) else {
                continue;
            };
            let Some(field_record_constraints) = self.required_field_record_constraints(
                query.topic_partition,
                block_ordinal,
                &required,
            ) else {
                continue;
            };
            let mut record_constraints = Vec::<&PostingList>::with_capacity(
                constraints.len() + field_record_constraints.len(),
            );
            for constraint in &constraints {
                let Ok(position) = constraint
                    .binary_search_by_key(&block_ordinal, |posting| posting.block_ordinal)
                else {
                    record_constraints.clear();
                    break;
                };
                record_constraints.push(&constraint[position].record_ordinals);
            }
            for posting in &field_record_constraints {
                record_constraints.push(posting);
            }
            let remaining = safe_limit.saturating_sub(hits.len());
            let newest_first = query.order == QueryOrder::NewestFirst;
            let record_ordinals = if record_constraints.is_empty() {
                if constraints.is_empty() && field_record_constraints.is_empty() {
                    if newest_first {
                        (0..metadata.record_count).rev().take(remaining).collect()
                    } else {
                        (0..metadata.record_count).take(remaining).collect()
                    }
                } else {
                    continue;
                }
            } else if record_constraints.len() == 1 {
                record_constraints[0].take_ordered(newest_first, remaining)
            } else {
                record_constraints.sort_unstable_by_key(|postings| postings.cardinality());
                if remaining.saturating_mul(8) < record_constraints[0].cardinality() {
                    intersect_postings_limited(&record_constraints, newest_first, remaining)
                } else {
                    let mut candidates = record_constraints[0].to_vec();
                    for postings in &record_constraints[1..] {
                        intersect_posting(&mut candidates, postings);
                        if candidates.is_empty() {
                            break;
                        }
                    }
                    if newest_first {
                        candidates.reverse();
                    }
                    candidates.truncate(remaining);
                    candidates
                }
            };
            hits.extend(
                record_ordinals
                    .into_iter()
                    .take(remaining)
                    .map(|record_ordinal| QueryHit {
                        block_ordinal,
                        record_ordinal,
                    }),
            );
            if hits.len() >= safe_limit {
                break;
            }
        }
        hits
    }

    /// Returns candidate block ordinals in deterministic offset order.
    ///
    /// This is the bounded entry point for sealed queries that require
    /// post-decode filtering. Call [`Self::candidate_hits_in_block`] for one
    /// returned block at a time, decode and filter those records, and stop once
    /// an offset-ordered page is complete.
    #[must_use]
    pub fn candidate_blocks(&self, query: &LogQuery) -> Vec<u32> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Vec::new();
        }
        let required = query.required_index_constraints();
        if required.impossible {
            return Vec::new();
        }
        let Some(required_message_slots) =
            self.prepare_required_message_trigrams(&required.message_literals)
        else {
            return Vec::new();
        };
        let Some(required_case_sensitive_message_slots) =
            self.prepare_case_sensitive_message_trigrams(&required.case_sensitive_message_literals)
        else {
            return Vec::new();
        };
        let mut constraints = Vec::<&[BlockPosting]>::with_capacity(
            required.terms.len().saturating_add(required.fields.len()),
        );
        let partition_terms = self.term_postings.get(&query.topic_partition);
        for &term in &required.terms {
            let normalized = normalize_term(term);
            let Some(postings) = partition_terms.and_then(|terms| terms.get(normalized.as_ref()))
            else {
                return Vec::new();
            };
            constraints.push(postings);
        }
        let partition_fields = self.field_postings.get(&query.topic_partition);
        for &(key, value) in &required.fields {
            let Some(postings) = partition_fields
                .and_then(|keys| keys.get(key))
                .and_then(|values| values.get(value))
            else {
                return Vec::new();
            };
            constraints.push(postings);
        }

        let mut blocks = if constraints.is_empty() {
            self.partition_blocks
                .get(&query.topic_partition)
                .cloned()
                .unwrap_or_default()
        } else {
            constraints.sort_unstable_by_key(|postings| postings.len());
            let mut blocks = constraints[0]
                .iter()
                .map(|posting| posting.block_ordinal)
                .collect::<Vec<_>>();
            for postings in &constraints[1..] {
                intersect_block_ordinals(&mut blocks, postings);
                if blocks.is_empty() {
                    return blocks;
                }
            }
            blocks
        };
        for field_blocks in self.required_field_block_constraints(query.topic_partition, &required)
        {
            intersect_u32(&mut blocks, &field_blocks);
            if blocks.is_empty() {
                return blocks;
            }
        }
        blocks.retain(|ordinal| {
            self.block(*ordinal)
                .is_some_and(|metadata| block_overlaps(metadata, query))
                && self.message_might_match(*ordinal, &required_message_slots)
                && self.case_sensitive_message_might_match(
                    *ordinal,
                    &required_case_sensitive_message_slots,
                )
        });
        blocks.sort_unstable_by_key(|ordinal| {
            self.block(*ordinal).map(|metadata| metadata.first_offset)
        });
        if query.order == QueryOrder::NewestFirst {
            blocks.reverse();
        }
        blocks
    }

    /// Plans the unbounded candidate records inside one selected block.
    ///
    /// This method deliberately ignores the query's global limit. The caller
    /// must decode these candidates, apply [`LogQuery::matches`], and enforce
    /// the limit only after residual filtering.
    #[must_use]
    pub fn candidate_hits_in_block(&self, query: &LogQuery, block_ordinal: u32) -> Vec<QueryHit> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Vec::new();
        }
        let Some(metadata) = self.block(block_ordinal) else {
            return Vec::new();
        };
        if metadata.topic_partition != query.topic_partition || !block_overlaps(metadata, query) {
            return Vec::new();
        }
        let required = query.required_index_constraints();
        if required.impossible {
            return Vec::new();
        }
        let Some(required_message_slots) =
            self.prepare_required_message_trigrams(&required.message_literals)
        else {
            return Vec::new();
        };
        if !self.message_might_match(block_ordinal, &required_message_slots) {
            return Vec::new();
        }
        let Some(required_case_sensitive_message_slots) =
            self.prepare_case_sensitive_message_trigrams(&required.case_sensitive_message_literals)
        else {
            return Vec::new();
        };
        if !self.case_sensitive_message_might_match(
            block_ordinal,
            &required_case_sensitive_message_slots,
        ) {
            return Vec::new();
        }
        let Some(field_record_constraints) =
            self.required_field_record_constraints(query.topic_partition, block_ordinal, &required)
        else {
            return Vec::new();
        };
        let mut constraints = Vec::<&PostingList>::with_capacity(
            required
                .terms
                .len()
                .saturating_add(required.fields.len())
                .saturating_add(field_record_constraints.len()),
        );
        let partition_terms = self.term_postings.get(&query.topic_partition);
        for &term in &required.terms {
            let normalized = normalize_term(term);
            let Some(posting) = partition_terms
                .and_then(|terms| terms.get(normalized.as_ref()))
                .and_then(|postings| posting_for_block(postings, block_ordinal))
            else {
                return Vec::new();
            };
            constraints.push(posting);
        }
        for posting in &field_record_constraints {
            constraints.push(posting);
        }
        let partition_fields = self.field_postings.get(&query.topic_partition);
        for &(key, value) in &required.fields {
            let Some(posting) = partition_fields
                .and_then(|keys| keys.get(key))
                .and_then(|values| values.get(value))
                .and_then(|postings| posting_for_block(postings, block_ordinal))
            else {
                return Vec::new();
            };
            constraints.push(posting);
        }

        let newest_first = query.order == QueryOrder::NewestFirst;
        let mut record_ordinals = if constraints.is_empty() {
            (0..metadata.record_count).collect::<Vec<_>>()
        } else {
            constraints.sort_unstable_by_key(|posting| posting.cardinality());
            let mut record_ordinals = constraints[0].to_vec();
            for posting in &constraints[1..] {
                intersect_posting(&mut record_ordinals, posting);
                if record_ordinals.is_empty() {
                    break;
                }
            }
            record_ordinals
        };
        if newest_first {
            record_ordinals.reverse();
        }
        record_ordinals
            .into_iter()
            .map(|record_ordinal| QueryHit {
                block_ordinal,
                record_ordinal,
            })
            .collect()
    }

    fn required_field_block_constraints(
        &self,
        topic_partition: TopicPartition,
        required: &RequiredIndexConstraints<'_>,
    ) -> Vec<Vec<u32>> {
        let mut constraints = Vec::with_capacity(
            required
                .field_exists
                .len()
                .saturating_add(required.field_in.len())
                .saturating_add(required.field_text.len())
                .saturating_add(required.field_regex.len())
                .saturating_add(required.field_numeric.len()),
        );
        for key in &required.field_exists {
            constraints.push(self.field_block_candidates(topic_partition, key, |_| true));
        }
        for (key, values) in &required.field_in {
            constraints.push(
                self.field_block_candidates(topic_partition, key, |value| values.contains(&value)),
            );
        }
        for (key, matcher) in &required.field_text {
            constraints.push(self.field_block_candidates(topic_partition, key, |value| {
                text_matches(value, matcher)
            }));
        }
        for (key, regex) in &required.field_regex {
            constraints.push(
                self.field_block_candidates(topic_partition, key, |value| regex.is_match(value)),
            );
        }
        for (key, comparison, target) in &required.field_numeric {
            constraints.push(self.field_block_candidates(topic_partition, key, |value| {
                value
                    .parse::<i128>()
                    .is_ok_and(|observed| compare_numeric(observed, *comparison, *target))
            }));
        }
        constraints
    }

    fn required_field_record_constraints(
        &self,
        topic_partition: TopicPartition,
        block_ordinal: u32,
        required: &RequiredIndexConstraints<'_>,
    ) -> Option<Vec<PostingList>> {
        let mut constraints = Vec::with_capacity(
            required
                .field_exists
                .len()
                .saturating_add(required.field_in.len())
                .saturating_add(required.field_text.len())
                .saturating_add(required.field_regex.len())
                .saturating_add(required.field_numeric.len()),
        );
        for key in &required.field_exists {
            constraints.push(
                PostingList::from_ordinals(self.field_record_candidates(
                    topic_partition,
                    key,
                    block_ordinal,
                    |_| true,
                )?)
                .ok()?,
            );
        }
        for (key, values) in &required.field_in {
            constraints.push(
                PostingList::from_ordinals(self.field_record_candidates(
                    topic_partition,
                    key,
                    block_ordinal,
                    |value| values.contains(&value),
                )?)
                .ok()?,
            );
        }
        for (key, matcher) in &required.field_text {
            constraints.push(
                PostingList::from_ordinals(self.field_record_candidates(
                    topic_partition,
                    key,
                    block_ordinal,
                    |value| text_matches(value, matcher),
                )?)
                .ok()?,
            );
        }
        for (key, regex) in &required.field_regex {
            constraints.push(
                PostingList::from_ordinals(self.field_record_candidates(
                    topic_partition,
                    key,
                    block_ordinal,
                    |value| regex.is_match(value),
                )?)
                .ok()?,
            );
        }
        for (key, comparison, target) in &required.field_numeric {
            constraints.push(
                PostingList::from_ordinals(self.field_record_candidates(
                    topic_partition,
                    key,
                    block_ordinal,
                    |value| {
                        value
                            .parse::<i128>()
                            .is_ok_and(|observed| compare_numeric(observed, *comparison, *target))
                    },
                )?)
                .ok()?,
            );
        }
        Some(constraints)
    }

    fn field_block_candidates(
        &self,
        topic_partition: TopicPartition,
        key: &str,
        mut matches_value: impl FnMut(&str) -> bool,
    ) -> Vec<u32> {
        let Some(values) = self
            .field_postings
            .get(&topic_partition)
            .and_then(|fields| fields.get(key))
        else {
            return Vec::new();
        };
        let mut blocks = Vec::new();
        for (value, postings) in values {
            if matches_value(value) {
                blocks.extend(postings.iter().map(|posting| posting.block_ordinal));
            }
        }
        blocks.sort_unstable();
        blocks.dedup();
        blocks
    }

    fn field_record_candidates(
        &self,
        topic_partition: TopicPartition,
        key: &str,
        block_ordinal: u32,
        mut matches_value: impl FnMut(&str) -> bool,
    ) -> Option<Vec<u32>> {
        let values = self
            .field_postings
            .get(&topic_partition)
            .and_then(|fields| fields.get(key))?;
        let mut ordinals = Vec::new();
        for (value, postings) in values {
            if matches_value(value)
                && let Some(posting) = posting_for_block(postings, block_ordinal)
            {
                ordinals.extend(posting.to_vec());
            }
        }
        ordinals.sort_unstable();
        ordinals.dedup();
        (!ordinals.is_empty()).then_some(ordinals)
    }

    /// Encodes the complete immutable directory with hybrid delta/run postings.
    pub fn encode(&self) -> TelemetryResult<Vec<u8>> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(QUERY_INDEX_MAGIC);
        write_varint(
            u64::try_from(self.blocks.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            &mut encoded,
        );
        for (block_index, metadata) in self.blocks.iter().enumerate() {
            write_varint(u64::from(metadata.block_ordinal), &mut encoded);
            encoded.extend_from_slice(&metadata.topic_partition.topic_id.get().to_le_bytes());
            encoded.extend_from_slice(&metadata.topic_partition.partition_id.get().to_le_bytes());
            write_varint(metadata.first_offset.get(), &mut encoded);
            write_varint(metadata.last_offset.get(), &mut encoded);
            write_varint(metadata.min_timestamp_unix_nanos, &mut encoded);
            write_varint(metadata.max_timestamp_unix_nanos, &mut encoded);
            write_varint(u64::from(metadata.record_count), &mut encoded);
            let trigram_start = block_index.saturating_mul(MESSAGE_TRIGRAM_FILTER_WORDS);
            for word in &self.message_trigram_words
                [trigram_start..trigram_start + MESSAGE_TRIGRAM_FILTER_WORDS]
            {
                encoded.extend_from_slice(&word.to_le_bytes());
            }
            let case_sensitive_trigram_start =
                block_index.saturating_mul(CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS);
            for word in &self.case_sensitive_message_trigram_words[case_sensitive_trigram_start
                ..case_sensitive_trigram_start + CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS]
            {
                encoded.extend_from_slice(&word.to_le_bytes());
            }

            let mut terms = self
                .term_postings
                .get(&metadata.topic_partition)
                .into_iter()
                .flat_map(HashMap::iter)
                .filter_map(|(term, postings)| {
                    posting_for_block(postings, metadata.block_ordinal)
                        .map(|posting| (term.as_ref(), posting))
                })
                .collect::<Vec<_>>();
            terms.sort_unstable_by(|left, right| left.0.cmp(right.0));
            write_varint(
                u64::try_from(terms.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
                &mut encoded,
            );
            for (term, posting) in terms {
                append_bytes(term.as_bytes(), &mut encoded)?;
                encode_posting(posting, &mut encoded)?;
            }

            let mut fields = self
                .field_postings
                .get(&metadata.topic_partition)
                .into_iter()
                .flat_map(HashMap::iter)
                .flat_map(|(key, values)| {
                    values.iter().filter_map(move |(value, postings)| {
                        posting_for_block(postings, metadata.block_ordinal)
                            .map(|posting| (key.as_ref(), value.as_ref(), posting))
                    })
                })
                .collect::<Vec<_>>();
            fields.sort_unstable_by(|left, right| {
                left.0.cmp(right.0).then_with(|| left.1.cmp(right.1))
            });
            write_varint(
                u64::try_from(fields.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
                &mut encoded,
            );
            for (key, value, posting) in fields {
                append_bytes(key.as_bytes(), &mut encoded)?;
                append_bytes(value.as_bytes(), &mut encoded)?;
                encode_posting(posting, &mut encoded)?;
            }
        }
        Ok(encoded)
    }

    /// Decodes and validates one immutable query directory.
    pub fn decode(encoded: &[u8]) -> TelemetryResult<Self> {
        Self::decode_internal(encoded, None)
    }

    fn decode_internal(encoded: &[u8], backing: Option<Arc<[u8]>>) -> TelemetryResult<Self> {
        let (magic_len, has_case_sensitive_filter) =
            if encoded.get(..QUERY_INDEX_MAGIC.len()) == Some(QUERY_INDEX_MAGIC) {
                (QUERY_INDEX_MAGIC.len(), true)
            } else if encoded.get(..QUERY_INDEX_MAGIC_V1.len()) == Some(QUERY_INDEX_MAGIC_V1) {
                (QUERY_INDEX_MAGIC_V1.len(), false)
            } else {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "missing query index magic",
                ));
            };
        let mut cursor = magic_len;
        let block_count = read_usize(encoded, &mut cursor)?;
        ensure_count(block_count, encoded.len().saturating_sub(cursor))?;
        let mut blocks = Vec::with_capacity(block_count);
        let mut message_trigram_words =
            Vec::with_capacity(block_count.saturating_mul(MESSAGE_TRIGRAM_FILTER_WORDS));
        let mut case_sensitive_message_trigram_words = Vec::with_capacity(
            block_count.saturating_mul(CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS),
        );
        let mut partition_blocks = HashMap::<TopicPartition, Vec<u32>>::new();
        let mut term_postings =
            HashMap::<TopicPartition, HashMap<Arc<str>, Vec<BlockPosting>>>::new();
        let mut field_postings = HashMap::<
            TopicPartition,
            HashMap<Arc<str>, HashMap<Arc<str>, Vec<BlockPosting>>>,
        >::new();
        let mut previous_block_ordinal = None;
        for _ in 0..block_count {
            let block_ordinal = read_u32(encoded, &mut cursor)?;
            if previous_block_ordinal.is_some_and(|previous| previous >= block_ordinal) {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "query blocks are not ordered",
                ));
            }
            previous_block_ordinal = Some(block_ordinal);
            let topic_end = cursor
                .checked_add(16)
                .ok_or(TelemetryError::InvalidBlockEncoding("query topic overflow"))?;
            let topic_id = TopicId::new(u128::from_le_bytes(
                encoded
                    .get(cursor..topic_end)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "truncated query topic",
                    ))?
                    .try_into()
                    .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid query topic"))?,
            ));
            cursor = topic_end;
            let partition_end =
                cursor
                    .checked_add(4)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "query partition overflow",
                    ))?;
            let partition_id = LogicalPartitionId::new(u32::from_le_bytes(
                encoded
                    .get(cursor..partition_end)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "truncated query partition",
                    ))?
                    .try_into()
                    .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid query partition"))?,
            ));
            cursor = partition_end;
            let first_offset = LogicalOffset::new(read_varint(encoded, &mut cursor)?);
            let last_offset = LogicalOffset::new(read_varint(encoded, &mut cursor)?);
            let min_timestamp_unix_nanos = read_varint(encoded, &mut cursor)?;
            let max_timestamp_unix_nanos = read_varint(encoded, &mut cursor)?;
            let record_count = read_u32(encoded, &mut cursor)?;
            let trigram_end = cursor.checked_add(MESSAGE_TRIGRAM_FILTER_BYTES).ok_or(
                TelemetryError::InvalidBlockEncoding("message trigram filter overflow"),
            )?;
            let message_trigrams =
                MessageTrigramFilter::from_bytes(encoded.get(cursor..trigram_end).ok_or(
                    TelemetryError::InvalidBlockEncoding("truncated message trigram filter"),
                )?)?;
            cursor = trigram_end;
            if first_offset > last_offset || min_timestamp_unix_nanos > max_timestamp_unix_nanos {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "invalid query block metadata",
                ));
            }
            let topic_partition = TopicPartition::new(topic_id, partition_id);
            partition_blocks
                .entry(topic_partition)
                .or_default()
                .push(block_ordinal);
            message_trigram_words.extend_from_slice(&message_trigrams.words);
            if has_case_sensitive_filter {
                let case_sensitive_trigram_end = cursor
                    .checked_add(CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_BYTES)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "case-sensitive message trigram filter overflow",
                    ))?;
                let case_sensitive_message_trigrams =
                    MessageTrigramFilter::from_bytes_case_sensitive(
                        encoded.get(cursor..case_sensitive_trigram_end).ok_or(
                            TelemetryError::InvalidBlockEncoding(
                                "truncated case-sensitive message trigram filter",
                            ),
                        )?,
                    )?;
                cursor = case_sensitive_trigram_end;
                case_sensitive_message_trigram_words
                    .extend_from_slice(&case_sensitive_message_trigrams.words);
            } else {
                case_sensitive_message_trigram_words.extend(std::iter::repeat_n(
                    u64::MAX,
                    CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS,
                ));
            }

            let term_count = read_usize(encoded, &mut cursor)?;
            ensure_count(term_count, encoded.len().saturating_sub(cursor))?;
            let mut block_terms = HashMap::with_capacity(term_count);
            for _ in 0..term_count {
                let term = decode_text(read_bytes(encoded, &mut cursor)?)?;
                if block_terms.insert(term.clone(), ()).is_some() {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "duplicate indexed term",
                    ));
                }
                let posting = decode_posting_for_directory(
                    encoded,
                    &mut cursor,
                    record_count,
                    backing.as_ref(),
                )?;
                term_postings
                    .entry(topic_partition)
                    .or_default()
                    .entry(term)
                    .or_default()
                    .push(BlockPosting {
                        block_ordinal,
                        record_ordinals: posting,
                    });
            }
            let field_count = read_usize(encoded, &mut cursor)?;
            ensure_count(field_count, encoded.len().saturating_sub(cursor))?;
            let mut block_fields =
                HashMap::<Arc<str>, HashMap<Arc<str>, ()>>::with_capacity(field_count);
            for _ in 0..field_count {
                let key = decode_text(read_bytes(encoded, &mut cursor)?)?;
                let value = decode_text(read_bytes(encoded, &mut cursor)?)?;
                if block_fields
                    .entry(key.clone())
                    .or_default()
                    .insert(value.clone(), ())
                    .is_some()
                {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "duplicate indexed field",
                    ));
                }
                let posting = decode_posting_for_directory(
                    encoded,
                    &mut cursor,
                    record_count,
                    backing.as_ref(),
                )?;
                field_postings
                    .entry(topic_partition)
                    .or_default()
                    .entry(key)
                    .or_default()
                    .entry(value)
                    .or_default()
                    .push(BlockPosting {
                        block_ordinal,
                        record_ordinals: posting,
                    });
            }
            blocks.push(QueryBlockMetadata {
                block_ordinal,
                topic_partition,
                first_offset,
                last_offset,
                min_timestamp_unix_nanos,
                max_timestamp_unix_nanos,
                record_count,
            });
        }
        if cursor != encoded.len() {
            return Err(TelemetryError::InvalidBlockEncoding(
                "trailing query index bytes",
            ));
        }
        let (message_trigram_union, message_trigram_intersection) =
            aggregate_message_trigrams(&message_trigram_words);
        let (case_sensitive_message_trigram_union, case_sensitive_message_trigram_intersection) =
            aggregate_case_sensitive_message_trigrams(&case_sensitive_message_trigram_words);
        Ok(Self {
            blocks,
            message_trigram_words: message_trigram_words.into_boxed_slice(),
            case_sensitive_message_trigram_words: case_sensitive_message_trigram_words
                .into_boxed_slice(),
            message_trigram_union,
            message_trigram_intersection,
            case_sensitive_message_trigram_union,
            case_sensitive_message_trigram_intersection,
            partition_blocks,
            term_postings,
            field_postings,
        })
    }

    /// Encodes and wraps the query directory in one zstd frame.
    pub fn encode_compressed(&self, level: i32) -> TelemetryResult<Vec<u8>> {
        let uncompressed = self.encode()?;
        let compressed = zstd::bulk::compress(&uncompressed, level)
            .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
        let mut encoded =
            Vec::with_capacity(COMPRESSED_QUERY_INDEX_MAGIC.len() + 8 + compressed.len());
        encoded.extend_from_slice(COMPRESSED_QUERY_INDEX_MAGIC);
        encoded.extend_from_slice(
            &u64::try_from(uncompressed.len())
                .map_err(|_| TelemetryError::RecordTooLarge)?
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&compressed);
        Ok(encoded)
    }

    /// Decodes a zstd-wrapped immutable query directory.
    pub fn decode_compressed(encoded: &[u8]) -> TelemetryResult<Self> {
        let magic_len = if encoded.get(..COMPRESSED_QUERY_INDEX_MAGIC.len())
            == Some(COMPRESSED_QUERY_INDEX_MAGIC)
        {
            COMPRESSED_QUERY_INDEX_MAGIC.len()
        } else if encoded.get(..COMPRESSED_QUERY_INDEX_MAGIC_V1.len())
            == Some(COMPRESSED_QUERY_INDEX_MAGIC_V1)
        {
            COMPRESSED_QUERY_INDEX_MAGIC_V1.len()
        } else {
            return Err(TelemetryError::InvalidBlockEncoding(
                "missing compressed query index magic",
            ));
        };
        let length_end = magic_len + 8;
        let uncompressed_len = usize::try_from(u64::from_le_bytes(
            encoded
                .get(magic_len..length_end)
                .ok_or(TelemetryError::InvalidBlockEncoding(
                    "truncated query index length",
                ))?
                .try_into()
                .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid query index length"))?,
        ))
        .map_err(|_| {
            TelemetryError::InvalidBlockEncoding("query index length does not fit usize")
        })?;
        let uncompressed = zstd::bulk::decompress(
            encoded
                .get(length_end..)
                .ok_or(TelemetryError::InvalidBlockEncoding(
                    "truncated compressed query index",
                ))?,
            uncompressed_len,
        )
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid compressed query index"))?;
        let backing: Arc<[u8]> = Arc::from(uncompressed);
        Self::decode_internal(backing.as_ref(), Some(Arc::clone(&backing)))
    }

    fn block(&self, block_ordinal: u32) -> Option<&QueryBlockMetadata> {
        self.blocks
            .binary_search_by_key(&block_ordinal, |metadata| metadata.block_ordinal)
            .ok()
            .and_then(|index| self.blocks.get(index))
    }

    fn message_might_match(&self, block_ordinal: u32, required_slots: &[usize]) -> bool {
        let Ok(index) = self
            .blocks
            .binary_search_by_key(&block_ordinal, |metadata| metadata.block_ordinal)
        else {
            return false;
        };
        let start = index.saturating_mul(MESSAGE_TRIGRAM_FILTER_WORDS);
        required_slots.iter().all(|slot| {
            self.message_trigram_words[start + *slot / u64::BITS as usize]
                & (1u64 << (*slot % u64::BITS as usize))
                != 0
        })
    }

    fn case_sensitive_message_might_match(
        &self,
        block_ordinal: u32,
        required_slots: &[usize],
    ) -> bool {
        let Ok(index) = self
            .blocks
            .binary_search_by_key(&block_ordinal, |metadata| metadata.block_ordinal)
        else {
            return false;
        };
        let start = index.saturating_mul(CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS);
        required_slots.iter().all(|slot| {
            self.case_sensitive_message_trigram_words
                .get(start + *slot / u64::BITS as usize)
                .is_some_and(|word| word & (1u64 << (*slot % u64::BITS as usize)) != 0)
        })
    }

    fn prepare_required_message_trigrams(&self, literals: &[&str]) -> Option<Vec<usize>> {
        let mut slots = required_message_trigram_slots(literals);
        if !self.message_trigram_union.might_contain_all(&slots) {
            return None;
        }
        slots.retain(|slot| {
            !self
                .message_trigram_intersection
                .might_contain_all(&[*slot])
        });
        Some(slots)
    }

    fn prepare_case_sensitive_message_trigrams(&self, literals: &[&str]) -> Option<Vec<usize>> {
        let mut slots = required_case_sensitive_message_trigram_slots(literals);
        if !self
            .case_sensitive_message_trigram_union
            .might_contain_all(&slots)
        {
            return None;
        }
        slots.retain(|slot| {
            !self
                .case_sensitive_message_trigram_intersection
                .might_contain_all(&[*slot])
        });
        Some(slots)
    }
}

fn aggregate_message_trigrams(words: &[u64]) -> (MessageTrigramFilter, MessageTrigramFilter) {
    let mut union = MessageTrigramFilter::new();
    let mut intersection = MessageTrigramFilter {
        words: vec![u64::MAX; MESSAGE_TRIGRAM_FILTER_WORDS].into_boxed_slice(),
    };
    if words.is_empty() {
        intersection.words.fill(0);
        return (union, intersection);
    }
    for filter in words.chunks_exact(MESSAGE_TRIGRAM_FILTER_WORDS) {
        for ((union, intersection), observed) in union
            .words
            .iter_mut()
            .zip(intersection.words.iter_mut())
            .zip(filter.iter())
        {
            *union |= *observed;
            *intersection &= *observed;
        }
    }
    (union, intersection)
}

fn aggregate_case_sensitive_message_trigrams(
    words: &[u64],
) -> (MessageTrigramFilter, MessageTrigramFilter) {
    let mut union = MessageTrigramFilter::new_case_sensitive();
    let mut intersection = MessageTrigramFilter {
        words: vec![u64::MAX; CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS].into_boxed_slice(),
    };
    if words.is_empty() {
        intersection.words.fill(0);
        return (union, intersection);
    }
    for filter in words.chunks_exact(CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS) {
        for ((union, intersection), observed) in union
            .words
            .iter_mut()
            .zip(intersection.words.iter_mut())
            .zip(filter.iter())
        {
            *union |= *observed;
            *intersection &= *observed;
        }
    }
    (union, intersection)
}
