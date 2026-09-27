use super::*;

impl LogStripe {
    /// Performs one exact Boolean lookup over normalized log records.
    ///
    /// This hot-index implementation is intentionally partition-local. A
    /// coordinator may fan out across selected time or tenant partitions, but
    /// that expensive choice is never implicit on a stripe.
    #[must_use]
    pub fn query(&self, query: &LogQuery) -> Vec<LogMatch> {
        self.query_checked(query).unwrap_or_default()
    }

    pub(crate) fn query_checked(&self, query: &LogQuery) -> TelemetryResult<Vec<LogMatch>> {
        self.query_checked_with_typed_metadata(query, true, true)
    }

    pub(super) fn query_checked_with_typed_metadata(
        &self,
        query: &LogQuery,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(Vec::new());
        }
        let mut matches = self.query_hot_matches(query, include_typed_metadata, include_fields);
        if let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) {
            matches.extend(self.query_indexed_frames(
                query,
                partition,
                include_typed_metadata,
                include_fields,
            )?);
        }
        matches.extend(self.query_tiered_groups(query, include_typed_metadata, include_fields)?);
        let has_indexed_frames = self
            .indexed_frame_partitions
            .get(&query.topic_partition)
            .is_some_and(|partition| !partition.appends.is_empty());
        if !has_indexed_frames && self.tier.is_none() {
            if let Some(limit) = query.limit {
                matches.truncate(limit);
            }
            return Ok(matches);
        }
        matches.sort_unstable_by(|left, right| query.compare(&left.record, &right.record));
        if let Some(limit) = query.limit {
            matches.truncate(limit);
        }
        Ok(matches)
    }

    pub(crate) fn query_partitions_checked(
        &self,
        queries: &[LogQuery],
    ) -> TelemetryResult<Vec<LogMatch>> {
        self.query_partitions_checked_projected(queries, true)
    }

    pub(crate) fn query_partitions_checked_projected(
        &self,
        queries: &[LogQuery],
        include_typed_metadata: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        self.query_partitions_checked_projected_with_fields(queries, include_typed_metadata, true)
    }

    pub(crate) fn query_partitions_checked_projected_with_fields(
        &self,
        queries: &[LogQuery],
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let Some(ordering_query) = queries.first() else {
            return Ok(Vec::new());
        };
        if self.tier.is_some() {
            let mut matches = queries.iter().try_fold(Vec::new(), |mut matches, query| {
                matches.extend(self.query_checked_with_typed_metadata(
                    query,
                    include_typed_metadata,
                    include_fields,
                )?);
                Ok::<_, TelemetryError>(matches)
            })?;
            matches.sort_unstable_by(|left, right| {
                ordering_query.compare(&left.record, &right.record)
            });
            if let Some(limit) = ordering_query.limit {
                matches.truncate(limit);
            }
            return Ok(matches);
        }
        let Some(limit) = ordering_query.limit else {
            return queries.iter().try_fold(Vec::new(), |mut matches, query| {
                matches.extend(self.query_checked_with_typed_metadata(
                    query,
                    include_typed_metadata,
                    include_fields,
                )?);
                Ok(matches)
            });
        };
        if ordering_query.sort != crate::QuerySort::Timestamp
            || !queries
                .iter()
                .all(|query| same_query_across_partition(ordering_query, query))
        {
            return queries.iter().try_fold(Vec::new(), |mut matches, query| {
                matches.extend(self.query_checked_with_typed_metadata(
                    query,
                    include_typed_metadata,
                    include_fields,
                )?);
                Ok(matches)
            });
        }

        let mut matches: Vec<LogMatch> = Vec::new();
        let mut frames = Vec::new();
        for query in queries {
            if query.limit == Some(0) || query.has_invalid_range() {
                continue;
            }
            matches.extend(self.query_hot_matches(query, include_typed_metadata, include_fields));
            let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) else {
                continue;
            };
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append) {
                    continue;
                }
                frames.extend(
                    append
                        .frames
                        .iter()
                        .filter(|frame| frame_matches_query_bounds(query, frame))
                        .map(|frame| IndexedFrameQuery {
                            query,
                            append,
                            frame,
                        }),
                );
            }
        }
        match ordering_query.order {
            QueryOrder::NewestFirst => frames.sort_unstable_by(|left, right| {
                right
                    .frame
                    .max_timestamp_unix_nanos
                    .cmp(&left.frame.max_timestamp_unix_nanos)
            }),
            QueryOrder::OldestFirst => frames.sort_unstable_by(|left, right| {
                left.frame
                    .min_timestamp_unix_nanos
                    .cmp(&right.frame.min_timestamp_unix_nanos)
            }),
        }
        sort_and_limit_matches(&mut matches, ordering_query, limit);
        for pending in frames {
            if matches.len() == limit {
                let boundary = matches
                    .last()
                    .expect("a full result page has a boundary")
                    .record
                    .timestamp_unix_nanos;
                let cannot_improve = match ordering_query.order {
                    QueryOrder::NewestFirst => pending.frame.max_timestamp_unix_nanos < boundary,
                    QueryOrder::OldestFirst => pending.frame.min_timestamp_unix_nanos > boundary,
                };
                if cannot_improve {
                    break;
                }
            }
            matches.extend(self.query_indexed_frame(
                pending.query,
                pending.append,
                pending.frame,
                include_typed_metadata,
                include_fields,
            )?);
            sort_and_limit_matches(&mut matches, ordering_query, limit);
        }
        Ok(matches)
    }

    pub(crate) fn query_partition_refs_checked_projected_each_with_fields(
        &self,
        queries: &[&LogQuery],
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<Vec<LogMatch>>> {
        queries
            .iter()
            .map(|query| {
                self.query_checked_with_typed_metadata(
                    query,
                    include_typed_metadata,
                    include_fields,
                )
            })
            .collect()
    }

    pub(crate) fn query_partitions_checked_messages_top_k(
        &self,
        queries: &[LogQuery],
        scorer: &crate::analytics::RelevanceScorer,
        limit: usize,
    ) -> TelemetryResult<Vec<LogMessageMatch>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut top = BinaryHeap::with_capacity(limit);
        for query in queries {
            self.for_each_checked_message_batch(query, &mut |matches| {
                for_each_message_match_score(&matches, scorer, &mut |matched, score| {
                    let item = MessageRelevanceTopKItem {
                        score,
                        timestamp_unix_nanos: matched.timestamp_unix_nanos,
                        offset: matched.record_ref.offset.get(),
                        matched: matched.clone(),
                    };
                    let should_keep =
                        top.len() < limit || top.peek().is_some_and(|Reverse(worst)| item > *worst);
                    if should_keep {
                        if top.len() == limit {
                            top.pop();
                        }
                        top.push(Reverse(item));
                    }
                    Ok(())
                })?;
                Ok(())
            })?;
        }
        Ok(top.into_iter().map(|Reverse(item)| item.matched).collect())
    }

    pub(crate) fn query_partitions_checked_trace_ids(
        &self,
        queries: &[LogQuery],
    ) -> TelemetryResult<Vec<TraceId>> {
        queries.iter().try_fold(Vec::new(), |mut trace_ids, query| {
            trace_ids.extend(self.query_checked_trace_ids(query)?);
            Ok(trace_ids)
        })
    }

    pub(super) fn for_each_checked_message_batch(
        &self,
        query: &LogQuery,
        emit: &mut dyn FnMut(Vec<LogMessageMatch>) -> TelemetryResult<()>,
    ) -> TelemetryResult<()> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(());
        }
        let message_predicate_key = Self::cached_message_predicate_key(&query.predicate);
        let hot_matches = self
            .query_hot_matches(query, false, true)
            .into_iter()
            .map(|matched| LogMessageMatch {
                record_ref: matched.record.record_ref,
                timestamp_unix_nanos: matched.record.timestamp_unix_nanos,
                message: Some(matched.record.message),
                relevance: None,
            })
            .collect::<Vec<_>>();
        emit(hot_matches)?;
        if let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) {
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append) {
                    continue;
                }
                for frame in &append.frames {
                    if frame_matches_query_bounds(query, frame) {
                        let frame_matches = self.query_indexed_frame_messages(
                            query,
                            append,
                            frame,
                            message_predicate_key.as_ref(),
                        )?;
                        emit(frame_matches)?;
                    }
                }
            }
        }
        if self.tier.is_some() {
            emit(self.query_tiered_groups_messages(query, message_predicate_key.as_ref())?)?;
        }
        Ok(())
    }

    pub(super) fn query_checked_trace_ids(
        &self,
        query: &LogQuery,
    ) -> TelemetryResult<Vec<TraceId>> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(Vec::new());
        }
        let message_predicate_key = Self::cached_message_predicate_key(&query.predicate);
        let mut trace_ids = self
            .query_hot_matches(query, true, true)
            .into_iter()
            .filter_map(|matched| matched.record.trace_id)
            .collect::<Vec<_>>();
        if let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) {
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append) {
                    continue;
                }
                for frame in &append.frames {
                    if frame_matches_query_bounds(query, frame) {
                        trace_ids.extend(self.query_indexed_frame_trace_ids(
                            query,
                            append,
                            frame,
                            message_predicate_key.as_ref(),
                        )?);
                    }
                }
            }
        }
        if self.tier.is_some() {
            trace_ids
                .extend(self.query_tiered_groups_trace_ids(query, message_predicate_key.as_ref())?);
        }
        Ok(trace_ids)
    }

    pub(super) fn query_indexed_frame_messages(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<LogMessageMatch>> {
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let message_cache_can_supply_the_base =
            message_cache_can_supply_frame_base(query, append.tenant.as_ref());
        let mut used_message_cache_base = false;
        let mut candidates: Vec<u32>;
        let mut cached_base_candidates: Option<Arc<[u32]>> = None;
        if message_cache_can_supply_the_base && exact_tokens.is_none() {
            if let Some(message_candidates) = message_predicate_key.and_then(|_| {
                self.cached_message_predicate_candidates_arc_if_present(
                    frame.frame_id,
                    message_predicate_key,
                )
            }) {
                used_message_cache_base = true;
                cached_base_candidates = Some(message_candidates);
                candidates = Vec::new();
            } else {
                candidates = indexed_frame_candidates_for_append(
                    query,
                    &frame.index,
                    frame.record_count,
                    append.tenant.as_ref(),
                );
            }
        } else if let Some(tokens) = exact_tokens
            .as_deref()
            .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
        {
            candidates = self
                .exact_indexed_frame_candidates(query, append, frame, tokens, &exact_fields)?
                .unwrap_or_else(|| {
                    indexed_frame_candidates_for_append(
                        query,
                        &frame.index,
                        frame.record_count,
                        append.tenant.as_ref(),
                    )
                });
        } else if message_cache_can_supply_the_base {
            candidates = (0..frame.record_count).collect();
        } else {
            candidates = indexed_frame_candidates_for_append(
                query,
                &frame.index,
                frame.record_count,
                append.tenant.as_ref(),
            );
        }
        if !used_message_cache_base {
            candidates =
                self.indexed_frame_field_predicate_candidates_owned(query, frame, candidates)?;
            if !matches!(query.predicate, LogPredicate::MatchAll) {
                let Some(message_candidates) = self
                    .cached_message_predicate_candidates_for_relevance(
                        query,
                        frame,
                        message_predicate_key,
                    )?
                else {
                    return Ok(Vec::new());
                };
                let mut selected = Some(candidates);
                intersect_frame_candidate_slice(&mut selected, &message_candidates);
                candidates = selected.unwrap_or_default();
            }
        }
        let candidates = cached_base_candidates.as_deref().unwrap_or(&candidates);
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let relevance_stats = self.cached_message_token_stats(&cached, frame.record_count)?;
        let mut matches = Vec::with_capacity(candidates.len());
        let needs_message_filter =
            used_message_cache_base && !cached_message_predicate_is_exact(&query.predicate);
        for ordinal in candidates.iter().copied() {
            if needs_message_filter {
                let message = relevance_stats.messages.get(ordinal as usize).ok_or(
                    TelemetryError::InvalidBlockEncoding(
                        "message candidate ordinal is out of range",
                    ),
                )?;
                if !query.message_candidate_matches(message).unwrap_or(false) {
                    continue;
                }
            }
            let index = usize::try_from(ordinal)
                .map_err(|_| TelemetryError::InvalidBlockEncoding("record ordinal overflow"))?;
            let timestamp_unix_nanos =
                *cached
                    .timestamps
                    .get(index)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "message candidate timestamp missing",
                    ))?;
            if !query.timestamp_matches(timestamp_unix_nanos) {
                continue;
            }
            let relative_offset =
                cached
                    .offsets
                    .get(index)
                    .copied()
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "message candidate offset missing",
                    ))?;
            let absolute_offset = append
                .first_offset
                .get()
                .checked_add(relative_offset.get())
                .map(LogicalOffset::new)
                .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
            matches.push(LogMessageMatch {
                record_ref: TelemetryRecordRef::new(query.topic_partition, absolute_offset),
                timestamp_unix_nanos,
                message: None,
                relevance: Some(IndexedMessageRelevance {
                    stats: Arc::clone(&relevance_stats),
                    ordinal,
                }),
            });
        }
        Ok(matches)
    }

    pub(super) fn query_indexed_frame_trace_ids(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<TraceId>> {
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let candidates = if let Some(tokens) = exact_tokens
            .as_deref()
            .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
        {
            self.exact_indexed_frame_candidates(query, append, frame, tokens, &exact_fields)?
                .unwrap_or_else(|| {
                    indexed_frame_candidates_for_append(
                        query,
                        &frame.index,
                        frame.record_count,
                        append.tenant.as_ref(),
                    )
                })
        } else {
            indexed_frame_candidates_for_append(
                query,
                &frame.index,
                frame.record_count,
                append.tenant.as_ref(),
            )
        };
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        self.decode_indexed_frame_trace_ids(
            query,
            append,
            frame,
            &candidates,
            message_predicate_key,
        )
    }

    pub(super) fn decode_indexed_frame_trace_ids(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        candidates: &[u32],
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<TraceId>> {
        let mut candidates = self
            .indexed_frame_field_predicate_candidates(query, frame, candidates)?
            .unwrap_or_else(|| candidates.to_vec());
        if query.exact_message_token_conjunction().is_none()
            && !matches!(query.predicate, LogPredicate::MatchAll)
        {
            let Some(message_candidates) = self.cached_message_predicate_candidates_with_key(
                query,
                frame,
                message_predicate_key,
            )?
            else {
                let matches = self
                    .query_indexed_frame(query, append, frame, true, true)?
                    .into_iter()
                    .filter_map(|matched| matched.record.trace_id)
                    .collect::<Vec<_>>();
                return Ok(matches);
            };
            let mut selected = Some(candidates);
            intersect_frame_candidate_slice(&mut selected, &message_candidates);
            candidates = selected.unwrap_or_default();
        }
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let mut verified = Vec::with_capacity(candidates.len());
        for ordinal in candidates {
            let index = usize::try_from(ordinal)
                .map_err(|_| TelemetryError::InvalidBlockEncoding("trace ID ordinal overflow"))?;
            let timestamp =
                *cached
                    .timestamps
                    .get(index)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "trace ID candidate timestamp missing",
                    ))?;
            if !query.timestamp_matches(timestamp) {
                continue;
            }
            let offset = append
                .first_offset
                .get()
                .checked_add(
                    cached
                        .offsets
                        .get(index)
                        .ok_or(TelemetryError::InvalidBlockEncoding(
                            "trace ID candidate offset missing",
                        ))?
                        .get(),
                )
                .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
            let offset = LogicalOffset::new(offset);
            if query.start_offset.is_some_and(|start| offset < start)
                || query.end_offset.is_some_and(|end| offset >= end)
                || query.after.is_some_and(|cursor| match query.order {
                    QueryOrder::OldestFirst => offset <= cursor.offset,
                    QueryOrder::NewestFirst => offset >= cursor.offset,
                })
            {
                continue;
            }
            verified.push(ordinal);
        }
        if verified.is_empty() {
            return Ok(Vec::new());
        }
        if !trace_predicate_candidates_are_exact(&query.predicate) || !query.terms.is_empty() {
            let messages = decode_structural_messages_with_embedded_index_and_templates(
                &cached.structural,
                &verified,
                &cached.embedded_index,
                &cached.templates,
            )?;
            verified = verified
                .into_iter()
                .zip(messages)
                .filter_map(|(ordinal, message)| {
                    query
                        .message_candidate_matches(&message)
                        .unwrap_or(true)
                        .then_some(ordinal)
                })
                .collect();
            if verified.is_empty() {
                return Ok(Vec::new());
            }
        }
        let trace_ids = self.cached_trace_ids(&cached, frame.record_count)?;
        Ok(verified
            .into_iter()
            .filter_map(|ordinal| {
                usize::try_from(ordinal)
                    .ok()
                    .and_then(|index| trace_ids.get(index).copied().flatten())
            })
            .collect())
    }

    pub(super) fn decode_indexed_frame_messages(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        candidates: &[u32],
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<LogMessageMatch>> {
        let message_cache_can_supply_the_base =
            message_cache_can_supply_frame_base(query, append.tenant.as_ref());
        let cached_base_candidates = (message_cache_can_supply_the_base
            && query.exact_message_token_conjunction().is_none())
        .then(|| {
            message_predicate_key.and_then(|_| {
                self.cached_message_predicate_candidates_arc_if_present(
                    frame.frame_id,
                    message_predicate_key,
                )
            })
        })
        .flatten();
        let used_message_cache_base = cached_base_candidates.is_some();
        let mut owned_candidates = cached_base_candidates
            .is_none()
            .then(|| candidates.to_vec());
        if !used_message_cache_base {
            let candidates = owned_candidates
                .as_mut()
                .expect("non-cached message candidates are owned");
            if let Some(filtered) =
                self.indexed_frame_field_predicate_candidates(query, frame, candidates.as_slice())?
            {
                *candidates = filtered;
            }
            if !matches!(query.predicate, LogPredicate::MatchAll) {
                let Some(message_candidates) = self.cached_message_predicate_candidates_with_key(
                    query,
                    frame,
                    message_predicate_key,
                )?
                else {
                    return Ok(Vec::new());
                };
                let mut selected = Some(std::mem::take(candidates));
                intersect_frame_candidate_slice(&mut selected, &message_candidates);
                *candidates = selected.unwrap_or_default();
            }
        }
        let candidates = cached_base_candidates
            .as_deref()
            .or(owned_candidates.as_deref())
            .unwrap_or(&[]);
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let relevance_stats = self.cached_message_token_stats(&cached, frame.record_count)?;
        let mut matches = Vec::with_capacity(candidates.len());
        let needs_message_filter =
            used_message_cache_base && !cached_message_predicate_is_exact(&query.predicate);
        for ordinal in candidates.iter().copied() {
            if needs_message_filter {
                let message = relevance_stats.messages.get(ordinal as usize).ok_or(
                    TelemetryError::InvalidBlockEncoding(
                        "message candidate ordinal is out of range",
                    ),
                )?;
                if !query.message_candidate_matches(message).unwrap_or(false) {
                    continue;
                }
            }
            let index = usize::try_from(ordinal)
                .map_err(|_| TelemetryError::InvalidBlockEncoding("message ordinal overflow"))?;
            let timestamp_unix_nanos =
                *cached
                    .timestamps
                    .get(index)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "message candidate timestamp missing",
                    ))?;
            if !query.timestamp_matches(timestamp_unix_nanos) {
                continue;
            }
            let relative_offset =
                cached
                    .offsets
                    .get(index)
                    .copied()
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "message candidate offset missing",
                    ))?;
            let absolute_offset = append
                .first_offset
                .get()
                .checked_add(relative_offset.get())
                .map(LogicalOffset::new)
                .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
            matches.push(LogMessageMatch {
                record_ref: TelemetryRecordRef::new(query.topic_partition, absolute_offset),
                timestamp_unix_nanos,
                message: None,
                relevance: Some(IndexedMessageRelevance {
                    stats: Arc::clone(&relevance_stats),
                    ordinal,
                }),
            });
        }
        Ok(matches)
    }
}
