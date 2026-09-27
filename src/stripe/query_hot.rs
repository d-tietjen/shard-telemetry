use super::*;

impl LogStripe {
    pub(super) fn query_hot_matches(
        &self,
        query: &LogQuery,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> Vec<LogMatch> {
        if let Some(matches) =
            self.query_hot_single_posting_matches(query, include_typed_metadata, include_fields)
        {
            return matches;
        }
        self.partitions
            .get(&query.topic_partition)
            .map(|partition| {
                let ordinals = self.query_ordinals(query, partition);
                let mut matches = Vec::with_capacity(ordinals.len());
                for ordinal in ordinals {
                    if let Some(record) = partition.records.get(ordinal as usize) {
                        let record = if include_typed_metadata {
                            record.record.clone()
                        } else {
                            let mut projected = DurableLog::new_projected(
                                record.record.stream_shard_id,
                                record.record.record_ref.topic_partition,
                                record.record.record_ref.offset,
                                record.record.timestamp_unix_nanos,
                                Arc::clone(&record.record.message),
                                record.record.compression_cohort,
                            );
                            projected.severity_text = Arc::clone(&record.record.severity_text);
                            if include_fields {
                                projected.fields = Arc::clone(&record.record.fields);
                            }
                            projected
                        };
                        matches.push(LogMatch { record });
                    }
                }
                matches
            })
            .unwrap_or_default()
    }

    pub(super) fn query_hot_single_posting_matches(
        &self,
        query: &LogQuery,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> Option<Vec<LogMatch>> {
        if query.sort != crate::QuerySort::Offset
            || !query.terms.is_empty()
            || !query.exact_fields.is_empty()
            || query.start_offset.is_some()
            || query.end_offset.is_some()
            || query.start_timestamp_unix_nanos.is_some()
            || query.end_timestamp_unix_nanos.is_some()
            || query.after.is_some()
            || !hot_single_posting_predicate(&query.predicate)
        {
            return None;
        }
        let partition = self.partitions.get(&query.topic_partition)?;
        let posting = hot_predicate_driver_posting(
            &query.predicate,
            partition,
            0,
            u32::try_from(partition.records.len()).expect("record count was bounded"),
        )?;
        let take = query.limit.unwrap_or(usize::MAX);
        let record_end = u32::try_from(partition.records.len()).expect("record count was bounded");
        let mut matches = Vec::with_capacity(take.min(posting.cardinality_in(0, record_end)));
        posting.visit_in(0, record_end, query.order, |ordinal| {
            if let Some(record) = partition.records.get(ordinal as usize) {
                let record = if include_typed_metadata {
                    record.record.clone()
                } else {
                    let mut projected = DurableLog::new_projected(
                        record.record.stream_shard_id,
                        record.record.record_ref.topic_partition,
                        record.record.record_ref.offset,
                        record.record.timestamp_unix_nanos,
                        Arc::clone(&record.record.message),
                        record.record.compression_cohort,
                    );
                    projected.severity_text = Arc::clone(&record.record.severity_text);
                    if include_fields {
                        projected.fields = Arc::clone(&record.record.fields);
                    }
                    projected
                };
                matches.push(LogMatch { record });
            }
            matches.len() < take
        });
        Some(matches)
    }

    /// Returns matching durable record references without cloning record data.
    ///
    /// Posting lists are offset ordered. The query starts with the shortest
    /// list and intersects each remaining list with a linear merge, making
    /// constraint order irrelevant to the asymptotic cost.
    #[must_use]
    pub fn query_refs(&self, query: &LogQuery) -> Vec<TelemetryRecordRef> {
        self.query_refs_checked(query).unwrap_or_default()
    }

    pub(super) fn query_refs_checked(
        &self,
        query: &LogQuery,
    ) -> TelemetryResult<Vec<TelemetryRecordRef>> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(Vec::new());
        }
        if self
            .indexed_frame_partitions
            .get(&query.topic_partition)
            .is_none_or(|partition| partition.appends.is_empty())
            && self.tier.is_none()
        {
            let Some(partition) = self.partitions.get(&query.topic_partition) else {
                return Ok(Vec::new());
            };
            let ordinals = self.query_ordinals(query, partition);
            let mut refs = Vec::with_capacity(ordinals.len());
            for ordinal in ordinals {
                if let Some(record) = partition.records.get(ordinal as usize) {
                    refs.push(record.record.record_ref);
                }
            }
            if let Some(limit) = query.limit {
                refs.truncate(limit);
            }
            return Ok(refs);
        }
        if query.sort != crate::QuerySort::Offset || self.tier.is_some() {
            return Ok(self
                .query(query)
                .into_iter()
                .map(|matched| matched.record.record_ref)
                .collect());
        }
        let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) else {
            return Ok(Vec::new());
        };
        let hot_partition = self
            .partitions
            .get(&query.topic_partition)
            .filter(|partition| !partition.records.is_empty());
        let mut refs = if let Some(hot_partition) = hot_partition {
            let mut hot_query = query.clone();
            hot_query.limit = None;
            self.query_ordinals(&hot_query, hot_partition)
                .into_iter()
                .filter_map(|ordinal| hot_partition.records.get(ordinal as usize))
                .map(|record| record.record.record_ref)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        for append in &partition.appends {
            if !append_matches_query_bounds(query, append) {
                continue;
            }
            for frame in &append.frames {
                if frame_matches_query_bounds(query, frame) {
                    refs.extend(self.query_indexed_frame_refs(query, append, frame)?);
                }
            }
        }
        refs.sort_unstable_by_key(|record_ref| record_ref.offset);
        if query.order == QueryOrder::NewestFirst {
            refs.reverse();
        }
        if let Some(limit) = query.limit {
            refs.truncate(limit);
        }
        Ok(refs)
    }

    pub(super) fn query_indexed_frame_refs(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
    ) -> TelemetryResult<Vec<TelemetryRecordRef>> {
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let exact_candidates_are_exact = exact_tokens
            .as_deref()
            .is_some_and(|tokens| !tokens.is_empty() || !exact_fields.is_empty());
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
        let candidates =
            self.indexed_frame_field_predicate_candidates_owned(query, frame, candidates)?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let decoded = decode_structural_records_with_cached_frame_data(
            &cached.structural,
            &candidates,
            &cached.embedded_index,
            &cached.templates,
            &cached.offsets,
            &cached.timestamps,
            false,
            true,
            None,
            None,
            Some(&cached.attribute_tables),
        )?;
        let mut refs = Vec::with_capacity(decoded.len());
        for record in &decoded {
            let absolute_offset = append
                .first_offset
                .get()
                .checked_add(record.offset.get())
                .map(LogicalOffset::new)
                .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
            let view = AbsoluteDecodedRecordView {
                record,
                absolute_offset,
            };
            if exact_candidates_are_exact || query.matches_index_candidate(&view) {
                refs.push(TelemetryRecordRef::new(
                    query.topic_partition,
                    absolute_offset,
                ));
            }
        }
        Ok(refs)
    }

    pub(super) fn query_indexed_frames(
        &self,
        query: &LogQuery,
        partition: &IndexedFramePartition,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let constraints = query.required_index_constraints();
        if constraints.impossible {
            return Ok(Vec::new());
        }
        let mut matches = Vec::new();
        for append in &partition.appends {
            if !append_matches_query_bounds(query, append) {
                continue;
            }
            for frame in &append.frames {
                if !frame_matches_query_bounds(query, frame) {
                    continue;
                }
                matches.extend(self.query_indexed_frame(
                    query,
                    append,
                    frame,
                    include_typed_metadata,
                    include_fields,
                )?);
            }
        }
        Ok(matches)
    }

    pub(super) fn query_ordinals(&self, query: &LogQuery, partition: &PartitionIndex) -> Vec<u32> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Vec::new();
        }
        let constraints = query.required_index_constraints();
        if constraints.impossible {
            return Vec::new();
        }
        let mut record_range =
            ordinal_record_window(&partition.records, query.start_offset, query.end_offset);
        let cursor_offset_applied = if query.sort == crate::QuerySort::Offset {
            if let Some(cursor) = query.after {
                match query.order {
                    QueryOrder::OldestFirst => {
                        let first_after = partition.records.partition_point(|record| {
                            record.record.record_ref.offset <= cursor.offset
                        });
                        record_range.start = record_range.start.max(first_after);
                    }
                    QueryOrder::NewestFirst => {
                        let first_at_or_after = partition.records.partition_point(|record| {
                            record.record.record_ref.offset < cursor.offset
                        });
                        record_range.end = record_range.end.min(first_at_or_after);
                    }
                }
                true
            } else {
                false
            }
        } else {
            false
        };
        if record_range.start > record_range.end {
            record_range.start = record_range.end;
        }
        let posting_start =
            u32::try_from(record_range.start).expect("record ordinal was bounded by ingest");
        let posting_end =
            u32::try_from(record_range.end).expect("record ordinal was bounded by ingest");
        // A fully indexable predicate already represents every leaf below
        // `query.predicate`. Keeping those leaves in `posting_lists` would
        // collect and intersect them once here and then repeat the same work
        // while building `predicate_candidates`. The legacy query builders
        // (`with_term`/`with_field`) remain separate constraints and still
        // need to be combined with the predicate result.
        let predicate_shape_is_index_exact = !matches!(query.predicate, LogPredicate::MatchAll)
            && hot_predicate_candidates_are_exact(&query.predicate);
        let predicate_limit = (predicate_shape_is_index_exact
            && query.terms.is_empty()
            && query.exact_fields.is_empty()
            && query.start_offset.is_none()
            && query.end_offset.is_none()
            && query.after.is_none()
            && query.sort == crate::QuerySort::Offset
            && query.order == QueryOrder::OldestFirst)
            .then_some(query.limit)
            .flatten();
        let mut predicate_candidates = if matches!(query.predicate, LogPredicate::MatchAll) {
            None
        } else {
            optimized_hot_predicate_candidates(
                &query.predicate,
                partition,
                posting_start,
                posting_end,
                predicate_limit,
            )
        };
        let predicate_is_index_exact = matches!(&query.predicate, LogPredicate::MatchAll)
            || (predicate_candidates.is_some() && predicate_shape_is_index_exact);
        let direct_terms = if predicate_is_index_exact {
            query.terms.iter().map(AsRef::as_ref).collect::<Vec<_>>()
        } else {
            constraints.terms.clone()
        };
        let direct_fields = if predicate_is_index_exact {
            query
                .exact_fields
                .iter()
                .map(|field| (field.key.as_ref(), field.value.as_ref()))
                .collect::<Vec<_>>()
        } else {
            constraints.fields.clone()
        };
        let mut posting_lists = Vec::<&HotPostingList>::with_capacity(
            direct_terms.len().saturating_add(direct_fields.len()),
        );
        for term in direct_terms {
            let normalized = normalize_term(term);
            let Some(term_id) = partition.term_ids.get(normalized.as_ref()) else {
                return Vec::new();
            };
            let Some(postings) = partition.term_postings.get(*term_id) else {
                return Vec::new();
            };
            if postings.is_empty_in(posting_start, posting_end) {
                return Vec::new();
            }
            posting_lists.push(postings);
        }
        for (key, value) in direct_fields {
            let Some(field_id) = partition
                .field_ids
                .get(key)
                .and_then(|values| values.get(value))
            else {
                return Vec::new();
            };
            let Some(postings) = partition.field_postings.get(*field_id) else {
                return Vec::new();
            };
            if postings.is_empty_in(posting_start, posting_end) {
                return Vec::new();
            }
            posting_lists.push(postings);
        }

        let mut predicate_postings = Vec::<Vec<u32>>::new();
        if predicate_candidates.is_none() {
            for key in constraints.field_exists {
                let Some(postings) = partition.field_presence_postings.get(key) else {
                    return Vec::new();
                };
                predicate_postings.push(postings.collect_in(
                    posting_start,
                    posting_end,
                    QueryOrder::OldestFirst,
                    None,
                ));
            }
            for (key, values) in constraints.field_in {
                let Some(value_ids) = partition.field_ids.get(key) else {
                    return Vec::new();
                };
                let mut field_postings = Vec::with_capacity(values.len());
                for value in values {
                    let Some(field_id) = value_ids.get(value) else {
                        continue;
                    };
                    let Some(posting) = partition.field_postings.get(*field_id) else {
                        continue;
                    };
                    field_postings.push(posting);
                }
                let ordinals =
                    collect_hot_posting_union(&field_postings, posting_start, posting_end, None);
                if ordinals.is_empty() {
                    return Vec::new();
                }
                predicate_postings.push(ordinals);
            }
            for (key, matcher) in constraints.field_text {
                let Some(candidates) =
                    hot_field_text_candidates(partition, key, matcher, posting_start, posting_end)
                else {
                    return Vec::new();
                };
                if candidates.is_empty() {
                    return Vec::new();
                }
                predicate_postings.push(candidates);
            }
            for (key, regex) in constraints.field_regex {
                let Some(candidates) = hot_field_predicate_candidates(
                    partition,
                    key,
                    |value| regex.is_match(value),
                    posting_start,
                    posting_end,
                ) else {
                    return Vec::new();
                };
                if candidates.is_empty() {
                    return Vec::new();
                }
                predicate_postings.push(candidates);
            }
            for (key, comparison, target) in constraints.field_numeric {
                let Some(candidates) = hot_numeric_field_candidates(
                    partition,
                    key,
                    comparison,
                    target,
                    posting_start,
                    posting_end,
                ) else {
                    return Vec::new();
                };
                if candidates.is_empty() {
                    return Vec::new();
                }
                predicate_postings.push(candidates);
            }
        }
        let needs_record_filter = if predicate_is_index_exact {
            query.start_timestamp_unix_nanos.is_some()
                || query.end_timestamp_unix_nanos.is_some()
                || (query.after.is_some() && !cursor_offset_applied)
        } else {
            query.needs_record_filter()
        };
        let mut ordinals_in_query_order = false;
        let mut ordinals = if posting_lists.is_empty() {
            if let Some(candidates) = predicate_candidates.take() {
                candidates
            } else if !needs_record_filter && predicate_postings.is_empty() {
                if query.sort == crate::QuerySort::Offset {
                    return collect_ordered_range(record_range, query.order, query.limit);
                }
                if partition.timestamp_order == TimestampOrder::NonDecreasing
                    && query.start_timestamp_unix_nanos.is_none()
                    && query.end_timestamp_unix_nanos.is_none()
                    && query.after.is_none()
                    && let Some(limit) = query.limit
                {
                    let limit = limit.min(record_range.len());
                    return match query.order {
                        QueryOrder::OldestFirst => (record_range.start
                            ..record_range.start.saturating_add(limit))
                            .map(|ordinal| {
                                u32::try_from(ordinal).expect("record ordinal was bounded")
                            })
                            .collect(),
                        QueryOrder::NewestFirst => (record_range.end.saturating_sub(limit)
                            ..record_range.end)
                            .rev()
                            .map(|ordinal| {
                                u32::try_from(ordinal).expect("record ordinal was bounded")
                            })
                            .collect(),
                    };
                }
                record_range
                    .map(|ordinal| u32::try_from(ordinal).expect("record ordinal was bounded"))
                    .collect::<Vec<_>>()
            } else {
                record_range
                    .map(|ordinal| u32::try_from(ordinal).expect("record ordinal was bounded"))
                    .collect::<Vec<_>>()
            }
        } else {
            posting_lists.sort_unstable_by_key(|postings| postings.cardinality);
            if posting_lists.len() == 1
                && !needs_record_filter
                && predicate_candidates.is_none()
                && predicate_postings.is_empty()
            {
                return posting_lists[0].collect_in(
                    posting_start,
                    posting_end,
                    query.order,
                    query.limit,
                );
            }
            let can_limit_intersection = predicate_postings.is_empty()
                && predicate_candidates.is_none()
                && !needs_record_filter
                && query.sort == crate::QuerySort::Offset;
            let ordinals = collect_hot_posting_intersection(
                &posting_lists,
                posting_start,
                posting_end,
                if can_limit_intersection {
                    query.order
                } else {
                    QueryOrder::OldestFirst
                },
                can_limit_intersection.then_some(query.limit).flatten(),
            );
            ordinals_in_query_order = can_limit_intersection;
            ordinals
        };
        if let Some(candidates) = predicate_candidates {
            let mut current = Some(ordinals);
            intersect_frame_candidate_slice(&mut current, &candidates);
            if current.as_ref().is_some_and(Vec::is_empty) {
                return Vec::new();
            }
            ordinals = current.unwrap_or_default();
        }
        if !predicate_postings.is_empty() {
            let mut current = Some(ordinals);
            for candidates in &predicate_postings {
                intersect_frame_candidate_slice(&mut current, candidates);
                if current.as_ref().is_some_and(Vec::is_empty) {
                    return Vec::new();
                }
            }
            ordinals = current.unwrap_or_default();
        }
        if needs_record_filter {
            ordinals.retain(|ordinal| {
                partition
                    .records
                    .get(*ordinal as usize)
                    .is_some_and(|record| {
                        if predicate_is_index_exact {
                            query.matches_index_bounds(&record.record)
                        } else {
                            query.matches_index_candidate(&record.record)
                        }
                    })
            });
        }
        if query.sort == crate::QuerySort::Timestamp {
            if let Some(limit) = query.limit
                && ordinals.len() > limit.saturating_mul(2).max(256)
            {
                retain_top_timestamp_ordinals(&mut ordinals, partition, query, limit);
            } else {
                ordinals.sort_unstable_by(|left, right| {
                    compare_timestamp_ordinals(partition, query.order, *left, *right)
                });
            }
        } else if query.order == QueryOrder::NewestFirst && !ordinals_in_query_order {
            ordinals.reverse();
        }
        if let Some(limit) = query.limit {
            ordinals.truncate(limit);
        }
        ordinals
    }
}
