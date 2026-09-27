use super::*;

impl LogStripe {
    /// Counts matching records without constructing `LogMatch` values.
    ///
    /// The durable-frame path still verifies candidate ordinals against the
    /// decoded structural records, so embedded-index collisions cannot change
    /// the result. It avoids building the larger `DurableLog` representation
    /// and is used by cardinality-only analytics scans.
    pub(crate) fn count_query_partitions_checked(
        &self,
        queries: &[LogQuery],
    ) -> TelemetryResult<u64> {
        queries.iter().try_fold(0_u64, |total, query| {
            let count = self.count_query_checked(query)?;
            total
                .checked_add(count)
                .ok_or(TelemetryError::RecordTooLarge)
        })
    }

    pub(crate) fn group_query_partitions_checked(
        &self,
        queries: &[LogQuery],
        keys: &[AnalyticsGroupKey],
    ) -> TelemetryResult<BTreeMap<Vec<Option<Arc<str>>>, u64>> {
        let mut groups = BTreeMap::new();
        for query in queries {
            if query.limit == Some(0) || query.has_invalid_range() {
                continue;
            }
            let message_predicate_key = Self::cached_message_predicate_key(&query.predicate);
            if self.tier.is_some() {
                self.group_tiered_query(query, keys, &mut groups, message_predicate_key.as_ref())?;
                continue;
            }
            if let Some(partition) = self.partitions.get(&query.topic_partition) {
                for ordinal in self.query_ordinals(query, partition) {
                    if let Some(record) = partition.records.get(ordinal as usize) {
                        let key = keys
                            .iter()
                            .map(|group| {
                                crate::analytics::durable_group_value(&record.record, *group)
                            })
                            .collect::<Vec<_>>();
                        *groups.entry(key).or_default() += 1;
                    }
                }
            }
            let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) else {
                continue;
            };
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append) {
                    continue;
                }
                for frame in &append.frames {
                    if !frame_matches_query_bounds(query, frame) {
                        continue;
                    }
                    let candidates = self
                        .cached_message_predicate_candidates_if_present(
                            frame.frame_id,
                            &query.predicate,
                        )
                        .map(|candidates| candidates.to_vec())
                        .unwrap_or_else(|| {
                            indexed_frame_candidates_for_append_with_phrase_mode(
                                query,
                                &frame.index,
                                frame.record_count,
                                append.tenant.as_ref(),
                                true,
                            )
                        });
                    self.group_indexed_frame_candidates(
                        query,
                        append,
                        frame,
                        candidates,
                        keys,
                        message_predicate_key.as_ref(),
                        &mut groups,
                    )?;
                }
            }
        }
        Ok(groups)
    }

    pub(super) fn group_tiered_query(
        &self,
        query: &LogQuery,
        keys: &[AnalyticsGroupKey],
        groups: &mut BTreeMap<Vec<Option<Arc<str>>>, u64>,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<()> {
        let Some(state) = &self.tier else {
            return Ok(());
        };
        let Some(tier) = state.tiers.get(&query.topic_partition) else {
            return Ok(());
        };
        let mut predicate_query = query.clone();
        predicate_query
            .exact_fields
            .retain(|field| field.key.as_ref() != "resource.loki.tenant");
        let tier_groups = tier.candidate_groups_cached(
            TierQueryRange {
                first_offset: query.start_offset.map(LogicalOffset::get),
                last_offset: query.end_offset.map(LogicalOffset::get),
                min_timestamp_unix_nanos: query.start_timestamp_unix_nanos,
                max_timestamp_unix_nanos: query.end_timestamp_unix_nanos,
                signal_identity: None,
            },
            &state.control_cache,
        )?;
        for group in tier_groups {
            let manifest = tier.load_group_cached(&group, &state.control_cache)?;
            let query_artifact = manifest
                .artifact(TierArtifactKind::QueryIndex)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no query index".into()))?;
            let appends = self.read_tier_ingest_group_cached(
                tier,
                query_artifact,
                &manifest.blocks,
                &state.control_cache,
            )?;
            let payload_artifact = manifest
                .artifact(TierArtifactKind::PayloadPack)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no payload pack".into()))?;
            let payload_metadata = ObjectMetadata {
                bytes: payload_artifact.bytes,
                version_token: payload_artifact.checksum.clone(),
                content_digest: payload_artifact.checksum.clone(),
            };
            let mut selected = Vec::new();
            let mut ranges = Vec::new();
            for append in appends.iter() {
                let bounds = IndexedFrameAppend {
                    tenant: Arc::from(append.tenant.as_str()),
                    first_offset: append.first_offset,
                    last_offset: append.last_offset,
                    record_count: append.record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                if !append_matches_query_bounds(query, &bounds)
                    || query.exact_fields.iter().any(|field| {
                        field.key.as_ref() == "resource.loki.tenant"
                            && field.value.as_ref() != bounds.tenant.as_ref()
                    })
                {
                    continue;
                }
                for cold_frame in &append.frames {
                    if !timestamp_bounds_overlap(
                        query,
                        cold_frame.min_timestamp_unix_nanos,
                        cold_frame.max_timestamp_unix_nanos,
                    ) {
                        continue;
                    }
                    let candidates = self
                        .cached_message_predicate_candidates_if_present(
                            cold_frame.frame_id,
                            &predicate_query.predicate,
                        )
                        .map(|candidates| candidates.to_vec())
                        .unwrap_or_else(|| {
                            indexed_frame_candidates_for_append_with_phrase_mode(
                                &predicate_query,
                                &cold_frame.index,
                                cold_frame.record_count,
                                bounds.tenant.as_ref(),
                                true,
                            )
                        });
                    if candidates.is_empty() {
                        continue;
                    }
                    let range_index = if self
                        .cached_indexed_frame_if_present(cold_frame.frame_id)
                        .is_some()
                    {
                        None
                    } else {
                        let range_end = cold_frame
                            .payload_offset
                            .checked_add(cold_frame.payload_bytes)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        let range_index = ranges.len();
                        ranges.push(cold_frame.payload_offset..range_end);
                        Some(range_index)
                    };
                    selected.push((
                        Arc::clone(&bounds.tenant),
                        bounds.first_offset,
                        bounds.last_offset,
                        bounds.record_count,
                        cold_frame.clone(),
                        candidates,
                        range_index,
                    ));
                }
            }
            let mut payloads = if ranges.is_empty() {
                Vec::new()
            } else {
                state.payload_cache.read_ranges_with_metadata(
                    tier.object_store(),
                    &payload_artifact.object_key,
                    &payload_metadata,
                    &ranges,
                )?
            };
            for (
                tenant,
                first_offset,
                last_offset,
                record_count,
                cold_frame,
                candidates,
                range_index,
            ) in selected
            {
                let compressed = range_index
                    .map(|index| Bytes::from(std::mem::take(&mut payloads[index])))
                    .unwrap_or_default();
                if range_index.is_some()
                    && blake3::hash(&compressed).to_hex().as_str() != cold_frame.payload_checksum
                {
                    return Err(TelemetryError::CorruptTier(format!(
                        "tiered frame {} payload checksum failed",
                        cold_frame.frame_id
                    )));
                }
                let frame = IndexedIngestFrame {
                    frame_id: cold_frame.frame_id,
                    cohort: cold_frame.cohort,
                    record_count: cold_frame.record_count,
                    structural_bytes: cold_frame.structural_bytes,
                    min_timestamp_unix_nanos: cold_frame.min_timestamp_unix_nanos,
                    max_timestamp_unix_nanos: cold_frame.max_timestamp_unix_nanos,
                    compressed,
                    index: cold_frame.index,
                };
                let append = IndexedFrameAppend {
                    tenant,
                    first_offset,
                    last_offset,
                    record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                self.group_indexed_frame_candidates(
                    &predicate_query,
                    &append,
                    &frame,
                    candidates,
                    keys,
                    message_predicate_key,
                    groups,
                )?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn group_indexed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        mut candidates: Vec<u32>,
        keys: &[AnalyticsGroupKey],
        message_predicate_key: Option<&Arc<str>>,
        groups: &mut BTreeMap<Vec<Option<Arc<str>>>, u64>,
    ) -> TelemetryResult<()> {
        candidates =
            self.indexed_frame_field_predicate_candidates_owned(query, frame, candidates)?;
        let cached_message_candidates =
            self.cached_message_predicate_candidates_with_key(query, frame, message_predicate_key)?;
        if let Some(message_candidates) = cached_message_candidates.as_ref() {
            let mut current = Some(candidates);
            intersect_frame_candidate_slice(&mut current, message_candidates);
            candidates = current.unwrap_or_default();
        }
        if candidates.is_empty() {
            return Ok(());
        }
        let cached = self.cached_indexed_frame(frame)?;
        retain_cached_timestamp_candidates(query, &cached, &mut candidates);
        if candidates.is_empty() {
            return Ok(());
        }
        if !cached_message_predicate_is_exact(&query.predicate)
            || keys
                .iter()
                .any(|key| !matches!(key, AnalyticsGroupKey::Minute))
        {
            for decoded in decode_structural_records_with_cached_frame_data(
                &cached.structural,
                &candidates,
                &cached.embedded_index,
                &cached.templates,
                &cached.offsets,
                &cached.timestamps,
                true,
                true,
                None,
                None,
                Some(&cached.attribute_tables),
            )? {
                let absolute_offset = append
                    .first_offset
                    .get()
                    .checked_add(decoded.offset.get())
                    .map(LogicalOffset::new)
                    .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
                let view = AbsoluteDecodedRecordView {
                    record: &decoded,
                    absolute_offset,
                };
                if query.matches(&view) {
                    let key = keys
                        .iter()
                        .map(|group| crate::analytics::decoded_group_value(&decoded, *group))
                        .collect::<Vec<_>>();
                    *groups.entry(key).or_default() += 1;
                }
            }
            return Ok(());
        }
        let mut field_postings = Vec::with_capacity(keys.len());
        let mut needs_decoded_fields = false;
        for group in keys {
            let posting = match group {
                AnalyticsGroupKey::SeverityText => self.cached_field_postings(
                    &cached,
                    frame.record_count,
                    "attr.loki.metadata.severity_text",
                )?,
                AnalyticsGroupKey::ScopeName => self.cached_field_postings(
                    &cached,
                    frame.record_count,
                    "attr.loki.metadata.scope_name",
                )?,
                AnalyticsGroupKey::Minute => None,
            };
            needs_decoded_fields |=
                posting.is_none() && !matches!(group, AnalyticsGroupKey::Minute);
            field_postings.push(posting);
        }
        let decoded_fields = needs_decoded_fields
            .then(|| decode_structural_fields(&cached.structural, &candidates))
            .transpose()?;
        let compact_grouping = !keys.is_empty()
            && keys.len() <= 2
            && field_postings.iter().enumerate().all(|(index, posting)| {
                posting.is_some() || matches!(keys[index], AnalyticsGroupKey::Minute)
            });
        if compact_grouping {
            if keys.len() == 1 {
                let mut compact_groups = HashMap::<IndexedGroupValue, u64>::new();
                for ordinal in &candidates {
                    let value =
                        indexed_group_value(keys[0], *ordinal, &cached, field_postings[0].as_ref());
                    *compact_groups.entry(value).or_default() += 1;
                }
                for (value, count) in compact_groups {
                    let key = vec![materialize_indexed_group_value(
                        value,
                        field_postings[0].as_ref(),
                    )];
                    *groups.entry(key).or_default() += count;
                }
            } else if keys.len() == 2 {
                let mut compact_groups =
                    HashMap::<(IndexedGroupValue, IndexedGroupValue), u64>::new();
                for ordinal in &candidates {
                    let first =
                        indexed_group_value(keys[0], *ordinal, &cached, field_postings[0].as_ref());
                    let second =
                        indexed_group_value(keys[1], *ordinal, &cached, field_postings[1].as_ref());
                    *compact_groups.entry((first, second)).or_default() += 1;
                }
                for ((first, second), count) in compact_groups {
                    let key = vec![
                        materialize_indexed_group_value(first, field_postings[0].as_ref()),
                        materialize_indexed_group_value(second, field_postings[1].as_ref()),
                    ];
                    *groups.entry(key).or_default() += count;
                }
            }
            return Ok(());
        }
        for (position, ordinal) in candidates.iter().enumerate() {
            let key = keys
                .iter()
                .enumerate()
                .map(|(key_index, group)| match group {
                    AnalyticsGroupKey::Minute => usize::try_from(*ordinal)
                        .ok()
                        .and_then(|index| cached.timestamps.get(index))
                        .map(|timestamp| {
                            Arc::<str>::from((timestamp / 60_000_000_000).to_string())
                        }),
                    AnalyticsGroupKey::SeverityText | AnalyticsGroupKey::ScopeName => {
                        field_postings[key_index]
                            .as_ref()
                            .and_then(|postings| {
                                postings
                                    .ordinal_value_ids
                                    .get(*ordinal as usize)
                                    .and_then(|id| postings.value_table.get(*id as usize))
                                    .cloned()
                            })
                            .or_else(|| {
                                decoded_fields.as_ref().and_then(|fields| {
                                    fields[position].iter().find_map(|field| {
                                        let wanted = match group {
                                            AnalyticsGroupKey::SeverityText => {
                                                "attr.loki.metadata.severity_text"
                                            }
                                            AnalyticsGroupKey::ScopeName => {
                                                "attr.loki.metadata.scope_name"
                                            }
                                            AnalyticsGroupKey::Minute => unreachable!(),
                                        };
                                        (field.key.as_ref() == wanted)
                                            .then(|| Arc::clone(&field.value))
                                    })
                                })
                            })
                    }
                })
                .collect::<Vec<_>>();
            *groups.entry(key).or_default() += 1;
        }
        Ok(())
    }

    pub(super) fn count_query_checked(&self, query: &LogQuery) -> TelemetryResult<u64> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(0);
        }
        let message_predicate_key = Self::cached_message_predicate_key(&query.predicate);
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let mut count = match self.partitions.get(&query.topic_partition) {
            Some(partition) => match self.count_hot_query_matches(query, partition) {
                Some(count) => count,
                None => u64::try_from(self.query_ordinals(query, partition).len())
                    .map_err(|_| TelemetryError::RecordTooLarge)?,
            },
            None => 0,
        };
        if let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) {
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append) {
                    continue;
                }
                for frame in &append.frames {
                    if frame_matches_query_bounds(query, frame) {
                        let frame_count = self.count_indexed_frame_matches(
                            query,
                            append,
                            frame,
                            exact_tokens.as_deref(),
                            &exact_fields,
                            message_predicate_key.as_ref(),
                        )?;
                        count = count
                            .checked_add(frame_count)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                    }
                }
            }
        }
        count = count
            .checked_add(self.count_tiered_groups(
                query,
                exact_tokens.as_deref(),
                &exact_fields,
                message_predicate_key.as_ref(),
            )?)
            .ok_or(TelemetryError::RecordTooLarge)?;
        if let Some(limit) = query.limit {
            count = count.min(u64::try_from(limit).unwrap_or(u64::MAX));
        }
        Ok(count)
    }

    /// Counts hot records from an index-backed candidate source without
    /// allocating the ordinal vector that the materializing query path needs.
    /// Returning `None` keeps the general path for predicates whose only safe
    /// candidate source requires residual decoding.
    pub(super) fn count_hot_query_matches(
        &self,
        query: &LogQuery,
        partition: &PartitionIndex,
    ) -> Option<u64> {
        let mut record_range =
            ordinal_record_window(&partition.records, query.start_offset, query.end_offset);
        if query.sort == crate::QuerySort::Offset
            && let Some(cursor) = query.after
        {
            match query.order {
                QueryOrder::OldestFirst => {
                    let first_after = partition
                        .records
                        .partition_point(|record| record.record.record_ref.offset <= cursor.offset);
                    record_range.start = record_range.start.max(first_after);
                }
                QueryOrder::NewestFirst => {
                    let first_at_or_after = partition
                        .records
                        .partition_point(|record| record.record.record_ref.offset < cursor.offset);
                    record_range.end = record_range.end.min(first_at_or_after);
                }
            }
        }
        if record_range.start >= record_range.end {
            return Some(0);
        }

        let posting_start = u32::try_from(record_range.start).ok()?;
        let posting_end = u32::try_from(record_range.end).ok()?;
        let mut direct_postings =
            Vec::with_capacity(query.terms.len().saturating_add(query.exact_fields.len()));
        for term in &query.terms {
            let Some(term_id) = partition.term_ids.get(normalize_term(term).as_ref()) else {
                return Some(0);
            };
            let Some(posting) = partition.term_postings.get(*term_id) else {
                return Some(0);
            };
            direct_postings.push(posting);
        }
        for field in &query.exact_fields {
            let Some(field_id) = partition
                .field_ids
                .get(field.key.as_ref())
                .and_then(|values| values.get(field.value.as_ref()))
            else {
                return Some(0);
            };
            let Some(posting) = partition.field_postings.get(*field_id) else {
                return Some(0);
            };
            direct_postings.push(posting);
        }
        if direct_postings
            .iter()
            .any(|posting| posting.is_empty_in(posting_start, posting_end))
        {
            return Some(0);
        }

        let predicate_driver =
            hot_predicate_driver_posting(&query.predicate, partition, posting_start, posting_end);
        let predicate_union = hot_predicate_postings(&query.predicate, partition);
        if predicate_driver.is_none()
            && direct_postings.is_empty()
            && predicate_union.is_none()
            && !matches!(query.predicate, LogPredicate::MatchAll)
        {
            return None;
        }
        if predicate_driver.is_none()
            && direct_postings.is_empty()
            && predicate_union.as_ref().is_some_and(Vec::is_empty)
        {
            return Some(0);
        }

        let mut source = predicate_driver;
        let mut source_covers_predicate = predicate_union.is_some() && source.is_some();
        if let Some(candidate) = direct_postings
            .iter()
            .copied()
            .min_by_key(|posting| posting.cardinality_in(posting_start, posting_end))
            && source.is_none_or(|current| {
                candidate.cardinality_in(posting_start, posting_end)
                    < current.cardinality_in(posting_start, posting_end)
            })
        {
            source = Some(candidate);
            source_covers_predicate = matches!(query.predicate, LogPredicate::MatchAll);
        }
        let use_predicate_union = source.is_none() && predicate_union.is_some();
        let source_covers_predicate = source_covers_predicate || use_predicate_union;

        let predicate_is_exact = hot_predicate_candidates_are_exact(&query.predicate);
        let needs_bounds_check = query.start_timestamp_unix_nanos.is_some()
            || query.end_timestamp_unix_nanos.is_some()
            || (query.after.is_some() && query.sort == crate::QuerySort::Timestamp);
        let mut count = 0_u64;
        let mut accept = |ordinal: u32| {
            let Some(record) = partition.records.get(ordinal as usize) else {
                return true;
            };
            if !direct_postings
                .iter()
                .all(|posting| posting.contains(ordinal))
            {
                return true;
            }
            let matches = if predicate_is_exact {
                (source_covers_predicate
                    || hot_predicate_matches_ordinal(&query.predicate, partition, ordinal))
                    && (!needs_bounds_check || query.matches_index_bounds(&record.record))
            } else {
                query.matches(&record.record)
            };
            if !matches {
                return true;
            }
            count = count.saturating_add(1);
            true
        };

        if let Some(source) = source {
            source.visit_in(
                posting_start,
                posting_end,
                QueryOrder::OldestFirst,
                &mut accept,
            );
        } else if let Some(postings) = predicate_union.as_ref() {
            visit_hot_posting_union(postings, posting_start, posting_end, &mut accept);
        } else {
            for ordinal in posting_start..posting_end {
                if !accept(ordinal) {
                    break;
                }
            }
        }
        Some(count)
    }

    pub(super) fn count_indexed_frame_matches(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        exact_tokens: Option<&[(&str, CaseSensitivity)]>,
        exact_fields: &[(Arc<str>, Arc<str>)],
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<u64> {
        if let Some(tokens) =
            exact_tokens.filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
            && !query.exact_fields.iter().any(|field| {
                field.key.as_ref() == "resource.loki.tenant"
                    && field.value.as_ref() != append.tenant.as_ref()
            })
            && let Some(candidates) =
                self.cached_exact_frame_candidates(frame.frame_id, tokens, exact_fields)
            && let Some(count) =
                self.count_cached_exact_candidates(query, frame.frame_id, &candidates)
        {
            return Ok(count);
        }
        if let Some(tokens) = exact_tokens.filter(|tokens| !tokens.is_empty()) {
            let cached = self.cached_indexed_frame(frame)?;
            let postings = self.cached_exact_message_terms(&cached, tokens)?;
            for ((token, case_sensitivity), posting) in tokens.iter().zip(&postings) {
                if let Some(posting) = posting {
                    self.cache_exact_posting(
                        exact_message_posting_key(frame.frame_id, token, *case_sensitivity),
                        Arc::clone(posting),
                    );
                }
            }
            if query.exact_fields.iter().any(|field| {
                field.key.as_ref() == "resource.loki.tenant"
                    && field.value.as_ref() != append.tenant.as_ref()
            }) {
                return Ok(0);
            }
            let field_postings = if exact_fields.is_empty() {
                Vec::new()
            } else {
                let postings = self.cached_exact_fields(&cached, exact_fields)?;
                for ((key, value), posting) in exact_fields.iter().zip(&postings) {
                    if let Some(posting) = posting {
                        self.cache_exact_posting(
                            ExactPostingKey::Field(
                                frame.frame_id,
                                Arc::clone(key),
                                Arc::clone(value),
                            ),
                            Arc::clone(posting),
                        );
                    }
                }
                postings
            };
            if exact_fields.is_empty() && postings.len() == 1 {
                let Some(posting) = postings.into_iter().next().flatten() else {
                    return Ok(0);
                };
                let count = count_cached_timestamp_candidates(query, &cached, &posting);
                return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
            }
            let mut exact_candidates = None;
            for posting in postings.into_iter().chain(field_postings) {
                let Some(posting) = posting else {
                    return Ok(0);
                };
                intersect_frame_candidate_slice(&mut exact_candidates, &posting);
                if exact_candidates.as_ref().is_some_and(Vec::is_empty) {
                    return Ok(0);
                }
            }
            let mut exact_candidates = exact_candidates.unwrap_or_default();
            retain_cached_timestamp_candidates(query, &cached, &mut exact_candidates);
            let count = exact_candidates.len();
            return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
        }
        if query.start_timestamp_unix_nanos.is_none()
            && query.end_timestamp_unix_nanos.is_none()
            && exact_fields.is_empty()
            && cached_message_predicate_is_exact(&query.predicate)
            && let Some(count) =
                self.exact_boolean_message_candidate_count(query, frame, message_predicate_key)?
        {
            return Ok(count);
        }
        if cached_message_predicate_is_exact(&query.predicate)
            && let Some(candidates) =
                self.exact_boolean_message_candidates(query, frame, message_predicate_key)?
        {
            if query.exact_fields.iter().any(|field| {
                field.key.as_ref() == "resource.loki.tenant"
                    && field.value.as_ref() != append.tenant.as_ref()
            }) {
                return Ok(0);
            }
            let cached = self.cached_indexed_frame(frame)?;
            let mut candidates = candidates;
            for posting in self.cached_exact_fields(&cached, exact_fields)? {
                let Some(posting) = posting else {
                    return Ok(0);
                };
                let mut current = Some(candidates);
                intersect_frame_candidate_slice(&mut current, &posting);
                candidates = current.unwrap_or_default();
                if candidates.is_empty() {
                    return Ok(0);
                }
            }
            retain_cached_timestamp_candidates(query, &cached, &mut candidates);
            let count = candidates.len();
            return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
        }
        if let Some(tokens) = query
            .exact_message_token_disjunction()
            .filter(|tokens| !tokens.is_empty())
        {
            if query.exact_fields.iter().any(|field| {
                field.key.as_ref() == "resource.loki.tenant"
                    && field.value.as_ref() != append.tenant.as_ref()
            }) {
                return Ok(0);
            }
            let cached = self.cached_indexed_frame(frame)?;
            let postings = self.cached_exact_message_terms(&cached, &tokens)?;
            let mut candidates = Vec::new();
            for posting in postings.into_iter().flatten() {
                union_sorted_ordinals(&mut candidates, posting.to_vec());
            }
            if candidates.is_empty() {
                return Ok(0);
            }
            for posting in self.cached_exact_fields(&cached, exact_fields)? {
                let Some(posting) = posting else {
                    return Ok(0);
                };
                let mut current = Some(candidates);
                intersect_frame_candidate_slice(&mut current, &posting);
                candidates = current.unwrap_or_default();
                if candidates.is_empty() {
                    return Ok(0);
                }
            }
            retain_cached_timestamp_candidates(query, &cached, &mut candidates);
            let count = candidates.len();
            return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
        }
        let exact_fields_are_authoritative_tenant = query.exact_fields.iter().all(|field| {
            field.key.as_ref() == "resource.loki.tenant"
                && field.value.as_ref() == append.tenant.as_ref()
        });
        if query.terms.is_empty()
            && (query.exact_fields.is_empty() || exact_fields_are_authoritative_tenant)
            && cached_message_predicate_is_exact(&query.predicate)
            && let Some(candidates) = self.cached_message_predicate_candidates_with_key(
                query,
                frame,
                message_predicate_key,
            )?
        {
            let cached = self.cached_indexed_frame(frame)?;
            let mut candidates = candidates.to_vec();
            retain_cached_timestamp_candidates(query, &cached, &mut candidates);
            let count = candidates.len();
            return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
        }
        let cached_message_candidates =
            self.cached_message_predicate_candidates_with_key(query, frame, message_predicate_key)?;
        let exact_fields_are_authoritative_tenant = query.exact_fields.iter().all(|field| {
            field.key.as_ref() == "resource.loki.tenant"
                && field.value.as_ref() == append.tenant.as_ref()
        });
        let required = query.required_index_constraints();
        let has_indexed_field_constraints = !required.field_exists.is_empty()
            || !required.field_in.is_empty()
            || !required.field_text.is_empty()
            || !required.field_regex.is_empty()
            || !required.field_numeric.is_empty();
        let message_candidates_are_base = exact_fields_are_authoritative_tenant
            && predicate_is_indexed_conjunction(&query.predicate)
            && has_indexed_field_constraints
            && cached_message_candidates.is_some();
        let candidates = if message_candidates_are_base {
            cached_message_candidates.as_ref().map_or_else(
                || {
                    indexed_frame_candidates_for_append(
                        query,
                        &frame.index,
                        frame.record_count,
                        append.tenant.as_ref(),
                    )
                },
                |candidates| candidates.clone(),
            )
        } else {
            indexed_frame_candidates_for_append(
                query,
                &frame.index,
                frame.record_count,
                append.tenant.as_ref(),
            )
        };
        let mut candidates =
            self.indexed_frame_field_predicate_candidates_owned(query, frame, candidates)?;
        if !message_candidates_are_base
            && let Some(message_candidates) = cached_message_candidates.as_ref()
        {
            let mut current = Some(candidates);
            intersect_frame_candidate_slice(&mut current, message_candidates);
            candidates = current.unwrap_or_default();
        }
        if candidates.is_empty() {
            return Ok(0);
        }
        if query.terms.is_empty()
            && query.exact_fields.iter().all(|field| {
                field.key.as_ref() == "resource.loki.tenant"
                    && field.value.as_ref() == append.tenant.as_ref()
            })
            && hot_predicate_candidates_are_exact(&query.predicate)
        {
            let cached = self.cached_indexed_frame(frame)?;
            retain_cached_timestamp_candidates(query, &cached, &mut candidates);
            let count = candidates.len();
            return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
        }
        if query.terms.is_empty() && cached_message_candidates.is_some() {
            let cached = self.cached_indexed_frame(frame)?;
            retain_cached_timestamp_candidates(query, &cached, &mut candidates);
            if candidates.is_empty() {
                return Ok(0);
            }
            let fields = decode_structural_fields(&cached.structural, &candidates)?;
            let matches = candidates
                .iter()
                .zip(fields.iter())
                .filter(|(ordinal, fields)| {
                    let timestamp_matches = usize::try_from(**ordinal)
                        .ok()
                        .and_then(|index| cached.timestamps.get(index))
                        .is_some_and(|timestamp| query.timestamp_matches(*timestamp));
                    let exact_fields_match = query.exact_fields.iter().all(|expected| {
                        if expected.key.as_ref() == "resource.loki.tenant" {
                            expected.value.as_ref() == append.tenant.as_ref()
                        } else {
                            fields.iter().any(|field| {
                                field.key == expected.key && field.value == expected.value
                            })
                        }
                    });
                    timestamp_matches
                        && exact_fields_match
                        && crate::query::predicate_fields_match(&query.predicate, fields.as_ref())
                })
                .count();
            return u64::try_from(matches).map_err(|_| TelemetryError::RecordTooLarge);
        }
        // The embedded index is deliberately a candidate superset. For a
        // cardinality-only message query, decode only message bodies and
        // verify the residual predicate instead of materializing every typed
        // field and attribute. The append tenant is authoritative for its
        // tenant field, so it can be checked without record decoding.
        if query.can_use_indexed_message_filter() {
            let cached = self.cached_indexed_frame(frame)?;
            let messages = decode_structural_messages_with_embedded_index_and_templates(
                &cached.structural,
                &candidates,
                &cached.embedded_index,
                &cached.templates,
            )?;
            let fields = (!query.exact_fields.is_empty())
                .then(|| decode_structural_fields(&cached.structural, &candidates))
                .transpose()?;
            let matches = candidates
                .iter()
                .zip(messages.iter())
                .enumerate()
                .filter(|(position, (ordinal, message))| {
                    let index = usize::try_from(**ordinal).ok();
                    let timestamp_matches = index
                        .and_then(|index| cached.timestamps.get(index))
                        .is_some_and(|timestamp| query.timestamp_matches(*timestamp));
                    let fields_match = fields.as_ref().is_none_or(|fields| {
                        fields.get(*position).is_some_and(|decoded_fields| {
                            query.exact_fields.iter().all(|expected| {
                                if expected.key.as_ref() == "resource.loki.tenant" {
                                    expected.value.as_ref() == append.tenant.as_ref()
                                } else {
                                    decoded_fields.iter().any(|field| {
                                        field.key == expected.key && field.value == expected.value
                                    })
                                }
                            })
                        })
                    });
                    timestamp_matches
                        && fields_match
                        && query.message_candidate_matches(message).unwrap_or(false)
                })
                .count();
            return u64::try_from(matches).map_err(|_| TelemetryError::RecordTooLarge);
        }
        let cached = self.cached_indexed_frame(frame)?;
        let matches = decode_structural_records_with_cached_frame_data(
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
        )?
        .into_iter()
        .filter(|record| {
            let absolute_offset = append
                .first_offset
                .get()
                .checked_add(record.offset.get())
                .map(LogicalOffset::new);
            absolute_offset.is_some_and(|absolute_offset| {
                query.matches_index_candidate(&AbsoluteDecodedRecordView {
                    record,
                    absolute_offset,
                })
            })
        })
        .count();
        u64::try_from(matches).map_err(|_| TelemetryError::RecordTooLarge)
    }

    /// Counts one tenant's records without reading or reconstructing payloads.
    ///
    /// Tenant identity is exact append metadata for compressed and tiered
    /// frames. The legacy hot-record path uses its exact (non-hashed) posting
    /// table. Consequently fingerprint collisions can never change this count.
    pub(crate) fn count_tenant_records(
        &self,
        tenant: &str,
        partitions: &[TopicPartition],
    ) -> TelemetryResult<u64> {
        let mut total = 0_u64;
        for topic_partition in partitions {
            if let Some(partition) = self.partitions.get(topic_partition)
                && let Some(posting) = partition
                    .field_ids
                    .get("resource.loki.tenant")
                    .and_then(|values| values.get(tenant))
                    .and_then(|field_id| partition.field_postings.get(*field_id))
            {
                total = total
                    .checked_add(
                        u64::try_from(posting.cardinality)
                            .map_err(|_| TelemetryError::RecordTooLarge)?,
                    )
                    .ok_or(TelemetryError::RecordTooLarge)?;
            }
            if let Some(partition) = self.indexed_frame_partitions.get(topic_partition) {
                for append in &partition.appends {
                    if append.tenant.as_ref() == tenant {
                        total = total
                            .checked_add(u64::from(append.record_count))
                            .ok_or(TelemetryError::RecordTooLarge)?;
                    }
                }
            }
            total = total
                .checked_add(self.count_tiered_tenant_records(*topic_partition, tenant)?)
                .ok_or(TelemetryError::RecordTooLarge)?;
        }
        Ok(total)
    }

    /// Returns only logical partitions that currently contain this tenant.
    ///
    /// The directory is reconstructed from hot postings, compressed-frame
    /// append metadata, and object-tier catalogs, so callers do not need to
    /// enumerate every configured logical partition after restart or offload.
    pub(crate) fn tenant_partitions(
        &mut self,
        tenant: &str,
    ) -> TelemetryResult<Vec<TopicPartition>> {
        if let Some(partitions) = self.active_partition_cache.get(tenant) {
            return Ok(partitions.clone());
        }
        let mut candidates = self.partitions.keys().copied().collect::<Vec<_>>();
        candidates.extend(self.indexed_frame_partitions.keys().copied());
        if let Some(state) = &self.tier {
            candidates.extend(state.tiers.keys().copied());
        }
        candidates.sort_unstable();
        candidates.dedup();

        let mut matches = Vec::with_capacity(candidates.len());
        for partition in candidates {
            if self.count_tenant_records(tenant, std::slice::from_ref(&partition))? > 0 {
                matches.push(partition);
            }
        }
        self.active_partition_cache
            .insert(Arc::from(tenant), matches.clone());
        Ok(matches)
    }

    pub(super) fn read_tier_ingest_group_cached(
        &self,
        tier: &TelemetryObjectTier<SharedTelemetryObjectStore>,
        query_artifact: &TierArtifact,
        blocks: &[crate::TierBlockEntry],
        control_cache: &SsdObjectCache,
    ) -> TelemetryResult<Arc<[DecodedTierIngestAppend]>> {
        if let Some(appends) = control_cache.parsed_tier_ingest_hit(&query_artifact.object_key)? {
            return Ok(appends);
        }
        let query_index = tier.read_artifact_cached(
            query_artifact,
            MAX_TIER_QUERY_INDEX_READ_BYTES,
            control_cache,
        )?;
        let appends =
            Arc::<[DecodedTierIngestAppend]>::from(decode_tier_ingest_group(&query_index, blocks)?);
        control_cache.admit_parsed_tier_ingest(
            query_artifact.object_key.clone(),
            Arc::clone(&appends),
            query_artifact.bytes,
        )?;
        Ok(appends)
    }

    pub(super) fn count_tiered_tenant_records(
        &self,
        topic_partition: TopicPartition,
        tenant: &str,
    ) -> TelemetryResult<u64> {
        let Some(state) = &self.tier else {
            return Ok(0);
        };
        let Some(tier) = state.tiers.get(&topic_partition) else {
            return Ok(0);
        };
        let groups = tier.candidate_groups_cached(
            TierQueryRange {
                first_offset: None,
                last_offset: None,
                min_timestamp_unix_nanos: None,
                max_timestamp_unix_nanos: None,
                signal_identity: None,
            },
            &state.control_cache,
        )?;
        let mut total = 0_u64;
        for group in groups {
            let manifest = tier.load_group_cached(&group, &state.control_cache)?;
            let query_artifact = manifest
                .artifact(TierArtifactKind::QueryIndex)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no query index".into()))?;
            let appends = self.read_tier_ingest_group_cached(
                tier,
                query_artifact,
                &manifest.blocks,
                &state.control_cache,
            )?;
            for append in appends.iter() {
                if append.tenant == tenant {
                    total = total
                        .checked_add(u64::from(append.record_count))
                        .ok_or(TelemetryError::RecordTooLarge)?;
                }
            }
        }
        Ok(total)
    }

    pub(super) fn count_tiered_groups(
        &self,
        query: &LogQuery,
        exact_tokens: Option<&[(&str, CaseSensitivity)]>,
        exact_fields: &[(Arc<str>, Arc<str>)],
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<u64> {
        let Some(state) = &self.tier else {
            return Ok(0);
        };
        let Some(tier) = state.tiers.get(&query.topic_partition) else {
            return Ok(0);
        };
        let groups = tier.candidate_groups_cached(
            TierQueryRange {
                first_offset: query.start_offset.map(LogicalOffset::get),
                last_offset: query.end_offset.map(LogicalOffset::get),
                min_timestamp_unix_nanos: query.start_timestamp_unix_nanos,
                max_timestamp_unix_nanos: query.end_timestamp_unix_nanos,
                signal_identity: None,
            },
            &state.control_cache,
        )?;
        let mut total = 0_u64;
        for group in groups {
            let manifest = tier.load_group_cached(&group, &state.control_cache)?;
            let query_artifact = manifest
                .artifact(TierArtifactKind::QueryIndex)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no query index".into()))?;
            let appends = self.read_tier_ingest_group_cached(
                tier,
                query_artifact,
                &manifest.blocks,
                &state.control_cache,
            )?;
            let payload_artifact = manifest
                .artifact(TierArtifactKind::PayloadPack)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no payload pack".into()))?;
            let payload_metadata = ObjectMetadata {
                bytes: payload_artifact.bytes,
                version_token: payload_artifact.checksum.clone(),
                content_digest: payload_artifact.checksum.clone(),
            };
            let mut selected = Vec::new();
            let mut ranges = Vec::new();
            for append in appends.iter() {
                let bounds = IndexedFrameAppend {
                    tenant: Arc::from(append.tenant.as_str()),
                    first_offset: append.first_offset,
                    last_offset: append.last_offset,
                    record_count: append.record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                if !append_matches_query_bounds(query, &bounds) {
                    continue;
                }
                if query.exact_fields.iter().any(|field| {
                    field.key.as_ref() == "resource.loki.tenant"
                        && field.value.as_ref() != bounds.tenant.as_ref()
                }) {
                    continue;
                }
                for cold_frame in &append.frames {
                    if exact_tokens.is_none()
                        && exact_fields.is_empty()
                        && query.terms.is_empty()
                        && cached_message_predicate_is_exact(&query.predicate)
                        && let Some(candidates) = self
                            .cached_message_predicate_candidates_arc_if_present(
                                cold_frame.frame_id,
                                message_predicate_key,
                            )
                        && let Some(count) = self.count_cached_message_predicate_candidates(
                            query,
                            cold_frame.frame_id,
                            &candidates,
                        )
                    {
                        total = total
                            .checked_add(count)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        continue;
                    }
                    let cached_exact_candidates = exact_tokens
                        .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                        .and_then(|tokens| {
                            self.cached_exact_frame_candidates(
                                cold_frame.frame_id,
                                tokens,
                                exact_fields,
                            )
                        });
                    if let Some(candidates) = cached_exact_candidates.as_deref()
                        && let Some(count) = self.count_cached_exact_candidates(
                            query,
                            cold_frame.frame_id,
                            candidates,
                        )
                    {
                        total = total
                            .checked_add(count)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        continue;
                    }
                    let candidates = cached_exact_candidates.unwrap_or_else(|| {
                        indexed_frame_candidates_for_append(
                            query,
                            &cold_frame.index,
                            cold_frame.record_count,
                            bounds.tenant.as_ref(),
                        )
                    });
                    if candidates.is_empty()
                        || !timestamp_bounds_overlap(
                            query,
                            cold_frame.min_timestamp_unix_nanos,
                            cold_frame.max_timestamp_unix_nanos,
                        )
                    {
                        continue;
                    }
                    let range_index = if self
                        .cached_indexed_frame_if_present(cold_frame.frame_id)
                        .is_some()
                    {
                        None
                    } else {
                        let range_end = cold_frame
                            .payload_offset
                            .checked_add(cold_frame.payload_bytes)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        let range_index = ranges.len();
                        ranges.push(cold_frame.payload_offset..range_end);
                        Some(range_index)
                    };
                    selected.push((
                        Arc::clone(&bounds.tenant),
                        bounds.first_offset,
                        bounds.last_offset,
                        bounds.record_count,
                        cold_frame.clone(),
                        candidates,
                        range_index,
                    ));
                }
            }
            let mut payloads = if ranges.is_empty() {
                Vec::new()
            } else {
                state.payload_cache.read_ranges_with_metadata(
                    tier.object_store(),
                    &payload_artifact.object_key,
                    &payload_metadata,
                    &ranges,
                )?
            };
            for (
                tenant,
                first_offset,
                last_offset,
                record_count,
                cold_frame,
                _candidates,
                range_index,
            ) in selected
            {
                let compressed = range_index
                    .map(|index| Bytes::from(std::mem::take(&mut payloads[index])))
                    .unwrap_or_default();
                if range_index.is_some()
                    && blake3::hash(&compressed).to_hex().as_str() != cold_frame.payload_checksum
                {
                    return Err(TelemetryError::CorruptTier(format!(
                        "tiered frame {} payload checksum failed",
                        cold_frame.frame_id
                    )));
                }
                let frame = IndexedIngestFrame {
                    frame_id: cold_frame.frame_id,
                    cohort: cold_frame.cohort,
                    record_count: cold_frame.record_count,
                    structural_bytes: cold_frame.structural_bytes,
                    min_timestamp_unix_nanos: cold_frame.min_timestamp_unix_nanos,
                    max_timestamp_unix_nanos: cold_frame.max_timestamp_unix_nanos,
                    compressed,
                    index: cold_frame.index,
                };
                let bounds = IndexedFrameAppend {
                    tenant,
                    first_offset,
                    last_offset,
                    record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                total = total
                    .checked_add(self.count_indexed_frame_matches(
                        query,
                        &bounds,
                        &frame,
                        exact_tokens,
                        exact_fields,
                        message_predicate_key,
                    )?)
                    .ok_or(TelemetryError::RecordTooLarge)?;
            }
        }
        Ok(total)
    }
}
