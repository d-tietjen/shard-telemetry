use super::*;

impl LogStripe {
    pub(super) fn query_indexed_frame(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        if query.sort == crate::QuerySort::Timestamp
            && query.limit.is_some_and(|limit| limit > 0)
            && let Some(candidates) =
                self.embedded_indexed_frame_candidates(query, append, frame)?
        {
            return self.decode_embedded_indexed_frame_candidates(
                query,
                append,
                frame,
                candidates,
                include_typed_metadata,
                include_fields,
            );
        }
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
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        self.decode_indexed_frame_candidates(
            query,
            append,
            frame,
            candidates,
            include_typed_metadata,
            include_fields,
            exact_candidates_are_exact,
        )
    }

    pub(super) fn embedded_indexed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        if exact_tokens
            .as_deref()
            .is_none_or(|tokens| tokens.is_empty())
            && exact_fields.is_empty()
        {
            return Ok(None);
        }
        // Keep token-only timestamp queries on the exact posting path. The
        // embedded field index is the bounded candidate driver here; without
        // a field constraint it would add no useful narrowing.
        if exact_fields.is_empty() {
            return Ok(None);
        }
        if query.exact_fields.iter().any(|field| {
            field.key.as_ref() == "resource.loki.tenant"
                && field.value.as_ref() != append.tenant.as_ref()
        }) {
            return Ok(Some(Vec::new()));
        }
        let mut candidates = None;
        let cached = self.cached_indexed_frame(frame)?;
        if let Some(tokens) = exact_tokens.filter(|tokens| !tokens.is_empty()) {
            for (token, _) in tokens {
                intersect_frame_candidate_slice(
                    &mut candidates,
                    &frame.index.term_candidate_ordinals(token),
                );
            }
        }
        for (key, value) in exact_fields {
            intersect_frame_candidate_slice(
                &mut candidates,
                &frame.index.field_candidate_ordinals(&key, &value),
            );
        }
        let mut candidates = candidates.unwrap_or_default();
        if candidates.is_empty() {
            return Ok(Some(candidates));
        }
        // The live/recovered frame already retains the embedded index. Avoid
        // decompressing and decoding structural state for frames that the
        // index proves cannot contain this exact token/field conjunction.
        retain_cached_timestamp_candidates(query, &cached, &mut candidates);
        Ok(Some(candidates))
    }

    pub(super) fn decode_embedded_indexed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        mut candidates: Vec<u32>,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let limit = query.limit.expect("bounded timestamp query has a limit");
        let cached = self.cached_indexed_frame(frame)?;
        let decode_fields = include_fields || !query.exact_fields.is_empty();
        let batch_len = limit.saturating_mul(2).max(256);
        let mut matches = Vec::with_capacity(limit.min(candidates.len()));
        while !candidates.is_empty() && matches.len() < limit {
            let take = batch_len.min(candidates.len());
            if take < candidates.len() {
                candidates.select_nth_unstable_by(take - 1, |left, right| {
                    let left = usize::try_from(*left).expect("embedded ordinal fits usize");
                    let right = usize::try_from(*right).expect("embedded ordinal fits usize");
                    let ordering = cached.timestamps[left]
                        .cmp(&cached.timestamps[right])
                        .then_with(|| cached.offsets[left].cmp(&cached.offsets[right]));
                    match query.order {
                        QueryOrder::OldestFirst => ordering,
                        QueryOrder::NewestFirst => ordering.reverse(),
                    }
                });
            }
            let mut batch: Vec<u32> = candidates.drain(..take).collect();
            batch.sort_unstable();
            matches.extend(self.decode_decompressed_frame_candidates(
                query,
                append,
                frame,
                &cached.structural,
                &cached.embedded_index,
                &cached.templates,
                &cached.offsets,
                &cached.timestamps,
                &cached.attribute_tables,
                &cached,
                &batch,
                include_typed_metadata,
                decode_fields,
                None,
                None,
                false,
                false,
            )?);
        }
        sort_and_limit_matches(&mut matches, query, limit);
        Ok(matches)
    }

    pub(super) fn exact_indexed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        exact_tokens: &[(&str, CaseSensitivity)],
        exact_fields: &[(Arc<str>, Arc<str>)],
    ) -> TelemetryResult<Option<Vec<u32>>> {
        if exact_tokens.is_empty() && exact_fields.is_empty() {
            return Ok(None);
        }
        if query.exact_fields.iter().any(|field| {
            field.key.as_ref() == "resource.loki.tenant"
                && field.value.as_ref() != append.tenant.as_ref()
        }) {
            return Ok(Some(Vec::new()));
        }
        let cached = self.cached_indexed_frame(frame)?;
        let message_postings = exact_tokens
            .iter()
            .map(|(token, case_sensitivity)| {
                self.cached_exact_posting(&exact_message_posting_key(
                    frame.frame_id,
                    token,
                    *case_sensitivity,
                ))
            })
            .collect::<Vec<_>>();
        let message_postings = if message_postings.iter().all(Option::is_some) {
            message_postings
        } else if exact_tokens.is_empty() {
            Vec::new()
        } else {
            let computed = self.cached_exact_message_terms(&cached, exact_tokens)?;
            for ((token, case_sensitivity), posting) in exact_tokens.iter().zip(&computed) {
                if let Some(posting) = posting {
                    self.cache_exact_posting(
                        exact_message_posting_key(frame.frame_id, token, *case_sensitivity),
                        Arc::clone(posting),
                    );
                }
            }
            computed
        };
        let field_postings = exact_fields
            .iter()
            .map(|(key, value)| {
                self.cached_exact_posting(&ExactPostingKey::Field(
                    frame.frame_id,
                    Arc::clone(key),
                    Arc::clone(value),
                ))
            })
            .collect::<Vec<_>>();
        let field_postings = if field_postings.iter().all(Option::is_some) {
            field_postings
        } else if exact_fields.is_empty() {
            Vec::new()
        } else {
            let computed = self.cached_exact_fields(&cached, exact_fields)?;
            for ((key, value), posting) in exact_fields.iter().zip(&computed) {
                if let Some(posting) = posting {
                    self.cache_exact_posting(
                        ExactPostingKey::Field(frame.frame_id, Arc::clone(key), Arc::clone(value)),
                        Arc::clone(posting),
                    );
                }
            }
            computed
        };
        let mut candidates = None;
        for posting in message_postings.into_iter().chain(field_postings) {
            let Some(posting) = posting else {
                return Ok(Some(Vec::new()));
            };
            intersect_frame_candidate_slice(&mut candidates, &posting);
            if candidates.as_ref().is_some_and(Vec::is_empty) {
                return Ok(Some(Vec::new()));
            }
        }
        let mut candidates = candidates.expect("an exact frame constraint has a posting");
        retain_cached_timestamp_candidates(query, &cached, &mut candidates);
        Ok(Some(candidates))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn decode_indexed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        candidates: Vec<u32>,
        include_typed_metadata: bool,
        include_fields: bool,
        candidates_are_exact: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let mut candidates =
            self.indexed_frame_field_predicate_candidates_owned(query, frame, candidates)?;
        let mut exact_message_candidates = false;
        if !matches!(query.predicate, LogPredicate::MatchAll)
            && let Some(message_candidates) =
                self.cached_message_predicate_candidates(query, frame)?
        {
            let mut current = Some(candidates);
            intersect_frame_candidate_slice(&mut current, &message_candidates);
            candidates = current.unwrap_or_default();
            exact_message_candidates = cached_message_predicate_is_exact(&query.predicate);
        }
        // Structural projection decoders consume record ordinals in ascending
        // order. Some bounded timestamp paths select candidates in query order
        // (newest first), so restore the decoder invariant before any cached
        // message or field projection is attempted.
        normalize_structural_candidate_ordinals(&mut candidates);
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let typed_metadata = include_typed_metadata
            .then(|| self.cached_typed_metadata(&cached, frame.record_count))
            .transpose()?;
        let message_filterable = query.message_candidate_matches("").is_some();
        let message_only_query = message_filterable
            && query
                .exact_fields
                .iter()
                .all(|field| field.key.as_ref() == "resource.loki.tenant");
        let message_predicate_checked = message_only_query && exact_message_candidates;
        let tenant_only_without_residual = query
            .exact_fields
            .iter()
            .all(|field| field.key.as_ref() == "resource.loki.tenant")
            && !query.has_residual_predicate();
        let decode_fields = include_typed_metadata
            || include_fields
            || !(message_predicate_checked || tenant_only_without_residual || candidates_are_exact);
        if query.sort == crate::QuerySort::Timestamp
            && (!query.has_residual_predicate() || message_filterable)
            && let Some(limit) = query.limit
            && candidates.len() > limit.saturating_mul(2).max(256)
        {
            let structural = cached.structural.as_ref();
            if frame.index.timestamp_offset_ordinal_ordered() {
                let filter_messages_first = query.has_residual_predicate() && message_filterable;
                let batch_len = if filter_messages_first {
                    limit.saturating_mul(4).max(1_024)
                } else {
                    limit.saturating_mul(2).max(256)
                };
                let mut matches = Vec::with_capacity(limit);
                let mut consumed = 0usize;
                while matches.len() < limit && consumed < candidates.len() {
                    let mut selected_messages = None;
                    let mut batch = match query.order {
                        QueryOrder::OldestFirst => {
                            let start = consumed;
                            let end = candidates.len().min(start.saturating_add(batch_len));
                            consumed = end;
                            candidates[start..end].to_vec()
                        }
                        QueryOrder::NewestFirst => {
                            let end = candidates.len().saturating_sub(consumed);
                            let start = end.saturating_sub(batch_len);
                            consumed = consumed.saturating_add(end - start);
                            candidates[start..end].to_vec()
                        }
                    };
                    if filter_messages_first {
                        let messages =
                            decode_structural_messages_with_embedded_index_and_templates(
                                structural,
                                &batch,
                                &cached.embedded_index,
                                &cached.templates,
                            )?;
                        let mut filtered_batch = Vec::with_capacity(messages.len());
                        let mut filtered_messages = Vec::with_capacity(messages.len());
                        for (ordinal, message) in batch.into_iter().zip(messages) {
                            if query.message_candidate_matches(&message).unwrap_or(false) {
                                filtered_batch.push(ordinal);
                                filtered_messages.push(message);
                            }
                        }
                        batch = filtered_batch;
                        selected_messages = Some(filtered_messages);
                    }
                    let message_predicate_checked = filter_messages_first && message_only_query;
                    if !batch.is_empty() {
                        matches.extend(
                            self.decode_decompressed_frame_candidates(
                                query,
                                append,
                                frame,
                                structural,
                                &cached.embedded_index,
                                &cached.templates,
                                &cached.offsets,
                                &cached.timestamps,
                                &cached.attribute_tables,
                                &cached,
                                &batch,
                                include_typed_metadata,
                                decode_fields,
                                selected_messages.as_deref(),
                                typed_metadata
                                    .as_ref()
                                    .map(|metadata| metadata.packed.as_ref()),
                                candidates_are_exact,
                                message_predicate_checked,
                            )?,
                        );
                    }
                }
                sort_and_limit_matches(&mut matches, query, limit);
                return Ok(matches);
            }
            let offsets = cached.offsets.as_ref();
            let timestamps = cached.timestamps.as_ref();
            let mut ranked = candidates.to_vec();
            for ordinal in &ranked {
                let index = usize::try_from(*ordinal).map_err(|_| {
                    TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
                })?;
                if index >= offsets.len() || index >= timestamps.len() {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "compressed ingest candidate ordinal is out of range",
                    ));
                }
            }
            let compare_positions = |left: &u32, right: &u32| {
                let left = usize::try_from(*left).expect("candidate ordinal was validated");
                let right = usize::try_from(*right).expect("candidate ordinal was validated");
                (timestamps[left], offsets[left]).cmp(&(timestamps[right], offsets[right]))
            };
            if !query.has_residual_predicate() {
                // Candidate membership is exact here, so select the requested page before
                // decoding structural records instead of sorting the whole frame.
                let keep = ranked.len().min(limit);
                ranked.select_nth_unstable_by(keep - 1, |left, right| {
                    let ordering = compare_positions(left, right);
                    match query.order {
                        QueryOrder::OldestFirst => ordering,
                        QueryOrder::NewestFirst => ordering.reverse(),
                    }
                });
                ranked.truncate(keep);
                ranked.sort_unstable();
                let mut matches = self.decode_decompressed_frame_candidates(
                    query,
                    append,
                    frame,
                    structural,
                    &cached.embedded_index,
                    &cached.templates,
                    &cached.offsets,
                    &cached.timestamps,
                    &cached.attribute_tables,
                    &cached,
                    &ranked,
                    include_typed_metadata,
                    decode_fields,
                    None,
                    typed_metadata
                        .as_ref()
                        .map(|metadata| metadata.packed.as_ref()),
                    candidates_are_exact,
                    false,
                )?;
                sort_and_limit_matches(&mut matches, query, limit);
                return Ok(matches);
            }
            let filter_messages_first = query.has_residual_predicate() && message_filterable;
            let batch_len = if filter_messages_first {
                limit.saturating_mul(4).max(1_024)
            } else {
                limit.saturating_mul(2).max(256)
            };
            let mut matches = Vec::with_capacity(limit);
            let already_ascending = ranked
                .windows(2)
                .all(|pair| compare_positions(&pair[0], &pair[1]).is_le());
            if already_ascending && query.order == QueryOrder::NewestFirst {
                ranked.reverse();
            }
            let mut consumed = 0usize;
            while matches.len() < limit && consumed < ranked.len() {
                let remaining = ranked.len().saturating_sub(consumed);
                let take = remaining.min(batch_len);
                if !already_ascending && remaining > take {
                    // Residual predicates may reject this batch, so select the next
                    // timestamp page repeatedly without sorting the whole frame.
                    ranked[consumed..].select_nth_unstable_by(take - 1, |left, right| {
                        let ordering = compare_positions(left, right);
                        match query.order {
                            QueryOrder::OldestFirst => ordering,
                            QueryOrder::NewestFirst => ordering.reverse(),
                        }
                    });
                }
                let end = consumed + take;
                let mut selected_messages = None;
                let mut batch = ranked[consumed..end].to_vec();
                batch.sort_unstable();
                if filter_messages_first {
                    let messages = decode_structural_messages_with_embedded_index_and_templates(
                        structural,
                        &batch,
                        &cached.embedded_index,
                        &cached.templates,
                    )?;
                    let mut filtered_batch = Vec::with_capacity(messages.len());
                    let mut filtered_messages = Vec::with_capacity(messages.len());
                    for (ordinal, message) in batch.into_iter().zip(messages) {
                        if query.message_candidate_matches(&message).unwrap_or(false) {
                            filtered_batch.push(ordinal);
                            filtered_messages.push(message);
                        }
                    }
                    batch = filtered_batch;
                    selected_messages = Some(filtered_messages);
                }
                let message_predicate_checked = filter_messages_first && message_only_query;
                if !batch.is_empty() {
                    matches.extend(
                        self.decode_decompressed_frame_candidates(
                            query,
                            append,
                            frame,
                            structural,
                            &cached.embedded_index,
                            &cached.templates,
                            &cached.offsets,
                            &cached.timestamps,
                            &cached.attribute_tables,
                            &cached,
                            &batch,
                            include_typed_metadata,
                            decode_fields,
                            selected_messages.as_deref(),
                            typed_metadata
                                .as_ref()
                                .map(|metadata| metadata.packed.as_ref()),
                            candidates_are_exact,
                            message_predicate_checked,
                        )?,
                    );
                }
                consumed = end;
            }
            sort_and_limit_matches(&mut matches, query, limit);
            return Ok(matches);
        }
        let mut matches = Vec::with_capacity(
            candidates
                .len()
                .min(query.limit.unwrap_or(candidates.len())),
        );
        let cached_messages = cached.cached_messages(&candidates);
        let cache_miss = cached_messages.is_none();
        let decode_all_fields = include_typed_metadata || decode_fields;
        let cached_fields = decode_all_fields
            .then(|| cached.cached_fields(&candidates))
            .flatten();
        let field_cache_miss = decode_all_fields && cached_fields.is_none();
        let decoded = decode_structural_records_with_cached_frame_data_and_fields(
            &cached.structural,
            &candidates,
            &cached.embedded_index,
            &cached.templates,
            &cached.offsets,
            &cached.timestamps,
            include_typed_metadata,
            decode_all_fields,
            cached_messages.as_deref(),
            typed_metadata
                .as_ref()
                .map(|metadata| metadata.packed.as_ref()),
            Some(&cached.attribute_tables),
            cached_fields.as_deref(),
        )?;
        cached.cache_messages(&candidates, &decoded);
        if decode_all_fields {
            cached.cache_fields(&candidates, &decoded);
        }
        if cache_miss || field_cache_miss {
            self.indexed_frame_query_cache
                .lock()
                .expect("indexed frame query cache lock is not poisoned")
                .enforce_budget();
        }
        for decoded in decoded {
            self.push_decoded_frame_match(
                query,
                append,
                frame,
                decoded,
                &mut matches,
                candidates_are_exact,
                message_predicate_checked,
            )?;
        }
        Ok(matches)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn decode_decompressed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        structural: &[u8],
        embedded_index: &EmbeddedFrameIndex,
        templates: &[Vec<Vec<u8>>],
        offsets: &[LogicalOffset],
        timestamps: &[u64],
        attribute_tables: &DecodedAttributeTables,
        cached_frame: &CachedIndexedFrame,
        candidates: &[u32],
        include_typed_metadata: bool,
        include_fields: bool,
        cached_messages: Option<&[Arc<str>]>,
        typed_metadata: Option<&PackedLogMetadata>,
        candidates_are_exact: bool,
        message_predicate_checked: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let mut matches = Vec::with_capacity(
            candidates
                .len()
                .min(query.limit.unwrap_or(candidates.len())),
        );
        let supplied_messages = cached_messages.is_some();
        let owned_cached_messages = if supplied_messages {
            None
        } else {
            cached_frame.cached_messages(candidates)
        };
        let cached_messages = cached_messages.or(owned_cached_messages.as_deref());
        let cache_miss = !supplied_messages && cached_messages.is_none();
        let decode_all_fields =
            include_typed_metadata || include_fields || !message_predicate_checked;
        let cached_fields = decode_all_fields
            .then(|| cached_frame.cached_fields(candidates))
            .flatten();
        let field_cache_miss = decode_all_fields && cached_fields.is_none();
        let decoded = decode_structural_records_with_cached_frame_data_and_fields(
            structural,
            candidates,
            embedded_index,
            templates,
            offsets,
            timestamps,
            include_typed_metadata,
            decode_all_fields,
            cached_messages,
            typed_metadata,
            Some(attribute_tables),
            cached_fields.as_deref(),
        )?;
        cached_frame.cache_messages(candidates, &decoded);
        if decode_all_fields {
            cached_frame.cache_fields(candidates, &decoded);
        }
        if cache_miss || field_cache_miss {
            self.indexed_frame_query_cache
                .lock()
                .expect("indexed frame query cache lock is not poisoned")
                .enforce_budget();
        }
        for decoded in decoded {
            self.push_decoded_frame_match(
                query,
                append,
                frame,
                decoded,
                &mut matches,
                candidates_are_exact,
                message_predicate_checked,
            )?;
        }
        Ok(matches)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn push_decoded_frame_match(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        decoded: crate::DecodedStructuralRecord,
        matches: &mut Vec<LogMatch>,
        candidates_are_exact: bool,
        message_predicate_checked: bool,
    ) -> TelemetryResult<()> {
        let relative_offset = decoded.offset.get();
        if relative_offset >= u64::from(append.record_count) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "compressed ingest record ordinal is out of range",
            ));
        }
        let absolute_offset = append
            .first_offset
            .get()
            .checked_add(relative_offset)
            .map(LogicalOffset::new)
            .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
        let severity_text = if decoded.severity_text.is_empty() {
            decoded
                .fields
                .iter()
                .find(|field| {
                    matches!(
                        field.key.as_ref(),
                        "otel.severity_text" | "attr.loki.metadata.severity_text"
                    )
                })
                .map(|field| Arc::clone(&field.value))
                .unwrap_or(decoded.severity_text)
        } else {
            decoded.severity_text
        };
        let record = DurableLog {
            stream_shard_id: self.stream_shard_id,
            record_ref: TelemetryRecordRef::new(query.topic_partition, absolute_offset),
            timestamp_unix_nanos: decoded.timestamp_unix_nanos,
            observed_timestamp_unix_nanos: decoded.observed_timestamp_unix_nanos,
            body: decoded.body,
            message: decoded.message,
            fields: decoded.fields,
            attributes: decoded.attributes,
            resource: decoded.resource,
            scope: decoded.scope,
            severity_number: decoded.severity_number,
            severity_text,
            dropped_attributes_count: decoded.dropped_attributes_count,
            flags: decoded.flags,
            trace_id: decoded.trace_id,
            span_id: decoded.span_id,
            event_name: decoded.event_name,
            compression_cohort: frame.cohort,
        };
        let tenant_exact_fields_only = query
            .exact_fields
            .iter()
            .all(|field| field.key.as_ref() == "resource.loki.tenant");
        if candidates_are_exact
            || message_predicate_checked
            || (tenant_exact_fields_only && !query.has_residual_predicate())
        {
            if query.matches_index_bounds(&record) {
                matches.push(LogMatch { record });
            }
        } else if query.matches(&record) {
            matches.push(LogMatch { record });
        }
        Ok(())
    }
}
