use super::*;

#[cfg(test)]
thread_local! {
    pub(super) static TIER_PAGE_FALLBACK_CANDIDATES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl LogStripe {
    pub(super) fn query_tiered_groups(
        &self,
        query: &LogQuery,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let Some(state) = &self.tier else {
            return Ok(Vec::new());
        };
        let Some(tier) = state.tiers.get(&query.topic_partition) else {
            return Ok(Vec::new());
        };
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
        let mut groups = tier.candidate_groups_cached(
            TierQueryRange {
                first_offset: query.start_offset.map(LogicalOffset::get),
                last_offset: query.end_offset.map(LogicalOffset::get),
                min_timestamp_unix_nanos: query.start_timestamp_unix_nanos,
                max_timestamp_unix_nanos: query.end_timestamp_unix_nanos,
                signal_identity: None,
            },
            &state.control_cache,
        )?;
        match query.order {
            QueryOrder::NewestFirst => groups.sort_unstable_by(|left, right| {
                right
                    .max_timestamp_unix_nanos
                    .cmp(&left.max_timestamp_unix_nanos)
            }),
            QueryOrder::OldestFirst => groups.sort_unstable_by(|left, right| {
                left.min_timestamp_unix_nanos
                    .cmp(&right.min_timestamp_unix_nanos)
            }),
        }
        let mut matches: Vec<LogMatch> = Vec::new();
        for group in groups {
            if query.sort == crate::QuerySort::Timestamp
                && let Some(limit) = query.limit
                && matches.len() >= limit
            {
                let boundary = matches
                    .last()
                    .expect("a full tier result page has a boundary")
                    .record
                    .timestamp_unix_nanos;
                let cannot_improve = match query.order {
                    QueryOrder::NewestFirst => group.max_timestamp_unix_nanos < boundary,
                    QueryOrder::OldestFirst => group.min_timestamp_unix_nanos > boundary,
                };
                if cannot_improve {
                    break;
                }
            }
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
            let page_head = query.sort == crate::QuerySort::Timestamp && query.limit == Some(1);
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
                    if !timestamp_bounds_overlap(
                        query,
                        cold_frame.min_timestamp_unix_nanos,
                        cold_frame.max_timestamp_unix_nanos,
                    ) {
                        continue;
                    }
                    if page_head {
                        // Sort only frame descriptors. Building fallback
                        // candidates here can collect every ordinal in a
                        // large group before the first page head is known.
                        selected.push((
                            Arc::clone(&bounds.tenant),
                            bounds.first_offset,
                            bounds.last_offset,
                            bounds.record_count,
                            cold_frame,
                            None,
                            None,
                            false,
                        ));
                        continue;
                    }
                    let cached_exact_candidates = exact_tokens
                        .as_deref()
                        .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                        .and_then(|tokens| {
                            self.cached_exact_frame_candidates(
                                cold_frame.frame_id,
                                tokens,
                                &exact_fields,
                            )
                        });
                    let exact_candidates_cached = cached_exact_candidates.is_some();
                    let cached_message_candidates = (query.terms.is_empty()
                        && exact_fields.is_empty())
                    .then(|| {
                        self.cached_message_predicate_candidates_if_present(
                            cold_frame.frame_id,
                            &query.predicate,
                        )
                    })
                    .flatten();
                    let candidates = cached_exact_candidates
                        .or(cached_message_candidates)
                        .unwrap_or_else(|| {
                            indexed_frame_candidates_for_append(
                                query,
                                &cold_frame.index,
                                cold_frame.record_count,
                                bounds.tenant.as_ref(),
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
                        cold_frame,
                        Some(candidates),
                        range_index,
                        exact_candidates_cached,
                    ));
                }
            }
            if query.sort == crate::QuerySort::Timestamp && query.limit.is_some() {
                selected.sort_unstable_by(|left, right| match query.order {
                    QueryOrder::NewestFirst => right
                        .4
                        .max_timestamp_unix_nanos
                        .cmp(&left.4.max_timestamp_unix_nanos),
                    QueryOrder::OldestFirst => left
                        .4
                        .min_timestamp_unix_nanos
                        .cmp(&right.4.min_timestamp_unix_nanos),
                });
            }
            let mut payloads = if ranges.is_empty() || page_head {
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
                exact_candidates_cached,
            ) in selected
            {
                if query.sort == crate::QuerySort::Timestamp
                    && let Some(limit) = query.limit
                    && matches.len() >= limit
                {
                    let boundary = matches
                        .last()
                        .expect("a full tier result page has a boundary")
                        .record
                        .timestamp_unix_nanos;
                    let cannot_improve = match query.order {
                        QueryOrder::NewestFirst => cold_frame.max_timestamp_unix_nanos < boundary,
                        QueryOrder::OldestFirst => cold_frame.min_timestamp_unix_nanos > boundary,
                    };
                    if cannot_improve {
                        break;
                    }
                }
                let mut exact_candidates_cached = exact_candidates_cached;
                let candidates = if let Some(candidates) = candidates {
                    candidates
                } else {
                    let cached_exact_candidates = exact_tokens
                        .as_deref()
                        .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                        .and_then(|tokens| {
                            self.cached_exact_frame_candidates(
                                cold_frame.frame_id,
                                tokens,
                                &exact_fields,
                            )
                        });
                    exact_candidates_cached = cached_exact_candidates.is_some();
                    let cached_message_candidates = (query.terms.is_empty()
                        && exact_fields.is_empty())
                    .then(|| {
                        self.cached_message_predicate_candidates_if_present(
                            cold_frame.frame_id,
                            &query.predicate,
                        )
                    })
                    .flatten();
                    let candidates = cached_exact_candidates
                        .or(cached_message_candidates)
                        .unwrap_or_else(|| {
                            #[cfg(test)]
                            TIER_PAGE_FALLBACK_CANDIDATES
                                .with(|count| count.set(count.get().saturating_add(1)));
                            indexed_frame_candidates_for_append(
                                query,
                                &cold_frame.index,
                                cold_frame.record_count,
                                tenant.as_ref(),
                            )
                        });
                    if candidates.is_empty() {
                        continue;
                    }
                    candidates
                };
                let range_index = if page_head
                    && self
                        .cached_indexed_frame_if_present(cold_frame.frame_id)
                        .is_none()
                {
                    let range_end = cold_frame
                        .payload_offset
                        .checked_add(cold_frame.payload_bytes)
                        .ok_or(TelemetryError::RecordTooLarge)?;
                    let index = ranges.len();
                    ranges.push(cold_frame.payload_offset..range_end);
                    Some(index)
                } else {
                    range_index
                };
                let compressed = if let Some(index) = range_index {
                    if page_head {
                        let range = ranges.get(index).ok_or_else(|| {
                            TelemetryError::CorruptTier(
                                "tiered frame payload range is missing".into(),
                            )
                        })?;
                        let payload = state.payload_cache.read_ranges_with_metadata(
                            tier.object_store(),
                            &payload_artifact.object_key,
                            &payload_metadata,
                            std::slice::from_ref(range),
                        )?;
                        Bytes::from(payload.into_iter().next().ok_or_else(|| {
                            TelemetryError::CorruptTier("tiered frame payload is missing".into())
                        })?)
                    } else {
                        Bytes::from(std::mem::take(&mut payloads[index]))
                    }
                } else {
                    Bytes::new()
                };
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
                    index: cold_frame.index.clone(),
                };
                let bounds = IndexedFrameAppend {
                    tenant,
                    first_offset,
                    last_offset,
                    record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                let candidates = if exact_candidates_cached {
                    candidates
                } else if let Some(tokens) = exact_tokens
                    .as_deref()
                    .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                {
                    self.exact_indexed_frame_candidates(
                        query,
                        &bounds,
                        &frame,
                        tokens,
                        &exact_fields,
                    )?
                    .unwrap_or(candidates)
                } else {
                    candidates
                };
                matches.extend(self.decode_indexed_frame_candidates(
                    query,
                    &bounds,
                    &frame,
                    candidates,
                    include_typed_metadata,
                    include_fields,
                    exact_candidates_are_exact,
                )?);
                if query.sort == crate::QuerySort::Timestamp
                    && let Some(limit) = query.limit
                {
                    sort_and_limit_matches(&mut matches, query, limit);
                }
            }
        }
        Ok(matches)
    }

    pub(super) fn query_tiered_groups_messages(
        &self,
        query: &LogQuery,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<LogMessageMatch>> {
        let Some(state) = &self.tier else {
            return Ok(Vec::new());
        };
        let Some(tier) = state.tiers.get(&query.topic_partition) else {
            return Ok(Vec::new());
        };
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let mut matches = Vec::new();
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
            let mut ranges: Vec<std::ops::Range<u64>> = Vec::new();
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
                    let message_cache_can_supply_the_base =
                        query.exact_message_token_conjunction().is_none()
                            && message_cache_can_supply_frame_base(query, bounds.tenant.as_ref());
                    let cached_exact_candidates = exact_tokens
                        .as_deref()
                        .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                        .and_then(|tokens| {
                            self.cached_exact_frame_candidates(
                                cold_frame.frame_id,
                                tokens,
                                &exact_fields,
                            )
                        });
                    let exact_candidates_cached = cached_exact_candidates.is_some();
                    let candidates = cached_exact_candidates.unwrap_or_else(|| {
                        if message_cache_can_supply_the_base {
                            Vec::new()
                        } else {
                            indexed_frame_candidates_for_append(
                                query,
                                &cold_frame.index,
                                cold_frame.record_count,
                                bounds.tenant.as_ref(),
                            )
                        }
                    });
                    if (!message_cache_can_supply_the_base && candidates.is_empty())
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
                        cold_frame,
                        candidates,
                        range_index,
                        exact_candidates_cached,
                        message_cache_can_supply_the_base,
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
                exact_candidates_cached,
                message_cache_can_supply_the_base,
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
                    index: cold_frame.index.clone(),
                };
                let bounds = IndexedFrameAppend {
                    tenant,
                    first_offset,
                    last_offset,
                    record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                let candidates = if message_cache_can_supply_the_base || exact_candidates_cached {
                    candidates
                } else if let Some(tokens) = exact_tokens
                    .as_deref()
                    .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                {
                    self.exact_indexed_frame_candidates(
                        query,
                        &bounds,
                        &frame,
                        tokens,
                        &exact_fields,
                    )?
                    .unwrap_or(candidates)
                } else {
                    candidates
                };
                let frame_matches = self.decode_indexed_frame_messages(
                    query,
                    &bounds,
                    &frame,
                    &candidates,
                    message_predicate_key,
                )?;
                matches.reserve(frame_matches.len());
                matches.extend(frame_matches);
            }
        }
        Ok(matches)
    }

    pub(super) fn query_tiered_groups_trace_ids(
        &self,
        query: &LogQuery,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<TraceId>> {
        let Some(state) = &self.tier else {
            return Ok(Vec::new());
        };
        let Some(tier) = state.tiers.get(&query.topic_partition) else {
            return Ok(Vec::new());
        };
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
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
        let mut trace_ids = Vec::new();
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
            let mut ranges: Vec<std::ops::Range<u64>> = Vec::new();
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
                    let cached_exact_candidates = exact_tokens
                        .as_deref()
                        .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                        .and_then(|tokens| {
                            self.cached_exact_frame_candidates(
                                cold_frame.frame_id,
                                tokens,
                                &exact_fields,
                            )
                        });
                    let exact_candidates_cached = cached_exact_candidates.is_some();
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
                        cold_frame,
                        candidates,
                        range_index,
                        exact_candidates_cached,
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
                exact_candidates_cached,
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
                    index: cold_frame.index.clone(),
                };
                let bounds = IndexedFrameAppend {
                    tenant,
                    first_offset,
                    last_offset,
                    record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                let candidates = if exact_candidates_cached {
                    candidates
                } else if let Some(tokens) = exact_tokens
                    .as_deref()
                    .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                {
                    self.exact_indexed_frame_candidates(
                        query,
                        &bounds,
                        &frame,
                        tokens,
                        &exact_fields,
                    )?
                    .unwrap_or(candidates)
                } else {
                    candidates
                };
                trace_ids.extend(self.decode_indexed_frame_trace_ids(
                    query,
                    &bounds,
                    &frame,
                    &candidates,
                    message_predicate_key,
                )?);
            }
        }
        Ok(trace_ids)
    }
}
