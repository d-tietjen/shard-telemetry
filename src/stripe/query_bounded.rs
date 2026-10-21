//! Native log traversal retains one frame and an admitted ordered result set.

use super::*;
use crate::NativeQueryWorkLimits;
use crate::query_work::LogQueryWorkBudget;
use std::cmp::Ordering;

struct LogWinner<'a> {
    query: &'a LogQuery,
    matched: LogMatch,
}

impl PartialEq for LogWinner<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for LogWinner<'_> {}
impl PartialOrd for LogWinner<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for LogWinner<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.query
            .compare(&self.matched.record, &other.matched.record)
    }
}

struct LogWinners<'a> {
    query: &'a LogQuery,
    work: NativeQueryWorkLimits,
    budget: LogQueryWorkBudget,
    heap: BinaryHeap<LogWinner<'a>>,
}

impl<'a> LogWinners<'a> {
    fn new(query: &'a LogQuery, work: NativeQueryWorkLimits) -> Self {
        Self {
            query,
            work,
            budget: LogQueryWorkBudget::new(Some(work)),
            heap: BinaryHeap::new(),
        }
    }

    fn offer(&mut self, matched: LogMatch) -> TelemetryResult<()> {
        if self.query.limit == Some(0) {
            return Ok(());
        }
        let replacing = self
            .query
            .limit
            .is_some_and(|limit| self.heap.len() >= limit);
        if replacing
            && self.heap.peek().is_some_and(|worst| {
                self.query.compare(&matched.record, &worst.matched.record) != Ordering::Less
            })
        {
            return Ok(());
        }
        self.work
            .winners(self.heap.len().saturating_add(usize::from(!replacing)))?;
        self.budget.replace(
            replacing
                .then(|| self.heap.peek().map(|winner| &winner.matched.record))
                .flatten(),
            &matched.record,
            size_of::<&LogQuery>(),
        )?;
        if replacing {
            self.heap.pop();
        }
        self.heap.push(LogWinner {
            query: self.query,
            matched,
        });
        Ok(())
    }

    fn finish(self) -> Vec<LogMatch> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|winner| winner.matched)
            .collect()
    }
}

pub(super) fn check_log_decoded_bytes(
    work: NativeQueryWorkLimits,
    bytes: usize,
    resource: &str,
) -> TelemetryResult<()> {
    if bytes > work.max_decoded_sidecar_bytes {
        return Err(TelemetryError::InvalidQuery(format!(
            "native query capacity exceeded for {resource}: {bytes} > {}",
            work.max_decoded_sidecar_bytes,
        )));
    }
    Ok(())
}

impl LogStripe {
    /// Installs bounded traversal and clears retained frame/posting caches when enabled.
    pub(crate) fn set_query_work_limits(&mut self, work: Option<NativeQueryWorkLimits>) {
        self.native_query_work = work;
        if work.is_some() {
            // A bounded query must not keep historical parsed frames/postings alive.
            self.indexed_frame_query_cache = Mutex::new(IndexedFrameQueryCache::default());
            self.exact_posting_cache = Mutex::new(ExactPostingCache::default());
            self.active_partition_cache = HashMap::new();
        }
    }

    pub(super) fn query_bounded(
        &self,
        query: &LogQuery,
        typed: bool,
        fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let work = self.native_query_work.ok_or(TelemetryError::InvalidConfig(
            "missing native log query limits",
        ))?;
        let mut winners = LogWinners::new(query, work);
        self.visit_bounded_log_matches(query, typed, fields, &mut winners)?;
        Ok(winners.finish())
    }

    pub(super) fn query_partitions_bounded(
        &self,
        queries: &[LogQuery],
        typed: bool,
        fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let Some(ordering) = queries.first() else {
            return Ok(Vec::new());
        };
        let work = self.native_query_work.ok_or(TelemetryError::InvalidConfig(
            "missing native log query limits",
        ))?;
        if self.tier.is_some()
            || (ordering.limit.is_some()
                && ordering.sort == crate::QuerySort::Timestamp
                && queries
                    .iter()
                    .all(|query| same_query_across_partition(ordering, query)))
        {
            if ordering.limit == Some(0) {
                return Ok(Vec::new());
            }
            let mut winners = LogWinners::new(ordering, work);
            for query in queries {
                self.visit_bounded_log_matches(query, typed, fields, &mut winners)?;
            }
            return Ok(winners.finish());
        }
        // Preserve the unmerged per-partition shape of the checked legacy API,
        // admitting the complete collection rather than silently dropping rows.
        let mut matches = Vec::new();
        let mut budget = LogQueryWorkBudget::new(Some(work));
        for query in queries {
            for matched in self.query_bounded(query, typed, fields)? {
                work.winners(matches.len().saturating_add(1))?;
                budget.replace(None, &matched.record, 0)?;
                matches.push(matched);
            }
        }
        Ok(matches)
    }

    fn visit_bounded_log_matches(
        &self,
        query: &LogQuery,
        typed: bool,
        fields: bool,
        winners: &mut LogWinners<'_>,
    ) -> TelemetryResult<()> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(());
        }
        if let Some(partition) = self.partitions.get(&query.topic_partition) {
            for indexed in &partition.records {
                let original = &indexed.record;
                if !query.matches_index_bounds(original) {
                    continue;
                }
                if query.exact_fields.iter().any(|field| {
                    field.key.as_ref() == "resource.loki.tenant"
                        && !original.fields.iter().any(|observed| observed == field)
                }) {
                    continue;
                }
                check_log_decoded_bytes(
                    winners.work,
                    original.message.len(),
                    "decoded log body bytes",
                )?;
                if !query.matches(original) {
                    continue;
                }
                let record = if typed {
                    original.clone()
                } else {
                    let mut projected = DurableLog::new_projected(
                        original.stream_shard_id,
                        original.record_ref.topic_partition,
                        original.record_ref.offset,
                        original.timestamp_unix_nanos,
                        Arc::clone(&original.message),
                        original.compression_cohort,
                    );
                    projected.severity_text = Arc::clone(&original.severity_text);
                    if fields {
                        projected.fields = Arc::clone(&original.fields);
                    }
                    projected
                };
                winners.offer(LogMatch { record })?;
            }
        }
        if let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) {
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append)
                    || !log_append_tenant_matches(query, &append.tenant)
                {
                    continue;
                }
                for frame in &append.frames {
                    if frame_matches_query_bounds(query, frame) {
                        self.visit_bounded_log_frame(query, append, frame, typed, fields, winners)?;
                    }
                }
            }
        }
        let Some(state) = &self.tier else {
            return Ok(());
        };
        let Some(tier) = state.tiers.get(&query.topic_partition) else {
            return Ok(());
        };
        tier.for_each_query_group_cached(
            TierQueryRange {
                first_offset: query.start_offset.map(LogicalOffset::get),
                last_offset: query.end_offset.map(LogicalOffset::get),
                min_timestamp_unix_nanos: query.start_timestamp_unix_nanos,
                max_timestamp_unix_nanos: query.end_timestamp_unix_nanos,
                signal_identity: None,
            },
            &state.control_cache,
            None,
            Some(winners.work),
            |group| {
                let manifest = tier.load_group_cached(group, &state.control_cache)?;
                let index = manifest
                    .artifact(TierArtifactKind::QueryIndex)
                    .ok_or_else(|| {
                        TelemetryError::CorruptTier("group has no query index".into())
                    })?;
                winners.work.control(index.bytes)?;
                let encoded = tier.read_artifact_cached(
                    index,
                    winners.work.max_control_object_bytes as u64,
                    &state.control_cache,
                )?;
                let appends = crate::tier_ingest::decode_tier_ingest_group_with_work_limits(
                    &encoded,
                    &manifest.blocks,
                    Some(winners.work),
                )?;
                let payload = manifest
                    .artifact(TierArtifactKind::PayloadPack)
                    .ok_or_else(|| {
                        TelemetryError::CorruptTier("group has no payload pack".into())
                    })?;
                let metadata = ObjectMetadata {
                    bytes: payload.bytes,
                    version_token: payload.checksum.clone(),
                    content_digest: payload.checksum.clone(),
                };
                for append in appends {
                    let bounds = IndexedFrameAppend {
                        tenant: Arc::from(append.tenant),
                        first_offset: append.first_offset,
                        last_offset: append.last_offset,
                        record_count: append.record_count,
                        frames: Vec::new(),
                        next_checkpoint: None,
                    };
                    if !append_matches_query_bounds(query, &bounds)
                        || !log_append_tenant_matches(query, &bounds.tenant)
                    {
                        continue;
                    }
                    for cold in append.frames {
                        if !timestamp_bounds_overlap(
                            query,
                            cold.min_timestamp_unix_nanos,
                            cold.max_timestamp_unix_nanos,
                        ) {
                            continue;
                        }
                        winners.work.encoded(
                            usize::try_from(cold.payload_bytes)
                                .map_err(|_| TelemetryError::RecordTooLarge)?,
                        )?;
                        winners.work.rows(cold.record_count as usize)?;
                        check_log_decoded_bytes(
                            winners.work,
                            cold.structural_bytes,
                            "decoded log frame bytes",
                        )?;
                        let end = cold
                            .payload_offset
                            .checked_add(cold.payload_bytes)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        let bytes = state
                            .payload_cache
                            .read_ranges_with_metadata(
                                tier.object_store(),
                                &payload.object_key,
                                &metadata,
                                std::slice::from_ref(&(cold.payload_offset..end)),
                            )?
                            .into_iter()
                            .next()
                            .ok_or_else(|| {
                                TelemetryError::CorruptTier(
                                    "tiered frame payload is missing".into(),
                                )
                            })?;
                        if blake3::hash(&bytes).to_hex().as_str() != cold.payload_checksum {
                            return Err(TelemetryError::CorruptTier(
                                "tiered frame payload checksum failed".into(),
                            ));
                        }
                        let frame = IndexedIngestFrame {
                            frame_id: cold.frame_id,
                            cohort: cold.cohort,
                            record_count: cold.record_count,
                            structural_bytes: cold.structural_bytes,
                            min_timestamp_unix_nanos: cold.min_timestamp_unix_nanos,
                            max_timestamp_unix_nanos: cold.max_timestamp_unix_nanos,
                            compressed: Bytes::from(bytes),
                            index: cold.index,
                        };
                        self.visit_bounded_log_frame(
                            query, &bounds, &frame, typed, fields, winners,
                        )?;
                    }
                }
                Ok(())
            },
        )
    }

    fn visit_bounded_log_frame(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        typed: bool,
        fields: bool,
        winners: &mut LogWinners<'_>,
    ) -> TelemetryResult<()> {
        let cached = self.cached_indexed_frame(frame)?;
        let metadata = typed
            .then(|| self.cached_typed_metadata(&cached, frame.record_count))
            .transpose()?;
        for (ordinal, relative) in cached.offsets.iter().enumerate() {
            if relative.get() >= u64::from(append.record_count) {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "compressed ingest record ordinal is out of range",
                ));
            }
            let offset = append
                .first_offset
                .get()
                .checked_add(relative.get())
                .map(LogicalOffset::new)
                .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
            let position = DurableLog::new_projected(
                self.stream_shard_id,
                query.topic_partition,
                offset,
                cached.timestamps[ordinal],
                Arc::from(""),
                frame.cohort,
            );
            if !query.matches_index_bounds(&position) {
                continue;
            }
            // A structural template plus one row's variable bytes cannot exceed
            // the admitted frame. Project one ordinal so expanded messages never
            // accumulate for the whole frame before applying predicates/top-K.
            let ordinal = u32::try_from(ordinal).map_err(|_| TelemetryError::RecordTooLarge)?;
            for mut matched in self.decode_decompressed_frame_candidates(
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
                std::slice::from_ref(&ordinal),
                typed,
                fields || !query.exact_fields.is_empty() || query.has_residual_predicate(),
                None,
                metadata.as_ref().map(|metadata| metadata.packed.as_ref()),
                false,
                false,
            )? {
                // The shared decoder normally receives an index-selected set;
                // this traversal offers every ordinal and must check all terms.
                if query.matches(&matched.record) {
                    if !typed && !fields {
                        matched.record.fields = Arc::new(Vec::new());
                    }
                    winners.offer(matched)?;
                }
            }
        }
        Ok(())
    }
}

fn log_append_tenant_matches(query: &LogQuery, tenant: &str) -> bool {
    query
        .exact_fields
        .iter()
        .all(|field| field.key.as_ref() != "resource.loki.tenant" || field.value.as_ref() == tenant)
}
