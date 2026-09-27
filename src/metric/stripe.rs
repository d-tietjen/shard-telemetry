use super::*;

impl MetricStripe {
    /// Creates a bounded metric stripe using production chunk and OOO limits.
    pub fn new(head_budget_bytes: usize) -> TelemetryResult<Self> {
        if head_budget_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "metric head budget must be nonzero",
            ));
        }
        Ok(Self {
            head_budget_bytes,
            head_bytes: 0,
            out_of_order_nanos: DEFAULT_OUT_OF_ORDER_NANOS,
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            chunk_points: DEFAULT_CHUNK_POINTS,
            chunk_nanos: DEFAULT_CHUNK_NANOS,
            series: HashMap::new(),
            chunks: HashMap::new(),
            pending_chunks: Vec::new(),
            next_chunk_id: 1,
            recovered_accumulators: HashMap::new(),
            name_index: HashMap::new(),
            label_index: HashMap::new(),
            identity_fingerprints: std::iter::repeat_with(|| None)
                .take(SERIES_ID_CACHE_ENTRIES)
                .collect(),
            decoded_chunks: RefCell::new(DecodedMetricCache::new(
                head_budget_bytes
                    .saturating_div(8)
                    .min(MAX_DECODED_METRIC_CACHE_BYTES),
            )),
        })
    }

    /// Applies one raw metric point under OTLP or Remote Write conflict rules.
    pub fn apply(
        &mut self,
        point: DurableMetricPoint,
        protocol: MetricIngestProtocol,
    ) -> TelemetryResult<MetricApplyOutcome> {
        self.apply_ref(&point, protocol)
    }

    /// Applies a borrowed durable metric point, cloning it only when it is
    /// accepted into the mutable series head. This avoids copying duplicate
    /// and obsolete retries on the durable sink path.
    pub fn apply_ref(
        &mut self,
        point: &DurableMetricPoint,
        protocol: MetricIngestProtocol,
    ) -> TelemetryResult<MetricApplyOutcome> {
        if point.record_ref.signal != TelemetrySignal::Metrics {
            return Err(TelemetryError::InvalidBlockEncoding(
                "non-metric record applied to metric stripe",
            ));
        }
        let fingerprint = self.series_fingerprint(&point.identity);
        if self.series.get(&fingerprint).is_some_and(|head| {
            head.identity.as_ref() != point.identity.as_ref()
                || head.topic_partition != point.record_ref.topic_partition
        }) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "series fingerprint collision",
            ));
        }
        if !self.series.contains_key(&fingerprint) {
            self.name_index
                .entry(Arc::clone(&point.identity.name))
                .or_default()
                .insert(fingerprint);
            for (name, value) in prometheus_string_labels(&point.identity) {
                self.label_index
                    .entry((name, value))
                    .or_default()
                    .insert(fingerprint);
            }
        }
        let estimated = point.estimated_head_bytes();
        if estimated > self.head_budget_bytes {
            return Err(TelemetryError::RecordTooLarge);
        }
        if self.head_bytes.saturating_add(estimated) > self.head_budget_bytes {
            self.seal_largest_head()?;
        }
        if self.head_bytes.saturating_add(estimated) > self.head_budget_bytes {
            return Err(TelemetryError::InvalidConfig(
                "metric head memory budget exhausted",
            ));
        }
        let recovered = self.recovered_accumulators.get(&fingerprint).cloned();
        if recovered.as_ref().is_some_and(|checkpoint| {
            checkpoint.topic_partition != point.record_ref.topic_partition
        }) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "recovered metric accumulator belongs to another partition",
            ));
        }
        self.recovered_accumulators.remove(&fingerprint);
        // Keep decoded sealed chunks alive across the mutable head update, but
        // avoid cloning every matching point just to inspect conflict metadata.
        let sealed_same_timestamp =
            self.sealed_chunks_at(fingerprint, point.timestamp_unix_nanos)?;
        let (outcome, should_seal) = {
            let head = self
                .series
                .entry(fingerprint)
                .or_insert_with(|| SeriesHead {
                    topic_partition: point.record_ref.topic_partition,
                    identity: Arc::clone(&point.identity),
                    points: BTreeMap::new(),
                    latest_timestamp: recovered
                        .as_ref()
                        .map_or(point.timestamp_unix_nanos, |value| {
                            value.latest_timestamp_unix_nanos
                        }),
                    bytes: 0,
                    reset_generation: recovered.as_ref().map_or(0, |value| value.reset_generation),
                    cumulative: recovered.as_ref().and_then(|value| value.cumulative),
                    conflicts: 0,
                });
            if point
                .timestamp_unix_nanos
                .saturating_add(self.out_of_order_nanos)
                < head.latest_timestamp
            {
                return Err(TelemetryError::InvalidMetricSample(
                    "metric point exceeds the 10 minute out-of-order window".into(),
                ));
            }
            let mut head_timestamp_keys = head
                .points
                .range(
                    (point.timestamp_unix_nanos, LogicalOffset::new(0))
                        ..=(point.timestamp_unix_nanos, LogicalOffset::new(u64::MAX)),
                )
                .map(|(key, _)| *key)
                .collect::<Vec<_>>();
            let mut winner_offset: Option<LogicalOffset> = None;
            let mut duplicate = false;
            for key in &head_timestamp_keys {
                let existing = head
                    .points
                    .get(key)
                    .expect("metric timestamp key is present");
                duplicate |= same_metric_sample_payload(existing, point);
                winner_offset = Some(match winner_offset {
                    Some(winner) => winner.max(existing.record_ref.offset),
                    None => existing.record_ref.offset,
                });
            }
            for decoded in &sealed_same_timestamp {
                for existing in decoded
                    .iter()
                    .filter(|existing| existing.timestamp_unix_nanos == point.timestamp_unix_nanos)
                {
                    duplicate |= same_metric_sample_payload(existing, point);
                    winner_offset = Some(match winner_offset {
                        Some(winner) => winner.max(existing.record_ref.offset),
                        None => existing.record_ref.offset,
                    });
                }
            }
            if duplicate {
                return Ok(MetricApplyOutcome::Duplicate);
            }
            if let Some(winner_offset) = winner_offset {
                if protocol == MetricIngestProtocol::RemoteWrite {
                    return Err(TelemetryError::MetricSampleConflict {
                        series: fingerprint.get(),
                        timestamp_unix_nanos: point.timestamp_unix_nanos,
                    });
                }
                head.conflicts = head.conflicts.saturating_add(1);
                if winner_offset >= point.record_ref.offset {
                    return Ok(MetricApplyOutcome::Obsolete);
                }
                for key in head_timestamp_keys.drain(..) {
                    if let Some(removed) = head.points.remove(&key) {
                        let bytes = removed.estimated_head_bytes();
                        head.bytes = head.bytes.saturating_sub(bytes);
                        self.head_bytes = self.head_bytes.saturating_sub(bytes);
                    }
                }
            }
            let out_of_order = point.timestamp_unix_nanos < head.latest_timestamp;
            let outcome = if winner_offset.is_none() {
                if out_of_order {
                    MetricApplyOutcome::OutOfOrder
                } else {
                    MetricApplyOutcome::Inserted
                }
            } else {
                MetricApplyOutcome::Replaced
            };
            head.latest_timestamp = head.latest_timestamp.max(point.timestamp_unix_nanos);
            update_accumulator(head, point);
            head.bytes = head.bytes.saturating_add(estimated);
            self.head_bytes = self.head_bytes.saturating_add(estimated);
            head.points.insert(
                (point.timestamp_unix_nanos, point.record_ref.offset),
                point.clone(),
            );
            let should_seal = head.points.len() >= self.chunk_points
                || head.bytes >= self.chunk_bytes
                || head
                    .points
                    .first_key_value()
                    .is_some_and(|((timestamp, _), _)| {
                        head.latest_timestamp.saturating_sub(*timestamp) >= self.chunk_nanos
                    });
            (outcome, should_seal)
        };
        if should_seal {
            self.seal_series(fingerprint)?;
        }
        Ok(outcome)
    }

    pub(super) fn series_fingerprint(
        &mut self,
        identity: &Arc<MetricIdentity>,
    ) -> SeriesFingerprint {
        let hash = foldhash::fast::FixedState::with_seed(0x5348_4152_444d_4554)
            .hash_one(identity.as_ref());
        let slot = hash as usize & (SERIES_ID_CACHE_ENTRIES - 1);
        if let Some(cached) = &self.identity_fingerprints[slot]
            && cached.hash == hash
            && (Arc::ptr_eq(&cached.identity, identity)
                || cached.identity.as_ref() == identity.as_ref())
        {
            return cached.fingerprint;
        }
        let fingerprint = identity.fingerprint();
        self.identity_fingerprints[slot] = Some(CachedSeriesIdentity {
            hash,
            identity: Arc::clone(identity),
            fingerprint,
        });
        fingerprint
    }

    /// Serializes exact accumulator checkpoints for restart recovery.
    pub fn accumulator_checkpoints(&self) -> TelemetryResult<Vec<u8>> {
        let mut checkpoints = self
            .recovered_accumulators
            .values()
            .cloned()
            .collect::<Vec<_>>();
        checkpoints.extend(
            self.series
                .iter()
                .map(|(series, head)| SeriesAccumulatorCheckpoint {
                    series: *series,
                    topic_partition: head.topic_partition,
                    reset_generation: head.reset_generation,
                    latest_timestamp_unix_nanos: head.latest_timestamp,
                    cumulative: head.cumulative,
                }),
        );
        checkpoints.sort_unstable_by_key(|checkpoint| checkpoint.series);
        rmp_serde::to_vec(&checkpoints)
            .map_err(|error| TelemetryError::StorageIo(error.to_string()))
    }

    pub(crate) fn accumulator_checkpoints_for_partition(
        &self,
        partition: TopicPartition,
    ) -> TelemetryResult<Vec<u8>> {
        let mut checkpoints = self
            .recovered_accumulators
            .values()
            .filter(|checkpoint| checkpoint.topic_partition == partition)
            .cloned()
            .collect::<Vec<_>>();
        checkpoints.extend(self.series.iter().filter_map(|(series, head)| {
            (head.topic_partition == partition).then_some(SeriesAccumulatorCheckpoint {
                series: *series,
                topic_partition: head.topic_partition,
                reset_generation: head.reset_generation,
                latest_timestamp_unix_nanos: head.latest_timestamp,
                cumulative: head.cumulative,
            })
        }));
        checkpoints.sort_unstable_by_key(|checkpoint| checkpoint.series);
        rmp_serde::to_vec(&checkpoints)
            .map_err(|error| TelemetryError::StorageIo(error.to_string()))
    }

    /// Restores accumulator generations before accepting new points.
    pub fn restore_accumulator_checkpoints(&mut self, encoded: &[u8]) -> TelemetryResult<()> {
        let checkpoints: Vec<SeriesAccumulatorCheckpoint> = rmp_serde::from_slice(encoded)
            .map_err(|error| TelemetryError::StorageIo(error.to_string()))?;
        for checkpoint in checkpoints {
            if checkpoint.topic_partition.topic_id != TelemetrySignal::Metrics.topic_id() {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "metric accumulator checkpoint uses the wrong signal topic",
                ));
            }
            if let Some(head) = self.series.get_mut(&checkpoint.series) {
                if head.topic_partition != checkpoint.topic_partition {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "metric accumulator checkpoint changed partitions",
                    ));
                }
                head.reset_generation = checkpoint.reset_generation;
                head.latest_timestamp = checkpoint.latest_timestamp_unix_nanos;
                head.cumulative = checkpoint.cumulative;
            } else if self
                .recovered_accumulators
                .insert(checkpoint.series, checkpoint)
                .is_some()
            {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "duplicate metric accumulator checkpoint",
                ));
            }
        }
        Ok(())
    }

    /// Returns immutable raw chunks for one series.
    #[must_use]
    pub fn chunks(&self, series: SeriesFingerprint) -> Vec<Arc<[u8]>> {
        self.chunks
            .get(&series)
            .into_iter()
            .flatten()
            .map(|chunk| Arc::clone(&chunk.payload))
            .collect()
    }

    /// Returns current head memory accounting.
    #[must_use]
    pub const fn head_bytes(&self) -> usize {
        self.head_bytes
    }

    /// Queries exact raw hot and immutable-chunk points with index pushdown.
    pub fn query(&self, query: &MetricQuery) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let limit = query.limit.max(1);
        if let Some(series) = query.series {
            return self.query_exact_series(query, series, limit);
        }
        let mut winners = BTreeMap::<(SeriesFingerprint, u64), DurableMetricPoint>::new();
        let candidates = self.query_candidates(query);
        if let Some(candidates) = candidates {
            for series in candidates {
                let Some(head) = self.series.get(&series) else {
                    continue;
                };
                if head.identity.tenant != query.tenant
                    || query
                        .name
                        .as_ref()
                        .is_some_and(|name| name.as_ref() != head.identity.name.as_ref())
                {
                    continue;
                }
                self.collect_metric_series_matches(query, series, head, &mut winners)?;
            }
        } else {
            for (series, head) in &self.series {
                if head.identity.tenant != query.tenant
                    || query
                        .name
                        .as_ref()
                        .is_some_and(|name| name.as_ref() != head.identity.name.as_ref())
                {
                    continue;
                }
                self.collect_metric_series_matches(query, *series, head, &mut winners)?;
            }
        }
        let mut points = winners
            .into_values()
            .filter(|point| metric_query_cursor_matches(query, point))
            .collect::<Vec<_>>();
        if query.partition.is_some() {
            points.sort_unstable_by_key(|point| point.record_ref.offset);
        } else {
            points.sort_unstable_by_key(|point| {
                (point.timestamp_unix_nanos, point.record_ref.offset)
            });
        }
        points.truncate(limit);
        Ok(points)
    }

    /// Queries a bounded set of exact timestamps for one series without
    /// materializing unrelated points in the surrounding time range.
    pub(crate) fn query_exact_timestamps(
        &self,
        query: &MetricQuery,
        timestamps: &[u64],
    ) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let Some(series) = query.series else {
            return Ok(Vec::new());
        };
        if timestamps.is_empty() {
            return Ok(Vec::new());
        }
        let Some(head) = self.series.get(&series) else {
            return Ok(Vec::new());
        };
        if !metric_identity_matches(query, &head.identity) {
            return Ok(Vec::new());
        }
        let mut winners = BTreeMap::<u64, DurableMetricPoint>::new();
        for chunk in self
            .chunks
            .get(&series)
            .into_iter()
            .flatten()
            .filter(|chunk| timestamps_intersect_chunk(timestamps, chunk))
        {
            let decoded = self.decode_chunk(chunk)?;
            for point in decoded.iter().filter(|point| {
                metric_exact_series_point_matches(query, point)
                    && timestamps
                        .binary_search(&point.timestamp_unix_nanos)
                        .is_ok()
            }) {
                retain_exact_metric_winner(&mut winners, point.clone());
            }
        }
        for point in head.points.values().filter(|point| {
            metric_exact_series_point_matches(query, point)
                && timestamps
                    .binary_search(&point.timestamp_unix_nanos)
                    .is_ok()
        }) {
            retain_exact_metric_winner(&mut winners, point.clone());
        }
        Ok(winners.into_values().collect())
    }

    pub(super) fn collect_metric_series_matches(
        &self,
        query: &MetricQuery,
        series: SeriesFingerprint,
        head: &SeriesHead,
        winners: &mut BTreeMap<(SeriesFingerprint, u64), DurableMetricPoint>,
    ) -> TelemetryResult<()> {
        for point in head
            .points
            .values()
            .filter(|point| metric_exact_series_point_matches(query, point))
        {
            retain_metric_winner(winners, series, point.clone());
        }
        for chunk in self.chunks.get(&series).into_iter().flatten() {
            if query
                .start_time_unix_nanos
                .is_some_and(|start| chunk.max_timestamp_unix_nanos < start)
                || query
                    .end_time_unix_nanos
                    .is_some_and(|end| chunk.min_timestamp_unix_nanos > end)
            {
                continue;
            }
            let decoded = self.decode_chunk(chunk)?;
            for point in decoded
                .iter()
                .filter(|point| metric_exact_series_point_matches(query, point))
            {
                retain_metric_winner(winners, series, point.clone());
            }
        }
        Ok(())
    }

    pub(super) fn query_exact_series(
        &self,
        query: &MetricQuery,
        series: SeriesFingerprint,
        limit: usize,
    ) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let Some(head) = self.series.get(&series) else {
            return Ok(Vec::new());
        };
        if !metric_identity_matches(query, &head.identity) {
            return Ok(Vec::new());
        }
        if query.partition.is_some() || query.start_offset.is_some() {
            return self.query_exact_series_by_offset(query, series, head, limit);
        }
        if self.exact_series_sources_are_disjoint(series, head) {
            return self.query_disjoint_exact_series(query, series, head, limit);
        }
        let mut sources = Vec::with_capacity(
            usize::from(!head.points.is_empty()) + self.chunks.get(&series).map_or(0, Vec::len),
        );
        if !head.points.is_empty() {
            sources.push(ExactMetricSource::Head(head));
        }
        sources.extend(
            self.chunks
                .get(&series)
                .into_iter()
                .flatten()
                .map(ExactMetricSource::Chunk),
        );
        sources.sort_unstable_by_key(ExactMetricSource::min_timestamp_unix_nanos);

        let mut winners = BTreeMap::<u64, DurableMetricPoint>::new();
        for source in sources {
            let min_timestamp = source.min_timestamp_unix_nanos();
            let max_timestamp = source.max_timestamp_unix_nanos();
            if query
                .start_time_unix_nanos
                .is_some_and(|start| max_timestamp < start)
                || query
                    .end_time_unix_nanos
                    .is_some_and(|end| min_timestamp > end)
            {
                continue;
            }
            if winners.len() >= limit
                && winners
                    .keys()
                    .nth(limit - 1)
                    .is_some_and(|cutoff| min_timestamp > *cutoff)
            {
                break;
            }
            match source {
                ExactMetricSource::Head(head) => {
                    for point in head
                        .points
                        .values()
                        .filter(|point| metric_exact_series_point_matches(query, point))
                    {
                        retain_exact_metric_winner(&mut winners, point.clone());
                    }
                }
                ExactMetricSource::Chunk(chunk) => {
                    let decoded = self.decode_chunk(chunk)?;
                    for point in decoded
                        .iter()
                        .filter(|point| metric_exact_series_point_matches(query, point))
                    {
                        retain_exact_metric_winner(&mut winners, point.clone());
                    }
                }
            }
        }
        let mut points = winners
            .into_values()
            .filter(|point| metric_query_cursor_matches(query, point))
            .collect::<Vec<_>>();
        if query.partition.is_some() {
            points.sort_unstable_by_key(|point| point.record_ref.offset);
        }
        points.truncate(limit);
        Ok(points)
    }

    pub(super) fn query_exact_series_by_offset(
        &self,
        query: &MetricQuery,
        series: SeriesFingerprint,
        head: &SeriesHead,
        limit: usize,
    ) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let mut winners = BTreeMap::<u64, DurableMetricPoint>::new();
        for chunk in self.chunks.get(&series).into_iter().flatten() {
            let decoded = self.decode_chunk(chunk)?;
            for point in decoded
                .iter()
                .filter(|point| metric_exact_series_point_matches(query, point))
            {
                retain_exact_metric_winner(&mut winners, point.clone());
            }
        }
        for point in head
            .points
            .values()
            .filter(|point| metric_exact_series_point_matches(query, point))
        {
            retain_exact_metric_winner(&mut winners, point.clone());
        }
        let mut points = winners
            .into_values()
            .filter(|point| metric_query_cursor_matches(query, point))
            .collect::<Vec<_>>();
        points.sort_unstable_by_key(|point| point.record_ref.offset);
        points.truncate(limit);
        Ok(points)
    }

    pub(super) fn exact_series_sources_are_disjoint(
        &self,
        series: SeriesFingerprint,
        head: &SeriesHead,
    ) -> bool {
        let chunks = self
            .chunks
            .get(&series)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if chunks
            .windows(2)
            .any(|pair| pair[0].max_timestamp_unix_nanos >= pair[1].min_timestamp_unix_nanos)
        {
            return false;
        }
        let Some(((head_min, _), _)) = head.points.first_key_value() else {
            return true;
        };
        chunks
            .last()
            .is_none_or(|chunk| chunk.max_timestamp_unix_nanos < *head_min)
    }

    pub(super) fn query_disjoint_exact_series(
        &self,
        query: &MetricQuery,
        series: SeriesFingerprint,
        head: &SeriesHead,
        limit: usize,
    ) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let chunks = self.chunks.get(&series).into_iter().flatten();
        let unbounded = query.partition.is_none()
            && query.start_offset.is_none()
            && query.start_time_unix_nanos.is_none()
            && query.end_time_unix_nanos.is_none();
        let initial_capacity = if self.chunks.get(&series).is_none_or(Vec::is_empty) {
            limit.min(head.points.len())
        } else {
            limit.min(head.points.len().saturating_add(4_096))
        };
        let mut selected = Vec::with_capacity(initial_capacity);
        if unbounded {
            for chunk in chunks {
                let decoded = self.decode_chunk(chunk)?;
                let remaining = limit.saturating_sub(selected.len());
                selected.extend(decoded.iter().take(remaining).cloned());
                if selected.len() == limit {
                    return Ok(selected);
                }
            }
            let remaining = limit.saturating_sub(selected.len());
            selected.extend(head.points.values().take(remaining).cloned());
            return Ok(selected);
        }
        for chunk in self.chunks.get(&series).into_iter().flatten() {
            if query
                .start_time_unix_nanos
                .is_some_and(|start| chunk.max_timestamp_unix_nanos < start)
                || query
                    .end_time_unix_nanos
                    .is_some_and(|end| chunk.min_timestamp_unix_nanos > end)
            {
                continue;
            }
            let decoded = self.decode_chunk(chunk)?;
            for point in decoded
                .iter()
                .filter(|point| metric_exact_series_point_matches(query, point))
            {
                selected.push(point.clone());
                if selected.len() == limit {
                    return Ok(selected);
                }
            }
        }
        for point in head
            .points
            .values()
            .filter(|point| metric_exact_series_point_matches(query, point))
        {
            selected.push(point.clone());
            if selected.len() == limit {
                break;
            }
        }
        Ok(selected)
    }

    pub(super) fn decode_chunk(
        &self,
        chunk: &SealedMetricChunk,
    ) -> TelemetryResult<Arc<[DurableMetricPoint]>> {
        if let Some(points) = self.decoded_chunks.borrow_mut().get(chunk.resident_id) {
            return Ok(points);
        }
        let points = Arc::<[DurableMetricPoint]>::from(decode_metric_chunk(&chunk.payload)?);
        let estimated_bytes = points.iter().fold(
            points.len().saturating_mul(size_of::<DurableMetricPoint>()),
            |total, point| total.saturating_add(point.estimated_head_bytes()),
        );
        self.decoded_chunks.borrow_mut().insert(
            chunk.resident_id,
            estimated_bytes,
            Arc::clone(&points),
        );
        Ok(points)
    }

    pub(super) fn sealed_chunks_at(
        &self,
        series: SeriesFingerprint,
        timestamp_unix_nanos: u64,
    ) -> TelemetryResult<Vec<Arc<[DurableMetricPoint]>>> {
        let mut chunks = Vec::new();
        for chunk in self
            .chunks
            .get(&series)
            .into_iter()
            .flatten()
            .filter(|chunk| {
                chunk.min_timestamp_unix_nanos <= timestamp_unix_nanos
                    && timestamp_unix_nanos <= chunk.max_timestamp_unix_nanos
            })
        {
            chunks.push(self.decode_chunk(chunk)?);
        }
        Ok(chunks)
    }

    pub(super) fn query_candidates(
        &self,
        query: &MetricQuery,
    ) -> Option<HashSet<SeriesFingerprint>> {
        let mut candidate = query
            .name
            .as_ref()
            .map(|name| self.name_index.get(name).cloned().unwrap_or_default());
        for (name, value) in query.exact_labels.iter() {
            let posting = self
                .label_index
                .get(&(Arc::clone(name), Arc::clone(value)))
                .cloned()
                .unwrap_or_default();
            candidate = Some(match candidate {
                Some(existing) => existing.intersection(&posting).copied().collect(),
                None => posting,
            });
        }
        candidate
    }

    pub(super) fn seal_largest_head(&mut self) -> TelemetryResult<()> {
        let Some(series) = self
            .series
            .iter()
            .max_by_key(|(_, head)| head.bytes)
            .map(|(series, _)| *series)
        else {
            return Ok(());
        };
        self.seal_series(series)
    }

    /// Seals every mutable series head belonging to one logical metric partition.
    pub(crate) fn seal_partition(&mut self, partition: TopicPartition) -> TelemetryResult<()> {
        let series = self
            .series
            .iter()
            .filter_map(|(series, head)| (head.topic_partition == partition).then_some(*series))
            .collect::<Vec<_>>();
        for series in series {
            self.seal_series(series)?;
        }
        Ok(())
    }

    pub(super) fn seal_series(&mut self, series: SeriesFingerprint) -> TelemetryResult<()> {
        let Some(head) = self.series.get_mut(&series) else {
            return Ok(());
        };
        if head.points.is_empty() {
            return Ok(());
        }
        let points = std::mem::take(&mut head.points)
            .into_values()
            .collect::<Vec<_>>();
        self.head_bytes = self.head_bytes.saturating_sub(head.bytes);
        head.bytes = 0;
        let encoded = encode_metric_chunk(&points)?;
        let resident_id = self.next_chunk_id;
        self.next_chunk_id = self.next_chunk_id.saturating_add(1);
        let payload = Arc::<[u8]>::from(encoded);
        let first_offset = points
            .iter()
            .map(|point| point.record_ref.offset.get())
            .min()
            .expect("sealed metric chunk is nonempty");
        let last_offset = points
            .iter()
            .map(|point| point.record_ref.offset.get())
            .max()
            .expect("sealed metric chunk is nonempty");
        let min_timestamp_unix_nanos = points
            .iter()
            .map(|point| point.timestamp_unix_nanos)
            .min()
            .expect("sealed metric chunk is nonempty");
        let max_timestamp_unix_nanos = points
            .iter()
            .map(|point| point.timestamp_unix_nanos)
            .max()
            .expect("sealed metric chunk is nonempty");
        self.pending_chunks.push(SignalTierPayload {
            resident_id,
            topic_partition: head.topic_partition,
            min_signal_identity: series.get(),
            max_signal_identity: series.get(),
            first_offset,
            last_offset,
            record_count: u32::try_from(points.len())
                .map_err(|_| TelemetryError::RecordTooLarge)?,
            min_timestamp_unix_nanos,
            max_timestamp_unix_nanos,
            payload: Arc::clone(&payload),
            correlation_filter: CorrelationBlockFilter::for_metrics(&points),
        });
        self.chunks
            .entry(series)
            .or_default()
            .push(SealedMetricChunk {
                resident_id,
                min_timestamp_unix_nanos,
                max_timestamp_unix_nanos,
                payload,
            });
        Ok(())
    }

    pub(crate) fn pending_partition(&self, partition: TopicPartition) -> Vec<SignalTierPayload> {
        self.pending_chunks
            .iter()
            .filter(|payload| payload.topic_partition == partition)
            .cloned()
            .collect()
    }

    pub(crate) fn release_published_chunks(&mut self, resident_ids: &[u64]) {
        self.decoded_chunks.borrow_mut().remove(resident_ids);
        self.pending_chunks
            .retain(|payload| !resident_ids.contains(&payload.resident_id));
        self.chunks.retain(|_, chunks| {
            chunks.retain(|chunk| !resident_ids.contains(&chunk.resident_id));
            !chunks.is_empty()
        });
    }

    pub(crate) fn retained_payload_bytes(&self) -> u64 {
        self.chunks
            .values()
            .flatten()
            .map(|chunk| u64::try_from(chunk.payload.len()).unwrap_or(u64::MAX))
            .sum()
    }
}
