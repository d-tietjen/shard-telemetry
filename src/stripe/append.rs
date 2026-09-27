use super::*;

impl LogStripe {
    /// Applies a record only after the corresponding shard-stream append is durable.
    ///
    /// Index postings are written before the visible watermark advances. A
    /// query constrained to [`Self::indexed_through`] consequently cannot see
    /// an incomplete posting update.
    pub fn apply_durable(&mut self, record: DurableLog) -> TelemetryResult<IndexReceipt> {
        if record.stream_shard_id != self.stream_shard_id {
            return Err(TelemetryError::WrongStripe {
                expected: self.stream_shard_id,
                observed: record.stream_shard_id,
            });
        }
        if self
            .partitions
            .get(&record.record_ref.topic_partition)
            .and_then(|partition| partition.record(record.record_ref.offset))
            .is_some()
        {
            return Err(TelemetryError::DuplicateRecord {
                partition: record.record_ref.topic_partition,
                offset: record.record_ref.offset,
            });
        }
        self.apply_durable_new(record)
    }

    pub(super) fn apply_durable_new(
        &mut self,
        record: DurableLog,
    ) -> TelemetryResult<IndexReceipt> {
        self.apply_durable_new_inner(record, true)
            .map(|applied| applied.receipt)
    }

    pub(super) fn apply_durable_new_inner(
        &mut self,
        record: DurableLog,
        index_record: bool,
    ) -> TelemetryResult<AppliedRecord> {
        self.active_partition_cache.clear();
        self.validate_offset(&record)?;

        let record_source_bytes = row_source_bytes(&record)?;
        let fingerprint = if self.block_collator.is_enabled() {
            fingerprint_message(&record.message, &record.fields)
        } else {
            MessageFingerprint {
                shape_hash: 0,
                locality_signature: 0,
            }
        };
        let compression_temperature = CompressionTemperature::new(fingerprint.locality_signature);
        let tentative_compression_placement = self
            .block_collator
            .tentative_placement(record.compression_cohort, fingerprint);
        let dictionary = self.resolve_dictionary(tentative_compression_placement.placement_id)?;
        let active_key = ActiveBlockKey {
            topic_partition: record.record_ref.topic_partition,
            source_compression_cohort: record.compression_cohort,
            placement_id: tentative_compression_placement.placement_id,
            dictionary_id: dictionary.dictionary_id,
        };
        let reference = record.record_ref;
        let pending = PendingRecord {
            record: record.clone(),
            source_bytes: record_source_bytes,
            fingerprint,
        };
        let record_ordinal = {
            let partition = self
                .partitions
                .entry(reference.topic_partition)
                .or_default();
            let record_ordinal = u32::try_from(partition.records.len())
                .map_err(|_| TelemetryError::RecordTooLarge)?;
            partition.records.push(IndexedRecord {
                record: record.clone(),
                tentative_placement: tentative_compression_placement,
                temperature: compression_temperature,
                final_placement: None,
            });
            record_ordinal
        };

        let next_source_bytes = self.active_blocks.get(&active_key).map_or_else(
            || record_source_bytes,
            |active| active.source_bytes.saturating_add(record_source_bytes),
        );
        let sealed_result = if next_source_bytes >= self.config.target_block_bytes {
            let active = match self.active_blocks.remove(&active_key) {
                Some(mut active) => {
                    active.append(pending);
                    active
                }
                None => ActiveBlock::new(pending, dictionary.payload),
            };
            self.rebalance_block(active_key, active, false)
        } else {
            match self.active_blocks.get_mut(&active_key) {
                Some(active) => {
                    active.append(pending);
                }
                None => {
                    self.active_blocks
                        .insert(active_key, ActiveBlock::new(pending, dictionary.payload));
                }
            }
            Ok(Vec::new())
        };
        let sealed_blocks = match sealed_result {
            Ok(sealed_blocks) => sealed_blocks,
            Err(error) => {
                let removed = self
                    .partitions
                    .get_mut(&reference.topic_partition)
                    .and_then(|partition| partition.records.pop());
                debug_assert!(
                    removed.is_some_and(|removed| removed.record.record_ref == reference)
                );
                return Err(error);
            }
        };

        if let Some(partition) = self.partitions.get_mut(&reference.topic_partition)
            && partition.timestamp_order == TimestampOrder::NonDecreasing
            && partition
                .records
                .get(partition.records.len().saturating_sub(2))
                .is_some_and(|previous| {
                    previous.record.timestamp_unix_nanos > record.timestamp_unix_nanos
                })
        {
            partition.timestamp_order = TimestampOrder::Unordered;
        }

        let (term_ids, message_trigram_keys, field_ids) = if index_record {
            let term_ids = self.index_terms(&record, record_ordinal);
            let message_trigram_keys = self.index_message_trigrams(&record, record_ordinal);
            let field_ids = self.index_fields(&record, record_ordinal);
            // This assignment is deliberately last: it is the publication
            // barrier for readers sharing this stripe's ordering domain.
            self.partitions
                .get_mut(&reference.topic_partition)
                .expect("record partition was inserted")
                .indexed_through = Some(reference.offset);
            (Some(term_ids), message_trigram_keys, Some(field_ids))
        } else {
            (None, None, None)
        };

        Ok(AppliedRecord {
            receipt: IndexReceipt {
                record_ref: reference,
                indexed_through: reference.offset,
                compression_temperature,
                tentative_compression_placement,
                sealed_blocks,
            },
            ordinal: record_ordinal,
            term_ids,
            message_trigram_keys,
            field_ids,
        })
    }

    /// Publishes OTLP events after shard-stream has made their append durable.
    ///
    /// The live ingestion path should decode the export once before appending,
    /// set shard-stream's `record_count` to `events.len()`, then pass the same
    /// events here after it receives `first_offset` in the append response.
    pub fn apply_otlp_events(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        events: impl IntoIterator<Item = OtlpLogEvent>,
    ) -> TelemetryResult<Vec<IndexReceipt>> {
        self.begin_append_batch()?;
        let events = events.into_iter().collect::<Vec<_>>();
        validate_batch_offset_range(topic_partition, first_offset, events.len())?;
        if self.can_index_as_homogeneous_range(topic_partition, first_offset, &events) {
            self.apply_homogeneous_events(topic_partition, first_offset, events)
        } else {
            events
                .into_iter()
                .enumerate()
                .map(|(index, event)| {
                    let offset = batch_offset(topic_partition, first_offset, index)?;
                    self.apply_durable_idempotent(event.into_durable(
                        self.stream_shard_id,
                        topic_partition,
                        offset,
                    ))
                })
                .collect()
        }
    }

    /// Publishes one durable compressed ingest pack without rebuilding a
    /// second per-record posting index.
    ///
    /// The authoritative compressed cohort frames remain resident and the
    /// compressor-derived indexes select candidates. Exact bodies and fields
    /// are reconstructed only for candidate records during lookup.
    #[cfg(test)]
    pub(crate) fn apply_indexed_ingest_pack(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        record_count: u32,
        payload: Bytes,
    ) -> TelemetryResult<()> {
        self.apply_indexed_ingest_pack_inner(
            Arc::from("test-tenant"),
            topic_partition,
            first_offset,
            record_count,
            payload,
            None,
            false,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_checkpointed_ingest_pack(
        &mut self,
        tenant: Arc<str>,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        record_count: u32,
        payload: Bytes,
        transient_context: Option<&[u8]>,
        payload_already_validated: bool,
        checkpoints: (DurableSinkCheckpoint, DurableSinkCheckpoint),
    ) -> TelemetryResult<()> {
        let (expected_checkpoint, next_checkpoint) = checkpoints;
        if expected_checkpoint.topic_partition != topic_partition
            || next_checkpoint.topic_partition != topic_partition
        {
            return Err(TelemetryError::CorruptSinkJournal(
                "indexed append checkpoints refer to another partition".into(),
            ));
        }
        self.apply_indexed_ingest_pack_inner(
            tenant,
            topic_partition,
            first_offset,
            record_count,
            payload,
            transient_context,
            payload_already_validated,
            Some(next_checkpoint),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_indexed_ingest_pack_inner(
        &mut self,
        tenant: Arc<str>,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        record_count: u32,
        payload: Bytes,
        transient_context: Option<&[u8]>,
        payload_already_validated: bool,
        next_checkpoint: Option<DurableSinkCheckpoint>,
    ) -> TelemetryResult<()> {
        self.active_partition_cache.clear();
        if tenant.is_empty() || record_count == 0 {
            return Err(TelemetryError::InvalidConfig(
                "compressed ingest append must have a tenant and contain records",
            ));
        }
        let last_offset = batch_offset(
            topic_partition,
            first_offset,
            usize::try_from(record_count - 1)
                .map_err(|_| TelemetryError::OffsetExhausted(topic_partition))?,
        )?;
        if let Some(previous) = self.indexed_through(topic_partition)
            && first_offset <= previous
        {
            if last_offset <= previous {
                return Ok(());
            }
            let expected = previous
                .get()
                .checked_add(1)
                .map(LogicalOffset::new)
                .ok_or(TelemetryError::OffsetExhausted(topic_partition))?;
            return Err(TelemetryError::OffsetOutOfOrder {
                partition: topic_partition,
                expected,
                observed: first_offset,
            });
        }
        let mut frames = if payload_already_validated {
            decode_indexed_ingest_frames_after_validation(payload, transient_context, record_count)?
        } else {
            decode_indexed_ingest_frames(payload, transient_context, record_count)?
        };
        for frame in &mut frames {
            frame.frame_id = self.next_frame_id;
            self.next_frame_id = self
                .next_frame_id
                .checked_add(1)
                .ok_or(TelemetryError::RecordTooLarge)?;
        }
        let partition = self
            .indexed_frame_partitions
            .entry(topic_partition)
            .or_default();
        partition.appends.push(IndexedFrameAppend {
            tenant,
            first_offset,
            last_offset,
            record_count,
            frames,
            next_checkpoint,
        });
        // Publication barrier: readers never observe a watermark before all
        // frame metadata and embedded index views are installed.
        partition.indexed_through = Some(last_offset);
        Ok(())
    }

    /// Decodes and publishes one OTLP `ExportLogsServiceRequest`.
    ///
    /// This convenience method is suited to replay and tests. A live OTLP
    /// receiver should instead decode before the shard-stream append, use the
    /// decoded event count for reservation, then call [`Self::apply_otlp_events`]
    /// after the durable append response.
    pub fn apply_otlp_export(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        payload: &[u8],
    ) -> TelemetryResult<Vec<IndexReceipt>> {
        self.apply_otlp_events(
            topic_partition,
            first_offset,
            OtlpLogDecoder.decode(payload)?,
        )
    }

    pub(super) fn validate_offset(&self, record: &DurableLog) -> TelemetryResult<()> {
        let Some(previous) = self
            .partitions
            .get(&record.record_ref.topic_partition)
            .and_then(PartitionIndex::last_offset)
        else {
            return Ok(());
        };
        let expected = previous
            .get()
            .checked_add(1)
            .map(LogicalOffset::new)
            .ok_or(TelemetryError::OffsetExhausted(
                record.record_ref.topic_partition,
            ))?;
        if record.record_ref.offset <= previous {
            return Err(TelemetryError::OffsetOutOfOrder {
                partition: record.record_ref.topic_partition,
                expected,
                observed: record.record_ref.offset,
            });
        }
        Ok(())
    }

    pub(super) fn apply_durable_idempotent(
        &mut self,
        record: DurableLog,
    ) -> TelemetryResult<IndexReceipt> {
        let topic_partition = record.record_ref.topic_partition;
        let offset = record.record_ref.offset;
        let existing = self.partitions.get(&topic_partition).and_then(|partition| {
            let last = partition.records.last()?;
            match last.record.record_ref.offset.cmp(&offset) {
                std::cmp::Ordering::Less => None,
                std::cmp::Ordering::Equal => Some(last),
                std::cmp::Ordering::Greater => partition.record(offset),
            }
        });
        if let Some(existing) = existing {
            if existing.record != record {
                return Err(TelemetryError::ConflictingRecord {
                    partition: topic_partition,
                    offset,
                });
            }
            return Ok(IndexReceipt {
                record_ref: record.record_ref,
                indexed_through: self.indexed_through(topic_partition).unwrap_or(offset),
                compression_temperature: existing.temperature,
                tentative_compression_placement: existing.tentative_placement,
                sealed_blocks: Vec::new(),
            });
        }
        self.apply_durable_new(record)
    }

    pub(super) fn can_index_as_homogeneous_range(
        &self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        events: &[OtlpLogEvent],
    ) -> bool {
        if events.len() < 2 {
            return false;
        }
        let first = events
            .first()
            .expect("the homogeneous range minimum length was checked");
        if self
            .partitions
            .get(&topic_partition)
            .and_then(PartitionIndex::last_offset)
            .is_some_and(|last_offset| first_offset <= last_offset)
        {
            return false;
        }
        events[1..].iter().all(|event| {
            same_message(&first.message, &event.message)
                && same_fields(&first.fields, &event.fields)
        })
    }

    pub(super) fn apply_homogeneous_events(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        events: Vec<OtlpLogEvent>,
    ) -> TelemetryResult<Vec<IndexReceipt>> {
        let mut events = events.into_iter().enumerate();
        let (first_index, first_event) = events
            .next()
            .expect("homogeneous event ranges contain at least two records");
        debug_assert_eq!(first_index, 0);
        let field_keys = first_event
            .fields
            .iter()
            .map(|field| Arc::clone(&field.key))
            .collect::<Vec<_>>();
        let first_applied = self.apply_durable_new_inner(
            first_event.into_durable(self.stream_shard_id, topic_partition, first_offset),
            true,
        )?;
        let first_ordinal = first_applied.ordinal;
        let term_ids = first_applied
            .term_ids
            .expect("the first homogeneous record was indexed");
        let message_trigram_keys = first_applied.message_trigram_keys;
        let field_ids = first_applied
            .field_ids
            .expect("the first homogeneous record was indexed");
        let mut receipts = Vec::with_capacity(events.size_hint().0.saturating_add(1));
        receipts.push(first_applied.receipt);
        let mut last_deferred = None;

        for (index, event) in events {
            let offset = batch_offset(topic_partition, first_offset, index)?;
            match self.apply_durable_new_inner(
                event.into_durable(self.stream_shard_id, topic_partition, offset),
                false,
            ) {
                Ok(applied) => {
                    debug_assert_eq!(
                        applied.ordinal,
                        first_ordinal
                            .checked_add(u32::try_from(index).expect("batch offset was bounded"))
                            .expect("record ordinal was bounded")
                    );
                    last_deferred = Some((applied.ordinal, offset));
                    receipts.push(applied.receipt);
                }
                Err(error) => {
                    if let Some((last_ordinal, last_offset)) = last_deferred {
                        self.publish_homogeneous_posting_range(
                            topic_partition,
                            first_ordinal + 1,
                            last_ordinal,
                            last_offset,
                            &term_ids,
                            message_trigram_keys.as_deref(),
                            &field_ids,
                            &field_keys,
                        );
                    }
                    return Err(error);
                }
            }
        }

        let (last_ordinal, last_offset) =
            last_deferred.expect("homogeneous event ranges contain a deferred record");
        self.publish_homogeneous_posting_range(
            topic_partition,
            first_ordinal + 1,
            last_ordinal,
            last_offset,
            &term_ids,
            message_trigram_keys.as_deref(),
            &field_ids,
            &field_keys,
        );
        Ok(receipts)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn publish_homogeneous_posting_range(
        &mut self,
        topic_partition: TopicPartition,
        first_ordinal: u32,
        last_ordinal: u32,
        last_offset: LogicalOffset,
        term_ids: &[usize],
        message_trigram_keys: Option<&[u32]>,
        field_ids: &[usize],
        field_keys: &[Arc<str>],
    ) {
        debug_assert!(first_ordinal <= last_ordinal);
        let partition = self
            .partitions
            .get_mut(&topic_partition)
            .expect("homogeneous records were inserted");
        for term_id in term_ids {
            partition
                .term_postings
                .get_mut(*term_id)
                .expect("interned term has a posting slot")
                .push_range(first_ordinal, last_ordinal);
        }
        if let Some(message_trigram_keys) = message_trigram_keys {
            for key in message_trigram_keys {
                if let Some(posting) = partition.message_trigram_postings.get_mut(key) {
                    posting.push_range(first_ordinal, last_ordinal);
                }
            }
        }
        for field_id in field_ids {
            partition
                .field_postings
                .get_mut(*field_id)
                .expect("interned field has a posting slot")
                .push_range(first_ordinal, last_ordinal);
        }
        for field_key in field_keys {
            partition
                .field_presence_postings
                .entry(Arc::clone(field_key))
                .or_default()
                .push_range(first_ordinal, last_ordinal);
        }
        // This assignment is the publication barrier for the deferred range.
        partition.indexed_through = Some(last_offset);
    }

    pub(super) fn index_message_trigrams(
        &mut self,
        record: &DurableLog,
        record_ordinal: u32,
    ) -> Option<Arc<[u32]>> {
        let keys = collect_message_trigram_keys(&record.message);
        let partition = self
            .partitions
            .get_mut(&record.record_ref.topic_partition)
            .expect("record partition was inserted");
        if !record.message.is_ascii() {
            partition.message_trigram_ascii_only = false;
        }
        if !partition.message_trigram_index_complete {
            return None;
        }
        for key in &keys {
            if !partition.message_trigram_postings.contains_key(key)
                && partition.message_trigram_postings.len() >= MAX_HOT_MESSAGE_TRIGRAM_KEYS
            {
                partition.message_trigram_index_complete = false;
                partition.message_trigram_postings.clear();
                return None;
            }
            partition
                .message_trigram_postings
                .entry(*key)
                .or_default()
                .push(record_ordinal);
        }
        Some(Arc::from(keys))
    }

    pub(super) fn index_terms(&mut self, record: &DurableLog, record_ordinal: u32) -> Arc<[usize]> {
        let topic_partition = record.record_ref.topic_partition;
        let cache_slot = message_term_cache_slot(topic_partition, record.message.as_bytes());
        let term_ids = if let Some(cached) = &self.message_term_cache[cache_slot]
            && cached.topic_partition == topic_partition
            && same_message(&cached.message, &record.message)
        {
            Arc::clone(&cached.term_ids)
        } else {
            let mut message_term_ids = Vec::new();
            {
                let partition = self
                    .partitions
                    .get_mut(&topic_partition)
                    .expect("record partition was inserted");
                scan_message_terms(&record.message, |term| {
                    let normalized = normalize_term(term);
                    let term_id = match partition.term_ids.get(normalized.as_ref()).copied() {
                        Some(term_id) => term_id,
                        None => {
                            let term_id = partition.term_postings.len();
                            partition
                                .term_ids
                                .insert(Arc::from(normalized.as_ref()), term_id);
                            partition.term_postings.push(HotPostingList::default());
                            term_id
                        }
                    };
                    if !message_term_ids.contains(&term_id) {
                        message_term_ids.push(term_id);
                    }
                });
            }
            let term_ids = Arc::<[usize]>::from(message_term_ids);
            self.message_term_cache[cache_slot] = Some(CachedMessageTerms {
                topic_partition,
                message: Arc::clone(&record.message),
                term_ids: Arc::clone(&term_ids),
            });
            term_ids
        };
        let term_postings = &mut self
            .partitions
            .get_mut(&topic_partition)
            .expect("record partition was inserted")
            .term_postings;
        for term_id in term_ids.iter().copied() {
            term_postings
                .get_mut(term_id)
                .expect("interned term has a posting slot")
                .push(record_ordinal);
        }
        term_ids
    }

    pub(super) fn index_fields(
        &mut self,
        record: &DurableLog,
        record_ordinal: u32,
    ) -> Arc<[usize]> {
        let topic_partition = record.record_ref.topic_partition;
        let cache_slot = field_cache_slot(topic_partition, &record.fields);
        let field_ids = if let Some(cached) = &self.field_cache[cache_slot]
            && cached.topic_partition == topic_partition
            && same_fields(&cached.fields, &record.fields)
        {
            Arc::clone(&cached.field_ids)
        } else {
            let mut record_field_ids = Vec::with_capacity(record.fields.len());
            let partition = self
                .partitions
                .get_mut(&topic_partition)
                .expect("record partition was inserted");
            for (index, field) in record.fields.iter().enumerate() {
                if record.fields[..index]
                    .iter()
                    .any(|existing| existing.key == field.key && existing.value == field.value)
                {
                    continue;
                }
                let field_id = match partition
                    .field_ids
                    .get(field.key.as_ref())
                    .and_then(|values| values.get(field.value.as_ref()))
                    .copied()
                {
                    Some(field_id) => field_id,
                    None => {
                        let field_id = partition.field_postings.len();
                        partition
                            .field_ids
                            .entry(Arc::clone(&field.key))
                            .or_default()
                            .insert(Arc::clone(&field.value), field_id);
                        if let Ok(observed) = field.value.parse::<i128>() {
                            partition
                                .numeric_field_values
                                .entry(Arc::clone(&field.key))
                                .or_default()
                                .push((observed, field_id));
                        }
                        partition.field_postings.push(HotPostingList::default());
                        field_id
                    }
                };
                record_field_ids.push(field_id);
            }
            let field_ids = Arc::<[usize]>::from(record_field_ids);
            self.field_cache[cache_slot] = Some(CachedFields {
                topic_partition,
                fields: Arc::clone(&record.fields),
                field_ids: Arc::clone(&field_ids),
            });
            field_ids
        };
        let partition = self
            .partitions
            .get_mut(&topic_partition)
            .expect("record partition was inserted");
        let field_postings = &mut partition.field_postings;
        for field_id in field_ids.iter().copied() {
            field_postings
                .get_mut(field_id)
                .expect("interned field has a posting slot")
                .push(record_ordinal);
        }
        let mut seen_keys = Vec::<&str>::new();
        for field in record.fields.iter() {
            if seen_keys.contains(&field.key.as_ref()) {
                continue;
            }
            seen_keys.push(field.key.as_ref());
            partition
                .field_presence_postings
                .entry(Arc::clone(&field.key))
                .or_default()
                .push(record_ordinal);
        }
        field_ids
    }
}
