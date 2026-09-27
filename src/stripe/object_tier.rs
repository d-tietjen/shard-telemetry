use super::*;

impl LogStripe {
    /// Seals every active block and returns their immutable descriptors.
    pub fn seal_active_blocks(&mut self) -> TelemetryResult<Vec<BlockDescriptor>> {
        let active_blocks = std::mem::take(&mut self.active_blocks);
        let mut sealed = Vec::new();
        for (key, active) in active_blocks {
            sealed.extend(self.rebalance_block(key, active, true)?);
        }
        Ok(sealed)
    }

    /// Publishes complete indexed append boundaries to immutable object storage.
    ///
    /// When `force` is false, only groups at the configured target size are
    /// published. A forced pass seals every remaining checkpointed append.
    pub(crate) fn offload_indexed_groups(&mut self, force: bool) -> TelemetryResult<usize> {
        let Some(state) = self.tier.as_ref() else {
            return Ok(0);
        };
        let partitions = state.tiers.keys().copied().collect::<Vec<_>>();
        let mut published = 0usize;
        for partition in partitions {
            loop {
                if !self.offload_one_indexed_group(partition, force)? {
                    break;
                }
                published = published.saturating_add(1);
            }
        }
        Ok(published)
    }

    pub(crate) fn reclaim_retired_object_generations(&mut self) -> TelemetryResult<()> {
        let Some(state) = self.tier.as_mut() else {
            return Ok(());
        };
        for tier in state.tiers.values_mut() {
            tier.reclaim_retired_objects()?;
        }
        Ok(())
    }

    pub(crate) fn retain_object_tier_since(
        &mut self,
        cutoff_timestamp_unix_nanos: u64,
    ) -> TelemetryResult<TierRetentionReport> {
        let Some(state) = self.tier.as_mut() else {
            return Ok(TierRetentionReport::default());
        };
        let mut total = TierRetentionReport::default();
        for tier in state.tiers.values_mut() {
            let report = tier.retain_since_timestamp(cutoff_timestamp_unix_nanos)?;
            total.retired_groups = total.retired_groups.saturating_add(report.retired_groups);
            total.retired_payload_bytes = total
                .retired_payload_bytes
                .saturating_add(report.retired_payload_bytes);
            total.retired_objects = total.retired_objects.saturating_add(report.retired_objects);
        }
        Ok(total)
    }

    pub(crate) fn retain_object_tier_to_payload_bytes(
        &mut self,
        max_payload_bytes_per_partition: u64,
    ) -> TelemetryResult<TierRetentionReport> {
        let Some(state) = self.tier.as_mut() else {
            return Ok(TierRetentionReport::default());
        };
        let mut total = TierRetentionReport::default();
        for tier in state.tiers.values_mut() {
            let report = tier.retain_to_payload_bytes(max_payload_bytes_per_partition)?;
            total.retired_groups = total.retired_groups.saturating_add(report.retired_groups);
            total.retired_payload_bytes = total
                .retired_payload_bytes
                .saturating_add(report.retired_payload_bytes);
            total.retired_objects = total.retired_objects.saturating_add(report.retired_objects);
        }
        Ok(total)
    }

    pub(super) fn offload_one_indexed_group(
        &mut self,
        partition: TopicPartition,
        force: bool,
    ) -> TelemetryResult<bool> {
        let state = self
            .tier
            .as_ref()
            .ok_or(TelemetryError::InvalidConfig("object tier is not attached"))?;
        let config = state.config;
        let Some(resident) = self.indexed_frame_partitions.get(&partition) else {
            return Ok(false);
        };
        let mut selected_appends = 0usize;
        let mut selected_payload_bytes = 0u64;
        let mut selected_frames = 0usize;
        for append in &resident.appends {
            if append.next_checkpoint.is_none() {
                break;
            }
            let append_payload_bytes = append.frames.iter().try_fold(0u64, |total, frame| {
                total
                    .checked_add(
                        u64::try_from(frame.compressed.len())
                            .map_err(|_| TelemetryError::RecordTooLarge)?,
                    )
                    .ok_or(TelemetryError::RecordTooLarge)
            })?;
            let append_frames = append.frames.len();
            if append_payload_bytes > config.max_group_payload_bytes
                || append_frames > config.max_blocks_per_group
            {
                return Err(TelemetryError::ObjectStore(
                    "one durable append exceeds the object-tier group limit".into(),
                ));
            }
            if selected_appends > 0
                && (selected_payload_bytes.saturating_add(append_payload_bytes)
                    > config.max_group_payload_bytes
                    || selected_frames.saturating_add(append_frames) > config.max_blocks_per_group)
            {
                break;
            }
            selected_appends += 1;
            selected_payload_bytes = selected_payload_bytes
                .checked_add(append_payload_bytes)
                .ok_or(TelemetryError::RecordTooLarge)?;
            selected_frames = selected_frames
                .checked_add(append_frames)
                .ok_or(TelemetryError::RecordTooLarge)?;
            if selected_payload_bytes >= config.target_group_payload_bytes {
                break;
            }
        }
        if selected_appends == 0
            || (!force && selected_payload_bytes < config.target_group_payload_bytes)
        {
            return Ok(false);
        }

        let sources = resident.appends[..selected_appends]
            .iter()
            .map(|append| TierIngestAppendSource {
                tenant: append.tenant.to_string(),
                first_offset: append.first_offset,
                last_offset: append.last_offset,
                record_count: append.record_count,
                frames: append
                    .frames
                    .iter()
                    .map(|frame| TierIngestFrameSource {
                        frame_id: frame.frame_id,
                        cohort: frame.cohort,
                        record_count: frame.record_count,
                        structural_bytes: frame.structural_bytes,
                        min_timestamp_unix_nanos: frame.min_timestamp_unix_nanos,
                        max_timestamp_unix_nanos: frame.max_timestamp_unix_nanos,
                        compressed: frame.compressed.clone(),
                        index: frame.index.clone(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let checkpoint = resident.appends[selected_appends - 1]
            .next_checkpoint
            .expect("selected checkpointed append has a next checkpoint");
        let state = self
            .tier
            .as_mut()
            .expect("object tier was checked before group selection");
        let tier = state
            .tiers
            .get_mut(&partition)
            .expect("selected partition has an object tier");
        let group_sequence = tier
            .root()
            .pages
            .last()
            .map_or(0, |page| page.last_group_sequence.saturating_add(1));
        let group_directory = state.spool_directory.join(format!(
            "topic-{}-partition-{}/group-{group_sequence:020}",
            partition.topic_id.get(),
            partition.partition_id.get()
        ));
        let payload_path = group_directory.join("payload.pack");
        let query_index_path = group_directory.join("query-index.sltqix");
        let blocks = write_tier_ingest_group(&sources, &payload_path, &query_index_path)?;
        let manifest = tier.publish_group(TierGroupSource {
            group_sequence,
            checkpoint: TierCheckpoint {
                next_placement_sequence: checkpoint.next_placement_sequence.get(),
                next_offset: checkpoint.next_offset.get(),
            },
            blocks,
            artifacts: vec![
                TierArtifactSource {
                    kind: TierArtifactKind::PayloadPack,
                    name: "payload.pack".into(),
                    path: payload_path.clone(),
                },
                TierArtifactSource {
                    kind: TierArtifactKind::QueryIndex,
                    name: "query-index.sltqix".into(),
                    path: query_index_path.clone(),
                },
            ],
        })?;
        if state.warm_local_cache_on_publish {
            let entry = tier
                .latest_group_cached(&state.control_cache)?
                .ok_or_else(|| {
                    TelemetryError::CorruptTier(
                        "published log group is missing from its catalog".into(),
                    )
                })?;
            let _ = tier.load_group_cached(&entry, &state.control_cache)?;
            let payload_artifact = manifest
                .artifact(TierArtifactKind::PayloadPack)
                .ok_or_else(|| TelemetryError::CorruptTier("log group has no payload".into()))?;
            state
                .payload_cache
                .admit_file(payload_artifact, &payload_path)?;
            let query_index_artifact =
                manifest
                    .artifact(TierArtifactKind::QueryIndex)
                    .ok_or_else(|| {
                        TelemetryError::CorruptTier("log group has no query index".into())
                    })?;
            state
                .control_cache
                .admit_file(query_index_artifact, &query_index_path)?;
        }
        self.indexed_frame_partitions
            .get_mut(&partition)
            .expect("resident partition remains present")
            .appends
            .drain(..selected_appends);
        remove_published_spool_file(&payload_path);
        remove_published_spool_file(&query_index_path);
        let _ = fs::remove_dir(&group_directory);
        if let Some(parent) = group_directory.parent() {
            let _ = fs::remove_dir(parent);
        }
        Ok(true)
    }

    /// Marks a sealed block as durably written to the object tier.
    pub fn mark_block_offloaded(
        &mut self,
        block_id: BlockId,
        object_key: impl Into<Arc<str>>,
    ) -> TelemetryResult<()> {
        self.catalog.mark_offloaded(block_id, object_key)
    }

    /// Marks a sealed block as a byte range inside a durable object-tier pack.
    pub fn mark_block_offloaded_range(
        &mut self,
        block_id: BlockId,
        object_key: impl Into<Arc<str>>,
        object_offset: u64,
    ) -> TelemetryResult<()> {
        self.catalog
            .mark_offloaded_range(block_id, object_key, object_offset)
    }

    pub(super) fn resolve_dictionary(
        &mut self,
        placement_id: CompressionPlacementId,
    ) -> TelemetryResult<DictionarySelection> {
        let Some(dictionary_id) = self.placement_dictionaries.get(&placement_id).copied() else {
            return Ok(DictionarySelection {
                dictionary_id: None,
                payload: None,
            });
        };
        if let Some(payload) = self.dictionary_cache.get(dictionary_id) {
            return Ok(DictionarySelection {
                dictionary_id: Some(dictionary_id),
                payload: Some(payload),
            });
        }

        let payload = self
            .dictionary_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.dictionary(dictionary_id))
            .ok_or(TelemetryError::MissingDictionary(dictionary_id))?;
        self.dictionary_cache
            .insert(dictionary_id, Arc::clone(&payload))?;
        Ok(DictionarySelection {
            dictionary_id: Some(dictionary_id),
            payload: Some(payload),
        })
    }

    pub(super) fn rebalance_block(
        &mut self,
        initial_key: ActiveBlockKey,
        initial_block: ActiveBlock,
        force_seal: bool,
    ) -> TelemetryResult<Vec<BlockDescriptor>> {
        if !self.block_collator.is_enabled() {
            let temperature = CompressionTemperature::new(0);
            let placement =
                CompressionPlacement::base(initial_key.source_compression_cohort, temperature);
            let score = CompressionBlockScore {
                temperature,
                shape_hash: 0,
                internal_variance_q8: 0,
                max_deviation: 0,
                source_bytes: initial_block.source_bytes,
                record_count: initial_block.records.len(),
            };
            return self
                .stage_active_block(initial_key, &initial_block, placement, score)
                .map(|descriptor| vec![descriptor]);
        }

        let mut work = vec![(initial_key, initial_block)];
        let mut sealed = Vec::new();
        while let Some((home_key, active)) = work.pop() {
            let home_dictionary_payload = active.dictionary_payload.clone();
            let next_pass = active.rebalance_passes.saturating_add(1);
            let locality_records = active
                .records
                .iter()
                .map(PendingRecord::locality)
                .collect::<Vec<_>>();
            let assignments = self.block_collator.collate(
                home_key.source_compression_cohort,
                home_key.placement_id,
                &locality_records,
            );
            let assignment_count = assignments.len();
            let mut records = active.records.into_iter().map(Some).collect::<Vec<_>>();

            for assignment in assignments {
                let placement = assignment.placement;
                let score = assignment.score;
                let group_records = assignment
                    .record_indices()
                    .map(|index| {
                        records[index]
                            .take()
                            .expect("collation membership contains each record once")
                    })
                    .collect::<Vec<_>>();
                let (target_key, dictionary_payload) =
                    if placement.placement_id == home_key.placement_id {
                        (home_key, home_dictionary_payload.clone())
                    } else {
                        let dictionary = self.resolve_dictionary(placement.placement_id)?;
                        (
                            ActiveBlockKey {
                                topic_partition: home_key.topic_partition,
                                source_compression_cohort: home_key.source_compression_cohort,
                                placement_id: placement.placement_id,
                                dictionary_id: dictionary.dictionary_id,
                            },
                            dictionary.payload,
                        )
                    };
                let mut group =
                    ActiveBlock::from_records(group_records, dictionary_payload, next_pass);

                if force_seal {
                    sealed.push(self.stage_active_block(target_key, &group, placement, score)?);
                    continue;
                }

                let merged_existing = if let Some(existing) = self.active_blocks.remove(&target_key)
                {
                    let mut existing = existing;
                    existing.append_block(group);
                    group = existing;
                    true
                } else {
                    false
                };
                if group.source_bytes < self.config.target_block_bytes {
                    self.active_blocks.insert(target_key, group);
                    continue;
                }

                let stable_home_block = !merged_existing
                    && assignment_count == 1
                    && target_key.placement_id == home_key.placement_id
                    && score.internal_variance_q8
                        <= self.config.compression_locality.split_variance_q8;
                if stable_home_block || group.rebalance_passes >= MAX_REBALANCE_PASSES {
                    sealed.push(self.stage_active_block(target_key, &group, placement, score)?);
                } else {
                    work.push((target_key, group));
                }
            }
            debug_assert!(records.iter().all(Option::is_none));
        }
        Ok(sealed)
    }

    pub(super) fn stage_active_block(
        &mut self,
        key: ActiveBlockKey,
        active: &ActiveBlock,
        placement: CompressionPlacement,
        score: CompressionBlockScore,
    ) -> TelemetryResult<BlockDescriptor> {
        let structural = encode_structural_records(&active.records)?;
        let structural_bytes = u64::try_from(structural.len()).unwrap_or(u64::MAX);
        let compressed = self.compressor.compress(
            &structural,
            key.dictionary_id,
            active.dictionary_payload.as_deref(),
        )?;
        let stored_bytes = u64::try_from(compressed.len()).unwrap_or(u64::MAX);
        if let Some(observer) = &self.realtime_dictionary {
            let _ = observer.observe_structural_block(key.placement_id, structural);
        }
        for pending in &active.records {
            if let Some(record) = self
                .partitions
                .get_mut(&pending.record.record_ref.topic_partition)
                .and_then(|partition| partition.record_mut(pending.record.record_ref.offset))
            {
                record.final_placement = Some(placement);
            }
        }
        Ok(self.catalog.seal(
            BlockDescriptor {
                block_id: BlockId::new(0),
                stream_shard_id: self.stream_shard_id,
                topic_partition: key.topic_partition,
                source_compression_cohort: key.source_compression_cohort,
                placement_id: key.placement_id,
                dictionary_id: key.dictionary_id,
                compression_codec: CompressionCodec::Zstd,
                compression_level: self.config.compression_level,
                first_offset: active.first_offset,
                last_offset: active.last_offset,
                record_count: active.record_count,
                source_bytes: active.source_bytes,
                structural_bytes,
                stored_bytes,
                min_timestamp_unix_nanos: active.min_timestamp_unix_nanos,
                max_timestamp_unix_nanos: active.max_timestamp_unix_nanos,
                compression_temperature: score.temperature.get(),
                compression_shape_hash: score.shape_hash,
                compression_temperature_variance_q8: score.internal_variance_q8,
                max_compression_temperature_deviation: score.max_deviation,
                object_key: None,
                object_offset: None,
            },
            Arc::from(compressed),
        ))
    }
}
