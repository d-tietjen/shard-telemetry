use super::*;

impl LogStripe {
    /// Creates a stripe owned by one physical shard-stream shard.
    pub fn new(stream_shard_id: ShardId, config: StripeConfig) -> TelemetryResult<Self> {
        config.validate()?;
        let compression_level = config.compression_level;
        let block_collator = CompressionBlockCollator::new(
            config.compression_locality.clone(),
            config.target_block_bytes,
        )?;
        Ok(Self {
            stream_shard_id,
            dictionary_cache: DictionaryCache::new(config.dictionary_cache_bytes)?,
            config,
            partitions: HashMap::new(),
            indexed_frame_partitions: HashMap::new(),
            indexed_frame_query_cache: Mutex::new(IndexedFrameQueryCache::default()),
            exact_posting_cache: Mutex::new(ExactPostingCache::default()),
            active_partition_cache: HashMap::new(),
            message_term_cache: std::iter::repeat_with(|| None)
                .take(MESSAGE_TERM_CACHE_ENTRIES)
                .collect(),
            field_cache: std::iter::repeat_with(|| None)
                .take(FIELD_CACHE_ENTRIES)
                .collect(),
            active_blocks: HashMap::new(),
            catalog: BlockCatalog::default(),
            placement_dictionaries: HashMap::new(),
            dictionary_catalog: None,
            dictionary_snapshot: None,
            dictionary_generation: 0,
            realtime_dictionary: None,
            block_collator,
            compressor: StripeCompressor::new(compression_level)?,
            tier: None,
            next_frame_id: 0,
        })
    }

    /// Creates a stripe that receives immutable dictionary publications from a
    /// shared control-plane catalog.
    pub fn with_dictionary_catalog(
        stream_shard_id: ShardId,
        config: StripeConfig,
        dictionary_catalog: Arc<DictionaryCatalog>,
    ) -> TelemetryResult<Self> {
        let mut stripe = Self::new(stream_shard_id, config)?;
        stripe.dictionary_catalog = Some(dictionary_catalog);
        stripe.refresh_dictionary_catalog()?;
        Ok(stripe)
    }

    /// Creates a stripe that contributes sealed blocks to a bounded real-time
    /// dictionary learner and adopts accepted immutable generations.
    pub fn with_realtime_dictionary(
        stream_shard_id: ShardId,
        config: StripeConfig,
        trainer: &RealtimeDictionaryTrainer,
    ) -> TelemetryResult<Self> {
        let mut stripe = Self::with_dictionary_catalog(stream_shard_id, config, trainer.catalog())?;
        stripe.realtime_dictionary = Some(trainer.observer());
        Ok(stripe)
    }

    /// Attaches a non-blocking real-time dictionary observer.
    ///
    /// The observer must publish into the same catalog configured for this
    /// stripe. A full learner queue drops only the observation, never the block.
    pub fn attach_realtime_dictionary(&mut self, observer: RealtimeDictionaryObserver) {
        self.realtime_dictionary = Some(observer);
    }

    /// Returns the shard-stream physical shard that owns this stripe.
    #[must_use]
    pub const fn stream_shard_id(&self) -> ShardId {
        self.stream_shard_id
    }

    /// Returns the visible indexed watermark for a partition.
    #[must_use]
    pub fn indexed_through(&self, topic_partition: TopicPartition) -> Option<LogicalOffset> {
        let record_watermark = self
            .partitions
            .get(&topic_partition)
            .and_then(|partition| partition.indexed_through);
        let frame_watermark = self
            .indexed_frame_partitions
            .get(&topic_partition)
            .and_then(|partition| partition.indexed_through);
        record_watermark.max(frame_watermark)
    }

    /// Returns the local catalog of sealed data blocks.
    #[must_use]
    pub const fn catalog(&self) -> &BlockCatalog {
        &self.catalog
    }

    /// Attaches partition-scoped immutable object catalogs and returns their
    /// durable recovery watermarks.
    pub(crate) fn attach_object_tier(
        &mut self,
        store: SharedTelemetryObjectStore,
        spool_directory: PathBuf,
        caches: (Arc<SsdObjectCache>, Arc<SsdObjectCache>),
        partitions: impl IntoIterator<Item = TopicPartition>,
        config: ObjectTierConfig,
        warm_local_cache_on_publish: bool,
    ) -> TelemetryResult<Vec<DurableSinkCheckpoint>> {
        if self.tier.is_some() {
            return Err(TelemetryError::InvalidConfig(
                "an object tier is already attached to this stripe",
            ));
        }
        let spool_directory = spool_directory.join(format!("shard-{}", self.stream_shard_id.get()));
        fs::create_dir_all(&spool_directory)
            .map_err(|error| TelemetryError::StorageIo(format!("create tier spool: {error}")))?;
        let mut tiers = HashMap::new();
        let mut checkpoints = Vec::new();
        let mut next_frame_id = self.next_frame_id;
        for partition in partitions {
            let object_tier =
                TelemetryObjectTier::open(store.clone(), self.stream_shard_id, partition, config)?;
            next_frame_id = next_frame_id.max(object_tier.root().next_block_id);
            if let Some(checkpoint) = object_tier.root().latest_checkpoint {
                checkpoints.push(DurableSinkCheckpoint {
                    topic_partition: partition,
                    next_placement_sequence: shard_stream_core::PlacementSequence::new(
                        checkpoint.next_placement_sequence,
                    ),
                    next_offset: LogicalOffset::new(checkpoint.next_offset),
                });
            }
            if tiers.insert(partition, object_tier).is_some() {
                return Err(TelemetryError::InvalidConfig(
                    "object tier contains a duplicate partition",
                ));
            }
        }
        if tiers.is_empty() {
            return Err(TelemetryError::InvalidConfig(
                "object tier requires at least one partition",
            ));
        }
        self.next_frame_id = next_frame_id;
        let (control_cache, payload_cache) = caches;
        self.tier = Some(StripeTierState {
            tiers,
            spool_directory,
            control_cache,
            payload_cache,
            warm_local_cache_on_publish,
            config,
        });
        Ok(checkpoints)
    }

    /// Returns compressed payload bytes that have not reached immutable object storage.
    #[must_use]
    pub(crate) fn retained_payload_bytes(&self) -> u64 {
        self.indexed_frame_partitions
            .values()
            .flat_map(|partition| &partition.appends)
            .flat_map(|append| &append.frames)
            .map(|frame| u64::try_from(frame.compressed.len()).unwrap_or(u64::MAX))
            .sum()
    }

    /// Returns the local catalog of sealed data blocks for offload bookkeeping.
    pub fn catalog_mut(&mut self) -> &mut BlockCatalog {
        &mut self.catalog
    }

    /// Returns the stripe-local cache of immutable compression dictionaries.
    #[must_use]
    pub const fn dictionary_cache(&self) -> &DictionaryCache {
        &self.dictionary_cache
    }

    /// Returns the stripe-local cache of immutable compression dictionaries.
    pub fn dictionary_cache_mut(&mut self) -> &mut DictionaryCache {
        &mut self.dictionary_cache
    }

    /// Returns the last immutable catalog generation observed by this stripe.
    #[must_use]
    pub const fn dictionary_generation(&self) -> u64 {
        self.dictionary_generation
    }

    /// Returns cumulative diagnostics from this stripe's block collator.
    #[must_use]
    pub fn compression_collation_stats(&self) -> CompressionLocalityStats {
        self.block_collator.stats()
    }

    /// Returns the final block placement once the record's block has sealed.
    #[must_use]
    pub fn final_compression_placement(
        &self,
        record_ref: TelemetryRecordRef,
    ) -> Option<CompressionPlacement> {
        self.partitions
            .get(&record_ref.topic_partition)
            .and_then(|partition| partition.record(record_ref.offset))
            .and_then(|record| record.final_placement)
    }

    /// Adopts control-plane state at an append boundary.
    pub fn begin_append_batch(&mut self) -> TelemetryResult<bool> {
        self.refresh_dictionary_catalog()
    }

    /// Adopts the latest immutable dictionary snapshot at a batch boundary.
    ///
    /// This is deliberately explicit: individual records only inspect the
    /// stripe-owned assignment map and LRU. The durable sink invokes it once
    /// before each append batch, while embedded callers can choose their own
    /// safe batch boundary.
    pub fn refresh_dictionary_catalog(&mut self) -> TelemetryResult<bool> {
        let Some(dictionary_catalog) = &self.dictionary_catalog else {
            return Ok(false);
        };
        let snapshot = dictionary_catalog.snapshot()?;
        if snapshot.generation() == self.dictionary_generation {
            return Ok(false);
        }

        self.placement_dictionaries.clear();
        for (placement_id, dictionary_id) in snapshot.assignments() {
            self.placement_dictionaries
                .insert(placement_id, dictionary_id);
        }
        self.dictionary_generation = snapshot.generation();
        self.dictionary_snapshot = Some(snapshot);
        Ok(true)
    }

    /// Installs an immutable dictionary for future blocks in a placement.
    ///
    /// Existing active blocks retain their previous dictionary identifier, so a
    /// dictionary rotation never makes already accepted log records ambiguous.
    pub fn install_dictionary(
        &mut self,
        placement_id: CompressionPlacementId,
        dictionary_id: DictionaryId,
        payload: Arc<[u8]>,
    ) -> TelemetryResult<DictionaryInsert> {
        if let Some(dictionary_catalog) = &self.dictionary_catalog {
            dictionary_catalog.publish(placement_id, dictionary_id, Arc::clone(&payload))?;
            self.refresh_dictionary_catalog()?;
        } else {
            self.placement_dictionaries
                .insert(placement_id, dictionary_id);
        }
        let insert = self.dictionary_cache.insert(dictionary_id, payload)?;
        Ok(insert)
    }
}
