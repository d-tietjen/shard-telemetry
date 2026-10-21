use super::*;

impl TelemetrySinkFactory {
    /// Creates a factory with one stripe available for each physical shard.
    pub fn new(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: OtlpSinkConfig,
    ) -> TelemetryResult<Self> {
        Self::new_with_optional_dictionary_catalog(shard_ids, config, None, None)
    }

    /// Creates a sink factory whose stripe workers adopt immutable dictionary
    /// publications once per durable append batch.
    pub fn with_dictionary_catalog(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: OtlpSinkConfig,
        dictionary_catalog: Arc<DictionaryCatalog>,
    ) -> TelemetryResult<Self> {
        Self::new_with_optional_dictionary_catalog(
            shard_ids,
            config,
            Some(dictionary_catalog),
            None,
        )
    }

    /// Creates sink workers that continuously sample sealed blocks and adopt
    /// admitted immutable dictionary generations at durable append boundaries.
    pub fn with_realtime_dictionary(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: OtlpSinkConfig,
        trainer: &RealtimeDictionaryTrainer,
    ) -> TelemetryResult<Self> {
        Self::new_with_optional_dictionary_catalog(
            shard_ids,
            config,
            Some(trainer.catalog()),
            Some(trainer.observer()),
        )
    }

    fn new_with_optional_dictionary_catalog(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: OtlpSinkConfig,
        dictionary_catalog: Option<Arc<DictionaryCatalog>>,
        realtime_dictionary: Option<RealtimeDictionaryObserver>,
    ) -> TelemetryResult<Self> {
        config.validate()?;
        let mut available = HashMap::new();
        let mut recovered_checkpoints = HashMap::new();
        let mut recovered_transactions = Vec::new();
        let mut journals = HashMap::new();
        let object_store = config.object_tier.as_ref().map(|tier| tier.store.clone());
        let tier_caches = config
            .object_tier
            .as_ref()
            .map(|tier| {
                Ok::<_, TelemetryError>(TierCaches {
                    control: Arc::new(SsdObjectCache::open(
                        &tier.control_cache_directory,
                        tier.control_cache,
                    )?),
                    payload: Arc::new(SsdObjectCache::open(
                        &tier.payload_cache_directory,
                        tier.payload_cache,
                    )?),
                })
            })
            .transpose()?;
        for shard_id in shard_ids {
            let mut logs = match &dictionary_catalog {
                Some(dictionary_catalog) => LogStripe::with_dictionary_catalog(
                    shard_id,
                    config.stripe.clone(),
                    Arc::clone(dictionary_catalog),
                )?,
                None => LogStripe::new(shard_id, config.stripe.clone())?,
            };
            logs.set_query_work_limits(config.native_query_work);
            if let Some(observer) = &realtime_dictionary {
                logs.attach_realtime_dictionary(observer.clone());
            }
            if let (Some(tier), Some(caches)) = (&config.object_tier, &tier_caches) {
                let log_partitions = tier
                    .partitions
                    .iter()
                    .copied()
                    .filter(|partition| partition.topic_id == TelemetrySignal::Logs.topic_id())
                    .collect::<Vec<_>>();
                if !log_partitions.is_empty() {
                    for checkpoint in logs.attach_object_tier(
                        tier.store.clone(),
                        tier.spool_directory.clone(),
                        (Arc::clone(&caches.control), Arc::clone(&caches.payload)),
                        log_partitions,
                        tier.tier,
                        tier.warm_local_cache_on_publish,
                    )? {
                        merge_recovered_checkpoint(&mut recovered_checkpoints, checkpoint)?;
                    }
                }
            }
            if let Some(directory) = &config.state_directory {
                let (journal, recovered) = SinkJournal::open(
                    directory,
                    shard_id,
                    config.max_journal_bytes,
                    config.journal_sync_each_append,
                )?;
                recovered_transactions.extend(
                    recovered
                        .into_iter()
                        .map(|transaction| (shard_id, transaction)),
                );
                journals.insert(shard_id, Arc::new(journal));
            }
            let mut signal_tiers = HashMap::new();
            let mut metric_recovery_states = Vec::new();
            if let (Some(tier), Some(caches)) = (&config.object_tier, &tier_caches) {
                for signal in [TelemetrySignal::Traces, TelemetrySignal::Metrics] {
                    let mut native_tier = tier.tier;
                    if let Some(resident) = config.native_resident {
                        native_tier.target_group_payload_bytes = native_tier
                            .target_group_payload_bytes
                            .min(resident.publication_target_bytes());
                        native_tier.max_group_payload_bytes = native_tier
                            .max_group_payload_bytes
                            .min(resident.max_payload_bytes_per_partition);
                    }
                    let partitions = tier
                        .partitions
                        .iter()
                        .copied()
                        .filter(|partition| partition.topic_id == signal.topic_id())
                        .collect::<Vec<_>>();
                    if let Some(opened) = SignalTierState::open(
                        signal,
                        shard_id,
                        tier.store.clone(),
                        tier.spool_directory.clone(),
                        caches,
                        partitions,
                        (native_tier, tier.warm_local_cache_on_publish),
                    )? {
                        for checkpoint in opened.checkpoints {
                            merge_recovered_checkpoint(&mut recovered_checkpoints, checkpoint)?;
                        }
                        metric_recovery_states.extend(opened.recovery_states);
                        signal_tiers.insert(signal, opened.state);
                    }
                }
            }
            let mut metrics =
                MetricStripe::new(config.signals.metrics.head_memory_bytes_per_stripe)?;
            for recovery_state in metric_recovery_states {
                metrics.restore_accumulator_checkpoints(&recovery_state)?;
            }
            let mut traces = TraceStripe::new(config.signals.traces.head_memory_bytes_per_stripe)?;
            traces.set_query_work_limits(config.native_query_work);
            metrics.set_query_work_limits(config.native_query_work);
            traces.set_resident_payload_limit(
                config
                    .native_resident
                    .map(|resident| resident.max_payload_bytes_per_partition),
            );
            metrics.set_resident_payload_limit(
                config
                    .native_resident
                    .map(|resident| resident.max_payload_bytes_per_partition),
            );
            let stripe = TelemetryStripeState {
                stream_shard_id: shard_id,
                logs,
                traces,
                metrics,
                correlations: CorrelationIndex::new(config.correlations),
                log_partitions: config.signals.logs.logical_partitions.get(),
                signal_tiers,
                indexed_publications: HashMap::new(),
                router: TelemetryRouter::from_config(&config.signals),
                native_query_work: config.native_query_work,
                native_resident: config.native_resident,
            };
            if available.insert(shard_id, stripe).is_some() {
                return Err(TelemetryError::DuplicateStripe(shard_id));
            }
        }
        if available.is_empty() {
            return Err(TelemetryError::InvalidConfig(
                "log sink requires at least one shard",
            ));
        }
        recovered_transactions.sort_unstable_by_key(|(_, transaction)| {
            (
                transaction.expected.topic_partition.topic_id,
                transaction.expected.topic_partition.partition_id,
                transaction.expected.next_placement_sequence,
            )
        });
        for (shard_id, transaction) in recovered_transactions {
            let actual = recovered_checkpoints
                .get(&transaction.expected.topic_partition)
                .copied()
                .unwrap_or_else(|| {
                    DurableSinkCheckpoint::initial(transaction.expected.topic_partition)
                });
            if checkpoint_covers(actual, transaction.next) {
                continue;
            }
            if !checkpoint_allows_lane_gap(actual, transaction.expected) {
                return Err(TelemetryError::CorruptSinkJournal(
                    "recovered checkpoint chain conflicts across stripes".into(),
                ));
            }
            let stripe = available
                .get_mut(&shard_id)
                .ok_or(TelemetryError::UnknownStripe(shard_id))?;
            for append in &transaction.appends {
                index_payload(
                    stripe,
                    append.topic_partition,
                    append.first_offset,
                    None,
                    &append.payload,
                    None,
                    None,
                    false,
                    (transaction.expected, transaction.next),
                )?;
            }
            recovered_checkpoints.insert(transaction.next.topic_partition, transaction.next);
        }
        let validated_signal_cache = if let Some(resident) = config.native_resident {
            let owners = u64::from(
                config
                    .signals
                    .traces
                    .physical_stripes
                    .get()
                    .max(config.signals.metrics.physical_stripes.get()),
            );
            ValidatedSignalCache::bounded(resident.cache_bytes(owners))
        } else {
            ValidatedSignalCache::default()
        };
        Ok(Self {
            config,
            available: Mutex::new(available),
            checkpoints: Arc::new(Mutex::new(recovered_checkpoints)),
            journals: Mutex::new(journals),
            query_workers: Arc::new(RwLock::new(QueryWorkerRegistry::default())),
            correlation_buffers: Arc::new(Mutex::new(CorrelationBufferPool::default())),
            active_log_partition_cache: Arc::new(Mutex::new(HashMap::new())),
            validated_signal_cache: Arc::new(validated_signal_cache),
            native_validation: Mutex::new(()),
            tier_caches,
            object_store,
        })
    }

    /// Returns a cloneable coordinator for querying the owner-only stripe
    /// workers after they have been opened by shard-stream.
    #[must_use]
    pub fn service(&self) -> TelemetryService {
        TelemetryService {
            max_ingest_bytes: self.config.native_resident.map_or(
                self.config.signals.max_otlp_request_bytes,
                |resident| {
                    self.config
                        .signals
                        .max_otlp_request_bytes
                        .min(resident.append_work.max_encoded_block_bytes)
                },
            ),
            native_query_work: self.config.native_query_work,
            native_resident: self.config.native_resident,
            workers: Arc::clone(&self.query_workers),
            correlation_buffers: Arc::clone(&self.correlation_buffers),
            active_log_partition_cache: Arc::clone(&self.active_log_partition_cache),
            tier_caches: self.tier_caches.clone(),
            object_store: self.object_store.clone(),
            router: TelemetryRouter::from_config(&self.config.signals),
        }
    }
}

impl DurableAppendSinkFactory for TelemetrySinkFactory {
    fn validate_append(&self, payload: &[u8], record_count: NonZeroU32) -> EngineResult<()> {
        let _materialization = if let Some(resident) = self.config.native_resident {
            if payload.len() > resident.append_work.max_encoded_block_bytes {
                return Err(EngineError::InvalidConfig(
                    "native append exceeds the resident encoded input allowance".into(),
                ));
            }
            resident
                .append_work
                .rows(record_count.get() as usize)
                .map_err(log_error_to_engine)?;
            resident
                .append_work
                .winners(record_count.get() as usize)
                .map_err(log_error_to_engine)?;
            Some(self.native_validation.lock().map_err(|_| {
                EngineError::DurableSinkUnavailable("native append preflight lock poisoned".into())
            })?)
        } else {
            None
        };
        if !TelemetryEnvelope::is_encoded(payload) {
            return Err(EngineError::InvalidConfig(
                "durable telemetry appends require the STEL envelope".into(),
            ));
        }
        let envelope = crate::envelope::TelemetryEnvelope::decode_view(payload)
            .map_err(log_error_to_engine)?;
        if envelope.item_count != record_count.get() {
            return Err(EngineError::InvalidConfig(format!(
                "STEL envelope contains {} items, request reserved {}",
                envelope.item_count,
                record_count.get()
            )));
        }
        let decoded_count = match envelope.signal {
            TelemetrySignal::Logs => {
                if let Some(resident) = self.config.native_resident {
                    crate::ingest_pack::validate_ingest_pack_with_work_limits(
                        envelope.payload,
                        envelope.item_count,
                        Some(resident.append_work),
                    )
                } else {
                    validate_ingest_pack(envelope.payload, envelope.item_count)
                }
                .map_err(log_error_to_engine)?;
                envelope.item_count as usize
            }
            TelemetrySignal::Traces => {
                let records = if let Some(resident) = self.config.native_resident {
                    crate::trace::decode_trace_block_with_work_limits(
                        envelope.payload,
                        None,
                        Some(resident.append_work),
                    )
                } else {
                    decode_trace_block(envelope.payload)
                }
                .map_err(log_error_to_engine)?;
                let charge = if let Some(resident) = self.config.native_resident {
                    resident
                        .charge_records(&records, |record| {
                            crate::encode_trace_block(std::slice::from_ref(record))
                        })
                        .map_err(log_error_to_engine)?
                } else {
                    envelope.payload.len()
                };
                let decoded_count = records.len();
                if decoded_count == envelope.item_count as usize {
                    self.validated_signal_cache.insert(
                        envelope.checksum,
                        charge,
                        ValidatedSignalPayload::Traces(records),
                    );
                }
                decoded_count
            }
            TelemetrySignal::Metrics => {
                let records = if let Some(resident) = self.config.native_resident {
                    crate::metric::decode_metric_chunk_with_work_limits(
                        envelope.payload,
                        Some(resident.append_work),
                    )
                } else {
                    decode_metric_chunk(envelope.payload)
                }
                .map_err(log_error_to_engine)?;
                let charge = if let Some(resident) = self.config.native_resident {
                    resident
                        .charge_records(&records, |record| {
                            crate::encode_metric_chunk(std::slice::from_ref(record))
                        })
                        .map_err(log_error_to_engine)?
                } else {
                    envelope.payload.len()
                };
                let decoded_count = records.len();
                if decoded_count == envelope.item_count as usize {
                    self.validated_signal_cache.insert(
                        envelope.checksum,
                        charge,
                        ValidatedSignalPayload::Metrics(records),
                    );
                }
                decoded_count
            }
        };
        if decoded_count != envelope.item_count as usize {
            return Err(EngineError::InvalidConfig(
                "STEL signal payload item count mismatch".into(),
            ));
        }
        Ok(())
    }

    fn load_checkpoint(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<Option<DurableSinkCheckpoint>> {
        self.checkpoints
            .lock()
            .map(|checkpoints| checkpoints.get(&topic_partition).copied())
            .map_err(|_| {
                EngineError::DurableSinkUnavailable(
                    "shard-telemetry checkpoint lock poisoned".into(),
                )
            })
    }

    #[cfg(unix)]
    fn load_checkpoint_until(
        &self,
        topic_partition: TopicPartition,
        work: &shard_stream_engine::NativeResumeWork,
    ) -> shard_stream_engine::NativeResumeCallbackResult<Option<DurableSinkCheckpoint>> {
        super::bounded_callback::load_checkpoint_until(&self.checkpoints, topic_partition, work)
    }

    fn open_shard(&self, shard_id: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
        let stripe = self
            .available
            .lock()
            .map_err(|_| {
                EngineError::CorruptState("shard-telemetry sink factory lock poisoned".into())
            })?
            .remove(&shard_id)
            .ok_or(EngineError::UnknownShard(shard_id))?;
        let (sender, receiver) = sync_channel(self.config.queue_slots);
        let checkpoints = Arc::clone(&self.checkpoints);
        let journal = self
            .journals
            .lock()
            .map_err(|_| EngineError::CorruptState("shard-telemetry journal lock poisoned".into()))?
            .remove(&shard_id);
        let active_log_partition_cache = Arc::clone(&self.active_log_partition_cache);
        let validated_signal_cache = Arc::clone(&self.validated_signal_cache);
        let worker = thread::Builder::new()
            .name(format!("shard-telemetry-index-{shard_id}"))
            .spawn(move || {
                run_sink_worker(
                    stripe,
                    checkpoints,
                    journal,
                    active_log_partition_cache,
                    validated_signal_cache,
                    receiver,
                )
            })
            .map_err(|error| {
                EngineError::InvalidConfig(format!("failed to spawn shard-telemetry sink: {error}"))
            })?;
        self.query_workers
            .write()
            .map_err(|_| {
                EngineError::CorruptState("shard-telemetry query registry poisoned".into())
            })?
            .insert(shard_id, sender.clone());
        Ok(Arc::new(ShardTelemetryStripeSink {
            state: ShardSinkOwner::Legacy(Arc::new(SinkState {
                shard_id,
                sender: Mutex::new(Some(sender)),
                worker: Mutex::new(Some(worker)),
                query_workers: Arc::clone(&self.query_workers),
            })),
        }))
    }
}
