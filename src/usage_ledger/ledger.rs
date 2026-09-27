use super::*;

impl EmbeddedUsageLedger {
    /// Opens or creates a ledger and exclusively locks its single data file.
    ///
    /// The maximum possible serialized state is calculated before the file is
    /// opened. Construction fails without creating the file if two crash-safe
    /// slots cannot fit within `max_file_bytes`.
    pub fn open(config: EmbeddedUsageLedgerConfig) -> Result<Self, LokiApiError> {
        let config = config.normalized()?;
        let maximum_state =
            PersistedState::maximum_sized(config.feature_ids.len(), config.monthly_buckets);
        let maximum_raw = rmp_serde::to_vec(&maximum_state).map_err(|error| {
            LokiApiError::internal(format!(
                "embedded usage ledger sizing serialization failed: {error}",
            ))
        })?;
        let max_raw_payload_bytes = maximum_raw.len();
        let slot_bytes = u64::try_from(HEADER_BYTES)
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(max_raw_payload_bytes).unwrap_or(u64::MAX));
        let file_bytes = slot_bytes.saturating_mul(SLOT_COUNT);
        if file_bytes > config.max_file_bytes {
            return Err(LokiApiError::configuration(format!(
                "embedded usage ledger requires {file_bytes} bytes for {} features and {} monthly buckets, exceeding the {}-byte hard quota",
                config.feature_ids.len(),
                config.monthly_buckets,
                config.max_file_bytes,
            )));
        }

        if let Some(parent) = config.path.parent() {
            std::fs::create_dir_all(parent).map_err(ledger_io)?;
        }
        let existing_metadata = match config.path.symlink_metadata() {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(ledger_io(error)),
        };
        if existing_metadata
            .as_ref()
            .is_some_and(|metadata| !metadata.is_file())
        {
            return Err(LokiApiError::configuration(format!(
                "embedded usage ledger path {} must be a regular file and not a symbolic link",
                config.path.display(),
            )));
        }
        let path_existed = existing_metadata.is_some();
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
        }
        if path_existed {
            options.create(false);
        } else {
            options.create_new(true);
        }
        let mut file = options.open(&config.path).map_err(ledger_io)?;
        if let Err(error) = file.try_lock_exclusive() {
            let error = LokiApiError::configuration(format!(
                "embedded usage ledger {} is already in use or cannot be locked: {error}",
                config.path.display(),
            ));
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                error,
            ));
        }
        let observed_bytes = match file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                return Err(clean_up_failed_open(
                    &config.path,
                    path_existed,
                    file,
                    ledger_io(error),
                ));
            }
        };
        let is_new = observed_bytes == 0;
        if !is_new && observed_bytes != file_bytes {
            let error = LokiApiError::configuration(format!(
                "embedded usage ledger {} has {observed_bytes} bytes but this configuration requires {file_bytes}",
                config.path.display(),
            ));
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                error,
            ));
        }
        if let Err(error) = file.allocate(file_bytes) {
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                ledger_io(error),
            ));
        }
        if is_new && let Err(error) = file.sync_all() {
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                ledger_io(error),
            ));
        }
        let initialize = if is_new {
            true
        } else {
            match slots_are_pristine(&mut file, slot_bytes) {
                Ok(pristine) => pristine,
                Err(error) => {
                    return Err(clean_up_failed_open(
                        &config.path,
                        path_existed,
                        file,
                        error,
                    ));
                }
            }
        };
        let allocated_file_bytes = match file.allocated_size() {
            Ok(bytes) => bytes,
            Err(error) => {
                return Err(clean_up_failed_open(
                    &config.path,
                    path_existed,
                    file,
                    ledger_io(error),
                ));
            }
        };
        if allocated_file_bytes < file_bytes {
            let error = LokiApiError::configuration(format!(
                "embedded usage ledger filesystem reserved only {allocated_file_bytes} of {file_bytes} required bytes",
            ));
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                error,
            ));
        }
        if allocated_file_bytes > config.max_file_bytes {
            let error = LokiApiError::configuration(format!(
                "embedded usage ledger requires {allocated_file_bytes} allocated filesystem bytes, exceeding the {}-byte hard quota",
                config.max_file_bytes,
            ));
            return Err(clean_up_failed_open(
                &config.path,
                path_existed,
                file,
                error,
            ));
        }

        let cleanup_path = config.path.clone();
        let feature_ids = Arc::new(config.feature_ids);
        let feature_indexes = feature_ids
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, feature_id)| (feature_id, index))
            .collect();
        let config_fingerprint = configuration_fingerprint(
            feature_ids.as_slice(),
            config.monthly_buckets,
            config.unknown_feature_policy,
        );
        let ledger = Self {
            path: config.path,
            feature_ids,
            feature_indexes,
            monthly_buckets: config.monthly_buckets,
            unknown_feature_policy: config.unknown_feature_policy,
            max_file_bytes: config.max_file_bytes,
            file_bytes,
            allocated_file_bytes,
            slot_bytes,
            max_raw_payload_bytes,
            config_fingerprint,
            inner: Mutex::new(LedgerInner {
                file,
                state: PersistedState::empty(0),
                active_slot: 1,
                generation: 0,
                current_generation_bytes: 0,
                compressed: false,
            }),
            checkpoint_failures: AtomicU64::new(0),
            rejected_updates: AtomicU64::new(0),
            rejected_unknown_features: AtomicU64::new(0),
            overflowed_unknown_events: AtomicU64::new(0),
            monthly_updates_outside_window: AtomicU64::new(0),
        };

        let ready = if initialize {
            ledger
                .inner
                .lock()
                .map_err(|_| ledger_lock_error())
                .and_then(|mut inner| {
                    inner.state = PersistedState::empty(ledger.feature_ids.len());
                    let mut initial = inner.state.clone();
                    initial.last_successful_checkpoint_unix_seconds = unix_seconds_now();
                    ledger.persist_locked(&mut inner, initial)
                })
        } else {
            ledger.recover().and_then(|recovered| {
                let mut inner = ledger.inner.lock().map_err(|_| ledger_lock_error())?;
                inner.state = recovered.state;
                inner.active_slot = recovered.slot;
                inner.generation = recovered.generation;
                inner.current_generation_bytes = recovered.payload_bytes;
                inner.compressed = recovered.compressed;
                Ok(())
            })
        };
        if let Err(error) = ready {
            drop(ledger);
            if !path_existed {
                let _ = std::fs::remove_file(cleanup_path);
            }
            return Err(error);
        }
        if !path_existed && let Err(error) = sync_parent_directory(&cleanup_path) {
            drop(ledger);
            let _ = std::fs::remove_file(&cleanup_path);
            let _ = sync_parent_directory(&cleanup_path);
            return Err(error);
        }
        Ok(ledger)
    }

    /// Adds one monotonic feature count at the current wall-clock time.
    pub fn record_feature(&self, feature_id: &str, increment: u64) -> Result<(), LokiApiError> {
        self.record_feature_at(feature_id, increment, unix_seconds_now())
    }

    /// Adds one monotonic feature count at an explicit Unix second.
    pub fn record_feature_at(
        &self,
        feature_id: &str,
        increment: u64,
        timestamp_unix_seconds: u64,
    ) -> Result<(), LokiApiError> {
        self.record_batch_at(timestamp_unix_seconds, 0, [(feature_id, increment)])
    }

    /// Adds monotonic active time at the current wall-clock time.
    pub fn record_active_seconds(&self, seconds: u64) -> Result<(), LokiApiError> {
        self.record_active_seconds_at(seconds, unix_seconds_now())
    }

    /// Adds monotonic active time at an explicit Unix second.
    pub fn record_active_seconds_at(
        &self,
        seconds: u64,
        timestamp_unix_seconds: u64,
    ) -> Result<(), LokiApiError> {
        self.record_batch_at(
            timestamp_unix_seconds,
            seconds,
            std::iter::empty::<(&str, u64)>(),
        )
    }

    /// Atomically records active seconds and feature increments at the current
    /// wall-clock time.
    pub fn record_batch<I, S>(
        &self,
        active_seconds: u64,
        feature_increments: I,
    ) -> Result<(), LokiApiError>
    where
        I: IntoIterator<Item = (S, u64)>,
        S: AsRef<str>,
    {
        self.record_batch_at(unix_seconds_now(), active_seconds, feature_increments)
    }

    /// Atomically records active seconds and any number of feature increments.
    ///
    /// Duplicate feature IDs in the iterator are added in order. Any rejected
    /// identifier or arithmetic overflow rejects the complete batch.
    pub fn record_batch_at<I, S>(
        &self,
        timestamp_unix_seconds: u64,
        active_seconds: u64,
        feature_increments: I,
    ) -> Result<(), LokiApiError>
    where
        I: IntoIterator<Item = (S, u64)>,
        S: AsRef<str>,
    {
        let mut increments = vec![0_u64; self.feature_ids.len()];
        let mut overflowed = 0_u64;
        let mut update_count = 0_usize;
        for (feature_id, increment) in feature_increments {
            update_count = update_count.saturating_add(1);
            if update_count > MAX_FEATURE_UPDATES_PER_BATCH {
                self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                return Err(LokiApiError::bad_request(format!(
                    "embedded usage batches support at most {MAX_FEATURE_UPDATES_PER_BATCH} feature updates",
                )));
            }
            if increment == 0 {
                continue;
            }
            let feature_id = feature_id.as_ref();
            if feature_id.len() > MAX_FEATURE_ID_BYTES {
                match self.unknown_feature_policy {
                    UnknownFeaturePolicy::Reject => {
                        self.rejected_unknown_features
                            .fetch_add(1, Ordering::Relaxed);
                        self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                        return Err(LokiApiError::bad_request(format!(
                            "unknown embedded usage feature IDs must contain at most {MAX_FEATURE_ID_BYTES} UTF-8 bytes",
                        )));
                    }
                    UnknownFeaturePolicy::AccumulateOverflow => {
                        overflowed = checked_add(overflowed, increment).inspect_err(|_| {
                            self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                        })?;
                        continue;
                    }
                }
            }
            if let Some(index) = self.feature_indexes.get(feature_id).copied() {
                increments[index] =
                    checked_add(increments[index], increment).inspect_err(|_| {
                        self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                    })?;
                continue;
            }
            match self.unknown_feature_policy {
                UnknownFeaturePolicy::Reject => {
                    self.rejected_unknown_features
                        .fetch_add(1, Ordering::Relaxed);
                    self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                    return Err(LokiApiError::bad_request(
                        "feature ID is not in the embedded usage registry",
                    ));
                }
                UnknownFeaturePolicy::AccumulateOverflow => {
                    overflowed = checked_add(overflowed, increment).inspect_err(|_| {
                        self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                    })?;
                }
            }
        }
        if active_seconds == 0 && overflowed == 0 && increments.iter().all(|value| *value == 0) {
            return Ok(());
        }
        let month_index = if self.monthly_buckets == 0 {
            0
        } else {
            calendar_month_index(timestamp_unix_seconds).inspect_err(|_| {
                self.rejected_updates.fetch_add(1, Ordering::Relaxed);
            })?
        };
        let mut inner = self.inner.lock().map_err(|_| ledger_lock_error())?;
        let mut staged = inner.state.clone();
        let month_position = self.prepare_month(&mut staged, month_index);

        staged.active_seconds =
            checked_add(staged.active_seconds, active_seconds).inspect_err(|_| {
                self.rejected_updates.fetch_add(1, Ordering::Relaxed);
            })?;
        if let Some(position) = month_position {
            staged.months[position].active_seconds =
                checked_add(staged.months[position].active_seconds, active_seconds).inspect_err(
                    |_| {
                        self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                    },
                )?;
        }

        for (index, increment) in increments.into_iter().enumerate() {
            if increment == 0 {
                continue;
            }
            staged.feature_counts[index] = checked_add(staged.feature_counts[index], increment)
                .inspect_err(|_| {
                    self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                })?;
            if let Some(position) = month_position {
                staged.months[position].feature_counts[index] =
                    checked_add(staged.months[position].feature_counts[index], increment)
                        .inspect_err(|_| {
                            self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                        })?;
            }
        }
        staged.overflow_feature_events = checked_add(staged.overflow_feature_events, overflowed)
            .inspect_err(|_| {
                self.rejected_updates.fetch_add(1, Ordering::Relaxed);
            })?;
        if let Some(position) = month_position {
            staged.months[position].overflow_feature_events =
                checked_add(staged.months[position].overflow_feature_events, overflowed)
                    .inspect_err(|_| {
                        self.rejected_updates.fetch_add(1, Ordering::Relaxed);
                    })?;
        }

        staged.last_successful_checkpoint_unix_seconds =
            unix_seconds_now().max(staged.last_successful_checkpoint_unix_seconds);
        if let Err(error) = self.persist_locked(&mut inner, staged) {
            self.checkpoint_failures.fetch_add(1, Ordering::Relaxed);
            self.rejected_updates.fetch_add(1, Ordering::Relaxed);
            return Err(error);
        }
        self.overflowed_unknown_events
            .fetch_add(overflowed, Ordering::Relaxed);
        Ok(())
    }

    /// Returns a consistent snapshot without accessing the data file.
    pub fn snapshot(&self) -> Result<EmbeddedUsageSnapshot, LokiApiError> {
        let inner = self.inner.lock().map_err(|_| ledger_lock_error())?;
        Ok(self.public_snapshot(&inner))
    }

    /// Returns quota, cardinality, durability, and rejection health counters.
    pub fn health(&self) -> Result<EmbeddedUsageHealth, LokiApiError> {
        let inner = self.inner.lock().map_err(|_| ledger_lock_error())?;
        Ok(EmbeddedUsageHealth {
            file_bytes: self.file_bytes,
            allocated_file_bytes: self.allocated_file_bytes,
            max_file_bytes: self.max_file_bytes,
            quota_headroom_bytes: self
                .max_file_bytes
                .saturating_sub(self.file_bytes.max(self.allocated_file_bytes)),
            current_generation_bytes: inner.current_generation_bytes,
            feature_count: self.feature_ids.len(),
            monthly_bucket_count: inner.state.months.len(),
            generation: inner.generation,
            last_successful_checkpoint_unix_seconds: inner
                .state
                .last_successful_checkpoint_unix_seconds,
            checkpoint_failures: self.checkpoint_failures.load(Ordering::Relaxed),
            rejected_updates: self.rejected_updates.load(Ordering::Relaxed),
            rejected_unknown_features: self.rejected_unknown_features.load(Ordering::Relaxed),
            overflowed_unknown_events: self.overflowed_unknown_events.load(Ordering::Relaxed),
            monthly_updates_outside_window: self
                .monthly_updates_outside_window
                .load(Ordering::Relaxed),
            pending_updates: 0,
            compressed: inner.compressed,
        })
    }

    fn prepare_month(&self, state: &mut PersistedState, month_index: u32) -> Option<usize> {
        if self.monthly_buckets == 0 {
            return None;
        }
        match state
            .months
            .binary_search_by_key(&month_index, |month| month.month_index)
        {
            Ok(position) => Some(position),
            Err(position) if state.months.len() < self.monthly_buckets => {
                state.months.insert(
                    position,
                    PersistedMonth {
                        month_index,
                        feature_counts: vec![0; self.feature_ids.len()],
                        active_seconds: 0,
                        overflow_feature_events: 0,
                    },
                );
                Some(position)
            }
            Err(0) => {
                self.monthly_updates_outside_window
                    .fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(position) => {
                state.months.remove(0);
                let adjusted = position - 1;
                state.months.insert(
                    adjusted,
                    PersistedMonth {
                        month_index,
                        feature_counts: vec![0; self.feature_ids.len()],
                        active_seconds: 0,
                        overflow_feature_events: 0,
                    },
                );
                Some(adjusted)
            }
        }
    }

    fn public_snapshot(&self, inner: &LedgerInner) -> EmbeddedUsageSnapshot {
        EmbeddedUsageSnapshot {
            features: self
                .feature_ids
                .iter()
                .cloned()
                .zip(inner.state.feature_counts.iter().copied())
                .map(|(feature_id, count)| FeatureUsage { feature_id, count })
                .collect(),
            active_seconds: inner.state.active_seconds,
            overflow_feature_events: inner.state.overflow_feature_events,
            months: inner
                .state
                .months
                .iter()
                .map(|month| {
                    let (year, calendar_month) = calendar_month(month.month_index);
                    MonthlyUsage {
                        year,
                        month: calendar_month,
                        features: self
                            .feature_ids
                            .iter()
                            .cloned()
                            .zip(month.feature_counts.iter().copied())
                            .map(|(feature_id, count)| FeatureUsage { feature_id, count })
                            .collect(),
                        active_seconds: month.active_seconds,
                        overflow_feature_events: month.overflow_feature_events,
                    }
                })
                .collect(),
            generation: inner.generation,
            last_successful_checkpoint_unix_seconds: inner
                .state
                .last_successful_checkpoint_unix_seconds,
        }
    }

    fn persist_locked(
        &self,
        inner: &mut LedgerInner,
        state: PersistedState,
    ) -> Result<(), LokiApiError> {
        validate_state(&state, self.feature_ids.len(), self.monthly_buckets)?;
        let raw = rmp_serde::to_vec(&state).map_err(|error| {
            LokiApiError::internal(format!(
                "embedded usage ledger serialization failed: {error}",
            ))
        })?;
        if raw.len() > self.max_raw_payload_bytes {
            return Err(LokiApiError::configuration(
                "embedded usage ledger state exceeds its precomputed hard quota",
            ));
        }
        let compressed = zstd::bulk::compress(&raw, ZSTD_LEVEL).map_err(|error| {
            LokiApiError::internal(format!("embedded usage ledger compression failed: {error}",))
        })?;
        let (codec, payload) = if compressed.len() < raw.len() {
            (CODEC_ZSTD, compressed.as_slice())
        } else {
            (CODEC_RAW, raw.as_slice())
        };
        if payload.len() > self.max_raw_payload_bytes {
            return Err(LokiApiError::configuration(
                "embedded usage ledger encoded state exceeds its fixed slot",
            ));
        }
        let generation = inner
            .generation
            .checked_add(1)
            .ok_or_else(|| LokiApiError::internal("embedded usage ledger generation exhausted"))?;
        let target_slot = (inner.active_slot + 1) % usize::try_from(SLOT_COUNT).unwrap_or(2);
        let slot_offset = self
            .slot_bytes
            .saturating_mul(u64::try_from(target_slot).unwrap_or(u64::MAX));
        inner
            .file
            .seek(SeekFrom::Start(slot_offset.saturating_add(
                u64::try_from(HEADER_BYTES).unwrap_or(u64::MAX),
            )))
            .and_then(|_| inner.file.write_all(payload))
            .and_then(|_| inner.file.sync_data())
            .map_err(ledger_io)?;
        let header = encode_header(
            codec,
            generation,
            payload.len(),
            raw.len(),
            &self.config_fingerprint,
            crc32c::crc32c(payload),
        )?;
        inner
            .file
            .seek(SeekFrom::Start(slot_offset))
            .and_then(|_| inner.file.write_all(&header))
            .and_then(|_| inner.file.sync_all())
            .map_err(ledger_io)?;
        inner.state = state;
        inner.active_slot = target_slot;
        inner.generation = generation;
        inner.current_generation_bytes =
            u64::try_from(HEADER_BYTES + payload.len()).unwrap_or(u64::MAX);
        inner.compressed = codec == CODEC_ZSTD;
        Ok(())
    }

    fn recover(&self) -> Result<RecoveredSlot, LokiApiError> {
        let mut inner = self.inner.lock().map_err(|_| ledger_lock_error())?;
        let mut recovered = Vec::new();
        let mut observed_other_configuration = false;
        for slot in 0..usize::try_from(SLOT_COUNT).unwrap_or(2) {
            match read_slot(
                &mut inner.file,
                slot,
                self.slot_bytes,
                self.max_raw_payload_bytes,
                &self.config_fingerprint,
                self.feature_ids.len(),
                self.monthly_buckets,
            )? {
                SlotRead::Valid(slot) => recovered.push(slot),
                SlotRead::OtherConfiguration => observed_other_configuration = true,
                SlotRead::Invalid => {}
            }
        }
        recovered
            .into_iter()
            .max_by_key(|slot| slot.generation)
            .ok_or_else(|| {
                if observed_other_configuration {
                    LokiApiError::configuration(
                        "embedded usage ledger feature registry or monthly policy changed",
                    )
                } else {
                    LokiApiError::internal(
                        "embedded usage ledger contains no valid checksummed generation",
                    )
                }
            })
    }
}
