use super::*;

impl SsdObjectCache {
    /// Opens an SSD cache and reconstructs its bounded local directory.
    pub fn open(root: impl AsRef<Path>, config: SsdCacheConfig) -> TelemetryResult<Self> {
        config.validate()?;
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|error| storage_io("create SSD cache", error))?;
        let mut state = CacheState::default();
        let entries = fs::read_dir(&root).map_err(|error| storage_io("scan SSD cache", error))?;
        for entry in entries {
            let entry = entry.map_err(|error| storage_io("read SSD cache entry", error))?;
            let path = entry.path();
            let Some(name) = path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| name.ends_with(".chunk") && name.len() == 70)
            else {
                continue;
            };
            let metadata = entry
                .metadata()
                .map_err(|error| storage_io("inspect SSD cache entry", error))?;
            if !metadata.is_file() {
                continue;
            }
            let bytes = metadata.len();
            state.used_bytes = state.used_bytes.saturating_add(bytes);
            state.entries.insert(
                name[..64].to_owned(),
                CacheEntry {
                    path,
                    bytes,
                    stamp: 0,
                },
            );
        }
        let cache = Self {
            root,
            config,
            state: Mutex::new(state),
        };
        cache.evict_to_budget()?;
        Ok(cache)
    }

    /// Returns currently occupied cache bytes.
    #[must_use]
    pub fn used_bytes(&self) -> u64 {
        self.state.lock().map(|state| state.used_bytes).unwrap_or(0)
    }

    /// Returns bounded cache occupancy and read-amplification counters.
    #[must_use]
    pub fn stats(&self) -> SsdCacheStats {
        self.state.lock().map_or_else(
            |_| SsdCacheStats::default(),
            |state| SsdCacheStats {
                entries: state.entries.len(),
                used_bytes: state.used_bytes,
                hits: state.hits,
                memory_hits: state.memory_hits,
                parsed_hits: state.parsed_hits,
                memory_entries: state
                    .memory_entries
                    .len()
                    .saturating_add(state.parsed_entries.len()),
                memory_used_bytes: state
                    .memory_used_bytes
                    .saturating_add(state.parsed_used_bytes),
                parsed_entries: state.parsed_entries.len(),
                parsed_used_bytes: state.parsed_used_bytes,
                misses: state.misses,
                source_bytes: state.source_bytes,
            },
        )
    }

    /// Returns the largest object extent accepted by one cache operation.
    #[must_use]
    pub const fn max_read_bytes(&self) -> u64 {
        self.config.max_read_bytes
    }

    /// Admits a newly published immutable artifact directly from its local
    /// staging file.
    ///
    /// S3-backed embedded deployments use this write-through path so the most
    /// recently published payloads and indexes remain locally queryable without
    /// first downloading them. The ordinary LRU budget expels older chunks.
    pub fn admit_file(&self, artifact: &TierArtifact, source: &Path) -> TelemetryResult<()> {
        let source_metadata = hash_file(source)?;
        if source_metadata.bytes != artifact.bytes
            || source_metadata.content_digest != artifact.checksum
        {
            return Err(TelemetryError::CorruptTier(
                "published artifact source changed before SSD-cache admission".into(),
            ));
        }
        let mut file = File::open(source)
            .map_err(|error| storage_io("open SSD-cache admission source", error))?;
        let mut chunk_index = 0_u64;
        let mut remaining = artifact.bytes;
        while remaining > 0 {
            let chunk_bytes = remaining.min(self.config.chunk_bytes);
            let chunk_len = usize::try_from(chunk_bytes).map_err(|_| {
                TelemetryError::StorageIo("SSD-cache admission chunk cannot fit in memory".into())
            })?;
            let mut bytes = vec![0; chunk_len];
            file.read_exact(&mut bytes)
                .map_err(|error| storage_io("read SSD-cache admission source", error))?;
            let cache_key = checksum_bytes(
                format!(
                    "{}\0{}\0{chunk_index}",
                    artifact.object_key, artifact.checksum
                )
                .as_bytes(),
            );
            self.install_chunk(&cache_key, &bytes)?;
            remaining -= chunk_bytes;
            chunk_index = chunk_index.saturating_add(1);
        }
        Ok(())
    }

    /// Reads an object range, filling and reusing fixed immutable SSD chunks.
    pub fn read_range<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        range: Range<u64>,
    ) -> TelemetryResult<Vec<u8>> {
        if range.start > range.end || range.end - range.start > self.config.max_read_bytes {
            return Err(TelemetryError::ObjectStore(
                "SSD cache read range is invalid or exceeds its limit".into(),
            ));
        }
        let metadata = store.head(object_key)?.ok_or_else(|| {
            TelemetryError::ObjectStore(format!("object {object_key} does not exist"))
        })?;
        self.read_range_with_metadata(store, object_key, &metadata, range)
    }

    /// Reads an object range using immutable metadata already held in a
    /// manifest, avoiding a remote HEAD request on the query path.
    pub fn read_range_with_metadata<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        range: Range<u64>,
    ) -> TelemetryResult<Vec<u8>> {
        if range.start > range.end || range.end - range.start > self.config.max_read_bytes {
            return Err(TelemetryError::ObjectStore(
                "SSD cache read range is invalid or exceeds its limit".into(),
            ));
        }
        if range.end > metadata.bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "SSD cache range exceeds object {object_key}"
            )));
        }
        if range.is_empty() {
            return Ok(Vec::new());
        }
        let output_bytes = usize::try_from(range.end - range.start).map_err(|_| {
            TelemetryError::ObjectStore("SSD cache read cannot fit in memory".into())
        })?;
        let mut output = Vec::with_capacity(output_bytes);
        let first_chunk = range.start / self.config.chunk_bytes;
        let last_chunk = (range.end - 1) / self.config.chunk_bytes;
        for chunk_index in first_chunk..=last_chunk {
            let chunk_start = chunk_index
                .checked_mul(self.config.chunk_bytes)
                .ok_or_else(|| TelemetryError::ObjectStore("cache chunk offset overflow".into()))?;
            let chunk_end = chunk_start
                .saturating_add(self.config.chunk_bytes)
                .min(metadata.bytes);
            let chunk = self.load_or_fetch_chunk(
                store,
                object_key,
                metadata,
                chunk_index,
                chunk_start..chunk_end,
            )?;
            let copy_start = range.start.max(chunk_start) - chunk_start;
            let copy_end = range.end.min(chunk_end) - chunk_start;
            let copy_start = usize::try_from(copy_start)
                .map_err(|_| TelemetryError::ObjectStore("cache slice offset overflow".into()))?;
            let copy_end = usize::try_from(copy_end)
                .map_err(|_| TelemetryError::ObjectStore("cache slice offset overflow".into()))?;
            output.extend_from_slice(&chunk[copy_start..copy_end]);
        }
        Ok(output)
    }

    /// Reads sorted, non-overlapping immutable ranges while retaining the
    /// most recently loaded cache chunk for the complete batch.
    ///
    /// Payload packs place block and frame extents in ascending order. A
    /// batched query therefore reads or fetches each shared SSD chunk once
    /// instead of reopening that chunk for every selected extent.
    pub fn read_ranges_with_metadata<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        ranges: &[Range<u64>],
    ) -> TelemetryResult<Vec<Vec<u8>>> {
        if ranges.windows(2).any(|pair| pair[0].end > pair[1].start) {
            return Err(TelemetryError::ObjectStore(
                "batched SSD cache ranges must be sorted and non-overlapping".into(),
            ));
        }
        let mut outputs = Vec::with_capacity(ranges.len());
        let mut loaded = None::<(u64, u64, Vec<u8>)>;
        for range in ranges {
            if range.start > range.end
                || range.end > metadata.bytes
                || range.end - range.start > self.config.max_read_bytes
            {
                return Err(TelemetryError::ObjectStore(
                    "batched SSD cache range is invalid or exceeds its limit".into(),
                ));
            }
            let output_bytes = usize::try_from(range.end - range.start).map_err(|_| {
                TelemetryError::ObjectStore("SSD cache read cannot fit in memory".into())
            })?;
            let mut output = Vec::with_capacity(output_bytes);
            if !range.is_empty() {
                let first_chunk = range.start / self.config.chunk_bytes;
                let last_chunk = (range.end - 1) / self.config.chunk_bytes;
                for chunk_index in first_chunk..=last_chunk {
                    let chunk_start = chunk_index
                        .checked_mul(self.config.chunk_bytes)
                        .ok_or_else(|| {
                            TelemetryError::ObjectStore("cache chunk offset overflow".into())
                        })?;
                    let chunk_end = chunk_start
                        .saturating_add(self.config.chunk_bytes)
                        .min(metadata.bytes);
                    if loaded
                        .as_ref()
                        .is_none_or(|(index, _, _)| *index != chunk_index)
                    {
                        loaded = Some((
                            chunk_index,
                            chunk_start,
                            self.load_or_fetch_chunk(
                                store,
                                object_key,
                                metadata,
                                chunk_index,
                                chunk_start..chunk_end,
                            )?,
                        ));
                    }
                    let (_, loaded_start, chunk) =
                        loaded.as_ref().expect("requested cache chunk was loaded");
                    let copy_start = usize::try_from(range.start.max(chunk_start) - *loaded_start)
                        .map_err(|_| {
                            TelemetryError::ObjectStore("cache slice offset overflow".into())
                        })?;
                    let copy_end = usize::try_from(range.end.min(chunk_end) - *loaded_start)
                        .map_err(|_| {
                            TelemetryError::ObjectStore("cache slice offset overflow".into())
                        })?;
                    output.extend_from_slice(&chunk[copy_start..copy_end]);
                }
            }
            outputs.push(output);
        }
        Ok(outputs)
    }

    /// Reads sorted immutable ranges as shared verified chunk slices.
    ///
    /// Ranges contained in one cache chunk allocate no payload copy after the
    /// chunk has been admitted to RAM. A range spanning chunks is assembled
    /// into one owned shared buffer.
    pub fn read_shared_ranges_with_metadata<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        ranges: &[Range<u64>],
    ) -> TelemetryResult<Vec<CachedObjectRange>> {
        if ranges.windows(2).any(|pair| pair[0].end > pair[1].start) {
            return Err(TelemetryError::ObjectStore(
                "shared SSD cache ranges must be sorted and non-overlapping".into(),
            ));
        }
        let mut outputs = Vec::with_capacity(ranges.len());
        let mut loaded = None::<(u64, u64, Arc<[u8]>)>;
        for range in ranges {
            if range.start > range.end
                || range.end > metadata.bytes
                || range.end - range.start > self.config.max_read_bytes
            {
                return Err(TelemetryError::ObjectStore(
                    "shared SSD cache range is invalid or exceeds its limit".into(),
                ));
            }
            if range.is_empty() {
                outputs.push(CachedObjectRange::empty());
                continue;
            }
            let first_chunk = range.start / self.config.chunk_bytes;
            let last_chunk = (range.end - 1) / self.config.chunk_bytes;
            if first_chunk == last_chunk {
                let chunk_start = first_chunk
                    .checked_mul(self.config.chunk_bytes)
                    .ok_or_else(|| {
                        TelemetryError::ObjectStore("cache chunk offset overflow".into())
                    })?;
                let chunk_end = chunk_start
                    .saturating_add(self.config.chunk_bytes)
                    .min(metadata.bytes);
                if loaded
                    .as_ref()
                    .is_none_or(|(index, _, _)| *index != first_chunk)
                {
                    loaded = Some((
                        first_chunk,
                        chunk_start,
                        self.load_or_fetch_shared_chunk(
                            store,
                            object_key,
                            metadata,
                            first_chunk,
                            chunk_start..chunk_end,
                        )?,
                    ));
                }
                let (_, loaded_start, chunk) =
                    loaded.as_ref().expect("requested cache chunk was loaded");
                let start = usize::try_from(range.start - *loaded_start).map_err(|_| {
                    TelemetryError::ObjectStore("cache slice offset overflow".into())
                })?;
                let end = usize::try_from(range.end - *loaded_start).map_err(|_| {
                    TelemetryError::ObjectStore("cache slice offset overflow".into())
                })?;
                outputs.push(CachedObjectRange {
                    bytes: Arc::clone(chunk),
                    start,
                    end,
                });
                continue;
            }

            let output_bytes = usize::try_from(range.end - range.start).map_err(|_| {
                TelemetryError::ObjectStore("SSD cache read cannot fit in memory".into())
            })?;
            let mut output = Vec::with_capacity(output_bytes);
            for chunk_index in first_chunk..=last_chunk {
                let chunk_start = chunk_index
                    .checked_mul(self.config.chunk_bytes)
                    .ok_or_else(|| {
                        TelemetryError::ObjectStore("cache chunk offset overflow".into())
                    })?;
                let chunk_end = chunk_start
                    .saturating_add(self.config.chunk_bytes)
                    .min(metadata.bytes);
                let chunk = self.load_or_fetch_shared_chunk(
                    store,
                    object_key,
                    metadata,
                    chunk_index,
                    chunk_start..chunk_end,
                )?;
                let copy_start = usize::try_from(range.start.max(chunk_start) - chunk_start)
                    .map_err(|_| {
                        TelemetryError::ObjectStore("cache slice offset overflow".into())
                    })?;
                let copy_end =
                    usize::try_from(range.end.min(chunk_end) - chunk_start).map_err(|_| {
                        TelemetryError::ObjectStore("cache slice offset overflow".into())
                    })?;
                output.extend_from_slice(&chunk[copy_start..copy_end]);
            }
            outputs.push(CachedObjectRange::from_owned(output));
        }
        Ok(outputs)
    }

    pub(super) fn load_or_fetch_chunk<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        chunk_index: u64,
        range: Range<u64>,
    ) -> TelemetryResult<Vec<u8>> {
        Ok(self
            .load_or_fetch_shared_chunk(store, object_key, metadata, chunk_index, range)?
            .as_ref()
            .to_vec())
    }

    pub(super) fn load_or_fetch_shared_chunk<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        chunk_index: u64,
        range: Range<u64>,
    ) -> TelemetryResult<Arc<[u8]>> {
        let cache_key = checksum_bytes(
            format!("{object_key}\0{}\0{chunk_index}", metadata.version_token).as_bytes(),
        );
        if let Some(bytes) = self.memory_cache_hit(&cache_key)? {
            return Ok(bytes);
        }
        if let Some(path) = self.cache_hit(&cache_key)? {
            match read_cache_chunk(&path) {
                Ok(bytes)
                    if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
                        == range.end - range.start =>
                {
                    self.record_cache_hit()?;
                    let bytes = Arc::<[u8]>::from(bytes);
                    self.admit_memory_chunk(cache_key, Arc::clone(&bytes))?;
                    return Ok(bytes);
                }
                Ok(_) | Err(_) => self.remove_entry(&cache_key)?,
            }
        }
        let bytes = store.get_range(object_key, range)?;
        self.record_cache_miss(u64::try_from(bytes.len()).unwrap_or(u64::MAX))?;
        self.install_chunk(&cache_key, &bytes)?;
        let bytes = Arc::<[u8]>::from(bytes);
        self.admit_memory_chunk(cache_key, Arc::clone(&bytes))?;
        Ok(bytes)
    }

    pub(super) fn memory_cache_hit(&self, cache_key: &str) -> TelemetryResult<Option<Arc<[u8]>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        let bytes = state.memory_entries.get_mut(cache_key).map(|entry| {
            entry.stamp = stamp;
            Arc::clone(&entry.bytes)
        });
        if bytes.is_some() {
            state.hits = state.hits.saturating_add(1);
            state.memory_hits = state.memory_hits.saturating_add(1);
        }
        Ok(bytes)
    }

    pub(super) fn parsed_page_hit(
        &self,
        cache_key: &str,
    ) -> TelemetryResult<Option<Arc<CatalogPage>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        let page = state
            .parsed_entries
            .get_mut(cache_key)
            .and_then(|entry| match &entry.object {
                ParsedControlObject::CatalogPage(page) => {
                    entry.stamp = stamp;
                    Some(Arc::clone(page))
                }
                ParsedControlObject::GroupManifest(_) | ParsedControlObject::TierIngestGroup(_) => {
                    None
                }
            });
        if page.is_some() {
            state.hits = state.hits.saturating_add(1);
            state.parsed_hits = state.parsed_hits.saturating_add(1);
        }
        Ok(page)
    }

    pub(super) fn parsed_manifest_hit(
        &self,
        cache_key: &str,
    ) -> TelemetryResult<Option<Arc<TierGroupManifest>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        let manifest =
            state
                .parsed_entries
                .get_mut(cache_key)
                .and_then(|entry| match &entry.object {
                    ParsedControlObject::GroupManifest(manifest) => {
                        entry.stamp = stamp;
                        Some(Arc::clone(manifest))
                    }
                    ParsedControlObject::CatalogPage(_)
                    | ParsedControlObject::TierIngestGroup(_) => None,
                });
        if manifest.is_some() {
            state.hits = state.hits.saturating_add(1);
            state.parsed_hits = state.parsed_hits.saturating_add(1);
        }
        Ok(manifest)
    }

    pub(crate) fn parsed_tier_ingest_hit(
        &self,
        cache_key: &str,
    ) -> TelemetryResult<Option<Arc<[DecodedTierIngestAppend]>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        let appends =
            state
                .parsed_entries
                .get_mut(cache_key)
                .and_then(|entry| match &entry.object {
                    ParsedControlObject::TierIngestGroup(appends) => {
                        entry.stamp = stamp;
                        Some(Arc::clone(appends))
                    }
                    ParsedControlObject::CatalogPage(_) | ParsedControlObject::GroupManifest(_) => {
                        None
                    }
                });
        if appends.is_some() {
            state.hits = state.hits.saturating_add(1);
            state.parsed_hits = state.parsed_hits.saturating_add(1);
        }
        Ok(appends)
    }

    pub(crate) fn admit_parsed_tier_ingest(
        &self,
        cache_key: String,
        appends: Arc<[DecodedTierIngestAppend]>,
        source_bytes: u64,
    ) -> TelemetryResult<()> {
        self.admit_parsed_control(
            cache_key,
            ParsedControlObject::TierIngestGroup(appends),
            source_bytes,
        )
    }

    pub(super) fn admit_parsed_control(
        &self,
        cache_key: String,
        object: ParsedControlObject,
        source_bytes: u64,
    ) -> TelemetryResult<()> {
        // JSON control objects expand into strings and vectors. Four times the
        // immutable source length is a conservative charge that keeps parsed
        // state under the same hard RAM budget as verified raw chunks.
        let accounted_bytes = source_bytes.saturating_mul(4);
        if self.config.parsed_memory_bytes == 0 || accounted_bytes > self.config.parsed_memory_bytes
        {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        if let Some(previous) = state.parsed_entries.insert(
            cache_key,
            ParsedControlEntry {
                object,
                accounted_bytes,
                stamp,
            },
        ) {
            state.parsed_used_bytes = state
                .parsed_used_bytes
                .saturating_sub(previous.accounted_bytes);
        }
        state.parsed_used_bytes = state.parsed_used_bytes.saturating_add(accounted_bytes);
        evict_memory_to_budgets(
            &mut state,
            self.config.memory_bytes,
            self.config.parsed_memory_bytes,
        );
        Ok(())
    }

    pub(super) fn admit_memory_chunk(
        &self,
        cache_key: String,
        bytes: Arc<[u8]>,
    ) -> TelemetryResult<()> {
        let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if self.config.memory_bytes == 0 || bytes_len > self.config.memory_bytes {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        if let Some(previous) = state
            .memory_entries
            .insert(cache_key, MemoryCacheEntry { bytes, stamp })
        {
            state.memory_used_bytes = state
                .memory_used_bytes
                .saturating_sub(u64::try_from(previous.bytes.len()).unwrap_or(u64::MAX));
        }
        state.memory_used_bytes = state.memory_used_bytes.saturating_add(bytes_len);
        evict_memory_to_budgets(
            &mut state,
            self.config.memory_bytes,
            self.config.parsed_memory_bytes,
        );
        Ok(())
    }

    pub(super) fn record_cache_hit(&self) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.hits = state.hits.saturating_add(1);
        Ok(())
    }

    pub(super) fn record_cache_miss(&self, bytes: u64) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.misses = state.misses.saturating_add(1);
        state.source_bytes = state.source_bytes.saturating_add(bytes);
        Ok(())
    }

    pub(super) fn cache_hit(&self, cache_key: &str) -> TelemetryResult<Option<PathBuf>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        Ok(state.entries.get_mut(cache_key).map(|entry| {
            entry.stamp = stamp;
            entry.path.clone()
        }))
    }

    pub(super) fn install_chunk(&self, cache_key: &str, bytes: &[u8]) -> TelemetryResult<()> {
        let framed_bytes = u64::try_from(bytes.len())
            .unwrap_or(u64::MAX)
            .saturating_add(CACHE_HEADER_BYTES as u64);
        if framed_bytes > self.config.max_bytes {
            return Ok(());
        }
        let path = self.root.join(format!("{cache_key}.chunk"));
        write_cache_chunk_atomically(&path, bytes)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        if let Some(previous) = state.entries.insert(
            cache_key.to_owned(),
            CacheEntry {
                path,
                bytes: framed_bytes,
                stamp,
            },
        ) {
            state.used_bytes = state.used_bytes.saturating_sub(previous.bytes);
        }
        state.used_bytes = state.used_bytes.saturating_add(framed_bytes);
        evict_locked(&mut state, self.config.max_bytes)
    }

    pub(super) fn remove_entry(&self, cache_key: &str) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        if let Some(entry) = state.entries.remove(cache_key) {
            state.used_bytes = state.used_bytes.saturating_sub(entry.bytes);
            remove_cache_file(&entry.path)?;
        }
        Ok(())
    }

    pub(super) fn evict_to_budget(&self) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        evict_locked(&mut state, self.config.max_bytes)
    }
}
