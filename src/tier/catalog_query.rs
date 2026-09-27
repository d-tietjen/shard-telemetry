use super::*;

impl<S: TelemetryObjectStore> TelemetryObjectTier<S> {
    /// Returns group entries whose coarse bounds overlap the query.
    ///
    /// Only overlapping catalog pages are loaded. This never lists objects.
    pub fn candidate_groups(
        &self,
        range: TierQueryRange,
    ) -> TelemetryResult<Vec<CatalogGroupEntry>> {
        let mut groups = Vec::new();
        self.for_each_candidate_group(range, |group| {
            groups.push(group.clone());
            Ok(true)
        })?;
        Ok(groups)
    }

    /// Returns overlapping groups while serving immutable catalog pages from
    /// the integrity-checked SSD cache when they fit its read bound.
    pub fn candidate_groups_cached(
        &self,
        range: TierQueryRange,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Vec<CatalogGroupEntry>> {
        let mut groups = Vec::new();
        self.for_each_candidate_group_with(
            range,
            |reference| self.load_page_cached(reference, cache),
            |group| {
                groups.push(group.clone());
                Ok(true)
            },
        )?;
        Ok(groups)
    }

    /// Returns correlation candidates after pruning immutable catalog pages
    /// and groups, before any group manifest or payload is loaded.
    pub fn candidate_groups_cached_for_correlation(
        &self,
        range: TierQueryRange,
        cache: &SsdObjectCache,
        query: &CorrelationQuery,
        signal: TelemetrySignal,
    ) -> TelemetryResult<Vec<CatalogGroupEntry>> {
        range.validate()?;
        let mut groups = Vec::new();
        for page_ref in &self.root.pages {
            if !range.overlaps(
                page_ref.first_offset,
                page_ref.last_offset,
                page_ref.min_timestamp_unix_nanos,
                page_ref.max_timestamp_unix_nanos,
                page_ref.min_signal_identity,
                page_ref.max_signal_identity,
            ) || !catalog_correlation_may_match(
                page_ref.correlation_filter.as_ref(),
                page_ref.min_signal_identity,
                page_ref.max_signal_identity,
                query,
                signal,
            ) {
                continue;
            }
            let page = self.load_page_cached(page_ref, cache)?;
            for group in &page.groups {
                if range.overlaps(
                    group.first_offset,
                    group.last_offset,
                    group.min_timestamp_unix_nanos,
                    group.max_timestamp_unix_nanos,
                    group.min_signal_identity,
                    group.max_signal_identity,
                ) && catalog_correlation_may_match(
                    group.correlation_filter.as_ref(),
                    group.min_signal_identity,
                    group.max_signal_identity,
                    query,
                    signal,
                ) {
                    groups.push(group.clone());
                }
            }
        }
        Ok(groups)
    }

    /// Loads only the final catalog page and returns its newest group entry.
    pub fn latest_group(&self) -> TelemetryResult<Option<CatalogGroupEntry>> {
        let Some(reference) = self.root.pages.last() else {
            return Ok(None);
        };
        Ok(self.load_page(reference)?.groups.last().cloned())
    }

    /// Loads the newest group through the bounded catalog-page cache.
    pub fn latest_group_cached(
        &self,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Option<CatalogGroupEntry>> {
        let Some(reference) = self.root.pages.last() else {
            return Ok(None);
        };
        Ok(self
            .load_page_cached(reference, cache)?
            .groups
            .last()
            .cloned())
    }

    /// Visits overlapping groups one at a time without materializing a
    /// corpus-sized candidate vector.
    ///
    /// Returning `false` from `visit` stops traversal successfully. Catalog
    /// pages are loaded and verified lazily, and object storage is never
    /// listed.
    pub fn for_each_candidate_group(
        &self,
        range: TierQueryRange,
        visit: impl FnMut(&CatalogGroupEntry) -> TelemetryResult<bool>,
    ) -> TelemetryResult<()> {
        self.for_each_candidate_group_with(
            range,
            |reference| self.load_page(reference).map(Arc::new),
            visit,
        )
    }

    pub(super) fn for_each_candidate_group_with(
        &self,
        range: TierQueryRange,
        mut load_page: impl FnMut(&CatalogPageRef) -> TelemetryResult<Arc<CatalogPage>>,
        mut visit: impl FnMut(&CatalogGroupEntry) -> TelemetryResult<bool>,
    ) -> TelemetryResult<()> {
        range.validate()?;
        for page_ref in &self.root.pages {
            if !range.overlaps(
                page_ref.first_offset,
                page_ref.last_offset,
                page_ref.min_timestamp_unix_nanos,
                page_ref.max_timestamp_unix_nanos,
                page_ref.min_signal_identity,
                page_ref.max_signal_identity,
            ) {
                continue;
            }
            let page = load_page(page_ref)?;
            for group in &page.groups {
                if range.overlaps(
                    group.first_offset,
                    group.last_offset,
                    group.min_timestamp_unix_nanos,
                    group.max_timestamp_unix_nanos,
                    group.min_signal_identity,
                    group.max_signal_identity,
                ) && !visit(group)?
                {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Loads and validates one group manifest selected from a catalog page.
    pub fn load_group(&self, entry: &CatalogGroupEntry) -> TelemetryResult<TierGroupManifest> {
        entry.validate()?;
        let bytes = self
            .store
            .get(&entry.manifest_key, self.config.max_control_object_bytes)?;
        self.decode_group(entry, &bytes)
    }

    /// Loads and validates a group manifest through the immutable SSD cache.
    pub fn load_group_cached(
        &self,
        entry: &CatalogGroupEntry,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Arc<TierGroupManifest>> {
        entry.validate()?;
        if entry.manifest_bytes > cache.max_read_bytes() {
            return self.load_group(entry).map(Arc::new);
        }
        if let Some(manifest) = cache.parsed_manifest_hit(&entry.manifest_key)? {
            return Ok(manifest);
        }
        let metadata = ObjectMetadata {
            bytes: entry.manifest_bytes,
            version_token: entry.manifest_checksum.clone(),
            content_digest: entry.manifest_checksum.clone(),
        };
        let bytes = cache.read_range_with_metadata(
            &self.store,
            &entry.manifest_key,
            &metadata,
            0..entry.manifest_bytes,
        )?;
        let manifest = Arc::new(self.decode_group(entry, &bytes)?);
        cache.admit_parsed_control(
            entry.manifest_key.clone(),
            ParsedControlObject::GroupManifest(Arc::clone(&manifest)),
            entry.manifest_bytes,
        )?;
        Ok(manifest)
    }

    /// Reads and verifies a complete immutable artifact on demand.
    pub fn read_artifact(
        &self,
        artifact: &TierArtifact,
        max_bytes: u64,
    ) -> TelemetryResult<Vec<u8>> {
        validate_artifact(artifact)?;
        if artifact.bytes > max_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "artifact {} exceeds read limit {max_bytes}",
                artifact.name
            )));
        }
        let bytes = self.store.get(&artifact.object_key, max_bytes)?;
        verify_expected_object(&bytes, artifact.bytes, &artifact.checksum, "group artifact")?;
        Ok(bytes)
    }

    /// Reads a complete immutable artifact through the bounded SSD cache.
    pub fn read_artifact_cached(
        &self,
        artifact: &TierArtifact,
        max_bytes: u64,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Vec<u8>> {
        validate_artifact(artifact)?;
        if artifact.bytes > max_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "artifact {} exceeds read limit {max_bytes}",
                artifact.name
            )));
        }
        if artifact.bytes > cache.max_read_bytes() {
            return self.read_artifact(artifact, max_bytes);
        }
        let metadata = ObjectMetadata {
            bytes: artifact.bytes,
            version_token: artifact.checksum.clone(),
            content_digest: artifact.checksum.clone(),
        };
        let bytes = cache.read_range_with_metadata(
            &self.store,
            &artifact.object_key,
            &metadata,
            0..artifact.bytes,
        )?;
        verify_expected_object(&bytes, artifact.bytes, &artifact.checksum, "group artifact")?;
        Ok(bytes)
    }

    pub(super) fn load_page(&self, reference: &CatalogPageRef) -> TelemetryResult<CatalogPage> {
        reference.validate()?;
        let bytes = self
            .store
            .get(&reference.page_key, self.config.max_control_object_bytes)?;
        self.decode_page(reference, &bytes)
    }

    pub(super) fn load_page_cached(
        &self,
        reference: &CatalogPageRef,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Arc<CatalogPage>> {
        reference.validate()?;
        if reference.page_bytes > cache.max_read_bytes() {
            return self.load_page(reference).map(Arc::new);
        }
        if let Some(page) = cache.parsed_page_hit(&reference.page_key)? {
            return Ok(page);
        }
        let metadata = ObjectMetadata {
            bytes: reference.page_bytes,
            version_token: reference.page_checksum.clone(),
            content_digest: reference.page_checksum.clone(),
        };
        let bytes = cache.read_range_with_metadata(
            &self.store,
            &reference.page_key,
            &metadata,
            0..reference.page_bytes,
        )?;
        let page = Arc::new(self.decode_page(reference, &bytes)?);
        cache.admit_parsed_control(
            reference.page_key.clone(),
            ParsedControlObject::CatalogPage(Arc::clone(&page)),
            reference.page_bytes,
        )?;
        Ok(page)
    }

    pub(super) fn decode_page(
        &self,
        reference: &CatalogPageRef,
        bytes: &[u8],
    ) -> TelemetryResult<CatalogPage> {
        verify_expected_object(
            bytes,
            reference.page_bytes,
            &reference.page_checksum,
            "catalog page",
        )?;
        let page: CatalogPage = decode_json(bytes, "catalog page")?;
        page.validate(self.shard_id, self.partition, self.config.groups_per_page)?;
        if page.page_sequence != reference.page_sequence {
            return Err(TelemetryError::CorruptTier(
                "catalog root and page sequences disagree".into(),
            ));
        }
        Ok(page)
    }

    pub(super) fn decode_group(
        &self,
        entry: &CatalogGroupEntry,
        bytes: &[u8],
    ) -> TelemetryResult<TierGroupManifest> {
        verify_expected_object(
            bytes,
            entry.manifest_bytes,
            &entry.manifest_checksum,
            "group manifest",
        )?;
        let manifest: TierGroupManifest = decode_json(bytes, "group manifest")?;
        manifest.validate(
            self.shard_id,
            self.partition,
            self.config.max_blocks_per_group,
            self.config.max_group_payload_bytes,
        )?;
        if manifest.group_sequence != entry.group_sequence {
            return Err(TelemetryError::CorruptTier(
                "catalog group and manifest sequences disagree".into(),
            ));
        }
        Ok(manifest)
    }
}
