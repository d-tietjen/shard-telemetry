use super::*;

impl<S: TelemetryObjectStore> TelemetryObjectTier<S> {
    /// Publishes a complete immutable group and conditionally advances `CURRENT`.
    ///
    /// A fixed `PENDING` ownership record is selected before any immutable
    /// object is created. A crash or stale writer can therefore relinquish
    /// every exact transaction key without object listing or tracing GC.
    pub fn publish_group(&mut self, source: TierGroupSource) -> TelemetryResult<TierGroupManifest> {
        self.reclaim_retired_objects()?;
        self.recover_pending_transaction()?;
        self.verify_current_pointer()?;
        validate_source(&source)?;
        let next_generation =
            self.root.generation.checked_add(1).ok_or_else(|| {
                TelemetryError::ObjectStore("catalog generation exhausted".into())
            })?;
        let transaction_id = self.transaction_id(next_generation);
        let mut artifacts = Vec::with_capacity(source.artifacts.len());
        let mut artifact_objects = Vec::with_capacity(source.artifacts.len());
        for artifact_source in &source.artifacts {
            let source_metadata = hash_file(&artifact_source.path)?;
            let object_key = format!(
                "{}/transactions/{}/groups/{:020}/{}-{}-{}",
                self.namespace,
                transaction_id,
                source.group_sequence,
                artifact_source.kind.key_name(),
                artifact_source.name,
                source_metadata.content_digest
            );
            artifacts.push(TierArtifact {
                kind: artifact_source.kind,
                name: artifact_source.name.clone(),
                object_key: object_key.clone(),
                bytes: source_metadata.bytes,
                checksum_algorithm: CHECKSUM_ALGORITHM.into(),
                checksum: source_metadata.content_digest.clone(),
            });
            artifact_objects.push((object_key, artifact_source.path.clone(), source_metadata));
        }
        let manifest = TierGroupManifest {
            format_version: TIER_FORMAT_VERSION,
            group_sequence: source.group_sequence,
            checkpoint: source.checkpoint,
            shard_id: self.shard_id.get(),
            topic_id: self.partition.topic_id.get().to_string(),
            partition_id: self.partition.partition_id.get(),
            blocks: source.blocks.clone(),
            artifacts,
        };
        manifest.validate(
            self.shard_id,
            self.partition,
            self.config.max_blocks_per_group,
            self.config.max_group_payload_bytes,
        )?;
        let manifest_bytes = encode_json(&manifest, "group manifest")?;
        ensure_control_size(
            manifest_bytes.len(),
            self.config.max_control_object_bytes,
            "group manifest",
        )?;
        let manifest_checksum = checksum_bytes(&manifest_bytes);
        let manifest_key = format!(
            "{}/transactions/{}/groups/{:020}/manifest-{}.json",
            self.namespace, transaction_id, manifest.group_sequence, manifest_checksum
        );
        let manifest_metadata = ObjectMetadata {
            bytes: u64::try_from(manifest_bytes.len()).unwrap_or(u64::MAX),
            version_token: String::new(),
            content_digest: manifest_checksum,
        };
        let entry = CatalogGroupEntry::from_manifest(&manifest, manifest_key, &manifest_metadata)?;

        if let Some(last_page_ref) = self.root.pages.last() {
            let last_page = self.load_page(last_page_ref)?;
            let last_group = last_page
                .groups
                .last()
                .expect("validated catalog pages are nonempty");
            if source.group_sequence == last_group.group_sequence {
                let existing = self.load_group(last_group)?;
                if !same_group_contents(&existing, &manifest) {
                    return Err(TelemetryError::CorruptTier(
                        "group sequence was retried with different contents".into(),
                    ));
                }
                return Ok(existing);
            }
            if source.group_sequence < last_group.group_sequence {
                return Err(TelemetryError::ObjectStore(
                    "group sequences must be published in increasing order".into(),
                ));
            }
            if !source.checkpoint.covers(last_group.checkpoint) {
                return Err(TelemetryError::ObjectStore(
                    "group checkpoints must advance monotonically".into(),
                ));
            }
        }

        let (page, replace_last) = match self.root.pages.last() {
            Some(last_ref) => {
                let mut last = self.load_page(last_ref)?;
                if last.groups.len() < self.config.groups_per_page {
                    last.groups.push(entry.clone());
                    (last, true)
                } else {
                    (
                        CatalogPage {
                            format_version: TIER_FORMAT_VERSION,
                            page_sequence: last.page_sequence.checked_add(1).ok_or_else(|| {
                                TelemetryError::ObjectStore(
                                    "catalog page sequence exhausted".into(),
                                )
                            })?,
                            shard_id: self.shard_id.get(),
                            topic_id: self.partition.topic_id.get().to_string(),
                            partition_id: self.partition.partition_id.get(),
                            groups: vec![entry.clone()],
                        },
                        false,
                    )
                }
            }
            None => (
                CatalogPage {
                    format_version: TIER_FORMAT_VERSION,
                    page_sequence: 0,
                    shard_id: self.shard_id.get(),
                    topic_id: self.partition.topic_id.get().to_string(),
                    partition_id: self.partition.partition_id.get(),
                    groups: vec![entry.clone()],
                },
                false,
            ),
        };
        page.validate(self.shard_id, self.partition, self.config.groups_per_page)?;
        let page_bytes = encode_json(&page, "catalog page")?;
        ensure_control_size(
            page_bytes.len(),
            self.config.max_control_object_bytes,
            "catalog page",
        )?;
        let page_checksum = checksum_bytes(&page_bytes);
        let page_key = format!(
            "{}/transactions/{}/pages/page-{:020}-{}.json",
            self.namespace, transaction_id, page.page_sequence, page_checksum
        );
        let page_metadata = ObjectMetadata {
            bytes: u64::try_from(page_bytes.len()).unwrap_or(u64::MAX),
            version_token: String::new(),
            content_digest: page_checksum,
        };
        let page_ref = CatalogPageRef::from_page(&page, page_key, &page_metadata)?;

        let mut next_root = (*self.root).clone();
        next_root.generation = next_generation;
        next_root.latest_checkpoint = Some(source.checkpoint);
        let first_block_id = manifest
            .blocks
            .first()
            .expect("validated group has blocks")
            .block_id;
        if first_block_id < self.root.next_block_id {
            return Err(TelemetryError::ObjectStore(
                "group block identifiers overlap an older group".into(),
            ));
        }
        next_root.next_block_id = manifest
            .blocks
            .last()
            .expect("validated group has blocks")
            .block_id
            .checked_add(1)
            .ok_or_else(|| TelemetryError::ObjectStore("block identifier exhausted".into()))?;
        if replace_last {
            *next_root
                .pages
                .last_mut()
                .expect("a replaced page has an existing reference") = page_ref.clone();
        } else {
            next_root.pages.push(page_ref.clone());
        }
        let mut newly_retired = Vec::with_capacity(2);
        let delete_after_unix_millis = unix_time_millis().saturating_add(
            u64::try_from(self.config.retirement_grace.as_millis()).unwrap_or(u64::MAX),
        );
        if let Some(root_key) = &self.current_root_key {
            newly_retired.push(RetiredObject {
                object_key: root_key.clone(),
                delete_after_unix_millis,
            });
        }
        if replace_last {
            newly_retired.push(RetiredObject {
                object_key: self
                    .root
                    .pages
                    .last()
                    .expect("a replaced page has an existing reference")
                    .page_key
                    .clone(),
                delete_after_unix_millis,
            });
        }
        next_root
            .retired_objects
            .extend(newly_retired.iter().cloned());
        next_root
            .retired_objects
            .sort_unstable_by(|left, right| left.object_key.cmp(&right.object_key));
        next_root
            .retired_objects
            .dedup_by(|left, right| left.object_key == right.object_key);
        if next_root.retired_objects.len() > self.config.max_retired_objects {
            return Err(TelemetryError::ObjectStore(
                "catalog generation exhausted its exact retired-object ownership bound".into(),
            ));
        }
        next_root.validate(self.shard_id, self.partition)?;
        let root_bytes = encode_json(&next_root, "catalog root")?;
        ensure_control_size(
            root_bytes.len(),
            self.config.max_control_object_bytes,
            "catalog root",
        )?;
        let root_checksum = checksum_bytes(&root_bytes);
        let root_key = format!(
            "{}/transactions/{}/roots/root-{:020}-{}.json",
            self.namespace, transaction_id, next_root.generation, root_checksum
        );
        let root_metadata = ObjectMetadata {
            bytes: u64::try_from(root_bytes.len()).unwrap_or(u64::MAX),
            version_token: String::new(),
            content_digest: root_checksum,
        };
        let pointer = CatalogPointer {
            format_version: TIER_FORMAT_VERSION,
            generation: next_root.generation,
            root_key: root_key.clone(),
            root_bytes: root_metadata.bytes,
            root_checksum: root_metadata.content_digest.clone(),
        };
        let pointer_bytes = encode_json(&pointer, "catalog CURRENT")?;
        if u64::try_from(pointer_bytes.len()).unwrap_or(u64::MAX) > POINTER_READ_LIMIT {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT pointer exceeds its read limit".into(),
            ));
        }
        let mut owned_objects = artifact_objects
            .iter()
            .map(|(key, _, _)| key.clone())
            .collect::<Vec<_>>();
        owned_objects.extend([
            entry.manifest_key.clone(),
            page_ref.page_key.clone(),
            root_key,
        ]);
        let transaction = CatalogTransaction {
            format_version: TIER_FORMAT_VERSION,
            transaction_id,
            target_generation: next_generation,
            target_root_key: pointer.root_key.clone(),
            reclaim_after_unix_millis: unix_time_millis().saturating_add(
                u64::try_from(self.config.transaction_lease.as_millis()).unwrap_or(u64::MAX),
            ),
            owned_objects,
        };
        self.begin_transaction(&transaction)?;

        let publication = (|| -> TelemetryResult<ObjectMetadata> {
            for (key, path, expected) in &artifact_objects {
                let stored = self.store.put_file_if_absent(key, path)?;
                verify_object_metadata(&stored, expected, "group artifact")?;
            }
            let stored_manifest = self
                .store
                .put_bytes_if_absent(&entry.manifest_key, &manifest_bytes)?;
            verify_object_metadata(&stored_manifest, &manifest_metadata, "group manifest")?;
            let stored_page = self
                .store
                .put_bytes_if_absent(&page_ref.page_key, &page_bytes)?;
            verify_object_metadata(&stored_page, &page_metadata, "catalog page")?;
            let stored_root = self
                .store
                .put_bytes_if_absent(&pointer.root_key, &root_bytes)?;
            verify_object_metadata(&stored_root, &root_metadata, "catalog root")?;

            self.store.compare_and_swap(
                &format!("{}/CURRENT", self.namespace),
                self.current_version.as_deref(),
                &pointer_bytes,
            )
        })();
        let current_metadata = match publication {
            Ok(metadata) => metadata,
            Err(error) => return Err(self.abort_transaction(&transaction, error)),
        };
        let retired_root = Arc::clone(&self.root);
        self.root = Arc::new(next_root);
        self.current_root_key = Some(pointer.root_key);
        self.current_version = Some(current_metadata.version_token);
        let retired_lease = Arc::downgrade(&retired_root);
        for retired in newly_retired {
            self.retired_leases
                .insert(retired.object_key, Weak::clone(&retired_lease));
        }
        drop(retired_root);
        self.complete_transaction();
        self.reclaim_retired_objects()?;
        Ok(manifest)
    }
}
