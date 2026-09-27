use super::*;

impl<S: TelemetryObjectStore> TelemetryObjectTier<S> {
    /// Removes complete groups older than `cutoff_timestamp_unix_nanos`.
    ///
    /// The transaction is bounded by `max_retired_objects`, rewrites only pages
    /// that actually lose groups, and records every relinquished key in the new
    /// root before `CURRENT` advances. The newest group remains as the durable
    /// recovery checkpoint anchor. No object listing or reachability scan occurs.
    pub fn retain_since_timestamp(
        &mut self,
        cutoff_timestamp_unix_nanos: u64,
    ) -> TelemetryResult<TierRetentionReport> {
        self.reclaim_retired_objects()?;
        self.recover_pending_transaction()?;
        self.verify_current_pointer()?;
        let Some(final_group_sequence) =
            self.root.pages.last().map(|page| page.last_group_sequence)
        else {
            return Ok(TierRetentionReport::default());
        };
        let available_retirements = self
            .config
            .max_retired_objects
            .saturating_sub(self.root.retired_objects.len())
            .saturating_sub(1);
        if available_retirements < 3 {
            return Ok(TierRetentionReport::default());
        }
        let next_generation =
            self.root.generation.checked_add(1).ok_or_else(|| {
                TelemetryError::ObjectStore("catalog generation exhausted".into())
            })?;
        let transaction_id = self.transaction_id(next_generation);

        let mut next_pages = Vec::with_capacity(self.root.pages.len());
        let mut replacement_objects = Vec::new();
        let mut retired_keys = Vec::new();
        let mut report = TierRetentionReport::default();
        for page_ref in &self.root.pages {
            let page = self.load_page(page_ref)?;
            let mut retained_groups = Vec::with_capacity(page.groups.len());
            let mut page_changed = false;
            for group in page.groups {
                if group.group_sequence == final_group_sequence
                    || group.max_timestamp_unix_nanos >= cutoff_timestamp_unix_nanos
                {
                    retained_groups.push(group);
                    continue;
                }
                let manifest = self.load_group(&group)?;
                let required = 1usize.saturating_add(manifest.artifacts.len());
                let page_key_cost = usize::from(!page_changed);
                if retired_keys
                    .len()
                    .saturating_add(required)
                    .saturating_add(page_key_cost)
                    > available_retirements
                {
                    retained_groups.push(group);
                    continue;
                }
                if !page_changed {
                    retired_keys.push(page_ref.page_key.clone());
                    page_changed = true;
                }
                retired_keys.push(group.manifest_key.clone());
                retired_keys.extend(
                    manifest
                        .artifacts
                        .iter()
                        .map(|artifact| artifact.object_key.clone()),
                );
                report.retired_groups = report.retired_groups.saturating_add(1);
                report.retired_payload_bytes = report
                    .retired_payload_bytes
                    .saturating_add(group.payload_bytes);
            }
            if !page_changed {
                next_pages.push(page_ref.clone());
                continue;
            }
            if retained_groups.is_empty() {
                continue;
            }
            let replacement = CatalogPage {
                format_version: TIER_FORMAT_VERSION,
                page_sequence: page.page_sequence,
                shard_id: page.shard_id,
                topic_id: page.topic_id,
                partition_id: page.partition_id,
                groups: retained_groups,
            };
            replacement.validate(self.shard_id, self.partition, self.config.groups_per_page)?;
            let bytes = encode_json(&replacement, "retained catalog page")?;
            ensure_control_size(
                bytes.len(),
                self.config.max_control_object_bytes,
                "retained catalog page",
            )?;
            let checksum = checksum_bytes(&bytes);
            let key = format!(
                "{}/transactions/{}/pages/page-{:020}-{}.json",
                self.namespace, transaction_id, replacement.page_sequence, checksum
            );
            let metadata = ObjectMetadata {
                bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                version_token: String::new(),
                content_digest: checksum,
            };
            next_pages.push(CatalogPageRef::from_page(
                &replacement,
                key.clone(),
                &metadata,
            )?);
            replacement_objects.push((key, bytes, metadata));
        }
        if report.retired_groups == 0 {
            return Ok(report);
        }

        let current_root_key = self.current_root_key.clone().ok_or_else(|| {
            TelemetryError::CorruptTier("nonempty catalog has no selected root key".into())
        })?;
        retired_keys.push(current_root_key);
        retired_keys.sort_unstable();
        retired_keys.dedup();
        let delete_after_unix_millis = unix_time_millis().saturating_add(
            u64::try_from(self.config.retirement_grace.as_millis()).unwrap_or(u64::MAX),
        );
        let newly_retired = retired_keys
            .into_iter()
            .map(|object_key| RetiredObject {
                object_key,
                delete_after_unix_millis,
            })
            .collect::<Vec<_>>();
        report.retired_objects = u64::try_from(newly_retired.len()).unwrap_or(u64::MAX);

        let mut next_root = (*self.root).clone();
        next_root.generation = next_generation;
        next_root.pages = next_pages;
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
                "retention exhausted the exact retired-object ownership bound".into(),
            ));
        }
        next_root.validate(self.shard_id, self.partition)?;
        let root_bytes = encode_json(&next_root, "retained catalog root")?;
        ensure_control_size(
            root_bytes.len(),
            self.config.max_control_object_bytes,
            "retained catalog root",
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
        let mut owned_objects = replacement_objects
            .iter()
            .map(|(key, _, _)| key.clone())
            .collect::<Vec<_>>();
        owned_objects.push(root_key);
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
            for (key, bytes, expected) in &replacement_objects {
                let stored = self.store.put_bytes_if_absent(key, bytes)?;
                verify_object_metadata(&stored, expected, "retained catalog page")?;
            }
            let stored_root = self
                .store
                .put_bytes_if_absent(&pointer.root_key, &root_bytes)?;
            verify_object_metadata(&stored_root, &root_metadata, "retained catalog root")?;
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
        Ok(report)
    }

    /// Removes the oldest complete groups until selected compressed payloads
    /// fit `max_payload_bytes`.
    ///
    /// The newest group remains as the recovery anchor even when it alone is
    /// larger than the configured budget. Callers should therefore configure
    /// a budget at least as large as one maximum group payload.
    pub fn retain_to_payload_bytes(
        &mut self,
        max_payload_bytes: u64,
    ) -> TelemetryResult<TierRetentionReport> {
        if max_payload_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "object-tier payload budget must be nonzero",
            ));
        }
        let Some(final_group_sequence) =
            self.root.pages.last().map(|page| page.last_group_sequence)
        else {
            return Ok(TierRetentionReport::default());
        };
        let mut total = 0_u64;
        let mut candidates = Vec::new();
        for page_ref in &self.root.pages {
            let page = self.load_page(page_ref)?;
            for group in &page.groups {
                total = total.saturating_add(group.payload_bytes);
                if group.group_sequence != final_group_sequence {
                    candidates.push((
                        group.max_timestamp_unix_nanos,
                        group.group_sequence,
                        group.payload_bytes,
                    ));
                }
            }
        }
        if total <= max_payload_bytes {
            return Ok(TierRetentionReport::default());
        }
        candidates.sort_unstable();
        let mut cutoff = None;
        for (timestamp, _, bytes) in candidates {
            if total <= max_payload_bytes {
                break;
            }
            total = total.saturating_sub(bytes);
            cutoff = Some(timestamp.saturating_add(1));
        }
        cutoff.map_or(Ok(TierRetentionReport::default()), |cutoff| {
            self.retain_since_timestamp(cutoff)
        })
    }
}
