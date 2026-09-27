use super::*;

impl<S: TelemetryObjectStore> TelemetryObjectTier<S> {
    /// Opens the current partition catalog without listing object storage.
    ///
    /// Startup validates only `CURRENT` and its root. Catalog pages, group
    /// manifests, and payloads are checked lazily as queries touch them.
    pub fn open(
        store: S,
        shard_id: ShardId,
        partition: TopicPartition,
        config: ObjectTierConfig,
    ) -> TelemetryResult<Self> {
        config.validate()?;
        let namespace = catalog_namespace(shard_id, partition);
        let current_key = format!("{namespace}/CURRENT");
        let Some(current_metadata) = store.head(&current_key)? else {
            let mut tier = Self {
                store,
                shard_id,
                partition,
                namespace,
                config,
                root: Arc::new(CatalogRoot::empty(shard_id, partition)),
                current_root_key: None,
                current_version: None,
                retired_leases: HashMap::new(),
            };
            tier.recover_pending_transaction()?;
            return Ok(tier);
        };
        if current_metadata.bytes > POINTER_READ_LIMIT {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT pointer exceeds its read limit".into(),
            ));
        }
        let pointer_bytes = store.get(&current_key, POINTER_READ_LIMIT)?;
        verify_bytes_metadata(&pointer_bytes, &current_metadata, "catalog CURRENT")?;
        let pointer: CatalogPointer = decode_json(&pointer_bytes, "catalog CURRENT")?;
        pointer.validate()?;
        let root_bytes = store.get(&pointer.root_key, config.max_control_object_bytes)?;
        verify_expected_object(
            &root_bytes,
            pointer.root_bytes,
            &pointer.root_checksum,
            "catalog root",
        )?;
        let root: CatalogRoot = decode_json(&root_bytes, "catalog root")?;
        root.validate(shard_id, partition)?;
        if root.generation != pointer.generation {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT and root generations disagree".into(),
            ));
        }
        if root.retired_objects.len() > config.max_retired_objects {
            return Err(TelemetryError::CorruptTier(
                "catalog root exceeds the retired-object ownership bound".into(),
            ));
        }
        let current_root_key = pointer.root_key;
        let mut tier = Self {
            store,
            shard_id,
            partition,
            namespace,
            config,
            root: Arc::new(root),
            current_root_key: Some(current_root_key),
            current_version: Some(current_metadata.version_token),
            retired_leases: HashMap::new(),
        };
        tier.recover_pending_transaction()?;
        tier.reclaim_retired_objects()?;
        Ok(tier)
    }

    /// Returns the currently selected immutable root.
    #[must_use]
    pub fn root(&self) -> &CatalogRoot {
        &self.root
    }

    /// Acquires an immutable generation lease for a query or background read.
    #[must_use]
    pub fn catalog_lease(&self) -> CatalogLease {
        CatalogLease {
            root: Arc::clone(&self.root),
        }
    }

    /// Returns the object-store adapter.
    #[must_use]
    pub fn object_store(&self) -> &S {
        &self.store
    }

    /// Returns exact retired keys still waiting for an in-process reader lease.
    #[must_use]
    pub fn pending_retired_objects(&self) -> usize {
        self.root.retired_objects.len()
    }

    pub(crate) fn reclaim_retired_objects(&mut self) -> TelemetryResult<()> {
        if self.root.retired_objects.is_empty() {
            return Ok(());
        }
        let retired = std::mem::take(&mut Arc::make_mut(&mut self.root).retired_objects);
        let mut pending = Vec::new();
        let mut first_error = None;
        let now = unix_time_millis();
        for retired_object in retired {
            let key = &retired_object.object_key;
            let leased = self
                .retired_leases
                .get(key)
                .and_then(Weak::upgrade)
                .is_some();
            if leased || now < retired_object.delete_after_unix_millis {
                pending.push(retired_object);
                continue;
            }
            match self.store.delete(key) {
                Ok(()) => {
                    self.retired_leases.remove(key);
                }
                Err(error) => {
                    pending.push(retired_object);
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        Arc::make_mut(&mut self.root).retired_objects = pending;
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn verify_current_pointer(&self) -> TelemetryResult<()> {
        let current_key = format!("{}/CURRENT", self.namespace);
        let observed = self.store.head(&current_key)?;
        let observed_version = observed
            .as_ref()
            .map(|metadata| metadata.version_token.as_str());
        if observed_version != self.current_version.as_deref() {
            return Err(TelemetryError::StaleCatalog {
                expected: self.current_version.clone(),
                observed: observed.map(|metadata| metadata.version_token),
            });
        }
        Ok(())
    }

    pub(super) fn pending_key(&self) -> String {
        format!("{}/PENDING", self.namespace)
    }

    pub(super) fn max_transaction_objects(&self) -> usize {
        self.config.max_retired_objects.saturating_add(4)
    }

    pub(super) fn transaction_id(&self, generation: u64) -> String {
        let sequence = TRANSACTION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let identity = format!(
            "{}:{}:{}:{}:{}:{}:{}",
            std::process::id(),
            timestamp,
            sequence,
            self.shard_id.get(),
            self.partition.topic_id.get(),
            self.partition.partition_id.get(),
            generation
        );
        checksum_bytes(identity.as_bytes())[..32].to_owned()
    }

    pub(super) fn recover_pending_transaction(&mut self) -> TelemetryResult<()> {
        let pending_key = self.pending_key();
        let Some(metadata) = self.store.head(&pending_key)? else {
            return Ok(());
        };
        if metadata.bytes > self.config.max_control_object_bytes {
            return Err(TelemetryError::CorruptTier(
                "catalog PENDING transaction exceeds its read limit".into(),
            ));
        }
        let bytes = self
            .store
            .get(&pending_key, self.config.max_control_object_bytes)?;
        verify_bytes_metadata(&bytes, &metadata, "catalog PENDING transaction")?;
        let transaction: CatalogTransaction = decode_json(&bytes, "catalog PENDING transaction")?;
        transaction.validate(&self.namespace, self.max_transaction_objects())?;

        let committed = self.current_root_key.as_deref()
            == Some(transaction.target_root_key.as_str())
            && self.root.generation == transaction.target_generation;
        if !committed {
            if unix_time_millis() < transaction.reclaim_after_unix_millis {
                return Err(TelemetryError::ObjectStore(format!(
                    "catalog transaction {} is still owned by an active writer lease",
                    transaction.transaction_id
                )));
            }
            for key in &transaction.owned_objects {
                self.store.delete(key)?;
            }
        }
        self.store.delete(&pending_key)
    }

    pub(super) fn begin_transaction(
        &mut self,
        transaction: &CatalogTransaction,
    ) -> TelemetryResult<()> {
        self.recover_pending_transaction()?;
        self.verify_current_pointer()?;
        transaction.validate(&self.namespace, self.max_transaction_objects())?;
        let bytes = encode_json(transaction, "catalog PENDING transaction")?;
        ensure_control_size(
            bytes.len(),
            self.config.max_control_object_bytes,
            "catalog PENDING transaction",
        )?;
        self.store
            .compare_and_swap(&self.pending_key(), None, &bytes)?;
        Ok(())
    }

    pub(super) fn abort_transaction(
        &self,
        transaction: &CatalogTransaction,
        primary: TelemetryError,
    ) -> TelemetryError {
        let mut cleanup_error = None;
        for key in &transaction.owned_objects {
            if let Err(error) = self.store.delete(key)
                && cleanup_error.is_none()
            {
                cleanup_error = Some(error);
            }
        }
        if cleanup_error.is_none()
            && let Err(error) = self.store.delete(&self.pending_key())
        {
            cleanup_error = Some(error);
        }
        if let Some(cleanup) = cleanup_error {
            TelemetryError::ObjectStore(format!(
                "{primary}; exact-key transaction cleanup will retry from PENDING: {cleanup}"
            ))
        } else {
            primary
        }
    }

    pub(super) fn complete_transaction(&self) {
        // `CURRENT` already makes the target root authoritative. If this
        // idempotent cleanup fails, startup observes that target and removes
        // only PENDING, preserving every selected object.
        let _ = self.store.delete(&self.pending_key());
    }
}
