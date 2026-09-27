use super::*;

pub(super) fn evict_memory_to_budgets(state: &mut CacheState, raw_budget: u64, parsed_budget: u64) {
    while state.memory_used_bytes > raw_budget {
        let Some(key) = state
            .memory_entries
            .iter()
            .min_by_key(|(key, entry)| (entry.stamp, *key))
            .map(|(key, _)| key.clone())
        else {
            state.memory_used_bytes = 0;
            break;
        };
        if let Some(removed) = state.memory_entries.remove(&key) {
            state.memory_used_bytes = state
                .memory_used_bytes
                .saturating_sub(u64::try_from(removed.bytes.len()).unwrap_or(u64::MAX));
        }
    }
    while state.parsed_used_bytes > parsed_budget {
        let Some(key) = state
            .parsed_entries
            .iter()
            .min_by_key(|(key, entry)| (entry.stamp, *key))
            .map(|(key, _)| key.clone())
        else {
            state.parsed_used_bytes = 0;
            break;
        };
        if let Some(removed) = state.parsed_entries.remove(&key) {
            state.parsed_used_bytes = state
                .parsed_used_bytes
                .saturating_sub(removed.accounted_bytes);
        }
    }
}

/// Writes selected staged block payloads as one immutable concatenated pack.
///
/// The returned entries contain exact byte extents and per-block checksums.
pub fn write_staged_payload_pack(
    catalog: &BlockCatalog,
    block_ids: &[BlockId],
    destination: impl AsRef<Path>,
) -> TelemetryResult<Vec<TierBlockEntry>> {
    if block_ids.is_empty() {
        return Err(TelemetryError::ObjectStore(
            "a payload pack requires at least one block".into(),
        ));
    }
    if block_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(TelemetryError::ObjectStore(
            "payload-pack block IDs must be strictly increasing".into(),
        ));
    }
    let first_descriptor = catalog
        .get(block_ids[0])
        .ok_or_else(|| TelemetryError::UnknownBlock(block_ids[0].get()))?;
    let destination = destination.as_ref();
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| storage_io("create payload-pack directory", error))?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)
        .map_err(|error| storage_io("create staged payload pack", error))?;
    let mut offset = 0u64;
    let mut entries = Vec::with_capacity(block_ids.len());
    for &block_id in block_ids {
        let descriptor = catalog
            .get(block_id)
            .ok_or_else(|| TelemetryError::UnknownBlock(block_id.get()))?;
        if descriptor.stream_shard_id != first_descriptor.stream_shard_id
            || descriptor.topic_partition != first_descriptor.topic_partition
        {
            return Err(TelemetryError::ObjectStore(
                "one payload pack cannot cross a shard or logical partition".into(),
            ));
        }
        let payload = catalog
            .staged_payload(block_id)
            .ok_or_else(|| TelemetryError::MissingStagedPayload(block_id.get()))?;
        if u64::try_from(payload.len()).unwrap_or(u64::MAX) != descriptor.stored_bytes {
            return Err(TelemetryError::CorruptTier(format!(
                "staged block {} length differs from its descriptor",
                block_id.get()
            )));
        }
        file.write_all(&payload)
            .map_err(|error| storage_io("write staged payload pack", error))?;
        entries.push(TierBlockEntry::from_descriptor(
            descriptor,
            offset,
            checksum_bytes(&payload),
        ));
        offset = offset
            .checked_add(descriptor.stored_bytes)
            .ok_or_else(|| TelemetryError::ObjectStore("payload-pack length overflow".into()))?;
    }
    file.sync_data()
        .map_err(|error| storage_io("sync staged payload pack", error))?;
    sync_parent(destination)?;
    Ok(entries)
}

/// Marks all local blocks in a published group as durable payload ranges.
pub fn mark_group_offloaded(
    catalog: &mut BlockCatalog,
    manifest: &TierGroupManifest,
) -> TelemetryResult<()> {
    let payload = manifest
        .artifact(TierArtifactKind::PayloadPack)
        .ok_or_else(|| TelemetryError::CorruptTier("group has no payload artifact".into()))?;

    // Validate the complete transition before mutating any block. A corrupt
    // or stale manifest must not leave a partially offloaded local catalog.
    for block in &manifest.blocks {
        let descriptor = catalog
            .get(BlockId::new(block.block_id))
            .ok_or(TelemetryError::UnknownBlock(block.block_id))?;
        let range_end = block
            .payload_offset
            .checked_add(descriptor.stored_bytes)
            .ok_or_else(|| TelemetryError::CorruptTier("payload range overflow".into()))?;
        if range_end > payload.bytes {
            return Err(TelemetryError::CorruptTier(format!(
                "block {} exceeds payload artifact length",
                block.block_id
            )));
        }
        if descriptor.stream_shard_id.get() != manifest.shard_id
            || descriptor.topic_partition.topic_id.get().to_string() != manifest.topic_id
            || descriptor.topic_partition.partition_id.get() != manifest.partition_id
        {
            return Err(TelemetryError::CorruptTier(format!(
                "block {} belongs to another catalog namespace",
                block.block_id
            )));
        }
    }

    for block in &manifest.blocks {
        catalog.mark_offloaded_range(
            BlockId::new(block.block_id),
            payload.object_key.clone(),
            block.payload_offset,
        )?;
    }
    Ok(())
}

pub(super) fn validate_source(source: &TierGroupSource) -> TelemetryResult<()> {
    if source.blocks.is_empty() || source.artifacts.is_empty() {
        return Err(TelemetryError::ObjectStore(
            "a tier group requires blocks and artifacts".into(),
        ));
    }
    if source
        .blocks
        .windows(2)
        .any(|pair| pair[0].block_id >= pair[1].block_id)
    {
        return Err(TelemetryError::ObjectStore(
            "tier group blocks must be strictly increasing".into(),
        ));
    }
    for artifact in &source.artifacts {
        validate_artifact_name(&artifact.name)?;
    }
    if source
        .artifacts
        .iter()
        .enumerate()
        .any(|(index, artifact)| {
            source.artifacts[index + 1..]
                .iter()
                .any(|other| artifact.kind == other.kind && artifact.name == other.name)
        })
    {
        return Err(TelemetryError::ObjectStore(
            "tier group artifact names must be unique per role".into(),
        ));
    }
    Ok(())
}

pub(super) fn same_group_contents(left: &TierGroupManifest, right: &TierGroupManifest) -> bool {
    left.format_version == right.format_version
        && left.group_sequence == right.group_sequence
        && left.checkpoint == right.checkpoint
        && left.shard_id == right.shard_id
        && left.topic_id == right.topic_id
        && left.partition_id == right.partition_id
        && left.blocks == right.blocks
        && left.artifacts.len() == right.artifacts.len()
        && left
            .artifacts
            .iter()
            .zip(&right.artifacts)
            .all(|(left, right)| {
                left.kind == right.kind
                    && left.name == right.name
                    && left.bytes == right.bytes
                    && left.checksum_algorithm == right.checksum_algorithm
                    && left.checksum == right.checksum
            })
}

pub(super) fn verify_object_metadata(
    observed: &ObjectMetadata,
    expected: &ObjectMetadata,
    context: &str,
) -> TelemetryResult<()> {
    if observed.bytes != expected.bytes || observed.content_digest != expected.content_digest {
        return Err(TelemetryError::CorruptTier(format!(
            "object store changed {context}"
        )));
    }
    Ok(())
}

pub(super) fn validate_artifact(artifact: &TierArtifact) -> TelemetryResult<()> {
    validate_artifact_name(&artifact.name)?;
    validate_object_key(&artifact.object_key)?;
    if artifact.bytes == 0
        || artifact.checksum_algorithm != CHECKSUM_ALGORITHM
        || !valid_checksum(&artifact.checksum)
    {
        return Err(TelemetryError::CorruptTier(
            "group artifact metadata is invalid".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_artifact_name(name: &str) -> TelemetryResult<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(TelemetryError::ObjectStore(
            "artifact names must be 1..=128 path-free ASCII characters".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_object_key(key: &str) -> TelemetryResult<()> {
    let path = Path::new(key);
    if key.is_empty()
        || key.contains('\\')
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(TelemetryError::ObjectStore(format!(
            "unsafe object key {key:?}"
        )));
    }
    Ok(())
}

pub(super) fn catalog_namespace(shard_id: ShardId, partition: TopicPartition) -> String {
    format!(
        "catalog/shard-{}/topic-{:032x}/partition-{}",
        shard_id.get(),
        partition.topic_id.get(),
        partition.partition_id.get()
    )
}

pub(super) fn encode_json<T: Serialize>(value: &T, context: &str) -> TelemetryResult<Vec<u8>> {
    serde_json::to_vec(value)
        .map_err(|error| TelemetryError::CorruptTier(format!("{context} encoding failed: {error}")))
}

pub(super) fn decode_json<T: DeserializeOwned>(bytes: &[u8], context: &str) -> TelemetryResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| TelemetryError::CorruptTier(format!("{context} decoding failed: {error}")))
}

pub(super) fn ensure_control_size(bytes: usize, limit: u64, context: &str) -> TelemetryResult<()> {
    if u64::try_from(bytes).unwrap_or(u64::MAX) > limit {
        return Err(TelemetryError::ObjectStore(format!(
            "{context} exceeds configured control-object limit {limit}"
        )));
    }
    Ok(())
}

pub(super) fn verify_bytes_metadata(
    bytes: &[u8],
    metadata: &ObjectMetadata,
    context: &str,
) -> TelemetryResult<()> {
    if metadata.content_digest.is_empty() {
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != metadata.bytes {
            return Err(TelemetryError::CorruptTier(format!(
                "{context} failed object-store length verification"
            )));
        }
        return Ok(());
    }
    verify_expected_object(bytes, metadata.bytes, &metadata.content_digest, context)
}

pub(super) fn verify_expected_object(
    bytes: &[u8],
    expected_bytes: u64,
    expected_checksum: &str,
    context: &str,
) -> TelemetryResult<()> {
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != expected_bytes
        || checksum_bytes(bytes) != expected_checksum
    {
        return Err(TelemetryError::CorruptTier(format!(
            "{context} failed length or BLAKE3 verification"
        )));
    }
    Ok(())
}

pub(super) fn valid_checksum(checksum: &str) -> bool {
    checksum.len() == 64
        && checksum
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

pub(super) fn metadata_for_bytes(bytes: &[u8]) -> ObjectMetadata {
    let content_digest = checksum_bytes(bytes);
    ObjectMetadata {
        bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        version_token: content_digest.clone(),
        content_digest,
    }
}

pub(super) fn checksum_bytes(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

pub(super) fn unix_time_millis() -> u64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

pub(crate) fn hash_file(path: &Path) -> TelemetryResult<ObjectMetadata> {
    let mut file =
        File::open(path).map_err(|error| storage_io("open file for BLAKE3 hashing", error))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0; COPY_BUFFER_BYTES];
    let mut bytes = 0u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage_io("read file for BLAKE3 hashing", error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| TelemetryError::StorageIo("file length overflow".into()))?;
    }
    Ok(ObjectMetadata {
        bytes,
        version_token: hasher.finalize().to_hex().to_string(),
        content_digest: hasher.finalize().to_hex().to_string(),
    })
}

pub(super) fn metadata_for_path_if_present(path: &Path) -> TelemetryResult<Option<ObjectMetadata>> {
    match path.metadata() {
        Ok(metadata) if metadata.is_file() => hash_file(path).map(Some),
        Ok(_) => Err(TelemetryError::ObjectStore(format!(
            "object path {} is not a regular file",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(storage_io("inspect local object", error)),
    }
}

pub(super) fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> TelemetryResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| storage_io("create object parent directory", error))?;
    }
    let temporary = temporary_path(path);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| storage_io("create temporary object", error))?;
    let result = (|| {
        file.write_all(bytes)
            .map_err(|error| storage_io("write temporary object", error))?;
        file.sync_data()
            .map_err(|error| storage_io("sync temporary object", error))?;
        fs::rename(&temporary, path)
            .map_err(|error| storage_io("publish temporary object", error))?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub(super) fn copy_file_atomically(
    source: &Path,
    destination: &Path,
) -> TelemetryResult<ObjectMetadata> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| storage_io("create object parent directory", error))?;
    }
    let temporary = temporary_path(destination);
    let mut input = File::open(source).map_err(|error| storage_io("open object source", error))?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| storage_io("create temporary object", error))?;
    let result = (|| {
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0; COPY_BUFFER_BYTES];
        let mut bytes = 0u64;
        loop {
            let read = input
                .read(&mut buffer)
                .map_err(|error| storage_io("read object source", error))?;
            if read == 0 {
                break;
            }
            output
                .write_all(&buffer[..read])
                .map_err(|error| storage_io("write temporary object", error))?;
            hasher.update(&buffer[..read]);
            bytes = bytes
                .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
                .ok_or_else(|| TelemetryError::StorageIo("object length overflow".into()))?;
        }
        output
            .sync_data()
            .map_err(|error| storage_io("sync temporary object", error))?;
        fs::rename(&temporary, destination)
            .map_err(|error| storage_io("publish temporary object", error))?;
        sync_parent(destination)?;
        Ok(ObjectMetadata {
            bytes,
            version_token: hasher.finalize().to_hex().to_string(),
            content_digest: hasher.finalize().to_hex().to_string(),
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub(super) fn temporary_path(path: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    path.with_extension(format!("tmp-{}-{sequence}", std::process::id()))
}

pub(super) fn sync_parent(path: &Path) -> TelemetryResult<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| storage_io("sync parent directory", error))
}

pub(super) fn unlock_file(file: &File) -> TelemetryResult<()> {
    FileExt::unlock(file).map_err(|error| storage_io("unlock object-store update lock", error))
}

pub(super) fn storage_io(context: &str, error: std::io::Error) -> TelemetryError {
    TelemetryError::StorageIo(format!("{context}: {error}"))
}

pub(super) fn object_io(key: &str, operation: &str, error: std::io::Error) -> TelemetryError {
    TelemetryError::ObjectStore(format!("{operation} object {key}: {error}"))
}

pub(super) fn write_cache_chunk_atomically(path: &Path, bytes: &[u8]) -> TelemetryResult<()> {
    let mut framed = Vec::with_capacity(bytes.len().saturating_add(CACHE_HEADER_BYTES));
    framed.extend_from_slice(CACHE_HEADER_MAGIC);
    framed.extend_from_slice(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    framed.extend_from_slice(blake3::hash(bytes).as_bytes());
    framed.extend_from_slice(bytes);
    write_bytes_atomically(path, &framed)
}

pub(super) fn read_cache_chunk(path: &Path) -> TelemetryResult<Vec<u8>> {
    let framed = fs::read(path).map_err(|error| storage_io("read SSD cache chunk", error))?;
    if framed.len() < CACHE_HEADER_BYTES || &framed[..8] != CACHE_HEADER_MAGIC {
        return Err(TelemetryError::CorruptTier(
            "SSD cache chunk header is invalid".into(),
        ));
    }
    let bytes = u64::from_le_bytes(
        framed[8..16]
            .try_into()
            .map_err(|_| TelemetryError::CorruptTier("SSD cache length is invalid".into()))?,
    );
    let payload = &framed[CACHE_HEADER_BYTES..];
    if u64::try_from(payload.len()).unwrap_or(u64::MAX) != bytes
        || blake3::hash(payload).as_bytes() != &framed[16..48]
    {
        return Err(TelemetryError::CorruptTier(
            "SSD cache chunk failed integrity verification".into(),
        ));
    }
    Ok(payload.to_vec())
}

pub(super) fn evict_locked(state: &mut CacheState, max_bytes: u64) -> TelemetryResult<()> {
    while state.used_bytes > max_bytes {
        let Some((key, _)) = state
            .entries
            .iter()
            .min_by_key(|(key, entry)| (entry.stamp, *key))
            .map(|(key, entry)| (key.clone(), entry.stamp))
        else {
            state.used_bytes = 0;
            break;
        };
        let entry = state
            .entries
            .remove(&key)
            .expect("selected cache entry still exists");
        state.used_bytes = state.used_bytes.saturating_sub(entry.bytes);
        remove_cache_file(&entry.path)?;
    }
    Ok(())
}

pub(super) fn remove_cache_file(path: &Path) -> TelemetryResult<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage_io("evict SSD cache chunk", error)),
    }
}
