use super::*;

pub(super) fn write_manifest(
    output_dir: &std::path::Path,
    entries: &mut [BlockResult],
) -> Result<u64, Box<dyn Error>> {
    entries.sort_unstable_by_key(|entry| entry.ordinal);
    let path = output_dir.join("manifest.bin");
    let mut manifest = OpenOptions::new().write(true).create_new(true).open(path)?;
    manifest.write_all(b"SLOGPACK2")?;
    manifest.write_all(&u64::try_from(entries.len())?.to_le_bytes())?;
    for entry in entries {
        manifest.write_all(&u64::try_from(entry.ordinal)?.to_le_bytes())?;
        manifest.write_all(&entry.source_offset.to_le_bytes())?;
        manifest.write_all(&entry.input_bytes.to_le_bytes())?;
        manifest.write_all(&entry.source_bytes.to_le_bytes())?;
        manifest.write_all(&entry.record_count.to_le_bytes())?;
        manifest.write_all(&entry.structural_bytes.to_le_bytes())?;
        manifest.write_all(&u64::try_from(entry.pack_worker)?.to_le_bytes())?;
        manifest.write_all(&entry.pack_offset.to_le_bytes())?;
        manifest.write_all(&entry.structural_stored_bytes.to_le_bytes())?;
        manifest.write_all(&entry.payload_checksum.to_le_bytes())?;
    }
    manifest.sync_all()?;
    Ok(manifest.metadata()?.len())
}

pub(super) fn encode_query_index(entries: &mut [BlockResult]) -> Result<Vec<u8>, Box<dyn Error>> {
    entries.sort_unstable_by_key(|entry| entry.ordinal);
    let blocks = entries
        .iter_mut()
        .filter_map(|entry| entry.query_index.take())
        .collect::<Vec<_>>();
    PersistentQueryIndex::from_blocks(blocks)?
        .encode_compressed(ZSTD_LEVEL)
        .map_err(Into::into)
}

pub(super) fn write_query_index(
    output_dir: &std::path::Path,
    entries: &mut [BlockResult],
) -> Result<u64, Box<dyn Error>> {
    let encoded = encode_query_index(entries)?;
    let path = output_dir.join("query-index.bin");
    let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
    output.write_all(&encoded)?;
    output.sync_all()?;
    Ok(output.metadata()?.len())
}

pub(super) fn write_dictionary_assignments(
    output_dir: &std::path::Path,
    entries: &mut [BlockResult],
) -> Result<u64, Box<dyn Error>> {
    entries.sort_unstable_by_key(|entry| entry.ordinal);
    let mut runs = Vec::<(usize, usize, DictionaryId)>::new();
    for entry in entries.iter() {
        let Some(dictionary_id) = entry.dictionary_id else {
            continue;
        };
        if let Some((start, length, previous_id)) = runs.last_mut()
            && *previous_id == dictionary_id
            && start.saturating_add(*length) == entry.ordinal
        {
            *length = length.saturating_add(1);
        } else {
            runs.push((entry.ordinal, 1, dictionary_id));
        }
    }
    if runs.is_empty() {
        return Ok(0);
    }
    let path = output_dir.join("dictionary-assignments.bin");
    let mut assignments = OpenOptions::new().write(true).create_new(true).open(path)?;
    assignments.write_all(b"SLOGDICT2")?;
    assignments.write_all(&u64::try_from(runs.len())?.to_le_bytes())?;
    for (start, length, dictionary_id) in runs {
        assignments.write_all(&u64::try_from(start)?.to_le_bytes())?;
        assignments.write_all(&u64::try_from(length)?.to_le_bytes())?;
        assignments.write_all(&dictionary_id.get().to_le_bytes())?;
    }
    assignments.sync_all()?;
    Ok(assignments.metadata()?.len())
}

pub(super) fn write_dictionaries(
    output_dir: &std::path::Path,
    snapshot: &shard_telemetry::DictionaryCatalogSnapshot,
) -> Result<u64, Box<dyn Error>> {
    let dictionaries = snapshot.dictionaries().collect::<Vec<_>>();
    if dictionaries.is_empty() {
        return Ok(0);
    }
    let directory = output_dir.join("dictionaries");
    std::fs::create_dir(&directory)?;
    let mut total = 0u64;
    for (dictionary_id, payload) in dictionaries {
        let path = directory.join(dictionary_file_name(dictionary_id));
        let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
        output.write_all(&payload)?;
        output.sync_all()?;
        total = total.saturating_add(u64::try_from(payload.len())?);
    }
    Ok(total)
}

pub(super) fn dictionary_file_name(dictionary_id: DictionaryId) -> String {
    format!("{:032x}.zdict", dictionary_id.get())
}

pub(super) fn verify_durable_output(output_dir: &std::path::Path) -> Result<u64, Box<dyn Error>> {
    let mut entries = read_manifest(output_dir)?;
    read_dictionary_assignments(output_dir, &mut entries)?;
    let query_index_path = output_dir.join("query-index.bin");
    if query_index_path.exists() {
        let query_index =
            PersistentQueryIndex::decode_compressed(&std::fs::read(query_index_path)?)?;
        for block in query_index.blocks() {
            let entry = entries
                .get(usize::try_from(block.block_ordinal)?)
                .ok_or("query index references an unknown manifest block")?;
            if entry.ordinal != usize::try_from(block.block_ordinal)?
                || entry.record_count != u64::from(block.record_count)
            {
                return Err("query index block metadata does not match the manifest".into());
            }
        }
    }
    for (ordinal, entry) in entries.iter().enumerate() {
        if entry.ordinal != ordinal {
            return Err("manifest block ordinals are not contiguous".into());
        }
    }
    for adjacent in entries.windows(2) {
        if adjacent[0]
            .source_offset
            .saturating_add(adjacent[0].input_bytes)
            != adjacent[1].source_offset
        {
            return Err("manifest source spans contain a gap or overlap".into());
        }
    }
    let mut packs = std::collections::HashMap::new();
    let sampled_ordinals = [
        entries.first().map(|entry| entry.ordinal),
        entries.get(entries.len() / 2).map(|entry| entry.ordinal),
        entries.last().map(|entry| entry.ordinal),
    ];
    for entry in &entries {
        let pack = match packs.entry(entry.pack_worker) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let worker_id = *entry.key();
                entry.insert(File::open(
                    output_dir.join(format!("worker-{worker_id:02}.pack")),
                )?)
            }
        };
        let mut payload = vec![0; usize::try_from(entry.structural_stored_bytes)?];
        pack.read_exact_at(&mut payload, entry.pack_offset)?;
        if fnv1a64(&payload) != entry.payload_checksum {
            return Err(format!("payload checksum mismatch for block {}", entry.ordinal).into());
        }
        if sampled_ordinals.contains(&Some(entry.ordinal)) {
            let dictionary = entry
                .dictionary_id
                .map(|dictionary_id| {
                    std::fs::read(
                        output_dir
                            .join("dictionaries")
                            .join(dictionary_file_name(dictionary_id)),
                    )
                })
                .transpose()?;
            let decoded =
                decode_locality_payload(&payload, entry.structural_bytes, dictionary.as_deref())?;
            if decoded.len() != usize::try_from(entry.record_count)? {
                return Err(format!(
                    "sampled block {} decoded {} records, expected {}",
                    entry.ordinal,
                    decoded.len(),
                    entry.record_count
                )
                .into());
            }
        }
    }
    Ok(u64::try_from(entries.len())?)
}

pub(super) fn read_dictionary_assignments(
    output_dir: &std::path::Path,
    entries: &mut [BlockResult],
) -> Result<(), Box<dyn Error>> {
    let path = output_dir.join("dictionary-assignments.bin");
    if !path.exists() {
        return Ok(());
    }
    const HEADER_BYTES: usize = 17;
    const ENTRY_BYTES: usize = 32;
    let assignments = File::open(path)?;
    let mut header = [0; HEADER_BYTES];
    assignments.read_exact_at(&mut header, 0)?;
    if &header[..9] != b"SLOGDICT2" {
        return Err("dictionary assignments have invalid magic".into());
    }
    let run_count = usize::try_from(u64::from_le_bytes(header[9..17].try_into()?))?;
    let expected_bytes = u64::try_from(HEADER_BYTES)?
        .saturating_add(u64::try_from(ENTRY_BYTES)?.saturating_mul(u64::try_from(run_count)?));
    if assignments.metadata()?.len() != expected_bytes {
        return Err("dictionary assignment length does not match run count".into());
    }
    let mut encoded = [0; ENTRY_BYTES];
    for index in 0..run_count {
        let offset = u64::try_from(HEADER_BYTES)?
            .saturating_add(u64::try_from(ENTRY_BYTES)?.saturating_mul(u64::try_from(index)?));
        assignments.read_exact_at(&mut encoded, offset)?;
        let start = usize::try_from(u64::from_le_bytes(encoded[..8].try_into()?))?;
        let length = usize::try_from(u64::from_le_bytes(encoded[8..16].try_into()?))?;
        let end = start
            .checked_add(length)
            .ok_or("dictionary assignment run overflows")?;
        let assigned = entries
            .get_mut(start..end)
            .ok_or("dictionary assignment run exceeds manifest")?;
        if assigned.is_empty() || assigned.iter().any(|entry| entry.dictionary_id.is_some()) {
            return Err("invalid or overlapping dictionary assignment run".into());
        }
        let dictionary_id = DictionaryId::new(u128::from_le_bytes(encoded[16..].try_into()?));
        for entry in assigned {
            entry.dictionary_id = Some(dictionary_id);
        }
    }
    Ok(())
}

pub(super) fn read_manifest(
    output_dir: &std::path::Path,
) -> Result<Vec<BlockResult>, Box<dyn Error>> {
    const HEADER_BYTES: usize = 17;
    const ENTRY_BYTES: usize = 80;

    let manifest = File::open(output_dir.join("manifest.bin"))?;
    let mut header = [0u8; HEADER_BYTES];
    manifest.read_exact_at(&mut header, 0)?;
    if &header[..9] != b"SLOGPACK2" {
        return Err("manifest has invalid magic".into());
    }
    let entry_count = usize::try_from(u64::from_le_bytes(header[9..17].try_into()?))?;
    let expected_bytes = u64::try_from(HEADER_BYTES)?
        .checked_add(
            u64::try_from(entry_count)?
                .checked_mul(u64::try_from(ENTRY_BYTES)?)
                .ok_or("manifest length overflow")?,
        )
        .ok_or("manifest length overflow")?;
    if manifest.metadata()?.len() != expected_bytes {
        return Err("manifest length does not match its entry count".into());
    }

    let mut entries = Vec::with_capacity(entry_count);
    let mut encoded = [0u8; ENTRY_BYTES];
    for index in 0..entry_count {
        let offset = u64::try_from(HEADER_BYTES)?
            .checked_add(
                u64::try_from(index)?
                    .checked_mul(u64::try_from(ENTRY_BYTES)?)
                    .ok_or("manifest offset overflow")?,
            )
            .ok_or("manifest offset overflow")?;
        manifest.read_exact_at(&mut encoded, offset)?;
        let mut cursor = 0usize;
        let mut next = || {
            let end = cursor + 8;
            let value = u64::from_le_bytes(
                encoded[cursor..end]
                    .try_into()
                    .expect("manifest field is eight bytes"),
            );
            cursor = end;
            value
        };
        entries.push(BlockResult {
            ordinal: usize::try_from(next())?,
            source_offset: next(),
            input_bytes: next(),
            source_bytes: next(),
            record_count: next(),
            rejected_records: 0,
            rejected_bytes: 0,
            structural_bytes: next(),
            embedded_index_bytes: 0,
            pack_worker: usize::try_from(next())?,
            pack_offset: next(),
            structural_stored_bytes: next(),
            payload_checksum: next(),
            dictionary_id: None,
            query_index: None,
            structural_compression_time: Duration::ZERO,
        });
    }
    Ok(entries)
}

pub(super) fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}
