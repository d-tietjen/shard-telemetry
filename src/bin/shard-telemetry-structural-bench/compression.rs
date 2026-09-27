use super::*;

pub(super) fn process_block(
    block: RawBlock<'_>,
    compressor: &mut BenchmarkCompressor,
    locality: &mut CompressionBlockCollator,
    persistent_query_index: bool,
) -> Result<(BlockResult, Vec<u8>), String> {
    let source_cohort = CompressionCohortId::UNCLASSIFIED;
    let estimated_records = block.raw.len() / 128 + 1;
    let mut parsed_records = Vec::<DockerStructuralRecord<'_>>::with_capacity(estimated_records);
    let mut message_cache = DockerMessageCache::new();
    let mut timestamp_cache = DockerTimestampPrefixCache::default();
    let mut locality_records = locality
        .is_enabled()
        .then(|| Vec::<CompressionLocalityRecord>::with_capacity(estimated_records));
    let mut local_offset = 0u64;
    let mut accepted_source_bytes = 0u64;
    let mut rejected_records = 0u64;
    let mut rejected_bytes = 0u64;
    let mut line_start = 0usize;
    let line_ends = memchr::memchr_iter(b'\n', block.raw)
        .map(|newline| newline + 1)
        .chain(std::iter::once(block.raw.len()));
    for line_end in line_ends {
        if line_end == line_start {
            continue;
        }
        let line = &block.raw[line_start..line_end];
        line_start = line_end;
        let line_bytes = u64::try_from(line.len()).map_err(|error| error.to_string())?;
        let (message, stream, timestamp_unix_nanos) = if let Some(parsed) =
            parse_canonical_docker_json(line, &mut message_cache, &mut timestamp_cache)
        {
            parsed
        } else {
            let docker: DockerJsonLine<'_> = match serde_json::from_slice(line) {
                Ok(docker) => docker,
                Err(_) => {
                    rejected_records = rejected_records.saturating_add(1);
                    rejected_bytes = rejected_bytes.saturating_add(line_bytes);
                    local_offset = local_offset.saturating_add(line_bytes);
                    continue;
                }
            };
            let timestamp_unix_nanos = match parse_docker_timestamp(&docker.time) {
                Ok(timestamp) => timestamp,
                Err(_) => {
                    rejected_records = rejected_records.saturating_add(1);
                    rejected_bytes = rejected_bytes.saturating_add(line_bytes);
                    local_offset = local_offset.saturating_add(line_bytes);
                    continue;
                }
            };
            (
                Rc::<str>::from(docker.log),
                docker.stream,
                timestamp_unix_nanos,
            )
        };
        let fingerprint = if locality.is_enabled() {
            fingerprint_message(&message, &[])
        } else {
            MessageFingerprint {
                shape_hash: 0,
                locality_signature: 0,
            }
        };
        if let Some(locality_records) = &mut locality_records {
            locality_records.push(CompressionLocalityRecord {
                fingerprint,
                source_bytes: line_bytes,
            });
        }
        parsed_records.push(DockerStructuralRecord {
            offset: LogicalOffset::new(block.source_offset.saturating_add(local_offset)),
            timestamp_unix_nanos,
            message,
            stream,
        });
        local_offset = local_offset.saturating_add(line_bytes);
        accepted_source_bytes = accepted_source_bytes.saturating_add(line_bytes);
    }
    let query_index = if persistent_query_index && !parsed_records.is_empty() {
        let first_offset = parsed_records
            .first()
            .expect("nonempty records have a first offset")
            .offset;
        let last_offset = parsed_records
            .last()
            .expect("nonempty records have a last offset")
            .offset;
        let (min_timestamp_unix_nanos, max_timestamp_unix_nanos) =
            parsed_records
                .iter()
                .fold((u64::MAX, 0u64), |(minimum, maximum), record| {
                    (
                        minimum.min(record.timestamp_unix_nanos),
                        maximum.max(record.timestamp_unix_nanos),
                    )
                });
        Some((
            QueryBlockMetadata {
                block_ordinal: u32::try_from(block.ordinal).map_err(|error| error.to_string())?,
                topic_partition: QUERY_PARTITION,
                first_offset,
                last_offset,
                min_timestamp_unix_nanos,
                max_timestamp_unix_nanos,
                record_count: u32::try_from(parsed_records.len())
                    .map_err(|error| error.to_string())?,
            },
            BlockQueryIndex::build(&parsed_records).map_err(|error| error.to_string())?,
        ))
    } else {
        None
    };
    let home = CompressionPlacementId::from_source_cohort(source_cohort);
    let (record_count, compressed_groups, structural_compression_time) =
        if let Some(locality_records) = locality_records {
            let mut groups =
                BTreeMap::<CompressionPlacementId, Vec<DockerStructuralRecord<'_>>>::new();
            let assignments = locality.collate(source_cohort, home, &locality_records);
            let mut record_placements = vec![home; parsed_records.len()];
            for assignment in assignments {
                for index in assignment.record_indices() {
                    record_placements[index] = assignment.placement.placement_id;
                }
            }
            for (record, placement_id) in parsed_records.into_iter().zip(record_placements) {
                groups.entry(placement_id).or_default().push(record);
            }
            if groups.is_empty() {
                groups.insert(home, Vec::new());
            }
            let record_count = groups.values().map(Vec::len).sum::<usize>();
            let started = Instant::now();
            let compressed_groups = compress_groups(&groups, compressor)?;
            let structural_compression_time = started.elapsed();
            if block.verify {
                let mut decoded = decode_locality_payload(
                    &compressed_groups.payload,
                    compressed_groups.structural_bytes,
                    compressed_groups.dictionary_payload.as_deref(),
                )?;
                decoded.sort_unstable_by_key(|record| record.offset);
                let mut expected = groups.values().flatten().collect::<Vec<_>>();
                expected.sort_unstable_by_key(|record| record.offset);
                verify_decoded_records(&decoded, expected.into_iter())?;
            }
            (record_count, compressed_groups, structural_compression_time)
        } else {
            let record_count = parsed_records.len();
            let started = Instant::now();
            let compressed_groups = compress_single_group(home, &parsed_records, compressor)?;
            let structural_compression_time = started.elapsed();
            if block.verify {
                let decoded = decode_locality_payload(
                    &compressed_groups.payload,
                    compressed_groups.structural_bytes,
                    compressed_groups.dictionary_payload.as_deref(),
                )?;
                verify_decoded_records(&decoded, parsed_records.iter())?;
            }
            (record_count, compressed_groups, structural_compression_time)
        };
    let structural_bytes = compressed_groups.structural_bytes;
    let embedded_index_bytes = compressed_groups.embedded_index_bytes;
    let compressed = compressed_groups.payload;
    let dictionary_id = compressed_groups.dictionary_id;
    let result = BlockResult {
        ordinal: block.ordinal,
        source_offset: block.source_offset,
        input_bytes: u64::try_from(block.raw.len()).map_err(|error| error.to_string())?,
        source_bytes: accepted_source_bytes,
        record_count: u64::try_from(record_count).map_err(|error| error.to_string())?,
        rejected_records,
        rejected_bytes,
        structural_bytes,
        embedded_index_bytes,
        structural_stored_bytes: u64::try_from(compressed.len())
            .map_err(|error| error.to_string())?,
        pack_worker: 0,
        pack_offset: 0,
        payload_checksum: fnv1a64(&compressed),
        dictionary_id,
        query_index,
        structural_compression_time,
    };
    Ok((result, compressed))
}

pub(super) fn verify_decoded_records<'a>(
    decoded: &[DecodedStructuralRecord],
    expected: impl ExactSizeIterator<Item = &'a DockerStructuralRecord<'a>>,
) -> Result<(), String> {
    if decoded.len() != expected.len()
        || decoded.iter().zip(expected).any(|(decoded, record)| {
            decoded.offset != record.offset
                || decoded.timestamp_unix_nanos != record.timestamp_unix_nanos
                || decoded.message.as_ref() != record.message.as_ref()
                || decoded.fields.len() != 1
                || decoded.fields[0].key.as_ref() != "docker.stream"
                || decoded.fields[0].value.as_ref() != record.stream
        })
    {
        return Err("structural first-block round trip failed".to_owned());
    }
    Ok(())
}

pub(super) fn compress_single_group(
    placement_id: CompressionPlacementId,
    records: &[DockerStructuralRecord<'_>],
    compressor: &mut BenchmarkCompressor,
) -> Result<CompressedGroups, String> {
    let indexed = encode_indexed_structural_records(records).map_err(|error| error.to_string())?;
    let embedded_index_bytes =
        u64::try_from(indexed.embedded_index_bytes).map_err(|error| error.to_string())?;
    let structural = indexed.structural;
    let structural_bytes = u64::try_from(structural.len()).map_err(|error| error.to_string())?;
    let frame = compressor.compress_log_block(placement_id, structural)?;
    Ok(CompressedGroups {
        structural_bytes,
        embedded_index_bytes,
        payload: frame.payload,
        dictionary_id: frame.dictionary_id,
        dictionary_payload: frame.dictionary_payload,
    })
}

pub(super) fn compress_groups(
    groups: &BTreeMap<CompressionPlacementId, Vec<DockerStructuralRecord<'_>>>,
    compressor: &mut BenchmarkCompressor,
) -> Result<CompressedGroups, String> {
    let mut frames = Vec::with_capacity(groups.len());
    let mut structural_total = 0u64;
    let mut embedded_index_total = 0u64;
    for (&placement_id, records) in groups {
        let indexed =
            encode_indexed_structural_records(records).map_err(|error| error.to_string())?;
        embedded_index_total = embedded_index_total.saturating_add(
            u64::try_from(indexed.embedded_index_bytes).map_err(|error| error.to_string())?,
        );
        let structural = indexed.structural;
        structural_total = structural_total
            .saturating_add(u64::try_from(structural.len()).map_err(|error| error.to_string())?);
        frames.push(compressor.compress_log_block(placement_id, structural)?);
    }
    if frames.len() == 1 {
        let frame = frames.pop().expect("one frame exists");
        return Ok(CompressedGroups {
            structural_bytes: structural_total,
            embedded_index_bytes: embedded_index_total,
            payload: frame.payload,
            dictionary_id: frame.dictionary_id,
            dictionary_payload: frame.dictionary_payload,
        });
    }
    if frames.iter().any(|frame| frame.dictionary_id.is_some()) {
        return Err(
            "real-time dictionaries cannot be combined with locality containers yet".to_owned(),
        );
    }

    let payload_capacity = frames.iter().fold(
        LOCALITY_CONTAINER_MAGIC.len() + size_of::<u32>(),
        |total, frame| {
            total
                .saturating_add(size_of::<u64>() * 2)
                .saturating_add(frame.payload.len())
        },
    );
    let mut payload = Vec::with_capacity(payload_capacity);
    payload.extend_from_slice(LOCALITY_CONTAINER_MAGIC);
    payload.extend_from_slice(
        &u32::try_from(frames.len())
            .map_err(|error| error.to_string())?
            .to_le_bytes(),
    );
    for frame in frames {
        payload.extend_from_slice(
            &u64::try_from(frame.structural_len)
                .map_err(|error| error.to_string())?
                .to_le_bytes(),
        );
        payload.extend_from_slice(
            &u64::try_from(frame.payload.len())
                .map_err(|error| error.to_string())?
                .to_le_bytes(),
        );
        payload.extend_from_slice(&frame.payload);
    }
    Ok(CompressedGroups {
        structural_bytes: structural_total,
        embedded_index_bytes: embedded_index_total,
        payload,
        dictionary_id: None,
        dictionary_payload: None,
    })
}

pub(super) fn decode_locality_payload(
    payload: &[u8],
    structural_total: u64,
    dictionary: Option<&[u8]>,
) -> Result<Vec<DecodedStructuralRecord>, String> {
    if !payload.starts_with(LOCALITY_CONTAINER_MAGIC) {
        let mut decompressor =
            zstd::bulk::Decompressor::with_dictionary(dictionary.unwrap_or_default())
                .map_err(|error| error.to_string())?;
        let structural = decompressor
            .decompress(
                payload,
                usize::try_from(structural_total).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
        return decode_structural_block(&structural).map_err(|error| error.to_string());
    }
    if dictionary.is_some() {
        return Err("a locality container cannot use one shared dictionary".to_owned());
    }

    let mut cursor = LOCALITY_CONTAINER_MAGIC.len();
    let count_end = cursor.saturating_add(size_of::<u32>());
    let frame_count = u32::from_le_bytes(
        payload
            .get(cursor..count_end)
            .ok_or("truncated locality frame count")?
            .try_into()
            .map_err(|_| "invalid locality frame count")?,
    );
    cursor = count_end;
    let mut decoded = Vec::new();
    let mut observed_structural = 0u64;
    for _ in 0..frame_count {
        let structural_len = read_container_u64(payload, &mut cursor)?;
        let compressed_len = read_container_u64(payload, &mut cursor)?;
        let compressed_len = usize::try_from(compressed_len).map_err(|error| error.to_string())?;
        let end = cursor
            .checked_add(compressed_len)
            .ok_or("locality frame length overflow")?;
        let frame = payload
            .get(cursor..end)
            .ok_or("truncated locality frame payload")?;
        cursor = end;
        let structural = zstd::bulk::decompress(
            frame,
            usize::try_from(structural_len).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        observed_structural = observed_structural.saturating_add(structural_len);
        decoded.extend(decode_structural_block(&structural).map_err(|error| error.to_string())?);
    }
    if cursor != payload.len() || observed_structural != structural_total {
        return Err("invalid locality frame container length".to_owned());
    }
    Ok(decoded)
}

pub(super) fn read_container_u64(payload: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let end = cursor
        .checked_add(size_of::<u64>())
        .ok_or("locality frame cursor overflow")?;
    let value = u64::from_le_bytes(
        payload
            .get(*cursor..end)
            .ok_or("truncated locality frame header")?
            .try_into()
            .map_err(|_| "invalid locality frame header")?,
    );
    *cursor = end;
    Ok(value)
}
