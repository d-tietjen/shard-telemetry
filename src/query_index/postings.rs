use super::*;

pub(super) fn block_overlaps(metadata: &QueryBlockMetadata, query: &LogQuery) -> bool {
    query
        .start_offset
        .is_none_or(|start| metadata.last_offset >= start)
        && query
            .end_offset
            .is_none_or(|end| metadata.first_offset < end)
        && query
            .start_timestamp_unix_nanos
            .is_none_or(|start| metadata.max_timestamp_unix_nanos >= start)
        && query
            .end_timestamp_unix_nanos
            .is_none_or(|end| metadata.min_timestamp_unix_nanos < end)
}

pub(super) fn posting_for_block(
    postings: &[BlockPosting],
    block_ordinal: u32,
) -> Option<&PostingList> {
    postings
        .binary_search_by_key(&block_ordinal, |posting| posting.block_ordinal)
        .ok()
        .map(|index| &postings[index].record_ordinals)
}

pub(super) fn intersect_block_ordinals(candidates: &mut Vec<u32>, postings: &[BlockPosting]) {
    let mut candidate_index = 0usize;
    let mut posting_index = 0usize;
    let mut write_index = 0usize;
    while candidate_index < candidates.len() && posting_index < postings.len() {
        match candidates[candidate_index].cmp(&postings[posting_index].block_ordinal) {
            std::cmp::Ordering::Less => candidate_index += 1,
            std::cmp::Ordering::Greater => posting_index += 1,
            std::cmp::Ordering::Equal => {
                candidates[write_index] = candidates[candidate_index];
                write_index += 1;
                candidate_index += 1;
                posting_index += 1;
            }
        }
    }
    candidates.truncate(write_index);
}

pub(super) fn intersect_u32(candidates: &mut Vec<u32>, postings: &[u32]) {
    if candidates.len().saturating_mul(8) < postings.len() {
        let mut posting_index = 0usize;
        let mut write_index = 0usize;
        for candidate_index in 0..candidates.len() {
            let candidate = candidates[candidate_index];
            posting_index +=
                postings[posting_index..].partition_point(|posting| *posting < candidate);
            let Some(posting) = postings.get(posting_index) else {
                break;
            };
            if *posting == candidate {
                candidates[write_index] = candidate;
                write_index += 1;
                posting_index += 1;
            }
        }
        candidates.truncate(write_index);
        return;
    }
    let mut candidate_index = 0usize;
    let mut posting_index = 0usize;
    let mut write_index = 0usize;
    while candidate_index < candidates.len() && posting_index < postings.len() {
        match candidates[candidate_index].cmp(&postings[posting_index]) {
            std::cmp::Ordering::Less => candidate_index += 1,
            std::cmp::Ordering::Greater => posting_index += 1,
            std::cmp::Ordering::Equal => {
                candidates[write_index] = candidates[candidate_index];
                write_index += 1;
                candidate_index += 1;
                posting_index += 1;
            }
        }
    }
    candidates.truncate(write_index);
}

pub(super) fn intersect_posting(candidates: &mut Vec<u32>, posting: &PostingList) {
    match posting {
        PostingList::Ordinals(postings) => intersect_u32(candidates, postings),
        PostingList::Runs { runs, .. } => {
            let mut candidate_index = 0usize;
            let mut run_index = 0usize;
            let mut write_index = 0usize;
            while candidate_index < candidates.len() && run_index < runs.len() {
                let candidate = candidates[candidate_index];
                let run = runs[run_index];
                let run_end = run.start + run.length;
                if candidate < run.start {
                    candidate_index += 1;
                } else if candidate >= run_end {
                    run_index += 1;
                } else {
                    candidates[write_index] = candidate;
                    write_index += 1;
                    candidate_index += 1;
                }
            }
            candidates.truncate(write_index);
        }
        PostingList::Encoded {
            bytes,
            start,
            end,
            kind,
            checkpoints,
            ..
        } => {
            if candidates.len().saturating_mul(8) < posting.cardinality() {
                candidates.retain(|ordinal| posting.contains(*ordinal));
            } else {
                intersect_encoded_posting(candidates, &bytes[*start..*end], *kind, checkpoints);
            }
        }
    }
}

pub(super) fn intersect_encoded_posting(
    candidates: &mut Vec<u32>,
    encoded: &[u8],
    kind: u8,
    checkpoints: &[PostingCheckpoint],
) {
    if candidates.is_empty() {
        return;
    }
    let candidate_count = candidates.len();
    let mut candidate_index = 0usize;
    let mut write_index = 0usize;
    visit_encoded_ordered(encoded, kind, checkpoints, false, |ordinal| {
        while candidate_index < candidate_count && candidates[candidate_index] < ordinal {
            candidate_index += 1;
        }
        if candidate_index == candidate_count {
            return true;
        }
        if candidates[candidate_index] == ordinal {
            candidates[write_index] = ordinal;
            write_index += 1;
            candidate_index += 1;
        }
        false
    });
    candidates.truncate(write_index);
}

pub(super) fn intersect_postings_limited(
    postings: &[&PostingList],
    newest_first: bool,
    limit: usize,
) -> Vec<u32> {
    let mut matches = Vec::with_capacity(limit);
    let mut observe = |ordinal| {
        if postings[1..]
            .iter()
            .all(|posting| posting.contains(ordinal))
        {
            matches.push(ordinal);
        }
        matches.len() == limit
    };
    match postings[0] {
        PostingList::Ordinals(ordinals) => {
            if newest_first {
                for ordinal in ordinals.iter().rev().copied() {
                    if observe(ordinal) {
                        break;
                    }
                }
            } else {
                for ordinal in ordinals.iter().copied() {
                    if observe(ordinal) {
                        break;
                    }
                }
            }
        }
        PostingList::Runs { runs, .. } => {
            if newest_first {
                'outer_newest: for run in runs.iter().rev() {
                    for ordinal in (run.start..run.start + run.length).rev() {
                        if observe(ordinal) {
                            break 'outer_newest;
                        }
                    }
                }
            } else {
                'outer_oldest: for run in runs {
                    for ordinal in run.start..run.start + run.length {
                        if observe(ordinal) {
                            break 'outer_oldest;
                        }
                    }
                }
            }
        }
        PostingList::Encoded {
            bytes,
            start,
            end,
            kind,
            checkpoints,
            ..
        } => visit_encoded_ordered(
            &bytes[*start..*end],
            *kind,
            checkpoints,
            newest_first,
            &mut observe,
        ),
    }
    matches
}

pub(super) fn encoded_posting_contains(
    encoded: &[u8],
    kind: u8,
    checkpoints: &[PostingCheckpoint],
    ordinal: u32,
) -> bool {
    let mut cursor = 0;
    debug_assert_eq!(encoded.get(cursor).copied(), Some(kind));
    match kind {
        DELTA_POSTING => {
            let _ = read_byte(encoded, &mut cursor);
            let count = read_usize(encoded, &mut cursor).expect("validated posting count");
            let mut previous = 0u32;
            let mut index = 0usize;
            if let Some(checkpoint) = checkpoints.get(..).and_then(|checkpoints| {
                let index =
                    checkpoints.partition_point(|checkpoint| checkpoint.previous <= ordinal);
                index
                    .checked_sub(1)
                    .and_then(|index| checkpoints.get(index))
            }) {
                cursor = usize::try_from(checkpoint.byte_offset).expect("posting offset fits");
                previous = checkpoint.previous;
                index = usize::try_from(checkpoint.index).expect("posting index fits");
                if index > 0 && previous == ordinal {
                    return true;
                }
            }
            for _ in index..count {
                let delta = read_u32(encoded, &mut cursor).expect("validated posting delta");
                let observed = previous
                    .checked_add(delta)
                    .expect("validated posting ordinal");
                if observed == ordinal {
                    return true;
                }
                if observed > ordinal {
                    return false;
                }
                previous = observed;
            }
            false
        }
        RUN_POSTING => {
            let _ = read_byte(encoded, &mut cursor);
            let run_count = read_usize(encoded, &mut cursor).expect("validated run count");
            let mut previous_end = 0u32;
            let mut run_index = 0usize;
            if let Some(checkpoint) = checkpoints.get(..).and_then(|checkpoints| {
                let index =
                    checkpoints.partition_point(|checkpoint| checkpoint.previous <= ordinal);
                index
                    .checked_sub(1)
                    .and_then(|index| checkpoints.get(index))
            }) {
                cursor = usize::try_from(checkpoint.byte_offset).expect("posting offset fits");
                previous_end = checkpoint.previous;
                run_index = usize::try_from(checkpoint.index).expect("posting index fits");
            }
            for _ in run_index..run_count {
                let start = previous_end
                    .checked_add(read_u32(encoded, &mut cursor).expect("validated run start"))
                    .expect("validated run start");
                let length = read_u32(encoded, &mut cursor).expect("validated run length");
                let end = start.checked_add(length).expect("validated run end");
                if ordinal < start {
                    return false;
                }
                if ordinal < end {
                    return true;
                }
                previous_end = end;
            }
            false
        }
        _ => false,
    }
}

pub(super) fn take_encoded_ordered(
    encoded: &[u8],
    kind: u8,
    checkpoints: &[PostingCheckpoint],
    newest_first: bool,
    limit: usize,
) -> Vec<u32> {
    if limit == 0 {
        return Vec::new();
    }
    let mut ordinals = Vec::with_capacity(limit);
    visit_encoded_ordered(encoded, kind, checkpoints, newest_first, |ordinal| {
        ordinals.push(ordinal);
        ordinals.len() == limit
    });
    ordinals
}

pub(super) fn visit_encoded_ordered(
    encoded: &[u8],
    kind: u8,
    checkpoints: &[PostingCheckpoint],
    newest_first: bool,
    mut observe: impl FnMut(u32) -> bool,
) {
    let mut cursor = 0;
    let encoded_kind = read_byte(encoded, &mut cursor).expect("validated posting kind");
    debug_assert_eq!(encoded_kind, kind);
    if kind == DELTA_POSTING {
        let count = read_usize(encoded, &mut cursor).expect("validated posting count");
        if !newest_first {
            let mut previous = 0u32;
            for _ in 0..count {
                let delta = read_u32(encoded, &mut cursor).expect("validated posting delta");
                previous = previous
                    .checked_add(delta)
                    .expect("validated posting ordinal");
                if observe(previous) {
                    return;
                }
            }
            return;
        }
        if checkpoints.is_empty() {
            let mut previous = 0u32;
            let mut ordinals = Vec::with_capacity(count);
            for _ in 0..count {
                let delta = read_u32(encoded, &mut cursor).expect("validated posting delta");
                previous = previous
                    .checked_add(delta)
                    .expect("validated posting ordinal");
                ordinals.push(previous);
            }
            for ordinal in ordinals.into_iter().rev() {
                if observe(ordinal) {
                    return;
                }
            }
            return;
        }
        for checkpoint_index in (0..checkpoints.len()).rev() {
            let checkpoint = checkpoints[checkpoint_index];
            let end_index = checkpoints.get(checkpoint_index + 1).map_or(count, |next| {
                usize::try_from(next.index).expect("index fits")
            });
            let mut cursor = usize::try_from(checkpoint.byte_offset).expect("posting offset fits");
            let mut previous = checkpoint.previous;
            let mut ordinals = Vec::with_capacity(
                end_index.saturating_sub(usize::try_from(checkpoint.index).expect("index fits")),
            );
            for _ in usize::try_from(checkpoint.index).expect("index fits")..end_index {
                let delta = read_u32(encoded, &mut cursor).expect("validated posting delta");
                previous = previous
                    .checked_add(delta)
                    .expect("validated posting ordinal");
                ordinals.push(previous);
            }
            for ordinal in ordinals.into_iter().rev() {
                if observe(ordinal) {
                    return;
                }
            }
        }
        return;
    }

    debug_assert_eq!(kind, RUN_POSTING);
    let run_count = read_usize(encoded, &mut cursor).expect("validated run count");
    if !newest_first {
        let mut previous_end = 0u32;
        for _ in 0..run_count {
            let start = previous_end
                .checked_add(read_u32(encoded, &mut cursor).expect("validated run start"))
                .expect("validated run start");
            let length = read_u32(encoded, &mut cursor).expect("validated run length");
            previous_end = start.checked_add(length).expect("validated run end");
            for ordinal in start..previous_end {
                if observe(ordinal) {
                    return;
                }
            }
        }
        return;
    }
    let mut runs = Vec::with_capacity(run_count);
    let mut previous_end = 0u32;
    for _ in 0..run_count {
        let start = previous_end
            .checked_add(read_u32(encoded, &mut cursor).expect("validated run start"))
            .expect("validated run start");
        let length = read_u32(encoded, &mut cursor).expect("validated run length");
        previous_end = start.checked_add(length).expect("validated run end");
        runs.push(OrdinalRun { start, length });
    }
    for run in runs.iter().rev() {
        for ordinal in (run.start..run.start + run.length).rev() {
            if observe(ordinal) {
                return;
            }
        }
    }
}

pub(super) fn posting_runs(posting: &[u32]) -> Vec<OrdinalRun> {
    let mut runs = Vec::<OrdinalRun>::new();
    for ordinal in posting.iter().copied() {
        if let Some(run) = runs.last_mut()
            && run.start.saturating_add(run.length) == ordinal
        {
            run.length = run.length.saturating_add(1);
        } else {
            runs.push(OrdinalRun {
                start: ordinal,
                length: 1,
            });
        }
    }
    runs
}

pub(super) fn encode_posting(posting: &PostingList, encoded: &mut Vec<u8>) -> TelemetryResult<()> {
    if let PostingList::Runs { runs, .. } = posting {
        return encode_run_posting(runs, encoded);
    }
    if let PostingList::Encoded {
        bytes, start, end, ..
    } = posting
    {
        encoded.extend_from_slice(&bytes[*start..*end]);
        return Ok(());
    }
    let PostingList::Ordinals(posting) = posting else {
        unreachable!("run postings return above");
    };
    encoded.push(DELTA_POSTING);
    write_varint(
        u64::try_from(posting.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        encoded,
    );
    let mut previous = 0u32;
    for (index, ordinal) in posting.iter().copied().enumerate() {
        if index > 0 && ordinal <= previous {
            return Err(TelemetryError::InvalidBlockEncoding(
                "query posting is not ordered",
            ));
        }
        write_varint(u64::from(ordinal - previous), encoded);
        previous = ordinal;
    }
    Ok(())
}

pub(super) fn encode_run_posting(
    runs: &[OrdinalRun],
    encoded: &mut Vec<u8>,
) -> TelemetryResult<()> {
    if runs.is_empty() {
        return Err(TelemetryError::InvalidBlockEncoding("empty query posting"));
    }
    encoded.push(RUN_POSTING);
    write_varint(
        u64::try_from(runs.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        encoded,
    );
    let mut previous_end = 0u32;
    for run in runs {
        write_varint(u64::from(run.start - previous_end), encoded);
        write_varint(u64::from(run.length), encoded);
        previous_end = run.start.saturating_add(run.length);
    }
    Ok(())
}

pub(super) fn decode_posting(
    encoded: &[u8],
    cursor: &mut usize,
    record_count: u32,
) -> TelemetryResult<PostingList> {
    match read_byte(encoded, cursor)? {
        DELTA_POSTING => {
            let count = read_usize(encoded, cursor)?;
            ensure_count(count, encoded.len().saturating_sub(*cursor))?;
            let mut posting = Vec::with_capacity(count);
            let mut previous = 0u32;
            for index in 0..count {
                let delta = read_u32(encoded, cursor)?;
                let ordinal =
                    previous
                        .checked_add(delta)
                        .ok_or(TelemetryError::InvalidBlockEncoding(
                            "query posting delta overflow",
                        ))?;
                if ordinal >= record_count || (index > 0 && ordinal <= previous) {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "invalid query posting ordinal",
                    ));
                }
                posting.push(ordinal);
                previous = ordinal;
            }
            if posting.is_empty() {
                return Err(TelemetryError::InvalidBlockEncoding("empty query posting"));
            }
            Ok(PostingList::Ordinals(posting))
        }
        RUN_POSTING => {
            let run_count = read_usize(encoded, cursor)?;
            ensure_count(run_count, encoded.len().saturating_sub(*cursor))?;
            let mut runs = Vec::with_capacity(run_count);
            let mut cardinality = 0usize;
            let mut previous_end = 0u32;
            for _ in 0..run_count {
                let start = previous_end.checked_add(read_u32(encoded, cursor)?).ok_or(
                    TelemetryError::InvalidBlockEncoding("query posting run overflow"),
                )?;
                let length = read_u32(encoded, cursor)?;
                let end = start
                    .checked_add(length)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "query posting run overflow",
                    ))?;
                if length == 0 || end > record_count {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "invalid query posting run",
                    ));
                }
                cardinality = cardinality
                    .checked_add(usize::try_from(length).map_err(|_| {
                        TelemetryError::InvalidBlockEncoding(
                            "query posting cardinality does not fit usize",
                        )
                    })?)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "query posting cardinality overflow",
                    ))?;
                runs.push(OrdinalRun { start, length });
                previous_end = end;
            }
            if runs.is_empty() {
                return Err(TelemetryError::InvalidBlockEncoding("empty query posting"));
            }
            Ok(PostingList::Runs { runs, cardinality })
        }
        _ => Err(TelemetryError::InvalidBlockEncoding(
            "unknown query posting encoding",
        )),
    }
}

pub(super) fn decode_posting_for_directory(
    encoded: &[u8],
    cursor: &mut usize,
    record_count: u32,
    backing: Option<&Arc<[u8]>>,
) -> TelemetryResult<PostingList> {
    let start = *cursor;
    if let Some(bytes) = backing {
        let (kind, cardinality, end, checkpoints) =
            scan_posting(encoded, cursor, record_count, start)?;
        return Ok(PostingList::Encoded {
            bytes: Arc::clone(bytes),
            start,
            end,
            kind,
            cardinality,
            checkpoints,
        });
    }
    decode_posting(encoded, cursor, record_count)
}

pub(super) fn scan_posting(
    encoded: &[u8],
    cursor: &mut usize,
    record_count: u32,
    posting_start: usize,
) -> TelemetryResult<(u8, usize, usize, Arc<[PostingCheckpoint]>)> {
    let kind = read_byte(encoded, cursor)?;
    match kind {
        DELTA_POSTING => {
            let count = read_usize(encoded, cursor)?;
            ensure_count(count, encoded.len().saturating_sub(*cursor))?;
            let mut previous = 0u32;
            let mut checkpoints = if count > 256 {
                Vec::with_capacity(count.div_ceil(128))
            } else {
                Vec::new()
            };
            for index in 0..count {
                let byte_offset = (*cursor).checked_sub(posting_start).ok_or(
                    TelemetryError::InvalidBlockEncoding("query posting checkpoint underflow"),
                )?;
                if index % 128 == 0 {
                    checkpoints.push(PostingCheckpoint {
                        index: u32::try_from(index).map_err(|_| TelemetryError::RecordTooLarge)?,
                        previous,
                        byte_offset: u32::try_from(byte_offset)
                            .map_err(|_| TelemetryError::RecordTooLarge)?,
                    });
                }
                let delta = read_u32(encoded, cursor)?;
                let ordinal =
                    previous
                        .checked_add(delta)
                        .ok_or(TelemetryError::InvalidBlockEncoding(
                            "query posting delta overflow",
                        ))?;
                if ordinal >= record_count || (index > 0 && ordinal <= previous) {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "invalid query posting ordinal",
                    ));
                }
                previous = ordinal;
            }
            if count == 0 {
                return Err(TelemetryError::InvalidBlockEncoding("empty query posting"));
            }
            Ok((kind, count, *cursor, checkpoints.into()))
        }
        RUN_POSTING => {
            let run_count = read_usize(encoded, cursor)?;
            ensure_count(run_count, encoded.len().saturating_sub(*cursor))?;
            let mut cardinality = 0usize;
            let mut previous_end = 0u32;
            let mut checkpoints = if run_count > 64 {
                Vec::with_capacity(run_count.div_ceil(32))
            } else {
                Vec::new()
            };
            for index in 0..run_count {
                let byte_offset = (*cursor).checked_sub(posting_start).ok_or(
                    TelemetryError::InvalidBlockEncoding("query posting checkpoint underflow"),
                )?;
                if index % 32 == 0 {
                    checkpoints.push(PostingCheckpoint {
                        index: u32::try_from(index).map_err(|_| TelemetryError::RecordTooLarge)?,
                        previous: previous_end,
                        byte_offset: u32::try_from(byte_offset)
                            .map_err(|_| TelemetryError::RecordTooLarge)?,
                    });
                }
                let start = previous_end.checked_add(read_u32(encoded, cursor)?).ok_or(
                    TelemetryError::InvalidBlockEncoding("query posting run overflow"),
                )?;
                let length = read_u32(encoded, cursor)?;
                let end = start
                    .checked_add(length)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "query posting run overflow",
                    ))?;
                if length == 0 || end > record_count {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "invalid query posting run",
                    ));
                }
                cardinality = cardinality
                    .checked_add(usize::try_from(length).map_err(|_| {
                        TelemetryError::InvalidBlockEncoding(
                            "query posting cardinality does not fit usize",
                        )
                    })?)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "query posting cardinality overflow",
                    ))?;
                previous_end = end;
            }
            if run_count == 0 {
                return Err(TelemetryError::InvalidBlockEncoding("empty query posting"));
            }
            Ok((kind, cardinality, *cursor, checkpoints.into()))
        }
        _ => Err(TelemetryError::InvalidBlockEncoding(
            "unknown query posting encoding",
        )),
    }
}
