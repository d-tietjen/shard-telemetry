use super::*;

pub(super) fn retain_top_timestamp_ordinals(
    ordinals: &mut Vec<u32>,
    partition: &PartitionIndex,
    query: &LogQuery,
    limit: usize,
) {
    if limit == 0 {
        ordinals.clear();
        return;
    }
    let keep = limit.min(ordinals.len());
    if keep < ordinals.len() {
        if partition.timestamp_order == TimestampOrder::NonDecreasing {
            match query.order {
                QueryOrder::OldestFirst => ordinals.truncate(keep),
                QueryOrder::NewestFirst => {
                    let mut selected = ordinals.split_off(ordinals.len() - keep);
                    selected.reverse();
                    *ordinals = selected;
                }
            }
            return;
        }
        let mut ascending = true;
        let mut descending = true;
        for pair in ordinals.windows(2) {
            match query.compare(
                &partition.records[pair[0] as usize].record,
                &partition.records[pair[1] as usize].record,
            ) {
                std::cmp::Ordering::Less => descending = false,
                std::cmp::Ordering::Greater => ascending = false,
                std::cmp::Ordering::Equal => {}
            }
            if !ascending && !descending {
                break;
            }
        }
        if ascending {
            ordinals.truncate(keep);
            return;
        }
        if descending {
            let mut selected = ordinals.split_off(ordinals.len() - keep);
            selected.reverse();
            *ordinals = selected;
            return;
        }
        ordinals.select_nth_unstable_by(keep - 1, |left, right| {
            compare_timestamp_ordinals(partition, query.order, *left, *right)
        });
        ordinals.truncate(keep);
    }
    ordinals.sort_unstable_by(|left, right| {
        compare_timestamp_ordinals(partition, query.order, *left, *right)
    });
}

pub(super) fn compare_timestamp_ordinals(
    partition: &PartitionIndex,
    order: QueryOrder,
    left: u32,
    right: u32,
) -> std::cmp::Ordering {
    let left = &partition
        .records
        .get(left as usize)
        .expect("indexed reference has a visible record")
        .record;
    let right = &partition
        .records
        .get(right as usize)
        .expect("indexed reference has a visible record")
        .record;
    let ordering = left
        .timestamp_unix_nanos
        .cmp(&right.timestamp_unix_nanos)
        .then_with(|| left.record_ref.offset.cmp(&right.record_ref.offset));
    match order {
        QueryOrder::OldestFirst => ordering,
        QueryOrder::NewestFirst => ordering.reverse(),
    }
}

pub(super) fn sort_and_limit_matches(matches: &mut Vec<LogMatch>, query: &LogQuery, limit: usize) {
    let already_sorted = matches
        .windows(2)
        .all(|pair| query.compare(&pair[0].record, &pair[1].record) != std::cmp::Ordering::Greater);
    if !already_sorted {
        matches.sort_unstable_by(|left, right| query.compare(&left.record, &right.record));
    }
    matches.truncate(limit);
}

pub(super) fn intersect_frame_candidates(current: &mut Option<Vec<u32>>, mut incoming: Vec<u32>) {
    let Some(existing) = current.as_mut() else {
        *current = Some(incoming);
        return;
    };
    if existing.len() > incoming.len() {
        std::mem::swap(existing, &mut incoming);
    }
    let mut existing_index = 0usize;
    let mut incoming_index = 0usize;
    let mut write_index = 0usize;
    while existing_index < existing.len() && incoming_index < incoming.len() {
        match existing[existing_index].cmp(&incoming[incoming_index]) {
            std::cmp::Ordering::Less => existing_index += 1,
            std::cmp::Ordering::Greater => incoming_index += 1,
            std::cmp::Ordering::Equal => {
                existing[write_index] = existing[existing_index];
                write_index += 1;
                existing_index += 1;
                incoming_index += 1;
            }
        }
    }
    existing.truncate(write_index);
}

pub(super) fn intersect_frame_candidate_slice(current: &mut Option<Vec<u32>>, incoming: &[u32]) {
    let Some(existing) = current.as_mut() else {
        *current = Some(incoming.to_vec());
        return;
    };
    if existing.len() <= incoming.len() {
        if existing.len().saturating_mul(4) < incoming.len() {
            existing.retain(|ordinal| incoming.binary_search(ordinal).is_ok());
            return;
        }
        let mut existing_index = 0usize;
        let mut incoming_index = 0usize;
        let mut write_index = 0usize;
        while existing_index < existing.len() && incoming_index < incoming.len() {
            match existing[existing_index].cmp(&incoming[incoming_index]) {
                std::cmp::Ordering::Less => existing_index += 1,
                std::cmp::Ordering::Greater => incoming_index += 1,
                std::cmp::Ordering::Equal => {
                    existing[write_index] = existing[existing_index];
                    write_index += 1;
                    existing_index += 1;
                    incoming_index += 1;
                }
            }
        }
        existing.truncate(write_index);
        return;
    }
    if incoming.len().saturating_mul(4) < existing.len() {
        let mut result = Vec::with_capacity(incoming.len());
        for ordinal in incoming {
            if existing.binary_search(ordinal).is_ok() {
                result.push(*ordinal);
            }
        }
        *existing = result;
        return;
    }
    let mut result = Vec::with_capacity(incoming.len());
    let mut existing_index = 0usize;
    let mut incoming_index = 0usize;
    while existing_index < existing.len() && incoming_index < incoming.len() {
        match existing[existing_index].cmp(&incoming[incoming_index]) {
            std::cmp::Ordering::Less => existing_index += 1,
            std::cmp::Ordering::Greater => incoming_index += 1,
            std::cmp::Ordering::Equal => {
                result.push(existing[existing_index]);
                existing_index += 1;
                incoming_index += 1;
            }
        }
    }
    *existing = result;
}
