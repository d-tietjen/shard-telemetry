use super::*;

pub(super) fn fingerprint_distance(left: MessageFingerprint, right: MessageFingerprint) -> u8 {
    locality_distance(
        CompressionTemperature::new(left.locality_signature),
        left.shape_hash,
        CompressionTemperature::new(right.locality_signature),
        right.shape_hash,
    )
}

pub(super) fn locality_distance(
    left_temperature: CompressionTemperature,
    left_shape_hash: u64,
    right_temperature: CompressionTemperature,
    right_shape_hash: u64,
) -> u8 {
    left_temperature
        .distance(right_temperature)
        .saturating_add(u8::from(left_shape_hash != right_shape_hash) * SHAPE_MISMATCH_DISTANCE)
}

pub(super) fn score_membership(
    records: &[CompressionLocalityRecord],
    membership: &BlockMembership,
) -> ScoredMembership {
    let seed_a = membership
        .indices()
        .next()
        .expect("scored membership is nonempty");
    let mut seed_b = seed_a;
    let mut seed_distance = 0u8;
    let mut one_weights = [0u64; 16];
    let mut total_weight = 0u64;
    let mut source_bytes = 0u64;
    let mut record_count = 0usize;
    let mut shape_hash = records[seed_a].fingerprint.shape_hash;
    let mut shape_vote_weight = 0u64;
    for index in membership.indices() {
        let record = records[index];
        let weight = locality_weight(record.source_bytes);
        total_weight = total_weight.saturating_add(weight);
        source_bytes = source_bytes.saturating_add(record.source_bytes);
        record_count = record_count.saturating_add(1);
        for (bit, one_weight) in one_weights.iter_mut().enumerate() {
            if record.fingerprint.locality_signature & (1u16 << bit) != 0 {
                *one_weight = one_weight.saturating_add(weight);
            }
        }
        if record.fingerprint.shape_hash == shape_hash {
            shape_vote_weight = shape_vote_weight.saturating_add(weight);
        } else if shape_vote_weight > weight {
            shape_vote_weight -= weight;
        } else {
            shape_hash = record.fingerprint.shape_hash;
            shape_vote_weight = weight - shape_vote_weight;
        }
        let distance = fingerprint_distance(records[seed_a].fingerprint, record.fingerprint);
        if distance > seed_distance {
            seed_b = index;
            seed_distance = distance;
        }
    }
    let temperature =
        CompressionTemperature::new(centroid_from_weights(&one_weights, total_weight));
    let mut squared_distance_weight = 0u128;
    let mut max_deviation = 0u8;
    for index in membership.indices() {
        let record = records[index];
        let distance = locality_distance(
            temperature,
            shape_hash,
            CompressionTemperature::new(record.fingerprint.locality_signature),
            record.fingerprint.shape_hash,
        );
        max_deviation = max_deviation.max(distance);
        squared_distance_weight = squared_distance_weight.saturating_add(
            u128::from(distance)
                .saturating_mul(u128::from(distance))
                .saturating_mul(u128::from(locality_weight(record.source_bytes))),
        );
    }
    let variance_q8 = squared_distance_weight
        .saturating_mul(256)
        .checked_div(u128::from(total_weight.max(1)))
        .unwrap_or(u128::from(u16::MAX))
        .min(u128::from(u16::MAX));
    ScoredMembership {
        score: CompressionBlockScore {
            temperature,
            shape_hash,
            internal_variance_q8: u16::try_from(variance_q8).expect("variance was bounded to u16"),
            max_deviation,
            source_bytes,
            record_count,
        },
        seed_a,
        seed_b,
    }
}

pub(super) fn split_membership(
    records: &[CompressionLocalityRecord],
    membership: &BlockMembership,
    config: &CompressionLocalityConfig,
    scored: ScoredMembership,
) -> Option<(BlockMembership, BlockMembership)> {
    let seed_a = scored.seed_a;
    let seed_b = scored.seed_b;
    if seed_a == seed_b {
        return None;
    }
    let goes_left = |index: usize| {
        fingerprint_distance(records[index].fingerprint, records[seed_a].fingerprint)
            <= fingerprint_distance(records[index].fingerprint, records[seed_b].fingerprint)
    };

    let mut left_records = 0usize;
    let mut right_records = 0usize;
    let mut left_bytes = 0u64;
    let mut right_bytes = 0u64;
    let packed_capacity = membership.indices().len().saturating_mul(INDEX_BYTES);
    let mut left = BytesMut::with_capacity(packed_capacity);
    let mut right = BytesMut::with_capacity(packed_capacity / 2);
    for index in membership.indices() {
        if goes_left(index) {
            left_records = left_records.saturating_add(1);
            left_bytes = left_bytes.saturating_add(records[index].source_bytes);
            left.put_u32_le(
                u32::try_from(index).expect("a block cannot contain more than u32 rows"),
            );
        } else {
            right_records = right_records.saturating_add(1);
            right_bytes = right_bytes.saturating_add(records[index].source_bytes);
            right.put_u32_le(
                u32::try_from(index).expect("a block cannot contain more than u32 rows"),
            );
        }
    }
    if left_records < config.min_split_records
        || right_records < config.min_split_records
        || left_bytes < config.min_split_bytes
        || right_bytes < config.min_split_bytes
    {
        return None;
    }

    let left_len = left.len();
    left.unsplit(right);
    let packed = left;
    let max_len = packed.len().max(1);
    let mut handoff = HandoffBuffer::from_tail_with_policy(
        packed,
        HandoffBufferConfig::new(max_len),
        HandoffBufferPolicy::new().with_small_prefix_copy_max(0),
    )
    .expect("packed sub-block membership fits its exact handoff limit");
    let left = handoff
        .split_prefix(left_len)
        .expect("counted left prefix is present");
    let right = handoff.freeze_all();
    Some((
        BlockMembership::Packed(left),
        BlockMembership::Packed(right),
    ))
}

pub(super) fn locality_weight(source_bytes: u64) -> u64 {
    source_bytes.max(1).min(u64::from(u32::MAX))
}

pub(super) fn centroid_from_weights(one_weights: &[u64; 16], total_weight: u64) -> u16 {
    let mut centroid = 0u16;
    for (bit, one_weight) in one_weights.iter().enumerate() {
        if one_weight.saturating_mul(2) >= total_weight.max(1) {
            centroid |= 1u16 << bit;
        }
    }
    centroid
}

pub(super) fn estimated_state_bytes(config: &CompressionLocalityConfig) -> usize {
    config
        .max_compression_shards
        .saturating_mul(size_of::<CompressionShard>())
        .saturating_add(size_of::<[SplitExploration; SPLIT_EXPLORATION_SLOTS]>())
}
