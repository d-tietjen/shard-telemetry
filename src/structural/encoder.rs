use super::*;

pub(super) fn select_templates(
    messages: &ParsedMessages<'_>,
) -> TelemetryResult<(Vec<TemplateEntry>, Vec<Option<usize>>)> {
    let mut hash_groups = HashMap::<u64, Vec<usize>>::new();
    let mut groups = Vec::<TemplateGroup>::new();
    let mut layout_groups = vec![None; messages.layouts.len()];
    for (layout_id, layout_group) in layout_groups.iter_mut().enumerate() {
        let message = &messages.layouts[layout_id];
        let group_id = if let Some(group_id) = *layout_group {
            group_id
        } else {
            let candidates = hash_groups.entry(message.signature_hash).or_default();
            let group_id = candidates
                .iter()
                .copied()
                .find(|group_id| {
                    same_template(&messages.layouts[groups[*group_id].representative], message)
                })
                .unwrap_or_else(|| {
                    let group_id = groups.len();
                    groups.push(TemplateGroup {
                        representative: layout_id,
                        count: 0,
                        template_id: None,
                    });
                    candidates.push(group_id);
                    group_id
                });
            *layout_group = Some(group_id);
            group_id
        };
        groups[group_id].count = groups[group_id]
            .count
            .saturating_add(messages.layout_counts[layout_id]);
    }

    let mut entries = Vec::new();
    for group in &mut groups {
        if group.count < 2 {
            continue;
        }
        let message = &messages.layouts[group.representative];
        let id = entries.len();
        entries.push(TemplateEntry {
            literals: message
                .literals
                .iter()
                .map(|range| message.message[range.clone()].to_vec())
                .collect(),
        });
        group.template_id = Some(id);
    }
    if entries.len() > usize::try_from(u32::MAX).expect("u32 fits usize") {
        return Err(TelemetryError::RecordTooLarge);
    }
    let template_ids = layout_groups
        .into_iter()
        .map(|group_id| group_id.and_then(|group_id| groups[group_id].template_id))
        .collect();
    Ok((entries, template_ids))
}

pub(super) fn same_template(left: &ParsedMessage<'_>, right: &ParsedMessage<'_>) -> bool {
    left.literals.len() == right.literals.len()
        && left
            .literals
            .iter()
            .zip(&right.literals)
            .all(|(left_range, right_range)| {
                left.message[left_range.clone()] == right.message[right_range.clone()]
            })
}

pub(super) fn build_attribute_tables<R: StructuralRecordView>(
    records: &[R],
) -> TelemetryResult<(AttributeTables, ResolvedFields, MembershipFilter)> {
    let mut keys = Vec::<Vec<u8>>::new();
    let mut key_ids = HashMap::<Vec<u8>, usize>::new();
    let mut value_counts = Vec::<AttributeValueCounts>::new();
    let mut resolved_fields = ResolvedFields {
        entries: Vec::new(),
        record_ends: Vec::with_capacity(records.len()),
    };
    let mut key_cache = [EMPTY_CACHED_ATTRIBUTE_KEY; ATTRIBUTE_KEY_CACHE_ENTRIES];
    let mut field_membership = MembershipFilter::new();
    for record in records {
        record.try_for_each_structural_field(|field_key, field_value| {
            let key = field_key.as_bytes();
            let address = key.as_ptr() as usize;
            let cache_slot =
                ((address >> 4) ^ address ^ key.len()) & (ATTRIBUTE_KEY_CACHE_ENTRIES - 1);
            let cached = key_cache[cache_slot];
            let key_id = if cached.key_id != EMPTY_ATTRIBUTE_KEY
                && cached.address == address
                && cached.length == key.len()
            {
                cached.key_id as usize
            } else {
                let key_id = match if keys.len() <= LINEAR_ATTRIBUTE_DICTIONARY_LIMIT {
                    keys.iter()
                        .position(|candidate| candidate.as_slice() == key)
                } else {
                    key_ids.get(key).copied()
                } {
                    Some(key_id) => key_id,
                    None => {
                        let key_id = keys.len();
                        let key = key.to_vec();
                        keys.push(key.clone());
                        key_ids.insert(key, key_id);
                        value_counts.push(AttributeValueCounts::default());
                        key_id
                    }
                };
                key_cache[cache_slot] = CachedAttributeKey {
                    address,
                    length: key.len(),
                    key_id: u32::try_from(key_id).map_err(|_| TelemetryError::RecordTooLarge)?,
                };
                key_id
            };
            let (value_id, inserted) = value_counts[key_id].increment(field_value.as_bytes())?;
            if inserted {
                field_membership.insert_pair(key, field_value.as_bytes());
            }
            resolved_fields.entries.push(ResolvedField {
                key_id: u32::try_from(key_id).map_err(|_| TelemetryError::RecordTooLarge)?,
                value_id,
            });
            Ok(())
        })?;
        resolved_fields.record_ends.push(
            u32::try_from(resolved_fields.entries.len())
                .map_err(|_| TelemetryError::RecordTooLarge)?,
        );
    }
    let mut values = Vec::with_capacity(keys.len());
    for counts in value_counts {
        values.push(counts.into_table()?);
    }
    Ok((
        AttributeTables { keys, values },
        resolved_fields,
        field_membership,
    ))
}

pub(super) fn encode_offsets<R: StructuralRecordView>(records: &[R]) -> TelemetryResult<Vec<u8>> {
    let mut encoded = Vec::new();
    let Some(first) = records.first() else {
        return Ok(encoded);
    };
    let mut previous = first.structural_offset().get();
    write_varint(previous, &mut encoded);
    for record in &records[1..] {
        let offset = record.structural_offset().get();
        let delta = offset
            .checked_sub(previous)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "offsets must increase",
            ))?;
        write_varint(delta, &mut encoded);
        previous = offset;
    }
    Ok(encoded)
}

pub(super) fn encode_timestamps<R: StructuralRecordView>(
    records: &[R],
) -> TelemetryResult<Vec<u8>> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let timestamps = records
        .iter()
        .map(StructuralRecordView::structural_timestamp_unix_nanos)
        .collect::<Vec<_>>();
    simple_compress(
        &timestamps,
        &ChunkConfig::default()
            .with_compression_level(TIMESTAMP_PCO_LEVEL)
            .with_mode_spec(ModeSpec::Classic)
            .with_delta_spec(DeltaSpec::TryConsecutive(1)),
    )
    .map_err(|error| {
        TelemetryError::CompressionFailed(format!("Pco timestamp encoding failed: {error}"))
    })
}

pub(super) fn encode_templates(templates: &[TemplateEntry]) -> TelemetryResult<Vec<u8>> {
    let mut encoded = Vec::new();
    write_varint(
        u64::try_from(templates.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        &mut encoded,
    );
    for template in templates {
        write_varint(
            u64::try_from(template.literals.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            &mut encoded,
        );
        for literal in &template.literals {
            append_bytes(&mut encoded, literal)?;
        }
    }
    Ok(encoded)
}

pub(super) fn encode_bodies(
    messages: &ParsedMessages<'_>,
    template_ids: &[Option<usize>],
) -> TelemetryResult<Vec<u8>> {
    let mut cached_bodies = Vec::with_capacity(messages.layouts.len());
    for (layout_id, message) in messages.layouts.iter().enumerate() {
        if messages.layout_counts[layout_id] < 2 {
            cached_bodies.push(None);
            continue;
        }
        let mut encoded = Vec::new();
        encode_body(message, &template_ids[layout_id], &mut encoded)?;
        cached_bodies.push(Some(encoded));
    }
    let mut payload = Vec::new();
    let mut checkpoints =
        Vec::with_capacity(messages.layout_ids.len().div_ceil(SEEK_CHECKPOINT_INTERVAL));
    for (record_ordinal, &layout_id) in messages.layout_ids.iter().enumerate() {
        if record_ordinal % SEEK_CHECKPOINT_INTERVAL == 0 {
            checkpoints.push(payload.len());
        }
        let layout_id = layout_id as usize;
        if let Some(cached) = &cached_bodies[layout_id] {
            payload.extend_from_slice(cached);
        } else {
            encode_body(
                &messages.layouts[layout_id],
                &template_ids[layout_id],
                &mut payload,
            )?;
        }
    }
    encode_seekable_record_lane(&payload, &checkpoints)
}

pub(super) fn encode_body(
    message: &ParsedMessage<'_>,
    template_id: &Option<usize>,
    encoded: &mut Vec<u8>,
) -> TelemetryResult<()> {
    match template_id {
        Some(_) => {
            for value in &message.values {
                append_bytes(encoded, &message.message[value.clone()])?;
            }
        }
        None => {
            append_bytes(encoded, message.message)?;
        }
    }
    Ok(())
}

pub(super) fn encode_attribute_tables(tables: &AttributeTables) -> TelemetryResult<Vec<u8>> {
    let mut encoded = Vec::new();
    write_varint(
        u64::try_from(tables.keys.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        &mut encoded,
    );
    for (key, values) in tables.keys.iter().zip(&tables.values) {
        append_bytes(&mut encoded, key)?;
        write_varint(
            u64::try_from(values.dictionary_len).map_err(|_| TelemetryError::RecordTooLarge)?,
            &mut encoded,
        );
        for value in values.dictionary() {
            append_bytes(&mut encoded, value)?;
        }
    }
    Ok(encoded)
}

pub(super) fn encode_fields(
    resolved: &ResolvedFields,
    tables: &AttributeTables,
    membership_filter: MembershipFilter,
) -> TelemetryResult<(Vec<u8>, ParsedFieldSets)> {
    let mut payload = Vec::new();
    let mut checkpoints = Vec::with_capacity(
        resolved
            .record_ends
            .len()
            .div_ceil(SEEK_CHECKPOINT_INTERVAL),
    );
    let mut indexed_pairs = Vec::<(u32, u32)>::new();
    let mut field_sets = Vec::<Vec<(u32, u32)>>::new();
    let mut field_set_ids = Vec::with_capacity(resolved.record_ends.len());
    let mut field_set_cache = [EMPTY_FIELD_SET; FIELD_SET_CACHE_ENTRIES];
    let mut field_start = 0_usize;
    for (record_ordinal, field_end) in resolved.record_ends.iter().copied().enumerate() {
        let field_end = field_end as usize;
        let record_fields = resolved.entries.get(field_start..field_end).ok_or(
            TelemetryError::InvalidBlockEncoding("resolved record field range is invalid"),
        )?;
        indexed_pairs.clear();
        if record_ordinal % SEEK_CHECKPOINT_INTERVAL == 0 {
            checkpoints.push(payload.len());
        }
        write_varint(
            u64::try_from(record_fields.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            &mut payload,
        );
        for field in record_fields {
            let key_id = field.key_id as usize;
            write_varint(u64::from(field.key_id), &mut payload);
            let values = tables
                .values
                .get(key_id)
                .ok_or(TelemetryError::InvalidBlockEncoding(
                    "resolved attribute key ID is out of range",
                ))?;
            let (value_id, field_value) = values.resolve(field.value_id)?;
            if value_id < values.dictionary_len {
                let value_id =
                    u32::try_from(value_id).map_err(|_| TelemetryError::RecordTooLarge)?;
                indexed_pairs.push((field.key_id, value_id));
                payload.push(DICTIONARY_ATTRIBUTE_VALUE);
                write_varint(u64::from(value_id), &mut payload);
            } else {
                payload.push(DIRECT_ATTRIBUTE_VALUE);
                append_bytes(&mut payload, field_value)?;
            }
        }
        field_start = field_end;
        let field_set_hash = hash_field_id_pairs(&indexed_pairs);
        let cache_slot = field_set_hash as usize & (FIELD_SET_CACHE_ENTRIES - 1);
        let cached = field_set_cache[cache_slot];
        let field_set_id =
            if cached != EMPTY_FIELD_SET && field_sets[cached as usize] == indexed_pairs {
                cached
            } else {
                let field_set_id =
                    u32::try_from(field_sets.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
                field_sets.push(indexed_pairs.clone());
                field_set_cache[cache_slot] = field_set_id;
                field_set_id
            };
        field_set_ids.push(field_set_id);
    }
    Ok((
        encode_seekable_record_lane(&payload, &checkpoints)?,
        ParsedFieldSets {
            sets: field_sets,
            set_ids: field_set_ids,
            membership_filter,
        },
    ))
}

pub(super) fn encode_typed_metadata<R: StructuralRecordView>(
    records: &[R],
) -> TelemetryResult<Vec<u8>> {
    if records
        .iter()
        .all(|record| record.structural_log_metadata().is_none())
    {
        return Ok(Vec::new());
    }
    let mut bodies = MetadataInterner::with_capacity(records.len());
    let mut attribute_sets = MetadataInterner::with_capacity(records.len());
    let mut resources = MetadataInterner::with_capacity(records.len());
    let mut scopes = MetadataInterner::with_capacity(records.len());
    let mut strings = MetadataInterner::with_capacity(records.len());
    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        let Some(metadata) = record.structural_log_metadata() else {
            rows.push(None);
            continue;
        };
        let body_id = match metadata.body {
            None => ABSENT_LOG_BODY_ID,
            Some(TelemetryValue::String(value))
                if value.as_ref() == record.structural_message() =>
            {
                MESSAGE_LOG_BODY_ID
            }
            Some(value) => bodies
                .intern(value, |candidate, value| candidate == value, Clone::clone)?
                .checked_add(LOG_BODY_DICTIONARY_ID_BASE)
                .ok_or(TelemetryError::RecordTooLarge)?,
        };
        let attributes_id = attribute_sets.intern(
            metadata.attributes,
            |candidate: &Arc<Vec<TelemetryAttribute>>, value| candidate.as_slice() == value,
            |value| Arc::new(value.to_vec()),
        )?;
        let resource_id = resources.intern(
            metadata.resource,
            |candidate: &Arc<ResourceContext>, value| candidate.as_ref() == value,
            |value| Arc::new(value.clone()),
        )?;
        let scope_id = scopes.intern(
            metadata.scope,
            |candidate: &Arc<ScopeContext>, value| candidate.as_ref() == value,
            |value| Arc::new(value.clone()),
        )?;
        let severity_text_id = intern_optional_string(&mut strings, metadata.severity_text)?;
        let event_name_id = intern_optional_string(&mut strings, metadata.event_name)?;
        let trace_id_from_fields = metadata.trace_id.is_some_and(|trace_id| {
            has_structural_hex_field(record, "otel.trace_id", trace_id.as_bytes())
        });
        let span_id_from_fields = metadata.span_id.is_some_and(|span_id| {
            has_structural_hex_field(record, "otel.span_id", span_id.as_bytes())
        });
        rows.push(Some(PackedLogMetadataRow {
            observed_timestamp_delta: metadata
                .observed_timestamp_unix_nanos
                .wrapping_sub(record.structural_timestamp_unix_nanos())
                as i64,
            body_id,
            attributes_id,
            resource_id,
            scope_id,
            severity_number: metadata.severity_number,
            severity_text_id,
            dropped_attributes_count: metadata.dropped_attributes_count,
            flags: metadata.flags,
            trace_id: (!trace_id_from_fields)
                .then_some(metadata.trace_id)
                .flatten(),
            trace_id_from_fields,
            span_id: (!span_id_from_fields).then_some(metadata.span_id).flatten(),
            span_id_from_fields,
            event_name_id,
        }));
    }
    let packed = PackedLogMetadata {
        bodies: bodies.into_values(),
        attribute_sets: attribute_sets.into_values(),
        resources: resources.into_values(),
        scopes: scopes.into_values(),
        strings: strings.into_values(),
        rows,
    };
    let raw = rmp_serde::to_vec(&packed)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    let compressed = TYPED_METADATA_COMPRESSOR.with_borrow_mut(|compressor| {
        compressor
            .compress(&raw)
            .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
    })?;
    let mut encoded = Vec::with_capacity(4 + compressed.len());
    encoded.extend_from_slice(
        &u32::try_from(raw.len())
            .map_err(|_| TelemetryError::RecordTooLarge)?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

pub(super) fn parse_message(message: &[u8]) -> ParsedMessage<'_> {
    let mut literals = Vec::new();
    let mut values = Vec::new();
    let mut terms = Vec::new();
    let mut literal_start = 0usize;
    let mut term_start = None;
    let mut cursor = 0usize;
    while cursor < message.len() {
        let start = cursor;
        let token = is_template_token_byte(message[cursor]);
        while cursor < message.len() && is_template_token_byte(message[cursor]) == token {
            if message.is_ascii() {
                match (term_start, message[cursor].is_ascii_alphanumeric()) {
                    (None, true) => term_start = Some(cursor),
                    (Some(start), false) => {
                        terms.push(start..cursor);
                        term_start = None;
                    }
                    _ => {}
                }
            }
            cursor += 1;
        }
        if token && is_variable_token(&message[start..cursor]) {
            literals.push(literal_start..start);
            values.push(start..cursor);
            literal_start = cursor;
        }
    }
    if let Some(start) = term_start {
        terms.push(start..message.len());
    }
    if !message.is_ascii() {
        terms = unicode_term_ranges(message);
    }
    literals.push(literal_start..message.len());
    let signature_hash = template_hash(message, &literals);
    ParsedMessage {
        message,
        signature_hash,
        literals,
        values,
        terms,
    }
}

pub(super) fn unicode_term_ranges(message: &[u8]) -> Vec<Range<usize>> {
    let message = std::str::from_utf8(message).expect("structural messages originate as UTF-8");
    let mut terms = Vec::new();
    let mut start = None;
    for (index, character) in message.char_indices() {
        match (start, character.is_alphanumeric()) {
            (None, true) => start = Some(index),
            (Some(term_start), false) => {
                terms.push(term_start..index);
                start = None;
            }
            _ => {}
        }
    }
    if let Some(term_start) = start {
        terms.push(term_start..message.len());
    }
    terms
}

pub(super) fn parse_messages<R: StructuralRecordView>(
    records: &[R],
) -> TelemetryResult<ParsedMessages<'_>> {
    let mut layouts = Vec::<ParsedMessage<'_>>::new();
    let mut layout_ids = Vec::with_capacity(records.len());
    let mut layout_counts = Vec::<usize>::new();
    let mut cache = [EMPTY_MESSAGE_LAYOUT; MESSAGE_LAYOUT_CACHE_ENTRIES];
    for record in records {
        let message = record.structural_message().as_bytes();
        let cache_slot = message_layout_cache_slot(message);
        let cached_layout = cache[cache_slot];
        let cached_layout_id = cached_layout as usize;
        let layout_id = if cached_layout != EMPTY_MESSAGE_LAYOUT
            && same_message_bytes(layouts[cached_layout_id].message, message)
        {
            cached_layout_id
        } else {
            let layout_id = layouts.len();
            let cached_layout =
                u32::try_from(layout_id).map_err(|_| TelemetryError::RecordTooLarge)?;
            layouts.push(parse_message(message));
            layout_counts.push(0);
            cache[cache_slot] = cached_layout;
            layout_id
        };
        layout_counts[layout_id] = layout_counts[layout_id].saturating_add(1);
        layout_ids.push(u32::try_from(layout_id).map_err(|_| TelemetryError::RecordTooLarge)?);
    }
    Ok(ParsedMessages {
        layouts,
        layout_ids,
        layout_counts,
    })
}

#[inline]
pub(super) fn same_message_bytes(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && (left.as_ptr() == right.as_ptr() || left == right)
}

#[inline]
pub(super) fn message_layout_cache_slot(message: &[u8]) -> usize {
    let length = message.len();
    if length >= 8 {
        let first = u64::from_le_bytes(message[..8].try_into().expect("length checked"));
        let last = u64::from_le_bytes(message[length - 8..].try_into().expect("length checked"));
        let mut hash =
            first ^ last.rotate_left(29) ^ (length as u64).wrapping_mul(0x9e37_79b1_85eb_ca87);
        hash ^= hash >> 32;
        return hash as usize & (MESSAGE_LAYOUT_CACHE_ENTRIES - 1);
    }
    let mut hash = (length as u64).wrapping_mul(0x9e37_79b1_85eb_ca87);
    for &byte in message {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as usize & (MESSAGE_LAYOUT_CACHE_ENTRIES - 1)
}

pub(super) fn is_template_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b':')
}

pub(super) fn is_variable_token(token: &[u8]) -> bool {
    token.iter().any(|byte| byte.is_ascii_digit())
}

pub(super) fn template_hash(message: &[u8], literals: &[Range<usize>]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in u64::try_from(literals.len())
        .expect("literal count fits u64")
        .to_le_bytes()
    {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    for literal in literals {
        for byte in u64::try_from(literal.len())
            .expect("literal length fits u64")
            .to_le_bytes()
        {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
        for byte in &message[literal.clone()] {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
}
