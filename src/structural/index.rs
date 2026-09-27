use super::*;

impl EmbeddedFrameIndex {
    /// Number of records addressed by this frame index.
    #[must_use]
    pub const fn record_count(&self) -> u32 {
        self.record_count
    }

    /// Returns whether record ordinals already have exact timestamp/offset
    /// order. Ordered top-k queries can then choose candidate ordinals without
    /// decoding the Pco position lanes first.
    #[must_use]
    pub const fn timestamp_offset_ordinal_ordered(&self) -> bool {
        self.timestamp_offset_ordinal_ordered
    }

    pub(crate) fn cache_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.layout_ids.values.capacity())
            .saturating_add(
                self.residual_layout_ids
                    .capacity()
                    .saturating_mul(size_of::<u32>()),
            )
            .saturating_add(
                self.term_membership
                    .words
                    .len()
                    .saturating_mul(size_of::<u64>()),
            )
            .saturating_add(
                self.terms
                    .capacity()
                    .saturating_mul(size_of::<EmbeddedTermLocator>()),
            )
            .saturating_add(
                self.terms
                    .iter()
                    .map(|locator| {
                        locator
                            .layout_ids
                            .capacity()
                            .saturating_mul(size_of::<u32>())
                    })
                    .sum::<usize>(),
            )
            .saturating_add(self.field_set_ids.values.capacity())
            .saturating_add(
                self.field_membership
                    .words
                    .len()
                    .saturating_mul(size_of::<u64>()),
            )
            .saturating_add(
                self.fields
                    .capacity()
                    .saturating_mul(size_of::<EmbeddedFieldLocator>()),
            )
            .saturating_add(
                self.fields
                    .iter()
                    .map(|locator| {
                        locator
                            .field_set_ids
                            .capacity()
                            .saturating_mul(size_of::<u32>())
                    })
                    .sum::<usize>(),
            )
    }

    /// Returns a lossless candidate superset for a case-insensitive token.
    #[must_use]
    pub fn term_candidate_ordinals(&self, term: &str) -> Vec<u32> {
        let normalized = normalize_index_term(term);
        if !self.term_membership.might_contain(normalized.as_bytes()) {
            return Vec::new();
        }
        let mut selected = self.residual_layout_ids.clone();
        let fingerprint = index_fingerprint(membership_hash(normalized.as_bytes()));
        if let Ok(position) = self
            .terms
            .binary_search_by_key(&fingerprint, |locator| locator.fingerprint)
        {
            selected.extend_from_slice(&self.terms[position].layout_ids);
        }
        selected.sort_unstable();
        selected.dedup();
        matching_packed_ids(
            &self.layout_ids,
            self.record_count,
            self.layout_count,
            &selected,
        )
        .expect("validated embedded layout column")
    }

    /// Returns the template layouts whose static literals contain a candidate
    /// for `term`. Records that use the fallback layout or a layout with
    /// dynamic values are deliberately excluded; callers can verify those
    /// records after selective decode.
    pub(crate) fn term_layout_ids(&self, term: &str) -> Vec<u32> {
        let normalized = normalize_index_term(term);
        if !self.term_membership.might_contain(normalized.as_bytes()) {
            return Vec::new();
        }
        let fingerprint = index_fingerprint(membership_hash(normalized.as_bytes()));
        self.terms
            .binary_search_by_key(&fingerprint, |locator| locator.fingerprint)
            .ok()
            .map(|position| self.terms[position].layout_ids.clone())
            .unwrap_or_default()
    }

    pub(crate) fn term_might_contain(&self, term: &str) -> bool {
        let normalized = normalize_index_term(term);
        self.term_membership.might_contain(normalized.as_bytes())
    }

    pub(crate) fn residual_layout_ids(&self) -> &[u32] {
        &self.residual_layout_ids
    }

    /// Materializes record ordinals for a set of validated layout IDs.
    pub(crate) fn record_ordinals_for_layout_ids(&self, layout_ids: &[u32]) -> Vec<u32> {
        matching_packed_ids(
            &self.layout_ids,
            self.record_count,
            self.layout_count,
            layout_ids,
        )
        .expect("validated embedded layout column")
    }

    /// Returns a lossless candidate superset for the union of token terms.
    ///
    /// The union is resolved against the packed layout column in one pass so
    /// OR predicates do not allocate and scan one ordinal vector per term.
    #[must_use]
    pub fn term_candidate_ordinals_union(&self, terms: &[&str]) -> Vec<u32> {
        if terms.is_empty() || self.record_count == 0 {
            return Vec::new();
        }
        let mut selected_ids =
            vec![false; usize::try_from(self.layout_count).expect("validated layout count")];
        let mut any_term = false;
        for term in terms {
            let normalized = normalize_index_term(term);
            if !self.term_membership.might_contain(normalized.as_bytes()) {
                continue;
            }
            any_term = true;
            for layout_id in &self.residual_layout_ids {
                selected_ids[*layout_id as usize] = true;
            }
            let fingerprint = index_fingerprint(membership_hash(normalized.as_bytes()));
            if let Ok(position) = self
                .terms
                .binary_search_by_key(&fingerprint, |locator| locator.fingerprint)
            {
                for layout_id in &self.terms[position].layout_ids {
                    selected_ids[*layout_id as usize] = true;
                }
            }
        }
        if !any_term {
            return Vec::new();
        }
        let mut ordinals = Vec::new();
        for ordinal in 0..self.record_count {
            if selected_ids[packed_id(&self.layout_ids, ordinal) as usize] {
                ordinals.push(ordinal);
            }
        }
        ordinals
    }

    /// Returns a lossless candidate superset for a metadata key/value pair.
    #[must_use]
    pub fn field_candidate_ordinals(&self, key: &str, value: &str) -> Vec<u32> {
        if !self
            .field_membership
            .might_contain_pair(key.as_bytes(), value.as_bytes())
        {
            return Vec::new();
        }
        let fingerprint = index_fingerprint(membership_pair_hash(key.as_bytes(), value.as_bytes()));
        let Ok(position) = self
            .fields
            .binary_search_by_key(&fingerprint, |locator| locator.fingerprint)
        else {
            return (0..self.record_count).collect();
        };
        matching_packed_ids(
            &self.field_set_ids,
            self.record_count,
            self.field_set_count,
            &self.fields[position].field_set_ids,
        )
        .expect("validated embedded field-set column")
    }

    /// Encoded bytes occupied by the embedded frame-index section.
    pub fn encoded_bytes(&self) -> TelemetryResult<Vec<u8>> {
        self.encode()
    }

    pub(super) fn build(
        messages: &ParsedMessages<'_>,
        template_ids: &[Option<usize>],
        attributes: &AttributeTables,
        fields: &ParsedFieldSets,
        timestamp_offset_ordinal_ordered: bool,
    ) -> TelemetryResult<Self> {
        let record_count =
            u32::try_from(messages.layout_ids.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
        if template_ids.len() != messages.layouts.len()
            || fields.set_ids.len() != messages.layout_ids.len()
        {
            return Err(TelemetryError::InvalidBlockEncoding(
                "embedded index column count mismatch",
            ));
        }
        let template_count = template_ids
            .iter()
            .flatten()
            .copied()
            .max()
            .map_or(0usize, |maximum| maximum.saturating_add(1));
        let fallback_layout_id =
            u32::try_from(template_count).map_err(|_| TelemetryError::RecordTooLarge)?;
        let layout_count = if record_count == 0 {
            0
        } else {
            fallback_layout_id
                .checked_add(1)
                .ok_or(TelemetryError::RecordTooLarge)?
        };
        let field_set_count =
            u32::try_from(fields.sets.len()).map_err(|_| TelemetryError::RecordTooLarge)?;

        let mut term_layouts = HashMap::<u32, Vec<u32>>::new();
        let mut term_membership = MembershipFilter::new();
        let mut residual_layout_ids = if record_count == 0 {
            Vec::new()
        } else {
            vec![fallback_layout_id]
        };
        for (layout_id, message) in messages.layouts.iter().enumerate() {
            let Some(template_id) = template_ids[layout_id] else {
                for term in &message.terms {
                    let term =
                        std::str::from_utf8(&message.message[term.clone()]).map_err(|_| {
                            TelemetryError::InvalidBlockEncoding("message term is invalid UTF-8")
                        })?;
                    let normalized = normalize_index_term(term);
                    term_membership.insert(normalized.as_bytes());
                }
                continue;
            };
            let template_id =
                u32::try_from(template_id).map_err(|_| TelemetryError::RecordTooLarge)?;
            if !message.values.is_empty() {
                residual_layout_ids.push(template_id);
            }
            for term_range in &message.terms {
                let is_dynamic = message
                    .values
                    .iter()
                    .any(|value| term_range.start < value.end && value.start < term_range.end);
                let term =
                    std::str::from_utf8(&message.message[term_range.clone()]).map_err(|_| {
                        TelemetryError::InvalidBlockEncoding("message term is invalid UTF-8")
                    })?;
                let normalized = normalize_index_term(term);
                term_membership.insert(normalized.as_bytes());
                if is_dynamic {
                    continue;
                }
                let fingerprint = index_fingerprint(membership_hash(normalized.as_bytes()));
                let layouts = term_layouts.entry(fingerprint).or_default();
                if layouts.last().copied() != Some(template_id) {
                    layouts.push(template_id);
                }
            }
        }
        residual_layout_ids.sort_unstable();
        residual_layout_ids.dedup();
        let mut terms = term_layouts
            .into_iter()
            .map(|(fingerprint, mut layout_ids)| {
                layout_ids.sort_unstable();
                layout_ids.dedup();
                EmbeddedTermLocator {
                    fingerprint,
                    layout_ids,
                }
            })
            .collect::<Vec<_>>();
        terms.sort_unstable_by_key(|locator| locator.fingerprint);

        let mut field_sets = HashMap::<u32, Vec<u32>>::new();
        for (field_set_id, field_set) in fields.sets.iter().enumerate() {
            let field_set_id =
                u32::try_from(field_set_id).map_err(|_| TelemetryError::RecordTooLarge)?;
            for (key_id, value_id) in field_set {
                let key = attributes.keys.get(*key_id as usize).ok_or(
                    TelemetryError::InvalidBlockEncoding("embedded field key ID is out of range"),
                )?;
                let value = attributes
                    .values
                    .get(*key_id as usize)
                    .and_then(|values| values.dictionary().get(*value_id as usize))
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "embedded field value ID is out of range",
                    ))?;
                let fingerprint = index_fingerprint(membership_pair_hash(key, value));
                let set_ids = field_sets.entry(fingerprint).or_default();
                if set_ids.last().copied() != Some(field_set_id) {
                    set_ids.push(field_set_id);
                }
            }
        }
        let mut field_locators = field_sets
            .into_iter()
            .map(|(fingerprint, field_set_ids)| EmbeddedFieldLocator {
                fingerprint,
                field_set_ids,
            })
            .collect::<Vec<_>>();
        field_locators.sort_unstable_by_key(|locator| locator.fingerprint);

        term_membership.merge(&fields.membership_filter);
        let field_membership = term_membership.clone();
        Ok(Self {
            record_count,
            timestamp_offset_ordinal_ordered,
            layout_count,
            layout_ids: pack_ids(
                &messages
                    .layout_ids
                    .iter()
                    .map(|layout_id| {
                        template_ids[*layout_id as usize]
                            .and_then(|template_id| u32::try_from(template_id).ok())
                            .unwrap_or(fallback_layout_id)
                    })
                    .collect::<Vec<_>>(),
                layout_count,
            )?,
            residual_layout_ids,
            term_membership,
            terms,
            field_set_count,
            field_set_ids: pack_ids(&fields.set_ids, field_set_count)?,
            field_membership,
            fields: field_locators,
        })
    }

    pub(super) fn encode(&self) -> TelemetryResult<Vec<u8>> {
        let mut encoded = Vec::new();
        encode_packed_column(
            &self.layout_ids,
            self.layout_count,
            self.record_count,
            self.timestamp_offset_ordinal_ordered,
            &mut encoded,
        )?;
        encode_optional_sorted_ids(&self.residual_layout_ids, self.layout_count, &mut encoded)?;
        encode_membership_filter(&self.term_membership, &mut encoded);
        write_varint(
            u64::try_from(self.terms.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            &mut encoded,
        );
        for locator in &self.terms {
            encode_index_fingerprint(locator.fingerprint, &mut encoded);
            encode_sorted_ids(&locator.layout_ids, self.layout_count, &mut encoded)?;
        }
        encode_packed_column(
            &self.field_set_ids,
            self.field_set_count,
            self.record_count,
            false,
            &mut encoded,
        )?;
        write_varint(
            u64::try_from(self.fields.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            &mut encoded,
        );
        for locator in &self.fields {
            encode_index_fingerprint(locator.fingerprint, &mut encoded);
            encode_sorted_ids(&locator.field_set_ids, self.field_set_count, &mut encoded)?;
        }
        Ok(encoded)
    }

    pub(super) fn decode(encoded: &[u8], record_count: u32) -> TelemetryResult<Self> {
        let mut cursor = 0;
        let (layout_count, layout_ids, timestamp_offset_ordinal_ordered) =
            decode_packed_column(encoded, &mut cursor, record_count, true)?;
        let residual_layout_ids = decode_optional_sorted_ids(encoded, &mut cursor, layout_count)?;
        let term_membership = decode_membership_filter(encoded, &mut cursor)?;
        let term_count = read_usize(encoded, &mut cursor)?;
        ensure_count_within(
            term_count,
            encoded.len().saturating_sub(cursor),
            "embedded term count",
        )?;
        let mut terms = Vec::with_capacity(term_count);
        for _ in 0..term_count {
            let fingerprint = decode_index_fingerprint(encoded, &mut cursor)?;
            let layout_ids = decode_sorted_ids(encoded, &mut cursor, layout_count)?;
            if terms
                .last()
                .is_some_and(|previous: &EmbeddedTermLocator| previous.fingerprint >= fingerprint)
            {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "embedded terms are not ordered",
                ));
            }
            terms.push(EmbeddedTermLocator {
                fingerprint,
                layout_ids,
            });
        }
        let (field_set_count, field_set_ids, field_position_flag) =
            decode_packed_column(encoded, &mut cursor, record_count, false)?;
        debug_assert!(!field_position_flag);
        let field_membership = term_membership.clone();
        let field_count = read_usize(encoded, &mut cursor)?;
        ensure_count_within(
            field_count,
            encoded.len().saturating_sub(cursor),
            "embedded field count",
        )?;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let fingerprint = decode_index_fingerprint(encoded, &mut cursor)?;
            let field_set_ids = decode_sorted_ids(encoded, &mut cursor, field_set_count)?;
            if fields
                .last()
                .is_some_and(|previous: &EmbeddedFieldLocator| previous.fingerprint >= fingerprint)
            {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "embedded fields are not ordered",
                ));
            }
            fields.push(EmbeddedFieldLocator {
                fingerprint,
                field_set_ids,
            });
        }
        require_consumed(encoded, cursor)?;
        Ok(Self {
            record_count,
            timestamp_offset_ordinal_ordered,
            layout_count,
            layout_ids,
            residual_layout_ids,
            term_membership,
            terms,
            field_set_count,
            field_set_ids,
            field_membership,
            fields,
        })
    }
}

pub(super) fn normalize_index_term(term: &str) -> Cow<'_, str> {
    if term.chars().any(char::is_uppercase) {
        Cow::Owned(term.to_lowercase())
    } else {
        Cow::Borrowed(term)
    }
}

pub(super) fn membership_hash(value: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in value {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

pub(super) fn membership_pair_hash(key: &[u8], value: &[u8]) -> u64 {
    let mut hash = membership_hash(key);
    hash = (hash ^ 0xff).wrapping_mul(0x0000_0100_0000_01b3);
    for byte in value {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

pub(super) fn index_fingerprint(hash: u64) -> u32 {
    hash as u32 & INDEX_FINGERPRINT_MASK
}

pub(super) fn encode_index_fingerprint(fingerprint: u32, encoded: &mut Vec<u8>) {
    debug_assert_eq!(fingerprint & !INDEX_FINGERPRINT_MASK, 0);
    encoded.extend_from_slice(&fingerprint.to_le_bytes()[..3]);
}

pub(super) fn encode_membership_filter(filter: &MembershipFilter, encoded: &mut Vec<u8>) {
    for word in filter.words.iter() {
        encoded.extend_from_slice(&word.to_le_bytes());
    }
}

pub(super) fn decode_membership_filter(
    encoded: &[u8],
    cursor: &mut usize,
) -> TelemetryResult<MembershipFilter> {
    let byte_count = EMBEDDED_MEMBERSHIP_FILTER_WORDS
        .checked_mul(size_of::<u64>())
        .ok_or(TelemetryError::RecordTooLarge)?;
    let end = cursor
        .checked_add(byte_count)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "membership filter length overflow",
        ))?;
    let bytes = encoded
        .get(*cursor..end)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "truncated membership filter",
        ))?;
    *cursor = end;
    let words = bytes
        .chunks_exact(size_of::<u64>())
        .map(|word| u64::from_le_bytes(word.try_into().expect("fixed word width")))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(MembershipFilter { words })
}

pub(super) fn bits_for_dictionary(dictionary_count: u32) -> u8 {
    if dictionary_count <= 1 {
        0
    } else {
        u8::try_from(u32::BITS - (dictionary_count - 1).leading_zeros())
            .expect("u32 dictionary width fits u8")
    }
}

pub(super) fn pack_ids(ids: &[u32], dictionary_count: u32) -> TelemetryResult<PackedIdColumn> {
    if (ids.is_empty() && dictionary_count != 0)
        || (!ids.is_empty() && (dictionary_count == 0 || dictionary_count as usize > ids.len()))
        || ids.iter().any(|id| *id >= dictionary_count)
    {
        return Err(TelemetryError::InvalidBlockEncoding(
            "invalid packed ID dictionary",
        ));
    }
    let bits_per_id = bits_for_dictionary(dictionary_count);
    if bits_per_id == 0 {
        return Ok(PackedIdColumn {
            bits_per_id,
            values: Vec::new(),
        });
    }
    let bit_count = ids
        .len()
        .checked_mul(usize::from(bits_per_id))
        .ok_or(TelemetryError::RecordTooLarge)?;
    let mut values = Vec::with_capacity(bit_count.div_ceil(u8::BITS as usize));
    let mut buffered = 0u64;
    let mut buffered_bits = 0u8;
    for id in ids {
        buffered |= u64::from(*id) << buffered_bits;
        buffered_bits += bits_per_id;
        while buffered_bits >= u8::BITS as u8 {
            values.push(buffered as u8);
            buffered >>= u8::BITS;
            buffered_bits -= u8::BITS as u8;
        }
    }
    if buffered_bits > 0 {
        values.push(buffered as u8);
    }
    Ok(PackedIdColumn {
        bits_per_id,
        values,
    })
}

pub(super) fn packed_id(column: &PackedIdColumn, ordinal: u32) -> u32 {
    if column.bits_per_id == 0 {
        return 0;
    }
    let bit = ordinal as usize * usize::from(column.bits_per_id);
    let byte = bit / u8::BITS as usize;
    let shift = bit % u8::BITS as usize;
    let mut window = 0u64;
    for index in 0..5 {
        if let Some(value) = column.values.get(byte + index) {
            window |= u64::from(*value) << (index * u8::BITS as usize);
        }
    }
    let mask = (1u64 << column.bits_per_id) - 1;
    u32::try_from((window >> shift) & mask).expect("packed ID is at most u32")
}

pub(super) fn matching_packed_ids(
    column: &PackedIdColumn,
    record_count: u32,
    dictionary_count: u32,
    selected: &[u32],
) -> TelemetryResult<Vec<u32>> {
    if selected.is_empty() || record_count == 0 {
        return Ok(Vec::new());
    }
    let mut selected_ids =
        vec![false; usize::try_from(dictionary_count).map_err(|_| TelemetryError::RecordTooLarge)?];
    for id in selected {
        let slot =
            selected_ids
                .get_mut(*id as usize)
                .ok_or(TelemetryError::InvalidBlockEncoding(
                    "embedded locator ID is out of range",
                ))?;
        *slot = true;
    }
    let mut ordinals = Vec::new();
    for ordinal in 0..record_count {
        if selected_ids[packed_id(column, ordinal) as usize] {
            ordinals.push(ordinal);
        }
    }
    Ok(ordinals)
}

pub(super) fn encode_packed_column(
    column: &PackedIdColumn,
    dictionary_count: u32,
    record_count: u32,
    position_ordered: bool,
    encoded: &mut Vec<u8>,
) -> TelemetryResult<()> {
    write_varint(u64::from(dictionary_count), encoded);
    let mut runs = Vec::<(u32, u32)>::new();
    for ordinal in 0..record_count {
        let id = packed_id(column, ordinal);
        if let Some((last_id, length)) = runs.last_mut()
            && *last_id == id
        {
            *length = length
                .checked_add(1)
                .ok_or(TelemetryError::RecordTooLarge)?;
        } else {
            runs.push((id, 1));
        }
    }

    let mut run_length = Vec::new();
    write_varint(
        u64::try_from(runs.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        &mut run_length,
    );
    for (id, length) in runs {
        write_varint(u64::from(id), &mut run_length);
        write_varint(u64::from(length), &mut run_length);
    }

    let value_bytes =
        u64::try_from(column.values.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
    let bitpacked_bytes = 1usize
        .checked_add(varint_length(value_bytes))
        .and_then(|length| length.checked_add(column.values.len()))
        .ok_or(TelemetryError::RecordTooLarge)?;
    let run_length_bytes = 1usize
        .checked_add(run_length.len())
        .ok_or(TelemetryError::RecordTooLarge)?;
    let position_flag = if position_ordered {
        PACKED_IDS_POSITION_ORDERED
    } else {
        0
    };
    if run_length_bytes < bitpacked_bytes {
        encoded.push(PACKED_IDS_RUN_LENGTH | position_flag);
        encoded.extend_from_slice(&run_length);
    } else {
        encoded.push(PACKED_IDS_BITPACKED | position_flag);
        append_bytes(encoded, &column.values)?;
    }
    Ok(())
}

pub(super) fn decode_packed_column(
    encoded: &[u8],
    cursor: &mut usize,
    record_count: u32,
    allow_position_flag: bool,
) -> TelemetryResult<(u32, PackedIdColumn, bool)> {
    let dictionary_count = read_u32(encoded, cursor)?;
    if (record_count == 0 && dictionary_count != 0)
        || (record_count != 0 && (dictionary_count == 0 || dictionary_count > record_count))
    {
        return Err(TelemetryError::InvalidBlockEncoding(
            "invalid embedded dictionary count",
        ));
    }
    let bits_per_id = bits_for_dictionary(dictionary_count);
    let expected_bytes = usize::try_from(record_count)
        .map_err(|_| TelemetryError::RecordTooLarge)?
        .checked_mul(usize::from(bits_per_id))
        .ok_or(TelemetryError::RecordTooLarge)?
        .div_ceil(u8::BITS as usize);
    let encoded_kind = read_byte(encoded, cursor)?;
    let position_ordered = encoded_kind & PACKED_IDS_POSITION_ORDERED != 0;
    if position_ordered && !allow_position_flag {
        return Err(TelemetryError::InvalidBlockEncoding(
            "packed ID column has an unexpected position-order flag",
        ));
    }
    let encoding = encoded_kind & !PACKED_IDS_POSITION_ORDERED;
    let values = match encoding {
        PACKED_IDS_BITPACKED => {
            let values = read_bytes(encoded, cursor)?.to_vec();
            if values.len() != expected_bytes {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "packed ID column length mismatch",
                ));
            }
            values
        }
        PACKED_IDS_RUN_LENGTH => {
            let run_count = read_usize(encoded, cursor)?;
            ensure_count_within(
                run_count,
                encoded.len().saturating_sub(*cursor),
                "packed ID run count",
            )?;
            let mut ids = Vec::with_capacity(
                usize::try_from(record_count).map_err(|_| TelemetryError::RecordTooLarge)?,
            );
            for _ in 0..run_count {
                let id = read_u32(encoded, cursor)?;
                let length = read_usize(encoded, cursor)?;
                if id >= dictionary_count || length == 0 {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "packed ID run is invalid",
                    ));
                }
                let new_length = ids
                    .len()
                    .checked_add(length)
                    .filter(|length| *length <= record_count as usize)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "packed ID runs exceed record count",
                    ))?;
                ids.resize(new_length, id);
            }
            if ids.len() != record_count as usize {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "packed ID runs do not cover record count",
                ));
            }
            pack_ids(&ids, dictionary_count)?.values
        }
        _ => {
            return Err(TelemetryError::InvalidBlockEncoding(
                "unknown packed ID column encoding",
            ));
        }
    };
    let column = PackedIdColumn {
        bits_per_id,
        values,
    };
    for ordinal in 0..record_count {
        if packed_id(&column, ordinal) >= dictionary_count {
            return Err(TelemetryError::InvalidBlockEncoding(
                "packed ID exceeds its dictionary",
            ));
        }
    }
    if let Some(last) = column.values.last()
        && expected_bytes > 0
    {
        let used_bits = usize::try_from(record_count)
            .expect("u32 record count fits usize")
            .saturating_mul(usize::from(bits_per_id))
            % u8::BITS as usize;
        if used_bits != 0 && *last >> used_bits != 0 {
            return Err(TelemetryError::InvalidBlockEncoding(
                "packed ID padding is nonzero",
            ));
        }
    }
    Ok((dictionary_count, column, position_ordered))
}

pub(super) fn encode_sorted_ids(
    ids: &[u32],
    upper_bound: u32,
    encoded: &mut Vec<u8>,
) -> TelemetryResult<()> {
    if ids.is_empty()
        || ids.windows(2).any(|adjacent| adjacent[0] >= adjacent[1])
        || ids.last().is_some_and(|id| *id >= upper_bound)
    {
        return Err(TelemetryError::InvalidBlockEncoding(
            "embedded locator IDs are invalid",
        ));
    }
    write_varint(
        u64::try_from(ids.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        encoded,
    );
    write_varint(u64::from(ids[0]), encoded);
    for adjacent in ids.windows(2) {
        write_varint(u64::from(adjacent[1] - adjacent[0]), encoded);
    }
    Ok(())
}

pub(super) fn encode_optional_sorted_ids(
    ids: &[u32],
    upper_bound: u32,
    encoded: &mut Vec<u8>,
) -> TelemetryResult<()> {
    if ids.is_empty() {
        write_varint(0, encoded);
        Ok(())
    } else {
        encode_sorted_ids(ids, upper_bound, encoded)
    }
}

pub(super) fn decode_optional_sorted_ids(
    encoded: &[u8],
    cursor: &mut usize,
    upper_bound: u32,
) -> TelemetryResult<Vec<u32>> {
    let count = read_usize(encoded, cursor)?;
    if count == 0 {
        return Ok(Vec::new());
    }
    decode_sorted_ids_with_count(encoded, cursor, upper_bound, count)
}

pub(super) fn decode_sorted_ids(
    encoded: &[u8],
    cursor: &mut usize,
    upper_bound: u32,
) -> TelemetryResult<Vec<u32>> {
    let count = read_usize(encoded, cursor)?;
    decode_sorted_ids_with_count(encoded, cursor, upper_bound, count)
}

pub(super) fn decode_sorted_ids_with_count(
    encoded: &[u8],
    cursor: &mut usize,
    upper_bound: u32,
    count: usize,
) -> TelemetryResult<Vec<u32>> {
    if count == 0 || count > encoded.len().saturating_sub(*cursor) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "invalid embedded locator count",
        ));
    }
    let mut ids = Vec::with_capacity(count);
    let first = read_u32(encoded, cursor)?;
    if first >= upper_bound {
        return Err(TelemetryError::InvalidBlockEncoding(
            "embedded locator ID is out of range",
        ));
    }
    ids.push(first);
    for _ in 1..count {
        let delta = read_u32(encoded, cursor)?;
        if delta == 0 {
            return Err(TelemetryError::InvalidBlockEncoding(
                "embedded locator IDs are not ordered",
            ));
        }
        let next = ids
            .last()
            .copied()
            .and_then(|previous| previous.checked_add(delta))
            .filter(|next| *next < upper_bound)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "embedded locator ID is out of range",
            ))?;
        ids.push(next);
    }
    Ok(ids)
}

pub(super) fn read_u32(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u32> {
    u32::try_from(read_varint(encoded, cursor)?)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("value does not fit u32"))
}

pub(super) fn decode_index_fingerprint(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u32> {
    let end = cursor
        .checked_add(3)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "fingerprint cursor overflow",
        ))?;
    let bytes = encoded
        .get(*cursor..end)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "fingerprint is truncated",
        ))?;
    *cursor = end;
    Ok(u32::from(bytes[0]) | u32::from(bytes[1]) << 8 | u32::from(bytes[2]) << 16)
}
