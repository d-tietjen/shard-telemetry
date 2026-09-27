use super::*;

impl BlockQueryIndex {
    /// Builds exact record-ordinal postings from normalized structural records.
    pub fn build<R: StructuralRecordView>(records: &[R]) -> TelemetryResult<Self> {
        let record_count =
            u32::try_from(records.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
        let mut term_ids = HashMap::<Arc<str>, usize>::new();
        let mut term_entries = Vec::<(Arc<str>, Vec<u32>)>::new();
        let mut message_cache = std::iter::repeat_with(|| None)
            .take(MESSAGE_TERM_CACHE_ENTRIES)
            .collect::<Vec<Option<CachedMessageTerms<'_>>>>();
        let mut term_cache = vec![None::<CachedTerm>; TERM_CACHE_ENTRIES];
        let mut message_trigrams = MessageTrigramFilter::new();
        let mut case_sensitive_message_trigrams = MessageTrigramFilter::new_case_sensitive();
        let mut field_postings = HashMap::<Arc<str>, HashMap<Arc<str>, Vec<u32>>>::new();
        for (record_ordinal, record) in records.iter().enumerate() {
            let record_ordinal =
                u32::try_from(record_ordinal).map_err(|_| TelemetryError::RecordTooLarge)?;
            let message = record.structural_message();
            let cache_slot = message_term_cache_slot(message.as_bytes());
            if let Some(cached) = &message_cache[cache_slot]
                && same_message(cached.message, message)
            {
                for &term_id in &cached.term_ids {
                    let postings = &mut term_entries[term_id].1;
                    if postings.last().copied() != Some(record_ordinal) {
                        postings.push(record_ordinal);
                    }
                }
            } else {
                message_trigrams.insert_message(message);
                case_sensitive_message_trigrams.insert_case_sensitive_message(message);
                let mut message_term_ids = message_cache[cache_slot]
                    .take()
                    .map(|cached| cached.term_ids)
                    .unwrap_or_default();
                message_term_ids.clear();
                let _ = analyze_message(message, &[], |term| {
                    let cache_slot = term_cache_slot(term.as_bytes());
                    let term_id = if let Some(cached) = term_cache[cache_slot]
                        && term_matches_cached(&term_entries[cached.term_id].0, term)
                    {
                        cached.term_id
                    } else {
                        let normalized = normalize_term(term);
                        let term_id = match term_ids.get(normalized.as_ref()).copied() {
                            Some(term_id) => term_id,
                            None => {
                                let term = Arc::<str>::from(normalized.as_ref());
                                let term_id = term_entries.len();
                                term_ids.insert(Arc::clone(&term), term_id);
                                term_entries.push((term, Vec::new()));
                                term_id
                            }
                        };
                        term_cache[cache_slot] = Some(CachedTerm { term_id });
                        term_id
                    };
                    message_term_ids.push(term_id);
                });
                for &term_id in &message_term_ids {
                    let postings = &mut term_entries[term_id].1;
                    if postings.last().copied() != Some(record_ordinal) {
                        postings.push(record_ordinal);
                    }
                }
                message_cache[cache_slot] = Some(CachedMessageTerms {
                    message,
                    term_ids: message_term_ids,
                });
            }

            for field_index in 0..record.structural_field_count() {
                let (key, value) = record.structural_field(field_index).ok_or(
                    TelemetryError::InvalidBlockEncoding(
                        "record field count changed while indexing",
                    ),
                )?;
                let values = match field_postings.get_mut(key) {
                    Some(values) => values,
                    None => {
                        field_postings.insert(Arc::from(key), HashMap::new());
                        field_postings.get_mut(key).expect("field key was inserted")
                    }
                };
                let postings = match values.get_mut(value) {
                    Some(postings) => postings,
                    None => {
                        values.insert(Arc::from(value), Vec::new());
                        values.get_mut(value).expect("field value was inserted")
                    }
                };
                if postings.last().copied() != Some(record_ordinal) {
                    postings.push(record_ordinal);
                }
            }
        }
        let term_postings = term_entries
            .into_iter()
            .map(|(term, posting)| Ok((term, PostingList::from_ordinals(posting)?)))
            .collect::<TelemetryResult<HashMap<_, _>>>()?;
        let field_postings = field_postings
            .into_iter()
            .map(|(key, values)| {
                let values = values
                    .into_iter()
                    .map(|(value, posting)| Ok((value, PostingList::from_ordinals(posting)?)))
                    .collect::<TelemetryResult<HashMap<_, _>>>()?;
                Ok((key, values))
            })
            .collect::<TelemetryResult<HashMap<_, _>>>()?;
        Ok(Self {
            record_count,
            message_trigrams,
            case_sensitive_message_trigrams,
            term_postings,
            field_postings,
        })
    }

    /// Number of indexed records.
    #[must_use]
    pub const fn record_count(&self) -> u32 {
        self.record_count
    }
}

#[inline]
fn same_message(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && (std::ptr::eq(left.as_ptr(), right.as_ptr()) || left.as_bytes() == right.as_bytes())
}

fn message_term_cache_slot(message: &[u8]) -> usize {
    let mut hash = message.len() as u64 ^ 0x9e37_79b9_7f4a_7c15;
    if message.len() >= 16 {
        let first = u64::from_le_bytes(
            message[..8]
                .try_into()
                .expect("eight-byte prefix is present"),
        );
        let last = u64::from_le_bytes(
            message[message.len() - 8..]
                .try_into()
                .expect("eight-byte suffix is present"),
        );
        hash ^= first.rotate_left(17) ^ last.rotate_left(41);
    } else {
        for byte in message {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    (hash as usize) & (MESSAGE_TERM_CACHE_ENTRIES - 1)
}

fn term_cache_slot(term: &[u8]) -> usize {
    let mut hash = term.len() as u64 ^ 0x517c_c1b7_2722_0a95;
    if let Some(first) = term.first() {
        hash ^= u64::from(*first) << 8;
    }
    if let Some(last) = term.last() {
        hash ^= u64::from(*last) << 24;
    }
    if term.len() >= 8 {
        hash ^= u64::from_le_bytes(term[..8].try_into().expect("eight-byte prefix is present"));
    } else {
        for byte in term {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash ^= hash >> 32;
    (hash as usize) & (TERM_CACHE_ENTRIES - 1)
}

fn term_matches_cached(indexed: &str, observed: &str) -> bool {
    if observed.is_ascii() {
        indexed.eq_ignore_ascii_case(observed)
    } else {
        normalize_term(observed).as_ref() == indexed
    }
}
