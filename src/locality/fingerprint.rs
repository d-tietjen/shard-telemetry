use super::*;

/// Scans a message once, calling `on_term` for every Unicode alphanumeric term.
///
/// Dynamic structural tokens contribute only type and logarithmic length
/// classes. Metadata values never enter either fingerprint.
pub fn analyze_message(
    message: &str,
    fields: &[MetadataField],
    mut on_term: impl FnMut(&str),
) -> MessageFingerprint {
    let (mut shape_hash, mut simhash) = if message.is_ascii() {
        analyze_ascii_body(message, &mut on_term)
    } else {
        analyze_unicode_body(message, &mut on_term)
    };

    for field in fields {
        shape_hash = fnv_bytes(shape_hash, &[0xf1]);
        shape_hash = fnv_bytes(shape_hash, field.key.as_bytes());
        add_simhash_feature(
            &mut simhash,
            fnv_bytes(FNV_OFFSET ^ 0x6d65_7461_6b65_7900, field.key.as_bytes()),
        );
    }

    let mut locality_signature = 0u16;
    for (bit, weight) in simhash.into_iter().enumerate() {
        if weight > 0 {
            locality_signature |= 1u16 << bit;
        }
    }
    MessageFingerprint {
        shape_hash,
        locality_signature,
    }
}

/// Scans only case-preserving search terms without calculating compression
/// fingerprints.
///
/// The default production locality policy is disabled. Keeping this path
/// separate avoids template hashing and 16-lane SimHash work when ingestion
/// only needs the inverted search index.
pub fn scan_message_terms(message: &str, mut on_term: impl FnMut(&str)) {
    if message.is_ascii() {
        let mut start = None;
        for (index, byte) in message.bytes().enumerate() {
            match (start, byte.is_ascii_alphanumeric()) {
                (None, true) => start = Some(index),
                (Some(term_start), false) => {
                    on_term(&message[term_start..index]);
                    start = None;
                }
                _ => {}
            }
        }
        if let Some(term_start) = start {
            on_term(&message[term_start..]);
        }
        return;
    }

    let mut start = None;
    for (index, character) in message.char_indices() {
        match (start, character.is_alphanumeric()) {
            (None, true) => start = Some(index),
            (Some(term_start), false) => {
                on_term(&message[term_start..index]);
                start = None;
            }
            _ => {}
        }
    }
    if let Some(term_start) = start {
        on_term(&message[term_start..]);
    }
}

pub(super) fn analyze_ascii_body(
    message: &str,
    on_term: &mut impl FnMut(&str),
) -> (u64, [i16; 16]) {
    let bytes = message.as_bytes();
    let mut shape_hash = FNV_OFFSET;
    let mut simhash = [0i16; 16];
    let mut structural_start = 0usize;
    let mut structural_is_token = None;
    let mut structural_has_digit = false;
    let mut term_start = None;

    for (index, byte) in bytes.iter().copied().enumerate() {
        let is_term = byte.is_ascii_alphanumeric();
        match (term_start, is_term) {
            (None, true) => term_start = Some(index),
            (Some(start), false) => {
                on_term(&message[start..index]);
                term_start = None;
            }
            _ => {}
        }

        let is_token = is_template_token_byte(byte);
        match structural_is_token {
            None => {
                structural_start = index;
                structural_is_token = Some(is_token);
                structural_has_digit = byte.is_ascii_digit();
            }
            Some(current) if current == is_token => {
                structural_has_digit |= byte.is_ascii_digit();
            }
            Some(current) => {
                analyze_structural_run(
                    &message[structural_start..index],
                    current,
                    structural_has_digit,
                    &mut shape_hash,
                    &mut simhash,
                );
                structural_start = index;
                structural_is_token = Some(is_token);
                structural_has_digit = byte.is_ascii_digit();
            }
        }
    }
    if let Some(start) = term_start {
        on_term(&message[start..]);
    }
    if let Some(is_token) = structural_is_token {
        analyze_structural_run(
            &message[structural_start..],
            is_token,
            structural_has_digit,
            &mut shape_hash,
            &mut simhash,
        );
    }
    (shape_hash, simhash)
}

pub(super) fn analyze_unicode_body(
    message: &str,
    on_term: &mut impl FnMut(&str),
) -> (u64, [i16; 16]) {
    let mut shape_hash = FNV_OFFSET;
    let mut simhash = [0i16; 16];
    let mut structural_start = 0usize;
    let mut structural_is_token = None;
    let mut structural_has_digit = false;
    let mut term_start = None;

    for (index, character) in message.char_indices() {
        let is_term = character.is_alphanumeric();
        match (term_start, is_term) {
            (None, true) => term_start = Some(index),
            (Some(start), false) => {
                on_term(&message[start..index]);
                term_start = None;
            }
            _ => {}
        }

        let is_token = is_template_token_character(character);
        match structural_is_token {
            None => {
                structural_start = index;
                structural_is_token = Some(is_token);
                structural_has_digit = character.is_ascii_digit();
            }
            Some(current) if current == is_token => {
                structural_has_digit |= character.is_ascii_digit();
            }
            Some(current) => {
                analyze_structural_run(
                    &message[structural_start..index],
                    current,
                    structural_has_digit,
                    &mut shape_hash,
                    &mut simhash,
                );
                structural_start = index;
                structural_is_token = Some(is_token);
                structural_has_digit = character.is_ascii_digit();
            }
        }
    }
    if let Some(start) = term_start {
        on_term(&message[start..]);
    }
    if let Some(is_token) = structural_is_token {
        analyze_structural_run(
            &message[structural_start..],
            is_token,
            structural_has_digit,
            &mut shape_hash,
            &mut simhash,
        );
    }
    (shape_hash, simhash)
}

/// Returns a fingerprint without observing search terms.
#[must_use]
pub fn fingerprint_message(message: &str, fields: &[MetadataField]) -> MessageFingerprint {
    analyze_message(message, fields, |_| {})
}

pub(super) fn analyze_structural_run(
    run: &str,
    is_token: bool,
    has_digit: bool,
    shape_hash: &mut u64,
    simhash: &mut [i16; 16],
) {
    if is_token && has_digit {
        *shape_hash = fnv_bytes(*shape_hash, &[0xd1]);
        let class = dynamic_class(run);
        let length_class = length_class(run.len());
        add_simhash_feature(
            simhash,
            splitmix64(0x6479_6e61_6d69_6300 ^ (u64::from(class) << 8) ^ u64::from(length_class)),
        );
    } else {
        *shape_hash = fnv_bytes(*shape_hash, &[0x51]);
        *shape_hash = fnv_bytes(*shape_hash, run.as_bytes());
        add_simhash_feature(
            simhash,
            fnv_bytes(FNV_OFFSET ^ 0x6c69_7465_7261_6c00, run.as_bytes()),
        );
    }
}

pub(super) fn is_template_token_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.' | '/' | ':')
}

pub(super) fn is_template_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b':')
}

pub(super) fn dynamic_class(run: &str) -> u8 {
    let bytes = run.as_bytes();
    if bytes.iter().all(u8::is_ascii_digit) {
        1
    } else if bytes.iter().all(u8::is_ascii_hexdigit) {
        2
    } else if bytes.contains(&b'-') {
        3
    } else if bytes.contains(&b'.') || bytes.contains(&b':') || bytes.contains(&b'/') {
        4
    } else {
        5
    }
}

pub(super) fn length_class(length: usize) -> u8 {
    let length = u64::try_from(length).unwrap_or(u64::MAX).max(1);
    u8::try_from(length.ilog2()).unwrap_or(u8::MAX).min(15)
}

pub(super) fn add_simhash_feature(weights: &mut [i16; 16], feature_hash: u64) {
    let hash = splitmix64(feature_hash);
    for (bit, weight) in weights.iter_mut().enumerate() {
        if hash & (1u64 << bit) == 0 {
            *weight = weight.saturating_sub(1);
        } else {
            *weight = weight.saturating_add(1);
        }
    }
}

pub(super) fn fnv_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub(super) const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
