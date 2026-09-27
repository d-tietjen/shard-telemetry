use super::*;

pub(super) fn parse_canonical_docker_json<'a>(
    line: &'a [u8],
    message_cache: &mut DockerMessageCache<'a>,
    timestamp_cache: &mut DockerTimestampPrefixCache,
) -> Option<(Rc<str>, Cow<'a, str>, u64)> {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    if !line.starts_with(DOCKER_JSON_PREFIX) || !line.ends_with(b"\"}") {
        return None;
    }
    let timestamp_end = line.len().checked_sub(2)?;
    let earliest_t = timestamp_end.checked_sub(20)?;
    let latest_t = timestamp_end.checked_sub(10)?;
    let timestamp_t = earliest_t.checked_add(
        line.get(earliest_t..=latest_t)?
            .iter()
            .position(|byte| *byte == b'T')?,
    )?;
    let timestamp_start = timestamp_t.checked_sub(10)?;
    if line.get(timestamp_start.checked_sub(1)?) != Some(&b'"') {
        return None;
    }
    let timestamp = parse_ascii_docker_timestamp_cached(
        line.get(timestamp_start..timestamp_end)?,
        timestamp_cache,
    )?;
    let before_timestamp = line.get(..timestamp_start)?;
    let (message_end, stream) = if before_timestamp.ends_with(DOCKER_JSON_STDERR_SUFFIX) {
        (
            timestamp_start.checked_sub(DOCKER_JSON_STDERR_SUFFIX.len())?,
            "stderr",
        )
    } else if before_timestamp.ends_with(DOCKER_JSON_STDOUT_SUFFIX) {
        (
            timestamp_start.checked_sub(DOCKER_JSON_STDOUT_SUFFIX.len())?,
            "stdout",
        )
    } else {
        return None;
    };
    let raw_message = line.get(DOCKER_JSON_PREFIX.len()..message_end)?;
    let message = message_cache.decode(raw_message)?;
    Some((message, Cow::Borrowed(stream), timestamp))
}

pub(super) fn docker_message_cache_slot(message: &[u8]) -> usize {
    let length = message.len();
    let first = sampled_message_u64(message, 0);
    let middle = sampled_message_u64(message, length.saturating_sub(8) / 2);
    let last = sampled_message_u64(message, length.saturating_sub(8));
    let mut hash = (length as u64)
        .wrapping_mul(0x9e37_79b1_85eb_ca87)
        .rotate_left(17);
    hash ^= first.wrapping_mul(0xc2b2_ae3d_27d4_eb4f);
    hash ^= middle.wrapping_mul(0x1656_67b1_9e37_79f9);
    hash ^= last.wrapping_mul(0x85eb_ca77_c2b2_ae63);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash as usize & (DOCKER_MESSAGE_CACHE_ENTRIES - 1)
}

pub(super) fn sampled_message_u64(message: &[u8], start: usize) -> u64 {
    if let Some(sample) = message.get(start..start.saturating_add(8))
        && let Ok(sample) = <[u8; 8]>::try_from(sample)
    {
        return u64::from_le_bytes(sample);
    }
    let end = start.saturating_add(8).min(message.len());
    let mut bytes = [0; 8];
    bytes[..end.saturating_sub(start)].copy_from_slice(&message[start..end]);
    u64::from_le_bytes(bytes)
}

pub(super) fn decode_common_json_message(raw: &[u8]) -> Option<Rc<str>> {
    if !raw.contains(&b'\\') {
        return std::str::from_utf8(raw).ok().map(Rc::from);
    }
    let mut decoded = Vec::with_capacity(raw.len());
    let mut cursor = 0usize;
    while let Some(&byte) = raw.get(cursor) {
        match byte {
            b'\\' => {
                cursor = cursor.checked_add(1)?;
                let escaped = *raw.get(cursor)?;
                if escaped == b'u' {
                    let (character, next_cursor) = decode_json_unicode_escape(raw, cursor)?;
                    let mut utf8 = [0; 4];
                    decoded.extend_from_slice(character.encode_utf8(&mut utf8).as_bytes());
                    cursor = next_cursor;
                    continue;
                }
                decoded.push(match escaped {
                    b'"' => b'"',
                    b'\\' => b'\\',
                    b'/' => b'/',
                    b'b' => 0x08,
                    b'f' => 0x0c,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    _ => return None,
                });
            }
            b'"' | 0x00..=0x1f => return None,
            _ => decoded.push(byte),
        }
        cursor = cursor.checked_add(1)?;
    }
    String::from_utf8(decoded).ok().map(Rc::from)
}

pub(super) fn decode_json_unicode_escape(
    raw: &[u8],
    unicode_marker: usize,
) -> Option<(char, usize)> {
    let first = parse_json_hex_quad(raw, unicode_marker.checked_add(1)?)?;
    let mut next_cursor = unicode_marker.checked_add(5)?;
    let scalar = if (0xd800..=0xdbff).contains(&first) {
        if raw.get(next_cursor..next_cursor.checked_add(2)?)? != b"\\u" {
            return None;
        }
        let second = parse_json_hex_quad(raw, next_cursor.checked_add(2)?)?;
        if !(0xdc00..=0xdfff).contains(&second) {
            return None;
        }
        next_cursor = next_cursor.checked_add(6)?;
        0x1_0000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
    } else if (0xdc00..=0xdfff).contains(&first) {
        return None;
    } else {
        u32::from(first)
    };
    char::from_u32(scalar).map(|character| (character, next_cursor))
}

pub(super) fn parse_json_hex_quad(raw: &[u8], start: usize) -> Option<u16> {
    let mut value = 0u16;
    for byte in raw.get(start..start.checked_add(4)?)? {
        value = value.checked_mul(16)?.checked_add(u16::from(match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        }))?;
    }
    Some(value)
}

pub(super) fn parse_docker_timestamp(input: &str) -> Result<u64, Box<dyn Error>> {
    let bytes = input.as_bytes();
    if let Some(timestamp) = parse_ascii_docker_timestamp(bytes) {
        return Ok(timestamp);
    }
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return Err(format!("unsupported Docker timestamp: {input}").into());
    }
    let year = i64::try_from(parse_digits(&bytes[0..4])?)?;
    let month = u32::try_from(parse_digits(&bytes[5..7])?)?;
    let day = u32::try_from(parse_digits(&bytes[8..10])?)?;
    let hour = parse_digits(&bytes[11..13])?;
    let minute = parse_digits(&bytes[14..16])?;
    let second = parse_digits(&bytes[17..19])?;
    if !(1..=12).contains(&month) || day == 0 || hour >= 24 || minute >= 60 || second >= 60 {
        return Err(format!("invalid Docker timestamp: {input}").into());
    }
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => unreachable!("month range was checked"),
    };
    if day > days_in_month {
        return Err(format!("invalid Docker timestamp: {input}").into());
    }
    let fraction = match &bytes[19..] {
        [b'Z'] => 0,
        [b'.', digits @ .., b'Z'] if !digits.is_empty() && digits.len() <= 9 => {
            let value = parse_digits(digits)?;
            value
                .checked_mul(10u64.pow(u32::try_from(9 - digits.len())?))
                .ok_or("timestamp fraction overflows")?
        }
        _ => return Err(format!("unsupported Docker timestamp timezone: {input}").into()),
    };
    let days = days_from_civil(year, month, day);
    if days < 0 {
        return Err(format!("timestamp predates Unix epoch: {input}").into());
    }
    let seconds = u64::try_from(days)?
        .checked_mul(86_400)
        .and_then(|value| value.checked_add(hour * 3_600 + minute * 60 + second))
        .ok_or("timestamp seconds overflow")?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(fraction))
        .ok_or_else(|| "timestamp nanoseconds overflow".into())
}

pub(super) fn parse_ascii_docker_timestamp(bytes: &[u8]) -> Option<u64> {
    parse_ascii_docker_timestamp_cached(bytes, &mut DockerTimestampPrefixCache::default())
}

pub(super) fn parse_ascii_docker_timestamp_cached(
    bytes: &[u8],
    cache: &mut DockerTimestampPrefixCache,
) -> Option<u64> {
    if !(20..=30).contains(&bytes.len()) {
        return None;
    }
    let fraction = match bytes.get(19..) {
        Some([b'Z']) => 0,
        Some([b'.', digits @ .., b'Z']) if !digits.is_empty() && digits.len() <= 9 => {
            ascii_fraction_nanos(digits)?
        }
        _ => return None,
    };
    let prefix = bytes.get(..19)?;
    let base_nanos = if cache.initialized && cache.prefix.as_slice() == prefix {
        cache.base_nanos
    } else {
        let base_nanos = parse_ascii_docker_timestamp_prefix(prefix)?;
        cache.prefix.copy_from_slice(prefix);
        cache.base_nanos = base_nanos;
        cache.initialized = true;
        base_nanos
    };
    base_nanos.checked_add(fraction)
}

pub(super) fn parse_ascii_docker_timestamp_prefix(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year = i64::try_from(four_ascii_digits(bytes, 0)?).ok()?;
    let month = u32::try_from(two_ascii_digits(bytes, 5)?).ok()?;
    let day = u32::try_from(two_ascii_digits(bytes, 8)?).ok()?;
    let hour = two_ascii_digits(bytes, 11)?;
    let minute = two_ascii_digits(bytes, 14)?;
    let second = two_ascii_digits(bytes, 17)?;
    if !(1..=12).contains(&month) || day == 0 || hour >= 24 || minute >= 60 || second >= 60 {
        return None;
    }
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return None,
    };
    if day > days_in_month {
        return None;
    }
    let days = u64::try_from(days_from_civil(year, month, day)).ok()?;
    days.checked_mul(86_400)?
        .checked_add(hour * 3_600 + minute * 60 + second)?
        .checked_mul(1_000_000_000)
}

pub(super) fn two_ascii_digits(bytes: &[u8], start: usize) -> Option<u64> {
    let high = bytes.get(start)?.wrapping_sub(b'0');
    let low = bytes.get(start.checked_add(1)?)?.wrapping_sub(b'0');
    (high < 10 && low < 10).then_some(u64::from(high) * 10 + u64::from(low))
}

pub(super) fn four_ascii_digits(bytes: &[u8], start: usize) -> Option<u64> {
    let high = two_ascii_digits(bytes, start)?;
    let low = two_ascii_digits(bytes, start.checked_add(2)?)?;
    Some(high * 100 + low)
}

pub(super) fn ascii_fraction_nanos(bytes: &[u8]) -> Option<u64> {
    let digit = |index: usize| {
        let digit = bytes.get(index)?.wrapping_sub(b'0');
        (digit < 10).then_some(u64::from(digit))
    };
    match bytes.len() {
        1 => Some(digit(0)? * 100_000_000),
        2 => Some(two_ascii_digits(bytes, 0)? * 10_000_000),
        3 => Some((two_ascii_digits(bytes, 0)? * 10 + digit(2)?) * 1_000_000),
        4 => Some(four_ascii_digits(bytes, 0)? * 100_000),
        5 => Some((four_ascii_digits(bytes, 0)? * 10 + digit(4)?) * 10_000),
        6 => Some((four_ascii_digits(bytes, 0)? * 100 + two_ascii_digits(bytes, 4)?) * 1_000),
        7 => Some(
            (four_ascii_digits(bytes, 0)? * 1_000 + two_ascii_digits(bytes, 4)? * 10 + digit(6)?)
                * 100,
        ),
        8 => Some(eight_ascii_digits(bytes)? * 10),
        9 => Some(eight_ascii_digits(bytes)? * 10 + digit(8)?),
        _ => None,
    }
}

#[inline]
pub(super) fn eight_ascii_digits(bytes: &[u8]) -> Option<u64> {
    let ascii = u64::from_le_bytes(bytes.get(..8)?.try_into().ok()?);
    let lower = ascii.wrapping_sub(0x3030_3030_3030_3030);
    let upper = ascii.wrapping_add(0x4646_4646_4646_4646);
    if (lower | upper) & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    let digits = ascii & 0x0f0f_0f0f_0f0f_0f0f;
    let pairs = ((digits & 0x00ff_00ff_00ff_00ff) * 10) + ((digits >> 8) & 0x00ff_00ff_00ff_00ff);
    let quads = ((pairs & 0x0000_ffff_0000_ffff) * 100) + ((pairs >> 16) & 0x0000_ffff_0000_ffff);
    Some((quads & 0x0000_0000_ffff_ffff) * 10_000 + (quads >> 32))
}

pub(super) fn parse_digits(bytes: &[u8]) -> Result<u64, Box<dyn Error>> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return Err("timestamp contains a non-digit".into());
    }
    bytes.iter().try_fold(0u64, |value, byte| {
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(byte - b'0')))
            .ok_or_else(|| "timestamp number overflows".into())
    })
}

pub(super) fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

pub(super) fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

pub(super) fn merge_locality_stats(
    aggregate: &mut CompressionLocalityStats,
    worker: CompressionLocalityStats,
) {
    aggregate.observations = aggregate.observations.saturating_add(worker.observations);
    aggregate.blocks_scored = aggregate.blocks_scored.saturating_add(worker.blocks_scored);
    aggregate.blocks_split = aggregate.blocks_split.saturating_add(worker.blocks_split);
    aggregate.subblocks_created = aggregate
        .subblocks_created
        .saturating_add(worker.subblocks_created);
    aggregate.split_explorations_suppressed = aggregate
        .split_explorations_suppressed
        .saturating_add(worker.split_explorations_suppressed);
    aggregate.base_placements = aggregate
        .base_placements
        .saturating_add(worker.base_placements);
    aggregate.collated_placements = aggregate
        .collated_placements
        .saturating_add(worker.collated_placements);
    aggregate.records_reassigned = aggregate
        .records_reassigned
        .saturating_add(worker.records_reassigned);
    aggregate.bytes_reassigned = aggregate
        .bytes_reassigned
        .saturating_add(worker.bytes_reassigned);
    aggregate.active_compression_shards = aggregate
        .active_compression_shards
        .saturating_add(worker.active_compression_shards);
    aggregate.max_internal_variance_q8 = aggregate
        .max_internal_variance_q8
        .max(worker.max_internal_variance_q8);
    aggregate.handoff_membership_bytes = aggregate
        .handoff_membership_bytes
        .saturating_add(worker.handoff_membership_bytes);
    aggregate.allocated_state_bytes = aggregate
        .allocated_state_bytes
        .saturating_add(worker.allocated_state_bytes);
}
