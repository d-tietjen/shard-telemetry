use super::*;

pub(super) fn append_bytes(encoded: &mut Vec<u8>, value: &[u8]) -> TelemetryResult<()> {
    write_varint(
        u64::try_from(value.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        encoded,
    );
    encoded.extend_from_slice(value);
    Ok(())
}

pub(super) fn structural_sections(encoded: &[u8]) -> TelemetryResult<(usize, &[u8])> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    for _ in 0..7 {
        let _ = read_section(encoded, &mut cursor)?;
    }
    let embedded_index = read_section(encoded, &mut cursor)?;
    require_consumed(encoded, cursor)?;
    Ok((record_count, embedded_index))
}

#[inline]
pub(super) fn write_varint(value: u64, encoded: &mut Vec<u8>) {
    if value < 0x80 {
        encoded.push(value as u8);
    } else {
        write_multibyte_varint(value, encoded);
    }
}

pub(super) fn varint_length(mut value: u64) -> usize {
    let mut length = 1usize;
    while value >= 0x80 {
        value >>= 7;
        length += 1;
    }
    length
}

#[inline(never)]
pub(super) fn write_multibyte_varint(mut value: u64, encoded: &mut Vec<u8>) {
    debug_assert!(value >= 0x80);
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
}

pub(super) fn read_section<'a>(encoded: &'a [u8], cursor: &mut usize) -> TelemetryResult<&'a [u8]> {
    read_bytes(encoded, cursor)
}

pub(super) fn read_bytes<'a>(encoded: &'a [u8], cursor: &mut usize) -> TelemetryResult<&'a [u8]> {
    let length = read_usize(encoded, cursor)?;
    let end = cursor
        .checked_add(length)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "section length overflow",
        ))?;
    let value = encoded
        .get(*cursor..end)
        .ok_or(TelemetryError::InvalidBlockEncoding("truncated section"))?;
    *cursor = end;
    Ok(value)
}

pub(super) fn read_usize(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<usize> {
    usize::try_from(read_varint(encoded, cursor)?)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("length does not fit usize"))
}

pub(super) fn ensure_count_within(
    count: usize,
    remaining: usize,
    label: &'static str,
) -> TelemetryResult<()> {
    if count <= remaining {
        Ok(())
    } else {
        Err(TelemetryError::InvalidBlockEncoding(label))
    }
}

pub(super) fn validate_selected_ordinals(
    selected: &[u32],
    record_count: usize,
) -> TelemetryResult<()> {
    let mut previous = None;
    for ordinal in selected.iter().copied() {
        let ordinal = usize::try_from(ordinal).map_err(|_| {
            TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
        })?;
        if ordinal >= record_count || previous.is_some_and(|previous| previous >= ordinal) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "selected record ordinals are not strictly increasing",
            ));
        }
        previous = Some(ordinal);
    }
    Ok(())
}

pub(super) fn read_varint(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = read_byte(encoded, cursor)?;
        let payload = u64::from(byte & 0x7f);
        if shift > 63 || (shift == 63 && payload > 1) {
            return Err(TelemetryError::InvalidBlockEncoding("varint overflow"));
        }
        value |= payload << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift = shift.saturating_add(7);
        if shift > 63 {
            return Err(TelemetryError::InvalidBlockEncoding("varint is too long"));
        }
    }
}

pub(super) fn read_byte(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u8> {
    let byte = *encoded
        .get(*cursor)
        .ok_or(TelemetryError::InvalidBlockEncoding("truncated block"))?;
    *cursor = cursor
        .checked_add(1)
        .ok_or(TelemetryError::InvalidBlockEncoding("cursor overflow"))?;
    Ok(byte)
}

pub(super) fn decode_text(bytes: Vec<u8>) -> TelemetryResult<Arc<str>> {
    String::from_utf8(bytes)
        .map(Arc::<str>::from)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid UTF-8 text"))
}

pub(super) fn require_consumed(encoded: &[u8], cursor: usize) -> TelemetryResult<()> {
    if cursor == encoded.len() {
        Ok(())
    } else {
        Err(TelemetryError::InvalidBlockEncoding(
            "trailing component bytes",
        ))
    }
}

pub(super) fn validate_u32_length(length: usize) -> TelemetryResult<()> {
    u32::try_from(length)
        .map(|_| ())
        .map_err(|_| TelemetryError::RecordTooLarge)
}
