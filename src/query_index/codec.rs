use super::*;

pub(super) fn append_bytes(bytes: &[u8], encoded: &mut Vec<u8>) -> TelemetryResult<()> {
    write_varint(
        u64::try_from(bytes.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        encoded,
    );
    encoded.extend_from_slice(bytes);
    Ok(())
}

pub(super) fn write_varint(mut value: u64, encoded: &mut Vec<u8>) {
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
}

pub(super) fn read_varint(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = read_byte(encoded, cursor)?;
        let payload = u64::from(byte & 0x7f);
        if shift > 63 || (shift == 63 && payload > 1) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "query index varint overflow",
            ));
        }
        value |= payload << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift = shift.saturating_add(7);
        if shift > 63 {
            return Err(TelemetryError::InvalidBlockEncoding(
                "query index varint too long",
            ));
        }
    }
}

pub(super) fn read_byte(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u8> {
    let byte = *encoded
        .get(*cursor)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "truncated query index",
        ))?;
    *cursor = cursor
        .checked_add(1)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "query index cursor overflow",
        ))?;
    Ok(byte)
}

pub(super) fn read_u32(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u32> {
    u32::try_from(read_varint(encoded, cursor)?)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("query index value does not fit u32"))
}

pub(super) fn read_usize(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<usize> {
    usize::try_from(read_varint(encoded, cursor)?)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("query index value does not fit usize"))
}

pub(super) fn read_bytes<'a>(encoded: &'a [u8], cursor: &mut usize) -> TelemetryResult<&'a [u8]> {
    let length = read_usize(encoded, cursor)?;
    let end = cursor
        .checked_add(length)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "query index byte length overflow",
        ))?;
    let bytes = encoded
        .get(*cursor..end)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "truncated query index bytes",
        ))?;
    *cursor = end;
    Ok(bytes)
}

pub(super) fn decode_text(bytes: &[u8]) -> TelemetryResult<Arc<str>> {
    std::str::from_utf8(bytes)
        .map(Arc::<str>::from)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("query index text is not UTF-8"))
}

pub(super) fn ensure_count(count: usize, remaining: usize) -> TelemetryResult<()> {
    if count <= remaining {
        Ok(())
    } else {
        Err(TelemetryError::InvalidBlockEncoding(
            "query index count exceeds remaining bytes",
        ))
    }
}
