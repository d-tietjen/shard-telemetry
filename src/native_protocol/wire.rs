use super::*;

pub(super) fn validate_tenant(tenant: &str) -> Result<(), NativeProtocolError> {
    if tenant.is_empty() {
        return Err(NativeProtocolError::new("native tenant must not be empty"));
    }
    if tenant.len() > MAX_TENANT_BYTES || tenant.len() > usize::from(u16::MAX) {
        return Err(NativeProtocolError::new(
            "native tenant exceeds its length limit",
        ));
    }
    Ok(())
}

pub(super) fn validate_query(query: &NativeQuery) -> Result<(), NativeProtocolError> {
    validate_tenant(&query.tenant)?;
    if query.labels.len() > MAX_LABELS_PER_STREAM {
        return Err(NativeProtocolError::new(
            "native query contains too many labels",
        ));
    }
    if query.terms.len() > MAX_QUERY_TERMS {
        return Err(NativeProtocolError::new(
            "native query contains too many terms",
        ));
    }
    if query.limit == 0 || query.limit > MAX_QUERY_LIMIT {
        return Err(NativeProtocolError::new(format!(
            "native query limit must be in 1..={MAX_QUERY_LIMIT}"
        )));
    }
    if let (Some(start), Some(end)) = (
        query.start_timestamp_unix_nanos,
        query.end_timestamp_unix_nanos,
    ) && start >= end
    {
        return Err(NativeProtocolError::new(
            "native query timestamp range must be nonempty",
        ));
    }
    for (key, value) in &query.labels {
        if key.is_empty() {
            return Err(NativeProtocolError::new(
                "native query label keys must not be empty",
            ));
        }
        validate_string16(key, "query label key")?;
        validate_string16(value, "query label value")?;
    }
    for term in &query.terms {
        if term.is_empty() {
            return Err(NativeProtocolError::new(
                "native query terms must not be empty",
            ));
        }
        validate_string16(term, "query term")?;
    }
    Ok(())
}

pub(super) fn payload_checksum(payload: &[u8]) -> u32 {
    u32::from_le_bytes(
        blake3::hash(payload).as_bytes()[0..4]
            .try_into()
            .expect("fixed range"),
    )
}

pub(super) fn put_u16(
    encoded: &mut Vec<u8>,
    value: usize,
    field: &'static str,
) -> Result<(), NativeProtocolError> {
    let value = u16::try_from(value)
        .map_err(|_| NativeProtocolError::new(format!("{field} exceeds u16")))?;
    encoded.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

pub(super) fn put_u32(
    encoded: &mut Vec<u8>,
    value: usize,
    field: &'static str,
) -> Result<(), NativeProtocolError> {
    let value = u32::try_from(value)
        .map_err(|_| NativeProtocolError::new(format!("{field} exceeds u32")))?;
    encoded.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

pub(super) fn validate_string16(
    value: &str,
    field: &'static str,
) -> Result<(), NativeProtocolError> {
    u16::try_from(value.len())
        .map(|_| ())
        .map_err(|_| NativeProtocolError::new(format!("{field} exceeds u16")))
}

pub(super) fn put_string16(
    encoded: &mut Vec<u8>,
    value: &str,
    field: &'static str,
) -> Result<(), NativeProtocolError> {
    put_u16(encoded, value.len(), field)?;
    encoded.extend_from_slice(value.as_bytes());
    Ok(())
}

pub(super) struct Cursor<'a> {
    bytes: &'a [u8],
    pub(super) offset: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn at(bytes: &'a [u8], offset: usize) -> Self {
        Self { bytes, offset }
    }

    pub(super) fn bytes(
        &mut self,
        len: usize,
        field: &'static str,
    ) -> Result<&'a [u8], NativeProtocolError> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| NativeProtocolError::new(format!("native {field} is truncated")))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    pub(super) fn u16(&mut self, field: &'static str) -> Result<u16, NativeProtocolError> {
        Ok(u16::from_le_bytes(
            self.bytes(2, field)?.try_into().expect("fixed range"),
        ))
    }

    pub(super) fn u32(&mut self, field: &'static str) -> Result<u32, NativeProtocolError> {
        Ok(u32::from_le_bytes(
            self.bytes(4, field)?.try_into().expect("fixed range"),
        ))
    }

    pub(super) fn u64(&mut self, field: &'static str) -> Result<u64, NativeProtocolError> {
        Ok(u64::from_le_bytes(
            self.bytes(8, field)?.try_into().expect("fixed range"),
        ))
    }

    pub(super) fn u128(&mut self, field: &'static str) -> Result<u128, NativeProtocolError> {
        Ok(u128::from_le_bytes(
            self.bytes(16, field)?.try_into().expect("fixed range"),
        ))
    }

    pub(super) fn string(
        &mut self,
        len: usize,
        field: &'static str,
    ) -> Result<&'a str, NativeProtocolError> {
        std::str::from_utf8(self.bytes(len, field)?)
            .map_err(|_| NativeProtocolError::new(format!("native {field} is not UTF-8")))
    }

    pub(super) fn string16(&mut self, field: &'static str) -> Result<&'a str, NativeProtocolError> {
        let len = usize::from(self.u16(field)?);
        self.string(len, field)
    }

    pub(super) fn finish(self) -> Result<(), NativeProtocolError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(NativeProtocolError::new(
                "native payload contains trailing bytes",
            ))
        }
    }
}
