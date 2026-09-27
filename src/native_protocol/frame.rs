use super::*;

impl NativeFrameHeader {
    /// Creates a request header for `payload`.
    pub fn request(
        opcode: NativeOpcode,
        request_id: u128,
        payload: &[u8],
    ) -> Result<Self, NativeProtocolError> {
        Self::new(opcode, request_id, NativeStatus::Ok, false, payload)
    }

    /// Creates a response header for `payload`.
    pub fn response(
        opcode: NativeOpcode,
        request_id: u128,
        status: NativeStatus,
        payload: &[u8],
    ) -> Result<Self, NativeProtocolError> {
        Self::new(opcode, request_id, status, true, payload)
    }

    pub(super) fn new(
        opcode: NativeOpcode,
        request_id: u128,
        status: NativeStatus,
        is_response: bool,
        payload: &[u8],
    ) -> Result<Self, NativeProtocolError> {
        if payload.len() > MAX_NATIVE_FRAME_BYTES {
            return Err(NativeProtocolError::new(format!(
                "native frame payload is {} bytes, exceeding {MAX_NATIVE_FRAME_BYTES}",
                payload.len()
            )));
        }
        Ok(Self {
            opcode,
            request_id,
            status,
            is_response,
            payload_len: u32::try_from(payload.len())
                .map_err(|_| NativeProtocolError::new("native frame payload exceeds u32"))?,
            payload_checksum: payload_checksum(payload),
        })
    }

    /// Encodes this header into its fixed-width wire representation.
    #[must_use]
    pub fn encode(self) -> [u8; NATIVE_FRAME_HEADER_BYTES] {
        let mut bytes = [0; NATIVE_FRAME_HEADER_BYTES];
        bytes[0..4].copy_from_slice(&FRAME_MAGIC);
        bytes[4] = FRAME_VERSION;
        bytes[5] = self.opcode as u8;
        bytes[6] = u8::from(self.is_response) * FRAME_FLAG_RESPONSE;
        bytes[7] = self.status as u8;
        bytes[8..24].copy_from_slice(&self.request_id.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.payload_len.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.payload_checksum.to_le_bytes());
        bytes
    }

    /// Decodes and validates a fixed-width wire header.
    pub fn decode(bytes: &[u8; NATIVE_FRAME_HEADER_BYTES]) -> Result<Self, NativeProtocolError> {
        if bytes[0..4] != FRAME_MAGIC {
            return Err(NativeProtocolError::new("invalid native frame magic"));
        }
        if bytes[4] != FRAME_VERSION {
            return Err(NativeProtocolError::new(format!(
                "unsupported native frame version {}",
                bytes[4]
            )));
        }
        if bytes[6] & !FRAME_FLAG_RESPONSE != 0 {
            return Err(NativeProtocolError::new(
                "native frame contains unknown flags",
            ));
        }
        let payload_len = u32::from_le_bytes(bytes[24..28].try_into().expect("fixed range"));
        if payload_len as usize > MAX_NATIVE_FRAME_BYTES {
            return Err(NativeProtocolError::new(format!(
                "native frame payload is {payload_len} bytes, exceeding {MAX_NATIVE_FRAME_BYTES}"
            )));
        }
        Ok(Self {
            opcode: NativeOpcode::from_byte(bytes[5])?,
            request_id: u128::from_le_bytes(bytes[8..24].try_into().expect("fixed range")),
            status: NativeStatus::from_byte(bytes[7])?,
            is_response: bytes[6] == FRAME_FLAG_RESPONSE,
            payload_len,
            payload_checksum: u32::from_le_bytes(bytes[28..32].try_into().expect("fixed range")),
        })
    }

    /// Verifies that `payload` has the declared length and BLAKE3 checksum.
    pub fn verify_payload(self, payload: &[u8]) -> Result<(), NativeProtocolError> {
        self.verify_payload_and_hash(payload).map(|_| ())
    }

    /// Verifies `payload` and returns the full BLAKE3 hash computed for the
    /// frame checksum. Callers that need a payload identity can reuse this
    /// value instead of hashing the complete payload a second time.
    pub fn verify_payload_and_hash(
        self,
        payload: &[u8],
    ) -> Result<blake3::Hash, NativeProtocolError> {
        if payload.len() != self.payload_len as usize {
            return Err(NativeProtocolError::new(
                "native frame payload length disagrees with its header",
            ));
        }
        let hash = blake3::hash(payload);
        if u32::from_le_bytes(hash.as_bytes()[0..4].try_into().expect("fixed range"))
            != self.payload_checksum
        {
            return Err(NativeProtocolError::new(
                "native frame payload checksum mismatch",
            ));
        }
        Ok(hash)
    }
}

impl NativeFrame {
    /// Creates a request frame.
    pub fn request(
        opcode: NativeOpcode,
        request_id: u128,
        payload: Vec<u8>,
    ) -> Result<Self, NativeProtocolError> {
        Ok(Self {
            header: NativeFrameHeader::request(opcode, request_id, &payload)?,
            payload,
        })
    }

    /// Creates a response frame.
    pub fn response(
        opcode: NativeOpcode,
        request_id: u128,
        status: NativeStatus,
        payload: Vec<u8>,
    ) -> Result<Self, NativeProtocolError> {
        Ok(Self {
            header: NativeFrameHeader::response(opcode, request_id, status, &payload)?,
            payload,
        })
    }

    /// Encodes the complete frame.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(NATIVE_FRAME_HEADER_BYTES + self.payload.len());
        encoded.extend_from_slice(&self.header.encode());
        encoded.extend_from_slice(&self.payload);
        encoded
    }
}
