//! Paid, allocation-free preflight of the exact current Classic/u64 wire domain.
use super::{Failure, RecoveryFrameWork, Result, Timestamps, add, invalid};

#[derive(Clone, Copy, Debug)]
pub(super) struct Shape {
    pub n: usize,
    pub bins: usize,
    pub ans_log: usize,
    pub delta: usize,
    pub start: usize,
    pub end: usize,
}
impl Timestamps {
    fn bits(&mut self, input: &[u8], work: &mut RecoveryFrameWork, width: usize) -> Result<u64> {
        if width > 64 {
            return Err(Failure::State);
        }
        let mut value = 0;
        for index in 0..width {
            // This actual local loop reads one source byte per bit. Charge it
            // before inspection, including every malformed/truncated attempt.
            self.pay(work, 0, 1, 1)?;
            let bit = self.usage.preflight_bits;
            let byte = *input
                .get(bit / 8)
                .ok_or_else(|| invalid("truncated Pco preflight"))?;
            value |= u64::from((byte >> (bit % 8)) & 1) << index;
            self.usage.preflight_bits = add(bit, 1)?;
        }
        Ok(value)
    }
    fn small(&mut self, input: &[u8], work: &mut RecoveryFrameWork, width: usize) -> Result<usize> {
        usize::try_from(self.bits(input, work, width)?).map_err(|_| Failure::Limits)
    }
    fn padding(&mut self, input: &[u8], work: &mut RecoveryFrameWork) -> Result<()> {
        let bits = (8 - self.usage.preflight_bits % 8) % 8;
        if self.bits(input, work, bits)? != 0 {
            return Err(invalid("nonzero Pco metadata padding"));
        }
        Ok(())
    }
    pub(super) fn preflight_file(
        &mut self,
        input: &[u8],
        work: &mut RecoveryFrameWork,
    ) -> Result<usize> {
        self.usage.preflight_bits = 0;
        for expected in *b"pco!" {
            if self.bits(input, work, 8)? != u64::from(expected) {
                return Err(invalid("Pco magic"));
            }
        }
        if self.bits(input, work, 8)? != 3 {
            return Err(Failure::Unsupported);
        }
        if !matches!(self.bits(input, work, 8)?, 0 | 2) {
            return Err(Failure::Unsupported);
        }
        // Hint is parsed to frame the actual file, never used for allocation.
        let power = add(self.small(input, work, 6)?, 1)?;
        let _hint = self.bits(input, work, power)?;
        self.padding(input, work)?;
        if self.bits(input, work, 8)? != 4 || self.bits(input, work, 8)? != 1 {
            return Err(Failure::Unsupported);
        }
        Ok(self.usage.preflight_bits / 8)
    }
    pub(super) fn preflight_chunk(
        &mut self,
        input: &[u8],
        work: &mut RecoveryFrameWork,
        start: usize,
    ) -> Result<Option<Shape>> {
        self.usage.preflight_bits = super::cost::mul(start, 8)?;
        let tag = self.bits(input, work, 8)?;
        if tag == 0 {
            return Ok(None);
        }
        if tag != 2 {
            return Err(Failure::Unsupported);
        } // actual U64 number byte.
        let n = add(self.small(input, work, 24)?, 1)?;
        if n > self
            .expected
            .checked_sub(self.usage.confirmed_records)
            .ok_or(Failure::State)?
        {
            return Err(invalid("Pco chunk exceeds authoritative structural count"));
        }
        if self.usage.chunks >= self.limits.max_chunks {
            return Err(Failure::Limits);
        }
        if self.bits(input, work, 4)? != 0 {
            return Err(Failure::Unsupported);
        } // Classic.
        let delta = match self.bits(input, work, 4)? {
            0 => 0,
            1 => {
                if self.bits(input, work, 3)? != 1 || self.bits(input, work, 1)? != 0 {
                    return Err(Failure::Unsupported);
                }
                1
            }
            _ => return Err(Failure::Unsupported),
        };
        let ans_log = self.small(input, work, 4)?;
        let bins = self.small(input, work, 15)?;
        let states = 1usize
            .checked_shl(u32::try_from(ans_log).map_err(|_| Failure::Limits)?)
            .ok_or(Failure::Limits)?;
        if ans_log > 14 || bins > states || (bins == 1 && ans_log != 0) {
            return Err(invalid("invalid Pco ANS geometry"));
        }
        if bins > self.limits.max_bins || ans_log > self.limits.max_ans_log {
            return Err(Failure::Limits);
        }
        let mut weights = 0usize;
        for _ in 0..bins {
            weights = add(weights, add(self.small(input, work, ans_log)?, 1)?)?;
            let _lower = self.bits(input, work, 64)?;
            if self.bits(input, work, 7)? > 64 {
                return Err(invalid("Pco offset bits exceed u64"));
            }
        }
        if (bins == 0 && (ans_log != 0 || n > delta)) || (bins != 0 && weights != states) {
            return Err(invalid("invalid Pco ANS weight sum"));
        }
        self.padding(input, work)?;
        // Current Classic has one primary lane: d uncompressed u64 moments
        // and four s-bit ANS terminal indexes. No LZ/Conv/secondary allocation.
        for _ in 0..delta {
            let _moment = self.bits(input, work, 64)?;
        }
        for _ in 0..4 {
            let _index = self.bits(input, work, ans_log)?;
        }
        self.padding(input, work)?;
        Ok(Some(Shape {
            n,
            bins,
            ans_log,
            delta,
            start,
            end: self.usage.preflight_bits / 8,
        }))
    }
}
