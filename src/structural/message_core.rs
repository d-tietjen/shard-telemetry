//! Shared template/body wire traversal; callers retain their own real output.
use super::*;
use std::mem::{size_of, size_of_val};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoreFailure {
    Native,
    Refused,
}
pub(crate) type CoreResult<T> = Result<T, CoreFailure>;
#[derive(Debug)]
pub(crate) enum HelperResult {
    Scalar(TelemetryResult<usize>),
    Bytes(TelemetryResult<Range<usize>>),
    Unit(TelemetryResult<()>),
    Template(TelemetryResult<Option<usize>>),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WireSource {
    Templates,
    Directory,
    Bodies,
}
#[derive(Default, Debug)]
pub(crate) struct WireState {
    #[cfg(unix)]
    pub(crate) source: Option<WireSource>,
    #[cfg(unix)]
    pub(crate) cursor: usize,
    pub(crate) actual: Option<HelperResult>,
}
pub(crate) trait WireHooks {
    fn admit(&mut self, bytes: usize, entries: usize, work: usize) -> CoreResult<()>;
    fn after(&mut self, phase: &'static str) -> CoreResult<()>;
}
impl WireState {
    fn before<T, U>(
        &self,
        hooks: &mut impl WireHooks,
        operation_bytes: usize,
        read_bytes: usize,
    ) -> CoreResult<()> {
        let frames = [
            size_of::<&mut Self>(),
            size_of::<&[u8]>(),
            size_of::<&mut usize>(),
            size_of::<TelemetryResult<T>>(),
            size_of::<TelemetryResult<U>>(),
            size_of::<HelperResult>(),
            size_of::<Option<HelperResult>>(),
            size_of::<CoreResult<U>>(),
            size_of::<CoreResult<()>>(),
            operation_bytes,
            size_of::<[usize; 11]>(),
        ];
        let bytes = frames
            .into_iter()
            .try_fold(0usize, usize::checked_add)
            .and_then(|n| n.checked_add(read_bytes))
            .ok_or(CoreFailure::Refused)?;
        hooks.admit(0, frames.len(), bytes)
    }
    pub(crate) fn scalar(
        &mut self,
        input: &[u8],
        cursor: &mut usize,
        _source: WireSource,
        hooks: &mut impl WireHooks,
    ) -> CoreResult<usize> {
        // read_varint inspects at most ten bytes, including nonminimal encodings.
        self.before::<usize, usize>(hooks, 0, 10)?;
        self.actual = Some(HelperResult::Scalar(read_usize(input, cursor)));
        #[cfg(unix)]
        {
            self.source = Some(_source);
            self.cursor = *cursor;
        }
        hooks.after("message-scalar")?;
        match self.actual.as_ref() {
            Some(HelperResult::Scalar(Ok(value))) => Ok(*value),
            _ => Err(CoreFailure::Native),
        }
    }
    pub(crate) fn bytes(
        &mut self,
        input: &[u8],
        cursor: &mut usize,
        _source: WireSource,
        hooks: &mut impl WireHooks,
    ) -> CoreResult<Range<usize>> {
        self.before::<&[u8], Range<usize>>(hooks, 0, 10)?;
        let actual = match read_bytes(input, cursor) {
            Ok(bytes) => Ok(*cursor - bytes.len()..*cursor),
            Err(error) => Err(error),
        };
        self.actual = Some(HelperResult::Bytes(actual));
        #[cfg(unix)]
        {
            self.source = Some(_source);
            self.cursor = *cursor;
        }
        hooks.after("message-range")?;
        match self.actual.as_ref() {
            Some(HelperResult::Bytes(Ok(range))) => Ok(range.clone()),
            _ => Err(CoreFailure::Native),
        }
    }
    pub(crate) fn unit(
        &mut self,
        operation: impl FnOnce() -> TelemetryResult<()>,
        hooks: &mut impl WireHooks,
    ) -> CoreResult<()> {
        self.before::<(), ()>(hooks, size_of_val(&operation), 0)?;
        self.actual = Some(HelperResult::Unit(operation()));
        hooks.after("message-helper-result")?;
        if matches!(self.actual, Some(HelperResult::Unit(Ok(())))) {
            Ok(())
        } else {
            Err(CoreFailure::Native)
        }
    }
    fn template(
        &mut self,
        index: &EmbeddedFrameIndex,
        ordinal: usize,
        templates: usize,
        selected: &mut Option<usize>,
        hooks: &mut impl WireHooks,
    ) -> CoreResult<Option<usize>> {
        // packed_id uses one fixed five-byte window, no allocation or scan.
        self.before::<Option<usize>, Option<usize>>(hooks, 0, 5)?;
        self.actual = Some(HelperResult::Template(decoder::body_template_id(
            index, ordinal, templates,
        )));
        *selected = match self.actual.as_ref() {
            Some(HelperResult::Template(Ok(value))) => *value,
            _ => None,
        };
        hooks.after("message-layout")?;
        match self.actual.as_ref() {
            Some(HelperResult::Template(Ok(value))) => Ok(*value),
            _ => Err(CoreFailure::Native),
        }
    }
    #[cfg(unix)]
    pub(crate) fn native_error(&self) -> Option<&TelemetryError> {
        match self.actual.as_ref()? {
            HelperResult::Scalar(Err(e))
            | HelperResult::Bytes(Err(e))
            | HelperResult::Unit(Err(e))
            | HelperResult::Template(Err(e)) => Some(e),
            _ => None,
        }
    }
    pub(crate) fn take_error(&mut self) -> TelemetryError {
        match self.actual.take() {
            Some(
                HelperResult::Scalar(Err(e))
                | HelperResult::Bytes(Err(e))
                | HelperResult::Unit(Err(e))
                | HelperResult::Template(Err(e)),
            ) => e,
            _ => TelemetryError::InvalidBlockEncoding("missing message traversal error"),
        }
    }
}
#[derive(Default, Debug)]
pub(crate) struct TemplateState {
    pub(crate) cursor: usize,
    pub(crate) count: usize,
    pub(crate) literal_count: usize,
}
pub(crate) trait TemplateHooks: WireHooks {
    fn templates(&mut self, count: usize) -> CoreResult<()>;
    fn begin_template(&mut self, literals: usize) -> CoreResult<()>;
    fn literal(&mut self, range: Range<usize>) -> CoreResult<()>;
    fn end_template(&mut self) -> CoreResult<()>;
}
pub(crate) fn visit_templates(
    input: &[u8],
    state: &mut TemplateState,
    wire: &mut WireState,
    hooks: &mut impl TemplateHooks,
) -> CoreResult<()> {
    state.count = wire.scalar(input, &mut state.cursor, WireSource::Templates, hooks)?;
    wire.unit(
        || {
            ensure_count_within(
                state.count,
                input.len().saturating_sub(state.cursor),
                "template count",
            )
        },
        hooks,
    )?;
    hooks.templates(state.count)?;
    for _ in 0..state.count {
        state.literal_count =
            wire.scalar(input, &mut state.cursor, WireSource::Templates, hooks)?;
        wire.unit(
            || {
                if state.literal_count == 0 {
                    Err(TelemetryError::InvalidBlockEncoding(
                        "template has no literals",
                    ))
                } else {
                    Ok(())
                }
            },
            hooks,
        )?;
        wire.unit(
            || {
                ensure_count_within(
                    state.literal_count,
                    input.len().saturating_sub(state.cursor),
                    "template literal count",
                )
            },
            hooks,
        )?;
        hooks.begin_template(state.literal_count)?;
        for _ in 0..state.literal_count {
            let range = wire.bytes(input, &mut state.cursor, WireSource::Templates, hooks)?;
            hooks.literal(range)?;
        }
        hooks.end_template()?;
    }
    wire.unit(|| require_consumed(input, state.cursor), hooks)
}
#[derive(Default, Debug)]
pub(crate) struct LaneState {
    pub(crate) footer_start: usize,
    pub(crate) directory_start: usize,
    pub(crate) cursor: usize,
    pub(crate) interval: usize,
    pub(crate) count: usize,
    pub(crate) index: usize,
    pub(crate) previous: usize,
    pub(crate) current: usize,
    pub(crate) checkpoints: Vec<usize>,
}
pub(crate) trait LaneHooks: WireHooks {
    fn reserve_checkpoints(&mut self, output: &mut Vec<usize>, count: usize) -> CoreResult<()>;
}
pub(crate) fn visit_lane(
    input: &[u8],
    records: usize,
    state: &mut LaneState,
    wire: &mut WireState,
    hooks: &mut impl LaneHooks,
) -> CoreResult<()> {
    // Fixed footer validation is the same original typed helper outcome.
    hooks.admit(0, 1, size_of::<u32>())?;
    wire.unit(
        || {
            state.footer_start = input.len().checked_sub(size_of::<u32>()).ok_or(
                TelemetryError::InvalidBlockEncoding("record lane footer is truncated"),
            )?;
            state.directory_start = usize::try_from(u32::from_le_bytes(
                input[state.footer_start..].try_into().map_err(|_| {
                    TelemetryError::InvalidBlockEncoding("record lane footer is invalid")
                })?,
            ))
            .map_err(|_| {
                TelemetryError::InvalidBlockEncoding("record lane footer does not fit usize")
            })?;
            input.get(state.directory_start..state.footer_start).ok_or(
                TelemetryError::InvalidBlockEncoding("record lane directory is invalid"),
            )?;
            input
                .get(..state.directory_start)
                .ok_or(TelemetryError::InvalidBlockEncoding(
                    "record lane payload is truncated",
                ))?;
            Ok(())
        },
        hooks,
    )?;
    let directory = &input[state.directory_start..state.footer_start];
    state.interval = wire.scalar(directory, &mut state.cursor, WireSource::Directory, hooks)?;
    wire.unit(
        || {
            if state.interval == 0 {
                Err(TelemetryError::InvalidBlockEncoding(
                    "record lane checkpoint interval is zero",
                ))
            } else {
                Ok(())
            }
        },
        hooks,
    )?;
    state.count = wire.scalar(directory, &mut state.cursor, WireSource::Directory, hooks)?;
    wire.unit(
        || {
            if state.count != records.div_ceil(state.interval) {
                Err(TelemetryError::InvalidBlockEncoding(
                    "record lane checkpoint count mismatch",
                ))
            } else {
                Ok(())
            }
        },
        hooks,
    )?;
    hooks.reserve_checkpoints(&mut state.checkpoints, state.count)?;
    for index in 0..state.count {
        state.index = index;
        let delta = wire.scalar(directory, &mut state.cursor, WireSource::Directory, hooks)?;
        wire.unit(
            || {
                state.current = state.previous.checked_add(delta).ok_or(
                    TelemetryError::InvalidBlockEncoding("record lane checkpoint overflow"),
                )?;
                if (index == 0 && state.current != 0)
                    || (index > 0 && state.current < state.previous)
                {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "record lane checkpoints are not ordered",
                    ));
                }
                Ok(())
            },
            hooks,
        )?;
        hooks.admit(0, 1, size_of::<usize>())?;
        state.checkpoints.push(state.current);
        state.previous = state.current;
        hooks.after("message-checkpoint")?;
    }
    wire.unit(|| require_consumed(directory, state.cursor), hooks)?;
    wire.unit(
        || {
            if state
                .checkpoints
                .last()
                .is_some_and(|n| *n > state.directory_start)
                || (records == 0 && state.directory_start != 0)
            {
                Err(TelemetryError::InvalidBlockEncoding(
                    "record lane checkpoint exceeds payload",
                ))
            } else {
                Ok(())
            }
        },
        hooks,
    )
}
#[derive(Debug, Clone)]
pub(crate) enum BodyRecord {
    Raw(Range<usize>),
    Exact(usize),
    Dynamic {
        template: usize,
        #[cfg(unix)]
        encoded: Range<usize>,
    },
}
#[derive(Default, Debug)]
pub(crate) struct BodyState {
    pub(crate) ordinal: usize,
    pub(crate) cursor: usize,
    pub(crate) record_start: usize,
    pub(crate) template: Option<usize>,
    pub(crate) replay: usize,
    pub(crate) actual: Option<BodyRecord>,
}
pub(crate) struct BodyInput<'a> {
    pub(crate) payload: &'a [u8],
    pub(crate) index: &'a EmbeddedFrameIndex,
    pub(crate) templates: usize,
    pub(crate) interval: usize,
    pub(crate) checkpoints: &'a [usize],
}
pub(crate) fn scan_body_record(
    input: BodyInput<'_>,
    literal_count: impl FnOnce(usize) -> usize,
    state: &mut BodyState,
    wire: &mut WireState,
    hooks: &mut impl WireHooks,
) -> CoreResult<()> {
    let frames = [
        size_of::<BodyRecord>(),
        size_of::<Option<BodyRecord>>(),
        size_of_val(&literal_count),
        size_of_val(&input),
        size_of::<[usize; 5]>(),
    ];
    let bytes = frames
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .ok_or(CoreFailure::Refused)?;
    hooks.admit(0, frames.len(), bytes)?;
    wire.unit(
        || {
            lanes::validate_checkpoint_parts(
                input.interval,
                input.checkpoints,
                state.ordinal,
                state.cursor,
            )
        },
        hooks,
    )?;
    state.actual = None;
    state.record_start = state.cursor;
    let _ = wire.template(
        input.index,
        state.ordinal,
        input.templates,
        &mut state.template,
        hooks,
    )?;
    let actual = match state.template {
        None => BodyRecord::Raw(wire.bytes(
            input.payload,
            &mut state.cursor,
            WireSource::Bodies,
            hooks,
        )?),
        Some(template) => {
            let count = literal_count(template);
            if count == 1 {
                BodyRecord::Exact(template)
            } else {
                for _ in 1..count {
                    let _ =
                        wire.bytes(input.payload, &mut state.cursor, WireSource::Bodies, hooks)?;
                }
                BodyRecord::Dynamic {
                    template,
                    #[cfg(unix)]
                    encoded: state.record_start..state.cursor,
                }
            }
        }
    };
    state.actual = Some(actual);
    hooks.after("message-body-record")
}
pub(crate) trait FragmentHooks: WireHooks {
    fn body_fragment(&mut self, range: Range<usize>) -> CoreResult<()>;
    fn template_fragment(&mut self, literal: usize) -> CoreResult<()>;
}
pub(crate) fn replay_body(
    input: &[u8],
    literal_count: usize,
    start: usize,
    state: &mut BodyState,
    wire: &mut WireState,
    hooks: &mut impl FragmentHooks,
) -> CoreResult<()> {
    state.replay = start;
    for literal in 1..literal_count {
        let range = wire.bytes(input, &mut state.replay, WireSource::Bodies, hooks)?;
        hooks.body_fragment(range)?;
        hooks.template_fragment(literal)?;
    }
    Ok(())
}

// The ordinary wrappers have no additional ledger. These hooks preserve their
// original infallible allocation/cache paths; they never stand in for owned work.
pub(super) struct Ordinary;
impl WireHooks for Ordinary {
    fn admit(&mut self, _bytes: usize, _entries: usize, _work: usize) -> CoreResult<()> {
        Ok(())
    }
    fn after(&mut self, _phase: &'static str) -> CoreResult<()> {
        Ok(())
    }
}
impl LaneHooks for Ordinary {
    fn reserve_checkpoints(&mut self, out: &mut Vec<usize>, count: usize) -> CoreResult<()> {
        *out = Vec::with_capacity(count);
        Ok(())
    }
}
pub(super) struct OrdinaryTemplates<'a> {
    pub(super) input: &'a [u8],
    pub(super) current: Vec<Vec<u8>>,
    pub(super) templates: Vec<Vec<Vec<u8>>>,
}
impl WireHooks for OrdinaryTemplates<'_> {
    fn admit(&mut self, _bytes: usize, _entries: usize, _work: usize) -> CoreResult<()> {
        Ok(())
    }
    fn after(&mut self, _phase: &'static str) -> CoreResult<()> {
        Ok(())
    }
}
impl TemplateHooks for OrdinaryTemplates<'_> {
    fn templates(&mut self, count: usize) -> CoreResult<()> {
        self.templates = Vec::with_capacity(count);
        Ok(())
    }
    fn begin_template(&mut self, count: usize) -> CoreResult<()> {
        self.current = Vec::with_capacity(count);
        Ok(())
    }
    fn literal(&mut self, range: Range<usize>) -> CoreResult<()> {
        self.current.push(self.input[range].to_vec());
        Ok(())
    }
    fn end_template(&mut self) -> CoreResult<()> {
        self.templates.push(std::mem::take(&mut self.current));
        Ok(())
    }
}
pub(super) struct OrdinaryFragments<'a> {
    pub(super) payload: &'a [u8],
    pub(super) literals: &'a [Vec<u8>],
    pub(super) output: &'a mut Vec<u8>,
}
impl WireHooks for OrdinaryFragments<'_> {
    fn admit(&mut self, _bytes: usize, _entries: usize, _work: usize) -> CoreResult<()> {
        Ok(())
    }
    fn after(&mut self, _phase: &'static str) -> CoreResult<()> {
        Ok(())
    }
}
impl FragmentHooks for OrdinaryFragments<'_> {
    fn body_fragment(&mut self, range: Range<usize>) -> CoreResult<()> {
        self.output.extend_from_slice(&self.payload[range]);
        Ok(())
    }
    fn template_fragment(&mut self, literal: usize) -> CoreResult<()> {
        self.output.extend_from_slice(&self.literals[literal]);
        Ok(())
    }
}

// Append inside structural/message_core.rs; reexport under the same cfg in structural.rs.
#[cfg(all(test, unix))]
pub(crate) fn message_test_wire_fixture(
    records: &[crate::DurableLog],
    templates: &[u8],
    bodies: &[u8],
    template_count: usize,
    layouts: &[u32],
) -> crate::TelemetryResult<Vec<u8>> {
    use super::*;
    let mut actual = crate::encode_indexed_structural_records(records)?;
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let count = read_usize(&actual.structural, &mut cursor)?;
    if count != records.len() || layouts.len() != count {
        return Err(crate::TelemetryError::InvalidBlockEncoding(
            "message test fixture count",
        ));
    }
    let mut sections = Vec::new();
    for _ in 0..8 {
        sections.push(read_section(&actual.structural, &mut cursor)?.to_vec());
    }
    require_consumed(&actual.structural, cursor)?;
    // These fixtures begin with the actual repeated base message. Keep its
    // real term/membership/field evidence; change only the true layout column.
    actual.index.layout_count = u32::try_from(template_count)
        .map_err(|_| crate::TelemetryError::RecordTooLarge)?
        .checked_add(1)
        .ok_or(crate::TelemetryError::RecordTooLarge)?;
    actual.index.layout_ids = pack_ids(layouts, actual.index.layout_count)?;
    if template_count == 0 {
        // The base emitter's original raw fallback was1. Retarget that real
        // encoded coordinate before native admission to the only raw layout0.
        actual.index.residual_layout_ids.clear();
        actual.index.residual_layout_ids.push(0);
    }
    sections[2] = templates.to_vec();
    sections[3] = bodies.to_vec();
    sections[7] = actual.index.encode()?;
    let mut wire = STRUCTURAL_BLOCK_MAGIC.to_vec();
    write_varint(
        u64::try_from(count).map_err(|_| crate::TelemetryError::RecordTooLarge)?,
        &mut wire,
    );
    for section in sections {
        append_bytes(&mut wire, &section)?;
    }
    Ok(wire)
}
