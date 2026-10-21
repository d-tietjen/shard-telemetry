use super::*;

pub(super) fn input(
    admission: &EvidenceAdmission,
    context: &EvidenceBatchContext,
    events: &[EvidenceEventIdentity],
    encoded: &[u8],
    limits: EvidenceExportLimits,
) -> Result<(), EvidenceExportError> {
    use EvidenceExportError::{InvalidInput, Limits};
    if limits.max_object_bytes == 0
        || limits.max_object_bytes > MAX_EVIDENCE_OBJECT_BYTES
        || limits.max_manifest_bytes == 0
        || limits.max_manifest_bytes > MAX_EVIDENCE_MANIFEST_BYTES
        || limits.max_events == 0
        || limits.max_events > MAX_EVIDENCE_EVENTS
    {
        return Err(Limits("zero or unsupported preparation limit"));
    }
    if encoded.len() > limits.max_object_bytes || events.len() > limits.max_events {
        return Err(Limits("encoded object or identity count"));
    }
    if admission.dataset_version != EVIDENCE_DATASET_VERSION {
        return Err(InvalidInput("unsupported dataset version"));
    }
    let owner = &admission.owner;
    if [
        owner.organization_id,
        owner.source_id,
        owner.source_incarnation,
    ]
    .contains(&[0; 16])
    {
        return Err(InvalidInput(
            "nil organization, source or source incarnation",
        ));
    }
    if owner.plane.is_empty()
        || owner.plane.len() > 32
        || owner.plane == "."
        || owner.plane == ".."
        || !owner
            .plane
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_-.".contains(&b))
    {
        return Err(InvalidInput(
            "plane must be a canonical safe namespace token",
        ));
    }
    string(&admission.consent_revision)?;
    if admission.allowed_signals.is_empty() || admission.allowed_signals.len() > 3 {
        return Err(InvalidInput("explicit allowed signal set required"));
    }
    for (i, signal) in admission.allowed_signals.iter().enumerate() {
        if i > 0 && admission.allowed_signals[i - 1] >= *signal {
            return Err(InvalidInput("allowed signals must be sorted and distinct"));
        }
    }
    let envelope = crate::TelemetryEnvelope::decode_view(encoded)?;
    if envelope.tenant != owner.plane
        || !admission.allowed_signals.contains(&envelope.signal)
        || envelope.item_count as usize != events.len()
        || context.source_partition.topic_id != envelope.signal.topic_id()
    {
        return Err(InvalidInput(
            "plane, allowed signal, count or source topic mismatch",
        ));
    }
    if let Some(domain) = &context.clock.domain {
        string(domain)?;
    } else if context.clock.uncertainty_nanos.is_some() {
        return Err(InvalidInput("clock uncertainty requires a clock domain"));
    }
    let frontier = context.observed_source_next_offset.get();
    if context
        .retained_source_start_offset
        .is_some_and(|offset| offset.get() > frontier)
    {
        return Err(InvalidInput(
            "retained source floor exceeds observed frontier",
        ));
    }
    let losses = match &context.loss {
        EvidenceLossStatus::Unknown => &[][..],
        EvidenceLossStatus::Reported(losses) if losses.len() <= 16 => losses.as_slice(),
        EvidenceLossStatus::Reported(_) => return Err(Limits("loss interval count")),
    };
    let mut previous_loss_end = None;
    for loss in losses {
        if loss.first_offset >= loss.next_offset
            || loss.next_offset > frontier
            || previous_loss_end.is_some_and(|end| end > loss.first_offset)
        {
            return Err(InvalidInput(
                "loss intervals must be sorted, disjoint and source-bounded",
            ));
        }
        previous_loss_end = Some(loss.next_offset);
    }
    let mut previous = None;
    for event in events {
        let offset = event.original.offset.get();
        if event.original.signal != envelope.signal
            || event.original.topic_partition != context.source_partition
            || offset >= frontier
            || previous.is_some_and(|offset_before| offset_before >= offset)
            || event.process_incarnation == [0; 16]
            || losses
                .iter()
                .any(|loss| loss.first_offset <= offset && offset < loss.next_offset)
        {
            return Err(InvalidInput(
                "invalid, duplicate, reordered or unavailable source identity",
            ));
        }
        if let Some(id) = &event.producer_event_id {
            string(id)?;
        }
        previous = Some(offset);
    }
    Ok(())
}

fn string(s: &str) -> Result<(), EvidenceExportError> {
    if s.is_empty() || s.len() > 128 {
        return Err(EvidenceExportError::InvalidInput("metadata string length"));
    }
    Ok(())
}
