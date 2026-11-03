//! Meter the exact evidence selected by a trusted Host for one diagnostic model.

use std::collections::BTreeSet;

use super::{
    evidence::{
        UnmeteredWorkPatrolEvidence, WorkPatrolEvidenceCoverage, WorkPatrolSourceEvidence,
        WorkPatrolSourceOutcome, MAX_SOURCE_KEY_BYTES, MAX_SOURCE_VERSION_BYTES,
        MAX_WORK_PATROL_EVIDENCE_BYTES, MAX_WORK_PATROL_EVIDENCE_REFS,
    },
    valid_id, valid_input_evidence, valid_input_tokens, valid_model, valid_scope, DiagnosticInput,
    EvidenceRef, Model, Observation, PortError,
};
use bamboo_tickets::{canonical_bytes, content_hash};
use serde::Serialize;

/// Convert one verified evidence snapshot into the exact input the meter saw.
/// The trusted local meter must include the selected model's complete framing,
/// instructions, and schema. The Host still owns authorization and admission.
pub fn prepare_diagnostic_input<R: Serialize, S: Serialize>(
    evidence: &UnmeteredWorkPatrolEvidence<R, S>,
    expected_commit: &str,
    since_cursor: u64,
    through: &Observation,
    model: &Model,
    meter: impl FnOnce(&Model, &str, &[EvidenceRef]) -> Result<u32, PortError>,
) -> Result<DiagnosticInput, PortError> {
    if evidence.scope != through.scope
        || evidence.source_commit != expected_commit
        || evidence.execution_facts_revision != through.source_revision
        || evidence.since_cursor != since_cursor
        || evidence.through_cursor != through.cursor
        || since_cursor > through.cursor
    {
        return Err(PortError::Conflict);
    }
    let has_sources = !evidence.sources.is_empty();
    if !valid_model(model)
        || !valid_scope(&through.scope)
        || !valid_id(expected_commit)
        || !valid_id(&evidence.adopted_context_hash)
        || through.cursor == 0
        || evidence.coverage != WorkPatrolEvidenceCoverage::BoundedSnapshot
        || evidence.reference_count > MAX_WORK_PATROL_EVIDENCE_REFS
        || evidence.reference_count.checked_sub(2) != Some(evidence.sources.len())
        || evidence.all_sources_matched != has_sources
        || !valid_source_pins(&evidence.sources)
    {
        return Err(PortError::Denied);
    }

    let bytes = canonical_bytes(evidence).map_err(|_| PortError::Unavailable)?;
    if bytes.len() > MAX_WORK_PATROL_EVIDENCE_BYTES {
        return Err(PortError::Denied);
    }
    let summary = String::from_utf8(bytes).map_err(|_| PortError::Unavailable)?;
    let scope = &evidence.scope;
    let revision = evidence.execution_facts_revision;
    let mut references = Vec::with_capacity(evidence.reference_count);
    references.push(reference(
        &("patrol_ticket_v1", scope, expected_commit),
        revision,
    )?);
    references.push(reference(
        &("patrol_context_v1", scope, &evidence.adopted_context_hash),
        revision,
    )?);
    for source in &evidence.sources {
        references.push(reference(
            &(
                "patrol_source_v1",
                scope,
                &source.source_key,
                &source.expected_version,
            ),
            revision,
        )?);
    }
    let mut input = DiagnosticInput {
        scope: scope.clone(),
        source_revision: revision,
        since_cursor,
        through_cursor: through.cursor,
        summary,
        references,
        input_tokens: 0,
    };
    if !valid_input_evidence(&input, model, through, since_cursor) {
        return Err(PortError::Denied);
    }
    input.input_tokens = meter(model, &input.summary, &input.references)?;
    if !valid_input_tokens(input.input_tokens, model) {
        return Err(PortError::Denied);
    }
    Ok(input)
}

fn valid_source_pins<R, S>(sources: &[WorkPatrolSourceEvidence<R, S>]) -> bool {
    let mut keys = BTreeSet::new();
    sources.iter().all(|source| {
        let key = source.source_key.as_str();
        let version = source.expected_version.as_str();
        !key.is_empty()
            && key.len() <= MAX_SOURCE_KEY_BYTES
            && !version.is_empty()
            && version.len() <= MAX_SOURCE_VERSION_BYTES
            && keys.insert(key)
            && matches!(
                source.outcome,
                WorkPatrolSourceOutcome::VersionMatched { .. }
            )
    })
}

fn reference(identity: &impl Serialize, revision: u64) -> Result<EvidenceRef, PortError> {
    let bytes = canonical_bytes(identity).map_err(|_| PortError::Unavailable)?;
    Ok(EvidenceRef {
        id: content_hash(&bytes),
        revision,
    })
}
