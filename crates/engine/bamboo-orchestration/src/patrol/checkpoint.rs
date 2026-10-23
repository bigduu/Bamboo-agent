//! Versioned bounded checkpoint codec. File ownership, locking and atomic I/O
//! belong to the outer Host adapter; this module stores no Ticket facts or funds.

use serde::{Deserialize, Serialize};

use super::{Checkpoint, Phase, PortError, Scope, StoredCheckpoint, Usage};

pub const MAX_CHECKPOINT_BYTES: usize = 512 * 1024;
const FORMAT: &str = "bamboo/patrol-checkpoint";
const SCHEMA: u32 = 1;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    format: String,
    schema: u32,
    record: StoredCheckpoint,
    sha256: String,
}

pub fn scope_key(scope: &Scope) -> Result<String, PortError> {
    if !super::valid_scope(scope) {
        return Err(PortError::Denied);
    }
    Ok(bamboo_tickets::content_hash(
        &bamboo_tickets::canonical_bytes(scope).map_err(|_| PortError::Unavailable)?,
    ))
}

pub fn encode(version: u64, checkpoint: &Checkpoint) -> Result<Vec<u8>, PortError> {
    let record = StoredCheckpoint {
        version,
        checkpoint: checkpoint.clone(),
    };
    validate(&checkpoint.scope, &record)?;
    let sha256 = bamboo_tickets::content_hash(
        &bamboo_tickets::canonical_bytes(&record).map_err(|_| PortError::Unavailable)?,
    );
    let envelope = Envelope {
        format: FORMAT.into(),
        schema: SCHEMA,
        record,
        sha256,
    };
    let bytes = bamboo_tickets::canonical_bytes(&envelope).map_err(|_| PortError::Unavailable)?;
    if bytes.len() > MAX_CHECKPOINT_BYTES {
        return Err(PortError::Unavailable);
    }
    Ok(bytes)
}

/// Absence is handled by the Host, never by converting unreadable/unknown bytes
/// into an empty checkpoint. Future schemas and corrupt records fail closed.
pub fn decode(scope: &Scope, bytes: &[u8]) -> Result<StoredCheckpoint, PortError> {
    if bytes.is_empty() || bytes.len() > MAX_CHECKPOINT_BYTES {
        return Err(PortError::Unavailable);
    }
    let envelope: Envelope = serde_json::from_slice(bytes).map_err(|_| PortError::Unavailable)?;
    if envelope.format != FORMAT
        || envelope.schema != SCHEMA
        || bamboo_tickets::content_hash(
            &bamboo_tickets::canonical_bytes(&envelope.record)
                .map_err(|_| PortError::Unavailable)?,
        ) != envelope.sha256
    {
        return Err(PortError::Unavailable);
    }
    validate(scope, &envelope.record)?;
    Ok(envelope.record)
}

fn validate(scope: &Scope, record: &StoredCheckpoint) -> Result<(), PortError> {
    if !super::valid_scope(scope) || record.version == 0 {
        return Err(PortError::Denied);
    }
    super::validate_checkpoint(&record.checkpoint, scope).map_err(|_| PortError::Unavailable)
}

/// CAS alone must not make a stale caller able to erase a persisted incident,
/// retry counter or delivered escalation. These are policy-bookkeeping checks,
/// never a Task transition, new permission or a budget reconciliation.
pub fn validate_update(previous: &Checkpoint, next: &Checkpoint) -> Result<(), PortError> {
    if previous.scope != next.scope {
        return Err(PortError::Denied);
    }
    if next.cursor < previous.cursor
        || next.source_revision < previous.source_revision
        || next.last_check_ms < previous.last_check_ms
    {
        return Err(PortError::Conflict);
    }
    if let Some(old) = &previous.incident {
        let Some(new) = &next.incident else {
            return Err(PortError::Conflict);
        };
        if old.id != new.id {
            if old.pending_settlement.is_some()
                || !matches!(old.phase, Phase::Resolved { .. })
                || new.id.progress_sequence <= old.id.progress_sequence
            {
                return Err(PortError::Conflict);
            }
        } else {
            if new.attempts < old.attempts
                || new.last_diagnostic_cursor < old.last_diagnostic_cursor
                || new.opened_at_ms != old.opened_at_ms
                || new.opened_cursor != old.opened_cursor
                || new.source_revision != old.source_revision
                || (matches!(old.phase, Phase::Resolved { .. })
                    && !matches!(new.phase, Phase::Resolved { .. }))
            {
                return Err(PortError::Conflict);
            }
            if new.pending_settlement.is_none() && new.pending_settlement_usage.is_some() {
                return Err(PortError::Conflict);
            }
            if let (Some(before), Some(after)) = (&old.pending_settlement, &new.pending_settlement)
            {
                if before != after
                    || old
                        .pending_settlement_usage
                        .is_some_and(|usage| new.pending_settlement_usage != Some(usage))
                    || (old.pending_settlement_usage.is_none()
                        && !matches!(old.phase, Phase::Diagnosing { .. })
                        && new
                            .pending_settlement_usage
                            .is_some_and(|usage| usage != Usage::Unknown))
                {
                    return Err(PortError::Conflict);
                }
            }
            if let Some(prior) = &old.escalation {
                let Some(current) = &new.escalation else {
                    return Err(PortError::Conflict);
                };
                if prior.operation != current.operation
                    || prior.reason != current.reason
                    || prior.source_revision != current.source_revision
                    || prior.cursor != current.cursor
                    || current.deliveries < prior.deliveries
                    || (prior.queued && !current.queued)
                {
                    return Err(PortError::Conflict);
                }
            }
            if let (
                Phase::Diagnosing {
                    request: before,
                    reservation: before_reservation,
                },
                Phase::Diagnosing {
                    request: after,
                    reservation: after_reservation,
                },
            ) = (&old.phase, &new.phase)
            {
                if before != after || before_reservation != after_reservation {
                    return Err(PortError::Conflict);
                }
            }
        }
    }
    Ok(())
}
