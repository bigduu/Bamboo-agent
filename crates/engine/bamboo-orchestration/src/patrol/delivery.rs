//! Bounded delivery bookkeeping over the original Patrol checkpoint.
//! The Host stages the notice in its canonical Inbox; this is no second queue,
//! execution grant, activation or model-budget reservation.

use super::{Checkpoint, CheckpointPort, Clock, Error, Escalation, Phase, Policy, Scope};

pub struct PendingNotice {
    pub escalation: Escalation,
    pub opened_at_ms: u64,
    /// False during cooldown or after the attempt bound. The Host may only
    /// reconcile the original durable receipt, never enqueue another notice.
    pub may_enqueue: bool,
    checkpoint: Checkpoint,
    version: u64,
}

impl PendingNotice {
    /// Call only after observing the exact original canonical Inbox receipt.
    /// Losing this ACK is safe: the next pass reconciles that same stable ID.
    pub fn acknowledge(mut self, checkpoints: &mut dyn CheckpointPort) -> Result<(), Error> {
        let escalation = self
            .checkpoint
            .incident
            .as_mut()
            .and_then(|incident| incident.escalation.as_mut())
            .ok_or(Error::InvalidCheckpoint)?;
        // A legacy journal with no persisted delivery attempt cannot have an
        // authentic receipt from this adapter. Do not fabricate an attempt.
        if escalation.deliveries == 0 {
            return Err(Error::InvalidCheckpoint);
        }
        escalation.queued = true;
        checkpoints.save(Some(self.version), &self.checkpoint)?;
        Ok(())
    }
}

/// Reserve an attempt durably before any enqueue. The exclusive Host journal
/// lease remains held until ACK, so concurrent program/delivery passes cannot
/// replace the incident. Every source/authority check still belongs to the Host.
pub fn prepare(
    scope: &Scope,
    policy: &Policy,
    clock: &dyn Clock,
    checkpoints: &mut dyn CheckpointPort,
) -> Result<Option<PendingNotice>, Error> {
    // Validate exactly the existing policy bounds without enabling semantics.
    super::Patrol::new(policy.clone(), None)?;
    let Some(mut stored) = checkpoints.load(scope)? else {
        return Ok(None);
    };
    super::validate_checkpoint(&stored.checkpoint, scope)?;
    if stored.version == 0 {
        return Err(Error::InvalidCheckpoint);
    }
    let now = clock.now_ms();
    if now < stored.checkpoint.last_check_ms {
        return Err(Error::ClockRegression);
    }
    let Some(incident) = stored.checkpoint.incident.as_mut() else {
        return Ok(None);
    };
    if matches!(incident.phase, Phase::Resolved { .. }) {
        return Ok(None);
    }
    let Some(escalation) = incident.escalation.as_mut() else {
        return Ok(None);
    };
    if escalation.queued {
        return Ok(None);
    }
    let may_enqueue = now >= escalation.next_delivery_ms
        && escalation.deliveries < policy.max_escalation_deliveries;
    if may_enqueue {
        escalation.deliveries += 1;
        escalation.next_delivery_ms = now.saturating_add(policy.cooldown_ms);
    }
    let escalation = escalation.clone();
    let opened_at_ms = incident.opened_at_ms;
    if may_enqueue {
        stored.version = checkpoints.save(Some(stored.version), &stored.checkpoint)?;
    }
    Ok(Some(PendingNotice {
        escalation,
        opened_at_ms,
        may_enqueue,
        checkpoint: stored.checkpoint,
        version: stored.version,
    }))
}
