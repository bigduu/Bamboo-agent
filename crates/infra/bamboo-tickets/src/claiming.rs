//! Bounded host-side selection for the first Supervisor claim slice.
//!
//! This is not a worker loop or a second authority store. The host supplies only
//! candidates whose current project/context, permission and remaining cumulative
//! budget and resource availability it has checked against `observed`. Unknown
//! eligibility must be omitted.
//! No grant is recovered from this input after a restart. Runtime admission still
//! needs fresh host authorization; `Start` retains all existing validation.
use crate::{
    service::{holds_resources, validate_authority},
    *,
};

/// Upper bound on a single host selection pass. No provider is called here.
pub const CLAIM_CANDIDATE_LIMIT: usize = 64;

/// Trusted-host input, deliberately not deserializable from worker/model JSON.
#[derive(Clone, Debug)]
pub struct ClaimCandidate {
    pub work_id: String,
    pub workspace: Option<ExecutionWorkspace>,
}

#[derive(Clone, Debug)]
pub enum ClaimDecision {
    /// Execute this exact command and retry this same command if its ACK is lost.
    /// Never prepare a new logical dispatch to resolve an unknown outcome.
    Prepared(Command),
    CapacityBlocked {
        occupied: usize,
        capacity: usize,
    },
    NoEligibleWork,
}

impl TicketService {
    /// Select oldest-updated Ready Work (ID breaks ties) from a bounded, already
    /// host-validated set. This method performs no write and does not start work.
    ///
    /// Selection and capacity are fenced by the *whole scope* seq/epoch in the
    /// returned command. Any intervening mutation makes execute conflict, even
    /// when two consumers select different Work for the last slot. On conflict,
    /// the host must read and validate eligibility again, not refresh the CAS.
    ///
    /// Capacity includes uncertain admission and unconfirmed stop/pause. Time
    /// passing or a heartbeat cannot free a slot or prove a side effect stopped.
    /// This is a scope-wide capacity ceiling, not foreground/background fairness.
    pub fn prepare_ready_claim(
        &self,
        authority: &Authority,
        observed: &SnapshotRef,
        operation_id: &str,
        candidates: &[ClaimCandidate],
        capacity: usize,
    ) -> Result<ClaimDecision> {
        let (commit, snapshot) = self.published()?;
        validate_authority(authority, &snapshot)?;
        if !matches!(
            authority.principal(),
            Principal::Supervisor { .. } | Principal::User { .. }
        ) {
            return Err(Error::ScopeDenied(
                "Supervisor/User claim authority required".into(),
            ));
        }
        if observed.commit != commit
            || observed.seq != snapshot.seq
            || observed.authority_epoch != snapshot.authority_epoch
        {
            return Err(Error::RevisionConflict);
        }
        if operation_id.is_empty()
            || operation_id.len() > 256
            || candidates.len() > CLAIM_CANDIDATE_LIMIT
            || candidates
                .iter()
                .map(|c| &c.work_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != candidates.len()
        {
            return Err(Error::InvalidTransition(
                "invalid claim id or candidate bound".into(),
            ));
        }
        // A logical claim ID cannot be repurposed after a successful/unknown ACK.
        // Replay the exact command instead, using execute's persisted receipt.
        if snapshot.receipts.contains_key(operation_id) {
            return Err(Error::IdempotencyConflict);
        }
        let occupied = snapshot
            .assignments
            .values()
            .filter(|a| holds_resources(a))
            .count();
        if occupied >= capacity {
            return Ok(ClaimDecision::CapacityBlocked { occupied, capacity });
        }
        let candidate = candidates
            .iter()
            .filter_map(|candidate| {
                let work = snapshot.tickets.get(&candidate.work_id)?;
                (work.kind == TicketKind::Work
                    && work.state == WorkState::Ready
                    && !work.paused
                    && !work.archived
                    && work.active_assignment.is_none()
                    && work.depends_on.iter().all(|id| {
                        snapshot.tickets.get(id).is_some_and(|dependency| {
                            dependency.state == WorkState::Accepted
                                && dependency
                                    .accepted_submission
                                    .as_ref()
                                    .is_some_and(|id| snapshot.submissions.contains_key(id))
                        })
                    }))
                .then_some((work, candidate))
            })
            .min_by(|(a, _), (b, _)| (a.updated_seq, &a.id).cmp(&(b.updated_seq, &b.id)));
        let Some((work, candidate)) = candidate else {
            return Ok(ClaimDecision::NoEligibleWork);
        };
        Ok(ClaimDecision::Prepared(Command {
            operation_id: operation_id.into(),
            binding: snapshot.binding,
            expected_seq: snapshot.seq,
            expected_epoch: snapshot.authority_epoch,
            source: None,
            operations: vec![Operation::Start {
                work_id: work.id.clone(),
                temp_id: "assignment".into(),
                workspace: candidate.workspace.clone(),
            }],
        }))
    }
}
