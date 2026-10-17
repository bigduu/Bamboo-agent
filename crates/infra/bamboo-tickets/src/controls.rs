//! Read projections of existing Assignment/Submission facts, never control grants.
use crate::{Assignment, Snapshot, Submission, Ticket};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkControlFacts {
    /// Producer contract only. Every Accept is still independently validated.
    pub accept_requires_stopped_assignment: bool,
    pub submission_id: Option<String>,
    pub assignment_id: Option<String>,
    /// None means no valid current Submission binding; false means unconfirmed,
    /// not proof that a process is currently alive.
    pub process_stopped: Option<bool>,
}

pub(crate) fn matching_submission_assignment<'a>(
    snapshot: &'a Snapshot,
    work: &Ticket,
    submission: &Submission,
) -> Option<&'a Assignment> {
    let a = snapshot.assignments.get(&submission.assignment_id)?;
    (work.current_submission.as_deref() == Some(&submission.id)
        && !submission.stale
        && submission.work_id == work.id
        && submission.generation == work.generation
        && submission.contract_revision == work.contract_revision
        && a.work_id == work.id
        && a.generation == submission.generation
        && a.contract_revision == submission.contract_revision
        && a.runtime.as_ref() == Some(&submission.runtime))
    .then_some(a)
}

pub(crate) fn facts(snapshot: &Snapshot, work: &Ticket) -> WorkControlFacts {
    let submission = work
        .current_submission
        .as_ref()
        .and_then(|id| snapshot.submissions.get(id));
    let assignment = submission.and_then(|s| matching_submission_assignment(snapshot, work, s));
    WorkControlFacts {
        accept_requires_stopped_assignment: true,
        submission_id: work.current_submission.clone(),
        assignment_id: submission.map(|s| s.assignment_id.clone()),
        process_stopped: assignment.map(|a| a.process_stopped),
    }
}
