//! Scope-local Sprint planning over canonical Work IDs. No execution or budget authority.
use std::collections::{BTreeMap, BTreeSet};

use bamboo_domain::ProjectId;
use serde::{Deserialize, Serialize};

use crate::{
    service::{validate_authority, validate_snapshot},
    *,
};

pub const SPRINT_MEMBER_LIMIT: usize = 128;
const HISTORY_LIMIT: usize = 2048;

/// A separate stable identity with the same bounded opaque representation as ProjectId.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SprintId(ProjectId);

impl SprintId {
    pub fn new() -> Self {
        Self(ProjectId::new())
    }
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        ProjectId::parse(value)
            .map(Self)
            .map_err(|e| invalid(&e.to_string()))
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
impl Default for SprintId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SprintStatus {
    Planned,
    Active,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SprintDefinition {
    pub name: String,
    pub goal: String,
    /// Unix seconds; dates are planning metadata, never an automatic start/stop trigger.
    pub starts_at: Option<i64>,
    pub ends_at: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SprintPlanningState {
    pub sprints: BTreeMap<SprintId, Sprint>,
}
impl SprintPlanningState {
    pub fn is_empty(&self) -> bool {
        self.sprints.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sprint {
    pub id: SprintId,
    pub project_id: ProjectId,
    pub definition: SprintDefinition,
    pub status: SprintStatus,
    pub revision: u64,
    pub created_seq: u64,
    pub updated_seq: u64,
    /// Current members while open; frozen closing members after Closed.
    pub members: BTreeSet<String>,
    pub history: Vec<SprintMembershipEvent>,
    pub closure: Option<SprintClosure>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SprintMembershipChange {
    Added,
    Removed,
    CarriedIn {
        from: SprintId,
    },
    /// None explicitly releases the same Work to backlog.
    CarriedOut {
        to: Option<SprintId>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SprintMembershipEvent {
    pub work_id: String,
    pub work_revision: u64,
    pub seq: u64,
    pub operation_id: String,
    pub principal: String,
    /// Bounded human/host provenance; this field confers no authority.
    pub source: String,
    pub change: SprintMembershipChange,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "destination", rename_all = "snake_case", deny_unknown_fields)]
pub enum SprintDestination {
    Backlog,
    Sprint { sprint_id: SprintId },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "disposition", rename_all = "snake_case")]
pub enum SprintCloseDisposition {
    Accepted,
    Cancelled,
    Backlog,
    Sprint { sprint_id: SprintId },
}

/// A versioned observation, never another mutable Work record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SprintWorkVersion {
    pub record_revision: u64,
    pub contract_revision: u64,
    pub generation: u64,
    pub state: WorkState,
    pub blocked_resume_state: Option<WorkState>,
    pub paused: bool,
    pub archived: bool,
    pub current_submission: Option<String>,
    pub accepted_submission: Option<String>,
    pub active_assignment: Option<String>,
    pub context_hash: Option<String>,
    /// Requires current exact, non-stale accepted evidence, not just a state label.
    pub accepted: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SprintCounts {
    pub total: usize,
    pub accepted: usize,
    pub pending_acceptance: usize,
    pub blocked: usize,
    pub cancelled: usize,
    /// Includes submitted and blocked Work; cancelled is separate.
    pub unfinished: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SprintClosedMember {
    pub version: SprintWorkVersion,
    pub disposition: SprintCloseDisposition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SprintClosure {
    pub source_commit: String,
    pub source_seq: u64,
    pub closed_seq: u64,
    pub members: BTreeMap<String, SprintClosedMember>,
    pub counts: SprintCounts,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SprintCommand {
    pub operation_id: String,
    pub binding: ScopeBinding,
    pub expected_seq: u64,
    pub expected_epoch: u64,
    pub project_id: ProjectId,
    pub sprint_id: SprintId,
    pub source: String,
    pub action: SprintAction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum SprintAction {
    Create {
        definition: SprintDefinition,
    },
    Edit {
        expected_revision: u64,
        definition: SprintDefinition,
    },
    Start {
        expected_revision: u64,
    },
    Add {
        expected_revision: u64,
        work_id: String,
    },
    Remove {
        expected_revision: u64,
        work_id: String,
    },
    Close {
        expected_revision: u64,
        /// Exact set of unfinished members; terminal members must not appear here.
        dispositions: BTreeMap<String, SprintDestination>,
        /// Exact distinct target set, including each target's CAS revision.
        target_revisions: BTreeMap<SprintId, u64>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SprintReceipt {
    pub operation: OperationReceipt,
    /// Full immutable closing result, also returned on an exact retry after restart.
    pub closure: Option<SprintClosure>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SprintInspectRequest {
    pub project_id: ProjectId,
    pub sprint_id: SprintId,
    pub fixed_commit: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SprintView {
    pub sprint: Sprint,
    /// Latest observations at the envelope's snapshot, distinct from frozen closure.
    pub current_members: BTreeMap<String, SprintWorkVersion>,
    pub current_counts: SprintCounts,
}

fn invalid(message: &str) -> Error {
    Error::InvalidTransition(message.into())
}

fn definition_valid(definition: &SprintDefinition) -> Result<()> {
    if definition.name.trim().is_empty()
        || definition.name.len() > 256
        || definition.goal.trim().is_empty()
        || definition.goal.len() > 4096
        || matches!((definition.starts_at, definition.ends_at), (Some(a), Some(b)) if b < a)
    {
        return Err(invalid("invalid Sprint name, goal or dates"));
    }
    Ok(())
}

fn work<'a>(snapshot: &'a Snapshot, project: &ProjectId, id: &str) -> Result<&'a Ticket> {
    let work = snapshot
        .tickets
        .get(id)
        .ok_or_else(|| invalid("Work not found"))?;
    if work.kind != TicketKind::Work || snapshot.project.links.get(id) != Some(project) {
        return Err(Error::ScopeDenied(
            "Sprint requires a Work in the same Project".into(),
        ));
    }
    Ok(work)
}

fn sprint<'a>(snapshot: &'a Snapshot, project: &ProjectId, id: &SprintId) -> Result<&'a Sprint> {
    let sprint = snapshot
        .sprint
        .sprints
        .get(id)
        .ok_or_else(|| invalid("Sprint not found"))?;
    if &sprint.project_id != project {
        return Err(Error::ScopeDenied("Sprint Project mismatch".into()));
    }
    Ok(sprint)
}

fn open_revision(sprint: &Sprint, expected: u64) -> Result<()> {
    if sprint.revision != expected {
        return Err(Error::RevisionConflict);
    }
    if sprint.status == SprintStatus::Closed {
        return Err(invalid("closed Sprint is immutable"));
    }
    Ok(())
}

fn version(snapshot: &Snapshot, work: &Ticket) -> SprintWorkVersion {
    let accepted = work.state == WorkState::Accepted
        && work
            .accepted_submission
            .as_ref()
            .and_then(|id| snapshot.submissions.get(id))
            .is_some_and(|s| {
                !s.stale
                    && s.work_id == work.id
                    && s.generation == work.generation
                    && s.contract_revision == work.contract_revision
                    && work.current_submission.as_deref() == Some(s.id.as_str())
            });
    SprintWorkVersion {
        record_revision: work.record_revision,
        contract_revision: work.contract_revision,
        generation: work.generation,
        state: work.state,
        blocked_resume_state: work.blocked.as_ref().map(|b| b.resume_state),
        paused: work.paused,
        archived: work.archived,
        current_submission: work.current_submission.clone(),
        accepted_submission: work.accepted_submission.clone(),
        active_assignment: work.active_assignment.clone(),
        context_hash: snapshot.project.work_contexts.get(&work.id).cloned(),
        accepted,
    }
}

fn counts<'a>(versions: impl Iterator<Item = &'a SprintWorkVersion>) -> SprintCounts {
    let mut counts = SprintCounts::default();
    for v in versions {
        counts.total += 1;
        counts.accepted += usize::from(v.accepted);
        counts.pending_acceptance += usize::from(v.state == WorkState::Submitted);
        counts.blocked += usize::from(v.state == WorkState::Blocked);
        counts.cancelled += usize::from(v.state == WorkState::Cancelled);
        counts.unfinished += usize::from(!v.accepted && v.state != WorkState::Cancelled);
    }
    counts
}

fn event(
    command: &SprintCommand,
    authority: &Authority,
    seq: u64,
    work: &Ticket,
    change: SprintMembershipChange,
) -> SprintMembershipEvent {
    SprintMembershipEvent {
        work_id: work.id.clone(),
        work_revision: work.record_revision,
        seq,
        operation_id: command.operation_id.clone(),
        principal: authority.identity(),
        source: command.source.clone(),
        change,
    }
}

fn touch(sprint: &mut Sprint, seq: u64) -> Result<()> {
    sprint.revision = sprint
        .revision
        .checked_add(1)
        .ok_or_else(|| invalid("Sprint revision overflow"))?;
    sprint.updated_seq = seq;
    Ok(())
}

impl TicketService {
    /// Trusted host planning port. It cannot Accept Work, dispatch, stop, or grant access.
    /// Empty Projects need a verified scope-local Ticket link before first use.
    pub fn execute_sprint(
        &self,
        authority: &Authority,
        command: &SprintCommand,
    ) -> Result<SprintReceipt> {
        let mut store = self.inner.lock().expect("store mutex");
        let (commit, snapshot) = store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?;
        validate_authority(authority, snapshot)?;
        authority.supervisor()?;
        if command.binding != snapshot.binding {
            return Err(Error::ScopeDenied("command scope".into()));
        }
        if command.operation_id.is_empty()
            || command.operation_id.len() > 256
            || command.source.trim().is_empty()
            || command.source.len() > 512
        {
            return Err(invalid("invalid Sprint operation identity or source"));
        }
        // Domain tag and the shared receipt map fence collisions with Work/file/migration commands.
        let bytes = canonical_bytes(&("sprint", command))?;
        if bytes.len() > 256 * 1024 {
            return Err(Error::ContextBudgetExceeded);
        }
        let hash = content_hash(&bytes);
        if let Some(receipt) = snapshot.receipts.get(&command.operation_id) {
            if receipt.principal != authority.identity() {
                return Err(Error::ScopeDenied("receipt subject".into()));
            }
            if receipt.request_hash != hash {
                return Err(Error::IdempotencyConflict);
            }
            return Ok(SprintReceipt {
                operation: receipt.clone(),
                closure: if matches!(command.action, SprintAction::Close { .. }) {
                    sprint(snapshot, &command.project_id, &command.sprint_id)?
                        .closure
                        .clone()
                } else {
                    None
                },
            });
        }
        if store.health != Health::Writable {
            return Err(Error::AuthorityUnavailable(format!("{:?}", store.health)));
        }
        if command.expected_seq != snapshot.seq
            || command.expected_epoch != snapshot.authority_epoch
        {
            return Err(Error::RevisionConflict);
        }
        if !snapshot
            .project
            .links
            .values()
            .any(|p| p == &command.project_id)
        {
            return Err(Error::ScopeDenied(
                "Project has no verified link in this scope".into(),
            ));
        }
        let mut next = snapshot.clone();
        next.seq = next.seq.checked_add(1).ok_or(Error::RevisionConflict)?;
        next.schema = next.schema.max(6);
        let closure = apply(&mut next, command, authority, commit)?;
        let receipt = OperationReceipt {
            operation_id: command.operation_id.clone(),
            principal: authority.identity(),
            request_hash: hash,
            canonical_request: String::from_utf8(bytes).expect("canonical UTF-8"),
            committed_seq: next.seq,
            ids: BTreeMap::from([("sprint".into(), command.sprint_id.as_str().into())]),
        };
        next.receipts
            .insert(command.operation_id.clone(), receipt.clone());
        validate_snapshot(&next)?;
        #[cfg(feature = "test-utils")]
        store.publish_operation(next, &command.operation_id)?;
        #[cfg(not(feature = "test-utils"))]
        store.publish(next)?;
        Ok(SprintReceipt {
            operation: receipt,
            closure,
        })
    }

    pub fn inspect_sprint(
        &self,
        authority: &Authority,
        request: &SprintInspectRequest,
    ) -> Result<ReadEnvelope<SprintView>> {
        let store = self.inner.lock().expect("store mutex");
        let (current_commit, current) = store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?;
        validate_authority(authority, current)?;
        authority.supervisor()?;
        let commit = request.fixed_commit.as_ref().unwrap_or(current_commit);
        let historical;
        let snapshot = if commit == current_commit {
            current
        } else {
            historical = store
                .load_snapshot(commit)
                .map_err(|_| Error::ResyncRequired)?;
            validate_authority(authority, &historical)?;
            &historical
        };
        let sprint = sprint(snapshot, &request.project_id, &request.sprint_id)?.clone();
        let current_members = sprint
            .members
            .iter()
            .map(|id| {
                Ok((
                    id.clone(),
                    version(snapshot, work(snapshot, &request.project_id, id)?),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let current_counts = counts(current_members.values());
        Ok(ReadEnvelope {
            snapshot: SnapshotRef {
                commit: commit.clone(),
                seq: snapshot.seq,
                authority_epoch: snapshot.authority_epoch,
            },
            index_seq: snapshot.seq,
            coverage: "authoritative_scope".into(),
            truncated: false,
            omitted_count: 0,
            next_cursor: None,
            data: SprintView {
                sprint,
                current_members,
                current_counts,
            },
        })
    }
}

fn apply(
    snapshot: &mut Snapshot,
    command: &SprintCommand,
    authority: &Authority,
    commit: &str,
) -> Result<Option<SprintClosure>> {
    let seq = snapshot.seq;
    let id = &command.sprint_id;
    let project = &command.project_id;
    match &command.action {
        SprintAction::Create { definition } => {
            definition_valid(definition)?;
            if snapshot.sprint.sprints.contains_key(id) {
                return Err(invalid("Sprint already exists"));
            }
            snapshot.sprint.sprints.insert(
                id.clone(),
                Sprint {
                    id: id.clone(),
                    project_id: project.clone(),
                    definition: definition.clone(),
                    status: SprintStatus::Planned,
                    revision: 1,
                    created_seq: seq,
                    updated_seq: seq,
                    members: BTreeSet::new(),
                    history: Vec::new(),
                    closure: None,
                },
            );
        }
        SprintAction::Edit {
            expected_revision,
            definition,
        } => {
            open_revision(sprint(snapshot, project, id)?, *expected_revision)?;
            definition_valid(definition)?;
            let sprint = snapshot.sprint.sprints.get_mut(id).expect("checked");
            sprint.definition = definition.clone();
            touch(sprint, seq)?;
        }
        SprintAction::Start { expected_revision } => {
            let s = sprint(snapshot, project, id)?;
            open_revision(s, *expected_revision)?;
            if s.status != SprintStatus::Planned {
                return Err(invalid("only planned Sprint can start"));
            }
            if snapshot
                .sprint
                .sprints
                .values()
                .any(|s| &s.project_id == project && s.status == SprintStatus::Active)
            {
                return Err(invalid("Project already has an active Sprint"));
            }
            let s = snapshot.sprint.sprints.get_mut(id).expect("checked");
            s.status = SprintStatus::Active;
            touch(s, seq)?;
        }
        SprintAction::Add {
            expected_revision,
            work_id,
        }
        | SprintAction::Remove {
            expected_revision,
            work_id,
        } => {
            let s = sprint(snapshot, project, id)?;
            open_revision(s, *expected_revision)?;
            // Reserve enough history capacity for a complete atomic close.
            if s.history.len() >= HISTORY_LIMIT - SPRINT_MEMBER_LIMIT {
                return Err(Error::ContextBudgetExceeded);
            }
            let w = work(snapshot, project, work_id)?;
            let adding = matches!(command.action, SprintAction::Add { .. });
            if adding {
                if w.archived {
                    return Err(invalid("archived Work cannot join a Sprint"));
                }
                if s.members.len() >= SPRINT_MEMBER_LIMIT {
                    return Err(Error::ContextBudgetExceeded);
                }
                if snapshot
                    .sprint
                    .sprints
                    .values()
                    .any(|s| s.status != SprintStatus::Closed && s.members.contains(work_id))
                {
                    return Err(invalid("Work already belongs to an open Sprint"));
                }
            } else if !s.members.contains(work_id) {
                return Err(invalid("Work is not a member"));
            }
            let event = event(
                command,
                authority,
                seq,
                w,
                if adding {
                    SprintMembershipChange::Added
                } else {
                    SprintMembershipChange::Removed
                },
            );
            let s = snapshot.sprint.sprints.get_mut(id).expect("checked");
            if adding {
                s.members.insert(work_id.clone());
            } else {
                s.members.remove(work_id);
            }
            s.history.push(event);
            touch(s, seq)?;
        }
        SprintAction::Close {
            expected_revision,
            dispositions,
            target_revisions,
        } => {
            let source = sprint(snapshot, project, id)?.clone();
            open_revision(&source, *expected_revision)?;
            if source.status != SprintStatus::Active {
                return Err(invalid("only active Sprint can close"));
            }
            let versions = source
                .members
                .iter()
                .map(|id| Ok((id.clone(), version(snapshot, work(snapshot, project, id)?))))
                .collect::<Result<BTreeMap<_, _>>>()?;
            let unfinished: BTreeSet<_> = versions
                .iter()
                .filter(|(_, v)| !v.accepted && v.state != WorkState::Cancelled)
                .map(|(id, _)| id.clone())
                .collect();
            if dispositions.keys().cloned().collect::<BTreeSet<_>>() != unfinished {
                return Err(invalid(
                    "every unfinished Work needs exactly one explicit disposition",
                ));
            }
            let mut targets: BTreeMap<SprintId, usize> = BTreeMap::new();
            for destination in dispositions.values() {
                if let SprintDestination::Sprint { sprint_id } = destination {
                    *targets.entry(sprint_id.clone()).or_default() += 1;
                }
            }
            if targets.keys().collect::<BTreeSet<_>>()
                != target_revisions.keys().collect::<BTreeSet<_>>()
            {
                return Err(invalid(
                    "carry-over target revisions must match exact destinations",
                ));
            }
            for (target, incoming) in &targets {
                if target == id {
                    return Err(invalid("cannot carry Work into closing Sprint"));
                }
                let s = sprint(snapshot, project, target)?;
                open_revision(s, target_revisions[target])?;
                if s.members.len() + incoming > SPRINT_MEMBER_LIMIT
                    || s.history.len() + incoming > HISTORY_LIMIT - SPRINT_MEMBER_LIMIT
                {
                    return Err(Error::ContextBudgetExceeded);
                }
            }
            let mut members = BTreeMap::new();
            let mut outgoing = Vec::new();
            for (work_id, v) in versions {
                let disposition = if v.accepted {
                    SprintCloseDisposition::Accepted
                } else if v.state == WorkState::Cancelled {
                    SprintCloseDisposition::Cancelled
                } else {
                    let to = match &dispositions[&work_id] {
                        SprintDestination::Backlog => None,
                        SprintDestination::Sprint { sprint_id } => Some(sprint_id.clone()),
                    };
                    let w = work(snapshot, project, &work_id)?;
                    outgoing.push(event(
                        command,
                        authority,
                        seq,
                        w,
                        SprintMembershipChange::CarriedOut { to: to.clone() },
                    ));
                    if let Some(target) = &to {
                        let incoming = event(
                            command,
                            authority,
                            seq,
                            w,
                            SprintMembershipChange::CarriedIn { from: id.clone() },
                        );
                        let target = snapshot.sprint.sprints.get_mut(target).expect("checked");
                        target.members.insert(work_id.clone());
                        target.history.push(incoming);
                    }
                    match to {
                        None => SprintCloseDisposition::Backlog,
                        Some(sprint_id) => SprintCloseDisposition::Sprint { sprint_id },
                    }
                };
                members.insert(
                    work_id,
                    SprintClosedMember {
                        version: v,
                        disposition,
                    },
                );
            }
            for target in targets.keys() {
                touch(
                    snapshot.sprint.sprints.get_mut(target).expect("checked"),
                    seq,
                )?;
            }
            let closure = SprintClosure {
                source_commit: commit.into(),
                source_seq: seq - 1,
                closed_seq: seq,
                counts: counts(members.values().map(|m| &m.version)),
                members,
            };
            let s = snapshot.sprint.sprints.get_mut(id).expect("checked");
            s.status = SprintStatus::Closed;
            s.history.extend(outgoing);
            s.closure = Some(closure.clone());
            touch(s, seq)?;
            return Ok(Some(closure));
        }
    }
    Ok(None)
}

pub(crate) fn validate(snapshot: &Snapshot) -> Result<()> {
    if !snapshot.sprint.is_empty() && snapshot.schema < 6 {
        return Err(invalid("Sprint planning requires schema 6"));
    }
    let mut active = BTreeSet::new();
    let mut membership = BTreeSet::new();
    for (id, s) in &snapshot.sprint.sprints {
        definition_valid(&s.definition)?;
        if id != &s.id
            || s.revision == 0
            || s.created_seq == 0
            || s.created_seq > s.updated_seq
            || s.updated_seq > snapshot.seq
            || s.members.len() > SPRINT_MEMBER_LIMIT
            || s.history.len() > HISTORY_LIMIT
        {
            return Err(invalid("invalid Sprint identity, revision or bounds"));
        }
        if s.status == SprintStatus::Active && !active.insert(&s.project_id) {
            return Err(invalid("multiple active Sprints in Project"));
        }
        for id in &s.members {
            work(snapshot, &s.project_id, id)?;
            if s.status != SprintStatus::Closed && !membership.insert(id) {
                return Err(invalid("multiple open Sprint memberships"));
            }
        }
        let mut historical = BTreeSet::new();
        let mut previous_seq = s.created_seq;
        for e in &s.history {
            work(snapshot, &s.project_id, &e.work_id)?;
            if e.seq < previous_seq
                || e.seq > s.updated_seq
                || e.work_revision == 0
                || e.source.trim().is_empty()
                || e.source.len() > 512
                || e.principal.is_empty()
                || !snapshot
                    .receipts
                    .get(&e.operation_id)
                    .is_some_and(|r| r.principal == e.principal && r.committed_seq == e.seq)
            {
                return Err(invalid("invalid Sprint membership provenance"));
            }
            previous_seq = e.seq;
            let valid = match &e.change {
                SprintMembershipChange::Added => historical.insert(e.work_id.clone()),
                SprintMembershipChange::Removed => historical.remove(&e.work_id),
                SprintMembershipChange::CarriedIn { from } => {
                    let source = sprint(snapshot, &s.project_id, from)?;
                    let matching = source
                        .closure
                        .as_ref()
                        .filter(|c| c.closed_seq == e.seq)
                        .and_then(|c| c.members.get(&e.work_id))
                        .filter(|m| m.version.record_revision == e.work_revision);
                    matching.is_some_and(|m| {
                        m.disposition
                            == SprintCloseDisposition::Sprint {
                                sprint_id: s.id.clone(),
                            }
                    }) && historical.insert(e.work_id.clone())
                }
                SprintMembershipChange::CarriedOut { to } => {
                    let expected = match to {
                        None => SprintCloseDisposition::Backlog,
                        Some(sprint_id) => {
                            sprint(snapshot, &s.project_id, sprint_id)?;
                            SprintCloseDisposition::Sprint {
                                sprint_id: sprint_id.clone(),
                            }
                        }
                    };
                    historical.contains(&e.work_id)
                        && s.closure.as_ref().is_some_and(|c| {
                            c.closed_seq == e.seq
                                && c.members
                                    .get(&e.work_id)
                                    .is_some_and(|m| m.disposition == expected)
                        })
                }
            };
            if !valid {
                return Err(invalid("invalid Sprint membership history"));
            }
        }
        if historical != s.members {
            return Err(invalid("Sprint members differ from history"));
        }
        match (&s.closure, s.status) {
            (None, SprintStatus::Planned | SprintStatus::Active) => {}
            (Some(c), SprintStatus::Closed) => {
                if c.source_commit.len() != 64
                    || !c
                        .source_commit
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    || c.source_seq.checked_add(1) != Some(c.closed_seq)
                    || c.closed_seq != s.updated_seq
                    || c.members.keys().cloned().collect::<BTreeSet<_>>() != s.members
                    || counts(c.members.values().map(|m| &m.version)) != c.counts
                {
                    return Err(invalid("invalid frozen Sprint closure"));
                }
                for (work_id, m) in &c.members {
                    let valid = match &m.disposition {
                        SprintCloseDisposition::Accepted => {
                            m.version.accepted
                                && m.version.state == WorkState::Accepted
                                && m.version.accepted_submission.is_some()
                                && m.version.accepted_submission == m.version.current_submission
                        }
                        SprintCloseDisposition::Cancelled => {
                            m.version.state == WorkState::Cancelled && !m.version.accepted
                        }
                        SprintCloseDisposition::Backlog | SprintCloseDisposition::Sprint { .. } => {
                            !m.version.accepted
                                && m.version.state != WorkState::Cancelled
                                && s.history.iter().any(|e| {
                                    e.work_id == *work_id
                                        && e.seq == c.closed_seq
                                        && matches!(
                                            e.change,
                                            SprintMembershipChange::CarriedOut { .. }
                                        )
                                })
                        }
                    };
                    if !valid {
                        return Err(invalid("invalid Sprint closing disposition"));
                    }
                }
            }
            _ => return Err(invalid("Sprint status and closure mismatch")),
        }
    }
    Ok(())
}
