use crate::{service::validate_authority, *};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const WORK_CONTEXT_BYTES_LIMIT: usize = 65536;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub commit: String,
    pub seq: u64,
    pub authority_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadEnvelope<T> {
    pub snapshot: SnapshotRef,
    pub index_seq: u64,
    pub coverage: String,
    pub truncated: bool,
    pub omitted_count: usize,
    pub next_cursor: Option<ReadCursor>,
    pub data: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadCursor {
    pub commit: String,
    pub query_hash: String,
    pub offset: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SearchFilter {
    pub query: String,
    pub kind: Option<TicketKind>,
    pub state: Option<WorkState>,
    pub updated_after: Option<u64>,
    pub updated_before: Option<u64>,
    pub include_archived: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TicketSummary {
    pub id: String,
    pub kind: TicketKind,
    pub title: String,
    pub state: WorkState,
    pub record_revision: u64,
    pub contract_revision: u64,
    pub generation: u64,
    pub updated_seq: u64,
    pub blocked: Option<BlockReason>,
}

impl From<&Ticket> for TicketSummary {
    fn from(ticket: &Ticket) -> Self {
        Self {
            id: ticket.id.clone(),
            kind: ticket.kind,
            title: ticket.contract.title.clone(),
            state: ticket.state,
            record_revision: ticket.record_revision,
            contract_revision: ticket.contract_revision,
            generation: ticket.generation,
            updated_seq: ticket.updated_seq,
            blocked: ticket.blocked.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Overview {
    pub ticket_count: usize,
    pub work_count: usize,
    pub states: BTreeMap<String, usize>,
    pub open_questions: usize,
    pub open_approvals: usize,
    pub needs_acceptance: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TicketView {
    pub ticket: Ticket,
    pub assignments: Vec<Assignment>,
    pub requests: Vec<PendingRequest>,
    pub submissions: Vec<Submission>,
    #[serde(default = "all_inspect_sections")]
    pub sections: BTreeSet<InspectSection>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectSection {
    Assignments,
    Requests,
    Submissions,
}

pub fn all_inspect_sections() -> BTreeSet<InspectSection> {
    BTreeSet::from([
        InspectSection::Assignments,
        InspectSection::Requests,
        InspectSection::Submissions,
    ])
}

pub struct InspectOptions {
    pub sections: BTreeSet<InspectSection>,
    pub depth: usize,
    pub budget_bytes: usize,
    pub fixed_commit: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextArtifact {
    pub source: DependencyInput,
    pub artifact: Artifact,
    pub utf8: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnsweredInput {
    pub request_id: String,
    pub work_id: String,
    pub assignment_id: Option<String>,
    pub generation: u64,
    pub contract_revision: u64,
    pub prompt_revision: u64,
    pub prompt: String,
    pub answer: String,
    pub updated_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkContextPacket {
    pub contract_ref: String,
    pub contract_revision: u64,
    pub generation: u64,
    pub assignment_id: String,
    pub binding: ScopeBinding,
    pub contract: Contract,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<ExecutionWorkspace>,
    pub inputs: Vec<DependencyInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_artifacts: Vec<ContextArtifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub answers: Vec<AnsweredInput>,
    pub result_contract: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkChange {
    pub seq: u64,
    pub entity: String,
    pub id: String,
}

fn visible(authority: &Authority, snapshot: &Snapshot) -> Result<BTreeSet<String>> {
    validate_authority(authority, snapshot)?;
    if let Principal::Worker { assignment_id, .. } = &authority.principal {
        let assignment = &snapshot.assignments[assignment_id];
        Ok(std::iter::once(assignment.work_id.clone())
            .chain(
                assignment
                    .dependency_inputs
                    .iter()
                    .map(|i| i.work_id.clone()),
            )
            .collect())
    } else {
        Ok(snapshot.tickets.keys().cloned().collect())
    }
}

fn can_read_private(authority: &Authority, assignment_id: &str) -> bool {
    match &authority.principal {
        Principal::Worker {
            assignment_id: own, ..
        } => own == assignment_id,
        _ => true,
    }
}

fn can_read_work_private(authority: &Authority, snapshot: &Snapshot, work_id: &str) -> bool {
    match &authority.principal {
        Principal::Worker { assignment_id, .. } => {
            snapshot.assignments[assignment_id].work_id == work_id
        }
        _ => true,
    }
}

fn envelope<T>(
    commit: String,
    snapshot: &Snapshot,
    authority: &Authority,
    data: T,
    omitted: usize,
    cursor: Option<ReadCursor>,
) -> ReadEnvelope<T> {
    ReadEnvelope {
        snapshot: SnapshotRef {
            commit,
            seq: snapshot.seq,
            authority_epoch: snapshot.authority_epoch,
        },
        index_seq: snapshot.seq,
        coverage: if matches!(authority.principal, Principal::Worker { .. }) {
            "capability_refs".into()
        } else {
            "authoritative_scope".into()
        },
        truncated: omitted > 0,
        omitted_count: omitted,
        next_cursor: cursor,
        data,
    }
}

impl TicketService {
    pub fn work_overview(&self, authority: &Authority) -> Result<ReadEnvelope<Overview>> {
        let (commit, snapshot) = self.published()?;
        let refs = visible(authority, &snapshot)?;
        let mut states = BTreeMap::new();
        for ticket in snapshot.tickets.values().filter(|t| refs.contains(&t.id)) {
            let state = serde_json::to_value(ticket.state)?
                .as_str()
                .expect("state string")
                .to_string();
            *states.entry(state).or_insert(0) += 1;
        }
        let requests: Vec<_> = snapshot
            .requests
            .values()
            .filter(|r| {
                can_read_work_private(authority, &snapshot, &r.work_id)
                    && r.status == RequestStatus::Open
            })
            .collect();
        let data = Overview {
            ticket_count: refs.len(),
            work_count: snapshot
                .tickets
                .values()
                .filter(|t| refs.contains(&t.id) && t.kind == TicketKind::Work)
                .count(),
            states,
            open_questions: requests
                .iter()
                .filter(|r| r.kind == RequestKind::Question)
                .count(),
            open_approvals: requests
                .iter()
                .filter(|r| matches!(r.kind, RequestKind::Approval { .. }))
                .count(),
            needs_acceptance: snapshot
                .tickets
                .values()
                .filter(|t| refs.contains(&t.id) && t.state == WorkState::Submitted)
                .count(),
        };
        Ok(envelope(commit, &snapshot, authority, data, 0, None))
    }

    pub fn work_search(
        &self,
        authority: &Authority,
        filter: &SearchFilter,
        limit: usize,
        cursor: Option<&ReadCursor>,
        fixed_commit: Option<&str>,
    ) -> Result<ReadEnvelope<Vec<TicketSummary>>> {
        if !(1..=100).contains(&limit) || filter.query.len() > 512 {
            return Err(Error::InvalidTransition(
                "search limit 1..100, query <=512 bytes".into(),
            ));
        }
        let query_hash = content_hash(&canonical_bytes(filter)?);
        let (commit, snapshot) =
            self.query_snapshot(authority, &query_hash, cursor, fixed_commit)?;
        let refs = visible(authority, &snapshot)?;
        let query = filter.query.to_lowercase();
        let rows: Vec<_> = snapshot
            .tickets
            .values()
            .filter(|t| {
                refs.contains(&t.id)
                    && (filter.include_archived || !t.archived)
                    && filter.kind.is_none_or(|k| t.kind == k)
                    && filter.state.is_none_or(|s| t.state == s)
                    && filter.updated_after.is_none_or(|s| t.updated_seq > s)
                    && filter.updated_before.is_none_or(|s| t.updated_seq < s)
                    && (t.id.contains(&query)
                        || t.contract.title.to_lowercase().contains(&query)
                        || t.contract.objective.to_lowercase().contains(&query))
            })
            .map(TicketSummary::from)
            .collect();
        page(
            commit, &snapshot, authority, &rows, limit, cursor, query_hash,
        )
    }

    fn query_snapshot(
        &self,
        authority: &Authority,
        query_hash: &str,
        cursor: Option<&ReadCursor>,
        fixed_commit: Option<&str>,
    ) -> Result<(String, Snapshot)> {
        validate_authority(authority, &self.published()?.1)?;
        if let Some(cursor) = cursor {
            if cursor.query_hash != query_hash || fixed_commit.is_some_and(|h| h != cursor.commit) {
                return Err(Error::ResyncRequired);
            }
            Ok((
                cursor.commit.clone(),
                self.fixed_snapshot(&cursor.commit)
                    .map_err(|_| Error::ResyncRequired)?,
            ))
        } else if let Some(commit) = fixed_commit {
            Ok((
                commit.to_string(),
                self.fixed_snapshot(commit)
                    .map_err(|_| Error::ResyncRequired)?,
            ))
        } else {
            self.published()
        }
    }

    pub fn work_inspect(
        &self,
        authority: &Authority,
        ids: &[String],
        depth: usize,
        budget_bytes: usize,
        fixed_commit: Option<&str>,
    ) -> Result<ReadEnvelope<Vec<TicketView>>> {
        self.work_inspect_sections(
            authority,
            ids,
            &InspectOptions {
                sections: all_inspect_sections(),
                depth,
                budget_bytes,
                fixed_commit: fixed_commit.map(str::to_owned),
            },
        )
    }

    /// Ticket/contract is always present. The response declares selected
    /// sections so an omitted request/attempt history cannot look empty.
    pub fn work_inspect_sections(
        &self,
        authority: &Authority,
        ids: &[String],
        options: &InspectOptions,
    ) -> Result<ReadEnvelope<Vec<TicketView>>> {
        let (depth, budget_bytes, fixed_commit) = (
            options.depth,
            options.budget_bytes,
            options.fixed_commit.as_deref(),
        );
        if ids.len() > 32 || depth > 4 || !(128..=65536).contains(&budget_bytes) {
            return Err(Error::InvalidTransition(
                "inspect <=32 IDs, depth<=4, budget 128..65536".into(),
            ));
        }
        let (commit, snapshot) = self.query_snapshot(authority, "inspect", None, fixed_commit)?;
        let refs = visible(authority, &snapshot)?;
        let mut selected: BTreeSet<_> = ids.iter().cloned().collect();
        for _ in 0..depth {
            let children: Vec<_> = snapshot
                .tickets
                .values()
                .filter(|t| t.parent.as_ref().is_some_and(|p| selected.contains(p)))
                .map(|t| t.id.clone())
                .collect();
            selected.extend(children);
        }
        if !selected.is_subset(&refs) {
            return Err(Error::ScopeDenied("inspect outside capability refs".into()));
        }
        let mut data = Vec::new();
        for id in selected {
            let mut ticket = snapshot
                .tickets
                .get(&id)
                .ok_or_else(|| Error::InvalidTransition("inspect ID absent at snapshot".into()))?
                .clone();
            let assignments = snapshot
                .assignments
                .values()
                .filter(|a| {
                    options.sections.contains(&InspectSection::Assignments)
                        && a.work_id == id
                        && can_read_private(authority, &a.id)
                })
                .cloned()
                .collect();
            let requests = snapshot
                .requests
                .values()
                .filter(|r| {
                    options.sections.contains(&InspectSection::Requests)
                        && r.work_id == id
                        && can_read_work_private(authority, &snapshot, &id)
                })
                .cloned()
                .collect();
            let submissions = snapshot
                .submissions
                .values()
                .filter(|s| {
                    options.sections.contains(&InspectSection::Submissions)
                        && s.work_id == id
                        && (can_read_private(authority, &s.assignment_id)
                            || ticket.accepted_submission.as_deref() == Some(&s.id))
                })
                .cloned()
                .collect();
            if !can_read_work_private(authority, &snapshot, &id) {
                ticket.active_assignment = None;
                ticket.current_submission = ticket.accepted_submission.clone();
                ticket.import_source = None;
            }
            data.push(TicketView {
                ticket,
                assignments,
                requests,
                submissions,
                sections: options.sections.clone(),
            });
        }
        if canonical_bytes(&data)?.len() > budget_bytes {
            return Err(Error::ContextBudgetExceeded);
        }
        Ok(envelope(commit, &snapshot, authority, data, 0, None))
    }

    /// Derive changes from fixed immutable commits, never a second authoritative stream.
    pub fn work_changes(
        &self,
        authority: &Authority,
        since_seq: u64,
        limit: usize,
        cursor: Option<&ReadCursor>,
    ) -> Result<ReadEnvelope<Vec<WorkChange>>> {
        if !(1..=100).contains(&limit) {
            return Err(Error::InvalidTransition("changes limit 1..100".into()));
        }
        let hash = content_hash(&canonical_bytes(
            &serde_json::json!({"changes_since":since_seq}),
        )?);
        let (commit, snapshot) = self.query_snapshot(authority, &hash, cursor, None)?;
        if since_seq > snapshot.seq {
            return Err(Error::ResyncRequired);
        }
        let refs = visible(authority, &snapshot)?;
        let mut next = Some(commit.clone());
        let mut changes = Vec::new();
        let mut scanned = 0;
        while let Some(current) = next {
            let state = self
                .fixed_snapshot(&current)
                .map_err(|_| Error::ResyncRequired)?;
            if state.seq <= since_seq {
                break;
            }
            scanned += 1;
            if scanned > 256 {
                return Err(Error::ResyncRequired);
            }
            for ticket in state
                .tickets
                .values()
                .filter(|t| refs.contains(&t.id) && t.updated_seq == state.seq)
            {
                changes.push(WorkChange {
                    seq: state.seq,
                    entity: "ticket".into(),
                    id: ticket.id.clone(),
                });
            }
            for a in state.assignments.values().filter(|a| {
                refs.contains(&a.work_id)
                    && can_read_private(authority, &a.id)
                    && a.updated_seq == state.seq
            }) {
                changes.push(WorkChange {
                    seq: state.seq,
                    entity: "assignment".into(),
                    id: a.id.clone(),
                });
            }
            for r in state.requests.values().filter(|r| {
                can_read_work_private(authority, &snapshot, &r.work_id)
                    && r.updated_seq == state.seq
            }) {
                changes.push(WorkChange {
                    seq: state.seq,
                    entity: "request".into(),
                    id: r.id.clone(),
                });
            }
            for s in state.submissions.values().filter(|s| {
                refs.contains(&s.work_id)
                    && can_read_private(authority, &s.assignment_id)
                    && s.updated_seq == state.seq
            }) {
                changes.push(WorkChange {
                    seq: state.seq,
                    entity: "submission".into(),
                    id: s.id.clone(),
                });
            }
            next = self
                .snapshot_parent(&current)
                .map_err(|_| Error::ResyncRequired)?;
        }
        changes.sort_by(|a, b| (&a.seq, &a.entity, &a.id).cmp(&(&b.seq, &b.entity, &b.id)));
        page(commit, &snapshot, authority, &changes, limit, cursor, hash)
    }

    pub fn child_context_packet(
        &self,
        authority: &Authority,
        assignment_id: &str,
        budget_bytes: usize,
    ) -> Result<WorkContextPacket> {
        let (_, snapshot) = self.published()?;
        validate_authority(authority, &snapshot)?;
        let assignment = snapshot
            .assignments
            .get(assignment_id)
            .ok_or_else(|| Error::InvalidTransition("assignment absent".into()))?;
        if !can_read_private(authority, assignment_id) {
            return Err(Error::ScopeDenied("sibling context denied".into()));
        }
        build_context_packet(&snapshot, assignment, budget_bytes, |artifact, budget| {
            self.read_artifact(authority, artifact, budget)
        })
    }

    pub fn work_update(
        &self,
        authority: &Authority,
        command: &Command,
    ) -> Result<OperationReceipt> {
        self.execute(authority, command)
    }

    /// Committed intent/assignment receipt, never a Worker completion claim.
    pub fn work_dispatch(
        &self,
        authority: &Authority,
        command: &Command,
    ) -> Result<OperationReceipt> {
        if command.operations.iter().any(|op| {
            !matches!(
                op,
                Operation::Start { .. }
                    | Operation::UpdateContract { .. }
                    | Operation::Cancel { .. }
                    | Operation::Pause { .. }
                    | Operation::Ready { .. }
                    | Operation::Reopen { .. }
            )
        }) {
            return Err(Error::InvalidTransition(
                "dispatch accepts start/steer/cancel/retry only".into(),
            ));
        }
        self.execute(authority, command)
    }
}

fn page<T: Clone>(
    commit: String,
    snapshot: &Snapshot,
    authority: &Authority,
    rows: &[T],
    limit: usize,
    cursor: Option<&ReadCursor>,
    query_hash: String,
) -> Result<ReadEnvelope<Vec<T>>> {
    let offset = cursor.map_or(0, |c| c.offset);
    if offset > rows.len() {
        return Err(Error::ResyncRequired);
    }
    let end = offset.saturating_add(limit).min(rows.len());
    let omitted = rows.len() - end;
    let next = (omitted > 0).then(|| ReadCursor {
        commit: commit.clone(),
        query_hash,
        offset: end,
    });
    Ok(envelope(
        commit,
        snapshot,
        authority,
        rows[offset..end].to_vec(),
        omitted,
        next,
    ))
}

/// The same complete packet is checked before Start publication and before
/// native admission; callers supply their already-verified artifact reader.
pub(crate) fn build_context_packet(
    snapshot: &Snapshot,
    assignment: &Assignment,
    budget_bytes: usize,
    mut read_artifact: impl FnMut(&Artifact, usize) -> Result<Vec<u8>>,
) -> Result<WorkContextPacket> {
    let assignment_id = assignment.id.as_str();
    let work = &snapshot.tickets[&assignment.work_id];
    if work.contract_revision != assignment.contract_revision
        || work.generation != assignment.generation
        || work.state != WorkState::Active
        || work.active_assignment.as_deref() != Some(assignment_id)
        || assignment.dependency_inputs.iter().any(|i| {
            snapshot.tickets.get(&i.work_id).is_none_or(|upstream| {
                upstream.state != WorkState::Accepted
                    || upstream.accepted_submission.as_deref() != Some(i.submission_id.as_str())
                    || upstream.contract_revision != i.contract_revision
            })
        })
    {
        return Err(Error::RevisionConflict);
    }
    if !(128..=WORK_CONTEXT_BYTES_LIMIT).contains(&budget_bytes) {
        return Err(Error::ContextBudgetExceeded);
    }
    let mut input_artifacts = Vec::new();
    let mut input_bytes = 0usize;
    for input in &assignment.dependency_inputs {
        let submission = snapshot
            .submissions
            .get(&input.submission_id)
            .ok_or(Error::RevisionConflict)?;
        if submission.work_id != input.work_id
            || submission.contract_revision != input.contract_revision
            || submission
                .artifacts
                .iter()
                .map(|a| a.sha256.clone())
                .collect::<Vec<_>>()
                != input.artifact_hashes
        {
            return Err(Error::RevisionConflict);
        }
        for artifact in &submission.artifacts {
            let bytes = read_artifact(artifact, budget_bytes)?;
            input_bytes = input_bytes.saturating_add(bytes.len());
            if input_bytes > budget_bytes {
                return Err(Error::ContextBudgetExceeded);
            }
            let utf8 = String::from_utf8(bytes).map_err(|_| {
                Error::AuthorityUnavailable(
                    "input Artifact requires a supported lossless UTF-8 resolver".into(),
                )
            })?;
            input_artifacts.push(ContextArtifact {
                source: input.clone(),
                artifact: artifact.clone(),
                utf8,
            });
        }
    }
    let packet=WorkContextPacket { contract_ref:work.id.clone(),contract_revision:work.contract_revision,generation:assignment.generation,
        assignment_id:assignment_id.into(),binding:snapshot.binding.clone(),contract:work.contract.clone(),inputs:assignment.dependency_inputs.clone(),
        input_artifacts, workspace:assignment.workspace.clone(),
        answers:snapshot.requests.values().filter(|r| r.work_id == work.id && r.contract_revision == assignment.contract_revision
            && r.generation < assignment.generation && r.kind == RequestKind::Question && r.status == RequestStatus::Answered)
            .map(|r|AnsweredInput { request_id:r.id.clone(),work_id:r.work_id.clone(),assignment_id:r.assignment_id.clone(),generation:r.generation,
                contract_revision:r.contract_revision,prompt_revision:r.prompt_revision,prompt:r.prompt.clone(),answer:r.answer.clone().expect("answered question"),updated_seq:r.updated_seq }).collect(),
        result_contract:"Submit exact assignment/generation/contract/input versions, artifact hashes and evidence. Completion means submitted.".into() };
    if canonical_bytes(&packet)?.len() > budget_bytes {
        return Err(Error::ContextBudgetExceeded);
    }
    Ok(packet)
}
