//! Explicit scope-local Project membership and bounded, immutable execution context.
//! These references grant no filesystem, tool, or network authority.
use std::collections::BTreeMap;

use bamboo_domain::{ProjectId, WorkspaceBinding};
use serde::{Deserialize, Serialize};

use crate::*;

pub const PROJECT_CONTEXT_BYTES_LIMIT: usize = 16 * 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProjectTicketState {
    pub links: BTreeMap<String, ProjectId>,
    pub contexts: BTreeMap<String, ProjectContextSnapshot>,
    pub work_contexts: BTreeMap<String, String>,
    /// Submission provenance follows its immutable assignment_id into this map.
    pub assignment_contexts: BTreeMap<String, String>,
}
impl ProjectTicketState {
    pub fn is_empty(&self) -> bool {
        self.links.is_empty()
            && self.contexts.is_empty()
            && self.work_contexts.is_empty()
            && self.assignment_contexts.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectContextSnapshot {
    pub project_id: ProjectId,
    pub project_revision: u64,
    pub resource_revision: u64,
    /// One selected registered binding. This is metadata, never a grant.
    pub workspace_binding: WorkspaceBinding,
    pub contract_revision: u64,
    pub contract: Contract,
    /// Bounded source identity -> immutable version/hash, not copied history.
    pub sources: BTreeMap<String, String>,
}

fn invalid(message: &str) -> Error {
    Error::InvalidTransition(message.into())
}

pub(crate) fn associate(snapshot: &mut Snapshot, id: &str, project_id: &ProjectId) -> Result<()> {
    let ticket = snapshot
        .tickets
        .get(id)
        .ok_or_else(|| invalid("ticket not found"))?;
    if let Some(existing) = snapshot.project.links.get(id) {
        return if existing == project_id {
            Ok(())
        } else {
            Err(invalid("Project membership is immutable"))
        };
    }
    // Never retrofit historical executions or implicitly move a subtree.
    if ticket.generation != 0
        || ticket.active_assignment.is_some()
        || !matches!(ticket.state, WorkState::Draft | WorkState::Ready)
        || snapshot
            .tickets
            .values()
            .any(|child| child.parent.as_deref() == Some(id))
    {
        return Err(invalid("associate a new or unexecuted leaf Ticket only"));
    }
    snapshot.project.links.insert(id.into(), project_id.clone());
    snapshot.schema = snapshot.schema.max(5);
    let ticket = snapshot.tickets.get_mut(id).expect("checked");
    ticket.record_revision += 1;
    ticket.updated_seq = snapshot.seq;
    Ok(())
}

pub(crate) fn set_context(
    snapshot: &mut Snapshot,
    id: &str,
    context: &ProjectContextSnapshot,
) -> Result<()> {
    let work = snapshot
        .tickets
        .get(id)
        .ok_or_else(|| invalid("work not found"))?;
    if work.kind != TicketKind::Work
        || work.active_assignment.is_some()
        || !matches!(work.state, WorkState::Draft | WorkState::Ready)
    {
        return Err(invalid(
            "context adoption requires stopped Draft/Ready Work",
        ));
    }
    if snapshot.project.links.get(id) != Some(&context.project_id)
        || work.contract_revision != context.contract_revision
        || work.contract != context.contract
    {
        return Err(invalid("context Project or contract mismatch"));
    }
    validate_context(context)?;
    let hash = content_hash(&canonical_bytes(context)?);
    snapshot
        .project
        .contexts
        .insert(hash.clone(), context.clone());
    snapshot.project.work_contexts.insert(id.into(), hash);
    snapshot.schema = snapshot.schema.max(5);
    let work = snapshot.tickets.get_mut(id).expect("checked");
    work.record_revision += 1;
    work.updated_seq = snapshot.seq;
    Ok(())
}

fn validate_context(context: &ProjectContextSnapshot) -> Result<()> {
    if canonical_bytes(context)?.len() > PROJECT_CONTEXT_BYTES_LIMIT {
        return Err(Error::ContextBudgetExceeded);
    }
    if context.project_revision == 0
        || context.resource_revision == 0
        || context.contract_revision == 0
        || context.workspace_binding.path.is_empty()
        || context.sources.len() > 32
        || context
            .sources
            .iter()
            .any(|(k, v)| k.is_empty() || v.is_empty())
    {
        return Err(invalid("invalid bounded Project context"));
    }
    Ok(())
}

pub(crate) fn context_for_start(
    snapshot: &Snapshot,
    work: &Ticket,
    workspace: Option<&ExecutionWorkspace>,
) -> Result<Option<String>> {
    let Some(project_id) = snapshot.project.links.get(&work.id) else {
        return Ok(None);
    };
    let hash = snapshot
        .project
        .work_contexts
        .get(&work.id)
        .ok_or_else(|| invalid("linked Work requires explicit Project context"))?;
    let context = snapshot
        .project
        .contexts
        .get(hash)
        .ok_or_else(|| invalid("context missing"))?;
    if &context.project_id != project_id
        || context.contract_revision != work.contract_revision
        || context.contract != work.contract
    {
        return Err(invalid(
            "Project context is stale; adopt at a stopped boundary",
        ));
    }
    let workspace =
        workspace.ok_or_else(|| invalid("linked Work requires one execution workspace"))?;
    // Repository selection is fixed. Worktree grants and canonical resource claims
    // are independently checked by Start and the trusted host admission path.
    if workspace.repo != context.workspace_binding.path {
        return Err(invalid("execution repository differs from adopted binding"));
    }
    Ok(Some(hash.clone()))
}

pub(crate) fn validate(snapshot: &Snapshot) -> Result<()> {
    if !snapshot.project.is_empty() && snapshot.schema < 5 {
        return Err(invalid("Project references require schema 5"));
    }
    for (id, project) in &snapshot.project.links {
        let ticket = snapshot
            .tickets
            .get(id)
            .ok_or_else(|| invalid("dangling Project link"))?;
        if ticket
            .parent
            .as_ref()
            .is_some_and(|p| snapshot.project.links.get(p) != Some(project))
            || ticket
                .depends_on
                .iter()
                .any(|d| snapshot.project.links.get(d) != Some(project))
        {
            return Err(invalid("cross-Project or unlinked containment/dependency"));
        }
    }
    // Unlinked children/dependents cannot bridge into a linked tree either.
    for ticket in snapshot.tickets.values() {
        for other in ticket.parent.iter().chain(ticket.depends_on.iter()) {
            if snapshot.project.links.get(&ticket.id) != snapshot.project.links.get(other) {
                return Err(invalid("Project membership must match related Tickets"));
            }
        }
    }
    for (hash, context) in &snapshot.project.contexts {
        validate_context(context)?;
        if hash != &content_hash(&canonical_bytes(context)?) {
            return Err(invalid("Project context hash mismatch"));
        }
    }
    for (id, hash) in &snapshot.project.work_contexts {
        let context = snapshot
            .project
            .contexts
            .get(hash)
            .ok_or_else(|| invalid("missing Work context"))?;
        if snapshot.project.links.get(id) != Some(&context.project_id) {
            return Err(invalid("Work context Project mismatch"));
        }
    }
    for (id, hash) in &snapshot.project.assignment_contexts {
        let assignment = snapshot
            .assignments
            .get(id)
            .ok_or_else(|| invalid("missing context Assignment"))?;
        let context = snapshot
            .project
            .contexts
            .get(hash)
            .ok_or_else(|| invalid("missing adopted context"))?;
        if snapshot
            .intents
            .get(&assignment.dispatch_key)
            .and_then(|i| i.immutable_spec.project_context_hash.as_ref())
            != Some(hash)
            || snapshot.project.links.get(&assignment.work_id) != Some(&context.project_id)
            || assignment.contract_revision != context.contract_revision
            || assignment
                .workspace
                .as_ref()
                .is_none_or(|w| w.repo != context.workspace_binding.path)
        {
            return Err(invalid("adopted Assignment context mismatch"));
        }
    }
    for assignment in snapshot.assignments.values() {
        if snapshot.project.links.contains_key(&assignment.work_id)
            && !snapshot
                .project
                .assignment_contexts
                .contains_key(&assignment.id)
        {
            return Err(invalid("linked Assignment missing adopted context"));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectBoardStage {
    Todo,
    Doing,
    Testing,
    Done,
}

fn stage(ticket: &Ticket) -> Option<ProjectBoardStage> {
    let state = if ticket.state == WorkState::Blocked {
        ticket
            .blocked
            .as_ref()
            .map(|b| b.resume_state)
            .unwrap_or(WorkState::Blocked)
    } else {
        ticket.state
    };
    match state {
        WorkState::Draft | WorkState::Ready => Some(ProjectBoardStage::Todo),
        WorkState::Active => Some(ProjectBoardStage::Doing),
        WorkState::Submitted => Some(ProjectBoardStage::Testing),
        WorkState::Accepted => Some(ProjectBoardStage::Done),
        WorkState::Blocked | WorkState::Cancelled => None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectBoardRow {
    pub ticket: Ticket,
    pub stage: Option<ProjectBoardStage>,
    pub context: Option<ProjectContextSnapshot>,
    pub adopted_context: Option<ProjectContextSnapshot>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProjectBoardCounts {
    pub todo: usize,
    pub doing: usize,
    pub testing: usize,
    pub done: usize,
    pub blocked: usize,
    pub cancelled: usize,
    pub total: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectBoard {
    pub project_id: ProjectId,
    pub counts: ProjectBoardCounts,
    pub rows: Vec<ProjectBoardRow>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectBoardRequest {
    pub project_id: ProjectId,
    #[serde(default)]
    pub include_cancelled: bool,
    #[serde(default)]
    pub include_archived: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sprint_filter: Option<SprintBoardFilter>,
    pub limit: usize,
    pub cursor: Option<ReadCursor>,
    pub fixed_commit: Option<String>,
}

impl TicketService {
    pub fn project_board(
        &self,
        authority: &Authority,
        request: &ProjectBoardRequest,
    ) -> Result<ReadEnvelope<ProjectBoard>> {
        // Board rows include private current-generation context. Worker
        // dependency capabilities must keep using the redacted inspect API.
        authority.supervisor()?;
        if !(1..=100).contains(&request.limit) {
            return Err(invalid("board limit 1..100"));
        }
        let hash = crate::sprint_query::board_query_hash(request)?;
        let (commit, snapshot) = self.query_snapshot(
            authority,
            &hash,
            request.cursor.as_ref(),
            request.fixed_commit.as_deref(),
        )?;
        let visible = crate::query::visible(authority, &snapshot)?;
        let sprint_members = crate::sprint_query::board_members(
            &snapshot,
            &request.project_id,
            request.sprint_filter.as_ref(),
        )?;
        let mut counts = ProjectBoardCounts::default();
        let mut rows = Vec::new();
        for ticket in snapshot.tickets.values().filter(|t| {
            t.kind == TicketKind::Work
                && visible.contains(&t.id)
                && snapshot.project.links.get(&t.id) == Some(&request.project_id)
                && (request.include_archived || !t.archived)
                && sprint_members
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&t.id))
        }) {
            counts.total += 1;
            if ticket.state == WorkState::Cancelled {
                counts.cancelled += 1;
            }
            if ticket.state == WorkState::Blocked {
                counts.blocked += 1;
            }
            let column = stage(ticket);
            match column {
                Some(ProjectBoardStage::Todo) => counts.todo += 1,
                Some(ProjectBoardStage::Doing) => counts.doing += 1,
                Some(ProjectBoardStage::Testing) => counts.testing += 1,
                Some(ProjectBoardStage::Done) => counts.done += 1,
                None => {}
            }
            if ticket.state == WorkState::Cancelled && !request.include_cancelled {
                continue;
            }
            let adopted = ticket
                .active_assignment
                .as_ref()
                .or_else(|| {
                    ticket
                        .current_submission
                        .as_ref()
                        .and_then(|id| snapshot.submissions.get(id))
                        .map(|s| &s.assignment_id)
                })
                .or_else(|| {
                    ticket
                        .accepted_submission
                        .as_ref()
                        .and_then(|id| snapshot.submissions.get(id))
                        .map(|s| &s.assignment_id)
                });
            rows.push(ProjectBoardRow {
                ticket: ticket.clone(),
                stage: column,
                context: snapshot
                    .project
                    .work_contexts
                    .get(&ticket.id)
                    .and_then(|h| snapshot.project.contexts.get(h))
                    .cloned(),
                adopted_context: adopted
                    .and_then(|id| snapshot.project.assignment_contexts.get(id))
                    .and_then(|h| snapshot.project.contexts.get(h))
                    .cloned(),
            });
        }
        let page = crate::query::page(
            commit,
            &snapshot,
            authority,
            &rows,
            request.limit,
            request.cursor.as_ref(),
            hash,
        )?;
        Ok(ReadEnvelope {
            snapshot: page.snapshot,
            index_seq: page.index_seq,
            coverage: page.coverage,
            truncated: page.truncated,
            omitted_count: page.omitted_count,
            next_cursor: page.next_cursor,
            data: ProjectBoard {
                project_id: request.project_id.clone(),
                counts,
                rows: page.data,
            },
        })
    }
}
