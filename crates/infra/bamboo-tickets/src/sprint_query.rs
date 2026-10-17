//! Bounded Sprint discovery and membership filters for the existing Project board.
use std::collections::BTreeSet;

use bamboo_domain::ProjectId;
use serde::{Deserialize, Serialize};

use crate::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SprintBoardFilter {
    /// Work outside every planned/active Sprint. Closed history does not claim Work.
    Backlog {},
    /// Closed Sprints keep their closing member IDs; Work facts use the read snapshot.
    Sprint { sprint_id: SprintId },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SprintListRequest {
    pub project_id: ProjectId,
    pub status: Option<SprintStatus>,
    pub limit: usize,
    pub cursor: Option<ReadCursor>,
    pub fixed_commit: Option<String>,
}

/// Discovery omits member/history payloads; inspect supplies bounded details.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SprintSummary {
    pub id: SprintId,
    pub project_id: ProjectId,
    pub definition: SprintDefinition,
    pub status: SprintStatus,
    pub revision: u64,
    pub created_seq: u64,
    pub updated_seq: u64,
    pub member_count: usize,
    /// Immutable closing counts, never a claim about current Work completion.
    pub closure_counts: Option<SprintCounts>,
}

impl From<&Sprint> for SprintSummary {
    fn from(sprint: &Sprint) -> Self {
        Self {
            id: sprint.id.clone(),
            project_id: sprint.project_id.clone(),
            definition: sprint.definition.clone(),
            status: sprint.status,
            revision: sprint.revision,
            created_seq: sprint.created_seq,
            updated_seq: sprint.updated_seq,
            member_count: sprint.members.len(),
            closure_counts: sprint.closure.as_ref().map(|c| c.counts.clone()),
        }
    }
}

impl TicketService {
    pub fn list_sprints(
        &self,
        authority: &Authority,
        request: &SprintListRequest,
    ) -> Result<ReadEnvelope<Vec<SprintSummary>>> {
        authority.supervisor()?;
        if !(1..=100).contains(&request.limit) {
            return Err(Error::InvalidTransition("Sprint list limit 1..100".into()));
        }
        let hash = content_hash(&canonical_bytes(&(
            "sprint-list-v1",
            &request.project_id,
            request.status,
        ))?);
        let (commit, snapshot) = self.query_snapshot(
            authority,
            &hash,
            request.cursor.as_ref(),
            request.fixed_commit.as_deref(),
        )?;
        crate::service::validate_authority(authority, &snapshot)?;
        let rows: Vec<_> = snapshot
            .sprint
            .sprints
            .values()
            .filter(|s| {
                s.project_id == request.project_id
                    && request.status.is_none_or(|status| s.status == status)
            })
            .collect();
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
            data: page.data.into_iter().map(SprintSummary::from).collect(),
        })
    }
}

pub(crate) fn board_query_hash(request: &ProjectBoardRequest) -> Result<String> {
    let bytes = match &request.sprint_filter {
        // Keep existing board cursor identities when clients omit the additive filter.
        None => canonical_bytes(&(
            "project-board-v1",
            &request.project_id,
            request.include_cancelled,
            request.include_archived,
        ))?,
        Some(filter) => canonical_bytes(&(
            "project-board-sprint-v1",
            &request.project_id,
            request.include_cancelled,
            request.include_archived,
            filter,
        ))?,
    };
    Ok(content_hash(&bytes))
}

pub(crate) fn board_members(
    snapshot: &Snapshot,
    project_id: &ProjectId,
    filter: Option<&SprintBoardFilter>,
) -> Result<Option<BTreeSet<String>>> {
    match filter {
        None => Ok(None),
        Some(SprintBoardFilter::Sprint { sprint_id }) => {
            let sprint = snapshot
                .sprint
                .sprints
                .get(sprint_id)
                .ok_or_else(|| Error::InvalidTransition("Sprint not found".into()))?;
            if &sprint.project_id != project_id {
                return Err(Error::ScopeDenied("Sprint Project mismatch".into()));
            }
            Ok(Some(sprint.members.clone()))
        }
        Some(SprintBoardFilter::Backlog {}) => {
            let planned: BTreeSet<_> = snapshot
                .sprint
                .sprints
                .values()
                .filter(|s| &s.project_id == project_id && s.status != SprintStatus::Closed)
                .flat_map(|s| s.members.iter())
                .collect();
            Ok(Some(
                snapshot
                    .project
                    .links
                    .iter()
                    .filter(|(id, project)| *project == project_id && !planned.contains(id))
                    .map(|(id, _)| id.clone())
                    .collect(),
            ))
        }
    }
}
