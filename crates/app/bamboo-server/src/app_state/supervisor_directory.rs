//! Host-installed authoritative session directory for the Supervisor.
//!
//! Merges two authoritative sources under the caller's verified Supervisor
//! authority: (1) `SupervisorManagementState.links` — roots this Supervisor
//! created, each entry `origin=supervisor_created`; (2) the global session
//! index filtered to `allowed_projects` — sessions the user opened, each entry
//! `origin=user_opened`. Entries carry the persisted progress mirror and the
//! canonical Human UI engagement mark. Observation only: no entry grants
//! followup/loop/attach authority, and no transcript content is exposed.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolCtx, ToolError};
use bamboo_domain::{Storage, SupervisorManagementState, MAX_SUPERVISOR_LINKS};
use bamboo_server_tools::session_control::directory::{
    SessionDirectoryEntry, SessionDirectoryOrigin, SupervisorDirectoryPage,
    SupervisorDirectoryPort, SupervisorDirectoryRequest, MAX_DIRECTORY_PAGE_SIZE,
};
use bamboo_storage::SessionStoreV2;
use serde::{Deserialize, Serialize};

pub struct SupervisorSessionDirectory {
    storage: Arc<dyn Storage>,
    session_store: Arc<SessionStoreV2>,
}

impl SupervisorSessionDirectory {
    pub fn new(storage: Arc<dyn Storage>, session_store: Arc<SessionStoreV2>) -> Self {
        Self {
            storage,
            session_store,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryCursor {
    version: u8,
    incarnation: uuid::Uuid,
    scope_revision: u64,
    query: Option<String>,
    limit: usize,
    after: String,
}

/// Caller authority snapshot: same contract as session_inspector's
/// SupervisorSessionDiscovery, kept local because that helper is private.
struct CallerAuthority {
    session_id: String,
    reference: bamboo_domain::SupervisorReference,
    scope_revision: u64,
    state: Option<SupervisorManagementState>,
}

impl SupervisorSessionDirectory {
    async fn caller_authority(&self, ctx: &ToolCtx) -> Result<CallerAuthority, ToolError> {
        let id = ctx.session_id().ok_or_else(|| {
            ToolError::Execution(
                "session_directory_authority_required: original authorized caller identity is required"
                    .into(),
            )
        })?;
        let observation = ctx
            .executing_supervisor_for(id)
            .ok_or_else(|| {
                ToolError::Execution(
                    "session_directory_authority_required: original authorized caller identity is required"
                        .into(),
                )
            })?;
        let reference = observation.supervisor_reference();
        let session = self
            .storage
            .load_root_authority(id)
            .await
            .map_err(|e| ToolError::Execution(format!("session_directory_authority_unavailable: {e}")))?
            .ok_or_else(|| {
                ToolError::Execution(
                    "session_directory_authority_required: original authorized caller identity is required"
                        .into(),
                )
            })?;
        if bamboo_agent_core::tools::ExecutingSupervisorObservation::capture_from_executing_session(
            &session,
        ) != Some(observation)
        {
            return Err(ToolError::Execution(
                "session_directory_authority_required: original authorized caller identity is required"
                    .into(),
            ));
        }
        let state = session.supervisor_management.clone();
        if let Some(state) = &state {
            state.validate(reference.incarnation_id).map_err(|e| {
                ToolError::Execution(format!("session_directory_authority_unavailable: {e}"))
            })?;
        }
        Ok(CallerAuthority {
            session_id: reference.session_id.clone(),
            reference,
            scope_revision: state.as_ref().map_or(0, |state| state.revision),
            state,
        })
    }

    async fn collect_entries(
        &self,
        caller: &CallerAuthority,
        query: Option<&str>,
    ) -> Result<Vec<SessionDirectoryEntry>, ToolError> {
        let mut entries: Vec<SessionDirectoryEntry> = Vec::new();
        let state = caller.state.as_ref();
        let allowed_projects = state.map_or_else(std::collections::BTreeSet::new, |s| {
            s.allowed_projects.clone()
        });

        // 1) Supervisor-created roots: authoritative management links. Each
        // link is revalidated against the current target: a moved Project, a
        // changed metadata version or a different lifetime means the stale
        // link no longer authorizes exposing the target's current metadata.
        if let Some(state) = state {
            if state.links.len() > MAX_SUPERVISOR_LINKS {
                return Err(ToolError::Execution(
                    "session_directory_authority_unavailable: management link capacity exceeded"
                        .into(),
                ));
            }
            for (id, link) in &state.links {
                if !allowed_projects.contains(&link.target_project_id) {
                    continue;
                }
                let Some(index) = self.session_store.get_index_entry(id).await else {
                    // A link without an index entry still belongs in the
                    // directory; report it with link-carried metadata only.
                    entries.push(SessionDirectoryEntry {
                        session_id: id.clone(),
                        origin: SessionDirectoryOrigin::SupervisorCreated,
                        linked: link.enabled,
                        project_id: link.target_project_id.to_string(),
                        title: String::new(),
                        created_at: link.target_created_at,
                        last_activity_at: link.target_created_at,
                        last_run_status: "unknown".into(),
                        has_pending_question: false,
                        last_human_input: None,
                    });
                    continue;
                };
                // The link's Project must still match the target's CURRENT
                // Project. A target PATCH-moved to another Project keeps its
                // historical link; that stale link no longer authorizes
                // exposing the moved target's current metadata. The created_at
                // lifetime check fences rebirth under the same id.
                let current_project_matches = index.project_id.as_deref()
                    == Some(link.target_project_id.as_str())
                    && index.created_at == link.target_created_at;
                if !current_project_matches {
                    continue;
                }
                if let Some(query) = query {
                    let id_match = id.to_lowercase().contains(query);
                    let project_match = link
                        .target_project_id
                        .to_string()
                        .to_lowercase()
                        .contains(query);
                    let title_match = index.title.to_lowercase().contains(query);
                    if !id_match && !project_match && !title_match {
                        continue;
                    }
                }
                entries.push(entry_from_index(
                    index,
                    SessionDirectoryOrigin::SupervisorCreated,
                    link.enabled,
                ));
            }
        }

        // 2) User-opened sessions: authorized-Project top-level roots that are
        // neither the caller nor any management link.
        let linked: std::collections::BTreeSet<&String> = state
            .map(|state| state.links.keys().collect())
            .unwrap_or_default();
        for index in self.session_store.list_index_entries().await {
            let Some(project) = index.project_id.as_deref() else {
                continue;
            };
            if !allowed_projects.iter().any(|p| p.as_str() == project) {
                continue;
            }
            if index.parent_session_id.is_some() || index.id == caller.session_id {
                continue;
            }
            if linked.contains(&index.id) {
                continue;
            }
            if let Some(query) = query {
                let id_match = index.id.to_lowercase().contains(query);
                let project_match = project.to_lowercase().contains(query);
                let title_match = index.title.to_lowercase().contains(query);
                if !id_match && !project_match && !title_match {
                    continue;
                }
            }
            entries.push(entry_from_index(
                index,
                SessionDirectoryOrigin::UserOpened,
                false,
            ));
        }
        entries.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        entries.dedup_by(|a, b| a.session_id == b.session_id);
        Ok(entries)
    }
}

fn entry_from_index(
    index: bamboo_storage::SessionIndexEntry,
    origin: SessionDirectoryOrigin,
    linked: bool,
) -> SessionDirectoryEntry {
    SessionDirectoryEntry {
        session_id: index.id,
        origin,
        linked,
        project_id: index.project_id.unwrap_or_default(),
        title: index.title.chars().take(256).collect(),
        created_at: index.created_at,
        last_activity_at: index.last_activity_at,
        last_run_status: index.last_run_status.unwrap_or_else(|| "unknown".into()),
        has_pending_question: index.has_pending_question,
        last_human_input: index.last_human_input,
    }
}

#[async_trait]
impl SupervisorDirectoryPort for SupervisorSessionDirectory {
    async fn directory(
        &self,
        ctx: &ToolCtx,
        request: SupervisorDirectoryRequest,
    ) -> Result<SupervisorDirectoryPage, ToolError> {
        request.validate()?;
        let caller = self.caller_authority(ctx).await?;
        let query = request
            .query
            .as_deref()
            .map(str::trim)
            .filter(|q| !q.is_empty())
            .map(str::to_lowercase);
        let limit = request.limit.unwrap_or(MAX_DIRECTORY_PAGE_SIZE);
        let after = request
            .cursor
            .as_ref()
            .map(|value| {
                let cursor: DirectoryCursor = serde_json::from_str(value).map_err(|_| {
                    ToolError::InvalidArguments(
                        "session_directory_invalid_request: malformed directory cursor".into(),
                    )
                })?;
                if cursor.version != 1
                    || cursor.incarnation != caller.reference.incarnation_id
                    || cursor.scope_revision != caller.scope_revision
                    || cursor.query != query
                    || cursor.limit != limit
                {
                    return Err(ToolError::Execution(
                        "session_directory_cursor_stale: caller scope or query changed; start a new page"
                            .into(),
                    ));
                }
                bamboo_domain::validate_supervisor_target_id(&cursor.after).map_err(|_| {
                    ToolError::InvalidArguments(
                        "session_directory_invalid_request: malformed cursor target".into(),
                    )
                })?;
                Ok(cursor.after)
            })
            .transpose()?;

        let mut entries = self.collect_entries(&caller, query.as_deref()).await?;
        if let Some(after) = after.as_ref() {
            if !entries.iter().any(|entry| &entry.session_id == after) {
                return Err(ToolError::Execution(
                    "session_directory_cursor_stale: cursor target is no longer in the current scope"
                        .into(),
                ));
            }
        }
        let mut page: Vec<_> = entries
            .iter()
            .filter(|entry| after.as_ref().is_none_or(|after| entry.session_id > *after))
            .take(limit + 1)
            .cloned()
            .collect();
        let has_more = page.len() > limit;
        page.truncate(limit);
        let next_cursor = if has_more {
            let value = serde_json::to_string(&DirectoryCursor {
                version: 1,
                incarnation: caller.reference.incarnation_id,
                scope_revision: caller.scope_revision,
                query,
                limit,
                after: page
                    .last()
                    .expect("nonempty bounded page")
                    .session_id
                    .clone(),
            })
            .map_err(|e| {
                ToolError::Execution(format!("session_directory_authority_unavailable: {e}"))
            })?;
            Some(value)
        } else {
            None
        };
        entries.clear();
        Ok(SupervisorDirectoryPage {
            caller_session_id: caller.session_id,
            scope_revision: caller.scope_revision,
            entries: page,
            next_cursor,
        })
    }
}
