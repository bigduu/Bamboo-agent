//! Bounded Session discovery over an installed Host authority adapter.
//!
//! A cursor locates a page; it never grants access. The compatibility adapter
//! below uses the existing Supervisor relationship readers. Other callers need
//! an explicitly installed adapter with equivalent final-read authorization.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{ExecutingSupervisorObservation, ToolCtx, ToolError};
use bamboo_domain::{
    ProjectId, Role, Session, Storage, SupervisorManagementState, SupervisorReference,
    TaskItemStatus, MAX_SUPERVISOR_LINKS,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const MAX_DIRECTORY_PAGE_SIZE: usize = 20;
pub const MAX_PROGRESS_EXCERPT_CHARS: usize = 8_000;
const MAX_QUERY_BYTES: usize = 128;
const MAX_CURSOR_BYTES: usize = 4_096;

#[derive(Debug, Clone, Default)]
pub struct AuthorizedSessionListRequest {
    pub query: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

impl AuthorizedSessionListRequest {
    pub fn validate(&self) -> Result<(), ToolError> {
        if self
            .limit
            .is_some_and(|limit| !(1..=MAX_DIRECTORY_PAGE_SIZE).contains(&limit))
        {
            return Err(arguments("list limit must be between 1 and 20"));
        }
        if self
            .query
            .as_ref()
            .is_some_and(|query| query.len() > MAX_QUERY_BYTES)
            || self
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.len() > MAX_CURSOR_BYTES)
        {
            return Err(arguments("list query or cursor exceeds its bound"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AuthorizedSessionInspectRequest {
    pub session_id: String,
    pub max_chars: Option<usize>,
}

impl AuthorizedSessionInspectRequest {
    pub fn validate(&self) -> Result<(), ToolError> {
        bamboo_domain::validate_supervisor_target_id(&self.session_id).map_err(arguments)?;
        if self
            .max_chars
            .is_some_and(|limit| !(1..=MAX_PROGRESS_EXCERPT_CHARS).contains(&limit))
        {
            return Err(arguments("inspect max_chars must be between 1 and 8000"));
        }
        Ok(())
    }
}

/// An already authorized metadata observation, not a reusable read/write grant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthorizedSessionTarget {
    pub session_id: String,
    pub project_id: ProjectId,
    pub created_at: DateTime<Utc>,
    pub metadata_version: u64,
    pub link_revision: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthorizedSessionPage {
    pub caller_session_id: String,
    pub scope_revision: u64,
    pub sessions: Vec<AuthorizedSessionTarget>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PersistedTaskCounts {
    pub pending: usize,
    pub in_progress: usize,
    pub completed: usize,
    pub blocked: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VisibleAssistantExcerpt {
    pub message_id: Option<String>,
    pub content: String,
    pub truncated: bool,
}

/// Session-scoped persisted evidence. It does not attest an active owner, a
/// particular accepted input's output, or successful task completion.
#[derive(Debug, Serialize, Deserialize)]
pub struct SessionProgressObservation {
    pub session_id: String,
    pub project_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub source_updated_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub metadata_version: u64,
    pub scope_revision: u64,
    pub title: String,
    pub last_persisted_status: String,
    pub has_pending_question: bool,
    pub task_counts: PersistedTaskCounts,
    pub latest_visible_assistant_excerpt: Option<VisibleAssistantExcerpt>,
    pub status_is_live: bool,
    pub input_specific: bool,
    pub completion_verified: bool,
}

/// One role-neutral capability surface. Hosts install an adapter; neither the
/// caller's role/title nor model arguments can select or manufacture a grant.
#[async_trait]
pub trait SessionDiscoveryPort: Send + Sync {
    async fn list(
        &self,
        ctx: &ToolCtx,
        request: AuthorizedSessionListRequest,
    ) -> Result<AuthorizedSessionPage, ToolError>;

    async fn inspect(
        &self,
        ctx: &ToolCtx,
        request: AuthorizedSessionInspectRequest,
    ) -> Result<SessionProgressObservation, ToolError>;
}

pub struct SupervisorSessionDiscovery {
    storage: Arc<dyn Storage>,
}

impl SupervisorSessionDiscovery {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    async fn caller(&self, ctx: &ToolCtx) -> Result<CallerSnapshot, ToolError> {
        let id = ctx.session_id().ok_or_else(authority_required)?;
        let observation = ctx
            .executing_supervisor_for(id)
            .ok_or_else(authority_required)?;
        let reference = observation.supervisor_reference();
        let session = self
            .storage
            .load_root_authority(id)
            .await
            .map_err(authority_unavailable)?
            .ok_or_else(authority_required)?;
        if ExecutingSupervisorObservation::capture_from_executing_session(&session)
            != Some(observation)
        {
            return Err(authority_required());
        }
        let state = session.supervisor_management.clone();
        if let Some(state) = &state {
            state
                .validate(reference.incarnation_id)
                .map_err(authority_unavailable)?;
        }
        Ok(CallerSnapshot {
            binding: CallerBinding {
                reference,
                created_at: session.created_at,
                tool_revision: session.root_tool_authority_revision,
                session_version: session.session_version,
                scope_revision: state.as_ref().map_or(0, |state| state.revision),
            },
            state,
        })
    }

    async fn unchanged(&self, ctx: &ToolCtx, binding: &CallerBinding) -> Result<(), ToolError> {
        if self.caller(ctx).await?.binding != *binding {
            return Err(stale(
                "caller or scope changed during discovery; start a new page",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallerBinding {
    reference: SupervisorReference,
    created_at: DateTime<Utc>,
    tool_revision: u64,
    session_version: Option<u32>,
    scope_revision: u64,
}

struct CallerSnapshot {
    binding: CallerBinding,
    state: Option<SupervisorManagementState>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryCursor {
    version: u8,
    caller: CallerBinding,
    query: Option<String>,
    limit: usize,
    after: String,
}

#[async_trait]
impl SessionDiscoveryPort for SupervisorSessionDiscovery {
    async fn list(
        &self,
        ctx: &ToolCtx,
        request: AuthorizedSessionListRequest,
    ) -> Result<AuthorizedSessionPage, ToolError> {
        request.validate()?;
        let caller = self.caller(ctx).await?;
        let query = request
            .query
            .as_deref()
            .map(str::trim)
            .filter(|query| !query.is_empty())
            .map(str::to_lowercase);
        let limit = request.limit.unwrap_or(MAX_DIRECTORY_PAGE_SIZE);
        let after = request
            .cursor
            .as_ref()
            .map(|value| {
                let cursor: DirectoryCursor = serde_json::from_str(value)
                    .map_err(|_| arguments("malformed authorized Session cursor"))?;
                if cursor.version != 1
                    || cursor.caller != caller.binding
                    || cursor.query != query
                    || cursor.limit != limit
                {
                    return Err(stale(
                        "cursor caller, scope or query changed; start a new page",
                    ));
                }
                bamboo_domain::validate_supervisor_target_id(&cursor.after).map_err(arguments)?;
                if !caller
                    .state
                    .as_ref()
                    .is_some_and(|state| state.links.contains_key(&cursor.after))
                {
                    return Err(stale("cursor target is no longer in the current scope"));
                }
                Ok(cursor.after)
            })
            .transpose()?;

        let mut authorized = Vec::new();
        if let Some(state) = caller.state.as_ref() {
            // Canonical state has a hard 256-link capacity, including tombstones.
            // We never scan the global Session index or read peer transcripts.
            if state.links.len() > MAX_SUPERVISOR_LINKS {
                return Err(authority_unavailable("management link capacity exceeded"));
            }
            for (id, link) in &state.links {
                if !link.enabled || !state.allowed_projects.contains(&link.target_project_id) {
                    continue;
                }
                let current = self
                    .storage
                    .inspect_supervisor_link(&caller.binding.reference, id)
                    .await
                    .map_err(authority_unavailable)?;
                if current.state_revision != caller.binding.scope_revision
                    || current.supervisor != caller.binding.reference
                    || current.target_session_id != *id
                    || current.link.as_ref() != Some(link)
                {
                    return Err(stale("management relationship changed; start a new page"));
                }
                if current.authorized
                    && query.as_ref().is_none_or(|query| {
                        id.to_lowercase().contains(query)
                            || link
                                .target_project_id
                                .to_string()
                                .to_lowercase()
                                .contains(query)
                    })
                {
                    authorized.push(AuthorizedSessionTarget {
                        session_id: id.clone(),
                        project_id: link.target_project_id.clone(),
                        created_at: link.target_created_at,
                        metadata_version: link.target_metadata_version,
                        link_revision: link.revision,
                    });
                }
            }
        }
        self.unchanged(ctx, &caller.binding).await?;
        // Authorization precedes both ordering and pagination. Each target was
        // independently observed under the existing strict authority locks.
        authorized.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        let mut sessions: Vec<_> = authorized
            .into_iter()
            .filter(|target| {
                after
                    .as_ref()
                    .is_none_or(|after| target.session_id > *after)
            })
            .take(limit + 1)
            .collect();
        let has_more = sessions.len() > limit;
        sessions.truncate(limit);
        let next_cursor = if has_more {
            Some(
                serde_json::to_string(&DirectoryCursor {
                    version: 1,
                    caller: caller.binding.clone(),
                    query,
                    limit,
                    after: sessions
                        .last()
                        .expect("nonempty bounded page")
                        .session_id
                        .clone(),
                })
                .map_err(authority_unavailable)?,
            )
        } else {
            None
        };
        Ok(AuthorizedSessionPage {
            caller_session_id: caller.binding.reference.session_id,
            scope_revision: caller.binding.scope_revision,
            sessions,
            next_cursor,
        })
    }

    async fn inspect(
        &self,
        ctx: &ToolCtx,
        request: AuthorizedSessionInspectRequest,
    ) -> Result<SessionProgressObservation, ToolError> {
        request.validate()?;
        let caller = self.caller(ctx).await?;
        // The same canonical reader retains caller/link/Project/birth authority
        // through the complete physical history read. A preflight+plain load
        // would not preserve this property.
        let session = self
            .storage
            .load_supervisor_managed_session(&caller.binding.reference, &request.session_id)
            .await
            .map_err(authority_unavailable)?
            .ok_or_else(|| authority_unavailable("authorized target is unavailable"))?;
        self.unchanged(ctx, &caller.binding).await?;
        Ok(progress_observation(
            &session,
            caller.binding.scope_revision,
            request.max_chars.unwrap_or(2_000),
        ))
    }
}

fn progress_observation(
    session: &Session,
    scope_revision: u64,
    max_chars: usize,
) -> SessionProgressObservation {
    let mut task_counts = PersistedTaskCounts::default();
    if let Some(list) = &session.task_list {
        for task in &list.items {
            match task.status {
                TaskItemStatus::Pending => task_counts.pending += 1,
                TaskItemStatus::InProgress => task_counts.in_progress += 1,
                TaskItemStatus::Completed => task_counts.completed += 1,
                TaskItemStatus::Blocked => task_counts.blocked += 1,
            }
        }
    }
    let latest_visible_assistant_excerpt = session
        .messages
        .iter()
        .rev()
        .find(|message| {
            message.role == Role::Assistant
                && !message.content.trim().is_empty()
                && !message.metadata.as_ref().is_some_and(|metadata| {
                    metadata
                        .get("hidden_from_ui")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
                })
        })
        .map(|message| VisibleAssistantExcerpt {
            message_id: (message.id.len() <= 256).then(|| message.id.clone()),
            content: message.content.chars().take(max_chars).collect(),
            truncated: message.content.chars().count() > max_chars,
        });
    let last_persisted_status = match session.last_run_status().as_deref() {
        Some("pending") => "pending",
        Some("running") => "running",
        Some("completed") => "completed",
        Some("error") => "error",
        Some("cancelled") => "cancelled",
        Some("suspended") => "suspended",
        Some("skipped") => "skipped",
        Some("timeout") => "timeout",
        _ => "unknown",
    };
    SessionProgressObservation {
        session_id: session.id.clone(),
        project_id: session.project_id_meta(),
        created_at: session.created_at,
        source_updated_at: session.updated_at,
        observed_at: Utc::now(),
        metadata_version: session.metadata_version,
        scope_revision,
        title: session.title.chars().take(256).collect(),
        last_persisted_status: last_persisted_status.into(),
        has_pending_question: session.has_pending_question(),
        task_counts,
        latest_visible_assistant_excerpt,
        status_is_live: false,
        input_specific: false,
        completion_verified: false,
    }
}

fn arguments(message: impl std::fmt::Display) -> ToolError {
    ToolError::InvalidArguments(format!("session_discovery_invalid_request: {message}"))
}
fn authority_required() -> ToolError {
    ToolError::Execution(
        "session_discovery_authority_required: original authorized caller identity is required"
            .into(),
    )
}
fn authority_unavailable(message: impl std::fmt::Display) -> ToolError {
    ToolError::Execution(format!(
        "session_discovery_authority_unavailable: {message}"
    ))
}
fn stale(message: &str) -> ToolError {
    ToolError::Execution(format!("session_discovery_cursor_stale: {message}"))
}
