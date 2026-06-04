//! Shared adapter implementing `ChildSessionPort` for server-side child session tools.
//!
//! The unified `SubAgentTool` delegates to this adapter instead of
//! duplicating `ChildSessionPort` implementations.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, Duration, Instant};

use crate::app_state::session_events::get_or_create_event_sender;
use crate::app_state::{AgentRunner, AgentStatus};
use crate::session_app::child_session::{
    ChildRunnerInfo, ChildSessionEntry, ChildSessionError, ChildSessionPort, DeleteChildResult,
};
use crate::spawn_scheduler::{SpawnJob, SpawnScheduler};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolError;
use bamboo_agent_core::{AgentEvent, Session, SessionKind};
use bamboo_domain::session::runtime_state::{
    AgentRuntimeState, ChildWaitPolicy, WaitingForChildrenState,
};
use bamboo_infrastructure::{Config, LockedSessionStore, SessionIndexEntry, SessionStoreV2};

/// Server-side adapter that bridges domain `ChildSessionPort` to infrastructure.
///
/// Holds all shared state needed by `SubAgentTool`.
/// Implements the full `ChildSessionPort` trait with real methods (no stubs).
pub struct ChildSessionAdapter {
    pub(crate) session_store: Arc<SessionStoreV2>,
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) persistence: Arc<LockedSessionStore>,
    pub(crate) scheduler: Arc<SpawnScheduler>,
    pub(crate) sessions_cache: bamboo_engine::SessionCache,
    pub(crate) agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub(crate) session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    /// Optional subagent model resolver: maps subagent_type → provider+model ref.
    pub(crate) subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
    /// Application config for resolving subagent routing and external agent profiles.
    pub(crate) config: Arc<RwLock<Config>>,
    /// Subagent profile registry. Used to resolve `subagent_type` →
    /// `system_prompt` and tool surface filter.
    pub(crate) subagent_profiles: Arc<bamboo_domain::subagent::SubagentProfileRegistry>,
    /// Cached list of all available tool names from the base executor.
    /// Used to compute the complement set for Allowlist policies.
    pub(crate) tool_names: Vec<String>,
}

const AGENT_RUNTIME_STATE_METADATA_KEY: &str = "agent.runtime.state";

fn read_runtime_state(session: &Session) -> AgentRuntimeState {
    session
        .agent_runtime_state
        .clone()
        .or_else(|| {
            session
                .metadata
                .get(AGENT_RUNTIME_STATE_METADATA_KEY)
                .and_then(|raw| serde_json::from_str::<AgentRuntimeState>(raw).ok())
        })
        .unwrap_or_else(|| AgentRuntimeState::new(format!("{}-wait", session.id)))
}

fn write_runtime_state(session: &mut Session, runtime_state: &AgentRuntimeState) {
    session.agent_runtime_state = Some(runtime_state.clone());
    if let Ok(serialized) = serde_json::to_string(runtime_state) {
        session
            .metadata
            .insert(AGENT_RUNTIME_STATE_METADATA_KEY.to_string(), serialized);
    }
}

impl ChildSessionAdapter {
    /// Resolve the provider+model ref for a given subagent_type using the configured resolver.
    pub async fn resolve_subagent_model(
        &self,
        subagent_type: &str,
    ) -> Option<bamboo_domain::ProviderModelRef> {
        match &self.subagent_model_resolver {
            Some(resolver) => resolver(subagent_type.to_string()).await,
            None => None,
        }
    }

    /// Resolve runtime metadata (e.g. external agent routing) for a subagent_type.
    pub async fn resolve_runtime_metadata(&self, subagent_type: &str) -> HashMap<String, String> {
        let config = self.config.read().await;
        crate::external_agents::config::resolve_runtime_metadata(&config, subagent_type)
    }

    /// Resolve the canonical system prompt for the given `subagent_type`.
    ///
    /// Always returns a prompt: unknown / empty `subagent_type` values fall
    /// back to the `general-purpose` profile (whose prompt is byte-equal to
    /// the legacy `CHILD_SYSTEM_PROMPT`).
    pub fn resolve_subagent_prompt(&self, subagent_type: &str) -> String {
        self.subagent_profiles
            .resolve(subagent_type)
            .system_prompt
            .clone()
    }

    /// Register a durable parent wait for an enqueued child session.
    ///
    /// This is intentionally idempotent: repeated registrations for the same
    /// child merge into the existing wait set. The child runner owns timeout
    /// and liveness; the parent wait timeout is a long lease for observability.
    pub async fn register_parent_wait_for_child(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
        tool_call_id: Option<&str>,
    ) -> Result<(), ChildSessionError> {
        let Some(mut parent) =
            self.storage
                .load_session(parent_session_id)
                .await
                .map_err(|error| {
                    ChildSessionError::Execution(format!(
                        "failed to load parent session {parent_session_id}: {error}"
                    ))
                })?
        else {
            return Err(ChildSessionError::NotFound(parent_session_id.to_string()));
        };

        let mut runtime_state = read_runtime_state(&parent);
        runtime_state
            .children
            .active_ids
            .retain(|id| id != child_session_id);
        runtime_state
            .children
            .completed_ids
            .retain(|id| id != child_session_id);
        runtime_state
            .children
            .active_ids
            .push(child_session_id.to_string());
        runtime_state.children.active_ids.sort();
        runtime_state.children.active_ids.dedup();
        runtime_state.children.active_children = runtime_state.children.active_ids.len() as u32;
        runtime_state.children.completed_children =
            runtime_state.children.completed_ids.len() as u32;

        let now = Utc::now();
        let mut wait = runtime_state
            .waiting_for_children
            .take()
            .unwrap_or_else(|| WaitingForChildrenState {
                child_session_ids: Vec::new(),
                wait_for: ChildWaitPolicy::All,
                registered_at: now,
                timeout_at: Some(now + ChronoDuration::hours(6)),
                registered_by_tool_call_id: tool_call_id.map(str::to_string),
            });
        if !wait
            .child_session_ids
            .iter()
            .any(|id| id == child_session_id)
        {
            wait.child_session_ids.push(child_session_id.to_string());
        }
        wait.child_session_ids.sort();
        wait.child_session_ids.dedup();
        if wait.registered_by_tool_call_id.is_none() {
            wait.registered_by_tool_call_id = tool_call_id.map(str::to_string);
        }
        runtime_state.waiting_for_children = Some(wait);

        write_runtime_state(&mut parent, &runtime_state);
        parent.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "waiting_for_children".to_string(),
        );
        parent.updated_at = Utc::now();

        self.persistence
            .merge_save_runtime(&mut parent)
            .await
            .map_err(|error| {
                ChildSessionError::Execution(format!("failed to save parent wait state: {error}"))
            })?;
        self.sessions_cache.insert(
            parent.id.clone(),
            Arc::new(parking_lot::RwLock::new(parent)),
        );

        Ok(())
    }
}

fn map_index_entry_to_child_entry(entry: &SessionIndexEntry) -> ChildSessionEntry {
    ChildSessionEntry {
        child_session_id: entry.id.clone(),
        title: entry.title.clone(),
        pinned: entry.pinned,
        message_count: entry.message_count,
        updated_at: entry.updated_at.to_rfc3339(),
        last_run_status: entry.last_run_status.clone(),
        last_run_error: entry.last_run_error.clone(),
    }
}

#[async_trait]
impl ChildSessionPort for ChildSessionAdapter {
    async fn load_root_session(&self, root_session_id: &str) -> Result<Session, ChildSessionError> {
        let Some(session) = self
            .storage
            .load_session(root_session_id)
            .await
            .map_err(|error| {
                ChildSessionError::Execution(format!(
                    "failed to load session {root_session_id}: {error}"
                ))
            })?
        else {
            return Err(ChildSessionError::NotFound(root_session_id.to_string()));
        };

        if session.kind != SessionKind::Root {
            return Err(ChildSessionError::NotRootSession(
                root_session_id.to_string(),
            ));
        }

        Ok(session)
    }

    async fn load_child_for_parent(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
    ) -> Result<Session, ChildSessionError> {
        let Some(child) = self
            .storage
            .load_session(child_session_id)
            .await
            .map_err(|error| {
                ChildSessionError::Execution(format!(
                    "failed to load child session {child_session_id}: {error}"
                ))
            })?
        else {
            return Err(ChildSessionError::NotFound(child_session_id.to_string()));
        };

        if child.kind != SessionKind::Child {
            return Err(ChildSessionError::NotChildSession(
                child_session_id.to_string(),
            ));
        }

        if child.parent_session_id.as_deref() != Some(parent_session_id) {
            return Err(ChildSessionError::NotChildOfParent {
                child_id: child_session_id.to_string(),
                parent_id: parent_session_id.to_string(),
            });
        }

        Ok(child)
    }

    async fn save_child_session(&self, child: &mut Session) -> Result<(), ChildSessionError> {
        self.persistence
            .merge_save_runtime(child)
            .await
            .map_err(|error| {
                ChildSessionError::Execution(format!("failed to save child session: {error}"))
            })?;

        self.sessions_cache.insert(
            child.id.clone(),
            Arc::new(parking_lot::RwLock::new(child.clone())),
        );

        Ok(())
    }

    async fn is_child_running(&self, child_session_id: &str) -> bool {
        let runners = self.agent_runners.read().await;
        runners
            .get(child_session_id)
            .is_some_and(|runner| matches!(runner.status, AgentStatus::Running))
    }

    async fn list_children(&self, parent_session_id: &str) -> Vec<ChildSessionEntry> {
        self.session_store
            .list_index_entries()
            .await
            .into_iter()
            .filter(|entry| {
                entry.kind == SessionKind::Child
                    && entry.parent_session_id.as_deref() == Some(parent_session_id)
            })
            .map(|entry| map_index_entry_to_child_entry(&entry))
            .collect()
    }

    async fn enqueue_child_run(
        &self,
        parent: &Session,
        child: &Session,
    ) -> Result<(), ChildSessionError> {
        let model = if child.model.trim().is_empty() {
            parent.model.clone()
        } else {
            child.model.clone()
        };
        if model.trim().is_empty() {
            return Err(ChildSessionError::Execution(
                "child model is empty and parent model is unavailable".to_string(),
            ));
        }

        // Resolve profile policy into schema-level disabled_tools.
        let disabled_tools = child
            .subagent_type()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .and_then(|subagent_type| {
                let profile = self.subagent_profiles.resolve(&subagent_type);
                match &profile.tools {
                    bamboo_domain::subagent::ToolPolicy::Inherit => None,
                    policy => {
                        let names = bamboo_domain::subagent::disabled_tools_for_profile(
                            policy,
                            &self.tool_names,
                        );
                        if names.is_empty() {
                            None
                        } else {
                            Some(names)
                        }
                    }
                }
            });

        self.register_parent_wait_for_child(&parent.id, &child.id, None)
            .await?;

        self.scheduler
            .enqueue(SpawnJob {
                parent_session_id: parent.id.clone(),
                child_session_id: child.id.clone(),
                model,
                disabled_tools,
            })
            .await
            .map_err(ChildSessionError::Execution)?;

        let parent_tx = get_or_create_event_sender(&self.session_event_senders, &parent.id).await;
        let _ = parent_tx.send(AgentEvent::SubAgentStarted {
            parent_session_id: parent.id.clone(),
            child_session_id: child.id.clone(),
            title: Some(child.title.clone()),
        });

        Ok(())
    }

    async fn cancel_child_run_and_wait(
        &self,
        child_session_id: &str,
    ) -> Result<(), ChildSessionError> {
        let cancelled = {
            let mut runners = self.agent_runners.write().await;
            if let Some(runner) = runners.get_mut(child_session_id) {
                if matches!(runner.status, AgentStatus::Running) {
                    runner.cancel_token.cancel();
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };

        if !cancelled {
            return Ok(());
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let still_running = {
                let runners = self.agent_runners.read().await;
                runners
                    .get(child_session_id)
                    .is_some_and(|runner| matches!(runner.status, AgentStatus::Running))
            };
            if !still_running {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ChildSessionError::Execution(format!(
                    "timed out waiting for child session {child_session_id} to stop after cancellation"
                )));
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn delete_child_session(
        &self,
        parent_session_id: &str,
        child_id: &str,
    ) -> Result<DeleteChildResult, ChildSessionError> {
        let cancelled_running_child = {
            let mut runners = self.agent_runners.write().await;
            if let Some(runner) = runners.remove(child_id) {
                runner.cancel_token.cancel();
                true
            } else {
                false
            }
        };

        let deleted = self
            .storage
            .delete_session(child_id)
            .await
            .map_err(|error| {
                ChildSessionError::Execution(format!("failed to delete child session: {error}"))
            })?;

        self.sessions_cache.remove(child_id);
        {
            let mut senders = self.session_event_senders.write().await;
            senders.remove(child_id);
            if cancelled_running_child {
                if let Some(parent_tx) = senders.get(parent_session_id) {
                    let _ = parent_tx.send(AgentEvent::SubAgentCompleted {
                        parent_session_id: parent_session_id.to_string(),
                        child_session_id: child_id.to_string(),
                        status: "cancelled".to_string(),
                        error: Some("Child session deleted while running".to_string()),
                    });
                }
            }
        }

        Ok(DeleteChildResult {
            deleted,
            cancelled_running_child,
        })
    }

    async fn get_child_runner_info(&self, child_id: &str) -> Option<ChildRunnerInfo> {
        let runners = self.agent_runners.read().await;
        runners.get(child_id).map(|runner| ChildRunnerInfo {
            started_at: Some(runner.started_at),
            completed_at: runner.completed_at,
            last_tool_name: runner.last_tool_name.clone(),
            last_tool_phase: runner.last_tool_phase.clone(),
            last_event_at: runner.last_event_at,
            round_count: runner.round_count,
        })
    }
}

/// Map a `ChildSessionError` to a server `ToolError`.
pub fn tool_error_from_child_session(error: ChildSessionError) -> ToolError {
    match error {
        ChildSessionError::NotFound(id) => ToolError::Execution(format!("session not found: {id}")),
        ChildSessionError::NotRootSession(id) => {
            ToolError::Execution(format!("session is not a root session: {id}"))
        }
        ChildSessionError::InvalidArguments(msg) => ToolError::InvalidArguments(msg),
        ChildSessionError::Execution(msg) => ToolError::Execution(msg),
        other => ToolError::Execution(other.to_string()),
    }
}
