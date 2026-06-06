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
use bamboo_engine::session_app::child_session::{
    ChildRunnerInfo, ChildSessionEntry, ChildSessionError, ChildSessionPort, DeleteChildResult,
    SubagentResolutionPort,
};
use bamboo_engine::execution::spawn::{SpawnJob, SpawnScheduler};
use bamboo_agent_core::storage::Storage;
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
    /// Coalesces concurrent parent-wait registrations for the same parent that
    /// arrive in one spawn round (the LLM emitting several `SubAgent.create`
    /// calls at once → `join_all`) into a single parent persist. See
    /// [`ChildSessionAdapter::register_parent_wait_for_child`].
    pub(crate) parent_wait_slots: Arc<dashmap::DashMap<String, Arc<ParentWaitSlot>>>,
}

/// Per-parent coalescing slot for batched wait registration.
///
/// `flush_lock` is a barrier distinct from the persistence per-session lock
/// (using the latter here would deadlock, since the flush itself takes it). The
/// first registration to win the barrier drains `pending` and persists the whole
/// batch once; concurrent registrations that find `pending` already drained were
/// persisted by that holder before it released the barrier, so they return
/// without an extra write.
#[derive(Default)]
pub(crate) struct ParentWaitSlot {
    flush_lock: tokio::sync::Mutex<()>,
    pending: parking_lot::Mutex<Vec<(String, Option<String>)>>,
}

const AGENT_RUNTIME_STATE_METADATA_KEY: &str = "agent.runtime.state";

/// Terminal child run statuses, as mirrored into the session index. A child not
/// in one of these states is considered active (still pending/running).
fn is_terminal_child_status(status: &str) -> bool {
    matches!(
        status,
        "completed" | "error" | "timeout" | "cancelled" | "skipped"
    )
}

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
        bamboo_engine::external_agents::config::resolve_runtime_metadata(&config, subagent_type)
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
    ///
    /// Registrations are **coalesced** per parent: when several children are
    /// spawned in one round (the LLM issuing multiple `SubAgent.create` calls
    /// that `join_all` runs concurrently), the first call to win the per-parent
    /// barrier drains all currently-pending registrations and persists the parent
    /// once, instead of each child triggering its own load+write. Callers whose
    /// child was drained-and-persisted by that holder return without an extra
    /// write — and only after the holder's write committed, so durability holds.
    pub async fn register_parent_wait_for_child(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
        tool_call_id: Option<&str>,
    ) -> Result<(), ChildSessionError> {
        let slot = self
            .parent_wait_slots
            .entry(parent_session_id.to_string())
            .or_default()
            .clone();

        // 1. Enqueue this registration.
        slot.pending.lock().push((
            child_session_id.to_string(),
            tool_call_id.map(str::to_string),
        ));

        // 2. Barrier: serialize flushers for this parent.
        let _flush_guard = slot.flush_lock.lock().await;

        // 3. Drain everything pending for this parent (siblings that enqueued
        //    while we waited for the barrier are picked up here too).
        let batch: Vec<(String, Option<String>)> = {
            let mut pending = slot.pending.lock();
            pending.drain(..).collect()
        };
        if batch.is_empty() {
            // A prior barrier holder already persisted our child before releasing
            // the barrier we just acquired — nothing left to write.
            return Ok(());
        }

        // 4. Persist the whole batch in a single parent write.
        if let Err(error) = self
            .flush_parent_waits(parent_session_id, &batch, ChildWaitPolicy::All)
            .await
        {
            // Re-queue so nothing is silently lost; a retry or sibling picks it up.
            let mut pending = slot.pending.lock();
            for item in batch {
                pending.push(item);
            }
            return Err(error);
        }
        Ok(())
    }

    /// Explicitly register a parent wait for an arbitrary set of children with a
    /// chosen policy. Used by the `SubAgent.wait` action (wait on all active
    /// children) and the end-of-turn safety net. A single parent write.
    ///
    /// Returns the number of children the wait now covers (0 means there was
    /// nothing to wait on and no wait was registered).
    pub async fn register_parent_wait_for_children(
        &self,
        parent_session_id: &str,
        child_session_ids: &[String],
        policy: ChildWaitPolicy,
    ) -> Result<usize, ChildSessionError> {
        if child_session_ids.is_empty() {
            return Ok(0);
        }
        let batch: Vec<(String, Option<String>)> = child_session_ids
            .iter()
            .map(|id| (id.clone(), None))
            .collect();
        self.flush_parent_waits(parent_session_id, &batch, policy)
            .await?;
        Ok(batch.len())
    }

    /// The parent's currently-active (non-terminal) children, derived from the
    /// session index (single source of truth).
    pub async fn active_child_ids(&self, parent_session_id: &str) -> Vec<String> {
        self.storage
            .list_child_run_statuses(parent_session_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|(_, status)| {
                !status
                    .as_deref()
                    .is_some_and(is_terminal_child_status)
            })
            .map(|(id, _)| id)
            .collect()
    }

    /// Persist a batch of parent-wait registrations in one runtime-only save.
    async fn flush_parent_waits(
        &self,
        parent_session_id: &str,
        batch: &[(String, Option<String>)],
        policy: ChildWaitPolicy,
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

        // The active/completed child sets are derived from the session index
        // (single source of truth), so we no longer maintain a denormalized copy
        // here. Only the durable wait state below is parent-owned.
        let mut runtime_state = read_runtime_state(&parent);

        let now = Utc::now();
        let mut wait = runtime_state
            .waiting_for_children
            .take()
            .unwrap_or_else(|| WaitingForChildrenState {
                child_session_ids: Vec::new(),
                wait_for: policy,
                registered_at: now,
                timeout_at: Some(now + ChronoDuration::hours(6)),
                registered_by_tool_call_id: None,
            });
        // An explicit wait re-asserts the policy on any pre-existing wait state.
        wait.wait_for = policy;
        for (child_session_id, tool_call_id) in batch {
            if !wait.child_session_ids.iter().any(|id| id == child_session_id) {
                wait.child_session_ids.push(child_session_id.clone());
            }
            if wait.registered_by_tool_call_id.is_none() {
                wait.registered_by_tool_call_id = tool_call_id.clone();
            }
        }
        wait.child_session_ids.sort();
        wait.child_session_ids.dedup();
        runtime_state.waiting_for_children = Some(wait);

        write_runtime_state(&mut parent, &runtime_state);
        parent.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "waiting_for_children".to_string(),
        );
        parent.updated_at = Utc::now();

        // Runtime-only save: registering a parent's wait mutates the
        // control-plane (runtime_state + suspend metadata) but NEVER the message
        // history. Writing just the sidecar keeps spawn O(1) in conversation
        // length instead of rewriting the parent's full session.json per child.
        self.persistence
            .save_runtime_only(&mut parent)
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
impl SubagentResolutionPort for ChildSessionAdapter {
    async fn resolve_subagent_model(
        &self,
        subagent_type: &str,
    ) -> Option<bamboo_domain::ProviderModelRef> {
        ChildSessionAdapter::resolve_subagent_model(self, subagent_type).await
    }

    async fn resolve_runtime_metadata(
        &self,
        subagent_type: &str,
    ) -> std::collections::HashMap<String, String> {
        ChildSessionAdapter::resolve_runtime_metadata(self, subagent_type).await
    }

    fn resolve_subagent_prompt(&self, subagent_type: &str) -> String {
        ChildSessionAdapter::resolve_subagent_prompt(self, subagent_type)
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

        // NOTE: enqueue only *runs* the child in the background. Registering the
        // parent's wait (which suspends the parent) is now an explicit, separate
        // step so the model can spawn several children without each one
        // suspending it — see `register_parent_wait_for_child` /
        // `register_parent_wait_for_children` and the `SubAgent.wait` action.
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

    async fn register_parent_wait_for_child(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
        tool_call_id: Option<&str>,
    ) -> Result<(), ChildSessionError> {
        ChildSessionAdapter::register_parent_wait_for_child(
            self,
            parent_session_id,
            child_session_id,
            tool_call_id,
        )
        .await
    }

    async fn register_parent_wait_for_children(
        &self,
        parent_session_id: &str,
        child_session_ids: &[String],
        policy: ChildWaitPolicy,
    ) -> Result<usize, ChildSessionError> {
        ChildSessionAdapter::register_parent_wait_for_children(
            self,
            parent_session_id,
            child_session_ids,
            policy,
        )
        .await
    }

    async fn active_child_ids(&self, parent_session_id: &str) -> Vec<String> {
        ChildSessionAdapter::active_child_ids(self, parent_session_id).await
    }

    async fn ensure_child_indexed(&self, child_session_id: &str) {
        let _ = self.session_store.get_index_entry(child_session_id).await;
    }
}
