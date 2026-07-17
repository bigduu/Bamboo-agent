//! Shared adapter implementing `ChildSessionPort` for server-side child session tools.
//!
//! The unified `SubAgentTool` delegates to this adapter instead of
//! duplicating `ChildSessionPort` implementations.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, Duration, Instant};

use crate::app_state::session_events::get_or_create_event_sender;
use crate::app_state::{AgentRunner, AgentStatus};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::{AgentEvent, Session, SessionKind};
use bamboo_domain::session::runtime_state::{
    AgentRuntimeState, ChildWaitPolicy, WaitingForChildrenState,
};
use bamboo_engine::execution::spawn::{SpawnJob, SpawnScheduler};
use bamboo_engine::session_app::child_session::{
    ChildRunnerInfo, ChildSessionEntry, ChildSessionError, ChildSessionPort, DeleteChildResult,
    SubagentResolutionPort,
};
use bamboo_llm::Config;
use bamboo_storage::{LockedSessionStore, SessionIndexEntry, SessionStoreV2};

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
    /// Coalesces concurrent parent-wait registrations for the same parent that
    /// arrive in one spawn round (the LLM emitting several `SubAgent.create`
    /// calls at once → `join_all`) into a single parent persist. See
    /// [`ChildSessionAdapter::register_parent_wait_for_child`].
    pub(crate) parent_wait_slots: Arc<dashmap::DashMap<String, Arc<ParentWaitSlot>>>,
    /// Deps to start the always-on notification relay for a NEWLY ENQUEUED
    /// child session (see [`ChildSessionAdapter::enqueue_child_run`]).
    ///
    /// Without this, a child's own events (e.g. a `run_in_background` Bash
    /// command finishing, or the child hitting critical context pressure)
    /// only ever get classified if a client happens to be subscribed to that
    /// specific child session's SSE/WS stream — the child's completion
    /// (`SubAgentCompleted`) still reaches the owner via the PARENT's own
    /// relay (already running for any in-process parent execution), but
    /// events that occur only on the child's own stream would otherwise be
    /// silently dropped for a headless/unwatched child. `None` for the
    /// out-of-process worker binary (`ChildSessionAdapter::new`), which has
    /// no local desktop/ntfy/bark config surface to deliver through.
    pub(crate) notification_relay: Option<crate::app_state::session_events::NotificationRelayDeps>,
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
    /// Construct an adapter. Public so a self-orchestrating WORKER (Phase 6:
    /// direct nested execution) can build its OWN child-session machinery
    /// against its own store/scheduler — the struct fields are `pub(crate)`, so
    /// out-of-crate callers (the worker binary) go through this constructor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_store: Arc<SessionStoreV2>,
        storage: Arc<dyn Storage>,
        persistence: Arc<LockedSessionStore>,
        scheduler: Arc<SpawnScheduler>,
        sessions_cache: bamboo_engine::SessionCache,
        agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
        session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
        subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
        config: Arc<RwLock<Config>>,
    ) -> Self {
        Self {
            session_store,
            storage,
            persistence,
            scheduler,
            sessions_cache,
            agent_runners,
            session_event_senders,
            subagent_model_resolver,
            config,
            // Fresh per-adapter wait-coalescing map (the type is private to this
            // crate, so out-of-crate callers can't supply it).
            parent_wait_slots: Arc::new(dashmap::DashMap::new()),
            // The worker binary runs on a different machine than the desktop
            // it would notify; it reports events back to the orchestrating
            // server over the wire instead. See the field doc.
            notification_relay: None,
        }
    }

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

        // 5. Self-clean: the slot exists only to coalesce a burst of sibling
        //    registrations for THIS parent. Now that the batch is durably
        //    persisted and nothing new is pending, drop the map entry so
        //    `parent_wait_slots` does not retain one entry per parent-that-ever-
        //    -spawned forever (issue #346). Still inside the flush barrier.
        //
        //    Race-freedom: `remove_if` re-checks `pending.is_empty()` under the
        //    DashMap shard lock. A sibling that enqueued after our drain made
        //    `pending` non-empty, so the predicate is false and we keep the slot;
        //    that sibling (blocked on the barrier we still hold) will flush it and
        //    run this same removal. A sibling that clones the slot Arc but has not
        //    yet pushed keeps a live handle, so removing the map entry never loses
        //    its child: whoever holds the barrier drains ALL pending on its Arc.
        self.parent_wait_slots
            .remove_if(parent_session_id, |_, slot| slot.pending.lock().is_empty());

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
            .filter(|(_, status)| !status.as_deref().is_some_and(is_terminal_child_status))
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
            .unwrap_or_else(|| WaitingForChildrenState::for_children(Vec::new(), policy, now));
        // An explicit wait re-asserts the policy on any pre-existing wait state.
        wait.wait_for = policy;
        for (child_session_id, tool_call_id) in batch {
            if !wait
                .child_session_ids
                .iter()
                .any(|id| id == child_session_id)
            {
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
}

/// Lets a [`ChildSessionAdapter`] act as the engine's guardian-review spawner.
///
/// `Arc<ChildSessionAdapter>` therefore doubles as `Arc<dyn GuardianSpawner>`
/// (wired onto `AppState`), so the terminal gate spawns the read-only reviewer
/// through the same child-session machinery the `SubAgent` tool uses — no second
/// spawn path. The reviewer is a real sub-agent: it fetches the diff and runs
/// tests itself via its (read-only) toolset.
#[async_trait]
impl bamboo_engine::GuardianSpawner for ChildSessionAdapter {
    async fn spawn_guardian_review(
        &self,
        parent_session: &Session,
        review_prompt: String,
        model: String,
        disabled_tools: Option<std::collections::BTreeSet<String>>,
    ) -> Result<String, String> {
        let input = bamboo_engine::session_app::child_session::CreateChildInput {
            parent_session: parent_session.clone(),
            child_id: format!("guardian-{}", uuid::Uuid::new_v4()),
            title: "Guardian review".to_string(),
            responsibility: "Adversarially verify the parent agent's completed work.".to_string(),
            assignment_prompt: review_prompt,
            // The coordinator branches on this subagent_type to recognize a
            // guardian completion and parse its verdict.
            subagent_type: "guardian".to_string(),
            workspace: parent_session.workspace_path_meta().unwrap_or_default(),
            model_override: Some(model),
            model_ref_override: None,
            runtime_metadata: HashMap::new(),
            auto_run: true,
            reasoning_effort: None,
            lifecycle: None,
            resident_name: None,
            resident_context: None,
            disabled_tools,
            context_fork: None,
        };
        bamboo_engine::session_app::child_session::create_child_action(self, input)
            .await
            .map(|result| result.child_session_id)
            .map_err(|error| error.to_string())
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
        // Parent-side control write: the parent authoritatively sets the child's
        // posture (e.g. the #74 resident-reuse bypass/no-human re-seed), so this
        // must persist the in-memory flags as-is, NOT adopt the child's stale
        // on-disk bypass (which the loop-protection in `merge_save_runtime`
        // would). #540.
        self.persistence
            .save_runtime_authoritative_flags(child)
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

    async fn find_resident_child(
        &self,
        root_session_id: &str,
        resident_name: &str,
    ) -> Option<String> {
        let name = resident_name.trim();
        if name.is_empty() {
            return None;
        }
        // Scan the index for a child in this root tree tagged with the resident
        // name. Prefer the most recently updated if (defensively) more than one
        // exists. Index-backed: no session.json loads.
        let mut best: Option<(String, chrono::DateTime<chrono::Utc>)> = None;
        for entry in self.session_store.list_index_entries().await {
            if entry.kind == SessionKind::Child
                && entry.root_session_id == root_session_id
                && entry.resident_name.as_deref() == Some(name)
            {
                match &best {
                    Some((_, ts)) if *ts >= entry.updated_at => {}
                    _ => best = Some((entry.id.clone(), entry.updated_at)),
                }
            }
        }
        best.map(|(id, _)| id)
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

        // Per-child tool denylist: persisted onto the child session by
        // `create_child_action` (JSON in metadata). Most sub-agents are full
        // agents and carry none; a read-only Guardian reviewer carries a
        // denylist here so the worker trims its toolset. `SpawnJob` wants a
        // `Vec<String>`, so collect the set.
        let disabled_tools = child
            .metadata
            .get("disabled_tools")
            .and_then(|raw| serde_json::from_str::<std::collections::BTreeSet<String>>(raw).ok())
            .filter(|set| !set.is_empty())
            .map(|set| set.into_iter().collect::<Vec<String>>());

        // Start the always-on notification relay for the CHILD's own session
        // before enqueueing the job — mirrors the execute handler starting it
        // at execution entry (`spawn_event_forwarder`) rather than waiting
        // for a client to subscribe. Idempotent (`try_begin_relay`) and races
        // harmlessly with the engine's own `get_or_create_event_sender` call
        // for the same id in `run_child_spawn`, since both resolve to the
        // same map entry. See the `notification_relay` field doc.
        if let Some(relay) = &self.notification_relay {
            let child_tx = get_or_create_event_sender(&self.session_event_senders, &child.id).await;
            crate::app_state::session_events::ensure_notification_relay(relay, &child.id, child_tx);
        }

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
