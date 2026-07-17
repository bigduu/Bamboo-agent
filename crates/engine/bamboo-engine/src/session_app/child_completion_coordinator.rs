//! Child-session completion coordinator.
//!
//! Receives terminal child runner notifications from `bamboo-engine`, updates
//! durable parent wait state, and resumes the parent when the configured wait
//! policy is satisfied.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock as StdRwLock};
use std::time::Duration;

use bamboo_domain::poison::PoisonRecover;

use crate::execution::{
    create_event_forwarder, spawn_session_execution, try_reserve_runner, AgentRunner,
    ChildCompletion, ChildCompletionHandler, RunnerReservation, SessionExecutionArgs,
};
use crate::runtime::config::{BashResumeHook, GuardianSpawner, BASH_COMPLETION_RESUME_KIND};
use crate::runtime::guardian_state::{
    parse_guardian_verdict, read_guardian_config, read_guardian_state, write_guardian_state,
    GuardianVerdict,
};
use crate::Agent;
use async_trait::async_trait;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{
    AgentEvent, BashCompletionInfo, BashCompletionSink, Message, Role, Session,
};
use bamboo_domain::session::runtime_state::{
    AgentRuntimeState, AgentStatusState, ChildWaitPolicy, SuspensionState,
};
use bamboo_llm::{Config, ProviderModelRouter, ProviderRegistry};
use bamboo_storage::LockedSessionStore;
use chrono::Utc;
use tokio::sync::{broadcast, RwLock};

use crate::model_areas::resolve_global_area_models;
use crate::model_config_helper::{
    resolve_fast_model, resolve_gold_config, GOLD_CONFIG_METADATA_KEY,
};
use crate::session_app::provider_model::session_effective_model_ref;
use crate::session_app::resume::{
    resume_session_execution, ResumeExecutionPort, ResumeSpawnRequest,
};
use crate::session_app::types::{ResumeConfigSnapshot, ResumeOutcome};

const AGENT_RUNTIME_STATE_METADATA_KEY: &str = "agent.runtime.state";
const RUNTIME_RESUME_MESSAGE_HIDDEN_KEY: &str = "hidden_from_ui";
const RUNTIME_RESUME_MESSAGE_KIND_KEY: &str = "runtime_kind";

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
        .unwrap_or_else(|| AgentRuntimeState::new(format!("{}-child-wait", session.id)))
}

fn write_runtime_state(session: &mut Session, runtime_state: &AgentRuntimeState) {
    session.agent_runtime_state = Some(runtime_state.clone());
    if let Ok(serialized) = serde_json::to_string(runtime_state) {
        session
            .metadata
            .insert(AGENT_RUNTIME_STATE_METADATA_KEY.to_string(), serialized);
    }
}

fn is_error_like(status: &str) -> bool {
    matches!(status, "error" | "timeout" | "cancelled")
}

/// Terminal child run statuses, as mirrored into the session index.
fn is_terminal_child_status(status: &str) -> bool {
    matches!(
        status,
        "completed" | "error" | "timeout" | "cancelled" | "skipped"
    )
}

/// Reconstruct the set of completed child session ids for a parent from the
/// session index (the single source of truth), folding in the child whose
/// completion event is being processed so a momentarily-lagging index can never
/// stall the parent's resume.
async fn derive_completed_child_ids(
    storage: &Arc<dyn Storage>,
    parent_session_id: &str,
    just_completed_child_id: &str,
) -> Vec<String> {
    let mut completed: Vec<String> = storage
        .list_child_run_statuses(parent_session_id)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|(_, status)| status.as_deref().is_some_and(is_terminal_child_status))
        .map(|(id, _)| id)
        .collect();
    if !completed.iter().any(|id| id == just_completed_child_id) {
        completed.push(just_completed_child_id.to_string());
    }
    completed.sort();
    completed.dedup();
    completed
}

fn read_config_snapshot(config: &Arc<RwLock<Config>>, cached_config: &StdRwLock<Config>) -> Config {
    if let Ok(config_guard) = config.try_read() {
        let snapshot = config_guard.clone();

        if let Ok(mut cached_guard) = cached_config.try_write() {
            *cached_guard = snapshot.clone();
        }

        snapshot
    } else {
        cached_config
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}

/// Per-parent async locks that serialize concurrent `on_child_completed`
/// invocations for the same parent session.
///
/// Race eliminated: when `wait_for=Any` and two child sessions complete
/// simultaneously, both invocations load the parent with
/// `waiting_for_children=Some` before either persists the cleared state, so
/// both pass `wait_policy_satisfied`, both clear `waiting_for_children`, add a
/// duplicate resume message, and call `resume_parent` — a double resume.
/// Holding this per-parent `tokio::sync::Mutex` across the load-check-save
/// critical section makes the second caller observe the already-cleared state.
///
/// The inner `std::sync::Mutex` guards only the brief HashMap lookup/insert
/// (no await inside); the per-parent `tokio::sync::Mutex` is the one held
/// across the async critical section. Entries accumulate but are small
/// (`Arc<tokio::sync::Mutex<()>>` ≈ 24 bytes) and bounded by the number of
/// distinct parent sessions.
fn parent_locks() -> &'static std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>> {
    static LOCKS: OnceLock<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
        OnceLock::new();
    LOCKS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Fetch (or create) the per-session async lock from [`parent_locks`]. Held
/// across the load-check-clear-resume critical section so the three resume
/// sources for one session — child completion, the loop-facing bash **push**
/// ([`BashCompletionSink::on_bash_completed`]), and the bash **backstop** poll
/// ([`ChildCompletionCoordinator::bash_self_resume`]) — can never double-resume.
/// The inner sync `Mutex` guards only the brief map lookup (no await inside).
fn session_resume_lock(session_id: &str) -> Arc<tokio::sync::Mutex<()>> {
    let mut map = parent_locks().lock().recover_poison();
    map.entry(session_id.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

fn wait_policy_satisfied(
    policy: ChildWaitPolicy,
    wait_child_ids: &[String],
    completed_child_ids: &[String],
    latest_status: &str,
) -> bool {
    if wait_child_ids.is_empty() {
        return false;
    }

    match policy {
        ChildWaitPolicy::All => wait_child_ids
            .iter()
            .all(|id| completed_child_ids.iter().any(|completed| completed == id)),
        ChildWaitPolicy::Any => completed_child_ids
            .iter()
            .any(|id| wait_child_ids.iter().any(|wait_id| wait_id == id)),
        ChildWaitPolicy::FirstError => {
            is_error_like(latest_status)
                || wait_child_ids
                    .iter()
                    .all(|id| completed_child_ids.iter().any(|completed| completed == id))
        }
    }
}

/// Extract the child session's last assistant content, if any. Returns `None`
/// when the child produced no assistant message (e.g. errored before the first
/// model response, or only emitted tool messages).
fn child_final_assistant_text(child: &Session) -> Option<String> {
    child
        .messages
        .iter()
        .rev()
        .find(|message| matches!(message.role, Role::Assistant))
        .map(|message| message.content.clone())
        .filter(|content| !content.trim().is_empty())
}

fn runtime_resume_message(
    completion: &ChildCompletion,
    remaining_children: usize,
    child_final_response: Option<&str>,
) -> Message {
    let mut body = format!(
        "Runtime notification: child session `{}` finished with status `{}`. Remaining child sessions: {}.",
        completion.child_session_id, completion.status, remaining_children
    );

    // Fold the child's full final response back into the parent — no
    // truncation. Sub-agents are first-class agents whose complete conclusion
    // should be available to the parent without an extra `SubAgent.get` round
    // trip. The message is left compressible (see `never_compress` below) so a
    // long transcript can still be reclaimed under parent compaction.
    let final_response = child_final_response.map(str::to_string);
    if let Some(response) = final_response.as_deref() {
        body.push_str("\n\nChild final response:\n");
        body.push_str(response);
    } else if let Some(error) = completion.error.as_deref() {
        if !error.is_empty() {
            body.push_str("\n\nChild error:\n");
            body.push_str(error);
        }
    }

    body.push_str(
        "\n\nResume the parent task using this child result and continue from the previous plan. \
         If you need the full child transcript, call SubAgent.get(child_session_id).",
    );

    let mut message = Message::user(body);
    message.metadata = Some(serde_json::json!({
        RUNTIME_RESUME_MESSAGE_HIDDEN_KEY: true,
        RUNTIME_RESUME_MESSAGE_KIND_KEY: "child_completion_resume",
        "child_session_id": completion.child_session_id,
        "child_status": completion.status,
        "child_error": completion.error,
        "completed_at": completion.completed_at,
        "child_final_response_included": final_response.is_some(),
    }));
    // Allow parent-side compaction to reclaim this (now untruncated) message if
    // the parent context grows — important once children nest and fold full
    // results upward. The `SubAgent.get` hint preserves recoverability.
    message.never_compress = false;
    message
}

/// The hidden resume message for a completed **guardian** review: a directive,
/// verdict-tailored note that carries the reviewer's findings straight into the
/// parent (so it can act without a `SubAgent.get`), mirroring
/// [`runtime_resume_message`]'s hidden/compressible shape.
fn guardian_resume_message(completion: &ChildCompletion, verdict: &GuardianVerdict) -> Message {
    let mut body = if verdict.approve {
        String::from(
            "Guardian review APPROVED: an independent reviewer verified the work and found no blocking issues. You may finalize the task.",
        )
    } else {
        String::from(
            "Guardian review REJECTED: an independent reviewer found issues. Address every finding below before completing — do NOT declare the task complete until they are resolved.",
        )
    };
    if let Some(summary) = verdict.summary.as_deref().filter(|s| !s.trim().is_empty()) {
        body.push_str("\n\nReviewer summary: ");
        body.push_str(summary);
    }
    if !verdict.findings.is_empty() {
        body.push_str("\n\nFindings:");
        for (idx, finding) in verdict.findings.iter().enumerate() {
            body.push_str(&format!("\n{}. {}", idx + 1, finding));
        }
    }
    body.push_str(
        "\n\nIf you need the full guardian transcript, call SubAgent.get(child_session_id).",
    );

    let mut message = Message::user(body);
    message.metadata = Some(serde_json::json!({
        RUNTIME_RESUME_MESSAGE_HIDDEN_KEY: true,
        RUNTIME_RESUME_MESSAGE_KIND_KEY: "guardian_review_resume",
        "child_session_id": completion.child_session_id,
        "child_status": completion.status,
        "guardian_approved": verdict.approve,
        "completed_at": completion.completed_at,
    }));
    message.never_compress = false;
    message
}

#[derive(Clone)]
pub struct ChildCompletionCoordinator {
    storage: Arc<dyn Storage>,
    persistence: Arc<bamboo_storage::LockedSessionStore>,
    sessions: crate::SessionCache,
    agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    agent: Arc<Agent>,
    config: Arc<RwLock<Config>>,
    provider_registry: Arc<ProviderRegistry>,
    provider_router: Arc<ProviderModelRouter>,
    app_data_dir: std::path::PathBuf,
    account_feed_inbox: Option<crate::execution::AccountFeedInbox>,
    root_tools: Arc<RwLock<Option<Arc<dyn ToolExecutor>>>>,
    /// Late-bound guardian reviewer spawner, set post-construction by the server
    /// (mirrors `root_tools`). Re-injected into resumed runs so a guardian's
    /// reject→fix verdict can be re-reviewed across the suspend/resume boundary.
    guardian_spawner: Arc<RwLock<Option<Arc<dyn GuardianSpawner>>>>,
}

impl ChildCompletionCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Arc<dyn Storage>,
        persistence: Arc<LockedSessionStore>,
        sessions: crate::SessionCache,
        agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
        session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
        agent: Arc<Agent>,
        config: Arc<RwLock<Config>>,
        provider_registry: Arc<ProviderRegistry>,
        provider_router: Arc<ProviderModelRouter>,
        app_data_dir: std::path::PathBuf,
        account_feed_inbox: Option<crate::execution::AccountFeedInbox>,
    ) -> Self {
        Self {
            storage,
            persistence,
            sessions,
            agent_runners,
            session_event_senders,
            agent,
            config,
            provider_registry,
            provider_router,
            app_data_dir,
            account_feed_inbox,
            root_tools: Arc::new(RwLock::new(None)),
            guardian_spawner: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn set_root_tools(&self, tools: Arc<dyn ToolExecutor>) {
        *self.root_tools.write().await = Some(tools);
    }

    /// Wire the guardian reviewer spawner (server-provided), so resumed runs can
    /// re-spawn a guardian to re-review a fix after a reject verdict.
    pub async fn set_guardian_spawner(&self, spawner: Arc<dyn GuardianSpawner>) {
        *self.guardian_spawner.write().await = Some(spawner);
    }

    fn build_resume_config(
        &self,
        session: &Session,
        config_snapshot: &Config,
    ) -> ResumeConfigSnapshot {
        crate::session_app::resolution::resolve_resume_config_snapshot(
            config_snapshot,
            &self.provider_registry,
            session,
            None,
        )
    }

    /// Drive a parent-resume and return the final [`ResumeOutcome`] so callers
    /// can distinguish a successful spawn (`Started`) from a gate-blocked
    /// attempt (`Completed`). The bash self-resume poll task uses this to
    /// detect the finalize-clobber case — its appended resume message was
    /// reverted by the suspending runner's final `merge_save_runtime`, so the
    /// resume port's `has_pending_user_message` gate fails and nothing spawns —
    /// and retry the clear→append→resume (see [`Self::bash_self_resume`]).
    async fn resume_parent(&self, parent_session_id: String) -> ResumeOutcome {
        for attempt in 0..=5u8 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(250 * attempt as u64)).await;
            }

            let Some(session) = self.load_session(&parent_session_id).await else {
                tracing::warn!(%parent_session_id, "cannot resume parent after child completion: session not found");
                return ResumeOutcome::NotFound;
            };
            let config_snapshot = self.config.read().await.clone();
            let resume_config = self.build_resume_config(&session, &config_snapshot);
            let outcome = resume_session_execution(self, &parent_session_id, resume_config).await;
            tracing::info!(
                %parent_session_id,
                attempt,
                outcome = outcome.as_str(),
                "child completion requested parent resume"
            );

            if !matches!(outcome, ResumeOutcome::AlreadyRunning { .. }) {
                return outcome;
            }
        }
        // Exhausted the AlreadyRunning retry budget; surface the final state.
        ResumeOutcome::AlreadyRunning {
            run_id: String::new(),
        }
    }

    async fn save_and_cache(&self, session: &mut Session) {
        if let Err(error) = self.persistence.merge_save_runtime(session).await {
            tracing::warn!(session_id = %session.id, %error, "failed to persist session");
        }
        self.sessions.insert(
            session.id.clone(),
            Arc::new(parking_lot::RwLock::new(session.clone())),
        );
    }
}

#[async_trait]
impl ChildCompletionHandler for ChildCompletionCoordinator {
    async fn on_child_completed(&self, completion: ChildCompletion) {
        // Acquire the per-session async lock to eliminate the concurrent
        // double-resume race (see `parent_locks` for the full scenario). The
        // inner std::sync::Mutex is released immediately so no sync lock is
        // held across the await that follows.
        let per_parent = session_resume_lock(&completion.parent_session_id);
        let _per_parent_guard = per_parent.lock().await;

        let Some(mut parent) = self.load_session(&completion.parent_session_id).await else {
            tracing::warn!(
                parent_session_id = %completion.parent_session_id,
                child_session_id = %completion.child_session_id,
                "child completion received for missing parent"
            );
            return;
        };

        // A parent may itself be a child (nested sub-agents): the rest of this
        // handler is kind-agnostic — it operates on `completion.parent_session_id`,
        // inspects that session's own `waiting_for_children` runtime state, and
        // resumes it. (Previously this bailed unless the parent was Root, which
        // silently dropped grandchild completions.)
        let mut runtime_state = read_runtime_state(&parent);

        // Single source of truth: reconstruct the completed-child set from the
        // session index rather than from a denormalized copy on the parent file.
        let completed_child_ids = derive_completed_child_ids(
            &self.storage,
            &completion.parent_session_id,
            &completion.child_session_id,
        )
        .await;

        let mut should_resume = false;
        let mut remaining_children = 0usize;
        if let Some(wait) = runtime_state.waiting_for_children.clone() {
            remaining_children = wait
                .child_session_ids
                .iter()
                .filter(|id| !completed_child_ids.iter().any(|completed| completed == *id))
                .count();
            should_resume = wait_policy_satisfied(
                wait.wait_for,
                &wait.child_session_ids,
                &completed_child_ids,
                &completion.status,
            );
            if should_resume {
                runtime_state.waiting_for_children = None;
                runtime_state.status = AgentStatusState::Idle;
                runtime_state.suspension = None;
            }
        }

        if should_resume {
            parent.metadata.remove("runtime.suspend_reason");
            // Load the completed child once. The guardian branch inspects its
            // subagent_type + final verdict; the generic path folds its final
            // assistant content into the hidden resume message (avoiding an extra
            // `SubAgent.get` round trip after resume).
            let loaded_child = match self
                .storage
                .load_session(&completion.child_session_id)
                .await
            {
                Ok(child) => child,
                Err(error) => {
                    tracing::warn!(
                        child_session_id = %completion.child_session_id,
                        %error,
                        "failed to load child session for runtime resume message"
                    );
                    None
                }
            };

            // Guardian branch: a completing guardian reviewer that matches the
            // parent's recorded review advances GuardianState (phase → Reviewed)
            // and resumes with a verdict-tailored, findings-carrying message. Any
            // id mismatch or unparseable verdict falls through to the generic
            // resume, so the parent is never stranded.
            let reviewed_round = runtime_state.round.current_round;
            let guardian_resume = loaded_child.as_ref().and_then(|child| {
                if child.subagent_type().as_deref() != Some("guardian") {
                    return None;
                }
                let mut guardian_state = read_guardian_state(&parent)?;
                if guardian_state.guardian_child_id.as_deref()
                    != Some(completion.child_session_id.as_str())
                {
                    // A *different* guardian is legitimately still in flight —
                    // leave its Pending state intact and use the generic resume.
                    tracing::warn!(
                        parent_session_id = %completion.parent_session_id,
                        child_session_id = %completion.child_session_id,
                        expected = ?guardian_state.guardian_child_id,
                        "guardian completion does not match recorded guardian_child_id; using generic resume"
                    );
                    return None;
                }
                // This IS the guardian we dispatched, so we MUST advance the
                // phase out of `Pending` — otherwise the next terminal gate's
                // `Pending => return None` would let the run complete unreviewed.
                // A reviewer that errored or produced unparseable output is
                // treated as a SYNTHETIC REJECT (never a silent pass), so the
                // budgeted re-review loop governs the outcome: fail-closed, but
                // still bounded by `max_reviews`.
                let verdict = child_final_assistant_text(child)
                    .and_then(|text| match parse_guardian_verdict(&text) {
                        Ok(verdict) => Some(verdict),
                        Err(error) => {
                            tracing::warn!(
                                child_session_id = %completion.child_session_id,
                                %error,
                                "guardian verdict unparseable; recording a synthetic reject"
                            );
                            None
                        }
                    })
                    .unwrap_or_else(|| {
                        GuardianVerdict::rejected(vec![
                            "The guardian reviewer did not return a usable verdict (it errored or \
                             emitted unparseable output); the work has NOT been independently \
                             verified."
                                .to_string(),
                        ])
                    });
                let approved = verdict.approve;
                let message = guardian_resume_message(&completion, &verdict);
                guardian_state.record_verdict(verdict, reviewed_round);
                write_guardian_state(&mut parent, guardian_state);
                tracing::info!(
                    parent_session_id = %completion.parent_session_id,
                    child_session_id = %completion.child_session_id,
                    approved,
                    "guardian verdict recorded; resuming parent"
                );
                Some(message)
            });

            let resume_message = guardian_resume.unwrap_or_else(|| {
                runtime_resume_message(
                    &completion,
                    remaining_children,
                    loaded_child
                        .as_ref()
                        .and_then(child_final_assistant_text)
                        .as_deref(),
                )
            });
            parent.add_message(resume_message);
        } else if runtime_state.waiting_for_children.is_some() {
            runtime_state.status = AgentStatusState::Suspended;
            runtime_state.suspension = Some(SuspensionState {
                reason: "waiting_for_children".to_string(),
                suspended_at: Utc::now(),
                resumable: true,
                hook_point: Some("ChildCompletion".to_string()),
            });
        }

        parent.updated_at = Utc::now();
        write_runtime_state(&mut parent, &runtime_state);
        self.save_and_cache(&mut parent).await;

        // Capture before releasing the per-parent lock so the borrow checker
        // is satisfied; `resume_parent` has its own retry loop and should not
        // hold the per-parent lock (it would block other completions for the
        // same parent, and the state is already durably settled above).
        let resume_parent_id = parent.id.clone();
        drop(_per_parent_guard);

        if should_resume {
            self.resume_parent(resume_parent_id).await;
        }
    }
}

#[async_trait]
impl ResumeExecutionPort for ChildCompletionCoordinator {
    async fn load_session(&self, session_id: &str) -> Option<Session> {
        match self.storage.load_session(session_id).await {
            Ok(Some(session)) => Some(session),
            Ok(None) => self
                .sessions
                .get(session_id)
                .map(|e| e.value().clone())
                .map(|arc| arc.read().clone()),
            Err(error) => {
                tracing::warn!(%session_id, %error, "failed to load session from storage");
                self.sessions
                    .get(session_id)
                    .map(|e| e.value().clone())
                    .map(|arc| arc.read().clone())
            }
        }
    }

    async fn save_and_cache_session(&self, session: &mut Session) {
        self.save_and_cache(session).await;
    }

    async fn try_reserve_runner(
        &self,
        session_id: &str,
        event_sender: &broadcast::Sender<AgentEvent>,
    ) -> Option<RunnerReservation> {
        try_reserve_runner(
            &self.agent_runners,
            &self.session_event_senders,
            session_id,
            event_sender,
        )
        .await
    }

    async fn get_existing_runner_run_id(&self, session_id: &str) -> Option<String> {
        let runners = self.agent_runners.read().await;
        runners.get(session_id).map(|r| r.run_id.clone())
    }

    async fn get_or_create_event_sender(&self, session_id: &str) -> broadcast::Sender<AgentEvent> {
        crate::execution::session_events::get_or_create_event_sender(
            &self.session_event_senders,
            session_id,
        )
        .await
    }

    async fn spawn_resume_execution(&self, request: ResumeSpawnRequest) {
        let ResumeSpawnRequest {
            session_id,
            session,
            cancel_token,
            run_id: _,
            event_sender,
            config,
        } = request;

        let Some(root_tools) = self.root_tools.read().await.clone() else {
            tracing::error!(%session_id, "cannot resume parent after child completion: root tool surface is not initialized");
            return;
        };

        let model = session.model.clone();
        let resolved_provider_name = session_effective_model_ref(&session)
            .map(|model_ref| model_ref.provider)
            .unwrap_or(config.provider_name);
        let provider_override = session_effective_model_ref(&session)
            .and_then(|model_ref| match self.provider_router.route(&model_ref) {
                Ok(provider) => Some(provider),
                Err(error) => {
                    tracing::warn!(
                        session_id = %session_id,
                        provider = %model_ref.provider,
                        model = %model_ref.model,
                        error = %error,
                        "failed to resolve provider override for child-completion parent resume; falling back to runtime provider"
                    );
                    None
                }
            });
        let config_snapshot = self.config.read().await.clone();
        let resolved_fast_provider = resolve_fast_model(
            &config_snapshot,
            &resolved_provider_name,
            &self.provider_registry,
        )
        .map(|model| model.provider);
        let reasoning_effort = session.reasoning_effort;
        let reasoning_effort_source = session
            .metadata
            .get("reasoning_effort_source")
            .cloned()
            .unwrap_or_default();
        let gold_config = resolve_gold_config(
            &config_snapshot,
            session
                .metadata
                .get(GOLD_CONFIG_METADATA_KEY)
                .map(String::as_str),
        )
        .or(config.gold_config.clone());

        let (mpsc_tx, _forwarder) = create_event_forwarder(
            session_id.clone(),
            event_sender,
            self.agent_runners.clone(),
            self.account_feed_inbox.clone(),
        );

        let config_handle = self.config.clone();
        let cached_config = Arc::new(StdRwLock::new(config_snapshot.clone()));
        let provider_registry = self.provider_registry.clone();
        let provider_name_for_aux = resolved_provider_name.clone();
        let auxiliary_model_resolver = std::sync::Arc::new(move || {
            let config_snapshot = read_config_snapshot(&config_handle, cached_config.as_ref());
            // Auxiliary models are global (config-derived), never session-bound.
            let areas = resolve_global_area_models(
                &config_snapshot,
                &provider_name_for_aux,
                &provider_registry,
            );
            crate::AuxiliaryModelConfig {
                fast_model_name: areas.fast.as_ref().map(|m| m.model_name.clone()),
                fast_model_provider: areas.fast.map(|m| m.provider),
                background_model_name: areas.background.as_ref().map(|m| m.model_name.clone()),
                planning_model_name: None,
                search_model_name: None,
                summarization_model_name: areas
                    .summarization
                    .as_ref()
                    .map(|m| m.model_name.clone()),
                background_model_provider: areas.background.map(|m| m.provider),
                summarization_model_provider: areas.summarization.map(|m| m.provider),
            }
        });
        let model_roster = crate::ModelRoster {
            model: Some(model),
            provider_name: Some(resolved_provider_name),
            provider_type: config.provider_type.clone(),
            fast: crate::RoleModel::from_parts(config.fast_model, resolved_fast_provider),
            background: crate::RoleModel::from_parts(
                config.background_model,
                config.background_model_provider,
            ),
            summarization: crate::RoleModel::from_parts(
                config.summarization_model,
                config.summarization_model_provider,
            ),
        };

        // Re-inject guardian state on resume so a reject→fix verdict can be
        // re-reviewed: config from the session (persisted at first spawn),
        // spawner from the coordinator-held handle. Absent guardian config this
        // stays `None`, and the approve→complete path is unchanged.
        let guardian_config = read_guardian_config(&session);
        let guardian_spawner = self.guardian_spawner.read().await.clone();

        spawn_session_execution(SessionExecutionArgs {
            agent: self.agent.clone(),
            session_id,
            session,
            tools_override: Some(root_tools),
            provider_override,
            model_roster,
            reasoning_effort,
            reasoning_effort_source,
            auxiliary_model_resolver: Some(auxiliary_model_resolver),
            // Resumed child runs keep the spawn-time disabled snapshot (#136 lives
            // on the long-running main agent path; children are short-lived).
            disabled_filter_resolver: None,
            disabled_tools: Some(config.disabled_tools),
            disabled_skill_ids: Some(config.disabled_skill_ids),
            selected_skill_ids: None,
            selected_skill_mode: None,
            cancel_token,
            mpsc_tx,
            image_fallback: config.image_fallback,
            gold_config,
            guardian_config,
            guardian_spawner,
            bash_resume_hook: {
                let hook: Arc<dyn BashResumeHook> = Arc::new(self.clone());
                Some(hook)
            },
            bash_completion_sink: {
                // Resumed runs keep the push wired too, so a background shell
                // launched after resume still notifies the loop.
                let sink: Arc<dyn BashCompletionSink> = Arc::new(self.clone());
                Some(sink)
            },
            app_data_dir: Some(self.app_data_dir.clone()),
            // Resume does not carry a fresh per-request override; the
            // config-level default (issue #221) still applies.
            run_budget: None,
            runners: self.agent_runners.clone(),
            sessions_cache: self.sessions.clone(),
            on_complete: None,
        });
    }
}

/// Hidden resume message for a bash-completion self-resume (issue #84 Phase 2b).
/// Mirrors [`runtime_resume_message`]'s hidden/compressible shape so the resume
/// port's `has_pending_user_message` gate is satisfied.
///
/// `timed_out` selects the wording: the normal path (all shells finished)
/// announces completion; the deadline path (the 6h+10m wait ceiling was hit with
/// shells STILL running) must NOT claim the shells completed — it says they may
/// still be running so the model verifies with BashOutput instead of assuming
/// success on a false premise.
fn bash_completion_resume_message(bash_ids: &[String], timed_out: bool) -> Message {
    let body = if timed_out {
        format!(
            "Runtime notification: the background-Bash wait ceiling was reached while one or more \
             shell(s) ({}) may still be running. The session is being resumed so it is not \
             stranded; verify their actual status with BashOutput before assuming completion.",
            bash_ids.join(", ")
        )
    } else {
        format!(
            "Runtime notification: all background Bash shell(s) ({}) have completed. \
             Review their output with BashOutput and resume the task from where you left off.",
            bash_ids.join(", ")
        )
    };
    let mut message = Message::user(body);
    message.metadata = Some(serde_json::json!({
        RUNTIME_RESUME_MESSAGE_HIDDEN_KEY: true,
        RUNTIME_RESUME_MESSAGE_KIND_KEY: BASH_COMPLETION_RESUME_KIND,
    }));
    message.never_compress = false;
    message
}

/// Decide whether the bash self-resume should retry its clear→append→resume
/// sequence after a resume attempt returned `outcome`, given that the persisted
/// bash wait is (`true`) / is not (`false`) still set on reload.
///
/// Retry **only** when the resume did NOT spawn (`Completed` — no pending user
/// message, i.e. our resume message was dropped — or `AlreadyRunning`) AND the
/// persisted bash wait is still set: the signature of the finalize-clobber, where
/// the suspending runner's one-shot final `merge_save_runtime` lands after our
/// save and reverts `waiting_for_bash=Some` while dropping our resume message, so
/// `has_pending_user_message` fails and nothing spawns. `Started` (resume fired)
/// and `NotFound` (session gone) never retry. Pure helper so the clobber
/// detection is unit-testable in isolation from async I/O.
fn bash_resume_should_retry(outcome: &ResumeOutcome, persisted_waiting_for_bash: bool) -> bool {
    match outcome {
        ResumeOutcome::Started { .. } | ResumeOutcome::NotFound => false,
        ResumeOutcome::Completed | ResumeOutcome::AlreadyRunning { .. } => {
            persisted_waiting_for_bash
        }
    }
}

/// Whether a background-shell completion push should **resume** the owning loop
/// (vs merely enqueue an injection). Resume only when the loop is actually
/// suspended on a bash wait AND every shell it was waiting on has now finished —
/// resuming while other waited shells are still running would drop them back into
/// a foreground turn prematurely. The last shell to finish (or the backstop)
/// drives the resume; earlier ones enqueue their notice. Pure so the invariant is
/// unit-testable in isolation.
fn bash_completion_should_resume(
    loop_suspended_on_bash: bool,
    all_waited_shells_done: bool,
) -> bool {
    loop_suspended_on_bash && all_waited_shells_done
}

/// Apply the bash-resume state transition to a loaded session **in place**: clear
/// the `waiting_for_bash` wait, mark the runtime Idle, drop the suspension +
/// `runtime.suspend_reason`, and append `resume_message`. Returns `false` (a
/// no-op) when the session was not actually waiting on bash — the double-resume
/// guard shared by the push and the backstop. Pure (no I/O) so both
/// [`ChildCompletionCoordinator::perform_bash_resume`] and unit tests exercise the
/// exact same transition.
fn apply_bash_resume_transition(session: &mut Session, resume_message: &Message) -> bool {
    let mut runtime_state = read_runtime_state(session);
    if runtime_state.waiting_for_bash.is_none() {
        return false;
    }
    runtime_state.waiting_for_bash = None;
    runtime_state.status = AgentStatusState::Idle;
    runtime_state.suspension = None;
    write_runtime_state(session, &runtime_state);
    session.metadata.remove("runtime.suspend_reason");
    session.add_message(resume_message.clone());
    true
}

/// Bash self-resume support (issue #84 Phase 2b; push follow-up).
impl ChildCompletionCoordinator {
    /// **Backstop** for a session suspended on `waiting_for_bash`. The primary,
    /// event-driven wake is the loop-facing push
    /// ([`BashCompletionSink::on_bash_completed`] → [`Self::deliver_bash_completion`]):
    /// the shell's completion task fires it the instant the process exits, and it
    /// resumes the loop directly. This task exists ONLY to catch a **lost push** —
    /// the completion landing in the window before the suspend was persisted (so
    /// the push saw no `waiting_for_bash` and only queued an injection), or a
    /// configuration with no sink wired — and to honour the wait ceiling.
    ///
    /// So it is deliberately NOT a hot spin: a coarse backoff (1 s → 30 s) that
    /// **yields to the push**. In the happy path the push has already cleared
    /// `waiting_for_bash` before the first check fires, so this returns after one
    /// cheap load with no registry polling at all. It only performs a resume when
    /// the shell(s) have finished but the loop is somehow still suspended, or the
    /// 6 h wait ceiling is reached.
    async fn bash_self_resume(&self, session_id: String, bash_ids: Vec<String>) {
        let mut delay = Duration::from_secs(1);
        let max_delay = Duration::from_secs(30);
        // Hard ceiling: the wait lease (6 h) + the registry GC TTL (5 min) +
        // margin. After this the shells are gone from the registry regardless,
        // so force-resume to avoid stranding the session on a GC edge case.
        let max_poll = Duration::from_secs(6 * 3600 + 600);
        let deadline = tokio::time::Instant::now() + max_poll;

        loop {
            tokio::time::sleep(delay).await;

            let Some(session) = self.load_session(&session_id).await else {
                tracing::info!(%session_id, "bash self-resume backstop: session gone; nothing to do");
                return;
            };
            if read_runtime_state(&session).waiting_for_bash.is_none() {
                // The push (or another path) already resumed. This is the common
                // case — the backstop yields silently after a single load.
                return;
            }

            let still_running =
                bamboo_tools::tools::bash_runtime::running_shells_for_session(&session_id);
            let timed_out = tokio::time::Instant::now() >= deadline;
            if still_running.is_empty() || timed_out {
                // The shell(s) finished but the loop is still suspended → the push
                // was lost (pre-persist window / no sink), or the ceiling hit.
                // Resume under the shared per-session lock so we never race the
                // push or a concurrent child-completion resume.
                let guard = session_resume_lock(&session_id);
                let _held = guard.lock().await;
                tracing::warn!(
                    %session_id,
                    shell_count = bash_ids.len(),
                    timed_out,
                    "bash self-resume backstop engaged (push lost or wait ceiling reached)"
                );
                self.perform_bash_resume(
                    &session_id,
                    bash_completion_resume_message(&bash_ids, timed_out),
                )
                .await;
                return;
            }

            delay = (delay * 2).min(max_delay);
        }
    }

    /// Clear a session's `waiting_for_bash` state, append `resume_message`, and
    /// drive the parent resume — the shared clear→append→resume used by BOTH the
    /// event-driven push ([`Self::deliver_bash_completion`]) and the backstop poll
    /// ([`Self::bash_self_resume`]).
    ///
    /// **The caller MUST hold the [`session_resume_lock`] for `session_id`** so the
    /// load-check-clear-resume critical section is serialized against every other
    /// resume source (no double resume). No-op when the persisted wait was already
    /// cleared (another source handled it first).
    ///
    /// The clear→append→resume is a **bounded retry loop** that closes the
    /// finalize-clobber strand. The suspending runner's `finalize_task_context`
    /// runs a full `save_runtime_session` (same `merge_save_runtime`, which
    /// overwrites the whole `messages` array) that can land AFTER our save,
    /// reverting `waiting_for_bash=Some` and dropping our resume message, so
    /// `has_pending_user_message` fails and `resume_parent` returns `Completed`
    /// without spawning. We detect that (persisted wait still set after a
    /// non-`Started` outcome) and re-clear/re-append/re-resume. It converges
    /// because the runner's finalize persist is one-shot: once landed, our retry's
    /// save is the last writer, the message sticks, and resume fires.
    async fn perform_bash_resume(&self, session_id: &str, resume_message: Message) {
        let retry_backoff = Duration::from_millis(200);
        const MAX_RESUME_ATTEMPTS: u8 = 5;
        for attempt in 0..MAX_RESUME_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(retry_backoff).await;
            }

            let Some(mut session) = self.load_session(session_id).await else {
                tracing::warn!(%session_id, "bash resume: session not found; nothing to resume");
                return;
            };

            if !apply_bash_resume_transition(&mut session, &resume_message) {
                // Double-resume guard: the wait was already cleared by another
                // source (the push, the backstop, or a user-driven resume). Do
                // not append a duplicate message or request a redundant resume.
                tracing::info!(
                    %session_id, attempt,
                    "bash resume: persisted bash wait already cleared; nothing to resume"
                );
                return;
            }
            session.updated_at = Utc::now();
            self.save_and_cache(&mut session).await;
            tracing::info!(
                %session_id, attempt,
                "bash resume: cleared bash wait and appended resume message"
            );

            let outcome = self.resume_parent(session_id.to_string()).await;
            match outcome {
                ResumeOutcome::Started { .. } => {
                    tracing::info!(%session_id, attempt, "bash resume: resume fired");
                    return;
                }
                ResumeOutcome::NotFound => {
                    tracing::warn!(%session_id, "bash resume: session vanished during resume");
                    return;
                }
                _ => {
                    // Completed (no pending user message ⇒ our resume message was
                    // dropped by the runner's finalize persist) or AlreadyRunning.
                    // Decide via the persisted bash wait: still set ⇒
                    // finalize-clobber ⇒ retry; cleared ⇒ the session is being
                    // handled (by us or a concurrent resume) ⇒ stop.
                    let clobbered = match self.load_session(session_id).await {
                        Some(reloaded) => read_runtime_state(&reloaded).waiting_for_bash.is_some(),
                        None => {
                            tracing::warn!(%session_id, "bash resume: session vanished after resume");
                            return;
                        }
                    };
                    if bash_resume_should_retry(&outcome, clobbered) {
                        tracing::warn!(
                            %session_id, attempt,
                            outcome = outcome.as_str(),
                            "bash resume: persisted wait still set after resume (finalize-clobber); retrying"
                        );
                        continue;
                    }
                    tracing::info!(
                        %session_id, attempt,
                        outcome = outcome.as_str(),
                        "bash resume: wait cleared and resume handled; stopping"
                    );
                    return;
                }
            }
        }

        tracing::warn!(
            %session_id,
            attempts = MAX_RESUME_ATTEMPTS,
            "bash resume: exhausted clobber-retry budget without confirming resume; giving up"
        );
    }
}

impl BashResumeHook for ChildCompletionCoordinator {
    fn arrange_bash_self_resume(&self, session_id: String, bash_ids: Vec<String>) {
        let coordinator = Arc::new(self.clone());
        tokio::spawn(async move {
            coordinator.bash_self_resume(session_id, bash_ids).await;
        });
    }
}

/// Build the injected user-message body for a completed background shell — a
/// concise notice plus a bounded output tail — so the model can act on the
/// result without a mandatory `BashOutput` round-trip (issue #84 Phase 2b
/// follow-up).
fn bash_completion_injection_body(info: &BashCompletionInfo) -> String {
    let exit = match info.exit_code {
        Some(code) => code.to_string(),
        None => "none (signal/killed)".to_string(),
    };
    let mut body = format!(
        "Runtime notification: background shell `{}` (`{}`) finished — status {}, exit code {}.",
        info.bash_id, info.command, info.status, exit
    );
    if info.output_tail.trim().is_empty() {
        body.push_str(" It produced no captured output.");
    } else {
        body.push_str("\n\nOutput tail:\n");
        body.push_str(&info.output_tail);
    }
    body.push_str(&format!(
        "\n\nUse BashOutput with bash_id=\"{}\" for the full output, then continue the task.",
        info.bash_id
    ));
    body
}

/// Loop-facing background-Bash completion delivery (issue #84 Phase 2b
/// follow-up). Pushes a completed shell's result into its owning session's loop,
/// mirroring how a sub-agent completion reaches its parent — but via the
/// running-loop channel, which children never exercise (a parent waiting on
/// children is always suspended when one completes; bash is the first completion
/// source that can land on a *live, iterating* loop).
///
/// The push enqueues onto `pending_injected_messages`, the same round-boundary
/// steering channel `send_message` uses. That covers every reachable loop state:
/// an actively-looping session drains it at its next round
/// (`merge_pending_injected_messages`); a session suspended on `waiting_for_bash`
/// drains it when the durable end-of-turn poll backstop (`bash_resume_hook`)
/// resumes it at round 0. A wired-sink session is never idle-with-a-running-shell
/// (ending a turn with one suspends), so no separate idle-wake path is needed —
/// keeping the push a pure latency optimization that never races the backstop.
/// Enqueue a completed shell's summary as a pending injected message on the
/// owning session. Race-safe: `update_runtime_config` loads the freshest session
/// under the per-session lock and re-saves, so it can never revert a message the
/// live loop appended concurrently — unlike `merge_save_runtime` (which writes
/// the caller's whole `messages` snapshot verbatim). Free fn so it is unit-
/// testable without constructing a full coordinator. Returns the saved session,
/// or `None` if the owning session no longer exists.
async fn enqueue_bash_completion_injection(
    persistence: &LockedSessionStore,
    info: &BashCompletionInfo,
) -> std::io::Result<Option<Session>> {
    let body = bash_completion_injection_body(info);
    let queued = serde_json::json!({
        "content": body,
        "created_at": Utc::now(),
    });
    persistence
        .update_runtime_config(&info.session_id, move |session| {
            let mut pending = session.pending_injected_messages().unwrap_or_default();
            pending.push(queued);
            session.set_pending_injected_messages(pending);
        })
        .await
}

/// Build the hidden, compressible resume message for a completed background
/// shell — the same rich notice body used for a live-loop injection
/// ([`bash_completion_injection_body`]), but tagged as a resume message so it
/// satisfies the `has_pending_user_message` gate that lets a suspended session
/// spawn. This is what the **push** appends when it wakes a suspended loop, so
/// the model gets the shell's status + output tail in one shot without a
/// separate `BashOutput` round-trip.
fn bash_resume_message_from_info(info: &BashCompletionInfo) -> Message {
    let mut message = Message::user(bash_completion_injection_body(info));
    message.metadata = Some(serde_json::json!({
        RUNTIME_RESUME_MESSAGE_HIDDEN_KEY: true,
        RUNTIME_RESUME_MESSAGE_KIND_KEY: BASH_COMPLETION_RESUME_KIND,
    }));
    message.never_compress = false;
    message
}

impl ChildCompletionCoordinator {
    /// Loop-facing delivery of a completed background shell. Two paths, chosen
    /// under the per-session resume lock so we never race the backstop poll or a
    /// concurrent child-completion resume:
    ///
    /// - **Suspended loop** (the model ended its turn with the shell running, so
    ///   `waiting_for_bash` is set) AND every waited shell has now finished →
    ///   **resume the loop directly**, event-driven, appending the rich completion
    ///   notice as the resume message. This is the push's whole point: no polling.
    /// - Otherwise (a live/iterating loop, or a suspend still waiting on OTHER
    ///   shells) → **enqueue** the notice as a pending injected message, drained at
    ///   the next round boundary (a live loop) or folded into the eventual resume
    ///   when the last shell finishes.
    async fn deliver_bash_completion(&self, info: BashCompletionInfo) {
        let guard = session_resume_lock(&info.session_id);
        let _held = guard.lock().await;

        let Some(session) = self.load_session(&info.session_id).await else {
            tracing::warn!(
                session_id = %info.session_id,
                bash_id = %info.bash_id,
                "background bash completion: owning session not found; nothing to notify"
            );
            return;
        };

        let waiting = read_runtime_state(&session).waiting_for_bash.is_some();
        // The producer flips the shell's `running` flag false BEFORE firing this
        // push, so a now-empty per-session registry means every shell the loop was
        // waiting on has finished — safe to resume. If OTHER waited shells are
        // still running, fall through to the enqueue path and let the last one (or
        // the backstop) drive the resume.
        let all_shells_done =
            bamboo_tools::tools::bash_runtime::running_shells_for_session(&info.session_id)
                .is_empty();

        if bash_completion_should_resume(waiting, all_shells_done) {
            tracing::info!(
                session_id = %info.session_id,
                bash_id = %info.bash_id,
                status = %info.status,
                "background bash completion: push-resuming suspended loop (event-driven)"
            );
            self.perform_bash_resume(&info.session_id, bash_resume_message_from_info(&info))
                .await;
            return;
        }

        match enqueue_bash_completion_injection(&self.persistence, &info).await {
            Ok(Some(_)) => tracing::info!(
                session_id = %info.session_id,
                bash_id = %info.bash_id,
                status = %info.status,
                waiting,
                "background bash completion queued for injection at the next round boundary"
            ),
            Ok(None) => tracing::warn!(
                session_id = %info.session_id,
                bash_id = %info.bash_id,
                "background bash completion: owning session not found; nothing to notify"
            ),
            Err(error) => tracing::warn!(
                session_id = %info.session_id,
                bash_id = %info.bash_id,
                %error,
                "background bash completion: failed to queue injection"
            ),
        }
    }
}

impl BashCompletionSink for ChildCompletionCoordinator {
    fn on_bash_completed(&self, info: BashCompletionInfo) {
        // Best-effort, off the shell's completion-poll task: hand the delivery to
        // a detached task so the producer is never blocked (mirrors
        // `arrange_bash_self_resume`).
        let coordinator = Arc::new(self.clone());
        tokio::spawn(async move {
            coordinator.deliver_bash_completion(info).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Message;

    fn make_completion(status: &str) -> ChildCompletion {
        ChildCompletion {
            parent_session_id: "parent-1".to_string(),
            child_session_id: "child-1".to_string(),
            status: status.to_string(),
            error: None,
            completed_at: Utc::now(),
        }
    }

    // ── ② derive completed children from the index ──────────────────────

    struct StubChildIndex {
        children: Vec<(String, Option<String>)>,
    }

    #[async_trait]
    impl Storage for StubChildIndex {
        async fn save_session(&self, _session: &Session) -> std::io::Result<()> {
            Ok(())
        }
        async fn load_session(&self, _id: &str) -> std::io::Result<Option<Session>> {
            Ok(None)
        }
        async fn delete_session(&self, _id: &str) -> std::io::Result<bool> {
            Ok(false)
        }
        async fn list_child_run_statuses(
            &self,
            _parent_session_id: &str,
        ) -> std::io::Result<Vec<(String, Option<String>)>> {
            Ok(self.children.clone())
        }
    }

    #[tokio::test]
    async fn derive_completed_only_includes_terminal_children() {
        let storage: Arc<dyn Storage> = Arc::new(StubChildIndex {
            children: vec![
                ("a".into(), Some("completed".into())),
                ("b".into(), Some("running".into())),
                ("c".into(), Some("error".into())),
                ("d".into(), None),
            ],
        });
        let completed = derive_completed_child_ids(&storage, "parent-1", "b").await;
        // Terminal from index: a, c. Plus the just-completed child b folded in.
        assert_eq!(
            completed,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[tokio::test]
    async fn derive_completed_folds_in_just_completed_when_index_lags() {
        // Index hasn't caught up — reports the child as still running.
        let storage: Arc<dyn Storage> = Arc::new(StubChildIndex {
            children: vec![("only".into(), Some("running".into()))],
        });
        let completed = derive_completed_child_ids(&storage, "parent-1", "only").await;
        assert_eq!(completed, vec!["only".to_string()]);
    }

    #[test]
    fn wait_policy_all_uses_derived_completed_set() {
        let waited = vec!["a".to_string(), "b".to_string()];
        assert!(!wait_policy_satisfied(
            ChildWaitPolicy::All,
            &waited,
            &["a".to_string()],
            "completed"
        ));
        assert!(wait_policy_satisfied(
            ChildWaitPolicy::All,
            &waited,
            &["a".to_string(), "b".to_string()],
            "completed"
        ));
    }

    #[test]
    fn child_final_assistant_text_returns_last_assistant() {
        let mut session = Session::new("child-1", "gpt-4");
        session.messages.push(Message::user("hi"));
        session
            .messages
            .push(Message::assistant("first answer", None));
        session.messages.push(Message::user("again"));
        session
            .messages
            .push(Message::assistant("final answer", None));

        assert_eq!(
            child_final_assistant_text(&session).as_deref(),
            Some("final answer")
        );
    }

    #[test]
    fn child_final_assistant_text_returns_none_when_blank() {
        let mut session = Session::new("child-1", "gpt-4");
        session.messages.push(Message::assistant("   ", None));
        assert!(child_final_assistant_text(&session).is_none());
    }

    #[test]
    fn child_final_assistant_text_returns_none_when_no_assistant() {
        let mut session = Session::new("child-1", "gpt-4");
        session.messages.push(Message::user("hi"));
        assert!(child_final_assistant_text(&session).is_none());
    }

    #[test]
    fn runtime_resume_message_folds_full_response_without_truncation() {
        // A very long child final response is folded in verbatim (no 4000-char
        // cap, no truncation marker).
        let completion = make_completion("completed");
        let long: String = "a".repeat(10_000);
        let message = runtime_resume_message(&completion, 0, Some(&long));
        assert!(message.content.contains(&long));
        assert!(!message.content.contains("truncated"));
    }

    #[test]
    fn runtime_resume_message_includes_child_response_when_provided() {
        let completion = make_completion("completed");
        let message = runtime_resume_message(&completion, 0, Some("the answer is 42"));

        assert!(matches!(message.role, Role::User));
        // Folded child results are now compressible so the parent context can
        // reclaim them under compaction.
        assert!(!message.never_compress);
        assert!(message.content.contains("Child final response:"));
        assert!(message.content.contains("the answer is 42"));

        let metadata = message.metadata.expect("metadata present");
        assert_eq!(
            metadata.get("hidden_from_ui").and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            metadata.get("runtime_kind").and_then(|v| v.as_str()),
            Some("child_completion_resume")
        );
        assert_eq!(
            metadata
                .get("child_final_response_included")
                .and_then(|v| v.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn runtime_resume_message_falls_back_to_error_when_no_response() {
        let mut completion = make_completion("error");
        completion.error = Some("boom".to_string());

        let message = runtime_resume_message(&completion, 1, None);
        assert!(message.content.contains("Child error:"));
        assert!(message.content.contains("boom"));
        let metadata = message.metadata.expect("metadata present");
        assert_eq!(
            metadata
                .get("child_final_response_included")
                .and_then(|v| v.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn runtime_resume_message_minimal_when_no_response_and_no_error() {
        let completion = make_completion("completed");
        let message = runtime_resume_message(&completion, 2, None);
        assert!(!message.content.contains("Child final response:"));
        assert!(!message.content.contains("Child error:"));
        assert!(message.content.contains("Resume the parent task"));
    }

    #[test]
    fn read_config_snapshot_refreshes_cached_snapshot_from_live_config() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        runtime.block_on(async {
            let config = Arc::new(RwLock::new(Config::default()));
            config.write().await.provider = "copilot".to_string();
            let cached_config = StdRwLock::new(Config::default());

            let snapshot = read_config_snapshot(&config, &cached_config);

            assert_eq!(snapshot.provider, "copilot");
            assert_eq!(
                cached_config.read().expect("cached snapshot lock").provider,
                "copilot"
            );
        });
    }

    #[test]
    fn read_config_snapshot_uses_cached_snapshot_when_live_lock_is_busy() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        runtime.block_on(async {
            let cached_snapshot = Config {
                provider: "cached-provider".to_string(),
                ..Default::default()
            };

            let config = Arc::new(RwLock::new(Config::default()));
            let cached_config = StdRwLock::new(cached_snapshot);
            let _write_guard = config.write().await;

            let snapshot = read_config_snapshot(&config, &cached_config);

            assert_eq!(snapshot.provider, "cached-provider");
        });
    }

    // ── Bash self-resume (issue #84 Phase 2b): deadline message + clobber-retry ──

    #[test]
    fn bash_completion_resume_message_normal_announces_completion() {
        let ids = vec!["bg-1".to_string(), "bg-2".to_string()];
        let message = bash_completion_resume_message(&ids, false);
        // Normal path: the shells genuinely finished.
        assert!(
            message.content.contains("have completed"),
            "normal resume message must announce completion: {}",
            message.content
        );
        // Hidden + compressible so the resume gate sees it but the UI hides it.
        let metadata = message.metadata.expect("metadata present");
        assert_eq!(
            metadata
                .get(RUNTIME_RESUME_MESSAGE_HIDDEN_KEY)
                .and_then(|v| v.as_bool()),
            Some(true),
            "resume message must be hidden from the UI"
        );
        assert_eq!(
            metadata
                .get(RUNTIME_RESUME_MESSAGE_KIND_KEY)
                .and_then(|v| v.as_str()),
            Some(BASH_COMPLETION_RESUME_KIND),
            "resume message must carry the bash-completion kind discriminant"
        );
    }

    #[test]
    fn bash_completion_resume_message_deadline_does_not_claim_completion() {
        // The 6h+10m deadline force-breaks with shells STILL running. The message
        // must NOT say "have completed" — that would let the model assume success
        // on a false premise. It must direct the model to verify with BashOutput.
        let ids = vec!["bg-long".to_string()];
        let message = bash_completion_resume_message(&ids, true);
        assert!(
            !message.content.contains("have completed"),
            "deadline resume message must NOT claim the shells completed: {}",
            message.content
        );
        assert!(
            message.content.contains("may still be running"),
            "deadline resume message must warn shells may still be running: {}",
            message.content
        );
        assert!(
            message.content.contains("BashOutput"),
            "deadline resume message must direct verification via BashOutput: {}",
            message.content
        );
        // Same hidden/kind shape so the resume gate is satisfied identically.
        let metadata = message.metadata.expect("metadata present");
        assert_eq!(
            metadata
                .get(RUNTIME_RESUME_MESSAGE_KIND_KEY)
                .and_then(|v| v.as_str()),
            Some(BASH_COMPLETION_RESUME_KIND)
        );
    }

    #[test]
    fn bash_resume_should_retry_matrix() {
        // The finalize-clobber retry predicate (issue #84 Phase 2b). Retry only
        // when the resume did NOT spawn (Completed / AlreadyRunning) AND the
        // persisted bash wait is still set on reload — the clobber signature.

        // Started: the resume fired — never retry, regardless of persisted state.
        assert!(!bash_resume_should_retry(
            &ResumeOutcome::Started { run_id: "r".into() },
            true
        ));
        assert!(!bash_resume_should_retry(
            &ResumeOutcome::Started { run_id: "r".into() },
            false
        ));

        // NotFound: session gone — never retry.
        assert!(!bash_resume_should_retry(&ResumeOutcome::NotFound, true));
        assert!(!bash_resume_should_retry(&ResumeOutcome::NotFound, false));

        // Completed + persisted wait still set ⇒ finalize-clobber ⇒ retry.
        assert!(bash_resume_should_retry(&ResumeOutcome::Completed, true));
        // Completed + persisted wait cleared ⇒ handled (our message stuck, or a
        // concurrent resume finished) ⇒ stop.
        assert!(!bash_resume_should_retry(&ResumeOutcome::Completed, false));

        // AlreadyRunning + persisted wait still set ⇒ clobbered while a runner is
        // (stale-)active ⇒ retry to re-establish the resume message.
        assert!(bash_resume_should_retry(
            &ResumeOutcome::AlreadyRunning { run_id: "r".into() },
            true
        ));
        // AlreadyRunning + wait cleared ⇒ a runner owns the session ⇒ stop.
        assert!(!bash_resume_should_retry(
            &ResumeOutcome::AlreadyRunning { run_id: "r".into() },
            false
        ));
    }

    // ── bash completion injection body (Phase 2b follow-up) ──────────────

    #[test]
    fn injection_body_includes_status_exit_command_and_tail() {
        let info = BashCompletionInfo {
            session_id: "s".into(),
            bash_id: "abc123".into(),
            command: "make build".into(),
            exit_code: Some(0),
            status: "completed".into(),
            output_tail: "BUILD OK".into(),
        };
        let body = bash_completion_injection_body(&info);
        assert!(body.contains("abc123"), "body: {body}");
        assert!(body.contains("make build"), "body: {body}");
        assert!(body.contains("completed"), "body: {body}");
        assert!(body.contains("exit code 0"), "body: {body}");
        assert!(body.contains("BUILD OK"), "body: {body}");
        // The model is pointed at BashOutput for the full log.
        assert!(body.contains("BashOutput"), "body: {body}");
        assert!(body.contains("bash_id=\"abc123\""), "body: {body}");
    }

    #[test]
    fn injection_body_handles_no_output_and_signal_kill() {
        let info = BashCompletionInfo {
            session_id: "s".into(),
            bash_id: "xyz".into(),
            command: "sleep 99".into(),
            exit_code: None,
            status: "killed".into(),
            output_tail: String::new(),
        };
        let body = bash_completion_injection_body(&info);
        assert!(body.contains("killed"), "body: {body}");
        assert!(body.contains("none (signal/killed)"), "body: {body}");
        assert!(body.contains("no captured output"), "body: {body}");
        // No output tail section when there is nothing to show.
        assert!(!body.contains("Output tail:"), "body: {body}");
    }

    async fn temp_store() -> (tempfile::TempDir, Arc<dyn Storage>, LockedSessionStore) {
        let temp = tempfile::tempdir().unwrap();
        let storage: Arc<dyn Storage> = Arc::new(
            bamboo_storage::v2::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .expect("storage init"),
        );
        let persistence = LockedSessionStore::new(storage.clone());
        (temp, storage, persistence)
    }

    #[tokio::test]
    async fn enqueue_writes_pending_injection_and_preserves_messages() {
        let (_temp, storage, persistence) = temp_store().await;

        let mut session = Session::new("sess-enq", "test-model");
        session.add_message(Message::user("do the build"));
        storage.save_session(&session).await.unwrap();

        let info = BashCompletionInfo {
            session_id: "sess-enq".into(),
            bash_id: "sh-1".into(),
            command: "make".into(),
            exit_code: Some(0),
            status: "completed".into(),
            output_tail: "done".into(),
        };
        let saved = enqueue_bash_completion_injection(&persistence, &info)
            .await
            .expect("enqueue io ok")
            .expect("session exists");

        let pending = saved
            .pending_injected_messages()
            .expect("pending injection present");
        assert_eq!(pending.len(), 1);
        let content = pending[0].get("content").and_then(|v| v.as_str()).unwrap();
        assert!(content.contains("sh-1"), "content: {content}");
        assert!(content.contains("make"), "content: {content}");
        assert!(content.contains("done"), "content: {content}");
        // The pre-existing conversation is untouched (no clobber).
        assert_eq!(saved.messages.len(), 1);
    }

    #[tokio::test]
    async fn enqueue_returns_none_for_missing_session() {
        let (_temp, _storage, persistence) = temp_store().await;
        let info = BashCompletionInfo {
            session_id: "does-not-exist".into(),
            bash_id: "x".into(),
            command: "true".into(),
            exit_code: Some(0),
            status: "completed".into(),
            output_tail: String::new(),
        };
        let result = enqueue_bash_completion_injection(&persistence, &info)
            .await
            .expect("io ok");
        assert!(result.is_none(), "no session → nothing enqueued");
    }

    // ── push-driven resume: the state transition + decision the push applies ──

    /// A session suspended on `waiting_for_bash`, given the rich completion
    /// message, is transitioned to a resumable state: the wait is cleared, the
    /// runtime is Idle, the suspend-reason marker is gone, and the resume message
    /// is appended. This is exactly what the PUSH does to wake the loop
    /// event-driven (vs the old backstop poll).
    #[test]
    fn apply_bash_resume_transition_clears_wait_and_appends_message() {
        use bamboo_domain::session::runtime_state::WaitingForBashState;

        let mut session = Session::new("sess-resume", "test-model");
        session.add_message(Message::user("kick off the build"));
        let mut rt = read_runtime_state(&session);
        rt.status = AgentStatusState::Running;
        rt.waiting_for_bash = Some(WaitingForBashState::for_bash(
            vec!["sh-1".into()],
            Utc::now(),
        ));
        write_runtime_state(&mut session, &rt);
        session.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "waiting_for_bash".to_string(),
        );

        let resume = bash_completion_resume_message(&["sh-1".to_string()], false);
        let did = apply_bash_resume_transition(&mut session, &resume);

        assert!(did, "a suspended session must transition");
        let after = read_runtime_state(&session);
        assert!(
            after.waiting_for_bash.is_none(),
            "bash wait must be cleared"
        );
        assert_eq!(after.status, AgentStatusState::Idle, "runtime must be Idle");
        assert!(
            !session.metadata.contains_key("runtime.suspend_reason"),
            "suspend-reason marker must be removed"
        );
        assert_eq!(session.messages.len(), 2, "resume message must be appended");
        assert!(matches!(
            session.messages.last().map(|m| &m.role),
            Some(Role::User)
        ));
    }

    /// The double-resume guard: a session NOT waiting on bash is a no-op — no
    /// message appended, nothing mutated. This is what makes the backstop poll
    /// harmlessly yield once the push has already resumed (and vice versa).
    #[test]
    fn apply_bash_resume_transition_noops_when_not_waiting() {
        let mut session = Session::new("sess-live", "test-model");
        session.add_message(Message::user("hi"));

        let resume = bash_completion_resume_message(&["sh-1".to_string()], false);
        let did = apply_bash_resume_transition(&mut session, &resume);

        assert!(!did, "a non-waiting session must not transition");
        assert_eq!(session.messages.len(), 1, "no resume message appended");
    }

    /// The resume invariant: push-resume fires ONLY when the loop is suspended on
    /// bash AND every waited shell has finished. A still-running sibling shell
    /// keeps it on the enqueue path.
    #[test]
    fn bash_completion_should_resume_only_when_suspended_and_all_done() {
        assert!(bash_completion_should_resume(true, true));
        assert!(!bash_completion_should_resume(true, false)); // other shells still running
        assert!(!bash_completion_should_resume(false, true)); // live loop, not suspended
        assert!(!bash_completion_should_resume(false, false));
    }

    /// The push's resume message carries the shell's identity + status + output
    /// tail (so the model needs no `BashOutput` round-trip) and is tagged as a
    /// bash-completion resume so it satisfies the `has_pending_user_message` gate.
    #[test]
    fn bash_resume_message_from_info_carries_bashid_tail_and_kind() {
        let info = BashCompletionInfo {
            session_id: "s".into(),
            bash_id: "sh-42".into(),
            command: "cargo test".into(),
            exit_code: Some(0),
            status: "completed".into(),
            output_tail: "test result: ok".into(),
        };
        let msg = bash_resume_message_from_info(&info);

        assert!(matches!(msg.role, Role::User));
        assert!(msg.content.contains("sh-42"), "content: {}", msg.content);
        assert!(
            msg.content.contains("cargo test"),
            "content: {}",
            msg.content
        );
        assert!(
            msg.content.contains("test result: ok"),
            "content: {}",
            msg.content
        );
        assert!(
            msg.content.contains("BashOutput"),
            "content: {}",
            msg.content
        );
        let meta = serde_json::to_string(&msg.metadata).unwrap();
        assert!(
            meta.contains(BASH_COMPLETION_RESUME_KIND),
            "resume message must be tagged as a bash-completion resume: {meta}"
        );
    }
}
