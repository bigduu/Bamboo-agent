//! Child-session completion coordinator.
//!
//! Receives terminal child runner notifications from `bamboo-engine`, updates
//! durable parent wait state, and resumes the parent when the configured wait
//! policy is satisfied.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock as StdRwLock};
use std::time::Duration;

use crate::execution::{
    create_event_forwarder, spawn_session_execution, try_reserve_runner, AgentRunner,
    ChildCompletion, ChildCompletionHandler, RunnerReservation, SessionExecutionArgs,
};
use crate::Agent;
use async_trait::async_trait;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Message, Role, Session, SessionKind};
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

/// Maximum number of characters of the child's final assistant content that
/// will be embedded into the hidden runtime resume message. Anything longer is
/// truncated with a marker so the root LLM can still call `SubAgent.get` to
/// fetch the full child output if needed.
const RUNTIME_RESUME_CHILD_RESULT_MAX_CHARS: usize = 4000;
const RUNTIME_RESUME_CHILD_RESULT_TRUNCATION_MARKER: &str =
    "\n\n[... child final response truncated; call SubAgent.get for full content ...]";

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

fn truncate_for_resume(content: &str) -> String {
    if content.chars().count() <= RUNTIME_RESUME_CHILD_RESULT_MAX_CHARS {
        return content.to_string();
    }
    let head: String = content
        .chars()
        .take(RUNTIME_RESUME_CHILD_RESULT_MAX_CHARS)
        .collect();
    format!("{head}{RUNTIME_RESUME_CHILD_RESULT_TRUNCATION_MARKER}")
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

    let truncated_response = child_final_response.map(truncate_for_resume);
    if let Some(response) = truncated_response.as_deref() {
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
        "child_final_response_included": truncated_response.is_some(),
    }));
    message.never_compress = true;
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
        }
    }

    pub async fn set_root_tools(&self, tools: Arc<dyn ToolExecutor>) {
        *self.root_tools.write().await = Some(tools);
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

    async fn resume_parent(&self, parent_session_id: String) {
        for attempt in 0..=5u8 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(250 * attempt as u64)).await;
            }

            let Some(session) = self.load_session(&parent_session_id).await else {
                tracing::warn!(%parent_session_id, "cannot resume parent after child completion: session not found");
                return;
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
                return;
            }
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
        // Acquire a per-parent async lock to eliminate the concurrent
        // double-resume race (see `parent_locks` for the full scenario). The
        // inner std::sync::Mutex is released immediately so no sync lock is
        // held across the await that follows.
        let per_parent = {
            let mut map = parent_locks().lock().expect("parent lock map poisoned");
            map.entry(completion.parent_session_id.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _per_parent_guard = per_parent.lock().await;

        let Some(mut parent) = self.load_session(&completion.parent_session_id).await else {
            tracing::warn!(
                parent_session_id = %completion.parent_session_id,
                child_session_id = %completion.child_session_id,
                "child completion received for missing parent"
            );
            return;
        };

        if parent.kind != SessionKind::Root {
            tracing::warn!(
                parent_session_id = %completion.parent_session_id,
                child_session_id = %completion.child_session_id,
                "child completion parent is not a root session"
            );
            return;
        }

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
            // Best-effort: load the child session so we can include its final
            // assistant content in the hidden resume message. This avoids the
            // root LLM having to make an extra `SubAgent.get` call after
            // resume just to read what the child concluded — which would cost
            // an additional LLM round trip.
            let child_final_response = match self
                .storage
                .load_session(&completion.child_session_id)
                .await
            {
                Ok(Some(child)) => child_final_assistant_text(&child),
                Ok(None) => None,
                Err(error) => {
                    tracing::warn!(
                        child_session_id = %completion.child_session_id,
                        %error,
                        "failed to load child session for runtime resume message"
                    );
                    None
                }
            };
            parent.add_message(runtime_resume_message(
                &completion,
                remaining_children,
                child_final_response.as_deref(),
            ));
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
        try_reserve_runner(&self.agent_runners, session_id, event_sender).await
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
            disabled_tools: Some(config.disabled_tools),
            disabled_skill_ids: Some(config.disabled_skill_ids),
            selected_skill_ids: None,
            selected_skill_mode: None,
            cancel_token,
            mpsc_tx,
            image_fallback: config.image_fallback,
            gold_config,
            app_data_dir: Some(self.app_data_dir.clone()),
            runners: self.agent_runners.clone(),
            sessions_cache: self.sessions.clone(),
            on_complete: None,
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
    fn truncate_for_resume_passthrough_when_short() {
        let s = "hello world";
        assert_eq!(truncate_for_resume(s), s);
    }

    #[test]
    fn truncate_for_resume_truncates_long_content() {
        let long: String = "a".repeat(RUNTIME_RESUME_CHILD_RESULT_MAX_CHARS + 100);
        let truncated = truncate_for_resume(&long);
        assert!(truncated.len() > RUNTIME_RESUME_CHILD_RESULT_MAX_CHARS);
        assert!(truncated.ends_with(RUNTIME_RESUME_CHILD_RESULT_TRUNCATION_MARKER));
    }

    #[test]
    fn runtime_resume_message_includes_child_response_when_provided() {
        let completion = make_completion("completed");
        let message = runtime_resume_message(&completion, 0, Some("the answer is 42"));

        assert!(matches!(message.role, Role::User));
        assert!(message.never_compress);
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
}
