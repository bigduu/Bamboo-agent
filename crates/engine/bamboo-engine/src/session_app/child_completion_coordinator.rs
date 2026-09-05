//! Child-session completion coordinator.
//!
//! Receives terminal child runner notifications from `bamboo-engine`, updates
//! durable parent wait state, and resumes the parent when the configured wait
//! policy is satisfied.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, OnceLock, RwLock as StdRwLock, Weak};
use std::time::Duration;

use bamboo_domain::poison::PoisonRecover;
use bamboo_domain::{
    AgentHookPoint, HookPayload, HookToolOutcome, SessionChildOutcome, SessionMessageBody,
    SessionMessageContent, SessionMessageEnvelope, SessionMessageId, SessionMessageKind,
    SessionMessageSource, SessionProviderMessage,
};

use crate::execution::{
    create_event_forwarder, finalize_runner, reserve_runner_core, reserve_session_execution,
    spawn_session_execution, AgentRunner, AgentStatus, ChildCompletion, ChildCompletionHandler,
    ReserveOutcome, SessionExecutionArgs, SessionExecutionReservation,
    SessionExecutionReserveOutcome, SpawnJob, SpawnScheduler,
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
    AgentEvent, BashCompletionInfo, BashCompletionSink, Message, Role, Session, SessionKind,
};
use bamboo_domain::session::runtime_state::{
    AgentRuntimeState, AgentStatusState, ChildWaitPolicy, SuspensionState, WaitingForChildrenState,
};
use bamboo_llm::{Config, ProviderModelRouter, ProviderRegistry};
use bamboo_storage::LockedSessionStore;
use chrono::Utc;
use sha2::{Digest, Sha256};
use tokio::sync::{broadcast, RwLock};

use crate::model_areas::resolve_global_area_models;
use crate::model_config_helper::{
    resolve_fast_model, resolve_gold_config, resolve_provider_routing_key, GOLD_CONFIG_METADATA_KEY,
};
use crate::session_activation::{
    SessionActivationLaunch, SessionActivationReserveOutcome, SessionActivationSpawner,
};
use crate::session_app::execute::consume_pending_clarification_resume;
use crate::session_app::provider_model::{persist_model_ref, session_effective_model_ref};
use crate::session_app::resume::{
    resume_session_execution, ResumeExecutionPort, ResumeSpawnRequest,
};
use crate::session_app::types::{ResumeConfigSnapshot, ResumeOutcome};

const AGENT_RUNTIME_STATE_METADATA_KEY: &str = "agent.runtime.state";
const RUNTIME_RESUME_MESSAGE_HIDDEN_KEY: &str = "hidden_from_ui";
const RUNTIME_RESUME_MESSAGE_KIND_KEY: &str = "runtime_kind";
const CHILD_COMPLETION_INLINE_FIELD_BYTES: usize = 48 * 1024;
const CHILD_COMPLETION_OVERSIZE_TAIL_BYTES: usize = 8 * 1024;

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

/// Re-read and prepare an activation target under the same per-session
/// persistence lock that commits the generic suspension clear.
///
/// The boolean is false when a specific child/Bash wait is present in the
/// latest durable snapshot; callers must leave the activation unreserved.
async fn prepare_session_inbox_activation(
    persistence: &LockedSessionStore,
    session_id: &str,
    interrupt_specific_wait: bool,
) -> std::io::Result<Option<(Session, bool)>> {
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ready_for_mutation = ready.clone();
    let saved = persistence
        .update_runtime_config(session_id, move |latest| {
            let mut runtime_state = read_runtime_state(latest);
            let specifically_waiting = runtime_state.waiting_for_children.is_some()
                || runtime_state.waiting_for_bash.is_some();
            if specifically_waiting && !interrupt_specific_wait {
                return;
            }
            // Explicit steering interrupts only this reasoning gate. The
            // durable child/Bash wait remains owned so later terminal events
            // still have exactly one coordinator and can authorize their
            // staged outcomes. End-of-run bookkeeping re-suspends if that wait
            // is still present.
            runtime_state.status = AgentStatusState::Idle;
            runtime_state.suspension = None;
            write_runtime_state(latest, &runtime_state);
            latest.metadata.remove("runtime.suspend_reason");
            latest.updated_at = Utc::now();
            ready_for_mutation.store(true, std::sync::atomic::Ordering::Release);
        })
        .await?;
    Ok(saved.map(|session| (session, ready.load(std::sync::atomic::Ordering::Acquire))))
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

fn child_completion_envelope(
    completion: &ChildCompletion,
    wait_registered_at: chrono::DateTime<Utc>,
    result: Option<String>,
    provider_message: &Message,
) -> SessionMessageEnvelope {
    fn bounded_terminal_field(
        label: &str,
        child_session_id: &str,
        value: Option<String>,
    ) -> (Option<String>, serde_json::Value, bool) {
        let Some(value) = value else {
            return (None, serde_json::Value::Null, false);
        };
        if value.len() <= CHILD_COMPLETION_INLINE_FIELD_BYTES {
            return (Some(value.clone()), serde_json::Value::String(value), false);
        }
        let digest = hex::encode(Sha256::digest(value.as_bytes()));
        let mut tail_start = value
            .len()
            .saturating_sub(CHILD_COMPLETION_OVERSIZE_TAIL_BYTES);
        while tail_start < value.len() && !value.is_char_boundary(tail_start) {
            tail_start += 1;
        }
        let tail = &value[tail_start..];
        let summary = format!(
            "Child {label} exceeded the durable inline limit ({} UTF-8 bytes, sha256={digest}). \
             Retrieve the full child transcript with SubAgent.get(child_session_id=\"{child_session_id}\").\
             \n\nBounded tail:\n{tail}",
            value.len()
        );
        (
            Some(summary),
            serde_json::json!({
                "oversized": true,
                "utf8_bytes": value.len(),
                "sha256": digest,
            }),
            true,
        )
    }

    let (stored_result, result_identity, _result_oversized) =
        bounded_terminal_field("result", &completion.child_session_id, result);
    let (stored_error, error_identity, _error_oversized) = bounded_terminal_field(
        "error",
        &completion.child_session_id,
        completion.error.clone(),
    );
    let mut bounded_provider_message = provider_message.clone();
    let provider_oversized = serde_json::to_vec(provider_message)
        .map(|bytes| bytes.len() > CHILD_COMPLETION_INLINE_FIELD_BYTES)
        .unwrap_or(true);
    if provider_oversized {
        let mut content = format!(
            "Runtime notification: child session `{}` finished with status `{}`.",
            completion.child_session_id, completion.status
        );
        if let Some(result) = stored_result.as_deref() {
            content.push_str("\n\n");
            content.push_str(result);
        }
        if let Some(error) = stored_error.as_deref() {
            content.push_str("\n\n");
            content.push_str(error);
        }
        bounded_provider_message.content = content;
        bounded_provider_message.content_parts = None;
    }
    let body = SessionMessageBody::ChildOutcome(SessionChildOutcome {
        child_session_id: completion.child_session_id.clone(),
        status: completion.status.clone(),
        result: stored_result,
        error: stored_error,
        provider_message: Some(session_provider_message(&bounded_provider_message)),
    });
    let semantic = serde_json::json!({
        "parent_session_id": completion.parent_session_id,
        "child_session_id": completion.child_session_id,
        "status": completion.status,
        "error": error_identity,
        "result": result_identity,
        "wait_registered_at": wait_registered_at,
    });
    SessionMessageEnvelope {
        id: SessionMessageId::stable("session_child_completion", &semantic),
        source: SessionMessageSource::Runtime {
            subsystem: "child_completion_coordinator".to_string(),
        },
        target_session_id: completion.parent_session_id.clone(),
        kind: SessionMessageKind::ChildOutcome,
        body,
        created_at: completion.completed_at,
        thread_id: None,
        in_reply_to: None,
        attempt: None,
        correlation_id: Some(format!("child_completion:{}", completion.child_session_id)),
    }
}

fn session_provider_message(message: &Message) -> SessionProviderMessage {
    SessionProviderMessage {
        content: SessionMessageContent {
            text: message.content.clone(),
            parts: message.content_parts.clone().unwrap_or_default(),
        },
        metadata: message
            .metadata
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default(),
        never_compress: message.never_compress,
    }
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
/// across the async critical section. Entries exist only while a holder or
/// waiter owns a `SessionResumeLock`; historical parent IDs are reclaimed.
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
fn session_resume_lock(session_id: &str) -> SessionResumeLock {
    let mut map = parent_locks().lock().recover_poison();
    let lock = map
        .entry(session_id.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    SessionResumeLock {
        session_id: session_id.to_string(),
        lock: Some(lock),
    }
}

/// The lease is constructed before awaiting the mutex, so cancelled waiters
/// also reclaim their registration. Lookup and last-owner removal use the same
/// brief registry lock; two live mutexes can never exist for the same ID.
struct SessionResumeLock {
    session_id: String,
    lock: Option<Arc<tokio::sync::Mutex<()>>>,
}

impl std::ops::Deref for SessionResumeLock {
    type Target = tokio::sync::Mutex<()>;
    fn deref(&self) -> &Self::Target {
        self.lock.as_ref().expect("live resume-lock lease")
    }
}

impl Drop for SessionResumeLock {
    fn drop(&mut self) {
        self.lock.take();
        let mut map = parent_locks().lock().recover_poison();
        if map
            .get(&self.session_id)
            .is_some_and(|lock| Arc::strong_count(lock) == 1)
        {
            map.remove(&self.session_id);
        }
    }
}

fn wait_policy_satisfied(
    policy: ChildWaitPolicy,
    wait_child_ids: &[String],
    completed_child_ids: &[String],
    latest_child_id: &str,
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
            // The error short-circuit only counts a completion from a child
            // this wait actually tracks (issue #546): a stray/duplicate
            // completion from an untracked child — e.g. a frozen runner's
            // task waking up after the watchdog already synthesized its
            // timeout, in a later run's wait — must not resume the parent.
            (is_error_like(latest_status) && wait_child_ids.iter().any(|id| id == latest_child_id))
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
    /// Weak late binding avoids the scheduler -> completion handler ->
    /// scheduler ownership cycle while still routing idle child activation
    /// through the canonical placement-aware spawn core.
    spawn_scheduler: Arc<RwLock<Weak<SpawnScheduler>>>,
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
            spawn_scheduler: Arc::new(RwLock::new(Weak::new())),
        }
    }

    pub async fn set_root_tools(&self, tools: Arc<dyn ToolExecutor>) {
        *self.root_tools.write().await = Some(tools);
    }

    pub async fn set_spawn_scheduler(&self, scheduler: &Arc<SpawnScheduler>) {
        *self.spawn_scheduler.write().await = Arc::downgrade(scheduler);
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
        // The wait state was already cleared and the resume message persisted,
        // so nothing event-driven will retry — the child-wait watchdog is the
        // backstop that picks this stranded parent up (it resumes suspended
        // sessions that hold a pending runtime resume message but no runner).
        tracing::error!(
            %parent_session_id,
            "parent resume gave up after AlreadyRunning retry budget; \
             relying on the child-wait watchdog backstop"
        );
        ResumeOutcome::AlreadyRunning {
            run_id: String::new(),
        }
    }

    async fn save_and_cache(&self, session: &mut Session) {
        if let Err(error) = self
            .persistence
            .merge_save_runtime_and_publish(session, |saved, _| {
                self.sessions.insert(
                    saved.id.clone(),
                    Arc::new(crate::SessionSnapshot::new(saved.clone())),
                );
            })
            .await
        {
            tracing::warn!(session_id = %session.id, %error, "failed to persist session");
        }
    }
}

#[async_trait]
impl ChildCompletionHandler for ChildCompletionCoordinator {
    async fn on_child_completed(&self, completion: ChildCompletion) {
        // Terminality guard: a child that reports a NON-terminal status (e.g.
        // "suspended" — awaiting parent approval, its own bash wait, or its own
        // grandchildren) is not done. It must never satisfy the parent's wait:
        // `derive_completed_child_ids` folds the just-reported child in
        // unconditionally, so without this guard a suspending child would
        // resume the parent with a premature "finished with status
        // `suspended`" message. The child will publish a real terminal
        // completion when it later resumes and finishes.
        if !is_terminal_child_status(&completion.status) {
            tracing::info!(
                parent_session_id = %completion.parent_session_id,
                child_session_id = %completion.child_session_id,
                status = %completion.status,
                "non-terminal child status; leaving the parent wait armed"
            );
            return;
        }

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
        let active_wait = runtime_state.waiting_for_children.clone();
        if let Some(wait) = active_wait.as_ref() {
            remaining_children = wait
                .child_session_ids
                .iter()
                .filter(|id| !completed_child_ids.iter().any(|completed| completed == *id))
                .count();
            should_resume = wait_policy_satisfied(
                wait.wait_for,
                &wait.child_session_ids,
                &completed_child_ids,
                &completion.child_session_id,
                &completion.status,
            );
        }

        // READ-SIDE OWNERSHIP GUARD (issue #546): `SubAgent.wait` ids are
        // model-provided and unvalidated, and the watchdog unstrands a wait
        // over a FOREIGN/unknown id by publishing a synthetic completion
        // here. We must resume the parent (so it is not stranded) but MUST
        // NOT fold that foreign session's transcript into the parent — that
        // would be a cross-session disclosure primitive. Decide ownership
        // from the child's OWN parent linkage (control-plane only, no
        // messages loaded), and only load its full content when it is truly
        // this parent's child. An unowned id resumes with the neutral/error
        // message (`runtime_resume_message` falls back to `completion.error`
        // when no child content is supplied).
        let reported_child_owned = match self
            .storage
            .load_runtime_control_plane(&completion.child_session_id)
            .await
        {
            Ok(Some(control_plane)) => completion_child_is_owned(
                &completion.parent_session_id,
                control_plane.parent_session_id.as_deref(),
            ),
            _ => false,
        };

        // Load the completed child once, ONLY when owned. The guardian
        // branch inspects its subagent_type + final verdict; the generic
        // path folds its final assistant content into the hidden resume
        // message (avoiding an extra `SubAgent.get` round trip after resume).
        let loaded_child = if reported_child_owned {
            match self
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
            }
        } else {
            tracing::warn!(
                parent_session_id = %completion.parent_session_id,
                child_session_id = %completion.child_session_id,
                "completion child is not a child of this parent; resuming with a neutral \
                 message and NOT folding its content"
            );
            None
        };

        let child_final_response = loaded_child.as_ref().and_then(child_final_assistant_text);
        // Select the exact provider-facing resume message before durable
        // admission. The typed body carries its content/parts and safe runtime
        // metadata, so the canonical path is semantically identical to the
        // rolling-upgrade transcript fallback.
        let guardian_resume = if should_resume {
            let reviewed_round = runtime_state.round.current_round;
            loaded_child.as_ref().and_then(|child| {
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
            })
        } else {
            None
        };
        let resume_message = guardian_resume.unwrap_or_else(|| {
            runtime_resume_message(
                &completion,
                remaining_children,
                child_final_response.as_deref(),
            )
        });

        // Stage the typed child outcome before clearing any durable wait. A
        // crash after this admission leaves the parent suspended with an
        // inspectable envelope; only a durably committed policy transition
        // below is allowed to activate it.
        let messenger = self.agent.session_messenger().cloned();
        let child_admission =
            if let (Some(wait), Some(messenger)) = (active_wait.as_ref(), messenger.as_ref()) {
                let envelope = child_completion_envelope(
                    &completion,
                    wait.registered_at,
                    child_final_response,
                    &resume_message,
                );
                match messenger.admit(envelope).await {
                    Ok(admission) => Some(admission),
                    Err(error) => {
                        tracing::warn!(
                            parent_session_id = %completion.parent_session_id,
                            child_session_id = %completion.child_session_id,
                            %error,
                            "child outcome SessionInbox admission failed; leaving parent wait armed"
                        );
                        return;
                    }
                }
            } else {
                None
            };

        if should_resume {
            if let (Some(messenger), Some(admission)) =
                (messenger.as_ref(), child_admission.as_ref())
            {
                if let Err(error) = messenger.prepare_activation(admission).await {
                    tracing::warn!(
                        parent_session_id = %completion.parent_session_id,
                        child_session_id = %completion.child_session_id,
                        %error,
                        "child outcome activation watermark failed; leaving parent wait armed"
                    );
                    return;
                }
            }
        }

        if should_resume {
            runtime_state.waiting_for_children = None;
            runtime_state.status = AgentStatusState::Idle;
            runtime_state.suspension = None;
            parent.metadata.remove("runtime.suspend_reason");

            if child_admission.is_none() {
                // Rolling-upgrade fallback only. The canonical path keeps the
                // child outcome solely in SessionInbox until the next safe
                // reasoning boundary.
                parent.add_message(resume_message);
            }
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
        if let Err(error) = self
            .persistence
            .checkpoint_runtime_session(&mut parent)
            .await
        {
            tracing::warn!(
                parent_session_id = %completion.parent_session_id,
                child_session_id = %completion.child_session_id,
                %error,
                "child outcome is durable but parent wait transition failed; leaving activation deferred"
            );
            return;
        }
        self.sessions.insert(
            parent.id.clone(),
            Arc::new(crate::SessionSnapshot::new(parent.clone())),
        );

        // Capture before releasing the per-parent lock so the borrow checker
        // is satisfied; `resume_parent` has its own retry loop and should not
        // hold the per-parent lock (it would block other completions for the
        // same parent, and the state is already durably settled above).
        let resume_parent_id = parent.id.clone();
        drop(_per_parent_guard);

        if should_resume {
            if let (Some(messenger), Some(admission)) = (messenger, child_admission) {
                if let Err(error) = messenger.activate_prepared(&admission).await {
                    tracing::warn!(
                        parent_session_id = %resume_parent_id,
                        %error,
                        "child outcome and wait transition are durable but activation failed"
                    );
                }
            } else {
                self.resume_parent(resume_parent_id).await;
            }
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

    async fn reserve_session_execution(
        &self,
        session_id: &str,
        event_sender: &broadcast::Sender<AgentEvent>,
    ) -> SessionExecutionReserveOutcome {
        reserve_session_execution(
            &self.agent,
            &self.agent_runners,
            &self.session_event_senders,
            session_id,
            event_sender,
        )
        .await
    }

    async fn get_or_create_event_sender(&self, session_id: &str) -> broadcast::Sender<AgentEvent> {
        crate::execution::session_events::get_or_create_event_sender(
            &self.session_event_senders,
            session_id,
        )
        .await
    }

    fn dispatch_resume_execution(
        &self,
        request: ResumeSpawnRequest,
    ) -> Result<(), ResumeSpawnRequest> {
        let owner = self.clone();
        tokio::spawn(async move {
            ResumeExecutionPort::spawn_resume_execution(&owner, request).await;
        });
        Ok(())
    }

    async fn spawn_resume_execution(&self, request: ResumeSpawnRequest) {
        let ResumeSpawnRequest {
            session_id,
            mut session,
            mut execution_reservation,
            event_sender,
            config,
        } = request;
        if let Err(error) = execution_reservation.ensure_registered().await {
            tracing::warn!(
                %session_id,
                run_id = %execution_reservation.run_id(),
                %error,
                "cannot resume after child completion without exact router ownership"
            );
            return;
        }

        let Some(root_tools) = self.root_tools.read().await.clone() else {
            tracing::error!(%session_id, "cannot resume parent after child completion: root tool surface is not initialized");
            return;
        };

        let config_snapshot = self.config.read().await.clone();
        let model = session.model.clone();
        let session_model_ref = session_effective_model_ref(&session);
        let requested_provider = session_model_ref
            .as_ref()
            .map(|model_ref| model_ref.provider.as_str())
            .unwrap_or(config.provider_name.as_str());
        let resolved_provider_name = match resolve_provider_routing_key(
            &config_snapshot,
            requested_provider,
            &self.provider_registry,
        ) {
            Ok(provider) => provider,
            Err(error) => {
                tracing::error!(
                    session_id = %session_id,
                    provider = requested_provider,
                    %error,
                    "child-completion resume provider is unavailable; refusing to fall back"
                );
                execution_reservation.abandon().await;
                return;
            }
        };
        let provider_override = if let Some(mut model_ref) = session_model_ref {
            model_ref.provider = resolved_provider_name.clone();
            persist_model_ref(&mut session, &model_ref);
            match self.provider_router.route(&model_ref) {
                Ok(provider) => Some(provider),
                Err(error) => {
                    tracing::error!(
                        session_id = %session_id,
                        provider = %model_ref.provider,
                        model = %model_ref.model,
                        %error,
                        "child-completion resume provider routing failed closed"
                    );
                    execution_reservation.abandon().await;
                    return;
                }
            }
        } else {
            match self.provider_registry.get(&resolved_provider_name) {
                Some(provider) => Some(provider),
                None => {
                    tracing::error!(
                        session_id = %session_id,
                        provider = %resolved_provider_name,
                        "child-completion resume provider disappeared after resolution; refusing to fall back"
                    );
                    execution_reservation.abandon().await;
                    return;
                }
            }
        };
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
            execution_reservation.run_id().to_string(),
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

        consume_pending_clarification_resume(&mut session);
        spawn_session_execution(SessionExecutionArgs {
            agent: self.agent.clone(),
            session_id,
            session,
            execution_reservation,
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
            // A resumed session that is itself a CHILD (nested sub-agents)
            // must publish its terminal completion so ITS parent is woken
            // in turn (issue #546).
            child_completion_handler: Some(Arc::new(self.clone())),
        });
    }
}

/// Real SessionInbox activation adapter. It reserves through the exact same
/// runner registry as every existing resume path, but deliberately bypasses
/// `has_pending_user_message`: the typed envelope is still in the durable inbox
/// and will be admitted by the loop's first safe turn boundary.
#[async_trait]
impl SessionActivationSpawner for ChildCompletionCoordinator {
    async fn reserve_activation(
        &self,
        target_session_id: &str,
        _inbox_generation: u64,
    ) -> Result<SessionActivationReserveOutcome, bamboo_domain::SessionActivationError> {
        let Some(inbox) = self.agent.session_inbox() else {
            return Err(bamboo_domain::SessionActivationError::Internal(
                "agent runtime has no SessionInbox".to_string(),
            ));
        };
        let backlog = inbox
            .inspect(target_session_id)
            .await
            .map_err(|error| bamboo_domain::SessionActivationError::Internal(error.to_string()))?;
        if !backlog.activation_pending() {
            return Ok(SessionActivationReserveOutcome::NoWork);
        }
        // Load and mutate under the persistence lock. A coordinator that armed
        // a child/Bash wait immediately before this activation therefore wins.
        let prepared = prepare_session_inbox_activation(
            &self.persistence,
            target_session_id,
            backlog.interrupt_pending(),
        )
        .await
        .map_err(|error| {
            bamboo_domain::SessionActivationError::Internal(format!(
                "persist resumable SessionInbox target: {error}"
            ))
        })?;
        let Some((session, ready)) = prepared else {
            return Ok(SessionActivationReserveOutcome::NotFound);
        };
        if !ready {
            tracing::info!(
                session_id = target_session_id,
                "SessionInbox backlog is activation-eligible but a specific durable wait remains armed"
            );
            return Ok(SessionActivationReserveOutcome::NoWork);
        }

        enum LaunchPlan {
            Root(Box<ResumeConfigSnapshot>),
            Child {
                scheduler: Arc<SpawnScheduler>,
                parent_session_id: String,
                model: String,
                disabled_tools: Option<Vec<String>>,
            },
        }

        // Derive every execution/security input from the latest locked session
        // returned above, never from a stale pre-lock snapshot.
        let launch_plan = match session.kind {
            SessionKind::Root => {
                if self.root_tools.read().await.is_none() {
                    return Err(bamboo_domain::SessionActivationError::Internal(
                        "root tool surface is not initialized".to_string(),
                    ));
                }
                let config_snapshot = self.config.read().await.clone();
                LaunchPlan::Root(Box::new(
                    self.build_resume_config(&session, &config_snapshot),
                ))
            }
            SessionKind::Child => {
                let scheduler = self.spawn_scheduler.read().await.upgrade().ok_or_else(|| {
                    bamboo_domain::SessionActivationError::Internal(
                        "child spawn scheduler is not initialized".to_string(),
                    )
                })?;
                let parent_session_id = session
                    .parent_session_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| {
                        bamboo_domain::SessionActivationError::Internal(format!(
                            "child SessionInbox target {target_session_id} has no parent owner"
                        ))
                    })?;
                let parent = self
                    .storage
                    .load_session(&parent_session_id)
                    .await
                    .map_err(|error| {
                        bamboo_domain::SessionActivationError::Internal(format!(
                            "load parent owner {parent_session_id}: {error}"
                        ))
                    })?
                    .ok_or_else(|| {
                        bamboo_domain::SessionActivationError::Internal(format!(
                            "child SessionInbox parent owner {parent_session_id} disappeared"
                        ))
                    })?;
                let child_root = if session.root_session_id.trim().is_empty() {
                    parent_session_id.as_str()
                } else {
                    session.root_session_id.as_str()
                };
                let parent_root = if parent.root_session_id.trim().is_empty() {
                    parent.id.as_str()
                } else {
                    parent.root_session_id.as_str()
                };
                if child_root != parent_root {
                    return Err(bamboo_domain::SessionActivationError::Internal(format!(
                        "child SessionInbox target {target_session_id} does not share its parent owner's root"
                    )));
                }
                let model = if session.model.trim().is_empty() {
                    parent.model.clone()
                } else {
                    session.model.clone()
                };
                if model.trim().is_empty() {
                    return Err(bamboo_domain::SessionActivationError::Internal(format!(
                        "child SessionInbox target {target_session_id} has no executable model"
                    )));
                }
                let disabled_tools = match session.metadata.get("disabled_tools") {
                    None => None,
                    Some(raw) => {
                        let tools = serde_json::from_str::<std::collections::BTreeSet<String>>(raw)
                            .map_err(|error| {
                                bamboo_domain::SessionActivationError::Internal(format!(
                                    "child SessionInbox target {target_session_id} has malformed disabled_tools: {error}"
                                ))
                            })?;
                        (!tools.is_empty()).then(|| tools.into_iter().collect())
                    }
                };
                LaunchPlan::Child {
                    scheduler,
                    parent_session_id,
                    model,
                    disabled_tools,
                }
            }
        };
        let event_sender =
            ResumeExecutionPort::get_or_create_event_sender(self, target_session_id).await;

        let reservation = match reserve_runner_core(
            &self.agent_runners,
            &self.session_event_senders,
            target_session_id,
            &event_sender,
        )
        .await
        {
            ReserveOutcome::Reserved(reservation) => reservation,
            ReserveOutcome::AlreadyRunning(run_id) => {
                return Ok(SessionActivationReserveOutcome::AlreadyRunning { run_id });
            }
        };
        let run_id = reservation.run_id.clone();
        let launch = match launch_plan {
            LaunchPlan::Root(config) => {
                let execution_reservation =
                    SessionExecutionReservation::from_activation_placeholder(
                        target_session_id,
                        reservation,
                        self.agent
                            .activation_router()
                            .expect("SessionInbox activation requires an activation router")
                            .clone(),
                        self.agent_runners.clone(),
                    );
                // Launch and rollback share one exact RAII reservation. Dropping
                // an unlaunched SessionActivationLaunch cannot race a raw slot
                // removal against the reservation's router-placeholder cleanup.
                let reservation_cell = Arc::new(StdMutex::new(Some(execution_reservation)));
                let launch_reservation = reservation_cell.clone();
                let rollback_reservation = reservation_cell;
                let coordinator = self.clone();
                let launch_sessions = self.sessions.clone();
                let launch_session_id = session.id.clone();
                let launch_session = session.clone();
                let request_session_id = target_session_id.to_string();
                SessionActivationLaunch::new_with_async_rollback(
                    run_id,
                    move || {
                        let mut execution_reservation = launch_reservation
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take()
                            .expect("activation reservation launches or rolls back exactly once");
                        execution_reservation.mark_activation_published();
                        // The router publishes the exact owner before invoking
                        // this closure, so only now may the prepared snapshot
                        // replace the shared cache entry.
                        launch_sessions.insert(
                            launch_session_id,
                            Arc::new(crate::SessionSnapshot::new(launch_session)),
                        );
                        let request = ResumeSpawnRequest {
                            session_id: request_session_id,
                            session,
                            execution_reservation,
                            event_sender,
                            config: *config,
                        };
                        tokio::spawn(async move {
                            ResumeExecutionPort::spawn_resume_execution(&coordinator, request)
                                .await;
                        });
                    },
                    move || async move {
                        let reservation = rollback_reservation
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                        if let Some(reservation) = reservation {
                            reservation.rollback_unpublished_activation().await;
                        }
                    },
                )
            }
            LaunchPlan::Child {
                scheduler,
                parent_session_id,
                model,
                disabled_tools,
            } => {
                let execution_reservation =
                    SessionExecutionReservation::from_activation_placeholder(
                        target_session_id,
                        reservation,
                        self.agent
                            .activation_router()
                            .expect("SessionInbox activation requires an activation router")
                            .clone(),
                        self.agent_runners.clone(),
                    );
                let reservation_cell = Arc::new(StdMutex::new(Some(execution_reservation)));
                let launch_reservation = reservation_cell.clone();
                let rollback_reservation = reservation_cell;
                let job = SpawnJob {
                    parent_session_id,
                    child_session_id: target_session_id.to_string(),
                    model,
                    disabled_tools,
                };
                let launch_sessions = self.sessions.clone();
                let launch_session_id = session.id.clone();
                let launch_session = session;
                SessionActivationLaunch::new_with_async_rollback(
                    run_id,
                    move || {
                        let mut execution_reservation = launch_reservation
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take()
                            .expect("activation reservation launches or rolls back exactly once");
                        execution_reservation.mark_activation_published();
                        // As with root activation, publish the prepared cache
                        // snapshot only after router ownership commits.
                        launch_sessions.insert(
                            launch_session_id,
                            Arc::new(crate::SessionSnapshot::new(launch_session)),
                        );
                        // Dropping a JoinHandle detaches the task. The captured
                        // combined reservation remains RAII-protected if the
                        // task is later aborted or unwinds during setup.
                        drop(scheduler.launch_reserved(job, execution_reservation));
                    },
                    move || async move {
                        let reservation = rollback_reservation
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                        if let Some(reservation) = reservation {
                            reservation.rollback_unpublished_activation().await;
                        }
                    },
                )
            }
        };
        Ok(SessionActivationReserveOutcome::Reserved(launch))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BashCompletionDeliveryPlan {
    /// Preserve the durable wait and do not reserve a successor. The last
    /// sibling completion (or the wait backstop) activates the accumulated
    /// inbox in order.
    DurableOnly,
    /// The loop is live/not waiting; notify its current owner after admission.
    Activate,
    /// The last waited shell finished: clear the wait durably, then activate.
    ClearWaitThenActivate,
}

fn bash_completion_delivery_plan(
    loop_suspended_on_bash: bool,
    all_waited_shells_done: bool,
) -> BashCompletionDeliveryPlan {
    if bash_completion_should_resume(loop_suspended_on_bash, all_waited_shells_done) {
        BashCompletionDeliveryPlan::ClearWaitThenActivate
    } else if loop_suspended_on_bash {
        BashCompletionDeliveryPlan::DurableOnly
    } else {
        BashCompletionDeliveryPlan::Activate
    }
}

fn background_bash_post_tool_payload(info: &BashCompletionInfo) -> HookPayload {
    let success = info.status == "completed" && info.exit_code == Some(0);
    let response = serde_json::json!({
        "bash_id": info.bash_id,
        "command": info.command,
        "exit_code": info.exit_code,
        "status": info.status,
        "output_tail": info.output_tail,
    });
    HookPayload::ToolResult {
        tool_name: "Bash".to_string(),
        tool_call_id: info.bash_id.clone(),
        outcome: HookToolOutcome {
            success,
            result: success.then(|| response.to_string()),
            error: (!success).then(|| response.to_string()),
            needs_human: false,
            duration_ms: 0,
        },
    }
}

fn append_background_bash_hook_feedback(info: &mut BashCompletionInfo, feedback: Vec<String>) {
    let feedback = feedback
        .into_iter()
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>();
    if feedback.is_empty() {
        return;
    }
    if !info.output_tail.is_empty() {
        info.output_tail.push_str("\n\n");
    }
    info.output_tail.push_str("<post_tool_use_feedback>\n");
    info.output_tail.push_str(&feedback.join("\n"));
    info.output_tail.push_str("\n</post_tool_use_feedback>");
}

async fn run_background_bash_post_tool_hooks(
    config: &bamboo_config::LifecycleHooksConfig,
    fallback_cwd: Option<std::path::PathBuf>,
    session: &Session,
    info: &mut BashCompletionInfo,
) -> bool {
    let runner = crate::HookRunner::new().with_lifecycle_config(config, fallback_cwd);
    if !runner.has_hooks_for(AgentHookPoint::AfterToolExecution) {
        return false;
    }

    let mut runtime_state = session
        .agent_runtime_state
        .clone()
        .unwrap_or_else(|| AgentRuntimeState::new(&session.id));
    let outcome = runner
        .run_observer_hooks(
            AgentHookPoint::AfterToolExecution,
            &background_bash_post_tool_payload(info),
            session,
            &mut runtime_state,
            None,
        )
        .await;
    append_background_bash_hook_feedback(info, outcome.injected_contexts);
    true
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

fn bash_completion_envelope(info: &BashCompletionInfo) -> bamboo_domain::SessionMessageEnvelope {
    let provider_message = bash_resume_message_from_info(info);
    let data = serde_json::json!({
        "session_id": info.session_id,
        "bash_id": info.bash_id,
        "command": info.command,
        "exit_code": info.exit_code,
        "status": info.status,
        "output_tail": info.output_tail,
    });
    // The id covers the immutable completion snapshot. An exact delivery retry
    // (whose transport timestamp may differ) is idempotent; a changed status,
    // output tail, command, or exit code is a distinct correction rather than
    // silently reusing one id with different semantics.
    let identity = data.clone();
    bamboo_domain::SessionMessageEnvelope {
        id: bamboo_domain::SessionMessageId::stable("background_bash_completion", &identity),
        source: bamboo_domain::SessionMessageSource::Runtime {
            subsystem: "background_bash".to_string(),
        },
        target_session_id: info.session_id.clone(),
        kind: bamboo_domain::SessionMessageKind::RuntimeInstruction,
        body: bamboo_domain::SessionMessageBody::RuntimeInstruction(
            bamboo_domain::SessionRuntimeInstruction {
                instruction: "background_bash_completed".to_string(),
                content: Some(bamboo_domain::SessionMessageContent::text(
                    bash_completion_injection_body(info),
                )),
                data: Some(data),
                provider_message: Some(session_provider_message(&provider_message)),
            },
        ),
        created_at: Utc::now(),
        thread_id: None,
        in_reply_to: None,
        attempt: None,
        correlation_id: Some(info.bash_id.clone()),
    }
}

/// Enqueue a completed shell's summary as a pending injected message on the
/// owning session for the rolling-upgrade polling backstop. New push delivery
/// uses [`SessionMessenger`]; this helper remains only so an older process that
/// has not wired the typed delivery plane can still resume persisted sessions.
/// Race-safe: `update_runtime_config` loads and saves under the per-session lock.
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
    /// - Every notice is then delivered through the same typed SessionMessenger
    ///   as peer/user steering. The durable inbox and activation router decide
    ///   whether to notify the current owner or reserve one successor.
    async fn deliver_bash_completion(&self, mut info: BashCompletionInfo) {
        // A background shell completes outside the originating tool call, so
        // fire its PostToolUse seam here before routing the completion into the
        // next round/resume message. Do not hold the resume lock while an
        // arbitrary command hook runs: its configured timeout must never block
        // the independent liveness backstop.
        if let Some(hook_session) = self.load_session(&info.session_id).await {
            let config_snapshot = self.config.read().await.clone();
            let _ = run_background_bash_post_tool_hooks(
                &config_snapshot.lifecycle_hooks,
                Some(self.app_data_dir.clone()),
                &hook_session,
                &mut info,
            )
            .await;
        }

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
        let delivery_plan = bash_completion_delivery_plan(waiting, all_shells_done);

        // Stage 1: make the completion durable BEFORE clearing the wait. The
        // activation is intentionally deferred until the wait-state mutation
        // below is durably committed; otherwise a successor can observe the old
        // suspension, while clear-before-deliver can lose the only wake on
        // crash.
        let messenger = self.agent.session_messenger().cloned();
        let admission = match messenger.as_ref() {
            Some(messenger) => match messenger.admit(bash_completion_envelope(&info)).await {
                Ok(admission) => Some(admission),
                Err(error) => {
                    tracing::warn!(
                        session_id = %info.session_id,
                        bash_id = %info.bash_id,
                        %error,
                        "background bash completion: durable SessionInbox admission failed; leaving wait armed"
                    );
                    return;
                }
            },
            None => {
                tracing::warn!(
                    session_id = %info.session_id,
                    bash_id = %info.bash_id,
                    "SessionMessenger unavailable; using compatibility injection before clearing wait"
                );
                match enqueue_bash_completion_injection(&self.persistence, &info).await {
                    Ok(Some(_)) => None,
                    Ok(None) => {
                        tracing::warn!(
                            session_id = %info.session_id,
                            "background bash compatibility target disappeared"
                        );
                        return;
                    }
                    Err(error) => {
                        tracing::warn!(
                            session_id = %info.session_id,
                            %error,
                            "background bash compatibility admission failed; leaving wait armed"
                        );
                        return;
                    }
                }
            }
        };

        if delivery_plan == BashCompletionDeliveryPlan::DurableOnly {
            tracing::info!(
                session_id = %info.session_id,
                bash_id = %info.bash_id,
                "background bash completion is durable; sibling shells still run, so wait remains armed"
            );
            return;
        }

        if let (Some(messenger), Some(admission)) = (messenger.as_ref(), admission.as_ref()) {
            if let Err(error) = messenger.prepare_activation(admission).await {
                tracing::warn!(
                    session_id = %info.session_id,
                    bash_id = %info.bash_id,
                    %error,
                    "background bash completion activation watermark failed; leaving wait armed"
                );
                return;
            }
        }

        if delivery_plan == BashCompletionDeliveryPlan::ClearWaitThenActivate {
            tracing::info!(
                session_id = %info.session_id,
                bash_id = %info.bash_id,
                status = %info.status,
                "background bash completion: push-resuming suspended loop (event-driven)"
            );
            let mut resumable = session.clone();
            let mut runtime_state = read_runtime_state(&resumable);
            runtime_state.waiting_for_bash = None;
            runtime_state.status = AgentStatusState::Idle;
            runtime_state.suspension = None;
            write_runtime_state(&mut resumable, &runtime_state);
            resumable.metadata.remove("runtime.suspend_reason");
            resumable.updated_at = Utc::now();
            if let Err(error) = self.persistence.merge_save_runtime(&mut resumable).await {
                tracing::warn!(
                    session_id = %info.session_id,
                    %error,
                    "background bash completion is durable but wait-state clear failed; leaving activation to the wait backstop"
                );
                return;
            }
            self.sessions.insert(
                resumable.id.clone(),
                Arc::new(crate::SessionSnapshot::new(resumable)),
            );
        }

        // Stage 3: activation is allowed only after the durable wait-state
        // clear. A crash before here leaves either the wait backstop or startup
        // inbox reconciliation able to recover.
        let (Some(messenger), Some(admission)) = (messenger, admission) else {
            if delivery_plan == BashCompletionDeliveryPlan::ClearWaitThenActivate {
                let _ = self.resume_parent(info.session_id.clone()).await;
            }
            return;
        };
        match messenger.activate_prepared(&admission).await {
            Ok(receipt) => tracing::info!(
                session_id = %info.session_id,
                bash_id = %info.bash_id,
                status = %info.status,
                waiting,
                generation = receipt.delivery.generation,
                activation = ?receipt.activation,
                "background bash completion delivered through SessionMessenger"
            ),
            Err(error) => tracing::warn!(
                session_id = %info.session_id,
                bash_id = %info.bash_id,
                %error,
                "background bash completion: SessionMessenger delivery failed"
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

// ---------------------------------------------------------------------------
// Child-wait watchdog (issue #546)
// ---------------------------------------------------------------------------

/// How often the child-wait watchdog sweeps suspended sessions.
const CHILD_WAIT_SWEEP_INTERVAL_SECS: u64 = 30;
/// Leave a freshly registered wait alone for this long so the event-driven
/// completion push always gets the first shot (and just-enqueued spawn jobs
/// have time to persist their running marker).
const CHILD_WAIT_REGISTRATION_GRACE_SECS: i64 = 60;
/// A waited child with NO live runner and a non-terminal index status is
/// declared dead once its control-plane has been quiet for this long.
const DEAD_CHILD_GRACE_SECS: i64 = 120;
/// Slack on top of the per-child liveness policy before a Running-but-frozen
/// runner entry (dead task) is force-finalized by the sweeper. The per-child
/// watchdog cancels at `max_idle`/`max_total` and the child then publishes its
/// own timeout; an entry frozen this far PAST those limits proves that
/// machinery is dead.
const STALE_RUNNER_SLACK_SECS: i64 = 600;

/// Whether a waited child's index status means the sweeper must consider it
/// DEAD when nothing is driving it: non-terminal and not legitimately
/// suspended. `None` = never ran (created-but-never-started, or a spawn that
/// was lost before its running marker persisted).
fn is_dead_child_candidate_status(status: Option<&str>) -> bool {
    match status {
        // "suspended" children wait on a human / their own children / bash —
        // their wake has its own driver; never declare them dead here.
        Some(status) => !is_terminal_child_status(status) && status != "suspended",
        None => true,
    }
}

/// Whether a reported completion's child id is genuinely a child of the parent
/// it claims (issue #546 read-side disclosure guard). `SubAgent.wait` ids are
/// model-provided, and the watchdog resolves an unowned id by publishing a
/// synthetic completion so the parent is unstranded — but the parent must NEVER
/// receive a FOREIGN session's content folded into its transcript.
/// `child_parent_linkage` is that session's own `parent_session_id` (`None`
/// when the session does not exist). Pure so the rule is unit-testable.
fn completion_child_is_owned(reported_parent: &str, child_parent_linkage: Option<&str>) -> bool {
    child_parent_linkage == Some(reported_parent)
}

/// Pick which terminal child's completion to replay when the wait is already
/// satisfied but the parent is still suspended (lost wake). Prefer an
/// error-like child so a `FirstError` policy re-evaluates truthfully.
fn select_replay_child(terminal: &[(String, String)]) -> Option<&(String, String)> {
    terminal
        .iter()
        .find(|(_, status)| is_error_like(status))
        .or_else(|| terminal.last())
}

fn child_wait_watchdog_resume_message(body: String) -> Message {
    let mut message = Message::user(body);
    message.metadata = Some(serde_json::json!({
        RUNTIME_RESUME_MESSAGE_HIDDEN_KEY: true,
        RUNTIME_RESUME_MESSAGE_KIND_KEY: "child_wait_watchdog_resume",
    }));
    message.never_compress = false;
    message
}

fn empty_child_wait_message() -> Message {
    child_wait_watchdog_resume_message(
        "Runtime notification: this session was suspended waiting for child sessions, but the \
         wait tracked no children (internal inconsistency). The session has been resumed; use \
         SubAgent.list to inspect child state and continue the task."
            .to_string(),
    )
}

fn child_wait_lease_expired_message(child_ids: &[String]) -> Message {
    child_wait_watchdog_resume_message(format!(
        "Runtime notification: the wait lease for child session(s) [{}] expired before they all \
         reported completion. They were NOT cancelled and may still be running or already \
         finished — verify their actual status with SubAgent.list / SubAgent.get before assuming \
         anything, then continue the task.",
        child_ids.join(", ")
    ))
}

/// Child-wait watchdog (issue #546): the heartbeat backstop for parents
/// suspended on `waiting_for_children`.
///
/// The primary wake is the event-driven completion push (child terminal →
/// [`ChildCompletionHandler::on_child_completed`] → resume). This sweeper
/// exists because ANY break in that chain — a panicked child task, a dead
/// spawn scheduler, a process restart, a clobbered/exhausted resume, a wait
/// registered over an already-terminal child — previously stranded the parent
/// forever. It mirrors the bash backstop's philosophy: coarse, cheap, yields
/// to the push, and only acts when the durable state proves nothing else can.
///
/// All wake decisions funnel through the SAME machinery the push uses
/// (synthetic/replayed completions → `on_child_completed`, per-parent
/// serialization via [`session_resume_lock`]), so there is exactly one resume
/// implementation.
impl ChildCompletionCoordinator {
    /// Spawn the watchdog: one boot-time reconciliation pass, then a sweep
    /// every [`CHILD_WAIT_SWEEP_INTERVAL_SECS`]. Call once at server startup.
    pub fn spawn_child_wait_watchdog(self: &Arc<Self>) {
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            use futures::FutureExt;
            if std::panic::AssertUnwindSafe(coordinator.reconcile_orphans_at_boot())
                .catch_unwind()
                .await
                .is_err()
            {
                tracing::error!("child-wait watchdog: boot reconciliation panicked");
            }
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(
                CHILD_WAIT_SWEEP_INTERVAL_SECS,
            ));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate tick (boot reconciliation just ran).
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if std::panic::AssertUnwindSafe(coordinator.sweep_child_waits())
                    .catch_unwind()
                    .await
                    .is_err()
                {
                    tracing::error!("child-wait watchdog: sweep panicked; continuing");
                }
            }
        });
    }

    /// One-shot startup reconciliation: a process restart kills every in-flight
    /// child task AND the in-memory bash backstop polls, but the durable state
    /// (child index status `running`, parents suspended on bash) survives —
    /// previously stranding those parents forever.
    async fn reconcile_orphans_at_boot(&self) {
        let cutoff = Utc::now();

        // (1) Children left `running` by the previous process: no task in THIS
        // process is driving them, so no completion will ever fire. Mark them
        // terminal and wake their parents through the canonical path. (Root
        // sessions left `running` are user-visible and user-recoverable; only
        // children hold a suspended parent hostage.)
        let running = self
            .storage
            .list_sessions_by_run_status("running")
            .await
            .unwrap_or_default();
        for (child_id, parent_id) in running {
            let Some(parent_id) = parent_id else { continue };
            if self.runner_is_running(&child_id).await {
                continue;
            }
            let Some(control_plane) = self.load_control_plane(&child_id).await else {
                continue;
            };
            // A child that started AFTER boot is alive by definition — the
            // cutoff guards the tiny window where a fresh spawn races this scan.
            if control_plane.updated_at >= cutoff {
                continue;
            }
            tracing::warn!(
                child_session_id = %child_id,
                parent_session_id = %parent_id,
                "boot reconciliation: child was running when the process died; \
                 marking it error and waking the parent"
            );
            self.synthesize_child_completion(
                &parent_id,
                &child_id,
                "error",
                Some(
                    "orphaned by server restart: the process died while this child session \
                     was running"
                        .to_string(),
                ),
            )
            .await;
        }

        // (2) Sessions suspended on `waiting_for_bash`: their backstop poll was
        // an in-memory task that died with the process. Re-arm it — with the
        // shell registry empty after a restart it resumes on its first check.
        let suspended = self
            .storage
            .list_sessions_by_run_status("suspended")
            .await
            .unwrap_or_default();
        for (session_id, _) in suspended {
            let Some(control_plane) = self.load_control_plane(&session_id).await else {
                continue;
            };
            if control_plane
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str)
                != Some("waiting_for_bash")
            {
                continue;
            }
            if let Some(wait) = read_runtime_state(&control_plane).waiting_for_bash {
                tracing::warn!(
                    %session_id,
                    "boot reconciliation: re-arming bash self-resume backstop lost in restart"
                );
                let coordinator = self.clone();
                tokio::spawn(async move {
                    coordinator
                        .bash_self_resume(session_id, wait.bash_ids)
                        .await;
                });
            }
        }
    }

    async fn runner_is_running(&self, session_id: &str) -> bool {
        let runners = self.agent_runners.read().await;
        runners
            .get(session_id)
            .is_some_and(|runner| matches!(runner.status, AgentStatus::Running))
    }

    async fn load_control_plane(&self, session_id: &str) -> Option<Session> {
        match self.storage.load_runtime_control_plane(session_id).await {
            Ok(session) => session,
            Err(error) => {
                tracing::warn!(
                    %session_id,
                    %error,
                    "child-wait watchdog: failed to load session control plane"
                );
                None
            }
        }
    }

    /// One sweep: inspect every session whose index status is `suspended`.
    /// Cheap in the common case — the candidate list is small and each check is
    /// a sidecar (control-plane) load.
    async fn sweep_child_waits(&self) {
        let suspended = match self.storage.list_sessions_by_run_status("suspended").await {
            Ok(entries) => entries,
            Err(error) => {
                tracing::warn!(%error, "child-wait watchdog: failed to list suspended sessions");
                return;
            }
        };
        for (session_id, _) in suspended {
            self.sweep_one_suspended_session(&session_id).await;
        }
    }

    async fn sweep_one_suspended_session(&self, session_id: &str) {
        // A live runner means the session already resumed (the index status
        // only advances at its next terminal) — nothing to do.
        if self.runner_is_running(session_id).await {
            return;
        }
        let Some(session) = self.load_control_plane(session_id).await else {
            return;
        };
        let runtime_state = read_runtime_state(&session);
        let suspend_reason = session
            .metadata
            .get("runtime.suspend_reason")
            .map(String::as_str)
            .unwrap_or_default()
            .to_string();
        match (
            suspend_reason.as_str(),
            runtime_state.waiting_for_children.clone(),
        ) {
            // Human-gated or bash-owned waits: not ours to time out.
            ("waiting_for_bash", _)
            | ("awaiting_clarification", _)
            | ("awaiting_parent_approval", _) => {}
            (_, Some(wait)) => self.sweep_child_wait(session_id, wait).await,
            // Suspended with NO armed wait: either the coordinator cleared the
            // wait but its resume never spawned, or state is half-cleared.
            ("waiting_for_children", None) | ("", None) => {
                self.rescue_stranded_resume(session_id).await;
            }
            _ => {}
        }
    }

    /// Evaluate one armed child wait against reality and act on what the
    /// durable state proves: dead children are synthesized terminal, an
    /// already-satisfied wait gets its lost wake replayed, an expired lease or
    /// empty wait force-resumes the parent.
    async fn sweep_child_wait(&self, parent_session_id: &str, wait: WaitingForChildrenState) {
        let now = Utc::now();

        if wait.child_session_ids.is_empty() {
            tracing::warn!(
                %parent_session_id,
                "child-wait watchdog: wait armed over an empty child set; force-resuming"
            );
            self.force_resume_child_wait(parent_session_id, empty_child_wait_message())
                .await;
            return;
        }

        // The 6h lease (previously written but never read). Expiry does not
        // kill children — child runners own child liveness; the parent is
        // resumed with a verify-don't-assume note.
        if wait.timeout_at.is_some_and(|deadline| now >= deadline) {
            tracing::warn!(
                %parent_session_id,
                "child-wait watchdog: wait lease expired; force-resuming parent"
            );
            self.force_resume_child_wait(
                parent_session_id,
                child_wait_lease_expired_message(&wait.child_session_ids),
            )
            .await;
            return;
        }

        // Yield to the event-driven push on fresh waits.
        if now.signed_duration_since(wait.registered_at).num_seconds()
            < CHILD_WAIT_REGISTRATION_GRACE_SECS
        {
            return;
        }

        let statuses: HashMap<String, Option<String>> = self
            .storage
            .list_child_run_statuses(parent_session_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .collect();

        struct DeadChild {
            child_id: String,
            status: String,
            reason: String,
            /// `false` = the id does not name a child of THIS parent (wait ids
            /// are model-provided and unvalidated): publish the wake but never
            /// persist onto / cancel / finalize the named session.
            owned: bool,
        }

        let mut terminal: Vec<(String, String)> = Vec::new();
        let mut dead: Vec<DeadChild> = Vec::new();
        for child_id in &wait.child_session_ids {
            let status = statuses.get(child_id).and_then(|status| status.as_deref());
            if let Some(status) = status {
                if is_terminal_child_status(status) {
                    terminal.push((child_id.clone(), status.to_string()));
                    continue;
                }
            }
            if !is_dead_child_candidate_status(status) {
                continue;
            }

            // Ownership check BEFORE anything destructive: `SubAgent.wait` ids
            // are model-provided and unvalidated, so an id absent from the
            // parent-scoped status map may name a REAL session in another tree
            // (a grandchild, a foreign root). Such a session must never be
            // mutated, cancelled, or finalized here — but the parent's bogus
            // wait entry still needs a synthetic completion to clear it.
            let control_plane = self.load_control_plane(child_id).await;
            let owned = control_plane
                .as_ref()
                .is_some_and(|cp| cp.parent_session_id.as_deref() == Some(parent_session_id));
            if !owned {
                dead.push(DeadChild {
                    child_id: child_id.clone(),
                    status: "error".to_string(),
                    reason: if control_plane.is_some() {
                        "waited-on session id is not a child of this session; clearing it \
                         from the wait without touching that session"
                            .to_string()
                    } else {
                        "waited-on child session does not exist".to_string()
                    },
                    owned: false,
                });
                continue;
            }

            let runner = { self.agent_runners.read().await.get(child_id).cloned() };
            match runner {
                Some(runner) if matches!(runner.status, AgentStatus::Running) => {
                    // A live-looking runner: only intervene when it is frozen
                    // far PAST the per-child liveness limits — which proves the
                    // per-child watchdog machinery itself is dead (task
                    // panicked or lost), because it would have cancelled and
                    // published a timeout long before.
                    let last_activity = runner.last_activity_at().unwrap_or(runner.started_at);
                    let idle_secs = now.signed_duration_since(last_activity).num_seconds();
                    let total_secs = now.signed_duration_since(runner.started_at).num_seconds();
                    let policy = match &control_plane {
                        Some(child) => {
                            crate::runtime::execution::spawn::watchdog_policy_for_session(child)
                        }
                        None => Default::default(),
                    };
                    let idle_limit = policy.max_idle_secs.saturating_add(STALE_RUNNER_SLACK_SECS);
                    let total_limit = policy
                        .max_total_secs
                        .saturating_add(STALE_RUNNER_SLACK_SECS);
                    if idle_secs >= idle_limit || total_secs >= total_limit {
                        runner.cancel_token.cancel();
                        dead.push(DeadChild {
                            child_id: child_id.clone(),
                            status: "timeout".to_string(),
                            reason: format!(
                                "child runner stalled: no events for {idle_secs}s \
                                 (limit {idle_limit}s including watchdog slack); \
                                 force-finalized by the child-wait watchdog"
                            ),
                            owned: true,
                        });
                    }
                }
                _ => {
                    // Nothing is driving this child, yet its index status will
                    // never advance by itself. The grace covers the enqueue →
                    // running-marker window and slow spawn queues.
                    let quiet_secs = control_plane
                        .as_ref()
                        .map(|child| now.signed_duration_since(child.updated_at).num_seconds())
                        .unwrap_or(i64::MAX);
                    if quiet_secs >= DEAD_CHILD_GRACE_SECS {
                        dead.push(DeadChild {
                            child_id: child_id.clone(),
                            status: "error".to_string(),
                            reason: format!(
                                "child runner lost (crashed task, dropped spawn job, or \
                                 process restart): index status {status:?} with no live \
                                 runner driving it"
                            ),
                            owned: true,
                        });
                    }
                }
            }
        }

        if !dead.is_empty() {
            for entry in dead {
                tracing::warn!(
                    %parent_session_id,
                    child_session_id = %entry.child_id,
                    status = %entry.status,
                    reason = %entry.reason,
                    owned = entry.owned,
                    "child-wait watchdog: synthesizing terminal completion for dead child"
                );
                if entry.owned {
                    self.synthesize_child_completion(
                        parent_session_id,
                        &entry.child_id,
                        &entry.status,
                        Some(entry.reason),
                    )
                    .await;
                } else {
                    // Foreign / nonexistent id: wake the parent only.
                    self.publish_synthetic_completion(
                        parent_session_id,
                        &entry.child_id,
                        &entry.status,
                        Some(entry.reason),
                    )
                    .await;
                }
            }
            // The publishes above re-evaluate the wait policy themselves.
            return;
        }

        // No dead children — but if the terminal set ALREADY satisfies the
        // policy, the original wake was lost (clobbered resume / retry budget
        // exhausted / completion raced the wait registration). Replay one real
        // completion through the canonical path; `on_child_completed` is
        // idempotent for an already-cleared wait.
        let terminal_ids: Vec<String> = terminal.iter().map(|(id, _)| id.clone()).collect();
        if let Some((child_id, status)) = select_replay_child(&terminal) {
            if wait_policy_satisfied(
                wait.wait_for,
                &wait.child_session_ids,
                &terminal_ids,
                child_id,
                status,
            ) {
                tracing::warn!(
                    %parent_session_id,
                    child_session_id = %child_id,
                    "child-wait watchdog: wait already satisfied but parent still suspended \
                     (lost wake); replaying the completion"
                );
                let error = self
                    .load_control_plane(child_id)
                    .await
                    .and_then(|child| child.last_run_error());
                self.publish_synthetic_completion(parent_session_id, child_id, status, error)
                    .await;
            }
        }
    }

    /// Persist a synthesized terminal status on the child (so the index flips
    /// and nothing re-detects or re-suspends on it), finalize any lingering
    /// runner entry (so a future re-run can reserve), then publish through the
    /// canonical completion path — broadcast + `on_child_completed`, exactly
    /// like a real child terminal.
    async fn synthesize_child_completion(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
        status: &str,
        error: Option<String>,
    ) {
        match self.storage.load_session(child_session_id).await {
            Ok(Some(mut child)) => {
                // Ownership guard (defense in depth — callers check too): only
                // a session that IS a child of this parent may be mutated. An
                // arbitrary session id named in a wait still wakes the parent
                // via the publish below, but its own state stays untouched.
                if child.parent_session_id.as_deref() != Some(parent_session_id) {
                    tracing::warn!(
                        %parent_session_id,
                        child_session_id = %child.id,
                        "child-wait watchdog: refusing to synthesize status onto a session \
                         that is not a child of this parent"
                    );
                    self.publish_synthetic_completion(
                        parent_session_id,
                        child_session_id,
                        status,
                        error,
                    )
                    .await;
                    return;
                }
                child.set_last_run_status(status);
                match &error {
                    Some(message) => child.set_last_run_error(message.clone()),
                    None => child.clear_last_run_error(),
                }
                child.updated_at = Utc::now();
                if let Err(save_error) = self.persistence.merge_save_runtime(&mut child).await {
                    tracing::warn!(
                        child_session_id = %child.id,
                        %save_error,
                        "child-wait watchdog: failed to persist synthesized terminal status"
                    );
                }
                self.sessions.insert(
                    child.id.clone(),
                    Arc::new(crate::SessionSnapshot::new(child)),
                );
            }
            Ok(None) => {}
            Err(load_error) => {
                tracing::warn!(
                    %child_session_id,
                    %load_error,
                    "child-wait watchdog: failed to load child for synthesized terminal status"
                );
            }
        }
        finalize_runner(
            &self.agent_runners,
            child_session_id,
            &Err(bamboo_agent_core::AgentError::LLM(
                error
                    .clone()
                    .unwrap_or_else(|| format!("synthesized {status}")),
            )),
        )
        .await;
        self.publish_synthetic_completion(parent_session_id, child_session_id, status, error)
            .await;
    }

    async fn publish_synthetic_completion(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
        status: &str,
        error: Option<String>,
    ) {
        let publisher =
            crate::runtime::execution::session_events::ReplayableSessionEventPublisher::new(
                self.agent_runners.clone(),
                self.session_event_senders.clone(),
                self.account_feed_inbox.clone(),
            );
        let handler: Arc<dyn ChildCompletionHandler> = Arc::new(self.clone());
        crate::runtime::execution::spawn::publish_child_completion_parts(
            &publisher,
            Some(handler),
            parent_session_id.to_string(),
            child_session_id.to_string(),
            status.to_string(),
            error,
        )
        .await;
    }

    /// A parent whose wait was already cleared (resume message appended) but
    /// whose resume never spawned — retry-budget exhaustion, root-tools not
    /// yet initialized, or a restart between clear and spawn. Detected by: no
    /// live runner, no armed wait, and a pending hidden runtime resume message
    /// as the LAST message. Resume is all that's left to do.
    async fn rescue_stranded_resume(&self, session_id: &str) {
        let Some(session) = self.load_session(session_id).await else {
            return;
        };
        let pending_runtime_resume = session.messages.last().is_some_and(|message| {
            matches!(message.role, Role::User)
                && message
                    .metadata
                    .as_ref()
                    .is_some_and(|meta| meta.get(RUNTIME_RESUME_MESSAGE_KIND_KEY).is_some())
        });
        if !pending_runtime_resume {
            return;
        }
        tracing::warn!(
            %session_id,
            "child-wait watchdog: stranded resume detected (wait cleared, resume never \
             spawned); resuming"
        );
        self.resume_parent(session_id.to_string()).await;
    }

    /// Clear the parent's child wait, append `resume_message`, and drive the
    /// resume — with a bounded clobber-retry mirroring
    /// [`Self::perform_bash_resume`]: a suspending runner's one-shot finalize
    /// save can land after ours and revert the wait while dropping the
    /// message; we detect the re-armed wait and re-clear.
    async fn force_resume_child_wait(&self, session_id: &str, resume_message: Message) {
        const MAX_ATTEMPTS: u8 = 5;
        let lock = session_resume_lock(session_id);
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            {
                let _held = lock.lock().await;
                let Some(mut session) = self.load_session(session_id).await else {
                    return;
                };
                let mut runtime_state = read_runtime_state(&session);
                if runtime_state.waiting_for_children.is_none() {
                    // Another source already resumed this parent.
                    return;
                }
                runtime_state.waiting_for_children = None;
                runtime_state.status = AgentStatusState::Idle;
                runtime_state.suspension = None;
                write_runtime_state(&mut session, &runtime_state);
                session.metadata.remove("runtime.suspend_reason");
                session.add_message(resume_message.clone());
                session.updated_at = Utc::now();
                self.save_and_cache(&mut session).await;
            }
            let outcome = self.resume_parent(session_id.to_string()).await;
            match outcome {
                ResumeOutcome::Started { .. } | ResumeOutcome::NotFound => return,
                ResumeOutcome::Completed | ResumeOutcome::AlreadyRunning { .. } => {
                    // Only retry when the persisted wait was clobbered back to
                    // armed; if it stayed cleared with the message intact, the
                    // next sweep's stranded-resume rescue finishes the job.
                    let clobbered = self
                        .load_session(session_id)
                        .await
                        .map(|session| read_runtime_state(&session).waiting_for_children.is_some())
                        .unwrap_or(false);
                    if !clobbered {
                        return;
                    }
                }
            }
        }
        tracing::error!(
            %session_id,
            "child-wait watchdog: force-resume exhausted its clobber-retry budget"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_resume_waiters_do_not_retain_historical_parent_ids() {
        for index in 0..512 {
            let id = format!("resume-lock-reclaim-{index}");
            let owner = session_resume_lock(&id);
            let held = owner.lock().await;
            let waiter = session_resume_lock(&id);
            let mut waiting = Box::pin(waiter.lock());
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            drop(held);
            drop(owner);
            drop(waiting);
            drop(waiter);
            assert!(!parent_locks().lock().recover_poison().contains_key(&id));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resume_lock_reclamation_preserves_exclusive_parent_wake_ownership() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let active = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let active = active.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..16 {
                    let lease = session_resume_lock("resume-lock-exclusive");
                    let _guard = lease.lock().await;
                    assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                    tokio::task::yield_now().await;
                    assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert!(!parent_locks()
            .lock()
            .recover_poison()
            .contains_key("resume-lock-exclusive"));
    }
    use bamboo_agent_core::Message;
    use bamboo_domain::SessionInboxPort;
    use futures::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct EmptyTools;

    #[async_trait]
    impl bamboo_agent_core::tools::ToolExecutor for EmptyTools {
        async fn execute(
            &self,
            _call: &bamboo_agent_core::tools::ToolCall,
        ) -> Result<bamboo_agent_core::tools::ToolResult, bamboo_agent_core::tools::ToolError>
        {
            Err(bamboo_agent_core::tools::ToolError::NotFound(
                "no tools".to_string(),
            ))
        }

        fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
            Vec::new()
        }
    }

    struct CompletedTestProvider;

    #[async_trait]
    impl bamboo_llm::LLMProvider for CompletedTestProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
            let chunks: Vec<bamboo_llm::provider::Result<bamboo_llm::LLMChunk>> = vec![
                Ok(bamboo_llm::LLMChunk::Token("done".to_string())),
                Ok(bamboo_llm::LLMChunk::Done),
            ];
            Ok(Box::pin(stream::iter(chunks)))
        }
    }

    struct CountingActivationSpawner {
        reservations: Arc<AtomicUsize>,
        launches: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SessionActivationSpawner for CountingActivationSpawner {
        async fn reserve_activation(
            &self,
            target_session_id: &str,
            inbox_generation: u64,
        ) -> Result<SessionActivationReserveOutcome, bamboo_domain::SessionActivationError>
        {
            self.reservations.fetch_add(1, Ordering::SeqCst);
            let launches = self.launches.clone();
            Ok(SessionActivationReserveOutcome::Reserved(
                SessionActivationLaunch::new(
                    format!("{target_session_id}-{inbox_generation}"),
                    move || {
                        launches.fetch_add(1, Ordering::SeqCst);
                    },
                ),
            ))
        }
    }

    async fn completion_inbox_fixture() -> (
        tempfile::TempDir,
        Arc<bamboo_storage::SessionStoreV2>,
        Arc<dyn SessionInboxPort>,
        Arc<ChildCompletionCoordinator>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let locked = Arc::new(LockedSessionStore::new(storage.clone()));
        let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        let router = crate::SessionActivationRouter::new();
        let messenger = Arc::new(crate::SessionMessenger::new(
            storage.clone(),
            inbox.clone(),
            router.clone(),
        ));
        let reservations = Arc::new(AtomicUsize::new(0));
        let launches = Arc::new(AtomicUsize::new(0));
        router
            .set_spawner(Arc::new(CountingActivationSpawner {
                reservations: reservations.clone(),
                launches: launches.clone(),
            }))
            .await;
        let provider: Arc<dyn bamboo_llm::LLMProvider> = Arc::new(CompletedTestProvider);
        let config = Arc::new(RwLock::new(Config::default()));
        let metrics = bamboo_metrics::MetricsCollector::spawn(
            Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                temp.path().join("metrics.db"),
            )),
            7,
        );
        let tools: Arc<dyn ToolExecutor> = Arc::new(EmptyTools);
        let agent = Arc::new(
            Agent::builder()
                .storage(storage.clone())
                .persistence(locked.clone())
                .session_inbox(inbox.clone())
                .activation_router(router)
                .session_messenger(messenger)
                .attachment_reader(store.clone())
                .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
                .metrics_collector(metrics)
                .config(config.clone())
                .provider(provider.clone())
                .default_tools(tools)
                .build()
                .unwrap(),
        );
        let mut providers = HashMap::new();
        providers.insert("test".to_string(), provider);
        let registry = Arc::new(ProviderRegistry::new(providers, "test".to_string()));
        let provider_router = Arc::new(ProviderModelRouter::new(registry.clone()));
        let coordinator = Arc::new(ChildCompletionCoordinator::new(
            storage,
            locked,
            Arc::default(),
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(HashMap::new())),
            agent,
            config,
            registry,
            provider_router,
            temp.path().to_path_buf(),
            None,
        ));
        (temp, store, inbox, coordinator, reservations, launches)
    }

    #[tokio::test]
    async fn supervisor_resume_rejects_old_incarnation_without_replacing_cache() {
        let (_temp, store, _inbox, coordinator, _reservations, _launches) =
            completion_inbox_fixture().await;
        let original = store
            .get_or_create_default_supervisor("model")
            .await
            .unwrap();
        let mut stale = store
            .load_session(&original.session_id)
            .await
            .unwrap()
            .unwrap();
        store.delete_session(&original.session_id).await.unwrap();
        let recreated = store
            .get_or_create_default_supervisor("replacement")
            .await
            .unwrap();
        assert_ne!(original.incarnation_id, recreated.incarnation_id);
        let mut current = store
            .load_session(&recreated.session_id)
            .await
            .unwrap()
            .unwrap();
        coordinator.save_and_cache(&mut current).await;
        let cached = coordinator
            .sessions
            .get(&current.id)
            .unwrap()
            .value()
            .clone();
        coordinator.save_and_cache(&mut stale).await;
        assert!(Arc::ptr_eq(
            &cached,
            coordinator.sessions.get(&current.id).unwrap().value()
        ));
        assert_eq!(
            store
                .load_session(&current.id)
                .await
                .unwrap()
                .unwrap()
                .authority_identity,
            current.authority_identity
        );
    }

    // ── child-wait watchdog pure helpers (issue #546) ────────────────────

    #[test]
    fn dead_child_candidate_status_matrix() {
        // Never ran / lost before the running marker: dead candidate.
        assert!(is_dead_child_candidate_status(None));
        // Actively-reported non-terminal statuses: dead candidates when nothing
        // is driving them.
        assert!(is_dead_child_candidate_status(Some("running")));
        assert!(is_dead_child_candidate_status(Some("pending")));
        // Legitimately quiescent: waiting on a human / own children / bash.
        assert!(!is_dead_child_candidate_status(Some("suspended")));
        // Terminal statuses can never be "dead" — they are already done.
        for status in ["completed", "error", "timeout", "cancelled", "skipped"] {
            assert!(!is_dead_child_candidate_status(Some(status)), "{status}");
        }
    }

    #[test]
    fn completion_child_ownership_gates_content_fold() {
        // Owned: the child's own parent linkage matches the reporting parent.
        assert!(completion_child_is_owned("parent-1", Some("parent-1")));
        // Foreign: a real session that belongs to a DIFFERENT parent — its
        // content must never be folded into parent-1's transcript.
        assert!(!completion_child_is_owned("parent-1", Some("parent-2")));
        // Root/unparented session, or a nonexistent id (linkage None).
        assert!(!completion_child_is_owned("parent-1", None));
    }

    #[test]
    fn replay_child_prefers_error_like_for_first_error_policy() {
        let terminal = vec![
            ("c-ok".to_string(), "completed".to_string()),
            ("c-err".to_string(), "timeout".to_string()),
            ("c-late".to_string(), "completed".to_string()),
        ];
        let (id, status) = select_replay_child(&terminal).expect("non-empty");
        assert_eq!(id, "c-err");
        assert_eq!(status, "timeout");

        let all_ok = vec![
            ("c-1".to_string(), "completed".to_string()),
            ("c-2".to_string(), "completed".to_string()),
        ];
        let (id, _) = select_replay_child(&all_ok).expect("non-empty");
        assert_eq!(id, "c-2");

        assert!(select_replay_child(&[]).is_none());
    }

    #[test]
    fn watchdog_resume_messages_are_hidden_runtime_messages() {
        for message in [
            empty_child_wait_message(),
            child_wait_lease_expired_message(&["c-1".to_string(), "c-2".to_string()]),
        ] {
            assert!(matches!(message.role, Role::User));
            let meta = message.metadata.expect("hidden runtime metadata");
            assert_eq!(meta[RUNTIME_RESUME_MESSAGE_HIDDEN_KEY], true);
            assert_eq!(
                meta[RUNTIME_RESUME_MESSAGE_KIND_KEY],
                "child_wait_watchdog_resume"
            );
        }
        let lease = child_wait_lease_expired_message(&["c-1".to_string()]);
        // The lease message must never claim the children finished.
        assert!(lease.content.contains("NOT cancelled"));
        assert!(lease.content.contains("c-1"));
    }

    // ── on_child_completed terminality guard (issue #546) ────────────────

    #[test]
    fn non_terminal_statuses_never_satisfy_wait_policies() {
        // The guard keys on `is_terminal_child_status`; "suspended" (and any
        // unknown non-terminal string) must not count toward any policy.
        assert!(!is_terminal_child_status("suspended"));
        assert!(!is_terminal_child_status("running"));
        assert!(!is_terminal_child_status("pending"));
    }

    fn make_completion(status: &str) -> ChildCompletion {
        ChildCompletion {
            parent_session_id: "parent-1".to_string(),
            child_session_id: "child-1".to_string(),
            status: status.to_string(),
            error: None,
            completed_at: Utc::now(),
        }
    }

    #[test]
    fn oversized_child_outcome_is_bounded_and_keeps_full_content_identity() {
        let mut completion = make_completion("completed");
        let wait_registered_at = Utc::now();
        let huge = format!("prefix-A-{}", "x".repeat(300 * 1024));
        let presentation = runtime_resume_message(&completion, 0, Some(&huge));
        let first = child_completion_envelope(
            &completion,
            wait_registered_at,
            Some(huge.clone()),
            &presentation,
        );
        assert!(
            serde_json::to_vec(&first).unwrap().len()
                < bamboo_domain::SessionInboxLimits::default().max_payload_bytes
        );
        let SessionMessageBody::ChildOutcome(outcome) = &first.body else {
            panic!("typed child outcome");
        };
        let stored = outcome.result.as_deref().unwrap();
        assert!(stored.contains("sha256="));
        assert!(stored.contains("SubAgent.get"));
        assert!(stored.len() < CHILD_COMPLETION_INLINE_FIELD_BYTES);

        // Retry-only completion timestamps and provider presentation do not
        // change the logical id; full oversized content does.
        completion.completed_at += chrono::Duration::seconds(1);
        let exact_retry = child_completion_envelope(
            &completion,
            wait_registered_at,
            Some(huge.clone()),
            &runtime_resume_message(&completion, 9, Some(&huge)),
        );
        assert_eq!(exact_retry.id, first.id);
        let changed = format!("prefix-B-{}", "x".repeat(300 * 1024));
        let corrected = child_completion_envelope(
            &completion,
            wait_registered_at,
            Some(changed.clone()),
            &runtime_resume_message(&completion, 0, Some(&changed)),
        );
        assert_ne!(corrected.id, first.id);
    }

    #[tokio::test]
    async fn oversized_child_completion_clears_wait_and_activates_exactly_once() {
        let (_temp, store, inbox, coordinator, reservations, launches) =
            completion_inbox_fixture().await;
        let parent_id = "oversized-parent";
        let child_id = "oversized-child";
        let now = Utc::now();
        let mut parent = Session::new(parent_id, "model");
        let mut parent_runtime = AgentRuntimeState::new("waiting-run");
        parent_runtime.status = AgentStatusState::Suspended;
        parent_runtime.waiting_for_children = Some(WaitingForChildrenState::for_children(
            vec![child_id.to_string()],
            ChildWaitPolicy::All,
            now,
        ));
        parent_runtime.suspension = Some(SuspensionState {
            reason: "waiting_for_children".to_string(),
            suspended_at: now,
            resumable: true,
            hook_point: Some("ChildCompletion".to_string()),
        });
        write_runtime_state(&mut parent, &parent_runtime);
        parent.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "waiting_for_children".to_string(),
        );
        store.save_session(&parent).await.unwrap();

        let mut child = Session::new_child(child_id, parent_id, "model", "Child");
        child.add_message(Message::assistant("z".repeat(300 * 1024), None));
        child.set_last_run_status("completed");
        store.save_session(&child).await.unwrap();
        let completion = ChildCompletion {
            parent_session_id: parent_id.to_string(),
            child_session_id: child_id.to_string(),
            status: "completed".to_string(),
            error: None,
            completed_at: Utc::now(),
        };

        ChildCompletionHandler::on_child_completed(coordinator.as_ref(), completion.clone()).await;
        let durable_parent = store.load_session(parent_id).await.unwrap().unwrap();
        let durable_runtime = read_runtime_state(&durable_parent);
        assert!(durable_runtime.waiting_for_children.is_none());
        assert_eq!(durable_runtime.status, AgentStatusState::Idle);
        assert!(!durable_parent
            .metadata
            .contains_key("runtime.suspend_reason"));
        let backlog = inbox.inspect(parent_id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 1);
        assert!(backlog.activation_pending());
        assert_eq!(reservations.load(Ordering::SeqCst), 1);
        assert_eq!(launches.load(Ordering::SeqCst), 1);

        let claim = inbox.claim(parent_id, 1).await.unwrap().remove(0);
        assert!(
            serde_json::to_vec(&claim.envelope).unwrap().len()
                < bamboo_domain::SessionInboxLimits::default().max_payload_bytes
        );
        // Duplicate terminal notification after the wait was already cleared
        // cannot enqueue or activate a second outcome.
        ChildCompletionHandler::on_child_completed(coordinator.as_ref(), completion).await;
        assert_eq!(reservations.load(Ordering::SeqCst), 1);
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        assert_eq!(inbox.inspect(parent_id).await.unwrap().claimed, 1);
    }

    #[tokio::test]
    async fn latest_locked_activation_mutation_preserves_wait_armed_after_stale_load() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let locked = LockedSessionStore::new(storage);
        let mut session = Session::new("activation-stale-wait", "model");
        session.agent_runtime_state = Some(AgentRuntimeState::new("old-run"));
        store.save_session(&session).await.unwrap();

        // The activation path has already loaded this stale, wait-free snapshot.
        let stale = store.load_session(&session.id).await.unwrap().unwrap();
        assert!(read_runtime_state(&stale).waiting_for_children.is_none());

        locked
            .update_runtime_config(&session.id, |latest| {
                let mut state = read_runtime_state(latest);
                state.status = AgentStatusState::Suspended;
                state.waiting_for_children = Some(WaitingForChildrenState::for_children(
                    vec!["child-new".to_string()],
                    ChildWaitPolicy::All,
                    Utc::now(),
                ));
                state.suspension = Some(SuspensionState {
                    reason: "waiting_for_children".to_string(),
                    suspended_at: Utc::now(),
                    resumable: true,
                    hook_point: None,
                });
                write_runtime_state(latest, &state);
                latest.metadata.insert(
                    "runtime.suspend_reason".to_string(),
                    "waiting_for_children".to_string(),
                );
            })
            .await
            .unwrap();

        let (prepared, ready) = prepare_session_inbox_activation(&locked, &session.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(!ready, "latest durable specific wait must block activation");
        let state = read_runtime_state(&prepared);
        assert!(state.waiting_for_children.is_some());
        assert_eq!(state.status, AgentStatusState::Suspended);
        assert_eq!(
            prepared
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("waiting_for_children")
        );
    }

    #[tokio::test]
    async fn partial_child_and_bash_backlogs_remain_inert_after_inbox_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        store
            .save_session(&Session::new("restart-child-parent", "model"))
            .await
            .unwrap();
        store
            .save_session(&Session::new("restart-bash-parent", "model"))
            .await
            .unwrap();
        let inbox = bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        );

        let completion = ChildCompletion {
            parent_session_id: "restart-child-parent".to_string(),
            child_session_id: "child-a".to_string(),
            status: "completed".to_string(),
            error: None,
            completed_at: Utc::now(),
        };
        let child_resume = runtime_resume_message(&completion, 1, Some("first child"));
        inbox
            .deliver(&child_completion_envelope(
                &completion,
                Utc::now(),
                Some("first child".to_string()),
                &child_resume,
            ))
            .await
            .unwrap();
        let bash = BashCompletionInfo {
            session_id: "restart-bash-parent".to_string(),
            bash_id: "bash-a".to_string(),
            command: "true".to_string(),
            exit_code: Some(0),
            status: "completed".to_string(),
            output_tail: String::new(),
        };
        inbox
            .deliver(&bash_completion_envelope(&bash))
            .await
            .unwrap();

        let reopened = bamboo_storage::FileSessionInbox::new(
            store,
            bamboo_domain::SessionInboxLimits::default(),
        );
        for session_id in ["restart-child-parent", "restart-bash-parent"] {
            let backlog = reopened.inspect(session_id).await.unwrap();
            assert_eq!(backlog.pending, 1);
            assert_eq!(backlog.activation_generation, 0);
            assert!(
                !backlog.activation_pending(),
                "startup must not run a partial wait backlog for {session_id}"
            );
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
            "a",
            "completed"
        ));
        assert!(wait_policy_satisfied(
            ChildWaitPolicy::All,
            &waited,
            &["a".to_string(), "b".to_string()],
            "b",
            "completed"
        ));
    }

    #[test]
    fn wait_policy_first_error_requires_tracked_membership() {
        let waited = vec!["a".to_string(), "b".to_string()];
        // An error from a TRACKED child resumes immediately.
        assert!(wait_policy_satisfied(
            ChildWaitPolicy::FirstError,
            &waited,
            &["a".to_string()],
            "a",
            "error"
        ));
        // An error-like completion from an UNTRACKED child (e.g. a zombie
        // task from an earlier run waking up late) must not resume the wait.
        assert!(!wait_policy_satisfied(
            ChildWaitPolicy::FirstError,
            &waited,
            &["a".to_string()],
            "stray-child",
            "timeout"
        ));
        // The all-complete fallback still applies regardless of the reporter.
        assert!(wait_policy_satisfied(
            ChildWaitPolicy::FirstError,
            &waited,
            &["a".to_string(), "b".to_string()],
            "stray-child",
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
            let mut cached_snapshot = Config::default();
            cached_snapshot.provider = "cached-provider".to_string();

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

    #[test]
    fn background_completion_builds_post_tool_use_payload_and_feedback() {
        let mut info = BashCompletionInfo {
            session_id: "s".into(),
            bash_id: "bg-7".into(),
            command: "cargo test".into(),
            exit_code: Some(0),
            status: "completed".into(),
            output_tail: "test result: ok".into(),
        };

        let payload = background_bash_post_tool_payload(&info);
        match payload {
            HookPayload::ToolResult {
                tool_name,
                tool_call_id,
                outcome,
            } => {
                assert_eq!(tool_name, "Bash");
                assert_eq!(tool_call_id, "bg-7");
                assert!(outcome.success);
                let response: serde_json::Value =
                    serde_json::from_str(outcome.result.as_deref().unwrap()).unwrap();
                assert_eq!(response["command"], "cargo test");
                assert_eq!(response["exit_code"], 0);
                assert_eq!(response["status"], "completed");
                assert_eq!(response["output_tail"], "test result: ok");
            }
            other => panic!("expected PostToolUse payload, got {other:?}"),
        }

        append_background_bash_hook_feedback(
            &mut info,
            vec!["Run the formatter before continuing".to_string()],
        );
        assert!(info.output_tail.contains("<post_tool_use_feedback>"));
        assert!(info
            .output_tail
            .contains("Run the formatter before continuing"));
    }

    #[tokio::test]
    async fn bash_completion_payload_identity_matches_file_inbox_idempotency() {
        let baseline = BashCompletionInfo {
            session_id: "session".into(),
            bash_id: "bg-7".into(),
            command: "cargo test".into(),
            exit_code: Some(0),
            status: "completed".into(),
            output_tail: "first tail".into(),
        };
        let mut retried = baseline.clone();
        retried.output_tail = "first tail\nlater bytes\n<hook feedback>".into();
        retried.status = "completed-after-hook".into();
        let baseline_envelope = bash_completion_envelope(&baseline);
        let exact_retry = bash_completion_envelope(&baseline);
        let corrected_envelope = bash_completion_envelope(&retried);
        assert_eq!(baseline_envelope.id, exact_retry.id);
        assert_ne!(
            baseline_envelope.id, corrected_envelope.id,
            "changed payload semantics must receive a distinct id"
        );

        let mut other_shell = baseline.clone();
        other_shell.bash_id = "bg-8".into();
        assert_ne!(
            bash_completion_envelope(&baseline).id,
            bash_completion_envelope(&other_shell).id
        );

        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        store
            .save_session(&Session::new("session", "model"))
            .await
            .unwrap();
        let inbox = bamboo_storage::FileSessionInbox::new(
            store,
            bamboo_domain::SessionInboxLimits::default(),
        );
        let first = inbox.deliver(&baseline_envelope).await.unwrap();
        let duplicate = inbox.deliver(&exact_retry).await.unwrap();
        let corrected = inbox.deliver(&corrected_envelope).await.unwrap();
        assert_eq!(duplicate, first, "exact payload retry is idempotent");
        assert_ne!(corrected.id, first.id);
        assert_eq!(corrected.generation, first.generation + 1);
        assert_eq!(inbox.inspect("session").await.unwrap().pending, 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn background_completion_fires_configured_post_tool_use_command() {
        use bamboo_config::{
            LifecycleHookGroup, LifecycleHookHandler, LifecycleHooksConfig,
            DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
        };

        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("background-post-tool.json");
        let command = format!(
            "cat > '{}'; printf '%s' '{{\"additional_context\":\"inspect the completed build log\"}}'",
            output.display()
        );
        let config = LifecycleHooksConfig {
            enabled: true,
            post_tool_use: vec![LifecycleHookGroup {
                enabled: true,
                matcher: Some("^Bash$".to_string()),
                hooks: vec![LifecycleHookHandler::command(
                    command,
                    DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                )],
            }],
            ..Default::default()
        };
        let mut session = Session::new("session-bg-hook", "test-model");
        session.workspace = Some(dir.path().to_string_lossy().into_owned());
        let mut info = BashCompletionInfo {
            session_id: session.id.clone(),
            bash_id: "bg-9".into(),
            command: "cargo test".into(),
            exit_code: Some(0),
            status: "completed".into(),
            output_tail: "test result: ok".into(),
        };

        assert!(run_background_bash_post_tool_hooks(&config, None, &session, &mut info).await);
        let envelope: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(output).unwrap()).unwrap();
        assert_eq!(envelope["hook_event_name"], "PostToolUse");
        assert_eq!(envelope["tool_name"], "Bash");
        assert_eq!(envelope["payload"]["tool_call_id"], "bg-9");
        let response = envelope["tool_response"]["result"]
            .as_str()
            .map(serde_json::from_str::<serde_json::Value>)
            .transpose()
            .unwrap()
            .unwrap();
        assert_eq!(response["command"], "cargo test");
        assert_eq!(response["status"], "completed");
        assert!(info.output_tail.contains("inspect the completed build log"));
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

    #[test]
    fn two_shell_delivery_stages_backlog_before_one_final_activation() {
        let mut waiting = true;
        let mut durable_backlog = 0;
        let mut reservations = 0;

        // First shell: its completion is durable, but a sibling still runs.
        durable_backlog += 1;
        let first = bash_completion_delivery_plan(waiting, false);
        assert_eq!(first, BashCompletionDeliveryPlan::DurableOnly);
        assert!(waiting);
        assert_eq!(durable_backlog, 1);
        assert_eq!(reservations, 0);

        // Last shell: its own completion joins the same ordered backlog, then
        // the durable wait is cleared and exactly one activation is requested.
        durable_backlog += 1;
        let last = bash_completion_delivery_plan(waiting, true);
        assert_eq!(last, BashCompletionDeliveryPlan::ClearWaitThenActivate);
        waiting = false;
        reservations += 1;
        assert!(!waiting);
        assert_eq!(durable_backlog, 2);
        assert_eq!(reservations, 1);
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
