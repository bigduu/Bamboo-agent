//! Chat ⇄ bamboo-session routing, busy lock + FIFO queue, and execution
//! through the single canonical [`spawn_session_execution`] path.
//!
//! Reference: `schedule_app::manager::run_schedule_job` — the SAME
//! `SessionExecutionArgs` + event-forwarder + reservation pattern is reused
//! here (see `run_prompt`), rather than inventing a parallel loop.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

use chrono::{DateTime, Utc};
use tokio::sync::{broadcast, Mutex as AsyncMutex, RwLock as TokioRwLock};
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Message, Session};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_engine::config::GoldConfig;
use bamboo_engine::execution::runner_state::AgentRunner;
use bamboo_engine::execution::{
    create_event_forwarder, get_or_create_event_sender, spawn_session_execution,
    try_reserve_runner, SessionExecutionArgs,
};
use bamboo_engine::{AuxiliaryModelConfig, ModelRoster, SessionRepository};
use bamboo_llm::{Config, ProviderRegistry};

use super::platform::{InboundMessage, OutboundMessage, Platform, ReplyCtx};
use super::render;

/// `platform:chat_id:user_id` — the chat-scoped routing key mapping to a
/// bamboo session id (see epic #447's "Bridge" design).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionKey {
    pub platform: String,
    pub chat_id: String,
    pub user_id: String,
}

impl SessionKey {
    pub fn as_string(&self) -> String {
        format!("{}:{}:{}", self.platform, self.chat_id, self.user_id)
    }
}

/// Shared dependencies the bridge needs to run sessions through the
/// canonical execution path. Cheap to clone (every field is an `Arc`, or
/// small/`Clone`).
#[derive(Clone)]
pub struct ConnectContext {
    pub agent: Arc<bamboo_engine::Agent>,
    pub tools: Arc<dyn ToolExecutor>,
    pub session_repo: SessionRepository,
    pub agent_runners: Arc<TokioRwLock<HashMap<String, AgentRunner>>>,
    pub session_event_senders: Arc<TokioRwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    pub account_feed_inbox: Option<bamboo_engine::execution::AccountFeedInbox>,
    pub app_data_dir: Option<PathBuf>,
    pub config: Arc<tokio::sync::RwLock<Config>>,
    pub provider_registry: Arc<ProviderRegistry>,
}

/// Per-chat runtime state: whether a run is currently executing, the FIFO
/// queue of messages that arrived while busy (drained at run end — mirrors
/// cc-connect engine.go's `queueMessageForBusySession`), and the cancel token
/// of the in-flight run (if any) so `/stop` can reach it without waiting in
/// the queue.
#[derive(Default)]
struct ChatState {
    busy: bool,
    queue: VecDeque<(Arc<dyn Platform>, InboundMessage)>,
    cancel_token: Option<CancellationToken>,
}

/// Resolved model/prompt/workspace configuration for a connect-driven run,
/// derived from the live global config. Mirrors
/// `schedule_app::manager::ResolvedRunConfig`, minus the per-job overrides a
/// scheduled run supports (a chat message has none).
struct ResolvedConnectRunConfig {
    model_roster: ModelRoster,
    reasoning_effort: Option<ReasoningEffort>,
    gold_config: Option<GoldConfig>,
    system_prompt: String,
    base_system_prompt: String,
    workspace_path: Option<String>,
}

fn resolve_connect_run_config(
    config_snapshot: &Config,
    provider_registry: &Arc<ProviderRegistry>,
) -> ResolvedConnectRunConfig {
    let model = config_snapshot.get_model().unwrap_or_default();
    let provider_name = Some(config_snapshot.effective_default_provider().to_string());
    let provider_type = provider_name.as_deref().and_then(|name| {
        bamboo_engine::model_config_helper::resolve_provider_type(
            config_snapshot,
            name,
            provider_registry,
        )
    });
    let capability_provider_name = provider_name
        .as_deref()
        .unwrap_or(config_snapshot.effective_default_provider());
    // Auxiliary models are global (config-derived), never session-bound —
    // same rationale as `schedule_app::manager::resolve_run_config_from_config`.
    let areas = bamboo_engine::model_areas::resolve_global_area_models(
        config_snapshot,
        capability_provider_name,
        provider_registry,
    );
    let reasoning_effort = config_snapshot.get_reasoning_effort();
    let base_system_prompt =
        bamboo_engine::prompt_defaults::read_global_default_system_prompt_template();
    let workspace_path = config_snapshot
        .get_default_work_area_path()
        .map(|path| bamboo_config::paths::path_to_display_string(&path));
    let system_prompt = bamboo_engine::context::assemble_system_prompt(
        &base_system_prompt,
        None,
        workspace_path.as_deref(),
    );
    let model_roster = ModelRoster::from_areas(Some(model), provider_name, provider_type, areas);

    ResolvedConnectRunConfig {
        model_roster,
        reasoning_effort,
        gold_config: bamboo_engine::model_config_helper::resolve_gold_config(config_snapshot, None),
        system_prompt,
        base_system_prompt,
        workspace_path,
    }
}

/// Builds a fresh session for a connect chat key. Mirrors
/// `schedule_app::session_factory::create_schedule_session`.
///
/// Sets `no_human_approver` (like a scheduled run): approvals/buttons are a
/// later phase of epic #447 (#452 is text-only), so there is no channel to
/// answer a gated-tool prompt yet — without this flag a gated action would
/// hang waiting on an approver that can never respond.
fn create_connect_session(
    key: &str,
    model: &str,
    system_prompt: &str,
    base_system_prompt: &str,
    workspace_path: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Session {
    let session_id = uuid::Uuid::new_v4().to_string();
    let mut session = Session::new(session_id.clone(), model.to_string());
    session.title = format!("Connect: {key}");
    session
        .metadata
        .insert("created_by_connect_key".to_string(), key.to_string());
    session.metadata.insert(
        "base_system_prompt".to_string(),
        base_system_prompt.to_string(),
    );
    if let Some(path) = workspace_path {
        session.set_workspace_path_meta(path);
        bamboo_tools::tools::workspace_state::ensure_session_workspace(
            &session_id,
            Some(PathBuf::from(path)),
        );
    }
    if let Some(effort) = reasoning_effort {
        session.set_reasoning_effort_meta(effort.as_str());
    }
    session.add_message(Message::system(system_prompt.to_string()));
    bamboo_engine::runner::refresh_prompt_snapshot(&mut session);
    session
        .agent_runtime_state
        .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
        .no_human_approver = true;
    session
}

/// Strips a Telegram-style `@BotName` command suffix (`/stop@MyBot` ->
/// `/stop`) so mention-qualified commands still match in group chats.
fn strip_command_suffix(text: &str) -> &str {
    text.split('@').next().unwrap_or(text)
}

async fn reply_text(platform: &Arc<dyn Platform>, ctx: &ReplyCtx, text: impl Into<String>) {
    if let Err(error) = platform.reply(ctx, OutboundMessage::text(text)).await {
        tracing::warn!("connect: failed to send reply: {error}");
    }
}

/// Routes inbound platform messages to bamboo sessions, enforces the
/// per-platform allow-list + dedup, and serializes execution per chat behind
/// a busy lock + FIFO queue.
pub struct ConnectBridge {
    ctx: ConnectContext,
    /// `SessionKey::as_string()` -> bamboo session id. Persisted as JSON
    /// (atomic write) so a chat's session survives a server restart.
    session_map: TokioRwLock<HashMap<String, String>>,
    map_path: Option<PathBuf>,
    chat_state: AsyncMutex<HashMap<String, ChatState>>,
    /// `platform:message_id` seen so far — dedup defense-in-depth alongside
    /// each adapter's own transport-level dedup (e.g. Telegram's offset
    /// advance). A `std::sync::Mutex` is fine here: only ever locked for a
    /// single `HashSet::insert`, never held across an `.await`.
    seen_message_ids: StdMutex<HashSet<String>>,
    process_start: DateTime<Utc>,
}

impl ConnectBridge {
    pub fn new(ctx: ConnectContext, map_path: Option<PathBuf>) -> Self {
        Self {
            ctx,
            session_map: TokioRwLock::new(HashMap::new()),
            map_path,
            chat_state: AsyncMutex::new(HashMap::new()),
            seen_message_ids: StdMutex::new(HashSet::new()),
            process_start: Utc::now(),
        }
    }

    /// Loads the persisted chat -> session map from disk, if a `map_path`
    /// was configured. Tolerates a missing or corrupt file (starts empty,
    /// logging a warning on corruption) — a fresh/lost map degrades to
    /// "every chat starts a new session," never a hard failure.
    pub async fn load_session_map(&self) {
        let Some(path) = &self.map_path else {
            return;
        };
        match tokio::fs::read(path).await {
            Ok(bytes) => match serde_json::from_slice::<HashMap<String, String>>(&bytes) {
                Ok(map) => *self.session_map.write().await = map,
                Err(error) => {
                    tracing::warn!(
                        "connect: session map at {path:?} is corrupt, starting empty: {error}"
                    );
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!("connect: failed to read session map at {path:?}: {error}");
            }
        }
    }

    pub async fn session_id_for_key(&self, key: &str) -> Option<String> {
        self.session_map.read().await.get(key).cloned()
    }

    async fn set_session_id_for_key(&self, key: &str, session_id: &str) {
        {
            let mut map = self.session_map.write().await;
            map.insert(key.to_string(), session_id.to_string());
        }
        self.persist_session_map().await;
    }

    async fn rotate_session(&self, key: &str) {
        {
            let mut map = self.session_map.write().await;
            map.remove(key);
        }
        self.persist_session_map().await;
    }

    async fn persist_session_map(&self) {
        let Some(path) = &self.map_path else {
            return;
        };
        let snapshot = self.session_map.read().await.clone();
        let json = match serde_json::to_vec_pretty(&snapshot) {
            Ok(json) => json,
            Err(error) => {
                tracing::warn!("connect: failed to serialize session map: {error}");
                return;
            }
        };
        if let Err(error) = atomic_write(path, &json).await {
            tracing::warn!("connect: failed to persist session map at {path:?}: {error}");
        }
    }

    async fn set_cancel_token(&self, key: &str, token: CancellationToken) {
        let mut guard = self.chat_state.lock().await;
        guard.entry(key.to_string()).or_default().cancel_token = Some(token);
    }

    async fn clear_cancel_token(&self, key: &str) {
        let mut guard = self.chat_state.lock().await;
        if let Some(state) = guard.get_mut(key) {
            state.cancel_token = None;
        }
    }

    /// Entry point for every inbound platform message. Enforces allow-list +
    /// dedup, answers `/stop` and `/status` immediately (bypassing the busy
    /// queue — a queued `/stop` could never reach a busy chat), and otherwise
    /// either runs the message right away or queues it behind the chat's
    /// current run.
    ///
    /// Takes `self: Arc<Self>` (not `&self`) so it can hand the bridge off to
    /// a detached `tokio::spawn` for the actual (potentially long-running)
    /// execution — this method itself returns as soon as the message is
    /// either answered inline or queued, so one slow chat can never block
    /// another chat's inbound dispatch loop.
    pub async fn handle_inbound(
        self: Arc<Self>,
        platform: Arc<dyn Platform>,
        allow_from: Vec<String>,
        msg: InboundMessage,
    ) {
        if !allow_from.iter().any(|allowed| allowed == &msg.user_id) {
            tracing::warn!(
                platform = %msg.platform,
                chat_id = %msg.chat_id,
                user_id = %msg.user_id,
                "connect: rejected inbound message — user not in allow_from"
            );
            return;
        }

        if msg.sent_at < self.process_start {
            tracing::debug!(
                platform = %msg.platform,
                message_id = %msg.message_id,
                "connect: dropping message older than process start"
            );
            return;
        }

        let dedup_key = format!("{}:{}", msg.platform, msg.message_id);
        {
            let mut seen = self.seen_message_ids.lock().unwrap();
            if !seen.insert(dedup_key) {
                tracing::debug!(
                    platform = %msg.platform,
                    message_id = %msg.message_id,
                    "connect: dropping duplicate message_id"
                );
                return;
            }
        }

        let key = SessionKey {
            platform: msg.platform.clone(),
            chat_id: msg.chat_id.clone(),
            user_id: msg.user_id.clone(),
        }
        .as_string();

        let command = strip_command_suffix(msg.text.trim());
        if command.eq_ignore_ascii_case("/stop") {
            self.handle_stop(&key, &platform, &msg.reply_ctx).await;
            return;
        }
        if command.eq_ignore_ascii_case("/status") {
            self.handle_status(&key, &platform, &msg.reply_ctx).await;
            return;
        }

        let mut guard = self.chat_state.lock().await;
        let state = guard.entry(key.clone()).or_default();
        if state.busy {
            state.queue.push_back((platform, msg));
            return;
        }
        state.busy = true;
        drop(guard);

        let bridge = self.clone();
        tokio::spawn(async move {
            bridge.drain_chat(key, platform, msg).await;
        });
    }

    /// Processes `msg`, then keeps draining `chat_state`'s queue for `key`
    /// (FIFO) until it is empty, at which point the chat is marked idle
    /// again. Runs in its own spawned task (see [`Self::handle_inbound`]).
    async fn drain_chat(
        self: Arc<Self>,
        key: String,
        mut platform: Arc<dyn Platform>,
        mut msg: InboundMessage,
    ) {
        loop {
            self.process_one(&key, platform.clone(), msg).await;

            let next = {
                let mut guard = self.chat_state.lock().await;
                match guard.get_mut(&key) {
                    Some(state) => match state.queue.pop_front() {
                        Some(item) => Some(item),
                        None => {
                            state.busy = false;
                            None
                        }
                    },
                    None => None,
                }
            };

            match next {
                Some((p, m)) => {
                    platform = p;
                    msg = m;
                }
                None => break,
            }
        }
    }

    async fn process_one(&self, key: &str, platform: Arc<dyn Platform>, msg: InboundMessage) {
        let command = strip_command_suffix(msg.text.trim());
        if command.eq_ignore_ascii_case("/new") {
            self.rotate_session(key).await;
            reply_text(&platform, &msg.reply_ctx, "Started a new session.").await;
            return;
        }

        let text = msg.text.trim();
        if text.is_empty() {
            return;
        }

        self.run_prompt(key, platform, &msg.reply_ctx, text).await;
    }

    async fn handle_stop(&self, key: &str, platform: &Arc<dyn Platform>, reply_ctx: &ReplyCtx) {
        let token = {
            self.chat_state
                .lock()
                .await
                .get(key)
                .and_then(|state| state.cancel_token.clone())
        };
        match token {
            Some(token) => {
                token.cancel();
                reply_text(platform, reply_ctx, "Stopping the current run…").await;
            }
            None => {
                reply_text(platform, reply_ctx, "Nothing is running.").await;
            }
        }
    }

    async fn handle_status(&self, key: &str, platform: &Arc<dyn Platform>, reply_ctx: &ReplyCtx) {
        let session_id = self.session_id_for_key(key).await;
        let busy = {
            self.chat_state
                .lock()
                .await
                .get(key)
                .map(|state| state.busy)
                .unwrap_or(false)
        };
        let text = match session_id {
            Some(id) => format!(
                "Session: {id}\nStatus: {}",
                if busy { "busy" } else { "idle" }
            ),
            None => "No session yet. Send a message to start one.".to_string(),
        };
        reply_text(platform, reply_ctx, text).await;
    }

    async fn create_and_register_session(
        &self,
        key: &str,
        resolved: &ResolvedConnectRunConfig,
    ) -> Session {
        let model = resolved.model_roster.model.clone().unwrap_or_default();
        let session = create_connect_session(
            key,
            &model,
            &resolved.system_prompt,
            &resolved.base_system_prompt,
            resolved.workspace_path.as_deref(),
            resolved.reasoning_effort,
        );
        self.set_session_id_for_key(key, &session.id).await;
        session
    }

    /// Runs `text` as a prompt for `key`'s session, through the canonical
    /// `spawn_session_execution` path (reference:
    /// `schedule_app::manager::run_schedule_job`), then renders the run's
    /// live event stream back to the platform (`render::stream_execution`)
    /// until it reaches a terminal state. Awaited inline by the caller (not
    /// detached) so the run's completion IS this call's completion — that is
    /// what lets [`Self::drain_chat`] serialize one run at a time per chat.
    async fn run_prompt(
        &self,
        key: &str,
        platform: Arc<dyn Platform>,
        reply_ctx: &ReplyCtx,
        text: &str,
    ) {
        let config_snapshot = self.ctx.config.read().await.clone();
        let resolved = resolve_connect_run_config(&config_snapshot, &self.ctx.provider_registry);

        if resolved
            .model_roster
            .model
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
        {
            reply_text(
                &platform,
                reply_ctx,
                "No model is configured for this bamboo instance; cannot run your request.",
            )
            .await;
            return;
        }

        let existing_id = self.session_id_for_key(key).await;
        let mut session = match existing_id {
            Some(id) => match self.ctx.session_repo.load_merged(&id).await {
                Some(session) => session,
                None => self.create_and_register_session(key, &resolved).await,
            },
            None => self.create_and_register_session(key, &resolved).await,
        };

        session.add_message(Message::user(text.to_string()));
        let session_id = session.id.clone();
        self.ctx.session_repo.save_and_cache(&mut session).await;

        let session_tx =
            get_or_create_event_sender(&self.ctx.session_event_senders, &session_id).await;
        let rx = session_tx.subscribe();

        let Some(reservation) = try_reserve_runner(
            &self.ctx.agent_runners,
            &self.ctx.session_event_senders,
            &session_id,
            &session_tx,
        )
        .await
        else {
            reply_text(
                &platform,
                reply_ctx,
                "This session is already running elsewhere; please wait for it to finish.",
            )
            .await;
            return;
        };

        self.set_cancel_token(key, reservation.cancel_token.clone())
            .await;

        let (mpsc_tx, _forwarder_handle) = create_event_forwarder(
            session_id.clone(),
            session_tx.clone(),
            self.ctx.agent_runners.clone(),
            self.ctx.account_feed_inbox.clone(),
        );

        // Auxiliary (fast/background/summarization) model resolver — mirrors
        // `schedule_app::manager::run_schedule_job` exactly.
        let aux_fast_model = resolved.model_roster.fast_model();
        let aux_fast_provider = resolved.model_roster.fast_model_provider();
        let aux_background_model = resolved.model_roster.background_model();
        let aux_background_provider = resolved.model_roster.background_model_provider();
        let aux_summarization_model = resolved.model_roster.summarization_model();
        let aux_summarization_provider = resolved.model_roster.summarization_model_provider();
        let auxiliary_model_resolver = Arc::new(move || AuxiliaryModelConfig {
            fast_model_name: aux_fast_model.clone(),
            fast_model_provider: aux_fast_provider.clone(),
            background_model_name: aux_background_model.clone(),
            planning_model_name: None,
            search_model_name: None,
            summarization_model_name: aux_summarization_model.clone(),
            background_model_provider: aux_background_provider.clone(),
            summarization_model_provider: aux_summarization_provider.clone(),
        });

        spawn_session_execution(SessionExecutionArgs {
            agent: self.ctx.agent.clone(),
            session_id: session_id.clone(),
            session,
            tools_override: Some(self.ctx.tools.clone()),
            provider_override: None,
            model_roster: resolved.model_roster.clone(),
            reasoning_effort: resolved.reasoning_effort,
            reasoning_effort_source: "connect".to_string(),
            auxiliary_model_resolver: Some(auxiliary_model_resolver),
            disabled_filter_resolver: None,
            disabled_tools: None,
            disabled_skill_ids: None,
            selected_skill_ids: None,
            selected_skill_mode: None,
            cancel_token: reservation.cancel_token,
            mpsc_tx,
            image_fallback: None,
            gold_config: resolved.gold_config.clone(),
            // Approvals (guardian, bash resume) are a later phase of epic
            // #447 — MVP has no channel to answer them (see
            // `create_connect_session`'s `no_human_approver` doc comment).
            guardian_config: None,
            guardian_spawner: None,
            bash_resume_hook: None,
            bash_completion_sink: None,
            app_data_dir: self.ctx.app_data_dir.clone(),
            runners: self.ctx.agent_runners.clone(),
            sessions_cache: self.ctx.session_repo.cache().clone(),
            on_complete: None,
        });

        render::stream_execution(platform, reply_ctx.clone(), rx).await;

        self.clear_cancel_token(key).await;
    }
}

/// Writes `bytes` to `path` atomically: temp file in the same directory,
/// fsync, rename over the target. Mirrors
/// `handlers::settings::bamboo_config::config_endpoints::common::atomic_write`
/// (private there) so a crash mid-write leaves the old session map intact.
async fn atomic_write(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));
    {
        let mut file = tokio::fs::File::create(&tmp).await?;
        tokio::io::AsyncWriteExt::write_all(&mut file, bytes).await?;
        file.sync_all().await?;
    }
    tokio::fs::rename(&tmp, path).await?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = tokio::fs::File::open(parent).await {
            let _ = dir.sync_all().await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::tools::ToolSurface;
    use std::time::Duration;
    use tokio::sync::Mutex as TokioMutex;

    /// mpsc-backed fake `Platform` (per issue #452's test spec): records every
    /// `reply()` call instead of talking to a real IM API.
    struct FakePlatform {
        label: String,
        sent: TokioMutex<Vec<String>>,
    }

    impl FakePlatform {
        fn new(label: &str) -> Arc<Self> {
            Arc::new(Self {
                label: label.to_string(),
                sent: TokioMutex::new(Vec::new()),
            })
        }

        async fn sent_texts(&self) -> Vec<String> {
            self.sent.lock().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl Platform for FakePlatform {
        fn name(&self) -> &str {
            &self.label
        }
        fn capabilities(&self) -> super::super::platform::Capabilities {
            Default::default()
        }
        async fn start(
            &self,
            _inbound: tokio::sync::mpsc::Sender<InboundMessage>,
        ) -> super::super::platform::PlatformResult<()> {
            Ok(())
        }
        async fn reply(
            &self,
            _ctx: &ReplyCtx,
            msg: OutboundMessage,
        ) -> super::super::platform::PlatformResult<super::super::platform::MessageRef> {
            self.sent.lock().await.push(msg.text);
            Ok(super::super::platform::MessageRef(serde_json::Value::Null))
        }
        async fn edit(
            &self,
            _msg_ref: &super::super::platform::MessageRef,
            _new: OutboundMessage,
        ) -> super::super::platform::PlatformResult<()> {
            Ok(())
        }
        async fn stop(&self) -> super::super::platform::PlatformResult<()> {
            Ok(())
        }
    }

    async fn test_context() -> (ConnectContext, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        // A full `AppState` gives the bridge a real `Agent`/tools/session-repo
        // without a network call. A model IS configured (so `run_prompt`'s
        // "no model configured" guard doesn't short-circuit before a session
        // is even created) but with no api_key, so the provider falls back to
        // `UnconfiguredProvider`: execution fails fast with an
        // `AgentEvent::Error` (non-retryable auth error), which is exactly
        // what these tests need to observe a run reach a terminal state
        // quickly without any real network access.
        let state = AppState::new(dir.path().to_path_buf())
            .await
            .expect("app state");
        {
            let mut cfg = state.config.write().await;
            cfg.provider = "openai".to_string();
            cfg.providers.openai = Some(bamboo_config::OpenAIConfig {
                api_key: String::new(),
                api_key_from_env: false,
                api_key_encrypted: None,
                base_url: None,
                model: Some("gpt-4o-mini".to_string()),
                fast_model: None,
                vision_model: None,
                reasoning_effort: None,
                responses_only_models: Vec::new(),
                request_overrides: None,
                extra: Default::default(),
            });
        }
        let ctx = ConnectContext {
            agent: state.agent.clone(),
            tools: state.tools_for(ToolSurface::Root),
            session_repo: state.session_repo.clone(),
            agent_runners: state.agent_runners.clone(),
            session_event_senders: state.session_event_senders.clone(),
            account_feed_inbox: None,
            app_data_dir: Some(state.app_data_dir.clone()),
            config: state.config.clone(),
            provider_registry: state.provider_registry.clone(),
        };
        (ctx, dir)
    }

    fn inbound(chat_id: &str, user_id: &str, message_id: &str, text: &str) -> InboundMessage {
        InboundMessage {
            platform: "fake".to_string(),
            chat_id: chat_id.to_string(),
            user_id: user_id.to_string(),
            message_id: message_id.to_string(),
            sent_at: Utc::now(),
            text: text.to_string(),
            reply_ctx: ReplyCtx(serde_json::json!({ "chat_id": chat_id })),
        }
    }

    fn key_for(chat_id: &str, user_id: &str) -> String {
        SessionKey {
            platform: "fake".to_string(),
            chat_id: chat_id.to_string(),
            user_id: user_id.to_string(),
        }
        .as_string()
    }

    #[test]
    fn session_key_formats_as_platform_chat_user() {
        let key = SessionKey {
            platform: "telegram".to_string(),
            chat_id: "42".to_string(),
            user_id: "7".to_string(),
        };
        assert_eq!(key.as_string(), "telegram:42:7");
    }

    #[tokio::test]
    async fn allow_from_denies_users_not_in_the_list() {
        let (ctx, _dir) = test_context().await;
        let bridge = Arc::new(ConnectBridge::new(ctx, None));
        let platform = FakePlatform::new("fake");

        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["allowed-user".to_string()],
            inbound("chat1", "someone-else", "1", "hello"),
        )
        .await;

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(platform.sent_texts().await.is_empty());
        assert!(bridge
            .session_id_for_key(&key_for("chat1", "someone-else"))
            .await
            .is_none());
    }

    #[tokio::test]
    async fn dedup_drops_repeated_message_ids() {
        let (ctx, _dir) = test_context().await;
        let bridge = Arc::new(ConnectBridge::new(ctx, None));
        let platform = FakePlatform::new("fake");
        let allow = vec!["u1".to_string()];

        // `/status` replies synchronously with no engine/queue involved, so a
        // duplicate delivery of the SAME message_id must yield exactly one
        // reply, not two.
        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            allow.clone(),
            inbound("chat1", "u1", "dup-1", "/status"),
        )
        .await;
        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            allow,
            inbound("chat1", "u1", "dup-1", "/status"),
        )
        .await;

        let sent = platform.sent_texts().await;
        assert_eq!(
            sent.len(),
            1,
            "duplicate message_id must be dropped: {sent:?}"
        );
    }

    #[tokio::test]
    async fn older_than_process_start_messages_are_dropped() {
        let (ctx, _dir) = test_context().await;
        let bridge = Arc::new(ConnectBridge::new(ctx, None));
        let platform = FakePlatform::new("fake");
        let mut msg = inbound("chat1", "u1", "1", "/status");
        msg.sent_at = bridge.process_start - chrono::Duration::seconds(5);

        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            msg,
        )
        .await;

        assert!(platform.sent_texts().await.is_empty());
    }

    #[tokio::test]
    async fn status_command_reports_idle_with_no_session_yet() {
        let (ctx, _dir) = test_context().await;
        let bridge = Arc::new(ConnectBridge::new(ctx, None));
        let platform = FakePlatform::new("fake");

        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            inbound("chat1", "u1", "1", "/status"),
        )
        .await;

        let sent = platform.sent_texts().await;
        assert_eq!(sent.len(), 1);
        assert!(sent[0].contains("No session yet"), "got: {:?}", sent[0]);
    }

    #[tokio::test]
    async fn stop_with_nothing_running_replies_nothing_running() {
        let (ctx, _dir) = test_context().await;
        let bridge = Arc::new(ConnectBridge::new(ctx, None));
        let platform = FakePlatform::new("fake");

        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            inbound("chat1", "u1", "1", "/stop"),
        )
        .await;

        assert_eq!(
            platform.sent_texts().await,
            vec!["Nothing is running.".to_string()]
        );
    }

    #[tokio::test]
    async fn prompt_creates_a_session_and_maps_it_to_the_chat_key() {
        let (ctx, _dir) = test_context().await;
        let bridge = Arc::new(ConnectBridge::new(ctx, None));
        let platform = FakePlatform::new("fake");
        let key = key_for("chat1", "u1");

        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            inbound("chat1", "u1", "1", "hello there"),
        )
        .await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if bridge.session_id_for_key(&key).await.is_some() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "session was never created for the chat key"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn new_command_rotates_the_session_mapping() {
        let (ctx, _dir) = test_context().await;
        let bridge = Arc::new(ConnectBridge::new(ctx, None));
        let platform = FakePlatform::new("fake");
        let key = key_for("chat1", "u1");

        bridge
            .set_session_id_for_key(&key, "pre-existing-session")
            .await;
        assert_eq!(
            bridge.session_id_for_key(&key).await.as_deref(),
            Some("pre-existing-session")
        );

        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            inbound("chat1", "u1", "1", "/new"),
        )
        .await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if bridge.session_id_for_key(&key).await.is_none() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "session mapping was never rotated away"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let sent = platform.sent_texts().await;
        assert!(sent.iter().any(|t| t == "Started a new session."));
    }

    #[tokio::test]
    async fn busy_queue_drains_a_second_message_after_the_first_finishes() {
        let (ctx, _dir) = test_context().await;
        let bridge = Arc::new(ConnectBridge::new(ctx, None));
        let platform = FakePlatform::new("fake");
        let allow = vec!["u1".to_string()];
        let key = key_for("chat1", "u1");

        // Two prompts arriving back-to-back for the SAME chat: the second
        // must queue behind the first (busy lock) rather than racing it, and
        // the chat must return to idle once both have run.
        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            allow.clone(),
            inbound("chat1", "u1", "1", "first"),
        )
        .await;
        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            allow,
            inbound("chat1", "u1", "2", "second"),
        )
        .await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let idle = {
                !bridge
                    .chat_state
                    .lock()
                    .await
                    .get(&key)
                    .map(|state| state.busy)
                    .unwrap_or(true)
            };
            if idle {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "chat never drained back to idle"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(bridge.session_id_for_key(&key).await.is_some());
    }

    #[tokio::test]
    async fn session_map_persists_and_reloads_across_bridge_instances() {
        let (ctx, dir) = test_context().await;
        let map_path = dir.path().join("connect_sessions.json");
        let bridge = ConnectBridge::new(ctx.clone(), Some(map_path.clone()));
        bridge.set_session_id_for_key("k1", "sess-1").await;

        let bridge2 = ConnectBridge::new(ctx, Some(map_path));
        bridge2.load_session_map().await;

        assert_eq!(
            bridge2.session_id_for_key("k1").await.as_deref(),
            Some("sess-1")
        );
    }
}
