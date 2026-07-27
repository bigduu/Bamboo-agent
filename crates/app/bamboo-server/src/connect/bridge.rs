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
use tokio::sync::{broadcast, mpsc, Mutex as AsyncMutex, RwLock as TokioRwLock};
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Message, Session};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_engine::execution::runner_state::AgentRunner;
use bamboo_engine::execution::{
    create_event_forwarder, get_or_create_event_sender, reserve_session_execution,
    spawn_session_execution, SessionExecutionArgs, SessionExecutionReserveOutcome,
};
use bamboo_engine::{AuxiliaryModelConfig, SessionRepository};
use bamboo_llm::{Config, ProviderRegistry};

use super::approvals::{self, ParkedAsk, RespondAndResumeOutcome, Responder};
use super::platform::{CallbackQuery, InboundMessage, OutboundMessage, Platform, ReplyCtx};
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

/// Max entries [`BoundedSeenSet`] retains before evicting the oldest
/// (issue #454 follow-up). This is defense-in-depth dedup, layered on top
/// of each adapter's own transport-level dedup (e.g. Telegram's offset
/// advance) — it only needs to cover the realistic in-flight
/// redelivery/retry window, not serve as a permanent audit log. A few
/// thousand entries comfortably covers any plausible burst across every
/// configured chat while keeping memory bounded for the life of the
/// process.
const DEDUP_CAPACITY: usize = 10_000;

/// A `HashSet` bounded to at most `capacity` entries via FIFO eviction:
/// once full, inserting a new key evicts the oldest still-tracked key.
/// Used for [`ConnectBridge::seen_message_ids`] — a plain `HashSet` there
/// would gain one entry per distinct `platform:message_id` for the life of
/// the process (issue #454 follow-up).
struct BoundedSeenSet {
    set: HashSet<String>,
    order: VecDeque<String>,
    capacity: usize,
}

impl BoundedSeenSet {
    fn new(capacity: usize) -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// Inserts `key`, evicting the oldest tracked key(s) if this pushes the
    /// set past capacity. Returns `true` if `key` was newly inserted (i.e.
    /// not a duplicate) — same contract as `HashSet::insert`.
    fn insert(&mut self, key: String) -> bool {
        if !self.set.insert(key.clone()) {
            return false;
        }
        self.order.push_back(key);
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        true
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.set.len()
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
    pub project_store: Arc<bamboo_projects::ProjectStore>,
    pub workspace_resolver: bamboo_agent_core::workspace_state::WorkspaceResolver,
    /// Connector type -> Project membership for newly-created sessions.
    /// Multi-instance connectors of the same type are currently rejected, so
    /// the platform type is an unambiguous key.
    pub project_ids_by_platform: Arc<HashMap<String, bamboo_domain::ProjectId>>,
    /// Shared with `AppState::permission_checker` — needed so an approved
    /// permission prompt (a gated tool asked to run, answered through
    /// connect) actually grants the session permission the re-executed tool
    /// call is checked against on resume (issue #458; see
    /// `approvals::EngineResponder`).
    pub permission_checker: Arc<dyn bamboo_tools::permission::PermissionChecker>,
}

/// Per-chat runtime state: whether a run is currently executing, the FIFO
/// queue of messages that arrived while busy (drained at run end — mirrors
/// cc-connect engine.go's `queueMessageForBusySession`), the cancel token of
/// the in-flight run (if any) so `/stop` can reach it without waiting in the
/// queue, and the chat's one parked ask (if any) — issue #458's
/// approval/question relay.
#[derive(Default)]
struct ChatState {
    busy: bool,
    queue: VecDeque<(Arc<dyn Platform>, InboundMessage)>,
    cancel_token: Option<CancellationToken>,
    /// The chat's single in-flight pending question, if the current run is
    /// paused on one (issue #458: "one parked ask per chat").
    pending_ask: Option<ParkedAsk>,
    /// Resolver for `pending_ask`, held by the render task
    /// (`ConnectBridge::render_until_settled`) that's waiting on it.
    /// `handle_inbound`/`handle_callback` push a resolution here instead of
    /// queuing a matching reply — this is what lets an answer "jump" the
    /// busy queue while the run is genuinely suspended waiting for exactly
    /// this. Buffered at 1 so a resolver can send without the render task
    /// having reached its `recv().await` yet.
    ask_resolution: Option<mpsc::Sender<AskResolution>>,
}

/// What resolved (or invalidated) a chat's parked ask.
#[derive(Debug, Clone)]
enum AskResolution {
    /// A button press or text reply matched the parked ask; submit this as
    /// the answer.
    Answer(String),
    /// `/new`, session rotation, or an explicit clear invalidated the ask
    /// before it was answered — the waiting render task must stop rendering
    /// this (now-abandoned) run rather than hang forever.
    Invalidated,
}

/// Resolved model/prompt/workspace configuration for a connect-driven run,
/// derived from the live global config.
///
/// This is a thin alias over [`bamboo_engine::resolved_defaults::ResolvedDefaultRunConfig`]
/// — the SOLE implementation of this resolution cascade, shared with the
/// public `GET /api/v1/execute/defaults` handler (issue #480). Do not
/// reimplement the model/prompt/workspace cascade here; extend the shared
/// helper instead.
type ResolvedConnectRunConfig = bamboo_engine::resolved_defaults::ResolvedDefaultRunConfig;

fn resolve_connect_run_config(
    config_snapshot: &Config,
    provider_registry: &Arc<ProviderRegistry>,
) -> ResolvedConnectRunConfig {
    bamboo_engine::resolved_defaults::resolve_default_run_config(config_snapshot, provider_registry)
}

/// Builds a fresh session for a connect chat key. Mirrors
/// `schedule_app::session_factory::create_schedule_session`.
///
/// `no_human_approver = false` (issue #458, flipped from phase 1's `true`): a
/// connect-bridged chat now HAS a human attached — the ConnectBridge itself,
/// relaying questions/approvals to and from the chat platform — so gated
/// actions and pending questions should escalate normally rather than being
/// tagged "no interactive approver available" (that tag only actually
/// changes behavior for out-of-process sub-agent workers routing to an
/// off-loop model reviewer — see `subagent_worker`'s `ApprovalProxy`; an
/// in-process run like this one was never auto-decided by it, so this flip
/// is a correctness/semantics fix for anything that reads the flag — e.g.
/// child-session inheritance — rather than a change to THIS run's own pause
/// behavior).
fn create_connect_session(
    key: &str,
    model: &str,
    system_prompt: &str,
    base_system_prompt: &str,
    workspace_path: Option<&str>,
    project_id: Option<&bamboo_domain::ProjectId>,
    reasoning_effort: Option<ReasoningEffort>,
    workspace_resolver: &bamboo_agent_core::workspace_state::WorkspaceResolver,
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
    if let Some(project_id) = project_id {
        session.set_project_id_meta(project_id.to_string());
    }
    if let Some(path) = workspace_path {
        let final_workspace = workspace_resolver.publish_resolved_workspace(
            &session_id,
            PathBuf::from(path),
            "connect",
        );
        session.set_workspace_path_meta(bamboo_config::paths::path_to_display_string(
            &final_workspace,
        ));
    }
    if let Some(effort) = reasoning_effort {
        session.set_reasoning_effort_meta(effort.as_str());
    }
    session.add_message(Message::system(system_prompt.to_string()));
    bamboo_engine::runner::refresh_prompt_snapshot(&mut session);
    session
        .agent_runtime_state
        .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
        .no_human_approver = false;
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
    /// advance). Bounded (issue #454 follow-up: see [`BoundedSeenSet`]) so
    /// it never grows without limit for the life of the process. A
    /// `std::sync::Mutex` is fine here: only ever locked for a single
    /// insert, never held across an `.await`.
    seen_message_ids: StdMutex<BoundedSeenSet>,
    process_start: DateTime<Utc>,
    /// The resolution seam (issue #458): `submit_pending_response` +
    /// `resume_session_execution`, or a fake in tests.
    responder: Arc<dyn Responder>,
}

impl ConnectBridge {
    /// Production constructor: wires up `approvals::EngineResponder` (the
    /// in-proc respond+resume path) automatically. See [`Self::with_responder`]
    /// for injecting a fake in tests.
    pub fn new(ctx: ConnectContext, map_path: Option<PathBuf>) -> Self {
        let responder = Arc::new(approvals::EngineResponder::new(ctx.clone()));
        Self::with_responder(ctx, map_path, responder)
    }

    /// Test/advanced constructor: inject a [`Responder`] seam instead of the
    /// production `EngineResponder` (issue #458: "Design a small Responder
    /// seam on the bridge so tests inject a fake instead of full AppState").
    pub fn with_responder(
        ctx: ConnectContext,
        map_path: Option<PathBuf>,
        responder: Arc<dyn Responder>,
    ) -> Self {
        Self {
            ctx,
            session_map: TokioRwLock::new(HashMap::new()),
            map_path,
            chat_state: AsyncMutex::new(HashMap::new()),
            seen_message_ids: StdMutex::new(BoundedSeenSet::new(DEDUP_CAPACITY)),
            process_start: Utc::now(),
            responder,
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

    /// Rotates the chat's session mapping (`/new`). Also invalidates any
    /// parked ask first (issue #458: "`/new` and session rotation invalidate
    /// parked asks") — an ask answered after its session has been rotated
    /// away would resolve a question nobody can see anymore.
    async fn rotate_session(&self, key: &str) {
        self.invalidate_pending_ask(key).await;
        {
            let mut map = self.session_map.write().await;
            map.remove(key);
        }
        self.persist_session_map().await;
    }

    /// Whether `key`'s chat currently has a parked ask awaiting resolution.
    async fn has_pending_ask(&self, key: &str) -> bool {
        self.chat_state
            .lock()
            .await
            .get(key)
            .is_some_and(|state| state.pending_ask.is_some())
    }

    /// If `key` has a parked ask AND `resolve` matches it, atomically clears
    /// the parked ask + its resolver (so a concurrent duplicate resolution —
    /// e.g. a button press racing a text reply — finds nothing left to
    /// match) and returns the answer plus the channel to notify the waiting
    /// render task on. `resolve` runs while holding the chat-state lock, so
    /// it must be cheap and non-async (pure pattern matching against the
    /// parked ask — see `approvals::match_text_answer`/`match_callback_data`).
    async fn try_resolve_pending_ask(
        &self,
        key: &str,
        resolve: impl FnOnce(&ParkedAsk) -> Option<String>,
    ) -> Option<(String, mpsc::Sender<AskResolution>)> {
        let mut guard = self.chat_state.lock().await;
        let state = guard.get_mut(key)?;
        let ask_ref = state.pending_ask.as_ref()?;
        let answer = resolve(ask_ref)?;
        let sender = state.ask_resolution.take()?;
        state.pending_ask = None;
        Some((answer, sender))
    }

    /// Clears `key`'s parked ask (if any) and wakes its waiting render task
    /// with [`AskResolution::Invalidated`] instead of an answer.
    async fn invalidate_pending_ask(&self, key: &str) {
        let sender = {
            let mut guard = self.chat_state.lock().await;
            match guard.get_mut(key) {
                Some(state) => {
                    state.pending_ask = None;
                    state.ask_resolution.take()
                }
                None => None,
            }
        };
        if let Some(sender) = sender {
            let _ = sender.send(AskResolution::Invalidated).await;
        }
    }

    /// Clears `key`'s parked ask + resolver without sending a resolution —
    /// used once a render task has already consumed one (whether an answer
    /// or an invalidation) so a stale entry never lingers.
    async fn clear_pending_ask(&self, key: &str) {
        let mut guard = self.chat_state.lock().await;
        if let Some(state) = guard.get_mut(key) {
            state.pending_ask = None;
            state.ask_resolution = None;
        }
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

        // Ask-resolution fast path (issue #458): a parked ask takes priority
        // over normal busy/queue routing, even while `busy` is still true —
        // the run backing it is genuinely suspended waiting for exactly this
        // reply, so it must never sit behind the FIFO queue. A non-matching
        // reply on a CLOSED ask (no free text allowed) falls through to the
        // normal busy/queue handling below, exactly like any other message.
        if let Some((answer, sender)) = self
            .try_resolve_pending_ask(&key, |ask| approvals::match_text_answer(ask, &msg.text))
            .await
        {
            let _ = sender.send(AskResolution::Answer(answer)).await;
            return;
        }

        // `/new` is always an immediate escape hatch out of a parked ask
        // (bypassing the queue, which would never drain while the chat waits
        // on an answer nobody typed correctly) — the ordinary `/new` path in
        // `process_one` still handles the non-paused case unchanged.
        if command.eq_ignore_ascii_case("/new") && self.has_pending_ask(&key).await {
            self.rotate_session(&key).await;
            reply_text(&platform, &msg.reply_ctx, "Started a new session.").await;
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

    /// Entry point for every inbound button-press callback (issue #458).
    /// Unlike text messages, a callback NEVER queues and NEVER starts a run —
    /// it can only ever resolve (or fail to resolve) the chat's parked ask.
    /// Per the design constraint, the platform is ALWAYS acked
    /// (`answer_callback`), even for a stale/forged/non-matching one, and a
    /// non-match is dropped silently rather than ever being forwarded as
    /// user text.
    pub async fn handle_callback(
        self: Arc<Self>,
        platform: Arc<dyn Platform>,
        allow_from: Vec<String>,
        callback: CallbackQuery,
    ) {
        if !allow_from
            .iter()
            .any(|allowed| allowed == &callback.user_id)
        {
            tracing::warn!(
                platform = %callback.platform,
                chat_id = %callback.chat_id,
                user_id = %callback.user_id,
                "connect: rejected callback query — user not in allow_from"
            );
            let _ = platform
                .answer_callback(&callback.callback_query_id, None)
                .await;
            return;
        }

        let key = SessionKey {
            platform: callback.platform.clone(),
            chat_id: callback.chat_id.clone(),
            user_id: callback.user_id.clone(),
        }
        .as_string();

        let resolved = self
            .try_resolve_pending_ask(&key, |ask| {
                approvals::match_callback_data(ask, &callback.data)
            })
            .await;

        match resolved {
            Some((answer, sender)) => {
                let _ = platform
                    .answer_callback(&callback.callback_query_id, None)
                    .await;
                let _ = sender.send(AskResolution::Answer(answer)).await;
            }
            None => {
                tracing::debug!(
                    platform = %callback.platform,
                    chat_id = %callback.chat_id,
                    "connect: dropping stale/forged callback_data"
                );
                let _ = platform
                    .answer_callback(
                        &callback.callback_query_id,
                        Some("This action has expired."),
                    )
                    .await;
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
        // A run paused on a parked ask has no live task for `cancel_token` to
        // reach (the round that produced the question already returned) — an
        // ask invalidation is what actually unblocks
        // `render_until_settled`'s wait in that case.
        let had_pending_ask = self.has_pending_ask(key).await;
        if had_pending_ask {
            self.invalidate_pending_ask(key).await;
        }
        match (token, had_pending_ask) {
            (Some(token), _) => {
                token.cancel();
                reply_text(platform, reply_ctx, "Stopping the current run…").await;
            }
            (None, true) => {
                reply_text(
                    platform,
                    reply_ctx,
                    "Stopped — the pending question was cancelled.",
                )
                .await;
            }
            (None, false) => {
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
    ) -> Result<Session, String> {
        let model = resolved.model_roster.model.clone().unwrap_or_default();
        let platform = key.split(':').next().unwrap_or_default();
        let project_id = self.ctx.project_ids_by_platform.get(platform);
        if let Some(project_id) = project_id {
            match self.ctx.project_store.get(project_id) {
                Ok(project) if project.status == bamboo_domain::ProjectStatus::Active => {}
                Ok(_) => {
                    return Err(format!(
                        "Connect Project {project_id} is archived; no session was created"
                    ));
                }
                Err(error) => {
                    return Err(format!(
                        "Connect Project {project_id} is unavailable; no session was created: {error}"
                    ));
                }
            }
        }
        let final_workspace = crate::project_context::validate_workspace_assignment_with_resolver(
            &self.ctx.project_store,
            project_id,
            project_id
                .is_none()
                .then_some(resolved.workspace_path.as_deref())
                .flatten(),
            &self.ctx.workspace_resolver,
        )
        .map_err(|error| {
            format!("Connect workspace is unavailable; no session was created: {error}")
        })?;
        let final_workspace_display = final_workspace
            .as_deref()
            .map(bamboo_config::paths::path_to_display_string);
        let binding_status = match (project_id, final_workspace.as_deref()) {
            (Some(project_id), Some(workspace)) => {
                let workspace = bamboo_config::paths::path_to_display_string(workspace);
                if self
                    .ctx
                    .project_store
                    .find_workspace_owner_for_path(&workspace)
                    .map_err(|error| format!("resolve Connect workspace owner: {error}"))?
                    .is_some_and(|owner| owner.id == *project_id)
                {
                    bamboo_engine::project_context::WorkspaceBindingStatus::Registered
                } else {
                    bamboo_engine::project_context::WorkspaceBindingStatus::Unregistered
                }
            }
            _ => bamboo_engine::project_context::WorkspaceBindingStatus::Unregistered,
        };
        let system_prompt =
            bamboo_engine::runtime::context::upsert_workspace_prompt_context_with_source(
                &resolved.system_prompt,
                final_workspace_display.as_deref(),
                binding_status,
                project_id.map(|_| bamboo_engine::project_context::WorkspaceSource::ProjectDefault),
            );
        let mut session = create_connect_session(
            key,
            &model,
            &system_prompt,
            &resolved.base_system_prompt,
            final_workspace_display.as_deref(),
            project_id,
            resolved.reasoning_effort,
            &self.ctx.workspace_resolver,
        );
        if project_id.is_some() {
            session.metadata.insert(
                bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
                bamboo_engine::project_context::WorkspaceSource::ProjectDefault
                    .as_str()
                    .to_string(),
            );
        }
        self.set_session_id_for_key(key, &session.id).await;
        Ok(session)
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
        let session = match existing_id {
            Some(id) => match self.ctx.session_repo.load_merged(&id).await {
                Some(session) => Ok(session),
                None => self.create_and_register_session(key, &resolved).await,
            },
            None => self.create_and_register_session(key, &resolved).await,
        };
        let mut session = match session {
            Ok(session) => session,
            Err(error) => {
                reply_text(&platform, reply_ctx, &error).await;
                return;
            }
        };

        let session_id = session.id.clone();
        let session_tx =
            get_or_create_event_sender(&self.ctx.session_event_senders, &session_id).await;
        let execution_reservation = match reserve_session_execution(
            &self.ctx.agent,
            &self.ctx.agent_runners,
            &self.ctx.session_event_senders,
            &session_id,
            &session_tx,
        )
        .await
        {
            SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
            SessionExecutionReserveOutcome::AlreadyRunning { .. } => {
                reply_text(
                    &platform,
                    reply_ctx,
                    "This session is already running elsewhere; please wait for it to finish.",
                )
                .await;
                return;
            }
        };
        let rx = session_tx.subscribe();

        // Only the exact shared runner/router owner may publish a new prompt or
        // mutate process-global permission workspace state.
        session.add_message(Message::user(text.to_string()));
        if let Some(config) = self.ctx.permission_checker.permission_config() {
            if let Some(workspace) = session.workspace.as_ref() {
                config.register_session_workspace(session_id.clone(), workspace.clone());
            }
            session.metadata.insert(
                "permission.policy_revision".to_string(),
                config.policy_revision().to_string(),
            );
            session.metadata.insert(
                "permission.effective_mode".to_string(),
                format!("{:?}", config.mode()).to_ascii_lowercase(),
            );
        }
        self.ctx.session_repo.save_and_cache(&mut session).await;

        self.set_cancel_token(key, execution_reservation.cancel_token().clone())
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
            execution_reservation,
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
            // No per-request override on this path; the config-level default
            // (issue #221) still applies.
            run_budget: None,
            runners: self.ctx.agent_runners.clone(),
            sessions_cache: self.ctx.session_repo.cache().clone(),
            on_complete: None,
            // Connect drives root sessions; a child finishing on this path
            // is backstopped by the child-wait watchdog (#546).
            child_completion_handler: None,
        });

        self.render_until_settled(key, platform, reply_ctx.clone(), &session_id, rx)
            .await;

        self.clear_cancel_token(key).await;
    }

    /// Renders one run to completion, looping back for as many
    /// pause/answer/resume cycles as it takes to reach a genuinely terminal
    /// state (issue #458). On [`render::RunOutcome::Paused`]: parks the ask,
    /// renders it (buttons when the platform supports them, always also a
    /// numbered text list), and waits for a resolution pushed by
    /// `handle_inbound`'s ask-resolution fast path or `handle_callback` —
    /// or an invalidation from `/new`/rotation/`/stop`. A resolved answer is
    /// submitted through `self.responder`, which mirrors
    /// `POST /sessions/{id}/respond`'s exact resolve-then-resume sequence;
    /// the resumed run's fresh broadcast receiver is looped back into
    /// another `render::stream_execution` call — together with the streaming
    /// renderer's carried-over [`render::StreamState`] — so the SAME chat
    /// keeps watching the SAME (now-continuing) run in the SAME status
    /// message (one "⏳ Working…" bubble per run, no matter how many times it
    /// pauses).
    async fn render_until_settled(
        &self,
        key: &str,
        platform: Arc<dyn Platform>,
        reply_ctx: ReplyCtx,
        session_id: &str,
        mut rx: broadcast::Receiver<AgentEvent>,
    ) {
        let mut stream_state: Option<Box<render::StreamState>> = None;
        loop {
            match render::stream_execution(
                platform.clone(),
                reply_ctx.clone(),
                rx,
                stream_state.take(),
            )
            .await
            {
                render::RunOutcome::Terminal => return,
                render::RunOutcome::Paused {
                    ask,
                    stream_state: paused_state,
                } => {
                    stream_state = paused_state;
                    let caps = platform.capabilities();
                    let parked =
                        ParkedAsk::new(approvals::new_nonce(), session_id.to_string(), &ask);

                    if let Err(error) =
                        approvals::render_ask(&platform, &reply_ctx, &parked, caps.buttons).await
                    {
                        tracing::warn!("connect: failed to render pending ask: {error}");
                    }

                    let (ask_tx, mut ask_rx) = mpsc::channel(1);
                    {
                        let mut guard = self.chat_state.lock().await;
                        let state = guard.entry(key.to_string()).or_default();
                        state.pending_ask = Some(parked);
                        state.ask_resolution = Some(ask_tx);
                    }

                    match ask_rx.recv().await {
                        Some(AskResolution::Answer(answer)) => {
                            match self.responder.respond_and_resume(session_id, answer).await {
                                Ok(RespondAndResumeOutcome::Resumed(new_rx)) => {
                                    rx = new_rx;
                                    continue;
                                }
                                Ok(RespondAndResumeOutcome::NotResumed(reason)) => {
                                    reply_text(&platform, &reply_ctx, format!("({reason})")).await;
                                    return;
                                }
                                Err(error) => {
                                    reply_text(
                                        &platform,
                                        &reply_ctx,
                                        format!("Failed to record your answer: {error}"),
                                    )
                                    .await;
                                    return;
                                }
                            }
                        }
                        Some(AskResolution::Invalidated) | None => {
                            // Already cleared by the invalidator in the
                            // common case; clear defensively so a stale entry
                            // never lingers if the sender was dropped instead
                            // (e.g. a bug elsewhere) rather than sending
                            // `Invalidated` explicitly.
                            self.clear_pending_ask(key).await;
                            return;
                        }
                    }
                }
            }
        }
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
    /// `reply()`/`edit()`/`answer_callback()` call instead of talking to a
    /// real IM API. `capabilities` is configurable (issue #458 tests need
    /// buttons+edit_message; the original #452 tests want the all-`false`
    /// default).
    struct FakePlatform {
        label: String,
        capabilities: super::super::platform::Capabilities,
        sent: TokioMutex<Vec<String>>,
        sent_messages: TokioMutex<Vec<OutboundMessage>>,
        edits: TokioMutex<Vec<String>>,
        answered_callbacks: TokioMutex<Vec<(String, Option<String>)>>,
    }

    impl FakePlatform {
        fn new(label: &str) -> Arc<Self> {
            Self::with_capabilities(label, Default::default())
        }

        fn with_capabilities(
            label: &str,
            capabilities: super::super::platform::Capabilities,
        ) -> Arc<Self> {
            Arc::new(Self {
                label: label.to_string(),
                capabilities,
                sent: TokioMutex::new(Vec::new()),
                sent_messages: TokioMutex::new(Vec::new()),
                edits: TokioMutex::new(Vec::new()),
                answered_callbacks: TokioMutex::new(Vec::new()),
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
            self.capabilities
        }
        async fn start(
            &self,
            _inbound: tokio::sync::mpsc::Sender<super::super::platform::Inbound>,
        ) -> super::super::platform::PlatformResult<()> {
            Ok(())
        }
        async fn reply(
            &self,
            _ctx: &ReplyCtx,
            msg: OutboundMessage,
        ) -> super::super::platform::PlatformResult<super::super::platform::MessageRef> {
            self.sent.lock().await.push(msg.text.clone());
            self.sent_messages.lock().await.push(msg);
            Ok(super::super::platform::MessageRef(serde_json::Value::Null))
        }
        async fn edit(
            &self,
            _msg_ref: &super::super::platform::MessageRef,
            new: OutboundMessage,
        ) -> super::super::platform::PlatformResult<()> {
            self.edits.lock().await.push(new.text);
            Ok(())
        }
        async fn answer_callback(
            &self,
            callback_query_id: &str,
            text: Option<&str>,
        ) -> super::super::platform::PlatformResult<()> {
            self.answered_callbacks
                .lock()
                .await
                .push((callback_query_id.to_string(), text.map(str::to_string)));
            Ok(())
        }
        async fn stop(&self) -> super::super::platform::PlatformResult<()> {
            Ok(())
        }
    }

    /// Fake [`Responder`] (issue #458: "tests inject a fake instead of full
    /// AppState"): records every submitted answer and hands back a
    /// broadcast receiver subscribed to a test-controlled sender, so a test
    /// can drive the "resumed run" by sending events directly.
    struct FakeResponder {
        calls: TokioMutex<Vec<(String, String)>>,
        resume_sender: broadcast::Sender<AgentEvent>,
        fail_with: Option<String>,
    }

    impl FakeResponder {
        fn new(resume_sender: broadcast::Sender<AgentEvent>) -> Arc<Self> {
            Arc::new(Self {
                calls: TokioMutex::new(Vec::new()),
                resume_sender,
                fail_with: None,
            })
        }

        fn failing(resume_sender: broadcast::Sender<AgentEvent>, reason: &str) -> Arc<Self> {
            Arc::new(Self {
                calls: TokioMutex::new(Vec::new()),
                resume_sender,
                fail_with: Some(reason.to_string()),
            })
        }
    }

    #[async_trait::async_trait]
    impl Responder for FakeResponder {
        async fn respond_and_resume(
            &self,
            session_id: &str,
            answer: String,
        ) -> Result<RespondAndResumeOutcome, super::super::approvals::ResponderError> {
            self.calls
                .lock()
                .await
                .push((session_id.to_string(), answer));
            if let Some(reason) = &self.fail_with {
                return Err(super::super::approvals::ResponderError::Other(
                    reason.clone(),
                ));
            }
            Ok(RespondAndResumeOutcome::Resumed(
                self.resume_sender.subscribe(),
            ))
        }
    }

    /// Polls `bridge`'s internal chat state until `key` has a parked ask (or
    /// panics past a 5s deadline) — used to synchronize with
    /// `render_until_settled`'s pause branch, which parks the ask
    /// asynchronously.
    async fn wait_for_parked_ask(bridge: &ConnectBridge, key: &str) -> ParkedAsk {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(ask) = bridge
                .chat_state
                .lock()
                .await
                .get(key)
                .and_then(|state| state.pending_ask.clone())
            {
                return ask;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "ask was never parked for {key}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Polls until `responder` has recorded at least `count` calls (or panics
    /// past a 5s deadline) — used to synchronize with
    /// `FakeResponder::subscribe` actually happening inside
    /// `render_until_settled`'s spawned task BEFORE a test sends an event on
    /// the "resumed" broadcast channel (`broadcast::Sender::send` errors with
    /// zero live subscribers).
    async fn wait_for_responder_calls(responder: &FakeResponder, count: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if responder.calls.lock().await.len() >= count {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "responder never reached {count} call(s)"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
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
            cfg.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
                api_key: String::new(),
                api_key_from_env: false,
                api_key_encrypted: None,
                credential_ref: None,
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
            project_store: state.project_store.clone(),
            workspace_resolver: state.workspace_resolver.clone(),
            project_ids_by_platform: Arc::new(HashMap::new()),
            permission_checker: state.permission_checker.clone(),
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

    // ---- Issue #454 follow-up: bounded dedup set ----

    #[test]
    fn bounded_seen_set_evicts_the_oldest_entry_once_over_capacity() {
        let mut set = BoundedSeenSet::new(2);
        assert!(set.insert("a".to_string()));
        assert!(set.insert("b".to_string()));
        assert_eq!(set.len(), 2);

        // Pushes past capacity: "a" (oldest) is evicted; "b" and "c" remain
        // tracked as seen.
        assert!(set.insert("c".to_string()));
        assert_eq!(set.len(), 2);
        assert!(!set.insert("b".to_string()), "b must still be tracked");
        assert!(!set.insert("c".to_string()), "c must still be tracked");

        // "a" was evicted — re-inserting it is treated as new, not a
        // duplicate (which in turn evicts "b", the now-oldest entry).
        assert!(set.insert("a".to_string()));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn bounded_seen_set_still_dedups_within_capacity() {
        let mut set = BoundedSeenSet::new(10);
        assert!(set.insert("x".to_string()));
        assert!(!set.insert("x".to_string()), "duplicate must be rejected");
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn bounded_seen_set_never_grows_past_capacity() {
        let mut set = BoundedSeenSet::new(50);
        for i in 0..5_000 {
            set.insert(format!("msg-{i}"));
        }
        assert_eq!(set.len(), 50);
    }

    #[tokio::test]
    async fn connect_session_creation_revalidates_project_after_startup() {
        let (mut ctx, _dir) = test_context().await;
        let project = ctx.project_store.create("Connect", None).unwrap();
        let mut projects = HashMap::new();
        projects.insert("fake".to_string(), project.id.clone());
        ctx.project_ids_by_platform = Arc::new(projects);
        ctx.project_store
            .archive(&project.id, project.revision)
            .unwrap();
        let resolved = {
            let config = ctx.config.read().await.clone();
            resolve_connect_run_config(&config, &ctx.provider_registry)
        };
        let bridge = ConnectBridge::new(ctx, None);

        let error = bridge
            .create_and_register_session("fake:chat:user", &resolved)
            .await
            .expect_err("archived Project must reject Connect session creation");
        assert!(error.contains("archived"));
        assert!(
            bridge.session_id_for_key("fake:chat:user").await.is_none(),
            "failed validation must not publish a chat-to-session mapping"
        );
    }

    #[test]
    fn connect_publication_uses_the_validating_instance_workspace_root() {
        let instance_root = tempfile::tempdir().expect("instance workspace root");
        let relocated = instance_root.path().join("connect-workspace");
        let resolver = bamboo_agent_core::workspace_state::WorkspaceResolver::new(|| None, {
            let root = instance_root.path().to_path_buf();
            move || bamboo_agent_core::workspace_state::WorkspaceRootConfig {
                root: root.clone(),
                confine: true,
            }
        });

        let session = create_connect_session(
            "fake:chat:user",
            "model",
            "system",
            "base",
            Some(relocated.to_string_lossy().as_ref()),
            None,
            None,
            &resolver,
        );

        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(relocated.to_string_lossy().as_ref())
        );
        assert!(
            relocated.is_dir(),
            "the AppState resolver must materialize its own validated target"
        );
    }

    #[tokio::test]
    async fn connect_session_uses_assigned_project_path_when_workspace_is_omitted() {
        let (mut ctx, _dir) = test_context().await;
        let project_path = tempfile::tempdir().expect("Project path");
        let project = ctx
            .project_store
            .create_with_project_path(
                "Connect",
                None,
                project_path.path().to_string_lossy(),
                Vec::new(),
            )
            .unwrap();
        let mut projects = HashMap::new();
        projects.insert("fake".to_string(), project.id.clone());
        ctx.project_ids_by_platform = Arc::new(projects);
        let mut resolved = {
            let config = ctx.config.read().await.clone();
            resolve_connect_run_config(&config, &ctx.provider_registry)
        };
        let foreign_global = tempfile::tempdir().expect("foreign global workspace");
        resolved.workspace_path = Some(foreign_global.path().to_string_lossy().into_owned());
        let bridge = ConnectBridge::new(ctx, None);

        let session = bridge
            .create_and_register_session("fake:chat:user", &resolved)
            .await
            .expect("configured Project must provide the Connect workspace");
        assert_eq!(
            session.project_id_meta().as_deref(),
            Some(project.id.as_str())
        );
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            project.project_path.as_deref()
        );
        assert_eq!(
            session
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str())
        );
        let system_prompt = session
            .messages
            .iter()
            .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
            .expect("Connect system prompt");
        assert!(system_prompt
            .content
            .contains("Workspace source: project_default"));
    }

    #[tokio::test]
    async fn connect_session_ignores_global_workspace_owned_by_another_project() {
        let (mut ctx, _dir) = test_context().await;
        let project_path = tempfile::tempdir().expect("Connect Project path");
        let workspace = tempfile::tempdir().expect("foreign global workspace");
        let connect_project = ctx
            .project_store
            .create_with_project_path(
                "Connect",
                None,
                project_path.path().to_string_lossy(),
                Vec::new(),
            )
            .unwrap();
        let _workspace_owner = ctx
            .project_store
            .create_with_bindings(
                "Workspace Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace.path().to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("Workspace Owner");
        let mut projects = HashMap::new();
        projects.insert("fake".to_string(), connect_project.id.clone());
        ctx.project_ids_by_platform = Arc::new(projects);
        let mut resolved = {
            let config = ctx.config.read().await.clone();
            resolve_connect_run_config(&config, &ctx.provider_registry)
        };
        resolved.workspace_path = Some(workspace.path().to_string_lossy().into_owned());
        let bridge = ConnectBridge::new(ctx, None);

        let session = bridge
            .create_and_register_session("fake:chat:user", &resolved)
            .await
            .expect("assigned Connect must ignore the foreign global default");
        assert_eq!(
            session.project_id_meta().as_deref(),
            Some(connect_project.id.as_str())
        );
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            connect_project.project_path.as_deref()
        );
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

    // ---- Issue #458: approval/question relay ----

    fn buttons_and_edit_capabilities() -> super::super::platform::Capabilities {
        super::super::platform::Capabilities {
            buttons: true,
            edit_message: true,
            images: false,
            files: false,
        }
    }

    fn need_clarification_event(
        question: &str,
        options: Vec<&str>,
        allow_custom: bool,
    ) -> AgentEvent {
        AgentEvent::NeedClarification {
            question: question.to_string(),
            options: Some(options.into_iter().map(str::to_string).collect()),
            tool_call_id: Some("call-1".to_string()),
            tool_name: Some("request_permissions".to_string()),
            allow_custom,
        }
    }

    #[tokio::test]
    async fn paused_run_renders_buttons_with_nonce_and_resolves_via_callback() {
        let (ctx, _dir) = test_context().await;
        let resume_tx = broadcast::channel::<AgentEvent>(16).0;
        let responder = FakeResponder::new(resume_tx.clone());
        let bridge = Arc::new(ConnectBridge::with_responder(ctx, None, responder.clone()));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("chat1", "u1");
        let reply_ctx = ReplyCtx(serde_json::json!({ "chat_id": "chat1" }));

        let (tx, rx) = broadcast::channel(16);
        tx.send(need_clarification_event(
            "Approve?",
            vec!["Approve", "Deny"],
            false,
        ))
        .unwrap();

        let render_handle = {
            let bridge = bridge.clone();
            let platform = platform.clone();
            let reply_ctx = reply_ctx.clone();
            let key = key.clone();
            tokio::spawn(async move {
                bridge
                    .render_until_settled(&key, platform, reply_ctx, "sess-1", rx)
                    .await;
            })
        };

        let parked = wait_for_parked_ask(&bridge, &key).await;
        assert_eq!(
            parked.options,
            vec!["Approve".to_string(), "Deny".to_string()]
        );

        // Buttons were rendered on the ask message, one per option, callback
        // data carrying the nonce (never raw option text/user text).
        let sent = platform.sent_messages.lock().await.clone();
        let ask_message = sent
            .iter()
            .find(|message| message.buttons.is_some())
            .expect("expected a buttoned ask message");
        let buttons = ask_message.buttons.as_ref().unwrap();
        assert_eq!(buttons.len(), 2);
        assert_eq!(buttons[0][0].callback_data, format!("{}:0", parked.nonce));
        assert_eq!(buttons[1][0].callback_data, format!("{}:1", parked.nonce));

        let callback = CallbackQuery {
            platform: "fake".to_string(),
            chat_id: "chat1".to_string(),
            user_id: "u1".to_string(),
            callback_query_id: "cbq-1".to_string(),
            data: format!("{}:0", parked.nonce),
            reply_ctx: reply_ctx.clone(),
        };
        ConnectBridge::handle_callback(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            callback,
        )
        .await;

        // Wait for `render_until_settled`'s spawned task to have actually
        // called through to the responder (and thus subscribed to
        // `resume_tx`) before sending on it — `broadcast::Sender::send`
        // errors out with zero live subscribers.
        wait_for_responder_calls(&responder, 1).await;

        resume_tx
            .send(AgentEvent::Complete {
                usage: bamboo_agent_core::TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    total_tokens: 2,
                },
            })
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), render_handle)
            .await
            .expect("render task must finish")
            .unwrap();

        assert_eq!(
            responder.calls.lock().await.as_slice(),
            &[("sess-1".to_string(), "Approve".to_string())]
        );
        assert_eq!(
            platform.answered_callbacks.lock().await.as_slice(),
            &[("cbq-1".to_string(), None)]
        );
        assert!(!bridge.has_pending_ask(&key).await);
    }

    #[tokio::test]
    async fn stale_callback_nonce_is_dropped_and_acked_without_resolving() {
        let (ctx, _dir) = test_context().await;
        let resume_tx = broadcast::channel::<AgentEvent>(16).0;
        let responder = FakeResponder::new(resume_tx.clone());
        let bridge = Arc::new(ConnectBridge::with_responder(ctx, None, responder.clone()));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("chat1", "u1");
        let reply_ctx = ReplyCtx(serde_json::json!({ "chat_id": "chat1" }));

        let (tx, rx) = broadcast::channel(16);
        tx.send(need_clarification_event(
            "Approve?",
            vec!["Approve", "Deny"],
            false,
        ))
        .unwrap();

        let render_handle = {
            let bridge = bridge.clone();
            let platform = platform.clone();
            let reply_ctx = reply_ctx.clone();
            let key = key.clone();
            tokio::spawn(async move {
                bridge
                    .render_until_settled(&key, platform, reply_ctx, "sess-1", rx)
                    .await;
            })
        };

        wait_for_parked_ask(&bridge, &key).await;

        let stale_callback = CallbackQuery {
            platform: "fake".to_string(),
            chat_id: "chat1".to_string(),
            user_id: "u1".to_string(),
            callback_query_id: "cbq-stale".to_string(),
            data: "totally-wrong-nonce:0".to_string(),
            reply_ctx: reply_ctx.clone(),
        };
        ConnectBridge::handle_callback(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            stale_callback,
        )
        .await;

        // Acked (Telegram-style — always ack), but with an "expired" style
        // note, and NOT forwarded as an answer.
        let acked = platform.answered_callbacks.lock().await.clone();
        assert_eq!(acked.len(), 1);
        assert_eq!(acked[0].0, "cbq-stale");
        assert!(acked[0].1.is_some());
        assert!(responder.calls.lock().await.is_empty());
        // The real ask is still parked — the stale callback never touched it.
        assert!(bridge.has_pending_ask(&key).await);

        bridge.invalidate_pending_ask(&key).await;
        tokio::time::timeout(Duration::from_secs(5), render_handle)
            .await
            .expect("render task must finish")
            .unwrap();
    }

    #[tokio::test]
    async fn text_answer_resolves_an_open_question() {
        let (ctx, _dir) = test_context().await;
        let resume_tx = broadcast::channel::<AgentEvent>(16).0;
        let responder = FakeResponder::new(resume_tx.clone());
        let bridge = Arc::new(ConnectBridge::with_responder(ctx, None, responder.clone()));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("chat1", "u1");
        let reply_ctx = ReplyCtx(serde_json::json!({ "chat_id": "chat1" }));

        let (tx, rx) = broadcast::channel(16);
        tx.send(need_clarification_event(
            "Anything else?",
            vec!["OK", "Need changes"],
            true,
        ))
        .unwrap();

        let render_handle = {
            let bridge = bridge.clone();
            let platform = platform.clone();
            let reply_ctx = reply_ctx.clone();
            let key = key.clone();
            tokio::spawn(async move {
                bridge
                    .render_until_settled(&key, platform, reply_ctx, "sess-1", rx)
                    .await;
            })
        };

        wait_for_parked_ask(&bridge, &key).await;

        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            inbound("chat1", "u1", "answer-1", "please also add tests"),
        )
        .await;

        // Wait for `render_until_settled`'s spawned task to have actually
        // called through to the responder (and thus subscribed to
        // `resume_tx`) before sending on it — `broadcast::Sender::send`
        // errors out with zero live subscribers.
        wait_for_responder_calls(&responder, 1).await;

        resume_tx
            .send(AgentEvent::Complete {
                usage: bamboo_agent_core::TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    total_tokens: 2,
                },
            })
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), render_handle)
            .await
            .expect("render task must finish")
            .unwrap();

        assert_eq!(
            responder.calls.lock().await.as_slice(),
            &[("sess-1".to_string(), "please also add tests".to_string())]
        );
    }

    #[tokio::test]
    async fn binary_ask_keyword_mapping_resolves_via_text() {
        let (ctx, _dir) = test_context().await;
        let resume_tx = broadcast::channel::<AgentEvent>(16).0;
        let responder = FakeResponder::new(resume_tx.clone());
        let bridge = Arc::new(ConnectBridge::with_responder(ctx, None, responder.clone()));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("chat1", "u1");
        let reply_ctx = ReplyCtx(serde_json::json!({ "chat_id": "chat1" }));

        let (tx, rx) = broadcast::channel(16);
        tx.send(need_clarification_event(
            "Approve?",
            vec!["Approve", "Deny"],
            false,
        ))
        .unwrap();

        let render_handle = {
            let bridge = bridge.clone();
            let platform = platform.clone();
            let reply_ctx = reply_ctx.clone();
            let key = key.clone();
            tokio::spawn(async move {
                bridge
                    .render_until_settled(&key, platform, reply_ctx, "sess-1", rx)
                    .await;
            })
        };

        wait_for_parked_ask(&bridge, &key).await;

        // "允许" (allow) is a first-affirmative keyword — resolves to
        // whichever option reads as approval, NEVER the raw keyword text.
        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            inbound("chat1", "u1", "answer-1", "允许"),
        )
        .await;

        // Wait for `render_until_settled`'s spawned task to have actually
        // called through to the responder (and thus subscribed to
        // `resume_tx`) before sending on it — `broadcast::Sender::send`
        // errors out with zero live subscribers.
        wait_for_responder_calls(&responder, 1).await;

        resume_tx
            .send(AgentEvent::Complete {
                usage: bamboo_agent_core::TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    total_tokens: 2,
                },
            })
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), render_handle)
            .await
            .expect("render task must finish")
            .unwrap();

        assert_eq!(
            responder.calls.lock().await.as_slice(),
            &[("sess-1".to_string(), "Approve".to_string())]
        );
    }

    #[tokio::test]
    async fn new_command_invalidates_a_parked_ask_instead_of_answering_it() {
        let (ctx, _dir) = test_context().await;
        let resume_tx = broadcast::channel::<AgentEvent>(16).0;
        let responder = FakeResponder::new(resume_tx.clone());
        let bridge = Arc::new(ConnectBridge::with_responder(ctx, None, responder.clone()));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("chat1", "u1");
        let reply_ctx = ReplyCtx(serde_json::json!({ "chat_id": "chat1" }));

        bridge.set_session_id_for_key(&key, "sess-1").await;

        let (tx, rx) = broadcast::channel(16);
        tx.send(need_clarification_event(
            "Approve?",
            vec!["Approve", "Deny"],
            false,
        ))
        .unwrap();

        let render_handle = {
            let bridge = bridge.clone();
            let platform = platform.clone();
            let reply_ctx = reply_ctx.clone();
            let key = key.clone();
            tokio::spawn(async move {
                bridge
                    .render_until_settled(&key, platform, reply_ctx, "sess-1", rx)
                    .await;
            })
        };

        wait_for_parked_ask(&bridge, &key).await;

        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            inbound("chat1", "u1", "new-1", "/new"),
        )
        .await;

        tokio::time::timeout(Duration::from_secs(5), render_handle)
            .await
            .expect("render task must finish")
            .unwrap();

        assert!(responder.calls.lock().await.is_empty());
        assert!(!bridge.has_pending_ask(&key).await);
        // Session mapping was rotated away, same as an ordinary `/new`.
        assert!(bridge.session_id_for_key(&key).await.is_none());
    }

    #[tokio::test]
    async fn respond_error_reports_to_the_chat_without_hanging() {
        let (ctx, _dir) = test_context().await;
        let resume_tx = broadcast::channel::<AgentEvent>(16).0;
        let responder = FakeResponder::failing(resume_tx.clone(), "boom");
        let bridge = Arc::new(ConnectBridge::with_responder(ctx, None, responder.clone()));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("chat1", "u1");
        let reply_ctx = ReplyCtx(serde_json::json!({ "chat_id": "chat1" }));

        let (tx, rx) = broadcast::channel(16);
        tx.send(need_clarification_event(
            "Approve?",
            vec!["Approve", "Deny"],
            false,
        ))
        .unwrap();

        let render_handle = {
            let bridge = bridge.clone();
            let platform = platform.clone();
            let reply_ctx = reply_ctx.clone();
            let key = key.clone();
            tokio::spawn(async move {
                bridge
                    .render_until_settled(&key, platform, reply_ctx, "sess-1", rx)
                    .await;
            })
        };

        let parked = wait_for_parked_ask(&bridge, &key).await;
        let callback = CallbackQuery {
            platform: "fake".to_string(),
            chat_id: "chat1".to_string(),
            user_id: "u1".to_string(),
            callback_query_id: "cbq-1".to_string(),
            data: format!("{}:0", parked.nonce),
            reply_ctx: reply_ctx.clone(),
        };
        ConnectBridge::handle_callback(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            callback,
        )
        .await;

        tokio::time::timeout(Duration::from_secs(5), render_handle)
            .await
            .expect("render task must finish even when the responder errors")
            .unwrap();

        let sent = platform.sent_texts().await;
        assert!(
            sent.iter()
                .any(|text| text.contains("Failed to record your answer")),
            "expected an error report, got: {sent:?}"
        );
    }

    /// PR #459 review must-fix: a run that pauses (and resumes) MULTIPLE
    /// times must keep exactly ONE status message — every resumed segment
    /// keeps EDITING the original "⏳ Working…" bubble via the carried
    /// `render::StreamState`, never opening a new one.
    #[tokio::test]
    async fn run_that_pauses_twice_keeps_a_single_status_message() {
        let (ctx, _dir) = test_context().await;
        let resume_tx = broadcast::channel::<AgentEvent>(16).0;
        let responder = FakeResponder::new(resume_tx.clone());
        let bridge = Arc::new(ConnectBridge::with_responder(ctx, None, responder.clone()));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("chat1", "u1");
        let reply_ctx = ReplyCtx(serde_json::json!({ "chat_id": "chat1" }));

        // Segment 1: text + pause #1.
        let (tx, rx) = broadcast::channel(16);
        tx.send(AgentEvent::Token {
            content: "segment one ".to_string(),
        })
        .unwrap();
        tx.send(need_clarification_event(
            "First?",
            vec!["Approve", "Deny"],
            false,
        ))
        .unwrap();

        let render_handle = {
            let bridge = bridge.clone();
            let platform = platform.clone();
            let reply_ctx = reply_ctx.clone();
            let key = key.clone();
            tokio::spawn(async move {
                bridge
                    .render_until_settled(&key, platform, reply_ctx, "sess-1", rx)
                    .await;
            })
        };

        let first_ask = wait_for_parked_ask(&bridge, &key).await;
        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            inbound("chat1", "u1", "a1", "1"),
        )
        .await;
        wait_for_responder_calls(&responder, 1).await;

        // Segment 2 (first resume): text + pause #2.
        resume_tx
            .send(AgentEvent::Token {
                content: "segment two ".to_string(),
            })
            .unwrap();
        resume_tx
            .send(need_clarification_event(
                "Second?",
                vec!["Approve", "Deny"],
                false,
            ))
            .unwrap();

        // Wait until the SECOND ask is parked (a fresh nonce distinguishes
        // it from the first, already-resolved one).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let parked = {
                bridge
                    .chat_state
                    .lock()
                    .await
                    .get(&key)
                    .and_then(|state| state.pending_ask.clone())
            };
            if let Some(ask) = parked {
                if ask.nonce != first_ask.nonce {
                    break;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "second ask was never parked"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            inbound("chat1", "u1", "a2", "1"),
        )
        .await;
        wait_for_responder_calls(&responder, 2).await;

        // Segment 3 (second resume): text + Complete.
        resume_tx
            .send(AgentEvent::Token {
                content: "segment three".to_string(),
            })
            .unwrap();
        resume_tx
            .send(AgentEvent::Complete {
                usage: bamboo_agent_core::TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    total_tokens: 2,
                },
            })
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), render_handle)
            .await
            .expect("render task must finish")
            .unwrap();

        // Exactly ONE "⏳ Working…" status message across the whole
        // twice-paused run; the other sends are the two ask messages.
        let sent = platform.sent_texts().await;
        let status_count = sent.iter().filter(|text| text.contains('⏳')).count();
        assert_eq!(
            status_count, 1,
            "expected exactly one status bubble, got: {sent:?}"
        );
        assert_eq!(sent.len(), 3, "status + 2 asks expected, got: {sent:?}");

        // Edits continued across both resumes: the final ✅ edit carries text
        // from every segment (the buffer survived both pauses).
        let edits = platform.edits.lock().await;
        let last = edits.last().expect("expected a final edit");
        assert!(last.starts_with('✅'), "final edit not a success: {last}");
        assert!(last.contains("segment one"));
        assert!(last.contains("segment two"));
        assert!(last.contains("segment three"));
    }

    /// PR #459 review item 3: `/stop` while a run is paused on a parked ask
    /// (no live cancel token — the round already returned) must invalidate
    /// the ask, unblock the render task, and reply with the dedicated
    /// "pending question was cancelled" message (the `(None, true)` branch).
    #[tokio::test]
    async fn stop_while_paused_cancels_the_pending_question() {
        let (ctx, _dir) = test_context().await;
        let resume_tx = broadcast::channel::<AgentEvent>(16).0;
        let responder = FakeResponder::new(resume_tx.clone());
        let bridge = Arc::new(ConnectBridge::with_responder(ctx, None, responder.clone()));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("chat1", "u1");
        let reply_ctx = ReplyCtx(serde_json::json!({ "chat_id": "chat1" }));

        let (tx, rx) = broadcast::channel(16);
        tx.send(need_clarification_event(
            "Approve?",
            vec!["Approve", "Deny"],
            false,
        ))
        .unwrap();

        let render_handle = {
            let bridge = bridge.clone();
            let platform = platform.clone();
            let reply_ctx = reply_ctx.clone();
            let key = key.clone();
            tokio::spawn(async move {
                bridge
                    .render_until_settled(&key, platform, reply_ctx, "sess-1", rx)
                    .await;
            })
        };

        wait_for_parked_ask(&bridge, &key).await;

        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            inbound("chat1", "u1", "stop-1", "/stop"),
        )
        .await;

        tokio::time::timeout(Duration::from_secs(5), render_handle)
            .await
            .expect("render task must finish after /stop invalidates the ask")
            .unwrap();

        let sent = platform.sent_texts().await;
        assert!(
            sent.iter()
                .any(|text| text == "Stopped — the pending question was cancelled."),
            "expected the pending-question-cancelled reply, got: {sent:?}"
        );
        assert!(!bridge.has_pending_ask(&key).await);
        assert!(responder.calls.lock().await.is_empty());
    }

    /// The `(Some(token), true)` `/stop` branch: a parked ask AND a live
    /// cancel token (e.g. `/stop` racing the pause) — the token is
    /// cancelled, the ask is invalidated, and the generic "Stopping the
    /// current run…" reply is used.
    #[tokio::test]
    async fn stop_while_paused_with_live_token_cancels_both() {
        let (ctx, _dir) = test_context().await;
        let resume_tx = broadcast::channel::<AgentEvent>(16).0;
        let responder = FakeResponder::new(resume_tx.clone());
        let bridge = Arc::new(ConnectBridge::with_responder(ctx, None, responder.clone()));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("chat1", "u1");
        let reply_ctx = ReplyCtx(serde_json::json!({ "chat_id": "chat1" }));

        let token = CancellationToken::new();
        bridge.set_cancel_token(&key, token.clone()).await;

        let (tx, rx) = broadcast::channel(16);
        tx.send(need_clarification_event(
            "Approve?",
            vec!["Approve", "Deny"],
            false,
        ))
        .unwrap();

        let render_handle = {
            let bridge = bridge.clone();
            let platform = platform.clone();
            let reply_ctx = reply_ctx.clone();
            let key = key.clone();
            tokio::spawn(async move {
                bridge
                    .render_until_settled(&key, platform, reply_ctx, "sess-1", rx)
                    .await;
            })
        };

        wait_for_parked_ask(&bridge, &key).await;

        ConnectBridge::handle_inbound(
            bridge.clone(),
            platform.clone(),
            vec!["u1".to_string()],
            inbound("chat1", "u1", "stop-1", "/stop"),
        )
        .await;

        tokio::time::timeout(Duration::from_secs(5), render_handle)
            .await
            .expect("render task must finish after /stop")
            .unwrap();

        assert!(token.is_cancelled(), "cancel token must be cancelled");
        assert!(!bridge.has_pending_ask(&key).await);
        let sent = platform.sent_texts().await;
        assert!(
            sent.iter().any(|text| text == "Stopping the current run…"),
            "expected the stopping reply, got: {sent:?}"
        );
    }
}
