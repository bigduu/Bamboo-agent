//! Ergonomic top-level Agent SDK.
//!
//! A concise facade over the engine runtime: the caller supplies their own
//! instruction (a system-prompt fragment), a model, and an optional tool
//! policy; the engine assembles the complete system prompt around it at run
//! time. Library consumers can write:
//!
//! ```rust,ignore
//! use bamboo_agent::agent::Agent;
//!
//! let agent = Agent::builder()
//!     .model("claude-sonnet-4-6")
//!     .instruction("You help users research topics thoroughly.")
//!     .with_defaults_for_data_dir(data_dir).await?
//!     .build()?;
//!
//! let mut session = Session::new("s1", "claude-sonnet-4-6");
//! agent.run(&mut session, "investigate X").await?;
//! ```
//!
//! ## Surface
//!
//! - [`Agent`] — stable entry point wrapping the engine runtime. `run` /
//!   `run_stream` execute the agent loop with the configured instruction +
//!   tool policy + model applied to the session.
//! - [`AgentBuilder`] — concise builder (`.model()`, `.instruction()`,
//!   `.tools()`) that assembles default deps via
//!   [`AgentBuilder::with_defaults_for_data_dir`].
//! - [`ExecuteRequestBuilder`] — ergonomic builder over the multi-field
//!   [`bamboo_engine::ExecuteRequest`].
//! - [`ToolSpec`] + [`builtin_tool_names`] — tool
//!   descriptors derived from the canonical `BUILTIN_TOOL_NAMES`.
//!
//! ## Anti-fork invariant
//!
//! The SDK never reimplements the agent loop. `run` / `run_stream` funnel into
//! `bamboo_engine::Agent::execute` (the single canonical execution path).

mod builder;
mod error;
mod execute_request;
mod tools;

use std::sync::Arc;

use async_trait::async_trait;
pub use builder::AgentBuilder;
pub use execute_request::ExecuteRequestBuilder;
use tokio::sync::mpsc;

use bamboo_engine::session_app::errors::{SessionLoadError, SessionSaveError};
use bamboo_engine::session_app::repository::SessionAccess;
use bamboo_engine::session_app::respond::submit_pending_response;
use bamboo_engine::session_app::types::RespondInput;

// Re-exported so callers can name the token returned by the `*_cancellable` /
// `*_with_cancel` run helpers without depending on `tokio-util` directly.
pub use tokio_util::sync::CancellationToken;
pub use tools::{
    builtin_tool_names, builtin_tool_specs, BuiltinTool, ToolSpec, CANONICAL_TOOL_NAMES,
};

pub use error::SdkError;

// Convenience re-exports of commonly used types (single source of truth — these
// supersede the old duplicate re-export chain, resolving TD-2).
pub use bamboo_agent_core::{
    AgentError, AgentEvent, Message, MessageContent, PendingQuestion, Role, Session,
    TokenBudgetUsage, TokenUsage,
};
pub use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
pub use bamboo_engine::session_app::respond::PlanModeTransition;
pub use bamboo_engine::ExecuteRequest;
pub use bamboo_llm::LLMProvider;
pub use bamboo_mcp::manager::McpServerManager;
pub use bamboo_mcp::McpServerConfig;
pub use bamboo_storage::SessionIndexEntry;
pub use bamboo_tools::permission::{PermissionChecker, PermissionType};
pub use bamboo_tools::{BuiltinToolExecutor, BuiltinToolExecutorBuilder, ToolOutputManager};

/// Default event-channel buffer used by [`Agent::run`].
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Stable, ergonomic entry point for agent execution.
///
/// Wraps a [`bamboo_engine::Agent`] (which owns the shared runtime) plus the
/// instruction / tool policy / model configured at build time. Clone is cheap.
#[derive(Clone)]
pub struct Agent {
    inner: bamboo_engine::Agent,
    /// Instruction (system-prompt fragment) injected into the session at `run`
    /// time; the engine assembles the full prompt around it.
    system_prompt: Option<String>,
    /// Model override applied to the session at `run` time.
    model: Option<String>,
    /// Concrete session-index handle, present only when assembled via
    /// [`AgentBuilder::with_defaults_for_data_dir`]. Backs
    /// [`list_sessions`](Self::list_sessions) — the type-erased
    /// `Arc<dyn Storage>` the engine builder takes can't list.
    session_store: Option<Arc<bamboo_storage::SessionStoreV2>>,
    /// Permission checker configured via
    /// [`AgentBuilder::permission_checker`], if any. Used by
    /// [`answer`](Self::answer) to apply permission grants implied by an
    /// approved permission prompt, mirroring what the HTTP `/respond` handler
    /// does for `state.permission_checker`.
    permission_checker: Option<Arc<dyn bamboo_tools::permission::PermissionChecker>>,
}

impl Agent {
    /// Return a new ergonomic builder.
    pub fn builder() -> AgentBuilder {
        AgentBuilder::new()
    }

    /// Wrap an existing engine [`Agent`](bamboo_engine::Agent) with no extra
    /// role configuration.
    pub fn from_runtime(inner: bamboo_engine::Agent) -> Self {
        Self {
            inner,
            system_prompt: None,
            model: None,
            session_store: None,
            permission_checker: None,
        }
    }

    /// Wrap an engine [`Agent`](bamboo_engine::Agent) plus the instruction /
    /// model configuration assembled by [`AgentBuilder`].
    pub(crate) fn from_runtime_with_config(
        inner: bamboo_engine::Agent,
        system_prompt: Option<String>,
        model: Option<String>,
        session_store: Option<Arc<bamboo_storage::SessionStoreV2>>,
        permission_checker: Option<Arc<dyn bamboo_tools::permission::PermissionChecker>>,
    ) -> Self {
        Self {
            inner,
            system_prompt,
            model,
            session_store,
            permission_checker,
        }
    }

    /// Run the agent loop on `session` with the given input, draining events
    /// internally until completion.
    ///
    /// The configured instruction + model are applied to the session before
    /// execution; the tool set was fixed on the agent's executor at build time.
    ///
    /// NOTE: this variant **discards every [`AgentEvent`]** (tool calls, tokens,
    /// intermediate errors) — you only get the final `Result`. To observe the
    /// run, use [`run_stream`](Self::run_stream) instead. To cancel a blocking
    /// run from another task, use [`run_with_cancel`](Self::run_with_cancel).
    pub async fn run(
        &self,
        session: &mut Session,
        input: impl Into<String>,
    ) -> Result<(), AgentError> {
        session.add_message(Message::user(input.into()));
        self.run_session(session).await
    }

    /// Like [`run`](Self::run) but driven by a caller-owned
    /// [`CancellationToken`]: cancelling the token from another task stops the
    /// loop at the next check point. Events are still discarded (see `run`).
    pub async fn run_with_cancel(
        &self,
        session: &mut Session,
        input: impl Into<String>,
        cancel_token: CancellationToken,
    ) -> Result<(), AgentError> {
        session.add_message(Message::user(input.into()));
        self.run_session_with_cancel(session, cancel_token).await
    }

    /// Run the agent loop on `session` exactly as it stands — i.e. on a
    /// caller-provided message list — without appending a new turn. The last
    /// `User` message already in the session drives execution.
    ///
    /// This is how you pass a full conversation / message list: build the
    /// session from your messages, then run it.
    ///
    /// ```rust,ignore
    /// let mut session = Session::new("s1", "claude-sonnet-4-6");
    /// session.add_message(Message::user("hi"));
    /// session.add_message(Message::assistant("hello!", None));
    /// session.add_message(Message::user("now summarize our chat"));
    /// agent.run_session(&mut session).await?; // no extra input appended
    /// ```
    pub async fn run_session(&self, session: &mut Session) -> Result<(), AgentError> {
        self.run_session_with_cancel(session, CancellationToken::new())
            .await
    }

    /// Like [`run_session`](Self::run_session) but driven by a caller-owned
    /// [`CancellationToken`], so a blocking run can be cancelled from another
    /// task. Events are still discarded (see [`run`](Self::run)).
    pub async fn run_session_with_cancel(
        &self,
        session: &mut Session,
        cancel_token: CancellationToken,
    ) -> Result<(), AgentError> {
        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(EVENT_CHANNEL_CAPACITY);

        // Drain events so the bounded channel never blocks the loop.
        let drain = tokio::spawn(async move { while event_rx.recv().await.is_some() {} });

        let result = self.execute_internal(session, event_tx, cancel_token).await;

        // Stop draining once execution returns. Detached engine tasks (e.g.
        // background evaluations) may still hold a cloned sender, so awaiting
        // natural channel closure could hang; abort instead.
        drain.abort();
        result
    }

    /// Append `input` as a new user turn, then stream the run's
    /// [`AgentEvent`]s. The execution runs on a background task; the caller
    /// drives it by reading from the returned receiver until it closes.
    pub fn run_stream(
        &self,
        mut session: Session,
        input: impl Into<String>,
    ) -> mpsc::Receiver<AgentEvent> {
        session.add_message(Message::user(input.into()));
        self.run_stream_session(session)
    }

    /// Like [`run_stream`](Self::run_stream), but also returns a
    /// [`CancellationToken`] for the run: call `token.cancel()` to stop the loop
    /// at the next check point. Dropping the receiver does NOT cancel the run, so
    /// this is the way to interrupt a streaming agent.
    pub fn run_stream_cancellable(
        &self,
        mut session: Session,
        input: impl Into<String>,
    ) -> (mpsc::Receiver<AgentEvent>, CancellationToken) {
        session.add_message(Message::user(input.into()));
        self.run_stream_session_cancellable(session)
    }

    /// Stream the run's [`AgentEvent`]s for a caller-provided message list,
    /// without appending a new turn (the last `User` message drives execution).
    pub fn run_stream_session(&self, session: Session) -> mpsc::Receiver<AgentEvent> {
        self.run_stream_session_with_cancel(session, CancellationToken::new())
    }

    /// Like [`run_stream_session`](Self::run_stream_session) but also returns the
    /// run's [`CancellationToken`] so the caller can interrupt it.
    pub fn run_stream_session_cancellable(
        &self,
        session: Session,
    ) -> (mpsc::Receiver<AgentEvent>, CancellationToken) {
        let cancel_token = CancellationToken::new();
        let rx = self.run_stream_session_with_cancel(session, cancel_token.clone());
        (rx, cancel_token)
    }

    /// Stream a caller-provided message list under a caller-owned
    /// [`CancellationToken`]. The shared entry point the other `run_stream*`
    /// helpers funnel into.
    pub fn run_stream_session_with_cancel(
        &self,
        mut session: Session,
        cancel_token: CancellationToken,
    ) -> mpsc::Receiver<AgentEvent> {
        let (event_tx, event_rx) = mpsc::channel::<AgentEvent>(EVENT_CHANNEL_CAPACITY);
        let agent = self.clone();

        tokio::spawn(async move {
            if let Err(error) = agent
                .execute_internal(&mut session, event_tx, cancel_token)
                .await
            {
                tracing::warn!("Agent::run_stream execution failed: {error}");
            }
        });

        event_rx
    }

    /// Escape hatch for full per-request control: run a fully-specified
    /// [`ExecuteRequest`] (split fast/background/summarization models, provider
    /// handle, skill selection, custom event channel, cancellation token, …) on
    /// `session` via the single canonical engine execution path — the same path
    /// [`run`](Self::run) / [`run_stream`](Self::run_stream) funnel into.
    ///
    /// Unlike `run`/`run_stream`, this does NOT apply the builder's configured
    /// instruction or model: the caller owns the request entirely. Build it with
    /// [`ExecuteRequestBuilder`].
    ///
    /// ```rust,ignore
    /// let (tx, _rx) = tokio::sync::mpsc::channel(256);
    /// let req = ExecuteRequestBuilder::new("investigate X", tx, Default::default())
    ///     .model("claude-sonnet-4-6")
    ///     .build();
    /// agent.execute(&mut session, req).await?;
    /// ```
    pub async fn execute(
        &self,
        session: &mut Session,
        request: ExecuteRequest,
    ) -> Result<(), AgentError> {
        self.inner.execute(session, request).await
    }

    /// Shared execution path: prepare the session (system prompt + model), build
    /// the [`ExecuteRequest`], and delegate to the canonical engine execution
    /// path. Tool restriction is applied via the agent's executor (built time).
    async fn execute_internal(
        &self,
        session: &mut Session,
        event_tx: mpsc::Sender<AgentEvent>,
        cancel_token: CancellationToken,
    ) -> Result<(), AgentError> {
        // Apply the instruction as the session's leading System message and set
        // the configured model via the single authoritative pre-execution
        // mutation point. The builder's prompt is AUTHORITATIVE: it replaces a
        // leading System message, otherwise inserts one at index 0, so a
        // caller-supplied session can't silently shadow the configured
        // instruction.
        bamboo_engine::session_app::execution_prep::prepare_session_for_execution(
            session,
            self.system_prompt.as_deref(),
            self.model.as_deref(),
        );

        // The last user message in the session drives execution (the engine
        // skips echoing `initial_message`, so we surface it for logging only).
        let initial_message = session
            .messages
            .iter()
            .rev()
            .find(|m| matches!(m.role, Role::User))
            .map(|m| m.content.clone())
            .unwrap_or_default();

        // Tool restriction is handled at build time: the agent's executor is
        // built from exactly the configured tool set, so no per-run
        // `disabled_tools` filter is needed here.
        let mut builder = ExecuteRequestBuilder::new(initial_message, event_tx, cancel_token);
        if let Some(model) = self.model.clone() {
            builder = builder.model(model);
        }

        self.inner.execute(session, builder.build()).await
    }

    /// Access the shared storage backend.
    pub fn storage(&self) -> &Arc<dyn bamboo_agent_core::storage::Storage> {
        self.inner.storage()
    }

    /// Access the runtime persistence adapter.
    pub fn persistence(&self) -> &Arc<dyn bamboo_domain::RuntimeSessionPersistence> {
        self.inner.persistence()
    }

    // ------------------------------------------------------------------
    // Permission / approval + resume
    // ------------------------------------------------------------------

    /// Answer a suspended session's pending question — a
    /// `conclusion_with_options` clarification OR a permission-approval
    /// prompt (`NeedClarification` / `ToolApprovalRequested` events; both
    /// suspend via the same `session.pending_question` mechanism, per
    /// `bamboo_engine::session_app::respond`). This is the in-process
    /// equivalent of the HTTP `POST /api/v1/sessions/{id}/respond` endpoint —
    /// same use case function (`submit_pending_response`), so behavior
    /// (validation, plan-mode transitions, permission-grant extraction)
    /// matches exactly.
    ///
    /// `response` must be one of the pending question's `options` unless it
    /// `allow_custom`s a free-form answer (returns
    /// [`SdkError::InvalidResponse`] otherwise). Loads the session by ID from
    /// [`storage`](Self::storage), so the run must have already suspended
    /// (and thus persisted) before calling this — the session is NOT taken
    /// from an in-memory handle.
    ///
    /// If this `Agent` was built with
    /// [`AgentBuilder::permission_checker`](AgentBuilder::permission_checker),
    /// any permission grants implied by an approved permission prompt are
    /// applied to it automatically (mirroring what the HTTP handler does for
    /// `state.permission_checker`), so the resumed re-attempt of the gated
    /// operation passes the checker without prompting again.
    ///
    /// After answering, resume execution with [`resume`](Self::resume) /
    /// [`resume_stream`](Self::resume_stream) on the returned
    /// [`AnswerOutcome::session`] — or use
    /// [`answer_and_resume_stream`](Self::answer_and_resume_stream) to do both
    /// in one call.
    ///
    /// NOTE: unlike the HTTP server's `/respond` handler, this does NOT
    /// re-execute a gated tool call after approval — the tool never actually
    /// ran (the permission gate intercepted it before execution), so the
    /// resumed loop sees only the synthetic "Selected response: Approve" tool
    /// result rather than the operation's real output. Full re-execution
    /// parity (`PERMISSION_REEXECUTE_METADATA_KEY`) is server-adapter logic
    /// not yet ported to the SDK.
    ///
    /// NOTE: `ChildApprovalRequested` (an out-of-process sub-agent worker's
    /// gated tool, proxied over the actor protocol) is a SEPARATE mechanism
    /// from `pending_question`/`respond` and is not covered by this method.
    pub async fn answer(
        &self,
        session_id: impl Into<String>,
        response: impl Into<String>,
    ) -> Result<AnswerOutcome, SdkError> {
        let input = RespondInput {
            session_id: session_id.into(),
            user_response: response.into(),
            model: None,
            model_ref: None,
            provider: None,
            reasoning_effort: None,
        };
        let (session, response, plan_mode_transition, permission_grants) =
            submit_pending_response(self, input).await?;

        if let Some(checker) = &self.permission_checker {
            for (perm_type, resource) in &permission_grants {
                checker.grant_session_permission(*perm_type, resource.clone());
            }
        }

        Ok(AnswerOutcome {
            session,
            response,
            plan_mode_transition,
            permission_grants,
        })
    }

    /// Resume execution on `session` — i.e. continue the agent loop from its
    /// current state (e.g. the tool result [`answer`](Self::answer) just
    /// appended) WITHOUT appending a new user turn, draining events
    /// internally until completion. An alias for
    /// [`run_session`](Self::run_session) that documents intent at the call
    /// site: the engine's execution entry point always resumes from whatever
    /// is already in `session.messages` (`run`/`run_stream` are the ones that
    /// append a fresh turn first).
    pub async fn resume(&self, session: &mut Session) -> Result<(), AgentError> {
        self.run_session(session).await
    }

    /// Like [`resume`](Self::resume), but driven by a caller-owned
    /// [`CancellationToken`].
    pub async fn resume_with_cancel(
        &self,
        session: &mut Session,
        cancel_token: CancellationToken,
    ) -> Result<(), AgentError> {
        self.run_session_with_cancel(session, cancel_token).await
    }

    /// Like [`resume`](Self::resume), but streams [`AgentEvent`]s instead of
    /// draining them. An alias for
    /// [`run_stream_session`](Self::run_stream_session).
    pub fn resume_stream(&self, session: Session) -> mpsc::Receiver<AgentEvent> {
        self.run_stream_session(session)
    }

    /// Like [`resume_stream`](Self::resume_stream), but also returns a
    /// [`CancellationToken`] for the resumed run.
    pub fn resume_stream_cancellable(
        &self,
        session: Session,
    ) -> (mpsc::Receiver<AgentEvent>, CancellationToken) {
        self.run_stream_session_cancellable(session)
    }

    /// Convenience: [`answer`](Self::answer) a pending question, then
    /// immediately [`resume_stream`](Self::resume_stream) on the resulting
    /// session — the common "ask → answer → resume" flow in one call.
    pub async fn answer_and_resume_stream(
        &self,
        session_id: impl Into<String>,
        response: impl Into<String>,
    ) -> Result<mpsc::Receiver<AgentEvent>, SdkError> {
        let outcome = self.answer(session_id, response).await?;
        Ok(self.resume_stream(outcome.session))
    }

    // ------------------------------------------------------------------
    // Session ergonomics
    // ------------------------------------------------------------------

    /// List every session in the data directory, most-recently-updated first.
    ///
    /// Only available when this `Agent` was built via
    /// [`AgentBuilder::with_defaults_for_data_dir`] (which assembles the
    /// concrete session-index handle this needs) — returns
    /// [`SdkError::Unsupported`] otherwise.
    pub async fn list_sessions(&self) -> Result<Vec<bamboo_storage::SessionIndexEntry>, SdkError> {
        let store = self.session_store.as_ref().ok_or_else(|| {
            SdkError::Unsupported(
                "list_sessions requires an Agent built via with_defaults_for_data_dir".to_string(),
            )
        })?;
        Ok(store.list_index_entries().await)
    }

    /// Load a session by ID (its full message history + runtime state), or
    /// `Ok(None)` if it doesn't exist.
    pub async fn get_session(&self, session_id: &str) -> Result<Option<Session>, SdkError> {
        self.storage()
            .load_session(session_id)
            .await
            .map_err(SdkError::Io)
    }

    /// The message history for a session, or [`SdkError::SessionNotFound`] if
    /// it doesn't exist.
    pub async fn session_history(&self, session_id: &str) -> Result<Vec<Message>, SdkError> {
        self.get_session(session_id)
            .await?
            .map(|session| session.messages)
            .ok_or_else(|| SdkError::SessionNotFound(session_id.to_string()))
    }

    /// Delete a session. Returns `true` if a session was actually deleted.
    pub async fn delete_session(&self, session_id: &str) -> Result<bool, SdkError> {
        self.storage()
            .delete_session(session_id)
            .await
            .map_err(SdkError::Io)
    }
}

/// Outcome of [`Agent::answer`] — the updated session, the recorded response,
/// and any side effects `submit_pending_response` computed (plan-mode
/// transitions, permission grants implied by an approval).
#[derive(Debug)]
pub struct AnswerOutcome {
    /// The session after the pending question was answered (tool result
    /// recorded, `pending_question` cleared, resume markers set).
    pub session: Session,
    /// The response text that was recorded.
    pub response: String,
    /// Plan-mode entered/exited transition, if the answered question was
    /// `EnterPlanMode`/`ExitPlanMode`.
    pub plan_mode_transition: Option<PlanModeTransition>,
    /// `(PermissionType, resource)` grants implied by approving a permission
    /// prompt (empty unless the pending question was a permission approval).
    /// Already applied to this `Agent`'s configured
    /// [`permission_checker`](AgentBuilder::permission_checker), if any.
    pub permission_grants: Vec<(bamboo_tools::permission::PermissionType, String)>,
}

/// [`SessionAccess`] for [`Agent`], backed by [`Agent::storage`] (reads) and
/// [`Agent::persistence`] (writes) — no separate cache tier, unlike the
/// server's cache+storage+persistence-backed `SessionRepository`, since the
/// SDK has no cross-request cache to keep coherent. This is what lets
/// [`Agent::answer`] call the same `bamboo_engine::session_app::respond`
/// use-case function the HTTP `/respond` handler calls on `AppState`.
#[async_trait]
impl SessionAccess for Agent {
    async fn load_session(&self, id: &str) -> Result<Option<Session>, SessionLoadError> {
        self.storage()
            .load_session(id)
            .await
            .map_err(|e| SessionLoadError::StorageError(e.to_string()))
    }

    async fn load_or_create(&self, id: &str, model: &str) -> Result<Session, SessionLoadError> {
        match SessionAccess::load_session(self, id).await? {
            Some(session) => Ok(session),
            None => Ok(Session::new(id.to_string(), model.to_string())),
        }
    }

    async fn load_merged(&self, id: &str) -> Result<Option<Session>, SessionLoadError> {
        // No separate cache tier — storage is the single source of truth.
        SessionAccess::load_session(self, id).await
    }

    async fn save_session(&self, session: &mut Session) -> Result<(), SessionSaveError> {
        self.persistence()
            .save_runtime_session(session)
            .await
            .map_err(|e| SessionSaveError::StorageError(e.to_string()))
    }

    async fn save_and_cache(&self, session: &mut Session) -> Result<(), SessionSaveError> {
        SessionAccess::save_session(self, session).await
    }
}

#[cfg(test)]
mod approval_and_session_tests {
    use super::*;
    use bamboo_tools::permission::{
        PermissionChecker, PermissionContext, PermissionError, PermissionMode, PermissionType,
    };
    use std::sync::Mutex as StdMutex;

    /// A minimal data dir with a keyless-but-constructible provider config, so
    /// `with_defaults_for_data_dir` succeeds without any network I/O — mirrors
    /// `tests/agent_sdk.rs`'s `s_t4_3` setup.
    async fn build_test_agent(data_dir: std::path::PathBuf) -> Agent {
        let config_json = r#"{
            "provider": "anthropic",
            "providers": {
                "anthropic": { "api_key": "test-key", "model": "claude-test" }
            }
        }"#;
        std::fs::write(data_dir.join("config.json"), config_json).expect("write config");

        AgentBuilder::new()
            .model("claude-test")
            .instruction("test agent")
            .with_defaults_for_data_dir(data_dir)
            .await
            .expect("defaults should assemble")
            .build()
            .expect("agent should build")
    }

    fn seed_session_with_pending_question(
        session_id: &str,
        options: Vec<String>,
        allow_custom: bool,
    ) -> Session {
        let mut session = Session::new(session_id.to_string(), "claude-test".to_string());
        session.set_pending_question(
            "call-1".to_string(),
            "ConclusionWithOptions".to_string(),
            "Pick one".to_string(),
            options,
            allow_custom,
        );
        session
    }

    #[tokio::test]
    async fn answer_resolves_pending_question_and_persists() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let agent = build_test_agent(tmp.path().to_path_buf()).await;

        let session = seed_session_with_pending_question(
            "sess-answer-ok",
            vec!["A".to_string(), "B".to_string()],
            false,
        );
        agent
            .storage()
            .save_session(&session)
            .await
            .expect("seed session");

        let outcome = agent
            .answer("sess-answer-ok", "A")
            .await
            .expect("answer should succeed");
        assert_eq!(outcome.response, "A");
        assert!(outcome.session.pending_question.is_none());
        assert!(outcome.permission_grants.is_empty());

        // Persisted: reloading independently shows the same state.
        let reloaded = agent
            .storage()
            .load_session("sess-answer-ok")
            .await
            .expect("load")
            .expect("present");
        assert!(reloaded.pending_question.is_none());
        assert!(reloaded
            .messages
            .iter()
            .any(|m| m.tool_call_id.as_deref() == Some("call-1")
                && m.content.contains("Selected response: A")));
    }

    #[tokio::test]
    async fn answer_rejects_response_outside_fixed_options() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let agent = build_test_agent(tmp.path().to_path_buf()).await;

        let session = seed_session_with_pending_question(
            "sess-answer-invalid",
            vec!["A".to_string(), "B".to_string()],
            false,
        );
        agent
            .storage()
            .save_session(&session)
            .await
            .expect("seed session");

        let error = agent
            .answer("sess-answer-invalid", "not-an-option")
            .await
            .expect_err("response outside options should be rejected");
        assert!(matches!(error, SdkError::InvalidResponse(_)));
    }

    #[tokio::test]
    async fn answer_errors_when_no_pending_question() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let agent = build_test_agent(tmp.path().to_path_buf()).await;

        let session = Session::new("sess-no-pending".to_string(), "claude-test".to_string());
        agent
            .storage()
            .save_session(&session)
            .await
            .expect("seed session");

        let error = agent
            .answer("sess-no-pending", "anything")
            .await
            .expect_err("no pending question should error");
        assert!(matches!(error, SdkError::NoPendingQuestion));
    }

    #[tokio::test]
    async fn answer_errors_when_session_missing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let agent = build_test_agent(tmp.path().to_path_buf()).await;

        let error = agent
            .answer("does-not-exist", "anything")
            .await
            .expect_err("missing session should error");
        assert!(matches!(error, SdkError::SessionNotFound(id) if id == "does-not-exist"));
    }

    #[tokio::test]
    async fn session_ergonomics_list_get_history_delete_round_trip() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let agent = build_test_agent(tmp.path().to_path_buf()).await;

        let mut session_a = Session::new("sess-a".to_string(), "claude-test".to_string());
        session_a.add_message(Message::user("hello"));
        agent
            .storage()
            .save_session(&session_a)
            .await
            .expect("save a");

        let session_b = Session::new("sess-b".to_string(), "claude-test".to_string());
        agent
            .storage()
            .save_session(&session_b)
            .await
            .expect("save b");

        let listed = agent.list_sessions().await.expect("list_sessions");
        let ids: Vec<&str> = listed.iter().map(|entry| entry.id.as_str()).collect();
        assert!(ids.contains(&"sess-a"));
        assert!(ids.contains(&"sess-b"));

        let history = agent
            .session_history("sess-a")
            .await
            .expect("session_history");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].content, "hello");

        let missing_history = agent.session_history("does-not-exist").await;
        assert!(matches!(
            missing_history,
            Err(SdkError::SessionNotFound(id)) if id == "does-not-exist"
        ));

        let deleted = agent.delete_session("sess-a").await.expect("delete");
        assert!(deleted);
        assert!(agent
            .get_session("sess-a")
            .await
            .expect("get_session")
            .is_none());
    }

    /// A stub `PermissionChecker` that only records `grant_session_permission`
    /// calls, so the test can assert `Agent::answer` applies the permission
    /// grants `submit_pending_response` extracts from an approved permission
    /// prompt — mirroring what the HTTP `/respond` handler does explicitly for
    /// `state.permission_checker`.
    #[derive(Default)]
    struct RecordingPermissionChecker {
        grants: StdMutex<Vec<(PermissionType, String)>>,
    }

    #[async_trait]
    impl PermissionChecker for RecordingPermissionChecker {
        async fn needs_confirmation(&self, _perm_type: PermissionType, _resource: &str) -> bool {
            false
        }

        async fn request_confirmation(
            &self,
            _ctx: PermissionContext,
        ) -> Result<bool, PermissionError> {
            Ok(true)
        }

        fn grant_session_permission(&self, perm_type: PermissionType, resource: String) {
            self.grants.lock().unwrap().push((perm_type, resource));
        }

        fn set_permission_mode(&self, _mode: PermissionMode) {}
    }

    #[tokio::test]
    async fn answer_applies_permission_grants_to_configured_checker() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let config_json = r#"{
            "provider": "anthropic",
            "providers": {
                "anthropic": { "api_key": "test-key", "model": "claude-test" }
            }
        }"#;
        std::fs::write(tmp.path().join("config.json"), config_json).expect("write config");

        let checker = Arc::new(RecordingPermissionChecker::default());
        let agent = AgentBuilder::new()
            .model("claude-test")
            .permission_checker(checker.clone())
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .expect("defaults should assemble")
            .build()
            .expect("agent should build");

        // Seed a session suspended on an approved permission prompt: the
        // synthesized `awaiting_permission_approval` tool-result payload
        // `check_permissions_for` writes before pausing (see
        // `bamboo_tools::executor` / `session_app::respond`).
        let mut session = Session::new("sess-permission".to_string(), "claude-test".to_string());
        session.set_pending_question(
            "call-perm-1".to_string(),
            "Write".to_string(),
            "Permission required".to_string(),
            vec!["Approve".to_string(), "Deny".to_string()],
            false,
        );
        session.add_message(Message::tool_result(
            "call-perm-1",
            serde_json::json!({
                "status": "awaiting_permission_approval",
                "question": "Permission required",
                "permission_type": "write_file",
                "resource": "/tmp/example.txt",
                "options": ["Approve", "Deny"],
                "allow_custom": false,
            })
            .to_string(),
        ));
        agent
            .storage()
            .save_session(&session)
            .await
            .expect("seed session");

        let outcome = agent
            .answer("sess-permission", "Approve")
            .await
            .expect("answer should succeed");
        assert_eq!(
            outcome.permission_grants,
            vec![(PermissionType::WriteFile, "/tmp/example.txt".to_string())]
        );

        let recorded = checker.grants.lock().unwrap();
        assert_eq!(
            *recorded,
            vec![(PermissionType::WriteFile, "/tmp/example.txt".to_string())]
        );
    }

    #[tokio::test]
    async fn list_sessions_unsupported_without_defaults_for_data_dir() {
        // An Agent wrapped directly via `from_runtime` (no session_store
        // handle) should report `Unsupported`, not panic.
        // Building one still needs a real engine Agent, so reuse the defaults
        // path and drop the handle to simulate a manually-injected Agent.
        let tmp = tempfile::tempdir().expect("tempdir");
        let agent = build_test_agent(tmp.path().to_path_buf()).await;
        let bare = Agent::from_runtime_with_config(
            // Reuse the inner engine agent — only the SDK-level session_store
            // handle is what `list_sessions` checks.
            agent.inner.clone(),
            None,
            None,
            None,
            None,
        );
        let result = bare.list_sessions().await;
        assert!(matches!(result, Err(SdkError::Unsupported(_))));
    }
}
