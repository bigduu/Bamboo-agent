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
use bamboo_engine::session_app::respond::{
    submit_pending_response, PERMISSION_REEXECUTE_METADATA_KEY,
};
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
    AgentError, AgentEvent, AgentHook, Message, MessageContent, PendingQuestion, Role, Session,
    TokenBudgetUsage, TokenUsage,
};
pub use bamboo_domain::{
    AgentHookPoint, HookPayload, HookResult, HookToolOutcome, TaskItem, TaskItemStatus, TaskList,
};
pub use bamboo_engine::session_app::respond::PlanModeTransition;
pub use bamboo_engine::{ExecuteRequest, HookRunner};
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
        // If `answer()` just approved a gated tool call, `session.metadata` carries
        // the re-execution marker `submit_pending_response` set — the gated tool
        // never actually ran (the permission gate intercepted it before
        // execution), so re-run it now for real and write the genuine output back
        // before the loop resumes. No-op when the marker is absent (the common,
        // non-permission path), so this is safe to run unconditionally on every
        // entry into the loop, not just `resume`. See
        // `reexecute_approved_tool_if_pending` for the full rationale.
        self.reexecute_approved_tool_if_pending(session, &event_tx)
            .await;

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

    /// Port of `bamboo-server`'s `resume_adapter.rs` re-execution logic: after
    /// [`answer`](Self::answer) approves a permission prompt,
    /// `submit_pending_response` stamps `session.metadata` with
    /// [`PERMISSION_REEXECUTE_METADATA_KEY`] (the approved tool call's id) — the
    /// gated tool was intercepted BEFORE it ran, so its recorded result is only
    /// the synthetic "Selected response: Approve" placeholder. This re-runs the
    /// original tool call for real, against the SAME executor the loop itself
    /// uses ([`bamboo_engine::Agent::default_tools`]), and overwrites the
    /// placeholder tool-result message with the genuine output — so the resumed
    /// loop sees what the operation actually did instead of inferring it.
    ///
    /// Emits the same `ToolStart`/`ToolComplete` (or `ToolError`) lifecycle
    /// events onto `event_tx` that a normal dispatch would, so a streaming
    /// consumer sees the re-run tool card update exactly like the HTTP surface
    /// does. Best-effort persists the updated session via
    /// [`persistence`](Self::persistence) so the real output survives even if the
    /// process stops before the loop's own next save — logged, not propagated,
    /// since the loop's subsequent save will also capture it.
    ///
    /// No-op (returns immediately) when the marker is absent, so it is safe to
    /// call unconditionally at the top of every execution, not just resumes.
    async fn reexecute_approved_tool_if_pending(
        &self,
        session: &mut Session,
        event_tx: &mpsc::Sender<AgentEvent>,
    ) {
        let Some(tool_call_id) = session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY) else {
            return;
        };

        let Some(tool_call) = find_pending_tool_call(session, &tool_call_id) else {
            tracing::warn!(
                session_id = %session.id,
                tool_call_id = %tool_call_id,
                "Permission re-exec marker set but tool call not found in history"
            );
            return;
        };

        let executor = self.inner.default_tools();
        let tool_name = tool_call.function.name.clone();
        let is_mutating = bamboo_tools::orchestrator::classify_tool(&tool_name)
            == bamboo_tools::orchestrator::ToolMutability::Mutating;

        // Frame the re-run with the same lifecycle events the normal loop emits
        // (via ToolEmitter) so a streaming consumer's tool card updates
        // (running -> finished) and ToolComplete carries the REAL output — raw
        // `execute_with_context` only streams tool tokens, not lifecycle.
        let mut emitter = bamboo_tools::ToolEmitter::new(&tool_call.id, &tool_name, is_mutating);
        emitter.set_auto_approved(true);
        let _ = event_tx
            .send(emitter.begin().clone().into_agent_event())
            .await;

        let exec_result = {
            let ctx = bamboo_agent_core::tools::ToolExecutionContext {
                session_id: Some(session.id.as_str()),
                tool_call_id: tool_call_id.as_str(),
                event_tx: Some(event_tx),
                available_tool_schemas: None,
                bypass_permissions: false,
                can_async_resume: false,
                bash_completion_sink: None,
                pre_parsed_args: None,
            };
            executor.execute_with_context(&tool_call, ctx).await
        };

        let (content, success) = match exec_result {
            Ok(tool_result) => {
                let _ = event_tx
                    .send(
                        emitter
                            .finish(Some("Re-executed after approval".to_string()))
                            .clone()
                            .into_agent_event(),
                    )
                    .await;
                let _ = event_tx
                    .send(AgentEvent::ToolComplete {
                        tool_call_id: tool_call.id.clone(),
                        result: tool_result.clone(),
                    })
                    .await;
                (tool_result.result, tool_result.success)
            }
            Err(error) => {
                let message = format!("Tool re-execution after approval failed: {error}");
                let _ = event_tx
                    .send(emitter.error(message.clone()).clone().into_agent_event())
                    .await;
                (message, false)
            }
        };

        tracing::info!(
            session_id = %session.id,
            tool_name = %tool_name,
            tool_call_id = %tool_call_id,
            success,
            "Re-executed approved tool after permission grant"
        );
        apply_tool_result(session, &tool_call_id, content, success);

        if let Err(error) = self.persistence().save_runtime_session(session).await {
            tracing::warn!(
                session_id = %session.id,
                %error,
                "Failed to persist session after tool re-execution (loop's own save will retry)"
            );
        }
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
    /// Like the HTTP server's `/respond` handler, approving a gated tool call
    /// here also re-executes it for real once the run resumes: `answer` (via
    /// `submit_pending_response`) stamps the returned session's metadata with
    /// `PERMISSION_REEXECUTE_METADATA_KEY`, and the next call into
    /// [`resume`](Self::resume)/[`resume_stream`](Self::resume_stream)/`run*`
    /// re-runs the originally-gated tool call against the agent's tool executor
    /// and overwrites the synthetic "Selected response: Approve" placeholder
    /// with the operation's genuine output before the loop continues — see
    /// `reexecute_approved_tool_if_pending`.
    ///
    /// NOTE: `ChildApprovalRequested` (an out-of-process sub-agent worker's
    /// gated tool, proxied over the actor protocol) is a SEPARATE mechanism
    /// from `pending_question`/`respond` and is not covered by this method —
    /// use [`answer_child_approval`](Self::answer_child_approval) instead.
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
            if let Some(request_id) = session.metadata.get(PERMISSION_REEXECUTE_METADATA_KEY) {
                for (perm_type, resource) in &permission_grants {
                    checker.grant_once(&session.id, request_id, *perm_type, resource.clone());
                }
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

    /// Answer an out-of-process child sub-agent's gated-tool approval request
    /// (an [`AgentEvent::ChildApprovalRequested`] surfaced on a run's event
    /// stream — e.g. from [`run_stream`](Self::run_stream)/
    /// [`resume_stream`](Self::resume_stream)) — the in-process equivalent of
    /// the HTTP `POST /api/v1/child-approval/{child_session_id}` endpoint (see
    /// `bamboo_server::handlers::agent::child_approval`).
    ///
    /// This is a SEPARATE mechanism from [`answer`](Self::answer)/
    /// [`pending_question`](Session::pending_question): a child sub-agent
    /// worker running out-of-process (over the actor protocol, e.g. a broker
    /// worker) that hits a gated tool escalates the approval request UP to this
    /// process rather than suspending its own `pending_question`, and the
    /// engine tracks it in a process-global pending-approval registry
    /// (`bamboo_engine::external_agents::live`) keyed by `(child_session_id,
    /// request_id)` — both taken verbatim from the surfaced event. There is no
    /// session to load/save here: this only delivers the decision over the
    /// child's live connection (or fails it closed if the child already
    /// disconnected or the request already resolved/timed out).
    ///
    /// Returns `true` if the decision was delivered to a genuinely-pending
    /// request, `false` if `request_id` is unknown, was already answered, timed
    /// out, or the child is no longer live — mirroring the HTTP handler's
    /// 200-vs-404 distinction. A `false` result does not need cleanup on the
    /// caller's part: the request is either already resolved or has moved on.
    ///
    /// # Boundary
    ///
    /// This method only covers the TOP-orchestrator, human-in-the-loop leg of
    /// child approval (the leg that surfaces `ChildApprovalRequested` at all).
    /// It requires the agent to actually be driving an out-of-process child —
    /// i.e. the caller has wired the engine's `external_agents` actor transport
    /// (broker/worker) — which `AgentBuilder::with_defaults_for_data_dir` does
    /// NOT assemble; that machinery is a separate, opt-in subsystem. Calling
    /// this without a live child matching `child_session_id`/`request_id`
    /// simply returns `false` (no panic, no error) — the same as an unmatched
    /// HTTP POST.
    pub fn answer_child_approval(
        &self,
        child_session_id: impl AsRef<str>,
        request_id: impl AsRef<str>,
        approved: bool,
    ) -> bool {
        bamboo_engine::external_agents::live::deliver_approval_checked(
            None,
            child_session_id.as_ref(),
            request_id.as_ref(),
            approved,
        )
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

/// Find the original tool call (with its arguments) by id in the session
/// history. Mirrors `bamboo-server`'s `resume_adapter::find_pending_tool_call`.
fn find_pending_tool_call(
    session: &Session,
    tool_call_id: &str,
) -> Option<bamboo_agent_core::tools::ToolCall> {
    session.messages.iter().find_map(|message| {
        message
            .tool_calls
            .as_ref()
            .and_then(|calls| calls.iter().find(|call| call.id == tool_call_id).cloned())
    })
}

/// Overwrite the tool-result message for `tool_call_id` with the real tool
/// output. Mirrors `bamboo-server`'s `resume_adapter::apply_tool_result`.
fn apply_tool_result(session: &mut Session, tool_call_id: &str, content: String, success: bool) {
    for message in &mut session.messages {
        if message.tool_call_id.as_deref() == Some(tool_call_id) {
            message.content = content;
            message.tool_success = Some(success);
            return;
        }
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

    /// A stub `PermissionChecker` that records one-shot grants
    /// calls, so the test can assert `Agent::answer` applies the permission
    /// grants `submit_pending_response` extracts from an approved permission
    /// prompt — mirroring what the HTTP `/respond` handler does explicitly for
    /// `state.permission_checker`.
    #[derive(Default)]
    struct RecordingPermissionChecker {
        grants: StdMutex<Vec<(String, String, PermissionType, String)>>,
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
            panic!("legacy unscoped grant used: {perm_type:?} {resource}");
        }

        fn grant_once(
            &self,
            session_id: &str,
            request_id: &str,
            perm_type: PermissionType,
            resource: String,
        ) {
            self.grants.lock().unwrap().push((
                session_id.to_string(),
                request_id.to_string(),
                perm_type,
                resource,
            ));
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
            vec![(
                "sess-permission".to_string(),
                "call-perm-1".to_string(),
                PermissionType::WriteFile,
                "/tmp/example.txt".to_string()
            )]
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

#[cfg(test)]
mod reexecute_and_child_approval_tests {
    use super::*;
    use bamboo_agent_core::tools::{FunctionCall, Tool, ToolCall, ToolCtx, ToolError, ToolOutcome};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A tool whose real output is trivially distinguishable from the
    /// synthetic "Selected response: Approve" placeholder `submit_pending_response`
    /// writes, and which counts invocations — so tests can assert it actually ran
    /// (not merely that the metadata marker was consumed).
    struct RealOutputTool {
        calls: AtomicUsize,
    }

    impl RealOutputTool {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl Tool for RealOutputTool {
        fn name(&self) -> &str {
            "real_output_tool"
        }

        fn description(&self) -> &str {
            "test-only tool that returns a distinctive real result"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            _ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutcome::Completed(
                bamboo_agent_core::tools::ToolResult::text(true, format!("REAL TOOL OUTPUT #{n}")),
            ))
        }
    }

    async fn build_test_agent_with_tool(
        data_dir: std::path::PathBuf,
        tool: Arc<RealOutputTool>,
    ) -> Agent {
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
            .tool_shared(tool)
            .with_defaults_for_data_dir(data_dir)
            .await
            .expect("defaults should assemble")
            .build()
            .expect("agent should build")
    }

    /// Seed a session suspended on an approved permission prompt for
    /// `real_output_tool`, matching the shape `check_permissions_for` writes
    /// before pausing: an assistant message carrying the gated tool call, plus
    /// the synthesized `awaiting_permission_approval` tool-result payload.
    fn seed_gated_tool_session(session_id: &str, tool_call_id: &str) -> Session {
        let mut session = Session::new(session_id.to_string(), "claude-test".to_string());
        session.add_message(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: tool_call_id.to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "real_output_tool".to_string(),
                    arguments: "{}".to_string(),
                },
            }]),
        ));
        session.set_pending_question(
            tool_call_id.to_string(),
            "real_output_tool".to_string(),
            "Permission required".to_string(),
            vec!["Approve".to_string(), "Deny".to_string()],
            false,
        );
        session.add_message(Message::tool_result(
            tool_call_id,
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
        session
    }

    #[tokio::test]
    async fn approve_marks_session_for_reexecution() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let tool = Arc::new(RealOutputTool::new());
        let agent = build_test_agent_with_tool(tmp.path().to_path_buf(), tool).await;

        let session = seed_gated_tool_session("sess-mark", "call-mark-1");
        agent
            .storage()
            .save_session(&session)
            .await
            .expect("seed session");

        let outcome = agent
            .answer("sess-mark", "Approve")
            .await
            .expect("answer should succeed");

        assert_eq!(
            outcome
                .session
                .metadata
                .get(PERMISSION_REEXECUTE_METADATA_KEY)
                .map(String::as_str),
            Some("call-mark-1"),
            "approving a permission prompt must stamp the re-exec marker"
        );
    }

    #[tokio::test]
    async fn deny_does_not_mark_session_for_reexecution() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let tool = Arc::new(RealOutputTool::new());
        let agent = build_test_agent_with_tool(tmp.path().to_path_buf(), tool).await;

        let session = seed_gated_tool_session("sess-deny", "call-deny-1");
        agent
            .storage()
            .save_session(&session)
            .await
            .expect("seed session");

        let outcome = agent
            .answer("sess-deny", "Deny")
            .await
            .expect("answer should succeed");

        assert!(outcome.permission_grants.is_empty());
        assert!(!outcome
            .session
            .metadata
            .contains_key(PERMISSION_REEXECUTE_METADATA_KEY));
        // The tool result stays the synthetic "Selected response: Deny" — the
        // gated tool must NOT have run.
        let tool_message = outcome
            .session
            .messages
            .iter()
            .find(|m| m.tool_call_id.as_deref() == Some("call-deny-1"))
            .expect("tool result message present");
        assert_eq!(tool_message.content, "Selected response: Deny");
    }

    #[tokio::test]
    async fn approve_then_reexecute_runs_real_tool_and_overwrites_placeholder() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let tool = Arc::new(RealOutputTool::new());
        let agent = build_test_agent_with_tool(tmp.path().to_path_buf(), tool.clone()).await;

        let session = seed_gated_tool_session("sess-reexec", "call-reexec-1");
        agent
            .storage()
            .save_session(&session)
            .await
            .expect("seed session");

        let outcome = agent
            .answer("sess-reexec", "Approve")
            .await
            .expect("answer should succeed");
        let mut session = outcome.session;

        // Sanity: before re-execution the tool result is still the synthetic
        // placeholder, and the real tool has not run yet.
        let placeholder = session
            .messages
            .iter()
            .find(|m| m.tool_call_id.as_deref() == Some("call-reexec-1"))
            .expect("tool result message present");
        assert_eq!(placeholder.content, "Selected response: Approve");
        assert_eq!(tool.calls.load(Ordering::SeqCst), 0);

        // Drive the same re-execution step `resume`/`run*` apply internally
        // (via `execute_internal`) at the top of the next execution.
        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(16);
        agent
            .reexecute_approved_tool_if_pending(&mut session, &event_tx)
            .await;
        drop(event_tx);

        // The gated tool ran exactly once, and the placeholder is replaced with
        // its real output.
        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
        let real_result = session
            .messages
            .iter()
            .find(|m| m.tool_call_id.as_deref() == Some("call-reexec-1"))
            .expect("tool result message present");
        assert_eq!(real_result.content, "REAL TOOL OUTPUT #0");
        assert_eq!(real_result.tool_success, Some(true));
        assert!(
            !session
                .metadata
                .contains_key(PERMISSION_REEXECUTE_METADATA_KEY),
            "the marker must be consumed (removed) after re-execution"
        );

        // Lifecycle events (ToolStart-equivalent begin + ToolComplete) were
        // emitted, matching what a normal dispatch would stream.
        let mut saw_tool_complete = false;
        while let Ok(event) = event_rx.try_recv() {
            if let AgentEvent::ToolComplete { tool_call_id, .. } = event {
                assert_eq!(tool_call_id, "call-reexec-1");
                saw_tool_complete = true;
            }
        }
        assert!(saw_tool_complete, "expected a ToolComplete event");

        // Persisted too (best-effort save inside the helper).
        let reloaded = agent
            .storage()
            .load_session("sess-reexec")
            .await
            .expect("load")
            .expect("present");
        let reloaded_result = reloaded
            .messages
            .iter()
            .find(|m| m.tool_call_id.as_deref() == Some("call-reexec-1"))
            .expect("tool result message present");
        assert_eq!(reloaded_result.content, "REAL TOOL OUTPUT #0");
    }

    #[tokio::test]
    async fn reexecute_is_noop_without_pending_marker() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let tool = Arc::new(RealOutputTool::new());
        let agent = build_test_agent_with_tool(tmp.path().to_path_buf(), tool.clone()).await;

        let mut session = Session::new("sess-noop".to_string(), "claude-test".to_string());
        session.add_message(Message::user("hi"));

        let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
        agent
            .reexecute_approved_tool_if_pending(&mut session, &event_tx)
            .await;

        assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
        assert_eq!(session.messages.len(), 1);
    }

    #[tokio::test]
    async fn reexecute_warns_and_clears_marker_when_tool_call_missing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let tool = Arc::new(RealOutputTool::new());
        let agent = build_test_agent_with_tool(tmp.path().to_path_buf(), tool.clone()).await;

        let mut session = Session::new("sess-missing".to_string(), "claude-test".to_string());
        // Marker set, but no matching tool_calls entry exists in history.
        session.metadata.insert(
            PERMISSION_REEXECUTE_METADATA_KEY.to_string(),
            "ghost-call".to_string(),
        );

        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(16);
        agent
            .reexecute_approved_tool_if_pending(&mut session, &event_tx)
            .await;
        drop(event_tx);

        assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
        assert!(event_rx.try_recv().is_err(), "no events should be emitted");
        assert!(
            !session
                .metadata
                .contains_key(PERMISSION_REEXECUTE_METADATA_KEY),
            "the marker is removed even when the tool call can't be found, so a \
             missing/pruned call can't wedge every future execution"
        );
    }

    #[tokio::test]
    async fn answer_child_approval_delivers_only_genuinely_pending_requests() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let tool = Arc::new(RealOutputTool::new());
        let agent = build_test_agent_with_tool(tmp.path().to_path_buf(), tool).await;

        // Unregistered pair: rejected, mirroring an unmatched HTTP POST.
        assert!(!agent.answer_child_approval("child-x", "req-unknown", true));

        // A live child connection (as the actor adapter registers for the
        // duration of a running child) plus the pending-approval marker it
        // records just before surfacing `ChildApprovalRequested` — both are
        // process-global engine state, set up here exactly as
        // `external_agents::actor_adapter::drive` would.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let _live_guard = bamboo_engine::external_agents::live::register("child-x", tx, 0, None);
        let (approval_event_tx, _approval_event_rx) = tokio::sync::mpsc::channel(4);
        bamboo_engine::external_agents::live::register_pending_approval_observed(
            None,
            "parent-x",
            "child-x",
            0,
            "req-1",
            "shell",
            "execute",
            "cargo test",
            approval_event_tx,
        );

        assert!(agent.answer_child_approval("child-x", "req-1", true));
        match rx.try_recv() {
            Ok(bamboo_subagent::proto::ParentFrame::ApprovalReply { id, approved }) => {
                assert_eq!(id, "req-1");
                assert!(approved);
            }
            other => panic!("expected an ApprovalReply frame, got {other:?}"),
        }

        // One-shot: a replay of the same request_id is rejected.
        assert!(!agent.answer_child_approval("child-x", "req-1", true));
    }
}
