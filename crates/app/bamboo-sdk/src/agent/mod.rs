//! Ergonomic top-level Agent SDK.
//!
//! A concise facade over the engine runtime: the caller supplies their own
//! instruction (a system-prompt fragment), a model, and an optional tool
//! policy; the engine assembles the complete system prompt around it at run
//! time. Library consumers can write:
//!
//! ```rust,no_run
//! # use std::path::PathBuf;
//! use bamboo_sdk::agent::{Agent, Session};
//! # async fn example(data_dir: PathBuf) -> Result<(), bamboo_sdk::agent::SdkError> {
//!
//! let agent = Agent::builder()
//!     .model("claude-sonnet-4-6")
//!     .instruction("You help users research topics thoroughly.")
//!     .with_defaults_for_data_dir(data_dir).await?
//!     .build()?;
//!
//! let mut session = Session::new("s1", "claude-sonnet-4-6");
//! agent.run(&mut session, "investigate X").await?;
//! # Ok(())
//! # }
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

use bamboo_engine::session_app::approval_replay::{
    refresh_approval_replay_posture, ApprovalReplayDecision,
};
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
    AgentHookPoint, HookPayload, HookResult, HookToolOutcome, SessionActivationDisposition,
    SessionActivationError, SessionActivationPolicy, SessionActivationPort, SessionChildOutcome,
    SessionInboxBacklog, SessionInboxClaim, SessionInboxError, SessionInboxLimits,
    SessionInboxPort, SessionInboxReceipt, SessionMessageBody, SessionMessageContent,
    SessionMessageEnvelope, SessionMessageId, SessionMessageKind, SessionMessageSource,
    SessionProviderMessage, SessionRuntimeInstruction, TaskItem, TaskItemStatus, TaskList,
};
pub use bamboo_engine::session_app::respond::PlanModeTransition;
pub use bamboo_engine::{
    Agent as RuntimeAgent, AgentBuilder as RuntimeAgentBuilder, ExecuteRequest, HookRunner,
    LifecycleHookEvent, LifecycleHookTestOutput, LifecycleScriptRunner, ScriptHook,
    SessionActivationLaunch, SessionActivationReserveOutcome, SessionActivationRouter,
    SessionActivationSpawner, SessionMessagingMetrics, SessionMessagingMetricsSnapshot,
    SessionMessenger, SessionMessengerAdmission, SessionMessengerError, SessionMessengerReceipt,
    SessionRunRegistration, SessionRunRegistrationError, ShellCommandHook, ShellHookEvent,
};
pub use bamboo_llm::LLMProvider;
pub use bamboo_mcp::manager::McpServerManager;
pub use bamboo_mcp::{McpServerConfig, StdioConfig, TransportConfig};
pub use bamboo_storage::{FileSessionInbox, SessionIndexEntry};
pub use bamboo_tools::permission::{PermissionChecker, PermissionMode, PermissionType};
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
    /// Effective configured model used only when creating a new session. This
    /// must not alter an existing caller-supplied session during execution.
    session_model: Option<String>,
    /// Default first-class Project membership for newly-created/unassigned sessions.
    project_id: Option<bamboo_domain::ProjectId>,
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
    /// Configured process posture retained independently from the checker.
    /// In particular, the SDK's explicit legacy Bypass policy deliberately has
    /// no checker, while approval replay still needs to derive exact flags.
    permission_mode: PermissionMode,
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
            session_model: None,
            project_id: None,
            session_store: None,
            permission_checker: None,
            permission_mode: PermissionMode::Default,
        }
    }

    /// Wrap an engine [`Agent`](bamboo_engine::Agent) plus the instruction /
    /// model configuration assembled by [`AgentBuilder`].
    pub(crate) fn from_runtime_with_config(
        inner: bamboo_engine::Agent,
        system_prompt: Option<String>,
        model: Option<String>,
        session_model: Option<String>,
        project_id: Option<bamboo_domain::ProjectId>,
        session_store: Option<Arc<bamboo_storage::SessionStoreV2>>,
        permission_checker: Option<Arc<dyn bamboo_tools::permission::PermissionChecker>>,
        permission_mode: PermissionMode,
    ) -> Self {
        Self {
            inner,
            system_prompt,
            model,
            session_model,
            project_id,
            session_store,
            permission_checker,
            permission_mode,
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
    /// ```rust,no_run
    /// # use bamboo_sdk::agent::{Agent, Message, Session};
    /// # async fn example(agent: &Agent) -> Result<(), bamboo_sdk::agent::AgentError> {
    /// let mut session = Session::new("s1", "claude-sonnet-4-6");
    /// session.add_message(Message::user("hi"));
    /// session.add_message(Message::assistant("hello!", None));
    /// session.add_message(Message::user("now summarize our chat"));
    /// agent.run_session(&mut session).await?; // no extra input appended
    /// # Ok(())
    /// # }
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
            // Keep one sender alive through the direct-execution terminal
            // handshake. The engine consumes/drops the request sender when the
            // provider loop returns, but the public stream must not close before
            // finalization has handed any terminal-window inbox generation to
            // its successor.
            let execution_tx = event_tx.clone();
            if let Err(error) = agent
                .execute_internal(&mut session, execution_tx, cancel_token)
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
    /// ```rust,no_run
    /// # use bamboo_sdk::agent::{Agent, CancellationToken, ExecuteRequestBuilder, Session};
    /// # async fn example(agent: &Agent, session: &mut Session) -> Result<(), bamboo_sdk::agent::AgentError> {
    /// let (tx, _rx) = tokio::sync::mpsc::channel(256);
    /// let req = ExecuteRequestBuilder::new("investigate X", tx, CancellationToken::new())
    ///     .model("claude-sonnet-4-6")
    ///     .build();
    /// agent.execute(session, req).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(
        &self,
        session: &mut Session,
        request: ExecuteRequest,
    ) -> Result<(), AgentError> {
        self.inner.execute_direct(session, request).await
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
        // Own the logical session before any pre-execution mutation or approved
        // tool replay. Two cloned SDK Session values must collide before either
        // can duplicate a mutating side effect.
        let direct_lease = self.inner.begin_direct_execution(&session.id).await?;
        if session.project_id_meta().is_none() {
            if let Some(project_id) = self.project_id.as_ref() {
                session.set_project_id_meta(project_id.to_string());
            }
        }

        // Complete the external Project/Workspace handoff before replaying an
        // approved mutating tool. The replay executor reads the runtime
        // workspace registry, so deferring this until the loop's first round
        // would execute against stale process state. Assigned sessions fail
        // closed here when this runtime has no Project resolver; the pending
        // replay marker remains intact for a correctly configured retry.
        self.inner
            .prepare_external_session_for_execution(session)
            .await?;

        // If `answer()` just approved a gated tool call, `session.metadata` carries
        // the re-execution marker `submit_pending_response` set — the gated tool
        // never actually ran (the permission gate intercepted it before
        // execution), so re-run it now for real and write the genuine output back
        // before the loop resumes. No-op when the marker is absent (the common,
        // non-permission path), so this is safe to run unconditionally on every
        // entry into the loop, not just `resume`. See
        // `reexecute_approved_tool_if_pending` for the full rationale.
        self.reexecute_approved_tool_if_pending(session, &event_tx)
            .await?;

        // Apply the instruction as the session's leading System message, set
        // the configured model, and refresh the typed prompt snapshot via the
        // single authoritative pre-execution mutation point. This intentionally
        // follows replay: a failed replay remains observable without replacing
        // caller System bytes, while a successful handoff always reaches the
        // provider with one clean configured System message.
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

        self.inner
            .execute_direct_registered(session, builder.build(), direct_lease)
            .await
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
    ) -> Result<(), AgentError> {
        let Some(tool_call_id) = session
            .metadata
            .get(PERMISSION_REEXECUTE_METADATA_KEY)
            .cloned()
        else {
            return Ok(());
        };

        let Some(tool_call) = find_pending_tool_call(session, &tool_call_id) else {
            session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY);
            tracing::warn!(
                session_id = %session.id,
                tool_call_id = %tool_call_id,
                "Permission re-exec marker set but tool call not found in history"
            );
            return Ok(());
        };

        let tool_name = tool_call.function.name.clone();
        let decision = refresh_approval_replay_posture(
            self.storage().as_ref(),
            session,
            self.permission_mode,
            &tool_name,
        )
        .await?;

        let flags = match decision {
            ApprovalReplayDecision::Execute(flags) => flags,
            ApprovalReplayDecision::BlockedByPlan(_) => {
                session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY);
                apply_tool_result(
                    session,
                    &tool_call_id,
                    format!(
                        "Plan mode blocked approved mutating tool '{tool_name}'; the stale approval was not executed"
                    ),
                    false,
                );
                if let Err(error) = self.persistence().save_runtime_session(session).await {
                    tracing::warn!(
                        session_id = %session.id,
                        %error,
                        "Failed to persist Plan-blocked approval replay (loop's own save will retry)"
                    );
                }
                return Ok(());
            }
        };
        session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY);

        let executor = self.inner.default_tools();
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
                root_session_id: Some(if session.root_session_id.trim().is_empty() {
                    session.id.as_str()
                } else {
                    session.root_session_id.as_str()
                }),
                tool_call_id: tool_call_id.as_str(),
                event_tx: Some(event_tx),
                available_tool_schemas: None,
                bypass_permissions: flags.bypass_permissions,
                auto_approve_permissions: flags.auto_approve_permissions,
                plan_read_only: flags.plan_read_only,
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
        Ok(())
    }

    /// Access the shared storage backend.
    pub fn storage(&self) -> &Arc<dyn bamboo_agent_core::storage::Storage> {
        self.inner.storage()
    }

    /// Access the runtime persistence adapter.
    pub fn persistence(&self) -> &Arc<dyn bamboo_domain::RuntimeSessionPersistence> {
        self.inner.persistence()
    }

    /// Submit typed logical-session messages through the coherent durable
    /// delivery plane configured by
    /// [`AgentBuilder::session_delivery`](AgentBuilder::session_delivery).
    pub fn session_messenger(&self) -> Option<&Arc<bamboo_engine::SessionMessenger>> {
        self.inner.session_messenger()
    }

    /// Inspect/claim the configured durable logical-session inbox.
    pub fn session_inbox(&self) -> Option<&Arc<dyn bamboo_domain::SessionInboxPort>> {
        self.inner.session_inbox()
    }

    /// Access the configured logical-session activation router for binding a
    /// host-specific [`SessionActivationSpawner`].
    pub fn activation_router(&self) -> Option<&Arc<bamboo_engine::SessionActivationRouter>> {
        self.inner.activation_router()
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

    /// Create a new in-memory session using this agent's configured model.
    ///
    /// The explicit [`AgentBuilder::model`] value wins; otherwise an agent
    /// assembled via [`AgentBuilder::with_defaults_for_data_dir`] captures the
    /// active provider's effective configured model. Returns
    /// [`SdkError::ModelNotConfigured`] instead of creating a session with an
    /// empty model when neither source supplies one. This does not persist the
    /// session until it is run or explicitly saved.
    pub fn new_session(&self, session_id: impl Into<String>) -> Result<Session, SdkError> {
        let model = self
            .model
            .as_deref()
            .or(self.session_model.as_deref())
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .ok_or(SdkError::ModelNotConfigured)?;
        let mut session = Session::new(session_id.into(), model.to_string());
        if let Some(project_id) = self.project_id.as_ref() {
            session.set_project_id_meta(project_id.to_string());
        }
        Ok(session)
    }

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
    pub async fn load_session(&self, session_id: &str) -> Result<Option<Session>, SdkError> {
        SessionAccess::load_session(self, session_id)
            .await
            .map_err(SdkError::from)
    }

    /// Compatibility alias for [`load_session`](Self::load_session).
    pub async fn get_session(&self, session_id: &str) -> Result<Option<Session>, SdkError> {
        self.load_session(session_id).await
    }

    /// The message history for a session, or [`SdkError::SessionNotFound`] if
    /// it doesn't exist.
    pub async fn session_history(&self, session_id: &str) -> Result<Vec<Message>, SdkError> {
        self.load_session(session_id)
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

        let mut session_a = agent.new_session("sess-a").expect("new_session");
        assert_eq!(session_a.model, "claude-test");
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
            .load_session("sess-a")
            .await
            .expect("load_session")
            .is_none());
    }

    #[tokio::test]
    async fn new_session_uses_effective_config_model_when_builder_model_is_unset() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let config_json = r#"{
            "provider": "anthropic",
            "providers": {
                "anthropic": { "api_key": "test-key", "model": "configured-model" }
            }
        }"#;
        std::fs::write(tmp.path().join("config.json"), config_json).expect("write config");
        let agent = AgentBuilder::new()
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .expect("defaults")
            .build()
            .expect("build");

        assert!(
            agent.model.is_none(),
            "the inferred session model must not become an execution override"
        );
        let session = agent.new_session("from-config").expect("configured model");
        assert_eq!(session.model, "configured-model");
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
            None,
            None,
            PermissionMode::Default,
        );
        let result = bare.list_sessions().await;
        assert!(matches!(result, Err(SdkError::Unsupported(_))));
        assert!(matches!(
            bare.new_session("missing-model"),
            Err(SdkError::ModelNotConfigured)
        ));
    }
}

#[cfg(test)]
mod reexecute_and_child_approval_tests {
    use super::*;
    use bamboo_agent_core::tools::{
        FunctionCall, Tool, ToolCall, ToolCtx, ToolError, ToolExecutionSessionFlags, ToolOutcome,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Notify;

    /// A tool whose real output is trivially distinguishable from the
    /// synthetic "Selected response: Approve" placeholder `submit_pending_response`
    /// writes, and which counts invocations — so tests can assert it actually ran
    /// (not merely that the metadata marker was consumed).
    struct RealOutputTool {
        calls: AtomicUsize,
        flags: StdMutex<Vec<ToolExecutionSessionFlags>>,
        workspaces: StdMutex<Vec<Option<std::path::PathBuf>>>,
    }

    impl RealOutputTool {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                flags: StdMutex::new(Vec::new()),
                workspaces: StdMutex::new(Vec::new()),
            }
        }
    }

    struct BlockingRealOutputTool {
        calls: AtomicUsize,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl Tool for BlockingRealOutputTool {
        fn name(&self) -> &str {
            "real_output_tool"
        }

        fn description(&self) -> &str {
            "test-only approved tool that blocks while ownership is challenged"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            _ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.notified().await;
            Ok(ToolOutcome::Completed(
                bamboo_agent_core::tools::ToolResult::text(true, "BLOCKING REAL OUTPUT"),
            ))
        }
    }

    struct ImmediateDoneProvider;

    struct CountingDoneProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl bamboo_llm::LLMProvider for ImmediateDoneProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
            Ok(Box::pin(futures::stream::iter([
                Ok(bamboo_llm::LLMChunk::Token("done".to_string())),
                Ok(bamboo_llm::LLMChunk::Done),
            ])))
        }
    }

    #[async_trait]
    impl bamboo_llm::LLMProvider for CountingDoneProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::iter([
                Ok(bamboo_llm::LLMChunk::Token("done".to_string())),
                Ok(bamboo_llm::LLMChunk::Done),
            ])))
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
            ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            self.flags.lock().unwrap().push(ToolExecutionSessionFlags {
                bypass_permissions: ctx.bypass_permissions,
                auto_approve_permissions: ctx.auto_approve_permissions,
                plan_read_only: ctx.plan_read_only,
            });
            self.workspaces.lock().unwrap().push(
                ctx.session_id
                    .as_deref()
                    .and_then(bamboo_agent_core::workspace_state::get_workspace),
            );
            Ok(ToolOutcome::Completed(
                bamboo_agent_core::tools::ToolResult::text(true, format!("REAL TOOL OUTPUT #{n}")),
            ))
        }
    }

    async fn build_test_agent_with_tool(
        data_dir: std::path::PathBuf,
        tool: Arc<RealOutputTool>,
    ) -> Agent {
        build_test_agent_with_tool_and_mode(data_dir, tool, None).await
    }

    async fn build_test_agent_with_tool_and_mode(
        data_dir: std::path::PathBuf,
        tool: Arc<RealOutputTool>,
        mode: Option<PermissionMode>,
    ) -> Agent {
        let config_json = r#"{
            "provider": "anthropic",
            "providers": {
                "anthropic": { "api_key": "test-key", "model": "claude-test" }
            }
        }"#;
        std::fs::write(data_dir.join("config.json"), config_json).expect("write config");

        let builder = AgentBuilder::new()
            .model("claude-test")
            .instruction("test agent")
            .tool_shared(tool);
        let builder = match mode {
            Some(PermissionMode::BypassPermissions) => builder.bypass_permissions(),
            Some(mode) => builder.permission_mode(mode),
            None => builder,
        };
        builder
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
        session.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new("test-run"));
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
            .await
            .expect("authoritative posture is available");
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
    async fn normal_run_prepares_project_workspace_before_approved_tool_replay() {
        let data_dir = tempfile::tempdir().expect("SDK data dir");
        let project_path = tempfile::tempdir().expect("SDK Project path");
        let project = bamboo_projects::ProjectStore::open(data_dir.path())
            .expect("Project store")
            .create_with_project_path(
                "Replay Project",
                None,
                project_path.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("Replay Project");
        std::fs::write(
            data_dir.path().join("config.json"),
            r#"{
                "provider": "anthropic",
                "providers": {
                    "anthropic": { "api_key": "test-key", "model": "claude-test" }
                }
            }"#,
        )
        .expect("SDK config");
        let tool = Arc::new(RealOutputTool::new());
        let agent = AgentBuilder::new()
            .provider(Arc::new(ImmediateDoneProvider))
            .model("claude-test")
            .instruction("configured System")
            .project_id(project.id.to_string())
            .tool_shared(tool.clone())
            .with_defaults_for_data_dir(data_dir.path().to_path_buf())
            .await
            .expect("defaults should assemble")
            .build()
            .expect("Project-backed SDK agent");

        let seed = seed_gated_tool_session("sdk-project-replay", "project-replay-call");
        agent
            .storage()
            .save_session(&seed)
            .await
            .expect("seed replay session");
        let outcome = agent
            .answer("sdk-project-replay", "Approve")
            .await
            .expect("approve replay");
        let mut session = outcome.session;
        session.add_message(Message::user("continue after replay"));

        agent
            .run_session(&mut session)
            .await
            .expect("normal run should complete");

        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
        let canonical = project_path
            .path()
            .canonicalize()
            .expect("canonical Project workspace");
        assert_eq!(
            tool.workspaces.lock().unwrap().as_slice(),
            &[Some(canonical.clone())],
            "approved replay must observe the published Project workspace on its first call"
        );
        assert_eq!(
            session.project_id_meta().as_deref(),
            Some(project.id.as_str())
        );
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(bamboo_config::paths::path_to_display_string(&canonical).as_str())
        );
        assert_eq!(
            session
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str())
        );
        assert_eq!(
            session
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_BINDING_STATUS_METADATA_KEY)
                .map(String::as_str),
            Some(bamboo_engine::project_context::WorkspaceBindingStatus::Registered.as_str())
        );
        let systems = session
            .messages
            .iter()
            .filter(|message| matches!(message.role, Role::System))
            .collect::<Vec<_>>();
        assert_eq!(systems.len(), 1);
        assert_eq!(systems[0].content, "configured System");
        assert!(!systems[0]
            .content
            .contains(canonical.to_string_lossy().as_ref()));
        assert!(!systems[0].content.contains("BAMBOO_WORKSPACE_CONTEXT"));
        let snapshot = session
            .prompt_snapshot
            .as_ref()
            .expect("SDK prompt snapshot");
        assert_eq!(snapshot.effective_system_prompt, "configured System");
        let workspace_context = snapshot
            .workspace_context
            .as_deref()
            .expect("typed SDK Workspace context");
        assert!(workspace_context.contains(canonical.to_string_lossy().as_ref()));
        assert!(workspace_context.contains("Workspace source: project_default"));
        assert!(workspace_context.contains("Binding status: registered"));
    }

    #[tokio::test]
    async fn normal_run_recovers_serialized_legacy_workspace_before_replay_and_replaces_system() {
        let data_dir = tempfile::tempdir().expect("SDK data dir");
        let legacy_workspace = tempfile::tempdir().expect("legacy SDK Workspace");
        let canonical = legacy_workspace
            .path()
            .canonicalize()
            .expect("canonical legacy Workspace");
        let display = bamboo_config::paths::path_to_display_string(&canonical);
        std::fs::write(
            data_dir.path().join("config.json"),
            r#"{
                "provider": "anthropic",
                "providers": {
                    "anthropic": { "api_key": "test-key", "model": "claude-test" }
                }
            }"#,
        )
        .expect("SDK config");
        let tool = Arc::new(RealOutputTool::new());
        let agent = AgentBuilder::new()
            .provider(Arc::new(ImmediateDoneProvider))
            .model("claude-test")
            .instruction("configured System")
            .tool_shared(tool.clone())
            .with_defaults_for_data_dir(data_dir.path().to_path_buf())
            .await
            .expect("defaults should assemble")
            .build()
            .expect("SDK agent");

        let mut legacy = seed_gated_tool_session("sdk-legacy-replay", "legacy-replay-call");
        legacy.messages.insert(
            0,
            Message::system(
                bamboo_engine::runtime::context::build_workspace_prompt_context(&display)
                    .expect("legacy Workspace marker"),
            ),
        );
        assert!(legacy.workspace_path_meta().is_none());
        let serialized = serde_json::to_vec(&legacy).expect("serialize legacy SDK session");
        let legacy: Session =
            serde_json::from_slice(&serialized).expect("deserialize legacy SDK session");
        agent
            .storage()
            .save_session(&legacy)
            .await
            .expect("seed serialized legacy session");
        let outcome = agent
            .answer("sdk-legacy-replay", "Approve")
            .await
            .expect("approve legacy replay");
        let mut session = outcome.session;
        session.add_message(Message::user("continue after legacy replay"));

        agent
            .run_session(&mut session)
            .await
            .expect("normal legacy run should complete");

        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            tool.workspaces.lock().unwrap().as_slice(),
            &[Some(canonical.clone())],
            "legacy Workspace must be published before the approved tool is replayed"
        );
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(display.as_str())
        );
        assert_eq!(
            session
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some(bamboo_engine::project_context::WorkspaceSource::Session.as_str())
        );
        assert_eq!(
            session
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_BINDING_STATUS_METADATA_KEY)
                .map(String::as_str),
            Some(bamboo_engine::project_context::WorkspaceBindingStatus::Unregistered.as_str())
        );
        let systems = session
            .messages
            .iter()
            .filter(|message| matches!(message.role, Role::System))
            .collect::<Vec<_>>();
        assert_eq!(systems.len(), 1);
        assert_eq!(systems[0].content, "configured System");
        assert!(!systems[0].content.contains(&display));
        assert!(!systems[0].content.contains("BAMBOO_WORKSPACE_CONTEXT"));
        let snapshot = session
            .prompt_snapshot
            .as_ref()
            .expect("SDK prompt snapshot");
        assert_eq!(snapshot.effective_system_prompt, "configured System");
        assert!(snapshot
            .workspace_context
            .as_deref()
            .is_some_and(|context| context.contains(&display)));
    }

    #[tokio::test]
    async fn missing_project_resolver_stops_sdk_before_replay_system_replacement_or_provider() {
        let data_dir = tempfile::tempdir().expect("manual SDK data dir");
        let legacy_workspace = tempfile::tempdir().expect("retryable legacy Workspace");
        let legacy_display = bamboo_config::paths::path_to_display_string(legacy_workspace.path());
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(data_dir.path().join("sessions"))
                .await
                .expect("session store"),
        );
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            Arc::new(bamboo_storage::LockedSessionStore::new(store.clone()));
        let metrics = bamboo_metrics::MetricsCollector::spawn(
            Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                data_dir.path().join("metrics.db"),
            )),
            7,
        );
        let tool = Arc::new(RealOutputTool::new());
        let registry = bamboo_tools::ToolRegistry::new();
        registry
            .register_shared(tool.clone())
            .expect("register replay tool");
        let provider = Arc::new(CountingDoneProvider {
            calls: AtomicUsize::new(0),
        });
        let runtime = bamboo_engine::Agent::builder()
            .storage(store.clone())
            .persistence(persistence)
            .attachment_reader(store.clone())
            .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
            .metrics_collector(metrics)
            .config(Arc::new(tokio::sync::RwLock::new(
                bamboo_llm::Config::default(),
            )))
            .provider(provider.clone())
            .default_tools(Arc::new(bamboo_tools::BuiltinToolExecutor::with_registry(
                registry,
            )))
            .build()
            .expect("manual runtime without Project resolver");
        let agent = Agent::from_runtime_with_config(
            runtime,
            Some("configured System".to_string()),
            Some("configured-model".to_string()),
            None,
            None,
            None,
            None,
            PermissionMode::Default,
        );

        let mut session = seed_gated_tool_session("sdk-missing-resolver", "missing-resolver-call");
        let legacy_block =
            bamboo_engine::runtime::context::build_workspace_prompt_context(&legacy_display)
                .expect("retryable legacy Workspace marker");
        session.messages.insert(
            0,
            Message::system(format!("caller System\n\n{legacy_block}")),
        );
        session.set_project_id_meta("project-missing-resolver");
        session.metadata.insert(
            PERMISSION_REEXECUTE_METADATA_KEY.to_string(),
            "missing-resolver-call".to_string(),
        );
        session.prompt_snapshot = Some(
            serde_json::from_value(serde_json::json!({
                "base_system_prompt": "stale SDK snapshot",
                "effective_system_prompt": "stale SDK snapshot"
            }))
            .expect("synthetic stale SDK prompt snapshot"),
        );
        agent
            .storage()
            .save_session(&session)
            .await
            .expect("seed assigned replay session");
        let before = serde_json::to_vec(&session).expect("serialize retryable SDK session");
        let (event_tx, mut event_rx) = mpsc::channel(16);

        let error = agent
            .execute_internal(&mut session, event_tx, CancellationToken::new())
            .await
            .expect_err("assigned SDK session must fail without Project resolver");

        assert!(matches!(error, AgentError::ProjectContext(_)));
        assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert!(
            event_rx.try_recv().is_err(),
            "prep failure must emit no events"
        );
        assert_eq!(
            serde_json::to_vec(&session).expect("serialize failed SDK session"),
            before,
            "missing resolver must preserve the full serialized retry state"
        );
        assert!(session.messages[0].content.contains(&legacy_display));
        assert!(session.messages[0]
            .content
            .contains("BAMBOO_WORKSPACE_CONTEXT"));
        assert_eq!(
            session
                .metadata
                .get(PERMISSION_REEXECUTE_METADATA_KEY)
                .map(String::as_str),
            Some("missing-resolver-call"),
            "approval replay marker must remain retryable"
        );
    }

    #[tokio::test]
    async fn latest_plan_consumes_stale_marker_without_tool_start_or_invocation() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let tool = Arc::new(RealOutputTool::new());
        let agent = build_test_agent_with_tool(tmp.path().to_path_buf(), tool.clone()).await;
        let mut session = seed_gated_tool_session("sdk-plan-replay", "plan-call");
        session.metadata.insert(
            PERMISSION_REEXECUTE_METADATA_KEY.to_string(),
            "plan-call".to_string(),
        );

        let mut latest = session.clone();
        let plan_state: bamboo_domain::PlanModeState = serde_json::from_value(serde_json::json!({
            "entered_at": "2026-07-31T00:00:00Z",
            "pre_permission_mode": "default",
            "status": "exploring"
        }))
        .expect("valid plan state");
        latest.agent_runtime_state.as_mut().unwrap().plan_mode = Some(plan_state);
        agent
            .storage()
            .save_session(&latest)
            .await
            .expect("persist latest Plan posture");

        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(16);
        agent
            .reexecute_approved_tool_if_pending(&mut session, &event_tx)
            .await
            .expect("latest Plan is a handled replay denial");
        drop(event_tx);

        assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
        assert!(
            event_rx.try_recv().is_err(),
            "Plan denial emits no ToolStart"
        );
        assert!(!session
            .metadata
            .contains_key(PERMISSION_REEXECUTE_METADATA_KEY));
        let blocked = session
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some("plan-call"))
            .expect("blocked result remains in history");
        assert_eq!(blocked.tool_success, Some(false));
        assert!(blocked.content.contains("Plan mode blocked"));
        assert!(session
            .agent_runtime_state
            .as_ref()
            .is_some_and(|runtime| runtime.plan_mode.is_some()));
    }

    #[tokio::test]
    async fn missing_authoritative_posture_retains_marker_and_aborts_replay() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let tool = Arc::new(RealOutputTool::new());
        let agent = build_test_agent_with_tool(tmp.path().to_path_buf(), tool.clone()).await;
        let mut session = seed_gated_tool_session("sdk-missing-replay", "missing-call");
        session.metadata.insert(
            PERMISSION_REEXECUTE_METADATA_KEY.to_string(),
            "missing-call".to_string(),
        );

        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(16);
        let error = agent
            .reexecute_approved_tool_if_pending(&mut session, &event_tx)
            .await
            .expect_err("missing durable posture must abort resume");
        drop(event_tx);

        assert!(error.to_string().contains("session missing"));
        assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
        assert!(
            event_rx.try_recv().is_err(),
            "failed refresh emits no events"
        );
        assert_eq!(
            session
                .metadata
                .get(PERMISSION_REEXECUTE_METADATA_KEY)
                .map(String::as_str),
            Some("missing-call"),
            "storage failure keeps the approval marker retryable"
        );
    }

    #[tokio::test]
    async fn configured_auto_and_explicit_bypass_reach_real_replay_context() {
        for (mode, expected) in [
            (
                PermissionMode::Auto,
                ToolExecutionSessionFlags {
                    bypass_permissions: false,
                    auto_approve_permissions: true,
                    plan_read_only: false,
                },
            ),
            (
                PermissionMode::BypassPermissions,
                ToolExecutionSessionFlags {
                    bypass_permissions: true,
                    auto_approve_permissions: false,
                    plan_read_only: false,
                },
            ),
        ] {
            let tmp = tempfile::tempdir().expect("tempdir");
            let tool = Arc::new(RealOutputTool::new());
            let agent = build_test_agent_with_tool_and_mode(
                tmp.path().to_path_buf(),
                tool.clone(),
                Some(mode),
            )
            .await;
            let mut session = seed_gated_tool_session("sdk-flags-replay", "flags-call");
            session.metadata.insert(
                PERMISSION_REEXECUTE_METADATA_KEY.to_string(),
                "flags-call".to_string(),
            );
            agent.storage().save_session(&session).await.unwrap();

            let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
            agent
                .reexecute_approved_tool_if_pending(&mut session, &event_tx)
                .await
                .expect("configured replay should execute");

            assert_eq!(*tool.flags.lock().unwrap(), vec![expected]);
        }
    }

    #[tokio::test]
    async fn rejected_clone_never_enters_approved_mutating_tool_replay() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let config_json = r#"{
            "provider": "anthropic",
            "providers": {
                "anthropic": { "api_key": "test-key", "model": "claude-test" }
            }
        }"#;
        std::fs::write(tmp.path().join("config.json"), config_json).expect("write config");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let tool = Arc::new(BlockingRealOutputTool {
            calls: AtomicUsize::new(0),
            entered: entered.clone(),
            release: release.clone(),
        });
        let router = bamboo_engine::SessionActivationRouter::new();
        let agent = AgentBuilder::new()
            .model("claude-test")
            .instruction("test agent")
            .provider(Arc::new(ImmediateDoneProvider))
            .tool_shared(tool.clone())
            .session_delivery(router)
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .expect("defaults should assemble")
            .build()
            .expect("agent should build");

        let mut first_session = seed_gated_tool_session("approved-replay-owner", "approved-call");
        first_session.metadata.insert(
            PERMISSION_REEXECUTE_METADATA_KEY.to_string(),
            "approved-call".to_string(),
        );
        first_session.add_message(Message::user("continue after approval"));
        agent
            .storage()
            .save_session(&first_session)
            .await
            .expect("seed approved session");
        let mut rejected_session = first_session.clone();

        let first_agent = agent.clone();
        let first = tokio::spawn(async move { first_agent.run_session(&mut first_session).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .expect("first owner must enter approved tool replay");

        let collision = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            agent.run_session(&mut rejected_session),
        )
        .await
        .expect("rejected clone must fail promptly")
        .expect_err("a second logical-session owner must collide");
        assert!(
            collision
                .to_string()
                .contains("session activation owner collision"),
            "unexpected collision error: {collision}"
        );
        assert_eq!(
            tool.calls.load(Ordering::SeqCst),
            1,
            "the rejected clone must collide before entering a mutating tool"
        );

        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), first)
            .await
            .expect("first owner must finish")
            .expect("first owner task must not panic")
            .expect("first owner execution must succeed");
        assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
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
            .await
            .expect("missing marker is a no-op");

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
            .await
            .expect("missing tool call clears the marker without replay");
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
        bamboo_engine::external_agents::live::observe_pending_approval(
            bamboo_engine::external_agents::live::PendingApprovalObservation {
                registry: None,
                parent_session_id: "parent-x",
                child_id: "child-x",
                child_attempt: 0,
                request_id: "req-1",
                tool_name: "shell",
                permission: "execute",
                resource: "cargo test",
                event_tx: approval_event_tx,
            },
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
