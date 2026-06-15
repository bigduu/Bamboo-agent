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
mod execute_request;
mod tools;

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub use builder::AgentBuilder;
pub use execute_request::ExecuteRequestBuilder;
pub use tools::{
    builtin_tool_names, builtin_tool_specs, BuiltinTool, ToolSpec, CANONICAL_TOOL_NAMES,
};

// Convenience re-exports of commonly used types (single source of truth — these
// supersede the old duplicate re-export chain, resolving TD-2).
pub use bamboo_agent_core::{
    AgentError, AgentEvent, Message, MessageContent, Role, Session, TokenBudgetUsage, TokenUsage,
};
pub use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
pub use bamboo_engine::ExecuteRequest;
pub use bamboo_llm::LLMProvider;
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
        }
    }

    /// Wrap an engine [`Agent`](bamboo_engine::Agent) plus the instruction /
    /// model configuration assembled by [`AgentBuilder`].
    pub(crate) fn from_runtime_with_config(
        inner: bamboo_engine::Agent,
        system_prompt: Option<String>,
        model: Option<String>,
    ) -> Self {
        Self {
            inner,
            system_prompt,
            model,
        }
    }

    /// Run the agent loop on `session` with the given input, draining events
    /// internally until completion.
    ///
    /// The configured instruction + model are applied to the session before
    /// execution; the tool set was fixed on the agent's executor at build time.
    pub async fn run(
        &self,
        session: &mut Session,
        input: impl Into<String>,
    ) -> Result<(), AgentError> {
        session.add_message(Message::user(input.into()));
        self.run_session(session).await
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
        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(EVENT_CHANNEL_CAPACITY);
        let cancel_token = CancellationToken::new();

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

    /// Stream the run's [`AgentEvent`]s for a caller-provided message list,
    /// without appending a new turn (the last `User` message drives execution).
    pub fn run_stream_session(&self, mut session: Session) -> mpsc::Receiver<AgentEvent> {
        let (event_tx, event_rx) = mpsc::channel::<AgentEvent>(EVENT_CHANNEL_CAPACITY);
        let cancel_token = CancellationToken::new();
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
}
