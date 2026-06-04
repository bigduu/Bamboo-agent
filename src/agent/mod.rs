//! Ergonomic top-level Agent SDK.
//!
//! This module promotes the `SubagentProfile` system into a first-class,
//! ergonomic SDK so library consumers can write:
//!
//! ```rust,ignore
//! use bamboo_agent::agent::Agent;
//!
//! let agent = Agent::builder()
//!     .researcher()
//!     .model("claude-sonnet-4-6")
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
//!   `run_stream` execute the agent loop with the builder's role-derived system
//!   prompt + tool policy applied to the session.
//! - [`AgentBuilder`] — profile-driven builder (`.researcher()`, `.coder()`,
//!   `.from_profile(..)`) that resolves built-in profiles from
//!   [`bamboo_engine::profiles`] and assembles default deps via
//!   [`AgentBuilder::with_defaults_for_data_dir`].
//! - [`ExecuteRequestBuilder`] — ergonomic builder over the multi-field
//!   [`bamboo_engine::ExecuteRequest`].
//! - [`ToolSpec`] + [`builtin_tool_names`] — tool
//!   descriptors derived from the canonical `BUILTIN_TOOL_NAMES`.
//!
//! ## Anti-fork invariant
//!
//! The SDK never reimplements the agent loop. `run` / `run_stream` funnel into
//! `bamboo_engine::Agent::execute` (the single canonical execution path); child
//! spawning likewise funnels into `bamboo_engine::run_child_spawn`.

mod builder;
mod execute_request;
mod tools;

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_domain::subagent::{disabled_tools_for_profile, ToolPolicy};

pub use builder::AgentBuilder;
pub use execute_request::ExecuteRequestBuilder;
pub use tools::{builtin_tool_names, builtin_tool_specs, ToolSpec, CANONICAL_TOOL_NAMES};

// Re-export the engine profile system so SDK consumers can enumerate and
// inspect built-in / loaded profiles without importing `bamboo_engine`.
pub use bamboo_engine::profiles;

// Convenience re-exports of commonly used types (single source of truth — these
// supersede the old duplicate re-export chain, resolving TD-2).
pub use bamboo_agent_core::{
    AgentError, AgentEvent, Message, MessageContent, Role, Session, TokenBudgetUsage, TokenUsage,
};
pub use bamboo_domain::subagent::{ModelHint, SubagentProfile, UiHint};
pub use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
pub use bamboo_engine::ExecuteRequest;
pub use bamboo_infrastructure::LLMProvider;
pub use bamboo_tools::{BuiltinToolExecutor, BuiltinToolExecutorBuilder, ToolOutputManager};

/// Default event-channel buffer used by [`Agent::run`].
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Stable, ergonomic entry point for agent execution.
///
/// Wraps a [`bamboo_engine::Agent`] (which owns the shared runtime) plus the
/// role-derived configuration captured at build time. Clone is cheap.
#[derive(Clone)]
pub struct Agent {
    inner: bamboo_engine::Agent,
    /// Role / instruction system prompt injected into the session at `run` time.
    system_prompt: Option<String>,
    /// Tool policy translated to `disabled_tools` at `run` time.
    tool_policy: Option<ToolPolicy>,
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
            tool_policy: None,
            model: None,
        }
    }

    /// Wrap an engine [`Agent`](bamboo_engine::Agent) plus the role-derived
    /// configuration assembled by [`AgentBuilder`].
    pub(crate) fn from_runtime_with_config(
        inner: bamboo_engine::Agent,
        system_prompt: Option<String>,
        tool_policy: Option<ToolPolicy>,
        model: Option<String>,
    ) -> Self {
        Self {
            inner,
            system_prompt,
            tool_policy,
            model,
        }
    }

    /// Run the agent loop on `session` with the given input, draining events
    /// internally until completion.
    ///
    /// The builder's role system prompt + model are applied to the session
    /// before execution; the tool policy is translated to `disabled_tools`.
    pub async fn run(
        &self,
        session: &mut Session,
        input: impl Into<String>,
    ) -> Result<(), AgentError> {
        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(EVENT_CHANNEL_CAPACITY);
        let cancel_token = CancellationToken::new();

        // Drain events so the bounded channel never blocks the loop.
        let drain = tokio::spawn(async move { while event_rx.recv().await.is_some() {} });

        let result = self
            .execute_internal(session, input.into(), event_tx, cancel_token)
            .await;

        let _ = drain.await;
        result
    }

    /// Run the agent loop on `session`, returning a receiver of [`AgentEvent`]s.
    ///
    /// The execution runs on a background task; the caller drives it by reading
    /// from the returned receiver until it closes. The role system prompt /
    /// model / tool policy are applied exactly as in [`run`](Self::run).
    pub fn run_stream(
        &self,
        mut session: Session,
        input: impl Into<String>,
    ) -> mpsc::Receiver<AgentEvent> {
        let (event_tx, event_rx) = mpsc::channel::<AgentEvent>(EVENT_CHANNEL_CAPACITY);
        let cancel_token = CancellationToken::new();
        let agent = self.clone();
        let input = input.into();

        tokio::spawn(async move {
            if let Err(error) = agent
                .execute_internal(&mut session, input, event_tx, cancel_token)
                .await
            {
                tracing::warn!("Agent::run_stream execution failed: {error}");
            }
        });

        event_rx
    }

    /// Shared execution path: prepare the session (system prompt + model), build
    /// the [`ExecuteRequest`] (with `disabled_tools` from the tool policy), and
    /// delegate to the canonical engine execution path.
    async fn execute_internal(
        &self,
        session: &mut Session,
        input: String,
        event_tx: mpsc::Sender<AgentEvent>,
        cancel_token: CancellationToken,
    ) -> Result<(), AgentError> {
        // Apply the role system prompt as the session's leading System message
        // (the engine extracts the system prompt from the session messages).
        // The builder's prompt is AUTHORITATIVE: it replaces an existing leading
        // System message rather than deferring to it, so a caller-supplied
        // session can't silently shadow the configured profile/instruction.
        if let Some(prompt) = self.system_prompt.as_ref() {
            match session
                .messages
                .iter_mut()
                .find(|m| matches!(m.role, Role::System))
            {
                Some(existing) => *existing = Message::system(prompt.clone()),
                None => session.messages.insert(0, Message::system(prompt.clone())),
            }
        }

        if let Some(model) = self.model.as_ref() {
            session.model = model.clone();
        }

        // The driving user message goes into the session; the engine runner is
        // configured to skip echoing the initial message, matching spawn
        // semantics where the last user message drives execution.
        session.add_message(Message::user(input.clone()));

        let disabled_tools = self.tool_policy.as_ref().map(|policy| {
            let all = builtin_tool_names();
            disabled_tools_for_profile(policy, &all)
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        });

        let mut builder = ExecuteRequestBuilder::new(input, event_tx, cancel_token);
        if let Some(model) = self.model.clone() {
            builder = builder.model(model);
        }
        if let Some(disabled) = disabled_tools {
            builder = builder.disabled_tools(disabled);
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
