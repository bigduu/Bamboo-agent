//! Ergonomic child-session runner facade over the canonical spawn core.
//!
//! [`ChildRunner`] is a thin wrapper around the existing [`SpawnContext`] (it
//! does NOT introduce a parallel `RuntimeDeps` god-struct). It builds a
//! [`SpawnJob`] and delegates to [`crate::sdk::spawn::run_child_spawn`] — the
//! single canonical spawn path.
//!
//! Sub-agents are plain full agents: there is no per-role tool trimming, so the
//! job's `disabled_tools` is always `None` here (the unrelated global
//! `config.disabled_tools` path is applied elsewhere, not via this runner).
//!
//! The assignment prompt and system prompt already live in the persisted child
//! session (matching real spawn semantics: `ExecuteRequest.initial_message` is
//! empty; the last user message in the child drives execution). `RunChildInput`
//! therefore stays minimal.

use bamboo_agent_core::AgentEvent;
use tokio::sync::broadcast;

use crate::runtime::execution::session_events::get_or_create_event_sender;
use crate::runtime::execution::spawn::{SpawnContext, SpawnJob};

/// Minimal input to run a child session.
///
/// The persisted child session already holds the system prompt + the pending user
/// message, so only routing identifiers and the resolved model are required here.
#[derive(Debug, Clone)]
pub struct RunChildInput {
    /// Child session id (already persisted with kind=child + a pending user msg).
    pub child_session_id: String,
    /// Parent session id whose event stream receives `SubAgent*` events.
    pub parent_session_id: String,
    /// Resolved model string for the child run.
    pub model: String,
}

/// Ergonomic facade for child spawns.
///
/// Reuses [`SpawnContext`] (agent, tools, caches, router, completion handler) —
/// the same dependency bundle the background scheduler uses.
pub struct ChildRunner {
    ctx: SpawnContext,
}

/// Construct a [`ChildRunner`] from an existing [`SpawnContext`].
pub fn child_runner(ctx: SpawnContext) -> ChildRunner {
    ChildRunner::new(ctx)
}

impl ChildRunner {
    /// Create a runner over the given spawn context.
    pub fn new(ctx: SpawnContext) -> Self {
        Self { ctx }
    }

    /// Build a [`SpawnJob`] for the given input.
    ///
    /// Sub-agents are full agents with the full toolset, so `disabled_tools`
    /// is always `None`.
    pub(crate) fn build_job(&self, input: &RunChildInput) -> SpawnJob {
        SpawnJob {
            parent_session_id: input.parent_session_id.clone(),
            child_session_id: input.child_session_id.clone(),
            model: input.model.clone(),
            disabled_tools: None,
        }
    }

    /// Run a child session via the canonical spawn core.
    ///
    /// ANTI-FORK: constructs a [`SpawnJob`] and delegates to
    /// [`crate::sdk::spawn::run_child_spawn`]; there is no inline execute/finalize.
    pub async fn run_child(&self, input: RunChildInput) -> Result<(), String> {
        let job = self.build_job(&input);
        crate::sdk::spawn::run_child_spawn(self.ctx.clone(), job).await
    }

    /// Run a child session and return a receiver of the child's
    /// [`AgentEvent`] stream.
    ///
    /// The receiver is subscribed from the existing broadcast infra
    /// (`ctx.session_event_senders`) *before* the spawn is started, so no events
    /// are missed. This reuses the canonical broadcast channel — it does NOT
    /// invent a parallel `RunOutcomeStream`/`status_rx` mpsc.
    pub async fn run_child_stream(
        &self,
        input: RunChildInput,
    ) -> Result<broadcast::Receiver<AgentEvent>, String> {
        let child_tx =
            get_or_create_event_sender(&self.ctx.session_event_senders, &input.child_session_id)
                .await;
        let rx = child_tx.subscribe();
        let job = self.build_job(&input);
        crate::sdk::spawn::run_child_spawn(self.ctx.clone(), job).await?;
        Ok(rx)
    }
}
