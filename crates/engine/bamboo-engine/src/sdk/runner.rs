//! Ergonomic profile-driven runner facade over the canonical spawn core.
//!
//! [`ProfileRunner`] is a thin wrapper around the existing [`SpawnContext`] (it
//! does NOT introduce a parallel `RuntimeDeps` god-struct). `run_profile` derives
//! the child's `disabled_tools` from the profile's [`ToolPolicy`] and the live
//! tool catalog, builds a [`SpawnJob`], and delegates to
//! [`crate::sdk::spawn::run_child_spawn`] — the single canonical spawn path.
//!
//! The assignment prompt and system prompt already live in the persisted child
//! session (matching real spawn semantics: `ExecuteRequest.initial_message` is
//! empty; the last user message in the child drives execution). `RunProfileInput`
//! therefore stays minimal.

use bamboo_agent_core::AgentEvent;
use bamboo_domain::subagent::{disabled_tools_for_profile, SubagentProfile};
use tokio::sync::broadcast;

use crate::runtime::execution::session_events::get_or_create_event_sender;
use crate::runtime::execution::spawn::{SpawnContext, SpawnJob};

/// Minimal input to run a child session under a [`SubagentProfile`].
///
/// The persisted child session already holds the system prompt + the pending user
/// message, so only routing identifiers and the resolved model are required here.
#[derive(Debug, Clone)]
pub struct RunProfileInput {
    /// Child session id (already persisted with kind=child + a pending user msg).
    pub child_session_id: String,
    /// Parent session id whose event stream receives `SubAgent*` events.
    pub parent_session_id: String,
    /// Resolved model string for the child run.
    pub model: String,
}

/// Ergonomic facade for profile-driven child spawns.
///
/// Reuses [`SpawnContext`] (agent, tools, caches, router, completion handler) —
/// the same dependency bundle the background scheduler uses.
pub struct ProfileRunner {
    ctx: SpawnContext,
}

/// Construct a [`ProfileRunner`] from an existing [`SpawnContext`].
pub fn profile_runner(ctx: SpawnContext) -> ProfileRunner {
    ProfileRunner::new(ctx)
}

impl ProfileRunner {
    /// Create a runner over the given spawn context.
    pub fn new(ctx: SpawnContext) -> Self {
        Self { ctx }
    }

    /// Tool names currently exposed by the runtime's tool executor.
    ///
    /// This is the canonical source for `disabled_tools_for_profile` (TD-8): the
    /// caller does not thread a separate `tool_names: Vec<String>`.
    fn tool_names(&self) -> Vec<String> {
        self.ctx
            .tools
            .list_tools()
            .into_iter()
            .map(|schema| schema.function.name)
            .collect()
    }

    /// Build a [`SpawnJob`] for the given profile + input, computing the schema-level
    /// `disabled_tools` from the profile's tool policy and the live tool catalog.
    pub(crate) fn build_job(&self, profile: &SubagentProfile, input: &RunProfileInput) -> SpawnJob {
        let tool_names = self.tool_names();
        let disabled = disabled_tools_for_profile(&profile.tools, &tool_names);
        let disabled_tools = if disabled.is_empty() {
            None
        } else {
            Some(disabled)
        };
        SpawnJob {
            parent_session_id: input.parent_session_id.clone(),
            child_session_id: input.child_session_id.clone(),
            model: input.model.clone(),
            disabled_tools,
        }
    }

    /// Run a child session under the given profile via the canonical spawn core.
    ///
    /// ANTI-FORK: constructs a [`SpawnJob`] and delegates to
    /// [`crate::sdk::spawn::run_child_spawn`]; there is no inline execute/finalize.
    pub async fn run_profile(
        &self,
        profile: &SubagentProfile,
        input: RunProfileInput,
    ) -> Result<(), String> {
        let job = self.build_job(profile, &input);
        crate::sdk::spawn::run_child_spawn(self.ctx.clone(), job).await
    }

    /// Run a child session under the given profile and return a receiver of the
    /// child's [`AgentEvent`] stream.
    ///
    /// The receiver is subscribed from the existing broadcast infra
    /// (`ctx.session_event_senders`) *before* the spawn is started, so no events
    /// are missed. This reuses the canonical broadcast channel — it does NOT
    /// invent a parallel `RunOutcomeStream`/`status_rx` mpsc.
    pub async fn run_profile_stream(
        &self,
        profile: &SubagentProfile,
        input: RunProfileInput,
    ) -> Result<broadcast::Receiver<AgentEvent>, String> {
        let child_tx =
            get_or_create_event_sender(&self.ctx.session_event_senders, &input.child_session_id)
                .await;
        let rx = child_tx.subscribe();
        let job = self.build_job(profile, &input);
        crate::sdk::spawn::run_child_spawn(self.ctx.clone(), job).await?;
        Ok(rx)
    }
}
