//! Agent lifecycle hook trait.
//!
//! Hooks are invoked at well-defined points in the agent loop (see
//! [`AgentHookPoint`](bamboo_domain::AgentHookPoint)).
//! They can inspect state, mutate session, and control flow via
//! [`HookResult`](bamboo_domain::HookResult).

use async_trait::async_trait;
use bamboo_domain::{AgentHookPoint, HookPayload, HookResult};

use super::types::Session;

/// Trait for agent lifecycle hooks.
///
/// Implementations must be `Send + Sync` because hooks may be invoked
/// from multiple concurrent agent runs.
#[async_trait]
pub trait AgentHook: Send + Sync {
    /// Which hook point this hook subscribes to.
    fn point(&self) -> AgentHookPoint;

    /// Execute the hook.
    ///
    /// Receives immutable access to the session, the current hook point, and a
    /// point-specific payload.
    /// Returns a [`HookResult`] to control flow.
    async fn run(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
    ) -> HookResult;

    /// Whether this hook applies to the point-specific payload.
    ///
    /// The default accepts every payload. Configured tool hooks override this
    /// to apply their tool-name matcher without spawning a process or emitting
    /// a misleading execution checkpoint for non-matching calls.
    fn matches(&self, _payload: &HookPayload) -> bool {
        true
    }

    /// Optional priority (lower runs first). Default is 100.
    fn priority(&self) -> u32 {
        100
    }

    /// Optional name for logging and debugging.
    fn name(&self) -> &str {
        "unnamed_hook"
    }
}
