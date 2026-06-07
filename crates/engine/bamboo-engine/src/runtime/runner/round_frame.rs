//! Shared execution-frame context for round-level runner functions.
//!
//! [`RoundFrame`] bundles the infrastructure parameters that are threaded
//! through `handle_tool_calls_path`, `execute_round_tool_calls`, and
//! related call chains.  Grouping them into a single struct reduces
//! per-function parameter counts below the clippy `too_many_arguments`
//! threshold (7) and makes call-sites more readable.

use std::sync::Arc;

use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::AgentEvent;
use bamboo_llm::LLMProvider;
use tokio::sync::mpsc;

use bamboo_metrics::MetricsCollector;
use crate::runtime::config::AgentLoopConfig;

/// Immutable infrastructure context shared across a single round of execution.
///
/// All fields are borrowed references — the struct is constructed at the
/// top of each round and passed down through the call chain without cloning.
pub(crate) struct RoundFrame<'a> {
    // -- Identity --
    pub session_id: &'a str,
    pub round_id: &'a str,
    pub turn: usize,
    pub debug_enabled: bool,

    // -- Observability --
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub metrics_collector: Option<&'a MetricsCollector>,

    // -- Engine resources --
    pub config: &'a AgentLoopConfig,
    pub llm: &'a Arc<dyn LLMProvider>,
    pub tools: &'a Arc<dyn ToolExecutor>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_frame_fields_are_accessible() {
        // Compile-time sanity: struct fields are accessible.
        // This test doesn't construct a RoundFrame (requires async plumbing),
        // but confirms the type layout compiles correctly.
        fn _assert_send<T: Send>() {}
        fn _assert_sync<T: Sync>() {}
        // RoundFrame contains only references and Arc — both Send + Sync.
        _assert_send::<RoundFrame<'_>>();
        _assert_sync::<RoundFrame<'_>>();
    }
}
