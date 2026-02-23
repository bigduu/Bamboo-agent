use std::sync::Arc;

use agent_core::budget::TokenBudget;
use agent_core::composition::CompositionExecutor;
use agent_core::storage::Storage;
use agent_core::tools::ToolSchema;
use agent_metrics::MetricsCollector;
use agent_skill::SkillManager;
use agent_tools::ToolRegistry;

/// Configuration for the agent loop.
pub struct AgentLoopConfig {
    pub max_rounds: usize,
    pub system_prompt: Option<String>,
    pub additional_tool_schemas: Vec<ToolSchema>,
    pub tool_registry: Arc<ToolRegistry>,
    pub composition_executor: Option<Arc<CompositionExecutor>>,
    pub skill_manager: Option<Arc<SkillManager>>,
    /// If true, skip appending the initial user message (already present in session).
    pub skip_initial_user_message: bool,
    /// Optional storage for persisting session changes
    pub storage: Option<Arc<dyn Storage>>,
    /// Optional asynchronous metrics collector
    pub metrics_collector: Option<MetricsCollector>,
    /// Model name used for metrics attribution
    pub model_name: Option<String>,
    /// Token budget for context management (optional, defaults to model's limits)
    pub token_budget: Option<TokenBudget>,
}

impl Default for AgentLoopConfig {
    fn default() -> Self {
        Self {
            max_rounds: 50,
            system_prompt: None,
            additional_tool_schemas: Vec::new(),
            tool_registry: Arc::new(ToolRegistry::new()),
            composition_executor: None,
            skill_manager: None,
            skip_initial_user_message: false,
            storage: None,
            metrics_collector: None,
            model_name: None,
            token_budget: None,
        }
    }
}
