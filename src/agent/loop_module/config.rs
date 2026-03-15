use std::sync::Arc;

use crate::agent::core::budget::TokenBudget;
use crate::agent::core::composition::CompositionExecutor;
use crate::agent::core::storage::AttachmentReader;
use crate::agent::core::storage::Storage;
use crate::agent::core::tools::ToolSchema;
use crate::agent::metrics::MetricsCollector;
use crate::agent::skill::SkillManager;
use crate::agent::tools::ToolRegistry;
use crate::core::ReasoningEffort;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageFallbackMode {
    Placeholder,
    Error,
    Ocr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageFallbackConfig {
    pub mode: ImageFallbackMode,
}

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
    /// Optional attachment reader for resolving `bamboo-attachment://...` references
    /// into `data:` URLs for upstream providers. This must not mutate session storage.
    pub attachment_reader: Option<Arc<dyn AttachmentReader>>,
    /// Optional asynchronous metrics collector
    pub metrics_collector: Option<MetricsCollector>,
    /// Model name used for metrics attribution
    pub model_name: Option<String>,
    /// Optional request-time reasoning effort override.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Token budget for context management (optional, defaults to model's limits)
    pub token_budget: Option<TokenBudget>,
    /// Optional image fallback behavior applied to *LLM requests only* (never persisted).
    ///
    /// This is intended for text-only provider paths where image parts must be degraded
    /// (placeholder / OCR / error) without leaking into stored session history or UI.
    pub image_fallback: Option<ImageFallbackConfig>,
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
            attachment_reader: None,
            metrics_collector: None,
            model_name: None,
            reasoning_effort: None,
            token_budget: None,
            image_fallback: None,
        }
    }
}

#[cfg(test)]
mod tests;
