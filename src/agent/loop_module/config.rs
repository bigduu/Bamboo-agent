use std::collections::BTreeSet;
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
    /// Use a vision-capable LLM to describe the image, then replace the image
    /// with the textual description so that text-only models can understand
    /// the content.
    Vision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageFallbackConfig {
    pub mode: ImageFallbackMode,
    /// Vision model name for `Vision` mode. Falls back to the session's main model
    /// when `None`.
    pub vision_model: Option<String>,
}

/// Configuration for the agent loop.
pub struct AgentLoopConfig {
    pub max_rounds: usize,
    pub system_prompt: Option<String>,
    /// Optional explicit skill selection for this execution.
    /// When set, only these skill IDs are considered for skill context and allowlists.
    pub selected_skill_ids: Option<Vec<String>>,
    /// Optional active skill mode for this execution.
    ///
    /// When set, skill discovery prefers `skills-<mode>` directories over generic
    /// directories for the same skill id.
    pub selected_skill_mode: Option<String>,
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
    /// Fast/cheap model name for lightweight tasks (summarization, task evaluation).
    /// Falls back to `model_name` when not set.
    pub fast_model_name: Option<String>,
    /// Provider name used for provider-specific request behavior.
    pub provider_name: Option<String>,
    /// Optional request-time reasoning effort override.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Tool names that should be excluded from schemas sent to the LLM.
    pub disabled_tools: BTreeSet<String>,
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
            max_rounds: 200,
            system_prompt: None,
            selected_skill_ids: None,
            selected_skill_mode: None,
            additional_tool_schemas: Vec::new(),
            tool_registry: Arc::new(ToolRegistry::new()),
            composition_executor: None,
            skill_manager: None,
            skip_initial_user_message: false,
            storage: None,
            attachment_reader: None,
            metrics_collector: None,
            model_name: None,
            fast_model_name: None,
            provider_name: None,
            reasoning_effort: None,
            disabled_tools: BTreeSet::new(),
            token_budget: None,
            image_fallback: None,
        }
    }
}

#[cfg(test)]
mod tests;
