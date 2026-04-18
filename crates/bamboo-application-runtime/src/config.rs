use std::collections::BTreeSet;
use std::sync::Arc;

use bamboo_application_memory::budget::TokenBudget;
use bamboo_application_agent::composition::CompositionExecutor;
use bamboo_application_agent::storage::AttachmentReader;
use bamboo_application_agent::storage::Storage;
use bamboo_application_agent::tools::ToolSchema;
use bamboo_application_metrics::MetricsCollector;
use bamboo_application_skills::SkillManager;
use bamboo_application_tools::ToolRegistry;
use bamboo_infrastructure_config::MemoryConfig;
use bamboo_shared_types::ReasoningEffort;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromptMemoryFlags {
    pub project_prompt_injection: bool,
    pub relevant_recall: bool,
    pub relevant_recall_rerank: bool,
    pub project_first_dream: bool,
}

impl Default for PromptMemoryFlags {
    fn default() -> Self {
        Self {
            project_prompt_injection: true,
            relevant_recall: true,
            relevant_recall_rerank: false,
            project_first_dream: true,
        }
    }
}

impl From<&MemoryConfig> for PromptMemoryFlags {
    fn from(value: &MemoryConfig) -> Self {
        Self {
            project_prompt_injection: value.project_prompt_injection,
            relevant_recall: value.relevant_recall,
            relevant_recall_rerank: value.relevant_recall_rerank,
            project_first_dream: value.project_first_dream,
        }
    }
}

/// Configuration for the agent loop.
pub struct AgentLoopConfig {
    pub max_rounds: usize,
    pub system_prompt: Option<String>,
    /// Skill IDs that are disabled globally for this execution.
    pub disabled_skill_ids: BTreeSet<String>,
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
    /// Optional explicit fast/cheap model name for lightweight foreground tasks
    /// such as task evaluation.
    ///
    /// Call sites may fall back to `model_name` when this is unset.
    pub fast_model_name: Option<String>,
    /// Dedicated background summarization model for host-side context compression
    /// and other non-interactive maintenance work.
    ///
    /// Unlike `fast_model_name`, this must not silently fall back to the main
    /// interaction model.
    pub background_model_name: Option<String>,
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
    /// Feature flags controlling prompt-time memory injection behavior.
    pub prompt_memory_flags: PromptMemoryFlags,
}

impl Default for AgentLoopConfig {
    fn default() -> Self {
        Self {
            max_rounds: 200,
            system_prompt: None,
            disabled_skill_ids: BTreeSet::new(),
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
            background_model_name: None,
            provider_name: None,
            reasoning_effort: None,
            disabled_tools: BTreeSet::new(),
            token_budget: None,
            image_fallback: None,
            prompt_memory_flags: PromptMemoryFlags::default(),
        }
    }
}

#[cfg(test)]
mod tests;
