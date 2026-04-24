use std::collections::BTreeSet;
use std::sync::Arc;

use crate::metrics::MetricsCollector;
use crate::skills::SkillManager;
use bamboo_agent_core::composition::CompositionExecutor;
use bamboo_agent_core::storage::AttachmentReader;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_compression::TokenBudget;
use bamboo_domain::ReasoningEffort;
use bamboo_infrastructure::config::PermissionMode;
use bamboo_infrastructure::LLMProvider;
use bamboo_infrastructure::MemoryConfig;
use bamboo_tools::ToolRegistry;

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
    /// Model for planning/coordination tasks (task decomposition, architecture).
    /// Falls back to `model_name` when unset.
    pub planning_model_name: Option<String>,
    /// Model for search/navigation tasks (grep, file listing, symbol resolution).
    /// Falls back to `fast_model_name` when unset.
    pub search_model_name: Option<String>,
    /// Custom instructions for conversation summarization, injected into the
    /// LLM summary prompt. Lets users control what the summary focuses on.
    ///
    /// Resolution order: session-level > config-level > built-in defaults.
    pub compression_instructions: Option<String>,
    /// Dedicated model for summarization. Falls back to `background_model_name`.
    pub summarization_model_name: Option<String>,
    /// Optional provider override for background/fast model LLM calls.
    ///
    /// When set, context compression, summarization, and other background
    /// model calls use this provider instead of the shared agent loop provider.
    pub background_model_provider: Option<Arc<dyn LLMProvider>>,
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
    /// Maximum tool calls allowed per round (default: 80).
    pub max_tool_calls_per_round: usize,
    /// Maximum consecutive failures per tool before circuit breaker (default: 3).
    pub max_consecutive_failures_per_tool: usize,
    /// Tool names that require strict argument validation.
    pub strict_argument_tool_names: Vec<String>,
    /// Per-tool execution timeout in seconds (default: 120).
    pub per_tool_timeout_secs: u64,
    /// Parallel batch execution timeout in seconds (default: 300).
    pub parallel_batch_timeout_secs: u64,
    /// Permission mode for this execution (default: None = use PermissionConfig's mode).
    pub permission_mode: Option<PermissionMode>,
    /// Enable dynamic per-round model routing based on task complexity.
    /// When true, the pipeline classifies complexity at each round end and
    /// stores the result in session metadata.
    pub features_dynamic_model_routing: bool,
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
            planning_model_name: None,
            search_model_name: None,
            compression_instructions: None,
            summarization_model_name: None,
            background_model_provider: None,
            provider_name: None,
            reasoning_effort: None,
            disabled_tools: BTreeSet::new(),
            token_budget: None,
            image_fallback: None,
            prompt_memory_flags: PromptMemoryFlags::default(),
            max_tool_calls_per_round: 80,
            max_consecutive_failures_per_tool: 3,
            strict_argument_tool_names: vec![
                "Write".into(),
                "Edit".into(),
                "NotebookEdit".into(),
                "apply_patch".into(),
                "Bash".into(),
                "Task".into(),
                "SubSession".into(),
                "scheduler".into(),
                "sub_session_manager".into(),
                "session_note".into(),
                "memory_note".into(),
            ],
            per_tool_timeout_secs: 120,
            parallel_batch_timeout_secs: 300,
            permission_mode: None,
            features_dynamic_model_routing: false,
        }
    }
}

#[cfg(test)]
mod tests;
