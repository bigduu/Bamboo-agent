use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use crate::metrics::MetricsCollector;
use crate::skills::SkillManager;
use bamboo_agent_core::composition::CompositionExecutor;
use bamboo_agent_core::storage::AttachmentReader;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::GoldConfidence;
use bamboo_compression::TokenBudget;
use bamboo_domain::ReasoningEffort;
use bamboo_domain::RuntimeSessionPersistence;
use bamboo_infrastructure::config::PermissionMode;
use bamboo_infrastructure::LLMProvider;
use bamboo_infrastructure::MemoryConfig;
use bamboo_tools::ToolRegistry;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
pub struct AuxiliaryModelConfig {
    pub fast_model_name: Option<String>,
    pub fast_model_provider: Option<Arc<dyn LLMProvider>>,
    pub background_model_name: Option<String>,
    pub planning_model_name: Option<String>,
    pub search_model_name: Option<String>,
    pub summarization_model_name: Option<String>,
    pub background_model_provider: Option<Arc<dyn LLMProvider>>,
    pub summarization_model_provider: Option<Arc<dyn LLMProvider>>,
}

fn default_gold_max_output_tokens() -> u32 {
    1024
}

fn default_gold_max_auto_continuations() -> u32 {
    3
}

fn default_gold_min_confidence() -> GoldConfidence {
    GoldConfidence::Medium
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct GoldConfig {
    /// Master switch for Gold observe-only evaluation.
    #[serde(default)]
    pub enabled: bool,
    /// Independent switch for Phase 2 low-risk auto-answer.
    ///
    /// Kept separate from `enabled` so Phase 1 observe-only users do not
    /// implicitly opt into automatic clarification responses.
    #[serde(default)]
    pub auto_answer_enabled: bool,
    /// Independent switch for Phase 3 server-side auto-continue.
    ///
    /// Kept separate from both `enabled` and `auto_answer_enabled` so users can
    /// opt into terminal auto-resume explicitly without enabling other Gold
    /// automation behaviors.
    #[serde(default)]
    pub auto_continue_enabled: bool,
    /// Optional dedicated model for Gold evaluation. Falls back to fast model,
    /// then the main chat model when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
    /// The user's goal for this session.
    ///
    /// Unlike `evaluation_prompt` (which only tunes the *judge*), the goal is
    /// surfaced to the *main* executing agent as a persistent system-prompt
    /// block so it actively works toward it. The Gold evaluator also measures
    /// progress against this text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<String>,
    /// Optional custom prompt suffix appended to the built-in Gold evaluator
    /// prompt. This tunes the judge only; it does not set the goal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation_prompt: Option<String>,
    /// Output token limit for the Gold evaluator call.
    #[serde(default = "default_gold_max_output_tokens")]
    pub max_output_tokens: u32,
    /// Maximum number of automatic Gold continuations allowed per session.
    #[serde(default = "default_gold_max_auto_continuations")]
    pub max_auto_continuations: u32,
    /// Minimum evaluator confidence required before Gold auto-continues or
    /// auto-answers. Defaults to `medium` so the loop fires on reasonably
    /// confident verdicts rather than only `high`.
    #[serde(default = "default_gold_min_confidence")]
    pub min_auto_continue_confidence: GoldConfidence,
}

impl Default for GoldConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            auto_answer_enabled: false,
            auto_continue_enabled: false,
            model_name: None,
            goal: None,
            evaluation_prompt: None,
            max_output_tokens: default_gold_max_output_tokens(),
            max_auto_continuations: default_gold_max_auto_continuations(),
            min_auto_continue_confidence: default_gold_min_confidence(),
        }
    }
}

impl GoldConfig {
    /// The session goal text, falling back to the legacy `evaluation_prompt`
    /// for sessions created before the dedicated `goal` field existed.
    ///
    /// Returns `None` when neither field holds non-empty text.
    pub fn effective_goal(&self) -> Option<&str> {
        self.goal
            .as_deref()
            .or(self.evaluation_prompt.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
}

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
#[non_exhaustive]
pub struct AgentLoopConfig {
    pub(crate) max_rounds: usize,
    pub(crate) system_prompt: Option<String>,
    /// Skill IDs that are disabled globally for this execution.
    pub(crate) disabled_skill_ids: BTreeSet<String>,
    /// Optional explicit skill selection for this execution.
    /// When set, only these skill IDs are considered for skill context and allowlists.
    pub(crate) selected_skill_ids: Option<Vec<String>>,
    /// Optional active skill mode for this execution.
    ///
    /// When set, skill discovery prefers `skills-<mode>` directories over generic
    /// directories for the same skill id.
    pub(crate) selected_skill_mode: Option<String>,
    pub(crate) additional_tool_schemas: Vec<ToolSchema>,
    pub(crate) tool_registry: Arc<ToolRegistry>,
    pub(crate) composition_executor: Option<Arc<CompositionExecutor>>,
    pub(crate) skill_manager: Option<Arc<SkillManager>>,
    /// If true, skip appending the initial user message (already present in session).
    pub(crate) skip_initial_user_message: bool,
    /// Optional storage for persisting session changes
    pub(crate) storage: Option<Arc<dyn Storage>>,
    /// Optional runtime persistence for non-authoritative session saves.
    /// When set, engine save sites use this instead of `storage` for writes.
    pub(crate) persistence: Option<Arc<dyn RuntimeSessionPersistence>>,
    /// Optional attachment reader for resolving `bamboo-attachment://...` references
    /// into `data:` URLs for upstream providers. This must not mutate session storage.
    pub(crate) attachment_reader: Option<Arc<dyn AttachmentReader>>,
    /// Optional asynchronous metrics collector
    pub(crate) metrics_collector: Option<MetricsCollector>,
    /// Model name used for metrics attribution
    pub(crate) model_name: Option<String>,
    /// Fast/cheap model for lightweight tasks (task evaluation, search, etc.).
    ///
    /// Call sites may fall back to `model_name` when this is unset.
    pub(crate) fast_model_name: Option<String>,
    /// Optional provider override for lightweight fast-model LLM calls.
    pub(crate) fast_model_provider: Option<Arc<dyn LLMProvider>>,
    /// Fast/cheap model for memory/background tasks.
    ///
    /// This must not silently fall back to the main interaction model.
    pub(crate) background_model_name: Option<String>,

    /// Model for planning/coordination tasks (task decomposition, architecture).
    /// Falls back to `model_name` when unset.
    pub(crate) planning_model_name: Option<String>,
    /// Model for search/navigation tasks (grep, file listing, symbol resolution).
    /// Falls back to `fast_model_name` when unset.
    pub(crate) search_model_name: Option<String>,
    /// Custom instructions for conversation summarization, injected into the
    /// LLM summary prompt. Lets users control what the summary focuses on.
    ///
    /// Resolution order: session-level > config-level > built-in defaults.
    pub(crate) compression_instructions: Option<String>,
    /// Dedicated model for summarization. Falls back to `background_model_name`.
    pub(crate) summarization_model_name: Option<String>,
    /// Optional provider override for memory/background model LLM calls.
    ///
    /// When set, memory recall rerank and other memory/background tasks use this
    /// provider instead of the shared agent loop provider.
    pub(crate) background_model_provider: Option<Arc<dyn LLMProvider>>,
    /// Optional provider override for summarization / context compression calls.
    ///
    /// When set, conversation/task summarization uses this provider instead of
    /// the shared agent loop provider.
    pub(crate) summarization_model_provider: Option<Arc<dyn LLMProvider>>,
    /// Provider routing key used for provider-specific request behavior.
    ///
    /// In multi-instance mode this may be the instance id.
    pub(crate) provider_name: Option<String>,
    /// Underlying provider type (for example `openai`, `anthropic`, `copilot`).
    ///
    /// This is distinct from `provider_name` so provider-specific behavior can
    /// remain correct when routing keys are instance ids.
    pub(crate) provider_type: Option<String>,
    /// Optional request-time reasoning effort override.
    pub(crate) reasoning_effort: Option<ReasoningEffort>,
    /// Bamboo application data directory (typically `~/.bamboo`).
    ///
    /// Used by runtime features that persist auxiliary artifacts outside the
    /// session store, such as durable plan mode files under `~/.bamboo/plan`.
    pub(crate) app_data_dir: Option<PathBuf>,
    /// Tool names that should be excluded from schemas sent to the LLM.
    pub(crate) disabled_tools: BTreeSet<String>,
    /// Token budget for context management (optional, defaults to model's limits)
    pub(crate) token_budget: Option<TokenBudget>,
    /// Optional image fallback behavior applied to *LLM requests only* (never persisted).
    ///
    /// This is intended for text-only provider paths where image parts must be degraded
    /// (placeholder / OCR / error) without leaking into stored session history or UI.
    pub(crate) image_fallback: Option<ImageFallbackConfig>,
    /// Feature flags controlling prompt-time memory injection behavior.
    pub(crate) prompt_memory_flags: PromptMemoryFlags,
    /// Maximum tool calls allowed per round (default: 80).
    pub(crate) max_tool_calls_per_round: usize,
    /// Maximum consecutive failures per tool before circuit breaker (default: 3).
    pub(crate) max_consecutive_failures_per_tool: usize,
    /// Tool names that require strict argument validation.
    pub(crate) strict_argument_tool_names: Vec<String>,
    /// Per-tool execution timeout in seconds (default: 120).
    pub(crate) per_tool_timeout_secs: u64,
    /// Parallel batch execution timeout in seconds (default: 300).
    pub(crate) parallel_batch_timeout_secs: u64,
    /// Permission mode for this execution (default: None = use PermissionConfig's mode).
    pub(crate) permission_mode: Option<PermissionMode>,
    /// Optional Gold observe-only evaluator configuration.
    ///
    /// When `None` or `enabled == false`, Gold evaluation is disabled and the
    /// existing execute/respond/resume loop remains unchanged.
    pub(crate) gold_config: Option<GoldConfig>,
    /// Enable dynamic per-round model routing based on task complexity.
    /// When true, the pipeline classifies complexity at each round end and
    /// stores the result in session metadata.
    pub(crate) features_dynamic_model_routing: bool,
    /// Optional per-round resolver for auxiliary model settings that should
    /// follow live global config rather than stay frozen for the whole run.
    ///
    /// The main chat model remains session/request scoped; this hook is only
    /// for fast/background/planning/search/summarization helpers.
    pub(crate) auxiliary_model_resolver:
        Option<Arc<dyn Fn() -> AuxiliaryModelConfig + Send + Sync>>,
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
            persistence: None,
            attachment_reader: None,
            metrics_collector: None,
            model_name: None,
            fast_model_name: None,
            fast_model_provider: None,
            background_model_name: None,
            planning_model_name: None,
            search_model_name: None,
            compression_instructions: None,
            summarization_model_name: None,
            background_model_provider: None,
            summarization_model_provider: None,
            provider_name: None,
            provider_type: None,
            reasoning_effort: None,
            app_data_dir: None,
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
                "SubAgent".into(),
                "scheduler".into(),
                "sub_session_manager".into(),
                "session_note".into(),
                "memory_note".into(),
            ],
            per_tool_timeout_secs: 120,
            parallel_batch_timeout_secs: 300,
            permission_mode: None,
            gold_config: None,
            features_dynamic_model_routing: false,
            auxiliary_model_resolver: None,
        }
    }
}

impl AgentLoopConfig {
    /// The active session goal to surface to the main agent, or `None` when
    /// Gold is disabled or no goal is set. Falls back to the legacy
    /// `evaluation_prompt` for back-compat via [`GoldConfig::effective_goal`].
    pub fn active_goal(&self) -> Option<&str> {
        self.gold_config
            .as_ref()
            .filter(|cfg| cfg.enabled)
            .and_then(GoldConfig::effective_goal)
    }
}

#[cfg(test)]
mod tests;
