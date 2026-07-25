use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use bamboo_agent_core::storage::AttachmentReader;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::GoldConfidence;
use bamboo_compression::TokenBudget;
use bamboo_config::MemoryConfig;
use bamboo_config::PermissionMode;
use bamboo_domain::ReasoningEffort;
use bamboo_domain::RuntimeSessionPersistence;
use bamboo_llm::LLMProvider;
use bamboo_metrics::MetricsCollector;
use bamboo_skills::SkillManager;
use bamboo_tools::ToolRegistry;
use serde::{Deserialize, Serialize};

use super::hooks::HookRunner;

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

fn default_guardian_max_reviews() -> u32 {
    2
}

/// Configuration for the guardian adversarial-review terminal gate.
///
/// Mirrors [`GoldConfig`]: a plain, serde-defaulting struct surfaced per run.
/// When `enabled` is false (the default) the guardian gate is inactive and the
/// terminal completion path is unchanged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct GuardianConfig {
    /// Master switch for the guardian review gate.
    #[serde(default)]
    pub enabled: bool,
    /// Optional dedicated reviewer model. Falls back to the run's main model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
    /// Maximum guardian review passes per run (budget; mirrors
    /// [`GoldConfig::max_auto_continuations`]).
    #[serde(default = "default_guardian_max_reviews")]
    pub max_reviews: u32,
}

impl Default for GuardianConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            model_name: None,
            max_reviews: default_guardian_max_reviews(),
        }
    }
}

/// Late-bound spawner for the guardian reviewer child.
///
/// The runner cannot construct a child directly: the `SpawnScheduler` is built
/// *after* the `Agent` that drives the runner (a construction-order cycle), so
/// the terminal gate spawns the reviewer through this trait object, injected
/// per-request on [`AgentLoopConfig`] exactly like `auxiliary_model_resolver`.
/// The implementation lives in the server (it captures the already-built
/// scheduler + child-session adapter); the engine holds only the trait, keeping
/// the engine free of any dependency on server/AppState types.
#[async_trait::async_trait]
pub trait GuardianSpawner: Send + Sync {
    /// Create a read-only reviewer child for `parent_session_id`, seeded with
    /// `review_prompt`, enqueue it to run, and return its session id so the
    /// caller can register a wait on it.
    async fn spawn_guardian_review(
        &self,
        parent_session: &bamboo_agent_core::Session,
        review_prompt: String,
        model: String,
        disabled_tools: Option<BTreeSet<String>>,
    ) -> Result<String, String>;
}

/// Hidden resume-message `runtime_kind` metadata value for a bash-completion
/// self-resume (issue #84 Phase 2b). Shared by the producer (the self-resume
/// task that appends the resume message) and the consumer (the suspend-
/// finalization discriminant arm that preserves it), so a typo in one cannot
/// desync from the other and silently drop the resume trigger.
pub const BASH_COMPLETION_RESUME_KIND: &str = "bash_completion_resume";

/// Re-exported so peers that already `use crate::runtime::config::{BashResumeHook, …}`
/// (the runtime/spawn threading) can name the completion sink the same way,
/// rather than reaching into `bamboo_agent_core` separately.
pub use bamboo_agent_core::BashCompletionSink;

/// Late-bound hook that arranges a self-resume for a session suspended waiting
/// on background Bash shells (issue #84 Phase 2b). Injected per-request on
/// [`AgentLoopConfig`] exactly like [`GuardianSpawner`]; the implementation
/// lives in the session-app layer (on the completion coordinator) where the
/// resume port ([`crate::session_app::resume::ResumeExecutionPort`]) is
/// reachable.
///
/// The hook spawns a detached task that **polls the live background-shell
/// registry** until every captured shell is no longer running, then clears the
/// wait and resumes the session. Polling — not the one-shot `BashCompleted`
/// event — is the liveness guarantee: even if a shell completes between the
/// suspend snapshot and the hook's first poll, or before any event subscriber
/// exists, the registry will report it as not-running and the session resumes.
pub trait BashResumeHook: Send + Sync {
    /// Arrange a detached self-resume for `session_id`, which has just been
    /// durably suspended waiting on the background shells in `bash_ids`.
    fn arrange_bash_self_resume(&self, session_id: String, bash_ids: Vec<String>);
}

/// A child sub-agent's request to have a gated tool approved by its parent.
///
/// A non-bypassed child cannot answer its own permission prompt (no human is
/// attached to a child session), so the request is delegated up to the parent.
#[derive(Debug, Clone)]
pub struct ChildApprovalRequest {
    pub child_session_id: String,
    pub parent_session_id: String,
    /// The gated tool call on the child to re-execute once approved.
    pub child_tool_call_id: String,
    pub tool_name: String,
    /// Permission type as a string (e.g. "WriteFile", "ExecuteCommand").
    pub permission_type: String,
    /// The concrete resource the permission applies to (path, command, …).
    pub resource: String,
    /// Human-facing approval question to surface on the parent.
    pub question: String,
    /// The raw `awaiting_permission_approval` payload the child's executor built,
    /// so the parent can reuse the existing grant-extraction path verbatim.
    pub approval_payload: serde_json::Value,
}

/// What the executor should do after delegating a child's approval upward.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildApprovalOutcome {
    /// Registered on the parent; the child must SUSPEND and await the decision.
    Delegated,
    /// Parent policy auto-approved (bypass / existing grant); proceed to execute.
    AutoApproved,
    /// Parent policy auto-denied; the executor must deny the tool.
    AutoDenied,
}

/// Late-bound delegate that routes a child's approval request up to its parent.
///
/// Injected per-request on [`AgentLoopConfig`] exactly like [`GuardianSpawner`];
/// the trait lives in the engine, the implementation in the server (it owns the
/// parent session store + pending-question + notification machinery).
#[async_trait::async_trait]
pub trait ApprovalDelegate: Send + Sync {
    /// Register `request` on its parent (or auto-resolve by policy) and report
    /// what the child's executor should do next.
    async fn delegate_child_approval(
        &self,
        request: ChildApprovalRequest,
    ) -> Result<ChildApprovalOutcome, String>;
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
    pub ledger_agenda: bool,
}

impl Default for PromptMemoryFlags {
    fn default() -> Self {
        Self {
            project_prompt_injection: true,
            relevant_recall: true,
            relevant_recall_rerank: false,
            project_first_dream: true,
            ledger_agenda: true,
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
            ledger_agenda: value.ledger_agenda_injection,
        }
    }
}

/// Configuration for the agent loop.
///
/// # One-config-per-run invariant (#44)
///
/// These values are SNAPSHOTTED once, from the live `Config` under a brief read
/// lock, at the start of `AgentRuntime::execute()`. The entire multi-round run —
/// which can last minutes — then uses this frozen snapshot. Changing config
/// (model names, provider, `disabled_tools`/`disabled_skills`, memory flags,
/// token budget, …) while a run is in flight does NOT affect that run; the new
/// values are picked up on the NEXT execution (i.e. the next user turn / session
/// restart), not the next round of the current run.
///
/// This is intentional: a run sees a stable configuration, so its behavior can't
/// shift underneath it mid-execution. The deliberate exceptions are the
/// late-bound, per-request trait objects that resolve LIVE each time they're
/// used rather than being snapshotted — `auxiliary_model_resolver` (auxiliary
/// model selection) and `guardian_spawner` (the reviewer child). If a frozen
/// field ever needs to become live-per-round, follow that resolver pattern
/// rather than widening the snapshot.
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
    pub(crate) skill_manager: Option<Arc<SkillManager>>,
    /// Stable Project identity/resource resolver. The server wires the
    /// authoritative registry adapter once on `AgentRuntimeBuilder`.
    pub(crate) project_context_resolver:
        Option<Arc<crate::project_context::ProjectContextResolver>>,
    /// If true, skip appending the initial user message (already present in session).
    pub(crate) skip_initial_user_message: bool,
    /// Optional storage for persisting session changes
    pub(crate) storage: Option<Arc<dyn Storage>>,
    /// Optional runtime persistence for non-authoritative session saves.
    /// When set, engine save sites use this instead of `storage` for writes.
    pub(crate) persistence: Option<Arc<dyn RuntimeSessionPersistence>>,
    /// Durable logical-session inbox admitted at safe round boundaries.
    pub(crate) session_inbox: Option<Arc<dyn bamboo_domain::SessionInboxPort>>,
    /// Active-owner wake generation. The loop consumes this at the same safe
    /// boundary where it drains SessionInbox; it never interrupts an in-flight
    /// provider/tool operation.
    pub(crate) session_activation_notifications:
        Option<Arc<parking_lot::Mutex<tokio::sync::watch::Receiver<u64>>>>,
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
    /// Legacy `config.json` `model_limits` value, snapshotted from the live
    /// in-memory Config when this loop config is built. Consulted only by
    /// `resolve_token_budget` as a last-resort fallback when `model_limits.json`
    /// fails to load — so the engine never does a fresh disk-reading
    /// `Config::new()` (which would also clobber the global env-var cache). #38.
    pub(crate) legacy_model_limits: Option<serde_json::Value>,
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
    /// Per-tool execution timeout in seconds (default: 120).
    pub(crate) per_tool_timeout_secs: u64,
    /// Parallel batch execution timeout in seconds (default: 300).
    pub(crate) parallel_batch_timeout_secs: u64,
    /// Resolved LLM stream transport/semantic watchdog policy. The same value
    /// is passed to main response streams and auxiliary silent streams.
    pub(crate) stream_timeout: bamboo_config::StreamTimeoutConfig,
    /// Permission mode for this execution (default: None = use PermissionConfig's mode).
    pub(crate) permission_mode: Option<PermissionMode>,
    /// Optional Gold observe-only evaluator configuration.
    ///
    /// When `None` or `enabled == false`, Gold evaluation is disabled and the
    /// existing execute/respond/resume loop remains unchanged.
    pub(crate) gold_config: Option<GoldConfig>,
    /// Optional guardian adversarial-review gate configuration. When `None` or
    /// `enabled == false`, the guardian terminal gate is inactive.
    pub(crate) guardian_config: Option<GuardianConfig>,
    /// Late-bound spawner for the guardian reviewer child. `None` (the default)
    /// leaves the guardian gate inert even when `guardian_config.enabled` is set,
    /// since the runner cannot create a child without it. Wired by the server.
    pub(crate) guardian_spawner: Option<Arc<dyn GuardianSpawner>>,
    /// Late-bound hook that arranges a self-resume for a session suspended
    /// waiting on background Bash shells (issue #84 Phase 2b). `None` (the
    /// default) leaves the bash suspend gate inert: the gate refuses to suspend
    /// without a wired hook, so a session can never strand itself without a
    /// resume path. Wired by the server (the completion coordinator impl).
    pub(crate) bash_resume_hook: Option<Arc<dyn BashResumeHook>>,
    /// Late-bound sink that pushes a completed background Bash shell's result
    /// into this session's loop (issue #84 Phase 2b follow-up) — injected at the
    /// next round boundary while the loop is actively iterating, or delivered via
    /// resume when it is idle. Threaded onto the tool dispatch context (like
    /// `can_async_resume`) so the Bash tool can hand it to the shell's
    /// completion-poll task. `None` (the default) leaves the push inert; the
    /// durable end-of-turn suspend/poll backstop (`bash_resume_hook`) still runs.
    /// Wired by the server (the completion coordinator impl).
    pub(crate) bash_completion_sink: Option<Arc<dyn bamboo_agent_core::BashCompletionSink>>,
    /// Late-bound delegate that routes a child's gated-tool approval request up
    /// to its parent (Phase 2). `None` (the default) leaves child gating on its
    /// legacy path. Wired by the server.
    pub(crate) approval_delegate: Option<Arc<dyn ApprovalDelegate>>,
    /// Frozen lifecycle-hook registry for this run. The default registry is
    /// empty, and every seam checks `has_hooks_for` before constructing payloads.
    pub(crate) hook_runner: Arc<HookRunner>,
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
    /// Optional per-round resolver for the disabled tool/skill sets so they follow
    /// LIVE global config instead of staying frozen for the whole run. Returns the
    /// current `(disabled_tools, disabled_skill_ids)`. When `None`, the snapshotted
    /// `disabled_tools` / `disabled_skill_ids` fields below are used (#44 behavior).
    /// Re-resolved each round at the tool-schema filter, so disabling/re-enabling a
    /// tool mid-run takes effect on the next round. #136.
    pub(crate) disabled_filter_resolver:
        Option<Arc<dyn Fn() -> (BTreeSet<String>, BTreeSet<String>) + Send + Sync>>,
    /// Server-level usage guidance contributed by the run's tool executor —
    /// chiefly the `instructions` connected MCP servers return from `initialize`.
    /// Captured once at config construction (from `ToolExecutor::tool_guidance`)
    /// and appended to the tool-guide section of the system prompt, so a server's
    /// own how-to-use notes appear only while that server is loaded for the run.
    pub(crate) mcp_tool_guidance: Option<String>,
    /// Per-run resource guardrails (issue #221): already resolved — the
    /// per-request override merged over the config-level default (see
    /// [`AgentRuntime::execute`](crate::runtime::runtime::AgentRuntime::execute)).
    /// Checked after every round; exceeding a configured limit gracefully
    /// stops the run (mirrors the `max_rounds` exhaustion path).
    pub(crate) run_budget: bamboo_config::RunBudgetConfig,
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
            skill_manager: None,
            project_context_resolver: None,
            skip_initial_user_message: false,
            storage: None,
            persistence: None,
            session_inbox: None,
            session_activation_notifications: None,
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
            legacy_model_limits: None,
            image_fallback: None,
            prompt_memory_flags: PromptMemoryFlags::default(),
            max_tool_calls_per_round: 80,
            max_consecutive_failures_per_tool: 3,
            per_tool_timeout_secs: 120,
            parallel_batch_timeout_secs: 300,
            stream_timeout: bamboo_config::StreamTimeoutConfig::default(),
            permission_mode: None,
            gold_config: None,
            guardian_config: None,
            guardian_spawner: None,
            bash_resume_hook: None,
            bash_completion_sink: None,
            approval_delegate: None,
            hook_runner: Arc::new(HookRunner::new()),
            features_dynamic_model_routing: false,
            auxiliary_model_resolver: None,
            disabled_filter_resolver: None,
            mcp_tool_guidance: None,
            run_budget: bamboo_config::RunBudgetConfig::default(),
        }
    }
}

impl AgentLoopConfig {
    /// Live `(disabled_tools, disabled_skill_ids)` for the current round: the
    /// resolver if one is wired (#136 — follows live global config between
    /// rounds), else the per-run snapshot (#44 frozen behavior). `Cow` avoids
    /// cloning the snapshot in the common no-resolver path (SDK / tests).
    pub(crate) fn resolve_disabled_filters(
        &self,
    ) -> (
        std::borrow::Cow<'_, BTreeSet<String>>,
        std::borrow::Cow<'_, BTreeSet<String>>,
    ) {
        match &self.disabled_filter_resolver {
            Some(resolver) => {
                let (tools, skills) = resolver();
                (
                    std::borrow::Cow::Owned(tools),
                    std::borrow::Cow::Owned(skills),
                )
            }
            None => (
                std::borrow::Cow::Borrowed(&self.disabled_tools),
                std::borrow::Cow::Borrowed(&self.disabled_skill_ids),
            ),
        }
    }

    /// The active session goal to surface to the main agent, or `None` when
    /// Gold is disabled or no goal is set. Falls back to the legacy
    /// `evaluation_prompt` for back-compat via [`GoldConfig::effective_goal`].
    pub fn active_goal(&self) -> Option<&str> {
        self.gold_config
            .as_ref()
            .filter(|cfg| cfg.enabled)
            .and_then(GoldConfig::effective_goal)
    }

    /// Whether the Codex-style autonomous goal loop is active for this run.
    ///
    /// This requires Gold to be enabled, a goal to be set, AND auto-continue to
    /// be on. Only then is the `update_goal` self-report tool surfaced to the
    /// model and the terminal double-check allowed to veto a premature stop.
    /// When Gold is enabled without auto-continue, the evaluator stays purely
    /// observational (legacy behavior).
    pub fn goal_loop_active(&self) -> bool {
        self.gold_config.as_ref().is_some_and(|cfg| {
            cfg.enabled && cfg.auto_continue_enabled && cfg.effective_goal().is_some()
        })
    }

    /// Whether the guardian review gate is active for this run: a spawner is
    /// wired (so the runner can actually create the reviewer child) AND the
    /// config is present and enabled.
    pub fn guardian_active(&self) -> bool {
        self.guardian_spawner.is_some()
            && self.guardian_config.as_ref().is_some_and(|cfg| cfg.enabled)
    }

    /// Maximum guardian review passes for this run (the budget). `0` when no
    /// guardian config is set.
    pub fn guardian_max_reviews(&self) -> u32 {
        self.guardian_config
            .as_ref()
            .map_or(0, |cfg| cfg.max_reviews)
    }

    /// The reviewer model override, if a guardian config sets one.
    pub fn guardian_model(&self) -> Option<&str> {
        self.guardian_config
            .as_ref()
            .and_then(|cfg| cfg.model_name.as_deref())
    }

    /// Whether child→parent approval delegation is wired for this run.
    pub fn delegation_active(&self) -> bool {
        self.approval_delegate.is_some()
    }
}

#[cfg(test)]
mod tests;
