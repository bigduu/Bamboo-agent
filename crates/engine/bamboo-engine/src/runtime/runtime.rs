//! Unified agent execution runtime.
//!
//! [`AgentRuntime`] holds shared resources assembled once at server startup,
//! including the LLM provider and default tool executor.
//! [`ExecuteRequest`] captures per-request parameters.  Together they replace
//! the three duplicated `AgentLoopConfig` construction sites in the server
//! layer (HTTP handler, spawn scheduler, schedule manager).

use std::collections::BTreeSet;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::storage::{AttachmentReader, Storage};
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Role, Session};
use bamboo_config::PermissionMode;
use bamboo_domain::ReasoningEffort;
use bamboo_llm::Config;
use bamboo_llm::LLMProvider;
use bamboo_metrics::MetricsCollector;
use bamboo_skills::SkillManager;

use crate::runtime::config::{
    AgentLoopConfig, AuxiliaryModelConfig, GoldConfig, ImageFallbackConfig, PromptMemoryFlags,
};
use crate::runtime::hooks::HookRunner;
use crate::runtime::managers::{
    LifecycleManager, LlmManager, MemoryManager, PromptManager, ToolManager,
};
use crate::runtime::model_roster::{ModelRoster, RoleModel};
use crate::runtime::runner::run_agent_loop_with_config;
use bamboo_domain::RuntimeSessionPersistence;

// ---------------------------------------------------------------------------
// AgentRuntime — shared resources (assembled once)
// ---------------------------------------------------------------------------

/// Shared runtime resources assembled once at server startup.
///
/// Each field is an `Arc` or `Clone`-cheap handle so that `AgentRuntime` itself
/// is cheaply cloneable across tasks.
#[derive(Clone)]
pub struct AgentRuntime {
    pub storage: Arc<dyn Storage>,
    pub persistence: Arc<dyn RuntimeSessionPersistence>,
    pub attachment_reader: Arc<dyn AttachmentReader>,
    pub skill_manager: Arc<SkillManager>,
    pub metrics_collector: MetricsCollector,
    pub config: Arc<RwLock<Config>>,

    /// Reloadable LLM provider handle (delegates to the latest provider).
    pub provider: Arc<dyn LLMProvider>,

    /// Default tool executor (root tools with full surface).
    /// Call sites that need a reduced tool set (child / schedule) pass their
    /// own via `ExecuteRequest::tools`.
    pub default_tools: Arc<dyn ToolExecutor>,

    // -- Optional manager overrides ----------------------------------------
    /// When `Some`, uses a custom prompt manager instead of the default adapter.
    pub prompt_manager: Option<Arc<dyn PromptManager>>,
    /// When `Some`, uses a custom memory manager instead of the default adapter.
    pub memory_manager: Option<Arc<dyn MemoryManager>>,
    /// When `Some`, uses a custom tool manager instead of the default adapter.
    pub tool_manager: Option<Arc<dyn ToolManager>>,
    /// When `Some`, uses a custom LLM manager instead of the default adapter.
    pub llm_manager: Option<Arc<dyn LlmManager>>,
    /// When `Some`, uses a custom lifecycle manager instead of the default adapter.
    pub lifecycle_manager: Option<Arc<dyn LifecycleManager>>,
    /// When `Some`, uses a custom hook runner instead of a no-op runner.
    pub hook_runner: Option<HookRunner>,
}

// ---------------------------------------------------------------------------
// AgentRuntimeBuilder
// ---------------------------------------------------------------------------

/// Builder for [`AgentRuntime`].
///
/// ```
/// # use bamboo_engine::AgentRuntimeBuilder;
/// // In real code all fields are provided by the server assembly.
/// let rt = AgentRuntimeBuilder::new()
///     // .storage(...)
///     // .provider(...)
///     .build();
/// ```
pub struct AgentRuntimeBuilder {
    storage: Option<Arc<dyn Storage>>,
    persistence: Option<Arc<dyn RuntimeSessionPersistence>>,
    attachment_reader: Option<Arc<dyn AttachmentReader>>,
    skill_manager: Option<Arc<SkillManager>>,
    metrics_collector: Option<MetricsCollector>,
    config: Option<Arc<RwLock<Config>>>,
    provider: Option<Arc<dyn LLMProvider>>,
    default_tools: Option<Arc<dyn ToolExecutor>>,
    prompt_manager: Option<Arc<dyn PromptManager>>,
    memory_manager: Option<Arc<dyn MemoryManager>>,
    tool_manager: Option<Arc<dyn ToolManager>>,
    llm_manager: Option<Arc<dyn LlmManager>>,
    lifecycle_manager: Option<Arc<dyn LifecycleManager>>,
    hook_runner: Option<HookRunner>,
}

impl AgentRuntimeBuilder {
    pub fn new() -> Self {
        Self {
            storage: None,
            persistence: None,
            attachment_reader: None,
            skill_manager: None,
            metrics_collector: None,
            config: None,
            provider: None,
            default_tools: None,
            prompt_manager: None,
            memory_manager: None,
            tool_manager: None,
            llm_manager: None,
            lifecycle_manager: None,
            hook_runner: None,
        }
    }

    pub fn storage(mut self, v: Arc<dyn Storage>) -> Self {
        self.storage = Some(v);
        self
    }

    pub fn persistence(mut self, v: Arc<dyn RuntimeSessionPersistence>) -> Self {
        self.persistence = Some(v);
        self
    }

    pub fn attachment_reader(mut self, v: Arc<dyn AttachmentReader>) -> Self {
        self.attachment_reader = Some(v);
        self
    }

    pub fn skill_manager(mut self, v: Arc<SkillManager>) -> Self {
        self.skill_manager = Some(v);
        self
    }

    pub fn metrics_collector(mut self, v: MetricsCollector) -> Self {
        self.metrics_collector = Some(v);
        self
    }

    pub fn config(mut self, v: Arc<RwLock<Config>>) -> Self {
        self.config = Some(v);
        self
    }

    pub fn provider(mut self, v: Arc<dyn LLMProvider>) -> Self {
        self.provider = Some(v);
        self
    }

    pub fn default_tools(mut self, v: Arc<dyn ToolExecutor>) -> Self {
        self.default_tools = Some(v);
        self
    }

    pub fn prompt_manager(mut self, v: Arc<dyn PromptManager>) -> Self {
        self.prompt_manager = Some(v);
        self
    }

    pub fn memory_manager(mut self, v: Arc<dyn MemoryManager>) -> Self {
        self.memory_manager = Some(v);
        self
    }

    pub fn tool_manager(mut self, v: Arc<dyn ToolManager>) -> Self {
        self.tool_manager = Some(v);
        self
    }

    pub fn llm_manager(mut self, v: Arc<dyn LlmManager>) -> Self {
        self.llm_manager = Some(v);
        self
    }

    pub fn lifecycle_manager(mut self, v: Arc<dyn LifecycleManager>) -> Self {
        self.lifecycle_manager = Some(v);
        self
    }

    pub fn hook_runner(mut self, v: HookRunner) -> Self {
        self.hook_runner = Some(v);
        self
    }

    pub fn build(self) -> Result<AgentRuntime, &'static str> {
        Ok(AgentRuntime {
            storage: self.storage.ok_or_else(|| format_missing("storage"))?,
            persistence: self
                .persistence
                .ok_or_else(|| format_missing("persistence"))?,
            attachment_reader: self
                .attachment_reader
                .ok_or_else(|| format_missing("attachment_reader"))?,
            skill_manager: self
                .skill_manager
                .ok_or_else(|| format_missing("skill_manager"))?,
            metrics_collector: self
                .metrics_collector
                .ok_or_else(|| format_missing("metrics_collector"))?,
            config: self.config.ok_or_else(|| format_missing("config"))?,
            provider: self.provider.ok_or_else(|| format_missing("provider"))?,
            default_tools: self
                .default_tools
                .ok_or_else(|| format_missing("default_tools"))?,
            prompt_manager: None,
            memory_manager: None,
            tool_manager: None,
            llm_manager: None,
            lifecycle_manager: None,
            hook_runner: None,
        })
    }
}

fn format_missing(field: &str) -> &'static str {
    // Static strings for the common fields keep the error type `&'static str`.
    // This is good enough for a builder that only runs at startup.
    match field {
        "storage" => "AgentRuntimeBuilder: missing storage",
        "persistence" => "AgentRuntimeBuilder: missing persistence",
        "attachment_reader" => "AgentRuntimeBuilder: missing attachment_reader",
        "skill_manager" => "AgentRuntimeBuilder: missing skill_manager",
        "metrics_collector" => "AgentRuntimeBuilder: missing metrics_collector",
        "config" => "AgentRuntimeBuilder: missing config",
        "provider" => "AgentRuntimeBuilder: missing provider",
        "default_tools" => "AgentRuntimeBuilder: missing default_tools",
        _ => "AgentRuntimeBuilder: missing required field",
    }
}

impl Default for AgentRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ExecuteRequest — per-request parameters
// ---------------------------------------------------------------------------

/// Per-request parameters for agent execution.
///
/// Required fields (`initial_message`, `event_tx`, `cancel_token`) must always
/// be provided.  The provider is taken from [`AgentRuntime::provider`]; tools
/// default to [`AgentRuntime::default_tools`] when `None`.
pub struct ExecuteRequest {
    // -- Required ----------------------------------------------------------
    pub initial_message: String,
    pub event_tx: mpsc::Sender<AgentEvent>,
    pub cancel_token: CancellationToken,

    // -- Tool override -----------------------------------------------------
    /// Override runtime's `default_tools`.  When `None`, uses the runtime's
    /// default tool executor.
    pub tools: Option<Arc<dyn ToolExecutor>>,
    /// Override the LLM provider for this execution. When `None`, uses the
    /// runtime's shared provider handle.
    pub provider_override: Option<Arc<dyn LLMProvider>>,

    // -- Model selection (None roles → config defaults) -------------------
    /// Cohesive primary + auxiliary model/provider selection. Replaces the old
    /// `model` / `provider_name` / `provider_type` / `fast_model(+provider)` /
    /// `background_model(+provider)` / `summarization_model(+provider)` clump.
    /// Per-role `None` preserves the same `None → Config::get_*` fallbacks
    /// applied during [`AgentRuntime::execute`].
    pub model_roster: ModelRoster,
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Optional per-round resolver for auxiliary model settings that should be
    /// re-read from live global config between rounds.
    pub auxiliary_model_resolver: Option<Arc<dyn Fn() -> AuxiliaryModelConfig + Send + Sync>>,
    /// When `None`, falls back to `Config::disabled_tool_names()`.
    pub disabled_tools: Option<BTreeSet<String>>,
    /// When `None`, falls back to `Config::disabled_skill_ids()`.
    pub disabled_skill_ids: Option<BTreeSet<String>>,
    pub selected_skill_ids: Option<Vec<String>>,
    pub selected_skill_mode: Option<String>,
    pub image_fallback: Option<ImageFallbackConfig>,
    pub gold_config: Option<GoldConfig>,
    /// Bamboo application data directory (typically `~/.bamboo`).
    pub app_data_dir: Option<std::path::PathBuf>,
}

// ---------------------------------------------------------------------------
// ExecuteRequestBuilder — ergonomic construction of ExecuteRequest
// ---------------------------------------------------------------------------

/// Fluent builder for [`ExecuteRequest`].
///
/// `ExecuteRequest` carries three required fields plus a long tail of optional
/// overrides; constructing it by hand forces callers to spell out ~20 `None`s.
/// This builder requires only `initial_message` + `event_tx` + `cancel_token`,
/// defaults every optional field to `None` (matching the runtime's spawn
/// defaults), and exposes fluent setters for the rest.
///
/// It lives in `bamboo-engine` so both the in-crate server layer (e.g. the
/// schedule manager) and the root `bamboo_agent` SDK facade (which re-exports
/// it) construct requests through one shared builder — no forked assembly.
pub struct ExecuteRequestBuilder {
    initial_message: String,
    event_tx: mpsc::Sender<AgentEvent>,
    cancel_token: CancellationToken,

    tools: Option<Arc<dyn ToolExecutor>>,
    provider_override: Option<Arc<dyn LLMProvider>>,
    // Individual model fields are accumulated here for backward-compatible
    // fluent setters; `build()` assembles them into a `ModelRoster`. A
    // `.model_roster(..)` setter seeds all of them at once.
    model: Option<String>,
    provider_name: Option<String>,
    provider_type: Option<String>,
    fast_model: Option<String>,
    fast_model_provider: Option<Arc<dyn LLMProvider>>,
    background_model: Option<String>,
    background_model_provider: Option<Arc<dyn LLMProvider>>,
    summarization_model: Option<String>,
    summarization_model_provider: Option<Arc<dyn LLMProvider>>,
    reasoning_effort: Option<ReasoningEffort>,
    auxiliary_model_resolver: Option<Arc<dyn Fn() -> AuxiliaryModelConfig + Send + Sync>>,
    disabled_tools: Option<BTreeSet<String>>,
    disabled_skill_ids: Option<BTreeSet<String>>,
    selected_skill_ids: Option<Vec<String>>,
    selected_skill_mode: Option<String>,
    image_fallback: Option<ImageFallbackConfig>,
    gold_config: Option<GoldConfig>,
    app_data_dir: Option<std::path::PathBuf>,
}

impl ExecuteRequestBuilder {
    /// Create a builder with the three required fields. All optional overrides
    /// default to `None`.
    pub fn new(
        initial_message: impl Into<String>,
        event_tx: mpsc::Sender<AgentEvent>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            initial_message: initial_message.into(),
            event_tx,
            cancel_token,
            tools: None,
            provider_override: None,
            model: None,
            provider_name: None,
            provider_type: None,
            fast_model: None,
            fast_model_provider: None,
            background_model: None,
            background_model_provider: None,
            summarization_model: None,
            summarization_model_provider: None,
            reasoning_effort: None,
            auxiliary_model_resolver: None,
            disabled_tools: None,
            disabled_skill_ids: None,
            selected_skill_ids: None,
            selected_skill_mode: None,
            image_fallback: None,
            gold_config: None,
            app_data_dir: None,
        }
    }

    /// Override the tool executor for this execution.
    pub fn tools(mut self, v: Arc<dyn ToolExecutor>) -> Self {
        self.tools = Some(v);
        self
    }

    /// Override the LLM provider for this execution.
    pub fn provider_override(mut self, v: Arc<dyn LLMProvider>) -> Self {
        self.provider_override = Some(v);
        self
    }

    /// Seed the full model selection from a [`ModelRoster`].
    ///
    /// Decomposes the roster back into the builder's individual fields so it
    /// composes with the existing per-field fluent setters; later individual
    /// setters override the corresponding roster entry.
    pub fn model_roster(mut self, roster: ModelRoster) -> Self {
        self.fast_model = roster.fast_model();
        self.fast_model_provider = roster.fast_model_provider();
        self.background_model = roster.background_model();
        self.background_model_provider = roster.background_model_provider();
        self.summarization_model = roster.summarization_model();
        self.summarization_model_provider = roster.summarization_model_provider();
        self.model = roster.model;
        self.provider_name = roster.provider_name;
        self.provider_type = roster.provider_type;
        self
    }

    /// Override the primary model name.
    pub fn model(mut self, v: impl Into<String>) -> Self {
        self.model = Some(v.into());
        self
    }

    /// Override the provider name.
    pub fn provider_name(mut self, v: impl Into<String>) -> Self {
        self.provider_name = Some(v.into());
        self
    }

    /// Override the provider type.
    pub fn provider_type(mut self, v: impl Into<String>) -> Self {
        self.provider_type = Some(v.into());
        self
    }

    /// Override the fast-model name.
    pub fn fast_model(mut self, v: impl Into<String>) -> Self {
        self.fast_model = Some(v.into());
        self
    }

    /// Override the provider used for fast-model calls.
    pub fn fast_model_provider(mut self, v: Arc<dyn LLMProvider>) -> Self {
        self.fast_model_provider = Some(v);
        self
    }

    /// Override the background-model name.
    pub fn background_model(mut self, v: impl Into<String>) -> Self {
        self.background_model = Some(v.into());
        self
    }

    /// Override the provider used for background/memory model calls.
    pub fn background_model_provider(mut self, v: Arc<dyn LLMProvider>) -> Self {
        self.background_model_provider = Some(v);
        self
    }

    /// Override the summarization-model name.
    pub fn summarization_model(mut self, v: impl Into<String>) -> Self {
        self.summarization_model = Some(v.into());
        self
    }

    /// Override the provider used for summarization/compression calls.
    pub fn summarization_model_provider(mut self, v: Arc<dyn LLMProvider>) -> Self {
        self.summarization_model_provider = Some(v);
        self
    }

    /// Set the reasoning effort.
    pub fn reasoning_effort(mut self, v: ReasoningEffort) -> Self {
        self.reasoning_effort = Some(v);
        self
    }

    /// Set the per-round auxiliary-model resolver.
    pub fn auxiliary_model_resolver(
        mut self,
        v: Arc<dyn Fn() -> AuxiliaryModelConfig + Send + Sync>,
    ) -> Self {
        self.auxiliary_model_resolver = Some(v);
        self
    }

    /// Set the disabled tool names (merged with config defaults at runtime).
    pub fn disabled_tools(mut self, v: BTreeSet<String>) -> Self {
        self.disabled_tools = Some(v);
        self
    }

    /// Set the disabled skill ids.
    pub fn disabled_skill_ids(mut self, v: BTreeSet<String>) -> Self {
        self.disabled_skill_ids = Some(v);
        self
    }

    /// Set the explicitly selected skill ids.
    pub fn selected_skill_ids(mut self, v: Vec<String>) -> Self {
        self.selected_skill_ids = Some(v);
        self
    }

    /// Set the skill-selection mode.
    pub fn selected_skill_mode(mut self, v: impl Into<String>) -> Self {
        self.selected_skill_mode = Some(v.into());
        self
    }

    /// Set the image fallback configuration.
    pub fn image_fallback(mut self, v: ImageFallbackConfig) -> Self {
        self.image_fallback = Some(v);
        self
    }

    /// Set the internal `gold_config` feature flag.
    ///
    /// `gold_config` is an internal feature flag (not part of the public SDK
    /// surface), so this setter is crate-visible only. Public SDK callers always
    /// leave it `None`; the in-crate spawn paths thread a resolved value through
    /// here. Defaults to `None`.
    pub(crate) fn gold_config(mut self, v: Option<GoldConfig>) -> Self {
        self.gold_config = v;
        self
    }

    /// Set the Bamboo application data directory.
    pub fn app_data_dir(mut self, v: std::path::PathBuf) -> Self {
        self.app_data_dir = Some(v);
        self
    }

    /// Materialize the underlying [`ExecuteRequest`].
    ///
    /// `gold_config` is an internal feature flag with only a crate-visible
    /// setter ([`Self::gold_config`]); public SDK callers leave it `None`.
    pub fn build(self) -> ExecuteRequest {
        let model_roster = ModelRoster {
            model: self.model,
            provider_name: self.provider_name,
            provider_type: self.provider_type,
            fast: RoleModel::from_parts(self.fast_model, self.fast_model_provider),
            background: RoleModel::from_parts(
                self.background_model,
                self.background_model_provider,
            ),
            summarization: RoleModel::from_parts(
                self.summarization_model,
                self.summarization_model_provider,
            ),
        };
        ExecuteRequest {
            initial_message: self.initial_message,
            event_tx: self.event_tx,
            cancel_token: self.cancel_token,
            tools: self.tools,
            provider_override: self.provider_override,
            model_roster,
            reasoning_effort: self.reasoning_effort,
            auxiliary_model_resolver: self.auxiliary_model_resolver,
            disabled_tools: self.disabled_tools,
            disabled_skill_ids: self.disabled_skill_ids,
            selected_skill_ids: self.selected_skill_ids,
            selected_skill_mode: self.selected_skill_mode,
            image_fallback: self.image_fallback,
            gold_config: self.gold_config,
            app_data_dir: self.app_data_dir,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the system prompt from session messages.
fn extract_system_prompt(session: &Session) -> Option<String> {
    session
        .messages
        .iter()
        .find(|m| matches!(m.role, Role::System))
        .map(|m| m.content.clone())
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

impl AgentRuntime {
    /// Execute the agent loop with the given request.
    ///
    /// Builds an [`AgentLoopConfig`] from the request parameters and shared
    /// runtime resources, then delegates to [`run_agent_loop_with_config`].
    pub async fn execute(
        &self,
        session: &mut Session,
        req: ExecuteRequest,
    ) -> crate::runtime::runner::Result<()> {
        let system_prompt = extract_system_prompt(session);
        let config = self.config.read().await;
        let ExecuteRequest {
            initial_message,
            event_tx,
            cancel_token,
            tools,
            provider_override,
            model_roster,
            reasoning_effort,
            auxiliary_model_resolver,
            disabled_tools,
            disabled_skill_ids,
            selected_skill_ids,
            selected_skill_mode,
            image_fallback,
            gold_config,
            app_data_dir,
        } = req;
        let tools = tools.unwrap_or_else(|| self.default_tools.clone());
        let llm = provider_override.unwrap_or_else(|| self.provider.clone());

        // Decompose the roster back into the loose locals the resolution logic
        // below expects. This preserves the byte-for-byte `None → Config`
        // fallbacks: a `None` role yields `None` name/provider exactly as the
        // old loose fields did.
        let fast_model = model_roster.fast_model();
        let fast_model_provider = model_roster.fast_model_provider();
        let background_model = model_roster.background_model();
        let background_model_provider = model_roster.background_model_provider();
        let summarization_model = model_roster.summarization_model();
        let summarization_model_provider = model_roster.summarization_model_provider();
        let ModelRoster {
            model,
            provider_name,
            provider_type,
            ..
        } = model_roster;

        let loop_config = AgentLoopConfig {
            max_rounds: 200,
            system_prompt,
            disabled_skill_ids: disabled_skill_ids.unwrap_or_else(|| config.disabled_skill_ids()),
            selected_skill_ids,
            selected_skill_mode,
            skill_manager: Some(self.skill_manager.clone()),
            skip_initial_user_message: true,
            storage: Some(self.storage.clone()),
            persistence: Some(self.persistence.clone()),
            attachment_reader: Some(self.attachment_reader.clone()),
            metrics_collector: Some(self.metrics_collector.clone()),
            model_name: model,
            fast_model_name: fast_model.or_else(|| config.get_fast_model()),
            fast_model_provider,
            background_model_name: background_model
                .or_else(|| config.get_memory_background_model()),
            planning_model_name: config
                .defaults
                .as_ref()
                .and_then(|d| d.planning.as_ref())
                .map(|r| r.model.clone()),
            search_model_name: config
                .defaults
                .as_ref()
                .and_then(|d| d.search.as_ref().or(d.fast.as_ref()))
                .map(|r| r.model.clone()),
            compression_instructions: None,
            summarization_model_name: summarization_model
                .or_else(|| config.get_task_summary_model()),
            background_model_provider,
            summarization_model_provider,
            provider_name: Some(provider_name.unwrap_or_else(|| config.provider.clone())),
            provider_type,
            reasoning_effort,
            auxiliary_model_resolver,
            disabled_tools: {
                let mut merged = config.disabled_tool_names();
                if let Some(dt) = disabled_tools {
                    merged.extend(dt);
                }
                merged
            },
            image_fallback,
            app_data_dir,
            prompt_memory_flags: config
                .memory
                .as_ref()
                .map(PromptMemoryFlags::from)
                .unwrap_or_default(),
            features_dynamic_model_routing: config.features.dynamic_model_routing,
            permission_mode: session
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.plan_mode.as_ref())
                .map(|_| PermissionMode::Plan),
            gold_config,
            // Capture the tool executor's server-level guidance (connected MCP
            // servers' `instructions`) once, so it lands in the system prompt only
            // while those servers are loaded for this run.
            mcp_tool_guidance: tools.tool_guidance(),
            ..Default::default()
        };

        drop(config);

        run_agent_loop_with_config(
            session,
            initial_message,
            event_tx,
            llm,
            tools,
            cancel_token,
            loop_config,
        )
        .await
    }
}
