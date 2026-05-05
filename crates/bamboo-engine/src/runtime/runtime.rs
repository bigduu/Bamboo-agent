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

use crate::metrics::MetricsCollector;
use crate::skills::SkillManager;
use bamboo_agent_core::storage::{AttachmentReader, Storage};
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Role, Session};
use bamboo_domain::ReasoningEffort;
use bamboo_infrastructure::config::PermissionMode;
use bamboo_infrastructure::Config;
use bamboo_infrastructure::LLMProvider;

use crate::runtime::config::{AgentLoopConfig, ImageFallbackConfig, PromptMemoryFlags};
use crate::runtime::hooks::HookRunner;
use crate::runtime::managers::{
    LifecycleManager, LlmManager, MemoryManager, PromptManager, ToolManager,
};
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

    // -- Optional overrides (None → config defaults) ----------------------
    pub model: Option<String>,
    pub provider_name: Option<String>,
    /// When `None`, falls back to `Config::get_memory_background_model()`.
    pub background_model: Option<String>,
    /// Optional provider override for background/fast model calls.
    /// When set, compression/summarization use this provider instead of the shared `llm`.
    pub background_model_provider: Option<Arc<dyn LLMProvider>>,
    pub reasoning_effort: Option<ReasoningEffort>,
    /// When `None`, falls back to `Config::disabled_tool_names()`.
    pub disabled_tools: Option<BTreeSet<String>>,
    /// When `None`, falls back to `Config::disabled_skill_ids()`.
    pub disabled_skill_ids: Option<BTreeSet<String>>,
    pub selected_skill_ids: Option<Vec<String>>,
    pub selected_skill_mode: Option<String>,
    pub image_fallback: Option<ImageFallbackConfig>,
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
            model,
            provider_name,
            background_model,
            background_model_provider,
            reasoning_effort,
            disabled_tools,
            disabled_skill_ids,
            selected_skill_ids,
            selected_skill_mode,
            image_fallback,
        } = req;
        let tools = tools.unwrap_or_else(|| self.default_tools.clone());
        let llm = provider_override.unwrap_or_else(|| self.provider.clone());

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
            fast_model_name: background_model.clone().or_else(|| config.get_fast_model()),
            background_model_name: background_model
                .or_else(|| config.get_memory_background_model()),
            background_model_provider,
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
            provider_name: Some(provider_name.unwrap_or_else(|| config.provider.clone())),
            reasoning_effort,
            disabled_tools: disabled_tools.unwrap_or_else(|| config.disabled_tool_names()),
            image_fallback,
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
