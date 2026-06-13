//! Ergonomic [`AgentBuilder`] for the root SDK facade.
//!
//! This wraps [`bamboo_engine::AgentBuilder`] with a concise, one-liner facade:
//! the caller supplies their own instruction (a system-prompt fragment) plus a
//! model and optional tool policy, and the engine dynamically assembles the
//! complete system prompt (tool guides, runtime context, …) around it at run
//! time.
//!
//! ```rust,ignore
//! let agent = Agent::builder()
//!     .model("claude-sonnet-4-6")
//!     .instruction("You help users research topics thoroughly.")
//!     .with_defaults_for_data_dir(data_dir).await?
//!     .build()?;
//! ```
//!
//! `.with_defaults_for_data_dir` assembles the eight runtime dependencies from
//! the **infrastructure / engine / tools** crates only — `bamboo-server` is
//! never pulled into the builder path (reverse-dep risk register §6).

use std::sync::Arc;

use std::path::PathBuf;
use tokio::sync::RwLock;

use bamboo_agent_core::tools::{Tool, ToolExecutor};
use bamboo_engine::AgentBuilder as EngineAgentBuilder;
use bamboo_llm::{create_provider_with_dir, Config, LLMProvider};
use bamboo_metrics::{MetricsCollector, SqliteMetricsStorage};
use bamboo_skills::{SkillManager, SkillStoreConfig};
use bamboo_storage::{LockedSessionStore, SessionStoreV2};
use bamboo_tools::ToolRegistry;

use super::Agent;

/// Default metrics retention window (days), mirroring `MetricsService::new`.
const DEFAULT_METRICS_RETENTION_DAYS: u32 = 90;

/// Ergonomic builder for [`Agent`].
///
/// Holds the configured instruction (system-prompt fragment), tool set, model,
/// and api key alongside the wrapped engine builder. Call
/// [`with_defaults_for_data_dir`](Self::with_defaults_for_data_dir) to assemble
/// the runtime dependencies, then [`build`](Self::build).
pub struct AgentBuilder {
    inner: EngineAgentBuilder,

    /// Caller-supplied instruction (system-prompt fragment), injected into the
    /// session at `run` time; the engine assembles the full prompt around it.
    system_prompt: Option<String>,
    /// The agent's tool set — built-ins (via [`BuiltinTool::tool`](super::BuiltinTool::tool))
    /// and/or custom `impl Tool`s. Empty means "all default built-in tools".
    tools: Vec<Arc<dyn Tool>>,
    /// Primary model override applied to the session at `run` time.
    model: Option<String>,
    /// API key applied to the active provider's config before provider creation.
    api_key: Option<String>,
}

impl AgentBuilder {
    /// Create an empty ergonomic builder.
    pub fn new() -> Self {
        Self {
            inner: EngineAgentBuilder::new(),
            system_prompt: None,
            tools: Vec::new(),
            model: None,
            api_key: None,
        }
    }

    // -- Configuration ------------------------------------------------------

    /// Set the primary model.
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Set the instruction — the caller's portion of the system prompt. The
    /// engine assembles the complete prompt (tool guides, runtime context, …)
    /// around it at run time.
    pub fn instruction(mut self, instruction: impl Into<String>) -> Self {
        self.system_prompt = Some(instruction.into());
        self
    }

    /// Set the agent's tool set — the actual tools it may use, as
    /// `Arc<dyn Tool>`. Built-ins come from the
    /// [`BuiltinTool`](super::BuiltinTool) catalog via
    /// [`BuiltinTool::tool`](super::BuiltinTool::tool); custom tools are any
    /// `impl Tool` wrapped in an `Arc`. Replaces any previous selection.
    ///
    /// ```rust,ignore
    /// agent.tools([BuiltinTool::WebSearch.tool(), BuiltinTool::Read.tool()]);
    /// ```
    ///
    /// Leaving this unset uses the full default built-in tool surface.
    pub fn tools<I>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn Tool>>,
    {
        self.tools = tools.into_iter().collect();
        self
    }

    /// Add a single custom tool (anything implementing
    /// [`Tool`](bamboo_agent_core::tools::Tool)) to the tool set.
    pub fn tool<T: Tool + 'static>(mut self, tool: T) -> Self {
        self.tools.push(Arc::new(tool));
        self
    }

    /// Add a single pre-built shared tool — e.g. `BuiltinTool::Read.tool()` or a
    /// shared custom tool — to the tool set.
    pub fn tool_shared(mut self, tool: Arc<dyn Tool>) -> Self {
        self.tools.push(tool);
        self
    }

    /// Set the API key applied to the active provider's config in
    /// [`with_defaults_for_data_dir`](Self::with_defaults_for_data_dir).
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    // -- Explicit dependency injection (passthrough) ------------------------

    /// Inject a pre-built LLM provider, bypassing config-driven creation.
    pub fn provider(mut self, provider: Arc<dyn LLMProvider>) -> Self {
        self.inner = self.inner.provider(provider);
        self
    }

    /// Inject a pre-built default tool executor.
    pub fn default_tools(mut self, tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>) -> Self {
        self.inner = self.inner.default_tools(tools);
        self
    }

    /// Inject a shared config handle.
    pub fn config(mut self, config: Arc<RwLock<Config>>) -> Self {
        self.inner = self.inner.config(config);
        self
    }

    // -- Default dependency assembly ---------------------------------------

    /// Assemble the eight runtime dependencies rooted at `data_dir`, using only
    /// the infrastructure / engine / tools layers (never `bamboo-server`):
    ///
    /// 1. `Config::from_data_dir(data_dir)` (with `api_key` applied if set)
    /// 2. `SessionStoreV2` → `storage` + `attachment_reader`
    /// 3. `LockedSessionStore` → `persistence`
    /// 4. `SkillManager` (+ `initialize`)
    /// 5. `MetricsCollector::spawn(SqliteMetricsStorage)`
    /// 6. provider via `create_provider_with_dir`
    /// 7. `BuiltinToolExecutor::new_with_config` → `default_tools`
    ///
    /// The engine builder is last-write-wins, so this method does NOT preserve
    /// dependencies set before it. Call `with_defaults_for_data_dir` FIRST, then
    /// override individual dependencies (e.g. [`provider`](Self::provider)) AFTER
    /// it to make those overrides take precedence.
    pub async fn with_defaults_for_data_dir(mut self, data_dir: PathBuf) -> Result<Self, String> {
        // 1. Config.
        let mut config = Config::from_data_dir(Some(data_dir.clone()));
        if let Some(api_key) = self.api_key.clone() {
            apply_api_key(&mut config, &api_key);
        }

        // 6. Provider (created before config is moved into the shared lock).
        let provider = create_provider_with_dir(&config, data_dir.clone())
            .await
            .map_err(|e| format!("failed to create provider: {e}"))?;

        // 7. Default tools (builtin + config-aware).
        let config = Arc::new(RwLock::new(config));
        let default_tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> = Arc::new(
            bamboo_tools::BuiltinToolExecutor::new_with_config(config.clone()),
        );

        // 2/3. Storage + persistence + attachment reader.
        let store = Arc::new(
            SessionStoreV2::new(data_dir.clone())
                .await
                .map_err(|e| format!("failed to initialize session store: {e}"))?,
        );
        let persistence = Arc::new(LockedSessionStore::new(store.clone()));

        // 4. Skill manager.
        let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
            skills_dir: data_dir.join("skills"),
            project_dir: std::env::current_dir().ok(),
            active_mode: None,
        }));
        skill_manager
            .initialize()
            .await
            .map_err(|e| format!("failed to initialize skill manager: {e}"))?;

        // 5. Metrics collector.
        let metrics_storage: Arc<dyn bamboo_metrics::storage::MetricsStorage> =
            Arc::new(SqliteMetricsStorage::new(data_dir.join("metrics.db")));
        let metrics_collector =
            MetricsCollector::spawn(metrics_storage, DEFAULT_METRICS_RETENTION_DAYS);

        self.inner = self
            .inner
            .storage(store.clone())
            .persistence(persistence)
            .attachment_reader(store)
            .skill_manager(skill_manager)
            .metrics_collector(metrics_collector)
            .config(config)
            .provider(provider)
            .default_tools(default_tools);

        Ok(self)
    }

    /// Finalize into an [`Agent`].
    ///
    /// If a tool set was configured via [`tools`](Self::tools) / [`tool`](Self::tool),
    /// the agent's default tool executor is built from exactly those tools, so
    /// the advertised tool surface is precisely the caller's selection. With no
    /// selection, the full default built-in surface is used. The configured
    /// `instruction` and `model` are carried onto the `Agent` for
    /// [`Agent::run`](super::Agent::run).
    pub fn build(mut self) -> Result<Agent, String> {
        if !self.tools.is_empty() {
            let registry = ToolRegistry::new();
            for tool in &self.tools {
                let _ = registry.register_shared(tool.clone());
            }
            let executor: Arc<dyn ToolExecutor> =
                Arc::new(bamboo_tools::BuiltinToolExecutor::with_registry(registry));
            self.inner = self.inner.default_tools(executor);
        }

        let runtime = self.inner.build().map_err(|e| e.to_string())?;
        Ok(Agent::from_runtime_with_config(
            runtime,
            self.system_prompt,
            self.model,
        ))
    }
}

impl Default for AgentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Apply `api_key` to the active provider's in-memory config slot when that
/// provider config already exists. Logs a warning otherwise (the SDK does not
/// fabricate a full provider config struct).
fn apply_api_key(config: &mut Config, api_key: &str) {
    let key = api_key.to_string();
    let applied = match config.provider.as_str() {
        "openai" => config
            .providers
            .openai
            .as_mut()
            .map(|c| c.api_key = key.clone())
            .is_some(),
        "anthropic" => config
            .providers
            .anthropic
            .as_mut()
            .map(|c| c.api_key = key.clone())
            .is_some(),
        "gemini" => config
            .providers
            .gemini
            .as_mut()
            .map(|c| c.api_key = key.clone())
            .is_some(),
        _ => false,
    };
    if !applied {
        tracing::warn!(
            provider = %config.provider,
            "AgentBuilder::api_key: no existing provider config for active provider; \
             api_key not applied (configure the provider in config.json first)"
        );
    }
}
