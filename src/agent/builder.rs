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

use bamboo_domain::subagent::ToolPolicy;
use bamboo_engine::{
    AgentBuilder as EngineAgentBuilder, MetricsCollector, SkillManager, SkillStoreConfig,
    SqliteMetricsStorage,
};
use bamboo_infrastructure::storage::{LockedSessionStore, SessionStoreV2};
use bamboo_infrastructure::{create_provider_with_dir, Config, LLMProvider};

use super::Agent;

/// Default metrics retention window (days), mirroring `MetricsService::new`.
const DEFAULT_METRICS_RETENTION_DAYS: u32 = 90;

/// Ergonomic builder for [`Agent`].
///
/// Holds the configured instruction (system-prompt fragment), tool policy,
/// model, and api key alongside the wrapped engine builder. Call
/// [`with_defaults_for_data_dir`](Self::with_defaults_for_data_dir) to assemble
/// the runtime dependencies, then [`build`](Self::build).
pub struct AgentBuilder {
    inner: EngineAgentBuilder,

    /// Caller-supplied instruction (system-prompt fragment), injected into the
    /// session at `run` time; the engine assembles the full prompt around it.
    system_prompt: Option<String>,
    /// Tool policy, translated to `disabled_tools` at `run` time.
    tool_policy: Option<ToolPolicy>,
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
            tool_policy: None,
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

    /// Activate exactly the given tools — the agent may use only these (an
    /// allowlist). Accepts tool names, e.g. `.tools(["WebSearch", "Read"])`.
    ///
    /// Use [`builtin_tool_names`](super::builtin_tool_names) to discover the
    /// built-in names; MCP / dynamic tool names may be listed here too. Calling
    /// this replaces any previously configured tool selection.
    pub fn tools<I, S>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let allow = tools.into_iter().map(Into::into).collect();
        self.tool_policy = Some(ToolPolicy::Allowlist { allow });
        self
    }

    /// Activate a single additional tool, appending to the allowlist (starting
    /// one if no selection has been made yet).
    pub fn tool(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        match self
            .tool_policy
            .get_or_insert_with(|| ToolPolicy::Allowlist { allow: Vec::new() })
        {
            ToolPolicy::Allowlist { allow } => allow.push(name),
            other => *other = ToolPolicy::Allowlist { allow: vec![name] },
        }
        self
    }

    /// Set an explicit [`ToolPolicy`] (advanced): `Inherit` (all default tools),
    /// `Allowlist` (only the listed tools), or `Denylist` (all default tools
    /// except the listed ones). Most callers want [`tools`](Self::tools).
    pub fn tool_policy(mut self, policy: ToolPolicy) -> Self {
        self.tool_policy = Some(policy);
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
    pub fn default_tools(
        mut self,
        tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>,
    ) -> Self {
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
        let metrics_storage: Arc<dyn bamboo_engine::MetricsStorage> =
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
    /// The configured `instruction`, `tool_policy`, and `model` are carried
    /// onto the `Agent` so that [`Agent::run`](super::Agent::run) can inject
    /// them into the session.
    pub fn build(self) -> Result<Agent, String> {
        let runtime = self.inner.build().map_err(|e| e.to_string())?;
        Ok(Agent::from_runtime_with_config(
            runtime,
            self.system_prompt,
            self.tool_policy,
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
