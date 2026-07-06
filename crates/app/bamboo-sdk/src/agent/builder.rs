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
    /// Provider to select (e.g. `anthropic`, `openai`, `gemini`, `copilot`,
    /// `bodhi`) in `with_defaults_for_data_dir`, overriding `config.json`'s
    /// `provider`. `None` keeps the configured default.
    provider_name: Option<String>,
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
            provider_name: None,
            api_key: None,
        }
    }

    // -- Configuration ------------------------------------------------------

    /// Set the primary model.
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Select the provider by name (`anthropic`, `openai`, `gemini`, `copilot`,
    /// `bodhi`) for [`with_defaults_for_data_dir`](Self::with_defaults_for_data_dir),
    /// overriding `config.json`'s `provider`. A following [`api_key`](Self::api_key)
    /// applies to *this* provider. The name is lower-cased (config matching is
    /// case-sensitive).
    ///
    /// Note: this drives the *eager* provider creation inside
    /// `with_defaults_for_data_dir` and can fail there (e.g. missing key). A later
    /// [`provider`](Self::provider) injection replaces the created provider but
    /// does NOT skip creation — so if you inject your own provider, either don't
    /// set `provider_name`, or ensure the named provider can still be constructed.
    pub fn provider_name(mut self, provider: impl Into<String>) -> Self {
        self.provider_name = Some(provider.into().trim().to_ascii_lowercase());
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
    ///
    /// # Precondition
    ///
    /// `<data_dir>/config.json` must define the active provider with a non-empty
    /// `api_key` (the same config `bamboo serve` reads). A fresh data dir with no
    /// `config.json` defaults to the `anthropic` provider with no key, so step 6
    /// (`create_provider_with_dir`) returns `Err("failed to create provider: …")`.
    /// The `copilot` provider is the only one that can authenticate keyless (via
    /// its cached OAuth token). Set the key via the config file, or pass it on the
    /// builder with [`api_key`](Self::api_key) **before** calling this method.
    pub async fn with_defaults_for_data_dir(mut self, data_dir: PathBuf) -> Result<Self, String> {
        // 1. Config.
        let mut config = Config::from_data_dir(Some(data_dir.clone()));
        // Select the provider first, so `api_key` and provider creation both act
        // on the chosen provider rather than config.json's default.
        if let Some(provider) = self.provider_name.clone() {
            config.provider = provider;
        }
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

/// Apply `api_key` to the active provider's in-memory config slot.
///
/// If the provider stanza already exists, its key is overwritten. If it is
/// absent (the common default-config / no-`config.json` case), a minimal stanza
/// is **fabricated** from `{"api_key": …}` — every other field is serde-default
/// (all `Option`/`Vec`/`flatten`), so `.api_key("sk-…")` alone is enough to make
/// a fresh data dir usable. Only the keyed providers (`openai` / `anthropic` /
/// `gemini`) are fabricated; other providers (e.g. `copilot`, which authenticates
/// via cached OAuth rather than a plain key) fall through to a warning.
fn apply_api_key(config: &mut Config, api_key: &str) {
    // A minimal `{"api_key": …}` stanza; every other provider-config field
    // deserializes to its serde default, so the target type is inferred from
    // the assignment below (no `serde` trait import needed).
    let stanza = || serde_json::json!({ "api_key": api_key });

    let applied = match config.provider.as_str() {
        "openai" => match config.providers.openai.as_mut() {
            Some(c) => {
                c.api_key = api_key.to_string();
                true
            }
            None => {
                config.providers.openai = serde_json::from_value(stanza()).ok();
                config.providers.openai.is_some()
            }
        },
        "anthropic" => match config.providers.anthropic.as_mut() {
            Some(c) => {
                c.api_key = api_key.to_string();
                true
            }
            None => {
                config.providers.anthropic = serde_json::from_value(stanza()).ok();
                config.providers.anthropic.is_some()
            }
        },
        "gemini" => match config.providers.gemini.as_mut() {
            Some(c) => {
                c.api_key = api_key.to_string();
                true
            }
            None => {
                config.providers.gemini = serde_json::from_value(stanza()).ok();
                config.providers.gemini.is_some()
            }
        },
        _ => false,
    };
    if !applied {
        tracing::warn!(
            provider = %config.provider,
            "AgentBuilder::api_key: key not applied — the active provider either \
             takes no plain api_key (e.g. copilot uses cached OAuth) or its config \
             could not be built from a key alone"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::apply_api_key;
    use bamboo_llm::Config;

    /// `.api_key()` must FABRICATE a usable stanza for each keyed provider when
    /// the config has none — i.e. a key-only JSON deserializes into the provider
    /// config. If a provider struct ever gains a required, non-`#[serde(default)]`
    /// field, `serde_json::from_value` fails and this test catches it (instead of
    /// the feature silently degrading to a confusing runtime warning).
    #[test]
    fn api_key_fabricates_stanza_for_keyed_providers() {
        for provider in ["openai", "anthropic", "gemini"] {
            let mut config = Config::default();
            config.provider = provider.to_string();
            // Force the absent-stanza (fabricate) path.
            config.providers.openai = None;
            config.providers.anthropic = None;
            config.providers.gemini = None;

            apply_api_key(&mut config, "sk-test-123");

            let key = match provider {
                "openai" => config.providers.openai.as_ref().map(|c| c.api_key.as_str()),
                "anthropic" => config
                    .providers
                    .anthropic
                    .as_ref()
                    .map(|c| c.api_key.as_str()),
                "gemini" => config.providers.gemini.as_ref().map(|c| c.api_key.as_str()),
                _ => unreachable!(),
            };
            assert_eq!(
                key,
                Some("sk-test-123"),
                "expected a fabricated {provider} stanza carrying the api_key"
            );
        }
    }

    /// The contract `.provider_name()` relies on: selecting the provider (setting
    /// `config.provider`) BEFORE `apply_api_key` routes the key onto the chosen
    /// provider, not config.json's default. Regression guard for the ordering in
    /// `with_defaults_for_data_dir`.
    #[test]
    fn provider_selection_before_api_key_routes_key_to_chosen_provider() {
        let mut config = Config::default();
        config.provider = "anthropic".to_string(); // config.json default
        config.providers.openai = None;
        config.providers.anthropic = None;

        // Simulate `.provider_name("openai")` (set first), then `.api_key(...)`.
        config.provider = "openai".to_string();
        apply_api_key(&mut config, "sk-openai-xyz");

        assert_eq!(
            config.providers.openai.as_ref().map(|c| c.api_key.as_str()),
            Some("sk-openai-xyz"),
            "api_key must land on the selected provider (openai), not the default"
        );
        assert!(
            config.providers.anthropic.is_none(),
            "the default provider must not receive the key"
        );
    }
}
