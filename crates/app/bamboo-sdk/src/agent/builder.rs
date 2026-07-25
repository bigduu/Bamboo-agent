//! Ergonomic [`AgentBuilder`] for the root SDK facade.
//!
//! This wraps [`bamboo_engine::AgentBuilder`] with a concise, one-liner facade:
//! the caller supplies their own instruction (a system-prompt fragment) plus a
//! model and optional tool policy, and the engine dynamically assembles the
//! complete system prompt (tool guides, runtime context, …) around it at run
//! time.
//!
//! ```rust,no_run
//! # use std::path::PathBuf;
//! # use bamboo_sdk::Agent;
//! # async fn example(data_dir: PathBuf) -> Result<(), bamboo_sdk::agent::SdkError> {
//! let agent = Agent::builder()
//!     .model("claude-sonnet-4-6")
//!     .instruction("You help users research topics thoroughly.")
//!     .with_defaults_for_data_dir(data_dir).await?
//!     .build()?;
//! # let _ = agent;
//! # Ok(())
//! # }
//! ```
//!
//! `.with_defaults_for_data_dir` assembles the eight runtime dependencies from
//! the **infrastructure / engine / tools** crates only — `bamboo-server` is
//! never pulled into the builder path (reverse-dep risk register §6).

use std::sync::Arc;

use std::path::PathBuf;
use tokio::sync::RwLock;

use bamboo_agent_core::tools::{Tool, ToolExecutor};
use bamboo_domain::{SessionInboxLimits, SessionInboxPort};
use bamboo_engine::{AgentBuilder as EngineAgentBuilder, HookRunner};
use bamboo_llm::{create_provider_with_dir, Config, LLMProvider};
use bamboo_mcp::executor::{CompositeToolExecutor, McpToolExecutor};
use bamboo_mcp::manager::McpServerManager;
use bamboo_mcp::McpServerConfig;
use bamboo_metrics::{MetricsCollector, SqliteMetricsStorage};
use bamboo_skills::{SkillManager, SkillStoreConfig};
use bamboo_storage::{LockedSessionStore, SessionStoreV2};
use bamboo_tools::permission::{
    ConfigPermissionChecker, ModeAwarePermissionChecker, PermissionChecker, PermissionConfig,
    PermissionMode,
};
use bamboo_tools::ToolRegistry;

use super::error::SdkError;
use super::Agent;

/// Default metrics retention window (days), mirroring `MetricsService::new`.
const DEFAULT_METRICS_RETENTION_DAYS: u32 = 90;

struct SdkProjectContextSource {
    store: Arc<bamboo_projects::ProjectStore>,
}

#[async_trait::async_trait]
impl bamboo_engine::project_context::ProjectContextSource for SdkProjectContextSource {
    async fn find_project(
        &self,
        project_id: &bamboo_domain::ProjectId,
    ) -> Result<
        Option<bamboo_engine::project_context::ProjectDescriptor>,
        bamboo_engine::project_context::ProjectContextError,
    > {
        let manifest = match self.store.get(project_id) {
            Ok(project) => project,
            Err(bamboo_projects::ProjectStoreError::NotFound(_)) => return Ok(None),
            Err(error) => {
                return Err(bamboo_engine::project_context::ProjectContextError::Source(
                    error.to_string(),
                ));
            }
        };
        let resources = self.store.resource_summary(project_id).map_err(|error| {
            bamboo_engine::project_context::ProjectContextError::Source(error.to_string())
        })?;
        let roots = self
            .store
            .project_memory_read_roots(project_id)
            .map_err(|error| {
                bamboo_engine::project_context::ProjectContextError::Source(error.to_string())
            })?;
        Ok(Some(bamboo_engine::project_context::ProjectDescriptor {
            id: manifest.id.clone(),
            name: manifest.name,
            home: self.store.paths().project_home(project_id),
            workspace_bindings: manifest.workspace_bindings,
            resources,
            memory_read_roots: bamboo_engine::project_context::ProjectMemoryReadRoots {
                primary: roots.primary,
                legacy_aliases: roots
                    .legacy_aliases
                    .into_iter()
                    .map(
                        |root| bamboo_memory::memory_store::LegacyProjectMemoryReadRoot {
                            project_key: root.legacy_project_key,
                            root: root.root,
                        },
                    )
                    .collect(),
            },
        }))
    }

    async fn list_projects(
        &self,
    ) -> Result<
        Vec<bamboo_engine::project_context::ProjectDescriptor>,
        bamboo_engine::project_context::ProjectContextError,
    > {
        let ids = self
            .store
            .list()
            .map_err(|error| {
                bamboo_engine::project_context::ProjectContextError::Source(error.to_string())
            })?
            .into_iter()
            .map(|project| project.id)
            .collect::<Vec<_>>();
        let mut projects = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(project) = self.find_project(&id).await? {
                projects.push(project);
            }
        }
        Ok(projects)
    }

    async fn find_workspace_owner(
        &self,
        workspace: &std::path::Path,
    ) -> Result<Option<bamboo_domain::ProjectId>, bamboo_engine::project_context::ProjectContextError>
    {
        self.store
            .find_workspace_owner_for_path(&bamboo_config::paths::path_to_display_string(workspace))
            .map(|owner| owner.map(|project| project.id))
            .map_err(|error| {
                bamboo_engine::project_context::ProjectContextError::Source(error.to_string())
            })
    }
}

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
    /// Explicit tool policy. `None` means the caller did not select tools and
    /// the assembled default surface is used; `Some([])` intentionally means
    /// zero tools.
    tools: Option<Vec<Arc<dyn Tool>>>,
    /// Primary model override applied to the session at `run` time.
    model: Option<String>,
    /// Effective configured model captured solely for `Agent::new_session`.
    /// Kept separate from `model` so adding session ergonomics does not start
    /// overwriting caller-supplied session models during execution.
    session_model: Option<String>,
    /// Stable Project membership applied to newly-created or still-unassigned
    /// sessions. Existing membership is never overwritten by execution.
    project_id: Option<String>,
    /// Provider to select (e.g. `anthropic`, `openai`, `gemini`, `copilot`,
    /// `bodhi`) in `with_defaults_for_data_dir`, overriding `config.json`'s
    /// `provider`. `None` keeps the configured default.
    provider_name: Option<String>,
    /// API key applied to the active provider's config before provider creation.
    api_key: Option<String>,
    /// MCP servers to connect and merge into the tool surface in
    /// `with_defaults_for_data_dir`. See [`mcp_server`](Self::mcp_server).
    mcp_servers: Vec<McpServerConfig>,
    /// Permission checker applied to the built-in tool executor assembled by
    /// `with_defaults_for_data_dir`. `None` (the default) means no permission
    /// gating at all — every tool call runs unprompted. See
    /// [`permission_checker`](Self::permission_checker).
    permission_checker: Option<Arc<dyn PermissionChecker>>,
    /// Explicit dependency overrides are retained outside the wrapped engine
    /// builder so defaults can never overwrite them based on call order.
    provider_override: Option<Arc<dyn LLMProvider>>,
    default_tools_override: Option<Arc<dyn ToolExecutor>>,
    config_override: Option<Arc<RwLock<Config>>>,
    /// Defaults assembly records the config and already-connected MCP executor
    /// here; the final built-in executor is created in `build()` from the final
    /// permission policy, so policy setters are order-independent.
    assembled_config: Option<Arc<RwLock<Config>>>,
    assembled_mcp_tools: Option<Arc<dyn ToolExecutor>>,
    /// Concrete session-index handle assembled by `with_defaults_for_data_dir`
    /// (internal — not settable directly). Carried onto [`Agent`] to back the
    /// session-listing ergonomics ([`Agent::list_sessions`](super::Agent::list_sessions)),
    /// which need the concrete `SessionStoreV2` rather than the type-erased
    /// `Arc<dyn Storage>` the engine builder takes.
    session_store: Option<Arc<SessionStoreV2>>,
    project_store: Option<Arc<bamboo_projects::ProjectStore>>,
    project_sessions: Option<bamboo_engine::SessionRepository>,
    /// Optional coherent logical-session delivery plane. The messenger is
    /// assembled at build time from the exact defaults-backed session store,
    /// so callers cannot accidentally inject three mutually inconsistent
    /// handles.
    session_delivery: Option<(
        SessionInboxLimits,
        Arc<bamboo_engine::SessionActivationRouter>,
    )>,
}

impl AgentBuilder {
    /// Create an empty ergonomic builder.
    pub fn new() -> Self {
        Self {
            inner: EngineAgentBuilder::new(),
            system_prompt: None,
            tools: None,
            model: None,
            session_model: None,
            project_id: None,
            provider_name: None,
            api_key: None,
            mcp_servers: Vec::new(),
            permission_checker: None,
            provider_override: None,
            default_tools_override: None,
            config_override: None,
            assembled_config: None,
            assembled_mcp_tools: None,
            session_store: None,
            project_store: None,
            project_sessions: None,
            session_delivery: None,
        }
    }

    // -- Configuration ------------------------------------------------------

    /// Set the primary model.
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Bind sessions created by this SDK facade to a first-class Project.
    /// The opaque id is preserved as supplied; execution only fills missing
    /// membership and never silently reassigns an existing session.
    pub fn project_id(mut self, project_id: impl Into<String>) -> Self {
        self.project_id = Some(project_id.into());
        self
    }

    /// Select the provider by name (`anthropic`, `openai`, `gemini`, `copilot`,
    /// `bodhi`) for [`with_defaults_for_data_dir`](Self::with_defaults_for_data_dir),
    /// overriding `config.json`'s `provider`. A following [`api_key`](Self::api_key)
    /// applies to *this* provider. The name is lower-cased (config matching is
    /// case-sensitive).
    ///
    /// Note: this drives provider creation inside `with_defaults_for_data_dir`
    /// and can fail there (e.g. missing key). A [`provider`](Self::provider)
    /// injected before defaults skips that creation entirely; one injected
    /// afterwards replaces the already-created provider at build time.
    pub fn provider_name(mut self, provider: impl Into<String>) -> Self {
        self.provider_name = Some(provider.into().trim().to_ascii_lowercase());
        self
    }

    /// Set the instruction — the caller's authoritative portion of the system
    /// prompt. At run time it replaces a session's existing leading `System`
    /// message (or is inserted at the front when none exists); leaving it unset
    /// preserves the session's leading `System` message. The engine assembles
    /// the complete prompt (tool guides, runtime context, …) around it.
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
    /// ```rust,no_run
    /// # use bamboo_sdk::{Agent, BuiltinTool};
    /// let builder = Agent::builder()
    ///     .tools([BuiltinTool::WebSearch.tool(), BuiltinTool::Read.tool()]);
    /// # let _ = builder;
    /// ```
    ///
    /// Leaving this unset uses the full default built-in tool surface.
    pub fn tools<I>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn Tool>>,
    {
        self.tools = Some(tools.into_iter().collect());
        self
    }

    /// Explicitly build an agent with no tools.
    ///
    /// This is equivalent to [`tools`](Self::tools) with an empty iterator and
    /// is distinct from leaving tools unset, which keeps the full assembled
    /// built-in (+ MCP, if configured) surface.
    pub fn no_tools(mut self) -> Self {
        self.tools = Some(Vec::new());
        self
    }

    /// Add a single custom tool (anything implementing
    /// [`Tool`](bamboo_agent_core::tools::Tool)) to the tool set.
    pub fn tool<T: Tool + 'static>(mut self, tool: T) -> Self {
        self.tools.get_or_insert_with(Vec::new).push(Arc::new(tool));
        self
    }

    /// Add a single pre-built shared tool — e.g. `BuiltinTool::Read.tool()` or a
    /// shared custom tool — to the tool set.
    pub fn tool_shared(mut self, tool: Arc<dyn Tool>) -> Self {
        self.tools.get_or_insert_with(Vec::new).push(tool);
        self
    }

    /// Set the API key applied to the active provider's config in
    /// [`with_defaults_for_data_dir`](Self::with_defaults_for_data_dir).
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Connect an MCP server and merge its tools into the agent's tool surface.
    ///
    /// Only takes effect via
    /// [`with_defaults_for_data_dir`](Self::with_defaults_for_data_dir), which
    /// starts every configured server (in call order) and composes their tools
    /// with the built-in surface via
    /// [`CompositeToolExecutor`](bamboo_mcp::executor::CompositeToolExecutor) —
    /// built-ins are tried first, falling back to MCP on `NotFound`. Each
    /// server's `initialize` `instructions` (if any) are folded into the tool
    /// guidance the engine injects into the system prompt automatically (no
    /// extra wiring needed).
    ///
    /// A later [`tools`](Self::tools)/[`tool`](Self::tool) selection REPLACES
    /// the whole assembled executor at [`build`](Self::build) time (existing
    /// behavior, unchanged by this method) — so an explicit tool selection
    /// currently excludes MCP tools. Select tools via `allowed_tools` on the
    /// server config instead if you need to restrict without losing MCP.
    ///
    /// ```rust,no_run
    /// # use std::path::PathBuf;
    /// use bamboo_sdk::Agent;
    /// use bamboo_sdk::agent::{McpServerConfig, StdioConfig, TransportConfig};
    /// # async fn example(data_dir: PathBuf) -> Result<(), bamboo_sdk::agent::SdkError> {
    ///
    /// let agent = Agent::builder()
    ///     .model("claude-sonnet-4-6")
    ///     .mcp_server(McpServerConfig {
    ///         id: "fs".into(),
    ///         name: Some("filesystem".into()),
    ///         enabled: true,
    ///         transport: TransportConfig::Stdio(StdioConfig {
    ///             command: "npx".into(),
    ///             args: vec!["-y".into(), "@modelcontextprotocol/server-filesystem".into()],
    ///             cwd: None,
    ///             env: Default::default(),
    ///             env_encrypted: Default::default(),
    ///             env_credential_refs: Default::default(),
    ///             startup_timeout_ms: 20_000,
    ///         }),
    ///         request_timeout_ms: 60_000,
    ///         healthcheck_interval_ms: 30_000,
    ///         reconnect: Default::default(),
    ///         allowed_tools: Vec::new(),
    ///         denied_tools: Vec::new(),
    ///     })
    ///     .with_defaults_for_data_dir(data_dir).await?
    ///     .build()?;
    /// # let _ = agent;
    /// # Ok(())
    /// # }
    /// ```
    pub fn mcp_server(mut self, config: McpServerConfig) -> Self {
        self.mcp_servers.push(config);
        self
    }

    /// Connect multiple MCP servers. See [`mcp_server`](Self::mcp_server).
    pub fn mcp_servers<I>(mut self, configs: I) -> Self
    where
        I: IntoIterator<Item = McpServerConfig>,
    {
        self.mcp_servers.extend(configs);
        self
    }

    /// Gate the built-in tool executor behind a permission checker (e.g.
    /// `bamboo_tools::permission::ConfigPermissionChecker`).
    ///
    /// Applied when [`build`](Self::build) constructs either the defaults-backed
    /// built-in executor or an explicit [`tools`](Self::tools) registry. The
    /// SDK's historical default (no checker configured) is **bypass
    /// everything** — no tool call is ever gated — so this is purely opt-in;
    /// see [`bypass_permissions`](Self::bypass_permissions) to make that intent
    /// explicit at the call site. A fully injected
    /// [`default_tools`](Self::default_tools) executor owns its own permission
    /// behavior and is not wrapped by this setter.
    ///
    /// Once gated, a tool call that needs approval suspends the run (a
    /// `NeedClarification`/`ToolApprovalRequested` event, session
    /// `pending_question` set) exactly like a `conclusion_with_options`
    /// clarification — resolve it with [`Agent::answer`](super::Agent::answer).
    pub fn permission_checker(mut self, checker: Arc<dyn PermissionChecker>) -> Self {
        self.permission_checker = Some(checker);
        self
    }

    /// Configure the standard Bamboo permission policy for the default
    /// built-in tool executor.
    ///
    /// [`PermissionMode::BypassPermissions`] is exactly equivalent to
    /// [`bypass_permissions`](Self::bypass_permissions). Every other mode
    /// installs Bamboo's canonical `PermissionConfig` +
    /// `ConfigPermissionChecker` + `ModeAwarePermissionChecker` stack. Calls to
    /// this method, [`permission_checker`](Self::permission_checker), and
    /// [`bypass_permissions`](Self::bypass_permissions) are last-call-wins,
    /// including when called after
    /// [`with_defaults_for_data_dir`](Self::with_defaults_for_data_dir).
    pub fn permission_mode(mut self, mode: PermissionMode) -> Self {
        self.permission_checker = permission_checker_for_mode(mode);
        self
    }

    /// Explicitly request the SDK's default: no permission checker, so every
    /// tool call runs unprompted. A no-op relative to never calling
    /// [`permission_checker`](Self::permission_checker) — provided so callers
    /// can say what they mean instead of relying on silent default behavior.
    pub fn bypass_permissions(mut self) -> Self {
        self.permission_checker = None;
        self
    }

    /// Install the immutable lifecycle-hook registry used by every run of this
    /// agent. The registry is snapshotted into the sealed loop configuration at
    /// run start, so one execution cannot observe mid-run registration changes.
    pub fn hook_runner(mut self, runner: Arc<HookRunner>) -> Self {
        self.inner = self.inner.hook_runner(runner);
        self
    }

    /// Inject the SDK's coherent internal logical-session delivery plane.
    ///
    /// The limits and activation router are retained until [`build`](Self::build),
    /// where the [`bamboo_storage::FileSessionInbox`] and one
    /// [`bamboo_engine::SessionMessenger`] are constructed from the exact
    /// session store assembled by
    /// [`with_defaults_for_data_dir`](Self::with_defaults_for_data_dir). The
    /// same inbox, router, and messenger are then installed on every execution
    /// created by this agent. Call order relative to defaults assembly does not
    /// matter.
    ///
    /// The caller must bind a real
    /// [`bamboo_engine::SessionActivationSpawner`] to `activation_router`
    /// before accepting messages; otherwise admission remains durable and
    /// activation fails closed.
    pub fn session_delivery(
        self,
        activation_router: Arc<bamboo_engine::SessionActivationRouter>,
    ) -> Self {
        self.session_delivery_with_limits(activation_router, SessionInboxLimits::default())
    }

    /// Configure logical-session delivery with explicit durable inbox bounds.
    pub fn session_delivery_with_limits(
        mut self,
        activation_router: Arc<bamboo_engine::SessionActivationRouter>,
        limits: SessionInboxLimits,
    ) -> Self {
        self.session_delivery = Some((limits, activation_router));
        self
    }

    // -- Explicit dependency injection (passthrough) ------------------------

    /// Inject a pre-built LLM provider, bypassing config-driven creation.
    pub fn provider(mut self, provider: Arc<dyn LLMProvider>) -> Self {
        self.provider_override = Some(provider);
        self
    }

    /// Inject a pre-built default tool executor.
    pub fn default_tools(mut self, tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>) -> Self {
        self.default_tools_override = Some(tools);
        self
    }

    /// Inject a shared config handle.
    pub fn config(mut self, config: Arc<RwLock<Config>>) -> Self {
        if self.model.is_none() {
            // A config injected after defaults is authoritative too. If it is
            // currently write-locked, fail closed for `new_session` (model=None)
            // rather than retaining a stale model from the displaced config.
            self.session_model = config.try_read().ok().and_then(|config| config.get_model());
        }
        self.config_override = Some(config);
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
    /// Explicit [`provider`](Self::provider), [`default_tools`](Self::default_tools),
    /// and [`config`](Self::config) injections always override the assembled
    /// defaults regardless of whether they are called before or after this
    /// method. An explicit [`tools`](Self::tools)/[`no_tools`](Self::no_tools)
    /// policy has final precedence over both assembled and injected executors.
    ///
    /// # Precondition
    ///
    /// `<data_dir>/config.json` must define the active provider with a non-empty
    /// `api_key` (the same config `bamboo serve` reads). A fresh data dir with no
    /// `config.json` defaults to the `anthropic` provider with no key, so step 6
    /// (`create_provider_with_dir`) returns [`SdkError::ProviderInit`].
    /// The `copilot` provider is the only one that can authenticate keyless (via
    /// its cached OAuth token). Set the key via the config file, or pass it on the
    /// builder with [`api_key`](Self::api_key) **before** calling this method.
    ///
    /// If [`mcp_server`](Self::mcp_server)/[`mcp_servers`](Self::mcp_servers) were
    /// configured, each server is connected here (in call order) and its tools
    /// merged into the built-in tool surface; a connection failure fails the
    /// whole call with [`SdkError::McpServerStart`]. The final
    /// [`permission_checker`](Self::permission_checker) /
    /// [`permission_mode`](Self::permission_mode) /
    /// [`bypass_permissions`](Self::bypass_permissions) choice is applied when
    /// [`build`](Self::build) constructs the built-in executor.
    pub async fn with_defaults_for_data_dir(mut self, data_dir: PathBuf) -> Result<Self, SdkError> {
        // 1. Config.
        let mut config = Config::from_data_dir(Some(data_dir.clone()));
        // Select the provider first, so `api_key` and provider creation both act
        // on the chosen provider rather than config.json's default.
        if let Some(provider) = self.provider_name.clone() {
            config.provider = provider;
        }
        if let Some(api_key) = self.api_key.clone() {
            apply_api_key(&mut config, &api_key)?;
        }

        // Capture the effective configured model for `Agent::new_session` when
        // the caller did not provide a stronger `.model(...)` override.
        if self.model.is_none() {
            self.session_model = match &self.config_override {
                Some(config_override) => config_override.read().await.get_model(),
                None => config.get_model(),
            };
        }

        // 6. Provider (created before config is moved into the shared lock).
        // An already-injected provider is authoritative, so do not perform a
        // redundant config-driven creation that could fail despite the caller
        // having supplied a usable provider.
        let provider = if self.provider_override.is_none() {
            Some(
                create_provider_with_dir(&config, data_dir.clone())
                    .await
                    .map_err(|e| SdkError::ProviderInit(e.to_string()))?,
            )
        } else {
            None
        };

        // 7. Connect MCP now (the fallible/async part), but defer construction
        // of the built-in/default executor until `build()`. That lets a later
        // permission_mode/checker/bypass call be authoritative without
        // reconnecting MCP or losing its tools/guidance.
        let loaded_config = Arc::new(RwLock::new(config));
        let assembled_config = self
            .config_override
            .clone()
            .unwrap_or_else(|| loaded_config.clone());
        let mcp_tools: Option<Arc<dyn ToolExecutor>> = if self.mcp_servers.is_empty() {
            None
        } else {
            let mcp_manager = Arc::new(McpServerManager::new_with_config(assembled_config.clone()));
            for server_config in &self.mcp_servers {
                let server_id = server_config.id.clone();
                mcp_manager
                    .start_server(server_config.clone())
                    .await
                    .map_err(|source| SdkError::McpServerStart { server_id, source })?;
            }
            Some(Arc::new(McpToolExecutor::new(
                mcp_manager.clone(),
                mcp_manager.tool_index(),
            )))
        };

        // 2/3. Storage + persistence + attachment reader.
        let store = Arc::new(
            SessionStoreV2::new(data_dir.clone())
                .await
                .map_err(|e| SdkError::StoreInit(e.to_string()))?,
        );
        let persistence = Arc::new(LockedSessionStore::new(store.clone()));
        let project_store = Arc::new(
            bamboo_projects::ProjectStore::open(&data_dir)
                .map_err(|error| SdkError::ProjectStoreInit(error.to_string()))?,
        );
        let project_context_resolver =
            Arc::new(bamboo_engine::project_context::ProjectContextResolver::new(
                Arc::new(SdkProjectContextSource {
                    store: project_store.clone(),
                }),
            ));
        let project_sessions = bamboo_engine::SessionRepository::new(
            Default::default(),
            store.clone(),
            persistence.clone(),
        );

        // 4. Skill manager.
        let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
            skills_dir: data_dir.join("skills"),
            project_dir: std::env::current_dir().ok(),
            active_mode: None,
        }));
        skill_manager
            .initialize()
            .await
            .map_err(|e| SdkError::SkillInit(e.to_string()))?;

        // 5. Metrics collector.
        let metrics_storage: Arc<dyn bamboo_metrics::storage::MetricsStorage> =
            Arc::new(SqliteMetricsStorage::new(data_dir.join("metrics.db")));
        let metrics_collector =
            MetricsCollector::spawn(metrics_storage, DEFAULT_METRICS_RETENTION_DAYS);

        self.session_store = Some(store.clone());
        self.project_store = Some(project_store);
        self.project_sessions = Some(project_sessions);
        self.assembled_config = Some(assembled_config.clone());
        self.assembled_mcp_tools = mcp_tools;
        self.inner = self
            .inner
            .storage(store.clone())
            .persistence(persistence)
            .attachment_reader(store)
            .skill_manager(skill_manager)
            .metrics_collector(metrics_collector)
            .project_context_resolver(project_context_resolver)
            .config(assembled_config);
        if let Some(provider) = provider {
            self.inner = self.inner.provider(provider);
        }

        Ok(self)
    }

    /// Finalize into an [`Agent`].
    ///
    /// If a tool set was configured via [`tools`](Self::tools) / [`tool`](Self::tool),
    /// the agent's default tool executor is built from exactly those tools, so
    /// the advertised tool surface is precisely the caller's selection (this
    /// REPLACES any MCP composition from
    /// [`mcp_server`](Self::mcp_server)/[`mcp_servers`](Self::mcp_servers) —
    /// existing behavior, unchanged). With no selection, the full default
    /// built-in (+ MCP, if configured) surface is used. The configured
    /// `instruction` and `model` are carried onto the `Agent` for
    /// [`Agent::run`](super::Agent::run).
    pub fn build(mut self) -> Result<Agent, SdkError> {
        if let Some((limits, activation_router)) = self.session_delivery.take() {
            let session_store = self
                .session_store
                .clone()
                .ok_or_else(|| {
                    SdkError::Unsupported(
                        "session_delivery requires with_defaults_for_data_dir so the runtime and messenger share one session store"
                        .to_string(),
                    )
                })?;
            let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
                session_store.clone(),
                limits,
            ));
            let sessions: Arc<dyn bamboo_agent_core::storage::Storage> = session_store;
            let activation: Arc<dyn bamboo_domain::SessionActivationPort> =
                activation_router.clone();
            let messenger = Arc::new(bamboo_engine::SessionMessenger::new(
                sessions,
                inbox.clone(),
                activation,
            ));
            self.inner = self
                .inner
                .session_inbox(inbox)
                .activation_router(activation_router)
                .session_messenger(messenger);
        }
        let effective_config = self.assembled_config.as_ref().map(|assembled| {
            self.config_override
                .clone()
                .unwrap_or_else(|| assembled.clone())
        });
        if let Some(config) = self.config_override.take() {
            self.inner = self.inner.config(config);
        }
        if let Some(provider) = self.provider_override.take() {
            self.inner = self.inner.provider(provider);
        }

        // An explicit `tools` policy (including the empty policy) always has
        // final precedence. Otherwise preserve an explicitly injected default
        // executor across defaults assembly.
        if let Some(tools) = self.tools.take() {
            let registry = ToolRegistry::new();
            for tool in tools {
                let _ = registry.register_shared(tool);
            }
            let executor: Arc<dyn ToolExecutor> = match self.permission_checker.clone() {
                Some(checker) => Arc::new(
                    bamboo_tools::BuiltinToolExecutor::with_registry_and_permissions(
                        registry, checker,
                    ),
                ),
                None => Arc::new(bamboo_tools::BuiltinToolExecutor::with_registry(registry)),
            };
            self.inner = self.inner.default_tools(executor);
        } else if let Some(executor) = self.default_tools_override.take() {
            self.inner = self.inner.default_tools(executor);
        } else if let Some(config) = effective_config {
            let builtin_tools = match self.permission_checker.clone() {
                Some(checker) => {
                    bamboo_tools::BuiltinToolExecutor::new_with_config_and_permissions(
                        config, checker,
                    )
                }
                None => bamboo_tools::BuiltinToolExecutor::new_with_config(config),
            };
            if let (Some(sessions), Some(projects)) =
                (self.project_sessions.clone(), self.project_store.clone())
            {
                if !builtin_tools.registry().unregister("Workspace") {
                    return Err(SdkError::Build(
                        "default Workspace tool was unavailable for Project-aware replacement"
                            .to_string(),
                    ));
                }
                builtin_tools
                    .register_tool(bamboo_server_tools::ProjectWorkspaceTool::new(
                        sessions.clone(),
                        projects.clone(),
                    ))
                    .map_err(|error| SdkError::Build(error.to_string()))?;
                builtin_tools
                    .register_tool(bamboo_server_tools::ProjectTool::new(sessions, projects))
                    .map_err(|error| SdkError::Build(error.to_string()))?;
            }
            let builtin_tools: Arc<dyn ToolExecutor> = Arc::new(builtin_tools);
            let executor: Arc<dyn ToolExecutor> = match self.assembled_mcp_tools.take() {
                Some(mcp_tools) => Arc::new(CompositeToolExecutor::new(builtin_tools, mcp_tools)),
                None => builtin_tools,
            };
            self.inner = self.inner.default_tools(executor);
        }

        let project_id = self
            .project_id
            .map(|value| {
                let normalized = value.trim().to_string();
                normalized
                    .parse::<bamboo_domain::ProjectId>()
                    .map_err(|_| SdkError::InvalidProjectId(value))
            })
            .transpose()?;
        if project_id.is_some() && self.project_store.is_none() {
            return Err(SdkError::Unsupported(
                "Project identity requires with_defaults_for_data_dir so the Project store, resolver, and Project-aware tools share one authority"
                    .to_string(),
            ));
        }
        if let (Some(store), Some(project_id)) = (self.project_store.as_ref(), project_id.as_ref())
        {
            let project = store
                .get(project_id)
                .map_err(|error| SdkError::ProjectUnavailable(error.to_string()))?;
            if project.status != bamboo_domain::ProjectStatus::Active {
                return Err(SdkError::ProjectUnavailable(format!(
                    "Project {project_id} is archived"
                )));
            }
        }
        let runtime = self
            .inner
            .build()
            .map_err(|e| SdkError::Build(e.to_string()))?;
        Ok(Agent::from_runtime_with_config(
            runtime,
            self.system_prompt,
            self.model,
            self.session_model,
            project_id,
            self.session_store,
            self.permission_checker,
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
/// via cached OAuth rather than a plain key) return a typed error.
fn apply_api_key(config: &mut Config, api_key: &str) -> Result<(), SdkError> {
    // A minimal `{"api_key": …}` stanza; every other provider-config field
    // deserializes to its serde default, so the target type is inferred from
    // the assignment below (no `serde` trait import needed).
    let stanza = || serde_json::json!({ "api_key": api_key });

    let provider = config.provider.clone();
    let providers = config.providers_mut();
    match provider.as_str() {
        "openai" => match providers.openai.as_mut() {
            Some(c) => {
                c.api_key = api_key.to_string();
                Ok(())
            }
            None => {
                providers.openai = Some(serde_json::from_value(stanza()).map_err(|error| {
                    SdkError::Config(format!(
                        "could not construct openai API-key config: {error}"
                    ))
                })?);
                Ok(())
            }
        },
        "anthropic" => match providers.anthropic.as_mut() {
            Some(c) => {
                c.api_key = api_key.to_string();
                Ok(())
            }
            None => {
                providers.anthropic = Some(serde_json::from_value(stanza()).map_err(|error| {
                    SdkError::Config(format!(
                        "could not construct anthropic API-key config: {error}"
                    ))
                })?);
                Ok(())
            }
        },
        "gemini" => match providers.gemini.as_mut() {
            Some(c) => {
                c.api_key = api_key.to_string();
                Ok(())
            }
            None => {
                providers.gemini = Some(serde_json::from_value(stanza()).map_err(|error| {
                    SdkError::Config(format!(
                        "could not construct gemini API-key config: {error}"
                    ))
                })?);
                Ok(())
            }
        },
        _ => Err(SdkError::UnsupportedApiKeyProvider { provider }),
    }
}

fn permission_checker_for_mode(mode: PermissionMode) -> Option<Arc<dyn PermissionChecker>> {
    if mode == PermissionMode::BypassPermissions {
        return None;
    }

    let config = Arc::new(PermissionConfig::new());
    config.set_mode(mode);
    let base: Arc<dyn PermissionChecker> = Arc::new(ConfigPermissionChecker::new(config.clone()));
    Some(Arc::new(ModeAwarePermissionChecker::new(base, config)))
}

#[cfg(test)]
mod tests {
    use super::{apply_api_key, permission_checker_for_mode, AgentBuilder};
    use async_trait::async_trait;
    use bamboo_agent_core::tools::{
        FunctionCall, Tool, ToolCall, ToolCtx, ToolError, ToolExecutionContext, ToolExecutor,
        ToolOutcome, ToolResult,
    };
    use bamboo_agent_core::{Message, ToolSchema};
    use bamboo_llm::{Config, LLMProvider, LLMStream};
    use bamboo_tools::permission::PermissionMode;
    use bamboo_tools::ToolRegistry;
    use futures::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::{Notify, RwLock};

    fn execution_context<'a>(
        session_id: &'a str,
        tool_call_id: &'a str,
    ) -> ToolExecutionContext<'a> {
        ToolExecutionContext {
            session_id: Some(session_id),
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
    }

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
            config.providers_mut().openai = None;
            config.providers_mut().anthropic = None;
            config.providers_mut().gemini = None;

            apply_api_key(&mut config, "sk-test-123").expect("keyed provider accepts api_key");

            let key = match provider {
                "openai" => config
                    .providers()
                    .openai
                    .as_ref()
                    .map(|c| c.api_key.as_str()),
                "anthropic" => config
                    .providers()
                    .anthropic
                    .as_ref()
                    .map(|c| c.api_key.as_str()),
                "gemini" => config
                    .providers()
                    .gemini
                    .as_ref()
                    .map(|c| c.api_key.as_str()),
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
        config.providers_mut().openai = None;
        config.providers_mut().anthropic = None;

        // Simulate `.provider_name("openai")` (set first), then `.api_key(...)`.
        config.provider = "openai".to_string();
        apply_api_key(&mut config, "sk-openai-xyz").expect("openai accepts api_key");

        assert_eq!(
            config
                .providers()
                .openai
                .as_ref()
                .map(|c| c.api_key.as_str()),
            Some("sk-openai-xyz"),
            "api_key must land on the selected provider (openai), not the default"
        );
        assert!(
            config.providers().anthropic.is_none(),
            "the default provider must not receive the key"
        );
    }

    #[test]
    fn unsupported_api_key_provider_is_typed() {
        for provider in ["copilot", "unknown-provider"] {
            let mut config = Config::default();
            config.provider = provider.to_string();
            let error = apply_api_key(&mut config, "not-applicable").unwrap_err();
            assert!(matches!(
                error,
                super::SdkError::UnsupportedApiKeyProvider { provider: actual }
                    if actual == provider
            ));
        }
    }

    #[tokio::test]
    async fn defaults_surface_typed_api_key_error_for_unsupported_provider() {
        for provider in ["copilot", "unknown-provider"] {
            let tmp = tempfile::tempdir().unwrap();
            std::fs::write(
                tmp.path().join("config.json"),
                serde_json::json!({ "provider": provider }).to_string(),
            )
            .unwrap();
            let error = AgentBuilder::new()
                .api_key("not-applicable")
                .with_defaults_for_data_dir(tmp.path().to_path_buf())
                .await
                .err()
                .expect("unsupported provider must fail before creation");
            assert!(matches!(
                error,
                super::SdkError::UnsupportedApiKeyProvider { provider: actual }
                    if actual == provider
            ));
        }
    }

    struct BlockingDoneProvider {
        entered: Arc<Notify>,
        release: Arc<Notify>,
        calls: AtomicUsize,
    }

    struct PanickingProvider {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl LLMProvider for PanickingProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, bamboo_llm::LLMError> {
            self.entered.notify_one();
            self.release.notified().await;
            panic!("injected direct SDK provider panic");
        }
    }

    #[async_trait]
    impl LLMProvider for BlockingDoneProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, bamboo_llm::LLMError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.notified().await;
            Ok(Box::pin(stream::iter([
                Ok(bamboo_llm::LLMChunk::Token("done".to_string())),
                Ok(bamboo_llm::LLMChunk::Done),
            ])))
        }
    }

    struct RecordingActivationSpawner {
        reservations: AtomicUsize,
        launches: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl bamboo_engine::SessionActivationSpawner for RecordingActivationSpawner {
        async fn reserve_activation(
            &self,
            _target_session_id: &str,
            _inbox_generation: u64,
        ) -> Result<
            bamboo_engine::SessionActivationReserveOutcome,
            bamboo_domain::SessionActivationError,
        > {
            let ordinal = self.reservations.fetch_add(1, Ordering::SeqCst) + 1;
            let launches = self.launches.clone();
            Ok(bamboo_engine::SessionActivationReserveOutcome::Reserved(
                bamboo_engine::SessionActivationLaunch::new(
                    format!("sdk-successor-{ordinal}"),
                    move || {
                        launches.fetch_add(1, Ordering::SeqCst);
                    },
                ),
            ))
        }
    }

    #[tokio::test]
    async fn session_delivery_injects_one_coherent_plane_for_every_run() {
        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        let router = bamboo_engine::SessionActivationRouter::new();

        let agent = AgentBuilder::new()
            .session_delivery(router.clone())
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();

        let runtime_inbox = agent
            .session_inbox()
            .expect("SDK must inject the inbox into every execution");
        assert!(Arc::ptr_eq(
            agent
                .activation_router()
                .expect("SDK must inject the activation router"),
            &router
        ));
        let messenger = agent
            .session_messenger()
            .expect("SDK must assemble one messenger from the same handles");
        assert!(Arc::ptr_eq(messenger.inbox(), runtime_inbox));
        let activation: Arc<dyn bamboo_domain::SessionActivationPort> = router;
        assert!(Arc::ptr_eq(messenger.activation(), &activation));
    }

    #[tokio::test]
    async fn direct_sdk_run_owns_delivery_and_terminal_race_launches_once() {
        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        let router = bamboo_engine::SessionActivationRouter::new();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let provider = Arc::new(BlockingDoneProvider {
            entered: entered.clone(),
            release: release.clone(),
            calls: AtomicUsize::new(0),
        });
        let launches = Arc::new(AtomicUsize::new(0));
        let spawner = Arc::new(RecordingActivationSpawner {
            reservations: AtomicUsize::new(0),
            launches: launches.clone(),
        });
        router.set_spawner(spawner.clone()).await;

        let agent = AgentBuilder::new()
            .model("test-model")
            .provider(provider.clone())
            .no_tools()
            .session_delivery(router.clone())
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        let mut session = agent.new_session("sdk-direct-delivery").unwrap();
        session.add_message(Message::user("first"));
        agent
            .storage()
            .save_session(&session)
            .await
            .expect("seed SDK session");

        let mut events = agent.run_stream_session(session);
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .expect("direct SDK provider must be active");
        let messenger = agent.session_messenger().expect("configured SDK messenger");
        let envelope = bamboo_domain::SessionMessageEnvelope::user_input(
            "sdk-direct-delivery",
            "terminal window",
        );
        let receipt = messenger.send(envelope.clone()).await.unwrap();
        assert_eq!(
            receipt.activation,
            bamboo_domain::SessionActivationDisposition::ActiveNotified
        );
        assert_eq!(
            spawner.reservations.load(Ordering::SeqCst),
            0,
            "an active direct SDK execution owns the logical session"
        );

        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while events.recv().await.is_some() {}
        })
        .await
        .expect("direct SDK run must finalize");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(launches.load(Ordering::SeqCst), 1);

        // The launched successor deliberately leaves the poison claim pending.
        // Replaying the same durable generation is bounded and cannot launch a
        // recursive provider loop.
        router
            .begin_finalization("sdk-direct-delivery", "sdk-successor-1")
            .await;
        assert_eq!(
            router
                .finish_finalization("sdk-direct-delivery", "sdk-successor-1", 0,)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            bamboo_domain::SessionActivationPort::request_activation(
                router.as_ref(),
                "sdk-direct-delivery",
                receipt.delivery.generation,
            )
            .await
            .unwrap(),
            bamboo_domain::SessionActivationDisposition::ActivationCoalesced
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn aborted_direct_sdk_run_releases_owner_and_launches_pending_successor() {
        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        let router = bamboo_engine::SessionActivationRouter::new();
        let entered = Arc::new(Notify::new());
        let provider = Arc::new(BlockingDoneProvider {
            entered: entered.clone(),
            release: Arc::new(Notify::new()),
            calls: AtomicUsize::new(0),
        });
        let launches = Arc::new(AtomicUsize::new(0));
        let spawner = Arc::new(RecordingActivationSpawner {
            reservations: AtomicUsize::new(0),
            launches: launches.clone(),
        });
        router.set_spawner(spawner.clone()).await;

        let agent = AgentBuilder::new()
            .model("test-model")
            .provider(provider)
            .no_tools()
            .session_delivery(router.clone())
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        let mut session = agent.new_session("sdk-direct-abort").unwrap();
        session.add_message(Message::user("block"));
        agent.storage().save_session(&session).await.unwrap();

        let running_agent = agent.clone();
        let task = tokio::spawn(async move { running_agent.run_session(&mut session).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .expect("direct SDK provider must be active before abort");
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        let receipt = agent
            .session_messenger()
            .unwrap()
            .send(bamboo_domain::SessionMessageEnvelope::user_input(
                "sdk-direct-abort",
                "after abort",
            ))
            .await
            .unwrap();
        assert!(matches!(
            receipt.activation,
            bamboo_domain::SessionActivationDisposition::ActiveNotified
                | bamboo_domain::SessionActivationDisposition::ActivationReserved
                | bamboo_domain::SessionActivationDisposition::ActivationCoalesced
        ));

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while launches.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("abandoned owner must hand durable work to one successor");
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        assert!(router.owns_run("sdk-direct-abort", "sdk-successor-1").await);
    }

    #[tokio::test]
    async fn panicking_direct_sdk_run_releases_owner_and_launches_pending_successor_once() {
        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        let router = bamboo_engine::SessionActivationRouter::new();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let launches = Arc::new(AtomicUsize::new(0));
        let spawner = Arc::new(RecordingActivationSpawner {
            reservations: AtomicUsize::new(0),
            launches: launches.clone(),
        });
        router.set_spawner(spawner.clone()).await;

        let agent = AgentBuilder::new()
            .model("test-model")
            .provider(Arc::new(PanickingProvider {
                entered: entered.clone(),
                release: release.clone(),
            }))
            .no_tools()
            .session_delivery(router.clone())
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        let mut session = agent.new_session("sdk-direct-panic").unwrap();
        session.add_message(Message::user("panic after durable delivery"));
        agent.storage().save_session(&session).await.unwrap();

        let running_agent = agent.clone();
        let task = tokio::spawn(async move { running_agent.run_session(&mut session).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .expect("direct SDK provider must be active before panic");
        let receipt = agent
            .session_messenger()
            .unwrap()
            .send(bamboo_domain::SessionMessageEnvelope::user_input(
                "sdk-direct-panic",
                "survive provider panic",
            ))
            .await
            .unwrap();
        assert_eq!(
            receipt.activation,
            bamboo_domain::SessionActivationDisposition::ActiveNotified
        );

        release.notify_one();
        assert!(task.await.unwrap_err().is_panic());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while launches.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("panicking owner must hand durable work to one successor");
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 1);
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        assert!(router.owns_run("sdk-direct-panic", "sdk-successor-1").await);
    }

    #[tokio::test]
    async fn cloned_sdk_agents_cannot_run_one_logical_session_concurrently() {
        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        let router = bamboo_engine::SessionActivationRouter::new();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let provider = Arc::new(BlockingDoneProvider {
            entered: entered.clone(),
            release: release.clone(),
            calls: AtomicUsize::new(0),
        });
        let agent = AgentBuilder::new()
            .model("test-model")
            .provider(provider.clone())
            .no_tools()
            .session_delivery(router)
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        let mut first_session = agent.new_session("sdk-direct-collision").unwrap();
        first_session.add_message(Message::user("first"));
        agent.storage().save_session(&first_session).await.unwrap();
        let mut second_session = first_session.clone();

        let first_agent = agent.clone();
        let first = tokio::spawn(async move { first_agent.run_session(&mut first_session).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .expect("first provider execution must start");

        let second_error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            agent.run_session(&mut second_session),
        )
        .await
        .expect("colliding SDK execution must fail promptly")
        .unwrap_err();
        assert!(
            second_error
                .to_string()
                .contains("session activation owner collision"),
            "unexpected collision error: {second_error}"
        );
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            1,
            "the rejected clone must never enter provider I/O"
        );

        release.notify_one();
        first.await.unwrap().unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn session_delivery_fails_closed_without_defaults_store() {
        let error = AgentBuilder::new()
            .session_delivery(bamboo_engine::SessionActivationRouter::new())
            .build()
            .err()
            .expect("session delivery without an authoritative SDK store must fail");
        assert!(
            matches!(error, super::SdkError::Unsupported(message) if message.contains("with_defaults_for_data_dir"))
        );
    }

    #[test]
    fn permission_configuration_is_last_call_wins() {
        assert!(AgentBuilder::new()
            .permission_mode(PermissionMode::Default)
            .bypass_permissions()
            .permission_checker
            .is_none());
        assert!(AgentBuilder::new()
            .bypass_permissions()
            .permission_mode(PermissionMode::Plan)
            .permission_checker
            .is_some());

        let custom = permission_checker_for_mode(PermissionMode::Default).unwrap();
        assert!(AgentBuilder::new()
            .permission_mode(PermissionMode::Plan)
            .permission_checker(custom)
            .permission_checker
            .is_some());
        assert!(permission_checker_for_mode(PermissionMode::BypassPermissions).is_none());
    }

    #[tokio::test]
    async fn permission_mode_gates_the_actual_builtin_executor() {
        let tmp = tempfile::tempdir().unwrap();
        let blocked_path = tmp.path().join("plan-blocked.txt");
        let allowed_path = tmp.path().join("accept-edits.txt");
        let config = Arc::new(RwLock::new(Config::default()));

        let plan = bamboo_tools::BuiltinToolExecutor::new_with_config_and_permissions(
            config.clone(),
            permission_checker_for_mode(PermissionMode::Plan).unwrap(),
        );
        let blocked = ToolCall {
            id: "plan-write".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Write".to_string(),
                arguments: serde_json::json!({
                    "file_path": blocked_path,
                    "content": "must not be written"
                })
                .to_string(),
            },
        };
        assert!(plan.execute(&blocked).await.is_err());
        assert!(!blocked_path.exists());

        let accept_edits = bamboo_tools::BuiltinToolExecutor::new_with_config_and_permissions(
            config,
            permission_checker_for_mode(PermissionMode::AcceptEdits).unwrap(),
        );
        let allowed = ToolCall {
            id: "accept-write".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Write".to_string(),
                arguments: serde_json::json!({
                    "file_path": allowed_path,
                    "content": "written"
                })
                .to_string(),
            },
        };
        accept_edits
            .execute(&allowed)
            .await
            .expect("AcceptEdits should allow Write through the executor");
        assert_eq!(std::fs::read_to_string(allowed_path).unwrap(), "written");

        let assembled_dir = tempfile::tempdir().unwrap();
        write_test_config(assembled_dir.path());
        let assembled = AgentBuilder::new()
            .permission_mode(PermissionMode::Plan)
            .with_defaults_for_data_dir(assembled_dir.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        let assembled_path = assembled_dir.path().join("assembled-plan-blocked.txt");
        let call = ToolCall {
            id: "assembled-plan-write".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Write".to_string(),
                arguments: serde_json::json!({
                    "file_path": assembled_path,
                    "content": "must not be written"
                })
                .to_string(),
            },
        };
        assert!(assembled
            .inner
            .default_tools()
            .execute(&call)
            .await
            .is_err());
        assert!(!assembled_path.exists());
    }

    fn write_call(id: &str, path: &std::path::Path, content: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Write".to_string(),
                arguments: serde_json::json!({
                    "file_path": path,
                    "content": content
                })
                .to_string(),
            },
        }
    }

    #[tokio::test]
    async fn permission_mode_after_defaults_blocks_actual_write() {
        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        let agent = AgentBuilder::new()
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap()
            .permission_mode(PermissionMode::Plan)
            .build()
            .unwrap();

        let path = tmp.path().join("post-defaults-plan-blocked.txt");
        assert!(agent
            .inner
            .default_tools()
            .execute(&write_call("post-defaults-plan", &path, "blocked"))
            .await
            .is_err());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn bypass_after_defaults_wins_over_earlier_plan_mode() {
        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        let agent = AgentBuilder::new()
            .permission_mode(PermissionMode::Plan)
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap()
            .bypass_permissions()
            .build()
            .unwrap();

        let path = tmp.path().join("post-defaults-bypass-allowed.txt");
        agent
            .inner
            .default_tools()
            .execute(&write_call("post-defaults-bypass", &path, "allowed"))
            .await
            .expect("final bypass policy must allow Write");
        assert_eq!(std::fs::read_to_string(path).unwrap(), "allowed");
    }

    struct McpMarkerTool;

    #[async_trait]
    impl Tool for McpMarkerTool {
        fn name(&self) -> &str {
            "mcp_marker"
        }

        fn description(&self) -> &str {
            "test-only marker for an already-connected MCP executor"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            _ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            Ok(ToolOutcome::Completed(ToolResult::text(true, "mcp")))
        }
    }

    #[tokio::test]
    async fn post_defaults_permission_change_preserves_connected_mcp_composition() {
        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        let mut builder = AgentBuilder::new()
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap();

        // Model the executor produced by the async MCP connection phase without
        // launching a process in this unit test. `build` must compose this saved
        // executor with the rebuilt, final-policy built-in executor.
        let registry = ToolRegistry::new();
        registry.register(McpMarkerTool).unwrap();
        builder.assembled_mcp_tools = Some(Arc::new(
            bamboo_tools::BuiltinToolExecutor::with_registry(registry),
        ));

        let agent = builder
            .permission_mode(PermissionMode::Plan)
            .build()
            .unwrap();
        let names: Vec<String> = agent
            .inner
            .default_tools()
            .list_tools()
            .into_iter()
            .map(|schema| schema.function.name)
            .collect();
        assert!(names.iter().any(|name| name == "mcp_marker"));
        assert!(names.iter().any(|name| name == "Write"));

        let path = tmp.path().join("mcp-composed-plan-blocked.txt");
        assert!(agent
            .inner
            .default_tools()
            .execute(&write_call("mcp-composed-plan", &path, "blocked"))
            .await
            .is_err());
        assert!(!path.exists());
    }

    fn write_test_config(data_dir: &std::path::Path) {
        std::fs::write(
            data_dir.join("config.json"),
            r#"{
                "provider": "anthropic",
                "providers": {
                    "anthropic": { "api_key": "test-key", "model": "claude-test" }
                }
            }"#,
        )
        .unwrap();
    }

    async fn build_with_tool_policy(
        configure: impl FnOnce(AgentBuilder) -> AgentBuilder,
    ) -> super::Agent {
        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        configure(AgentBuilder::new())
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn unset_tools_keeps_defaults_but_explicit_empty_means_zero_tools() {
        let defaults = build_with_tool_policy(|builder| builder).await;
        assert!(!defaults.inner.default_tools().list_tools().is_empty());

        let no_tools = build_with_tool_policy(AgentBuilder::no_tools).await;
        assert!(no_tools.inner.default_tools().list_tools().is_empty());

        let empty_tools =
            build_with_tool_policy(|builder| builder.tools(std::iter::empty::<Arc<dyn Tool>>()))
                .await;
        assert!(empty_tools.inner.default_tools().list_tools().is_empty());

        let selected = build_with_tool_policy(|builder| {
            builder.tools([super::super::BuiltinTool::Write.tool()])
        })
        .await;
        let schemas = selected.inner.default_tools().list_tools();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].function.name, "Write");
    }

    #[tokio::test]
    async fn explicit_tools_preserve_configured_permission_mode() {
        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        let agent = AgentBuilder::new()
            .permission_mode(PermissionMode::Plan)
            .tools([super::super::BuiltinTool::Write.tool()])
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        let path = tmp.path().join("selected-plan-blocked.txt");
        let call = ToolCall {
            id: "selected-plan-write".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Write".to_string(),
                arguments: serde_json::json!({
                    "file_path": path,
                    "content": "must not be written"
                })
                .to_string(),
            },
        };
        assert!(agent.inner.default_tools().execute(&call).await.is_err());
        assert!(!path.exists());
    }

    struct NeverCalledProvider;

    #[async_trait]
    impl LLMProvider for NeverCalledProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, bamboo_llm::LLMError> {
            Err(bamboo_llm::LLMError::Api(
                "test provider must not be called".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn defaults_backed_sdk_validates_and_propagates_project_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = bamboo_projects::ProjectStore::open(dir.path()).unwrap();
        let project = store.create("SDK Project", None).unwrap();
        let empty_executor: Arc<dyn ToolExecutor> = Arc::new(
            bamboo_tools::BuiltinToolExecutor::with_registry(ToolRegistry::new()),
        );
        let agent = AgentBuilder::new()
            .provider(Arc::new(NeverCalledProvider))
            .default_tools(empty_executor)
            .model("test-model")
            .project_id(project.id.to_string())
            .with_defaults_for_data_dir(dir.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();

        let session = agent.new_session("sdk-project-session").unwrap();
        assert_eq!(session.project_id_meta(), Some(project.id.to_string()));
    }

    #[test]
    fn project_identity_without_defaults_backing_fails_closed() {
        let result = AgentBuilder::new().project_id("project-sdk").build();
        assert!(matches!(
            result,
            Err(super::SdkError::Unsupported(message))
                if message.contains("with_defaults_for_data_dir")
        ));
    }

    #[tokio::test]
    async fn defaults_backed_sdk_workspace_tool_enforces_project_ownership_before_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let workspace_a = dir.path().join("workspace-a");
        let workspace_b = dir.path().join("workspace-b");
        let ephemeral = dir.path().join("ephemeral");
        std::fs::create_dir_all(&workspace_a).unwrap();
        std::fs::create_dir_all(&workspace_b).unwrap();
        std::fs::create_dir_all(&ephemeral).unwrap();
        let store = bamboo_projects::ProjectStore::open(dir.path()).unwrap();
        let project_a = store
            .create_with_bindings(
                "Project A",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace_a.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .unwrap();
        let project_b = store
            .create_with_bindings(
                "Project B",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace_b.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .unwrap();
        let agent = AgentBuilder::new()
            .provider(Arc::new(NeverCalledProvider))
            .model("test-model")
            .project_id(project_a.id.to_string())
            .with_defaults_for_data_dir(dir.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        assert!(
            agent
                .inner
                .default_tools()
                .list_tools()
                .iter()
                .any(|schema| schema.function.name == "Project"),
            "SDK defaults must expose the same Project contract as the server"
        );

        let mut session = agent.new_session("sdk-project-workspace").unwrap();
        session.set_workspace_path_meta(workspace_a.to_string_lossy().into_owned());
        agent.inner.storage().save_session(&session).await.unwrap();
        bamboo_agent_core::workspace_state::set_workspace(
            &session.id,
            workspace_a.canonicalize().unwrap(),
        );
        let call = |id: &str, path: &std::path::Path| ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Workspace".to_string(),
                arguments: serde_json::json!({"path": path}).to_string(),
            },
        };
        let conflict = agent
            .inner
            .default_tools()
            .execute_with_context(
                &call("workspace-conflict", &workspace_b),
                execution_context(&session.id, "workspace-conflict"),
            )
            .await
            .expect("structured conflict result");
        assert!(!conflict.success);
        let conflict_value: serde_json::Value =
            serde_json::from_str(&conflict.result).expect("conflict JSON");
        assert_eq!(conflict_value["code"], "project_workspace_conflict");
        assert_eq!(conflict_value["owner_project_id"], project_b.id.to_string());
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(&session.id),
            Some(workspace_a.canonicalize().unwrap()),
            "rejected SDK call must not mutate runtime workspace"
        );
        let persisted = agent
            .inner
            .storage()
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.workspace_path_meta(),
            session.workspace_path_meta()
        );

        for (id, path, expected_binding) in [
            (
                "workspace-same-project",
                workspace_a.as_path(),
                "registered",
            ),
            ("workspace-ephemeral", ephemeral.as_path(), "unregistered"),
        ] {
            let success = agent
                .inner
                .default_tools()
                .execute_with_context(&call(id, path), execution_context(&session.id, id))
                .await
                .expect("allowed workspace result");
            assert!(success.success, "{id}: {}", success.result);
            let value: serde_json::Value =
                serde_json::from_str(&success.result).expect("workspace JSON");
            assert_eq!(value["binding_status"], expected_binding);
        }
        let persisted = agent
            .inner
            .storage()
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        let ephemeral_display =
            bamboo_config::paths::path_to_display_string(&ephemeral.canonicalize().unwrap());
        assert_eq!(
            persisted.workspace_path_meta().as_deref(),
            Some(ephemeral_display.as_str())
        );
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(&session.id),
            Some(ephemeral.canonicalize().unwrap())
        );
    }

    #[tokio::test]
    async fn explicit_dependencies_survive_defaults_regardless_of_call_order() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("config.json"),
            r#"{"provider":"unknown-provider"}"#,
        )
        .unwrap();

        let explicit_config = Arc::new(RwLock::new(Config::default()));
        let empty_executor: Arc<dyn ToolExecutor> = Arc::new(
            bamboo_tools::BuiltinToolExecutor::with_registry(bamboo_tools::ToolRegistry::new()),
        );
        let builder = AgentBuilder::new()
            .provider(Arc::new(NeverCalledProvider))
            .default_tools(empty_executor)
            .config(explicit_config)
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .expect("an injected provider must skip invalid config-driven creation");

        assert!(builder.provider_override.is_some());
        assert!(builder.default_tools_override.is_some());
        assert!(builder.config_override.is_some());
        let agent = builder.build().expect("explicit dependencies should build");
        assert!(agent.inner.default_tools().list_tools().is_empty());

        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        let later_executor: Arc<dyn ToolExecutor> = Arc::new(
            bamboo_tools::BuiltinToolExecutor::with_registry(bamboo_tools::ToolRegistry::new()),
        );
        let later = AgentBuilder::new()
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap()
            .default_tools(later_executor)
            .build()
            .unwrap();
        assert!(later.inner.default_tools().list_tools().is_empty());
    }

    #[tokio::test]
    async fn explicit_config_model_is_captured_before_or_after_defaults() {
        let copilot_config = || {
            let mut config = Config::default();
            config.provider = "copilot".to_string();
            Arc::new(RwLock::new(config))
        };

        let before_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            before_dir.path().join("config.json"),
            r#"{"provider":"unknown-provider"}"#,
        )
        .unwrap();
        let before = AgentBuilder::new()
            .provider(Arc::new(NeverCalledProvider))
            .config(copilot_config())
            .with_defaults_for_data_dir(before_dir.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(before.new_session("before").unwrap().model, "gpt-4o");

        let after_dir = tempfile::tempdir().unwrap();
        write_test_config(after_dir.path());
        let after = AgentBuilder::new()
            .with_defaults_for_data_dir(after_dir.path().to_path_buf())
            .await
            .unwrap()
            .config(copilot_config())
            .build()
            .unwrap();
        assert_eq!(after.new_session("after").unwrap().model, "gpt-4o");

        let explicit_dir = tempfile::tempdir().unwrap();
        write_test_config(explicit_dir.path());
        let explicit = AgentBuilder::new()
            .model("explicit-model")
            .with_defaults_for_data_dir(explicit_dir.path().to_path_buf())
            .await
            .unwrap()
            .config(copilot_config())
            .build()
            .unwrap();
        assert_eq!(
            explicit.new_session("explicit").unwrap().model,
            "explicit-model"
        );
    }

    #[tokio::test]
    async fn explicit_tool_policy_has_final_precedence_over_injected_executor() {
        let tmp = tempfile::tempdir().unwrap();
        write_test_config(tmp.path());
        let full_executor: Arc<dyn ToolExecutor> =
            Arc::new(bamboo_tools::BuiltinToolExecutor::new());
        let agent = AgentBuilder::new()
            .no_tools()
            .default_tools(full_executor)
            .with_defaults_for_data_dir(tmp.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        assert!(agent.inner.default_tools().list_tools().is_empty());
    }
}
