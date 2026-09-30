use std::sync::Arc;

use crate::runtime::execution::{ExternalChildRunner, SessionInboxRuntimeBinding, SpawnJob};
use async_trait::async_trait;
use bamboo_a2a::A2AJsonRpcClient;
use bamboo_agent_core::{AgentError, AgentEvent};
use bamboo_llm::Config;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::a2a_adapter::A2AExternalChildRunner;
use super::actor_adapter::{ActorChildRunner, ChildApprovalReviewer, CodexRunTokenAuthority};
use super::config::{parse_external_agents, ExternalAgentProtocol};

/// Host-owned observation of its actual concrete tool surface at activation.
/// The caller cannot submit a resolved ceiling through the public tool schema.
#[async_trait]
pub trait NativeToolCeilingSource: Send + Sync {
    async fn observe(&self, session: &bamboo_domain::Session) -> Result<Vec<String>, String>;
}

fn codex_auth_mode_name(mode: bamboo_config::CodexAuthMode) -> String {
    match mode {
        bamboo_config::CodexAuthMode::Inherit => "inherit",
        bamboo_config::CodexAuthMode::ApiKey => "api_key",
        bamboo_config::CodexAuthMode::Custom => "custom",
        bamboo_config::CodexAuthMode::Bamboo => "bamboo",
    }
    .to_string()
}

fn codex_wire_api_name(wire_api: bamboo_config::CodexWireApi) -> String {
    match wire_api {
        bamboo_config::CodexWireApi::Responses => "responses",
    }
    .to_string()
}

fn codex_mode_name(mode: bamboo_config::CodexMode) -> String {
    match mode {
        bamboo_config::CodexMode::Exec => "exec",
        bamboo_config::CodexMode::AppServer => "app_server",
    }
    .to_string()
}

fn codex_sandbox_name(sandbox: bamboo_config::CodexSandbox) -> String {
    match sandbox {
        bamboo_config::CodexSandbox::ReadOnly => "read-only",
        bamboo_config::CodexSandbox::WorkspaceWrite => "workspace-write",
        bamboo_config::CodexSandbox::DangerFullAccess => "danger-full-access",
    }
    .to_string()
}

fn codex_approval_policy_name(policy: bamboo_config::CodexApprovalPolicy) -> String {
    match policy {
        bamboo_config::CodexApprovalPolicy::Never => "never",
        bamboo_config::CodexApprovalPolicy::OnFailure => "on-failure",
        bamboo_config::CodexApprovalPolicy::OnRequest => "on-request",
    }
    .to_string()
}

fn codex_base_url(
    config: &Config,
    mode: bamboo_config::CodexAuthMode,
    custom: Option<String>,
) -> Option<String> {
    match mode {
        bamboo_config::CodexAuthMode::Custom => custom,
        bamboo_config::CodexAuthMode::Bamboo => {
            let scheme = if config.server.tls.is_some() {
                "https"
            } else {
                "http"
            };
            Some(format!(
                "{scheme}://127.0.0.1:{}/openai/v1",
                config.server.port
            ))
        }
        bamboo_config::CodexAuthMode::Inherit | bamboo_config::CodexAuthMode::ApiKey => None,
    }
}

/// Composite router that delegates to the first matching external child runner.
pub struct CompositeExternalChildRunner {
    runners: Vec<Arc<dyn ExternalChildRunner>>,
}

impl CompositeExternalChildRunner {
    pub fn new(runners: Vec<Arc<dyn ExternalChildRunner>>) -> Self {
        Self { runners }
    }
}

#[async_trait]
impl ExternalChildRunner for CompositeExternalChildRunner {
    async fn validate_required_child_context_route(
        &self,
        session: &bamboo_agent_core::Session,
    ) -> Result<(), String> {
        for runner in &self.runners {
            if runner.should_handle(session).await {
                return runner.validate_required_child_context_route(session).await;
            }
        }
        Err("required_child_context_unsupported: no matching registered worker route".into())
    }
    async fn should_handle(&self, session: &bamboo_agent_core::Session) -> bool {
        for runner in &self.runners {
            if runner.should_handle(session).await {
                return true;
            }
        }
        false
    }

    async fn execute_external_child(
        &self,
        session: &mut bamboo_agent_core::Session,
        job: &SpawnJob,
        event_tx: mpsc::Sender<AgentEvent>,
        cancel_token: CancellationToken,
    ) -> crate::runtime::runner::Result<()> {
        for runner in &self.runners {
            if runner.should_handle(session).await {
                return runner
                    .execute_external_child(session, job, event_tx, cancel_token)
                    .await;
            }
        }
        Err(AgentError::LLM(
            "No matching external child runner found for session metadata".to_string(),
        ))
    }

    async fn confirm_durable_child_delivery(
        &self,
        session: &bamboo_agent_core::Session,
        activation_run_id: &str,
        save_succeeded: bool,
    ) -> Result<(), String> {
        for runner in &self.runners {
            runner
                .confirm_durable_child_delivery(session, activation_run_id, save_succeeded)
                .await?;
        }
        Ok(())
    }

    async fn prepare_durable_child_delivery(
        &self,
        session: &bamboo_agent_core::Session,
        activation_run_id: &str,
    ) -> Result<bool, String> {
        let mut prepared = false;
        for runner in &self.runners {
            prepared |= runner
                .prepare_durable_child_delivery(session, activation_run_id)
                .await?;
        }
        Ok(prepared)
    }

    /// #68: fan the per-run escalation bridge out to every inner runner. The
    /// composite is what `build_external_child_runner` returns and what the
    /// worker retains, so without this forward the bind would hit the trait's
    /// no-op default and the wrapped `ActorChildRunner`s would never see it.
    fn set_escalation_bridge(&self, bridge: Option<bamboo_subagent::executor::HostBridge>) {
        for runner in &self.runners {
            runner.set_escalation_bridge(bridge.clone());
        }
    }

    fn set_session_inbox_runtime(&self, binding: Option<SessionInboxRuntimeBinding>) {
        for runner in &self.runners {
            runner.set_session_inbox_runtime(binding.clone());
        }
    }

    fn set_actor_directory_store(&self, store: Option<Arc<bamboo_storage::SessionStoreV2>>) {
        for runner in &self.runners {
            runner.set_actor_directory_store(store.clone());
        }
    }

    fn set_canonical_subagent_tool(&self, tool: Option<Arc<dyn bamboo_agent_core::tools::Tool>>) {
        for runner in &self.runners {
            runner.set_canonical_subagent_tool(tool.clone());
        }
    }

    fn set_actor_event_observer(
        &self,
        observer: Option<Arc<dyn super::actor_event_stream::ActorEventObserver>>,
    ) {
        for runner in &self.runners {
            runner.set_actor_event_observer(observer.clone());
        }
    }
}

/// Build the child runner from the application config.
///
/// Sub-agents always run as actors (the in-process runtime was removed), so the
/// built-in **local actor** worker is always part of the composite — its worker
/// binary, arguments, and discovery dir are all derived; no expert tables
/// needed. Expert `externalAgents` profiles add extra routers so
/// `external.agent_id` metadata can pin specific roles to other agents. Returns
/// a composite router that delegates to the first matching runner.
pub fn build_external_child_runner(config: &Config) -> Arc<dyn ExternalChildRunner> {
    build_external_child_runner_with_registry(config, None)
}

/// Build the child runner with an AppState-scoped durable approval registry.
pub fn build_external_child_runner_with_registry(
    config: &Config,
    approval_registry: Option<super::approval_registry::SharedApprovalRegistry>,
) -> Arc<dyn ExternalChildRunner> {
    build_external_child_runner_with_registry_and_reviewer(config, approval_registry, None, None)
}

/// Build the child runner with durable approval state and an optional
/// parent-agent model reviewer for forced-ask requests.
pub fn build_external_child_runner_with_registry_and_reviewer(
    config: &Config,
    approval_registry: Option<super::approval_registry::SharedApprovalRegistry>,
    approval_reviewer: Option<Arc<dyn ChildApprovalReviewer>>,
    permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
) -> Arc<dyn ExternalChildRunner> {
    build_external_child_runner_with_codex_tokens(
        config,
        approval_registry,
        approval_reviewer,
        permission_config,
        None,
    )
}

/// Full server wiring, including the process-ephemeral Codex per-run token
/// authority. Non-server callers keep using the compatibility wrapper above.
pub fn build_external_child_runner_with_codex_tokens(
    config: &Config,
    approval_registry: Option<super::approval_registry::SharedApprovalRegistry>,
    approval_reviewer: Option<Arc<dyn ChildApprovalReviewer>>,
    permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    codex_run_tokens: Option<Arc<dyn CodexRunTokenAuthority>>,
) -> Arc<dyn ExternalChildRunner> {
    build_external_child_runner_internal(
        config,
        None,
        approval_registry,
        approval_reviewer,
        permission_config,
        codex_run_tokens,
        None,
    )
}

/// Full server wiring with an AppState-owned live config source. Provider
/// credentials and the default provider are resolved for each new child
/// activation, so provider reloads do not require rebuilding the actor runner.
pub fn build_external_child_runner_with_live_config_and_codex_tokens(
    config: &Config,
    live_provider_config: Arc<tokio::sync::RwLock<Config>>,
    approval_registry: Option<super::approval_registry::SharedApprovalRegistry>,
    approval_reviewer: Option<Arc<dyn ChildApprovalReviewer>>,
    permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    codex_run_tokens: Option<Arc<dyn CodexRunTokenAuthority>>,
) -> Arc<dyn ExternalChildRunner> {
    build_external_child_runner_internal(
        config,
        Some(live_provider_config),
        approval_registry,
        approval_reviewer,
        permission_config,
        codex_run_tokens,
        None,
    )
}

/// The server binds this to the same Builtin Arc and complete base routing
/// chain assembled for this AppState, before applying the Root role fence.
#[allow(clippy::too_many_arguments)]
pub fn build_external_child_runner_with_native_tool_ceiling(
    config: &Config,
    live_provider_config: Arc<tokio::sync::RwLock<Config>>,
    approval_registry: Option<super::approval_registry::SharedApprovalRegistry>,
    approval_reviewer: Option<Arc<dyn ChildApprovalReviewer>>,
    permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    codex_run_tokens: Option<Arc<dyn CodexRunTokenAuthority>>,
    native_tool_ceiling: Arc<dyn NativeToolCeilingSource>,
) -> Arc<dyn ExternalChildRunner> {
    build_external_child_runner_internal(
        config,
        Some(live_provider_config),
        approval_registry,
        approval_reviewer,
        permission_config,
        codex_run_tokens,
        Some(native_tool_ceiling),
    )
}

fn build_external_child_runner_internal(
    config: &Config,
    live_provider_config: Option<Arc<tokio::sync::RwLock<Config>>>,
    approval_registry: Option<super::approval_registry::SharedApprovalRegistry>,
    approval_reviewer: Option<Arc<dyn ChildApprovalReviewer>>,
    permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    codex_run_tokens: Option<Arc<dyn CodexRunTokenAuthority>>,
    native_tool_ceiling: Option<Arc<dyn NativeToolCeilingSource>>,
) -> Arc<dyn ExternalChildRunner> {
    let agents = parse_external_agents(config);

    let mut runners: Vec<Arc<dyn ExternalChildRunner>> = Vec::new();

    // The built-in local actor worker is the default runtime for every
    // sub-agent. Always build it; a build failure here is logged and leaves the
    // composite without a default handler (dispatch then errors clearly).
    match build_local_actor_runner(
        config,
        approval_registry.clone(),
        approval_reviewer.clone(),
        permission_config.clone(),
        codex_run_tokens.clone(),
        live_provider_config.clone(),
        native_tool_ceiling,
    ) {
        Ok(runner) => runners.push(runner),
        Err(e) => tracing::error!("local actor sub-agent runner unavailable: {e}"),
    }

    for (_agent_id, profile) in agents {
        // Actor protocol: spawn a local worker binary over the bamboo-subagent WS protocol.
        if matches!(profile.protocol, ExternalAgentProtocol::Actor) {
            let Some(worker_bin) = profile.worker_bin.as_ref() else {
                tracing::error!(
                    "Actor agent profile {} has no worker_bin; skipping",
                    profile.agent_id
                );
                continue;
            };
            // #217: default under the persistent data-dir subagents home
            // instead of `env::temp_dir()`, so fabric discovery state
            // survives reboots and stays inside the tenant's data dir.
            let fabric_dir = profile
                .fabric_dir
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(bamboo_config::paths::subagents_dir);
            let executor = match profile.executor.as_deref() {
                Some("echo") => bamboo_subagent::provision::ExecutorSpec::Echo,
                Some("bamboo_runtime") | None => {
                    bamboo_subagent::provision::ExecutorSpec::BambooRuntime
                }
                // #443: binary/model/permission_mode/isolation/env-forward
                // are plumbed from the profile's `claude_code_*` fields.
                Some("claude_code") => bamboo_subagent::provision::ExecutorSpec::ClaudeCode {
                    binary: profile.claude_code_binary.clone(),
                    model: profile.claude_code_model.clone(),
                    permission_mode: profile.claude_code_permission_mode.clone(),
                    inherit_user_config: profile.claude_code_inherit_user_config,
                    forward_env: profile.claude_code_forward_env.clone(),
                },
                Some("codex") => bamboo_subagent::provision::ExecutorSpec::Codex {
                    binary: profile.codex_binary.clone(),
                    model: profile.codex_model.clone(),
                    mode: profile.codex_mode.map(codex_mode_name),
                    sandbox: profile.codex_sandbox.map(codex_sandbox_name),
                    inherit_user_config: None,
                    auth_mode: Some(codex_auth_mode_name(
                        profile.codex_auth_mode.unwrap_or_default(),
                    )),
                    base_url: codex_base_url(
                        config,
                        profile.codex_auth_mode.unwrap_or_default(),
                        profile.codex_base_url.clone(),
                    ),
                    wire_api: profile.codex_wire_api.map(codex_wire_api_name),
                    provider_key_ref: profile
                        .codex_provider_key_ref
                        .as_ref()
                        .map(|reference| reference.as_str().to_string()),
                    forward_env: profile.codex_forward_env.clone(),
                    approval_policy: profile
                        .codex_approval_policy
                        .map(codex_approval_policy_name),
                    network_access: profile.codex_network_access,
                    allow_danger_bypass: profile.codex_allow_danger_bypass,
                    permission_profile: Some(profile.permission_profile.clone()),
                    workspace_owned: None,
                },
                Some(other) => {
                    tracing::error!(
                        "Actor agent profile {} has unknown executor '{}'; skipping",
                        profile.agent_id,
                        other
                    );
                    continue;
                }
            };
            let mut runner = ActorChildRunner::new(
                profile.agent_id.clone(),
                std::path::PathBuf::from(worker_bin),
                profile.worker_args.clone(),
                fabric_dir,
                executor,
                extract_provider_credentials(config),
                config.effective_default_provider().to_string(),
                config
                    .subagents()
                    .max_concurrent
                    .unwrap_or(super::actor_adapter::DEFAULT_MAX_CONCURRENT_ACTORS),
            );
            if let Some(registry) = approval_registry.clone() {
                runner = runner.with_approval_registry(registry);
            }
            if let Some(reviewer) = approval_reviewer.clone() {
                runner = runner.with_approval_reviewer(reviewer);
            }
            if let Some(config) = permission_config.clone() {
                runner = runner.with_permission_config(config);
            }
            if let Some(config) = live_provider_config.clone() {
                runner = runner.with_live_provider_config(config);
            }
            runner = runner.with_codex_run_tokens(codex_run_tokens.clone());
            runners.push(Arc::new(runner));
            continue;
        }

        if !matches!(profile.protocol, ExternalAgentProtocol::A2aJsonRpc) {
            tracing::warn!(
                "External agent profile {} uses unsupported protocol {:?}",
                profile.agent_id,
                profile.protocol
            );
            continue;
        }

        let auth_token = match profile.auth_ref.as_ref() {
            Some(ref_name) => match std::env::var(ref_name) {
                Ok(token) => Some(token),
                Err(_) => {
                    tracing::error!(
                        "External agent profile {} auth_ref env var {} is not set",
                        profile.agent_id,
                        ref_name
                    );
                    continue;
                }
            },
            None => None,
        };

        let client_config = match A2AExternalChildRunner::build_client_config(&profile, auth_token)
        {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::error!(
                    "Failed to build A2A client config for profile {}: {}",
                    profile.agent_id,
                    e
                );
                continue;
            }
        };

        let client = match A2AJsonRpcClient::new(client_config) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(
                    "Failed to create A2A JSON-RPC client for profile {}: {}",
                    profile.agent_id,
                    e
                );
                continue;
            }
        };

        runners.push(Arc::new(A2AExternalChildRunner::new(client, profile)));
    }

    Arc::new(CompositeExternalChildRunner::new(runners))
}

/// Build the built-in local actor runner from the typed `subagents`
/// config. Everything is derived: worker = the current bamboo executable +
/// `subagent-worker`, fabric = per-user temp dir — unless expert fields
/// override them.
fn build_local_actor_runner(
    config: &Config,
    approval_registry: Option<super::approval_registry::SharedApprovalRegistry>,
    approval_reviewer: Option<Arc<dyn ChildApprovalReviewer>>,
    permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    codex_run_tokens: Option<Arc<dyn CodexRunTokenAuthority>>,
    live_provider_config: Option<Arc<tokio::sync::RwLock<Config>>>,
    native_tool_ceiling: Option<Arc<dyn NativeToolCeilingSource>>,
) -> Result<Arc<dyn ExternalChildRunner>, String> {
    let sub = config.subagents();

    let (worker_bin, worker_args) = match &sub.worker_bin {
        Some(custom) => (
            std::path::PathBuf::from(custom),
            sub.worker_args.clone().unwrap_or_default(),
        ),
        None => (
            std::env::current_exe().map_err(|e| format!("cannot locate own executable: {e}"))?,
            sub.worker_args
                .clone()
                .unwrap_or_else(|| vec!["subagent-worker".to_string()]),
        ),
    };

    // #217: default under the persistent data-dir subagents home instead of
    // `env::temp_dir()` (mirrors the `build_external_child_runner` arm above).
    let fabric_dir = sub
        .fabric_dir
        .clone()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(bamboo_config::paths::subagents_dir);

    let executor = subagent_executor_spec(config)?;

    let mut runner = ActorChildRunner::new(
        super::config::LOCAL_ACTOR_AGENT_ID.to_string(),
        worker_bin,
        worker_args,
        fabric_dir,
        executor,
        extract_provider_credentials(config),
        config.effective_default_provider().to_string(),
        sub.max_concurrent
            .unwrap_or(super::actor_adapter::DEFAULT_MAX_CONCURRENT_ACTORS),
    )
    .with_builtin_required_context_route(sub.worker_bin.is_none() && sub.worker_args.is_none())
    .with_remote_placements(resolve_remote_placements(
        &sub.remote_placements,
        &config.cluster_fabric.nodes,
    ))
    .with_schedulable_placements(resolve_schedulable_placements(
        &sub.schedulable_placements,
        &config.cluster_fabric.nodes,
    ))
    .with_bus(sub.broker.as_ref().map(|b| bamboo_subagent::BusEndpoint {
        endpoint: b.endpoint.clone(),
        token: b.token.clone(),
    }))
    .with_codex_run_tokens(codex_run_tokens)
    .with_native_tool_ceiling_source(native_tool_ceiling);
    if let Some(registry) = approval_registry {
        runner = runner.with_approval_registry(registry);
    }
    if let Some(reviewer) = approval_reviewer {
        runner = runner.with_approval_reviewer(reviewer);
    }
    if let Some(config) = permission_config {
        runner = runner.with_permission_config(config);
    }
    if let Some(config) = live_provider_config {
        runner = runner.with_live_provider_config(config);
    }
    Ok(Arc::new(runner))
}

/// Convert the durable typed `subagents` section into the exact worker
/// provisioning executor. This is deliberately independent of actor launch so
/// the settings-to-spawn contract can be tested directly.
fn subagent_executor_spec(
    config: &Config,
) -> Result<bamboo_subagent::provision::ExecutorSpec, String> {
    let sub = config.subagents();
    Ok(match sub.executor.as_deref() {
        Some("echo") => bamboo_subagent::provision::ExecutorSpec::Echo,
        Some("bamboo_runtime") | None => bamboo_subagent::provision::ExecutorSpec::BambooRuntime,
        Some("claude_code") => bamboo_subagent::provision::ExecutorSpec::ClaudeCode {
            binary: sub.claude_code_binary.clone(),
            model: sub.claude_code_model.clone(),
            permission_mode: sub.claude_code_permission_mode.clone(),
            inherit_user_config: sub.claude_code_inherit_user_config,
            forward_env: sub.claude_code_forward_env.clone(),
        },
        Some("codex") => bamboo_subagent::provision::ExecutorSpec::Codex {
            binary: sub.codex_binary.clone(),
            model: sub.codex_model.clone(),
            mode: sub.codex_mode.map(codex_mode_name),
            sandbox: sub.codex_sandbox.map(codex_sandbox_name),
            inherit_user_config: None,
            auth_mode: Some(codex_auth_mode_name(
                sub.codex_auth_mode.unwrap_or_default(),
            )),
            base_url: codex_base_url(
                config,
                sub.codex_auth_mode.unwrap_or_default(),
                sub.codex_base_url.clone(),
            ),
            wire_api: sub.codex_wire_api.map(codex_wire_api_name),
            provider_key_ref: sub
                .codex_provider_key_ref
                .as_ref()
                .map(|reference| reference.as_str().to_string()),
            forward_env: sub.codex_forward_env.clone(),
            approval_policy: sub.codex_approval_policy.map(codex_approval_policy_name),
            network_access: sub.codex_network_access,
            allow_danger_bypass: sub.codex_allow_danger_bypass,
            permission_profile: None,
            workspace_owned: None,
        },
        Some(other) => return Err(format!("unknown subagents.executor '{other}'")),
    })
}

fn valid_placement_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_placement_requirements(value: &bamboo_config::OperatorPlacementRequirements) -> bool {
    valid_placement_identifier(&value.trust_zone)
        && value
            .workspace_label
            .as_deref()
            .is_none_or(valid_placement_identifier)
        && value
            .network_zone
            .as_deref()
            .is_none_or(valid_placement_identifier)
        && value.required_tools.len() <= 256
        && value
            .required_tools
            .iter()
            .all(|tool| valid_placement_identifier(tool))
}

fn valid_scoped_broker_endpoint(value: &str) -> bool {
    value.len() <= 2048
        && url::Url::parse(value).is_ok_and(|url| {
            url.scheme() == "wss"
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
        })
}

fn resolved_broker_token(name: Option<&str>) -> Option<String> {
    name.filter(|name| !name.is_empty() && name.len() <= 256)
        .and_then(|name| std::env::var(name).ok())
        .filter(|token| {
            (32..=256).contains(&token.len()) && token.bytes().all(|byte| byte.is_ascii_graphic())
        })
}

/// Resolve every configured role, including invalid legacy entries. An explicit
/// scheduled role must never silently fall through to Local.
fn resolve_schedulable_placements(
    placements: &[bamboo_config::SchedulablePlacement],
    nodes: &[bamboo_config::cluster_fabric::Node],
) -> std::collections::HashMap<String, super::actor_adapter::ResolvedSchedulablePlacement> {
    let mut out = std::collections::HashMap::new();
    for p in placements {
        let token = resolved_broker_token(p.token_env.as_deref());
        let valid = valid_placement_identifier(&p.pool)
            && valid_scoped_broker_endpoint(&p.registry_url)
            && p.token_env.is_some()
            && token.is_some()
            && p.ca_cert_file.as_ref().is_some_and(|path| {
                bamboo_broker::client_config_trusting_cert(std::path::Path::new(path)).is_ok()
            })
            && p.broker_parent.as_ref().is_some_and(|parent| {
                valid_placement_identifier(&parent.parent_mailbox)
                    && valid_placement_identifier(&parent.parent_role)
            })
            && p.placement_requirements
                .as_ref()
                .is_some_and(valid_placement_requirements);
        let resolved = super::actor_adapter::ResolvedSchedulablePlacement {
            pool: p.pool.clone(),
            host_label: node_label_for_role(nodes, &p.pool),
            endpoint: p.registry_url.clone(),
            token: if valid { token } else { None },
            ca_cert_file: p.ca_cert_file.as_ref().map(std::path::PathBuf::from),
            broker_parent: if valid { p.broker_parent.clone() } else { None },
            requirements: if valid {
                p.placement_requirements.clone()
            } else {
                None
            },
        };
        if out.insert(p.role.clone(), resolved).is_some() {
            // Ambiguous authority is unavailable, regardless of entry order.
            if let Some(route) = out.get_mut(&p.role) {
                route.broker_parent = None;
            }
        }
    }
    out
}

/// Friendly display name for a cluster node whose worker serves `role`
/// (`deploy.default_role`) — the operator `label`, else its ssh host. Used to
/// stamp the UI placement badge from the node's own metadata.
fn node_label_for_role(
    nodes: &[bamboo_config::cluster_fabric::Node],
    role: &str,
) -> Option<String> {
    nodes
        .iter()
        .find(|n| n.deploy.default_role.as_deref() == Some(role))
        .map(node_display_name)
}

/// Friendly display name for a cluster node whose ssh host matches `endpoint`'s
/// host — so a `remote_placements` endpoint pointing at a known node shows the
/// node's label rather than a bare IP.
fn node_label_for_endpoint(
    nodes: &[bamboo_config::cluster_fabric::Node],
    endpoint: &str,
) -> Option<String> {
    let host = endpoint
        .trim()
        .trim_start_matches("wss://")
        .trim_start_matches("ws://")
        .split(['/', ':'])
        .next()
        .unwrap_or("");
    if host.is_empty() {
        return None;
    }
    nodes
        .iter()
        .find(|n| match &n.placement {
            bamboo_config::cluster_fabric::NodePlacement::Ssh(t) => t.host == host,
            bamboo_config::cluster_fabric::NodePlacement::Local => false,
        })
        .map(node_display_name)
}

fn node_display_name(n: &bamboo_config::cluster_fabric::Node) -> String {
    if !n.label.trim().is_empty() {
        return n.label.clone();
    }
    match &n.placement {
        bamboo_config::cluster_fabric::NodePlacement::Ssh(t) => t.host.clone(),
        bamboo_config::cluster_fabric::NodePlacement::Local => "local".to_string(),
    }
}

/// Resolve pinned remote placements. Broker peer selection is required to run;
/// old direct-worker configurations remain visible but unavailable, without
/// exposing their credentials or silently rerouting to Local.
fn resolve_remote_placements(
    placements: &[bamboo_config::RemoteActorPlacement],
    nodes: &[bamboo_config::cluster_fabric::Node],
) -> std::collections::HashMap<String, super::actor_adapter::ResolvedRemotePlacement> {
    let mut out = std::collections::HashMap::new();
    for p in placements {
        let duplicate_strict = placements
            .iter()
            .any(|other| other.role == p.role && other.broker_peer.is_some())
            && placements
                .iter()
                .filter(|other| other.role == p.role)
                .count()
                != 1;
        if duplicate_strict {
            out.insert(
                p.role.clone(),
                super::actor_adapter::ResolvedRemotePlacement {
                    endpoint: String::new(),
                    token: None,
                    ca_cert_file: None,
                    host_label: Some("remote".into()),
                    broker_peer: Some(Err(())),
                    requirements: None,
                },
            );
            continue;
        }
        if let Some(peer) = &p.broker_peer {
            let token = resolved_broker_token(p.token_env.as_deref());
            let valid = peer.valid()
                && valid_scoped_broker_endpoint(&p.endpoint)
                && p.token_env.is_some()
                && token.is_some()
                && p.ca_cert_file.as_ref().is_some_and(|path| {
                    bamboo_broker::client_config_trusting_cert(std::path::Path::new(path)).is_ok()
                })
                && p.placement_requirements
                    .as_ref()
                    .is_some_and(valid_placement_requirements);
            out.insert(
                p.role.clone(),
                super::actor_adapter::ResolvedRemotePlacement {
                    endpoint: p.endpoint.clone(),
                    token: if valid { token } else { None },
                    ca_cert_file: p.ca_cert_file.as_ref().map(std::path::PathBuf::from),
                    host_label: Some("remote".into()),
                    broker_peer: Some(if valid { Ok(peer.clone()) } else { Err(()) }),
                    requirements: if valid {
                        p.placement_requirements.clone()
                    } else {
                        None
                    },
                },
            );
            continue; // Invalid explicit routes remain selected, never Local fallback.
        }
        out.insert(
            p.role.clone(),
            super::actor_adapter::ResolvedRemotePlacement {
                endpoint: p.endpoint.clone(),
                token: None,
                ca_cert_file: p.ca_cert_file.as_ref().map(std::path::PathBuf::from),
                // Badge from the node's own metadata when the endpoint points at
                // a known cluster node; else the endpoint host is used downstream.
                host_label: node_label_for_endpoint(nodes, &p.endpoint),
                broker_peer: Some(Err(())),
                requirements: None,
            },
        );
    }
    out
}

/// Snapshot per-provider credentials from the parent config for actor
/// provisioning. `api_key` (plaintext, in-memory only) is `#[serde(skip_serializing)]`
/// on every legacy single-instance provider struct — it's hydrated from
/// `api_key_encrypted` at load time but deliberately never round-tripped
/// through serde, so a `serde_json::to_value` projection of `config.providers`
/// sees none of it (#495). Read each typed struct's `api_key` field directly
/// instead, mirroring how `provider_instances` below already has to.
pub fn extract_provider_credentials(
    config: &Config,
) -> Vec<bamboo_subagent::provision::ScopedCredential> {
    let mut out = Vec::new();

    fn push_instance(
        out: &mut Vec<bamboo_subagent::provision::ScopedCredential>,
        id: &str,
        instance: &bamboo_config::ProviderInstanceConfig,
    ) {
        if !instance.enabled {
            return;
        }
        let api_key = instance.api_key.trim().to_string();
        if api_key.is_empty() {
            return;
        }
        out.push(bamboo_subagent::provision::ScopedCredential {
            provider: id.to_string(),
            api_key,
            base_url: instance.base_url.clone(),
            provider_type: Some(instance.provider_type.clone()),
            credential_ref: instance
                .credential_ref
                .as_ref()
                .map(|reference| reference.as_str().to_string()),
        });
    }

    if !config.provider_instances.is_empty() {
        // Native instances are authoritative. Export enabled explicit
        // instances only; stale legacy slots must not leak into child workers.
        for (id, instance) in &config.provider_instances {
            push_instance(&mut out, id, instance);
        }

        // Narrow #780 compatibility seam: a hybrid default may still name a
        // real legacy alias not yet materialized. Mirror the registry's exact
        // rule and add only that default, never every legacy credential.
        let default_id = config.effective_default_provider();
        if !config.provider_instances.contains_key(default_id) {
            if let Some((_, instance)) = bamboo_config::synthesize_legacy_instances(config)
                .into_iter()
                .find(|(id, _)| id == default_id)
            {
                push_instance(&mut out, default_id, &instance);
            }
        }
        return out;
    }

    // Legacy single-instance slots: providers.anthropic / openai / gemini /
    // bodhi. `copilot` is intentionally omitted — it authenticates via device
    // flow and has no `api_key` field to extract.
    let mut push_legacy =
        |name: &str, api_key: &str, base_url: Option<String>, credential_ref: Option<String>| {
            let api_key = api_key.trim().to_string();
            if api_key.is_empty() {
                return;
            }
            out.push(bamboo_subagent::provision::ScopedCredential {
                provider: name.to_string(),
                api_key,
                base_url,
                provider_type: Some(name.to_string()),
                credential_ref,
            });
        };
    if let Some(c) = &config.providers().openai {
        push_legacy(
            "openai",
            &c.api_key,
            c.base_url.clone(),
            c.credential_ref
                .as_ref()
                .map(|reference| reference.as_str().to_string()),
        );
    }
    if let Some(c) = &config.providers().anthropic {
        push_legacy(
            "anthropic",
            &c.api_key,
            c.base_url.clone(),
            c.credential_ref
                .as_ref()
                .map(|reference| reference.as_str().to_string()),
        );
    }
    if let Some(c) = &config.providers().gemini {
        push_legacy(
            "gemini",
            &c.api_key,
            c.base_url.clone(),
            c.credential_ref
                .as_ref()
                .map(|reference| reference.as_str().to_string()),
        );
    }
    if let Some(c) = &config.providers().bodhi {
        push_legacy(
            "bodhi",
            &c.api_key,
            c.base_url.clone(),
            c.credential_ref
                .as_ref()
                .map(|reference| reference.as_str().to_string()),
        );
    }

    out
}

#[cfg(test)]
mod codex_runtime_config_tests {
    #[tokio::test]
    async fn required_context_uses_registered_launch_snapshot_after_live_config_flip() {
        let mut candidate = bamboo_agent_core::Session::new("preflight", "model");
        candidate.metadata = super::super::config::resolve_runtime_metadata(
            &bamboo_llm::Config::default(),
            "worker",
        );
        candidate
            .metadata
            .insert("subagent_type".into(), "worker".into());
        for kind in ["custom", "args", "codex", "remote"] {
            let mut config = bamboo_llm::Config::default();
            let sub = config.subagents_mut();
            sub.broker = Some(bamboo_config::BrokerClientConfig {
                endpoint: "ws://127.0.0.1:9998".into(),
                token: "fixture".into(),
                ..Default::default()
            });
            match kind {
                "custom" => sub.worker_bin = Some("/bin/false".into()),
                "args" => sub.worker_args = Some(vec!["subagent-worker".into()]),
                "codex" => sub.executor = Some("codex".into()),
                _ => sub
                    .remote_placements
                    .push(bamboo_config::RemoteActorPlacement {
                        role: "worker".into(),
                        endpoint: "ws://127.0.0.1:9999".into(),
                        ..Default::default()
                    }),
            }
            let live = std::sync::Arc::new(tokio::sync::RwLock::new(config.clone()));
            let runner = super::build_external_child_runner_with_live_config_and_codex_tokens(
                &config,
                live.clone(),
                None,
                None,
                None,
                None,
            );
            *live.write().await = bamboo_llm::Config::default();
            let error = runner
                .validate_required_child_context_route(&candidate)
                .await
                .unwrap_err();
            assert!(
                error.contains("required_child_context_unsupported"),
                "{kind}"
            );
        }
    }

    use super::{
        codex_approval_policy_name, codex_auth_mode_name, codex_base_url, codex_mode_name,
        codex_sandbox_name, codex_wire_api_name, subagent_executor_spec,
    };
    use bamboo_config::{
        CodexApprovalPolicy, CodexAuthMode, CodexMode, CodexSandbox, CodexWireApi, CredentialRef,
    };
    use bamboo_llm::Config;
    use bamboo_subagent::provision::ExecutorSpec;

    #[test]
    fn codex_runtime_mapping_keeps_parent_loopback_and_custom_url_unambiguous() {
        let mut config = Config::default();
        config.server.port = 5700;

        assert_eq!(codex_auth_mode_name(CodexAuthMode::Bamboo), "bamboo");
        assert_eq!(codex_mode_name(CodexMode::AppServer), "app_server");
        assert_eq!(codex_wire_api_name(CodexWireApi::Responses), "responses");
        assert_eq!(codex_sandbox_name(CodexSandbox::ReadOnly), "read-only");
        assert_eq!(
            codex_sandbox_name(CodexSandbox::WorkspaceWrite),
            "workspace-write"
        );
        assert_eq!(
            codex_approval_policy_name(CodexApprovalPolicy::OnFailure),
            "on-failure"
        );
        assert_eq!(
            codex_base_url(&config, CodexAuthMode::Bamboo, None).as_deref(),
            Some("http://127.0.0.1:5700/openai/v1")
        );
        assert_eq!(
            codex_base_url(
                &config,
                CodexAuthMode::Custom,
                Some("https://provider.example/v1".to_string()),
            )
            .as_deref(),
            Some("https://provider.example/v1")
        );
        assert_eq!(codex_base_url(&config, CodexAuthMode::Inherit, None), None);
        assert_eq!(codex_base_url(&config, CodexAuthMode::ApiKey, None), None);
    }

    #[test]
    fn durable_codex_fields_map_without_loss_to_worker_spawn_spec() {
        let mut config = Config::default();
        let subagents = config.subagents_mut();
        subagents.executor = Some("codex".to_string());
        subagents.codex_binary = Some("/opt/codex/bin/codex".to_string());
        subagents.codex_model = Some("gpt-5.4".to_string());
        subagents.codex_mode = Some(CodexMode::AppServer);
        subagents.codex_auth_mode = Some(CodexAuthMode::Custom);
        subagents.codex_base_url = Some("https://provider.example/v1".to_string());
        subagents.codex_wire_api = Some(CodexWireApi::Responses);
        subagents.codex_provider_key_ref = Some(
            CredentialRef::parse("provider.codex-work.api_key").expect("valid credential ref"),
        );
        subagents.codex_forward_env = Some(vec!["HTTPS_PROXY".to_string()]);
        subagents.codex_sandbox = Some(CodexSandbox::WorkspaceWrite);
        subagents.codex_approval_policy = Some(CodexApprovalPolicy::OnRequest);
        subagents.codex_network_access = Some(true);
        subagents.codex_allow_danger_bypass = Some(false);

        let spec = subagent_executor_spec(&config).expect("Codex config maps to executor spec");
        let ExecutorSpec::Codex {
            binary,
            model,
            mode,
            sandbox,
            auth_mode,
            base_url,
            wire_api,
            provider_key_ref,
            forward_env,
            approval_policy,
            network_access,
            allow_danger_bypass,
            ..
        } = spec
        else {
            panic!("expected Codex executor spec");
        };
        assert_eq!(binary.as_deref(), Some("/opt/codex/bin/codex"));
        assert_eq!(model.as_deref(), Some("gpt-5.4"));
        assert_eq!(mode.as_deref(), Some("app_server"));
        assert_eq!(sandbox.as_deref(), Some("workspace-write"));
        assert_eq!(auth_mode.as_deref(), Some("custom"));
        assert_eq!(base_url.as_deref(), Some("https://provider.example/v1"));
        assert_eq!(wire_api.as_deref(), Some("responses"));
        assert_eq!(
            provider_key_ref.as_deref(),
            Some("provider.codex-work.api_key")
        );
        assert_eq!(forward_env, Some(vec!["HTTPS_PROXY".to_string()]));
        assert_eq!(approval_policy.as_deref(), Some("on-request"));
        assert_eq!(network_access, Some(true));
        assert_eq!(allow_danger_bypass, Some(false));
    }
}

#[cfg(test)]
mod extract_provider_credentials_tests {
    use super::extract_provider_credentials;
    use bamboo_config::{
        AnthropicConfig, BodhiConfig, Config, OpenAIConfig, ProviderInstanceConfig,
    };

    fn instance(provider_type: &str, api_key: &str) -> ProviderInstanceConfig {
        ProviderInstanceConfig {
            provider_type: provider_type.to_string(),
            label: None,
            api_key: api_key.to_string(),
            api_key_encrypted: None,
            credential_ref: None,
            base_url: None,
            model: None,
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: Vec::new(),
            request_overrides: None,
            enabled: true,
            extra: Default::default(),
        }
    }

    #[test]
    fn no_config_yields_no_credentials() {
        let config = Config::default();
        assert!(extract_provider_credentials(&config).is_empty());
    }

    /// #495 — a legacy single-instance provider (`config.providers.anthropic`
    /// etc.) must yield its `api_key` even though the field is
    /// `#[serde(skip_serializing)]`, because the extraction now reads the
    /// typed struct instead of projecting through `serde_json::to_value`.
    #[test]
    fn legacy_only_config_yields_credential() {
        let mut config = Config::default();
        config.providers_mut().anthropic = Some(AnthropicConfig {
            api_key: "sk-ant-legacy".to_string(),
            base_url: Some("https://api.anthropic.com".to_string()),
            ..Default::default()
        });

        let creds = extract_provider_credentials(&config);
        assert_eq!(creds.len(), 1);
        let c = &creds[0];
        assert_eq!(c.provider, "anthropic");
        assert_eq!(c.api_key, "sk-ant-legacy");
        assert_eq!(c.base_url.as_deref(), Some("https://api.anthropic.com"));
        assert_eq!(c.provider_type.as_deref(), Some("anthropic"));
    }

    /// `bodhi` doesn't derive `Default`, so it's exercised separately —
    /// covers the last of the four legacy structs the fix touches
    /// (openai/anthropic/gemini already share the `Default`-derive path).
    #[test]
    fn legacy_bodhi_config_yields_credential() {
        let mut config = Config::default();
        config.providers_mut().bodhi = Some(BodhiConfig {
            api_key: "bhi_sk_legacy".to_string(),
            api_key_encrypted: None,
            credential_ref: None,
            base_url: None,
            target_provider: None,
            reasoning_effort: None,
            extra: Default::default(),
        });

        let creds = extract_provider_credentials(&config);
        assert_eq!(creds.len(), 1);
        assert_eq!(creds[0].provider, "bodhi");
        assert_eq!(creds[0].api_key, "bhi_sk_legacy");
    }

    /// A legacy slot with an empty `api_key` (struct present but never
    /// configured) must not produce a bogus empty credential.
    #[test]
    fn legacy_config_with_empty_api_key_is_skipped() {
        let mut config = Config::default();
        config.providers_mut().openai = Some(OpenAIConfig::default());
        assert!(extract_provider_credentials(&config).is_empty());
    }

    /// Explicit instances are authoritative: unrelated stale legacy slots do
    /// not cross the actor provisioning boundary.
    #[test]
    fn instance_mode_omits_unrelated_legacy_credentials() {
        let mut config = Config::default();
        config.providers_mut().anthropic = Some(AnthropicConfig {
            api_key: "sk-ant-legacy".to_string(),
            ..Default::default()
        });
        let mut openai_work = instance("openai", "sk-oai-work");
        openai_work.credential_ref = Some(
            bamboo_config::CredentialRef::parse("provider.openai-work.api_key")
                .expect("valid provider credential reference"),
        );
        config
            .provider_instances
            .insert("openai-work".to_string(), openai_work);
        config.default_provider_instance = Some("openai-work".to_string());

        let creds = extract_provider_credentials(&config);

        assert_eq!(creds.len(), 1);
        assert_eq!(creds[0].provider, "openai-work");
        assert_eq!(creds[0].api_key, "sk-oai-work");
        assert_eq!(creds[0].provider_type.as_deref(), Some("openai"));
        assert_eq!(
            creds[0].credential_ref.as_deref(),
            Some("provider.openai-work.api_key")
        );
    }

    #[test]
    fn hybrid_legacy_default_is_the_only_legacy_credential_exported() {
        let mut config = Config::default();
        config.providers_mut().anthropic = Some(AnthropicConfig {
            api_key: "sk-ant-default".to_string(),
            ..Default::default()
        });
        config.providers_mut().openai = Some(OpenAIConfig {
            api_key: "sk-oai-stale".to_string(),
            ..Default::default()
        });
        config
            .provider_instances
            .insert("work".to_string(), instance("openai", "sk-oai-work"));
        config.default_provider_instance = Some("anthropic".to_string());

        let mut creds = extract_provider_credentials(&config);
        creds.sort_by(|a, b| a.provider.cmp(&b.provider));

        assert_eq!(creds.len(), 2);
        assert_eq!(creds[0].provider, "anthropic");
        assert_eq!(creds[0].api_key, "sk-ant-default");
        assert_eq!(creds[1].provider, "work");
        assert_eq!(creds[1].api_key, "sk-oai-work");
        assert!(creds
            .iter()
            .all(|credential| credential.api_key != "sk-oai-stale"));
    }

    #[test]
    fn hybrid_legacy_provider_fallback_without_explicit_default_remains_exportable() {
        let mut config = Config::default();
        config.provider = "anthropic".to_string();
        config.providers_mut().anthropic = Some(AnthropicConfig {
            api_key: "sk-ant-effective-default".to_string(),
            ..Default::default()
        });
        config
            .provider_instances
            .insert("work".to_string(), instance("openai", "sk-oai-work"));

        let mut creds = extract_provider_credentials(&config);
        creds.sort_by(|a, b| a.provider.cmp(&b.provider));

        assert_eq!(creds.len(), 2);
        assert_eq!(creds[0].provider, "anthropic");
        assert_eq!(creds[1].provider, "work");
    }

    #[test]
    fn disabled_instance_credential_is_not_exported() {
        let mut config = Config::default();
        let mut disabled = instance("openai", "sk-disabled");
        disabled.enabled = false;
        config
            .provider_instances
            .insert("disabled".to_string(), disabled);

        assert!(extract_provider_credentials(&config).is_empty());
    }
}

#[cfg(test)]
mod placement_resolver_tests {
    use super::{node_display_name, resolve_remote_placements, resolve_schedulable_placements};
    use bamboo_config::cluster_fabric::{
        DeployProfile, Node, NodePlacement, SshAuth, SshTarget, TrustLevel,
    };
    use bamboo_config::{RemoteActorPlacement, SchedulablePlacement};

    #[test]
    fn explicit_broker_route_retains_unavailable_selection_and_redacted_debug() {
        let route = bamboo_config::RemoteBrokerPeer {
            parent_mailbox: "host".into(),
            worker_mailbox: "worker".into(),
            parent_role: Some("host".into()),
            worker_role: Some("worker".into()),
        };
        assert!(route.valid());
        let mut invalid = route.clone();
        invalid.worker_mailbox = "Worker".into();
        assert!(!invalid.valid());
        invalid.worker_mailbox = "../worker".into();
        assert!(!invalid.valid());
        let placement = RemoteActorPlacement {
            role: "worker".into(),
            endpoint: "ws://private.invalid".into(),
            token_env: Some("BAMBOO_1431_MISSING_FIXTURE_TOKEN".into()),
            ca_cert_file: None,
            broker_peer: Some(route),
            placement_requirements: None,
        };
        let parsed: RemoteActorPlacement =
            serde_json::from_value(serde_json::to_value(&placement).unwrap()).unwrap();
        assert_eq!(parsed, placement);
        let resolved = resolve_remote_placements(&[placement], &[]);
        let selected = &resolved["worker"];
        assert!(matches!(selected.broker_peer, Some(Err(()))));
        assert!(selected.token.is_none());
        assert_eq!(selected.host_label.as_deref(), Some("remote"));
        assert!(!format!("{selected:?}").contains("private.invalid"));
        assert!(serde_json::from_value::<bamboo_config::RemoteBrokerPeer>(serde_json::json!({
            "parent_mailbox":"host", "worker_mailbox":"worker", "parent_role":null, "worker_role":null,"credential":"secret"
        })).is_err());
    }

    fn ssh_node(id: &str, label: &str, host: &str, default_role: Option<&str>) -> Node {
        Node {
            id: id.into(),
            label: label.into(),
            placement: NodePlacement::Ssh(SshTarget {
                host: host.into(),
                port: 22,
                username: "u".into(),
                auth: SshAuth::SystemSshConfig,
                host_key_fingerprint: None,
            }),
            trust_level: TrustLevel::default(),
            deploy: DeployProfile {
                default_role: default_role.map(String::from),
                ..Default::default()
            },
            state: None,
            enabled: true,
        }
    }

    #[test]
    fn node_display_name_prefers_label_then_ssh_host() {
        let n = ssh_node("n1", "mini", "mini.local", None);
        assert_eq!(node_display_name(&n), "mini");
        let mut unlabeled = n.clone();
        unlabeled.label = String::new();
        assert_eq!(node_display_name(&unlabeled), "mini.local");
    }

    #[test]
    fn schedulable_placement_takes_host_label_from_node_by_default_role() {
        let nodes = vec![ssh_node(
            "n1",
            "mini",
            "mini.local",
            Some("mac-mini-monitor"),
        )];
        let placements = vec![SchedulablePlacement {
            role: "mac-mini-monitor".into(),
            pool: "mac-mini-monitor".into(),
            ..Default::default()
        }];
        let out = resolve_schedulable_placements(&placements, &nodes);
        let r = out.get("mac-mini-monitor").expect("role resolved");
        assert_eq!(r.pool, "mac-mini-monitor");
        assert_eq!(r.host_label.as_deref(), Some("mini"));
    }

    #[test]
    fn remote_placement_takes_host_label_from_node_by_ssh_host() {
        let nodes = vec![ssh_node("n1", "mini", "mini.local", None)];
        let placements = vec![RemoteActorPlacement {
            role: "explorer".into(),
            endpoint: "ws://mini.local:8899".into(),
            ..Default::default()
        }];
        let out = resolve_remote_placements(&placements, &nodes);
        assert_eq!(
            out.get("explorer").unwrap().host_label.as_deref(),
            Some("mini")
        );
        assert!(matches!(out["explorer"].broker_peer, Some(Err(()))));
        assert!(out["explorer"].token.is_none());
    }

    #[test]
    fn no_host_label_when_no_node_matches() {
        let nodes = vec![ssh_node("n1", "mini", "mini.local", Some("other-role"))];
        let sched = vec![SchedulablePlacement {
            role: "x".into(),
            pool: "unmatched".into(),
            ..Default::default()
        }];
        assert_eq!(
            resolve_schedulable_placements(&sched, &nodes)
                .get("x")
                .unwrap()
                .host_label,
            None
        );
        let remote = vec![RemoteActorPlacement {
            role: "y".into(),
            endpoint: "ws://other-host:9000".into(),
            ..Default::default()
        }];
        assert_eq!(
            resolve_remote_placements(&remote, &nodes)
                .get("y")
                .unwrap()
                .host_label,
            None
        );
    }
}
