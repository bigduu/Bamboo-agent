use std::sync::Arc;

use crate::runtime::execution::{ExternalChildRunner, SpawnJob};
use async_trait::async_trait;
use bamboo_a2a::A2AJsonRpcClient;
use bamboo_agent_core::{AgentError, AgentEvent};
use bamboo_llm::Config;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::a2a_adapter::A2AExternalChildRunner;
use super::actor_adapter::{ActorChildRunner, ChildApprovalReviewer};
use super::config::{parse_external_agents, ExternalAgentProtocol};

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

    /// #68: fan the per-run escalation bridge out to every inner runner. The
    /// composite is what `build_external_child_runner` returns and what the
    /// worker retains, so without this forward the bind would hit the trait's
    /// no-op default and the wrapped `ActorChildRunner`s would never see it.
    fn set_escalation_bridge(&self, bridge: Option<bamboo_subagent::executor::HostBridge>) {
        for runner in &self.runners {
            runner.set_escalation_bridge(bridge.clone());
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
                config.provider.clone(),
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

    let executor = match sub.executor.as_deref() {
        Some("echo") => bamboo_subagent::provision::ExecutorSpec::Echo,
        Some("bamboo_runtime") | None => bamboo_subagent::provision::ExecutorSpec::BambooRuntime,
        // #443: binary/model/permission_mode/isolation/env-forward are
        // plumbed from `subagents.claude_code_*` — mirrors the matching arm
        // in `build_external_child_runner` above.
        Some("claude_code") => bamboo_subagent::provision::ExecutorSpec::ClaudeCode {
            binary: sub.claude_code_binary.clone(),
            model: sub.claude_code_model.clone(),
            permission_mode: sub.claude_code_permission_mode.clone(),
            inherit_user_config: sub.claude_code_inherit_user_config,
            forward_env: sub.claude_code_forward_env.clone(),
        },
        Some(other) => return Err(format!("unknown subagents.executor '{other}'")),
    };

    let mut runner = ActorChildRunner::new(
        super::config::LOCAL_ACTOR_AGENT_ID.to_string(),
        worker_bin,
        worker_args,
        fabric_dir,
        executor,
        extract_provider_credentials(config),
        config.provider.clone(),
        sub.max_concurrent
            .unwrap_or(super::actor_adapter::DEFAULT_MAX_CONCURRENT_ACTORS),
    )
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
    }));
    if let Some(registry) = approval_registry {
        runner = runner.with_approval_registry(registry);
    }
    if let Some(reviewer) = approval_reviewer {
        runner = runner.with_approval_reviewer(reviewer);
    }
    if let Some(config) = permission_config {
        runner = runner.with_permission_config(config);
    }
    Ok(Arc::new(runner))
}

/// Resolve config `schedulable_placements` into runner-ready handles (#181, P2b),
/// keyed by role. Mirrors `resolve_remote_placements`: the bearer is read from
/// `token_env` HERE (the raw token never rides the config) and is used for BOTH
/// the registry query and the chosen worker's connect. If `token_env` is `Some`
/// but the env var is UNSET, log an error and SKIP that placement so a misconfig
/// fails SAFE to the local path rather than querying/connecting with no bearer. A
/// placement with no `token_env` is tokenless (trusted/loopback link only).
/// Duplicate roles: last one wins.
fn resolve_schedulable_placements(
    placements: &[bamboo_config::SchedulablePlacement],
    nodes: &[bamboo_config::cluster_fabric::Node],
) -> std::collections::HashMap<String, super::actor_adapter::ResolvedSchedulablePlacement> {
    // Phase 3: a pool is just a bus role. The runner picks a live connected worker
    // of that role via the bus presence query — no registry url / token / cert.
    placements
        .iter()
        .map(|p| {
            (
                p.role.clone(),
                super::actor_adapter::ResolvedSchedulablePlacement {
                    pool: p.pool.clone(),
                    // The badge shows the cluster node's own metadata: a node
                    // deployed to serve this pool (its `deploy.default_role`).
                    host_label: node_label_for_role(nodes, &p.pool),
                },
            )
        })
        .collect()
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

/// Resolve config `remote_placements` into runner-ready handles (#193), keyed by
/// role. The bearer is read from `token_env` HERE (mirroring the A2A `auth_ref`
/// handling at ~runtime.rs:142): if the env var is set use it; if `token_env` is
/// `Some` but the var is UNSET, log an error and SKIP that placement so a
/// misconfig fails SAFE to the local path rather than connecting to a remote
/// worker with no bearer. A placement with no `token_env` connects tokenless
/// (trusted/loopback link only). Duplicate roles: last one wins.
/// Heuristic: does this endpoint reach off-box (so a missing bearer is a real
/// exposure)? `wss://` is always public-grade; for `ws://` we flag any host that
/// is not loopback/localhost.
fn endpoint_looks_public(endpoint: &str) -> bool {
    if endpoint.starts_with("wss://") {
        return true;
    }
    let host = endpoint
        .strip_prefix("ws://")
        .unwrap_or(endpoint)
        .split(['/', ':'])
        .next()
        .unwrap_or("");
    !(host == "localhost" || host == "127.0.0.1" || host == "::1" || host.is_empty())
}

fn resolve_remote_placements(
    placements: &[bamboo_config::RemoteActorPlacement],
    nodes: &[bamboo_config::cluster_fabric::Node],
) -> std::collections::HashMap<String, super::actor_adapter::ResolvedRemotePlacement> {
    let mut out = std::collections::HashMap::new();
    for p in placements {
        let token = match p.token_env.as_deref() {
            Some(env_var) => match std::env::var(env_var) {
                Ok(token) => Some(token),
                Err(_) => {
                    tracing::error!(
                        "remote placement for role '{}' token_env '{}' is not set; \
                         skipping (role falls back to local, NOT unauthenticated remote)",
                        p.role,
                        env_var
                    );
                    continue;
                }
            },
            None => {
                // A tokenless placement is only safe on a trusted link. Warn if
                // it targets what looks like a public endpoint (wss:// or a
                // non-loopback host) so an operator footgun is visible in logs.
                if endpoint_looks_public(&p.endpoint) {
                    tracing::warn!(
                        "remote placement for role '{}' has no token_env but targets a \
                         public-looking endpoint '{}'; work will be dispatched with NO bearer. \
                         Set token_env (and use wss://) for any non-loopback worker.",
                        p.role,
                        p.endpoint
                    );
                }
                None
            }
        };
        out.insert(
            p.role.clone(),
            super::actor_adapter::ResolvedRemotePlacement {
                endpoint: p.endpoint.clone(),
                token,
                ca_cert_file: p.ca_cert_file.as_ref().map(std::path::PathBuf::from),
                // Badge from the node's own metadata when the endpoint points at
                // a known cluster node; else the endpoint host is used downstream.
                host_label: node_label_for_endpoint(nodes, &p.endpoint),
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

    // Legacy single-instance slots: providers.anthropic / openai / gemini /
    // bodhi. `copilot` is intentionally omitted — it authenticates via device
    // flow and has no `api_key` field to extract.
    let mut push_legacy = |name: &str, api_key: &str, base_url: Option<String>| {
        let api_key = api_key.trim().to_string();
        if api_key.is_empty() {
            return;
        }
        out.push(bamboo_subagent::provision::ScopedCredential {
            provider: name.to_string(),
            api_key,
            base_url,
            provider_type: Some(name.to_string()),
        });
    };
    if let Some(c) = &config.providers().openai {
        push_legacy("openai", &c.api_key, c.base_url.clone());
    }
    if let Some(c) = &config.providers().anthropic {
        push_legacy("anthropic", &c.api_key, c.base_url.clone());
    }
    if let Some(c) = &config.providers().gemini {
        push_legacy("gemini", &c.api_key, c.base_url.clone());
    }
    if let Some(c) = &config.providers().bodhi {
        push_legacy("bodhi", &c.api_key, c.base_url.clone());
    }

    // Multi-instance providers: provider_instances keyed by instance id; the
    // child routes by instance id, the worker constructs by provider_type.
    // Read the typed struct directly — `api_key` is hydrated in memory but
    // deliberately `skip_serializing`, so a serde projection would miss it.
    out.extend(config.provider_instances.iter().filter_map(|(id, inst)| {
        let api_key = inst.api_key.trim().to_string();
        if api_key.is_empty() {
            return None;
        }
        Some(bamboo_subagent::provision::ScopedCredential {
            provider: id.clone(),
            api_key,
            base_url: inst.base_url.clone(),
            provider_type: Some(inst.provider_type.clone()),
        })
    }));

    out
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

    /// Legacy + `provider_instances` coexisting: both must surface, with no
    /// duplication/clobbering between the two sources.
    #[test]
    fn legacy_and_instances_both_present_no_duplicates() {
        let mut config = Config::default();
        config.providers_mut().anthropic = Some(AnthropicConfig {
            api_key: "sk-ant-legacy".to_string(),
            ..Default::default()
        });
        config
            .provider_instances
            .insert("openai-work".to_string(), instance("openai", "sk-oai-work"));

        let mut creds = extract_provider_credentials(&config);
        creds.sort_by(|a, b| a.provider.cmp(&b.provider));

        assert_eq!(creds.len(), 2);
        assert_eq!(creds[0].provider, "anthropic");
        assert_eq!(creds[0].api_key, "sk-ant-legacy");
        assert_eq!(creds[1].provider, "openai-work");
        assert_eq!(creds[1].api_key, "sk-oai-work");
        assert_eq!(creds[1].provider_type.as_deref(), Some("openai"));
    }
}

#[cfg(test)]
mod placement_resolver_tests {
    use super::{node_display_name, resolve_remote_placements, resolve_schedulable_placements};
    use bamboo_config::cluster_fabric::{
        DeployProfile, Node, NodePlacement, SshAuth, SshTarget, TrustLevel,
    };
    use bamboo_config::{RemoteActorPlacement, SchedulablePlacement};

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
