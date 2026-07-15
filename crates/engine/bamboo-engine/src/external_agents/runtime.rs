use std::sync::Arc;

use crate::runtime::execution::{ExternalChildRunner, SpawnJob};
use async_trait::async_trait;
use bamboo_a2a::A2AJsonRpcClient;
use bamboo_agent_core::{AgentError, AgentEvent};
use bamboo_llm::Config;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::a2a_adapter::A2AExternalChildRunner;
use super::actor_adapter::ActorChildRunner;
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
    let agents = parse_external_agents(config);

    let mut runners: Vec<Arc<dyn ExternalChildRunner>> = Vec::new();

    // The built-in local actor worker is the default runtime for every
    // sub-agent. Always build it; a build failure here is logged and leaves the
    // composite without a default handler (dispatch then errors clearly).
    match build_local_actor_runner(config) {
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
            runners.push(Arc::new(ActorChildRunner::new(
                profile.agent_id.clone(),
                std::path::PathBuf::from(worker_bin),
                profile.worker_args.clone(),
                fabric_dir,
                executor,
                extract_provider_credentials(config),
                config.provider.clone(),
                config
                    .subagents
                    .max_concurrent
                    .unwrap_or(super::actor_adapter::DEFAULT_MAX_CONCURRENT_ACTORS),
            )));
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
fn build_local_actor_runner(config: &Config) -> Result<Arc<dyn ExternalChildRunner>, String> {
    let sub = &config.subagents;

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

    Ok(Arc::new(
        ActorChildRunner::new(
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
        })),
    ))
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
/// provisioning.
///
/// `api_key` on every typed provider-config struct (both the legacy
/// single-instance `config.providers.*` slots and `config.provider_instances`
/// entries) is plaintext-in-memory but `#[serde(skip_serializing)]` — on disk
/// it lives as `api_key_encrypted` and gets hydrated into `api_key` on load.
/// A `serde_json::to_value` projection therefore never observes it (#495);
/// every typed slot below is read via direct field access instead, the way
/// `provider_instances` already was.
///
/// Precedence when a legacy slot and a `provider_instances` entry share the
/// same routing key (e.g. a hand-authored instance id of `"anthropic"`):
/// the explicit `provider_instances` entry wins and the legacy slot is
/// dropped, not merged. This mirrors the only other place the codebase
/// reconciles the two config shapes —
/// [`bamboo_config::synthesize_legacy_instances`], reused here — and the
/// provider registry's "explicit instance takes precedence" rule
/// (`bamboo-llm/src/provider_registry.rs`).
pub fn extract_provider_credentials(
    config: &Config,
) -> Vec<bamboo_subagent::provision::ScopedCredential> {
    let mut out = Vec::new();

    // Multi-instance providers: provider_instances keyed by instance id; the
    // child routes by instance id, the worker constructs by provider_type.
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

    // Legacy single-instance slots: providers.anthropic / openai / gemini /
    // bodhi (copilot has no api_key — device auth, so it never contributes).
    // `synthesize_legacy_instances` already turns each populated slot into a
    // `ProviderInstanceConfig` keyed by provider-type name via typed field
    // access, and already skips any id present in `provider_instances`; the
    // `contains_key` check here is the same defensive belt-and-suspenders
    // the provider registry does at its equivalent call site.
    let legacy = bamboo_config::synthesize_legacy_instances(config);
    out.extend(legacy.into_iter().filter_map(|(id, inst)| {
        if config.provider_instances.contains_key(&id) {
            return None;
        }
        let api_key = inst.api_key.trim().to_string();
        if api_key.is_empty() {
            return None;
        }
        Some(bamboo_subagent::provision::ScopedCredential {
            provider: id,
            api_key,
            base_url: inst.base_url,
            provider_type: Some(inst.provider_type),
        })
    }));

    // Forward-compat: provider keys unknown to this binary are preserved
    // verbatim as raw JSON in `ProviderConfigs::extra` (flattened). These
    // were never subject to the `skip_serializing` problem above — they're
    // plain `serde_json::Value`, not a typed struct — so a direct scan for
    // `api_key` is safe and keeps this path working for the same shapes the
    // old generic projection used to (accidentally) cover.
    out.extend(config.providers.extra.iter().filter_map(|(name, slot)| {
        if config.provider_instances.contains_key(name) {
            return None;
        }
        let api_key = slot.get("api_key")?.as_str()?.trim().to_string();
        if api_key.is_empty() {
            return None;
        }
        Some(bamboo_subagent::provision::ScopedCredential {
            provider: name.clone(),
            api_key,
            base_url: slot
                .get("base_url")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            provider_type: Some(name.clone()),
        })
    }));

    out
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

#[cfg(test)]
mod extract_provider_credentials_tests {
    use super::extract_provider_credentials;
    use bamboo_config::{
        AnthropicConfig, Config, OpenAIConfig, ProviderConfigs, ProviderInstanceConfig,
    };

    /// `Config::default()` is in-memory only (#38, see `impl Default for
    /// Config`) — no filesystem/env bleed — so we can build straight on top
    /// of it, but pin `providers` / `provider_instances` explicitly anyway
    /// so these tests can't be affected by future default-config changes.
    fn base_config() -> Config {
        Config {
            providers: ProviderConfigs::default(),
            provider_instances: std::collections::HashMap::new(),
            ..Config::default()
        }
    }

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
            responses_only_models: vec![],
            request_overrides: None,
            enabled: true,
            extra: Default::default(),
        }
    }

    /// #495: `providers.anthropic.api_key` is `#[serde(skip_serializing)]`
    /// (plaintext, in-memory only — on disk it's `api_key_encrypted`,
    /// hydrated on load). The old `serde_json::to_value(&config.providers)`
    /// projection never observed it, so a worker deployed from a
    /// legacy-only config got zero credentials and failed at first LLM
    /// call with no deploy-time error.
    #[test]
    fn legacy_only_config_yields_credential() {
        let mut config = base_config();
        config.providers.anthropic = Some(AnthropicConfig {
            api_key: "sk-ant-legacy".into(),
            base_url: Some("https://api.anthropic.com".into()),
            ..Default::default()
        });

        let creds = extract_provider_credentials(&config);
        assert_eq!(creds.len(), 1, "expected exactly one credential: {creds:?}");
        assert_eq!(creds[0].provider, "anthropic");
        assert_eq!(creds[0].api_key, "sk-ant-legacy");
        assert_eq!(creds[0].provider_type.as_deref(), Some("anthropic"));
        assert_eq!(
            creds[0].base_url.as_deref(),
            Some("https://api.anthropic.com")
        );
    }

    #[test]
    fn legacy_slot_with_empty_api_key_yields_nothing() {
        let mut config = base_config();
        config.providers.anthropic = Some(AnthropicConfig::default());
        assert!(extract_provider_credentials(&config).is_empty());
    }

    #[test]
    fn no_providers_configured_yields_nothing() {
        let config = base_config();
        assert!(extract_provider_credentials(&config).is_empty());
    }

    /// `provider_instances`-only configs already worked before #495 (typed
    /// field access); this pins that behavior stays unchanged.
    #[test]
    fn instances_only_config_unchanged() {
        let mut config = base_config();
        config.provider_instances.insert(
            "anthropic-work".to_string(),
            instance("anthropic", "sk-instance"),
        );

        let creds = extract_provider_credentials(&config);
        assert_eq!(creds.len(), 1, "expected exactly one credential: {creds:?}");
        assert_eq!(creds[0].provider, "anthropic-work");
        assert_eq!(creds[0].api_key, "sk-instance");
        assert_eq!(creds[0].provider_type.as_deref(), Some("anthropic"));
    }

    /// Both configured, distinct routing keys: neither shape clobbers the
    /// other; both credentials are present.
    #[test]
    fn legacy_and_instances_with_different_keys_both_present() {
        let mut config = base_config();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "sk-oa-legacy".into(),
            ..Default::default()
        });
        config.provider_instances.insert(
            "anthropic-work".to_string(),
            instance("anthropic", "sk-instance"),
        );

        let creds = extract_provider_credentials(&config);
        let mut providers: Vec<&str> = creds.iter().map(|c| c.provider.as_str()).collect();
        providers.sort_unstable();
        assert_eq!(providers, vec!["anthropic-work", "openai"]);
    }

    /// Documented precedence (#495): when a `provider_instances` entry and a
    /// legacy `providers.*` slot share the same routing key, the explicit
    /// instance wins outright — the legacy credential is dropped, not
    /// merged and not duplicated. This mirrors
    /// `bamboo_config::synthesize_legacy_instances` (which already skips
    /// synthesizing a legacy id present in `provider_instances`) and the
    /// provider registry's "explicit instance takes precedence" comment in
    /// `bamboo-llm/src/provider_registry.rs`.
    #[test]
    fn instance_takes_precedence_over_legacy_slot_with_same_key() {
        let mut config = base_config();
        config.providers.anthropic = Some(AnthropicConfig {
            api_key: "sk-ant-legacy".into(),
            ..Default::default()
        });
        config.provider_instances.insert(
            "anthropic".to_string(),
            instance("anthropic", "sk-ant-instance"),
        );

        let creds = extract_provider_credentials(&config);
        assert_eq!(
            creds.len(),
            1,
            "instance and legacy slot must not both appear: {creds:?}"
        );
        assert_eq!(creds[0].provider, "anthropic");
        assert_eq!(creds[0].api_key, "sk-ant-instance");
    }

    /// Forward-compat: provider keys unknown to this binary are preserved
    /// verbatim as raw JSON (`ProviderConfigs::extra`, flattened). These
    /// were never subject to the `skip_serializing` bug — plain
    /// `serde_json::Value`, not a typed struct — so the fix keeps scanning
    /// them the way the old generic projection (accidentally) did.
    #[test]
    fn forward_compat_unknown_provider_extra_key_still_extracted() {
        let mut config = base_config();
        config.providers.extra.insert(
            "mistral".to_string(),
            serde_json::json!({ "api_key": "sk-mistral", "base_url": "https://api.mistral.ai" }),
        );

        let creds = extract_provider_credentials(&config);
        assert_eq!(creds.len(), 1, "expected exactly one credential: {creds:?}");
        assert_eq!(creds[0].provider, "mistral");
        assert_eq!(creds[0].api_key, "sk-mistral");
        assert_eq!(creds[0].base_url.as_deref(), Some("https://api.mistral.ai"));
    }

    /// Copilot has no `api_key` field at all (device auth) — it must never
    /// contribute a credential.
    #[test]
    fn copilot_without_api_key_yields_nothing() {
        let mut config = base_config();
        config.providers.copilot = Some(bamboo_config::CopilotConfig {
            enabled: true,
            ..Default::default()
        });
        assert!(extract_provider_credentials(&config).is_empty());
    }
}
