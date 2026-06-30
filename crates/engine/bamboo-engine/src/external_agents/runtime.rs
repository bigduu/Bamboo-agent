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
            let fabric_dir = profile
                .fabric_dir
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::env::temp_dir().join("bamboo-subagents"));
            let executor = match profile.executor.as_deref() {
                Some("echo") => bamboo_subagent::provision::ExecutorSpec::Echo,
                Some("bamboo_runtime") | None => {
                    bamboo_subagent::provision::ExecutorSpec::BambooRuntime
                }
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

    let fabric_dir = sub
        .fabric_dir
        .clone()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("bamboo-subagents"));

    let executor = match sub.executor.as_deref() {
        Some("echo") => bamboo_subagent::provision::ExecutorSpec::Echo,
        Some("bamboo_runtime") | None => bamboo_subagent::provision::ExecutorSpec::BambooRuntime,
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
        .with_remote_placements(resolve_remote_placements(&sub.remote_placements))
        .with_schedulable_placements(resolve_schedulable_placements(&sub.schedulable_placements))
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
) -> std::collections::HashMap<String, super::actor_adapter::ResolvedSchedulablePlacement> {
    let mut out = std::collections::HashMap::new();
    for p in placements {
        let token = match p.token_env.as_deref() {
            Some(env_var) => match std::env::var(env_var) {
                Ok(token) => Some(token),
                Err(_) => {
                    tracing::error!(
                        "schedulable placement for role '{}' token_env '{}' is not set; \
                         skipping (role falls back to local, NOT an unauthenticated registry query)",
                        p.role,
                        env_var
                    );
                    continue;
                }
            },
            None => {
                // A tokenless schedulable placement only makes sense on a trusted
                // link. Warn if it targets what looks like a public registry so an
                // operator footgun is visible in logs.
                if registry_url_looks_public(&p.registry_url) {
                    tracing::warn!(
                        "schedulable placement for role '{}' has no token_env but targets a \
                         public-looking registry '{}'; the registry query AND the worker connect \
                         will carry NO bearer. Set token_env for any non-loopback control plane.",
                        p.role,
                        p.registry_url
                    );
                }
                None
            }
        };
        out.insert(
            p.role.clone(),
            super::actor_adapter::ResolvedSchedulablePlacement {
                pool: p.pool.clone(),
                registry_url: p.registry_url.clone(),
                token,
                ca_cert_file: p.ca_cert_file.as_ref().map(std::path::PathBuf::from),
            },
        );
    }
    out
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

/// Like `endpoint_looks_public` but for a registry URL (`http://` / `https://`).
/// `https://` is always public-grade; for `http://` we flag any non-loopback
/// host. Used to surface a tokenless-schedulable-placement footgun in logs.
fn registry_url_looks_public(url: &str) -> bool {
    if url.starts_with("https://") {
        return true;
    }
    let host = url
        .strip_prefix("http://")
        .unwrap_or(url)
        .split(['/', ':'])
        .next()
        .unwrap_or("");
    !(host == "localhost" || host == "127.0.0.1" || host == "::1" || host.is_empty())
}

fn resolve_remote_placements(
    placements: &[bamboo_config::RemoteActorPlacement],
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
            },
        );
    }
    out
}

/// Snapshot per-provider credentials from the parent config for actor
/// provisioning. Serialized generically so this code does not chase the
/// per-provider config struct shapes — any slot with a non-empty `api_key`
/// yields a scoped credential (plus `base_url` when present).
pub fn extract_provider_credentials(
    config: &Config,
) -> Vec<bamboo_subagent::provision::ScopedCredential> {
    let mut out = Vec::new();

    // Legacy single-instance slots: providers.anthropic / openai / …
    if let Ok(serde_json::Value::Object(providers)) = serde_json::to_value(&config.providers) {
        out.extend(providers.into_iter().filter_map(|(name, slot)| {
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
                provider_type: Some(name),
            })
        }));
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
