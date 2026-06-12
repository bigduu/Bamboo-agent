use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::{AgentError, AgentEvent};
use crate::runtime::execution::{ExternalChildRunner, SpawnJob};
use bamboo_a2a::A2AJsonRpcClient;
use bamboo_llm::Config;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::a2a_adapter::A2AExternalChildRunner;
use super::config::{parse_external_agents, ExternalAgentProtocol};
use super::actor_adapter::ActorChildRunner;

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
}

/// Build an external child runner from the application config.
///
/// Returns `None` when nothing routes externally. Builds a composite router
/// over all configured runners so `external.agent_id` metadata selects the
/// right one. The friendly `subagents.runtime = "actor"` switch
/// synthesizes a local actor runner automatically — worker binary,
/// arguments, and discovery dir are all derived; no expert tables needed.
pub fn build_external_child_runner(config: &Config) -> Option<Arc<dyn ExternalChildRunner>> {
    let agents = parse_external_agents(config);

    let mut runners: Vec<Arc<dyn ExternalChildRunner>> = Vec::new();

    // Friendly path: one switch in typed config -> built-in local worker.
    if config.subagents.any_actor() {
        match build_local_actor_runner(config) {
            Ok(runner) => runners.push(runner),
            Err(e) => tracing::error!("subagents.runtime=actor unavailable: {e}"),
        }
    }

    if agents.is_empty() && runners.is_empty() {
        return None;
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

    if runners.is_empty() {
        None
    } else {
        Some(Arc::new(CompositeExternalChildRunner::new(runners)))
    }
}

/// Build the built-in local actor runner from the typed `subagents`
/// config. Everything is derived: worker = the current bamboo executable +
/// `subagent-worker`, fabric = per-user temp dir — unless expert fields
/// override them.
fn build_local_actor_runner(
    config: &Config,
) -> Result<Arc<dyn ExternalChildRunner>, String> {
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

    Ok(Arc::new(ActorChildRunner::new(
        super::config::LOCAL_ACTOR_AGENT_ID.to_string(),
        worker_bin,
        worker_args,
        fabric_dir,
        executor,
        extract_provider_credentials(config),
        config.provider.clone(),
    )))
}

/// Snapshot per-provider credentials from the parent config for actor
/// provisioning. Serialized generically so this code does not chase the
/// per-provider config struct shapes — any slot with a non-empty `api_key`
/// yields a scoped credential (plus `base_url` when present).
fn extract_provider_credentials(
    config: &Config,
) -> Vec<bamboo_subagent::provision::ScopedCredential> {
    let Ok(serde_json::Value::Object(providers)) = serde_json::to_value(&config.providers) else {
        return Vec::new();
    };
    providers
        .into_iter()
        .filter_map(|(name, slot)| {
            let api_key = slot.get("api_key")?.as_str()?.trim().to_string();
            if api_key.is_empty() {
                return None;
            }
            Some(bamboo_subagent::provision::ScopedCredential {
                provider: name,
                api_key,
                base_url: slot
                    .get("base_url")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
            })
        })
        .collect()
}
