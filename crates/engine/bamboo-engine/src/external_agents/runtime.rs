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
use super::subprocess_adapter::SubprocessChildRunner;

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
/// Returns `None` if no A2A profiles are configured.
/// Builds a composite router over all configured A2A profiles so that
/// `external.agent_id` metadata selects the correct runner.
pub fn build_external_child_runner(config: &Config) -> Option<Arc<dyn ExternalChildRunner>> {
    let agents = parse_external_agents(config);
    if agents.is_empty() {
        return None;
    }

    let mut runners: Vec<Arc<dyn ExternalChildRunner>> = Vec::new();

    for (_agent_id, profile) in agents {
        // Subprocess protocol: spawn a local worker binary over the bamboo-subagent WS protocol.
        if matches!(profile.protocol, ExternalAgentProtocol::Subprocess) {
            let Some(worker_bin) = profile.worker_bin.as_ref() else {
                tracing::error!(
                    "Subprocess agent profile {} has no worker_bin; skipping",
                    profile.agent_id
                );
                continue;
            };
            let fabric_dir = profile
                .fabric_dir
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::env::temp_dir().join("bamboo-subagents"));
            runners.push(Arc::new(SubprocessChildRunner::new(
                profile.agent_id.clone(),
                std::path::PathBuf::from(worker_bin),
                fabric_dir,
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
