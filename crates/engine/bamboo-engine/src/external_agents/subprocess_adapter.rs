//! Subprocess external child runner.
//!
//! Runs a child session as a **separate OS process** that speaks the `bamboo-subagent` WebSocket
//! protocol. This is the engine-side adapter on the `wants_external` seam: it spawns the worker
//! binary, waits for it to self-register into the Tier-1 file fabric, connects, sends the
//! assignment, and forwards the child's `AgentEvent`s back onto the parent's `event_tx`.
//!
//! Gated entirely behind config (`externalAgents` + `subagentRouting`); when no `subprocess`
//! profile is configured, this runner is never built and default behavior is unchanged.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use bamboo_agent_core::{AgentError, AgentEvent, Role, Session};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_subagent::fleet::spawn_worker;
use bamboo_subagent::proto::{ChildFrame, ParentFrame, RunSpec, TerminalStatus};
use bamboo_subagent::provision::{ChildIdentity, ExecutorSpec, ModelRefSpec, ProvisionSpec};
use bamboo_subagent::transport::ChildClient;

use crate::runtime::execution::{ExternalChildRunner, SpawnJob};

/// Spawns and drives a child session as a `bamboo-subagent` worker subprocess.
pub struct SubprocessChildRunner {
    agent_id: String,
    worker_bin: PathBuf,
    worker_args: Vec<String>,
    fabric_dir: PathBuf,
    executor: ExecutorSpec,
    spawn_timeout: Duration,
}

impl SubprocessChildRunner {
    pub fn new(
        agent_id: String,
        worker_bin: PathBuf,
        worker_args: Vec<String>,
        fabric_dir: PathBuf,
        executor: ExecutorSpec,
    ) -> Self {
        Self {
            agent_id,
            worker_bin,
            worker_args,
            fabric_dir,
            executor,
            spawn_timeout: Duration::from_secs(30),
        }
    }

    /// Assemble the parent-resolved provisioning document for this child.
    fn build_spec(&self, session: &Session, job: &SpawnJob) -> ProvisionSpec {
        let mut spec = ProvisionSpec::new(
            ChildIdentity {
                child_id: job.child_session_id.clone(),
                parent_id: Some(job.parent_session_id.clone()),
                project_key: None,
                role: session
                    .metadata
                    .get("subagent_type")
                    .cloned()
                    .unwrap_or_else(|| "worker".to_string()),
            },
            self.executor.clone(),
            self.fabric_dir.to_string_lossy().into_owned(),
        );
        spec.workspace = session.workspace.clone();
        // Final model: the session's pinned model_ref (create.model / routing already applied).
        spec.model = session
            .model_ref
            .as_ref()
            .map(|r| ModelRefSpec {
                provider: r.provider.clone(),
                model: r.model.clone(),
            })
            .or_else(|| {
                let m = job.model.trim();
                (!m.is_empty()).then(|| ModelRefSpec {
                    provider: String::new(), // worker resolves via its own default provider
                    model: m.to_string(),
                })
            });
        spec.disabled_tools = job.disabled_tools.clone();
        spec
    }
}

#[async_trait]
impl ExternalChildRunner for SubprocessChildRunner {
    async fn should_handle(&self, session: &Session) -> bool {
        session.metadata.get("runtime.kind") == Some(&"external".to_string())
            && session.metadata.get("external.protocol") == Some(&"subprocess".to_string())
            && session.metadata.get("external.agent_id") == Some(&self.agent_id)
    }

    async fn execute_external_child(
        &self,
        session: &mut Session,
        job: &SpawnJob,
        event_tx: mpsc::Sender<AgentEvent>,
        cancel_token: CancellationToken,
    ) -> crate::runtime::runner::Result<()> {
        let assignment = extract_assignment(session);
        let spec = self.build_spec(session, job);

        let spawned = spawn_worker(&self.worker_bin, &self.worker_args, &spec, self.spawn_timeout)
            .await
            .map_err(|e| AgentError::LLM(format!("subprocess spawn/register failed: {e}")))?;

        let mut client = ChildClient::connect(&spawned.record.endpoint)
            .await
            .map_err(|e| AgentError::LLM(format!("subprocess connect failed: {e}")))?;
        client
            .send(ParentFrame::Run(RunSpec {
                assignment,
                reasoning_effort: None,
            }))
            .await
            .map_err(|e| AgentError::LLM(format!("subprocess run dispatch failed: {e}")))?;

        let result = drive(&mut client, &event_tx, &cancel_token).await;

        let _ = client.close().await;
        spawned.kill().await;
        result
    }
}

/// Pump child frames -> parent events until a terminal frame (or cancellation).
async fn drive(
    client: &mut ChildClient,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
) -> crate::runtime::runner::Result<()> {
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                // fall through to the cancel handling below
                break;
            }
            frame = client.next_frame() => {
                match frame {
                    Ok(Some(ChildFrame::Event { event })) => {
                        // AgentEvent is serialized verbatim on the wire (zero mapping).
                        if let Ok(ev) = serde_json::from_value::<AgentEvent>(event) {
                            let _ = event_tx.send(ev).await;
                        }
                    }
                    Ok(Some(ChildFrame::Terminal { status, error, .. })) => {
                        return match status {
                            TerminalStatus::Completed => Ok(()),
                            TerminalStatus::Cancelled => Err(AgentError::Cancelled),
                            TerminalStatus::Error => Err(AgentError::LLM(
                                error.unwrap_or_else(|| "subprocess child errored".to_string()),
                            )),
                        };
                    }
                    Ok(None) => {
                        return Err(AgentError::LLM(
                            "subprocess child closed before terminal".to_string(),
                        ));
                    }
                    Err(e) => {
                        return Err(AgentError::LLM(format!("subprocess transport error: {e}")));
                    }
                }
            }
        }
    }

    // Only reached on cancellation: ask the child to stop (best-effort), then report cancelled.
    let _ = client.send(ParentFrame::Cancel).await;
    Err(AgentError::Cancelled)
}

/// The assignment text = the child session's latest user message (falls back to its title).
fn extract_assignment(session: &Session) -> String {
    session
        .messages
        .iter()
        .rev()
        .find(|m| matches!(m.role, Role::User))
        .map(|m| m.content.clone())
        .unwrap_or_else(|| {
            session
                .metadata
                .get("title")
                .cloned()
                .unwrap_or_else(|| "Execute task".to_string())
        })
}
