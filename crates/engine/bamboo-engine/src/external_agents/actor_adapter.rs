//! Actor external child runner.
//!
//! Runs a child session as an independent **actor**: a separate OS process with its own
//! isolated context, speaking the `bamboo-subagent` WebSocket protocol. This is the
//! engine-side adapter on the `wants_external` seam: it spawns the worker binary, waits for
//! it to self-register into the Tier-1 file fabric, connects, sends the assignment, and
//! forwards the child's `AgentEvent`s back onto the parent's `event_tx`.
//!
//! Gated entirely behind config (`subagents.runtime = "actor"` or the expert
//! `externalAgents` tables); when nothing routes to an actor, this runner is never built
//! and default behavior is unchanged.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use bamboo_agent_core::{AgentError, AgentEvent, Role, Session};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_subagent::fleet::spawn_worker;
use bamboo_subagent::proto::{ChildFrame, ParentFrame, RunSpec, TerminalStatus};
use bamboo_subagent::provision::{
    ChildIdentity, ExecutorSpec, ModelRefSpec, ProvisionSpec, ScopedCredential,
};
use bamboo_subagent::transport::ChildClient;

use crate::runtime::execution::{ExternalChildRunner, SpawnJob};

/// Default cap on simultaneously running actor processes.
pub const DEFAULT_MAX_CONCURRENT_ACTORS: usize = 8;

/// Spawns and drives a child session as an independent actor: a `bamboo-subagent` worker process.
pub struct ActorChildRunner {
    agent_id: String,
    worker_bin: PathBuf,
    worker_args: Vec<String>,
    fabric_dir: PathBuf,
    executor: ExecutorSpec,
    /// Per-provider credentials snapshotted from the parent config at build
    /// time; the spec carries only the ONE the child's provider needs.
    credentials: Vec<ScopedCredential>,
    /// Parent's default provider (used when the child has no explicit one).
    default_provider: String,
    /// Backpressure: bounds the number of live actor processes; further
    /// spawns wait for a slot instead of exploding the process table.
    concurrency: std::sync::Arc<tokio::sync::Semaphore>,
    spawn_timeout: Duration,
}

impl ActorChildRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        agent_id: String,
        worker_bin: PathBuf,
        worker_args: Vec<String>,
        fabric_dir: PathBuf,
        executor: ExecutorSpec,
        credentials: Vec<ScopedCredential>,
        default_provider: String,
        max_concurrent: usize,
    ) -> Self {
        Self {
            agent_id,
            worker_bin,
            worker_args,
            fabric_dir,
            executor,
            credentials,
            default_provider,
            concurrency: std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent.max(1))),
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
        // Final model: the session's pinned model_ref (create.model / routing already applied),
        // falling back to the job's bare model on the parent's default provider.
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
                    provider: self.default_provider.clone(),
                    model: m.to_string(),
                })
            });
        spec.disabled_tools = job.disabled_tools.clone();
        // Least-privilege secrets: only the credential for the child's provider.
        let provider = spec
            .model
            .as_ref()
            .map(|m| m.provider.as_str())
            .filter(|p| !p.trim().is_empty())
            .unwrap_or(&self.default_provider);
        if let Some(cred) = self.credentials.iter().find(|c| c.provider == provider) {
            spec.secrets.provider_credentials.push(cred.clone());
        } else {
            tracing::warn!(
                "actor child {}: no credential found for provider '{}'",
                job.child_session_id,
                provider
            );
        }
        spec
    }
}

#[async_trait]
impl ExternalChildRunner for ActorChildRunner {
    async fn should_handle(&self, session: &Session) -> bool {
        session.metadata.get("runtime.kind") == Some(&"external".to_string())
            && session.metadata.get("external.protocol") == Some(&"actor".to_string())
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
        // Rehydration: the child session in the parent's store is the actor's
        // durable state. Ship the full conversation so a reactivation
        // (send_message / update / rerun) carries its history.
        let messages: Vec<serde_json::Value> = session
            .messages
            .iter()
            .filter_map(|m| serde_json::to_value(m).ok())
            .collect();

        // Backpressure: hold a concurrency slot for the lifetime of the actor
        // process (cancellation still proceeds — the cancel branch in drive()
        // runs while we hold the permit).
        let _slot = self
            .concurrency
            .acquire()
            .await
            .map_err(|_| AgentError::LLM("actor concurrency limiter closed".to_string()))?;

        let spawned = spawn_worker(&self.worker_bin, &self.worker_args, &spec, self.spawn_timeout)
            .await
            .map_err(|e| AgentError::LLM(format!("actor spawn/register failed: {e}")))?;

        let mut client = ChildClient::connect(&spawned.record.endpoint)
            .await
            .map_err(|e| AgentError::LLM(format!("actor connect failed: {e}")))?;
        client
            .send(ParentFrame::Run(RunSpec {
                assignment,
                reasoning_effort: None,
                messages,
            }))
            .await
            .map_err(|e| AgentError::LLM(format!("actor run dispatch failed: {e}")))?;

        // Register as a live actor so send_message (running, no interrupt) can
        // steer this child in-band over the existing WS connection. The guard
        // unregisters on every exit path.
        let (live_tx, mut live_rx) = mpsc::unbounded_channel::<ParentFrame>();
        let _live_guard = super::live::register(&job.child_session_id, live_tx);

        let result = drive(&mut client, &event_tx, &cancel_token, &mut live_rx).await;

        let _ = client.close().await;
        spawned.kill().await;

        // Write-back: persist the actor's final reply onto the child session so
        // the transcript survives and the NEXT activation sees it as history.
        // (run_child_spawn saves the session right after we return.)
        match result {
            Ok(Some(text)) => {
                if !text.is_empty() {
                    session.add_message(bamboo_agent_core::Message::assistant(text, None));
                }
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Pump child frames -> parent events until a terminal frame (or cancellation).
/// On success, yields the actor's final result text (for session write-back).
/// `live_rx` carries in-band frames (steering messages) from the live registry.
async fn drive(
    client: &mut ChildClient,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
    live_rx: &mut mpsc::UnboundedReceiver<ParentFrame>,
) -> crate::runtime::runner::Result<Option<String>> {
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                // fall through to the cancel handling below
                break;
            }
            Some(frame) = live_rx.recv() => {
                // Forward in-band steering to the worker over the existing WS.
                if client.send(frame).await.is_err() {
                    tracing::warn!("live steering frame could not be sent; connection failing");
                }
            }
            frame = client.next_frame() => {
                match frame {
                    Ok(Some(ChildFrame::Event { event })) => {
                        // AgentEvent is serialized verbatim on the wire (zero mapping).
                        if let Ok(ev) = serde_json::from_value::<AgentEvent>(event) {
                            let _ = event_tx.send(ev).await;
                        }
                    }
                    Ok(Some(ChildFrame::Terminal { status, result, error })) => {
                        return match status {
                            TerminalStatus::Completed => Ok(result),
                            TerminalStatus::Cancelled => Err(AgentError::Cancelled),
                            TerminalStatus::Error => Err(AgentError::LLM(
                                error.unwrap_or_else(|| "actor child errored".to_string()),
                            )),
                        };
                    }
                    Ok(None) => {
                        return Err(AgentError::LLM(
                            "actor child closed before terminal".to_string(),
                        ));
                    }
                    Err(e) => {
                        return Err(AgentError::LLM(format!("actor transport error: {e}")));
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
