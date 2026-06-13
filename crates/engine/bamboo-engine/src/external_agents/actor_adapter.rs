//! Actor external child runner.
//!
//! Runs a child session as an independent **actor**: a separate OS process with its own
//! isolated context, speaking the `bamboo-subagent` WebSocket protocol. This is the
//! engine-side adapter on the `wants_external` seam: it spawns the worker binary, waits for
//! it to self-register into the Tier-1 file fabric, connects, sends the assignment, and
//! forwards the child's `AgentEvent`s back onto the parent's `event_tx`.
//!
//! The built-in **local actor** instance of this runner is the default runtime for
//! every sub-agent (the in-process runtime was removed). The expert `externalAgents`
//! tables can additionally route specific roles to other actor/a2a agents.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bamboo_agent_core::{AgentError, AgentEvent, Role, Session};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use bamboo_subagent::discovery::Fabric;
use bamboo_subagent::fleet::{spawn_worker, SpawnedChild};
use bamboo_subagent::proto::{ChildFrame, ParentFrame, RunSpec, TerminalStatus};
use bamboo_subagent::provision::{
    ChildIdentity, ExecutorSpec, ModelRefSpec, ProvisionSpec, ScopedCredential,
};
use bamboo_subagent::transport::ChildClient;

use crate::runtime::execution::{ExternalChildRunner, SpawnJob};

/// Default cap on simultaneously running actor processes.
pub const DEFAULT_MAX_CONCURRENT_ACTORS: usize = 8;

/// Default cap on idle pooled (warm, reusable) workers kept per fingerprint.
const DEFAULT_MAX_IDLE_PER_KEY: usize = 4;

/// How long a pooled worker waits for its next assignment before reclaiming
/// itself (must comfortably exceed the gap between sibling spawns).
const POOLED_IDLE_TIMEOUT_SECS: u64 = 300;

/// A warm worker parked for reuse: its process handle (killed on drop), the WS
/// endpoint to reconnect to, and the id it registered under in the fabric.
struct PooledActor {
    worker: SpawnedChild,
    endpoint: String,
    agent_id: String,
}

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
    /// Backpressure: bounds the number of concurrently *running* actors; further
    /// runs wait for a slot instead of exploding the process table. (Idle pooled
    /// workers do not hold a slot.)
    concurrency: std::sync::Arc<tokio::sync::Semaphore>,
    spawn_timeout: Duration,
    /// Warm-worker pool keyed by a reuse fingerprint
    /// (role/provider/model/workspace/disabled-tools). A finished run parks its
    /// worker here so the next matching child reuses it instead of spawning a
    /// fresh process — collapsing N sibling sub-agents onto a few processes.
    pool: Arc<Mutex<HashMap<String, Vec<PooledActor>>>>,
    max_idle_per_key: usize,
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
            pool: Arc::new(Mutex::new(HashMap::new())),
            max_idle_per_key: DEFAULT_MAX_IDLE_PER_KEY,
        }
    }

    /// Reuse fingerprint: two children are interchangeable on one warm worker iff
    /// they share role, provider, model, workspace, and disabled-tool set (those
    /// are baked into the worker at provision time; everything else — assignment,
    /// history — is shipped per-run in the `RunSpec`).
    fn fingerprint(spec: &ProvisionSpec) -> String {
        let role = spec.identity.role.as_str();
        let (provider, model) = spec
            .model
            .as_ref()
            .map(|m| (m.provider.as_str(), m.model.as_str()))
            .unwrap_or(("", ""));
        let workspace = spec.workspace.as_deref().unwrap_or("");
        let mut tools = spec.disabled_tools.clone().unwrap_or_default();
        tools.sort();
        format!(
            "{role}\u{1}{provider}\u{1}{model}\u{1}{workspace}\u{1}{}",
            tools.join(",")
        )
    }

    /// Check out a worker for this assignment: reuse a live pooled one matching
    /// `key`, else spawn a fresh reusable worker.
    async fn acquire_worker(
        &self,
        key: &str,
        spec: &ProvisionSpec,
    ) -> crate::runtime::runner::Result<PooledActor> {
        // Drain the pool bucket, validating liveness; a worker that hit its idle
        // timeout has exited and withdrawn its fabric record — skip and reap it.
        loop {
            let candidate = {
                let mut pool = self.pool.lock().await;
                pool.get_mut(key).and_then(|bucket| bucket.pop())
            };
            let Some(candidate) = candidate else { break };
            let alive = Fabric::at(&self.fabric_dir)
                .resolve(&candidate.agent_id)
                .await
                .ok()
                .flatten()
                .is_some();
            if alive {
                return Ok(candidate);
            }
            candidate.worker.kill().await;
        }

        let spawned = spawn_worker(
            &self.worker_bin,
            &self.worker_args,
            spec,
            self.spawn_timeout,
        )
        .await
        .map_err(|e| AgentError::LLM(format!("actor spawn/register failed: {e}")))?;
        let endpoint = spawned.record.endpoint.clone();
        let agent_id = spawned.record.agent_id.clone();
        Ok(PooledActor {
            worker: spawned,
            endpoint,
            agent_id,
        })
    }

    /// Park a worker for reuse after a clean run; if the bucket is full, retire it.
    async fn release_worker(&self, key: &str, actor: PooledActor) {
        let mut pool = self.pool.lock().await;
        let bucket = pool.entry(key.to_string()).or_default();
        if bucket.len() >= self.max_idle_per_key {
            drop(pool);
            self.retire_worker(actor).await;
            return;
        }
        bucket.push(actor);
    }

    /// Forcefully stop a worker and clean its discovery record.
    async fn retire_worker(&self, actor: PooledActor) {
        let agent_id = actor.agent_id.clone();
        actor.worker.kill().await;
        let _ = Fabric::at(&self.fabric_dir).withdraw(&agent_id).await;
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
        let mut spec = self.build_spec(session, job);
        // Make every actor a warm, reusable worker so the pool can recycle it for
        // the next sibling with a matching fingerprint.
        spec.reusable = true;
        if spec.limits.idle_timeout_secs.is_none() {
            spec.limits.idle_timeout_secs = Some(POOLED_IDLE_TIMEOUT_SECS);
        }
        let pool_key = Self::fingerprint(&spec);
        // Rehydration: the child session in the parent's store is the actor's
        // durable state. Ship the full conversation so a reactivation
        // (send_message / update / rerun) carries its history. A reused worker is
        // stateless between runs, so this is also what isolates each child's
        // context on a shared process.
        let messages: Vec<serde_json::Value> = session
            .messages
            .iter()
            .filter_map(|m| serde_json::to_value(m).ok())
            .collect();

        // Backpressure: hold a concurrency slot for the lifetime of the *run*
        // (cancellation still proceeds — the cancel branch in drive() runs while
        // we hold the permit). Released when this fn returns, i.e. once the worker
        // is parked back into the pool, so idle workers don't pin slots.
        let _slot = self
            .concurrency
            .acquire()
            .await
            .map_err(|_| AgentError::LLM("actor concurrency limiter closed".to_string()))?;

        // Check out a warm worker (reuse-or-spawn).
        let mut actor = self.acquire_worker(&pool_key, &spec).await?;

        let mut client = match ChildClient::connect(&actor.endpoint).await {
            Ok(client) => client,
            Err(e) => {
                // The pooled worker may have died between checkout and connect;
                // retire it and spawn one fresh, once.
                self.retire_worker(actor).await;
                let spawned = spawn_worker(
                    &self.worker_bin,
                    &self.worker_args,
                    &spec,
                    self.spawn_timeout,
                )
                .await
                .map_err(|e2| {
                    AgentError::LLM(format!("actor respawn after reuse miss ({e}): {e2}"))
                })?;
                let endpoint = spawned.record.endpoint.clone();
                let agent_id = spawned.record.agent_id.clone();
                let client = ChildClient::connect(&endpoint)
                    .await
                    .map_err(|e2| AgentError::LLM(format!("actor connect failed: {e2}")))?;
                actor = PooledActor {
                    worker: spawned,
                    endpoint,
                    agent_id,
                };
                client
            }
        };

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
        let live_guard = super::live::register(&job.child_session_id, live_tx);

        let result = drive(&mut client, &event_tx, &cancel_token, &mut live_rx).await;
        // Unregister IMMEDIATELY: after drive returns nobody consumes live_rx,
        // so a send_message landing in the close/park window below must see
        // "not live" and take the durable-queue fallback instead of vanishing.
        // (Even if one slipped in earlier, send_message also appends it to the
        // durable transcript, so the next activation still rehydrates it.)
        drop(live_guard);

        // Close the connection: the worker's serve loop then accepts the next
        // assignment (reuse) or idles out. Park the worker on a clean run; retire
        // it on error/cancel (a wedged worker must not be reused).
        let _ = client.close().await;
        match &result {
            Ok(_) => self.release_worker(&pool_key, actor).await,
            Err(_) => self.retire_worker(actor).await,
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_with(
        role: &str,
        provider: &str,
        model: &str,
        workspace: Option<&str>,
        disabled: Option<Vec<&str>>,
    ) -> ProvisionSpec {
        let mut spec = ProvisionSpec::new(
            ChildIdentity {
                child_id: "c".into(),
                parent_id: None,
                project_key: None,
                role: role.into(),
            },
            ExecutorSpec::Echo,
            "/tmp/fab".into(),
        );
        spec.workspace = workspace.map(|w| w.to_string());
        spec.model = Some(ModelRefSpec {
            provider: provider.into(),
            model: model.into(),
        });
        spec.disabled_tools = disabled.map(|d| d.into_iter().map(String::from).collect());
        spec
    }

    #[test]
    fn fingerprint_matches_interchangeable_children() {
        // Same role/provider/model/workspace and equal tool sets (order-insensitive)
        // are interchangeable on one warm worker — and differ only in child_id.
        let a = spec_with(
            "explorer",
            "p",
            "m",
            Some("/ws"),
            Some(vec!["Bash", "Edit"]),
        );
        let mut b = spec_with(
            "explorer",
            "p",
            "m",
            Some("/ws"),
            Some(vec!["Edit", "Bash"]),
        );
        b.identity.child_id = "other".into();
        assert_eq!(
            ActorChildRunner::fingerprint(&a),
            ActorChildRunner::fingerprint(&b)
        );
    }

    #[test]
    fn fingerprint_separates_distinct_runtimes() {
        let base = spec_with("explorer", "p", "m", Some("/ws"), None);
        let base_fp = ActorChildRunner::fingerprint(&base);
        // Each axis that is baked into the worker must split the pool bucket.
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&spec_with("writer", "p", "m", Some("/ws"), None))
        );
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&spec_with("explorer", "p2", "m", Some("/ws"), None))
        );
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&spec_with("explorer", "p", "m2", Some("/ws"), None))
        );
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&spec_with("explorer", "p", "m", Some("/ws2"), None))
        );
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&spec_with(
                "explorer",
                "p",
                "m",
                Some("/ws"),
                Some(vec!["Bash"])
            ))
        );
    }
}
