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

/// Max nesting depth for direct nested execution (Phase 6). A worker whose
/// session `spawn_depth` is below this gets its own spawn stack + the real
/// SubAgent tool; at/over it, neither (and the tool itself refuses). Mirrors
/// `bamboo_server_tools::DEFAULT_MAX_SPAWN_DEPTH` (kept in sync; engine can't
/// depend on server-tools). Root orchestrator = 0 ⇒ 4 levels of sub-agents.
pub const MAX_SPAWN_DEPTH: u32 = 4;

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
    /// Host-side decision for a child's gated-tool approval request (Phase 2).
    /// `None` ⇒ fail-closed DENY (the safe default). A wired decider (policy or
    /// human-routing bridge) returns approve/deny over the actor WS.
    approval_decider: Option<Arc<dyn ChildApprovalDecider>>,
    /// Host-side fulfilment of a child's nested SubAgent spawn request (Phase 6).
    /// `None` ⇒ graceful "not available" error (the safe default).
    nested_spawn_handler: Option<Arc<dyn NestedSpawnHandler>>,
}

/// Decides how the host answers a child worker's gated-tool approval request
/// (Phase 2: child → parent approval delegation). Async so an implementation
/// can consult a policy or defer to a human. With no decider wired the host
/// replies with a fail-closed DENY.
///
/// NOTE: `decide` is awaited inside the per-child frame pump, so an
/// implementation must resolve promptly (e.g. a policy lookup). A human-in-the-
/// loop decision that may block indefinitely should instead be delivered
/// out-of-band as a `ParentFrame::ApprovalReply` via the live steering channel
/// (`super::live`), which `drive()` already forwards to the worker without
/// stalling the pump.
#[async_trait]
pub trait ChildApprovalDecider: Send + Sync {
    /// Decide whether `child_session_id` may perform the gated action described
    /// by `request` (`{tool_name, permission, resource}`).
    async fn decide(&self, child_session_id: &str, request: &serde_json::Value) -> bool;
}

/// Resolve a child approval request to approve/deny. Fail-closed (DENY) when no
/// decider is wired — the single, testable seam for the host-side decision.
async fn decide_child_approval(
    decider: Option<&Arc<dyn ChildApprovalDecider>>,
    child_session_id: &str,
    request: &serde_json::Value,
) -> bool {
    match decider {
        Some(decider) => decider.decide(child_session_id, request).await,
        None => false,
    }
}

/// How long the host waits for a human approval decision before failing the
/// child's gated tool closed (DENY). Bounds an unanswered request so it can't
/// hang the worker indefinitely.
const CHILD_APPROVAL_TIMEOUT: Duration = Duration::from_secs(300);

/// Extract `(tool_name, permission, resource)` from a worker's approval request
/// body (`{tool_name, permission, resource}`); missing fields default to empty.
fn approval_request_fields(body: &serde_json::Value) -> (String, String, String) {
    let field = |k: &str| {
        body.get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    (field("tool_name"), field("permission"), field("resource"))
}

/// Fulfils a child worker's nested SubAgent spawn request on the host (Phase 6:
/// nested execution). When a child wants a grandchild, its `host.subagent_call`
/// arrives as a `ChildFrame::SubagentRequest`; an implementation runs the real
/// spawn (parenting the grandchild under the requesting child) and returns the
/// SubAgent tool's result JSON. With no handler wired the host replies with a
/// graceful error so the child's tool fails cleanly instead of hanging.
#[async_trait]
pub trait NestedSpawnHandler: Send + Sync {
    /// Run a nested SubAgent spawn requested by `child_session_id`; `request` is
    /// the SubAgent tool-call body. Returns the tool-result JSON, or an error
    /// string (surfaced to the child as a failed tool result).
    async fn spawn_nested(
        &self,
        child_session_id: &str,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, String>;
}

/// The graceful "not available" reply body for an unfulfilled nested spawn.
fn nested_spawn_unavailable(reason: &str) -> serde_json::Value {
    serde_json::json!({
        "success": false,
        "result": reason,
        "display_preference": null,
    })
}

/// Resolve a nested-spawn request to a `SubagentReply` body. With no handler
/// wired, returns the graceful "not available" error body (unchanged default).
async fn fulfil_nested_spawn(
    handler: Option<&Arc<dyn NestedSpawnHandler>>,
    child_session_id: &str,
    request: serde_json::Value,
) -> serde_json::Value {
    match handler {
        Some(handler) => match handler.spawn_nested(child_session_id, request).await {
            Ok(result) => result,
            Err(e) => nested_spawn_unavailable(&format!("nested sub-agent spawn failed: {e}")),
        },
        None => nested_spawn_unavailable("nested sub-agent spawn is not available in this build"),
    }
}

/// Process-global slot for the singleton host-side nested-spawn handler.
///
/// Set once at server startup — AFTER the `ChildSessionAdapter` exists, which is
/// how we resolve the runner→scheduler→adapter construction-order cycle (the
/// runner is built before the adapter that would be its handler, and it's then
/// dyn-erased into the composite runner so it can't be reached for a setter).
/// Read by `drive()` for every child run. Mirrors the `super::live` registry.
fn nested_spawn_handler_slot() -> &'static std::sync::OnceLock<Arc<dyn NestedSpawnHandler>> {
    static SLOT: std::sync::OnceLock<Arc<dyn NestedSpawnHandler>> = std::sync::OnceLock::new();
    &SLOT
}

/// Install the process-global nested-spawn handler (idempotent; first wins).
pub fn set_nested_spawn_handler(handler: Arc<dyn NestedSpawnHandler>) {
    let _ = nested_spawn_handler_slot().set(handler);
}

/// The process-global nested-spawn handler, if installed.
pub fn nested_spawn_handler() -> Option<Arc<dyn NestedSpawnHandler>> {
    nested_spawn_handler_slot().get().cloned()
}

/// Off-loop reviewer for a child's gated-tool approval request (Phase 6, Part B).
///
/// Installed (process-global) by a BYPASSED self-orchestrating worker so its
/// children's forced-ask (dangerous) gated actions — which still raise
/// `ConfirmationRequired` even under bypass — get an LLM reasonableness check
/// rather than a blind pass. `review` is an LLM call: `drive()` invokes it in a
/// SPAWNED task (NEVER in the frame pump) and delivers the verdict async via the
/// live channel, so the agent loop is never blocked.
#[async_trait]
pub trait ChildApprovalReviewer: Send + Sync {
    /// Judge whether the gated action `request` (`{tool_name, permission,
    /// resource}`) is reasonable for `child_session_id`'s task. `true` = approve.
    async fn review(&self, child_session_id: &str, request: &serde_json::Value) -> bool;
}

fn child_approval_reviewer_slot() -> &'static std::sync::OnceLock<Arc<dyn ChildApprovalReviewer>> {
    static SLOT: std::sync::OnceLock<Arc<dyn ChildApprovalReviewer>> = std::sync::OnceLock::new();
    &SLOT
}

/// Install the process-global child-approval reviewer (idempotent; first wins).
pub fn set_child_approval_reviewer(reviewer: Arc<dyn ChildApprovalReviewer>) {
    let _ = child_approval_reviewer_slot().set(reviewer);
}

/// The process-global child-approval reviewer, if installed.
pub fn child_approval_reviewer() -> Option<Arc<dyn ChildApprovalReviewer>> {
    child_approval_reviewer_slot().get().cloned()
}

/// Per-run escalation bridge for non-bypass child-approval routing (Phase 6,
/// Part B). A WORKER's `run()` installs its OWN host bridge here; its `drive()`
/// (driving grandchildren) uses it to RE-PROXY a child's approval request UP to
/// its own parent — chaining the request up every level until a bypass level
/// (model-review) or the top orchestrator (human) decides, then relaying the
/// reply back down. The top server never installs one ⇒ its drive() falls to the
/// human-loop. Set per-run (a worker serves runs sequentially, so one active
/// bridge); a stale bridge from a finished run errors on call ⇒ fail-closed.
fn escalation_bridge_slot(
) -> &'static std::sync::Mutex<Option<bamboo_subagent::executor::HostBridge>> {
    static SLOT: std::sync::Mutex<Option<bamboo_subagent::executor::HostBridge>> =
        std::sync::Mutex::new(None);
    &SLOT
}

/// Install (or clear with `None`) the process escalation host bridge.
pub fn set_escalation_host_bridge(bridge: Option<bamboo_subagent::executor::HostBridge>) {
    *escalation_bridge_slot().lock().unwrap() = bridge;
}

fn escalation_host_bridge() -> Option<bamboo_subagent::executor::HostBridge> {
    escalation_bridge_slot().lock().unwrap().clone()
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
            approval_decider: None,
            nested_spawn_handler: None,
        }
    }

    /// Wire the host-side decider for child gated-tool approval requests
    /// (Phase 2). Without this the host fail-closed DENYs every request.
    pub fn with_approval_decider(mut self, decider: Arc<dyn ChildApprovalDecider>) -> Self {
        self.approval_decider = Some(decider);
        self
    }

    /// Wire the host-side nested SubAgent spawn handler (Phase 6). Without it, a
    /// child's nested spawn request fails gracefully ("not available").
    pub fn with_nested_spawn_handler(mut self, handler: Arc<dyn NestedSpawnHandler>) -> Self {
        self.nested_spawn_handler = Some(handler);
        self
    }

    /// Reuse fingerprint: two children are interchangeable on one warm worker iff
    /// they share role, provider, model, workspace, disabled-tool set, AND every
    /// capability the worker BAKES at provision time (`BambooRuntimeExecutor`
    /// stamps these once and reuses them across runs): nesting depth, nested-spawn
    /// stack, bypass mode, permission enforcement, and the depth cap. Omitting any
    /// of these lets the pool hand a run a worker baked for a DIFFERENT posture —
    /// e.g. a depth-1 worker (with its own spawn stack) reused for a depth-4
    /// child would re-stamp `spawn_depth=1` and pass the depth-cap check, breaking
    /// the recursion bound; or a bypass worker reused for a non-bypass child. So
    /// these MUST split the pool bucket. Everything else (assignment, history) is
    /// shipped per-run in the `RunSpec` and does not affect the fingerprint.
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
        let caps = &spec.capabilities;
        format!(
            "{role}\u{1}{provider}\u{1}{model}\u{1}{workspace}\u{1}{}\u{1}d={}\u{1}ns={}\u{1}by={}\u{1}ep={}\u{1}md={}",
            tools.join(","),
            spec.identity.depth,
            caps.nested_spawn,
            caps.bypass,
            caps.enforce_permissions,
            caps.max_spawn_depth.unwrap_or(0),
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
                // The child session already carries the correct depth
                // (create_child_action's new_child_of did parent.spawn_depth+1);
                // stamp it so the worker can re-establish it on its run session
                // and enforce the max-depth cap across the actor boundary.
                depth: session.spawn_depth,
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
        // Phase 6 (direct nested execution): a worker BELOW the depth cap may
        // orchestrate its OWN children — on startup it builds its own spawn
        // stack and runs the real SubAgent tool (no host proxy). The cap (the
        // SubAgent tool refuses to spawn at/over `max_spawn_depth`) bounds the
        // recursion. Driven purely by the child's depth, so it auto-propagates
        // down the tree without any extra config threading.
        spec.capabilities.nested_spawn = session.spawn_depth < MAX_SPAWN_DEPTH;
        spec.capabilities.max_spawn_depth = Some(MAX_SPAWN_DEPTH);
        // #69: activate child-approval review. Sub-agents enforce permissions so
        // their DANGEROUS actions (the worker uses a HIGH threshold) reach the
        // parent for review — escalated to the human, or model-reviewed off-loop
        // when the parent is in bypass. The worker installs no checker without
        // this, so the whole review chain would otherwise stay dormant.
        spec.capabilities.enforce_permissions = true;
        // Propagate "bypass permissions" so a self-orchestrating worker knows it
        // is a bypassed parent and installs the off-loop model-reviewer for its
        // children's forced-ask actions (Phase 6, Part B). The child session
        // already carries the inherited flag (create_child_action seeds it).
        spec.capabilities.bypass = session
            .agent_runtime_state
            .as_ref()
            .is_some_and(|s| s.bypass_permissions);
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

        // The nested-spawn handler is normally the process-global installed at
        // startup (see `set_nested_spawn_handler`); an explicit per-runner field
        // overrides it (used in tests).
        let nested_handler = self
            .nested_spawn_handler
            .clone()
            .or_else(nested_spawn_handler);
        let result = drive(
            &mut client,
            &job.child_session_id,
            self.approval_decider.as_ref(),
            nested_handler.as_ref(),
            &event_tx,
            &cancel_token,
            &mut live_rx,
        )
        .await;
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
#[allow(clippy::too_many_arguments)]
async fn drive(
    client: &mut ChildClient,
    child_session_id: &str,
    approval_decider: Option<&Arc<dyn ChildApprovalDecider>>,
    nested_spawn_handler: Option<&Arc<dyn NestedSpawnHandler>>,
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
                    Ok(Some(ChildFrame::SubagentRequest { id, body })) => {
                        // Phase 6: a nested worker child proxied a SubAgent tool
                        // call back to the host. A wired `NestedSpawnHandler` runs
                        // the real spawn (parenting a grandchild under this child)
                        // and returns the tool result; with none wired we reply
                        // with a graceful "not available" error so the worker's
                        // tool call fails cleanly instead of hanging.
                        let reply =
                            fulfil_nested_spawn(nested_spawn_handler, child_session_id, body).await;
                        if client
                            .send(ParentFrame::SubagentReply { id, body: reply })
                            .await
                            .is_err()
                        {
                            tracing::warn!("failed to answer subagent_request; connection failing");
                        }
                    }
                    Ok(Some(ChildFrame::ApprovalRequest { id, body })) => {
                        // Phase 2: a worker proxied a gated-tool approval back to
                        // the host. The WORKER side is live — its executor installs
                        // a per-run task-local `ApprovalProxy` (subagent_worker.rs)
                        // that calls `host.approval_call`, so this frame arrives
                        // when a child hits `ConfirmationRequired`.
                        if let Some(reviewer) = child_approval_reviewer() {
                            // Phase 6, Part B: a BYPASSED parent worker
                            // model-reviews its children's forced-ask (dangerous)
                            // actions. The review is an LLM call, so run it OFF
                            // the frame pump in a spawned task and deliver the
                            // verdict async via the live channel — the pump keeps
                            // forwarding events and the agent loop never blocks. A
                            // timeout denies a hung review so the child can't hang.
                            let child = child_session_id.to_string();
                            let req_id = id.clone();
                            let body = body.clone();
                            tokio::spawn(async move {
                                let approved = tokio::time::timeout(
                                    CHILD_APPROVAL_TIMEOUT,
                                    reviewer.review(&child, &body),
                                )
                                .await
                                .unwrap_or(false);
                                super::live::deliver_approval(&child, &req_id, approved);
                            });
                        } else if approval_decider.is_some() {
                            // A decider is wired (policy / auto): decide promptly
                            // and reply inline. (Must not block the pump — see the
                            // `ChildApprovalDecider` doc.)
                            let approved =
                                decide_child_approval(approval_decider, child_session_id, &body)
                                    .await;
                            if client
                                .send(ParentFrame::ApprovalReply { id, approved })
                                .await
                                .is_err()
                            {
                                tracing::warn!(
                                    "failed to answer approval_request; connection failing"
                                );
                            }
                        } else if let Some(host) = escalation_host_bridge() {
                            // Non-bypass WORKER: ESCALATE up our own actor link
                            // (re-proxy) so the request chains to our parent — and
                            // up every level until a bypass level (model-review) or
                            // the top orchestrator (human) decides. Off-loop so the
                            // pump never blocks; relay the reply down to the child.
                            let child = child_session_id.to_string();
                            let req_id = id.clone();
                            let body = body.clone();
                            tokio::spawn(async move {
                                let approved = match tokio::time::timeout(
                                    CHILD_APPROVAL_TIMEOUT,
                                    host.approval_call(body),
                                )
                                .await
                                {
                                    Ok(Ok(reply)) => reply
                                        .get("approved")
                                        .and_then(|v| v.as_bool())
                                        .unwrap_or(false),
                                    // Transport error or timeout ⇒ fail closed.
                                    _ => false,
                                };
                                super::live::deliver_approval(&child, &req_id, approved);
                            });
                        } else {
                            // Top orchestrator (no escalation bridge): human-in-the-
                            // loop. Surface the request on the parent's event stream
                            // and DEFER — the decision arrives out-of-band via
                            // `live::deliver_approval(child, request_id, approved)`
                            // (→ this child's `live_rx` → forwarded to the worker
                            // above). A timeout denies a never-answered request so
                            // it can't hang the child forever.
                            let (tool_name, permission, resource) =
                                approval_request_fields(&body);
                            let _ = event_tx
                                .send(AgentEvent::ChildApprovalRequested {
                                    child_session_id: child_session_id.to_string(),
                                    request_id: id.clone(),
                                    tool_name,
                                    permission,
                                    resource,
                                })
                                .await;
                            let child = child_session_id.to_string();
                            tokio::spawn(async move {
                                tokio::time::sleep(CHILD_APPROVAL_TIMEOUT).await;
                                // No-op if already answered (worker ignores an
                                // unknown/duplicate id) or the child is gone.
                                super::live::deliver_approval(&child, &id, false);
                            });
                        }
                    }
                    Ok(Some(ChildFrame::Terminal { status, result, error, .. })) => {
                        return match status {
                            TerminalStatus::Completed => Ok(result),
                            TerminalStatus::Cancelled => Err(AgentError::Cancelled),
                            TerminalStatus::Error => Err(AgentError::LLM(
                                error.unwrap_or_else(|| "actor child errored".to_string()),
                            )),
                            // The suspend/resume round-trip (host re-dispatch of a
                            // nested parent) is not wired here yet; a worker in
                            // this build never emits Suspended, so this is
                            // unreachable in practice.
                            TerminalStatus::Suspended => Err(AgentError::LLM(
                                "nested sub-agent suspend received but resume transport is not wired"
                                    .to_string(),
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
                depth: 0,
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

    #[test]
    fn fingerprint_splits_on_baked_capabilities() {
        // Every capability baked once at provision time must split the pool
        // bucket, else a worker baked for one posture gets reused for another
        // (e.g. a depth-1 worker re-stamping spawn_depth onto a depth-4 child,
        // breaking the depth cap; or a bypass worker reused for a non-bypass one).
        let base_fp =
            ActorChildRunner::fingerprint(&spec_with("explorer", "p", "m", Some("/ws"), None));

        let mut depth = spec_with("explorer", "p", "m", Some("/ws"), None);
        depth.identity.depth = 2;
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&depth),
            "depth must split"
        );

        let mut nested = spec_with("explorer", "p", "m", Some("/ws"), None);
        nested.capabilities.nested_spawn = true;
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&nested),
            "nested_spawn must split"
        );

        let mut bypass = spec_with("explorer", "p", "m", Some("/ws"), None);
        bypass.capabilities.bypass = true;
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&bypass),
            "bypass must split"
        );

        let mut enforce = spec_with("explorer", "p", "m", Some("/ws"), None);
        enforce.capabilities.enforce_permissions = true;
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&enforce),
            "enforce_permissions must split"
        );

        let mut cap = spec_with("explorer", "p", "m", Some("/ws"), None);
        cap.capabilities.max_spawn_depth = Some(8);
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&cap),
            "max_spawn_depth must split"
        );
    }

    struct StaticDecider(bool);

    #[async_trait]
    impl ChildApprovalDecider for StaticDecider {
        async fn decide(&self, _child: &str, _req: &serde_json::Value) -> bool {
            self.0
        }
    }

    #[tokio::test]
    async fn child_approval_fails_closed_without_decider() {
        // No decider wired ⇒ the host denies (safe default), unchanged behavior.
        let body = serde_json::json!({"tool_name":"Bash","permission":"run","resource":"rm -rf /"});
        assert!(!decide_child_approval(None, "child-1", &body).await);
    }

    #[tokio::test]
    async fn child_approval_honors_wired_decider() {
        let body =
            serde_json::json!({"tool_name":"Write","permission":"write","resource":"/tmp/x"});
        let approve: Arc<dyn ChildApprovalDecider> = Arc::new(StaticDecider(true));
        let deny: Arc<dyn ChildApprovalDecider> = Arc::new(StaticDecider(false));
        assert!(decide_child_approval(Some(&approve), "child-1", &body).await);
        assert!(!decide_child_approval(Some(&deny), "child-1", &body).await);
    }

    struct StaticSpawn(Result<serde_json::Value, String>);

    #[async_trait]
    impl NestedSpawnHandler for StaticSpawn {
        async fn spawn_nested(
            &self,
            _child: &str,
            _req: serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            self.0.clone()
        }
    }

    #[tokio::test]
    async fn nested_spawn_unavailable_without_handler() {
        let reply = fulfil_nested_spawn(None, "child-1", serde_json::json!({})).await;
        assert_eq!(reply["success"], serde_json::json!(false));
        assert!(reply["result"].as_str().unwrap().contains("not available"));
    }

    #[tokio::test]
    async fn nested_spawn_returns_handler_result_or_error_body() {
        let ok: Arc<dyn NestedSpawnHandler> = Arc::new(StaticSpawn(Ok(
            serde_json::json!({"success": true, "result": "spawned"}),
        )));
        let reply = fulfil_nested_spawn(Some(&ok), "child-1", serde_json::json!({})).await;
        assert_eq!(reply["result"], serde_json::json!("spawned"));

        let err: Arc<dyn NestedSpawnHandler> = Arc::new(StaticSpawn(Err("boom".to_string())));
        let reply = fulfil_nested_spawn(Some(&err), "child-1", serde_json::json!({})).await;
        assert_eq!(reply["success"], serde_json::json!(false));
        assert!(reply["result"].as_str().unwrap().contains("boom"));
    }

    #[test]
    fn approval_request_fields_extracts_and_defaults() {
        let full = serde_json::json!({"tool_name":"Bash","permission":"run","resource":"ls"});
        assert_eq!(
            approval_request_fields(&full),
            ("Bash".to_string(), "run".to_string(), "ls".to_string())
        );
        // Missing fields default to empty strings.
        let partial = serde_json::json!({"tool_name":"Write"});
        assert_eq!(
            approval_request_fields(&partial),
            ("Write".to_string(), String::new(), String::new())
        );
    }
}
