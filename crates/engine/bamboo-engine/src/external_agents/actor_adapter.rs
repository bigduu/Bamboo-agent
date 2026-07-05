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
use bamboo_domain::poison::PoisonRecover;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_subagent::fleet::{spawn_worker_on_bus, SpawnedChild};
use bamboo_subagent::proto::{AgentRecord, ChildFrame, ParentFrame, RunSpec, TerminalStatus};
use bamboo_subagent::provision::{
    ChildIdentity, ExecutorSpec, ModelRefSpec, Placement, ProvisionSpec, ScopedCredential,
};
use bamboo_subagent::transport::{client_config_trusting_cert, ChildClient};

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

/// Deadline for a local worker's FIRST frame after a Run is dispatched. A warm
/// worker answers in seconds; a cold spawn within tens. Total silence past this
/// means the worker is dead (e.g. a pooled worker that exited right after its
/// liveness check) and its Run is queued with nobody to serve it — trip it so the
/// runner respawns once instead of hanging forever. Generous, to never false-trip
/// a slow-but-healthy cold start.
const WORKER_FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(60);

/// A warm worker on the mailbox bus, parked for reuse between runs. It stays
/// dialed-in + subscribed to `mailbox_id`; the next interchangeable child
/// delivers its `Run` there instead of spawning a fresh process. Dropping it
/// kills a local kill-on-drop subprocess; a remote / schedulable handle is
/// process-less (`kill()` is a no-op — it self-manages via its idle timeout).
struct PooledWorker {
    worker: SpawnedChild,
    /// The bus mailbox this worker subscribes to (where its `Run`s are delivered).
    mailbox_id: String,
}

/// A role pinned to a remote resident worker (remote-actor-plan §3.4 / P1.5,
/// #193), resolved at runner-build time from `SubagentsConfig.remote_placements`:
/// the env-named bearer is already READ into `token` here (the raw token never
/// rides the config), and `ca_cert_file` is the path to a PEM pinning a
/// self-signed worker cert (`None` ⇒ default webpki roots / plaintext `ws://`).
#[derive(Debug, Clone)]
pub struct ResolvedRemotePlacement {
    pub endpoint: String,
    pub token: Option<String>,
    pub ca_cert_file: Option<PathBuf>,
    /// Display name for the machine this role runs on — the matching cluster
    /// node's `label`/host, surfaced on the UI placement badge. `None` ⇒ derive
    /// from the endpoint host.
    pub host_label: Option<String>,
}

/// A role routed to a SCHEDULED worker (remote-actor-plan §3.4 / P2b, #181),
/// resolved at runner-build time from `SubagentsConfig.schedulable_placements`.
/// Names the logical `pool` (= the bus role) whose LIVE connected workers are the
/// scheduling candidates — the runner picks one via the bus presence query
/// (`BrokerClient::list_connected`). Phase 3 retired the old HTTP registry, so a
/// pool is now just a role on the bus.
#[derive(Debug, Clone)]
pub struct ResolvedSchedulablePlacement {
    pub pool: String,
    /// Display name for the machine this pool's workers run on — the matching
    /// cluster node's `label`/host, surfaced on the UI placement badge. `None` ⇒
    /// fall back to the pool name.
    pub host_label: Option<String>,
}

/// How `execute_external_child` should obtain its worker connection, decided
/// once from `spec.placement`. Splits the divergent acquire/connect + retire
/// logic three ways while the shared middle (Run dispatch, live registration,
/// drive, close) stays identical. `Local` is the unchanged pre-#193 path;
/// `Remote` is the unchanged #194 path; `Schedulable` (#181, P2b) is new.
enum PlacementKind {
    Local,
    Remote,
    Schedulable,
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
    /// The mailbox bus to run local children over (the unified transport). Local
    /// sub-agents require it; `None` only when no broker could be embedded.
    bus: Option<bamboo_subagent::BusEndpoint>,
    /// Backpressure: bounds the number of concurrently *running* actors; further
    /// runs wait for a slot instead of exploding the process table. (Idle pooled
    /// workers do not hold a slot.)
    concurrency: std::sync::Arc<tokio::sync::Semaphore>,
    /// Warm-worker pool keyed by a reuse fingerprint
    /// (role/provider/model/workspace/disabled-tools/baked-caps). A finished run
    /// parks its bus worker here so the next interchangeable child reuses it
    /// (delivers its `Run` to the same mailbox) instead of spawning a fresh
    /// process — collapsing N sibling sub-agents onto a few warm workers.
    pool: Arc<tokio::sync::Mutex<HashMap<String, Vec<PooledWorker>>>>,
    max_idle_per_key: usize,
    /// Host-side decision for a child's gated-tool approval request (Phase 2).
    /// `None` ⇒ fail-closed DENY (the safe default). A wired decider (policy or
    /// human-routing bridge) returns approve/deny over the actor WS.
    approval_decider: Option<Arc<dyn ChildApprovalDecider>>,
    /// Per-run escalation host bridge for non-bypass child-approval routing (#68;
    /// Phase 6, Part B). The owning worker's `run()` installs its OWN host bridge
    /// here via `set_escalation_bridge`; `execute_external_child` CAPTURES it at
    /// grandchild-spawn time and hands the owned value to `drive()`, which uses it
    /// to RE-PROXY a child's approval request UP to the parent run — chaining up
    /// every level until a bypass level (model-review) or the top orchestrator
    /// (human) decides, then relaying the reply back down. Was a process-global
    /// slot; now per-runner so a fire-and-forget grandchild that OUTLIVES the run
    /// that spawned it keeps that run's bridge for its whole lifetime instead of
    /// reading a stale/overwritten global at approval time (→ fail-closed deny).
    escalation_bridge: Arc<std::sync::Mutex<Option<bamboo_subagent::executor::HostBridge>>>,
    /// Roles pinned to a REMOTE resident worker (#193), keyed by sub-agent role
    /// (the child's `subagent_type`). A role present here routes through the
    /// dedicated remote branch in `execute_external_child` (Bearer-authenticated
    /// `wss://` connect, no spawn, no pool, no kill) instead of the local
    /// subprocess + warm-pool path. Empty (the default) = all-local behavior.
    remote_placements: HashMap<String, ResolvedRemotePlacement>,
    /// Roles routed to a REGISTRY-SCHEDULED worker (#181, P2b), keyed by sub-agent
    /// role. A role present here (AND not already in `remote_placements`, which
    /// wins) routes through the dedicated SCHEDULABLE branch in
    /// `execute_external_child`: query the registry for live workers in the pool,
    /// pick one (round-robin), connect over `wss://` — no spawn, no pool, no kill,
    /// and NO local-subprocess fallback (no live worker ⇒ a clear error). Empty
    /// (the default) = all-local behavior.
    schedulable_placements: HashMap<String, ResolvedSchedulablePlacement>,
    /// Per-pool round-robin cursor for schedulable scheduling (#181, P2b). Bumped
    /// once per pick so successive sibling spawns SPREAD across a pool's live
    /// workers instead of all landing on the first candidate. Best-effort spread,
    /// not a load balancer — the registry's live set can change between picks.
    schedule_cursor: Arc<std::sync::Mutex<HashMap<String, usize>>>,
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
            bus: None,
            concurrency: std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent.max(1))),
            pool: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            max_idle_per_key: DEFAULT_MAX_IDLE_PER_KEY,
            approval_decider: None,
            escalation_bridge: Arc::new(std::sync::Mutex::new(None)),
            remote_placements: HashMap::new(),
            schedulable_placements: HashMap::new(),
            schedule_cursor: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Run children over the mailbox bus (the unified actor+mailbox transport).
    /// When set, local children dial this bus and are driven by mailbox id; when
    /// unset they use the legacy direct-WS path. The server passes its in-process
    /// broker here (`subagents.broker`); tests without a broker leave it unset.
    pub fn with_bus(mut self, bus: Option<bamboo_subagent::BusEndpoint>) -> Self {
        self.bus = bus.filter(|b| !b.endpoint.trim().is_empty());
        self
    }

    /// Wire the host-side decider for child gated-tool approval requests
    /// (Phase 2). Without this the host fail-closed DENYs every request.
    pub fn with_approval_decider(mut self, decider: Arc<dyn ChildApprovalDecider>) -> Self {
        self.approval_decider = Some(decider);
        self
    }

    /// Pin specific sub-agent roles to remote resident workers (#193). The map
    /// is keyed by role (`subagent_type`); a child whose role is present connects
    /// over `wss://` to the resolved endpoint instead of spawning a local
    /// subprocess. Default (empty) keeps every role on the local path — exactly
    /// today's behavior.
    pub fn with_remote_placements(
        mut self,
        placements: HashMap<String, ResolvedRemotePlacement>,
    ) -> Self {
        self.remote_placements = placements;
        self
    }

    /// Route specific sub-agent roles to a registry-SCHEDULED worker (#181, P2b).
    /// The map is keyed by role (`subagent_type`); a child whose role is present
    /// (and NOT already pinned by `remote_placements`, which takes precedence) is
    /// run on a live worker discovered from the registry instead of a local
    /// subprocess. Default (empty) keeps every role on the local path.
    pub fn with_schedulable_placements(
        mut self,
        placements: HashMap<String, ResolvedSchedulablePlacement>,
    ) -> Self {
        self.schedulable_placements = placements;
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
    /// Reuse fingerprint (role/provider/model/workspace/disabled-tools/baked
    /// caps): two children with the same fingerprint are interchangeable on one
    /// warm worker, so they share a pool bucket. Any axis the worker bakes ONCE
    /// at provision time MUST be in here, else a worker baked for one posture
    /// gets reused for another (see the `fingerprint_*` tests).
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
            "{role}\u{1}{provider}\u{1}{model}\u{1}{workspace}\u{1}{}\u{1}d={}\u{1}ns={}\u{1}by={}\u{1}ep={}\u{1}md={}\u{1}nha={}\u{1}gro={}",
            tools.join(","),
            spec.identity.depth,
            caps.nested_spawn,
            caps.bypass,
            caps.enforce_permissions,
            caps.max_spawn_depth.unwrap_or(0),
            // #73 review (P1): a worker bakes `no_human_review` ONCE from this flag
            // at build() and never re-reads it per run, so the pool MUST NOT hand a
            // worker baked for one approval posture to a run of the opposite one —
            // else a scheduled-root worker reused for an interactive child would
            // silently model-review instead of asking the human (and vice-versa,
            // reintroducing the 300s-deny). Split the bucket on it.
            caps.no_human_approver,
            // #71: the read-only Bash checker is baked once at build() from this
            // flag, so a guardian-reviewer worker must NOT be reused for an
            // ordinary child (which expects unrestricted Bash), and vice-versa.
            caps.guardian_read_only,
        )
    }

    /// Check out a warm bus worker for `key`, reusing a live parked one if any,
    /// else spawning a fresh one that dials the bus. The returned worker is OWNED
    /// by the caller for the run's duration (checkout removes it from the pool, so
    /// a concurrent sibling gets a different worker or spawns its own — one run per
    /// worker at a time, matching the pre-bus pool semantics).
    async fn acquire_bus_worker(
        &self,
        key: &str,
        spec: &ProvisionSpec,
    ) -> crate::runtime::runner::Result<PooledWorker> {
        // Drain the bucket, skipping (and reaping) any worker whose process exited
        // while parked. A live one is handed straight out for reuse.
        loop {
            let candidate = {
                let mut pool = self.pool.lock().await;
                pool.get_mut(key).and_then(|bucket| bucket.pop())
            };
            let Some(mut candidate) = candidate else { break };
            if candidate.worker.is_alive() {
                return Ok(candidate);
            }
            candidate.worker.kill().await;
        }

        let spawned = spawn_worker_on_bus(&self.worker_bin, &self.worker_args, spec)
            .await
            .map_err(|e| AgentError::LLM(format!("actor spawn (bus) failed: {e}")))?;
        let mailbox_id = spawned.record.agent_id.clone();
        Ok(PooledWorker {
            worker: spawned,
            mailbox_id,
        })
    }

    /// Park a warm bus worker for reuse after a clean run; if its bucket is full
    /// (or it died), kill it instead. The worker stays dialed-in + subscribed
    /// while parked, so a reusing child just delivers a new `Run` to its mailbox.
    async fn release_bus_worker(&self, key: &str, mut worker: PooledWorker) {
        if !worker.worker.is_alive() {
            worker.worker.kill().await;
            return;
        }
        let mut pool = self.pool.lock().await;
        let bucket = pool.entry(key.to_string()).or_default();
        if bucket.len() >= self.max_idle_per_key {
            drop(pool);
            worker.worker.kill().await;
            return;
        }
        bucket.push(worker);
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
        // Unified transport: when a bus is configured, the child dials it (no
        // listen socket / file discovery) and the parent drives it by mailbox id.
        spec.bus = self.bus.clone();
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
        // #73: propagate "no interactive human approver" (headless / scheduled /
        // deployed root, inherited by the child session). When set, the worker's
        // per-run approval proxy model-reviews a gated action locally instead of
        // escalating to a human who will never answer (which would 300s-deny).
        spec.capabilities.no_human_approver = session
            .agent_runtime_state
            .as_ref()
            .is_some_and(|s| s.no_human_approver);
        // #71: mark a READ-ONLY Guardian reviewer so the worker installs the
        // read-only Bash allowlist checker. The reviewer is spawned by
        // `spawn_guardian_review` with `subagent_type == "guardian"` (the SAME
        // marker the completion coordinator branches on to parse the verdict) AND
        // the `guardian_read_only_disabled_tools` denylist. Keyed off that role
        // marker (already read above to set `identity.role`), so it rides the same
        // session-metadata path the denylist/subagent_type use — no new wire seam.
        // Without this the worker keeps an UNRESTRICTED Bash, so the reviewer could
        // still `rm -rf` / `git push` / `curl | sh`, defeating "read-only".
        spec.capabilities.guardian_read_only =
            session.metadata.get("subagent_type").map(String::as_str) == Some("guardian");
        // #193: route this role to a REMOTE resident worker when one is pinned.
        // `spec.identity.role` was just computed from `subagent_type` above; a
        // match flips the placement to Remote and rides the worker's bearer on the
        // scoped secrets envelope (TLS handshake / Authorization header only — the
        // token is never logged). No match leaves the default `Placement::Local`,
        // so the local path is byte-for-byte unchanged for every non-pinned role.
        if let Some(placement) = self.remote_placements.get(spec.identity.role.as_str()) {
            spec.placement = Placement::Remote {
                endpoint: placement.endpoint.clone(),
            };
            spec.secrets.worker_auth_token = placement.token.clone();
        } else if let Some(placement) = self.schedulable_placements.get(spec.identity.role.as_str())
        {
            // #181 (P2b): route this role to a SCHEDULED worker — ONLY when it is
            // NOT already pinned to a fixed remote endpoint (the `else if` makes
            // remote_placements take precedence for a role in both). The concrete
            // worker is picked at run time in `execute_external_child` from the bus
            // (a live connected worker of the pool role). No per-placement bearer
            // now — the bus connection uses the bus token. No match in either map
            // leaves the default `Placement::Local`.
            spec.placement = Placement::Schedulable {
                pool: placement.pool.clone(),
            };
        }
        spec
    }

    /// The `metadata["placement"]` JSON to stamp on a child from its resolved
    /// placement, preferring the matching cluster node's `host_label` (its
    /// operator label/host) over the raw endpoint/pool. `None` for a Local child
    /// (the DTO defaults it to the backend's own host). Split out of
    /// `execute_external_child` so the role→placement→host resolution is unit-testable.
    fn placement_stamp_for(&self, spec: &ProvisionSpec) -> Option<String> {
        let host_label = match &spec.placement {
            Placement::Remote { .. } => self
                .remote_placements
                .get(spec.identity.role.as_str())
                .and_then(|p| p.host_label.as_deref()),
            Placement::Schedulable { .. } => self
                .schedulable_placements
                .get(spec.identity.role.as_str())
                .and_then(|p| p.host_label.as_deref()),
            Placement::Local => None,
        };
        placement_metadata(&spec.placement, host_label)
    }

    /// Pick a live worker for a SCHEDULABLE role from the BUS (#181, Phase 3):
    /// ask the broker which actors are connected serving the pool role (presence
    /// is connection-truth — no HTTP registry, no leases, no connect-fail
    /// failover), then round-robin one per resolve for spread. Returns the chosen
    /// worker's mailbox id. An empty pool ⇒ a terminal `AgentError` — NEVER a
    /// local-subprocess fallback (that would silently defeat the placement).
    async fn resolve_schedulable_worker(
        &self,
        role: &str,
    ) -> std::result::Result<String, AgentError> {
        let pool = self
            .schedulable_placements
            .get(role)
            .ok_or_else(|| {
                AgentError::LLM(format!(
                    "schedulable placement for role '{role}' vanished before scheduling"
                ))
            })?
            .pool
            .clone();
        let bus = self.bus.as_ref().ok_or_else(|| {
            AgentError::LLM(format!(
                "schedulable role '{role}': no mailbox bus configured (subagents.broker)"
            ))
        })?;

        // Ask the BUS who is connected serving the pool role — presence is
        // connection-truth (no HTTP registry, no leases, no stale-record failover).
        let mut q = bamboo_broker::BrokerClient::connect(
            &bus.endpoint,
            bamboo_subagent::AgentRef {
                session_id: format!("sched-q-{role}"),
                role: None,
            },
            &bus.token,
        )
        .await
        .map_err(|e| AgentError::LLM(format!("schedulable role '{role}': bus connect failed: {e}")))?;
        let candidates = q.list_connected(&pool).await.map_err(|e| {
            AgentError::LLM(format!("schedulable role '{role}': bus presence query failed: {e}"))
        })?;

        if candidates.is_empty() {
            return Err(AgentError::LLM(format!(
                "schedulable role '{role}': no live worker in pool '{pool}' on the bus \
                 (NOT spawning a local subprocess — a schedulable role has no local fallback)"
            )));
        }

        // Round-robin: advance a per-pool cursor once per resolve so successive
        // sibling spawns spread across the connected pool workers. No failover
        // needed — a listed worker is connected NOW (the bus only lists live
        // subscribers), so there is no stale-but-leased candidate to skip.
        let idx = {
            let mut cursors = self.schedule_cursor.lock().recover_poison();
            let cursor = cursors.entry(pool.clone()).or_insert(0);
            let i = *cursor % candidates.len();
            *cursor = cursor.wrapping_add(1);
            i
        };
        Ok(candidates[idx].clone())
    }
}

#[async_trait]
impl ExternalChildRunner for ActorChildRunner {
    async fn should_handle(&self, session: &Session) -> bool {
        session.metadata.get("runtime.kind") == Some(&"external".to_string())
            && session.metadata.get("external.protocol") == Some(&"actor".to_string())
            && session.metadata.get("external.agent_id") == Some(&self.agent_id)
    }

    fn set_escalation_bridge(&self, bridge: Option<bamboo_subagent::executor::HostBridge>) {
        *self.escalation_bridge.lock().recover_poison() = bridge;
    }

    async fn execute_external_child(
        &self,
        session: &mut Session,
        job: &SpawnJob,
        event_tx: mpsc::Sender<AgentEvent>,
        cancel_token: CancellationToken,
    ) -> crate::runtime::runner::Result<()> {
        // #68 CORRECTNESS CRUX: capture the per-run escalation bridge HERE, at the
        // moment this grandchild is spawned — while the parent run's bridge is
        // still in our slot — into an owned local handed to `drive()` for this
        // grandchild's whole lifetime. A fire-and-forget grandchild that OUTLIVES
        // the run that spawned it must NOT re-read `self.escalation_bridge` at
        // approval time: by then `run()` may have cleared/overwritten it (a worker
        // serves runs sequentially), and re-proxying through a closed bridge
        // fail-closed denies. Capturing at spawn pins the right bridge per run.
        let escalation = self.escalation_bridge.lock().recover_poison().clone();
        let assignment = extract_assignment(session);
        let mut spec = self.build_spec(session, job);
        // Mark the worker reusable + give it an idle timeout so it self-reaps if
        // orphaned. Warm bus workers are pooled per fingerprint and reused.
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

        // Split LOCAL (spawn + warm-pool) from the two process-less remote paths
        // ONLY at the divergent spots — acquire/connect here and the park/retire at
        // the end. Everything between (Run dispatch, live-actor registration,
        // drive, the close) is identical for all three. `kind` is the single guard.
        //   - Local       (#0):  byte-for-byte the pre-#193 reuse-or-spawn path.
        //   - Remote       (#194): connect to a FIXED resident endpoint, no spawn.
        //   - Schedulable  (#181): resolve a live worker from the registry, connect.
        let kind = match spec.placement {
            Placement::Remote { .. } => PlacementKind::Remote,
            Placement::Schedulable { .. } => PlacementKind::Schedulable,
            Placement::Local => PlacementKind::Local,
        };
        let remote = !matches!(kind, PlacementKind::Local);

        // Stamp WHICH machine this child runs on onto its session metadata, so the
        // UI can show it (mirrored into the session index → SessionSummary.placement).
        // Only remote/scheduled placements need a stamp — a Local child falls through
        // to the DTO default (this backend's own host). Persisted by the caller with
        // the rest of the child session after we return.
        if let Some(placement_meta) = self.placement_stamp_for(&spec) {
            session
                .metadata
                .insert("placement".to_string(), placement_meta);
        }

        // Retry-once loop: a pooled local worker can die between its liveness
        // check and handling the Run (a tiny TOCTOU window) — its Run then sits
        // queued with no server. The first-frame watchdog in `drive` surfaces that
        // as `WorkerUnresponsive`; we reap the dead worker and re-acquire ONCE
        // (which spawns fresh / reuses the next live one). Remote/schedulable have
        // no spawn fallback, so they never retry.
        let mut attempt = 0u8;
        let (result, actor) = loop {
        let (actor, mut client) = match kind {
            PlacementKind::Remote => {
                // REMOTE branch: connect to a resident worker. No spawn, no pool
                // touch, no drain. We do not own the worker, so a connect failure
                // has NO respawn fallback — it is a clear, terminal error.
                let placement = self
                    .remote_placements
                    .get(spec.identity.role.as_str())
                    .ok_or_else(|| {
                        AgentError::LLM(format!(
                            "remote placement for role '{}' vanished before connect",
                            spec.identity.role
                        ))
                    })?;
                let endpoint = placement.endpoint.clone();
                // Build the TLS trust: a pinned CA pins a self-signed worker cert;
                // otherwise default webpki roots (or plaintext for `ws://`).
                let trust_cfg = match placement.ca_cert_file.as_deref() {
                    Some(path) => Some(client_config_trusting_cert(path).map_err(|e| {
                        AgentError::LLM(format!("remote worker CA cert '{}': {e}", path.display()))
                    })?),
                    None => None,
                };
                let client = ChildClient::connect_with_auth_tls(
                    &endpoint,
                    placement.token.as_deref(),
                    trust_cfg,
                )
                .await
                .map_err(|e| {
                    AgentError::LLM(format!("remote actor connect to '{endpoint}' failed: {e}"))
                })?;
                // Process-less handle so live-actor registration (in-band steering)
                // works exactly as for a local worker; `kill()` is a no-op.
                let record = AgentRecord {
                    agent_id: job.child_session_id.clone(),
                    role: spec.identity.role.clone(),
                    labels: Vec::new(),
                    endpoint: endpoint.clone(),
                    pid: 0,
                    version: String::new(),
                    started_at: chrono::Utc::now(),
                    lease_expires_at: chrono::Utc::now(),
                };
                let _ = endpoint;
                let actor = PooledWorker {
                    worker: SpawnedChild::remote(record),
                    mailbox_id: job.child_session_id.clone(),
                };
                let client: Box<dyn bamboo_subagent::ChildLink> = Box::new(client);
                (actor, client)
            }
            PlacementKind::Schedulable => {
                // SCHEDULABLE branch (#181): pick a LIVE worker of the pool role
                // from the BUS (presence = connection-truth; no HTTP registry, no
                // leases, no failover) and drive it by mailbox id. The pool worker
                // stays connected and is reused next time. No spawn, no kill, NO
                // local fallback — an empty pool is a terminal error (raised in
                // resolve_schedulable_worker).
                let bus = self.bus.as_ref().ok_or_else(|| {
                    AgentError::LLM(
                        "schedulable sub-agents require a mailbox bus (subagents.broker)"
                            .to_string(),
                    )
                })?;
                let mailbox_id = self
                    .resolve_schedulable_worker(spec.identity.role.as_str())
                    .await?;
                let parent = bamboo_subagent::AgentRef {
                    session_id: format!("p-{}", job.child_session_id),
                    role: None,
                };
                let link = bamboo_broker::BrokerChildLink::connect(
                    &bus.endpoint,
                    parent,
                    &bus.token,
                    mailbox_id.clone(),
                )
                .await
                .map_err(|e| {
                    AgentError::LLM(format!("schedulable link connect to '{mailbox_id}' failed: {e}"))
                })?;
                // Process-less handle — a bus-resident pool worker is never ours to
                // kill (remote ⇒ dropped, not pooled, after the run).
                let actor = PooledWorker {
                    worker: SpawnedChild::remote(AgentRecord {
                        agent_id: mailbox_id.clone(),
                        role: spec.identity.role.clone(),
                        labels: Vec::new(),
                        endpoint: bus.endpoint.clone(),
                        pid: 0,
                        version: String::new(),
                        started_at: chrono::Utc::now(),
                        lease_expires_at: chrono::Utc::now(),
                    }),
                    mailbox_id,
                };
                let client: Box<dyn bamboo_subagent::ChildLink> = Box::new(link);
                (actor, client)
            }
            PlacementKind::Local => {
                // LOCAL = the mailbox bus (the unified transport): check out a warm
                // pooled worker (reuse a live parked one, else spawn fresh) and
                // drive it by mailbox id — no listen socket, no file discovery, no
                // respawn-on-connect-miss (the broker queues the Run until the
                // worker handles it). The legacy direct-WS path was retired; the bus
                // is required.
                let bus = self.bus.as_ref().ok_or_else(|| {
                    AgentError::LLM(
                        "local sub-agents require a mailbox bus (subagents.broker); none is \
                         configured and the bus could not be embedded"
                            .to_string(),
                    )
                })?;
                let actor = self.acquire_bus_worker(&pool_key, &spec).await?;
                let parent = bamboo_subagent::AgentRef {
                    session_id: format!("p-{}", job.child_session_id),
                    role: None,
                };
                let link = bamboo_broker::BrokerChildLink::connect(
                    &bus.endpoint,
                    parent,
                    &bus.token,
                    actor.mailbox_id.clone(),
                )
                .await
                .map_err(|e| AgentError::LLM(format!("broker child link connect failed: {e}")))?;
                let client: Box<dyn bamboo_subagent::ChildLink> = Box::new(link);
                (actor, client)
            }
        };

        if let Err(e) = client
            .send(ParentFrame::Run(RunSpec {
                // Cloned (not moved) so a retry can re-dispatch to a fresh worker.
                assignment: assignment.clone(),
                reasoning_effort: None,
                messages: messages.clone(),
            }))
            .await
        {
            if !remote {
                actor.worker.kill().await;
            }
            return Err(AgentError::LLM(format!("actor run dispatch failed: {e}")));
        }

        // Register as a live actor so send_message (running, no interrupt) can
        // steer this child in-band over the existing WS connection. The guard
        // unregisters on every exit path.
        let (live_tx, mut live_rx) = mpsc::unbounded_channel::<ParentFrame>();
        let live_guard = super::live::register(&job.child_session_id, live_tx);

        let result = drive(
            &mut *client,
            &job.child_session_id,
            self.approval_decider.as_ref(),
            escalation.clone(),
            &event_tx,
            &cancel_token,
            &mut live_rx,
            // First-frame watchdog for EVERY placement: a wedged-but-connected
            // worker (subscribed ≠ serving — e.g. stuck on a prior LLM call) emits
            // no first frame; without a deadline drive() blocks forever. Bounding it
            // turns the "running-but-unresponsive" hang into a recoverable
            // WorkerUnresponsive (reap+respawn local / re-pick schedulable / error
            // on a fixed remote endpoint).
            Some(WORKER_FIRST_FRAME_TIMEOUT),
        )
        .await;
        // Unregister IMMEDIATELY: after drive returns nobody consumes live_rx,
        // so a send_message landing in the close/park window below must see
        // "not live" and take the durable-queue fallback instead of vanishing.
        // (Even if one slipped in earlier, send_message also appends it to the
        // durable transcript, so the next activation still rehydrates it.)
        drop(live_guard);
        // Close the parent link (dropping it closes our broker connection; the
        // worker stays dialed-in + subscribed, ready for its next Run).
        drop(client);

        // No first frame ⇒ the worker is wedged. Recover ONCE before giving up:
        //   - Local: reap the dead pooled worker + respawn.
        //   - Schedulable: not ours to kill — drop it and re-select a live pool
        //     member (a wedged worker must not fail the run when the pool has others).
        //   - Remote: a FIXED endpoint has no alternative — fall through to a bounded
        //     WorkerUnresponsive error (far better than the previous infinite hang).
        if attempt == 0 && matches!(result, Err(AgentError::WorkerUnresponsive(_))) {
            match kind {
                PlacementKind::Local => {
                    tracing::warn!(
                        "actor child {} got no first frame; reaping the worker and respawning once",
                        job.child_session_id
                    );
                    actor.worker.kill().await;
                    attempt += 1;
                    continue;
                }
                PlacementKind::Schedulable => {
                    tracing::warn!(
                        "scheduled actor child {} got no first frame; re-selecting a pool worker",
                        job.child_session_id
                    );
                    drop(actor);
                    attempt += 1;
                    continue;
                }
                PlacementKind::Remote => {}
            }
        }
        break (result, actor);
        };

        // Park the warm worker for reuse on a clean run, or kill it on
        // error/cancel (a wedged worker must not be reused). Remote / schedulable
        // workers are registry-managed — never ours to pool/kill, just drop.
        if remote {
            drop(actor);
        } else {
            match &result {
                Ok(_) => self.release_bus_worker(&pool_key, actor).await,
                Err(_) => actor.worker.kill().await,
            }
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

/// The `{kind,host}` placement descriptor stamped onto a child session's metadata
/// under `"placement"` — read back by the storage index → `SessionSummary.placement`
/// → the UI's machine badge. `None` for `Local` (those fall through to the DTO's
/// default of this backend's own host). The value is a JSON string matching
/// `bamboo_storage::SessionPlacement { kind, host }`.
fn placement_metadata(placement: &Placement, host_label: Option<&str>) -> Option<String> {
    // Prefer the cluster node's own label/host (its metadata) when the placement
    // maps to a node; else fall back to the raw endpoint host / pool name.
    let value = match placement {
        Placement::Local => return None,
        Placement::Remote { endpoint } => serde_json::json!({
            "kind": "remote",
            "host": host_label.map(str::to_string).unwrap_or_else(|| host_of_endpoint(endpoint)),
        }),
        Placement::Schedulable { pool } => serde_json::json!({
            "kind": "remote",
            "host": host_label.unwrap_or(pool),
        }),
    };
    serde_json::to_string(&value).ok()
}

/// Extract the host from a `ws[s]://host:port[/path]` bus endpoint, for display.
fn host_of_endpoint(endpoint: &str) -> String {
    endpoint
        .trim()
        .trim_start_matches("wss://")
        .trim_start_matches("ws://")
        .split(['/', ':'])
        .next()
        .unwrap_or(endpoint)
        .to_string()
}

/// Pump child frames -> parent events until a terminal frame (or cancellation).
/// On success, yields the actor's final result text (for session write-back).
/// `live_rx` carries in-band frames (steering messages) from the live registry.
///
/// `escalation_bridge` (#68) is the per-run escalation host bridge CAPTURED BY
/// VALUE at spawn time in `execute_external_child` (NOT read live here): when a
/// non-bypass child re-proxies an approval request, this owned bridge routes it
/// UP to the parent run. Owning it for the call's lifetime is what lets a
/// fire-and-forget grandchild that outlives its spawning run still escalate to
/// the correct (then-current) parent bridge rather than a stale/overwritten one.
async fn drive(
    client: &mut dyn bamboo_subagent::ChildLink,
    child_session_id: &str,
    approval_decider: Option<&Arc<dyn ChildApprovalDecider>>,
    escalation_bridge: Option<bamboo_subagent::executor::HostBridge>,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
    live_rx: &mut mpsc::UnboundedReceiver<ParentFrame>,
    first_frame_timeout: Option<Duration>,
) -> crate::runtime::runner::Result<Option<String>> {
    // First-frame watchdog: a live worker emits its first frame (run-started /
    // first token) within seconds; total silence past the deadline means the
    // worker is dead (e.g. a pooled worker that exited right after checkout), so
    // its Run sits queued forever. We trip ONLY before the first frame — once any
    // frame arrives the worker is proven live and a legitimately long run (a slow
    // tool between tokens) never trips it.
    let mut got_first_frame = false;
    let mut first_frame_watch = first_frame_timeout.map(|d| Box::pin(tokio::time::sleep(d)));
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                // fall through to the cancel handling below
                break;
            }
            _ = async {
                match first_frame_watch.as_mut() {
                    Some(s) => s.as_mut().await,
                    None => std::future::pending::<()>().await,
                }
            }, if !got_first_frame => {
                return Err(AgentError::WorkerUnresponsive(format!(
                    "child {child_session_id} produced no frame within {:?}",
                    first_frame_timeout.unwrap_or_default()
                )));
            }
            Some(frame) = live_rx.recv() => {
                // Forward in-band steering to the worker over the existing WS.
                if client.send(frame).await.is_err() {
                    tracing::warn!("live steering frame could not be sent; connection failing");
                }
            }
            frame = client.next_frame() => {
                // Any frame (event / approval / terminal / close / error) proves
                // the worker responded — disarm the first-frame watchdog.
                got_first_frame = true;
                first_frame_watch = None;
                match frame {
                    Ok(Some(ChildFrame::Event { event })) => {
                        // AgentEvent is serialized verbatim on the wire (zero mapping).
                        if let Ok(ev) = serde_json::from_value::<AgentEvent>(event) {
                            let _ = event_tx.send(ev).await;
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
                        } else if let Some(host) = escalation_bridge.clone() {
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
                            // Register the pending request BEFORE surfacing it so
                            // the external handler's `deliver_approval_checked` can
                            // correlate an out-of-band POST against a genuine
                            // human-loop request (and consume it one-shot).
                            super::live::register_pending_approval(child_session_id, &id);
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
                                // Deny only if still pending: a one-shot consume so
                                // we don't double-deliver if the human already
                                // answered (the POST took it), and so a late POST
                                // after this fires finds nothing pending.
                                if super::live::take_pending_approval(&child, &id) {
                                    super::live::deliver_approval(&child, &id, false);
                                }
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

        // #73 (P1): the worker bakes `no_human_review` from this flag once at
        // build(), so it MUST split the pool or a worker baked for one approval
        // posture is reused for the opposite one.
        let mut nha = spec_with("explorer", "p", "m", Some("/ws"), None);
        nha.capabilities.no_human_approver = true;
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&nha),
            "no_human_approver must split"
        );

        // #71: the read-only Bash checker is baked once at build() from this flag,
        // so a guardian reviewer worker must not be reused for an ordinary child.
        let mut gro = spec_with("explorer", "p", "m", Some("/ws"), None);
        gro.capabilities.guardian_read_only = true;
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&gro),
            "guardian_read_only must split"
        );
    }

    struct StaticDecider(bool);

    #[async_trait]
    impl ChildApprovalDecider for StaticDecider {
        async fn decide(&self, _child: &str, _req: &serde_json::Value) -> bool {
            self.0
        }
    }

    // ---- first-frame watchdog (dead-pooled-worker recovery) -----------------

    /// A link that never yields a frame — models a worker that died (or never
    /// subscribed) so its Run sits queued with no server.
    struct SilentLink;
    #[async_trait]
    impl bamboo_subagent::ChildLink for SilentLink {
        async fn send(&mut self, _: ParentFrame) -> bamboo_subagent::TransportResult<()> {
            Ok(())
        }
        async fn next_frame(
            &mut self,
        ) -> bamboo_subagent::TransportResult<Option<ChildFrame>> {
            std::future::pending().await
        }
    }

    /// A link that immediately yields one terminal frame (a healthy fast worker).
    struct InstantTerminalLink {
        done: bool,
    }
    #[async_trait]
    impl bamboo_subagent::ChildLink for InstantTerminalLink {
        async fn send(&mut self, _: ParentFrame) -> bamboo_subagent::TransportResult<()> {
            Ok(())
        }
        async fn next_frame(
            &mut self,
        ) -> bamboo_subagent::TransportResult<Option<ChildFrame>> {
            if self.done {
                std::future::pending().await
            } else {
                self.done = true;
                Ok(Some(ChildFrame::Terminal {
                    status: TerminalStatus::Completed,
                    result: Some("done".into()),
                    error: None,
                    transcript: vec![],
                }))
            }
        }
    }

    #[tokio::test]
    async fn drive_trips_first_frame_watchdog_on_a_silent_worker() {
        let (event_tx, _rx) = mpsc::channel::<AgentEvent>(8);
        let cancel = CancellationToken::new();
        let (_live_tx, mut live_rx) = mpsc::unbounded_channel::<ParentFrame>();
        let mut link = SilentLink;
        let r = drive(
            &mut link,
            "child-x",
            None,
            None,
            &event_tx,
            &cancel,
            &mut live_rx,
            Some(Duration::from_millis(100)),
        )
        .await;
        assert!(
            matches!(r, Err(AgentError::WorkerUnresponsive(_))),
            "a silent worker must trip the first-frame watchdog, got {r:?}"
        );
    }

    #[tokio::test]
    async fn drive_does_not_trip_when_a_frame_arrives() {
        let (event_tx, _rx) = mpsc::channel::<AgentEvent>(8);
        let cancel = CancellationToken::new();
        let (_live_tx, mut live_rx) = mpsc::unbounded_channel::<ParentFrame>();
        let mut link = InstantTerminalLink { done: false };
        // Even a tiny timeout must NOT trip: the terminal frame arrives first and
        // disarms the watchdog.
        let r = drive(
            &mut link,
            "child-y",
            None,
            None,
            &event_tx,
            &cancel,
            &mut live_rx,
            Some(Duration::from_millis(50)),
        )
        .await;
        assert_eq!(r.ok().flatten().as_deref(), Some("done"));
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

    // ---- #193: remote placement routing -------------------------------------

    use crate::runtime::execution::SpawnJob;
    use bamboo_agent_core::Session;

    /// A runner with a BOGUS worker_bin (`/bin/false`): a local spawn here would
    /// FAIL, so a passing remote test proves the remote path never spawns.
    fn bogus_runner(placements: HashMap<String, ResolvedRemotePlacement>) -> ActorChildRunner {
        ActorChildRunner::new(
            "test-actor".into(),
            PathBuf::from("/bin/false"),
            vec![],
            std::env::temp_dir().join("bamboo-test-fab-193"),
            ExecutorSpec::Echo,
            vec![],
            "anthropic".into(),
            4,
        )
        .with_remote_placements(placements)
    }

    /// A child session of the given role (the role rides `subagent_type`, the
    /// path build_spec + the remote lookup both read).
    fn session_of_role(role: &str, assignment: &str) -> Session {
        let mut s = Session::new("child-1", "test-model");
        s.metadata
            .insert("subagent_type".to_string(), role.to_string());
        s.add_message(bamboo_agent_core::Message::user(assignment));
        s
    }

    fn job_for(child: &str) -> SpawnJob {
        SpawnJob {
            parent_session_id: "parent-1".into(),
            child_session_id: child.into(),
            model: String::new(),
            disabled_tools: None,
        }
    }

    #[test]
    fn build_spec_sets_remote_placement_for_matching_role() {
        let mut placements = HashMap::new();
        placements.insert(
            "explorer".to_string(),
            ResolvedRemotePlacement {
                endpoint: "wss://gpu-host:8443".into(),
                token: Some("T-secret".into()),
                ca_cert_file: None,
                host_label: None,
            },
        );
        let runner = bogus_runner(placements);

        // Matching role -> Placement::Remote + the bearer on the secrets envelope.
        let s = session_of_role("explorer", "do the thing");
        let spec = runner.build_spec(&s, &job_for("child-1"));
        match &spec.placement {
            Placement::Remote { endpoint } => assert_eq!(endpoint, "wss://gpu-host:8443"),
            other => panic!("expected Remote, got {other:?}"),
        }
        assert_eq!(spec.secrets.worker_auth_token.as_deref(), Some("T-secret"));
    }

    #[test]
    fn build_spec_leaves_local_for_unmatched_role() {
        let mut placements = HashMap::new();
        placements.insert(
            "explorer".to_string(),
            ResolvedRemotePlacement {
                endpoint: "wss://gpu-host:8443".into(),
                token: Some("T".into()),
                ca_cert_file: None,
                host_label: None,
            },
        );
        let runner = bogus_runner(placements);

        // A DIFFERENT role keeps the default Local placement + no bearer.
        let s = session_of_role("writer", "do the thing");
        let spec = runner.build_spec(&s, &job_for("child-1"));
        assert_eq!(spec.placement, Placement::Local);
        assert!(spec.secrets.worker_auth_token.is_none());
    }

    #[test]
    fn build_spec_local_when_no_placements() {
        let runner = bogus_runner(HashMap::new());
        let s = session_of_role("explorer", "do the thing");
        let spec = runner.build_spec(&s, &job_for("child-1"));
        assert_eq!(spec.placement, Placement::Local);
        assert!(spec.secrets.worker_auth_token.is_none());
    }

    #[test]
    fn placement_metadata_stamps_remote_and_schedulable_not_local() {
        // Local children carry no stamp — the DTO defaults them to the backend host.
        assert_eq!(placement_metadata(&Placement::Local, None), None);

        // Remote, no node label → host derived from the endpoint.
        let r = placement_metadata(
            &Placement::Remote {
                endpoint: "wss://10.0.0.5:8443/stream".into(),
            },
            None,
        )
        .unwrap();
        assert!(r.contains(r#""kind":"remote""#), "{r}");
        assert!(r.contains(r#""host":"10.0.0.5""#), "{r}");

        // A cluster node's label (its metadata) OVERRIDES the raw endpoint host.
        let labeled = placement_metadata(
            &Placement::Remote {
                endpoint: "ws://169.254.230.101:8899".into(),
            },
            Some("mini"),
        )
        .unwrap();
        assert!(labeled.contains(r#""host":"mini""#), "{labeled}");

        // Schedulable → {kind:"remote", host:<node label, else pool>}.
        let s =
            placement_metadata(&Placement::Schedulable { pool: "explorers".into() }, Some("mini"))
                .unwrap();
        assert!(s.contains(r#""kind":"remote""#), "{s}");
        assert!(s.contains(r#""host":"mini""#), "{s}");

        // The stamp round-trips through the storage placement type.
        let p: bamboo_storage::SessionPlacement = serde_json::from_str(&labeled).unwrap();
        assert_eq!(p.kind, "remote");
        assert_eq!(p.host, "mini");
    }

    /// End-to-end remote run through `execute_external_child`: a resident worker
    /// (Bearer-gated `WsServer` + `EchoExecutor`) serves the role; the runner is
    /// built with a `remote_placements` entry pointing at it AND a BOGUS
    /// worker_bin (`/bin/false`). A passing test proves the remote path CONNECTS
    /// to the resident worker and NEVER spawns (a spawn would fail on /bin/false),
    /// and that a terminal/echo result flows back.
    #[tokio::test]
    async fn execute_external_child_routes_role_to_remote_worker_without_spawning() {
        // 1. Stand up the resident worker on loopback with a required bearer.
        let token = "remote-test-token";
        let server = bamboo_subagent::transport::WsServer::bind_with_token(
            (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            Some(token.to_string()),
        )
        .await
        .expect("bind resident worker");
        let endpoint = server.ws_endpoint(); // ws://127.0.0.1:<port>
        let srv = tokio::spawn(async move {
            // serve() loops connection-after-connection; the test exits, dropping it.
            let _ = server
                .serve(Arc::new(bamboo_subagent::executor::EchoExecutor))
                .await;
        });

        // 2. Build the runner: role "explorer" pinned remote, bogus worker_bin.
        let mut placements = HashMap::new();
        placements.insert(
            "explorer".to_string(),
            ResolvedRemotePlacement {
                endpoint: endpoint.clone(),
                token: Some(token.to_string()),
                ca_cert_file: None,
                host_label: Some("mini-e2e".into()), // node label, surfaced on the badge
            },
        );
        let runner = bogus_runner(placements);

        // 3. Drive a real run for that role.
        let mut session = session_of_role("explorer", "hello remote");
        let job = job_for("child-1");
        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(64);
        let cancel = CancellationToken::new();

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            runner.execute_external_child(&mut session, &job, event_tx, cancel),
        )
        .await
        .expect("run did not hang")
        .expect("remote run succeeded (connected to resident worker, did not spawn)");

        let _ = result;
        // The EchoExecutor's reply is written back onto the child session as an
        // assistant message — proof a terminal result flowed back over the link.
        let last = session
            .messages
            .iter()
            .rev()
            .find(|m| matches!(m.role, Role::Assistant))
            .expect("an assistant reply was written back");
        assert!(
            last.content.contains("echo:"),
            "expected echo reply, got {:?}",
            last.content
        );

        // A remote run must stamp WHICH machine it ran on onto the child session
        // (mirrored to the UI badge) using the placement's node label.
        let placement = session
            .metadata
            .get("placement")
            .expect("remote child session stamped with a placement");
        assert!(placement.contains(r#""kind":"remote""#), "{placement}");
        assert!(placement.contains(r#""host":"mini-e2e""#), "{placement}");

        // Drain a couple of streamed events to confirm the event pipe carried the
        // worker's tokens too (best-effort; the reply assertion above is primary).
        let mut saw_event = false;
        while let Ok(Some(_ev)) =
            tokio::time::timeout(Duration::from_millis(50), event_rx.recv()).await
        {
            saw_event = true;
        }
        let _ = saw_event;

        srv.abort();
    }

    // ---- #181 (P2b): schedulable placement routing --------------------------


    /// A bogus-worker_bin runner carrying SCHEDULABLE placements (and optionally
    /// remote ones, to test precedence). A local spawn here would fail on
    /// `/bin/false`, so a passing schedulable test proves no subprocess spawned.
    fn bogus_sched_runner(
        remote: HashMap<String, ResolvedRemotePlacement>,
        sched: HashMap<String, ResolvedSchedulablePlacement>,
    ) -> ActorChildRunner {
        ActorChildRunner::new(
            "test-actor".into(),
            PathBuf::from("/bin/false"),
            vec![],
            std::env::temp_dir().join("bamboo-test-fab-181"),
            ExecutorSpec::Echo,
            vec![],
            "anthropic".into(),
            4,
        )
        .with_remote_placements(remote)
        .with_schedulable_placements(sched)
    }

    fn sched_placement(pool: &str, _registry_url: impl Into<String>) -> ResolvedSchedulablePlacement {
        ResolvedSchedulablePlacement { pool: pool.into(), host_label: None }
    }

    #[test]
    fn build_spec_sets_schedulable_placement_for_matching_role() {
        let mut sched = HashMap::new();
        sched.insert(
            "explorer".to_string(),
            sched_placement("gpu-pool", "unused"),
        );
        let runner = bogus_sched_runner(HashMap::new(), sched);

        let s = session_of_role("explorer", "do the thing");
        let spec = runner.build_spec(&s, &job_for("child-1"));
        match &spec.placement {
            Placement::Schedulable { pool } => assert_eq!(pool, "gpu-pool"),
            other => panic!("expected Schedulable, got {other:?}"),
        }
        // No per-placement bearer now — the bus connection carries the bus token.
        assert!(spec.secrets.worker_auth_token.is_none());
    }

    #[test]
    fn build_spec_remote_wins_when_role_in_both_maps() {
        // A role present in BOTH remote_placements and schedulable_placements must
        // resolve to the FIXED remote placement (documented precedence).
        let mut remote = HashMap::new();
        remote.insert(
            "explorer".to_string(),
            ResolvedRemotePlacement {
                endpoint: "wss://fixed-host:8443".into(),
                token: Some("T-remote".into()),
                ca_cert_file: None,
                host_label: None,
            },
        );
        let mut sched = HashMap::new();
        sched.insert(
            "explorer".to_string(),
            sched_placement("gpu-pool", "https://control-plane:9562"),
        );
        let runner = bogus_sched_runner(remote, sched);

        let s = session_of_role("explorer", "do the thing");
        let spec = runner.build_spec(&s, &job_for("child-1"));
        match &spec.placement {
            Placement::Remote { endpoint } => assert_eq!(endpoint, "wss://fixed-host:8443"),
            other => panic!("expected Remote (precedence), got {other:?}"),
        }
        assert_eq!(spec.secrets.worker_auth_token.as_deref(), Some("T-remote"));
    }

    #[test]
    fn build_spec_local_for_unmatched_schedulable_role() {
        let mut sched = HashMap::new();
        sched.insert(
            "explorer".to_string(),
            sched_placement("gpu-pool", "https://control-plane:9562"),
        );
        let runner = bogus_sched_runner(HashMap::new(), sched);
        let s = session_of_role("writer", "do the thing");
        let spec = runner.build_spec(&s, &job_for("child-1"));
        assert_eq!(spec.placement, Placement::Local);
        assert!(spec.secrets.worker_auth_token.is_none());
    }

    /// The full role → resolved-placement → badge-host chain: a child routed to a
    /// remote/schedulable placement carrying a cluster node's `host_label` stamps
    /// that label; without a label it falls back to the endpoint host / pool; a
    /// Local child gets no stamp (the DTO defaults it to the backend host).
    #[test]
    fn placement_stamp_uses_node_label_for_remote_and_schedulable() {
        // Remote WITH a node label → {remote, <label>}, overriding the raw IP.
        let mut remote = HashMap::new();
        remote.insert(
            "explorer".to_string(),
            ResolvedRemotePlacement {
                endpoint: "ws://169.254.230.101:8899".into(),
                token: None,
                ca_cert_file: None,
                host_label: Some("mini".into()),
            },
        );
        let runner = bogus_runner(remote);
        let spec = runner.build_spec(&session_of_role("explorer", "go"), &job_for("c1"));
        let stamp = runner.placement_stamp_for(&spec).expect("remote child is stamped");
        assert!(stamp.contains(r#""kind":"remote""#), "{stamp}");
        assert!(stamp.contains(r#""host":"mini""#), "{stamp}");

        // Remote WITHOUT a node label → falls back to the endpoint host.
        let mut remote_nolabel = HashMap::new();
        remote_nolabel.insert(
            "explorer".to_string(),
            ResolvedRemotePlacement {
                endpoint: "ws://169.254.230.101:8899".into(),
                token: None,
                ca_cert_file: None,
                host_label: None,
            },
        );
        let r2 = bogus_runner(remote_nolabel);
        let spec2 = r2.build_spec(&session_of_role("explorer", "go"), &job_for("c1"));
        assert!(
            r2.placement_stamp_for(&spec2)
                .unwrap()
                .contains(r#""host":"169.254.230.101""#)
        );

        // Schedulable WITH a node label → {remote, <label>} (a node, not a pool name).
        let mut sched = HashMap::new();
        sched.insert(
            "mac-mini-monitor".to_string(),
            ResolvedSchedulablePlacement {
                pool: "mac-mini-monitor".into(),
                host_label: Some("mini".into()),
            },
        );
        let sr = bogus_sched_runner(HashMap::new(), sched);
        let spec3 = sr.build_spec(&session_of_role("mac-mini-monitor", "go"), &job_for("c1"));
        let stamp3 = sr.placement_stamp_for(&spec3).expect("scheduled child is stamped");
        assert!(stamp3.contains(r#""kind":"remote""#), "{stamp3}");
        assert!(stamp3.contains(r#""host":"mini""#), "{stamp3}");

        // A Local (unmatched) child gets NO stamp.
        let local = bogus_runner(HashMap::new());
        let spec4 = local.build_spec(&session_of_role("writer", "go"), &job_for("c1"));
        assert_eq!(local.placement_stamp_for(&spec4), None);
    }

    // ---- #181: schedulable selection over the BUS (Phase 3 cutover) ----------

    async fn start_bus() -> (String, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let core = std::sync::Arc::new(bamboo_broker::BrokerCore::new(dir.path()));
        let server = std::sync::Arc::new(bamboo_broker::BrokerServer::new(core, "t"));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        (format!("ws://{addr}"), dir)
    }

    async fn join_pool(endpoint: &str, id: &str, pool: &str) -> bamboo_broker::BrokerClient {
        let mut c = bamboo_broker::BrokerClient::connect(
            endpoint,
            bamboo_subagent::AgentRef {
                session_id: id.into(),
                role: Some(pool.into()),
            },
            "t",
        )
        .await
        .unwrap();
        c.subscribe().await.unwrap();
        c
    }

    fn sched_runner_on_bus(endpoint: &str, child_role: &str, pool: &str) -> ActorChildRunner {
        let mut sched = HashMap::new();
        sched.insert(child_role.to_string(), sched_placement(pool, "unused"));
        bogus_sched_runner(HashMap::new(), sched).with_bus(Some(bamboo_subagent::BusEndpoint {
            endpoint: endpoint.into(),
            token: "t".into(),
        }))
    }

    #[tokio::test]
    async fn resolve_schedulable_picks_a_live_bus_worker() {
        let (endpoint, _dir) = start_bus().await;
        let _w = join_pool(&endpoint, "w-gpu", "gpu-pool").await;
        let runner = sched_runner_on_bus(&endpoint, "explorer", "gpu-pool");

        let mailbox = runner
            .resolve_schedulable_worker("explorer")
            .await
            .expect("a live pool worker is found on the bus");
        assert_eq!(mailbox, "w-gpu");
    }

    #[tokio::test]
    async fn resolve_schedulable_round_robins_over_pool_workers() {
        let (endpoint, _dir) = start_bus().await;
        let _a = join_pool(&endpoint, "w-a", "gpu-pool").await;
        let _b = join_pool(&endpoint, "w-b", "gpu-pool").await;
        let runner = sched_runner_on_bus(&endpoint, "explorer", "gpu-pool");

        // Successive resolves spread across both connected workers.
        let mut picked = std::collections::HashSet::new();
        for _ in 0..6 {
            picked.insert(runner.resolve_schedulable_worker("explorer").await.unwrap());
        }
        assert_eq!(
            picked,
            ["w-a".to_string(), "w-b".to_string()].into_iter().collect(),
            "round-robin must cover every connected pool worker"
        );
    }

    #[tokio::test]
    async fn resolve_schedulable_errors_on_empty_pool() {
        let (endpoint, _dir) = start_bus().await;
        // No worker subscribes to "gpu-pool".
        let runner = sched_runner_on_bus(&endpoint, "explorer", "gpu-pool");

        let err = runner
            .resolve_schedulable_worker("explorer")
            .await
            .expect_err("an empty pool is terminal — no local fallback")
            .to_string();
        assert!(err.contains("no live worker in pool"), "got: {err}");
        assert!(err.contains("NOT spawning"), "got: {err}");
    }

    /// FULL schedulable run over the bus: a worker SERVING `EchoExecutor` joins the
    /// pool by role; `execute_external_child` with a Schedulable placement resolves
    /// it from the bus (no local subprocess — the worker_bin is `/bin/false`),
    /// drives the run, gets the echo back, AND stamps the child session with the
    /// pool's cluster-node label — `{kind:remote, host:"mini"}`. The end-to-end
    /// analogue of the live `mac-mini-monitor`→mini run.
    #[tokio::test]
    async fn execute_external_child_runs_schedulable_over_bus_and_stamps_node_label() {
        let (endpoint, _dir) = start_bus().await;

        // A bus worker SERVING runs (not just presence), joined to the pool by role.
        let ep = endpoint.clone();
        let worker = tokio::spawn(async move {
            let _ = bamboo_broker::serve_executor(
                &ep,
                bamboo_subagent::AgentRef {
                    session_id: "mmm-worker".into(),
                    role: Some("mac-mini-monitor".into()),
                },
                "t",
                std::sync::Arc::new(bamboo_subagent::executor::EchoExecutor),
            )
            .await;
        });

        // Wait until the worker is visible on the bus so the pool is non-empty
        // when execute_external_child resolves it (serve_executor connects async).
        let mut probe = bamboo_broker::BrokerClient::connect(
            &endpoint,
            bamboo_subagent::AgentRef { session_id: "probe".into(), role: None },
            "t",
        )
        .await
        .unwrap();
        let mut ready = false;
        for _ in 0..100 {
            if probe
                .list_connected("mac-mini-monitor")
                .await
                .unwrap()
                .iter()
                .any(|id| id == "mmm-worker")
            {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        assert!(ready, "worker never joined the pool");

        // Runner: child role → schedulable pool "mac-mini-monitor" carrying the
        // cluster node's label "mini"; bogus worker_bin so any local spawn fails.
        let mut sched = HashMap::new();
        sched.insert(
            "mac-mini-monitor".to_string(),
            ResolvedSchedulablePlacement {
                pool: "mac-mini-monitor".into(),
                host_label: Some("mini".into()),
            },
        );
        let runner = bogus_sched_runner(HashMap::new(), sched).with_bus(Some(
            bamboo_subagent::BusEndpoint { endpoint: endpoint.clone(), token: "t".into() },
        ));

        let mut session = session_of_role("mac-mini-monitor", "hello scheduled");
        let job = job_for("child-1");
        let (event_tx, _rx) = mpsc::channel::<AgentEvent>(64);
        let cancel = CancellationToken::new();

        tokio::time::timeout(
            Duration::from_secs(10),
            runner.execute_external_child(&mut session, &job, event_tx, cancel),
        )
        .await
        .expect("run did not hang")
        .expect("schedulable run succeeded over the bus (no local spawn)");

        // Echo reply flowed back — proves it routed to the bus worker, not local.
        let last = session
            .messages
            .iter()
            .rev()
            .find(|m| matches!(m.role, Role::Assistant))
            .expect("an assistant reply was written back");
        assert!(last.content.contains("echo:"), "got {:?}", last.content);

        // ...and the child is stamped with the pool's cluster-node label.
        let placement = session
            .metadata
            .get("placement")
            .expect("scheduled child session stamped with a placement");
        assert!(placement.contains(r#""kind":"remote""#), "{placement}");
        assert!(placement.contains(r#""host":"mini""#), "{placement}");

        worker.abort();
    }
}
