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

use bamboo_subagent::discovery::{Discovery, Fabric};
use bamboo_subagent::fleet::{spawn_worker, SpawnedChild};
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

/// A warm worker parked for reuse: its process handle (killed on drop), the WS
/// endpoint to reconnect to, and the id it registered under in the fabric.
struct PooledActor {
    worker: SpawnedChild,
    endpoint: String,
    agent_id: String,
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
}

/// A role routed to a registry-SCHEDULED worker (remote-actor-plan §3.4 / P2b,
/// #181), resolved at runner-build time from `SubagentsConfig.schedulable_placements`.
/// Unlike a fixed `ResolvedRemotePlacement` (one endpoint), this names the agent
/// `registry_url` to query and the logical `pool` (= the registry `role`) whose
/// LIVE workers are scheduling candidates. The env-named bearer is already READ
/// into `token` here (the raw token never rides the config) and is used for BOTH
/// the registry query and the chosen worker's `wss://` connect. `ca_cert_file`
/// pins a self-signed worker/registry cert (`None` ⇒ default webpki roots).
#[derive(Debug, Clone)]
pub struct ResolvedSchedulablePlacement {
    pub pool: String,
    pub registry_url: String,
    pub token: Option<String>,
    pub ca_cert_file: Option<PathBuf>,
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
    /// Per-`(registry_url, token)` cache of built [`RegistryFabric`]s (#202). A fabric
    /// holds a reqwest `Client` (already connection-pooled), so under sibling
    /// fan-out we construct ONE fabric per registry and reuse it across schedules
    /// instead of rebuilding a fresh client every spawn. Construct-once-then-reuse;
    /// a benign duplicate build under a startup race is harmless (last writer wins,
    /// the loser's fabric drops). The bearer lives INSIDE the fabric's sensitive
    /// `Authorization` header — never logged, never re-stringified here.
    // Keyed by (registry_url, token) — NOT url alone — so two placements that
    // share a registry but present DIFFERENT bearers never reuse each other's
    // fabric (which would issue a discover query with the wrong token). #202.
    fabric_cache: Arc<
        std::sync::Mutex<HashMap<(String, Option<String>), Arc<bamboo_subagent::RegistryFabric>>>,
    >,
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
            concurrency: std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent.max(1))),
            spawn_timeout: Duration::from_secs(30),
            pool: Arc::new(Mutex::new(HashMap::new())),
            max_idle_per_key: DEFAULT_MAX_IDLE_PER_KEY,
            approval_decider: None,
            escalation_bridge: Arc::new(std::sync::Mutex::new(None)),
            remote_placements: HashMap::new(),
            schedulable_placements: HashMap::new(),
            schedule_cursor: Arc::new(std::sync::Mutex::new(HashMap::new())),
            fabric_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
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
            // #181 (P2b): route this role to a registry-SCHEDULED worker — ONLY
            // when it is NOT already pinned to a fixed remote endpoint (the
            // `else if` makes remote_placements take precedence for a role in
            // both). The concrete endpoint is resolved at run time in
            // `execute_external_child`; here we only flip the placement to
            // Schedulable{pool} and ride the bearer on the scoped secrets
            // envelope (used for the registry query AND the worker connect). No
            // match in either map leaves the default `Placement::Local`, so the
            // local path is byte-for-byte unchanged for every non-routed role.
            spec.placement = Placement::Schedulable {
                pool: placement.pool.clone(),
            };
            spec.secrets.worker_auth_token = placement.token.clone();
        }
        spec
    }

    /// Max DISTINCT live candidates a single schedulable resolve will try to
    /// connect to before giving up (#202). Bounds the connect-fail failover so a
    /// pool full of stale-but-leased workers can't make one spawn walk the whole
    /// fleet; the effective cap is `min(candidates.len(), MAX_SCHEDULE_CONNECT_ATTEMPTS)`.
    const MAX_SCHEDULE_CONNECT_ATTEMPTS: usize = 3;

    /// Get-or-build the [`RegistryFabric`] for `placement.registry_url`, caching
    /// it on the runner (#202). A fabric wraps a connection-pooled reqwest
    /// `Client`; under sibling fan-out this reuses ONE client per registry instead
    /// of constructing a fresh one every schedule. Construct-once-then-reuse: a
    /// cache miss builds (Bearer if configured) and stores it. The bearer stays
    /// inside the fabric's sensitive `Authorization` header — never logged here.
    ///
    /// Concurrency: a brief race can build two fabrics for the same url; that is
    /// harmless (both are valid, last-insert wins, the loser drops). We never hold
    /// the cache lock across the build, so an `await`-free critical section can't
    /// deadlock or stall siblings.
    fn fabric_for(
        &self,
        placement: &ResolvedSchedulablePlacement,
        role: &str,
    ) -> std::result::Result<Arc<bamboo_subagent::RegistryFabric>, AgentError> {
        // (registry_url, token) identity: a wrong-token fabric must never be
        // reused for a discover query (#202).
        let key = (placement.registry_url.clone(), placement.token.clone());
        if let Some(fabric) = self.fabric_cache.lock().unwrap().get(&key).cloned() {
            return Ok(fabric);
        }

        // Cache miss: build outside the lock (construction is sync + cheap but we
        // keep the critical section minimal and await-free regardless).
        let built = match placement.token.as_deref() {
            Some(token) => {
                bamboo_subagent::RegistryFabric::with_token(placement.registry_url.clone(), token)
            }
            None => bamboo_subagent::RegistryFabric::new(placement.registry_url.clone()),
        }
        .map_err(|e| {
            AgentError::LLM(format!(
                "schedulable role '{role}': registry client for '{}' failed: {e}",
                placement.registry_url
            ))
        })?;
        let arc = Arc::new(built);

        // Insert-or-reuse: if a concurrent caller already populated the slot, take
        // theirs (the duplicate we built drops) so all siblings share one fabric.
        let mut cache = self.fabric_cache.lock().unwrap();
        Ok(cache.entry(key).or_insert(arc).clone())
    }

    /// Pick a live worker for a SCHEDULABLE role from the agent registry (#181,
    /// P2b) and CONNECT to it, with connect-fail failover (#202).
    ///
    /// Uses a cached [`RegistryFabric`] (per `registry_url`) to list live records
    /// (the registry already excludes expired leases — health == a live lease),
    /// filters to those whose `role` == the configured `pool`, then picks a
    /// starting candidate via per-pool ROUND-ROBIN. Because a live lease is only a
    /// COARSE health signal (a leased worker can have a dead process / a network
    /// blip), it does not connect blindly to that one pick: it attempts
    /// `connect_with_auth_tls`, and on failure WALKS FORWARD to the next DISTINCT
    /// live candidate and retries — up to `min(candidates.len(),
    /// MAX_SCHEDULE_CONNECT_ATTEMPTS)` attempts. A single stale-but-leased worker
    /// therefore no longer fails the whole schedule.
    ///
    /// Returns the connected [`ChildClient`] plus the chosen [`AgentRecord`] and
    /// the placement. No live candidate (empty pool) ⇒ a terminal `AgentError`;
    /// ALL attempted candidates failing to connect ⇒ a terminal `AgentError` —
    /// the caller NEVER falls back to a local subprocess (that would silently
    /// defeat the placement). A registry-query failure is likewise terminal. The
    /// error logs HOW MANY candidates were tried, never the token.
    async fn resolve_schedulable_worker(
        &self,
        role: &str,
    ) -> std::result::Result<(ChildClient, AgentRecord, ResolvedSchedulablePlacement), AgentError>
    {
        let placement = self
            .schedulable_placements
            .get(role)
            .ok_or_else(|| {
                AgentError::LLM(format!(
                    "schedulable placement for role '{role}' vanished before scheduling"
                ))
            })?
            .clone();

        // Query the registry through the cached, connection-pooled fabric (#202).
        let fabric = self.fabric_for(&placement, role)?;
        let records = fabric.discover().await.map_err(|e| {
            AgentError::LLM(format!(
                "schedulable role '{role}': registry '{}' query failed: {e}",
                placement.registry_url
            ))
        })?;

        // Live candidates = records published under the pool's role. The registry
        // already dropped expired leases, so presence == health.
        let candidates: Vec<AgentRecord> = records
            .into_iter()
            .filter(|r| r.role == placement.pool)
            .collect();

        if candidates.is_empty() {
            return Err(AgentError::LLM(format!(
                "schedulable role '{role}': no live worker in pool '{}' at registry '{}' \
                 (NOT spawning a local subprocess — a schedulable role has no local fallback)",
                placement.pool, placement.registry_url
            )));
        }

        // Round-robin START: advance a per-pool cursor ONCE to pick the starting
        // index, then walk forward through the remaining candidates on connect
        // failure. Bumping the cursor only once per resolve keeps the spread
        // across sibling spawns (each resolve starts one slot further on); the
        // failover walk is local to this resolve and does not perturb the cursor.
        let start = {
            let mut cursors = self.schedule_cursor.lock().unwrap();
            let cursor = cursors.entry(placement.pool.clone()).or_insert(0);
            let i = *cursor % candidates.len();
            *cursor = cursor.wrapping_add(1);
            i
        };

        // Derive the TLS trust ONCE (pinned CA or default roots / plaintext ws://).
        let trust_cfg = match placement.ca_cert_file.as_deref() {
            Some(path) => Some(client_config_trusting_cert(path).map_err(|e| {
                AgentError::LLM(format!(
                    "scheduled worker CA cert '{}': {e}",
                    path.display()
                ))
            })?),
            None => None,
        };

        // Connect-fail failover: try the starting candidate, then walk forward to
        // the NEXT DISTINCT live candidate on failure, up to the bounded cap. Each
        // attempt hits a different candidate (modulo wrap from the start index).
        let max_attempts = candidates.len().min(Self::MAX_SCHEDULE_CONNECT_ATTEMPTS);
        let mut last_err: Option<String> = None;
        for attempt in 0..max_attempts {
            let idx = (start + attempt) % candidates.len();
            let record = &candidates[idx];
            let endpoint = record.endpoint.clone();
            match ChildClient::connect_with_auth_tls(
                &endpoint,
                placement.token.as_deref(),
                trust_cfg.clone(),
            )
            .await
            {
                Ok(client) => {
                    if attempt > 0 {
                        // We skipped one or more stale-but-leased workers — record
                        // how many (never the endpoint contents beyond the host we
                        // connected to, never the token).
                        tracing::info!(
                            "schedulable role '{role}': connected to pool '{}' worker after \
                             {attempt} stale candidate(s) skipped",
                            placement.pool
                        );
                    }
                    return Ok((client, candidates[idx].clone(), placement));
                }
                Err(e) => {
                    tracing::warn!(
                        "schedulable role '{role}': pool '{}' candidate connect failed \
                         (attempt {}/{max_attempts}): {e}",
                        placement.pool,
                        attempt + 1
                    );
                    last_err = Some(e.to_string());
                }
            }
        }

        // All attempted candidates failed to connect ⇒ terminal error. NO spawn
        // fallback (a schedulable role has none). Log how many were tried, not the
        // token.
        Err(AgentError::LLM(format!(
            "schedulable role '{role}': all {max_attempts} live candidate(s) in pool '{}' at \
             registry '{}' failed to connect (last error: {}) — NOT spawning a local subprocess",
            placement.pool,
            placement.registry_url,
            last_err.as_deref().unwrap_or("unknown")
        )))
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
        *self.escalation_bridge.lock().unwrap() = bridge;
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
        let escalation = self.escalation_bridge.lock().unwrap().clone();
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
                let actor = PooledActor {
                    worker: SpawnedChild::remote(record),
                    endpoint,
                    agent_id: job.child_session_id.clone(),
                };
                (actor, client)
            }
            PlacementKind::Schedulable => {
                // SCHEDULABLE branch (#181, P2b + #202): resolve a LIVE worker from
                // the registry (round-robin over the pool's live records) AND
                // connect to it, with connect-fail failover — on a stale-but-leased
                // worker, resolve walks to the next live candidate (bounded). No
                // spawn, no pool, no kill, NO local fallback — no live worker, or
                // ALL candidates dead ⇒ a terminal error (raised inside
                // resolve_schedulable_worker, which owns the connect now).
                let (client, record, _placement) = self
                    .resolve_schedulable_worker(spec.identity.role.as_str())
                    .await?;
                let endpoint = record.endpoint.clone();
                // The chosen registry record IS the AgentRecord — synthesize the
                // process-less handle straight from it (registry-managed worker).
                let actor = PooledActor {
                    worker: SpawnedChild::remote(record),
                    endpoint,
                    agent_id: job.child_session_id.clone(),
                };
                (actor, client)
            }
            PlacementKind::Local => {
                // LOCAL branch — the EXACT pre-#193 path: reuse-or-spawn + the
                // respawn-on-connect-miss fallback. Unchanged in behavior, ordering,
                // and error text.
                let mut actor = self.acquire_worker(&pool_key, &spec).await?;
                let client = match ChildClient::connect(&actor.endpoint).await {
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
                (actor, client)
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

        let result = drive(
            &mut client,
            &job.child_session_id,
            self.approval_decider.as_ref(),
            escalation,
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
        if remote {
            // #193 / #181: we do NOT own a remote or scheduled worker — never park
            // it into the local pool (its endpoint/agent_id are not ours to
            // recycle) and never kill it (`SpawnedChild::remote.kill()` is a no-op
            // anyway). Just let the connection drop above; the resident /
            // registry-managed worker self-manages via its own idle timeout, ready
            // for the next parent. `actor` is dropped here. (Schedulable shares this
            // arm — release is a no-op for both registry-managed cases.)
            drop(actor);
        } else {
            match &result {
                Ok(_) => self.release_worker(&pool_key, actor).await,
                Err(_) => self.retire_worker(actor).await,
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
                ca_cert_file: None, // plaintext ws:// loopback, default trust
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

    use wiremock::matchers::{method as wm_method, path as wm_path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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

    fn sched_placement(
        pool: &str,
        registry_url: impl Into<String>,
    ) -> ResolvedSchedulablePlacement {
        ResolvedSchedulablePlacement {
            pool: pool.into(),
            registry_url: registry_url.into(),
            token: None,
            ca_cert_file: None,
        }
    }

    fn live_record(agent_id: &str, role: &str, endpoint: &str) -> AgentRecord {
        AgentRecord {
            agent_id: agent_id.into(),
            role: role.into(),
            labels: Vec::new(),
            endpoint: endpoint.into(),
            pid: 0,
            version: String::new(),
            started_at: chrono::Utc::now(),
            lease_expires_at: chrono::Utc::now() + chrono::Duration::seconds(60),
        }
    }

    #[test]
    fn build_spec_sets_schedulable_placement_for_matching_role() {
        let mut sched = HashMap::new();
        sched.insert("explorer".to_string(), {
            let mut p = sched_placement("gpu-pool", "https://control-plane:9562");
            p.token = Some("T-sched".into());
            p
        });
        let runner = bogus_sched_runner(HashMap::new(), sched);

        let s = session_of_role("explorer", "do the thing");
        let spec = runner.build_spec(&s, &job_for("child-1"));
        match &spec.placement {
            Placement::Schedulable { pool } => assert_eq!(pool, "gpu-pool"),
            other => panic!("expected Schedulable, got {other:?}"),
        }
        // The bearer rides the scoped secrets envelope (registry + worker connect).
        assert_eq!(spec.secrets.worker_auth_token.as_deref(), Some("T-sched"));
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

    /// Spin up `n` loopback Echo `WsServer`s; returns their endpoints and the
    /// JoinHandles (abort on test end). Each is a REAL connectable worker.
    async fn spawn_echo_workers(n: usize) -> (Vec<String>, Vec<tokio::task::JoinHandle<()>>) {
        let mut endpoints = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..n {
            let server = bamboo_subagent::transport::WsServer::bind_loopback()
                .await
                .expect("bind echo worker");
            endpoints.push(server.ws_endpoint());
            handles.push(tokio::spawn(async move {
                let _ = server
                    .serve(Arc::new(bamboo_subagent::executor::EchoExecutor))
                    .await;
            }));
        }
        (endpoints, handles)
    }

    #[tokio::test]
    async fn resolve_schedulable_worker_round_robin_spreads_over_candidates() {
        // Registry returns three live, CONNECTABLE workers in the pool; successive
        // picks must advance the per-pool cursor and cover all three (round-robin
        // spread). resolve now also connects, so the candidates are real servers.
        let (eps, handles) = spawn_echo_workers(3).await;
        let registry = MockServer::start().await;
        let recs = vec![
            live_record("w-0", "gpu-pool", &eps[0]),
            live_record("w-1", "gpu-pool", &eps[1]),
            live_record("w-2", "gpu-pool", &eps[2]),
            // A worker in a DIFFERENT pool must be filtered out.
            live_record("other", "cpu-pool", "ws://127.0.0.1:9"),
        ];
        Mock::given(wm_method("GET"))
            .and(wm_path("/v1/agents"))
            .respond_with(ResponseTemplate::new(200).set_body_json(recs))
            .mount(&registry)
            .await;

        let mut sched = HashMap::new();
        sched.insert(
            "explorer".to_string(),
            sched_placement("gpu-pool", registry.uri()),
        );
        let runner = bogus_sched_runner(HashMap::new(), sched);

        // Three picks → cursor 0,1,2 → agent_ids w-0, w-1, w-2 in order.
        let mut picked = Vec::new();
        for _ in 0..3 {
            let (client, rec, placement) = match runner.resolve_schedulable_worker("explorer").await
            {
                Ok(v) => v,
                Err(e) => panic!("a live worker is picked: {e}"),
            };
            assert_eq!(placement.pool, "gpu-pool");
            assert_eq!(rec.role, "gpu-pool", "only pool workers are candidates");
            picked.push(rec.agent_id);
            let _ = client.close().await;
        }
        picked.sort();
        assert_eq!(
            picked,
            vec!["w-0".to_string(), "w-1".to_string(), "w-2".to_string()],
            "round-robin covered every candidate over three picks"
        );
        for h in handles {
            h.abort();
        }
    }

    #[tokio::test]
    async fn resolve_schedulable_worker_fails_over_dead_first_candidate() {
        // The FIRST candidate (by cursor order: cursor starts at 0) points at a
        // DEAD endpoint (no listener on a closed port); a LATER candidate is a real
        // Echo worker. resolve must SKIP the dead one and connect to the live one,
        // returning the live record — connect-fail failover (#202).
        let (eps, handles) = spawn_echo_workers(1).await;
        // A port with no listener: bind then drop to free it (best-effort closed).
        let dead = {
            let l = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .unwrap();
            let port = l.local_addr().unwrap().port();
            drop(l);
            format!("ws://127.0.0.1:{port}")
        };
        let registry = MockServer::start().await;
        let recs = vec![
            // idx 0 = cursor start = DEAD
            live_record("w-dead", "gpu-pool", &dead),
            // idx 1 = LIVE echo worker
            live_record("w-live", "gpu-pool", &eps[0]),
        ];
        Mock::given(wm_method("GET"))
            .and(wm_path("/v1/agents"))
            .respond_with(ResponseTemplate::new(200).set_body_json(recs))
            .mount(&registry)
            .await;

        let mut sched = HashMap::new();
        sched.insert(
            "explorer".to_string(),
            sched_placement("gpu-pool", registry.uri()),
        );
        let runner = bogus_sched_runner(HashMap::new(), sched);

        let (client, rec, _placement) = match runner.resolve_schedulable_worker("explorer").await {
            Ok(v) => v,
            Err(e) => panic!("failover skips the dead candidate, connects to the live one: {e}"),
        };
        assert_eq!(
            rec.agent_id, "w-live",
            "the dead first candidate was skipped; the live one was chosen"
        );
        let _ = client.close().await;
        for h in handles {
            h.abort();
        }
    }

    #[tokio::test]
    async fn resolve_schedulable_worker_errors_when_all_candidates_dead() {
        // ALL candidates point at dead endpoints: resolve must error (no panic),
        // and CRUCIALLY no spawn (a schedulable role has no local fallback). The
        // error names how many were tried.
        let mut dead = Vec::new();
        for _ in 0..2 {
            let l = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .unwrap();
            let port = l.local_addr().unwrap().port();
            drop(l);
            dead.push(format!("ws://127.0.0.1:{port}"));
        }
        let registry = MockServer::start().await;
        let recs = vec![
            live_record("d-0", "gpu-pool", &dead[0]),
            live_record("d-1", "gpu-pool", &dead[1]),
        ];
        Mock::given(wm_method("GET"))
            .and(wm_path("/v1/agents"))
            .respond_with(ResponseTemplate::new(200).set_body_json(recs))
            .mount(&registry)
            .await;

        let mut sched = HashMap::new();
        sched.insert(
            "explorer".to_string(),
            sched_placement("gpu-pool", registry.uri()),
        );
        let runner = bogus_sched_runner(HashMap::new(), sched);

        let msg = match runner.resolve_schedulable_worker("explorer").await {
            Ok(_) => panic!("all candidates dead must error, not connect"),
            Err(e) => e.to_string(),
        };
        assert!(
            msg.contains("failed to connect"),
            "names the connect failure: {msg}"
        );
        assert!(msg.contains("gpu-pool"), "names the pool: {msg}");
        assert!(
            msg.contains("NOT spawning"),
            "confirms no local-subprocess fallback: {msg}"
        );
    }

    #[tokio::test]
    async fn fabric_cache_reuses_one_fabric_per_registry_url() {
        // Two resolves to the SAME registry_url must REUSE one cached fabric (#202):
        // after N calls the cache has exactly one entry for that url.
        let (eps, handles) = spawn_echo_workers(1).await;
        let registry = MockServer::start().await;
        Mock::given(wm_method("GET"))
            .and(wm_path("/v1/agents"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(vec![live_record("w-0", "gpu-pool", &eps[0])]),
            )
            .mount(&registry)
            .await;

        let mut sched = HashMap::new();
        sched.insert(
            "explorer".to_string(),
            sched_placement("gpu-pool", registry.uri()),
        );
        let runner = bogus_sched_runner(HashMap::new(), sched);

        for _ in 0..3 {
            let (client, _rec, _p) = match runner.resolve_schedulable_worker("explorer").await {
                Ok(v) => v,
                Err(e) => panic!("resolve succeeds: {e}"),
            };
            let _ = client.close().await;
        }

        let cache = runner.fabric_cache.lock().unwrap();
        assert_eq!(
            cache.len(),
            1,
            "three resolves to the same (registry_url, token) reused ONE cached fabric"
        );
        assert!(cache.contains_key(&(registry.uri(), None)));
        drop(cache);
        for h in handles {
            h.abort();
        }
    }

    #[test]
    fn fabric_cache_keys_on_registry_url_and_token() {
        // Two placements sharing a registry_url but presenting DIFFERENT tokens
        // must NOT share a fabric — a reused wrong-bearer fabric would issue the
        // discover query with the other role's token (#202 review F1).
        let runner = bogus_sched_runner(HashMap::new(), HashMap::new());
        let url = "http://127.0.0.1:9/".to_string();
        let mut p = sched_placement("pool", url.clone());

        p.token = Some("token-a".into());
        let fa = runner.fabric_for(&p, "role-a").expect("build a");
        p.token = Some("token-b".into());
        let fb = runner.fabric_for(&p, "role-b").expect("build b");
        // Same url, different token → two distinct cache entries (not aliased).
        assert!(
            !Arc::ptr_eq(&fa, &fb),
            "different tokens must not share a fabric"
        );
        assert_eq!(runner.fabric_cache.lock().unwrap().len(), 2);
        // Re-requesting token-a reuses the first fabric.
        p.token = Some("token-a".into());
        let fa2 = runner.fabric_for(&p, "role-a").expect("reuse a");
        assert!(Arc::ptr_eq(&fa, &fa2), "same (url, token) must reuse");
        assert_eq!(runner.fabric_cache.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn resolve_schedulable_worker_errors_with_no_live_worker() {
        // The pool has no live workers (registry returns an empty / off-pool set):
        // a clear error, and CRUCIALLY no spawn (a schedulable role has no local
        // fallback).
        let registry = MockServer::start().await;
        Mock::given(wm_method("GET"))
            .and(wm_path("/v1/agents"))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![live_record(
                "x",
                "cpu-pool",
                "ws://127.0.0.1:9",
            )]))
            .mount(&registry)
            .await;
        let mut sched = HashMap::new();
        sched.insert(
            "explorer".to_string(),
            sched_placement("gpu-pool", registry.uri()),
        );
        let runner = bogus_sched_runner(HashMap::new(), sched);

        let msg = match runner.resolve_schedulable_worker("explorer").await {
            Ok(_) => panic!("no live worker in pool must error, not connect"),
            Err(e) => e.to_string(),
        };
        assert!(msg.contains("no live worker"), "clear error: {msg}");
        assert!(msg.contains("gpu-pool"), "names the pool: {msg}");
    }

    /// Full schedulable e2e: a resident `WsServer` Echo worker is registered (via a
    /// wiremock registry) into pool "gpu-pool"; the runner is configured with a
    /// schedulable placement for role "explorer" → that pool, plus a BOGUS
    /// worker_bin (`/bin/false`). A passing run proves the SCHEDULABLE path RESOLVES
    /// the worker from the registry and connects to it WITHOUT ever spawning a local
    /// subprocess (a spawn would fail on /bin/false), and that an echo result flows
    /// back.
    #[tokio::test]
    async fn execute_external_child_schedules_role_from_registry_without_spawning() {
        // 1. Stand up the resident Echo worker on loopback (no bearer needed here).
        let server = bamboo_subagent::transport::WsServer::bind_loopback()
            .await
            .expect("bind resident worker");
        let endpoint = server.ws_endpoint(); // ws://127.0.0.1:<port>
        let srv = tokio::spawn(async move {
            let _ = server
                .serve(Arc::new(bamboo_subagent::executor::EchoExecutor))
                .await;
        });

        // 2. Registry: returns the live worker registered into pool "gpu-pool"
        //    pointing at the real worker endpoint.
        let registry = MockServer::start().await;
        Mock::given(wm_method("GET"))
            .and(wm_path("/v1/agents"))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![live_record(
                "live-explorer",
                "gpu-pool",
                &endpoint,
            )]))
            .mount(&registry)
            .await;

        // 3. Runner: role "explorer" → schedulable on "gpu-pool"; bogus worker_bin.
        let mut sched = HashMap::new();
        sched.insert(
            "explorer".to_string(),
            sched_placement("gpu-pool", registry.uri()),
        );
        let runner = bogus_sched_runner(HashMap::new(), sched);

        // 4. Drive a real run for that role.
        let mut session = session_of_role("explorer", "hello scheduled");
        let job = job_for("child-1");
        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(64);
        let cancel = CancellationToken::new();

        tokio::time::timeout(
            Duration::from_secs(10),
            runner.execute_external_child(&mut session, &job, event_tx, cancel),
        )
        .await
        .expect("run did not hang")
        .expect("scheduled run succeeded (resolved from registry, did not spawn)");

        // The EchoExecutor's reply is written back — proof a terminal flowed back.
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

        // Best-effort: confirm streamed events also flowed.
        let mut saw_event = false;
        while let Ok(Some(_ev)) =
            tokio::time::timeout(Duration::from_millis(50), event_rx.recv()).await
        {
            saw_event = true;
        }
        let _ = saw_event;

        srv.abort();
    }
}
