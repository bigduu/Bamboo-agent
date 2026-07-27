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

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bamboo_agent_core::{AgentError, AgentEvent, Role, Session};
use bamboo_domain::poison::PoisonRecover;
use bamboo_domain::SessionInboxClaim;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_subagent::fleet::{spawn_worker_on_bus, SpawnedChild};
use bamboo_subagent::proto::{
    AgentRecord, ChildFrame, LogicalSessionIdentity, ParentFrame, PermissionPolicyContext, RunSpec,
    SessionMessageDelivery, TerminalStatus,
};
use bamboo_subagent::provision::{
    ChildIdentity, ExecutorSpec, ModelRefSpec, Placement, ProvisionSpec, ScopedCredential,
};
use bamboo_subagent::transport::{client_config_trusting_cert, ChildClient};

use crate::runtime::execution::{ExternalChildRunner, SessionInboxRuntimeBinding, SpawnJob};

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

/// Plaintext token returned once by the server authority for one Codex run.
/// `token_id` is non-secret and is the handle used for guaranteed revocation.
pub struct IssuedCodexRunToken {
    pub token_id: String,
    pub token: String,
}

/// Server-owned authority for Bamboo-as-provider Codex credentials. The engine
/// only needs mint/revoke; verification remains inside the HTTP server.
pub trait CodexRunTokenAuthority: Send + Sync + 'static {
    fn issue(&self, session_id: &str) -> Result<IssuedCodexRunToken, String>;
    fn revoke(&self, token_id: &str);
}

struct CodexRunTokenGuard {
    authority: Arc<dyn CodexRunTokenAuthority>,
    token_id: String,
}

impl Drop for CodexRunTokenGuard {
    fn drop(&mut self) {
        self.authority.revoke(&self.token_id);
    }
}

fn executor_uses_bamboo_codex(executor: &ExecutorSpec) -> bool {
    matches!(
        executor,
        ExecutorSpec::Codex {
            auth_mode: Some(mode),
            ..
        } if mode == "bamboo"
    ) || matches!(
        executor,
        ExecutorSpec::Codex {
            auth_mode: None,
            inherit_user_config,
            ..
        } if !inherit_user_config.unwrap_or(false)
    )
}

fn workspace_is_bamboo_owned(raw: &str) -> bool {
    let workspace = std::fs::canonicalize(raw).unwrap_or_else(|_| PathBuf::from(raw));
    let configured_root = bamboo_config::paths::resolve_workspace_root();
    let configured_root = std::fs::canonicalize(&configured_root).unwrap_or(configured_root);
    if workspace.starts_with(&configured_root) {
        return true;
    }

    // Project worktrees created by Bamboo live under
    // `<project>/.bamboo/worktree/<name>` and carry the ownership marker used
    // by the project-worktree lifecycle. A path that merely imitates the
    // directory shape is not sufficient to bypass Codex's git guard.
    workspace.ancestors().any(|candidate| {
        let Some(name) = candidate.file_name().and_then(|name| name.to_str()) else {
            return false;
        };
        let Some(worktree_root) = candidate.parent() else {
            return false;
        };
        if worktree_root.file_name() != Some(std::ffi::OsStr::new("worktree"))
            || worktree_root.parent().and_then(Path::file_name)
                != Some(std::ffi::OsStr::new(".bamboo"))
        {
            return false;
        }
        let marker = worktree_root.join(".bamboo-owned").join(name);
        std::fs::read_to_string(marker).is_ok_and(|branch| branch == format!("bamboo/{name}"))
    })
}

fn build_codex_run_secrets(
    executor: &ExecutorSpec,
    authority: Option<Arc<dyn CodexRunTokenAuthority>>,
    child_session_id: &str,
) -> Result<
    (
        bamboo_subagent::proto::RunSecrets,
        Option<CodexRunTokenGuard>,
    ),
    AgentError,
> {
    if !executor_uses_bamboo_codex(executor) {
        return Ok((bamboo_subagent::proto::RunSecrets::default(), None));
    }

    let authority = authority.ok_or_else(|| {
        AgentError::LLM(
            "Codex auth mode 'bamboo' requires the server per-run token authority".to_string(),
        )
    })?;
    let issued = authority
        .issue(child_session_id)
        .map_err(|error| AgentError::LLM(format!("mint Codex per-run provider token: {error}")))?;
    let guard = CodexRunTokenGuard {
        authority,
        token_id: issued.token_id,
    };
    Ok((
        bamboo_subagent::proto::RunSecrets {
            codex_provider_token: Some(bamboo_subagent::proto::SecretValue::new(issued.token)),
        },
        Some(guard),
    ))
}

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
    approval_registry: Option<super::approval_registry::SharedApprovalRegistry>,
    permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
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
    /// Off-loop parent-agent reviewer for forced-ask requests. The root server
    /// wires a session-aware reviewer; nested workers wire their owning model
    /// reviewer directly into the per-run runner.
    approval_reviewer: Option<Arc<dyn ChildApprovalReviewer>>,
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
    /// Optional server authority used only by `Codex` in `bamboo` auth mode.
    codex_run_tokens: Option<Arc<dyn CodexRunTokenAuthority>>,
    /// Canonical logical-session inbox resources, late-bound by each owning
    /// runtime. Kept per runner/runtime; never process-global.
    session_inbox_runtime: Arc<std::sync::Mutex<Option<SessionInboxRuntimeBinding>>>,
}

/// Decides how the host answers a child worker's gated-tool approval request
/// (Phase 2: child → parent approval delegation). Async so an implementation
/// can consult a policy. With no decider wired the host replies with a
/// fail-closed DENY.
///
/// NOTE: `decide` is awaited inside the per-child frame pump, so an
/// implementation must resolve promptly (e.g. a policy lookup). Model-based
/// review belongs in [`ChildApprovalReviewer`], which runs off-loop and returns
/// through the live steering channel without stalling the frame pump.
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

/// How long a chained parent-agent review may take before the child's gated
/// tool fails closed (DENY). Bounds an unanswered request so it cannot hang the
/// worker indefinitely.
const CHILD_APPROVAL_TIMEOUT: Duration = Duration::from_secs(300);

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
    async fn review(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
        request: &serde_json::Value,
    ) -> bool;
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
            approval_registry: None,
            permission_config: None,
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
            approval_reviewer: None,
            escalation_bridge: Arc::new(std::sync::Mutex::new(None)),
            remote_placements: HashMap::new(),
            schedulable_placements: HashMap::new(),
            schedule_cursor: Arc::new(std::sync::Mutex::new(HashMap::new())),
            codex_run_tokens: None,
            session_inbox_runtime: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub fn with_approval_registry(
        mut self,
        registry: super::approval_registry::SharedApprovalRegistry,
    ) -> Self {
        self.approval_registry = Some(registry);
        self
    }

    pub fn with_permission_config(
        mut self,
        config: Arc<bamboo_tools::permission::PermissionConfig>,
    ) -> Self {
        self.permission_config = Some(config);
        self
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

    pub fn with_approval_reviewer(mut self, reviewer: Arc<dyn ChildApprovalReviewer>) -> Self {
        self.approval_reviewer = Some(reviewer);
        self
    }

    pub fn with_codex_run_tokens(
        mut self,
        authority: Option<Arc<dyn CodexRunTokenAuthority>>,
    ) -> Self {
        self.codex_run_tokens = authority;
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
        // The worker constructs its executor exactly once. In particular,
        // Codex exec and app-server workers are not interchangeable.
        let executor = serde_json::to_string(&spec.executor).unwrap_or_default();
        format!(
            "{role}\u{1}{provider}\u{1}{model}\u{1}{workspace}\u{1}{}\u{1}d={}\u{1}ns={}\u{1}by={}\u{1}ep={}\u{1}md={}\u{1}nha={}\u{1}gro={}\u{1}executor={executor}",
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
            let Some(mut candidate) = candidate else {
                break;
            };
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
        if let ExecutorSpec::Codex {
            workspace_owned, ..
        } = &mut spec.executor
        {
            *workspace_owned = Some(
                spec.workspace
                    .as_deref()
                    .is_some_and(workspace_is_bamboo_owned),
            );
        }
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
        match &spec.executor {
            // Codex auth is independent of the session's normal Bamboo model
            // provider. Inherit/API-key/Bamboo modes need no credential-store
            // secret at provisioning; custom mode gets exactly its referenced
            // key. This prevents an unrelated upstream provider key from
            // reaching a Codex worker that only needs a per-run bcx1_ token.
            ExecutorSpec::Codex {
                auth_mode,
                provider_key_ref,
                ..
            } => {
                if auth_mode.as_deref() == Some("custom") {
                    if let Some(reference) = provider_key_ref {
                        if let Some(credential) = self.credentials.iter().find(|credential| {
                            credential.credential_ref.as_deref() == Some(reference)
                        }) {
                            spec.secrets.provider_credentials.push(credential.clone());
                        } else {
                            tracing::warn!(
                                "actor child {}: custom Codex credential reference '{}' did not resolve",
                                job.child_session_id,
                                reference
                            );
                        }
                    } else {
                        tracing::warn!(
                            "actor child {}: custom Codex executor has no credential reference",
                            job.child_session_id
                        );
                    }
                }
            }
            // Other executors keep the existing least-privilege contract: only
            // the credential for the child session's selected provider.
            _ => {
                let provider = spec
                    .model
                    .as_ref()
                    .map(|model| model.provider.as_str())
                    .filter(|provider| !provider.trim().is_empty())
                    .unwrap_or(&self.default_provider);
                if let Some(credential) = self
                    .credentials
                    .iter()
                    .find(|credential| credential.provider == provider)
                {
                    spec.secrets.provider_credentials.push(credential.clone());
                } else {
                    tracing::warn!(
                        "actor child {}: no credential found for provider '{}'",
                        job.child_session_id,
                        provider
                    );
                }
            }
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
        if spec.capabilities.guardian_read_only {
            if let ExecutorSpec::Codex {
                permission_profile, ..
            } = &mut spec.executor
            {
                *permission_profile = Some("read-only".to_string());
            }
        }
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
        .map_err(|e| {
            AgentError::LLM(format!(
                "schedulable role '{role}': bus connect failed: {e}"
            ))
        })?;
        let candidates = q.list_connected(&pool).await.map_err(|e| {
            AgentError::LLM(format!(
                "schedulable role '{role}': bus presence query failed: {e}"
            ))
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

    fn set_session_inbox_runtime(&self, binding: Option<SessionInboxRuntimeBinding>) {
        *self.session_inbox_runtime.lock().recover_poison() = binding;
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
        let session_inbox_runtime = self.session_inbox_runtime.lock().recover_poison().clone();
        let assignment = extract_assignment(session);
        let mut spec = self.build_spec(session, job);
        // Mark the worker reusable + give it an idle timeout so it self-reaps if
        // orphaned. Warm bus workers are pooled per fingerprint and reused.
        spec.reusable = true;
        if spec.limits.idle_timeout_secs.is_none() {
            spec.limits.idle_timeout_secs = Some(POOLED_IDLE_TIMEOUT_SECS);
        }
        let pool_key = Self::fingerprint(&spec);

        // The recommended provider URL is deliberately parent-loopback. A
        // resident remote worker would interpret 127.0.0.1 as itself, not this
        // server, so reject that ambiguous deployment instead of minting a
        // credential that can never authenticate to the intended parent.
        if executor_uses_bamboo_codex(&spec.executor) && !matches!(spec.placement, Placement::Local)
        {
            return Err(AgentError::LLM(
                "Codex auth mode 'bamboo' requires local actor placement; use custom mode with a reachable URL for remote workers"
                    .to_string(),
            ));
        }
        let project_id = project_id_for_actor_run(session)?;
        // Policy is captured per activation (not only when a worker is
        // provisioned), so reused local workers and resident remote/broker
        // workers observe the latest durable revision and bypass flag at the
        // next run boundary. Session grants are intentionally not inherited.
        let permission_policy = self.permission_config.as_ref().and_then(|config| {
            serde_json::to_value(config.to_serializable())
                .ok()
                .map(|policy| PermissionPolicyContext {
                    revision: config.policy_revision(),
                    bypass_permissions: session
                        .agent_runtime_state
                        .as_ref()
                        .is_some_and(|state| state.bypass_permissions),
                    session_id: session.id.clone(),
                    workspace_path: session.workspace.clone(),
                    inherit_session_grants: false,
                    policy,
                })
        });

        // Backpressure: hold a concurrency slot for the lifetime of the *run*
        // (cancellation still proceeds — the cancel branch in drive() runs while
        // we hold the permit). Released when this fn returns, i.e. once the worker
        // is parked back into the pool, so idle workers don't pin slots.
        let _slot = self
            .concurrency
            .acquire()
            .await
            .map_err(|_| AgentError::LLM("actor concurrency limiter closed".to_string()))?;

        // Bamboo-as-provider credentials are minted at the activation boundary,
        // after backpressure admits the run and never at worker provisioning.
        // This is load-bearing for warm workers: a parked process must never
        // retain a token from its previous run. The guard revokes on every return
        // path (success, error, cancellation, dispatch failure, or first-frame
        // retry exhaustion).
        let (run_secrets, _codex_token_guard) = build_codex_run_secrets(
            &spec.executor,
            self.codex_run_tokens.clone(),
            &job.child_session_id,
        )?;

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
                            AgentError::LLM(format!(
                                "remote worker CA cert '{}': {e}",
                                path.display()
                            ))
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
                        AgentError::LLM(format!(
                            "schedulable link connect to '{mailbox_id}' failed: {e}"
                        ))
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
                    .map_err(|e| {
                        AgentError::LLM(format!("broker child link connect failed: {e}"))
                    })?;
                    let client: Box<dyn bamboo_subagent::ChildLink> = Box::new(link);
                    (actor, client)
                }
            };

            // Publish the actor delivery owner and claim the complete bounded
            // authorized prefix before dispatching Run. These deliveries ride
            // inside RunSpec, so the worker durably enqueues them before its
            // first provider boundary rather than racing a later steer frame.
            let (delivery_tx, mut delivery_rx) = mpsc::unbounded_channel::<u64>();
            let bound_activation_run_id = match session_inbox_runtime.as_ref() {
                Some(binding) => {
                    let run_id = binding
                        .router
                        .attach_delivery_sink(&job.child_session_id, delivery_tx.clone())
                        .await;
                    if run_id.is_none() {
                        tracing::debug!(
                            session_id = %job.child_session_id,
                            "actor driver had no current SessionInbox activation owner to bind"
                        );
                    }
                    run_id
                }
                None => None,
            };
            drop(delivery_tx);
            let initial_pairs = match (
                session_inbox_runtime.as_ref(),
                bound_activation_run_id.as_deref(),
            ) {
                (Some(binding), Some(run_id)) => {
                    match claim_canonical_deliveries(binding, session, run_id, usize::MAX).await {
                        Ok(deliveries) => deliveries,
                        Err(error) => {
                            binding
                                .router
                                .detach_delivery_sink(&job.child_session_id, run_id)
                                .await;
                            if !remote {
                                actor.worker.kill().await;
                            }
                            return Err(error);
                        }
                    }
                }
                _ => Vec::new(),
            };
            let initial_session_messages = initial_pairs
                .iter()
                .map(|(_, delivery)| delivery.clone())
                .collect::<Vec<_>>();
            let initial_inflight_claims = initial_pairs
                .into_iter()
                .map(|(claim, _)| claim)
                .collect::<VecDeque<_>>();
            // Recompute after claim reconciliation: a warm retry may have had a
            // canonical receipt whose transcript proof was restored above.
            let messages = session
                .messages
                .iter()
                .filter_map(|message| serde_json::to_value(message).ok())
                .collect();

            if let Err(e) = client
                .send(ParentFrame::Run(RunSpec {
                    // Cloned (not moved) so a retry can re-dispatch to a fresh worker.
                    assignment: assignment.clone(),
                    logical_session: Some(logical_identity_for_actor_run(session, job)),
                    project_id: project_id.clone(),
                    reasoning_effort: None,
                    permission_policy: permission_policy.clone(),
                    messages,
                    activation_run_id: bound_activation_run_id.clone(),
                    initial_session_messages,
                    secrets: run_secrets.clone(),
                }))
                .await
            {
                if let (Some(binding), Some(run_id)) = (
                    session_inbox_runtime.as_ref(),
                    bound_activation_run_id.as_deref(),
                ) {
                    binding
                        .router
                        .detach_delivery_sink(&job.child_session_id, run_id)
                        .await;
                }
                if !remote {
                    actor.worker.kill().await;
                }
                return Err(AgentError::LLM(format!("actor run dispatch failed: {e}")));
            }

            // Register as a live actor so send_message (running, no interrupt) can
            // steer this child in-band over the existing WS connection. The guard
            // unregisters on every exit path.
            let (live_tx, mut live_rx) = mpsc::unbounded_channel::<ParentFrame>();
            let live_guard = super::live::register(
                &job.child_session_id,
                live_tx,
                attempt as u32,
                self.approval_registry.clone(),
            );

            let result = drive(ActorDriveContext {
                client: &mut *client,
                parent_session_id: &job.parent_session_id,
                child_session_id: &job.child_session_id,
                child_attempt: attempt as u32,
                approval_registry: self.approval_registry.as_ref(),
                approval_decider: self.approval_decider.as_ref(),
                approval_reviewer: self.approval_reviewer.as_ref(),
                escalation_bridge: escalation.clone(),
                event_tx: &event_tx,
                cancel_token: &cancel_token,
                live_rx: &mut live_rx,
                delivery_rx: &mut delivery_rx,
                logical_session: session,
                session_inbox_runtime: session_inbox_runtime.as_ref(),
                activation_run_id: bound_activation_run_id.as_deref(),
                initial_inflight_claims,
                // First-frame watchdog for EVERY placement: a wedged-but-connected
                // worker (subscribed ≠ serving — e.g. stuck on a prior LLM call) emits
                // no first frame; without a deadline drive() blocks forever. Bounding it
                // turns the "running-but-unresponsive" hang into a recoverable
                // WorkerUnresponsive (reap+respawn local / re-pick schedulable / error
                // on a fixed remote endpoint).
                first_frame_timeout: Some(WORKER_FIRST_FRAME_TIMEOUT),
            })
            .await;
            if let (Some(binding), Some(run_id)) = (
                session_inbox_runtime.as_ref(),
                bound_activation_run_id.as_deref(),
            ) {
                binding
                    .router
                    .detach_delivery_sink(&job.child_session_id, run_id)
                    .await;
            }
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

async fn reconcile_already_admitted_claim(
    binding: &SessionInboxRuntimeBinding,
    session: &mut Session,
    claim: &SessionInboxClaim,
) -> crate::runtime::runner::Result<()> {
    let latest = binding
        .storage
        .load_session(&session.id)
        .await
        .map_err(|error| {
            AgentError::LLM(format!(
                "load canonical SessionInbox checkpoint for {}: {error}",
                session.id
            ))
        })?
        .ok_or_else(|| {
            AgentError::LLM(format!(
                "canonical SessionInbox target disappeared: {}",
                session.id
            ))
        })?;
    let Some(message) = latest
        .messages
        .iter()
        .find(|message| bamboo_domain::is_matching_session_message(message, &claim.envelope))
        .cloned()
    else {
        return Err(AgentError::LLM(format!(
            "canonical admitted receipt for {} exists without transcript message {}",
            session.id, claim.envelope.id
        )));
    };
    if let Some(existing) = session
        .messages
        .iter_mut()
        .find(|existing| existing.id == message.id)
    {
        *existing = message;
    } else {
        session.add_message(message);
    }
    bamboo_domain::merge_session_inbox_admission(session, &latest);
    binding
        .inbox
        .ack(&session.id, claim)
        .await
        .map_err(|error| {
            AgentError::LLM(format!(
                "ack recovered canonical SessionInbox claim {}: {error}",
                claim.envelope.id
            ))
        })
}

/// Checkpoint a worker-confirmed envelope into the canonical logical Session,
/// then create the permanent host receipt/remove its exact claim. This order is
/// the actor-side equivalent of the local state_bridge crash boundary.
async fn checkpoint_and_ack_canonical_claim(
    binding: &SessionInboxRuntimeBinding,
    session: &mut Session,
    claim: &SessionInboxClaim,
) -> crate::runtime::runner::Result<()> {
    if claim.envelope.target_session_id != session.id {
        return Err(AgentError::LLM(format!(
            "canonical SessionInbox claim target {} does not match active logical session {}",
            claim.envelope.target_session_id, session.id
        )));
    }
    if binding
        .inbox
        .was_admitted(&session.id, &claim.envelope.id)
        .await
        .map_err(|error| {
            AgentError::LLM(format!(
                "inspect canonical admitted receipt {}: {error}",
                claim.envelope.id
            ))
        })?
    {
        return reconcile_already_admitted_claim(binding, session, claim).await;
    }

    let transcript_has_id = session
        .messages
        .iter()
        .any(|message| bamboo_domain::is_matching_session_message(message, &claim.envelope));
    if session
        .messages
        .iter()
        .any(|message| message.id == claim.envelope.id.as_str())
        && !transcript_has_id
    {
        return Err(AgentError::LLM(format!(
            "canonical SessionInbox id {} collides with a non-matching transcript message",
            claim.envelope.id
        )));
    }
    let cursor_has_id = session
        .session_inbox_admission()
        .is_some_and(|state| state.contains(&claim.envelope.id));
    if cursor_has_id && !transcript_has_id {
        return Err(AgentError::LLM(format!(
            "canonical SessionInbox cursor exists without transcript message {}",
            claim.envelope.id
        )));
    }
    let before = session.clone();
    if !transcript_has_id {
        let message = claim.envelope.to_provider_message().map_err(|error| {
            AgentError::LLM(format!(
                "translate canonical SessionInbox envelope {}: {error}",
                claim.envelope.id
            ))
        })?;
        session.add_message(message);
    }
    session
        .session_inbox_admission_mut()
        .record(claim.envelope.id.clone(), claim.generation);
    session.updated_at = chrono::Utc::now();

    if let Err(error) = binding
        .persistence
        .checkpoint_runtime_session(session)
        .await
    {
        *session = before;
        return Err(AgentError::LLM(format!(
            "checkpoint canonical SessionInbox claim {}: {error}",
            claim.envelope.id
        )));
    }
    if !session
        .messages
        .iter()
        .any(|message| bamboo_domain::is_matching_session_message(message, &claim.envelope))
    {
        *session = before;
        return Err(AgentError::LLM(format!(
            "canonical SessionInbox checkpoint lost typed transcript proof for {}",
            claim.envelope.id
        )));
    }
    binding
        .inbox
        .ack(&session.id, claim)
        .await
        .map_err(|error| {
            AgentError::LLM(format!(
                "ack canonical SessionInbox claim {} after checkpoint: {error}",
                claim.envelope.id
            ))
        })
}

/// Durably seed claimed typed messages into the canonical host transcript
/// before dispatching them to any actor worker, while deliberately leaving the
/// admission cursor and `cur/` claims untouched.
///
/// This is the cross-placement lost-confirmation invariant: if worker A admits
/// and reasons over the batch but its confirmations are lost, a retry on worker
/// B receives a host snapshot already containing each stable typed message
/// exactly once. Worker B's local safe boundary then records/acks the same ids
/// without appending duplicates. Only an exact worker confirmation advances the
/// host cursor; only a durable cursor checkpoint precedes host ack.
async fn checkpoint_claim_context_before_dispatch(
    binding: &SessionInboxRuntimeBinding,
    session: &mut Session,
    claims: &[SessionInboxClaim],
) -> crate::runtime::runner::Result<()> {
    if claims.is_empty() {
        return Ok(());
    }
    let before = session.clone();
    for claim in claims {
        if claim.envelope.target_session_id != session.id {
            return Err(AgentError::LLM(format!(
                "canonical SessionInbox claim target {} does not match actor session {}",
                claim.envelope.target_session_id, session.id
            )));
        }
        let matching = session
            .messages
            .iter()
            .any(|message| bamboo_domain::is_matching_session_message(message, &claim.envelope));
        if session
            .messages
            .iter()
            .any(|message| message.id == claim.envelope.id.as_str())
            && !matching
        {
            return Err(AgentError::LLM(format!(
                "canonical SessionInbox id {} collides before actor dispatch",
                claim.envelope.id
            )));
        }
        if session
            .session_inbox_admission()
            .is_some_and(|state| state.contains(&claim.envelope.id))
            && !matching
        {
            return Err(AgentError::LLM(format!(
                "canonical SessionInbox cursor exists without transcript proof for {}",
                claim.envelope.id
            )));
        }
        if !matching {
            let message = claim.envelope.to_provider_message().map_err(|error| {
                AgentError::LLM(format!(
                    "translate canonical SessionInbox envelope {} before actor dispatch: {error}",
                    claim.envelope.id
                ))
            })?;
            session.add_message(message);
        }
    }
    session.updated_at = chrono::Utc::now();
    if let Err(error) = binding
        .persistence
        .checkpoint_runtime_session(session)
        .await
    {
        *session = before;
        return Err(AgentError::LLM(format!(
            "checkpoint canonical SessionInbox actor context: {error}"
        )));
    }
    for claim in claims {
        if !session
            .messages
            .iter()
            .any(|message| bamboo_domain::is_matching_session_message(message, &claim.envelope))
        {
            *session = before;
            return Err(AgentError::LLM(format!(
                "actor context checkpoint lost typed transcript proof for {}",
                claim.envelope.id
            )));
        }
    }
    Ok(())
}

async fn claim_canonical_deliveries(
    binding: &SessionInboxRuntimeBinding,
    session: &mut Session,
    activation_run_id: &str,
    limit: usize,
) -> crate::runtime::runner::Result<Vec<(SessionInboxClaim, SessionMessageDelivery)>> {
    let claims = binding
        .inbox
        .claim(&session.id, limit)
        .await
        .map_err(|error| {
            AgentError::LLM(format!(
                "claim canonical SessionInbox for active actor {}: {error}",
                session.id
            ))
        })?;
    if claims.is_empty() {
        return Ok(Vec::new());
    }
    let interrupt_generation = binding
        .inbox
        .inspect(&session.id)
        .await
        .map_err(|error| {
            AgentError::LLM(format!(
                "inspect canonical SessionInbox activation policy for {}: {error}",
                session.id
            ))
        })?
        .interrupt_generation;
    let mut unconfirmed = Vec::with_capacity(claims.len());
    for claim in claims {
        if binding
            .inbox
            .was_admitted(&session.id, &claim.envelope.id)
            .await
            .map_err(|error| {
                AgentError::LLM(format!(
                    "inspect canonical SessionInbox claim {}: {error}",
                    claim.envelope.id
                ))
            })?
        {
            reconcile_already_admitted_claim(binding, session, &claim).await?;
            continue;
        }
        unconfirmed.push(claim);
    }
    checkpoint_claim_context_before_dispatch(binding, session, &unconfirmed).await?;

    let mut deliveries = Vec::with_capacity(unconfirmed.len());
    for claim in unconfirmed {
        // Cursor+transcript without the permanent tombstone is the recoverable
        // crash window after an exact worker confirmation was checkpointed but
        // before host ack removed `cur/`. Finish that ack without exposing the
        // message to another provider run.
        if session
            .session_inbox_admission()
            .is_some_and(|state| state.contains(&claim.envelope.id))
        {
            binding
                .inbox
                .ack(&session.id, &claim)
                .await
                .map_err(|error| {
                    AgentError::LLM(format!(
                        "finish confirmed canonical SessionInbox ack {}: {error}",
                        claim.envelope.id
                    ))
                })?;
            continue;
        }
        let activation_policy = if claim.generation <= interrupt_generation {
            bamboo_domain::SessionActivationPolicy::InterruptSpecificWait
        } else {
            bamboo_domain::SessionActivationPolicy::RespectSpecificWait
        };
        let delivery = SessionMessageDelivery {
            target_session_id: session.id.clone(),
            envelope: claim.envelope.clone(),
            canonical_claim_generation: claim.generation,
            activation_run_id: activation_run_id.to_string(),
            activation_policy,
        };
        deliveries.push((claim, delivery));
    }
    Ok(deliveries)
}

async fn forward_next_canonical_claim(
    client: &mut dyn bamboo_subagent::ChildLink,
    binding: &SessionInboxRuntimeBinding,
    session: &mut Session,
    activation_run_id: &str,
    inflight: &mut VecDeque<SessionInboxClaim>,
) -> crate::runtime::runner::Result<()> {
    if !inflight.is_empty() {
        return Ok(());
    }
    let Some((claim, delivery)) =
        claim_canonical_deliveries(binding, session, activation_run_id, 1)
            .await?
            .pop()
    else {
        return Ok(());
    };
    client
        .send(ParentFrame::SessionMessage { delivery })
        .await
        .map_err(|error| {
            AgentError::LLM(format!(
                "forward canonical SessionInbox claim {} to active actor: {error}",
                claim.envelope.id
            ))
        })?;
    inflight.push_back(claim);
    Ok(())
}

/// Borrowed and per-run-owned inputs for one actor frame pump.
struct ActorDriveContext<'a> {
    client: &'a mut dyn bamboo_subagent::ChildLink,
    parent_session_id: &'a str,
    child_session_id: &'a str,
    child_attempt: u32,
    approval_registry: Option<&'a super::approval_registry::SharedApprovalRegistry>,
    approval_decider: Option<&'a Arc<dyn ChildApprovalDecider>>,
    approval_reviewer: Option<&'a Arc<dyn ChildApprovalReviewer>>,
    escalation_bridge: Option<bamboo_subagent::executor::HostBridge>,
    event_tx: &'a mpsc::Sender<AgentEvent>,
    cancel_token: &'a CancellationToken,
    live_rx: &'a mut mpsc::UnboundedReceiver<ParentFrame>,
    delivery_rx: &'a mut mpsc::UnboundedReceiver<u64>,
    logical_session: &'a mut Session,
    session_inbox_runtime: Option<&'a SessionInboxRuntimeBinding>,
    activation_run_id: Option<&'a str>,
    initial_inflight_claims: VecDeque<SessionInboxClaim>,
    first_frame_timeout: Option<Duration>,
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
async fn drive(context: ActorDriveContext<'_>) -> crate::runtime::runner::Result<Option<String>> {
    let ActorDriveContext {
        client,
        parent_session_id,
        child_session_id,
        child_attempt,
        approval_registry,
        approval_decider,
        approval_reviewer,
        escalation_bridge,
        event_tx,
        cancel_token,
        live_rx,
        delivery_rx,
        logical_session,
        session_inbox_runtime,
        activation_run_id,
        initial_inflight_claims,
        first_frame_timeout,
    } = context;

    // First-frame watchdog: a live worker emits its first frame (run-started /
    // first token) within seconds; total silence past the deadline means the
    // worker is dead (e.g. a pooled worker that exited right after checkout), so
    // its Run sits queued forever. We trip ONLY before the first frame — once any
    // frame arrives the worker is proven live and a legitimately long run (a slow
    // tool between tokens) never trips it.
    let mut got_first_frame = false;
    let mut first_frame_watch = first_frame_timeout.map(|d| Box::pin(tokio::time::sleep(d)));
    let mut inflight_claims = initial_inflight_claims;
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
            Some(_generation) = delivery_rx.recv(),
                if session_inbox_runtime.is_some() && activation_run_id.is_some() =>
            {
                forward_next_canonical_claim(
                    client,
                    session_inbox_runtime.expect("guarded"),
                    logical_session,
                    activation_run_id.expect("guarded"),
                    &mut inflight_claims,
                )
                .await?;
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
                        if let Some(reviewer) = approval_reviewer
                            .cloned()
                            .or_else(child_approval_reviewer)
                        {
                            // Phase 6, Part B: a BYPASSED parent worker
                            // model-reviews its children's forced-ask (dangerous)
                            // actions. The review is an LLM call, so run it OFF
                            // the frame pump in a spawned task and deliver the
                            // verdict async via the live channel — the pump keeps
                            // forwarding events and the agent loop never blocks. A
                            // timeout denies a hung review so the child can't hang.
                            let child = child_session_id.to_string();
                            let parent = parent_session_id.to_string();
                            let req_id = id.clone();
                            let body = body.clone();
                            let registry = approval_registry.cloned();
                            tokio::spawn(async move {
                                let approved = tokio::time::timeout(
                                    CHILD_APPROVAL_TIMEOUT,
                                    reviewer.review(&parent, &child, &body),
                                )
                                .await
                                .unwrap_or(false);
                                super::live::deliver_approval_scoped(
                                    registry.as_ref(),
                                    &child,
                                    child_attempt,
                                    &req_id,
                                    approved,
                                );
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
                            // up every level until a bypass level or the top
                            // orchestrator's model reviewer decides. With no such
                            // reviewer the top level fails closed. Off-loop so the
                            // pump never blocks; relay the reply down to the child.
                            let child = child_session_id.to_string();
                            let req_id = id.clone();
                            let body = body.clone();
                            let registry = approval_registry.cloned();
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
                                super::live::deliver_approval_scoped(
                                    registry.as_ref(),
                                    &child,
                                    child_attempt,
                                    &req_id,
                                    approved,
                                );
                            });
                        } else {
                            // There is no parent-agent reviewer or upstream actor
                            // to own this decision. Never open a manual/UI approval
                            // path: forced-ask is parent-reviewed or fail-closed.
                            tracing::warn!(
                                parent_session_id,
                                child_session_id,
                                request_id = %id,
                                "forced-ask request has no parent-agent reviewer; denying"
                            );
                            if client
                                .send(ParentFrame::ApprovalReply {
                                    id,
                                    approved: false,
                                })
                                .await
                                .is_err()
                            {
                                tracing::warn!(
                                    "failed to send fail-closed approval reply; connection failing"
                                );
                            }
                        }
                    }
                    Ok(Some(ChildFrame::SessionMessageAdmitted { confirmation })) => {
                        let Some(binding) = session_inbox_runtime else {
                            tracing::warn!(
                                child_session_id,
                                "ignoring SessionInbox confirmation without a runtime binding"
                            );
                            continue;
                        };
                        let Some(bound_run_id) = activation_run_id else {
                            tracing::warn!(
                                child_session_id,
                                "ignoring SessionInbox confirmation without an activation owner"
                            );
                            continue;
                        };
                        let Some(claim) = inflight_claims.front() else {
                            tracing::warn!(
                                child_session_id,
                                envelope_id = %confirmation.envelope_id,
                                "rejecting stale SessionInbox confirmation with no in-flight canonical claim"
                            );
                            continue;
                        };
                        let exact = confirmation.target_session_id == logical_session.id
                            && confirmation.envelope_id == claim.envelope.id.as_str()
                            && confirmation.canonical_claim_generation == claim.generation
                            && confirmation.activation_run_id == bound_run_id;
                        if !exact
                            || !binding
                                .router
                                .owns_run(&logical_session.id, bound_run_id)
                                .await
                        {
                            tracing::warn!(
                                child_session_id,
                                expected_target = %logical_session.id,
                                received_target = %confirmation.target_session_id,
                                expected_envelope_id = %claim.envelope.id,
                                received_envelope_id = %confirmation.envelope_id,
                                expected_generation = claim.generation,
                                received_generation = confirmation.canonical_claim_generation,
                                expected_run_id = bound_run_id,
                                received_run_id = %confirmation.activation_run_id,
                                "rejecting stale or mismatched SessionInbox admission confirmation"
                            );
                            continue;
                        }
                        let claim = inflight_claims
                            .pop_front()
                            .expect("validated in-flight canonical claim");
                        // On failure the durable canonical cur file remains
                        // recoverable for the next owner.
                        checkpoint_and_ack_canonical_claim(binding, logical_session, &claim)
                            .await?;
                        // Ordered single-consumer: only after the exact prior
                        // claim is checkpointed+acked may the driver claim and
                        // forward the next envelope.
                        if inflight_claims.is_empty() {
                            forward_next_canonical_claim(
                                client,
                                binding,
                                logical_session,
                                bound_run_id,
                                &mut inflight_claims,
                            )
                            .await?;
                        }
                    }
                    Ok(Some(ChildFrame::Terminal { status, result, error, .. })) => {
                        if let Some(claim) = inflight_claims.front() {
                            return Err(AgentError::LLM(format!(
                                "actor terminated before durably admitting SessionInbox message {}; canonical claim remains recoverable",
                                claim.envelope.id
                            )));
                        }
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
fn project_id_for_actor_run(
    session: &Session,
) -> Result<Option<bamboo_domain::ProjectId>, AgentError> {
    match crate::project_context::ProjectContextResolver::session_project_identity(session) {
        crate::project_context::SessionProjectIdentity::Assigned(project_id) => {
            Ok(Some(project_id))
        }
        crate::project_context::SessionProjectIdentity::Unassigned => Ok(None),
        crate::project_context::SessionProjectIdentity::Invalid { raw, message } => {
            Err(AgentError::LLM(format!(
                "child session carries an invalid Project identity '{raw}': {message}"
            )))
        }
    }
}

fn logical_identity_for_actor_run(session: &Session, job: &SpawnJob) -> LogicalSessionIdentity {
    LogicalSessionIdentity {
        session_id: session.id.clone(),
        parent_session_id: session
            .parent_session_id
            .clone()
            .or_else(|| Some(job.parent_session_id.clone())),
        root_session_id: if session.root_session_id.trim().is_empty() {
            job.parent_session_id.clone()
        } else {
            session.root_session_id.clone()
        },
    }
}

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
    use crate::SessionActivationRouter;
    use bamboo_domain::{RuntimeSessionPersistence, SessionInboxPort, Storage};

    struct ActorFaultingPersistence {
        inner: Arc<bamboo_storage::LockedSessionStore>,
        fail_checkpoint_once: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl RuntimeSessionPersistence for ActorFaultingPersistence {
        async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            self.inner.merge_save_runtime(session).await
        }

        async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            if self
                .fail_checkpoint_once
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(std::io::Error::other("injected actor checkpoint failure"));
            }
            self.inner.checkpoint_runtime_session(session).await
        }

        async fn load_runtime_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            self.inner.storage().load_session(session_id).await
        }
    }

    struct ActorFailBeforeAckInbox {
        inner: Arc<dyn SessionInboxPort>,
        fail_once: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl SessionInboxPort for ActorFailBeforeAckInbox {
        async fn deliver(
            &self,
            envelope: &bamboo_domain::SessionMessageEnvelope,
        ) -> Result<bamboo_domain::SessionInboxReceipt, bamboo_domain::SessionInboxError> {
            self.inner.deliver(envelope).await
        }

        async fn mark_activation_eligible(
            &self,
            target_session_id: &str,
            generation: u64,
            policy: bamboo_domain::SessionActivationPolicy,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            self.inner
                .mark_activation_eligible(target_session_id, generation, policy)
                .await
        }

        async fn claim(
            &self,
            target_session_id: &str,
            limit: usize,
        ) -> Result<Vec<SessionInboxClaim>, bamboo_domain::SessionInboxError> {
            self.inner.claim(target_session_id, limit).await
        }

        async fn was_admitted(
            &self,
            target_session_id: &str,
            id: &bamboo_domain::SessionMessageId,
        ) -> Result<bool, bamboo_domain::SessionInboxError> {
            self.inner.was_admitted(target_session_id, id).await
        }

        async fn ack(
            &self,
            target_session_id: &str,
            claim: &SessionInboxClaim,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            if self
                .fail_once
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(bamboo_domain::SessionInboxError::Storage(
                    "injected actor pre-ack failure".to_string(),
                ));
            }
            self.inner.ack(target_session_id, claim).await
        }

        async fn inspect(
            &self,
            target_session_id: &str,
        ) -> Result<bamboo_domain::SessionInboxBacklog, bamboo_domain::SessionInboxError> {
            self.inner.inspect(target_session_id).await
        }
    }

    async fn actor_inbox_fixture(
        session_id: &str,
    ) -> (
        tempfile::TempDir,
        Arc<bamboo_storage::SessionStoreV2>,
        Arc<bamboo_storage::LockedSessionStore>,
        Arc<dyn SessionInboxPort>,
        Session,
        SessionInboxClaim,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let locked = Arc::new(bamboo_storage::LockedSessionStore::new(storage));
        let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        let session = Session::new(session_id, "model");
        store.save_session(&session).await.unwrap();
        let mut envelope =
            bamboo_domain::SessionMessageEnvelope::user_input(session_id, "actor follow-up");
        envelope.id =
            bamboo_domain::SessionMessageId::parse(format!("{session_id}-message")).unwrap();
        let receipt = inbox.deliver(&envelope).await.unwrap();
        inbox
            .mark_activation_eligible(
                session_id,
                receipt.generation,
                bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let claim = inbox.claim(session_id, 1).await.unwrap().remove(0);
        (temp, store, locked, inbox, session, claim)
    }

    fn actor_binding(
        store: Arc<bamboo_storage::SessionStoreV2>,
        inbox: Arc<dyn SessionInboxPort>,
        persistence: Arc<dyn RuntimeSessionPersistence>,
    ) -> SessionInboxRuntimeBinding {
        let storage: Arc<dyn Storage> = store;
        SessionInboxRuntimeBinding {
            router: SessionActivationRouter::new(),
            inbox,
            storage,
            persistence,
        }
    }

    #[tokio::test]
    async fn actor_mismatched_typed_marker_id_collision_never_acks() {
        let (_temp, store, locked, inbox, mut session, claim) =
            actor_inbox_fixture("actor-live-id-collision").await;
        let mut forged = claim.envelope.to_provider_message().unwrap();
        forged.metadata = Some(serde_json::json!({
            "session_message": {
                "id": claim.envelope.id,
                "target_session_id": "different-session"
            }
        }));
        session.add_message(forged);
        let persistence: Arc<dyn RuntimeSessionPersistence> = locked;
        let binding = actor_binding(store, inbox.clone(), persistence);

        assert!(
            checkpoint_and_ack_canonical_claim(&binding, &mut session, &claim)
                .await
                .is_err()
        );
        assert_eq!(inbox.inspect(&session.id).await.unwrap().claimed, 1);
        assert!(!inbox
            .was_admitted(&session.id, &claim.envelope.id)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn actor_concurrent_durable_id_collision_after_claim_never_acks() {
        let (_temp, store, locked, inbox, mut session, claim) =
            actor_inbox_fixture("actor-durable-id-collision").await;
        let mut concurrent = store.load_session(&session.id).await.unwrap().unwrap();
        let mut forged = bamboo_agent_core::Message::user("concurrent actor collision");
        forged.id = claim.envelope.id.to_string();
        concurrent.add_message(forged);
        store.save_session(&concurrent).await.unwrap();
        let persistence: Arc<dyn RuntimeSessionPersistence> = locked;
        let binding = actor_binding(store.clone(), inbox.clone(), persistence);

        assert!(
            checkpoint_and_ack_canonical_claim(&binding, &mut session, &claim)
                .await
                .is_err()
        );
        assert_eq!(inbox.inspect(&session.id).await.unwrap().claimed, 1);
        assert!(!inbox
            .was_admitted(&session.id, &claim.envelope.id)
            .await
            .unwrap());
        let durable = store.load_session(&session.id).await.unwrap().unwrap();
        assert!(!durable.messages.iter().any(|message| {
            bamboo_domain::is_matching_session_message(message, &claim.envelope)
        }));
    }

    #[tokio::test]
    async fn actor_concurrent_durable_typed_body_mismatch_never_acks() {
        let (_temp, store, locked, inbox, mut session, claim) =
            actor_inbox_fixture("actor-durable-typed-body-collision").await;
        let mut different = claim.envelope.clone();
        different.body = bamboo_domain::SessionMessageBody::Content(
            bamboo_domain::SessionMessageContent::text("forged actor body"),
        );
        let mut concurrent = store.load_session(&session.id).await.unwrap().unwrap();
        concurrent.add_message(different.to_provider_message().unwrap());
        store.save_session(&concurrent).await.unwrap();
        let persistence: Arc<dyn RuntimeSessionPersistence> = locked;
        let binding = actor_binding(store.clone(), inbox.clone(), persistence);

        assert!(
            checkpoint_and_ack_canonical_claim(&binding, &mut session, &claim)
                .await
                .is_err()
        );
        assert_eq!(inbox.inspect(&session.id).await.unwrap().claimed, 1);
        assert!(!inbox
            .was_admitted(&session.id, &claim.envelope.id)
            .await
            .unwrap());
        let durable = store.load_session(&session.id).await.unwrap().unwrap();
        assert!(!durable
            .messages
            .iter()
            .any(|message| bamboo_domain::is_matching_session_message(message, &claim.envelope)));
    }

    #[tokio::test]
    async fn actor_checkpoint_failure_rolls_back_and_restart_admits_once() {
        let (_temp, store, locked, inbox, mut session, claim) =
            actor_inbox_fixture("actor-checkpoint-failure").await;
        let envelope_id = claim.envelope.id.clone();
        let fault: Arc<dyn RuntimeSessionPersistence> = Arc::new(ActorFaultingPersistence {
            inner: locked.clone(),
            fail_checkpoint_once: std::sync::atomic::AtomicBool::new(true),
        });
        let binding = actor_binding(store.clone(), inbox.clone(), fault);

        assert!(
            checkpoint_and_ack_canonical_claim(&binding, &mut session, &claim)
                .await
                .is_err()
        );
        assert!(!session
            .messages
            .iter()
            .any(|message| message.id == envelope_id.as_str()));
        assert_eq!(inbox.inspect(&session.id).await.unwrap().claimed, 1);
        assert!(!inbox.was_admitted(&session.id, &envelope_id).await.unwrap());

        let reopened: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        let recovered = reopened.claim(&session.id, 1).await.unwrap().remove(0);
        let persistence: Arc<dyn RuntimeSessionPersistence> = locked;
        let binding = actor_binding(store.clone(), reopened.clone(), persistence);
        let mut restarted = store.load_session(&session.id).await.unwrap().unwrap();
        checkpoint_and_ack_canonical_claim(&binding, &mut restarted, &recovered)
            .await
            .unwrap();
        assert_eq!(
            restarted
                .messages
                .iter()
                .filter(|message| message.id == envelope_id.as_str())
                .count(),
            1
        );
        assert!(reopened
            .was_admitted(&session.id, &envelope_id)
            .await
            .unwrap());
        let backlog = reopened.inspect(&session.id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
    }

    #[tokio::test]
    async fn actor_checkpoint_success_pre_ack_failure_recovers_without_duplicate() {
        let (_temp, store, locked, real_inbox, mut session, claim) =
            actor_inbox_fixture("actor-pre-ack-failure").await;
        let envelope_id = claim.envelope.id.clone();
        let faulted: Arc<dyn SessionInboxPort> = Arc::new(ActorFailBeforeAckInbox {
            inner: real_inbox.clone(),
            fail_once: std::sync::atomic::AtomicBool::new(true),
        });
        let persistence: Arc<dyn RuntimeSessionPersistence> = locked.clone();
        let binding = actor_binding(store.clone(), faulted, persistence);

        assert!(
            checkpoint_and_ack_canonical_claim(&binding, &mut session, &claim)
                .await
                .is_err()
        );
        let durable = store.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            durable
                .messages
                .iter()
                .filter(|message| message.id == envelope_id.as_str())
                .count(),
            1
        );
        assert_eq!(real_inbox.inspect(&session.id).await.unwrap().claimed, 1);
        assert!(!real_inbox
            .was_admitted(&session.id, &envelope_id)
            .await
            .unwrap());

        let reopened: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        let recovered = reopened.claim(&session.id, 1).await.unwrap().remove(0);
        let persistence: Arc<dyn RuntimeSessionPersistence> = locked;
        let binding = actor_binding(store.clone(), reopened.clone(), persistence);
        let mut restarted = durable;
        checkpoint_and_ack_canonical_claim(&binding, &mut restarted, &recovered)
            .await
            .unwrap();
        assert_eq!(
            restarted
                .messages
                .iter()
                .filter(|message| message.id == envelope_id.as_str())
                .count(),
            1
        );
        assert!(reopened
            .was_admitted(&session.id, &envelope_id)
            .await
            .unwrap());
        let backlog = reopened.inspect(&session.id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
    }

    struct ConfirmationSequenceLink {
        frames: VecDeque<ChildFrame>,
        sent: Vec<ParentFrame>,
    }

    #[async_trait]
    impl bamboo_subagent::ChildLink for ConfirmationSequenceLink {
        async fn send(&mut self, frame: ParentFrame) -> bamboo_subagent::TransportResult<()> {
            self.sent.push(frame);
            Ok(())
        }

        async fn next_frame(&mut self) -> bamboo_subagent::TransportResult<Option<ChildFrame>> {
            match self.frames.pop_front() {
                Some(frame) => Ok(Some(frame)),
                None => std::future::pending().await,
            }
        }
    }

    fn admission_confirmation(
        session_id: &str,
        claim: &SessionInboxClaim,
        run_id: &str,
    ) -> bamboo_subagent::proto::SessionMessageAdmissionConfirmation {
        bamboo_subagent::proto::SessionMessageAdmissionConfirmation {
            target_session_id: session_id.to_string(),
            envelope_id: claim.envelope.id.to_string(),
            canonical_claim_generation: claim.generation,
            activation_run_id: run_id.to_string(),
        }
    }

    #[tokio::test]
    async fn actor_initial_batch_acks_in_order_and_rejects_stale_confirmation() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let locked = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
        let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        let session_id = "actor-confirmation-order";
        let run_id = "actor-run-current";
        let mut session = Session::new(session_id, "model");
        store.save_session(&session).await.unwrap();
        for (id, text) in [("actor-first", "first"), ("actor-second", "second")] {
            let mut envelope = bamboo_domain::SessionMessageEnvelope::user_input(session_id, text);
            envelope.id = bamboo_domain::SessionMessageId::parse(id).unwrap();
            inbox.deliver(&envelope).await.unwrap();
        }
        inbox
            .mark_activation_eligible(
                session_id,
                2,
                bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let router = SessionActivationRouter::new();
        let mut owner_registration = router.register_run(session_id, run_id).await.unwrap();
        let binding = SessionInboxRuntimeBinding {
            router,
            inbox: inbox.clone(),
            storage,
            persistence: locked,
        };
        let pairs = claim_canonical_deliveries(&binding, &mut session, run_id, usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            pairs
                .iter()
                .map(|(claim, _)| claim.envelope.id.as_str())
                .collect::<Vec<_>>(),
            vec!["actor-first", "actor-second"]
        );
        let seeded = store.load_session(session_id).await.unwrap().unwrap();
        assert_eq!(
            seeded
                .messages
                .iter()
                .filter(|message| matches!(message.id.as_str(), "actor-first" | "actor-second"))
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["actor-first", "actor-second"],
            "host context must be durable before actor dispatch"
        );
        for (claim, _) in &pairs {
            assert_eq!(
                seeded
                    .messages
                    .iter()
                    .filter(|message| bamboo_domain::is_matching_session_message(
                        message,
                        &claim.envelope
                    ))
                    .count(),
                1,
                "pre-dispatch host checkpoint must contain exactly one canonical marker for {}",
                claim.envelope.id
            );
        }
        assert!(
            seeded.session_inbox_admission().is_none_or(|cursor| {
                !cursor.contains(&pairs[0].0.envelope.id)
                    && !cursor.contains(&pairs[1].0.envelope.id)
            }),
            "pre-dispatch transcript seeding must not forge worker confirmation"
        );
        assert_eq!(inbox.inspect(session_id).await.unwrap().claimed, 2);
        let claims = pairs
            .into_iter()
            .map(|(claim, _)| claim)
            .collect::<VecDeque<_>>();
        let first = claims[0].clone();
        let second = claims[1].clone();
        let mut stale = admission_confirmation(session_id, &first, "stale-run");
        stale.canonical_claim_generation = second.generation;
        let mut link = ConfirmationSequenceLink {
            frames: VecDeque::from([
                ChildFrame::SessionMessageAdmitted {
                    confirmation: stale,
                },
                ChildFrame::SessionMessageAdmitted {
                    confirmation: admission_confirmation(session_id, &first, run_id),
                },
                ChildFrame::SessionMessageAdmitted {
                    confirmation: admission_confirmation(session_id, &second, run_id),
                },
                ChildFrame::Terminal {
                    status: TerminalStatus::Completed,
                    result: Some("done".to_string()),
                    error: None,
                    transcript: Vec::new(),
                },
            ]),
            sent: Vec::new(),
        };
        let (event_tx, _event_rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let (_live_tx, mut live_rx) = mpsc::unbounded_channel();
        let (_delivery_tx, mut delivery_rx) = mpsc::unbounded_channel();
        let result = drive(ActorDriveContext {
            client: &mut link,
            parent_session_id: "parent",
            child_session_id: session_id,
            child_attempt: 0,
            approval_registry: None,
            approval_decider: None,
            approval_reviewer: None,
            escalation_bridge: None,
            event_tx: &event_tx,
            cancel_token: &cancel,
            live_rx: &mut live_rx,
            delivery_rx: &mut delivery_rx,
            logical_session: &mut session,
            session_inbox_runtime: Some(&binding),
            activation_run_id: Some(run_id),
            initial_inflight_claims: claims,
            first_frame_timeout: Some(Duration::from_secs(1)),
        })
        .await
        .unwrap();
        assert_eq!(result.as_deref(), Some("done"));
        assert_eq!(
            session
                .messages
                .iter()
                .filter(|message| matches!(message.id.as_str(), "actor-first" | "actor-second"))
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["actor-first", "actor-second"]
        );
        let backlog = inbox.inspect(session_id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
        let confirmed = store.load_session(session_id).await.unwrap().unwrap();
        let cursor = confirmed
            .session_inbox_admission()
            .expect("exact worker confirmation must checkpoint the admission cursor");
        assert!(cursor.contains(&first.envelope.id));
        assert!(cursor.contains(&second.envelope.id));
        for claim in [&first, &second] {
            assert_eq!(
                confirmed
                    .messages
                    .iter()
                    .filter(|message| bamboo_domain::is_matching_session_message(
                        message,
                        &claim.envelope
                    ))
                    .count(),
                1,
                "confirmation must retain one exact canonical marker for {}",
                claim.envelope.id
            );
        }
        assert!(inbox
            .was_admitted(session_id, &first.envelope.id)
            .await
            .unwrap());
        assert!(inbox
            .was_admitted(session_id, &second.envelope.id)
            .await
            .unwrap());
        owner_registration.begin_finalization().await;
        owner_registration.finish(2).await.unwrap();
    }

    #[derive(Default)]
    struct RecordingCodexTokenAuthority {
        issued_for: std::sync::Mutex<Vec<String>>,
        revoked: std::sync::Mutex<Vec<String>>,
    }

    impl CodexRunTokenAuthority for RecordingCodexTokenAuthority {
        fn issue(&self, session_id: &str) -> Result<IssuedCodexRunToken, String> {
            self.issued_for
                .lock()
                .expect("issued fixture lock")
                .push(session_id.to_string());
            Ok(IssuedCodexRunToken {
                token_id: format!("id-{session_id}"),
                token: format!("bcx1_secret-{session_id}"),
            })
        }

        fn revoke(&self, token_id: &str) {
            self.revoked
                .lock()
                .expect("revoked fixture lock")
                .push(token_id.to_string());
        }
    }

    fn codex_executor(auth_mode: Option<&str>, inherit_user_config: Option<bool>) -> ExecutorSpec {
        let bamboo_mode = auth_mode == Some("bamboo")
            || (auth_mode.is_none() && !inherit_user_config.unwrap_or(false));
        ExecutorSpec::Codex {
            binary: None,
            model: None,
            mode: None,
            sandbox: None,
            inherit_user_config,
            auth_mode: auth_mode.map(str::to_string),
            base_url: bamboo_mode.then(|| "http://127.0.0.1:9562/openai/v1".to_string()),
            wire_api: Some("responses".to_string()),
            provider_key_ref: None,
            forward_env: None,
            approval_policy: None,
            network_access: None,
            allow_danger_bypass: None,
            permission_profile: None,
            workspace_owned: None,
        }
    }

    #[test]
    fn only_bamboo_managed_non_git_workspaces_are_marked_owned() {
        let project = tempfile::tempdir().unwrap();
        let managed = project.path().join(".bamboo/worktree/child-571");
        std::fs::create_dir_all(&managed).unwrap();
        assert!(!workspace_is_bamboo_owned(managed.to_str().unwrap()));
        let marker = project
            .path()
            .join(".bamboo/worktree/.bamboo-owned/child-571");
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        std::fs::write(&marker, "bamboo/child-571").unwrap();
        assert!(workspace_is_bamboo_owned(managed.to_str().unwrap()));
        let nested = managed.join("nested/path");
        std::fs::create_dir_all(&nested).unwrap();
        assert!(workspace_is_bamboo_owned(nested.to_str().unwrap()));

        let arbitrary = tempfile::tempdir().unwrap();
        assert!(!workspace_is_bamboo_owned(
            arbitrary.path().to_str().unwrap()
        ));
    }

    #[test]
    fn bamboo_codex_token_is_per_run_redacted_and_revoked_on_guard_drop() {
        let authority = Arc::new(RecordingCodexTokenAuthority::default());
        let authority_dyn: Arc<dyn CodexRunTokenAuthority> = authority.clone();

        let (secrets, guard) = build_codex_run_secrets(
            &codex_executor(Some("bamboo"), None),
            Some(authority_dyn),
            "child-570",
        )
        .unwrap();

        let token = secrets
            .codex_provider_token
            .as_ref()
            .expect("bamboo mode mints a token");
        assert_eq!(token.expose(), "bcx1_secret-child-570");
        assert!(!format!("{token:?}").contains("secret-child-570"));
        assert_eq!(
            authority.issued_for.lock().unwrap().as_slice(),
            ["child-570"]
        );
        assert!(authority.revoked.lock().unwrap().is_empty());

        drop(guard);
        assert_eq!(
            authority.revoked.lock().unwrap().as_slice(),
            ["id-child-570"]
        );
    }

    #[test]
    fn non_bamboo_codex_never_mints_and_bamboo_fails_closed_without_authority() {
        let authority = Arc::new(RecordingCodexTokenAuthority::default());
        let authority_dyn: Arc<dyn CodexRunTokenAuthority> = authority.clone();
        let (secrets, guard) = build_codex_run_secrets(
            &codex_executor(Some("custom"), None),
            Some(authority_dyn),
            "child-custom",
        )
        .unwrap();
        assert!(secrets.codex_provider_token.is_none());
        assert!(guard.is_none());
        assert!(authority.issued_for.lock().unwrap().is_empty());

        let error = build_codex_run_secrets(
            &codex_executor(Some("bamboo"), None),
            None,
            "child-no-authority",
        )
        .err()
        .expect("bamboo mode without an authority must fail closed");
        assert!(error.to_string().contains("per-run token authority"));
    }

    #[test]
    fn codex_provisioning_never_leaks_the_session_provider_credential() {
        let credentials = vec![ScopedCredential {
            provider: "openai".to_string(),
            api_key: "upstream-secret-must-not-cross".to_string(),
            base_url: None,
            provider_type: Some("openai".to_string()),
            credential_ref: Some("provider.openai.api_key".to_string()),
        }];

        for (mode, label) in [
            (Some("inherit"), "inherit"),
            (Some("api_key"), "api_key"),
            (Some("bamboo"), "bamboo"),
            (None, "default-bamboo"),
        ] {
            let runner = ActorChildRunner::new(
                format!("codex-{label}-test"),
                PathBuf::from("/bin/false"),
                Vec::new(),
                std::env::temp_dir().join(format!("bamboo-codex-{label}-570")),
                codex_executor(mode, None),
                credentials.clone(),
                "openai".to_string(),
                1,
            );
            let mut session = Session::new(format!("child-{label}"), "model");
            session.add_message(bamboo_agent_core::Message::user("test"));
            let spec = runner.build_spec(
                &session,
                &crate::runtime::execution::SpawnJob {
                    parent_session_id: "parent".to_string(),
                    child_session_id: format!("child-{label}"),
                    model: "gpt-5.4".to_string(),
                    disabled_tools: None,
                },
            );
            assert!(
                spec.secrets.provider_credentials.is_empty(),
                "{label} Codex must not receive the session provider key"
            );
        }
    }

    #[test]
    fn non_codex_provisioning_still_receives_only_its_selected_provider_credential() {
        let credentials = vec![
            ScopedCredential {
                provider: "openai".to_string(),
                api_key: "selected-openai-secret".to_string(),
                base_url: None,
                provider_type: Some("openai".to_string()),
                credential_ref: Some("provider.openai.api_key".to_string()),
            },
            ScopedCredential {
                provider: "other".to_string(),
                api_key: "unrelated-secret".to_string(),
                base_url: None,
                provider_type: Some("openai".to_string()),
                credential_ref: Some("provider.other.api_key".to_string()),
            },
        ];
        let runner = ActorChildRunner::new(
            "echo-test".to_string(),
            PathBuf::from("/bin/false"),
            Vec::new(),
            std::env::temp_dir().join("bamboo-echo-provider-570"),
            ExecutorSpec::Echo,
            credentials,
            "openai".to_string(),
            1,
        );
        let spec = runner.build_spec(
            &Session::new("child-echo", "model"),
            &crate::runtime::execution::SpawnJob {
                parent_session_id: "parent".to_string(),
                child_session_id: "child-echo".to_string(),
                model: "gpt-5.4".to_string(),
                disabled_tools: None,
            },
        );

        assert_eq!(spec.secrets.provider_credentials.len(), 1);
        assert_eq!(
            spec.secrets.provider_credentials[0].api_key,
            "selected-openai-secret"
        );
    }

    #[test]
    fn custom_codex_provisioning_scopes_only_the_referenced_credential() {
        let mut executor = codex_executor(Some("custom"), None);
        if let ExecutorSpec::Codex {
            base_url,
            provider_key_ref,
            ..
        } = &mut executor
        {
            *base_url = Some("https://provider.example/v1".to_string());
            *provider_key_ref = Some("provider.custom.api_key".to_string());
        }
        let credentials = vec![
            ScopedCredential {
                provider: "openai".to_string(),
                api_key: "session-provider-secret".to_string(),
                base_url: None,
                provider_type: Some("openai".to_string()),
                credential_ref: Some("provider.openai.api_key".to_string()),
            },
            ScopedCredential {
                provider: "custom".to_string(),
                api_key: "selected-secret".to_string(),
                base_url: None,
                provider_type: Some("openai".to_string()),
                credential_ref: Some("provider.custom.api_key".to_string()),
            },
            ScopedCredential {
                provider: "other".to_string(),
                api_key: "unrelated-secret".to_string(),
                base_url: None,
                provider_type: Some("openai".to_string()),
                credential_ref: Some("provider.other.api_key".to_string()),
            },
        ];
        let runner = ActorChildRunner::new(
            "codex-test".to_string(),
            PathBuf::from("/bin/false"),
            Vec::new(),
            std::env::temp_dir().join("bamboo-codex-570"),
            executor,
            credentials,
            "openai".to_string(),
            1,
        );
        let mut session = Session::new("child-custom", "model");
        session.add_message(bamboo_agent_core::Message::user("test"));
        let spec = runner.build_spec(
            &session,
            &crate::runtime::execution::SpawnJob {
                parent_session_id: "parent".to_string(),
                child_session_id: "child-custom".to_string(),
                model: "gpt-5.4".to_string(),
                disabled_tools: None,
            },
        );

        assert_eq!(spec.secrets.provider_credentials.len(), 1);
        assert_eq!(
            spec.secrets.provider_credentials[0]
                .credential_ref
                .as_deref(),
            Some("provider.custom.api_key")
        );
        assert_eq!(
            spec.secrets.provider_credentials[0].api_key,
            "selected-secret"
        );
    }

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
    fn logical_identity_is_invariant_across_local_remote_scheduled_and_warm_reuse() {
        let mut session =
            Session::new_child("logical-child-681", "logical-parent-681", "model", "child");
        session.root_session_id = "logical-root-681".to_string();
        let job = SpawnJob {
            parent_session_id: "logical-parent-681".to_string(),
            child_session_id: "logical-child-681".to_string(),
            model: "model".to_string(),
            disabled_tools: None,
        };
        let expected = LogicalSessionIdentity {
            session_id: "logical-child-681".to_string(),
            parent_session_id: Some("logical-parent-681".to_string()),
            root_session_id: "logical-root-681".to_string(),
        };

        let placements_and_transport_ids = [
            (Placement::Local, "local-mailbox-first"),
            (
                Placement::Remote {
                    endpoint: "wss://remote.example/actor".to_string(),
                },
                "remote-process-44",
            ),
            (
                Placement::Schedulable {
                    pool: "gpu-pool".to_string(),
                },
                "scheduled-mailbox-9",
            ),
            // Same logical child reactivated on a different pooled mailbox.
            (Placement::Local, "warm-mailbox-reused-77"),
        ];
        for (placement, transport_id) in placements_and_transport_ids {
            let mut provision = spec_with("worker", "provider", "model", None, None);
            provision.placement = placement;
            provision.identity.child_id = transport_id.to_string();
            assert_eq!(logical_identity_for_actor_run(&session, &job), expected);
            assert_ne!(
                provision.identity.child_id, expected.session_id,
                "test fixture must prove transport identity is independent"
            );
        }
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

    #[test]
    fn fingerprint_splits_codex_exec_and_app_server_workers() {
        let mut exec = spec_with("explorer", "p", "m", Some("/ws"), None);
        exec.executor = codex_executor(Some("inherit"), None);
        let mut app_server = exec.clone();
        if let ExecutorSpec::Codex { mode, .. } = &mut app_server.executor {
            *mode = Some("app_server".to_string());
        }
        assert_ne!(
            ActorChildRunner::fingerprint(&exec),
            ActorChildRunner::fingerprint(&app_server)
        );
    }

    struct StaticDecider(bool);

    #[async_trait]
    impl ChildApprovalDecider for StaticDecider {
        async fn decide(&self, _child: &str, _req: &serde_json::Value) -> bool {
            self.0
        }
    }

    struct RecordingReviewer {
        reviewed: mpsc::UnboundedSender<(String, String, serde_json::Value)>,
    }

    #[async_trait]
    impl ChildApprovalReviewer for RecordingReviewer {
        async fn review(&self, parent: &str, child: &str, request: &serde_json::Value) -> bool {
            let _ = self
                .reviewed
                .send((parent.to_string(), child.to_string(), request.clone()));
            true
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
        async fn next_frame(&mut self) -> bamboo_subagent::TransportResult<Option<ChildFrame>> {
            std::future::pending().await
        }
    }

    /// A link that immediately yields one terminal frame (a healthy fast worker).
    struct InstantTerminalLink {
        done: bool,
    }

    struct ApprovalRoundTripLink {
        step: u8,
        approval_reply: Option<(String, bool)>,
    }

    #[async_trait]
    impl bamboo_subagent::ChildLink for ApprovalRoundTripLink {
        async fn send(&mut self, frame: ParentFrame) -> bamboo_subagent::TransportResult<()> {
            if let ParentFrame::ApprovalReply { id, approved } = frame {
                self.approval_reply = Some((id, approved));
                self.step = 2;
            }
            Ok(())
        }

        async fn next_frame(&mut self) -> bamboo_subagent::TransportResult<Option<ChildFrame>> {
            match self.step {
                0 => {
                    self.step = 1;
                    Ok(Some(ChildFrame::ApprovalRequest {
                        id: "approval-1".into(),
                        body: serde_json::json!({
                            "tool_name": "Bash",
                            "permission": "execute",
                            "resource": "rm -rf target",
                            "permission_request": {"reason_code": "hard_dangerous"}
                        }),
                    }))
                }
                1 => std::future::pending().await,
                2 => {
                    self.step = 3;
                    Ok(Some(ChildFrame::Terminal {
                        status: TerminalStatus::Completed,
                        result: Some("done".into()),
                        error: None,
                        transcript: vec![],
                    }))
                }
                _ => std::future::pending().await,
            }
        }
    }

    #[tokio::test]
    async fn drive_routes_forced_ask_to_parent_reviewer_without_human_event() {
        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(8);
        let (review_tx, mut review_rx) = mpsc::unbounded_channel();
        let reviewer: Arc<dyn ChildApprovalReviewer> = Arc::new(RecordingReviewer {
            reviewed: review_tx,
        });
        let cancel = CancellationToken::new();
        let (live_tx, mut live_rx) = mpsc::unbounded_channel::<ParentFrame>();
        let (_delivery_tx, mut delivery_rx) = mpsc::unbounded_channel();
        let mut logical_session = Session::new("child-reviewer", "model");
        let live_guard = crate::external_agents::live::register("child-reviewer", live_tx, 0, None);
        let mut link = ApprovalRoundTripLink {
            step: 0,
            approval_reply: None,
        };

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            drive(ActorDriveContext {
                client: &mut link,
                parent_session_id: "parent-reviewer",
                child_session_id: "child-reviewer",
                child_attempt: 0,
                approval_registry: None,
                approval_decider: None,
                approval_reviewer: Some(&reviewer),
                escalation_bridge: None,
                event_tx: &event_tx,
                cancel_token: &cancel,
                live_rx: &mut live_rx,
                delivery_rx: &mut delivery_rx,
                logical_session: &mut logical_session,
                session_inbox_runtime: None,
                activation_run_id: None,
                initial_inflight_claims: VecDeque::new(),
                first_frame_timeout: None,
            }),
        )
        .await
        .expect("worker must receive the reviewer verdict before terminating");

        assert_eq!(result.ok().flatten().as_deref(), Some("done"));
        assert_eq!(
            link.approval_reply,
            Some(("approval-1".to_string(), true)),
            "reviewer verdict must traverse the live route back to the worker"
        );
        let (parent, child, body) = tokio::time::timeout(Duration::from_secs(1), review_rx.recv())
            .await
            .expect("reviewer should be invoked off-loop")
            .expect("review channel should remain open");
        assert_eq!(parent, "parent-reviewer");
        assert_eq!(child, "child-reviewer");
        assert_eq!(
            body.pointer("/permission_request/reason_code")
                .and_then(serde_json::Value::as_str),
            Some("hard_dangerous")
        );
        assert!(
            event_rx.try_recv().is_err(),
            "must not emit a human-review event"
        );
        drop(live_guard);
    }

    #[tokio::test]
    async fn drive_denies_forced_ask_without_parent_reviewer_or_manual_event() {
        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(8);
        let cancel = CancellationToken::new();
        let (_live_tx, mut live_rx) = mpsc::unbounded_channel::<ParentFrame>();
        let (_delivery_tx, mut delivery_rx) = mpsc::unbounded_channel();
        let mut logical_session = Session::new("child-no-reviewer", "model");
        let mut link = ApprovalRoundTripLink {
            step: 0,
            approval_reply: None,
        };

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            drive(ActorDriveContext {
                client: &mut link,
                parent_session_id: "parent-no-reviewer",
                child_session_id: "child-no-reviewer",
                child_attempt: 0,
                approval_registry: None,
                approval_decider: None,
                approval_reviewer: None,
                escalation_bridge: None,
                event_tx: &event_tx,
                cancel_token: &cancel,
                live_rx: &mut live_rx,
                delivery_rx: &mut delivery_rx,
                logical_session: &mut logical_session,
                session_inbox_runtime: None,
                activation_run_id: None,
                initial_inflight_claims: VecDeque::new(),
                first_frame_timeout: None,
            }),
        )
        .await
        .expect("fail-closed reply must unblock the child immediately");

        assert_eq!(result.ok().flatten().as_deref(), Some("done"));
        assert_eq!(link.approval_reply, Some(("approval-1".to_string(), false)));
        assert!(
            event_rx.try_recv().is_err(),
            "missing parent review must not surface a manual approval event"
        );
    }
    #[async_trait]
    impl bamboo_subagent::ChildLink for InstantTerminalLink {
        async fn send(&mut self, _: ParentFrame) -> bamboo_subagent::TransportResult<()> {
            Ok(())
        }
        async fn next_frame(&mut self) -> bamboo_subagent::TransportResult<Option<ChildFrame>> {
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
        let (_delivery_tx, mut delivery_rx) = mpsc::unbounded_channel();
        let mut logical_session = Session::new("child-x", "model");
        let mut link = SilentLink;
        let r = drive(ActorDriveContext {
            client: &mut link,
            parent_session_id: "parent-x",
            child_session_id: "child-x",
            child_attempt: 0,
            approval_registry: None,
            approval_decider: None,
            approval_reviewer: None,
            escalation_bridge: None,
            event_tx: &event_tx,
            cancel_token: &cancel,
            live_rx: &mut live_rx,
            delivery_rx: &mut delivery_rx,
            logical_session: &mut logical_session,
            session_inbox_runtime: None,
            activation_run_id: None,
            initial_inflight_claims: VecDeque::new(),
            first_frame_timeout: Some(Duration::from_millis(100)),
        })
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
        let (_delivery_tx, mut delivery_rx) = mpsc::unbounded_channel();
        let mut logical_session = Session::new("child-y", "model");
        let mut link = InstantTerminalLink { done: false };
        // Even a tiny timeout must NOT trip: the terminal frame arrives first and
        // disarms the watchdog.
        let r = drive(ActorDriveContext {
            client: &mut link,
            parent_session_id: "parent-y",
            child_session_id: "child-y",
            child_attempt: 0,
            approval_registry: None,
            approval_decider: None,
            approval_reviewer: None,
            escalation_bridge: None,
            event_tx: &event_tx,
            cancel_token: &cancel,
            live_rx: &mut live_rx,
            delivery_rx: &mut delivery_rx,
            logical_session: &mut logical_session,
            session_inbox_runtime: None,
            activation_run_id: None,
            initial_inflight_claims: VecDeque::new(),
            first_frame_timeout: Some(Duration::from_millis(50)),
        })
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

    #[derive(Default)]
    struct RecordingChildSessionPort {
        saved: std::sync::Mutex<Option<Session>>,
    }

    impl RecordingChildSessionPort {
        fn saved_child(&self) -> Session {
            self.saved
                .lock()
                .expect("saved-child fixture lock")
                .clone()
                .expect("create_child_action must save the child")
        }
    }

    #[async_trait]
    impl crate::session_app::child_session::ChildSessionPort for RecordingChildSessionPort {
        async fn load_root_session(
            &self,
            _root_id: &str,
        ) -> Result<Session, crate::session_app::child_session::ChildSessionError> {
            unreachable!("create_child_action does not load the root")
        }

        async fn load_child_for_parent(
            &self,
            _parent_id: &str,
            _child_id: &str,
        ) -> Result<Session, crate::session_app::child_session::ChildSessionError> {
            unreachable!("create_child_action does not reload the child")
        }

        async fn save_child_session(
            &self,
            child: &mut Session,
        ) -> Result<(), crate::session_app::child_session::ChildSessionError> {
            *self.saved.lock().expect("saved-child fixture lock") = Some(child.clone());
            Ok(())
        }

        async fn save_child_session_authoritative_flags(
            &self,
            _child: &mut Session,
        ) -> Result<(), crate::session_app::child_session::ChildSessionError> {
            unreachable!("new-child creation uses the ordinary save")
        }

        async fn is_child_running(&self, _child_id: &str) -> bool {
            false
        }

        async fn list_children(
            &self,
            _parent_id: &str,
        ) -> Vec<crate::session_app::child_session::ChildSessionEntry> {
            Vec::new()
        }

        async fn enqueue_child_run(
            &self,
            _parent: &Session,
            _child: &Session,
        ) -> Result<(), crate::session_app::child_session::ChildSessionError> {
            unreachable!("fixture creates the child with auto_run=false")
        }

        async fn cancel_child_run_and_wait(
            &self,
            _child_id: &str,
        ) -> Result<(), crate::session_app::child_session::ChildSessionError> {
            unreachable!("create_child_action does not cancel")
        }

        async fn delete_child_session(
            &self,
            _parent_id: &str,
            _child_id: &str,
        ) -> Result<
            crate::session_app::child_session::DeleteChildResult,
            crate::session_app::child_session::ChildSessionError,
        > {
            unreachable!("create_child_action does not delete")
        }

        async fn get_child_runner_info(
            &self,
            _child_id: &str,
        ) -> Option<crate::session_app::child_session::ChildRunnerInfo> {
            None
        }

        async fn register_parent_wait_for_child(
            &self,
            _parent_session_id: &str,
            _child_session_id: &str,
            _tool_call_id: Option<&str>,
        ) -> Result<(), crate::session_app::child_session::ChildSessionError> {
            unreachable!("create_child_action does not register a wait")
        }

        async fn register_parent_wait_for_children(
            &self,
            _parent_session_id: &str,
            _child_session_ids: &[String],
            _policy: bamboo_domain::session::runtime_state::ChildWaitPolicy,
        ) -> Result<usize, crate::session_app::child_session::ChildSessionError> {
            unreachable!("create_child_action does not register a wait")
        }

        async fn active_child_ids(&self, _parent_session_id: &str) -> Vec<String> {
            Vec::new()
        }

        async fn find_resident_child(
            &self,
            _root_session_id: &str,
            _resident_name: &str,
        ) -> Option<String> {
            None
        }

        async fn ensure_child_indexed(&self, _child_session_id: &str) {}
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

    #[tokio::test]
    async fn build_spec_preserves_inherited_bypass_for_child_worker() {
        // Exercise the real creation path instead of pre-seeding the child by
        // hand: a bypassed parent's posture must survive both child creation and
        // the actor provisioning boundary.
        let runner = bogus_runner(HashMap::new());
        let mut parent = Session::new("parent-bypass", "test-model");
        parent
            .agent_runtime_state
            .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
            .bypass_permissions = true;
        let workspace = tempfile::tempdir().expect("workspace fixture");
        let port = RecordingChildSessionPort::default();
        let child_id = format!("child-bypass-{}", uuid::Uuid::new_v4());
        crate::session_app::child_session::create_child_action(
            &port,
            crate::session_app::child_session::CreateChildInput {
                parent_session: parent,
                child_id: child_id.clone(),
                title: "Bypassed child".to_string(),
                responsibility: "Run ordinary commands".to_string(),
                assignment_prompt: "run an ordinary command".to_string(),
                subagent_type: "explorer".to_string(),
                workspace: workspace.path().to_string_lossy().into_owned(),
                workspace_source: crate::project_context::WorkspaceSource::Explicit,
                model_override: None,
                model_ref_override: None,
                runtime_metadata: HashMap::new(),
                auto_run: false,
                reasoning_effort: None,
                lifecycle: None,
                resident_name: None,
                resident_context: None,
                disabled_tools: None,
                context_fork: None,
            },
        )
        .await
        .expect("create inherited-bypass child");
        let child = port.saved_child();

        assert!(
            child
                .agent_runtime_state
                .as_ref()
                .is_some_and(|state| state.bypass_permissions),
            "create_child_action must inherit bypass from the parent"
        );

        let spec = runner.build_spec(&child, &job_for(&child_id));

        assert!(spec.capabilities.bypass, "child worker must inherit bypass");
        assert!(
            spec.capabilities.enforce_permissions,
            "forced-ask evaluation must remain active under bypass"
        );
    }

    #[tokio::test]
    async fn child_resident_and_guardian_inherit_project_through_actor_run_spec() {
        let project_id = bamboo_domain::ProjectId::parse("project-inherited").expect("Project id");
        let workspace = tempfile::tempdir().expect("workspace fixture");

        for (role, lifecycle, resident_name) in [
            ("explorer", None, None),
            ("resident", Some("resident"), Some("stable-reviewer")),
            ("guardian", None, None),
        ] {
            let mut parent = Session::new(format!("parent-{role}"), "test-model");
            parent.set_project_id_meta(project_id.to_string());
            let port = RecordingChildSessionPort::default();
            let child_id = format!("child-{role}-{}", uuid::Uuid::new_v4());
            crate::session_app::child_session::create_child_action(
                &port,
                crate::session_app::child_session::CreateChildInput {
                    parent_session: parent,
                    child_id: child_id.clone(),
                    title: format!("{role} child"),
                    responsibility: "Review the assigned work".to_string(),
                    assignment_prompt: "inspect the change".to_string(),
                    subagent_type: role.to_string(),
                    workspace: workspace.path().to_string_lossy().into_owned(),
                    workspace_source: crate::project_context::WorkspaceSource::Explicit,
                    model_override: None,
                    model_ref_override: None,
                    runtime_metadata: HashMap::new(),
                    auto_run: false,
                    reasoning_effort: None,
                    lifecycle: lifecycle.map(str::to_string),
                    resident_name: resident_name.map(str::to_string),
                    resident_context: None,
                    disabled_tools: None,
                    context_fork: None,
                },
            )
            .await
            .expect("create Project-inheriting child");
            let child = port.saved_child();
            assert_eq!(
                crate::project_context::ProjectContextResolver::project_id_from_session(&child),
                Some(project_id.clone()),
                "{role} child must inherit its parent's Project"
            );

            assert_eq!(
                project_id_for_actor_run(&child).expect("valid actor Project identity"),
                Some(project_id.clone()),
                "{role} actor RunSpec must preserve inherited Project identity"
            );
        }
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
        let s = placement_metadata(
            &Placement::Schedulable {
                pool: "explorers".into(),
            },
            Some("mini"),
        )
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

    fn sched_placement(
        pool: &str,
        _registry_url: impl Into<String>,
    ) -> ResolvedSchedulablePlacement {
        ResolvedSchedulablePlacement {
            pool: pool.into(),
            host_label: None,
        }
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
        let stamp = runner
            .placement_stamp_for(&spec)
            .expect("remote child is stamped");
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
        assert!(r2
            .placement_stamp_for(&spec2)
            .unwrap()
            .contains(r#""host":"169.254.230.101""#));

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
        let stamp3 = sr
            .placement_stamp_for(&spec3)
            .expect("scheduled child is stamped");
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
            bamboo_subagent::AgentRef {
                session_id: "probe".into(),
                role: None,
            },
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
            bamboo_subagent::BusEndpoint {
                endpoint: endpoint.clone(),
                token: "t".into(),
            },
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
