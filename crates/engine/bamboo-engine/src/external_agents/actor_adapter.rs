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

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::placement_scheduler::PlacementScheduler;
use async_trait::async_trait;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::tool_start_arguments_for_display;
use bamboo_agent_core::{AgentError, AgentEvent, Role, Session};
use bamboo_domain::poison::PoisonRecover;
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFence, ActorActivationFinish, ActorDirectoryPort,
    ActorLogicalState, ActorSnapshotLimits, ActorSnapshotPort, ActorSnapshotPrincipal, HookResult,
    ParentQuestion, ParentQuestionCheckpointV1, SessionInboxClaim, SessionInboxConsumerId,
    SessionInboxFailureReport, SessionInboxLeaseRequest, SessionInboxOwnedClaim, SessionInboxPort,
    PARENT_QUESTION_CHECKPOINT_ACTION,
};
use bamboo_domain::{HostPlacementIntent, HostPlacementRequest, WorkerSlotLease};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_subagent::fleet::{spawn_worker_on_bus, SpawnedChild};
use bamboo_subagent::proto::{
    ActorEventBatch, AgentRecord, ChildFrame, LogicalSessionIdentity, ParentFrame,
    PermissionPolicyContext, RunSpec, SessionMessageDelivery, TerminalStatus,
};
use bamboo_subagent::provision::{
    ChildIdentity, ExecutorSpec, ModelRefSpec, Placement, ProvisionSpec, ScopedCredential,
};

use super::actor_event_router::{
    ActorEventClass, ActorEventRoute, ActorEventRouteError, ActorEventRouter,
};
use super::actor_event_stream::{ActorEventObserver, PublicActorEvent};
use crate::runtime::execution::{ExternalChildRunner, SessionInboxRuntimeBinding, SpawnJob};

/// Default cap on simultaneously running actor activations. The event and
/// broker transports are bounded independently, so 200 active sub-agents are a
/// supported operating point rather than an opt-in escape hatch.
pub const DEFAULT_MAX_CONCURRENT_ACTORS: usize = 200;

/// Max nesting depth for direct nested execution (Phase 6). A worker whose
/// session `spawn_depth` is below this gets its own spawn stack + the real
/// SubAgent tool; at/over it, neither (and the tool itself refuses). Mirrors
/// `bamboo_server_tools::DEFAULT_MAX_SPAWN_DEPTH` (kept in sync; engine can't
/// depend on server-tools). Root orchestrator = 0 ⇒ 4 levels of sub-agents.
pub const MAX_SPAWN_DEPTH: u32 = 4;

/// Default cap on idle pooled (warm, reusable) workers kept per fingerprint.
const DEFAULT_MAX_IDLE_PER_KEY: usize = 4;

/// Process-wide-per-runner cap across every reuse fingerprint. Without this,
/// workloads that continually change role/model/workspace can leave one parked
/// worker in an unbounded number of otherwise-small buckets.
const DEFAULT_MAX_IDLE_TOTAL: usize = 16;

/// How long a pooled worker waits for its next assignment before reclaiming
/// itself (must comfortably exceed the gap between sibling spawns).
const POOLED_IDLE_TIMEOUT_SECS: u64 = 300;

/// Sweep often enough to retire an expired/dead pool entry even when no later
/// child checks out the same fingerprint. The worker's own idle deadline is the
/// primary process-side guard; this task closes the parent handle and pool entry.
const POOLED_REAPER_INTERVAL: Duration = Duration::from_secs(30);

/// Deadline for a local worker's FIRST frame after a Run is dispatched. A warm
/// worker answers in seconds; a cold spawn within tens. Total silence past this
/// means the worker is dead (e.g. a pooled worker that exited right after its
/// liveness check) and its Run is queued with nobody to serve it — trip it so the
/// runner respawns once instead of hanging forever. Generous, to never false-trip
/// a slow-but-healthy cold start.
const WORKER_FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(60);

fn active_scoped_session_deny_count(
    config: &bamboo_tools::permission::PermissionConfig,
    session_id: &str,
) -> usize {
    config
        .temporary_grants()
        .into_iter()
        .filter(|grant| {
            grant.scope == bamboo_tools::permission::TemporaryPermissionGrantScope::Session
                && grant.effect == bamboo_tools::permission::TemporaryPermissionGrantEffect::Deny
                && grant.session_id.as_deref() == Some(session_id)
        })
        .count()
}

fn ensure_no_active_scoped_session_denies(
    config: &bamboo_tools::permission::PermissionConfig,
    session_id: &str,
) -> Result<(), AgentError> {
    let count = active_scoped_session_deny_count(config, session_id);
    if count == 0 {
        Ok(())
    } else {
        Err(AgentError::LLM(format!(
            "external executor activation blocked by {count} active session-scoped explicit deny rule(s)"
        )))
    }
}

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

fn executor_has_read_only_permission_profile(executor: &ExecutorSpec) -> bool {
    match executor {
        ExecutorSpec::ClaudeCode {
            permission_mode, ..
        } => permission_mode
            .as_deref()
            .is_some_and(|mode| mode.eq_ignore_ascii_case("plan")),
        ExecutorSpec::Codex {
            permission_profile,
            sandbox,
            ..
        } => {
            permission_profile
                .as_deref()
                .is_some_and(|profile| profile.eq_ignore_ascii_case("read-only"))
                || sandbox
                    .as_deref()
                    .is_some_and(|value| value.eq_ignore_ascii_case("read-only"))
        }
        _ => false,
    }
}

/// Exact non-secret executor posture the host expects for one typed activation.
///
/// Echo is intentionally a transport-only smoke executor and CliAdapter is not
/// implemented by the production worker. Neither claims the typed permission
/// contract, so legacy/custom frame handling remains available only for those
/// two variants. Every executable permission-aware variant must prove the
/// mapping derived from its provisioned spec before any execution event.
fn expected_permission_executor_mapping(
    executor: &ExecutorSpec,
    resolution: bamboo_domain::PermissionModeResolution,
    has_explicit_deny: bool,
) -> Result<Option<String>, AgentError> {
    let mapping = match executor {
        ExecutorSpec::Echo | ExecutorSpec::CliAdapter { .. } => return Ok(None),
        ExecutorSpec::BambooRuntime => {
            format!("bamboo_runtime:{}", resolution.effective.as_str())
        }
        ExecutorSpec::ClaudeCode {
            permission_mode, ..
        } => {
            if has_explicit_deny {
                "claude_code:blocked_explicit_deny".to_string()
            } else {
                let mode = match resolution.effective {
                    bamboo_domain::PermissionMode::Plan => "plan",
                    bamboo_domain::PermissionMode::Auto => "bypassPermissions",
                    bamboo_domain::PermissionMode::AcceptEdits => "acceptEdits",
                    bamboo_domain::PermissionMode::DontAsk => "dontAsk",
                    bamboo_domain::PermissionMode::Default
                    | bamboo_domain::PermissionMode::BypassPermissions => {
                        permission_mode.as_deref().unwrap_or("default")
                    }
                };
                format!("claude_code:permission_mode={mode}")
            }
        }
        ExecutorSpec::Codex {
            mode,
            sandbox,
            approval_policy,
            allow_danger_bypass,
            ..
        } => match mode.as_deref().unwrap_or("exec") {
            "exec" => {
                let approval_policy = expected_codex_exec_approval_policy(
                    sandbox.as_deref(),
                    approval_policy.as_deref(),
                    allow_danger_bypass.unwrap_or(false),
                    resolution,
                )?;
                if has_explicit_deny {
                    "codex_exec:blocked_explicit_deny".to_string()
                } else {
                    format!("codex_exec:approval_policy={approval_policy}")
                }
            }
            "app_server" => {
                if !matches!(approval_policy.as_deref(), None | Some("on-request")) {
                    return Err(AgentError::LLM(
                        "invalid Codex app-server permission posture configuration".to_string(),
                    ));
                }
                if has_explicit_deny {
                    "codex_app_server:blocked_explicit_deny".to_string()
                } else {
                    let approval_policy = if resolution.suppress_approval_prompts()
                        || resolution.effective == bamboo_domain::PermissionMode::Plan
                    {
                        "never"
                    } else {
                        "on-request"
                    };
                    format!("codex_app_server:approvalPolicy={approval_policy}")
                }
            }
            _ => {
                return Err(AgentError::LLM(
                    "unsupported Codex executor mode for permission posture contract".to_string(),
                ));
            }
        },
    };
    Ok(Some(mapping))
}

fn expected_codex_exec_approval_policy(
    sandbox: Option<&str>,
    approval_policy: Option<&str>,
    allow_danger_bypass: bool,
    resolution: bamboo_domain::PermissionModeResolution,
) -> Result<&'static str, AgentError> {
    let configured = match approval_policy {
        None | Some("never") => "never",
        Some("on-failure") => "on-failure",
        Some(_) => {
            return Err(AgentError::LLM(
                "invalid Codex exec permission posture configuration".to_string(),
            ));
        }
    };
    if resolution.suppress_approval_prompts()
        || resolution.effective == bamboo_domain::PermissionMode::Plan
    {
        return Ok("never");
    }
    match sandbox {
        Some("danger-full-access") => Ok("never"),
        Some("read-only") | Some("workspace-write") => Ok(configured),
        None if allow_danger_bypass || resolution.bypass_permissions() => Ok("never"),
        None => Ok(configured),
        Some(_) => Err(AgentError::LLM(
            "invalid Codex exec permission posture configuration".to_string(),
        )),
    }
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
    /// Set only while the worker is parked. Checked both at checkout and by the
    /// background pool sweep; cleared before the worker is handed to a run.
    parked_at: Option<Instant>,
}

type WorkerPool = HashMap<String, Vec<PooledWorker>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolReapReason {
    ProcessExited,
    IdleTimeout,
}

impl PoolReapReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::ProcessExited => "process_exited_while_parked",
            Self::IdleTimeout => "pool_idle_timeout",
        }
    }
}

fn parked_worker_expired(parked_at: Option<Instant>, now: Instant, timeout: Duration) -> bool {
    parked_at.is_some_and(|parked_at| now.saturating_duration_since(parked_at) >= timeout)
}

fn idle_pool_limit_reason(
    pool: &WorkerPool,
    key: &str,
    max_per_key: usize,
    max_total: usize,
) -> Option<&'static str> {
    if pool.get(key).map_or(0, Vec::len) >= max_per_key {
        Some("pool_per_key_limit")
    } else if pool.values().map(Vec::len).sum::<usize>() >= max_total {
        Some("pool_global_limit")
    } else {
        None
    }
}

fn take_reapable_workers(
    pool: &mut WorkerPool,
    now: Instant,
    timeout: Duration,
) -> Vec<(String, PoolReapReason, PooledWorker)> {
    let mut reaped = Vec::new();
    for (key, bucket) in pool.iter_mut() {
        let mut retained = Vec::with_capacity(bucket.len());
        for mut worker in std::mem::take(bucket) {
            let reason = if !worker.worker.is_alive() {
                Some(PoolReapReason::ProcessExited)
            } else if parked_worker_expired(worker.parked_at, now, timeout) {
                Some(PoolReapReason::IdleTimeout)
            } else {
                None
            };
            if let Some(reason) = reason {
                reaped.push((key.clone(), reason, worker));
            } else {
                retained.push(worker);
            }
        }
        *bucket = retained;
    }
    pool.retain(|_, bucket| !bucket.is_empty());
    reaped
}

async fn reap_worker_pool_once(pool: &Arc<tokio::sync::Mutex<WorkerPool>>) -> usize {
    let now = Instant::now();
    let timeout = Duration::from_secs(POOLED_IDLE_TIMEOUT_SECS);
    let reaped = {
        let mut pool = pool.lock().await;
        take_reapable_workers(&mut pool, now, timeout)
    };
    let count = reaped.len();
    for (pool_key, reason, worker) in reaped {
        let idle_ms = worker
            .parked_at
            .map(|parked_at| now.saturating_duration_since(parked_at).as_millis() as u64);
        tracing::info!(
            pool_key,
            mailbox_id = %worker.mailbox_id,
            worker_pid = ?worker.worker.pid(),
            worker_idle_ms = ?idle_ms,
            shutdown_reason = reason.as_str(),
            "reaping parked sub-agent worker"
        );
        worker.worker.kill().await;
    }
    count
}

/// A role pinned to a remote resident worker (remote-actor-plan §3.4 / P1.5,
/// #193), resolved at runner-build time from `SubagentsConfig.remote_placements`:
/// the env-named broker bearer is already READ into `token` here (the raw token
/// never rides the config), and `ca_cert_file` pins the broker's TLS cert.
#[derive(Clone, Default)]
pub struct ResolvedRemotePlacement {
    pub endpoint: String,
    pub token: Option<String>,
    pub ca_cert_file: Option<PathBuf>,
    /// Display name for the machine this role runs on — the matching cluster
    /// node's `label`/host, surfaced on the UI placement badge. `None` ⇒ derive
    /// from the endpoint host.
    pub host_label: Option<String>,
    /// Some(Err) preserves explicit unavailable selection; never falls back.
    pub broker_peer: Option<Result<bamboo_config::RemoteBrokerPeer, ()>>,
    pub requirements: Option<bamboo_config::OperatorPlacementRequirements>,
}
impl std::fmt::Debug for ResolvedRemotePlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedRemotePlacement")
            .field("broker_peer", &self.broker_peer.is_some())
            .finish_non_exhaustive()
    }
}

/// A role routed to a SCHEDULED worker (remote-actor-plan §3.4 / P2b, #181),
/// resolved at runner-build time from `SubagentsConfig.schedulable_placements`.
/// Names the logical `pool` (= the bus role) whose LIVE connected workers are the
/// scheduling candidates — the runner picks one via the bus presence query
/// (`BrokerClient::list_connected`). Phase 3 retired the old HTTP registry, so a
/// pool is now just a role on the bus.
#[derive(Debug, Clone, Default)]
pub struct ResolvedSchedulablePlacement {
    pub pool: String,
    /// Display name for the machine this pool's workers run on — the matching
    /// cluster node's `label`/host, surfaced on the UI placement badge. `None` ⇒
    /// fall back to the pool name.
    pub host_label: Option<String>,
    pub endpoint: String,
    pub token: Option<String>,
    pub ca_cert_file: Option<PathBuf>,
    pub broker_parent: Option<bamboo_config::SchedulableBrokerParent>,
    pub requirements: Option<bamboo_config::OperatorPlacementRequirements>,
}

#[derive(Clone)]
struct ScopedPlacementRoute {
    endpoint: String,
    token: String,
    ca_cert_file: PathBuf,
    parent: bamboo_subagent::AgentRef,
    worker_role: String,
    pinned_mailbox: Option<String>,
    requirements: bamboo_config::OperatorPlacementRequirements,
}

impl ScopedPlacementRoute {
    async fn query(&self) -> Result<bamboo_broker::BrokerClient, AgentError> {
        let tls = bamboo_broker::client_config_trusting_cert(&self.ca_cert_file)
            .map_err(|_| remote_broker_unavailable())?;
        bamboo_broker::BrokerClient::connect_with_tls(
            &self.endpoint,
            self.parent.clone(),
            &self.token,
            Some(tls),
        )
        .await
        .map_err(|_| remote_broker_unavailable())
    }
}

/// Holds one atomic HostRegistry slot for the entire remote Run. A fresh
/// authenticated broker observation extends the Host lease before the slot is
/// renewed. Losing either identity cancels the Run; Drop releases the exact
/// generation/epoch and cannot free a replacement worker's slot.
struct ActivePlacementLease {
    registry: bamboo_storage::v2::FileHostRegistry,
    actor_store: Arc<bamboo_storage::SessionStoreV2>,
    route: ScopedPlacementRoute,
    mailbox: String,
    host_ref: String,
    fence: ActorActivationFence,
    placement_ref: bamboo_domain::ActorPlacementRef,
    lease: Arc<tokio::sync::Mutex<WorkerSlotLease>>,
    heartbeat: tokio::task::JoinHandle<()>,
    run_cancel: CancellationToken,
    lost: Arc<AtomicBool>,
    release_on_drop: AtomicBool,
}

/// No Run has reached a worker yet, so a failed placement preflight may end
/// only the activation it claimed. A replacement activation has a different
/// fence and must remain untouched.
async fn fail_pre_dispatch_activation(
    actor_store: &bamboo_storage::SessionStoreV2,
    fence: &ActorActivationFence,
) -> Result<(), bamboo_domain::ActorDirectoryError> {
    actor_store
        .finish_activation(fence, chrono::Utc::now(), ActorActivationFinish::Failed)
        .await
        .map(|_| ())
}

/// Once the Run send is invoked, an error may mean that the broker persisted
/// the FencedRun but its delivery receipt was lost. Only errors before this
/// call can safely roll back the Actor activation and its reserved slot.
async fn send_actor_run(
    client: &mut dyn bamboo_subagent::ChildLink,
    spec: RunSpec,
    send_invoked: &mut bool,
) -> Result<(), AgentError> {
    *send_invoked = true;
    client
        .send(ParentFrame::Run(spec))
        .await
        .map_err(|error| AgentError::LLM(format!("actor run dispatch failed: {error}")))
}

fn remote_activation_finish(
    result: &Result<Option<String>, AgentError>,
    accepted_terminal: Option<TerminalStatus>,
) -> ActorActivationFinish {
    if result.is_ok() {
        ActorActivationFinish::Succeeded
    } else if matches!(result, Err(AgentError::Cancelled))
        && accepted_terminal == Some(TerminalStatus::Cancelled)
    {
        ActorActivationFinish::Cancelled
    } else {
        ActorActivationFinish::Failed
    }
}

/// Commit a remote reply through the current Actor fence before marking the
/// activation terminal. The SDK's later status save may then carry exactly
/// the canonical main transcript instead of trying an unfenced append.
async fn append_fenced_remote_reply(
    actor_store: &bamboo_storage::SessionStoreV2,
    fence: &ActorActivationFence,
    session: &mut Session,
    text: &str,
) -> Result<(), AgentError> {
    let message = bamboo_agent_core::Message::assistant(text, None);
    let committed = actor_store
        .append_actor_transcript(bamboo_storage::ActorTranscriptAppend {
            fence: fence.clone(),
            expected_created_at: session.created_at,
            expected_messages: session.messages.clone(),
            expected_provider_transcript: session.provider_transcript.clone(),
            messages: vec![message],
            native_groups: Vec::new(),
        })
        .await
        .map_err(|error| AgentError::LLM(format!("remote actor reply commit failed: {error}")))?;
    session.messages = committed.messages;
    session.provider_transcript = committed.provider_transcript;
    session.updated_at = committed.updated_at;
    Ok(())
}

impl ActivePlacementLease {
    async fn reserve(
        registry: bamboo_storage::v2::FileHostRegistry,
        actor_store: Arc<bamboo_storage::SessionStoreV2>,
        route: ScopedPlacementRoute,
        mailbox: String,
        actor_id: String,
        run_id: String,
        project_id: Option<bamboo_domain::ProjectId>,
        required_tools: BTreeSet<String>,
        parent_cancel: &CancellationToken,
    ) -> Result<Self, AgentError> {
        let scheduler = PlacementScheduler::new(registry.clone());
        let mut query = route.query().await?;
        let observed = query
            .observe_host_capacity(&mailbox, &route.worker_role)
            .await
            .map_err(|_| remote_broker_unavailable())?
            .ok_or_else(remote_broker_unavailable)?;
        let host = scheduler
            .observe_scoped_host(
                &mut query,
                &observed.host_ref,
                &mailbox,
                &route.worker_role,
                chrono::Duration::seconds(110),
            )
            .await
            .map_err(|_| remote_broker_unavailable())?;
        if host.capabilities.placement_class != bamboo_domain::ActorPlacementClass::Remote
            || host.connection_generation != observed.connection_generation
            || route
                .pinned_mailbox
                .as_deref()
                .is_some_and(|pinned| pinned != mailbox)
        {
            return Err(remote_broker_unavailable());
        }
        let now = chrono::Utc::now();
        let request = HostPlacementRequest {
            intent: HostPlacementIntent::PinnedHost {
                host_ref: host.host_ref.clone(),
            },
            actor_id,
            run_id,
            project_id: project_id.clone(),
            trust_zone: route.requirements.trust_zone.clone(),
            workspace_label: route.requirements.workspace_label.clone(),
            executor: "bamboo-runtime".into(),
            required_tools,
            network_zone: route.requirements.network_zone.clone(),
            require_network_isolation: route.requirements.require_network_isolation,
            preferred_host_ref: None,
            now,
            lease_expires_at: now + chrono::Duration::seconds(80),
        };
        if !request.eligible(&host) {
            return Err(remote_broker_unavailable());
        }
        let lease = scheduler
            .reserve(request)
            .await
            .map_err(|_| remote_broker_unavailable())?;
        let placement_ref = bamboo_domain::ActorPlacementRef {
            class: bamboo_domain::ActorPlacementClass::Remote,
            lease_id: lease.lease_id.clone(),
            slot_epoch: Some(lease.epoch),
        };
        let mut claimed_fence = None;
        let actor_binding = async {
            let actor = actor_store
                .ensure_actor(&lease.actor_id)
                .await
                .map_err(|_| remote_broker_unavailable())?;
            if actor.actor.project_id != project_id.as_ref().map(|id| id.to_string()) {
                return Err(remote_broker_unavailable());
            }
            let placement_intent = if route.pinned_mailbox.is_some() {
                bamboo_domain::ActorPlacementIntent::PinnedHost {
                    host_ref: host.host_ref.clone(),
                }
            } else {
                bamboo_domain::ActorPlacementIntent::Pool {
                    pool_id: route.worker_role.clone(),
                }
            };
            actor_store
                .bind_control_plane(
                    &lease.actor_id,
                    bamboo_domain::ActorControlPlaneBinding {
                        policy_revision: None,
                        placement_intent: Some(placement_intent),
                    },
                )
                .await
                .map_err(|_| {
                    tracing::warn!(stage = "actor_bind", "remote placement admission failed");
                    remote_broker_unavailable()
                })?;
            let activation = actor_store
                .claim_activation(&ActorActivationClaim {
                    actor_id: lease.actor_id.clone(),
                    run_id: lease.run_id.clone(),
                    lease_owner: format!("worker-host-{}", uuid::Uuid::new_v4()),
                    lease_expires_at: lease.expires_at,
                    inbox_generation: 0,
                    placement_ref: Some(placement_ref.clone()),
                    now: chrono::Utc::now(),
                })
                .await
                .map_err(|_| {
                    tracing::warn!(stage = "actor_claim", "remote placement admission failed");
                    remote_broker_unavailable()
                })?;
            let fence = activation.fence();
            claimed_fence = Some(fence.clone());
            actor_store
                .start_activation(&fence, chrono::Utc::now())
                .await
                .map_err(|_| {
                    tracing::warn!(stage = "actor_start", "remote placement admission failed");
                    remote_broker_unavailable()
                })?;
            let reservation = registry
                .validate_run_reservation(bamboo_domain::WorkerRunReservationCheck {
                    placement_ref: placement_ref.clone(),
                    actor_id: lease.actor_id.clone(),
                    run_id: lease.run_id.clone(),
                    observed_host_ref: host.host_ref.clone(),
                    observed_connection_generation: host.connection_generation.clone(),
                    now: chrono::Utc::now(),
                })
                .await
                .map_err(|error| {
                    tracing::warn!(stage = "slot_preflight", reason = %error, "remote placement admission failed");
                    remote_broker_unavailable()
                })?;
            if reservation.mailbox != mailbox || reservation.lease != lease {
                tracing::warn!(stage = "slot_binding", "remote placement admission failed");
                return Err(remote_broker_unavailable());
            }
            Ok::<_, AgentError>(fence)
        }
        .await;
        let fence = match actor_binding {
            Ok(fence) => fence,
            Err(error) => {
                if let Some(fence) = claimed_fence.as_ref() {
                    if let Err(rollback_error) =
                        fail_pre_dispatch_activation(actor_store.as_ref(), fence).await
                    {
                        tracing::warn!(
                            stage = "actor_pre_dispatch_rollback",
                            reason = %rollback_error,
                            "remote placement activation rollback failed"
                        );
                    }
                }
                let _ = registry.release_slot(&lease).await;
                return Err(error);
            }
        };
        let lease = Arc::new(tokio::sync::Mutex::new(lease));
        let host_ref = host.host_ref.clone();
        let lost = Arc::new(AtomicBool::new(false));
        let run_cancel = parent_cancel.child_token();
        let heartbeat = {
            let registry = registry.clone();
            let actor_store = actor_store.clone();
            let route = route.clone();
            let host_ref = host_ref.clone();
            let mailbox = mailbox.clone();
            let fence = fence.clone();
            let placement_ref = placement_ref.clone();
            let lease = lease.clone();
            let lost = lost.clone();
            let run_cancel = run_cancel.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(30));
                interval.tick().await;
                loop {
                    tokio::select! {
                        _ = run_cancel.cancelled() => break,
                        _ = interval.tick() => {}
                    }
                    let refreshed = async {
                        let mut query = route.query().await?;
                        let host = PlacementScheduler::new(registry.clone())
                            .observe_scoped_host(
                                &mut query,
                                &host_ref,
                                &mailbox,
                                &route.worker_role,
                                chrono::Duration::seconds(110),
                            )
                            .await
                            .map_err(|_| remote_broker_unavailable())?;
                        let current = lease.lock().await.clone();
                        if host.connection_generation != current.connection_generation {
                            return Err(remote_broker_unavailable());
                        }
                        let entry = actor_store
                            .inspect_actor(&current.actor_id)
                            .await
                            .map_err(|_| remote_broker_unavailable())?;
                        if entry.activation.as_ref().is_none_or(|activation| {
                            activation.fence() != fence
                                || activation.placement_ref.as_ref() != Some(&placement_ref)
                        }) {
                            return Err(remote_broker_unavailable());
                        }
                        let now = chrono::Utc::now();
                        registry
                            .validate_run_reservation(bamboo_domain::WorkerRunReservationCheck {
                                placement_ref: placement_ref.clone(),
                                actor_id: current.actor_id.clone(),
                                run_id: current.run_id.clone(),
                                observed_host_ref: host.host_ref.clone(),
                                observed_connection_generation: host.connection_generation.clone(),
                                now,
                            })
                            .await
                            .map_err(|_| remote_broker_unavailable())?;
                        let renewed = registry
                            .renew_slot(&current, now, now + chrono::Duration::seconds(80))
                            .await
                            .map_err(|_| remote_broker_unavailable())?;
                        *lease.lock().await = renewed;
                        let expiry = lease.lock().await.expires_at;
                        actor_store
                            .renew_activation(&fence, now, expiry)
                            .await
                            .map_err(|_| remote_broker_unavailable())?;
                        Ok::<(), AgentError>(())
                    }
                    .await;
                    if refreshed.is_err() {
                        lost.store(true, Ordering::SeqCst);
                        run_cancel.cancel();
                        break;
                    }
                }
            })
        };
        Ok(Self {
            registry,
            actor_store,
            route,
            mailbox,
            host_ref,
            fence,
            placement_ref,
            lease,
            heartbeat,
            run_cancel,
            lost,
            release_on_drop: AtomicBool::new(false),
        })
    }

    fn lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst)
    }

    async fn abort_if_run_unsent(&mut self, send_invoked: bool) -> Result<bool, AgentError> {
        if send_invoked {
            return Ok(false);
        }
        self.abort_before_dispatch().await?;
        Ok(true)
    }

    async fn abort_before_dispatch(&mut self) -> Result<(), AgentError> {
        self.heartbeat.abort();
        let _ = (&mut self.heartbeat).await;
        let current = self.lease.lock().await.clone();
        let failed = fail_pre_dispatch_activation(self.actor_store.as_ref(), &self.fence).await;
        let released = self.registry.release_slot(&current).await;
        if let Err(error) = failed {
            tracing::warn!(stage = "actor_pre_dispatch_rollback", reason = %error, "remote activation failure could not be recorded");
            return Err(remote_broker_unavailable());
        }
        if let Err(error) = released {
            if !matches!(error, bamboo_domain::HostRegistryError::StaleLease) {
                tracing::warn!(stage = "slot_pre_dispatch_rollback", reason = %error, "remote slot release failed");
                return Err(remote_broker_unavailable());
            }
        }
        Ok(())
    }

    async fn complete(
        &mut self,
        outcome: ActorActivationFinish,
        terminal_proven: bool,
    ) -> Result<(), AgentError> {
        if !terminal_proven {
            // Keep the activation live until its lease expires. A silent Worker
            // may still be executing; this blocks an immediate ActorId retry.
            return if outcome == ActorActivationFinish::Succeeded {
                Err(remote_broker_unavailable())
            } else {
                Ok(())
            };
        }
        // Wait for the heartbeat to stop before reading the exact lease. It
        // must not renew the slot after terminal verification or release.
        self.heartbeat.abort();
        let _ = (&mut self.heartbeat).await;
        let current = self.lease.lock().await.clone();
        // A correlated Outcome from the selected mailbox alone cannot release
        // a new generation's slot. Reobserve the scoped broker identity at
        // terminal time, including fast reconnects between heartbeat ticks.
        let mut query = self.route.query().await?;
        let observed = query
            .observe_host_capacity(&self.mailbox, &self.route.worker_role)
            .await
            .map_err(|_| remote_broker_unavailable())?
            .ok_or_else(remote_broker_unavailable)?;
        if observed.host_ref != self.host_ref
            || observed.connection_generation != current.connection_generation
            || observed.mailbox != self.mailbox
            || observed.role.as_deref() != Some(self.route.worker_role.as_str())
        {
            return Err(remote_broker_unavailable());
        }
        let entry = self
            .actor_store
            .inspect_actor(&current.actor_id)
            .await
            .map_err(|_| remote_broker_unavailable())?;
        if entry.activation.as_ref().is_none_or(|activation| {
            activation.fence() != self.fence
                || activation.placement_ref.as_ref() != Some(&self.placement_ref)
        }) {
            return Err(remote_broker_unavailable());
        }
        self.registry
            .validate_slot(&current, chrono::Utc::now())
            .await
            .map_err(|_| remote_broker_unavailable())?;
        self.actor_store
            .finish_activation(&self.fence, chrono::Utc::now(), outcome)
            .await
            .map_err(|_| remote_broker_unavailable())?;
        // A terminal ActorDirectory record permits an immediate retry. Free
        // its exact slot before returning rather than racing a Drop task.
        self.release_on_drop.store(true, Ordering::SeqCst);
        match self.registry.release_slot(&current).await {
            Ok(()) | Err(bamboo_domain::HostRegistryError::StaleLease) => {
                self.release_on_drop.store(false, Ordering::SeqCst);
                Ok(())
            }
            Err(error) => {
                tracing::warn!(stage = "slot_terminal_release", reason = %error, "remote slot release failed");
                Err(remote_broker_unavailable())
            }
        }
    }
}

impl Drop for ActivePlacementLease {
    fn drop(&mut self) {
        self.run_cancel.cancel();
        self.heartbeat.abort();
        if !self.release_on_drop.load(Ordering::SeqCst) {
            return;
        }
        let registry = self.registry.clone();
        let actor_store = self.actor_store.clone();
        let fence = self.fence.clone();
        let placement_ref = self.placement_ref.clone();
        let lease = self.lease.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let current = lease.lock().await.clone();
                if actor_store
                    .inspect_actor(&current.actor_id)
                    .await
                    .ok()
                    .and_then(|entry| entry.activation)
                    .is_some_and(|activation| {
                        activation.fence() == fence
                            && activation.placement_ref.as_ref() == Some(&placement_ref)
                    })
                {
                    let _ = registry.release_slot(&current).await;
                }
            });
        }
    }
}

/// Host-only receipt repair uses the same immutable boot configuration as the
/// actor runner. It never infers a broker route from Child display metadata or
/// a Worker claim. A role whose fixed remote route is ambiguous or incomplete
/// stays blocked until an operator restores a provable route.
pub struct BrokerTerminalReceiptReconciler {
    store: Arc<bamboo_storage::SessionStoreV2>,
    bus: Option<bamboo_subagent::BusEndpoint>,
    remote: HashMap<String, Option<ReceiptRemoteRoute>>,
}

struct ReceiptRemoteRoute {
    endpoint: String,
    token: String,
    ca_cert_file: PathBuf,
    parent: bamboo_subagent::AgentRef,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BrokerReceiptRepairReport {
    pub candidates: usize,
    pub repaired: usize,
    pub blocked: usize,
}

impl BrokerTerminalReceiptReconciler {
    pub fn new(store: Arc<bamboo_storage::SessionStoreV2>, config: &bamboo_config::Config) -> Self {
        let subagents = config.subagents();
        let mut counts = HashMap::<&str, usize>::new();
        for placement in &subagents.remote_placements {
            *counts.entry(placement.role.as_str()).or_default() += 1;
        }
        let mut remote = HashMap::new();
        for placement in &subagents.remote_placements {
            let route = (counts.get(placement.role.as_str()) == Some(&1))
                .then(|| receipt_remote_route(placement))
                .flatten();
            remote.insert(placement.role.clone(), route);
        }
        Self {
            store,
            bus: subagents.broker.clone().and_then(|broker| {
                (!broker.endpoint.trim().is_empty() && !broker.token.is_empty()).then_some(
                    bamboo_subagent::BusEndpoint {
                        endpoint: broker.endpoint,
                        token: broker.token,
                    },
                )
            }),
            remote,
        }
    }

    /// One physical-tree scan. Every Child is independent; a failed route,
    /// broker, or ACK retains its Host receipt and never blocks another Child.
    pub async fn reconcile_once(&self) -> BrokerReceiptRepairReport {
        let candidates = match self
            .store
            .discover_unconfirmed_broker_terminal_children()
            .await
        {
            Ok(candidates) => candidates,
            Err(error) => {
                tracing::warn!(%error, "broker terminal receipt scan blocked; no ACK attempted");
                return BrokerReceiptRepairReport {
                    blocked: 1,
                    ..BrokerReceiptRepairReport::default()
                };
            }
        };
        let mut report = BrokerReceiptRepairReport {
            candidates: candidates.len(),
            ..BrokerReceiptRepairReport::default()
        };
        for child in candidates {
            let result =
                tokio::time::timeout(Duration::from_secs(15), self.reconcile_child(&child)).await;
            match result {
                Ok(Ok(())) => report.repaired += 1,
                Ok(Err(reason)) => {
                    report.blocked += 1;
                    tracing::warn!(child_id = %child.id, reason, "broker terminal receipt retained for retry");
                }
                Err(_) => {
                    report.blocked += 1;
                    tracing::warn!(child_id = %child.id, "broker terminal receipt repair timed out; retained for retry");
                }
            }
        }
        report
    }

    async fn reconcile_child(&self, child: &Session) -> Result<(), &'static str> {
        if child.metadata.get("runtime.kind").map(String::as_str) != Some("external")
            || child.metadata.get("external.protocol").map(String::as_str) != Some("actor")
            || child.metadata.get("external.agent_id").map(String::as_str)
                != Some(super::config::LOCAL_ACTOR_AGENT_ID)
        {
            return Err("Child has no trusted built-in actor recovery route");
        }
        let current = self
            .store
            .load_session(&child.id)
            .await
            .map_err(|_| "canonical Child reload failed")?
            .ok_or("canonical Child disappeared")?;
        if current.kind != bamboo_domain::SessionKind::Child
            || current.id != child.id
            || current.created_at != child.created_at
            || current.parent_session_id != child.parent_session_id
            || current.root_session_id != child.root_session_id
            || current.project_id_meta() != child.project_id_meta()
            || current.metadata.get("runtime.kind").map(String::as_str) != Some("external")
            || current
                .metadata
                .get("external.protocol")
                .map(String::as_str)
                != Some("actor")
            || current
                .metadata
                .get("external.agent_id")
                .map(String::as_str)
                != Some(super::config::LOCAL_ACTOR_AGENT_ID)
            || bamboo_domain::ActorSession::from_session(&current).is_err()
        {
            return Err("canonical Child birth, Root, or Project changed");
        }
        let root = self
            .store
            .load_session(&child.root_session_id)
            .await
            .map_err(|_| "canonical Root reload failed")?
            .ok_or("canonical Root disappeared")?;
        if root.kind != bamboo_domain::SessionKind::Root
            || root.id != child.root_session_id
            || root.root_session_id != child.root_session_id
            || root.project_id_meta() != child.project_id_meta()
        {
            return Err("canonical Root identity changed");
        }
        let parent_id = child
            .parent_session_id
            .as_deref()
            .ok_or("Child parent identity missing")?;
        let parent = self
            .store
            .load_session(parent_id)
            .await
            .map_err(|_| "canonical parent reload failed")?
            .ok_or("canonical parent disappeared")?;
        if parent.id != parent_id
            || !matches!(
                parent.kind,
                bamboo_domain::SessionKind::Root | bamboo_domain::SessionKind::Child
            )
            || parent.root_session_id != child.root_session_id
            || parent.project_id_meta() != child.project_id_meta()
            || parent.spawn_depth.checked_add(1) != Some(current.spawn_depth)
        {
            return Err("canonical parent lineage changed");
        }

        // Validate the Host checkpoint before opening a recovery connection.
        // This rejects an uncommitted terminal, stale transcript, or changed
        // Child identity before touching the broker.
        let receipts = self
            .store
            .recover_broker_terminal_receipts(&current)
            .await
            .map_err(|_| "Host broker terminal checkpoint unverified")?;
        if receipts.is_empty() {
            return Ok(());
        }
        let role = current
            .metadata
            .get("subagent_type")
            .map(String::as_str)
            .unwrap_or("worker");
        for receipt in receipts {
            let local_mailbox = format!("p-{}", child.id);
            // The committed receipt records the route used by this run. A
            // later role configuration change must not redirect its ACK.
            let mut link = if receipt.parent_mailbox == local_mailbox {
                let bus = self
                    .bus
                    .as_ref()
                    .ok_or("local broker receipt bus unavailable")?;
                bamboo_broker::BrokerChildLink::connect_receipt_recovery(
                    &bus.endpoint,
                    bamboo_subagent::AgentRef {
                        session_id: local_mailbox,
                        role: None,
                    },
                    &bus.token,
                )
                .await
                .map_err(|_| "local broker receipt connection unavailable")?
            } else if let Some(remote) = self.remote.get(role) {
                let route = remote
                    .as_ref()
                    .ok_or("fixed remote broker route is unavailable or ambiguous")?;
                if receipt.parent_mailbox != route.parent.session_id {
                    return Err("fixed remote parent mailbox changed since Host checkpoint");
                }
                let tls = bamboo_broker::client_config_trusting_cert(&route.ca_cert_file)
                    .map_err(|_| "fixed remote broker TLS pin unavailable")?;
                bamboo_broker::BrokerChildLink::connect_receipt_recovery_with_tls(
                    &route.endpoint,
                    route.parent.clone(),
                    &route.token,
                    tls,
                )
                .await
                .map_err(|_| "fixed remote broker receipt connection unavailable")?
            } else {
                return Err("committed broker receipt route is unavailable");
            };
            if link.parent_mailbox() != receipt.parent_mailbox {
                return Err("broker receipt link mailbox differs from Host checkpoint");
            }
            bamboo_subagent::ChildLink::acknowledge_recovered_durable_frames(
                &mut link,
                &receipt.broker_identity,
                &receipt.message_ids,
            )
            .await
            .map_err(|_| "broker identity changed or exact ACK unconfirmed")?;
            self.store
                .clear_acknowledged_broker_terminal_receipt(
                    &receipt.session_id,
                    receipt.created_at,
                    &receipt.activation_run_id,
                    &receipt.parent_mailbox,
                )
                .await
                .map_err(|_| "broker ACK succeeded but Host receipt cleanup failed")?;
        }
        Ok(())
    }
}

fn receipt_remote_route(
    placement: &bamboo_config::RemoteActorPlacement,
) -> Option<ReceiptRemoteRoute> {
    let peer = placement.broker_peer.as_ref().filter(|peer| peer.valid())?;
    let endpoint = url::Url::parse(&placement.endpoint).ok()?;
    if placement.endpoint.len() > 2048
        || endpoint.scheme() != "wss"
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return None;
    }
    let token_env = placement
        .token_env
        .as_deref()
        .filter(|name| !name.is_empty() && name.len() <= 256)?;
    let token = std::env::var(token_env).ok()?;
    if !(32..=256).contains(&token.len()) || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return None;
    }
    let ca_cert_file = PathBuf::from(placement.ca_cert_file.as_deref()?);
    bamboo_broker::client_config_trusting_cert(&ca_cert_file).ok()?;
    Some(ReceiptRemoteRoute {
        endpoint: placement.endpoint.clone(),
        token,
        ca_cert_file,
        parent: bamboo_subagent::AgentRef {
            session_id: peer.parent_mailbox.clone(),
            role: peer.parent_role.clone(),
        },
    })
}

/// How `execute_external_child` should obtain its worker connection, decided
/// once from `spec.placement`. Splits the divergent acquire/connect + retire
/// logic three ways while the shared middle (Run dispatch, live registration,
/// drive, close) stays identical. Remote uses a scoped broker peer route.
enum PlacementKind {
    Local,
    Remote,
    Schedulable,
}

/// A read-only activation needs an executor and worker whose tool surface and
/// no-shell checker enforce the typed capability. Codex's read-only sandbox
/// prevents writes but still permits command execution, so neither Codex mode
/// can currently satisfy the no-shell contract. Local Bamboo/Claude workers are
/// fingerprinted by their baked fields, while remote/schedulable placements
/// connect to an already-provisioned resident and [`RunSpec`] currently carries
/// no equivalent per-activation authority. Fail closed before provisioning or
/// dispatch instead of treating either posture as read-only enforcement.
fn ensure_read_only_activation_is_enforceable(spec: &ProvisionSpec) -> Result<(), AgentError> {
    if !spec.capabilities.read_only_enforced() {
        return Ok(());
    }

    if matches!(spec.executor, ExecutorSpec::Codex { .. }) {
        return Err(AgentError::LLM(format!(
            "read-only actor role '{}' cannot use the Codex executor because its read-only sandbox still permits command execution; use bamboo_runtime or claude_code",
            spec.identity.role
        )));
    }

    if !matches!(spec.placement, Placement::Local) {
        return Err(AgentError::LLM(format!(
            "read-only actor role '{}' requires local placement; remote and schedulable resident workers cannot verify this activation's read-only tool boundary",
            spec.identity.role
        )));
    }

    Ok(())
}

/// Spawns and drives a child session as an independent actor: a `bamboo-subagent` worker process.
pub struct ActorChildRunner {
    approval_registry: Option<super::approval_registry::SharedApprovalRegistry>,
    permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    agent_id: String,
    worker_bin: PathBuf,
    worker_args: Vec<String>,
    builtin_required_context_route: bool,
    native_tool_ceiling: Option<Arc<dyn super::runtime::NativeToolCeilingSource>>,
    fabric_dir: PathBuf,
    executor: ExecutorSpec,
    /// Per-provider credentials snapshotted from the parent config at build
    /// time; the spec carries only the ONE the child's provider needs. Server
    /// runtimes install `live_provider_config` below so a provider hot reload
    /// cannot strand this fallback snapshot for the rest of the process.
    credentials: Vec<ScopedCredential>,
    /// Parent's default provider (used when the child has no explicit one).
    default_provider: String,
    /// Live server configuration used to resolve provider credentials at child
    /// activation time. This is deliberately limited to provider provisioning:
    /// executor/placement policy remains the immutable runner configuration.
    live_provider_config: Option<Arc<tokio::sync::RwLock<bamboo_llm::Config>>>,
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
    pool: Arc<tokio::sync::Mutex<WorkerPool>>,
    max_idle_per_key: usize,
    max_idle_total: usize,
    pool_reaper_started: AtomicBool,
    /// Monotonic fencing token for concrete worker attempts in this runtime.
    /// `activation_run_id` fences across runtime restarts; this epoch also
    /// fences a late first worker when an activation is retried in-place.
    next_execution_epoch: AtomicU64,
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
    /// Fixed parent mailbox subscription has exactly one in-process owner.
    strict_remote_subscription: tokio::sync::Mutex<()>,
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
    /// Optional server authority used only by `Codex` in `bamboo` auth mode.
    codex_run_tokens: Option<Arc<dyn CodexRunTokenAuthority>>,
    /// Canonical logical-session inbox resources, late-bound by each owning
    /// runtime. Kept per runner/runtime; never process-global.
    session_inbox_runtime: Arc<std::sync::Mutex<Option<SessionInboxRuntimeBinding>>>,
    actor_directory_store: std::sync::Mutex<Option<Arc<bamboo_storage::SessionStoreV2>>>,
    canonical_subagent_tool: std::sync::Mutex<Option<Arc<dyn bamboo_agent_core::tools::Tool>>>,
    actor_event_observer: std::sync::Mutex<Option<Arc<dyn ActorEventObserver>>>,
    /// A processed broker terminal still needs the Host's final Child Session
    /// save before its exact Event/Outcome mailbox receipts can be ACKed.
    pending_durable_links: tokio::sync::Mutex<
        HashMap<(String, chrono::DateTime<chrono::Utc>, String), PendingDurableChildLink>,
    >,
}

struct PendingDurableChildLink {
    link: Box<dyn bamboo_subagent::ChildLink>,
    /// Captured from this Host frame pump after canonical nested-wait checks.
    /// The map key binds it to the exact Child birth and activation Run.
    host_nested_wait_handoff: bool,
}

fn effective_broker_terminal_status(
    raw: TerminalStatus,
    host_nested_wait_handoff: bool,
) -> Result<&'static str, String> {
    if host_nested_wait_handoff {
        if raw != TerminalStatus::Completed {
            return Err("nested wait cannot project a failed broker terminal".into());
        }
        Ok("suspended")
    } else {
        Ok(broker_terminal_status_label(raw))
    }
}

fn broker_terminal_status_label(status: TerminalStatus) -> &'static str {
    match status {
        TerminalStatus::Completed => "completed",
        TerminalStatus::Suspended => "suspended",
        TerminalStatus::Error => "error",
        TerminalStatus::Cancelled => "cancelled",
    }
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
/// Process-local Host scope. Durable data can compare its stamp but cannot
/// deserialize a new live authority from it.
#[derive(Clone)]
pub struct ChildApprovalScope {
    stamp: serde_json::Value,
    deadline: chrono::DateTime<chrono::Utc>,
    parent: String,
    child: String,
    run: String,
    epoch: u64,
    current_epoch: Arc<AtomicU64>,
    cancel: CancellationToken,
    router: Arc<crate::SessionActivationRouter>,
    admission: Arc<AtomicUsize>,
}

pub struct ChildApprovalAdmission(Arc<AtomicUsize>);
impl Drop for ChildApprovalAdmission {
    fn drop(&mut self) {
        self.0.store(2, Ordering::Release);
    }
}

impl ChildApprovalScope {
    /// Called by the actual Host frame pump, never from a wire payload.
    pub fn new(
        parent: &str,
        child: &str,
        transport: (u32, &str, u64, &str, chrono::DateTime<chrono::Utc>),
        router: Arc<crate::SessionActivationRouter>,
        cancel: CancellationToken,
        current_epoch: Arc<AtomicU64>,
    ) -> Self {
        let (attempt, run, epoch, reply, deadline) = transport;
        let deadline = deadline.min(chrono::Utc::now() + chrono::Duration::seconds(240));
        Self {
            stamp: serde_json::json!({"host_scope": uuid::Uuid::new_v4().to_string(),
                "attempt": attempt, "run": run, "epoch": epoch, "reply": reply,
                "deadline": deadline}),
            deadline,
            parent: parent.into(),
            child: child.into(),
            run: run.into(),
            epoch,
            current_epoch,
            cancel,
            router,
            admission: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn stamp(&self) -> &serde_json::Value {
        &self.stamp
    }
    pub fn deadline(&self) -> chrono::DateTime<chrono::Utc> {
        self.deadline
    }
    pub fn first_admission(&self) -> Option<ChildApprovalAdmission> {
        self.admission
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| ChildApprovalAdmission(self.admission.clone()))
    }
    pub fn is_admitting(&self) -> bool {
        self.admission.load(Ordering::Acquire) == 1
    }
    pub async fn is_current(&self, parent: &str, child: &str) -> bool {
        self.parent == parent
            && self.child == child
            && !self.cancel.is_cancelled()
            && self.current_epoch.load(Ordering::Acquire) == self.epoch
            && self.router.owns_run(child, &self.run).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildApprovalReview {
    Reply(bool),
    NoReply,
}

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

    async fn review_scoped(
        &self,
        parent: &str,
        child: &str,
        request: &serde_json::Value,
        _scope: &ChildApprovalScope,
    ) -> ChildApprovalReview {
        ChildApprovalReview::Reply(self.review(parent, child, request).await)
    }
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
            builtin_required_context_route: false,
            fabric_dir,
            executor,
            credentials,
            default_provider,
            live_provider_config: None,
            native_tool_ceiling: None,
            bus: None,
            concurrency: std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent.max(1))),
            pool: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            max_idle_per_key: DEFAULT_MAX_IDLE_PER_KEY,
            max_idle_total: DEFAULT_MAX_IDLE_TOTAL,
            pool_reaper_started: AtomicBool::new(false),
            next_execution_epoch: AtomicU64::new(0),
            approval_decider: None,
            approval_reviewer: None,
            escalation_bridge: Arc::new(std::sync::Mutex::new(None)),
            remote_placements: HashMap::new(),
            strict_remote_subscription: tokio::sync::Mutex::new(()),
            schedulable_placements: HashMap::new(),
            codex_run_tokens: None,
            session_inbox_runtime: Arc::new(std::sync::Mutex::new(None)),
            actor_directory_store: std::sync::Mutex::new(None),
            canonical_subagent_tool: std::sync::Mutex::new(None),
            actor_event_observer: std::sync::Mutex::new(None),
            pending_durable_links: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Bind the AppState-owned live configuration. Child activations read this
    /// after a successful provider reload, so newly added/rotated provider
    /// credentials take effect without restarting Bamboo.
    /// Only the built-in factory sets this from its immutable launch config.
    pub(crate) fn with_builtin_required_context_route(mut self, supported: bool) -> Self {
        self.builtin_required_context_route = supported;
        self
    }

    pub(crate) fn with_native_tool_ceiling_source(
        mut self,
        source: Option<Arc<dyn super::runtime::NativeToolCeilingSource>>,
    ) -> Self {
        self.native_tool_ceiling = source;
        self
    }

    async fn require_initial_input_release_worker(&self) -> crate::runtime::runner::Result<()> {
        bamboo_subagent::fleet::require_worker_capability(
            &self.worker_bin,
            &self.worker_args,
            bamboo_subagent::provision::INITIAL_INPUT_RELEASE_WORKER_CAPABILITY,
        )
        .await
        .map_err(|_| AgentError::LLM("owned initial release worker capability unconfirmed".into()))
    }

    pub fn with_live_provider_config(
        mut self,
        config: Arc<tokio::sync::RwLock<bamboo_llm::Config>>,
    ) -> Self {
        self.live_provider_config = Some(config);
        self
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
    /// they share role, provider, model, provider-credential revision,
    /// workspace, disabled-tool set, AND every
    /// capability the worker BAKES at provision time (`BambooRuntimeExecutor`
    /// stamps these once and reuses them across runs): nesting depth, nested-spawn
    /// stack, requested/effective permission modes, legacy bypass/auto flags,
    /// permission enforcement, and the depth cap. Omitting any
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
        // Provider credentials are baked into a worker at provision time. Hash
        // the scoped envelope so a hot-reloaded key/base URL cannot reuse a
        // worker provisioned with stale authority. The key itself never enters
        // the pool key or logs.
        let credential_revision = if spec.secrets.provider_credentials.is_empty() {
            "none".to_string()
        } else {
            let serialized = serde_json::to_vec(&spec.secrets.provider_credentials)
                .expect("scoped provider credentials are serializable");
            hex::encode(Sha256::digest(serialized))
        };
        // The worker constructs its executor exactly once. In particular,
        // Codex exec and app-server workers are not interchangeable.
        let executor = serde_json::to_string(&spec.executor).unwrap_or_default();
        format!(
            "{role}\u{1}{provider}\u{1}{model}\u{1}cred={credential_revision}\u{1}{workspace}\u{1}{}\u{1}d={}\u{1}ns={}\u{1}pr={}\u{1}pe={}\u{1}by={}\u{1}auto={}\u{1}ep={}\u{1}md={}\u{1}nha={}\u{1}ro={}\u{1}gro={}\u{1}ci={}\u{1}executor={executor}",
            tools.join(","),
            spec.identity.depth,
            caps.nested_spawn,
            caps.permission_requested_mode,
            caps.permission_effective_mode,
            caps.bypass,
            caps.auto_approve_permissions,
            caps.enforce_permissions,
            caps.max_spawn_depth.unwrap_or(0),
            // #73 review (P1): a worker bakes `no_human_review` ONCE from this flag
            // at build() and never re-reads it per run, so the pool MUST NOT hand a
            // worker baked for one approval posture to a run of the opposite one —
            // else a scheduled-root worker reused for an interactive child would
            // silently model-review instead of asking the human (and vice-versa,
            // reintroducing the 300s-deny). Split the bucket on it.
            caps.no_human_approver,
            // The read-only no-shell checker is baked once at build(), so a planner
            // or Guardian worker must NOT be reused for an ordinary child.
            caps.read_only,
            // Preserve the legacy Guardian bit as a distinct fingerprint axis
            // during rolling upgrades even though both bits enforce read-only.
            caps.guardian_read_only,
            caps.child_creation_identity,
        )
    }

    /// Start one weakly-owned sweep task for this runner. It does not keep the
    /// runner/pool alive, and it never holds the pool lock while killing a child.
    fn ensure_pool_reaper(&self) {
        if self
            .pool_reaper_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let pool = Arc::downgrade(&self.pool);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(POOLED_REAPER_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Tokio intervals tick immediately once; consume that tick so a new
            // runner does not scan an empty pool before its first worker parks.
            interval.tick().await;
            loop {
                interval.tick().await;
                let Some(pool) = pool.upgrade() else {
                    break;
                };
                let _ = reap_worker_pool_once(&pool).await;
            }
        });
    }

    fn fresh_required_worker_spec(spec: &ProvisionSpec) -> ProvisionSpec {
        let mut physical = spec.clone();
        // A killed one-shot worker may leave an unACKed Run in its mailbox.
        // A fresh subscriber must not replay that Run. Logical birth, storage
        // namespace, typed input targets and authority remain on the RunSpec.
        physical.identity.child_id = format!("required-worker-{}", uuid::Uuid::new_v4());
        physical
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
        if spec.capabilities.required_child_context {
            let physical = Self::fresh_required_worker_spec(spec);
            let worker = spawn_worker_on_bus(&self.worker_bin, &self.worker_args, &physical)
                .await
                .map_err(|error| {
                    AgentError::LLM(format!("required-context worker spawn failed: {error}"))
                })?;
            let mailbox_id = worker.record.agent_id.clone();
            return Ok(PooledWorker {
                worker,
                mailbox_id,
                parked_at: None,
            });
        }
        self.ensure_pool_reaper();
        // Drain the bucket, skipping (and reaping) any worker whose process exited
        // or crossed its idle deadline while parked. A live, fresh one is handed
        // straight out for reuse.
        loop {
            let candidate = {
                let mut pool = self.pool.lock().await;
                let candidate = pool.get_mut(key).and_then(|bucket| bucket.pop());
                if pool.get(key).is_some_and(Vec::is_empty) {
                    pool.remove(key);
                }
                candidate
            };
            let Some(mut candidate) = candidate else {
                break;
            };
            let now = Instant::now();
            let expired = parked_worker_expired(
                candidate.parked_at,
                now,
                Duration::from_secs(POOLED_IDLE_TIMEOUT_SECS),
            );
            if candidate.worker.is_alive() && !expired {
                candidate.parked_at = None;
                return Ok(candidate);
            }
            tracing::info!(
                pool_key = key,
                mailbox_id = %candidate.mailbox_id,
                worker_pid = ?candidate.worker.pid(),
                shutdown_reason = if expired {
                    PoolReapReason::IdleTimeout.as_str()
                } else {
                    PoolReapReason::ProcessExited.as_str()
                },
                "discarding unusable parked sub-agent worker during checkout"
            );
            candidate.worker.kill().await;
        }

        let spawned = spawn_worker_on_bus(&self.worker_bin, &self.worker_args, spec)
            .await
            .map_err(|e| AgentError::LLM(format!("actor spawn (bus) failed: {e}")))?;
        let mailbox_id = spawned.record.agent_id.clone();
        Ok(PooledWorker {
            worker: spawned,
            mailbox_id,
            parked_at: None,
        })
    }

    /// Park a warm bus worker for reuse after a clean run; if its bucket is full
    /// (or it died), kill it instead. The worker stays dialed-in + subscribed
    /// while parked, so a reusing child just delivers a new `Run` to its mailbox.
    async fn release_bus_worker(&self, key: &str, mut worker: PooledWorker) {
        self.ensure_pool_reaper();
        if !worker.worker.is_alive() {
            tracing::info!(
                pool_key = key,
                mailbox_id = %worker.mailbox_id,
                worker_pid = ?worker.worker.pid(),
                shutdown_reason = "process_exited_before_park",
                "discarding sub-agent worker instead of parking"
            );
            worker.worker.kill().await;
            return;
        }
        worker.parked_at = Some(Instant::now());
        let mut pool = self.pool.lock().await;
        let key_count = pool.get(key).map_or(0, Vec::len);
        let total_count = pool.values().map(Vec::len).sum::<usize>();
        let limit_reason =
            idle_pool_limit_reason(&pool, key, self.max_idle_per_key, self.max_idle_total);
        if let Some(shutdown_reason) = limit_reason {
            drop(pool);
            tracing::info!(
                pool_key = key,
                mailbox_id = %worker.mailbox_id,
                worker_pid = ?worker.worker.pid(),
                idle_workers_for_key = key_count,
                idle_workers_total = total_count,
                max_idle_per_key = self.max_idle_per_key,
                max_idle_total = self.max_idle_total,
                shutdown_reason,
                "discarding sub-agent worker because the warm pool is full"
            );
            worker.worker.kill().await;
            return;
        }
        pool.entry(key.to_string()).or_default().push(worker);
    }

    /// Assemble the parent-resolved provisioning document for this child using
    /// one coherent provider snapshot.
    fn build_spec_with_provider_config(
        &self,
        session: &Session,
        job: &SpawnJob,
        credentials: &[ScopedCredential],
        default_provider: &str,
    ) -> ProvisionSpec {
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
                    provider: default_provider.to_string(),
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
                        if let Some(credential) = credentials.iter().find(|credential| {
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
                    .unwrap_or(default_provider);
                if let Some(credential) = credentials
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
        let requested_permission_mode = session
            .agent_runtime_state
            .as_ref()
            .map(|state| state.effective_permission_mode())
            .unwrap_or_default();
        let configured_permission_mode = self
            .permission_config
            .as_ref()
            .map(|config| config.mode())
            .unwrap_or_default();
        // #73: propagate "no interactive human approver" (headless / scheduled /
        // deployed root, inherited by the child session). When set, the worker's
        // per-run approval proxy model-reviews a gated action locally instead of
        // escalating to a human who will never answer (which would 300s-deny).
        spec.capabilities.no_human_approver = session
            .agent_runtime_state
            .as_ref()
            .is_some_and(|s| s.no_human_approver);
        // Read-only authority comes from the child's typed runtime state, never
        // its cosmetic role label. The sole legacy recovery path additionally
        // requires the old Guardian role AND its host-owned mutation denylist,
        // preserving in-flight pre-upgrade reviewers without making a label an
        // authority boundary.
        let typed_read_only = session
            .agent_runtime_state
            .as_ref()
            .is_some_and(|state| state.read_only);
        let legacy_guardian_read_only = spec.identity.role == "guardian"
            && spec.disabled_tools.as_ref().is_some_and(|disabled| {
                ["Edit", "Write", "SubAgent", "WebFetch"]
                    .iter()
                    .all(|name| disabled.iter().any(|disabled_name| disabled_name == *name))
            });
        let read_only = typed_read_only || legacy_guardian_read_only;
        spec.capabilities.read_only = read_only;
        // Emit the old field for every read-only child. Older workers ignore
        // `read_only` but understand this bit and will install the strict
        // no-shell checker, which prevents a rolling upgrade from weakening Plan.
        spec.capabilities.guardian_read_only = read_only;
        if spec.capabilities.read_only_enforced() {
            // Do not even construct nested delegation machinery for a read-only
            // inspection child; the denylist below remains a second boundary.
            spec.capabilities.nested_spawn = false;
            // Treat the capability as the authority boundary, not as a hint
            // that every caller must remember to pair with a schema filter.
            // Preserve caller-specific disables while always removing Bamboo's
            // shell, mutating, persistent, interactive, and delegation surfaces.
            spec.disabled_tools
                .get_or_insert_with(Default::default)
                .extend(crate::runtime::guardian_state::read_only_child_disabled_tools());
            match &mut spec.executor {
                ExecutorSpec::ClaudeCode {
                    permission_mode,
                    inherit_user_config,
                    ..
                } => {
                    *permission_mode = Some("plan".to_string());
                    // User config may add MCP tools/hooks outside the provisioned
                    // surface, so it is never inherited by a read-only child.
                    *inherit_user_config = Some(false);
                }
                ExecutorSpec::Codex {
                    mode,
                    sandbox,
                    inherit_user_config,
                    approval_policy,
                    network_access,
                    allow_danger_bypass,
                    permission_profile,
                    ..
                } => {
                    *sandbox = Some("read-only".to_string());
                    *inherit_user_config = Some(false);
                    // app-server provisioning accepts `on-request`; the typed
                    // Plan activation maps that to `approvalPolicy=never`.
                    *approval_policy = Some(
                        if mode.as_deref() == Some("app_server") {
                            "on-request"
                        } else {
                            "never"
                        }
                        .to_string(),
                    );
                    *network_access = Some(false);
                    *allow_danger_bypass = Some(false);
                    *permission_profile = Some("read-only".to_string());
                }
                _ => {}
            }
        }
        let read_only_overlay = session
            .agent_runtime_state
            .as_ref()
            .is_some_and(|state| state.plan_mode.is_some())
            || spec.capabilities.read_only_enforced()
            || executor_has_read_only_permission_profile(&spec.executor);
        let permission_resolution = bamboo_domain::resolve_permission_mode_with_read_only(
            requested_permission_mode,
            configured_permission_mode,
            read_only_overlay,
        );
        spec.capabilities.bypass = permission_resolution.bypass_permissions();
        spec.capabilities.auto_approve_permissions =
            permission_resolution.suppress_approval_prompts();
        spec.capabilities.permission_requested_mode =
            permission_resolution.requested.as_str().to_string();
        spec.capabilities.permission_effective_mode =
            permission_resolution.effective.as_str().to_string();
        // #193: route this role to a REMOTE resident worker when one is pinned.
        // `spec.identity.role` was just computed from `subagent_type` above; a
        // match flips the placement to Remote. The scoped broker credential stays
        // on the Host transport and never enters the child RunSpec. No match
        // leaves the default `Placement::Local`,
        // so the local path is byte-for-byte unchanged for every non-pinned role.
        if let Some(placement) = self.remote_placements.get(spec.identity.role.as_str()) {
            spec.placement = Placement::Remote {
                endpoint: placement.endpoint.clone(),
            };
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

    /// Assemble from the startup snapshot. Kept synchronous for focused unit
    /// tests and non-server callers that do not install a live config source.
    fn build_spec(&self, session: &Session, job: &SpawnJob) -> ProvisionSpec {
        self.build_spec_with_provider_config(
            session,
            job,
            &self.credentials,
            &self.default_provider,
        )
    }

    /// Assemble from the latest AppState config when available. Taking the
    /// async read lock here gives every new activation a post-reload credential
    /// view without copying secrets into a second independently refreshed
    /// runtime store.
    async fn build_live_spec(&self, session: &Session, job: &SpawnJob) -> ProvisionSpec {
        let Some(config) = self.live_provider_config.as_ref() else {
            return self.build_spec(session, job);
        };

        let config = config.read().await;
        let credentials = super::runtime::extract_provider_credentials(&config);
        let default_provider = config.effective_default_provider().to_string();
        self.build_spec_with_provider_config(session, job, &credentials, &default_provider)
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

    fn scoped_placement_route(
        &self,
        spec: &ProvisionSpec,
    ) -> Result<Option<ScopedPlacementRoute>, AgentError> {
        match &spec.placement {
            Placement::Local => Ok(None),
            Placement::Remote { .. } => {
                let placement = self
                    .remote_placements
                    .get(&spec.identity.role)
                    .ok_or_else(remote_broker_unavailable)?;
                let peer = placement
                    .broker_peer
                    .as_ref()
                    .and_then(|peer| peer.as_ref().ok())
                    .ok_or_else(remote_broker_unavailable)?;
                let worker_role = peer
                    .worker_role
                    .clone()
                    .ok_or_else(remote_broker_unavailable)?;
                Ok(Some(ScopedPlacementRoute {
                    endpoint: placement.endpoint.clone(),
                    token: placement
                        .token
                        .clone()
                        .ok_or_else(remote_broker_unavailable)?,
                    ca_cert_file: placement
                        .ca_cert_file
                        .clone()
                        .ok_or_else(remote_broker_unavailable)?,
                    parent: bamboo_subagent::AgentRef {
                        session_id: peer.parent_mailbox.clone(),
                        role: peer.parent_role.clone(),
                    },
                    worker_role,
                    pinned_mailbox: Some(peer.worker_mailbox.clone()),
                    requirements: placement
                        .requirements
                        .clone()
                        .ok_or_else(remote_broker_unavailable)?,
                }))
            }
            Placement::Schedulable { .. } => {
                let placement = self
                    .schedulable_placements
                    .get(&spec.identity.role)
                    .ok_or_else(remote_broker_unavailable)?;
                let parent = placement
                    .broker_parent
                    .as_ref()
                    .ok_or_else(remote_broker_unavailable)?;
                Ok(Some(ScopedPlacementRoute {
                    endpoint: placement.endpoint.clone(),
                    token: placement
                        .token
                        .clone()
                        .ok_or_else(remote_broker_unavailable)?,
                    ca_cert_file: placement
                        .ca_cert_file
                        .clone()
                        .ok_or_else(remote_broker_unavailable)?,
                    parent: bamboo_subagent::AgentRef {
                        session_id: parent.parent_mailbox.clone(),
                        role: Some(parent.parent_role.clone()),
                    },
                    worker_role: placement.pool.clone(),
                    pinned_mailbox: None,
                    requirements: placement
                        .requirements
                        .clone()
                        .ok_or_else(remote_broker_unavailable)?,
                }))
            }
        }
    }

    async fn select_scoped_worker(
        &self,
        route: &ScopedPlacementRoute,
        registry: &bamboo_storage::v2::FileHostRegistry,
        actor_id: &str,
        project_id: Option<bamboo_domain::ProjectId>,
        required_tools: &BTreeSet<String>,
    ) -> Result<String, AgentError> {
        if let Some(mailbox) = &route.pinned_mailbox {
            return Ok(mailbox.clone());
        }
        let mut query = route.query().await?;
        let candidates = query
            .list_connected(&route.worker_role)
            .await
            .map_err(|_| remote_broker_unavailable())?;
        let scheduler = PlacementScheduler::new(registry.clone());
        let mut selected: Option<(String, usize, u16)> = None;
        for mailbox in candidates {
            let Some(observed) = query
                .observe_host_capacity(&mailbox, &route.worker_role)
                .await
                .map_err(|_| remote_broker_unavailable())?
            else {
                continue;
            };
            let Ok(host) = scheduler
                .observe_scoped_host(
                    &mut query,
                    &observed.host_ref,
                    &mailbox,
                    &route.worker_role,
                    chrono::Duration::seconds(110),
                )
                .await
            else {
                continue;
            };
            let now = chrono::Utc::now();
            let request = HostPlacementRequest {
                intent: HostPlacementIntent::Pool {
                    pool_id: route.worker_role.clone(),
                },
                actor_id: actor_id.to_owned(),
                run_id: "selection".into(),
                project_id: project_id.clone(),
                trust_zone: route.requirements.trust_zone.clone(),
                workspace_label: route.requirements.workspace_label.clone(),
                executor: "bamboo-runtime".into(),
                required_tools: required_tools.clone(),
                network_zone: route.requirements.network_zone.clone(),
                require_network_isolation: route.requirements.require_network_isolation,
                preferred_host_ref: None,
                now,
                lease_expires_at: now + chrono::Duration::seconds(80),
            };
            if host.capabilities.placement_class != bamboo_domain::ActorPlacementClass::Remote
                || !request.eligible(&host)
            {
                continue;
            }
            let load = host.active_slots(now);
            if selected.as_ref().is_none_or(|(_, old_load, old_slots)| {
                load * usize::from(*old_slots) < *old_load * usize::from(host.max_slots)
            }) {
                selected = Some((mailbox, load, host.max_slots));
            }
        }
        selected
            .map(|(mailbox, _, _)| mailbox)
            .ok_or_else(remote_broker_unavailable)
    }
}

#[async_trait]
impl ExternalChildRunner for ActorChildRunner {
    async fn validate_required_child_context_route(&self, session: &Session) -> Result<(), String> {
        let role = session
            .metadata
            .get("subagent_type")
            .map(String::as_str)
            .unwrap_or("worker");
        let current = std::env::current_exe()
            .ok()
            .and_then(|path| path.canonicalize().ok());
        if self.native_tool_ceiling.is_none()
            || !self.builtin_required_context_route
            || !self.should_handle(session).await
            || current.is_none()
            || self.worker_bin.canonicalize().ok() != current
            || self.worker_args != ["subagent-worker".to_string()]
            || !matches!(self.executor, ExecutorSpec::BambooRuntime)
            || self.bus.is_none()
            || self.remote_placements.contains_key(role)
            || self.schedulable_placements.contains_key(role)
        {
            return Err(
                "required_child_context_unsupported: built-in fresh local Bamboo worker required"
                    .into(),
            );
        }
        bamboo_subagent::fleet::require_worker_capability(
            &self.worker_bin,
            &self.worker_args,
            bamboo_subagent::provision::REQUIRED_CHILD_CONTEXT_WORKER_CAPABILITY,
        )
        .await
        .map_err(|_| {
            "required_child_context_unsupported: worker capability unconfirmed".to_string()
        })?;
        bamboo_subagent::fleet::require_worker_capability(
            &self.worker_bin,
            &self.worker_args,
            bamboo_subagent::provision::NATIVE_TOOL_CEILING_WORKER_CAPABILITY,
        )
        .await
        .map_err(|_| {
            "native_tool_ceiling_unsupported: worker capability unconfirmed".to_string()
        })?;
        bamboo_subagent::fleet::require_worker_capability(
            &self.worker_bin,
            &self.worker_args,
            bamboo_subagent::provision::CHILD_CREATION_IDENTITY_WORKER_CAPABILITY,
        )
        .await
        .map_err(|_| {
            "required_child_context_unsupported: worker birth capability unconfirmed".into()
        })
    }
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

    fn set_actor_directory_store(&self, store: Option<Arc<bamboo_storage::SessionStoreV2>>) {
        *self.actor_directory_store.lock().recover_poison() = store;
    }

    fn set_canonical_subagent_tool(&self, tool: Option<Arc<dyn bamboo_agent_core::tools::Tool>>) {
        *self.canonical_subagent_tool.lock().recover_poison() = tool;
    }

    fn set_actor_event_observer(&self, observer: Option<Arc<dyn ActorEventObserver>>) {
        *self.actor_event_observer.lock().recover_poison() = observer;
    }

    async fn prepare_durable_child_delivery(
        &self,
        session: &Session,
        activation_run_id: &str,
    ) -> Result<bool, String> {
        let key = (
            session.id.clone(),
            session.created_at,
            activation_run_id.to_owned(),
        );
        let (delivery, host_nested_wait_handoff) = {
            let pending = self.pending_durable_links.lock().await;
            let Some(pending) = pending.get(&key) else {
                return Ok(false);
            };
            (
                pending
                    .link
                    .durable_delivery_receipt()
                    .ok_or_else(|| "broker terminal has no exact durable receipt".to_string())?,
                pending.host_nested_wait_handoff,
            )
        };
        let expected_status =
            effective_broker_terminal_status(delivery.terminal_status, host_nested_wait_handoff)?;
        if session.last_run_status().as_deref() != Some(expected_status) {
            return Err("Host final status differs from accepted broker terminal".into());
        }
        let store = self
            .actor_directory_store
            .lock()
            .recover_poison()
            .clone()
            .ok_or_else(|| "Host broker receipt store unavailable".to_string())?;
        let binding = self
            .session_inbox_runtime
            .lock()
            .recover_poison()
            .clone()
            .ok_or_else(|| "Host activation fence unavailable for broker receipt".to_string())?;
        if !binding
            .router
            .owns_run(&session.id, activation_run_id)
            .await
        {
            return Err("stale Child activation cannot prepare broker receipt".into());
        }
        store
            .prepare_broker_terminal_receipt(
                session,
                activation_run_id,
                &delivery.broker_identity,
                &delivery.parent_mailbox,
                &delivery.correlation_id,
                &delivery.message_ids,
            )
            .await
            .map_err(|error| format!("Host broker receipt prepare failed: {error}"))?;
        Ok(true)
    }

    async fn confirm_durable_child_delivery(
        &self,
        session: &Session,
        activation_run_id: &str,
        save_succeeded: bool,
    ) -> Result<(), String> {
        let key = (
            session.id.clone(),
            session.created_at,
            activation_run_id.to_owned(),
        );
        let Some(mut pending) = self.pending_durable_links.lock().await.remove(&key) else {
            return Ok(());
        };
        // A later SDK timeout or Host postprocessing error cannot convert an
        // accepted Worker terminal into a different receipt status.
        let delivery = pending
            .link
            .durable_delivery_receipt()
            .ok_or_else(|| "broker terminal lost its exact durable receipt".to_string())?;
        let expected_status = effective_broker_terminal_status(
            delivery.terminal_status,
            pending.host_nested_wait_handoff,
        )?;
        if !save_succeeded || session.last_run_status().as_deref() != Some(expected_status) {
            return Ok(());
        }
        let store = self
            .actor_directory_store
            .lock()
            .recover_poison()
            .clone()
            .ok_or_else(|| "Host broker receipt store unavailable".to_string())?;
        let committed = store
            .commit_broker_terminal_receipt(session, activation_run_id)
            .await
            .map_err(|error| format!("Host broker receipt checkpoint unconfirmed: {error}"))?;
        if committed.broker_identity != delivery.broker_identity
            || committed.parent_mailbox != delivery.parent_mailbox
            || committed.broker_correlation_id != delivery.correlation_id
            || committed.message_ids != delivery.message_ids
            || committed.terminal_status != expected_status
        {
            return Err("Host broker receipt changed before ACK".into());
        }
        pending
            .link
            .acknowledge_durable_frames()
            .await
            .map_err(|error| format!("child broker durable ACK unconfirmed: {error}"))?;
        store
            .clear_acknowledged_broker_terminal_receipt(
                &session.id,
                session.created_at,
                activation_run_id,
                &delivery.parent_mailbox,
            )
            .await
            .map_err(|error| {
                format!("broker ACK succeeded but Host receipt cleanup failed: {error}")
            })
    }

    async fn execute_external_child(
        &self,
        session: &mut Session,
        job: &SpawnJob,
        event_tx: mpsc::Sender<AgentEvent>,
        cancel_token: CancellationToken,
    ) -> crate::runtime::runner::Result<()> {
        crate::session_app::child_session::named_profile::validate_named_profile(session)
            .map_err(|error| AgentError::LLM(error.to_string()))?;
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
        let actor_directory_store = self.actor_directory_store.lock().recover_poison().clone();
        let canonical_subagent_tool = self.canonical_subagent_tool.lock().recover_poison().clone();
        let actor_event_observer = self.actor_event_observer.lock().recover_poison().clone();
        let required_context = bamboo_domain::ChildContextBinding::from_session(session)
            .map_err(|error| AgentError::Budget(error.to_string()))?;
        if crate::session_app::child_session::named_profile::has_named_profile(session)
            && required_context.is_none()
        {
            return Err(AgentError::LLM(
                "named_profile_requires_strict_context_route".into(),
            ));
        }
        let assignment = required_context
            .as_ref()
            .map(|binding| binding.payload.required_assignment.clone())
            .unwrap_or_else(|| extract_assignment(session));
        let mut spec = self.build_live_spec(session, job).await;
        let scoped_route = self.scoped_placement_route(&spec)?;
        let strict_remote = scoped_route.as_ref();
        if strict_remote.is_some() && !matches!(spec.executor, ExecutorSpec::BambooRuntime) {
            return Err(remote_broker_unavailable());
        }
        let lineage = if strict_remote.is_some() {
            Some(
                remote_canonical_lineage(
                    session_inbox_runtime
                        .as_ref()
                        .ok_or_else(remote_broker_unavailable)?,
                    session,
                )
                .await?,
            )
        } else {
            None
        };
        let creation = if (matches!(spec.placement, Placement::Local) || strict_remote.is_some())
            && matches!(spec.executor, ExecutorSpec::BambooRuntime)
        {
            let binding = session_inbox_runtime.as_ref().ok_or_else(|| {
                AgentError::LLM(
                    "child creation identity unavailable: canonical host Storage required".into(),
                )
            })?;
            spec.capabilities.child_creation_identity = true;
            Some(canonical_child_creation(binding, session, job).await?)
        } else {
            None
        };
        // Mark the worker reusable + give it an idle timeout so it self-reaps if
        // orphaned. Warm bus workers are pooled per fingerprint and reused.
        spec.reusable = required_context.is_none();
        spec.capabilities.required_child_context = required_context.is_some();
        if required_context.is_some() {
            self.validate_required_child_context_route(session)
                .await
                .map_err(AgentError::LLM)?;
        }
        if required_context.is_some() {
            let names = self
                .native_tool_ceiling
                .as_ref()
                .unwrap()
                .observe(session)
                .await
                .map_err(AgentError::LLM)?;
            let birth = creation
                .as_ref()
                .ok_or_else(|| AgentError::LLM("native_tool_ceiling_birth_missing".into()))?;
            spec.capabilities.native_tool_ceiling_required = true;
            spec.capabilities.native_tool_ceiling =
                Some(bamboo_subagent::proto::NativeToolCeiling {
                    version: 1,
                    child_session_id: session.id.clone(),
                    parent_session_id: job.parent_session_id.clone(),
                    root_session_id: session.root_session_id.clone(),
                    created_at: birth.created_at,
                    spawn_depth: birth.spawn_depth,
                    project_id: project_id_for_actor_run(session)?,
                    tools: names,
                });
            // Positive native names are a leaf surface, never an expansion path.
            spec.capabilities.mcp = None;
            spec.capabilities.mcp_proxy = None;
            spec.capabilities.skills_dir = None;
            spec.capabilities.nested_spawn = false;
            spec.validate()
                .map_err(|_| AgentError::LLM("native_tool_ceiling_invalid".into()))?;
        }
        // Eligibility uses the actual Host callable ceiling, after the strict
        // built-in route and birth capability checks. A role label is not a grant.
        let first_reply_store = actor_directory_store.clone().filter(|_| {
            required_context.is_some()
                && crate::session_app::child_session::named_profile::has_named_profile(session)
                && matches!(spec.placement, Placement::Local)
                && matches!(spec.executor, ExecutorSpec::BambooRuntime)
                && spec.capabilities.native_tool_ceiling_required
                && spec
                    .capabilities
                    .native_tool_ceiling
                    .as_ref()
                    .is_some_and(|ceiling| {
                        ceiling.tools.is_empty()
                            || (spec.capabilities.read_only_enforced() && ceiling.tools == ["Glob"])
                    })
        });
        let plain_actor_store = if let Some(store) = first_reply_store {
            let root = bamboo_agent_core::storage::Storage::load_session(
                store.as_ref(),
                &session.root_session_id,
            )
            .await
            .map_err(|_| plain_actor_unsupported())?
            .ok_or_else(plain_actor_unsupported)?;
            if root.id != session.root_session_id || root.kind != bamboo_domain::SessionKind::Root {
                return Err(plain_actor_unsupported());
            }
            if root.root_orchestration_only_enabled() {
                if !plain_initial_history(session)
                    && (spec
                        .capabilities
                        .native_tool_ceiling
                        .as_ref()
                        .is_none_or(|ceiling| !ceiling.tools.is_empty())
                        || pre_ack_input(session)?.is_none())
                {
                    return Err(plain_actor_unsupported());
                }
                Some(store)
            } else if spec
                .capabilities
                .native_tool_ceiling
                .as_ref()
                .is_some_and(|ceiling| ceiling.tools == ["Glob"])
            {
                // Ordinary Glob remains legacy. Observe only its own mapped
                // initialization signals; never require a whole-tree snapshot.
                require_unowned_glob(store.as_ref(), session).await?;
                None
            } else {
                // Public bounded observation is only a no-replay check. It is
                // never an activation grant. Ordinary first runs stay legacy.
                let observed = store
                    .actor_subtree_snapshot(
                        ActorSnapshotPrincipal::host_owner(),
                        &session.root_session_id,
                        &session.id,
                        ActorSnapshotLimits::default(),
                    )
                    .await
                    .map_err(|_| plain_actor_unsupported())?;
                if observed
                    .nodes
                    .iter()
                    .any(|node| node.actor_id == session.id && node.activation.is_some())
                {
                    return Err(plain_actor_unsupported());
                }
                None
            }
        } else {
            None
        };
        let readonly_actor = plain_actor_store.is_some()
            && spec
                .capabilities
                .native_tool_ceiling
                .as_ref()
                .is_some_and(|ceiling| ceiling.tools == ["Glob"]);
        let local_history_tools = (plain_actor_store.is_none()
            && required_context.is_some()
            && crate::session_app::child_session::named_profile::has_named_profile(session)
            && matches!(spec.placement, Placement::Local)
            && matches!(spec.executor, ExecutorSpec::BambooRuntime))
        .then(|| spec.capabilities.native_tool_ceiling.as_ref())
        .flatten()
        .filter(|ceiling| {
            bamboo_subagent::proto::LocalToolMessages::supports_tools(
                &ceiling.tools,
                spec.capabilities.read_only_enforced(),
            )
        })
        .map(|ceiling| ceiling.tools.clone());
        if plain_actor_store.is_some() && !readonly_actor {
            self.require_initial_input_release_worker().await?;
            spec.capabilities.initial_input_release_required = true;
            spec.validate().map_err(|_| plain_actor_unsupported())?;
        }
        let mut readonly_messages = Vec::new();
        let mut plain_activation = None;
        if spec.limits.idle_timeout_secs.is_none() {
            spec.limits.idle_timeout_secs = Some(POOLED_IDLE_TIMEOUT_SECS);
        }
        ensure_read_only_activation_is_enforceable(&spec)?;
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
        let requested_permission_mode = session
            .agent_runtime_state
            .as_ref()
            .map(|state| state.effective_permission_mode())
            .unwrap_or_default();
        // Policy is captured per activation (not only when a worker is
        // provisioned), so reused local workers and resident remote/broker
        // workers observe the latest durable revision and bypass flag at the
        // next run boundary. Session grants are intentionally not inherited.
        let permission_policy = if let Some(config) = self.permission_config.as_ref() {
            ensure_no_active_scoped_session_denies(config, &session.id)?;
            let read_only_overlay = session
                .agent_runtime_state
                .as_ref()
                .is_some_and(|state| state.plan_mode.is_some())
                || spec.capabilities.read_only_enforced()
                || executor_has_read_only_permission_profile(&spec.executor);
            let resolution = bamboo_domain::resolve_permission_mode_with_read_only(
                requested_permission_mode,
                config.mode(),
                read_only_overlay,
            );
            let policy = serde_json::to_value(config.to_serializable()).map_err(|error| {
                AgentError::LLM(format!(
                    "failed to serialize permission policy for external executor: {error}"
                ))
            })?;
            Some(PermissionPolicyContext {
                revision: config.policy_revision(),
                requested_mode: resolution.requested.as_str().to_string(),
                effective_mode: resolution.effective.as_str().to_string(),
                bypass_permissions: resolution.bypass_permissions(),
                auto_approve_permissions: resolution.suppress_approval_prompts(),
                session_id: session.id.clone(),
                workspace_path: session.workspace.clone(),
                environment_lease: None,
                inherit_session_grants: false,
                policy,
            })
        } else {
            None
        };
        let provisioned_permission =
            spec.capabilities.permission_resolution().map_err(|error| {
                AgentError::LLM(format!("invalid provisioned permission posture: {error}"))
            })?;
        let policy_resolution = permission_policy
            .as_ref()
            .map(|context| {
                context.resolved_modes().map(|(requested, effective)| {
                    bamboo_domain::PermissionModeResolution {
                        requested,
                        effective,
                    }
                })
            })
            .transpose()
            .map_err(|error| {
                AgentError::LLM(format!("invalid host permission posture: {error}"))
            })?;
        let host_audit = bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata);
        let has_explicit_deny = self.permission_config.as_ref().is_some_and(|config| {
            bamboo_tools::permission::explicit_deny_policy_reason(&config.to_serializable())
                .is_some()
        });
        let expected_executor_mapping = expected_permission_executor_mapping(
            &spec.executor,
            policy_resolution.unwrap_or(provisioned_permission),
            has_explicit_deny,
        )?;
        let mut expected_permission_posture =
            expected_executor_mapping.map(|executor_mapping| ExpectedPermissionPosture {
                policy_revision: permission_policy
                    .as_ref()
                    .map(|context| context.revision)
                    .or_else(|| host_audit.as_ref().map(|audit| audit.policy_revision))
                    .unwrap_or_default(),
                resolution: policy_resolution.unwrap_or(provisioned_permission),
                expected_audit_revision: host_audit.as_ref().map(|audit| audit.audit_revision),
                executor_mapping,
            });

        // Backpressure: hold a concurrency slot for the lifetime of the *run*
        // (cancellation still proceeds — the cancel branch in drive() runs while
        // we hold the permit). Released when this fn returns, i.e. once the worker
        // is parked back into the pool, so idle workers don't pin slots.
        let _slot = if strict_remote.is_some() {
            tokio::select! { biased;
                _ = cancel_token.cancelled() => return Err(AgentError::Cancelled),
                permit = self.concurrency.acquire() => permit.map_err(|_| remote_broker_unavailable())?,
            }
        } else {
            self.concurrency
                .acquire()
                .await
                .map_err(|_| AgentError::LLM("actor concurrency limiter closed".into()))?
        };
        let _remote_subscription = if strict_remote.is_some() {
            Some(tokio::select! { biased;
                _ = cancel_token.cancelled() => return Err(AgentError::Cancelled),
                guard = self.strict_remote_subscription.lock() => guard,
            })
        } else {
            None
        };

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
        //   - Remote       (#1311): connect to a scoped broker peer, no spawn.
        //   - Schedulable  (#181): resolve a live worker from the registry, connect.
        let kind = match spec.placement {
            Placement::Remote { .. } => PlacementKind::Remote,
            Placement::Schedulable { .. } => PlacementKind::Schedulable,
            Placement::Local => PlacementKind::Local,
        };
        let remote = !matches!(kind, PlacementKind::Local);
        let scoped_registry = if remote {
            let store = actor_directory_store
                .as_ref()
                .ok_or_else(remote_broker_unavailable)?;
            Some(
                bamboo_storage::v2::FileHostRegistry::new(store.bamboo_home_dir().to_path_buf())
                    .await
                    .map_err(|_| remote_broker_unavailable())?,
            )
        } else {
            None
        };
        let scoped_tools = scoped_route.as_ref().map(|route| {
            let mut tools = route.requirements.required_tools.clone();
            if let Some(ceiling) = &spec.capabilities.native_tool_ceiling {
                tools.extend(ceiling.tools.iter().cloned());
            }
            tools
        });

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
            let execution_epoch = self.next_execution_epoch.fetch_add(1, Ordering::Relaxed) + 1;
            let (actor, mut client) = match kind {
                PlacementKind::Remote => {
                    let route = scoped_route
                        .as_ref()
                        .ok_or_else(remote_broker_unavailable)?;
                    let mailbox_id = route
                        .pinned_mailbox
                        .clone()
                        .ok_or_else(remote_broker_unavailable)?;
                    let tls = bamboo_broker::client_config_trusting_cert(&route.ca_cert_file)
                        .map_err(|_| remote_broker_unavailable())?;
                    let connect =
                        bamboo_broker::BrokerChildLink::connect_strict_with_tls_environment_lease(
                            &route.endpoint,
                            route.parent.clone(),
                            &route.token,
                            bamboo_subagent::AgentRef {
                                session_id: mailbox_id.clone(),
                                role: Some(route.worker_role.clone()),
                            },
                            tls,
                        );
                    let link = tokio::select! { biased;
                        _ = cancel_token.cancelled() => return Err(AgentError::Cancelled),
                        result = tokio::time::timeout(Duration::from_secs(30), connect) =>
                            result.map_err(|_| remote_broker_unavailable())?.map_err(|_| remote_broker_unavailable())?,
                    };
                    let record = AgentRecord {
                        agent_id: job.child_session_id.clone(),
                        role: spec.identity.role.clone(),
                        labels: Vec::new(),
                        endpoint: String::new(),
                        pid: 0,
                        version: String::new(),
                        started_at: chrono::Utc::now(),
                        lease_expires_at: chrono::Utc::now(),
                    };
                    (
                        PooledWorker {
                            worker: SpawnedChild::remote(record),
                            mailbox_id,
                            parked_at: None,
                        },
                        Box::new(link) as Box<dyn bamboo_subagent::ChildLink>,
                    )
                }
                PlacementKind::Schedulable => {
                    let route = scoped_route
                        .as_ref()
                        .ok_or_else(remote_broker_unavailable)?;
                    let registry = scoped_registry
                        .as_ref()
                        .ok_or_else(remote_broker_unavailable)?;
                    let mailbox_id = self
                        .select_scoped_worker(
                            route,
                            registry,
                            &session.id,
                            project_id.clone(),
                            scoped_tools
                                .as_ref()
                                .ok_or_else(remote_broker_unavailable)?,
                        )
                        .await?;
                    let tls = bamboo_broker::client_config_trusting_cert(&route.ca_cert_file)
                        .map_err(|_| remote_broker_unavailable())?;
                    let connect =
                        bamboo_broker::BrokerChildLink::connect_strict_with_tls_environment_lease(
                            &route.endpoint,
                            route.parent.clone(),
                            &route.token,
                            bamboo_subagent::AgentRef {
                                session_id: mailbox_id.clone(),
                                role: Some(route.worker_role.clone()),
                            },
                            tls,
                        );
                    let link = tokio::select! { biased;
                        _ = cancel_token.cancelled() => return Err(AgentError::Cancelled),
                        result = tokio::time::timeout(Duration::from_secs(30), connect) =>
                            result.map_err(|_| remote_broker_unavailable())?.map_err(|_| remote_broker_unavailable())?,
                    };
                    let actor = PooledWorker {
                        worker: SpawnedChild::remote(AgentRecord {
                            agent_id: mailbox_id.clone(),
                            role: spec.identity.role.clone(),
                            labels: Vec::new(),
                            endpoint: route.endpoint.clone(),
                            pid: 0,
                            version: String::new(),
                            started_at: chrono::Utc::now(),
                            lease_expires_at: chrono::Utc::now(),
                        }),
                        mailbox_id,
                        parked_at: None,
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

            // Recover an older Run's already Host-checkpointed Event/Outcome
            // receipts before dispatching a successor. The private V2 ledger
            // binds exact Child birth, Run, transcript prefix and broker
            // Maildir identity. A different broker or an unproven final save
            // blocks this activation instead of deleting the wrong mailbox.
            if let Some(store) = actor_directory_store.as_ref() {
                let recovery = async {
                    let receipts = store
                        .recover_broker_terminal_receipts(session)
                        .await
                        .map_err(|error| {
                            format!("Host broker receipt recovery blocked: {error}")
                        })?;
                    for receipt in receipts {
                        if client.broker_parent_mailbox() != Some(receipt.parent_mailbox.as_str()) {
                            return Err(
                                "old Child broker parent mailbox changed before ACK".to_string()
                            );
                        }
                        client
                            .acknowledge_recovered_durable_frames(
                                &receipt.broker_identity,
                                &receipt.message_ids,
                            )
                            .await
                            .map_err(|error| {
                                format!("old Child broker ACK unconfirmed: {error}")
                            })?;
                        store
                            .clear_acknowledged_broker_terminal_receipt(
                                &receipt.session_id,
                                receipt.created_at,
                                &receipt.activation_run_id,
                                &receipt.parent_mailbox,
                            )
                            .await
                            .map_err(|error| {
                                format!("old Child broker ACK cleanup failed: {error}")
                            })?;
                    }
                    Ok::<(), String>(())
                }
                .await;
                if let Err(error) = recovery {
                    if !remote {
                        actor.worker.kill().await;
                    }
                    return Err(AgentError::LLM(error));
                }
            }

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
                (Some(binding), Some(run_id)) if plain_actor_store.is_none() && !remote => {
                    match claim_canonical_deliveries(binding, session, run_id, usize::MAX, None)
                        .await
                    {
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
            let mut initial_session_messages = initial_pairs
                .iter()
                .map(|(_, delivery)| delivery.clone())
                .collect::<Vec<_>>();
            let mut initial_inflight_claims = initial_pairs
                .into_iter()
                .map(|(claim, _)| claim)
                .collect::<VecDeque<_>>();
            // Recompute after claim reconciliation: a warm retry may have had a
            // canonical receipt whose transcript proof was restored above.
            let mut messages = if let Some(binding) = &required_context {
                binding
                    .validate_messages(&session.id, &session.messages)
                    .map_err(|error| AgentError::Budget(error.to_string()))?;
                session
                    .messages
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|_| {
                        AgentError::Budget(
                            bamboo_domain::ChildContextPacketError::Invalid.to_string(),
                        )
                    })?
            } else {
                session
                    .messages
                    .iter()
                    .filter_map(|message| serde_json::to_value(message).ok())
                    .collect()
            };

            if strict_remote.is_some() {
                let current =
                    remote_canonical_lineage(session_inbox_runtime.as_ref().unwrap(), session)
                        .await;
                if current.as_ref().ok() != lineage.as_ref()
                    || bound_activation_run_id
                        .as_deref()
                        .map_or(true, str::is_empty)
                    || cancel_token.is_cancelled()
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
                    return Err(if cancel_token.is_cancelled() {
                        AgentError::Cancelled
                    } else {
                        remote_broker_unavailable()
                    });
                }
            }
            if let Some(expected) = creation.as_ref() {
                let checked =
                    canonical_child_creation(session_inbox_runtime.as_ref().unwrap(), session, job)
                        .await;
                if checked.as_ref().ok() != Some(expected) {
                    if let (Some(binding), Some(run_id)) = (
                        session_inbox_runtime.as_ref(),
                        bound_activation_run_id.as_deref(),
                    ) {
                        binding
                            .router
                            .detach_delivery_sink(&job.child_session_id, run_id)
                            .await;
                    }
                    actor.worker.kill().await;
                    return Err(AgentError::LLM(
                        "child creation identity changed before dispatch".into(),
                    ));
                }
            }
            if let Some(store) = &plain_actor_store {
                let started = async {
                    // The Run posture was resolved from Host configuration
                    // before provisioning. Bind that exact revision and the
                    // Host-selected local placement before claiming authority;
                    // a worker event or display placement cannot supply either.
                    let policy = permission_policy
                        .as_ref()
                        .ok_or_else(plain_actor_unsupported)?;
                    let config = self
                        .permission_config
                        .as_ref()
                        .ok_or_else(plain_actor_unsupported)?;
                    if config.policy_revision() != policy.revision
                        || !matches!(spec.placement, Placement::Local)
                    {
                        return Err(plain_actor_unsupported());
                    }
                    store
                        .bind_control_plane(
                            &session.id,
                            bamboo_domain::ActorControlPlaneBinding {
                                policy_revision: Some(policy.revision),
                                placement_intent: Some(bamboo_domain::ActorPlacementIntent::Local),
                            },
                        )
                        .await
                        .map_err(|error| AgentError::LLM(error.to_string()))?;
                    PlainActorActivation::start_and_prepare(
                        store.clone(),
                        session,
                        session_inbox_runtime.as_ref().unwrap(),
                        bound_activation_run_id.as_deref(),
                        self.permission_config.clone(),
                        !readonly_actor,
                        spec.capabilities
                            .initial_input_release_required
                            .then_some(actor.mailbox_id.as_str()),
                    )
                    .await
                }
                .await;
                match started {
                    Ok((activation, prepared)) => {
                        if let Some((prefix, delivery)) = prepared {
                            messages = prefix;
                            initial_session_messages = vec![delivery];
                        }
                        plain_activation = Some(activation);
                    }
                    Err(error) => {
                        if let Some(run_id) = bound_activation_run_id.as_deref() {
                            session_inbox_runtime
                                .as_ref()
                                .unwrap()
                                .router
                                .detach_delivery_sink(&job.child_session_id, run_id)
                                .await;
                        }
                        actor.worker.kill().await;
                        return Err(error);
                    }
                }
            }
            let mut logical_identity = logical_identity_for_actor_run(session, job);
            logical_identity.creation = creation.clone();
            let run_permission_policy = if strict_remote.is_some() {
                let prepared = async {
                    let mut policy = permission_policy.clone().ok_or_else(|| {
                        AgentError::LLM("remote_environment_permission_policy_missing".into())
                    })?;
                    let workspace = session.workspace.as_deref().ok_or_else(|| {
                        AgentError::LLM("remote_environment_workspace_missing".into())
                    })?;
                    let run_id = bound_activation_run_id.as_deref().ok_or_else(|| {
                        AgentError::LLM("remote_environment_activation_missing".into())
                    })?;
                    let lease = bamboo_subagent::environment::EnvironmentLease::capture(
                        workspace,
                        &session.id,
                        run_id,
                        execution_epoch,
                    )
                    .await
                    .map_err(|error| AgentError::LLM(error.into()))?;
                    policy.workspace_path = None;
                    policy.environment_lease = Some(lease);
                    Ok::<_, AgentError>(Some(policy))
                }
                .await;
                match prepared {
                    Ok(policy) => policy,
                    Err(error) => {
                        if let (Some(binding), Some(run_id)) = (
                            session_inbox_runtime.as_ref(),
                            bound_activation_run_id.as_deref(),
                        ) {
                            binding
                                .router
                                .detach_delivery_sink(&job.child_session_id, run_id)
                                .await;
                        }
                        return Err(error);
                    }
                }
            } else {
                permission_policy.clone()
            };
            let mut run_spec = RunSpec {
                // Cloned (not moved) so a retry can re-dispatch to a fresh worker.
                assignment: assignment.clone(),
                logical_session: Some(logical_identity),
                project_id: project_id.clone(),
                // Creation/update already resolves the child's explicit
                // selection and own role preference into this field. Do
                // not infer effort from a possibly inherited model_ref.
                reasoning_effort: if !remote && matches!(self.executor, ExecutorSpec::BambooRuntime)
                {
                    session
                        .reasoning_effort
                        .map(|effort| effort.as_str().to_owned())
                } else {
                    None
                },
                permission_policy: run_permission_policy,
                messages,
                activation_run_id: bound_activation_run_id.clone(),
                execution_epoch,
                initial_session_messages,
                secrets: run_secrets.clone(),
            };
            let mut active_lease = if remote {
                let reserve = ActivePlacementLease::reserve(
                    scoped_registry
                        .as_ref()
                        .ok_or_else(remote_broker_unavailable)?
                        .clone(),
                    actor_directory_store
                        .as_ref()
                        .ok_or_else(remote_broker_unavailable)?
                        .clone(),
                    scoped_route
                        .as_ref()
                        .ok_or_else(remote_broker_unavailable)?
                        .clone(),
                    actor.mailbox_id.clone(),
                    session.id.clone(),
                    bound_activation_run_id
                        .clone()
                        .ok_or_else(remote_broker_unavailable)?,
                    project_id.clone(),
                    scoped_tools.clone().ok_or_else(remote_broker_unavailable)?,
                    &cancel_token,
                )
                .await;
                match reserve {
                    Ok(lease) => Some(lease),
                    Err(error) => {
                        if let (Some(binding), Some(run_id)) = (
                            session_inbox_runtime.as_ref(),
                            bound_activation_run_id.as_deref(),
                        ) {
                            binding
                                .router
                                .detach_delivery_sink(&job.child_session_id, run_id)
                                .await;
                        }
                        return Err(error);
                    }
                }
            } else {
                None
            };
            if let Some(lease) = active_lease.as_mut() {
                let seeded = async {
                    let binding = session_inbox_runtime
                        .as_ref()
                        .ok_or_else(remote_broker_unavailable)?;
                    let run_id = bound_activation_run_id
                        .as_deref()
                        .ok_or_else(remote_broker_unavailable)?;
                    let pairs = claim_canonical_deliveries(
                        binding,
                        session,
                        run_id,
                        usize::MAX,
                        Some((lease.actor_store.as_ref(), &lease.fence)),
                    )
                    .await?;
                    if let Some(binding) = &required_context {
                        binding
                            .validate_messages(&session.id, &session.messages)
                            .map_err(|error| AgentError::Budget(error.to_string()))?;
                    }
                    run_spec.messages = session
                        .messages
                        .iter()
                        .map(serde_json::to_value)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|_| remote_broker_unavailable())?;
                    run_spec.initial_session_messages =
                        pairs.iter().map(|(_, delivery)| delivery.clone()).collect();
                    initial_inflight_claims = pairs
                        .into_iter()
                        .map(|(claim, _)| claim)
                        .collect::<VecDeque<_>>();
                    Ok::<(), AgentError>(())
                }
                .await;
                if let Err(error) = seeded {
                    if let Err(rollback_error) = lease.abort_before_dispatch().await {
                        tracing::warn!(stage = "claim_pre_dispatch_rollback", reason = %rollback_error, "remote claim rollback failed");
                    }
                    if let (Some(binding), Some(run_id)) = (
                        session_inbox_runtime.as_ref(),
                        bound_activation_run_id.as_deref(),
                    ) {
                        binding
                            .router
                            .detach_delivery_sink(&job.child_session_id, run_id)
                            .await;
                    }
                    return Err(error);
                }
            }
            let mut run_send_invoked = false;
            let dispatch = async {
                if let Some(activation) = plain_activation
                    .as_ref()
                    .filter(|activation| activation.initial_input.is_some())
                {
                    activation.refresh_continuation_posture(
                        session,
                        &run_spec,
                        &mut expected_permission_posture,
                    )?;
                }
                send_actor_run(&mut *client, run_spec.clone(), &mut run_send_invoked).await
            }
            .await;
            if let Err(e) = dispatch {
                if remote {
                    tracing::warn!(stage = "run_dispatch", "remote placement admission failed");
                }
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
                if let Some(activation) = &plain_activation {
                    activation.finish(ActorActivationFinish::Failed).await?;
                }
                if let Some(lease) = active_lease.as_mut() {
                    match lease.abort_if_run_unsent(run_send_invoked).await {
                        Ok(false) => {
                            tracing::warn!(stage = "run_dispatch_unconfirmed", "remote Run send failed after publication became possible; retaining Actor activation and slot until terminal proof or expiry");
                        }
                        Ok(true) => {}
                        Err(rollback_error) => {
                            tracing::warn!(stage = "run_pre_dispatch_rollback", reason = %rollback_error, "remote Run rollback failed");
                        }
                    }
                }
                return Err(e);
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
                cancel_token: active_lease
                    .as_ref()
                    .map_or(&cancel_token, |lease| &lease.run_cancel),
                live_rx: &mut live_rx,
                delivery_rx: &mut delivery_rx,
                logical_session: session,
                expected_permission_posture: expected_permission_posture.clone(),
                expected_creation: creation.as_ref(),
                session_inbox_runtime: session_inbox_runtime.as_ref(),
                actor_directory_store: actor_directory_store.as_deref(),
                canonical_subagent_tool: canonical_subagent_tool.clone(),
                activation_run_id: bound_activation_run_id.as_deref(),
                execution_epoch,
                expected_source_actor_id: &actor.mailbox_id,
                initial_inflight_claims,
                // First-frame watchdog for EVERY placement: a wedged-but-connected
                // worker (subscribed ≠ serving — e.g. stuck on a prior LLM call) emits
                // no first frame; without a deadline drive() blocks forever. Bounding it
                // turns the "running-but-unresponsive" hang into a recoverable
                // WorkerUnresponsive (reap+respawn local / re-pick schedulable / error
                // on a fixed remote endpoint).
                plain_actor: plain_activation.is_some(),
                remote_environment_lease: strict_remote.is_some(),
                readonly_output: if readonly_actor {
                    Some(&mut readonly_messages)
                } else {
                    None
                },
                local_history_tools: local_history_tools.as_deref(),
                local_history_read_only: spec.capabilities.read_only_enforced(),
                plain_input: plain_activation.as_ref(),
                canonical_activation: active_lease.as_ref().map(|lease| &lease.fence),
                canonical_placement_ref: active_lease.as_ref().map(|lease| &lease.placement_ref),
                actor_event_observer: actor_event_observer.as_deref(),
                // BrokerChildLink replaces the actual correlation for each Run;
                // direct legacy WS Terminal frames carry no such identity.
                plain_run: self
                    .bus
                    .as_ref()
                    .map(|_| (&run_spec, &self.next_execution_epoch)),
                first_frame_timeout: Some(WORKER_FIRST_FRAME_TIMEOUT),
            })
            .await;
            let mut result = if active_lease
                .as_ref()
                .is_some_and(ActivePlacementLease::lost)
            {
                Err(remote_broker_unavailable())
            } else {
                result
            };
            if local_history_tools.is_none() {
                let reply = result
                    .as_ref()
                    .ok()
                    .and_then(|reply| reply.as_deref())
                    .filter(|reply| !reply.is_empty())
                    .map(str::to_owned);
                if let (Some(lease), Some(reply)) = (active_lease.as_ref(), reply) {
                    if let Err(error) = append_fenced_remote_reply(
                        lease.actor_store.as_ref(),
                        &lease.fence,
                        session,
                        &reply,
                    )
                    .await
                    {
                        result = Err(error);
                    }
                }
            }
            if remote {
                if let Err(error) = &result {
                    let code = match error {
                        AgentError::LLM(message) if message.starts_with("actor event") => {
                            "actor_event"
                        }
                        AgentError::LLM(message) if message.starts_with("remote_actor") => {
                            "remote_actor"
                        }
                        AgentError::LLM(message) if message.starts_with("remote_environment") => {
                            if message.len() <= 128
                                && message.bytes().all(|byte| {
                                    byte.is_ascii_lowercase()
                                        || byte.is_ascii_digit()
                                        || byte == b'_'
                                })
                            {
                                message.as_str()
                            } else {
                                "environment"
                            }
                        }
                        AgentError::LLM(message) if message.contains("permission posture") => {
                            "permission_posture"
                        }
                        AgentError::LLM(message) if message.contains("broker") => "broker",
                        AgentError::LLM(_) => "other_llm",
                        AgentError::WorkerUnresponsive(_) => "unresponsive",
                        AgentError::Cancelled => "cancelled",
                        _ => "other",
                    };
                    tracing::warn!(stage = "actor_drive", code, "remote actor run failed");
                }
            }
            if let Some(lease) = active_lease.as_mut() {
                let accepted_terminal = client
                    .durable_delivery_receipt()
                    .map(|receipt| receipt.terminal_status);
                if let Err(error) = lease
                    .complete(
                        remote_activation_finish(&result, accepted_terminal),
                        client.has_pending_durable_terminal(),
                    )
                    .await
                {
                    tracing::warn!(
                        stage = "actor_complete",
                        "remote placement admission failed"
                    );
                    if let (Some(binding), Some(run_id)) = (
                        session_inbox_runtime.as_ref(),
                        bound_activation_run_id.as_deref(),
                    ) {
                        binding
                            .router
                            .detach_delivery_sink(&job.child_session_id, run_id)
                            .await;
                    }
                    drop(live_guard);
                    drop(client); // No Host checkpoint proof means no broker ACK.
                    drop(active_lease);
                    return Err(error);
                }
            }
            drop(active_lease);
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
            // Keep only a fully processed broker terminal's exact mailbox
            // receipts alive until the SDK has saved this Child's canonical
            // transcript and final status. No run fence means no Host proof.
            if client.has_pending_durable_terminal() {
                if let Some(run_id) = bound_activation_run_id.as_deref() {
                    let key = (session.id.clone(), session.created_at, run_id.to_owned());
                    let host_nested_wait_handoff = session
                        .metadata
                        .get("runtime.actor_nested_wait_handoff")
                        .is_some_and(|value| value == "true");
                    let mut pending = self.pending_durable_links.lock().await;
                    if pending.contains_key(&key) {
                        return Err(AgentError::LLM(
                            "child broker terminal receipt already awaits Host checkpoint".into(),
                        ));
                    }
                    pending.insert(
                        key,
                        PendingDurableChildLink {
                            link: client,
                            host_nested_wait_handoff,
                        },
                    );
                } else {
                    // A Worker-local transcript is not Host checkpoint proof.
                    drop(client);
                }
            } else {
                drop(client);
            }

            // Retry only a local worker that we can kill. A silent remote
            // Worker may still be executing its first Run; starting the same
            // Actor on another Host before a terminal fence would duplicate it.
            if required_context.is_none()
                && attempt == 0
                && matches!(kind, PlacementKind::Local)
                && matches!(result, Err(AgentError::WorkerUnresponsive(_)))
            {
                tracing::warn!(
                    "actor child {} got no first frame; reaping the worker and respawning once",
                    job.child_session_id
                );
                actor.worker.kill().await;
                attempt += 1;
                continue;
            }
            break (result, actor);
        };

        let result = if strict_remote.is_some() {
            result.map_err(|error| match &error {
                AgentError::Cancelled => error,
                AgentError::LLM(message) => remote_environment_error_code(message)
                    .map(|code| AgentError::LLM(code.to_owned()))
                    .unwrap_or_else(remote_broker_unavailable),
                _ => remote_broker_unavailable(),
            })
        } else {
            result
        };

        // Park the warm worker for reuse on a clean run, or kill it on
        // error/cancel (a wedged worker must not be reused). Remote / schedulable
        // workers are registry-managed — never ours to pool/kill, just drop.
        if remote {
            drop(actor);
        } else if required_context.is_some() {
            actor.worker.kill().await;
        } else {
            match &result {
                Ok(_) => self.release_bus_worker(&pool_key, actor).await,
                Err(_) => actor.worker.kill().await,
            }
        }

        if let Some(activation) = plain_activation {
            return match result {
                Ok(Some(text)) if !text.is_empty() => {
                    if readonly_actor {
                        let messages = std::mem::take(&mut readonly_messages);
                        let committed = activation
                            .store
                            .append_actor_transcript(bamboo_storage::ActorTranscriptAppend {
                                fence: activation.fence.clone(),
                                expected_created_at: activation.created_at,
                                expected_messages: session.messages.clone(),
                                expected_provider_transcript: activation
                                    .provider_transcript
                                    .clone(),
                                messages: messages.clone(),
                                native_groups: Vec::new(),
                            })
                            .await;
                        return match committed {
                            Ok(committed) => {
                                *session = committed;
                                for message in messages {
                                    let _ = event_tx
                                        .send(AgentEvent::MessageAppended {
                                            session_id: session.id.clone(),
                                            message_id: message.id,
                                            role: message.role,
                                            content: message.content,
                                            created_at: message.created_at,
                                        })
                                        .await;
                                }
                                activation.finish(ActorActivationFinish::Succeeded).await
                            }
                            Err(error) => {
                                let _ = activation.finish(ActorActivationFinish::Failed).await;
                                Err(AgentError::LLM(format!(
                                    "actor reply commit failed: {error}"
                                )))
                            }
                        };
                    }
                    match activation.append_reply(session, text, &event_tx).await {
                        Ok(()) => activation.finish(ActorActivationFinish::Succeeded).await,
                        Err(error) => {
                            // OutcomeUnconfirmed is an error, never ordinary-save fallback.
                            let _ = activation.finish(ActorActivationFinish::Failed).await;
                            Err(AgentError::LLM(format!(
                                "actor reply commit failed: {error}"
                            )))
                        }
                    }
                }
                other => {
                    let outcome = if matches!(&other, Err(AgentError::Cancelled)) {
                        ActorActivationFinish::Cancelled
                    } else {
                        ActorActivationFinish::Failed
                    };
                    activation.finish(outcome).await?;
                    match other {
                        Err(error) => Err(error),
                        _ => Err(plain_actor_unsupported()),
                    }
                }
            };
        }

        // Write-back: persist the actor's final reply onto the child session so
        // the transcript survives and the NEXT activation sees it as history.
        // (run_child_spawn saves the session right after we return.)
        match result {
            Ok(Some(text)) => {
                if !remote && local_history_tools.is_none() && !text.is_empty() {
                    session.add_message(bamboo_agent_core::Message::assistant(text, None));
                }
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

// This is a conservative no-replay observation, not Actor authority. The
// canonical mapping comes from Storage; record/marker names match its writer.
async fn require_unowned_glob(
    store: &bamboo_storage::SessionStoreV2,
    session: &Session,
) -> Result<(), AgentError> {
    let rel = store
        .resolve_rel_path(&session.id)
        .await
        .ok_or_else(plain_actor_unsupported)?;
    if !store.bamboo_home_dir().is_absolute()
        || rel
            != format!(
                "sessions/{}/children/{}",
                session.root_session_id, session.id
            )
    {
        return Err(plain_actor_unsupported());
    }
    let directory = store.bamboo_home_dir().join(rel);
    let mut checked = PathBuf::new();
    for component in directory.components() {
        if !matches!(
            component,
            std::path::Component::RootDir | std::path::Component::Normal(_)
        ) {
            return Err(plain_actor_unsupported());
        }
        checked.push(component.as_os_str());
        if !tokio::fs::symlink_metadata(&checked)
            .await
            .map_err(|_| plain_actor_unsupported())?
            .is_dir()
        {
            return Err(plain_actor_unsupported());
        }
    }
    for name in ["actor-authority.json", "actor-authority.initialized.json"] {
        match tokio::fs::symlink_metadata(directory.join(name)).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return Err(plain_actor_unsupported()),
        }
    }
    Ok(())
}

// Zero-tool or read-only Glob named Child of a durable Ultra Root.
// Glob uses its typed tail; fresh zero-tool supports two bounded corrections.
// No lease renewal/reclaim, raw steering, remote activation or automatic restart.
struct PlainActorActivation {
    store: Arc<bamboo_storage::SessionStoreV2>,
    fence: ActorActivationFence,
    inbox_generation: u64,
    created_at: chrono::DateTime<chrono::Utc>,
    provider_transcript: bamboo_domain::ProviderTranscriptState,
    input_consumer: SessionInboxConsumerId,
    input_deadline: chrono::DateTime<chrono::Utc>,
    actor_deadline: chrono::DateTime<chrono::Utc>,
    initial_release_required: bool,
    input_inbox: bamboo_storage::FileSessionInbox,
    permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    initial_input: Option<SessionInboxOwnedClaim>,
    recovering_pre_ack: bool,
}
type PlainInitialDelivery = (Vec<serde_json::Value>, SessionMessageDelivery);
fn plain_actor_unsupported() -> AgentError {
    AgentError::LLM("This Actor Child supports a fresh plain activation, two bounded corrections while Running, a Failed retry with one new input, or verified expired pre-ACK input recovery through run(reset_to_last_user=false). Other continuation is unsupported; durable history is preserved.".into())
}
fn plain_initial_history(session: &Session) -> bool {
    !session.messages.iter().any(|message| {
        matches!(message.role, Role::Assistant | Role::Tool)
            || message
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.get("_bamboo_owned_input_checkpoint").is_some())
    }) && session.provider_transcript.is_empty()
        && session.session_inbox_admission().is_none()
        && session.pending_injected_messages().is_none()
}
// Selection only. Storage's closed raw bookkeeper/current-prefix validator is
// still mandatory after the actual replacement claim; this metadata is no grant.
fn pre_ack_input(
    session: &Session,
) -> Result<Option<(bamboo_domain::SessionMessageId, u64)>, AgentError> {
    let Some(message) = session.messages.last() else {
        return Ok(None);
    };
    let Some(proof) = message
        .metadata
        .as_ref()
        .and_then(|v| v.get("_bamboo_owned_input_checkpoint"))
    else {
        return Ok(None);
    };
    let id = bamboo_domain::SessionMessageId::parse(message.id.clone())
        .map_err(|_| plain_actor_unsupported())?;
    let generation = proof
        .get("generation")
        .and_then(serde_json::Value::as_u64)
        .filter(|g| *g > 0)
        .ok_or_else(plain_actor_unsupported)?;
    if message.role != Role::User
        || session
            .messages
            .iter()
            .filter(|m| m.id == message.id)
            .count()
            != 1
        || !session
            .session_inbox_admission()
            .is_some_and(|cursor| cursor.contains(&id))
        || !session.provider_transcript.is_empty()
        || session.pending_injected_messages().is_some()
        || session
            .messages
            .iter()
            .any(|m| m.role == Role::Tool || m.tool_calls.is_some())
    {
        return Err(plain_actor_unsupported());
    }
    Ok(Some((id, generation)))
}
fn qualified_pre_ack_owner(
    entry: &bamboo_domain::ActorDirectoryEntry,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let Some(old) = entry.activation.as_ref() else {
        return false;
    };
    let recoverable = (entry.actor.state == ActorLogicalState::Failed
        && old.status == bamboo_domain::ActorActivationStatus::Failed)
        || (entry.actor.state == ActorLogicalState::Active
            && old.status.is_live()
            && old.lease_expires_at <= now);
    let trusted = old.placement_ref.as_ref().is_some_and(|placement| {
        placement.class == bamboo_domain::ActorPlacementClass::Local
            && placement
                .lease_id
                .strip_prefix("owned-initial-release-v1:")
                .is_some_and(|worker| {
                    !worker.is_empty()
                        && worker.len() <= 256
                        && !worker.contains("..")
                        && worker.bytes().all(|b| {
                            b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.'
                        })
                })
    });
    recoverable && trusted
}
async fn expired_pre_ack_input(
    inbox: &bamboo_storage::FileSessionInbox,
    session: &Session,
) -> Result<Option<(bamboo_domain::SessionMessageId, u64)>, AgentError> {
    let Some((id, generation)) = pre_ack_input(session)? else {
        return Ok(None);
    };
    let backlog = inbox
        .inspect(&session.id)
        .await
        .map_err(|_| plain_actor_unsupported())?;
    let leases = inbox
        .inspect_owned_leases(&session.id, 2, chrono::Utc::now())
        .await
        .map_err(|_| plain_actor_unsupported())?;
    if backlog.pending != 0
        || backlog.claimed != 1
        || backlog.generation != generation
        || backlog.oldest_generation != Some(generation)
        || !backlog.activation_pending()
        || leases.len() != 1
        || !leases[0].expired
        || leases[0].generation != generation
        || inbox
            .was_admitted(&session.id, &id)
            .await
            .map_err(|_| plain_actor_unsupported())?
    {
        return Err(plain_actor_unsupported());
    }
    Ok(Some((id, generation)))
}
impl PlainActorActivation {
    // Callee-side boxing keeps this storage chain out of the already large
    // external-runner future without changing its cancellation semantics.
    fn start<'a>(
        store: Arc<bamboo_storage::SessionStoreV2>,
        session: &'a Session,
        binding: &'a SessionInboxRuntimeBinding,
        run_id: Option<&'a str>,
        permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
        allow_failed_retry: bool,
        release_worker: Option<&'a str>,
    ) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<Self, AgentError>> + Send + 'a>>
    {
        Box::pin(async move {
            let run_id = run_id.ok_or_else(plain_actor_unsupported)?;
            let backlog = binding
                .inbox
                .inspect(&session.id)
                .await
                .map_err(|_| plain_actor_unsupported())?;
            let entry = store
                .ensure_actor(&session.id)
                .await
                .map_err(|error| AgentError::LLM(error.to_string()))?;
            let config = permission_config
                .as_ref()
                .ok_or_else(plain_actor_unsupported)?;
            if entry.actor.policy_revision != Some(config.policy_revision())
                || entry.actor.placement_intent != Some(bamboo_domain::ActorPlacementIntent::Local)
            {
                return Err(plain_actor_unsupported());
            }
            let fresh = entry.actor.current_attempt == 0
                && entry.actor.state == ActorLogicalState::Cold
                && entry.activation.is_none()
                && backlog.pending == 0
                && backlog.claimed == 0
                && backlog.generation == 0;
            let retry = allow_failed_retry
                && entry.actor.state == ActorLogicalState::Failed
                && entry.actor.current_attempt > 0
                && entry.activation.as_ref().is_some_and(|old| {
                    old.status == bamboo_domain::ActorActivationStatus::Failed
                        && backlog.generation > old.inbox_generation
                })
                && backlog.pending == 1
                && backlog.claimed == 0
                && backlog.oldest_generation == Some(backlog.generation)
                && backlog.activation_pending();
            let input_inbox = bamboo_storage::FileSessionInbox::new(
                store.clone(),
                bamboo_domain::SessionInboxLimits::default(),
            );
            let now = chrono::Utc::now();
            let recovery = if allow_failed_retry
                && release_worker.is_some()
                && qualified_pre_ack_owner(&entry, now)
            {
                expired_pre_ack_input(&input_inbox, session).await?
            } else {
                None
            };
            if recovery.is_some()
                && (!entry.actor.matches_session(session)
                    || entry.actor.project_id
                        != project_id_for_actor_run(session)?.map(|id| id.to_string()))
            {
                return Err(plain_actor_unsupported());
            }
            if recovery.is_none() && ((!fresh && !retry) || !plain_initial_history(session)) {
                return Err(plain_actor_unsupported());
            }
            let duration = crate::runtime::execution::spawn::watchdog_policy_for_session(session)
                .max_total_secs
                .checked_add(60)
                .and_then(chrono::Duration::try_seconds)
                .ok_or_else(plain_actor_unsupported)?;
            let expires = now
                .checked_add_signed(duration)
                .ok_or_else(plain_actor_unsupported)?;
            let input_consumer = SessionInboxConsumerId::new();
            let input_deadline = expires.min(now + chrono::Duration::hours(1));
            let recovered = if let Some((id, generation)) = &recovery {
                if !binding.router.owns_run(&session.id, run_id).await {
                    return Err(plain_actor_unsupported());
                }
                let lease = SessionInboxLeaseRequest {
                    consumer: input_consumer.clone(),
                    now,
                    duration: input_deadline - now,
                };
                let mut claims = input_inbox
                    .claim_owned(&session.id, 1, Some(run_id), &lease)
                    .await
                    .map_err(|_| plain_actor_unsupported())?;
                let claim = claims.pop().ok_or_else(plain_actor_unsupported)?;
                if claim.claim.envelope.id != *id
                    || claim.claim.generation != *generation
                    || !matches!(&claim.claim.envelope.body, bamboo_domain::SessionMessageBody::Content(content)
                        if content.parts.is_empty() && !content.text.trim().is_empty() && content.text.len() <= 8192)
                {
                    return Err(plain_actor_unsupported());
                }
                Some(claim)
            } else {
                None
            };
            let claimed = store
                .claim_activation(&ActorActivationClaim {
                    actor_id: session.id.clone(),
                    run_id: run_id.into(),
                    lease_owner: input_consumer.as_str().into(),
                    lease_expires_at: expires,
                    inbox_generation: recovered
                        .as_ref()
                        .map_or(backlog.generation, |claim| claim.claim.generation),
                    placement_ref: release_worker.map(|worker| bamboo_domain::ActorPlacementRef {
                        class: bamboo_domain::ActorPlacementClass::Local,
                        // Written only after the real worker capability probe
                        // and spawn. Opaque provenance, never a live-owner grant.
                        lease_id: format!("owned-initial-release-v1:{worker}"),
                        slot_epoch: None,
                    }),
                    now,
                })
                .await
                .map_err(|error| AgentError::LLM(error.to_string()))?;
            let activation = Self {
                store,
                fence: claimed.fence(),
                inbox_generation: claimed.inbox_generation,
                created_at: session.created_at,
                provider_transcript: session.provider_transcript.clone(),
                input_consumer,
                input_deadline,
                actor_deadline: claimed.lease_expires_at,
                initial_release_required: release_worker.is_some(),
                input_inbox,
                permission_config,
                initial_input: recovered,
                recovering_pre_ack: recovery.is_some(),
            };
            if let Err(error) = activation
                .store
                .start_activation(&activation.fence, chrono::Utc::now())
                .await
            {
                let _ = activation.finish(ActorActivationFinish::Failed).await;
                return Err(AgentError::LLM(error.to_string()));
            }
            Ok(activation)
        })
    }
    fn start_and_prepare<'a>(
        store: Arc<bamboo_storage::SessionStoreV2>,
        session: &'a mut Session,
        binding: &'a SessionInboxRuntimeBinding,
        run_id: Option<&'a str>,
        permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
        allow_failed_retry: bool,
        release_worker: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<
            impl std::future::Future<
                    Output = Result<(Self, Option<PlainInitialDelivery>), AgentError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mut activation = Self::start(
                store,
                session,
                binding,
                run_id,
                permission_config,
                allow_failed_retry,
                release_worker,
            )
            .await?;
            if activation.inbox_generation == 0 {
                return Ok((activation, None));
            }
            let prepared = async {
                let run_id = run_id.ok_or_else(plain_actor_unsupported)?;
                let backlog = activation
                    .input_inbox
                    .inspect(&session.id)
                    .await
                    .map_err(|_| plain_actor_unsupported())?;
                let expected = if activation.recovering_pre_ack {
                    (0, 1)
                } else {
                    (1, 0)
                };
                if (backlog.pending, backlog.claimed) != expected
                    || backlog.generation != activation.inbox_generation
                {
                    return Err(plain_actor_unsupported());
                }
                let claim = match activation.initial_input.clone() {
                    Some(claim) => claim,
                    None => activation
                        .claim_input(binding, session, run_id)
                        .await?
                        .ok_or_else(plain_actor_unsupported)?,
                };
                if claim.claim.generation != activation.inbox_generation {
                    return Err(plain_actor_unsupported());
                }
                let delivery = activation.checkpoint_input(session, run_id, &claim).await?;
                activation.initial_input = Some(claim);
                Ok(delivery)
            }
            .await;
            match prepared {
                Ok(delivery) => Ok((activation, Some(delivery))),
                Err(error) => {
                    activation.finish(ActorActivationFinish::Failed).await?;
                    Err(error)
                }
            }
        })
    }
    fn claim_input<'a>(
        &'a self,
        binding: &'a SessionInboxRuntimeBinding,
        session: &'a Session,
        run_id: &'a str,
    ) -> std::pin::Pin<
        Box<
            impl std::future::Future<Output = Result<Option<SessionInboxOwnedClaim>, AgentError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            if !self
                .input_inbox
                .inspect(&session.id)
                .await
                .map_err(|_| plain_actor_unsupported())?
                .activation_pending()
            {
                return Ok(None);
            }
            if run_id != self.fence.run_id || !binding.router.owns_run(&session.id, run_id).await {
                return Err(plain_actor_unsupported());
            }
            let now = chrono::Utc::now();
            if now >= self.input_deadline {
                return Err(plain_actor_unsupported());
            }
            let lease = SessionInboxLeaseRequest {
                consumer: self.input_consumer.clone(),
                now,
                duration: self.input_deadline - now,
            };
            let claim = self
                .input_inbox
                .claim_owned(&session.id, 1, Some(run_id), &lease)
                .await
                .map_err(|error| {
                    tracing::warn!(%error, "Actor correction claim unconfirmed");
                    AgentError::LLM(
                        "Actor correction claim unconfirmed; durable input is preserved".into(),
                    )
                })?
                .into_iter()
                .next();
            if let Some(claim) = &claim {
                if !matches!(&claim.claim.envelope.body, bamboo_domain::SessionMessageBody::Content(content)
                    if content.parts.is_empty() && !content.text.trim().is_empty() && content.text.len() <= 8192)
                {
                    self.input_inbox
                        .fail_owned(
                            &session.id,
                            claim,
                            &SessionInboxFailureReport {
                                now: chrono::Utc::now(),
                                error_code: "unsupported_actor_input".into(),
                            },
                        )
                        .await
                        .map_err(|error| {
                            tracing::warn!(%error, "Actor correction failure report unconfirmed");
                            AgentError::LLM(
                                "Actor correction failure report unconfirmed; durable input is preserved".into(),
                            )
                        })?;
                    return Err(plain_actor_unsupported());
                }
            }
            Ok(claim)
        })
    }
    fn checkpoint_input<'a>(
        &'a self,
        session: &'a mut Session,
        run_id: &'a str,
        claim: &'a SessionInboxOwnedClaim,
    ) -> std::pin::Pin<
        Box<
            impl std::future::Future<Output = Result<PlainInitialDelivery, AgentError>> + Send + 'a,
        >,
    > {
        Box::pin(async move {
            // The worker receives the committed pre-input prefix. Only typed
            // delivery adds the User; the Host-private bookkeeper is not a grant.
            let committed = self.input_inbox.checkpoint_actor_input(bamboo_storage::ActorInputCheckpoint {
                fence: self.fence.clone(), expected_created_at: self.created_at,
                claim: claim.clone(), expected_messages: session.messages.clone(),
                expected_provider_transcript: session.provider_transcript.clone(),
                expected_admission: session.session_inbox_admission().cloned(),
            }).await.map_err(|error| {
                tracing::warn!(%error, "Actor correction checkpoint rejected or unconfirmed; no worker dispatch or ACK");
                AgentError::LLM("Actor correction checkpoint unconfirmed; durable Inbox and history are preserved".into())
            })?;
            if self.recovering_pre_ack
                && committed.status
                    != bamboo_storage::ActorInputCheckpointStatus::AlreadyCheckpointed
            {
                return Err(plain_actor_unsupported());
            }
            let messages = session
                .messages
                .iter()
                .map(|message| {
                    let mut worker = message.clone();
                    if committed.status
                        == bamboo_storage::ActorInputCheckpointStatus::AlreadyCheckpointed
                        && worker.id == committed.envelope.id.as_str()
                    {
                        worker
                            .metadata
                            .as_mut()
                            .and_then(serde_json::Value::as_object_mut)
                            .ok_or_else(plain_actor_unsupported)?
                            .remove("_bamboo_owned_input_checkpoint");
                        if !bamboo_domain::is_matching_session_message(&worker, &committed.envelope)
                        {
                            return Err(plain_actor_unsupported());
                        }
                    }
                    serde_json::to_value(worker).map_err(|_| plain_actor_unsupported())
                })
                .collect::<Result<Vec<_>, _>>()?;
            *session = committed.session;
            Ok((
                messages,
                SessionMessageDelivery {
                    target_session_id: session.id.clone(),
                    envelope: committed.envelope,
                    canonical_claim_generation: committed.generation,
                    activation_run_id: run_id.into(),
                    activation_policy: claim.claim.activation_policy,
                },
            ))
        })
    }
    fn append_reply<'a>(
        &'a self,
        session: &'a mut Session,
        text: String,
        event_tx: &'a mpsc::Sender<AgentEvent>,
    ) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<(), AgentError>> + Send + 'a>>
    {
        Box::pin(async move {
            let message = bamboo_agent_core::Message::assistant(text, None);
            *session = self
                .store
                .append_actor_transcript(bamboo_storage::ActorTranscriptAppend {
                    fence: self.fence.clone(),
                    expected_created_at: self.created_at,
                    expected_messages: session.messages.clone(),
                    expected_provider_transcript: self.provider_transcript.clone(),
                    messages: vec![message.clone()],
                    native_groups: Vec::new(),
                })
                .await
                .map_err(|error| AgentError::LLM(format!("actor reply commit failed: {error}")))?;
            let _ = event_tx
                .send(AgentEvent::MessageAppended {
                    session_id: session.id.clone(),
                    message_id: message.id,
                    role: message.role,
                    content: message.content,
                    created_at: message.created_at,
                })
                .await;
            Ok(())
        })
    }
    // First Terminal is a real completed plain reply, not a cache event. Commit
    // it before appending the typed correction, then use the existing startup
    // admission barrier on a second correlated Run under the SAME Host fence.
    fn continue_input<'a>(
        &'a self,
        client: &'a mut dyn bamboo_subagent::ChildLink,
        binding: &'a SessionInboxRuntimeBinding,
        session: &'a mut Session,
        run_id: &'a str,
        run: Option<(&'a RunSpec, &'a AtomicU64)>,
        expected_posture: &'a mut Option<ExpectedPermissionPosture>,
        text: String,
        event_tx: &'a mpsc::Sender<AgentEvent>,
    ) -> std::pin::Pin<
        Box<
            impl std::future::Future<
                    Output = Result<Option<(SessionInboxOwnedClaim, u64)>, AgentError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let Some(claim) = self.claim_input(binding, session, run_id).await? else {
                return Ok(None);
            };
            let (run, epochs) = run.ok_or_else(plain_actor_unsupported)?;
            self.append_reply(session, text, event_tx).await?;
            let (messages, delivery) = self.checkpoint_input(session, run_id, &claim).await?;
            // Fresh actual Host audit, unchanged permission/ceiling checks.
            self.refresh_continuation_posture(session, run, expected_posture)?;
            let epoch = epochs
                .fetch_add(1, Ordering::Relaxed)
                .checked_add(1)
                .ok_or_else(plain_actor_unsupported)?;
            let mut next = run.clone();
            next.messages = messages;
            next.initial_session_messages = vec![delivery];
            next.execution_epoch = epoch;
            client.send(ParentFrame::Run(next)).await.map_err(|_| {
                AgentError::LLM(
                    "Actor correction dispatch unconfirmed; durable input is preserved".into(),
                )
            })?;
            Ok(Some((claim, epoch)))
        })
    }
    fn refresh_continuation_posture(
        &self,
        session: &Session,
        run: &RunSpec,
        expected: &mut Option<ExpectedPermissionPosture>,
    ) -> Result<(), AgentError> {
        let Some(expected) = expected.as_mut() else {
            return Err(plain_actor_unsupported());
        };
        let audit = bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata)
            .ok_or_else(plain_actor_unsupported)?;
        if audit.policy_revision != expected.policy_revision
            || audit.resolution != expected.resolution
            || audit.executor_mapping != expected.executor_mapping
        {
            return Err(plain_actor_unsupported());
        }
        match (&self.permission_config, &run.permission_policy) {
            (Some(config), Some(policy)) => {
                ensure_no_active_scoped_session_denies(config, &session.id)?;
                if config.policy_revision() != policy.revision
                    || policy.revision != audit.policy_revision
                    || policy.session_id != session.id
                    || policy.workspace_path != session.workspace
                    || policy.inherit_session_grants
                    || serde_json::to_value(config.to_serializable())
                        .map_err(|_| plain_actor_unsupported())?
                        != policy.policy
                {
                    return Err(plain_actor_unsupported());
                }
            }
            (None, None) => {}
            _ => return Err(plain_actor_unsupported()),
        }
        expected.expected_audit_revision = Some(audit.audit_revision);
        Ok(())
    }
    fn release_initial_input<'a>(
        &'a self,
        binding: &'a SessionInboxRuntimeBinding,
        session: &'a Session,
        run: &'a RunSpec,
        epoch: u64,
        claim: &'a SessionInboxOwnedClaim,
        request: &'a bamboo_subagent::proto::InitialInputReleaseRequest,
        expected: &'a mut Option<ExpectedPermissionPosture>,
        previous: Option<&'a bamboo_subagent::proto::InitialInputRelease>,
    ) -> std::pin::Pin<
        Box<
            impl std::future::Future<
                    Output = Result<bamboo_subagent::proto::InitialInputRelease, AgentError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            if !self.initial_release_required {
                return Err(plain_actor_unsupported());
            }
            let delivery = SessionMessageDelivery {
                target_session_id: session.id.clone(),
                envelope: claim.claim.envelope.clone(),
                canonical_claim_generation: claim.claim.generation,
                activation_run_id: self.fence.run_id.clone(),
                activation_policy: claim.claim.activation_policy,
            };
            let mut current_run = run.clone();
            current_run.execution_epoch = epoch;
            let exact = bamboo_subagent::proto::InitialInputReleaseRequest::from_run(
                &current_run,
                &delivery,
                request.nonce.clone(),
            )
            .map_err(|_| plain_actor_unsupported())?;
            if &exact != request
                || request.child_id != session.id
                || request.created_at != session.created_at
                || request.spawn_depth != session.spawn_depth
                || request.parent_id != session.parent_session_id.as_deref().unwrap_or_default()
                || request.root_id != session.root_session_id
                || request.project_id != project_id_for_actor_run(session)?
                || !binding
                    .router
                    .owns_run(&session.id, &self.fence.run_id)
                    .await
            {
                return Err(plain_actor_unsupported());
            }
            let deadline = self
                .actor_deadline
                .min(self.input_deadline)
                .min(claim.lease.expires_at);
            if chrono::Utc::now() >= deadline {
                return Err(plain_actor_unsupported());
            }
            self.store
                .validate_fence(&self.fence, chrono::Utc::now())
                .await
                .map_err(|_| plain_actor_unsupported())?;
            let actual =
                bamboo_agent_core::storage::Storage::load_session(self.store.as_ref(), &session.id)
                    .await
                    .map_err(|_| plain_actor_unsupported())?
                    .ok_or_else(plain_actor_unsupported)?;
            if actual.created_at != session.created_at
                || actual.root_session_id != session.root_session_id
                || actual.parent_session_id != session.parent_session_id
                || actual.spawn_depth != session.spawn_depth
                || project_id_for_actor_run(&actual)? != request.project_id
                || serde_json::to_value(&actual.messages).map_err(|_| plain_actor_unsupported())?
                    != serde_json::to_value(&session.messages)
                        .map_err(|_| plain_actor_unsupported())?
                || serde_json::to_value(&actual.provider_transcript)
                    .map_err(|_| plain_actor_unsupported())?
                    != serde_json::to_value(&session.provider_transcript)
                        .map_err(|_| plain_actor_unsupported())?
                || serde_json::to_value(actual.session_inbox_admission())
                    .map_err(|_| plain_actor_unsupported())?
                    != serde_json::to_value(session.session_inbox_admission())
                        .map_err(|_| plain_actor_unsupported())?
            {
                return Err(plain_actor_unsupported());
            }
            self.refresh_continuation_posture(&actual, &current_run, expected)?;
            if let Some(previous) = previous {
                if !previous.permits(request, chrono::Utc::now())
                    || previous.expires_at != deadline
                    || !self
                        .input_inbox
                        .was_admitted(&session.id, &claim.claim.envelope.id)
                        .await
                        .map_err(|_| plain_actor_unsupported())?
                {
                    return Err(plain_actor_unsupported());
                }
            } else {
                // Full current-prefix AlreadyCheckpointed readback still checks
                // the actual physical owner. A local worker receipt is insufficient.
                self.input_inbox
                    .checkpoint_actor_input(bamboo_storage::ActorInputCheckpoint {
                        fence: self.fence.clone(),
                        expected_created_at: self.created_at,
                        claim: claim.clone(),
                        expected_messages: actual.messages.clone(),
                        expected_provider_transcript: actual.provider_transcript.clone(),
                        expected_admission: actual.session_inbox_admission().cloned(),
                    })
                    .await
                    .map_err(|_| plain_actor_unsupported())?;
                self.confirm_input(
                    binding,
                    &actual,
                    &self.fence.run_id,
                    claim,
                    &bamboo_subagent::proto::SessionMessageAdmissionConfirmation {
                        target_session_id: request.child_id.clone(),
                        envelope_id: request.envelope_id.clone(),
                        canonical_claim_generation: request.generation,
                        activation_run_id: request.activation_run_id.clone(),
                    },
                )
                .await?;
            }
            // ACK's terminal receipt is idempotent even after expiry. It cannot
            // replace these final current-owner/deadline/policy checks.
            if chrono::Utc::now() >= deadline
                || !binding
                    .router
                    .owns_run(&session.id, &self.fence.run_id)
                    .await
            {
                return Err(plain_actor_unsupported());
            }
            self.store
                .validate_fence(&self.fence, chrono::Utc::now())
                .await
                .map_err(|_| plain_actor_unsupported())?;
            let final_session =
                bamboo_agent_core::storage::Storage::load_session(self.store.as_ref(), &session.id)
                    .await
                    .map_err(|_| plain_actor_unsupported())?
                    .ok_or_else(plain_actor_unsupported)?;
            self.store
                .validate_fence(&self.fence, chrono::Utc::now())
                .await
                .map_err(|_| plain_actor_unsupported())?;
            self.refresh_continuation_posture(&final_session, &current_run, expected)?;
            if chrono::Utc::now() >= deadline {
                return Err(plain_actor_unsupported());
            }
            Ok(bamboo_subagent::proto::InitialInputRelease {
                request: request.clone(),
                expires_at: deadline,
            })
        })
    }
    async fn confirm_input(
        &self,
        binding: &SessionInboxRuntimeBinding,
        session: &Session,
        run_id: &str,
        claim: &SessionInboxOwnedClaim,
        confirmation: &bamboo_subagent::proto::SessionMessageAdmissionConfirmation,
    ) -> Result<(), AgentError> {
        if confirmation.target_session_id != session.id
            || confirmation.envelope_id != claim.claim.envelope.id.as_str()
            || confirmation.canonical_claim_generation != claim.claim.generation
            || confirmation.activation_run_id != run_id
            || run_id != self.fence.run_id
            || !binding.router.owns_run(&session.id, run_id).await
        {
            return Err(plain_actor_unsupported());
        }
        self.input_inbox
            .ack_owned(&session.id, claim, chrono::Utc::now())
            .await
            .map_err(|error| {
                tracing::warn!(%error, "Actor correction ACK unresolved");
                AgentError::LLM(
                    "Actor correction ACK unresolved; durable input is preserved".into(),
                )
            })
    }
    async fn finish(&self, outcome: ActorActivationFinish) -> Result<(), AgentError> {
        self.store
            .finish_activation(&self.fence, chrono::Utc::now(), outcome)
            .await
            .map(|_| ())
            .map_err(|error| AgentError::LLM(format!("Actor completion unconfirmed: {error}")))
    }
}
fn local_tool_history_unsupported() -> AgentError {
    AgentError::LLM("local_tool_history_unsupported".into())
}

#[derive(Default)]
struct LocalToolCollector {
    messages: Option<Vec<bamboo_agent_core::Message>>,
    starts: HashMap<String, (String, serde_json::Value)>,
    outcomes: HashMap<String, (bool, String)>,
}

impl LocalToolCollector {
    fn event(&mut self, value: &serde_json::Value) -> Result<bool, AgentError> {
        use bamboo_subagent::proto::LocalToolMessages;
        if self.messages.is_some() {
            return Err(local_tool_history_unsupported());
        }
        if value["type"] == LocalToolMessages::TYPE {
            let observation: LocalToolMessages = serde_json::from_value(value.clone())
                .map_err(|_| local_tool_history_unsupported())?;
            self.messages = Some(
                observation
                    .validate()
                    .map_err(|_| local_tool_history_unsupported())?,
            );
            return Ok(false);
        }
        let event: AgentEvent =
            serde_json::from_value(value.clone()).map_err(|_| local_tool_history_unsupported())?;
        match event {
            AgentEvent::ToolStart {
                tool_call_id,
                tool_name,
                arguments,
            } => {
                if !arguments.is_object()
                    || !matches!(tool_name.as_str(), "Read" | "Glob" | "Write")
                    || tool_call_id.is_empty()
                    || tool_call_id.len() > 128
                    || self.starts.len() >= LocalToolMessages::MAX_PAIRS
                    || self.outcomes.contains_key(&tool_call_id)
                    || self
                        .starts
                        .insert(tool_call_id, (tool_name, arguments))
                        .is_some()
                {
                    return Err(local_tool_history_unsupported());
                }
            }
            AgentEvent::ToolComplete {
                tool_call_id,
                result,
            } => {
                if !result.images.is_empty() {
                    return Err(local_tool_history_unsupported());
                }
                self.outcome(tool_call_id, result.success, result.result)?;
            }
            AgentEvent::ToolError {
                tool_call_id,
                error,
            } => {
                self.outcome(tool_call_id, false, format!("Error: {error}"))?;
            }
            AgentEvent::ToolToken { tool_call_id, .. } => {
                if !self.starts.contains_key(&tool_call_id)
                    || self.outcomes.contains_key(&tool_call_id)
                {
                    return Err(local_tool_history_unsupported());
                }
            }
            AgentEvent::ToolLifecycle {
                tool_call_id,
                tool_name,
                is_mutating,
                auto_approved,
                phase,
                ..
            } => {
                if self.starts.get(&tool_call_id).map(|(name, _)| name) != Some(&tool_name)
                    || is_mutating != (tool_name == "Write")
                    || auto_approved != (tool_name != "Write")
                    || !matches!(phase.as_str(), "begin" | "finished" | "error" | "cancelled")
                {
                    return Err(local_tool_history_unsupported());
                }
            }
            AgentEvent::ReasoningToken { .. }
            | AgentEvent::ContextSummarized { .. }
            | AgentEvent::ContextArchived { .. }
            | AgentEvent::ContextCompressionStatus { .. }
            | AgentEvent::NeedClarification { .. }
            | AgentEvent::SubAgentStarted { .. }
            | AgentEvent::SubAgentEvent { .. } => return Err(local_tool_history_unsupported()),
            // Worker commit events are observations, not canonical Host commits.
            AgentEvent::SessionHistoryCommitted { .. } | AgentEvent::MessageAppended { .. } => {
                return Ok(false)
            }
            _ => {}
        }
        Ok(true)
    }

    fn outcome(&mut self, id: String, success: bool, text: String) -> Result<(), AgentError> {
        if id.is_empty()
            || id.len() > 128
            || text.len() > bamboo_subagent::proto::LocalToolMessages::MAX_BYTES
            || self.outcomes.len() >= bamboo_subagent::proto::LocalToolMessages::MAX_PAIRS
            || self.outcomes.insert(id, (success, text)).is_some()
        {
            return Err(local_tool_history_unsupported());
        }
        Ok(())
    }

    fn suffix(
        self,
        host: &Session,
        tools: &[String],
        read_only: bool,
        terminal: Option<&str>,
    ) -> Result<Vec<bamboo_agent_core::Message>, AgentError> {
        use bamboo_subagent::proto::LocalToolMessages;
        let data = self.messages.ok_or_else(local_tool_history_unsupported)?;
        let candidate: Vec<_> = data.iter().skip_while(|m| m.role == Role::System).collect();
        let prefix: Vec<_> = host
            .messages
            .iter()
            .filter(|m| m.role != Role::System)
            .collect();
        if !host.provider_transcript.groups().is_empty()
            || candidate.len() <= prefix.len()
            || serde_json::to_value(&candidate[..prefix.len()])
                .map_err(|_| local_tool_history_unsupported())?
                != serde_json::to_value(&prefix).map_err(|_| local_tool_history_unsupported())?
        {
            return Err(local_tool_history_unsupported());
        }
        let mut ids = std::collections::HashSet::new();
        let mut calls = std::collections::HashSet::new();
        for message in &host.messages {
            if !ids.insert(message.id.clone()) {
                return Err(local_tool_history_unsupported());
            }
            for call in message.tool_calls.iter().flatten() {
                calls.insert(call.id.clone());
            }
            if let Some(id) = &message.tool_call_id {
                calls.insert(id.clone());
            }
        }
        let suffix: Vec<_> = candidate[prefix.len()..]
            .iter()
            .map(|message| (**message).clone())
            .collect();
        let mut pending = HashMap::new();
        let mut used = std::collections::HashSet::new();
        for message in &suffix {
            if !ids.insert(message.id.clone()) || message.never_compress {
                return Err(local_tool_history_unsupported());
            }
            match message.role {
                Role::Assistant => {
                    if !pending.is_empty()
                        || message.metadata.is_some()
                        || message.tool_call_id.is_some()
                        || message.tool_success.is_some()
                    {
                        return Err(local_tool_history_unsupported());
                    }
                    if let Some(batch) = &message.tool_calls {
                        if batch.is_empty() {
                            return Err(local_tool_history_unsupported());
                        }
                        for call in batch {
                            let arguments =
                                serde_json::from_str::<serde_json::Value>(&call.function.arguments)
                                    .ok()
                                    .filter(|arguments| arguments.is_object())
                                    .ok_or_else(local_tool_history_unsupported)?;
                            if call.id.is_empty()
                                || call.id.len() > 128
                                || call.tool_type != "function"
                                || !calls.insert(call.id.clone())
                                || calls.len() > LocalToolMessages::MAX_MESSAGES
                                || !matches!(call.function.name.as_str(), "Read" | "Glob" | "Write")
                                || self.starts.get(&call.id).is_some_and(|(name, actual)| {
                                    name != &call.function.name || actual != &arguments
                                })
                            {
                                return Err(local_tool_history_unsupported());
                            }
                            pending.insert(call.id.clone(), call.function.name.clone());
                        }
                    } else if message.content.trim().is_empty() {
                        return Err(local_tool_history_unsupported());
                    }
                }
                Role::Tool => {
                    let id = message
                        .tool_call_id
                        .as_ref()
                        .ok_or_else(local_tool_history_unsupported)?;
                    let name = pending
                        .remove(id)
                        .ok_or_else(local_tool_history_unsupported)?;
                    let success = message
                        .tool_success
                        .ok_or_else(local_tool_history_unsupported)?;
                    if message.tool_calls.is_some()
                        || message.phase.is_some()
                        || self.outcomes.get(id) != Some(&(success, message.content.clone()))
                        || !used.insert(id.clone())
                        || used.len() > LocalToolMessages::MAX_PAIRS
                    {
                        return Err(local_tool_history_unsupported());
                    }
                    let denied_write = name == "Write" && (read_only || !tools.contains(&name));
                    if denied_write {
                        if success
                            || !(message.content.contains("native_tool_ceiling_denied")
                                || message
                                    .content
                                    .contains("not callable at the current conversation position"))
                        {
                            return Err(local_tool_history_unsupported());
                        }
                    } else if !tools.contains(&name)
                        || self.starts.get(id).map(|(actual, _)| actual) != Some(&name)
                    {
                        return Err(local_tool_history_unsupported());
                    }
                    if let Some(metadata) = &message.metadata {
                        let object = metadata
                            .as_object()
                            .ok_or_else(local_tool_history_unsupported)?;
                        if object.len() != 5
                            || metadata["elapsed_ms"].as_u64().is_none()
                            || metadata["tool_name"].as_str() != Some(name.as_str())
                            || metadata["is_mutating"].as_bool() != Some(name == "Write")
                            || metadata["auto_approved"].as_bool() != Some(name != "Write")
                            || metadata["success"].as_bool() != Some(success)
                        {
                            return Err(local_tool_history_unsupported());
                        }
                    }
                }
                _ => return Err(local_tool_history_unsupported()),
            }
        }
        let last = suffix.last().ok_or_else(local_tool_history_unsupported)?;
        if !pending.is_empty()
            || used.len() != self.outcomes.len()
            || self.starts.keys().any(|id| !used.contains(id))
            || last.role != Role::Assistant
            || last.tool_calls.is_some()
            || last.phase == Some(bamboo_domain::MessagePhase::Commentary)
            || terminal != Some(last.content.as_str())
        {
            return Err(local_tool_history_unsupported());
        }
        Ok(suffix)
    }
}

async fn commit_local_tool_history(
    collector: LocalToolCollector,
    session: &mut Session,
    binding: &SessionInboxRuntimeBinding,
    run_id: &str,
    selection: (&[String], bool),
    terminal: Option<&str>,
    cancel: &CancellationToken,
) -> Result<(), AgentError> {
    if cancel.is_cancelled() || !binding.router.owns_run(&session.id, run_id).await {
        return Err(local_tool_history_unsupported());
    }
    let latest = binding
        .storage
        .load_session(&session.id)
        .await
        .map_err(|_| local_tool_history_unsupported())?
        .ok_or_else(local_tool_history_unsupported)?;
    if latest.id != session.id
        || latest.kind != session.kind
        || latest.created_at != session.created_at
        || latest.root_session_id != session.root_session_id
        || latest.parent_session_id != session.parent_session_id
        || latest.spawn_depth != session.spawn_depth
        || latest.project_id_meta() != session.project_id_meta()
        || serde_json::to_value(&latest.provider_transcript)
            .map_err(|_| local_tool_history_unsupported())?
            != serde_json::to_value(&session.provider_transcript)
                .map_err(|_| local_tool_history_unsupported())?
    {
        return Err(local_tool_history_unsupported());
    }
    let suffix = collector.suffix(&latest, selection.0, selection.1, terminal)?;
    let before = session.clone();
    let mut staged = session.clone();
    staged.messages = latest.messages.clone();
    staged.messages.extend(suffix.iter().cloned());
    staged.updated_at = chrono::Utc::now();
    if cancel.is_cancelled() || !binding.router.owns_run(&session.id, run_id).await {
        return Err(local_tool_history_unsupported());
    }
    binding
        .persistence
        .checkpoint_runtime_session(&mut staged)
        .await
        .map_err(|_| local_tool_history_unsupported())?;
    let saved = binding
        .storage
        .load_session(&session.id)
        .await
        .map_err(|_| local_tool_history_unsupported())?
        .ok_or_else(local_tool_history_unsupported)?;
    if serde_json::to_value(&saved.provider_transcript)
        .map_err(|_| local_tool_history_unsupported())?
        != serde_json::to_value(&latest.provider_transcript)
            .map_err(|_| local_tool_history_unsupported())?
    {
        return Err(local_tool_history_unsupported());
    }
    let start = saved
        .messages
        .iter()
        .position(|message| message.id == suffix[0].id)
        .ok_or_else(local_tool_history_unsupported)?;
    if saved.messages.len() < latest.messages.len()
        || serde_json::to_value(&saved.messages[..latest.messages.len()])
            .map_err(|_| local_tool_history_unsupported())?
            != serde_json::to_value(&latest.messages)
                .map_err(|_| local_tool_history_unsupported())?
        || serde_json::to_value(saved.messages.get(start..start + suffix.len()))
            .map_err(|_| local_tool_history_unsupported())?
            != serde_json::to_value(Some(&suffix[..]))
                .map_err(|_| local_tool_history_unsupported())?
    {
        return Err(local_tool_history_unsupported());
    }
    // Append-safe reconciliation may add a concurrent Host User; it may never
    // rewrite the validated Host prefix or any worker row being committed.
    let expected = latest.messages.iter().chain(&suffix);
    for message in expected {
        let matches: Vec<_> = saved
            .messages
            .iter()
            .filter(|current| current.id == message.id)
            .collect();
        if matches.len() != 1
            || serde_json::to_value(matches[0]).map_err(|_| local_tool_history_unsupported())?
                != serde_json::to_value(message).map_err(|_| local_tool_history_unsupported())?
        {
            *session = before;
            return Err(local_tool_history_unsupported());
        }
    }
    if cancel.is_cancelled() || !binding.router.owns_run(&session.id, run_id).await {
        return Err(local_tool_history_unsupported());
    }
    staged.messages = saved.messages;
    *session = staged;
    Ok(())
}

#[derive(Default)]
struct ReadOnlyActorCollector {
    start: Option<(String, serde_json::Value)>,
    result: Option<String>,
    messages: Option<Vec<bamboo_agent_core::Message>>,
}
impl ReadOnlyActorCollector {
    fn event(&mut self, value: &serde_json::Value) -> Result<bool, AgentError> {
        if self.messages.is_some() {
            return Err(plain_actor_unsupported());
        }
        if value["type"] == "owned_readonly_transcript" {
            let tail: bamboo_subagent::proto::ReadOnlyActorTranscript =
                serde_json::from_value(value.clone()).map_err(|_| plain_actor_unsupported())?;
            tail.validate().map_err(|_| plain_actor_unsupported())?;
            let bamboo_subagent::proto::ReadOnlyActorTranscript::Complete { messages } = tail;
            let typed: Vec<bamboo_agent_core::Message> = messages
                .iter()
                .map(|raw| {
                    let message: bamboo_agent_core::Message =
                        serde_json::from_value(raw.clone())
                            .map_err(|_| plain_actor_unsupported())?;
                    if serde_json::to_value(&message).map_err(|_| plain_actor_unsupported())?
                        != *raw
                    {
                        return Err(plain_actor_unsupported());
                    }
                    Ok(message)
                })
                .collect::<Result<_, _>>()?;
            let (id, args) = self.start.as_ref().ok_or_else(plain_actor_unsupported)?;
            let call = typed[0]
                .tool_calls
                .as_ref()
                .and_then(|calls| {
                    if calls.len() == 1 {
                        calls.first()
                    } else {
                        None
                    }
                })
                .ok_or_else(plain_actor_unsupported)?;
            if call.id != *id
                || call.function.name != "Glob"
                || serde_json::from_str::<serde_json::Value>(&call.function.arguments)
                    .map_err(|_| plain_actor_unsupported())?
                    != *args
                || typed[1].tool_call_id.as_ref() != Some(id)
                || typed[1].tool_success != Some(true)
                || self.result.as_ref() != Some(&typed[1].content)
            {
                return Err(plain_actor_unsupported());
            }
            self.messages = Some(typed);
            return Ok(false);
        }
        let event: AgentEvent =
            serde_json::from_value(value.clone()).map_err(|_| plain_actor_unsupported())?;
        match event {
            AgentEvent::ToolStart {
                tool_call_id,
                tool_name,
                arguments,
            } => {
                if tool_name != "Glob"
                    || tool_call_id.trim().is_empty()
                    || self.start.is_some()
                    || !arguments.is_object()
                {
                    return Err(plain_actor_unsupported());
                }
                self.start = Some((tool_call_id, arguments));
                Ok(true)
            }
            AgentEvent::ToolComplete {
                tool_call_id,
                result,
            } => {
                if self.start.as_ref().map(|s| &s.0) != Some(&tool_call_id)
                    || self.result.is_some()
                    || !result.success
                    || !result.images.is_empty()
                {
                    return Err(plain_actor_unsupported());
                }
                self.result = Some(result.result);
                Ok(true)
            }
            AgentEvent::ToolToken { tool_call_id, .. } => {
                if self.start.as_ref().map(|s| &s.0) != Some(&tool_call_id) || self.result.is_some()
                {
                    return Err(plain_actor_unsupported());
                }
                Ok(true)
            }
            AgentEvent::ToolLifecycle {
                tool_call_id,
                tool_name,
                is_mutating,
                phase,
                error,
                ..
            } => {
                if tool_name != "Glob"
                    || is_mutating
                    || !matches!(phase.as_str(), "begin" | "finished")
                    || error.is_some()
                    || self.start.as_ref().map(|s| &s.0) != Some(&tool_call_id)
                {
                    return Err(plain_actor_unsupported());
                }
                Ok(true)
            }
            _ => plain_actor_event(value),
        }
    }
    fn finish(self, terminal: Option<&str>) -> Result<Vec<bamboo_agent_core::Message>, AgentError> {
        let messages = self.messages.ok_or_else(plain_actor_unsupported)?;
        if terminal != Some(messages[2].content.as_str()) || messages[2].content.trim().is_empty() {
            return Err(plain_actor_unsupported());
        }
        Ok(messages)
    }
}

fn plain_actor_event(value: &serde_json::Value) -> Result<bool, AgentError> {
    let event: AgentEvent =
        serde_json::from_value(value.clone()).map_err(|_| plain_actor_unsupported())?;
    match event {
        AgentEvent::ReasoningToken { .. }
        | AgentEvent::ToolToken { .. }
        | AgentEvent::ToolStart { .. }
        | AgentEvent::ToolComplete { .. }
        | AgentEvent::ToolError { .. }
        | AgentEvent::ToolLifecycle { .. }
        | AgentEvent::ContextSummarized { .. }
        | AgentEvent::ContextArchived { .. }
        | AgentEvent::ContextCompressionStatus { .. }
        | AgentEvent::NeedClarification { .. }
        | AgentEvent::SubAgentStarted { .. }
        | AgentEvent::SubAgentEvent { .. }
        | AgentEvent::ChildApprovalRequested { .. } => Err(plain_actor_unsupported()),
        // Worker cache commits are not host Store commits. Publish only the
        // actual guarded host append after it returns a committed snapshot.
        AgentEvent::MessageAppended { .. }
        | AgentEvent::SessionHistoryCommitted { .. }
        | AgentEvent::VisibleMessageStart { .. }
        | AgentEvent::VisibleMessageDiscard { .. } => Ok(false),
        event if event.is_durable_change() => Ok(false),
        _ => Ok(true),
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
    fenced: Option<(&bamboo_storage::SessionStoreV2, &ActorActivationFence)>,
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

    if let Some((store, fence)) = fenced {
        let committed = store
            .confirm_actor_claim_context(
                concrete_file_inbox(binding)?,
                bamboo_storage::ActorClaimContextSeed {
                    fence: fence.clone(),
                    expected_created_at: session.created_at,
                    expected_messages: session.messages.clone(),
                    expected_provider_transcript: session.provider_transcript.clone(),
                    expected_admission: session.session_inbox_admission().cloned(),
                    claims: vec![claim.clone()],
                },
            )
            .await;
        let committed = match committed {
            Ok(committed) => committed,
            Err(error) => {
                // An error after atomic replacement may still have advanced
                // Main. Keep the claim unacked and use that canonical context
                // for the SDK's terminal status save.
                adopt_latest_actor_context(store, session).await;
                return Err(AgentError::LLM(format!(
                    "confirm fenced Actor claim context unconfirmed: {error}"
                )));
            }
        };
        bamboo_domain::merge_session_inbox_admission(session, &committed);
        session.messages = committed.messages;
        session.provider_transcript = committed.provider_transcript;
        session.updated_at = committed.updated_at;
        if !session
            .messages
            .iter()
            .any(|message| bamboo_domain::is_matching_session_message(message, &claim.envelope))
            || !session
                .session_inbox_admission()
                .is_some_and(|cursor| cursor.contains(&claim.envelope.id))
        {
            return Err(AgentError::LLM(
                "fenced Actor claim confirmation lost durable proof".into(),
            ));
        }
        return binding
            .inbox
            .ack(&session.id, claim)
            .await
            .map_err(|error| {
                AgentError::LLM(format!(
                    "ack fenced Actor claim {} after confirmation: {error}",
                    claim.envelope.id
                ))
            });
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

fn concrete_file_inbox(
    binding: &SessionInboxRuntimeBinding,
) -> Result<bamboo_storage::FileSessionInbox, AgentError> {
    binding
        .inbox
        .as_any()
        .and_then(|inbox| inbox.downcast_ref::<bamboo_storage::FileSessionInbox>())
        .cloned()
        .ok_or_else(|| AgentError::LLM("remote Actor requires canonical FileSessionInbox".into()))
}

async fn adopt_latest_actor_context(store: &bamboo_storage::SessionStoreV2, session: &mut Session) {
    if let Ok(Some(latest)) = store.load_session(&session.id).await {
        if latest.created_at == session.created_at {
            bamboo_domain::merge_session_inbox_admission(session, &latest);
            session.messages = latest.messages;
            session.provider_transcript = latest.provider_transcript;
            session.updated_at = latest.updated_at;
        }
    }
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
    fenced: Option<(&bamboo_storage::SessionStoreV2, &ActorActivationFence)>,
) -> crate::runtime::runner::Result<()> {
    if claims.is_empty() {
        return Ok(());
    }
    let before = session.clone();
    let mut additions = Vec::new();
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
            additions.push(message);
        }
    }
    let fenced_seed = fenced.is_some();
    if let Some((store, fence)) = fenced {
        let committed = store
            .seed_actor_claim_context(
                concrete_file_inbox(binding)?,
                bamboo_storage::ActorClaimContextSeed {
                    fence: fence.clone(),
                    expected_created_at: session.created_at,
                    expected_messages: session.messages.clone(),
                    expected_provider_transcript: session.provider_transcript.clone(),
                    expected_admission: session.session_inbox_admission().cloned(),
                    claims: claims.to_vec(),
                },
            )
            .await;
        let committed = match committed {
            Ok(committed) => committed,
            Err(error) => {
                // Publication may have occurred before an unconfirmed
                // readback. Do not dispatch or ACK; adopt only the current
                // canonical context so SDK can still save an error status.
                adopt_latest_actor_context(store, session).await;
                return Err(AgentError::LLM(format!(
                    "seed fenced Actor claim context unconfirmed: {error}"
                )));
            }
        };
        bamboo_domain::merge_session_inbox_admission(session, &committed);
        session.messages = committed.messages;
        session.provider_transcript = committed.provider_transcript;
        session.updated_at = committed.updated_at;
    } else {
        for message in additions {
            session.add_message(message);
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
    }
    for claim in claims {
        if !session
            .messages
            .iter()
            .any(|message| bamboo_domain::is_matching_session_message(message, &claim.envelope))
        {
            if !fenced_seed {
                *session = before;
            }
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
    fenced: Option<(&bamboo_storage::SessionStoreV2, &ActorActivationFence)>,
) -> crate::runtime::runner::Result<Vec<(SessionInboxClaim, SessionMessageDelivery)>> {
    let claims = binding
        .inbox
        .claim_for_turn(&session.id, limit, Some(activation_run_id))
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
    checkpoint_claim_context_before_dispatch(binding, session, &unconfirmed, fenced).await?;

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
        let delivery = SessionMessageDelivery {
            target_session_id: session.id.clone(),
            envelope: claim.envelope.clone(),
            canonical_claim_generation: claim.generation,
            activation_run_id: activation_run_id.to_string(),
            activation_policy: claim.activation_policy,
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
    fenced: Option<(&bamboo_storage::SessionStoreV2, &ActorActivationFence)>,
) -> crate::runtime::runner::Result<()> {
    if !inflight.is_empty() {
        return Ok(());
    }
    let Some((claim, delivery)) =
        claim_canonical_deliveries(binding, session, activation_run_id, 1, fenced)
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
    expected_permission_posture: Option<ExpectedPermissionPosture>,
    expected_creation: Option<&'a bamboo_subagent::proto::ChildCreationIdentity>,
    session_inbox_runtime: Option<&'a SessionInboxRuntimeBinding>,
    actor_directory_store: Option<&'a bamboo_storage::SessionStoreV2>,
    canonical_subagent_tool: Option<Arc<dyn bamboo_agent_core::tools::Tool>>,
    activation_run_id: Option<&'a str>,
    execution_epoch: u64,
    expected_source_actor_id: &'a str,
    initial_inflight_claims: VecDeque<SessionInboxClaim>,
    plain_actor: bool,
    remote_environment_lease: bool,
    readonly_output: Option<&'a mut Vec<bamboo_agent_core::Message>>,
    local_history_tools: Option<&'a [String]>,
    local_history_read_only: bool,
    plain_input: Option<&'a PlainActorActivation>,
    canonical_activation: Option<&'a ActorActivationFence>,
    canonical_placement_ref: Option<&'a bamboo_domain::ActorPlacementRef>,
    actor_event_observer: Option<&'a dyn ActorEventObserver>,
    plain_run: Option<(&'a RunSpec, &'a AtomicU64)>,
    first_frame_timeout: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedPermissionPosture {
    policy_revision: u64,
    resolution: bamboo_domain::PermissionModeResolution,
    expected_audit_revision: Option<u64>,
    executor_mapping: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PermissionPostureHandshake {
    /// Legacy/custom actors were dispatched without a typed posture contract.
    NotRequired,
    /// The host dispatched an exact posture and no matching, durably recorded
    /// worker activation has arrived yet.
    Awaiting,
    /// One worker posture matched and its host-owned audit write succeeded.
    Confirmed,
}

impl PermissionPostureHandshake {
    fn new(expected: Option<&ExpectedPermissionPosture>) -> Self {
        if expected.is_some() {
            Self::Awaiting
        } else {
            Self::NotRequired
        }
    }

    fn is_awaiting(self) -> bool {
        self == Self::Awaiting
    }

    fn posture_was_confirmed(self) -> bool {
        self == Self::Confirmed
    }
}

fn permission_posture_seed_from_event(
    session: &Session,
    event: &AgentEvent,
) -> Result<Option<bamboo_domain::PermissionAuditSeed>, String> {
    let AgentEvent::PermissionPostureActivated {
        session_id,
        policy_revision,
        requested_mode,
        effective_mode,
        executor_mapping,
    } = event
    else {
        return Ok(None);
    };
    if session_id != &session.id {
        return Err("permission posture event targets a different logical session".to_string());
    }
    let requested = bamboo_domain::SessionPermissionMode::from_audit_str(requested_mode)
        .ok_or_else(|| "permission posture event has an invalid requested mode".to_string())?;
    let effective = bamboo_domain::PermissionMode::from_audit_str(effective_mode)
        .ok_or_else(|| "permission posture event has an invalid effective mode".to_string())?;
    let resolution = bamboo_domain::PermissionModeResolution {
        requested,
        effective,
    };
    if !resolution.is_consistent() {
        return Err("permission posture event has an inconsistent mode pair".to_string());
    }
    let current_requested = session
        .agent_runtime_state
        .as_ref()
        .map(|state| state.effective_permission_mode())
        .unwrap_or_default();
    if current_requested != requested {
        return Err("permission posture event is stale for the host typed mode".to_string());
    }
    let mapping_chars = executor_mapping.chars().count();
    if mapping_chars == 0 || mapping_chars > bamboo_domain::MAX_PERMISSION_EXECUTOR_MAPPING_CHARS {
        return Err("permission posture event has an invalid executor mapping".to_string());
    }
    Ok(Some(bamboo_domain::PermissionAuditSeed::new(
        *policy_revision,
        resolution,
        executor_mapping,
    )))
}

fn validate_actor_event_batch(
    batch: &ActorEventBatch,
    logical_session: &Session,
    parent_session_id: &str,
    activation_run_id: Option<&str>,
    execution_epoch: u64,
    expected_source_actor_id: &str,
    expected_creation: Option<&bamboo_subagent::proto::ChildCreationIdentity>,
) -> Result<(), AgentError> {
    batch
        .validate()
        .map_err(|error| AgentError::LLM(format!("actor emitted invalid event batch: {error}")))?;
    let identity = batch.logical_session.as_ref().ok_or_else(|| {
        AgentError::LLM("actor event batch omitted its logical session identity".to_string())
    })?;
    // Fence against the exact canonical identity used to build RunSpec. Some
    // migrated/test sessions have not yet materialized parent ancestry on the
    // in-memory Session, so comparing the batch directly with those raw fields
    // can reject the identity the host itself just dispatched.
    let expected_identity = LogicalSessionIdentity {
        creation: expected_creation.cloned(),
        session_id: logical_session.id.clone(),
        parent_session_id: logical_session
            .parent_session_id
            .clone()
            .or_else(|| Some(parent_session_id.to_string())),
        root_session_id: if logical_session.root_session_id.trim().is_empty() {
            parent_session_id.to_string()
        } else {
            logical_session.root_session_id.clone()
        },
    };
    if identity != &expected_identity {
        return Err(AgentError::LLM(
            "actor event batch targets a different logical session".to_string(),
        ));
    }
    if batch.activation_id.as_deref() != activation_run_id {
        return Err(AgentError::LLM(
            "actor event batch belongs to a stale activation".to_string(),
        ));
    }
    if batch.execution_epoch != execution_epoch {
        return Err(AgentError::LLM(
            "actor event batch belongs to a stale execution epoch".to_string(),
        ));
    }
    if batch
        .source_actor_id
        .as_deref()
        .is_some_and(|source| source != expected_source_actor_id)
    {
        return Err(AgentError::LLM(
            "actor event batch declares a different physical actor".to_string(),
        ));
    }
    Ok(())
}

/// The plain Actor path already owns a durable Directory activation. Check that
/// exact owner before admitting either batched or rolling-upgrade live frames;
/// worker-provided batch identity alone is never an activation authority.
async fn validate_directory_actor_event(
    store: &bamboo_storage::SessionStoreV2,
    fence: &ActorActivationFence,
    session: &Session,
    run_id: Option<&str>,
) -> Result<bamboo_domain::ActorDirectoryEntry, AgentError> {
    store
        .validate_fence(fence, chrono::Utc::now())
        .await
        .map_err(|_| AgentError::LLM("actor event owner is stale".into()))?;
    let entry = store
        .inspect_actor(&fence.actor_id)
        .await
        .map_err(|_| AgentError::LLM("actor event authority is unavailable".into()))?;
    if !entry.actor.matches_session(session)
        || entry.actor.project_id != project_id_for_actor_run(session)?.map(|id| id.to_string())
        || !entry.activation.as_ref().is_some_and(|current| {
            current.matches_fence(fence)
                && current.status == bamboo_domain::ActorActivationStatus::Running
        })
        || run_id.is_some_and(|id| id != fence.run_id)
    {
        return Err(AgentError::LLM("actor event authority changed".into()));
    }
    Ok(entry)
}

/// The Worker supplies only a cursor. All identity and scope inputs come from
/// this Host's active drive and its canonical durable Session store.
async fn inspect_active_owned_tree(
    store: &bamboo_storage::SessionStoreV2,
    binding: &SessionInboxRuntimeBinding,
    run_id: &str,
    child_id: &str,
    parent_id: &str,
    logical_session: &Session,
    creation: &bamboo_subagent::proto::ChildCreationIdentity,
    plain_input: Option<&PlainActorActivation>,
    cursor: Option<&str>,
) -> Option<serde_json::Value> {
    use bamboo_agent_core::storage::Storage;
    let current = match store.load_session(child_id).await {
        Ok(Some(current)) => current,
        _ => {
            tracing::warn!("owned tree denied: canonical caller unavailable");
            return None;
        }
    };
    let current_project = match project_id_for_actor_run(&current) {
        Ok(project) => project,
        Err(_) => {
            tracing::warn!("owned tree denied: invalid canonical Project");
            return None;
        }
    };
    let logical_project = match project_id_for_actor_run(logical_session) {
        Ok(project) => project,
        Err(_) => {
            tracing::warn!("owned tree denied: invalid logical Project");
            return None;
        }
    };
    if current.id != child_id
        || current.kind != bamboo_domain::SessionKind::Child
        || current.parent_session_id.as_deref() != Some(parent_id)
        || current.parent_session_id != logical_session.parent_session_id
        || current.root_session_id != logical_session.root_session_id
        || current.created_at != creation.created_at
        || current.created_at != logical_session.created_at
        || current.spawn_depth != creation.spawn_depth
        || current.spawn_depth != logical_session.spawn_depth
        || current_project != logical_project
    {
        tracing::warn!("owned tree denied: caller lifetime or Project changed");
        return None;
    }
    if let Some(activation) = plain_input {
        validate_directory_actor_event(
            activation.store.as_ref(),
            &activation.fence,
            &current,
            Some(run_id),
        )
        .await
        .ok()?;
    } else if !binding.router.owns_run(child_id, run_id).await {
        tracing::warn!("owned tree denied: activation owner changed");
        return None;
    }
    let page = match crate::session_app::child_session::owned_tree::inspect_owned_tree(
        store, child_id, cursor,
    )
    .await
    {
        Ok(page) => page,
        Err(error) => {
            tracing::warn!(?error, "owned tree denied: canonical tree read failed");
            return None;
        }
    };
    let again = match store.load_session(child_id).await {
        Ok(Some(again)) => again,
        _ => {
            tracing::warn!("owned tree denied: canonical caller disappeared");
            return None;
        }
    };
    if again.created_at != current.created_at
        || again.parent_session_id != current.parent_session_id
        || again.root_session_id != current.root_session_id
        || again.spawn_depth != current.spawn_depth
        || project_id_for_actor_run(&again).ok()? != current_project
    {
        tracing::warn!("owned tree denied: caller changed during inspection");
        return None;
    }
    if let Some(activation) = plain_input {
        validate_directory_actor_event(
            activation.store.as_ref(),
            &activation.fence,
            &again,
            Some(run_id),
        )
        .await
        .ok()?;
    } else if !binding.router.owns_run(child_id, run_id).await {
        tracing::warn!("owned tree denied: activation owner changed after inspection");
        return None;
    }
    serde_json::to_vec(&page)
        .ok()
        .filter(|bytes| bytes.len() <= 8192)?;
    Some(page)
}

/// Resolve the caller of a mutating nested SubAgent RPC entirely from the
/// Host's active drive and canonical store. The frame never supplies identity,
/// Project, ancestry, or a run fence.
async fn load_active_subagent_caller(
    store: &bamboo_storage::SessionStoreV2,
    binding: &SessionInboxRuntimeBinding,
    run_id: &str,
    child_id: &str,
    parent_id: &str,
    logical_session: &Session,
    creation: &bamboo_subagent::proto::ChildCreationIdentity,
    plain_input: Option<&PlainActorActivation>,
) -> Option<Session> {
    use bamboo_agent_core::storage::Storage;
    let current = store.load_session(child_id).await.ok().flatten()?;
    let current_project = project_id_for_actor_run(&current).ok()?;
    let logical_project = project_id_for_actor_run(logical_session).ok()?;
    if current.id != child_id
        || current.kind != bamboo_domain::SessionKind::Child
        || current.parent_session_id.as_deref() != Some(parent_id)
        || current.parent_session_id != logical_session.parent_session_id
        || current.root_session_id != logical_session.root_session_id
        || current.created_at != creation.created_at
        || current.created_at != logical_session.created_at
        || current.spawn_depth != creation.spawn_depth
        || current.spawn_depth != logical_session.spawn_depth
        || current_project != logical_project
        || bamboo_domain::ActorSession::from_session(&current).is_err()
    {
        return None;
    }
    if let Some(activation) = plain_input {
        validate_directory_actor_event(
            activation.store.as_ref(),
            &activation.fence,
            &current,
            Some(run_id),
        )
        .await
        .ok()?;
    } else if !binding.router.owns_run(child_id, run_id).await {
        return None;
    }
    Some(current)
}

async fn invoke_active_subagent(
    tool: &dyn bamboo_agent_core::tools::Tool,
    caller: &Session,
    args: serde_json::Value,
    tool_call_id: &str,
) -> serde_json::Value {
    use bamboo_agent_core::tools::{ToolCtx, ToolOutcome};
    let mut ctx = ToolCtx::none(tool_call_id.to_owned());
    ctx.session_id = Some(Arc::from(caller.id.as_str()));
    match tool.invoke(args, ctx).await {
        Ok(ToolOutcome::Completed(result))
            if result.images.is_empty()
                && serde_json::to_vec(&result).is_ok_and(|bytes| bytes.len() <= 16 * 1024) =>
        {
            serde_json::json!({"result":result})
        }
        _ => serde_json::json!({
            "error":"SubAgent operation failed or its result exceeded the limit; inspect logical child state before retrying"
        }),
    }
}

/// Commit one already-persisted Worker clarification under the Host's current
/// Child birth and Run fence. Only the Host supplies caller/parent identity.
#[allow(clippy::too_many_arguments)]
async fn checkpoint_active_parent_question(
    store: &bamboo_storage::SessionStoreV2,
    binding: &SessionInboxRuntimeBinding,
    run_id: &str,
    child_id: &str,
    parent_id: &str,
    logical_session: &mut Session,
    creation: &bamboo_subagent::proto::ChildCreationIdentity,
    plain_input: Option<&PlainActorActivation>,
    observation: ParentQuestionCheckpointV1,
) -> Option<String> {
    use bamboo_agent_core::storage::Storage;
    if plain_input.is_some() || observation.validate_shape().is_err() {
        return None;
    }
    // SessionRepository's final save and the direct-parent answer resolver
    // use this same Host-owned lock. Hold it across the V2 read/CAS/save so
    // neither can read an older Child and overwrite the checkpoint afterward.
    let _session_guard = binding
        .parent_question_lock
        .as_ref()?
        .acquire_lock(child_id)
        .await;
    let caller = load_active_subagent_caller(
        store,
        binding,
        run_id,
        child_id,
        parent_id,
        logical_session,
        creation,
        plain_input,
    )
    .await?;
    let parent = store.load_session(parent_id).await.ok().flatten()?;
    if parent.created_at > caller.created_at || !binding.router.owns_run(child_id, run_id).await {
        return None;
    }
    let (saved, question) = store
        .checkpoint_parent_question(&caller, &parent, &observation)
        .await
        .ok()
        .flatten()?;
    // The current Run's local snapshot must carry the committed canonical
    // prefix into finalization; sdk/spawn rechecks disk before its last save.
    logical_session.messages = saved.messages;
    logical_session.pending_question = saved.pending_question;
    if let Some(reason) = saved.metadata.get("runtime.suspend_reason") {
        logical_session
            .metadata
            .insert("runtime.suspend_reason".into(), reason.clone());
    }
    if let Some(request) = saved
        .metadata
        .get(bamboo_domain::PARENT_QUESTION_REQUEST_KEY)
    {
        logical_session.metadata.insert(
            bamboo_domain::PARENT_QUESTION_REQUEST_KEY.into(),
            request.clone(),
        );
    }
    logical_session.metadata.insert(
        "runtime.actor_parent_question_handoff".into(),
        "true".into(),
    );
    Some(question.id.as_str().to_owned())
}

/// An actor Worker cannot see the Host's canonical descendants. At the end of
/// an otherwise completed Child turn, apply the same orphan-child safety net
/// used by the in-process root loop before publishing this Child as terminal.
/// The existing Host wait, including an explicit `any` policy, always wins.
async fn ensure_active_nested_wait(
    tool: &dyn bamboo_agent_core::tools::Tool,
    store: &bamboo_storage::SessionStoreV2,
    binding: &SessionInboxRuntimeBinding,
    run_id: &str,
    child_id: &str,
    parent_id: &str,
    logical_session: &Session,
    creation: &bamboo_subagent::proto::ChildCreationIdentity,
    plain_input: Option<&PlainActorActivation>,
) -> Result<bool, AgentError> {
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{ToolCtx, ToolOutcome};

    let caller = load_active_subagent_caller(
        store,
        binding,
        run_id,
        child_id,
        parent_id,
        logical_session,
        creation,
        plain_input,
    )
    .await
    .ok_or_else(|| AgentError::LLM("nested wait caller is no longer active".into()))?;
    if caller
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.waiting_for_children.as_ref())
        .is_some()
    {
        return Ok(true);
    }

    let active = store
        .list_child_run_statuses(child_id)
        .await
        .map_err(|error| AgentError::LLM(format!("nested child status scan failed: {error}")))?
        .into_iter()
        .filter(|(_, status)| {
            !status.as_deref().is_some_and(|status| {
                matches!(
                    status,
                    "completed" | "error" | "timeout" | "cancelled" | "skipped"
                )
            })
        })
        .map(|(id, _)| id)
        .collect::<Vec<_>>();
    if active.is_empty() {
        return Ok(false);
    }

    let mut ctx = ToolCtx::none(format!("host-nested-orphan-wait:{run_id}"));
    ctx.session_id = Some(Arc::from(caller.id.as_str()));
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        tool.invoke(
            serde_json::json!({"action":"wait", "child_session_ids":active}),
            ctx,
        ),
    )
    .await
    .map_err(|_| AgentError::LLM("nested wait registration timed out".into()))?
    .map_err(|error| AgentError::LLM(format!("nested wait registration failed: {error}")))?;
    let wait_registered = match outcome {
        ToolOutcome::Completed(result) if result.success => {
            let payload: serde_json::Value = serde_json::from_str(&result.result)
                .map_err(|_| AgentError::LLM("nested wait result is invalid".into()))?;
            payload["status"] == "waiting"
        }
        _ => {
            return Err(AgentError::LLM(
                "nested wait registration was not completed".into(),
            ))
        }
    };

    // A grandchild may finish while the wait is being registered and clear it.
    // A clear wait is safe only when no canonical descendants remain active.
    let latest = load_active_subagent_caller(
        store,
        binding,
        run_id,
        child_id,
        parent_id,
        logical_session,
        creation,
        plain_input,
    )
    .await
    .ok_or_else(|| AgentError::LLM("nested wait caller changed during registration".into()))?;
    if latest
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.waiting_for_children.as_ref())
        .is_some()
    {
        return Ok(true);
    }
    let still_active = store
        .list_child_run_statuses(child_id)
        .await
        .map_err(|error| {
            AgentError::LLM(format!("nested child wait verification failed: {error}"))
        })?
        .into_iter()
        .any(|(_, status)| {
            !status.as_deref().is_some_and(|status| {
                matches!(
                    status,
                    "completed" | "error" | "timeout" | "cancelled" | "skipped"
                )
            })
        });
    if still_active {
        return Err(AgentError::LLM(
            "nested child remains active without a durable parent wait".into(),
        ));
    }
    // Keep the current Run's suspend intent even if the last grandchild
    // finished and cleared the Host wait before SDK finalization. Its outcome
    // has already admitted a successor; publishing this Run as completed
    // would release the grandparent on the Child's interim response.
    Ok(wait_registered)
}

fn actor_event_route_error(error: ActorEventRouteError) -> AgentError {
    let message = match error {
        ActorEventRouteError::SequenceGap { .. } => {
            "actor event sequence gap; authoritative session snapshot is required"
        }
        ActorEventRouteError::LifecycleMissing => {
            "actor emitted live content before its activation lifecycle event"
        }
        ActorEventRouteError::ForgedSession => {
            "actor event payload targets a different canonical session"
        }
        ActorEventRouteError::ConflictingReplay => {
            "actor replay reused an event sequence with different content"
        }
        ActorEventRouteError::StaleAuthority => "actor event owner is stale",
        ActorEventRouteError::SequenceExhausted => "actor event sequence is exhausted",
    };
    AgentError::LLM(message.into())
}

const MAX_DISPLAY_CALLS: usize = 4_096;
const MAX_NESTED_DISPLAY_DEPTH: usize = 8;
const MAX_DISPLAY_ID_BYTES: usize = 256;
const MAX_DISPLAY_HOOK_RESULT_DEPTH: usize = 16;

fn hook_result_for_display(result: HookResult, depth: usize) -> Option<HookResult> {
    if depth >= MAX_DISPLAY_HOOK_RESULT_DEPTH {
        return None;
    }
    Some(match result {
        HookResult::Continue => HookResult::Continue,
        HookResult::Mutated => HookResult::Mutated,
        HookResult::Allow => HookResult::Allow,
        HookResult::Ask => HookResult::Ask,
        HookResult::Deny { .. } => HookResult::Deny {
            reason: "Hook denied".into(),
        },
        HookResult::InjectContext { .. } => HookResult::InjectContext {
            text: "Hook context hidden".into(),
        },
        HookResult::WithContext { result, .. } => HookResult::WithContext {
            result: Box::new(hook_result_for_display(*result, depth + 1)?),
            text: "Hook context hidden".into(),
        },
        HookResult::Suspend { .. } => HookResult::Suspend {
            reason: "Hook suspended".into(),
        },
        HookResult::Abort { .. } => HookResult::Abort {
            reason: "Hook aborted".into(),
        },
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DisplayCallKind {
    Ordinary,
    PrivateBrowser,
    Reused,
}

#[derive(Clone, Copy)]
struct DisplayCall {
    kind: DisplayCallKind,
    started: bool,
}

#[derive(Default)]
struct ActorEventDisplay {
    calls: HashMap<String, DisplayCall>,
    hook_names: HashMap<String, usize>,
    children: HashMap<(String, String), Box<ActorEventDisplay>>,
    // Shared by every nested child: a per-level cap alone permits an
    // exponential number of identities across an actor event tree.
    identities: Arc<AtomicUsize>,
    overflowed: bool,
    nested_overflowed: bool,
}

fn is_browser_tool_name(tool_name: &str) -> bool {
    tool_name
        .trim()
        .rsplit("::")
        .next()
        .is_some_and(|name| name.eq_ignore_ascii_case("browser"))
}

fn known_browser_action(args: &serde_json::Value) -> bool {
    matches!(
        args.get("action").and_then(serde_json::Value::as_str),
        Some(
            "tabs"
                | "new_tab"
                | "activate_tab"
                | "close_tab"
                | "navigate"
                | "history"
                | "viewport"
                | "snapshot"
                | "click"
                | "click_at"
                | "hover"
                | "drag"
                | "fill"
                | "select_option"
                | "set_file_input"
                | "type"
                | "press"
                | "key"
                | "scroll"
                | "download"
                | "screenshot"
                | "dialog_respond"
        )
    )
}

fn is_private_browser_call(tool_name: &str, args: &serde_json::Value) -> bool {
    is_browser_tool_name(tool_name)
        && (!known_browser_action(args)
            || args
                .get("action")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|action| {
                    action.eq_ignore_ascii_case("download") || action == "set_file_input"
                })
            || args.get("data_base64").is_some())
}

fn actor_arguments_for_display(tool_name: &str, args: &serde_json::Value) -> serde_json::Value {
    if is_browser_tool_name(tool_name) {
        // A malformed or unknown action has no reliable privacy class. A
        // mixed-version actor may emit it before its schema rejects it.
        if !known_browser_action(args) {
            return serde_json::json!({"details":"[redacted]"});
        }
    }
    tool_start_arguments_for_display(tool_name, args)
}

impl ActorEventDisplay {
    fn fail_closed(&mut self) {
        self.identities
            .store(MAX_DISPLAY_CALLS + 1, Ordering::Relaxed);
        self.overflowed = true;
        self.calls.clear();
        self.hook_names.clear();
        self.nested_overflowed = true;
        self.children.clear();
    }

    fn reserve_identity(&mut self) -> bool {
        if self.identities.fetch_add(1, Ordering::Relaxed) < MAX_DISPLAY_CALLS {
            true
        } else {
            self.fail_closed();
            false
        }
    }

    fn remember_start(&mut self, id: &str, private: bool) -> DisplayCallKind {
        if self.overflowed || id.is_empty() || id.len() > MAX_DISPLAY_ID_BYTES {
            return DisplayCallKind::Reused;
        }
        let kind = if private {
            DisplayCallKind::PrivateBrowser
        } else {
            DisplayCallKind::Ordinary
        };
        if let Some(call) = self.calls.get_mut(id) {
            if call.started || call.kind != kind {
                call.kind = DisplayCallKind::Reused;
            }
            call.started = true;
            return call.kind;
        }
        if !self.reserve_identity() {
            return DisplayCallKind::Reused;
        }
        self.calls.insert(
            id.to_string(),
            DisplayCall {
                kind,
                started: true,
            },
        );
        kind
    }

    fn remember_approval(&mut self, id: &str, private: bool) -> DisplayCallKind {
        if self.overflowed || id.is_empty() || id.len() > MAX_DISPLAY_ID_BYTES {
            return DisplayCallKind::Reused;
        }
        let kind = if private {
            DisplayCallKind::PrivateBrowser
        } else {
            DisplayCallKind::Ordinary
        };
        if let Some(call) = self.calls.get_mut(id) {
            if call.kind != kind {
                call.kind = DisplayCallKind::Reused;
            }
            return call.kind;
        }
        if !self.reserve_identity() {
            return DisplayCallKind::Reused;
        }
        self.calls.insert(
            id.to_string(),
            DisplayCall {
                kind,
                started: false,
            },
        );
        kind
    }

    fn known_kind(&self, id: &str) -> Option<DisplayCallKind> {
        if self.overflowed || self.identities.load(Ordering::Relaxed) > MAX_DISPLAY_CALLS {
            return None;
        }
        self.calls
            .get(id)
            .filter(|call| call.started)
            .map(|call| call.kind)
    }

    fn display_hook_name(&mut self, hook_name: &str) -> Option<String> {
        if self.overflowed || hook_name.is_empty() || hook_name.len() > MAX_DISPLAY_ID_BYTES {
            self.fail_closed();
            return None;
        }
        if let Some(index) = self.hook_names.get(hook_name) {
            return Some(format!("actor-hook-{index}"));
        }
        if !self.reserve_identity() {
            return None;
        }
        let index = self.hook_names.len() + 1;
        self.hook_names.insert(hook_name.to_string(), index);
        Some(format!("actor-hook-{index}"))
    }

    fn project(&mut self, event: AgentEvent, depth: usize) -> Option<AgentEvent> {
        let call_id = match &event {
            AgentEvent::ToolStart { tool_call_id, .. }
            | AgentEvent::ToolApprovalRequested { tool_call_id, .. }
            | AgentEvent::ToolToken { tool_call_id, .. }
            | AgentEvent::ToolComplete { tool_call_id, .. }
            | AgentEvent::ToolError { tool_call_id, .. }
            | AgentEvent::ToolLifecycle { tool_call_id, .. } => Some(tool_call_id),
            _ => None,
        };
        if call_id.is_some_and(|id| id.is_empty() || id.len() > MAX_DISPLAY_ID_BYTES) {
            self.fail_closed();
            return None;
        }
        if self.identities.load(Ordering::Relaxed) > MAX_DISPLAY_CALLS {
            self.fail_closed();
        }
        Some(match event {
            AgentEvent::ToolStart {
                tool_call_id,
                tool_name,
                arguments,
            } => {
                let private = is_private_browser_call(&tool_name, &arguments);
                let kind = self.remember_start(&tool_call_id, private);
                let arguments = if kind == DisplayCallKind::Reused {
                    serde_json::json!({"details":"[redacted]"})
                } else {
                    actor_arguments_for_display(&tool_name, &arguments)
                };
                AgentEvent::ToolStart {
                    tool_call_id,
                    tool_name: match kind {
                        DisplayCallKind::Reused => "unknown".into(),
                        _ if is_browser_tool_name(&tool_name) => "browser".into(),
                        _ => tool_name,
                    },
                    arguments,
                }
            }
            AgentEvent::ToolApprovalRequested {
                tool_call_id,
                tool_name,
                parameters,
            } => {
                let private = is_private_browser_call(&tool_name, &parameters);
                let kind = self.remember_approval(&tool_call_id, private);
                let parameters = if kind == DisplayCallKind::Reused {
                    serde_json::json!({"details":"[redacted]"})
                } else {
                    actor_arguments_for_display(&tool_name, &parameters)
                };
                AgentEvent::ToolApprovalRequested {
                    tool_call_id,
                    tool_name: match kind {
                        DisplayCallKind::Reused => "unknown".into(),
                        _ if is_browser_tool_name(&tool_name) => "browser".into(),
                        _ => tool_name,
                    },
                    parameters,
                }
            }
            AgentEvent::ToolToken {
                tool_call_id,
                content,
            } => {
                let content = match self.known_kind(&tool_call_id) {
                    Some(DisplayCallKind::Ordinary) => content,
                    Some(DisplayCallKind::PrivateBrowser) => "Browser tool output hidden".into(),
                    _ => "Tool output hidden".into(),
                };
                AgentEvent::ToolToken {
                    tool_call_id,
                    content,
                }
            }
            AgentEvent::ToolComplete {
                tool_call_id,
                result,
            } => {
                let result = match self.known_kind(&tool_call_id) {
                    Some(DisplayCallKind::Ordinary) => result,
                    Some(DisplayCallKind::PrivateBrowser) => {
                        bamboo_agent_core::tools::ToolResult::text(
                            result.success,
                            "Browser tool result hidden",
                        )
                    }
                    _ => bamboo_agent_core::tools::ToolResult::text(
                        result.success,
                        "Tool result hidden",
                    ),
                };
                AgentEvent::ToolComplete {
                    tool_call_id,
                    result,
                }
            }
            AgentEvent::ToolError {
                tool_call_id,
                error,
            } => {
                let error = match self.known_kind(&tool_call_id) {
                    Some(DisplayCallKind::Ordinary) => error,
                    Some(DisplayCallKind::PrivateBrowser) => "Browser tool failed".into(),
                    _ => "Tool failed".into(),
                };
                AgentEvent::ToolError {
                    tool_call_id,
                    error,
                }
            }
            AgentEvent::ToolLifecycle {
                tool_call_id,
                tool_name,
                phase,
                elapsed_ms,
                is_mutating,
                auto_approved,
                summary,
                error,
            } => {
                let kind = self.known_kind(&tool_call_id);
                let (summary, error) = match kind {
                    Some(DisplayCallKind::Ordinary) => (summary, error),
                    Some(DisplayCallKind::PrivateBrowser) => (
                        summary.map(|_| "Browser tool activity hidden".into()),
                        error.map(|_| "Browser tool failed".into()),
                    ),
                    _ => (
                        summary.map(|_| "Tool activity hidden".into()),
                        error.map(|_| "Tool failed".into()),
                    ),
                };
                AgentEvent::ToolLifecycle {
                    tool_call_id,
                    tool_name: match kind {
                        Some(DisplayCallKind::Ordinary) => tool_name,
                        Some(DisplayCallKind::PrivateBrowser) => "browser".into(),
                        _ => "unknown".into(),
                    },
                    phase: if kind == Some(DisplayCallKind::Ordinary)
                        || matches!(phase.as_str(), "begin" | "finished" | "error" | "cancelled")
                    {
                        phase
                    } else {
                        "event".into()
                    },
                    elapsed_ms,
                    is_mutating,
                    auto_approved,
                    summary,
                    error,
                }
            }
            AgentEvent::HookLifecycle {
                hook_name,
                point,
                phase,
                duration_ms,
                decision,
            } => {
                let Some(decision) = hook_result_for_display(decision, 0) else {
                    self.fail_closed();
                    return None;
                };
                let hook_name = self.display_hook_name(&hook_name)?;
                let phase = match phase.as_str() {
                    "completed" | "started" | "error" => phase,
                    _ => "event".into(),
                };
                AgentEvent::HookLifecycle {
                    hook_name,
                    point,
                    phase,
                    duration_ms,
                    decision,
                }
            }
            AgentEvent::ChildApprovalRequested {
                child_session_id,
                request_id,
                tool_name,
                permission,
                resource,
            } => {
                let (tool_name, permission, resource) = if is_browser_tool_name(&tool_name) {
                    (
                        "browser".into(),
                        "Browser interaction approval".into(),
                        "[redacted]".into(),
                    )
                } else {
                    (tool_name, permission, resource)
                };
                AgentEvent::ChildApprovalRequested {
                    child_session_id,
                    request_id,
                    tool_name,
                    permission,
                    resource,
                }
            }
            AgentEvent::ChildApprovalChanged { tool_name, .. }
                if is_browser_tool_name(&tool_name) =>
            {
                // This durable approval delta is host-owned. An actor-supplied
                // copy is neither authoritative nor safe to mirror to the
                // account feed; the host publishes its own record separately.
                return None;
            }
            AgentEvent::SubAgentEvent {
                parent_session_id,
                child_session_id,
                event,
            } => {
                if depth >= MAX_NESTED_DISPLAY_DEPTH || self.nested_overflowed {
                    return None;
                }
                if parent_session_id.is_empty()
                    || child_session_id.is_empty()
                    || parent_session_id.len() > MAX_DISPLAY_ID_BYTES
                    || child_session_id.len() > MAX_DISPLAY_ID_BYTES
                {
                    self.fail_closed();
                    return None;
                }
                let key = (parent_session_id.clone(), child_session_id.clone());
                if !self.children.contains_key(&key) && !self.reserve_identity() {
                    return None;
                }
                let identities = self.identities.clone();
                let child = self.children.entry(key).or_insert_with(|| {
                    Box::new(ActorEventDisplay {
                        identities,
                        ..Default::default()
                    })
                });
                let event = child.project(*event, depth + 1)?;
                if self.identities.load(Ordering::Relaxed) > MAX_DISPLAY_CALLS {
                    self.fail_closed();
                    return None;
                }
                AgentEvent::SubAgentEvent {
                    parent_session_id,
                    child_session_id,
                    event: Box::new(event),
                }
            }
            other => other,
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_actor_event(
    event: serde_json::Value,
    strict_permission_events: bool,
    permission_handshake: &mut PermissionPostureHandshake,
    expected_permission_posture: Option<&ExpectedPermissionPosture>,
    session_inbox_runtime: Option<&SessionInboxRuntimeBinding>,
    logical_session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    display: &mut ActorEventDisplay,
) -> crate::runtime::runner::Result<()> {
    let event = match serde_json::from_value::<AgentEvent>(event) {
        Ok(event) => event,
        Err(_) if strict_permission_events => {
            // Serde errors can quote an unrecognized variant or field value.
            // The actor event is untrusted display input, including browser
            // download metadata, so never echo its decode details to SSE.
            return Err(AgentError::LLM(
                "actor emitted malformed AgentEvent under a typed permission posture contract"
                    .to_string(),
            ));
        }
        Err(_) => {
            // A legacy actor can omit a malformed ToolStart that reuses an
            // earlier public call ID. Its later result must not inherit the
            // stale public display classification.
            display.fail_closed();
            return Ok(());
        }
    };
    process_typed_actor_event(
        event,
        permission_handshake,
        expected_permission_posture,
        session_inbox_runtime,
        logical_session,
        event_tx,
        display,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn process_typed_actor_event(
    event: AgentEvent,
    permission_handshake: &mut PermissionPostureHandshake,
    expected_permission_posture: Option<&ExpectedPermissionPosture>,
    session_inbox_runtime: Option<&SessionInboxRuntimeBinding>,
    logical_session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    display: &mut ActorEventDisplay,
) -> crate::runtime::runner::Result<()> {
    if matches!(&event, AgentEvent::PermissionPostureActivated { .. }) {
        if permission_handshake.posture_was_confirmed() {
            return Err(AgentError::LLM(
                "actor emitted a duplicate permission posture activation".to_string(),
            ));
        }
        let seed = permission_posture_seed_from_event(logical_session, &event)
            .map_err(AgentError::LLM)?
            .ok_or_else(|| {
                AgentError::LLM(
                    "actor permission posture event did not decode as a posture".to_string(),
                )
            })?;
        if let Some(expected) = expected_permission_posture {
            if seed.policy_revision != expected.policy_revision
                || seed.resolution != expected.resolution
            {
                return Err(AgentError::LLM(
                    "permission posture event does not match the host-dispatched policy"
                        .to_string(),
                ));
            }
            if seed.executor_mapping() != expected.executor_mapping {
                return Err(AgentError::LLM(
                    "permission posture event does not match the host-dispatched executor mapping"
                        .to_string(),
                ));
            }
        }
        if let Some(binding) = session_inbox_runtime {
            let saved = binding
                .persistence
                .record_permission_posture_activation(
                    &logical_session.id,
                    expected_permission_posture
                        .and_then(|expected| expected.expected_audit_revision),
                    &seed,
                )
                .await
                .map_err(|error| {
                    AgentError::LLM(format!(
                        "persist child permission posture bootstrap: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    AgentError::LLM(
                        "persist child permission posture bootstrap: session not found".to_string(),
                    )
                })?;
            let snapshot = bamboo_domain::PermissionAuditSnapshot::from_metadata(&saved.metadata)
                .ok_or_else(|| {
                AgentError::LLM(
                    "persisted child permission posture audit is incomplete".to_string(),
                )
            })?;
            snapshot.write_to(&mut logical_session.metadata);
        } else {
            // In-memory/custom actor embeddings still use a host-owned clock.
            // Durable server paths always take the atomic branch.
            bamboo_domain::record_permission_audit(&mut logical_session.metadata, &seed, None)
                .map_err(|error| {
                    AgentError::LLM(format!(
                        "record in-memory child permission posture: {error}"
                    ))
                })?;
        }
        // Confirmation is deliberately last: matching alone is not enough.
        // The host-owned audit write must succeed first.
        *permission_handshake = PermissionPostureHandshake::Confirmed;
    } else if permission_handshake.is_awaiting() {
        return Err(AgentError::LLM(
            "actor emitted an execution event before permission posture confirmation".to_string(),
        ));
    }
    if let Some(event) = display.project(event, 0) {
        let _ = event_tx.send(event).await;
    }
    Ok(())
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
        mut expected_permission_posture,
        expected_creation,
        session_inbox_runtime,
        actor_directory_store,
        canonical_subagent_tool,
        activation_run_id,
        execution_epoch,
        expected_source_actor_id,
        initial_inflight_claims,
        first_frame_timeout,
        plain_actor,
        remote_environment_lease,
        mut readonly_output,
        local_history_tools,
        local_history_read_only,
        plain_input,
        canonical_activation,
        canonical_placement_ref,
        actor_event_observer,
        plain_run,
    } = context;

    // A projection belongs only to this Host-validated Run. A loaded Session
    // or a prior attempt may carry a stale marker; only the canonical wait
    // checks in the terminal branch below may set it again.
    logical_session
        .metadata
        .remove("runtime.actor_nested_wait_handoff");

    // First-frame watchdog: a live worker emits its first frame (run-started /
    // first token) within seconds; total silence past the deadline means the
    // worker is dead (e.g. a pooled worker that exited right after checkout), so
    // its Run sits queued forever. We trip ONLY before the first frame — once any
    // frame arrives the worker is proven live and a legitimately long run (a slow
    // tool between tokens) never trips it.
    let mut got_first_frame = false;
    let mut first_frame_watch = first_frame_timeout.map(|d| Box::pin(tokio::time::sleep(d)));
    let mut inflight_claims = initial_inflight_claims;
    let mut owned_input = plain_input.and_then(|activation| activation.initial_input.clone());
    // Retry/recovery already consumes its one input; only a fresh activation
    // can continue twice. Neither budget restarts the Actor lease or watchdog.
    let continuation_limit = if owned_input.is_some() { 1 } else { 2 };
    let mut continuations = usize::from(owned_input.is_some());
    let mut released_input: Option<(
        SessionInboxOwnedClaim,
        bamboo_subagent::proto::InitialInputRelease,
    )> = None;
    let mut current_epoch = execution_epoch;
    let approval_epoch = Arc::new(AtomicU64::new(execution_epoch));
    let approval_cancel = cancel_token.child_token();
    let _approval_lifetime = approval_cancel.clone().drop_guard();
    let mut approval_scopes: HashMap<String, (serde_json::Value, ChildApprovalScope)> =
        HashMap::new();
    let mut owned_tree_requests = 0usize;
    let mut canonical_subagent_requests = 0usize;
    // Once this Run registers a canonical Child wait, a fast completion may
    // clear it before the Worker sends Terminal. Keep the current Run
    // nonterminal so its already admitted outcome belongs to the successor.
    let mut nested_wait_registered_this_run = false;
    let mut parent_question_checkpoint_id: Option<String> = None;
    let strict_permission_events = expected_permission_posture.is_some();
    let mut permission_handshake =
        PermissionPostureHandshake::new(expected_permission_posture.as_ref());
    let mut next_actor_event_seq = 1u64;
    let mut canonical_router: Option<ActorEventRouter> = None;
    let canonical_fence = plain_input
        .map(|activation| &activation.fence)
        .or(canonical_activation);
    let canonical_store = plain_input
        .map(|activation| activation.store.as_ref())
        .or(actor_directory_store);
    let remote_claim_fence =
        canonical_activation.and_then(|fence| actor_directory_store.map(|store| (store, fence)));
    let mut remote_cancel_deadline: Option<tokio::time::Instant> = None;
    let mut display = ActorEventDisplay::default();
    let mut readonly = readonly_output
        .as_ref()
        .map(|_| ReadOnlyActorCollector::default());
    let mut local_history = local_history_tools.map(|_| LocalToolCollector::default());
    loop {
        tokio::select! {
            _ = cancel_token.cancelled(), if remote_cancel_deadline.is_none() => {
                if remote_claim_fence.is_some() {
                    // A remote Worker may finish after its subscriber cancels.
                    // Keep the same correlated frame pump alive briefly so an
                    // exact Cancelled Outcome can terminalize the Actor fence.
                    // Without that proof the activation stays live until its
                    // lease expires and a retry must remain blocked.
                    let _ = client.send(ParentFrame::Cancel).await;
                    remote_cancel_deadline = Some(
                        tokio::time::Instant::now() + Duration::from_secs(10)
                    );
                    continue;
                }
                // Direct/local links retain their existing cancel behavior.
                break;
            }
            _ = async {
                if let Some(deadline) = remote_cancel_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                return Err(AgentError::Cancelled);
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
                if plain_input.is_some() {
                    // Keep the real Inbox pending until this Run's completed
                    // reply can be committed BEFORE its correction User.
                    continue;
                }
                forward_next_canonical_claim(
                    client,
                    session_inbox_runtime.expect("guarded"),
                    logical_session,
                    activation_run_id.expect("guarded"),
                    &mut inflight_claims,
                    remote_claim_fence,
                )
                .await?;
            }
            Some(frame) = live_rx.recv() => {
                if plain_actor { return Err(plain_actor_unsupported()); }
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
                        if canonical_activation.is_some() {
                            return Err(AgentError::LLM("remote_actor_event_batch_required".into()));
                        }
                        if local_history.is_some() {
                            return Err(local_tool_history_unsupported());
                        }
                        if event.get("initial_input_control").is_some() {
                            let bamboo_subagent::proto::InitialInputControl::Request { request } =
                                bamboo_subagent::proto::InitialInputControl::decode(event).map_err(|_| plain_actor_unsupported())?
                            else { return Err(plain_actor_unsupported()); };
                            if !permission_handshake.posture_was_confirmed() || cancel_token.is_cancelled() { return Err(plain_actor_unsupported()); }
                            let activation = plain_input.ok_or_else(plain_actor_unsupported)?;
                            let binding = session_inbox_runtime.ok_or_else(plain_actor_unsupported)?;
                            let (run, _) = plain_run.ok_or_else(plain_actor_unsupported)?;
                            let claim = owned_input.as_ref().or_else(|| released_input.as_ref().map(|(claim, _)| claim))
                                .ok_or_else(plain_actor_unsupported)?;
                            let release = activation.release_initial_input(binding, logical_session, run, current_epoch, claim,
                                &request, &mut expected_permission_posture, released_input.as_ref().map(|(_, release)| release)).await?;
                            if cancel_token.is_cancelled() { return Err(AgentError::Cancelled); }
                            client.send(ParentFrame::InitialInputRelease { release: release.clone() }).await
                                .map_err(|_| AgentError::LLM("Actor initial release unconfirmed; durable input is preserved".into()))?;
                            released_input = Some((claim.clone(), release));
                            owned_input = None;
                            continue;
                        }
                        if expected_creation.is_some() {
                            return Err(AgentError::LLM("worker omitted required Child creation event identity".into()));
                        }
                        if event["type"] == bamboo_subagent::proto::LocalToolMessages::TYPE { continue; }
                        if event["type"] == "owned_readonly_transcript" {
                            if plain_actor { return Err(plain_actor_unsupported()); }
                            continue;
                        }
                        if plain_actor && !plain_actor_event(&event)? { continue; }
                        if let Some(activation) = plain_input {
                            validate_directory_actor_event(
                                activation.store.as_ref(),
                                &activation.fence,
                                logical_session,
                                None,
                            ).await?;
                        }
                        // Rolling-upgrade compatibility: old actors have no
                        // route/sequence metadata, but retain the same typed
                        // permission handshake and event validation.
                        process_actor_event(
                            event,
                            strict_permission_events,
                            &mut permission_handshake,
                            expected_permission_posture.as_ref(),
                            session_inbox_runtime,
                            logical_session,
                            event_tx,
                            &mut display,
                        )
                        .await?;
                    }
                    Ok(Some(ChildFrame::EventBatch { batch })) => {
                        validate_actor_event_batch(
                            &batch,
                            logical_session,
                            parent_session_id,
                            activation_run_id,
                            current_epoch,
                            expected_source_actor_id,
                            expected_creation,
                        )?;
                        let directory_entry = if let (Some(store), Some(fence)) = (canonical_store, canonical_fence) {
                            let entry = validate_directory_actor_event(
                                store, fence, logical_session, batch.activation_id.as_deref(),
                            ).await?;
                            if canonical_placement_ref.is_some_and(|expected| {
                                entry.activation.as_ref().and_then(|activation| activation.placement_ref.as_ref()) != Some(expected)
                            }) {
                                return Err(AgentError::LLM("remote_actor_placement_ref_changed".into()));
                            }
                            Some(entry)
                        } else { None };
                        if let (Some(entry), Some(fence)) = (&directory_entry, canonical_fence) {
                            if canonical_router.is_none() {
                                canonical_router = Some(
                                    ActorEventRouter::new(
                                        entry,
                                        fence,
                                        current_epoch,
                                        strict_permission_events && permission_handshake.is_awaiting(),
                                    )
                                    .map_err(actor_event_route_error)?,
                                );
                            }
                        }
                        if local_history.is_some() && batch.first_seq != next_actor_event_seq {
                            return Err(local_tool_history_unsupported());
                        }
                        if batch.last_seq < next_actor_event_seq {
                            continue;
                        }
                        if batch.first_seq > next_actor_event_seq {
                            if plain_actor {
                                return Err(AgentError::LLM(
                                    "actor event sequence gap; authoritative session snapshot is required".into(),
                                ));
                            }
                            tracing::warn!(
                                child_session_id,
                                expected_seq = next_actor_event_seq,
                                received_seq = batch.first_seq,
                                "actor event sequence gap; authoritative session snapshot is required"
                            );
                            // A missing ToolStart may have reused an earlier
                            // call ID for a private browser download. No prior
                            // identity remains safe for display after a gap.
                            display.fail_closed();
                        }
                        let skip = next_actor_event_seq
                            .saturating_sub(batch.first_seq)
                            .min(batch.events.len() as u64) as usize;
                        for (offset, event) in batch.events.into_iter().enumerate().skip(skip) {
                            let seq = batch.first_seq + offset as u64;
                            let mut publish = true;
                            if let Some(collector) = local_history.as_mut() {
                                if permission_handshake.is_awaiting() && event["type"] != "permission_posture_activated" {
                                    return Err(local_tool_history_unsupported());
                                }
                                publish = collector.event(&event)?;
                            } else if event["type"] == bamboo_subagent::proto::LocalToolMessages::TYPE {
                                // Completion DATA is private and grants nothing on an unselected route.
                                publish = false;
                            }
                            if publish { publish = if let Some(collector) = readonly.as_mut() {
                                if permission_handshake.is_awaiting()
                                    && event["type"] != "permission_posture_activated" {
                                    return Err(plain_actor_unsupported());
                                }
                                collector.event(&event)?
                            } else if event["type"] == "owned_readonly_transcript" {
                                if plain_actor { return Err(plain_actor_unsupported()); }
                                false // A worker cache observation is not a legacy Host commit.
                            } else if plain_actor { plain_actor_event(&event)? } else { true };
                            }
                            if let (Some(router), Some(entry), Some(fence)) =
                                (canonical_router.as_mut(), directory_entry.as_ref(), canonical_fence)
                            {
                                let typed = if publish {
                                    Some(serde_json::from_value::<AgentEvent>(event.clone())
                                        .map_err(|_| plain_actor_unsupported())?)
                                } else {
                                    None
                                };
                                match router.route(entry, fence, current_epoch, seq, typed)
                                    .map_err(actor_event_route_error)?
                                {
                                    ActorEventRoute::Publish(envelope) => {
                                        let public_event = actor_event_observer
                                            .filter(|_| envelope.class != ActorEventClass::Ephemeral)
                                            .map(|_| PublicActorEvent::from(&envelope));
                                        tracing::trace!(
                                            actor_id = %envelope.actor_id,
                                            event_id = %envelope.event_id,
                                            class = ?envelope.class,
                                            "publishing canonical Actor event"
                                        );
                                        process_typed_actor_event(
                                            envelope.payload,
                                            &mut permission_handshake,
                                            expected_permission_posture.as_ref(),
                                            session_inbox_runtime,
                                            logical_session,
                                            event_tx,
                                            &mut display,
                                        ).await?;
                                        if let (Some(observer), Some(public_event)) =
                                            (actor_event_observer, public_event)
                                        {
                                            observer.publish(public_event);
                                        }
                                    }
                                    ActorEventRoute::Duplicate | ActorEventRoute::Suppressed => {}
                                }
                                next_actor_event_seq = seq.saturating_add(1);
                                continue;
                            }
                            if !publish {
                                next_actor_event_seq = seq.saturating_add(1);
                                continue;
                            }
                            process_actor_event(
                                event,
                                strict_permission_events,
                                &mut permission_handshake,
                                expected_permission_posture.as_ref(),
                                session_inbox_runtime,
                                logical_session,
                                event_tx,
                                &mut display,
                            )
                            .await?;
                            next_actor_event_seq = seq.saturating_add(1);
                        }
                    }
                    Ok(Some(ChildFrame::OwnedTreeRequest { id, cursor })) => {
                        owned_tree_requests += 1;
                        if owned_tree_requests > 16 {
                            return Err(AgentError::LLM("owned tree request limit exceeded".into()));
                        }
                        let page = if id.len() <= 128
                            && cursor.as_ref().is_none_or(|value| !value.is_empty() && value.len() <= 128)
                            && !cancel_token.is_cancelled()
                        {
                            match (actor_directory_store, session_inbox_runtime,
                                activation_run_id, expected_creation) {
                                (Some(store), Some(binding), Some(run_id), Some(creation)) => {
                                    tokio::time::timeout(
                                        Duration::from_secs(8),
                                        inspect_active_owned_tree(
                                            store, binding, run_id, child_session_id,
                                            parent_session_id, logical_session, creation,
                                            plain_input, cursor.as_deref(),
                                        ),
                                    ).await.ok().flatten()
                                }
                                _ => {
                                    tracing::warn!("owned tree denied: canonical Host scope unavailable");
                                    None
                                },
                            }
                        } else {
                            tracing::warn!("owned tree denied: request bounds or cancellation");
                            None
                        };
                        if cancel_token.is_cancelled() {
                            return Err(AgentError::Cancelled);
                        }
                        client.send(ParentFrame::OwnedTreeReply { id, page }).await
                            .map_err(|_| AgentError::LLM("owned tree reply transport closed".into()))?;
                    }
                    Ok(Some(ChildFrame::SubAgentRequest { id, tool_call_id, args })) => {
                        canonical_subagent_requests += 1;
                        if canonical_subagent_requests > 64 {
                            return Err(AgentError::LLM("canonical SubAgent request limit exceeded".into()));
                        }
                        let valid_shape = !id.is_empty() && id.len() <= 128
                            && !tool_call_id.is_empty() && tool_call_id.len() <= 128
                            && args.is_object()
                            && serde_json::to_vec(&args)
                                .is_ok_and(|bytes| bytes.len() <= 64 * 1024);
                        let registers_wait = matches!(args.get("action").and_then(|value| value.as_str()),
                            Some("wait"))
                            || (args.get("action").and_then(|value| value.as_str()) == Some("create")
                                && args.get("wait").and_then(|value| value.as_bool()) == Some(true));
                        let checkpoint_payload = args.get(PARENT_QUESTION_CHECKPOINT_ACTION).cloned();
                        let result = if valid_shape
                            && !cancel_token.is_cancelled()
                            && !permission_handshake.is_awaiting()
                        {
                            if let Some(payload) = checkpoint_payload {
                                let parsed = (args.as_object().is_some_and(|object| object.len() == 1))
                                    .then(|| serde_json::from_value::<ParentQuestionCheckpointV1>(payload).ok())
                                    .flatten()
                                    .filter(|observation| observation.tool_call_id == tool_call_id);
                                match (parsed, actor_directory_store, session_inbox_runtime,
                                    activation_run_id, expected_creation) {
                                    (Some(observation), Some(store), Some(binding), Some(run_id), Some(creation)) => {
                                        let receipt = tokio::time::timeout(Duration::from_secs(25),
                                            checkpoint_active_parent_question(
                                                store, binding, run_id, child_session_id,
                                                parent_session_id, logical_session, creation,
                                                plain_input, observation,
                                            )).await.ok().flatten();
                                        if let Some(request_id) = receipt {
                                            parent_question_checkpoint_id = Some(request_id.clone());
                                            serde_json::json!({"result":{"request_id":request_id}})
                                        } else {
                                            serde_json::json!({"error":"canonical Child question checkpoint rejected; persisted worker question is preserved"})
                                        }
                                    }
                                    _ => serde_json::json!({"error":"canonical Child question checkpoint unavailable"}),
                                }
                            } else {
                            match (canonical_subagent_tool.as_deref(), actor_directory_store,
                                session_inbox_runtime, activation_run_id, expected_creation) {
                                (Some(tool), Some(store), Some(binding), Some(run_id), Some(creation)) => {
                                    if let Some(caller) = load_active_subagent_caller(
                                        store, binding, run_id, child_session_id,
                                        parent_session_id, logical_session, creation,
                                        plain_input,
                                    ).await {
                                        tokio::time::timeout(
                                            Duration::from_secs(30),
                                            invoke_active_subagent(tool, &caller, args, &tool_call_id),
                                        ).await.unwrap_or_else(|_| serde_json::json!({
                                            "error":"canonical SubAgent operation timed out; inspect logical child state before retrying"
                                        }))
                                    } else {
                                        serde_json::json!({"error":"canonical SubAgent caller is no longer active"})
                                    }
                                }
                                _ => serde_json::json!({"error":"canonical SubAgent Host authority unavailable"}),
                            }
                            }
                        } else {
                            serde_json::json!({"error":"canonical SubAgent request rejected"})
                        };
                        if registers_wait
                            && result["result"]["success"] == true
                            && result["result"]["result"]
                                .as_str()
                                .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                                .is_some_and(|value| value["status"] == "waiting")
                        {
                            nested_wait_registered_this_run = true;
                        }
                        if cancel_token.is_cancelled() {
                            return Err(AgentError::Cancelled);
                        }
                        client.send(ParentFrame::SubAgentReply { id, result }).await
                            .map_err(|_| AgentError::LLM("canonical SubAgent reply transport closed".into()))?;
                    }
                    Ok(Some(ChildFrame::ApprovalRequest { id, body })) => {
                        if plain_actor { return Err(plain_actor_unsupported()); }
                        if permission_handshake.is_awaiting() {
                            return Err(AgentError::LLM(
                                "actor requested approval before permission posture confirmation"
                                    .to_string(),
                            ));
                        }
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
                            let Some((binding, run)) = session_inbox_runtime.zip(activation_run_id) else {
                                // Legacy/custom reviewers retain their original boolean seam;
                                // the canonical server reviewer denies without a live scope.
                                tokio::spawn(async move {
                                    let approved = tokio::time::timeout(CHILD_APPROVAL_TIMEOUT,
                                        reviewer.review(&parent, &child, &body)).await.unwrap_or(false);
                                    super::live::deliver_approval_scoped(registry.as_ref(), &child,
                                        child_attempt, &req_id, approved);
                                });
                                continue;
                            };
                            let generation = body.pointer("/permission_request/request_generation")
                                .and_then(serde_json::Value::as_str).unwrap_or("").to_string();
                            let scope = if let Some((original, scope)) = approval_scopes.get(&generation) {
                                if original != &body || scope.stamp()["reply"] != req_id {
                                    return Err(AgentError::LLM("approval generation changed in live scope".into()));
                                }
                                scope.clone()
                            } else {
                                if approval_scopes.len() >= 64 {
                                    return Err(AgentError::LLM("approval live scope limit exceeded".into()));
                                }
                                let scope = ChildApprovalScope::new(&parent, &child,
                                    (child_attempt, run, current_epoch, &req_id, chrono::Utc::now() + chrono::Duration::seconds(240)), binding.router.clone(),
                                    approval_cancel.clone(), approval_epoch.clone());
                                approval_scopes.insert(generation, (body.clone(), scope.clone()));
                                scope
                            };
                            tokio::spawn(async move {
                                let result = tokio::time::timeout(
                                    CHILD_APPROVAL_TIMEOUT,
                                    reviewer.review_scoped(&parent, &child, &body, &scope),
                                ).await.unwrap_or(ChildApprovalReview::Reply(false));
                                if let ChildApprovalReview::Reply(approved) = result {
                                    if scope.is_current(&parent, &child).await {
                                        super::live::deliver_approval_scoped(registry.as_ref(), &child,
                                            child_attempt, &req_id, approved && chrono::Utc::now() < scope.deadline());
                                    }
                                }
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
                        if let Some(activation) = plain_input {
                            if activation.initial_release_required { return Err(plain_actor_unsupported()); }
                            let binding = session_inbox_runtime.ok_or_else(plain_actor_unsupported)?;
                            let run_id = activation_run_id.ok_or_else(plain_actor_unsupported)?;
                            let claim = owned_input.as_ref().ok_or_else(plain_actor_unsupported)?;
                            activation.confirm_input(binding, logical_session, run_id, claim, &confirmation).await?;
                            owned_input = None;
                            continue;
                        }
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
                        checkpoint_and_ack_canonical_claim(
                            binding,
                            logical_session,
                            &claim,
                            remote_claim_fence,
                        )
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
                                remote_claim_fence,
                            )
                            .await?;
                        }
                    }
                    Ok(Some(ChildFrame::Terminal { status, result, error, .. })) => {
                        if remote_cancel_deadline.is_some() && status == TerminalStatus::Cancelled {
                            // A selected Worker terminal proves that its Run
                            // ended even if permission posture or an Inbox
                            // correction was still pending. Claims remain
                            // unacked for retry; only the placement fence can
                            // now be released after SDK saves Cancelled.
                            client.accept_durable_terminal(status);
                            return Err(AgentError::Cancelled);
                        }
                        if parent_question_checkpoint_id.is_some()
                            && status != TerminalStatus::Suspended
                        {
                            return Err(AgentError::LLM(
                                "Child question checkpoint must end in a suspended Run".into(),
                            ));
                        }
                        if permission_handshake.is_awaiting() {
                            if remote_environment_lease && status == TerminalStatus::Error {
                                if let Some(code) = error.as_deref().and_then(remote_environment_error_code) {
                                    // The Worker rejected the EnvironmentLease before it
                                    // could confirm its permission posture. This exact
                                    // correlated Outcome still proves the Run ended, so
                                    // retain its durable receipt and release the slot after
                                    // the Host records the failure. Other early terminals
                                    // remain unproven and keep their placement fence.
                                    client.accept_durable_terminal(status);
                                    return Err(AgentError::LLM(code.to_owned()));
                                }
                            }
                            return Err(AgentError::LLM(
                                "actor terminated before permission posture confirmation"
                                    .to_string(),
                            ));
                        }
                        if owned_input.is_some() {
                            return Err(AgentError::LLM("Actor terminated before confirming its durable correction; Inbox claim is retained".into()));
                        }
                        if let Some(claim) = inflight_claims.front() {
                            return Err(AgentError::LLM(format!(
                                "actor terminated before durably admitting SessionInbox message {}; canonical claim remains recoverable",
                                claim.envelope.id
                            )));
                        }
                        if status == TerminalStatus::Completed {
                            if let Some(collector) = local_history.take() {
                                commit_local_tool_history(
                                    collector, logical_session,
                                    session_inbox_runtime.ok_or_else(local_tool_history_unsupported)?,
                                    activation_run_id.ok_or_else(local_tool_history_unsupported)?,
                                    (local_history_tools.ok_or_else(local_tool_history_unsupported)?, local_history_read_only),
                                    result.as_deref(), cancel_token,
                                ).await?;
                            }
                        }
                        if let Some(collector) = readonly.take() {
                            if status == TerminalStatus::Completed {
                                let activation = plain_input.ok_or_else(plain_actor_unsupported)?;
                                let backlog = activation.input_inbox.inspect(&logical_session.id).await
                                    .map_err(|_| plain_actor_unsupported())?;
                                if backlog.pending != 0 || backlog.claimed != 0 || backlog.activation_pending() {
                                    return Err(AgentError::LLM("Glob Actor continuation is unsupported; pending Inbox input is preserved".into()));
                                }
                                *readonly_output.as_deref_mut().ok_or_else(plain_actor_unsupported)? =
                                    collector.finish(result.as_deref())?;
                            }
                        } else if let Some(activation) = plain_input {
                            if status == TerminalStatus::Completed {
                                let binding = session_inbox_runtime.ok_or_else(plain_actor_unsupported)?;
                                let run_id = activation_run_id.ok_or_else(plain_actor_unsupported)?;
                                if continuations < continuation_limit {
                                    if let Some((claim, epoch)) = activation.continue_input(
                                        client, binding, logical_session, run_id, plain_run, &mut expected_permission_posture,
                                        result.clone().filter(|text| !text.is_empty()).ok_or_else(plain_actor_unsupported)?, event_tx,
                                    ).await? {
                                        owned_input = Some(claim);
                                        released_input = None;
                                        continuations += 1;
                                        current_epoch = epoch;
                                        approval_epoch.store(epoch, Ordering::Release);
                                        // These are native coordinates scoped to the newly
                                        // correlated epoch. Public Host feed seq is untouched.
                                        next_actor_event_seq = 1;
                                        canonical_router = None;
                                        permission_handshake = PermissionPostureHandshake::new(expected_permission_posture.as_ref());
                                        display = ActorEventDisplay::default();
                                        continue;
                                    }
                                } else if activation.input_inbox.inspect(&logical_session.id).await
                                    .map_err(|_| plain_actor_unsupported())?.activation_pending() {
                                    if continuation_limit == 1 {
                                        return Err(AgentError::LLM("Only one Actor correction continuation is supported; remaining Inbox input is preserved".into()));
                                    }
                                    // The outer success path appends the last reply. An
                                    // explicit cap refusal must preserve it here instead,
                                    // without claiming/acking the excess input or saving
                                    // through the ordinary writer on this owned Actor.
                                    activation.append_reply(logical_session,
                                        result.clone().filter(|text| !text.is_empty()).ok_or_else(plain_actor_unsupported)?,
                                        event_tx).await?;
                                    return Err(AgentError::LLM("Actor correction continuation limit reached; completed reply committed and remaining Inbox input preserved".into()));
                                }
                            }
                        }
                        if status == TerminalStatus::Completed {
                            // Only an activation with a canonical Child creation
                            // identity can invoke the Host SubAgent route. Legacy
                            // Echo and other executors may share this runner's
                            // installed tool but have no nested wait to fence.
                            if let (Some(tool), Some(creation)) =
                                (canonical_subagent_tool.as_deref(), expected_creation)
                            {
                                let (Some(store), Some(binding), Some(run_id)) =
                                    (actor_directory_store, session_inbox_runtime,
                                        activation_run_id)
                                else {
                                    return Err(AgentError::LLM(
                                        "nested SubAgent Host authority unavailable at Child terminal"
                                            .into(),
                                    ));
                                };
                                let must_suspend = ensure_active_nested_wait(
                                    tool, store, binding, run_id, child_session_id,
                                    parent_session_id, logical_session, creation, plain_input,
                                ).await?;
                                if must_suspend || nested_wait_registered_this_run {
                                    // This is only a current-Run handoff fence.
                                    // sdk/spawn re-reads the durable Host wait;
                                    // copying a stale wait into this Session
                                    // would re-arm it after a fast grandchild
                                    // completion, while copying no wait would
                                    // erase an active registration on final save.
                                    logical_session.metadata.insert(
                                        "runtime.actor_nested_wait_handoff".into(),
                                        "true".into(),
                                    );
                                }
                            }
                        }
                        let terminal_result = match status {
                            TerminalStatus::Completed => Ok(result),
                            TerminalStatus::Cancelled => Err(AgentError::Cancelled),
                            TerminalStatus::Error => Err(AgentError::LLM(
                                error.unwrap_or_else(|| "actor child errored".to_string()),
                            )),
                            TerminalStatus::Suspended => {
                                use bamboo_agent_core::storage::Storage;
                                let (Some(request_id), Some(store), Some(binding), Some(run_id), Some(creation)) =
                                    (parent_question_checkpoint_id.as_ref(), actor_directory_store,
                                        session_inbox_runtime, activation_run_id, expected_creation)
                                else {
                                    return Err(AgentError::LLM("uncheckpointed Child suspension rejected".into()));
                                };
                                if load_active_subagent_caller(
                                    store, binding, run_id, child_session_id,
                                    parent_session_id, logical_session, creation, plain_input,
                                ).await.is_none() {
                                    return Err(AgentError::LLM("stale Child question suspension rejected".into()));
                                }
                                let id = bamboo_domain::SessionMessageId::parse(request_id.clone())
                                    .map_err(|_| AgentError::LLM("invalid Child question receipt".into()))?;
                                let child = store.load_session(child_session_id).await
                                    .map_err(|_| AgentError::LLM("canonical Child question unavailable".into()))?
                                    .ok_or_else(|| AgentError::LLM("canonical Child question missing".into()))?;
                                let parent = store.load_session(parent_session_id).await
                                    .map_err(|_| AgentError::LLM("canonical direct parent unavailable".into()))?
                                    .ok_or_else(|| AgentError::LLM("canonical direct parent missing".into()))?;
                                let pending = ParentQuestion::for_pending(&parent, &child)
                                    .is_some_and(|question| question.id == id);
                                let answered = bamboo_domain::ParentQuestionResolution::from_child(
                                    &parent, &child, &id,
                                ).is_some();
                                if !pending && !answered {
                                    return Err(AgentError::LLM("canonical Child question proof changed".into()));
                                }
                                logical_session.metadata.insert(
                                    "runtime.actor_parent_question_handoff".into(), "true".into(),
                                );
                                Ok(None)
                            }
                        };
                        // This marker is set only after every Host terminal
                        // validation above succeeded. A surfaced broker Outcome
                        // that failed permission, Inbox, or question checks must
                        // never receive a durable ACK merely because SDK saved
                        // an error status. Genuine Worker Error/Cancelled frames
                        // are accepted terminals even though drive returns Err.
                        client.accept_durable_terminal(status);
                        return terminal_result;
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

fn remote_broker_unavailable() -> AgentError {
    AgentError::LLM("remote_broker_activation_unavailable".into())
}

/// Only these bounded worker admission errors may cross the fixed Remote
/// diagnostic boundary before permission posture confirmation. Transport and
/// arbitrary worker text retain the generic remote error.
fn remote_environment_error_code(value: &str) -> Option<&'static str> {
    const CODES: &[&str] = &[
        "remote_environment_lease_invalid",
        "remote_environment_snapshot_mismatch",
        "remote_environment_checkout_not_clean",
        "remote_environment_checkout_unsupported",
        "remote_environment_workspace_unavailable",
        "remote_environment_workspace_outside_git_root",
        "remote_environment_path_unsupported",
        "remote_environment_git_unavailable",
        "remote_environment_git_inspection_failed",
        "remote_environment_git_inspection_timed_out",
        "remote_environment_git_output_too_large",
        "remote_environment_snapshot_too_large",
        "remote_environment_snapshot_changed",
        "remote_environment_index_invalid",
        "remote_environment_index_unsupported",
        "remote_environment_host_path_or_grants_rejected",
        "remote_environment_worker_workspace_missing",
        "remote_environment_runspec_invalid",
        "remote_environment_lease_missing_or_host_path_present",
    ];
    CODES.iter().copied().find(|code| *code == value)
}

// Read only: no ensure/repair/Actor initialization or subtree budget. Capture
// exactly the canonical chain and compare it again immediately before Run.
async fn remote_canonical_lineage(
    binding: &SessionInboxRuntimeBinding,
    session: &Session,
) -> Result<Vec<bamboo_domain::ActorSession>, AgentError> {
    let mut current = bamboo_domain::ActorSession::from_session(session)
        .map_err(|_| remote_broker_unavailable())?;
    let mut chain = Vec::new();
    loop {
        if chain.len() > MAX_SPAWN_DEPTH as usize {
            return Err(remote_broker_unavailable());
        }
        let saved = binding
            .storage
            .load_session(&current.actor_id)
            .await
            .map_err(|_| remote_broker_unavailable())?
            .ok_or_else(remote_broker_unavailable)?;
        let observed = bamboo_domain::ActorSession::from_session(&saved)
            .map_err(|_| remote_broker_unavailable())?;
        if observed.actor_id != current.actor_id
            || observed.session_created_at != current.session_created_at
            || observed.parent_actor_id != current.parent_actor_id
            || observed.root_actor_id != current.root_actor_id
            || observed.spawn_depth != current.spawn_depth
            || observed.project_id != current.project_id
        {
            return Err(remote_broker_unavailable());
        }
        chain.push(observed.clone());
        let Some(parent_id) = observed.parent_actor_id.as_ref() else {
            if observed.actor_id != session.root_session_id || observed.spawn_depth != 0 {
                return Err(remote_broker_unavailable());
            }
            return Ok(chain);
        };
        let parent = binding
            .storage
            .load_session(parent_id)
            .await
            .map_err(|_| remote_broker_unavailable())?
            .ok_or_else(remote_broker_unavailable)?;
        let parent = bamboo_domain::ActorSession::from_session(&parent)
            .map_err(|_| remote_broker_unavailable())?;
        if parent.root_actor_id != observed.root_actor_id
            || parent.project_id != observed.project_id
            || parent.spawn_depth.checked_add(1) != Some(observed.spawn_depth)
            || parent.session_created_at > observed.session_created_at
        {
            return Err(remote_broker_unavailable());
        }
        current = parent;
    }
}

async fn canonical_child_creation(
    binding: &SessionInboxRuntimeBinding,
    session: &Session,
    job: &SpawnJob,
) -> Result<bamboo_subagent::proto::ChildCreationIdentity, AgentError> {
    let conflict = || {
        AgentError::LLM(
            "child creation identity unavailable or changed in canonical host Storage".into(),
        )
    };
    let current = binding
        .storage
        .load_session(&job.child_session_id)
        .await
        .map_err(|_| conflict())?
        .ok_or_else(conflict)?;
    if bamboo_domain::ActorSession::from_session(&current).is_err()
        || bamboo_domain::ActorSession::from_session(session).is_err()
        || current.kind != bamboo_domain::SessionKind::Child
        || session.kind != current.kind
        || current.id != session.id
        || current.id != job.child_session_id
        || current.parent_session_id.as_deref() != Some(job.parent_session_id.as_str())
        || current.parent_session_id != session.parent_session_id
        || current.root_session_id.trim().is_empty()
        || current.root_session_id != session.root_session_id
        || current.created_at != session.created_at
        || current.spawn_depth != session.spawn_depth
        || current.spawn_depth == 0
        || project_id_for_actor_run(&current)? != project_id_for_actor_run(session)?
    {
        return Err(conflict());
    }
    Ok(bamboo_subagent::proto::ChildCreationIdentity {
        created_at: current.created_at,
        spawn_depth: current.spawn_depth,
    })
}

fn logical_identity_for_actor_run(session: &Session, job: &SpawnJob) -> LogicalSessionIdentity {
    LogicalSessionIdentity {
        creation: None,
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

    #[tokio::test]
    async fn pre_dispatch_placement_failure_fails_only_claimed_activation() {
        let temp = tempfile::tempdir().unwrap();
        let store = bamboo_storage::SessionStoreV2::new(temp.path().into())
            .await
            .unwrap();
        let parent = Session::new("placement-parent", "model");
        let child = Session::new_child_of("placement-child", &parent, "model", "task");
        store.save_session(&parent).await.unwrap();
        store.save_session(&child).await.unwrap();
        store.ensure_actor(&child.id).await.unwrap();
        store
            .bind_control_plane(
                &child.id,
                bamboo_domain::ActorControlPlaneBinding {
                    policy_revision: None,
                    placement_intent: Some(bamboo_domain::ActorPlacementIntent::Pool {
                        pool_id: "remote-worker".into(),
                    }),
                },
            )
            .await
            .unwrap();

        let claim = |run_id: &str| ActorActivationClaim {
            actor_id: child.id.clone(),
            run_id: run_id.into(),
            lease_owner: format!("host-{run_id}"),
            lease_expires_at: chrono::Utc::now() + chrono::Duration::seconds(80),
            inbox_generation: 0,
            placement_ref: Some(bamboo_domain::ActorPlacementRef {
                class: bamboo_domain::ActorPlacementClass::Remote,
                lease_id: format!("slot-{run_id}"),
                slot_epoch: Some(1),
            }),
            now: chrono::Utc::now(),
        };
        let first = store.claim_activation(&claim("first")).await.unwrap();
        store
            .start_activation(&first.fence(), chrono::Utc::now())
            .await
            .unwrap();
        assert!(store.claim_activation(&claim("retry")).await.is_err());

        // This is the post-claim placement preflight failure path. No Run has
        // been dispatched, so the same ActorId can retry before lease expiry.
        fail_pre_dispatch_activation(&store, &first.fence())
            .await
            .unwrap();
        let failed = store.inspect_actor(&child.id).await.unwrap();
        assert_eq!(
            failed.activation.unwrap().status,
            bamboo_domain::ActorActivationStatus::Failed
        );
        let retry = store.claim_activation(&claim("retry")).await.unwrap();
        assert_eq!(retry.attempt, first.attempt + 1);
        assert!(fail_pre_dispatch_activation(&store, &first.fence())
            .await
            .is_err());
        assert_eq!(
            store.inspect_actor(&child.id).await.unwrap().activation,
            Some(retry)
        );
    }

    #[tokio::test]
    async fn lost_run_delivery_receipt_keeps_actor_activation_live() {
        struct AcceptedWithoutReceipt {
            published: bool,
        }

        #[async_trait]
        impl bamboo_subagent::ChildLink for AcceptedWithoutReceipt {
            async fn send(&mut self, frame: ParentFrame) -> bamboo_subagent::TransportResult<()> {
                assert!(matches!(frame, ParentFrame::Run(_)));
                self.published = true;
                Err(bamboo_subagent::TransportError::Protocol(
                    "broker delivery receipt lost after publication".into(),
                ))
            }

            async fn next_frame(&mut self) -> bamboo_subagent::TransportResult<Option<ChildFrame>> {
                std::future::pending().await
            }
        }

        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().into())
                .await
                .unwrap(),
        );
        let parent = Session::new("dispatch-parent", "model");
        let child = Session::new_child_of("dispatch-child", &parent, "model", "task");
        store.save_session(&parent).await.unwrap();
        store.save_session(&child).await.unwrap();
        store.ensure_actor(&child.id).await.unwrap();
        let now = chrono::Utc::now();
        let claim = ActorActivationClaim {
            actor_id: child.id.clone(),
            run_id: "dispatch-run".into(),
            lease_owner: "dispatch-host".into(),
            lease_expires_at: now + chrono::Duration::seconds(80),
            inbox_generation: 0,
            placement_ref: None,
            now,
        };
        let activation = store.claim_activation(&claim).await.unwrap();
        store
            .start_activation(&activation.fence(), chrono::Utc::now())
            .await
            .unwrap();
        let mut link = AcceptedWithoutReceipt { published: false };
        let mut send_invoked = false;
        let run = RunSpec {
            assignment: "task".into(),
            logical_session: None,
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: Vec::new(),
            activation_run_id: Some(claim.run_id.clone()),
            execution_epoch: 1,
            initial_session_messages: Vec::new(),
            secrets: Default::default(),
        };
        assert!(send_actor_run(&mut link, run, &mut send_invoked)
            .await
            .is_err());
        assert!(send_invoked && link.published);
        let slot = WorkerSlotLease {
            lease_id: "dispatch-slot".into(),
            host_ref: "dispatch-worker".into(),
            connection_generation: "dispatch-generation".into(),
            slot: 0,
            epoch: 1,
            actor_id: child.id.clone(),
            run_id: claim.run_id.clone(),
            expires_at: claim.lease_expires_at,
        };
        let mut lease = ActivePlacementLease {
            registry: bamboo_storage::v2::FileHostRegistry::new(temp.path().into())
                .await
                .unwrap(),
            actor_store: store.clone(),
            route: ScopedPlacementRoute {
                endpoint: String::new(),
                token: String::new(),
                ca_cert_file: PathBuf::new(),
                parent: bamboo_subagent::AgentRef {
                    session_id: "dispatch-parent".into(),
                    role: Some("host".into()),
                },
                worker_role: "worker".into(),
                pinned_mailbox: None,
                requirements: Default::default(),
            },
            mailbox: "dispatch-worker".into(),
            host_ref: "dispatch-worker".into(),
            fence: activation.fence(),
            placement_ref: bamboo_domain::ActorPlacementRef {
                class: bamboo_domain::ActorPlacementClass::Remote,
                lease_id: slot.lease_id.clone(),
                slot_epoch: Some(slot.epoch),
            },
            lease: Arc::new(tokio::sync::Mutex::new(slot)),
            heartbeat: tokio::spawn(std::future::pending()),
            run_cancel: CancellationToken::new(),
            lost: Arc::new(AtomicBool::new(false)),
            release_on_drop: AtomicBool::new(false),
        };
        assert!(!lease.abort_if_run_unsent(send_invoked).await.unwrap());
        assert_eq!(
            store
                .inspect_actor(&child.id)
                .await
                .unwrap()
                .activation
                .unwrap()
                .status,
            bamboo_domain::ActorActivationStatus::Running
        );
        let mut retry = claim.clone();
        retry.run_id = "dispatch-retry".into();
        retry.now = chrono::Utc::now();
        retry.lease_expires_at = retry.now + chrono::Duration::seconds(80);
        assert!(matches!(
            store.claim_activation(&retry).await,
            Err(bamboo_domain::ActorDirectoryError::Busy)
        ));
    }

    #[test]
    fn accepted_cancelled_terminal_maps_to_cancelled_actor_activation() {
        let cancelled: Result<Option<String>, AgentError> = Err(AgentError::Cancelled);
        assert_eq!(
            remote_activation_finish(&cancelled, Some(TerminalStatus::Cancelled)),
            ActorActivationFinish::Cancelled
        );
        assert_eq!(
            remote_activation_finish(&cancelled, None),
            ActorActivationFinish::Failed
        );
        assert_eq!(
            remote_activation_finish(&cancelled, Some(TerminalStatus::Completed)),
            ActorActivationFinish::Failed
        );
    }

    #[tokio::test]
    async fn remote_reply_is_fenced_before_terminal_status_save() {
        let temp = tempfile::tempdir().unwrap();
        let store = bamboo_storage::SessionStoreV2::new(temp.path().into())
            .await
            .unwrap();
        let parent = Session::new("remote-reply-parent", "model");
        let mut child = Session::new_child_of("remote-reply-child", &parent, "model", "task");
        child.add_message(bamboo_agent_core::Message::user("task"));
        store.save_session(&parent).await.unwrap();
        store.save_session(&child).await.unwrap();
        store.ensure_actor(&child.id).await.unwrap();
        store
            .bind_control_plane(
                &child.id,
                bamboo_domain::ActorControlPlaneBinding {
                    policy_revision: None,
                    placement_intent: Some(bamboo_domain::ActorPlacementIntent::Pool {
                        pool_id: "remote-worker".into(),
                    }),
                },
            )
            .await
            .unwrap();
        let now = chrono::Utc::now();
        let activation = store
            .claim_activation(&ActorActivationClaim {
                actor_id: child.id.clone(),
                run_id: "remote-reply-run".into(),
                lease_owner: "remote-worker".into(),
                lease_expires_at: now + chrono::Duration::seconds(80),
                inbox_generation: 0,
                placement_ref: Some(bamboo_domain::ActorPlacementRef {
                    class: bamboo_domain::ActorPlacementClass::Remote,
                    lease_id: "remote-slot".into(),
                    slot_epoch: Some(1),
                }),
                now,
            })
            .await
            .unwrap();
        let fence = activation.fence();
        store.start_activation(&fence, now).await.unwrap();

        append_fenced_remote_reply(&store, &fence, &mut child, "done")
            .await
            .unwrap();
        store
            .finish_activation(&fence, chrono::Utc::now(), ActorActivationFinish::Succeeded)
            .await
            .unwrap();
        child.set_last_run_status("completed");
        store.save_session(&child).await.unwrap();
        let saved = store.load_session(&child.id).await.unwrap().unwrap();
        assert_eq!(saved.last_run_status().as_deref(), Some("completed"));
        assert_eq!(saved.messages.len(), 2);
        assert_eq!(saved.messages.last().unwrap().content, "done");
        assert!(
            append_fenced_remote_reply(&store, &fence, &mut child, "duplicate")
                .await
                .is_err()
        );
        assert_eq!(
            store
                .load_session(&child.id)
                .await
                .unwrap()
                .unwrap()
                .messages
                .len(),
            2
        );
    }

    async fn start_bus() -> (String, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let core = Arc::new(bamboo_broker::BrokerCore::new(dir.path()));
        let server = Arc::new(bamboo_broker::BrokerServer::new(core, "t"));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        (format!("ws://{addr}"), dir)
    }

    struct DurableAckProbe {
        calls: Arc<AtomicUsize>,
        terminal_status: TerminalStatus,
        parent_mailbox: String,
    }

    #[async_trait]
    impl bamboo_subagent::ChildLink for DurableAckProbe {
        async fn send(&mut self, _frame: ParentFrame) -> bamboo_subagent::TransportResult<()> {
            Ok(())
        }

        async fn next_frame(&mut self) -> bamboo_subagent::TransportResult<Option<ChildFrame>> {
            Ok(None)
        }

        fn has_pending_durable_terminal(&self) -> bool {
            true
        }

        fn durable_delivery_receipt(&self) -> Option<bamboo_subagent::DurableChildDeliveryReceipt> {
            Some(bamboo_subagent::DurableChildDeliveryReceipt {
                broker_identity: "00000000-0000-4000-8000-000000000001".into(),
                parent_mailbox: self.parent_mailbox.clone(),
                correlation_id: "broker-run-1".into(),
                message_ids: vec!["broker-outcome-1".into()],
                terminal_status: self.terminal_status,
            })
        }

        async fn acknowledge_durable_frames(&mut self) -> bamboo_subagent::TransportResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    async fn insert_ack_probe(
        runner: &ActorChildRunner,
        session: &Session,
        calls: &Arc<AtomicUsize>,
        terminal_status: TerminalStatus,
    ) {
        insert_ack_probe_with_nested_wait(runner, session, calls, terminal_status, false).await;
    }

    async fn insert_ack_probe_with_nested_wait(
        runner: &ActorChildRunner,
        session: &Session,
        calls: &Arc<AtomicUsize>,
        terminal_status: TerminalStatus,
        host_nested_wait_handoff: bool,
    ) {
        runner.pending_durable_links.lock().await.insert(
            (session.id.clone(), session.created_at, "run-1".into()),
            PendingDurableChildLink {
                link: Box::new(DurableAckProbe {
                    calls: calls.clone(),
                    terminal_status,
                    parent_mailbox: format!("p-{}", session.id),
                }),
                host_nested_wait_handoff,
            },
        );
    }

    #[tokio::test]
    async fn broker_receipt_reconciler_repairs_only_the_proven_local_mailbox() {
        let (endpoint, broker_dir) = start_bus().await;
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let root = Session::new("receipt-root", "model");
        store.save_session(&root).await.unwrap();
        let mut sender = bamboo_broker::BrokerClient::connect(
            &endpoint,
            bamboo_subagent::AgentRef {
                session_id: "receipt-worker".into(),
                role: None,
            },
            "t",
        )
        .await
        .unwrap();
        let broker_identity = sender.durable_broker_identity().await.unwrap();
        let wrong_broker_identity = if broker_identity == "00000000-0000-4000-8000-000000000001" {
            "00000000-0000-4000-8000-000000000002"
        } else {
            "00000000-0000-4000-8000-000000000001"
        };

        for (id, receipt_mailbox, receipt_broker) in [
            ("receipt-good", "p-receipt-good", broker_identity.as_str()),
            (
                "receipt-wrong-mailbox",
                "p-another-child",
                broker_identity.as_str(),
            ),
            (
                "receipt-wrong-broker",
                "p-receipt-wrong-broker",
                wrong_broker_identity,
            ),
            (
                "receipt-local-role-changed",
                "p-receipt-local-role-changed",
                broker_identity.as_str(),
            ),
            (
                "receipt-remote-unproven",
                "remote-parent-unproven",
                broker_identity.as_str(),
            ),
        ] {
            let mut child = Session::new_child_of(id, &root, "model", "task");
            child
                .metadata
                .insert("runtime.kind".into(), "external".into());
            child
                .metadata
                .insert("external.protocol".into(), "actor".into());
            child.metadata.insert(
                "external.agent_id".into(),
                crate::external_agents::config::LOCAL_ACTOR_AGENT_ID.into(),
            );
            if id == "receipt-local-role-changed" || id == "receipt-remote-unproven" {
                child
                    .metadata
                    .insert("subagent_type".into(), "remote-worker".into());
            }
            child.add_message(bamboo_agent_core::Message::user("work"));
            store.save_session(&child).await.unwrap();
            let message_id = bamboo_subagent::MsgId::new();
            sender
                .deliver(
                    &format!("p-{id}"),
                    bamboo_subagent::InboxMessage {
                        id: message_id.clone(),
                        from: bamboo_subagent::AgentRef {
                            session_id: "receipt-worker".into(),
                            role: None,
                        },
                        kind: bamboo_subagent::InboxKind::Outcome,
                        body: serde_json::to_value(bamboo_subagent::ChildOutcome::completed(
                            "done",
                        ))
                        .unwrap(),
                        created_at: chrono::Utc::now(),
                        correlation_id: Some(bamboo_subagent::MsgId::new()),
                    },
                )
                .await
                .unwrap();
            // The Host can checkpoint only a frame already surfaced from the
            // broker, which moves its Maildir file from new/ into cur/.
            let mailbox =
                bamboo_subagent::Mailbox::at(broker_dir.path().join(format!("mailboxes/p-{id}")));
            assert_eq!(mailbox.drain().await.unwrap().len(), 1);
            child.add_message(bamboo_agent_core::Message::assistant("done", None));
            child.set_last_run_status("completed");
            store
                .prepare_broker_terminal_receipt(
                    &child,
                    "run-1",
                    receipt_broker,
                    receipt_mailbox,
                    "correlation-1",
                    &[message_id.as_str().to_owned()],
                )
                .await
                .unwrap();
            store.save_session(&child).await.unwrap();
        }

        let mut config = bamboo_config::Config::default();
        config.subagents_mut().broker = Some(bamboo_config::BrokerClientConfig {
            endpoint,
            token: "t".into(),
            ..Default::default()
        });
        config
            .subagents_mut()
            .remote_placements
            .push(bamboo_config::RemoteActorPlacement {
                role: "remote-worker".into(),
                endpoint: "ws://unscoped.example.invalid".into(),
                token_env: None,
                ca_cert_file: None,
                broker_peer: None,
                placement_requirements: None,
            });
        let repair = BrokerTerminalReceiptReconciler::new(store.clone(), &config);
        let report = repair.reconcile_once().await;
        assert_eq!(report.candidates, 5);
        assert_eq!(report.repaired, 2);
        assert_eq!(report.blocked, 3);

        let good = bamboo_subagent::Mailbox::at(broker_dir.path().join("mailboxes/p-receipt-good"));
        let wrong = bamboo_subagent::Mailbox::at(
            broker_dir.path().join("mailboxes/p-receipt-wrong-mailbox"),
        );
        let wrong_broker = bamboo_subagent::Mailbox::at(
            broker_dir.path().join("mailboxes/p-receipt-wrong-broker"),
        );
        let local_role_changed = bamboo_subagent::Mailbox::at(
            broker_dir
                .path()
                .join("mailboxes/p-receipt-local-role-changed"),
        );
        let remote_unproven = bamboo_subagent::Mailbox::at(
            broker_dir
                .path()
                .join("mailboxes/p-receipt-remote-unproven"),
        );
        assert_eq!(good.pending_count().await.unwrap(), 0);
        assert_eq!(wrong.pending_count().await.unwrap(), 1);
        assert_eq!(wrong_broker.pending_count().await.unwrap(), 1);
        assert_eq!(local_role_changed.pending_count().await.unwrap(), 0);
        assert_eq!(remote_unproven.pending_count().await.unwrap(), 1);
        let good_child = store.load_session("receipt-good").await.unwrap().unwrap();
        let wrong_child = store
            .load_session("receipt-wrong-mailbox")
            .await
            .unwrap()
            .unwrap();
        let wrong_broker_child = store
            .load_session("receipt-wrong-broker")
            .await
            .unwrap()
            .unwrap();
        let local_role_changed_child = store
            .load_session("receipt-local-role-changed")
            .await
            .unwrap()
            .unwrap();
        let remote_unproven_child = store
            .load_session("receipt-remote-unproven")
            .await
            .unwrap()
            .unwrap();
        assert!(store
            .recover_broker_terminal_receipts(&good_child)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store
                .recover_broker_terminal_receipts(&wrong_child)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .recover_broker_terminal_receipts(&wrong_broker_child)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(store
            .recover_broker_terminal_receipts(&local_role_changed_child)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store
                .recover_broker_terminal_receipts(&remote_unproven_child)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn broker_receipt_confirmation_requires_exact_birth_run_and_committed_status() {
        let runner = ActorChildRunner::new(
            "test".into(),
            PathBuf::new(),
            vec![],
            PathBuf::new(),
            ExecutorSpec::BambooRuntime,
            vec![],
            "test".into(),
            1,
        );
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let root = Session::new("ack-parent", "model");
        store.save_session(&root).await.unwrap();
        let mut child = Session::new_child_of("ack-child", &root, "model", "task");
        child.add_message(bamboo_agent_core::Message::user("work"));
        store.save_session(&child).await.unwrap();
        child.add_message(bamboo_agent_core::Message::assistant("done", None));
        child.set_last_run_status("completed");
        store
            .prepare_broker_terminal_receipt(
                &child,
                "run-1",
                "00000000-0000-4000-8000-000000000001",
                "p-ack-child",
                "broker-run-1",
                &["broker-outcome-1".into()],
            )
            .await
            .unwrap();
        store.save_session(&child).await.unwrap();
        runner.set_actor_directory_store(Some(store));
        let calls = Arc::new(AtomicUsize::new(0));
        insert_ack_probe(&runner, &child, &calls, TerminalStatus::Completed).await;
        runner
            .confirm_durable_child_delivery(&child, "wrong-run", true)
            .await
            .unwrap();
        let mut wrong_birth = child.clone();
        wrong_birth.created_at += chrono::Duration::seconds(1);
        runner
            .confirm_durable_child_delivery(&wrong_birth, "run-1", true)
            .await
            .unwrap();
        assert_eq!(runner.pending_durable_links.lock().await.len(), 1);
        child.set_last_run_status("completed");
        runner
            .confirm_durable_child_delivery(&child, "run-1", false)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        insert_ack_probe(&runner, &child, &calls, TerminalStatus::Completed).await;
        child.set_last_run_status("error");
        runner
            .confirm_durable_child_delivery(&child, "run-1", true)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        insert_ack_probe(&runner, &child, &calls, TerminalStatus::Completed).await;
        child.set_last_run_status("completed");
        runner
            .confirm_durable_child_delivery(&child, "run-1", true)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn broker_receipt_confirms_canonical_worker_error_and_cancelled_status() {
        for (terminal_status, status) in [
            (TerminalStatus::Error, "error"),
            (TerminalStatus::Cancelled, "cancelled"),
        ] {
            let runner = ActorChildRunner::new(
                "test".into(),
                PathBuf::new(),
                vec![],
                PathBuf::new(),
                ExecutorSpec::BambooRuntime,
                vec![],
                "test".into(),
                1,
            );
            let home = tempfile::tempdir().unwrap();
            let store = Arc::new(
                bamboo_storage::SessionStoreV2::new(home.path().to_path_buf())
                    .await
                    .unwrap(),
            );
            let root = Session::new("failed-ack-parent", "model");
            store.save_session(&root).await.unwrap();
            let mut child = Session::new_child_of("failed-ack-child", &root, "model", "task");
            child.add_message(bamboo_agent_core::Message::user("work"));
            store.save_session(&child).await.unwrap();
            child.set_last_run_status(status);
            child.set_last_run_error(format!("worker {status}"));
            store
                .prepare_broker_terminal_receipt(
                    &child,
                    "run-1",
                    "00000000-0000-4000-8000-000000000001",
                    "p-failed-ack-child",
                    "broker-run-1",
                    &["broker-outcome-1".into()],
                )
                .await
                .unwrap();
            store.save_session(&child).await.unwrap();
            runner.set_actor_directory_store(Some(store));
            let calls = Arc::new(AtomicUsize::new(0));
            insert_ack_probe(&runner, &child, &calls, terminal_status).await;
            runner
                .confirm_durable_child_delivery(&child, "run-1", true)
                .await
                .unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1, "{status}");
        }
    }

    #[tokio::test]
    async fn broker_completed_terminal_projects_only_host_verified_nested_wait() {
        assert_eq!(
            effective_broker_terminal_status(TerminalStatus::Completed, true).unwrap(),
            "suspended"
        );
        for raw in [TerminalStatus::Error, TerminalStatus::Cancelled] {
            assert!(effective_broker_terminal_status(raw, true).is_err());
        }
        let runner = ActorChildRunner::new(
            "test".into(),
            PathBuf::new(),
            vec![],
            PathBuf::new(),
            ExecutorSpec::BambooRuntime,
            vec![],
            "test".into(),
            1,
        );
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let root = Session::new("nested-ack-parent", "model");
        store.save_session(&root).await.unwrap();
        let mut child = Session::new_child_of("nested-ack-child", &root, "model", "task");
        child.add_message(bamboo_agent_core::Message::user("spawn a grandchild"));
        store.save_session(&child).await.unwrap();
        child.set_last_run_status("suspended");
        store
            .prepare_broker_terminal_receipt(
                &child,
                "run-1",
                "00000000-0000-4000-8000-000000000001",
                "p-nested-ack-child",
                "broker-run-1",
                &["broker-outcome-1".into()],
            )
            .await
            .unwrap();
        store.save_session(&child).await.unwrap();
        runner.set_actor_directory_store(Some(store.clone()));
        let calls = Arc::new(AtomicUsize::new(0));

        insert_ack_probe(&runner, &child, &calls, TerminalStatus::Completed).await;
        runner
            .confirm_durable_child_delivery(&child, "run-1", true)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        insert_ack_probe_with_nested_wait(&runner, &child, &calls, TerminalStatus::Completed, true)
            .await;
        runner
            .confirm_durable_child_delivery(&child, "run-1", true)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store
                .load_session(&child.id)
                .await
                .unwrap()
                .unwrap()
                .last_run_status()
                .as_deref(),
            Some("suspended")
        );
        assert!(store
            .recover_broker_terminal_receipts(&child)
            .await
            .unwrap()
            .is_empty());
    }

    async fn bind_local_control_plane(
        store: &bamboo_storage::SessionStoreV2,
        actor_id: &str,
        policy: &bamboo_tools::permission::PermissionConfig,
    ) {
        store
            .bind_control_plane(
                actor_id,
                bamboo_domain::ActorControlPlaneBinding {
                    policy_revision: Some(policy.policy_revision()),
                    placement_intent: Some(bamboo_domain::ActorPlacementIntent::Local),
                },
            )
            .await
            .unwrap();
    }

    fn local_tool_sample(
        host: &Session,
        denied_write: bool,
    ) -> (LocalToolCollector, Vec<bamboo_agent_core::Message>) {
        use bamboo_agent_core::{FunctionCall, Message, ToolCall};
        let calls = ["Read", "Write"]
            .map(|name| ToolCall {
                id: format!("local-{name}"),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: name.into(),
                    arguments: "{\"file_path\":\"answer.txt\"}".into(),
                },
            })
            .to_vec();
        let mut rows = vec![Message::assistant("checking", Some(calls))];
        let mut collector = LocalToolCollector::default();
        for name in ["Read", "Write"] {
            let denied = name == "Write" && denied_write;
            let id = format!("local-{name}");
            if !denied {
                collector
                    .event(
                        &serde_json::to_value(AgentEvent::ToolStart {
                            tool_call_id: id.clone(),
                            tool_name: name.into(),
                            arguments: serde_json::json!({"file_path":"answer.txt"}),
                        })
                        .unwrap(),
                    )
                    .unwrap();
            }
            let text = if denied {
                "Error: native_tool_ceiling_denied: Write"
            } else {
                "actual file result"
            };
            let mut message = Message::tool_result_with_status(id.clone(), text, !denied);
            message.metadata = Some(serde_json::json!({"elapsed_ms":7,"tool_name":name,
                "is_mutating":name == "Write","auto_approved":name != "Write","success":!denied}));
            rows.push(message);
            let event = if denied {
                AgentEvent::ToolError {
                    tool_call_id: id,
                    error: "native_tool_ceiling_denied: Write".into(),
                }
            } else {
                AgentEvent::ToolComplete {
                    tool_call_id: id,
                    result: bamboo_agent_core::tools::ToolResult::text(true, text),
                }
            };
            collector
                .event(&serde_json::to_value(event).unwrap())
                .unwrap();
        }
        rows.push(Message::assistant("complete exact report", None));
        let mut messages = host.messages.clone();
        if let Some(system) = messages
            .iter_mut()
            .find(|message| message.role == Role::System)
        {
            system.content = "worker-private system never imported".into();
        }
        messages.extend(rows.iter().cloned());
        let data = bamboo_subagent::proto::LocalToolMessages::Complete {
            version: 1,
            messages: messages
                .iter()
                .map(|message| serde_json::to_value(message).unwrap())
                .collect(),
        };
        assert!(!collector
            .event(&serde_json::to_value(data).unwrap())
            .unwrap());
        (collector, rows)
    }

    #[test]
    fn local_tool_suffix_preserves_full_current_prefix_and_rejects_authority_or_collisions() {
        let mut host = Session::new("tool-history-prefix", "model");
        host.messages
            .push(bamboo_agent_core::Message::user("assignment"));
        let mut confirmed = bamboo_agent_core::Message::user("confirmed during Run");
        confirmed.metadata = Some(
            serde_json::json!({"session_message":{"id":confirmed.id,"target_session_id":host.id}}),
        );
        host.messages.push(confirmed);
        let tools = vec!["Read".into(), "Write".into()];
        let (good, rows) = local_tool_sample(&host, false);
        assert_eq!(
            serde_json::to_value(
                good.suffix(&host, &tools, false, Some("complete exact report"))
                    .unwrap()
            )
            .unwrap(),
            serde_json::to_value(&rows).unwrap()
        );
        for case in [
            "missing",
            "changed",
            "metadata",
            "system",
            "user",
            "collision",
            "dangling",
            "result",
            "terminal",
            "tool_metadata",
            "grant",
            "start_arguments",
            "ledger",
        ] {
            let (mut collector, _) = local_tool_sample(&host, false);
            let mut current = host.clone();
            let messages = collector.messages.as_mut().unwrap();
            match case {
                "missing" => {
                    messages.remove(host.messages.len() - 1);
                }
                "changed" => messages[0].content.push('!'),
                "metadata" => messages[1].metadata = None,
                "system" => messages.last_mut().unwrap().role = Role::System,
                "user" => messages.last_mut().unwrap().role = Role::User,
                "collision" => messages.last_mut().unwrap().id = host.messages[0].id.clone(),
                "dangling" => {
                    messages[host.messages.len() + 1].tool_call_id = Some("foreign".into())
                }
                "result" => messages[host.messages.len() + 1].content.push('!'),
                "tool_metadata" => {
                    messages[host.messages.len() + 1].metadata.as_mut().unwrap()["permission"] =
                        true.into()
                }
                "grant" => {
                    messages[host.messages.len()].tool_calls.as_mut().unwrap()[0]
                        .function
                        .name = "Bash".into()
                }
                "start_arguments" => {
                    collector.starts.get_mut("local-Read").unwrap().1 =
                        serde_json::json!({"file_path":"different.txt"})
                }
                "ledger" => {
                    current.append_provider_transcript_group(current.messages[0].id.clone(), None, vec![
                        bamboo_domain::session::provider_transcript::ProviderTranscriptItem::try_from_payload(
                            bamboo_domain::ProviderFamily::OpenAi,
                            bamboo_domain::ProviderProtocol::OpenAiResponsesV1,
                            bamboo_domain::session::provider_transcript::ProviderTranscriptOrigin::HostToolSearch,
                            bamboo_domain::session::provider_transcript::ProviderTranscriptAuthor::ToolResult,
                            serde_json::json!({"type":"tool_search_output","execution":"client","call_id":"search","status":"completed","tools":[]}),
                        ).unwrap(),
                    ]).unwrap();
                }
                "terminal" => {}
                _ => unreachable!(),
            }
            let terminal = if case == "terminal" {
                "different"
            } else {
                "complete exact report"
            };
            assert!(
                collector
                    .suffix(&current, &tools, false, Some(terminal))
                    .is_err(),
                "{case}"
            );
        }
        let (denied, _) = local_tool_sample(&host, true);
        assert!(denied
            .suffix(&host, &["Read".into()], true, Some("complete exact report"))
            .is_ok());
        let (mut contradicted, _) = local_tool_sample(&host, true);
        contradicted.starts.insert(
            "local-Write".into(),
            ("Read".into(), serde_json::json!({"file_path":"answer.txt"})),
        );
        assert!(contradicted
            .suffix(&host, &["Read".into()], true, Some("complete exact report"))
            .is_err());
        let (successful, _) = local_tool_sample(&host, false);
        assert!(successful
            .suffix(&host, &["Read".into()], true, Some("complete exact report"))
            .is_err());
        let mut empty = LocalToolCollector::default();
        assert!(empty
            .event(
                &serde_json::to_value(AgentEvent::ReasoningToken {
                    content: "private".into()
                })
                .unwrap()
            )
            .is_err());
        let (mut completed, _) = local_tool_sample(&host, false);
        assert!(completed.event(&serde_json::json!({"type":"local_client_tool_messages_v1","version":1,"messages":[]})).is_err());
    }

    struct LocalToolConcurrentCheckpoint {
        real: SessionInboxRuntimeBinding,
        claim: SessionInboxClaim,
    }
    #[async_trait]
    impl RuntimeSessionPersistence for LocalToolConcurrentCheckpoint {
        async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            self.real.persistence.save_runtime_session(session).await
        }
        async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            // A real canonical admission wins after validation, before the
            // final append-safe checkpoint acquires its serialization lock.
            let mut latest = self.real.storage.load_session(&session.id).await?.unwrap();
            checkpoint_and_ack_canonical_claim(&self.real, &mut latest, &self.claim, None)
                .await
                .map_err(|_| std::io::Error::other("concurrent admission failed"))?;
            self.real
                .persistence
                .checkpoint_runtime_session(session)
                .await
        }
        async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
            self.real.storage.load_session(id).await
        }
    }

    #[tokio::test]
    async fn local_tool_history_checkpoint_preserves_confirmed_input_and_cold_pairs_or_refuses() {
        for case in [
            "success",
            "concurrent_input",
            "checkpoint_error",
            "stale_prefix",
            "cancelled",
            "lost_owner",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let store = Arc::new(
                bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                    .await
                    .unwrap(),
            );
            let root = Session::new("local-tool-root", "model");
            store.save_session(&root).await.unwrap();
            let mut child = Session::new_child("local-tool-child", &root.id, "model", "child");
            let mut system = bamboo_agent_core::Message::user("Host-owned immutable System");
            system.role = Role::System;
            child.messages.push(system);
            child
                .messages
                .push(bamboo_agent_core::Message::user("assignment"));
            store.save_session(&child).await.unwrap();
            let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
                store.clone(),
                bamboo_domain::SessionInboxLimits::default(),
            ));
            let locked = Arc::new(bamboo_storage::LockedSessionStore::new(store.clone()));
            let binding = actor_binding(store.clone(), inbox.clone(), locked.clone());
            let mut owner = Some(
                binding
                    .router
                    .register_run(&child.id, "current-run")
                    .await
                    .unwrap(),
            );
            let envelope = bamboo_domain::SessionMessageEnvelope::user_input(
                &child.id,
                "current confirmed User",
            );
            let receipt = inbox.deliver(&envelope).await.unwrap();
            inbox
                .mark_activation_eligible(
                    &child.id,
                    receipt.generation,
                    bamboo_domain::SessionActivationPolicy::RespectSpecificWait,
                )
                .await
                .unwrap();
            let claim = inbox.claim(&child.id, 1).await.unwrap().remove(0);
            checkpoint_and_ack_canonical_claim(&binding, &mut child, &claim, None)
                .await
                .unwrap();
            assert!(inbox.was_admitted(&child.id, &envelope.id).await.unwrap());
            let before = serde_json::to_value(&child).unwrap();
            let (collector, rows) = local_tool_sample(&child, false);
            let mut binding = binding;
            let late = bamboo_domain::SessionMessageEnvelope::user_input(
                &child.id,
                "concurrent confirmed User",
            );
            if case == "concurrent_input" {
                let receipt = inbox.deliver(&late).await.unwrap();
                inbox
                    .mark_activation_eligible(
                        &child.id,
                        receipt.generation,
                        bamboo_domain::SessionActivationPolicy::RespectSpecificWait,
                    )
                    .await
                    .unwrap();
                let late_claim = inbox.claim(&child.id, 1).await.unwrap().remove(0);
                binding.persistence = Arc::new(LocalToolConcurrentCheckpoint {
                    real: actor_binding(store.clone(), inbox.clone(), locked.clone()),
                    claim: late_claim,
                });
            }
            if case == "checkpoint_error" {
                binding.persistence = Arc::new(ActorFaultingPersistence {
                    inner: locked.clone(),
                    fail_checkpoint_once: AtomicBool::new(true),
                });
            }
            if case == "stale_prefix" {
                let mut latest = child.clone();
                latest
                    .messages
                    .push(bamboo_agent_core::Message::user("new confirmed prefix"));
                store.save_session(&latest).await.unwrap();
            }
            let cancel = CancellationToken::new();
            if case == "cancelled" {
                cancel.cancel();
            }
            if case == "lost_owner" {
                owner.take().unwrap().abandon().await;
            }
            let durable_before =
                serde_json::to_value(store.load_session(&child.id).await.unwrap()).unwrap();
            let result = commit_local_tool_history(
                collector,
                &mut child,
                &binding,
                "current-run",
                (&["Read".into(), "Write".into()], false),
                Some("complete exact report"),
                &cancel,
            )
            .await;
            let cold = bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap()
                .load_session(&child.id)
                .await
                .unwrap()
                .unwrap();
            if !matches!(case, "success" | "concurrent_input") {
                assert!(result.is_err(), "{case}");
                assert_eq!(
                    serde_json::to_value(&child).unwrap(),
                    before,
                    "{case}: no partial mutable success"
                );
                assert_eq!(
                    serde_json::to_value(Some(&cold)).unwrap(),
                    durable_before,
                    "{case}: no write"
                );
                continue;
            }
            result.unwrap();
            assert_eq!(
                serde_json::to_value(&cold.messages[..3]).unwrap(),
                before["messages"]
            );
            let suffix_start = if case == "concurrent_input" {
                assert!(inbox.was_admitted(&child.id, &late.id).await.unwrap());
                assert_eq!(
                    serde_json::to_value(&cold.messages[3]).unwrap(),
                    serde_json::to_value(late.to_provider_message().unwrap()).unwrap()
                );
                assert!(child
                    .messages
                    .iter()
                    .any(|message| message.id == late.id.as_str()));
                4
            } else {
                3
            };
            assert_eq!(
                serde_json::to_value(&cold.messages[suffix_start..]).unwrap(),
                serde_json::to_value(rows).unwrap()
            );
            assert_eq!(
                cold.messages
                    .iter()
                    .filter(|message| message.id == envelope.id.as_str())
                    .count(),
                1
            );
            assert_eq!(
                cold.messages
                    .iter()
                    .filter(|message| message.content == "complete exact report")
                    .count(),
                1
            );
            assert_eq!(cold.metadata, child.metadata);
            assert!(cold.provider_transcript.is_empty());
        }
    }

    #[tokio::test]
    async fn local_tool_completion_requires_current_ordered_batch_and_never_falls_back_to_text() {
        for case in [
            "legacy",
            "missing",
            "gap",
            "duplicate",
            "epoch",
            "source",
            "birth",
            "qos",
        ] {
            let mut session = Session::new_child("frame-child", "frame-root", "model", "child");
            session
                .messages
                .push(bamboo_agent_core::Message::user("original"));
            let before = serde_json::to_value(&session).unwrap();
            let creation = bamboo_subagent::proto::ChildCreationIdentity {
                created_at: session.created_at,
                spawn_depth: session.spawn_depth,
            };
            let mut messages = session.messages.clone();
            messages.push(bamboo_agent_core::Message::assistant("complete", None));
            let event = serde_json::to_value(bamboo_subagent::proto::LocalToolMessages::Complete {
                version: 1,
                messages: messages
                    .iter()
                    .map(|message| serde_json::to_value(message).unwrap())
                    .collect(),
            })
            .unwrap();
            let mut batch = ActorEventBatch {
                logical_session: Some(LogicalSessionIdentity {
                    session_id: session.id.clone(),
                    parent_session_id: session.parent_session_id.clone(),
                    root_session_id: session.root_session_id.clone(),
                    creation: Some(creation.clone()),
                }),
                activation_id: Some("current".into()),
                execution_epoch: 7,
                source_node_id: None,
                source_actor_id: Some("selected".into()),
                first_seq: 1,
                last_seq: 1,
                qos: bamboo_subagent::proto::ActorEventQos::Durable,
                events: vec![event.clone()],
            };
            match case {
                "gap" => {
                    batch.first_seq = 2;
                    batch.last_seq = 2;
                }
                "epoch" => batch.execution_epoch = 6,
                "source" => batch.source_actor_id = Some("replaced".into()),
                "birth" => {
                    batch
                        .logical_session
                        .as_mut()
                        .unwrap()
                        .creation
                        .as_mut()
                        .unwrap()
                        .created_at += chrono::Duration::nanoseconds(1)
                }
                "qos" => batch.qos = bamboo_subagent::proto::ActorEventQos::Ephemeral,
                _ => {}
            }
            let terminal = ChildFrame::Terminal {
                status: TerminalStatus::Completed,
                result: Some("complete".into()),
                error: None,
                transcript: vec![],
            };
            let frames = match case {
                "legacy" => vec![ChildFrame::Event { event }, terminal],
                "missing" => vec![terminal],
                "duplicate" => vec![
                    ChildFrame::EventBatch {
                        batch: batch.clone(),
                    },
                    ChildFrame::EventBatch { batch },
                    terminal,
                ],
                _ => vec![ChildFrame::EventBatch { batch }, terminal],
            };
            let mut link = ConfirmationSequenceLink {
                frames: frames.into(),
                sent: vec![],
            };
            let (tx, mut rx) = mpsc::channel(32);
            let (_live, mut live_rx) = mpsc::unbounded_channel();
            let (_delivery, mut delivery_rx) = mpsc::unbounded_channel();
            let cancel = CancellationToken::new();
            let tools = vec!["Read".into()];
            let outcome = drive(ActorDriveContext {
                client: &mut link,
                parent_session_id: "frame-root",
                child_session_id: "frame-child",
                child_attempt: 0,
                approval_registry: None,
                approval_decider: None,
                approval_reviewer: None,
                escalation_bridge: None,
                event_tx: &tx,
                cancel_token: &cancel,
                live_rx: &mut live_rx,
                delivery_rx: &mut delivery_rx,
                logical_session: &mut session,
                expected_permission_posture: None,
                expected_creation: Some(&creation),
                session_inbox_runtime: None,
                actor_directory_store: None,
                canonical_subagent_tool: None,
                activation_run_id: Some("current"),
                execution_epoch: 7,
                expected_source_actor_id: "selected",
                initial_inflight_claims: vec![].into(),
                plain_actor: false,
                remote_environment_lease: false,
                readonly_output: None,
                local_history_tools: Some(&tools),
                local_history_read_only: true,
                plain_input: None,
                canonical_activation: None,
                canonical_placement_ref: None,
                actor_event_observer: None,
                plain_run: None,
                first_frame_timeout: Some(Duration::from_secs(1)),
            })
            .await;
            assert!(outcome.is_err(), "{case}: no last-text success fallback");
            assert_eq!(serde_json::to_value(&session).unwrap(), before, "{case}");
            assert!(link.sent.is_empty());
            assert!(rx.try_recv().is_err(), "DATA must never enter public feed");
        }
    }

    fn processless_pool_worker(mailbox_id: &str) -> PooledWorker {
        PooledWorker {
            worker: SpawnedChild::remote(AgentRecord {
                agent_id: mailbox_id.to_string(),
                role: "test".to_string(),
                labels: Vec::new(),
                endpoint: "ws://127.0.0.1:1".to_string(),
                pid: 0,
                version: String::new(),
                started_at: chrono::Utc::now(),
                lease_expires_at: chrono::Utc::now(),
            }),
            mailbox_id: mailbox_id.to_string(),
            parked_at: None,
        }
    }

    #[test]
    fn parked_worker_deadline_is_inclusive_and_ignores_checked_out_workers() {
        let now = Instant::now();
        let timeout = Duration::from_secs(300);
        assert!(!parked_worker_expired(None, now, timeout));
        assert!(!parked_worker_expired(
            Some(now - timeout + Duration::from_millis(1)),
            now,
            timeout
        ));
        assert!(parked_worker_expired(Some(now - timeout), now, timeout));
    }

    #[test]
    fn idle_pool_global_cap_bounds_distinct_fingerprints() {
        let mut pool = WorkerPool::new();
        pool.insert(
            "fingerprint-a".to_string(),
            vec![processless_pool_worker("a")],
        );
        pool.insert(
            "fingerprint-b".to_string(),
            vec![processless_pool_worker("b")],
        );

        assert_eq!(
            idle_pool_limit_reason(&pool, "fingerprint-c", 4, 2),
            Some("pool_global_limit")
        );
        assert_eq!(idle_pool_limit_reason(&pool, "fingerprint-c", 4, 3), None);
        assert_eq!(
            idle_pool_limit_reason(&pool, "fingerprint-a", 1, 8),
            Some("pool_per_key_limit")
        );
    }

    #[test]
    fn pool_sweep_removes_dead_workers_and_empty_fingerprint_buckets() {
        let mut pool = WorkerPool::new();
        pool.insert(
            "fingerprint-a".to_string(),
            vec![processless_pool_worker("dead")],
        );

        let reaped = take_reapable_workers(
            &mut pool,
            Instant::now(),
            Duration::from_secs(POOLED_IDLE_TIMEOUT_SECS),
        );

        assert!(pool.is_empty());
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].1, PoolReapReason::ProcessExited);
    }

    #[test]
    fn actor_preflight_counts_only_current_session_scoped_denies() {
        let config = bamboo_tools::permission::PermissionConfig::new();
        let secret_matcher = "TOP_SECRET_ACTOR_DENY_MATCHER";
        config.deny_scoped_session_permission(
            "target-session",
            bamboo_tools::permission::PermissionType::ExecuteCommand,
            secret_matcher,
        );
        config.deny_scoped_session_permission(
            "other-session",
            bamboo_tools::permission::PermissionType::WriteFile,
            "/other/**",
        );

        assert_eq!(
            active_scoped_session_deny_count(&config, "target-session"),
            1
        );
        assert_eq!(
            active_scoped_session_deny_count(&config, "clean-session"),
            0
        );
        let error = ensure_no_active_scoped_session_denies(&config, "target-session")
            .unwrap_err()
            .to_string();
        assert!(!error.contains(secret_matcher));
        ensure_no_active_scoped_session_denies(&config, "clean-session")
            .expect("another session's deny must not block this activation");
    }

    #[test]
    fn permission_posture_event_rejects_oversized_executor_mapping() {
        let session = Session::new("mapping-bound", "model");
        let event = AgentEvent::PermissionPostureActivated {
            session_id: session.id.clone(),
            policy_revision: 1,
            requested_mode: "default".to_string(),
            effective_mode: "default".to_string(),
            executor_mapping: "x".repeat(bamboo_domain::MAX_PERMISSION_EXECUTOR_MAPPING_CHARS + 1),
        };

        assert!(permission_posture_seed_from_event(&session, &event)
            .unwrap_err()
            .contains("executor mapping"));
    }

    #[test]
    fn remote_audit_revision_and_timestamp_fields_cannot_poison_host_audit() {
        let mut session = Session::new("host-resigns-audit", "model");
        let hostile_timestamp = "9".repeat(1024);
        let event: AgentEvent = serde_json::from_value(serde_json::json!({
            "type": "permission_posture_activated",
            "session_id": session.id.clone(),
            "policy_revision": 31,
            "requested_mode": "default",
            "effective_mode": "default",
            "executor_mapping": "codex_exec:approval_policy=never",
            "audit_revision": u64::MAX,
            "transitioned_at": hostile_timestamp,
        }))
        .expect("unknown remote audit fields are ignored by the typed event");
        let seed = permission_posture_seed_from_event(&session, &event)
            .unwrap()
            .expect("permission event");

        let host_revision =
            bamboo_domain::record_permission_audit(&mut session.metadata, &seed, None).unwrap();
        let host_audit = bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata)
            .expect("host-generated complete audit");
        assert_eq!(host_audit.audit_revision, host_revision);
        assert!(host_audit.audit_revision < bamboo_domain::MAX_PERMISSION_AUDIT_REVISION);
        assert_ne!(host_audit.transitioned_at, hostile_timestamp);
        assert!(chrono::DateTime::parse_from_rfc3339(&host_audit.transitioned_at).is_ok());
    }

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
            parent_question_lock: None,
        }
    }

    #[tokio::test]
    async fn remote_claim_seed_rejects_unsupported_inbox_before_dispatch() {
        let (_temp, store, locked, inbox, mut session, claim) =
            actor_inbox_fixture("unsupported-remote-inbox").await;
        let wrapped: Arc<dyn SessionInboxPort> = Arc::new(ActorFailBeforeAckInbox {
            inner: inbox.clone(),
            fail_once: std::sync::atomic::AtomicBool::new(false),
        });
        let binding = actor_binding(store.clone(), wrapped, locked);
        let fence = ActorActivationFence {
            schema_version: 1,
            actor_id: session.id.clone(),
            activation_id: "unsupported-inbox-activation".into(),
            attempt: 1,
            run_id: "unsupported-inbox-run".into(),
            lease_owner: "unsupported-inbox-host".into(),
            lease_epoch: 1,
        };
        let before = session.clone();

        let error = checkpoint_claim_context_before_dispatch(
            &binding,
            &mut session,
            &[claim],
            Some((store.as_ref(), &fence)),
        )
        .await
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("requires canonical FileSessionInbox"));
        assert_eq!(
            serde_json::to_value(&session.messages).unwrap(),
            serde_json::to_value(&before.messages).unwrap()
        );
        assert_eq!(
            serde_json::to_value(
                &store
                    .load_session(&session.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .messages
            )
            .unwrap(),
            serde_json::to_value(&before.messages).unwrap()
        );
        assert_eq!(inbox.inspect(&session.id).await.unwrap().claimed, 1);
    }

    #[tokio::test]
    async fn remote_broker_lineage_rejects_missing_parent_and_project_drift() {
        let (_temp, store, locked, inbox, parent, _claim) =
            actor_inbox_fixture("remote-parent").await;
        let child = Session::new_child_of("remote-child", &parent, "model", "child");
        store.save_session(&child).await.unwrap();
        let binding = actor_binding(store.clone(), inbox, locked);
        let first = remote_canonical_lineage(&binding, &child).await.unwrap();
        assert_eq!(first.len(), 2);
        let mut changed = child.clone();
        changed.set_project_id_meta("different-project");
        assert!(remote_canonical_lineage(&binding, &changed).await.is_err());
        store.delete_session(&parent.id).await.unwrap();
        assert!(remote_canonical_lineage(&binding, &child).await.is_err());
    }

    #[tokio::test]
    async fn child_creation_requires_canonical_host_lifetime_not_cached_candidate() {
        let (_temp, store, locked, inbox, parent, _claim) =
            actor_inbox_fixture("creation-parent").await;
        let mut child = Session::new_child_of("creation-child", &parent, "model", "child");
        child.set_project_id_meta("creation-project");
        store.save_session(&child).await.unwrap();
        let binding = actor_binding(store.clone(), inbox, locked);
        let job = SpawnJob {
            parent_session_id: parent.id.clone(),
            child_session_id: child.id.clone(),
            model: "model".into(),
            disabled_tools: None,
        };
        let creation = canonical_child_creation(&binding, &child, &job)
            .await
            .unwrap();
        assert_eq!(creation.created_at, child.created_at);
        assert_eq!(creation.spawn_depth, child.spawn_depth);
        for damage in 0..7 {
            let mut held = child.clone();
            match damage {
                0 => held.created_at += chrono::Duration::nanoseconds(1),
                1 => held.parent_session_id = Some("foreign-parent".into()),
                2 => held.root_session_id = "foreign-root".into(),
                3 => held.spawn_depth += 1,
                4 => held.set_project_id_meta("foreign-project"),
                5 => held.kind = bamboo_domain::SessionKind::Root,
                _ => held.id = " creation-child ".into(),
            }
            assert!(
                canonical_child_creation(&binding, &held, &job)
                    .await
                    .is_err(),
                "damage {damage}"
            );
        }
        store.delete_session(&child.id).await.unwrap();
        assert!(canonical_child_creation(&binding, &child, &job)
            .await
            .is_err());
        let mut replacement = Session::new_child_of(&child.id, &parent, "model", "replacement");
        replacement.set_project_id_meta("creation-project");
        assert_ne!(replacement.created_at, child.created_at);
        store.save_session(&replacement).await.unwrap();
        assert!(
            canonical_child_creation(&binding, &child, &job)
                .await
                .is_err(),
            "old held spec cannot adopt replacement birth"
        );
        canonical_child_creation(&binding, &replacement, &job)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn actor_immediate_claims_keep_exact_policy_across_mixed_order_ack_and_restart() {
        use bamboo_domain::{SessionActivationPolicy, SessionInboxLimits, SessionMessageEnvelope};
        for policies in [
            [
                SessionActivationPolicy::RespectSpecificWait,
                SessionActivationPolicy::InterruptSpecificWait,
            ],
            [
                SessionActivationPolicy::InterruptSpecificWait,
                SessionActivationPolicy::RespectSpecificWait,
            ],
        ] {
            let temp = tempfile::tempdir().unwrap();
            let store = Arc::new(
                bamboo_storage::SessionStoreV2::new(temp.path().into())
                    .await
                    .unwrap(),
            );
            let session_id = "actor-mixed-intent-policy";
            store
                .save_session(&Session::new(session_id, "model"))
                .await
                .unwrap();
            let inbox = bamboo_storage::FileSessionInbox::new(store, SessionInboxLimits::default());
            let staged = SessionMessageEnvelope::user_input(session_id, "staged sibling");
            let staged_receipt = inbox.deliver(&staged).await.unwrap();
            let mut sent = Vec::new();
            for policy in policies {
                let envelope = SessionMessageEnvelope::user_input(session_id, "immediate");
                let receipt = inbox
                    .deliver_with_activation_intent(&envelope, policy, None)
                    .await
                    .unwrap();
                sent.push((envelope, policy, receipt));
            }
            let restarted = Arc::new(
                bamboo_storage::SessionStoreV2::new(temp.path().into())
                    .await
                    .unwrap(),
            );
            let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
                restarted.clone(),
                SessionInboxLimits::default(),
            ));
            let binding = actor_binding(
                restarted.clone(),
                inbox.clone(),
                Arc::new(bamboo_storage::LockedSessionStore::new(restarted.clone())),
            );
            let mut session = restarted.load_session(session_id).await.unwrap().unwrap();
            let pairs =
                claim_canonical_deliveries(&binding, &mut session, "run-1", usize::MAX, None)
                    .await
                    .unwrap();
            assert_eq!(pairs.len(), 2, "the staged sibling has no permission");
            for ((claim, delivery), (envelope, policy, receipt)) in pairs.iter().zip(&sent) {
                assert_eq!(claim.envelope.id, envelope.id);
                assert_eq!(claim.generation, receipt.generation);
                assert_eq!(claim.activation_policy, *policy);
                assert_eq!(delivery.activation_policy, *policy);
                checkpoint_and_ack_canonical_claim(&binding, &mut session, claim, None)
                    .await
                    .unwrap();
            }
            let restarted = Arc::new(
                bamboo_storage::SessionStoreV2::new(temp.path().into())
                    .await
                    .unwrap(),
            );
            let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
                restarted.clone(),
                SessionInboxLimits::default(),
            ));
            let binding = actor_binding(
                restarted.clone(),
                inbox.clone(),
                Arc::new(bamboo_storage::LockedSessionStore::new(restarted.clone())),
            );
            session = restarted.load_session(session_id).await.unwrap().unwrap();
            for (envelope, policy, receipt) in &sent {
                assert_eq!(
                    inbox
                        .deliver_with_activation_intent(envelope, *policy, None)
                        .await
                        .unwrap(),
                    *receipt
                );
            }
            assert!(
                claim_canonical_deliveries(&binding, &mut session, "run-2", usize::MAX, None)
                    .await
                    .unwrap()
                    .is_empty()
            );
            inbox
                .mark_activation_eligible(
                    session_id,
                    staged_receipt.generation,
                    SessionActivationPolicy::RespectSpecificWait,
                )
                .await
                .unwrap();
            let pairs =
                claim_canonical_deliveries(&binding, &mut session, "run-2", usize::MAX, None)
                    .await
                    .unwrap();
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs[0].0.envelope.id, staged.id);
            assert_eq!(
                pairs[0].1.activation_policy,
                SessionActivationPolicy::RespectSpecificWait
            );
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
            checkpoint_and_ack_canonical_claim(&binding, &mut session, &claim, None)
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
            checkpoint_and_ack_canonical_claim(&binding, &mut session, &claim, None)
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
            checkpoint_and_ack_canonical_claim(&binding, &mut session, &claim, None)
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
            checkpoint_and_ack_canonical_claim(&binding, &mut session, &claim, None)
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
        checkpoint_and_ack_canonical_claim(&binding, &mut restarted, &recovered, None)
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
            checkpoint_and_ack_canonical_claim(&binding, &mut session, &claim, None)
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
        checkpoint_and_ack_canonical_claim(&binding, &mut restarted, &recovered, None)
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

    #[tokio::test]
    async fn failed_actor_retry_checkpoints_new_input_and_rejects_old_or_stale_state() {
        for case in [
            "accepted",
            "readonly-refusal",
            "prefix-refusal",
            "old-claim",
            "old-checkpoint",
            "succeeded",
            "changed-birth",
            "oversize",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let store = Arc::new(
                bamboo_storage::SessionStoreV2::new(temp.path().into())
                    .await
                    .unwrap(),
            );
            let parent = Session::new("retry-parent", "model");
            store.save_session(&parent).await.unwrap();
            let mut child =
                Session::new_child_of("retry-child", &parent, "model", "original assignment");
            child.add_message(bamboo_agent_core::Message::user("original assignment"));
            store.save_session(&child).await.unwrap();
            let policy = Arc::new(bamboo_tools::permission::PermissionConfig::new());
            bind_local_control_plane(store.as_ref(), &child.id, policy.as_ref()).await;
            let locked = Arc::new(bamboo_storage::LockedSessionStore::new(store.clone()));
            let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
                store.clone(),
                bamboo_domain::SessionInboxLimits::default(),
            ));
            let binding = actor_binding(store.clone(), inbox, locked);
            let registration = binding
                .router
                .register_run(&child.id, "first-run")
                .await
                .unwrap();
            let first = PlainActorActivation::start(
                store.clone(),
                &child,
                &binding,
                Some("first-run"),
                Some(policy.clone()),
                true,
                None,
            )
            .await
            .unwrap();
            if matches!(case, "old-claim" | "old-checkpoint") {
                let old = bamboo_domain::SessionMessageEnvelope::user_input(
                    &child.id,
                    "unconfirmed old input",
                );
                let receipt = first.input_inbox.deliver(&old).await.unwrap();
                first
                    .input_inbox
                    .mark_activation_eligible(
                        &child.id,
                        receipt.generation,
                        bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
                    )
                    .await
                    .unwrap();
                let claim = first
                    .claim_input(&binding, &child, "first-run")
                    .await
                    .unwrap()
                    .unwrap();
                if case == "old-checkpoint" {
                    first
                        .checkpoint_input(&mut child, "first-run", &claim)
                        .await
                        .unwrap();
                }
            }
            first
                .finish(if case == "succeeded" {
                    ActorActivationFinish::Succeeded
                } else {
                    ActorActivationFinish::Failed
                })
                .await
                .unwrap();
            drop(registration);
            let envelope = bamboo_domain::SessionMessageEnvelope::user_input(
                &child.id,
                if case == "oversize" {
                    "x".repeat(8193)
                } else {
                    "NEW_CORRECTION".into()
                },
            );
            let receipt = first.input_inbox.deliver(&envelope).await.unwrap();
            first
                .input_inbox
                .mark_activation_eligible(
                    &child.id,
                    receipt.generation,
                    bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
                )
                .await
                .unwrap();
            let before = store.load_session(&child.id).await.unwrap().unwrap();
            let expected_messages = serde_json::to_value(&before.messages).unwrap();
            child = before.clone();
            if case == "prefix-refusal" {
                child.messages.last_mut().unwrap().content = "stale cached prefix".into();
            }
            if case == "changed-birth" {
                child.created_at += chrono::Duration::milliseconds(1);
            }
            let _retry_registration = binding
                .router
                .register_run(&child.id, "retry-run")
                .await
                .unwrap();
            let result = PlainActorActivation::start_and_prepare(
                store.clone(),
                &mut child,
                &binding,
                Some("retry-run"),
                Some(policy.clone()),
                case != "readonly-refusal",
                None,
            )
            .await;
            if case != "accepted" {
                assert!(result.is_err(), "{case}: no retry Run may be dispatched");
                let actual = store.load_session(&before.id).await.unwrap().unwrap();
                assert_eq!(
                    serde_json::to_value(&actual.messages).unwrap(),
                    expected_messages,
                    "{case}: failed preparation preserves Main history"
                );
                assert!(
                    !first
                        .input_inbox
                        .was_admitted(&before.id, &envelope.id)
                        .await
                        .unwrap(),
                    "{case}: no ACK"
                );
                if case == "readonly-refusal" {
                    let entry = store.inspect_actor(&before.id).await.unwrap();
                    assert_eq!(entry.actor.current_attempt, 1);
                    assert_eq!(
                        entry.activation.unwrap().activation_id,
                        first.fence.activation_id
                    );
                    let backlog = first.input_inbox.inspect(&before.id).await.unwrap();
                    assert_eq!((backlog.pending, backlog.claimed), (1, 0));
                }
                if matches!(case, "prefix-refusal" | "changed-birth" | "oversize") {
                    assert_eq!(
                        first.input_inbox.inspect(&before.id).await.unwrap().claimed,
                        1
                    );
                    assert_eq!(
                        store
                            .inspect_actor(&before.id)
                            .await
                            .unwrap()
                            .activation
                            .unwrap()
                            .status,
                        bamboo_domain::ActorActivationStatus::Failed
                    );
                }
                if case == "oversize" {
                    let reopened = Arc::new(
                        bamboo_storage::SessionStoreV2::new(temp.path().into())
                            .await
                            .unwrap(),
                    );
                    let cold_inbox = bamboo_storage::FileSessionInbox::new(
                        reopened,
                        bamboo_domain::SessionInboxLimits::default(),
                    );
                    let now = chrono::Utc::now();
                    let leases = cold_inbox
                        .inspect_owned_leases(&before.id, 2, now)
                        .await
                        .unwrap();
                    assert_eq!(leases.len(), 1);
                    assert_eq!(leases[0].failure_count, 1);
                    assert_eq!(
                        leases[0].last_error_code.as_deref(),
                        Some("unsupported_actor_input")
                    );
                    let retry_after = leases[0].retry_after.unwrap();
                    assert!(retry_after > now);
                    let readiness = cold_inbox
                        .inspect_wake_readiness(&before.id, now)
                        .await
                        .unwrap();
                    assert!(
                        readiness.ready.is_none(),
                        "failed correction must not hot-loop"
                    );
                    assert_eq!(readiness.next_due_at, Some(retry_after));
                }
                continue;
            }
            let (retry, prepared) = result.unwrap();
            let (prefix, delivery) = prepared.unwrap();
            assert_eq!(
                prefix,
                serde_json::to_value(&before.messages)
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .clone()
            );
            assert_eq!(delivery.envelope, envelope);
            assert_eq!(delivery.activation_run_id, "retry-run");
            assert_eq!(delivery.canonical_claim_generation, receipt.generation);
            assert!(store
                .validate_fence(&first.fence, chrono::Utc::now())
                .await
                .is_err());
            let running = store.inspect_actor(&child.id).await.unwrap();
            assert_eq!(running.actor.current_attempt, 2);
            assert_eq!(
                running.activation.as_ref().unwrap().status,
                bamboo_domain::ActorActivationStatus::Running
            );
            assert!(running.activation.unwrap().lease_epoch > first.fence.lease_epoch);
            let claim = retry.initial_input.as_ref().unwrap();
            assert!(child
                .session_inbox_admission()
                .unwrap()
                .contains(&envelope.id));
            assert!(
                !retry
                    .input_inbox
                    .was_admitted(&child.id, &envelope.id)
                    .await
                    .unwrap(),
                "checkpoint is not startup confirmation"
            );
            let mut confirmation = admission_confirmation(&child.id, &claim.claim, "retry-run");
            confirmation.activation_run_id = "first-run".into();
            assert!(retry
                .confirm_input(&binding, &child, "retry-run", claim, &confirmation)
                .await
                .is_err());
            assert!(!retry
                .input_inbox
                .was_admitted(&child.id, &envelope.id)
                .await
                .unwrap());
            confirmation.activation_run_id = "retry-run".into();
            retry
                .confirm_input(&binding, &child, "retry-run", claim, &confirmation)
                .await
                .unwrap();
            let (tx, mut rx) = mpsc::channel(1);
            retry
                .append_reply(&mut child, "RETRY_REPLY".into(), &tx)
                .await
                .unwrap();
            rx.recv().await.unwrap();
            retry
                .finish(ActorActivationFinish::Succeeded)
                .await
                .unwrap();
            let reopened = Arc::new(
                bamboo_storage::SessionStoreV2::new(temp.path().into())
                    .await
                    .unwrap(),
            );
            let cold = reopened.load_session(&child.id).await.unwrap().unwrap();
            assert_eq!(cold.created_at, before.created_at);
            assert_eq!(cold.messages.len(), before.messages.len() + 2);
            assert_eq!(
                cold.messages[before.messages.len()].id,
                envelope.id.as_str()
            );
            assert_eq!(cold.messages.last().unwrap().content, "RETRY_REPLY");
            assert_eq!(
                cold.messages
                    .iter()
                    .filter(|m| m.id == envelope.id.as_str())
                    .count(),
                1
            );
            let cold_inbox = bamboo_storage::FileSessionInbox::new(
                reopened,
                bamboo_domain::SessionInboxLimits::default(),
            );
            assert!(cold_inbox
                .was_admitted(&cold.id, &envelope.id)
                .await
                .unwrap());
            assert_eq!(cold_inbox.inspect(&cold.id).await.unwrap().claimed, 0);
        }
    }

    #[tokio::test]
    async fn pre_ack_owned_recovery_requires_expiry_provenance_and_verified_already() {
        for case in [
            "accepted",
            "actor-live",
            "claim-live",
            "legacy",
            "ack",
            "readonly",
            "prefix",
            "duplicate",
            "missing-marker",
            "mixed",
            "project",
            "birth",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let store = Arc::new(
                bamboo_storage::SessionStoreV2::new(temp.path().into())
                    .await
                    .unwrap(),
            );
            let parent = Session::new("recovery-parent", "model");
            store.save_session(&parent).await.unwrap();
            let mut child =
                Session::new_child_of("recovery-child", &parent, "model", "original task");
            child.add_message(bamboo_agent_core::Message::user("original task"));
            store.save_session(&child).await.unwrap();
            let policy = Arc::new(bamboo_tools::permission::PermissionConfig::new());
            bind_local_control_plane(store.as_ref(), &child.id, policy.as_ref()).await;
            let inbox = bamboo_storage::FileSessionInbox::new(
                store.clone(),
                bamboo_domain::SessionInboxLimits::default(),
            );
            let now = chrono::Utc::now();
            let cutoff = now + chrono::Duration::seconds(1);
            let consumer = SessionInboxConsumerId::new();
            let old = store
                .claim_activation(&ActorActivationClaim {
                    actor_id: child.id.clone(),
                    run_id: "old-run".into(),
                    lease_owner: consumer.as_str().into(),
                    lease_expires_at: if case == "actor-live" {
                        now + chrono::Duration::minutes(1)
                    } else {
                        cutoff
                    },
                    inbox_generation: 0,
                    placement_ref: if case == "legacy" {
                        None
                    } else {
                        Some(bamboo_domain::ActorPlacementRef {
                            class: bamboo_domain::ActorPlacementClass::Local,
                            lease_id: "owned-initial-release-v1:actual-old-worker".into(),
                            slot_epoch: None,
                        })
                    },
                    now,
                })
                .await
                .unwrap();
            store.start_activation(&old.fence(), now).await.unwrap();
            let envelope =
                bamboo_domain::SessionMessageEnvelope::user_input(&child.id, "RECOVER_SAME_INPUT");
            let receipt = inbox.deliver(&envelope).await.unwrap();
            inbox
                .mark_activation_eligible(
                    &child.id,
                    receipt.generation,
                    bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
                )
                .await
                .unwrap();
            let claim = inbox
                .claim_owned(
                    &child.id,
                    1,
                    Some("old-run"),
                    &SessionInboxLeaseRequest {
                        consumer,
                        now,
                        duration: if case == "claim-live" {
                            chrono::Duration::minutes(1)
                        } else {
                            cutoff - now
                        },
                    },
                )
                .await
                .unwrap()
                .pop()
                .unwrap();
            child = inbox
                .checkpoint_actor_input(bamboo_storage::ActorInputCheckpoint {
                    fence: old.fence(),
                    expected_created_at: child.created_at,
                    claim: claim.clone(),
                    expected_messages: child.messages.clone(),
                    expected_provider_transcript: child.provider_transcript.clone(),
                    expected_admission: None,
                })
                .await
                .unwrap()
                .session;
            if case == "ack" {
                inbox
                    .ack_owned(&child.id, &claim, chrono::Utc::now())
                    .await
                    .unwrap();
            }
            if case == "mixed" {
                inbox
                    .deliver(&bamboo_domain::SessionMessageEnvelope::user_input(
                        &child.id,
                        "different input",
                    ))
                    .await
                    .unwrap();
            }
            let directory = store
                .bamboo_home_dir()
                .join(store.resolve_rel_path(&child.id).await.unwrap());
            let main = std::fs::read(directory.join("session.json")).unwrap();
            let canonical = child.clone();
            match case {
                "prefix" => child.messages[0].content.push_str(" stale"),
                "duplicate" => child.messages.push(child.messages.last().unwrap().clone()),
                "missing-marker" => {
                    child
                        .messages
                        .last_mut()
                        .unwrap()
                        .metadata
                        .as_mut()
                        .unwrap()
                        .as_object_mut()
                        .unwrap()
                        .remove("_bamboo_owned_input_checkpoint");
                }
                "project" => child.set_project_id_meta(bamboo_domain::ProjectId::new().to_string()),
                "birth" => child.created_at += chrono::Duration::nanoseconds(1),
                _ => {}
            }
            let delay = (cutoff - chrono::Utc::now()).to_std().unwrap_or_default();
            tokio::time::sleep(delay + Duration::from_millis(10)).await;
            let second = Arc::new(
                bamboo_storage::SessionStoreV2::new(temp.path().into())
                    .await
                    .unwrap(),
            );
            let binding = actor_binding(
                second.clone(),
                Arc::new(bamboo_storage::FileSessionInbox::new(
                    second.clone(),
                    bamboo_domain::SessionInboxLimits::default(),
                )),
                Arc::new(bamboo_storage::LockedSessionStore::new(second.clone())),
            );
            let _owner = binding
                .router
                .register_run(&child.id, "replacement-run")
                .await
                .unwrap();
            let prepared = PlainActorActivation::start_and_prepare(
                second.clone(),
                &mut child,
                &binding,
                Some("replacement-run"),
                Some(policy.clone()),
                case != "readonly",
                Some("probed-new-worker"),
            )
            .await;
            assert_eq!(
                std::fs::read(directory.join("session.json")).unwrap(),
                main,
                "{case}: Already or refusal never rewrites Main"
            );
            if case != "accepted" {
                assert!(prepared.is_err(), "{case}");
                assert_eq!(
                    inbox.was_admitted(&child.id, &envelope.id).await.unwrap(),
                    case == "ack"
                );
                continue;
            }
            let (replacement, delivery) = prepared.unwrap();
            assert!(replacement.recovering_pre_ack);
            let (messages, delivery) = delivery.unwrap();
            let new_claim = replacement.initial_input.as_ref().unwrap();
            assert_eq!(new_claim.claim.envelope, claim.claim.envelope);
            assert_eq!(new_claim.claim.generation, claim.claim.generation);
            assert_eq!(
                new_claim.claim.activation_policy,
                claim.claim.activation_policy
            );
            assert!(new_claim.lease.epoch > claim.lease.epoch);
            assert_ne!(new_claim.lease.incarnation, claim.lease.incarnation);
            assert_eq!(
                replacement.inbox_generation, 1,
                "old Actor generation was zero"
            );
            assert_eq!(
                second
                    .inspect_actor(&child.id)
                    .await
                    .unwrap()
                    .actor
                    .current_attempt,
                2
            );
            assert!(second
                .validate_fence(&old.fence(), chrono::Utc::now())
                .await
                .is_err());
            assert!(inbox
                .ack_owned(&child.id, &claim, chrono::Utc::now())
                .await
                .is_err());
            assert_eq!(
                serde_json::to_value(&child.messages).unwrap(),
                serde_json::to_value(&canonical.messages).unwrap()
            );
            let mut worker: Vec<bamboo_agent_core::Message> = messages
                .iter()
                .cloned()
                .map(|v| serde_json::from_value(v).unwrap())
                .collect();
            let target = worker.last_mut().unwrap();
            assert!(target
                .metadata
                .as_ref()
                .unwrap()
                .get("_bamboo_owned_input_checkpoint")
                .is_none());
            assert!(bamboo_domain::is_matching_session_message(
                target, &envelope
            ));
            let mut wanted = canonical.messages.last().unwrap().clone();
            wanted
                .metadata
                .as_mut()
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove("_bamboo_owned_input_checkpoint");
            assert_eq!(
                serde_json::to_value(target).unwrap(),
                serde_json::to_value(wanted).unwrap()
            );
            let run = RunSpec {
                assignment: "original task".into(),
                logical_session: Some(LogicalSessionIdentity {
                    session_id: child.id.clone(),
                    parent_session_id: child.parent_session_id.clone(),
                    root_session_id: child.root_session_id.clone(),
                    creation: Some(bamboo_subagent::proto::ChildCreationIdentity {
                        created_at: child.created_at,
                        spawn_depth: child.spawn_depth,
                    }),
                }),
                project_id: None,
                reasoning_effort: None,
                permission_policy: Some(PermissionPolicyContext {
                    revision: policy.policy_revision(),
                    requested_mode: "default".into(),
                    effective_mode: "default".into(),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    session_id: child.id.clone(),
                    workspace_path: child.workspace.clone(),
                    environment_lease: None,
                    inherit_session_grants: false,
                    policy: serde_json::to_value(policy.to_serializable()).unwrap(),
                }),
                messages,
                activation_run_id: Some("replacement-run".into()),
                execution_epoch: 2,
                initial_session_messages: vec![delivery.clone()],
                secrets: Default::default(),
            };
            let policy_revision = policy.policy_revision();
            let mut expected = Some(expected_default_permission_posture(policy_revision));
            let ChildFrame::Event { event } = permission_posture_frame(&child.id, policy_revision)
            else {
                unreachable!()
            };
            let (events, mut rx) = mpsc::channel(1);
            process_actor_event(
                event,
                true,
                &mut PermissionPostureHandshake::Awaiting,
                expected.as_ref(),
                Some(&binding),
                &mut child,
                &events,
                &mut ActorEventDisplay::default(),
            )
            .await
            .unwrap();
            rx.recv().await.unwrap();
            let request = bamboo_subagent::proto::InitialInputReleaseRequest::from_run(
                &run,
                &delivery,
                uuid::Uuid::new_v4().to_string(),
            )
            .unwrap();
            replacement
                .release_initial_input(
                    &binding,
                    &child,
                    &run,
                    2,
                    new_claim,
                    &request,
                    &mut expected,
                    None,
                )
                .await
                .unwrap();
            assert!(inbox.was_admitted(&child.id, &envelope.id).await.unwrap());
            replacement
                .append_reply(&mut child, "RECOVERY_REPLY".into(), &events)
                .await
                .unwrap();
            rx.recv().await.unwrap();
            replacement
                .finish(ActorActivationFinish::Succeeded)
                .await
                .unwrap();
            let cold = second.load_session(&child.id).await.unwrap().unwrap();
            assert_eq!(cold.created_at, canonical.created_at);
            assert_eq!(
                serde_json::to_value(&cold.messages[..canonical.messages.len()]).unwrap(),
                serde_json::to_value(&canonical.messages).unwrap()
            );
            assert_eq!(cold.messages.last().unwrap().content, "RECOVERY_REPLY");
            assert_eq!(
                cold.messages
                    .iter()
                    .filter(|m| m.id == envelope.id.as_str())
                    .count(),
                1
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn owned_initial_release_missing_only_new_capability_rejects_before_spawn_or_run() {
        use bamboo_subagent::provision::*;
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let worker = temp.path().join("mixed-worker");
        let spawned = temp.path().join("spawned");
        let run = temp.path().join("run");
        let probes = temp.path().join("probes");
        let fabric = temp.path().join("fabric");
        let mut report = WorkerCapabilityReport::current();
        report
            .capabilities
            .retain(|name| name != INITIAL_INPUT_RELEASE_WORKER_CAPABILITY);
        assert!(!report.supports(INITIAL_INPUT_RELEASE_WORKER_CAPABILITY));
        let report = serde_json::to_string(&report).unwrap();
        std::fs::write(&worker, format!(
            "#!/bin/sh\nfor arg in \"$@\"; do last=\"$arg\"; done\nif [ \"$last\" = --print-capabilities ]; then\nprintf '%s\\n' '{report}'\nprintf 'probe\\n' >> \"$3\"\nelse\n: > \"$1\"\n: > \"$2\"\ncat >/dev/null\nfi\n"
        )).unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700)).unwrap();
        let args = vec![
            spawned.to_str().unwrap().into(),
            run.to_str().unwrap().into(),
            probes.to_str().unwrap().into(),
        ];
        // Real subprocess probes prove the rejection cannot be an earlier missing bit.
        for capability in [
            REQUIRED_CHILD_CONTEXT_WORKER_CAPABILITY,
            CHILD_CREATION_IDENTITY_WORKER_CAPABILITY,
            NATIVE_TOOL_CEILING_WORKER_CAPABILITY,
            TYPED_READ_ONLY_WORKER_CAPABILITY,
        ] {
            bamboo_subagent::fleet::require_worker_capability(&worker, &args, capability)
                .await
                .unwrap();
        }
        let runner = ActorChildRunner::new(
            "mixed".into(),
            worker,
            args,
            fabric.clone(),
            ExecutorSpec::BambooRuntime,
            vec![],
            "openai".into(),
            1,
        );
        // The actual Host method remains mandatory before claim/provision/Run.
        let error = runner
            .require_initial_input_release_worker()
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("owned initial release worker capability unconfirmed"));
        assert_eq!(std::fs::read_to_string(probes).unwrap().lines().count(), 5);
        assert!(!spawned.exists() && !run.exists() && !fabric.exists());
        assert!(runner.pool.lock().await.is_empty());
    }

    #[tokio::test]
    async fn ordinary_glob_only_observes_own_initialization_signals() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let store = bamboo_storage::SessionStoreV2::new(home.clone())
            .await
            .unwrap();
        let parent = Session::new("glob-standard-parent", "model");
        store.save_session(&parent).await.unwrap();
        let child = Session::new_child_of("glob-standard-child", &parent, "model", "one Glob");
        store.save_session(&child).await.unwrap();
        let directory = home.join(store.resolve_rel_path(&child.id).await.unwrap());
        // An unrelated bad child would reject a full tree snapshot, but is not
        // part of this own-session no-replay observation.
        let unrelated = directory.parent().unwrap().join("unrelated");
        std::fs::create_dir(&unrelated).unwrap();
        std::fs::write(unrelated.join("session.json"), b"invalid").unwrap();
        require_unowned_glob(&store, &child).await.unwrap();
        for name in ["actor-authority.json", "actor-authority.initialized.json"] {
            let path = directory.join(name);
            std::fs::write(&path, b"invalid prior signal").unwrap();
            assert!(require_unowned_glob(&store, &child).await.is_err());
            std::fs::remove_file(&path).unwrap();
        }
        #[cfg(unix)]
        {
            let marker = directory.join("actor-authority.initialized.json");
            std::os::unix::fs::symlink(directory.join("missing"), &marker).unwrap();
            assert!(require_unowned_glob(&store, &child).await.is_err());
        }
    }

    #[test]
    fn owned_glob_collector_requires_actual_ordered_tool_trace_and_typed_tail() {
        let call = bamboo_domain::ToolCall {
            id: "glob".into(),
            tool_type: "function".into(),
            function: bamboo_domain::FunctionCall {
                name: "Glob".into(),
                arguments: r#"{"pattern":"marker.txt"}"#.into(),
            },
        };
        let messages = vec![
            bamboo_agent_core::Message::assistant("", Some(vec![call])),
            bamboo_agent_core::Message::tool_result("glob", "marker.txt"),
            bamboo_agent_core::Message::assistant("done", None),
        ];
        let start = serde_json::to_value(AgentEvent::ToolStart {
            tool_call_id: "glob".into(),
            tool_name: "Glob".into(),
            arguments: serde_json::json!({"pattern":"marker.txt"}),
        })
        .unwrap();
        let complete = serde_json::to_value(AgentEvent::ToolComplete {
            tool_call_id: "glob".into(),
            result: bamboo_domain::ToolResult::text(true, "marker.txt"),
        })
        .unwrap();
        let tail =
            serde_json::to_value(bamboo_subagent::proto::ReadOnlyActorTranscript::Complete {
                messages: messages
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<Result<_, _>>()
                    .unwrap(),
            })
            .unwrap();
        let mut ordered = ReadOnlyActorCollector::default();
        assert!(ordered.event(&start).unwrap());
        assert!(ordered.event(&complete).unwrap());
        assert!(!ordered.event(&tail).unwrap());
        assert!(ordered.event(&tail).is_err(), "duplicate suffix");
        assert_eq!(ordered.finish(Some("done")).unwrap().len(), 3);
        assert!(ReadOnlyActorCollector::default()
            .finish(Some("done"))
            .is_err());
        let mut unfinished = ReadOnlyActorCollector::default();
        unfinished.event(&start).unwrap();
        unfinished.event(&complete).unwrap();
        assert!(unfinished.finish(Some("done")).is_err(), "missing suffix");
        let mut closed = tail.clone();
        closed["grant"] = true.into();
        assert!(
            serde_json::from_value::<bamboo_subagent::proto::ReadOnlyActorTranscript>(closed)
                .is_err()
        );
        let mut oversized = tail.clone();
        oversized["messages"][2]["content"] = "x".repeat(64 * 1024).into();
        assert!(
            serde_json::from_value::<bamboo_subagent::proto::ReadOnlyActorTranscript>(oversized)
                .unwrap()
                .validate()
                .is_err()
        );
        assert!(ReadOnlyActorCollector::default().event(&complete).is_err());
        assert!(ReadOnlyActorCollector::default().event(&tail).is_err());
        let mut bad = start.clone();
        bad["tool_name"] = "Bash".into();
        assert!(ReadOnlyActorCollector::default().event(&bad).is_err());
        let mut duplicate = ReadOnlyActorCollector::default();
        duplicate.event(&start).unwrap();
        assert!(duplicate.event(&start).is_err());
        let hidden = serde_json::to_value(AgentEvent::ReasoningToken {
            content: "hidden".into(),
        })
        .unwrap();
        assert!(ReadOnlyActorCollector::default().event(&hidden).is_err());
        let mut mismatched = ReadOnlyActorCollector::default();
        mismatched.event(&start).unwrap();
        let mut bad = complete.clone();
        bad["tool_call_id"] = "foreign".into();
        assert!(mismatched.event(&bad).is_err());
        mismatched.event(&complete).unwrap();
        let mut changed = tail.clone();
        changed["messages"][1]["content"] = "fabricated".into();
        assert!(mismatched.event(&changed).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn owned_initial_release_requires_current_prefix_ack_and_same_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().into())
                .await
                .unwrap(),
        );
        let parent = Session::new("release-host-parent", "model");
        store.save_session(&parent).await.unwrap();
        let mut child = Session::new_child_of("release-host-child", &parent, "model", "assignment");
        store.save_session(&child).await.unwrap();
        let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        let binding = actor_binding(
            store.clone(),
            inbox,
            Arc::new(bamboo_storage::LockedSessionStore::new(store.clone())),
        );
        let _owner = binding
            .router
            .register_run(&child.id, "release-host-run")
            .await
            .unwrap();
        let policy = Arc::new(bamboo_tools::permission::PermissionConfig::new());
        let revision = policy.policy_revision();
        bind_local_control_plane(store.as_ref(), &child.id, policy.as_ref()).await;
        let activation = PlainActorActivation::start(
            store.clone(),
            &child,
            &binding,
            Some("release-host-run"),
            Some(policy.clone()),
            true,
            Some("probed-physical-worker"),
        )
        .await
        .unwrap();
        let envelope = bamboo_domain::SessionMessageEnvelope::user_input(&child.id, "RELEASE_ONCE");
        let receipt = activation.input_inbox.deliver(&envelope).await.unwrap();
        activation
            .input_inbox
            .mark_activation_eligible(
                &child.id,
                receipt.generation,
                bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let claim = activation
            .claim_input(&binding, &child, "release-host-run")
            .await
            .unwrap()
            .unwrap();
        let (prefix, delivery) = activation
            .checkpoint_input(&mut child, "release-host-run", &claim)
            .await
            .unwrap();
        let mut expected = Some(expected_default_permission_posture(revision));
        let ChildFrame::Event { event } = permission_posture_frame(&child.id, revision) else {
            unreachable!()
        };
        let (events, mut events_rx) = mpsc::channel(1);
        process_actor_event(
            event,
            true,
            &mut PermissionPostureHandshake::Awaiting,
            expected.as_ref(),
            Some(&binding),
            &mut child,
            &events,
            &mut ActorEventDisplay::default(),
        )
        .await
        .unwrap();
        events_rx.recv().await.unwrap();
        let run = RunSpec {
            assignment: "assignment".into(),
            logical_session: Some(LogicalSessionIdentity {
                session_id: child.id.clone(),
                parent_session_id: child.parent_session_id.clone(),
                root_session_id: child.root_session_id.clone(),
                creation: Some(bamboo_subagent::proto::ChildCreationIdentity {
                    created_at: child.created_at,
                    spawn_depth: child.spawn_depth,
                }),
            }),
            project_id: None,
            reasoning_effort: None,
            permission_policy: Some(bamboo_subagent::proto::PermissionPolicyContext {
                revision,
                requested_mode: "default".into(),
                effective_mode: "default".into(),
                bypass_permissions: false,
                auto_approve_permissions: false,
                session_id: child.id.clone(),
                workspace_path: child.workspace.clone(),
                environment_lease: None,
                inherit_session_grants: false,
                policy: serde_json::to_value(policy.to_serializable()).unwrap(),
            }),
            messages: prefix,
            activation_run_id: Some("release-host-run".into()),
            execution_epoch: 9,
            initial_session_messages: vec![delivery.clone()],
            secrets: Default::default(),
        };
        let request = bamboo_subagent::proto::InitialInputReleaseRequest::from_run(
            &run,
            &delivery,
            uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
        let mut stale = child.clone();
        stale.add_message(bamboo_agent_core::Message::user("not canonical"));
        assert!(activation
            .release_initial_input(
                &binding,
                &stale,
                &run,
                9,
                &claim,
                &request,
                &mut expected,
                None
            )
            .await
            .is_err());
        assert!(!activation
            .input_inbox
            .was_admitted(&child.id, &envelope.id)
            .await
            .unwrap());
        // Keep receipt lookup readable so Already reaches the real ACK write.
        // Unix root can bypass these modes; this FS fault needs an unprivileged user.
        use std::os::unix::fs::PermissionsExt;
        struct RestorePermissions(std::path::PathBuf, std::fs::Permissions);
        impl Drop for RestorePermissions {
            fn drop(&mut self) {
                let _ = std::fs::set_permissions(&self.0, self.1.clone());
            }
        }
        let admitted = store
            .bamboo_home_dir()
            .join(store.resolve_rel_path(&child.id).await.unwrap())
            .join("inbox/admitted");
        std::fs::create_dir_all(&admitted).unwrap();
        let restore = RestorePermissions(
            admitted.clone(),
            std::fs::metadata(&admitted).unwrap().permissions(),
        );
        std::fs::set_permissions(&admitted, std::fs::Permissions::from_mode(0o555)).unwrap();
        let verified = activation
            .input_inbox
            .checkpoint_actor_input(bamboo_storage::ActorInputCheckpoint {
                fence: activation.fence.clone(),
                expected_created_at: child.created_at,
                claim: claim.clone(),
                expected_messages: child.messages.clone(),
                expected_provider_transcript: child.provider_transcript.clone(),
                expected_admission: child.session_inbox_admission().cloned(),
            })
            .await
            .unwrap();
        assert_eq!(
            verified.status,
            bamboo_storage::ActorInputCheckpointStatus::AlreadyCheckpointed
        );
        let error = activation
            .release_initial_input(
                &binding,
                &child,
                &run,
                9,
                &claim,
                &request,
                &mut expected,
                None,
            )
            .await
            .unwrap_err();
        std::fs::set_permissions(&admitted, restore.1.clone()).unwrap();
        drop(restore);
        assert!(
            error.to_string().contains("ACK unresolved"),
            "actual release error (<=512B): {}",
            error.to_string().chars().take(128).collect::<String>()
        );
        let release = activation
            .release_initial_input(
                &binding,
                &child,
                &run,
                9,
                &claim,
                &request,
                &mut expected,
                None,
            )
            .await
            .unwrap();
        assert!(activation
            .input_inbox
            .was_admitted(&child.id, &envelope.id)
            .await
            .unwrap());
        assert_eq!(
            release.expires_at,
            activation
                .actor_deadline
                .min(claim.lease.expires_at)
                .min(activation.input_deadline)
        );
        assert_eq!(
            activation
                .release_initial_input(
                    &binding,
                    &child,
                    &run,
                    9,
                    &claim,
                    &request,
                    &mut expected,
                    Some(&release)
                )
                .await
                .unwrap(),
            release
        );
        let mut wrong = request.clone();
        wrong.nonce = uuid::Uuid::new_v4().to_string();
        assert!(activation
            .release_initial_input(
                &binding,
                &child,
                &run,
                9,
                &claim,
                &wrong,
                &mut expected,
                Some(&release)
            )
            .await
            .is_err());
        assert!(activation
            .release_initial_input(
                &binding,
                &child,
                &run,
                10,
                &claim,
                &request,
                &mut expected,
                Some(&release)
            )
            .await
            .is_err());
        // A second real checkpoint/release reuses the activation, not its old epoch or nonce.
        let deadlines = (
            activation.fence.clone(),
            activation.actor_deadline,
            activation.input_deadline,
        );
        let second = bamboo_domain::SessionMessageEnvelope::user_input(&child.id, "RELEASE_TWICE");
        let receipt = activation.input_inbox.deliver(&second).await.unwrap();
        activation
            .input_inbox
            .mark_activation_eligible(
                &child.id,
                receipt.generation,
                bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let mut link = ConfirmationSequenceLink {
            frames: VecDeque::new(),
            sent: Vec::new(),
        };
        let epochs = AtomicU64::new(9);
        let (claim2, epoch2) = activation
            .continue_input(
                &mut link,
                &binding,
                &mut child,
                "release-host-run",
                Some((&run, &epochs)),
                &mut expected,
                "SECOND_REPLY".into(),
                &events,
            )
            .await
            .unwrap()
            .unwrap();
        events_rx.recv().await.unwrap();
        assert_eq!(epoch2, 10);
        let ParentFrame::Run(next) = &link.sent[0] else {
            panic!("second correlated Run")
        };
        assert_eq!(next.logical_session, run.logical_session);
        assert_eq!(next.activation_run_id, run.activation_run_id);
        assert_eq!(next.initial_session_messages.len(), 1);
        assert!(next
            .messages
            .iter()
            .any(|m| m["id"] == envelope.id.as_str()));
        assert!(next.messages.iter().any(|m| m["content"] == "SECOND_REPLY"));
        assert!(!next.messages.iter().any(|m| m["id"] == second.id.as_str()));
        let request2 = bamboo_subagent::proto::InitialInputReleaseRequest::from_run(
            next,
            &next.initial_session_messages[0],
            uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
        assert_ne!(request2.nonce, request.nonce);
        macro_rules! release_second {
            ($candidate:expr, $request:expr, $epoch:expr, $previous:expr) => {
                activation
                    .release_initial_input(
                        &binding,
                        $candidate,
                        next,
                        $epoch,
                        &claim2,
                        $request,
                        &mut expected,
                        $previous,
                    )
                    .await
            };
        }
        assert!(release_second!(&child, &request, epoch2, None).is_err());
        assert!(release_second!(&child, &request2, 9, None).is_err());
        let mut stale2 = child.clone();
        stale2.add_message(bamboo_agent_core::Message::user("stale second prefix"));
        assert!(release_second!(&stale2, &request2, epoch2, None).is_err());
        assert!(!activation
            .input_inbox
            .was_admitted(&child.id, &second.id)
            .await
            .unwrap());
        let release2 = release_second!(&child, &request2, epoch2, None).unwrap();
        assert_eq!(
            release_second!(&child, &request2, epoch2, Some(&release2)).unwrap(),
            release2
        );
        let mut wrong2 = request2.clone();
        wrong2.nonce = request.nonce.clone();
        assert!(release_second!(&child, &wrong2, epoch2, Some(&release2)).is_err());
        assert!(activation
            .input_inbox
            .was_admitted(&child.id, &second.id)
            .await
            .unwrap());
        let expired = bamboo_domain::SessionMessageEnvelope::user_input(&child.id, "EXPIRED_INPUT");
        let receipt = activation.input_inbox.deliver(&expired).await.unwrap();
        activation
            .input_inbox
            .mark_activation_eligible(
                &child.id,
                receipt.generation,
                bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        let now = chrono::Utc::now();
        let expired_claim = activation
            .input_inbox
            .claim_owned(
                &child.id,
                1,
                Some("release-host-run"),
                &SessionInboxLeaseRequest {
                    consumer: activation.input_consumer.clone(),
                    now,
                    duration: chrono::Duration::seconds(1),
                },
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        let (prefix, delivery3) = activation
            .checkpoint_input(&mut child, "release-host-run", &expired_claim)
            .await
            .unwrap();
        let mut expired_run = next.clone();
        expired_run.execution_epoch = 11;
        expired_run.messages = prefix;
        expired_run.initial_session_messages = vec![delivery3.clone()];
        let expired_request = bamboo_subagent::proto::InitialInputReleaseRequest::from_run(
            &expired_run,
            &delivery3,
            uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
        tokio::time::sleep(
            (expired_claim.lease.expires_at - chrono::Utc::now())
                .to_std()
                .unwrap_or_default(),
        )
        .await;
        assert!(chrono::Utc::now() >= expired_claim.lease.expires_at);
        assert!(activation
            .release_initial_input(
                &binding,
                &child,
                &expired_run,
                11,
                &expired_claim,
                &expired_request,
                &mut expected,
                None
            )
            .await
            .is_err());
        assert!(!activation
            .input_inbox
            .was_admitted(&child.id, &expired.id)
            .await
            .unwrap());
        assert_eq!(
            (
                activation.fence.clone(),
                activation.actor_deadline,
                activation.input_deadline
            ),
            deadlines
        );
        assert_eq!(
            link.sent.len(),
            1,
            "no dispatch from rejected release checks"
        );
        let cold = store.load_session(&child.id).await.unwrap().unwrap();
        for id in [&envelope.id, &second.id] {
            assert_eq!(
                cold.messages.iter().filter(|m| m.id == id.as_str()).count(),
                1
            );
            assert!(cold.session_inbox_admission().unwrap().contains(id));
        }
        // Mutate the actual captured policy Arc; cached ACK/release is not a fresh grant.
        policy.deny_scoped_session_permission(
            &child.id,
            bamboo_tools::permission::PermissionType::ExecuteCommand,
            "blocked-action",
        );
        assert!(release_second!(&child, &request2, epoch2, Some(&release2)).is_err());
        assert!(activation
            .input_inbox
            .was_admitted(&child.id, &second.id)
            .await
            .unwrap());
        activation
            .finish(ActorActivationFinish::Failed)
            .await
            .unwrap();
        // Terminal ACK can be idempotent; stopped/expired Actor cannot grant.
        activation
            .input_inbox
            .ack_owned(&child.id, &claim, chrono::Utc::now())
            .await
            .unwrap();
        assert!(activation
            .release_initial_input(
                &binding,
                &child,
                &run,
                9,
                &claim,
                &request,
                &mut expected,
                Some(&release)
            )
            .await
            .is_err());
        let cold = bamboo_storage::SessionStoreV2::new(temp.path().into())
            .await
            .unwrap()
            .load_session(&child.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m.id == envelope.id.as_str())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn owned_actor_input_checkpoint_precedes_dispatch_and_exact_ack() {
        for reject_checkpoint in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let store = Arc::new(
                bamboo_storage::SessionStoreV2::new(temp.path().into())
                    .await
                    .unwrap(),
            );
            let parent = Session::new("owned-input-parent", "model");
            store.save_session(&parent).await.unwrap();
            let mut child =
                Session::new_child_of("owned-input-child", &parent, "model", "one plain task");
            store.save_session(&child).await.unwrap();
            let policy = Arc::new(bamboo_tools::permission::PermissionConfig::new());
            let raw_storage: Arc<dyn Storage> = store.clone();
            let locked = Arc::new(bamboo_storage::LockedSessionStore::new(raw_storage));
            let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
                store.clone(),
                bamboo_domain::SessionInboxLimits::default(),
            ));
            let binding = actor_binding(store.clone(), inbox, locked);
            let run_id = "owned-input-run";
            let _registration = binding
                .router
                .register_run(&child.id, run_id)
                .await
                .unwrap();
            assert!(PlainActorActivation::start(
                store.clone(),
                &child,
                &binding,
                Some(run_id),
                None,
                true,
                None,
            )
            .await
            .is_err());
            assert!(PlainActorActivation::start(
                store.clone(),
                &child,
                &binding,
                Some(run_id),
                Some(policy.clone()),
                true,
                None,
            )
            .await
            .is_err());
            assert_eq!(
                store
                    .inspect_actor(&child.id)
                    .await
                    .unwrap()
                    .actor
                    .current_attempt,
                0
            );
            bind_local_control_plane(store.as_ref(), &child.id, policy.as_ref()).await;
            let activation = PlainActorActivation::start(
                store.clone(),
                &child,
                &binding,
                Some(run_id),
                Some(policy.clone()),
                true,
                None,
            )
            .await
            .unwrap();
            let envelope =
                bamboo_domain::SessionMessageEnvelope::user_input(&child.id, "CORRECTION_ONCE");
            let receipt = activation.input_inbox.deliver(&envelope).await.unwrap();
            activation
                .input_inbox
                .mark_activation_eligible(
                    &child.id,
                    receipt.generation,
                    bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
                )
                .await
                .unwrap();
            let mut link = ConfirmationSequenceLink {
                frames: VecDeque::new(),
                sent: Vec::new(),
            };
            let policy_revision = policy.policy_revision();
            let run = RunSpec {
                assignment: "one plain task".into(),
                logical_session: None,
                project_id: None,
                reasoning_effort: None,
                permission_policy: Some(PermissionPolicyContext {
                    revision: policy_revision,
                    requested_mode: "default".into(),
                    effective_mode: "default".into(),
                    bypass_permissions: false,
                    auto_approve_permissions: false,
                    session_id: child.id.clone(),
                    workspace_path: child.workspace.clone(),
                    environment_lease: None,
                    inherit_session_grants: false,
                    policy: serde_json::to_value(policy.to_serializable()).unwrap(),
                }),
                messages: vec![],
                activation_run_id: Some(run_id.into()),
                execution_epoch: 1,
                initial_session_messages: vec![],
                secrets: Default::default(),
            };
            let epochs = AtomicU64::new(1);
            let (tx, mut rx) = mpsc::channel(1);
            let mut expected = Some(expected_default_permission_posture(policy_revision));
            let ChildFrame::Event { event } = permission_posture_frame(&child.id, policy_revision)
            else {
                unreachable!()
            };
            process_actor_event(
                event,
                true,
                &mut PermissionPostureHandshake::Awaiting,
                expected.as_ref(),
                Some(&binding),
                &mut child,
                &tx,
                &mut ActorEventDisplay::default(),
            )
            .await
            .unwrap();
            rx.recv().await.unwrap();
            let first_audit =
                bamboo_domain::PermissionAuditSnapshot::from_metadata(&child.metadata)
                    .unwrap()
                    .audit_revision;
            if reject_checkpoint {
                // A real bounded publication channel blocks AFTER first-reply
                // commit, so change actual Actor authority before checkpoint.
                tx.send(AgentEvent::Complete {
                    usage: Default::default(),
                })
                .await
                .unwrap();
            }
            let mut operation = activation.continue_input(
                &mut link,
                &binding,
                &mut child,
                run_id,
                Some((&run, &epochs)),
                &mut expected,
                "FIRST_PLAIN_REPLY".into(),
                &tx,
            );
            if reject_checkpoint {
                tokio::select! {
                    result = &mut operation => panic!("publication barrier released early: {result:?}"),
                    _ = async {
                        tokio::time::timeout(Duration::from_secs(5), async {
                            loop {
                                let actual = store.load_session("owned-input-child").await.unwrap().unwrap();
                                if actual.messages.iter().any(|m| m.content == "FIRST_PLAIN_REPLY") { break; }
                                tokio::task::yield_now().await;
                            }
                        }).await.unwrap();
                        activation.finish(ActorActivationFinish::Failed).await.unwrap();
                        rx.recv().await.unwrap();
                    } => {}
                }
            }
            let result = operation.await;
            if reject_checkpoint {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("checkpoint unconfirmed"));
                assert!(
                    link.sent.is_empty(),
                    "zero worker/provider correction dispatch"
                );
                let durable = store.load_session(&child.id).await.unwrap().unwrap();
                assert!(durable
                    .messages
                    .iter()
                    .any(|m| m.content == "FIRST_PLAIN_REPLY"));
                assert!(!durable
                    .messages
                    .iter()
                    .any(|m| m.id == envelope.id.as_str()));
                assert!(!activation
                    .input_inbox
                    .was_admitted(&child.id, &envelope.id)
                    .await
                    .unwrap());
                assert_eq!(
                    activation
                        .input_inbox
                        .inspect(&child.id)
                        .await
                        .unwrap()
                        .claimed,
                    1
                );
                continue;
            }
            let (claim, epoch) = result.unwrap().unwrap();
            rx.recv().await.unwrap(); // Actual first-reply publication.
            assert_eq!(epoch, 2);
            assert_eq!(link.sent.len(), 1);
            let ParentFrame::Run(next) = &link.sent[0] else {
                panic!("correlated continuation Run")
            };
            assert_eq!(next.execution_epoch, 2);
            assert_eq!(next.activation_run_id.as_deref(), Some(run_id));
            assert!(next
                .messages
                .iter()
                .any(|m| m["content"] == "FIRST_PLAIN_REPLY"));
            assert!(!next
                .messages
                .iter()
                .any(|m| m["id"] == envelope.id.as_str()));
            let delivery = &next.initial_session_messages[0];
            assert_eq!(delivery.envelope, envelope);
            let durable = store.load_session(&child.id).await.unwrap().unwrap();
            assert!(durable
                .session_inbox_admission()
                .unwrap()
                .contains(&envelope.id));
            assert_eq!(
                durable
                    .messages
                    .iter()
                    .filter(|m| m.id == envelope.id.as_str())
                    .count(),
                1
            );
            assert!(
                !activation
                    .input_inbox
                    .was_admitted(&child.id, &envelope.id)
                    .await
                    .unwrap(),
                "checkpoint is not a worker admission receipt"
            );
            let mut confirmation = admission_confirmation(&child.id, &claim.claim, run_id);
            confirmation.canonical_claim_generation += 1;
            assert!(activation
                .confirm_input(&binding, &child, run_id, &claim, &confirmation)
                .await
                .is_err());
            assert!(!activation
                .input_inbox
                .was_admitted(&child.id, &envelope.id)
                .await
                .unwrap());
            confirmation.canonical_claim_generation = claim.claim.generation;
            activation
                .confirm_input(&binding, &child, run_id, &claim, &confirmation)
                .await
                .unwrap();
            assert!(activation
                .input_inbox
                .was_admitted(&child.id, &envelope.id)
                .await
                .unwrap());
            assert_eq!(
                expected.as_ref().unwrap().expected_audit_revision,
                Some(first_audit)
            );
            let ChildFrame::Event { event } = permission_posture_frame(&child.id, policy_revision)
            else {
                unreachable!()
            };
            process_actor_event(
                event,
                true,
                &mut PermissionPostureHandshake::Awaiting,
                expected.as_ref(),
                Some(&binding),
                &mut child,
                &tx,
                &mut ActorEventDisplay::default(),
            )
            .await
            .unwrap();
            rx.recv().await.unwrap();
            assert!(
                bamboo_domain::PermissionAuditSnapshot::from_metadata(&child.metadata)
                    .unwrap()
                    .audit_revision
                    > first_audit
            );
            let reopened = bamboo_storage::SessionStoreV2::new(temp.path().into())
                .await
                .unwrap();
            let cold = reopened.load_session(&child.id).await.unwrap().unwrap();
            assert_eq!(
                cold.messages
                    .iter()
                    .filter(|m| m.id == envelope.id.as_str())
                    .count(),
                1
            );
            assert!(cold
                .session_inbox_admission()
                .unwrap()
                .contains(&envelope.id));
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

    fn expected_default_permission_posture(policy_revision: u64) -> ExpectedPermissionPosture {
        ExpectedPermissionPosture {
            policy_revision,
            resolution: bamboo_domain::PermissionModeResolution {
                requested: bamboo_domain::SessionPermissionMode::Default,
                effective: bamboo_domain::PermissionMode::Default,
            },
            expected_audit_revision: None,
            executor_mapping: "test_actor:permission_mode=default".to_string(),
        }
    }

    fn permission_posture_frame(session_id: &str, policy_revision: u64) -> ChildFrame {
        ChildFrame::Event {
            event: serde_json::to_value(AgentEvent::PermissionPostureActivated {
                session_id: session_id.to_string(),
                policy_revision,
                requested_mode: "default".to_string(),
                effective_mode: "default".to_string(),
                executor_mapping: "test_actor:permission_mode=default".to_string(),
            })
            .expect("serialize permission posture event"),
        }
    }

    fn actor_event_frame(event: AgentEvent) -> ChildFrame {
        ChildFrame::Event {
            event: serde_json::to_value(event).expect("serialize actor event"),
        }
    }

    #[test]
    fn actor_event_batch_is_fenced_by_session_activation_and_epoch() {
        let session = Session::new_child("logical-child", "logical-parent", "model", "child");
        let mut batch = ActorEventBatch {
            logical_session: Some(LogicalSessionIdentity {
                creation: None,
                session_id: session.id.clone(),
                parent_session_id: session.parent_session_id.clone(),
                root_session_id: session.root_session_id.clone(),
            }),
            activation_id: Some("activation-7".into()),
            execution_epoch: 11,
            source_node_id: Some("node-a".into()),
            source_actor_id: Some("worker-a".into()),
            first_seq: 1,
            last_seq: 1,
            qos: bamboo_subagent::ActorEventQos::Ephemeral,
            events: vec![serde_json::json!({"type":"token","content":"ok"})],
        };
        validate_actor_event_batch(
            &batch,
            &session,
            "logical-parent",
            Some("activation-7"),
            11,
            "worker-a",
            None,
        )
        .unwrap();

        let creation = bamboo_subagent::proto::ChildCreationIdentity {
            created_at: session.created_at,
            spawn_depth: session.spawn_depth,
        };
        assert!(validate_actor_event_batch(
            &batch,
            &session,
            "logical-parent",
            Some("activation-7"),
            11,
            "worker-a",
            Some(&creation)
        )
        .is_err());
        batch.logical_session.as_mut().unwrap().creation = Some(creation.clone());
        validate_actor_event_batch(
            &batch,
            &session,
            "logical-parent",
            Some("activation-7"),
            11,
            "worker-a",
            Some(&creation),
        )
        .unwrap();
        batch
            .logical_session
            .as_mut()
            .unwrap()
            .creation
            .as_mut()
            .unwrap()
            .created_at += chrono::Duration::nanoseconds(1);
        assert!(validate_actor_event_batch(
            &batch,
            &session,
            "logical-parent",
            Some("activation-7"),
            11,
            "worker-a",
            Some(&creation)
        )
        .is_err());
        batch.logical_session.as_mut().unwrap().creation = None;

        batch.execution_epoch = 10;
        assert!(validate_actor_event_batch(
            &batch,
            &session,
            "logical-parent",
            Some("activation-7"),
            11,
            "worker-a",
            None,
        )
        .unwrap_err()
        .to_string()
        .contains("execution epoch"));
        batch.execution_epoch = 11;
        batch.activation_id = Some("replaced-activation".into());
        assert!(validate_actor_event_batch(
            &batch,
            &session,
            "logical-parent",
            Some("activation-7"),
            11,
            "worker-a",
            None,
        )
        .unwrap_err()
        .to_string()
        .contains("stale activation"));
        batch.activation_id = Some("activation-7".into());
        batch
            .logical_session
            .as_mut()
            .expect("logical identity")
            .session_id = "foreign-child".into();
        assert!(validate_actor_event_batch(
            &batch,
            &session,
            "logical-parent",
            Some("activation-7"),
            11,
            "worker-a",
            None,
        )
        .unwrap_err()
        .to_string()
        .contains("different logical session"));
        batch
            .logical_session
            .as_mut()
            .expect("logical identity")
            .session_id = session.id.clone();
        batch.source_actor_id = Some("worker-b".into());
        assert!(validate_actor_event_batch(
            &batch,
            &session,
            "logical-parent",
            Some("activation-7"),
            11,
            "worker-a",
            None,
        )
        .unwrap_err()
        .to_string()
        .contains("different physical actor"));
    }

    #[tokio::test]
    async fn directory_event_ingress_rejects_old_attempt_and_forged_identity() {
        use bamboo_domain::Storage;

        let temp = tempfile::tempdir().unwrap();
        let store = bamboo_storage::SessionStoreV2::new(temp.path().into())
            .await
            .unwrap();
        let parent = Session::new("event-parent", "model");
        let child = Session::new_child_of("event-child", &parent, "model", "task");
        store.save_session(&parent).await.unwrap();
        store.save_session(&child).await.unwrap();
        store.ensure_actor(&child.id).await.unwrap();
        let now = chrono::Utc::now();
        let claim = |run_id: &str| ActorActivationClaim {
            actor_id: child.id.clone(),
            run_id: run_id.into(),
            lease_owner: "event-owner".into(),
            lease_expires_at: now + chrono::Duration::minutes(2),
            inbox_generation: 0,
            placement_ref: None,
            now,
        };
        let first = store.claim_activation(&claim("run-one")).await.unwrap();
        store.start_activation(&first.fence(), now).await.unwrap();
        let first_fence = first.fence();
        validate_directory_actor_event(&store, &first_fence, &child, Some("run-one"))
            .await
            .unwrap();
        validate_directory_actor_event(&store, &first_fence, &child, None)
            .await
            .unwrap(); // rolling-upgrade frame uses the same owner
        assert!(
            validate_directory_actor_event(&store, &first_fence, &child, Some("forged-run"))
                .await
                .is_err()
        );
        let mut forged = child.clone();
        forged.root_session_id = "foreign-root".into();
        assert!(
            validate_directory_actor_event(&store, &first_fence, &forged, Some("run-one"))
                .await
                .is_err()
        );

        store
            .finish_activation(&first_fence, now, ActorActivationFinish::Failed)
            .await
            .unwrap();
        let second = store.claim_activation(&claim("run-two")).await.unwrap();
        store.start_activation(&second.fence(), now).await.unwrap();
        assert!(
            validate_directory_actor_event(&store, &first_fence, &child, Some("run-one"))
                .await
                .is_err()
        );
        assert!(
            validate_directory_actor_event(&store, &first_fence, &child, None)
                .await
                .is_err()
        );
        validate_directory_actor_event(&store, &second.fence(), &child, Some("run-two"))
            .await
            .unwrap();
    }

    fn completed_actor_frame() -> ChildFrame {
        ChildFrame::Terminal {
            status: TerminalStatus::Completed,
            result: Some("done".to_string()),
            error: None,
            transcript: Vec::new(),
        }
    }

    async fn drive_permission_handshake_frames(
        session_id: &str,
        frames: impl IntoIterator<Item = ChildFrame>,
        expected: ExpectedPermissionPosture,
    ) -> (
        crate::runtime::runner::Result<Option<String>>,
        Session,
        Vec<AgentEvent>,
        Vec<ParentFrame>,
    ) {
        let mut link = ConfirmationSequenceLink {
            frames: frames.into_iter().collect(),
            sent: Vec::new(),
        };
        let (event_tx, mut event_rx) = mpsc::channel(128);
        let cancel = CancellationToken::new();
        let (_live_tx, mut live_rx) = mpsc::unbounded_channel();
        let (_delivery_tx, mut delivery_rx) = mpsc::unbounded_channel();
        let mut session = Session::new(session_id, "model");
        let result = drive(ActorDriveContext {
            expected_creation: None,
            client: &mut link,
            parent_session_id: "permission-parent",
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
            expected_permission_posture: Some(expected),
            session_inbox_runtime: None,
            actor_directory_store: None,
            canonical_subagent_tool: None,
            activation_run_id: None,
            execution_epoch: 0,
            expected_source_actor_id: session_id,
            initial_inflight_claims: VecDeque::new(),
            plain_actor: false,
            remote_environment_lease: false,
            readonly_output: None,
            local_history_tools: None,
            local_history_read_only: false,
            plain_input: None,
            canonical_activation: None,
            canonical_placement_ref: None,
            actor_event_observer: None,
            plain_run: None,
            first_frame_timeout: Some(Duration::from_secs(1)),
        })
        .await;
        let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        (result, session, events, link.sent)
    }

    #[tokio::test]
    async fn actor_permission_handshake_rejects_terminal_without_posture() {
        let session_id = "permission-missing";
        let (result, session, events, _) = drive_permission_handshake_frames(
            session_id,
            [completed_actor_frame()],
            expected_default_permission_posture(7),
        )
        .await;

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("terminated before permission posture confirmation"));
        assert!(events.is_empty());
        assert!(bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata).is_none());
    }

    #[tokio::test]
    async fn remote_environment_rejection_accepts_only_allowlisted_durable_terminal() {
        struct TerminalProofProbe {
            frame: Option<ChildFrame>,
            accepted: bool,
        }

        #[async_trait]
        impl bamboo_subagent::ChildLink for TerminalProofProbe {
            async fn send(&mut self, _frame: ParentFrame) -> bamboo_subagent::TransportResult<()> {
                Ok(())
            }

            async fn next_frame(&mut self) -> bamboo_subagent::TransportResult<Option<ChildFrame>> {
                Ok(self.frame.take())
            }

            fn accept_durable_terminal(&mut self, status: TerminalStatus) {
                self.accepted = status == TerminalStatus::Error;
            }

            fn has_pending_durable_terminal(&self) -> bool {
                self.accepted
            }
        }

        for (status, error, expected_proof) in [
            (
                TerminalStatus::Error,
                Some("remote_environment_checkout_not_clean"),
                true,
            ),
            (TerminalStatus::Error, Some("untrusted worker error"), false),
            (
                TerminalStatus::Cancelled,
                Some("remote_environment_checkout_not_clean"),
                false,
            ),
        ] {
            let mut link = TerminalProofProbe {
                frame: Some(ChildFrame::Terminal {
                    status,
                    result: None,
                    error: error.map(str::to_owned),
                    transcript: Vec::new(),
                }),
                accepted: false,
            };
            let (event_tx, _event_rx) = mpsc::channel(8);
            let cancel = CancellationToken::new();
            let (_live_tx, mut live_rx) = mpsc::unbounded_channel();
            let (_delivery_tx, mut delivery_rx) = mpsc::unbounded_channel();
            let mut session = Session::new("remote-terminal-proof", "model");
            let result = drive(ActorDriveContext {
                client: &mut link,
                parent_session_id: "proof-parent",
                child_session_id: "remote-terminal-proof",
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
                expected_permission_posture: Some(expected_default_permission_posture(7)),
                expected_creation: None,
                session_inbox_runtime: None,
                actor_directory_store: None,
                canonical_subagent_tool: None,
                activation_run_id: None,
                execution_epoch: 0,
                expected_source_actor_id: "remote-terminal-proof",
                initial_inflight_claims: VecDeque::new(),
                plain_actor: false,
                remote_environment_lease: true,
                readonly_output: None,
                local_history_tools: None,
                local_history_read_only: false,
                plain_input: None,
                canonical_activation: None,
                canonical_placement_ref: None,
                actor_event_observer: None,
                plain_run: None,
                first_frame_timeout: Some(Duration::from_secs(1)),
            })
            .await;
            assert!(result.is_err());
            assert_eq!(
                bamboo_subagent::ChildLink::has_pending_durable_terminal(&link),
                expected_proof
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn remote_cancel_requires_selected_worker_terminal_for_release_proof() {
        struct CancelOutcomeLink {
            cancel_sent: bool,
            settle: bool,
            accepted: bool,
        }

        #[async_trait]
        impl bamboo_subagent::ChildLink for CancelOutcomeLink {
            async fn send(&mut self, frame: ParentFrame) -> bamboo_subagent::TransportResult<()> {
                self.cancel_sent = matches!(frame, ParentFrame::Cancel);
                Ok(())
            }

            async fn next_frame(&mut self) -> bamboo_subagent::TransportResult<Option<ChildFrame>> {
                if !self.cancel_sent || !self.settle {
                    std::future::pending::<bamboo_subagent::TransportResult<Option<ChildFrame>>>()
                        .await
                } else {
                    self.cancel_sent = false;
                    Ok(Some(ChildFrame::Terminal {
                        status: TerminalStatus::Cancelled,
                        result: None,
                        error: None,
                        transcript: Vec::new(),
                    }))
                }
            }

            fn accept_durable_terminal(&mut self, status: TerminalStatus) {
                self.accepted = status == TerminalStatus::Cancelled;
            }

            fn has_pending_durable_terminal(&self) -> bool {
                self.accepted
            }
        }

        let temp = tempfile::tempdir().unwrap();
        let store = bamboo_storage::SessionStoreV2::new(temp.path().into())
            .await
            .unwrap();
        let fence = ActorActivationFence {
            schema_version: 2,
            actor_id: "cancelled-remote-child".into(),
            activation_id: "cancelled-remote-activation".into(),
            attempt: 1,
            run_id: "cancelled-remote-run".into(),
            lease_owner: "cancelled-remote-host".into(),
            lease_epoch: 1,
        };
        for settle in [true, false] {
            let mut link = CancelOutcomeLink {
                cancel_sent: false,
                settle,
                accepted: false,
            };
            let (event_tx, _event_rx) = mpsc::channel(8);
            let cancel = CancellationToken::new();
            cancel.cancel();
            let (_live_tx, mut live_rx) = mpsc::unbounded_channel();
            let (_delivery_tx, mut delivery_rx) = mpsc::unbounded_channel();
            let mut session = Session::new("cancelled-remote-child", "model");
            let result = drive(ActorDriveContext {
                expected_creation: None,
                client: &mut link,
                parent_session_id: "cancelled-remote-parent",
                child_session_id: "cancelled-remote-child",
                child_attempt: 1,
                approval_registry: None,
                approval_decider: None,
                approval_reviewer: None,
                escalation_bridge: None,
                event_tx: &event_tx,
                cancel_token: &cancel,
                live_rx: &mut live_rx,
                delivery_rx: &mut delivery_rx,
                logical_session: &mut session,
                expected_permission_posture: Some(expected_default_permission_posture(7)),
                session_inbox_runtime: None,
                actor_directory_store: Some(&store),
                canonical_subagent_tool: None,
                activation_run_id: Some("cancelled-remote-run"),
                execution_epoch: 0,
                expected_source_actor_id: "cancelled-remote-worker",
                initial_inflight_claims: VecDeque::new(),
                plain_actor: false,
                remote_environment_lease: true,
                readonly_output: None,
                local_history_tools: None,
                local_history_read_only: false,
                plain_input: None,
                canonical_activation: Some(&fence),
                canonical_placement_ref: None,
                actor_event_observer: None,
                plain_run: None,
                first_frame_timeout: None,
            })
            .await;
            assert!(matches!(result, Err(AgentError::Cancelled)));
            assert_eq!(
                link.accepted, settle,
                "only a consumed exact terminal can release the remote fence"
            );
            assert_eq!(
                bamboo_subagent::ChildLink::has_pending_durable_terminal(&link),
                settle
            );
        }
    }

    #[tokio::test]
    async fn actor_permission_handshake_rejects_malformed_agent_event() {
        let session_id = "permission-malformed";
        let (result, session, events, _) = drive_permission_handshake_frames(
            session_id,
            [ChildFrame::Event {
                event: serde_json::json!({"type": "token", "content": 42}),
            }],
            expected_default_permission_posture(7),
        )
        .await;

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("malformed AgentEvent"));
        assert!(events.is_empty());
        assert!(bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata).is_none());

        let (result, _, events, _) = drive_permission_handshake_frames(
            session_id,
            [
                permission_posture_frame(session_id, 7),
                ChildFrame::Event {
                    event: serde_json::json!({"type":"private-download-secret"}),
                },
            ],
            expected_default_permission_posture(7),
        )
        .await;
        assert!(!result
            .unwrap_err()
            .to_string()
            .contains("private-download-secret"));
        assert_eq!(
            events.len(),
            1,
            "only the confirmed posture may be forwarded"
        );
    }

    #[tokio::test]
    async fn actor_permission_handshake_rejects_execution_event_before_posture() {
        let session_id = "permission-early-event";
        let early_events = [
            (
                "progress",
                AgentEvent::RunnerProgress {
                    session_id: session_id.to_string(),
                    round_count: 1,
                },
            ),
            (
                "token",
                AgentEvent::Token {
                    content: "must-not-forward".to_string(),
                },
            ),
            (
                "tool",
                AgentEvent::ToolStart {
                    tool_call_id: "early-tool".to_string(),
                    tool_name: "Read".to_string(),
                    arguments: serde_json::json!({"file_path": "README.md"}),
                },
            ),
        ];
        for (kind, event) in early_events {
            let (result, session, events, _) = drive_permission_handshake_frames(
                session_id,
                [
                    actor_event_frame(event),
                    permission_posture_frame(session_id, 7),
                    completed_actor_frame(),
                ],
                expected_default_permission_posture(7),
            )
            .await;

            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("execution event before permission posture confirmation"),
                "{kind} must fail closed before posture"
            );
            assert!(events.is_empty(), "{kind} must not be forwarded");
            assert!(
                bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata).is_none(),
                "{kind} must not advance the permission audit"
            );
        }
    }

    #[tokio::test]
    async fn actor_permission_handshake_rejects_approval_before_posture() {
        let session_id = "permission-early-approval";
        let (result, session, events, sent) = drive_permission_handshake_frames(
            session_id,
            [ChildFrame::ApprovalRequest {
                id: "approval-before-posture".to_string(),
                body: serde_json::json!({
                    "tool_name": "Bash",
                    "permission": "execute",
                    "resource": "echo must-not-run"
                }),
            }],
            expected_default_permission_posture(7),
        )
        .await;

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requested approval before permission posture confirmation"));
        assert!(events.is_empty());
        assert!(
            sent.is_empty(),
            "an unconfirmed actor must receive no approval reply"
        );
        assert!(bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata).is_none());
    }

    #[tokio::test]
    async fn actor_permission_handshake_rejects_mismatched_posture() {
        let session_id = "permission-mismatch";
        let (result, session, events, _) = drive_permission_handshake_frames(
            session_id,
            [permission_posture_frame(session_id, 8)],
            expected_default_permission_posture(7),
        )
        .await;

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("does not match the host-dispatched policy"));
        assert!(events.is_empty());
        assert!(bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata).is_none());
    }

    #[tokio::test]
    async fn actor_permission_handshake_rejects_untrusted_executor_mapping() {
        let session_id = "permission-hostile-mapping";
        for hostile_mapping in [
            "wrong_executor:permission_mode=default",
            "test_actor:permission_mode=default;credential=must-not-persist",
        ] {
            let frame = ChildFrame::Event {
                event: serde_json::to_value(AgentEvent::PermissionPostureActivated {
                    session_id: session_id.to_string(),
                    policy_revision: 7,
                    requested_mode: "default".to_string(),
                    effective_mode: "default".to_string(),
                    executor_mapping: hostile_mapping.to_string(),
                })
                .expect("serialize hostile posture fixture"),
            };
            let (result, session, events, _) = drive_permission_handshake_frames(
                session_id,
                [frame],
                expected_default_permission_posture(7),
            )
            .await;

            let error = result.unwrap_err().to_string();
            assert!(error.contains("host-dispatched executor mapping"));
            assert!(
                !error.contains(hostile_mapping),
                "untrusted mapping must not be reflected in host errors"
            );
            assert!(events.is_empty());
            assert!(
                bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata).is_none(),
                "untrusted mapping must not reach durable or in-memory audit state"
            );
        }
    }

    #[tokio::test]
    async fn actor_permission_handshake_rejects_duplicate_posture() {
        let session_id = "permission-duplicate";
        let (result, session, events, _) = drive_permission_handshake_frames(
            session_id,
            [
                permission_posture_frame(session_id, 7),
                permission_posture_frame(session_id, 7),
                completed_actor_frame(),
            ],
            expected_default_permission_posture(7),
        )
        .await;

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("duplicate permission posture activation"));
        assert_eq!(
            events.len(),
            1,
            "only the confirmed posture may be forwarded"
        );
        assert!(matches!(
            events[0],
            AgentEvent::PermissionPostureActivated { .. }
        ));
        let audit = bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata)
            .expect("the first matching posture must be recorded");
        assert_eq!(audit.policy_revision, 7);
    }

    #[tokio::test]
    async fn actor_permission_handshake_happy_path_persists_before_forwarding_execution() {
        let session_id = "permission-happy";
        let (result, session, events, _) = drive_permission_handshake_frames(
            session_id,
            [
                permission_posture_frame(session_id, 7),
                actor_event_frame(AgentEvent::RunnerProgress {
                    session_id: session_id.to_string(),
                    round_count: 1,
                }),
                actor_event_frame(AgentEvent::Token {
                    content: "working".to_string(),
                }),
                actor_event_frame(AgentEvent::ToolStart {
                    tool_call_id: "tool-1".to_string(),
                    tool_name: "Read".to_string(),
                    arguments: serde_json::json!({"file_path": "README.md"}),
                }),
                completed_actor_frame(),
            ],
            expected_default_permission_posture(7),
        )
        .await;

        assert_eq!(result.unwrap().as_deref(), Some("done"));
        assert!(matches!(
            events.as_slice(),
            [
                AgentEvent::PermissionPostureActivated { .. },
                AgentEvent::RunnerProgress { .. },
                AgentEvent::Token { .. },
                AgentEvent::ToolStart { .. }
            ]
        ));
        let audit = bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata)
            .expect("matching posture must be recorded before execution events are accepted");
        assert_eq!(audit.policy_revision, 7);
        assert_eq!(audit.executor_mapping, "test_actor:permission_mode=default");
    }

    #[tokio::test]
    async fn actor_download_tool_start_is_projected_before_parent_forwarding() {
        for (index, tool_name) in ["browser", "default::browser", "private-namespace::browser"]
            .into_iter()
            .enumerate()
        {
            let session_id = format!("actor-download-{index}");
            let args = serde_json::json!({
                "action":"download",
                "selector":"a[data-secret='private-selector']",
                "expected_epoch":17,
                "data_base64":"private-extra-bytes",
                "extra":{"url":"https://private.test/file"},
            });
            let original = args.clone();
            let read_args = serde_json::json!({"file_path":"README.md"});
            let (result, _session, events, _) = drive_permission_handshake_frames(
                &session_id,
                [
                    permission_posture_frame(&session_id, 7),
                    actor_event_frame(AgentEvent::ToolStart {
                        tool_call_id: "download-call".to_string(),
                        tool_name: tool_name.to_string(),
                        arguments: args.clone(),
                    }),
                    actor_event_frame(AgentEvent::ToolStart {
                        tool_call_id: "read-call".to_string(),
                        tool_name: "Read".to_string(),
                        arguments: read_args.clone(),
                    }),
                    completed_actor_frame(),
                ],
                expected_default_permission_posture(7),
            )
            .await;
            assert_eq!(result.expect("actor completed").as_deref(), Some("done"));
            assert_eq!(args, original);
            let starts: Vec<_> = events
                .iter()
                .filter_map(|event| match event {
                    AgentEvent::ToolStart {
                        tool_name,
                        arguments,
                        ..
                    } => Some((tool_name.as_str(), arguments)),
                    _ => None,
                })
                .collect();
            assert_eq!(starts.len(), 2);
            assert_eq!(starts[0].0, "browser");
            assert_eq!(
                starts[0].1,
                &serde_json::json!({"action":"download","expected_epoch":17})
            );
            assert_eq!(starts[1], ("Read", &read_args));
            assert!(!format!("{events:?}").contains("private"));
        }
    }

    #[tokio::test]
    async fn actor_hook_lifecycle_hides_every_decision_text_across_event_batch_and_nested_stream() {
        let session_id = "actor-hook-display";
        let private = "private-selector-url-filename-response";
        let hook_name = format!("audit-{private}");
        let hook = |duration_ms, phase: &str, decision| AgentEvent::HookLifecycle {
            hook_name: hook_name.clone(),
            point: bamboo_domain::AgentHookPoint::BeforeToolExecution,
            phase: phase.into(),
            duration_ms,
            decision,
        };
        let decisions = [
            HookResult::Deny {
                reason: private.into(),
            },
            HookResult::InjectContext {
                text: private.into(),
            },
            HookResult::WithContext {
                result: Box::new(HookResult::WithContext {
                    result: Box::new(HookResult::Deny {
                        reason: private.into(),
                    }),
                    text: private.into(),
                }),
                text: private.into(),
            },
            HookResult::Suspend {
                reason: private.into(),
            },
            HookResult::Abort {
                reason: private.into(),
            },
        ];
        let originals = decisions.to_vec();
        let batch_events = [
            hook(2, "completed", decisions[1].clone()),
            hook(3, "completed", decisions[2].clone()),
            hook(4, "completed", decisions[3].clone()),
        ]
        .into_iter()
        .map(|event| serde_json::to_value(event).unwrap())
        .collect::<Vec<_>>();
        let batch = ActorEventBatch {
            logical_session: Some(LogicalSessionIdentity {
                creation: None,
                session_id: session_id.into(),
                parent_session_id: Some("permission-parent".into()),
                root_session_id: session_id.into(),
            }),
            activation_id: None,
            execution_epoch: 0,
            source_node_id: None,
            source_actor_id: Some(session_id.into()),
            first_seq: 1,
            last_seq: batch_events.len() as u64,
            qos: bamboo_subagent::ActorEventQos::classify(&batch_events[0]),
            events: batch_events,
        };
        let nested = AgentEvent::SubAgentEvent {
            parent_session_id: "parent".into(),
            child_session_id: "child".into(),
            event: Box::new(hook(5, "completed", decisions[4].clone())),
        };
        let raw = serde_json::to_string(&originals).unwrap();
        assert!(raw.contains(private));

        let (outcome, _session, forwarded, _) = drive_permission_handshake_frames(
            session_id,
            [
                permission_posture_frame(session_id, 7),
                actor_event_frame(hook(1, private, decisions[0].clone())),
                ChildFrame::EventBatch { batch },
                actor_event_frame(nested),
                completed_actor_frame(),
            ],
            expected_default_permission_posture(7),
        )
        .await;
        assert_eq!(outcome.unwrap().as_deref(), Some("done"));
        let wire = serde_json::to_string(&forwarded).unwrap();
        assert!(
            !wire.contains(private),
            "private hook text reached SSE: {wire}"
        );

        let hooks = forwarded
            .iter()
            .filter_map(|event| match event {
                AgentEvent::HookLifecycle {
                    hook_name,
                    point,
                    phase,
                    duration_ms,
                    decision,
                } => Some((hook_name, point, phase, duration_ms, decision)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(hooks.len(), 4);
        assert!(hooks.iter().all(|(name, point, _, _, _)| {
            name == &&"actor-hook-1".to_string()
                && **point == bamboo_domain::AgentHookPoint::BeforeToolExecution
        }));
        assert_eq!(hooks[0].2, "event");
        assert_eq!(hooks[1].2, "completed");
        assert_eq!(*hooks[0].3, 1);
        assert_eq!(*hooks[3].3, 4);
        assert!(matches!(hooks[0].4, HookResult::Deny { reason } if reason == "Hook denied"));
        assert!(
            matches!(hooks[1].4, HookResult::InjectContext { text } if text == "Hook context hidden")
        );
        assert!(
            matches!(hooks[2].4, HookResult::WithContext { result, text }
            if text == "Hook context hidden" && matches!(result.as_ref(),
                HookResult::WithContext { result, text }
                    if text == "Hook context hidden" && matches!(result.as_ref(),
                        HookResult::Deny { reason } if reason == "Hook denied")))
        );
        assert!(matches!(hooks[3].4, HookResult::Suspend { reason } if reason == "Hook suspended"));
        assert!(forwarded.iter().any(|event| matches!(event,
            AgentEvent::SubAgentEvent { event, .. }
                if matches!(event.as_ref(), AgentEvent::HookLifecycle {
                    hook_name, duration_ms: 5,
                    decision: HookResult::Abort { reason }, ..
                } if hook_name == "actor-hook-1" && reason == "Hook aborted")
        )));
    }

    #[test]
    fn actor_hook_display_depth_and_identity_limits_fail_closed() {
        let mut display = ActorEventDisplay::default();
        let mut deep = HookResult::Allow;
        for _ in 0..=MAX_DISPLAY_HOOK_RESULT_DEPTH {
            deep = HookResult::WithContext {
                result: Box::new(deep),
                text: "private-download-bytes".into(),
            };
        }
        let event = |hook_name: String, decision| AgentEvent::HookLifecycle {
            hook_name,
            point: bamboo_domain::AgentHookPoint::BeforeToolExecution,
            phase: "completed".into(),
            duration_ms: 7,
            decision,
        };
        assert!(display.project(event("deep".into(), deep), 0).is_none());
        assert!(display.overflowed);
        assert!(display
            .project(
                event(
                    "private-download-bytes".into(),
                    HookResult::Deny {
                        reason: "private-download-bytes".into(),
                    },
                ),
                0,
            )
            .is_none());

        let mut oversized = ActorEventDisplay::default();
        assert!(oversized
            .project(
                event("x".repeat(MAX_DISPLAY_ID_BYTES + 1), HookResult::Allow),
                0,
            )
            .is_none());
        assert!(oversized.overflowed);

        // A new actor connection gets a fresh opaque alias, never the raw
        // hook name. Repeated names keep the same alias until the shared
        // identity budget is exhausted, then the stream fails closed.
        let mut reconnected = ActorEventDisplay::default();
        let private = "private-download-bytes";
        for _ in 0..2 {
            let projected = reconnected
                .project(event(private.into(), HookResult::Allow), 0)
                .unwrap();
            assert!(
                matches!(projected, AgentEvent::HookLifecycle { hook_name, .. }
                if hook_name == "actor-hook-1")
            );
        }
        assert_eq!(reconnected.identities.load(Ordering::Relaxed), 1);
        for index in 1..MAX_DISPLAY_CALLS {
            assert!(reconnected
                .project(event(format!("hook-{index}"), HookResult::Allow), 0)
                .is_some());
        }
        assert!(reconnected
            .project(event("overflow-private".into(), HookResult::Allow), 0)
            .is_none());
        assert!(reconnected.overflowed);
        assert!(reconnected.hook_names.is_empty());
    }

    #[test]
    fn actor_hook_display_preserves_nontext_decision_kinds() {
        for result in [
            HookResult::Continue,
            HookResult::Mutated,
            HookResult::Allow,
            HookResult::Ask,
        ] {
            assert_eq!(hook_result_for_display(result.clone(), 0), Some(result));
        }
    }

    #[tokio::test]
    async fn malformed_actor_hook_events_never_fall_back_to_raw_display() {
        let session_id = "actor-malformed-hook";
        let private = "private-download-selector";
        let original = AgentEvent::HookLifecycle {
            hook_name: "audit".into(),
            point: bamboo_domain::AgentHookPoint::BeforeToolExecution,
            phase: "completed".into(),
            duration_ms: 9,
            decision: HookResult::Deny {
                reason: private.into(),
            },
        };
        let mut invalid = serde_json::to_value(&original).unwrap();
        invalid["decision"]["type"] = serde_json::json!("unknown_decision");
        let nested = AgentEvent::SubAgentEvent {
            parent_session_id: "parent".into(),
            child_session_id: "child".into(),
            event: Box::new(original),
        };
        let mut invalid_nested = serde_json::to_value(nested).unwrap();
        invalid_nested["event"]["decision"]["type"] = serde_json::json!("unknown_decision");
        let batch = ActorEventBatch {
            logical_session: Some(LogicalSessionIdentity {
                creation: None,
                session_id: session_id.into(),
                parent_session_id: Some("permission-parent".into()),
                root_session_id: session_id.into(),
            }),
            activation_id: None,
            execution_epoch: 0,
            source_node_id: None,
            source_actor_id: Some(session_id.into()),
            first_seq: 1,
            last_seq: 1,
            qos: bamboo_subagent::ActorEventQos::classify(&invalid),
            events: vec![invalid.clone()],
        };
        for frame in [
            ChildFrame::Event {
                event: invalid.clone(),
            },
            ChildFrame::EventBatch { batch },
            ChildFrame::Event {
                event: invalid_nested,
            },
        ] {
            let (outcome, _session, forwarded, _) = drive_permission_handshake_frames(
                session_id,
                [permission_posture_frame(session_id, 7), frame],
                expected_default_permission_posture(7),
            )
            .await;
            let error = outcome.unwrap_err().to_string();
            assert!(error.contains("malformed AgentEvent"), "{error}");
            assert!(!error.contains(private));
            assert!(!serde_json::to_string(&forwarded).unwrap().contains(private));
        }

        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut session = Session::new("legacy-hook", "model");
        let mut handshake = PermissionPostureHandshake::NotRequired;
        let mut display = ActorEventDisplay::default();
        process_actor_event(
            invalid,
            false,
            &mut handshake,
            None,
            None,
            &mut session,
            &event_tx,
            &mut display,
        )
        .await
        .unwrap();
        assert!(display.overflowed);
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn actor_download_event_batch_hides_output_and_preserves_read_output() {
        let session_id = "actor-download-batch";
        let private = "private-download-bytes-and-url";
        let events = vec![
            AgentEvent::ToolStart {
                tool_call_id: "download-call".into(),
                tool_name: "default::browser".into(),
                arguments: serde_json::json!({
                    "action":"download", "selector": private, "expected_epoch": 17,
                }),
            },
            AgentEvent::ToolApprovalRequested {
                tool_call_id: "download-call".into(),
                tool_name: format!("{private}::browser"),
                parameters: serde_json::json!({
                    "action":"download", "selector": private, "expected_epoch": 17,
                }),
            },
            AgentEvent::ToolStart {
                tool_call_id: "read-call".into(),
                tool_name: "Read".into(),
                arguments: serde_json::json!({"file_path":"README.md"}),
            },
        ];
        let batch = ActorEventBatch {
            logical_session: Some(LogicalSessionIdentity {
                creation: None,
                session_id: session_id.into(),
                parent_session_id: Some("permission-parent".into()),
                root_session_id: session_id.into(),
            }),
            activation_id: None,
            execution_epoch: 0,
            source_node_id: None,
            source_actor_id: Some(session_id.into()),
            first_seq: 1,
            last_seq: events.len() as u64,
            qos: bamboo_subagent::ActorEventQos::Durable,
            events: events
                .into_iter()
                .map(|event| serde_json::to_value(event).unwrap())
                .collect(),
        };
        let result = bamboo_agent_core::tools::ToolResult {
            success: true,
            result: format!("{{\"data_base64\":\"{private}\"}}"),
            display_preference: Some(private.into()),
            images: vec![bamboo_agent_core::tools::ToolResultImage {
                mime_type: "image/png".into(),
                data: private.into(),
            }],
        };
        let (outcome, _, forwarded, _) = drive_permission_handshake_frames(
            session_id,
            [
                permission_posture_frame(session_id, 7),
                ChildFrame::EventBatch { batch },
                actor_event_frame(AgentEvent::ToolToken {
                    tool_call_id: "download-call".into(),
                    content: private.into(),
                }),
                actor_event_frame(AgentEvent::ToolToken {
                    tool_call_id: "read-call".into(),
                    content: "public live output".into(),
                }),
                actor_event_frame(AgentEvent::ToolComplete {
                    tool_call_id: "download-call".into(),
                    result,
                }),
                actor_event_frame(AgentEvent::ToolLifecycle {
                    tool_call_id: "download-call".into(),
                    tool_name: "browser".into(),
                    phase: "finished".into(),
                    elapsed_ms: Some(1),
                    is_mutating: false,
                    auto_approved: false,
                    summary: Some(private.into()),
                    error: Some(private.into()),
                }),
                actor_event_frame(AgentEvent::ToolError {
                    tool_call_id: "download-call".into(),
                    error: private.into(),
                }),
                actor_event_frame(AgentEvent::ToolComplete {
                    tool_call_id: "read-call".into(),
                    result: bamboo_agent_core::tools::ToolResult::text(true, "public README text"),
                }),
                completed_actor_frame(),
            ],
            expected_default_permission_posture(7),
        )
        .await;
        assert_eq!(outcome.unwrap().as_deref(), Some("done"));
        let wire = serde_json::to_string(&forwarded).unwrap();
        assert!(
            !wire.contains(private),
            "private content reached actor SSE: {wire}"
        );
        assert!(wire.contains("public README text"));
        assert!(wire.contains("public live output"));
        assert!(forwarded.iter().any(|event| matches!(event,
            AgentEvent::ToolComplete { tool_call_id, result }
                if tool_call_id == "download-call"
                && result.result == "Browser tool result hidden"
                && result.images.is_empty() && result.display_preference.is_none()
        )));
    }

    #[tokio::test]
    async fn actor_missing_reused_and_nested_tool_identity_fails_closed() {
        let private = "private-download-content";
        let nested = |event| AgentEvent::SubAgentEvent {
            parent_session_id: "parent".into(),
            child_session_id: "nested-child".into(),
            event: Box::new(event),
        };
        let (outcome, _, forwarded, _) = drive_permission_handshake_frames(
            "actor-reorder",
            [
                permission_posture_frame("actor-reorder", 7),
                actor_event_frame(AgentEvent::ToolComplete {
                    tool_call_id: "missing".into(),
                    result: bamboo_agent_core::tools::ToolResult::text(true, private),
                }),
                actor_event_frame(AgentEvent::ToolLifecycle {
                    tool_call_id: "missing".into(),
                    tool_name: "browser".into(),
                    phase: private.into(),
                    elapsed_ms: None,
                    is_mutating: false,
                    auto_approved: false,
                    summary: Some(private.into()),
                    error: Some(private.into()),
                }),
                actor_event_frame(AgentEvent::ToolApprovalRequested {
                    tool_call_id: "approval-only".into(),
                    tool_name: "browser".into(),
                    parameters: serde_json::json!({"action":"download","selector":private}),
                }),
                actor_event_frame(AgentEvent::ToolToken {
                    tool_call_id: "approval-only".into(),
                    content: private.into(),
                }),
                actor_event_frame(AgentEvent::ToolStart {
                    tool_call_id: "malformed-browser".into(),
                    tool_name: format!("{private}::browser"),
                    arguments: serde_json::json!({"selector":private}),
                }),
                actor_event_frame(AgentEvent::ToolComplete {
                    tool_call_id: "malformed-browser".into(),
                    result: bamboo_agent_core::tools::ToolResult::text(true, private),
                }),
                actor_event_frame(AgentEvent::ToolStart {
                    tool_call_id: "reused".into(),
                    tool_name: "Read".into(),
                    arguments: serde_json::json!({"file_path":"README.md"}),
                }),
                actor_event_frame(AgentEvent::ToolStart {
                    tool_call_id: "reused".into(),
                    tool_name: "browser".into(),
                    arguments: serde_json::json!({"action":"download","selector":private}),
                }),
                actor_event_frame(AgentEvent::ToolToken {
                    tool_call_id: "reused".into(),
                    content: private.into(),
                }),
                actor_event_frame(nested(AgentEvent::ToolStart {
                    tool_call_id: "nested".into(),
                    tool_name: "browser".into(),
                    arguments: serde_json::json!({"action":"download","selector":private}),
                })),
                actor_event_frame(nested(AgentEvent::ToolComplete {
                    tool_call_id: "nested".into(),
                    result: bamboo_agent_core::tools::ToolResult::text(true, private),
                })),
                actor_event_frame(AgentEvent::ChildApprovalRequested {
                    child_session_id: "nested-child".into(),
                    request_id: "approval".into(),
                    tool_name: format!("{private}::browser"),
                    permission: private.into(),
                    resource: private.into(),
                }),
                actor_event_frame(AgentEvent::ChildApprovalChanged {
                    parent_session_id: "parent".into(),
                    child_session_id: "nested-child".into(),
                    child_attempt: 1,
                    request_id: "approval".into(),
                    version: 1,
                    status: "pending".into(),
                    reason: Some(private.into()),
                    tool_name: "browser".into(),
                    permission: private.into(),
                    resource: private.into(),
                    created_at: "2026-01-01T00:00:00Z".into(),
                    resolved_at: None,
                }),
                completed_actor_frame(),
            ],
            expected_default_permission_posture(7),
        )
        .await;
        assert_eq!(outcome.unwrap().as_deref(), Some("done"));
        let wire = serde_json::to_string(&forwarded).unwrap();
        assert!(
            !wire.contains(private),
            "private content reached actor SSE: {wire}"
        );
        assert!(wire.contains("Tool result hidden"));
        assert!(wire.contains("Browser tool result hidden"));
        assert!(wire.contains("Browser interaction approval"));
        assert!(!forwarded
            .iter()
            .any(|event| matches!(event, AgentEvent::ChildApprovalChanged { .. })));
    }

    #[test]
    fn actor_display_tracking_overflow_never_reclassifies_old_private_id() {
        let mut display = ActorEventDisplay::default();
        display
            .project(
                AgentEvent::ToolStart {
                    tool_call_id: "private-call".into(),
                    tool_name: "browser".into(),
                    arguments: serde_json::json!({"action":"download","selector":"private"}),
                },
                0,
            )
            .unwrap();
        for index in 0..MAX_DISPLAY_CALLS {
            display
                .project(
                    AgentEvent::ToolStart {
                        tool_call_id: format!("ordinary-{index}"),
                        tool_name: "Read".into(),
                        arguments: serde_json::json!({"file_path":"README.md"}),
                    },
                    0,
                )
                .unwrap();
        }
        assert!(display.overflowed);
        let forwarded = display
            .project(
                AgentEvent::ToolComplete {
                    tool_call_id: "private-call".into(),
                    result: bamboo_agent_core::tools::ToolResult::text(true, "private-bytes"),
                },
                0,
            )
            .unwrap();
        assert!(!serde_json::to_string(&forwarded)
            .unwrap()
            .contains("private-bytes"));
    }

    #[test]
    fn nested_actor_display_uses_one_identity_budget_and_bounded_keys() {
        let mut display = ActorEventDisplay::default();
        let nested = |index: usize| AgentEvent::SubAgentEvent {
            parent_session_id: "parent".into(),
            child_session_id: format!("child-{index}"),
            event: Box::new(AgentEvent::ToolStart {
                tool_call_id: format!("read-{index}"),
                tool_name: "Read".into(),
                arguments: serde_json::json!({"file_path":"README.md"}),
            }),
        };
        for index in 0..(MAX_DISPLAY_CALLS / 2) {
            assert!(display.project(nested(index), 0).is_some());
        }
        assert_eq!(
            display.identities.load(Ordering::Relaxed),
            MAX_DISPLAY_CALLS
        );
        assert!(display.project(nested(MAX_DISPLAY_CALLS / 2), 0).is_none());
        assert!(display.overflowed);
        assert!(display.children.is_empty());

        let mut oversized = ActorEventDisplay::default();
        assert!(oversized
            .project(
                AgentEvent::SubAgentEvent {
                    parent_session_id: "parent".into(),
                    child_session_id: "x".repeat(MAX_DISPLAY_ID_BYTES + 1),
                    event: Box::new(AgentEvent::ToolComplete {
                        tool_call_id: "call".into(),
                        result: bamboo_agent_core::tools::ToolResult::text(
                            true,
                            "private-download-bytes",
                        ),
                    }),
                },
                0,
            )
            .is_none());
        assert!(oversized.overflowed);
        assert!(oversized
            .project(
                AgentEvent::ToolStart {
                    tool_call_id: "x".repeat(MAX_DISPLAY_ID_BYTES + 1),
                    tool_name: "Read".into(),
                    arguments: serde_json::json!({"file_path":"README.md"}),
                },
                0,
            )
            .is_none());
    }

    #[tokio::test]
    async fn actor_batch_sequence_gap_invalidates_prior_tool_identity() {
        let session_id = "actor-gap";
        let batch = |seq, event: AgentEvent| {
            let event = serde_json::to_value(event).unwrap();
            ChildFrame::EventBatch {
                batch: ActorEventBatch {
                    logical_session: Some(LogicalSessionIdentity {
                        creation: None,
                        session_id: session_id.into(),
                        parent_session_id: Some("permission-parent".into()),
                        root_session_id: session_id.into(),
                    }),
                    activation_id: None,
                    execution_epoch: 0,
                    source_node_id: None,
                    source_actor_id: Some(session_id.into()),
                    first_seq: seq,
                    last_seq: seq,
                    qos: bamboo_subagent::ActorEventQos::classify(&event),
                    events: vec![event],
                },
            }
        };
        let (outcome, _, forwarded, _) = drive_permission_handshake_frames(
            session_id,
            [
                permission_posture_frame(session_id, 7),
                batch(
                    1,
                    AgentEvent::ToolStart {
                        tool_call_id: "reused-call".into(),
                        tool_name: "Read".into(),
                        arguments: serde_json::json!({"file_path":"README.md"}),
                    },
                ),
                // Sequence 2 may have replaced this ID with a private browser
                // call. The actor's next result must not inherit Read display.
                batch(
                    3,
                    AgentEvent::ToolComplete {
                        tool_call_id: "reused-call".into(),
                        result: bamboo_agent_core::tools::ToolResult::text(
                            true,
                            "private-download-bytes",
                        ),
                    },
                ),
                completed_actor_frame(),
            ],
            expected_default_permission_posture(7),
        )
        .await;
        assert_eq!(outcome.unwrap().as_deref(), Some("done"));
        let wire = serde_json::to_string(&forwarded).unwrap();
        assert!(!wire.contains("private-download-bytes"), "{wire}");
        assert!(wire.contains("Tool result hidden"));
    }

    #[tokio::test]
    async fn legacy_malformed_actor_event_invalidates_prior_tool_identity() {
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut session = Session::new("legacy-malformed-private", "model");
        let mut handshake = PermissionPostureHandshake::NotRequired;
        let mut display = ActorEventDisplay::default();
        let read_start = serde_json::to_value(AgentEvent::ToolStart {
            tool_call_id: "reused-call".into(),
            tool_name: "Read".into(),
            arguments: serde_json::json!({"file_path":"README.md"}),
        })
        .unwrap();
        process_actor_event(
            read_start,
            false,
            &mut handshake,
            None,
            None,
            &mut session,
            &event_tx,
            &mut display,
        )
        .await
        .unwrap();

        // A mixed-version actor may send a browser ToolStart whose name has
        // the wrong wire type. Serde drops it; the reused ID is no longer safe.
        let mut malformed = serde_json::to_value(AgentEvent::ToolStart {
            tool_call_id: "reused-call".into(),
            tool_name: "browser".into(),
            arguments: serde_json::json!({
                "action":"download", "selector":"private-download-selector"
            }),
        })
        .unwrap();
        malformed["tool_name"] = serde_json::json!(7);
        process_actor_event(
            malformed,
            false,
            &mut handshake,
            None,
            None,
            &mut session,
            &event_tx,
            &mut display,
        )
        .await
        .unwrap();
        process_actor_event(
            serde_json::to_value(AgentEvent::ToolComplete {
                tool_call_id: "reused-call".into(),
                result: bamboo_agent_core::tools::ToolResult::text(true, "private-download-bytes"),
            })
            .unwrap(),
            false,
            &mut handshake,
            None,
            None,
            &mut session,
            &event_tx,
            &mut display,
        )
        .await
        .unwrap();
        let forwarded = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(forwarded.len(), 2);
        let wire = serde_json::to_string(&forwarded).unwrap();
        assert!(!wire.contains("private-download"), "{wire}");
        assert!(wire.contains("Tool result hidden"));
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
            parent_question_lock: None,
        };
        let pairs = claim_canonical_deliveries(&binding, &mut session, run_id, usize::MAX, None)
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
            expected_creation: None,
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
            expected_permission_posture: None,
            session_inbox_runtime: Some(&binding),
            actor_directory_store: None,
            canonical_subagent_tool: None,
            activation_run_id: Some(run_id),
            execution_epoch: 0,
            expected_source_actor_id: session_id,
            initial_inflight_claims: claims,
            plain_actor: false,
            remote_environment_lease: false,
            readonly_output: None,
            local_history_tools: None,
            local_history_read_only: false,
            plain_input: None,
            canonical_activation: None,
            canonical_placement_ref: None,
            actor_event_observer: None,
            plain_run: None,
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

    fn permission_resolution(
        requested: bamboo_domain::SessionPermissionMode,
        effective: bamboo_domain::PermissionMode,
    ) -> bamboo_domain::PermissionModeResolution {
        bamboo_domain::PermissionModeResolution {
            requested,
            effective,
        }
    }

    #[test]
    fn permission_posture_mapping_contract_is_exact_for_supported_executors() {
        use bamboo_domain::{PermissionMode, SessionPermissionMode};

        let default =
            permission_resolution(SessionPermissionMode::Default, PermissionMode::Default);
        assert_eq!(
            expected_permission_executor_mapping(&ExecutorSpec::BambooRuntime, default, false)
                .unwrap()
                .as_deref(),
            Some("bamboo_runtime:default")
        );
        assert_eq!(
            expected_permission_executor_mapping(&ExecutorSpec::Echo, default, false).unwrap(),
            None,
            "transport-only Echo must not claim the typed permission contract"
        );
        assert_eq!(
            expected_permission_executor_mapping(
                &ExecutorSpec::CliAdapter {
                    command: "must-not-appear-in-contract".to_string(),
                    args: vec!["credential-like-argument".to_string()],
                },
                default,
                false,
            )
            .unwrap(),
            None,
            "unimplemented CliAdapter must not leak command data into a contract"
        );

        let claude = ExecutorSpec::ClaudeCode {
            binary: None,
            model: None,
            permission_mode: Some("default".to_string()),
            inherit_user_config: None,
            forward_env: None,
        };
        for (resolution, mapping) in [
            (
                permission_resolution(SessionPermissionMode::Default, PermissionMode::Plan),
                "claude_code:permission_mode=plan",
            ),
            (
                permission_resolution(SessionPermissionMode::Auto, PermissionMode::Auto),
                "claude_code:permission_mode=bypassPermissions",
            ),
            (
                permission_resolution(SessionPermissionMode::Default, PermissionMode::AcceptEdits),
                "claude_code:permission_mode=acceptEdits",
            ),
            (
                permission_resolution(SessionPermissionMode::Default, PermissionMode::DontAsk),
                "claude_code:permission_mode=dontAsk",
            ),
            (
                permission_resolution(
                    SessionPermissionMode::Bypass,
                    PermissionMode::BypassPermissions,
                ),
                "claude_code:permission_mode=default",
            ),
        ] {
            assert_eq!(
                expected_permission_executor_mapping(&claude, resolution, false)
                    .unwrap()
                    .as_deref(),
                Some(mapping)
            );
        }
        assert_eq!(
            expected_permission_executor_mapping(&claude, default, true)
                .unwrap()
                .as_deref(),
            Some("claude_code:blocked_explicit_deny")
        );

        let mut codex_exec = codex_executor(Some("inherit"), Some(true));
        if let ExecutorSpec::Codex {
            approval_policy, ..
        } = &mut codex_exec
        {
            *approval_policy = Some("on-failure".to_string());
        }
        assert_eq!(
            expected_permission_executor_mapping(&codex_exec, default, false)
                .unwrap()
                .as_deref(),
            Some("codex_exec:approval_policy=on-failure")
        );
        assert_eq!(
            expected_permission_executor_mapping(
                &codex_exec,
                permission_resolution(SessionPermissionMode::Auto, PermissionMode::Auto),
                false,
            )
            .unwrap()
            .as_deref(),
            Some("codex_exec:approval_policy=never")
        );
        assert_eq!(
            expected_permission_executor_mapping(&codex_exec, default, true)
                .unwrap()
                .as_deref(),
            Some("codex_exec:blocked_explicit_deny")
        );

        let mut codex_app_server = codex_executor(Some("inherit"), Some(true));
        if let ExecutorSpec::Codex {
            mode,
            approval_policy,
            ..
        } = &mut codex_app_server
        {
            *mode = Some("app_server".to_string());
            *approval_policy = Some("on-request".to_string());
        }
        assert_eq!(
            expected_permission_executor_mapping(&codex_app_server, default, false)
                .unwrap()
                .as_deref(),
            Some("codex_app_server:approvalPolicy=on-request")
        );
        assert_eq!(
            expected_permission_executor_mapping(
                &codex_app_server,
                permission_resolution(SessionPermissionMode::Auto, PermissionMode::Auto),
                false,
            )
            .unwrap()
            .as_deref(),
            Some("codex_app_server:approvalPolicy=never")
        );
        assert_eq!(
            expected_permission_executor_mapping(&codex_app_server, default, true)
                .unwrap()
                .as_deref(),
            Some("codex_app_server:blocked_explicit_deny")
        );
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

    #[tokio::test]
    async fn live_provider_config_refreshes_credentials_and_default_per_activation() {
        fn provider_instance(
            provider_type: &str,
            api_key: &str,
        ) -> bamboo_config::ProviderInstanceConfig {
            bamboo_config::ProviderInstanceConfig {
                provider_type: provider_type.to_string(),
                label: None,
                api_key: api_key.to_string(),
                api_key_encrypted: None,
                credential_ref: None,
                base_url: None,
                model: None,
                fast_model: None,
                vision_model: None,
                reasoning_effort: None,
                responses_only_models: Vec::new(),
                request_overrides: None,
                enabled: true,
                extra: Default::default(),
            }
        }

        let mut initial = bamboo_llm::Config::default();
        initial.provider_instances.insert(
            "old-provider".to_string(),
            provider_instance("openai", "old-key"),
        );
        initial.default_provider_instance = Some("old-provider".to_string());
        let live = Arc::new(tokio::sync::RwLock::new(initial));

        let runner = ActorChildRunner::new(
            "echo-live-config-test".to_string(),
            PathBuf::from("/bin/false"),
            Vec::new(),
            std::env::temp_dir().join("bamboo-echo-live-config"),
            ExecutorSpec::Echo,
            Vec::new(),
            "stale-provider".to_string(),
            1,
        )
        .with_live_provider_config(live.clone());
        let job = crate::runtime::execution::SpawnJob {
            parent_session_id: "parent".to_string(),
            child_session_id: "child".to_string(),
            model: "model".to_string(),
            disabled_tools: None,
        };

        let first = runner
            .build_live_spec(&Session::new("child", "model"), &job)
            .await;
        assert_eq!(
            first.model.as_ref().map(|model| model.provider.as_str()),
            Some("old-provider")
        );
        assert_eq!(first.secrets.provider_credentials.len(), 1);
        assert_eq!(first.secrets.provider_credentials[0].api_key, "old-key");

        let mut reloaded = bamboo_llm::Config::default();
        reloaded.provider_instances.insert(
            "new-provider".to_string(),
            provider_instance("openai", "new-key"),
        );
        reloaded.default_provider_instance = Some("new-provider".to_string());
        *live.write().await = reloaded;

        let second = runner
            .build_live_spec(&Session::new("child", "model"), &job)
            .await;
        assert_eq!(
            second.model.as_ref().map(|model| model.provider.as_str()),
            Some("new-provider")
        );
        assert_eq!(second.secrets.provider_credentials.len(), 1);
        assert_eq!(second.secrets.provider_credentials[0].api_key, "new-key");
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
            creation: None,
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
        let mut original = spec_with("worker", "provider", "model", Some("/ws"), None);
        original.executor = ExecutorSpec::BambooRuntime;
        original.identity.depth = session.spawn_depth;
        original.capabilities.required_child_context = true;
        original.capabilities.child_creation_identity = true;
        original.capabilities.enforce_permissions = true;
        original.capabilities.native_tool_ceiling_required = true;
        original.capabilities.native_tool_ceiling =
            Some(bamboo_subagent::proto::NativeToolCeiling {
                version: 1,
                child_session_id: session.id.clone(),
                parent_session_id: job.parent_session_id.clone(),
                root_session_id: session.root_session_id.clone(),
                created_at: session.created_at,
                spawn_depth: session.spawn_depth,
                project_id: None,
                tools: vec![],
            });
        original.validate().unwrap();
        let first = ActorChildRunner::fresh_required_worker_spec(&original);
        let second = ActorChildRunner::fresh_required_worker_spec(&original);
        assert_ne!(first.identity.child_id, second.identity.child_id);
        assert_ne!(first.identity.child_id, original.identity.child_id);
        for physical in [first, second] {
            physical.validate().unwrap();
            let mut actual = serde_json::to_value(&physical).unwrap();
            actual["identity"]["child_id"] = serde_json::json!(original.identity.child_id);
            assert_eq!(actual, serde_json::to_value(&original).unwrap());
            assert_eq!(logical_identity_for_actor_run(&session, &job), expected);
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
    fn fingerprint_splits_rotated_provider_credentials_without_exposing_them() {
        let mut before = spec_with("explorer", "provider", "model", Some("/ws"), None);
        before.secrets.provider_credentials.push(ScopedCredential {
            provider: "provider".to_string(),
            api_key: "old-secret-key".to_string(),
            base_url: Some("https://provider.example/v1".to_string()),
            provider_type: Some("openai".to_string()),
            credential_ref: None,
        });
        let mut after = before.clone();
        after.secrets.provider_credentials[0].api_key = "new-secret-key".to_string();

        let before_fingerprint = ActorChildRunner::fingerprint(&before);
        let after_fingerprint = ActorChildRunner::fingerprint(&after);
        assert_ne!(before_fingerprint, after_fingerprint);
        assert!(!before_fingerprint.contains("old-secret-key"));
        assert!(!after_fingerprint.contains("new-secret-key"));
    }

    #[test]
    fn fingerprint_splits_on_baked_capabilities() {
        // Every capability baked once at provision time must split the pool
        // bucket, else a worker baked for one posture gets reused for another
        // (e.g. a depth-1 worker re-stamping spawn_depth onto a depth-4 child,
        // breaking the depth cap; or a bypass worker reused for a non-bypass one).
        let base_fp =
            ActorChildRunner::fingerprint(&spec_with("explorer", "p", "m", Some("/ws"), None));

        let mut creation = spec_with("explorer", "p", "m", Some("/ws"), None);
        creation.capabilities.child_creation_identity = true;
        let typed = ActorChildRunner::fingerprint(&creation);
        assert_ne!(
            base_fp, typed,
            "legacy worker pool cannot satisfy birth-required Run"
        );
        creation.identity.child_id = "different-logical-child".into();
        assert_eq!(
            typed,
            ActorChildRunner::fingerprint(&creation),
            "birth-required mode retains cross-Child pool reuse"
        );

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

        let mut auto = spec_with("explorer", "p", "m", Some("/ws"), None);
        auto.capabilities.auto_approve_permissions = true;
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&auto),
            "auto_approve_permissions must split"
        );

        let mut global_auto = spec_with("explorer", "p", "m", Some("/ws"), None);
        global_auto.capabilities.permission_requested_mode = "default".to_string();
        global_auto.capabilities.permission_effective_mode = "auto".to_string();
        global_auto.capabilities.auto_approve_permissions = true;
        let mut explicit_auto = global_auto.clone();
        explicit_auto.capabilities.permission_requested_mode = "auto".to_string();
        assert_ne!(
            ActorChildRunner::fingerprint(&global_auto),
            ActorChildRunner::fingerprint(&explicit_auto),
            "permission_requested_mode must split global and explicit Auto"
        );

        let mut plan_overlay = explicit_auto.clone();
        plan_overlay.capabilities.permission_effective_mode = "plan".to_string();
        assert_ne!(
            ActorChildRunner::fingerprint(&explicit_auto),
            ActorChildRunner::fingerprint(&plan_overlay),
            "permission_effective_mode must split Plan overlay from Auto"
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

        // The generic read-only no-shell checker is baked once at build(), so a
        // planner/Guardian worker must not be reused for an ordinary child.
        let mut read_only = spec_with("explorer", "p", "m", Some("/ws"), None);
        read_only.capabilities.read_only = true;
        assert_ne!(
            base_fp,
            ActorChildRunner::fingerprint(&read_only),
            "read_only must split"
        );

        // Preserve the legacy Guardian bit as its own rolling-upgrade axis.
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
                expected_creation: None,
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
                expected_permission_posture: None,
                session_inbox_runtime: None,
                actor_directory_store: None,
                canonical_subagent_tool: None,
                activation_run_id: None,
                execution_epoch: 0,
                expected_source_actor_id: "child-reviewer",
                initial_inflight_claims: VecDeque::new(),
                plain_actor: false,
                remote_environment_lease: false,
                readonly_output: None,
                local_history_tools: None,
                local_history_read_only: false,
                plain_input: None,
                canonical_activation: None,
                canonical_placement_ref: None,
                actor_event_observer: None,
                plain_run: None,
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
                expected_creation: None,
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
                expected_permission_posture: None,
                session_inbox_runtime: None,
                actor_directory_store: None,
                canonical_subagent_tool: None,
                activation_run_id: None,
                execution_epoch: 0,
                expected_source_actor_id: "child-no-reviewer",
                initial_inflight_claims: VecDeque::new(),
                plain_actor: false,
                remote_environment_lease: false,
                readonly_output: None,
                local_history_tools: None,
                local_history_read_only: false,
                plain_input: None,
                canonical_activation: None,
                canonical_placement_ref: None,
                actor_event_observer: None,
                plain_run: None,
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
            expected_creation: None,
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
            expected_permission_posture: None,
            session_inbox_runtime: None,
            actor_directory_store: None,
            canonical_subagent_tool: None,
            activation_run_id: None,
            execution_epoch: 0,
            expected_source_actor_id: "child-x",
            initial_inflight_claims: VecDeque::new(),
            plain_actor: false,
            remote_environment_lease: false,
            readonly_output: None,
            local_history_tools: None,
            local_history_read_only: false,
            plain_input: None,
            canonical_activation: None,
            canonical_placement_ref: None,
            actor_event_observer: None,
            plain_run: None,
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
            expected_creation: None,
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
            expected_permission_posture: None,
            session_inbox_runtime: None,
            actor_directory_store: None,
            canonical_subagent_tool: None,
            activation_run_id: None,
            execution_epoch: 0,
            expected_source_actor_id: "child-y",
            initial_inflight_claims: VecDeque::new(),
            plain_actor: false,
            remote_environment_lease: false,
            readonly_output: None,
            local_history_tools: None,
            local_history_read_only: false,
            plain_input: None,
            canonical_activation: None,
            canonical_placement_ref: None,
            actor_event_observer: None,
            plain_run: None,
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
                broker_peer: None,
                requirements: None,
            },
        );
        let runner = bogus_runner(placements);

        // The broker credential never rides the child RunSpec secrets envelope.
        let s = session_of_role("explorer", "do the thing");
        let spec = runner.build_spec(&s, &job_for("child-1"));
        match &spec.placement {
            Placement::Remote { endpoint } => assert_eq!(endpoint, "wss://gpu-host:8443"),
            other => panic!("expected Remote, got {other:?}"),
        }
        assert!(spec.secrets.worker_auth_token.is_none());
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
                broker_peer: None,
                requirements: None,
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
    fn typed_child_state_is_the_read_only_authority_under_auto_and_bypass() {
        for mode in [
            bamboo_domain::SessionPermissionMode::Auto,
            bamboo_domain::SessionPermissionMode::Bypass,
        ] {
            let runner = bogus_runner(HashMap::new());
            let mut child = session_of_role("planner", "inspect and plan");
            let runtime = child
                .agent_runtime_state
                .get_or_insert_with(bamboo_domain::AgentRuntimeState::default);
            runtime.set_permission_mode(mode);
            runtime.read_only = true;

            let spec = runner.build_spec(&child, &job_for("read-only-child"));

            assert!(spec.capabilities.read_only);
            assert!(spec.capabilities.read_only_enforced());
            assert!(
                spec.capabilities.guardian_read_only,
                "the compatibility alias must protect planners on older workers"
            );
            assert_eq!(spec.capabilities.permission_requested_mode, mode.as_str());
            assert_eq!(spec.capabilities.permission_effective_mode, "plan");
            assert!(
                !spec.capabilities.bypass,
                "read-only must win over requested {mode:?}"
            );
            assert_eq!(
                spec.capabilities.auto_approve_permissions,
                mode == bamboo_domain::SessionPermissionMode::Auto,
                "Auto may suppress prompts but must not widen Plan authorization"
            );
            let disabled = spec
                .disabled_tools
                .as_ref()
                .expect("read-only state must provision a host-owned denylist");
            for tool_name in [
                "Edit",
                "Write",
                "Plan",
                "SubAgent",
                "session_control",
                "request_permissions",
            ] {
                assert!(
                    disabled.iter().any(|name| name == tool_name),
                    "read-only provisioning must disable {tool_name}"
                );
            }
        }

        let runner = bogus_runner(HashMap::new());
        let cosmetic_planner = runner.build_spec(
            &session_of_role("planner", "label only"),
            &job_for("cosmetic"),
        );
        assert!(
            !cosmetic_planner.capabilities.read_only_enforced(),
            "a cosmetic role label must not change authority"
        );
        assert!(cosmetic_planner.disabled_tools.is_none());

        let cosmetic_guardian = runner.build_spec(
            &session_of_role("guardian", "label only"),
            &job_for("cosmetic-guardian"),
        );
        assert!(
            !cosmetic_guardian.capabilities.read_only_enforced(),
            "a Guardian label without typed state or the legacy denylist is cosmetic"
        );

        let legacy_guardian = session_of_role("guardian", "pre-upgrade reviewer");
        let mut legacy_job = job_for("legacy-guardian");
        legacy_job.disabled_tools = Some(vec![
            "Edit".to_string(),
            "Write".to_string(),
            "SubAgent".to_string(),
            "WebFetch".to_string(),
        ]);
        let legacy_spec = runner.build_spec(&legacy_guardian, &legacy_job);
        assert!(
            legacy_spec.capabilities.read_only_enforced(),
            "an in-flight pre-upgrade Guardian keeps its read-only authority"
        );

        let mut differently_named = session_of_role("explorer", "typed authority");
        differently_named
            .agent_runtime_state
            .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
            .read_only = true;
        let typed = runner.build_spec(&differently_named, &job_for("typed"));
        assert!(
            typed.capabilities.read_only_enforced(),
            "typed state must enforce read-only independently of role"
        );
    }

    #[test]
    fn typed_read_only_forces_native_external_executor_profiles() {
        let mut planner = session_of_role("planner", "inspect and plan");
        planner
            .agent_runtime_state
            .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
            .read_only = true;

        let claude_runner = ActorChildRunner::new(
            "test-actor".into(),
            PathBuf::from("/bin/false"),
            vec![],
            std::env::temp_dir().join("bamboo-test-plan-claude"),
            ExecutorSpec::ClaudeCode {
                binary: None,
                model: None,
                permission_mode: Some("bypassPermissions".to_string()),
                inherit_user_config: Some(true),
                forward_env: None,
            },
            vec![],
            "anthropic".into(),
            4,
        );
        let claude_spec = claude_runner.build_spec(&planner, &job_for("planner-claude"));
        let disabled = claude_spec
            .disabled_tools
            .as_ref()
            .expect("typed read-only child must receive a host-owned denylist");
        for tool in ["Bash", "BashInput", "BashOutput", "KillShell"] {
            assert!(
                disabled.iter().any(|name| name == tool),
                "typed read-only child must not advertise {tool}"
            );
        }
        assert!(matches!(
            claude_spec.executor,
            ExecutorSpec::ClaudeCode {
                permission_mode: Some(ref mode),
                inherit_user_config: Some(false),
                ..
            } if mode == "plan"
        ));
        assert_eq!(claude_spec.capabilities.permission_effective_mode, "plan");

        let mut codex_exec = codex_executor(Some("inherit"), Some(true));
        if let ExecutorSpec::Codex {
            sandbox,
            approval_policy,
            network_access,
            allow_danger_bypass,
            permission_profile,
            ..
        } = &mut codex_exec
        {
            *sandbox = Some("danger-full-access".to_string());
            *approval_policy = Some("on-failure".to_string());
            *network_access = Some(true);
            *allow_danger_bypass = Some(true);
            *permission_profile = Some("full-access".to_string());
        }
        let codex_runner = ActorChildRunner::new(
            "test-actor".into(),
            PathBuf::from("/bin/false"),
            vec![],
            std::env::temp_dir().join("bamboo-test-plan-codex"),
            codex_exec,
            vec![],
            "openai".into(),
            4,
        );
        let codex_spec = codex_runner.build_spec(&planner, &job_for("planner-codex"));
        assert_eq!(
            expected_permission_executor_mapping(
                &codex_spec.executor,
                codex_spec.capabilities.permission_resolution().unwrap(),
                false,
            )
            .unwrap()
            .as_deref(),
            Some("codex_exec:approval_policy=never")
        );
        assert!(matches!(
            codex_spec.executor,
            ExecutorSpec::Codex {
                sandbox: Some(ref sandbox),
                inherit_user_config: Some(false),
                approval_policy: Some(ref approval_policy),
                network_access: Some(false),
                allow_danger_bypass: Some(false),
                permission_profile: Some(ref profile),
                ..
            } if sandbox == "read-only"
                && approval_policy == "never"
                && profile == "read-only"
        ));

        let mut app_server = codex_executor(Some("inherit"), Some(true));
        if let ExecutorSpec::Codex {
            mode,
            sandbox,
            approval_policy,
            network_access,
            allow_danger_bypass,
            permission_profile,
            ..
        } = &mut app_server
        {
            *mode = Some("app_server".to_string());
            *sandbox = Some("danger-full-access".to_string());
            *approval_policy = Some("on-request".to_string());
            *network_access = Some(true);
            *allow_danger_bypass = Some(true);
            *permission_profile = Some("full-access".to_string());
        }
        let app_server_runner = ActorChildRunner::new(
            "test-actor".into(),
            PathBuf::from("/bin/false"),
            vec![],
            std::env::temp_dir().join("bamboo-test-plan-codex-app-server"),
            app_server,
            vec![],
            "openai".into(),
            4,
        );
        let app_server_spec =
            app_server_runner.build_spec(&planner, &job_for("planner-codex-app-server"));
        assert_eq!(
            expected_permission_executor_mapping(
                &app_server_spec.executor,
                app_server_spec
                    .capabilities
                    .permission_resolution()
                    .unwrap(),
                false,
            )
            .unwrap()
            .as_deref(),
            Some("codex_app_server:approvalPolicy=never")
        );
        assert!(matches!(
            app_server_spec.executor,
            ExecutorSpec::Codex {
                mode: Some(ref mode),
                sandbox: Some(ref sandbox),
                inherit_user_config: Some(false),
                approval_policy: Some(ref approval_policy),
                network_access: Some(false),
                allow_danger_bypass: Some(false),
                permission_profile: Some(ref profile),
                ..
            } if mode == "app_server"
                && sandbox == "read-only"
                && approval_policy == "on-request"
                && profile == "read-only"
        ));
    }

    #[test]
    fn typed_read_only_rejects_both_codex_executor_modes() {
        let mut planner = session_of_role("planner", "inspect and plan");
        planner
            .agent_runtime_state
            .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
            .read_only = true;

        for mode in ["exec", "app_server"] {
            let mut executor = codex_executor(Some("inherit"), Some(true));
            if let ExecutorSpec::Codex {
                mode: configured_mode,
                ..
            } = &mut executor
            {
                *configured_mode = Some(mode.to_string());
            }
            let runner = ActorChildRunner::new(
                "test-actor".into(),
                PathBuf::from("/bin/false"),
                vec![],
                std::env::temp_dir().join(format!("bamboo-test-plan-codex-{mode}")),
                executor,
                vec![],
                "openai".into(),
                4,
            );
            let spec = runner.build_spec(&planner, &job_for(&format!("planner-codex-{mode}")));
            let error = ensure_read_only_activation_is_enforceable(&spec)
                .expect_err("Codex command execution must fail closed for read-only activations");
            let message = error.to_string();
            assert!(message.contains("Codex executor"), "{message}");
            assert!(message.contains("command execution"), "{message}");
        }
    }

    #[tokio::test]
    async fn typed_read_only_codex_fails_before_worker_spawn_or_dispatch() {
        let mut planner = session_of_role("planner", "inspect and plan");
        planner
            .agent_runtime_state
            .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
            .read_only = true;

        let runner = ActorChildRunner::new(
            "test-actor".into(),
            PathBuf::from("/bin/false"),
            vec![],
            std::env::temp_dir().join("bamboo-test-plan-codex-no-spawn"),
            codex_executor(Some("inherit"), Some(true)),
            vec![],
            "openai".into(),
            4,
        );
        let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(4);
        let error = runner
            .execute_external_child(
                &mut planner,
                &job_for("planner-codex-no-spawn"),
                event_tx,
                CancellationToken::new(),
            )
            .await
            .expect_err("typed read-only Codex must fail before invoking /bin/false");
        let message = error.to_string();
        assert!(message.contains("Codex executor"), "{message}");
        assert!(message.contains("command execution"), "{message}");
    }

    #[test]
    fn typed_read_only_rejects_resident_actor_placements() {
        let runner = bogus_runner(HashMap::new());
        let mut planner = session_of_role("planner", "inspect and plan");
        planner
            .agent_runtime_state
            .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
            .read_only = true;

        for placement in [
            Placement::Remote {
                endpoint: "wss://resident.example/actor".to_string(),
            },
            Placement::Schedulable {
                pool: "resident-pool".to_string(),
            },
        ] {
            let mut spec = runner.build_spec(&planner, &job_for("read-only-resident"));
            spec.placement = placement;

            let error = ensure_read_only_activation_is_enforceable(&spec)
                .expect_err("resident placement must fail closed for read-only activation");
            let message = error.to_string();
            assert!(message.contains("requires local placement"), "{message}");
            assert!(message.contains("read-only tool boundary"), "{message}");
        }

        let local = runner.build_spec(&planner, &job_for("read-only-local"));
        ensure_read_only_activation_is_enforceable(&local)
            .expect("a locally provisioned read-only worker is enforceable");
    }

    #[tokio::test]
    async fn build_spec_preserves_exact_inherited_permission_mode_for_child_worker() {
        // Exercise the real creation path instead of pre-seeding the child by
        // hand: both legacy Bypass and zero-prompt Auto must survive child
        // creation and the actor provisioning boundary without collapsing.
        for (label, mode) in [
            ("bypass", bamboo_domain::SessionPermissionMode::Bypass),
            ("auto", bamboo_domain::SessionPermissionMode::Auto),
        ] {
            let runner = bogus_runner(HashMap::new());
            let mut parent = Session::new(format!("parent-{label}"), "test-model");
            parent
                .agent_runtime_state
                .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
                .set_permission_mode(mode);
            let workspace = tempfile::tempdir().expect("workspace fixture");
            let port = RecordingChildSessionPort::default();
            let child_id = format!("child-{label}-{}", uuid::Uuid::new_v4());
            crate::session_app::child_session::create_child_action(
                &port,
                crate::session_app::child_session::CreateChildInput {
                    parent_session: parent,
                    child_id: child_id.clone(),
                    title: format!("{label} child"),
                    responsibility: "Run ordinary commands".to_string(),
                    assignment_prompt: "run an ordinary command".to_string(),
                    subagent_type: "explorer".to_string(),
                    workspace: workspace.path().to_string_lossy().into_owned(),
                    workspace_source: crate::project_context::WorkspaceSource::Explicit,
                    model_override: None,
                    model_ref_override: None,
                    runtime_metadata: HashMap::new(),
                    read_only: false,
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
            .expect("create inherited-permission child");
            let child = port.saved_child();

            assert_eq!(
                child
                    .agent_runtime_state
                    .as_ref()
                    .map(bamboo_domain::AgentRuntimeState::effective_permission_mode),
                Some(mode),
                "create_child_action must inherit {label} from the parent"
            );

            let spec = runner.build_spec(&child, &job_for(&child_id));

            assert_eq!(
                spec.capabilities.bypass,
                mode == bamboo_domain::SessionPermissionMode::Bypass
            );
            assert_eq!(
                spec.capabilities.auto_approve_permissions,
                mode == bamboo_domain::SessionPermissionMode::Auto
            );
            assert!(
                spec.capabilities.enforce_permissions,
                "policy evaluation must remain active under {label}"
            );
        }
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
                    read_only: false,
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

    /// A legacy direct worker endpoint cannot bypass the broker activation
    /// contract, even when the endpoint is reachable and has a bearer token.
    #[tokio::test]
    async fn execute_external_child_rejects_direct_remote_worker() {
        let mut placements = HashMap::new();
        placements.insert(
            "explorer".to_string(),
            ResolvedRemotePlacement {
                endpoint: "ws://127.0.0.1:1".into(),
                token: Some("remote-test-token".into()),
                ca_cert_file: None,
                host_label: Some("mini-e2e".into()),
                broker_peer: None,
                requirements: None,
            },
        );
        let runner = bogus_runner(placements);
        let mut session = session_of_role("explorer", "hello remote");
        let job = job_for("child-1");
        let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(64);
        let error = runner
            .execute_external_child(&mut session, &job, event_tx, CancellationToken::new())
            .await
            .err()
            .expect("direct remote transport must fail closed");
        assert!(error
            .to_string()
            .contains("remote_broker_activation_unavailable"));
        assert!(session
            .messages
            .iter()
            .all(|message| message.role != Role::Assistant));
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
            requirements: None,
            ..Default::default()
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
                broker_peer: None,
                requirements: None,
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
        assert!(spec.secrets.worker_auth_token.is_none());
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
                broker_peer: None,
                requirements: None,
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
                broker_peer: None,
                requirements: None,
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
                requirements: None,
                ..Default::default()
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

    #[tokio::test]
    async fn legacy_schedulable_route_fails_closed_without_local_fallback() {
        let mut sched = HashMap::new();
        sched.insert(
            "explorer".to_string(),
            sched_placement("gpu-pool", "unused"),
        );
        let runner = bogus_sched_runner(HashMap::new(), sched);
        let mut session = session_of_role("explorer", "do the thing");
        let (event_tx, _rx) = mpsc::channel::<AgentEvent>(8);
        let error = runner
            .execute_external_child(
                &mut session,
                &job_for("child-1"),
                event_tx,
                CancellationToken::new(),
            )
            .await
            .expect_err("legacy bus presence cannot grant placement");
        assert!(error
            .to_string()
            .contains("remote_broker_activation_unavailable"));
    }
}
