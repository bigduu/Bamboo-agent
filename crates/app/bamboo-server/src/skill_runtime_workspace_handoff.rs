//! In-process Source transactions for the actual ordinary Main execution.
//! A registry entry is only a Weak observation of an already admitted run.
//! The Host setter takes Source W before Session; Native calls retain Source R.

use super::*;
use bamboo_domain::{ActorActivationStatus, ActorDirectoryPort};
use bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY;
use bamboo_server_tools::project_tools::workspace_diagnostics::{
    annotated_error, fence_fingerprint, record_failure, safe_identifier, scope_fingerprint,
    WorkspaceLogContext,
};
use bamboo_server_tools::project_tools::{PreparedWorkspaceHandoff, WorkspaceHandoffHook};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

type Runners = Arc<RwLock<HashMap<String, crate::AgentRunner>>>;

#[derive(Default)]
struct RegistryEntry {
    run: Weak<NativeRun>,
    pending: bool,
}

struct Registry {
    storage: Arc<dyn bamboo_agent_core::Storage>,
    runners: Runners,
    entries: Mutex<HashMap<String, RegistryEntry>>,
    directory: Option<Arc<dyn ActorDirectoryPort>>,
    #[cfg(test)]
    uncertain_legacy_commits: Mutex<std::collections::BTreeSet<String>>,
}

/// Shared by the real Workspace overlay and every registered Native producer.
/// This owns neither a runner nor an Actor lease, and cannot activate one.
pub(crate) struct WorkspaceHandoffs {
    registry: Arc<Registry>,
}

impl WorkspaceHandoffs {
    pub(crate) fn new(storage: Arc<dyn bamboo_agent_core::Storage>, runners: Runners) -> Self {
        Self {
            registry: Arc::new(Registry {
                storage,
                runners,
                entries: Mutex::new(HashMap::new()),
                directory: None,
                #[cfg(test)]
                uncertain_legacy_commits: Mutex::default(),
            }),
        }
    }

    pub(crate) fn with_actor_directory(mut self, directory: Arc<dyn ActorDirectoryPort>) -> Self {
        // Configuration precedes sharing; never substitute a directory while
        // an execution or a prepared Workspace transaction can observe it.
        Arc::get_mut(&mut self.registry)
            .expect("unshared Workspace coordinator")
            .directory = Some(directory);
        self
    }

    pub(super) fn register(&self, run: &Arc<NativeRun>) -> Result<(), ToolError> {
        let mut entries = self
            .registry
            .entries
            .lock()
            .map_err(|_| denied("Native Source registry is unavailable"))?;
        let entry = entries.entry(run.policy.session_id.clone()).or_default();
        if entry.pending {
            return Err(denied("Native Source has a pending Workspace transaction"));
        }
        if entry
            .run
            .upgrade()
            .is_some_and(|old| old.check_live().is_ok())
        {
            return Err(denied("Native Source already has an active owner"));
        }
        entry.run = Arc::downgrade(run);
        Ok(())
    }

    pub(super) fn unregister(&self, run: &NativeRun) {
        if let Ok(mut entries) = self.registry.entries.lock() {
            if let Some(entry) = entries.get_mut(&run.policy.session_id) {
                if std::ptr::eq(entry.run.as_ptr(), run) {
                    entry.run = Weak::new();
                    if !entry.pending {
                        entries.remove(&run.policy.session_id);
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn lose_legacy_commit_confirmation(&self, session_id: &str) {
        self.registry
            .uncertain_legacy_commits
            .lock()
            .unwrap()
            .insert(session_id.to_owned());
    }

    #[cfg(test)]
    pub(super) fn legacy_commit_confirmation_pending(&self, session_id: &str) -> bool {
        self.registry
            .uncertain_legacy_commits
            .lock()
            .unwrap()
            .contains(session_id)
    }
}

/// The marker is installed synchronously before observing runners or awaiting
/// Source W. Cancellation clears it, including the no-run -> new-bind gap.
struct Pending {
    registry: Arc<Registry>,
    session_id: String,
}
impl Drop for Pending {
    fn drop(&mut self) {
        if let Ok(mut entries) = self.registry.entries.lock() {
            if let Some(entry) = entries.get_mut(&self.session_id) {
                entry.pending = false;
                if entry.run.strong_count() == 0 {
                    entries.remove(&self.session_id);
                }
            }
        }
    }
}

#[async_trait]
impl WorkspaceHandoffHook for WorkspaceHandoffs {
    async fn prepare(
        &self,
        session_id: &str,
    ) -> Result<Box<dyn PreparedWorkspaceHandoff>, ToolError> {
        let run = {
            let mut entries = self
                .registry
                .entries
                .lock()
                .map_err(|_| denied("Native Source registry is unavailable"))?;
            let entry = entries.entry(session_id.to_owned()).or_default();
            if entry.pending {
                return Err(denied("Native Source has a pending Workspace transaction"));
            }
            entry.pending = true;
            entry.run.upgrade()
        };
        let pending = Pending {
            registry: self.registry.clone(),
            session_id: session_id.to_owned(),
        };
        let mut legacy = None;
        let source_write = match &run {
            Some(run) => Some(run.source.gate.clone().write_owned().await),
            None => {
                legacy = {
                    let runners = self.registry.runners.read().await;
                    let active = runners.get(session_id).filter(|runner| {
                        matches!(
                            runner.status,
                            crate::AgentStatus::Pending | crate::AgentStatus::Running
                        )
                    });
                    match (
                        active,
                        bamboo_agent_core::tools::context::root_actor_tool_writer(),
                    ) {
                        (Some(runner), Some(Some(owner)))
                            if owner.fence.actor_id == session_id
                                && owner.fence.run_id == runner.run_id
                                && !runner.cancel_token.is_cancelled() =>
                        {
                            Some(LegacyOwner {
                                owner,
                                cancel: runner.cancel_token.clone(),
                            })
                        }
                        (Some(_), _) | (None, Some(Some(_))) => {
                            return Err(denied(
                                "Workspace execution has no matching original Root dispatch owner",
                            ));
                        }
                        (None, _) => None,
                    }
                };
                None
            }
        };
        Ok(Box::new(Prepared {
            pending,
            run,
            _source_write: source_write,
            candidate: None,
            before_scope: None,
            next_generation: None,
            next_workspace: None,
            earlier_scopes: Vec::new(),
            idle_activation: None,
            legacy,
            cancel_uncertain_legacy: false,
            diagnostic_id: None,
        }))
    }
}

#[derive(Clone, PartialEq, Eq)]
struct WorkspaceScope {
    session_id: String,
    created_at: chrono::DateTime<chrono::Utc>,
    project: Option<String>,
    workspace: Option<String>,
    source: Option<String>,
    metadata_version: u64,
    title_version: u64,
}
impl WorkspaceScope {
    fn of(session: &Session) -> Self {
        Self {
            session_id: session.id.clone(),
            created_at: session.created_at,
            project: session.project_id_meta(),
            workspace: session.workspace_path_meta(),
            source: session.metadata.get(WORKSPACE_SOURCE_METADATA_KEY).cloned(),
            metadata_version: session.metadata_version,
            title_version: session.title_version,
        }
    }

    fn matches(&self, session: &Session) -> bool {
        *self == Self::of(session)
    }

    fn scope_changed(&self, other: &Self) -> bool {
        self.workspace != other.workspace || self.source != other.source
    }

    fn title_successor(&self, session: &Session) -> bool {
        let next = Self::of(session);
        let metadata_delta = next.metadata_version.checked_sub(self.metadata_version);
        let title_delta = next.title_version.checked_sub(self.title_version);
        self.session_id == next.session_id
            && self.created_at == next.created_at
            && self.project == next.project
            && self.workspace == next.workspace
            && self.source == next.source
            && metadata_delta.is_some_and(|delta| delta > 0 && Some(delta) == title_delta)
    }

    fn matches_or_title_successor(&self, session: &Session) -> bool {
        self.matches(session) || self.title_successor(session)
    }
}

#[derive(Clone)]
struct CommittedScope {
    before: WorkspaceScope,
    after: WorkspaceScope,
    // Coalesce only actual Host receipts until the next runtime checkpoint.
    // The live runner may already have adopted an intermediate commit through
    // its ordinary bound persistence before that round boundary is reached.
    earlier: Vec<WorkspaceScope>,
    diagnostic_id: Option<String>,
}

struct SourceState {
    generation: u64,
    binding: Option<Arc<SourceBinding>>,
    adopted: WorkspaceScope,
    receipt: Option<CommittedScope>,
    diagnostic_id: Option<String>,
}

pub(super) struct NativeSource {
    gate: Arc<RwLock<()>>,
    state: SyncRwLock<SourceState>,
}
impl NativeSource {
    pub(super) fn new(session: &Session) -> Self {
        Self {
            gate: Arc::new(RwLock::new(())),
            state: SyncRwLock::new(SourceState {
                generation: 0,
                binding: None,
                adopted: WorkspaceScope::of(session),
                receipt: None,
                diagnostic_id: None,
            }),
        }
    }

    pub(super) async fn read(&self) -> OwnedRwLockReadGuard<()> {
        self.gate.clone().read_owned().await
    }

    pub(super) fn install_initial(&self, binding: Arc<SourceBinding>) -> Result<(), ToolError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| denied("Native Source is unavailable"))?;
        if state.binding.is_some() || state.generation != 0 || binding.generation != 0 {
            return Err(denied("Native initial Source changed"));
        }
        state.binding = Some(binding);
        Ok(())
    }

    pub(super) fn binding(&self) -> Result<Arc<SourceBinding>, ToolError> {
        self.state
            .read()
            .map_err(|_| denied("Native Source is unavailable"))?
            .binding
            .clone()
            .ok_or_else(|| denied("Native Source is unavailable"))
    }

    pub(super) fn check_generation(&self, generation: u64) -> Result<(), ToolError> {
        let state = self
            .state
            .read()
            .map_err(|_| denied("Native Source is unavailable"))?;
        if state.generation != generation || state.binding.is_none() {
            return Err(denied("Native Source generation is no longer active"));
        }
        Ok(())
    }

    fn unavailable(&self) {
        if let Ok(mut state) = self.state.write() {
            state.binding = None;
            state.receipt = None;
        }
    }

    #[cfg(test)]
    pub(super) fn has_no_publication(&self) -> bool {
        let state = self.state.read().expect("controlled Source state");
        state.binding.is_none() && state.receipt.is_none()
    }
}

pub(super) struct SourceBinding {
    pub(super) generation: u64,
    policy: Arc<Policy>,
    pub(super) catalog: Arc<SkillsListTool>,
    pub(super) tools: Arc<dyn ToolExecutor>,
}
impl SourceBinding {
    pub(super) fn new(run: &Arc<NativeRun>, policy: Policy, generation: u64) -> Arc<Self> {
        let policy = Arc::new(policy);
        let resolver = Arc::new(NativeResolver {
            run: Arc::downgrade(run),
            policy: policy.clone(),
            generation,
        });
        let catalog = Arc::new(policy.catalog(resolver.clone()));
        let reader = Arc::new(SkillsReadTool::new(policy.catalog(resolver)));
        #[cfg(all(test, unix))]
        tests::observe_workspace_reader(run, &reader);
        let tools = Arc::new(OverlayToolExecutor::new(
            policy.base.clone(),
            catalog.clone(),
        )) as Arc<dyn ToolExecutor>;
        let tools = Arc::new(OverlayToolExecutor::new(tools, reader)) as Arc<dyn ToolExecutor>;
        Arc::new(Self {
            generation,
            policy,
            catalog,
            tools,
        })
    }

    pub(super) async fn check_session(&self, session: Session) -> Result<(), ToolError> {
        check_session(self.policy.clone(), session).await
    }
}

// Canonical filesystem probes may block. The existing Policy guard remains
// the sole Source comparison; the async boundary only moves its I/O off Tokio.
pub(super) async fn check_session(policy: Arc<Policy>, session: Session) -> Result<(), ToolError> {
    tokio::task::spawn_blocking(move || policy.check_session(&session))
        .await
        .map_err(|_| denied("Native canonical Source check is unavailable"))?
}

fn equal_sessions(a: &Session, b: &Session) -> Result<bool, ToolError> {
    let a = serde_json::to_value(a).map_err(|error| denied(&error.to_string()))?;
    let b = serde_json::to_value(b).map_err(|error| denied(&error.to_string()))?;
    Ok(a == b)
}

fn observed_error(
    session_id: &str,
    run_id: Option<&str>,
    diagnostic_id: Option<&str>,
    phase: &'static str,
    error: &(dyn std::error::Error + 'static),
) -> ToolError {
    let id = record_failure(
        WorkspaceLogContext {
            session_id,
            run_id,
            request_id: None,
            diagnostic_id,
        },
        phase,
        error,
    );
    annotated_error(denied(&format!("Workspace {phase} failed: {error}")), &id)
}

// The new handoff probes preserve raw I/O causes for the Host's diagnostic.
// Policy::check_session and its original fail-closed Source guard stay intact.
fn workspace_probe(session: &Session) -> std::io::Result<Option<std::path::PathBuf>> {
    session
        .workspace_path_meta()
        .map(|path| {
            let canonical = std::fs::canonicalize(path)?;
            if !std::fs::metadata(&canonical)?.is_dir() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotADirectory,
                    "Native canonical workspace is not a directory",
                ));
            }
            Ok(canonical)
        })
        .transpose()
}

fn validate_candidate(before: &Session, candidate: &Session) -> Result<bool, ToolError> {
    let old_scope = WorkspaceScope::of(before);
    let next_scope = WorkspaceScope::of(candidate);
    if old_scope.session_id != next_scope.session_id
        || old_scope.created_at != next_scope.created_at
        || old_scope.project != next_scope.project
    {
        return Err(denied(
            "Workspace transaction changed its Session or Project identity",
        ));
    }
    if next_scope.source.as_deref() != Some("explicit") {
        return Err(denied(
            "Workspace transaction is not an explicit Host setter",
        ));
    }
    let changed = old_scope.scope_changed(&next_scope);
    let expected_version = if changed {
        before
            .metadata_version
            .checked_add(1)
            .ok_or_else(|| denied("Native Source revision cannot advance"))?
    } else {
        before.metadata_version
    };
    if candidate.metadata_version != expected_version {
        return Err(denied(
            "Workspace transaction has an invalid metadata revision",
        ));
    }
    let workspace = candidate
        .workspace_path_meta()
        .ok_or_else(|| denied("Workspace transaction has no canonical directory"))?;
    let mut expected = before.clone();
    expected.set_workspace_path_meta(workspace);
    match candidate.metadata.get(WORKSPACE_SOURCE_METADATA_KEY) {
        Some(source) => {
            expected
                .metadata
                .insert(WORKSPACE_SOURCE_METADATA_KEY.into(), source.clone());
        }
        None => {
            expected.metadata.remove(WORKSPACE_SOURCE_METADATA_KEY);
        }
    }
    expected.metadata_version = expected_version;
    if changed {
        expected.updated_at = candidate.updated_at;
    }
    if !equal_sessions(&expected, candidate)? {
        return Err(denied(
            "Workspace transaction changed fields outside its Source scope",
        ));
    }
    Ok(changed)
}

async fn observe_owner(
    registry: &Registry,
    run: &NativeRun,
    session: &Session,
    diagnostic_id: Option<&str>,
) -> Result<(), ToolError> {
    run.check_owner().await?;
    run.policy.check_execution_session(session)?;
    let owner = run
        .root_writer
        .as_ref()
        .ok_or_else(|| denied("Native Workspace requires its original Root writer"))?;
    if owner.created_at != session.created_at
        || owner.fence.actor_id != session.id
        || owner.fence.run_id != run.reservation_id
    {
        return Err(denied("Native Workspace Root writer identity changed"));
    }
    if let Some(ambient) = bamboo_agent_core::tools::context::root_actor_tool_writer() {
        if ambient.as_ref().is_none_or(|actual| {
            actual.fence != owner.fence || actual.created_at != owner.created_at
        }) {
            return Err(denied(
                "Native Workspace dispatch has a different Root writer",
            ));
        }
    }
    observe_exact_root(registry, owner, session, diagnostic_id).await?;
    run.check_owner().await?;
    Ok(())
}

async fn observe_exact_root(
    registry: &Registry,
    owner: &bamboo_domain::RootActorRuntimeWrite,
    session: &Session,
    diagnostic_id: Option<&str>,
) -> Result<(), ToolError> {
    if !registered_main(session)
        || owner.created_at != session.created_at
        || owner.fence.actor_id != session.id
    {
        return Err(denied("Workspace requires its exact ordinary Root birth"));
    }
    let directory = registry
        .directory
        .as_ref()
        .ok_or_else(|| denied("Native Workspace Actor observations are unavailable"))?;
    let entry = directory
        .inspect_actor(&session.id)
        .await
        .map_err(|error| {
            observed_error(
                &session.id,
                Some(&owner.fence.run_id),
                diagnostic_id,
                "observe_root_actor",
                &error,
            )
        })?;
    let expected = bamboo_domain::ActorSession::from_session(session)
        .map_err(|error| denied(&error.to_string()))?;
    let actor = &entry.actor;
    let now = chrono::Utc::now();
    if actor.actor_id != expected.actor_id
        || actor.session_created_at != expected.session_created_at
        || actor.root_actor_id != expected.root_actor_id
        || actor.parent_actor_id != expected.parent_actor_id
        || actor.spawn_depth != expected.spawn_depth
        || actor.project_id != expected.project_id
        || actor.observed_metadata_version != session.metadata_version
        || !entry.activation.as_ref().is_some_and(|activation| {
            activation.fence() == owner.fence
                && activation.project_id == expected.project_id
                && activation.status == ActorActivationStatus::Running
                && activation.lease_expires_at > now
        })
    {
        tracing::warn!(target: "bamboo.workspace_handoff",
            diagnostic_id = ?diagnostic_id.map(safe_identifier),
            session_id = %safe_identifier(&session.id),
            run_id = %safe_identifier(&owner.fence.run_id),
            phase = "root_observation_rejected",
            expected_lease_epoch = owner.fence.lease_epoch,
            observed_lease_epoch = ?entry.activation.as_ref().map(|activation| activation.lease_epoch),
            expected_attempt = owner.fence.attempt,
            observed_attempt = ?entry.activation.as_ref().map(|activation| activation.attempt),
            expected_fence = %fence_fingerprint(&owner.fence),
            observed_fence = ?entry.activation.as_ref().map(|activation| fence_fingerprint(&activation.fence())),
            expected_metadata_version = session.metadata_version,
            observed_metadata_version = actor.observed_metadata_version,
            "Workspace original Actor observation does not match current authority");
        return Err(denied(
            "Native Workspace Actor observation does not match its exact owner and revision",
        ));
    }
    Ok(())
}

/// Captured only from the actual per_call task-local Root capability and its
/// current runner. This observes an existing owner; it never claims a lease.
struct LegacyOwner {
    owner: bamboo_domain::RootActorRuntimeWrite,
    cancel: CancellationToken,
}
impl LegacyOwner {
    async fn check_runner(&self, registry: &Registry, session_id: &str) -> Result<(), ToolError> {
        let ambient = bamboo_agent_core::tools::context::root_actor_tool_writer()
            .flatten()
            .ok_or_else(|| denied("Legacy Workspace requires its actual Root dispatch"))?;
        if ambient.fence != self.owner.fence || ambient.created_at != self.owner.created_at {
            return Err(denied(
                "Legacy Workspace dispatch changed its original Root writer",
            ));
        }
        let runners = registry.runners.read().await;
        if self.cancel.is_cancelled()
            || !runners.get(session_id).is_some_and(|runner| {
                runner.run_id == self.owner.fence.run_id
                    && runner.cancel_token == self.cancel
                    && matches!(
                        runner.status,
                        crate::AgentStatus::Pending | crate::AgentStatus::Running
                    )
            })
        {
            return Err(denied(
                "Legacy Workspace runner no longer matches its original dispatch",
            ));
        }
        Ok(())
    }

    async fn observe(
        &self,
        registry: &Registry,
        session: &Session,
        diagnostic_id: Option<&str>,
    ) -> Result<(), ToolError> {
        if !ordinary_main(session) {
            return Err(denied(
                "Legacy Workspace requires the actual ordinary Main Root",
            ));
        }
        self.check_runner(registry, &session.id).await?;
        observe_exact_root(registry, &self.owner, session, diagnostic_id).await?;
        self.check_runner(registry, &session.id).await
    }
}

async fn observe_idle_root(
    registry: &Registry,
    session: &Session,
    diagnostic_id: Option<&str>,
) -> Result<Option<bamboo_domain::ActorActivationFence>, ToolError> {
    let directory = registry
        .directory
        .as_ref()
        .ok_or_else(|| denied("Idle Workspace Actor observations are unavailable"))?;
    let active = {
        let runners = registry.runners.read().await;
        runners.get(&session.id).is_some_and(|runner| {
            matches!(
                runner.status,
                crate::AgentStatus::Pending | crate::AgentStatus::Running
            )
        })
    };
    if active {
        return Err(denied("Idle Workspace acquired a new execution owner"));
    }
    let expected = bamboo_domain::ActorSession::from_session(session)
        .map_err(|error| denied(&error.to_string()))?;
    let entry = directory
        .inspect_actor(&session.id)
        .await
        .map_err(|error| {
            observed_error(
                &session.id,
                None,
                diagnostic_id,
                "observe_idle_actor",
                &error,
            )
        })?;
    if entry.actor.actor_id != expected.actor_id
        || entry.actor.session_created_at != expected.session_created_at
        || entry.actor.root_actor_id != expected.root_actor_id
        || entry.actor.parent_actor_id != expected.parent_actor_id
        || entry.actor.spawn_depth != expected.spawn_depth
        || entry.actor.project_id != expected.project_id
        || entry.actor.observed_metadata_version != session.metadata_version
        || entry.actor.state == bamboo_domain::ActorLogicalState::Retired
        || entry.activation.as_ref().is_some_and(|activation| {
            activation.status.is_live() && activation.lease_expires_at > chrono::Utc::now()
        })
    {
        return Err(denied(
            "Idle Workspace Actor does not match its exact canonical revision or is still owned",
        ));
    }
    // Expiry does not change logical Active to Cold. Observe each real N+1
    // Workspace commit so two legitimate idle changes cannot conceal a gap.
    // No claim, renewal, finish, retirement or Root writer is created here.
    Ok(entry.activation.map(|activation| activation.fence()))
}

struct Prepared {
    pending: Pending,
    run: Option<Arc<NativeRun>>,
    _source_write: Option<OwnedRwLockWriteGuard<()>>,
    candidate: Option<Session>,
    before_scope: Option<WorkspaceScope>,
    next_generation: Option<u64>,
    next_workspace: Option<std::path::PathBuf>,
    earlier_scopes: Vec<WorkspaceScope>,
    idle_activation: Option<Option<bamboo_domain::ActorActivationFence>>,
    legacy: Option<LegacyOwner>,
    cancel_uncertain_legacy: bool,
    diagnostic_id: Option<String>,
}

impl Prepared {
    fn io_error(
        &self,
        phase: &'static str,
        error: &(dyn std::error::Error + 'static),
    ) -> ToolError {
        let run_id = self
            .run
            .as_ref()
            .map(|run| run.reservation_id.as_str())
            .or_else(|| {
                self.legacy
                    .as_ref()
                    .map(|legacy| legacy.owner.fence.run_id.as_str())
            });
        observed_error(
            &self.pending.session_id,
            run_id,
            self.diagnostic_id.as_deref(),
            phase,
            error,
        )
    }
}

impl Drop for Prepared {
    fn drop(&mut self) {
        if self.cancel_uncertain_legacy {
            if let Some(legacy) = &self.legacy {
                // Never look up and cancel a replacement runner. An uncertain
                // save can only revoke this captured original run's next turn.
                legacy.cancel.cancel();
                tracing::warn!(target: "bamboo.workspace_handoff",
                    diagnostic_id = ?self.diagnostic_id.as_deref().map(safe_identifier),
                    session_id = %safe_identifier(&self.pending.session_id),
                    run_id = %safe_identifier(&legacy.owner.fence.run_id),
                    fence = %fence_fingerprint(&legacy.owner.fence),
                    lease_epoch = legacy.owner.fence.lease_epoch,
                    phase = "legacy_unknown_commit_cancelled",
                    "Workspace completion is uncertain; original runner cancelled");
            }
        }
    }
}

#[async_trait]
impl PreparedWorkspaceHandoff for Prepared {
    fn set_diagnostic_id(&mut self, diagnostic_id: &str) {
        self.diagnostic_id = Some(safe_identifier(diagnostic_id));
    }

    async fn before_write(
        &mut self,
        before: &Session,
        candidate: &Session,
    ) -> Result<(), ToolError> {
        if self.candidate.is_some() || before.id != self.pending.session_id {
            return Err(denied(
                "Workspace preparation has a different or repeated transaction",
            ));
        }
        let changed = validate_candidate(before, candidate)?;
        let registry = &self.pending.registry;
        let actual = registry
            .storage
            .load_session(&before.id)
            .await
            .map_err(|error| self.io_error("load_prepared_scope", &error))?
            .ok_or_else(|| denied("Workspace canonical Session disappeared"))?;
        if !equal_sessions(&actual, before)? {
            return Err(denied(
                "Workspace locked Session changed before its Source transaction",
            ));
        }
        if let Some(run) = &self.run {
            let binding = run.source.binding()?;
            binding.check_session(before.clone()).await?;
            run.policy.check_execution_session(candidate)?;
            if candidate.project_id_meta() != run.policy.project {
                return Err(denied("Native Workspace cannot change its Project"));
            }
            let proposed = candidate.clone();
            let canonical = tokio::task::spawn_blocking(move || workspace_probe(&proposed))
                .await
                .map_err(|error| self.io_error("probe_proposed_scope_task", &error))?
                .map_err(|error| self.io_error("probe_proposed_scope", &error))?
                .ok_or_else(|| denied("Native proposed Source has no directory"))?;
            if candidate
                .workspace_path_meta()
                .map(std::path::PathBuf::from)
                .as_ref()
                != Some(&canonical)
            {
                return Err(denied(
                    "Native proposed Source is not the canonical Host directory",
                ));
            }
            self.next_workspace = Some(canonical);
            // Observe N before the real N+1 save. ActorDirectory's existing
            // Project and gap>=2 guard remains in force at both observations.
            observe_owner(registry, run, before, self.diagnostic_id.as_deref()).await?;
            if changed {
                let mut state = run
                    .source
                    .state
                    .write()
                    .map_err(|_| denied("Native Source is unavailable"))?;
                let previous_generation = state.generation;
                let generation = previous_generation
                    .checked_add(1)
                    .ok_or_else(|| denied("Native Source generation cannot advance"))?;
                if let Some(receipt) = &state.receipt {
                    self.earlier_scopes = receipt.earlier.clone();
                    for scope in [&receipt.before, &receipt.after] {
                        if !self.earlier_scopes.contains(scope) {
                            self.earlier_scopes.push(scope.clone());
                        }
                    }
                }
                state.generation = generation;
                state.binding = None;
                state.receipt = None;
                state.diagnostic_id = self.diagnostic_id.clone();
                self.next_generation = Some(generation);
                tracing::info!(target: "bamboo.workspace_handoff",
                    diagnostic_id = ?self.diagnostic_id,
                    session_id = %safe_identifier(&before.id),
                    run_id = %safe_identifier(&run.reservation_id), previous_generation, generation,
                    fence = ?run.root_writer.as_ref().map(|owner| fence_fingerprint(&owner.fence)),
                    lease_epoch = ?run.root_writer.as_ref().map(|owner| owner.fence.lease_epoch),
                    old_scope = %scope_fingerprint(before), new_scope = %scope_fingerprint(candidate),
                    phase = "source_revoked", "Previous Native Source revoked before save");
                // From this instant every error, cancellation or uncertain
                // save leaves Source unavailable. Drop never restores A.
            }
        } else if let Some(legacy) = &self.legacy {
            legacy
                .observe(registry, before, self.diagnostic_id.as_deref())
                .await?;
            // All validation precedes arming. Any cancellation or error from
            // this point through committed() must stop only this exact run.
            self.cancel_uncertain_legacy = true;
        } else if before.kind == bamboo_domain::SessionKind::Root
            && before.authority_identity.is_ordinary()
            && registry.directory.is_some()
        {
            // This is an observation of the canonical ordinary Root type,
            // never a fabricated Root capability for Child or Supervisor.
            self.idle_activation =
                Some(observe_idle_root(registry, before, self.diagnostic_id.as_deref()).await?);
        }
        self.before_scope = Some(WorkspaceScope::of(before));
        self.candidate = Some(candidate.clone());
        Ok(())
    }

    async fn committed(&mut self, session: &Session) -> Result<(), ToolError> {
        let expected = self
            .candidate
            .as_ref()
            .ok_or_else(|| denied("Workspace commit has no validated preparation"))?;
        if !equal_sessions(expected, session)? {
            return Err(denied("Workspace committed a different candidate"));
        }
        let registry = &self.pending.registry;
        let actual = registry
            .storage
            .load_session(&session.id)
            .await
            .map_err(|error| self.io_error("load_committed_candidate", &error))?
            .ok_or_else(|| denied("Workspace committed Session disappeared"))?;
        if !equal_sessions(expected, &actual)? {
            return Err(denied(
                "Workspace canonical commit does not match its exact candidate",
            ));
        }
        if let Some(run) = &self.run {
            observe_owner(registry, run, &actual, self.diagnostic_id.as_deref()).await?;
            if let Some(generation) = self.next_generation.take() {
                let candidate = actual.clone();
                let workspace = tokio::task::spawn_blocking(move || workspace_probe(&candidate))
                    .await
                    .map_err(|error| self.io_error("probe_committed_scope_task", &error))?
                    .map_err(|error| self.io_error("probe_committed_scope", &error))?;
                if workspace != self.next_workspace {
                    return Err(denied(
                        "Native committed canonical Source changed during the save",
                    ));
                }
                let mut policy = run.policy.clone();
                policy.workspace = workspace;
                // Same Project, permissions and Root tool revision were checked
                // above. Only this Host's actual commit can replace the scope.
                let binding = SourceBinding::new(run, policy, generation);
                let mut state = run
                    .source
                    .state
                    .write()
                    .map_err(|_| denied("Native Source is unavailable"))?;
                if state.generation != generation || state.binding.is_some() {
                    return Err(denied("Native Source changed during Workspace publication"));
                }
                state.receipt = Some(CommittedScope {
                    before: self
                        .before_scope
                        .clone()
                        .ok_or_else(|| denied("Native Source preparation disappeared"))?,
                    after: WorkspaceScope::of(&actual),
                    earlier: std::mem::take(&mut self.earlier_scopes),
                    diagnostic_id: self.diagnostic_id.clone(),
                });
                state.binding = Some(binding);
                state.diagnostic_id = self.diagnostic_id.clone();
                tracing::info!(target: "bamboo.workspace_handoff",
                    diagnostic_id = ?self.diagnostic_id,
                    session_id = %safe_identifier(&actual.id),
                    run_id = %safe_identifier(&run.reservation_id), generation,
                    fence = ?run.root_writer.as_ref().map(|owner| fence_fingerprint(&owner.fence)),
                    lease_epoch = ?run.root_writer.as_ref().map(|owner| owner.fence.lease_epoch),
                    scope = %scope_fingerprint(&actual), phase = "source_published",
                    "Committed Native Source published");
            } else {
                run.source.binding()?.check_session(actual).await?;
            }
        } else if let Some(legacy) = &self.legacy {
            #[cfg(test)]
            if registry
                .uncertain_legacy_commits
                .lock()
                .unwrap()
                .remove(&actual.id)
            {
                return Err(denied(
                    "Controlled loss of legacy Workspace commit confirmation",
                ));
            }
            legacy
                .observe(registry, &actual, self.diagnostic_id.as_deref())
                .await?;
            self.cancel_uncertain_legacy = false;
        } else if let Some(expected) = &self.idle_activation {
            if observe_idle_root(registry, &actual, self.diagnostic_id.as_deref())
                .await?
                .as_ref()
                != expected.as_ref()
            {
                return Err(denied(
                    "Idle Workspace Actor owner changed during publication",
                ));
            }
        }
        self.candidate = None;
        Ok(())
    }
}

impl NativeRun {
    fn bind_refresh_execution(&self, execution_id: &str) -> Result<(), ToolError> {
        if execution_id.is_empty() {
            return Err(denied(
                "Native Workspace adoption has no execution identity",
            ));
        }
        let mut current = self
            .current
            .write()
            .map_err(|_| denied("Native input owner is unavailable"))?;
        match current.execution_id.as_deref() {
            Some(current) if current == execution_id => Ok(()),
            Some(_) => Err(denied(
                "Native Workspace adoption has a different execution",
            )),
            None if current.epoch == 0
                && self.epoch.load(Ordering::Acquire) == 0
                && current.current_input_id.is_none() =>
            {
                // The first actual private Host callback can precede Q's
                // observation. Bind only its execution id; no input or epoch
                // is created, and later callbacks still require this id.
                current.execution_id = Some(execution_id.to_owned());
                Ok(())
            }
            None => Err(denied("Native Workspace adoption has already observed Q")),
        }
    }

    pub(super) async fn refresh_workspace_scope(
        &self,
        session: &mut Session,
        execution_id: &str,
    ) -> Result<(), ToolError> {
        self.refresh_workspace_scope_inner(session, execution_id)
            .await
            .map_err(|error| {
                let diagnostic_id = self.source.state.read().ok().and_then(|state| {
                    state
                        .receipt
                        .as_ref()
                        .and_then(|receipt| receipt.diagnostic_id.clone())
                        .or_else(|| state.diagnostic_id.clone())
                });
                let id = record_failure(
                    WorkspaceLogContext {
                        session_id: &self.policy.session_id,
                        run_id: Some(&self.reservation_id),
                        request_id: None,
                        diagnostic_id: diagnostic_id.as_deref(),
                    },
                    "runtime_scope_adoption_rejected",
                    &error,
                );
                annotated_error(error, &id)
            })
    }

    fn adoption_io_error(
        &self,
        phase: &'static str,
        error: &(dyn std::error::Error + 'static),
    ) -> ToolError {
        let diagnostic_id = self
            .source
            .state
            .read()
            .ok()
            .and_then(|state| state.diagnostic_id.clone());
        // Record the original I/O/authority source chain before ToolError's
        // string boundary; the outer rejection remains correlated to this id.
        let id = record_failure(
            WorkspaceLogContext {
                session_id: &self.policy.session_id,
                run_id: Some(&self.reservation_id),
                request_id: None,
                diagnostic_id: diagnostic_id.as_deref(),
            },
            phase,
            error,
        );
        if diagnostic_id.is_none() {
            if let Ok(mut state) = self.source.state.write() {
                state.diagnostic_id = Some(id.clone());
            }
        }
        annotated_error(
            denied(&format!("Native Workspace {phase} failed: {error}")),
            &id,
        )
    }

    async fn refresh_workspace_scope_inner(
        &self,
        session: &mut Session,
        execution_id: &str,
    ) -> Result<(), ToolError> {
        let _source = self.source.read().await;
        self.policy.check_execution_session(session)?;
        {
            let current = self
                .current
                .read()
                .map_err(|_| denied("Native input owner is unavailable"))?;
            if current
                .execution_id
                .as_deref()
                .is_some_and(|id| id != execution_id)
            {
                return Err(denied(
                    "Native Workspace adoption has a different execution",
                ));
            }
        }
        let binding = self.source.binding()?;
        let (receipt, adopted) = {
            let state = self
                .source
                .state
                .read()
                .map_err(|_| denied("Native Source is unavailable"))?;
            (state.receipt.clone(), state.adopted.clone())
        };
        let Some(receipt) = receipt else {
            self.check_owner().await?;
            return self.bind_refresh_execution(execution_id);
        };
        if !adopted.matches(session)
            && !receipt.before.matches(session)
            && !receipt.after.matches_or_title_successor(session)
            && !receipt.earlier.iter().any(|scope| scope.matches(session))
        {
            return Err(denied(
                "Native live Source does not match its committed handoff",
            ));
        }
        let handoffs = self
            .policy
            .state
            .workspace_handoffs
            .as_ref()
            .ok_or_else(|| denied("Native Workspace coordinator disappeared"))?;
        let actual = handoffs
            .registry
            .storage
            .load_session(&self.policy.session_id)
            .await
            .map_err(|error| self.adoption_io_error("load_committed_scope", &error))?
            .ok_or_else(|| denied("Native Workspace Session disappeared"))?;
        if !receipt.after.matches_or_title_successor(&actual) {
            return Err(denied(
                "Native canonical Source changed after its committed handoff",
            ));
        }
        if receipt.after.title_successor(&actual) {
            // Validate the fresh canonical snapshot, not the live transcript.
            // This strict port does not ensure/repair Actor observations. It
            // must run before observe_owner could observe a newer revision.
            handoffs
                .registry
                .storage
                .validate_title_observations(&actual)
                .await
                .map_err(|error| self.adoption_io_error("validate_title_suffix", &error))?;
        }
        let target = WorkspaceScope::of(&actual);
        if session.metadata_version >= target.metadata_version && !target.matches(session) {
            return Err(denied(
                "Native live Source conflicts with its committed metadata revision",
            ));
        }
        binding.check_session(actual.clone()).await?;
        observe_owner(
            &handoffs.registry,
            self,
            &actual,
            receipt.diagnostic_id.as_deref(),
        )
        .await?;
        self.bind_refresh_execution(execution_id)?;
        if session.metadata_version < target.metadata_version {
            let persistence = self.persistence.as_ref().ok_or_else(|| {
                denied("Native Workspace adoption has no bound execution persistence")
            })?;
            let owner = self
                .root_writer
                .as_ref()
                .ok_or_else(|| denied("Native Workspace adoption has no original Root writer"))?;
            if persistence
                .root_actor_writer()
                .as_ref()
                .is_none_or(|actual| {
                    actual.fence != owner.fence || actual.created_at != owner.created_at
                })
            {
                return Err(denied(
                    "Native Workspace adoption persistence has a different Root writer",
                ));
            }
            // Use the existing atomic canonical-metadata merge and append-safe
            // checkpoint. Stamping N+1 onto only the workspace would let the
            // rest of a stale metadata group masquerade as a current revision.
            if let Err(error) = persistence.checkpoint_runtime_session(session).await {
                self.source.unavailable();
                return Err(self.adoption_io_error("checkpoint_committed_scope", &error));
            }
            if !receipt.after.matches_or_title_successor(session) {
                self.source.unavailable();
                return Err(denied(
                    "Native Workspace checkpoint did not adopt its exact committed scope",
                ));
            }
            let canonical = handoffs
                .registry
                .storage
                .load_session(&self.policy.session_id)
                .await
                .map_err(|error| self.adoption_io_error("reload_adopted_scope", &error))?
                .ok_or_else(|| denied("Native Workspace checkpoint Session disappeared"))?;
            if !WorkspaceScope::of(session).matches(&canonical) {
                self.source.unavailable();
                return Err(denied(
                    "Native Workspace checkpoint canonical revision changed",
                ));
            }
            if receipt.after.title_successor(&canonical) {
                handoffs
                    .registry
                    .storage
                    .validate_title_observations(&canonical)
                    .await
                    .map_err(|error| {
                        self.adoption_io_error("validate_adopted_title_suffix", &error)
                    })?;
            }
            binding.check_session(session.clone()).await?;
            observe_owner(
                &handoffs.registry,
                self,
                session,
                receipt.diagnostic_id.as_deref(),
            )
            .await?;
        }
        let mut state = self
            .source
            .state
            .write()
            .map_err(|_| denied("Native Source is unavailable"))?;
        state.adopted = WorkspaceScope::of(session);
        state.receipt = None;
        tracing::info!(target: "bamboo.workspace_handoff",
            diagnostic_id = ?receipt.diagnostic_id,
            session_id = %safe_identifier(&self.policy.session_id),
            run_id = %safe_identifier(&self.reservation_id), generation = state.generation,
            fence = ?self.root_writer.as_ref().map(|owner| fence_fingerprint(&owner.fence)),
            lease_epoch = ?self.root_writer.as_ref().map(|owner| owner.fence.lease_epoch),
            scope = %scope_fingerprint(session), metadata_version = session.metadata_version,
            phase = "runtime_scope_adopted", "Live Native scope adopted committed Host metadata");
        Ok(())
    }
}
