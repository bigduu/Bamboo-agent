//! One canonical Supervisor relationship writer. No target mutation or registry.

#[path = "supervisor_current_user.rs"]
mod current_user;
pub(crate) use current_user::CurrentUserTargetGuard;
#[path = "observer_subscriptions.rs"]
pub(super) mod subscriptions;

use std::collections::{BTreeMap, BTreeSet};

use super::default_actor_context::LifecycleGuard;
use super::*;
use bamboo_domain::{
    validate_supervisor_target_id, SessionAuthorityConflict, SupervisorLinkObservation,
    SupervisorManagedLink, SupervisorManagementMutation, SupervisorManagementReceipt,
    SupervisorManagementRequest, SupervisorManagementState, SupervisorReference,
    SupervisorScopeObservation, MAX_SUPERVISOR_LINKS, MAX_SUPERVISOR_PROJECTS,
    SUPERVISOR_MANAGEMENT_SCHEMA_VERSION,
};

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn denied(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

fn next_revision(revision: u64) -> io::Result<u64> {
    revision
        .checked_add(1)
        .ok_or_else(|| invalid("Supervisor revision exhausted"))
}

fn reference_is_valid(reference: &SupervisorReference) -> io::Result<()> {
    if reference.session_id != DEFAULT_SUPERVISOR_SESSION_ID || reference.incarnation_id.is_nil() {
        return Err(denied("a canonical Supervisor reference is required"));
    }
    Ok(())
}

fn target_id_is_valid(target: &str) -> io::Result<()> {
    validate_supervisor_target_id(target).map_err(invalid)?;
    if target == DEFAULT_SUPERVISOR_SESSION_ID {
        return Err(denied("the Supervisor cannot manage itself"));
    }
    Ok(())
}

fn target_project(target: &Session) -> io::Result<ProjectId> {
    if !target.authority_identity.is_ordinary() || target.kind != SessionKind::Root {
        return Err(denied("a target must be an independent Ordinary Root"));
    }
    target
        .project_id_meta()
        .ok_or_else(|| denied("target has no Project"))?
        .parse::<ProjectId>()
        .map_err(|_| denied("target has an invalid Project"))
}

/// Shared canonical candidate check for both existing Supervisor creation and
/// explicitly scoped Host creation. Neither route can create an elevated Root.
pub(super) fn new_root_candidate_project(candidate: &Session) -> io::Result<ProjectId> {
    let project = target_project(candidate)?;
    candidate
        .check_execution_compatibility()
        .map_err(|error| denied(&error.to_string()))?;
    candidate
        .validate_conversation_state()
        .map_err(|error| denied(&error.to_string()))?;
    if candidate.root_session_id != candidate.id
        || candidate.parent_session_id.is_some()
        || candidate.parent_created_at.is_some()
        || candidate.spawn_depth != 0
        || candidate.root_orchestration_only_enabled()
        || candidate.supervisor_management.is_some()
        || candidate.conversation_reset.is_some()
        || candidate.metadata_version != 0
        || candidate
            .workspace_path_meta()
            .is_none_or(|path| path.trim().is_empty())
    {
        return Err(denied(
            "creation requires a new Standard Ordinary Root with an explicit workspace",
        ));
    }
    Ok(project)
}

pub(super) fn reserved_root_matches(target: &Session, candidate: &Session) -> bool {
    target.id == candidate.id
        && target.created_at == candidate.created_at
        && target_project(target).ok() == target_project(candidate).ok()
        && target.workspace_path_meta() == candidate.workspace_path_meta()
        && target.metadata_version == 0
        && target.root_session_id == target.id
        && target.parent_session_id.is_none()
        && target.parent_created_at.is_none()
        && target.spawn_depth == 0
        && target.supervisor_management.is_none()
        && !target.root_orchestration_only_enabled()
        && target.session_version == candidate.session_version
        && candidate.check_conversation_generation(target).is_ok()
}

fn empty_state(incarnation_id: Uuid) -> SupervisorManagementState {
    SupervisorManagementState {
        schema_version: SUPERVISOR_MANAGEMENT_SCHEMA_VERSION,
        incarnation_id,
        revision: 0,
        allowed_projects: BTreeSet::new(),
        links: BTreeMap::new(),
        observers: BTreeMap::new(),
        subscriptions: BTreeMap::new(),
    }
}

/// One retained instance of the existing authority lock set, never a durable
/// grant or a second lifecycle protocol. Constructed only by the strict reader.
pub(crate) struct SupervisorFollowupGuard {
    sessions: Vec<SessionWriteGuard>,
    _task: RuntimeTaskTransactionReadGuard,
    _lifecycle: SessionLifecycleReadGuard,
    // A private observation held stable by these same target Session locks.
    // It closes only new Loop carriers, never reads or exact receipt retries.
    scoped_loop_stopped: bool,
    scoped_native: Option<Box<super::scoped_transport_expiry::ScopedTransportCensus>>,
}

impl SupervisorFollowupGuard {
    pub(crate) fn scoped_native(
        &self,
    ) -> Option<super::scoped_transport_expiry::ScopedTransportCensus> {
        self.scoped_native.as_deref().cloned()
    }

    pub(crate) fn scoped_loop_stopped(&self) -> bool {
        self.scoped_loop_stopped
    }
}

/// Management publication owns the existing locks, not a new authority grant.
/// Fields release Sessions → Task → lifecycle; pop reverses sorted acquisition.
pub(super) struct SupervisorManagementGuards {
    sessions: Vec<SessionWriteGuard>,
    _task: RuntimeTaskTransactionReadGuard,
    _lifecycle: LifecycleGuard,
    _host_namespace: Option<crate::host_session_scope::HostSessionScopeAbsence>,
}

impl Drop for SupervisorManagementGuards {
    fn drop(&mut self) {
        while let Some(_guard) = self.sessions.pop() {}
    }
}

impl SupervisorManagementGuards {
    pub(super) fn retains_exclusive_lifecycle(&self) -> bool {
        self._lifecycle.retains_exclusive()
    }
}

impl Drop for SupervisorFollowupGuard {
    fn drop(&mut self) {
        while let Some(_guard) = self.sessions.pop() {}
    }
}

fn link_authorizes_target(
    state: &SupervisorManagementState,
    link: &SupervisorManagedLink,
    target: &Session,
) -> bool {
    link.enabled
        && target_project(target).is_ok_and(|project| {
            project == link.target_project_id && state.allowed_projects.contains(&project)
        })
        && target.created_at == link.target_created_at
        && target.metadata_version == link.target_metadata_version
}

fn observer_link_authorizes_target(
    state: &SupervisorManagementState,
    observer: &bamboo_domain::SupervisorObserverRegistration,
    target_id: &str,
    link: &SupervisorManagedLink,
    target: &Session,
) -> bool {
    if observer.current_user_targets {
        link.enabled
            && target.id == target_id
            && target_project(target).is_ok_and(|project| project == link.target_project_id)
            && target.created_at == link.target_created_at
            && target.metadata_version == link.target_metadata_version
    } else {
        state.links.get(target_id) == Some(link) && link_authorizes_target(state, link, target)
    }
}

impl SessionStoreV2 {
    async fn strict_observer_child(
        &self,
        parent: &Session,
        child_id: &str,
    ) -> io::Result<Option<Session>> {
        let directory = self.abs_path_from_rel(&Self::child_rel_path(&parent.id, child_id));
        if Self::read_runtime_sidecar_at(&directory.join(RUNTIME_SIDECAR_FILE), child_id)
            .await?
            .is_none()
        {
            return Err(denied("Observer current runtime sidecar is unavailable"));
        }
        // Unlike ordinary Child compatibility reads, an Observer never borrows
        // an old Main after its current sidecar disappeared. The existing full
        // pair validator preserves canonical birth and tool/generation proofs.
        self.load_child_wait_session(parent, child_id, true).await
    }

    async fn observer_guards(
        &self,
        child_id: &str,
        targets: impl IntoIterator<Item = String>,
        absence: Option<crate::host_session_scope::HostSessionScopeAbsence>,
        registering: bool,
    ) -> io::Result<Arc<SupervisorManagementGuards>> {
        target_id_is_valid(child_id)?;
        let lifecycle = self.lock_session_lifecycle_shared().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        {
            // Never put an existing Root in the later Child lock position.
            // Reserved Root creation holds exclusive lifecycle, so an absent
            // candidate cannot become a Root while these guards are retained.
            let _child = self.acquire_session_maintenance_lock(child_id).await?;
            if self
                .load_root_authority_unchecked(child_id)
                .await?
                .is_some()
            {
                return Err(denied("an independent Root cannot become an Observer"));
            }
            if registering {
                let parent = self
                    .load_root_authority_unchecked(DEFAULT_SUPERVISOR_SESSION_ID)
                    .await?
                    .ok_or_else(|| denied("Observer parent is missing"))?;
                let child = self
                    .strict_observer_child(&parent, child_id)
                    .await?
                    .ok_or_else(|| denied("Observer Child is missing"))?;
                if !observer_native_child(&child) {
                    return Err(denied(
                        "Observer registration requires a read-only native Child",
                    ));
                }
            }
        }
        let mut roots: BTreeSet<String> = targets.into_iter().collect();
        for id in &roots {
            target_id_is_valid(id)?;
            if !self.preview_management_root(id).await? {
                return Err(denied("Observer target Root does not exist"));
            }
        }
        roots.insert(DEFAULT_SUPERVISOR_SESSION_ID.into());
        let mut sessions = Vec::with_capacity(roots.len() + 1);
        for id in roots {
            sessions.push(self.acquire_session_maintenance_lock(&id).await?);
        }
        sessions.push(self.acquire_session_maintenance_lock(child_id).await?);
        Ok(Arc::new(SupervisorManagementGuards {
            sessions,
            _task: task,
            _lifecycle: LifecycleGuard::Shared { _guard: lifecycle },
            _host_namespace: absence,
        }))
    }

    /// Trusted two-stage Host API. The existing Creator has already saved a
    /// fresh read-only Child with auto_run=false. This operation allocates no
    /// Child, so retries always identify the original Child and registration.
    pub async fn register_supervisor_observer(
        &self,
        reference: &SupervisorReference,
        expected_revision: u64,
        operation_id: Uuid,
        child_id: &str,
        targets: BTreeSet<String>,
    ) -> io::Result<bamboo_domain::SupervisorObserverReceipt> {
        use bamboo_domain::{
            SupervisorObserverRegistration, MAX_SUPERVISOR_OBSERVERS,
            MAX_SUPERVISOR_OBSERVER_TARGETS, SUPERVISOR_CURRENT_USER_MANAGEMENT_SCHEMA_VERSION,
        };
        reference_is_valid(reference)?;
        target_id_is_valid(child_id)?;
        if operation_id.is_nil()
            || targets.is_empty()
            || targets.len() > MAX_SUPERVISOR_OBSERVER_TARGETS
        {
            return Err(invalid("invalid bounded Observer registration"));
        }
        for id in &targets {
            target_id_is_valid(id)?;
            if id == child_id {
                return Err(denied("an Observer cannot observe itself"));
            }
        }
        let guards = self
            .observer_guards(child_id, targets.iter().cloned(), None, true)
            .await?;
        let mut parent = self.management_supervisor_locked(reference).await?;
        self.check_management_session_compatibility(&parent).await?;
        let mut state = parent
            .supervisor_management
            .clone()
            .unwrap_or_else(|| empty_state(reference.incarnation_id));
        let child = self
            .strict_observer_child(&parent, child_id)
            .await?
            .ok_or_else(|| denied("Observer Child is missing"))?;
        if let Some(old) = state.observers.get(child_id) {
            if old.operation_id != operation_id
                || old.parent_created_at != parent.created_at
                || old.child_created_at != child.created_at
                || old.target_links.keys().cloned().collect::<BTreeSet<_>>() != targets
            {
                return Err(denied(
                    "Observer registration retry conflicts with its original request",
                ));
            }
            return Ok(observer_receipt(
                reference,
                child_id,
                state.revision,
                old,
                false,
            ));
        }
        if state.revision != expected_revision {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Supervisor changed before Observer registration",
            ));
        }
        if state.observers.len() >= MAX_SUPERVISOR_OBSERVERS
            || state
                .observers
                .values()
                .any(|old| old.operation_id == operation_id)
        {
            return Err(denied(
                "Observer registration capacity or operation conflict",
            ));
        }
        let runtime = child
            .agent_runtime_state
            .as_ref()
            .ok_or_else(|| denied("Observer requires typed read-only runtime state"))?;
        if !observer_native_child(&child)
            || child.parent_created_at != Some(parent.created_at)
            || child.spawn_depth != 1
            || child.root_session_id != parent.id
            || child.project_id_meta() != parent.project_id_meta()
            || child.pending_question.is_some()
            || runtime.waiting_for_children.is_some()
            || runtime.waiting_for_bash.is_some()
            || child.child_launch_generation() != 1
            || child.recoverable_child_launch_generation().is_some()
            || child.last_run_status().as_deref() != Some("pending")
            || !runtime.run_id.is_empty()
            || runtime.round.current_round != 0
            || runtime.round.total_tool_calls != 0
            || parent
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.waiting_for_children.as_ref())
                .is_some_and(|wait| wait.child_session_ids.iter().any(|id| id == child_id))
        {
            return Err(denied(
                "only a fresh unqueued read-only native Child may become an Observer",
            ));
        }
        let mut links = BTreeMap::new();
        let registration_revision = next_revision(state.revision)?;
        for id in &targets {
            let target = self
                .load_root_authority_unchecked(id)
                .await?
                .ok_or_else(|| denied("Observer target is missing"))?;
            links.insert(
                id.clone(),
                SupervisorManagedLink {
                    revision: registration_revision,
                    enabled: true,
                    target_created_at: target.created_at,
                    target_project_id: target_project(&target)?,
                    target_metadata_version: target.metadata_version,
                },
            );
        }
        state.revision = registration_revision;
        state.schema_version = state
            .schema_version
            .max(SUPERVISOR_CURRENT_USER_MANAGEMENT_SCHEMA_VERSION);
        let registration = SupervisorObserverRegistration {
            operation_id,
            revision: state.revision,
            enabled: true,
            parent_created_at: parent.created_at,
            child_created_at: child.created_at,
            target_links: links,
            current_user_targets: true,
        };
        state
            .observers
            .insert(child_id.into(), registration.clone());
        state.validate(reference.incarnation_id).map_err(invalid)?;
        self.publish_management_state(&mut parent, &state, &guards)
            .await?;
        Ok(observer_receipt(
            reference,
            child_id,
            state.revision,
            &registration,
            true,
        ))
    }

    /// A disabled exact-birth tombstone is never implicitly re-enabled.
    pub async fn revoke_supervisor_observer(
        &self,
        reference: &SupervisorReference,
        expected_revision: u64,
        child_id: &str,
    ) -> io::Result<bamboo_domain::SupervisorObserverReceipt> {
        reference_is_valid(reference)?;
        target_id_is_valid(child_id)?;
        let guards = self.observer_guards(child_id, [], None, false).await?;
        let mut parent = self.management_supervisor_locked(reference).await?;
        self.check_management_session_compatibility(&parent).await?;
        let mut state = parent
            .supervisor_management
            .clone()
            .unwrap_or_else(|| empty_state(reference.incarnation_id));
        let old = state
            .observers
            .get(child_id)
            .ok_or_else(|| denied("Observer is not registered"))?
            .clone();
        if !old.enabled {
            return Ok(observer_receipt(
                reference,
                child_id,
                state.revision,
                &old,
                false,
            ));
        }
        if state.revision != expected_revision {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Supervisor changed before Observer revocation",
            ));
        }
        state.revision = next_revision(state.revision)?;
        let mut revoked = old;
        revoked.enabled = false;
        revoked.revision = state.revision;
        state.observers.insert(child_id.into(), revoked.clone());
        self.publish_management_state(&mut parent, &state, &guards)
            .await?;
        Ok(observer_receipt(
            reference,
            child_id,
            state.revision,
            &revoked,
            true,
        ))
    }

    pub(super) async fn observer_classification(
        &self,
        expected_parent: &Session,
        child_id: &str,
    ) -> io::Result<bool> {
        let Some(reference) = observer_parent_reference(expected_parent) else {
            return Ok(false);
        };
        let _guards = self.observer_guards(child_id, [], None, false).await?;
        let parent = self.management_supervisor_locked(&reference).await?;
        if parent.created_at != expected_parent.created_at {
            return Err(denied("Observer parent lifetime changed"));
        }
        let Some(state) = parent.supervisor_management.as_ref() else {
            return Ok(false);
        };
        if !state.observers.contains_key(child_id) {
            return Ok(false);
        }
        let child = self
            .strict_observer_child(&parent, child_id)
            .await?
            .ok_or_else(|| denied("registered Observer Child is unavailable"))?;
        Ok(state.classifies_observer(&parent, &child))
    }

    pub(super) async fn observer_activation(&self, expected: &Session) -> io::Result<bool> {
        if expected.kind != SessionKind::Child
            || expected.parent_session_id.as_deref() != Some(DEFAULT_SUPERVISOR_SESSION_ID)
        {
            return Ok(false);
        }
        // First establish that this exact lifetime is registered. Ordinary
        // Children retain their existing runtime even if a Host namespace is
        // absent or revoked; a cosmetic tracker label changes nothing.
        let Some(preview) =
            Storage::load_root_authority(self, DEFAULT_SUPERVISOR_SESSION_ID).await?
        else {
            return Ok(false);
        };
        let Some(reference) = observer_parent_reference(&preview) else {
            return Ok(false);
        };
        let Some(registration) = preview
            .supervisor_management
            .as_ref()
            .and_then(|state| state.observers.get(&expected.id))
        else {
            return Ok(false);
        };
        if registration.child_created_at != expected.created_at
            || Some(registration.parent_created_at) != expected.parent_created_at
        {
            return Ok(false);
        }
        let ids = registration
            .target_links
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let caller = bamboo_domain::SessionCallerReference::from(expected);
        // A registered Observer never replaces a failed Host lookup with its
        // durable fallback. Retain the selected publication fence until checks end.
        let token = self.host_scope_registry.lookup(&caller).await?;
        let _host_scope = match token.as_ref() {
            Some(token) => Some(self.host_scope_registry.retain(token, &caller).await?),
            None => None,
        };
        let _absence = if token.is_none() {
            Some(
                self.host_scope_registry
                    .retain_never_installed(&caller.session_id)
                    .await?,
            )
        } else {
            None
        };
        let _guards = self.observer_guards(&expected.id, ids, None, false).await?;
        let parent = self.management_supervisor_locked(&reference).await?;
        let state = parent
            .supervisor_management
            .as_ref()
            .ok_or_else(|| denied("Observer state is missing"))?;
        let child = self
            .strict_observer_child(&parent, &expected.id)
            .await?
            .ok_or_else(|| denied("Observer Child is missing"))?;
        if !caller.matches_session(&child)
            || !observer_native_child(&child)
            || !state.classifies_observer(&parent, &child)
        {
            return Err(denied(
                "Observer activation identity or native route changed",
            ));
        }
        let registration = state
            .observers
            .get(&child.id)
            .expect("classified registration");
        if !registration.enabled {
            return Err(denied("Observer was revoked"));
        }
        for (id, link) in &registration.target_links {
            let target = self
                .load_root_authority_unchecked(id)
                .await?
                .ok_or_else(|| denied("Observer target is missing"))?;
            if !observer_link_authorizes_target(state, registration, id, link, &target) {
                return Err(denied("Observer activation target scope changed"));
            }
        }
        Ok(true)
    }

    pub(super) async fn observer_context_target(
        &self,
        caller: &bamboo_domain::SessionCallerReference,
        target_id: &str,
    ) -> io::Result<(Session, bamboo_domain::SupervisorObserverContext)> {
        if caller.kind != SessionKind::Child
            || caller.parent_session_id.as_deref() != Some(DEFAULT_SUPERVISOR_SESSION_ID)
        {
            return Err(denied("an explicitly registered Observer is required"));
        }
        target_id_is_valid(target_id)?;
        let absence = self
            .host_scope_registry
            .retain_never_installed(&caller.session_id)
            .await?;
        let _guards = self
            .observer_guards(&caller.session_id, [target_id.into()], Some(absence), false)
            .await?;
        let parent = self
            .load_root_authority_unchecked(DEFAULT_SUPERVISOR_SESSION_ID)
            .await?
            .ok_or_else(|| denied("Observer parent is missing"))?;
        let reference = observer_parent_reference(&parent)
            .ok_or_else(|| denied("Observer parent is not the canonical Supervisor"))?;
        let state = parent
            .supervisor_management
            .as_ref()
            .ok_or_else(|| denied("Observer state is missing"))?;
        let child = self
            .strict_observer_child(&parent, &caller.session_id)
            .await?
            .ok_or_else(|| denied("Observer Child is missing"))?;
        if !caller.matches_session(&child)
            || !observer_native_child(&child)
            || !state.classifies_observer(&parent, &child)
        {
            return Err(denied("Observer original caller changed"));
        }
        let registration = state
            .observers
            .get(&child.id)
            .expect("classified registration");
        let link = registration
            .target_links
            .get(target_id)
            .filter(|_| registration.enabled)
            .ok_or_else(|| denied("Observer has no current view grant"))?;
        let target = self
            .load_root_authority_unchecked(target_id)
            .await?
            .ok_or_else(|| denied("Observer target is missing"))?;
        if !observer_link_authorizes_target(state, registration, target_id, link, &target) {
            return Err(denied("Observer view grant changed"));
        }
        Ok((
            target,
            bamboo_domain::SupervisorObserverContext {
                supervisor: reference,
                parent_created_at: parent.created_at,
                child_created_at: child.created_at,
                operation_id: registration.operation_id,
                registration_revision: registration.revision,
                target_link: link.clone(),
            },
        ))
    }

    /// Retain the existing lifecycle/Task/Session authority while staging a
    /// decision for this exact Supervisor, without managing itself as a Root.
    pub(crate) async fn lock_supervisor_patrol_notice(
        &self,
        reference: &SupervisorReference,
        orchestration_revision: u64,
    ) -> io::Result<SupervisorFollowupGuard> {
        reference_is_valid(reference)?;
        let lifecycle = self.lock_session_lifecycle_shared().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let sessions = self.management_session_locks(None).await?;
        let current = self.management_supervisor_locked(reference).await?;
        self.check_management_session_compatibility(&current)
            .await?;
        if orchestration_revision == 0
            || !current.root_orchestration_only_enabled()
            || current.root_tool_authority_revision != orchestration_revision
        {
            return Err(denied("Supervisor orchestration binding changed"));
        }
        Ok(SupervisorFollowupGuard {
            sessions,
            _task: task,
            _lifecycle: lifecycle,
            scoped_loop_stopped: false,
            scoped_native: None,
        })
    }

    /// Each started job owns the complete physical guard set through sync and
    /// error cleanup, even if its caller or originating runtime disappears.
    /// This does not promise that the next management stage will be started.
    pub(super) async fn write_management_bytes(
        &self,
        path: &Path,
        bytes: Vec<u8>,
        stage: supervisor_proof::SupervisorProofFault,
        guards: &Arc<SupervisorManagementGuards>,
    ) -> io::Result<()> {
        let path = path.to_path_buf();
        let guards = Arc::clone(guards);
        #[cfg(test)]
        let hook = self.management_write_hook.lock().unwrap().clone();
        tokio::task::spawn_blocking(move || {
            let _guards = guards;
            durable_atomic_write_blocking(&path, &bytes, |phase| {
                #[cfg(test)]
                if let Some(hook) = &hook {
                    return hook.visit(stage, &path, phase);
                }
                let _ = (stage, phase);
                Ok(())
            })
        })
        .await
        .map_err(|error| {
            other_io_error(format!(
                "join Supervisor management filesystem job: {error}"
            ))
        })?
    }

    /// Acquire in the canonical lifecycle → Task → sorted Session order.
    /// FileSessionInbox keeps this guard alive until its own receipt commits.
    pub(crate) async fn lock_supervisor_followup(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<SupervisorFollowupGuard> {
        reference_is_valid(reference)?;
        target_id_is_valid(target_id)?;
        let lifecycle = self.lock_session_lifecycle_shared().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let sessions = self.management_session_locks(Some(target_id)).await?;
        let current = self.management_supervisor_locked(reference).await?;
        let state = current
            .supervisor_management
            .unwrap_or_else(|| empty_state(reference.incarnation_id));
        let link = state
            .links
            .get(target_id)
            .ok_or_else(|| denied("target is not attached to this Supervisor"))?;
        let target = self
            .load_root_authority_unchecked(target_id)
            .await?
            .ok_or_else(|| denied("Supervisor followup target does not exist"))?;
        if !link_authorizes_target(&state, link, &target) {
            return Err(denied(
                "Supervisor followup link or target authority is no longer valid",
            ));
        }
        Ok(SupervisorFollowupGuard {
            sessions,
            _task: task,
            _lifecycle: lifecycle,
            scoped_loop_stopped: false,
            scoped_native: None,
        })
    }

    /// The existing sorted authority lock set, retained through the caller's
    /// physical read/write jobs. Do not call a public writer while holding it.
    pub(crate) async fn lock_scoped_loop_authority(
        &self,
        binding: &bamboo_domain::ScopedLoopControlBinding,
    ) -> io::Result<SupervisorFollowupGuard> {
        binding.validate().map_err(invalid)?;
        let mut guard = self
            .lock_supervisor_followup(&binding.supervisor, &binding.target_session_id)
            .await?;
        let source = self
            .load_execution_admission_unchecked(&binding.supervisor.session_id)
            .await?
            .ok_or_else(|| denied("Supervisor disappeared"))?;
        let target = self
            .load_execution_admission_unchecked(&binding.target_session_id)
            .await?
            .ok_or_else(|| denied("Loop target disappeared"))?;
        let state = source
            .supervisor_management
            .as_ref()
            .ok_or_else(|| denied("Supervisor scope missing"))?;
        let link = state
            .links
            .get(&binding.target_session_id)
            .ok_or_else(|| denied("Supervisor link missing"))?;
        let blocked = |session: &Session, tool: &str| -> io::Result<bool> {
            let disabled: BTreeSet<String> = match session.metadata.get("disabled_tools") {
                Some(raw) => serde_json::from_str(raw)
                    .map_err(|_| denied("invalid disabled tools authority"))?,
                None => BTreeSet::new(),
            };
            // This checks one known core capability, not a provider catalog.
            // Unknown qualified references keep the shared denial fallback.
            Ok(disabled.iter().any(|reference| {
                bamboo_domain::resolve_disabled_tool_reference_name(reference, |name| name == tool)
                    .as_deref()
                    == Some(tool)
            }) || session
                .agent_runtime_state
                .as_ref()
                .is_some_and(|runtime| runtime.read_only || runtime.plan_mode.is_some()))
        };
        if source.supervisor_incarnation_id() != Some(binding.supervisor.incarnation_id)
            || source.root_tool_authority_revision != binding.supervisor_tool_authority_revision
            || !source.allows_model_tool_execution("session_control")
            || blocked(&source, "session_control")?
            || blocked(&target, "loop_control")?
            || target.root_orchestration_only_enabled()
            || !link_authorizes_target(state, link, &target)
            || link.revision != binding.link_revision
            || target.created_at != binding.target_created_at
            || target.metadata_version != binding.target_metadata_version
            || target_project(&target)? != binding.target_project_id
            || target
                .agent_runtime_state
                .as_ref()
                .and_then(|runtime| runtime.execution_id.as_deref())
                != Some(binding.execution_id.as_str())
        {
            return Err(denied("Supervisor Loop canonical authority changed"));
        }
        // Read-only native physical authority: a logical slot cannot keep
        // admitting carriers after its Actor lease expires or is superseded.
        let census = super::scoped_transport_expiry::ScopedTransportCensus::read(
            &self.sessions_dir.join(&target.id),
        )?
        .ok_or_else(|| denied("Loop target canonical census missing"))?;
        census.require_live_binding(binding)?;
        guard.scoped_native = Some(Box::new(census));
        guard.scoped_loop_stopped = Self::scoped_loop_execution_stopped(&target, binding)?;
        Ok(guard)
    }

    pub(super) async fn management_history(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<Option<Session>> {
        if target_id == reference.session_id {
            reference_is_valid(reference)?;
            let _lifecycle = self.lock_session_lifecycle_shared().await?;
            let _task = self.lock_runtime_task_sidecar_shared().await?;
            let _sessions = self.management_session_locks(None).await?;
            self.management_supervisor_locked(reference).await?;
            return self.load_session_unlocked(target_id).await;
        }
        self.current_user_supervisor_history(reference, target_id)
            .await
            .map(Some)
    }

    pub(super) async fn management_context(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<(Session, SupervisorLinkObservation)> {
        self.current_user_supervisor_context(reference, target_id)
            .await
    }

    pub(super) async fn supervisor_orchestration_job(
        &self,
        reference: &SupervisorReference,
        expected_revision: u64,
        operation: Box<dyn FnOnce() -> io::Result<()> + Send>,
    ) -> io::Result<()> {
        reference_is_valid(reference)?;
        let lifecycle = self.lock_session_lifecycle_shared().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let sessions = self.management_session_locks(None).await?;
        let current = self.management_supervisor_locked(reference).await?;
        if current.supervisor_orchestration_revision() != Some(expected_revision) {
            return Err(denied("Supervisor orchestration authority changed"));
        }
        let guards = Arc::new(SupervisorManagementGuards {
            sessions,
            _task: task,
            _lifecycle: LifecycleGuard::Shared { _guard: lifecycle },
            _host_namespace: None,
        });
        // The physical job owns the guards through every file publication,
        // including if the originating HTTP future or runtime disappears.
        tokio::task::spawn_blocking(move || {
            let _guards = guards;
            operation()
        })
        .await
        .map_err(|error| other_io_error(format!("join Supervisor publication: {error}")))?
    }

    /// Private loader only: every caller below owns lifecycle, Task and Session
    /// locks. Calling the public authority reader here would re-enter locks.
    async fn management_supervisor_locked(
        &self,
        reference: &SupervisorReference,
    ) -> io::Result<Session> {
        let current = self
            .load_root_authority_unchecked(&reference.session_id)
            .await?
            .ok_or_else(|| denied("Supervisor does not exist"))?;
        if current.authority_identity
            != (SessionAuthorityIdentity::Supervisor {
                incarnation_id: reference.incarnation_id,
            })
        {
            return Err(denied(
                "Supervisor incarnation does not match canonical authority",
            ));
        }
        Ok(current)
    }

    /// Caller retains lifecycle and Task guards. A malformed Child ID must
    /// never select a Root-level lock position. Hold only that target's lock
    /// during preflight, then release it before acquiring the sorted Root set.
    /// The retained lifecycle prevents Root deletion/rebirth; the existing
    /// lifetime fence also forbids replacing a former Root ID with a Child.
    /// Every caller still revalidates its authority under the final lock set.
    async fn preview_management_root(&self, target: &str) -> io::Result<bool> {
        let _target = self.acquire_session_maintenance_lock(target).await?;
        Ok(self.load_root_authority_unchecked(target).await?.is_some())
    }

    async fn management_session_locks(
        &self,
        target: Option<&str>,
    ) -> io::Result<Vec<SessionWriteGuard>> {
        if let Some(target) = target {
            if !self.preview_management_root(target).await? {
                return Err(denied("target Root does not exist"));
            }
        }
        self.management_root_session_locks(target).await
    }

    /// Existing targets have passed strict Root preflight. Only trusted new
    /// Root creation may skip it, while retaining exclusive lifecycle ownership
    /// through publication so its vacant candidate cannot race a Child writer.
    async fn management_root_session_locks(
        &self,
        target: Option<&str>,
    ) -> io::Result<Vec<SessionWriteGuard>> {
        let mut ids = vec![DEFAULT_SUPERVISOR_SESSION_ID];
        if let Some(target) = target {
            ids.push(target);
        }
        ids.sort_unstable();
        ids.dedup();
        let mut guards = Vec::with_capacity(ids.len());
        for id in ids {
            guards.push(self.acquire_session_maintenance_lock(id).await?);
        }
        Ok(guards)
    }

    pub(super) async fn management_scope(
        &self,
        reference: &SupervisorReference,
    ) -> io::Result<SupervisorScopeObservation> {
        reference_is_valid(reference)?;
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _task = self.lock_runtime_task_sidecar_shared().await?;
        let _sessions = self.management_session_locks(None).await?;
        let current = self.management_supervisor_locked(reference).await?;
        let state = current
            .supervisor_management
            .unwrap_or_else(|| empty_state(reference.incarnation_id));
        Ok(SupervisorScopeObservation {
            supervisor: reference.clone(),
            state_revision: state.revision,
            allowed_projects: state.allowed_projects,
        })
    }

    /// A write-only compatibility preflight under the existing physical locks.
    /// Keep bounded authority/history observations readable. Publications also
    /// inspect Main's behavior stamp; a compatible sidecar cannot override it.
    async fn check_management_session_compatibility(&self, current: &Session) -> io::Result<()> {
        reject_incompatible_session_write(current)?;
        let canonical = self
            .load_session_from_dir_strict(
                &self.sessions_dir.join(&current.id),
                &current.id,
                SessionKind::Root,
                &current.id,
            )
            .await?
            .ok_or_else(|| denied("Supervisor disappeared before management publication"))?;
        reject_incompatible_session_write(&canonical)?;
        reject_conversation_generation_change(current, &canonical)
    }

    async fn publish_management_state(
        &self,
        current: &mut Session,
        state: &SupervisorManagementState,
        guards: &Arc<SupervisorManagementGuards>,
    ) -> io::Result<()> {
        state.validate(state.incarnation_id).map_err(invalid)?;
        reject_incompatible_session_write(current)?;
        current.supervisor_management = Some(state.clone());
        // The only writer that may change this field. Revalidation above
        // and the complete lock set remain live through atomic publication.
        // Do not call an ordinary save (which must reject relation changes),
        // update a target, or publish a history-free snapshot into a cache.
        let bytes = serde_json::to_vec_pretty(&runtime_sidecar_snapshot(current))
            .map_err(|error| other_io_error(error.to_string()))?;
        self.prepare_supervisor_management_proof(current, guards)
            .await?;
        self.maybe_fail_supervisor_proof(supervisor_proof::SupervisorProofFault::Prepared)?;
        self.write_management_bytes(
            &self
                .sessions_dir
                .join(&current.id)
                .join(RUNTIME_SIDECAR_FILE),
            bytes,
            supervisor_proof::SupervisorProofFault::Runtime,
            guards,
        )
        .await?;
        self.maybe_fail_supervisor_proof(supervisor_proof::SupervisorProofFault::Runtime)?;
        self.commit_supervisor_management_proof(current, guards)
            .await?;
        self.maybe_fail_supervisor_proof(supervisor_proof::SupervisorProofFault::Committed)?;
        Ok(())
    }

    /// Create and attach only the exact new Ordinary Root reserved by the
    /// Host's existing session-create operation. No arbitrary attach/recreate
    /// authority is exposed to the model. The caller's operation owns its UUID
    /// and birth; both must survive retries unchanged.
    pub async fn create_and_attach_supervised_root(
        &self,
        supervisor: &SupervisorReference,
        expected_state_revision: u64,
        candidate: &Session,
    ) -> io::Result<SupervisorManagementReceipt> {
        self.create_and_attach_supervised_root_inner(
            supervisor,
            expected_state_revision,
            candidate,
            None,
        )
        .await
    }

    /// Generic-caller compatibility only. Retain a real never-installed Host
    /// namespace through all physical Root/link jobs, and compare the original
    /// caller under the existing canonical Supervisor locks. The old explicit
    /// Supervisor API retains its independent authorization contract above.
    pub async fn create_and_attach_supervised_root_observed(
        &self,
        caller: &bamboo_domain::SessionCallerReference,
        supervisor: &SupervisorReference,
        expected_state_revision: u64,
        candidate: &Session,
    ) -> io::Result<SupervisorManagementReceipt> {
        self.create_and_attach_supervised_root_inner(
            supervisor,
            expected_state_revision,
            candidate,
            Some(caller),
        )
        .await
    }

    async fn create_and_attach_supervised_root_inner(
        &self,
        supervisor: &SupervisorReference,
        expected_state_revision: u64,
        candidate: &Session,
        original_caller: Option<&bamboo_domain::SessionCallerReference>,
    ) -> io::Result<SupervisorManagementReceipt> {
        reference_is_valid(supervisor)?;
        target_id_is_valid(&candidate.id)?;
        new_root_candidate_project(candidate)?;
        if original_caller.is_some_and(|caller| caller.session_id != supervisor.session_id) {
            return Err(denied("current-user original caller mismatch"));
        }
        let lifecycle = self.lock_session_lifecycle_exclusive().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let sessions = self
            .management_root_session_locks(Some(&candidate.id))
            .await?;
        let guards = Arc::new(SupervisorManagementGuards {
            sessions,
            _task: task,
            _lifecycle: LifecycleGuard::Exclusive { _guard: lifecycle },
            _host_namespace: None,
        });
        let current = self.management_supervisor_locked(supervisor).await?;
        if original_caller.is_some_and(|caller| !caller.matches_session(&current)) {
            return Err(denied("host_session_operation_caller_changed"));
        }
        self.check_management_session_compatibility(&current)
            .await?;
        let state = current
            .supervisor_management
            .clone()
            .unwrap_or_else(|| empty_state(supervisor.incarnation_id));
        let existing = self.load_root_authority_unchecked(&candidate.id).await?;
        if let Some(target) = existing {
            if !reserved_root_matches(&target, candidate) {
                return Err(denied(
                    "reserved creation target has a different lifetime or context",
                ));
            }
            // Preserve any historical annotation/receipt, but it is not an ACL
            // and an old detach never authorizes replacing this lifetime.
            return Ok(SupervisorManagementReceipt {
                supervisor: supervisor.clone(),
                state_revision: state.revision,
                changed: false,
                link_revision: state.links.get(&candidate.id).map(|link| link.revision),
            });
        }
        if state.revision != expected_state_revision {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                SessionAuthorityConflict("Supervisor state changed before Root creation".into()),
            ));
        }
        // The existing reservation and normal Root writer retain the original
        // caller, candidate, lifecycle/Task/Session locks through publication.
        // No global Project grant or management-link entry is manufactured.
        let writer = DefaultWriterGuards::supervised_creation(guards);
        self.save_session_after_lock(candidate, Instant::now(), &writer, None)
            .await?;
        Ok(SupervisorManagementReceipt {
            supervisor: supervisor.clone(),
            state_revision: state.revision,
            changed: true,
            link_revision: None,
        })
    }

    pub(super) async fn management_mutate(
        &self,
        request: &SupervisorManagementRequest,
    ) -> io::Result<SupervisorManagementReceipt> {
        reference_is_valid(&request.supervisor)?;
        let target = match &request.mutation {
            SupervisorManagementMutation::ConfigureProjectScope { allowed_projects } => {
                if allowed_projects.len() > MAX_SUPERVISOR_PROJECTS {
                    return Err(invalid("Supervisor Project scope capacity exceeded"));
                }
                None
            }
            SupervisorManagementMutation::Attach { target_session_id }
            | SupervisorManagementMutation::Detach { target_session_id } => {
                target_id_is_valid(target_session_id)?;
                Some(target_session_id.as_str())
            }
        };
        let lifecycle = self.lock_session_lifecycle_shared().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        // Detach needs only the verified Supervisor and its stored tombstone.
        // Target corruption/deletion must never prevent revocation.
        let lock_target = matches!(
            request.mutation,
            SupervisorManagementMutation::Attach { .. }
        )
        .then_some(target)
        .flatten();
        let sessions = self.management_session_locks(lock_target).await?;
        let guards = Arc::new(SupervisorManagementGuards {
            sessions,
            _task: task,
            _lifecycle: LifecycleGuard::Shared { _guard: lifecycle },
            _host_namespace: None,
        });
        let mut current = self
            .management_supervisor_locked(&request.supervisor)
            .await?;
        self.check_management_session_compatibility(&current)
            .await?;
        let mut state = current
            .supervisor_management
            .clone()
            .unwrap_or_else(|| empty_state(request.supervisor.incarnation_id));
        if state.revision != request.expected_state_revision {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                SessionAuthorityConflict(
                    "Supervisor management revision changed; reload scope before retrying".into(),
                ),
            ));
        }
        let changed = match &request.mutation {
            SupervisorManagementMutation::ConfigureProjectScope { allowed_projects } => {
                if &state.allowed_projects == allowed_projects {
                    false
                } else {
                    for link in state.links.values_mut() {
                        if link.enabled && !allowed_projects.contains(&link.target_project_id) {
                            link.revision = next_revision(link.revision)?;
                            link.enabled = false;
                        }
                    }
                    state.allowed_projects = allowed_projects.clone();
                    true
                }
            }
            SupervisorManagementMutation::Attach { target_session_id } => {
                let target = self
                    .load_root_authority_unchecked(target_session_id)
                    .await?
                    .ok_or_else(|| denied("target Root does not exist"))?;
                let project = target_project(&target)?;
                if !state.allowed_projects.contains(&project) {
                    return Err(denied(
                        "target Project is outside configured Supervisor scope",
                    ));
                }
                let previous = state.links.get(target_session_id);
                let unchanged = previous.is_some_and(|link| {
                    link.enabled
                        && link.target_created_at == target.created_at
                        && link.target_project_id == project
                        && link.target_metadata_version == target.metadata_version
                });
                if !unchanged {
                    if previous.is_none() && state.links.len() >= MAX_SUPERVISOR_LINKS {
                        return Err(invalid("Supervisor link/tombstone capacity exceeded"));
                    }
                    let revision = next_revision(previous.map_or(0, |link| link.revision))?;
                    state.links.insert(
                        target_session_id.clone(),
                        SupervisorManagedLink {
                            revision,
                            enabled: true,
                            target_created_at: target.created_at,
                            target_project_id: project,
                            target_metadata_version: target.metadata_version,
                        },
                    );
                }
                !unchanged
            }
            SupervisorManagementMutation::Detach { target_session_id } => {
                if let Some(link) = state
                    .links
                    .get_mut(target_session_id)
                    .filter(|link| link.enabled)
                {
                    link.revision = next_revision(link.revision)?;
                    link.enabled = false;
                    true
                } else {
                    false
                }
            }
        };
        if changed {
            state.revision = next_revision(state.revision)?;
            for observer in state.observers.values_mut() {
                if observer.enabled
                    && !observer.current_user_targets
                    && observer
                        .target_links
                        .iter()
                        .any(|(id, link)| state.links.get(id) != Some(link))
                {
                    observer.enabled = false;
                    observer.revision = state.revision;
                }
            }
            self.publish_management_state(&mut current, &state, &guards)
                .await?;
        }
        Ok(SupervisorManagementReceipt {
            supervisor: request.supervisor.clone(),
            state_revision: state.revision,
            changed,
            link_revision: target
                .and_then(|id| state.links.get(id))
                .map(|link| link.revision),
        })
    }

    pub(super) async fn management_link(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<SupervisorLinkObservation> {
        self.current_user_supervisor_link(reference, target_id)
            .await
    }
}

fn observer_receipt(
    reference: &SupervisorReference,
    child_id: &str,
    state_revision: u64,
    registration: &bamboo_domain::SupervisorObserverRegistration,
    changed: bool,
) -> bamboo_domain::SupervisorObserverReceipt {
    bamboo_domain::SupervisorObserverReceipt {
        supervisor: reference.clone(),
        child_session_id: child_id.into(),
        operation_id: registration.operation_id,
        state_revision,
        registration_revision: registration.revision,
        enabled: registration.enabled,
        changed,
    }
}

fn observer_parent_reference(parent: &Session) -> Option<SupervisorReference> {
    match parent.authority_identity {
        SessionAuthorityIdentity::Supervisor { incarnation_id }
            if parent.id == DEFAULT_SUPERVISOR_SESSION_ID && parent.kind == SessionKind::Root =>
        {
            Some(SupervisorReference {
                session_id: parent.id.clone(),
                incarnation_id,
            })
        }
        _ => None,
    }
}

fn observer_native_child(child: &Session) -> bool {
    child.kind == SessionKind::Child
        && child.authority_identity.is_ordinary()
        && child
            .agent_runtime_state
            .as_ref()
            .is_some_and(|state| state.read_only && state.plan_mode.is_none())
        && !child.metadata.keys().any(|key| {
            key.starts_with("external.") || key == "runtime.kind" || key == "child.named_profile.v1"
        })
}

#[cfg(test)]
mod observer_lock_order_tests {
    use super::*;

    #[tokio::test]
    async fn observer_lock_order_rejects_non_root_slots_before_waiting_for_supervisor() {
        let home = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(home.path().into()).await.unwrap();
        let receipt = store
            .get_or_create_default_supervisor("model")
            .await
            .unwrap();
        let reference = SupervisorReference::from(&receipt);
        let parent = store
            .load_root_authority(&receipt.session_id)
            .await
            .unwrap()
            .unwrap();
        let mut child = Session::new_child_of("a-observer", &parent, "model", "observe");
        child
            .agent_runtime_state
            .get_or_insert_with(Default::default)
            .read_only = true;
        store.save_session(&child).await.unwrap();
        let other = Session::new_child_of("a-invalid-target", &parent, "model", "work");
        store.save_session(&other).await.unwrap();
        let root = Session::new("a-independent-root", "model");
        store.save_session(&root).await.unwrap();

        // These IDs sort before the Supervisor. The previous Attach took its
        // Child lock and waited for this Supervisor, while classification held
        // the Supervisor and waited for that same Child. Every malformed call
        // below must now finish before trying the held Supervisor lock.
        let _supervisor = store
            .acquire_session_maintenance_lock(&parent.id)
            .await
            .unwrap();
        let wait = std::time::Duration::from_secs(2);
        let request = SupervisorManagementRequest {
            supervisor: reference.clone(),
            expected_state_revision: 0,
            mutation: SupervisorManagementMutation::Attach {
                target_session_id: child.id.clone(),
            },
        };
        assert!(
            tokio::time::timeout(wait, store.management_mutate(&request))
                .await
                .unwrap()
                .is_err()
        );
        assert!(tokio::time::timeout(
            wait,
            store.register_supervisor_observer(
                &reference,
                0,
                Uuid::new_v4(),
                &child.id,
                BTreeSet::from([other.id.clone()]),
            ),
        )
        .await
        .unwrap()
        .is_err());
        assert!(tokio::time::timeout(
            wait,
            store.register_supervisor_observer(
                &reference,
                0,
                Uuid::new_v4(),
                "a-missing-child",
                BTreeSet::from([root.id.clone()]),
            ),
        )
        .await
        .unwrap()
        .is_err());
        assert!(
            tokio::time::timeout(wait, store.observer_guards(&root.id, [], None, false),)
                .await
                .unwrap()
                .is_err()
        );
    }
}
