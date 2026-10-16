//! One canonical Supervisor relationship writer. No target mutation or registry.

use std::collections::{BTreeMap, BTreeSet};

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

fn empty_state(incarnation_id: Uuid) -> SupervisorManagementState {
    SupervisorManagementState {
        schema_version: SUPERVISOR_MANAGEMENT_SCHEMA_VERSION,
        incarnation_id,
        revision: 0,
        allowed_projects: BTreeSet::new(),
        links: BTreeMap::new(),
    }
}

/// One retained instance of the existing authority lock set, never a durable
/// grant or a second lifecycle protocol. Constructed only by the strict reader.
pub(crate) struct SupervisorFollowupGuard {
    sessions: Vec<SessionWriteGuard>,
    _task: RuntimeTaskTransactionReadGuard,
    _lifecycle: SessionLifecycleReadGuard,
}

/// Management publication owns the existing locks, not a new authority grant.
/// Fields release Sessions → Task → lifecycle; pop reverses sorted acquisition.
pub(super) struct SupervisorManagementGuards {
    sessions: Vec<SessionWriteGuard>,
    _task: RuntimeTaskTransactionReadGuard,
    _lifecycle: SessionLifecycleReadGuard,
}

impl Drop for SupervisorManagementGuards {
    fn drop(&mut self) {
        while let Some(_guard) = self.sessions.pop() {}
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

impl SessionStoreV2 {
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
        })
    }

    /// The existing sorted authority lock set, retained through the caller's
    /// physical read/write jobs. Do not call a public writer while holding it.
    pub(crate) async fn lock_scoped_loop_authority(
        &self,
        binding: &bamboo_domain::ScopedLoopControlBinding,
    ) -> io::Result<SupervisorFollowupGuard> {
        binding.validate().map_err(invalid)?;
        let guard = self
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
            Ok(disabled.contains(tool)
                || session
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
        // This guard retains caller, link, Project and exact target lifetime
        // validation through the complete physical read and overlay checks.
        let _guard = self.lock_supervisor_followup(reference, target_id).await?;
        self.load_session_unlocked(target_id).await
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
        if !current.root_orchestration_only_enabled()
            || expected_revision == 0
            || current.root_tool_authority_revision != expected_revision
        {
            return Err(denied("Supervisor orchestration authority changed"));
        }
        let guards = Arc::new(SupervisorManagementGuards {
            sessions,
            _task: task,
            _lifecycle: lifecycle,
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

    async fn management_session_locks(
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
            _lifecycle: lifecycle,
        });
        let mut current = self
            .management_supervisor_locked(&request.supervisor)
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
            state
                .validate(request.supervisor.incarnation_id)
                .map_err(invalid)?;
            current.supervisor_management = Some(state.clone());
            // The only writer that may change this field. Revalidation above
            // and the complete lock set remain live through atomic publication.
            // Do not call an ordinary save (which must reject relation changes),
            // update a target, or publish a history-free snapshot into a cache.
            let bytes = serde_json::to_vec_pretty(&runtime_sidecar_snapshot(&current))
                .map_err(|error| other_io_error(error.to_string()))?;
            self.prepare_supervisor_management_proof(&current, &guards)
                .await?;
            self.maybe_fail_supervisor_proof(supervisor_proof::SupervisorProofFault::Prepared)?;
            self.write_management_bytes(
                &self
                    .sessions_dir
                    .join(&current.id)
                    .join(RUNTIME_SIDECAR_FILE),
                bytes,
                supervisor_proof::SupervisorProofFault::Runtime,
                &guards,
            )
            .await?;
            self.maybe_fail_supervisor_proof(supervisor_proof::SupervisorProofFault::Runtime)?;
            self.commit_supervisor_management_proof(&current, &guards)
                .await?;
            self.maybe_fail_supervisor_proof(supervisor_proof::SupervisorProofFault::Committed)?;
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
        reference_is_valid(reference)?;
        target_id_is_valid(target_id)?;
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _task = self.lock_runtime_task_sidecar_shared().await?;
        let _sessions = self.management_session_locks(Some(target_id)).await?;
        let current = self.management_supervisor_locked(reference).await?;
        let state = current
            .supervisor_management
            .unwrap_or_else(|| empty_state(reference.incarnation_id));
        let link = state.links.get(target_id).cloned();
        let mut authorized = false;
        if let Some(link) = link.as_ref().filter(|link| link.enabled) {
            if let Some(target) = self.load_root_authority_unchecked(target_id).await? {
                authorized = link_authorizes_target(&state, link, &target);
            }
        }
        Ok(SupervisorLinkObservation {
            supervisor: reference.clone(),
            state_revision: state.revision,
            target_session_id: target_id.to_string(),
            link,
            authorized,
        })
    }
}
