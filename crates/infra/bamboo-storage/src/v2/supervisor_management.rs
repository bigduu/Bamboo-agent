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

impl SessionStoreV2 {
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
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _task = self.lock_runtime_task_sidecar_shared().await?;
        // Detach needs only the verified Supervisor and its stored tombstone.
        // Target corruption/deletion must never prevent revocation.
        let lock_target = matches!(
            request.mutation,
            SupervisorManagementMutation::Attach { .. }
        )
        .then_some(target)
        .flatten();
        let _sessions = self.management_session_locks(lock_target).await?;
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
            durable_atomic_write(
                &self
                    .sessions_dir
                    .join(&current.id)
                    .join(RUNTIME_SIDECAR_FILE),
                &bytes,
            )
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
                authorized = target_project(&target).is_ok_and(|project| {
                    project == link.target_project_id && state.allowed_projects.contains(&project)
                }) && target.created_at == link.target_created_at
                    && target.metadata_version == link.target_metadata_version;
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
