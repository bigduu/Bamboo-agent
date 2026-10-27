//! Canonical current-user resolution. No ACL population or target mutation.

use super::*;
use bamboo_domain::{
    HostScopeSessionObservation, SupervisorCurrentUserPage, SupervisorCurrentUserScope,
    SupervisorCurrentUserTarget, MAX_SUPERVISOR_DIRECTORY_PAGE,
};

pub(crate) struct CurrentUserTargetGuard {
    pub(crate) source: Session,
    pub(crate) target: Session,
    pub(crate) inbox_dir: PathBuf,
    pub(crate) reference: SupervisorCurrentUserTarget,
    pub(crate) _authority: super::super::host_session_scope::HostScopedSessionGuards,
}

impl SessionStoreV2 {
    pub(crate) async fn current_user_supervisor_scope(
        &self,
        reference: &SupervisorReference,
    ) -> io::Result<SupervisorCurrentUserScope> {
        reference_is_valid(reference)?;
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _task = self.lock_runtime_task_sidecar_shared().await?;
        let _sessions = self.management_session_locks(None).await?;
        let current = self.management_supervisor_locked(reference).await?;
        Ok(current_user_scope(reference, &current))
    }

    /// This first read only selects canonical lock locations. It uses the
    /// existing ambiguity-rejecting Actor locator, never a derived index hint.
    /// The complete observations are checked again by the retained final set.
    async fn current_user_target_source(
        &self,
        target_id: &str,
    ) -> io::Result<(Session, HostScopeSessionObservation)> {
        target_id_is_valid(target_id)?;
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _task = self.lock_runtime_task_sidecar_shared().await?;
        let _target = self.acquire_session_maintenance_lock(target_id).await?;
        let (relative, _) = self
            .actor_authority_location(target_id)
            .await
            .map_err(|error| other_io_error(error.to_string()))?;
        let (_, root_id) = Self::copy_source_identity_from_rel(target_id, &relative)?;
        self.load_root_authority_unchecked(&root_id)
            .await?
            .ok_or_else(|| denied("canonical target Root authority is unavailable"))?;
        let target = self.peer_delivery_session(target_id, &root_id).await?;
        let actor = bamboo_domain::ActorSession::from_session(&target)
            .map_err(|error| denied(&error.to_string()))?;
        if !target.authority_identity.is_ordinary() {
            return Err(denied("current-user target must be an ordinary Session"));
        }
        let ancestors = self
            .validate_actor_lineage(&actor)
            .await
            .map_err(|error| other_io_error(error.to_string()))?;
        let observation = HostScopeSessionObservation::capture(&target, ancestors)
            .map_err(|error| denied(&error.to_string()))?;
        Ok((target, observation))
    }

    pub(crate) async fn lock_current_user_target(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<CurrentUserTargetGuard> {
        reference_is_valid(reference)?;
        target_id_is_valid(target_id)?;
        let proof = self
            .load_root_authority(&reference.session_id)
            .await?
            .ok_or_else(|| denied("canonical Supervisor is unavailable"))?;
        if proof.supervisor_incarnation_id() != Some(reference.incarnation_id) {
            return Err(denied("Supervisor current-user incarnation changed"));
        }
        // Compatible legacy pairs can have different optional session_version
        // values in Main and the control sidecar. Capture the same full merged
        // representation that the final guard compares, after checking proof;
        // never rewrite either file merely to normalize an observation.
        let source = self
            .peer_delivery_session(&reference.session_id, &reference.session_id)
            .await?;
        if source.created_at != proof.created_at
            || source.authority_identity != proof.authority_identity
        {
            return Err(denied("Supervisor proof and full source lifetime changed"));
        }
        let caller = HostScopeSessionObservation::capture(&source, vec![])
            .map_err(|error| denied(&error.to_string()))?;
        let (_, target) = self.current_user_target_source(target_id).await?;
        let authority = self
            .lock_host_scope_observations(&[caller, target.clone()], None, &[])
            .await?;
        // Re-read protected proof while the same source/target/ancestry guards
        // are held. Copying serialized observations never creates authority.
        let proof = self.management_supervisor_locked(reference).await?;
        let source = self
            .peer_delivery_session(&reference.session_id, &reference.session_id)
            .await?;
        if source.created_at != proof.created_at
            || source.authority_identity != proof.authority_identity
        {
            return Err(denied("Supervisor proof and full source lifetime changed"));
        }
        self.load_root_authority_unchecked(&target.identity.root_session_id)
            .await?
            .ok_or_else(|| denied("canonical target Root authority changed"))?;
        let current = self
            .peer_delivery_session(target_id, &target.identity.root_session_id)
            .await?;
        target
            .check_session(&current)
            .map_err(|error| denied(&error.to_string()))?;
        let reference =
            SupervisorCurrentUserTarget::capture_with_ancestors(&current, &target.ancestors)
                .map_err(denied)?;
        let relative = if current.kind == SessionKind::Root {
            Self::root_rel_path(target_id)
        } else {
            Self::child_rel_path(&current.root_session_id, target_id)
        };
        Ok(CurrentUserTargetGuard {
            source,
            target: current,
            inbox_dir: self.abs_path_from_rel(&relative).join("inbox"),
            reference,
            _authority: authority,
        })
    }

    pub(crate) async fn current_user_supervisor_target(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<SupervisorCurrentUserTarget> {
        let guard = self.lock_current_user_target(reference, target_id).await?;
        Ok(guard.reference.clone())
    }

    pub(crate) async fn current_user_supervisor_link(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<SupervisorLinkObservation> {
        let guard = self.lock_current_user_target(reference, target_id).await?;
        Ok(current_user_link(reference, &guard.source, target_id))
    }

    pub(crate) async fn current_user_supervisor_history(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<Session> {
        let guard = self.lock_current_user_target(reference, target_id).await?;
        Ok(guard.target.clone())
    }

    pub(crate) async fn current_user_supervisor_context(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<(Session, SupervisorLinkObservation)> {
        let guard = self.lock_current_user_target(reference, target_id).await?;
        let mut target = guard.target.clone();
        target.messages.clear();
        Ok((
            target,
            current_user_link(reference, &guard.source, target_id),
        ))
    }

    /// Canonical directory enumeration is bounded independently of the page.
    /// Each returned target still goes through the strict retained read above;
    /// a directory name or global index row never acts as an authorization.
    async fn current_user_target_ids(&self) -> io::Result<BTreeSet<String>> {
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let mut entries = fs::read_dir(&self.sessions_dir).await?;
        let mut ids = BTreeSet::new();
        let mut roots = 0usize;
        while let Some(entry) = entries.next_entry().await? {
            let kind = entry.file_type().await?;
            if kind.is_symlink() {
                return Err(denied("canonical Session directory cannot be a symlink"));
            }
            if !kind.is_dir() {
                continue;
            }
            let id = entry
                .file_name()
                .into_string()
                .map_err(|_| invalid("invalid canonical Root directory name"))?;
            validate_session_id(&id)?;
            roots += 1;
            if roots > 4096 {
                return Err(invalid(
                    "canonical Supervisor directory scan budget exceeded",
                ));
            }
            if id != DEFAULT_SUPERVISOR_SESSION_ID {
                ids.insert(id.clone());
            }
            for child in self.canonical_root_child_ids(&id).await? {
                if !ids.insert(child) {
                    return Err(denied("ambiguous canonical Session directory identity"));
                }
            }
            if ids.len() > 16384 {
                return Err(invalid(
                    "canonical Supervisor directory scan budget exceeded",
                ));
            }
        }
        Ok(ids)
    }

    pub(crate) async fn current_user_supervisor_page(
        &self,
        reference: &SupervisorReference,
        query: Option<&str>,
        after: Option<&str>,
        limit: usize,
    ) -> io::Result<SupervisorCurrentUserPage> {
        if !(1..=MAX_SUPERVISOR_DIRECTORY_PAGE).contains(&limit)
            || query.is_some_and(|query| query.len() > 128)
        {
            return Err(invalid("invalid current-user directory bounds"));
        }
        if let Some(after) = after {
            validate_supervisor_target_id(after).map_err(invalid)?;
        }
        let scope = self.current_user_supervisor_scope(reference).await?;
        let query = query
            .map(str::trim)
            .filter(|q| !q.is_empty())
            .map(str::to_lowercase);
        let mut targets = Vec::with_capacity(limit + 1);
        for id in self.current_user_target_ids().await? {
            if after.is_some_and(|after| id.as_str() <= after) {
                continue;
            }
            let target = self.current_user_supervisor_target(reference, &id).await?;
            if query.as_ref().is_some_and(|query| {
                !id.to_lowercase().contains(query)
                    && !target
                        .project_id
                        .as_ref()
                        .is_some_and(|project| project.as_str().to_lowercase().contains(query))
            }) {
                continue;
            }
            targets.push(target);
            if targets.len() > limit {
                break;
            }
        }
        if self.current_user_supervisor_scope(reference).await? != scope {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Supervisor directory caller changed",
            ));
        }
        let next_after = (targets.len() > limit).then(|| targets[limit - 1].session_id.clone());
        targets.truncate(limit);
        Ok(SupervisorCurrentUserPage {
            scope,
            targets,
            next_after,
        })
    }
}

fn current_user_scope(
    reference: &SupervisorReference,
    session: &Session,
) -> SupervisorCurrentUserScope {
    SupervisorCurrentUserScope {
        supervisor: reference.clone(),
        created_at: session.created_at,
        orchestration_revision: session
            .supervisor_orchestration_revision()
            .expect("canonical Supervisor was checked"),
        legacy_state_revision: session
            .supervisor_management
            .as_ref()
            .map_or(0, |s| s.revision),
    }
}

fn current_user_link(
    reference: &SupervisorReference,
    source: &Session,
    target_id: &str,
) -> SupervisorLinkObservation {
    let state = source.supervisor_management.as_ref();
    SupervisorLinkObservation {
        supervisor: reference.clone(),
        state_revision: state.map_or(0, |s| s.revision),
        target_session_id: target_id.to_owned(),
        link: state.and_then(|s| s.links.get(target_id)).cloned(),
        authorized: true,
    }
}
