//! Canonical current-user resolution. No ACL population or target mutation.

use super::*;
use bamboo_domain::{
    HostScopeSessionObservation, SupervisorCurrentUserPage, SupervisorCurrentUserScope,
    SupervisorCurrentUserTarget, MAX_SUPERVISOR_DIRECTORY_PAGE,
};

pub(crate) struct CurrentUserTargetGuard {
    pub(crate) lock_wait: SessionLockWait,
    pub(crate) source: Session,
    pub(crate) target: Session,
    pub(crate) inbox_dir: PathBuf,
    pub(crate) reference: SupervisorCurrentUserTarget,
    pub(crate) _authority: super::super::host_session_scope::HostScopedSessionGuards,
    // Drop the physical locks before the namespace publication fence.
    pub(crate) _send_scope: Option<super::super::host_session_scope::CurrentUserSendScope>,
}

impl SessionStoreV2 {
    /// Run a synchronous Host Root-control effect while retaining the original
    /// Supervisor, target, ancestry and never-installed finite-scope fences.
    /// The caller must be the original executing observation; neither it nor
    /// `target` is a grant. Finite Host scopes do not currently confer a cancel
    /// right, so an installed or previously revoked scope fails closed.
    ///
    /// The effect is moved into an owned blocking job with the guards: dropping
    /// the async caller cannot release authority before an already-started
    /// signal. The callback receives the canonical Workspace path while its
    /// guard is held; it must not await or reenter Session storage.
    pub async fn with_unscoped_supervisor_root_control<T, F>(
        &self,
        caller: &bamboo_domain::SessionCallerReference,
        supervisor: &SupervisorReference,
        target: &SupervisorCurrentUserTarget,
        effect: F,
    ) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Option<&str>) -> io::Result<T> + Send + 'static,
    {
        let authority = self
            .lock_unscoped_supervisor_root(caller, supervisor, target)
            .await?;
        tokio::task::spawn_blocking(move || {
            let workspace_path = authority.target.workspace_path_meta();
            let result = effect(workspace_path.as_deref());
            // Keep the physical locks alive for the complete blocking effect.
            drop(authority);
            result
        })
        .await
        .map_err(|error| io::Error::other(format!("Root control job failed: {error}")))?
    }

    /// Read both canonical Main sessions under the original Supervisor,
    /// target and finite-scope fences. The callback must not reenter storage.
    pub async fn with_unscoped_supervisor_root_state<T, F>(
        &self,
        caller: &bamboo_domain::SessionCallerReference,
        supervisor: &SupervisorReference,
        target: &SupervisorCurrentUserTarget,
        effect: F,
    ) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Session, &Session) -> io::Result<T> + Send + 'static,
    {
        let (authority, source, current) = self
            .lock_supervisor_approval(caller, supervisor, target)
            .await?;
        tokio::task::spawn_blocking(move || {
            let result = effect(&source, &current);
            drop(authority);
            result
        })
        .await
        .map_err(|error| io::Error::other(format!("Root approval job failed: {error}")))?
    }

    pub(crate) async fn lock_supervisor_approval(
        &self,
        caller: &bamboo_domain::SessionCallerReference,
        supervisor: &SupervisorReference,
        target: &SupervisorCurrentUserTarget,
    ) -> io::Result<(CurrentUserTargetGuard, Session, Session)> {
        let authority = self
            .lock_unscoped_supervisor_root(caller, supervisor, target)
            .await?;
        let source = self
            .peer_delivery_session(&caller.session_id, &caller.session_id)
            .await?;
        let current = self
            .peer_delivery_session(&target.session_id, &target.session_id)
            .await?;
        if !caller.matches_session(&source) || !target.matches(&current) {
            return Err(denied("Managed approval authority changed"));
        }
        Ok((authority, source, current))
    }

    pub(crate) async fn lock_unscoped_supervisor_root(
        &self,
        caller: &bamboo_domain::SessionCallerReference,
        supervisor: &SupervisorReference,
        target: &SupervisorCurrentUserTarget,
    ) -> io::Result<CurrentUserTargetGuard> {
        if caller.session_id != supervisor.session_id
            || caller.authority_identity
                != (SessionAuthorityIdentity::Supervisor {
                    incarnation_id: supervisor.incarnation_id,
                })
            || target.kind != SessionKind::Root
            || target.session_id == caller.session_id
        {
            return Err(denied("invalid Supervisor Root control identity"));
        }
        // Acquire publication before lifecycle/Task/Session/tree locks, as for
        // the existing unscoped Host delivery boundary.
        let scope = self
            .host_scope_registry
            .retain_never_installed(&caller.session_id)
            .await?;
        let mut authority = self
            .lock_current_user_target(supervisor, &target.session_id)
            .await?;
        if !caller.matches_session(&authority.source) || *target != authority.reference {
            return Err(denied("Supervisor caller or Root target changed"));
        }
        authority._send_scope = Some(
            super::host_session_scope::CurrentUserSendScope::NeverInstalled { _absence: scope },
        );
        Ok(authority)
    }

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
        wait: SessionLockWait,
    ) -> io::Result<Option<(Session, HostScopeSessionObservation)>> {
        target_id_is_valid(target_id)?;
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _task = self.lock_runtime_task_sidecar_shared().await?;
        let _target = match wait {
            SessionLockWait::Wait => self.acquire_session_maintenance_lock(target_id).await?,
            SessionLockWait::Try => self.try_acquire_session_maintenance_lock(target_id).await?,
        };
        let (relative, _) = self
            .actor_authority_location(target_id)
            .await
            .map_err(|error| other_io_error(error.to_string()))?;
        let (_, root_id) = Self::copy_source_identity_from_rel(target_id, &relative)?;
        self.load_root_authority_unchecked(&root_id)
            .await?
            .ok_or_else(|| denied("canonical target Root authority is unavailable"))?;
        let target = self
            .peer_delivery_control_session(target_id, &root_id)
            .await?;
        let actor = bamboo_domain::ActorSession::from_session(&target)
            .map_err(|error| denied(&error.to_string()))?;
        if !target.authority_identity.is_ordinary() {
            return Err(denied("current-user target must be an ordinary Session"));
        }
        // Pre-schema children predate ancestor birth proofs: no `parent_created_at`
        // and no actor-authority record, so strict lineage validation can never
        // succeed. They carry no usable managed-target authority, so exclude them
        // from the directory rather than failing the whole page (#2424).
        if target.kind == bamboo_domain::SessionKind::Child && target.parent_created_at.is_none() {
            return Ok(None);
        }
        let ancestors = self
            .validate_actor_control_lineage(&actor)
            .await
            .map_err(|error| other_io_error(error.to_string()))?;
        let observation = HostScopeSessionObservation::capture(&target, ancestors)
            .map_err(|error| denied(&error.to_string()))?;
        Ok(Some((target, observation)))
    }

    pub(in crate::v2) async fn current_user_peer_session(
        &self,
        caller: &bamboo_domain::SessionCallerReference,
        target_id: &str,
    ) -> io::Result<Session> {
        if !caller.is_ordinary_root() {
            return Err(denied(
                "current-user peer history requires an ordinary Root",
            ));
        }
        let _scope = self
            .host_scope_registry
            .retain_never_installed(&caller.session_id)
            .await?;
        let wait = SessionLockWait::Wait;
        let (source, source_observation) = self
            .current_user_target_source(&caller.session_id, wait)
            .await?
            .ok_or_else(|| denied("current-user history caller is unavailable"))?;
        validate_peer_history_caller(caller, &source)?;
        let (_, target) = self
            .current_user_target_source(target_id, wait)
            .await?
            .ok_or_else(|| denied("current-user history target is unavailable"))?;
        let target_root = target.identity.root_session_id.clone();
        let _guards = self
            .lock_host_scope_control_observations(&[source_observation, target], wait)
            .await?;
        for root_id in [&caller.root_session_id, &target_root] {
            self.load_root_authority_unchecked(root_id)
                .await?
                .ok_or_else(|| denied("current-user history Root is unavailable"))?;
        }
        let current = self
            .peer_delivery_control_session(&caller.session_id, &caller.root_session_id)
            .await?;
        validate_peer_history_caller(caller, &current)?;
        self.peer_delivery_session(target_id, &target_root).await
    }

    pub(crate) async fn lock_current_user_target(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<CurrentUserTargetGuard> {
        self.lock_current_user_target_with_wait(reference, target_id, super::SessionLockWait::Wait)
            .await
    }

    pub(crate) async fn try_lock_current_user_target(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<CurrentUserTargetGuard> {
        self.lock_current_user_target_with_wait(reference, target_id, super::SessionLockWait::Try)
            .await
    }

    async fn lock_current_user_target_with_wait(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
        wait: super::SessionLockWait,
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
            .peer_delivery_control_session(&reference.session_id, &reference.session_id)
            .await?;
        if source.created_at != proof.created_at
            || source.authority_identity != proof.authority_identity
        {
            return Err(denied("Supervisor proof and full source lifetime changed"));
        }
        let caller = HostScopeSessionObservation::capture(&source, vec![])
            .map_err(|error| denied(&error.to_string()))?;
        let (_, target) = self
            .current_user_target_source(target_id, wait)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "current-user target has no usable actor authority",
                )
            })?;
        let authority = self
            .lock_host_scope_control_observations(&[caller, target.clone()], wait)
            .await?;
        // Re-read protected proof while the same source/target/ancestry guards
        // are held. Copying serialized observations never creates authority.
        let proof = self.management_supervisor_locked(reference).await?;
        let source = self
            .peer_delivery_control_session(&reference.session_id, &reference.session_id)
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
            .peer_delivery_control_session(target_id, &target.identity.root_session_id)
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
            lock_wait: wait,
            source,
            target: current,
            inbox_dir: self.abs_path_from_rel(&relative).join("inbox"),
            reference,
            _authority: authority,
            _send_scope: None,
        })
    }

    pub(crate) async fn current_user_supervisor_target(
        &self,
        reference: &SupervisorReference,
        target_id: &str,
    ) -> io::Result<Option<SupervisorCurrentUserTarget>> {
        let guard = match self.lock_current_user_target(reference, target_id).await {
            Ok(guard) => guard,
            // Excluded targets (pre-schema children with no actor authority)
            // are absent from the directory listing, not a page error (#2424).
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(Some(guard.reference.clone()))
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
        self.peer_delivery_session(target_id, &guard.target.root_session_id)
            .await
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

    async fn current_user_project_candidate(&self, id: &str, query: &str) -> io::Result<bool> {
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _task = self.lock_runtime_task_sidecar_shared().await?;
        let _session = self.acquire_session_maintenance_lock(id).await?;
        let (relative, _) = self
            .actor_authority_location(id)
            .await
            .map_err(|error| other_io_error(error.to_string()))?;
        let raw = match fs::read(self.abs_path_from_rel(&relative).join(RUNTIME_SIDECAR_FILE)).await
        {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // A legacy Child may have only canonical Main. Reuse its strict
                // control view; Root runtime/proof and Child lifetime checks
                // remain intact, and messages are never decoded for a query.
                let (_, root_id) = Self::copy_source_identity_from_rel(id, &relative)?;
                let candidate = self.peer_delivery_control_session(id, &root_id).await?;
                return Ok(candidate
                    .project_id_meta()
                    .is_some_and(|project| project.to_lowercase().contains(query)));
            }
            Err(error) => return Err(error),
        };
        #[derive(Deserialize, Default)]
        struct Project {
            #[serde(default)]
            project_id: Option<String>,
        }
        #[derive(Deserialize)]
        struct Candidate {
            #[serde(default)]
            metadata: Project,
            #[serde(default)]
            runtime_metadata: Option<Project>,
        }
        let candidate: Candidate =
            serde_json::from_slice(&raw).map_err(|error| other_io_error(error.to_string()))?;
        Ok(candidate
            .runtime_metadata
            .and_then(|m| m.project_id)
            .or(candidate.metadata.project_id)
            .is_some_and(|project| project.to_lowercase().contains(query)))
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
            // Filter bounded control metadata before any canonical Main read.
            // This candidate is never a grant: selected rows are revalidated
            // below, including the query after the final protected read.
            if let Some(query) = query.as_deref() {
                if !id.to_lowercase().contains(query)
                    && !self.current_user_project_candidate(&id, query).await?
                {
                    continue;
                }
            }
            let Some(target) = self.current_user_supervisor_target(reference, &id).await? else {
                continue;
            };
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

fn validate_peer_history_caller(
    caller: &bamboo_domain::SessionCallerReference,
    source: &Session,
) -> io::Result<()> {
    if !caller.matches_session(source) {
        return Err(denied("current-user history caller changed"));
    }
    if source
        .metadata
        .contains_key(bamboo_domain::CONNECT_SESSION_ORIGIN_KEY)
    {
        return Err(denied(
            "current-user peer history is unavailable to a Connect chat",
        ));
    }
    Ok(())
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

#[cfg(test)]
mod observed_history_guard_tests {
    use super::*;
    use bamboo_domain::{Message, Storage};
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn observed_history_guard_fences_same_id_recreation_until_snapshot_finishes(
    ) -> io::Result<()> {
        let home = tempfile::tempdir()?;
        let store = Arc::new(SessionStoreV2::new(home.path().to_path_buf()).await?);
        let supervisor: SupervisorReference = (&store
            .get_or_create_default_supervisor("synthetic-model")
            .await?)
            .into();
        let mut original = Session::new("observed-history-root", "synthetic-model");
        original.add_message(Message::user("synthetic original"));
        store.save_session(&original).await?;

        // This is the exact guard retained by the public observed-session port.
        let guard = store
            .lock_current_user_target(&supervisor, &original.id)
            .await?;
        let old_observation = guard.reference.clone();
        let writer_store = Arc::clone(&store);
        let target_id = original.id.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let mut writer = tokio::spawn(async move {
            let _ = started.send(());
            assert!(writer_store.delete_session(&target_id).await?);
            let mut replacement = writer_store
                .recreate_root_session(&target_id, "synthetic-model")
                .await?;
            replacement.add_message(Message::user("synthetic replacement"));
            writer_store.save_session(&replacement).await?;
            Ok::<_, io::Error>(replacement)
        });
        ready.await.map_err(io::Error::other)?;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut writer)
                .await
                .is_err(),
            "recreation must wait while the observed history guard is held"
        );
        let guarded = store
            .peer_delivery_session(&original.id, &guard.target.root_session_id)
            .await?;
        assert_eq!(guarded.created_at, old_observation.created_at);
        assert_eq!(guarded.messages[0].content, "synthetic original");
        drop(guard);

        let replacement = writer.await.map_err(io::Error::other)??;
        let (history, new_observation) = store
            .load_supervisor_current_user_observed_session(&supervisor, &original.id)
            .await?;
        assert_ne!(new_observation.created_at, old_observation.created_at);
        assert_eq!(history.created_at, replacement.created_at);
        assert_eq!(history.created_at, new_observation.created_at);
        assert_eq!(history.messages[0].content, "synthetic replacement");
        Ok(())
    }
    #[tokio::test]
    async fn bound_history_rejects_stale_target() -> io::Result<()> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let supervisor: SupervisorReference = (&store
            .get_or_create_default_supervisor("synthetic-model")
            .await?)
            .into();
        let mut original = Session::new("bound-history-checks", "synthetic-model");
        original.add_message(Message::user("synthetic durable"));
        store.save_session(&original).await?;
        let expected = store
            .inspect_supervisor_current_user_target(&supervisor, &original.id)
            .await?;
        let mut stale = expected.clone();
        stale.metadata_version += 1;
        let error = store
            .load_supervisor_current_user_bound_history(&supervisor, &stale)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        let (history, observed) = store
            .load_supervisor_current_user_bound_history(&supervisor, &expected)
            .await?;
        assert_eq!(observed, expected);
        assert_eq!(history.messages[0].content, "synthetic durable");
        Ok(())
    }
}
