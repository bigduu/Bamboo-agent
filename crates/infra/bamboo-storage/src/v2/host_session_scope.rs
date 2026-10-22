//! Final reads for finite Host scopes. Never borrow Supervisor authority.

use super::*;
use crate::host_session_scope::HostSessionScopeUse;
use bamboo_domain::{
    HostScopeSessionObservation, HostSessionRight, HostSessionScopeBinding, HostSessionScopeToken,
    HostSessionTargetScope, SessionCallerReference,
};
use std::collections::BTreeSet;

fn denied(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message.into())
}

/// Publication → lifecycle → Task → canonical depth/ID Sessions → sorted trees.
/// The same set is retained by any owned physical Inbox job using this scope.
pub(crate) struct HostScopedSessionGuards {
    trees: Vec<ActorTreeWriteGuard>,
    tree_roots: BTreeSet<String>,
    sessions: Vec<SessionWriteGuard>,
    _task: RuntimeTaskTransactionReadGuard,
    _lifecycle: SessionLifecycleReadGuard,
    _scope: Option<HostSessionScopeUse>,
}

impl HostScopedSessionGuards {
    /// Report only the physical tree guards successfully acquired by this set.
    /// Writers use this owned authority to avoid reacquiring the same lock.
    pub(super) fn holds_tree(&self, root_id: &str) -> bool {
        self.tree_roots.contains(root_id)
    }
}

impl Drop for HostScopedSessionGuards {
    fn drop(&mut self) {
        while let Some(guard) = self.trees.pop() {
            drop(guard);
        }
        while let Some(guard) = self.sessions.pop() {
            drop(guard);
        }
    }
}

impl SessionStoreV2 {
    /// Explicit trusted in-process Host installation, never a model tool/HTTP
    /// capability. Empty/default stores and ordinary save paths install nothing.
    pub async fn install_host_session_scope(
        &self,
        expected_revision: u64,
        binding: HostSessionScopeBinding,
    ) -> io::Result<HostSessionScopeToken> {
        binding
            .validate()
            .map_err(|error| denied(error.to_string()))?;
        let publication = self
            .host_scope_registry
            .begin_install(&binding.caller.identity.session_id, expected_revision)
            .await?;
        let observations: Vec<_> = std::iter::once(binding.caller.clone())
            .chain(
                binding
                    .targets
                    .values()
                    .map(|target| target.observation.clone()),
            )
            .collect();
        let _guards = self
            .lock_host_scope_observations(&observations, None, &[])
            .await?;
        // All filesystem observations completed under the physical guards. No
        // scope is published when validation fails or this awaiter disappears.
        publication.install(binding)
    }

    pub async fn revoke_host_session_scope(
        &self,
        caller_id: &str,
        expected_revision: u64,
    ) -> io::Result<u64> {
        validate_session_id(caller_id)?;
        self.host_scope_registry
            .revoke(caller_id, expected_revision)
            .await
    }

    async fn check_host_scope_lineage(
        &self,
        observation: &HostScopeSessionObservation,
        mut current: Session,
    ) -> io::Result<()> {
        let root = &observation.identity.root_session_id;
        for expected in &observation.ancestors {
            let parent_id = current
                .parent_session_id
                .as_deref()
                .ok_or_else(|| denied("host_session_scope_ancestry_changed"))?;
            if parent_id != expected.actor_id {
                return Err(denied("host_session_scope_ancestry_changed"));
            }
            let parent = self.peer_delivery_session(parent_id, root).await?;
            parent
                .validate_conversation_state()
                .map_err(|error| denied(error.to_string()))?;
            if parent.id != expected.actor_id
                || parent.created_at != expected.session_created_at
                || parent.metadata_version != expected.metadata_version
                || parent.project_id_meta() != current.project_id_meta()
                || current.parent_created_at != Some(parent.created_at)
                || parent.spawn_depth.checked_add(1) != Some(current.spawn_depth)
            {
                return Err(denied("host_session_scope_ancestry_changed"));
            }
            current = parent;
        }
        if current.id != *root
            || current.kind != SessionKind::Root
            || current.parent_session_id.is_some()
            || current.spawn_depth != 0
        {
            return Err(denied("host_session_scope_ancestry_changed"));
        }
        Ok(())
    }

    pub(super) async fn lock_host_scope_observations(
        &self,
        observations: &[HostScopeSessionObservation],
        scope: Option<HostSessionScopeUse>,
        extra_root_ids: &[String],
    ) -> io::Result<HostScopedSessionGuards> {
        let lifecycle = self.lock_session_lifecycle_shared().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let mut ordered = BTreeSet::new();
        let mut roots = BTreeSet::new();
        // A creation-purpose writer may reserve its new Root before creating
        // any files. These are lock locations, never observations or grants.
        if extra_root_ids.len() > bamboo_domain::MAX_HOST_SESSION_CREATES {
            return Err(denied("host_session_scope_reserved_root_limit"));
        }
        for id in extra_root_ids {
            validate_session_id(id)?;
            ordered.insert((0, id.clone()));
            roots.insert(id.clone());
        }
        for observation in observations {
            observation
                .validate()
                .map_err(|error| denied(error.to_string()))?;
            let expected = &observation.identity;
            validate_session_id(&expected.session_id)?;
            validate_session_id(&expected.root_session_id)?;
            // Canonical preflight only selects a lock order. It cannot authorize
            // a later read; every observation is checked again under the set.
            let current = self
                .peer_delivery_session(&expected.session_id, &expected.root_session_id)
                .await?;
            observation
                .check_session(&current)
                .map_err(|error| denied(error.to_string()))?;
            current
                .check_execution_compatibility()
                .map_err(|error| denied(error.to_string()))?;
            ordered.insert((0, expected.root_session_id.clone()));
            ordered.insert((current.spawn_depth, current.id.clone()));
            roots.insert(current.root_session_id.clone());
        }
        let mut sessions = Vec::with_capacity(ordered.len());
        for (_, id) in ordered {
            sessions.push(self.acquire_session_maintenance_lock(&id).await?);
        }
        let mut trees = Vec::with_capacity(roots.len());
        let mut tree_roots = BTreeSet::new();
        for root in roots {
            trees.push(self.acquire_actor_tree_write_guard(&root).await?);
            tree_roots.insert(root);
        }
        let guards = HostScopedSessionGuards {
            trees,
            tree_roots,
            sessions,
            _task: task,
            _lifecycle: lifecycle,
            _scope: scope,
        };
        for observation in observations {
            let expected = &observation.identity;
            let current = self
                .peer_delivery_session(&expected.session_id, &expected.root_session_id)
                .await?;
            observation
                .check_session(&current)
                .map_err(|error| denied(error.to_string()))?;
            current
                .check_execution_compatibility()
                .map_err(|error| denied(error.to_string()))?;
            self.check_host_scope_lineage(observation, current).await?;
        }
        Ok(guards)
    }

    pub(super) async fn list_host_scoped_targets_impl(
        &self,
        token: &HostSessionScopeToken,
        caller: &SessionCallerReference,
    ) -> io::Result<Vec<HostSessionTargetScope>> {
        let scope = self.host_scope_registry.retain(token, caller).await?;
        let binding = scope.token().binding();
        let targets: Vec<_> = binding
            .targets
            .values()
            .filter(|target| target.rights.contains(&HostSessionRight::ListMetadata))
            .cloned()
            .collect();
        let observations: Vec<_> = std::iter::once(binding.caller.clone())
            .chain(targets.iter().map(|target| target.observation.clone()))
            .collect();
        let _guards = self
            .lock_host_scope_observations(&observations, Some(scope), &[])
            .await?;
        let current = self
            .peer_delivery_session(&caller.session_id, &caller.root_session_id)
            .await?;
        // The stable grant survives reset; the original operation never does.
        if !caller.matches_session(&current) {
            return Err(denied("host_session_operation_caller_changed"));
        }
        Ok(targets)
    }

    pub(super) async fn load_host_scoped_target_impl(
        &self,
        token: &HostSessionScopeToken,
        caller: &SessionCallerReference,
        target_id: &str,
    ) -> io::Result<Session> {
        let scope = self.host_scope_registry.retain(token, caller).await?;
        let binding = scope.token().binding();
        let target = binding
            .target(target_id, HostSessionRight::InspectBoundedProgress)
            .map_err(|error| denied(error.to_string()))?
            .observation
            .clone();
        let observations = [binding.caller.clone(), target.clone()];
        let _guards = self
            .lock_host_scope_observations(&observations, Some(scope), &[])
            .await?;
        let current = self
            .peer_delivery_session(&caller.session_id, &caller.root_session_id)
            .await?;
        if !caller.matches_session(&current) {
            return Err(denied("host_session_operation_caller_changed"));
        }
        self.peer_delivery_session(
            &target.identity.session_id,
            &target.identity.root_session_id,
        )
        .await
    }
}

#[cfg(test)]
#[path = "host_session_scope_tests.rs"]
mod tests;
