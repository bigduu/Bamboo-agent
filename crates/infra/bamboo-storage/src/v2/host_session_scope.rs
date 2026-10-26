//! Final reads for finite Host scopes. Never borrow Supervisor authority.

use super::default_actor_context::LifecycleGuard;
use super::*;
use crate::host_session_scope::{HostSessionScopeAbsence, HostSessionScopeUse};
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
    _lifecycle: LifecycleGuard,
    _scope: Option<HostSessionScopeUse>,
}

impl HostScopedSessionGuards {
    pub(super) fn retains_exclusive_lifecycle(&self) -> bool {
        self._lifecycle.retains_exclusive()
    }

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

/// A SendPeer check is inseparable from these original physical guards. The
/// existing Inbox owns this value until every started filesystem job completes.
pub(crate) struct HostScopedDeliveryGuards {
    pub(crate) inbox_dir: PathBuf,
    pub(crate) identity: bamboo_domain::HostSessionPeerIdentity,
    _authority: HostScopedSessionGuards,
}

enum LegacySessionDeliveryAuthority {
    Peer { _guard: PeerDeliveryGuard },
    Supervisor { _guard: SupervisorFollowupGuard },
}

/// Compatibility authority, with a retained never-installed publication fence.
/// Physical authority drops before its publication guard.
pub(crate) struct LegacySessionDeliveryGuards {
    pub(crate) inbox_dir: PathBuf,
    _authority: LegacySessionDeliveryAuthority,
    _absence: HostSessionScopeAbsence,
}

impl SessionStoreV2 {
    pub(crate) async fn lock_unscoped_peer_delivery(
        &self,
        caller: &SessionCallerReference,
        source: &bamboo_domain::ActorSession,
        target: &bamboo_domain::ActorSession,
    ) -> io::Result<LegacySessionDeliveryGuards> {
        validate_session_id(&caller.session_id)?;
        if caller.session_id != source.actor_id
            || caller.authority_identity != bamboo_domain::SessionAuthorityIdentity::Ordinary
        {
            return Err(denied("host_session_legacy_caller_mismatch"));
        }
        let absence = self
            .host_scope_registry
            .retain_never_installed(&caller.session_id)
            .await?;
        let authority = self.lock_peer_delivery(source, target, None).await?;
        let current = self
            .peer_delivery_session(&caller.session_id, &caller.root_session_id)
            .await?;
        if !caller.matches_session(&current) {
            return Err(denied("host_session_operation_caller_changed"));
        }
        Ok(LegacySessionDeliveryGuards {
            inbox_dir: authority.inbox_dir.clone(),
            _authority: LegacySessionDeliveryAuthority::Peer { _guard: authority },
            _absence: absence,
        })
    }

    pub(crate) async fn lock_unscoped_supervisor_delivery(
        &self,
        caller: &SessionCallerReference,
        supervisor: &bamboo_domain::SupervisorReference,
        target_id: &str,
    ) -> io::Result<LegacySessionDeliveryGuards> {
        validate_session_id(&caller.session_id)?;
        if caller.session_id != supervisor.session_id
            || caller.authority_identity
                != (bamboo_domain::SessionAuthorityIdentity::Supervisor {
                    incarnation_id: supervisor.incarnation_id,
                })
        {
            return Err(denied("host_session_legacy_caller_mismatch"));
        }
        let absence = self
            .host_scope_registry
            .retain_never_installed(&caller.session_id)
            .await?;
        let authority = self.lock_supervisor_followup(supervisor, target_id).await?;
        let current = self
            .peer_delivery_session(&caller.session_id, &caller.root_session_id)
            .await?;
        if !caller.matches_session(&current) {
            return Err(denied("host_session_operation_caller_changed"));
        }
        Ok(LegacySessionDeliveryGuards {
            inbox_dir: self
                .abs_path_from_rel(&Self::root_rel_path(target_id))
                .join("inbox"),
            _authority: LegacySessionDeliveryAuthority::Supervisor { _guard: authority },
            _absence: absence,
        })
    }

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
        // Only a reserved Root birth needs exclusive lifecycle. Ordinary
        // scoped reads keep their existing shared concurrency contract.
        let lifecycle = if extra_root_ids.is_empty() {
            LifecycleGuard::Shared {
                _guard: self.lock_session_lifecycle_shared().await?,
            }
        } else {
            LifecycleGuard::Exclusive {
                _guard: self.lock_session_lifecycle_exclusive().await?,
            }
        };
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

    pub(crate) async fn lock_host_scoped_peer_delivery(
        &self,
        token: &HostSessionScopeToken,
        caller: &SessionCallerReference,
        envelope: &bamboo_domain::SessionMessageEnvelope,
    ) -> io::Result<HostScopedDeliveryGuards> {
        use bamboo_domain::{SessionMessageBody, SessionMessageKind, SessionMessageSource};
        envelope
            .validate()
            .map_err(|error| denied(error.to_string()))?;
        let scope = self.host_scope_registry.retain(token, caller).await?;
        let binding = scope.token().binding();
        let target = binding
            .target(&envelope.target_session_id, HostSessionRight::SendPeer)
            .map_err(|error| denied(error.to_string()))?
            .observation
            .clone();
        let operation = envelope
            .correlation_id
            .as_deref()
            .ok_or_else(|| denied("host_session_send_operation_missing"))?;
        bamboo_domain::SessionMessageId::parse(operation)
            .map_err(|error| denied(error.to_string()))?;
        let identity = bamboo_domain::HostSessionPeerIdentity::for_caller(
            caller,
            &target.identity.session_id,
            target.identity.created_at,
            operation,
        );
        let text = match &envelope.body {
            SessionMessageBody::Content(content)
                if content.parts.is_empty() && content.skill_request.is_none() =>
            {
                &content.text
            }
            _ => return Err(denied("host_session_send_requires_bounded_text")),
        };
        if envelope.source
            != (SessionMessageSource::Session {
                session_id: caller.session_id.clone(),
            })
            || envelope.kind != SessionMessageKind::PeerMessage
            || envelope.id != identity.message_id
            || envelope.thread_id != identity.thread_id
            || envelope.in_reply_to.is_some()
            || envelope.attempt.is_some()
            || text.trim().is_empty()
            || text.contains('\0')
            || text.len() > bamboo_domain::MAX_SESSION_PEER_MESSAGE_BYTES
        {
            return Err(denied("host_session_send_envelope_mismatch"));
        }
        let observations = [binding.caller.clone(), target.clone()];
        let guards = self
            .lock_host_scope_observations(&observations, Some(scope), &[])
            .await?;
        let current = self
            .peer_delivery_session(&caller.session_id, &caller.root_session_id)
            .await?;
        if !caller.matches_session(&current) {
            return Err(denied("host_session_operation_caller_changed"));
        }
        let rel = if target.identity.session_id == target.identity.root_session_id {
            Self::root_rel_path(&target.identity.session_id)
        } else {
            Self::child_rel_path(
                &target.identity.root_session_id,
                &target.identity.session_id,
            )
        };
        Ok(HostScopedDeliveryGuards {
            inbox_dir: self.abs_path_from_rel(&rel).join("inbox"),
            identity,
            _authority: guards,
        })
    }
}

#[cfg(test)]
#[path = "host_session_scope_tests.rs"]
mod tests;
