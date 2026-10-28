//! A creation permit is distinct from target SendPeer or management authority.

use super::*;
use bamboo_domain::{
    host_session_create_identity, HostScopeSessionObservation, HostSessionScopeToken,
    SessionCallerReference,
};

/// Constructed only after a current creation permit and both original Session
/// observations have been checked under the complete physical authority set.
pub(crate) struct HostCreatedRootDeliveryGuards {
    pub(crate) inbox_dir: PathBuf,
    _authority: super::host_session_scope::HostScopedSessionGuards,
}

fn denied(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message.into())
}

fn root_first_input_request(
    caller: &SessionCallerReference,
    operation_id: Uuid,
    candidate: &Session,
    envelope: &bamboo_domain::SessionMessageEnvelope,
) -> io::Result<(bamboo_domain::ProjectId, String)> {
    use bamboo_domain::{SessionMessageBody, SessionMessageKind, SessionMessageSource};
    let project = super::supervisor_management::new_root_candidate_project(candidate)?;
    let workspace = candidate
        .workspace_path_meta()
        .ok_or_else(|| denied("host_session_create_workspace_missing"))?;
    let (id, thread) =
        bamboo_domain::host_root_first_input_identity(caller, operation_id, candidate)
            .map_err(denied)?;
    envelope
        .validate()
        .map_err(|error| denied(error.to_string()))?;
    let text = match &envelope.body {
        SessionMessageBody::Content(content)
            if content.parts.is_empty() && content.skill_request.is_none() =>
        {
            &content.text
        }
        _ => return Err(denied("host_root_first_input_requires_bounded_text")),
    };
    if envelope.source
        != (SessionMessageSource::Session {
            session_id: caller.session_id.clone(),
        })
        || envelope.target_session_id != candidate.id
        || envelope.kind != SessionMessageKind::PeerMessage
        || envelope.id != id
        || envelope.thread_id != thread
        || envelope.correlation_id.as_deref() != Some(operation_id.to_string().as_str())
        || envelope.in_reply_to.is_some()
        || envelope.attempt.is_some()
        || text.trim().is_empty()
        || text.contains('\0')
        || text.len() > bamboo_domain::MAX_SESSION_PEER_MESSAGE_BYTES
    {
        return Err(denied("host_root_first_input_envelope_mismatch"));
    }
    Ok((project, workspace))
}

impl SessionStoreV2 {
    // Caller must own the complete final target authority before this read.
    async fn check_first_input_reserved_root(&self, candidate: &Session) -> io::Result<()> {
        let current = self
            .load_root_authority_unchecked(&candidate.id)
            .await?
            .ok_or_else(|| denied("host_root_first_input_reserved_root_missing"))?;
        if !super::supervisor_management::reserved_root_matches(&current, candidate)
            || !SessionCallerReference::from(candidate).matches_session(&current)
        {
            return Err(denied("host_root_first_input_reserved_root_changed"));
        }
        Ok(())
    }

    pub(crate) async fn lock_unscoped_supervisor_root_first_input(
        &self,
        caller: &SessionCallerReference,
        supervisor: &bamboo_domain::SupervisorReference,
        operation_id: Uuid,
        candidate: &Session,
        envelope: &bamboo_domain::SessionMessageEnvelope,
    ) -> io::Result<super::host_session_scope::LegacySessionDeliveryGuards> {
        root_first_input_request(caller, operation_id, candidate, envelope)?;
        let authority = self
            .lock_unscoped_supervisor_delivery(
                caller,
                supervisor,
                &candidate.id,
                candidate.created_at,
            )
            .await?;
        self.check_first_input_reserved_root(candidate).await?;
        Ok(authority)
    }

    pub(crate) async fn lock_host_root_first_input(
        &self,
        token: &HostSessionScopeToken,
        caller: &SessionCallerReference,
        operation_id: Uuid,
        candidate: &Session,
        envelope: &bamboo_domain::SessionMessageEnvelope,
    ) -> io::Result<HostCreatedRootDeliveryGuards> {
        let (project, workspace) =
            root_first_input_request(caller, operation_id, candidate, envelope)?;
        let scope = self.host_scope_registry.retain(token, caller).await?;
        let binding = scope.token().binding();
        binding
            .create(operation_id, &project, &workspace)
            .map_err(|error| denied(error.to_string()))?;
        let target = HostScopeSessionObservation::capture(candidate, vec![])
            .map_err(|error| denied(error.to_string()))?;
        let observations = [binding.caller.clone(), target];
        let authority = self
            .lock_host_scope_observations(&observations, Some(scope), &[])
            .await?;
        let current = self
            .peer_delivery_session(&caller.session_id, &caller.root_session_id)
            .await?;
        if !caller.matches_session(&current) {
            return Err(denied("host_session_operation_caller_changed"));
        }
        self.check_first_input_reserved_root(candidate).await?;
        Ok(HostCreatedRootDeliveryGuards {
            inbox_dir: self
                .abs_path_from_rel(&Self::root_rel_path(&candidate.id))
                .join("inbox"),
            _authority: authority,
        })
    }

    /// Trusted Host-only final writer. `expected_revision` is required for a new
    /// operation. Only the existing durable, claimed creation reservation may
    /// omit it on an exact retry; the current exact permit is still mandatory.
    /// This creates neither a management link nor a general follow-up grant.
    pub async fn create_host_scoped_root(
        &self,
        token: &HostSessionScopeToken,
        caller: &SessionCallerReference,
        operation_id: Uuid,
        expected_revision: Option<u64>,
        candidate: &Session,
    ) -> io::Result<()> {
        let (_, reserved_id) =
            host_session_create_identity(caller, operation_id).map_err(denied)?;
        if candidate.id != reserved_id || candidate.id == caller.session_id {
            return Err(denied("host_session_create_reservation_changed"));
        }
        let project = super::supervisor_management::new_root_candidate_project(candidate)?;
        let workspace = candidate
            .workspace_path_meta()
            .ok_or_else(|| denied("host_session_create_workspace_missing"))?;
        let scope = self.host_scope_registry.retain(token, caller).await?;
        let binding = scope.token().binding();
        let permit = binding
            .create(operation_id, &project, &workspace)
            .map_err(|error| denied(error.to_string()))?;
        if !permit.created_target_rights.is_empty() {
            // Publishing a new target grant requires its own explicit Host CAS.
            // Never claim to install recorded rights as a creation side effect.
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "host_session_created_target_rights_require_separate_host_installation",
            ));
        }
        if expected_revision.is_some_and(|expected| expected != binding.revision) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "host_session_create_scope_revision_changed",
            ));
        }
        let observations = [binding.caller.clone()];
        let guards = Arc::new(
            self.lock_host_scope_observations(
                &observations,
                Some(scope),
                std::slice::from_ref(&reserved_id),
            )
            .await?,
        );
        let current = self
            .peer_delivery_session(&caller.session_id, &caller.root_session_id)
            .await?;
        if !caller.matches_session(&current) {
            return Err(denied("host_session_operation_caller_changed"));
        }
        match self.load_root_authority_unchecked(&candidate.id).await? {
            Some(target)
                if super::supervisor_management::reserved_root_matches(&target, candidate) =>
            {
                Ok(())
            }
            Some(_) => Err(denied(
                "host_session_create_target_lifetime_or_context_changed",
            )),
            None if expected_revision.is_none() => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "host_session_create_pending_root_absent_requires_recovery",
            )),
            None => {
                // Reuse the canonical ordinary Root writer. It also fences
                // partial main/runtime/Actor context and retains these complete
                // physical guards through any owned filesystem job.
                let writer = DefaultWriterGuards::host_creation(guards);
                self.save_session_after_lock(candidate, Instant::now(), &writer, None)
                    .await
            }
        }
    }
}

#[cfg(test)]
#[path = "host_root_create_tests.rs"]
mod tests;
