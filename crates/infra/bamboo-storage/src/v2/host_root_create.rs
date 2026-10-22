//! A creation permit is distinct from target SendPeer or management authority.

use super::*;
use bamboo_domain::{host_session_create_identity, HostSessionScopeToken, SessionCallerReference};

fn denied(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message.into())
}

impl SessionStoreV2 {
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
