//! Host-only adapter for the existing direct-parent outcome CAS.
//! Reuse an actual execution's immutable capability, or own one short control
//! execution through the same ActorDirectory protocol while the Root is idle.

use super::*;
use bamboo_domain::{
    ActorActivationFinish, ActorActivationStatus, RootActorExecutionBinding,
    RuntimeSessionPersistence,
};

pub struct ParentOutcomeRoute {
    directory: Option<std::sync::Weak<dyn bamboo_domain::ActorDirectoryPort>>,
    writers: std::sync::Weak<
        dashmap::DashMap<String, std::sync::Weak<bamboo_domain::RootActorRuntimeWrite>>,
    >,
}

impl ParentOutcomeRoute {
    /// Keep the original Host route in deadline jobs without retaining its
    /// lifetime. Rebuilding a repository must not erase the named writer gate.
    pub fn rebind(&self, mut repository: SessionRepository) -> Option<SessionRepository> {
        if let Some(directory) = &self.directory {
            repository.root_actor_directory = Some(directory.upgrade()?);
            repository.root_actor_writers = self.writers.upgrade()?;
        }
        Some(repository)
    }
}

pub struct ParentOutcomeWriter {
    repository: SessionRepository,
    execution: Option<RootActorExecutionBinding>,
}

impl ParentOutcomeWriter {
    pub fn repository(&self) -> &SessionRepository {
        &self.repository
    }

    /// Only the control execution created here is finished. A borrowed
    /// provider execution retains its own renewal and finalization lifetime.
    pub async fn finish(mut self) -> std::io::Result<()> {
        let Some(mut execution) = self.execution.take() else {
            return Ok(());
        };
        execution
            .directory
            .finish_activation(
                &execution.owner.fence,
                chrono::Utc::now(),
                ActorActivationFinish::Succeeded,
            )
            .await
            .map_err(root_actor_binding_error)?;
        execution.disarm_abandonment();
        Ok(())
    }
}

impl SessionRepository {
    pub fn downgrade_parent_outcome_route(&self) -> ParentOutcomeRoute {
        ParentOutcomeRoute {
            directory: self.root_actor_directory.as_ref().map(Arc::downgrade),
            writers: Arc::downgrade(&self.root_actor_writers),
        }
    }
    /// A named adapter for trusted Host parent-request projections. Default
    /// saves retain their rejection boundary; request identity, lineage,
    /// deadline and single-winner validation remain in the original CAS.
    pub async fn bind_parent_outcome_writer(
        &self,
        session_id: &str,
    ) -> std::io::Result<ParentOutcomeWriter> {
        let session = self
            .storage
            .load_session(session_id)
            .await?
            .ok_or_else(|| root_actor_binding_error("Parent outcome target is missing"))?;
        if let Some((_, owner)) = &self.root_actor_owner {
            if owner.fence.actor_id != session_id || owner.created_at != session.created_at {
                return Err(root_actor_binding_error(
                    "Parent outcome execution target changed",
                ));
            }
            return Ok(ParentOutcomeWriter {
                repository: self.clone(),
                execution: None,
            });
        }
        if !self.root_actor_execution_required(&session) {
            return Ok(ParentOutcomeWriter {
                repository: self.clone(),
                execution: None,
            });
        }
        if let Some(origin) = bamboo_agent_core::tools::context::root_actor_tool_writer() {
            let owner = origin.ok_or_else(|| {
                root_actor_binding_error("Parent reply has no Root execution capability")
            })?;
            if owner.fence.actor_id != session_id || owner.created_at != session.created_at {
                return Err(root_actor_binding_error(
                    "Parent reply execution target changed",
                ));
            }
            return Ok(ParentOutcomeWriter {
                repository: self.bind_root_response_writer(owner)?,
                execution: None,
            });
        }
        let directory = self.root_actor_directory.as_ref().ok_or_else(|| {
            root_actor_binding_error("Parent outcome requires its Host Actor directory")
        })?;
        let active_owner = self
            .root_actor_writers
            .get(session_id)
            .and_then(|entry| entry.value().upgrade());
        if let Some(owner) = active_owner {
            let current = directory
                .inspect_actor(session_id)
                .await
                .map_err(root_actor_binding_error)?;
            if current.activation.is_some_and(|activation| {
                activation.fence() == owner.fence
                    && activation.status == ActorActivationStatus::Running
                    && activation.lease_expires_at > chrono::Utc::now()
            }) {
                // Observation only rejects a captured capability. Final Main
                // and cache publication recheck it under the physical guards.
                return Ok(ParentOutcomeWriter {
                    repository: self.bind_root_response_writer((*owner).clone())?,
                    execution: None,
                });
            }
        }
        // A different live Host owner is rejected by claim_activation. Never
        // borrow its fence or force replacement. Cancellation drops this exact
        // binding; filesystem jobs retain the existing physical guards.
        let execution = self
            .bind_root_actor_execution(
                &session,
                &format!("parent-outcome-{}", uuid::Uuid::new_v4()),
            )
            .await?
            .ok_or_else(|| root_actor_binding_error("Parent outcome Root binding is absent"))?;
        let repository = self.bind_root_response_writer(execution.owner.clone())?;
        Ok(ParentOutcomeWriter {
            repository,
            execution: Some(execution),
        })
    }
}
