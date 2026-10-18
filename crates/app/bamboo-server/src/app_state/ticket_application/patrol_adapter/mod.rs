//! Host-only patrol adapters. No registration, worker action or provider call.

mod checkpoint_store;
pub use checkpoint_store::PatrolCheckpointStore;

use bamboo_orchestration::patrol::{host, Scope};

use super::{Error, Principal, Result, TicketApplication};

impl TicketApplication {
    /// Prepare exactly one program observation after the existing live Host
    /// authority check. IDs/previous checkpoints cannot recreate a grant.
    pub async fn patrol_observation(&self, scope: &Scope) -> Result<host::TicketObservation> {
        let (service, authority) = self.authority(Principal::Runtime).await?;
        host::read_observation(&service, &authority, scope).map_err(patrol_error)
    }

    /// Discover a stable recovery identity from the original DispatchSpec.
    pub async fn patrol_scope(
        &self,
        project_id: &str,
        work_id: &str,
        assignment_id: &str,
    ) -> Result<Scope> {
        let (service, authority) = self.authority(Principal::Runtime).await?;
        host::scope_for_assignment(&service, &authority, project_id, work_id, assignment_id)
            .map_err(patrol_error)
    }

    /// An independent Host policy journal, never a Ticket snapshot, runtime
    /// receipt or balance. Only one live owner may open a scope's journal.
    pub async fn open_patrol_checkpoints(&self, scope: &Scope) -> Result<PatrolCheckpointStore> {
        let (service, authority) = self.authority(Principal::Runtime).await?;
        let (commit, snapshot) = service.published()?;
        let _authorized =
            host::read_observation(&service, &authority, scope).map_err(patrol_error)?;
        if service.published()?.0 != commit {
            return Err(Error::RevisionConflict);
        }
        let binding =
            bamboo_tickets::content_hash(&bamboo_tickets::canonical_bytes(&snapshot.binding)?);
        // Resolve the already configured Host root, not a client-selected path.
        // Keep fsync/locking away from the async runtime and Ticket mutex.
        let root = self
            .root
            .canonicalize()?
            .join("orchestration")
            .join("patrol")
            .join(binding);
        tokio::task::spawn_blocking(move || PatrolCheckpointStore::open(&root))
            .await
            .map_err(|_| Error::AuthorityUnavailable("patrol checkpoint opener stopped".into()))?
            .map_err(patrol_error)
    }
}

fn patrol_error(error: bamboo_orchestration::patrol::PortError) -> Error {
    match error {
        bamboo_orchestration::patrol::PortError::Denied => {
            Error::ScopeDenied("patrol scope denied".into())
        }
        bamboo_orchestration::patrol::PortError::Conflict => Error::RevisionConflict,
        bamboo_orchestration::patrol::PortError::OutcomeUnknown => Error::AuthorityUnavailable(
            "patrol checkpoint outcome requires reopen/reconciliation".into(),
        ),
        bamboo_orchestration::patrol::PortError::Unavailable => {
            Error::AuthorityUnavailable("patrol source/checkpoint unavailable".into())
        }
    }
}
