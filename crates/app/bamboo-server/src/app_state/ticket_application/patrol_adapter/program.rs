//! Assembly for the existing opt-in Ready worker; no new loop or policy grant.

use super::super::{Error, Principal, Result, TicketApplication};
use super::{patrol_error, PatrolCheckpointStore};
use bamboo_orchestration::{
    patrol::{
        program::{ProgramPatrol, ProgramSelection},
        Checkpoint, CheckpointPort, Clock, Policy, PortError, Scope, StoredCheckpoint,
    },
    ReadyAdmissionFence,
};

struct SystemClock(u64);
impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

struct FencedJournal<'a> {
    store: PatrolCheckpointStore,
    fence: ReadyAdmissionFence,
    service: &'a bamboo_tickets::TicketService,
    commit: String,
}

impl FencedJournal<'_> {
    fn current(&self) -> std::result::Result<(), PortError> {
        self.fence.check().map_err(|_| PortError::Denied)?;
        if self.service.health() != bamboo_tickets::Health::Writable {
            return Err(PortError::Unavailable);
        }
        if self
            .service
            .published()
            .map_err(|_| PortError::Unavailable)?
            .0
            != self.commit
        {
            return Err(PortError::Conflict);
        }
        Ok(())
    }
}

impl CheckpointPort for FencedJournal<'_> {
    fn load(&mut self, scope: &Scope) -> std::result::Result<Option<StoredCheckpoint>, PortError> {
        self.current()?;
        let loaded = self.store.load(scope)?;
        self.current()?;
        Ok(loaded)
    }

    fn save(
        &mut self,
        version: Option<u64>,
        checkpoint: &Checkpoint,
    ) -> std::result::Result<u64, PortError> {
        self.current()?;
        let saved = self.store.save(version, checkpoint)?;
        self.current()?;
        Ok(saved)
    }
}

impl TicketApplication {
    pub(in crate::app_state::ticket_application) async fn reconcile_ready_program_patrol(
        &self,
        selection: &ProgramSelection<'_>,
        fence: &ReadyAdmissionFence,
    ) -> Result<()> {
        fence.check()?;
        let (service, authority) = self.authority(Principal::Runtime).await?;
        let (commit, snapshot) = service.published()?;
        let binding =
            bamboo_tickets::content_hash(&bamboo_tickets::canonical_bytes(&snapshot.binding)?);
        let root = self.root.clone();
        let project_id = selection.project_id.clone();
        let work_ids = selection.work_ids.to_vec();
        let fence = fence.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            fence.check()?;
            let root = root
                .canonicalize()?
                .join("orchestration")
                .join("patrol")
                .join(binding);
            let store = PatrolCheckpointStore::open(&root).map_err(patrol_error)?;
            let mut journal = FencedJournal {
                store,
                fence,
                service: &service,
                commit,
            };
            journal.current().map_err(patrol_error)?;
            let program = ProgramPatrol::new(Policy::default())
                .map_err(|_| Error::AuthorityUnavailable("invalid program patrol policy".into()))?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                .ok_or_else(|| {
                    Error::AuthorityUnavailable("program patrol clock unavailable".into())
                })?;
            program
                .tick_project(
                    &service,
                    &authority,
                    &ProgramSelection {
                        project_id: &project_id,
                        work_ids: &work_ids,
                    },
                    &SystemClock(now),
                    &mut journal,
                )
                .map_err(|error| match error {
                    bamboo_orchestration::patrol::Error::Port(error) => patrol_error(error),
                    other => Error::AuthorityUnavailable(format!(
                        "program patrol observation rejected: {other:?}"
                    )),
                })?;
            Ok(())
        })
        .await
        .map_err(|_| Error::AuthorityUnavailable("program patrol worker stopped".into()))?
    }
}
