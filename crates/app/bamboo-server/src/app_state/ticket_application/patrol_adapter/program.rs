//! Assembly for the existing opt-in Ready worker; no new loop or policy grant.

use super::super::{Error, Principal, Result, TicketApplication};
use super::{
    checkpoint_record::{self, NoticeSource},
    patrol_error, runtime_observation, PatrolCheckpointStore,
};
use bamboo_orchestration::{
    patrol::{
        program::{ProgramPatrol, ProgramSelection},
        Checkpoint, CheckpointPort, Clock, Policy, PortError, Scope, StoredCheckpoint,
    },
    ReadyAdmissionFence,
};

#[cfg(all(test, unix))]
mod evidence_assembly_tests;

struct SystemClock(u64);
impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

struct FencedJournal {
    store: PatrolCheckpointStore,
    fence: ReadyAdmissionFence,
    service: std::sync::Arc<bamboo_tickets::TicketService>,
    facts_revision: u64,
    scopes: Vec<Scope>,
}

impl FencedJournal {
    fn current(&self) -> std::result::Result<(), PortError> {
        self.fence.check().map_err(|_| PortError::Denied)?;
        if self.service.health() != bamboo_tickets::Health::Writable {
            return Err(PortError::Unavailable);
        }
        if self
            .service
            .published()
            .map_err(|_| PortError::Unavailable)?
            .1
            .execution_facts_revision()
            != self.facts_revision
        {
            return Err(PortError::Conflict);
        }
        Ok(())
    }
}

impl CheckpointPort for FencedJournal {
    fn load(&mut self, scope: &Scope) -> std::result::Result<Option<StoredCheckpoint>, PortError> {
        self.current()?;
        if !self.scopes.contains(scope) {
            self.scopes.push(scope.clone());
        }
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
        let previous = self.store.load(&checkpoint.scope)?;
        let source = if checkpoint_record::new_escalation(
            previous.as_ref().map(|r| &r.checkpoint),
            checkpoint,
        ) {
            Some(NoticeSource::capture(&self.service, checkpoint)?)
        } else {
            None
        };
        let saved = self.store.save_with_source(version, checkpoint, source)?;
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
        let (_, snapshot) = service.published()?;
        let facts_revision = snapshot.execution_facts_revision();
        // Canonical runtime I/O stays outside the synchronous journal/Ticket
        // pass. Each prepared sample remains bound to its original commit and
        // receipt; the factory rechecks both against fresh ledger authority.
        let observations =
            runtime_observation::prepare_selection(self, &service, &authority, selection).await?;
        fence.check()?;
        let expected_binding = snapshot.binding.clone();
        let binding =
            bamboo_tickets::content_hash(&bamboo_tickets::canonical_bytes(&snapshot.binding)?);
        let root = self.root.clone();
        let project_id = selection.project_id.clone();
        let work_ids = selection.work_ids.to_vec();
        let fence = fence.clone();
        let mut journal = tokio::task::spawn_blocking(move || -> Result<FencedJournal> {
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
                service: service.clone(),
                facts_revision,
                scopes: Vec::new(),
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
                .tick_project_with_observations(
                    &service,
                    &authority,
                    &ProgramSelection {
                        project_id: &project_id,
                        work_ids: &work_ids,
                    },
                    &SystemClock(now),
                    &mut journal,
                    &mut |scope| match observations.get(scope) {
                        Some(runtime) => {
                            bamboo_orchestration::patrol::host::read_observation_with_runtime(
                                &service,
                                &authority,
                                scope,
                                runtime,
                                &Policy::default(),
                            )
                        }
                        None => bamboo_orchestration::patrol::host::read_observation(
                            &service, &authority, scope,
                        ),
                    },
                )
                .map_err(|error| match error {
                    bamboo_orchestration::patrol::Error::Port(error) => patrol_error(error),
                    other => Error::AuthorityUnavailable(format!(
                        "program patrol observation rejected: {other:?}"
                    )),
                })?;
            Ok(journal)
        })
        .await
        .map_err(|_| Error::AuthorityUnavailable("program patrol worker stopped".into()))??;
        // Keep the same exclusive journal lease across the staged Inbox call.
        // The existing worker remains the only scheduler, with no model call.
        let scopes = std::mem::take(&mut journal.scopes);
        for scope in scopes {
            let prepared = tokio::task::spawn_blocking(move || -> Result<_> {
                journal.current().map_err(patrol_error)?;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()
                    .and_then(|d| u64::try_from(d.as_millis()).ok())
                    .ok_or_else(|| {
                        Error::AuthorityUnavailable("Patrol clock unavailable".into())
                    })?;
                let pending = bamboo_orchestration::patrol::delivery::prepare(
                    &scope,
                    &Policy::default(),
                    &SystemClock(now),
                    &mut journal,
                )
                .map_err(|error| {
                    Error::AuthorityUnavailable(format!("Patrol delivery rejected: {error:?}"))
                })?;
                journal.current().map_err(patrol_error)?;
                let source = match &pending {
                    Some(pending) => journal
                        .store
                        .notice_source(&pending.escalation.operation.incident.scope)
                        .map_err(patrol_error)?,
                    None => None,
                };
                Ok((journal, pending, source))
            })
            .await
            .map_err(|_| Error::AuthorityUnavailable("Patrol delivery worker stopped".into()))??;
            journal = prepared.0;
            if let Some(pending) = prepared.1 {
                journal.fence.check()?;
                if self
                    .stage_ready_patrol_notice(
                        &expected_binding,
                        &pending,
                        prepared.2.as_ref(),
                        &journal.fence,
                    )
                    .await?
                {
                    journal = tokio::task::spawn_blocking(move || -> Result<_> {
                        journal.current().map_err(patrol_error)?;
                        #[cfg(all(test, unix))]
                        evidence_assembly_tests::before_ack(&mut journal.store);
                        pending.acknowledge(&mut journal).map_err(|error| {
                            Error::AuthorityUnavailable(format!(
                                "Patrol delivery ACK rejected: {error:?}"
                            ))
                        })?;
                        journal.current().map_err(patrol_error)?;
                        Ok(journal)
                    })
                    .await
                    .map_err(|_| {
                        Error::AuthorityUnavailable("Patrol ACK worker stopped".into())
                    })??;
                }
            }
        }
        Ok(())
    }
}
