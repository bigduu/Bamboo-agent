//! Assembly for the existing opt-in Ready worker; no new loop or policy grant.

use super::super::{Error, Principal, Result, TicketApplication};
use super::{
    checkpoint_record::{self, NoticeSource},
    evidence::configured_model,
    patrol_error, runtime_observation, semantic, PatrolCheckpointStore,
};
use bamboo_orchestration::{
    patrol::{
        budget::TicketBudget,
        program::{ProgramPatrol, ProgramSelection},
        staged::{self, InputDemand, Outcome, PreparedInput},
        Checkpoint, CheckpointPort, Clock, Patrol, Policy, PortError, Scope, StoredCheckpoint,
    },
    ReadyAdmissionFence,
};

#[cfg(all(test, unix))]
mod evidence_assembly_tests;

pub(super) struct SystemClock(u64);
impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

impl SystemClock {
    pub(super) fn current() -> Result<Self> {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| u64::try_from(d.as_millis()).ok())
            .map(Self)
            .ok_or_else(|| Error::AuthorityUnavailable("Patrol clock unavailable".into()))
    }
}

fn pass_error(error: bamboo_orchestration::patrol::Error) -> Error {
    match error {
        bamboo_orchestration::patrol::Error::Port(error) => patrol_error(error),
        other => Error::AuthorityUnavailable(format!("patrol observation rejected: {other:?}")),
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
        self.reconcile_ready_patrol_with_policy(selection, fence, Policy::default())
            .await
    }

    async fn reconcile_ready_patrol_with_policy(
        &self,
        selection: &ProgramSelection<'_>,
        fence: &ReadyAdmissionFence,
        policy: Policy,
    ) -> Result<()> {
        fence.check()?;
        let (service, authority) = self.authority(Principal::Runtime).await?;
        self.reconcile_semantic_accounting(&service, &authority)
            .await?;
        fence.check()?;
        let (_, snapshot) = service.published()?;
        let facts_revision = snapshot.execution_facts_revision();
        // Canonical runtime I/O stays outside the synchronous journal/Ticket
        // pass. Each prepared sample remains bound to its original commit and
        // receipt. At projection the factory rechecks the live Task instance.
        let observations =
            runtime_observation::prepare_selection(self, &service, &authority, selection).await?;
        fence.check()?;
        let expected_binding = snapshot.binding.clone();
        let binding =
            bamboo_tickets::content_hash(&bamboo_tickets::canonical_bytes(&snapshot.binding)?);
        let root = self.root.clone();
        let project_id = selection.project_id.clone();
        let work_ids = selection.work_ids.to_vec();
        let configured = self
            .config
            .read()
            .await
            .patrol_model
            .clone()
            .filter(|selection| {
                (selection.endpoint.is_some()
                    || selection.protocol
                        == bamboo_config::PatrolModelProtocol::RegisteredProviderV1)
                    && self.patrol_sources.get().is_some()
            });
        let model = configured.as_ref().map(configured_model);
        let live_semantics = model.is_some();
        let calls = self.patrol_calls.clone();
        let run_fence = fence.clone();
        let resume_policy = policy.clone();
        let (mut journal, demand) =
            tokio::task::spawn_blocking(move || -> Result<(FencedJournal, Option<InputDemand>)> {
                run_fence.check()?;
                let root = root
                    .canonicalize()?
                    .join("orchestration")
                    .join("patrol")
                    .join(binding);
                let store = PatrolCheckpointStore::open(&root).map_err(patrol_error)?;
                let semantic_fence = run_fence.clone();
                let mut journal = FencedJournal {
                    store,
                    fence: run_fence,
                    service: service.clone(),
                    facts_revision,
                    scopes: Vec::new(),
                };
                journal.current().map_err(patrol_error)?;
                let program = ProgramPatrol::new(policy.clone()).map_err(pass_error)?;
                let reconciliation = Patrol::new(policy.clone(), None).map_err(pass_error)?;
                let patrol = Patrol::new(policy.clone(), model).map_err(pass_error)?;
                let clock = SystemClock::current()?;
                let mut demand = None;
                program
                    .visit_project_with_observations(
                        &service,
                        &authority,
                        &ProgramSelection {
                            project_id: &project_id,
                            work_ids: &work_ids,
                        },
                        &clock,
                        &mut journal,
                        &mut |scope| match observations.get(scope) {
                            Some(runtime) => runtime.read_observation(
                                &service,
                                &authority,
                                scope,
                                &policy,
                                Some(clock.now_ms()),
                            ),
                            None => bamboo_orchestration::patrol::host::read_observation(
                                &service, &authority, scope,
                            ),
                        },
                        &mut |scope, checkpoints, observation, existing| {
                            let may_prepare = live_semantics
                                && semantic_fence
                                    .check_semantic_scope(&snapshot, scope)
                                    .is_ok();
                            if !may_prepare && !staged::has_semantic_debt(existing) {
                                program.tick(scope, &clock, checkpoints, observation)?;
                                return Ok(());
                            }
                            let mut host = semantic::Ports {
                                journal: checkpoints,
                                observation,
                                budget: TicketBudget {
                                    service: &service,
                                    authority: &authority,
                                    clock: &clock,
                                },
                            };
                            let driver = if may_prepare {
                                &patrol
                            } else {
                                &reconciliation
                            };
                            match driver.tick_staged(
                                scope,
                                &clock,
                                &mut host,
                                None,
                                |operation| calls.status(operation),
                            )? {
                                Outcome::InputRequired(next) => {
                                    demand.get_or_insert(next);
                                }
                                Outcome::Complete { reconciled, .. } => {
                                    if let Some(reconciled) = reconciled {
                                        calls.acknowledge(reconciled);
                                    }
                                }
                            }
                            Ok(())
                        },
                    )
                    .map_err(pass_error)?;
                Ok((journal, demand))
            })
            .await
            .map_err(|_| Error::AuthorityUnavailable("program patrol worker stopped".into()))??;
        if let (Some(demand), Some(selection)) = (demand, configured) {
            journal = self
                .resume_ready_semantic(journal, demand, selection, resume_policy)
                .await?;
        }
        // Keep the same exclusive journal lease across the staged Inbox call.
        // The existing worker remains the only scheduler.
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

    async fn resume_ready_semantic(
        &self,
        mut journal: FencedJournal,
        demand: InputDemand,
        selection: bamboo_config::PatrolModelConfig,
        policy: Policy,
    ) -> Result<FencedJournal> {
        let (service, authority) = self.authority(Principal::Runtime).await?;
        if !std::sync::Arc::ptr_eq(&service, &journal.service) {
            return Err(Error::RevisionConflict);
        }
        let prepared = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.prepare_semantic_call(
                &service,
                &authority,
                demand.clone(),
                selection.clone(),
                journal.fence.clone(),
            ),
        )
        .await
        .unwrap_or_else(|_| {
            Err(Error::AuthorityUnavailable(
                "patrol evidence preparation expired".into(),
            ))
        });
        let (input, io) = match prepared {
            Ok(prepared) => (prepared.input, Some(prepared.io)),
            Err(Error::RevisionConflict) => return Ok(journal),
            Err(_) => (
                PreparedInput {
                    demand: demand.clone(),
                    result: Err(PortError::Unavailable),
                },
                None,
            ),
        };
        let scope = demand.through.scope;
        let runtime = runtime_observation::prepare(self, &service, &authority, &scope).await?;
        let calls = self.patrol_calls.clone();
        let config = self.config.clone();
        let (journal, submission, io) = tokio::task::spawn_blocking(move || -> Result<_> {
            journal.current().map_err(patrol_error)?;
            let (_, current_snapshot) = service.published()?;
            journal
                .fence
                .check_semantic_scope(&current_snapshot, &scope)?;
            let clock = SystemClock::current()?;
            let mut observation = match runtime {
                Some(runtime) => runtime.read_observation(
                    &service,
                    &authority,
                    &scope,
                    &policy,
                    Some(clock.now_ms()),
                ),
                None => bamboo_orchestration::patrol::host::read_observation(
                    &service, &authority, &scope,
                ),
            }
            .map_err(patrol_error)?;
            let current = config.try_read().map_err(|_| Error::RevisionConflict)?;
            if current.patrol_model.as_ref() != Some(&selection)
                || !current.features.ticket_mutation
                || !current.features.ticket_dispatch
                || io
                    .as_ref()
                    .is_some_and(|io| io.check_prepared_route(&current).is_err())
            {
                return Ok((journal, None, io));
            }
            let mut host = semantic::Ports {
                journal: &mut journal,
                observation: &mut observation,
                budget: TicketBudget {
                    service: &service,
                    authority: &authority,
                    clock: &clock,
                },
            };
            let patrol =
                Patrol::new(policy, Some(configured_model(&selection))).map_err(pass_error)?;
            let outcome = patrol
                .tick_staged(&scope, &clock, &mut host, Some(input), |operation| {
                    calls.status(operation)
                })
                .map_err(pass_error)?;
            let submission = match outcome {
                Outcome::Complete {
                    submission,
                    reconciled,
                    ..
                } => {
                    if let Some(reconciled) = reconciled {
                        calls.acknowledge(reconciled);
                    }
                    submission
                }
                Outcome::InputRequired(_) => None,
            };
            Ok((journal, submission, io))
        })
        .await
        .map_err(|_| Error::AuthorityUnavailable("semantic patrol worker stopped".into()))??;
        if let (Some(submission), Some(io)) = (submission, io) {
            journal.fence.check()?;
            self.patrol_calls
                .launch(submission, SystemClock::current()?.now_ms(), io)
                .map_err(patrol_error)?;
        }
        Ok(journal)
    }
}
