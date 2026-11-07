//! Host wiring for the explicit Supervisor Work Start route.
use super::*;
use bamboo_domain::{ProjectStatus, ProviderModelRef, SessionCallerReference};
use bamboo_engine::model_config_helper::{
    get_fast_model_for_provider, get_reasoning_effort_for_provider, resolve_provider_routing_key,
};
use bamboo_orchestration::managed_work;
use bamboo_server_tools::session_control::root_creation::SupervisedRootCreateRequest;
use bamboo_server_tools::session_control::root_run::SupervisedRootRunPort;
use std::collections::BTreeMap;

use crate::app_state::supervised_root_create::SupervisedRootCreator;
use crate::app_state::supervised_root_run::SupervisedRootRunController;

#[derive(Clone)]
pub(super) struct ManagedWorkHost {
    creator: Arc<SupervisedRootCreator>,
    providers: Arc<bamboo_llm::ProviderRegistry>,
    runs: OnceLock<Arc<SupervisedRootRunController>>,
}

impl TicketApplication {
    pub(in crate::app_state) fn bind_managed_work_host(
        &self,
        creator: Arc<SupervisedRootCreator>,
        providers: Arc<bamboo_llm::ProviderRegistry>,
    ) {
        let _ = self.managed_work_host.set(ManagedWorkHost {
            creator,
            providers,
            runs: OnceLock::new(),
        });
    }

    pub(in crate::app_state) fn bind_managed_work_runs(
        &self,
        runs: Arc<SupervisedRootRunController>,
    ) {
        if let Some(host) = self.managed_work_host.get() {
            let _ = host.runs.set(runs);
        }
    }

    pub(in crate::app_state) fn managed_work_service(
        &self,
    ) -> std::sync::Weak<OnceLock<Arc<TicketService>>> {
        Arc::downgrade(&self.service)
    }

    pub(crate) async fn dispatch_from_supervisor(
        &self,
        principal: Principal,
        command: &Command,
        caller: Option<&SessionCallerReference>,
    ) -> Result<Value> {
        let starts: Vec<_> = command
            .operations
            .iter()
            .filter_map(|operation| match operation {
                Operation::Start { workspace, .. } => Some(workspace.as_ref()),
                _ => None,
            })
            .collect();
        // The existing workspace-free Work route also handles legacy rework.
        // An explicit worktree selects the independent Session admission below.
        if starts.iter().all(|workspace| workspace.is_none()) {
            return self.dispatch(principal, command).await;
        }
        if command.operations.len() > 64 {
            return Err(Error::ContextBudgetExceeded);
        }
        let caller = caller
            .ok_or_else(|| Error::ScopeDenied("original Work dispatch caller missing".into()))?;
        let (service, authority) = self.authority(principal).await?;
        let (_, snapshot) = service.published()?;
        let host = self.managed_work_host.get().ok_or_else(host_unavailable)?;
        let supervisor = host
            .creator
            .check_original_caller(caller)
            .await
            .map_err(|error| Error::ScopeDenied(error.to_string()))?;
        if snapshot.receipts.contains_key(&command.operation_id) {
            let receipt = service.work_dispatch(&authority, command)?;
            return self.enqueue_receipt(&service, command, receipt).await;
        }
        self.require_managed_work_enabled().await?;
        let reference = bamboo_domain::SupervisorReference {
            session_id: supervisor.id.clone(),
            incarnation_id: supervisor
                .supervisor_incarnation_id()
                .ok_or_else(|| Error::ScopeDenied("canonical Work Supervisor required".into()))?,
        };
        let scope = host
            .creator
            .store
            .inspect_supervisor_current_user_scope(&reference)
            .await?;
        let model = host.fast_model(&*self.config.read().await)?;
        let mut projects = BTreeMap::new();
        for workspace in starts {
            let workspace = workspace.ok_or_else(|| {
                Error::ScopeDenied("Managed Work requires an explicit isolated worktree".into())
            })?;
            bamboo_engine::ticket_worker_plan::files::verify_workspace(workspace.clone()).await?;
            let project = host
                .creator
                .projects
                .find_workspace_owner_for_path(&workspace.worktree)
                .map_err(|error| Error::ScopeDenied(error.to_string()))?
                .filter(|project| project.status == ProjectStatus::Active)
                .ok_or_else(|| Error::ScopeDenied("Worktree has no active Project".into()))?;
            let path = crate::project_context::validate_explicit_session_workspace_with_resolver(
                &host.creator.projects,
                Some(&project.id),
                &workspace.worktree,
                &host.creator.workspace,
            )
            .map_err(|error| Error::ScopeDenied(error.to_string()))?;
            if path != Path::new(&workspace.worktree) {
                return Err(Error::ScopeDenied("Worktree path must be canonical".into()));
            }
            projects.insert(workspace.worktree.clone(), project.id);
        }
        self.validate_project_operations(&service, command)?;
        host.creator
            .check_original_caller(caller)
            .await
            .map_err(|error| Error::ScopeDenied(error.to_string()))?;
        let receipt = service.dispatch_managed_work(
            &authority,
            command,
            &|spec| {
                managed_work::validate_start(spec)?;
                let project_id = spec
                    .workspace
                    .as_ref()
                    .and_then(|workspace| projects.get(&workspace.worktree))
                    .cloned()
                    .ok_or_else(|| Error::ScopeDenied("Worktree Project changed".into()))?;
                Ok(ManagedWorkRoot {
                    operation_id: uuid::Uuid::new_v4(),
                    caller: caller.clone(),
                    scope_revision: scope.legacy_state_revision,
                    project_id,
                    model: model.clone(),
                })
            },
            &|packet| {
                managed_work::format_instruction(
                    packet,
                    bamboo_engine::session_messaging::MAX_SUPERVISOR_FOLLOWUP_BYTES,
                )
                .map(|_| ())
            },
        )?;
        self.enqueue_receipt(&service, command, receipt).await
    }

    async fn require_managed_work_enabled(&self) -> Result<()> {
        let config = self.config.read().await;
        if !config.features.ticket_mutation || !config.features.ticket_dispatch {
            return Err(Error::AuthorityUnavailable(
                "Work dispatch is disabled".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn ensure_managed_work(
        &self,
        service: &TicketService,
        key: &str,
        spec: &DispatchSpec,
    ) -> Result<ticket_runtime::DispatchObservation> {
        use ticket_runtime::DispatchObservation;
        let observation =
            ticket_runtime::query_dispatch(self.storage.as_ref(), service, key, spec).await?;
        if !matches!(
            observation,
            DispatchObservation::Missing | DispatchObservation::Pending { .. }
        ) {
            return Ok(observation);
        }
        self.require_managed_work_enabled().await?;
        let host = self.managed_work_host.get().ok_or_else(host_unavailable)?;
        let dispatch = ticket_runtime::dispatch_binding(service, key, spec)?;
        ticket_runtime::require_dispatch_permission(service, &dispatch)?;
        let snapshot = service.published()?.1;
        self.validate_dispatch_project_context(&snapshot, spec)?;
        let target = spec.managed_root.as_ref().expect("managed dispatch");
        if snapshot
            .project
            .links
            .get(&spec.work_id)
            .is_some_and(|project_id| project_id != &target.project_id)
        {
            return Err(Error::ScopeDenied(
                "managed Root Project differs from the Work Project".into(),
            ));
        }
        let workspace = spec
            .workspace
            .as_ref()
            .ok_or_else(|| Error::ScopeDenied("Worktree missing".into()))?;
        bamboo_engine::ticket_worker_plan::files::verify_workspace(workspace.clone()).await?;
        let request = SupervisedRootCreateRequest {
            operation_id: target.operation_id,
            expected_state_revision: target.scope_revision,
            project_id: target.project_id.clone(),
            workspace_path: workspace.worktree.clone(),
            message: managed_work::first_instruction(
                service,
                spec,
                bamboo_engine::session_messaging::MAX_SUPERVISOR_FOLLOWUP_BYTES,
            )?,
            title: Some(spec.contract.title.trim().chars().take(256).collect()),
        };
        let result = host
            .creator
            .create_managed_work(service, spec, &dispatch, request)
            .await
            .map_err(|error| Error::AuthorityUnavailable(error.to_string()))?;
        let response: Value = serde_json::from_str(&result.result)?;
        if response["admitted"] != true {
            return Ok(DispatchObservation::OutcomeUnknown {
                receipt: None,
                reason: response["code"]
                    .as_str()
                    .unwrap_or("work_creation_unconfirmed")
                    .into(),
            });
        }
        let observed =
            ticket_runtime::query_dispatch(self.storage.as_ref(), service, key, spec).await?;
        let DispatchObservation::Pending { session_id, .. } = observed else {
            return Ok(observed);
        };
        let receipt_id = target
            .input_id(&session_id)
            .filter(|id| response["receipt_id"].as_str() == Some(id.as_str()))
            .ok_or_else(|| Error::ScopeDenied("managed Root input identity changed".into()))?;
        let generation = response["generation"]
            .as_u64()
            .filter(|generation| *generation != 0)
            .ok_or_else(|| Error::ScopeDenied("managed Root input generation missing".into()))?;
        let operation_id = bamboo_domain::SessionMessageId::parse(target.operation_id.to_string())
            .map_err(|error| Error::ScopeDenied(error.to_string()))?;
        Ok(DispatchObservation::Pending {
            session_id,
            first_input: Some(bamboo_domain::ObserverSupervisedRootInput {
                receipt_id,
                generation,
                operation_id,
            }),
        })
    }
}

impl ManagedWorkHost {
    pub(super) async fn cancel(&self, service: &TicketService, key: &str) -> Result<()> {
        let (_, snapshot) = service.published()?;
        let intent = &snapshot.intents[key];
        let assignment = &snapshot.assignments[&intent.assignment_id];
        if assignment.process_stopped
            || !matches!(
                assignment.state,
                AssignmentState::Cancelling | AssignmentState::OutcomeUnknown
            )
        {
            return Ok(());
        }
        let (binding, supervisor) = ticket_runtime::verified_scope_binding_for_service(
            self.creator.store.as_ref(),
            service,
            &snapshot.binding.supervisor_session_id,
        )
        .await?;
        let target = intent
            .immutable_spec
            .managed_root
            .as_ref()
            .expect("managed cancellation");
        let Some(receipt) = &assignment.runtime else {
            // The same creation claim guards durable reservation and first
            // input. A retained reservation or Root requires its own stop proof.
            let (digest, id) =
                bamboo_domain::host_session_create_identity(&target.caller, target.operation_id)
                    .map_err(|error| Error::ScopeDenied(error.into()))?;
            let _creation = self.creator.operations.lock(&digest).await?;
            if self
                .creator
                .operations
                .load_for_status(&digest)
                .await?
                .is_some()
                || self.creator.store.load_root_authority(&id).await?.is_some()
            {
                return Err(Error::AuthorityUnavailable(
                    "managed Work creation has no confirmed stop".into(),
                ));
            }
            let authority = Authority::from_verified_host(binding.clone(), Principal::Runtime);
            service.execute(
                &authority,
                &Command {
                    operation_id: format!("runtime-queued-stop/{key}"),
                    binding,
                    expected_seq: snapshot.seq,
                    expected_epoch: snapshot.authority_epoch,
                    operations: vec![Operation::ConfirmStopped {
                        assignment_id: assignment.id.clone(),
                        effects_reconciled: true,
                    }],
                    source: None,
                },
            )?;
            return Ok(());
        };
        let admission = intent.managed_admission.as_ref().ok_or_else(|| {
            Error::ScopeDenied("managed Work cancellation has no exact admission".into())
        })?;
        let expected = ticket_runtime::dispatch_binding(service, key, &intent.immutable_spec)?;
        let root = self
            .creator
            .store
            .load_runtime_control_plane(&receipt.session_id)
            .await?
            .filter(|root| {
                root.created_at == admission.target_created_at
                    && managed_work::matches_root(target, &intent.immutable_spec, root)
                    && ticket_runtime::read_dispatch(root).ok().flatten().as_ref()
                        == Some(&expected)
            })
            .ok_or_else(|| {
                Error::ScopeDenied("managed Work cancellation lifetime changed".into())
            })?;
        let root = bamboo_domain::SupervisorCurrentUserTarget::capture(&root)
            .map_err(|error| Error::ScopeDenied(error.into()))?;
        // The committed Work cancellation is the Host's authority. The
        // canonical Root control still fences the exact target birth and run.
        let result = self
            .runs
            .get()
            .ok_or_else(host_unavailable)?
            .cancel_run(
                &SessionCallerReference::from(&supervisor),
                &root,
                &receipt.run_id,
                bamboo_domain::AdmissionGate::default(),
            )
            .await
            .map_err(|error| Error::AuthorityUnavailable(error.to_string()))?;
        if !result.success {
            return Err(Error::AuthorityUnavailable(
                "managed Work exact cancellation unconfirmed".into(),
            ));
        }
        Ok(())
    }

    fn fast_model(&self, config: &Config) -> Result<ProviderModelRef> {
        let configured = config
            .defaults
            .as_ref()
            .map(|defaults| defaults.fast.as_ref().unwrap_or(&defaults.chat));
        let provider = resolve_provider_routing_key(
            config,
            configured.map_or(config.provider.as_str(), |model| &model.provider),
            &self.providers,
        )
        .map_err(|error| Error::AuthorityUnavailable(error.to_string()))?;
        let model = configured
            .map(|model| model.model.clone())
            .or_else(|| get_fast_model_for_provider(config, &provider))
            .filter(|model| !model.trim().is_empty())
            .ok_or_else(|| {
                Error::AuthorityUnavailable("configured Fast route unavailable".into())
            })?;
        Ok(ProviderModelRef {
            model,
            reasoning_effort: configured
                .and_then(|model| model.reasoning_effort)
                .or_else(|| get_reasoning_effort_for_provider(config, &provider)),
            provider,
        })
    }
}

fn host_unavailable() -> Error {
    Error::AuthorityUnavailable("managed Work Root Host unavailable".into())
}
