//! One opt-in scope over the existing canonical Supervisor and Child Runtime.
//! Authority always comes from Host storage; client JSON never selects a role.
use std::{
    path::Path,
    sync::{Arc, OnceLock},
};

use bamboo_config::Config;
use bamboo_domain::{SessionAuthorityIdentity, Storage, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_engine::ticket_worker_plan::tickets::*;
use bamboo_engine::{session_app::child_session::ChildSessionPort, ticket_runtime};
use serde_json::{json, Value};
use tokio::sync::RwLock;

use crate::tools::{ticket_dispatch_adapter::TicketDispatchPolicy, ChildSessionAdapter};

pub struct TicketApplication {
    storage: Arc<dyn Storage>,
    config: Arc<RwLock<Config>>,
    service: Option<Arc<TicketService>>,
    unavailable: Option<String>,
    adapter: OnceLock<Arc<ChildSessionAdapter>>,
    messenger: OnceLock<Arc<bamboo_engine::SessionMessenger>>,
    workspace: String,
}

mod resolution;
#[cfg(test)]
mod tests;

impl TicketApplication {
    pub async fn open(root: &Path, storage: Arc<dyn Storage>, config: Arc<RwLock<Config>>) -> Self {
        let opened = Self::open_scope(root, storage.as_ref(), &config).await;
        let (service, unavailable) = match opened {
            Ok(service) => (Some(Arc::new(service)), None),
            Err(error) => (None, Some(error.to_string())),
        };
        Self {
            storage,
            config,
            service,
            unavailable,
            adapter: OnceLock::new(),
            messenger: OnceLock::new(),
            workspace: root
                .join("workspaces")
                .join(DEFAULT_SUPERVISOR_SESSION_ID)
                .to_string_lossy()
                .into_owned(),
        }
    }

    async fn open_scope(
        root: &Path,
        storage: &dyn Storage,
        config: &RwLock<Config>,
    ) -> Result<TicketService> {
        let config = config.read().await;
        let existing = storage
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await?;
        let (supervisor, freshly_created) = match existing {
            Some(supervisor) => (supervisor, false),
            None if config.features.ticket_mutation => {
                let model =
                    bamboo_engine::model_config_helper::get_default_model_from_config(&config)
                        .map_err(|e| Error::AuthorityUnavailable(e.to_string()))?;
                // Only this atomic Storage port can confer Supervisor identity.
                let receipt = storage.get_or_create_default_supervisor(&model).await?;
                if !receipt.created {
                    return Err(Error::AuthorityUnavailable(
                        "Supervisor was concurrently created; explicit scope recovery required"
                            .into(),
                    ));
                }
                let mut supervisor = storage
                    .load_root_authority(&receipt.session_id)
                    .await?
                    .ok_or_else(|| Error::AuthorityUnavailable("new Supervisor missing".into()))?;
                supervisor
                    .set_root_orchestration_only(true)
                    .map_err(|e| Error::InvalidTransition(e.to_string()))?;
                storage.save_session(&supervisor).await?;
                (supervisor, true)
            }
            None => {
                return Err(Error::AuthorityUnavailable(
                    "Ticket mutation feature is disabled".into(),
                ))
            }
        };
        let SessionAuthorityIdentity::Supervisor { incarnation_id } = supervisor.authority_identity
        else {
            return Err(Error::ScopeDenied(
                "canonical Supervisor identity required".into(),
            ));
        };
        let scope_root = root.join("tickets").join(incarnation_id.to_string());
        // Existing Supervisors are never silently attached, even with the flag
        // enabled. A new empty bootstrap has no legacy history to take over.
        if !scope_root.join("HEAD").exists() && !freshly_created {
            return Err(Error::AuthorityUnavailable(
                "existing Supervisor needs explicit attach/import; no automatic takeover".into(),
            ));
        }
        drop(config);
        let (binding, _) = ticket_runtime::verified_scope_binding(storage, &supervisor.id).await?;
        let service = TicketService::open(scope_root, binding)?;
        #[cfg(feature = "ticket-runtime-fixtures")]
        Self::install_fixture_fault(&service)?;
        Ok(service)
    }

    /// Compile-time opt-in for isolated process-exit/I/O acceptance only. Normal
    /// builds neither read these fixture variables nor expose the installer.
    #[cfg(feature = "ticket-runtime-fixtures")]
    fn install_fixture_fault(service: &TicketService) -> Result<()> {
        let Ok(prefix) = std::env::var("BAMBOO_TICKET_FIXTURE_OPERATION_PREFIX") else {
            return Ok(());
        };
        let (point, errno) = match std::env::var("BAMBOO_TICKET_FIXTURE_BOUNDARY").as_deref() {
            Ok("before_head") => (FaultPoint::BeforeHeadRename, None),
            Ok("after_head") => (FaultPoint::AfterHeadRename, None),
            Ok("before_head_enospc") => (FaultPoint::BeforeHeadRename, Some(28)),
            Ok("before_head_eacces") => (FaultPoint::BeforeHeadRename, Some(13)),
            _ => {
                return Err(Error::AuthorityUnavailable(
                    "unsupported Ticket fixture boundary".into(),
                ))
            }
        };
        if !matches!(
            (prefix.as_str(), errno),
            (
                "fixture-intent" | "runtime-admit/" | "runtime-submit/",
                None
            ) | ("worker-file/", Some(13 | 28))
        ) {
            return Err(Error::AuthorityUnavailable(
                "unsupported Ticket fixture operation".into(),
            ));
        }
        let label = prefix.clone();
        service.set_operation_publication_fault(
            prefix,
            Arc::new(move |observed| {
                if observed == point {
                    if let Some(errno) = errno {
                        eprintln!("TICKET_FIXTURE_IO {label} {observed:?} errno={errno}");
                        return Err(std::io::Error::from_raw_os_error(errno));
                    }
                    eprintln!("TICKET_FIXTURE_EXIT {label} {observed:?}");
                    std::process::exit(71);
                }
                Ok(())
            }),
        );
        Ok(())
    }

    pub fn service(&self) -> Result<Arc<TicketService>> {
        self.service.clone().ok_or_else(|| {
            Error::AuthorityUnavailable(
                self.unavailable
                    .clone()
                    .unwrap_or_else(|| "Ticket scope unavailable".into()),
            )
        })
    }

    pub fn bind_adapter(&self, adapter: Arc<ChildSessionAdapter>) {
        let _ = self.adapter.set(adapter);
    }
    pub fn bind_messenger(&self, messenger: Arc<bamboo_engine::SessionMessenger>) {
        let _ = self.messenger.set(messenger);
    }

    pub async fn authority(&self, principal: Principal) -> Result<(Arc<TicketService>, Authority)> {
        let service = self.service()?;
        let (binding, _) = ticket_runtime::verified_scope_binding(
            self.storage.as_ref(),
            DEFAULT_SUPERVISOR_SESSION_ID,
        )
        .await?;
        if service.published()?.1.binding != binding {
            return Err(Error::ScopeDenied(
                "Supervisor scope binding changed".into(),
            ));
        }
        Ok((service, Authority::from_verified_host(binding, principal)))
    }

    pub async fn status(&self) -> Value {
        let flags = self.config.read().await.features.clone();
        match self
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await
        {
            Ok((service, authority)) => match service.work_overview(&authority) {
                Ok(overview) => {
                    json!({"available": true, "binding": service.published().map(|(_,s)| s.binding).ok(),
                    "health": match service.health() { Health::Writable => "writable", _ => "read_only" },
                    "mutation_enabled": flags.ticket_mutation, "dispatch_enabled": flags.ticket_dispatch,
                    "capabilities":{"ticket_scope_v1":true,"multi_pending_v1":true,"precise_request_response_v1":true,"message_references_v1":true,"semantic_messages_v1":true},
                    "overview": overview})
                }
                Err(error) => json!({"available":false,"reason":error.to_string()}),
            },
            Err(error) => json!({"available": false, "reason": error.to_string(),
                "mutation_enabled": flags.ticket_mutation, "dispatch_enabled": flags.ticket_dispatch}),
        }
    }

    pub async fn update(
        &self,
        principal: Principal,
        command: &Command,
    ) -> Result<OperationReceipt> {
        if command.operations.iter().any(|op| {
            matches!(
                op,
                Operation::Import { .. } | Operation::AttachLegacy { .. }
            )
        }) {
            return Err(Error::ScopeDenied(
                "legacy attach/import requires the explicit verified offline Host port".into(),
            ));
        }
        if !self.config.read().await.features.ticket_mutation {
            return Err(Error::AuthorityUnavailable(
                "Ticket mutation feature is disabled".into(),
            ));
        }
        let (service, authority) = self.authority(principal.clone()).await?;
        let receipt = service.work_update(&authority, command)?;
        self.cancel_after_commit(&service, command);
        if matches!(principal, Principal::User { .. })
            && command.operations.iter().any(|op| {
                matches!(
                    op,
                    Operation::Answer { .. } | Operation::DecideApproval { .. }
                )
            })
        {
            self.wake_after_receipt(&receipt).await;
        }
        Ok(receipt)
    }

    /// Commit before enqueue, return admission observations without waiting for
    /// the Worker. Errors after commit are reported alongside its receipt.
    pub async fn dispatch(&self, principal: Principal, command: &Command) -> Result<Value> {
        let flags = self.config.read().await.features.clone();
        if !flags.ticket_mutation {
            return Err(Error::AuthorityUnavailable(
                "Ticket mutation feature is disabled".into(),
            ));
        }
        let (service, authority) = self.authority(principal).await?;
        let receipt = service.work_dispatch(&authority, command)?;
        self.enqueue_receipt(&service, command, receipt).await
    }

    async fn enqueue_receipt(
        &self,
        service: &Arc<TicketService>,
        command: &Command,
        receipt: OperationReceipt,
    ) -> Result<Value> {
        self.cancel_after_commit(service, command);
        let snapshot = service.published()?.1;
        let mut runtime = Vec::new();
        let mut errors = Vec::new();
        for assignment in snapshot
            .assignments
            .values()
            .filter(|a| receipt.ids.values().any(|id| id == &a.id))
        {
            let intent = &snapshot.intents[&assignment.dispatch_key];
            let observation = self
                .ensure(&assignment.dispatch_key, &intent.immutable_spec)
                .await;
            match observation {
                Ok(observation) => runtime.push(json!({"dispatch_key":assignment.dispatch_key, "observation":observation})),
                Err(error) => errors.push(json!({"dispatch_key":assignment.dispatch_key,"status_code":error.status_code(),"reason":error.to_string()})),
            }
        }
        Ok(
            json!({"status":"accepted_for_dispatch", "receipt":receipt,"runtime":runtime,"errors":errors}),
        )
    }

    fn cancel_after_commit(&self, service: &Arc<TicketService>, command: &Command) {
        let Some(adapter) = self.adapter.get() else {
            return;
        };
        let Ok((_, snapshot)) = service.published() else {
            return;
        };
        for work in command.operations.iter().filter_map(|op| match op {
            Operation::Cancel { work_id }
            | Operation::Pause { work_id, .. }
            | Operation::UpdateContract { work_id, .. } => Some(work_id),
            _ => None,
        }) {
            let Some(assignment_id) = snapshot
                .tickets
                .get(work)
                .and_then(|w| w.active_assignment.as_ref())
            else {
                continue;
            };
            let assignment = snapshot.assignments[assignment_id].clone();
            if assignment.process_stopped
                || !matches!(
                    assignment.state,
                    AssignmentState::Cancelling | AssignmentState::OutcomeUnknown
                )
            {
                continue;
            }
            let service = service.clone();
            let adapter = adapter.clone();
            tokio::spawn(async move {
                let child_id = ticket_runtime::ticket_child_id(&assignment.dispatch_key);
                // Cancellation interrupts existing execution even if future
                // dispatch is disabled. Runner/lease disappearance alone does
                // not grant a stopped-process or released-resource fact.
                if let Err(error) = adapter.cancel_child_run_and_wait(&child_id).await {
                    tracing::warn!(assignment_id = %assignment.id, %error, "Ticket cancellation retained for reconciliation");
                    return;
                }
                let Ok(current) = service.published() else {
                    return;
                };
                let a = &current.1.assignments[&assignment.id];
                if a.runtime.is_some() || a.process_stopped {
                    return;
                }
                // An exact cancelled launch with no prepared Run receipt cannot
                // have received a RunSpec. Check canonical Host control plane.
                let Ok(Some(child)) = adapter.storage.load_runtime_control_plane(&child_id).await
                else {
                    return;
                };
                let Ok(Some(dispatch)) = ticket_runtime::read_dispatch(&child) else {
                    return;
                };
                if dispatch.assignment_id != a.id
                    || dispatch.receipt.is_some()
                    || child.last_run_status().as_deref() != Some("cancelled")
                    || !child.is_child_launch_cancelled(child.child_launch_generation())
                {
                    return;
                }
                let authority =
                    Authority::from_verified_host(current.1.binding, Principal::Runtime);
                let id = format!("runtime-queued-stop/{}", a.dispatch_key);
                if let Ok(command) = service.prepare_command(
                    &authority,
                    &id,
                    vec![Operation::ConfirmStopped {
                        assignment_id: a.id.clone(),
                        effects_reconciled: true,
                    }],
                ) {
                    if let Err(error) = service.execute(&authority, &command) {
                        tracing::warn!(assignment_id = %a.id, %error, "Ticket queued cancellation stop not published");
                    }
                }
            });
        }
    }

    async fn ensure(
        &self,
        key: &str,
        spec: &DispatchSpec,
    ) -> Result<ticket_runtime::DispatchObservation> {
        let service = self.service()?;
        let enabled = self.config.read().await.features.ticket_dispatch;
        if enabled {
            std::fs::create_dir_all(&self.workspace)?;
        }
        let adapter = self.adapter.get().ok_or_else(|| {
            Error::AuthorityUnavailable("existing Child adapter not ready".into())
        })?;
        adapter
            .ensure_ticket_dispatch(
                service,
                key,
                spec,
                &TicketDispatchPolicy {
                    enabled,
                    worker_role: "worker".into(),
                    workspace: self.workspace.clone(),
                },
            )
            .await
    }

    pub async fn query_dispatch(&self, key: &str) -> Result<ticket_runtime::DispatchObservation> {
        let (service, _) = self.authority(Principal::Runtime).await?;
        let spec = service
            .published()?
            .1
            .intents
            .get(key)
            .ok_or_else(|| Error::InvalidTransition("dispatch key absent".into()))?
            .immutable_spec
            .clone();
        ticket_runtime::query_dispatch(self.storage.as_ref(), &service, key, &spec).await
    }
}
