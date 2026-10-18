//! Server Host adapter for the independent orchestration policy crate.
use super::*;
use bamboo_orchestration::{ReadyDispatch, ReadyWorkHost};

impl TicketApplication {
    pub async fn install_ready_work_policy(&self, policy: ReadyWorkPolicy) -> Result<()> {
        self.ready_scheduler.install(self, policy).await
    }

    pub async fn revoke_ready_work_policy(&self) {
        self.ready_scheduler.revoke().await;
    }

    pub fn wake_ready_work(&self) {
        self.ready_scheduler.wake();
    }

    pub async fn reconcile_ready_work(&self) -> Result<ReadyReconcile> {
        self.ready_scheduler.reconcile(self).await
    }

    pub(in crate::app_state) fn spawn_ready_worker(
        self: &Arc<Self>,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        let host: Arc<dyn ReadyWorkHost> = self.clone();
        self.ready_scheduler.spawn(Arc::downgrade(&host), shutdown);
    }

    pub(in crate::app_state) async fn shutdown_ready_worker(&self) {
        self.ready_scheduler.shutdown().await;
    }
}

#[async_trait::async_trait]
impl ReadyWorkHost for TicketApplication {
    async fn authority(&self) -> Result<(Arc<TicketService>, Authority)> {
        TicketApplication::authority(
            self,
            Principal::Supervisor {
                session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
            },
        )
        .await
    }

    async fn check_budget(&self, max_rounds: u32) -> Result<()> {
        if self
            .adapter
            .get()
            .is_some_and(|adapter| !adapter.supports_native_ticket_budget())
        {
            return Err(Error::ScopeDenied(
                "Ready Work requires the assembled native Tokio backend".into(),
            ));
        }
        let config = self.config.read().await;
        if !config.features.ticket_mutation || !config.features.ticket_dispatch {
            return Err(Error::AuthorityUnavailable(
                "Ticket scheduling features are disabled".into(),
            ));
        }
        if !config
            .run_budget
            .max_rounds
            .is_some_and(|n| n > 0 && n <= max_rounds)
        {
            return Err(Error::ResourceBlocked(
                "a finite configured run round ceiling is required".into(),
            ));
        }
        Ok(())
    }

    async fn preflight(&self, command: &Command) -> Result<()> {
        let service = self.service()?;
        self.validate_project_operations(&service, command)?;
        self.preflight_ticket_starts(
            command
                .operations
                .iter()
                .filter_map(|operation| {
                    if let Operation::Start { workspace, .. } = operation {
                        Some(workspace.clone())
                    } else {
                        None
                    }
                })
                .collect(),
        )
        .await
    }

    async fn ensure(
        &self,
        key: &str,
        spec: &DispatchSpec,
        max_rounds: u32,
    ) -> Result<ReadyDispatch> {
        self.ensure_ready_dispatch(key, spec, max_rounds, None)
            .await
    }
    async fn dispatch(&self, command: &Command, max_rounds: u32) -> Result<ReadyDispatch> {
        self.dispatch_ready_command(command, max_rounds, None).await
    }
    async fn ensure_fenced(
        &self,
        key: &str,
        spec: &DispatchSpec,
        max_rounds: u32,
        fence: &bamboo_orchestration::ReadyAdmissionFence,
    ) -> Result<ReadyDispatch> {
        fence.check()?;
        self.ensure_ready_dispatch(key, spec, max_rounds, Some(fence))
            .await
    }
    async fn dispatch_fenced(
        &self,
        command: &Command,
        max_rounds: u32,
        fence: &bamboo_orchestration::ReadyAdmissionFence,
    ) -> Result<ReadyDispatch> {
        fence.check()?;
        self.dispatch_ready_command(command, max_rounds, Some(fence))
            .await
    }
}

impl TicketApplication {
    async fn ensure_ready_dispatch(
        &self,
        key: &str,
        spec: &DispatchSpec,
        max_rounds: u32,
        fence: Option<&bamboo_orchestration::ReadyAdmissionFence>,
    ) -> Result<ReadyDispatch> {
        match self
            .ensure_with_round_ceiling(key, spec, Some(max_rounds), fence)
            .await?
        {
            ticket_runtime::DispatchObservation::OutcomeUnknown { reason, .. } => {
                Ok(ReadyDispatch::Blocked {
                    kind: ReadyBlockKind::OutcomeUnknown,
                    reason,
                })
            }
            _ => Ok(ReadyDispatch::Observed {
                dispatch_key: key.into(),
            }),
        }
    }

    async fn dispatch_ready_command(
        &self,
        command: &Command,
        max_rounds: u32,
        fence: Option<&bamboo_orchestration::ReadyAdmissionFence>,
    ) -> Result<ReadyDispatch> {
        let response = self
            .dispatch_with_round_ceiling(
                Principal::Supervisor {
                    session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
                },
                command,
                Some(max_rounds),
                fence,
            )
            .await?;
        if response["errors"]
            .as_array()
            .is_some_and(|errors| !errors.is_empty())
        {
            let kind = match response["errors"][0]["status_code"].as_u64() {
                Some(403) => ReadyBlockKind::Permission,
                Some(409 | 422) => ReadyBlockKind::Configuration,
                Some(423) => ReadyBlockKind::Resource,
                _ => ReadyBlockKind::Transient,
            };
            return Ok(ReadyDispatch::Blocked {
                kind,
                reason: response["errors"].to_string(),
            });
        }
        if response["runtime"].as_array().is_some_and(|observations| {
            observations
                .iter()
                .any(|o| o["observation"]["status"] == "outcome_unknown")
        }) {
            return Ok(ReadyDispatch::Blocked {
                kind: ReadyBlockKind::OutcomeUnknown,
                reason: "runtime admission outcome is unknown; retained the same intent".into(),
            });
        }
        let (_, snapshot) = self.service()?.published()?;
        let id = response["receipt"]["ids"]["assignment"]
            .as_str()
            .ok_or_else(|| Error::AuthorityUnavailable("claimed Assignment missing".into()))?;
        let assignment = snapshot
            .assignments
            .get(id)
            .ok_or_else(|| Error::AuthorityUnavailable("claimed Assignment missing".into()))?;
        Ok(ReadyDispatch::Observed {
            dispatch_key: assignment.dispatch_key.clone(),
        })
    }
}
