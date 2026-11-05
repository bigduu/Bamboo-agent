//! Bind the explicit Work Review to canonical Host records and a managed provider.

use std::{sync::Arc, time::Duration};

use bamboo_config::Config;
use bamboo_domain::{
    ProjectManifest, ProviderModelRef, SessionCallerReference, SupervisorReference,
};
use bamboo_engine::{model_config_helper::try_resolve_work_review_model, ticket_runtime};
use bamboo_llm::{work_text, LLMError, ProviderRegistry};
use bamboo_orchestration::work_review::{
    PreparedWorkReview, WorkReviewAdmission, WorkReviewEvidence, WorkReviewIo,
    WorkReviewProviderReply, WorkReviewSessionObservation, WorkReviewUsage,
};
use bamboo_tickets::{
    Authority, ExecutionTokenOverCapUsage, TicketService, WorkTokenReservation, WorkTokenSubmission,
};
use serde_json::{json, Value};

use super::{
    evidence::current_project_manifest,
    semantic::{check_project_after_ticket_proof, RegisteredRoute},
    Error, Principal, Result, TicketApplication,
};

fn review_target(config: &Config, registry: &Arc<ProviderRegistry>) -> Option<ProviderModelRef> {
    let route = try_resolve_work_review_model(config, registry).ok()??;
    route.managed_origin?;
    let mut target = route.target;
    target.reasoning_effort = target
        .reasoning_effort
        .or_else(|| config.reasoning_effort_for_key(&target.provider));
    Some(target)
}

#[derive(Clone)]
struct ReviewHost {
    app: Arc<TicketApplication>,
    service: Arc<TicketService>,
    supervisor: SupervisorReference,
    original: SessionCallerReference,
    project: ProjectManifest,
    route: Arc<RegisteredRoute>,
}

impl ReviewHost {
    async fn with_live_route<T>(
        &self,
        prepared: &PreparedWorkReview,
        admission: &WorkReviewAdmission,
        effect: impl FnOnce(&Authority) -> Result<T> + Send,
    ) -> Result<T> {
        let authority = self.accounting_authority().await?;
        if self
            .app
            .review_session_observation(&self.service, &prepared.evidence)
            .await
            != prepared.evidence.session_observation
        {
            return Err(Error::RevisionConflict);
        }
        let sources = self
            .app
            .patrol_sources
            .get()
            .ok_or(Error::RevisionConflict)?;
        let _settings = sources.config_io_lock.lock().await;
        let config = self.app.config.read().await;
        if !config.features.ticket_mutation
            || review_target(&config, &sources.providers).as_ref() != Some(&prepared.target)
            || self.route.check(&config).is_err()
        {
            return Err(Error::RevisionConflict);
        }
        self.app.verify_review_caller(&self.original).await?;
        let project = current_project_manifest(
            &sources.projects,
            &self.app.root,
            &self.service,
            &authority,
            &prepared.evidence.scope,
            prepared.evidence.source_revision,
        )?;
        if project != self.project {
            return Err(Error::RevisionConflict);
        }
        admission.while_open(|| effect(&authority))
    }
}

#[async_trait::async_trait]
impl WorkReviewIo for ReviewHost {
    async fn reserve(
        &self,
        prepared: &PreparedWorkReview,
        now_ms: u64,
        admission: &WorkReviewAdmission,
    ) -> Result<WorkTokenSubmission> {
        self.with_live_route(prepared, admission, |authority| {
            prepared.reserve(&self.service, authority, now_ms)
        })
        .await
    }

    async fn complete(
        &self,
        prepared: &PreparedWorkReview,
        receipt: &WorkTokenReservation,
        admission: &WorkReviewAdmission,
    ) -> WorkReviewProviderReply {
        let admission: work_text::Admission = {
            let host = self.clone();
            let prepared = prepared.clone();
            let receipt = receipt.clone();
            let gate = admission.clone();
            Arc::new(move || {
                let host = host.clone();
                let prepared = prepared.clone();
                let receipt = receipt.clone();
                let gate = gate.clone();
                Box::pin(async move {
                    host.with_live_route(&prepared, &gate, |authority| {
                        let sources = host
                            .app
                            .patrol_sources
                            .get()
                            .ok_or(Error::RevisionConflict)?;
                        check_project_after_ticket_proof(
                            &sources.projects,
                            &host.app.root,
                            &host.project,
                            || {
                                prepared
                                    .confirm_submission(&host.service, authority, &receipt)
                                    .map_err(|_| bamboo_orchestration::patrol::PortError::Conflict)
                            },
                        )
                        .map_err(|_| Error::RevisionConflict)
                    })
                    .await
                    .map_err(|_| LLMError::Auth("Work review admission changed".into()))
                })
            })
        };
        let response = work_text::review(
            &self.route.transport,
            self.route.provider.as_ref(),
            work_text::Request {
                model: &prepared.target.model,
                messages: &prepared.messages,
                input_cap: receipt.request.input_cap,
                output_cap: receipt.request.output_cap,
                remaining: Duration::from_secs(60),
            },
            admission,
            prepared.target.reasoning_effort,
        )
        .await;
        match response {
            work_text::WorkTextReply::Complete { text, exact_usage } => WorkReviewProviderReply {
                text,
                usage: exact_usage.map_or(WorkReviewUsage::Unknown, WorkReviewUsage::Observed),
            },
            work_text::WorkTextReply::OverCap(report) => WorkReviewProviderReply {
                text: None,
                usage: WorkReviewUsage::OverCap(ExecutionTokenOverCapUsage {
                    input_tokens: report.input_tokens,
                    output_tokens: report.output_tokens,
                    total_tokens: report.total_tokens,
                }),
            },
            work_text::WorkTextReply::NotSent => WorkReviewProviderReply {
                text: None,
                usage: WorkReviewUsage::NotSent,
            },
            work_text::WorkTextReply::Unknown => WorkReviewProviderReply {
                text: None,
                usage: WorkReviewUsage::Unknown,
            },
        }
    }

    async fn accounting_authority(&self) -> Result<Authority> {
        let (service, authority) = self.app.authority(Principal::Runtime).await?;
        if !Arc::ptr_eq(&service, &self.service) {
            return Err(Error::RevisionConflict);
        }
        verify_supervisor(&service, &self.supervisor)?;
        Ok(authority)
    }
}

fn verify_supervisor(service: &TicketService, supervisor: &SupervisorReference) -> Result<()> {
    let binding = service.published()?.1.binding;
    if binding.supervisor_session_id != supervisor.session_id
        || binding.scope_id != format!("supervisor/{}", supervisor.incarnation_id)
    {
        return Err(Error::ScopeDenied(
            "Work Review Supervisor lifetime changed".into(),
        ));
    }
    Ok(())
}

impl TicketApplication {
    pub(crate) async fn review_work(
        self: &Arc<Self>,
        supervisor: SupervisorReference,
        original: SessionCallerReference,
        project_id: &str,
        work_id: &str,
        assignment_id: &str,
        operation_id: uuid::Uuid,
    ) -> Result<Value> {
        let (service, authority) = self.authority(Principal::Runtime).await?;
        verify_supervisor(&service, &supervisor)?;
        if original.session_id != supervisor.session_id {
            return Err(Error::ScopeDenied(
                "Work Review caller differs from Supervisor".into(),
            ));
        }
        self.verify_review_caller(&original).await?;
        let mut evidence =
            WorkReviewEvidence::capture(&service, &authority, project_id, work_id, assignment_id)?;
        evidence.session_observation = self.review_session_observation(&service, &evidence).await;
        // A replay remains readable during an accounting outage. The live
        // review admission path requires that retained evidence settle first.
        let _ = self.work_reviews.reconcile_accounting(&service, &authority);
        let operation_id = operation_id.to_string();
        if let Some(record) = evidence.recorded_reservation(&service, &authority, &operation_id)? {
            return Ok(
                json!({"status":"existing_receipt","accounting":record,"evidence":evidence}),
            );
        }
        let Some(sources) = self.patrol_sources.get() else {
            return Ok(json!({"status":"model_unavailable","evidence":evidence}));
        };
        let route = {
            let _settings = sources.config_io_lock.lock().await;
            let config = self.config.read().await;
            if !config.features.ticket_mutation {
                return Ok(json!({"status":"model_unavailable","evidence":evidence}));
            }
            review_target(&config, &sources.providers).and_then(|target| {
                RegisteredRoute::capture_configured(sources, &config, target).ok()
            })
        };
        let Some(route) = route else {
            return Ok(json!({"status":"model_unavailable","evidence":evidence}));
        };
        let prepared = match PreparedWorkReview::new(
            evidence.clone(),
            route.target.clone(),
            &operation_id,
            route.model_ceilings,
        ) {
            Ok(prepared) => prepared,
            Err(Error::ContextBudgetExceeded) => {
                return Ok(json!({"status":"evidence_exceeds_call_ceiling","evidence":evidence}));
            }
            Err(error) => return Err(error),
        };
        let project = match current_project_manifest(
            &sources.projects,
            &self.root,
            &service,
            &authority,
            &prepared.evidence.scope,
            prepared.evidence.source_revision,
        ) {
            Ok(project) => project,
            Err(_) => {
                return Ok(json!({"status":"source_unavailable","evidence":prepared.evidence}))
            }
        };
        let host = ReviewHost {
            app: self.clone(),
            service: service.clone(),
            supervisor,
            original,
            project,
            route: Arc::new(route),
        };
        let now = chrono::Utc::now()
            .timestamp_millis()
            .try_into()
            .map_err(|_| Error::AuthorityUnavailable("Work Review clock unavailable".into()))?;
        let evidence = serde_json::to_value(&prepared.evidence)?;
        let result = self
            .work_reviews
            .execute(service, prepared, now, host)
            .await?;
        let mut output = serde_json::to_value(result)?;
        output["evidence"] = evidence;
        Ok(output)
    }

    async fn verify_review_caller(&self, original: &SessionCallerReference) -> Result<()> {
        let current = self
            .storage
            .load_root_authority(&original.session_id)
            .await?;
        if !current
            .as_ref()
            .is_some_and(|session| original.matches_session(session))
        {
            return Err(Error::ScopeDenied("Work Review caller changed".into()));
        }
        Ok(())
    }

    async fn review_session_observation(
        &self,
        service: &TicketService,
        evidence: &WorkReviewEvidence,
    ) -> Option<WorkReviewSessionObservation> {
        let recorded = evidence.session.as_ref()?;
        let (_, snapshot) = service.published().ok()?;
        let intent = snapshot.intents.get(&recorded.dispatch_key)?;
        let observation = ticket_runtime::query_dispatch(
            self.storage.as_ref(),
            service,
            &recorded.dispatch_key,
            &intent.immutable_spec,
        )
        .await
        .ok()?;
        match observation {
            ticket_runtime::DispatchObservation::Admitted { receipt } if &receipt == recorded => {
                Some(WorkReviewSessionObservation::Admitted)
            }
            ticket_runtime::DispatchObservation::Terminal {
                receipt,
                terminal_status,
            } if &receipt == recorded => {
                Some(WorkReviewSessionObservation::Terminal { terminal_status })
            }
            _ => None,
        }
    }
}
