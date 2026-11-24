use super::*;
use bamboo_domain::SessionActivationPolicy;
use bamboo_domain::{
    SessionMessageBody, SessionMessageContent, SessionMessageEnvelope, SessionMessageId,
    SessionMessageKind, SessionMessageSource, SessionProviderMessage, SessionRuntimeInstruction,
};
use serde::Deserialize;

impl TicketApplication {
    async fn latest_admitted_human(&self, snapshot: &Snapshot) -> Result<Option<String>> {
        if !snapshot
            .resolutions
            .values()
            .any(MessageResolution::is_pending_human)
        {
            return Ok(None);
        }
        let session = self
            .storage
            .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
            .await?
            .filter(|session| {
                session
                    .supervisor_incarnation_id()
                    .is_some_and(|incarnation| {
                        snapshot.binding.scope_id == format!("supervisor/{incarnation}")
                    })
            })
            .ok_or_else(|| Error::ScopeDenied("canonical Supervisor session changed".into()))?;
        // Ingress registration can run ahead of Inbox delivery. Pick the
        // latest Human input checkpointed in the transcript, then test its
        // resolution state. Older or queued input cannot become the default.
        let latest = session.messages.iter().rev().find(|message| {
            message
                .metadata
                .as_ref()
                .and_then(|m| m.get("session_message"))
                .and_then(|value| SessionMessageEnvelope::deserialize(value).ok())
                .is_some_and(|envelope| {
                    envelope.id.as_str() == message.id && envelope.is_human_interactive_input()
                })
        });
        Ok(latest
            .and_then(|message| snapshot.resolutions.get(&message.id))
            .filter(|record| record.is_pending_human())
            .map(|record| record.message_id.clone()))
    }

    /// The Host derives the User from its authenticated persisted ingress. No
    /// model argument can select the User, source, scope, or operation identity.
    async fn message_authority(&self, message_id: &str) -> Result<(Arc<TicketService>, Authority)> {
        let service = self.service()?;
        let snapshot = service.published()?.1;
        let user_id = snapshot
            .resolutions
            .get(message_id)
            .and_then(|r| r.ingress.as_ref())
            .map(|r| r.user_id.clone())
            .ok_or_else(|| Error::ScopeDenied("canonical Host Human ingress missing".into()))?;
        self.authority(Principal::User { user_id }).await
    }

    pub async fn pending_message(
        &self,
        message_id: Option<String>,
        plan_read_only: bool,
    ) -> Result<Value> {
        let id = match message_id {
            Some(id) => id,
            None => {
                let (_, snapshot) = self.service()?.published()?;
                let Some(id) = self.latest_admitted_human(&snapshot).await? else {
                    return Ok(Value::Null);
                };
                id
            }
        };
        let (service, authority) = self.message_authority(&id).await?;
        if plan_read_only {
            return Ok(json!({"read_only":true,"message_id":id}));
        }
        if !self.config.read().await.features.ticket_mutation
            || service.health() != Health::Writable
        {
            return Ok(
                json!({"read_only":true,"message":service.message_resolution(&authority,&id)?}),
            );
        }
        let input = service.resolution_input(&authority, &id, 100, 65536)?;
        let saved = service.message_resolution(&authority, &id)?;
        Ok(json!({"input":input,"saved_proposal":saved.proposal,"group_results":saved.groups}))
    }

    /// Save the complete proposal before settling independent groups. Model
    /// inference happened outside all Ticket transactions. Dispatch is the
    /// same existing post-receipt path as typed commands, never a second loop.
    pub async fn resolve_message(
        &self,
        message_id: &str,
        proposal: &MessageProposal,
    ) -> Result<Value> {
        if !self.config.read().await.features.ticket_mutation {
            return Err(Error::AuthorityUnavailable(
                "Ticket mutation feature is disabled".into(),
            ));
        }
        let mut admission = Some(
            self.update_admission
                .enter()
                .await
                .map_err(|error| Error::ResourceBlocked(error.to_string()))?,
        );
        let (service, authority) = self.message_authority(message_id).await?;
        let prior = service.message_resolution(&authority, message_id)?;
        if prior.proposal.is_some() {
            // Reuse the core's frozen identity check before mutable Host facts
            // can mask an altered retry. A saved proposal does not republish.
            service.save_message_proposal(&authority, message_id, proposal)?;
        }
        // Reject an unusable new Start before freezing its immutable proposal.
        // Already terminal groups replay their original receipts; the service
        // still checks the complete saved proposal identity below.
        let pending_groups = proposal
            .groups
            .iter()
            .filter(|group| {
                prior
                    .proposal
                    .as_ref()
                    .and_then(|saved| {
                        saved
                            .groups
                            .iter()
                            .position(|saved| saved.group_id == group.group_id)
                    })
                    .and_then(|index| prior.groups.get(index))
                    .is_none_or(|saved| saved.status == ResolutionStatus::Proposed)
            })
            .collect::<Vec<_>>();
        let workspaces = pending_groups
            .iter()
            .flat_map(|group| group.operations.iter())
            .filter_map(|op| match op {
                SemanticOperation::Start { workspace, .. } => Some(workspace.clone()),
                _ => None,
            })
            .collect();
        self.preflight_ticket_starts(workspaces).await?;
        let snapshot = service.published()?.1;
        for group in pending_groups {
            let operations = group
                .operations
                .iter()
                .map(SemanticOperation::to_typed_operation)
                .collect::<Vec<_>>();
            self.validate_project_snapshot_operations(&snapshot, &operations)?;
        }
        service.save_message_proposal(&authority, message_id, proposal)?;
        let resolution = service.settle_message(&authority, message_id)?;
        let mut dispatch = Vec::new();
        for group in &resolution.groups {
            let Some(receipt) = &group.receipt else {
                continue;
            };
            let command: Command = serde_json::from_str(&receipt.canonical_request)?;
            // Only replay still-current pending/admitted intents. A stopped,
            // superseded or outcome-unknown attempt must never be revived.
            let snapshot = service.published()?.1;
            let may_enqueue = snapshot.assignments.values().any(|a| {
                receipt.ids.values().any(|id| id == &a.id)
                    && !a.process_stopped
                    && matches!(
                        a.state,
                        AssignmentState::DispatchPending | AssignmentState::Admitted
                    )
                    && snapshot.tickets.get(&a.work_id).is_some_and(|w| {
                        w.generation == a.generation
                            && w.contract_revision == a.contract_revision
                            && w.active_assignment.as_ref() == Some(&a.id)
                    })
            });
            let dispatches = may_enqueue || self.cancel_after_commit(&service, &command);
            if dispatches {
                // A pending runtime receipt or a registered cancellation now
                // owns work. Release the permit before ensure tries admission.
                if let Some(mut permit) = admission.take() {
                    permit.record_dispatch();
                }
            }
            if may_enqueue {
                dispatch.push(
                    self.enqueue_receipt(&service, &command, receipt.clone())
                        .await?,
                );
            }
        }
        Ok(json!({"status":"resolved","resolution":resolution,"dispatch":dispatch}))
    }

    pub(crate) async fn dismiss_ingress(
        &self,
        principal: Principal,
        binding: &ScopeBinding,
        message_id: &str,
    ) -> Result<()> {
        if !self.config.read().await.features.ticket_mutation {
            return Err(Error::AuthorityUnavailable(
                "Ticket mutation feature is disabled".into(),
            ));
        }
        let (service, authority) = self.authority(principal).await?;
        if &service.published()?.1.binding != binding {
            return Err(Error::ScopeDenied(
                "Supervisor scope binding changed".into(),
            ));
        }
        // The store checks the owner and the immutable proposal under its
        // writer lock. The exact dismissal can replay; a saved Work proposal
        // conflicts with the empty proposal.
        service.save_message_proposal(
            &authority,
            message_id,
            &MessageProposal { groups: vec![] },
        )?;
        Ok(())
    }

    // Precise UI decisions wake the existing Inbox/activation machinery after
    // the durable decision. The notification carries no User approval grant.
    pub(super) async fn wake_after_receipt(&self, receipt: &OperationReceipt) -> Result<()> {
        let messenger = self.messenger.get().ok_or_else(|| {
            Error::AuthorityUnavailable(format!(
                "Ticket decision {} committed; retry the exact command to deliver its wake: messenger unavailable",
                receipt.operation_id
            ))
        })?;
        let display = format!("Ticket decision committed at seq {}. Read work_overview and exact requests before scheduling any ready work.", receipt.committed_seq);
        let content = SessionMessageContent::text(display);
        let envelope = SessionMessageEnvelope {
            id: SessionMessageId::stable(
                "ticket-decision-wake-v1",
                &json!({"operation_id":receipt.operation_id,"request_hash":receipt.request_hash}),
            ),
            source: SessionMessageSource::Runtime {
                subsystem: "ticket_application".into(),
            },
            target_session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
            kind: SessionMessageKind::RuntimeInstruction,
            body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                instruction: "ticket_decision_committed_v1".into(),
                content: Some(content.clone()),
                data: Some(
                    json!({"committed_seq":receipt.committed_seq,"operation_id":receipt.operation_id}),
                ),
                provider_message: Some(SessionProviderMessage {
                    content,
                    metadata: json!({"hidden_from_ui":true})
                        .as_object()
                        .expect("metadata object")
                        .clone(),
                    never_compress: true,
                }),
            }),
            created_at: chrono::Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: None,
        };
        messenger
            .send_with_policy(envelope, SessionActivationPolicy::InterruptSpecificWait)
            .await
            .map_err(|error| Error::AuthorityUnavailable(format!(
                "Ticket decision {} committed; retry the exact command to deliver its wake: {error}",
                receipt.operation_id
            )))?;
        Ok(())
    }
}
