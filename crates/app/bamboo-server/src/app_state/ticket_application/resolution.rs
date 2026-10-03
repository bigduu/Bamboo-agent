use super::*;
use bamboo_domain::SessionActivationPolicy;
use bamboo_domain::{
    SessionMessageBody, SessionMessageContent, SessionMessageEnvelope, SessionMessageId,
    SessionMessageKind, SessionMessageSource, SessionProviderMessage, SessionRuntimeInstruction,
};

impl TicketApplication {
    pub fn oldest_unresolved_human(&self) -> Result<Option<String>> {
        let (_, snapshot) = self.service()?.published()?;
        Ok(snapshot
            .resolutions
            .values()
            .filter(|r| {
                r.ingress.is_some()
                    && (r.proposal.is_none()
                        || r.groups
                            .iter()
                            .any(|g| g.status == ResolutionStatus::Proposed))
            })
            .min_by_key(|r| r.ingress_seq)
            .map(|r| r.message_id.clone()))
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

    pub async fn pending_message(&self) -> Result<Value> {
        let Some(id) = self.oldest_unresolved_human()? else {
            return Ok(Value::Null);
        };
        let (service, authority) = self.message_authority(&id).await?;
        if !self.config.read().await.features.ticket_mutation {
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
        let (service, authority) = self.message_authority(message_id).await?;
        let prior = service.message_resolution(&authority, message_id)?;
        if prior.proposal.is_none()
            && self.oldest_unresolved_human()?.as_deref() != Some(message_id)
        {
            return Err(Error::ResourceBlocked(
                "resolve the oldest Human ingress first".into(),
            ));
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
            if may_enqueue {
                dispatch.push(
                    self.enqueue_receipt(&service, &command, receipt.clone())
                        .await?,
                );
            } else {
                self.cancel_after_commit(&service, &command);
            }
        }
        Ok(json!({"status":"resolved","resolution":resolution,"dispatch":dispatch}))
    }

    // Precise UI decisions wake the existing Inbox/activation machinery after
    // the durable decision. The notification carries no User approval grant.
    pub(super) async fn wake_after_receipt(&self, receipt: &OperationReceipt) {
        let Some(messenger) = self.messenger.get() else {
            return;
        };
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
                    metadata: Default::default(),
                    never_compress: true,
                }),
            }),
            created_at: chrono::Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: None,
        };
        if let Err(error) = messenger
            .send_with_policy(envelope, SessionActivationPolicy::InterruptSpecificWait)
            .await
        {
            tracing::warn!(operation_id = %receipt.operation_id, %error, "committed Ticket decision notification needs reconciliation");
        }
    }
}
