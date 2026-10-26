//! Stage the existing decision intent without granting model activation.

use bamboo_domain::{
    SessionMessageBody, SessionMessageContent, SessionMessageEnvelope, SessionMessageId,
    SessionMessageKind, SessionMessageSource, SessionRuntimeInstruction, SupervisorReference,
};
use bamboo_orchestration::patrol::{delivery::PendingNotice, host};
use bamboo_tickets::ScopeBinding;
use serde_json::json;

use super::super::{Error, Principal, Result, TicketApplication};

struct CancelPending(bamboo_domain::AdmissionGate);
impl Drop for CancelPending {
    fn drop(&mut self) {
        self.0.cancel_if_pending();
    }
}

fn envelope(binding: &ScopeBinding, notice: &PendingNotice) -> Result<SessionMessageEnvelope> {
    let escalation = &notice.escalation;
    // Neither mutable delivery counters nor retry timestamps enter the logical
    // ID or payload. An enqueue-before-checkpoint-ACK crash retries equal bytes.
    let data = json!({
        "binding": binding,
        "operation": escalation.operation,
        "reason": escalation.reason,
        "source_revision": escalation.source_revision,
        "cursor": escalation.cursor,
        "opened_at_ms": notice.opened_at_ms,
    });
    let id = SessionMessageId::stable("ticket-patrol-escalation-v1", &data);
    let scope = &escalation.operation.incident.scope;
    let content = format!(
        "Patrol decision pending for Project {}, Work {}, Assignment {}, generation {}: {:?}. \
         Evidence revision {}, cursor {}. This notice does not establish completion or recovery; \
         inspect current authoritative facts before deciding. It grants no execution or model budget.",
        scope.project_id, scope.work_id, scope.assignment_id, scope.generation,
        escalation.reason, escalation.source_revision, escalation.cursor,
    );
    let notice = SessionMessageEnvelope {
        id,
        source: SessionMessageSource::Runtime {
            subsystem: "ticket_patrol".into(),
        },
        target_session_id: binding.supervisor_session_id.clone(),
        kind: SessionMessageKind::RuntimeInstruction,
        body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
            instruction: "ticket_patrol_escalation_v1".into(),
            content: Some(SessionMessageContent::text(content)),
            data: Some(data),
            provider_message: None,
        }),
        created_at: chrono::DateTime::from_timestamp_millis(
            i64::try_from(notice.opened_at_ms).map_err(|_| Error::RevisionConflict)?,
        )
        .ok_or(Error::RevisionConflict)?,
        thread_id: None,
        in_reply_to: None,
        attempt: None,
        correlation_id: None,
    };
    notice
        .validate()
        .map_err(|error| Error::AuthorityUnavailable(error.to_string()))?;
    if serde_json::to_vec(&notice)?.len() > 16 * 1024 {
        return Err(Error::ContextBudgetExceeded);
    }
    Ok(notice)
}

impl TicketApplication {
    pub(super) async fn stage_ready_patrol_notice(
        &self,
        expected: &ScopeBinding,
        pending: &PendingNotice,
        fence: &bamboo_orchestration::ReadyAdmissionFence,
    ) -> Result<bool> {
        let (service, authority) = self.authority(Principal::Runtime).await?;
        if &service.published()?.1.binding != expected {
            return Err(Error::RevisionConflict);
        }
        // Scope selectors and a recovered checkpoint are never read grants.
        host::read_observation(
            &service,
            &authority,
            &pending.escalation.operation.incident.scope,
        )
        .map_err(super::patrol_error)?;
        let supervisor = SupervisorReference {
            session_id: expected.supervisor_session_id.clone(),
            incarnation_id: expected
                .scope_id
                .strip_prefix("supervisor/")
                .and_then(|id| id.parse().ok())
                .ok_or_else(|| Error::ScopeDenied("invalid Supervisor binding".into()))?,
        };
        let envelope = envelope(expected, pending)?;
        let inbox = self
            .messenger
            .get()
            .ok_or_else(|| Error::AuthorityUnavailable("Patrol Inbox is not assembled".into()))?
            .inbox();
        if pending.may_enqueue {
            let guard = CancelPending(fence.patrol_notice_gate()?);
            inbox
                .stage_supervisor_patrol_notice(
                    &supervisor,
                    expected.binding_revision,
                    &envelope,
                    &guard.0,
                )
                .await
                .map_err(|error| Error::AuthorityUnavailable(error.to_string()))?;
            Ok(true)
        } else {
            // Cooldown/exhaustion must not hide an enqueue-before-ACK crash.
            // This lookup cannot enqueue or grant activation to another item.
            inbox
                .existing_staged_delivery(&envelope)
                .await
                .map(|receipt| receipt.is_some())
                .map_err(|error| Error::AuthorityUnavailable(error.to_string()))
        }
    }
}
