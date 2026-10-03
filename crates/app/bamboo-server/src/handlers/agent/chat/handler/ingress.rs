//! Authenticated transport admission; client references never select a role.
use super::ChatRequest;
use crate::{app_state::AppState, handlers::agent::tickets};
use actix_web::{http::StatusCode, HttpRequest, HttpResponse, ResponseError};
use bamboo_domain::{Session, SessionInboxReceipt, SessionMessageEnvelope, SessionMessageId};
use bamboo_engine::ticket_worker_plan::tickets::{
    HumanIngressRecord, Principal, VerifiedUserIngress,
};

fn error(status: StatusCode, reason: impl ToString) -> HttpResponse {
    crate::error::json_error(status, reason.to_string())
}

pub(super) async fn queue(
    state: &AppState,
    session: &Session,
    request: &ChatRequest,
    effective_message: &str,
    http: &HttpRequest,
) -> Result<Option<SessionInboxReceipt>, HttpResponse> {
    let ticket = state.config.read().await.features.ticket_mutation
        && state.tickets.service().ok().is_some_and(|s| {
            s.published()
                .is_ok_and(|(_, snapshot)| snapshot.binding.supervisor_session_id == session.id)
        });
    if !ticket
        && [
            &request.message_id,
            &request.thread_id,
            &request.in_reply_to,
            &request.correlation_id,
        ]
        .into_iter()
        .all(Option::is_none)
    {
        return Ok(None);
    }
    let principal = tickets::user(state, http)
        .await
        .map_err(|e| tickets::TicketHttpError::from(e).error_response())?;
    if [
        &request.thread_id,
        &request.in_reply_to,
        &request.correlation_id,
    ]
    .into_iter()
    .flatten()
    .any(|s| s.is_empty() || s.len() > 128)
        || request
            .correlation_id
            .as_deref()
            .is_some_and(|s| s.starts_with("session-guidance") || s.starts_with("child_completion"))
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "Invalid message references; tracing cannot select Runtime activation policy",
        ));
    }
    if ticket && (request.message.is_empty() || request.message.len() > 32768) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "Ticket Human input requires 1..32768 UTF-8 bytes",
        ));
    }
    let mut envelope = SessionMessageEnvelope::user_input(&session.id, effective_message);
    if let Some(id) = &request.message_id {
        envelope.id =
            SessionMessageId::parse(id.clone()).map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    }
    envelope.thread_id = request.thread_id.clone();
    envelope.in_reply_to = request
        .in_reply_to
        .as_ref()
        .map(|id| SessionMessageId::parse(id.clone()))
        .transpose()
        .map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    envelope.correlation_id = request.correlation_id.clone();
    if let Some(images) = request.images.as_ref().filter(|v| !v.is_empty()) {
        if images.len() > 16 {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "A message supports up to 16 images",
            ));
        }
        let mut parts = vec![bamboo_domain::MessagePart::Text {
            text: effective_message.into(),
        }];
        for image in images {
            let (_, url) = state
                .session_store
                .write_image_attachment_deduplicated(
                    session,
                    &image.base64,
                    image.mime_type.as_deref(),
                )
                .await
                .map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
            parts.push(bamboo_domain::MessagePart::ImageUrl {
                image_url: bamboo_domain::ImageUrlRef { url, detail: None },
            });
        }
        envelope.body =
            bamboo_domain::SessionMessageBody::Content(bamboo_domain::SessionMessageContent {
                text: effective_message.into(),
                parts,
            });
    }
    envelope
        .validate()
        .map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    // A new ordinary session needs a canonical address before delivery. This
    // contains no User turn or live workflow pin; the final chat checkpoint
    // remains responsible for those. A failed delivery can retry the same ID.
    if state
        .persistence
        .storage()
        .load_session(&session.id)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e))?
        .is_none()
    {
        state
            .persistence
            .storage()
            .save_session(session)
            .await
            .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    }
    if ticket {
        let Principal::User { user_id } = &principal else {
            unreachable!("verified owner")
        };
        let (service, authority) = state
            .tickets
            .authority(principal.clone())
            .await
            .map_err(|e| tickets::TicketHttpError::from(e).error_response())?;
        let snapshot = service
            .published()
            .map_err(|e| tickets::TicketHttpError::from(e).error_response())?
            .1;
        // The session persistence guard serializes authenticated HTTP ingress.
        // Ticket ingress order and Inbox delivery generation are independent
        // axes. Persist the Human intent first, so an Inbox/Host crash cannot
        // discard its source or let a later message overtake an unrecorded one.
        let source_ingress_seq = snapshot
            .resolutions
            .get(envelope.id.as_str())
            .and_then(|r| r.ingress.as_ref())
            .map(|r| r.source_ingress_seq)
            .unwrap_or_else(|| {
                snapshot
                    .resolutions
                    .values()
                    .filter_map(|r| r.ingress.as_ref())
                    .map(|r| r.source_ingress_seq)
                    .max()
                    .unwrap_or(0)
                    + 1
            });
        let ingress = VerifiedUserIngress::from_verified_host(
            envelope.id.to_string(),
            HumanIngressRecord {
                user_id: user_id.clone(),
                source_ingress_seq,
                text: request.message.clone(),
                thread_id: envelope.thread_id.clone(),
                in_reply_to: envelope.in_reply_to.as_ref().map(ToString::to_string),
                correlation_id: envelope.correlation_id.clone(),
            },
        )
        .map_err(|e| tickets::TicketHttpError::from(e).error_response())?;
        service
            .register_user_ingress(&authority, &ingress)
            .map_err(|e| tickets::TicketHttpError::from(e).error_response())?;
    }
    let receipt = state
        .session_messenger
        .admit_with_activation_intent(
            envelope,
            bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
            None,
        )
        .await
        .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, e))?
        .delivery;
    // The existing SDK admits this exact durable envelope at its safe boundary
    // and emits MessageAppended. Do not append a second HTTP-owned User turn.
    Ok(Some(receipt))
}

/// Fresh HTTP execute needs a User turn before its legacy preparation gate.
/// Reuse the SDK's exact checkpoint/receipt/ACK boundary, and never compete
/// with a live runner's inbox consumer. This adapter owns no second protocol.
pub(crate) async fn admit_for_execute(state: &AppState, id: &str) -> Result<(), HttpResponse> {
    let runners = state.agent_runners.read().await;
    if runners.get(id).is_some_and(|r| {
        matches!(
            r.status,
            crate::app_state::AgentStatus::Pending | crate::app_state::AgentStatus::Running
        )
    }) {
        return Ok(());
    }
    let Some(mut session) = state
        .storage
        .load_session(id)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e))?
    else {
        return Ok(());
    };
    let Some(queued_id) = session.metadata.get("chat.queued_ingress.v1").cloned() else {
        return Ok(());
    };
    let persistence: std::sync::Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
        state.persistence.clone();
    let refreshed = bamboo_engine::runner::refresh_turn_boundary_with_inbox(
        &mut session,
        Some(&state.storage),
        Some(&persistence),
        Some(&state.session_inbox),
    )
    .await;
    if let Some(reason) = refreshed.admission_error {
        return Err(error(StatusCode::SERVICE_UNAVAILABLE, reason));
    }
    if !session.messages.iter().any(|m| m.id == queued_id) {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Queued User admission is still pending; retry execute",
        ));
    }
    let _guard = state.persistence.acquire_lock(id).await;
    let mut latest = state
        .persistence
        .storage()
        .load_session(id)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "Session missing"))?;
    if latest.metadata.get("chat.queued_ingress.v1") == Some(&queued_id) {
        latest.metadata.remove("chat.queued_ingress.v1");
        crate::handlers::agent::events::mark_pending_turn(&mut latest);
        super::persist_and_cache_session_locked(state, &latest)
            .await
            .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    }
    for message in refreshed.committed_messages {
        state.account_sink.record(
            Some(id),
            &bamboo_agent_core::AgentEvent::message_appended(id, &message),
        );
    }
    Ok(())
}
