//! Authenticated transport admission; client references never select a role.
use crate::error::ResponseResult;

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

pub(super) fn validate_skill_request(request: &ChatRequest) -> ResponseResult<()> {
    if let Some(selection) = &request.workflow_selection {
        bamboo_domain::SessionSkillSelection::validate_borrowed(
            &selection.id, selection.source.as_str(), selection.revision, &selection.args,
        ).map_err(|_| error(StatusCode::BAD_REQUEST,
            "Invalid Skill request: canonical id (1..256 bytes), known source, nonzero revision; args limit 64 container levels, 8192 nodes and 8192 JSON bytes"))?;
    }
    Ok(())
}

pub(super) fn skill_request(
    request: &ChatRequest,
) -> ResponseResult<Option<bamboo_domain::SessionSkillRequest>> {
    validate_skill_request(request)?;
    let Some(selection) = &request.workflow_selection else {
        return Ok(None);
    };
    let data = bamboo_domain::SessionSkillRequest {
        selections: vec![bamboo_domain::SessionSkillSelection {
            id: selection.id.clone(),
            source: selection.source.as_str().to_owned(),
            revision: selection.revision,
            args: selection.args.clone(),
        }],
        mode: None,
    };
    data.validate().map_err(|_| {
        error(
            StatusCode::BAD_REQUEST,
            "Invalid bounded Skill request data",
        )
    })?;
    Ok(Some(data))
}

pub(super) async fn construct_user_envelope(
    state: &AppState,
    session: &Session,
    request: &ChatRequest,
    effective_message: &str,
) -> ResponseResult<SessionMessageEnvelope> {
    let skill_request = skill_request(request)?;
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
            )
            .into());
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
                skill_request: None,
            });
    }
    if let bamboo_domain::SessionMessageBody::Content(content) = &mut envelope.body {
        content.skill_request = skill_request;
    }
    envelope
        .validate()
        .map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    Ok(envelope)
}

pub(super) async fn queue(
    state: &AppState,
    session: &Session,
    request: &ChatRequest,
    effective_message: &str,
    http: &HttpRequest,
) -> ResponseResult<Option<SessionInboxReceipt>> {
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
        )
        .into());
    }
    if ticket && (request.message.is_empty() || request.message.len() > 32768) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "Ticket Human input requires 1..32768 UTF-8 bytes",
        )
        .into());
    }
    let envelope = construct_user_envelope(state, session, request, effective_message).await?;
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
pub(crate) async fn admit_for_execute(
    state: &AppState,
    id: &str,
) -> ResponseResult<Option<bamboo_engine::config::UntrustedExecutionInputs>> {
    let runners = state.agent_runners.read().await;
    if runners.get(id).is_some_and(|r| {
        matches!(
            r.status,
            crate::app_state::AgentStatus::Pending | crate::app_state::AgentStatus::Running
        )
    }) {
        return Ok(None);
    }
    let Some(mut session) = state
        .storage
        .load_session(id)
        .await
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e))?
    else {
        return Ok(None);
    };
    let Some(queued_id) = session.metadata.get("chat.queued_ingress.v1").cloned() else {
        return Ok(None);
    };
    let persistence: std::sync::Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
        state.persistence.clone();
    let (refreshed, inputs) =
        bamboo_engine::config::UntrustedExecutionInputs::admit_with_startup_observation(
            &mut session,
            Some(&state.storage),
            Some(&persistence),
            Some(&state.session_inbox),
        )
        .await;
    // SDK admission may durably commit and ACK one bounded batch while the
    // newest queued input is still pending. Emit every committed message even
    // when this call must return a retryable admission error or tail response.
    for message in &refreshed.committed_messages {
        state.account_sink.record(
            Some(id),
            &bamboo_agent_core::AgentEvent::message_appended(id, message),
        );
    }
    if let Some(reason) = refreshed.admission_error {
        return Err(error(StatusCode::SERVICE_UNAVAILABLE, reason).into());
    }
    if !session.messages.iter().any(|m| m.id == queued_id) {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Queued User admission is still pending; retry execute",
        )
        .into());
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
    // Return data only after the entire checked admission/startup handoff
    // succeeds. A retry/recovered transcript is not a new observation.
    Ok(inputs)
}

#[cfg(test)]
mod skill_request_tests {
    use super::*;

    #[test]
    fn skill_request_maps_each_source_and_preserves_original_scalar_args() {
        for source in ["builtin", "project", "workspace", "user", "plugin"] {
            for args in [
                serde_json::Value::Null,
                serde_json::json!([1, "原样"]),
                serde_json::json!({"key":"value"}),
            ] {
                let request: ChatRequest = serde_json::from_value(serde_json::json!({
                    "message":"ordinary", "workflow_selection":{"id":"exact Case", "source":source, "revision":7, "args":args}
                })).unwrap();
                let data = skill_request(&request).unwrap().unwrap();
                assert_eq!(data.mode, None);
                assert_eq!(data.selections.len(), 1);
                assert_eq!(data.selections[0].id, "exact Case");
                assert_eq!(data.selections[0].source, source);
                assert_eq!(data.selections[0].revision, 7);
                assert_eq!(data.selections[0].args, args);
                data.validate().unwrap();
                assert_eq!(request.workflow_selection.as_ref().unwrap().args, args);
            }
        }
        let request: ChatRequest = serde_json::from_value(serde_json::json!({
            "message":"ordinary", "selected_skill_ids":["historical"], "selected_skill_mode":"plan"
        }))
        .unwrap();
        assert!(
            skill_request(&request).unwrap().is_none(),
            "no historical/config/selected-ID producer"
        );
    }

    #[actix_web::test]
    async fn skill_request_programmatic_depth_rejects_before_attachment_or_session_writes() {
        let root = tempfile::tempdir().unwrap();
        let state = AppState::new(root.path().into()).await.unwrap();
        let session = Session::new("preclone-deep-skill", "test-model");
        let mut request: ChatRequest = serde_json::from_value(serde_json::json!({
            "message":"ordinary", "message_id":"preclone-id", "workflow_selection":{"id":"review", "source":"builtin", "revision":1},
            "images":[{"base64":"invalid%%%","type":"image/png"}]
        })).unwrap();
        for depth in [65, 180] {
            let args = (0..depth).fold(serde_json::Value::Null, |value, _| {
                serde_json::Value::Array(vec![value])
            });
            assert!(serde_json::to_vec(&args).unwrap().len() < 8192);
            request.workflow_selection.as_mut().unwrap().args = args;
            let response = construct_user_envelope(&state, &session, &request, "ordinary")
                .await
                .unwrap_err();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = actix_web::body::to_bytes(response.into_body())
                .await
                .unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(
                body.contains("64 container levels"),
                "shape failed before invalid image: {body}"
            );
            assert!(body.len() < 512, "bounded reason does not echo args");
            assert!(state
                .storage
                .load_session(&session.id)
                .await
                .unwrap()
                .is_none());
            assert!(!root
                .path()
                .join("sessions")
                .join(&session.id)
                .join("attachments")
                .exists());
        }
    }
}
