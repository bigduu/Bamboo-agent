//! Durable text guidance for the next available model turn.
use crate::app_state::AppState;
use actix_web::{web, HttpResponse};
use bamboo_domain::{SessionInboxError, SessionMessageEnvelope, SessionMessageId};
use bamboo_engine::session_messaging::SessionMessengerError;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
pub struct GuidanceRequest {
    pub id: String,
    pub text: String,
}

fn inbox_error(error: SessionInboxError) -> HttpResponse {
    match error {
        SessionInboxError::TargetNotFound(_) => {
            HttpResponse::NotFound().json(json!({"error": "Session not found"}))
        }
        SessionInboxError::PayloadTooLarge { .. } => {
            HttpResponse::PayloadTooLarge().json(json!({"error": error.to_string()}))
        }
        SessionInboxError::BacklogFull { .. } => {
            HttpResponse::TooManyRequests().json(json!({"error": error.to_string()}))
        }
        SessionInboxError::InvalidClaim(_) => {
            HttpResponse::Conflict().json(json!({"error": error.to_string()}))
        }
        other => {
            tracing::error!(%other, "guidance queue failed");
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn list(state: web::Data<AppState>, path: web::Path<String>) -> HttpResponse {
    match state.session_inbox.pending_guidance(&path).await {
        Ok(messages) => HttpResponse::Ok().json(json!({"messages": messages.into_iter().filter_map(|message| {
            let bamboo_domain::SessionMessageBody::Content(content) = message.body else { return None };
            Some(json!({"id": message.id, "text": content.text, "created_at": message.created_at}))
        }).collect::<Vec<_>>()})),
        Err(error) => inbox_error(error),
    }
}

pub async fn cancel(state: web::Data<AppState>, path: web::Path<(String, String)>) -> HttpResponse {
    let (session_id, message_id) = path.into_inner();
    let Ok(id) = SessionMessageId::parse(message_id) else {
        return HttpResponse::BadRequest().finish();
    };
    match state.session_inbox.cancel_guidance(&session_id, &id).await {
        Ok(true) => HttpResponse::NoContent().finish(),
        Ok(false) => {
            HttpResponse::Conflict().json(json!({"error": "Guidance is no longer pending"}))
        }
        Err(error) => inbox_error(error),
    }
}

pub async fn send(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<GuidanceRequest>,
) -> HttpResponse {
    let Ok(id) = SessionMessageId::parse(body.id.clone()) else {
        return HttpResponse::BadRequest().finish();
    };
    if body.text.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({"error": "Guidance cannot be empty"}));
    }
    if body.text.len() > 64 * 1024 {
        return HttpResponse::PayloadTooLarge().finish();
    }
    let mut envelope = SessionMessageEnvelope::user_input(path.into_inner(), body.text.clone());
    envelope.id = id;
    envelope.correlation_id = Some("session-guidance".into());
    match state.session_messenger.send(envelope).await {
        Ok(receipt) => HttpResponse::Accepted()
            .json(json!({"id": receipt.delivery.id, "activation_pending": false})),
        Err(SessionMessengerError::Activation { receipt, .. }) => {
            HttpResponse::Accepted().json(json!({"id": receipt.id, "activation_pending": true}))
        }
        Err(SessionMessengerError::TargetNotFound(_)) => HttpResponse::NotFound().finish(),
        Err(SessionMessengerError::Inbox(error)) => inbox_error(error),
        Err(SessionMessengerError::InvalidEnvelope(error)) => {
            HttpResponse::BadRequest().json(json!({"error": error}))
        }
        Err(error) => {
            tracing::error!(%error, "guidance delivery failed");
            HttpResponse::InternalServerError().finish()
        }
    }
}
