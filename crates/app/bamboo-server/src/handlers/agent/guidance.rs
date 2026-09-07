//! Durable text guidance for the next available model turn.
use crate::app_state::AppState;
use crate::error::json_error;
use actix_web::{http::StatusCode, web, HttpResponse};
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
            json_error(StatusCode::NOT_FOUND, "Session not found")
        }
        SessionInboxError::PayloadTooLarge { .. } => {
            json_error(StatusCode::PAYLOAD_TOO_LARGE, error.to_string())
        }
        SessionInboxError::BacklogFull { .. } => {
            json_error(StatusCode::TOO_MANY_REQUESTS, error.to_string())
        }
        SessionInboxError::InvalidClaim(_) => json_error(StatusCode::CONFLICT, error.to_string()),
        other => {
            tracing::error!(%other, "guidance queue failed");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "Guidance storage failed")
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
        return json_error(StatusCode::BAD_REQUEST, "Invalid guidance message id");
    };
    match state.session_inbox.cancel_guidance(&session_id, &id).await {
        Ok(true) => HttpResponse::NoContent().finish(),
        Ok(false) => json_error(StatusCode::CONFLICT, "Guidance is no longer pending"),
        Err(error) => inbox_error(error),
    }
}

pub async fn send(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<GuidanceRequest>,
) -> HttpResponse {
    let Ok(id) = SessionMessageId::parse(body.id.clone()) else {
        return json_error(StatusCode::BAD_REQUEST, "Invalid guidance message id");
    };
    if body.text.trim().is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "Guidance cannot be empty");
    }
    if body.text.len() > 64 * 1024 {
        return json_error(StatusCode::PAYLOAD_TOO_LARGE, "Guidance exceeds 64 KiB");
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
        Err(SessionMessengerError::TargetNotFound(_)) => {
            json_error(StatusCode::NOT_FOUND, "Session not found")
        }
        Err(SessionMessengerError::Inbox(error)) => inbox_error(error),
        Err(SessionMessengerError::InvalidEnvelope(error)) => {
            json_error(StatusCode::BAD_REQUEST, error)
        }
        Err(error) => {
            tracing::error!(%error, "guidance delivery failed");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "Guidance storage failed")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_web::test]
    async fn inbox_errors_use_the_native_envelope_and_status() {
        for (error, expected) in [
            (
                SessionInboxError::TargetNotFound("missing".into()),
                StatusCode::NOT_FOUND,
            ),
            (
                SessionInboxError::PayloadTooLarge {
                    actual: 20,
                    limit: 10,
                },
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
            (
                SessionInboxError::BacklogFull {
                    current: 10,
                    limit: 10,
                },
                StatusCode::TOO_MANY_REQUESTS,
            ),
            (
                SessionInboxError::InvalidClaim("changed".into()),
                StatusCode::CONFLICT,
            ),
            (
                SessionInboxError::Storage("unavailable".into()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ] {
            let response = inbox_error(error);
            assert_eq!(response.status(), expected);
            let bytes = actix_web::body::to_bytes(response.into_body())
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["error"]["type"], "api_error");
            assert!(body["error"]["message"].is_string());
        }
    }
}
