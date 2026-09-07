//! Durable multimodal guidance for a model boundary or successor run.
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
    #[serde(default)]
    pub mode: GuidanceMode,
    #[serde(default)]
    pub images: Vec<super::chat::ChatImage>,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuidanceMode {
    #[default]
    AfterRound,
    AfterRun,
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
            Some(json!({"id": message.id, "text": content.text, "created_at": message.created_at, "images": content.parts.iter().filter_map(|part| {
                let bamboo_domain::MessagePart::ImageUrl { image_url } = part else { return None };
                image_url.url.strip_prefix(&format!("bamboo-attachment://{}/", message.target_session_id)).map(str::to_string)
            }).collect::<Vec<_>>(), "mode": if message.correlation_id.as_deref().is_some_and(|value| value.starts_with("session-guidance-after-run")) { "after_run" } else { "after_round" }}))
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
    if body.text.trim().is_empty() && body.images.is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "A queued message needs text or an image",
        );
    }
    if body.text.len() > 64 * 1024 {
        return json_error(StatusCode::PAYLOAD_TOO_LARGE, "Guidance exceeds 64 KiB");
    }
    let mut envelope = SessionMessageEnvelope::user_input(path.into_inner(), body.text.clone());
    if body.images.len() > 16 {
        return json_error(
            StatusCode::BAD_REQUEST,
            "A queued message supports up to 16 images",
        );
    }
    if !body.images.is_empty() {
        let Some(session) = state.load_session_merged(&envelope.target_session_id).await else {
            return json_error(StatusCode::NOT_FOUND, "Session not found");
        };
        let mut parts = vec![bamboo_domain::MessagePart::Text {
            text: body.text.clone(),
        }];
        for image in &body.images {
            let (_, url) = match state
                .session_store
                .write_image_attachment_deduplicated(
                    &session,
                    &image.base64,
                    image.mime_type.as_deref(),
                )
                .await
            {
                Ok(stored) => stored,
                Err(error) => {
                    return json_error(
                        StatusCode::BAD_REQUEST,
                        format!("Failed to store image attachment: {error}"),
                    )
                }
            };
            parts.push(bamboo_domain::MessagePart::ImageUrl {
                image_url: bamboo_domain::ImageUrlRef { url, detail: None },
            });
        }
        envelope.body =
            bamboo_domain::SessionMessageBody::Content(bamboo_domain::SessionMessageContent {
                text: body.text.clone(),
                parts,
            });
    }
    envelope.id = id;
    envelope.correlation_id = Some(match body.mode {
        GuidanceMode::AfterRound => "session-guidance".into(),
        GuidanceMode::AfterRun => match state
            .session_activation_router
            .current_run_id(&envelope.target_session_id)
            .await
        {
            Some(run_id) => format!("session-guidance-after-run:{run_id}"),
            None => "session-guidance-after-run".into(),
        },
    });
    let result = async {
        let admission = state.session_messenger.admit(envelope).await?;
        let policy = match body.mode {
            GuidanceMode::AfterRound => {
                bamboo_domain::SessionActivationPolicy::InterruptSpecificWait
            }
            GuidanceMode::AfterRun => bamboo_domain::SessionActivationPolicy::RespectSpecificWait,
        };
        state
            .session_messenger
            .activate_with_policy(&admission, policy)
            .await
    }
    .await;
    match result {
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
    async fn queued_images_use_durable_references_and_retries_do_not_duplicate_attachments() {
        use bamboo_domain::{Session, SessionInboxLimits, Storage};
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        state
            .session_store
            .save_session(&Session::new("queued-images", "model"))
            .await
            .unwrap();
        let registration = state
            .session_activation_router
            .register_run("queued-images", "run-a")
            .await
            .unwrap();
        let request = || {
            web::Json(GuidanceRequest {
                id: "image-guidance".into(),
                text: String::new(),
                mode: GuidanceMode::AfterRun,
                images: vec![super::super::chat::ChatImage {
                    base64: "data:image/png;base64,aGVsbG8=".into(),
                    name: Some("test.png".into()),
                    size: Some(5),
                    mime_type: Some("image/png".into()),
                }],
            })
        };
        for _ in 0..2 {
            assert_eq!(
                send(
                    state.clone(),
                    web::Path::from("queued-images".to_string()),
                    request()
                )
                .await
                .status(),
                StatusCode::ACCEPTED
            );
        }
        let pending = state
            .session_inbox
            .pending_guidance("queued-images")
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        let bamboo_domain::SessionMessageBody::Content(content) = &pending[0].body else {
            panic!("content")
        };
        assert!(content.text.is_empty());
        let bamboo_domain::MessagePart::ImageUrl { image_url } = &content.parts[1] else {
            panic!("image")
        };
        assert!(image_url
            .url
            .starts_with("bamboo-attachment://queued-images/"));
        assert!(!serde_json::to_string(&pending[0])
            .unwrap()
            .contains("aGVsbG8="));
        let attachment_id = image_url.url.rsplit('/').next().unwrap();
        assert_eq!(
            state
                .session_store
                .read_attachment("queued-images", attachment_id)
                .await
                .unwrap()
                .unwrap()
                .0,
            b"hello"
        );
        let attachment_dir = dir.path().join("sessions/queued-images/attachments");
        assert_eq!(std::fs::read_dir(&attachment_dir).unwrap().count(), 1);
        assert!(state
            .session_inbox
            .claim_for_turn("queued-images", 128, Some("run-a"))
            .await
            .unwrap()
            .is_empty());
        let list_response = list(state.clone(), web::Path::from("queued-images".to_string())).await;
        let list_bytes = actix_web::body::to_bytes(list_response.into_body())
            .await
            .unwrap();
        let listed: serde_json::Value = serde_json::from_slice(&list_bytes).unwrap();
        assert_eq!(listed["messages"][0]["images"][0], attachment_id);
        let reopened = bamboo_storage::FileSessionInbox::new(
            state.session_store.clone(),
            SessionInboxLimits::default(),
        );
        use bamboo_domain::SessionInboxPort;
        let claims = reopened
            .claim_for_turn("queued-images", 128, Some("run-b"))
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        let message = claims[0].envelope.to_provider_message().unwrap();
        assert_eq!(message.content_parts.as_ref(), Some(&content.parts));
        // The test owns this lease but does not run the provider. Keep cleanup
        // from dispatching a real successor after checking the claimed payload.
        reopened.ack("queued-images", &claims[0]).await.unwrap();
        registration.finish(1).await.unwrap();
    }

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
