use crate::error::ResponseResult;
use actix_web::{web, HttpResponse};

use crate::app_state::AppState;
use bamboo_agent_core::Session;
use bamboo_llm::models::{ContentPart, ImageUrl};

use super::super::ChatImage;

pub(super) async fn append_user_message(
    state: &web::Data<AppState>,
    session: &mut Session,
    message: &str,
    images: Option<&[ChatImage]>,
) -> ResponseResult<()> {
    let user = construct_user_message(state, session, message, images).await?;
    session.add_message(user);

    // Persist a durable handoff marker with the new turn. A reconnect may occur
    // before POST /execute reserves a Pending runner; without this marker, the
    // previous run's Cancelled/Failed runtime snapshot can be mistaken for the
    // terminal state of this new request.
    crate::handlers::agent::events::mark_pending_turn(session);

    Ok(())
}

pub(super) async fn construct_user_message(
    state: &web::Data<AppState>,
    session: &Session,
    message: &str,
    images: Option<&[ChatImage]>,
) -> ResponseResult<bamboo_agent_core::Message> {
    // Preserve multimodal parts so that preflight hooks (OCR/fallback) and/or multimodal
    // upstream models can use the images.
    if let Some(images) = images.filter(|items| !items.is_empty()) {
        let mut parts = Vec::new();
        // Always include a text part to keep downstream behavior stable.
        parts.push(ContentPart::Text {
            text: message.to_string(),
        });

        for image in images {
            let (_, url) = match state
                .session_store
                .write_image_attachment(session, &image.base64, image.mime_type.as_deref())
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    return Err(HttpResponse::BadRequest()
                        .json(serde_json::json!({
                            "error": crate::error::error_value(format!(
                                "Failed to store image attachment: {error}"
                            ))
                        }))
                        .into());
                }
            };
            parts.push(ContentPart::ImageUrl {
                image_url: ImageUrl { url, detail: None },
            });
        }

        Ok(bamboo_agent_core::Message::user_with_parts(
            message.to_string(),
            parts.into_iter().map(Into::into).collect(),
        ))
    } else {
        Ok(bamboo_agent_core::Message::user(message.to_string()))
    }
}

/// Preserve C's real post-hook identity, timestamp and every attachment.
pub(super) async fn construct_native_envelope(
    state: &web::Data<AppState>,
    session: &Session,
    request: &super::ChatRequest,
    message: &str,
) -> ResponseResult<bamboo_domain::SessionMessageEnvelope> {
    let data = super::ingress::skill_request(request)?;
    let user = construct_user_message(state, session, message, request.images.as_deref()).await?;
    let mut envelope =
        bamboo_domain::SessionMessageEnvelope::user_input(&session.id, &user.content);
    envelope.id = bamboo_domain::SessionMessageId::parse(user.id).map_err(|e| {
        crate::error::json_error(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            e.to_string(),
        )
    })?;
    envelope.created_at = user.created_at;
    envelope.body =
        bamboo_domain::SessionMessageBody::Content(bamboo_domain::SessionMessageContent {
            text: user.content,
            parts: user.content_parts.unwrap_or_default(),
            skill_request: data,
        });
    envelope.validate().map_err(|e| {
        crate::error::json_error(actix_web::http::StatusCode::BAD_REQUEST, e.to_string())
    })?;
    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_web::test]
    async fn native_envelope_keeps_seventeen_nondeduplicated_images_and_original_handoff() {
        let home = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(home.path().into()).await.unwrap());
        let mut session = Session::new("native-seventeen", "test-model");
        session.set_last_run_status("error");
        session.set_last_run_error("old owned error");
        let original = serde_json::to_value(&session).unwrap();
        let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jF0cAAAAASUVORK5CYII=";
        let request: super::super::ChatRequest = serde_json::from_value(serde_json::json!({
            "message":"before hook", "images":(0..17).map(|_|serde_json::json!({"base64":png,"type":"image/png"})).collect::<Vec<_>>()
        })).unwrap();
        let envelope = construct_native_envelope(&state, &session, &request, "after hook 原样")
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&session).unwrap(),
            original,
            "construction appends no User or handoff metadata"
        );
        let delivered = envelope.to_provider_message().unwrap();
        assert_eq!(delivered.id, envelope.id.as_str());
        assert_eq!(delivered.created_at, envelope.created_at);
        assert_eq!(delivered.content, "after hook 原样");
        let parts = delivered.content_parts.as_ref().unwrap();
        assert_eq!(parts.len(), 18);
        let urls = parts
            .iter()
            .filter_map(|part| match part {
                bamboo_domain::MessagePart::ImageUrl { image_url } => Some(&image_url.url),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            urls.len(),
            17,
            "identical images retain all original distinct attachments"
        );
        assert!(urls
            .iter()
            .all(|url| url.starts_with("bamboo-attachment://")));
        let mut invalid = request;
        invalid.images.as_mut().unwrap()[8].base64 = "not base64!".into();
        assert!(
            construct_native_envelope(&state, &session, &invalid, "after hook")
                .await
                .is_err()
        );
        assert_eq!(
            serde_json::to_value(&session).unwrap(),
            original,
            "partial attachment failure adds no canonical input"
        );
        invalid.images.as_mut().unwrap()[8].base64 = png.into();
        invalid.session_id = Some(session.id.clone());
        invalid.model = Some("test-model".into());
        let response = super::super::handler(
            state.clone(),
            actix_web::test::TestRequest::post().to_http_request(),
            web::Json(invalid),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::CREATED);
        let receipt: serde_json::Value = serde_json::from_slice(
            &actix_web::body::to_bytes(response.into_body())
                .await
                .unwrap(),
        )
        .unwrap();
        let input = receipt["message_id"].as_str().unwrap();
        assert!(!state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap()
            .messages
            .iter()
            .any(|m| m.id == input));
        let claims = state.session_inbox.claim(&session.id, 128).await.unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].envelope.id.as_str(), input);
        let delivered = claims[0].envelope.to_provider_message().unwrap();
        let parts = delivered.content_parts.as_ref().unwrap();
        assert_eq!(parts.len(), 18);
        assert_eq!(
            parts
                .iter()
                .filter_map(|p| match p {
                    bamboo_domain::MessagePart::ImageUrl { image_url } => Some(&image_url.url),
                    _ => None,
                })
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            17
        );
        let consumed = state.admit_chat_for_execute(&session.id).await.unwrap();
        assert_eq!(consumed.inputs.unwrap().observations()[0].input_id(), input);
        let cold = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cold.messages.iter().filter(|m| m.id == input).count(), 1);
        let canonical = cold.messages.iter().find(|m| m.id == input).unwrap();
        assert!(bamboo_domain::is_matching_session_message(
            canonical,
            &claims[0].envelope
        ));
        assert_eq!(canonical.content_parts.as_ref().unwrap().len(), 18);
        assert!(state
            .session_inbox
            .was_admitted(&session.id, &claims[0].envelope.id)
            .await
            .unwrap());
        assert_eq!(
            state
                .session_inbox
                .inspect(&session.id)
                .await
                .unwrap()
                .pending,
            0
        );
    }

    #[actix_web::test]
    async fn new_user_turn_replaces_stale_terminal_metadata_with_pending() {
        let dir = tempfile::tempdir().expect("temporary app data");
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let mut session = Session::new("new-turn", "test-model");
        session.set_last_run_status("error");
        session.set_last_run_error("old failure");

        assert!(append_user_message(&state, &mut session, "try again", None)
            .await
            .is_ok());
        assert_eq!(session.last_run_status().as_deref(), Some("pending"));
        assert!(session.last_run_error().is_none());
    }

    #[actix_web::test]
    async fn constructor_parity_native_preserves_user_parts_and_pending_boundary() {
        let root = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(root.path().into()).await.unwrap());
        let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jF0cAAAAASUVORK5CYII=";
        for count in [None, Some(0), Some(2), Some(17)] {
            let mut session = Session::new(format!("constructor-native-{count:?}"), "test-model");
            session.set_last_run_status("error");
            session.set_last_run_error("original failure");
            let images = count.map(|n| {
                (0..n)
                    .map(|_| ChatImage {
                        base64: png.into(),
                        name: None,
                        size: None,
                        mime_type: Some("image/png".into()),
                    })
                    .collect::<Vec<_>>()
            });
            let before = std::time::SystemTime::now();
            append_user_message(
                &state,
                &mut session,
                "原样 \"text\"\nnext",
                images.as_deref(),
            )
            .await
            .unwrap();
            assert_eq!(session.messages.len(), 1);
            let user = &session.messages[0];
            assert_eq!(user.role, bamboo_agent_core::Role::User);
            assert_eq!(user.content, "原样 \"text\"\nnext");
            assert!(!user.id.is_empty());
            let minted: std::time::SystemTime = user.created_at.into();
            assert!(minted >= before && minted <= std::time::SystemTime::now());
            if let Some(count) = count.filter(|count| *count > 0) {
                let parts = user.content_parts.as_ref().unwrap();
                assert_eq!(parts.len(), count + 1);
                assert!(
                    matches!(&parts[0], bamboo_domain::MessagePart::Text {text} if text == &user.content)
                );
                let urls = parts[1..]
                    .iter()
                    .map(|p| match p {
                        bamboo_domain::MessagePart::ImageUrl { image_url } => image_url.url.clone(),
                        _ => panic!("actual attachment"),
                    })
                    .collect::<Vec<_>>();
                assert_ne!(
                    urls[0], urls[1],
                    "native attachment storage remains nondeduplicated"
                );
                assert_eq!(
                    urls.iter().collect::<std::collections::BTreeSet<_>>().len(),
                    count
                );
            } else {
                assert!(user.content_parts.is_none());
            }
            assert_eq!(session.last_run_status().as_deref(), Some("pending"));
            assert!(session.last_run_error().is_none());
        }
    }

    #[actix_web::test]
    async fn constructor_parity_native_storage_error_keeps_original_session() {
        let root = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(root.path().into()).await.unwrap());
        let mut session = Session::new("constructor-native-rejected", "test-model");
        session.add_message(bamboo_agent_core::Message::user("existing User"));
        session.set_last_run_status("error");
        session.set_last_run_error("original failure");
        let original = serde_json::to_value(&session).unwrap();
        let images = [ChatImage {
            base64: "invalid%%%".into(),
            name: None,
            size: None,
            mime_type: Some("image/png".into()),
        }];
        let error = append_user_message(&state, &mut session, "must not append", Some(&images))
            .await
            .unwrap_err();
        assert_eq!(error.status(), actix_web::http::StatusCode::BAD_REQUEST);
        assert_eq!(serde_json::to_value(&session).unwrap(), original);
    }
    #[actix_web::test]
    async fn native_real_factory_full_utf8_envelope_exact_limit_and_no_truncation() {
        use bamboo_domain::{SessionMessageBody, SessionMessageId};
        let limit = bamboo_domain::SessionInboxLimits::default().max_payload_bytes;
        for delta in [-1_i64, 0, 1] {
            let home = tempfile::tempdir().unwrap();
            let state = web::Data::new(AppState::new(home.path().into()).await.unwrap());
            let id = format!("native-byte-boundary-{delta}");
            let mut session = Session::new(&id, "test-model");
            session.set_last_run_status("error");
            session.set_last_run_error("original failure");
            state.storage.save_session(&session).await.unwrap();
            let mut request =
                serde_json::from_value::<super::super::ChatRequest>(serde_json::json!({
                    "session_id":id,"message":"界".repeat(limit / 3 - 1024),"model":"test-model"
                }))
                .unwrap();
            let target = (limit as i64 + delta) as usize;
            // Each attempt uses C's real ID/time and the complete current text.
            // Adjust ASCII padding before reconstruction; never alter the selected C output.
            let mut selected = None;
            for _ in 0..16 {
                let envelope =
                    construct_native_envelope(&state, &session, &request, &request.message)
                        .await
                        .unwrap();
                let actual = serde_json::to_vec(&envelope).unwrap().len();
                if actual == target {
                    selected = Some(envelope);
                    break;
                }
                if actual < target {
                    request.message.push_str(&"x".repeat(target - actual));
                } else {
                    let keep = request.message.len() - (actual - target);
                    assert!(request.message.is_char_boundary(keep));
                    request.message.truncate(keep);
                }
            }
            let envelope =
                selected.expect("real C timestamp precision converges within bounded attempts");
            assert_eq!(serde_json::to_vec(&envelope).unwrap().len(), target);
            let input = envelope.id.clone();
            let host_guard = state.persistence.acquire_lock(&id).await;
            let result = super::super::ingress::commit_native_input(
                state.clone(),
                session,
                None,
                false,
                envelope.clone(),
                host_guard,
                None,
            )
            .await;
            if delta > 0 {
                assert_eq!(
                    result.unwrap_err().status(),
                    actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
                );
                assert!(state
                    .storage
                    .load_session(&id)
                    .await
                    .unwrap()
                    .unwrap()
                    .messages
                    .is_empty());
                assert_eq!(state.session_inbox.inspect(&id).await.unwrap().pending, 0);
                assert!(!state.session_inbox.was_admitted(&id, &input).await.unwrap());
                continue;
            }
            let receipt = result.unwrap();
            assert_eq!(receipt.id, input);
            let claims = state.session_inbox.claim(&id, 128).await.unwrap();
            assert_eq!(claims.len(), 1);
            assert_eq!(
                serde_json::to_value(&claims[0].envelope).unwrap(),
                serde_json::to_value(&envelope).unwrap()
            );
            let SessionMessageBody::Content(content) = &claims[0].envelope.body else {
                panic!("Native content")
            };
            assert_eq!(content.text, request.message);
            assert!(content.text.contains('界'));
            let mut feed = state.account_sink.subscribe();
            let checked = state.admit_chat_for_execute(&id).await.unwrap();
            assert!(
                checked.generate_title,
                "actual New User is required for this effect"
            );
            drop(checked.inputs); // Optional bounded Q mirror is not the canonical admission proof.
            let cold = state.storage.load_session(&id).await.unwrap().unwrap();
            assert_eq!(cold.messages.len(), 1);
            assert_eq!(cold.messages[0].content, request.message);
            assert!(bamboo_domain::is_matching_session_message(
                &cold.messages[0],
                &envelope
            ));
            assert!(state
                .session_inbox
                .was_admitted(&id, &SessionMessageId::parse(input.to_string()).unwrap())
                .await
                .unwrap());
            assert_eq!(state.session_inbox.inspect(&id).await.unwrap().pending, 0);
            let event = tokio::time::timeout(std::time::Duration::from_secs(2), feed.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(&event.event, bamboo_agent_core::AgentEvent::MessageAppended {message_id,..} if message_id == input.as_str())
            );
            let no_new = state.admit_chat_for_execute(&id).await.unwrap();
            assert!(no_new.inputs.is_none());
            assert!(!no_new.generate_title);
            assert!(feed.try_recv().is_err());
        }
    }
}
