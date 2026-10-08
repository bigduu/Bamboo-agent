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

        session.add_message(bamboo_agent_core::Message::user_with_parts(
            message.to_string(),
            parts.into_iter().map(Into::into).collect(),
        ));
    } else {
        session.add_message(bamboo_agent_core::Message::user(message.to_string()));
    }

    // Persist a durable handoff marker with the new turn. A reconnect may occur
    // before POST /execute reserves a Pending runner; without this marker, the
    // previous run's Cancelled/Failed runtime snapshot can be mistaken for the
    // terminal state of this new request.
    crate::handlers::agent::events::mark_pending_turn(session);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        for count in [None, Some(0), Some(2)] {
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
            if count == Some(2) {
                let parts = user.content_parts.as_ref().unwrap();
                assert_eq!(parts.len(), 3);
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
}
