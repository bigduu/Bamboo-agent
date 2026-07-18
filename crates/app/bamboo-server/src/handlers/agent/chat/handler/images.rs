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
) -> Result<(), HttpResponse> {
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
                    return Err(HttpResponse::BadRequest().json(serde_json::json!({
                        "error": crate::error::error_value(format!(
                            "Failed to store image attachment: {error}"
                        ))
                    })));
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
    session.set_last_run_status("pending");
    session.clear_last_run_error();

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
}
