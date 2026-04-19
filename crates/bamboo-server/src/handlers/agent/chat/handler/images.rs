use actix_web::{web, HttpResponse};

use crate::app_state::AppState;
use bamboo_agent_core::Session;
use bamboo_infrastructure::models::{ContentPart, ImageUrl};

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
                        "error": format!("Failed to store image attachment: {error}")
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

    Ok(())
}
