//! Pre-execution validation helpers.

use bamboo_domain::Session;

/// Validate images after resolving the provider and exact executing model.
/// Explicit support bypasses the independent legacy fallback; missing support
/// retains its opt-in policy. Explicit false uses transient ViewImage references;
/// the HTTP host separately validates the explicit auxiliary target configuration.
pub fn validate_image_fallback_for_session(
    session: &Session,
    image_fallback: Option<&crate::ImageFallbackConfig>,
    _model_name: &str,
    vision_override: Option<bool>,
) -> Result<(), String> {
    use crate::ImageFallbackMode;

    if vision_override.is_some() {
        return Ok(());
    }
    let images_seen = session
        .messages
        .iter()
        .filter_map(|message| message.content_parts.as_ref())
        .flat_map(|parts| parts.iter())
        .filter(|part| matches!(part, bamboo_agent_core::MessagePart::ImageUrl { .. }))
        .count();
    if matches!(
        image_fallback,
        Some(crate::ImageFallbackConfig {
            mode: ImageFallbackMode::Error,
            ..
        })
    ) && images_seen > 0
    {
        return Err(format!(
            "This server does not currently support image inputs (found {images_seen} image part(s)). \
             Configure hooks.image_fallback.mode='placeholder' or 'ocr' to degrade gracefully."
        ));
    }

    Ok(())
}

#[cfg(test)]
mod image_reference_tests {
    use super::*;
    use bamboo_domain::{ImageUrlRef, Message, MessagePart};

    #[test]
    fn explicit_nonvision_new_upload_and_history_followup_pass_without_rewriting_messages() {
        for followup in [false, true] {
            let mut session = Session::new("image-reference-preflight", "text-only");
            session.add_message(Message::user_with_parts(
                "image",
                vec![MessagePart::ImageUrl {
                    image_url: ImageUrlRef {
                        url: "bamboo-attachment://image-reference-preflight/image1".into(),
                        detail: None,
                    },
                }],
            ));
            if followup {
                session.add_message(Message::user("inspect the earlier image"));
            }
            let before = serde_json::to_vec(&session).unwrap();
            validate_image_fallback_for_session(
                &session,
                Some(&crate::ImageFallbackConfig {
                    mode: crate::ImageFallbackMode::Error,
                    vision_model: None,
                }),
                "text-only",
                Some(false),
            )
            .unwrap();
            assert_eq!(serde_json::to_vec(&session).unwrap(), before);
        }
    }
}
