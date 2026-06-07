//! Pre-execution validation helpers.

use bamboo_domain::Session;

pub(crate) fn validate_image_fallback_for_session(
    session: &Session,
    image_fallback: Option<&crate::ImageFallbackConfig>,
) -> Result<(), String> {
    use crate::ImageFallbackMode;

    if matches!(
        image_fallback,
        Some(crate::ImageFallbackConfig {
            mode: ImageFallbackMode::Error,
            ..
        })
    ) {
        let images_seen = session
            .messages
            .iter()
            .filter_map(|message| message.content_parts.as_ref())
            .flat_map(|parts| parts.iter())
            .filter(|part| matches!(part, bamboo_agent_core::MessagePart::ImageUrl { .. }))
            .count();

        if images_seen > 0 {
            return Err(format!(
                "This server does not currently support image inputs (found {images_seen} image part(s)). \
                 Configure hooks.image_fallback.mode='placeholder' or 'ocr' to degrade gracefully."
            ));
        }
    }

    Ok(())
}
