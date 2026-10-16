//! Pre-execution validation helpers.

use bamboo_domain::Session;

/// Validate images after resolving the provider and exact executing model.
/// Explicit support bypasses the independent legacy fallback; missing support
/// retains its opt-in policy, while explicit false fails without dropping images.
pub fn validate_image_fallback_for_session(
    session: &Session,
    image_fallback: Option<&crate::ImageFallbackConfig>,
    model_name: &str,
    vision_override: Option<bool>,
) -> Result<(), String> {
    use crate::ImageFallbackMode;

    if vision_override == Some(true) {
        return Ok(());
    }
    let images_seen = session
        .messages
        .iter()
        .filter_map(|message| message.content_parts.as_ref())
        .flat_map(|parts| parts.iter())
        .filter(|part| matches!(part, bamboo_agent_core::MessagePart::ImageUrl { .. }))
        .count();
    if images_seen > 0 && vision_override == Some(false) {
        return Err(format!("Model '{model_name}' does not support Vision; image history was preserved but cannot be sent. Select a Vision-capable model or enable supports_vision for this model in provider settings."));
    }

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
