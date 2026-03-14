use crate::agent::core::Session;
use crate::agent::llm::models::ContentPart;
use crate::agent::loop_module::{ImageFallbackConfig, ImageFallbackMode};
use crate::core::Config;

pub(super) fn resolve_image_fallback(
    config_snapshot: &Config,
) -> Result<Option<ImageFallbackConfig>, String> {
    if !config_snapshot.hooks.image_fallback.enabled {
        return Ok(None);
    }

    let mode_str = config_snapshot
        .hooks
        .image_fallback
        .mode
        .trim()
        .to_ascii_lowercase();

    let mode = match mode_str.as_str() {
        "placeholder" => ImageFallbackMode::Placeholder,
        "error" => ImageFallbackMode::Error,
        "ocr" => ImageFallbackMode::Ocr,
        other => {
            return Err(format!(
                "Invalid config: hooks.image_fallback.mode must be 'placeholder', 'error', or 'ocr' (got '{other}')"
            ));
        }
    };

    Ok(Some(ImageFallbackConfig { mode }))
}

pub(super) fn validate_image_fallback_for_session(
    session: &Session,
    image_fallback: Option<&ImageFallbackConfig>,
) -> Result<(), String> {
    // Preserve previous semantics: in "error" mode, reject sessions containing images.
    if matches!(
        image_fallback,
        Some(ImageFallbackConfig {
            mode: ImageFallbackMode::Error
        })
    ) {
        let images_seen = session
            .messages
            .iter()
            .filter_map(|message| message.content_parts.as_ref())
            .flat_map(|parts| parts.iter())
            .filter(|part| matches!(part, ContentPart::ImageUrl { .. }))
            .count();

        if images_seen > 0 {
            return Err(format!(
                "This server does not currently support image inputs (found {images_seen} image part(s)). Configure hooks.image_fallback.mode='placeholder' or 'ocr' to degrade gracefully."
            ));
        }
    }

    Ok(())
}
