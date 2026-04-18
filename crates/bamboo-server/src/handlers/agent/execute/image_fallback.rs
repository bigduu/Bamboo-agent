use bamboo_application_runtime::{ImageFallbackConfig, ImageFallbackMode};
use bamboo_infrastructure_config::Config;

pub(crate) fn resolve_image_fallback(
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
        "vision" => ImageFallbackMode::Vision,
        other => {
            return Err(format!(
                "Invalid config: hooks.image_fallback.mode must be 'placeholder', 'error', 'ocr', or 'vision' (got '{other}')"
            ));
        }
    };

    let vision_model = if mode == ImageFallbackMode::Vision {
        config_snapshot.get_vision_model()
    } else {
        None
    };

    Ok(Some(ImageFallbackConfig { mode, vision_model }))
}
