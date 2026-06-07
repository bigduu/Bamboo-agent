//! Message preflight hooks.
//!
//! These hooks run before we forward requests upstream (proxy endpoints) and before we
//! enter the agent loop. They operate on internal `bamboo_agent_core::Message` so the
//! same behavior applies across OpenAI-compatible, Anthropic, Gemini, and agent routes.

use bamboo_agent_core::Message;
use bamboo_llm::Config;

#[derive(Debug, thiserror::Error)]
pub enum HookError {
    #[error("Invalid hook configuration: {0}")]
    InvalidConfig(String),
    #[error("Request not supported: {0}")]
    Unsupported(String),
}

/// Apply all configured preflight hooks.
pub async fn apply_message_preflight_hooks(
    attachment_reader: Option<&dyn bamboo_agent_core::storage::AttachmentReader>,
    config: &Config,
    _model: &str,
    messages: &mut [Message],
) -> Result<(), HookError> {
    apply_image_fallback_hook(attachment_reader, config, messages).await
}

async fn apply_image_fallback_hook(
    attachment_reader: Option<&dyn bamboo_agent_core::storage::AttachmentReader>,
    config: &Config,
    messages: &mut [Message],
) -> Result<(), HookError> {
    let hook_cfg = &config.hooks.image_fallback;
    if !hook_cfg.enabled {
        return Ok(());
    }

    let mode = hook_cfg.mode.trim().to_ascii_lowercase();
    let fallback_mode = match mode.as_str() {
        "placeholder" => crate::ImageFallbackMode::Placeholder,
        "error" => crate::ImageFallbackMode::Error,
        "ocr" => crate::ImageFallbackMode::Ocr,
        _ => {
            return Err(HookError::InvalidConfig(format!(
                "hooks.image_fallback.mode must be 'placeholder', 'error', or 'ocr' (got '{mode}')"
            )));
        }
    };

    let fallback = crate::ImageFallbackConfig {
        mode: fallback_mode,
        vision_model: None,
    };

    crate::runtime::runner::image_fallback::apply_image_fallback_to_llm_messages(
        messages,
        fallback,
        attachment_reader,
        None,
    )
    .await
    .map_err(|e| HookError::Unsupported(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_llm::models::{ContentPart, ImageUrl};
    use tempfile::TempDir;

    fn base_config(mode: &str) -> Config {
        let dir = TempDir::new().expect("tempdir");
        let mut cfg = Config::from_data_dir(Some(dir.path().to_path_buf()));
        cfg.hooks.image_fallback.enabled = true;
        cfg.hooks.image_fallback.mode = mode.to_string();
        cfg
    }

    #[tokio::test]
    async fn image_fallback_placeholder_rewrites_images_to_text_without_leaking_data() {
        let cfg = base_config("placeholder");

        let mut messages = vec![Message::user_with_parts(
            "What is in this image?",
            vec![
                ContentPart::Text {
                    text: "What is in this image?".to_string(),
                },
                ContentPart::ImageUrl {
                    image_url: ImageUrl {
                        url: "data:image/png;base64,AAAABBBBCCCC".to_string(),
                        detail: None,
                    },
                },
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
        )];

        apply_message_preflight_hooks(None, &cfg, "m", &mut messages)
            .await
            .expect("hook ok");

        assert!(messages[0].content.contains("Image omitted: image/png"));
        assert!(!messages[0].content.contains("AAAABBBBCCCC"));
        assert!(messages[0].content_parts.is_none());
    }

    #[tokio::test]
    async fn image_fallback_error_rejects_requests_with_images() {
        let cfg = base_config("error");

        let mut messages = vec![Message::user_with_parts(
            "",
            vec![ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "https://example.com/image.png".to_string(),
                    detail: None,
                },
            }]
            .into_iter()
            .map(Into::into)
            .collect(),
        )];

        let err = apply_message_preflight_hooks(None, &cfg, "m", &mut messages)
            .await
            .expect_err("should err");
        assert!(err
            .to_string()
            .contains("does not currently support image inputs"));
    }

    #[tokio::test]
    async fn image_fallback_invalid_mode_errors() {
        let cfg = base_config("wat");
        let mut messages = Vec::new();
        let err = apply_message_preflight_hooks(None, &cfg, "m", &mut messages)
            .await
            .expect_err("should err");
        assert!(matches!(err, HookError::InvalidConfig(_)));
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn image_fallback_ocr_non_windows_leaves_images_intact() {
        let cfg = base_config("ocr");

        let mut messages = vec![Message::user_with_parts(
            "hi",
            vec![
                ContentPart::Text {
                    text: "hi".to_string(),
                },
                ContentPart::ImageUrl {
                    image_url: ImageUrl {
                        url: "data:image/png;base64,AAAABBBBCCCC".to_string(),
                        detail: None,
                    },
                },
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
        )];

        apply_message_preflight_hooks(None, &cfg, "m", &mut messages)
            .await
            .expect("hook ok");

        assert!(messages[0].content_parts.is_some());
        assert!(messages[0].content.contains("hi"));
    }
}
