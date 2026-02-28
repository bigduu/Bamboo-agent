//! Message preflight hooks.
//!
//! These hooks run before we forward requests upstream (proxy endpoints) and before we
//! enter the agent loop. They operate on internal `crate::agent::core::Message` so the
//! same behavior applies across OpenAI-compatible, Anthropic, Gemini, and agent routes.

use crate::agent::core::Message;
use crate::agent::llm::models::ContentPart;
use crate::core::Config;

#[derive(Debug, thiserror::Error)]
pub enum HookError {
    #[error("Invalid hook configuration: {0}")]
    InvalidConfig(String),
    #[error("Request not supported: {0}")]
    Unsupported(String),
}

/// Apply all configured preflight hooks.
pub async fn apply_message_preflight_hooks(
    config: &Config,
    _model: &str,
    messages: &mut Vec<Message>,
) -> Result<(), HookError> {
    apply_image_fallback_hook(config, messages).await
}

async fn apply_image_fallback_hook(
    config: &Config,
    messages: &mut Vec<Message>,
) -> Result<(), HookError> {
    let hook_cfg = &config.hooks.image_fallback;
    if !hook_cfg.enabled {
        return Ok(());
    }

    let mode = hook_cfg.mode.trim().to_ascii_lowercase();
    if mode != "placeholder" && mode != "error" && mode != "ocr" {
        return Err(HookError::InvalidConfig(format!(
            "hooks.image_fallback.mode must be 'placeholder', 'error', or 'ocr' (got '{mode}')"
        )));
    }

    let mut images_seen = 0usize;
    let mut rewritten_messages = 0usize;

    for msg in messages.iter_mut() {
        let Some(parts) = msg.content_parts.as_ref() else {
            continue;
        };

        if parts
            .iter()
            .any(|p| matches!(p, ContentPart::ImageUrl { .. }))
        {
            images_seen += parts
                .iter()
                .filter(|p| matches!(p, ContentPart::ImageUrl { .. }))
                .count();
        }

        match mode.as_str() {
            "error" => {
                // Defer returning until after we count images so we can include a helpful message.
            }
            "placeholder" => {
                let rewritten = rewrite_parts_to_placeholder(parts);
                msg.content = rewritten;
                msg.content_parts = None;
                rewritten_messages += 1;
            }
            "ocr" => {
                // For now:
                // - On Windows we plan to use the built-in OCR API.
                // - On non-Windows we log and fall back to placeholder mode.
                //
                // This keeps behavior predictable on all platforms and prevents leaking
                // base64 data URIs into logs/responses.
                #[cfg(windows)]
                {
                    // TODO: implement Windows OCR using WinRT (Windows.Media.Ocr).
                    log::info!(
                        "OCR hook enabled but Windows OCR is not implemented yet; leaving images intact."
                    );
                }
                #[cfg(not(windows))]
                {
                    log::info!(
                        "OCR hook enabled but OCR is currently Windows-only; leaving images intact."
                    );
                }
            }
            _ => {}
        }
    }

    if images_seen > 0 && mode == "error" {
        return Err(HookError::Unsupported(format!(
            "This server does not currently support image inputs (found {images_seen} image part(s)). Configure hooks.image_fallback.mode='placeholder' or 'ocr' to degrade gracefully."
        )));
    }

    if images_seen > 0 && mode == "placeholder" && rewritten_messages > 0 {
        log::warn!(
            "Image inputs detected ({} part(s)); rewrote {} message(s) to placeholder text. Disable hooks.image_fallback.enabled to preserve images.",
            images_seen,
            rewritten_messages
        );
    }

    Ok(())
}

fn rewrite_parts_to_placeholder(parts: &[ContentPart]) -> String {
    let mut out = String::new();
    for part in parts.iter() {
        match part {
            ContentPart::Text { text } => out.push_str(text),
            ContentPart::ImageUrl { image_url } => {
                let summary = summarize_image_url(&image_url.url);
                out.push_str("\n[Image omitted: ");
                out.push_str(&summary);
                out.push_str("]\n");
            }
        }
    }
    out
}

fn summarize_image_url(url: &str) -> String {
    let trimmed = url.trim();
    if trimmed.starts_with("data:") {
        // data:<mime>;base64,<data...>
        // Keep summary stable and avoid ever echoing base64 content.
        let mut mime = "unknown".to_string();
        if let Some(semi_idx) = trimmed.find(';') {
            let header = &trimmed["data:".len()..semi_idx];
            if !header.trim().is_empty() {
                mime = header.trim().to_string();
            }
        }

        let approx_bytes = trimmed
            .split_once(',')
            .map(|(_, data)| {
                let len = data.trim().len();
                // Base64 is ~4/3 expansion.
                (len.saturating_mul(3)) / 4
            })
            .unwrap_or(0);

        return format!("{mime} (~{approx_bytes} bytes)");
    }

    // For normal URLs, truncate to keep logs/responses compact.
    const MAX: usize = 120;
    if trimmed.len() <= MAX {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..MAX])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::llm::models::{ContentPart, ImageUrl};
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
            ],
        )];

        apply_message_preflight_hooks(&cfg, "m", &mut messages)
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
            }],
        )];

        let err = apply_message_preflight_hooks(&cfg, "m", &mut messages)
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
        let err = apply_message_preflight_hooks(&cfg, "m", &mut messages)
            .await
            .expect_err("should err");
        assert!(matches!(err, HookError::InvalidConfig(_)));
    }
}
