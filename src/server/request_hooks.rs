//! Request preflight hooks.
//!
//! These hooks run before we convert/forward OpenAI-compatible requests into Bamboo's
//! internal message format. They allow us to inspect and rewrite requests (e.g. image
//! fallback handling) in a single, easy-to-extend place.

use crate::agent::llm::api::models::{ChatCompletionRequest, ChatMessage, Content, ContentPart};
use crate::core::Config;

#[derive(Debug, thiserror::Error)]
pub enum HookError {
    #[error("Invalid hook configuration: {0}")]
    InvalidConfig(String),
    #[error("Request not supported: {0}")]
    Unsupported(String),
}

pub fn apply_openai_preflight_hooks(
    config: &Config,
    _model: &str,
    request: &mut ChatCompletionRequest,
) -> Result<(), HookError> {
    apply_image_fallback_hook(config, &mut request.messages)
}

pub fn apply_openai_preflight_hooks_to_messages(
    config: &Config,
    messages: &mut Vec<ChatMessage>,
) -> Result<(), HookError> {
    apply_image_fallback_hook(config, messages)
}

fn apply_image_fallback_hook(config: &Config, messages: &mut Vec<ChatMessage>) -> Result<(), HookError> {
    let hook_cfg = &config.hooks.image_fallback;
    if !hook_cfg.enabled {
        return Ok(());
    }

    let mode = hook_cfg.mode.trim().to_ascii_lowercase();
    if mode != "placeholder" && mode != "error" {
        return Err(HookError::InvalidConfig(format!(
            "hooks.image_fallback.mode must be 'placeholder' or 'error' (got '{mode}')"
        )));
    }

    let mut images_seen = 0usize;

    for msg in messages.iter_mut() {
        let Content::Parts(parts) = &msg.content else {
            continue;
        };

        let mut out_text = String::new();
        for part in parts.iter() {
            match part {
                ContentPart::Text { text } => out_text.push_str(text),
                ContentPart::ImageUrl { image_url } => {
                    images_seen += 1;
                    if mode == "error" {
                        // Defer returning until after we count all images, so we can include a helpful message.
                        continue;
                    }

                    let summary = summarize_image_url(&image_url.url);
                    out_text.push_str("\n[Image omitted: ");
                    out_text.push_str(&summary);
                    out_text.push_str("]\n");
                }
            }
        }

        if mode == "placeholder" {
            msg.content = Content::Text(out_text);
        }
    }

    if images_seen > 0 && mode == "error" {
        return Err(HookError::Unsupported(format!(
            "This server does not currently support image inputs (found {images_seen} image part(s)). Configure hooks.image_fallback.mode='placeholder' to degrade gracefully."
        )));
    }

    Ok(())
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
    use crate::agent::llm::api::models::{ImageUrl, Role};
    use tempfile::TempDir;

    fn base_config(mode: &str) -> Config {
        let dir = TempDir::new().expect("tempdir");
        let mut cfg = Config::from_data_dir(Some(dir.path().to_path_buf()));
        cfg.hooks.image_fallback.enabled = true;
        cfg.hooks.image_fallback.mode = mode.to_string();
        cfg
    }

    #[test]
    fn image_fallback_placeholder_rewrites_images_to_text_without_leaking_data() {
        let cfg = base_config("placeholder");

        let mut messages = vec![ChatMessage {
            role: Role::User,
            content: Content::Parts(vec![
                ContentPart::Text {
                    text: "What is in this image?".to_string(),
                },
                ContentPart::ImageUrl {
                    image_url: ImageUrl {
                        url: "data:image/png;base64,AAAABBBBCCCC".to_string(),
                        detail: None,
                    },
                },
            ]),
            tool_calls: None,
            tool_call_id: None,
        }];

        apply_openai_preflight_hooks_to_messages(&cfg, &mut messages).expect("hook ok");

        match &messages[0].content {
            Content::Text(text) => {
                assert!(text.contains("Image omitted: image/png"), "summary present");
                assert!(!text.contains("AAAABBBBCCCC"), "base64 not leaked");
            }
            _ => panic!("expected Content::Text after rewrite"),
        }
    }

    #[test]
    fn image_fallback_error_rejects_requests_with_images() {
        let cfg = base_config("error");

        let mut messages = vec![ChatMessage {
            role: Role::User,
            content: Content::Parts(vec![ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "https://example.com/image.png".to_string(),
                    detail: None,
                },
            }]),
            tool_calls: None,
            tool_call_id: None,
        }];

        let err =
            apply_openai_preflight_hooks_to_messages(&cfg, &mut messages).expect_err("should err");
        assert!(err.to_string().contains("does not currently support image inputs"));
    }

    #[test]
    fn image_fallback_invalid_mode_errors() {
        let cfg = base_config("wat");
        let mut messages = Vec::new();
        let err =
            apply_openai_preflight_hooks_to_messages(&cfg, &mut messages).expect_err("should err");
        assert!(matches!(err, HookError::InvalidConfig(_)));
    }
}
