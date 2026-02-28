//! Message preflight hooks.
//!
//! These hooks run before we forward requests upstream (proxy endpoints) and before we
//! enter the agent loop. They operate on internal `crate::agent::core::Message` so the
//! same behavior applies across OpenAI-compatible, Anthropic, Gemini, and agent routes.

use crate::agent::core::Message;
use crate::agent::llm::models::ContentPart;
use crate::core::Config;

#[cfg(windows)]
use base64::Engine;

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

        let image_parts = parts
            .iter()
            .filter(|p| matches!(p, ContentPart::ImageUrl { .. }))
            .count();
        if image_parts > 0 {
            images_seen += image_parts;
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
                if image_parts == 0 {
                    continue;
                }

                // Windows: run OCR and rewrite image parts into text (with bounds).
                // Non-Windows: log only (leave images intact).
                #[cfg(windows)]
                {
                    let rewritten = rewrite_parts_to_ocr_text(parts).await;
                    msg.content = rewritten;
                    msg.content_parts = None;
                    rewritten_messages += 1;
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

#[cfg(windows)]
async fn rewrite_parts_to_ocr_text(parts: &[ContentPart]) -> String {
    let mut out = String::new();
    let mut image_index = 0usize;

    for part in parts.iter() {
        match part {
            ContentPart::Text { text } => out.push_str(text),
            ContentPart::ImageUrl { image_url } => {
                image_index += 1;
                let summary = summarize_image_url(&image_url.url);

                match ocr_image_url_to_lines(&image_url.url).await {
                    Ok(lines) if !lines.is_empty() => {
                        out.push_str("\n\n[OCR extracted from image ");
                        out.push_str(&image_index.to_string());
                        out.push_str(": ");
                        out.push_str(&summary);
                        out.push_str("]\n");
                        for l in lines {
                            // Format: x,y,w,h are in pixels, relative to the image.
                            out.push_str(&format!(
                                "({},{},{},{}) {}\n",
                                l.left, l.top, l.width, l.height, l.text
                            ));
                        }
                    }
                    Ok(_) => {
                        out.push_str("\n\n[OCR extracted from image ");
                        out.push_str(&image_index.to_string());
                        out.push_str(": ");
                        out.push_str(&summary);
                        out.push_str("]\n(no text detected)\n");
                    }
                    Err(err) => {
                        log::warn!(
                            "OCR failed for image {} ({}): {}",
                            image_index,
                            summary,
                            err
                        );
                        out.push_str("\n[Image omitted: ");
                        out.push_str(&summary);
                        out.push_str("]\n");
                    }
                }
            }
        }
    }

    out
}

#[cfg(windows)]
#[derive(Debug, Clone)]
struct OcrLine {
    text: String,
    left: i32,
    top: i32,
    width: i32,
    height: i32,
}

#[cfg(windows)]
async fn ocr_image_url_to_lines(url: &str) -> anyhow::Result<Vec<OcrLine>> {
    let (mime, data) = match parse_data_url_base64(url) {
        Some(v) => v,
        None => anyhow::bail!("only data: URLs are supported (got non-data URL)"),
    };

    if mime != "image/png" {
        anyhow::bail!("unsupported mime type '{mime}' (only image/png is supported)");
    }

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data.as_bytes())
        .map_err(|e| anyhow::anyhow!("invalid base64 data: {e}"))?;

    // Basic validation to avoid passing junk into the decoder.
    const PNG_SIG: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];
    if bytes.len() < PNG_SIG.len() || bytes[..PNG_SIG.len()] != PNG_SIG {
        anyhow::bail!("decoded data is not a PNG");
    }

    // rust_ocr currently expects a PNG file path (it uses BitmapDecoder::PngDecoderId()).
    let tmp_path = std::env::temp_dir().join(format!("bamboo_ocr_{}.png", uuid::Uuid::new_v4()));
    std::fs::write(&tmp_path, &bytes)?;

    // WinRT OCR can block; keep it off the async executor.
    let tmp_path2 = tmp_path.clone();
    let coords = tokio::task::spawn_blocking(move || rust_ocr::ocr_with_bounds(&tmp_path2))
        .await
        .map_err(|e| anyhow::anyhow!("ocr task join failed: {e}"))?
        // rust_ocr uses `Box<dyn Error>` which is not guaranteed to be `Send + Sync`,
        // so we stringify it instead of relying on `anyhow`'s `From` conversion.
        .map_err(|e| anyhow::anyhow!("ocr failed: {e}"))?;

    let _ = std::fs::remove_file(&tmp_path);

    Ok(extract_line_candidates(coords))
}

#[cfg(windows)]
fn extract_line_candidates(coords: Vec<rust_ocr::Coordinates>) -> Vec<OcrLine> {
    // `rust_ocr::ocr_with_bounds` yields word-level coordinates and then a line-level
    // coordinate for each OCR line. We pick the line-level entries by matching them
    // against the accumulated words for that line.
    let mut out = Vec::new();
    let mut current_words: Vec<String> = Vec::new();

    for c in coords.into_iter() {
        let text = c.text.trim().to_string();
        if text.is_empty() {
            continue;
        }

        if !current_words.is_empty() {
            let joined = current_words.join(" ");
            if normalize_ws(&joined) == normalize_ws(&text) {
                out.push(OcrLine {
                    text,
                    left: c.x.round() as i32,
                    top: c.y.round() as i32,
                    width: c.width.round() as i32,
                    height: c.height.round() as i32,
                });
                current_words.clear();
                continue;
            }
        }

        current_words.push(text);
    }

    // Fallback: if we couldn't identify lines, emit a compact word list instead.
    if out.is_empty() && !current_words.is_empty() {
        out.push(OcrLine {
            text: current_words.join(" "),
            left: 0,
            top: 0,
            width: 0,
            height: 0,
        });
    }

    out
}

#[cfg(windows)]
fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(windows)]
fn parse_data_url_base64(url: &str) -> Option<(String, String)> {
    // data:<mime>;base64,<data...>
    let trimmed = url.trim();
    if !trimmed.starts_with("data:") {
        return None;
    }
    let (header, data) = trimmed.split_once(',')?;
    if !header.contains(";base64") {
        return None;
    }
    let mime = header
        .strip_prefix("data:")?
        .split(';')
        .next()
        .unwrap_or("application/octet-stream")
        .trim()
        .to_string();
    Some((mime, data.trim().to_string()))
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
            ],
        )];

        apply_message_preflight_hooks(&cfg, "m", &mut messages)
            .await
            .expect("hook ok");

        assert!(messages[0].content_parts.is_some());
        assert!(messages[0].content.contains("hi"));
    }
}
