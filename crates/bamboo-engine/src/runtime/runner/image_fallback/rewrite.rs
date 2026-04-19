use std::sync::Arc;

use bamboo_agent_core::storage::AttachmentReader;
use bamboo_agent_core::{AgentError, Message};
use bamboo_agent_core::MessagePart;
use bamboo_infrastructure::models::ContentPart;
use bamboo_infrastructure::LLMProvider;
use crate::runtime::config::{ImageFallbackConfig, ImageFallbackMode};
use futures::StreamExt;

#[cfg(windows)]
use super::ocr::rewrite_parts_to_ocr_text;
use super::placeholder::{rewrite_parts_to_placeholder, summarize_image_url};

pub(super) async fn apply_image_fallback_to_llm_messages(
    messages: &mut [Message],
    fallback: ImageFallbackConfig,
    attachment_reader: Option<&dyn AttachmentReader>,
    llm: Option<&Arc<dyn LLMProvider>>,
) -> std::result::Result<(), AgentError> {
    #[cfg(not(windows))]
    let _ = attachment_reader;

    for message in messages.iter_mut() {
        if !has_image_parts(message) {
            continue;
        }

        apply_fallback_to_message(message, &fallback, attachment_reader, llm).await?;
    }

    Ok(())
}

fn has_image_parts(message: &Message) -> bool {
    message.content_parts.as_ref().is_some_and(|parts| {
        parts
            .iter()
            .any(|part| matches!(part, MessagePart::ImageUrl { .. }))
    })
}

async fn apply_fallback_to_message(
    message: &mut Message,
    fallback: &ImageFallbackConfig,
    attachment_reader: Option<&dyn AttachmentReader>,
    llm: Option<&Arc<dyn LLMProvider>>,
) -> std::result::Result<(), AgentError> {
    #[cfg(not(windows))]
    let _ = attachment_reader;

    let Some(parts) = message.content_parts.as_ref() else {
        return Ok(());
    };

    match fallback.mode {
        ImageFallbackMode::Error => Err(AgentError::LLM(
            "This server does not currently support image inputs. Configure hooks.image_fallback.mode='placeholder', 'ocr', or 'vision' to degrade gracefully.".to_string(),
        )),
        ImageFallbackMode::Placeholder => {
            message.content = rewrite_parts_to_placeholder(parts);
            message.content_parts = None;
            Ok(())
        }
        ImageFallbackMode::Ocr => {
            #[cfg(windows)]
            {
                let rewritten = rewrite_parts_to_ocr_text(
                    attachment_reader,
                    parts,
                    message.image_ocr.as_deref(),
                )
                .await
                .map_err(AgentError::LLM)?;
                message.content = rewritten;
                message.content_parts = None;
            }

            #[cfg(not(windows))]
            {
                tracing::info!(
                    "OCR image fallback requested but OCR is currently Windows-only; leaving images intact."
                );
            }

            Ok(())
        }
        ImageFallbackMode::Vision => {
            let Some(llm) = llm else {
                tracing::warn!(
                    "Vision image fallback requested but no LLM provider available; leaving images intact."
                );
                return Ok(());
            };

            let vision_model = fallback
                .vision_model
                .as_deref()
                .unwrap_or("gpt-4o");

            let rewritten = rewrite_parts_via_vision(llm, vision_model, parts).await?;
            message.content = rewritten;
            message.content_parts = None;
            Ok(())
        }
    }
}

/// Use a vision-capable LLM to describe each image, then reassemble the message
/// with image descriptions replacing the original image URLs.
async fn rewrite_parts_via_vision(
    llm: &Arc<dyn LLMProvider>,
    vision_model: &str,
    parts: &[MessagePart],
) -> std::result::Result<String, AgentError> {
    let mut out = String::new();
    let mut image_index = 0usize;

    for part in parts {
        match part {
            MessagePart::Text { text } => out.push_str(text),
            MessagePart::ImageUrl { image_url } => {
                image_index += 1;
                let summary = summarize_image_url(&image_url.url);

                // Build a single-turn vision request: send the image to the
                // vision model and ask for a description.
                let vision_messages = vec![Message::user_with_parts(
                    "Describe this image in detail. Include all visible text, layout, colors, and important elements. Be concise but thorough.",
                    vec![
                        ContentPart::Text {
                            text: "Describe this image in detail. Include all visible text, layout, colors, and important elements. Be concise but thorough.".to_string(),
                        },
                        ContentPart::ImageUrl {
                            image_url: bamboo_infrastructure::models::ImageUrl {
                                url: image_url.url.clone(),
                                detail: image_url.detail.clone(),
                            },
                        },
                    ]
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                )];

                match describe_image_via_llm(llm, vision_model, &vision_messages).await {
                    Ok(description) => {
                        out.push_str("\n\n[Vision description of image ");
                        out.push_str(&image_index.to_string());
                        out.push_str(": ");
                        out.push_str(&summary);
                        out.push_str("]\n");
                        out.push_str(&description);
                        out.push('\n');
                    }
                    Err(err) => {
                        tracing::warn!(
                            "Vision fallback failed for image {} ({}): {}",
                            image_index,
                            summary,
                            err
                        );
                        // Fall back to placeholder on error
                        out.push_str("\n[Image omitted (vision failed): ");
                        out.push_str(&summary);
                        out.push_str("]\n");
                    }
                }
            }
        }
    }

    Ok(out)
}

/// Send image to a vision-capable LLM and collect the full text response.
async fn describe_image_via_llm(
    llm: &Arc<dyn LLMProvider>,
    vision_model: &str,
    messages: &[Message],
) -> std::result::Result<String, String> {
    let stream = llm
        .chat_stream(messages, &[], Some(1024), vision_model)
        .await
        .map_err(|e| format!("Vision LLM call failed: {e}"))?;

    use bamboo_infrastructure::types::LLMChunk;

    let mut description = String::new();
    let mut stream = std::pin::pin!(stream);
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(LLMChunk::Token(text)) => {
                description.push_str(&text);
            }
            Ok(_) => {} // Skip ResponseId, ReasoningToken, ToolCalls, Done
            Err(e) => {
                return Err(format!("Vision LLM stream error: {e}"));
            }
        }
    }

    Ok(description)
}
