use crate::agent::core::storage::AttachmentReader;
use crate::agent::core::{AgentError, Message};
use crate::agent::llm::models::ContentPart;
use crate::agent::loop_module::config::{ImageFallbackConfig, ImageFallbackMode};

#[cfg(windows)]
use super::ocr::rewrite_parts_to_ocr_text;
use super::placeholder::rewrite_parts_to_placeholder;

pub(super) async fn apply_image_fallback_to_llm_messages(
    messages: &mut [Message],
    fallback: ImageFallbackConfig,
    attachment_reader: Option<&dyn AttachmentReader>,
) -> std::result::Result<(), AgentError> {
    #[cfg(not(windows))]
    let _ = attachment_reader;

    for message in messages.iter_mut() {
        if !has_image_parts(message) {
            continue;
        }

        apply_fallback_to_message(message, fallback.mode, attachment_reader).await?;
    }

    Ok(())
}

fn has_image_parts(message: &Message) -> bool {
    message.content_parts.as_ref().is_some_and(|parts| {
        parts
            .iter()
            .any(|part| matches!(part, ContentPart::ImageUrl { .. }))
    })
}

async fn apply_fallback_to_message(
    message: &mut Message,
    mode: ImageFallbackMode,
    attachment_reader: Option<&dyn AttachmentReader>,
) -> std::result::Result<(), AgentError> {
    #[cfg(not(windows))]
    let _ = attachment_reader;

    let Some(parts) = message.content_parts.as_ref() else {
        return Ok(());
    };

    match mode {
        ImageFallbackMode::Error => Err(AgentError::LLM(
            "This server does not currently support image inputs. Configure hooks.image_fallback.mode='placeholder' or 'ocr' to degrade gracefully.".to_string(),
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
                log::info!(
                    "OCR image fallback requested but OCR is currently Windows-only; leaving images intact."
                );
            }

            Ok(())
        }
    }
}
