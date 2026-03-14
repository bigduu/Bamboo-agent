use crate::agent::core::budget::PreparedContext;
use crate::agent::core::AgentError;
use crate::agent::loop_module::config::AgentLoopConfig;

use super::super::super::image_fallback::{
    apply_image_fallback_to_llm_messages, resolve_bamboo_attachments_for_llm,
};

pub(super) async fn apply_message_transforms(
    config: &AgentLoopConfig,
    prepared_context: &mut PreparedContext,
) -> Result<(), AgentError> {
    apply_image_fallback(config, prepared_context).await?;
    resolve_attachments(config, prepared_context).await?;
    Ok(())
}

async fn apply_image_fallback(
    config: &AgentLoopConfig,
    prepared_context: &mut PreparedContext,
) -> Result<(), AgentError> {
    // Apply image fallback (placeholder / OCR / error) to the prepared LLM context only.
    // This must never mutate the persisted session messages (UI should still show images).
    if let Some(fallback) = config.image_fallback {
        apply_image_fallback_to_llm_messages(
            &mut prepared_context.messages,
            fallback,
            config.attachment_reader.as_deref(),
        )
        .await?;
    }

    Ok(())
}

async fn resolve_attachments(
    config: &AgentLoopConfig,
    prepared_context: &mut PreparedContext,
) -> Result<(), AgentError> {
    // Resolve `bamboo-attachment://...` URLs into `data:` URLs for upstream providers.
    // This must only mutate the prepared context (never the persisted session messages).
    if let Some(reader) = config.attachment_reader.as_deref() {
        resolve_bamboo_attachments_for_llm(&mut prepared_context.messages, reader).await?;
    }

    Ok(())
}
