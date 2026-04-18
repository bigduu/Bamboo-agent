//! Image fallback and attachment resolution helpers for the agent loop runner.

use std::sync::Arc;

use bamboo_application_agent::storage::AttachmentReader;
use bamboo_application_agent::{AgentError, Message};
use bamboo_infrastructure_llm::LLMProvider;
use crate::config::ImageFallbackConfig;

mod attachment_urls;
#[cfg(windows)]
mod ocr;
mod placeholder;
mod rewrite;

#[cfg(windows)]
pub(super) async fn ensure_session_image_ocr_cached(
    session: &mut bamboo_application_agent::Session,
    attachment_reader: Option<&dyn AttachmentReader>,
) -> bool {
    ocr::ensure_session_image_ocr_cached(session, attachment_reader).await
}

pub(super) async fn resolve_bamboo_attachments_for_llm(
    messages: &mut [Message],
    reader: &dyn AttachmentReader,
) -> std::result::Result<(), AgentError> {
    attachment_urls::resolve_bamboo_attachments_for_llm(messages, reader).await
}

#[cfg(any(test, windows))]
pub(super) fn persistable_image_urls(
    parts: &[bamboo_domain_session::MessagePart],
) -> Vec<String> {
    placeholder::persistable_image_urls(parts)
}

pub(super) async fn apply_image_fallback_to_llm_messages(
    messages: &mut [Message],
    fallback: ImageFallbackConfig,
    attachment_reader: Option<&dyn AttachmentReader>,
    llm: Option<&Arc<dyn LLMProvider>>,
) -> std::result::Result<(), AgentError> {
    rewrite::apply_image_fallback_to_llm_messages(messages, fallback, attachment_reader, llm).await
}

#[cfg(test)]
mod tests;
