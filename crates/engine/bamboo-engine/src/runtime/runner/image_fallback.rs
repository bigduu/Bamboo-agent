//! Image fallback and attachment resolution helpers for the agent loop runner.

use std::sync::Arc;

use crate::runtime::config::ImageFallbackConfig;
use bamboo_agent_core::storage::AttachmentReader;
use bamboo_agent_core::{AgentError, Message};
use bamboo_llm::{LLMProvider, ResolvedModel};

mod attachment_urls;
mod input_references;
#[cfg(test)]
pub(crate) use input_references::conversation_image_reference;
pub(crate) use input_references::{
    image_reference_reader, project_nonvision_images, round_needs_image_references,
};
#[cfg(windows)]
mod ocr;
mod placeholder;
mod rewrite;
mod vision_target;
pub(crate) use vision_target::{configured_vision_resolver, validate_configured_vision_target};

#[cfg(windows)]
pub async fn ensure_session_image_ocr_cached(
    session: &mut bamboo_agent_core::Session,
    attachment_reader: Option<&dyn AttachmentReader>,
) -> bool {
    ocr::ensure_session_image_ocr_cached(session, attachment_reader).await
}

pub async fn resolve_bamboo_attachments_for_llm(
    messages: &mut [Message],
    reader: &dyn AttachmentReader,
) -> std::result::Result<(), AgentError> {
    attachment_urls::resolve_bamboo_attachments_for_llm(messages, reader).await
}

#[cfg(any(test, windows))]
pub fn persistable_image_urls(parts: &[bamboo_domain::MessagePart]) -> Vec<String> {
    placeholder::persistable_image_urls(parts)
}

/// The independent legacy hook omits an image when its description request
/// fails. It must not inherit the stricter tool-result error contract.
pub async fn apply_image_fallback_to_llm_messages(
    messages: &mut [Message],
    fallback: ImageFallbackConfig,
    attachment_reader: Option<&dyn AttachmentReader>,
    llm: Option<&Arc<dyn LLMProvider>>,
) -> std::result::Result<(), AgentError> {
    if fallback.mode == crate::runtime::config::ImageFallbackMode::Vision {
        bamboo_llm::validate_tool_image_budget(messages.iter()).map_err(AgentError::LLM)?;
    }
    rewrite::apply_image_fallback_to_llm_messages(
        messages,
        fallback,
        attachment_reader,
        llm,
        rewrite::VisionFailurePolicy::OmitImage,
    )
    .await
}

/// Fresh ViewImage results fail atomically; a partial description is never
/// substituted for a successful tool result.
async fn apply_strict_image_fallback_to_llm_messages(
    messages: &mut [Message],
    fallback: ImageFallbackConfig,
    attachment_reader: Option<&dyn AttachmentReader>,
    llm: Option<&Arc<dyn LLMProvider>>,
) -> std::result::Result<(), AgentError> {
    bamboo_llm::validate_tool_image_budget(messages.iter()).map_err(AgentError::LLM)?;
    tokio::time::timeout(
        VISION_TIMEOUT,
        rewrite::apply_image_fallback_to_llm_messages(
            messages,
            fallback,
            attachment_reader,
            llm,
            rewrite::VisionFailurePolicy::Error,
        ),
    )
    .await
    .map_err(|_| AgentError::LLM(VISION_TIMEOUT_MESSAGE.into()))?
}

#[cfg(test)]
mod tests;

/// A host resolver exclusively for ViewImage tool results, kept separate
/// from model arguments. Never apply it to conversation uploads or history.
pub(crate) type VisionModelResolver = Arc<
    dyn Fn() -> futures::future::BoxFuture<'static, Result<ResolvedModel, AgentError>>
        + Send
        + Sync,
>;

pub(crate) const VISION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const VISION_TIMEOUT_MESSAGE: &str =
    "Vision description timed out; no image or partial description was sent to the main model.";
pub(crate) const VISION_CONFIGURATION_ERROR: &str = "ViewImage requires a configured Vision model for a main model without Vision. Original images and attachment references were preserved. Configure defaults.vision (provider instance and model, with features.provider_model_ref enabled), or vision_model on the active provider, or select a genuinely Vision-capable main model. No image was sent.";

pub(crate) async fn resolve_vision_target(
    config: &crate::runtime::config::AgentLoopConfig,
) -> Result<ResolvedModel, AgentError> {
    let resolver = config
        .vision_model_resolver
        .as_ref()
        .ok_or_else(|| AgentError::LLM(VISION_CONFIGURATION_ERROR.into()))?;
    tokio::time::timeout(VISION_TIMEOUT, async {
        let target = resolver().await?;
        if target.model_name.trim().is_empty() {
            return Err(AgentError::LLM(VISION_CONFIGURATION_ERROR.into()));
        }
        if !target.provider.supports_vision(&target.model_name).await {
            return Err(AgentError::LLM(format!(
                "Configured Vision model does not support Vision. {VISION_CONFIGURATION_ERROR}"
            )));
        }
        Ok(target)
    })
    .await
    .map_err(|_| AgentError::LLM("Vision model resolution timed out. No image was sent.".into()))?
}

pub(crate) async fn describe_tool_images(
    target: &ResolvedModel,
    result: &mut bamboo_agent_core::ToolResult,
) -> Result<(), AgentError> {
    bamboo_llm::validate_tool_result_images(&result.images).map_err(AgentError::LLM)?;
    let mut messages = [Message::tool_result_with_images(
        "vision-description",
        &result.result,
        true,
        result.images.clone(),
    )];
    apply_strict_image_fallback_to_llm_messages(
        &mut messages,
        ImageFallbackConfig {
            mode: crate::runtime::config::ImageFallbackMode::Vision,
            vision_model: Some(target.model_name.clone()),
        },
        None,
        Some(&target.provider),
    )
    .await?;
    result.result = std::mem::take(&mut messages[0].content);
    result.images.clear();
    Ok(())
}
