//! Transient non-Vision image references. Original Session messages are never rewritten.

use std::collections::BTreeMap;
use std::sync::Arc;

use bamboo_agent_core::storage::AttachmentReader;
use bamboo_agent_core::{Message, MessagePart, Session, ToolError};
use bamboo_tools::tools::view_image::{ViewImageReferenceReader, MAX_VIEW_IMAGE_BYTES};
use base64::Engine;

/// Bind image bytes only when an admitted call may consume a conversation reference.
/// Existing pre-tool hooks may rewrite ViewImage arguments, so keep that one
/// case conservative; this predicate grants no callability or execution authority.
pub(crate) fn round_needs_image_references(
    calls: &[bamboo_agent_core::ToolCall],
    callable: &bamboo_domain::EffectiveCallableSet,
    has_pre_tool_hooks: bool,
) -> bool {
    calls.iter().any(|call| {
        if callable
            .resolve_callable_reference(&call.function.name)
            .as_deref()
            != Some("ViewImage")
        {
            return false;
        }
        if has_pre_tool_hooks {
            return true;
        }
        let (arguments, _) =
            bamboo_agent_core::tools::parse_tool_args_best_effort(&call.function.arguments);
        arguments
            .get("path")
            .and_then(|path| path.as_str())
            .is_some_and(|path| {
                let path = path.trim();
                path.starts_with("bamboo-attachment://") || path.starts_with("bamboo-image://")
            })
    })
}

pub(crate) fn conversation_image_reference(
    session_id: &str,
    message_id: &str,
    part_index: usize,
    url: &str,
) -> String {
    if url.trim().starts_with("bamboo-attachment://") {
        url.trim().to_string()
    } else {
        format!("bamboo-image://{session_id}/{message_id}/{part_index}")
    }
}

/// This is a prepared-context projection, not a durable message or description.
pub(crate) fn project_nonvision_images(messages: &mut [Message], session_id: &str) {
    for message in messages {
        let Some(parts) = message.content_parts.as_ref() else {
            continue;
        };
        if !parts
            .iter()
            .any(|part| matches!(part, MessagePart::ImageUrl { .. }))
        {
            continue;
        }
        let mut content = String::new();
        if !parts
            .iter()
            .any(|part| matches!(part, MessagePart::Text { .. }))
        {
            content.push_str(&message.content);
        }
        for (index, part) in parts.iter().enumerate() {
            match part {
                MessagePart::Text { text } => content.push_str(text),
                MessagePart::ImageUrl { image_url } => {
                    let reference = conversation_image_reference(
                        session_id,
                        &message.id,
                        index,
                        &image_url.url,
                    );
                    let call = serde_json::json!({"path": reference});
                    content.push_str(&format!(
                        "\n[Image retained; not inspected by this non-Vision main]\n\
                         If ViewImage is currently callable, call it with {call}. \
                         If permitted Deferred discovery is available, discover it first. \
                         If the tool is disabled, unavailable or cannot be loaded in this mode, \
                         report that actual blocker instead of claiming this reference was inspected. \
                         Only a successful tool result supplies an image description. \
                         This requires an explicitly configured Vision model: defaults.vision \
                         with features.provider_model_ref enabled, or vision_model on the active provider. If unavailable, tell the user \
                         the original image is preserved and ask them to configure that target \
                         or select a genuinely Vision-capable main model. Do not claim to have seen it.\n"
                    ));
                }
            }
        }
        message.content = content;
        message.content_parts = None;
    }
}

/// Bind exact references to original parts of this executing Session. There is
/// no resolver for arbitrary attachment IDs, other Sessions or remote URLs.
pub(crate) fn image_reference_reader(
    session: &Session,
    attachment_reader: Option<Arc<dyn AttachmentReader>>,
) -> ViewImageReferenceReader {
    let session_id = session.id.clone();
    let mut sources = BTreeMap::new();
    for message in &session.messages {
        for (index, part) in message.content_parts.iter().flatten().enumerate() {
            if let MessagePart::ImageUrl { image_url } = part {
                sources.insert(
                    conversation_image_reference(&session_id, &message.id, index, &image_url.url),
                    image_url.url.clone(),
                );
            }
        }
    }
    Arc::new(move |reference| {
        let source = sources.get(&reference).cloned();
        let session_id = session_id.clone();
        let reader = attachment_reader.clone();
        Box::pin(async move {
            let source = source.ok_or_else(|| {
                ToolError::InvalidArguments(
                "Unknown image reference. Use the exact reference from this executing conversation."
                    .into(),
            )
            })?;
            if let Some((owner, attachment_id)) =
                super::attachment_urls::parse_bamboo_attachment_url(&source)
            {
                if owner != session_id
                    || attachment_id.is_empty()
                    || !attachment_id
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
                {
                    return Err(ToolError::InvalidArguments(
                        "Image reference is invalid or belongs to another Session; no attachment was read.".into(),
                    ));
                }
                let reader = reader.ok_or_else(|| ToolError::Execution(
                    "The retained image attachment reader is unavailable. Use an authorized local image file or ask the host to restore attachment access.".into(),
                ))?;
                let (bytes, _) = reader.read_attachment(owner, attachment_id).await
                    .map_err(|_| ToolError::Execution(
                        "The retained image could not be read. Reattach the original image or use an authorized local image file.".into(),
                    ))?
                    .ok_or_else(|| ToolError::Execution(
                        "The retained image attachment was not found. Reattach the original image or use an authorized local image file.".into(),
                    ))?;
                if bytes.len() > MAX_VIEW_IMAGE_BYTES {
                    return Err(ToolError::Execution(
                        "Retained image exceeds the ViewImage size limit.".into(),
                    ));
                }
                return Ok(bytes);
            }
            if let Some((header, data)) = source.trim().split_once(',') {
                if header.starts_with("data:image/") && header.ends_with(";base64") {
                    if data.len() > MAX_VIEW_IMAGE_BYTES.div_ceil(3) * 4 {
                        return Err(ToolError::Execution(
                            "Retained image exceeds the ViewImage size limit.".into(),
                        ));
                    }
                    return base64::engine::general_purpose::STANDARD.decode(data)
                        .map_err(|_| ToolError::Execution(
                            "Retained inline image data is invalid. Reattach the original image or use an authorized local image file.".into(),
                        ));
                }
            }
            Err(ToolError::InvalidArguments(
                "This retained image source cannot be opened by ViewImage. Reattach the original image or use an authorized local image file; no remote URL was fetched.".into(),
            ))
        })
    })
}

#[cfg(test)]
#[path = "input_references_tests.rs"]
mod tests;
