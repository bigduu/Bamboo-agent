use base64::Engine;

use bamboo_agent_core::storage::AttachmentReader;
use bamboo_agent_core::MessagePart;
use bamboo_agent_core::{AgentError, Message};

pub(crate) fn parse_bamboo_attachment_url(url: &str) -> Option<(&str, &str)> {
    let trimmed = url.trim();
    let rest = trimmed.strip_prefix("bamboo-attachment://")?;
    let (session_id, attachment_id) = rest.split_once('/')?;
    if session_id.is_empty() || attachment_id.is_empty() {
        return None;
    }
    Some((session_id, attachment_id))
}

pub(super) async fn resolve_bamboo_attachments_for_llm(
    messages: &mut [Message],
    reader: &dyn AttachmentReader,
) -> std::result::Result<(), AgentError> {
    for msg in messages.iter_mut() {
        let Some(parts) = msg.content_parts.as_mut() else {
            continue;
        };
        for part in parts.iter_mut() {
            let MessagePart::ImageUrl { image_url } = part else {
                continue;
            };
            if let Some(url) = resolve_attachment_image_url(&image_url.url, reader).await? {
                image_url.url = url;
            }
        }
    }
    Ok(())
}

pub(super) async fn resolve_attachment_image_url(
    url: &str,
    reader: &dyn AttachmentReader,
) -> Result<Option<String>, AgentError> {
    let Some((session_id, attachment_id)) = parse_bamboo_attachment_url(url) else {
        return Ok(None);
    };
    let Some((bytes, mime)) = reader
        .read_attachment(session_id, attachment_id)
        .await
        .map_err(|error| AgentError::LLM(format!("failed to read attachment: {error}")))?
    else {
        return Err(AgentError::LLM(format!(
            "attachment not found: {session_id}/{attachment_id}"
        )));
    };
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    Ok(Some(format!("data:{mime};base64,{encoded}")))
}

#[cfg(test)]
mod tests {
    use super::parse_bamboo_attachment_url;

    #[test]
    fn parse_bamboo_attachment_url_accepts_well_formed_urls() {
        assert_eq!(
            parse_bamboo_attachment_url("bamboo-attachment://session-1/attach-1"),
            Some(("session-1", "attach-1"))
        );
    }

    #[test]
    fn parse_bamboo_attachment_url_rejects_invalid_urls() {
        assert_eq!(
            parse_bamboo_attachment_url("https://example.com/a.png"),
            None
        );
        assert_eq!(
            parse_bamboo_attachment_url("bamboo-attachment://session-only"),
            None
        );
        assert_eq!(
            parse_bamboo_attachment_url("bamboo-attachment:///attach"),
            None
        );
        assert_eq!(
            parse_bamboo_attachment_url("bamboo-attachment://session/"),
            None
        );
    }
}
