use std::collections::HashSet;
use std::sync::Arc;

use bamboo_compression::PreparedContext;
use bamboo_agent_core::{AgentError, Message, Role};
use bamboo_infrastructure::LLMProvider;
use crate::runtime::config::AgentLoopConfig;

use super::super::super::image_fallback::{
    apply_image_fallback_to_llm_messages, resolve_bamboo_attachments_for_llm,
};

pub(super) async fn apply_message_transforms(
    config: &AgentLoopConfig,
    prepared_context: &mut PreparedContext,
    llm: &Arc<dyn LLMProvider>,
    session_id: &str,
) -> Result<(), AgentError> {
    normalize_tool_chains(&mut prepared_context.messages, session_id);
    apply_image_fallback(config, prepared_context, llm).await?;
    resolve_attachments(config, prepared_context).await?;
    Ok(())
}

fn normalize_tool_chains(messages: &mut Vec<Message>, session_id: &str) {
    let resolved_tool_result_ids: HashSet<String> = messages
        .iter()
        .filter(|message| matches!(message.role, Role::Tool))
        .filter_map(|message| {
            message
                .tool_call_id
                .as_deref()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(str::to_string)
        })
        .collect();

    let mut removed_assistant_calls = 0usize;
    for message in messages.iter_mut() {
        if !matches!(message.role, Role::Assistant) {
            continue;
        }
        let Some(tool_calls) = message.tool_calls.take() else {
            continue;
        };
        let original_len = tool_calls.len();
        let kept_calls = tool_calls
            .into_iter()
            .filter(|call| {
                let id = call.id.trim();
                !id.is_empty() && resolved_tool_result_ids.contains(id)
            })
            .collect::<Vec<_>>();
        removed_assistant_calls += original_len.saturating_sub(kept_calls.len());
        message.tool_calls = if kept_calls.is_empty() {
            None
        } else {
            Some(kept_calls)
        };
    }

    let valid_tool_call_ids: HashSet<String> = messages
        .iter()
        .filter_map(|message| message.tool_calls.as_ref())
        .flatten()
        .filter_map(|call| {
            let id = call.id.trim();
            if id.is_empty() {
                None
            } else {
                Some(id.to_string())
            }
        })
        .collect();

    let before_tool_result_count = messages
        .iter()
        .filter(|message| matches!(message.role, Role::Tool))
        .count();
    messages.retain(|message| {
        if !matches!(message.role, Role::Tool) {
            return true;
        }
        message
            .tool_call_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .is_some_and(|id| valid_tool_call_ids.contains(id))
    });
    let after_tool_result_count = messages
        .iter()
        .filter(|message| matches!(message.role, Role::Tool))
        .count();
    let removed_tool_results = before_tool_result_count.saturating_sub(after_tool_result_count);

    if removed_assistant_calls == 0 && removed_tool_results == 0 {
        return;
    }

    tracing::warn!(
        "[{}] Sanitized malformed tool chains in prepared context: removed_assistant_tool_calls={}, removed_tool_results={}",
        session_id,
        removed_assistant_calls,
        removed_tool_results
    );
}

async fn apply_image_fallback(
    config: &AgentLoopConfig,
    prepared_context: &mut PreparedContext,
    llm: &Arc<dyn LLMProvider>,
) -> Result<(), AgentError> {
    // Apply image fallback (placeholder / OCR / error / vision) to the prepared
    // LLM context only. This must never mutate the persisted session messages
    // (UI should still show images).
    if let Some(fallback) = config.image_fallback.clone() {
        apply_image_fallback_to_llm_messages(
            &mut prepared_context.messages,
            fallback,
            config.attachment_reader.as_deref(),
            Some(llm),
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
