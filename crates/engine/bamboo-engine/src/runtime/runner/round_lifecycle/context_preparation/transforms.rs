use std::collections::HashSet;
use std::sync::Arc;

use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::{AgentError, Message, Role};
use bamboo_compression::PreparedContext;
use bamboo_llm::LLMProvider;

use super::super::super::image_fallback::{
    apply_image_fallback_to_llm_messages, resolve_bamboo_attachments_for_llm,
};

pub(super) async fn apply_message_transforms(
    config: &AgentLoopConfig,
    prepared_context: &mut PreparedContext,
    llm: &Arc<dyn LLMProvider>,
    session_id: &str,
    model_name: &str,
) -> Result<(), AgentError> {
    normalize_tool_chains(&mut prepared_context.messages, session_id);
    if !llm.supports_vision(model_name).await {
        if prepared_context.messages.iter().any(|message| {
            message.content_parts.as_ref().is_some_and(|parts| {
                parts
                    .iter()
                    .any(|part| matches!(part, bamboo_domain::MessagePart::ImageUrl { .. }))
            })
        }) {
            return Err(AgentError::LLM(format!("Model '{model_name}' does not support Vision; image history was preserved but cannot be sent. Select a Vision-capable model or enable supports_vision for this model in provider settings.")));
        }
    } else if llm.vision_support_override(model_name).await != Some(true) {
        // Vision remains enabled by default. Preserve a legacy fallback only
        // when the user independently opted into that transform; it is not a
        // declaration that this model lacks Vision. Explicit support sends the
        // native image parts without rewriting them.
        apply_image_fallback(config, prepared_context, llm).await?;
    }
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

#[cfg(test)]
mod vision_tests {
    use super::*;
    struct SwitchingProvider;
    #[async_trait::async_trait]
    impl LLMProvider for SwitchingProvider {
        async fn vision_support_override(&self, model: &str) -> Option<bool> {
            match model {
                "image" => Some(true),
                "text" => Some(false),
                _ => None,
            }
        }
        async fn chat_stream(
            &self,
            _: &[Message],
            _: &[bamboo_domain::ToolSchema],
            _: Option<u32>,
            _: &str,
        ) -> bamboo_llm::provider::Result<bamboo_llm::LLMStream> {
            panic!("no paid fallback during capability checks")
        }
    }
    #[tokio::test]
    async fn vision_model_switch_preserves_history_and_true_bypasses_legacy_fallback() {
        let image = Message::user_with_parts(
            "image",
            vec![bamboo_domain::MessagePart::ImageUrl {
                image_url: bamboo_domain::ImageUrlRef {
                    url: "data:image/png;base64,AAAA".into(),
                    detail: None,
                },
            }],
        );
        let prepared = || PreparedContext {
            messages: vec![image.clone()],
            token_usage: bamboo_domain::TokenUsageBreakdown {
                system_tokens: 0,
                summary_tokens: 0,
                window_tokens: 0,
                total_tokens: 0,
                budget_limit: 1000,
            },
            truncation_occurred: false,
            segments_removed: 0,
            compressed_message_ids: vec![],
            prompt_cached_tool_outputs: 0,
            prompt_cached_tool_tokens_saved: 0,
        };
        let config = AgentLoopConfig {
            image_fallback: Some(crate::runtime::config::ImageFallbackConfig {
                mode: crate::runtime::config::ImageFallbackMode::Placeholder,
                vision_model: None,
            }),
            ..Default::default()
        };
        let llm: Arc<dyn LLMProvider> = Arc::new(SwitchingProvider);
        let mut messages = prepared();
        apply_message_transforms(&config, &mut messages, &llm, "switch", "image")
            .await
            .unwrap();
        assert!(messages.messages[0].content_parts.is_some());
        let error = apply_message_transforms(&config, &mut messages, &llm, "switch", "text")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("does not support Vision"));
        assert!(
            messages.messages[0].content_parts.is_some(),
            "images are not silently removed"
        );
        apply_message_transforms(&config, &mut messages, &llm, "switch", "image")
            .await
            .unwrap();
        assert!(llm.supports_vision("old").await);
        let mut default_on = prepared();
        apply_message_transforms(
            &AgentLoopConfig::default(),
            &mut default_on,
            &llm,
            "switch",
            "old",
        )
        .await
        .unwrap();
        assert!(
            default_on.messages[0].content_parts.is_some(),
            "missing capability defaults to supported image transport"
        );
        let mut legacy = prepared();
        apply_message_transforms(&config, &mut legacy, &llm, "switch", "old")
            .await
            .unwrap();
        assert!(
            legacy.messages[0].content_parts.is_none(),
            "inherited model retains existing fallback"
        );
    }
}
