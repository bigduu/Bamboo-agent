use async_trait::async_trait;

use bamboo_config::KeywordMaskingConfig;
use bamboo_domain::Message;
use bamboo_domain::MessagePart;
use bamboo_domain::ToolSchema;

use crate::prompt_ir::PromptIR;
use crate::provider::{LLMProvider, LLMRequestOptions, LLMStream, ProviderModelInfo, Result};

/// Decorates an [`LLMProvider`] by applying keyword masking to outgoing messages.
///
/// Masking is applied only when the provided [`KeywordMaskingConfig`] has at least
/// one enabled entry.
pub struct MaskingProviderDecorator<P: LLMProvider> {
    inner: P,
    masking_config: KeywordMaskingConfig,
}

impl<P: LLMProvider> MaskingProviderDecorator<P> {
    pub fn new(inner: P, masking_config: KeywordMaskingConfig) -> Self {
        Self {
            inner,
            masking_config,
        }
    }

    fn log_masking_applied(session_id: Option<&str>, message_count: usize) {
        if let Some(session_id) = session_id {
            tracing::debug!(
                "[{}] Applied keyword masking to {} messages",
                session_id,
                message_count
            );
            return;
        }

        tracing::debug!("Applied keyword masking to {} messages", message_count);
    }
}

#[async_trait]
impl<P: LLMProvider> LLMProvider for MaskingProviderDecorator<P> {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream> {
        if self.masking_config.entries.is_empty() {
            return self
                .inner
                .chat_stream(messages, tools, max_output_tokens, model)
                .await;
        }

        let masked_messages: Vec<Message> = messages
            .iter()
            .map(|m| {
                let mut masked = m.clone();
                masked.content = self.masking_config.apply_masking(&m.content);
                if let Some(parts) = m.content_parts.as_ref() {
                    let masked_parts = parts
                        .iter()
                        .map(|part| match part {
                            MessagePart::Text { text } => MessagePart::Text {
                                text: self.masking_config.apply_masking(text),
                            },
                            MessagePart::ImageUrl { image_url } => MessagePart::ImageUrl {
                                image_url: image_url.clone(),
                            },
                        })
                        .collect::<Vec<_>>();
                    masked.content_parts = Some(masked_parts);
                }
                masked
            })
            .collect();

        Self::log_masking_applied(None, masked_messages.len());

        self.inner
            .chat_stream(&masked_messages, tools, max_output_tokens, model)
            .await
    }

    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        if self.masking_config.entries.is_empty() {
            return self
                .inner
                .chat_stream_with_options(messages, tools, max_output_tokens, model, options)
                .await;
        }

        let masked_messages: Vec<Message> = messages
            .iter()
            .map(|m| {
                let mut masked = m.clone();
                masked.content = self.masking_config.apply_masking(&m.content);
                if let Some(parts) = m.content_parts.as_ref() {
                    let masked_parts = parts
                        .iter()
                        .map(|part| match part {
                            MessagePart::Text { text } => MessagePart::Text {
                                text: self.masking_config.apply_masking(text),
                            },
                            MessagePart::ImageUrl { image_url } => MessagePart::ImageUrl {
                                image_url: image_url.clone(),
                            },
                        })
                        .collect::<Vec<_>>();
                    masked.content_parts = Some(masked_parts);
                }
                masked
            })
            .collect();

        let session_id = options.and_then(|value| value.session_id.as_deref());
        Self::log_masking_applied(session_id, masked_messages.len());

        self.inner
            .chat_stream_with_options(&masked_messages, tools, max_output_tokens, model, options)
            .await
    }

    async fn chat_stream_ir(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        // Forward the canonical IR to the inner provider so its `chat_stream_ir`
        // override (Anthropic block-native system, OpenAI/Copilot Responses view)
        // actually runs — masking the IR's text in place rather than collapsing it
        // to a flat message list (which would bypass the inner override entirely).
        if self.masking_config.entries.is_empty() {
            return self
                .inner
                .chat_stream_ir(ir, tools, max_output_tokens, model, options)
                .await;
        }

        let mut masked = ir.clone();
        masked.system_text = self.masking_config.apply_masking(&masked.system_text);
        for block in masked.system_blocks.iter_mut() {
            block.text = self.masking_config.apply_masking(&block.text);
        }
        let mut message_count = 0usize;
        for segment in masked.segments.iter_mut() {
            for message in segment.messages.iter_mut() {
                message.content = self.masking_config.apply_masking(&message.content);
                if let Some(parts) = message.content_parts.as_mut() {
                    for part in parts.iter_mut() {
                        if let MessagePart::Text { text } = part {
                            *text = self.masking_config.apply_masking(text);
                        }
                    }
                }
                message_count += 1;
            }
        }

        let session_id = options.and_then(|value| value.session_id.as_deref());
        Self::log_masking_applied(session_id, message_count);

        self.inner
            .chat_stream_ir(&masked, tools, max_output_tokens, model, options)
            .await
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        self.inner.list_models().await
    }

    async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>> {
        self.inner.list_model_info().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures::stream;

    use super::*;
    use bamboo_config::keyword_masking::{KeywordEntry, MatchType};

    #[derive(Clone, Default)]
    struct RecordingProvider {
        seen: Arc<Mutex<Vec<Vec<Message>>>>,
        /// Set with the IR's (system_text, all-message-contents) whenever the
        /// canonical `chat_stream_ir` override is reached — proving the decorator
        /// forwards the IR rather than collapsing it to a flat list.
        ir_seen: Arc<Mutex<Option<(String, Vec<String>)>>>,
    }

    #[async_trait]
    impl LLMProvider for RecordingProvider {
        async fn chat_stream(
            &self,
            messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream> {
            self.seen.lock().expect("lock").push(messages.to_vec());
            Ok(Box::pin(stream::empty()))
        }

        async fn chat_stream_ir(
            &self,
            ir: &PromptIR,
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
            _options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream> {
            let contents = ir
                .segments
                .iter()
                .flat_map(|segment| segment.messages.iter())
                .map(|message| message.content.clone())
                .collect();
            *self.ir_seen.lock().expect("lock") = Some((ir.system_text.clone(), contents));
            Ok(Box::pin(stream::empty()))
        }
    }

    fn ir_with(system: &str, message: &str) -> PromptIR {
        use crate::prompt_ir::{Segment, SegmentRole};
        PromptIR {
            system_text: system.to_string(),
            segments: vec![Segment::new(
                SegmentRole::Conversation,
                vec![Message::user(message)],
            )],
            ..PromptIR::default()
        }
    }

    #[tokio::test]
    async fn masks_message_content_when_entries_present() {
        let inner = RecordingProvider::default();
        let seen = inner.seen.clone();

        let config = KeywordMaskingConfig {
            entries: vec![KeywordEntry {
                pattern: "secret".to_string(),
                match_type: MatchType::Exact,
                enabled: true,
            }],
        };

        let decorator = MaskingProviderDecorator::new(inner, config);

        let messages = vec![Message::user("This is secret")];
        let tools: Vec<ToolSchema> = Vec::new();

        let _stream = decorator
            .chat_stream(&messages, &tools, None, "test-model")
            .await
            .expect("chat_stream");

        let recorded = seen.lock().expect("lock");
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].len(), 1);
        assert_eq!(recorded[0][0].content, "This is [MASKED]");
    }

    #[tokio::test]
    async fn passes_through_when_config_is_empty() {
        let inner = RecordingProvider::default();
        let seen = inner.seen.clone();

        let decorator = MaskingProviderDecorator::new(inner, KeywordMaskingConfig::default());

        let messages = vec![Message::user("This is secret")];
        let tools: Vec<ToolSchema> = Vec::new();

        let _stream = decorator
            .chat_stream(&messages, &tools, None, "test-model")
            .await
            .expect("chat_stream");

        let recorded = seen.lock().expect("lock");
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].len(), 1);
        assert_eq!(recorded[0][0].content, "This is secret");
    }

    #[tokio::test]
    async fn chat_stream_ir_forwards_to_inner_ir_override_and_masks() {
        // The decorator must forward the canonical IR to the inner provider's
        // `chat_stream_ir` override (so Anthropic block-native / OpenAI Responses
        // overrides actually run), masking the IR's text in place — NOT collapse it
        // to a flat list via the trait default.
        let inner = RecordingProvider::default();
        let ir_seen = inner.ir_seen.clone();
        let flat_seen = inner.seen.clone();

        let config = KeywordMaskingConfig {
            entries: vec![KeywordEntry {
                pattern: "secret".to_string(),
                match_type: MatchType::Exact,
                enabled: true,
            }],
        };
        let decorator = MaskingProviderDecorator::new(inner, config);

        let _stream = decorator
            .chat_stream_ir(
                &ir_with("system has a secret", "message with a secret"),
                &[],
                None,
                "test-model",
                None,
            )
            .await
            .expect("chat_stream_ir");

        // The inner IR override was reached (the flat path was NOT).
        assert!(
            flat_seen.lock().expect("lock").is_empty(),
            "must not collapse the IR to the flat chat path"
        );
        let (system, contents) = ir_seen
            .lock()
            .expect("lock")
            .clone()
            .expect("inner chat_stream_ir reached");
        assert_eq!(system, "system has a [MASKED]");
        assert_eq!(contents, vec!["message with a [MASKED]".to_string()]);
    }

    #[tokio::test]
    async fn chat_stream_ir_passthrough_forwards_unmasked_when_config_empty() {
        let inner = RecordingProvider::default();
        let ir_seen = inner.ir_seen.clone();
        let decorator = MaskingProviderDecorator::new(inner, KeywordMaskingConfig::default());

        let _stream = decorator
            .chat_stream_ir(
                &ir_with("system has a secret", "message with a secret"),
                &[],
                None,
                "test-model",
                None,
            )
            .await
            .expect("chat_stream_ir");

        let (system, contents) = ir_seen
            .lock()
            .expect("lock")
            .clone()
            .expect("inner chat_stream_ir reached");
        assert_eq!(system, "system has a secret");
        assert_eq!(contents, vec!["message with a secret".to_string()]);
    }
}
