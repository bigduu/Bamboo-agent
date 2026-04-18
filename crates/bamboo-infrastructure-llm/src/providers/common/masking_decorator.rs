use async_trait::async_trait;

use bamboo_domain_session::{Message};
use bamboo_application_tool_types::{ToolSchema};
use bamboo_domain_session::MessagePart;
use bamboo_infrastructure_config::KeywordMaskingConfig;

use crate::provider::{
    LLMProvider, LLMRequestOptions, LLMStream, ProviderModelInfo, Result,
};

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
    use bamboo_infrastructure_config::keyword_masking::{KeywordEntry, MatchType};

    #[derive(Clone, Default)]
    struct RecordingProvider {
        seen: Arc<Mutex<Vec<Vec<Message>>>>,
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
}
