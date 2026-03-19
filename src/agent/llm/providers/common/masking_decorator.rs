use async_trait::async_trait;

use crate::agent::core::{tools::ToolSchema, Message};
use crate::agent::llm::models::ContentPart;
use crate::core::keyword_masking::KeywordMaskingConfig;

use crate::agent::llm::provider::{LLMProvider, LLMRequestOptions, LLMStream, Result};

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
                            ContentPart::Text { text } => ContentPart::Text {
                                text: self.masking_config.apply_masking(text),
                            },
                            ContentPart::ImageUrl { image_url } => ContentPart::ImageUrl {
                                image_url: image_url.clone(),
                            },
                        })
                        .collect::<Vec<_>>();
                    masked.content_parts = Some(masked_parts);
                }
                masked
            })
            .collect();

        tracing::debug!(
            "Applied keyword masking to {} messages",
            masked_messages.len()
        );

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
                            ContentPart::Text { text } => ContentPart::Text {
                                text: self.masking_config.apply_masking(text),
                            },
                            ContentPart::ImageUrl { image_url } => ContentPart::ImageUrl {
                                image_url: image_url.clone(),
                            },
                        })
                        .collect::<Vec<_>>();
                    masked.content_parts = Some(masked_parts);
                }
                masked
            })
            .collect();

        tracing::debug!(
            "Applied keyword masking to {} messages",
            masked_messages.len()
        );

        self.inner
            .chat_stream_with_options(&masked_messages, tools, max_output_tokens, model, options)
            .await
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        self.inner.list_models().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures::stream;

    use super::*;
    use crate::core::keyword_masking::{KeywordEntry, MatchType};

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
