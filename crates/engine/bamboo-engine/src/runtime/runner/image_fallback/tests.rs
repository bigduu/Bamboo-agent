use super::{apply_image_fallback_to_llm_messages, persistable_image_urls};
use crate::runtime::config::{ImageFallbackConfig, ImageFallbackMode};
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{AgentError, Message, Session};
use bamboo_domain::MessagePart;
use bamboo_llm::models::{ContentPart, ImageUrl};
use bamboo_llm::provider::{LLMError, LLMProvider, LLMStream};
use bamboo_llm::types::LLMChunk;
use futures::stream;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct RecordingVisionProvider {
    models: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl LLMProvider for RecordingVisionProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.models
            .lock()
            .expect("model lock should not be poisoned")
            .push(model.to_string());
        Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Token(
            "vision summary".to_string(),
        ))])))
    }
}

#[test]
fn persistable_image_urls_filters_out_data_urls() {
    let parts = vec![
        ContentPart::Text {
            text: "hello".to_string(),
        },
        ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "data:image/png;base64,AAAA".to_string(),
                detail: None,
            },
        },
        ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "bamboo-attachment://s1/a1".to_string(),
                detail: None,
            },
        },
    ];

    let urls = persistable_image_urls(
        &parts
            .into_iter()
            .map(Into::into)
            .collect::<Vec<MessagePart>>(),
    );
    assert_eq!(urls, vec!["bamboo-attachment://s1/a1".to_string()]);
}

#[tokio::test]
async fn image_fallback_placeholder_does_not_mutate_persisted_session_messages() {
    let parts = vec![
        ContentPart::Text {
            text: "这个内容有什么".to_string(),
        },
        ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "bamboo-attachment://s1/a1".to_string(),
                detail: None,
            },
        },
    ];

    let mut session = Session::new("s1", "m");
    session.messages.push(Message::user_with_parts(
        "这个内容有什么",
        parts.into_iter().map(Into::into).collect(),
    ));

    let mut llm_messages = session.messages.clone();
    apply_image_fallback_to_llm_messages(
        &mut llm_messages,
        ImageFallbackConfig {
            mode: ImageFallbackMode::Placeholder,
            vision_model: None,
        },
        None,
        None,
    )
    .await
    .unwrap();

    assert!(session.messages[0].content_parts.is_some());
    assert!(llm_messages[0].content_parts.is_none());
    assert!(llm_messages[0]
        .content
        .contains("[Image omitted: bamboo-attachment://s1/a1]"));
}

#[tokio::test]
async fn image_fallback_error_mode_rejects_messages_with_images() {
    let parts = vec![
        ContentPart::Text {
            text: "请描述图片".to_string(),
        },
        ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "bamboo-attachment://s1/a1".to_string(),
                detail: None,
            },
        },
    ];
    let mut messages = vec![Message::user_with_parts(
        "请描述图片",
        parts.into_iter().map(Into::into).collect(),
    )];

    let result = apply_image_fallback_to_llm_messages(
        &mut messages,
        ImageFallbackConfig {
            mode: ImageFallbackMode::Error,
            vision_model: None,
        },
        None,
        None,
    )
    .await;

    assert!(matches!(result, Err(AgentError::LLM(_))));
}

#[tokio::test]
async fn image_fallback_skips_messages_without_image_parts() {
    let mut messages = vec![Message::user("纯文本消息")];

    apply_image_fallback_to_llm_messages(
        &mut messages,
        ImageFallbackConfig {
            mode: ImageFallbackMode::Placeholder,
            vision_model: None,
        },
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(messages[0].content, "纯文本消息");
    assert!(messages[0].content_parts.is_none());
}

#[tokio::test]
async fn image_fallback_vision_rewrites_with_llm_description() {
    let mut messages = vec![Message::user_with_parts(
        "请描述图片",
        vec![ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "https://example.com/image.png".to_string(),
                detail: None,
            },
        }]
        .into_iter()
        .map(Into::into)
        .collect(),
    )];

    let recording = Arc::new(RecordingVisionProvider::default());
    let llm: Arc<dyn LLMProvider> = recording.clone();
    apply_image_fallback_to_llm_messages(
        &mut messages,
        ImageFallbackConfig {
            mode: ImageFallbackMode::Vision,
            vision_model: Some("vision-model-test".to_string()),
        },
        None,
        Some(&llm),
    )
    .await
    .expect("vision fallback should rewrite images");

    assert!(messages[0].content_parts.is_none());
    assert!(messages[0]
        .content
        .contains("[Vision description of image 1:"));
    assert!(messages[0].content.contains("vision summary"));
    assert_eq!(
        recording
            .models
            .lock()
            .expect("model lock should not be poisoned")
            .as_slice(),
        ["vision-model-test"]
    );
}

#[tokio::test]
async fn image_fallback_vision_without_llm_leaves_images_intact() {
    let mut messages = vec![Message::user_with_parts(
        "请描述图片",
        vec![ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "https://example.com/image.png".to_string(),
                detail: None,
            },
        }]
        .into_iter()
        .map(Into::into)
        .collect(),
    )];

    apply_image_fallback_to_llm_messages(
        &mut messages,
        ImageFallbackConfig {
            mode: ImageFallbackMode::Vision,
            vision_model: Some("vision-model-test".to_string()),
        },
        None,
        None,
    )
    .await
    .expect("vision fallback without llm should not fail");

    assert!(messages[0].content_parts.is_some());
}
