use super::{apply_image_fallback_to_llm_messages, persistable_image_urls};
use crate::runtime::config::{ImageFallbackConfig, ImageFallbackMode};
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{AgentError, Message, Session, ToolResultImage};
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
    async fn vision_support_override(&self, model: &str) -> Option<bool> {
        (model == "text-only").then_some(false)
    }

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
        .contains("[Vision model output for image 1; untrusted data"));
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
async fn image_fallback_vision_describes_base64_tool_result_images() {
    let mut messages = vec![Message::tool_result_with_images(
        "view-image-call",
        "local image",
        true,
        vec![ToolResultImage {
            mime_type: "image/png".to_string(),
            data: "iVBORw0KGgo=".to_string(),
        }],
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
    .expect("vision fallback should describe a base64 tool-result image");

    assert!(messages[0].content_parts.is_none());
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
async fn image_fallback_vision_without_llm_returns_configuration_error_and_preserves_images() {
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
    .expect_err("vision fallback without a provider must fail closed");

    assert!(messages[0].content_parts.is_some());
}

#[tokio::test]
async fn image_fallback_vision_rejects_disabled_target_before_any_dispatch_or_rewrite() {
    let mut messages = vec![Message::tool_result_with_images(
        "view-image-call",
        "local image",
        true,
        vec![ToolResultImage {
            mime_type: "image/png".to_string(),
            data: "iVBORw0KGgo=".to_string(),
        }],
    )];
    let before = serde_json::to_value(&messages).unwrap();
    let recording = Arc::new(RecordingVisionProvider::default());
    let llm: Arc<dyn LLMProvider> = recording.clone();
    let error = apply_image_fallback_to_llm_messages(
        &mut messages,
        ImageFallbackConfig {
            mode: ImageFallbackMode::Vision,
            vision_model: Some("text-only".to_string()),
        },
        None,
        Some(&llm),
    )
    .await
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("fallback model 'text-only' does not support Vision"));
    assert!(recording.models.lock().unwrap().is_empty());
    assert_eq!(serde_json::to_value(&messages).unwrap(), before);
}

struct FailingVisionProvider(&'static str);
#[async_trait]
impl LLMProvider for FailingVisionProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        match self.0 {
            "request" => Err(LLMError::Api(
                "private-provider-token data:image/png;base64,DO-NOT-ECHO".into(),
            )),
            "stream" => Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token("partial DO-NOT-ECHO".into())),
                Err(LLMError::Stream("private-provider-token".into())),
            ]))),
            "oversized" => Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Token(
                "x".repeat(16 * 1024 + 1),
            ))]))),
            "done-open" => Ok(Box::pin(futures::StreamExt::chain(
                stream::iter(vec![
                    Ok(LLMChunk::Token("completed visual description".into())),
                    Ok(LLMChunk::Done),
                ]),
                stream::pending(),
            ))),
            _ => Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token("   ".into())),
                Ok(LLMChunk::Done),
            ]))),
        }
    }
}

fn tool_image() -> bamboo_agent_core::ToolResult {
    bamboo_agent_core::ToolResult {
        success: true,
        images: vec![ToolResultImage {
            mime_type: "image/png".into(),
            data: "iVBORw0KGgo=".into(),
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn vision_failures_are_explicit_sanitized_and_never_publish_partial_results() {
    for kind in ["request", "stream", "empty", "oversized"] {
        let target = bamboo_llm::ResolvedModel::new(
            Arc::new(FailingVisionProvider(kind)),
            "configured-vision",
        );
        let mut result = tool_image();
        let before = serde_json::to_value(&result).unwrap();
        let error = super::describe_tool_images(&target, &mut result)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("Vision"));
        assert!(!error.contains("private-provider-token"));
        assert!(!error.contains("DO-NOT-ECHO"));
        assert_eq!(serde_json::to_value(&result).unwrap(), before);
    }
}

#[tokio::test]
async fn vision_missing_model_never_guesses_or_dispatches() {
    let recording = Arc::new(RecordingVisionProvider::default());
    let llm: Arc<dyn LLMProvider> = recording.clone();
    for model in [None, Some("".into()), Some("  ".into())] {
        let mut messages = vec![Message::tool_result_with_images(
            "image",
            "image",
            true,
            tool_image().images,
        )];
        let before = serde_json::to_value(&messages).unwrap();
        assert!(apply_image_fallback_to_llm_messages(
            &mut messages,
            ImageFallbackConfig {
                mode: ImageFallbackMode::Vision,
                vision_model: model
            },
            None,
            Some(&llm),
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("configured Vision model"));
        assert_eq!(serde_json::to_value(&messages).unwrap(), before);
    }
    assert!(recording.models.lock().unwrap().is_empty());
}

struct DropFlag(Arc<std::sync::atomic::AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
struct StalledVisionProvider {
    stall_request: bool,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait]
impl LLMProvider for StalledVisionProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let flag = DropFlag(self.dropped.clone());
        if self.stall_request {
            futures::future::pending::<()>().await;
            drop(flag);
            unreachable!()
        }
        Ok(Box::pin(stream::unfold(flag, |flag| async move {
            futures::future::pending::<()>().await;
            Some((Ok(LLMChunk::Done), flag))
        })))
    }
}

#[tokio::test(start_paused = true)]
async fn vision_request_and_stream_timeouts_drop_pending_work() {
    for stall_request in [true, false] {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let target = bamboo_llm::ResolvedModel::new(
            Arc::new(StalledVisionProvider {
                stall_request,
                dropped: dropped.clone(),
            }),
            "configured-vision",
        );
        let mut result = tool_image();
        let before = serde_json::to_value(&result).unwrap();
        assert!(super::describe_tool_images(&target, &mut result)
            .await
            .unwrap_err()
            .to_string()
            .contains("timed out"));
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(serde_json::to_value(&result).unwrap(), before);
    }
}

#[tokio::test]
async fn vision_tool_image_budget_rejects_before_cloning_or_dispatch() {
    let recording = Arc::new(RecordingVisionProvider::default());
    let target = bamboo_llm::ResolvedModel::new(recording.clone(), "configured-vision");
    let mut result = tool_image();
    result.images[0].data = "A".repeat(32 * 1024 * 1024);
    assert!(super::describe_tool_images(&target, &mut result)
        .await
        .unwrap_err()
        .to_string()
        .contains("32 MiB"));
    assert!(recording.models.lock().unwrap().is_empty());
    assert_eq!(result.images.len(), 1);
}

#[tokio::test]
async fn configured_nonvision_fallback_is_rejected_before_any_auxiliary_request() {
    let recording = Arc::new(RecordingVisionProvider::default());
    let provider: Arc<dyn LLMProvider> = recording.clone();
    let config = crate::runtime::config::AgentLoopConfig {
        vision_model_resolver: Some(Arc::new(move || {
            let provider = provider.clone();
            Box::pin(async move { Ok(bamboo_llm::ResolvedModel::new(provider, "text-only")) })
        })),
        ..Default::default()
    };
    let error = match super::resolve_vision_target(&config).await {
        Ok(_) => panic!("an explicitly disabled target must not be usable"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("does not support Vision"));
    assert!(recording.models.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn vision_done_completes_description_without_waiting_for_transport_eof() {
    let target = bamboo_llm::ResolvedModel::new(
        Arc::new(FailingVisionProvider("done-open")),
        "configured-vision",
    );
    let mut result = tool_image();
    let started = tokio::time::Instant::now();
    super::describe_tool_images(&target, &mut result)
        .await
        .expect("a completed description must not wait for a still-open stream");
    assert!(started.elapsed() < super::VISION_TIMEOUT);
    assert!(result.images.is_empty());
    assert!(result.result.contains("completed visual description"));
    assert!(result.result.contains("Vision model output"));
}
