use super::{apply_image_fallback_to_llm_messages, persistable_image_urls};
use crate::agent::core::{AgentError, Message, Session};
use crate::agent::llm::models::{ContentPart, ImageUrl};
use crate::agent::loop_module::config::{ImageFallbackConfig, ImageFallbackMode};

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

    let urls = persistable_image_urls(&parts);
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
    session
        .messages
        .push(Message::user_with_parts("这个内容有什么", parts));

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
    let mut messages = vec![Message::user_with_parts("请描述图片", parts)];

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
