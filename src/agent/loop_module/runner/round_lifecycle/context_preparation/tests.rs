use super::prepare_round_context;
use crate::agent::core::{Message, Role, Session};
use crate::agent::llm::models::{ContentPart, ImageUrl};
use crate::agent::loop_module::config::{AgentLoopConfig, ImageFallbackConfig, ImageFallbackMode};

#[tokio::test]
async fn prepare_round_context_applies_placeholder_fallback_only_to_prepared_context() {
    let mut session = Session::new("session-cp-1", "test-model");
    session.messages.push(Message::user_with_parts(
        "看图",
        vec![ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "bamboo-attachment://s1/a1".to_string(),
                detail: None,
            },
        }],
    ));

    let config = AgentLoopConfig {
        model_name: Some("test-model".to_string()),
        image_fallback: Some(ImageFallbackConfig {
            mode: ImageFallbackMode::Placeholder,
        }),
        ..Default::default()
    };

    let prepared = prepare_round_context(&mut session, &config, "test-model", "session-cp-1")
        .await
        .expect("prepare round context");

    let prepared_user = prepared
        .prepared_context
        .messages
        .iter()
        .find(|m| matches!(m.role, Role::User))
        .expect("prepared user message should exist");

    assert!(prepared_user.content_parts.is_none());
    assert!(prepared_user.content.contains("[Image omitted:"));

    let persisted_user = session
        .messages
        .iter()
        .find(|m| matches!(m.role, Role::User))
        .expect("persisted user message should exist");
    assert!(persisted_user.content_parts.is_some());
}
