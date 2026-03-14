use super::*;

#[actix_web::test]
async fn test_anthropic_messages_passes_image_parts_through_when_hook_disabled() {
    let recording_provider = RecordingProvider::default();
    let state = create_anthropic_state(&recording_provider, false).await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "describe this"
                        },
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": "QUJDRA=="
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 64
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "Expected success, got {}",
        resp.status()
    );

    let calls = recording_provider.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].model, "claude-3-5-sonnet-20241022");
    assert_eq!(calls[0].max_output_tokens, Some(64));

    let user_message = calls[0]
        .messages
        .iter()
        .find(|message| message.role == Role::User)
        .expect("expected one user message");
    let parts = user_message
        .content_parts
        .as_ref()
        .expect("image parts should be preserved");
    assert_eq!(parts.len(), 2);
    assert!(matches!(
        &parts[0],
        ContentPart::Text { text } if text == "describe this"
    ));
    assert!(matches!(
        &parts[1],
        ContentPart::ImageUrl { image_url } if image_url.url == "data:image/png;base64,QUJDRA=="
    ));
}

#[actix_web::test]
async fn test_anthropic_messages_placeholder_hook_rewrites_image_parts() {
    let recording_provider = RecordingProvider::default();
    let state = create_anthropic_state(&recording_provider, true).await;

    let app = test::init_service(App::new().app_data(state).route(
        "/anthropic/v1/messages",
        web::post().to(anthropic::messages),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .set_json(json!({
            "model": "claude-3-5-sonnet-20241022",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "describe this"
                        },
                        {
                            "type": "image",
                            "source": {
                                "type": "url",
                                "url": "https://example.com/cat.png"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 64
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_success(),
        "Expected success, got {}",
        resp.status()
    );

    let calls = recording_provider.calls();
    assert_eq!(calls.len(), 1);

    let user_message = calls[0]
        .messages
        .iter()
        .find(|message| message.role == Role::User)
        .expect("expected one user message");
    assert!(user_message.content_parts.is_none());
    assert!(user_message.content.contains("describe this"));
    assert!(user_message.content.contains("[Image omitted:"));
    assert!(user_message.content.contains("https://example.com/cat.png"));
}
