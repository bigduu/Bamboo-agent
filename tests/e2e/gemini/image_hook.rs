use super::*;
use bamboo_domain::MessagePart;

#[actix_web::test]
async fn test_gemini_generate_content_passes_image_parts_through_when_hook_disabled() {
    let recording_provider = RecordingProvider::default();
    let state = create_gemini_state(&recording_provider, false).await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:generateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        { "text": "describe this" },
                        {
                            "inlineData": {
                                "mimeType": "image/png",
                                "data": "QUJDRA=="
                            }
                        }
                    ]
                }
            ]
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
    assert_eq!(calls[0].model, "gemini-2.0-flash-exp");

    let user_message = calls[0]
        .messages
        .iter()
        .find(|message| message.role == Role::User)
        .expect("expected one user message");
    let parts = user_message
        .content_parts
        .as_ref()
        .expect("image parts should be preserved");
    assert!(matches!(
        &parts[0],
        MessagePart::Text { text } if text == "describe this"
    ));
    assert!(matches!(
        &parts[1],
        MessagePart::ImageUrl { image_url } if image_url.url == "data:image/png;base64,QUJDRA=="
    ));
}

#[actix_web::test]
async fn test_gemini_generate_content_placeholder_hook_rewrites_image_parts() {
    let recording_provider = RecordingProvider::default();
    let state = create_gemini_state(&recording_provider, true).await;

    let app = test::init_service(App::new().app_data(state).route(
        "/gemini/v1beta/models/{model}:generateContent",
        web::post().to(gemini::generate_content),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/gemini/v1beta/models/gemini-2.0-flash-exp:generateContent")
        .set_json(json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        { "text": "describe this" },
                        {
                            "fileData": {
                                "fileUri": "https://example.com/cat.png"
                            }
                        }
                    ]
                }
            ]
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
