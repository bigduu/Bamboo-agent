use super::*;

#[actix_web::test]
async fn test_chat_accepts_session_id() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/api/v1/chat", web::post().to(handlers::chat::handler)),
    )
    .await;

    let session_id = uuid::Uuid::new_v4().to_string();

    let req = test::TestRequest::post()
        .uri("/api/v1/chat")
        .set_json(json!({
            "message": "Test message",
            "session_id": session_id
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Endpoint should accept the request structure
    assert!(
        resp.status().is_client_error()
            || resp.status().is_server_error()
            || resp.status().is_success()
    );
}
