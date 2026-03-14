use super::*;

#[actix_web::test]
async fn test_get_system_prompt_default() {
    let _lock = crate::e2e::common::claude_fs_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/agent/system-prompt",
        web::get().to(agent_api::get_system_prompt),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/agent/system-prompt")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let prompt: Value = serde_json::from_slice(&body).expect("Failed to parse response");
    assert!(prompt["content"].is_string());
    assert!(prompt["path"].is_string());
}

#[actix_web::test]
async fn test_save_and_get_system_prompt() {
    let _lock = crate::e2e::common::claude_fs_lock();
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route(
                "/v1/agent/system-prompt",
                web::post().to(agent_api::save_system_prompt),
            )
            .route(
                "/v1/agent/system-prompt",
                web::get().to(agent_api::get_system_prompt),
            ),
    )
    .await;

    let save_req = test::TestRequest::post()
        .uri("/v1/agent/system-prompt")
        .set_json(json!({
            "content": "# Test System Prompt\n\nYou are a helpful assistant."
        }))
        .to_request();

    let save_resp = test::call_service(&app, save_req).await;
    assert!(save_resp.status().is_success());

    let get_req = test::TestRequest::get()
        .uri("/v1/agent/system-prompt")
        .to_request();

    let get_resp = test::call_service(&app, get_req).await;
    assert!(get_resp.status().is_success());

    let body = test::read_body(get_resp).await;
    let prompt: Value = serde_json::from_slice(&body).expect("Failed to parse response");

    assert!(prompt["content"].is_string());
    assert!(prompt["content"]
        .as_str()
        .expect("content should be string")
        .contains("Test System Prompt"));
}
