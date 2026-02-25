//! Regression tests for Actix-web `Data<T>` wiring.
//!
//! These tests ensure handlers only extract the unified `AppState` registered by the server.
//! If a handler accidentally adds an extra `web::Data<...>` (e.g. a legacy AppState),
//! Actix will return 500 before reaching JSON extraction/validation.

use actix_web::{test, App};
use bamboo_agent::server::configure_routes;

#[actix_web::test]
async fn test_anthropic_messages_does_not_require_extra_app_state() {
    let state = crate::e2e::common::create_test_app().await;
    let app = test::init_service(App::new().app_data(state).configure(configure_routes)).await;

    // No JSON body: if `Data<AppState>` extraction works, this should be a 4xx from JSON extractor.
    // If state wiring is broken (missing/wrong Data type), Actix returns 500.
    let req = test::TestRequest::post()
        .uri("/anthropic/v1/messages")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_client_error(),
        "expected 4xx, got {}",
        resp.status()
    );
}

#[actix_web::test]
async fn test_openai_chat_completions_does_not_require_extra_app_state() {
    let state = crate::e2e::common::create_test_app().await;
    let app = test::init_service(App::new().app_data(state).configure(configure_routes)).await;

    let req = test::TestRequest::post()
        .uri("/v1/chat/completions")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(
        resp.status().is_client_error(),
        "expected 4xx, got {}",
        resp.status()
    );
}
