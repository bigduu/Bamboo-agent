use super::*;

#[actix_web::test]
async fn test_models_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/models", web::get().to(openai::get_models)),
    )
    .await;

    let req = test::TestRequest::get().uri("/v1/models").to_request();

    let resp = test::call_service(&app, req).await;

    // The endpoint should respond successfully
    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_models_returns_list() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/models", web::get().to(openai::get_models)),
    )
    .await;

    let req = test::TestRequest::get().uri("/v1/models").to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;

    // Should be valid JSON
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Should have OpenAI-compatible structure
    assert!(result.is_object());
    assert_eq!(result.get("object").unwrap(), "list");
    assert!(result.get("data").is_some());
    assert!(result["data"].is_array());

    // If there are models, check their structure
    if let Some(models) = result["data"].as_array() {
        for model in models {
            assert!(model.get("id").is_some());
            assert_eq!(model.get("object").unwrap(), "model");
            assert!(model.get("created").is_some());
            assert!(model.get("owned_by").is_some());
        }
    }
}

#[actix_web::test]
async fn test_models_endpoint_method_not_allowed() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/models", web::get().to(openai::get_models)),
    )
    .await;

    // Try POST request to GET-only endpoint
    let req = test::TestRequest::post().uri("/v1/models").to_request();

    let resp = test::call_service(&app, req).await;

    // Actix-web returns 404 when route doesn't match the method
    // This is expected behavior - the route exists but only for GET
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}
