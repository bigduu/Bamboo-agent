use super::*;

#[actix_web::test]
async fn test_get_keyword_masking_config() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/keyword-masking",
        web::get().to(settings::get_keyword_masking_config),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/v1/bamboo/keyword-masking")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body(resp).await;
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    assert!(result.get("entries").is_some());
    assert!(result["entries"].is_array());
}
