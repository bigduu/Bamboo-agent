use super::*;

#[actix_web::test]
async fn test_save_workflow_with_invalid_name() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/workflows",
        web::post().to(settings::save_workflow),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/v1/bamboo/workflows")
        .set_json(json!({
            "name": "../../../etc/passwd",
            "content": "malicious content"
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn test_delete_nonexistent_workflow() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/v1/bamboo/workflows/{name}",
        web::delete().to(settings::delete_workflow),
    ))
    .await;

    let req = test::TestRequest::delete()
        .uri("/v1/bamboo/workflows/nonexistent-workflow")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}
