use super::*;

#[actix_web::test]
async fn test_list_skills_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/skills", web::get().to(skill::list_skills)),
    )
    .await;

    let req = test::TestRequest::get().uri("/v1/skills").to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_list_skills_returns_json() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/skills", web::get().to(skill::list_skills)),
    )
    .await;

    let req = test::TestRequest::get().uri("/v1/skills").to_request();

    let resp = test::call_service(&app, req).await;
    let body = test::read_body(resp).await;

    // Should be valid JSON
    let result: serde_json::Value =
        serde_json::from_slice(&body).expect("Response should be valid JSON");

    // Should have skills array
    assert!(result.is_object());
    assert!(result.get("skills").is_some());
    assert!(result.get("total").is_some());
}

#[actix_web::test]
async fn test_get_skill_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/v1/skills/{id}", web::get().to(skill::get_skill)),
    )
    .await;

    // Test with a non-existent skill ID
    let req = test::TestRequest::get()
        .uri("/v1/skills/non-existent-skill-id")
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should return 404 for non-existent skill
    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}
