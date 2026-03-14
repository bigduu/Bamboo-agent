use super::*;

#[actix_web::test]
async fn test_skills_endpoints_with_query_params() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/v1/skills", web::get().to(skill::list_skills))
            .route(
                "/v1/skills/filtered-tools",
                web::get().to(skill::get_filtered_tools),
            ),
    )
    .await;

    // Test list_skills with query params
    let req = test::TestRequest::get()
        .uri("/v1/skills?category=test&search=query&refresh=true")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    // Test filtered-tools with query params
    for query in ["chat_id=test-chat", "session_id=test-session"] {
        let req = test::TestRequest::get()
            .uri(&format!("/v1/skills/filtered-tools?{query}"))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
    }
}
