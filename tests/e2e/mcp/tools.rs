use super::*;

#[actix_web::test]
async fn test_mcp_list_tools() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/mcp/tools",
        web::get().to(handlers::mcp::list_tools),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/mcp/tools")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}
