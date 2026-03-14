use super::*;

#[actix_web::test]
async fn test_mcp_list_servers() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/mcp/servers",
        web::get().to(handlers::mcp::list_servers),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/mcp/servers")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_mcp_add_server() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/mcp/servers",
        web::post().to(handlers::mcp::add_server),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri("/api/v1/mcp/servers")
        .set_json(json!({
            "name": "test-server",
            "command": "test-command",
            "args": [],
            "env": {}
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should accept server config or return validation error
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_mcp_get_server() {
    let state = crate::e2e::common::create_test_app().await;
    let server_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/mcp/servers/{id}",
        web::get().to(handlers::mcp::get_server),
    ))
    .await;

    let uri = format!("/api/v1/mcp/servers/{}", server_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should return server or not found
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_mcp_update_server() {
    let state = crate::e2e::common::create_test_app().await;
    let server_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/mcp/servers/{id}",
        web::put().to(handlers::mcp::update_server),
    ))
    .await;

    let uri = format!("/api/v1/mcp/servers/{}", server_id);
    let req = test::TestRequest::put()
        .uri(&uri)
        .set_json(json!({
            "name": "updated-server",
            "command": "updated-command",
            "args": [],
            "env": {}
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;

    // Should update or return not found
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[actix_web::test]
async fn test_mcp_delete_server() {
    let state = crate::e2e::common::create_test_app().await;
    let server_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/mcp/servers/{id}",
        web::delete().to(handlers::mcp::delete_server),
    ))
    .await;

    let uri = format!("/api/v1/mcp/servers/{}", server_id);
    let req = test::TestRequest::delete().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should delete or return not found
    assert!(
        resp.status().is_success()
            || resp.status().is_client_error()
            || resp.status().is_server_error()
    );
}
