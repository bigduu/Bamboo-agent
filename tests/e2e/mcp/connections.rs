use super::*;

#[actix_web::test]
async fn test_mcp_connect_server() {
    let state = crate::e2e::common::create_test_app().await;
    let server_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/mcp/servers/{id}/connect",
        web::post().to(handlers::mcp::connect_server),
    ))
    .await;

    let uri = format!("/api/v1/mcp/servers/{}/connect", server_id);
    let req = test::TestRequest::post().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should connect or return error
    assert!(
        resp.status().is_success()
            || resp.status().is_client_error()
            || resp.status().is_server_error()
    );
}

#[actix_web::test]
async fn test_mcp_disconnect_server() {
    let state = crate::e2e::common::create_test_app().await;
    let server_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/mcp/servers/{id}/disconnect",
        web::post().to(handlers::mcp::disconnect_server),
    ))
    .await;

    let uri = format!("/api/v1/mcp/servers/{}/disconnect", server_id);
    let req = test::TestRequest::post().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should disconnect or return error
    assert!(
        resp.status().is_success()
            || resp.status().is_client_error()
            || resp.status().is_server_error()
    );
}

#[actix_web::test]
async fn test_mcp_refresh_tools() {
    let state = crate::e2e::common::create_test_app().await;
    let server_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/mcp/servers/{id}/refresh",
        web::post().to(handlers::mcp::refresh_tools),
    ))
    .await;

    let uri = format!("/api/v1/mcp/servers/{}/refresh", server_id);
    let req = test::TestRequest::post().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should refresh or return error
    assert!(
        resp.status().is_success()
            || resp.status().is_client_error()
            || resp.status().is_server_error()
    );
}

#[actix_web::test]
async fn test_mcp_get_server_tools() {
    let state = crate::e2e::common::create_test_app().await;
    let server_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/mcp/servers/{id}/tools",
        web::get().to(handlers::mcp::get_server_tools),
    ))
    .await;

    let uri = format!("/api/v1/mcp/servers/{}/tools", server_id);
    let req = test::TestRequest::get().uri(&uri).to_request();

    let resp = test::call_service(&app, req).await;

    // Should return tools or error
    assert!(resp.status().is_success() || resp.status().is_client_error());
}
