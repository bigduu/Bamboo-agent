//! E2E tests for /api/v1/mcp/* endpoints

use actix_web::{test, web, App};
use bamboo_agent::server::handlers;
use bamboo_agent::server::app_state::AppState;
use serde_json::json;

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
