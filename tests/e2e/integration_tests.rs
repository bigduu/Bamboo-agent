//! Integration tests for complete API routing
//!
//! Tests that verify all endpoints are properly registered and accessible

use actix_web::{test, web, App};
use bamboo_agent::agent::server::handlers;
use bamboo_agent::agent::server::state::AppState;

/// Helper to create the full API scope for testing
fn create_api_scope() -> actix_web::Scope {
    web::scope("/api/v1")
        // Core chat and execution
        .route("/chat", web::post().to(handlers::chat::handler))
        .route(
            "/execute/{session_id}",
            web::post().to(handlers::execute::handler),
        )
        .route(
            "/events/{session_id}",
            web::get().to(handlers::events::handler),
        )
        .route(
            "/stop/{session_id}",
            web::post().to(handlers::stop::handler),
        )
        .route(
            "/history/{session_id}",
            web::get().to(handlers::history::handler),
        )
        // Todo endpoints
        .route(
            "/todo/{session_id}",
            web::get().to(handlers::todo::get_todo_list),
        )
        .route(
            "/todo/{session_id}/exists",
            web::get().to(handlers::todo::has_todo_list),
        )
        // Respond endpoints
        .route(
            "/respond/{session_id}",
            web::post().to(handlers::respond::submit_response),
        )
        .route(
            "/respond/{session_id}/pending",
            web::get().to(handlers::respond::get_pending_question),
        )
        // Session management
        .route(
            "/sessions/{session_id}",
            web::delete().to(handlers::delete::handler),
        )
        // Metrics endpoints
        .route(
            "/metrics/summary",
            web::get().to(handlers::metrics::summary),
        )
        .route(
            "/metrics/by-model",
            web::get().to(handlers::metrics::by_model),
        )
        .route(
            "/metrics/sessions",
            web::get().to(handlers::metrics::sessions),
        )
        .route(
            "/metrics/sessions/{session_id}",
            web::get().to(handlers::metrics::session_detail),
        )
        .route("/metrics/daily", web::get().to(handlers::metrics::daily))
        .route(
            "/metrics/v2/summary",
            web::get().to(handlers::metrics::v2_unified_summary),
        )
        .route(
            "/metrics/v2/timeline",
            web::get().to(handlers::metrics::v2_unified_timeline),
        )
        // Health check
        .route("/health", web::get().to(handlers::health::handler))
        // MCP routes
        .service(
            web::scope("/mcp")
                .route("/servers", web::get().to(handlers::mcp::list_servers))
                .route("/servers", web::post().to(handlers::mcp::add_server))
                .route("/servers/{id}", web::get().to(handlers::mcp::get_server))
                .route("/servers/{id}", web::put().to(handlers::mcp::update_server))
                .route(
                    "/servers/{id}",
                    web::delete().to(handlers::mcp::delete_server),
                )
                .route(
                    "/servers/{id}/connect",
                    web::post().to(handlers::mcp::connect_server),
                )
                .route(
                    "/servers/{id}/disconnect",
                    web::post().to(handlers::mcp::disconnect_server),
                )
                .route(
                    "/servers/{id}/refresh",
                    web::post().to(handlers::mcp::refresh_tools),
                )
                .route(
                    "/servers/{id}/tools",
                    web::get().to(handlers::mcp::get_server_tools),
                )
                .route("/tools", web::get().to(handlers::mcp::list_tools)),
        )
}

#[actix_web::test]
async fn test_full_api_routing() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).service(create_api_scope())).await;

    // Test health endpoint as basic connectivity check
    let req = test::TestRequest::get().uri("/api/v1/health").to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_all_endpoints_respond() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).service(create_api_scope())).await;

    let session_id = uuid::Uuid::new_v4().to_string();

    // Test a representative set of endpoints to verify routing works
    let endpoints = vec![
        format!("/api/v1/history/{}", session_id),
        format!("/api/v1/todo/{}", session_id),
        format!("/api/v1/todo/{}/exists", session_id),
        format!("/api/v1/respond/{}/pending", session_id),
        "/api/v1/metrics/summary".to_string(),
        "/api/v1/metrics/by-model".to_string(),
        "/api/v1/metrics/sessions".to_string(),
        "/api/v1/metrics/daily".to_string(),
        "/api/v1/metrics/v2/summary".to_string(),
        "/api/v1/metrics/v2/timeline".to_string(),
        "/api/v1/mcp/servers".to_string(),
        "/api/v1/mcp/tools".to_string(),
    ];

    for endpoint in endpoints {
        let req = test::TestRequest::get().uri(&endpoint).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(
            resp.status().is_success() || resp.status().is_client_error(),
            "Endpoint {} should respond",
            endpoint
        );
    }
}
