//! Integration tests for complete API routing.
//!
//! Tests that verify all endpoints are properly registered and accessible.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers;

mod routing;

/// Helper to create the full API scope for testing.
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
        // Task endpoints
        .route(
            "/task/{session_id}",
            web::get().to(handlers::task::get_task_list),
        )
        .route(
            "/task/{session_id}/exists",
            web::get().to(handlers::task::has_task_list),
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
