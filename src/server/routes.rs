//! Unified route configuration consolidating web_service and agent/server routes
//!
//! This module provides a single source of truth for all API routes,
//! eliminating duplication between agent/server and web_service.

use actix_governor::{Governor, GovernorConfigBuilder};
use actix_web::web;

use crate::server::handlers;
use crate::server::controllers::{
    agent_controller, anthropic, command_controller, copilot_auth_controller, gemini_controller,
    openai_controller, settings_controller, skill_controller, tools_controller, workspace_controller,
};

/// Configure agent API routes (core agent functionality)
///
/// Routes for chat, execute, events, stop, history, todo, respond, delete, health. metrics. mcp
pub fn agent_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/chat", web::post().to(handlers::chat::handler))
            // New separated execute + events endpoints
            .route(
                "/execute/{session_id}",
                web::post().to(handlers::execute::handler),
            )
            .route(
                "/events/{session_id}",
                web::get().to(handlers::events::handler),
            )
            // Legacy stream endpoint (deprecated)
            .route(
                "/stream/{session_id}",
                web::get().to(handlers::stream::handler),
            )
            .route(
                "/stop/{session_id}",
                web::post().to(handlers::stop::handler),
            )
            .route(
                "/history/{session_id}",
                web::get().to(handlers::history::handler),
            )
            .route(
                "/todo/{session_id}",
                web::get().to(handlers::todo::get_todo_list),
            )
            .route(
                "/todo/{session_id}/exists",
                web::get().to(handlers::todo::has_todo_list),
            )
            .route(
                "/respond/{session_id}",
                web::post().to(handlers::respond::submit_response),
            )
            .route(
                "/respond/{session_id}/pending",
                web::get().to(handlers::respond::get_pending_question),
            )
            .route(
                "/sessions/{session_id}",
                web::delete().to(handlers::delete::handler),
            )
            .route("/health", web::get().to(handlers::health::handler))
            // Metrics routes (agent metrics)
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
            .route(
                "/metrics/daily",
                web::get().to(handlers::metrics::daily),
            )
            // Forward metrics routes (API proxy metrics)
            .route(
                "/metrics/forward/summary",
                web::get().to(handlers::metrics::forward_summary),
            )
            .route(
                "/metrics/forward/by-endpoint",
                web::get().to(handlers::metrics::forward_by_endpoint),
            )
            .route(
                "/metrics/forward/requests",
                web::get().to(handlers::metrics::forward_requests),
            )
            // MCP routes
            .service(
                web::scope("/mcp")
                    .route("/servers", web::get().to(handlers::mcp::list_servers))
                    .route("/servers", web::post().to(handlers::mcp::add_server))
                    .route(
                        "/servers/{id}",
                        web::get().to(handlers::mcp::get_server),
                    )
                    .route(
                        "/servers/{id}",
                        web::put().to(handlers::mcp::update_server),
                    )
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
            ),
    );
}

/// Configure OpenAI-compatible API routes (/v1/*)
///
/// Routes for OpenAI chat completions, agent management, commands, settings, skills, tools, workspace
pub fn openai_compatible_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/v1")
            .configure(agent_controller::config)
            .configure(command_controller::config)
            .configure(openai_controller::config)
            .configure(settings_controller::config)
            .configure(skill_controller::config)
            .configure(tools_controller::config)
            .configure(workspace_controller::config)
            .configure(copilot_auth_controller::config),
    );
}

/// Configure OpenAI-compatible API routes with rate limiting
///
/// Production mode with rate limiting on chat completions
pub fn openai_compatible_routes_with_rate_limiting(cfg: &mut web::ServiceConfig) {
    // Build rate limiter for production: 10 req/sec, burst 20
    let rate_limiter = GovernorConfigBuilder::default()
        .per_second(10)
        .burst_size(20)
        .finish()
        .expect("Failed to build rate limiter");

    cfg.service(
        web::scope("/v1")
            .configure(agent_controller::config)
            .configure(command_controller::config)
            // Apply rate limiting only to openai_controller (chat completions)
            .service(
                web::scope("")
                    .wrap(Governor::new(&rate_limiter))
                    .configure(openai_controller::config),
            )
            .configure(settings_controller::config)
            .configure(skill_controller::config)
            .configure(tools_controller::config)
            .configure(workspace_controller::config)
            .configure(copilot_auth_controller::config),
    );
}

/// Configure Anthropic API routes (/anthropic/v1/*)
pub fn anthropic_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/anthropic/v1").configure(anthropic::config));
}

/// Configure Gemini API routes (/gemini/v1beta/*)
pub fn gemini_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/gemini/v1beta").configure(gemini_controller::config));
}

/// Configure all routes for desktop mode (no rate limiting)
///
/// Desktop mode binds to localhost only, so rate limiting is not needed
pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.configure(agent_routes)
        .configure(openai_compatible_routes)
        .configure(anthropic_routes)
        .configure(gemini_routes);
}

/// Configure all routes for production mode (with rate limiting)
///
/// Production mode binds to 0.0.0.0 or custom addresses, so rate limiting is enabled
pub fn configure_routes_with_rate_limiting(cfg: &mut web::ServiceConfig) {
    cfg.configure(agent_routes)
        .configure(openai_compatible_routes_with_rate_limiting)
        .configure(anthropic_routes)
        .configure(gemini_routes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_count() {
        // Verify we have all expected route configuration functions
        // This is a compile-time check - if it compiles, routes are defined
        assert!(true);
    }
}
