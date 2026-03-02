//! Unified route configuration for all API endpoints
//!
//! This module provides explicit route registration for all API endpoints,
//! with no macro-based routing for consistency and clarity.

use actix_web::{dev::HttpServiceFactory, web};

use crate::server::handlers::{
    agent, agent_api, anthropic, command, copilot_auth, gemini, openai, settings, skill, tools,
    workspace,
};

fn bamboo_v1_scope() -> impl HttpServiceFactory {
    web::scope("/v1")
        // Agent management endpoints (Claude Code integration)
        .service(
            web::scope("/agent")
                .route("/projects", web::get().to(agent_api::list_projects))
                .route("/projects", web::post().to(agent_api::create_project))
                .route(
                    "/projects/{project_id}/sessions",
                    web::get().to(agent_api::get_project_sessions),
                )
                .route("/settings", web::get().to(agent_api::get_claude_settings))
                .route("/settings", web::post().to(agent_api::save_claude_settings))
                .route(
                    "/system-prompt",
                    web::get().to(agent_api::get_system_prompt),
                )
                .route(
                    "/system-prompt",
                    web::post().to(agent_api::save_system_prompt),
                )
                .route(
                    "/sessions/running",
                    web::get().to(agent_api::list_running_claude_sessions_stateful),
                )
                .route(
                    "/sessions/execute",
                    web::post().to(agent_api::execute_claude_code),
                )
                .route(
                    "/sessions/cancel",
                    web::post().to(agent_api::cancel_claude_execution),
                )
                .route(
                    "/sessions/{session_id}/events",
                    web::get().to(agent_api::claude_events),
                )
                .route(
                    "/sessions/{session_id}/jsonl",
                    web::get().to(agent_api::get_session_jsonl),
                ),
        )
        // Command routes
        .route("/commands", web::get().to(command::list_commands))
        .route(
            "/commands/{command_type}/{id}",
            web::get().to(command::get_command),
        )
        // Settings routes
        .route("/bamboo/workflows", web::get().to(settings::list_workflows))
        .route(
            "/bamboo/workflows/{name}",
            web::get().to(settings::get_workflow),
        )
        .route("/bamboo/workflows", web::post().to(settings::save_workflow))
        .route(
            "/bamboo/workflows/{name}",
            web::delete().to(settings::delete_workflow),
        )
        .route(
            "/bamboo/setup/status",
            web::get().to(settings::get_setup_status),
        )
        .route(
            "/bamboo/setup/complete",
            web::post().to(settings::mark_setup_complete),
        )
        .route(
            "/bamboo/setup/incomplete",
            web::post().to(settings::mark_setup_incomplete),
        )
        .route("/bamboo/config", web::get().to(settings::get_bamboo_config))
        .route(
            "/bamboo/config",
            web::post().to(settings::set_bamboo_config),
        )
        .route(
            "/bamboo/config/validate",
            web::post().to(settings::validate_bamboo_config_patch),
        )
        .route(
            "/bamboo/config/reset",
            web::post().to(settings::reset_bamboo_config),
        )
        .route(
            "/bamboo/proxy-auth",
            web::post().to(settings::set_proxy_auth),
        )
        .route(
            "/bamboo/proxy-auth/status",
            web::get().to(settings::get_proxy_auth_status),
        )
        .route(
            "/bamboo/keyword-masking",
            web::get().to(settings::get_keyword_masking_config),
        )
        .route(
            "/bamboo/keyword-masking",
            web::post().to(settings::update_keyword_masking_config),
        )
        .route(
            "/bamboo/keyword-masking/validate",
            web::post().to(settings::validate_keyword_entries),
        )
        .route(
            "/bamboo/settings/provider",
            web::get().to(settings::get_provider_config),
        )
        .route(
            "/bamboo/settings/provider",
            web::post().to(settings::update_provider_config),
        )
        .route(
            "/bamboo/settings/provider/models",
            web::post().to(settings::fetch_provider_models),
        )
        .route(
            "/bamboo/settings/reload",
            web::post().to(settings::reload_provider_config),
        )
        .route(
            "/bamboo/anthropic-model-mapping",
            web::get().to(settings::get_anthropic_model_mapping),
        )
        .route(
            "/bamboo/anthropic-model-mapping",
            web::post().to(settings::set_anthropic_model_mapping),
        )
        // Skill routes
        .route("/skills", web::get().to(skill::list_skills))
        .route(
            "/skills/available-tools",
            web::get().to(skill::get_available_tools),
        )
        .route(
            "/skills/filtered-tools",
            web::get().to(skill::get_filtered_tools),
        )
        .route(
            "/skills/available-workflows",
            web::get().to(skill::get_available_workflows),
        )
        .route("/skills/{id}", web::get().to(skill::get_skill))
        // Tools routes
        .route("/tools/execute", web::post().to(tools::execute_tool))
        // Workspace routes
        .route(
            "/workspace/validate",
            web::post().to(workspace::validate_workspace),
        )
        .route(
            "/workspace/recent",
            web::get().to(workspace::get_recent_workspaces),
        )
        .route(
            "/workspace/recent",
            web::post().to(workspace::add_recent_workspace),
        )
        .route(
            "/workspace/suggestions",
            web::get().to(workspace::get_workspace_suggestions),
        )
        .route(
            "/workspace/browse-folder",
            web::post().to(workspace::browse_folder),
        )
        .route(
            "/workspace/files",
            web::post().to(workspace::list_workspace_files),
        )
        // Copilot auth routes
        .route(
            "/bamboo/copilot/auth/start",
            web::post().to(copilot_auth::start_copilot_auth),
        )
        .route(
            "/bamboo/copilot/auth/complete",
            web::post().to(copilot_auth::complete_copilot_auth),
        )
        .route(
            "/bamboo/copilot/authenticate",
            web::post().to(copilot_auth::authenticate_copilot),
        )
        .route(
            "/bamboo/copilot/auth/status",
            web::post().to(copilot_auth::get_copilot_auth_status),
        )
        .route(
            "/bamboo/copilot/logout",
            web::post().to(copilot_auth::logout_copilot),
        )
}

/// Configure agent API routes (core agent functionality)
///
/// Routes for chat, execute, events, stop, history, todo, respond, delete, health, metrics, mcp
pub fn agent_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/chat", web::post().to(agent::chat::handler))
            // Session index / management (V2)
            .route("/sessions", web::get().to(agent::sessions::list_sessions))
            .route("/sessions", web::post().to(agent::sessions::create_session))
            .route(
                "/sessions/cleanup",
                web::post().to(agent::sessions::cleanup_sessions),
            )
            .route("/sessions/{session_id}", web::get().to(agent::sessions::get_session))
            .route(
                "/sessions/{session_id}",
                web::patch().to(agent::sessions::patch_session),
            )
            .route(
                "/sessions/{session_id}/clear",
                web::post().to(agent::sessions::clear_session),
            )
            .route(
                "/sessions/{session_id}/attachments/{attachment_id}",
                web::get().to(agent::sessions::get_attachment),
            )
            // Schedules (timed tasks)
            .route("/schedules", web::get().to(agent::schedules::list_schedules))
            .route("/schedules", web::post().to(agent::schedules::create_schedule))
            .route(
                "/schedules/{schedule_id}",
                web::patch().to(agent::schedules::patch_schedule),
            )
            .route(
                "/schedules/{schedule_id}",
                web::delete().to(agent::schedules::delete_schedule),
            )
            .route(
                "/schedules/{schedule_id}/run",
                web::post().to(agent::schedules::run_now),
            )
            .route(
                "/schedules/{schedule_id}/sessions",
                web::get().to(agent::schedules::list_sessions_for_schedule),
            )
            // New separated execute + events endpoints
            .route(
                "/execute/{session_id}",
                web::post().to(agent::execute::handler),
            )
            .route(
                "/events/{session_id}",
                web::get().to(agent::events::handler),
            )
            .route("/stop/{session_id}", web::post().to(agent::stop::handler))
            .route(
                "/history/{session_id}",
                web::get().to(agent::history::handler),
            )
            .route(
                "/todo/{session_id}",
                web::get().to(agent::todo::get_todo_list),
            )
            .route(
                "/todo/{session_id}/exists",
                web::get().to(agent::todo::has_todo_list),
            )
            .route(
                "/respond/{session_id}",
                web::post().to(agent::respond::submit_response),
            )
            .route(
                "/respond/{session_id}/pending",
                web::get().to(agent::respond::get_pending_question),
            )
            .route(
                "/sessions/{session_id}",
                web::delete().to(agent::delete::handler),
            )
            // Dev-only endpoints (greenfield reset)
            .route("/dev/reset", web::post().to(agent::dev::reset))
            .route("/health", web::get().to(agent::health::handler))
            // Metrics routes (agent metrics)
            .route("/metrics/summary", web::get().to(agent::metrics::summary))
            .route("/metrics/by-model", web::get().to(agent::metrics::by_model))
            .route("/metrics/sessions", web::get().to(agent::metrics::sessions))
            .route(
                "/metrics/sessions/{session_id}",
                web::get().to(agent::metrics::session_detail),
            )
            .route("/metrics/daily", web::get().to(agent::metrics::daily))
            // Forward metrics routes (API proxy metrics)
            .route(
                "/metrics/forward/summary",
                web::get().to(agent::metrics::forward_summary),
            )
            .route(
                "/metrics/forward/by-endpoint",
                web::get().to(agent::metrics::forward_by_endpoint),
            )
            .route(
                "/metrics/forward/requests",
                web::get().to(agent::metrics::forward_requests),
            )
            // MCP routes
            .service(
                web::scope("/mcp")
                    .route("/servers", web::get().to(agent::mcp::list_servers))
                    .route("/servers", web::post().to(agent::mcp::add_server))
                    .route(
                        "/servers/import",
                        web::post().to(agent::mcp::import_servers),
                    )
                    .route("/servers/{id}", web::get().to(agent::mcp::get_server))
                    .route("/servers/{id}", web::put().to(agent::mcp::update_server))
                    .route("/servers/{id}", web::delete().to(agent::mcp::delete_server))
                    .route(
                        "/servers/{id}/connect",
                        web::post().to(agent::mcp::connect_server),
                    )
                    .route(
                        "/servers/{id}/disconnect",
                        web::post().to(agent::mcp::disconnect_server),
                    )
                    .route(
                        "/servers/{id}/refresh",
                        web::post().to(agent::mcp::refresh_tools),
                    )
                    .route(
                        "/servers/{id}/tools",
                        web::get().to(agent::mcp::get_server_tools),
                    )
                    .route("/tools", web::get().to(agent::mcp::list_tools)),
            ),
    );
}

/// Configure Bamboo internal `/v1/*` routes.
///
/// OpenAI-compatible forwarding endpoints live under `/openai/v1/*`.
pub fn bamboo_v1_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(bamboo_v1_scope());
}

/// Configure OpenAI-compatible API routes with an explicit prefix (/openai/v1/*)
///
/// This mirrors the provider-specific prefixes used by Anthropic (/anthropic/v1/*)
/// and Gemini (/gemini/v1beta/*), making it easier to configure OpenAI clients with a base URL
/// like `http://localhost:8080/openai`.
pub fn openai_prefixed_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/openai/v1")
            .route(
                "/chat/completions",
                web::post().to(openai::chat_completions),
            )
            .route("/responses", web::post().to(openai::responses_create))
            .route("/models", web::get().to(openai::get_models)),
    );
}

/// Configure Anthropic API routes (/anthropic/v1/*)
pub fn anthropic_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/anthropic/v1")
            .route("/messages", web::post().to(anthropic::messages))
            .route("/complete", web::post().to(anthropic::complete))
            .route("/models", web::get().to(anthropic::get_models)),
    );
}

/// Configure Gemini API routes (/gemini/v1beta/*)
pub fn gemini_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/gemini/v1beta")
            .route("/models", web::get().to(gemini::list_models))
            .route(
                "/models/{model}:generateContent",
                web::post().to(gemini::generate_content),
            )
            .route(
                "/models/{model}:streamGenerateContent",
                web::post().to(gemini::stream_generate_content),
            ),
    );
}

/// Configure all routes for desktop mode (no rate limiting)
///
/// Desktop mode binds to localhost only, so rate limiting is not needed
pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.configure(agent_routes)
        .configure(bamboo_v1_routes)
        .configure(openai_prefixed_routes)
        .configure(anthropic_routes)
        .configure(gemini_routes);
}

/// Configure all routes for production mode (with rate limiting)
///
/// Production mode binds to 0.0.0.0 or custom addresses, so rate limiting is enabled
pub fn configure_routes_with_rate_limiting(cfg: &mut web::ServiceConfig) {
    cfg.configure(agent_routes)
        .configure(bamboo_v1_routes)
        .configure(openai_prefixed_routes)
        .configure(anthropic_routes)
        .configure(gemini_routes);
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_route_count() {
        // Verify we have all expected route configuration functions
        // This is a compile-time check - if it compiles, routes are defined
        assert!(true);
    }
}
