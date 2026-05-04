use actix_web::{dev::HttpServiceFactory, web};

use crate::handlers::{agent, settings};

fn mcp_scope() -> impl HttpServiceFactory {
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
        .route("/tools", web::get().to(agent::mcp::list_tools))
}

/// Configure agent API routes (core agent functionality)
///
/// Routes for chat, execute, events, stop, history, task, respond, delete, health, metrics, mcp
pub fn agent_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .wrap(actix_web::middleware::from_fn(
                settings::enforce_access_password_middleware,
            ))
            .route("/chat", web::post().to(agent::chat::handler))
            .route(
                "/prompt-presets",
                web::get().to(agent::prompt_presets::list_prompt_presets),
            )
            .route(
                "/prompt-presets",
                web::post().to(agent::prompt_presets::create_prompt_preset),
            )
            .route(
                "/prompt-presets/{preset_id}",
                web::patch().to(agent::prompt_presets::patch_prompt_preset),
            )
            .route(
                "/prompt-presets/{preset_id}",
                web::delete().to(agent::prompt_presets::delete_prompt_preset),
            )
            // Session index / management (V2)
            .route(
                "/runs/active",
                web::get().to(agent::sessions::running_sessions_snapshot),
            )
            .route("/sessions", web::get().to(agent::sessions::list_sessions))
            .route("/sessions", web::post().to(agent::sessions::create_session))
            .route(
                "/sessions/cleanup",
                web::post().to(agent::sessions::cleanup_sessions),
            )
            .route(
                "/sessions/{session_id}",
                web::get().to(agent::sessions::get_session),
            )
            .route(
                "/sessions/{session_id}/system-prompt",
                web::get().to(agent::sessions::get_system_prompt_snapshot),
            )
            .route(
                "/sessions/{session_id}/discoverable-tools",
                web::get().to(agent::sessions::list_discoverable_tools),
            )
            .route(
                "/sessions/{session_id}/discoverable-tools",
                web::post().to(agent::sessions::activate_discoverable_tools),
            )
            .route(
                "/sessions/{session_id}/discoverable-tools",
                web::delete().to(agent::sessions::deactivate_discoverable_tools),
            )
            .route(
                "/sessions/{session_id}",
                web::patch().to(agent::sessions::patch_session),
            )
            .route(
                "/sessions/{session_id}/clear",
                web::post().to(agent::sessions::clear_session),
            )
            .route(
                "/sessions/{session_id}/project-dream/run",
                web::post().to(agent::sessions::run_project_dream),
            )
            // Message management
            .route(
                "/sessions/{session_id}/messages/truncate",
                web::post().to(agent::messages::truncate_messages),
            )
            .route(
                "/sessions/{session_id}/restore",
                web::post().to(agent::messages::restore_session_state),
            )
            .route(
                "/sessions/{session_id}/messages/{message_id}",
                web::patch().to(agent::messages::patch_message),
            )
            .route(
                "/sessions/{session_id}/messages/{message_id}",
                web::delete().to(agent::messages::delete_message),
            )
            .route(
                "/sessions/{session_id}/attachments/{attachment_id}",
                web::get().to(agent::sessions::get_attachment),
            )
            // Schedules (timed tasks)
            .route(
                "/schedules",
                web::get().to(agent::schedules::list_schedules),
            )
            .route(
                "/schedules",
                web::post().to(agent::schedules::create_schedule),
            )
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
            .route(
                "/schedules/{schedule_id}/runs",
                web::get().to(agent::schedules::list_runs_for_schedule),
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
                "/task/{session_id}",
                web::get().to(agent::task::get_task_list),
            )
            .route(
                "/task/{session_id}/exists",
                web::get().to(agent::task::has_task_list),
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
            .route(
                "/metrics/usage-breakdown",
                web::get().to(agent::metrics::usage_breakdown),
            )
            .route(
                "/metrics/memory/summary",
                web::get().to(agent::metrics::memory_summary),
            )
            .route(
                "/metrics/memory/timeline",
                web::get().to(agent::metrics::memory_timeline),
            )
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
            .route(
                "/metrics/v2/summary",
                web::get().to(agent::metrics::v2_unified_summary),
            )
            .route(
                "/metrics/v2/timeline",
                web::get().to(agent::metrics::v2_unified_timeline),
            )
            // MCP routes
            .service(mcp_scope()),
    );
}
