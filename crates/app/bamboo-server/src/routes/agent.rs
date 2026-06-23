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
    let mut scope = web::scope("/api/v1")
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
            "/sessions/{session_id}/regenerate-title",
            web::post().to(agent::sessions::regenerate_session_title),
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
        // Account-scoped change feed: one resumable SSE stream across all
        // sessions (multi-client sync, replaces session-index polling).
        .route("/stream", web::get().to(agent::stream::handler))
        .route("/stop/{session_id}", web::post().to(agent::stop::handler))
        // Phase 2: deliver a human approval decision to a child sub-agent's
        // blocked gated tool (surfaced via AgentEvent::ChildApprovalRequested).
        .route(
            "/child-approval/{child_session_id}",
            web::post().to(agent::child_approval::handler),
        )
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
        // Notification preferences (backend-owned; replaces frontend localStorage)
        .route(
            "/notifications/preferences",
            web::get().to(agent::notifications::get_preferences),
        )
        .route(
            "/notifications/preferences",
            web::put().to(agent::notifications::update_preferences),
        )
        .route(
            "/sessions/{session_id}",
            web::delete().to(agent::delete::handler),
        )
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
        .service(mcp_scope());

    // Dev-only endpoints are a greenfield wipe of ALL sessions (`dev_reset`) with
    // no auth. Register them ONLY when explicitly enabled, so a production/Docker
    // deployment (release build, no env) never exposes an unauthenticated
    // data-wipe POST. #11.
    if dev_endpoints_enabled() {
        scope = scope.route("/dev/reset", web::post().to(agent::dev::reset));
    }

    cfg.service(scope);

    // v2-P1 (#181): the unified WebSocket multiplex. A single `GET /v2/stream`
    // WS replaces the two v1 SSE streams plus a `stop` control uplink. Behind
    // the SAME access-password middleware as `/api/v1`, so `local_bypass` keeps
    // desktop loopback frictionless and public access still requires the
    // password. The v1 SSE/REST endpoints above stay unchanged (dual-track).
    // v2-P2 (#181): `/v2/pair` is on the public whitelist (a new device has no
    // credential yet) and self-gates via the owner root password in its body.
    // `/v2/stream` stays GATED by the same middleware.
    // v2-P2 (#181, slice 2): `/v2/pair/code` (request a one-time code) and the
    // `/v2/devices` management endpoints (list / revoke / rotate) are all GATED
    // by the same middleware — only `/v2/pair` itself is on the public whitelist,
    // because a brand-new device redeeming a code has no credential yet.
    let v2_scope = web::scope("/v2")
        .wrap(actix_web::middleware::from_fn(
            settings::enforce_access_password_middleware,
        ))
        .route("/pair", web::post().to(settings::pair_device))
        .route("/pair/code", web::post().to(settings::create_pairing_code))
        .route("/devices", web::get().to(settings::list_devices))
        .route(
            "/devices/{device_id}",
            web::delete().to(settings::revoke_device),
        )
        .route(
            "/devices/{device_id}/rotate",
            web::post().to(settings::rotate_device),
        )
        .route("/stream", web::get().to(agent::ws_v2::handler));
    cfg.service(v2_scope);
}

/// Whether the dev-only HTTP endpoints (e.g. `POST /api/v1/dev/reset`) should be
/// registered. OFF by default in release builds; ON in debug builds (the local
/// dev workflow) or when `BAMBOO_ENABLE_DEV_ENDPOINTS` is explicitly truthy. #11.
fn dev_endpoints_enabled() -> bool {
    cfg!(debug_assertions) || dev_endpoints_env_enabled()
}

/// The env-var half of [`dev_endpoints_enabled`], split out so it's testable
/// independently of the build profile (`cfg!(debug_assertions)` is always true
/// under `cargo test`).
fn dev_endpoints_env_enabled() -> bool {
    std::env::var("BAMBOO_ENABLE_DEV_ENDPOINTS")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod dev_gate_tests {
    use super::dev_endpoints_env_enabled;

    const VAR: &str = "BAMBOO_ENABLE_DEV_ENDPOINTS";

    // Single test so the process-global env var is mutated serially (no other test
    // reads this var). Saves/restores any pre-existing value.
    #[test]
    fn dev_endpoints_env_gate_is_off_by_default_and_only_on_for_truthy_values() {
        let saved = std::env::var(VAR).ok();

        std::env::remove_var(VAR);
        assert!(
            !dev_endpoints_env_enabled(),
            "unset -> dev endpoints OFF (production default)"
        );

        for off in ["0", "false", "no", "off", "", "  ", "maybe"] {
            std::env::set_var(VAR, off);
            assert!(
                !dev_endpoints_env_enabled(),
                "{off:?} must NOT enable dev endpoints"
            );
        }

        for on in [
            "1", "true", "TRUE", "True", "yes", "Yes", "on", "ON", " on ",
        ] {
            std::env::set_var(VAR, on);
            assert!(
                dev_endpoints_env_enabled(),
                "{on:?} must enable dev endpoints"
            );
        }

        match saved {
            Some(v) => std::env::set_var(VAR, v),
            None => std::env::remove_var(VAR),
        }
    }
}
