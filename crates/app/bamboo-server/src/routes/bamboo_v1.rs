use actix_web::{dev::HttpServiceFactory, web};

use crate::handlers::{command, copilot_auth, settings, skill, tools, workflow_runs, workspace};

/// Builds the full Bamboo internal route table (commands / settings / skills /
/// tools / workspace / copilot-auth / provider-catalog / provider-instances /
/// cluster-fabric) as a set of routes relative to whatever scope mounts them —
/// this factory adds NO path prefix and NO middleware wrap of its own.
///
/// Nested TWICE (#251 finding 1):
///   - inside `routes::agent::agent_routes`'s single `/api/v1` scope (the
///     canonical mount — see that function's `.service(bamboo_v1::bamboo_relative_routes())`
///     call), so it inherits that scope's prefix AND its
///     `enforce_access_password_middleware` wrap;
///   - inside [`bamboo_v1_routes`]'s own `/v1` scope below, as a permanent
///     back-compat alias for existing clients (Lotus, bamboo CLI/SDK, magpie)
///     that still call bare `/v1/*`.
///
/// Deliberately NOT a second top-level `Scope("/api/v1")`: actix-web routes a
/// request to the FIRST registered `Scope` whose prefix matches and then hands
/// it the whole sub-tree — an unmatched path inside that scope 404s directly,
/// it does not fall through to try a sibling scope with an identical prefix.
/// Two competing `Scope("/api/v1")` registrations would therefore silently
/// shadow one other (this exact bug was caught by
/// `routes::tests::bamboo_v1_routes_resolve_under_both_canonical_and_legacy_prefix`
/// during development). Nesting inside the ONE scope `agent_routes` already
/// owns is the only pattern that works here.
pub(crate) fn bamboo_relative_routes() -> impl HttpServiceFactory {
    web::scope("")
        // Command routes
        .route("/commands", web::get().to(command::list_commands))
        .route(
            "/commands/{command_type}/{id}",
            web::get().to(command::get_command),
        )
        // Settings routes
        .route(
            "/bamboo/workflow-catalog",
            web::get().to(settings::list_workflow_catalog),
        )
        .route(
            "/bamboo/workflow-catalog/{workflow_id}/migrate",
            web::post().to(settings::migrate_workflow),
        )
        .route(
            "/sessions/{session_id}/workflow-runs",
            web::post().to(workflow_runs::start),
        )
        .route(
            "/sessions/{session_id}/workflow-runs",
            web::get().to(workflow_runs::list),
        )
        .route(
            "/sessions/{session_id}/workflow-runs/{run_id}",
            web::get().to(workflow_runs::get),
        )
        .route(
            "/sessions/{session_id}/workflow-runs/{run_id}/events",
            web::get().to(workflow_runs::events),
        )
        .route(
            "/sessions/{session_id}/workflow-runs/{run_id}/cancel",
            web::post().to(workflow_runs::cancel),
        )
        .route(
            "/sessions/{session_id}/workflow-runs/{run_id}/restart",
            web::post().to(workflow_runs::restart),
        )
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
            "/bamboo/access/status",
            web::get().to(settings::get_access_status),
        )
        .route(
            "/bamboo/access/verify",
            web::post().to(settings::verify_access_password),
        )
        .route(
            "/bamboo/access/password",
            web::post().to(settings::update_access_password),
        )
        .route(
            "/bamboo/model-limits/defaults",
            web::get().to(settings::get_model_limit_defaults),
        )
        .route(
            "/bamboo/config/validate",
            web::post().to(settings::validate_bamboo_config_patch),
        )
        .route(
            "/bamboo/config/codex/detect",
            web::post().to(settings::detect_codex_cli),
        )
        .route(
            "/bamboo/hooks/test",
            web::post().to(settings::test_lifecycle_hook),
        )
        .route(
            "/bamboo/config/reset",
            web::post().to(settings::reset_bamboo_config),
        )
        .route(
            "/bamboo/config/recovery-status",
            web::get().to(settings::get_config_recovery_status),
        )
        .route(
            "/bamboo/config/recovery/confirm",
            web::post().to(settings::confirm_config_recovery),
        )
        .route(
            "/bamboo/config/live-health",
            web::get().to(settings::get_live_config_health),
        )
        .route(
            "/bamboo/config/sections/providers",
            web::get().to(settings::get_provider_section),
        )
        .route(
            "/bamboo/config/sections/providers",
            web::put().to(settings::put_provider_section),
        )
        .route(
            "/bamboo/config/sections/mcp",
            web::get().to(settings::get_mcp_section),
        )
        .route(
            "/bamboo/config/sections/mcp",
            web::put().to(settings::put_mcp_section),
        )
        .route(
            "/bamboo/config/sections/{section}",
            web::get().to(settings::get_typed_section),
        )
        .route(
            "/bamboo/config/sections/{section}",
            web::put().to(settings::put_typed_section),
        )
        .route(
            "/bamboo/config/credentials",
            web::get().to(settings::list_credentials),
        )
        .route(
            "/bamboo/config/credentials/{credential_ref}",
            web::get().to(settings::get_credential_status),
        )
        .route(
            "/bamboo/config/credentials/{credential_ref}",
            web::put().to(settings::replace_credential),
        )
        .route(
            "/bamboo/config/credentials/{credential_ref}/clear",
            web::post().to(settings::clear_credential),
        )
        .route(
            "/bamboo/config/notifications",
            web::get().to(settings::get_notification_config),
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
            "/bamboo/permission/ask-rules",
            web::get().to(settings::get_permission_ask_rules),
        )
        .route(
            "/bamboo/permission/ask-rules",
            web::put().to(settings::update_permission_ask_rules),
        )
        .route(
            "/bamboo/permission/policy",
            web::get().to(settings::get_permission_policy),
        )
        .route(
            "/bamboo/permission/rules",
            web::get().to(settings::get_permission_policy),
        )
        .route(
            "/bamboo/permission/rules",
            web::post().to(settings::create_permission_rule),
        )
        .route(
            "/bamboo/permission/rules/{rule_id}",
            web::put().to(settings::update_permission_rule),
        )
        .route(
            "/bamboo/permission/rules/{rule_id}",
            web::delete().to(settings::delete_permission_rule),
        )
        .route(
            "/bamboo/permission/diagnose",
            web::post().to(settings::diagnose_permission),
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
        .route("/bamboo/tools", web::get().to(settings::get_bamboo_tools))
        // Env vars routes
        .route("/bamboo/env-vars", web::get().to(settings::list_env_vars))
        .route("/bamboo/env-vars", web::post().to(settings::upsert_env_var))
        .route(
            "/bamboo/env-vars/replace",
            web::post().to(settings::replace_env_vars),
        )
        .route(
            "/bamboo/env-vars/{name}",
            web::delete().to(settings::delete_env_var),
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
        // Provider catalog routes
        .route(
            "/bamboo/provider-catalog",
            web::get().to(settings::get_provider_catalog),
        )
        .route(
            "/bamboo/provider-catalog/fetch-models",
            web::post().to(settings::fetch_catalog_models),
        )
        // ── Provider Instance CRUD ────────────────────────────────────
        .route(
            "/bamboo/settings/provider-instances",
            web::get().to(settings::list_provider_instances),
        )
        .route(
            "/bamboo/settings/provider-instances",
            web::post().to(settings::create_provider_instance),
        )
        .route(
            "/bamboo/settings/provider-instances/{instance_id}",
            web::put().to(settings::update_provider_instance),
        )
        .route(
            "/bamboo/settings/provider-instances/{instance_id}",
            web::delete().to(settings::delete_provider_instance),
        )
        .route(
            "/bamboo/settings/provider-instances/default",
            web::post().to(settings::set_default_provider_instance),
        )
        // ── Cluster Fabric: nodes & clusters ──────────────────────────
        .route(
            "/bamboo/settings/nodes",
            web::get().to(settings::list_nodes),
        )
        .route(
            "/bamboo/settings/nodes",
            web::post().to(settings::create_node),
        )
        .route(
            "/bamboo/settings/nodes/{id}",
            web::get().to(settings::get_node),
        )
        .route(
            "/bamboo/settings/nodes/{id}",
            web::put().to(settings::update_node),
        )
        .route(
            "/bamboo/settings/nodes/{id}",
            web::delete().to(settings::delete_node),
        )
        .route(
            "/bamboo/settings/nodes/{id}/test",
            web::post().to(settings::node_test),
        )
        .route(
            "/bamboo/settings/nodes/{id}/deploy",
            web::post().to(settings::node_deploy),
        )
        .route(
            "/bamboo/settings/nodes/{id}/stop",
            web::post().to(settings::node_stop),
        )
        .route(
            "/bamboo/settings/nodes/{id}/status",
            web::get().to(settings::node_status),
        )
        .route(
            "/bamboo/settings/nodes/{id}/logs",
            web::get().to(settings::node_logs),
        )
        .route(
            "/bamboo/settings/clusters",
            web::post().to(settings::create_cluster),
        )
        .route(
            "/bamboo/settings/clusters/{name}",
            web::put().to(settings::update_cluster),
        )
        .route(
            "/bamboo/settings/clusters/{name}",
            web::delete().to(settings::delete_cluster),
        )
}

/// Configure the legacy `/v1/*` alias for Bamboo's internal routes.
///
/// Three native-API prefixes used to coexist (`/api/v1` for the agent surface,
/// bare `/v1` for settings/skills/tools/workspace/copilot/cluster, `/v2` for
/// pairing/devices/ws) — pure historical drift for the first two, since both
/// are bamboo's own API and nothing distinguishes them by version or
/// capability. `/api/v1` is now canonical for ALL of bamboo's native REST
/// surface (mounted by `routes::agent::agent_routes`, which nests
/// [`bamboo_relative_routes`] into its own `/api/v1` scope); `/v1` here is
/// kept mounted as a permanent back-compat alias (not scheduled for removal —
/// no deprecation window has been announced to consumers). `/v2` is a
/// distinct, intentionally-versioned newer generation (the WS multiplex +
/// device pairing) and is unaffected by this alias. #251 (finding 1).
///
/// OpenAI-compatible forwarding endpoints live under `/openai/v1/*`.
pub fn bamboo_v1_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/v1")
            .wrap(actix_web::middleware::from_fn(
                settings::enforce_access_password_middleware,
            ))
            .service(bamboo_relative_routes()),
    );
}
