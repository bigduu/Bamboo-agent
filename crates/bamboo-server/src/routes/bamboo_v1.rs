use actix_web::{dev::HttpServiceFactory, web};

use crate::handlers::{
    command, copilot_auth, settings, skill, subagent_profiles, tools, workspace,
};

fn bamboo_v1_scope() -> impl HttpServiceFactory {
    web::scope("/v1")
        .wrap(actix_web::middleware::from_fn(
            settings::enforce_access_password_middleware,
        ))
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
        // Subagent profile routes
        .route(
            "/subagent_profiles",
            web::get().to(subagent_profiles::list_subagent_profiles),
        )
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
}

/// Configure Bamboo internal `/v1/*` routes.
///
/// OpenAI-compatible forwarding endpoints live under `/openai/v1/*`.
pub fn bamboo_v1_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(bamboo_v1_scope());
}
