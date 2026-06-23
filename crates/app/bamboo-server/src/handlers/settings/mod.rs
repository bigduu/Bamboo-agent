//! Settings and configuration handlers.
//!
//! This module is split by domain so each area can evolve independently
//! without growing a single monolithic file.

mod access_control;
mod bamboo_config;
mod env_vars;
mod keyword_masking;
mod permission;
mod provider;
mod provider_instances;
mod redaction;
mod setup;
mod workflows;

#[cfg(test)]
pub(crate) use access_control::issue_device_token;
pub use access_control::{
    create_pairing_code, enforce_access_password_middleware, get_access_status, list_devices,
    pair_device, revoke_device, rotate_device, update_access_password, verify_access_password,
    PairingCodeEntry, PairingCodeGuard, RootPasswordGuard,
};
pub(crate) use access_control::{request_is_authorized, verify_device_token};
pub use bamboo_config::{
    get_bamboo_config, get_bamboo_tools, get_model_limit_defaults, get_proxy_auth_status,
    reset_bamboo_config, set_bamboo_config, set_proxy_auth, validate_bamboo_config_patch,
    ProxyAuthPayload,
};
pub use env_vars::{delete_env_var, list_env_vars, replace_env_vars, upsert_env_var};
pub use keyword_masking::{
    get_keyword_masking_config, update_keyword_masking_config, validate_keyword_entries,
};
pub use permission::{get_permission_ask_rules, update_permission_ask_rules};
pub use provider::{
    fetch_catalog_models, fetch_provider_models, get_provider_catalog, get_provider_config,
    reload_provider_config, update_provider_config, UpdateProviderRequest,
};
pub use provider_instances::{
    create_provider_instance, delete_provider_instance, list_provider_instances,
    set_default_provider_instance, update_provider_instance,
};
pub use redaction::{redact_config_for_api, redact_providers_for_api};
pub use setup::{get_setup_status, mark_setup_complete, mark_setup_incomplete};
pub(crate) use workflows::is_safe_workflow_name;
pub use workflows::{
    delete_workflow, get_workflow, list_workflows, save_workflow, SaveWorkflowRequest,
};

use actix_web::web;

/// Configures settings-related routes.
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/bamboo/workflows", web::get().to(list_workflows))
        .route("/bamboo/workflows/{name}", web::get().to(get_workflow))
        .route("/bamboo/workflows", web::post().to(save_workflow))
        .route(
            "/bamboo/workflows/{name}",
            web::delete().to(delete_workflow),
        )
        .route("/bamboo/setup/status", web::get().to(get_setup_status))
        .route(
            "/bamboo/setup/complete",
            web::post().to(mark_setup_complete),
        )
        .route(
            "/bamboo/setup/incomplete",
            web::post().to(mark_setup_incomplete),
        )
        .route("/bamboo/config", web::get().to(get_bamboo_config))
        .route("/bamboo/config", web::post().to(set_bamboo_config))
        .route("/bamboo/access/status", web::get().to(get_access_status))
        .route(
            "/bamboo/access/verify",
            web::post().to(verify_access_password),
        )
        .route(
            "/bamboo/access/password",
            web::post().to(update_access_password),
        )
        .route(
            "/bamboo/model-limits/defaults",
            web::get().to(get_model_limit_defaults),
        )
        .route(
            "/bamboo/config/validate",
            web::post().to(validate_bamboo_config_patch),
        )
        .route("/bamboo/config/reset", web::post().to(reset_bamboo_config))
        .route("/bamboo/proxy-auth", web::post().to(set_proxy_auth))
        .route(
            "/bamboo/proxy-auth/status",
            web::get().to(get_proxy_auth_status),
        )
        .route(
            "/bamboo/keyword-masking",
            web::get().to(get_keyword_masking_config),
        )
        .route(
            "/bamboo/keyword-masking",
            web::post().to(update_keyword_masking_config),
        )
        .route(
            "/bamboo/keyword-masking/validate",
            web::post().to(validate_keyword_entries),
        )
        .route(
            "/bamboo/settings/provider",
            web::get().to(get_provider_config),
        )
        .route(
            "/bamboo/settings/provider",
            web::post().to(update_provider_config),
        )
        .route(
            "/bamboo/settings/provider/models",
            web::post().to(fetch_provider_models),
        )
        .route(
            "/bamboo/settings/reload",
            web::post().to(reload_provider_config),
        )
        .route("/bamboo/tools", web::get().to(get_bamboo_tools))
        // ── Env vars ──────────────────────────────────────────────
        .route("/bamboo/env-vars", web::get().to(list_env_vars))
        .route("/bamboo/env-vars", web::post().to(upsert_env_var))
        .route("/bamboo/env-vars/replace", web::post().to(replace_env_vars))
        .route("/bamboo/env-vars/{name}", web::delete().to(delete_env_var));
}
