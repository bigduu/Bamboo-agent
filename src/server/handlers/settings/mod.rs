//! Settings and configuration handlers.
//!
//! This module is split by domain so each area can evolve independently
//! without growing a single monolithic file.

mod bamboo_config;
mod keyword_masking;
mod provider;
mod redaction;
mod setup;
mod workflows;

pub use bamboo_config::{
    get_anthropic_model_mapping, get_bamboo_config, get_proxy_auth_status, reset_bamboo_config,
    set_anthropic_model_mapping, set_bamboo_config, set_proxy_auth, validate_bamboo_config_patch,
    ProxyAuthPayload,
};
pub use keyword_masking::{
    get_keyword_masking_config, update_keyword_masking_config, validate_keyword_entries,
};
pub use provider::{
    fetch_provider_models, get_provider_config, reload_provider_config, update_provider_config,
    UpdateProviderRequest,
};
pub use setup::{get_setup_status, mark_setup_complete, mark_setup_incomplete};
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
        .route(
            "/bamboo/anthropic-model-mapping",
            web::get().to(get_anthropic_model_mapping),
        )
        .route(
            "/bamboo/anthropic-model-mapping",
            web::post().to(set_anthropic_model_mapping),
        );
}
