mod codex;
mod config_endpoints;
mod connect;
mod credentials;
mod notifications;
mod proxy_auth;
mod sections;
mod tools;
mod types;
mod validation;

#[cfg(test)]
mod tests;

pub use codex::detect_codex_cli;
pub use config_endpoints::{
    confirm_config_recovery, get_bamboo_config, get_config_recovery_status,
    get_model_limit_defaults, reset_bamboo_config, set_bamboo_config,
};
pub use connect::{get_connect_config, put_connect_config};
pub use credentials::{
    clear_credential, get_credential_status, get_live_config_health, list_credentials,
    replace_credential, reset_credentials,
};
pub use notifications::{get_notification_config, put_notification_config};
pub use proxy_auth::{get_proxy_auth_status, set_proxy_auth};
pub(crate) use sections::scrub_unsafe_request_override_literals;
pub use sections::{
    get_mcp_section, get_provider_section, get_provider_settings_section, get_typed_section,
    put_mcp_section, put_provider_section, put_provider_settings_section, put_typed_section,
    reset_typed_section,
};
pub use tools::get_bamboo_tools;
pub use types::ProxyAuthPayload;
pub use validation::validate_bamboo_config_patch;
