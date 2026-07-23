//! Settings and configuration handlers.
//!
//! This module is split by domain so each area can evolve independently
//! without growing a single monolithic file.

mod access_control;
mod bamboo_config;
mod cluster_fabric;
mod env_vars;
mod keyword_masking;
mod lifecycle_hooks;
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
    clear_credential, confirm_config_recovery, detect_codex_cli, get_bamboo_config,
    get_bamboo_tools, get_config_recovery_status, get_credential_status, get_live_config_health,
    get_mcp_section, get_model_limit_defaults, get_notification_config, get_provider_section,
    get_proxy_auth_status, get_typed_section, list_credentials, put_mcp_section,
    put_provider_section, put_typed_section, replace_credential, reset_bamboo_config,
    reset_credentials, reset_typed_section, set_bamboo_config, set_proxy_auth,
    validate_bamboo_config_patch, ProxyAuthPayload,
};
pub use cluster_fabric::{
    create_cluster, create_node, delete_cluster, delete_node, get_node, list_nodes, node_deploy,
    node_logs, node_status, node_stop, node_test, update_cluster, update_node,
};
pub use env_vars::{delete_env_var, list_env_vars, replace_env_vars, upsert_env_var};
pub use keyword_masking::{
    get_keyword_masking_config, update_keyword_masking_config, validate_keyword_entries,
};
pub use lifecycle_hooks::{test_lifecycle_hook, LifecycleHookTestRequest};
pub use permission::{
    create_permission_rule, delete_permission_rule, diagnose_permission, get_permission_ask_rules,
    get_permission_policy, update_permission_ask_rules, update_permission_rule,
};
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
    delete_workflow, get_workflow, list_workflow_catalog, list_workflows, migrate_workflow,
    save_workflow, MigrateWorkflowRequest, SaveWorkflowRequest, WorkflowCatalogQuery,
};

// NOTE: the production `/bamboo/*` route map lives in `routes::bamboo_v1_routes`
// (`routes/bamboo_v1.rs`). A second `config()` copy used to live here, but it had
// drifted (29 routes vs. the 70 in production) and had no callers, so it was
// removed to eliminate the drift hazard — a stale duplicate route map that tests
// could pass against while diverging from what actually serves. #251 (finding 5).
