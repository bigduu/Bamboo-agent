use std::collections::BTreeMap;

use actix_web::{web, HttpResponse};
use bamboo_config::{
    LifecycleHookGroup, LifecycleHookHandler, LifecycleHooksConfig, LIFECYCLE_HOOK_EVENT_NAMES,
    MAX_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES, MAX_LIFECYCLE_HOOK_TIMEOUT_MS,
    MIN_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES, MIN_LIFECYCLE_HOOK_TIMEOUT_MS,
};
use regex::Regex;
use serde_json::Value;

use crate::config_manager;
use crate::{app_state::AppState, error::AppError};
use bamboo_llm::Config;

use super::types::{ValidateConfigResponse, ValidationIssue};

/// Validates a config patch without persisting it.
///
/// # HTTP Route
/// `POST /bamboo/config/validate`
///
/// This endpoint is designed for UX flows that want to surface issues early without
/// forcing strict validation on the permissive `/bamboo/config` patch endpoint.
pub async fn validate_bamboo_config_patch(
    app_state: web::Data<AppState>,
    payload: web::Json<Value>,
) -> Result<HttpResponse, AppError> {
    let patch = payload.into_inner();
    let mut patch_obj = config_manager::assert_json_object(patch)?;
    if patch_obj.contains_key("env_vars") {
        return Err(AppError::BadRequest(
            "env_vars must be changed through the dedicated revisioned env-vars API".to_string(),
        ));
    }
    if patch_obj.contains_key("cluster_fabric") {
        return Err(AppError::BadRequest(
            "cluster_fabric must be changed through the dedicated revisioned cluster API"
                .to_string(),
        ));
    }
    let current = app_state.config.read().await.clone();
    config_manager::remove_unchanged_core_proxy_echo(&current, &mut patch_obj)?;
    config_manager::sanitize_root_patch(&mut patch_obj);

    let lifecycle_schema_issues = patch_obj
        .get("lifecycle_hooks")
        .map(validate_lifecycle_hooks_shape)
        .unwrap_or_default();
    if !lifecycle_schema_issues.is_empty() {
        let mut errors = BTreeMap::new();
        errors.insert("lifecycle_hooks".to_string(), lifecycle_schema_issues);
        return Ok(HttpResponse::Ok().json(ValidateConfigResponse {
            valid: false,
            errors,
        }));
    }

    let codex_schema_issues = patch_obj
        .get("subagents")
        .map(validate_codex_subagents_shape)
        .unwrap_or_default();
    if !codex_schema_issues.is_empty() {
        let mut errors = BTreeMap::new();
        errors.insert("subagents".to_string(), codex_schema_issues);
        return Ok(HttpResponse::Ok().json(ValidateConfigResponse {
            valid: false,
            errors,
        }));
    }

    let merged = config_manager::build_merged_config(&current, patch_obj.clone())?;
    let domains = config_manager::domains_for_root_patch(&patch_obj);

    let mut errors: BTreeMap<String, Vec<ValidationIssue>> = BTreeMap::new();

    let mut push_error = |domain: &str, path: &str, message: String| {
        errors
            .entry(domain.to_string())
            .or_default()
            .push(ValidationIssue {
                path: path.to_string(),
                message,
            });
    };

    if domains.proxy {
        if let Err(err) = bamboo_llm::http_client::build_proxy(&merged) {
            push_error("proxy", "http_proxy/https_proxy", err.to_string());
        }
    }

    if domains.provider {
        if let Err(err) = bamboo_llm::validate_provider_config(&merged) {
            let (path, message) = provider_validation_issue(&merged, err.to_string());
            push_error("provider", path, message);
        }
    }

    if domains.setup {
        if let Some(setup) = merged.extra.get("setup") {
            if !setup.is_object() {
                push_error(
                    "setup",
                    "setup",
                    "config.setup must be a JSON object".to_string(),
                );
            }
        }
    }

    if domains.lifecycle_hooks {
        for issue in validate_lifecycle_hooks_config(&merged.lifecycle_hooks) {
            push_error("lifecycle_hooks", &issue.path, issue.message);
        }
    }

    if patch_obj.contains_key("subagents") {
        if let Err(message) = bamboo_config::validate_codex_subagents_config(merged.subagents()) {
            let path = if message.contains("base_url") {
                "subagents.codex_base_url"
            } else if message.contains("provider_key_ref") {
                "subagents.codex_provider_key_ref"
            } else if message.contains("forward_env") || message.contains("OPENAI_API_KEY") {
                "subagents.codex_forward_env"
            } else if message.contains("network_access") || message.contains("workspace-write") {
                "subagents.codex_network_access"
            } else if message.contains("allow_danger_bypass") {
                "subagents.codex_allow_danger_bypass"
            } else if message.contains("approval") {
                "subagents.codex_approval_policy"
            } else if message.contains("codex_mode") || message.contains("app_server") {
                "subagents.codex_mode"
            } else if message.contains("sandbox") || message.contains("danger-full-access") {
                "subagents.codex_sandbox"
            } else {
                "subagents.codex_auth_mode"
            };
            push_error("subagents", path, message);
        }
    }

    let valid = errors.values().all(|items| items.is_empty());
    Ok(HttpResponse::Ok().json(ValidateConfigResponse { valid, errors }))
}

fn issue(path: impl Into<String>, message: impl Into<String>) -> ValidationIssue {
    ValidationIssue {
        path: path.into(),
        message: message.into(),
    }
}

fn validate_codex_subagents_shape(value: &Value) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let Some(object) = value.as_object() else {
        issues.push(issue("subagents", "subagents must be a JSON object"));
        return issues;
    };

    if let Some(mode) = object.get("codex_mode") {
        let valid = mode.is_null()
            || mode
                .as_str()
                .is_some_and(|mode| matches!(mode, "exec" | "app_server"));
        if !valid {
            issues.push(issue(
                "subagents.codex_mode",
                "codex_mode must be exec or app_server",
            ));
        }
    }
    if let Some(mode) = object.get("codex_auth_mode") {
        let valid = mode.is_null()
            || mode
                .as_str()
                .is_some_and(|mode| matches!(mode, "inherit" | "api_key" | "custom" | "bamboo"));
        if !valid {
            issues.push(issue(
                "subagents.codex_auth_mode",
                "codex_auth_mode must be inherit, api_key, custom, or bamboo",
            ));
        }
    }
    if let Some(wire_api) = object.get("codex_wire_api") {
        if !(wire_api.is_null() || wire_api.as_str() == Some("responses")) {
            issues.push(issue(
                "subagents.codex_wire_api",
                "codex_wire_api must be responses for Codex CLI >= 0.144",
            ));
        }
    }
    if let Some(sandbox) = object.get("codex_sandbox") {
        let valid = sandbox.is_null()
            || sandbox.as_str().is_some_and(|sandbox| {
                matches!(
                    sandbox,
                    "read-only" | "workspace-write" | "danger-full-access"
                )
            });
        if !valid {
            issues.push(issue(
                "subagents.codex_sandbox",
                "codex_sandbox must be read-only, workspace-write, or danger-full-access",
            ));
        }
    }
    if let Some(policy) = object.get("codex_approval_policy") {
        let valid = policy.is_null()
            || policy
                .as_str()
                .is_some_and(|policy| matches!(policy, "never" | "on-failure" | "on-request"));
        if !valid {
            issues.push(issue(
                "subagents.codex_approval_policy",
                "codex_approval_policy must be never, on-failure, or on-request",
            ));
        }
    }
    for field in ["codex_network_access", "codex_allow_danger_bypass"] {
        if object
            .get(field)
            .is_some_and(|value| !value.is_null() && !value.is_boolean())
        {
            issues.push(issue(
                format!("subagents.{field}"),
                format!("{field} must be a boolean or null"),
            ));
        }
    }
    for field in ["codex_base_url", "codex_provider_key_ref"] {
        if object
            .get(field)
            .is_some_and(|value| !value.is_null() && !value.is_string())
        {
            issues.push(issue(
                format!("subagents.{field}"),
                format!("{field} must be a string or null"),
            ));
        }
    }
    if let Some(reference) = object.get("codex_provider_key_ref").and_then(Value::as_str) {
        if bamboo_config::CredentialRef::parse(reference.to_string()).is_err() {
            issues.push(issue(
                "subagents.codex_provider_key_ref",
                "codex_provider_key_ref has an invalid format",
            ));
        }
    }
    if let Some(forward_env) = object.get("codex_forward_env") {
        let valid = forward_env.is_null()
            || forward_env
                .as_array()
                .is_some_and(|items| items.iter().all(Value::is_string));
        if !valid {
            issues.push(issue(
                "subagents.codex_forward_env",
                "codex_forward_env must be an array of environment variable names",
            ));
        }
    }
    issues
}

/// Validate the JSON shape before deserializing the merged config so serde
/// failures (notably negative timeouts and missing command fields) are returned
/// through the endpoint's structured field-error contract instead of a generic
/// 400 response. Semantic checks run against the typed merged config below.
fn validate_lifecycle_hooks_shape(value: &Value) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let Some(root) = value.as_object() else {
        issues.push(issue(
            "lifecycle_hooks",
            "lifecycle_hooks must be a JSON object",
        ));
        return issues;
    };

    if let Some(enabled) = root.get("enabled") {
        if !enabled.is_boolean() {
            issues.push(issue(
                "lifecycle_hooks.enabled",
                "enabled must be a boolean",
            ));
        }
    }

    for (event, groups) in root {
        if event == "enabled" {
            continue;
        }
        if !LIFECYCLE_HOOK_EVENT_NAMES.contains(&event.as_str()) {
            issues.push(issue(
                format!("lifecycle_hooks.{event}"),
                format!("unknown lifecycle hook event '{event}'"),
            ));
            continue;
        }
        let Some(groups) = groups.as_array() else {
            issues.push(issue(
                format!("lifecycle_hooks.{event}"),
                "event hooks must be an array",
            ));
            continue;
        };
        for (group_index, group) in groups.iter().enumerate() {
            let group_path = format!("lifecycle_hooks.{event}[{group_index}]");
            let Some(group) = group.as_object() else {
                issues.push(issue(&group_path, "hook group must be an object"));
                continue;
            };
            if group
                .get("enabled")
                .is_some_and(|value| !value.is_boolean())
            {
                issues.push(issue(
                    format!("{group_path}.enabled"),
                    "enabled must be a boolean",
                ));
            }
            if group
                .get("matcher")
                .is_some_and(|value| !value.is_null() && !value.is_string())
            {
                issues.push(issue(
                    format!("{group_path}.matcher"),
                    "matcher must be a string or null",
                ));
            }
            let Some(hooks) = group.get("hooks") else {
                continue;
            };
            let Some(hooks) = hooks.as_array() else {
                issues.push(issue(
                    format!("{group_path}.hooks"),
                    "hooks must be an array",
                ));
                continue;
            };
            for (hook_index, hook) in hooks.iter().enumerate() {
                let hook_path = format!("{group_path}.hooks[{hook_index}]");
                let Some(hook) = hook.as_object() else {
                    issues.push(issue(&hook_path, "hook must be an object"));
                    continue;
                };
                match hook.get("type").and_then(Value::as_str) {
                    Some("command") => {
                        if !hook.get("command").is_some_and(Value::is_string) {
                            issues.push(issue(
                                format!("{hook_path}.command"),
                                "command must be a string",
                            ));
                        }
                    }
                    Some("javascript") => {
                        if !hook.get("source").is_some_and(Value::is_string) {
                            issues.push(issue(
                                format!("{hook_path}.source"),
                                "source must be a string",
                            ));
                        }
                        if hook
                            .get("memory_limit_bytes")
                            .is_some_and(|value| value.as_u64().is_none())
                        {
                            issues.push(issue(
                                format!("{hook_path}.memory_limit_bytes"),
                                "memory_limit_bytes must be a non-negative integer",
                            ));
                        }
                    }
                    Some(other) => issues.push(issue(
                        format!("{hook_path}.type"),
                        format!("unsupported lifecycle hook type '{other}'"),
                    )),
                    None => issues.push(issue(
                        format!("{hook_path}.type"),
                        "hook type must be 'command' or 'javascript'",
                    )),
                }
                if hook
                    .get("timeout_ms")
                    .is_some_and(|value| value.as_u64().is_none())
                {
                    issues.push(issue(
                        format!("{hook_path}.timeout_ms"),
                        "timeout_ms must be a non-negative integer",
                    ));
                }
            }
        }
    }

    issues
}

fn validate_lifecycle_hooks_config(config: &LifecycleHooksConfig) -> Vec<ValidationIssue> {
    let events: [(&str, &[LifecycleHookGroup]); 8] = [
        ("SessionStart", &config.session_start),
        ("UserPromptSubmit", &config.user_prompt_submit),
        ("PreToolUse", &config.pre_tool_use),
        ("PostToolUse", &config.post_tool_use),
        ("Stop", &config.stop),
        ("SessionEnd", &config.session_end),
        ("PreCompact", &config.pre_compact),
        ("Notification", &config.notification),
    ];
    let mut issues = Vec::new();
    for (event, groups) in events {
        for (group_index, group) in groups.iter().enumerate() {
            let group_path = format!("lifecycle_hooks.{event}[{group_index}]");
            if let Some(matcher) = group.matcher.as_deref() {
                if let Err(error) = Regex::new(matcher) {
                    issues.push(issue(
                        format!("{group_path}.matcher"),
                        format!("invalid matcher regex: {error}"),
                    ));
                }
            }
            for (hook_index, hook) in group.hooks.iter().enumerate() {
                let hook_path = format!("{group_path}.hooks[{hook_index}]");
                let timeout_ms = hook.timeout_ms();
                match hook {
                    LifecycleHookHandler::Command { command, .. } => {
                        if command.trim().is_empty() {
                            issues.push(issue(
                                format!("{hook_path}.command"),
                                "command must not be empty",
                            ));
                        }
                    }
                    LifecycleHookHandler::JavaScript {
                        source,
                        memory_limit_bytes,
                        ..
                    } => {
                        if source.trim().is_empty() {
                            issues.push(issue(
                                format!("{hook_path}.source"),
                                "source must not be empty",
                            ));
                        }
                        if !(MIN_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES
                            ..=MAX_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES)
                            .contains(memory_limit_bytes)
                        {
                            issues.push(issue(
                                format!("{hook_path}.memory_limit_bytes"),
                                format!(
                                    "memory_limit_bytes must be between {MIN_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES} and {MAX_JAVASCRIPT_HOOK_MEMORY_LIMIT_BYTES}"
                                ),
                            ));
                        }
                    }
                }
                if !(MIN_LIFECYCLE_HOOK_TIMEOUT_MS..=MAX_LIFECYCLE_HOOK_TIMEOUT_MS)
                    .contains(&timeout_ms)
                {
                    issues.push(issue(
                        format!("{hook_path}.timeout_ms"),
                        format!(
                            "timeout_ms must be between {MIN_LIFECYCLE_HOOK_TIMEOUT_MS} and {MAX_LIFECYCLE_HOOK_TIMEOUT_MS}"
                        ),
                    ));
                }
            }
        }
    }
    issues
}

pub(super) fn provider_validation_issue(
    config: &Config,
    fallback_error: String,
) -> (&'static str, String) {
    match config.provider.as_str() {
        "openai" => provider_issue(
            config
                .providers()
                .openai
                .as_ref()
                .map(|provider| {
                    !provider.api_key.trim().is_empty()
                        || provider
                            .api_key_encrypted
                            .as_deref()
                            .map(|value| !value.trim().is_empty())
                            .unwrap_or(false)
                })
                .unwrap_or(false),
            "providers.openai.api_key",
            "OpenAI API key is required",
            fallback_error,
        ),
        "anthropic" => provider_issue(
            config
                .providers()
                .anthropic
                .as_ref()
                .map(|provider| {
                    !provider.api_key.trim().is_empty()
                        || provider
                            .api_key_encrypted
                            .as_deref()
                            .map(|value| !value.trim().is_empty())
                            .unwrap_or(false)
                })
                .unwrap_or(false),
            "providers.anthropic.api_key",
            "Anthropic API key is required",
            fallback_error,
        ),
        "gemini" => provider_issue(
            config
                .providers()
                .gemini
                .as_ref()
                .map(|provider| {
                    !provider.api_key.trim().is_empty()
                        || provider
                            .api_key_encrypted
                            .as_deref()
                            .map(|value| !value.trim().is_empty())
                            .unwrap_or(false)
                })
                .unwrap_or(false),
            "providers.gemini.api_key",
            "Gemini API key is required",
            fallback_error,
        ),
        _ => ("provider", fallback_error),
    }
}

fn provider_issue(
    is_configured: bool,
    missing_path: &'static str,
    missing_message: &'static str,
    fallback_error: String,
) -> (&'static str, String) {
    if is_configured {
        ("provider", fallback_error)
    } else {
        (missing_path, missing_message.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_shape_validation_reports_precise_fields() {
        let issues = validate_codex_subagents_shape(&serde_json::json!({
            "codex_mode": "future",
            "codex_auth_mode": "future",
            "codex_wire_api": "chat",
            "codex_base_url": 42,
            "codex_provider_key_ref": "not a credential ref",
            "codex_forward_env": ["OPENAI_API_KEY", 1],
            "codex_sandbox": "host-write",
            "codex_approval_policy": "auto",
            "codex_network_access": "yes",
            "codex_allow_danger_bypass": 1
        }));
        let paths = issues
            .iter()
            .map(|issue| issue.path.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        for expected in [
            "subagents.codex_mode",
            "subagents.codex_auth_mode",
            "subagents.codex_wire_api",
            "subagents.codex_base_url",
            "subagents.codex_provider_key_ref",
            "subagents.codex_forward_env",
            "subagents.codex_sandbox",
            "subagents.codex_approval_policy",
            "subagents.codex_network_access",
            "subagents.codex_allow_danger_bypass",
        ] {
            assert!(
                paths.contains(expected),
                "missing path {expected}: {paths:?}"
            );
        }
    }

    #[test]
    fn codex_shape_validation_accepts_the_documented_surface() {
        assert!(validate_codex_subagents_shape(&serde_json::json!({
            "codex_mode": "exec",
            "codex_auth_mode": "custom",
            "codex_wire_api": "responses",
            "codex_base_url": "https://provider.example/v1",
            "codex_provider_key_ref": "provider.openai.api_key",
            "codex_forward_env": ["LANG"],
            "codex_sandbox": "workspace-write",
            "codex_approval_policy": "never",
            "codex_network_access": true,
            "codex_allow_danger_bypass": false
        }))
        .is_empty());

        assert!(validate_codex_subagents_shape(&serde_json::json!({
            "codex_mode": "app_server",
            "codex_approval_policy": "on-request"
        }))
        .is_empty());
    }
}
