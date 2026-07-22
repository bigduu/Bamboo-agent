use std::collections::BTreeMap;

use actix_web::{web, HttpResponse};
use bamboo_config::{
    LifecycleHookGroup, LifecycleHooksConfig, LIFECYCLE_HOOK_EVENT_NAMES,
    MAX_LIFECYCLE_HOOK_TIMEOUT_MS, MIN_LIFECYCLE_HOOK_TIMEOUT_MS,
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

    let current = app_state.config.read().await.clone();
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
            errors
                .entry("lifecycle_hooks".to_string())
                .or_default()
                .push(issue);
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
                    Some("command") => {}
                    Some(other) => issues.push(issue(
                        format!("{hook_path}.type"),
                        format!("unsupported lifecycle hook type '{other}'"),
                    )),
                    None => issues.push(issue(
                        format!("{hook_path}.type"),
                        "hook type must be 'command'",
                    )),
                }
                if !hook.get("command").is_some_and(Value::is_string) {
                    issues.push(issue(
                        format!("{hook_path}.command"),
                        "command must be a string",
                    ));
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
                if hook.command.trim().is_empty() {
                    issues.push(issue(
                        format!("{hook_path}.command"),
                        "command must not be empty",
                    ));
                }
                if !(MIN_LIFECYCLE_HOOK_TIMEOUT_MS..=MAX_LIFECYCLE_HOOK_TIMEOUT_MS)
                    .contains(&hook.timeout_ms)
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
