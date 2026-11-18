//! Config-driven request override helpers shared by provider implementations.

use std::collections::HashMap;

use super::log_identity;
use chrono::Utc;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::provider::{
    current_completion_diagnostics, LLMError, OutputLimitRecoveryControls, ProviderModelInfo,
    RequestToolSelection, Result, OUTPUT_LIMIT_CONVERGENCE_HINT,
};
use bamboo_config::{
    BodyPatchOp, KeywordMaskingConfig, ModelRequestRule, PatchValue, RequestOverridesConfig,
    RequestScopeOverride, TemplateExpr, TemplateExprSpec,
};
use bamboo_domain::ReasoningEffort;

pub const ENDPOINT_CHAT_COMPLETIONS: &str = "chat_completions";
pub const ENDPOINT_RESPONSES: &str = "responses";
pub const ENDPOINT_MESSAGES: &str = "messages";
pub const ENDPOINT_STREAM_GENERATE_CONTENT: &str = "stream_generate_content";
pub const ENDPOINT_MODELS: &str = "models";

const FORBIDDEN_HEADER_NAMES: [&str; 4] =
    ["host", "content-length", "transfer-encoding", "connection"];

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathSegment {
    Key(String),
    Index(usize),
}

pub fn apply_overrides_to_header_map(
    headers: &mut HeaderMap,
    overrides: Option<&RequestOverridesConfig>,
    endpoint: &str,
    model: Option<&str>,
) {
    let env_vars = bamboo_config::Config::current_env_vars();
    apply_overrides_to_header_map_with_env(headers, overrides, endpoint, model, &env_vars);
}

pub fn apply_overrides_to_header_map_with_env(
    headers: &mut HeaderMap,
    overrides: Option<&RequestOverridesConfig>,
    endpoint: &str,
    model: Option<&str>,
    env_vars: &HashMap<String, String>,
) {
    let Some(overrides) = overrides else {
        return;
    };

    let resolved = resolve_scope(overrides, endpoint, model);
    for (name, expr) in resolved.headers {
        let header_name = name.trim();
        if header_name.is_empty() {
            continue;
        }
        if is_forbidden_header_name(header_name) {
            tracing::warn!(
                "Skipping forbidden request override header_hash={} for endpoint '{}'",
                log_identity(header_name),
                endpoint
            );
            continue;
        }

        let Some(value) = resolve_template_expr(&expr, env_vars) else {
            tracing::warn!(
                "Skipping request override header_hash={} because its value did not resolve",
                log_identity(header_name)
            );
            continue;
        };

        let parsed_name = match HeaderName::from_bytes(header_name.as_bytes()) {
            Ok(name) => name,
            Err(_) => {
                tracing::warn!(
                    "Skipping invalid request override header_name_hash={}",
                    log_identity(header_name)
                );
                continue;
            }
        };
        let parsed_value = match HeaderValue::from_str(&value) {
            Ok(value) => value,
            Err(_) => {
                tracing::warn!(
                    "Skipping request override header_hash={} due to invalid value",
                    log_identity(header_name)
                );
                continue;
            }
        };

        headers.insert(parsed_name, parsed_value);
    }
}

pub fn apply_overrides_to_body(
    body: &mut Value,
    overrides: Option<&RequestOverridesConfig>,
    endpoint: &str,
    model: Option<&str>,
) {
    let env_vars = bamboo_config::Config::current_env_vars();
    apply_overrides_to_body_with_env(body, overrides, endpoint, model, &env_vars);
}

/// Operator overrides can select functions from the request's eligible
/// catalog. The model-facing declarations and native transcript are owned by
/// the agent, since either can otherwise introduce provider-side tools.
pub(crate) fn configured_tool_selection(
    overrides: Option<&RequestOverridesConfig>,
    endpoint: &str,
    model: &str,
) -> Result<RequestToolSelection> {
    let Some(overrides) = overrides else {
        return Ok(RequestToolSelection::Automatic);
    };
    let scope = resolve_scope(overrides, endpoint, Some(model));
    if scope.body_patch.iter().any(|patch| {
        !matches!(
            parse_path(&patch.path).first(),
            Some(PathSegment::Key(key)) if !matches!(
                key.as_str(),
                "tools" | "functions" | "function_call" | "mcp_servers" | "mcpServers"
                    | "input" | "messages" | "contents" | "conversation" | "previous_response_id"
                    | "prompt" | "cachedContent" | "cached_content"
            )
        )
    }) {
        return Err(LLMError::RequestFailed(
            "Agent request overrides cannot replace tool declarations or conversation input".into(),
        ));
    }
    let mut body = serde_json::json!({});
    apply_overrides_to_body(&mut body, Some(overrides), endpoint, Some(model));
    if [
        "tools",
        "functions",
        "function_call",
        "tool_choice",
        "toolConfig",
        "tool_config",
        "mcp_servers",
        "input",
        "messages",
        "contents",
        "conversation",
        "previous_response_id",
    ]
    .iter()
    .any(|key| body["extra_body"].get(key).is_some())
    {
        return Err(LLMError::RequestFailed(
            "Agent tool selection must use the generation endpoint's own tool choice field".into(),
        ));
    }
    parse_tool_selection(&body, endpoint)
}

fn parse_tool_selection(body: &Value, endpoint: &str) -> Result<RequestToolSelection> {
    use RequestToolSelection::{Automatic, Disabled, Forced, Named, Required};

    let invalid = || LLMError::RequestFailed("Invalid or unsupported agent tool choice".into());
    let named = |mut names: Vec<&str>, required: bool| {
        if names.is_empty() || names.iter().any(|name| name.is_empty()) {
            return Err(invalid());
        }
        names.sort_unstable();
        names.dedup();
        Ok(Named {
            names: names.into_iter().map(str::to_owned).collect(),
            required,
        })
    };
    let forced = |name: &Value| {
        name.as_str()
            .filter(|name| !name.is_empty())
            .map(|name| Forced(name.to_owned()))
            .ok_or_else(invalid)
    };
    if endpoint == ENDPOINT_STREAM_GENERATE_CONTENT {
        let Some(config) = body
            .get("toolConfig")
            .or_else(|| body.get("tool_config"))
            .and_then(|config| {
                config
                    .get("functionCallingConfig")
                    .or_else(|| config.get("function_calling_config"))
            })
        else {
            return Ok(Automatic);
        };
        let mode = config.get("mode").and_then(Value::as_str);
        if mode == Some("NONE") {
            return Ok(Disabled);
        }
        if let Some(names) = config
            .get("allowedFunctionNames")
            .or_else(|| config.get("allowed_function_names"))
        {
            let names = names.as_array().ok_or_else(invalid)?;
            if !names.is_empty() {
                return named(
                    names
                        .iter()
                        .map(|name| name.as_str().ok_or_else(invalid))
                        .collect::<Result<Vec<_>>>()?,
                    mode == Some("ANY"),
                );
            }
        }
        return Ok(if mode == Some("ANY") {
            Required
        } else {
            Automatic
        });
    }
    let choice = &body["tool_choice"];
    let kind = choice.as_str().or_else(|| choice["type"].as_str());
    match kind {
        None if choice.is_null() || (endpoint == ENDPOINT_MESSAGES && choice.is_object()) => {
            Ok(Automatic)
        }
        Some("auto") => Ok(Automatic),
        Some("required" | "any") => Ok(Required),
        Some("none") => Ok(Disabled),
        Some("tool") if endpoint == ENDPOINT_MESSAGES => forced(&choice["name"]),
        Some("function") if endpoint != ENDPOINT_MESSAGES => {
            let name = if endpoint == ENDPOINT_CHAT_COMPLETIONS {
                &choice["function"]["name"]
            } else {
                &choice["name"]
            };
            forced(name)
        }
        Some("allowed_tools") if endpoint != ENDPOINT_MESSAGES => {
            let choice = if endpoint == ENDPOINT_CHAT_COMPLETIONS {
                &choice["allowed_tools"]
            } else {
                choice
            };
            if !matches!(choice["mode"].as_str(), Some("auto" | "required")) {
                return Err(invalid());
            }
            named(
                choice["tools"]
                    .as_array()
                    .ok_or_else(invalid)?
                    .iter()
                    .map(|tool| {
                        if tool["type"] != "function" {
                            return Err(invalid());
                        }
                        if endpoint == ENDPOINT_CHAT_COMPLETIONS {
                            tool["function"]["name"].as_str().ok_or_else(invalid)
                        } else {
                            tool["name"].as_str().ok_or_else(invalid)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?,
                choice["mode"] == "required",
            )
        }
        _ => Err(invalid()),
    }
}

fn wire_declares_tool(body: &Value, endpoint: &str, name: &str) -> bool {
    body["tools"].as_array().is_some_and(|tools| {
        tools.iter().any(|tool| match endpoint {
            ENDPOINT_CHAT_COMPLETIONS => tool["function"]["name"] == name,
            ENDPOINT_STREAM_GENERATE_CONTENT => tool["functionDeclarations"]
                .as_array()
                .is_some_and(|functions| functions.iter().any(|function| function["name"] == name)),
            _ => tool["name"] == name,
        })
    })
}

/// Gateways can route the same model ID differently under request-specific
/// headers. Keep catalog IDs but withhold capacities when these scopes differ.
pub(crate) fn bind_catalog_limits(
    mut models: Vec<ProviderModelInfo>,
    overrides: Option<&RequestOverridesConfig>,
    generation_endpoints: impl Fn(&str) -> &'static [&'static str],
) -> Vec<ProviderModelInfo> {
    let Some(overrides) = overrides.filter(|overrides| {
        overrides
            .endpoints
            .values()
            .any(|scope| !scope.headers.is_empty())
            || overrides
                .rules
                .iter()
                .any(|rule| !rule.scope.headers.is_empty())
    }) else {
        return models;
    };
    let catalog_headers = resolve_scope(overrides, ENDPOINT_MODELS, None).headers;
    for model in &mut models {
        if generation_endpoints(&model.id).iter().any(|endpoint| {
            resolve_scope(overrides, endpoint, Some(&model.id)).headers != catalog_headers
        }) {
            *model = ProviderModelInfo::from_id(std::mem::take(&mut model.id));
        }
    }
    models
}

/// Inspect the final wire controls after masking and operator body patches.
/// Keep recovery bound to the observed protocol, effort and output cap.
pub(crate) fn mask_and_prepare_agent_request(
    body: &mut Value,
    masking: &KeywordMaskingConfig,
    overrides: Option<&RequestOverridesConfig>,
    endpoint: &'static str,
    model: &str,
) -> Result<()> {
    crate::masking::mask_outbound_body(body, masking);
    let Some(diagnostics) = current_completion_diagnostics().filter(|d| d.is_agent_round()) else {
        return Ok(());
    };
    if parse_tool_selection(body, endpoint)?
        .names()
        .iter()
        .any(|name| !wire_declares_tool(body, endpoint, name))
    {
        return Err(LLMError::RequestFailed(
            "A tool selected by the provider request is missing from its declarations".into(),
        ));
    }
    if diagnostics.is_task_split() {
        if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
            tools.retain(|tool| {
                tool.get("name")
                    .or_else(|| tool.pointer("/function/name"))
                    .and_then(Value::as_str)
                    != Some(bamboo_domain::DISCOVERY_CONTROL_FALLBACK_TOOL_NAME)
            });
        }
    }
    let (effort_path, limit_path, low) = match endpoint {
        ENDPOINT_CHAT_COMPLETIONS => ("/reasoning_effort", "/max_completion_tokens", "low"),
        ENDPOINT_RESPONSES => ("/reasoning/effort", "/max_output_tokens", "low"),
        ENDPOINT_MESSAGES => ("/output_config/effort", "/max_tokens", "low"),
        ENDPOINT_STREAM_GENERATE_CONTENT => (
            "/generationConfig/thinkingConfig/thinkingLevel",
            "/generationConfig/maxOutputTokens",
            "LOW",
        ),
        _ => return Ok(()),
    };
    let legacy_limit_path = (endpoint == ENDPOINT_CHAT_COMPLETIONS).then_some("/max_tokens");
    if let Some(window_limit) = diagnostics.model_window_limit() {
        // Gemini addresses the model in the URL. The other adapters must
        // still address the model whose capacity the runtime resolved.
        if endpoint != ENDPOINT_STREAM_GENERATE_CONTENT
            && body.get("model").and_then(Value::as_str) != Some(model)
        {
            return Err(LLMError::RequestFailed(
                "Set the routed model in provider settings when using model windows".into(),
            ));
        }
        let limit_paths = std::iter::once(limit_path).chain(legacy_limit_path);
        let mut wire_limit = u64::from(window_limit);
        let mut has_output_control = false;
        for field in limit_paths.clone().filter_map(|path| body.pointer(path)) {
            let configured = field.as_u64().filter(|limit| *limit > 0).ok_or_else(|| {
                LLMError::RequestFailed("Model output limit must be a positive integer".into())
            })?;
            wire_limit = wire_limit.min(configured);
            has_output_control = true;
        }
        if !has_output_control
            && (endpoint == ENDPOINT_MESSAGES || !diagnostics.allows_provider_default_output())
        {
            return Err(LLMError::RequestFailed(
                "Model window requires an output limit on the final request".into(),
            ));
        }
        for path in limit_paths {
            if let Some(field) = body.pointer_mut(path) {
                *field = Value::from(wire_limit);
            }
        }
    }
    let limit = body
        .pointer(limit_path)
        .or_else(|| legacy_limit_path.and_then(|path| body.pointer(path)))
        .and_then(Value::as_u64)
        .filter(|limit| *limit > 0);
    let effort = body
        .pointer(effort_path)
        .and_then(Value::as_str)
        .and_then(ReasoningEffort::parse)
        .filter(|effort| *effort != ReasoningEffort::Disabled);
    let native_control = match endpoint {
        ENDPOINT_MESSAGES => {
            body.pointer("/thinking/type").and_then(Value::as_str) == Some("adaptive")
        }
        ENDPOINT_CHAT_COMPLETIONS => {
            body.get("enable_thinking").is_none() && body.get("thinking").is_none()
        }
        ENDPOINT_STREAM_GENERATE_CONTENT => body
            .pointer("/generationConfig/thinkingConfig/thinkingBudget")
            .is_none(),
        _ => true,
    };
    let compatible_messages = endpoint == ENDPOINT_MESSAGES && diagnostics.is_compatible_messages();
    let candidate = limit
        .filter(|_| {
            client_tools_only(endpoint, body) && !crate::retry::work_request_budget_active()
        })
        .and_then(|output_limit| {
            let patches = overrides.map(|config| resolve_scope(config, endpoint, Some(model)));
            let mut patches = patches.iter().flat_map(|scope| &scope.body_patch);
            let effort_segments = parse_path(effort_path);
            let control_is_pinned = patches.clone().any(|patch| {
                let path = parse_path(&patch.path);
                patches_reasoning_control(endpoint, &path)
                    && !(effort == Some(ReasoningEffort::Low) && path == effort_segments)
            });
            // Altering effort and resuming unfinished work have separate
            // prerequisites. A custom gateway can keep reasoning even when the
            // operator removes, pins or replaces its reasoning controls.
            let lowerable_effort = effort.filter(|effort| {
                native_control
                    && !control_is_pinned
                    && (*effort == ReasoningEffort::Low || masking.apply_masking(low) == low)
            });
            if !compatible_messages && lowerable_effort.is_none() {
                return None;
            }
            let (prompt_key, candidate_count) = match endpoint {
                ENDPOINT_CHAT_COMPLETIONS => ("messages", body.get("n")),
                ENDPOINT_MESSAGES => ("messages", None),
                ENDPOINT_RESPONSES => ("input", None),
                _ => ("contents", body.pointer("/generationConfig/candidateCount")),
            };
            if candidate_count.is_some_and(|count| count.as_u64() != Some(1)) {
                return None;
            }
            let allows_convergence = !patches.any(|patch| match parse_path(&patch.path).first() {
                Some(PathSegment::Key(key)) => key == prompt_key || key == "model",
                _ => true,
            }) && masking.apply_masking(OUTPUT_LIMIT_CONVERGENCE_HINT)
                == OUTPUT_LIMIT_CONVERGENCE_HINT;
            Some(OutputLimitRecoveryControls {
                endpoint,
                lowerable_effort,
                output_limit,
                allows_convergence,
                compatible_messages,
            })
        });
    if diagnostics.observe_agent_round_request(endpoint, limit, candidate)? {
        *body
            .pointer_mut(effort_path)
            .expect("validated wire effort field") = Value::String(low.into());
    }
    Ok(())
}

fn client_tools_only(endpoint: &str, body: &Value) -> bool {
    if body.get("mcp_servers").is_some() || body.get("mcpServers").is_some() {
        return false;
    }
    body.get("tools").is_none_or(|tools| {
        tools.as_array().is_some_and(|tools| {
            tools.iter().all(|tool| match endpoint {
                ENDPOINT_MESSAGES => {
                    tool.get("input_schema").is_some()
                        && tool.get("type").is_none_or(|kind| kind == "custom")
                }
                ENDPOINT_STREAM_GENERATE_CONTENT => tool.as_object().is_some_and(|tool| {
                    tool.len() == 1 && tool.contains_key("functionDeclarations")
                }),
                _ => tool["type"] == "function",
            })
        })
    })
}

fn patches_reasoning_control(endpoint: &str, path: &[PathSegment]) -> bool {
    if path.is_empty() {
        return true;
    }
    let key = |index| match path.get(index) {
        Some(PathSegment::Key(key)) => Some(key.as_str()),
        _ => None,
    };
    match (endpoint, key(0)) {
        (ENDPOINT_MESSAGES, Some("thinking")) => key(1) != Some("budget_tokens"),
        (ENDPOINT_MESSAGES, Some("output_config")) => key(1).is_none_or(|key| key == "effort"),
        (
            ENDPOINT_CHAT_COMPLETIONS,
            Some("reasoning_effort" | "reasoning" | "thinking" | "enable_thinking" | "extra_body"),
        ) => true,
        (ENDPOINT_RESPONSES, Some("reasoning")) => key(1).is_none_or(|key| key == "effort"),
        (ENDPOINT_STREAM_GENERATE_CONTENT, Some("generationConfig")) => {
            key(1).is_none_or(|key| key == "thinkingConfig")
        }
        _ => false,
    }
}

pub(crate) fn messages_controls_overridden(
    overrides: Option<&RequestOverridesConfig>,
    model: &str,
) -> bool {
    overrides.is_some_and(|config| {
        resolve_scope(config, ENDPOINT_MESSAGES, Some(model))
            .body_patch
            .iter()
            .any(|patch| {
                let path = parse_path(&patch.path);
                patches_reasoning_control(ENDPOINT_MESSAGES, &path)
                    || matches!(path.first(), Some(PathSegment::Key(key)) if key == "model" || key == "tool_choice")
            })
    })
}

pub(crate) fn chat_completions_controls_overridden(
    overrides: Option<&RequestOverridesConfig>,
    model: &str,
) -> bool {
    overrides.is_some_and(|config| {
        resolve_scope(config, ENDPOINT_CHAT_COMPLETIONS, Some(model))
            .body_patch
            .iter()
            .any(|patch| {
                let path = parse_path(&patch.path);
                patches_reasoning_control(ENDPOINT_CHAT_COMPLETIONS, &path)
                    || matches!(path.first(), Some(PathSegment::Key(key)) if matches!(key.as_str(), "model" | "max_tokens" | "max_completion_tokens"))
            })
    })
}

/// A configured Work budget cannot accept arbitrary wire body patches (which
/// may replace/remove token limits, model, prompts or candidate counts). Reject
/// the resolved patch before any model request; header-only overrides remain.
pub(crate) fn validate_budgeted_overrides(
    overrides: Option<&RequestOverridesConfig>,
    endpoint: &str,
    model: Option<&str>,
) -> crate::provider::Result<()> {
    if crate::retry::work_request_budget_active()
        && overrides
            .is_some_and(|config| !resolve_scope(config, endpoint, model).body_patch.is_empty())
    {
        return Err(crate::provider::LLMError::RequestFailed(
            "Configured Work token accounting does not permit request body patches".into(),
        ));
    }
    Ok(())
}

pub fn apply_overrides_to_body_with_env(
    body: &mut Value,
    overrides: Option<&RequestOverridesConfig>,
    endpoint: &str,
    model: Option<&str>,
    env_vars: &HashMap<String, String>,
) {
    let Some(overrides) = overrides else {
        return;
    };

    let resolved = resolve_scope(overrides, endpoint, model);
    for patch in resolved.body_patch {
        let path = parse_path(&patch.path);
        match patch.op {
            BodyPatchOp::Set => {
                let Some(raw) = patch.value.as_ref() else {
                    tracing::warn!(
                        "Skipping body_patch set op without value for path_hash={}",
                        log_identity(&patch.path)
                    );
                    continue;
                };
                let Some(value) = resolve_patch_value(raw, env_vars) else {
                    tracing::warn!(
                        "Skipping body_patch set op for path_hash={} because value did not resolve",
                        log_identity(&patch.path)
                    );
                    continue;
                };
                set_value_at_path(body, &path, value);
            }
            BodyPatchOp::Remove => {
                remove_value_at_path(body, &path);
            }
        }
    }
}

fn resolve_scope(
    overrides: &RequestOverridesConfig,
    endpoint: &str,
    model: Option<&str>,
) -> RequestScopeOverride {
    let mut resolved = RequestScopeOverride::default();
    merge_scope(&mut resolved, &overrides.common);

    if let Some((_, endpoint_scope)) = overrides
        .endpoints
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(endpoint))
    {
        merge_scope(&mut resolved, endpoint_scope);
    }

    if let Some(model) = model {
        let mut matched_rules: Vec<(u8, usize, usize, &ModelRequestRule)> = overrides
            .rules
            .iter()
            .enumerate()
            .filter_map(|(index, rule)| {
                match_rule_specificity(rule, endpoint, model)
                    .map(|(tier, len)| (tier, len, index, rule))
            })
            .collect();

        // Generic -> specific (so specific rules win on overwrite).
        matched_rules.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));

        for (_, _, _, rule) in matched_rules {
            merge_scope(&mut resolved, &rule.scope);
        }
    }

    resolved
}

fn merge_scope(dst: &mut RequestScopeOverride, src: &RequestScopeOverride) {
    for (key, value) in &src.headers {
        dst.headers
            .insert(key.trim().to_ascii_lowercase(), value.clone());
    }
    dst.body_patch.extend(src.body_patch.clone());
}

fn match_rule_specificity(
    rule: &ModelRequestRule,
    endpoint: &str,
    model: &str,
) -> Option<(u8, usize)> {
    if let Some(rule_endpoint) = rule.endpoint.as_deref() {
        if !rule_endpoint.eq_ignore_ascii_case(endpoint) {
            return None;
        }
    }

    let pattern = rule.model_pattern.trim().to_ascii_lowercase();
    if pattern.is_empty() {
        return None;
    }

    let normalized_model = model.trim().to_ascii_lowercase();
    if let Some(prefix) = pattern.strip_suffix('*') {
        if normalized_model.starts_with(prefix) {
            return Some((0, prefix.len()));
        }
        return None;
    }

    if normalized_model == pattern {
        return Some((1, pattern.len()));
    }

    None
}

fn is_forbidden_header_name(name: &str) -> bool {
    let lower = name.trim().to_ascii_lowercase();
    FORBIDDEN_HEADER_NAMES
        .iter()
        .any(|forbidden| lower == *forbidden)
}

fn resolve_patch_value(value: &PatchValue, env_vars: &HashMap<String, String>) -> Option<Value> {
    match value {
        PatchValue::Json(value) => Some(value.clone()),
        PatchValue::Template(expr) => resolve_template_expr(expr, env_vars).map(Value::String),
    }
}

fn resolve_template_expr(
    expr: &TemplateExpr,
    env_vars: &HashMap<String, String>,
) -> Option<String> {
    match expr {
        TemplateExpr::Literal(value) => Some(value.clone()),
        TemplateExpr::Structured(spec) => match spec {
            TemplateExprSpec::Literal { value } => Some(value.clone()),
            TemplateExprSpec::EnvRef { name, fallback } => {
                let key = name.trim();
                if key.is_empty() {
                    return fallback.clone();
                }
                env_vars.get(key).cloned().or_else(|| fallback.clone())
            }
            TemplateExprSpec::Generated { generator } => Some(match generator {
                bamboo_config::GeneratedValue::Uuid => Uuid::new_v4().to_string(),
                bamboo_config::GeneratedValue::UnixMs => Utc::now().timestamp_millis().to_string(),
            }),
            TemplateExprSpec::Format { template } => {
                Some(render_format_template(template, env_vars))
            }
        },
    }
}

fn render_format_template(template: &str, env_vars: &HashMap<String, String>) -> String {
    let chars: Vec<char> = template.chars().collect();
    let mut output = String::with_capacity(template.len());
    let mut index = 0usize;

    while index < chars.len() {
        if chars[index] == '{' {
            let mut end = index + 1;
            while end < chars.len() && chars[end] != '}' {
                end += 1;
            }
            if end < chars.len() && chars[end] == '}' {
                let token: String = chars[index + 1..end].iter().collect();
                if let Some(resolved) = resolve_format_token(token.trim(), env_vars) {
                    output.push_str(&resolved);
                } else {
                    output.push('{');
                    output.push_str(&token);
                    output.push('}');
                }
                index = end + 1;
                continue;
            }
        }

        output.push(chars[index]);
        index += 1;
    }

    output
}

fn resolve_format_token(token: &str, env_vars: &HashMap<String, String>) -> Option<String> {
    if token.eq_ignore_ascii_case("uuid") {
        return Some(Uuid::new_v4().to_string());
    }
    if token.eq_ignore_ascii_case("unix_ms") {
        return Some(Utc::now().timestamp_millis().to_string());
    }
    if let Some(key) = token.strip_prefix("env:") {
        return Some(env_vars.get(key.trim()).cloned().unwrap_or_default());
    }
    None
}

fn parse_path(path: &str) -> Vec<PathSegment> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    let segments: Vec<String> = if trimmed.starts_with('/') {
        if trimmed == "/" {
            Vec::new()
        } else {
            trimmed
                .split('/')
                .skip(1)
                .map(unescape_json_pointer_segment)
                .collect()
        }
    } else {
        trimmed
            .split('.')
            .filter(|segment| !segment.trim().is_empty())
            .map(|segment| segment.trim().to_string())
            .collect()
    };

    segments
        .into_iter()
        .map(|segment| match segment.parse::<usize>() {
            Ok(index) => PathSegment::Index(index),
            Err(_) => PathSegment::Key(segment),
        })
        .collect()
}

fn unescape_json_pointer_segment(segment: &str) -> String {
    segment.replace("~1", "/").replace("~0", "~")
}

fn set_value_at_path(root: &mut Value, path: &[PathSegment], value: Value) {
    let Some((last, parent_path)) = path.split_last() else {
        *root = value;
        return;
    };

    let mut cursor = root;
    for segment in parent_path {
        match segment {
            PathSegment::Key(key) => {
                if !cursor.is_object() {
                    *cursor = Value::Object(Map::new());
                }
                let object = cursor
                    .as_object_mut()
                    .expect("object just created or already present");
                cursor = object.entry(key.clone()).or_insert(Value::Null);
            }
            PathSegment::Index(index) => {
                if !cursor.is_array() {
                    *cursor = Value::Array(Vec::new());
                }
                let array = cursor
                    .as_array_mut()
                    .expect("array just created or already present");
                if array.len() <= *index {
                    array.resize(*index + 1, Value::Null);
                }
                cursor = array
                    .get_mut(*index)
                    .expect("index resized to be available");
            }
        }
    }

    match last {
        PathSegment::Key(key) => {
            if !cursor.is_object() {
                *cursor = Value::Object(Map::new());
            }
            let object = cursor
                .as_object_mut()
                .expect("object just created or already present");
            object.insert(key.clone(), value);
        }
        PathSegment::Index(index) => {
            if !cursor.is_array() {
                *cursor = Value::Array(Vec::new());
            }
            let array = cursor
                .as_array_mut()
                .expect("array just created or already present");
            if array.len() <= *index {
                array.resize(*index + 1, Value::Null);
            }
            array[*index] = value;
        }
    }
}

fn remove_value_at_path(root: &mut Value, path: &[PathSegment]) {
    let Some((last, parent_path)) = path.split_last() else {
        *root = Value::Null;
        return;
    };

    let mut cursor = root;
    for segment in parent_path {
        match segment {
            PathSegment::Key(key) => {
                let Some(object) = cursor.as_object_mut() else {
                    return;
                };
                let Some(next) = object.get_mut(key) else {
                    return;
                };
                cursor = next;
            }
            PathSegment::Index(index) => {
                let Some(array) = cursor.as_array_mut() else {
                    return;
                };
                let Some(next) = array.get_mut(*index) else {
                    return;
                };
                cursor = next;
            }
        }
    }

    match last {
        PathSegment::Key(key) => {
            if let Some(object) = cursor.as_object_mut() {
                object.remove(key);
            }
        }
        PathSegment::Index(index) => {
            if let Some(array) = cursor.as_array_mut() {
                if *index < array.len() {
                    array.remove(*index);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::{
        BodyPatch, GeneratedValue, ModelRequestRule, RequestOverridesConfig, RequestScopeOverride,
        TemplateExpr, TemplateExprSpec,
    };

    #[tokio::test]
    async fn messages_reasoning_overrides_are_preserved_on_task_split() {
        use crate::provider::{scope_completion_diagnostics, CompletionDiagnostics};
        use serde_json::json;

        for (path, op, value) in [
            ("thinking", "remove", Value::Null),
            ("thinking", "set", json!({"type":"disabled"})),
            ("output_config.effort", "set", json!("medium")),
        ] {
            let overrides = serde_json::from_value::<RequestOverridesConfig>(json!({
                "common":{"body_patch":[{"path":path,"op":op,"value":value}]}
            }))
            .unwrap();
            let mut body = json!({
                "max_tokens":4096,"thinking":{"type":"adaptive"},"output_config":{"effort":"high"}
            });
            apply_overrides_to_body(
                &mut body,
                Some(&overrides),
                ENDPOINT_MESSAGES,
                Some("fixture"),
            );
            let expected = body.clone();
            let dispatch = |diagnostics, mut body| {
                let overrides = &overrides;
                async move {
                    scope_completion_diagnostics(diagnostics, async {
                        mask_and_prepare_agent_request(
                            &mut body,
                            &Default::default(),
                            Some(overrides),
                            ENDPOINT_MESSAGES,
                            "fixture",
                        )
                        .unwrap();
                        body
                    })
                    .await
                }
            };
            let diagnostics = CompletionDiagnostics::for_agent_round(None);
            diagnostics.mark_compatible_messages();
            assert_eq!(dispatch(diagnostics.clone(), body.clone()).await, expected);
            let split = diagnostics.output_limit_recovery().unwrap();
            assert!(split.prefers_task_split(), "{path}");
            let continuation = CompletionDiagnostics::for_agent_round(Some(split.for_task_split()));
            continuation.mark_compatible_messages();
            assert_eq!(dispatch(continuation, body).await, expected);
        }
    }

    #[tokio::test]
    async fn missing_effort_only_allows_new_recovery_on_compatible_messages() {
        use crate::provider::{scope_completion_diagnostics, CompletionDiagnostics};
        use serde_json::json;

        for (endpoint, mut body) in [
            (ENDPOINT_MESSAGES, json!({"max_tokens":4096})),
            (
                ENDPOINT_CHAT_COMPLETIONS,
                json!({"max_completion_tokens":4096}),
            ),
            (ENDPOINT_RESPONSES, json!({"max_output_tokens":4096})),
            (
                ENDPOINT_STREAM_GENERATE_CONTENT,
                json!({"generationConfig":{"maxOutputTokens":4096}}),
            ),
        ] {
            let diagnostics = CompletionDiagnostics::for_agent_round(None);
            scope_completion_diagnostics(diagnostics.clone(), async {
                mask_and_prepare_agent_request(
                    &mut body,
                    &Default::default(),
                    None,
                    endpoint,
                    "fixture",
                )
                .unwrap();
            })
            .await;
            assert!(diagnostics.output_limit_recovery().is_none(), "{endpoint}");
        }
    }

    #[tokio::test]
    async fn recovery_accepts_a_tighter_window_and_records_the_actual_wire_limit() {
        use crate::provider::{scope_completion_diagnostics, CompletionDiagnostics};
        use serde_json::json;

        let mut body = json!({"model":"fixture","max_tokens":4096});
        let initial =
            CompletionDiagnostics::for_agent_round(None).with_model_window_limit(Some(4096));
        initial.mark_compatible_messages();
        scope_completion_diagnostics(initial.clone(), async {
            mask_and_prepare_agent_request(
                &mut body,
                &Default::default(),
                None,
                ENDPOINT_MESSAGES,
                "fixture",
            )
            .unwrap();
        })
        .await;
        let recovery = initial.output_limit_recovery().unwrap().for_task_split();
        let follow_up =
            CompletionDiagnostics::for_agent_round(Some(recovery.tighten_output_limit(3900)))
                .with_model_window_limit(Some(3900));
        follow_up.mark_compatible_messages();
        scope_completion_diagnostics(follow_up.clone(), async {
            mask_and_prepare_agent_request(
                &mut body,
                &Default::default(),
                None,
                ENDPOINT_MESSAGES,
                "fixture",
            )
            .unwrap();
        })
        .await;
        assert_eq!(body["max_tokens"], 3900);
        assert_eq!(follow_up.output_limit(), Some(3900));
        assert!(follow_up.output_limit_recovery().is_none());
    }

    #[tokio::test]
    async fn dynamic_window_rejects_a_changed_wire_model_before_observing_the_request() {
        use crate::provider::{scope_completion_diagnostics, CompletionDiagnostics};
        use serde_json::json;

        for (endpoint, mut body) in [
            (
                ENDPOINT_MESSAGES,
                json!({"model":"small","max_tokens":4096}),
            ),
            (
                ENDPOINT_CHAT_COMPLETIONS,
                json!({"model":"small","max_tokens":4096}),
            ),
            (
                ENDPOINT_RESPONSES,
                json!({"model":"small","max_output_tokens":4096}),
            ),
        ] {
            let diagnostics =
                CompletionDiagnostics::for_agent_round(None).with_model_window_limit(Some(4096));
            let error = scope_completion_diagnostics(diagnostics.clone(), async {
                mask_and_prepare_agent_request(
                    &mut body,
                    &Default::default(),
                    None,
                    endpoint,
                    "large",
                )
            })
            .await
            .unwrap_err();
            assert!(matches!(error, LLMError::RequestFailed(_)), "{endpoint}");
            assert_eq!(diagnostics.output_limit(), None, "{endpoint}");
        }
    }

    #[tokio::test]
    async fn provider_default_keeps_numeric_overrides_and_required_fields_guarded() {
        use crate::provider::{scope_completion_diagnostics, CompletionDiagnostics};
        use serde_json::json;

        let diagnostics = CompletionDiagnostics::for_agent_round(None)
            .with_model_window_limit(Some(4_096))
            .with_provider_default_output(true);
        scope_completion_diagnostics(diagnostics.clone(), async {
            let mut body = json!({"model": "fixture", "max_tokens": 6_000});
            mask_and_prepare_agent_request(
                &mut body,
                &Default::default(),
                None,
                ENDPOINT_CHAT_COMPLETIONS,
                "fixture",
            )
            .unwrap();
            assert_eq!(body["max_tokens"], 4_096);

            let error = mask_and_prepare_agent_request(
                &mut json!({"model": "fixture"}),
                &Default::default(),
                None,
                ENDPOINT_MESSAGES,
                "fixture",
            )
            .unwrap_err();
            assert!(matches!(error, LLMError::RequestFailed(_)));
        })
        .await;
        assert_eq!(diagnostics.output_limit(), Some(4_096));
    }

    #[tokio::test]
    async fn dynamic_chat_window_uses_one_limit_for_both_output_fields() {
        use crate::provider::{scope_completion_diagnostics, CompletionDiagnostics};
        use serde_json::json;

        let mut body = json!({"model":"fixture","max_tokens":4096,"max_completion_tokens":5000});
        let diagnostics =
            CompletionDiagnostics::for_agent_round(None).with_model_window_limit(Some(4500));
        scope_completion_diagnostics(diagnostics.clone(), async {
            mask_and_prepare_agent_request(
                &mut body,
                &Default::default(),
                None,
                ENDPOINT_CHAT_COMPLETIONS,
                "fixture",
            )
        })
        .await
        .unwrap();
        assert_eq!(body["max_tokens"], 4096);
        assert_eq!(body["max_completion_tokens"], 4096);
        assert_eq!(diagnostics.output_limit(), Some(4096));
    }

    #[tokio::test]
    async fn provider_tools_disable_all_output_limit_recovery() {
        use crate::provider::{scope_completion_diagnostics, CompletionDiagnostics};
        for (field, tools) in [
            (
                "tools",
                serde_json::json!([{"type":"web_search_20250305","name":"web_search"}]),
            ),
            (
                "mcp_servers",
                serde_json::json!([{"type":"url","name":"remote"}]),
            ),
        ] {
            let diagnostics = CompletionDiagnostics::for_agent_round(None);
            let mut body = serde_json::json!({
                "max_tokens":4096,"thinking":{"type":"adaptive"},"output_config":{"effort":"medium"}
            });
            body[field] = tools;
            scope_completion_diagnostics(diagnostics.clone(), async {
                mask_and_prepare_agent_request(
                    &mut body,
                    &Default::default(),
                    None,
                    ENDPOINT_MESSAGES,
                    "fixture",
                )
                .unwrap();
            })
            .await;
            assert!(diagnostics.output_limit_recovery().is_none(), "{field}");
        }
    }

    #[tokio::test]
    async fn work_budget_rejects_resolved_body_patch_and_keeps_legacy_behavior() {
        for (path, op, value) in [
            ("max_tokens", BodyPatchOp::Remove, None),
            (
                "max_output_tokens",
                BodyPatchOp::Set,
                Some(PatchValue::Json(Value::from(999999))),
            ),
            (
                "generationConfig.maxOutputTokens",
                BodyPatchOp::Set,
                Some(PatchValue::Json(Value::from(999999))),
            ),
            (
                "n",
                BodyPatchOp::Set,
                Some(PatchValue::Json(Value::from(10))),
            ),
        ] {
            let overrides = RequestOverridesConfig {
                common: RequestScopeOverride {
                    headers: Default::default(),
                    body_patch: vec![BodyPatch {
                        path: path.into(),
                        op,
                        value,
                    }],
                },
                ..Default::default()
            };
            assert!(validate_budgeted_overrides(
                Some(&overrides),
                ENDPOINT_RESPONSES,
                Some("local-model")
            )
            .is_ok());
            let scoped = crate::retry::WorkPhysicalRequest::default()
                .scope(async {
                    validate_budgeted_overrides(
                        Some(&overrides),
                        ENDPOINT_RESPONSES,
                        Some("local-model"),
                    )
                })
                .await;
            assert!(matches!(
                scoped,
                Err(crate::provider::LLMError::RequestFailed(_))
            ));
        }
        crate::retry::WorkPhysicalRequest::default()
            .scope(async {
                assert!(
                    validate_budgeted_overrides(None, ENDPOINT_RESPONSES, Some("local-model"))
                        .is_ok()
                );
                assert!(validate_budgeted_overrides(
                    Some(&RequestOverridesConfig::default()),
                    ENDPOINT_RESPONSES,
                    Some("local-model")
                )
                .is_ok());
            })
            .await;
    }

    #[test]
    fn model_rule_specificity_prefers_exact_over_prefix() {
        let mut endpoint_scope = RequestScopeOverride::default();
        endpoint_scope.headers.insert(
            "x-endpoint".to_string(),
            TemplateExpr::Literal("endpoint".to_string()),
        );

        let mut wildcard_scope = RequestScopeOverride::default();
        wildcard_scope.headers.insert(
            "x-rule".to_string(),
            TemplateExpr::Literal("wildcard".to_string()),
        );

        let mut exact_scope = RequestScopeOverride::default();
        exact_scope.headers.insert(
            "x-rule".to_string(),
            TemplateExpr::Literal("exact".to_string()),
        );

        let overrides = RequestOverridesConfig {
            common: RequestScopeOverride::default(),
            endpoints: [("responses".to_string(), endpoint_scope)]
                .into_iter()
                .collect(),
            rules: vec![
                ModelRequestRule {
                    model_pattern: "gpt-*".to_string(),
                    endpoint: Some("responses".to_string()),
                    scope: wildcard_scope,
                },
                ModelRequestRule {
                    model_pattern: "gpt-4o".to_string(),
                    endpoint: Some("responses".to_string()),
                    scope: exact_scope,
                },
            ],
        };

        let resolved = resolve_scope(&overrides, "responses", Some("gpt-4o"));
        assert_eq!(
            resolved.headers.get("x-rule"),
            Some(&TemplateExpr::Literal("exact".to_string()))
        );
        assert!(resolved.headers.contains_key("x-endpoint"));
    }

    #[test]
    fn template_resolution_supports_env_generated_and_format() {
        let env_vars = HashMap::from([
            ("TOKEN".to_string(), "abc".to_string()),
            ("TENANT".to_string(), "dev".to_string()),
        ]);

        let env = TemplateExpr::Structured(TemplateExprSpec::EnvRef {
            name: "TOKEN".to_string(),
            fallback: None,
        });
        let generated = TemplateExpr::Structured(TemplateExprSpec::Generated {
            generator: GeneratedValue::Uuid,
        });
        let formatted = TemplateExpr::Structured(TemplateExprSpec::Format {
            template: "Bearer {env:TOKEN}-{env:TENANT}-{unix_ms}".to_string(),
        });

        let resolved_env = resolve_template_expr(&env, &env_vars).expect("env ref should resolve");
        assert_eq!(resolved_env, "abc");

        let resolved_uuid =
            resolve_template_expr(&generated, &env_vars).expect("generated uuid should resolve");
        assert!(Uuid::parse_str(&resolved_uuid).is_ok());

        let resolved_format =
            resolve_template_expr(&formatted, &env_vars).expect("format should resolve");
        assert!(resolved_format.starts_with("Bearer abc-dev-"));
    }

    #[test]
    fn forbidden_headers_are_ignored() {
        let overrides = RequestOverridesConfig {
            common: RequestScopeOverride {
                headers: [
                    (
                        "host".to_string(),
                        TemplateExpr::Literal("example.com".to_string()),
                    ),
                    (
                        "content-length".to_string(),
                        TemplateExpr::Literal("1".to_string()),
                    ),
                    (
                        "x-custom".to_string(),
                        TemplateExpr::Literal("ok".to_string()),
                    ),
                ]
                .into_iter()
                .collect(),
                body_patch: vec![],
            },
            endpoints: Default::default(),
            rules: vec![],
        };

        let mut headers = HeaderMap::new();
        apply_overrides_to_header_map_with_env(
            &mut headers,
            Some(&overrides),
            ENDPOINT_CHAT_COMPLETIONS,
            Some("gpt-4o"),
            &HashMap::new(),
        );

        assert!(!headers.contains_key("host"));
        assert!(!headers.contains_key("content-length"));
        assert_eq!(
            headers
                .get("x-custom")
                .expect("x-custom should be present")
                .to_str()
                .expect("valid header"),
            "ok"
        );
    }

    #[test]
    fn body_patch_set_and_remove_follow_merged_order() {
        let overrides = RequestOverridesConfig {
            common: RequestScopeOverride {
                headers: Default::default(),
                body_patch: vec![BodyPatch {
                    path: "metadata.a".to_string(),
                    op: BodyPatchOp::Set,
                    value: Some(PatchValue::Json(Value::from(5))),
                }],
            },
            endpoints: [(
                ENDPOINT_RESPONSES.to_string(),
                RequestScopeOverride {
                    headers: Default::default(),
                    body_patch: vec![BodyPatch {
                        path: "metadata.c".to_string(),
                        op: BodyPatchOp::Set,
                        value: Some(PatchValue::Json(serde_json::json!({ "k": true }))),
                    }],
                },
            )]
            .into_iter()
            .collect(),
            rules: vec![ModelRequestRule {
                model_pattern: "gpt-4o".to_string(),
                endpoint: Some(ENDPOINT_RESPONSES.to_string()),
                scope: RequestScopeOverride {
                    headers: Default::default(),
                    body_patch: vec![
                        BodyPatch {
                            path: "metadata.b".to_string(),
                            op: BodyPatchOp::Remove,
                            value: None,
                        },
                        BodyPatch {
                            path: "metadata.a".to_string(),
                            op: BodyPatchOp::Set,
                            value: Some(PatchValue::Json(Value::from(9))),
                        },
                    ],
                },
            }],
        };

        let mut body = serde_json::json!({
            "metadata": { "a": 1, "b": 2 }
        });

        apply_overrides_to_body_with_env(
            &mut body,
            Some(&overrides),
            ENDPOINT_RESPONSES,
            Some("gpt-4o"),
            &HashMap::new(),
        );

        assert_eq!(body["metadata"]["a"], 9);
        assert!(body["metadata"].get("b").is_none());
        assert_eq!(body["metadata"]["c"]["k"], true);
    }
}
