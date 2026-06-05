//! Config-driven request override helpers shared by provider implementations.

use std::collections::HashMap;

use chrono::Utc;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Map, Value};
use uuid::Uuid;

use bamboo_config::{
    BodyPatchOp, ModelRequestRule, PatchValue, RequestOverridesConfig, RequestScopeOverride,
    TemplateExpr, TemplateExprSpec,
};

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
                "Skipping forbidden request override header '{}' for endpoint '{}'",
                header_name,
                endpoint
            );
            continue;
        }

        let Some(value) = resolve_template_expr(&expr, env_vars) else {
            tracing::warn!(
                "Skipping request override header '{}' because its value did not resolve",
                header_name
            );
            continue;
        };

        let parsed_name = match HeaderName::from_bytes(header_name.as_bytes()) {
            Ok(name) => name,
            Err(error) => {
                tracing::warn!(
                    "Skipping invalid request override header name '{}': {}",
                    header_name,
                    error
                );
                continue;
            }
        };
        let parsed_value = match HeaderValue::from_str(&value) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    "Skipping request override header '{}' due to invalid value: {}",
                    header_name,
                    error
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
                        "Skipping body_patch set op without value for path '{}'",
                        patch.path
                    );
                    continue;
                };
                let Some(value) = resolve_patch_value(raw, env_vars) else {
                    tracing::warn!(
                        "Skipping body_patch set op for path '{}' because value did not resolve",
                        patch.path
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
        dst.headers.insert(key.clone(), value.clone());
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
