use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use bamboo_llm::Config;
use bamboo_skills::access_control;
use bamboo_skills::runtime_metadata::{
    LAST_LOADED_SKILL_ID_METADATA_KEY, LAST_LOADED_SKILL_SUMMARY_METADATA_KEY,
    LOADED_SKILL_IDS_METADATA_KEY,
};
use bamboo_skills::SkillManager;

use bamboo_agent_core::tools::{
    FunctionCall, Tool, ToolCall, ToolCtx, ToolError, ToolExecutionContext,
    ToolExecutionSessionFlags, ToolExecutor, ToolOutcome, ToolResult,
};

use super::{
    skill_access_error_to_tool_error, validate_runtime_activation,
    validate_runtime_activation_descriptor, SkillToolAccess,
};

#[derive(Debug, Deserialize)]
struct LoadSkillArgs {
    skill_id: String,
}

const LOAD_SKILL_OWNED_METADATA_KEYS: &[&str] = &[
    bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_CATALOG_KEY,
    bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY,
    bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY,
    bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY,
    bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY,
    bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY,
    bamboo_skills::WORKFLOW_LAST_DYNAMIC_CONTEXT_METADATA_KEY,
    bamboo_skills::WORKFLOW_CONTEXT_CACHE_METADATA_KEY,
    LOADED_SKILL_IDS_METADATA_KEY,
    LAST_LOADED_SKILL_ID_METADATA_KEY,
    LAST_LOADED_SKILL_SUMMARY_METADATA_KEY,
];

pub struct LoadSkillTool {
    access: SkillToolAccess,
    context_tools: Option<Arc<dyn ToolExecutor>>,
    dynamic_context_permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
}

impl LoadSkillTool {
    pub fn new(
        skill_manager: Arc<SkillManager>,
        config: Arc<RwLock<Config>>,
        session_repo: bamboo_engine::SessionRepository,
    ) -> Self {
        Self {
            access: SkillToolAccess::new(skill_manager, config, session_repo),
            context_tools: None,
            dynamic_context_permission_config: None,
        }
    }

    /// Register the permission-wrapped base tool surface used by declared
    /// dynamic context providers. Keeping this explicit prevents arbitrary
    /// shell preprocessing and avoids recursive load_skill/workflow_run calls.
    pub fn with_fail_closed_context_registry(mut self, tools: Arc<dyn ToolExecutor>) -> Self {
        self.context_tools = Some(tools);
        self
    }

    /// Register the same permission-wrapped tool surface and typed policy used
    /// by normal tool dispatch. A missing typed config deliberately leaves the
    /// fail-closed registry above inactive.
    pub fn with_permission_checked_context_registry(
        mut self,
        tools: Arc<dyn ToolExecutor>,
        permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    ) -> Self {
        self.context_tools = Some(tools);
        self.dynamic_context_permission_config = permission_config;
        self
    }

    /// Test-only injection seam for provider behavior.
    #[cfg(test)]
    pub(crate) fn with_test_context_tools(mut self, tools: Arc<dyn ToolExecutor>) -> Self {
        self.context_tools = Some(tools);
        self.dynamic_context_permission_config =
            Some(Arc::new(bamboo_tools::permission::PermissionConfig::new()));
        self
    }

    async fn persist_owned_metadata(
        &self,
        session_id: &str,
        source: &bamboo_agent_core::Session,
        operation: &str,
    ) -> Result<(), ToolError> {
        let updates = LOAD_SKILL_OWNED_METADATA_KEYS
            .iter()
            .map(|key| ((*key).to_string(), source.metadata.get(*key).cloned()))
            .collect::<Vec<_>>();
        self.access
            .session_repo
            .update_runtime_session(session_id, LOAD_SKILL_OWNED_METADATA_KEYS, move |latest| {
                for (key, value) in updates {
                    if let Some(value) = value {
                        latest.metadata.insert(key, value);
                    } else {
                        latest.metadata.remove(&key);
                    }
                }
            })
            .await
            .map_err(|error| ToolError::Execution(format!("{operation}: {error}")))?
            .ok_or_else(|| ToolError::Execution(format!("Session '{session_id}' not found")))?;
        Ok(())
    }

    async fn resolve_dynamic_context(
        &self,
        skill: &bamboo_skills::SkillDefinition,
        session: &mut bamboo_agent_core::Session,
        ctx: &ToolCtx,
    ) -> Result<Vec<bamboo_skills::DynamicContextBlock>, ToolError> {
        const MAX_PROVIDERS: usize = 8;
        const MAX_PROVIDER_CHARS: usize = 16_384;
        const MAX_TOTAL_CHARS: usize = 32_768;
        const MAX_TIMEOUT_MS: u64 = 15_000;
        const MAX_CACHE_TTL_SECS: u64 = 3_600;
        const MAX_CACHE_ENTRIES: usize = 64;
        const MAX_CACHE_SERIALIZED_CHARS: usize = 131_072;
        const REGISTERED_DYNAMIC_CONTEXT_TOOLS: &[&str] = &[
            "Read",
            "read_file",
            "GetFileInfo",
            "Glob",
            "Grep",
            "list_directory",
        ];
        let now = chrono::Utc::now();
        let degraded_metadata = |message: &str| {
            vec![bamboo_skills::DynamicContextBlock {
                provider_id: "metadata".to_string(),
                tool: "none".to_string(),
                provenance: "invalid_workflow_metadata".to_string(),
                generated_at: now,
                expires_at: None,
                status: bamboo_skills::WorkflowActivationStatus::Degraded,
                stop_on_failure: true,
                content: String::new(),
                diagnostic: Some(bamboo_skills::WorkflowActivationDiagnostic {
                    code: bamboo_skills::WorkflowActivationErrorCode::ProviderOutputInvalid,
                    message: message.to_string(),
                    recoverable: true,
                }),
            }]
        };
        let declarations = match skill
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("dynamic_context"))
        {
            Some(value) => match serde_json::from_value::<
                Vec<bamboo_skills::DynamicContextDeclaration>,
            >(value.clone())
            {
                Ok(value) => value,
                Err(_) => {
                    return Ok(degraded_metadata(
                        "workflow dynamic context declaration metadata is invalid",
                    ));
                }
            },
            None => Vec::new(),
        };
        if declarations.is_empty() {
            return Ok(Vec::new());
        }
        if declarations.len() > MAX_PROVIDERS {
            return Ok(degraded_metadata(
                "workflow declares too many dynamic context providers",
            ));
        }
        let Some(permission_config) = self.dynamic_context_permission_config.as_ref() else {
            return Ok(declarations
                .into_iter()
                .map(|declaration| bamboo_skills::DynamicContextBlock {
                    provider_id: declaration.id,
                    tool: declaration.tool,
                    provenance: "typed_authority_unavailable".to_string(),
                    generated_at: now,
                    expires_at: None,
                    status: bamboo_skills::WorkflowActivationStatus::Degraded,
                    stop_on_failure: declaration.stop_on_failure,
                    content: String::new(),
                    diagnostic: Some(bamboo_skills::WorkflowActivationDiagnostic {
                        code: bamboo_skills::WorkflowActivationErrorCode::ProviderFailed,
                        message: "dynamic context provider authority is unavailable until the typed permission dependency is active".to_string(),
                        recoverable: true,
                    }),
                })
                .collect());
        };
        let Some(tools) = self.context_tools.as_ref() else {
            return Ok(degraded_metadata(
                "dynamic context provider registry is unavailable",
            ));
        };
        let mut cache = match session
            .metadata
            .get(bamboo_skills::WORKFLOW_CONTEXT_CACHE_METADATA_KEY)
        {
            Some(raw) => match serde_json::from_str::<bamboo_skills::DynamicContextCache>(raw) {
                Ok(cache) => cache,
                Err(_) => {
                    return Ok(degraded_metadata(
                        "workflow dynamic context cache metadata is invalid",
                    ));
                }
            },
            None => bamboo_skills::DynamicContextCache::new(),
        };
        cache.retain(|_, block| block.expires_at.is_some_and(|expires| expires > now));
        let mut blocks = Vec::new();
        let mut total_chars = 0usize;
        for declaration in declarations {
            if declaration.id.trim().is_empty()
                || declaration.tool.trim().is_empty()
                || !REGISTERED_DYNAMIC_CONTEXT_TOOLS
                    .iter()
                    .any(|tool| tool.eq_ignore_ascii_case(&declaration.tool))
            {
                return Ok(degraded_metadata(
                    "workflow dynamic context declaration is invalid",
                ));
            }
            if bamboo_agent_core::tools::classify_tool(&declaration.tool)
                != bamboo_agent_core::tools::ToolMutability::ReadOnly
            {
                return Ok(degraded_metadata(
                    "dynamic context provider is not classified read-only",
                ));
            }
            let provider_input = match confine_dynamic_provider_input(
                session,
                &declaration.tool,
                &declaration.input,
            )
            .await
            {
                Ok(input) => input,
                Err(diagnostic) => {
                    blocks.push(bamboo_skills::DynamicContextBlock {
                        provider_id: declaration.id,
                        tool: declaration.tool,
                        provenance: "workspace_confinement_denied".to_string(),
                        generated_at: now,
                        expires_at: None,
                        status: bamboo_skills::WorkflowActivationStatus::Degraded,
                        stop_on_failure: declaration.stop_on_failure,
                        content: String::new(),
                        diagnostic: Some(diagnostic),
                    });
                    if declaration.stop_on_failure {
                        break;
                    }
                    continue;
                }
            };
            let catalog_identity = session
                .metadata
                .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_CATALOG_KEY)
                .and_then(|raw| {
                    serde_json::from_str::<Vec<bamboo_skills::WorkflowCatalogEntry>>(raw).ok()
                })
                .and_then(|entries| entries.into_iter().find(|entry| entry.id == skill.id))
                .map(|entry| format!("{:?}:{}", entry.source, entry.revision))
                .unwrap_or_else(|| "unknown:0".to_string());
            let workspace_scope = session.workspace_path_meta().unwrap_or_default();
            let cache_material = json!({
                "workflow": skill.id,
                "catalog": catalog_identity,
                "provider": declaration.id,
                "tool": declaration.tool,
                "input": provider_input,
                "workspace": workspace_scope,
                "permission_scope": "strict_no_bypass",
                "permission_policy_revision": permission_config.policy_revision(),
            });
            let cache_key = hex::encode(Sha256::digest(cache_material.to_string().as_bytes()));
            // Cached content is never injected without re-running the concrete
            // tool permission gate. The current ToolExecutor surface has no
            // authorization-only check or immutable policy revision, so a raw
            // cache hit could bypass an allow -> deny/approval policy change.
            let available = tools.list_tools();
            let canonical_provider = bamboo_tools::exposure::canonical_tool_name(&declaration.tool);
            if !available.iter().any(|schema| {
                bamboo_tools::exposure::canonical_tool_name(&schema.function.name)
                    == canonical_provider
            }) {
                let diagnostic = bamboo_skills::WorkflowActivationDiagnostic {
                    code: bamboo_skills::WorkflowActivationErrorCode::ProviderFailed,
                    message: format!(
                        "dynamic context provider '{}' is not registered",
                        declaration.id
                    ),
                    recoverable: true,
                };
                blocks.push(bamboo_skills::DynamicContextBlock {
                    provider_id: declaration.id,
                    tool: declaration.tool,
                    provenance: "registered_tool".to_string(),
                    generated_at: now,
                    expires_at: None,
                    status: bamboo_skills::WorkflowActivationStatus::Degraded,
                    stop_on_failure: declaration.stop_on_failure,
                    content: String::new(),
                    diagnostic: Some(diagnostic),
                });
                if declaration.stop_on_failure {
                    break;
                }
                continue;
            }
            let call_id = format!("dynamic-context-{}-{}", skill.id, declaration.id);
            let call = ToolCall {
                id: call_id.clone(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: declaration.tool.clone(),
                    arguments: provider_input.to_string(),
                },
            };
            let (fallback_tx, _fallback_rx) = tokio::sync::mpsc::channel(1);
            let event_tx = ctx.event_tx.as_ref().unwrap_or(&fallback_tx);
            let execution_context = ToolExecutionContext::for_dispatch(
                session.id.as_str(),
                &call_id,
                event_tx,
                &available,
                // Dynamic context never inherits bypass. Every provider must
                // pass the normal permission/workspace gate independently.
                ToolExecutionSessionFlags::default(),
                false,
                None,
                Some(&provider_input),
            );
            let timeout_ms = declaration.timeout_ms.clamp(1, MAX_TIMEOUT_MS);
            let result = tokio::time::timeout(
                Duration::from_millis(timeout_ms),
                tools.execute_with_context_outcome(&call, execution_context),
            )
            .await;
            let block = match result {
                Ok(Ok(ToolOutcome::Completed(result)))
                    if result.success && !tool_result_requires_approval(&result) =>
                {
                    let max_chars = declaration.max_chars.clamp(1, MAX_PROVIDER_CHARS);
                    let redacted = redact_dynamic_context(&result.result);
                    let original_chars = redacted.chars().count();
                    let remaining_chars = MAX_TOTAL_CHARS.saturating_sub(total_chars);
                    let output_chars = max_chars.min(remaining_chars);
                    let content = redacted.chars().take(output_chars).collect::<String>();
                    let truncated = original_chars > output_chars;
                    total_chars = total_chars.saturating_add(content.chars().count());
                    let ttl = declaration.cache_ttl_secs.min(MAX_CACHE_TTL_SECS);
                    bamboo_skills::DynamicContextBlock {
                        provider_id: declaration.id,
                        tool: declaration.tool,
                        provenance: "registered_tool_permission_checked".to_string(),
                        generated_at: now,
                        expires_at: (ttl > 0)
                            .then(|| now + chrono::Duration::seconds(ttl as i64)),
                        status: bamboo_skills::WorkflowActivationStatus::Active,
                        stop_on_failure: declaration.stop_on_failure,
                        content,
                        diagnostic: truncated.then(|| bamboo_skills::WorkflowActivationDiagnostic {
                            code: bamboo_skills::WorkflowActivationErrorCode::ProviderOutputInvalid,
                            message: format!(
                                "dynamic context output was truncated from {original_chars} to {output_chars} characters"
                            ),
                            recoverable: true,
                        }),
                    }
                }
                other => {
                    let reason = match other {
                        Err(_) => "provider timed out".to_string(),
                        Ok(Err(error)) => format!("provider execution failed: {error}"),
                        Ok(Ok(ToolOutcome::Completed(result)))
                            if tool_result_requires_approval(&result) =>
                        {
                            "provider requires permission approval".to_string()
                        }
                        Ok(Ok(ToolOutcome::Completed(_))) => {
                            "provider returned an unsuccessful result".to_string()
                        }
                        Ok(Ok(ToolOutcome::NeedsHuman { .. })) => {
                            "provider requires human approval".to_string()
                        }
                        Ok(Ok(ToolOutcome::Running(_))) => {
                            "provider attempted detached execution".to_string()
                        }
                    };
                    bamboo_skills::DynamicContextBlock {
                        provider_id: declaration.id,
                        tool: declaration.tool,
                        provenance: "registered_tool_permission_checked".to_string(),
                        generated_at: now,
                        expires_at: None,
                        status: bamboo_skills::WorkflowActivationStatus::Degraded,
                        stop_on_failure: declaration.stop_on_failure,
                        content: String::new(),
                        diagnostic: Some(bamboo_skills::WorkflowActivationDiagnostic {
                            code: bamboo_skills::WorkflowActivationErrorCode::ProviderFailed,
                            message: reason,
                            recoverable: true,
                        }),
                    }
                }
            };
            tracing::info!(
                workflow_id = %skill.id,
                provider_id = %block.provider_id,
                tool = %block.tool,
                status = ?block.status,
                stop_on_failure = block.stop_on_failure,
                content_chars = block.content.chars().count(),
                "dynamic workflow context provider completed"
            );
            if block.expires_at.is_some() {
                cache.insert(cache_key, block.clone());
                while cache.len() > MAX_CACHE_ENTRIES
                    || serde_json::to_string(&cache)
                        .map(|raw| raw.chars().count() > MAX_CACHE_SERIALIZED_CHARS)
                        .unwrap_or(true)
                {
                    let Some(oldest_key) = cache
                        .iter()
                        .min_by_key(|(_, block)| block.generated_at)
                        .map(|(key, _)| key.clone())
                    else {
                        break;
                    };
                    cache.remove(&oldest_key);
                }
            }
            blocks.push(block);
            if blocks.last().is_some_and(|block| {
                block.status == bamboo_skills::WorkflowActivationStatus::Degraded
                    && block.stop_on_failure
            }) {
                break;
            }
        }
        session.metadata.insert(
            bamboo_skills::WORKFLOW_CONTEXT_CACHE_METADATA_KEY.to_string(),
            serde_json::to_string(&cache).unwrap_or_else(|_| "{}".to_string()),
        );
        Ok(blocks)
    }
}

fn confinement_diagnostic() -> bamboo_skills::WorkflowActivationDiagnostic {
    bamboo_skills::WorkflowActivationDiagnostic {
        code: bamboo_skills::WorkflowActivationErrorCode::ProviderFailed,
        message:
            "dynamic context provider input is not confined to the canonical session workspace"
                .to_string(),
        recoverable: true,
    }
}

fn tool_result_requires_approval(result: &ToolResult) -> bool {
    result
        .display_preference
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("request_permissions"))
        || serde_json::from_str::<serde_json::Value>(&result.result)
            .ok()
            .is_some_and(|value| {
                value["status"].as_str().is_some_and(|status| {
                    status.eq_ignore_ascii_case("awaiting_permission_approval")
                }) || value["request_permissions"].is_object()
            })
}

fn path_has_parent_component(path: &std::path::Path) -> bool {
    path.components()
        .any(|component| component == std::path::Component::ParentDir)
}

async fn canonicalize_scoped_path(
    workspace: &std::path::Path,
    raw: &str,
    allow_missing_leaf: bool,
) -> Result<std::path::PathBuf, bamboo_skills::WorkflowActivationDiagnostic> {
    let path = std::path::PathBuf::from(raw.trim());
    if raw.trim().is_empty() || path_has_parent_component(&path) {
        return Err(confinement_diagnostic());
    }
    let candidate = if path.is_absolute() {
        path
    } else {
        workspace.join(path)
    };
    match tokio::fs::canonicalize(&candidate).await {
        Ok(canonical) if canonical.starts_with(workspace) => return Ok(canonical),
        Ok(_) => return Err(confinement_diagnostic()),
        Err(error) if allow_missing_leaf && error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(confinement_diagnostic()),
    }

    // GetFileInfo intentionally supports `exists:false`. Canonicalize the
    // nearest existing ancestor (including symlinks), verify it remains in the
    // workspace, then append only the already parent-free missing suffix.
    let mut ancestor = candidate.as_path();
    while !ancestor.exists() {
        ancestor = ancestor.parent().ok_or_else(confinement_diagnostic)?;
    }
    let canonical_ancestor = tokio::fs::canonicalize(ancestor)
        .await
        .map_err(|_| confinement_diagnostic())?;
    if !canonical_ancestor.starts_with(workspace) {
        return Err(confinement_diagnostic());
    }
    let suffix = candidate
        .strip_prefix(ancestor)
        .map_err(|_| confinement_diagnostic())?;
    Ok(canonical_ancestor.join(suffix))
}

async fn confine_dynamic_provider_input(
    session: &bamboo_agent_core::Session,
    tool: &str,
    input: &serde_json::Value,
) -> Result<serde_json::Value, bamboo_skills::WorkflowActivationDiagnostic> {
    let workspace = session
        .workspace_path_meta()
        .filter(|path| !path.trim().is_empty())
        .ok_or_else(confinement_diagnostic)?;
    let workspace = tokio::fs::canonicalize(workspace)
        .await
        .map_err(|_| confinement_diagnostic())?;
    let mut normalized = input
        .as_object()
        .cloned()
        .ok_or_else(confinement_diagnostic)?;
    let canonical_tool = tool.to_ascii_lowercase();
    if matches!(canonical_tool.as_str(), "read" | "read_file") {
        let raw = normalized
            .get("file_path")
            .or_else(|| normalized.get("path"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(confinement_diagnostic)?;
        let canonical = canonicalize_scoped_path(&workspace, raw, false).await?;
        normalized.remove("path");
        normalized.insert(
            "file_path".to_string(),
            serde_json::Value::String(canonical.to_string_lossy().into_owned()),
        );
    } else if canonical_tool == "getfileinfo" {
        let raw = normalized
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(confinement_diagnostic)?;
        let canonical = canonicalize_scoped_path(&workspace, raw, true).await?;
        normalized.remove("file_path");
        normalized.insert(
            "path".to_string(),
            serde_json::Value::String(canonical.to_string_lossy().into_owned()),
        );
    } else if matches!(canonical_tool.as_str(), "glob" | "grep" | "list_directory") {
        let raw = normalized
            .get("path")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(".");
        let canonical = canonicalize_scoped_path(&workspace, raw, false).await?;
        normalized.insert(
            "path".to_string(),
            serde_json::Value::String(canonical.to_string_lossy().into_owned()),
        );
        for pattern_key in ["pattern", "glob"] {
            if let Some(pattern) = normalized
                .get(pattern_key)
                .and_then(serde_json::Value::as_str)
            {
                let pattern_path = std::path::Path::new(pattern);
                if pattern_path.is_absolute() || path_has_parent_component(pattern_path) {
                    return Err(confinement_diagnostic());
                }
            }
        }
    } else {
        return Err(confinement_diagnostic());
    }
    Ok(serde_json::Value::Object(normalized))
}

fn redact_dynamic_context(raw: &str) -> String {
    fn redact_value(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, value) in map {
                    let sensitive = [
                        "token",
                        "secret",
                        "password",
                        "authorization",
                        "cookie",
                        "api_key",
                    ]
                    .iter()
                    .any(|needle| key.to_ascii_lowercase().contains(needle));
                    if sensitive {
                        *value = serde_json::Value::String("[REDACTED]".to_string());
                    } else {
                        redact_value(value);
                    }
                }
            }
            serde_json::Value::Array(values) => values.iter_mut().for_each(redact_value),
            serde_json::Value::String(text) => {
                let lower = text.to_ascii_lowercase();
                if [
                    "-----begin",
                    "private key-----",
                    "bearer ",
                    "akia",
                    "github_pat_",
                    "ghp_",
                    "sk-",
                ]
                .iter()
                .any(|marker| lower.contains(marker))
                {
                    *text = "[REDACTED]".to_string();
                }
            }
            _ => {}
        }
    }
    if let Ok(mut value) = serde_json::from_str::<serde_json::Value>(raw) {
        redact_value(&mut value);
        return value.to_string();
    }
    let mut in_private_key = false;
    raw.lines()
        .map(|line| {
            let lower = line.to_ascii_lowercase();
            if lower.contains("-----begin") && lower.contains("private key-----") {
                in_private_key = true;
                return "[REDACTED]".to_string();
            }
            if in_private_key {
                if lower.contains("-----end") && lower.contains("private key-----") {
                    in_private_key = false;
                }
                return "[REDACTED]".to_string();
            }
            if [
                "token=",
                "token:",
                "secret=",
                "secret:",
                "password=",
                "password:",
                "authorization:",
                "api_key=",
                "api_key:",
                "bearer ",
                "akia",
                "github_pat_",
                "ghp_",
                "sk-",
            ]
            .iter()
            .any(|needle| lower.contains(needle))
            {
                "[REDACTED]".to_string()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod redaction_tests {
    use super::{confine_dynamic_provider_input, redact_dynamic_context};
    use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolExecutor};
    use bamboo_agent_core::Session;
    use bamboo_tools::BuiltinToolExecutorBuilder;

    async fn execute(
        executor: &dyn ToolExecutor,
        name: &str,
        input: serde_json::Value,
    ) -> bamboo_agent_core::tools::ToolResult {
        let call = ToolCall {
            id: format!("test-{name}"),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: input.to_string(),
            },
        };
        executor.execute(&call).await.expect("builtin executes")
    }

    #[test]
    fn redacts_common_non_json_secret_formats() {
        let raw = "safe=value\ntoken: tok-value\nAuthorization: Bearer bearer-value\nAWS_ACCESS_KEY_ID=AKIAABCDEFGHIJKLMNOP\nghp_exampletoken\n-----BEGIN PRIVATE KEY-----\nprivate-material\n-----END PRIVATE KEY-----\nafter=safe";
        let redacted = redact_dynamic_context(raw);
        assert!(redacted.contains("safe=value"));
        assert!(redacted.contains("after=safe"));
        for secret in [
            "tok-value",
            "bearer-value",
            "AKIAABCDEFGHIJKLMNOP",
            "ghp_exampletoken",
            "private-material",
        ] {
            assert!(!redacted.contains(secret), "leaked {secret}");
        }

        let json = redact_dynamic_context(
            r#"{"data":"-----BEGIN PRIVATE KEY----- hidden","value":"ghp_json_secret"}"#,
        );
        assert!(!json.contains("hidden"));
        assert!(!json.contains("ghp_json_secret"));
    }

    #[tokio::test]
    async fn real_builtins_are_workspace_confined_with_aliases_and_missing_file_info() {
        let workspace = tempfile::tempdir().expect("workspace");
        let outside = tempfile::tempdir().expect("outside");
        std::fs::create_dir_all(workspace.path().join("src")).expect("src");
        std::fs::write(
            workspace.path().join("src/inside.txt"),
            "WORKSPACE_ONLY_MARKER",
        )
        .expect("inside file");
        std::fs::write(outside.path().join("secret.txt"), "OUTSIDE_SECRET").expect("outside");
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), workspace.path().join("escape"))
            .expect("escape symlink");

        let mut session = Session::new("confinement", "model");
        session.set_workspace_path_meta(workspace.path().to_string_lossy().into_owned());
        let canonical_workspace =
            std::fs::canonicalize(workspace.path()).expect("canonical workspace");
        let executor = BuiltinToolExecutorBuilder::new()
            .with_default_tools()
            .build();

        for (name, input, expected_field) in [
            (
                "Read",
                serde_json::json!({"file_path": "src/inside.txt"}),
                "file_path",
            ),
            (
                "read_file",
                serde_json::json!({"path": "src/inside.txt"}),
                "file_path",
            ),
            (
                "Glob",
                serde_json::json!({"path": "src", "pattern": "*.txt"}),
                "path",
            ),
            ("list_directory", serde_json::json!({"path": "src"}), "path"),
            (
                "Grep",
                serde_json::json!({"path": "src", "pattern": "WORKSPACE_ONLY"}),
                "path",
            ),
        ] {
            let confined = confine_dynamic_provider_input(&session, name, &input)
                .await
                .unwrap_or_else(|error| panic!("{name} should be confined: {}", error.message));
            assert!(
                confined[expected_field].as_str().is_some_and(
                    |path| path.starts_with(canonical_workspace.to_string_lossy().as_ref())
                ),
                "{name} normalized input: {confined}"
            );
            let result = execute(&executor, name, confined).await;
            assert!(result.success, "{name}: {}", result.result);
            assert!(!result.result.contains("OUTSIDE_SECRET"));
        }

        let missing = confine_dynamic_provider_input(
            &session,
            "GetFileInfo",
            &serde_json::json!({"path": "src/missing.txt"}),
        )
        .await
        .expect("missing workspace leaf is safe");
        assert!(missing.get("file_path").is_none());
        let result = execute(&executor, "GetFileInfo", missing).await;
        assert!(result.success);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&result.result).expect("file info json")
                ["exists"],
            false
        );

        for (name, input) in [
            (
                "Read",
                serde_json::json!({"file_path": outside.path().join("secret.txt")}),
            ),
            (
                "Read",
                serde_json::json!({"file_path": "../outside/secret.txt"}),
            ),
            (
                "GetFileInfo",
                serde_json::json!({"path": "escape/missing.txt"}),
            ),
        ] {
            let error = confine_dynamic_provider_input(&session, name, &input)
                .await
                .expect_err("escape must be denied");
            assert!(!error.message.contains("secret.txt"));
            assert!(!error
                .message
                .contains(outside.path().to_string_lossy().as_ref()));
        }

        let no_workspace = Session::new("no-workspace", "model");
        assert!(confine_dynamic_provider_input(
            &no_workspace,
            "Read",
            &serde_json::json!({"file_path": "src/inside.txt"})
        )
        .await
        .is_err());
    }
}

#[async_trait]
impl Tool for LoadSkillTool {
    fn name(&self) -> &str {
        "load_skill"
    }

    fn description(&self) -> &str {
        "Load a skill's detailed SKILL.md instructions by skill_id."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "skill_id": {
                    "type": "string",
                    "description": "Skill ID from the advertised skill list (for example: skill-creator)."
                }
            },
            "required": ["skill_id"]
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: LoadSkillArgs = serde_json::from_value(args).map_err(|err| {
            ToolError::InvalidArguments(format!("Invalid load_skill args: {err}"))
        })?;
        let skill_id = parsed.skill_id.trim();
        if skill_id.is_empty() {
            return Err(ToolError::InvalidArguments(
                "skill_id must be a non-empty string".to_string(),
            ));
        }

        let session_id = ctx.session_id().ok_or_else(|| {
            ToolError::Execution("load_skill requires a session_id in tool context".to_string())
        })?;
        let store = self.access.skill_store(ctx.session_id()).await?;
        let reused_published_activation =
            validate_runtime_activation(&self.access, store.as_ref(), session_id, skill_id).await?;
        if !reused_published_activation {
            access_control::ensure_skill_allowed(&self.access, skill_id, ctx.session_id())
                .await
                .map_err(skill_access_error_to_tool_error)?;
            let skill_mode =
                access_control::selected_skill_mode(&self.access, ctx.session_id()).await;
            let selected_ids =
                access_control::selected_skill_allowlist(&self.access, ctx.session_id())
                    .await
                    .ok_or_else(|| {
                        ToolError::Execution(
                            "load_skill cannot pin a request with no published skill selection"
                                .to_string(),
                        )
                    })?
                    .into_iter()
                    .collect::<Vec<_>>();
            let session = self
                .access
                .session_for_context(Some(session_id))
                .await
                .ok_or_else(|| ToolError::Execution(format!("Session '{session_id}' not found")))?;
            let workspace = session.workspace_path_meta().map(std::path::PathBuf::from);
            self.access
                .skill_manager
                .pin_current_activation_for_workspace(
                    session_id,
                    workspace.as_deref(),
                    &selected_ids,
                    skill_mode.as_deref(),
                )
                .await
                .map_err(|err| {
                    ToolError::Execution(format!(
                        "Failed to pin workflow activation for '{skill_id}': {err}"
                    ))
                })?;
        }
        let (skill, skill_root, revision, resources, payload_descriptor) = store
            .get_pinned_skill_with_root_and_descriptor(session_id, skill_id)
            .await
            .map_err(|err| {
                ToolError::Execution(format!("Failed to load skill '{skill_id}': {err}"))
            })?;
        validate_runtime_activation_descriptor(
            &self.access,
            &payload_descriptor,
            session_id,
            skill_id,
        )
        .await?;
        let catalog_entry = store
            .pinned_activation_catalog_entries(session_id)
            .await
            .and_then(|entries| entries.into_iter().find(|entry| entry.id == skill_id))
            .ok_or_else(|| {
                ToolError::Execution("pinned workflow catalog entry is missing".to_string())
            })?;
        if catalog_entry.kind != bamboo_skills::WorkflowKind::Instruction {
            return Err(ToolError::InvalidArguments(
                "orchestration workflows cannot be loaded as instruction skills; use workflow_run"
                    .to_string(),
            ));
        }
        let restored_snapshot = store.activation_was_restored(session_id).await;
        let canonical_skill_root = if restored_snapshot {
            None
        } else {
            Some(
                tokio::fs::canonicalize(&skill_root)
                    .await
                    .unwrap_or(skill_root),
            )
        };
        let mut session = self
            .access
            .session_for_context(Some(session_id))
            .await
            .ok_or_else(|| ToolError::Execution(format!("Session '{session_id}' not found")))?;
        // Direct SDK/tool integrations may pin through the guarded fallback
        // above without running the agent's setup publisher. Materialize the
        // same immutable catalog/snapshot metadata from that pin; never rebuild
        // it from the live catalog after the fact.
        if !reused_published_activation
            || !session
                .metadata
                .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_CATALOG_KEY)
        {
            let pinned_catalog = store
                .pinned_activation_catalog_entries(session_id)
                .await
                .ok_or_else(|| {
                    ToolError::Execution("pinned workflow catalog is unavailable".to_string())
                })?;
            session.metadata.insert(
                bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_CATALOG_KEY.to_string(),
                serde_json::to_string(&pinned_catalog).map_err(|_| {
                    ToolError::Execution("pinned workflow catalog is invalid".to_string())
                })?,
            );
        }
        if !reused_published_activation
            || !session
                .metadata
                .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY)
        {
            let snapshot = store
                .export_activation_snapshot(session_id)
                .await
                .ok_or_else(|| {
                    ToolError::Execution("pinned workflow snapshot is unavailable".to_string())
                })?;
            session.metadata.insert(
                bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY.to_string(),
                serde_json::to_string(&snapshot).map_err(|_| {
                    ToolError::Execution("pinned workflow snapshot is invalid".to_string())
                })?,
            );
        }
        if let Some(selection) = session
            .metadata
            .get(bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<bamboo_skills::WorkflowSelection>(raw).ok())
        {
            bamboo_domain::validate_schema(&catalog_entry.argument_schema, &selection.args)
                .map_err(|error| {
                    ToolError::InvalidArguments(format!(
                        "workflow arguments do not match the pinned schema: {error}"
                    ))
                })?;
        }
        let dynamic_context = self
            .resolve_dynamic_context(&skill, &mut session, &ctx)
            .await?;
        session.metadata.insert(
            bamboo_skills::WORKFLOW_LAST_DYNAMIC_CONTEXT_METADATA_KEY.to_string(),
            serde_json::to_string(&dynamic_context).unwrap_or_else(|_| "[]".to_string()),
        );
        let activation_stopped = dynamic_context.iter().any(|block| {
            block.status == bamboo_skills::WorkflowActivationStatus::Degraded
                && block.stop_on_failure
        });
        let payload = json!({
            "skill_id": skill.id.clone(),
            "revision": revision,
            "name": skill.name.clone(),
            "description": skill.description.clone(),
            "license": skill.license.clone(),
            "compatibility": skill.compatibility.clone(),
            "allowed_tools": skill.tool_refs.clone(),
            "instructions": skill.prompt.clone(),
            "skill_base_dir": canonical_skill_root
                .as_ref()
                .map(|root| bamboo_config::paths::path_to_display_string(root)),
            "snapshot_provenance": if restored_snapshot { "durable_session_lkg" } else { "live_catalog_pin" },
            "resource_files": resources,
            "dynamic_context": dynamic_context.clone(),
            // Non-stopping provider degradation is carried inside the typed
            // runtime blocks while the workflow itself remains active.
            "activation_status": if activation_stopped { "degraded" } else { "active" },
        });
        if activation_stopped {
            session.metadata.insert(
                bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY.to_string(),
                serde_json::to_string(&dynamic_context).unwrap_or_else(|_| {
                    "dynamic workflow context degraded before activation".to_string()
                }),
            );
            session
                .metadata
                .remove(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY);
            session
                .metadata
                .remove(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY);
            self.persist_owned_metadata(
                session_id,
                &session,
                "degraded workflow state could not be persisted",
            )
            .await?;
            return Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: payload.to_string(),
                display_preference: Some("Collapsible".to_string()),
                images: Vec::new(),
            }));
        }
        let canonical_context = json!({
            "id": skill.id,
            "revision": revision,
            "selection": session.metadata.get(bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY),
            "instructions": skill.prompt,
            "dynamic_context": payload.get("dynamic_context"),
        });
        let fingerprint = hex::encode(Sha256::digest(canonical_context.to_string().as_bytes()));
        let mut loaded_ids = session
            .metadata
            .get(LOADED_SKILL_IDS_METADATA_KEY)
            .map(|raw| access_control::parse_loaded_skill_ids(raw))
            .unwrap_or_default();
        loaded_ids.insert(skill_id.to_string());
        session.metadata.insert(
            LOADED_SKILL_IDS_METADATA_KEY.to_string(),
            access_control::serialize_loaded_skill_ids(&loaded_ids),
        );
        session.metadata.insert(
            LAST_LOADED_SKILL_ID_METADATA_KEY.to_string(),
            skill_id.to_string(),
        );
        session.metadata.insert(
            LAST_LOADED_SKILL_SUMMARY_METADATA_KEY.to_string(),
            json!({"skill_id": skill_id, "loaded_count": loaded_ids.len()}).to_string(),
        );
        let already_active = session
            .metadata
            .get(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<bamboo_skills::ActiveWorkflow>(raw).ok())
            .is_some_and(|active| {
                active.id == skill_id
                    && active.revision == revision
                    && active.status == bamboo_skills::WorkflowActivationStatus::Active
            });
        if already_active {
            // Repeated model calls may reload the payload, but they are not a
            // second activation and must not duplicate lifecycle events.
            self.persist_owned_metadata(
                session_id,
                &session,
                "workflow loaded state could not be persisted",
            )
            .await?;
            return Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: payload.to_string(),
                display_preference: Some("Collapsible".to_string()),
                images: Vec::new(),
            }));
        }
        let active = match bamboo_skills::record_loaded_workflow_activation(
            &mut session.metadata,
            skill_id,
            fingerprint,
        ) {
            Ok(active) => active,
            Err(diagnostic) => {
                session.metadata.insert(
                    bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY.to_string(),
                    serde_json::to_string(&diagnostic)
                        .unwrap_or_else(|_| diagnostic.message.clone()),
                );
                session
                    .metadata
                    .remove(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY);
                session
                    .metadata
                    .remove(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY);
                session
                    .metadata
                    .remove(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY);
                self.persist_owned_metadata(
                    session_id,
                    &session,
                    "degraded workflow activation could not be persisted",
                )
                .await?;
                return Ok(ToolOutcome::Completed(ToolResult {
                    success: true,
                    result: json!({
                        "skill_id": skill_id,
                        "revision": revision,
                        "activation_status": "degraded",
                        "diagnostic": diagnostic,
                    })
                    .to_string(),
                    display_preference: Some("Collapsible".to_string()),
                    images: Vec::new(),
                }));
            }
        };
        self.persist_owned_metadata(
            session_id,
            &session,
            "workflow activation could not be persisted",
        )
        .await?;
        let pending_event = session
            .metadata
            .get(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .ok_or_else(|| {
                ToolError::Execution("workflow activation event metadata is invalid".to_string())
            })?;
        let activation_event = bamboo_agent_core::AgentEvent::WorkflowActivated {
            event_id: bamboo_skills::workflow_lifecycle_event_id(session_id, &pending_event),
            session_id: session_id.to_string(),
            workflow_id: active.id,
            revision: active.revision,
            invoked_by: format!("{:?}", active.invoked_by).to_ascii_lowercase(),
        };
        if let Some(sender) = ctx.cloned_sender() {
            if sender.send(activation_event).await.is_ok() {
                // The persisted pending event is the crash-recovery publication
                // record. Remove it only after the live runner accepted it.
                session
                    .metadata
                    .remove(bamboo_skills::WORKFLOW_ACTIVATION_EVENT_METADATA_KEY);
                self.persist_owned_metadata(
                    session_id,
                    &session,
                    "workflow activation event acknowledgement could not be persisted",
                )
                .await?;
            }
        }

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: payload.to_string(),
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        }))
    }
}
