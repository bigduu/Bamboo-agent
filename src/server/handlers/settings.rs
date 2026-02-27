use crate::core::keyword_masking::{KeywordEntry, KeywordMaskingConfig};
use crate::core::{Config, ProxyAuth};
use crate::server::config_manager;
use crate::server::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};
use actix_web::{web, HttpResponse};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs;

use crate::agent::llm::AVAILABLE_PROVIDERS;

// ============================================================================
// Response Types
// ============================================================================

/// Workflow list item for API responses
#[derive(Serialize)]
struct WorkflowListItem {
    /// Workflow name
    name: String,
    /// Filename (e.g., "myworkflow.md")
    filename: String,
    /// File size in bytes
    size: u64,
    /// Last modified timestamp (currently not populated)
    modified_at: Option<String>,
}

/// Full workflow data with content
#[derive(Serialize)]
struct WorkflowGetResponse {
    /// Workflow name
    name: String,
    /// Filename
    filename: String,
    /// Workflow markdown content
    content: String,
    /// File size in bytes
    size: u64,
    /// Last modified timestamp (currently not populated)
    modified_at: Option<String>,
}

// ============================================================================
// Helper Functions
// ============================================================================

fn redact_config_for_api(mut value: Value, config: &Config) -> Value {
    // Never send decrypted secrets. Also avoid sending encrypted key material.
    if let Some(root) = value.as_object_mut() {
        root.remove("proxy_auth_encrypted");
        // Back-compat: older Bodhi/Tauri stored proxy auth using these keys.
        root.remove("http_proxy_auth_encrypted");
        root.remove("https_proxy_auth_encrypted");

        if let Some(providers) = root.get_mut("providers").and_then(|v| v.as_object_mut()) {
            for (name, provider_cfg) in providers.iter_mut() {
                let Some(provider_obj) = provider_cfg.as_object_mut() else {
                    continue;
                };

                provider_obj.remove("api_key_encrypted");

                let configured = match name.as_str() {
                    "openai" => config
                        .providers
                        .openai
                        .as_ref()
                        .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
                        .unwrap_or(false),
                    "anthropic" => config
                        .providers
                        .anthropic
                        .as_ref()
                        .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
                        .unwrap_or(false),
                    "gemini" => config
                        .providers
                        .gemini
                        .as_ref()
                        .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
                        .unwrap_or(false),
                    _ => false,
                };

                if configured {
                    provider_obj.insert(
                        "api_key".to_string(),
                        Value::String("****...****".to_string()),
                    );
                } else {
                    provider_obj.remove("api_key");
                }
            }
        }

        // MCP config may contain credentials in env vars / headers. Do not return either plaintext
        // or encrypted blobs to clients; return masked placeholders instead.
        //
        // On disk / API we use the mainstream `mcpServers` key (map form). We still support
        // older installs that may have been serialized as `mcp` (legacy list form).
        if let Some(mcp_servers) = root.get_mut("mcpServers").and_then(|v| v.as_object_mut()) {
            for (_server_id, server_cfg) in mcp_servers.iter_mut() {
                let Some(server_obj) = server_cfg.as_object_mut() else {
                    continue;
                };

                // stdio server: env values masked
                if server_obj.get("command").is_some() {
                    // Drop legacy encrypted blobs if present.
                    let mut keys: Vec<String> = server_obj
                        .get("env_encrypted")
                        .and_then(|v| v.as_object())
                        .map(|obj| obj.keys().cloned().collect())
                        .unwrap_or_default();
                    server_obj.remove("env_encrypted");

                    if let Some(env_obj) = server_obj.get_mut("env").and_then(|v| v.as_object_mut())
                    {
                        for (_k, v) in env_obj.iter_mut() {
                            *v = Value::String("****...****".to_string());
                        }
                    } else if !keys.is_empty() {
                        // If the config only had encrypted env vars, still expose the keys so
                        // clients can see which variables are configured.
                        let env_obj = keys
                            .drain(..)
                            .map(|k| (k, Value::String("****...****".to_string())))
                            .collect::<serde_json::Map<String, Value>>();
                        server_obj.insert("env".to_string(), Value::Object(env_obj));
                    }
                }

                // sse server: header values masked
                if server_obj.get("url").is_some() {
                    // Mainstream style: headers is an object map.
                    if let Some(headers_obj) = server_obj
                        .get_mut("headers")
                        .and_then(|v| v.as_object_mut())
                    {
                        for (_k, v) in headers_obj.iter_mut() {
                            *v = Value::String("****...****".to_string());
                        }
                    }

                    // Legacy style: headers is an array of {name,value,value_encrypted}.
                    if let Some(headers) =
                        server_obj.get_mut("headers").and_then(|v| v.as_array_mut())
                    {
                        for header in headers.iter_mut() {
                            let Some(header_obj) = header.as_object_mut() else {
                                continue;
                            };
                            header_obj.remove("value_encrypted");
                            header_obj.insert(
                                "value".to_string(),
                                Value::String("****...****".to_string()),
                            );
                        }
                    }
                }
            }
        } else if let Some(mcp) = root.get_mut("mcp").and_then(|v| v.as_object_mut()) {
            // Legacy list-form redaction (best-effort).
            if let Some(servers) = mcp.get_mut("servers").and_then(|v| v.as_array_mut()) {
                for server in servers.iter_mut() {
                    let Some(server_obj) = server.as_object_mut() else {
                        continue;
                    };
                    let server_id = server_obj
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();

                    let Some(transport) = server_obj
                        .get_mut("transport")
                        .and_then(|v| v.as_object_mut())
                    else {
                        continue;
                    };

                    let transport_type = transport
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();

                    match transport_type {
                        "stdio" => {
                            let mut keys: Vec<String> = transport
                                .get("env_encrypted")
                                .and_then(|v| v.as_object())
                                .map(|obj| obj.keys().cloned().collect())
                                .unwrap_or_default();

                            if keys.is_empty() {
                                if let Some(cfg_server) =
                                    config.mcp.servers.iter().find(|s| s.id == server_id)
                                {
                                    if let crate::agent::mcp::TransportConfig::Stdio(stdio) =
                                        &cfg_server.transport
                                    {
                                        keys = stdio.env.keys().cloned().collect();
                                    }
                                }
                            }

                            transport.remove("env_encrypted");
                            let env_obj = keys
                                .into_iter()
                                .map(|k| (k, Value::String("****...****".to_string())))
                                .collect::<serde_json::Map<String, Value>>();
                            transport.insert("env".to_string(), Value::Object(env_obj));
                        }
                        "sse" => {
                            if let Some(headers) =
                                transport.get_mut("headers").and_then(|v| v.as_array_mut())
                            {
                                for header in headers.iter_mut() {
                                    let Some(header_obj) = header.as_object_mut() else {
                                        continue;
                                    };
                                    header_obj.remove("value_encrypted");
                                    header_obj.insert(
                                        "value".to_string(),
                                        Value::String("****...****".to_string()),
                                    );
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    value
}

fn redact_providers_for_api(mut value: Value, config: &Config) -> Value {
    let Some(obj) = value.as_object_mut() else {
        return value;
    };

    for (name, provider_cfg) in obj.iter_mut() {
        let Some(provider_obj) = provider_cfg.as_object_mut() else {
            continue;
        };

        provider_obj.remove("api_key_encrypted");

        let configured = match name.as_str() {
            "openai" => config
                .providers
                .openai
                .as_ref()
                .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
                .unwrap_or(false),
            "anthropic" => config
                .providers
                .anthropic
                .as_ref()
                .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
                .unwrap_or(false),
            "gemini" => config
                .providers
                .gemini
                .as_ref()
                .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
                .unwrap_or(false),
            _ => false,
        };

        if configured {
            provider_obj.insert(
                "api_key".to_string(),
                Value::String("****...****".to_string()),
            );
        } else {
            provider_obj.remove("api_key");
        }
    }

    value
}

/// Validates workflow names for security (prevents path traversal, etc.)
fn is_safe_workflow_name(name: &str) -> bool {
    // Check basic constraints
    if name.is_empty() || name.len() > 255 {
        return false;
    }

    // Trim and check for whitespace issues
    let trimmed = name.trim();
    if trimmed != name || trimmed.is_empty() {
        return false;
    }

    // Check for path separators and traversal patterns
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return false;
    }

    // Check for null bytes and control characters
    if name.chars().any(|c| c.is_control() || c == '\0') {
        return false;
    }

    // Check for reserved Windows names
    let upper = name.to_uppercase();
    let stem = upper.split('.').next().unwrap_or(&upper);
    let reserved = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    if reserved.contains(&stem) {
        return false;
    }

    // Only allow alphanumeric, dash, underscore, dot, and space
    name.chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == ' ')
}

// ============================================================================
// Workflow Handlers
// ============================================================================

/// Lists all workflow markdown files
///
/// # HTTP Route
/// `GET /bamboo/workflows`
///
/// # Response Format
/// Returns array of [`WorkflowListItem`]:
/// ```json
/// [
///   {
///     "name": "myworkflow",
///     "filename": "myworkflow.md",
///     "size": 1234,
///     "modified_at": null
///   }
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved workflow list
///
/// # Example
/// ```bash
/// curl http://localhost:3000/bamboo/workflows
/// ```
pub async fn list_workflows(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let dir = app_state.app_data_dir.join("workflows");

    fs::create_dir_all(&dir).await?;

    let mut entries = fs::read_dir(&dir).await?;
    let mut workflows: Vec<WorkflowListItem> = Vec::new();

    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_file() {
            continue;
        }

        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("md") {
            continue;
        }

        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };

        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();

        let metadata = entry.metadata().await?;
        workflows.push(WorkflowListItem {
            name: stem.to_string(),
            filename,
            size: metadata.len(),
            modified_at: None,
        });
    }

    workflows.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(HttpResponse::Ok().json(workflows))
}

/// Gets a specific workflow by name
///
/// # HTTP Route
/// `GET /bamboo/workflows/{name}`
///
/// # Path Parameters
/// - `name`: Workflow name (without .md extension)
///
/// # Response Format
/// Returns [`WorkflowGetResponse`] with full content:
/// ```json
/// {
///   "name": "myworkflow",
///   "filename": "myworkflow.md",
///   "content": "# My Workflow\n...",
///   "size": 1234,
///   "modified_at": null
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Workflow found and returned
/// - `404 Not Found`: Workflow not found or invalid name
/// - `500 Internal Server Error`: Failed to read workflow
///
/// # Example
/// ```bash
/// curl http://localhost:3000/bamboo/workflows/myworkflow
/// ```
pub async fn get_workflow(
    app_state: web::Data<AppState>,
    workflow_name: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let name = workflow_name.into_inner();
    if !is_safe_workflow_name(&name) {
        return Err(AppError::NotFound("Workflow".to_string()));
    }

    let dir = app_state.app_data_dir.join("workflows");
    fs::create_dir_all(&dir).await?;

    let filename = format!("{name}.md");
    let file_path = dir.join(&filename);

    let metadata = match fs::metadata(&file_path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(AppError::NotFound(format!("Workflow '{name}'")))
        }
        Err(e) => return Err(AppError::StorageError(e)),
    };

    let content = fs::read_to_string(&file_path).await?;

    Ok(HttpResponse::Ok().json(WorkflowGetResponse {
        name,
        filename,
        content,
        size: metadata.len(),
        modified_at: None,
    }))
}

/// Request body for saving a workflow
#[derive(Deserialize)]
pub struct SaveWorkflowRequest {
    /// Workflow name
    name: String,
    /// Workflow markdown content
    content: String,
}

/// Creates or updates a workflow
///
/// # HTTP Route
/// `POST /bamboo/workflows`
///
/// # Request Body
/// ```json
/// {
///   "name": "myworkflow",
///   "content": "# My Workflow\n\nStep 1: ..."
/// }
/// ```
///
/// # Response Format
/// ```json
/// {
///   "success": true,
///   "path": "/path/to/workflows/myworkflow.md"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Workflow saved successfully
/// - `400 Bad Request`: Invalid workflow name
/// - `500 Internal Server Error`: Failed to save workflow
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/workflows \
///   -H "Content-Type: application/json" \
///   -d '{"name": "myworkflow", "content": "# My Workflow"}'
/// ```
pub async fn save_workflow(
    app_state: web::Data<AppState>,
    payload: web::Json<SaveWorkflowRequest>,
) -> Result<HttpResponse, AppError> {
    let name = payload.name.trim();
    if !is_safe_workflow_name(name) {
        return Err(AppError::BadRequest("Invalid workflow name".to_string()));
    }

    let dir = app_state.app_data_dir.join("workflows");
    fs::create_dir_all(&dir).await?;

    let file_path = dir.join(format!("{}.md", name));
    fs::write(&file_path, &payload.content).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "path": file_path.to_string_lossy()
    })))
}

/// Deletes a workflow file
///
/// # HTTP Route
/// `DELETE /bamboo/workflows/{name}`
///
/// # Path Parameters
/// - `name`: Workflow name to delete
///
/// # Response Format
/// ```json
/// {
///   "success": true
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Workflow deleted successfully
/// - `400 Bad Request`: Invalid workflow name
/// - `404 Not Found`: Workflow not found
/// - `500 Internal Server Error`: Failed to delete workflow
///
/// # Example
/// ```bash
/// curl -X DELETE http://localhost:3000/bamboo/workflows/myworkflow
/// ```
pub async fn delete_workflow(
    app_state: web::Data<AppState>,
    workflow_name: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let name = workflow_name.into_inner();
    if !is_safe_workflow_name(&name) {
        return Err(AppError::BadRequest("Invalid workflow name".to_string()));
    }

    let dir = app_state.app_data_dir.join("workflows");
    let file_path = dir.join(format!("{}.md", name));

    if !file_path.exists() {
        return Err(AppError::NotFound(format!("Workflow '{}'", name)));
    }

    fs::remove_file(&file_path).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

// ============================================================================
// Setup Status Handlers
// ============================================================================

/// Setup status response
#[derive(Serialize)]
struct SetupStatus {
    /// Whether setup is complete
    is_complete: bool,
    /// Whether proxy config exists in config.json
    has_proxy_config: bool,
    /// Whether proxy env vars are detected
    has_proxy_env: bool,
    /// Status message
    message: String,
}

/// Checks if proxy configuration exists in config
fn has_proxy_config(config: &Value) -> bool {
    config
        .get("http_proxy")
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.trim().is_empty())
        || config
            .get("https_proxy")
            .and_then(|value| value.as_str())
            .is_some_and(|value| !value.trim().is_empty())
}

fn collect_proxy_environment_flags() -> Vec<&'static str> {
    ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]
        .iter()
        .copied()
        .filter(|key| {
            std::env::var(key)
                .ok()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
        })
        .collect()
}

fn is_setup_completed_from_typed(config: &Config) -> bool {
    config
        .extra
        .get("setup")
        .and_then(|setup| setup.get("completed"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

#[cfg(test)]
fn deep_merge_json(dst: &mut Value, src: Value) {
    match (dst, src) {
        (Value::Object(dst_obj), Value::Object(src_obj)) => {
            for (k, v) in src_obj {
                deep_merge_json(dst_obj.entry(k).or_insert(Value::Null), v);
            }
        }
        (dst_slot, src_val) => {
            *dst_slot = src_val;
        }
    }
}

fn should_show_setup(setup_completed: bool, _has_proxy_config: bool, _has_proxy_env: bool) -> bool {
    !setup_completed
}

fn setup_status_message(
    setup_completed: bool,
    has_proxy_config: bool,
    proxy_environment_flags: &[&str],
) -> String {
    if setup_completed {
        return "Setup has already been completed in config.json.".to_string();
    }

    if has_proxy_config {
        return "Proxy configuration already exists in config.json. Setup is not required."
            .to_string();
    }

    if !proxy_environment_flags.is_empty() {
        return format!(
            "Detected proxy environment variables: {}. Please confirm proxy settings in setup.",
            proxy_environment_flags.join(", ")
        );
    }

    "No proxy configuration or proxy environment variables detected. Setup is not required."
        .to_string()
}

/// Gets the setup completion status
///
/// # HTTP Route
/// `GET /bamboo/setup/status`
///
/// # Response Format
/// ```json
/// {
///   "is_complete": true,
///   "has_proxy_config": false,
///   "has_proxy_env": false,
///   "message": "Setup has already been completed in config.json."
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Status retrieved successfully
///
/// # Example
/// ```bash
/// curl http://localhost:3000/bamboo/setup/status
/// ```
pub async fn get_setup_status(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await.clone();
    let config_value = serde_json::to_value(&config)?;
    let has_proxy_config = has_proxy_config(&config_value);
    let proxy_environment_flags = collect_proxy_environment_flags();
    let has_proxy_env = !proxy_environment_flags.is_empty();
    let setup_completed = is_setup_completed_from_typed(&config);

    let is_complete = !should_show_setup(setup_completed, has_proxy_config, has_proxy_env);
    let message = setup_status_message(setup_completed, has_proxy_config, &proxy_environment_flags);

    Ok(HttpResponse::Ok().json(SetupStatus {
        is_complete,
        has_proxy_config,
        has_proxy_env,
        message,
    }))
}

/// Marks the setup as complete
///
/// # HTTP Route
/// `POST /bamboo/setup/complete`
///
/// # Response Format
/// ```json
/// {
///   "success": true
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Setup marked as complete
/// - `500 Internal Server Error`: Failed to update config
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/setup/complete
/// ```
pub async fn mark_setup_complete(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let completed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    app_state
        .update_config(
            |config| {
                let setup_entry = config
                    .extra
                    .entry("setup".to_string())
                    .or_insert_with(|| serde_json::json!({}));
                let setup_obj = setup_entry.as_object_mut().ok_or_else(|| {
                    AppError::BadRequest("config.setup must be a JSON object".to_string())
                })?;

                setup_obj.insert("completed".to_string(), Value::Bool(true));
                setup_obj.insert("completed_at".to_string(), Value::String(completed_at));
                setup_obj.insert("version".to_string(), Value::Number(1.into()));
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

/// Resets setup status to incomplete
///
/// # HTTP Route
/// `POST /bamboo/setup/incomplete`
///
/// # Response Format
/// ```json
/// {
///   "success": true
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Setup marked as incomplete
/// - `500 Internal Server Error`: Failed to update config
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/setup/incomplete
/// ```
pub async fn mark_setup_incomplete(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let reset_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    app_state
        .update_config(
            |config| {
                if let Some(setup_entry) = config.extra.get_mut("setup") {
                    if let Some(setup_obj) = setup_entry.as_object_mut() {
                        setup_obj.insert("completed".to_string(), Value::Bool(false));
                        setup_obj.insert("reset_at".to_string(), Value::String(reset_at));
                    }
                }
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

// ============================================================================
// Configuration Handlers
// ============================================================================

/// Gets the Bamboo application configuration
///
/// # HTTP Route
/// `GET /bamboo/config`
///
/// # Response Format
/// Returns the config.json contents (with sensitive fields removed):
/// ```json
/// {
///   "provider": "copilot",
///   "http_proxy": "http://proxy:8080",
///   "providers": {...}
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Config retrieved successfully (empty object if not found)
///
/// # Security
/// Proxy authentication credentials are stripped from the response.
///
/// # Example
/// ```bash
/// curl http://localhost:3000/bamboo/config
/// ```
pub async fn get_bamboo_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let path = app_state.app_data_dir.join("config.json");
    if !path.exists() {
        return Ok(HttpResponse::Ok().json(serde_json::json!({})));
    }

    let mut config = app_state.config.read().await.clone();
    config.refresh_proxy_auth_encrypted()?;
    config.refresh_provider_api_keys_encrypted()?;
    let value = serde_json::to_value(&config)?;
    Ok(HttpResponse::Ok().json(redact_config_for_api(value, &config)))
}

/// Updates the Bamboo application configuration
///
/// # HTTP Route
/// `POST /bamboo/config`
///
/// # Request Body
/// Configuration JSON object:
/// ```json
/// {
///   "provider": "openai",
///   "http_proxy": "http://proxy:8080",
///   "providers": {
///     "openai": {
///       "api_key": "sk-..."
///     }
///   }
/// }
/// ```
///
/// # Response Format
/// Returns the saved config (with sensitive fields removed):
/// ```json
/// {
///   "provider": "openai",
///   ...
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Config saved successfully
/// - `500 Internal Server Error`: Failed to save config
///
/// # Security
/// Proxy auth fields are automatically encrypted before saving.
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/config \
///   -H "Content-Type: application/json" \
///   -d '{"provider": "openai"}'
/// ```
pub async fn set_bamboo_config(
    app_state: web::Data<AppState>,
    payload: web::Json<Value>,
) -> Result<HttpResponse, AppError> {
    let patch = payload.into_inner();
    let mut patch_obj = config_manager::assert_json_object(patch)?;
    config_manager::sanitize_root_patch(&mut patch_obj);
    let api_key_intents = config_manager::provider_api_key_intents(&patch_obj);
    let effects = config_manager::effects_for_root_patch(&patch_obj);

    // Apply the patch under the config write lock to avoid clobbering concurrent updates.
    let new_config = app_state
        .update_config(
            move |config| {
                let current = config.clone();
                let mut patch_obj = patch_obj;
                config_manager::preserve_masked_provider_api_keys(&mut patch_obj, &current);
                let mut new_config = config_manager::build_merged_config(&current, patch_obj)?;
                config_manager::sync_provider_api_keys_encrypted_for_patch(
                    &mut new_config,
                    &api_key_intents,
                )?;
                *config = new_config;
                Ok(())
            },
            ConfigUpdateEffects {
                // Best-effort: setup/UX flows must be able to persist partial config even when
                // provider init isn't possible yet.
                reload_provider: false,
                reconcile_mcp: effects.reconcile_mcp,
            },
        )
        .await?;

    if effects.reload_provider == config_manager::ReloadMode::BestEffort {
        if let Err(e) = app_state.reload_provider().await {
            log::warn!(
                "Config updated (provider={}, requested_reload=true) but provider reload failed: {}",
                new_config.provider,
                e
            );
        }
    }

    let mut config_for_response = new_config.clone();
    config_for_response.refresh_proxy_auth_encrypted()?;
    config_for_response.refresh_provider_api_keys_encrypted()?;
    let value = serde_json::to_value(&config_for_response)?;
    Ok(HttpResponse::Ok().json(redact_config_for_api(value, &config_for_response)))
}

#[derive(Serialize)]
struct ValidationIssue {
    path: String,
    message: String,
}

#[derive(Serialize)]
struct ValidateConfigResponse {
    valid: bool,
    errors: std::collections::BTreeMap<String, Vec<ValidationIssue>>,
}

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
    config_manager::sanitize_root_patch(&mut patch_obj);

    let current = app_state.config.read().await.clone();
    let merged = config_manager::build_merged_config(&current, patch_obj.clone())?;
    let domains = config_manager::domains_for_root_patch(&patch_obj);

    let mut errors: std::collections::BTreeMap<String, Vec<ValidationIssue>> =
        std::collections::BTreeMap::new();

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
        if let Err(e) = crate::agent::llm::http_client::build_proxy(&merged) {
            push_error("proxy", "http_proxy/https_proxy", e.to_string());
        }
    }

    if domains.provider {
        if let Err(e) = crate::agent::llm::validate_provider_config(&merged) {
            let provider = merged.provider.as_str();
            let (path, message) = match provider {
                "openai" => {
                    let configured = merged
                        .providers
                        .openai
                        .as_ref()
                        .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
                        .unwrap_or(false);
                    if configured {
                        ("provider", e.to_string())
                    } else {
                        (
                            "providers.openai.api_key",
                            "OpenAI API key is required".to_string(),
                        )
                    }
                }
                "anthropic" => {
                    let configured = merged
                        .providers
                        .anthropic
                        .as_ref()
                        .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
                        .unwrap_or(false);
                    if configured {
                        ("provider", e.to_string())
                    } else {
                        (
                            "providers.anthropic.api_key",
                            "Anthropic API key is required".to_string(),
                        )
                    }
                }
                "gemini" => {
                    let configured = merged
                        .providers
                        .gemini
                        .as_ref()
                        .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
                        .unwrap_or(false);
                    if configured {
                        ("provider", e.to_string())
                    } else {
                        (
                            "providers.gemini.api_key",
                            "Gemini API key is required".to_string(),
                        )
                    }
                }
                _ => ("provider", e.to_string()),
            };

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

    let valid = errors.values().all(|v| v.is_empty());
    Ok(HttpResponse::Ok().json(ValidateConfigResponse { valid, errors }))
}

/// Request body for setting proxy authentication
#[derive(Deserialize)]
pub struct ProxyAuthPayload {
    /// Proxy username
    username: Option<String>,
    /// Proxy password
    password: Option<String>,
}

/// Sets proxy authentication credentials
///
/// # HTTP Route
/// `POST /bamboo/proxy-auth`
///
/// # Request Body
/// ```json
/// {
///   "username": "user",
///   "password": "pass"
/// }
/// ```
///
/// # Response Format
/// ```json
/// {
///   "success": true
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Proxy auth saved and provider reloaded
/// - `500 Internal Server Error`: Failed to save or reload
///
/// # Security
/// Credentials are encrypted before storage in config.json.
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/proxy-auth \
///   -H "Content-Type: application/json" \
///   -d '{"username": "user", "password": "pass"}'
/// ```
pub async fn set_proxy_auth(
    app_state: web::Data<AppState>,
    payload: web::Json<ProxyAuthPayload>,
) -> Result<HttpResponse, AppError> {
    let username = payload.username.clone().unwrap_or_default();
    let password = payload.password.clone().unwrap_or_default();

    // Store proxy auth in config
    let auth = if username.trim().is_empty() {
        None
    } else {
        Some(ProxyAuth { username, password })
    };

    app_state
        .update_config(
            |config| {
                config.proxy_auth = auth;
                config.refresh_proxy_auth_encrypted().map_err(|e| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Failed to encrypt proxy auth before save: {e}"
                    ))
                })?;
                Ok(())
            },
            ConfigUpdateEffects {
                // Best-effort: setup flows often set proxy auth before provider config is complete.
                // Persisting should not fail just because provider init can't happen yet.
                reload_provider: false,
                // Proxy auth can affect SSE-based MCP servers too.
                reconcile_mcp: true,
            },
        )
        .await?;

    if let Err(e) = app_state.reload_provider().await {
        log::warn!("Proxy auth updated but provider reload failed: {}", e);
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

/// Gets proxy authentication status
///
/// # HTTP Route
/// `GET /bamboo/proxy-auth/status`
///
/// # Response Format
/// ```json
/// {
///   "configured": true,
///   "username": "myuser"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Status retrieved successfully
///
/// # Note
/// Password is never returned, only whether auth is configured and the username.
///
/// # Example
/// ```bash
/// curl http://localhost:3000/bamboo/proxy-auth/status
/// ```
pub async fn get_proxy_auth_status(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    // Defensive: ensure in-memory proxy_auth is hydrated from encrypted fields.
    // Some call paths update config via JSON patching and may only carry encrypted values.
    let mut config = app_state.config.write().await;
    config.hydrate_proxy_auth_from_encrypted();

    if let Some(auth) = config.proxy_auth.as_ref() {
        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "configured": true,
            "username": auth.username,
        })));
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "configured": false,
        "username": serde_json::Value::Null
    })))
}

/// Resets (deletes) the Bamboo configuration file
///
/// # HTTP Route
/// `POST /bamboo/config/reset`
///
/// # Response Format
/// ```json
/// {
///   "success": true
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Config reset successfully
/// - `500 Internal Server Error`: Failed to delete config
///
/// # Warning
/// This permanently deletes the config.json file. Use with caution.
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/config/reset
/// ```
pub async fn reset_bamboo_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let path = app_state.app_data_dir.join("config.json");
    // Try to delete config.json if it exists
    match fs::try_exists(&path).await {
        Ok(true) => {
            fs::remove_file(&path)
                .await
                .map_err(AppError::StorageError)?;
        }
        Ok(false) => {
            // Config file doesn't exist, nothing to do
        }
        Err(err) => return Err(AppError::StorageError(err)),
    }

    // Reset in-memory config and best-effort reload provider.
    let new_config = app_state.reload_config().await;
    if let Err(e) = app_state.reload_provider().await {
        log::warn!(
            "Config reset updated config to provider={}, but provider reload failed: {}",
            new_config.provider,
            e
        );
    }
    // Config reset may remove/disable MCP servers; reconcile to stop any running servers.
    app_state
        .mcp_manager
        .reconcile_from_config(&new_config.mcp)
        .await;
    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

pub async fn get_anthropic_model_mapping(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await;
    Ok(HttpResponse::Ok().json(config.anthropic_model_mapping.clone()))
}

pub async fn set_anthropic_model_mapping(
    app_state: web::Data<AppState>,
    payload: web::Json<crate::core::model_mapping::AnthropicModelMapping>,
) -> Result<HttpResponse, AppError> {
    let mapping = payload.into_inner();
    app_state
        .update_config(
            |config| {
                config.anthropic_model_mapping = mapping.clone();
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;
    Ok(HttpResponse::Ok().json(mapping))
}

// ============================================================================
// Keyword Masking Handlers
// ============================================================================

/// Response for keyword masking configuration
#[derive(Debug, Serialize, Deserialize)]
struct KeywordMaskingResponse {
    /// List of keyword masking entries
    entries: Vec<KeywordEntry>,
}

/// Validation error for keyword entries
#[derive(Debug, Serialize, Deserialize)]
struct ValidationError {
    /// Index of the invalid entry
    index: usize,
    /// Error message
    message: String,
}

/// Gets keyword masking configuration
///
/// # HTTP Route
/// `GET /bamboo/keyword-masking`
///
/// # Response Format
/// ```json
/// {
///   "entries": [
///     {
///       "pattern": "secret",
///       "mask_type": "full",
///       "case_sensitive": false
///     }
///   ]
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Config retrieved successfully
///
/// # Example
/// ```bash
/// curl http://localhost:3000/bamboo/keyword-masking
/// ```
pub async fn get_keyword_masking_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await;
    Ok(HttpResponse::Ok().json(KeywordMaskingResponse {
        entries: config.keyword_masking.entries.clone(),
    }))
}

/// Updates keyword masking configuration
///
/// # HTTP Route
/// `POST /bamboo/keyword-masking`
///
/// # Request Body
/// Array of keyword entries:
/// ```json
/// [
///   {
///     "pattern": "secret",
///     "mask_type": "full",
///     "case_sensitive": false
///   }
/// ]
/// ```
///
/// # Response Format
/// Returns the saved configuration:
/// ```json
/// {
///   "entries": [...]
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Config saved successfully
/// - `400 Bad Request`: Validation failed (max 100 entries, max 500 char patterns)
/// - `500 Internal Server Error`: Failed to save config
///
/// # Limits
/// - Maximum 100 entries
/// - Maximum 500 characters per pattern
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/keyword-masking \
///   -H "Content-Type: application/json" \
///   -d '[{"pattern": "secret", "mask_type": "full"}]'
/// ```
pub async fn update_keyword_masking_config(
    app_state: web::Data<AppState>,
    payload: web::Json<Vec<KeywordEntry>>,
) -> Result<HttpResponse, AppError> {
    let entries = payload.into_inner();

    // Input validation limits to prevent DoS
    const MAX_ENTRIES: usize = 100;
    const MAX_PATTERN_LENGTH: usize = 500;

    if entries.len() > MAX_ENTRIES {
        return Err(AppError::BadRequest(format!(
            "Too many entries: {} (max {})",
            entries.len(),
            MAX_ENTRIES
        )));
    }

    // Validate pattern lengths
    for (idx, entry) in entries.iter().enumerate() {
        if entry.pattern.len() > MAX_PATTERN_LENGTH {
            return Err(AppError::BadRequest(format!(
                "Pattern at index {} too long: {} chars (max {})",
                idx,
                entry.pattern.len(),
                MAX_PATTERN_LENGTH
            )));
        }
    }

    let config = KeywordMaskingConfig { entries };

    // Validate all entries
    if let Err(errors) = config.validate() {
        let validation_errors: Vec<ValidationError> = errors
            .into_iter()
            .map(|(idx, msg)| ValidationError {
                index: idx,
                message: msg,
            })
            .collect();
        return Err(AppError::BadRequest(format!(
            "Validation failed: {:?}",
            validation_errors
        )));
    }

    app_state
        .update_config(
            |current| {
                current.keyword_masking = config.clone();
                Ok(())
            },
            ConfigUpdateEffects {
                // Best-effort: keyword masking is a UX feature and should remain configurable
                // even when the provider is not yet configured.
                reload_provider: false,
                reconcile_mcp: false,
            },
        )
        .await?;

    if let Err(e) = app_state.reload_provider().await {
        log::warn!("Keyword masking updated but provider reload failed: {}", e);
    }

    Ok(HttpResponse::Ok().json(KeywordMaskingResponse {
        entries: config.entries,
    }))
}

/// Validates keyword masking entries without saving
///
/// # HTTP Route
/// `POST /bamboo/keyword-masking/validate`
///
/// # Request Body
/// Array of keyword entries to validate
///
/// # Response Format
/// Success:
/// ```json
/// {
///   "valid": true
/// }
/// ```
///
/// Validation errors:
/// ```json
/// {
///   "valid": false,
///   "errors": [
///     {
///       "index": 0,
///       "message": "Pattern cannot be empty"
///     }
///   ]
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Validation completed (check `valid` field for result)
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/keyword-masking/validate \
///   -H "Content-Type: application/json" \
///   -d '[{"pattern": "test", "mask_type": "full"}]'
/// ```
pub async fn validate_keyword_entries(
    payload: web::Json<Vec<KeywordEntry>>,
) -> Result<HttpResponse, AppError> {
    let entries = payload.into_inner();
    let config = KeywordMaskingConfig { entries };

    match config.validate() {
        Ok(()) => Ok(HttpResponse::Ok().json(serde_json::json!({ "valid": true }))),
        Err(errors) => {
            let validation_errors: Vec<ValidationError> = errors
                .into_iter()
                .map(|(idx, msg)| ValidationError {
                    index: idx,
                    message: msg,
                })
                .collect();
            Ok(HttpResponse::Ok().json(serde_json::json!({
                "valid": false,
                "errors": validation_errors
            })))
        }
    }
}

// ============================================================================
// Provider Configuration Handlers
// ============================================================================

/// Response for provider configuration
#[derive(Serialize)]
struct ProviderConfigResponse {
    /// Currently active provider
    provider: String,
    /// List of available provider types
    available_providers: Vec<String>,
    /// Provider-specific configurations (API keys masked)
    providers: Value,
}

/// Request body for updating provider configuration
#[derive(Deserialize)]
pub struct UpdateProviderRequest {
    /// Provider to activate
    provider: String,
    /// Provider-specific configurations
    #[serde(default)]
    providers: Value,
}

/// Gets current provider configuration with API keys masked
///
/// # HTTP Route
/// `GET /bamboo/settings/provider`
///
/// # Response Format
/// ```json
/// {
///   "provider": "openai",
///   "available_providers": ["copilot", "openai", "anthropic", "gemini"],
///   "providers": {
///     "openai": {
///       "api_key": "****...****",
///       "model": "gpt-4"
///     }
///   }
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Configuration retrieved successfully
///
/// # Security
/// API keys are masked to prevent exposure.
///
/// # Example
/// ```bash
/// curl http://localhost:3000/bamboo/settings/provider
/// ```
pub async fn get_provider_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let mut config = app_state.config.read().await.clone();
    let provider = config.provider.clone();
    config.refresh_provider_api_keys_encrypted()?;
    let providers = serde_json::to_value(&config.providers)?;
    let masked_providers = redact_providers_for_api(providers, &config);

    let response = ProviderConfigResponse {
        provider,
        available_providers: AVAILABLE_PROVIDERS.iter().map(|s| s.to_string()).collect(),
        providers: masked_providers,
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Updates provider configuration and reloads the provider
///
/// # HTTP Route
/// `POST /bamboo/settings/provider`
///
/// # Request Body
/// ```json
/// {
///   "provider": "openai",
///   "providers": {
///     "openai": {
///       "api_key": "sk-...",
///       "model": "gpt-4"
///     }
///   }
/// }
/// ```
///
/// # Response Format
/// Success:
/// ```json
/// {
///   "success": true,
///   "provider": "openai"
/// }
/// ```
///
/// Error:
/// ```json
/// {
///   "success": false,
///   "error": "Configuration saved but invalid: ..."
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Configuration updated (check `success` field)
/// - `400 Bad Request`: Invalid configuration
/// - `500 Internal Server Error`: Failed to save or reload
///
/// # Features
/// - Preserves existing API keys if masked values are sent
/// - Validates configuration before applying
/// - Automatically reloads provider (no separate reload call required)
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/settings/provider \
///   -H "Content-Type: application/json" \
///   -d '{"provider": "openai", "providers": {"openai": {"api_key": "sk-..."}}}'
/// ```
pub async fn update_provider_config(
    app_state: web::Data<AppState>,
    payload: web::Json<UpdateProviderRequest>,
) -> Result<HttpResponse, AppError> {
    let mut patch_obj = serde_json::Map::new();
    patch_obj.insert(
        "provider".to_string(),
        Value::String(payload.provider.clone()),
    );
    patch_obj.insert("providers".to_string(), payload.providers.clone());

    config_manager::sanitize_root_patch(&mut patch_obj);
    let api_key_intents = config_manager::provider_api_key_intents(&patch_obj);

    let new_config = match app_state
        .update_config(
            move |config| {
                let current = config.clone();
                let mut patch_obj = patch_obj;
                config_manager::preserve_masked_provider_api_keys(&mut patch_obj, &current);
                let mut new_config = config_manager::build_merged_config(&current, patch_obj)?;
                config_manager::sync_provider_api_keys_encrypted_for_patch(
                    &mut new_config,
                    &api_key_intents,
                )?;

                if let Err(e) = crate::agent::llm::validate_provider_config(&new_config) {
                    return Err(AppError::BadRequest(format!("Invalid configuration: {e}")));
                }

                *config = new_config;
                Ok(())
            },
            // Persist config first; reload below so we can control error reporting.
            ConfigUpdateEffects {
                reload_provider: false,
                reconcile_mcp: true,
            },
        )
        .await
    {
        Ok(cfg) => cfg,
        Err(AppError::BadRequest(msg)) => {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "success": false,
                "error": msg
            })));
        }
        Err(e) => return Err(e),
    };

    if let Err(e) = app_state.reload_provider().await {
        return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "success": false,
            "error": format!("Failed to reload provider: {e}")
        })));
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "provider": new_config.provider
    })))
}

/// Fetch available models for a specific provider
pub async fn fetch_provider_models(
    app_state: web::Data<AppState>,
    payload: web::Json<serde_json::Value>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await.clone();
    let provider_type = payload
        .get("provider")
        .and_then(|v| v.as_str())
        .unwrap_or(config.provider.as_str());

    // Build a proxy-aware HTTP client for all outbound calls.
    let client = crate::agent::llm::http_client::build_http_client(&config).map_err(|e| {
        AppError::InternalError(anyhow::anyhow!("Failed to build HTTP client: {e}"))
    })?;

    let models =
        match provider_type {
            "copilot" => {
                let provider = app_state.get_provider().await;
                provider.list_models().await.map_err(|e| {
                    let msg = e.to_string();
                    if msg.contains("proxy") || msg.contains("407") {
                        AppError::ProxyAuthRequired
                    } else {
                        AppError::InternalError(anyhow::anyhow!("Failed to fetch models: {e}"))
                    }
                })?
            }
            "openai" => {
                let openai = config.providers.openai.as_ref().ok_or_else(|| {
                    AppError::BadRequest("OpenAI configuration required".to_string())
                })?;
                if openai.api_key.trim().is_empty() {
                    return Err(AppError::BadRequest("API key not configured".to_string()));
                }
                fetch_models_from_api(
                    &client,
                    "openai",
                    &openai.api_key,
                    openai.base_url.as_deref(),
                )
                .await?
            }
            "anthropic" => {
                let anthropic = config.providers.anthropic.as_ref().ok_or_else(|| {
                    AppError::BadRequest("Anthropic configuration required".to_string())
                })?;
                if anthropic.api_key.trim().is_empty() {
                    return Err(AppError::BadRequest("API key not configured".to_string()));
                }
                fetch_models_from_api(
                    &client,
                    "anthropic",
                    &anthropic.api_key,
                    anthropic.base_url.as_deref(),
                )
                .await?
            }
            "gemini" => {
                let gemini = config.providers.gemini.as_ref().ok_or_else(|| {
                    AppError::BadRequest("Gemini configuration required".to_string())
                })?;
                if gemini.api_key.trim().is_empty() {
                    return Err(AppError::BadRequest("API key not configured".to_string()));
                }
                fetch_models_from_api(
                    &client,
                    "gemini",
                    &gemini.api_key,
                    gemini.base_url.as_deref(),
                )
                .await?
            }
            other => {
                return Err(AppError::BadRequest(format!(
                    "Unsupported provider: {other}"
                )));
            }
        };

    Ok(HttpResponse::Ok().json(serde_json::json!({ "models": models })))
}

/// Fetch models from external API
async fn fetch_models_from_api(
    client: &reqwest::Client,
    provider: &str,
    api_key: &str,
    base_url: Option<&str>,
) -> Result<Vec<String>, AppError> {
    let (url, auth_header, use_query_param) = match provider {
        "openai" => {
            let url = if let Some(base) = base_url {
                let base = base.trim_end_matches('/');
                format!("{}/models", base)
            } else {
                "https://api.openai.com/v1/models".to_string()
            };
            (url, format!("Bearer {}", api_key), false)
        }
        "anthropic" => {
            let url = if let Some(base) = base_url {
                let base = base.trim_end_matches('/');
                format!("{}/models", base)
            } else {
                "https://api.anthropic.com/v1/models".to_string()
            };
            (url, api_key.to_string(), false) // Anthropic uses x-api-key header
        }
        "gemini" => {
            let url = if let Some(base) = base_url {
                let base = base.trim_end_matches('/');
                format!("{}?key={}", base, api_key)
            } else {
                format!(
                    "https://generativelanguage.googleapis.com/v1beta/models?key={}",
                    api_key
                )
            };
            (url, String::new(), true) // Gemini uses query param for auth
        }
        _ => {
            return Err(AppError::BadRequest(format!(
                "Unsupported provider: {}",
                provider
            )));
        }
    };

    log::info!("Fetching models from: {}", url);

    let mut request = client.get(&url);

    // Set appropriate authentication header based on provider (not for Gemini)
    if !use_query_param {
        if provider == "anthropic" {
            request = request.header("x-api-key", auth_header);
        } else {
            request = request.header("Authorization", auth_header);
        }
    }

    let response = request
        .send()
        .await
        .map_err(|e| AppError::InternalError(anyhow::anyhow!("Request failed: {}", e)))?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalError(anyhow::anyhow!(
            "API request failed: {} - {}",
            status,
            error_text
        )));
    }

    let json: Value = response
        .json()
        .await
        .map_err(|e| AppError::InternalError(anyhow::anyhow!("Failed to parse JSON: {}", e)))?;

    // Extract model IDs from different response formats
    let models: Vec<String> = if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
        // Standard OpenAI format
        data.iter()
            .filter_map(|model| {
                model
                    .get("id")
                    .and_then(|id| id.as_str())
                    .map(|s| s.to_string())
            })
            .collect()
    } else if let Some(models_arr) = json.get("models").and_then(|m| m.as_array()) {
        // Alternative format: { models: [...] } - Gemini uses this
        models_arr
            .iter()
            .filter_map(|model| {
                // Gemini models have "name" field
                if let Some(name) = model.get("name").and_then(|n| n.as_str()) {
                    Some(name.to_string())
                } else if let Some(id) = model.get("id").and_then(|i| i.as_str()) {
                    Some(id.to_string())
                } else {
                    model.as_str().map(|s| s.to_string())
                }
            })
            .collect()
    } else if let Some(arr) = json.as_array() {
        // Direct array format
        arr.iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect()
    } else {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "Unexpected response format"
        )));
    };

    log::info!("Fetched {} models", models.len());
    Ok(models)
}

/// Reloads provider configuration from file and recreates the provider
///
/// # HTTP Route
/// `POST /bamboo/settings/reload`
///
/// # Response Format
/// Success:
/// ```json
/// {
///   "success": true,
///   "provider": "openai"
/// }
/// ```
///
/// Error:
/// ```json
/// {
///   "success": false,
///   "error": "Invalid configuration: ..."
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Reload completed (check `success` field)
/// - `400 Bad Request`: Invalid configuration
/// - `500 Internal Server Error`: Failed to reload provider
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/settings/reload
/// ```
///
/// # Notes
/// In most cases you should not need to call this endpoint, because
/// `POST /bamboo/settings/provider` already saves the config and reloads the provider.
pub async fn reload_provider_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    // First, reload the configuration from file into AppState
    let new_config = app_state.reload_config().await;

    // Validate the configuration
    if let Err(e) = crate::agent::llm::validate_provider_config(&new_config) {
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "success": false,
            "error": e.to_string()
        })));
    }

    // Reload the provider in AppState using the updated config
    if let Err(e) = app_state.reload_provider().await {
        return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "success": false,
            "error": format!("Failed to reload provider: {}", e)
        })));
    }

    // Reconcile MCP runtimes in case the file-based config changed (e.g. manual edit).
    app_state
        .mcp_manager
        .reconcile_from_config(&new_config.mcp)
        .await;

    log::info!("Provider reloaded successfully: {}", new_config.provider);

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "provider": new_config.provider
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::agent::mcp::{
        HeaderConfig, McpServerConfig, SseConfig, StdioConfig, TransportConfig,
    };
    use crate::core::encryption::set_test_encryption_key;
    use crate::core::{OpenAIConfig, ProviderConfigs};
    use std::collections::HashMap;

    fn build_config_with_mcp_secrets(temp_dir: &std::path::Path) -> Config {
        let mut cfg = Config {
            provider: "openai".to_string(),
            providers: ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "sk-test".to_string(),
                    api_key_encrypted: None,
                    base_url: None,
                    model: Some("gpt-4o".to_string()),
                    responses_only_models: vec![],
                    extra: Default::default(),
                }),
                ..ProviderConfigs::default()
            },
            ..Config::default()
        };

        cfg.mcp.servers = vec![
            McpServerConfig {
                id: "stdio-secret".to_string(),
                name: Some("Stdio Secret".to_string()),
                enabled: false, // tests must not spawn actual MCP servers
                transport: TransportConfig::Stdio(StdioConfig {
                    command: "echo".to_string(),
                    args: vec!["hello".to_string()],
                    cwd: None,
                    env: HashMap::from([("TOKEN".to_string(), "super-secret".to_string())]),
                    env_encrypted: HashMap::new(),
                    startup_timeout_ms: 5000,
                }),
                request_timeout_ms: 5000,
                healthcheck_interval_ms: 1000,
                reconnect: Default::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            },
            McpServerConfig {
                id: "sse-secret".to_string(),
                name: Some("SSE Secret".to_string()),
                enabled: false,
                transport: TransportConfig::Sse(SseConfig {
                    url: "http://localhost:9999/sse".to_string(),
                    headers: vec![HeaderConfig {
                        name: "Authorization".to_string(),
                        value: "Bearer super-secret".to_string(),
                        value_encrypted: None,
                    }],
                    connect_timeout_ms: 1000,
                }),
                request_timeout_ms: 5000,
                healthcheck_interval_ms: 1000,
                reconnect: Default::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            },
        ];

        // Ensure encrypted-at-rest blobs exist on disk (what the settings endpoints round-trip).
        cfg.refresh_provider_api_keys_encrypted().unwrap();
        cfg.refresh_mcp_secrets_encrypted().unwrap();

        cfg.save_to_dir(temp_dir.to_path_buf()).unwrap();
        Config::from_data_dir(Some(temp_dir.to_path_buf()))
    }

    #[test]
    fn update_provider_config_preserves_mcp_secrets() {
        let _key_guard = set_test_encryption_key([7u8; 32]);
        let temp_dir = tempfile::tempdir().unwrap();
        let current = build_config_with_mcp_secrets(temp_dir.path());

        // Mimic `update_provider_config`'s core flow (JSON round-trip + hydrate + save).
        let mut merged = serde_json::to_value(&current).unwrap();
        let patch = serde_json::json!({
            "provider": "openai",
            "providers": {
                "openai": {
                    "api_key": "****...****",
                    "model": "gpt-4o"
                }
            }
        });
        deep_merge_json(&mut merged, patch);

        let mut new_config: Config = serde_json::from_value(merged).unwrap();
        new_config.hydrate_proxy_auth_from_encrypted();
        new_config.hydrate_provider_api_keys_from_encrypted();
        new_config.hydrate_mcp_secrets_from_encrypted();

        // This is the critical part: if MCP secrets weren't hydrated, the save would
        // re-encrypt empty placeholders and permanently lose credentials.
        new_config
            .save_to_dir(temp_dir.path().to_path_buf())
            .unwrap();

        // Reload from disk and ensure secrets survive.
        let reloaded = Config::from_data_dir(Some(temp_dir.path().to_path_buf()));

        let stdio = reloaded
            .mcp
            .servers
            .iter()
            .find(|s| s.id == "stdio-secret")
            .unwrap();
        match &stdio.transport {
            TransportConfig::Stdio(stdio) => {
                assert_eq!(
                    stdio.env.get("TOKEN").map(|v| v.as_str()),
                    Some("super-secret")
                );
            }
            _ => panic!("expected stdio transport"),
        }

        let sse = reloaded
            .mcp
            .servers
            .iter()
            .find(|s| s.id == "sse-secret")
            .unwrap();
        match &sse.transport {
            TransportConfig::Sse(sse) => {
                let header = sse
                    .headers
                    .iter()
                    .find(|h| h.name == "Authorization")
                    .unwrap();
                assert_eq!(header.value.as_str(), "Bearer super-secret");
            }
            _ => panic!("expected sse transport"),
        }
    }
}

/// Configures settings-related routes
///
/// # Routes
/// ## Workflows
/// - `GET /bamboo/workflows` - List all workflows
/// - `GET /bamboo/workflows/{name}` - Get workflow content
/// - `POST /bamboo/workflows` - Create/update workflow
/// - `DELETE /bamboo/workflows/{name}` - Delete workflow
///
/// ## Setup
/// - `GET /bamboo/setup/status` - Get setup status
/// - `POST /bamboo/setup/complete` - Mark setup complete
/// - `POST /bamboo/setup/incomplete` - Reset setup status
///
/// ## Configuration
/// - `GET /bamboo/config` - Get application config
/// - `POST /bamboo/config` - Update application config
/// - `POST /bamboo/config/reset` - Reset configuration
/// - `POST /bamboo/proxy-auth` - Set proxy credentials
/// - `GET /bamboo/proxy-auth/status` - Get proxy auth status
///
/// ## Keyword Masking
/// - `GET /bamboo/keyword-masking` - Get keyword masking config
/// - `POST /bamboo/keyword-masking` - Update keyword masking
/// - `POST /bamboo/keyword-masking/validate` - Validate entries
///
/// ## Provider Settings
/// - `GET /bamboo/settings/provider` - Get provider config
/// - `POST /bamboo/settings/provider` - Update provider config
/// - `POST /bamboo/settings/provider/models` - Fetch available models
/// - `POST /bamboo/settings/reload` - Reload provider
///
/// ## Other
/// - `GET /bamboo/anthropic-model-mapping` - Get model mapping
/// - `POST /bamboo/anthropic-model-mapping` - Update model mapping
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/bamboo/workflows", web::get().to(list_workflows))
        .route("/bamboo/workflows/{name}", web::get().to(get_workflow))
        .route("/bamboo/workflows", web::post().to(save_workflow))
        .route(
            "/bamboo/workflows/{name}",
            web::delete().to(delete_workflow),
        )
        // Setup status endpoints
        .route("/bamboo/setup/status", web::get().to(get_setup_status))
        .route(
            "/bamboo/setup/complete",
            web::post().to(mark_setup_complete),
        )
        .route(
            "/bamboo/setup/incomplete",
            web::post().to(mark_setup_incomplete),
        )
        // Config endpoints
        .route("/bamboo/config", web::get().to(get_bamboo_config))
        .route("/bamboo/config", web::post().to(set_bamboo_config))
        .route("/bamboo/config/reset", web::post().to(reset_bamboo_config))
        // Proxy auth endpoints (also registered with rate limiting in production via app_config_with_rate_limiting)
        .route("/bamboo/proxy-auth", web::post().to(set_proxy_auth))
        .route(
            "/bamboo/proxy-auth/status",
            web::get().to(get_proxy_auth_status),
        )
        // Keyword masking endpoints
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
        // Provider configuration endpoints
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
        // Other endpoints
        .route(
            "/bamboo/anthropic-model-mapping",
            web::get().to(get_anthropic_model_mapping),
        )
        .route(
            "/bamboo/anthropic-model-mapping",
            web::post().to(set_anthropic_model_mapping),
        );
}
