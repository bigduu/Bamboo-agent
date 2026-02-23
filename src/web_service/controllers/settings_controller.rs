use crate::core::keyword_masking::{KeywordEntry, KeywordMaskingConfig};
use crate::core::ProxyAuth;
use crate::web_service::{error::AppError, server::AppState};
use actix_web::{delete, get, post, web, HttpResponse};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use tokio::fs;

use crate::agent::llm::AVAILABLE_PROVIDERS;

#[derive(Serialize)]
struct WorkflowListItem {
    name: String,
    filename: String,
    size: u64,
    modified_at: Option<String>,
}

#[derive(Serialize)]
struct WorkflowGetResponse {
    name: String,
    filename: String,
    content: String,
    size: u64,
    modified_at: Option<String>,
}

fn config_path(app_state: &AppState) -> PathBuf {
    app_state.app_data_dir.join("config.json")
}

fn strip_proxy_auth(mut config: Value) -> Value {
    if let Some(obj) = config.as_object_mut() {
        obj.remove("proxy_auth");
        obj.remove("proxy_auth_encrypted");
    }
    config
}

/// Clean empty proxy fields from config
fn clean_empty_proxy_fields(mut config: Value) -> Value {
    if let Some(obj) = config.as_object_mut() {
        // Remove empty http_proxy
        if let Some(http_proxy) = obj.get("http_proxy") {
            if http_proxy.as_str().is_none_or(|s| s.is_empty()) {
                obj.remove("http_proxy");
            }
        }
        // Remove empty https_proxy
        if let Some(https_proxy) = obj.get("https_proxy") {
            if https_proxy.as_str().is_none_or(|s| s.is_empty()) {
                obj.remove("https_proxy");
            }
        }
    }
    config
}

/// Encrypt proxy auth before storing to config file
fn encrypt_proxy_auth(config: &mut Value) -> Result<(), AppError> {
    if let Some(obj) = config.as_object_mut() {
        // Encrypt proxy_auth
        if let Some(auth) = obj.get("proxy_auth").cloned() {
            if let Ok(auth_str) = serde_json::to_string(&auth) {
                match crate::core::encryption::encrypt(&auth_str) {
                    Ok(encrypted) => {
                        obj.insert(
                            "proxy_auth_encrypted".to_string(),
                            serde_json::Value::String(encrypted),
                        );
                        obj.remove("proxy_auth");
                    }
                    Err(e) => log::warn!("Failed to encrypt proxy_auth: {}", e),
                }
            }
        }
    }
    Ok(())
}

/// Decrypt proxy auth when loading from config file
fn decrypt_proxy_auth(config: &mut Value) {
    if let Some(obj) = config.as_object_mut() {
        // Decrypt proxy_auth
        if let Some(encrypted) = obj.get("proxy_auth_encrypted").and_then(|v| v.as_str()) {
            match crate::core::encryption::decrypt(encrypted) {
                Ok(decrypted) => {
                    if let Ok(auth) = serde_json::from_str::<serde_json::Value>(&decrypted) {
                        obj.insert("proxy_auth".to_string(), auth);
                    }
                }
                Err(e) => log::warn!("Failed to decrypt proxy_auth: {}", e),
            }
        }
    }
}

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

#[get("/bamboo/workflows")]
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

#[get("/bamboo/workflows/{name}")]
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

#[derive(Deserialize)]
struct SaveWorkflowRequest {
    name: String,
    content: String,
}

#[post("/bamboo/workflows")]
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

#[delete("/bamboo/workflows/{name}")]
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

// Setup status endpoints

#[derive(Serialize)]
struct SetupStatus {
    is_complete: bool,
    has_proxy_config: bool,
    has_proxy_env: bool,
    message: String,
}

fn has_proxy_config(config: &Value) -> bool {
    let has_http_proxy = config
        .get("http_proxy")
        .and_then(|value| value.as_str())
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    let has_https_proxy = config
        .get("https_proxy")
        .and_then(|value| value.as_str())
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);

    has_http_proxy || has_https_proxy
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

fn is_setup_completed(config: &Value) -> bool {
    config
        .get("setup")
        .and_then(|setup| setup.get("completed"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
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

#[get("/bamboo/setup/status")]
pub async fn get_setup_status(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let path = config_path(&app_state);
    let config = match fs::read_to_string(&path).await {
        Ok(content) => serde_json::from_str::<Value>(&content)?,
        Err(_) => serde_json::json!({}),
    };

    let has_proxy_config = has_proxy_config(&config);
    let proxy_environment_flags = collect_proxy_environment_flags();
    let has_proxy_env = !proxy_environment_flags.is_empty();
    let setup_completed = is_setup_completed(&config);

    let is_complete = !should_show_setup(setup_completed, has_proxy_config, has_proxy_env);
    let message = setup_status_message(setup_completed, has_proxy_config, &proxy_environment_flags);

    Ok(HttpResponse::Ok().json(SetupStatus {
        is_complete,
        has_proxy_config,
        has_proxy_env,
        message,
    }))
}

#[post("/bamboo/setup/complete")]
pub async fn mark_setup_complete(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let path = config_path(&app_state);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let mut config = match fs::read_to_string(&path).await {
        Ok(content) => serde_json::from_str::<Value>(&content)?,
        Err(_) => serde_json::json!({}),
    };

    let completed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    // Mark setup complete in config
    let config_obj = config
        .as_object_mut()
        .ok_or_else(|| AppError::BadRequest("config.json must be a JSON object".to_string()))?;

    let setup_entry = config_obj
        .entry("setup".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let setup_obj = setup_entry
        .as_object_mut()
        .ok_or_else(|| AppError::BadRequest("config.setup must be a JSON object".to_string()))?;

    setup_obj.insert("completed".to_string(), Value::Bool(true));
    setup_obj.insert("completed_at".to_string(), Value::String(completed_at));
    setup_obj.insert("version".to_string(), Value::Number(1.into()));

    let content = serde_json::to_string_pretty(&config)?;
    fs::write(&path, content).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

#[post("/bamboo/setup/incomplete")]
pub async fn mark_setup_incomplete(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let path = config_path(&app_state);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let mut config = match fs::read_to_string(&path).await {
        Ok(content) => serde_json::from_str::<Value>(&content)?,
        Err(_) => serde_json::json!({}),
    };

    // If setup field exists and is an object, set completed to false
    if let Some(config_obj) = config.as_object_mut() {
        if let Some(setup_entry) = config_obj.get_mut("setup") {
            if let Some(setup_obj) = setup_entry.as_object_mut() {
                setup_obj.insert("completed".to_string(), Value::Bool(false));
                setup_obj.insert(
                    "reset_at".to_string(),
                    Value::String(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)),
                );
            }
        }
    }

    let content = serde_json::to_string_pretty(&config)?;
    fs::write(&path, content).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

#[get("/bamboo/config")]
pub async fn get_bamboo_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let path = config_path(&app_state);
    match fs::read_to_string(&path).await {
        Ok(content) => {
            let mut config = serde_json::from_str::<Value>(&content)?;
            // Decrypt proxy auth for internal use, but strip before returning to client
            decrypt_proxy_auth(&mut config);
            Ok(HttpResponse::Ok().json(strip_proxy_auth(config.clone())))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            Ok(HttpResponse::Ok().json(serde_json::json!({})))
        }
        Err(err) => Err(AppError::StorageError(err)),
    }
}

#[post("/bamboo/config")]
pub async fn set_bamboo_config(
    app_state: web::Data<AppState>,
    payload: web::Json<Value>,
) -> Result<HttpResponse, AppError> {
    let path = config_path(&app_state);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    // Preserve existing encrypted proxy auth field before processing
    let existing_encrypted_auth = fs::read_to_string(&path).await.ok().and_then(|content| {
        let existing: Value = serde_json::from_str(&content).ok()?;
        existing.get("proxy_auth_encrypted").cloned()
    });

    let config = strip_proxy_auth(payload.into_inner());
    let mut config = clean_empty_proxy_fields(config);

    // Restore encrypted proxy auth field if it existed
    if let Some(encrypted_val) = existing_encrypted_auth {
        if let Some(obj) = config.as_object_mut() {
            obj.insert("proxy_auth_encrypted".to_string(), encrypted_val);
        }
    }

    let content = serde_json::to_string_pretty(&config)?;
    fs::write(path, content).await?;
    Ok(HttpResponse::Ok().json(config))
}

#[derive(Deserialize)]
struct ProxyAuthPayload {
    username: Option<String>,
    password: Option<String>,
}

#[post("/bamboo/proxy-auth")]
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

    // Update config file
    let path = config_path(&app_state);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    // Read existing config
    let mut config_value: Value = match fs::read_to_string(&path).await {
        Ok(content) => {
            let mut config: Value = serde_json::from_str(&content)?;
            decrypt_proxy_auth(&mut config);
            config
        }
        Err(_) => serde_json::json!({}),
    };

    // Update proxy auth
    if let Some(obj) = config_value.as_object_mut() {
        if let Some(auth) = auth {
            obj.insert("proxy_auth".to_string(), serde_json::to_value(&auth)?);
        } else {
            obj.remove("proxy_auth");
            obj.remove("proxy_auth_encrypted");
        }
    }

    // Encrypt and save
    let mut config_to_save = config_value.clone();
    encrypt_proxy_auth(&mut config_to_save)?;
    let content = serde_json::to_string_pretty(&config_to_save)?;
    fs::write(&path, content).await?;

    // Reload provider to apply new proxy settings
    app_state.reload_provider().await.map_err(|e| {
        AppError::InternalError(anyhow::anyhow!(
            "Failed to reload provider after updating proxy auth: {e}"
        ))
    })?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

#[get("/bamboo/proxy-auth/status")]
pub async fn get_proxy_auth_status(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let path = config_path(&app_state);

    if !path.exists() {
        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "configured": false,
            "username": serde_json::Value::Null
        })));
    }

    let content = fs::read_to_string(&path).await?;
    let config: serde_json::Value = serde_json::from_str(&content)?;

    // Check for encrypted proxy auth
    if let Some(encrypted) = config.get("proxy_auth_encrypted").and_then(|v| v.as_str()) {
        match crate::core::encryption::decrypt(encrypted) {
            Ok(decrypted) => {
                if let Ok(auth) = serde_json::from_str::<crate::core::ProxyAuth>(&decrypted) {
                    return Ok(HttpResponse::Ok().json(serde_json::json!({
                        "configured": true,
                        "username": auth.username
                    })));
                }
            }
            Err(e) => log::warn!("Failed to decrypt proxy auth: {}", e),
        }
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "configured": false,
        "username": serde_json::Value::Null
    })))
}

#[post("/bamboo/config/reset")]
pub async fn reset_bamboo_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let path = config_path(&app_state);
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
    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

#[get("/bamboo/anthropic-model-mapping")]
pub async fn get_anthropic_model_mapping(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    use crate::web_service::services::anthropic_model_mapping_service::load_anthropic_model_mapping;
    let mapping = load_anthropic_model_mapping(&app_state.app_data_dir).await?;
    Ok(HttpResponse::Ok().json(mapping))
}

#[post("/bamboo/anthropic-model-mapping")]
pub async fn set_anthropic_model_mapping(
    app_state: web::Data<AppState>,
    payload: web::Json<
        crate::web_service::services::anthropic_model_mapping_service::AnthropicModelMapping,
    >,
) -> Result<HttpResponse, AppError> {
    use crate::web_service::services::anthropic_model_mapping_service::save_anthropic_model_mapping;
    let mapping =
        save_anthropic_model_mapping(&app_state.app_data_dir, payload.into_inner()).await?;
    Ok(HttpResponse::Ok().json(mapping))
}

// Keyword masking endpoints

#[derive(Debug, Serialize, Deserialize)]
struct KeywordMaskingResponse {
    entries: Vec<KeywordEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ValidationError {
    index: usize,
    message: String,
}

#[get("/bamboo/keyword-masking")]
pub async fn get_keyword_masking_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let path = app_state.app_data_dir.join("keyword_masking.json");

    if !path.exists() {
        return Ok(HttpResponse::Ok().json(KeywordMaskingResponse {
            entries: Vec::new(),
        }));
    }

    let content = fs::read_to_string(&path).await?;
    let config: KeywordMaskingConfig = serde_json::from_str(&content)?;

    Ok(HttpResponse::Ok().json(KeywordMaskingResponse {
        entries: config.entries,
    }))
}

#[post("/bamboo/keyword-masking")]
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

    let path = app_state.app_data_dir.join("keyword_masking.json");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let content = serde_json::to_string_pretty(&config)?;
    fs::write(&path, content).await?;

    Ok(HttpResponse::Ok().json(KeywordMaskingResponse {
        entries: config.entries,
    }))
}

#[post("/bamboo/keyword-masking/validate")]
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

// Provider configuration endpoints

#[derive(Serialize)]
struct ProviderConfigResponse {
    provider: String,
    available_providers: Vec<String>,
    providers: Value,
}

#[derive(Deserialize)]
struct UpdateProviderRequest {
    provider: String,
    #[serde(default)]
    providers: Value,
}

/// Get current provider configuration
#[get("/bamboo/settings/provider")]
pub async fn get_provider_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let path = config_path(&app_state);

    let config_value = match fs::read_to_string(&path).await {
        Ok(content) => {
            let mut config: Value = serde_json::from_str(&content)?;
            decrypt_proxy_auth(&mut config);

            let mut needs_save = false;

            // Migration 1: Move root-level "model" field to provider-specific config
            if let Some(old_model) = config
                .get("model")
                .and_then(|m| m.as_str())
                .map(|s| s.to_string())
            {
                let provider = config
                    .get("provider")
                    .and_then(|p| p.as_str())
                    .unwrap_or("copilot")
                    .to_string();

                // Only migrate for non-Copilot providers
                if provider != "copilot" {
                    if let Some(providers) = config.get_mut("providers") {
                        if let Some(provider_config) = providers.get_mut(&provider) {
                            // Only set if not already present
                            if provider_config.get("model").is_none() {
                                provider_config["model"] = Value::String(old_model.clone());
                                log::info!(
                                    "Migrated root-level model '{}' to provider '{}' config",
                                    old_model,
                                    provider
                                );

                                // Remove root-level model field
                                if let Some(obj) = config.as_object_mut() {
                                    obj.remove("model");
                                }
                                needs_save = true;
                            }
                        }
                    }
                }
            }

            // Migration 2: Move root-level "headless_auth" to providers.copilot.headless_auth
            if let Some(headless_auth) = config.get("headless_auth").and_then(|h| h.as_bool()) {
                if let Some(providers) = config.get_mut("providers") {
                    // Ensure copilot config exists
                    if providers.get("copilot").is_none() {
                        providers["copilot"] = Value::Object(serde_json::Map::new());
                    }

                    if let Some(copilot_config) = providers.get_mut("copilot") {
                        // Only set if not already present
                        if copilot_config.get("headless_auth").is_none() {
                            copilot_config["headless_auth"] = Value::Bool(headless_auth);
                            log::info!(
                                "Migrated root-level headless_auth to providers.copilot config"
                            );

                            // Remove root-level headless_auth field
                            if let Some(obj) = config.as_object_mut() {
                                obj.remove("headless_auth");
                            }
                            needs_save = true;
                        }
                    }
                }
            }

            // Save migrated config if needed
            if needs_save {
                let mut config_to_save = config.clone();
                encrypt_proxy_auth(&mut config_to_save)?;
                let content = serde_json::to_string_pretty(&config_to_save)?;
                fs::write(&path, content).await?;
                log::info!("Saved migrated configuration to file");
            }

            config
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // Return default config if file doesn't exist
            serde_json::json!({
                "provider": "copilot",
                "providers": {}
            })
        }
        Err(err) => return Err(AppError::StorageError(err)),
    };

    let provider = config_value
        .get("provider")
        .and_then(|v| v.as_str())
        .unwrap_or("copilot")
        .to_string();

    // Get providers config (mask API keys for security)
    let providers = config_value
        .get("providers")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    // Mask API keys in the response
    let masked_providers = mask_api_keys_in_providers(&providers);

    let response = ProviderConfigResponse {
        provider,
        available_providers: AVAILABLE_PROVIDERS.iter().map(|s| s.to_string()).collect(),
        providers: masked_providers,
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Mask API keys in provider config for security
fn mask_api_keys_in_providers(providers: &Value) -> Value {
    let mut masked = providers.clone();

    if let Some(obj) = masked.as_object_mut() {
        for (_, provider_config) in obj.iter_mut() {
            if let Some(config_obj) = provider_config.as_object_mut() {
                if let Some(api_key) = config_obj.get_mut("api_key") {
                    if let Some(key_str) = api_key.as_str() {
                        // Always use fixed-length mask to prevent information disclosure
                        if !key_str.is_empty() {
                            *api_key = Value::String("****...****".to_string());
                        }
                    }
                }
            }
        }
    }

    masked
}

/// Update provider configuration
#[post("/bamboo/settings/provider")]
pub async fn update_provider_config(
    app_state: web::Data<AppState>,
    payload: web::Json<UpdateProviderRequest>,
) -> Result<HttpResponse, AppError> {
    let path = config_path(&app_state);

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    // Read existing config
    let mut existing_config: Value = match fs::read_to_string(&path).await {
        Ok(content) => {
            let mut config: Value = serde_json::from_str(&content)?;
            decrypt_proxy_auth(&mut config);
            config
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            serde_json::json!({})
        }
        Err(err) => return Err(AppError::StorageError(err)),
    };

    // Update provider
    if let Some(obj) = existing_config.as_object_mut() {
        obj.insert(
            "provider".to_string(),
            Value::String(payload.provider.clone()),
        );

        // Merge providers config
        if let Some(existing_providers) = obj.get_mut("providers") {
            if let Some(existing_obj) = existing_providers.as_object_mut() {
                if let Some(new_providers) = payload.providers.as_object() {
                    for (key, value) in new_providers.iter() {
                        // Don't overwrite with masked values
                        if let Some(new_obj) = value.as_object() {
                            if let Some(api_key) = new_obj.get("api_key") {
                                if let Some(key_str) = api_key.as_str() {
                                    if key_str.contains("***") || key_str.contains("...") {
                                        // This is a masked key, preserve the existing one
                                        if let Some(existing_provider) = existing_obj.get(key) {
                                            if let Some(existing_key) =
                                                existing_provider.get("api_key")
                                            {
                                                let mut merged = value.clone();
                                                if let Some(merged_obj) = merged.as_object_mut() {
                                                    merged_obj.insert(
                                                        "api_key".to_string(),
                                                        existing_key.clone(),
                                                    );
                                                }
                                                existing_obj.insert(key.clone(), merged);
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        existing_obj.insert(key.clone(), value.clone());
                    }
                }
            } else {
                obj.insert("providers".to_string(), payload.providers.clone());
            }
        } else {
            obj.insert("providers".to_string(), payload.providers.clone());
        }
    }

    // Clean empty proxy fields
    let mut config_to_save = clean_empty_proxy_fields(existing_config.clone());

    // Encrypt proxy auth if present
    encrypt_proxy_auth(&mut config_to_save)?;

    // Save to file
    let content = serde_json::to_string_pretty(&config_to_save)?;
    fs::write(&path, content).await?;

    log::info!("Provider configuration updated to: {}", payload.provider);

    // First, reload the configuration from file into AppState
    let new_config = app_state.reload_config().await;

    // Validate the configuration
    if let Err(e) = crate::agent::llm::validate_provider_config(&new_config) {
        log::error!("Invalid configuration after update: {}", e);
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "success": false,
            "error": format!("Configuration saved but invalid: {}", e)
        })));
    }

    // Reload provider to apply new configuration
    if let Err(e) = app_state.reload_provider().await {
        log::error!(
            "Failed to reload provider after updating configuration: {}",
            e
        );
        return Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "success": false,
            "error": format!("Configuration saved but failed to reload provider: {}", e)
        })));
    }

    log::info!("Provider reloaded successfully after configuration update");

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "provider": payload.provider
    })))
}

/// Fetch available models for a specific provider
#[post("/bamboo/settings/provider/models")]
pub async fn fetch_provider_models(
    app_state: web::Data<AppState>,
    payload: web::Json<serde_json::Value>,
) -> Result<HttpResponse, AppError> {
    let provider_type = payload
        .get("provider")
        .and_then(|v| v.as_str())
        .unwrap_or("openai");

    // Read current config to get the real API key
    let path = config_path(&app_state);
    let config_value = match fs::read_to_string(&path).await {
        Ok(content) => {
            let mut config: Value = serde_json::from_str(&content)?;
            decrypt_proxy_auth(&mut config);
            config
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(AppError::NotFound(
                "Configuration file not found".to_string(),
            ));
        }
        Err(err) => return Err(AppError::StorageError(err)),
    };

    // Get provider-specific config
    let provider_config = config_value
        .get("providers")
        .and_then(|p| p.get(provider_type))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    let api_key = provider_config
        .get("api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if api_key.is_empty() {
        return Err(AppError::BadRequest("API key not configured".to_string()));
    }

    let base_url = provider_config.get("base_url").and_then(|v| v.as_str());

    // Fetch models from the API
    let models = fetch_models_from_api(provider_type, api_key, base_url).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "models": models
    })))
}

/// Fetch models from external API
async fn fetch_models_from_api(
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

    let client = reqwest::Client::new();
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

/// Reload configuration and recreate provider
#[post("/bamboo/settings/reload")]
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

    log::info!("Provider reloaded successfully: {}", new_config.provider);

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "provider": new_config.provider
    })))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(list_workflows)
        .service(get_workflow)
        .service(save_workflow)
        .service(delete_workflow)
        // Setup status endpoints
        .service(get_setup_status)
        .service(mark_setup_complete)
        .service(mark_setup_incomplete)
        // Config endpoints
        .service(get_bamboo_config)
        .service(set_bamboo_config)
        .service(reset_bamboo_config)
        // Proxy auth endpoints (also registered with rate limiting in production via app_config_with_rate_limiting)
        .service(set_proxy_auth)
        .service(get_proxy_auth_status)
        // Keyword masking endpoints
        .service(get_keyword_masking_config)
        .service(update_keyword_masking_config)
        .service(validate_keyword_entries)
        // Provider configuration endpoints
        .service(get_provider_config)
        .service(update_provider_config)
        .service(reload_provider_config)
        .service(fetch_provider_models)
        // Other endpoints
        .service(get_anthropic_model_mapping)
        .service(set_anthropic_model_mapping);
}
