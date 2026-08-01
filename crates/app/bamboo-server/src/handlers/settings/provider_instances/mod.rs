//! Provider instance CRUD APIs.
//!
//! Endpoints:
//! - `GET    /bamboo/settings/provider-instances`         — list all instances
//! - `POST   /bamboo/settings/provider-instances`         — create a new instance
//! - `PUT    /bamboo/settings/provider-instances/{id}`    — update an instance
//! - `DELETE /bamboo/settings/provider-instances/{id}`    — delete an instance
//! - `POST   /bamboo/settings/provider-instances/default` — set default instance

use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use bamboo_config::{FeatureFlags, ProviderInstanceConfig};
use bamboo_llm::AVAILABLE_PROVIDERS;

use crate::app_state::{AppState, ConfigUpdateEffects};
use crate::config_manager;
use crate::error::AppError;

// ─── Types ──────────────────────────────────────────────────────────

/// Frontend-facing response body for `GET /provider-instances`.
#[derive(Serialize)]
pub struct ListInstancesResponse {
    pub instances: Vec<ProviderInstanceResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_provider_instance_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defaults: Option<bamboo_config::DefaultsConfig>,
    pub features: FeatureFlags,
}

/// Frontend-facing provider instance shape.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProviderInstanceResponse {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub label: String,
    pub enabled: bool,
    pub config: Map<String, Value>,
}

/// Request body for `POST /provider-instances` (create).
#[derive(Debug, Clone, Deserialize)]
pub struct CreateInstanceRequest {
    #[serde(rename = "type")]
    pub provider_type: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    pub config: Value,
}

/// Request body for `PUT /provider-instances/{id}` (update).
#[derive(Debug, Clone, Deserialize)]
pub struct UpdateInstanceRequest {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub config: Option<Value>,
}

/// Request body for `POST /provider-instances/default` (set default).
#[derive(Debug, Clone, Deserialize)]
pub struct SetDefaultInstanceRequest {
    #[serde(alias = "instance_id")]
    pub default_provider_instance_id: String,
}

// ─── Helpers ────────────────────────────────────────────────────────

fn default_label_for_provider_type(provider_type: &str) -> String {
    match provider_type {
        "openai" => "OpenAI".to_string(),
        "anthropic" => "Anthropic".to_string(),
        "gemini" => "Gemini".to_string(),
        "copilot" => "GitHub Copilot".to_string(),
        "bodhi" => "Bodhi".to_string(),
        other => other.to_string(),
    }
}

fn instance_config_to_api(instance: &ProviderInstanceConfig) -> Map<String, Value> {
    let mut config = Map::new();

    if !instance.api_key.trim().is_empty()
        || instance.api_key_encrypted.is_some()
        || instance.credential_ref.is_some()
    {
        config.insert(
            "api_key".to_string(),
            Value::String("****...****".to_string()),
        );
    }

    if let Some(base_url) = &instance.base_url {
        config.insert("base_url".to_string(), Value::String(base_url.clone()));
    }
    if let Some(model) = &instance.model {
        config.insert("model".to_string(), Value::String(model.clone()));
    }
    if let Some(fast_model) = &instance.fast_model {
        config.insert("fast_model".to_string(), Value::String(fast_model.clone()));
    }
    if let Some(vision_model) = &instance.vision_model {
        config.insert(
            "vision_model".to_string(),
            Value::String(vision_model.clone()),
        );
    }
    if let Some(reasoning_effort) = instance.reasoning_effort {
        config.insert(
            "reasoning_effort".to_string(),
            serde_json::to_value(reasoning_effort).expect("reasoning_effort should serialize"),
        );
    }
    if !instance.responses_only_models.is_empty() {
        config.insert(
            "responses_only_models".to_string(),
            serde_json::to_value(&instance.responses_only_models)
                .expect("responses_only_models should serialize"),
        );
    }
    if let Some(request_overrides) = &instance.request_overrides {
        config.insert(
            "request_overrides".to_string(),
            serde_json::to_value(request_overrides).expect("request_overrides should serialize"),
        );
    }

    for (key, value) in &instance.extra {
        if key == "api_key_encrypted" {
            continue;
        }
        config.insert(key.clone(), value.clone());
    }

    config
}

fn instance_to_api(id: &str, instance: &ProviderInstanceConfig) -> ProviderInstanceResponse {
    ProviderInstanceResponse {
        id: id.to_string(),
        r#type: instance.provider_type.clone(),
        label: instance
            .label
            .clone()
            .unwrap_or_else(|| default_label_for_provider_type(&instance.provider_type)),
        enabled: instance.enabled,
        config: instance_config_to_api(instance),
    }
}

fn validate_instance_id(id: &str) -> Result<(), AppError> {
    if id.trim().is_empty() {
        return Err(AppError::BadRequest(
            "instance_id must not be empty".to_string(),
        ));
    }
    if id.len() > 128 {
        return Err(AppError::BadRequest(
            "instance_id must be at most 128 characters".to_string(),
        ));
    }
    Ok(())
}

fn validate_provider_type(provider_type: &str) -> Result<(), AppError> {
    if !AVAILABLE_PROVIDERS.contains(&provider_type) {
        return Err(AppError::BadRequest(format!(
            "Unknown provider_type '{}'. Available: {}",
            provider_type,
            AVAILABLE_PROVIDERS.join(", ")
        )));
    }
    Ok(())
}

fn validate_instance_config(instance: &ProviderInstanceConfig) -> Result<(), AppError> {
    validate_provider_type(&instance.provider_type)?;

    if let Some(value) = instance
        .extra
        .get(bamboo_config::OPENAI_EXPLICIT_PROMPT_CACHE_CONFIG_KEY)
    {
        if instance.provider_type != "openai" {
            return Err(AppError::BadRequest(format!(
                "{} is only accepted for OpenAI provider instances",
                bamboo_config::OPENAI_EXPLICIT_PROMPT_CACHE_CONFIG_KEY
            )));
        }
        if !value.is_boolean() {
            return Err(AppError::BadRequest(format!(
                "{} must be a boolean",
                bamboo_config::OPENAI_EXPLICIT_PROMPT_CACHE_CONFIG_KEY
            )));
        }
    }

    if instance.provider_type != "copilot" && instance.api_key.trim().is_empty() {
        return Err(AppError::BadRequest(
            "api_key is required for non-copilot providers".to_string(),
        ));
    }

    Ok(())
}

fn config_value_as_object(value: Value) -> Result<Map<String, Value>, AppError> {
    config_manager::assert_json_object(value)
}

fn build_instance_from_create(
    payload: &CreateInstanceRequest,
) -> Result<ProviderInstanceConfig, AppError> {
    validate_provider_type(&payload.provider_type)?;

    let mut obj = config_value_as_object(payload.config.clone())?;
    obj.remove("api_key_encrypted");
    obj.remove("credential_ref");
    obj.insert(
        "provider_type".to_string(),
        Value::String(payload.provider_type.clone()),
    );
    if let Some(label) = payload.label.clone() {
        obj.insert("label".to_string(), Value::String(label));
    }
    if let Some(enabled) = payload.enabled {
        obj.insert("enabled".to_string(), Value::Bool(enabled));
    }

    let instance: ProviderInstanceConfig = serde_json::from_value(Value::Object(obj))
        .map_err(|e| AppError::BadRequest(format!("Invalid provider instance config: {e}")))?;
    // On CREATE there is no existing key a placeholder could refer to — storing
    // the literal `****...****` as the api_key would break the provider (#430).
    if config_manager::is_masked_api_key(&instance.api_key) {
        return Err(AppError::BadRequest(
            "api_key looks like a masked placeholder (`****...****`); enter the real key"
                .to_string(),
        ));
    }
    validate_instance_config(&instance)?;
    Ok(instance)
}

fn instance_to_patchable_value(instance: &ProviderInstanceConfig) -> Result<Value, AppError> {
    let mut value = serde_json::to_value(instance).map_err(|e| {
        AppError::InternalError(anyhow::anyhow!(
            "Failed to serialize provider instance for patching: {e}"
        ))
    })?;

    let Some(obj) = value.as_object_mut() else {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "Serialized provider instance was not a JSON object"
        )));
    };

    if !instance.api_key.trim().is_empty() {
        obj.insert(
            "api_key".to_string(),
            Value::String(instance.api_key.clone()),
        );
    }

    Ok(value)
}

fn apply_instance_update(
    existing: &ProviderInstanceConfig,
    payload: &UpdateInstanceRequest,
) -> Result<ProviderInstanceConfig, AppError> {
    let mut merged = instance_to_patchable_value(existing)?;
    let Some(obj) = merged.as_object_mut() else {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "Serialized provider instance was not a JSON object"
        )));
    };

    if let Some(label) = payload.label.clone() {
        obj.insert("label".to_string(), Value::String(label));
    }
    if let Some(enabled) = payload.enabled {
        obj.insert("enabled".to_string(), Value::Bool(enabled));
    }

    if let Some(config_patch) = payload.config.clone() {
        let mut patch_obj = config_value_as_object(config_patch)?;

        if let Some(api_key) = patch_obj.get("api_key").and_then(|v| v.as_str()) {
            if config_manager::is_masked_api_key(api_key) {
                if existing.api_key.trim().is_empty() {
                    patch_obj.remove("api_key");
                } else {
                    patch_obj.insert(
                        "api_key".to_string(),
                        Value::String(existing.api_key.clone()),
                    );
                }
            }
        }

        patch_obj.remove("provider_type");
        patch_obj.remove("label");
        patch_obj.remove("enabled");
        patch_obj.remove("api_key_encrypted");
        patch_obj.remove("credential_ref");

        for (key, value) in patch_obj {
            obj.insert(key, value);
        }
    }

    obj.insert(
        "provider_type".to_string(),
        Value::String(existing.provider_type.clone()),
    );

    let updated: ProviderInstanceConfig = serde_json::from_value(merged)
        .map_err(|e| AppError::BadRequest(format!("Invalid provider instance config: {e}")))?;
    validate_instance_config(&updated)?;
    Ok(updated)
}

// ─── Handlers ───────────────────────────────────────────────────────

/// GET /bamboo/settings/provider-instances
pub async fn list_provider_instances(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await;
    let instances: Vec<ProviderInstanceResponse> = config
        .provider_instances
        .iter()
        .map(|(id, instance)| instance_to_api(id, instance))
        .collect();

    Ok(HttpResponse::Ok().json(ListInstancesResponse {
        instances,
        default_provider_instance_id: config.default_provider_instance.clone(),
        defaults: config.defaults.clone(),
        features: config.features.clone(),
    }))
}

/// POST /bamboo/settings/provider-instances
pub async fn create_provider_instance(
    app_state: web::Data<AppState>,
    payload: web::Json<CreateInstanceRequest>,
) -> Result<HttpResponse, AppError> {
    let instance_id = Uuid::new_v4().to_string();
    validate_instance_id(&instance_id)?;

    let instance_config = build_instance_from_create(&payload)?;
    let instance_id_for_response = instance_id.clone();
    let instance_id_for_intent = instance_id.clone();

    let new_config = app_state
        .update_config_with_provider_credentials(
            move |config| {
                if config.provider_instances.contains_key(&instance_id) {
                    return Err(AppError::BadRequest(format!(
                        "Provider instance '{}' already exists",
                        instance_id
                    )));
                }
                let should_set_default = config.default_provider_instance.is_none();
                config
                    .provider_instances
                    .insert(instance_id.clone(), instance_config.clone());
                if should_set_default {
                    config.default_provider_instance = Some(instance_id.clone());
                }
                // No explicit ciphertext refresh needed here: `update_config`
                // runs `Config::refresh_encrypted_secrets` on the live config
                // after this closure (#515/#516), so `api_key_encrypted` is
                // populated in memory before persist/response.
                Ok(())
            },
            std::collections::BTreeSet::new(),
            std::collections::BTreeSet::from([instance_id_for_intent]),
            ConfigUpdateEffects {
                reload_provider: true,
                reconcile_mcp: false,
            },
        )
        .await?;

    let created = new_config
        .provider_instances
        .get(&instance_id_for_response)
        .ok_or_else(|| {
            AppError::InternalError(anyhow::anyhow!(
                "Created provider instance '{}' missing from config snapshot",
                instance_id_for_response
            ))
        })?;

    Ok(HttpResponse::Created().json(instance_to_api(&instance_id_for_response, created)))
}

/// PUT /bamboo/settings/provider-instances/{instance_id}
pub async fn update_provider_instance(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<UpdateInstanceRequest>,
) -> Result<HttpResponse, AppError> {
    let instance_id = path.into_inner();
    validate_instance_id(&instance_id)?;
    let payload_inner = payload.into_inner();
    let instance_id_for_response = instance_id.clone();
    let has_api_key_intent = payload_inner
        .config
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|config| config.get("api_key"))
        .is_some_and(|value| match value {
            Value::Null => true,
            Value::String(value) => !config_manager::is_masked_api_key(value),
            _ => false,
        });
    let provider_instance_intents = if has_api_key_intent {
        std::collections::BTreeSet::from([instance_id.clone()])
    } else {
        std::collections::BTreeSet::new()
    };

    let new_config = app_state
        .update_config_with_provider_credentials(
            move |config| {
                let existing = config
                    .provider_instances
                    .get(&instance_id)
                    .cloned()
                    .ok_or_else(|| {
                        AppError::BadRequest(format!(
                            "Provider instance '{}' not found",
                            instance_id
                        ))
                    })?;

                let updated = apply_instance_update(&existing, &payload_inner)?;
                config
                    .provider_instances
                    .insert(instance_id.clone(), updated);
                // Ciphertext sync happens centrally in `update_config` via
                // `Config::refresh_encrypted_secrets` (#515/#516).
                Ok(())
            },
            std::collections::BTreeSet::new(),
            provider_instance_intents,
            ConfigUpdateEffects {
                reload_provider: true,
                reconcile_mcp: false,
            },
        )
        .await?;

    let updated = new_config
        .provider_instances
        .get(&instance_id_for_response)
        .ok_or_else(|| {
            AppError::InternalError(anyhow::anyhow!(
                "Updated provider instance '{}' missing from config snapshot",
                instance_id_for_response
            ))
        })?;

    Ok(HttpResponse::Ok().json(instance_to_api(&instance_id_for_response, updated)))
}

/// DELETE /bamboo/settings/provider-instances/{instance_id}
pub async fn delete_provider_instance(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let instance_id = path.into_inner();
    validate_instance_id(&instance_id)?;
    let instance_id_for_closure = instance_id.clone();
    let provider_instance_intents = std::collections::BTreeSet::from([instance_id.clone()]);

    let new_config = app_state
        .update_config_with_provider_credentials(
            move |config| {
                if config
                    .provider_instances
                    .remove(&instance_id_for_closure)
                    .is_none()
                {
                    return Err(AppError::BadRequest(format!(
                        "Provider instance '{}' not found",
                        instance_id_for_closure
                    )));
                }
                if config.default_provider_instance.as_deref() == Some(&instance_id_for_closure) {
                    config.default_provider_instance = None;
                }
                Ok(())
            },
            std::collections::BTreeSet::new(),
            provider_instance_intents,
            ConfigUpdateEffects {
                reload_provider: true,
                reconcile_mcp: false,
            },
        )
        .await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "deleted": instance_id,
        "default_provider_instance_id": new_config.default_provider_instance,
    })))
}

/// POST /bamboo/settings/provider-instances/default
pub async fn set_default_provider_instance(
    app_state: web::Data<AppState>,
    payload: web::Json<SetDefaultInstanceRequest>,
) -> Result<HttpResponse, AppError> {
    let target_id = payload.default_provider_instance_id.clone();

    let new_config = app_state
        .update_config(
            move |config| {
                let is_instance = config.provider_instances.contains_key(&target_id);
                let is_legacy_type = AVAILABLE_PROVIDERS.contains(&target_id.as_str());
                if !is_instance && !is_legacy_type {
                    return Err(AppError::BadRequest(format!(
                        "Cannot set '{}' as default: not a known provider instance or type",
                        target_id
                    )));
                }
                config.default_provider_instance = Some(target_id.clone());
                Ok(())
            },
            ConfigUpdateEffects {
                reload_provider: true,
                reconcile_mcp: false,
            },
        )
        .await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "default_provider_instance_id": new_config.default_provider_instance,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_request(api_key: &str) -> CreateInstanceRequest {
        CreateInstanceRequest {
            provider_type: "openai".to_string(),
            label: None,
            enabled: None,
            config: serde_json::json!({ "api_key": api_key }),
        }
    }

    #[test]
    fn create_rejects_masked_placeholder_api_key() {
        let err = build_instance_from_create(&create_request("****...****"))
            .expect_err("placeholder api_key must be rejected on create");
        assert!(matches!(err, AppError::BadRequest(_)));
    }

    #[test]
    fn create_accepts_real_api_key_even_with_placeholder_residue() {
        // A paste that didn't fully clear the prefilled placeholder is a real
        // (if garbled) key — it must be stored as given, not silently dropped.
        let instance = build_instance_from_create(&create_request("****...****sk-new"))
            .expect("mixed value is not a placeholder");
        assert_eq!(instance.api_key, "****...****sk-new");
    }

    #[test]
    fn openai_instance_accepts_explicit_prompt_cache_switch() {
        let mut request = create_request("sk-real");
        request.config["explicit_prompt_cache"] = Value::Bool(false);

        let instance = build_instance_from_create(&request).expect("valid OpenAI switch");
        assert_eq!(
            instance
                .extra
                .get(bamboo_config::OPENAI_EXPLICIT_PROMPT_CACHE_CONFIG_KEY),
            Some(&Value::Bool(false))
        );
        assert_eq!(
            instance_config_to_api(&instance)
                .get(bamboo_config::OPENAI_EXPLICIT_PROMPT_CACHE_CONFIG_KEY),
            Some(&Value::Bool(false))
        );
    }

    #[test]
    fn explicit_prompt_cache_switch_rejects_wrong_type_or_provider() {
        let mut wrong_type = create_request("sk-real");
        wrong_type.config["explicit_prompt_cache"] = Value::String("false".to_string());
        assert!(matches!(
            build_instance_from_create(&wrong_type),
            Err(AppError::BadRequest(_))
        ));

        let mut wrong_provider = create_request("sk-real");
        wrong_provider.provider_type = "anthropic".to_string();
        wrong_provider.config["explicit_prompt_cache"] = Value::Bool(false);
        assert!(matches!(
            build_instance_from_create(&wrong_provider),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn create_and_update_ignore_client_owned_credential_metadata() {
        let mut request = create_request("sk-real");
        request.config["api_key_encrypted"] = Value::String("client-cipher".to_string());
        request.config["credential_ref"] = Value::String("attacker.chosen.ref".to_string());
        let instance = build_instance_from_create(&request).unwrap();
        assert!(instance.api_key_encrypted.is_none());
        assert!(instance.credential_ref.is_none());

        let updated = apply_instance_update(
            &instance,
            &UpdateInstanceRequest {
                label: None,
                enabled: None,
                config: Some(serde_json::json!({
                    "api_key_encrypted": "replacement-cipher",
                    "credential_ref": "attacker.replacement.ref",
                    "model": "gpt-safe"
                })),
            },
        )
        .unwrap();
        assert!(updated.api_key_encrypted.is_none());
        assert!(updated.credential_ref.is_none());
        assert_eq!(updated.model.as_deref(), Some("gpt-safe"));
    }

    #[test]
    fn api_reports_ref_backed_instance_as_configured_without_exposing_ref() {
        let mut instance = build_instance_from_create(&create_request("sk-real")).unwrap();
        instance.api_key.clear();
        instance.credential_ref =
            Some(bamboo_config::credential_ref("provider_instance", "work", "api_key").unwrap());
        let api = instance_config_to_api(&instance);
        assert_eq!(api["api_key"], "****...****");
        assert!(!api.contains_key("credential_ref"));
        assert!(!api.contains_key("api_key_encrypted"));
    }

    // A provider-instance secret is committed to credentials.json before its
    // ref-backed metadata is published. Later generic saves must retain the ref
    // without reintroducing ciphertext into config.json.
    #[tokio::test]
    async fn settings_save_preserves_instance_credential_ref_same_session() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let app_state = web::Data::new(state);

        let create_resp = create_provider_instance(
            app_state.clone(),
            web::Json(create_request("sk-instance-secret")),
        )
        .await
        .expect("create should succeed");
        assert_eq!(create_resp.status(), actix_web::http::StatusCode::CREATED);

        // A settings save that does not mention `provider_instances` at all.
        let save_payload: Value = serde_json::json!({ "headless_auth": true });
        crate::handlers::settings::set_bamboo_config(app_state.clone(), web::Json(save_payload))
            .await
            .expect("unrelated settings save should succeed");

        let config_path = temp_dir.path().join("providers.json");
        let raw = std::fs::read_to_string(&config_path).expect("providers.json should exist");
        let on_disk: Value =
            serde_json::from_str(&raw).expect("providers.json should be valid JSON");
        let instances = on_disk
            .get("data")
            .and_then(|data| data.get("provider_instances"))
            .and_then(|v| v.as_object())
            .expect("provider_instances should be present on disk");
        assert_eq!(instances.len(), 1, "exactly one instance was created");
        let (_id, instance) = instances.iter().next().expect("instance present");
        assert!(instance.get("api_key_encrypted").is_none());
        let reference = bamboo_config::CredentialRef::parse(
            instance["credential_ref"].as_str().expect("credential ref"),
        )
        .unwrap();
        assert_eq!(
            bamboo_config::CredentialStore::open(temp_dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-instance-secret"
        );
    }

    // Same as above but through the update path: an update that never touches
    // `config` (so `api_key` isn't in the patch at all) must also keep the
    // live in-memory ciphertext in sync, so a later unrelated settings save
    // doesn't wipe it either.
    #[tokio::test]
    async fn settings_save_preserves_instance_credential_ref_after_update_same_session() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let app_state = web::Data::new(state);

        create_provider_instance(
            app_state.clone(),
            web::Json(create_request("sk-instance-secret")),
        )
        .await
        .expect("create should succeed");

        let instance_id = {
            let config = app_state.config.read().await;
            config
                .provider_instances
                .keys()
                .next()
                .cloned()
                .expect("instance exists")
        };

        let update_payload = UpdateInstanceRequest {
            label: Some("Renamed".to_string()),
            enabled: None,
            config: None,
        };
        update_provider_instance(
            app_state.clone(),
            web::Path::from(instance_id.clone()),
            web::Json(update_payload),
        )
        .await
        .expect("update should succeed");

        let save_payload: Value = serde_json::json!({ "headless_auth": true });
        crate::handlers::settings::set_bamboo_config(app_state.clone(), web::Json(save_payload))
            .await
            .expect("unrelated settings save should succeed");

        let config_path = temp_dir.path().join("providers.json");
        let raw = std::fs::read_to_string(&config_path).expect("providers.json should exist");
        let on_disk: Value =
            serde_json::from_str(&raw).expect("providers.json should be valid JSON");
        let instance = &on_disk["data"]["provider_instances"][&instance_id];
        assert!(instance.get("api_key_encrypted").is_none());
        let reference = bamboo_config::CredentialRef::parse(
            instance["credential_ref"].as_str().expect("credential ref"),
        )
        .unwrap();
        assert_eq!(
            bamboo_config::CredentialStore::open(temp_dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-instance-secret"
        );
        assert_eq!(
            instance.get("label").and_then(|v| v.as_str()),
            Some("Renamed"),
            "the update itself should still apply"
        );
    }
}
