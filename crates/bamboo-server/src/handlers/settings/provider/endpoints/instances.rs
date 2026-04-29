//! Provider-instance CRUD endpoints (Slice C).
//!
//! Instances are the user-facing primitive of the multi-provider model: each
//! instance is identified by a user-supplied `id`, has a wire `format`
//! (openai / anthropic / gemini / copilot / bodhi), and carries its own
//! credentials, base_url, model overrides and `custom_models` list.
//!
//! Persistence: writes go through [`AppState::update_config`] which already
//! handles encryption (via `refresh_provider_api_keys_encrypted`) and on-disk
//! save. The provider registry is reloaded after every mutating call so
//! downstream chat/router traffic immediately sees the new state.
//!
//! API key handling on read: `ProviderInstance::api_key` is
//! `#[serde(skip_serializing)]`, so list/get responses naturally never expose
//! decrypted credentials. We additionally strip `api_key_encrypted` before
//! returning, mirroring the legacy provider redaction behaviour.

use actix_web::{web, HttpResponse};
use serde::Deserialize;
use serde_json::json;

use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};
use bamboo_infrastructure::ProviderInstance;

/// Strip encrypted-credential fields from a serialized instance before
/// returning it through the API. The plaintext `api_key` is already filtered
/// out by serde (`skip_serializing` on the field).
fn redact_instance(mut value: serde_json::Value) -> serde_json::Value {
    if let Some(obj) = value.as_object_mut() {
        // Encrypted material is only useful server-side.
        obj.remove("api_key_encrypted");
        // Defensive: in case future serde changes start emitting it.
        obj.remove("api_key");
    }
    value
}

fn instance_to_redacted_json(instance: &ProviderInstance) -> Result<serde_json::Value, AppError> {
    let v = serde_json::to_value(instance).map_err(|e| {
        AppError::InternalError(anyhow::anyhow!("Failed to serialize instance: {e}"))
    })?;
    Ok(redact_instance(v))
}

/// `GET /v1/bamboo/settings/provider/instances`
pub async fn list_instances(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await;
    let items: Vec<serde_json::Value> = config
        .providers
        .instances
        .iter()
        .map(instance_to_redacted_json)
        .collect::<Result<_, _>>()?;
    Ok(HttpResponse::Ok().json(json!({
        "instances": items,
        "default_instance": config.provider.clone(),
    })))
}

/// `GET /v1/bamboo/settings/provider/instances/{id}`
pub async fn get_instance(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let config = app_state.config.read().await;
    match config.providers.find_instance(&id) {
        Some(inst) => Ok(HttpResponse::Ok().json(instance_to_redacted_json(inst)?)),
        None => Ok(not_found(&id)),
    }
}

/// `POST /v1/bamboo/settings/provider/instances`
///
/// Body: full `ProviderInstance` JSON. `id` must be unique.
/// On success the instance is appended, persisted, and the provider registry
/// is rebuilt so the new instance becomes addressable for chat/list_models.
pub async fn create_instance(
    app_state: web::Data<AppState>,
    payload: web::Json<ProviderInstance>,
) -> Result<HttpResponse, AppError> {
    let new_instance = payload.into_inner();

    if new_instance.id.trim().is_empty() {
        return Ok(bad_request("Instance id must be non-empty"));
    }

    let id_for_response = new_instance.id.clone();

    let result = app_state
        .update_config(
            move |config| {
                config
                    .providers
                    .add_instance(new_instance.clone())
                    .map_err(AppError::BadRequest)
            },
            ConfigUpdateEffects {
                reload_provider: true,
                reconcile_mcp: false,
            },
        )
        .await;

    match result {
        Ok(cfg) => {
            let inst = cfg
                .providers
                .find_instance(&id_for_response)
                .ok_or_else(|| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Instance '{id_for_response}' missing after successful create"
                    ))
                })?;
            Ok(HttpResponse::Created().json(instance_to_redacted_json(inst)?))
        }
        Err(AppError::BadRequest(message)) => Ok(bad_request(&message)),
        Err(error) => Err(error),
    }
}

/// Body for `PUT`: full instance record (excluding api_key_encrypted, which is
/// recomputed on save).
#[derive(Deserialize)]
pub struct UpdateInstanceBody {
    #[serde(flatten)]
    pub instance: ProviderInstance,
}

/// `PUT /v1/bamboo/settings/provider/instances/{id}`
///
/// Replaces the entire instance record. The path `id` must match the body
/// `id`. If the body omits `api_key` (or sends an empty string), the existing
/// stored api_key is preserved — this matches the legacy "masked key"
/// behaviour so frontend round-trips of the redacted GET payload don't wipe
/// credentials.
pub async fn update_instance(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<UpdateInstanceBody>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let mut new_instance = payload.into_inner().instance;

    if new_instance.id != id {
        return Ok(bad_request(&format!(
            "Path id '{id}' does not match body id '{}'",
            new_instance.id
        )));
    }

    // Preserve api_key if the client didn't send one (e.g. they round-tripped
    // a redacted GET payload). The hydrated in-memory config still holds the
    // plaintext from startup, so we read it back here.
    if new_instance.api_key.trim().is_empty() {
        let cfg = app_state.config.read().await;
        if let Some(existing) = cfg.providers.find_instance(&id) {
            new_instance.api_key = existing.api_key.clone();
            new_instance.api_key_encrypted = existing.api_key_encrypted.clone();
        }
    }

    let id_for_response = id.clone();
    let result = app_state
        .update_config(
            move |config| {
                config
                    .providers
                    .replace_instance(&id, new_instance.clone())
                    .map_err(AppError::BadRequest)
            },
            ConfigUpdateEffects {
                reload_provider: true,
                reconcile_mcp: false,
            },
        )
        .await;

    match result {
        Ok(cfg) => {
            let inst = cfg
                .providers
                .find_instance(&id_for_response)
                .ok_or_else(|| {
                    AppError::InternalError(anyhow::anyhow!(
                        "Instance '{id_for_response}' missing after successful update"
                    ))
                })?;
            Ok(HttpResponse::Ok().json(instance_to_redacted_json(inst)?))
        }
        Err(AppError::BadRequest(message)) => {
            // Distinguish "not found" from generic validation issues.
            if message.contains("not found") {
                Ok(not_found(&id_for_response))
            } else {
                Ok(bad_request(&message))
            }
        }
        Err(error) => Err(error),
    }
}

/// `DELETE /v1/bamboo/settings/provider/instances/{id}`
pub async fn delete_instance(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let id_for_branch = id.clone();
    let result = app_state
        .update_config(
            move |config| {
                config
                    .providers
                    .remove_instance(&id)
                    .map(|_| ())
                    .map_err(AppError::BadRequest)
            },
            ConfigUpdateEffects {
                reload_provider: true,
                reconcile_mcp: false,
            },
        )
        .await;

    match result {
        Ok(_) => Ok(HttpResponse::Ok().json(json!({"success": true}))),
        Err(AppError::BadRequest(message)) => {
            if message.contains("not found") {
                Ok(not_found(&id_for_branch))
            } else {
                Ok(bad_request(&message))
            }
        }
        Err(error) => Err(error),
    }
}

fn bad_request(message: &str) -> HttpResponse {
    HttpResponse::BadRequest().json(json!({"success": false, "error": message}))
}

fn not_found(id: &str) -> HttpResponse {
    HttpResponse::NotFound().json(json!({
        "success": false,
        "error": format!("Provider instance '{id}' not found")
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::ProviderFormat;

    fn sample_instance() -> ProviderInstance {
        ProviderInstance {
            id: "deepseek".to_string(),
            display_name: Some("DeepSeek".to_string()),
            format: ProviderFormat::OpenAI,
            enabled: true,
            api_key: "sk-secret".to_string(),
            api_key_encrypted: Some("enc:abc".to_string()),
            base_url: Some("https://api.deepseek.com".to_string()),
            model: Some("deepseek-chat".to_string()),
            fast_model: None,
            vision_model: None,
            max_tokens: None,
            reasoning_effort: None,
            headless_auth: false,
            responses_only_models: vec![],
            target_provider: None,
            request_overrides: None,
            custom_models: vec!["deepseek-coder".to_string()],
            extra: Default::default(),
        }
    }

    #[test]
    fn redact_instance_strips_credentials() {
        let inst = sample_instance();
        let json = instance_to_redacted_json(&inst).unwrap();
        assert!(json.get("api_key").is_none());
        assert!(json.get("api_key_encrypted").is_none());
        assert_eq!(json["id"], "deepseek");
        assert_eq!(json["format"], "openai");
        assert_eq!(json["custom_models"][0], "deepseek-coder");
    }
}
