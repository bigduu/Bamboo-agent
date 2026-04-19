use actix_web::{web, HttpResponse};

use bamboo_infrastructure::config::EnvVarEntry;
use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};

use super::{
    types::{EnvVarResponse, EnvVarsListResponse, ReplaceEnvVarsRequest, UpsertEnvVarRequest},
    validation::{check_duplicate_names, validate_env_var_name, validate_env_var_value},
};

/// `GET /bamboo/env-vars` – list all env vars (secrets masked).
pub async fn list_env_vars(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await;
    let entries: Vec<EnvVarResponse> = config
        .env_vars
        .iter()
        .map(EnvVarResponse::from_entry)
        .collect();
    Ok(HttpResponse::Ok().json(EnvVarsListResponse { entries }))
}

/// `POST /bamboo/env-vars` – create or update a single env var.
pub async fn upsert_env_var(
    app_state: web::Data<AppState>,
    payload: web::Json<UpsertEnvVarRequest>,
) -> Result<HttpResponse, AppError> {
    let req = payload.into_inner();
    validate_env_var_name(&req.name)?;
    validate_env_var_value(&req.value)?;

    let updated = app_state
        .update_config(
            |cfg| {
                // Replace existing or push new.
                if let Some(existing) = cfg.env_vars.iter_mut().find(|e| e.name == req.name) {
                    // When editing: if value is empty, keep the existing value
                    // (the frontend sends empty to mean "keep current secret").
                    if !req.value.trim().is_empty() {
                        existing.value = req.value.clone();
                    }
                    existing.secret = req.secret;
                    existing.value_encrypted = None; // re-encrypted on persist
                    existing.description = req.description.clone();
                } else {
                    cfg.env_vars.push(EnvVarEntry {
                        name: req.name.clone(),
                        value: req.value.clone(),
                        secret: req.secret,
                        value_encrypted: None,
                        description: req.description.clone(),
                    });
                }
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    let entries: Vec<EnvVarResponse> = updated
        .env_vars
        .iter()
        .map(EnvVarResponse::from_entry)
        .collect();
    Ok(HttpResponse::Ok().json(EnvVarsListResponse { entries }))
}

/// `PUT /bamboo/env-vars` – replace the entire env vars list.
pub async fn replace_env_vars(
    app_state: web::Data<AppState>,
    payload: web::Json<ReplaceEnvVarsRequest>,
) -> Result<HttpResponse, AppError> {
    let req = payload.into_inner();

    // Validate all entries up-front.
    let names: Vec<&str> = req.entries.iter().map(|e| e.name.as_str()).collect();
    check_duplicate_names(&names)?;
    for entry in &req.entries {
        validate_env_var_name(&entry.name)?;
        validate_env_var_value(&entry.value)?;
    }

    let new_entries: Vec<EnvVarEntry> = req
        .entries
        .into_iter()
        .map(|e| EnvVarEntry {
            name: e.name,
            value: e.value,
            secret: e.secret,
            value_encrypted: None,
            description: e.description,
        })
        .collect();

    let updated = app_state
        .update_config(
            |cfg| {
                cfg.env_vars = new_entries.clone();
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    let entries: Vec<EnvVarResponse> = updated
        .env_vars
        .iter()
        .map(EnvVarResponse::from_entry)
        .collect();
    Ok(HttpResponse::Ok().json(EnvVarsListResponse { entries }))
}

/// `DELETE /bamboo/env-vars/{name}` – delete a single env var.
pub async fn delete_env_var(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let name = path.into_inner();

    let updated = app_state
        .update_config(
            |cfg| {
                let before = cfg.env_vars.len();
                cfg.env_vars.retain(|e| e.name != name);
                if cfg.env_vars.len() == before {
                    return Err(AppError::NotFound(format!(
                        "Environment variable '{}' not found",
                        name
                    )));
                }
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    let entries: Vec<EnvVarResponse> = updated
        .env_vars
        .iter()
        .map(EnvVarResponse::from_entry)
        .collect();
    Ok(HttpResponse::Ok().json(EnvVarsListResponse { entries }))
}
