use std::path::{Path, PathBuf};

use crate::error::AppError;
use bamboo_compression::limits::ModelLimit;
use bamboo_llm::Config;
use serde_json::{Map, Value};

use super::super::super::redaction::redact_config_for_api;

const MODEL_LIMITS_KEY: &str = "model_limits";

pub(super) fn config_file_path(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join("config.json")
}

pub(super) fn model_limits_file_path(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join("model_limits.json")
}

pub(super) async fn read_model_limits_file(app_data_dir: &Path) -> Result<Option<Value>, AppError> {
    let path = model_limits_file_path(app_data_dir);
    match tokio::fs::try_exists(&path).await {
        Ok(false) => Ok(None),
        Ok(true) => {
            let content = tokio::fs::read_to_string(&path)
                .await
                .map_err(AppError::StorageError)?;
            let limits: Vec<ModelLimit> = serde_json::from_str(&content)?;
            Ok(Some(serde_json::to_value(limits)?))
        }
        Err(error) => Err(AppError::StorageError(error)),
    }
}

pub(super) async fn write_model_limits_file(
    app_data_dir: &Path,
    value: Option<&Value>,
) -> Result<(), AppError> {
    let path = model_limits_file_path(app_data_dir);
    match value {
        None => remove_file_if_exists(&path).await,
        Some(value) => {
            // Diff-only storage: a row identical to the global default carries no
            // information, so drop it. This keeps `model_limits.json` to genuine
            // overrides and lets future default changes propagate to everyone.
            let limits: Vec<ModelLimit> = serde_json::from_value(value.clone())?;
            let overrides: Vec<ModelLimit> = limits
                .into_iter()
                .filter(|limit| !bamboo_compression::limits::is_default_limit(limit))
                .collect();

            if overrides.is_empty() {
                return remove_file_if_exists(&path).await;
            }

            let content = serde_json::to_string_pretty(&overrides)?;
            tokio::fs::write(&path, content)
                .await
                .map_err(AppError::StorageError)?;
            Ok(())
        }
    }
}

async fn remove_file_if_exists(path: &Path) -> Result<(), AppError> {
    if tokio::fs::try_exists(path)
        .await
        .map_err(AppError::StorageError)?
    {
        tokio::fs::remove_file(path)
            .await
            .map_err(AppError::StorageError)?;
    }
    Ok(())
}

pub(super) fn take_model_limits_patch(patch_obj: &mut Map<String, Value>) -> Option<Value> {
    patch_obj.remove(MODEL_LIMITS_KEY)
}

pub(super) async fn redacted_config_json(
    config: &Config,
    app_data_dir: &Path,
) -> Result<Value, AppError> {
    let mut config_for_response = config.clone();
    config_for_response.refresh_proxy_auth_encrypted()?;
    config_for_response.refresh_provider_api_keys_encrypted()?;
    let value = serde_json::to_value(&config_for_response)?;
    let mut redacted = redact_config_for_api(value, &config_for_response);

    if let Some(model_limits) = read_model_limits_file(app_data_dir).await? {
        if let Some(obj) = redacted.as_object_mut() {
            obj.insert(MODEL_LIMITS_KEY.to_string(), model_limits);
        }
    } else if let Some(value) = config.extra.get(MODEL_LIMITS_KEY) {
        if let Some(obj) = redacted.as_object_mut() {
            obj.insert(MODEL_LIMITS_KEY.to_string(), value.clone());
        }
    }

    Ok(redacted)
}
