use std::path::{Path, PathBuf};

use crate::core::Config;
use crate::server::error::AppError;
use serde_json::Value;

use super::super::super::redaction::redact_config_for_api;

pub(super) fn config_file_path(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join("config.json")
}

pub(super) fn redacted_config_json(config: &Config) -> Result<Value, AppError> {
    let mut config_for_response = config.clone();
    config_for_response.refresh_proxy_auth_encrypted()?;
    config_for_response.refresh_provider_api_keys_encrypted()?;
    let value = serde_json::to_value(&config_for_response)?;
    Ok(redact_config_for_api(value, &config_for_response))
}
