use crate::web_service::error::AppError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnthropicModelMapping {
    #[serde(default)]
    pub mappings: HashMap<String, String>,
}

pub async fn load_anthropic_model_mapping(
    data_dir: &PathBuf,
) -> Result<AnthropicModelMapping, AppError> {
    let path = data_dir.join("anthropic-model-mapping.json");
    match fs::read(&path).await {
        Ok(content) => {
            let mapping = serde_json::from_slice::<AnthropicModelMapping>(&content)?;
            Ok(mapping)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // Return default mapping if file doesn't exist
            Ok(AnthropicModelMapping::default())
        }
        Err(err) => Err(AppError::StorageError(err)),
    }
}

pub async fn save_anthropic_model_mapping(
    data_dir: &PathBuf,
    mapping: AnthropicModelMapping,
) -> Result<AnthropicModelMapping, AppError> {
    let path = data_dir.join("anthropic-model-mapping.json");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let data = serde_json::to_vec_pretty(&mapping)?;
    fs::write(path, data).await?;
    Ok(mapping)
}
