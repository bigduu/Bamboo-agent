use crate::web_service::error::AppError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GeminiModelMapping {
    #[serde(default)]
    pub mappings: HashMap<String, String>,
}

pub async fn load_gemini_model_mapping(data_dir: &Path) -> Result<GeminiModelMapping, AppError> {
    let path = data_dir.join("gemini-model-mapping.json");
    match fs::read(&path).await {
        Ok(content) => {
            let mapping = serde_json::from_slice::<GeminiModelMapping>(&content)?;
            Ok(mapping)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // Return default mapping if file doesn't exist
            Ok(GeminiModelMapping::default())
        }
        Err(err) => Err(AppError::StorageError(err)),
    }
}

pub async fn save_gemini_model_mapping(
    data_dir: &Path,
    mapping: GeminiModelMapping,
) -> Result<GeminiModelMapping, AppError> {
    let path = data_dir.join("gemini-model-mapping.json");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let data = serde_json::to_vec_pretty(&mapping)?;
    fs::write(path, data).await?;
    Ok(mapping)
}

/// Resolve a Gemini model name to the actual backend model
pub async fn resolve_model(
    data_dir: &Path,
    gemini_model: &str,
) -> anyhow::Result<ModelResolution> {
    let mapping = load_gemini_model_mapping(data_dir).await?;

    log::info!(
        "Resolving Gemini model '{}', available mappings: {:?}",
        gemini_model,
        mapping.mappings
    );

    // Match by keyword in model name (case-insensitive)
    let model_lower = gemini_model.to_lowercase();

    // Extract model type from Gemini model names like:
    // - gemini-pro, gemini-ultra, gemini-1.5-pro, gemini-1.5-flash, etc.
    let model_type = if model_lower.contains("ultra") {
        "ultra"
    } else if model_lower.contains("1.5") && model_lower.contains("flash") {
        "flash-1.5"
    } else if model_lower.contains("1.5") && model_lower.contains("pro") {
        "pro-1.5"
    } else if model_lower.contains("flash") {
        "flash"
    } else if model_lower.contains("pro") {
        "pro"
    } else {
        log::warn!(
            "No Gemini model mapping found for '{}', falling back to default model",
            gemini_model
        );
        return Ok(ModelResolution {
            mapped_model: String::new(),
            response_model: gemini_model.to_string(),
        });
    };

    if let Some(mapped) = mapping
        .mappings
        .get(model_type)
        .filter(|value| !value.trim().is_empty())
    {
        log::info!(
            "Model '{}' (type: {}) mapped to '{}'",
            gemini_model,
            model_type,
            mapped
        );
        return Ok(ModelResolution {
            mapped_model: mapped.to_string(),
            response_model: gemini_model.to_string(),
        });
    }

    log::warn!(
        "No mapping configured for model type '{}', falling back to default model",
        model_type
    );

    Ok(ModelResolution {
        mapped_model: String::new(),
        response_model: gemini_model.to_string(),
    })
}

#[derive(Clone)]
pub struct ModelResolution {
    pub mapped_model: String,
    pub response_model: String,
}
