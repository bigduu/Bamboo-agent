use crate::core::keyword_masking::{KeywordEntry, KeywordMaskingConfig};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::core::Config;

/// Response for keyword masking configuration
#[derive(Debug, Serialize, Deserialize)]
pub struct KeywordMaskingResponse {
    pub entries: Vec<KeywordEntry>,
}

/// Error response for validation failures
#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationError {
    pub index: usize,
    pub message: String,
}

pub fn load_keyword_masking_config(path: &Path) -> Result<KeywordMaskingConfig, String> {
    if !path.exists() {
        return Ok(KeywordMaskingConfig::default());
    }
    let content = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    serde_json::from_str(&content).map_err(|e| format!("Failed to parse keyword_masking.json: {e}"))
}

pub fn save_keyword_masking_config(
    path: &Path,
    config: &KeywordMaskingConfig,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let content = serde_json::to_string_pretty(config).map_err(|e| e.to_string())?;
    std::fs::write(path, content).map_err(|e| e.to_string())
}

/// Get the global keyword masking configuration
pub async fn get_keyword_masking_config() -> Result<KeywordMaskingResponse, String> {
    log::info!("Getting keyword masking configuration");

    let config = Config::new();

    Ok(KeywordMaskingResponse {
        entries: config.keyword_masking.entries,
    })
}

/// Update the global keyword masking configuration
pub async fn update_keyword_masking_config(
    entries: Vec<KeywordEntry>,
) -> Result<KeywordMaskingResponse, String> {
    log::info!(
        "Updating keyword masking configuration with {} entries",
        entries.len()
    );

    // Validate all entries
    let config = KeywordMaskingConfig { entries };

    if let Err(errors) = config.validate() {
        let error_messages: Vec<String> = errors
            .into_iter()
            .map(|(idx, msg)| format!("Entry {}: {}", idx, msg))
            .collect();
        return Err(format!("Validation failed: {}", error_messages.join(", ")));
    }

    let mut root = Config::new();
    root.keyword_masking = config.clone();
    root.save().map_err(|e| e.to_string())?;

    log::info!("Keyword masking configuration saved successfully");

    Ok(KeywordMaskingResponse {
        entries: config.entries,
    })
}

/// Validate keyword masking entries without saving
pub async fn validate_keyword_entries(
    entries: Vec<KeywordEntry>,
) -> Result<(), Vec<ValidationError>> {
    let config = KeywordMaskingConfig { entries };

    match config.validate() {
        Ok(()) => Ok(()),
        Err(errors) => {
            let validation_errors: Vec<ValidationError> = errors
                .into_iter()
                .map(|(idx, msg)| ValidationError {
                    index: idx,
                    message: msg,
                })
                .collect();
            Err(validation_errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_keyword_masking_defaults_when_missing() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("keyword_masking.json");

        let config = load_keyword_masking_config(&path).unwrap();
        assert!(config.entries.is_empty());
    }
}
