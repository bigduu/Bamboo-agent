use crate::core::keyword_masking::{KeywordEntry, KeywordMaskingConfig};
use serde::{Deserialize, Serialize};

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
