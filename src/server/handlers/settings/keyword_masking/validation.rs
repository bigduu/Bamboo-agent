use crate::{
    core::keyword_masking::{KeywordEntry, KeywordMaskingConfig},
    server::error::AppError,
};

use super::{
    constants::{MAX_ENTRIES, MAX_PATTERN_LENGTH},
    types::{map_validation_errors, ValidationError},
};

pub(super) fn build_validated_config(
    entries: Vec<KeywordEntry>,
) -> Result<KeywordMaskingConfig, AppError> {
    validate_entry_limits(&entries)?;

    let config = KeywordMaskingConfig { entries };
    if let Err(errors) = config.validate() {
        return Err(AppError::BadRequest(format!(
            "Validation failed: {:?}",
            map_validation_errors(errors)
        )));
    }

    Ok(config)
}

pub(super) fn validate_entries_only(
    entries: Vec<KeywordEntry>,
) -> Result<(), Vec<ValidationError>> {
    let config = KeywordMaskingConfig { entries };
    config.validate().map_err(map_validation_errors)
}

fn validate_entry_limits(entries: &[KeywordEntry]) -> Result<(), AppError> {
    if entries.len() > MAX_ENTRIES {
        return Err(AppError::BadRequest(format!(
            "Too many entries: {} (max {})",
            entries.len(),
            MAX_ENTRIES
        )));
    }

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

    Ok(())
}
