use bamboo_infrastructure_config::keyword_masking::{KeywordEntry, KeywordMaskingConfig};
use crate::error::AppError;

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

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_infrastructure_config::keyword_masking::MatchType;

    #[test]
    fn test_validate_entries_with_empty_list() {
        let entries = vec![];
        let result = validate_entries_only(entries);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_entries_with_valid_entry() {
        let entries = vec![KeywordEntry {
            pattern: "test".to_string(),
            match_type: MatchType::Exact,
            enabled: true,
        }];
        let result = validate_entries_only(entries);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_entries_with_invalid_regex() {
        let entries = vec![KeywordEntry {
            pattern: "[invalid(regex".to_string(),
            match_type: MatchType::Regex,
            enabled: true,
        }];
        let result = validate_entries_only(entries);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_validate_entry_limits_with_too_many_entries() {
        let entries: Vec<KeywordEntry> = (0..=MAX_ENTRIES)
            .map(|i| KeywordEntry {
                pattern: format!("pattern{}", i),
                match_type: MatchType::Exact,
                enabled: true,
            })
            .collect();
        let result = validate_entry_limits(&entries);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_entry_limits_with_max_entries() {
        let entries: Vec<KeywordEntry> = (0..MAX_ENTRIES)
            .map(|i| KeywordEntry {
                pattern: format!("pattern{}", i),
                match_type: MatchType::Exact,
                enabled: true,
            })
            .collect();
        let result = validate_entry_limits(&entries);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_entry_limits_with_pattern_too_long() {
        let entries = vec![KeywordEntry {
            pattern: "a".repeat(MAX_PATTERN_LENGTH + 1),
            match_type: MatchType::Exact,
            enabled: true,
        }];
        let result = validate_entry_limits(&entries);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_entry_limits_with_max_pattern_length() {
        let entries = vec![KeywordEntry {
            pattern: "a".repeat(MAX_PATTERN_LENGTH),
            match_type: MatchType::Exact,
            enabled: true,
        }];
        let result = validate_entry_limits(&entries);
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_validated_config_with_empty_entries() {
        let entries = vec![];
        let result = build_validated_config(entries);
        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.entries.is_empty());
    }

    #[test]
    fn test_build_validated_config_with_valid_entries() {
        let entries = vec![
            KeywordEntry {
                pattern: "secret1".to_string(),
                match_type: MatchType::Exact,
                enabled: true,
            },
            KeywordEntry {
                pattern: r"\d{4}".to_string(),
                match_type: MatchType::Regex,
                enabled: true,
            },
        ];
        let result = build_validated_config(entries);
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.entries.len(), 2);
    }

    #[test]
    fn test_build_validated_config_with_invalid_regex() {
        let entries = vec![KeywordEntry {
            pattern: "[invalid".to_string(),
            match_type: MatchType::Regex,
            enabled: true,
        }];
        let result = build_validated_config(entries);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_validated_config_with_too_many_entries() {
        let entries: Vec<KeywordEntry> = (0..=MAX_ENTRIES)
            .map(|i| KeywordEntry {
                pattern: format!("pattern{}", i),
                match_type: MatchType::Exact,
                enabled: true,
            })
            .collect();
        let result = build_validated_config(entries);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_validated_config_with_disabled_entry() {
        let entries = vec![KeywordEntry {
            pattern: "test".to_string(),
            match_type: MatchType::Exact,
            enabled: false,
        }];
        let result = build_validated_config(entries);
        assert!(result.is_ok());
    }
}
