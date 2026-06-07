use crate::error::AppError;
use bamboo_config::keyword_masking::KeywordEntry;

use super::{
    constants::{MAX_ENTRIES, MAX_PATTERN_LENGTH},
    validation::{build_validated_config, validate_entries_only},
};

#[test]
fn build_validated_config_accepts_valid_entries() {
    let entries = vec![
        KeywordEntry::exact("secret".to_string()),
        KeywordEntry::regex("[0-9]+".to_string()),
    ];

    let config = build_validated_config(entries).expect("valid config should build");
    assert_eq!(config.entries.len(), 2);
}

#[test]
fn build_validated_config_accepts_empty_entries_list() {
    let entries = vec![];
    let config = build_validated_config(entries).expect("empty entries should be valid");
    assert_eq!(config.entries.len(), 0);
}

#[test]
fn build_validated_config_accepts_max_entries() {
    let entries: Vec<KeywordEntry> = (0..MAX_ENTRIES)
        .map(|idx| KeywordEntry::exact(format!("keyword-{idx}")))
        .collect();

    let config = build_validated_config(entries).expect("max entries should be valid");
    assert_eq!(config.entries.len(), MAX_ENTRIES);
}

#[test]
fn build_validated_config_rejects_too_many_entries() {
    let entries = (0..=MAX_ENTRIES)
        .map(|idx| KeywordEntry::exact(format!("keyword-{idx}")))
        .collect();

    let error = build_validated_config(entries).expect_err("expected too-many-entries error");
    match error {
        AppError::BadRequest(message) => {
            assert!(message.contains("Too many entries"));
            assert!(message.contains(&MAX_ENTRIES.to_string()));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn build_validated_config_accepts_max_pattern_length() {
    let pattern = "x".repeat(MAX_PATTERN_LENGTH);
    let entries = vec![KeywordEntry::exact(pattern)];

    let config = build_validated_config(entries).expect("max length pattern should be valid");
    assert_eq!(config.entries[0].pattern.len(), MAX_PATTERN_LENGTH);
}

#[test]
fn build_validated_config_rejects_oversized_pattern() {
    let entries = vec![KeywordEntry::exact("x".repeat(MAX_PATTERN_LENGTH + 1))];

    let error = build_validated_config(entries).expect_err("expected oversized-pattern error");
    match error {
        AppError::BadRequest(message) => {
            assert!(message.contains("Pattern at index 0 too long"));
            assert!(message.contains(&MAX_PATTERN_LENGTH.to_string()));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn build_validated_config_rejects_oversized_pattern_at_different_indices() {
    let mut entries: Vec<KeywordEntry> = (0..5)
        .map(|idx| KeywordEntry::exact(format!("valid-{idx}")))
        .collect();
    entries.push(KeywordEntry::exact("x".repeat(MAX_PATTERN_LENGTH + 1)));

    let error = build_validated_config(entries).expect_err("expected error at index 5");
    match error {
        AppError::BadRequest(message) => {
            assert!(message.contains("Pattern at index 5 too long"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn build_validated_config_rejects_invalid_regex() {
    let entries = vec![KeywordEntry::regex("[a-z+".to_string())];

    let error = build_validated_config(entries).expect_err("expected invalid regex error");
    match error {
        AppError::BadRequest(message) => {
            assert!(message.contains("Validation failed"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn build_validated_config_accepts_valid_regex_patterns() {
    let entries = vec![
        KeywordEntry::regex("[0-9]+".to_string()),
        KeywordEntry::regex("(foo|bar)".to_string()),
        KeywordEntry::regex("test.*".to_string()),
    ];

    let config = build_validated_config(entries).expect("valid regex patterns should work");
    assert_eq!(config.entries.len(), 3);
}

#[test]
fn build_validated_config_handles_unicode_patterns() {
    let entries = vec![
        KeywordEntry::exact("密码".to_string()),
        KeywordEntry::exact("пароль".to_string()),
        KeywordEntry::exact("🔑".to_string()),
    ];

    let config = build_validated_config(entries).expect("unicode patterns should be valid");
    assert_eq!(config.entries.len(), 3);
}

#[test]
fn build_validated_config_accepts_empty_pattern() {
    let entries = vec![KeywordEntry::exact("".to_string())];

    // Empty pattern is allowed (though might not match anything useful)
    let config = build_validated_config(entries).expect("empty pattern should be valid");
    assert_eq!(config.entries[0].pattern, "");
}

#[test]
fn build_validated_config_accepts_whitespace_pattern() {
    let entries = vec![
        KeywordEntry::exact("   ".to_string()),
        KeywordEntry::exact("\t\n".to_string()),
    ];

    let config = build_validated_config(entries).expect("whitespace patterns should be valid");
    assert_eq!(config.entries.len(), 2);
}

#[test]
fn validate_entries_only_returns_structured_validation_errors() {
    let entries = vec![KeywordEntry::regex("[a-z+".to_string())];
    let errors = validate_entries_only(entries).expect_err("expected regex validation errors");

    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].index, 0);
    assert!(errors[0].message.contains("Invalid regex pattern"));
}

#[test]
fn validate_entries_only_returns_multiple_errors() {
    let entries = vec![
        KeywordEntry::regex("[a-z+".to_string()),
        KeywordEntry::exact("valid".to_string()),
        KeywordEntry::regex("(unclosed".to_string()),
    ];

    let errors = validate_entries_only(entries).expect_err("expected multiple validation errors");
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0].index, 0);
    assert_eq!(errors[1].index, 2);
}

#[test]
fn validate_entries_only_accepts_valid_entries() {
    let entries = vec![
        KeywordEntry::exact("password".to_string()),
        KeywordEntry::regex("[0-9]{4}".to_string()),
    ];

    let result = validate_entries_only(entries);
    assert!(result.is_ok());
}

#[test]
fn validate_entries_only_accepts_empty_entries() {
    let entries = vec![];
    let result = validate_entries_only(entries);
    assert!(result.is_ok());
}

#[test]
fn build_validated_config_handles_mixed_valid_and_invalid() {
    let entries = vec![
        KeywordEntry::exact("valid".to_string()),
        KeywordEntry::regex("[invalid".to_string()),
    ];

    let error = build_validated_config(entries).expect_err("expected validation error");
    match error {
        AppError::BadRequest(message) => {
            assert!(message.contains("Validation failed"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn build_validated_config_rejects_both_limit_and_validation_errors() {
    // When both limit errors and validation errors exist, limit is checked first
    let mut entries: Vec<KeywordEntry> = (0..=MAX_ENTRIES)
        .map(|idx| KeywordEntry::exact(format!("keyword-{idx}")))
        .collect();
    entries.push(KeywordEntry::regex("[invalid".to_string()));

    let error = build_validated_config(entries).expect_err("expected error");
    match error {
        AppError::BadRequest(message) => {
            // Limit check happens before validation
            assert!(message.contains("Too many entries"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn build_validated_config_accepts_special_characters() {
    let entries = vec![
        KeywordEntry::exact("test@example.com".to_string()),
        KeywordEntry::exact("C:\\Users\\test".to_string()),
        KeywordEntry::exact("$HOME/.bashrc".to_string()),
        KeywordEntry::exact("foo|bar".to_string()),
    ];

    let config = build_validated_config(entries).expect("special chars should be valid");
    assert_eq!(config.entries.len(), 4);
}

#[test]
fn build_validated_config_handles_very_long_patterns() {
    let long_pattern = "x".repeat(MAX_PATTERN_LENGTH - 1);
    let entries = vec![KeywordEntry::exact(long_pattern.clone())];

    let config = build_validated_config(entries).expect("long pattern should be valid");
    assert_eq!(config.entries[0].pattern.len(), MAX_PATTERN_LENGTH - 1);
}
