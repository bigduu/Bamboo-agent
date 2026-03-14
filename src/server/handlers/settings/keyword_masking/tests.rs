use crate::{core::keyword_masking::KeywordEntry, server::error::AppError};

use super::{
    constants::{MAX_ENTRIES, MAX_PATTERN_LENGTH},
    validation::{build_validated_config, validate_entries_only},
};

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
fn validate_entries_only_returns_structured_validation_errors() {
    let entries = vec![KeywordEntry::regex("[a-z+")];
    let errors = validate_entries_only(entries).expect_err("expected regex validation errors");

    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].index, 0);
    assert!(errors[0].message.contains("Invalid regex pattern"));
}
