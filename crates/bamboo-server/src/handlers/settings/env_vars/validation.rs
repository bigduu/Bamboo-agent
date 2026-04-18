use regex::Regex;
use std::sync::LazyLock;

use crate::error::AppError;

/// Valid POSIX environment variable name: starts with letter or underscore,
/// followed by letters, digits, or underscores.
static ENV_VAR_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap());

/// Maximum length for an env var name.
const MAX_NAME_LEN: usize = 256;

/// Maximum length for an env var value.
const MAX_VALUE_LEN: usize = 65_536;

/// Validate a single env var name.
pub fn validate_env_var_name(name: &str) -> Result<(), AppError> {
    if name.is_empty() {
        return Err(AppError::BadRequest(
            "Environment variable name cannot be empty".to_string(),
        ));
    }
    if name.len() > MAX_NAME_LEN {
        return Err(AppError::BadRequest(format!(
            "Environment variable name exceeds {} characters",
            MAX_NAME_LEN
        )));
    }
    if !ENV_VAR_NAME_RE.is_match(name) {
        return Err(AppError::BadRequest(format!(
            "Invalid environment variable name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            name
        )));
    }
    Ok(())
}

/// Validate a single env var value length.
pub fn validate_env_var_value(value: &str) -> Result<(), AppError> {
    if value.len() > MAX_VALUE_LEN {
        return Err(AppError::BadRequest(format!(
            "Environment variable value exceeds {} characters",
            MAX_VALUE_LEN
        )));
    }
    Ok(())
}

/// Check for duplicate names in a list of entries.
pub fn check_duplicate_names(names: &[&str]) -> Result<(), AppError> {
    let mut seen = std::collections::HashSet::new();
    for name in names {
        if !seen.insert(*name) {
            return Err(AppError::BadRequest(format!(
                "Duplicate environment variable name: '{}'",
                name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── validate_env_var_name ────────────────────────────────

    #[test]
    fn name_accepts_simple_identifiers() {
        assert!(validate_env_var_name("HOME").is_ok());
        assert!(validate_env_var_name("_PRIVATE").is_ok());
        assert!(validate_env_var_name("GITHUB_TOKEN").is_ok());
        assert!(validate_env_var_name("a").is_ok());
        assert!(validate_env_var_name("_").is_ok());
        assert!(validate_env_var_name("A1B2C3").is_ok());
    }

    #[test]
    fn name_rejects_empty() {
        let err = validate_env_var_name("").unwrap_err();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("cannot be empty")));
    }

    #[test]
    fn name_rejects_starting_with_digit() {
        assert!(validate_env_var_name("1ABC").is_err());
        assert!(validate_env_var_name("9_VAR").is_err());
    }

    #[test]
    fn name_rejects_special_characters() {
        assert!(validate_env_var_name("MY-VAR").is_err());
        assert!(validate_env_var_name("MY.VAR").is_err());
        assert!(validate_env_var_name("MY VAR").is_err());
        assert!(validate_env_var_name("MY@VAR").is_err());
        assert!(validate_env_var_name("MY=VAR").is_err());
        assert!(validate_env_var_name("MY$VAR").is_err());
        assert!(validate_env_var_name("path/var").is_err());
    }

    #[test]
    fn name_rejects_unicode() {
        assert!(validate_env_var_name("变量").is_err());
        assert!(validate_env_var_name("café").is_err());
    }

    #[test]
    fn name_rejects_too_long() {
        let long = "A".repeat(257);
        assert!(validate_env_var_name(&long).is_err());
    }

    #[test]
    fn name_accepts_max_length() {
        let exactly_256 = "A".repeat(256);
        assert!(validate_env_var_name(&exactly_256).is_ok());
    }

    // ── validate_env_var_value ────────────────────────────────

    #[test]
    fn value_accepts_normal_strings() {
        assert!(validate_env_var_value("hello").is_ok());
        assert!(validate_env_var_value("").is_ok()); // empty is fine
        assert!(validate_env_var_value("sk-proj-1234567890").is_ok());
    }

    #[test]
    fn value_accepts_max_length() {
        let val = "x".repeat(65_536);
        assert!(validate_env_var_value(&val).is_ok());
    }

    #[test]
    fn value_rejects_too_long() {
        let val = "x".repeat(65_537);
        assert!(validate_env_var_value(&val).is_err());
    }

    // ── check_duplicate_names ────────────────────────────────

    #[test]
    fn duplicates_allows_unique_names() {
        assert!(check_duplicate_names(&["A", "B", "C"]).is_ok());
    }

    #[test]
    fn duplicates_detects_same_name() {
        let err = check_duplicate_names(&["TOKEN", "SECRET", "TOKEN"]).unwrap_err();
        assert!(matches!(err, AppError::BadRequest(msg) if msg.contains("TOKEN")));
    }

    #[test]
    fn duplicates_allows_empty_list() {
        assert!(check_duplicate_names(&[]).is_ok());
    }

    #[test]
    fn duplicates_single_entry() {
        assert!(check_duplicate_names(&["ONLY_ONE"]).is_ok());
    }

    #[test]
    fn duplicates_case_sensitive() {
        // "token" and "TOKEN" are different names
        assert!(check_duplicate_names(&["token", "TOKEN"]).is_ok());
    }
}
