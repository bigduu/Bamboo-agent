use bamboo_domain::poison::PoisonRecover;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// Match type for keyword masking
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum MatchType {
    /// Exact string match (substring search)
    #[default]
    Exact,
    /// Regex pattern match
    Regex,
}

/// A single keyword masking entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeywordEntry {
    /// The pattern to match (string for exact, regex pattern for regex)
    pub pattern: String,
    /// Type of matching: exact or regex
    #[serde(default)]
    pub match_type: MatchType,
    /// Whether this entry is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

impl KeywordEntry {
    /// Create a new exact match keyword entry
    pub fn exact(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            match_type: MatchType::Exact,
            enabled: true,
        }
    }

    /// Create a new regex match keyword entry
    pub fn regex(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            match_type: MatchType::Regex,
            enabled: true,
        }
    }

    /// Validate the regex pattern if this is a regex entry
    pub fn validate(&self) -> Result<(), String> {
        if self.match_type == MatchType::Regex {
            regex::Regex::new(&self.pattern)
                .map_err(|e| format!("Invalid regex pattern '{}': {}", self.pattern, e))?;
        }
        Ok(())
    }
}

/// Configuration for global keyword masking
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KeywordMaskingConfig {
    /// List of keyword masking entries
    #[serde(default)]
    pub entries: Vec<KeywordEntry>,
}

/// Process-wide cache of compiled keyword-masking regexes, keyed by pattern.
///
/// `Some(re)` = a pattern compiled exactly once and reused on every call;
/// `None` = a pattern that failed to compile (cached so the failing compile is
/// never retried and the entry is skipped, matching the previous per-call
/// `Regex::new(...).ok()` behavior). This sits on the outbound request hot path
/// — [`KeywordMaskingConfig::apply_masking`] is invoked once per text value of
/// every serialized request body — so patterns are compiled once, not per call.
static REGEX_CACHE: OnceLock<RwLock<HashMap<String, Option<Regex>>>> = OnceLock::new();

fn regex_cache() -> &'static RwLock<HashMap<String, Option<Regex>>> {
    REGEX_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

impl KeywordMaskingConfig {
    /// Create a new empty config
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new keyword entry
    pub fn add_entry(&mut self, entry: KeywordEntry) {
        self.entries.push(entry);
    }

    /// Validate all regex entries
    pub fn validate(&self) -> Result<(), Vec<(usize, String)>> {
        let mut errors = Vec::new();
        for (idx, entry) in self.entries.iter().enumerate() {
            if let Err(e) = entry.validate() {
                errors.push((idx, e));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Apply masking to text.
    ///
    /// Regex patterns are compiled once (process-wide, see [`REGEX_CACHE`]) and
    /// reused across calls instead of being recompiled on every invocation. Invalid
    /// regex patterns are handled exactly as before: silently skipped (cached as
    /// `None` so the failing compile is never retried), so such an entry applies no
    /// masking rather than panicking.
    pub fn apply_masking(&self, text: &str) -> String {
        let mut result = text.to_string();

        if self.entries.is_empty() {
            return result;
        }

        let cache = regex_cache();

        // Ensure every enabled regex pattern is compiled exactly once and cached.
        // Collect misses under a shared read lock; only take the exclusive write
        // lock when there is something new to compile, so the steady-state hot path
        // never blocks other threads.
        {
            let missing: Vec<&str> = {
                let read = cache.read().recover_poison();
                self.entries
                    .iter()
                    .filter(|entry| {
                        entry.enabled
                            && entry.match_type == MatchType::Regex
                            && !read.contains_key(&entry.pattern)
                    })
                    .map(|entry| entry.pattern.as_str())
                    .collect()
            };
            if !missing.is_empty() {
                let mut write = cache.write().recover_poison();
                for pattern in missing {
                    write
                        .entry(pattern.to_string())
                        .or_insert_with(|| Regex::new(pattern).ok());
                }
            }
        }

        // Apply masking, reusing the cached compiled regexes. Regex entries that
        // failed to compile are stored as `None` and fall through (no masking),
        // matching the previous per-call `if let Ok(regex) = ...` behavior.
        let read = cache.read().recover_poison();
        for entry in &self.entries {
            if !entry.enabled {
                continue;
            }

            match entry.match_type {
                MatchType::Exact => {
                    result = result.replace(&entry.pattern, "[MASKED]");
                }
                MatchType::Regex => {
                    if let Some(regex) = read.get(&entry.pattern).and_then(|opt| opt.as_ref()) {
                        result = regex.replace_all(&result, "[MASKED]").to_string();
                    }
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_masking() {
        let config = KeywordMaskingConfig {
            entries: vec![KeywordEntry::exact("secret-token")],
        };

        let result = config.apply_masking("This has secret-token in it");
        assert_eq!(result, "This has [MASKED] in it");
    }

    #[test]
    fn test_regex_masking() {
        let config = KeywordMaskingConfig {
            entries: vec![KeywordEntry::regex(r"sk-[A-Za-z0-9]+")],
        };

        let result = config.apply_masking("API key: sk-abc123xyz");
        assert_eq!(result, "API key: [MASKED]");
    }

    #[test]
    fn test_disabled_entry_not_applied() {
        let config = KeywordMaskingConfig {
            entries: vec![KeywordEntry {
                pattern: "secret".to_string(),
                match_type: MatchType::Exact,
                enabled: false,
            }],
        };

        let result = config.apply_masking("This has secret in it");
        assert_eq!(result, "This has secret in it");
    }

    #[test]
    fn test_multiple_entries() {
        let config = KeywordMaskingConfig {
            entries: vec![KeywordEntry::exact("foo"), KeywordEntry::exact("bar")],
        };

        let result = config.apply_masking("foo and bar");
        assert_eq!(result, "[MASKED] and [MASKED]");
    }

    #[test]
    fn test_validate_regex() {
        let entry = KeywordEntry::regex(r"[a-z+");
        assert!(entry.validate().is_err());

        let entry = KeywordEntry::regex(r"[a-z]+");
        assert!(entry.validate().is_ok());
    }

    #[test]
    fn test_validate_config() {
        let config = KeywordMaskingConfig {
            entries: vec![
                KeywordEntry::regex(r"[a-z+"),  // invalid
                KeywordEntry::regex(r"[a-z]+"), // valid
            ],
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, 0); // First entry has error
    }

    /// An invalid user-supplied regex must be silently skipped (no panic, no
    /// masking applied by it) — exactly as the pre-caching per-call behavior did —
    /// while surrounding valid entries (exact and regex) still mask normally.
    #[test]
    fn test_invalid_regex_pattern_skipped_but_valid_entries_still_mask() {
        let config = KeywordMaskingConfig {
            entries: vec![
                KeywordEntry::exact("literal-secret"),
                KeywordEntry::regex(r"[a-z+"), // invalid regex
                KeywordEntry::regex(r"sk-[A-Za-z0-9]+"),
            ],
        };

        let result =
            config.apply_masking("literal-secret and sk-abc123 plus [a-z+ garbage and more text");
        // Invalid regex applies no masking; valid exact + regex entries do.
        assert_eq!(
            result,
            "[MASKED] and [MASKED] plus [a-z+ garbage and more text"
        );
    }

    /// Masking output must be identical across repeated calls — this guards the
    /// compiled-regex cache: a second call reuses the cached regex and must not
    /// diverge from the first.
    #[test]
    fn test_apply_masking_is_stable_across_repeated_calls() {
        let config = KeywordMaskingConfig {
            entries: vec![
                KeywordEntry::regex(r"\d{3}-\d{4}"),
                KeywordEntry::exact("secret"),
            ],
        };
        let input = "call secret at 555-1234 or 999-0000";

        let first = config.apply_masking(input);
        let second = config.apply_masking(input);
        let third = config.apply_masking(&format!("again {input}"));

        assert_eq!(
            first, second,
            "repeated calls must produce identical output"
        );
        assert_eq!(
            first, "call [MASKED] at [MASKED] or [MASKED]",
            "sanity-check expected masking"
        );
        assert_eq!(third, "again call [MASKED] at [MASKED] or [MASKED]");
    }
}
