use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct KeywordMaskingConfig {
    /// List of keyword masking entries
    #[serde(default)]
    pub entries: Vec<KeywordEntry>,
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

    /// Apply masking to text
    pub fn apply_masking(&self, text: &str) -> String {
        let mut result = text.to_string();

        for entry in &self.entries {
            if !entry.enabled {
                continue;
            }

            match entry.match_type {
                MatchType::Exact => {
                    result = result.replace(&entry.pattern, "[MASKED]");
                }
                MatchType::Regex => {
                    if let Ok(regex) = regex::Regex::new(&entry.pattern) {
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
}
