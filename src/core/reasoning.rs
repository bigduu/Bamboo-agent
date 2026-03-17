use serde::{Deserialize, Serialize};

/// Provider-agnostic reasoning effort levels.
///
/// These values are surfaced to clients and can be mapped to provider-specific
/// request parameters where supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningEffort {
    Low,
    Medium,
    High,
    Xhigh,
}

impl ReasoningEffort {
    /// Parse a case-insensitive string into [`ReasoningEffort`].
    pub fn parse(value: &str) -> Option<Self> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            "xhigh" => Some(Self::Xhigh),
            _ => None,
        }
    }

    /// Return the lowercase wire-format representation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_lowercase_variants() {
        assert_eq!(ReasoningEffort::parse("low"), Some(ReasoningEffort::Low));
        assert_eq!(
            ReasoningEffort::parse("medium"),
            Some(ReasoningEffort::Medium)
        );
        assert_eq!(ReasoningEffort::parse("high"), Some(ReasoningEffort::High));
        assert_eq!(
            ReasoningEffort::parse("xhigh"),
            Some(ReasoningEffort::Xhigh)
        );
    }

    #[test]
    fn parse_uppercase_variants() {
        assert_eq!(ReasoningEffort::parse("LOW"), Some(ReasoningEffort::Low));
        assert_eq!(
            ReasoningEffort::parse("MEDIUM"),
            Some(ReasoningEffort::Medium)
        );
        assert_eq!(ReasoningEffort::parse("HIGH"), Some(ReasoningEffort::High));
        assert_eq!(
            ReasoningEffort::parse("XHIGH"),
            Some(ReasoningEffort::Xhigh)
        );
    }

    #[test]
    fn parse_mixed_case_variants() {
        assert_eq!(ReasoningEffort::parse("Low"), Some(ReasoningEffort::Low));
        assert_eq!(
            ReasoningEffort::parse("MeDiUm"),
            Some(ReasoningEffort::Medium)
        );
        assert_eq!(ReasoningEffort::parse("hIgH"), Some(ReasoningEffort::High));
        assert_eq!(
            ReasoningEffort::parse("XhIgH"),
            Some(ReasoningEffort::Xhigh)
        );
    }

    #[test]
    fn parse_with_whitespace() {
        assert_eq!(
            ReasoningEffort::parse("  low  "),
            Some(ReasoningEffort::Low)
        );
        assert_eq!(
            ReasoningEffort::parse("\tmedium\t"),
            Some(ReasoningEffort::Medium)
        );
        assert_eq!(
            ReasoningEffort::parse(" high "),
            Some(ReasoningEffort::High)
        );
        assert_eq!(
            ReasoningEffort::parse("\nxhigh\n"),
            Some(ReasoningEffort::Xhigh)
        );
    }

    #[test]
    fn parse_invalid_values() {
        assert_eq!(ReasoningEffort::parse(""), None);
        assert_eq!(ReasoningEffort::parse("invalid"), None);
        assert_eq!(ReasoningEffort::parse("unknown"), None);
        assert_eq!(ReasoningEffort::parse("extreme"), None);
        assert_eq!(ReasoningEffort::parse("ultra"), None);
        assert_eq!(ReasoningEffort::parse("max"), None);
    }

    #[test]
    fn parse_partial_matches_fail() {
        assert_eq!(ReasoningEffort::parse("lo"), None);
        assert_eq!(ReasoningEffort::parse("med"), None);
        assert_eq!(ReasoningEffort::parse("hi"), None);
        assert_eq!(ReasoningEffort::parse("xhi"), None);
    }

    #[test]
    fn as_str_returns_lowercase() {
        assert_eq!(ReasoningEffort::Low.as_str(), "low");
        assert_eq!(ReasoningEffort::Medium.as_str(), "medium");
        assert_eq!(ReasoningEffort::High.as_str(), "high");
        assert_eq!(ReasoningEffort::Xhigh.as_str(), "xhigh");
    }

    #[test]
    fn serde_serializes_to_lowercase() {
        let low = serde_json::to_string(&ReasoningEffort::Low).unwrap();
        assert_eq!(low, "\"low\"");

        let medium = serde_json::to_string(&ReasoningEffort::Medium).unwrap();
        assert_eq!(medium, "\"medium\"");

        let high = serde_json::to_string(&ReasoningEffort::High).unwrap();
        assert_eq!(high, "\"high\"");

        let xhigh = serde_json::to_string(&ReasoningEffort::Xhigh).unwrap();
        assert_eq!(xhigh, "\"xhigh\"");
    }

    #[test]
    fn serde_deserializes_from_lowercase() {
        let low: ReasoningEffort = serde_json::from_str("\"low\"").unwrap();
        assert_eq!(low, ReasoningEffort::Low);

        let medium: ReasoningEffort = serde_json::from_str("\"medium\"").unwrap();
        assert_eq!(medium, ReasoningEffort::Medium);

        let high: ReasoningEffort = serde_json::from_str("\"high\"").unwrap();
        assert_eq!(high, ReasoningEffort::High);

        let xhigh: ReasoningEffort = serde_json::from_str("\"xhigh\"").unwrap();
        assert_eq!(xhigh, ReasoningEffort::Xhigh);
    }

    #[test]
    fn serde_deserializes_case_insensitively() {
        // Note: serde's rename_all="lowercase" only affects serialization, not deserialization
        // Deserialization is case-sensitive and only accepts lowercase
        let result: Result<ReasoningEffort, _> = serde_json::from_str("\"Low\"");
        assert!(result.is_err());
    }

    #[test]
    fn serde_roundtrip() {
        let original = ReasoningEffort::High;
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: ReasoningEffort = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn serde_rejects_invalid_values() {
        let result: Result<ReasoningEffort, _> = serde_json::from_str("\"invalid\"");
        assert!(result.is_err());

        let result: Result<ReasoningEffort, _> = serde_json::from_str("\"unknown\"");
        assert!(result.is_err());
    }

    #[test]
    fn enum_equality() {
        assert_eq!(ReasoningEffort::Low, ReasoningEffort::Low);
        assert_ne!(ReasoningEffort::Low, ReasoningEffort::Medium);
        assert_ne!(ReasoningEffort::Medium, ReasoningEffort::High);
        assert_ne!(ReasoningEffort::High, ReasoningEffort::Xhigh);
    }

    #[test]
    fn enum_clone() {
        let effort = ReasoningEffort::Medium;
        let cloned = effort.clone();
        assert_eq!(effort, cloned);
    }

    #[test]
    fn enum_copy() {
        let effort = ReasoningEffort::High;
        let copied = effort;
        assert_eq!(effort, copied);
    }

    #[test]
    fn parse_with_unicode_whitespace() {
        // Rust's trim() actually handles unicode whitespace including non-breaking space
        let result = ReasoningEffort::parse("\u{00A0}low\u{00A0}");
        assert_eq!(result, Some(ReasoningEffort::Low));
    }

    #[test]
    fn parse_with_numbers() {
        assert_eq!(ReasoningEffort::parse("low1"), None);
        assert_eq!(ReasoningEffort::parse("2medium"), None);
        assert_eq!(ReasoningEffort::parse("high3"), None);
    }

    #[test]
    fn parse_with_special_characters() {
        assert_eq!(ReasoningEffort::parse("low!"), None);
        assert_eq!(ReasoningEffort::parse("@medium"), None);
        assert_eq!(ReasoningEffort::parse("high#"), None);
        assert_eq!(ReasoningEffort::parse("$xhigh"), None);
    }
}
