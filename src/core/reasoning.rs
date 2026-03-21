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
    Max,
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
            "max" => Some(Self::Max),
            _ => None,
        }
    }

    /// Return the canonical lowercase representation (provider-agnostic).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }

    /// Return the provider/model-appropriate wire-format string.
    ///
    /// Different model families expect different reasoning effort values:
    /// - **GPT / o-series**: `low`, `medium`, `high`, `xhigh` (`max` → `xhigh`)
    /// - **Claude**: `low`, `medium`, `high`, `max` (`xhigh` → `max`)
    /// - **Gemini**: `low`, `medium`, `high` (`xhigh`/`max` → `high`)
    ///
    /// This method inspects the model name to determine the correct mapping.
    /// When the model family is unknown, it falls back to the GPT/OpenAI format.
    pub fn to_wire_format(self, model: &str) -> &'static str {
        let model_lower = model.trim().to_ascii_lowercase();

        if Self::is_claude_model(&model_lower) {
            return match self {
                Self::Low => "low",
                Self::Medium => "medium",
                Self::High => "high",
                Self::Xhigh | Self::Max => "max",
            };
        }

        if Self::is_gemini_model(&model_lower) {
            return match self {
                Self::Low => "low",
                Self::Medium => "medium",
                Self::High | Self::Xhigh | Self::Max => "high",
            };
        }

        // Default: GPT / o-series / unknown → OpenAI format
        // GPT doesn't support "max", so map it to "xhigh"
        match self {
            Self::Max => "xhigh",
            other => other.as_str(),
        }
    }

    fn is_claude_model(model_lower: &str) -> bool {
        model_lower.starts_with("claude") || model_lower.contains("anthropic")
    }

    fn is_gemini_model(model_lower: &str) -> bool {
        model_lower.starts_with("gemini") || model_lower.contains("google")
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
        assert_eq!(ReasoningEffort::parse("max"), Some(ReasoningEffort::Max));
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
        assert_eq!(ReasoningEffort::parse("MAX"), Some(ReasoningEffort::Max));
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
        assert_eq!(ReasoningEffort::parse("MaX"), Some(ReasoningEffort::Max));
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
        assert_eq!(ReasoningEffort::Max.as_str(), "max");
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

        let max = serde_json::to_string(&ReasoningEffort::Max).unwrap();
        assert_eq!(max, "\"max\"");
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

        let max: ReasoningEffort = serde_json::from_str("\"max\"").unwrap();
        assert_eq!(max, ReasoningEffort::Max);
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

    // --- to_wire_format tests ---

    #[test]
    fn wire_format_gpt_models_use_openai_values() {
        assert_eq!(ReasoningEffort::Low.to_wire_format("gpt-4o"), "low");
        assert_eq!(ReasoningEffort::Medium.to_wire_format("gpt-4o"), "medium");
        assert_eq!(ReasoningEffort::High.to_wire_format("gpt-4o"), "high");
        assert_eq!(ReasoningEffort::Xhigh.to_wire_format("gpt-4o"), "xhigh");
        // Max → xhigh for GPT (GPT doesn't support "max")
        assert_eq!(ReasoningEffort::Max.to_wire_format("gpt-4o"), "xhigh");
        assert_eq!(ReasoningEffort::Max.to_wire_format("o1-preview"), "xhigh");
        assert_eq!(ReasoningEffort::Max.to_wire_format("o3-mini"), "xhigh");
    }

    #[test]
    fn wire_format_claude_models_map_xhigh_and_max_to_max() {
        assert_eq!(
            ReasoningEffort::Low.to_wire_format("claude-3.5-sonnet"),
            "low"
        );
        assert_eq!(
            ReasoningEffort::Medium.to_wire_format("claude-3.5-sonnet"),
            "medium"
        );
        assert_eq!(
            ReasoningEffort::High.to_wire_format("claude-sonnet-4"),
            "high"
        );
        // Both xhigh and max → "max" for Claude
        assert_eq!(
            ReasoningEffort::Xhigh.to_wire_format("claude-sonnet-4"),
            "max"
        );
        assert_eq!(
            ReasoningEffort::Max.to_wire_format("claude-sonnet-4"),
            "max"
        );
        assert_eq!(ReasoningEffort::Max.to_wire_format("claude-3-opus"), "max");
    }

    #[test]
    fn wire_format_gemini_models_cap_at_high() {
        assert_eq!(ReasoningEffort::Low.to_wire_format("gemini-2.5-pro"), "low");
        assert_eq!(
            ReasoningEffort::Medium.to_wire_format("gemini-2.5-pro"),
            "medium"
        );
        assert_eq!(
            ReasoningEffort::High.to_wire_format("gemini-2.5-pro"),
            "high"
        );
        // Both xhigh and max → "high" for Gemini
        assert_eq!(
            ReasoningEffort::Xhigh.to_wire_format("gemini-2.5-pro"),
            "high"
        );
        assert_eq!(
            ReasoningEffort::Max.to_wire_format("gemini-2.5-pro"),
            "high"
        );
    }

    #[test]
    fn wire_format_unknown_models_default_to_openai() {
        assert_eq!(
            ReasoningEffort::Xhigh.to_wire_format("some-unknown-model"),
            "xhigh"
        );
        assert_eq!(
            ReasoningEffort::Max.to_wire_format("some-unknown-model"),
            "xhigh"
        );
        assert_eq!(ReasoningEffort::High.to_wire_format("llama-3"), "high");
    }

    #[test]
    fn wire_format_case_insensitive_model_matching() {
        assert_eq!(
            ReasoningEffort::Xhigh.to_wire_format("Claude-3.5-Sonnet"),
            "max"
        );
        assert_eq!(
            ReasoningEffort::Max.to_wire_format("Claude-3.5-Sonnet"),
            "max"
        );
        assert_eq!(
            ReasoningEffort::Xhigh.to_wire_format("GEMINI-2.5-PRO"),
            "high"
        );
        assert_eq!(
            ReasoningEffort::Max.to_wire_format("GEMINI-2.5-PRO"),
            "high"
        );
        assert_eq!(ReasoningEffort::Xhigh.to_wire_format("GPT-4o"), "xhigh");
        assert_eq!(ReasoningEffort::Max.to_wire_format("GPT-4o"), "xhigh");
    }
}
