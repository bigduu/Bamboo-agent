use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::ReasoningEffort;

/// Unified provider+model selection unit.
///
/// Every call site that needs to identify "which model on which provider" should
/// use this type instead of bare strings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ProviderModelRef {
    pub provider: String,
    pub model: String,
    /// Optional reasoning effort for this exact role/model.
    ///
    /// When omitted, callers preserve their existing task/provider defaults.
    /// Keeping this on the reference makes fallback resolution atomic:
    /// whichever model wins also contributes its own effort preference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
}

impl ProviderModelRef {
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            reasoning_effort: None,
        }
    }

    pub fn with_reasoning_effort(mut self, reasoning_effort: ReasoningEffort) -> Self {
        self.reasoning_effort = Some(reasoning_effort);
        self
    }

    pub fn to_pair(&self) -> (&str, &str) {
        (&self.provider, &self.model)
    }
}

impl fmt::Display for ProviderModelRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.provider, self.model)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseProviderModelRefError;

impl fmt::Display for ParseProviderModelRefError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "expected format 'provider/model'")
    }
}

impl std::error::Error for ParseProviderModelRefError {}

impl FromStr for ProviderModelRef {
    type Err = ParseProviderModelRefError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (provider, model) = s.split_once('/').ok_or(ParseProviderModelRefError)?;
        Ok(Self {
            provider: provider.to_string(),
            model: model.to_string(),
            reasoning_effort: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let r = ProviderModelRef::new("openai", "gpt-4.1");
        assert_eq!(r.provider, "openai");
        assert_eq!(r.model, "gpt-4.1");
    }

    #[test]
    fn test_to_pair() {
        let r = ProviderModelRef::new("anthropic", "claude-sonnet-4");
        let (p, m) = r.to_pair();
        assert_eq!(p, "anthropic");
        assert_eq!(m, "claude-sonnet-4");
    }

    #[test]
    fn test_display() {
        let r = ProviderModelRef::new("openai", "gpt-4.1");
        assert_eq!(format!("{r}"), "openai/gpt-4.1");
    }

    #[test]
    fn test_from_str() {
        let r: ProviderModelRef = "anthropic/claude-sonnet-4".parse().unwrap();
        assert_eq!(r.provider, "anthropic");
        assert_eq!(r.model, "claude-sonnet-4");
    }

    #[test]
    fn test_from_str_missing_slash() {
        let result: Result<ProviderModelRef, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_serde_roundtrip() {
        let r = ProviderModelRef::new("gemini", "gemini-2.5-pro");
        let json = serde_json::to_string(&r).unwrap();
        let back: ProviderModelRef = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn test_serde_json_shape() {
        let r = ProviderModelRef::new("openai", "gpt-4.1");
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"provider\""));
        assert!(json.contains("\"model\""));
        assert!(!json.contains("reasoning_effort"));
    }

    #[test]
    fn test_serde_roundtrip_with_reasoning_effort() {
        let r = ProviderModelRef::new("openai", "gpt-5.6-sol")
            .with_reasoning_effort(ReasoningEffort::High);
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"reasoning_effort\":\"high\""));
        let back: ProviderModelRef = serde_json::from_str(&json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn test_equality_and_hash() {
        let a = ProviderModelRef::new("openai", "gpt-4.1");
        let b = ProviderModelRef::new("openai", "gpt-4.1");
        let c = ProviderModelRef::new("anthropic", "claude-sonnet-4");
        assert_eq!(a, b);
        assert_ne!(a, c);
        use std::collections::HashSet;
        let set: HashSet<_> = [&a, &b, &c].into_iter().cloned().collect();
        assert_eq!(set.len(), 2);
    }
}
