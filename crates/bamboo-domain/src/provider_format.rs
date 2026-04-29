use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ProviderFormat {
    #[serde(rename = "openai")]
    OpenAI,
    #[serde(rename = "anthropic")]
    Anthropic,
    #[serde(rename = "gemini")]
    Gemini,
    #[serde(rename = "copilot")]
    Copilot,
    #[serde(rename = "bodhi")]
    Bodhi,
}

impl fmt::Display for ProviderFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ProviderFormat::OpenAI => "openai",
            ProviderFormat::Anthropic => "anthropic",
            ProviderFormat::Gemini => "gemini",
            ProviderFormat::Copilot => "copilot",
            ProviderFormat::Bodhi => "bodhi",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseProviderFormatError(pub String);

impl fmt::Display for ParseProviderFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown provider format: {}", self.0)
    }
}
impl std::error::Error for ParseProviderFormatError {}

impl FromStr for ProviderFormat {
    type Err = ParseProviderFormatError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "openai" => Ok(Self::OpenAI),
            "anthropic" => Ok(Self::Anthropic),
            "gemini" => Ok(Self::Gemini),
            "copilot" => Ok(Self::Copilot),
            "bodhi" => Ok(Self::Bodhi),
            other => Err(ParseProviderFormatError(other.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn display_roundtrip() {
        for f in [
            ProviderFormat::OpenAI,
            ProviderFormat::Anthropic,
            ProviderFormat::Gemini,
            ProviderFormat::Copilot,
            ProviderFormat::Bodhi,
        ] {
            assert_eq!(ProviderFormat::from_str(&f.to_string()).unwrap(), f);
        }
    }
    #[test]
    fn serde_snake_case() {
        let json = serde_json::to_string(&ProviderFormat::OpenAI).unwrap();
        assert_eq!(json, "\"openai\"");
        let parsed: ProviderFormat = serde_json::from_str("\"anthropic\"").unwrap();
        assert_eq!(parsed, ProviderFormat::Anthropic);
    }
    #[test]
    fn from_str_unknown() {
        assert!(ProviderFormat::from_str("xxx").is_err());
    }
}
