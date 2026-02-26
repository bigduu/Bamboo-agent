use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Anthropic model mapping configuration.
///
/// Used to map OpenAI-compatible model ids (e.g. "claude-3-opus") to the actual
/// upstream Anthropic model id that should be used by the provider.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnthropicModelMapping {
    #[serde(default)]
    pub mappings: HashMap<String, String>,
}

/// Gemini model mapping configuration.
///
/// Used to map OpenAI-compatible model ids (e.g. "gemini-pro") to the actual
/// upstream Gemini model id that should be used by the provider.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GeminiModelMapping {
    #[serde(default)]
    pub mappings: HashMap<String, String>,
}
