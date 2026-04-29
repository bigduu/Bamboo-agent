use bamboo_domain::{ProviderFormat, ReasoningEffort};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::config::config::RequestOverridesConfig;

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderInstance {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub format: ProviderFormat,
    #[serde(default = "default_true")]
    pub enabled: bool,

    #[serde(default, skip_serializing)]
    pub api_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_encrypted: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,

    #[serde(default)]
    pub headless_auth: bool,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses_only_models: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_provider: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_overrides: Option<RequestOverridesConfig>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_models: Vec<String>,

    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deserialize_minimal() {
        let json = r#"{"id":"my-openai","format":"openai"}"#;
        let v: ProviderInstance = serde_json::from_str(json).unwrap();
        assert_eq!(v.id, "my-openai");
        assert_eq!(v.format, ProviderFormat::OpenAI);
        assert!(v.enabled);
        assert!(v.custom_models.is_empty());
    }
    #[test]
    fn deserialize_full() {
        let json = r#"{
            "id":"deepseek","display_name":"DeepSeek",
            "format":"openai","enabled":true,
            "base_url":"https://api.deepseek.com",
            "model":"deepseek-chat",
            "custom_models":["deepseek-coder","deepseek-reasoner"]
        }"#;
        let v: ProviderInstance = serde_json::from_str(json).unwrap();
        assert_eq!(v.id, "deepseek");
        assert_eq!(v.custom_models.len(), 2);
    }
}
