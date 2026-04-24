use serde::{Deserialize, Serialize};

/// Summary info about a configured provider.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderDescriptor {
    pub id: String,
    pub display_name: String,
    pub enabled: bool,
    pub authenticated: bool,
}

/// Capabilities declared for a specific model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelCapabilities {
    #[serde(default)]
    pub supports_tools: bool,
    #[serde(default)]
    pub supports_vision: bool,
    #[serde(default)]
    pub supports_reasoning: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_streaming: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_context_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
}

impl Default for ModelCapabilities {
    fn default() -> Self {
        Self {
            supports_tools: true,
            supports_vision: false,
            supports_reasoning: false,
            supports_streaming: None,
            max_context_tokens: None,
            max_output_tokens: None,
        }
    }
}

/// Where a model entry came from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelSource {
    Upstream,
    Static,
    Manual,
}

/// A single model entry inside the aggregated catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderModelDescriptor {
    pub reference: crate::ProviderModelRef,
    pub display_name: String,
    pub provider_display_name: String,
    #[serde(default)]
    pub capabilities: ModelCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<ModelSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovered_at: Option<String>,
}

/// Aggregated view of all providers and their models.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderCatalog {
    pub providers: Vec<ProviderDescriptor>,
    pub models: Vec<ProviderModelDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_descriptor_serde() {
        let d = ProviderDescriptor {
            id: "openai".into(),
            display_name: "OpenAI".into(),
            enabled: true,
            authenticated: true,
        };
        let json = serde_json::to_string(&d).unwrap();
        let back: ProviderDescriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_model_capabilities_default() {
        let c = ModelCapabilities::default();
        assert!(c.supports_tools);
        assert!(!c.supports_vision);
        assert!(!c.supports_reasoning);
    }

    #[test]
    fn test_provider_model_descriptor_serde() {
        let d = ProviderModelDescriptor {
            reference: crate::ProviderModelRef::new("openai", "gpt-4.1"),
            display_name: "GPT-4.1".into(),
            provider_display_name: "OpenAI".into(),
            capabilities: ModelCapabilities {
                supports_tools: true,
                supports_vision: true,
                supports_reasoning: true,
                ..Default::default()
            },
            source: Some(ModelSource::Upstream),
            discovered_at: None,
        };
        let json = serde_json::to_string(&d).unwrap();
        let back: ProviderModelDescriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn test_provider_catalog_serde() {
        let catalog = ProviderCatalog {
            providers: vec![ProviderDescriptor {
                id: "openai".into(),
                display_name: "OpenAI".into(),
                enabled: true,
                authenticated: true,
            }],
            models: vec![],
            updated_at: Some("2026-04-20T00:00:00Z".into()),
        };
        let json = serde_json::to_string(&catalog).unwrap();
        let back: ProviderCatalog = serde_json::from_str(&json).unwrap();
        assert_eq!(catalog, back);
    }
}
