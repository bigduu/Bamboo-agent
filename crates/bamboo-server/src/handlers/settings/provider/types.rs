use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Response for provider configuration.
#[derive(Serialize)]
pub(super) struct ProviderConfigResponse {
    /// Currently active provider.
    pub(super) provider: String,
    /// List of available provider types.
    pub(super) available_providers: Vec<String>,
    /// Provider-specific configurations (API keys masked).
    pub(super) providers: Value,
    /// Default model assignments for specific capabilities.
    pub(super) defaults: Option<bamboo_infrastructure::DefaultsConfig>,
    /// Feature flags from server config.
    pub(super) features: bamboo_infrastructure::FeatureFlags,
}

#[derive(Deserialize, Default)]
pub struct UpdateFeatureFlagsRequest {
    #[serde(default)]
    pub provider_model_ref: Option<bool>,
    #[serde(default)]
    pub dynamic_model_routing: Option<bool>,
}

/// Request body for updating provider configuration.
#[derive(Deserialize)]
pub struct UpdateProviderRequest {
    /// Provider to activate.
    pub provider: String,
    /// Provider-specific configurations.
    #[serde(default)]
    pub providers: Value,
    /// Default model assignments for specific capabilities.
    #[serde(default)]
    pub defaults: Option<bamboo_infrastructure::DefaultsConfig>,
    /// Optional feature-flag patch to merge into config.features.
    #[serde(default)]
    pub features: UpdateFeatureFlagsRequest,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_config_response_serialization() {
        let response = ProviderConfigResponse {
            provider: "anthropic".to_string(),
            available_providers: vec!["anthropic".to_string(), "openai".to_string()],
            providers: serde_json::json!({"anthropic": {"api_key": "***"}}),
            defaults: None,
            features: Default::default(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("anthropic"));
        assert!(json.contains("openai"));
    }

    #[test]
    fn test_provider_config_response_with_providers() {
        let response = ProviderConfigResponse {
            provider: "openai".to_string(),
            available_providers: vec!["openai".to_string()],
            providers: serde_json::json!({"openai": {"model": "gpt-4"}}),
            defaults: None,
            features: Default::default(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("gpt-4"));
    }

    #[test]
    fn test_provider_config_response_with_defaults() {
        let defaults = bamboo_infrastructure::DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef {
                provider: "openai".to_string(),
                model: "gpt-4o".to_string(),
            },
            fast: None,
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: std::collections::HashMap::new(),
        };
        let response = ProviderConfigResponse {
            provider: "openai".to_string(),
            available_providers: vec!["openai".to_string()],
            providers: serde_json::json!({"openai": {"model": "gpt-4"}}),
            defaults: Some(defaults),
            features: Default::default(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("gpt-4o"));
        assert!(json.contains("defaults"));
    }

    #[test]
    fn test_update_provider_request_deserialization() {
        let json = r#"{"provider":"anthropic","providers":{"anthropic":{"api_key":"test-key"}}}"#;
        let req: UpdateProviderRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.provider, "anthropic");
        assert!(req.providers["anthropic"]["api_key"].is_string());
        assert!(req.defaults.is_none());
        assert!(req.features.provider_model_ref.is_none());
    }

    #[test]
    fn test_update_provider_request_minimal() {
        let json = r#"{"provider":"openai"}"#;
        let req: UpdateProviderRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.provider, "openai");
        // When providers field is missing with #[serde(default)], it becomes Null
        assert!(req.providers.is_null());
        assert!(req.defaults.is_none());
    }

    #[test]
    fn test_update_provider_request_with_empty_providers() {
        let json = r#"{"provider":"gemini","providers":{}}"#;
        let req: UpdateProviderRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.provider, "gemini");
        assert!(req.providers.is_object());
        assert!(req.defaults.is_none());
    }

    #[test]
    fn test_update_provider_request_with_defaults() {
        let json = r#"{
            "provider":"copilot",
            "providers":{"copilot":{"model":"gpt-5.5"}},
            "defaults":{"chat":{"provider":"copilot","model":"gpt-5.5"}}
        }"#;
        let req: UpdateProviderRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.provider, "copilot");
        assert_eq!(
            req.defaults.as_ref().map(|defaults| &defaults.chat.model),
            Some(&"gpt-5.5".to_string())
        );
    }

    #[test]
    fn test_update_provider_request_with_features_patch() {
        let json = r#"{
            "provider":"openai",
            "features":{"provider_model_ref":true}
        }"#;
        let req: UpdateProviderRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.provider, "openai");
        assert_eq!(req.features.provider_model_ref, Some(true));
        assert_eq!(req.features.dynamic_model_routing, None);
    }
}
