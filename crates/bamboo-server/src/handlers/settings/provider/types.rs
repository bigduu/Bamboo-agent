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
    /// Feature flags from server config.
    pub(super) features: bamboo_infrastructure::FeatureFlags,
}

/// Request body for updating provider configuration.
#[derive(Deserialize)]
pub struct UpdateProviderRequest {
    /// Provider to activate.
    pub provider: String,
    /// Provider-specific configurations.
    #[serde(default)]
    pub providers: Value,
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
            features: Default::default(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("gpt-4"));
    }

    #[test]
    fn test_update_provider_request_deserialization() {
        let json = r#"{"provider":"anthropic","providers":{"anthropic":{"api_key":"test-key"}}}"#;
        let req: UpdateProviderRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.provider, "anthropic");
        assert!(req.providers["anthropic"]["api_key"].is_string());
    }

    #[test]
    fn test_update_provider_request_minimal() {
        let json = r#"{"provider":"openai"}"#;
        let req: UpdateProviderRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.provider, "openai");
        // When providers field is missing with #[serde(default)], it becomes Null
        assert!(req.providers.is_null());
    }

    #[test]
    fn test_update_provider_request_with_empty_providers() {
        let json = r#"{"provider":"gemini","providers":{}}"#;
        let req: UpdateProviderRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.provider, "gemini");
        assert!(req.providers.is_object());
    }
}
