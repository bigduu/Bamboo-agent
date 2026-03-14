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
