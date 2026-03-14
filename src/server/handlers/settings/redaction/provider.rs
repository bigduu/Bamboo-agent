use crate::core::Config;
use serde_json::Value;

use super::constants::masked_secret_value;

pub(super) fn redact_provider_entry(name: &str, provider_cfg: &mut Value, config: &Config) {
    let Some(provider_obj) = provider_cfg.as_object_mut() else {
        return;
    };

    provider_obj.remove("api_key_encrypted");

    if provider_is_configured(name, config) {
        provider_obj.insert("api_key".to_string(), masked_secret_value());
    } else {
        provider_obj.remove("api_key");
    }
}

fn provider_is_configured(name: &str, config: &Config) -> bool {
    match name {
        "openai" => config
            .providers
            .openai
            .as_ref()
            .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
            .unwrap_or(false),
        "anthropic" => config
            .providers
            .anthropic
            .as_ref()
            .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
            .unwrap_or(false),
        "gemini" => config
            .providers
            .gemini
            .as_ref()
            .map(|c| !c.api_key.trim().is_empty() || c.api_key_encrypted.is_some())
            .unwrap_or(false),
        _ => false,
    }
}
