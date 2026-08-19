use actix_web::{web, HttpResponse};
use serde_json::Value;

use crate::{
    app_state::AppState, error::AppError,
    handlers::settings::bamboo_config::scrub_unsafe_request_override_literals,
};
use bamboo_config::{Config, ProviderConfigs};
use bamboo_llm::AVAILABLE_PROVIDERS;

use super::super::types::ProviderConfigResponse;

pub(super) async fn handle_get_provider_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await.clone();
    let provider = legacy_active_provider_type(&config);
    let masked_providers = legacy_provider_response_view(&config)?;

    let response = ProviderConfigResponse {
        provider,
        available_providers: AVAILABLE_PROVIDERS
            .iter()
            .map(|value| value.to_string())
            .collect(),
        providers: masked_providers,
        defaults: config.defaults.clone(),
        features: config.features.clone(),
    };

    Ok(HttpResponse::Ok().json(response))
}

fn legacy_active_provider_type(config: &Config) -> String {
    let effective = config.effective_default_provider();
    config
        .provider_instances
        .get(effective)
        .map(|instance| instance.provider_type.clone())
        .unwrap_or_else(|| effective.to_string())
}

/// Build the deprecated type-keyed provider response from the instance-native
/// authority. This boundary intentionally owns its masking instead of using
/// the generic config redactor: the projected provider may be ref-backed even
/// though the durable legacy slot no longer exists, and a credential reference
/// is itself server-owned metadata that old clients must never receive.
fn legacy_provider_response_view(config: &Config) -> Result<Value, AppError> {
    let mut view = bamboo_config::legacy_provider_compatibility_view(config);
    let configured = legacy_provider_configured(&view);
    clear_legacy_provider_credentials(&mut view);
    let mut value = serde_json::to_value(view)?;
    scrub_unsafe_request_override_literals(&mut value);
    scrub_legacy_credential_metadata(&mut value);
    let Some(object) = value.as_object_mut() else {
        return Ok(value);
    };

    for (name, is_configured) in configured {
        let Some(provider) = object.get_mut(name).and_then(Value::as_object_mut) else {
            continue;
        };
        // `extra` is flattened, so remove these keys even after the typed
        // fields above have been cleared. Only the compatibility mask crosses
        // this endpoint.
        provider.remove("api_key");
        provider.remove("api_key_encrypted");
        provider.remove("credential_ref");
        provider.remove(bamboo_config::PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY);
        if is_configured {
            provider.insert(
                "api_key".to_string(),
                Value::String("****...****".to_string()),
            );
        }
    }

    Ok(value)
}

/// Unknown provider metadata is forward-compatible and therefore flattened at
/// several levels. Treat credential-shaped keys as secret at every depth so a
/// future provider cannot accidentally expose its credential authority through
/// this deprecated compatibility endpoint.
fn scrub_legacy_credential_metadata(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.retain(|key, _| {
                !matches!(
                    key.as_str(),
                    "api_key"
                        | "api_key_encrypted"
                        | "credential_ref"
                        | bamboo_config::PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY
                )
            });
            for value in object.values_mut() {
                scrub_legacy_credential_metadata(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                scrub_legacy_credential_metadata(value);
            }
        }
        _ => {}
    }
}

fn legacy_provider_configured(providers: &ProviderConfigs) -> Vec<(&'static str, bool)> {
    macro_rules! configured {
        ($provider:expr) => {
            $provider.as_ref().is_some_and(|provider| {
                // This endpoint reports the live compatibility view. A
                // durable ref or env binding without a successfully hydrated
                // runtime value must not be rendered as a configured mask.
                !provider.api_key.trim().is_empty()
            })
        };
    }

    vec![
        ("openai", configured!(providers.openai)),
        ("anthropic", configured!(providers.anthropic)),
        ("gemini", configured!(providers.gemini)),
        (
            "bodhi",
            providers
                .bodhi
                .as_ref()
                .is_some_and(|provider| !provider.api_key.trim().is_empty()),
        ),
    ]
}

fn clear_legacy_provider_credentials(providers: &mut ProviderConfigs) {
    macro_rules! clear {
        ($field:ident) => {
            if let Some(provider) = providers.$field.as_mut() {
                provider.api_key.clear();
                provider.api_key_encrypted = None;
                provider.credential_ref = None;
            }
        };
    }
    clear!(openai);
    clear!(anthropic);
    clear!(gemini);
    clear!(bodhi);
}

#[cfg(test)]
mod tests {
    use super::{legacy_active_provider_type, legacy_provider_response_view};
    use bamboo_config::{Config, CredentialRef, ProviderInstanceConfig};

    fn ref_backed_instance() -> ProviderInstanceConfig {
        let mut instance: ProviderInstanceConfig = serde_json::from_value(serde_json::json!({
            "provider_type": "openai",
            "label": "Work",
            "api_key_encrypted": "runtime-ciphertext",
            "credential_ref": "provider.work.api_key",
            "base_url": "https://work.example/v1",
            "model": "gpt-work",
            "enabled": true,
            "api_key_from_env": true,
            "request_overrides": {
                "common": {
                    "headers": {
                        "Authorization": "Bearer override-header-secret",
                        "X-Api-Key": {"type": "env_ref", "name": "PROJECTED_API_KEY"},
                        "X-Trace": "public-trace"
                    },
                    "body_patch": [
                        {"path": "/api_key", "value": "override-body-secret"},
                        {"path": "/api_key", "value": {"type": "env_ref", "name": "PROJECTED_API_KEY"}},
                        {"path": "/temperature", "value": 0.2}
                    ]
                }
            }
        }))
        .expect("valid instance");
        instance.api_key = "sk-runtime-plaintext".to_string();
        instance.credential_ref = Some(
            CredentialRef::parse("provider.work.api_key").expect("valid credential reference"),
        );
        instance
    }

    #[test]
    fn legacy_get_projection_uses_default_instance_type_and_leaks_no_credential_metadata() {
        let mut config = Config::default();
        config.provider = "anthropic".to_string();
        config
            .provider_instances
            .insert("work".to_string(), ref_backed_instance());
        config.default_provider_instance = Some("work".to_string());
        config.providers_mut().extra.insert(
            "future-provider".to_string(),
            serde_json::json!({
                "api_key": "future-plaintext",
                "nested": {
                    "api_key_encrypted": "future-ciphertext",
                    "credential_ref": "provider.future.api_key",
                    "api_key_from_env": true
                }
            }),
        );

        assert_eq!(legacy_active_provider_type(&config), "openai");
        let value = legacy_provider_response_view(&config).expect("projection");
        assert_eq!(value["openai"]["api_key"], "****...****");
        assert_eq!(value["openai"]["model"], "gpt-work");
        assert!(value["openai"]["request_overrides"]["common"]["headers"]
            .get("Authorization")
            .is_none());
        assert_eq!(
            value["openai"]["request_overrides"]["common"]["headers"]["X-Api-Key"]["type"],
            "env_ref"
        );
        assert_eq!(
            value["openai"]["request_overrides"]["common"]["body_patch"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let serialized = serde_json::to_string(&value).unwrap();
        for forbidden in [
            "sk-runtime-plaintext",
            "runtime-ciphertext",
            "provider.work.api_key",
            "credential_ref",
            "api_key_encrypted",
            "future-plaintext",
            "future-ciphertext",
            "provider.future.api_key",
            "api_key_from_env",
            "override-header-secret",
            "override-body-secret",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "legacy response leaked {forbidden}: {serialized}"
            );
        }
    }

    #[test]
    fn legacy_get_projection_does_not_mask_an_unavailable_environment_binding() {
        let mut config = Config::default();
        let instance: ProviderInstanceConfig = serde_json::from_value(serde_json::json!({
            "provider_type": "openai",
            "enabled": true,
            "api_key_from_env": true,
            "credential_ref": "provider.missing.api_key"
        }))
        .expect("valid instance");
        config
            .provider_instances
            .insert("missing".to_string(), instance);
        config.default_provider_instance = Some("missing".to_string());

        let value = legacy_provider_response_view(&config).expect("projection");
        assert!(value["openai"].get("api_key").is_none());
        let serialized = serde_json::to_string(&value).unwrap();
        assert!(!serialized.contains("****...****"));
        assert!(!serialized.contains("provider.missing.api_key"));
    }
}
