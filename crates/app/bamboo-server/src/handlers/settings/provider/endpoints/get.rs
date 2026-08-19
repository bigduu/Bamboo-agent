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
    let masked_providers =
        legacy_provider_response_view(&config, Some(&app_state.credential_store))?;

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
fn legacy_provider_response_view(
    config: &Config,
    credential_store: Option<&bamboo_config::CredentialStore>,
) -> Result<Value, AppError> {
    let mut view = bamboo_config::legacy_provider_compatibility_view(config);
    let configured = legacy_provider_configured(&view, credential_store);
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
    // `ProviderConfigs` flattens unknown metadata. Reuse the same conservative
    // classifier as durable config validation so recovered legacy/LKG values
    // cannot expose private_key/client_secret or nested credential payloads.
    bamboo_config::scrub_provider_metadata_credentials(value);
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

fn legacy_provider_configured(
    providers: &ProviderConfigs,
    credential_store: Option<&bamboo_config::CredentialStore>,
) -> Vec<(&'static str, bool)> {
    macro_rules! configured {
        ($provider_type:literal, $provider:expr) => {
            $provider.as_ref().is_some_and(|provider| {
                // This endpoint reports the live compatibility view. A
                // durable ref or env binding without a successfully hydrated
                // runtime value must not be rendered as a configured mask.
                if provider.api_key_from_env
                    && bamboo_config::provider_api_key_environment_override_active(
                        $provider_type,
                        &provider.api_key,
                    )
                {
                    true
                } else if let (Some(store), Some(reference)) =
                    (credential_store, provider.credential_ref.as_ref())
                {
                    store
                        .status_with_crypto_validation(reference)
                        .is_ok_and(|status| status.configured)
                } else {
                    !provider.api_key_from_env && !provider.api_key.trim().is_empty()
                }
            })
        };
    }

    vec![
        ("openai", configured!("openai", providers.openai)),
        ("anthropic", configured!("anthropic", providers.anthropic)),
        ("gemini", configured!("gemini", providers.gemini)),
        (
            "bodhi",
            providers.bodhi.as_ref().is_some_and(|provider| {
                if let (Some(store), Some(reference)) =
                    (credential_store, provider.credential_ref.as_ref())
                {
                    store
                        .status_with_crypto_validation(reference)
                        .is_ok_and(|status| status.configured)
                } else {
                    !provider.api_key.trim().is_empty()
                }
            }),
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
            "private_key": "instance-private-extra",
            "secrets": {"primary": "instance-plural-extra"},
            "api_keys": {"primary": "instance-api-keys-extra"},
            "tokens": ["instance-tokens-extra"],
            "client_tokens": ["instance-client-tokens-extra"],
            "oauth": {
                "client_id": "public-client-id",
                "value": "instance-oauth-extra"
            },
            "request_overrides": {
                "common": {
                    "headers": {
                        "Authorization": "Bearer override-header-secret",
                        "X-Access-Key": "override-access-key-secret",
                        "X-Private-Key": "override-private-key-secret",
                        "X-Device-Key": "override-device-key-secret",
                        "X-Client-Tokens": "override-client-tokens-secret",
                        "X-Api-Key": {"type": "env_ref", "name": "PROJECTED_API_KEY"},
                        "X-Trace": "public-trace"
                    },
                    "body_patch": [
                        {"path": "/api_key", "value": "override-body-secret"},
                        {"path": "/credential", "value": "override-credential-secret"},
                        {"path": "/secrets/primary", "value": "override-plural-secret"},
                        {"path": "/client_tokens/0", "value": "override-client-token-body-secret"},
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
        let _key = bamboo_config::encryption::set_test_encryption_key([0x6f; 32]);
        let credential_dir = tempfile::tempdir().expect("credential tempdir");
        let credential_store = bamboo_config::CredentialStore::open(credential_dir.path());
        credential_store
            .replace(
                CredentialRef::parse("provider.work.api_key").expect("valid credential reference"),
                "stored-provider-secret",
                bamboo_config::CredentialSource::Migrated,
                0,
            )
            .expect("store provider credential");
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
                    "api_key_from_env": true,
                    "client_secret": "future-client-extra"
                }
            }),
        );

        assert_eq!(legacy_active_provider_type(&config), "openai");
        let value =
            legacy_provider_response_view(&config, Some(&credential_store)).expect("projection");
        assert_eq!(value["openai"]["api_key"], "****...****");
        assert_eq!(value["openai"]["model"], "gpt-work");
        assert_eq!(value["openai"]["oauth"]["client_id"], "public-client-id");
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
            "stored-provider-secret",
            "runtime-ciphertext",
            "provider.work.api_key",
            "credential_ref",
            "api_key_encrypted",
            "future-plaintext",
            "future-ciphertext",
            "provider.future.api_key",
            "api_key_from_env",
            "override-header-secret",
            "override-access-key-secret",
            "override-private-key-secret",
            "override-device-key-secret",
            "override-client-tokens-secret",
            "override-client-token-body-secret",
            "override-body-secret",
            "override-credential-secret",
            "override-plural-secret",
            "instance-private-extra",
            "instance-plural-extra",
            "instance-api-keys-extra",
            "instance-tokens-extra",
            "instance-client-tokens-extra",
            "instance-oauth-extra",
            "future-client-extra",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "legacy response leaked {forbidden}: {serialized}"
            );
        }
    }

    #[test]
    fn legacy_get_projection_does_not_mask_an_unavailable_environment_binding() {
        let _openai_env =
            bamboo_config::test_support::override_runtime_env_var("BAMBOO_OPENAI_API_KEY", None);
        let mut config = Config::default();
        let instance: ProviderInstanceConfig = serde_json::from_value(serde_json::json!({
            "provider_type": "openai",
            "enabled": true,
            "api_key": "stale-runtime-environment-key",
            "api_key_from_env": true,
            "credential_ref": "provider.missing.api_key"
        }))
        .expect("valid instance");
        config
            .provider_instances
            .insert("missing".to_string(), instance);
        config.default_provider_instance = Some("missing".to_string());

        let value = legacy_provider_response_view(&config, None).expect("projection");
        assert!(value["openai"].get("api_key").is_none());
        let serialized = serde_json::to_string(&value).unwrap();
        assert!(!serialized.contains("****...****"));
        assert!(!serialized.contains("provider.missing.api_key"));
        assert!(!serialized.contains("stale-runtime-environment-key"));
    }
}
