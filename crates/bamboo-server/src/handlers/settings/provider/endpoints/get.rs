use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};
use bamboo_infrastructure::AVAILABLE_PROVIDERS;

use super::super::super::redaction::redact_providers_for_api;
use super::super::types::ProviderConfigResponse;

pub(super) async fn handle_get_provider_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let mut config = app_state.config.read().await.clone();
    let provider = config.provider.clone();
    config.refresh_provider_api_keys_encrypted()?;
    let providers = serde_json::to_value(&config.providers)?;
    let masked_providers = redact_providers_for_api(providers, &config);

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
