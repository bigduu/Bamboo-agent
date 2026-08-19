use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};

mod dispatch;
mod upstream;

#[cfg(test)]
mod tests;

/// Fetch available models for a specific provider.
pub async fn fetch_provider_models(
    app_state: web::Data<AppState>,
    payload: web::Json<serde_json::Value>,
) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await.clone();
    let provider_key = dispatch::provider_key_from_payload(&payload, &config);
    let target = dispatch::resolve_model_fetch_target(&config, &provider_key)?;
    let client = dispatch::build_proxy_aware_http_client(&config)?;
    let models = dispatch::fetch_models_for_provider(app_state.get_ref(), &target, &client).await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "models": models })))
}
