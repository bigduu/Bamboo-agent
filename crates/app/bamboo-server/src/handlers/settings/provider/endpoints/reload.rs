use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};

pub(super) async fn handle_reload_provider_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let new_config = app_state.reload_config_and_runtime().await?;
    let provider = new_config.effective_default_provider().to_string();

    tracing::info!("Provider reloaded successfully: {}", provider);

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "provider": provider
    })))
}
