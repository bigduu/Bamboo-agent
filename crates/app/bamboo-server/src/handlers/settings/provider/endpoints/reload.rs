use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};

pub(super) async fn handle_reload_provider_config(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let new_config = app_state.reload_config_and_runtime().await?;

    tracing::info!("Provider reloaded successfully: {}", new_config.provider);

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "provider": new_config.provider
    })))
}
