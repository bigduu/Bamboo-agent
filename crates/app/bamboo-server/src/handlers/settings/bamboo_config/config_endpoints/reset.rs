use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};

/// Resets (deletes) the Bamboo configuration file.
pub async fn reset_bamboo_config(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    app_state.reset_legacy_config_and_runtime().await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}
