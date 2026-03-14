use actix_web::{web, HttpResponse};
use chrono::{SecondsFormat, Utc};

use crate::server::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};

mod status;
mod updates;

#[cfg(test)]
mod tests;

/// Gets the setup completion status
///
/// # HTTP Route
/// `GET /bamboo/setup/status`
///
/// # Response Format
/// ```json
/// {
///   "is_complete": true,
///   "has_proxy_config": false,
///   "has_proxy_env": false,
///   "message": "Setup has already been completed in config.json."
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Status retrieved successfully
///
/// # Example
/// ```bash
/// curl http://localhost:3000/bamboo/setup/status
/// ```
pub async fn get_setup_status(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let config = app_state.config.read().await.clone();
    let proxy_environment_flags = status::collect_proxy_environment_flags();
    let setup_status = status::build_setup_status(&config, &proxy_environment_flags);

    Ok(HttpResponse::Ok().json(setup_status))
}

/// Marks the setup as complete
///
/// # HTTP Route
/// `POST /bamboo/setup/complete`
///
/// # Response Format
/// ```json
/// {
///   "success": true
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Setup marked as complete
/// - `500 Internal Server Error`: Failed to update config
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/setup/complete
/// ```
pub async fn mark_setup_complete(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let completed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    app_state
        .update_config(
            |config| updates::set_setup_complete(config, completed_at.clone()),
            ConfigUpdateEffects::default(),
        )
        .await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

/// Resets setup status to incomplete
///
/// # HTTP Route
/// `POST /bamboo/setup/incomplete`
///
/// # Response Format
/// ```json
/// {
///   "success": true
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Setup marked as incomplete
/// - `500 Internal Server Error`: Failed to update config
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/bamboo/setup/incomplete
/// ```
pub async fn mark_setup_incomplete(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let reset_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    app_state
        .update_config(
            |config| {
                updates::set_setup_incomplete(config, reset_at.clone());
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}
