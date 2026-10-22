use actix_web::{web, HttpResponse};
use bamboo_config::SectionId;
use chrono::{SecondsFormat, Utc};

use super::bamboo_config::map_mutation_error;

use crate::{
    app_state::{AppState, ConfigUpdateEffects},
    error::AppError,
};

mod status;
mod updates;

#[cfg(test)]
mod http_tests;
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
    update_setup_config(&app_state, |config| {
        updates::set_setup_complete(config, completed_at)
    })
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
    update_setup_config(&app_state, |config| {
        updates::set_setup_incomplete(config, reset_at);
        Ok(())
    })
    .await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
}

/// Setup owns only Core metadata. A full compatibility projection can also
/// canonicalize unrelated provider settings, turning this one-domain update
/// into a rejected multi-section write. Reuse the exact Core read and CAS
/// publication path so unrelated values and credential bindings stay intact.
async fn update_setup_config<F>(app_state: &AppState, update: F) -> Result<(), AppError>
where
    F: FnOnce(&mut bamboo_llm::Config) -> Result<(), AppError>,
{
    if app_state.config_facade.is_none() {
        // Retain the existing recovery guard for legacy/recovered layouts.
        app_state
            .update_config(update, ConfigUpdateEffects::default())
            .await?;
        return Ok(());
    }

    let mut exact = app_state
        .read_exact_credential_section(SectionId::Core)
        .await?;
    update(&mut exact.config)?;
    if let Some(setup) = exact.config.extra.get("setup") {
        let core = exact.section.data.as_object_mut().ok_or_else(|| {
            AppError::BadRequest("Core configuration must be a JSON object".to_string())
        })?;
        core.insert("setup".to_string(), setup.clone());
    }
    app_state
        .put_ordinary_section(SectionId::Core, exact.section.revision, exact.section.data)
        .await
        .map_err(map_mutation_error)?;
    Ok(())
}
