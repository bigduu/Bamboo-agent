use actix_web::{web, HttpResponse};
use bamboo_config::{
    CredentialRef, CredentialStore, COPILOT_CHAT_CONFIG_CREDENTIAL_REF,
    COPILOT_GITHUB_ACCESS_CREDENTIAL_REF,
};

use crate::{app_state::AppState, error::AppError};

use super::types::AuthStatus;

pub async fn get_copilot_auth_status(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let chat_ref = CredentialRef::parse(COPILOT_CHAT_CONFIG_CREDENTIAL_REF.to_string())
        .map_err(|_| copilot_status_error())?;
    if let Some(cached) = CredentialStore::open(&app_state.app_data_dir)
        .resolve(&chat_ref)
        .map_err(|_| copilot_status_error())?
    {
        let Some(status) = auth_status_from_token_content(cached.expose(), current_unix_secs())
        else {
            return Err(copilot_status_error());
        };
        return Ok(HttpResponse::Ok().json(status));
    }

    Ok(HttpResponse::Ok().json(AuthStatus {
        authenticated: false,
        message: Some("No cached token found".to_string()),
    }))
}

pub async fn logout_copilot(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let store = CredentialStore::open(&app_state.app_data_dir);
    for reference in [
        COPILOT_GITHUB_ACCESS_CREDENTIAL_REF,
        COPILOT_CHAT_CONFIG_CREDENTIAL_REF,
    ] {
        let reference = CredentialRef::parse(reference.to_string()).map_err(|_| {
            AppError::InternalError(anyhow::anyhow!("Copilot credential cleanup failed"))
        })?;
        store.clear_system_managed(&reference).map_err(|_| {
            AppError::InternalError(anyhow::anyhow!("Copilot credential cleanup failed"))
        })?;
    }
    tracing::info!("Copilot logged out successfully");
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "message": "Logged out successfully"
    })))
}

fn copilot_status_error() -> AppError {
    AppError::InternalError(anyhow::anyhow!("Copilot credential status is unavailable"))
}

pub(super) fn auth_status_from_token_content(content: &str, now: u64) -> Option<AuthStatus> {
    let token_data = serde_json::from_str::<serde_json::Value>(content).ok()?;
    let expires_at = token_data
        .get("expires_at")
        .and_then(|value| value.as_u64())?;

    if expires_at.saturating_sub(60) > now {
        let remaining = expires_at.saturating_sub(now);
        Some(AuthStatus {
            authenticated: true,
            message: Some(format!("Token expires in {} minutes", remaining / 60)),
        })
    } else {
        Some(AuthStatus {
            authenticated: false,
            message: Some("Token expired".to_string()),
        })
    }
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
