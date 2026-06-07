use std::{fs, path::Path};

use actix_web::{web, HttpResponse};

use crate::{app_state::AppState, error::AppError};

use super::types::AuthStatus;

pub async fn get_copilot_auth_status(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let app_data_dir = app_state.app_data_dir.clone();
    let copilot_token_path = app_data_dir.join(".copilot_token.json");

    if let Some(status) = load_auth_status(&copilot_token_path, current_unix_secs()) {
        return Ok(HttpResponse::Ok().json(status));
    }

    Ok(HttpResponse::Ok().json(AuthStatus {
        authenticated: false,
        message: Some("No cached token found".to_string()),
    }))
}

pub async fn logout_copilot(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let app_data_dir = app_state.app_data_dir.clone();
    let token_path = app_data_dir.join(".token");
    let copilot_token_path = app_data_dir.join(".copilot_token.json");

    let mut success = true;
    let mut messages = Vec::new();

    success &= remove_if_exists(&token_path, ".token", &mut messages);
    success &= remove_if_exists(&copilot_token_path, ".copilot_token.json", &mut messages);

    if success {
        tracing::info!("Copilot logged out successfully");
        Ok(HttpResponse::Ok().json(serde_json::json!({
            "success": true,
            "message": "Logged out successfully"
        })))
    } else {
        tracing::error!("Failed to logout: {}", messages.join(", "));
        Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "success": false,
            "error": messages.join(", ")
        })))
    }
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

fn load_auth_status(token_path: &Path, now: u64) -> Option<AuthStatus> {
    let content = fs::read_to_string(token_path).ok()?;
    auth_status_from_token_content(&content, now)
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn remove_if_exists(path: &Path, display_name: &str, messages: &mut Vec<String>) -> bool {
    if !path.exists() {
        return true;
    }

    match fs::remove_file(path) {
        Ok(_) => {
            messages.push(format!("Deleted {display_name}"));
            true
        }
        Err(err) => {
            messages.push(format!("Failed to delete {display_name}: {err}"));
            false
        }
    }
}
