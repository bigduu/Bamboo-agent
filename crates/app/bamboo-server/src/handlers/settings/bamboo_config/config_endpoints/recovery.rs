//! Config-corruption recovery status + confirm/reject API (#153).
//!
//! `config.json` load-time corruption recovery (salvage / `.bak` / defaults,
//! #37 / #135) is quarantine-and-recover, but never auto-persisted over the
//! corrupt original until a caller explicitly confirms — see
//! [`bamboo_config::ConfigRecoveryStatus`] and `Config::save_to_dir`'s guard.
//! These two endpoints are the surface for that: read the pending status, and
//! accept/reject it.

use actix_web::{web, HttpResponse};
use serde::Deserialize;

use crate::{app_state::AppState, error::AppError};

/// `GET /v1/bamboo/config/recovery-status` — whether `config.json` was
/// recovered from corruption at load and is awaiting confirmation.
pub async fn get_config_recovery_status(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let cfg = app_state.config.read().await;
    Ok(HttpResponse::Ok().json(recovery_status_json(cfg.recovery_status())))
}

#[derive(Debug, Deserialize)]
pub struct ConfirmRecoveryRequest {
    /// `true` to confirm the recovery (persists the recovered config over
    /// the quarantined-corrupt `config.json` and clears the pending flag);
    /// `false` to reject it (no-op — `config.json` stays untouched and the
    /// pending flag stays set; hand-fix the file and reload/restart, or
    /// accept later).
    pub accept: bool,
}

/// `POST /v1/bamboo/config/recovery/confirm` — accept or reject a pending
/// config-corruption recovery.
pub async fn confirm_config_recovery(
    app_state: web::Data<AppState>,
    payload: web::Json<ConfirmRecoveryRequest>,
) -> Result<HttpResponse, AppError> {
    let updated = app_state
        .confirm_config_recovery(payload.into_inner().accept)
        .await?;
    Ok(HttpResponse::Ok().json(recovery_status_json(updated.recovery_status())))
}

fn recovery_status_json(status: Option<&bamboo_config::ConfigRecoveryStatus>) -> serde_json::Value {
    match status {
        Some(status) => serde_json::json!({
            "pending": true,
            "status": status,
        }),
        None => serde_json::json!({ "pending": false }),
    }
}
