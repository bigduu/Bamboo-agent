//! HTTP handlers for the subagent-profile catalogue.
//!
//! Exposes the same registry consulted by `SubSession.action=list_profiles`
//! over a plain REST endpoint so the frontend can populate role pickers
//! without going through the tool-call surface.

use actix_web::{web, HttpResponse};
use serde_json::json;

use crate::app_state::AppState;
use crate::error::AppError;

/// `GET /subagent_profiles`
///
/// Returns every registered subagent profile (built-ins plus any
/// user/project overrides) in the registry's stable insertion order.
///
/// Response shape (kept identical to the `list_profiles` tool action so
/// the frontend can consume either surface uniformly):
///
/// ```jsonc
/// {
///   "profiles": [
///     {
///       "id": "researcher",
///       "display_name": "Researcher",
///       "description": "...",
///       "tools": { "mode": "allowlist", "allow": ["Read", "Grep"] },
///       "model_hint": null,
///       "default_responsibility": null,
///       "ui": { "icon": "🔎", "color": "blue" }
///     }
///   ],
///   "fallback_id": "general-purpose",
///   "count": 6
/// }
/// ```
///
/// `system_prompt` is intentionally omitted (can be lengthy; UI does
/// not need it for role selection).
pub async fn list_subagent_profiles(state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let registry = state.subagent_profiles.as_ref();

    let profiles: Vec<serde_json::Value> = registry
        .iter()
        .map(|p| {
            json!({
                "id": p.id,
                "display_name": p.display_name,
                "description": p.description,
                "tools": p.tools,
                "model_hint": p.model_hint,
                "default_responsibility": p.default_responsibility,
                "ui": p.ui,
            })
        })
        .collect();

    Ok(HttpResponse::Ok().json(json!({
        "profiles": profiles,
        "fallback_id": registry.fallback_id(),
        "count": registry.len(),
    })))
}
