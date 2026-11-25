use actix_web::{web, HttpResponse, Result};
use serde::{Deserialize, Serialize};

use crate::app_state::AppState;
use crate::title_gen::{self, TitleGenerationOutcome};

#[derive(Deserialize)]
pub struct RegenerateTitleQuery {
    #[serde(default)]
    wait: bool,
}

#[derive(Serialize)]
struct RegenerateTitleResponse<'a> {
    session_id: &'a str,
    #[serde(flatten)]
    outcome: TitleGenerationOutcome,
}

/// `POST /api/v1/sessions/{session_id}/regenerate-title`
///
/// Legacy callers receive `202 Accepted`. Pass `?wait=true` to receive the
/// outcome. A committed title is also published on the session change feed.
pub async fn regenerate_session_title(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<RegenerateTitleQuery>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();
    let session = state
        .storage
        .load_session(&session_id)
        .await
        .map_err(|error| {
            crate::error::json_internal_server_error(format!("Failed to load session: {error}"))
        })?;
    let Some(session) = session else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Session not found"),
            "session_id": session_id
        })));
    };

    let result = title_gen::spawn_title_regeneration(state.into_inner(), &session);
    if query.wait {
        let outcome = result.await.unwrap_or(TitleGenerationOutcome::Unavailable);
        return Ok(HttpResponse::Ok().json(RegenerateTitleResponse {
            session_id: &session_id,
            outcome,
        }));
    }

    Ok(HttpResponse::Accepted().json(serde_json::json!({
        "status": "accepted",
        "session_id": session_id
    })))
}
