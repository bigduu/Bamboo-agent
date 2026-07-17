use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

/// `GET /api/v1/sessions/{session_id}/attachments/{attachment_id}`
pub async fn get_attachment(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse> {
    let (session_id, attachment_id) = path.into_inner();
    match state
        .session_store
        .read_attachment(&session_id, &attachment_id)
        .await
    {
        Ok(Some((bytes, mime))) => Ok(HttpResponse::Ok()
            .content_type(mime)
            .append_header(("Cache-Control", "no-store"))
            .body(bytes)),
        Ok(None) => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("Attachment not found"),
            "session_id": session_id,
            "attachment_id": attachment_id
        }))),
        Err(error) => Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": crate::error::error_value(format!("Failed to read attachment: {error}")),
            "session_id": session_id,
            "attachment_id": attachment_id
        }))),
    }
}
