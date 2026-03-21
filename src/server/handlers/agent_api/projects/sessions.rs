use actix_web::{web, HttpResponse};

use crate::server::error::AppError;

use super::super::fs::get_claude_dir;
use super::super::types::Session;
use super::{filesystem_created_at, list_project_session_files, read_project_path};

/// Gets all sessions for a specific project.
pub async fn get_project_sessions(path: web::Path<String>) -> Result<HttpResponse, AppError> {
    let claude_dir = get_claude_dir()?;
    let project_id = path.into_inner();
    let project_dir = claude_dir.join(&project_id);

    if !project_dir.exists() {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "Project not found"
        )));
    }

    let project_path = read_project_path(&project_dir);
    let sessions = list_project_session_files(&project_dir)
        .into_iter()
        .map(|session_path| {
            let session_id = session_path
                .file_stem()
                .and_then(|name| name.to_str())
                .unwrap_or("")
                .to_string();

            Session {
                id: session_id,
                project_id: project_id.clone(),
                project_path: project_path.clone(),
                task_data: None,
                created_at: filesystem_created_at(&session_path),
                first_message: None,
                message_timestamp: None,
            }
        })
        .collect::<Vec<_>>();

    Ok(HttpResponse::Ok().json(sessions))
}
