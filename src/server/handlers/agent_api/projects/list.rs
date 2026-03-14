use actix_web::HttpResponse;

use crate::server::error::AppError;

use super::super::fs::get_claude_dir;
use super::super::types::Project;
use super::{filesystem_created_at, list_project_session_files, read_project_path};

/// Lists all Claude Code projects.
pub async fn list_projects() -> Result<HttpResponse, AppError> {
    let claude_dir = get_claude_dir()?;
    let mut projects = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&claude_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() && path.join(".project_path").exists() {
                let project_id = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("")
                    .to_string();

                let project_path = read_project_path(&path);
                let sessions = list_project_session_files(&path)
                    .into_iter()
                    .filter_map(|session_path| {
                        session_path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .map(ToString::to_string)
                    })
                    .collect();

                projects.push(Project {
                    id: project_id,
                    path: project_path,
                    sessions,
                    created_at: filesystem_created_at(&path),
                    most_recent_session: None,
                });
            }
        }
    }

    Ok(HttpResponse::Ok().json(projects))
}
