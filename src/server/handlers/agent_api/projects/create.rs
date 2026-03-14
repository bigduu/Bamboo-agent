use std::path::PathBuf;

use actix_web::{web, HttpResponse};

use crate::server::error::AppError;

use super::super::fs::{get_claude_dir, write_file};
use super::super::types::{CreateProjectRequest, Project};
use super::project_id_from_canonical_path;

/// Creates a new Claude Code project.
pub async fn create_project(
    req: web::Json<CreateProjectRequest>,
) -> Result<HttpResponse, AppError> {
    let claude_dir = get_claude_dir()?;
    let path = PathBuf::from(&req.path);

    if !path.exists() || !path.is_dir() {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "Path does not exist or is not a directory: {}",
            req.path
        )));
    }

    let canonical = path.canonicalize().map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to canonicalize path: {}", error))
    })?;
    let project_id = project_id_from_canonical_path(&canonical);

    let project_dir = claude_dir.join(&project_id);
    std::fs::create_dir_all(&project_dir).map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to create project dir: {}", error))
    })?;

    write_file(
        &project_dir.join(".project_path"),
        canonical.to_string_lossy().as_bytes(),
        "project path",
    )?;

    let project = Project {
        id: project_id,
        path: req.path.clone(),
        sessions: Vec::new(),
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        most_recent_session: None,
    };

    Ok(HttpResponse::Ok().json(project))
}
