use actix_web::{web, HttpResponse};
use std::path::Path;

use crate::server::app_state::AppState;
use crate::server::error::AppError;

use super::super::path::{
    home_dir, should_skip_entry, to_display_name, validate_workspace_path, DEFAULT_MAX_DEPTH,
    DEFAULT_MAX_ENTRIES, MAX_ALLOWED_ENTRIES,
};
use super::super::types::{
    BrowseFolderRequest, BrowseFolderResponse, FolderItem, WorkspaceFileEntry,
    WorkspaceFilesRequest,
};

pub async fn browse_folder(
    _app_state: web::Data<AppState>,
    payload: web::Json<BrowseFolderRequest>,
) -> Result<HttpResponse, AppError> {
    let target_path = match payload.path.as_ref() {
        Some(path) if !path.trim().is_empty() => validate_workspace_path(path)?,
        _ => home_dir()?,
    };

    let metadata = tokio::fs::metadata(&target_path).await?;
    if !metadata.is_dir() {
        return Err(AppError::NotFound("Folder".to_string()));
    }

    let mut entries = tokio::fs::read_dir(&target_path).await?;
    let mut folders = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_dir() {
            continue;
        }
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|segment| segment.to_str())
            .unwrap_or_default()
            .to_string();
        folders.push(FolderItem {
            name,
            path: path.to_string_lossy().to_string(),
        });
    }
    folders.sort_by(|left, right| left.name.to_lowercase().cmp(&right.name.to_lowercase()));

    let parent_path = target_path.parent().map(path_to_string);
    Ok(HttpResponse::Ok().json(BrowseFolderResponse {
        current_path: path_to_string(&target_path),
        parent_path,
        folders,
    }))
}

pub async fn list_workspace_files(
    _app_state: web::Data<AppState>,
    payload: web::Json<WorkspaceFilesRequest>,
) -> Result<HttpResponse, AppError> {
    let root_path = validate_workspace_path(&payload.path)?;

    let metadata = match tokio::fs::metadata(&root_path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(AppError::NotFound("Workspace".to_string()));
        }
        Err(error) => return Err(AppError::StorageError(error)),
    };
    if !metadata.is_dir() {
        return Err(AppError::NotFound("Workspace".to_string()));
    }

    let max_depth = payload.max_depth.unwrap_or(DEFAULT_MAX_DEPTH);
    let mut max_entries = payload.max_entries.unwrap_or(DEFAULT_MAX_ENTRIES);
    if max_entries > MAX_ALLOWED_ENTRIES {
        max_entries = MAX_ALLOWED_ENTRIES;
    }
    let include_hidden = payload.include_hidden.unwrap_or(false);

    let mut files = Vec::new();
    let mut stack = vec![(root_path.clone(), 0_usize)];
    while let Some((current_path, depth)) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&current_path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_symlink() {
                continue;
            }

            let name = entry.file_name().to_string_lossy().to_string();
            let is_dir = file_type.is_dir();
            if should_skip_entry(&name, is_dir, include_hidden) {
                continue;
            }

            let path = entry.path();
            if is_dir {
                if depth < max_depth {
                    stack.push((path, depth + 1));
                }
                continue;
            }

            files.push(WorkspaceFileEntry {
                name: to_display_name(&root_path, &path),
                path: path.to_string_lossy().to_string(),
                is_directory: false,
            });

            if files.len() >= max_entries {
                return Ok(HttpResponse::Ok().json(files));
            }
        }
    }

    Ok(HttpResponse::Ok().json(files))
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().to_string()
}
