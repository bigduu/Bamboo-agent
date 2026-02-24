use actix_web::{get, post, web, HttpResponse};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::server::error::AppError;
use crate::server::app_state::AppState;

/// Validates and canonicalizes a workspace path to prevent directory traversal attacks.
/// Ensures the path exists and is accessible.
fn validate_workspace_path(input_path: &str) -> Result<PathBuf, AppError> {
    let trimmed = input_path.trim();

    if trimmed.is_empty() {
        return Err(AppError::BadRequest("Path cannot be empty".to_string()));
    }

    // Basic path traversal check before canonicalization
    if trimmed.contains("..") {
        return Err(AppError::BadRequest(
            "Path cannot contain '..' sequences".to_string(),
        ));
    }

    let path = PathBuf::from(trimmed);

    // Canonicalize to resolve symlinks and normalize path
    let canonical = path.canonicalize().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AppError::NotFound(format!("Path does not exist: {}", trimmed))
        } else {
            AppError::BadRequest(format!("Invalid path: {}", e))
        }
    })?;

    Ok(canonical)
}

#[derive(Deserialize)]
struct WorkspacePathRequest {
    path: String,
}

#[derive(Deserialize)]
struct BrowseFolderRequest {
    path: Option<String>,
}

#[derive(Deserialize)]
struct WorkspaceFilesRequest {
    path: String,
    max_depth: Option<usize>,
    max_entries: Option<usize>,
    include_hidden: Option<bool>,
}

#[derive(Serialize)]
struct BrowseFolderResponse {
    current_path: String,
    parent_path: Option<String>,
    folders: Vec<FolderItem>,
}

#[derive(Serialize)]
struct FolderItem {
    name: String,
    path: String,
}

#[derive(Serialize)]
struct WorkspaceFileEntry {
    name: String,
    path: String,
    is_directory: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct WorkspaceMetadata {
    workspace_name: Option<String>,
    description: Option<String>,
    tags: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Clone)]
struct RecentWorkspaceEntry {
    path: String,
    metadata: Option<WorkspaceMetadata>,
    last_opened: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct RecentWorkspaceStore {
    items: Vec<RecentWorkspaceEntry>,
}

#[derive(Serialize)]
struct WorkspaceInfo {
    path: String,
    is_valid: bool,
    error_message: Option<String>,
    file_count: Option<u64>,
    last_modified: Option<String>,
    size_bytes: Option<u64>,
    workspace_name: Option<String>,
}

#[derive(Serialize)]
struct PathSuggestion {
    path: String,
    name: String,
    description: Option<String>,
    suggestion_type: String,
}

#[derive(Serialize)]
struct PathSuggestionsResponse {
    suggestions: Vec<PathSuggestion>,
}

#[derive(Deserialize)]
struct AddRecentWorkspaceRequest {
    path: String,
    metadata: Option<WorkspaceMetadata>,
}

fn home_dir() -> Result<PathBuf, AppError> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("HOME not set")))?;
    Ok(PathBuf::from(home))
}

fn workspace_store_path(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join("workspaces").join("recent.json")
}

const DEFAULT_MAX_DEPTH: usize = 6;
const DEFAULT_MAX_ENTRIES: usize = 2000;
const MAX_ALLOWED_ENTRIES: usize = 10000;
const IGNORED_DIRS: [&str; 10] = [
    ".git",
    "node_modules",
    "target",
    "dist",
    "build",
    ".next",
    ".turbo",
    ".cache",
    ".idea",
    ".vscode",
];

fn should_skip_entry(name: &str, is_dir: bool, include_hidden: bool) -> bool {
    if !include_hidden && name.starts_with('.') {
        return true;
    }
    if is_dir && IGNORED_DIRS.iter().any(|ignored| ignored == &name) {
        return true;
    }
    false
}

fn to_display_name(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

async fn load_recent_store(app_data_dir: &Path) -> Result<RecentWorkspaceStore, AppError> {
    let path = workspace_store_path(app_data_dir);
    let content = match tokio::fs::read_to_string(&path).await {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RecentWorkspaceStore::default())
        }
        Err(e) => return Err(AppError::StorageError(e)),
    };
    let store = serde_json::from_str::<RecentWorkspaceStore>(&content)
        .map_err(AppError::SerializationError)?;
    Ok(store)
}

async fn save_recent_store(
    app_data_dir: &Path,
    store: &RecentWorkspaceStore,
) -> Result<(), AppError> {
    let path = workspace_store_path(app_data_dir);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let content = serde_json::to_string_pretty(store).map_err(AppError::SerializationError)?;
    tokio::fs::write(&path, content).await?;
    Ok(())
}

async fn build_workspace_info(path: &str) -> WorkspaceInfo {
    let workspace_name = PathBuf::from(path)
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string());

    let metadata = tokio::fs::metadata(path).await;
    match metadata {
        Ok(meta) => {
            if !meta.is_dir() {
                return WorkspaceInfo {
                    path: path.to_string(),
                    is_valid: false,
                    error_message: Some("Not a directory".to_string()),
                    file_count: None,
                    last_modified: None,
                    size_bytes: None,
                    workspace_name,
                };
            }

            let mut count: u64 = 0;
            if let Ok(mut entries) = tokio::fs::read_dir(path).await {
                while let Ok(Some(_)) = entries.next_entry().await {
                    count += 1;
                }
            }

            WorkspaceInfo {
                path: path.to_string(),
                is_valid: true,
                error_message: None,
                file_count: Some(count),
                last_modified: None,
                size_bytes: None,
                workspace_name,
            }
        }
        Err(err) => WorkspaceInfo {
            path: path.to_string(),
            is_valid: false,
            error_message: Some(err.to_string()),
            file_count: None,
            last_modified: None,
            size_bytes: None,
            workspace_name,
        },
    }
}

#[post("/workspace/validate")]
pub async fn validate_workspace(
    _app_state: web::Data<AppState>,
    payload: web::Json<WorkspacePathRequest>,
) -> Result<HttpResponse, AppError> {
    // Validate path exists (but don't require canonicalization for validation endpoint)
    let path = payload.path.trim();
    if path.is_empty() {
        return Err(AppError::BadRequest("Path cannot be empty".to_string()));
    }

    let info = build_workspace_info(path).await;
    Ok(HttpResponse::Ok().json(info))
}

#[get("/workspace/recent")]
pub async fn get_recent_workspaces(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let store = load_recent_store(&app_state.app_data_dir).await?;
    let mut infos = Vec::new();
    for item in store.items.iter() {
        let mut info = build_workspace_info(&item.path).await;
        if info.workspace_name.is_none() {
            info.workspace_name = item
                .metadata
                .as_ref()
                .and_then(|m| m.workspace_name.clone());
        }
        infos.push(info);
    }
    Ok(HttpResponse::Ok().json(infos))
}

#[post("/workspace/recent")]
pub async fn add_recent_workspace(
    app_state: web::Data<AppState>,
    payload: web::Json<AddRecentWorkspaceRequest>,
) -> Result<HttpResponse, AppError> {
    let mut store = load_recent_store(&app_state.app_data_dir).await?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if let Some(existing) = store.items.iter_mut().find(|i| i.path == payload.path) {
        existing.metadata = payload.metadata.clone();
        existing.last_opened = now;
    } else {
        store.items.insert(
            0,
            RecentWorkspaceEntry {
                path: payload.path.clone(),
                metadata: payload.metadata.clone(),
                last_opened: now,
            },
        );
    }

    store
        .items
        .sort_by(|a, b| b.last_opened.cmp(&a.last_opened));
    store.items.truncate(50);

    save_recent_store(&app_state.app_data_dir, &store).await?;

    Ok(HttpResponse::NoContent().finish())
}

#[get("/workspace/suggestions")]
pub async fn get_workspace_suggestions(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let mut suggestions: Vec<PathSuggestion> = Vec::new();

    let home = home_dir()?;
    let home_str = home.to_string_lossy().to_string();
    suggestions.push(PathSuggestion {
        path: home_str.clone(),
        name: "Home".to_string(),
        description: None,
        suggestion_type: "home".to_string(),
    });

    let candidates = vec![
        ("documents", "Documents"),
        ("desktop", "Desktop"),
        ("downloads", "Downloads"),
    ];

    for (suggestion_type, folder) in candidates {
        let path = home.join(folder);
        if tokio::fs::metadata(&path).await.is_ok() {
            suggestions.push(PathSuggestion {
                path: path.to_string_lossy().to_string(),
                name: folder.to_string(),
                description: None,
                suggestion_type: suggestion_type.to_string(),
            });
        }
    }

    let store = load_recent_store(&app_state.app_data_dir).await?;
    for item in store.items.iter() {
        let name = item
            .metadata
            .as_ref()
            .and_then(|m| m.workspace_name.clone())
            .or_else(|| {
                PathBuf::from(&item.path)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| item.path.clone());

        suggestions.push(PathSuggestion {
            path: item.path.clone(),
            name,
            description: None,
            suggestion_type: "recent".to_string(),
        });
    }

    let mut seen = std::collections::HashSet::new();
    suggestions.retain(|item| seen.insert(item.path.clone()));

    Ok(HttpResponse::Ok().json(PathSuggestionsResponse { suggestions }))
}

#[post("/workspace/browse-folder")]
pub async fn browse_folder(
    _app_state: web::Data<AppState>,
    payload: web::Json<BrowseFolderRequest>,
) -> Result<HttpResponse, AppError> {
    let target_path = match payload.path.as_ref() {
        Some(path) if !path.trim().is_empty() => {
            // Validate and canonicalize user-provided path
            validate_workspace_path(path)?
        }
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
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        folders.push(FolderItem {
            name,
            path: path.to_string_lossy().to_string(),
        });
    }

    folders.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));

    let parent_path = target_path
        .parent()
        .map(|p| p.to_string_lossy().to_string());

    Ok(HttpResponse::Ok().json(BrowseFolderResponse {
        current_path: target_path.to_string_lossy().to_string(),
        parent_path,
        folders,
    }))
}

#[post("/workspace/files")]
pub async fn list_workspace_files(
    _app_state: web::Data<AppState>,
    payload: web::Json<WorkspaceFilesRequest>,
) -> Result<HttpResponse, AppError> {
    // Validate and canonicalize the path
    let root_path = validate_workspace_path(&payload.path)?;

    let metadata = match tokio::fs::metadata(&root_path).await {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(AppError::NotFound("Workspace".to_string()))
        }
        Err(err) => return Err(AppError::StorageError(err)),
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

    let mut files: Vec<WorkspaceFileEntry> = Vec::new();
    let mut stack: Vec<(PathBuf, usize)> = vec![(root_path.clone(), 0)];

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

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(validate_workspace)
        .service(get_recent_workspaces)
        .service(add_recent_workspace)
        .service(get_workspace_suggestions)
        .service(browse_folder)
        .service(list_workspace_files);
}
