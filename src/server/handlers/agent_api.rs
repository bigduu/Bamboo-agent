use crate::server::error::AppError;
use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// Types (mirrored from src-tauri/src/command/claude_code.rs)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub path: String,
    pub sessions: Vec<String>,
    pub created_at: u64,
    pub most_recent_session: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub project_id: String,
    pub project_path: String,
    pub todo_data: Option<serde_json::Value>,
    pub created_at: u64,
    pub first_message: Option<String>,
    pub message_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeSettings {
    #[serde(flatten)]
    pub data: serde_json::Value,
}

impl Default for ClaudeSettings {
    fn default() -> Self {
        Self {
            data: serde_json::json!({}),
        }
    }
}

// Request/Response types
#[derive(Debug, Deserialize)]
pub struct CreateProjectRequest {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct SaveSettingsRequest {
    pub settings: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct SaveSystemPromptRequest {
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    pub project_path: String,
    pub prompt: String,
    pub session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CancelRequest {
    pub session_id: String,
}

// Helper functions
fn get_claude_dir() -> Result<PathBuf, AppError> {
    dirs::home_dir()
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("Could not find home directory")))?
        .join(".claude")
        .canonicalize()
        .map_err(|e| {
            AppError::InternalError(anyhow::anyhow!("Could not find ~/.claude directory: {}", e))
        })
}

// Endpoints

/// GET /agent/projects - List all projects
pub async fn list_projects() -> Result<HttpResponse, AppError> {
    let claude_dir = get_claude_dir()?;
    let mut projects = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&claude_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() && path.join(".project_path").exists() {
                let project_id = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string();

                let project_path = std::fs::read_to_string(path.join(".project_path"))
                    .unwrap_or_default()
                    .trim()
                    .to_string();

                let sessions = std::fs::read_dir(&path)
                    .map(|entries| {
                        entries
                            .flatten()
                            .filter(|e| {
                                e.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl")
                            })
                            .filter_map(|e| e.file_name().into_string().ok())
                            .collect()
                    })
                    .unwrap_or_default();

                let metadata = std::fs::metadata(&path)
                    .ok()
                    .and_then(|m| m.created().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                projects.push(Project {
                    id: project_id,
                    path: project_path,
                    sessions,
                    created_at: metadata,
                    most_recent_session: None,
                });
            }
        }
    }

    Ok(HttpResponse::Ok().json(projects))
}

/// POST /agent/projects - Create a new project
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

    // Create project ID from path
    let canonical = path.canonicalize().map_err(|e| {
        AppError::InternalError(anyhow::anyhow!("Failed to canonicalize path: {}", e))
    })?;
    let project_id = canonical.to_string_lossy().replace(['/', '\\'], "-");

    let project_dir = claude_dir.join(&project_id);
    std::fs::create_dir_all(&project_dir).map_err(|e| {
        AppError::InternalError(anyhow::anyhow!("Failed to create project dir: {}", e))
    })?;

    // Write project path file
    std::fs::write(
        project_dir.join(".project_path"),
        canonical.to_string_lossy().as_bytes(),
    )
    .map_err(|e| AppError::InternalError(anyhow::anyhow!("Failed to write project path: {}", e)))?;

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

/// GET /agent/projects/{id}/sessions - Get sessions for a project
pub async fn get_project_sessions(path: web::Path<String>) -> Result<HttpResponse, AppError> {
    let claude_dir = get_claude_dir()?;
    let project_id = path.into_inner();
    let project_dir = claude_dir.join(&project_id);

    if !project_dir.exists() {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "Project not found"
        )));
    }

    let project_path = std::fs::read_to_string(project_dir.join(".project_path"))
        .unwrap_or_default()
        .trim()
        .to_string();

    let mut sessions = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&project_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                let session_id = path
                    .file_stem()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string();

                let metadata = std::fs::metadata(&path)
                    .ok()
                    .and_then(|m| m.created().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                sessions.push(Session {
                    id: session_id,
                    project_id: project_id.clone(),
                    project_path: project_path.clone(),
                    todo_data: None,
                    created_at: metadata,
                    first_message: None,
                    message_timestamp: None,
                });
            }
        }
    }

    Ok(HttpResponse::Ok().json(sessions))
}

/// GET /agent/settings - Get Claude settings
pub async fn get_claude_settings() -> Result<HttpResponse, AppError> {
    let settings_path = dirs::home_dir()
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("Home directory not found")))?
        .join(".claude")
        .join("settings.json");

    if settings_path.exists() {
        let content = std::fs::read_to_string(&settings_path).map_err(|e| {
            AppError::InternalError(anyhow::anyhow!("Failed to read settings: {}", e))
        })?;
        let data: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
            AppError::InternalError(anyhow::anyhow!("Failed to parse settings: {}", e))
        })?;
        Ok(HttpResponse::Ok().json(ClaudeSettings { data }))
    } else {
        Ok(HttpResponse::Ok().json(ClaudeSettings::default()))
    }
}

/// POST /agent/settings - Save Claude settings
pub async fn save_claude_settings(
    req: web::Json<SaveSettingsRequest>,
) -> Result<HttpResponse, AppError> {
    let settings_path = dirs::home_dir()
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("Home directory not found")))?
        .join(".claude")
        .join("settings.json");

    let content = serde_json::to_string_pretty(&req.settings).map_err(|e| {
        AppError::InternalError(anyhow::anyhow!("Failed to serialize settings: {}", e))
    })?;

    std::fs::write(&settings_path, content)
        .map_err(|e| AppError::InternalError(anyhow::anyhow!("Failed to write settings: {}", e)))?;

    Ok(HttpResponse::Ok().json(serde_json::json!({"success": true, "path": settings_path})))
}

/// GET /agent/system-prompt - Get system prompt
pub async fn get_system_prompt() -> Result<HttpResponse, AppError> {
    let prompt_path = dirs::home_dir()
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("Home directory not found")))?
        .join(".claude")
        .join("system-prompt.md");

    if prompt_path.exists() {
        let content = std::fs::read_to_string(&prompt_path).map_err(|e| {
            AppError::InternalError(anyhow::anyhow!("Failed to read system prompt: {}", e))
        })?;
        Ok(HttpResponse::Ok().json(serde_json::json!({ "content": content, "path": prompt_path })))
    } else {
        Ok(HttpResponse::Ok().json(serde_json::json!({ "content": "", "path": prompt_path })))
    }
}

/// POST /agent/system-prompt - Save system prompt
pub async fn save_system_prompt(
    req: web::Json<SaveSystemPromptRequest>,
) -> Result<HttpResponse, AppError> {
    let prompt_path = dirs::home_dir()
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("Home directory not found")))?
        .join(".claude")
        .join("system-prompt.md");

    std::fs::write(&prompt_path, &req.content).map_err(|e| {
        AppError::InternalError(anyhow::anyhow!("Failed to write system prompt: {}", e))
    })?;

    Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true, "path": prompt_path })))
}

/// GET /agent/sessions/running - List running Claude sessions
pub async fn list_running_claude_sessions() -> Result<HttpResponse, AppError> {
    // This would need process registry integration
    // For now, return empty list
    Ok(HttpResponse::Ok().json(Vec::<serde_json::Value>::new()))
}

/// POST /agent/sessions/execute - Execute Claude code
pub async fn execute_claude_code(
    _req: web::Json<ExecuteRequest>,
) -> Result<HttpResponse, AppError> {
    // This requires process management and streaming
    // Placeholder implementation
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "message": "Execution started - streaming not yet implemented"
    })))
}

/// POST /agent/sessions/cancel - Cancel Claude execution
pub async fn cancel_claude_execution(
    _req: web::Json<CancelRequest>,
) -> Result<HttpResponse, AppError> {
    // This requires process registry integration
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "message": "Cancellation request sent"
    })))
}

/// GET /agent/sessions/{id}/jsonl - Get session JSONL content
pub async fn get_session_jsonl(
    path: web::Path<String>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> Result<HttpResponse, AppError> {
    let claude_dir = get_claude_dir()?;
    let session_id = path.into_inner();
    let project_id = query.get("project_id").ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!("project_id query parameter required"))
    })?;

    let project_dir = claude_dir.join(project_id);
    let session_path = project_dir.join(format!("{}.jsonl", session_id));

    if !session_path.exists() {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "Session not found"
        )));
    }

    let content = std::fs::read_to_string(&session_path)
        .map_err(|e| AppError::InternalError(anyhow::anyhow!("Failed to read session: {}", e)))?;

    let lines: Vec<serde_json::Value> = content
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();

    Ok(HttpResponse::Ok().json(lines))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/agent")
            .route("/projects", web::get().to(list_projects))
            .route("/projects", web::post().to(create_project))
            .route(
                "/projects/{project_id}/sessions",
                web::get().to(get_project_sessions),
            )
            .route("/settings", web::get().to(get_claude_settings))
            .route("/settings", web::post().to(save_claude_settings))
            .route("/system-prompt", web::get().to(get_system_prompt))
            .route("/system-prompt", web::post().to(save_system_prompt))
            .route(
                "/sessions/running",
                web::get().to(list_running_claude_sessions),
            )
            .route("/sessions/execute", web::post().to(execute_claude_code))
            .route(
                "/sessions/cancel",
                web::post().to(cancel_claude_execution),
            )
            .route(
                "/sessions/{session_id}/jsonl",
                web::get().to(get_session_jsonl),
            ),
    );
}
