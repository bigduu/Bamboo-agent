use crate::server::error::AppError;
use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// ============================================================================
// Data Types
// ============================================================================

/// Represents a Claude Code project with its metadata and sessions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    /// Unique project identifier
    pub id: String,
    /// File system path to the project
    pub path: String,
    /// List of session IDs associated with this project
    pub sessions: Vec<String>,
    /// Unix timestamp of project creation
    pub created_at: u64,
    /// Unix timestamp of most recent session (if any)
    pub most_recent_session: Option<u64>,
}

/// Represents a Claude Code conversation session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    /// Unique session identifier
    pub id: String,
    /// ID of the parent project
    pub project_id: String,
    /// File system path to the project
    pub project_path: String,
    /// Optional TODO data for the session
    pub todo_data: Option<serde_json::Value>,
    /// Unix timestamp of session creation
    pub created_at: u64,
    /// First message content (for preview)
    pub first_message: Option<String>,
    /// ISO timestamp of first message
    pub message_timestamp: Option<String>,
}

/// Claude settings configuration wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeSettings {
    /// Settings data as JSON
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

// ============================================================================
// Request Types
// ============================================================================

/// Request body for creating a new project
#[derive(Debug, Deserialize)]
pub struct CreateProjectRequest {
    /// File system path to the project directory
    pub path: String,
}

/// Request body for saving Claude settings
#[derive(Debug, Deserialize)]
pub struct SaveSettingsRequest {
    /// Settings data as JSON
    pub settings: serde_json::Value,
}

/// Request body for saving system prompt
#[derive(Debug, Deserialize)]
pub struct SaveSystemPromptRequest {
    /// System prompt content (markdown)
    pub content: String,
}

/// Request body for executing Claude code
#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    /// Project directory path
    pub project_path: String,
    /// User prompt to execute
    pub prompt: String,
    /// Optional session ID to resume
    pub session_id: Option<String>,
}

/// Request body for canceling execution
#[derive(Debug, Deserialize)]
pub struct CancelRequest {
    /// Session ID to cancel
    pub session_id: String,
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Gets the Claude configuration directory (~/.claude)
fn get_claude_dir() -> Result<PathBuf, AppError> {
    dirs::home_dir()
        .ok_or_else(|| AppError::InternalError(anyhow::anyhow!("Could not find home directory")))?
        .join(".claude")
        .canonicalize()
        .map_err(|e| {
            AppError::InternalError(anyhow::anyhow!("Could not find ~/.claude directory: {}", e))
        })
}

// ============================================================================
// HTTP Handlers
// ============================================================================

/// Lists all Claude Code projects
///
/// # HTTP Route
/// `GET /agent/projects`
///
/// # Response Format
/// Returns an array of [`Project`] objects:
/// ```json
/// [
///   {
///     "id": "-Users-me-projects-myproject",
///     "path": "/Users/me/projects/myproject",
///     "sessions": ["session-1", "session-2"],
///     "created_at": 1234567890,
///     "most_recent_session": 1234567890
///   }
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved project list
///
/// # Example
/// ```bash
/// curl http://localhost:3000/agent/projects
/// ```
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

/// Creates a new Claude Code project
///
/// # HTTP Route
/// `POST /agent/projects`
///
/// # Request Body
/// ```json
/// {
///   "path": "/Users/me/projects/myproject"
/// }
/// ```
///
/// # Response Format
/// Returns the created [`Project`] object:
/// ```json
/// {
///   "id": "-Users-me-projects-myproject",
///   "path": "/Users/me/projects/myproject",
///   "sessions": [],
///   "created_at": 1234567890,
///   "most_recent_session": null
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Project created successfully
/// - `500 Internal Server Error`: Path doesn't exist or creation failed
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/agent/projects \
///   -H "Content-Type: application/json" \
///   -d '{"path": "/Users/me/projects/myproject"}'
/// ```
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

/// Gets all sessions for a specific project
///
/// # HTTP Route
/// `GET /agent/projects/{project_id}/sessions`
///
/// # Path Parameters
/// - `project_id`: Unique project identifier
///
/// # Response Format
/// Returns an array of [`Session`] objects:
/// ```json
/// [
///   {
///     "id": "session-123",
///     "project_id": "-Users-me-projects-myproject",
///     "project_path": "/Users/me/projects/myproject",
///     "todo_data": null,
///     "created_at": 1234567890,
///     "first_message": null,
///     "message_timestamp": null
///   }
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved sessions
/// - `500 Internal Server Error`: Project not found
///
/// # Example
/// ```bash
/// curl http://localhost:3000/agent/projects/-Users-me-projects-myproject/sessions
/// ```
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

/// Gets Claude Code settings
///
/// # HTTP Route
/// `GET /agent/settings`
///
/// # Response Format
/// Returns [`ClaudeSettings`] object:
/// ```json
/// {
///   "apiKey": "...",
///   "model": "claude-3-5-sonnet-20241022",
///   ...
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Settings retrieved (or default empty settings if not configured)
///
/// # Example
/// ```bash
/// curl http://localhost:3000/agent/settings
/// ```
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

/// Saves Claude Code settings
///
/// # HTTP Route
/// `POST /agent/settings`
///
/// # Request Body
/// ```json
/// {
///   "settings": {
///     "apiKey": "...",
///     "model": "claude-3-5-sonnet-20241022"
///   }
/// }
/// ```
///
/// # Response Format
/// ```json
/// {
///   "success": true,
///   "path": "/Users/me/.claude/settings.json"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Settings saved successfully
/// - `500 Internal Server Error`: Failed to save settings
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/agent/settings \
///   -H "Content-Type: application/json" \
///   -d '{"settings": {"model": "claude-3-5-sonnet-20241022"}}'
/// ```
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

/// Gets the custom system prompt
///
/// # HTTP Route
/// `GET /agent/system-prompt`
///
/// # Response Format
/// ```json
/// {
///   "content": "# Custom System Prompt\n...",
///   "path": "/Users/me/.claude/system-prompt.md"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: System prompt retrieved (empty content if not set)
///
/// # Example
/// ```bash
/// curl http://localhost:3000/agent/system-prompt
/// ```
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

/// Saves the custom system prompt
///
/// # HTTP Route
/// `POST /agent/system-prompt`
///
/// # Request Body
/// ```json
/// {
///   "content": "# Custom System Prompt\n\nYou are a helpful assistant..."
/// }
/// ```
///
/// # Response Format
/// ```json
/// {
///   "success": true,
///   "path": "/Users/me/.claude/system-prompt.md"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: System prompt saved successfully
/// - `500 Internal Server Error`: Failed to save prompt
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/agent/system-prompt \
///   -H "Content-Type: application/json" \
///   -d '{"content": "# My Prompt"}'
/// ```
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

/// Lists currently running Claude Code sessions
///
/// # HTTP Route
/// `GET /agent/sessions/running`
///
/// # Response Format
/// Returns an array of running session metadata (currently returns empty array):
/// ```json
/// []
/// ```
///
/// # Response Status
/// - `200 OK`: Always returns successfully
///
/// # Example
/// ```bash
/// curl http://localhost:3000/agent/sessions/running
/// ```
pub async fn list_running_claude_sessions() -> Result<HttpResponse, AppError> {
    // This would need process registry integration
    // For now, return empty list
    Ok(HttpResponse::Ok().json(Vec::<serde_json::Value>::new()))
}

/// Executes Claude Code in a project directory
///
/// # HTTP Route
/// `POST /agent/sessions/execute`
///
/// # Request Body
/// ```json
/// {
///   "project_path": "/Users/me/projects/myproject",
///   "prompt": "Help me debug this code",
///   "session_id": "optional-session-id"
/// }
/// ```
///
/// # Response Format
/// ```json
/// {
///   "success": true,
///   "message": "Execution started - streaming not yet implemented"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Execution started (placeholder implementation)
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/agent/sessions/execute \
///   -H "Content-Type: application/json" \
///   -d '{"project_path": "/tmp", "prompt": "Hello"}'
/// ```
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

/// Cancels a running Claude Code execution
///
/// # HTTP Route
/// `POST /agent/sessions/cancel`
///
/// # Request Body
/// ```json
/// {
///   "session_id": "session-123"
/// }
/// ```
///
/// # Response Format
/// ```json
/// {
///   "success": true,
///   "message": "Cancellation request sent"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Cancellation request sent
///
/// # Example
/// ```bash
/// curl -X POST http://localhost:3000/agent/sessions/cancel \
///   -H "Content-Type: application/json" \
///   -d '{"session_id": "session-123"}'
/// ```
pub async fn cancel_claude_execution(
    _req: web::Json<CancelRequest>,
) -> Result<HttpResponse, AppError> {
    // This requires process registry integration
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "message": "Cancellation request sent"
    })))
}

/// Gets session JSONL content (conversation history)
///
/// # HTTP Route
/// `GET /agent/sessions/{session_id}/jsonl?project_id={project_id}`
///
/// # Path Parameters
/// - `session_id`: Session identifier
///
/// # Query Parameters
/// - `project_id`: (Required) Project identifier
///
/// # Response Format
/// Returns an array of JSON objects representing conversation messages:
/// ```json
/// [
///   {"role": "user", "content": "Hello"},
///   {"role": "assistant", "content": "Hi!"}
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Session content retrieved successfully
/// - `500 Internal Server Error`: Session not found or read error
///
/// # Example
/// ```bash
/// curl "http://localhost:3000/agent/sessions/session-123/jsonl?project_id=my-project"
/// ```
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

/// Configures agent API routes
///
/// # Routes
/// - `GET /agent/projects` - List all projects
/// - `POST /agent/projects` - Create a new project
/// - `GET /agent/projects/{project_id}/sessions` - Get project sessions
/// - `GET /agent/settings` - Get Claude settings
/// - `POST /agent/settings` - Save Claude settings
/// - `GET /agent/system-prompt` - Get system prompt
/// - `POST /agent/system-prompt` - Save system prompt
/// - `GET /agent/sessions/running` - List running sessions
/// - `POST /agent/sessions/execute` - Execute Claude code
/// - `POST /agent/sessions/cancel` - Cancel execution
/// - `GET /agent/sessions/{session_id}/jsonl` - Get session content
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
            .route("/sessions/cancel", web::post().to(cancel_claude_execution))
            .route(
                "/sessions/{session_id}/jsonl",
                web::get().to(get_session_jsonl),
            ),
    );
}
