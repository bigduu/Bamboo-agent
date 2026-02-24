use crate::agent::server::state::AppState as AgentAppState;
use crate::agent::skill::SkillDefinition;
use crate::server::error::AppError;
use crate::server::app_state::AppState;
use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};

/// Command type enumeration for categorizing different command sources
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum CommandType {
    /// Workflow commands from markdown files
    Workflow,
    /// Skill commands defined in the skill system
    Skill,
    /// MCP (Model Context Protocol) tool commands
    Mcp,
}

/// Represents a unified command item from various sources (workflows, skills, MCP tools)
#[derive(Debug, Serialize)]
pub struct CommandItem {
    /// Unique identifier for the command (e.g., "workflow-myworkflow", "skill-myskill", "mcp-server-tool")
    pub id: String,
    /// Short name/identifier for the command
    pub name: String,
    /// Human-readable display name
    pub display_name: String,
    /// Description of what the command does
    pub description: String,
    /// Type of command: "workflow", "skill", or "mcp"
    #[serde(rename = "type")]
    pub command_type: String,
    /// Optional category for grouping commands
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    /// Optional tags for filtering and search
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    /// Additional metadata specific to the command type
    pub metadata: serde_json::Value,
}

/// Response structure for listing all available commands
#[derive(Debug, Serialize)]
pub struct CommandListResponse {
    /// List of all available commands
    pub commands: Vec<CommandItem>,
    /// Total number of commands
    pub total: usize,
}

/// Lists all available commands from workflows, skills, and MCP tools
///
/// # HTTP Route
/// `GET /commands`
///
/// # Response Format
/// Returns a [`CommandListResponse`] containing all available commands:
/// ```json
/// {
///   "commands": [
///     {
///       "id": "workflow-myworkflow",
///       "name": "myworkflow",
///       "display_name": "myworkflow",
///       "description": "Workflow: myworkflow",
///       "type": "workflow",
///       "metadata": { ... }
///     }
///   ],
///   "total": 10
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved command list
///
/// # Example
/// ```bash
/// curl http://localhost:3000/commands
/// ```
pub async fn list_commands(
    app_state: web::Data<AppState>,
    agent_state: web::Data<AgentAppState>,
) -> Result<HttpResponse, AppError> {
    let mut commands = Vec::new();

    // 1. Load Workflows
    match list_workflows_as_commands(&app_state.app_data_dir).await {
        Ok(workflows) => commands.extend(workflows),
        Err(e) => {
            log::warn!("Failed to load workflows: {}", e);
            // Continue loading other types, don't interrupt
        }
    }

    // 2. Load Skills
    let skills = agent_state
        .skill_manager
        .store()
        .list_skills(None, false)
        .await;

    let skill_commands: Vec<CommandItem> = skills
        .into_iter()
        .map(|skill| skill_to_command(&skill))
        .collect();
    commands.extend(skill_commands);

    // 3. Load MCP Tools
    match list_mcp_tools_as_commands(&agent_state).await {
        Ok(mcp_tools) => commands.extend(mcp_tools),
        Err(e) => {
            log::warn!("Failed to load MCP tools: {}", e);
        }
    }

    // Sort by name
    commands.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(HttpResponse::Ok().json(CommandListResponse {
        total: commands.len(),
        commands,
    }))
}

/// Retrieves a specific command by type and ID
///
/// # HTTP Route
/// `GET /commands/{command_type}/{id}`
///
/// # Path Parameters
/// - `command_type`: Type of command ("workflow", "skill", or "mcp")
/// - `id`: Unique identifier of the command
///
/// # Response Format
/// Returns command details including content (for workflows and skills):
/// ```json
/// {
///   "id": "workflow-myworkflow",
///   "name": "myworkflow",
///   "content": "# My Workflow\n...",
///   "type": "workflow"
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Command found and returned
/// - `404 Not Found`: Command not found or MCP tool (which doesn't support content retrieval)
///
/// # Example
/// ```bash
/// curl http://localhost:3000/commands/workflow/myworkflow
/// curl http://localhost:3000/commands/skill/myskill
/// ```
pub async fn get_command(
    app_state: web::Data<AppState>,
    agent_state: web::Data<AgentAppState>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, AppError> {
    let (command_type, id) = path.into_inner();

    match command_type.as_str() {
        "workflow" => {
            // Call existing workflow retrieval logic
            let workflows_dir = app_state.app_data_dir.join("workflows");
            let filename = format!("{}.md", id);
            let filepath = workflows_dir.join(&filename);

            if !filepath.exists() {
                return Err(AppError::NotFound(format!("Workflow {} not found", id)));
            }

            let content = tokio::fs::read_to_string(&filepath).await.map_err(|e| {
                AppError::InternalError(anyhow::anyhow!("Failed to read workflow: {}", e))
            })?;

            Ok(HttpResponse::Ok().json(serde_json::json!({
                "id": format!("workflow-{}", id),
                "name": id,
                "content": content,
                "type": "workflow"
            })))
        }
        "skill" => match agent_state.skill_manager.store().get_skill(&id).await {
            Ok(skill) => Ok(HttpResponse::Ok().json(skill)),
            Err(e) => Err(AppError::NotFound(format!("Skill {} not found: {}", id, e))),
        },
        "mcp" => {
            // MCP tools don't need separate content retrieval
            Err(AppError::NotFound(
                "MCP tools do not support content retrieval".to_string(),
            ))
        }
        _ => Err(AppError::NotFound(format!(
            "Unknown command type: {}",
            command_type
        ))),
    }
}

/// Internal helper to list all workflow markdown files as command items
///
/// Scans the workflows directory and creates command items for each `.md` file
async fn list_workflows_as_commands(
    data_dir: &std::path::Path,
) -> Result<Vec<CommandItem>, AppError> {
    let dir = data_dir.join("workflows");
    tokio::fs::create_dir_all(&dir).await.map_err(|e| {
        AppError::InternalError(anyhow::anyhow!("Failed to create workflows dir: {}", e))
    })?;

    let mut entries = tokio::fs::read_dir(&dir).await.map_err(|e| {
        AppError::InternalError(anyhow::anyhow!("Failed to read workflows dir: {}", e))
    })?;

    let mut commands = Vec::new();

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| AppError::InternalError(anyhow::anyhow!("Failed to read entry: {}", e)))?
    {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("md") {
            continue;
        }

        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();

        if name.is_empty() {
            continue;
        }

        let metadata = entry.metadata().await.map_err(|e| {
            AppError::InternalError(anyhow::anyhow!("Failed to read metadata: {}", e))
        })?;

        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();

        commands.push(CommandItem {
            id: format!("workflow-{}", name),
            name: name.clone(),
            display_name: name.clone(),
            description: format!("Workflow: {}", name),
            command_type: "workflow".to_string(),
            category: None,
            tags: None,
            metadata: serde_json::json!({
                "filename": filename,
                "size": metadata.len(),
                "source": "global"
            }),
        });
    }

    Ok(commands)
}

/// Internal helper to convert a skill definition to a command item
fn skill_to_command(skill: &SkillDefinition) -> CommandItem {
    CommandItem {
        id: format!("skill-{}", skill.id),
        name: skill.id.clone(),
        display_name: skill.name.clone(),
        description: skill.description.clone(),
        command_type: "skill".to_string(),
        category: Some(skill.category.clone()),
        tags: Some(skill.tags.clone()),
        metadata: serde_json::json!({
            "prompt": skill.prompt,
            "toolRefs": skill.tool_refs,
            "workflowRefs": skill.workflow_refs,
            "visibility": skill.visibility,
        }),
    }
}

/// Internal helper to list all MCP tools as command items
///
/// Retrieves all tool aliases from the MCP manager and converts them to command items
async fn list_mcp_tools_as_commands(state: &AgentAppState) -> Result<Vec<CommandItem>, AppError> {
    let aliases = state.mcp_manager.tool_index().all_aliases();

    let commands: Vec<CommandItem> = aliases
        .into_iter()
        .filter_map(|alias| {
            state
                .mcp_manager
                .get_tool_info(&alias.server_id, &alias.original_name)
                .map(|tool| CommandItem {
                    id: format!("mcp-{}-{}", alias.server_id, alias.original_name),
                    name: alias.alias.clone(),
                    display_name: alias.alias.clone(),
                    description: tool.description.clone(),
                    command_type: "mcp".to_string(),
                    category: Some("MCP Tools".to_string()),
                    tags: None,
                    metadata: serde_json::json!({
                        "serverId": alias.server_id,
                        "originalName": alias.original_name,
                    }),
                })
        })
        .collect();

    Ok(commands)
}

/// Configures command-related routes
///
/// # Routes
/// - `GET /commands` - List all commands
/// - `GET /commands/{command_type}/{id}` - Get specific command details
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/commands", web::get().to(list_commands))
        .route("/commands/{command_type}/{id}", web::get().to(get_command));
}
