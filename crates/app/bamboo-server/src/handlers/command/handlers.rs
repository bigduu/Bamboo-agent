use actix_web::{web, HttpResponse};

use crate::app_state::AppState;
use crate::error::AppError;
use crate::handlers::settings::is_safe_workflow_name;

use super::sources::{list_mcp_tools_as_commands, list_workflows_as_commands, skill_to_command};
use super::types::CommandListResponse;

/// Lists all available commands from workflows, skills, and MCP tools.
pub async fn list_commands(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let mut commands = Vec::new();

    match list_workflows_as_commands(&app_state.app_data_dir).await {
        Ok(workflows) => commands.extend(workflows),
        Err(error) => {
            tracing::warn!("Failed to load workflows: {error}");
        }
    }

    let skills = app_state
        .skill_manager
        .store()
        .list_skills(None, false)
        .await;
    let skill_commands = skills.into_iter().map(|skill| skill_to_command(&skill));
    commands.extend(skill_commands);

    match list_mcp_tools_as_commands(app_state.get_ref()).await {
        Ok(mcp_tools) => commands.extend(mcp_tools),
        Err(error) => {
            tracing::warn!("Failed to load MCP tools: {error}");
        }
    }

    commands.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(HttpResponse::Ok().json(CommandListResponse {
        total: commands.len(),
        commands,
    }))
}

/// Retrieves a specific command by type and ID.
pub async fn get_command(
    app_state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, AppError> {
    let (command_type, id) = path.into_inner();

    match command_type.as_str() {
        "workflow" => {
            if !is_safe_workflow_name(&id) {
                return Err(AppError::BadRequest("Invalid workflow name".to_string()));
            }

            let workflows_dir = app_state.app_data_dir.join("workflows");
            let filename = format!("{id}.md");
            let filepath = workflows_dir.join(&filename);

            if !filepath.exists() {
                return Err(AppError::NotFound(format!("Workflow {id} not found")));
            }

            let content = tokio::fs::read_to_string(&filepath)
                .await
                .map_err(|error| {
                    AppError::InternalError(anyhow::anyhow!("Failed to read workflow: {error}"))
                })?;

            Ok(HttpResponse::Ok().json(serde_json::json!({
                "id": format!("workflow-{id}"),
                "name": id,
                "content": content,
                "type": "workflow"
            })))
        }
        "skill" => match app_state.skill_manager.store().get_skill(&id).await {
            Ok(skill) => Ok(HttpResponse::Ok().json(skill)),
            Err(error) => Err(AppError::NotFound(format!("Skill {id} not found: {error}"))),
        },
        "mcp" => Err(AppError::NotFound(
            "MCP tools do not support content retrieval".to_string(),
        )),
        _ => Err(AppError::NotFound(format!(
            "Unknown command type: {command_type}"
        ))),
    }
}

/// Configures command-related routes.
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/commands", web::get().to(list_commands))
        .route("/commands/{command_type}/{id}", web::get().to(get_command));
}
