use std::collections::HashSet;

use actix_web::{web, HttpResponse};

use crate::app_state::AppState;
use crate::error::AppError;
use crate::handlers::settings::is_safe_workflow_name;

use super::sources::{
    catalog_entry_to_command, list_markdown_commands, list_mcp_tools_as_commands,
    list_prompt_presets_as_commands, safe_project_commands_dir,
};
use super::types::{CommandItem, CommandListResponse, GetCommandQuery, ListCommandsQuery};

pub(super) fn append_unique(
    commands: &mut Vec<CommandItem>,
    seen: &mut HashSet<String>,
    items: Vec<CommandItem>,
) {
    for item in items {
        if seen.insert(item.name.clone()) {
            commands.push(item);
        }
    }
}

pub(super) fn expand_arguments(template: &str, arguments: &str) -> String {
    template.replace("$ARGUMENTS", arguments)
}

async fn session_workspace(
    app_state: &AppState,
    session_id: Option<&str>,
    legacy_workspace_path: Option<&str>,
) -> Result<Option<std::path::PathBuf>, AppError> {
    if legacy_workspace_path.is_some_and(|value| !value.trim().is_empty()) {
        return Err(AppError::BadRequest(
            "workspace_path is deprecated; provide session_id so workspace access is session-bound"
                .to_string(),
        ));
    }
    let Some(session_id) = session_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let session = app_state
        .load_session(session_id)
        .await
        .ok_or_else(|| AppError::NotFound(format!("Session '{session_id}'")))?;
    let Some(workspace) = session.workspace_path_meta() else {
        return Ok(None);
    };
    let workspace = tokio::fs::canonicalize(workspace)
        .await
        .map_err(|error| AppError::BadRequest(format!("Invalid session workspace: {error}")))?;
    if !workspace.is_dir() {
        return Err(AppError::BadRequest(
            "Session workspace must be a directory".to_string(),
        ));
    }
    Ok(Some(workspace))
}

/// Lists all available commands from workflows, skills, and MCP tools.
pub async fn list_commands(
    app_state: web::Data<AppState>,
    query: web::Query<ListCommandsQuery>,
) -> Result<HttpResponse, AppError> {
    let mut commands = Vec::new();
    let mut seen = HashSet::new();
    let workspace = session_workspace(
        app_state.get_ref(),
        query.session_id.as_deref(),
        query.workspace_path.as_deref(),
    )
    .await?;

    // Conflict precedence is deliberately source-based and stable:
    // project markdown > global markdown > global preset > workflow > skill > MCP.
    if let Some(workspace_path) = workspace.as_ref() {
        if let Some(dir) = safe_project_commands_dir(workspace_path.to_string_lossy().as_ref()) {
            let project = list_markdown_commands(&dir, "project")
                .await
                .into_iter()
                .map(|command| command.item)
                .collect();
            append_unique(&mut commands, &mut seen, project);
        }
    }

    let global_dir = bamboo_config::paths::commands_dir_in(&app_state.app_data_dir);
    let global = list_markdown_commands(&global_dir, "global")
        .await
        .into_iter()
        .map(|command| command.item)
        .collect();
    append_unique(&mut commands, &mut seen, global);

    append_unique(
        &mut commands,
        &mut seen,
        list_prompt_presets_as_commands(&app_state.app_data_dir).await,
    );

    let catalog = if let Some(workspace) = workspace.as_ref() {
        app_state
            .skill_manager
            .store()
            .workflow_catalog_for_workspace(workspace)
            .await
            .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?
    } else {
        app_state
            .skill_manager
            .store()
            .workflow_catalog_snapshot()
            .await
    };
    let skill_commands = catalog
        .entries
        .into_iter()
        .map(|entry| catalog_entry_to_command(&entry))
        .collect();
    append_unique(&mut commands, &mut seen, skill_commands);

    match list_mcp_tools_as_commands(app_state.get_ref()).await {
        Ok(mcp_tools) => append_unique(&mut commands, &mut seen, mcp_tools),
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
    query: web::Query<GetCommandQuery>,
) -> Result<HttpResponse, AppError> {
    let (command_type, id) = path.into_inner();
    let workspace = session_workspace(
        app_state.get_ref(),
        query.session_id.as_deref(),
        query.workspace_path.as_deref(),
    )
    .await?;

    match command_type.as_str() {
        "prompt" => {
            let mut sources = Vec::new();
            if let Some(workspace_path) = workspace.as_ref() {
                if let Some(dir) =
                    safe_project_commands_dir(workspace_path.to_string_lossy().as_ref())
                {
                    sources.push((dir, "project"));
                }
            }
            sources.push((
                bamboo_config::paths::commands_dir_in(&app_state.app_data_dir),
                "global",
            ));

            for (dir, source) in sources {
                if let Some(command) = list_markdown_commands(&dir, source)
                    .await
                    .into_iter()
                    .find(|command| command.item.name == id)
                {
                    let arguments = query.arguments.as_deref().unwrap_or_default();
                    let content = expand_arguments(&command.content, arguments);
                    return Ok(HttpResponse::Ok().json(serde_json::json!({
                        "id": command.item.id,
                        "name": command.item.name,
                        "content": content,
                        "type": "prompt",
                        "metadata": command.item.metadata,
                    })));
                }
            }
            if let Some(preset) = list_prompt_presets_as_commands(&app_state.app_data_dir)
                .await
                .into_iter()
                .find(|command| command.name == id)
            {
                let arguments = query.arguments.as_deref().unwrap_or_default();
                let content = preset.metadata["prompt"].as_str().unwrap_or_default();
                return Ok(HttpResponse::Ok().json(serde_json::json!({
                    "id": preset.id,
                    "name": preset.name,
                    "content": expand_arguments(content, arguments),
                    "type": "prompt",
                    "metadata": preset.metadata,
                })));
            }
            Err(AppError::NotFound(format!("Prompt command {id} not found")))
        }
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
        "skill" => {
            let store = app_state
                .skill_manager
                .store_for_workspace(workspace.as_deref())
                .await
                .map_err(|error| {
                    AppError::BadRequest(format!("Invalid session workspace: {error}"))
                })?;
            match store.get_skill(&id).await {
                Ok(skill) => Ok(HttpResponse::Ok().json(skill)),
                Err(error) => Err(AppError::NotFound(format!("Skill {id} not found: {error}"))),
            }
        }
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
    cfg.route("/commands", web::get().to(list_commands)).route(
        "/commands/{command_type}/{id:.*}",
        web::get().to(get_command),
    );
}
