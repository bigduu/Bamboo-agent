use std::collections::HashSet;

use actix_web::{web, HttpResponse};

use crate::app_state::AppState;
use crate::error::AppError;
use crate::handlers::settings::is_safe_workflow_name;

use super::sources::{
    legacy_workflow_catalog_entry_to_command, list_markdown_commands, list_mcp_tools_as_commands,
    list_prompt_presets_as_commands, safe_project_commands_dir, skill_catalog_entry_to_command,
};
use super::types::{CommandItem, CommandListResponse, GetCommandQuery, ListCommandsQuery};

struct SessionResourceContext {
    workspace: Option<std::path::PathBuf>,
    project_id: Option<bamboo_domain::ProjectId>,
    project_home: Option<std::path::PathBuf>,
}

pub(super) fn append_unique(
    commands: &mut Vec<CommandItem>,
    seen: &mut HashSet<(String, String)>,
    items: Vec<CommandItem>,
) {
    for item in items {
        if seen.insert((item.command_type.clone(), item.name.clone())) {
            commands.push(item);
        }
    }
}

pub(super) fn expand_arguments(template: &str, arguments: &str) -> String {
    template.replace("$ARGUMENTS", arguments)
}

async fn session_resource_context(
    app_state: &AppState,
    session_id: Option<&str>,
    legacy_workspace_path: Option<&str>,
) -> Result<SessionResourceContext, AppError> {
    if legacy_workspace_path.is_some_and(|value| !value.trim().is_empty()) {
        return Err(AppError::BadRequest(
            "workspace_path is deprecated; provide session_id so workspace access is session-bound"
                .to_string(),
        ));
    }
    let Some(session_id) = session_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(SessionResourceContext {
            workspace: None,
            project_id: None,
            project_home: None,
        });
    };
    let session = app_state
        .load_session(session_id)
        .await
        .ok_or_else(|| AppError::NotFound(format!("Session '{session_id}'")))?;
    let project_id =
        match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(
            &session,
        ) {
            bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => {
                Some(project_id)
            }
            bamboo_engine::project_context::SessionProjectIdentity::Unassigned => None,
            bamboo_engine::project_context::SessionProjectIdentity::Invalid { raw, message } => {
                return Err(AppError::BadRequest(format!(
                    "Session carries an invalid Project identity '{raw}': {message}"
                )));
            }
        };
    let persisted_workspace = (session
        .metadata
        .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
        .map(String::as_str)
        != Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str()))
    .then(|| session.workspace_path_meta())
    .flatten();
    let workspace = crate::project_context::validate_workspace_assignment_with_resolver(
        &app_state.project_store,
        project_id.as_ref(),
        persisted_workspace.as_deref(),
        &app_state.workspace_resolver,
    )
    .map_err(|error| match error {
        crate::project_context::ProjectWorkspaceValidationError::Invalid { .. }
        | crate::project_context::ProjectWorkspaceValidationError::Conflict { .. } => {
            AppError::BadRequest(error.to_string())
        }
        crate::project_context::ProjectWorkspaceValidationError::Store(error) => {
            AppError::InternalError(anyhow::anyhow!(error))
        }
    })?;
    let project_home = if let Some(project_id) = project_id.as_ref() {
        app_state.project_store.get(project_id).map_err(|error| {
            AppError::BadRequest(format!("Assigned Project is unavailable: {error}"))
        })?;
        Some(app_state.project_store.paths().project_home(project_id))
    } else {
        None
    };
    Ok(SessionResourceContext {
        workspace,
        project_id,
        project_home,
    })
}

async fn scoped_skill_store(
    app_state: &AppState,
    context: &SessionResourceContext,
) -> Result<std::sync::Arc<bamboo_skills::SkillStore>, AppError> {
    if let (Some(project_id), Some(project_home)) =
        (context.project_id.as_ref(), context.project_home.as_ref())
    {
        app_state
            .skill_manager
            .store_for_project_workspace(project_id, project_home, context.workspace.as_deref())
            .await
    } else {
        app_state
            .skill_manager
            .store_for_workspace(context.workspace.as_deref())
            .await
    }
    .map_err(|error| AppError::BadRequest(format!("Invalid session resource scope: {error}")))
}

/// Lists all available commands from workflows, skills, and MCP tools.
pub async fn list_commands(
    app_state: web::Data<AppState>,
    query: web::Query<ListCommandsQuery>,
) -> Result<HttpResponse, AppError> {
    let mut commands = Vec::new();
    let mut seen = HashSet::new();
    let context = session_resource_context(
        app_state.get_ref(),
        query.session_id.as_deref(),
        query.workspace_path.as_deref(),
    )
    .await?;

    // Conflict precedence is deliberately source-based and stable:
    // workspace markdown > Project markdown > global markdown > global preset
    // > workflow/skill catalog > MCP.
    if let Some(workspace_path) = context.workspace.as_ref() {
        if let Some(dir) = safe_project_commands_dir(workspace_path.to_string_lossy().as_ref()) {
            let workspace_commands = list_markdown_commands(&dir, "workspace")
                .await
                .into_iter()
                .map(|command| command.item)
                .collect();
            append_unique(&mut commands, &mut seen, workspace_commands);
        }
    }
    if let Some(project_id) = context.project_id.as_ref() {
        let project = list_markdown_commands(
            &app_state.project_store.paths().commands_dir(project_id),
            "project",
        )
        .await
        .into_iter()
        .map(|command| command.item)
        .collect();
        append_unique(&mut commands, &mut seen, project);
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

    let store = scoped_skill_store(app_state.get_ref(), &context).await?;
    let (skill_catalog, workflow_catalog) = store.command_catalog_snapshots().await;
    let skill_commands = skill_catalog
        .entries
        .into_iter()
        .filter_map(|entry| skill_catalog_entry_to_command(&entry))
        .collect();
    append_unique(&mut commands, &mut seen, skill_commands);
    let workflow_commands = workflow_catalog
        .entries
        .into_iter()
        .filter_map(|entry| legacy_workflow_catalog_entry_to_command(&entry))
        .collect();
    append_unique(&mut commands, &mut seen, workflow_commands);

    match list_mcp_tools_as_commands(app_state.get_ref()).await {
        Ok(mcp_tools) => append_unique(&mut commands, &mut seen, mcp_tools),
        Err(error) => {
            tracing::warn!("Failed to load MCP tools: {error}");
        }
    }

    commands.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.command_type.cmp(&right.command_type))
    });
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
    let context = session_resource_context(
        app_state.get_ref(),
        query.session_id.as_deref(),
        query.workspace_path.as_deref(),
    )
    .await?;

    match command_type.as_str() {
        "prompt" => {
            let mut sources = Vec::new();
            if let Some(workspace_path) = context.workspace.as_ref() {
                if let Some(dir) =
                    safe_project_commands_dir(workspace_path.to_string_lossy().as_ref())
                {
                    sources.push((dir, "workspace"));
                }
            }
            if let Some(project_id) = context.project_id.as_ref() {
                sources.push((
                    app_state.project_store.paths().commands_dir(project_id),
                    "project",
                ));
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

            let store = scoped_skill_store(app_state.get_ref(), &context).await?;
            let filepath = store
                .get_legacy_workflow_source(&id)
                .await
                .map_err(|_| AppError::NotFound(format!("Workflow {id} not found")))?;
            let content = bamboo_skills::legacy::read_legacy_markdown_workflow(&filepath)
                .await
                .map_err(|_| AppError::NotFound(format!("Workflow {id} not found")))?;

            Ok(HttpResponse::Ok().json(serde_json::json!({
                "id": format!("workflow-{id}"),
                "name": id,
                "content": content,
                "type": "workflow"
            })))
        }
        "skill" => {
            let store = scoped_skill_store(app_state.get_ref(), &context).await?;
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
