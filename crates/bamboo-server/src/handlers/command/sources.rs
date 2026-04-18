use std::path::Path;

use bamboo_application_skills::SkillDefinition;
use crate::app_state::AppState;
use crate::error::AppError;

use super::types::CommandItem;

pub(super) async fn list_workflows_as_commands(
    data_dir: &Path,
) -> Result<Vec<CommandItem>, AppError> {
    let dir = data_dir.join("workflows");
    tokio::fs::create_dir_all(&dir).await.map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to create workflows dir: {error}"))
    })?;

    let mut entries = tokio::fs::read_dir(&dir).await.map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to read workflows dir: {error}"))
    })?;

    let mut commands = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to read entry: {error}"))
    })? {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("md") {
            continue;
        }

        let name = path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_string();
        if name.is_empty() {
            continue;
        }

        let metadata = entry.metadata().await.map_err(|error| {
            AppError::InternalError(anyhow::anyhow!("Failed to read metadata: {error}"))
        })?;
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_string();

        commands.push(CommandItem {
            id: format!("workflow-{name}"),
            name: name.clone(),
            display_name: name.clone(),
            description: format!("Workflow: {name}"),
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

pub(super) fn skill_to_command(skill: &SkillDefinition) -> CommandItem {
    CommandItem {
        id: format!("skill-{}", skill.id),
        name: skill.id.clone(),
        display_name: skill.name.clone(),
        description: skill.description.clone(),
        command_type: "skill".to_string(),
        category: None,
        tags: None,
        metadata: serde_json::json!({
            "prompt": skill.prompt,
            "toolRefs": skill.tool_refs,
            "license": skill.license,
            "compatibility": skill.compatibility,
            "metadata": skill.metadata,
        }),
    }
}

pub(super) async fn list_mcp_tools_as_commands(
    state: &AppState,
) -> Result<Vec<CommandItem>, AppError> {
    let aliases = state.mcp_manager.tool_index().all_aliases();
    let commands = aliases
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
