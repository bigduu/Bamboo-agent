use std::path::{Component, Path, PathBuf};

use crate::app_state::AppState;
use crate::error::AppError;
use bamboo_skills::{
    LegacyWorkflowMigrationStatus, WorkflowCatalogEntry, WorkflowKind, WorkflowStatus,
};
use serde::Deserialize;

use super::types::CommandItem;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct CommandFrontmatter {
    description: Option<String>,
    argument_hint: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct MarkdownCommand {
    pub item: CommandItem,
    pub content: String,
}

fn command_name(root: &Path, path: &Path) -> Option<String> {
    let relative = path.strip_prefix(root).ok()?.with_extension("");
    let mut parts = Vec::new();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            return None;
        };
        let part = part.to_str()?.trim();
        if part.is_empty() || part == "." || part == ".." {
            return None;
        }
        parts.push(part);
    }
    (!parts.is_empty()).then(|| parts.join("/"))
}

fn parse_command_markdown(raw: &str) -> (CommandFrontmatter, String) {
    let normalized = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    let normalized_storage;
    let normalized = if normalized.contains("\r\n") {
        normalized_storage = normalized.replace("\r\n", "\n");
        normalized_storage.as_str()
    } else {
        normalized
    };
    if let Some(rest) = normalized.strip_prefix("---\n") {
        if let Some((frontmatter, body)) = rest.split_once("\n---\n") {
            match serde_yaml::from_str::<CommandFrontmatter>(frontmatter) {
                Ok(metadata) => return (metadata, body.trim().to_string()),
                Err(error) => {
                    tracing::warn!("Ignoring invalid command frontmatter: {error}");
                    return (CommandFrontmatter::default(), body.trim().to_string());
                }
            }
        }
    }
    (CommandFrontmatter::default(), normalized.trim().to_string())
}

pub(super) fn safe_project_commands_dir(workspace_path: &str) -> Option<PathBuf> {
    let workspace = Path::new(workspace_path);
    if !workspace.is_absolute() {
        tracing::warn!("Ignoring relative workspace_path for command discovery");
        return None;
    }
    let workspace = std::fs::canonicalize(workspace).ok()?;
    if !workspace.is_dir() {
        return None;
    }
    // A session may work in a repository subdirectory. Resolve `<project>` as
    // the nearest Git boundary; linked worktrees use a regular `.git` file.
    let mut project = workspace.clone();
    let mut cursor = workspace.as_path();
    loop {
        let marker = cursor.join(".git");
        let is_git_boundary = std::fs::symlink_metadata(&marker)
            .map(|metadata| {
                !metadata.file_type().is_symlink()
                    && (metadata.file_type().is_dir() || metadata.file_type().is_file())
            })
            .unwrap_or(false);
        if is_git_boundary {
            project = cursor.to_path_buf();
            break;
        }
        let Some(parent) = cursor.parent() else {
            break;
        };
        cursor = parent;
    }
    let commands = bamboo_config::paths::project_commands_dir(&project);
    if !commands.exists() {
        return Some(commands);
    }
    let canonical_commands = std::fs::canonicalize(&commands).ok()?;
    canonical_commands
        .starts_with(&project)
        .then_some(canonical_commands)
        .or_else(|| {
            tracing::warn!(
                "Ignoring project commands directory outside workspace boundary: {}",
                commands.display()
            );
            None
        })
}

async fn find_markdown_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut entries = match tokio::fs::read_dir(root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return files,
        Err(error) => {
            tracing::warn!(
                "Failed to read command directory {}: {error}",
                root.display()
            );
            return files;
        }
    };
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(error) => {
                tracing::warn!("Failed while scanning command directory: {error}");
                break;
            }
        };
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            files.extend(Box::pin(find_markdown_files(&path)).await);
        } else if file_type.is_file()
            && path.extension().and_then(|extension| extension.to_str()) == Some("md")
        {
            files.push(path);
        }
    }
    files.sort();
    files
}

pub(super) async fn list_markdown_commands(root: &Path, source: &str) -> Vec<MarkdownCommand> {
    let mut commands = Vec::new();
    for path in find_markdown_files(root).await {
        let Some(name) = command_name(root, &path) else {
            continue;
        };
        let raw = match tokio::fs::read_to_string(&path).await {
            Ok(raw) => raw,
            Err(error) => {
                tracing::warn!("Failed to read command {}: {error}", path.display());
                continue;
            }
        };
        let (metadata, content) = parse_command_markdown(&raw);
        if content.is_empty() {
            continue;
        }
        let description = metadata
            .description
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| format!("{source} command: {name}"));
        let argument_hint = metadata
            .argument_hint
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let item = CommandItem {
            id: format!("command-{source}-{}", name.replace('/', "-")),
            name: name.clone(),
            display_name: name,
            description,
            command_type: "prompt".to_string(),
            category: Some("Commands".to_string()),
            tags: None,
            metadata: serde_json::json!({
                "source": source,
                "argumentHint": argument_hint,
                "prompt": content,
            }),
        };
        commands.push(MarkdownCommand { item, content });
    }
    commands
}

pub(super) async fn list_prompt_presets_as_commands(data_dir: &Path) -> Vec<CommandItem> {
    match crate::handlers::agent::prompt_presets::load_stored_presets(data_dir).await {
        Ok(presets) => presets
            .into_iter()
            .map(|preset| CommandItem {
                id: format!("preset-{}", preset.id),
                name: preset.id,
                display_name: preset.name,
                description: preset
                    .description
                    .unwrap_or_else(|| "Prompt preset".to_string()),
                command_type: "prompt".to_string(),
                category: Some("Prompt Presets".to_string()),
                tags: None,
                metadata: serde_json::json!({"source": "preset", "prompt": preset.content}),
            })
            .collect(),
        Err(error) => {
            tracing::warn!("Failed to load prompt presets as commands: {error}");
            Vec::new()
        }
    }
}

pub(super) fn skill_catalog_entry_to_command(entry: &WorkflowCatalogEntry) -> Option<CommandItem> {
    (entry.winner && entry.status == WorkflowStatus::Valid).then(|| CommandItem {
        id: format!("skill-{}", entry.id),
        name: entry.id.clone(),
        display_name: entry.name.clone(),
        description: entry.description.clone(),
        command_type: "skill".to_string(),
        category: None,
        tags: None,
        metadata: serde_json::json!({
            "kind": entry.kind,
            "source": entry.source,
            "revision": entry.revision,
            "version": entry.version,
            "invocationPolicy": entry.invocation_policy,
            "argumentSchema": entry.argument_schema,
            "status": entry.status,
        }),
    })
}

pub(super) fn legacy_workflow_catalog_entry_to_command(
    entry: &WorkflowCatalogEntry,
) -> Option<CommandItem> {
    (entry.winner
        && entry.kind == WorkflowKind::Instruction
        && entry.status == WorkflowStatus::Valid
        && entry.migration_status == Some(LegacyWorkflowMigrationStatus::Available))
    .then(|| CommandItem {
        id: format!("workflow-{}", entry.id),
        name: entry.id.clone(),
        display_name: entry.name.clone(),
        description: entry.description.clone(),
        command_type: "workflow".to_string(),
        category: None,
        tags: None,
        metadata: serde_json::json!({
            "kind": entry.kind,
            "source": entry.source,
            "revision": entry.revision,
            "version": entry.version,
            "invocationPolicy": entry.invocation_policy,
            "argumentSchema": entry.argument_schema,
            "status": entry.status,
            "migrationStatus": entry.migration_status,
        }),
    })
}

pub(super) async fn list_mcp_tools_as_commands(
    state: &AppState,
) -> Result<Vec<CommandItem>, AppError> {
    let snapshot = state.mcp_manager.snapshot();
    let aliases = snapshot.aliases();
    let commands = aliases
        .into_iter()
        .filter_map(|alias| {
            snapshot
                .tool(&alias.server_id, &alias.original_name)
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
