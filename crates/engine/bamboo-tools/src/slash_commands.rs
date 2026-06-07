//! Slash command management for Bamboo agents.
//!
//! This module provides functionality for discovering, loading, and managing
//! slash commands from both project-level and user-level directories.
//!
//! # Command Discovery
//!
//! Commands are discovered from multiple locations:
//! - **Default commands**: Built-in commands like `/add-dir`, `/init`, `/review`
//! - **Project commands**: `.claude/commands/` directory in the project root
//! - **User commands**: `~/.claude/commands/` directory in the user's home
//!
//! # Command Format
//!
//! Commands are Markdown files with optional YAML frontmatter:
//!
//! ```markdown
//! ---
//! description: Build the project
//! allowed-tools:
//!   - execute_command
//!   - read_file
//! ---
//!
//! Run the following command to build:
//! !`cargo build --release`
//! ```
//!
//! # Namespaced Commands
//!
//! Commands can be organized in subdirectories to create namespaces:
//! - `commands/dev/build.md` → `/dev:build`
//! - `commands/team/review.md` → `/team:review`
//!
//! # Example
//!
//! ```rust,ignore
//! use bamboo_agent::commands::slash_commands::slash_commands_list;
//!
//! #[tokio::main]
//! async fn main() {
//!     // List all available commands
//!     let commands = slash_commands_list(Some("./my-project".to_string()))
//!         .await
//!         .unwrap();
//!
//!     for cmd in commands {
//!         println!("{}: {:?}", cmd.full_command, cmd.description);
//!     }
//! }
//! ```

use anyhow::{Context, Result};
use dirs;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, error, info};

/// Represents a slash command with its metadata and content.
///
/// A slash command is a reusable prompt template that can be invoked
/// by the user using the `/` prefix. Commands can be namespaced and
/// may include special syntax for bash commands, file references,
/// and argument placeholders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlashCommand {
    /// Unique identifier for the command (e.g., "project-commands-build-md").
    pub id: String,

    /// The command name without namespace (e.g., "build").
    pub name: String,

    /// The full command including namespace prefix (e.g., "/dev:build").
    pub full_command: String,

    /// The scope where the command is defined: "default", "project", or "user".
    pub scope: String,

    /// Optional namespace for the command (e.g., "dev" for "/dev:build").
    pub namespace: Option<String>,

    /// Absolute path to the command file on disk.
    pub file_path: String,

    /// The Markdown content of the command (body only, without frontmatter).
    pub content: String,

    /// Human-readable description of what the command does.
    pub description: Option<String>,

    /// List of tool names that this command is allowed to use.
    /// If empty, all tools are allowed.
    pub allowed_tools: Vec<String>,

    /// Whether the command contains bash command syntax (!`...`).
    pub has_bash_commands: bool,

    /// Whether the command contains file reference syntax (@file).
    pub has_file_references: bool,

    /// Whether the command accepts $ARGUMENTS placeholder.
    pub accepts_arguments: bool,
}

/// YAML frontmatter metadata for a command.
///
/// This structure represents the optional YAML frontmatter that can
/// appear at the beginning of a command Markdown file, enclosed by `---`.
#[derive(Debug, Deserialize)]
struct CommandFrontmatter {
    /// List of tool names that this command is allowed to invoke.
    #[serde(rename = "allowed-tools")]
    allowed_tools: Option<Vec<String>>,

    /// Human-readable description of the command's purpose.
    description: Option<String>,
}

/// Parse Markdown content with optional YAML frontmatter.
///
/// Extracts YAML metadata enclosed between `---` delimiters at the
/// beginning of the file, returning both the parsed frontmatter
/// and the remaining body content.
///
/// # Arguments
///
/// * `content` - The raw file content to parse.
///
/// # Returns
///
/// A tuple containing:
/// - `Option<CommandFrontmatter>` - Parsed frontmatter if present and valid.
/// - `String` - The body content (everything after the frontmatter).
///
/// # Example
///
/// ```ignore
/// let content = "---\ndescription: Test\n---\nBody content";
/// let (frontmatter, body) = parse_markdown_with_frontmatter(content)?;
/// assert_eq!(body, "Body content");
/// ```
fn parse_markdown_with_frontmatter(content: &str) -> Result<(Option<CommandFrontmatter>, String)> {
    let lines: Vec<&str> = content.lines().collect();

    if lines.is_empty() || lines[0] != "---" {
        return Ok((None, content.to_string()));
    }

    let mut frontmatter_end = None;
    for (i, line) in lines.iter().enumerate().skip(1) {
        if *line == "---" {
            frontmatter_end = Some(i);
            break;
        }
    }

    if let Some(end) = frontmatter_end {
        let frontmatter_content = lines[1..end].join("\n");
        let body_content = lines[(end + 1)..].join("\n");

        match serde_yaml::from_str::<CommandFrontmatter>(&frontmatter_content) {
            Ok(frontmatter) => Ok((Some(frontmatter), body_content)),
            Err(e) => {
                debug!("Failed to parse frontmatter: {}", e);
                Ok((None, content.to_string()))
            }
        }
    } else {
        Ok((None, content.to_string()))
    }
}

/// Extract command name and namespace from file path.
///
/// Parses the file path relative to the base commands directory to
/// determine the command name and optional namespace hierarchy.
///
/// # Arguments
///
/// * `file_path` - Absolute path to the command file.
/// * `base_path` - Base directory for commands (e.g., `.claude/commands`).
///
/// # Returns
///
/// A tuple of `(name, namespace)` where:
/// - `name` is the command name (filename without extension).
/// - `namespace` is `None` for top-level commands, or `Some("ns1:ns2")`
///   for nested commands.
///
/// # Examples
///
/// - `commands/build.md` → `("build", None)`
/// - `commands/dev/build.md` → `("build", Some("dev"))`
/// - `commands/team/dev/build.md` → `("build", Some("team:dev"))`
fn extract_command_info(file_path: &Path, base_path: &Path) -> Result<(String, Option<String>)> {
    let relative_path = file_path
        .strip_prefix(base_path)
        .context("Failed to get relative path")?;

    let path_without_ext = relative_path
        .with_extension("")
        .to_string_lossy()
        .to_string();

    let components: Vec<&str> = path_without_ext.split(std::path::MAIN_SEPARATOR).collect();

    if components.is_empty() {
        return Err(anyhow::anyhow!("Invalid command path"));
    }

    if components.len() == 1 {
        Ok((components[0].to_string(), None))
    } else {
        let Some((command_name, namespace_parts)) = components.split_last() else {
            return Err(anyhow::anyhow!("Invalid command path"));
        };
        let namespace = namespace_parts.join(":");
        Ok(((*command_name).to_string(), Some(namespace)))
    }
}

/// Load a slash command from a Markdown file.
///
/// Reads the file content, parses frontmatter, extracts command metadata,
/// and constructs a complete `SlashCommand` instance.
///
/// # Arguments
///
/// * `file_path` - Path to the command Markdown file.
/// * `base_path` - Base commands directory for calculating relative paths.
/// * `scope` - The command scope ("default", "project", or "user").
///
/// # Returns
///
/// A `SlashCommand` with all metadata populated.
///
/// # Errors
///
/// Returns an error if:
/// - The file cannot be read.
/// - The file path is invalid.
/// - YAML frontmatter is malformed (logs warning and continues without it).
fn load_command_from_file(file_path: &Path, base_path: &Path, scope: &str) -> Result<SlashCommand> {
    debug!("Loading command from: {:?}", file_path);

    let content = fs::read_to_string(file_path).context("Failed to read command file")?;

    let (frontmatter, body) = parse_markdown_with_frontmatter(&content)?;

    let (name, namespace) = extract_command_info(file_path, base_path)?;

    let full_command = match &namespace {
        Some(ns) => format!("/{ns}:{name}"),
        None => format!("/{name}"),
    };

    let id = format!(
        "{}-{}",
        scope,
        file_path.to_string_lossy().replace(['/', '\\'], "-")
    );

    let has_bash_commands = body.contains("!`");
    let has_file_references = body.contains('@');
    let accepts_arguments = body.contains("$ARGUMENTS");

    let (description, allowed_tools) = if let Some(fm) = frontmatter {
        (fm.description, fm.allowed_tools.unwrap_or_default())
    } else {
        (None, Vec::new())
    };

    Ok(SlashCommand {
        id,
        name,
        full_command,
        scope: scope.to_string(),
        namespace,
        file_path: file_path.to_string_lossy().to_string(),
        content: body,
        description,
        allowed_tools,
        has_bash_commands,
        has_file_references,
        accepts_arguments,
    })
}

/// Recursively find all Markdown files in a directory.
///
/// Traverses the directory tree and collects all `.md` files,
/// skipping hidden files and directories (starting with `.`).
///
/// # Arguments
///
/// * `dir` - Directory to search.
/// * `files` - Vector to collect found file paths.
///
/// # Returns
///
/// `Ok(())` on success, or an error if directory traversal fails.
fn find_markdown_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }

    let mut stack = vec![dir.to_path_buf()];
    let mut visited = std::collections::HashSet::new();

    while let Some(current_dir) = stack.pop() {
        let canonical = match fs::canonicalize(&current_dir) {
            Ok(path) => path,
            Err(_) => current_dir.clone(),
        };
        if !visited.insert(canonical) {
            continue;
        }

        for entry in fs::read_dir(&current_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with('.') {
                    continue;
                }
            }

            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                continue;
            }

            if metadata.is_dir() {
                stack.push(path);
            } else if metadata.is_file() && path.extension().is_some_and(|ext| ext == "md") {
                files.push(path);
            }
        }
    }

    Ok(())
}

/// Create the list of default built-in commands.
///
/// These commands are always available and do not require files on disk:
/// - `/add-dir` - Add additional working directories.
/// - `/init` - Initialize project with CLAUDE.md guide.
/// - `/review` - Request code review.
fn create_default_commands() -> Vec<SlashCommand> {
    vec![
        SlashCommand {
            id: "default-add-dir".to_string(),
            name: "add-dir".to_string(),
            full_command: "/add-dir".to_string(),
            scope: "default".to_string(),
            namespace: None,
            file_path: "".to_string(),
            content: "Add additional working directories".to_string(),
            description: Some("Add additional working directories".to_string()),
            allowed_tools: vec![],
            has_bash_commands: false,
            has_file_references: false,
            accepts_arguments: false,
        },
        SlashCommand {
            id: "default-init".to_string(),
            name: "init".to_string(),
            full_command: "/init".to_string(),
            scope: "default".to_string(),
            namespace: None,
            file_path: "".to_string(),
            content: "Initialize project with CLAUDE.md guide".to_string(),
            description: Some("Initialize project with CLAUDE.md guide".to_string()),
            allowed_tools: vec![],
            has_bash_commands: false,
            has_file_references: false,
            accepts_arguments: false,
        },
        SlashCommand {
            id: "default-review".to_string(),
            name: "review".to_string(),
            full_command: "/review".to_string(),
            scope: "default".to_string(),
            namespace: None,
            file_path: "".to_string(),
            content: "Request code review".to_string(),
            description: Some("Request code review".to_string()),
            allowed_tools: vec![],
            has_bash_commands: false,
            has_file_references: false,
            accepts_arguments: false,
        },
        SlashCommand {
            id: "default-plan".to_string(),
            name: "plan".to_string(),
            full_command: "/plan".to_string(),
            scope: "default".to_string(),
            namespace: None,
            file_path: "".to_string(),
            content: "I want you to enter plan mode. This is a complex task that requires exploration and design before implementation.\n\nUse the EnterPlanMode tool to switch to read-only exploration. Then:\n1. Explore the codebase to understand the problem\n2. Design a concrete implementation approach\n3. Break the work into steps using the Task tool\n4. Write the plan to the plan file\n5. Exit plan mode when ready for my review".to_string(),
            description: Some("Enter plan mode for complex tasks".to_string()),
            allowed_tools: vec![],
            has_bash_commands: false,
            has_file_references: false,
            accepts_arguments: false,
        },
    ]
}

/// List all available slash commands.
///
/// Discovers and loads slash commands from:
/// 1. Default built-in commands.
/// 2. Project-level commands in `.claude/commands/`.
/// 3. User-level commands in `~/.claude/commands/`.
///
/// Commands from all sources are merged, with project and user commands
/// taking precedence over defaults.
///
/// # Arguments
///
/// * `project_path` - Optional path to the project root directory.
///
/// # Returns
///
/// A vector of all discovered `SlashCommand` instances.
///
/// # Errors
///
/// Returns an error string if command loading fails critically.
///
/// # Example
///
/// ```rust,ignore
/// let commands = slash_commands_list(Some("./my-project".to_string())).await?;
/// for cmd in commands {
///     println!("{} - {:?}", cmd.full_command, cmd.description);
/// }
/// ```
pub async fn slash_commands_list(
    project_path: Option<String>,
) -> Result<Vec<SlashCommand>, String> {
    info!("Discovering slash commands");
    let mut commands = Vec::new();

    commands.extend(create_default_commands());

    if let Some(proj_path) = project_path {
        let project_commands_dir = PathBuf::from(&proj_path).join(".claude").join("commands");
        if project_commands_dir.exists() {
            debug!("Scanning project commands at: {:?}", project_commands_dir);

            let mut md_files = Vec::new();
            if let Err(e) = find_markdown_files(&project_commands_dir, &mut md_files) {
                error!("Failed to find project command files: {}", e);
            } else {
                for file_path in md_files {
                    match load_command_from_file(&file_path, &project_commands_dir, "project") {
                        Ok(cmd) => {
                            debug!("Loaded project command: {}", cmd.full_command);
                            commands.push(cmd);
                        }
                        Err(e) => {
                            error!("Failed to load command from {:?}: {}", file_path, e);
                        }
                    }
                }
            }
        }
    }

    if let Some(home_dir) = dirs::home_dir() {
        let user_commands_dir = home_dir.join(".claude").join("commands");
        if user_commands_dir.exists() {
            debug!("Scanning user commands at: {:?}", user_commands_dir);

            let mut md_files = Vec::new();
            if let Err(e) = find_markdown_files(&user_commands_dir, &mut md_files) {
                error!("Failed to find user command files: {}", e);
            } else {
                for file_path in md_files {
                    match load_command_from_file(&file_path, &user_commands_dir, "user") {
                        Ok(cmd) => {
                            debug!("Loaded user command: {}", cmd.full_command);
                            commands.push(cmd);
                        }
                        Err(e) => {
                            error!("Failed to load command from {:?}: {}", file_path, e);
                        }
                    }
                }
            }
        }
    }

    info!("Found {} slash commands", commands.len());
    Ok(commands)
}

/// Get a specific slash command by its ID.
///
/// Searches through all available commands to find one matching
/// the given command ID.
///
/// # Arguments
///
/// * `command_id` - The unique identifier of the command (e.g., "project-commands-build-md").
///
/// # Returns
///
/// The matching `SlashCommand` if found.
///
/// # Errors
///
/// Returns an error string if:
/// - The command ID format is invalid.
/// - No command with the given ID is found.
///
/// # Example
///
/// ```rust,ignore
/// let command = slash_command_get("project-commands-build-md".to_string()).await?;
/// println!("Command: {}", command.full_command);
/// ```
pub async fn slash_command_get(command_id: String) -> Result<SlashCommand, String> {
    debug!("Getting slash command: {}", command_id);

    let parts: Vec<&str> = command_id.split('-').collect();
    if parts.len() < 2 {
        return Err("Invalid command ID".to_string());
    }

    let commands = slash_commands_list(None).await?;

    commands
        .into_iter()
        .find(|cmd| cmd.id == command_id)
        .ok_or_else(|| format!("Command not found: {}", command_id))
}

/// Save a new slash command to disk.
///
/// Creates a new command file with optional YAML frontmatter and
/// writes it to the appropriate directory based on scope.
///
/// # Arguments
///
/// * `scope` - Where to save the command: "project" or "user".
/// * `name` - The command name (filename without extension).
/// * `namespace` - Optional namespace hierarchy (e.g., "dev" for `/dev:build`).
/// * `content` - The Markdown body content of the command.
/// * `description` - Optional description for the frontmatter.
/// * `allowed_tools` - List of tools the command can use (empty = all).
/// * `project_path` - Required if scope is "project".
///
/// # Returns
///
/// The newly created `SlashCommand` instance.
///
/// # Errors
///
/// Returns an error string if:
/// - The command name is empty.
/// - The scope is invalid (not "project" or "user").
/// - The project path is missing for project scope.
/// - Directory creation or file writing fails.
///
/// # Example
///
/// ```rust,ignore
/// let command = slash_command_save(
///     "project".to_string(),
///     "build".to_string(),
///     Some("dev".to_string()),
///     "Run cargo build".to_string(),
///     Some("Build the project".to_string()),
///     vec!["execute_command".to_string()],
///     Some("./my-project".to_string()),
/// ).await?;
/// ```
pub async fn slash_command_save(
    scope: String,
    name: String,
    namespace: Option<String>,
    content: String,
    description: Option<String>,
    allowed_tools: Vec<String>,
    project_path: Option<String>,
) -> Result<SlashCommand, String> {
    info!("Saving slash command: {} in scope: {}", name, scope);

    if name.is_empty() {
        return Err("Command name cannot be empty".to_string());
    }

    if !["project", "user"].contains(&scope.as_str()) {
        return Err("Invalid scope. Must be 'project' or 'user'".to_string());
    }

    let base_dir = if scope == "project" {
        if let Some(proj_path) = project_path {
            PathBuf::from(proj_path).join(".claude").join("commands")
        } else {
            return Err("Project path required for project scope".to_string());
        }
    } else {
        dirs::home_dir()
            .ok_or_else(|| "Could not find home directory".to_string())?
            .join(".claude")
            .join("commands")
    };

    let mut file_path = base_dir.clone();
    if let Some(ns) = &namespace {
        for component in ns.split(':') {
            file_path = file_path.join(component);
        }
    }

    fs::create_dir_all(&file_path).map_err(|e| format!("Failed to create directories: {}", e))?;

    file_path = file_path.join(format!("{}.md", name));

    let mut full_content = String::new();

    if description.is_some() || !allowed_tools.is_empty() {
        full_content.push_str("---\n");

        if let Some(desc) = &description {
            full_content.push_str(&format!("description: {}\n", desc));
        }

        if !allowed_tools.is_empty() {
            full_content.push_str("allowed-tools:\n");
            for tool in &allowed_tools {
                full_content.push_str(&format!("  - {}\n", tool));
            }
        }

        full_content.push_str("---\n\n");
    }

    full_content.push_str(&content);

    fs::write(&file_path, &full_content)
        .map_err(|e| format!("Failed to write command file: {}", e))?;

    load_command_from_file(&file_path, &base_dir, &scope)
        .map_err(|e| format!("Failed to load saved command: {}", e))
}

/// Delete a slash command from disk.
///
/// Removes the command file and cleans up any empty parent directories
/// that were created for namespacing.
///
/// # Arguments
///
/// * `command_id` - The unique identifier of the command to delete.
/// * `project_path` - Required if deleting a project-scoped command.
///
/// # Returns
///
/// A success message indicating which command was deleted.
///
/// # Errors
///
/// Returns an error string if:
/// - The command is not found.
/// - A project command is deleted without providing project_path.
/// - File deletion fails.
///
/// # Example
///
/// ```rust,ignore
/// let result = slash_command_delete(
///     "project-commands-build-md".to_string(),
///     Some("./my-project".to_string()),
/// ).await?;
/// println!("{}", result); // "Deleted command: /dev:build"
/// ```
pub async fn slash_command_delete(
    command_id: String,
    project_path: Option<String>,
) -> Result<String, String> {
    info!("Deleting slash command: {}", command_id);

    let is_project_command = command_id.starts_with("project-");

    if is_project_command && project_path.is_none() {
        return Err("Project path required to delete project commands".to_string());
    }

    let commands = slash_commands_list(project_path).await?;

    let command = commands
        .into_iter()
        .find(|cmd| cmd.id == command_id)
        .ok_or_else(|| format!("Command not found: {}", command_id))?;

    fs::remove_file(&command.file_path)
        .map_err(|e| format!("Failed to delete command file: {}", e))?;

    if let Some(parent) = Path::new(&command.file_path).parent() {
        let _ = remove_empty_dirs(parent);
    }

    Ok(format!("Deleted command: {}", command.full_command))
}

/// Recursively remove empty directories after command deletion.
///
/// Walks up the directory tree from the deleted command's location
/// and removes any empty parent directories created for namespacing.
///
/// # Arguments
///
/// * `dir` - Directory to check and potentially remove.
///
/// # Returns
///
/// `Ok(())` on success, or an error if directory operations fail.
fn remove_empty_dirs(dir: &Path) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }

    let is_empty = fs::read_dir(dir)?.next().is_none();

    if is_empty {
        fs::remove_dir(dir)?;

        if let Some(parent) = dir.parent() {
            let _ = remove_empty_dirs(parent);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_extract_command_info_with_namespace() {
        let base_path = PathBuf::from("/home/user/.claude/commands");
        let file_path = PathBuf::from("/home/user/.claude/commands/dev/build.md");

        let (name, namespace) = extract_command_info(&file_path, &base_path).unwrap();

        assert_eq!(name, "build");
        assert_eq!(namespace, Some("dev".to_string()));
    }

    #[test]
    fn test_extract_command_info_without_namespace() {
        let base_path = PathBuf::from("/home/user/.claude/commands");
        let file_path = PathBuf::from("/home/user/.claude/commands/help.md");

        let (name, namespace) = extract_command_info(&file_path, &base_path).unwrap();

        assert_eq!(name, "help");
        assert_eq!(namespace, None);
    }

    #[test]
    fn test_extract_command_info_nested_namespace() {
        let base_path = PathBuf::from("/home/user/.claude/commands");
        let file_path = PathBuf::from("/home/user/.claude/commands/team/dev/build.md");

        let (name, namespace) = extract_command_info(&file_path, &base_path).unwrap();

        assert_eq!(name, "build");
        assert_eq!(namespace, Some("team:dev".to_string()));
    }

    #[test]
    fn test_extract_command_info_cross_platform_path_separator() {
        // This test verifies that the path separator handling works correctly
        // The fix uses std::path::MAIN_SEPARATOR which is '/' on Unix and '\\' on Windows
        let base_path = PathBuf::from("/base");
        let file_path = PathBuf::from("/base/category/command.md");

        let (name, namespace) = extract_command_info(&file_path, &base_path).unwrap();

        assert_eq!(name, "command");
        assert_eq!(namespace, Some("category".to_string()));
    }

    #[test]
    fn test_command_id_replaces_both_separators() {
        // Test that both '/' and '\\' are replaced with '-' in command IDs
        // This ensures cross-platform compatibility
        let path_with_forward_slash = "/home/user/file.md";
        let path_with_backslash = "\\home\\user\\file.md";

        let id_forward = path_with_forward_slash.replace(['/', '\\'], "-");
        let id_backslash = path_with_backslash.replace(['/', '\\'], "-");

        assert_eq!(id_forward, "-home-user-file.md");
        assert_eq!(id_backslash, "-home-user-file.md");
    }

    #[cfg(unix)]
    #[test]
    fn test_find_markdown_files_skips_symlink_loops() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let commands_root = dir.path().join("commands");
        let nested = commands_root.join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("hello.md"), "# hello").unwrap();

        // Create a cycle: nested/loop -> commands_root
        symlink(&commands_root, nested.join("loop")).unwrap();

        let mut files = Vec::new();
        find_markdown_files(&commands_root, &mut files).unwrap();

        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("hello.md"));
    }
}
