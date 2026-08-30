//! Tool name constants and normalization functions.
//!
//! These are pure data and functions with zero crate dependencies. They are
//! used by both `core::config` and `agent::tools::executor`.

/// List of all built-in tool names.
///
/// This list intentionally includes only tools that are always registered by
/// `BuiltinToolExecutor::new()`. Optional tools (for example integrations that
/// depend on host binaries) should NOT be added here.
pub const BUILTIN_TOOL_NAMES: [&str; 23] = [
    "conclusion_with_options",
    "Bash",
    "BashInput",
    "BashOutput",
    "Edit",
    "EnterPlanMode",
    "ExitPlanMode",
    "GetFileInfo",
    "Glob",
    "Grep",
    "js_repl",
    "KillShell",
    "session_note",
    "NotebookEdit",
    "Read",
    "request_permissions",
    "Sleep",
    "Task",
    "update_goal",
    "WebFetch",
    "WebSearch",
    "Workspace",
    "Write",
];

/// Tool names that are accepted as aliases for built-in/server tools but are not
/// independently listed in `BUILTIN_TOOL_NAMES`/`SERVER_TOOL_NAMES`. Calls to these names are
/// transparently routed to their canonical counterpart.
pub const BUILTIN_TOOL_ALIASES: [(&str, &str); 10] = [
    // apply_patch is a patch-only alias for Edit
    ("apply_patch", "Edit"),
    // FileExists is subsumed by GetFileInfo (returns {exists: false} for missing paths)
    ("FileExists", "GetFileInfo"),
    // GetCurrentDir + SetWorkspace are subsumed by Workspace
    ("GetCurrentDir", "Workspace"),
    ("SetWorkspace", "Workspace"),
    // Session note rename
    ("memory_note", "session_note"),
    // Server tool renames: `recall` was ambiguous with durable-memory recall
    // (RAG-style relevant-memory injection). The canonical name is now
    // `session_history` — strictly a read-only viewer over local SQLite
    // session storage. Older names still route to the canonical tool.
    ("recall", "session_history"),
    ("session_inspector", "session_history"),
    ("schedule_tasks", "scheduler"),
    // SubAgent manager alias
    ("sub_session_manager", "SubAgent"),
    ("SubSession", "SubAgent"),
];

/// Legacy spellings normalized before canonical alias resolution.
///
/// This table is public so classification, compatibility tests, and future
/// admission code can prove that every accepted spelling shares one resolver.
pub const LEGACY_TOOL_NAME_ALIASES: [(&str, &str); 33] = [
    ("conclusionWithOptions", "conclusion_with_options"),
    ("execute_command", "Bash"),
    ("file_exists", "FileExists"),
    ("fileExists", "FileExists"),
    ("get_current_dir", "GetCurrentDir"),
    ("getCurrentDir", "GetCurrentDir"),
    ("get_file_info", "GetFileInfo"),
    ("getFileInfo", "GetFileInfo"),
    ("list_directory", "Glob"),
    ("memory_note", "memory_note"),
    ("read_file", "Read"),
    ("set_workspace", "SetWorkspace"),
    ("setWorkspace", "SetWorkspace"),
    ("sleep", "Sleep"),
    ("applyPatch", "apply_patch"),
    ("spawn_session", "SubAgent"),
    ("spawnSession", "SubAgent"),
    ("sub_session", "SubAgent"),
    ("subSession", "SubAgent"),
    ("sub_task", "SubAgent"),
    ("subTask", "SubAgent"),
    ("team_agent", "SubAgent"),
    ("teamAgent", "SubAgent"),
    ("child_session", "SubAgent"),
    ("childSession", "SubAgent"),
    ("sub_session_manager", "SubAgent"),
    ("sub_agent", "SubAgent"),
    ("subAgent", "SubAgent"),
    ("SubSession", "SubAgent"),
    ("subsession", "SubAgent"),
    ("write_file", "Write"),
    ("sessionInspector", "session_inspector"),
    ("scheduleTasks", "schedule_tasks"),
];

pub const SERVER_TOOL_NAMES: [&str; 8] = [
    "SubAgent",
    "compact_context",
    "scheduler",
    "session_history",
    "memory",
    "ledger",
    "load_skill",
    "read_skill_resource",
];

/// Canonical capability names supplied by Bamboo's server-side overlays.
///
/// This superset is used by catalogs and discovery without broadening the
/// legacy `normalize_tool_ref`/`is_builtin_tool` acceptance surface above.
pub const SERVER_CAPABILITY_NAMES: [&str; 14] = [
    "SubAgent",
    "Project",
    "ask_agent",
    "cluster",
    "compact_context",
    "deploy_agent",
    "ledger",
    "load_skill",
    "memory",
    "notify",
    "read_skill_resource",
    "scheduler",
    "session_history",
    "workflow_run",
];

/// Resolve any model- or host-facing tool reference to one canonical identity.
///
/// Legacy `namespace::tool` references use the executable name after the last
/// separator. MCP aliases already encode their namespace as
/// `mcp__server__function`, so they remain distinct. Known builtin/server names
/// are also restored to their declared case. Unknown registrations are kept as
/// executable names and are classified fail-closed elsewhere. Registered schema
/// execution identities remain exact and are carried separately by
/// `ClassifiedToolIdentity`; this function must not be used to rename schemas.
pub fn canonical_tool_name(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let unqualified = trimmed.rsplit("::").next().unwrap_or(trimmed).trim();
    let normalized = normalize_builtin_alias(unqualified);
    let canonical = resolve_alias(normalized).unwrap_or(normalized);

    BUILTIN_TOOL_NAMES
        .iter()
        .chain(SERVER_CAPABILITY_NAMES.iter())
        .find(|known| known.eq_ignore_ascii_case(canonical))
        .map(|known| (*known).to_string())
        .unwrap_or_else(|| canonical.to_string())
}

/// Normalizes a tool reference to a standard tool name.
///
/// Returns None if the tool name is not recognized.
pub fn normalize_tool_ref(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let raw_tool_name = trimmed.split("::").last().unwrap_or(trimmed);
    let normalized = normalize_builtin_alias(raw_tool_name);

    // Check canonical tool names first
    if let Some(name) = BUILTIN_TOOL_NAMES
        .iter()
        .chain(SERVER_TOOL_NAMES.iter())
        .find(|name| name.eq_ignore_ascii_case(normalized))
    {
        return Some((*name).to_string());
    }

    // Check aliases – return the *alias* name so callers see what was typed,
    // while the executor routes it to the canonical tool.
    BUILTIN_TOOL_ALIASES
        .iter()
        .find(|(alias, _)| alias.eq_ignore_ascii_case(normalized))
        .map(|(alias, _)| (*alias).to_string())
}

/// Returns the canonical tool name for an alias, or `None` if the name is
/// not an alias.
pub fn resolve_alias(name: &str) -> Option<&'static str> {
    BUILTIN_TOOL_ALIASES
        .iter()
        .find(|(alias, _)| alias.eq_ignore_ascii_case(name))
        .map(|(_, canonical)| *canonical)
}

pub fn normalize_builtin_alias(name: &str) -> &str {
    LEGACY_TOOL_NAME_ALIASES
        .iter()
        .find(|(alias, _)| *alias == name)
        .map(|(_, normalized)| *normalized)
        .unwrap_or(name)
}

/// Checks if a tool reference is a built-in tool
pub fn is_builtin_tool(value: &str) -> bool {
    normalize_tool_ref(value).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_tool_ref_accepts_claude_style_names() {
        assert_eq!(
            normalize_tool_ref("default::Bash"),
            Some("Bash".to_string())
        );
    }

    #[test]
    fn test_normalize_tool_ref_accepts_legacy_camel_aliases() {
        assert_eq!(
            normalize_tool_ref("default::fileExists"),
            Some("FileExists".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::getCurrentDir"),
            Some("GetCurrentDir".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::getFileInfo"),
            Some("GetFileInfo".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::setWorkspace"),
            Some("SetWorkspace".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::sleep"),
            Some("Sleep".to_string())
        );
    }

    #[test]
    fn test_normalize_tool_ref_accepts_legacy_snake_case_aliases() {
        assert_eq!(
            normalize_tool_ref("default::execute_command"),
            Some("Bash".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::file_exists"),
            Some("FileExists".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::get_current_dir"),
            Some("GetCurrentDir".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::get_file_info"),
            Some("GetFileInfo".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::list_directory"),
            Some("Glob".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::memory_note"),
            Some("memory_note".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::read_file"),
            Some("Read".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::set_workspace"),
            Some("SetWorkspace".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::write_file"),
            Some("Write".to_string())
        );
    }

    #[test]
    fn test_normalize_tool_ref_accepts_spawn_task_aliases() {
        for alias in [
            "default::spawn_session",
            "default::sub_session",
            "default::sub_task",
            "default::team_agent",
            "default::child_session",
            "default::sub_agent",
            "default::subAgent",
            "default::SubSession",
            "default::subsession",
        ] {
            assert_eq!(normalize_tool_ref(alias), Some("SubAgent".to_string()));
        }
    }

    #[test]
    fn test_normalize_tool_ref_accepts_server_overlay_tools() {
        assert_eq!(normalize_tool_ref("compress_context"), None);
        assert_eq!(
            normalize_tool_ref("default::read_skill_resource"),
            Some("read_skill_resource".to_string())
        );
    }

    #[test]
    fn test_normalize_tool_ref_rejects_unknown_tool() {
        assert_eq!(normalize_tool_ref("default::search"), None);
    }

    #[test]
    fn test_is_builtin_tool() {
        assert!(is_builtin_tool("Bash"));
        assert!(is_builtin_tool("default::Bash"));
        assert!(!is_builtin_tool("unknown_tool"));
        assert!(!is_builtin_tool("compress_context"));
    }

    #[test]
    fn test_resolve_alias() {
        assert_eq!(resolve_alias("apply_patch"), Some("Edit"));
        assert_eq!(resolve_alias("FileExists"), Some("GetFileInfo"));
        assert_eq!(resolve_alias("Bash"), None);
    }

    #[test]
    fn canonical_name_resolves_aliases_namespaces_and_known_case() {
        assert_eq!(canonical_tool_name(" default::applyPatch "), "Edit");
        assert_eq!(canonical_tool_name("DEFAULT::read"), "Read");
        assert_eq!(canonical_tool_name("default::set_workspace"), "Workspace");
        assert_eq!(canonical_tool_name("DEFAULT::WORKFLOW_RUN"), "workflow_run");
    }

    #[test]
    fn canonical_reference_name_unqualifies_dynamic_refs_and_preserves_mcp_aliases() {
        assert_eq!(canonical_tool_name("default::custom_tool"), "custom_tool");
        assert_eq!(
            canonical_tool_name("mcp__alpha__read_file"),
            "mcp__alpha__read_file"
        );
        assert_eq!(
            canonical_tool_name("mcp__beta__read_file"),
            "mcp__beta__read_file"
        );
        assert_ne!(
            canonical_tool_name("mcp__alpha__read_file"),
            canonical_tool_name("mcp__beta__read_file")
        );
        assert!(canonical_tool_name("  ").is_empty());
    }

    #[test]
    fn legacy_server_names_are_a_subset_of_the_classified_server_catalog() {
        for name in SERVER_TOOL_NAMES {
            assert!(
                SERVER_CAPABILITY_NAMES.contains(&name),
                "legacy server tool {name} must remain classifiable"
            );
        }
    }
}
