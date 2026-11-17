//! Tool name constants and normalization functions.
//!
//! These are pure data and functions with zero crate dependencies. They are
//! used by both `core::config` and `agent::tools::executor`.

/// List of all built-in tool names.
///
/// This list intentionally includes only tools that are always registered by
/// `BuiltinToolExecutor::new()`. Optional tools (for example integrations that
/// depend on host binaries) should NOT be added here.
pub const BUILTIN_TOOL_NAMES: [&str; 18] = [
    "exec_command",
    "write_stdin",
    "Edit",
    "ExitPlanMode",
    "GetFileInfo",
    "Glob",
    "Grep",
    "session_note",
    "Read",
    "request_permissions",
    "Sleep",
    "Task",
    "update_goal",
    "loop_control",
    "ViewImage",
    "WebFetch",
    "Workspace",
    "Write",
];

/// Tool names that are accepted as aliases for built-in/server tools but are not
/// independently listed in `BUILTIN_TOOL_NAMES`/`SERVER_TOOL_NAMES`. Calls to these names are
/// transparently routed to their canonical counterpart.
pub const BUILTIN_TOOL_ALIASES: [(&str, &str); 3] = [
    // apply_patch is a patch-only alias for Edit
    ("apply_patch", "Edit"),
    // SubAgent manager alias
    ("sub_session_manager", "SubAgent"),
    ("SubSession", "SubAgent"),
];

/// Legacy spellings normalized before canonical alias resolution.
///
/// This table is public so classification, compatibility tests, and future
/// admission code can prove that every accepted spelling shares one resolver.
pub const LEGACY_TOOL_NAME_ALIASES: [(&str, &str); 25] = [
    ("execute_command", "Bash"),
    ("get_file_info", "GetFileInfo"),
    ("getFileInfo", "GetFileInfo"),
    ("list_directory", "Glob"),
    ("read_file", "Read"),
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
    ("view_image", "ViewImage"),
    ("viewImage", "ViewImage"),
    ("write_file", "Write"),
];

pub const SERVER_TOOL_NAMES: [&str; 12] = [
    "SubAgent",
    "archive_context",
    "compact_context",
    "scheduler",
    "session_history",
    "session_history_current",
    "session_message",
    "session_control",
    "memory",
    "ledger",
    "load_skill",
    "read_skill_resource",
];

/// Canonical capability names supplied by Bamboo's server-side overlays.
///
/// This superset is used by catalogs and discovery without broadening the
/// legacy `normalize_tool_ref`/`is_builtin_tool` acceptance surface above.
pub const SERVER_CAPABILITY_NAMES: [&str; 27] = [
    "SubAgent",
    "Project",
    "archive_context",
    "ask_agent",
    "browser",
    "browser_eval",
    "cluster",
    "compact_context",
    "deploy_agent",
    "ledger",
    "load_skill",
    "memory",
    "notify",
    "read_skill_resource",
    "scheduler",
    "session_control",
    "session_history",
    "session_history_current",
    "session_message",
    "workflow_run",
    "work_overview",
    "work_search",
    "work_inspect",
    "work_changes",
    "work_update",
    "work_dispatch",
    "work_review",
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

    let mut names = BUILTIN_TOOL_NAMES
        .iter()
        .chain(SERVER_CAPABILITY_NAMES.iter())
        .copied();
    names
        .clone()
        .find(|known| *known == canonical)
        .or_else(|| names.find(|known| known.eq_ignore_ascii_case(canonical)))
        .map(str::to_string)
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
    // Keep the ambiguous spelling until the actual Session registry/version
    // selects its owner. Canonical v1 alias resolution itself remains unchanged.
    if crate::independent_root_spawn_reference(raw_tool_name) {
        return Some("spawn_session".into());
    }
    let normalized = normalize_builtin_alias(raw_tool_name);

    let mut names = BUILTIN_TOOL_NAMES
        .iter()
        .chain(SERVER_TOOL_NAMES.iter())
        .copied();
    if let Some(name) = names
        .clone()
        .find(|name| *name == normalized)
        .or_else(|| names.find(|name| name.eq_ignore_ascii_case(normalized)))
    {
        return Some(name.to_string());
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

/// Preserve a disabled legacy shell capability after the model surface rename.
/// Call only after resolving an exact registered identity; foreign tools with
/// the same suffix must retain their own policies.
pub fn legacy_shell_replacement(execution_name: &str) -> Option<&'static str> {
    match execution_name {
        "Bash" => Some("exec_command"),
        "BashInput" | "BashOutput" | "KillShell" => Some("write_stdin"),
        _ => None,
    }
}

/// Denial-only expansion for current catalogs.
pub fn legacy_shell_replacements(execution_name: &str) -> impl Iterator<Item = &'static str> {
    let names: &[&str] = match execution_name {
        "Bash" => &["exec_command"],
        "BashInput" | "BashOutput" | "KillShell" => &["write_stdin"],
        _ => &[],
    };
    names.iter().copied()
}

/// Resolve disabled references, including retired aliases and shell names, when a
/// positive catalog omits the old route. Exact registrations keep priority.
/// Missing unqualified and default-namespace references may use alias replacements;
/// callable resolution and qualified foreign identities remain unchanged.
pub fn resolve_disabled_tool_reference_name(
    reference: &str,
    mut contains_registered_name: impl FnMut(&str) -> bool,
) -> Option<String> {
    if let Some(name) = crate::resolve_tool_reference_name(reference, &mut contains_registered_name)
    {
        return Some(name);
    }
    let reference = reference.trim();
    let unqualified = reference.strip_prefix("default::").unwrap_or(reference);
    let retired_replacement = match unqualified {
        name if name.eq_ignore_ascii_case("FileExists") || name == "file_exists" => {
            Some("GetFileInfo")
        }
        name if name.eq_ignore_ascii_case("GetCurrentDir")
            || name.eq_ignore_ascii_case("SetWorkspace")
            || matches!(name, "get_current_dir" | "set_workspace") =>
        {
            Some("Workspace")
        }
        name if name.eq_ignore_ascii_case("memory_note") => Some("session_note"),
        name if name.eq_ignore_ascii_case("recall")
            || name.eq_ignore_ascii_case("session_inspector")
            || name == "sessionInspector" =>
        {
            Some("session_history")
        }
        name if name.eq_ignore_ascii_case("schedule_tasks") || name == "scheduleTasks" => {
            Some("scheduler")
        }
        _ => None,
    };
    if let Some(replacement) = retired_replacement.filter(|name| contains_registered_name(name)) {
        return Some(replacement.to_owned());
    }
    if reference.contains("::") {
        return None;
    }
    retired_shell_policy_name(reference).map(str::to_owned)
}

fn retired_shell_policy_name(reference: &str) -> Option<&'static str> {
    let normalized = normalize_builtin_alias(reference);
    ["Bash", "BashInput", "BashOutput", "KillShell"]
        .into_iter()
        .find(|name| name.eq_ignore_ascii_case(normalized))
}

/// Preserve an old default-namespace deny after exact resolution misses.
/// Use only where Bamboo's own terminal is installed.
pub fn retired_default_shell_policy_name(reference: &str) -> Option<String> {
    let name = reference.trim().strip_prefix("default::")?.trim();
    retired_shell_policy_name(name).map(str::to_owned)
}

/// Checks if a tool reference is a built-in tool
pub fn is_builtin_tool(value: &str) -> bool {
    normalize_tool_ref(value).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_names_have_only_disabled_policy_replacements() {
        for (reference, replacement) in [
            ("FileExists", "GetFileInfo"),
            ("file_exists", "GetFileInfo"),
            ("default::fileExists", "GetFileInfo"),
            ("fIlEeXiStS", "GetFileInfo"),
            ("GetCurrentDir", "Workspace"),
            ("get_current_dir", "Workspace"),
            ("default::getCurrentDir", "Workspace"),
            ("SetWorkspace", "Workspace"),
            ("set_workspace", "Workspace"),
            ("default::setWorkspace", "Workspace"),
            ("memory_note", "session_note"),
            ("default::memory_note", "session_note"),
            ("recall", "session_history"),
            ("session_inspector", "session_history"),
            ("default::sessionInspector", "session_history"),
            ("schedule_tasks", "scheduler"),
            ("default::scheduleTasks", "scheduler"),
        ] {
            let registered = [
                "GetFileInfo",
                "Workspace",
                "session_note",
                "session_history",
                "scheduler",
            ];
            assert_eq!(normalize_tool_ref(reference), None, "{reference}");
            assert_eq!(
                crate::resolve_tool_reference_name(reference, |name| registered.contains(&name)),
                None
            );
            assert_eq!(
                resolve_disabled_tool_reference_name(reference, |name| registered.contains(&name)),
                Some(replacement.into())
            );
        }
        assert_eq!(
            resolve_disabled_tool_reference_name("FileExists", |_| false),
            None
        );
        assert_eq!(
            resolve_disabled_tool_reference_name("vendor::FileExists", |name| name
                == "GetFileInfo"),
            None
        );
        assert_eq!(
            resolve_disabled_tool_reference_name("FileExists", |name| [
                "FileExists",
                "GetFileInfo"
            ]
            .contains(&name)),
            Some("FileExists".into())
        );
    }

    #[test]
    fn legacy_shell_disabled_policy_only_expands_exact_builtin_names() {
        assert_eq!(
            legacy_shell_replacements("Bash").collect::<Vec<_>>(),
            ["exec_command"]
        );
        assert_eq!(
            legacy_shell_replacements("BashOutput").collect::<Vec<_>>(),
            ["write_stdin"]
        );
        assert_eq!(legacy_shell_replacements("vendor::Bash").next(), None);
        assert_eq!(legacy_shell_replacements("exec_command").next(), None);
    }

    #[test]
    fn disabled_shell_fallback_reuses_only_supported_missing_unqualified_aliases() {
        for (reference, legacy) in [
            ("Bash", "Bash"),
            (" bash ", "Bash"),
            ("bAsH", "Bash"),
            ("execute_command", "Bash"),
            ("bashinput", "BashInput"),
            ("bashoutput", "BashOutput"),
            ("kIlLsHeLl", "KillShell"),
        ] {
            assert_eq!(
                crate::resolve_tool_reference_name(reference, |name| {
                    matches!(name, "exec_command" | "write_stdin")
                }),
                None,
                "{reference}",
            );
            assert_eq!(
                resolve_disabled_tool_reference_name(reference, |_| false),
                Some(legacy.to_owned()),
                "{reference}",
            );
        }
        for reference in [
            "",
            "EXECUTE_COMMAND",
            "exec_command",
            "unknown",
            "Read",
            "write_stdin",
            "vendor::Bash",
            "default::Bash",
            "vendor::execute_command",
            "default::bashoutput",
        ] {
            assert_eq!(
                resolve_disabled_tool_reference_name(reference, |_| false),
                None,
                "{reference}",
            );
        }
    }

    #[test]
    fn disabled_shell_fallback_keeps_registered_exact_and_alias_resolution_first() {
        let registered = [
            "Bash",
            "bash",
            "execute_command",
            "vendor::Bash",
            "default::Bash",
            "default::execute_command",
            "Read",
        ];
        for reference in registered {
            assert_eq!(
                resolve_disabled_tool_reference_name(reference, |name| registered.contains(&name)),
                Some(reference.to_owned()),
                "{reference}",
            );
        }
        // A real legacy route keeps the shared resolver's qualified-alias
        // behavior; only the additional missing-route fallback is unqualified.
        assert_eq!(
            resolve_disabled_tool_reference_name("default::execute_command", |name| name == "Bash"),
            Some("Bash".to_owned()),
        );
        assert_eq!(
            resolve_disabled_tool_reference_name("read_file", |name| name == "Read"),
            Some("Read".to_owned()),
        );
    }

    #[test]
    fn retired_shell_names_are_not_normalized_as_builtins() {
        for name in [
            "Bash",
            "default::Bash",
            "default::bash",
            "execute_command",
            "BashInput",
            "BashOutput",
            "KillShell",
        ] {
            assert_eq!(normalize_tool_ref(name), None, "{name}");
        }
    }

    #[test]
    fn test_normalize_tool_ref_accepts_legacy_camel_aliases() {
        assert_eq!(normalize_tool_ref("default::fileExists"), None);
        assert_eq!(normalize_tool_ref("default::getCurrentDir"), None);
        assert_eq!(
            normalize_tool_ref("default::getFileInfo"),
            Some("GetFileInfo".to_string())
        );
        assert_eq!(normalize_tool_ref("default::setWorkspace"), None);
        assert_eq!(
            normalize_tool_ref("default::sleep"),
            Some("Sleep".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::viewImage"),
            Some("ViewImage".to_string())
        );
    }

    #[test]
    fn retired_conclusion_tool_and_alias_are_not_recognized() {
        assert_eq!(normalize_tool_ref("conclusion_with_options"), None);
        assert_eq!(normalize_tool_ref("default::conclusionWithOptions"), None);
    }

    #[test]
    fn test_normalize_tool_ref_accepts_legacy_snake_case_aliases() {
        assert_eq!(normalize_tool_ref("default::file_exists"), None);
        assert_eq!(normalize_tool_ref("default::get_current_dir"), None);
        assert_eq!(
            normalize_tool_ref("default::get_file_info"),
            Some("GetFileInfo".to_string())
        );
        assert_eq!(
            normalize_tool_ref("default::list_directory"),
            Some("Glob".to_string())
        );
        assert_eq!(normalize_tool_ref("default::memory_note"), None);
        assert_eq!(
            normalize_tool_ref("default::read_file"),
            Some("Read".to_string())
        );
        assert_eq!(normalize_tool_ref("default::set_workspace"), None);
        assert_eq!(
            normalize_tool_ref("default::write_file"),
            Some("Write".to_string())
        );
    }

    #[test]
    fn test_normalize_tool_ref_accepts_spawn_task_aliases() {
        for reference in [
            "spawn_session",
            "default::spawn_session",
            "default::spawnSession",
        ] {
            assert_eq!(normalize_tool_ref(reference), Some("spawn_session".into()));
            assert_eq!(canonical_tool_name(reference), "SubAgent");
        }
        for alias in [
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
        assert!(is_builtin_tool("exec_command"));
        assert!(is_builtin_tool("default::write_stdin"));
        assert!(!is_builtin_tool("unknown_tool"));
        assert!(!is_builtin_tool("compress_context"));
    }

    #[test]
    fn test_resolve_alias() {
        assert_eq!(resolve_alias("apply_patch"), Some("Edit"));
        assert_eq!(resolve_alias("FileExists"), None);
        assert_eq!(resolve_alias("Bash"), None);
    }

    #[test]
    fn canonical_name_resolves_aliases_namespaces_and_known_case() {
        assert_eq!(canonical_tool_name(" default::applyPatch "), "Edit");
        assert_eq!(canonical_tool_name("DEFAULT::read"), "Read");
        assert_eq!(
            canonical_tool_name("default::set_workspace"),
            "set_workspace"
        );
        assert_eq!(canonical_tool_name("DEFAULT::WORKFLOW_RUN"), "workflow_run");
        assert_eq!(
            canonical_tool_name("DEFAULT::SESSION_HISTORY_CURRENT"),
            "session_history_current"
        );
        assert_eq!(
            normalize_tool_ref("session_history_current"),
            Some("session_history_current".to_string())
        );
        assert_eq!(resolve_alias("session_history_current"), None);
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
