use std::path::{Path, PathBuf};
use std::sync::OnceLock;

static BAMBOO_DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Convert a filesystem path to a user-facing string.
///
/// On Windows, `std::fs::canonicalize()` may produce verbatim paths like `\\?\C:\...`
/// which are valid for Win32 APIs but confusing for users and sometimes incompatible
/// with external tools. We strip the verbatim prefix for display and API payloads.
pub fn path_to_display_string(path: &Path) -> String {
    let s = path.to_string_lossy().to_string();

    #[cfg(windows)]
    {
        if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
            // \\?\UNC\server\share\path -> \\server\share\path
            return format!(r"\\{}", rest);
        }
        if let Some(rest) = s.strip_prefix(r"\\?\") {
            // \\?\C:\path -> C:\path
            return rest.to_string();
        }
    }

    s
}

/// Resolve the Bamboo data directory from runtime configuration.
///
/// Order:
/// 1) `BAMBOO_DATA_DIR` environment variable
/// 2) `${HOME}/.bamboo`
///
/// Note: this does not consult the in-process global. Use [`bamboo_dir`] for the
/// stabilized value after startup.
pub fn resolve_bamboo_dir() -> PathBuf {
    std::env::var("BAMBOO_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| match dirs::home_dir() {
            Some(home) => home.join(".bamboo"),
            None => PathBuf::from(".bamboo"),
        })
}

/// Initialize the global Bamboo data directory (set once per process).
///
/// Call this once during startup (e.g. in the binary entrypoint) so all modules
/// read a consistent data dir even if the environment changes later.
pub fn init_bamboo_dir(dir: PathBuf) {
    // First call wins; subsequent calls are ignored to keep the value stable.
    let _ = BAMBOO_DATA_DIR.set(dir);
}

/// Get Bamboo data directory (stabilized for the lifetime of the process).
pub fn bamboo_dir() -> PathBuf {
    // If initialized at startup, return the stabilized in-process value.
    // Otherwise, fall back to resolving from the current environment/home.
    BAMBOO_DATA_DIR
        .get()
        .cloned()
        .unwrap_or_else(resolve_bamboo_dir)
}

/// A user-facing string for the stabilized Bamboo data directory.
pub fn bamboo_dir_display() -> String {
    path_to_display_string(&bamboo_dir())
}

/// Conventional cross-agent skill directory (`~/.agents/skills`).
pub fn agents_skills_dir() -> Option<PathBuf> {
    dirs::home_dir().map(|home| home.join(".agents").join("skills"))
}

/// Get config.json path (in data directory)
pub fn config_json_path() -> PathBuf {
    bamboo_dir().join("config.json")
}

/// Get keyword_masking.json path
pub fn keyword_masking_json_path() -> PathBuf {
    bamboo_dir().join("keyword_masking.json")
}

/// Get workflows directory
pub fn workflows_dir() -> PathBuf {
    bamboo_dir().join("workflows")
}

/// Get the global markdown slash-command directory (`{bamboo_dir}/commands`).
pub fn commands_dir() -> PathBuf {
    commands_dir_in(&bamboo_dir())
}

/// Resolve a commands directory below an explicit Bamboo data directory.
pub fn commands_dir_in(bamboo_data_dir: &Path) -> PathBuf {
    bamboo_data_dir.join("commands")
}

/// Get the project-local Bamboo configuration directory.
pub fn project_bamboo_dir(project_dir: &Path) -> PathBuf {
    project_dir.join(".bamboo")
}

/// Get the project-local markdown slash-command directory.
pub fn project_commands_dir(project_dir: &Path) -> PathBuf {
    project_bamboo_dir(project_dir).join("commands")
}

/// Root for Git worktrees owned by Bamboo for this project.
pub fn project_worktree_dir(project_dir: &Path) -> PathBuf {
    project_bamboo_dir(project_dir).join("worktree")
}

/// Root for project-bound scratch data.
pub fn project_tmp_dir(project_dir: &Path) -> PathBuf {
    project_bamboo_dir(project_dir).join("tmp")
}

/// Get the local plugin bundles root (`~/.bamboo/plugins`).
///
/// Each installed plugin lives at `plugins_dir()/<plugin_id>/`, keeping the
/// plugin's own files together (manifest, skills, prompts, workflows,
/// optional per-platform binaries under `bin/`). See `bamboo-plugin` for the
/// manifest/provenance schema.
pub fn plugins_dir() -> PathBuf {
    bamboo_dir().join("plugins")
}

/// Get the installation root for a single plugin (`~/.bamboo/plugins/<id>`).
pub fn plugin_dir(id: &str) -> PathBuf {
    plugins_dir().join(id)
}

/// Get the plugin provenance registry path (`~/.bamboo/plugins/installed.json`).
pub fn plugins_installed_json_path() -> PathBuf {
    plugins_dir().join("installed.json")
}

/// Whether `name` is a safe workflow file-name stem: rejects empty / over-long /
/// untrimmed names, path separators and `..`, null bytes / control characters,
/// reserved Windows device names, and anything outside the
/// `[alphanumeric - _ . space]` allowlist. The single strict validator shared by
/// the HTTP workflow handlers and the Tauri-IPC commands so the surfaces can't
/// drift (#34 / #97). A workflow file is `{name}.md` under [`workflows_dir`].
pub fn is_safe_workflow_name(name: &str) -> bool {
    // Basic constraints.
    if name.is_empty() || name.len() > 255 {
        return false;
    }

    // No leading/trailing whitespace.
    let trimmed = name.trim();
    if trimmed != name || trimmed.is_empty() {
        return false;
    }

    // Path separators and traversal.
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return false;
    }

    // Null bytes and control characters.
    if name.chars().any(|ch| ch.is_control() || ch == '\0') {
        return false;
    }

    // Reserved Windows device names.
    let upper = name.to_uppercase();
    let stem = upper.split('.').next().unwrap_or(&upper);
    let reserved = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    if reserved.contains(&stem) {
        return false;
    }

    // Allowlist: alphanumeric (incl. unicode), dash, underscore, dot, space.
    name.chars()
        .all(|ch| ch.is_alphanumeric() || ch == '-' || ch == '_' || ch == '.' || ch == ' ')
}

/// Get anthropic-model-mapping.json path
pub fn anthropic_model_mapping_path() -> PathBuf {
    bamboo_dir().join("anthropic-model-mapping.json")
}

/// Get gemini-model-mapping.json path
pub fn gemini_model_mapping_path() -> PathBuf {
    bamboo_dir().join("gemini-model-mapping.json")
}

/// Ensure bamboo directory exists
pub fn ensure_bamboo_dir() -> std::io::Result<PathBuf> {
    let dir = bamboo_dir();
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Get sessions directory (`{bamboo_dir}/sessions`)
pub fn sessions_dir() -> PathBuf {
    bamboo_dir().join("sessions")
}

/// Get the change-feed event journal directory (`{bamboo_dir}/events`).
///
/// Holds the durable JSONL journal for the account change feed
/// (`GET /api/v1/stream`).
pub fn events_dir() -> PathBuf {
    bamboo_dir().join("events")
}

/// Get the local actor sub-agent state directory (`{bamboo_dir}/subagents`).
///
/// Issue #217: the persistent home for the sub-agent discovery fabric +
/// isolated per-child storage, replacing the old `env::temp_dir()/bamboo-
/// subagents` default so sub-agent state survives reboots and stays inside
/// the tenant's data dir instead of scattering into `/tmp`.
pub fn subagents_dir() -> PathBuf {
    bamboo_dir().join("subagents")
}

/// Resolve the workspace root directory from runtime configuration.
///
/// Order:
/// 1) `BAMBOO_WORKSPACE_ROOT` environment variable (an operator-chosen
///    location — e.g. a mounted volume — that need not live under `bamboo_dir()`)
/// 2) `{bamboo_dir}/workspaces`
///
/// This is the default home for a session's workspace when none is
/// explicitly configured (issue #217 acceptance criterion 1), and the
/// confinement root explicit workspace paths are pinned under when
/// [`workspace_confinement_enforced`] is on (criterion 2).
///
/// Note: like [`resolve_bamboo_dir`], this does not consult an in-process
/// global — it re-reads the environment every call, matching the pattern of
/// [`resolve_bamboo_dir`] vs [`bamboo_dir`].
pub fn resolve_workspace_root() -> PathBuf {
    std::env::var("BAMBOO_WORKSPACE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| bamboo_dir().join("workspaces"))
}

/// Whether explicit workspace paths must be canonicalized and confined to
/// [`resolve_workspace_root`] (issue #217 acceptance criterion 2) —
/// escapes (`..`, a symlink pointing outside, or an absolute path elsewhere
/// on disk) get relocated to a deterministic folder under the root instead of
/// honored as-is.
///
/// OFF by default: local single-user back-compat. A session's workspace may
/// point anywhere on disk exactly as before this issue — e.g. pointing bamboo
/// at an existing project outside `~/.bamboo`. An orchestrator opts into
/// "one folder = one tenant's entire state" containment by setting
/// `BAMBOO_WORKSPACE_CONFINE=1`, or implicitly by setting
/// `BAMBOO_WORKSPACE_ROOT` (choosing a dedicated workspace root is itself a
/// signal the deployment wants containment).
pub fn workspace_confinement_enforced() -> bool {
    let explicit = std::env::var("BAMBOO_WORKSPACE_CONFINE")
        .ok()
        .map(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "True" | "yes" | "YES"));
    match explicit {
        Some(value) => value,
        None => std::env::var_os("BAMBOO_WORKSPACE_ROOT").is_some(),
    }
}

/// Load JSON config file
pub fn load_config_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    if !path.exists() {
        return Err(format!("Config file not found: {}", path.display()));
    }
    let content =
        std::fs::read_to_string(path).map_err(|e| format!("Failed to read config: {e}"))?;
    serde_json::from_str(&content).map_err(|e| format!("Failed to parse config: {e}"))
}

/// Save JSON config file
pub fn save_config_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("Failed to create directory: {e}"))?;
    }
    let content = serde_json::to_string_pretty(value)
        .map_err(|e| format!("Failed to serialize config: {e}"))?;
    std::fs::write(path, content).map_err(|e| format!("Failed to write config: {e}"))
}

/// Get the user-level settings file path: `~/.bamboo/settings.json`
pub fn user_settings_path() -> PathBuf {
    bamboo_dir().join("settings.json")
}

/// Ensure project runtime directories exist and incrementally maintain the
/// local `.bamboo/.gitignore` without overwriting user-authored entries.
pub fn ensure_project_runtime_dirs(project_dir: &Path) -> std::io::Result<()> {
    let bamboo_dir = project_bamboo_dir(project_dir);
    std::fs::create_dir_all(project_worktree_dir(project_dir))?;
    std::fs::create_dir_all(project_tmp_dir(project_dir))?;

    let ignore_path = bamboo_dir.join(".gitignore");
    let existing = std::fs::read_to_string(&ignore_path).unwrap_or_default();
    let mut content = existing.clone();
    for entry in ["worktree/", "tmp/", "settings.local.json"] {
        if !existing.lines().any(|line| line.trim() == entry) {
            if !content.is_empty() && !content.ends_with('\n') {
                content.push('\n');
            }
            content.push_str(entry);
            content.push('\n');
        }
    }
    if content != existing {
        std::fs::write(ignore_path, content)?;
    }
    Ok(())
}

/// Backward-compatible settings name for [`project_bamboo_dir`].
pub fn project_settings_dir(project_dir: &Path) -> PathBuf {
    project_bamboo_dir(project_dir)
}

/// Get the project-level settings file: `<project>/.bamboo/settings.json`
pub fn project_settings_path(project_dir: &Path) -> PathBuf {
    project_settings_dir(project_dir).join("settings.json")
}

/// Get the local project-level settings file: `<project>/.bamboo/settings.local.json`
pub fn local_project_settings_path(project_dir: &Path) -> PathBuf {
    project_settings_dir(project_dir).join("settings.local.json")
}

/// Get the managed (enterprise) settings path — highest priority, read-only.
///
/// Platform locations:
/// - Linux: `/etc/bamboo/settings.json`
/// - macOS: `/Library/Application Support/Bamboo/settings.json`
/// - Windows: `C:\ProgramData\Bamboo\settings.json`
pub fn managed_settings_path() -> PathBuf {
    #[cfg(target_os = "linux")]
    {
        PathBuf::from("/etc/bamboo/settings.json")
    }
    #[cfg(target_os = "macos")]
    {
        PathBuf::from("/Library/Application Support/Bamboo/settings.json")
    }
    #[cfg(target_os = "windows")]
    {
        PathBuf::from("C:\\ProgramData\\Bamboo\\settings.json")
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        PathBuf::from("/etc/bamboo/settings.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};
    use tempfile::tempdir;

    #[test]
    fn is_safe_workflow_name_accepts_allowlisted_names() {
        for ok in [
            "my-workflow",
            "workflow_123",
            "My Workflow",
            "test.workflow",
            "workflow.md",
            "v1.0",
            "2024-01-15",
            "a",
            "工作流",
            "ワークフロー",
            "العربية",
        ] {
            assert!(is_safe_workflow_name(ok), "{ok:?} should be accepted");
        }
    }

    #[test]
    fn is_safe_workflow_name_rejects_unsafe_names() {
        for bad in [
            "",                // empty
            "workflow/name",   // path separator
            "/workflow",       // path separator
            "workflow\\name",  // path separator
            "..",              // traversal
            "../workflow",     // traversal
            "workflow..test",  // traversal substring
            "workflow (v1)",   // not in allowlist
            "workflow [test]", // not in allowlist
            "workflow@2.0",    // not in allowlist
            "workflow#1",      // not in allowlist
            "workflow$var",    // not in allowlist
            "workflow*",       // not in allowlist
            "workflow+test",   // not in allowlist
            "🚀-workflow",     // emoji is not alphanumeric
            " workflow",       // leading whitespace
            "workflow ",       // trailing whitespace
            "\tworkflow",      // control char + leading whitespace
            "work\u{0}flow",   // null byte
            "CON",             // reserved Windows name
            "nul.md",          // reserved Windows name (stem)
        ] {
            assert!(!is_safe_workflow_name(bad), "{bad:?} should be rejected");
        }
        // Over-length is rejected.
        assert!(!is_safe_workflow_name(&"a".repeat(256)));
    }

    #[test]
    fn test_resolve_bamboo_dir_prefers_env() {
        // Single crate-wide test lock: serialize with all other tests that
        // mutate the process-global `BAMBOO_DATA_DIR` env / state.
        let _guard = crate::test_support::env_cache_lock_acquire();

        let temp_dir = tempdir().expect("Failed to create temp dir");
        let bamboo_home = temp_dir.path().to_string_lossy().to_string();

        // Save current env
        let original = std::env::var_os("BAMBOO_DATA_DIR");

        std::env::set_var("BAMBOO_DATA_DIR", &bamboo_home);

        assert_eq!(resolve_bamboo_dir(), PathBuf::from(&bamboo_home));

        // Restore original env
        if let Some(val) = original {
            std::env::set_var("BAMBOO_DATA_DIR", val);
        } else {
            std::env::remove_var("BAMBOO_DATA_DIR");
        }
    }

    #[test]
    fn test_sessions_dir_is_under_bamboo_dir() {
        assert_eq!(sessions_dir(), bamboo_dir().join("sessions"));
    }

    #[test]
    fn test_subagents_dir_is_under_bamboo_dir() {
        assert_eq!(subagents_dir(), bamboo_dir().join("subagents"));
    }

    #[test]
    fn test_resolve_workspace_root_defaults_under_bamboo_dir() {
        let _guard = crate::test_support::env_cache_lock_acquire();
        let original = std::env::var_os("BAMBOO_WORKSPACE_ROOT");
        std::env::remove_var("BAMBOO_WORKSPACE_ROOT");

        assert_eq!(resolve_workspace_root(), bamboo_dir().join("workspaces"));

        if let Some(val) = original {
            std::env::set_var("BAMBOO_WORKSPACE_ROOT", val);
        }
    }

    #[test]
    fn test_resolve_workspace_root_prefers_env_override() {
        let _guard = crate::test_support::env_cache_lock_acquire();
        let original = std::env::var_os("BAMBOO_WORKSPACE_ROOT");

        std::env::set_var("BAMBOO_WORKSPACE_ROOT", "/mnt/tenant-workspaces");
        assert_eq!(
            resolve_workspace_root(),
            PathBuf::from("/mnt/tenant-workspaces")
        );

        if let Some(val) = original {
            std::env::set_var("BAMBOO_WORKSPACE_ROOT", val);
        } else {
            std::env::remove_var("BAMBOO_WORKSPACE_ROOT");
        }
    }

    #[test]
    fn test_workspace_confinement_off_by_default() {
        let _guard = crate::test_support::env_cache_lock_acquire();
        let original_confine = std::env::var_os("BAMBOO_WORKSPACE_CONFINE");
        let original_root = std::env::var_os("BAMBOO_WORKSPACE_ROOT");
        std::env::remove_var("BAMBOO_WORKSPACE_CONFINE");
        std::env::remove_var("BAMBOO_WORKSPACE_ROOT");

        assert!(!workspace_confinement_enforced());

        if let Some(val) = original_confine {
            std::env::set_var("BAMBOO_WORKSPACE_CONFINE", val);
        }
        if let Some(val) = original_root {
            std::env::set_var("BAMBOO_WORKSPACE_ROOT", val);
        }
    }

    #[test]
    fn test_workspace_confinement_enabled_explicitly_or_via_root_override() {
        let _guard = crate::test_support::env_cache_lock_acquire();
        let original_confine = std::env::var_os("BAMBOO_WORKSPACE_CONFINE");
        let original_root = std::env::var_os("BAMBOO_WORKSPACE_ROOT");

        std::env::remove_var("BAMBOO_WORKSPACE_ROOT");
        std::env::set_var("BAMBOO_WORKSPACE_CONFINE", "1");
        assert!(workspace_confinement_enforced());

        std::env::remove_var("BAMBOO_WORKSPACE_CONFINE");
        std::env::set_var("BAMBOO_WORKSPACE_ROOT", "/mnt/tenant-workspaces");
        assert!(
            workspace_confinement_enforced(),
            "setting a dedicated workspace root implicitly opts into confinement"
        );

        // An explicit `false` wins even when a root override is set.
        std::env::set_var("BAMBOO_WORKSPACE_CONFINE", "false");
        assert!(!workspace_confinement_enforced());

        if let Some(val) = original_confine {
            std::env::set_var("BAMBOO_WORKSPACE_CONFINE", val);
        } else {
            std::env::remove_var("BAMBOO_WORKSPACE_CONFINE");
        }
        if let Some(val) = original_root {
            std::env::set_var("BAMBOO_WORKSPACE_ROOT", val);
        } else {
            std::env::remove_var("BAMBOO_WORKSPACE_ROOT");
        }
    }

    #[test]
    fn test_config_json_path() {
        let path = config_json_path();
        assert!(path.ends_with("config.json"));
        assert!(path.parent().is_some());
    }

    #[test]
    fn test_keyword_masking_json_path() {
        let path = keyword_masking_json_path();
        assert!(path.ends_with("keyword_masking.json"));
    }

    #[test]
    fn test_workflows_dir() {
        let path = workflows_dir();
        assert!(path.ends_with("workflows"));
    }

    #[test]
    fn project_command_paths_are_scoped_below_project_bamboo_dir() {
        let project = Path::new("/workspace/project");
        assert_eq!(project_bamboo_dir(project), project.join(".bamboo"));
        assert_eq!(
            project_commands_dir(project),
            project.join(".bamboo/commands")
        );
        assert_eq!(
            commands_dir_in(Path::new("/data/bamboo")),
            PathBuf::from("/data/bamboo/commands")
        );
    }

    #[test]
    fn project_runtime_paths_and_gitignore_are_scoped_and_incremental() {
        let temp = tempdir().expect("tempdir");
        let project = temp.path().join("project");
        std::fs::create_dir_all(project_bamboo_dir(&project)).expect("bamboo dir");
        std::fs::write(
            project_bamboo_dir(&project).join(".gitignore"),
            "custom-cache/\n",
        )
        .expect("custom ignore");

        ensure_project_runtime_dirs(&project).expect("runtime dirs");
        ensure_project_runtime_dirs(&project).expect("idempotent");

        assert_eq!(
            project_worktree_dir(&project),
            project.join(".bamboo/worktree")
        );
        assert_eq!(project_tmp_dir(&project), project.join(".bamboo/tmp"));
        assert!(project_worktree_dir(&project).is_dir());
        assert!(project_tmp_dir(&project).is_dir());
        let ignore = std::fs::read_to_string(project_bamboo_dir(&project).join(".gitignore"))
            .expect("ignore");
        assert!(ignore.contains("custom-cache/"));
        for entry in ["worktree/", "tmp/", "settings.local.json"] {
            assert_eq!(ignore.lines().filter(|line| *line == entry).count(), 1);
        }
    }

    #[test]
    fn test_plugins_dir() {
        let path = plugins_dir();
        assert!(path.ends_with("plugins"));
    }

    #[test]
    fn test_plugin_dir() {
        let path = plugin_dir("hello-plugin");
        assert!(path.ends_with("plugins/hello-plugin") || path.ends_with("plugins\\hello-plugin"));
    }

    #[test]
    fn test_plugins_installed_json_path() {
        let path = plugins_installed_json_path();
        assert!(path.ends_with("installed.json"));
        assert!(path.parent().unwrap().ends_with("plugins"));
    }

    #[test]
    fn test_anthropic_model_mapping_path() {
        let path = anthropic_model_mapping_path();
        assert!(path.ends_with("anthropic-model-mapping.json"));
    }

    #[test]
    fn test_gemini_model_mapping_path() {
        let path = gemini_model_mapping_path();
        assert!(path.ends_with("gemini-model-mapping.json"));
    }

    #[test]
    fn test_ensure_bamboo_dir_creates_directory() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let test_dir = temp_dir.path().join("test_bamboo");

        // Single crate-wide test lock: serialize with all other tests that
        // mutate the process-global `BAMBOO_DATA_DIR` env / state.
        let _guard = crate::test_support::env_cache_lock_acquire();

        // Save and set env
        let original = std::env::var_os("BAMBOO_DATA_DIR");
        std::env::set_var("BAMBOO_DATA_DIR", &test_dir);

        // NOTE: do NOT call `BAMBOO_DATA_DIR.set(...)` here. That OnceLock is
        // process-global and cannot be reset, so seeding it with this test's
        // tempdir (which is deleted at test end) permanently poisons
        // `bamboo_dir()` for every later test in the binary — a cross-test
        // flake. `ensure_bamboo_dir()` resolves via the env var we set above
        // (the OnceLock stays unset), so this test is self-contained.

        let result = ensure_bamboo_dir();
        assert!(result.is_ok());
        assert!(test_dir.exists());

        // Restore
        if let Some(val) = original {
            std::env::set_var("BAMBOO_DATA_DIR", val);
        } else {
            std::env::remove_var("BAMBOO_DATA_DIR");
        }
    }

    #[test]
    fn test_load_config_json_missing_file() {
        let result: Result<String, _> = load_config_json(Path::new("/nonexistent/file.json"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Config file not found"));
    }

    #[test]
    fn test_load_config_json_valid_file() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.json");

        std::fs::write(&file_path, r#"{"key": "value"}"#).expect("Failed to write file");

        #[derive(serde::Deserialize)]
        struct TestConfig {
            key: String,
        }

        let result: Result<TestConfig, _> = load_config_json(&file_path);
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.key, "value");
    }

    #[test]
    fn test_load_config_json_invalid_json() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("invalid.json");

        std::fs::write(&file_path, "not valid json").expect("Failed to write file");

        let result: Result<String, _> = load_config_json(&file_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to parse config"));
    }

    #[test]
    fn test_save_config_json_creates_file() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("new_config.json");

        #[derive(serde::Serialize)]
        struct TestConfig {
            key: String,
        }

        let config = TestConfig {
            key: "value".to_string(),
        };

        let result = save_config_json(&file_path, &config);
        assert!(result.is_ok());
        assert!(file_path.exists());

        let content = std::fs::read_to_string(&file_path).expect("Failed to read file");
        assert!(content.contains("key"));
        assert!(content.contains("value"));
    }

    #[test]
    fn test_save_config_json_creates_parent_directory() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("subdir/nested/config.json");

        #[derive(serde::Serialize)]
        struct TestConfig {
            key: String,
        }

        let config = TestConfig {
            key: "value".to_string(),
        };

        let result = save_config_json(&file_path, &config);
        assert!(result.is_ok());
        assert!(file_path.exists());
    }

    #[test]
    fn test_path_to_display_string_simple() {
        let path = Path::new("/home/user/test");
        let result = path_to_display_string(path);
        assert_eq!(result, "/home/user/test");
    }

    #[test]
    fn test_path_to_display_string_empty() {
        let path = Path::new("");
        let result = path_to_display_string(path);
        assert_eq!(result, "");
    }

    #[test]
    fn test_bamboo_dir_display() {
        let result = bamboo_dir_display();
        // Just ensure it returns a non-empty string
        assert!(!result.is_empty());
    }

    #[test]
    fn test_init_bamboo_dir_first_call_wins() {
        static INIT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = INIT_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("INIT_LOCK poisoned");

        // Create a new OnceLock for this test
        static TEST_DIR: OnceLock<PathBuf> = OnceLock::new();
        let first = PathBuf::from("/first/path");
        let second = PathBuf::from("/second/path");

        let _ = TEST_DIR.set(first.clone());
        let result = TEST_DIR.set(second);

        // Second set should fail (returns Err)
        assert!(result.is_err());

        // Value should still be first
        assert_eq!(TEST_DIR.get().unwrap(), &first);
    }
}
