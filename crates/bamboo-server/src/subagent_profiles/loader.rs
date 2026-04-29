//! Layered loader that composes the final [`SubagentProfileRegistry`].
//!
//! Layers (each later one overrides earlier entries by `id`):
//!
//! 1. Built-in profiles ([`super::builtin::builtin_profiles`]).
//! 2. User global config: `<bamboo_data_dir>/subagent_profiles.json`.
//! 3. Project-level config: `<workspace>/.bamboo/subagent_profiles.json`
//!    (only loaded when a workspace path is provided).
//! 4. Env override: `BAMBOO_SUBAGENT_PROFILES_FILE=<path>` if set.
//!
//! Missing files are silently skipped; malformed JSON returns
//! [`LoaderError::Parse`] so misconfiguration is surfaced loudly.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bamboo_domain::subagent::{
    SubagentProfileFile, SubagentProfileRegistry, SubagentProfileRegistryError,
};
use thiserror::Error;

use super::builtin::builtin_profiles;

/// Environment variable name for ad-hoc override files.
pub const ENV_OVERRIDE_FILE: &str = "BAMBOO_SUBAGENT_PROFILES_FILE";

/// File name used for both the user-global and project-level config layers.
pub const CONFIG_FILE_NAME: &str = "subagent_profiles.json";

#[derive(Debug, Error)]
pub enum LoaderError {
    #[error("failed to read subagent profile file `{path}`: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse subagent profile file `{path}`: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    Registry(#[from] SubagentProfileRegistryError),
}

/// Build the merged [`SubagentProfileRegistry`].
///
/// `bamboo_data_dir` is the active Bamboo data directory (e.g. `~/.bamboo`).
/// `workspace_dir` is optional; when provided, a project-level override file
/// at `<workspace_dir>/.bamboo/subagent_profiles.json` is layered on top of
/// the user-global one.
pub fn load_registry(
    bamboo_data_dir: &Path,
    workspace_dir: Option<&Path>,
) -> Result<Arc<SubagentProfileRegistry>, LoaderError> {
    let mut builder = SubagentProfileRegistry::builder().extend(builtin_profiles());

    // 2. User-global config.
    let user_global = bamboo_data_dir.join(CONFIG_FILE_NAME);
    if let Some(file) = read_optional(&user_global)? {
        builder = builder.extend_from_file(file);
    }

    // 3. Project-level config.
    if let Some(ws) = workspace_dir {
        let project_path = ws.join(".bamboo").join(CONFIG_FILE_NAME);
        if let Some(file) = read_optional(&project_path)? {
            builder = builder.extend_from_file(file);
        }
    }

    // 4. Environment override (always read if set; missing path is an error).
    if let Some(env_path) = env_override_path() {
        let file = read_required(&env_path)?;
        builder = builder.extend_from_file(file);
    }

    Ok(Arc::new(builder.build()?))
}

fn env_override_path() -> Option<PathBuf> {
    std::env::var_os(ENV_OVERRIDE_FILE)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn read_optional(path: &Path) -> Result<Option<SubagentProfileFile>, LoaderError> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(parse(path, &text)?)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(LoaderError::Io {
            path: path.to_path_buf(),
            source: err,
        }),
    }
}

fn read_required(path: &Path) -> Result<SubagentProfileFile, LoaderError> {
    let text = std::fs::read_to_string(path).map_err(|source| LoaderError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    parse(path, &text)
}

fn parse(path: &Path, text: &str) -> Result<SubagentProfileFile, LoaderError> {
    serde_json::from_str(text).map_err(|source| LoaderError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn write_file(path: &Path, body: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, body).unwrap();
    }

    #[test]
    fn builtins_only_when_no_files_present() {
        let dir = TempDir::new().unwrap();
        let reg = load_registry(dir.path(), None).unwrap();
        // Six builtins.
        assert_eq!(reg.len(), 6);
        assert_eq!(reg.resolve("researcher").id, "researcher");
        // Unknown id falls back to general-purpose.
        assert_eq!(reg.resolve("does-not-exist").id, "general-purpose");
    }

    #[test]
    fn user_global_overrides_builtin() {
        let dir = TempDir::new().unwrap();
        write_file(
            &dir.path().join(CONFIG_FILE_NAME),
            r#"{"profiles":[{
                "id":"researcher",
                "display_name":"Custom",
                "system_prompt":"OVERRIDDEN",
                "tools":{"mode":"inherit"}
            }]}"#,
        );
        let reg = load_registry(dir.path(), None).unwrap();
        let r = reg.resolve("researcher");
        assert_eq!(r.display_name, "Custom");
        assert_eq!(r.system_prompt, "OVERRIDDEN");
        // Unaffected entries remain.
        assert_eq!(reg.resolve("plan").display_name, "Planner");
    }

    #[test]
    fn project_layer_overrides_user_global() {
        let bamboo_dir = TempDir::new().unwrap();
        let ws_dir = TempDir::new().unwrap();

        write_file(
            &bamboo_dir.path().join(CONFIG_FILE_NAME),
            r#"{"profiles":[{
                "id":"researcher","display_name":"User-Global",
                "system_prompt":"global","tools":{"mode":"inherit"}
            }]}"#,
        );
        write_file(
            &ws_dir.path().join(".bamboo").join(CONFIG_FILE_NAME),
            r#"{"profiles":[{
                "id":"researcher","display_name":"Project",
                "system_prompt":"project","tools":{"mode":"inherit"}
            }]}"#,
        );

        let reg = load_registry(bamboo_dir.path(), Some(ws_dir.path())).unwrap();
        let r = reg.resolve("researcher");
        assert_eq!(r.display_name, "Project");
        assert_eq!(r.system_prompt, "project");
    }

    #[test]
    fn env_override_takes_highest_precedence() {
        // Use a unique scratch dir; the env var name is a process-global so
        // we restore it after the test.
        let bamboo_dir = TempDir::new().unwrap();
        let env_file_dir = TempDir::new().unwrap();
        let env_file = env_file_dir.path().join("env_overrides.json");
        write_file(
            &env_file,
            r#"{"profiles":[{
                "id":"coder","display_name":"EnvCoder",
                "system_prompt":"env","tools":{"mode":"inherit"}
            }]}"#,
        );

        // SAFETY: tests in this module run sequentially within the same
        // process; we reset the var unconditionally below.
        let prev = std::env::var_os(ENV_OVERRIDE_FILE);
        std::env::set_var(ENV_OVERRIDE_FILE, &env_file);

        let reg = load_registry(bamboo_dir.path(), None).unwrap();

        match prev {
            Some(v) => std::env::set_var(ENV_OVERRIDE_FILE, v),
            None => std::env::remove_var(ENV_OVERRIDE_FILE),
        }

        assert_eq!(reg.resolve("coder").display_name, "EnvCoder");
    }

    #[test]
    fn malformed_json_surfaces_parse_error() {
        let dir = TempDir::new().unwrap();
        write_file(&dir.path().join(CONFIG_FILE_NAME), "{ not json");
        let err = load_registry(dir.path(), None).unwrap_err();
        assert!(matches!(err, LoaderError::Parse { .. }));
    }

    #[test]
    fn missing_user_global_file_is_silent() {
        let dir = TempDir::new().unwrap();
        // No file written.
        let reg = load_registry(dir.path(), None).unwrap();
        assert_eq!(reg.len(), 6);
    }
}
