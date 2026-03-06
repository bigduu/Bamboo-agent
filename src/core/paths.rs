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
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .expect("Could not determine home directory")
                .join(".bamboo")
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::sync::{Mutex, OnceLock};

    #[test]
    fn test_resolve_bamboo_dir_prefers_env() {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("ENV_LOCK poisoned");

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
}
