use std::path::{Path, PathBuf};

/// Get Bamboo data directory (~/.bamboo)
pub fn bamboo_dir() -> PathBuf {
    std::env::var("BAMBOO_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .expect("Could not determine home directory")
                .join(".bamboo")
        })
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

/// Get sessions directory (~/.bamboo/sessions)
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

    #[test]
    fn test_sessions_dir_returns_bamboo_sessions() {
        // Set BAMBOO_DATA_DIR for test
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let bamboo_home = temp_dir.path().to_string_lossy().to_string();

        // Save current env
        let original = std::env::var_os("BAMBOO_DATA_DIR");

        std::env::set_var("BAMBOO_DATA_DIR", &bamboo_home);

        let sessions = sessions_dir();
        let sessions_str = sessions.to_str().unwrap();

        // Should end with /sessions
        assert!(sessions_str.ends_with("sessions"));

        // Restore original env
        if let Some(val) = original {
            std::env::set_var("BAMBOO_DATA_DIR", val);
        } else {
            std::env::remove_var("BAMBOO_DATA_DIR");
        }
    }
}
