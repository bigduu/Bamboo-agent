//! Path management for Bamboo
//!
//! All Bamboo data is stored under ~/.bamboo/ directory:
//! - Config: ~/.bamboo/config.json
//! - Data: ~/.bamboo/ (sessions, skills, workflows, etc.)
//! - Cache: ~/.bamboo/cache/
//! - Runtime: ~/.bamboo/runtime/

use anyhow::{Context, Result};
use std::env;
use std::fs;
use std::path::PathBuf;

/// Get the Bamboo home directory (~/.bamboo)
///
/// Can be overridden with BAMBOO_DATA_DIR environment variable
pub fn bamboo_home() -> PathBuf {
    env::var("BAMBOO_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .expect("Could not determine home directory")
                .join(".bamboo")
        })
}

/// Bamboo config directory: ~/.bamboo/
pub fn bamboo_config_dir() -> PathBuf {
    bamboo_home()
}

/// Bamboo data directory: ~/.bamboo/
pub fn bamboo_data_dir() -> PathBuf {
    bamboo_home()
}

/// Bamboo cache directory: ~/.bamboo/cache/
pub fn bamboo_cache_dir() -> PathBuf {
    bamboo_home().join("cache")
}

/// Bamboo runtime directory: ~/.bamboo/runtime/
pub fn bamboo_runtime_dir() -> PathBuf {
    bamboo_home().join("runtime")
}

/// Bamboo config file path: ~/.bamboo/config.json
pub fn bamboo_config_file() -> PathBuf {
    bamboo_home().join("config.json")
}

/// Ensure a directory exists, creating it if necessary
fn ensure_dir_exists(path: &PathBuf) -> Result<()> {
    if !path.exists() {
        fs::create_dir_all(path)
            .with_context(|| format!("Failed to create directory: {:?}", path))?;
    }
    Ok(())
}

/// Ensure all Bamboo directories exist
pub fn ensure_bamboo_dirs() -> Result<()> {
    ensure_dir_exists(&bamboo_home())?;
    ensure_dir_exists(&bamboo_cache_dir())?;
    ensure_dir_exists(&bamboo_runtime_dir())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn test_bamboo_paths() {
        // Test that paths are generated correctly
        let home = bamboo_home();
        let config = bamboo_config_dir();
        let data = bamboo_data_dir();
        let cache = bamboo_cache_dir();

        // All should end with .bamboo or be subdirectories
        assert!(home.ends_with(".bamboo"));
        assert!(config.ends_with(".bamboo"));
        assert!(data.ends_with(".bamboo"));
        assert!(cache.ends_with("cache"));
    }

    #[test]
    fn test_ensure_dirs() {
        // Avoid writing to real user directories in unit tests.
        // Use BAMBOO_DATA_DIR to override the home directory
        static ENV_LOCK: Mutex<()> = Mutex::new(());
        let _guard = ENV_LOCK.lock().expect("env lock");

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let bamboo_home_override = temp_dir.path().join("bamboo");

        let old_home = env::var("BAMBOO_DATA_DIR").ok();
        env::set_var("BAMBOO_DATA_DIR", &bamboo_home_override);

        let result = ensure_bamboo_dirs();

        // Restore environment
        match old_home {
            Some(v) => env::set_var("BAMBOO_DATA_DIR", v),
            None => env::remove_var("BAMBOO_DATA_DIR"),
        }

        assert!(result.is_ok());
    }
}
