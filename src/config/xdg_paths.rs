//! XDG Base Directory specification implementation for Bamboo
//!
//! Follows the XDG Base Directory specification: <https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html>
//!
//! Default paths:
//! - Config: ~/.config/bamboo/
//! - Data: ~/.local/share/bamboo/
//! - Cache: ~/.cache/bamboo/
//! - Runtime: /tmp/bamboo-$UID/ (or $XDG_RUNTIME_DIR/bamboo/)

use anyhow::{Context, Result};
use std::env;
use std::fs;
use std::path::PathBuf;

/// Get XDG_CONFIG_HOME or default (~/.config)
pub fn xdg_config_home() -> PathBuf {
    env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .expect("Could not determine home directory")
                .join(".config")
        })
}

/// Get XDG_DATA_HOME or default (~/.local/share)
pub fn xdg_data_home() -> PathBuf {
    env::var("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .expect("Could not determine home directory")
                .join(".local")
                .join("share")
        })
}

/// Get XDG_CACHE_HOME or default (~/.cache)
pub fn xdg_cache_home() -> PathBuf {
    env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .expect("Could not determine home directory")
                .join(".cache")
        })
}

/// Get XDG_RUNTIME_DIR or fallback
/// On Unix: /tmp/bamboo-$UID
/// On Windows: %TEMP%\bamboo
pub fn xdg_runtime_dir() -> PathBuf {
    env::var("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            #[cfg(unix)]
            {
                let uid = unsafe { libc::getuid() };
                PathBuf::from(format!("/tmp/bamboo-{}", uid))
            }
            #[cfg(windows)]
            {
                // On Windows, use the temp directory
                std::env::temp_dir().join("bamboo")
            }
            #[cfg(not(any(unix, windows)))]
            {
                // Fallback for other platforms
                std::env::temp_dir().join("bamboo")
            }
        })
}

/// Bamboo config directory: $XDG_CONFIG_HOME/bamboo/
pub fn bamboo_config_dir() -> PathBuf {
    xdg_config_home().join("bamboo")
}

/// Bamboo data directory: $XDG_DATA_HOME/bamboo/
pub fn bamboo_data_dir() -> PathBuf {
    xdg_data_home().join("bamboo")
}

/// Bamboo cache directory: $XDG_CACHE_HOME/bamboo/
pub fn bamboo_cache_dir() -> PathBuf {
    xdg_cache_home().join("bamboo")
}

/// Bamboo runtime directory: $XDG_RUNTIME_DIR/bamboo/
pub fn bamboo_runtime_dir() -> PathBuf {
    xdg_runtime_dir().join("bamboo")
}

/// Bamboo config file path: $XDG_CONFIG_HOME/bamboo/config.json
pub fn bamboo_config_file() -> PathBuf {
    bamboo_config_dir().join("config.json")
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
    ensure_dir_exists(&bamboo_config_dir())?;
    ensure_dir_exists(&bamboo_data_dir())?;
    ensure_dir_exists(&bamboo_cache_dir())?;
    ensure_dir_exists(&bamboo_runtime_dir())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn test_xdg_paths() {
        // Test that paths are generated correctly
        let config = bamboo_config_dir();
        let data = bamboo_data_dir();
        let cache = bamboo_cache_dir();

        // Should end with /bamboo
        assert!(config.ends_with("bamboo"));
        assert!(data.ends_with("bamboo"));
        assert!(cache.ends_with("bamboo"));
    }

    #[test]
    fn test_ensure_dirs() {
        // Avoid writing to real user directories in unit tests.
        //
        // Note: environment variables are process-global, so guard against tests running in
        // parallel while we temporarily override XDG paths.
        static ENV_LOCK: Mutex<()> = Mutex::new(());
        let _guard = ENV_LOCK.lock().expect("env lock");

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config_home = temp_dir.path().join("config");
        let data_home = temp_dir.path().join("data");
        let cache_home = temp_dir.path().join("cache");
        let runtime_dir = temp_dir.path().join("runtime");

        let old_config_home = env::var("XDG_CONFIG_HOME").ok();
        let old_data_home = env::var("XDG_DATA_HOME").ok();
        let old_cache_home = env::var("XDG_CACHE_HOME").ok();
        let old_runtime_dir = env::var("XDG_RUNTIME_DIR").ok();

        env::set_var("XDG_CONFIG_HOME", &config_home);
        env::set_var("XDG_DATA_HOME", &data_home);
        env::set_var("XDG_CACHE_HOME", &cache_home);
        env::set_var("XDG_RUNTIME_DIR", &runtime_dir);

        let result = ensure_bamboo_dirs();

        // Restore environment.
        match old_config_home {
            Some(v) => env::set_var("XDG_CONFIG_HOME", v),
            None => env::remove_var("XDG_CONFIG_HOME"),
        }
        match old_data_home {
            Some(v) => env::set_var("XDG_DATA_HOME", v),
            None => env::remove_var("XDG_DATA_HOME"),
        }
        match old_cache_home {
            Some(v) => env::set_var("XDG_CACHE_HOME", v),
            None => env::remove_var("XDG_CACHE_HOME"),
        }
        match old_runtime_dir {
            Some(v) => env::set_var("XDG_RUNTIME_DIR", v),
            None => env::remove_var("XDG_RUNTIME_DIR"),
        }

        assert!(result.is_ok());
    }
}
