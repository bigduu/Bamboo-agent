//! Permission storage for persisting whitelist configuration.
//!
//! This module provides functionality to save and load permission configurations
//! from persistent storage (e.g., Tauri config directory).

use std::path::PathBuf;

use crate::agent::tools::permission::config::{PermissionConfig, SerializablePermissionConfig};
use crate::core::Config;

/// Storage for permission configuration
///
/// This struct handles loading and saving permission configurations
/// to a persistent storage location.
#[derive(Debug, Clone)]
pub struct PermissionStorage {
    config_dir: PathBuf,
    filename: String,
}

impl PermissionStorage {
    /// The default filename for permission configuration
    pub const DEFAULT_FILENAME: &str = "permissions.json";

    /// Create a new permission storage with the given config directory
    pub fn new(config_dir: impl Into<PathBuf>) -> Self {
        Self {
            config_dir: config_dir.into(),
            filename: Self::DEFAULT_FILENAME.to_string(),
        }
    }

    /// Create a new permission storage with a custom filename
    pub fn with_filename(config_dir: impl Into<PathBuf>, filename: impl Into<String>) -> Self {
        Self {
            config_dir: config_dir.into(),
            filename: filename.into(),
        }
    }

    /// Get the full path to the permission config file
    pub fn config_path(&self) -> PathBuf {
        // Unified configuration: permissions are stored in `config.json`.
        self.config_dir.join("config.json")
    }

    fn legacy_permissions_path(&self) -> PathBuf {
        self.config_dir.join(&self.filename)
    }

    /// Load permission configuration from storage
    ///
    /// Returns `Ok(None)` if the file doesn't exist.
    /// Returns an error if the file exists but cannot be read or parsed.
    pub async fn load(&self) -> Result<Option<PermissionConfig>, PermissionStorageError> {
        const KEY: &str = "permissions";

        // Load unified config.json first.
        let config = Config::from_data_dir(Some(self.config_dir.clone()));
        if let Some(value) = config.extra.get(KEY).cloned() {
            let serializable: SerializablePermissionConfig = serde_json::from_value(value)
                .map_err(|e| PermissionStorageError::ParseError {
                    path: self.config_path(),
                    source: e,
                })?;
            return Ok(Some(PermissionConfig::from_serializable(serializable)));
        }

        // Legacy migration: permissions.json -> config.json["permissions"]
        let legacy = self.legacy_permissions_path();
        if !legacy.exists() {
            return Ok(None);
        }

        let content = tokio::fs::read_to_string(&legacy).await.map_err(|e| {
            PermissionStorageError::ReadError {
                path: legacy.clone(),
                source: e,
            }
        })?;
        if content.trim().is_empty() {
            return Ok(None);
        }

        let serializable: SerializablePermissionConfig =
            serde_json::from_str(&content).map_err(|e| PermissionStorageError::ParseError {
                path: legacy.clone(),
                source: e,
            })?;

        let mut config = config;
        config.extra.insert(
            KEY.to_string(),
            serde_json::to_value(&serializable).map_err(|e| {
                PermissionStorageError::SerializationError {
                    path: self.config_path(),
                    source: e,
                }
            })?,
        );

        // Persist unified config.
        let data_dir = self.config_dir.clone();
        tokio::task::spawn_blocking(move || config.save_to_dir(data_dir))
            .await
            .map_err(|e| PermissionStorageError::WriteError {
                path: self.config_path(),
                source: std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            })?
            .map_err(|e| PermissionStorageError::WriteError {
                path: self.config_path(),
                source: std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            })?;

        // Best-effort backup legacy file.
        if let Some(name) = legacy.file_name().and_then(|s| s.to_str()) {
            let backup = legacy.with_file_name(format!("{name}.migrated.bak"));
            let _ = tokio::fs::rename(&legacy, backup).await;
        }

        Ok(Some(PermissionConfig::from_serializable(serializable)))
    }

    /// Load permission configuration with fallback to default
    ///
    /// Returns the loaded config, or a default config if loading fails
    /// or the file doesn't exist.
    pub async fn load_or_default(&self) -> Result<PermissionConfig, PermissionStorageError> {
        match self.load().await {
            Ok(Some(config)) => Ok(config),
            Ok(None) => Ok(PermissionConfig::new()),
            Err(e) => Err(e),
        }
    }

    /// Save permission configuration to storage
    pub async fn save(&self, config: &PermissionConfig) -> Result<(), PermissionStorageError> {
        const KEY: &str = "permissions";

        // Ensure the config directory exists
        if !self.config_dir.exists() {
            tokio::fs::create_dir_all(&self.config_dir)
                .await
                .map_err(|e| PermissionStorageError::WriteError {
                    path: self.config_path(),
                    source: e,
                })?;
        }

        let serializable = config.to_serializable();
        let mut root = Config::from_data_dir(Some(self.config_dir.clone()));
        root.extra.insert(
            KEY.to_string(),
            serde_json::to_value(&serializable).map_err(|e| {
                PermissionStorageError::SerializationError {
                    path: self.config_path(),
                    source: e,
                }
            })?,
        );

        let data_dir = self.config_dir.clone();
        tokio::task::spawn_blocking(move || root.save_to_dir(data_dir))
            .await
            .map_err(|e| PermissionStorageError::WriteError {
                path: self.config_path(),
                source: std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            })?
            .map_err(|e| PermissionStorageError::WriteError {
                path: self.config_path(),
                source: std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            })?;

        Ok(())
    }

    /// Check if a configuration file exists
    pub fn exists(&self) -> bool {
        self.config_path().exists()
    }

    /// Delete the configuration file
    pub async fn delete(&self) -> Result<(), PermissionStorageError> {
        let path = self.config_path();

        if path.exists() {
            tokio::fs::remove_file(&path).await.map_err(|e| {
                PermissionStorageError::WriteError {
                    path: path.clone(),
                    source: e,
                }
            })?;
        }

        Ok(())
    }
}

/// Error type for permission storage operations
#[derive(Debug, thiserror::Error)]
pub enum PermissionStorageError {
    #[error("Failed to read permission config from {path}: {source}")]
    ReadError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to write permission config to {path}: {source}")]
    WriteError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to parse permission config from {path}: {source}")]
    ParseError {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error("Failed to serialize permission config for {path}: {source}")]
    SerializationError {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

/// Get the default permission storage location for Tauri apps
///
/// Returns `None` if the Bamboo data directory cannot be determined.
pub fn default_storage() -> Option<PermissionStorage> {
    // Keep storage consistent with the unified config.json location:
    // all persisted state lives under `paths::bamboo_dir()` (BAMBOO_DATA_DIR or `${HOME}/.bamboo`).
    Some(PermissionStorage::new(crate::core::paths::bamboo_dir()))
}

/// Get the default permission storage for a specific app name
pub fn app_storage(app_name: &str) -> Option<PermissionStorage> {
    // App-specific storage is a legacy escape hatch; prefer `default_storage()` to
    // avoid splitting persisted config across multiple roots.
    Some(PermissionStorage::new(
        crate::core::paths::bamboo_dir().join(app_name),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::tools::permission::config::{PermissionRule, PermissionType};

    #[tokio::test]
    async fn test_save_and_load() {
        let temp_dir = std::env::temp_dir().join("bamboo_permission_test");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;

        let storage = PermissionStorage::new(&temp_dir);

        // Create a config with some rules
        let config = PermissionConfig::new();
        config.add_rule(PermissionRule::new(PermissionType::WriteFile, "*.rs", true));
        config.add_rule(PermissionRule::new(
            PermissionType::ExecuteCommand,
            "cargo *",
            true,
        ));

        // Save the config
        storage.save(&config).await.unwrap();

        // Load it back
        let loaded = storage.load().await.unwrap().unwrap();

        // Verify the rules were saved
        let rules = loaded.get_rules();
        assert_eq!(rules.len(), 2);

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_load_nonexistent() {
        let temp_dir = std::env::temp_dir().join("bamboo_permission_test_nonexistent");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;

        let storage = PermissionStorage::new(&temp_dir);
        let result = storage.load().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_load_or_default() {
        let temp_dir = std::env::temp_dir().join("bamboo_permission_test_default");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;

        let storage = PermissionStorage::new(&temp_dir);

        // Should return default when file doesn't exist
        let config = storage.load_or_default().await.unwrap();
        assert!(config.is_enabled());

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}
