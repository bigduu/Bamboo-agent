//! Permission storage for persisting whitelist configuration.
//!
//! This module provides functionality to save and load permission configurations
//! from persistent storage (e.g., Tauri config directory).

use std::path::PathBuf;

use crate::permission::config::{PermissionConfig, SerializablePermissionConfig};

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
        self.config_dir.join(&self.filename)
    }

    /// Load permission configuration from storage
    ///
    /// Returns `Ok(None)` if the file doesn't exist.
    /// Returns an error if the file exists but cannot be read or parsed.
    pub async fn load(&self) -> Result<Option<PermissionConfig>, PermissionStorageError> {
        let path = self.config_path();

        if !path.exists() {
            return Ok(None);
        }

        let content = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| PermissionStorageError::ReadError {
                path: path.clone(),
                source: e,
            })?;

        if content.trim().is_empty() {
            return Ok(None);
        }

        let serializable: SerializablePermissionConfig =
            serde_json::from_str(&content).map_err(|e| PermissionStorageError::ParseError {
                path: path.clone(),
                source: e,
            })?;

        Ok(Some(PermissionConfig::from_serializable(serializable)))
    }

    /// Load permission configuration with fallback to default
    ///
    /// Returns the loaded config, or a default config if loading fails
    /// or the file doesn't exist.
    pub async fn load_or_default(&self,
    ) -> Result<PermissionConfig, PermissionStorageError> {
        match self.load().await {
            Ok(Some(config)) => Ok(config),
            Ok(None) => Ok(PermissionConfig::new()),
            Err(e) => Err(e),
        }
    }

    /// Save permission configuration to storage
    pub async fn save(
        &self,
        config: &PermissionConfig,
    ) -> Result<(), PermissionStorageError> {
        let path = self.config_path();

        // Ensure the config directory exists
        if !self.config_dir.exists() {
            tokio::fs::create_dir_all(&self.config_dir)
                .await
                .map_err(|e| PermissionStorageError::WriteError {
                    path: path.clone(),
                    source: e,
                })?;
        }

        let serializable = config.to_serializable();
        let content = serde_json::to_string_pretty(&serializable).map_err(|e| {
            PermissionStorageError::SerializationError {
                path: path.clone(),
                source: e,
            }
        })?;

        tokio::fs::write(&path, content).await.map_err(|e| {
            PermissionStorageError::WriteError {
                path: path.clone(),
                source: e,
            }
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
            tokio::fs::remove_file(&path)
                .await
                .map_err(|e| PermissionStorageError::WriteError {
                    path: path.clone(),
                    source: e,
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
/// Returns `None` if the config directory cannot be determined.
pub fn default_storage() -> Option<PermissionStorage> {
    dirs::config_dir().map(|config_dir| {
        let app_config_dir = config_dir.join("bamboo");
        PermissionStorage::new(app_config_dir)
    })
}

/// Get the default permission storage for a specific app name
pub fn app_storage(app_name: &str) -> Option<PermissionStorage> {
    dirs::config_dir().map(|config_dir| {
        let app_config_dir = config_dir.join(app_name);
        PermissionStorage::new(app_config_dir)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permission::config::{PermissionRule, PermissionType};

    #[tokio::test]
    async fn test_save_and_load() {
        let temp_dir = std::env::temp_dir().join("bamboo_permission_test");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;

        let storage = PermissionStorage::new(&temp_dir);

        // Create a config with some rules
        let config = PermissionConfig::new();
        config.add_rule(PermissionRule::new(PermissionType::WriteFile, "*.rs", true));
        config.add_rule(PermissionRule::new(PermissionType::ExecuteCommand, "cargo *", true));

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
