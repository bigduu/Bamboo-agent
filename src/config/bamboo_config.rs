//! Bamboo configuration management
//!
//! Handles loading/saving configuration from XDG-compliant paths

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::fs;
use anyhow::{Result, Context};

use super::xdg_paths;

/// Main configuration for Bamboo server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BambooConfig {
    /// Server configuration
    pub server: ServerConfig,

    /// Data directory (defaults to XDG_DATA_HOME/bamboo)
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
}

/// HTTP server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Port to listen on
    #[serde(default = "default_port")]
    pub port: u16,

    /// Bind address (127.0.0.1, 0.0.0.0, etc.)
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Static files directory (for Docker mode)
    pub static_dir: Option<PathBuf>,

    /// Worker count for Actix-web
    #[serde(default = "default_workers")]
    pub workers: usize,
}

fn default_port() -> u16 { 8080 }
fn default_bind() -> String { "127.0.0.1".to_string() }
fn default_workers() -> usize { 10 }
fn default_data_dir() -> PathBuf { xdg_paths::bamboo_data_dir() }

impl Default for BambooConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            data_dir: default_data_dir(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            bind: default_bind(),
            static_dir: None,
            workers: default_workers(),
        }
    }
}

impl BambooConfig {
    /// Load configuration from XDG config path
    ///
    /// Returns default config if file doesn't exist
    pub fn load() -> Result<Self> {
        let config_path = xdg_paths::bamboo_config_file();

        if !config_path.exists() {
            // Return default config if file doesn't exist
            return Ok(Self::default());
        }

        let content = fs::read_to_string(&config_path)
            .with_context(|| format!("Failed to read config file: {:?}", config_path))?;

        let config: BambooConfig = serde_json::from_str(&content)
            .with_context(|| "Failed to parse config file as JSON")?;

        Ok(config)
    }

    /// Save configuration to XDG config path
    pub fn save(&self) -> Result<()> {
        // Ensure config directory exists
        xdg_paths::ensure_bamboo_dirs()?;

        let config_path = xdg_paths::bamboo_config_file();

        let content = serde_json::to_string_pretty(self)
            .context("Failed to serialize config to JSON")?;

        fs::write(&config_path, content)
            .with_context(|| format!("Failed to write config file: {:?}", config_path))?;

        Ok(())
    }

    /// Load config with environment variable overrides
    ///
    /// Environment variables:
    /// - BAMBOO_PORT: Override server port
    /// - BAMBOO_BIND: Override bind address
    /// - BAMBOO_DATA_DIR: Override data directory
    pub fn from_env() -> Result<Self> {
        let mut config = Self::load()?;

        if let Ok(port) = std::env::var("BAMBOO_PORT") {
            config.server.port = port.parse()
                .context("Invalid BAMBOO_PORT value")?;
        }

        if let Ok(bind) = std::env::var("BAMBOO_BIND") {
            config.server.bind = bind;
        }

        if let Ok(data_dir) = std::env::var("BAMBOO_DATA_DIR") {
            config.data_dir = PathBuf::from(data_dir);
        }

        Ok(config)
    }

    /// Get the full server address (bind:port)
    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.server.bind, self.server.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BambooConfig::default();
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.server.bind, "127.0.0.1");
    }

    #[test]
    fn test_server_addr() {
        let config = BambooConfig::default();
        assert_eq!(config.server_addr(), "127.0.0.1:8080");
    }

    #[test]
    fn test_save_and_load() {
        let mut config = BambooConfig::default();
        config.server.port = 9000;
        config.server.bind = "0.0.0.0".to_string();

        // Save
        config.save().expect("Failed to save config");

        // Load
        let loaded = BambooConfig::load().expect("Failed to load config");

        // Verify
        assert_eq!(loaded.server.port, 9000);
        assert_eq!(loaded.server.bind, "0.0.0.0");
    }
}
