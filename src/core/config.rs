//! Configuration management for Bamboo agent
//!
//! This module provides unified configuration types and loading logic for the entire
//! Bamboo agent system. It supports multiple LLM providers, proxy settings,
//! and JSON configuration format.
//!
//! # Configuration File
//!
//! Configuration is stored in `config.json` under the unified data directory
//! (defaults to `~/.bamboo/`). Environment variables can override file values.
//!
//! # Example (JSON)
//!
//! ```json
//! {
//!   "provider": "anthropic",
//!   "server": {
//!     "port": 8080,
//!     "bind": "127.0.0.1"
//!   },
//!   "providers": {
//!     "anthropic": {
//!       "api_key": "sk-ant-...",
//!       "model": "claude-3-5-sonnet-20241022"
//!     },
//!     "openai": {
//!       "api_key": "sk-...",
//!       "base_url": "https://api.openai.com/v1"
//!     }
//!   }
//! }
//! ```
//!
//! # Priority Order
//!
//! Configuration values are loaded in this order (later overrides earlier):
//! 1. Code defaults (hardcoded default values)
//! 2. Config file values (from `~/.bamboo/config.json`)
//! 3. Environment variables (e.g., `BAMBOO_PORT`)
//! 4. CLI arguments (e.g., `--port 9000`)
//!
//! # Environment Variables
//!
//! - `BAMBOO_DATA_DIR`: Override data directory location
//! - `BAMBOO_PORT`: Override server port
//! - `BAMBOO_BIND`: Override server bind address
//! - `BAMBOO_PROVIDER`: Override default provider
//! - `BAMBOO_HEADLESS`: Enable headless authentication mode
//! - `MODEL`: Override default model

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Main configuration structure for Bamboo agent
///
/// Contains all settings needed to run the agent, including provider credentials,
/// proxy settings, model selection, and server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// HTTP proxy URL (e.g., `http://proxy.example.com:8080`)
    #[serde(default)]
    pub http_proxy: String,
    /// HTTPS proxy URL (e.g., `https://proxy.example.com:8080`)
    #[serde(default)]
    pub https_proxy: String,
    /// Proxy authentication credentials
    pub proxy_auth: Option<ProxyAuth>,
    /// Default model to use (can be overridden per provider)
    pub model: Option<String>,
    /// Deprecated: Use `providers.copilot.headless_auth` instead
    #[serde(default)]
    pub headless_auth: bool,

    /// Default LLM provider to use (e.g., "anthropic", "openai", "gemini", "copilot")
    #[serde(default = "default_provider")]
    pub provider: String,

    /// Provider-specific configurations
    #[serde(default)]
    pub providers: ProviderConfigs,

    /// HTTP server configuration
    #[serde(default)]
    pub server: ServerConfig,

    /// Data directory path (defaults to ~/.bamboo)
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
}

/// Container for provider-specific configurations
///
/// Each field is optional, allowing users to configure only the providers they need.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderConfigs {
    /// OpenAI provider configuration
    pub openai: Option<OpenAIConfig>,
    /// Anthropic provider configuration
    pub anthropic: Option<AnthropicConfig>,
    /// Google Gemini provider configuration
    pub gemini: Option<GeminiConfig>,
    /// GitHub Copilot provider configuration
    pub copilot: Option<CopilotConfig>,
}

/// OpenAI provider configuration
///
/// # Example
///
/// ```json
/// "openai": {
///   "api_key": "sk-...",
///   "base_url": "https://api.openai.com/v1",
///   "model": "gpt-4"
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAIConfig {
    /// OpenAI API key
    pub api_key: String,
    /// Custom API base URL (for Azure or self-hosted deployments)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Default model to use (e.g., "gpt-4", "gpt-3.5-turbo")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// Anthropic provider configuration
///
/// # Example
///
/// ```json
/// "anthropic": {
///   "api_key": "sk-ant-...",
///   "model": "claude-3-5-sonnet-20241022",
///   "max_tokens": 4096
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicConfig {
    /// Anthropic API key
    pub api_key: String,
    /// Custom API base URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Default model to use (e.g., "claude-3-5-sonnet-20241022")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Maximum tokens in model response
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
}

/// Google Gemini provider configuration
///
/// # Example
///
/// ```json
/// "gemini": {
///   "api_key": "AIza...",
///   "model": "gemini-2.0-flash-exp"
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiConfig {
    /// Google AI API key
    pub api_key: String,
    /// Custom API base URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Default model to use (e.g., "gemini-2.0-flash-exp")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// GitHub Copilot provider configuration
///
/// # Example
///
/// ```json
/// "copilot": {
///   "enabled": true,
///   "headless_auth": false
/// }
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CopilotConfig {
    /// Whether Copilot provider is enabled
    #[serde(default)]
    pub enabled: bool,
    /// Print login URL to console instead of opening browser
    #[serde(default)]
    pub headless_auth: bool,
}

/// Returns the default provider name ("anthropic")
fn default_provider() -> String {
    "anthropic".to_string()
}

/// Returns the default server port (8080)
fn default_port() -> u16 {
    8080
}

/// Returns the default bind address (127.0.0.1)
fn default_bind() -> String {
    "127.0.0.1".to_string()
}

/// Returns the default worker count (10)
fn default_workers() -> usize {
    10
}

/// Returns the default data directory (~/.bamboo)
fn default_data_dir() -> PathBuf {
    super::paths::bamboo_dir()
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

/// Proxy authentication credentials
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyAuth {
    /// Proxy username
    pub username: String,
    /// Proxy password
    pub password: String,
}

/// Configuration file name
const CONFIG_FILE_PATH: &str = "config.toml";

/// Parse a boolean value from environment variable strings
///
/// Accepts: "1", "true", "yes", "y", "on" (case-insensitive)
fn parse_bool_env(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on"
    )
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Config {
    /// Load configuration from file with environment variable overrides
    ///
    /// Configuration loading order:
    /// 1. Try loading from `config.json` (data_dir/config.json)
    /// 2. Migrate old format if detected
    /// 3. Fallback to `config.toml` in current directory
    /// 4. Use defaults
    /// 5. Apply environment variable overrides (highest priority)
    ///
    /// # Environment Variables
    ///
    /// - `BAMBOO_PORT`: Override server port
    /// - `BAMBOO_BIND`: Override bind address
    /// - `BAMBOO_DATA_DIR`: Override data directory
    /// - `BAMBOO_PROVIDER`: Override default provider
    /// - `MODEL`: Default model name
    /// - `BAMBOO_HEADLESS`: Enable headless authentication mode
    pub fn new() -> Self {
        Self::from_data_dir(None)
    }

    /// Load configuration from a specific data directory
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Optional data directory path. If None, uses default (~/.bamboo)
    pub fn from_data_dir(data_dir: Option<PathBuf>) -> Self {
        // Determine data_dir early (needed to find config file)
        let data_dir = data_dir
            .or_else(|| std::env::var("BAMBOO_DATA_DIR").ok().map(PathBuf::from))
            .unwrap_or_else(default_data_dir);

        let config_path = data_dir.join("config.json");

        let mut config = if config_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&config_path) {
                // Try to parse as old format first (for migration)
                if let Ok(old_config) = serde_json::from_str::<OldConfig>(&content) {
                    // Check if it has old-only fields (indicating a true old config that needs migration)
                    let has_old_fields = old_config.http_proxy_auth.is_some()
                        || old_config.https_proxy_auth.is_some()
                        || old_config.api_key.is_some()
                        || old_config.api_base.is_some();

                    if has_old_fields {
                        log::info!("Migrating old config format to new format");
                        let migrated = migrate_config(old_config);
                        // Save migrated config
                        if let Ok(new_content) = serde_json::to_string_pretty(&migrated) {
                            let _ = std::fs::write(&config_path, new_content);
                        }
                        migrated
                    } else {
                        // No old fields, so try to parse as new Config
                        // OldConfig successfully parsed common fields like http_proxy, model, provider, etc.
                        // Try Config, but if it fails (e.g., due to syntax errors), use OldConfig values
                        match serde_json::from_str::<Config>(&content) {
                            Ok(config) => config,
                            Err(_) => {
                                // Config parse failed, but OldConfig worked, so preserve those values
                                migrate_config(old_config)
                            }
                        }
                    }
                } else {
                    // Couldn't parse as OldConfig, try as Config
                    serde_json::from_str::<Config>(&content)
                        .unwrap_or_else(|_| Self::create_default())
                }
            } else {
                Self::create_default()
            }
        } else {
            // Fallback to legacy config.toml
            if std::path::Path::new(CONFIG_FILE_PATH).exists() {
                if let Ok(content) = std::fs::read_to_string(CONFIG_FILE_PATH) {
                    if let Ok(old_config) = toml::from_str::<OldConfig>(&content) {
                        migrate_config(old_config)
                    } else {
                        Self::create_default()
                    }
                } else {
                    Self::create_default()
                }
            } else {
                Self::create_default()
            }
        };

        // Ensure data_dir is set correctly
        config.data_dir = data_dir;

        // Apply environment variable overrides (highest priority)
        if let Ok(port) = std::env::var("BAMBOO_PORT") {
            if let Ok(port) = port.parse() {
                config.server.port = port;
            }
        }

        if let Ok(bind) = std::env::var("BAMBOO_BIND") {
            config.server.bind = bind;
        }

        // Note: BAMBOO_DATA_DIR already handled above
        if let Ok(provider) = std::env::var("BAMBOO_PROVIDER") {
            config.provider = provider;
        }

        if let Ok(model) = std::env::var("MODEL") {
            config.model = Some(model);
        }

        if let Ok(headless) = std::env::var("BAMBOO_HEADLESS") {
            config.headless_auth = parse_bool_env(&headless);
        }

        config
    }

    /// Create a default configuration without loading from file
    fn create_default() -> Self {
        Config {
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            model: None,
            headless_auth: false,
            provider: default_provider(),
            providers: ProviderConfigs::default(),
            server: ServerConfig::default(),
            data_dir: default_data_dir(),
        }
    }

    /// Get the full server address (bind:port)
    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.server.bind, self.server.port)
    }

    /// Save configuration to disk
    pub fn save(&self) -> Result<()> {
        let path = self.data_dir.join("config.json");

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create config dir: {:?}", parent))?;
        }

        let content =
            serde_json::to_string_pretty(self).context("Failed to serialize config to JSON")?;

        std::fs::write(&path, content)
            .with_context(|| format!("Failed to write config file: {:?}", path))?;

        Ok(())
    }
}

/// Legacy configuration format for backward compatibility
///
/// This struct is used to migrate old configuration files to the new format.
/// It supports the previous single-provider model.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OldConfig {
    #[serde(default)]
    http_proxy: String,
    #[serde(default)]
    https_proxy: String,
    #[serde(default)]
    http_proxy_auth: Option<ProxyAuth>,
    #[serde(default)]
    https_proxy_auth: Option<ProxyAuth>,
    api_key: Option<String>,
    api_base: Option<String>,
    model: Option<String>,
    #[serde(default)]
    headless_auth: bool,
    // Also capture new fields so we don't lose them during fallback
    #[serde(default = "default_provider")]
    provider: String,
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    providers: ProviderConfigs,
    #[serde(default)]
    data_dir: Option<PathBuf>,
}

/// Migrate old configuration format to new multi-provider format
///
/// Converts the legacy single-provider configuration to the new structure
/// with explicit provider configurations.
fn migrate_config(old: OldConfig) -> Config {
    // Log warning about deprecated fields
    if old.api_key.is_some() {
        log::warn!(
            "api_key is no longer used. CopilotClient automatically manages authentication."
        );
    }
    if old.api_base.is_some() {
        log::warn!(
            "api_base is no longer used. CopilotClient automatically manages API endpoints."
        );
    }

    Config {
        http_proxy: old.http_proxy,
        https_proxy: old.https_proxy,
        // Use https_proxy_auth if available, otherwise fallback to http_proxy_auth
        proxy_auth: old.https_proxy_auth.or(old.http_proxy_auth),
        model: old.model,
        headless_auth: old.headless_auth,
        provider: old.provider,
        providers: old.providers,
        server: old.server,
        data_dir: old.data_dir.unwrap_or_else(default_data_dir),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }

        fn unset(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            std::env::remove_var(key);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    struct TempHome {
        path: PathBuf,
    }

    impl TempHome {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "chat-core-config-test-{}-{}",
                std::process::id(),
                nanos
            ));
            std::fs::create_dir_all(&path).expect("failed to create temp home dir");
            Self { path }
        }

        fn set_config_json(&self, content: &str) {
            // Use .bamboo directory
            let config_dir = self.path.join(".bamboo");
            std::fs::create_dir_all(&config_dir).expect("failed to create config dir");
            std::fs::write(config_dir.join("config.json"), content)
                .expect("failed to write config.json");
        }
    }

    impl Drop for TempHome {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn parse_bool_env_true_values() {
        for value in ["1", "true", "TRUE", " yes ", "Y", "on"] {
            assert!(parse_bool_env(value), "value {value:?} should be true");
        }
    }

    #[test]
    fn parse_bool_env_false_values() {
        for value in ["0", "false", "no", "off", "", "  "] {
            assert!(!parse_bool_env(value), "value {value:?} should be false");
        }
    }

    #[test]
    fn config_new_ignores_http_proxy_env_vars() {
        let _lock = env_lock().lock().expect("env lock poisoned");
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "http_proxy": "",
  "https_proxy": ""
}"#,
        );

        let home = temp_home.path.to_string_lossy().to_string();
        let _home = EnvVarGuard::set("HOME", &home);
        let _http_proxy = EnvVarGuard::set("HTTP_PROXY", "http://env-proxy.example.com:8080");
        let _https_proxy = EnvVarGuard::set("HTTPS_PROXY", "http://env-proxy.example.com:8443");

        let config = Config::new();

        assert!(
            config.http_proxy.is_empty(),
            "config should ignore HTTP_PROXY env var"
        );
        assert!(
            config.https_proxy.is_empty(),
            "config should ignore HTTPS_PROXY env var"
        );
    }

    #[test]
    fn config_new_loads_config_when_proxy_fields_omitted() {
        let _lock = env_lock().lock().expect("env lock poisoned");
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "model": "gpt-4"
}"#,
        );

        let home = temp_home.path.to_string_lossy().to_string();
        let _home = EnvVarGuard::set("HOME", &home);
        let _http_proxy = EnvVarGuard::unset("HTTP_PROXY");
        let _https_proxy = EnvVarGuard::unset("HTTPS_PROXY");

        let config = Config::new();

        assert_eq!(
            config.model.as_deref(),
            Some("gpt-4"),
            "config should load model from config file even when proxy fields are omitted"
        );
        assert!(config.http_proxy.is_empty());
        assert!(config.https_proxy.is_empty());
    }

    #[test]
    fn config_new_ignores_proxy_env_vars_when_proxy_fields_omitted() {
        let _lock = env_lock().lock().expect("env lock poisoned");
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "model": "gpt-4"
}"#,
        );

        let home = temp_home.path.to_string_lossy().to_string();
        let _home = EnvVarGuard::set("HOME", &home);
        let _http_proxy = EnvVarGuard::set("HTTP_PROXY", "http://env-proxy.example.com:8080");
        let _https_proxy = EnvVarGuard::set("HTTPS_PROXY", "http://env-proxy.example.com:8443");

        let config = Config::new();

        assert_eq!(config.model.as_deref(), Some("gpt-4"));
        assert!(
            config.http_proxy.is_empty(),
            "config should keep http_proxy empty when field is omitted"
        );
        assert!(
            config.https_proxy.is_empty(),
            "config should keep https_proxy empty when field is omitted"
        );
    }

    #[test]
    fn config_migrates_old_format_to_new() {
        let _lock = env_lock().lock().expect("env lock poisoned");
        let temp_home = TempHome::new();

        // Create config with old format
        temp_home.set_config_json(
            r#"{
  "http_proxy": "http://proxy.example.com:8080",
  "https_proxy": "http://proxy.example.com:8443",
  "http_proxy_auth": {
    "username": "http_user",
    "password": "http_pass"
  },
  "https_proxy_auth": {
    "username": "https_user",
    "password": "https_pass"
  },
  "api_key": "old_key",
  "api_base": "https://old.api.com",
  "model": "gpt-4",
  "headless_auth": true
}"#,
        );

        let home = temp_home.path.to_string_lossy().to_string();
        let _home = EnvVarGuard::set("HOME", &home);

        let config = Config::new();

        // Verify migration
        assert_eq!(config.http_proxy, "http://proxy.example.com:8080");
        assert_eq!(config.https_proxy, "http://proxy.example.com:8443");

        // Should use https_proxy_auth (higher priority)
        assert!(config.proxy_auth.is_some());
        let auth = config.proxy_auth.unwrap();
        assert_eq!(auth.username, "https_user");
        assert_eq!(auth.password, "https_pass");

        // Model and headless_auth should be preserved
        assert_eq!(config.model.as_deref(), Some("gpt-4"));
        assert!(config.headless_auth);

        // api_key and api_base are no longer in Config
    }

    #[test]
    fn config_migrates_only_http_proxy_auth() {
        let _lock = env_lock().lock().expect("env lock poisoned");
        let temp_home = TempHome::new();

        // Create config with only http_proxy_auth
        temp_home.set_config_json(
            r#"{
  "http_proxy": "http://proxy.example.com:8080",
  "http_proxy_auth": {
    "username": "http_user",
    "password": "http_pass"
  }
}"#,
        );

        let home = temp_home.path.to_string_lossy().to_string();
        let _home = EnvVarGuard::set("HOME", &home);

        let config = Config::new();

        // Should fallback to http_proxy_auth when https_proxy_auth is absent
        assert!(
            config.proxy_auth.is_some(),
            "proxy_auth should be migrated from http_proxy_auth"
        );
        let auth = config.proxy_auth.unwrap();
        assert_eq!(auth.username, "http_user");
        assert_eq!(auth.password, "http_pass");
    }

    #[test]
    fn test_server_config_defaults() {
        let _lock = env_lock().lock().expect("env lock poisoned");
        let temp_home = TempHome::new();

        // Set temp home BEFORE creating config
        let home = temp_home.path.to_string_lossy().to_string();
        let _home = EnvVarGuard::set("HOME", &home);

        let config = Config::default();
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.server.bind, "127.0.0.1");
        assert_eq!(config.server.workers, 10);
        assert!(config.server.static_dir.is_none());
    }

    #[test]
    fn test_server_addr() {
        let mut config = Config::default();
        config.server.port = 9000;
        config.server.bind = "0.0.0.0".to_string();
        assert_eq!(config.server_addr(), "0.0.0.0:9000");
    }

    #[test]
    fn test_env_var_overrides() {
        let _lock = env_lock().lock().expect("env lock poisoned");
        let temp_home = TempHome::new();

        // Set temp home to avoid loading real config
        let home = temp_home.path.to_string_lossy().to_string();
        let _home = EnvVarGuard::set("HOME", &home);

        let _port = EnvVarGuard::set("BAMBOO_PORT", "9999");
        let _bind = EnvVarGuard::set("BAMBOO_BIND", "192.168.1.1");
        let _provider = EnvVarGuard::set("BAMBOO_PROVIDER", "openai");

        let config = Config::new();
        assert_eq!(config.server.port, 9999);
        assert_eq!(config.server.bind, "192.168.1.1");
        assert_eq!(config.provider, "openai");
    }

    #[test]
    fn test_config_save_and_load() {
        let _lock = env_lock().lock().expect("env lock poisoned");
        let temp_home = TempHome::new();

        // Set temp home BEFORE creating config
        let home = temp_home.path.to_string_lossy().to_string();
        let _home = EnvVarGuard::set("HOME", &home);

        let mut config = Config::default();
        config.server.port = 9000;
        config.server.bind = "0.0.0.0".to_string();
        config.provider = "anthropic".to_string();

        // Save
        config.save().expect("Failed to save config");

        // Load again
        let loaded = Config::new();

        // Verify
        assert_eq!(loaded.server.port, 9000);
        assert_eq!(loaded.server.bind, "0.0.0.0");
        assert_eq!(loaded.provider, "anthropic");
    }
}
