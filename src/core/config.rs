//! Configuration management for Bamboo agent
//!
//! This module provides configuration types and loading logic for the entire
//! Bamboo agent system. It supports multiple LLM providers, proxy settings,
//! and both JSON and TOML configuration formats.
//!
//! # Configuration File
//!
//! Configuration is stored in `config.toml` (preferred) or `config.json` (legacy)
//! under the XDG-compliant data directory (typically `~/.local/share/bamboo/`).
//!
//! # Example (TOML)
//!
//! ```toml
//! provider = "anthropic"
//!
//! [providers.anthropic]
//! api_key = "sk-ant-..."
//! model = "claude-3-5-sonnet-20241022"
//!
//! [providers.openai]
//! api_key = "sk-..."
//! base_url = "https://api.openai.com/v1"
//! ```
//!
//! # Environment Variables
//!
//! - `BAMBOO_PROVIDER`: Override default provider
//! - `BAMBOO_HEADLESS_AUTH`: Enable headless authentication mode
//! - `HTTP_PROXY` / `HTTPS_PROXY`: Proxy settings

use serde::{Deserialize, Serialize};

/// Main configuration structure for Bamboo agent
///
/// Contains all settings needed to run the agent, including provider credentials,
/// proxy settings, and model selection.
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
/// ```toml
/// [providers.openai]
/// api_key = "sk-..."
/// base_url = "https://api.openai.com/v1"  # Optional
/// model = "gpt-4"  # Optional
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
/// ```toml
/// [providers.anthropic]
/// api_key = "sk-ant-..."
/// model = "claude-3-5-sonnet-20241022"
/// max_tokens = 4096  # Optional
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
/// ```toml
/// [providers.gemini]
/// api_key = "AIza..."
/// model = "gemini-2.0-flash-exp"
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
/// ```toml
/// [providers.copilot]
/// enabled = true
/// headless_auth = false  # Set to true for servers without browser
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

/// Returns the default provider name ("copilot")
fn default_provider() -> String {
    "copilot".to_string()
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
    /// 1. Try loading from `config.json` (XDG path)
    /// 2. Migrate old format if detected
    /// 3. Fallback to `config.toml` in current directory
    /// 4. Use defaults with environment variable overrides
    ///
    /// # Environment Variables
    ///
    /// - `MODEL`: Default model name
    /// - `BAMBOO_HEADLESS`: Enable headless authentication mode
    pub fn new() -> Self {
        use super::paths::config_json_path;

        let json_path = config_json_path();
        if json_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&json_path) {
                // Try to parse as old format first (for migration)
                if let Ok(old_config) = serde_json::from_str::<OldConfig>(&content) {
                    // Check if it has old-only fields
                    let has_old_fields = old_config.http_proxy_auth.is_some()
                        || old_config.https_proxy_auth.is_some()
                        || old_config.api_key.is_some()
                        || old_config.api_base.is_some();

                    if has_old_fields {
                        let migrated = migrate_config(old_config);
                        // Save migrated config
                        if let Ok(new_content) = serde_json::to_string_pretty(&migrated) {
                            let _ = std::fs::write(&json_path, new_content);
                        }
                        return migrated;
                    }
                }

                // Try to parse as new Config format
                if let Ok(config) = serde_json::from_str::<Config>(&content) {
                    return config;
                }
            }
        }

        // Fallback to legacy config.toml
        if std::path::Path::new(CONFIG_FILE_PATH).exists() {
            if let Ok(content) = std::fs::read_to_string(CONFIG_FILE_PATH) {
                if let Ok(old_config) = toml::from_str::<OldConfig>(&content) {
                    return migrate_config(old_config);
                }
            }
        }

        // Default config with environment variable overrides
        let mut config = Config {
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            model: None,
            headless_auth: false,
            provider: default_provider(),
            providers: ProviderConfigs::default(),
        };

        if let Ok(model) = std::env::var("MODEL") {
            config.model = Some(model);
        }
        if let Ok(headless) = std::env::var("BAMBOO_HEADLESS") {
            config.headless_auth = parse_bool_env(&headless);
        }
        config
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
        provider: default_provider(),
        providers: ProviderConfigs::default(),
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
            // Use XDG data directory instead of .bamboo
            let config_dir = self.path.join(".local").join("share").join("bamboo");
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
        let _xdg_data_home = EnvVarGuard::set("XDG_DATA_HOME", &format!("{}/.local/share", home));
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
        let _xdg_data_home = EnvVarGuard::set("XDG_DATA_HOME", &format!("{}/.local/share", home));
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
        let _xdg_data_home = EnvVarGuard::set("XDG_DATA_HOME", &format!("{}/.local/share", home));
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
        let _xdg_data_home = EnvVarGuard::set("XDG_DATA_HOME", &format!("{}/.local/share", home));

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
        let _xdg_data_home = EnvVarGuard::set("XDG_DATA_HOME", &format!("{}/.local/share", home));

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
}
