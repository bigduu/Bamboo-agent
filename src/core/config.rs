//! Configuration management for Bamboo agent
//!
//! This module provides unified configuration types and loading logic for the entire
//! Bamboo agent system. It supports multiple LLM providers, proxy settings,
//! and JSON configuration format.
//!
//! # Configuration File
//!
//! Configuration is stored in `config.json` under the unified data directory
//! (defaults to `${HOME}/.bamboo/`). Environment variables can override file values.
//!
//! # Example (JSON)
//!
//! ```json
//! {
//!   "provider": "anthropic",
//!   "server": {
//!     "port": 9562,
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
//! 2. Config file values (from `${HOME}/.bamboo/config.json`)
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

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write;
use std::path::PathBuf;
use std::sync::RwLock;

use crate::agent::tools::normalize_tool_ref;
use crate::core::keyword_masking::KeywordMaskingConfig;
use crate::core::model_mapping::{AnthropicModelMapping, GeminiModelMapping};
use crate::core::ReasoningEffort;

/// A user-managed environment variable that is injected into Bash tool processes.
///
/// Secret entries are encrypted at rest: `value` is empty on disk and populated
/// in memory after hydration from `value_encrypted`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvVarEntry {
    /// Variable name (must match `^[A-Za-z_][A-Za-z0-9_]*$`).
    pub name: String,
    /// Plaintext value – populated in memory after hydration.
    /// For `secret=true` entries this field is empty on disk.
    #[serde(default)]
    pub value: String,
    /// Whether this variable contains sensitive data (token, password, etc.).
    #[serde(default)]
    pub secret: bool,
    /// Encrypted ciphertext (only present on disk for secret entries).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_encrypted: Option<String>,
    /// Optional human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

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
    ///
    /// Note: this is kept in-memory only. On disk we store `proxy_auth_encrypted`.
    #[serde(skip_serializing)]
    pub proxy_auth: Option<ProxyAuth>,
    /// Encrypted proxy authentication credentials (nonce:ciphertext)
    ///
    /// This is the at-rest storage representation. When present, Bamboo will
    /// decrypt it into `proxy_auth` at load time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_auth_encrypted: Option<String>,
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

    /// Global keyword masking configuration.
    ///
    /// Previously persisted in `keyword_masking.json` (now unified into `config.json`).
    #[serde(default)]
    pub keyword_masking: KeywordMaskingConfig,

    /// Anthropic model mapping configuration.
    ///
    /// Previously persisted in `anthropic-model-mapping.json` (now unified into `config.json`).
    #[serde(default)]
    pub anthropic_model_mapping: AnthropicModelMapping,

    /// Gemini model mapping configuration.
    ///
    /// Previously persisted in `gemini-model-mapping.json` (now unified into `config.json`).
    #[serde(default)]
    pub gemini_model_mapping: GeminiModelMapping,

    /// Request preflight hooks.
    ///
    /// These hooks can inspect and rewrite outgoing requests before they are sent upstream
    /// (e.g. image fallback behavior for text-only models).
    #[serde(default)]
    pub hooks: HooksConfig,

    /// Global tool toggles.
    ///
    /// Any tool listed in `disabled` is omitted from the tool schemas sent to the LLM.
    #[serde(default, skip_serializing_if = "ToolsConfig::is_empty")]
    pub tools: ToolsConfig,

    /// User-managed environment variables injected into Bash tool processes.
    ///
    /// Secret entries are encrypted at rest; plaintext values are hydrated in memory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env_vars: Vec<EnvVarEntry>,

    /// MCP server configuration.
    ///
    /// Previously persisted in `mcp.json` (now unified into `config.json`).
    // On disk we use the mainstream `mcpServers` key (matching Claude Desktop / MCP ecosystem
    // conventions). We still accept the legacy `mcp` key for backward compatibility.
    #[serde(default, rename = "mcpServers", alias = "mcp")]
    pub mcp: crate::agent::mcp::McpConfig,

    /// Extension fields stored at the root of `config.json`.
    ///
    /// This keeps the config forward-compatible and allows unrelated subsystems
    /// (e.g. setup UI state) to persist their own keys without getting dropped by
    /// typed (de)serialization.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Container for provider-specific configurations
///
/// Each field is optional, allowing users to configure only the providers they need.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderConfigs {
    /// OpenAI provider configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai: Option<OpenAIConfig>,
    /// Anthropic provider configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anthropic: Option<AnthropicConfig>,
    /// Google Gemini provider configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gemini: Option<GeminiConfig>,
    /// GitHub Copilot provider configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copilot: Option<CopilotConfig>,

    /// Preserve unknown provider keys (forward compatibility).
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Request hook configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HooksConfig {
    /// Image fallback behavior for OpenAI-compatible requests (chat/responses).
    #[serde(default)]
    pub image_fallback: ImageFallbackHookConfig,
}

/// Request override configuration for provider-specific HTTP behavior.
///
/// Overrides are merged in this order (later wins):
/// 1. `common`
/// 2. `endpoints[endpoint]`
/// 3. matching `rules` (sorted by specificity)
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RequestOverridesConfig {
    /// Overrides applied to all endpoints.
    #[serde(default, skip_serializing_if = "RequestScopeOverride::is_empty")]
    pub common: RequestScopeOverride,
    /// Endpoint-specific overrides (`chat_completions`, `responses`, `messages`, etc.).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub endpoints: BTreeMap<String, RequestScopeOverride>,
    /// Model-conditional overrides.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<ModelRequestRule>,
}

/// A conditional override rule matching a model pattern.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelRequestRule {
    /// Model pattern (exact: `gpt-4o`, prefix wildcard: `gpt-5*`).
    pub model_pattern: String,
    /// Optional endpoint constraint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Overrides applied when this rule matches.
    #[serde(default, skip_serializing_if = "RequestScopeOverride::is_empty")]
    pub scope: RequestScopeOverride,
}

/// Request overrides applied in a specific scope.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RequestScopeOverride {
    /// Extra or overridden HTTP headers.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, TemplateExpr>,
    /// JSON body patch operations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub body_patch: Vec<BodyPatch>,
}

impl RequestScopeOverride {
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty() && self.body_patch.is_empty()
    }
}

/// Body patch operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BodyPatch {
    /// Target path (`foo.bar.0` or `/foo/bar/0`).
    pub path: String,
    /// Operation type.
    #[serde(default)]
    pub op: BodyPatchOp,
    /// Value for `set` operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<PatchValue>,
}

/// Supported body patch operations.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BodyPatchOp {
    #[default]
    Set,
    Remove,
}

/// Body patch value: either a template expression or a raw JSON value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PatchValue {
    Template(TemplateExpr),
    Json(Value),
}

/// String template expression used by headers/body patch values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TemplateExpr {
    /// Shorthand literal value.
    Literal(String),
    /// Structured template expression.
    Structured(TemplateExprSpec),
}

/// Structured template expression.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TemplateExprSpec {
    /// Literal string value.
    Literal { value: String },
    /// Reference a value from Bamboo env vars.
    EnvRef {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fallback: Option<String>,
    },
    /// Generate a runtime value.
    Generated { generator: GeneratedValue },
    /// Format string with placeholders (`{env:NAME}`, `{uuid}`, `{unix_ms}`).
    Format { template: String },
}

/// Supported generated value kinds.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GeneratedValue {
    Uuid,
    UnixMs,
}

/// Global tool toggle configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ToolsConfig {
    /// Tool names that are disabled globally.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled: Vec<String>,
}

impl ToolsConfig {
    fn is_empty(&self) -> bool {
        self.disabled.is_empty()
    }
}

/// When a request contains image parts but the effective provider path is text-only,
/// we can either:
/// - error fast (preferred for strict setups), or
/// - degrade gracefully by replacing images with a placeholder text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageFallbackHookConfig {
    #[serde(default = "default_true_hooks")]
    pub enabled: bool,

    /// "placeholder" (default) or "error"
    #[serde(default = "default_image_fallback_mode")]
    pub mode: String,
}

impl Default for ImageFallbackHookConfig {
    fn default() -> Self {
        Self {
            enabled: default_true_hooks(),
            mode: default_image_fallback_mode(),
        }
    }
}

fn default_image_fallback_mode() -> String {
    "placeholder".to_string()
}

fn default_true_hooks() -> bool {
    // Default to disabled so image inputs are preserved unless the user explicitly
    // opts into fallback rewriting (placeholder/error/ocr).
    false
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
    /// OpenAI API key (plaintext, in-memory only).
    ///
    /// On disk this is stored as `api_key_encrypted` and hydrated on load.
    #[serde(default, skip_serializing)]
    pub api_key: String,
    /// Encrypted OpenAI API key (nonce:ciphertext).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_encrypted: Option<String>,
    /// Custom API base URL (for Azure or self-hosted deployments)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Default model to use (e.g., "gpt-4", "gpt-3.5-turbo")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Fast/cheap model for lightweight tasks (title generation and summarization).
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,
    /// Vision-capable model for image understanding tasks.
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    /// Default reasoning effort for OpenAI requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,

    /// Models that must use the OpenAI Responses API upstream (instead of chat/completions).
    ///
    /// Example:
    /// ```json
    /// "responses_only_models": ["gpt-5.3-codex", "gpt-5*"]
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses_only_models: Vec<String>,
    /// Optional request overrides (headers/body patches/model rules).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_overrides: Option<RequestOverridesConfig>,

    /// Preserve unknown keys under `providers.openai`.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
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
    /// Anthropic API key (plaintext, in-memory only).
    ///
    /// On disk this is stored as `api_key_encrypted` and hydrated on load.
    #[serde(default, skip_serializing)]
    pub api_key: String,
    /// Encrypted Anthropic API key (nonce:ciphertext).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_encrypted: Option<String>,
    /// Custom API base URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Default model to use (e.g., "claude-3-5-sonnet-20241022")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Fast/cheap model for lightweight tasks (title generation, mermaid fix, summarization).
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,
    /// Vision-capable model for image understanding tasks.
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    /// Maximum tokens in model response
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    /// Default reasoning effort for Anthropic requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Optional request overrides (headers/body patches/model rules).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_overrides: Option<RequestOverridesConfig>,

    /// Preserve unknown keys under `providers.anthropic`.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
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
    /// Google AI API key (plaintext, in-memory only).
    ///
    /// On disk this is stored as `api_key_encrypted` and hydrated on load.
    #[serde(default, skip_serializing)]
    pub api_key: String,
    /// Encrypted Google AI API key (nonce:ciphertext).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_encrypted: Option<String>,
    /// Custom API base URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Default model to use (e.g., "gemini-2.0-flash-exp")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Fast/cheap model for lightweight tasks (title generation, mermaid fix, summarization).
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,
    /// Vision-capable model for image understanding tasks.
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    /// Default reasoning effort for Gemini requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Optional request overrides (headers/body patches/model rules).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_overrides: Option<RequestOverridesConfig>,

    /// Preserve unknown keys under `providers.gemini`.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// GitHub Copilot provider configuration
///
/// # Example
///
/// ```json
/// "copilot": {
///   "enabled": true,
///   "headless_auth": false,
///   "model": "gpt-4o"
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
    /// Default model to use for Copilot (used when clients request the "default" model)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Fast/cheap model for lightweight tasks (title generation, mermaid fix, summarization).
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,
    /// Vision-capable model for image understanding tasks.
    /// Falls back to `model` when not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    /// Default reasoning effort for Copilot requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,

    /// Models that must use the OpenAI Responses API upstream (instead of chat/completions).
    ///
    /// This is useful for newer Copilot models that only support Responses-style requests.
    ///
    /// Example:
    /// ```json
    /// "responses_only_models": ["gpt-5.3-codex", "gpt-5*"]
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses_only_models: Vec<String>,
    /// Optional request overrides (headers/body patches/model rules).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_overrides: Option<RequestOverridesConfig>,

    /// Preserve unknown keys under `providers.copilot`.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Returns the default provider name ("anthropic")
fn default_provider() -> String {
    "anthropic".to_string()
}

/// Returns the default server port (9562)
fn default_port() -> u16 {
    9562
}

/// Returns the default bind address (127.0.0.1)
fn default_bind() -> String {
    "127.0.0.1".to_string()
}

/// Returns the default worker count (10)
fn default_workers() -> usize {
    10
}

/// Returns the default data directory (`BAMBOO_DATA_DIR` or `${HOME}/.bamboo`)
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

    /// Preserve unknown keys under `server`.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            bind: default_bind(),
            static_dir: None,
            workers: default_workers(),
            extra: BTreeMap::new(),
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

/// Global cache of user-managed env vars for injection into child processes.
///
/// Updated whenever the config is loaded or reloaded via [`Config::publish_env_vars`].
static ENV_VARS_CACHE: std::sync::LazyLock<RwLock<HashMap<String, String>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

impl Config {
    /// Load configuration from file with environment variable overrides
    ///
    /// Configuration loading order:
    /// 1. Try loading from `config.json` (`{data_dir}/config.json`)
    /// 2. Use defaults
    /// 3. Apply environment variable overrides (highest priority)
    ///
    /// # Environment Variables
    ///
    /// - `BAMBOO_PORT`: Override server port
    /// - `BAMBOO_BIND`: Override bind address
    /// - `BAMBOO_DATA_DIR`: Override data directory
    /// - `BAMBOO_PROVIDER`: Override default provider
    /// - `BAMBOO_HEADLESS`: Enable headless authentication mode
    pub fn new() -> Self {
        Self::from_data_dir(None)
    }

    /// Load configuration from a specific data directory
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Optional data directory path. If None, uses default (`BAMBOO_DATA_DIR` or `${HOME}/.bamboo`)
    pub fn from_data_dir(data_dir: Option<PathBuf>) -> Self {
        // Determine data_dir early (needed to find config file)
        let data_dir = data_dir
            .or_else(|| std::env::var("BAMBOO_DATA_DIR").ok().map(PathBuf::from))
            .unwrap_or_else(default_data_dir);

        let config_path = data_dir.join("config.json");

        let mut config = if config_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&config_path) {
                serde_json::from_str::<Config>(&content)
                    .map(|mut config| {
                        config.hydrate_proxy_auth_from_encrypted();
                        config.hydrate_provider_api_keys_from_encrypted();
                        config.hydrate_mcp_secrets_from_encrypted();
                        config.hydrate_env_vars_from_encrypted();
                        config.normalize_tool_settings();
                        config
                    })
                    .unwrap_or_else(|e| {
                        tracing::warn!("Failed to parse config.json ({}), using defaults", e);
                        Self::create_default()
                    })
            } else {
                Self::create_default()
            }
        } else {
            Self::create_default()
        };

        // Decrypt encrypted proxy auth into in-memory plaintext form.
        config.hydrate_proxy_auth_from_encrypted();
        // Decrypt encrypted provider API keys into in-memory plaintext form.
        config.hydrate_provider_api_keys_from_encrypted();
        // Decrypt encrypted MCP secrets into in-memory plaintext form.
        config.hydrate_mcp_secrets_from_encrypted();
        // Decrypt encrypted env vars into in-memory plaintext form.
        config.hydrate_env_vars_from_encrypted();
        config.normalize_tool_settings();

        // Legacy: `data_dir` is no longer a persisted config field. The data directory is
        // derived from runtime (BAMBOO_DATA_DIR or `${HOME}/.bamboo`).
        config.extra.remove("data_dir");

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

        if let Ok(headless) = std::env::var("BAMBOO_HEADLESS") {
            config.headless_auth = parse_bool_env(&headless);
        }

        // Publish env vars to the global cache so Bash tools can inject them.
        config.publish_env_vars();

        config
    }

    /// Get the effective default model for the currently active provider.
    ///
    /// Note: for most providers this is a required config value (returns None when absent).
    /// Copilot has a built-in fallback when no model is configured.
    pub fn get_model(&self) -> Option<String> {
        match self.provider.as_str() {
            "openai" => self.providers.openai.as_ref().and_then(|c| c.model.clone()),
            "anthropic" => self
                .providers
                .anthropic
                .as_ref()
                .and_then(|c| c.model.clone()),
            "gemini" => self.providers.gemini.as_ref().and_then(|c| c.model.clone()),
            "copilot" => Some(
                self.providers
                    .copilot
                    .as_ref()
                    .and_then(|c| c.model.clone())
                    .unwrap_or_else(|| "gpt-4o".to_string()),
            ),
            _ => None,
        }
    }

    /// Get the fast/cheap model for the currently active provider.
    ///
    /// Used for lightweight tasks like title generation and summarization.
    /// Falls back to `get_model()` when no fast_model is configured.
    pub fn get_fast_model(&self) -> Option<String> {
        let fast = match self.provider.as_str() {
            "openai" => self
                .providers
                .openai
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            "anthropic" => self
                .providers
                .anthropic
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            "gemini" => self
                .providers
                .gemini
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            "copilot" => self
                .providers
                .copilot
                .as_ref()
                .and_then(|c| c.fast_model.clone()),
            _ => None,
        };
        fast.or_else(|| self.get_model())
    }

    /// Get the vision-capable model for the currently active provider.
    ///
    /// Used for image understanding tasks.
    /// Falls back to `get_model()` when no vision_model is configured.
    pub fn get_vision_model(&self) -> Option<String> {
        let vision = match self.provider.as_str() {
            "openai" => self
                .providers
                .openai
                .as_ref()
                .and_then(|c| c.vision_model.clone()),
            "anthropic" => self
                .providers
                .anthropic
                .as_ref()
                .and_then(|c| c.vision_model.clone()),
            "gemini" => self
                .providers
                .gemini
                .as_ref()
                .and_then(|c| c.vision_model.clone()),
            "copilot" => self
                .providers
                .copilot
                .as_ref()
                .and_then(|c| c.vision_model.clone()),
            _ => None,
        };
        vision.or_else(|| self.get_model())
    }

    /// Get the default reasoning effort for the currently active provider.
    pub fn get_reasoning_effort(&self) -> Option<ReasoningEffort> {
        match self.provider.as_str() {
            "openai" => self
                .providers
                .openai
                .as_ref()
                .and_then(|c| c.reasoning_effort),
            "anthropic" => self
                .providers
                .anthropic
                .as_ref()
                .and_then(|c| c.reasoning_effort),
            "gemini" => self
                .providers
                .gemini
                .as_ref()
                .and_then(|c| c.reasoning_effort),
            "copilot" => self
                .providers
                .copilot
                .as_ref()
                .and_then(|c| c.reasoning_effort),
            _ => None,
        }
    }

    /// Get normalized disabled tool names.
    pub fn disabled_tool_names(&self) -> BTreeSet<String> {
        self.tools
            .disabled
            .iter()
            .map(|name| name.trim())
            .filter(|name| !name.is_empty())
            .map(|name| normalize_tool_ref(name).unwrap_or_else(|| name.to_string()))
            .collect()
    }

    /// Normalize tool settings (trim / dedupe / sort).
    pub fn normalize_tool_settings(&mut self) {
        self.tools.disabled = self.disabled_tool_names().into_iter().collect();
    }

    /// Populate `proxy_auth` (plaintext) from `proxy_auth_encrypted` if present.
    ///
    /// Many parts of the code rely on `proxy_auth` being hydrated in-memory so
    /// we can re-encrypt deterministically on save without ever persisting
    /// plaintext credentials.
    pub fn hydrate_proxy_auth_from_encrypted(&mut self) {
        if self.proxy_auth.is_some() {
            return;
        }

        // Backward compatibility:
        // Older Bodhi/Tauri builds persisted proxy auth as per-scheme encrypted fields:
        // `http_proxy_auth_encrypted` / `https_proxy_auth_encrypted`.
        //
        // Those live under `extra` (flatten) in the unified config. Seed the new
        // `proxy_auth_encrypted` field so the rest of the code can stay uniform.
        if self
            .proxy_auth_encrypted
            .as_deref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true)
        {
            let legacy = self
                .extra
                .get("https_proxy_auth_encrypted")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    self.extra
                        .get("http_proxy_auth_encrypted")
                        .and_then(|v| v.as_str())
                })
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());

            if let Some(legacy) = legacy {
                self.proxy_auth_encrypted = Some(legacy);
            }
        }

        let Some(encrypted) = self.proxy_auth_encrypted.as_deref() else {
            return;
        };

        match crate::core::encryption::decrypt(encrypted) {
            Ok(decrypted) => match serde_json::from_str::<ProxyAuth>(&decrypted) {
                Ok(auth) => {
                    self.proxy_auth = Some(auth);
                    // Once hydrated successfully, drop legacy keys so a future save writes only
                    // the canonical `proxy_auth_encrypted` field.
                    self.extra.remove("http_proxy_auth_encrypted");
                    self.extra.remove("https_proxy_auth_encrypted");
                }
                Err(e) => tracing::warn!("Failed to parse decrypted proxy auth JSON: {}", e),
            },
            Err(e) => tracing::warn!("Failed to decrypt proxy auth: {}", e),
        }
    }

    /// Refresh `proxy_auth_encrypted` from the current in-memory `proxy_auth`.
    ///
    /// This is used both when persisting the config to disk and when generating
    /// API responses that should never include plaintext proxy credentials.
    pub fn refresh_proxy_auth_encrypted(&mut self) -> Result<()> {
        // Keep on-disk representation fully derived from the in-memory plaintext:
        // - Some(auth)  => always (re-)encrypt and store `proxy_auth_encrypted`
        // - None        => remove `proxy_auth_encrypted`
        let Some(auth) = self.proxy_auth.as_ref() else {
            self.proxy_auth_encrypted = None;
            return Ok(());
        };

        let auth_str = serde_json::to_string(auth).context("Failed to serialize proxy auth")?;
        let encrypted =
            crate::core::encryption::encrypt(&auth_str).context("Failed to encrypt proxy auth")?;
        self.proxy_auth_encrypted = Some(encrypted);
        Ok(())
    }

    pub fn hydrate_provider_api_keys_from_encrypted(&mut self) {
        if let Some(openai) = self.providers.openai.as_mut() {
            if openai.api_key.trim().is_empty() {
                if let Some(encrypted) = openai.api_key_encrypted.as_deref() {
                    match crate::core::encryption::decrypt(encrypted) {
                        Ok(value) => openai.api_key = value,
                        Err(e) => tracing::warn!("Failed to decrypt OpenAI api_key: {}", e),
                    }
                }
            }
        }

        if let Some(anthropic) = self.providers.anthropic.as_mut() {
            if anthropic.api_key.trim().is_empty() {
                if let Some(encrypted) = anthropic.api_key_encrypted.as_deref() {
                    match crate::core::encryption::decrypt(encrypted) {
                        Ok(value) => anthropic.api_key = value,
                        Err(e) => tracing::warn!("Failed to decrypt Anthropic api_key: {}", e),
                    }
                }
            }
        }

        if let Some(gemini) = self.providers.gemini.as_mut() {
            if gemini.api_key.trim().is_empty() {
                if let Some(encrypted) = gemini.api_key_encrypted.as_deref() {
                    match crate::core::encryption::decrypt(encrypted) {
                        Ok(value) => gemini.api_key = value,
                        Err(e) => tracing::warn!("Failed to decrypt Gemini api_key: {}", e),
                    }
                }
            }
        }
    }

    pub fn refresh_provider_api_keys_encrypted(&mut self) -> Result<()> {
        if let Some(openai) = self.providers.openai.as_mut() {
            let api_key = openai.api_key.trim();
            openai.api_key_encrypted = if api_key.is_empty() {
                None
            } else {
                Some(
                    crate::core::encryption::encrypt(api_key)
                        .context("Failed to encrypt OpenAI api_key")?,
                )
            };
        }

        if let Some(anthropic) = self.providers.anthropic.as_mut() {
            let api_key = anthropic.api_key.trim();
            anthropic.api_key_encrypted = if api_key.is_empty() {
                None
            } else {
                Some(
                    crate::core::encryption::encrypt(api_key)
                        .context("Failed to encrypt Anthropic api_key")?,
                )
            };
        }

        if let Some(gemini) = self.providers.gemini.as_mut() {
            let api_key = gemini.api_key.trim();
            gemini.api_key_encrypted = if api_key.is_empty() {
                None
            } else {
                Some(
                    crate::core::encryption::encrypt(api_key)
                        .context("Failed to encrypt Gemini api_key")?,
                )
            };
        }

        Ok(())
    }

    pub fn hydrate_mcp_secrets_from_encrypted(&mut self) {
        for server in self.mcp.servers.iter_mut() {
            match &mut server.transport {
                crate::agent::mcp::TransportConfig::Stdio(stdio) => {
                    if stdio.env_encrypted.is_empty() {
                        continue;
                    }

                    // Avoid borrow-checker gymnastics by iterating a cloned map.
                    for (key, encrypted) in stdio.env_encrypted.clone() {
                        let should_hydrate = stdio
                            .env
                            .get(&key)
                            .map(|v| v.trim().is_empty())
                            .unwrap_or(true);
                        if !should_hydrate {
                            continue;
                        }

                        match crate::core::encryption::decrypt(&encrypted) {
                            Ok(value) => {
                                stdio.env.insert(key, value);
                            }
                            Err(e) => tracing::warn!("Failed to decrypt MCP stdio env var: {}", e),
                        }
                    }
                }
                crate::agent::mcp::TransportConfig::Sse(sse) => {
                    for header in sse.headers.iter_mut() {
                        if !header.value.trim().is_empty() {
                            continue;
                        }
                        let Some(encrypted) = header.value_encrypted.as_deref() else {
                            continue;
                        };
                        match crate::core::encryption::decrypt(encrypted) {
                            Ok(value) => header.value = value,
                            Err(e) => {
                                tracing::warn!("Failed to decrypt MCP SSE header value: {}", e)
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn refresh_mcp_secrets_encrypted(&mut self) -> Result<()> {
        for server in self.mcp.servers.iter_mut() {
            match &mut server.transport {
                crate::agent::mcp::TransportConfig::Stdio(stdio) => {
                    stdio.env_encrypted.clear();
                    for (key, value) in &stdio.env {
                        let encrypted =
                            crate::core::encryption::encrypt(value).with_context(|| {
                                format!("Failed to encrypt MCP stdio env var '{key}'")
                            })?;
                        stdio.env_encrypted.insert(key.clone(), encrypted);
                    }
                }
                crate::agent::mcp::TransportConfig::Sse(sse) => {
                    for header in sse.headers.iter_mut() {
                        let configured = !header.value.trim().is_empty();
                        header.value_encrypted = if !configured {
                            None
                        } else {
                            Some(
                                crate::core::encryption::encrypt(&header.value).with_context(
                                    || {
                                        format!(
                                            "Failed to encrypt MCP SSE header '{}'",
                                            header.name
                                        )
                                    },
                                )?,
                            )
                        };
                    }
                }
            }
        }

        Ok(())
    }

    // ── Env vars encryption ─────────────────────────────────────────────

    /// Decrypt secret env vars into in-memory plaintext after loading config.
    pub fn hydrate_env_vars_from_encrypted(&mut self) {
        for entry in &mut self.env_vars {
            if !entry.secret {
                continue;
            }
            if !entry.value.trim().is_empty() {
                // Already has plaintext (e.g. in-memory update).
                continue;
            }
            let Some(encrypted) = &entry.value_encrypted else {
                continue;
            };
            match crate::core::encryption::decrypt(encrypted) {
                Ok(value) => entry.value = value,
                Err(e) => tracing::warn!("Failed to decrypt env var '{}': {}", entry.name, e),
            }
        }
    }

    /// Re-encrypt secret env vars before persisting to disk.
    pub fn refresh_env_vars_encrypted(&mut self) -> Result<()> {
        for entry in &mut self.env_vars {
            if entry.secret && !entry.value.trim().is_empty() {
                entry.value_encrypted = Some(
                    crate::core::encryption::encrypt(&entry.value)
                        .with_context(|| format!("Failed to encrypt env var '{}'", entry.name))?,
                );
            } else if !entry.secret {
                entry.value_encrypted = None;
            }
        }
        Ok(())
    }

    /// Clear plaintext values for secrets before serialization to disk.
    pub fn sanitize_env_vars_for_disk(&mut self) {
        for entry in &mut self.env_vars {
            if entry.secret {
                entry.value = String::new();
            }
        }
    }

    /// Build a flat map of all env vars with non-empty values (for process injection).
    pub fn env_vars_as_map(&self) -> HashMap<String, String> {
        self.env_vars
            .iter()
            .filter(|e| !e.value.trim().is_empty())
            .map(|e| (e.name.clone(), e.value.clone()))
            .collect()
    }

    /// Update the global env vars cache (called on config load / reload).
    pub fn publish_env_vars(&self) {
        let map = self.env_vars_as_map();
        if let Ok(mut guard) = ENV_VARS_CACHE.write() {
            *guard = map;
        }
    }

    /// Read the current env vars snapshot (called by Bash tool at process spawn time).
    pub fn current_env_vars() -> HashMap<String, String> {
        ENV_VARS_CACHE
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Create a default configuration without loading from file
    fn create_default() -> Self {
        Config {
            http_proxy: String::new(),
            https_proxy: String::new(),
            proxy_auth: None,
            proxy_auth_encrypted: None,
            headless_auth: false,
            provider: default_provider(),
            providers: ProviderConfigs::default(),
            server: ServerConfig::default(),
            keyword_masking: KeywordMaskingConfig::default(),
            anthropic_model_mapping: AnthropicModelMapping::default(),
            gemini_model_mapping: GeminiModelMapping::default(),
            hooks: HooksConfig::default(),
            tools: ToolsConfig::default(),
            env_vars: Vec::new(),
            mcp: crate::agent::mcp::McpConfig::default(),
            extra: BTreeMap::new(),
        }
    }

    /// Get the full server address (bind:port)
    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.server.bind, self.server.port)
    }

    /// Save configuration to disk
    pub fn save(&self) -> Result<()> {
        self.save_to_dir(default_data_dir())
    }

    /// Save configuration to disk under the provided data directory.
    ///
    /// Configuration is always stored as `{data_dir}/config.json`.
    pub fn save_to_dir(&self, data_dir: PathBuf) -> Result<()> {
        let path = data_dir.join("config.json");

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create config dir: {:?}", parent))?;
        }

        let mut to_save = self.clone();
        // Never persist `data_dir` into config.json (data dir is runtime-derived).
        to_save.extra.remove("data_dir");
        // Root-level `model` is deprecated; do not persist it.
        to_save.extra.remove("model");
        to_save.refresh_proxy_auth_encrypted()?;
        to_save.refresh_provider_api_keys_encrypted()?;
        to_save.refresh_env_vars_encrypted()?;
        to_save.sanitize_env_vars_for_disk();
        to_save.normalize_tool_settings();
        let content =
            serde_json::to_string_pretty(&to_save).context("Failed to serialize config to JSON")?;
        write_atomic(&path, content.as_bytes())
            .with_context(|| format!("Failed to write config file: {:?}", path))?;

        Ok(())
    }
}

fn write_atomic(path: &std::path::Path, content: &[u8]) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return std::fs::write(path, content);
    };

    std::fs::create_dir_all(parent)?;

    // Write to a temp file in the same directory then rename to ensure atomic replace.
    // (Rename is atomic on Unix when source/dest are on the same filesystem.)
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("config.json");
    let tmp_name = format!(".{}.tmp.{}", file_name, std::process::id());
    let tmp_path = parent.join(tmp_name);

    {
        let mut file = std::fs::File::create(&tmp_path)?;
        file.write_all(content)?;
        file.sync_all()?;
    }

    std::fs::rename(&tmp_path, path)?;
    Ok(())
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
            // Treat `path` as the Bamboo data dir and write `config.json` into it.
            // Tests should prefer BAMBOO_DATA_DIR over HOME to avoid global env contention.
            std::fs::create_dir_all(&self.path).expect("failed to create config dir");
            std::fs::write(self.path.join("config.json"), content)
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

    /// Acquire the environment lock, recovering from poison if a previous test failed
    fn env_lock_acquire() -> std::sync::MutexGuard<'static, ()> {
        env_lock().lock().unwrap_or_else(|poisoned| {
            // Lock was poisoned by a previous test failure - recover it
            poisoned.into_inner()
        })
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
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "http_proxy": "",
  "https_proxy": ""
}"#,
        );

        let _http_proxy = EnvVarGuard::set("HTTP_PROXY", "http://env-proxy.example.com:8080");
        let _https_proxy = EnvVarGuard::set("HTTPS_PROXY", "http://env-proxy.example.com:8443");

        let config = Config::from_data_dir(Some(temp_home.path.clone()));

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
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "provider": "openai",
  "providers": {
    "openai": {
      "api_key": "sk-test",
      "model": "gpt-4o"
    }
  }
}"#,
        );

        let _http_proxy = EnvVarGuard::unset("HTTP_PROXY");
        let _https_proxy = EnvVarGuard::unset("HTTPS_PROXY");

        let config = Config::from_data_dir(Some(temp_home.path.clone()));

        assert_eq!(
            config
                .providers
                .openai
                .as_ref()
                .and_then(|c| c.model.as_deref()),
            Some("gpt-4o"),
            "config should load provider model from config file even when proxy fields are omitted"
        );
        assert!(config.http_proxy.is_empty());
        assert!(config.https_proxy.is_empty());
    }

    #[test]
    fn config_new_ignores_proxy_env_vars_when_proxy_fields_omitted() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "provider": "openai",
  "providers": {
    "openai": {
      "api_key": "sk-test",
      "model": "gpt-4o"
    }
  }
}"#,
        );

        let _http_proxy = EnvVarGuard::set("HTTP_PROXY", "http://env-proxy.example.com:8080");
        let _https_proxy = EnvVarGuard::set("HTTPS_PROXY", "http://env-proxy.example.com:8443");

        let config = Config::from_data_dir(Some(temp_home.path.clone()));

        assert_eq!(
            config
                .providers
                .openai
                .as_ref()
                .and_then(|c| c.model.as_deref()),
            Some("gpt-4o")
        );
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
    fn normalize_tool_settings_trims_dedupes_canonicalizes_and_sorts() {
        let mut config = Config::default();
        config.tools.disabled = vec![
            "  read_file  ".to_string(),
            "".to_string(),
            "read_file".to_string(),
            "bash".to_string(),
            "default::getCurrentDir".to_string(),
        ];

        config.normalize_tool_settings();

        assert_eq!(config.tools.disabled, vec!["Bash", "GetCurrentDir", "Read"]);
    }

    #[test]
    fn config_load_reads_disabled_tools_as_canonical_names() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "tools": {
    "disabled": ["bash", " read_file ", "bash", "default::getCurrentDir"]
  }
}"#,
        );

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        assert_eq!(config.tools.disabled, vec!["Bash", "GetCurrentDir", "Read"]);
        assert!(config.disabled_tool_names().contains("Bash"));
        assert!(config.disabled_tool_names().contains("Read"));
        assert!(config.disabled_tool_names().contains("GetCurrentDir"));
    }

    #[test]
    fn test_server_config_defaults() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        assert_eq!(config.server.port, 9562);
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
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        let _port = EnvVarGuard::set("BAMBOO_PORT", "9999");
        let _bind = EnvVarGuard::set("BAMBOO_BIND", "192.168.1.1");
        let _provider = EnvVarGuard::set("BAMBOO_PROVIDER", "openai");

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        assert_eq!(config.server.port, 9999);
        assert_eq!(config.server.bind, "192.168.1.1");
        assert_eq!(config.provider, "openai");
    }

    #[test]
    fn test_config_save_and_load() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        let mut config = Config::from_data_dir(Some(temp_home.path.clone()));
        config.server.port = 9000;
        config.server.bind = "0.0.0.0".to_string();
        config.provider = "anthropic".to_string();

        // Save
        config
            .save_to_dir(temp_home.path.clone())
            .expect("Failed to save config");

        // Load again
        let loaded = Config::from_data_dir(Some(temp_home.path.clone()));

        // Verify
        assert_eq!(loaded.server.port, 9000);
        assert_eq!(loaded.server.bind, "0.0.0.0");
        assert_eq!(loaded.provider, "anthropic");
    }

    #[test]
    fn config_decrypts_proxy_auth_from_encrypted_field() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        // Use a stable encryption key so this test doesn't depend on host identifiers.
        let key_guard = crate::core::encryption::set_test_encryption_key([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]);

        let auth = ProxyAuth {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        let auth_str = serde_json::to_string(&auth).expect("serialize proxy auth");
        let encrypted = crate::core::encryption::encrypt(&auth_str).expect("encrypt proxy auth");

        temp_home.set_config_json(&format!(
            r#"{{
  "http_proxy": "http://proxy.example.com:8080",
  "proxy_auth_encrypted": "{encrypted}"
}}"#
        ));
        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        let loaded_auth = config.proxy_auth.expect("proxy auth should be hydrated");
        assert_eq!(loaded_auth.username, "user");
        assert_eq!(loaded_auth.password, "pass");
        drop(key_guard);
    }

    #[test]
    fn config_decrypts_proxy_auth_from_legacy_scheme_encrypted_fields() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        // Use a stable encryption key so this test doesn't depend on host identifiers.
        let key_guard = crate::core::encryption::set_test_encryption_key([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]);

        let auth = ProxyAuth {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        let auth_str = serde_json::to_string(&auth).expect("serialize proxy auth");
        let encrypted = crate::core::encryption::encrypt(&auth_str).expect("encrypt proxy auth");

        // Simulate older Bodhi/Tauri persisted config keys.
        temp_home.set_config_json(&format!(
            r#"{{
  "http_proxy": "http://proxy.example.com:8080",
  "http_proxy_auth_encrypted": "{encrypted}",
  "https_proxy_auth_encrypted": "{encrypted}"
}}"#
        ));

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        let loaded_auth = config.proxy_auth.expect("proxy auth should be hydrated");
        assert_eq!(loaded_auth.username, "user");
        assert_eq!(loaded_auth.password, "pass");
        drop(key_guard);
    }

    #[test]
    fn config_save_encrypts_proxy_auth_and_load_hydrates_plaintext() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        // Use a stable encryption key so this test doesn't depend on host identifiers.
        let key_guard = crate::core::encryption::set_test_encryption_key([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]);

        let mut config = Config::from_data_dir(Some(temp_home.path.clone()));
        config.proxy_auth = Some(ProxyAuth {
            username: "user".to_string(),
            password: "pass".to_string(),
        });
        config
            .save_to_dir(temp_home.path.clone())
            .expect("save should encrypt proxy auth");

        let content =
            std::fs::read_to_string(temp_home.path.join("config.json")).expect("read config.json");
        assert!(
            content.contains("proxy_auth_encrypted"),
            "config.json should store encrypted proxy auth"
        );
        assert!(
            !content.contains("\"proxy_auth\""),
            "config.json should not store plaintext proxy_auth"
        );

        let loaded = Config::from_data_dir(Some(temp_home.path.clone()));
        let loaded_auth = loaded.proxy_auth.expect("proxy auth should be hydrated");
        assert_eq!(loaded_auth.username, "user");
        assert_eq!(loaded_auth.password, "pass");
        drop(key_guard);
    }

    #[test]
    fn config_save_encrypts_provider_api_keys_and_does_not_persist_plaintext() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        // Use a stable encryption key so this test doesn't depend on host identifiers.
        let key_guard = crate::core::encryption::set_test_encryption_key([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]);

        let mut config = Config::from_data_dir(Some(temp_home.path.clone()));
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "sk-test-provider-key".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: None,
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
        });

        config
            .save_to_dir(temp_home.path.clone())
            .expect("save should encrypt provider api keys");

        let content =
            std::fs::read_to_string(temp_home.path.join("config.json")).expect("read config.json");
        assert!(
            content.contains("\"api_key_encrypted\""),
            "config.json should store encrypted provider keys"
        );
        assert!(
            !content.contains("\"api_key\""),
            "config.json should not store plaintext provider keys"
        );

        let loaded = Config::from_data_dir(Some(temp_home.path.clone()));
        let openai = loaded
            .providers
            .openai
            .expect("openai config should be present");
        assert_eq!(openai.api_key, "sk-test-provider-key");

        drop(key_guard);
    }

    #[test]
    fn config_save_persists_mcp_servers_in_mainstream_format() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();

        let mut config = Config::from_data_dir(Some(temp_home.path.clone()));

        let mut env = std::collections::HashMap::new();
        env.insert("TOKEN".to_string(), "supersecret".to_string());

        config.mcp.servers = vec![
            crate::agent::mcp::McpServerConfig {
                id: "stdio-secret".to_string(),
                name: None,
                enabled: true,
                transport: crate::agent::mcp::TransportConfig::Stdio(
                    crate::agent::mcp::StdioConfig {
                        command: "echo".to_string(),
                        args: vec![],
                        cwd: None,
                        env,
                        env_encrypted: std::collections::HashMap::new(),
                        startup_timeout_ms: 5000,
                    },
                ),
                request_timeout_ms: 5000,
                healthcheck_interval_ms: 1000,
                reconnect: crate::agent::mcp::ReconnectConfig::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            },
            crate::agent::mcp::McpServerConfig {
                id: "sse-secret".to_string(),
                name: None,
                enabled: true,
                transport: crate::agent::mcp::TransportConfig::Sse(crate::agent::mcp::SseConfig {
                    url: "http://localhost:8080/sse".to_string(),
                    headers: vec![crate::agent::mcp::HeaderConfig {
                        name: "Authorization".to_string(),
                        value: "Bearer token123".to_string(),
                        value_encrypted: None,
                    }],
                    connect_timeout_ms: 5000,
                }),
                request_timeout_ms: 5000,
                healthcheck_interval_ms: 1000,
                reconnect: crate::agent::mcp::ReconnectConfig::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            },
        ];

        config
            .save_to_dir(temp_home.path.clone())
            .expect("save should persist MCP servers");

        let content =
            std::fs::read_to_string(temp_home.path.join("config.json")).expect("read config.json");
        assert!(
            content.contains("\"mcpServers\""),
            "config.json should store MCP servers under the mainstream 'mcpServers' key"
        );
        assert!(
            content.contains("supersecret"),
            "config.json should persist MCP stdio env in mainstream format"
        );
        assert!(
            content.contains("Bearer token123"),
            "config.json should persist MCP SSE headers in mainstream format"
        );
        assert!(
            !content.contains("\"env_encrypted\""),
            "config.json should not persist legacy env_encrypted fields"
        );
        assert!(
            !content.contains("\"value_encrypted\""),
            "config.json should not persist legacy value_encrypted fields"
        );

        let loaded = Config::from_data_dir(Some(temp_home.path.clone()));
        let stdio = loaded
            .mcp
            .servers
            .iter()
            .find(|s| s.id == "stdio-secret")
            .expect("stdio server should exist");
        match &stdio.transport {
            crate::agent::mcp::TransportConfig::Stdio(stdio) => {
                assert_eq!(
                    stdio.env.get("TOKEN").map(|s| s.as_str()),
                    Some("supersecret")
                );
            }
            _ => panic!("Expected stdio transport"),
        }

        let sse = loaded
            .mcp
            .servers
            .iter()
            .find(|s| s.id == "sse-secret")
            .expect("sse server should exist");
        match &sse.transport {
            crate::agent::mcp::TransportConfig::Sse(sse) => {
                assert_eq!(sse.headers[0].value, "Bearer token123");
            }
            _ => panic!("Expected SSE transport"),
        }
    }

    // ── Env vars lifecycle tests ──────────────────────────────

    #[test]
    fn env_vars_as_map_includes_only_non_empty_values() {
        let mut config = Config::default();
        config.env_vars = vec![
            EnvVarEntry {
                name: "A".to_string(),
                value: "val_a".to_string(),
                secret: false,
                value_encrypted: None,
                description: None,
            },
            EnvVarEntry {
                name: "B".to_string(),
                value: "".to_string(), // empty → should be excluded
                secret: true,
                value_encrypted: None,
                description: None,
            },
            EnvVarEntry {
                name: "C".to_string(),
                value: "  ".to_string(), // whitespace-only → excluded
                secret: false,
                value_encrypted: None,
                description: None,
            },
            EnvVarEntry {
                name: "D".to_string(),
                value: "val_d".to_string(),
                secret: true,
                value_encrypted: Some("enc".to_string()),
                description: Some("desc".to_string()),
            },
        ];

        let map = config.env_vars_as_map();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("A"), Some(&"val_a".to_string()));
        assert_eq!(map.get("D"), Some(&"val_d".to_string()));
        assert!(!map.contains_key("B"));
        assert!(!map.contains_key("C"));
    }

    #[test]
    fn sanitize_env_vars_for_disk_clears_secret_plaintext() {
        let mut config = Config::default();
        config.env_vars = vec![
            EnvVarEntry {
                name: "PLAIN".to_string(),
                value: "visible".to_string(),
                secret: false,
                value_encrypted: None,
                description: None,
            },
            EnvVarEntry {
                name: "SECRET".to_string(),
                value: "hidden_value".to_string(),
                secret: true,
                value_encrypted: Some("enc_data".to_string()),
                description: None,
            },
        ];

        config.sanitize_env_vars_for_disk();

        assert_eq!(config.env_vars[0].value, "visible"); // plain kept
        assert_eq!(config.env_vars[1].value, ""); // secret cleared
    }

    #[test]
    fn sanitize_env_vars_for_disk_preserves_encrypted() {
        let mut config = Config::default();
        config.env_vars = vec![
            EnvVarEntry {
                name: "OPEN".to_string(),
                value: "val".to_string(),
                secret: false,
                value_encrypted: None,
                description: None,
            },
            EnvVarEntry {
                name: "HIDDEN".to_string(),
                value: "real_secret".to_string(),
                secret: true,
                value_encrypted: Some("enc".to_string()),
                description: None,
            },
        ];

        config.sanitize_env_vars_for_disk();

        // Plain value untouched
        assert_eq!(config.env_vars[0].value, "val");
        // Secret plaintext cleared, but encrypted preserved
        assert_eq!(config.env_vars[1].value, "");
        assert_eq!(config.env_vars[1].value_encrypted.as_deref(), Some("enc"));
    }

    #[test]
    fn refresh_env_vars_encrypted_round_trip() {
        let mut config = Config::default();
        config.env_vars = vec![
            EnvVarEntry {
                name: "TOKEN".to_string(),
                value: "my-secret-token".to_string(),
                secret: true,
                value_encrypted: None,
                description: Some("A token".to_string()),
            },
            EnvVarEntry {
                name: "PLAIN_VAR".to_string(),
                value: "hello".to_string(),
                secret: false,
                value_encrypted: None,
                description: None,
            },
        ];

        // Encrypt
        config
            .refresh_env_vars_encrypted()
            .expect("encryption should succeed");

        // Secret should now have encrypted value
        assert!(config.env_vars[0].value_encrypted.is_some());
        // Plain should have no encrypted value
        assert!(config.env_vars[1].value_encrypted.is_none());

        // Save encrypted value for later comparison
        let encrypted = config.env_vars[0].value_encrypted.clone().unwrap();
        assert_ne!(encrypted, "my-secret-token"); // shouldn't be plaintext

        // Clear plaintext (simulating disk write)
        config.sanitize_env_vars_for_disk();
        assert_eq!(config.env_vars[0].value, "");

        // Hydrate (simulating disk read)
        config.hydrate_env_vars_from_encrypted();
        assert_eq!(config.env_vars[0].value, "my-secret-token");
        assert_eq!(config.env_vars[1].value, "hello"); // plain untouched
    }

    #[test]
    fn publish_and_current_env_vars_round_trip() {
        let mut config = Config::default();
        config.env_vars = vec![EnvVarEntry {
            name: "TEST_PUBLISH".to_string(),
            value: "pub_value".to_string(),
            secret: false,
            value_encrypted: None,
            description: None,
        }];

        config.publish_env_vars();
        let map = Config::current_env_vars();
        assert_eq!(map.get("TEST_PUBLISH"), Some(&"pub_value".to_string()));
    }

    #[test]
    fn hydrate_skips_non_secret_entries() {
        let mut config = Config::default();
        config.env_vars = vec![EnvVarEntry {
            name: "PLAIN".to_string(),
            value: "original".to_string(),
            secret: false,
            value_encrypted: Some("should-be-ignored".to_string()),
            description: None,
        }];

        config.hydrate_env_vars_from_encrypted();
        // Non-secret entry should keep its original value
        assert_eq!(config.env_vars[0].value, "original");
    }

    #[test]
    fn default_config_has_empty_env_vars() {
        let config = Config::default();
        assert!(config.env_vars.is_empty());
    }

    #[test]
    fn serde_round_trip_with_env_vars() {
        let mut config = Config::default();
        config.env_vars = vec![
            EnvVarEntry {
                name: "KEY1".to_string(),
                value: "val1".to_string(),
                secret: false,
                value_encrypted: None,
                description: Some("First key".to_string()),
            },
            EnvVarEntry {
                name: "KEY2".to_string(),
                value: "".to_string(), // on-disk secret has no plaintext
                secret: true,
                value_encrypted: Some("enc123".to_string()),
                description: None,
            },
        ];

        let json = serde_json::to_string(&config).unwrap();
        let restored: Config = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.env_vars.len(), 2);
        assert_eq!(restored.env_vars[0].name, "KEY1");
        assert_eq!(restored.env_vars[0].value, "val1");
        assert!(!restored.env_vars[0].secret);
        assert_eq!(restored.env_vars[1].name, "KEY2");
        assert!(restored.env_vars[1].secret);
        assert_eq!(
            restored.env_vars[1].value_encrypted.as_deref(),
            Some("enc123")
        );
    }
}
