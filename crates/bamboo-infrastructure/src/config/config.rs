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

use crate::keyword_masking::KeywordMaskingConfig;
use crate::model_mapping::{AnthropicModelMapping, GeminiModelMapping};
use bamboo_domain::tool_names::normalize_tool_ref;
use bamboo_domain::ReasoningEffort;

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

/// Default work area configuration.
///
/// Allows Bamboo to operate without an explicit initial workspace while still
/// providing a stable fallback directory for relative-path tool execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DefaultWorkAreaConfig {
    /// Optional default filesystem path used when a session has no active workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// Access control configuration for password-based UI/API gating.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccessControlConfig {
    /// Whether password protection is enabled.
    #[serde(default)]
    pub password_enabled: bool,
    /// Password hash (hex-encoded). Never expose via API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_hash: Option<String>,
    /// Salt used for hashing (hex-encoded). Never expose via API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_salt: Option<String>,
    /// Last update timestamp for auditing / debugging.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// Memory and background summarization configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryConfig {
    /// Optional dedicated model for memory/session summarization and reflection.
    /// Falls back to the provider fast model when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_model: Option<String>,
    /// Whether lightweight automatic Dream-style consolidation should run in the background.
    #[serde(default)]
    pub auto_dream_enabled: bool,
    /// Whether project durable-memory index injection is enabled for the main prompt.
    #[serde(
        default = "default_true_memory_project_prompt_injection",
        alias = "memory_project_prompt_injection"
    )]
    pub project_prompt_injection: bool,
    /// Whether automatic relevant durable-memory recall is enabled for the main prompt.
    #[serde(
        default = "default_true_memory_relevant_recall",
        alias = "memory_relevant_recall"
    )]
    pub relevant_recall: bool,
    /// Whether relevant durable-memory recall should rerank lexical shortlist candidates
    /// using the configured memory/background model.
    #[serde(default, alias = "memory_relevant_recall_rerank")]
    pub relevant_recall_rerank: bool,
    /// Whether Dream prompt injection should prefer project Dream and only use global Dream as fallback.
    #[serde(
        default = "default_true_memory_project_first_dream",
        alias = "memory_project_first_dream"
    )]
    pub project_first_dream: bool,
    /// Whether Dream generation should refine from the existing notebook when present.
    ///
    /// This rolls out cumulative/refining Dream synthesis behind an explicit opt-in flag.
    #[serde(default, alias = "memory_dream_refine_mode")]
    pub dream_refine_mode: bool,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            background_model: None,
            auto_dream_enabled: false,
            project_prompt_injection: default_true_memory_project_prompt_injection(),
            relevant_recall: default_true_memory_relevant_recall(),
            relevant_recall_rerank: false,
            project_first_dream: default_true_memory_project_first_dream(),
            dream_refine_mode: false,
        }
    }
}

fn default_true_memory_project_prompt_injection() -> bool {
    true
}

fn default_true_memory_relevant_recall() -> bool {
    true
}

fn default_true_memory_project_first_dream() -> bool {
    true
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

    /// Default model assignments (used when features.provider_model_ref is enabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<DefaultsConfig>,

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

    /// Global skill toggles.
    ///
    /// Any skill listed in `disabled` is excluded from skill context construction and
    /// cannot be loaded through the skill runtime tools.
    #[serde(default, skip_serializing_if = "SkillsConfig::is_empty")]
    pub skills: SkillsConfig,

    /// User-managed environment variables injected into Bash tool processes.
    ///
    /// Secret entries are encrypted at rest; plaintext values are hydrated in memory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env_vars: Vec<EnvVarEntry>,

    /// Default work area used when a session has no explicit active workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_work_area: Option<DefaultWorkAreaConfig>,

    /// Access control / password gate configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_control: Option<AccessControlConfig>,

    /// Feature flags for incremental rollout.
    #[serde(default)]
    pub features: FeatureFlags,

    /// Memory/background summarization settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryConfig>,

    /// MCP server configuration.
    ///
    /// Previously persisted in `mcp.json` (now unified into `config.json`).
    // On disk we use the mainstream `mcpServers` key (matching Claude Desktop / MCP ecosystem
    // conventions). We still accept the legacy `mcp` key for backward compatibility.
    #[serde(default, rename = "mcpServers", alias = "mcp")]
    pub mcp: bamboo_domain::mcp_config::McpConfig,

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
    /// Bodhi proxy provider configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bodhi: Option<BodhiConfig>,

    /// Preserve unknown provider keys (forward compatibility).
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Feature flags for incremental rollout of new subsystems.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeatureFlags {
    /// Enable the ProviderModelRef system (multi-provider + unified model selection).
    #[serde(default)]
    pub provider_model_ref: bool,
    /// Enable MiniLoop-based complexity evaluation and dynamic per-round model switching.
    #[serde(default)]
    pub dynamic_model_routing: bool,
}

/// Default model assignments for specific capabilities.
///
/// Used when `features.provider_model_ref` is enabled.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DefaultsConfig {
    pub chat: bamboo_domain::ProviderModelRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast: Option<bamboo_domain::ProviderModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision: Option<bamboo_domain::ProviderModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_background: Option<bamboo_domain::ProviderModelRef>,
    /// Model for planning/coordination tasks (task decomposition, architecture).
    /// Falls back to `chat` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planning: Option<bamboo_domain::ProviderModelRef>,
    /// Model for search/navigation tasks (grep, file listing, symbol resolution).
    /// Falls back to `fast` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<bamboo_domain::ProviderModelRef>,
    /// Model for code review tasks.
    /// Falls back to `chat` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_review: Option<bamboo_domain::ProviderModelRef>,
    /// Default model for child SubSession runs.
    /// Falls back to `fast`, then `chat` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_session: Option<bamboo_domain::ProviderModelRef>,
    /// Per-subagent-type model overrides.
    /// Key = subagent_type (e.g. "researcher", "coder"), Value = ProviderModelRef.
    /// Falls back to `chat` when no match is found for a given type.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub subagent_models: HashMap<String, bamboo_domain::ProviderModelRef>,
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

/// Global skill toggle configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SkillsConfig {
    /// Skill IDs that are disabled globally.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled: Vec<String>,
}

impl SkillsConfig {
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

/// Bodhi proxy provider configuration.
///
/// Routes LLM requests through a bodhi-server instance so that raw provider
/// API keys never reach the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BodhiConfig {
    /// Bodhi server API key (e.g. "bhi_sk_xxx").  In-memory only.
    #[serde(default, skip_serializing)]
    pub api_key: String,
    /// Encrypted form of the API key stored on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_encrypted: Option<String>,
    /// Bodhi server base URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Which upstream provider to route through bodhi ("openai", "anthropic", "gemini").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_provider: Option<String>,
    /// Default reasoning effort.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,

    /// Preserve unknown keys.
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

fn expand_user_path(value: &str) -> PathBuf {
    let trimmed = value.trim();
    if let Some(rest) = trimmed.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(trimmed)
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

/// Prompt-safe snapshot of configured env vars.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptSafeEnvVarEntry {
    pub name: String,
    pub secret: bool,
    pub description: Option<String>,
}

/// Global cache of user-managed env vars for injection into child processes.
///
/// Updated whenever the config is loaded or reloaded via [`Config::publish_env_vars`].
static ENV_VARS_CACHE: std::sync::LazyLock<RwLock<HashMap<String, String>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

static PROMPT_SAFE_ENV_VARS_CACHE: std::sync::LazyLock<RwLock<Vec<PromptSafeEnvVarEntry>>> =
    std::sync::LazyLock::new(|| RwLock::new(Vec::new()));

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
    /// - `BAMBOO_MEMORY_PROJECT_PROMPT_INJECTION`: Override project durable-memory index prompt injection
    /// - `BAMBOO_MEMORY_RELEVANT_RECALL`: Override relevant durable-memory recall prompt injection
    /// - `BAMBOO_MEMORY_RELEVANT_RECALL_RERANK`: Override model-based relevant recall reranking
    /// - `BAMBOO_MEMORY_PROJECT_FIRST_DREAM`: Override project-first Dream prompt behavior
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
                        config.normalize_skill_settings();
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
        config.normalize_skill_settings();

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

        if let Ok(project_prompt_injection) =
            std::env::var("BAMBOO_MEMORY_PROJECT_PROMPT_INJECTION")
        {
            let memory = config.memory.get_or_insert_with(MemoryConfig::default);
            memory.project_prompt_injection = parse_bool_env(&project_prompt_injection);
        }

        if let Ok(relevant_recall) = std::env::var("BAMBOO_MEMORY_RELEVANT_RECALL") {
            let memory = config.memory.get_or_insert_with(MemoryConfig::default);
            memory.relevant_recall = parse_bool_env(&relevant_recall);
        }

        if let Ok(relevant_recall_rerank) = std::env::var("BAMBOO_MEMORY_RELEVANT_RECALL_RERANK") {
            let memory = config.memory.get_or_insert_with(MemoryConfig::default);
            memory.relevant_recall_rerank = parse_bool_env(&relevant_recall_rerank);
        }

        if let Ok(project_first_dream) = std::env::var("BAMBOO_MEMORY_PROJECT_FIRST_DREAM") {
            let memory = config.memory.get_or_insert_with(MemoryConfig::default);
            memory.project_first_dream = parse_bool_env(&project_first_dream);
        }

        // Publish env vars to the global cache so Bash tools can inject them.
        config.publish_env_vars();

        config
    }

    /// Get the effective default model for the currently active provider.
    ///
    /// When `features.provider_model_ref` is enabled, reads from `defaults.chat`
    /// before falling back to legacy provider-specific config.
    ///
    /// Note: for most providers this is a required config value (returns None when absent).
    /// Copilot has a built-in fallback when no model is configured.
    pub fn get_model(&self) -> Option<String> {
        if self.features.provider_model_ref {
            if let Some(model_ref) = self.defaults.as_ref().map(|d| &d.chat) {
                return Some(model_ref.model.clone());
            }
        }
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
    /// When `features.provider_model_ref` is enabled, reads from `defaults.fast`
    /// before falling back to legacy provider-specific config.
    ///
    /// Used for lightweight tasks like title generation and summarization.
    /// Falls back to `get_model()` when no fast_model is configured.
    pub fn get_fast_model(&self) -> Option<String> {
        if self.features.provider_model_ref {
            if let Some(model_ref) = self.defaults.as_ref().and_then(|d| d.fast.as_ref()) {
                return Some(model_ref.model.clone());
            }
        }
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

    /// Get the configured memory/background summarization model.
    ///
    /// When `features.provider_model_ref` is enabled, reads from
    /// `defaults.memory_background` before falling back to legacy config.
    ///
    /// Falls back to the provider fast model when no background model is
    /// configured or resolves to an empty string.
    ///
    /// IMPORTANT: this intentionally does **not** fall back to the main
    /// interaction model. Memory compaction / reflection should be skipped or
    /// fail loudly when no background/fast model is configured.
    pub fn get_memory_background_model(&self) -> Option<String> {
        if self.features.provider_model_ref {
            if let Some(model_ref) = self
                .defaults
                .as_ref()
                .and_then(|d| d.memory_background.as_ref())
            {
                return Some(model_ref.model.clone());
            }
            if let Some(model_ref) = self.defaults.as_ref().and_then(|d| d.fast.as_ref()) {
                return Some(model_ref.model.clone());
            }
        }
        let configured = self
            .memory
            .as_ref()
            .and_then(|memory| memory.background_model.as_ref())
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        configured.or_else(|| match self.provider.as_str() {
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
        })
    }

    /// Resolve the configured default work area path when present.
    ///
    /// This validates that the configured directory exists, but intentionally
    /// returns the stable expanded path rather than the platform-specific
    /// canonicalized path. On macOS, `canonicalize()` may rewrite `/var/...`
    /// to `/private/var/...`, which is correct at the filesystem layer but
    /// undesirable as a user-facing/config-derived workspace path.
    pub fn get_default_work_area_path(&self) -> Option<PathBuf> {
        let raw = self
            .default_work_area
            .as_ref()
            .and_then(|config| config.path.as_ref())
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())?;

        let candidate = expand_user_path(raw);
        if candidate.is_absolute() {
            let canonical = std::fs::canonicalize(&candidate).ok();
            return canonical
                .as_ref()
                .filter(|path| path.is_dir())
                .map(|_| candidate.clone())
                .or_else(|| candidate.is_dir().then_some(candidate));
        }

        let from_bamboo_dir = crate::paths::bamboo_dir().join(&candidate);
        let canonical = std::fs::canonicalize(&from_bamboo_dir).ok();
        canonical
            .as_ref()
            .filter(|path| path.is_dir())
            .map(|_| from_bamboo_dir.clone())
            .or_else(|| from_bamboo_dir.is_dir().then_some(from_bamboo_dir))
            .or_else(|| candidate.is_dir().then_some(candidate))
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

    /// Get normalized disabled skill IDs.
    pub fn disabled_skill_ids(&self) -> BTreeSet<String> {
        self.skills
            .disabled
            .iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .map(|id| id.to_string())
            .collect()
    }

    /// Normalize skill settings (trim / dedupe / sort).
    pub fn normalize_skill_settings(&mut self) {
        self.skills.disabled = self.disabled_skill_ids().into_iter().collect();
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

        match crate::encryption::decrypt(encrypted) {
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
            crate::encryption::encrypt(&auth_str).context("Failed to encrypt proxy auth")?;
        self.proxy_auth_encrypted = Some(encrypted);
        Ok(())
    }

    pub fn hydrate_provider_api_keys_from_encrypted(&mut self) {
        if let Some(openai) = self.providers.openai.as_mut() {
            if openai.api_key.trim().is_empty() {
                if let Some(encrypted) = openai.api_key_encrypted.as_deref() {
                    match crate::encryption::decrypt(encrypted) {
                        Ok(value) => openai.api_key = value,
                        Err(e) => tracing::warn!("Failed to decrypt OpenAI api_key: {}", e),
                    }
                }
            }
        }

        if let Some(anthropic) = self.providers.anthropic.as_mut() {
            if anthropic.api_key.trim().is_empty() {
                if let Some(encrypted) = anthropic.api_key_encrypted.as_deref() {
                    match crate::encryption::decrypt(encrypted) {
                        Ok(value) => anthropic.api_key = value,
                        Err(e) => tracing::warn!("Failed to decrypt Anthropic api_key: {}", e),
                    }
                }
            }
        }

        if let Some(gemini) = self.providers.gemini.as_mut() {
            if gemini.api_key.trim().is_empty() {
                if let Some(encrypted) = gemini.api_key_encrypted.as_deref() {
                    match crate::encryption::decrypt(encrypted) {
                        Ok(value) => gemini.api_key = value,
                        Err(e) => tracing::warn!("Failed to decrypt Gemini api_key: {}", e),
                    }
                }
            }
        }

        if let Some(bodhi) = self.providers.bodhi.as_mut() {
            if bodhi.api_key.trim().is_empty() {
                if let Some(encrypted) = bodhi.api_key_encrypted.as_deref() {
                    match crate::encryption::decrypt(encrypted) {
                        Ok(value) => bodhi.api_key = value,
                        Err(e) => tracing::warn!("Failed to decrypt Bodhi api_key: {}", e),
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
                    crate::encryption::encrypt(api_key)
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
                    crate::encryption::encrypt(api_key)
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
                    crate::encryption::encrypt(api_key)
                        .context("Failed to encrypt Gemini api_key")?,
                )
            };
        }

        if let Some(bodhi) = self.providers.bodhi.as_mut() {
            let api_key = bodhi.api_key.trim();
            bodhi.api_key_encrypted = if api_key.is_empty() {
                None
            } else {
                Some(
                    crate::encryption::encrypt(api_key)
                        .context("Failed to encrypt Bodhi api_key")?,
                )
            };
        }

        Ok(())
    }

    pub fn hydrate_mcp_secrets_from_encrypted(&mut self) {
        for server in self.mcp.servers.iter_mut() {
            match &mut server.transport {
                bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => {
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

                        match crate::encryption::decrypt(&encrypted) {
                            Ok(value) => {
                                stdio.env.insert(key, value);
                            }
                            Err(e) => tracing::warn!("Failed to decrypt MCP stdio env var: {}", e),
                        }
                    }
                }
                bamboo_domain::mcp_config::TransportConfig::Sse(sse) => {
                    for header in sse.headers.iter_mut() {
                        if !header.value.trim().is_empty() {
                            continue;
                        }
                        let Some(encrypted) = header.value_encrypted.as_deref() else {
                            continue;
                        };
                        match crate::encryption::decrypt(encrypted) {
                            Ok(value) => header.value = value,
                            Err(e) => {
                                tracing::warn!("Failed to decrypt MCP SSE header value: {}", e)
                            }
                        }
                    }
                }
                bamboo_domain::mcp_config::TransportConfig::StreamableHttp(sh) => {
                    for header in sh.headers.iter_mut() {
                        if !header.value.trim().is_empty() {
                            continue;
                        }
                        let Some(encrypted) = header.value_encrypted.as_deref() else {
                            continue;
                        };
                        match crate::encryption::decrypt(encrypted) {
                            Ok(value) => header.value = value,
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to decrypt MCP StreamableHTTP header value: {}",
                                    e
                                )
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
                bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => {
                    stdio.env_encrypted.clear();
                    for (key, value) in &stdio.env {
                        let encrypted = crate::encryption::encrypt(value).with_context(|| {
                            format!("Failed to encrypt MCP stdio env var '{key}'")
                        })?;
                        stdio.env_encrypted.insert(key.clone(), encrypted);
                    }
                }
                bamboo_domain::mcp_config::TransportConfig::Sse(sse) => {
                    for header in sse.headers.iter_mut() {
                        let configured = !header.value.trim().is_empty();
                        header.value_encrypted = if !configured {
                            None
                        } else {
                            Some(crate::encryption::encrypt(&header.value).with_context(|| {
                                format!("Failed to encrypt MCP SSE header '{}'", header.name)
                            })?)
                        };
                    }
                }
                bamboo_domain::mcp_config::TransportConfig::StreamableHttp(sh) => {
                    for header in sh.headers.iter_mut() {
                        let configured = !header.value.trim().is_empty();
                        header.value_encrypted = if !configured {
                            None
                        } else {
                            Some(crate::encryption::encrypt(&header.value).with_context(|| {
                                format!(
                                    "Failed to encrypt MCP StreamableHTTP header '{}'",
                                    header.name
                                )
                            })?)
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
            match crate::encryption::decrypt(encrypted) {
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
                    crate::encryption::encrypt(&entry.value)
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

    fn prompt_safe_env_vars(&self) -> Vec<PromptSafeEnvVarEntry> {
        self.env_vars
            .iter()
            .filter(|entry| !entry.name.trim().is_empty() && !entry.value.trim().is_empty())
            .map(|entry| PromptSafeEnvVarEntry {
                name: entry.name.clone(),
                secret: entry.secret,
                description: entry
                    .description
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
            })
            .collect()
    }

    /// Update the global env vars cache (called on config load / reload).
    pub fn publish_env_vars(&self) {
        let map = self.env_vars_as_map();
        let mut env_guard = ENV_VARS_CACHE
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *env_guard = map;

        let prompt_safe = self.prompt_safe_env_vars();
        let mut prompt_guard = PROMPT_SAFE_ENV_VARS_CACHE
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *prompt_guard = prompt_safe;
    }

    /// Read the current env vars snapshot (called by Bash tool at process spawn time).
    pub fn current_env_vars() -> HashMap<String, String> {
        ENV_VARS_CACHE
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Read the current prompt-safe env var snapshot (names + metadata only; no secret values).
    pub fn current_prompt_safe_env_vars() -> Vec<PromptSafeEnvVarEntry> {
        PROMPT_SAFE_ENV_VARS_CACHE
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
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
            skills: SkillsConfig::default(),
            env_vars: Vec::new(),
            default_work_area: None,
            access_control: None,
            features: FeatureFlags::default(),
            defaults: None,
            memory: None,
            mcp: bamboo_domain::mcp_config::McpConfig::default(),
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
        to_save.normalize_skill_settings();
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
    fn publish_env_vars_updates_prompt_safe_snapshot_without_secret_values() {
        let _lock = crate::test_support::env_cache_lock_acquire();
        let mut config = Config::default();
        config.env_vars = vec![
            EnvVarEntry {
                name: "SECRET_TOKEN".to_string(),
                value: "top-secret".to_string(),
                secret: true,
                value_encrypted: None,
                description: Some("Service token".to_string()),
            },
            EnvVarEntry {
                name: "API_BASE".to_string(),
                value: "https://internal.example".to_string(),
                secret: false,
                value_encrypted: None,
                description: Some("Internal API base".to_string()),
            },
        ];

        config.publish_env_vars();

        let injected = Config::current_env_vars();
        assert_eq!(
            injected.get("SECRET_TOKEN").map(String::as_str),
            Some("top-secret")
        );
        assert_eq!(
            injected.get("API_BASE").map(String::as_str),
            Some("https://internal.example")
        );

        let prompt_safe = Config::current_prompt_safe_env_vars();
        assert_eq!(prompt_safe.len(), 2);
        assert!(prompt_safe.iter().any(|entry| {
            entry.name == "SECRET_TOKEN"
                && entry.secret
                && entry.description.as_deref() == Some("Service token")
        }));
        assert!(prompt_safe.iter().any(|entry| {
            entry.name == "API_BASE"
                && !entry.secret
                && entry.description.as_deref() == Some("Internal API base")
        }));
        assert!(!prompt_safe
            .iter()
            .any(|entry| entry.name.contains("top-secret")));
        assert!(!prompt_safe.iter().any(|entry| {
            entry
                .description
                .as_deref()
                .is_some_and(|value| value.contains("https://internal.example"))
        }));
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
    fn get_memory_background_model_prefers_memory_specific_override() {
        let mut config = Config::default();
        config.features.provider_model_ref = false;
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-main".to_string()),
            fast_model: Some("gpt-fast".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: BTreeMap::new(),
        });
        config.memory = Some(MemoryConfig {
            background_model: Some("memory-fast".to_string()),
            ..MemoryConfig::default()
        });

        assert_eq!(
            config.get_memory_background_model().as_deref(),
            Some("memory-fast")
        );
    }

    #[test]
    fn get_memory_background_model_falls_back_to_provider_fast_model() {
        let mut config = Config::default();
        config.features.provider_model_ref = false;
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-main".to_string()),
            fast_model: Some("gpt-fast".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: BTreeMap::new(),
        });

        assert_eq!(
            config.get_memory_background_model().as_deref(),
            Some("gpt-fast")
        );
    }

    #[test]
    fn get_memory_background_model_does_not_fall_back_to_main_model() {
        let mut config = Config::default();
        config.features.provider_model_ref = false;
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-main".to_string()),
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: BTreeMap::new(),
        });

        assert!(config.get_memory_background_model().is_none());
    }

    #[test]
    fn memory_config_preserves_auto_dream_dream_refine_and_prompt_flags() {
        let config = Config {
            memory: Some(MemoryConfig {
                background_model: Some("dream-fast".to_string()),
                auto_dream_enabled: true,
                project_prompt_injection: false,
                relevant_recall: false,
                relevant_recall_rerank: true,
                project_first_dream: false,
                dream_refine_mode: true,
                ..MemoryConfig::default()
            }),
            ..Config::default()
        };

        let serialized = serde_json::to_string(&config).expect("config should serialize");
        let roundtrip: Config =
            serde_json::from_str(&serialized).expect("config should deserialize");
        let memory = roundtrip.memory.expect("memory config should exist");
        assert!(memory.auto_dream_enabled);
        assert!(!memory.project_prompt_injection);
        assert!(!memory.relevant_recall);
        assert!(memory.relevant_recall_rerank);
        assert!(!memory.project_first_dream);
        assert!(memory.dream_refine_mode);
    }

    #[test]
    fn memory_config_env_overrides_prompt_flags() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        let _home = EnvVarGuard::set("HOME", temp_home.path.to_string_lossy().as_ref());
        let _project_prompt = EnvVarGuard::set("BAMBOO_MEMORY_PROJECT_PROMPT_INJECTION", "false");
        let _relevant_recall = EnvVarGuard::set("BAMBOO_MEMORY_RELEVANT_RECALL", "0");
        let _relevant_recall_rerank =
            EnvVarGuard::set("BAMBOO_MEMORY_RELEVANT_RECALL_RERANK", "yes");
        let _project_first_dream = EnvVarGuard::set("BAMBOO_MEMORY_PROJECT_FIRST_DREAM", "no");

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        let memory = config
            .memory
            .expect("memory config should be created by env overrides");
        assert!(!memory.project_prompt_injection);
        assert!(!memory.relevant_recall);
        assert!(memory.relevant_recall_rerank);
        assert!(!memory.project_first_dream);
    }

    #[test]
    fn get_default_work_area_path_expands_tilde_and_requires_directory() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        let _home = EnvVarGuard::set("HOME", temp_home.path.to_string_lossy().as_ref());
        let target = temp_home.path.join("workspace-default");
        std::fs::create_dir_all(&target).expect("default work area dir should exist");

        let mut config = Config::default();
        config.default_work_area = Some(DefaultWorkAreaConfig {
            path: Some("~/workspace-default".to_string()),
        });

        assert_eq!(config.get_default_work_area_path(), Some(target));
    }

    #[test]
    fn get_default_work_area_path_returns_none_for_missing_directory() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        let _home = EnvVarGuard::set("HOME", temp_home.path.to_string_lossy().as_ref());

        let mut config = Config::default();
        config.default_work_area = Some(DefaultWorkAreaConfig {
            path: Some("~/missing-default-work-area".to_string()),
        });

        assert!(config.get_default_work_area_path().is_none());
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
    fn normalize_skill_settings_trims_dedupes_and_sorts() {
        let mut config = Config::default();
        config.skills.disabled = vec![
            " pdf ".to_string(),
            "".to_string(),
            "pdf".to_string(),
            "skill-creator".to_string(),
        ];

        config.normalize_skill_settings();

        assert_eq!(
            config.skills.disabled,
            vec!["pdf".to_string(), "skill-creator".to_string()]
        );
    }

    #[test]
    fn config_load_reads_disabled_skills_as_normalized_ids() {
        let _lock = env_lock_acquire();
        let temp_home = TempHome::new();
        temp_home.set_config_json(
            r#"{
  "skills": {
    "disabled": [" pdf ", "skill-creator", "pdf", ""]
  }
}"#,
        );

        let config = Config::from_data_dir(Some(temp_home.path.clone()));
        assert_eq!(
            config.skills.disabled,
            vec!["pdf".to_string(), "skill-creator".to_string()]
        );
        assert!(config.disabled_skill_ids().contains("pdf"));
        assert!(config.disabled_skill_ids().contains("skill-creator"));
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
        let key_guard = crate::encryption::set_test_encryption_key([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]);

        let auth = ProxyAuth {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        let auth_str = serde_json::to_string(&auth).expect("serialize proxy auth");
        let encrypted = crate::encryption::encrypt(&auth_str).expect("encrypt proxy auth");

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
        let key_guard = crate::encryption::set_test_encryption_key([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]);

        let auth = ProxyAuth {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        let auth_str = serde_json::to_string(&auth).expect("serialize proxy auth");
        let encrypted = crate::encryption::encrypt(&auth_str).expect("encrypt proxy auth");

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
        let key_guard = crate::encryption::set_test_encryption_key([
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
        let key_guard = crate::encryption::set_test_encryption_key([
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
            bamboo_domain::mcp_config::McpServerConfig {
                id: "stdio-secret".to_string(),
                name: None,
                enabled: true,
                transport: bamboo_domain::mcp_config::TransportConfig::Stdio(
                    bamboo_domain::mcp_config::StdioConfig {
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
                reconnect: bamboo_domain::mcp_config::ReconnectConfig::default(),
                allowed_tools: vec![],
                denied_tools: vec![],
            },
            bamboo_domain::mcp_config::McpServerConfig {
                id: "sse-secret".to_string(),
                name: None,
                enabled: true,
                transport: bamboo_domain::mcp_config::TransportConfig::Sse(
                    bamboo_domain::mcp_config::SseConfig {
                        url: "http://localhost:8080/sse".to_string(),
                        headers: vec![bamboo_domain::mcp_config::HeaderConfig {
                            name: "Authorization".to_string(),
                            value: "Bearer token123".to_string(),
                            value_encrypted: None,
                        }],
                        connect_timeout_ms: 5000,
                    },
                ),
                request_timeout_ms: 5000,
                healthcheck_interval_ms: 1000,
                reconnect: bamboo_domain::mcp_config::ReconnectConfig::default(),
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
            bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => {
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
            bamboo_domain::mcp_config::TransportConfig::Sse(sse) => {
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

        for _ in 0..10 {
            config.publish_env_vars();
            let map = Config::current_env_vars();
            if map.get("TEST_PUBLISH") == Some(&"pub_value".to_string()) {
                return;
            }
        }
        panic!("TEST_PUBLISH not found in cache after retries");
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

    // ---- defaults.* model resolution tests ----

    #[test]
    fn get_model_prefers_defaults_chat_when_provider_model_ref_enabled() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("legacy-gpt-4o".to_string()),
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
        });
        config.features.provider_model_ref = true;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("anthropic", "claude-3-7-sonnet"),
            fast: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_session: None,
            subagent_models: Default::default(),
        });

        assert_eq!(config.get_model(), Some("claude-3-7-sonnet".to_string()));
    }

    #[test]
    fn get_model_ignores_defaults_chat_when_provider_model_ref_disabled() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("legacy-gpt-4o".to_string()),
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
        });
        config.features.provider_model_ref = false;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("anthropic", "claude-3-7-sonnet"),
            fast: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_session: None,
            subagent_models: Default::default(),
        });

        assert_eq!(config.get_model(), Some("legacy-gpt-4o".to_string()));
    }

    #[test]
    fn get_fast_model_prefers_defaults_fast_when_provider_model_ref_enabled() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-4o".to_string()),
            fast_model: Some("legacy-gpt-4o-mini".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
        });
        config.features.provider_model_ref = true;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("openai", "gpt-4o"),
            fast: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_session: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_fast_model(),
            Some("claude-3-5-haiku".to_string())
        );
    }

    #[test]
    fn get_fast_model_ignores_defaults_fast_when_provider_model_ref_disabled() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-4o".to_string()),
            fast_model: Some("legacy-gpt-4o-mini".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
        });
        config.features.provider_model_ref = false;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("openai", "gpt-4o"),
            fast: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_session: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_fast_model(),
            Some("legacy-gpt-4o-mini".to_string())
        );
    }

    #[test]
    fn get_fast_model_falls_back_to_defaults_chat_when_fast_unset() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.features.provider_model_ref = true;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("anthropic", "claude-3-7-sonnet"),
            fast: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_session: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_fast_model(),
            Some("claude-3-7-sonnet".to_string())
        );
    }

    #[test]
    fn get_memory_background_model_prefers_defaults_memory_background() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-4o".to_string()),
            fast_model: Some("gpt-4o-mini".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
        });
        config.features.provider_model_ref = true;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("openai", "gpt-4o"),
            fast: Some(bamboo_domain::ProviderModelRef::new(
                "openai",
                "gpt-4o-mini",
            )),
            vision: None,
            memory_background: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            planning: None,
            search: None,
            code_review: None,
            sub_session: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_memory_background_model(),
            Some("claude-3-5-haiku".to_string())
        );
    }

    #[test]
    fn get_memory_background_model_falls_back_to_defaults_fast_when_memory_background_unset() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.features.provider_model_ref = true;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("openai", "gpt-4o"),
            fast: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_session: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_memory_background_model(),
            Some("claude-3-5-haiku".to_string())
        );
    }

    #[test]
    fn get_memory_background_model_ignores_defaults_when_provider_model_ref_disabled() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(OpenAIConfig {
            api_key: "test".to_string(),
            api_key_encrypted: None,
            base_url: None,
            model: Some("gpt-4o".to_string()),
            fast_model: Some("legacy-gpt-4o-mini".to_string()),
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: vec![],
            request_overrides: None,
            extra: Default::default(),
        });
        config.features.provider_model_ref = false;
        config.defaults = Some(DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new("openai", "gpt-4o"),
            fast: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            vision: None,
            memory_background: Some(bamboo_domain::ProviderModelRef::new(
                "anthropic",
                "claude-3-5-haiku",
            )),
            planning: None,
            search: None,
            code_review: None,
            sub_session: None,
            subagent_models: Default::default(),
        });

        assert_eq!(
            config.get_memory_background_model(),
            Some("legacy-gpt-4o-mini".to_string())
        );
    }
}
