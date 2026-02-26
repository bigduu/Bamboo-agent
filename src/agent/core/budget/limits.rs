//! Model context window limits registry.
//!
//! Provides known context window sizes for common models, with fallback to
//! configurable user limits via file or session overrides.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Known model context window sizes.
///
/// These are the default context window limits for popular models.
/// Users can override these via configuration files.
pub const KNOWN_MODEL_LIMITS: &[(&str, u32)] = &[
    // OpenAI models
    ("gpt-4o", 128_000),
    ("gpt-4o-mini", 128_000),
    ("gpt-4-turbo", 128_000),
    // Treat gpt-4.1 as a modern long-context OpenAI model by default.
    // If your provider exposes a different context size, override via ~/.bamboo/model_limits.json.
    ("gpt-4.1", 128_000),
    ("gpt-4", 8_192),
    ("gpt-3.5-turbo", 16_385),
    // Anthropic models
    ("claude-3-5-sonnet", 200_000),
    ("claude-3-5-sonnet-20241022", 200_000),
    ("claude-3-5-sonnet-20240620", 200_000),
    ("claude-3-opus", 200_000),
    ("claude-3-opus-20240229", 200_000),
    ("claude-3-sonnet", 200_000),
    ("claude-3-haiku", 200_000),
    // Copilot models (same as OpenAI in Copilot Chat)
    ("copilot-chat", 128_000),
    // Default fallback
    ("default", 128_000),
];

/// Default maximum output tokens (reserve ~25% for response).
pub const DEFAULT_MAX_OUTPUT_TOKENS: u32 = 4096;

/// Default safety margin for token counting errors.
pub const DEFAULT_SAFETY_MARGIN: u32 = 1000;

/// Model limit configuration (user-overridable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelLimit {
    /// Model identifier (partial match supported, e.g., "gpt-4" matches "gpt-4o")
    pub model_pattern: String,
    /// Maximum context window size in tokens
    pub max_context_tokens: u32,
    /// Maximum output tokens (defaults to min(4096, max_context / 4))
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    /// Safety margin for token counting (defaults to 1000)
    #[serde(default)]
    pub safety_margin: Option<u32>,
}

impl ModelLimit {
    /// Create a new model limit with defaults.
    pub fn new(model_pattern: impl Into<String>, max_context_tokens: u32) -> Self {
        Self {
            model_pattern: model_pattern.into(),
            max_context_tokens,
            max_output_tokens: None,
            safety_margin: None,
        }
    }

    /// Get max output tokens with default calculation.
    pub fn get_max_output_tokens(&self) -> u32 {
        self.max_output_tokens
            .unwrap_or_else(|| (self.max_context_tokens / 4).min(4096))
    }

    /// Get safety margin with default.
    pub fn get_safety_margin(&self) -> u32 {
        self.safety_margin.unwrap_or(DEFAULT_SAFETY_MARGIN)
    }
}

/// Registry for model limits with built-in defaults and user overrides.
#[derive(Debug, Clone)]
pub struct ModelLimitsRegistry {
    /// User-provided overrides (higher priority than built-in)
    user_limits: HashMap<String, ModelLimit>,
    /// Default path for user configuration file
    config_path: Option<PathBuf>,
}

impl ModelLimitsRegistry {
    /// Create a new registry with built-in defaults only.
    pub fn new() -> Self {
        Self {
            user_limits: HashMap::new(),
            config_path: None,
        }
    }

    /// Create a registry with a specific config file path.
    pub fn with_config_path(path: impl Into<PathBuf>) -> Self {
        Self {
            user_limits: HashMap::new(),
            config_path: Some(path.into()),
        }
    }

    /// Load user overrides from the default configuration path.
    ///
    /// Default path: `~/.bamboo/model_limits.json`
    pub async fn load_user_config(&mut self) -> std::io::Result<()> {
        let path = self
            .config_path
            .clone()
            .unwrap_or_else(get_default_config_path);

        if !path.exists() {
            return Ok(());
        }

        let content = tokio::fs::read_to_string(&path).await?;
        let limits: Vec<ModelLimit> = serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        for limit in limits {
            self.user_limits.insert(limit.model_pattern.clone(), limit);
        }

        tracing::info!(
            "Loaded {} user model limits from {:?}",
            self.user_limits.len(),
            path
        );
        Ok(())
    }

    /// Add a user limit override.
    pub fn add_limit(&mut self, limit: ModelLimit) {
        self.user_limits.insert(limit.model_pattern.clone(), limit);
    }

    /// Get limit for a model, with user overrides taking priority.
    ///
    /// Returns `None` if no matching limit is found.
    ///
    /// # Matching Strategy
    /// 1. Exact match (highest priority)
    /// 2. Model contains pattern (e.g., "gpt-4o-mini" contains "gpt-4o")
    /// 3. Pattern contains model (e.g., "gpt-4" contains "gpt")
    ///
    /// For partial matches, the longest (most specific) pattern wins.
    pub fn get(&self, model: &str) -> Option<ModelLimit> {
        // First check user limits for exact match
        if let Some(limit) = self.user_limits.get(model) {
            return Some(limit.clone());
        }

        // Check built-in limits for exact match
        for (pattern, tokens) in KNOWN_MODEL_LIMITS {
            if *pattern == model {
                return Some(ModelLimit::new(model.to_string(), *tokens));
            }
        }

        // Find the best partial match from user limits
        // Sort by pattern length (longer = more specific) for deterministic selection
        let best_user_match = self
            .user_limits
            .iter()
            .filter(|(pattern, _)| model.contains(*pattern) || pattern.contains(model))
            .max_by_key(|(pattern, _)| pattern.len())
            .map(|(_, limit)| limit.clone());

        if let Some(limit) = best_user_match {
            return Some(limit);
        }

        // Find the best partial match from built-in limits
        let best_builtin_match = KNOWN_MODEL_LIMITS
            .iter()
            .filter(|(pattern, _)| model.contains(*pattern) || pattern.contains(model))
            .max_by_key(|(pattern, _)| pattern.len());

        if let Some((pattern, tokens)) = best_builtin_match {
            return Some(ModelLimit::new(pattern.to_string(), *tokens));
        }

        None
    }

    /// Get limit for a model with fallback to default.
    pub fn get_or_default(&self, model: &str) -> ModelLimit {
        self.get(model).unwrap_or_else(|| {
            let default = KNOWN_MODEL_LIMITS
                .iter()
                .find(|(k, _)| *k == "default")
                .map(|(_, v)| *v)
                .unwrap_or(128_000);
            ModelLimit::new("default", default)
        })
    }

    /// Save current user limits to the configuration file.
    pub async fn save_user_config(&self) -> std::io::Result<()> {
        let path = self
            .config_path
            .clone()
            .unwrap_or_else(get_default_config_path);

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let limits: Vec<&ModelLimit> = self.user_limits.values().collect();
        let content = serde_json::to_string_pretty(&limits)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        tokio::fs::write(&path, content).await?;

        Ok(())
    }

    /// List all user-defined limits.
    pub fn list_user_limits(&self) -> Vec<&ModelLimit> {
        self.user_limits.values().collect()
    }
}

impl Default for ModelLimitsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Get the default configuration file path.
///
/// Returns `~/.bamboo/model_limits.json` on Unix systems,
/// or the appropriate equivalent on Windows.
pub fn get_default_config_path() -> PathBuf {
    crate::core::paths::bamboo_dir().join("model_limits.json")
}

/// Create a token budget for a specific model.
///
/// This is a convenience function that creates a budget with appropriate defaults.
pub fn create_budget_for_model(
    model: &str,
    strategy: crate::agent::core::budget::BudgetStrategy,
) -> crate::agent::core::budget::TokenBudget {
    let registry = ModelLimitsRegistry::default();
    let limit = registry.get_or_default(model);

    crate::agent::core::budget::TokenBudget {
        max_context_tokens: limit.max_context_tokens,
        max_output_tokens: limit.get_max_output_tokens(),
        strategy,
        safety_margin: limit.get_safety_margin(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_limits_contain_common_models() {
        let gpt4 = KNOWN_MODEL_LIMITS
            .iter()
            .find(|(k, _)| *k == "gpt-4o")
            .expect("Should have gpt-4o");
        assert_eq!(gpt4.1, 128_000);
    }

    #[test]
    fn registry_finds_builtin_by_exact_match() {
        let registry = ModelLimitsRegistry::new();
        let limit = registry.get("gpt-4o").expect("Should find gpt-4o");
        assert_eq!(limit.max_context_tokens, 128_000);
    }

    #[test]
    fn registry_finds_builtin_by_partial_match() {
        let registry = ModelLimitsRegistry::new();
        // "gpt-4o-mini" contains "gpt-4o"
        let limit = registry
            .get("gpt-4o-mini")
            .expect("Should find gpt-4o-mini");
        assert_eq!(limit.max_context_tokens, 128_000);
    }

    #[test]
    fn registry_returns_default_for_unknown() {
        let registry = ModelLimitsRegistry::new();
        let limit = registry.get_or_default("unknown-model-xyz");
        assert_eq!(limit.model_pattern, "default");
    }

    #[test]
    fn user_override_takes_precedence() {
        let mut registry = ModelLimitsRegistry::new();
        registry.add_limit(ModelLimit::new("gpt-4o", 64_000)); // Override with smaller limit

        let limit = registry
            .get("gpt-4o")
            .expect("Should find overridden limit");
        assert_eq!(limit.max_context_tokens, 64_000);
    }

    #[test]
    fn model_limit_calculates_default_output_tokens() {
        let limit = ModelLimit::new("test", 100_000);
        // Default is min(max_context / 4, 4096) = min(25000, 4096) = 4096
        assert_eq!(limit.get_max_output_tokens(), 4096);
    }

    #[test]
    fn model_limit_uses_custom_output_tokens() {
        let mut limit = ModelLimit::new("test", 100_000);
        limit.max_output_tokens = Some(8192);
        assert_eq!(limit.get_max_output_tokens(), 8192);
    }

    #[test]
    fn model_limit_calculates_small_context_output() {
        let limit = ModelLimit::new("test", 8_192);
        // Default is min(8192 / 4, 4096) = 2048
        assert_eq!(limit.get_max_output_tokens(), 2048);
    }
}
