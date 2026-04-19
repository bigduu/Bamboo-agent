use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::Session;
use bamboo_compression::limits::{load_model_limits_from_unified_config, ModelLimit};
use bamboo_compression::{ModelLimitsRegistry, TokenBudget};
use bamboo_infrastructure::provider::LLMProvider;

pub(super) async fn resolve_token_budget(
    session: &Session,
    config: &AgentLoopConfig,
    model_name: &str,
    llm: &dyn LLMProvider,
) -> TokenBudget {
    // Priority: session override > config override > model defaults.
    if let Some(ref budget) = session.token_budget {
        tracing::debug!("Using session-specific token budget");
        return budget.clone();
    }

    if let Some(ref budget) = config.token_budget {
        tracing::debug!("Using config token budget");
        return budget.clone();
    }

    // Default to model limits:
    // 1. built-in defaults
    // 2. provider runtime metadata (copilot only)
    // 3. dedicated config file: model_limits.json
    // 4. legacy fallback: config.json -> model_limits
    let mut registry = ModelLimitsRegistry::default();

    if let Some(provider_limit) = resolve_provider_runtime_limit(config, llm, model_name).await {
        registry.add_limit(provider_limit);
    }

    let loaded_from_file = match registry.load_user_config().await {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(
                "Failed to load model limits from {:?}: {}. Falling back to legacy config.json key.",
                bamboo_compression::limits::get_default_config_path(),
                error
            );
            false
        }
    };

    if !loaded_from_file {
        let unified_model_limits = match tokio::task::spawn_blocking(|| {
            let config = bamboo_infrastructure::Config::new();
            load_model_limits_from_unified_config(&config)
        })
        .await
        {
            Ok(Ok(limits)) => limits,
            Ok(Err(error)) => {
                tracing::warn!(
                    "Failed to parse legacy model limits from config.json key 'model_limits': {}.",
                    error
                );
                None
            }
            Err(error) => {
                tracing::warn!(
                    "Failed to load legacy model limits from config.json: {}.",
                    error
                );
                None
            }
        };

        if let Some(limits) = unified_model_limits {
            for limit in limits {
                registry.add_limit(limit);
            }
        }
    }

    let matched_limit = registry.get(model_name);
    let model_limit = matched_limit
        .clone()
        .unwrap_or_else(|| registry.get_or_default(model_name));

    if matched_limit.is_some() {
        tracing::debug!(
            "Using model limit for '{}': context={}, max_output={}, safety_margin={}",
            model_name,
            model_limit.max_context_tokens,
            model_limit.get_max_output_tokens(),
            model_limit.get_safety_margin()
        );
    } else {
        tracing::info!(
            "No model limit match for '{}', using fallback '{}' (context={}). Override via {:?}",
            model_name,
            model_limit.model_pattern,
            model_limit.max_context_tokens,
            bamboo_compression::limits::get_default_config_path()
        );
    }

    TokenBudget::with_safety_margin(
        model_limit.max_context_tokens,
        model_limit.get_max_output_tokens(),
        bamboo_compression::BudgetStrategy::default(),
        model_limit.get_safety_margin(),
    )
}

async fn resolve_provider_runtime_limit(
    config: &AgentLoopConfig,
    llm: &dyn LLMProvider,
    model_name: &str,
) -> Option<ModelLimit> {
    if config.provider_name.as_deref() != Some("copilot") {
        return None;
    }

    let model_info = match llm.list_model_info().await {
        Ok(models) => models.into_iter().find(|entry| entry.id == model_name),
        Err(error) => {
            tracing::warn!(
                "Failed to fetch Copilot model metadata for token budget: {}",
                error
            );
            None
        }
    }?;

    let max_context_tokens = model_info.max_context_tokens?;

    let mut limit = ModelLimit::new(model_name.to_string(), max_context_tokens);
    limit.max_output_tokens = model_info.max_output_tokens;

    tracing::info!(
        "Using Copilot runtime model metadata for '{}': context={}, max_output={}",
        model_name,
        max_context_tokens,
        model_info
            .max_output_tokens
            .map(|value| value.to_string())
            .unwrap_or_else(|| "auto".to_string())
    );

    Some(limit)
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use async_trait::async_trait;
    use futures::{stream, Stream};

    use super::*;
    use bamboo_agent_core::{tools::ToolSchema, Message};
    use bamboo_infrastructure::provider::{LLMError, ProviderModelInfo, Result};
    use bamboo_infrastructure::types::LLMChunk;

    #[derive(Default)]
    struct MetadataProvider {
        models: Vec<ProviderModelInfo>,
    }

    #[async_trait]
    impl LLMProvider for MetadataProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<LLMChunk>> + Send>>> {
            Ok(Box::pin(stream::empty()))
        }

        async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>> {
            Ok(self.models.clone())
        }
    }

    #[tokio::test]
    async fn resolve_provider_runtime_limit_uses_copilot_metadata() {
        let mut config = AgentLoopConfig::default();
        config.provider_name = Some("copilot".to_string());

        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "gpt-5.3-codex".to_string(),
                max_context_tokens: Some(222_000),
                max_output_tokens: Some(33_000),
            }],
        };

        let limit = resolve_provider_runtime_limit(&config, &provider, "gpt-5.3-codex")
            .await
            .expect("copilot metadata should resolve");
        assert_eq!(limit.max_context_tokens, 222_000);
        assert_eq!(limit.max_output_tokens, Some(33_000));
    }

    #[tokio::test]
    async fn resolve_provider_runtime_limit_ignores_non_copilot_provider() {
        let mut config = AgentLoopConfig::default();
        config.provider_name = Some("openai".to_string());

        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "gpt-5.3-codex".to_string(),
                max_context_tokens: Some(222_000),
                max_output_tokens: Some(33_000),
            }],
        };

        let limit = resolve_provider_runtime_limit(&config, &provider, "gpt-5.3-codex").await;
        assert!(limit.is_none());
    }

    #[tokio::test]
    async fn resolve_provider_runtime_limit_requires_context_tokens() {
        let mut config = AgentLoopConfig::default();
        config.provider_name = Some("copilot".to_string());

        let provider = MetadataProvider {
            models: vec![ProviderModelInfo {
                id: "gpt-5.3-codex".to_string(),
                max_context_tokens: None,
                max_output_tokens: Some(33_000),
            }],
        };

        let limit = resolve_provider_runtime_limit(&config, &provider, "gpt-5.3-codex").await;
        assert!(limit.is_none());
    }

    #[tokio::test]
    async fn resolve_provider_runtime_limit_returns_none_on_model_info_error() {
        struct FailingProvider;

        #[async_trait]
        impl LLMProvider for FailingProvider {
            async fn chat_stream(
                &self,
                _messages: &[Message],
                _tools: &[ToolSchema],
                _max_output_tokens: Option<u32>,
                _model: &str,
            ) -> Result<Pin<Box<dyn Stream<Item = Result<LLMChunk>> + Send>>> {
                Ok(Box::pin(stream::empty()))
            }

            async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>> {
                Err(LLMError::Api("boom".to_string()))
            }
        }

        let mut config = AgentLoopConfig::default();
        config.provider_name = Some("copilot".to_string());

        let limit =
            resolve_provider_runtime_limit(&config, &FailingProvider, "gpt-5.3-codex").await;
        assert!(limit.is_none());
    }
}
