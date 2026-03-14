use crate::agent::core::budget::limits::load_model_limits_from_unified_config;
use crate::agent::core::budget::{ModelLimitsRegistry, TokenBudget};
use crate::agent::core::Session;
use crate::agent::loop_module::config::AgentLoopConfig;

pub(super) async fn resolve_token_budget(
    session: &Session,
    config: &AgentLoopConfig,
    model_name: &str,
) -> TokenBudget {
    // Priority: session override > config override > model defaults.
    if let Some(ref budget) = session.token_budget {
        log::debug!("Using session-specific token budget");
        return budget.clone();
    }

    if let Some(ref budget) = config.token_budget {
        log::debug!("Using config token budget");
        return budget.clone();
    }

    // Default to model limits:
    // 1. built-in defaults
    // 2. optional unified config override: config.json -> model_limits
    // 3. legacy fallback: model_limits.json
    let mut registry = ModelLimitsRegistry::default();

    let unified_model_limits = match tokio::task::spawn_blocking(|| {
        let config = crate::core::Config::new();
        load_model_limits_from_unified_config(&config)
    })
    .await
    {
        Ok(Ok(limits)) => limits,
        Ok(Err(error)) => {
            log::warn!(
                "Failed to parse model limits from config.json key 'model_limits': {}. Falling back to legacy file.",
                error
            );
            None
        }
        Err(error) => {
            log::warn!(
                "Failed to load model limits from config.json: {}. Falling back to legacy file.",
                error
            );
            None
        }
    };

    if let Some(limits) = unified_model_limits {
        for limit in limits {
            registry.add_limit(limit);
        }
    } else if let Err(error) = registry.load_user_config().await {
        log::warn!(
            "Failed to load model limits from legacy {:?}: {}",
            crate::agent::core::budget::limits::get_default_config_path(),
            error
        );
    }

    let matched_limit = registry.get(model_name);
    let model_limit = matched_limit
        .clone()
        .unwrap_or_else(|| registry.get_or_default(model_name));

    if matched_limit.is_some() {
        log::debug!(
            "Using model limit for '{}': context={}, max_output={}, safety_margin={}",
            model_name,
            model_limit.max_context_tokens,
            model_limit.get_max_output_tokens(),
            model_limit.get_safety_margin()
        );
    } else {
        log::info!(
            "No model limit match for '{}', using fallback '{}' (context={}). Override via {:?}",
            model_name,
            model_limit.model_pattern,
            model_limit.max_context_tokens,
            crate::agent::core::budget::limits::get_default_config_path()
        );
    }

    TokenBudget::with_safety_margin(
        model_limit.max_context_tokens,
        model_limit.get_max_output_tokens(),
        crate::agent::core::budget::BudgetStrategy::default(),
        model_limit.get_safety_margin(),
    )
}
