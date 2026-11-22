//! Claude effort mapping from LiteLLM c3cafa3782642685ef10777e4c5931400c0a7176.
//! Model capabilities are projected from the pinned model catalog. The rules
//! follow `litellm-rust/crates/llms/src/anthropic/messages/thinking.rs`.

use bamboo_domain::ReasoningEffort;
use bamboo_litellm_llms_types::formats::messages::{EffortLevel, ThinkingConfig, ThinkingDisplay};

use crate::provider::{LLMError, Result};

use super::capabilities::CLAUDE_THINKING;

const MIN_LEGACY_BUDGET: u64 = 1024;

#[derive(Clone, Copy)]
pub(super) enum ClaudeThinking {
    Legacy,
    Adaptive {
        supports_xhigh: bool,
        disable_with_thinking_field: bool,
        always_on: bool,
    },
}

fn claude_thinking(model: &str) -> Option<ClaudeThinking> {
    // Use exact catalog IDs so a new model or a gateway alias keeps Bamboo's
    // established Messages behavior until its capabilities are verified.
    let model = model.strip_prefix("anthropic/").unwrap_or(model);
    CLAUDE_THINKING
        .binary_search_by_key(&model, |(name, _)| *name)
        .ok()
        .map(|index| CLAUDE_THINKING[index].1)
}

pub(super) fn controls(
    model: &str,
    effort: Option<ReasoningEffort>,
) -> Result<(Option<ThinkingConfig>, Option<EffortLevel>)> {
    let Some(effort) = effort else {
        return Ok((None, None));
    };
    let profile = claude_thinking(model);
    let (level, budget) = match effort {
        ReasoningEffort::Disabled => {
            // Only default-on models with an off switch need an explicit
            // disabled field. Unknown models keep Bamboo's existing behavior.
            let send_disabled = profile.is_none_or(|profile| {
                matches!(
                    profile,
                    ClaudeThinking::Adaptive {
                        disable_with_thinking_field: true,
                        ..
                    }
                )
            });
            return Ok((
                send_disabled.then(|| ThinkingConfig::Disabled(Default::default())),
                None,
            ));
        }
        ReasoningEffort::Low => (EffortLevel::Low, 1024),
        ReasoningEffort::Medium => (EffortLevel::Medium, 2048),
        ReasoningEffort::High => (EffortLevel::High, 4096),
        ReasoningEffort::Xhigh => (EffortLevel::Xhigh, 8192),
        ReasoningEffort::Max => (EffortLevel::Max, 16384),
    };

    match profile {
        Some(ClaudeThinking::Legacy) => Ok((Some(ThinkingConfig::enabled(budget)), None)),
        Some(ClaudeThinking::Adaptive {
            supports_xhigh: false,
            ..
        }) if level == EffortLevel::Xhigh => Err(LLMError::Api(
            "Claude Messages does not support xhigh reasoning for the selected model".into(),
        )),
        Some(ClaudeThinking::Adaptive { .. }) => Ok((
            Some(ThinkingConfig::adaptive(Some(ThinkingDisplay::Summarized))),
            Some(level),
        )),
        None => Ok((Some(ThinkingConfig::adaptive(None)), Some(level))),
    }
}

pub(crate) fn thinking_replay_enabled(model: &str, effort: Option<ReasoningEffort>) -> bool {
    if effort.is_some_and(|effort| effort != ReasoningEffort::Disabled) {
        return true;
    }
    match claude_thinking(model) {
        Some(ClaudeThinking::Adaptive {
            always_on,
            disable_with_thinking_field,
            ..
        }) => always_on || (effort.is_none() && disable_with_thinking_field),
        _ => false,
    }
}

pub(super) fn fit_legacy_budget(budget: u64, max_tokens: u64) -> Result<u64> {
    if max_tokens <= MIN_LEGACY_BUDGET {
        // The pinned transform omits thinking here. Bamboo has already planned
        // a reasoning-enabled turn and requires room for its signed history.
        return Err(LLMError::Api(format!(
            "Claude Messages legacy thinking requires max_tokens above {MIN_LEGACY_BUDGET}"
        )));
    }
    Ok(budget.min(max_tokens - 1))
}
