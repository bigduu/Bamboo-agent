//! Provider-agnostic prompt caching.
//!
//! Prompt caching has two halves that differ per provider but share a single
//! policy, so the policy lives here and each provider only renders it:
//!
//! 1. **Where the cacheable prefix ends.** Anthropic needs explicit
//!    `cache_control` breakpoints (at most [`MAX_ANTHROPIC_CACHE_BREAKPOINTS`]).
//!    GPT-5.6+ Responses requests can lower the same plan into explicit OpenAI
//!    content breakpoints; older OpenAI models, Gemini, and Copilot retain their
//!    automatic prefix caching. In every case a cache *hit* requires the bytes
//!    before the selected prefix boundary to be identical to a previous request.
//!    That means the engine must keep per-round volatile content (task list,
//!    recalled memory, plan state) **out** of the cacheable prefix and order it
//!    last — otherwise the boundary moves every round and the cache read size
//!    swings or drops to zero.
//!
//! 2. **How cached-token usage is reported.** Anthropic reports
//!    `cache_read_input_tokens`; OpenAI-compatible APIs report
//!    `prompt_tokens_details.cached_tokens` (or `input_tokens_details.cached_tokens`
//!    on the Responses API); Gemini reports `cachedContentTokenCount`. The
//!    OpenAI terminal usage is normalized into a combined
//!    [`LLMChunk::ProviderUsage`] snapshot, while the remaining
//!    `cache_usage_from_*` helpers use [`LLMChunk::CacheUsage`]. Both feed the
//!    same downstream cache fields (and frontend cache badge).
//!
//! [`PromptCachePlan`] is the provider-agnostic description of (1): the engine
//! builds it once from the prompt envelope and each provider renders it in its
//! own dialect.

use crate::types::LLMChunk;
use serde_json::Value;

/// Anthropic accepts at most this many `cache_control` breakpoints per request.
/// Exceeding it is an API error, so renderers must clamp to this budget.
pub const MAX_ANTHROPIC_CACHE_BREAKPOINTS: usize = 4;

/// TTL hint for providers that expose a configurable cache lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheTtl {
    /// Provider default (Anthropic: 5 minutes).
    #[default]
    Default,
    /// Extended lifetime (Anthropic: 1 hour). Requires the
    /// `extended-cache-ttl-2025-04-11` beta header on the request.
    Extended,
}

impl CacheTtl {
    /// The `ttl` string Anthropic expects inside `cache_control`, if any.
    pub fn anthropic_ttl(self) -> Option<&'static str> {
        match self {
            CacheTtl::Default => None,
            CacheTtl::Extended => Some("1h"),
        }
    }
}

/// Provider-agnostic description of which logical regions of a request form a
/// stable, cacheable prefix.
///
/// Breakpoints are identified by **message id** rather than position so the plan
/// survives provider-side message reshaping — for example, Anthropic merges
/// consecutive tool-result messages into the preceding user message, which would
/// invalidate positional indices.
#[derive(Debug, Clone, Default)]
pub struct PromptCachePlan {
    /// Cache the tool-definition block (stable for the whole session).
    pub cache_tools: bool,
    /// Cache the system prompt. Only set this when the system prompt is free of
    /// per-round volatile content (the engine guarantees this by moving volatile
    /// context blocks to the conversation tail).
    pub cache_system: bool,
    /// Ids of messages that end a stable prefix; each becomes a cache
    /// breakpoint. Order is not significant.
    pub breakpoint_message_ids: Vec<String>,
    /// TTL hint for providers that support it.
    pub ttl: CacheTtl,
}

impl PromptCachePlan {
    /// A plan that requests no caching.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// True when the plan asks for at least one cache breakpoint.
    pub fn is_enabled(&self) -> bool {
        self.cache_tools || self.cache_system || !self.breakpoint_message_ids.is_empty()
    }

    /// Whether the given message id is marked as a cache breakpoint.
    pub fn is_breakpoint(&self, message_id: &str) -> bool {
        self.breakpoint_message_ids
            .iter()
            .any(|id| id == message_id)
    }
}

/// Normalize an OpenAI-style `usage` object into a cache-only chunk, if it
/// reports cached prompt tokens.
///
/// OpenAI exposes cached input tokens under `prompt_tokens_details.cached_tokens`
/// (Chat Completions) or `input_tokens_details.cached_tokens` (Responses API).
/// Returns `None` when no cache hit is reported, so callers can skip emitting.
///
/// Streaming parsers use [`provider_usage_from_openai_usage`] instead so input,
/// output, reasoning, and cache fields from one terminal event remain atomic.
pub fn cache_usage_from_openai_usage(usage: &Value) -> Option<LLMChunk> {
    let cached = usage
        .get("prompt_tokens_details")
        .or_else(|| usage.get("input_tokens_details"))
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    // Non-cached fresh input = total prompt input minus the cached portion.
    let prompt = usage
        .get("prompt_tokens")
        .or_else(|| usage.get("input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    (cached > 0).then_some(LLMChunk::CacheUsage {
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: cached,
        input_tokens: prompt.saturating_sub(cached),
    })
}

/// Preserve every authoritative token field reported by an OpenAI-compatible
/// terminal usage object in one [`LLMChunk::ProviderUsage`] snapshot.
///
/// Chat Completions and Responses use different field names, so both spellings
/// are accepted. Each value remains optional: an explicit zero is retained,
/// while an absent or non-numeric field is not synthesized. OpenAI reports
/// reasoning tokens as a subset of output/completion tokens.
pub fn provider_usage_from_openai_usage(usage: &Value) -> Option<LLMChunk> {
    fn direct_u64(value: &Value, key: &str) -> Option<u64> {
        value.get(key).and_then(Value::as_u64)
    }

    fn nested_u64(value: &Value, parent: &str, key: &str) -> Option<u64> {
        value
            .get(parent)
            .and_then(|details| details.get(key))
            .and_then(Value::as_u64)
    }

    let input_tokens =
        direct_u64(usage, "prompt_tokens").or_else(|| direct_u64(usage, "input_tokens"));
    let output_tokens =
        direct_u64(usage, "completion_tokens").or_else(|| direct_u64(usage, "output_tokens"));
    let total_tokens = direct_u64(usage, "total_tokens");
    let reasoning_tokens = nested_u64(usage, "completion_tokens_details", "reasoning_tokens")
        .or_else(|| nested_u64(usage, "output_tokens_details", "reasoning_tokens"))
        .or_else(|| direct_u64(usage, "reasoning_tokens"));
    let cache_read_input_tokens = nested_u64(usage, "prompt_tokens_details", "cached_tokens")
        .or_else(|| nested_u64(usage, "input_tokens_details", "cached_tokens"));
    let cache_write_input_tokens = nested_u64(usage, "prompt_tokens_details", "cache_write_tokens")
        .or_else(|| nested_u64(usage, "input_tokens_details", "cache_write_tokens"));

    if input_tokens.is_none()
        && output_tokens.is_none()
        && total_tokens.is_none()
        && reasoning_tokens.is_none()
        && cache_read_input_tokens.is_none()
        && cache_write_input_tokens.is_none()
    {
        return None;
    }

    Some(LLMChunk::ProviderUsage {
        input_tokens,
        output_tokens,
        total_tokens,
        reasoning_tokens,
        cache_creation_input_tokens: None,
        cache_read_input_tokens,
        cache_write_input_tokens,
    })
}

/// Normalize a Gemini `usageMetadata` object into a [`LLMChunk::CacheUsage`], if
/// it reports cached content tokens (`cachedContentTokenCount`).
pub fn cache_usage_from_gemini_usage(usage_metadata: &Value) -> Option<LLMChunk> {
    let cached = usage_metadata
        .get("cachedContentTokenCount")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let prompt = usage_metadata
        .get("promptTokenCount")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    (cached > 0).then_some(LLMChunk::CacheUsage {
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: cached,
        input_tokens: prompt.saturating_sub(cached),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_plan_is_not_enabled() {
        assert!(!PromptCachePlan::disabled().is_enabled());
    }

    #[test]
    fn plan_with_any_region_is_enabled() {
        assert!(PromptCachePlan {
            cache_system: true,
            ..Default::default()
        }
        .is_enabled());
        assert!(PromptCachePlan {
            breakpoint_message_ids: vec!["m1".to_string()],
            ..Default::default()
        }
        .is_enabled());
    }

    #[test]
    fn is_breakpoint_matches_only_listed_ids() {
        let plan = PromptCachePlan {
            breakpoint_message_ids: vec!["a".to_string(), "b".to_string()],
            ..Default::default()
        };
        assert!(plan.is_breakpoint("a"));
        assert!(plan.is_breakpoint("b"));
        assert!(!plan.is_breakpoint("c"));
    }

    #[test]
    fn extended_ttl_maps_to_one_hour() {
        assert_eq!(CacheTtl::Extended.anthropic_ttl(), Some("1h"));
        assert_eq!(CacheTtl::Default.anthropic_ttl(), None);
    }

    #[test]
    fn openai_cache_usage_reads_prompt_and_input_details() {
        let chat = serde_json::json!({"prompt_tokens_details": {"cached_tokens": 1234}});
        match cache_usage_from_openai_usage(&chat) {
            Some(LLMChunk::CacheUsage {
                cache_read_input_tokens,
                ..
            }) => assert_eq!(cache_read_input_tokens, 1234),
            other => panic!("expected CacheUsage, got {other:?}"),
        }

        let responses = serde_json::json!({"input_tokens_details": {"cached_tokens": 99}});
        match cache_usage_from_openai_usage(&responses) {
            Some(LLMChunk::CacheUsage {
                cache_read_input_tokens,
                ..
            }) => assert_eq!(cache_read_input_tokens, 99),
            other => panic!("expected CacheUsage, got {other:?}"),
        }
    }

    #[test]
    fn openai_cache_usage_none_when_no_cache_hit() {
        let usage = serde_json::json!({"prompt_tokens_details": {"cached_tokens": 0}});
        assert!(cache_usage_from_openai_usage(&usage).is_none());
        assert!(cache_usage_from_openai_usage(&serde_json::json!({})).is_none());
    }

    #[test]
    fn openai_provider_usage_preserves_chat_and_responses_fields() {
        let chat = serde_json::json!({
            "prompt_tokens": 100,
            "completion_tokens": 30,
            "prompt_tokens_details": {
                "cached_tokens": 0,
                "cache_write_tokens": 12
            },
            "completion_tokens_details": {"reasoning_tokens": 7}
        });
        assert!(matches!(
            provider_usage_from_openai_usage(&chat),
            Some(LLMChunk::ProviderUsage {
                input_tokens: Some(100),
                output_tokens: Some(30),
                total_tokens: None,
                reasoning_tokens: Some(7),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(0),
                cache_write_input_tokens: Some(12),
            })
        ));

        let responses = serde_json::json!({
            "input_tokens": 80,
            "output_tokens": 20,
            "total_tokens": 100,
            "input_tokens_details": {
                "cached_tokens": 24,
                "cache_write_tokens": 64
            },
            "output_tokens_details": {"reasoning_tokens": 5}
        });
        assert!(matches!(
            provider_usage_from_openai_usage(&responses),
            Some(LLMChunk::ProviderUsage {
                input_tokens: Some(80),
                output_tokens: Some(20),
                total_tokens: Some(100),
                reasoning_tokens: Some(5),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(24),
                cache_write_input_tokens: Some(64),
            })
        ));
    }

    #[test]
    fn openai_provider_usage_does_not_invent_missing_fields() {
        assert!(provider_usage_from_openai_usage(&serde_json::json!({})).is_none());
        assert!(matches!(
            provider_usage_from_openai_usage(&serde_json::json!({
            "total_tokens": 42,
            "prompt_tokens": null,
            "output_tokens_details": {}
            })),
            Some(LLMChunk::ProviderUsage {
                input_tokens: None,
                output_tokens: None,
                total_tokens: Some(42),
                reasoning_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                cache_write_input_tokens: None,
            })
        ));
    }

    #[test]
    fn gemini_cache_usage_reads_cached_content_tokens() {
        let usage = serde_json::json!({"cachedContentTokenCount": 555});
        match cache_usage_from_gemini_usage(&usage) {
            Some(LLMChunk::CacheUsage {
                cache_read_input_tokens,
                ..
            }) => assert_eq!(cache_read_input_tokens, 555),
            other => panic!("expected CacheUsage, got {other:?}"),
        }
        assert!(cache_usage_from_gemini_usage(&serde_json::json!({})).is_none());
    }
}
