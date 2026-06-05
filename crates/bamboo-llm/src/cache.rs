//! Provider-agnostic prompt caching.
//!
//! Prompt caching has two halves that differ per provider but share a single
//! policy, so the policy lives here and each provider only renders it:
//!
//! 1. **Where the cacheable prefix ends.** Anthropic needs explicit
//!    `cache_control` breakpoints (at most [`MAX_ANTHROPIC_CACHE_BREAKPOINTS`]);
//!    OpenAI / Gemini / Copilot cache an identical prefix automatically. In every
//!    case a cache *hit* requires the bytes before the breakpoint to be identical
//!    to a previous request. That means the engine must keep per-round volatile
//!    content (task list, recalled memory, plan state) **out** of the cacheable
//!    prefix and order it last — otherwise the breakpoint moves every round and
//!    the cache read size swings or drops to zero.
//!
//! 2. **How cached-token usage is reported.** Anthropic reports
//!    `cache_read_input_tokens`; OpenAI-compatible APIs report
//!    `prompt_tokens_details.cached_tokens` (or `input_tokens_details.cached_tokens`
//!    on the Responses API); Gemini reports `cachedContentTokenCount`. The
//!    `cache_usage_from_*` helpers normalize these into [`LLMChunk::CacheUsage`]
//!    so the same downstream accounting (and the frontend cache badge) works for
//!    every provider.
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

/// Normalize an OpenAI-style `usage` object into a [`LLMChunk::CacheUsage`], if
/// it reports cached prompt tokens.
///
/// OpenAI exposes cached input tokens under `prompt_tokens_details.cached_tokens`
/// (Chat Completions) or `input_tokens_details.cached_tokens` (Responses API).
/// Returns `None` when no cache hit is reported, so callers can skip emitting.
pub fn cache_usage_from_openai_usage(usage: &Value) -> Option<LLMChunk> {
    let cached = usage
        .get("prompt_tokens_details")
        .or_else(|| usage.get("input_tokens_details"))
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    (cached > 0).then_some(LLMChunk::CacheUsage {
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: cached,
    })
}

/// Normalize a Gemini `usageMetadata` object into a [`LLMChunk::CacheUsage`], if
/// it reports cached content tokens (`cachedContentTokenCount`).
pub fn cache_usage_from_gemini_usage(usage_metadata: &Value) -> Option<LLMChunk> {
    let cached = usage_metadata
        .get("cachedContentTokenCount")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    (cached > 0).then_some(LLMChunk::CacheUsage {
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: cached,
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
