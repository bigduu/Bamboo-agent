//! Append-only, per-LLM-call token-usage log.
//!
//! Each agent LLM call appends one [`TokenUsageRecord`] (as a JSON line) to the
//! session's `token-usage.jsonl`, written into the session's own directory next
//! to `session.json`. Unlike `session.token_usage` — a single snapshot that is
//! overwritten on every call — this log keeps the full per-round history so
//! cache effectiveness and cost can be analyzed offline: confirming the 1-hour
//! prompt-cache TTL keeps hitting across pauses, or measuring the cold round
//! right after a context compression.
//!
//! The record bridges two sources: the prompt-side budget snapshot
//! ([`TokenBudgetUsage`], already on the session) and the server-returned stream
//! stats — notably `cache_creation_input_tokens`, which is NOT part of
//! `TokenBudgetUsage` and would otherwise only exist in the logs.

use bamboo_domain::TokenBudgetUsage;
use serde::Serialize;

/// One per-LLM-call usage record. Flattened for line-oriented analysis
/// (jq / DuckDB / pandas over the JSONL).
#[derive(Debug, Clone, Serialize)]
pub struct TokenUsageRecord {
    /// RFC3339 wall-clock timestamp captured when the call completed.
    pub ts: String,
    pub session_id: String,
    pub model: String,
    pub provider: String,
    /// Conversation length (message count) at emit time — a monotonic-ish
    /// ordinal to correlate records with conversation growth.
    pub message_count: usize,

    // --- server-returned usage (this call) ---
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    /// Non-cached "fresh" input tokens (server-reported), disjoint from the two
    /// cache counts. The precise prompt size is
    /// `input_tokens + cache_read + cache_creation`, and the exact cache-hit
    /// ratio is `cache_read / that_sum`.
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub thinking_tokens: u64,

    // --- prompt-side budget snapshot (this call) ---
    pub system_tokens: u32,
    pub summary_tokens: u32,
    pub window_tokens: u32,
    pub total_tokens: u32,
    pub max_context_tokens: u32,
    pub budget_limit: u32,
    pub prompt_cached_tool_outputs: usize,
    pub prompt_cached_tool_tokens_saved: u32,
    pub truncation_occurred: bool,
    pub segments_removed: usize,
}

impl TokenUsageRecord {
    /// Build a record from the prompt-side budget snapshot (`usage`) and the
    /// server-side stream stats. The cache-creation count lives only on the
    /// stream output — it is not part of [`TokenBudgetUsage`] — so it is passed
    /// in explicitly.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ts: String,
        session_id: &str,
        model: &str,
        provider: &str,
        message_count: usize,
        usage: Option<&TokenBudgetUsage>,
        cache_creation_input_tokens: u64,
        cache_read_input_tokens: u64,
        input_tokens: u64,
        output_tokens: u64,
        thinking_tokens: u64,
    ) -> Self {
        Self {
            ts,
            session_id: session_id.to_string(),
            model: model.to_string(),
            provider: provider.to_string(),
            message_count,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            input_tokens,
            output_tokens,
            thinking_tokens,
            system_tokens: usage.map(|u| u.system_tokens).unwrap_or(0),
            summary_tokens: usage.map(|u| u.summary_tokens).unwrap_or(0),
            window_tokens: usage.map(|u| u.window_tokens).unwrap_or(0),
            total_tokens: usage.map(|u| u.total_tokens).unwrap_or(0),
            max_context_tokens: usage.map(|u| u.max_context_tokens).unwrap_or(0),
            budget_limit: usage.map(|u| u.budget_limit).unwrap_or(0),
            prompt_cached_tool_outputs: usage.map(|u| u.prompt_cached_tool_outputs).unwrap_or(0),
            prompt_cached_tool_tokens_saved: usage
                .map(|u| u.prompt_cached_tool_tokens_saved)
                .unwrap_or(0),
            truncation_occurred: usage.map(|u| u.truncation_occurred).unwrap_or(false),
            segments_removed: usage.map(|u| u.segments_removed).unwrap_or(0),
        }
    }

    /// Serialize to a single-line JSON string (no trailing newline; the storage
    /// layer frames the line).
    pub fn to_json_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_serializes_to_single_json_line_with_cache_creation() {
        let usage = TokenBudgetUsage {
            system_tokens: 5000,
            summary_tokens: 2000,
            window_tokens: 3000,
            total_tokens: 10000,
            max_context_tokens: 200_000,
            budget_limit: 180_000,
            truncation_occurred: false,
            segments_removed: 0,
            prompt_cached_tool_outputs: 1,
            prompt_cached_tool_tokens_saved: 42,
            thinking_tokens: 7,
            cache_read_input_tokens: 12_000,
        };
        let record = TokenUsageRecord::new(
            "2026-06-15T00:00:00Z".to_string(),
            "sess-1",
            "claude-opus-4-8",
            "anthropic",
            24,
            Some(&usage),
            1500, // cache_creation — only present on the stream output
            12_000,
            800, // input_tokens (fresh, non-cached)
            300,
            7,
        );
        let line = record.to_json_line().expect("serializes");
        assert!(!line.contains('\n'), "must be a single line");
        assert!(line.contains("\"cache_creation_input_tokens\":1500"));
        assert!(line.contains("\"cache_read_input_tokens\":12000"));
        assert!(line.contains("\"input_tokens\":800"));
        assert!(line.contains("\"session_id\":\"sess-1\""));
    }
}
