use bamboo_agent_core::tools::ToolCallAccumulator;

use super::{
    InterruptedStreamOutput, PartialToolCallSnapshot, ProviderUsageSnapshot, StreamHandlingOutput,
};

pub(super) struct StreamAccumulationState {
    response_id: Option<String>,
    content: String,
    reasoning_content: String,
    /// Provider-minted signature covering `reasoning_content` (#520). `None`
    /// until a signature arrives; an empty-string invalidation marker clears it
    /// permanently for this stream (`reasoning_signature_invalidated`).
    reasoning_signature: Option<String>,
    reasoning_signature_invalidated: bool,
    token_count: usize,
    tool_calls: ToolCallAccumulator,
    output_tokens: u64,
    thinking_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
    provider_usage: Option<ProviderUsageSnapshot>,
    input_tokens: u64,
}

impl StreamAccumulationState {
    pub(super) fn new() -> Self {
        Self {
            response_id: None,
            content: String::new(),
            reasoning_content: String::new(),
            reasoning_signature: None,
            reasoning_signature_invalidated: false,
            token_count: 0,
            tool_calls: ToolCallAccumulator::new(),
            output_tokens: 0,
            thinking_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            provider_usage: None,
            input_tokens: 0,
        }
    }

    pub(super) fn append_token(&mut self, token: &str) {
        self.token_count += token.len();
        self.content.push_str(token);
    }

    pub(super) fn append_reasoning_token(&mut self, token: &str) {
        self.reasoning_content.push_str(token);
    }

    /// Record a provider-minted reasoning signature (#520). An empty string is
    /// the parser's invalidation marker — the turn produced multiple thinking
    /// blocks (or a redacted one), so no single signature covers the
    /// accumulated reasoning text and none may be persisted.
    pub(super) fn record_reasoning_signature(&mut self, signature: String) {
        if signature.is_empty() {
            self.reasoning_signature = None;
            self.reasoning_signature_invalidated = true;
        } else if !self.reasoning_signature_invalidated {
            self.reasoning_signature = Some(signature);
        }
    }

    pub(super) fn set_response_id(&mut self, response_id: String) {
        self.response_id = Some(response_id);
    }

    pub(super) fn extend_tool_calls(
        &mut self,
        partial_calls: Vec<bamboo_agent_core::tools::ToolCall>,
    ) {
        self.tool_calls.extend(partial_calls);
    }

    /// Accumulate index-tagged tool-call fragments, routing each to its call by
    /// provider index (the OpenAI-compatible chat-completions path). #236.
    pub(super) fn extend_tool_calls_indexed(
        &mut self,
        partial_calls: Vec<(u32, bamboo_agent_core::tools::ToolCall)>,
    ) {
        self.tool_calls.extend_indexed(partial_calls);
    }

    pub(super) fn record_usage(&mut self, output_tokens: u64, thinking_tokens: u64) {
        let provider_usage = self.provider_usage.unwrap_or_default();
        if provider_usage.output_tokens.is_none() {
            self.output_tokens = output_tokens;
        }
        if provider_usage.reasoning_tokens.is_none() {
            self.thinking_tokens = thinking_tokens;
        }
    }

    pub(super) fn record_cache(&mut self, creation: u64, read: u64, input: u64) {
        self.cache_creation_input_tokens =
            self.cache_creation_input_tokens.saturating_add(creation);
        self.cache_read_input_tokens = self.cache_read_input_tokens.saturating_add(read);
        // input_tokens is the (fixed) fresh prompt size, reported once in
        // message_start. Take the max rather than accumulating so a delta that
        // echoes it does not double-count.
        self.input_tokens = self.input_tokens.max(input);
        self.normalize_provider_input_compatibility_view();
    }

    /// Record an authoritative provider usage snapshot.
    ///
    /// Snapshot values replace previously observed values rather than being
    /// summed. OpenAI-compatible terminal usage objects are cumulative for the
    /// request, so addition would double-count if an upstream repeats a final
    /// frame. Omitted fields leave existing state untouched; an explicit zero
    /// remains authoritative.
    pub(super) fn record_provider_usage(
        &mut self,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        reasoning_tokens: Option<u64>,
        cache_creation_input_tokens: Option<u64>,
        cache_read_input_tokens: Option<u64>,
    ) {
        {
            let snapshot = self.provider_usage.get_or_insert_default();
            if let Some(input_tokens) = input_tokens {
                snapshot.input_tokens = Some(input_tokens);
            }
            if let Some(output_tokens) = output_tokens {
                snapshot.output_tokens = Some(output_tokens);
            }
            if let Some(reasoning_tokens) = reasoning_tokens {
                snapshot.reasoning_tokens = Some(reasoning_tokens);
            }
            if let Some(cache_creation_input_tokens) = cache_creation_input_tokens {
                snapshot.cache_creation_input_tokens = Some(cache_creation_input_tokens);
            }
            if let Some(cache_read_input_tokens) = cache_read_input_tokens {
                snapshot.cache_read_input_tokens = Some(cache_read_input_tokens);
            }
        }

        if let Some(output_tokens) = output_tokens {
            self.output_tokens = output_tokens;
        }
        if let Some(reasoning_tokens) = reasoning_tokens {
            self.thinking_tokens = reasoning_tokens;
        }
        if let Some(cache_creation_input_tokens) = cache_creation_input_tokens {
            self.cache_creation_input_tokens = cache_creation_input_tokens;
        }
        if let Some(cache_read_input_tokens) = cache_read_input_tokens {
            self.cache_read_input_tokens = cache_read_input_tokens;
        }
        self.normalize_provider_input_compatibility_view();
    }

    /// Normalize the historical flat counters without mutating the raw provider
    /// snapshot.
    ///
    /// Cache counts must form a subset of the authoritative prompt total so
    /// legacy analytics can continue using
    /// `fresh + cache_read + cache_creation`. Cache-read is retained first
    /// because it carries the cache-hit/badge semantics; cache-creation is
    /// clamped to the remaining total, and fresh input receives the remainder.
    /// This also handles malformed `cache > input` reports without inflating
    /// the normalized prompt beyond the provider total.
    fn normalize_provider_input_compatibility_view(&mut self) {
        let Some(snapshot) = self.provider_usage else {
            return;
        };

        // Provider cache fields are independently authoritative even when the
        // provider omitted its input total. Restore them before considering
        // legacy cache deltas so arrival order cannot turn one cumulative
        // provider value into an accidental sum.
        if let Some(cache_read_input_tokens) = snapshot.cache_read_input_tokens {
            self.cache_read_input_tokens = cache_read_input_tokens;
        }
        if let Some(cache_creation_input_tokens) = snapshot.cache_creation_input_tokens {
            self.cache_creation_input_tokens = cache_creation_input_tokens;
        }

        let Some(total_input_tokens) = snapshot.input_tokens else {
            return;
        };

        let raw_cache_read = snapshot
            .cache_read_input_tokens
            .unwrap_or(self.cache_read_input_tokens);
        let normalized_cache_read = raw_cache_read.min(total_input_tokens);
        let after_cache_read = total_input_tokens.saturating_sub(normalized_cache_read);

        let raw_cache_creation = snapshot
            .cache_creation_input_tokens
            .unwrap_or(self.cache_creation_input_tokens);
        let normalized_cache_creation = raw_cache_creation.min(after_cache_read);

        self.cache_read_input_tokens = normalized_cache_read;
        self.cache_creation_input_tokens = normalized_cache_creation;
        self.input_tokens = after_cache_read.saturating_sub(normalized_cache_creation);
    }

    pub(super) fn into_output(self) -> StreamHandlingOutput {
        StreamHandlingOutput {
            response_id: self.response_id,
            content: self.content,
            reasoning_content: self.reasoning_content,
            reasoning_signature: self.reasoning_signature,
            token_count: self.token_count,
            tool_calls: self.tool_calls.finalize(),
            output_tokens: self.output_tokens,
            thinking_tokens: self.thinking_tokens,
            cache_creation_input_tokens: self.cache_creation_input_tokens,
            cache_read_input_tokens: self.cache_read_input_tokens,
            provider_usage: self.provider_usage,
            input_tokens: self.input_tokens,
        }
    }

    pub(super) fn into_interrupted_output(self) -> InterruptedStreamOutput {
        InterruptedStreamOutput {
            content: self.content,
            reasoning_content: self.reasoning_content,
            partial_tool_calls: self
                .tool_calls
                .parts()
                .iter()
                .map(PartialToolCallSnapshot::from)
                .collect(),
        }
    }
}
