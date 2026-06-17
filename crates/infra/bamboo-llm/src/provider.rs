//! LLM provider trait and types
//!
//! This module defines the interface for LLM (Large Language Model) providers,
//! enabling support for multiple LLM backends through a common trait.

use crate::prompt_ir::PromptIR;
use crate::types::LLMChunk;
use async_trait::async_trait;
use bamboo_domain::Message;
use bamboo_domain::ReasoningEffort;
use bamboo_domain::ToolSchema;
use futures::Stream;
use std::pin::Pin;
use thiserror::Error;

/// Errors that can occur when working with LLM providers
#[derive(Error, Debug)]
pub enum LLMError {
    /// HTTP request/response errors
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON serialization/deserialization errors
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Streaming response errors
    #[error("Stream error: {0}")]
    Stream(String),

    /// LLM API errors (rate limits, invalid requests, etc.)
    #[error("API error: {0}")]
    Api(String),

    /// Authentication/authorization errors
    #[error("Authentication error: {0}")]
    Auth(String),

    /// Protocol conversion errors
    #[error("Protocol conversion error: {0}")]
    Protocol(#[from] crate::protocol::ProtocolError),
}

/// Convenient result type for LLM operations
pub type Result<T> = std::result::Result<T, LLMError>;

/// Type alias for boxed streaming LLM responses
pub type LLMStream = Pin<Box<dyn Stream<Item = Result<LLMChunk>> + Send>>;

/// Metadata for a provider model returned by `list_model_info`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderModelInfo {
    /// Model identifier.
    pub id: String,
    /// Maximum context window (input + output) in tokens when known.
    pub max_context_tokens: Option<u32>,
    /// Maximum output/completion tokens when known.
    pub max_output_tokens: Option<u32>,
}

impl ProviderModelInfo {
    /// Create metadata with only model id (no token limits).
    pub fn from_id(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            max_context_tokens: None,
            max_output_tokens: None,
        }
    }
}

/// Optional request-time controls for provider calls.
#[derive(Debug, Clone, Default)]
pub struct ResponsesRequestOptions {
    /// Optional top-level instructions for Responses API requests.
    pub instructions: Option<String>,
    /// Optional message list to serialize into the Responses API `input` array.
    ///
    /// When omitted, providers fall back to the generic `messages` slice passed
    /// to `chat_stream_with_options`. This lets the engine provide a
    /// Responses-specific input view (for example, without a duplicated stable
    /// system message) while preserving backward compatibility for non-Responses
    /// callers and providers.
    pub input_messages: Option<Vec<Message>>,
    /// Optional reasoning summary control for Responses API requests
    /// (e.g. "auto", "concise", "detailed").
    pub reasoning_summary: Option<String>,
    /// Optional include list for Responses API requests.
    pub include: Option<Vec<String>>,
    /// Whether Responses API should store the response server-side.
    pub store: Option<bool>,
    /// Optional continuation handle for stateful Responses API turns.
    pub previous_response_id: Option<String>,
    /// Optional truncation mode for Responses API requests
    /// (e.g. "auto", "disabled").
    pub truncation: Option<String>,
    /// Optional text verbosity for Responses API requests
    /// (e.g. "low", "medium", "high").
    pub text_verbosity: Option<String>,
}

/// Optional request-time controls for provider calls.
#[derive(Debug, Clone, Default)]
pub struct LLMRequestOptions {
    /// Session identifier used for request-scoped logging correlation.
    pub session_id: Option<String>,
    /// Override reasoning effort for this request.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Request provider-side parallel tool call planning when supported.
    ///
    /// - OpenAI/Copilot: maps to `parallel_tool_calls`
    /// - Anthropic: maps to `tool_choice.disable_parallel_tool_use` (inverse)
    pub parallel_tool_calls: Option<bool>,
    /// Responses API specific overrides.
    pub responses: Option<ResponsesRequestOptions>,
    /// Purpose of this request for observability (e.g., "agent_loop", "task_evaluation").
    pub request_purpose: Option<String>,
    /// Provider-agnostic prompt-cache plan describing the stable, cacheable
    /// prefix of this request. Providers render it in their own dialect
    /// (Anthropic `cache_control` breakpoints; OpenAI/Gemini rely on the stable
    /// prefix automatically). `None` means "no explicit cache hints".
    pub cache: Option<crate::cache::PromptCachePlan>,
}

/// Trait for LLM provider implementations
///
/// This trait defines the interface that all LLM providers must implement
/// to work with Bamboo's agent system. Providers handle communication with
/// specific LLM services (OpenAI, Anthropic, local models, etc.).
///
/// # Design Principle
///
/// The `model` parameter is **required** in `chat_stream`, not optional.
/// This ensures that the calling code explicitly specifies which model to use,
/// preventing accidental use of unintended models and making model selection
/// explicit and auditable.
///
/// # Example
///
/// ```ignore
/// use bamboo_agent::agent::llm::provider::LLMProvider;
///
/// async fn use_provider(provider: &dyn LLMProvider) {
///     let stream = provider.chat_stream(
///         &messages,
///         &tools,
///         Some(4096),
///         "claude-sonnet-4-6", // Model is required
///     ).await?;
/// }
/// ```
#[async_trait]
pub trait LLMProvider: Send + Sync {
    /// Stream chat completion from the LLM
    ///
    /// This is the primary method for interacting with LLMs, returning
    /// a stream of response chunks that can be processed incrementally.
    ///
    /// # Arguments
    ///
    /// * `messages` - Conversation history and current prompt
    /// * `tools` - Available tools the LLM can call
    /// * `max_output_tokens` - Optional limit on response length
    /// * `model` - **Required** model identifier (e.g., "claude-sonnet-4-6")
    ///
    /// # Returns
    ///
    /// A stream of `LLMChunk` items containing partial responses
    ///
    /// # Errors
    ///
    /// Returns `LLMError` on network failures, API errors, or invalid requests
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream>;

    /// Stream chat completion with optional request-level controls.
    ///
    /// Default implementation preserves backward compatibility by delegating to
    /// [`LLMProvider::chat_stream`].
    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        _options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        self.chat_stream(messages, tools, max_output_tokens, model)
            .await
    }

    /// Stream from the canonical [`PromptIR`] — the single, rich, provider-agnostic
    /// request the engine emits once per round.
    ///
    /// A provider renders the IR into its own wire format by calling the lowering
    /// methods ([`PromptIR::system_field`], [`PromptIR::body_chat`],
    /// [`PromptIR::responses_input`], [`PromptIR::continuation_delta`]). The IR
    /// carries the stateful Responses continuation, so an adapter derives the
    /// delta itself rather than the engine pre-baking it.
    ///
    /// The default implementation lowers the IR for BOTH wire families and
    /// delegates to [`chat_stream_with_options`](Self::chat_stream_with_options):
    /// - the flat message list (`continuation_delta` mid-tool-loop, else `flatten`)
    ///   for the Chat-Completions path;
    /// - the Responses-API view (`instructions` / `input_messages` /
    ///   `previous_response_id`) derived via [`PromptIR::responses_request_options`]
    ///   and merged onto the request POLICY, so a Responses provider works WITHOUT
    ///   overriding this method (Chat-Completions providers ignore those options).
    ///
    /// This is byte-identical to the pre-IR request. Block-native providers (e.g.
    /// Anthropic) still override this to consume `system_blocks` structurally.
    async fn chat_stream_ir(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream> {
        let messages = if ir.continuation.is_some() {
            ir.continuation_delta()
        } else {
            ir.flatten()
        };
        let mut effective_options = options.cloned().unwrap_or_default();
        effective_options.responses =
            Some(ir.responses_request_options(effective_options.responses.as_ref()));
        self.chat_stream_with_options(
            &messages,
            tools,
            max_output_tokens,
            model,
            Some(&effective_options),
        )
        .await
    }

    /// Lists available models from this provider
    ///
    /// Returns a list of model identifiers that can be used with `chat_stream`.
    /// Default implementation returns an empty list.
    async fn list_models(&self) -> Result<Vec<String>> {
        // Default implementation returns empty list
        Ok(vec![])
    }

    /// Lists available models with optional token limit metadata.
    ///
    /// Default implementation preserves backward compatibility by adapting
    /// `list_models()` output into metadata entries without limits.
    async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>> {
        Ok(self
            .list_models()
            .await?
            .into_iter()
            .map(ProviderModelInfo::from_id)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::{stream, StreamExt};

    use super::*;

    #[tokio::test]
    async fn chat_stream_ir_default_flattens_and_delegates() {
        use crate::prompt_ir::{PromptIR, Segment, SegmentRole};

        // A provider that captures the message list AND the options it is handed.
        #[derive(Default)]
        struct Capture {
            seen: Arc<Mutex<Vec<Message>>>,
            seen_responses: Arc<Mutex<Option<crate::provider::ResponsesRequestOptions>>>,
        }
        #[async_trait]
        impl LLMProvider for Capture {
            async fn chat_stream(
                &self,
                _m: &[Message],
                _t: &[ToolSchema],
                _mt: Option<u32>,
                _model: &str,
            ) -> Result<LLMStream> {
                unreachable!("default chat_stream_ir must route via chat_stream_with_options")
            }
            async fn chat_stream_with_options(
                &self,
                messages: &[Message],
                _t: &[ToolSchema],
                _mt: Option<u32>,
                _model: &str,
                o: Option<&LLMRequestOptions>,
            ) -> Result<LLMStream> {
                *self.seen.lock().expect("seen lock") = messages.to_vec();
                *self.seen_responses.lock().expect("resp lock") =
                    o.and_then(|value| value.responses.clone());
                Ok(Box::pin(stream::iter(Vec::<Result<LLMChunk>>::new())))
            }
        }

        let cap = Capture::default();
        let ir = PromptIR {
            system_text: "sys".into(),
            segments: vec![
                Segment::new(SegmentRole::StablePrefix, vec![Message::user("guide")]),
                Segment::new(SegmentRole::DynamicContext, vec![Message::user("dyn")]),
                Segment::new(SegmentRole::Conversation, vec![Message::user("ask")]),
            ],
            ..PromptIR::default()
        };
        let _ = cap
            .chat_stream_ir(&ir, &[], None, "m", None)
            .await
            .expect("ir stream");

        let seen = cap.seen.lock().expect("seen lock").clone();
        let expected = ir.flatten();
        assert_eq!(seen.len(), expected.len(), "delegates the flattened IR");
        for (got, want) in seen.iter().zip(expected.iter()) {
            assert_eq!(got.role, want.role);
            assert_eq!(got.content, want.content);
        }
        // system + guide + dyn + ask
        assert_eq!(seen.len(), 4);
        assert!(matches!(seen[0].role, bamboo_domain::Role::System));

        // SAFETY NET: the default also derives the Responses-API view from the IR, so
        // a Responses provider works without overriding `chat_stream_ir`. instructions
        // = the (trimmed) system field; input_messages = the full responses_input view
        // (system lifted out, so it does not lead with a system message).
        let responses = cap
            .seen_responses
            .lock()
            .expect("resp lock")
            .clone()
            .expect("default derives Responses options from the IR");
        assert_eq!(responses.instructions.as_deref(), Some("sys"));
        let input = responses.input_messages.expect("input_messages derived");
        assert_eq!(
            input.iter().map(|m| m.content.clone()).collect::<Vec<_>>(),
            vec!["guide".to_string(), "dyn".to_string(), "ask".to_string()],
            "input_messages is the responses_input view: NO leading system message"
        );
    }

    #[derive(Clone, Default)]
    struct RecordingProvider {
        requested_models: Arc<Mutex<Vec<String>>>,
        requested_max_tokens: Arc<Mutex<Vec<Option<u32>>>>,
    }

    #[async_trait]
    impl LLMProvider for RecordingProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            max_output_tokens: Option<u32>,
            model: &str,
        ) -> Result<LLMStream> {
            if let Ok(mut models) = self.requested_models.lock() {
                models.push(model.to_string());
            }
            if let Ok(mut max_tokens) = self.requested_max_tokens.lock() {
                max_tokens.push(max_output_tokens);
            }

            Ok(Box::pin(stream::empty()))
        }
    }

    #[tokio::test]
    async fn chat_stream_with_options_delegates_to_chat_stream_with_same_model_and_tokens() {
        let provider = RecordingProvider::default();
        let options = LLMRequestOptions::default();

        let mut stream = provider
            .chat_stream_with_options(&[], &[], Some(512), "gpt-test", Some(&options))
            .await
            .expect("delegation should succeed");
        assert!(stream.next().await.is_none());

        assert_eq!(
            provider
                .requested_models
                .lock()
                .expect("lock poisoned")
                .as_slice(),
            ["gpt-test"]
        );
        assert_eq!(
            provider
                .requested_max_tokens
                .lock()
                .expect("lock poisoned")
                .as_slice(),
            [Some(512)]
        );
    }

    #[tokio::test]
    async fn list_models_returns_empty_by_default() {
        let provider = RecordingProvider::default();
        let models = provider
            .list_models()
            .await
            .expect("default list_models should succeed");
        assert!(models.is_empty());
    }

    #[test]
    fn request_options_default_has_no_purpose() {
        let opts = LLMRequestOptions::default();
        assert!(opts.request_purpose.is_none());
    }

    #[test]
    fn request_options_purpose_is_set_and_readable() {
        let opts = LLMRequestOptions {
            request_purpose: Some("title_generation".to_string()),
            ..Default::default()
        };
        assert_eq!(opts.request_purpose.as_deref(), Some("title_generation"));
    }
}
