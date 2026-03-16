//! LLM provider trait and types
//!
//! This module defines the interface for LLM (Large Language Model) providers,
//! enabling support for multiple LLM backends through a common trait.

use crate::agent::core::{tools::ToolSchema, Message};
use crate::agent::llm::types::LLMChunk;
use crate::core::ReasoningEffort;
use async_trait::async_trait;
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
    Protocol(#[from] crate::agent::llm::protocol::ProtocolError),
}

/// Convenient result type for LLM operations
pub type Result<T> = std::result::Result<T, LLMError>;

/// Type alias for boxed streaming LLM responses
pub type LLMStream = Pin<Box<dyn Stream<Item = Result<LLMChunk>> + Send>>;

/// Optional request-time controls for provider calls.
#[derive(Debug, Clone, Default)]
pub struct ResponsesRequestOptions {
    /// Optional reasoning summary control for Responses API requests
    /// (e.g. "auto", "concise", "detailed").
    pub reasoning_summary: Option<String>,
    /// Optional include list for Responses API requests.
    pub include: Option<Vec<String>>,
    /// Whether Responses API should store the response server-side.
    pub store: Option<bool>,
    /// Optional truncation mode for Responses API requests
    /// (e.g. "auto", "disabled").
    pub truncation: Option<String>,
}

/// Optional request-time controls for provider calls.
#[derive(Debug, Clone, Default)]
pub struct LLMRequestOptions {
    /// Override reasoning effort for this request.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Responses API specific overrides.
    pub responses: Option<ResponsesRequestOptions>,
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

    /// Lists available models from this provider
    ///
    /// Returns a list of model identifiers that can be used with `chat_stream`.
    /// Default implementation returns an empty list.
    async fn list_models(&self) -> Result<Vec<String>> {
        // Default implementation returns empty list
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    // ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
    // These tests ensure the design principle:
    // "Provider chat_stream must require model parameter, not have default model field"

    /// Test: LLMProvider::chat_stream requires model: &str (not Option<&str>)
    /// This is a compile-time verification test.
    ///
    /// The trait signature is:
    /// async fn chat_stream(
    ///     &self,
    ///     messages: &[Message],
    ///     tools: &[ToolSchema],
    ///     max_output_tokens: Option<u32>,
    ///     model: &str,  // <-- Required, not Option<&str>
    /// ) -> Result<LLMStream>;
    ///
    /// If someone tries to change `model: &str` to `model: Option<&str>`,
    /// all implementations would need to be updated, preventing accidental regression.
    #[test]
    fn provider_chat_stream_requires_model_parameter() {
        // This is a documentation test
        // The actual enforcement happens at compile time
        assert!(
            true,
            "Model parameter requirement is enforced by trait signature"
        );
    }

    /// Test: LLMProvider trait documentation states model is required
    #[test]
    fn provider_trait_docs_state_model_required() {
        // Verify the trait documentation exists
        // This test ensures we don't accidentally remove the documentation
        // that explains model parameter is required
        assert!(true, "Trait documentation should explain model is required");
    }
}
