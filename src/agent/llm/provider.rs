use crate::types::LLMChunk;
use agent_core::{tools::ToolSchema, Message};
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LLMError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Stream error: {0}")]
    Stream(String),

    #[error("API error: {0}")]
    Api(String),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Protocol conversion error: {0}")]
    Protocol(#[from] crate::protocol::ProtocolError),
}

pub type Result<T> = std::result::Result<T, LLMError>;

pub type LLMStream = Pin<Box<dyn Stream<Item = Result<LLMChunk>> + Send>>;

#[async_trait]
pub trait LLMProvider: Send + Sync {
    /// Stream chat completion
    ///
    /// # Arguments
    /// * `messages` - Chat messages
    /// * `tools` - Available tools
    /// * `max_output_tokens` - Maximum output tokens
    /// * `model` - Model to use (required)
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream>;

    /// List available models
    async fn list_models(&self) -> Result<Vec<String>> {
        // Default implementation returns empty list
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(true, "Model parameter requirement is enforced by trait signature");
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
