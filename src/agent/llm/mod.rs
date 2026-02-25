//! LLM provider abstraction and integration for Bamboo.
//!
//! This module provides a unified interface for working with multiple LLM providers,
//! including Anthropic, OpenAI, Google Gemini, and GitHub Copilot.
//!
//! # Architecture
//!
//! The LLM system is organized into several layers:
//!
//! - **provider**: Core trait definition for LLM providers
//! - **providers**: Concrete implementations for each LLM service
//! - **protocol**: Request/response protocol adapters
//! - **models**: Common data models for LLM interactions
//! - **types**: Supporting types and utilities
//!
//! # Supported Providers
//!
//! - **Anthropic**: Claude models via Anthropic API
//! - **OpenAI**: GPT models via OpenAI API
//! - **Gemini**: Google's Gemini models via Vertex AI
//! - **Copilot**: GitHub Copilot integration
//!
//! # Key Concepts
//!
//! ## Provider Trait
//!
//! All LLM providers implement the [`LLMProvider`] trait, which defines:
//!
//! - Stream-based completion generation
//! - Model capability queries
//! - Configuration validation
//!
//! ## Protocol Adapters
//!
//! The protocol system handles differences between provider APIs:
//!
//! - [`AnthropicProtocol`]: Anthropic-specific request/response format
//! - [`OpenAIProtocol`]: OpenAI-compatible format
//! - [`GeminiProtocol`]: Google Gemini format
//!
//! ## Factory Functions
//!
//! Use [`create_provider`] to instantiate providers from configuration:
//!
//! ```no_run
//! use bamboo_agent::llm::create_provider;
//! use bamboo_agent::Config;
//!
//! // Typically you would load from disk; for docs we use a default config.
//! let config = Config::default();
//! let _provider = create_provider(&config);
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use bamboo_agent::llm::{create_provider, LLMProvider};
//! use bamboo_agent::Config;
//! use futures::StreamExt;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = Config::default();
//! let provider = create_provider(&config)?;
//!
//! let messages = vec![]; // build provider-specific messages
//! let mut stream = provider.complete(messages).await?;
//! while let Some(chunk) = stream.next().await {
//!     println!("{:?}", chunk?);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Re-exports
//!
//! Core types and functions:
//!
//! - Provider trait: [`LLMProvider`], [`LLMStream`], [`LLMError`]
//! - Protocol types: [`AnthropicProtocol`], [`OpenAIProtocol`], [`GeminiProtocol`]
//! - Factory functions: [`create_provider`], [`validate_provider_config`]
//! - Concrete providers: [`AnthropicProvider`], [`OpenAIProvider`], [`GeminiProvider`], [`CopilotProvider`]

pub mod error;
pub mod models;
pub mod protocol;
pub mod provider;
pub mod providers;
pub mod types;

pub mod api {
    pub mod models {
        pub use crate::agent::llm::models::*;
    }

    pub mod stream_tool_accumulator {
        pub use crate::agent::llm::providers::common::stream_tool_accumulator::*;
    }
}

pub mod provider_factory;

pub use crate::core::Config;
pub use error::ProxyAuthRequiredError;
pub use models::*;
pub use protocol::{
    AnthropicProtocol, FromProvider, GeminiProtocol, OpenAIProtocol, ProtocolError, ProtocolResult,
    ToProvider,
};
pub use provider::{LLMError, LLMProvider, LLMStream};
pub use provider_factory::{
    create_provider, create_provider_with_dir, validate_provider_config, AVAILABLE_PROVIDERS,
};
pub use providers::{AnthropicProvider, CopilotProvider, GeminiProvider, OpenAIProvider};
pub use types::LLMChunk;
