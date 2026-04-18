//! LLM provider abstraction and integration for Bamboo.

pub mod error;
pub mod http_client;
pub mod models;
pub mod protocol;
pub mod provider;
pub mod provider_factory;
pub mod providers;
pub mod types;

pub mod api {
    pub mod models {
        pub use crate::models::*;
    }

    pub mod stream_tool_accumulator {
        pub use crate::providers::common::stream_tool_accumulator::*;
    }
}

pub use bamboo_infrastructure_config::Config;
pub use error::ProxyAuthRequiredError;
pub use models::*;
pub use protocol::{
    AnthropicProtocol, FromProvider, GeminiProtocol, OpenAIProtocol, ProtocolError, ProtocolResult,
    ToProvider,
};
pub use provider::{LLMError, LLMProvider, LLMRequestOptions, LLMStream};
pub use provider_factory::{
    create_provider, create_provider_with_dir, validate_provider_config, AVAILABLE_PROVIDERS,
};
pub use providers::{AnthropicProvider, CopilotProvider, GeminiProvider, OpenAIProvider};
pub use types::LLMChunk;
