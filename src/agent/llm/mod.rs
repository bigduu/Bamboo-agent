pub mod error;
pub mod models;
pub mod protocol;
pub mod provider;
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

pub mod provider_factory;

pub use chat_core::Config;
pub use error::ProxyAuthRequiredError;
pub use models::*;
pub use protocol::{AnthropicProtocol, GeminiProtocol, OpenAIProtocol, FromProvider, ToProvider, ProtocolError, ProtocolResult};
pub use provider::{LLMError, LLMProvider, LLMStream};
pub use provider_factory::{create_provider, create_provider_with_dir, validate_provider_config, AVAILABLE_PROVIDERS};
pub use providers::{AnthropicProvider, CopilotProvider, GeminiProvider, OpenAIProvider};
pub use types::LLMChunk;
