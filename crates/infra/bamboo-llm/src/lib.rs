//! LLM provider abstraction and integration for Bamboo.

pub mod cache;
pub mod error;
pub mod http_client;
pub mod model_catalog;
pub mod models;
pub mod prompt_ir;
pub mod protocol;
pub mod provider;
pub mod provider_factory;
pub mod provider_registry;
pub mod providers;
pub mod resolved_model;
pub mod router;
pub mod types;

pub mod api {
    pub mod models {
        pub use crate::models::*;
    }

    pub mod stream_tool_accumulator {
        pub use crate::providers::common::stream_tool_accumulator::*;
    }
}

pub use bamboo_config::Config;
pub use cache::{CacheTtl, PromptCachePlan};
pub use error::ProxyAuthRequiredError;
pub use model_catalog::ModelCatalogService;
pub use models::*;
pub use prompt_ir::{Continuation, PromptIR, Segment, SegmentRole};
pub use protocol::{
    AnthropicProtocol, FromProvider, GeminiProtocol, OpenAIProtocol, ProtocolError, ProtocolResult,
    ToProvider,
};
pub use provider::{LLMError, LLMProvider, LLMRequestOptions, LLMStream, PromptLanes};
pub use provider_factory::{
    create_provider, create_provider_by_name, create_provider_with_dir, validate_provider_config,
    AVAILABLE_PROVIDERS,
};
pub use provider_registry::ProviderRegistry;
pub use providers::{AnthropicProvider, CopilotProvider, GeminiProvider, OpenAIProvider};
pub use resolved_model::ResolvedModel;
pub use router::ProviderModelRouter;
pub use types::LLMChunk;
