//! Protocol conversion traits and types.
//!
//! This module defines the hub-and-spoke conversion architecture where all provider-specific
//! types convert to/from internal types (agent_core).
//!
//! # Architecture
//!
//! ```text
//! Provider Types (OpenAI, Anthropic, etc.)
//!     ↕
//! Internal Types (bamboo_domain_session::Message, ToolSchema)
//! ```

mod anthropic;
mod errors;
pub mod gemini;
mod openai;

pub use anthropic::AnthropicProtocol;
pub use errors::{ProtocolError, ProtocolResult};
pub use gemini::GeminiProtocol;
pub use openai::OpenAIProtocol;

use bamboo_domain_session::Message;

/// Trait for converting provider-specific types to internal types.
///
/// This is the "spoke → hub" conversion.
pub trait FromProvider<T>: Sized {
    /// Convert from provider-specific type to internal type.
    fn from_provider(value: T) -> ProtocolResult<Self>;
}

/// Trait for converting internal types to provider-specific types.
///
/// This is the "hub → spoke" conversion.
pub trait ToProvider<T>: Sized {
    /// Convert from internal type to provider-specific type.
    fn to_provider(&self) -> ProtocolResult<T>;
}

/// Batch conversion for multiple messages.
pub trait FromProviderBatch<T>: Sized {
    fn from_provider_batch(values: Vec<T>) -> ProtocolResult<Vec<Self>>;
}

/// Batch conversion for multiple messages.
pub trait ToProviderBatch<T>: Sized {
    fn to_provider_batch(&self) -> ProtocolResult<Vec<T>>;
}

// Implement batch conversion for specific types

impl FromProviderBatch<crate::api::models::ChatMessage> for Message {
    fn from_provider_batch(
        values: Vec<crate::api::models::ChatMessage>,
    ) -> ProtocolResult<Vec<Self>> {
        values.into_iter().map(Self::from_provider).collect()
    }
}

impl ToProviderBatch<crate::api::models::ChatMessage> for Vec<Message> {
    fn to_provider_batch(
        &self,
    ) -> ProtocolResult<Vec<crate::api::models::ChatMessage>> {
        self.iter().map(|msg| msg.to_provider()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trait_bounds() {
        // This test just verifies the trait design compiles
        fn assert_from_provider<T, U>()
        where
            T: FromProvider<U>,
        {
        }

        fn assert_to_provider<T, U>()
        where
            T: ToProvider<U>,
        {
        }

        // Ensure at least one concrete spoke type satisfies the bounds.
        assert_from_provider::<Message, crate::api::models::ChatMessage>();
        assert_to_provider::<Message, crate::api::models::ChatMessage>();
    }
}
