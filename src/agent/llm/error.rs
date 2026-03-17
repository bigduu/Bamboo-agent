//! Error types for LLM operations.
//!
//! This module provides error types for handling LLM-related failures,
//! including authentication, network, and API errors.

use thiserror::Error;

/// Re-export of the main LLM error type.
pub use crate::agent::llm::provider::LLMError;

/// Error indicating that proxy authentication is required.
///
/// This error is returned when a proxy server requires authentication
/// credentials that were not provided.
#[derive(Debug, Error)]
#[error("proxy_auth_required")]
pub struct ProxyAuthRequiredError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_auth_required_error_creation() {
        let err = ProxyAuthRequiredError;
        assert_eq!(err.to_string(), "proxy_auth_required");
    }

    #[test]
    fn test_proxy_auth_required_error_debug() {
        let err = ProxyAuthRequiredError;
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("ProxyAuthRequiredError"));
    }

    #[test]
    fn test_proxy_auth_required_error_display() {
        let err = ProxyAuthRequiredError;
        let display_str = format!("{}", err);
        assert_eq!(display_str, "proxy_auth_required");
    }
}
