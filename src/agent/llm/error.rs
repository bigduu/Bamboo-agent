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
