use thiserror::Error;

pub use crate::agent::llm::provider::LLMError;

#[derive(Debug, Error)]
#[error("proxy_auth_required")]
pub struct ProxyAuthRequiredError;
