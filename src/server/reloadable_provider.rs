use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::agent::core::{tools::ToolSchema, Message};
use crate::agent::llm::provider::{LLMProvider, Result};
use crate::agent::llm::LLMStream;

/// An `LLMProvider` wrapper that always delegates to the latest provider stored in a shared lock.
///
/// This prevents stale provider snapshots after runtime config changes (provider/model/proxy),
/// while keeping the call sites ergonomic (`Arc<dyn LLMProvider>`).
pub struct ReloadableProvider {
    inner: Arc<RwLock<Arc<dyn LLMProvider>>>,
}

impl ReloadableProvider {
    pub fn new(inner: Arc<RwLock<Arc<dyn LLMProvider>>>) -> Self {
        Self { inner }
    }

    async fn current(&self) -> Arc<dyn LLMProvider> {
        self.inner.read().await.clone()
    }
}

#[async_trait]
impl LLMProvider for ReloadableProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream> {
        let provider = self.current().await;
        provider
            .chat_stream(messages, tools, max_output_tokens, model)
            .await
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        let provider = self.current().await;
        provider.list_models().await
    }
}
