//! E2E tests for Anthropic-compatible API endpoints.

use std::sync::{Arc, Mutex};

use actix_web::{test, web, App};
use async_trait::async_trait;
use bamboo_agent::agent::{Message, Role};
use bamboo_agent::server::handlers::anthropic;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_llm::{LLMChunk, LLMProvider, LLMStream};
use futures::stream;
use serde_json::json;

mod complete_models;
mod image_hook;
mod messages;

#[derive(Debug, Clone)]
struct RecordedChatCall {
    messages: Vec<Message>,
    model: String,
    max_output_tokens: Option<u32>,
}

#[derive(Clone, Default)]
struct RecordingProvider {
    calls: Arc<Mutex<Vec<RecordedChatCall>>>,
}

impl RecordingProvider {
    fn calls(&self) -> Vec<RecordedChatCall> {
        self.calls
            .lock()
            .expect("recording provider lock poisoned")
            .clone()
    }
}

#[async_trait]
impl LLMProvider for RecordingProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        self.calls
            .lock()
            .expect("recording provider lock poisoned")
            .push(RecordedChatCall {
                messages: messages.to_vec(),
                model: model.to_string(),
                max_output_tokens,
            });

        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::Token("ok".to_string())),
            Ok(LLMChunk::Done),
        ])))
    }

    async fn list_models(&self) -> bamboo_llm::provider::Result<Vec<String>> {
        Ok(vec!["claude-3-5-sonnet-20241022".to_string()])
    }
}

async fn create_anthropic_state(
    recording_provider: &RecordingProvider,
    image_hook_enabled: bool,
) -> actix_web::web::Data<bamboo_agent::server::AppState> {
    let state = crate::e2e::common::create_test_app().await;

    {
        let mut config = state.config.write().await;
        config.anthropic_model_mapping.mappings.insert(
            "sonnet".to_string(),
            "claude-3-5-sonnet-20241022".to_string(),
        );
        config.hooks.image_fallback.enabled = image_hook_enabled;
        config.hooks.image_fallback.mode = "placeholder".to_string();
    }

    {
        let mut provider = state.provider.write().await;
        *provider = Arc::new(recording_provider.clone());
    }

    state
}
