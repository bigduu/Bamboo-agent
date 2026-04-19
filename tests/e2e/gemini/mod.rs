//! E2E tests for Gemini-compatible API endpoints.

use std::sync::{Arc, Mutex};

use actix_web::{test, web, App};
use async_trait::async_trait;
use bamboo_agent::agent::{Message, Role};
use bamboo_agent::server::handlers::gemini;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_infrastructure::{LLMChunk, LLMProvider, LLMStream};
use futures::stream;
use serde_json::json;

mod generate;
mod image_hook;
mod stream_endpoints;

#[derive(Debug, Clone)]
struct RecordedChatCall {
    messages: Vec<Message>,
    model: String,
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
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> bamboo_infrastructure::provider::Result<LLMStream> {
        self.calls
            .lock()
            .expect("recording provider lock poisoned")
            .push(RecordedChatCall {
                messages: messages.to_vec(),
                model: model.to_string(),
            });

        Ok(Box::pin(stream::iter(vec![
            Ok(LLMChunk::Token("ok".to_string())),
            Ok(LLMChunk::Done),
        ])))
    }

    async fn list_models(&self) -> bamboo_infrastructure::provider::Result<Vec<String>> {
        Ok(vec!["gemini-2.0-flash-exp".to_string()])
    }
}

async fn create_gemini_state(
    recording_provider: &RecordingProvider,
    image_hook_enabled: bool,
) -> actix_web::web::Data<bamboo_agent::server::AppState> {
    let state = crate::e2e::common::create_test_app().await;

    {
        let mut config = state.config.write().await;
        config
            .gemini_model_mapping
            .mappings
            .insert("flash".to_string(), "gemini-2.0-flash-exp".to_string());
        config.hooks.image_fallback.enabled = image_hook_enabled;
        config.hooks.image_fallback.mode = "placeholder".to_string();
    }

    {
        let mut provider = state.provider.write().await;
        *provider = Arc::new(recording_provider.clone());
    }

    state
}
