//! Isolated A/B/C accounts for real host dispatch tests; no network providers.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bamboo_agent_core::{Message, ToolSchema};
use bamboo_llm::{
    Config, LLMChunk, LLMError, LLMProvider, LLMRequestOptions, LLMStream, ProviderRegistry,
};

#[derive(Default)]
pub(crate) struct CaptureProvider {
    pub(crate) models: Mutex<Vec<String>>,
    pub(crate) efforts: Mutex<Vec<Option<bamboo_domain::ReasoningEffort>>>,
}

#[async_trait::async_trait]
impl LLMProvider for CaptureProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.models.lock().unwrap().push(model.into());
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(LLMChunk::Token("Synthetic routing completion".into())),
            Ok(LLMChunk::Done),
        ])))
    }
    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        self.efforts
            .lock()
            .unwrap()
            .push(options.and_then(|options| options.reasoning_effort));
        self.chat_stream(messages, tools, max, model).await
    }
}

pub(crate) async fn state() -> (
    tempfile::TempDir,
    crate::AppState,
    HashMap<String, Arc<CaptureProvider>>,
) {
    let directory = tempfile::tempdir().unwrap();
    let providers: HashMap<_, _> = ["startup-a", "chat-b", "fast-c"]
        .into_iter()
        .map(|id| (id.to_string(), Arc::new(CaptureProvider::default())))
        .collect();
    let state = crate::AppState::new_with_provider(
        directory.path().to_path_buf(),
        Config::default(),
        providers["startup-a"].clone(),
    )
    .await
    .unwrap();
    let mut saved = Config::default();
    saved.provider = "startup-a".into();
    saved.default_provider_instance = Some("startup-a".into());
    saved.features.provider_model_ref = true;
    *saved.memory_mut() = Some(
        serde_json::from_value(serde_json::json!({
            "enabled":false, "auto_dream_enabled":false, "project_prompt_injection":false,
            "relevant_recall":false, "relevant_recall_rerank":false, "project_first_dream":false,
            "ledger_agenda":false,
        }))
        .unwrap(),
    );
    for (id, model) in [
        ("startup-a", "startup-model"),
        ("chat-b", "chat-model"),
        ("fast-c", "cheap-model"),
    ] {
        saved.provider_instances.insert(
            id.into(),
            serde_json::from_value(serde_json::json!({
                "provider_type":"openai", "enabled":true, "model":model, "runtime_models":[model],
            }))
            .unwrap(),
        );
    }
    saved.defaults = Some(
        serde_json::from_value(serde_json::json!({
            "chat":{"provider":"chat-b","model":"chat-model"},
            "fast":{"provider":"fast-c","model":"cheap-model"},
        }))
        .unwrap(),
    );
    *state.config.write().await = saved;
    let registry = ProviderRegistry::new(
        providers
            .iter()
            .map(|(id, provider)| (id.clone(), provider.clone() as Arc<dyn LLMProvider>))
            .collect(),
        "startup-a".into(),
    );
    registry.set_runtime_models(HashMap::from([
        ("startup-a".into(), vec!["startup-model".into()]),
        ("chat-b".into(), vec!["chat-model".into()]),
        ("fast-c".into(), vec!["cheap-model".into()]),
    ]));
    state.provider_registry.replace_with(registry);
    (directory, state, providers)
}
