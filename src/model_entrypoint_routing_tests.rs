//! Real SDK entrypoints, synthetic clients and loopback pixels only.
use std::sync::{Arc, Mutex};

use bamboo_agent_core::{AgentEvent, Message, Session, ToolSchema};
use bamboo_engine::config::GoldConfig;
use bamboo_engine::{AuxiliaryModelConfig, ExecuteRequestBuilder};
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMRequestOptions, LLMStream};
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

pub(crate) const PIXEL: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=";

#[derive(Default)]
pub(crate) struct Capture {
    pub(crate) calls: Mutex<Vec<(String, String, bool)>>,
}

impl Capture {
    fn respond(
        &self,
        messages: &[Message],
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        let purpose = options
            .and_then(|o| o.request_purpose.as_deref())
            .unwrap_or("primary");
        let images = messages.iter().any(|m| {
            m.content_parts.as_ref().is_some_and(|parts| {
                parts
                    .iter()
                    .any(|p| matches!(p, bamboo_domain::MessagePart::ImageUrl { .. }))
            })
        });
        self.calls
            .lock()
            .unwrap()
            .push((model.into(), purpose.into(), images));
        if purpose == "gold_evaluation" {
            return Err(LLMError::Auth(
                "synthetic Gold refusal; never retry another account".into(),
            ));
        }
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(LLMChunk::Token("Synthetic completion".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[async_trait::async_trait]
impl LLMProvider for Capture {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.respond(messages, model, None)
    }
    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        self.respond(messages, model, options)
    }
}

pub(crate) fn config(flag: bool, fast_provider: &str) -> Config {
    let mut c = Config::default();
    c.provider = "openai".into();
    c.default_provider_instance = Some("chat-a".into());
    c.features.provider_model_ref = flag;
    *c.memory_mut() = Some(
        serde_json::from_value(serde_json::json!({
            "auto_dream_enabled":false,"project_prompt_injection":false,"relevant_recall":false,
            "relevant_recall_rerank":false,"project_first_dream":false,"ledger_agenda":false,
        }))
        .unwrap(),
    );
    c.defaults = Some(
        serde_json::from_value(serde_json::json!({
            "chat":{"provider":"chat-a","model":"main-a"},
            "fast":{"provider":fast_provider,"model":"cheap"},
        }))
        .unwrap(),
    );
    c.provider_instances.insert("chat-a".into(), serde_json::from_value(serde_json::json!({
        "provider_type":"openai","enabled":true,"model":"main-a","runtime_models":["main-a","cheap"],
    })).unwrap());
    c
}

async fn agent(c: Config, main: Arc<Capture>, dir: &std::path::Path) -> bamboo_sdk::Agent {
    bamboo_sdk::Agent::builder()
        .provider(main)
        .model("main-a")
        .config(Arc::new(RwLock::new(c)))
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(dir.to_path_buf())
        .await
        .unwrap()
        .build()
        .unwrap()
}

async fn execute_gold(
    c: Config,
    override_target: Option<Arc<Capture>>,
) -> Vec<(String, String, bool)> {
    let dir = tempfile::tempdir().unwrap();
    let main = Arc::new(Capture::default());
    let agent = agent(c, main.clone(), dir.path()).await;
    let mut session = Session::new("synthetic-sdk-routing", "main-a");
    session.add_message(Message::user("Synthetic goal routing witness"));
    let (tx, mut rx) = mpsc::channel::<AgentEvent>(256);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let mut builder = ExecuteRequestBuilder::new(
        "Synthetic goal routing witness",
        tx,
        CancellationToken::new(),
    )
    .model("main-a")
    .provider_name("chat-a");
    if let Some(target) = override_target {
        builder = builder.auxiliary_model_resolver(Arc::new(move || AuxiliaryModelConfig {
            fast_model_name: Some("cheap".into()),
            fast_model_provider: Some(target.clone()),
            ..Default::default()
        }));
    }
    let mut request = builder.build();
    request.gold_config = Some(GoldConfig {
        enabled: true,
        goal: Some("Synthetic goal".into()),
        ..Default::default()
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        agent.execute(&mut session, request),
    )
    .await
    .expect("synthetic SDK consumer must finish")
    .unwrap();
    drain.abort();
    let result = main.calls.lock().unwrap().clone();
    assert!(result
        .iter()
        .any(|(_, purpose, _)| purpose != "gold_evaluation"));
    result
}

#[tokio::test]
async fn entrypoint_routing_sdk_rejects_unknown_fast_without_primary_account_dispatch_flag_off() {
    let calls = execute_gold(config(false, "missing-b"), None).await;
    assert!(
        calls
            .iter()
            .all(|(_, purpose, _)| purpose != "gold_evaluation"),
        "explicit rejected Fast is not an unset role: {calls:?}"
    );
}

#[tokio::test]
async fn entrypoint_routing_sdk_rejects_unknown_fast_without_primary_account_dispatch_flag_on() {
    let calls = execute_gold(config(true, "missing-b"), None).await;
    assert!(
        calls
            .iter()
            .all(|(_, purpose, _)| purpose != "gold_evaluation"),
        "configured model must never be sent to another account: {calls:?}"
    );
}

#[tokio::test]
async fn entrypoint_routing_sdk_same_account_saved_fast_is_independent_of_chat_flag() {
    let calls = execute_gold(config(false, "chat-a"), None).await;
    assert_eq!(
        calls
            .iter()
            .filter(|(_, purpose, _)| purpose == "gold_evaluation")
            .map(|(model, _, _)| model.as_str())
            .collect::<Vec<_>>(),
        vec!["cheap"]
    );
}

#[tokio::test]
async fn entrypoint_routing_sdk_explicit_host_resolver_keeps_cross_provider_client() {
    let target = Arc::new(Capture::default());
    let calls = execute_gold(config(true, "missing-b"), Some(target.clone())).await;
    assert!(calls
        .iter()
        .all(|(_, purpose, _)| purpose != "gold_evaluation"));
    assert_eq!(
        target
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(model, purpose, _)| (model.as_str(), purpose.as_str()))
            .collect::<Vec<_>>(),
        vec![("cheap", "gold_evaluation")]
    );
}

#[tokio::test]
async fn entrypoint_routing_legacy_image_transform_keeps_configured_vision_account() {
    use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    let sse = "data: {\"id\":\"synthetic\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"vision-b\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Synthetic pixel\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n";
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(sse, "text/event-stream"))
        .mount(&server)
        .await;
    let mut c = config(true, "chat-a");
    c.hooks.image_fallback.enabled = true;
    c.hooks.image_fallback.mode = "vision".into();
    c.defaults.as_mut().unwrap().vision =
        Some(bamboo_domain::ProviderModelRef::new("vision-b", "vision-b"));
    c.provider_instances.insert("vision-b".into(), serde_json::from_value(serde_json::json!({
        "provider_type":"openai","enabled":true,"api_key":"synthetic-no-real-key","base_url":server.uri(),
        "model":"vision-b","runtime_models":["vision-b"],"model_capabilities":{"vision-b":{"supports_vision":true}},
    })).unwrap());
    let fallback = bamboo_engine::model_config_helper::resolve_image_fallback(&c)
        .unwrap()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let main = Arc::new(Capture::default());
    let agent = agent(c, main.clone(), dir.path()).await;
    let mut session = Session::new("synthetic-legacy-vision", "main-a");
    let user = Message::user_with_parts(
        "Inspect synthetic pixel",
        vec![bamboo_domain::MessagePart::ImageUrl {
            image_url: bamboo_domain::ImageUrlRef {
                url: PIXEL.into(),
                detail: None,
            },
        }],
    );
    let original = user.clone();
    session.add_message(user);
    let (tx, mut rx) = mpsc::channel(256);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let request =
        ExecuteRequestBuilder::new("Inspect synthetic pixel", tx, CancellationToken::new())
            .model("main-a")
            .provider_name("chat-a")
            .image_fallback(fallback)
            .build();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        agent.execute(&mut session, request),
    )
    .await
    .unwrap()
    .unwrap();
    drain.abort();
    let calls = main.calls.lock().unwrap().clone();
    assert!(
        calls.iter().all(|(_, _, images)| !images),
        "legacy conversion dispatched pixels to the primary account: {calls:?}"
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].body_json::<serde_json::Value>().unwrap()["model"],
        "vision-b"
    );
    assert_eq!(
        session
            .messages
            .iter()
            .find(|m| m.id == original.id)
            .unwrap()
            .content_parts,
        original.content_parts
    );
}
