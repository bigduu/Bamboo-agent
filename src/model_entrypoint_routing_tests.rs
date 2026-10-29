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
    vision_support: Option<bool>,
    saw_omission: Mutex<bool>,
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
        if messages
            .iter()
            .any(|m| m.content.contains("omitted (vision failed)"))
        {
            *self.saw_omission.lock().unwrap() = true;
        }
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
    async fn vision_support_override(&self, _: &str) -> Option<bool> {
        self.vision_support
    }

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
            "relevant_recall_rerank":false,"project_first_dream":false,"ledger_agenda_injection":false,
        }))
        .unwrap(),
    );
    let memory = c.memory().as_ref().unwrap();
    assert!(
        !memory.project_prompt_injection
            && !memory.relevant_recall
            && !memory.relevant_recall_rerank
            && !memory.project_first_dream
            && !memory.ledger_agenda_injection,
        "routing fixtures must disable canonical memory reads"
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
        auto_continue_enabled: true,
        goal: Some("Synthetic goal".into()),
        ..Default::default()
    });
    request.app_data_dir = Some(dir.path().to_path_buf());
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
    let models = calls
        .iter()
        .filter(|(_, purpose, _)| purpose == "gold_evaluation")
        .map(|(model, _, _)| model.as_str())
        .collect::<Vec<_>>();
    assert!(
        !models.is_empty(),
        "positive control must exercise Gold dispatch"
    );
    assert!(models.iter().all(|model| *model == "cheap"), "{models:?}");
}

#[tokio::test]
async fn entrypoint_routing_sdk_explicit_host_resolver_keeps_cross_provider_client() {
    let target = Arc::new(Capture::default());
    let calls = execute_gold(config(true, "missing-b"), Some(target.clone())).await;
    assert!(calls
        .iter()
        .all(|(_, purpose, _)| purpose != "gold_evaluation"));
    let requests = target
        .calls
        .lock()
        .unwrap()
        .iter()
        .map(|(model, purpose, _)| (model.as_str(), purpose.as_str()))
        .map(|(model, purpose)| (model.to_string(), purpose.to_string()))
        .collect::<Vec<_>>();
    assert!(
        !requests.is_empty(),
        "positive control must exercise Gold dispatch"
    );
    assert!(
        requests
            .iter()
            .all(|(model, purpose)| model == "cheap" && purpose == "gold_evaluation"),
        "{requests:?}"
    );
}

#[tokio::test]
async fn entrypoint_routing_sdk_default_constructor_keeps_chat_role_account_and_model() {
    use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};
    for flag in [false, true] {
        let a = MockServer::start().await;
        let b = MockServer::start().await;
        let sse = "data: {\"id\":\"synthetic\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"synthetic\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Synthetic completion\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n";
        for server in [&a, &b] {
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(200).set_body_raw(sse, "text/event-stream"))
                .mount(server)
                .await;
        }
        let mut c = config(flag, "chat-a");
        c.defaults.as_mut().unwrap().chat =
            bamboo_domain::ProviderModelRef::new("chat-b", "main-b");
        c.provider_instances.get_mut("chat-a").unwrap().base_url = Some(a.uri());
        c.provider_instances.insert("chat-b".into(),serde_json::from_value(serde_json::json!({
            "provider_type":"openai","enabled":true,"base_url":b.uri(),"model":"main-b","runtime_models":["main-b"],
        })).unwrap());
        let mut value = serde_json::to_value(&c).unwrap();
        for id in ["chat-a", "chat-b"] {
            value["provider_instances"][id]["api_key"] = "synthetic-no-real-key".into();
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("config.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        let agent = bamboo_sdk::Agent::builder()
            .tools(std::iter::empty())
            .with_defaults_for_data_dir(dir.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        let mut session = agent.new_session("synthetic-default-sdk").unwrap();
        session.add_message(Message::user("Synthetic constructor routing witness"));
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            agent.run_session(&mut session),
        )
        .await
        .unwrap()
        .unwrap();
        let requests_a = a
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| r.body_json::<serde_json::Value>().unwrap()["model"].clone())
            .collect::<Vec<_>>();
        let requests_b = b
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| r.body_json::<serde_json::Value>().unwrap()["model"].clone())
            .collect::<Vec<_>>();
        let expected = if flag {
            (Vec::new(), vec![serde_json::json!("main-b")])
        } else {
            (vec![serde_json::json!("main-a")], Vec::new())
        };
        assert_eq!((requests_a, requests_b), expected, "flag={flag}");
    }
}

fn legacy_config(flag: bool, endpoint: &str) -> Config {
    let mut c = config(flag, "chat-a");
    c.hooks.image_fallback.enabled = true;
    c.hooks.image_fallback.mode = "vision".into();
    c.defaults.as_mut().unwrap().vision =
        Some(bamboo_domain::ProviderModelRef::new("vision-b", "vision-b"));
    c.provider_instances.insert("vision-b".into(), serde_json::from_value(serde_json::json!({
        "provider_type":"openai","enabled":true,"api_key":"synthetic-no-real-key","base_url":endpoint,
        "model":"vision-b","runtime_models":["vision-b","cheap"],
        "model_capabilities":{"vision-b":{"supports_vision":true},"cheap":{"supports_vision":true}},
    })).unwrap());
    c
}

async fn execute_legacy(
    c: Config,
    main: Arc<Capture>,
) -> Result<(), bamboo_agent_core::AgentError> {
    let fallback = bamboo_engine::model_config_helper::resolve_image_fallback(&c)
        .unwrap()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let agent = agent(c, main, dir.path()).await;
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
    let mut request =
        ExecuteRequestBuilder::new("Inspect synthetic pixel", tx, CancellationToken::new())
            .model("main-a")
            .provider_name("chat-a")
            .image_fallback(fallback)
            .build();
    request.app_data_dir = Some(dir.path().to_path_buf());
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        agent.execute(&mut session, request),
    )
    .await
    .expect("controlled legacy consumer must finish within its bounded request policy");
    drain.abort();
    assert_eq!(
        session
            .messages
            .iter()
            .find(|m| m.id == original.id)
            .unwrap()
            .content_parts,
        original.content_parts,
        "every success and failure must preserve original session pixels"
    );
    outcome
}

async fn vision_server(status: u16) -> wiremock::MockServer {
    use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    let sse = "data: {\"id\":\"synthetic\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"vision-b\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Synthetic pixel\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n";
    let response = if status == 200 {
        ResponseTemplate::new(status).set_body_raw(sse, "text/event-stream")
    } else {
        let response = ResponseTemplate::new(status)
            .set_body_json(serde_json::json!({"error":{"message":"synthetic refusal"}}));
        if matches!(status, 429 | 503) {
            // The server hint exceeds every supported finite test budget, so
            // this routing test exercises the error without a long backoff.
            response.insert_header("Retry-After", "7200")
        } else {
            response
        }
    };
    Mock::given(method("POST"))
        .respond_with(response)
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn entrypoint_routing_legacy_image_transform_keeps_configured_vision_account() {
    // Include identical model IDs on A/B: a model name cannot identify its account.
    for flag in [false, true] {
        for model in ["vision-b", "cheap"] {
            let server = vision_server(200).await;
            let mut c = legacy_config(flag, &server.uri());
            c.defaults.as_mut().unwrap().vision.as_mut().unwrap().model = model.into();
            let main = Arc::new(Capture::default());
            execute_legacy(c, main.clone()).await.unwrap();
            let calls = main.calls.lock().unwrap().clone();
            assert!(!calls.is_empty());
            assert!(calls.iter().all(|(_, _, images)| !images), "{calls:?}");
            let requests = server.received_requests().await.unwrap();
            assert!(!requests.is_empty(), "flag={flag}, model={model}");
            assert!(requests
                .iter()
                .all(|r| r.body_json::<serde_json::Value>().unwrap()["model"] == model));
        }
    }
}

#[tokio::test]
async fn entrypoint_routing_legacy_rejects_ungranted_or_invalid_targets_before_any_send() {
    for flag in [false, true] {
        for case in [
            "fast-only-cross",
            "chat-only-cross",
            "unknown",
            "disabled",
            "unadmitted",
            "unsupported",
            "same-account-unsupported",
        ] {
            let server = vision_server(200).await;
            let mut c = legacy_config(flag, &server.uri());
            let defaults = c.defaults.as_mut().unwrap();
            match case {
                "fast-only-cross" => {
                    defaults.vision = None;
                    defaults.fast =
                        Some(bamboo_domain::ProviderModelRef::new("vision-b", "vision-b"));
                }
                "chat-only-cross" => {
                    defaults.vision = None;
                    defaults.fast = None;
                    defaults.chat = bamboo_domain::ProviderModelRef::new("vision-b", "vision-b");
                }
                "unknown" => defaults.vision.as_mut().unwrap().provider = "absent-account".into(),
                "disabled" => c.provider_instances.get_mut("vision-b").unwrap().enabled = false,
                "unadmitted" => defaults.vision.as_mut().unwrap().model = "absent-model".into(),
                "unsupported" => {
                    let instance = c.provider_instances.get("vision-b").unwrap();
                    let mut value = serde_json::to_value(instance).unwrap();
                    value["model_capabilities"]["vision-b"]["supports_vision"] = false.into();
                    c.provider_instances
                        .insert("vision-b".into(), serde_json::from_value(value).unwrap());
                }
                "same-account-unsupported" => {
                    defaults.vision = Some(bamboo_domain::ProviderModelRef::new("chat-a", "cheap"));
                    let mut value =
                        serde_json::to_value(c.provider_instances.get("chat-a").unwrap()).unwrap();
                    value["model_capabilities"] =
                        serde_json::json!({"cheap":{"supports_vision":false}});
                    c.provider_instances
                        .insert("chat-a".into(), serde_json::from_value(value).unwrap());
                }
                _ => unreachable!(),
            }
            let main = Arc::new(Capture::default());
            let error = execute_legacy(c, main.clone()).await.unwrap_err();
            assert!(
                error.to_string().contains("preserved"),
                "{case}, flag={flag}: {error}"
            );
            assert!(main.calls.lock().unwrap().is_empty(), "{case}, flag={flag}");
            assert!(
                server.received_requests().await.unwrap().is_empty(),
                "{case}, flag={flag}"
            );
        }
    }
}

#[tokio::test]
async fn entrypoint_routing_legacy_native_main_bypasses_unavailable_auxiliary() {
    let server = vision_server(200).await;
    let mut c = legacy_config(false, &server.uri());
    c.provider_instances.get_mut("vision-b").unwrap().enabled = false;
    let main = Arc::new(Capture {
        vision_support: Some(true),
        ..Default::default()
    });
    execute_legacy(c, main.clone()).await.unwrap();
    let calls = main.calls.lock().unwrap().clone();
    assert!(!calls.is_empty());
    assert!(
        calls
            .iter()
            .all(|(model, _, images)| model == "main-a" && *images),
        "{calls:?}"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn entrypoint_routing_legacy_unset_vision_can_use_same_account_fast() {
    let server = vision_server(200).await;
    let mut c = legacy_config(false, &server.uri());
    c.defaults.as_mut().unwrap().vision = None;
    let main = Arc::new(Capture::default());
    execute_legacy(c, main.clone()).await.unwrap();
    let calls = main.calls.lock().unwrap().clone();
    assert!(
        calls
            .iter()
            .any(|(model, _, images)| model == "cheap" && *images),
        "{calls:?}"
    );
    assert!(
        calls
            .iter()
            .any(|(model, _, images)| model == "main-a" && !images),
        "{calls:?}"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn entrypoint_routing_legacy_auth_quota_and_server_failure_never_switch_image_accounts() {
    for status in [401, 429, 503] {
        let server = vision_server(status).await;
        let main = Arc::new(Capture::default());
        execute_legacy(legacy_config(true, &server.uri()), main.clone())
            .await
            .unwrap();
        let calls = main.calls.lock().unwrap().clone();
        assert!(!calls.is_empty(), "status={status}");
        assert!(
            calls
                .iter()
                .all(|(model, _, images)| model == "main-a" && !images),
            "status={status}: {calls:?}"
        );
        assert!(
            *main.saw_omission.lock().unwrap(),
            "legacy omission marker must remain explicit: status={status}"
        );
        let requests = server.received_requests().await.unwrap();
        assert!(!requests.is_empty(), "status={status}");
        let request_paths = requests
            .iter()
            .map(|request| (request.method.to_string(), request.url.path().to_string()))
            .collect::<Vec<_>>();
        eprintln!("synthetic status={status} request paths: {request_paths:?}");
        assert!(requests
            .iter()
            .all(|r| r.body_json::<serde_json::Value>().unwrap()["model"] == "vision-b"));
    }
}

#[tokio::test]
async fn entrypoint_routing_sdk_run_keeps_existing_session_model_without_builder_override() {
    let c = config(false, "chat-a");
    let main = Arc::new(Capture::default());
    let dir = tempfile::tempdir().unwrap();
    let agent = bamboo_sdk::Agent::builder()
        .provider(main.clone())
        .config(Arc::new(RwLock::new(c)))
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(dir.path().to_path_buf())
        .await
        .unwrap()
        .build()
        .unwrap();
    let mut session = Session::new("synthetic-existing-sdk-session", "cheap");
    session.add_message(Message::user("Synthetic existing session model witness"));
    agent.run_session(&mut session).await.unwrap();
    let calls = main.calls.lock().unwrap().clone();
    assert!(!calls.is_empty());
    assert!(
        calls.iter().all(|(model, _, _)| model == "cheap"),
        "{calls:?}"
    );
}

#[tokio::test]
async fn entrypoint_routing_sdk_injected_client_keeps_declared_account_default_in_both_orders() {
    let mut observations = Vec::new();
    for flag in [false, true] {
        for before in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut c = config(flag, "chat-a");
            c.defaults.as_mut().unwrap().chat =
                bamboo_domain::ProviderModelRef::new("chat-b", "main-b");
            c.provider_instances.insert("chat-b".into(), serde_json::from_value(serde_json::json!({
                "provider_type":"openai","enabled":true,"model":"main-b","runtime_models":["main-b"],
            })).unwrap());
            std::fs::write(
                dir.path().join("config.json"),
                serde_json::to_vec(&c).unwrap(),
            )
            .unwrap();
            let shared = Arc::new(RwLock::new(c));
            let main = Arc::new(Capture::default());
            let mut builder = bamboo_sdk::Agent::builder()
                .provider(main.clone())
                .tools(std::iter::empty());
            if before {
                builder = builder.config(shared.clone());
            }
            builder = builder
                .with_defaults_for_data_dir(dir.path().to_path_buf())
                .await
                .unwrap();
            if !before {
                builder = builder.config(shared);
            }
            let agent = builder.build().unwrap();
            let mut session = agent.new_session("synthetic-injected-client").unwrap();
            session.add_message(Message::user("Synthetic injected client witness"));
            agent.run_session(&mut session).await.unwrap();
            let calls = main.calls.lock().unwrap().clone();
            observations.push((flag, before, calls));
        }
    }
    assert!(
        observations.iter().all(|(_, _, calls)| !calls.is_empty()
            && calls.iter().all(|(model, _, _)| model == "main-a")),
        "{observations:?}"
    );
}

#[tokio::test]
async fn entrypoint_routing_sdk_native_client_rejects_replacement_invalid_chat_before_dispatch() {
    let mut observations = Vec::new();
    for case in ["cross-account", "disabled", "unadmitted"] {
        let server = vision_server(200).await;
        let mut c = config(true, "chat-a");
        let mut value = serde_json::to_value(c.provider_instances.get("chat-a").unwrap()).unwrap();
        value["api_key"] = "synthetic-no-real-key".into();
        value["base_url"] = server.uri().into();
        c.provider_instances
            .insert("chat-a".into(), serde_json::from_value(value).unwrap());
        let dir = tempfile::tempdir().unwrap();
        // Public serialization omits credentials; restore only this mock key.
        let mut persisted = serde_json::to_value(&c).unwrap();
        persisted["provider_instances"]["chat-a"]["api_key"] = "synthetic-no-real-key".into();
        std::fs::write(
            dir.path().join("config.json"),
            serde_json::to_vec(&persisted).unwrap(),
        )
        .unwrap();
        let builder = bamboo_sdk::Agent::builder()
            .tools(std::iter::empty())
            .with_defaults_for_data_dir(dir.path().to_path_buf())
            .await
            .unwrap();
        match case {
            "cross-account" => c.defaults.as_mut().unwrap().chat.provider = "chat-b".into(),
            "disabled" => c.provider_instances.get_mut("chat-a").unwrap().enabled = false,
            "unadmitted" => c.defaults.as_mut().unwrap().chat.model = "absent-model".into(),
            _ => unreachable!(),
        }
        observations.push((
            case,
            builder
                .config(Arc::new(RwLock::new(c)))
                .build()
                .err()
                .map(|e| e.to_string()),
        ));
        assert!(server.received_requests().await.unwrap().is_empty());
    }
    assert!(
        observations.iter().all(|(_, error)| error.is_some()),
        "{observations:?}"
    );
}

async fn sdk_request_with_retry_mode(
    agent: &bamboo_sdk::Agent,
    session: &mut Session,
    request: bamboo_engine::ExecuteRequest,
    retry_entry: bool,
) -> Result<(), bamboo_agent_core::AgentError> {
    if retry_entry {
        agent
            .execute_with_upstream_retry(
                session,
                request,
                bamboo_sdk::agent::UpstreamRetry::Bounded,
            )
            .await
    } else {
        agent.execute(session, request).await
    }
}

fn add_synthetic_chat_b(config: &mut Config) {
    config.provider_instances.insert(
        "chat-b".into(),
        serde_json::from_value(serde_json::json!({
            "provider_type":"openai","enabled":true,
            "model":"main-b","runtime_models":["main-b","cheap"]
        }))
        .unwrap(),
    );
}

#[tokio::test]
async fn entrypoint_routing_sdk_rejects_unpaired_account_for_both_execute_entries() {
    for retry_entry in [false, true] {
        for override_without_name in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut c = config(true, "chat-a");
            add_synthetic_chat_b(&mut c);
            let main = Arc::new(Capture::default());
            let other = Arc::new(Capture::default());
            let agent = agent(c, main.clone(), dir.path()).await;
            let mut session = Session::new("synthetic-unpaired-client", "main-b");
            session.add_message(Message::user("Synthetic account identity witness"));
            let before = serde_json::to_value(&session).unwrap();
            let (tx, mut rx) = mpsc::channel(256);
            let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
            let mut builder = ExecuteRequestBuilder::new(
                "Synthetic account identity witness",
                tx,
                CancellationToken::new(),
            )
            .model("main-b");
            builder = if override_without_name {
                builder.provider_override(other.clone())
            } else {
                builder.provider_name("chat-b")
            };
            let result =
                sdk_request_with_retry_mode(&agent, &mut session, builder.build(), retry_entry)
                    .await;
            drain.abort();
            assert!(result.is_err(), "unpaired target was accepted: retry={retry_entry}, override_only={override_without_name}");
            assert!(main.calls.lock().unwrap().is_empty());
            assert!(other.calls.lock().unwrap().is_empty());
            assert_eq!(serde_json::to_value(&session).unwrap(), before);
        }
    }
}

#[tokio::test]
async fn entrypoint_routing_sdk_accepts_explicit_matching_override_for_both_execute_entries() {
    for retry_entry in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut c = config(true, "chat-a");
        add_synthetic_chat_b(&mut c);
        let main = Arc::new(Capture::default());
        let other = Arc::new(Capture::default());
        let agent = agent(c, main.clone(), dir.path()).await;
        let mut session = Session::new("synthetic-paired-client", "main-b");
        session.add_message(Message::user("Synthetic explicit paired override"));
        let (tx, mut rx) = mpsc::channel(256);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let request = ExecuteRequestBuilder::new(
            "Synthetic explicit paired override",
            tx,
            CancellationToken::new(),
        )
        .model("main-b")
        .provider_name("chat-b")
        .provider_override(other.clone())
        .build();
        sdk_request_with_retry_mode(&agent, &mut session, request, retry_entry)
            .await
            .unwrap();
        drain.abort();
        assert!(main.calls.lock().unwrap().is_empty());
        let calls = other.calls.lock().unwrap().clone();
        assert!(
            !calls.is_empty() && calls.iter().all(|(model, _, _)| model == "main-b"),
            "{calls:?}"
        );
    }
}

#[tokio::test]
async fn entrypoint_routing_sdk_same_account_alias_preserves_gold_on_both_execute_entries() {
    for retry_entry in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut c = config(true, "chat-a");
        add_synthetic_chat_b(&mut c);
        let main = Arc::new(Capture::default());
        let agent = agent(c, main.clone(), dir.path()).await;
        let mut session = Session::new("synthetic-provider-alias", "main-a");
        session.add_message(Message::user("Synthetic same-account alias"));
        let (tx, mut rx) = mpsc::channel(256);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let mut request = ExecuteRequestBuilder::new(
            "Synthetic same-account alias",
            tx,
            CancellationToken::new(),
        )
        .model("main-a")
        .provider_name("openai")
        .build();
        request.gold_config = Some(GoldConfig {
            enabled: true,
            auto_continue_enabled: true,
            goal: Some("Synthetic goal".into()),
            ..Default::default()
        });
        request.app_data_dir = Some(dir.path().to_path_buf());
        sdk_request_with_retry_mode(&agent, &mut session, request, retry_entry)
            .await
            .unwrap();
        drain.abort();
        let calls = main.calls.lock().unwrap().clone();
        assert!(
            calls
                .iter()
                .any(|(model, purpose, _)| model == "cheap" && purpose == "gold_evaluation"),
            "{calls:?}"
        );
    }
}

#[tokio::test]
async fn entrypoint_routing_sdk_session_ref_cannot_switch_the_bound_client() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = config(true, "chat-a");
    add_synthetic_chat_b(&mut c);
    let main = Arc::new(Capture::default());
    let agent = bamboo_sdk::Agent::builder()
        .provider(main.clone())
        .config(Arc::new(RwLock::new(c)))
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(dir.path().to_path_buf())
        .await
        .unwrap()
        .build()
        .unwrap();
    let mut session = Session::new("synthetic-session-account", "main-b");
    session.model_ref = Some(bamboo_domain::ProviderModelRef::new("chat-b", "main-b"));
    session.add_message(Message::user("Synthetic saved target"));
    let before = serde_json::to_value(&session).unwrap();
    assert!(agent.run_session(&mut session).await.is_err());
    assert!(main.calls.lock().unwrap().is_empty());
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
}

#[tokio::test]
async fn entrypoint_routing_sdk_locked_replacement_config_fails_closed() {
    let server = vision_server(200).await;
    let dir = tempfile::tempdir().unwrap();
    let mut c = config(true, "chat-a");
    let mut value = serde_json::to_value(c.provider_instances.get("chat-a").unwrap()).unwrap();
    value["api_key"] = "synthetic-no-real-key".into();
    value["base_url"] = server.uri().into();
    c.provider_instances
        .insert("chat-a".into(), serde_json::from_value(value).unwrap());
    let mut persisted = serde_json::to_value(&c).unwrap();
    persisted["provider_instances"]["chat-a"]["api_key"] = "synthetic-no-real-key".into();
    std::fs::write(
        dir.path().join("config.json"),
        serde_json::to_vec(&persisted).unwrap(),
    )
    .unwrap();
    let shared = Arc::new(RwLock::new(c));
    let builder = bamboo_sdk::Agent::builder()
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(dir.path().to_path_buf())
        .await
        .unwrap();
    let lock = shared.write().await;
    let locked_result = builder.config(shared.clone()).build();
    assert!(
        locked_result.is_err(),
        "locked replacement config bypassed admission"
    );
    drop(lock);
    let unlocked_result = bamboo_sdk::Agent::builder()
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(dir.path().to_path_buf())
        .await
        .unwrap()
        .config(shared)
        .build();
    assert!(
        unlocked_result.is_ok(),
        "unlocked matching config should remain usable"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn entrypoint_routing_sdk_override_client_cannot_inherit_session_account() {
    for retry_entry in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut c = config(true, "chat-a");
        add_synthetic_chat_b(&mut c);
        let main = Arc::new(Capture::default());
        let other = Arc::new(Capture::default());
        let agent = agent(c, main.clone(), dir.path()).await;
        let mut session = Session::new("synthetic-override-session", "main-a");
        session.model_ref = Some(bamboo_domain::ProviderModelRef::new("chat-a", "main-a"));
        session.add_message(Message::user("Synthetic unpaired override"));
        let before = serde_json::to_value(&session).unwrap();
        let (tx, mut rx) = mpsc::channel(256);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let request =
            ExecuteRequestBuilder::new("Synthetic unpaired override", tx, CancellationToken::new())
                .provider_override(other.clone())
                .build();
        let outcome = sdk_request_with_retry_mode(&agent, &mut session, request, retry_entry).await;
        drain.abort();
        assert!(outcome.is_err());
        assert!(main.calls.lock().unwrap().is_empty());
        assert!(other.calls.lock().unwrap().is_empty());
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
    }
}

#[tokio::test]
async fn entrypoint_routing_sdk_invalid_typed_session_ref_cannot_fall_back_to_legacy_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let main = Arc::new(Capture::default());
    let agent = bamboo_sdk::Agent::builder()
        .provider(main.clone())
        .config(Arc::new(RwLock::new(config(true, "chat-a"))))
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(dir.path().to_path_buf())
        .await
        .unwrap()
        .build()
        .unwrap();
    let mut session = Session::new("synthetic-invalid-typed-ref", "main-a");
    session.set_provider_name("chat-a");
    session.model_ref = Some(bamboo_domain::ProviderModelRef::new("", "main-a"));
    session.add_message(Message::user("Synthetic invalid typed target"));
    let before = serde_json::to_value(&session).unwrap();
    assert!(agent.run_session(&mut session).await.is_err());
    assert!(main.calls.lock().unwrap().is_empty());
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
}

#[tokio::test]
async fn entrypoint_routing_sdk_retry_uses_bound_account_when_live_default_differs() {
    for retry_entry in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut c = config(true, "chat-b");
        add_synthetic_chat_b(&mut c);
        let selected = Arc::new(Capture::default());
        let agent = bamboo_sdk::Agent::builder()
            .provider(selected.clone())
            .provider_name("chat-b")
            .config(Arc::new(RwLock::new(c)))
            .tools(std::iter::empty())
            .with_defaults_for_data_dir(dir.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        let mut session = Session::new("synthetic-bound-retry", "main-b");
        session.add_message(Message::user("Synthetic bound retry"));
        let (tx, mut rx) = mpsc::channel(256);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let mut request =
            ExecuteRequestBuilder::new("Synthetic bound retry", tx, CancellationToken::new())
                .build();
        request.gold_config = Some(GoldConfig {
            enabled: true,
            auto_continue_enabled: true,
            goal: Some("Synthetic goal".into()),
            ..Default::default()
        });
        request.app_data_dir = Some(dir.path().to_path_buf());
        sdk_request_with_retry_mode(&agent, &mut session, request, retry_entry)
            .await
            .unwrap();
        drain.abort();
        let calls = selected.calls.lock().unwrap().clone();
        assert!(
            calls
                .iter()
                .any(|(model, purpose, _)| model == "main-b" && purpose != "gold_evaluation"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|(model, purpose, _)| model == "cheap" && purpose == "gold_evaluation"),
            "{calls:?}"
        );
    }
}

#[tokio::test]
async fn entrypoint_routing_sdk_legacy_pixels_reject_unpaired_named_account() {
    let server = vision_server(200).await;
    let dir = tempfile::tempdir().unwrap();
    let c = legacy_config(true, &server.uri());
    let fallback = bamboo_engine::model_config_helper::resolve_image_fallback(&c)
        .unwrap()
        .unwrap();
    let main = Arc::new(Capture::default());
    let agent = agent(c, main.clone(), dir.path()).await;
    let mut session = Session::new("synthetic-unpaired-image", "vision-b");
    session.add_message(Message::user_with_parts(
        "Inspect synthetic pixel",
        vec![bamboo_domain::MessagePart::ImageUrl {
            image_url: bamboo_domain::ImageUrlRef {
                url: PIXEL.into(),
                detail: None,
            },
        }],
    ));
    let before = serde_json::to_value(&session).unwrap();
    let (tx, mut rx) = mpsc::channel(256);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let request =
        ExecuteRequestBuilder::new("Inspect synthetic pixel", tx, CancellationToken::new())
            .model("vision-b")
            .provider_name("vision-b")
            .image_fallback(fallback)
            .build();
    let result = agent.execute(&mut session, request).await;
    drain.abort();
    assert!(result.is_err());
    assert!(main.calls.lock().unwrap().is_empty());
    assert!(server.received_requests().await.unwrap().is_empty());
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
}

#[tokio::test]
async fn entrypoint_routing_sdk_paired_override_normalizes_account_for_gold() {
    for retry_entry in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut c = config(true, "chat-b");
        add_synthetic_chat_b(&mut c);
        let main = Arc::new(Capture::default());
        let other = Arc::new(Capture::default());
        let agent = agent(c, main.clone(), dir.path()).await;
        let mut session = Session::new("synthetic-padded-account", "main-b");
        session.add_message(Message::user("Synthetic padded account"));
        let (tx, mut rx) = mpsc::channel(256);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let mut request =
            ExecuteRequestBuilder::new("Synthetic padded account", tx, CancellationToken::new())
                .model("main-b")
                .provider_name(" chat-b ")
                .provider_override(other.clone())
                .build();
        request.gold_config = Some(GoldConfig {
            enabled: true,
            auto_continue_enabled: true,
            goal: Some("Synthetic goal".into()),
            ..Default::default()
        });
        request.app_data_dir = Some(dir.path().to_path_buf());
        sdk_request_with_retry_mode(&agent, &mut session, request, retry_entry)
            .await
            .unwrap();
        drain.abort();
        assert!(main.calls.lock().unwrap().is_empty());
        let calls = other.calls.lock().unwrap().clone();
        assert!(
            calls
                .iter()
                .any(|(model, purpose, _)| model == "main-b" && purpose != "gold_evaluation"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|(model, purpose, _)| model == "cheap" && purpose == "gold_evaluation"),
            "{calls:?}"
        );
    }
}

#[tokio::test]
async fn entrypoint_routing_sdk_rejects_live_disabled_bound_provider_before_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let shared = Arc::new(RwLock::new(config(true, "chat-a")));
    let main = Arc::new(Capture::default());
    let agent = bamboo_sdk::Agent::builder()
        .provider(main.clone())
        .config(shared.clone())
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(dir.path().to_path_buf())
        .await
        .unwrap()
        .build()
        .unwrap();
    shared
        .write()
        .await
        .provider_instances
        .get_mut("chat-a")
        .unwrap()
        .enabled = false;

    let mut session = Session::new("synthetic-disabled-account", "main-a");
    session.model_ref = Some(bamboo_domain::ProviderModelRef::new("chat-a", "main-a"));
    session.add_message(Message::user("Synthetic disabled target"));
    let before = serde_json::to_value(&session).unwrap();
    assert!(agent.run_session(&mut session).await.is_err());
    assert_eq!(serde_json::to_value(&session).unwrap(), before);

    for retry_entry in [false, true] {
        let (tx, mut rx) = mpsc::channel(256);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let request =
            ExecuteRequestBuilder::new("Synthetic disabled target", tx, CancellationToken::new())
                .model("main-a")
                .provider_name("chat-a")
                .build();
        assert!(
            sdk_request_with_retry_mode(&agent, &mut session, request, retry_entry)
                .await
                .is_err()
        );
        drain.abort();
    }
    assert!(main.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn entrypoint_routing_sdk_explicit_model_admission_preserves_unset_list_behavior() {
    for explicit_list in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let mut c = config(true, "chat-a");
        if !explicit_list {
            c.provider_instances
                .get_mut("chat-a")
                .unwrap()
                .extra
                .remove("runtime_models");
        }
        let main = Arc::new(Capture::default());
        let agent = bamboo_sdk::Agent::builder()
            .provider(main.clone())
            .config(Arc::new(RwLock::new(c)))
            .tools(std::iter::empty())
            .with_defaults_for_data_dir(dir.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();
        let mut session = Session::new("synthetic-model-admission", "unlisted");
        session.model_ref = Some(bamboo_domain::ProviderModelRef::new("chat-a", "unlisted"));
        session.add_message(Message::user("Synthetic model admission"));
        let before = serde_json::to_value(&session).unwrap();
        let result = agent.run_session(&mut session).await;
        let calls = main.calls.lock().unwrap().clone();
        if explicit_list {
            assert!(result.is_err());
            assert!(calls.is_empty());
            assert_eq!(serde_json::to_value(&session).unwrap(), before);
        } else {
            result.unwrap();
            assert!(
                !calls.is_empty() && calls.iter().all(|(model, _, _)| model == "unlisted"),
                "{calls:?}"
            );
        }
    }
}

#[tokio::test]
async fn entrypoint_routing_sdk_explicit_fast_client_replaces_rejected_saved_route() {
    let dir = tempfile::tempdir().unwrap();
    let main = Arc::new(Capture::default());
    let fast = Arc::new(Capture::default());
    let agent = agent(config(true, "missing-b"), main.clone(), dir.path()).await;
    let mut session = Session::new("synthetic-explicit-fast", "main-a");
    session.add_message(Message::user("Synthetic explicit Fast override"));
    let (tx, mut rx) = mpsc::channel(256);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let mut request = ExecuteRequestBuilder::new(
        "Synthetic explicit Fast override",
        tx,
        CancellationToken::new(),
    )
    .model("main-a")
    .provider_name("chat-a")
    .fast_model("cheap")
    .fast_model_provider(fast.clone())
    .build();
    request.gold_config = Some(GoldConfig {
        enabled: true,
        auto_continue_enabled: true,
        goal: Some("Synthetic goal".into()),
        ..Default::default()
    });
    request.app_data_dir = Some(dir.path().to_path_buf());
    agent.execute(&mut session, request).await.unwrap();
    drain.abort();
    let main_calls = main.calls.lock().unwrap().clone();
    let fast_calls = fast.calls.lock().unwrap().clone();
    assert!(main_calls
        .iter()
        .all(|(_, purpose, _)| purpose != "gold_evaluation"));
    assert!(
        fast_calls
            .iter()
            .any(|(model, purpose, _)| model == "cheap" && purpose == "gold_evaluation"),
        "{fast_calls:?}"
    );
}

#[tokio::test]
async fn entrypoint_routing_sdk_native_reasoning_tracks_provider_source() {
    use bamboo_domain::ReasoningEffort::{Low, Max};

    let server = vision_server(200).await;
    let mut config = config(true, "openai");
    config.provider = "openai".into();
    config.default_provider_instance = Some("openai".into());
    config.provider_instances.clear();
    config.provider_instances.insert(
        "openai".into(),
        serde_json::from_value(serde_json::json!({
            "provider_type": "openai", "enabled": true,
            "api_key": "synthetic-no-real-key", "base_url": server.uri(),
            "model": "main-a", "runtime_models": ["main-a"]
        }))
        .unwrap(),
    );
    config.defaults.as_mut().unwrap().chat =
        bamboo_domain::ProviderModelRef::new("openai", "main-a").with_reasoning_effort(Low);
    let live = Arc::new(RwLock::new(config));
    let dir = tempfile::tempdir().unwrap();
    let agent = bamboo_sdk::Agent::builder()
        .config(live.clone())
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(dir.path().to_path_buf())
        .await
        .unwrap()
        .build()
        .unwrap();

    let mut first = Session::new("synthetic-native-origin-first", "main-a");
    first.model_ref = Some(bamboo_domain::ProviderModelRef::new("openai", "main-a"));
    first.add_message(Message::user("Synthetic native origin witness"));
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        agent.run_session(&mut first),
    )
    .await
    .unwrap()
    .unwrap();
    let first_requests = server.received_requests().await.unwrap();
    assert_eq!(first_requests.len(), 1);
    let first_body = first_requests[0].body_json::<serde_json::Value>().unwrap();
    assert_eq!(first_body["model"], "main-a");
    assert_eq!(first_body["reasoning_effort"], "low");

    // Removing the instance does not replace the assembled native client.
    // Refuse the changed source before any second request can reach it.
    {
        let mut changed = live.write().await;
        changed.provider_instances.remove("openai");
        changed.default_provider_instance = None;
        changed.defaults.as_mut().unwrap().chat.reasoning_effort = Some(Max);
    }
    let mut second = Session::new("synthetic-native-origin-second", "main-a");
    second.model_ref = Some(bamboo_domain::ProviderModelRef::new("openai", "main-a"));
    second.add_message(Message::user("Synthetic changed origin witness"));
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        agent.run_session(&mut second),
    )
    .await
    .unwrap()
    .expect_err("changed source cannot use the old native client");
    assert!(
        error.to_string().contains("sdk_provider_binding:"),
        "{error}"
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

/// A default-on capture supports the synthetic Vision model but leaves
/// `vision_support_override(main-a)` unspecified. An explicit `Some(true)`
/// for the main model bypasses the legacy transform, so it cannot witness it.
fn routing_review_allow_a_models(c: &mut Config, model: &str) {
    let account = c.provider_instances.get_mut("chat-a").unwrap();
    account.extra.insert(
        "runtime_models".into(),
        serde_json::json!(["main-a", "cheap", model]),
    );
    let mut capabilities = serde_json::Map::new();
    capabilities.insert(
        model.to_string(),
        serde_json::json!({"supports_vision": true}),
    );
    account.extra.insert(
        "model_capabilities".into(),
        serde_json::Value::Object(capabilities),
    );
}

/// The server-style fallback was derived from B/vision-b. Before execution,
/// the live role moves to C/vision-c. Either a consistent old snapshot, a
/// consistent new snapshot, or a refusal is acceptable; sending the image
/// to the unrelated main account A is not.
#[tokio::test]
async fn entrypoint_routing_legacy_stale_derived_role_never_rebinds_to_main() {
    for flag in [false, true] {
        let b = vision_server(200).await;
        let c_server = vision_server(200).await;
        let mut initial = legacy_config(flag, &b.uri());
        routing_review_allow_a_models(&mut initial, "vision-b");
        initial.provider_instances.insert(
            "vision-c".into(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai", "enabled": true,
                "api_key": "synthetic-no-real-key", "base_url": c_server.uri(),
                "model": "vision-c", "runtime_models": ["vision-c"],
                "model_capabilities": {"vision-c": {"supports_vision": true}}
            }))
            .unwrap(),
        );
        // Connect/child resume obtains the host-derived image fallback from
        // an earlier config snapshot, before the runtime reads live config.
        let main = Arc::new(Capture::default());
        let main_provider: Arc<dyn LLMProvider> = main.clone();
        let registry = Arc::new(bamboo_llm::ProviderRegistry::new(
            std::collections::HashMap::from([("chat-a".to_string(), main_provider)]),
            "chat-a".to_string(),
        ));
        let compatibility = bamboo_engine::session_app::resolution::resolve_resume_config_snapshot(
            &initial,
            &registry,
            &Session::new("synthetic-resume-compatibility", "main-a"),
            None,
        );
        assert_eq!(
            compatibility
                .image_fallback
                .unwrap()
                .vision_model
                .as_deref(),
            Some("vision-b")
        );
        let snapshot =
            bamboo_engine::session_app::resolution::resolve_resume_config_snapshot_for_execution(
                &initial,
                &registry,
                &Session::new("synthetic-resume-snapshot", "main-a"),
                None,
            );
        let fallback = snapshot.image_fallback.unwrap();
        assert_eq!(fallback.vision_model, None);

        let shared = Arc::new(RwLock::new(initial));
        assert!(main.supports_vision("vision-b").await);
        assert_ne!(main.vision_support_override("main-a").await, Some(true));
        let dir = tempfile::tempdir().unwrap();
        let agent = bamboo_sdk::Agent::builder()
            .provider(main.clone())
            .model("main-a")
            .config(shared.clone())
            .tools(std::iter::empty())
            .with_defaults_for_data_dir(dir.path().to_path_buf())
            .await
            .unwrap()
            .build()
            .unwrap();

        shared.write().await.defaults.as_mut().unwrap().vision =
            Some(bamboo_domain::ProviderModelRef::new("vision-c", "vision-c"));
        let mut session = Session::new(format!("synthetic-stale-role-{flag}"), "main-a");
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
                .app_data_dir(dir.path().to_path_buf())
                .build();
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            agent.execute(&mut session, request),
        )
        .await
        .expect("controlled legacy consumer must finish");
        drain.abort();
        let a_calls = main.calls.lock().unwrap().clone();
        let b_calls = b.received_requests().await.unwrap();
        let c_calls = c_server.received_requests().await.unwrap();
        let b_saw_pixel = b_calls
            .iter()
            .any(|request| String::from_utf8_lossy(&request.body).contains(PIXEL));
        let c_saw_pixel = c_calls
            .iter()
            .any(|request| String::from_utf8_lossy(&request.body).contains(PIXEL));
        assert_eq!(
            session
                .messages
                .iter()
                .find(|message| message.id == original.id)
                .unwrap()
                .content_parts,
            original.content_parts,
            "the original synthetic image must remain in session history"
        );
        assert!(
            a_calls.iter().all(|(_, _, has_images)| !has_images),
            "flag={flag} A received a synthetic image: {a_calls:?}; B={}, C={}, outcome={outcome:?}",
            b_calls.len(),
            c_calls.len(),
        );
        assert!(outcome.is_ok(), "flag={flag}: {outcome:?}");
        assert!(
            !b_saw_pixel && c_saw_pixel,
            "current Vision role must receive the synthetic image: flag={flag}, B={}, C={}",
            b_calls.len(),
            c_calls.len()
        );
    }
}

/// A real caller-supplied bare model override still belongs to its explicitly
/// supplied active client; it must not be confused with a derived role.
#[tokio::test]
async fn entrypoint_routing_legacy_explicit_bare_override_stays_on_active_client() {
    let b = vision_server(200).await;
    let mut c = legacy_config(true, &b.uri());
    routing_review_allow_a_models(&mut c, "vision-on-a");
    let dir = tempfile::tempdir().unwrap();
    let main = Arc::new(Capture::default());
    let agent = agent(c, main.clone(), dir.path()).await;
    let mut session = Session::new("synthetic-bare-vision-override", "main-a");
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
            .image_fallback(bamboo_engine::ImageFallbackConfig {
                mode: bamboo_engine::ImageFallbackMode::Vision,
                vision_model: Some("vision-on-a".into()),
            })
            .app_data_dir(dir.path().to_path_buf())
            .build();
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        agent.execute(&mut session, request),
    )
    .await
    .expect("controlled bare override must finish")
    .unwrap();
    drain.abort();
    let calls = main.calls.lock().unwrap().clone();
    assert!(
        calls
            .iter()
            .any(|(model, _, images)| model == "vision-on-a" && *images),
        "explicit override did not reach active client: {calls:?}"
    );
    assert!(b.received_requests().await.unwrap().is_empty());
    assert_eq!(
        session
            .messages
            .iter()
            .find(|message| message.id == original.id)
            .unwrap()
            .content_parts,
        original.content_parts,
    );
}

#[tokio::test]
async fn entrypoint_routing_legacy_explicit_same_name_override_stays_on_active_client() {
    let b = vision_server(200).await;
    let mut c = legacy_config(true, &b.uri());
    routing_review_allow_a_models(&mut c, "vision-b");
    let dir = tempfile::tempdir().unwrap();
    let main = Arc::new(Capture::default());
    let agent = agent(c, main.clone(), dir.path()).await;
    let mut session = Session::new("synthetic-matching-bare-vision-override", "main-a");
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
            .image_fallback(bamboo_engine::ImageFallbackConfig {
                mode: bamboo_engine::ImageFallbackMode::Vision,
                vision_model: Some("vision-b".into()),
            })
            .app_data_dir(dir.path().to_path_buf())
            .build();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        agent.execute(&mut session, request),
    )
    .await
    .expect("controlled bare override must finish");
    drain.abort();
    let calls = main.calls.lock().unwrap().clone();
    let requests = b.received_requests().await.unwrap();
    let b_saw_pixel = requests
        .iter()
        .any(|request| String::from_utf8_lossy(&request.body).contains(PIXEL));
    let b_models = requests
        .iter()
        .map(|request| request.body_json::<serde_json::Value>().unwrap()["model"].clone())
        .collect::<Vec<_>>();
    assert!(outcome.is_ok()
        && calls.iter().any(|(model, _, images)| model == "vision-b" && *images)
        && requests.is_empty(),
        "explicit override must use A: outcome={outcome:?}, A={calls:?}, B_models={b_models:?}, B_saw_pixel={b_saw_pixel}");
    assert_eq!(
        session
            .messages
            .iter()
            .find(|message| message.id == original.id)
            .unwrap()
            .content_parts,
        original.content_parts,
    );
}

#[tokio::test]
async fn entrypoint_routing_legacy_public_helper_bare_model_stays_on_active_client() {
    let b = vision_server(200).await;
    let mut c = legacy_config(true, &b.uri());
    routing_review_allow_a_models(&mut c, "vision-b");
    let dir = tempfile::tempdir().unwrap();
    let main = Arc::new(Capture::default());
    let fallback = bamboo_engine::model_config_helper::resolve_image_fallback(&c)
        .unwrap()
        .unwrap();
    let agent = agent(c, main.clone(), dir.path()).await;
    let mut session = Session::new("synthetic-public-helper-vision-override", "main-a");
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
            .app_data_dir(dir.path().to_path_buf())
            .build();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        agent.execute(&mut session, request),
    )
    .await
    .expect("controlled bare override must finish");
    drain.abort();
    let calls = main.calls.lock().unwrap().clone();
    let requests = b.received_requests().await.unwrap();
    let b_saw_pixel = requests
        .iter()
        .any(|request| String::from_utf8_lossy(&request.body).contains(PIXEL));
    let b_models = requests
        .iter()
        .map(|request| request.body_json::<serde_json::Value>().unwrap()["model"].clone())
        .collect::<Vec<_>>();
    assert!(outcome.is_ok()
        && calls.iter().any(|(model, _, images)| model == "vision-b" && *images)
        && requests.is_empty(),
        "explicit override must use A: outcome={outcome:?}, A={calls:?}, B_models={b_models:?}, B_saw_pixel={b_saw_pixel}");
    assert_eq!(
        session
            .messages
            .iter()
            .find(|message| message.id == original.id)
            .unwrap()
            .content_parts,
        original.content_parts,
    );
}

#[tokio::test]
async fn entrypoint_routing_legacy_missing_or_empty_explicit_override_never_sends_image() {
    let mut outcomes = Vec::new();
    for (index, model) in [Some(""), Some("   "), None].into_iter().enumerate() {
        let b = vision_server(200).await;
        let c = legacy_config(true, &b.uri());
        let dir = tempfile::tempdir().unwrap();
        let main = Arc::new(Capture::default());
        let agent = agent(c, main.clone(), dir.path()).await;
        let mut session = Session::new(format!("synthetic-empty-vision-{index}"), "main-a");
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
                .image_fallback(bamboo_engine::ImageFallbackConfig {
                    mode: bamboo_engine::ImageFallbackMode::Vision,
                    vision_model: model.map(str::to_string),
                })
                .app_data_dir(dir.path().to_path_buf())
                .build();
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            agent.execute(&mut session, request),
        )
        .await
        .expect("controlled missing/empty override must finish");
        drain.abort();
        let calls = main.calls.lock().unwrap().clone();
        let requests = b.received_requests().await.unwrap();
        let b_saw_pixel = requests
            .iter()
            .any(|request| String::from_utf8_lossy(&request.body).contains(PIXEL));
        let preserved = session
            .messages
            .iter()
            .find(|message| message.id == original.id)
            .is_some_and(|message| message.content_parts == original.content_parts);
        outcomes.push((
            index,
            outcome,
            calls,
            requests.len(),
            b_saw_pixel,
            preserved,
        ));
    }
    assert!(
        outcomes.iter().all(
            |(_, outcome, calls, count, pixel, preserved)| outcome.is_err()
                && calls.iter().all(|(_, _, images)| !images)
                && *count == 0
                && !pixel
                && *preserved
        ),
        "missing or empty caller overrides must refuse before any image send: {outcomes:?}"
    );
}

/// A native SDK client was made from instance A named `openai`. Replacing that
/// instance with a legacy `openai` pointing to B does not change the name but
/// does change the account. A later configured Vision target must not reuse A.
#[tokio::test]
async fn entrypoint_routing_legacy_same_key_changed_origin_never_reuses_native_client() {
    let a = vision_server(200).await;
    let b = vision_server(200).await;
    let mut c = config(true, "openai");
    c.provider = "openai".into();
    c.default_provider_instance = Some("openai".into());
    c.provider_instances.clear();
    c.provider_instances.insert(
        "openai".into(),
        serde_json::from_value(serde_json::json!({
            "provider_type": "openai", "enabled": true,
            "api_key": "synthetic-a", "base_url": a.uri(),
            "model": "main-a", "runtime_models": ["main-a", "vision-b"],
            "model_capabilities": {"vision-b": {"supports_vision": true}}
        }))
        .unwrap(),
    );
    c.defaults.as_mut().unwrap().chat = bamboo_domain::ProviderModelRef::new("openai", "main-a");
    c.defaults.as_mut().unwrap().vision =
        Some(bamboo_domain::ProviderModelRef::new("openai", "vision-b"));
    c.hooks.image_fallback.enabled = true;
    c.hooks.image_fallback.mode = "vision".into();
    let shared = Arc::new(RwLock::new(c));
    let dir = tempfile::tempdir().unwrap();
    let agent = bamboo_sdk::Agent::builder()
        .config(shared.clone())
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(dir.path().to_path_buf())
        .await
        .unwrap()
        .build()
        .unwrap();

    let fallback = {
        let mut next = shared.write().await;
        next.provider_instances.remove("openai");
        next.default_provider_instance = None;
        next.providers_mut().openai = Some(
            serde_json::from_value(serde_json::json!({
                "api_key": "synthetic-b", "base_url": b.uri(),
                "model": "main-a", "vision_model": "vision-b",
                "runtime_models": ["main-a", "vision-b"],
                "model_capabilities": {"vision-b": {"supports_vision": true}}
            }))
            .unwrap(),
        );
        bamboo_engine::model_config_helper::resolve_image_fallback(&next)
            .unwrap()
            .unwrap()
    };
    let mut session = Session::new("synthetic-origin-replaced", "main-a");
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
            .provider_name("openai")
            .image_fallback(fallback)
            .app_data_dir(dir.path().to_path_buf())
            .build();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        agent.execute(&mut session, request),
    )
    .await
    .expect("controlled changed-origin consumer must finish");
    drain.abort();
    let a_requests = a.received_requests().await.unwrap();
    let b_requests = b.received_requests().await.unwrap();
    let a_saw_pixel = a_requests
        .iter()
        .any(|request| String::from_utf8_lossy(&request.body).contains(PIXEL));
    let b_saw_pixel = b_requests
        .iter()
        .any(|request| String::from_utf8_lossy(&request.body).contains(PIXEL));
    assert_eq!(
        session
            .messages
            .iter()
            .find(|message| message.id == original.id)
            .unwrap()
            .content_parts,
        original.content_parts,
    );
    assert!(
        !a_saw_pixel,
        "removed instance A received the image after legacy B took the same key: A={}, B={}, outcome={outcome:?}",
        a_requests.len(),
        b_requests.len(),
    );
    assert!(
        outcome.is_err() || b_saw_pixel,
        "successful conversion must use legacy B or report refusal: A={}, B={}",
        a_requests.len(),
        b_requests.len(),
    );
}

#[tokio::test]
async fn entrypoint_routing_legacy_live_snapshot_keeps_copilot_default() {
    let mut config = config(false, "copilot");
    config.provider = "copilot".into();
    config.default_provider_instance = None;
    config.provider_instances.clear();
    config.defaults = None;
    config.hooks.image_fallback.enabled = true;
    config.hooks.image_fallback.mode = "vision".into();
    assert_eq!(config.get_vision_model().as_deref(), Some("gpt-4o"));

    let main = Arc::new(Capture::default());
    let main_provider: Arc<dyn LLMProvider> = main.clone();
    let registry = Arc::new(bamboo_llm::ProviderRegistry::new(
        std::collections::HashMap::from([("copilot".to_string(), main_provider)]),
        "copilot".to_string(),
    ));
    let snapshot =
        bamboo_engine::session_app::resolution::resolve_resume_config_snapshot_for_execution(
            &config,
            &registry,
            &Session::new("synthetic-copilot-snapshot", "main-a"),
            None,
        );
    let fallback = snapshot.image_fallback.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let agent = bamboo_sdk::Agent::builder()
        .provider(main.clone())
        .model("main-a")
        .config(Arc::new(RwLock::new(config)))
        .tools(std::iter::empty())
        .with_defaults_for_data_dir(dir.path().to_path_buf())
        .await
        .unwrap()
        .build()
        .unwrap();
    let mut session = Session::new("synthetic-copilot-fallback", "main-a");
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
            .provider_name("copilot")
            .image_fallback(fallback)
            .app_data_dir(dir.path().to_path_buf())
            .build();
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        agent.execute(&mut session, request),
    )
    .await
    .expect("controlled Copilot fallback must finish")
    .unwrap();
    drain.abort();
    let calls = main.calls.lock().unwrap().clone();
    assert!(
        calls
            .iter()
            .any(|(model, _, images)| model == "gpt-4o" && *images),
        "the default must stay on the active client: {calls:?}"
    );
    assert_eq!(
        session
            .messages
            .iter()
            .find(|message| message.id == original.id)
            .unwrap()
            .content_parts,
        original.content_parts,
    );
}

#[tokio::test]
async fn entrypoint_routing_sdk_native_overlay_keeps_selected_client_in_both_builder_orders() {
    for config_before_defaults in [false, true] {
        let server = vision_server(200).await;
        let mut config = config(true, "openai");
        config.provider = "openai".into();
        config.default_provider_instance = Some("openai".into());
        config.provider_instances.clear();
        config.provider_instances.insert(
            "openai".into(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "openai", "enabled": true,
                "api_key": "synthetic-config-key", "base_url": server.uri(),
                "model": "main-a", "runtime_models": ["main-a"]
            }))
            .unwrap(),
        );
        config.defaults.as_mut().unwrap().chat =
            bamboo_domain::ProviderModelRef::new("openai", "main-a");
        let shared = Arc::new(RwLock::new(config.clone()));
        let dir = tempfile::tempdir().unwrap();
        // Config serialization omits keys; restore only this synthetic fixture
        // key in its isolated temporary data root for the disk-first order.
        let mut disk = serde_json::to_value(&config).unwrap();
        disk["provider_instances"]["openai"]["api_key"] = "synthetic-config-key".into();
        tokio::fs::write(
            dir.path().join("config.json"),
            serde_json::to_vec(&disk).unwrap(),
        )
        .await
        .unwrap();
        let builder = bamboo_sdk::Agent::builder()
            .provider_name("openai")
            .api_key("synthetic-overlay-key")
            .model("main-a")
            .tools(std::iter::empty());
        let builder = if config_before_defaults {
            builder
                .config(shared.clone())
                .with_defaults_for_data_dir(dir.path().to_path_buf())
                .await
                .unwrap()
        } else {
            builder
                .with_defaults_for_data_dir(dir.path().to_path_buf())
                .await
                .unwrap()
                .config(shared.clone())
        };
        let agent = builder.build().unwrap();
        let mut session = Session::new("synthetic-native-overlay", "main-a");
        session.model_ref = Some(bamboo_domain::ProviderModelRef::new("openai", "main-a"));
        session.add_message(Message::user("Synthetic overlay control"));
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            agent.run_session(&mut session),
        )
        .await
        .unwrap()
        .unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests.len(),
            1,
            "config_before_defaults={config_before_defaults}"
        );
        assert_eq!(
            requests[0].body_json::<serde_json::Value>().unwrap()["model"],
            "main-a"
        );
        assert_eq!(
            requests[0]
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer synthetic-overlay-key"),
            "the selected local client must use the explicit synthetic overlay"
        );
        assert_eq!(
            shared.read().await.provider_instances["openai"].api_key,
            "synthetic-config-key"
        );
    }
}

#[tokio::test]
async fn entrypoint_routing_sdk_named_provider_new_session_is_independent_of_builder_order() {
    for config_before_defaults in [false, true] {
        for override_model in [false, true] {
            let a = vision_server(200).await;
            let b = vision_server(200).await;
            let mut config = config(false, "chat-a");
            config.provider = "openai".into();
            config.defaults = None;
            config.default_provider_instance = Some("chat-b".into());
            config.provider_instances.clear();
            for (id, model, server) in [("chat-a", "model-a", &a), ("chat-b", "model-b", &b)] {
                config.provider_instances.insert(
                    id.into(),
                    serde_json::from_value(serde_json::json!({
                        "provider_type": "openai", "enabled": true, "api_key": "synthetic-key",
                        "base_url": server.uri(), "model": model
                    }))
                    .unwrap(),
                );
            }
            let dir = tempfile::tempdir().unwrap();
            let mut disk = serde_json::to_value(&config).unwrap();
            for id in ["chat-a", "chat-b"] {
                disk["provider_instances"][id]["api_key"] = "synthetic-key".into();
            }
            tokio::fs::write(
                dir.path().join("config.json"),
                serde_json::to_vec(&disk).unwrap(),
            )
            .await
            .unwrap();
            let shared = Arc::new(RwLock::new(config));
            let mut builder = bamboo_sdk::Agent::builder()
                .provider_name("chat-a")
                .tools(std::iter::empty());
            if override_model {
                builder = builder.model("manual-model");
            }
            let builder = if config_before_defaults {
                builder
                    .config(shared)
                    .with_defaults_for_data_dir(dir.path().to_path_buf())
                    .await
                    .unwrap()
            } else {
                builder
                    .with_defaults_for_data_dir(dir.path().to_path_buf())
                    .await
                    .unwrap()
                    .config(shared)
            };
            let agent = builder.build().unwrap();
            let expected = if override_model {
                "manual-model"
            } else {
                "model-a"
            };
            let mut session = agent.new_session("synthetic-provider-order").unwrap();
            assert_eq!(session.model, expected);
            session.add_message(Message::user("Synthetic model selection"));
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                agent.run_session(&mut session),
            )
            .await
            .unwrap()
            .unwrap();
            let requests = a.received_requests().await.unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0].body_json::<serde_json::Value>().unwrap()["model"],
                expected
            );
            assert!(b.received_requests().await.unwrap().is_empty());
        }
    }
}
