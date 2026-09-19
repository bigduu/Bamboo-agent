//! #1097: invalid instance PUTs must fail before metadata/credential commit.
//! All provider traffic and all Bamboo/Jiandu data belong to these fixtures.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use actix_web::http::{Method, StatusCode};
use actix_web::{test, web, App};
use bamboo_domain::{Message, ProviderModelRef, Role};
use bamboo_llm::LLMChunk;
use bamboo_server::{configure_routes, AppState};
use futures::StreamExt;
use serde_json::{json, Value};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const INSTANCES: &str = "/api/v1/bamboo/settings/provider-instances";
const SETTINGS: &str = "/api/v1/bamboo/config/provider-settings";
const DEFAULT_ID: &str = "fixture-a";
const OTHER_ID: &str = "fixture-b";
const MODEL: &str = "fixture-chat";

async fn request(
    state: &web::Data<AppState>,
    method: Method,
    uri: &str,
    payload: Option<Value>,
) -> (StatusCode, Value) {
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let mut request = test::TestRequest::default().method(method).uri(uri);
    if let Some(payload) = payload {
        request = request.set_json(payload);
    }
    let response = test::call_service(&app, request.to_request()).await;
    let status = response.status();
    (status, test::read_body_json(response).await)
}

async fn state_from_fixture(data_dir: &Path) -> web::Data<AppState> {
    web::Data::new(
        AppState::new_with_memory_store(
            data_dir.to_path_buf(),
            bamboo_memory::memory_store::MemoryStore::new(data_dir.join("jiandu")),
        )
        .await
        .expect("synthetic app state"),
    )
}

async fn fixture() -> (tempfile::TempDir, MockServer, web::Data<AppState>) {
    let data_dir = tempfile::tempdir().unwrap();
    bamboo_config::paths::init_bamboo_dir(data_dir.path().to_path_buf());
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            concat!(
                "data: {\"id\":\"fixture-response\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"fixture response\"}}]}\n\n",
                "data: {\"id\":\"fixture-response\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n"
            ),
            "text/event-stream",
        ))
        .mount(&upstream)
        .await;
    let config: bamboo_config::Config = serde_json::from_value(json!({
        "headless_auth": true,
        "features": {"provider_model_ref": true},
        "default_provider_instance": DEFAULT_ID,
        "defaults": {
            "chat": {"provider": DEFAULT_ID, "model": MODEL},
            "fast": {"provider": DEFAULT_ID, "model": MODEL}
        },
        "provider_instances": {
            DEFAULT_ID: {
                "provider_type": "openai", "enabled": true,
                "label": "Synthetic A", "model": MODEL,
                "base_url": format!("{}/v1", upstream.uri()),
                "api_key": "synthetic-a-not-a-real-key"
            },
            OTHER_ID: {
                "provider_type": "openai", "enabled": false,
                "label": "Synthetic B", "model": MODEL,
                "base_url": format!("{}/v1", upstream.uri()),
                "api_key": "synthetic-b-not-a-real-key"
            }
        }
    }))
    .unwrap();
    let state = state_from_fixture(data_dir.path()).await;
    state
        .update_config_with_provider_credentials(
            move |candidate| {
                candidate.provider_instances = config.provider_instances.clone();
                candidate.default_provider_instance = config.default_provider_instance.clone();
                candidate.defaults = config.defaults.clone();
                candidate.features = config.features.clone();
                Ok(())
            },
            BTreeSet::new(),
            BTreeSet::from([DEFAULT_ID.to_string(), OTHER_ID.to_string()]),
            bamboo_server::app_state::ConfigUpdateEffects {
                reload_provider: bamboo_config::patch::ReloadMode::Strict,
                reconcile_mcp: bamboo_config::patch::ReloadMode::None,
            },
        )
        .await
        .expect("seed synthetic instances through the existing credential transaction");
    (data_dir, upstream, state)
}

fn durable_documents(data_dir: &Path) -> BTreeMap<String, Option<Vec<u8>>> {
    [
        "config.json",
        "providers.json",
        "credentials.json",
        "model-policy.json",
        "model_limits.json",
    ]
    .into_iter()
    .flat_map(|name| [name.to_string(), format!("{name}.bak")])
    .map(|name| {
        let bytes = match std::fs::read(data_dir.join(&name)) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => panic!("cannot inspect fixture document {name}: {error}"),
        };
        (name, bytes)
    })
    .collect()
}

async fn assert_provider_responds(state: &web::Data<AppState>, instance: &str) {
    let provider = state
        .get_provider_for_model_ref(&ProviderModelRef::new(instance, MODEL))
        .expect("configured instance remains routable");
    let mut stream = provider
        .chat_stream(&[Message::user("synthetic test message")], &[], None, MODEL)
        .await
        .expect("loopback provider accepts chat");
    let mut text = String::new();
    while let Some(chunk) = stream.next().await {
        if let LLMChunk::Token(token) = chunk.unwrap() {
            text.push_str(&token);
        }
    }
    assert_eq!(text, "fixture response");
}

async fn assert_execution_completed(state: &web::Data<AppState>, chat: &Value) {
    let session_id = chat["session_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status = state
                .agent_runners
                .read()
                .await
                .get(session_id)
                .map(|runner| runner.status.clone());
            match status {
                Some(bamboo_server::app_state::AgentStatus::Completed) => break,
                Some(bamboo_server::app_state::AgentStatus::Error(error)) => {
                    panic!("synthetic native execution failed: {error}")
                }
                Some(bamboo_server::app_state::AgentStatus::Cancelled) => {
                    panic!("synthetic execution unexpectedly cancelled")
                }
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("synthetic native execution completes promptly");
    let session = state
        .storage
        .load_session(session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(session
        .messages
        .iter()
        .any(|message| message.role == Role::Assistant && message.content == "fixture response"));
}

async fn assert_native_chat(state: &web::Data<AppState>, instance: Option<&str>, message: &str) {
    let mut chat_payload = json!({"message": message});
    let mut execute_payload = json!({});
    if let Some(instance) = instance {
        let model_ref = json!({"provider": instance, "model": MODEL});
        chat_payload["model_ref"] = model_ref.clone();
        execute_payload["model_ref"] = model_ref;
    }
    let (status, chat) = request(state, Method::POST, "/api/v1/chat", Some(chat_payload)).await;
    assert_eq!(status, StatusCode::CREATED);
    let execution = request(
        state,
        Method::POST,
        &format!("/api/v1/execute/{}", chat["session_id"].as_str().unwrap()),
        Some(execute_payload),
    )
    .await;
    assert!(
        execution.0.is_success(),
        "synthetic native execution must start"
    );
    assert_execution_completed(state, &chat).await;
}

async fn assert_valid_disable_and_default_switch(state: &web::Data<AppState>) {
    // Having another enabled provider must not silently change the explicit
    // default; the user must choose the replacement before disabling A.
    let other_path = format!("{INSTANCES}/{OTHER_ID}");
    assert_eq!(
        request(
            state,
            Method::PUT,
            &other_path,
            Some(json!({"enabled": true}))
        )
        .await
        .0,
        StatusCode::OK
    );
    let enabled = request(state, Method::GET, SETTINGS, None).await;
    let rejected = request(
        state,
        Method::PUT,
        &format!("{INSTANCES}/{DEFAULT_ID}"),
        Some(json!({"enabled": false})),
    )
    .await;
    assert_eq!(rejected.0, StatusCode::BAD_REQUEST);
    assert_eq!(request(state, Method::GET, SETTINGS, None).await, enabled);

    assert_eq!(
        request(
            state,
            Method::PUT,
            &other_path,
            Some(json!({"enabled": false}))
        )
        .await
        .0,
        StatusCode::OK
    );
    let disabled = request(state, Method::GET, SETTINGS, None).await.1;
    assert_eq!(
        disabled["data"]["provider_instances"][OTHER_ID]["enabled"],
        false
    );
    assert_eq!(disabled["data"]["default_provider_instance_id"], DEFAULT_ID);
    assert_native_chat(
        state,
        Some(DEFAULT_ID),
        "chat after valid non-default disable",
    )
    .await;

    assert_eq!(
        request(
            state,
            Method::PUT,
            &other_path,
            Some(json!({"enabled": true}))
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        request(
            state,
            Method::POST,
            &format!("{INSTANCES}/default"),
            Some(json!({"default_provider_instance_id": OTHER_ID}))
        )
        .await
        .0,
        StatusCode::OK
    );
    let selected = request(state, Method::GET, SETTINGS, None).await.1;
    assert_eq!(selected["data"]["default_provider_instance_id"], OTHER_ID);
    assert_eq!(state.provider_registry.default_provider_name(), OTHER_ID);
    // Model assignments are an explicit, separate settings choice. Do not
    // infer a new model-selection policy from changing the default instance.
    let mut data = selected["data"].clone();
    data["defaults"]["chat"]["provider"] = json!(OTHER_ID);
    data["defaults"]["fast"]["provider"] = json!(OTHER_ID);
    assert_eq!(
        request(
            state,
            Method::PUT,
            SETTINGS,
            Some(json!({
                "expected_revision": selected["revision"], "data": data
            }))
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        request(
            state,
            Method::PUT,
            &format!("{INSTANCES}/{DEFAULT_ID}"),
            Some(json!({"enabled": false}))
        )
        .await
        .0,
        StatusCode::OK
    );
    let settings = request(state, Method::GET, SETTINGS, None).await.1;
    assert_eq!(settings["data"]["default_provider_instance_id"], OTHER_ID);
    assert_eq!(state.provider_registry.default_provider_name(), OTHER_ID);
    assert_eq!(
        settings["data"]["provider_instances"][DEFAULT_ID]["enabled"],
        false
    );
    assert_eq!(
        settings["data"]["provider_instances"][OTHER_ID]["enabled"],
        true
    );
    assert_eq!(settings["data"]["defaults"]["chat"]["provider"], OTHER_ID);
    assert_eq!(settings["data"]["defaults"]["fast"]["provider"], OTHER_ID);
    let instances = request(state, Method::GET, INSTANCES, None).await.1;
    assert_eq!(instances["default_provider_instance_id"], OTHER_ID);
    for instance in instances["instances"].as_array().unwrap() {
        let id = instance["id"].as_str().unwrap();
        assert_eq!(
            instance["enabled"],
            settings["data"]["provider_instances"][id]["enabled"]
        );
    }
    assert_native_chat(
        state,
        None,
        "implicit-default chat after explicit default switch and old-provider disable",
    )
    .await;
}

#[actix_web::test]
async fn default_disable_retries_preserve_durable_live_and_restart_state() {
    let (data_dir, _upstream, state) = fixture().await;
    assert_provider_responds(&state, DEFAULT_ID).await;
    assert_native_chat(
        &state,
        Some(DEFAULT_ID),
        "working native chat before invalid disable",
    )
    .await;
    let before = request(&state, Method::GET, SETTINGS, None).await;
    assert_eq!(before.0, StatusCode::OK);
    let durable_before = durable_documents(data_dir.path());
    let live_before = serde_json::to_value(state.config.read().await.clone()).unwrap();
    // Config serialization deliberately skips plaintext credentials, so the
    // live snapshot needs an explicit comparison for this synthetic key.
    let live_key_before = state.config.read().await.provider_instances[DEFAULT_ID]
        .api_key
        .clone();
    assert_eq!(live_key_before, "synthetic-a-not-a-real-key");
    let provider_before = state.provider.read().await.clone();
    let registry_before = state.provider_registry.get_default().unwrap();
    let health_before =
        serde_json::to_value(state.config_live_health.read().unwrap().clone()).unwrap();
    let event_seq_before = state.account_sink.latest_seq();
    let mut statuses = Vec::new();
    for _ in 0..3 {
        let (status, body) = request(
            &state,
            Method::PUT,
            &format!("{INSTANCES}/{DEFAULT_ID}"),
            Some(json!({"enabled": false})),
        )
        .await;
        statuses.push(status);
        if status == StatusCode::BAD_REQUEST {
            let message = body["error"]["message"].as_str().unwrap();
            assert!(message.contains("set another enabled provider instance as default first"));
            assert_eq!(durable_documents(data_dir.path()), durable_before);
        }
    }
    // The same rejection must precede the credential-owning transaction too,
    // including both replacement and clear intents carried by an instance PUT.
    for key in [json!("synthetic-rejected-replacement"), Value::Null] {
        for _ in 0..3 {
            let rejected = request(
                &state,
                Method::PUT,
                &format!("{INSTANCES}/{DEFAULT_ID}"),
                Some(json!({
                    "enabled": false, "label": "rejected label", "config": {"api_key": key}
                })),
            )
            .await;
            assert_eq!(rejected.0, StatusCode::BAD_REQUEST);
            assert!(rejected.1["error"]["message"]
                .as_str()
                .unwrap()
                .contains("set another enabled provider instance as default first"));
            assert_eq!(durable_documents(data_dir.path()), durable_before);
            assert_eq!(request(&state, Method::GET, SETTINGS, None).await, before);
            assert_eq!(
                state.config.read().await.provider_instances[DEFAULT_ID].api_key,
                live_key_before
            );
        }
    }
    let after = request(&state, Method::GET, SETTINGS, None).await;
    let chat = request(
        &state,
        Method::POST,
        "/api/v1/chat",
        Some(json!({
            "message": "chat after rejected default disable",
            "model_ref": {"provider": DEFAULT_ID, "model": MODEL}
        })),
    )
    .await;
    let execution = request(
        &state,
        Method::POST,
        &format!("/api/v1/execute/{}", chat.1["session_id"].as_str().unwrap()),
        Some(json!({"model_ref": {"provider": DEFAULT_ID, "model": MODEL}})),
    )
    .await;
    eprintln!(
        "default-disable retries: statuses={statuses:?}, revision_before={}, revision_after={}, live_enabled={}, durable_changed={}, subsequent_chat={}, execution={}",
        before.1["revision"], after.1["revision"],
        state.config.read().await.provider_instances[DEFAULT_ID].enabled,
        durable_documents(data_dir.path()) != durable_before, chat.0, execution.0
    );
    assert!(statuses
        .iter()
        .all(|status| *status == StatusCode::BAD_REQUEST));
    assert_eq!(after, before, "canonical provider settings must not change");
    assert_eq!(durable_documents(data_dir.path()), durable_before);
    assert_eq!(
        serde_json::to_value(state.config.read().await.clone()).unwrap(),
        live_before
    );
    assert!(Arc::ptr_eq(
        &state.provider.read().await.clone(),
        &provider_before
    ));
    assert!(Arc::ptr_eq(
        &state.provider_registry.get_default().unwrap(),
        &registry_before
    ));
    assert_eq!(state.provider_registry.default_provider_name(), DEFAULT_ID);
    assert_eq!(
        serde_json::to_value(state.config_live_health.read().unwrap().clone()).unwrap(),
        health_before
    );
    assert_eq!(
        chat.0,
        StatusCode::CREATED,
        "subsequent native chat must remain available"
    );
    assert!(
        execution.0.is_success(),
        "native execution must remain available"
    );
    assert_execution_completed(&state, &chat.1).await;
    assert_provider_responds(&state, DEFAULT_ID).await;
    let provider_events = bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), event_seq_before)
        .unwrap().into_iter().filter(|entry| matches!(&entry.event,
            bamboo_agent_core::AgentEvent::ConfigChanged { section, .. }
            | bamboo_agent_core::AgentEvent::ConfigInvalid { section, .. }
            | bamboo_agent_core::AgentEvent::ConfigRecovered { section, .. } if section == "providers"
        )).collect::<Vec<_>>();
    assert!(
        provider_events.is_empty(),
        "rejected PUTs must not publish provider generations"
    );
    assert_eq!(
        state.config.read().await.provider_instances[DEFAULT_ID].api_key,
        live_key_before
    );
    state.shutdown().await;
    drop(state);
    let restarted = state_from_fixture(data_dir.path()).await;
    let restarted_settings = request(&restarted, Method::GET, SETTINGS, None).await;
    assert_eq!(restarted_settings.1["revision"], before.1["revision"]);
    assert_eq!(restarted_settings.1["data"], before.1["data"]);
    assert_provider_responds(&restarted, DEFAULT_ID).await;
    assert_native_chat(&restarted, Some(DEFAULT_ID), "chat after restart").await;
    assert_valid_disable_and_default_switch(&restarted).await;
    restarted.shutdown().await;
}
