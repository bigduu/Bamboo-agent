use super::*;
use bamboo_agent_core::Message;
use futures::StreamExt;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

async fn request_sink() -> MockServer {
    let sink = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(concat!(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n",
                    "data: [DONE]\n\n"
                )),
        )
        .mount(&sink)
        .await;
    sink
}

async fn configured_state(proxy: &str) -> (tempfile::TempDir, AppState) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("config.json"),
        serde_json::to_vec(&serde_json::json!({"provider":"openai","http_proxy":proxy})).unwrap(),
    )
    .unwrap();
    // Seed the supported legacy encrypted fixture so startup exercises the
    // credential migration. Plaintext provider saves are intentionally rejected.
    // All network requests are intercepted by a local proxy; loopback endpoints
    // themselves bypass the proxy and would not prove client reconstruction.
    std::fs::write(
        dir.path().join("providers.json"),
        serde_json::to_vec(&serde_json::json!({"openai":{
            "api_key_encrypted":bamboo_config::encryption::encrypt("dependent-runtime-test-key").unwrap(),
            "base_url":"http://provider.invalid/v1"
        }}))
        .unwrap(),
    )
    .unwrap();
    let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
    state.stop_config_watcher_for_test();
    (dir, state)
}

async fn unavailable_mcp(state: &AppState) -> MockServer {
    let endpoint = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&endpoint)
        .await;
    let mcp: bamboo_mcp::config::McpConfig = serde_json::from_value(serde_json::json!({
        "version":1,
        "servers":[{
            "id":"configured-but-unavailable",
            "transport":{"type":"streamable_http","url":endpoint.uri(),"connect_timeout_ms":100},
            "request_timeout_ms":500,
            "reconnect":{"enabled":false}
        }]
    }))
    .unwrap();
    let facade = state.config_facade.as_ref().unwrap();
    let revision = section(state, SectionId::Mcp).revision;
    facade
        .registry()
        .commit_value_with_envelope(
            SectionId::Mcp,
            revision,
            serde_json::to_value(&mcp).unwrap(),
        )
        .unwrap();
    // Model a configured server whose bootstrap failed: its desired metadata
    // stays effective while the manager has no usable transport to publish.
    // A new proxy generation must retry it before committing either dependency.
    state.config.write().await.mcp = mcp;
    endpoint
}

fn section(state: &AppState, id: SectionId) -> bamboo_config::SectionEnvelope<Value> {
    state
        .config_facade
        .as_ref()
        .unwrap()
        .registry()
        .envelope_value(id)
        .unwrap()
}

async fn request(state: &AppState) {
    let provider = state.get_provider().await;
    let mut stream = provider
        .chat_stream(&[Message::user("project-code")], &[], None, "gpt-4o")
        .await
        .unwrap();
    while let Some(chunk) = stream.next().await {
        chunk.unwrap();
    }
}

fn masking(data: &mut Value) {
    data["keyword_masking"] = serde_json::json!({
        "entries": [{"pattern": "project-code", "match_type": "exact", "enabled": true}]
    });
}

fn external_edit(dir: &Path, id: SectionId, update: impl FnOnce(&mut Value)) {
    let path = dir.join(id.descriptor().file_name);
    let mut document: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    update(&mut document["data"]);
    // Keep the editor's revision unchanged; the ordinary reload seam must
    // normalize the new bytes before applying this external generation.
    std::fs::write(path, serde_json::to_vec(&document).unwrap()).unwrap();
}

#[tokio::test]
async fn dependent_typed_proxy_and_masking_affect_subsequent_provider_requests() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x61; 32]);
    let first = request_sink().await;
    let second = request_sink().await;
    let (_dir, state) = configured_state(&first.uri()).await;
    request(&state).await;

    let mut core = section(&state, SectionId::Core);
    core.data["http_proxy"] = Value::String(second.uri());
    state
        .put_ordinary_section(SectionId::Core, core.revision, core.data)
        .await
        .unwrap();
    request(&state).await;
    let mut policy = section(&state, SectionId::ModelPolicy);
    masking(&mut policy.data);
    state
        .put_ordinary_section(SectionId::ModelPolicy, policy.revision, policy.data)
        .await
        .unwrap();
    request(&state).await;

    assert_eq!(first.received_requests().await.unwrap().len(), 1);
    let requests = second.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(String::from_utf8_lossy(&requests[0].body).contains("project-code"));
    assert!(!String::from_utf8_lossy(&requests[1].body).contains("project-code"));
}

#[tokio::test]
async fn dependent_external_reload_proxy_and_masking_affect_provider_requests() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x62; 32]);
    let first = request_sink().await;
    let second = request_sink().await;
    let (dir, state) = configured_state(&first.uri()).await;
    external_edit(dir.path(), SectionId::Core, |data| {
        data["http_proxy"] = Value::String(second.uri());
    });
    state
        .reload_ordinary_section_for_test(SectionId::Core)
        .await;
    request(&state).await;
    external_edit(dir.path(), SectionId::ModelPolicy, masking);
    state
        .reload_ordinary_section_for_test(SectionId::ModelPolicy)
        .await;
    request(&state).await;
    assert!(first.received_requests().await.unwrap().is_empty());
    let requests = second.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(String::from_utf8_lossy(&requests[0].body).contains("project-code"));
    assert!(!String::from_utf8_lossy(&requests[1].body).contains("project-code"));
}

#[tokio::test]
async fn dependent_metadata_only_edits_keep_provider_handles() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x63; 32]);
    let sink = request_sink().await;
    let (dir, state) = configured_state(&sink.uri()).await;
    let _unavailable = unavailable_mcp(&state).await;
    let provider = state.provider_registry.get_default().unwrap();
    let mut core = section(&state, SectionId::Core);
    core.data["server"]["port"] = serde_json::json!(24_123);
    state
        .put_ordinary_section(SectionId::Core, core.revision, core.data)
        .await
        .unwrap();
    let mut policy = section(&state, SectionId::ModelPolicy);
    policy.data["anthropic_model_mapping"] = serde_json::json!({"default_model": "gpt-4o"});
    state
        .put_ordinary_section(SectionId::ModelPolicy, policy.revision, policy.data)
        .await
        .unwrap();
    external_edit(dir.path(), SectionId::Core, |data| {
        data["server"]["port"] = serde_json::json!(24_124);
    });
    state
        .reload_ordinary_section_for_test(SectionId::Core)
        .await;
    assert!(Arc::ptr_eq(
        &provider,
        &state.provider_registry.get_default().unwrap()
    ));
    assert_eq!(state.config.read().await.server.port, 24_124);
}

#[tokio::test]
async fn dependent_edits_remain_configurable_without_a_default_provider() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x64; 32]);
    let dir = tempfile::tempdir().unwrap();
    let mut initial = Config::default();
    initial.provider = "openai".to_string();
    initial.save_to_dir(dir.path().to_path_buf()).unwrap();
    let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
    state.stop_config_watcher_for_test();
    assert!(state.provider_registry.get_default().is_none());
    let mut core = section(&state, SectionId::Core);
    core.data["server"]["port"] = serde_json::json!(24_125);
    core.data["http_proxy"] = serde_json::json!("http://127.0.0.1:12345");
    state
        .put_ordinary_section(SectionId::Core, core.revision, core.data)
        .await
        .unwrap();
    let mut policy = section(&state, SectionId::ModelPolicy);
    masking(&mut policy.data);
    state
        .put_ordinary_section(SectionId::ModelPolicy, policy.revision, policy.data)
        .await
        .unwrap();
    assert_eq!(state.config.read().await.server.port, 24_125);
    assert_eq!(state.config.read().await.keyword_masking.entries.len(), 1);
}

#[tokio::test]
async fn dependent_typed_failure_does_not_commit_or_replace_usable_runtime() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x65; 32]);
    let sink = request_sink().await;
    let replacement = request_sink().await;
    let (dir, state) = configured_state(&sink.uri()).await;
    let _unavailable = unavailable_mcp(&state).await;
    let path = dir.path().join("core.json");
    let before = std::fs::read(&path).unwrap();
    let provider = state.provider_registry.get_default().unwrap();
    let mut core = section(&state, SectionId::Core);
    core.data["http_proxy"] = Value::String(replacement.uri());
    assert!(matches!(
        state
            .put_ordinary_section(SectionId::Core, core.revision, core.data)
            .await,
        Err(ConfigSectionMutationError::Runtime(_))
    ));
    assert_eq!(std::fs::read(path).unwrap(), before);
    assert_eq!(state.config.read().await.http_proxy, sink.uri());
    assert!(Arc::ptr_eq(
        &provider,
        &state.provider_registry.get_default().unwrap()
    ));
    assert_eq!(
        section(&state, SectionId::Core).status,
        SectionStatus::Degraded
    );
    request(&state).await;
}

#[tokio::test]
async fn dependent_external_failure_retains_effective_generation_and_recovers() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x66; 32]);
    let sink = request_sink().await;
    let replacement = request_sink().await;
    let (dir, state) = configured_state(&sink.uri()).await;
    let _unavailable = unavailable_mcp(&state).await;
    let provider = state.provider_registry.get_default().unwrap();
    external_edit(dir.path(), SectionId::Core, |data| {
        data["http_proxy"] = serde_json::json!("http://");
    });
    state
        .reload_ordinary_section_for_test(SectionId::Core)
        .await;
    assert_eq!(
        section(&state, SectionId::Core).status,
        SectionStatus::Invalid
    );
    assert_eq!(state.config.read().await.http_proxy, sink.uri());
    external_edit(dir.path(), SectionId::Core, |data| {
        data["http_proxy"] = Value::String(replacement.uri());
    });
    state
        .reload_ordinary_section_for_test(SectionId::Core)
        .await;
    assert_eq!(state.config.read().await.http_proxy, sink.uri());
    assert!(Arc::ptr_eq(
        &provider,
        &state.provider_registry.get_default().unwrap()
    ));
    assert_eq!(
        section(&state, SectionId::Core).status,
        SectionStatus::Degraded
    );
    request(&state).await;
    external_edit(dir.path(), SectionId::Core, |data| {
        data["http_proxy"] = Value::String(sink.uri());
        data["server"]["port"] = serde_json::json!(24_126);
    });
    state
        .reload_ordinary_section_for_test(SectionId::Core)
        .await;
    assert_eq!(state.config.read().await.server.port, 24_126);
    assert_eq!(
        section(&state, SectionId::Core).status,
        SectionStatus::Healthy
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dependent_cancelled_commit_finishes_before_later_configuration_generation() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x67; 32]);
    let first = request_sink().await;
    let second = request_sink().await;
    let (dir, state) = configured_state(&first.uri()).await;
    let state = Arc::new(state);
    let mut core = section(&state, SectionId::Core);
    core.data["http_proxy"] = Value::String(second.uri());
    let mut policy = section(&state, SectionId::ModelPolicy);
    masking(&mut policy.data);
    let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    set_generic_before_provider_publish_test_hook(dir.path(), move || {
        committed_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    let caller = {
        let state = state.clone();
        tokio::spawn(async move {
            state
                .put_ordinary_section(SectionId::Core, core.revision, core.data)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), committed_rx)
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(state.config_io_lock.try_lock().is_err());
    let later = {
        let state = state.clone();
        tokio::spawn(async move {
            state
                .put_ordinary_section(SectionId::ModelPolicy, policy.revision, policy.data)
                .await
        })
    };
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), later)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(state.config.read().await.http_proxy, second.uri());
    assert_eq!(state.config.read().await.keyword_masking.entries.len(), 1);
    request(&state).await;
    let requests = second.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(!String::from_utf8_lossy(&requests[0].body).contains("project-code"));
}
