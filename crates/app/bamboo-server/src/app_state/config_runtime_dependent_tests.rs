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
    // Building an AppState publishes the default retry policy process-wide (the
    // builder does that for real startup too). Every builder in this module
    // therefore holds this guard, so a test that must observe the published
    // policy is not raced by a sibling test's AppState construction.
    let _guard = policy_lock().lock().await;
    configured_state_inner(proxy).await
}

/// This guard only serializes builders in this module. Unrelated AppState
/// publishers do not take it, so assertions against the global retry slot use
/// an isolated invocation of this same Cargo-built executable below.
fn policy_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

const POLICY_CASE_ENV: &str = "BAMBOO_RETRY_POLICY_ISOLATED_CASE";

async fn isolated_policy_case(case: &str) -> bool {
    let full_name = format!("app_state::config_runtime::dependent_runtime_tests::{case}");
    if let Some(selected) = std::env::var_os(POLICY_CASE_ENV) {
        assert_eq!(selected, std::ffi::OsStr::new(&full_name));
        return false;
    }
    let root = tempfile::tempdir().unwrap();
    let home = root.path().canonicalize().unwrap();
    let stdout_path = home.join("stdout");
    let stderr_path = home.join("stderr");
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", &full_name, "--nocapture", "--test-threads=1"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env_clear()
        .env(POLICY_CASE_ENV, &full_name)
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("TMPDIR", &home)
        .env("TEMP", &home)
        .env("TMP", &home)
        .env("RUST_MIN_STACK", "8388608")
        .stdout(std::fs::File::create(&stdout_path).unwrap())
        .stderr(std::fs::File::create(&stderr_path).unwrap())
        .kill_on_drop(true);
    // Retain only executable/platform loader inputs, never provider, workflow
    // or publication credentials. The case supplies its own synthetic key and
    // loopback proxy; no Cargo/compiler is launched by the child.
    for key in [
        "PATH",
        "SystemRoot",
        "WINDIR",
        "COMSPEC",
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
    ] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    let mut child = command.spawn().unwrap();
    let status = match tokio::time::timeout(Duration::from_secs(30), child.wait()).await {
        Ok(status) => Some(status.unwrap()),
        Err(_) => {
            child.kill().await.unwrap();
            None
        }
    };
    let stdout = std::fs::read_to_string(stdout_path).unwrap();
    let stderr = std::fs::read_to_string(stderr_path).unwrap();
    assert!(
        status.is_some_and(|status| status.success())
            && stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored;")
            && stdout.contains(&format!("retry-policy-assertions-complete:{case}")),
        "isolated policy case {full_name} failed or did not execute its assertions: \
         {status:?}\nstdout={stdout}\nstderr={stderr}"
    );
    true
}

#[tokio::test]
async fn unrelated_app_state_publication_is_not_excluded_by_the_module_guard() {
    const CASE: &str = "unrelated_app_state_publication_is_not_excluded_by_the_module_guard";
    if isolated_policy_case(CASE).await {
        return;
    }
    let _key = bamboo_config::encryption::set_test_encryption_key([0x68; 32]);
    let _guard = policy_lock().lock().await;
    let sink = request_sink().await;
    let (_first_dir, first) = configured_state_inner(&sink.uri()).await;
    let mut core = section(&first, SectionId::Core);
    core.data["llm_retry"] = serde_json::json!({
        "max_attempts":11,"attempt_timeout_ms":240_000,"total_budget_ms":1_800_000,
        "base_delay_ms":900,"max_delay_ms":45_000,
    });
    first
        .put_ordinary_section(SectionId::Core, core.revision, core.data)
        .await
        .unwrap();
    assert_eq!(bamboo_llm::retry::global().max_attempts, 11);
    // This is the real constructor used by sibling modules, not the guarded
    // helper. It can publish None while the module's guard remains held.
    let second_dir = tempfile::tempdir().unwrap();
    let mut second = AppState::new(second_dir.path().to_path_buf())
        .await
        .unwrap();
    second.stop_config_watcher_for_test();
    assert_eq!(
        bamboo_llm::retry::global().max_attempts,
        bamboo_llm::retry::RetryConfig::from_env().max_attempts
    );
    assert_ne!(bamboo_llm::retry::global().max_attempts, 11);
    assert!(policy_lock().try_lock().is_err());
    println!("retry-policy-assertions-complete:{CASE}");
}

async fn configured_state_inner(proxy: &str) -> (tempfile::TempDir, AppState) {
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
        .mcp
        .commit(revision, bamboo_config::McpSection(mcp.clone()))
        .unwrap();
    // Model a configured server whose bootstrap failed: its desired metadata
    // stays effective while the manager has no usable transport to publish.
    // A new proxy generation must retry it before committing either dependency.
    state.config.write().await.mcp = mcp;
    endpoint
}

async fn application_consumer(state: &AppState, id: &str) -> Value {
    let _io = state.config_io_lock.lock().await;
    let report = serde_json::to_value(state.config_application_diagnostics_locked().await).unwrap();
    report["consumers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|consumer| consumer["id"] == id)
        .unwrap()
        .clone()
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
    policy.data["anthropic_model_mapping"] =
        serde_json::json!({"mappings":{"claude-alias":"claude-upstream"}});
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
    assert_eq!(
        state.config.read().await.anthropic_model_mapping.mappings["claude-alias"],
        "claude-upstream"
    );
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
    let before_provider = application_consumer(&state, "provider-runtime").await;
    let before_mcp = application_consumer(&state, "mcp-runtime").await;
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
    let failed_provider = application_consumer(&state, "provider-runtime").await;
    let failed_mcp = application_consumer(&state, "mcp-runtime").await;
    assert_eq!(
        failed_provider["applied_dependencies"],
        before_provider["applied_dependencies"]
    );
    assert_eq!(
        failed_mcp["applied_dependencies"],
        before_mcp["applied_dependencies"]
    );
    assert_eq!(failed_provider["state"], "pending_apply");
    assert_eq!(failed_mcp["state"], "failed");
    assert_eq!(
        failed_mcp["last_apply_error"]["code"],
        "mcp_initialization_failed"
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
    let repaired_provider = application_consumer(&state, "provider-runtime").await;
    assert_eq!(repaired_provider["state"], "in_sync");
    assert_eq!(
        repaired_provider["applied_dependencies"],
        before_provider["applied_dependencies"]
    );
    // The bootstrap-failure fixture still has an unavailable desired MCP server.
    // Repair that consumer through its real transactional publication.
    let mcp = section(&state, SectionId::Mcp);
    state
        .put_mcp_section(mcp.revision, bamboo_mcp::McpConfig::default())
        .await
        .unwrap();
    let repaired_mcp = application_consumer(&state, "mcp-runtime").await;
    assert_eq!(repaired_mcp["state"], "in_sync");
    assert_eq!(repaired_mcp["last_apply_error"], Value::Null);
    assert_eq!(
        repaired_mcp["applied_dependencies"],
        repaired_mcp["desired_dependencies"]
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

/// An accepted Core API write must reach the provider layer immediately. The
/// registry suppresses self-write echoes, so if publication only happened on
/// watcher reloads the settings page would appear to save while requests kept
/// using the previous policy until a restart or an unrelated edit.
#[tokio::test]
async fn accepted_core_write_publishes_the_llm_retry_policy_to_the_provider_layer() {
    const CASE: &str = "accepted_core_write_publishes_the_llm_retry_policy_to_the_provider_layer";
    if isolated_policy_case(CASE).await {
        return;
    }
    let _key = bamboo_config::encryption::set_test_encryption_key([0x63; 32]);
    // Hold the builder guard across build + write + read: a sibling test's
    // AppState construction would otherwise republish the defaults mid-flight.
    let _guard = policy_lock().lock().await;
    let sink = request_sink().await;
    let (_dir, state) = configured_state_inner(&sink.uri()).await;

    // Restore afterwards: the policy is process-global.
    let previous_retry = bamboo_llm::retry::global();
    let previous_limits = bamboo_llm::retry::initial_request_limits();

    let mut core = section(&state, SectionId::Core);
    core.data["llm_retry"] = serde_json::json!({
        "max_attempts": 11,
        "attempt_timeout_ms": 240_000,
        "total_budget_ms": 1_800_000,
        "base_delay_ms": 900,
        "max_delay_ms": 45_000,
    });
    let committed = state
        .put_ordinary_section(SectionId::Core, core.revision, core.data)
        .await
        .unwrap();
    assert_eq!(committed.data["llm_retry"]["max_attempts"], 11);

    let published = bamboo_llm::retry::global();
    let limits = bamboo_llm::retry::initial_request_limits();
    bamboo_llm::retry::configure(previous_limits, previous_retry);

    assert_eq!(published.max_attempts, 11);
    assert_eq!(published.base_delay, Duration::from_millis(900));
    assert_eq!(published.max_delay, Duration::from_millis(45_000));
    assert_eq!(limits.attempt_timeout, Duration::from_millis(240_000));
    assert_eq!(limits.total_budget, Duration::from_millis(1_800_000));
    println!("retry-policy-assertions-complete:{CASE}");
}

/// An unset policy must not be published. Otherwise upgrading a deployment that
/// tunes `BAMBOO_LLM_RETRY_*` silently replaces those settings with the built-in
/// defaults, because publication stops the environment from being consulted.
#[tokio::test]
async fn unset_llm_retry_leaves_the_environment_derived_policy_in_force() {
    const CASE: &str = "unset_llm_retry_leaves_the_environment_derived_policy_in_force";
    if isolated_policy_case(CASE).await {
        return;
    }
    let _key = bamboo_config::encryption::set_test_encryption_key([0x65; 32]);
    let _guard = policy_lock().lock().await;
    let sink = request_sink().await;
    let (_dir, state) = configured_state_inner(&sink.uri()).await;

    // Simulate a deployment tuned only through the environment: no published
    // override exists, so `global()`/`initial_request_limits()` fall back to
    // these variables.
    let previous_retry = bamboo_llm::retry::global();
    let previous_limits = bamboo_llm::retry::initial_request_limits();
    bamboo_llm::retry::clear_published_policy();
    std::env::set_var("BAMBOO_LLM_MAX_RETRIES", "2");
    std::env::set_var("BAMBOO_LLM_RETRY_ATTEMPT_TIMEOUT_MS", "111000");
    std::env::set_var("BAMBOO_LLM_RETRY_TOTAL_BUDGET_MS", "1234000");

    // The freshly built state has no persisted policy.
    assert!(state.config.read().await.llm_retry.is_none());
    apply_llm_retry_policy(&state.config.read().await.clone());

    let kept = bamboo_llm::retry::global();
    let kept_limits = bamboo_llm::retry::initial_request_limits();

    std::env::remove_var("BAMBOO_LLM_MAX_RETRIES");
    std::env::remove_var("BAMBOO_LLM_RETRY_ATTEMPT_TIMEOUT_MS");
    std::env::remove_var("BAMBOO_LLM_RETRY_TOTAL_BUDGET_MS");
    bamboo_llm::retry::configure(previous_limits, previous_retry);

    // BAMBOO_LLM_MAX_RETRIES=2 means two retries after the first attempt.
    assert_eq!(
        kept.max_attempts, 3,
        "an unset policy must not publish defaults"
    );
    assert_eq!(kept_limits.attempt_timeout, Duration::from_millis(111_000));
    assert_eq!(kept_limits.total_budget, Duration::from_millis(1_234_000));
    println!("retry-policy-assertions-complete:{CASE}");
}

/// Removing a previously saved policy must drop the live override, otherwise
/// requests keep using settings the configuration no longer contains.
#[tokio::test]
async fn removing_the_persisted_policy_clears_the_published_override() {
    const CASE: &str = "removing_the_persisted_policy_clears_the_published_override";
    if isolated_policy_case(CASE).await {
        return;
    }
    let _key = bamboo_config::encryption::set_test_encryption_key([0x66; 32]);
    let _guard = policy_lock().lock().await;
    let sink = request_sink().await;
    let (_dir, state) = configured_state_inner(&sink.uri()).await;

    let previous_retry = bamboo_llm::retry::global();
    let previous_limits = bamboo_llm::retry::initial_request_limits();

    let mut core = section(&state, SectionId::Core);
    core.data["llm_retry"] = serde_json::json!({
        "max_attempts": 9,
        "attempt_timeout_ms": 200_000,
        "total_budget_ms": 900_000,
        "base_delay_ms": 500,
        "max_delay_ms": 30_000,
    });
    state
        .put_ordinary_section(SectionId::Core, core.revision, core.data)
        .await
        .unwrap();
    assert_eq!(bamboo_llm::retry::global().max_attempts, 9);

    let mut core = section(&state, SectionId::Core);
    core.data["llm_retry"] = serde_json::Value::Null;
    state
        .put_ordinary_section(SectionId::Core, core.revision, core.data)
        .await
        .unwrap();

    let after_removal = bamboo_llm::retry::global();
    bamboo_llm::retry::configure(previous_limits, previous_retry);

    assert_ne!(
        after_removal.max_attempts, 9,
        "a removed policy must not stay in force"
    );
    assert_eq!(after_removal.max_attempts, previous_retry.max_attempts);
    println!("retry-policy-assertions-complete:{CASE}");
}

/// Resetting Core removes the saved policy, so it must clear the live override
/// too. The reset transaction adopts its own revision, so the watcher does not
/// repair this afterwards.
#[tokio::test]
async fn resetting_core_clears_the_published_retry_policy() {
    const CASE: &str = "resetting_core_clears_the_published_retry_policy";
    if isolated_policy_case(CASE).await {
        return;
    }
    let _key = bamboo_config::encryption::set_test_encryption_key([0x67; 32]);
    let _guard = policy_lock().lock().await;
    let sink = request_sink().await;
    let (_dir, state) = configured_state_inner(&sink.uri()).await;

    let previous_retry = bamboo_llm::retry::global();
    let previous_limits = bamboo_llm::retry::initial_request_limits();

    let mut core = section(&state, SectionId::Core);
    core.data["llm_retry"] = serde_json::json!({
        "max_attempts": 7,
        "attempt_timeout_ms": 200_000,
        "total_budget_ms": 900_000,
        "base_delay_ms": 500,
        "max_delay_ms": 30_000,
    });
    state
        .put_ordinary_section(SectionId::Core, core.revision, core.data)
        .await
        .unwrap();
    assert_eq!(bamboo_llm::retry::global().max_attempts, 7);

    let revision = section(&state, SectionId::Core).revision;
    state
        .reset_credential_backed_section(SectionId::Core, revision)
        .await
        .unwrap();

    let after_reset = bamboo_llm::retry::global();
    bamboo_llm::retry::configure(previous_limits, previous_retry);

    assert_ne!(
        after_reset.max_attempts, 7,
        "resetting Core must not leave the saved policy active"
    );
    println!("retry-policy-assertions-complete:{CASE}");
}

/// An out-of-range policy must be rejected before it is committed, so the API
/// cannot report success for settings that requests would refuse to use.
#[tokio::test]
async fn core_write_rejects_an_invalid_llm_retry_policy_before_committing() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x64; 32]);
    let sink = request_sink().await;
    let (_dir, state) = configured_state(&sink.uri()).await;

    let mut core = section(&state, SectionId::Core);
    core.data["llm_retry"] = serde_json::json!({
        "max_attempts": 0,
        "attempt_timeout_ms": 180_000,
        "total_budget_ms": 900_000,
        "base_delay_ms": 500,
        "max_delay_ms": 30_000,
    });
    assert!(
        state
            .put_ordinary_section(SectionId::Core, core.revision, core.data)
            .await
            .is_err(),
        "max_attempts: 0 must not be accepted"
    );
    let live = state.config.read().await.llm_retry;
    assert_ne!(
        live.map(|policy| policy.max_attempts),
        Some(0),
        "the rejected policy must not reach the live configuration"
    );
}
