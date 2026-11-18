//! Exercise CLI setup and output against the real binary and loopback provider APIs.

use std::fs::File;
use std::path::Path;
use std::process::{Output, Stdio};
use std::time::Duration;

use bamboo_agent_core::{storage::Storage, Message, Session};
use bamboo_domain::{AgentRuntimeState, AgentStatusState};
use serde_json::{json, Value};
use tokio::process::Command;
use wiremock::matchers::{
    body_partial_json, body_string_contains, header, method, path, query_param,
};
use wiremock::{Mock, MockServer, ResponseTemplate};

const MODEL: &str = "fixture-model";
const KEY: &str = "synthetic-api-key";
const ANSWER: &str = "Fixture answer";
const REASONING: &str = "fixture private thought";
const ENCRYPTION_KEY: [u8; 32] = [0xc1; 32];

fn gateway_environment(command: &mut Command, provider: &str, endpoint: &str) {
    let prefix = format!("BAMBOO_{}", provider.to_ascii_uppercase());
    command
        .env(format!("{prefix}_BASE_URL"), endpoint)
        .env(format!("{prefix}_API_KEY"), KEY)
        .env(format!("{prefix}_MODEL"), MODEL);
}

fn bamboo(directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_bamboo"));
    command
        .current_dir(directory)
        .env_clear()
        .env("HOME", directory)
        .env("USERPROFILE", directory)
        .env("BAMBOO_DATA_DIR", directory.join("bamboo"))
        .env("BAMBOO_JIANDU_DATA_DIR", directory.join("jiandu"))
        .env("BAMBOO_CONFIG_ENCRYPTION_KEY", hex::encode(ENCRYPTION_KEY))
        .env("RUST_LOG", "error")
        .stdin(Stdio::null())
        .kill_on_drop(true);
    #[cfg(windows)]
    if let Some(root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", root);
    }
    command
}

async fn run(command: &mut Command) -> Output {
    let output = run_to_exit(command).await;
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

async fn run_to_exit(command: &mut Command) -> Output {
    tokio::time::timeout(Duration::from_secs(35), command.output())
        .await
        .expect("Bamboo must exit")
        .expect("launch the Bamboo binary")
}

fn stream(body: String) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_raw(body, "text/event-stream")
}

fn openai_stream() -> ResponseTemplate {
    let thinking = json!({
        "choices": [{"index":0,"delta":{"reasoning_content":REASONING},"finish_reason":null}]
    });
    let answer = json!({
        "choices": [{"index":0,"delta":{"content":ANSWER},"finish_reason":"stop"}]
    });
    stream(format!(
        "data: {thinking}\n\ndata: {answer}\n\ndata: [DONE]\n\n"
    ))
}

fn anthropic_stream() -> ResponseTemplate {
    let events = [
        (
            "content_block_start",
            json!({"index":0,"content_block":{"type":"text","text":""}}),
        ),
        (
            "content_block_delta",
            json!({"index":0,"delta":{"type":"text_delta","text":ANSWER}}),
        ),
        ("content_block_stop", json!({"index":0})),
        (
            "message_delta",
            json!({"delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}),
        ),
        ("message_stop", json!({})),
    ];
    stream(
        events
            .into_iter()
            .map(|(name, body)| format!("event: {name}\ndata: {body}\n\n"))
            .collect(),
    )
}

fn ndjson_events(output: &Output) -> Vec<Value> {
    let events: Vec<Value> = std::str::from_utf8(&output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        events
            .iter()
            .filter(|event| event["type"] == "result")
            .count(),
        1,
        "stdout: {:?}, stderr: {}",
        events,
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(events.last().unwrap()["type"], "result");
    events
}

fn terminal_usage(input: u64, output: u64) -> Value {
    json!({"input_tokens": input, "output_tokens": output, "total_tokens": input + output})
}

fn anthropic_counted_stream(
    input: u64,
    output: u64,
    reason: &str,
    block: Value,
    delta: Value,
) -> ResponseTemplate {
    let events = [
        (
            "message_start",
            json!({"message":{
                "id":"msg_fixture","type":"message","role":"assistant","model":MODEL,"content":[],
                "usage":{"input_tokens":input,"output_tokens":0,
                    "cache_creation_input_tokens":0,"cache_read_input_tokens":0}
            }}),
        ),
        (
            "content_block_start",
            json!({"index":0,"content_block":block}),
        ),
        ("content_block_delta", json!({"index":0,"delta":delta})),
        ("content_block_stop", json!({"index":0})),
        (
            "message_delta",
            json!({"delta":{"stop_reason":reason},"usage":{"output_tokens":output}}),
        ),
        ("message_stop", json!({})),
    ];
    stream(
        events
            .into_iter()
            .map(|(name, data)| format!("event: {name}\ndata: {data}\n\n"))
            .collect(),
    )
}

#[tokio::test]
async fn headless_stream_result_reports_usage_on_success_and_output_limit_after_resume() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("bamboo");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(
        data.join("config.json"),
        r#"{"memory":{"auto_dream_enabled":false}}"#,
    )
    .unwrap();
    let server = MockServer::start().await;
    let file = dir.path().join("fixture.txt");
    std::fs::write(&file, "Fixture content").unwrap();
    let tool_input = json!({"file_path": file}).to_string();
    Mock::given(path("/initial/v1/messages"))
        .respond_with(anthropic_counted_stream(
            101,
            17,
            "tool_use",
            json!({"type":"tool_use","id":"toolu_fixture_read","name":"Read","input":{}}),
            json!({"type":"input_json_delta","partial_json":tool_input}),
        ))
        .with_priority(2)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/initial/v1/messages"))
        .and(body_string_contains("toolu_fixture_read"))
        .respond_with(anthropic_counted_stream(
            211,
            29,
            "end_turn",
            json!({"type":"text","text":""}),
            json!({"type":"text_delta","text":ANSWER}),
        ))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/resume/v1/messages"))
        .respond_with(anthropic_counted_stream(
            43,
            13,
            "max_tokens",
            json!({"type":"thinking","thinking":"","signature":""}),
            json!({"type":"thinking_delta","thinking":REASONING}),
        ))
        .expect(1..)
        .mount(&server)
        .await;

    let mut initial = bamboo(dir.path());
    gateway_environment(
        &mut initial,
        "anthropic",
        &format!("{}/initial/v1/", server.uri()),
    );
    let output = run(initial.args([
        "-p",
        "Read fixture.txt and answer.",
        "--stream-json",
        "--provider",
        "anthropic",
        "--permission-mode",
        "auto",
        "--timeout",
        "15",
    ]))
    .await;
    let events = ndjson_events(&output);
    let result = events.last().unwrap();
    assert_eq!(events[0]["type"], "session_started");
    assert!(result["session_id"].as_str().is_some());
    assert_eq!(events[0]["session_id"], result["session_id"]);
    assert!(events.iter().any(|event| event["type"] == "tool_complete"));
    assert_eq!(result["status"], "finished");
    assert_eq!(result["result"], ANSWER);
    assert_eq!(result["usage"], terminal_usage(312, 46));

    // A saved legacy session can have usage from a previous turn without an
    // Actor activation. Its counters must not carry into the next execution.
    let session_id = uuid::Uuid::new_v4().to_string();
    let mut session = Session::new(&session_id, MODEL);
    session.title_generated = true;
    session.add_message(Message::user("Previous prompt"));
    session.add_message(Message::assistant("Previous answer", None));
    let mut previous = AgentRuntimeState::new(&session_id);
    previous.status = AgentStatusState::Completed;
    previous.round.last_round_id = Some("previous-round".into());
    previous.round.total_prompt_tokens = 312;
    previous.round.total_completion_tokens = 46;
    session.agent_runtime_state = Some(previous);
    bamboo_storage::SessionStoreV2::new(data)
        .await
        .unwrap()
        .save_session(&session)
        .await
        .unwrap();

    let mut resume = bamboo(dir.path());
    gateway_environment(
        &mut resume,
        "anthropic",
        &format!("{}/resume/v1/", server.uri()),
    );
    let output = run_to_exit(resume.args([
        "-p",
        "Continue with a new answer.",
        "-s",
        &session_id,
        "--stream-json",
        "--provider",
        "anthropic",
        "--timeout",
        "15",
    ]))
    .await;
    assert_eq!(output.status.code(), Some(1));
    let events = ndjson_events(&output);
    let result = events.last().unwrap();
    assert_eq!(events[0]["type"], "session_started", "{events:#?}");
    assert_eq!(events[0]["resumed"], true);
    assert_eq!(result["session_id"], session_id);
    assert_eq!(result["status"], "error");
    assert!(result["error"]
        .as_str()
        .unwrap()
        .contains("output token limit"));
    let attempts = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.url.path() == "/resume/v1/messages")
        .count() as u64;
    assert!(attempts > 0);
    assert_eq!(
        result["usage"],
        terminal_usage(43 * attempts, 13 * attempts)
    );
}

#[tokio::test]
async fn headless_stream_startup_errors_emit_one_result_without_a_session() {
    let dir = tempfile::tempdir().unwrap();
    for (args, error_text, exit_code) in [
        (
            ["-p", "prompt", "--timeout", "0"].as_slice(),
            "invalid --timeout",
            1,
        ),
        (["-p", "-"].as_slice(), "empty prompt on stdin", 1),
        (
            ["-p", "prompt", "--log-level", "bogus"].as_slice(),
            "invalid --log-level",
            2,
        ),
    ] {
        let output = run_to_exit(bamboo(dir.path()).arg("--stream-json").args(args)).await;
        assert_eq!(output.status.code(), Some(exit_code));
        let events = ndjson_events(&output);
        assert_eq!(events.len(), 1);
        let result = &events[0];
        assert_eq!(result["status"], "error");
        assert!(result["session_id"].is_null());
        assert!(result["result"].is_null());
        assert!(result["error"].as_str().unwrap().contains(error_text));
        assert_eq!(result["usage"], terminal_usage(0, 0));
        assert!(String::from_utf8_lossy(&output.stderr).contains(error_text));
    }
}

#[tokio::test]
async fn headless_stream_execute_rejection_closes_an_announced_session() {
    let dir = tempfile::tempdir().unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let data = dir.path().join("bamboo");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(
        data.join("config.json"),
        json!({
            "default_provider_instance": "fixture",
            "provider_instances": {"fixture": {
                "provider_type": "anthropic",
                "api_key": KEY,
                "base_url": format!("{}/v1/", server.uri()),
            }},
        })
        .to_string(),
    )
    .unwrap();
    let output = run_to_exit(bamboo(dir.path()).args(["-p", "prompt", "--stream-json"])).await;
    assert_eq!(output.status.code(), Some(1));
    let events = ndjson_events(&output);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["type"], "session_started");
    assert!(events[0]["session_id"].as_str().is_some());
    assert_eq!(events[1]["session_id"], events[0]["session_id"]);
    assert_eq!(events[1]["status"], "error");
    assert!(events[1]["error"]
        .as_str()
        .unwrap()
        .starts_with("execute rejected: HTTP 400"));
    assert_eq!(events[1]["usage"], terminal_usage(0, 0));
}

#[tokio::test]
async fn headless_configures_each_supported_protocol_from_environment_alone() {
    struct Gateway {
        provider: &'static str,
        base_path: &'static str,
        request: Mock,
    }
    let gateways = [
        Gateway {
            provider: "openai",
            base_path: "/gateway/v1",
            request: Mock::given(path("/gateway/v1/chat/completions"))
                .and(header("authorization", format!("Bearer {KEY}").as_str()))
                .and(body_partial_json(json!({"model": MODEL})))
                .respond_with(openai_stream()),
        },
        Gateway {
            provider: "anthropic",
            base_path: "/gateway/v1",
            request: Mock::given(path("/gateway/v1/messages"))
                .and(header("x-api-key", KEY))
                .and(body_partial_json(json!({"model": MODEL})))
                .respond_with(anthropic_stream()),
        },
        Gateway {
            provider: "gemini",
            base_path: "/gateway/v1beta",
            request: Mock::given(path(format!("/gateway/v1beta/models/{MODEL}:streamGenerateContent")))
                .and(query_param("alt", "sse"))
                .and(header("x-goog-api-key", KEY))
                .respond_with(stream(format!(
                    "data: {}\n\n",
                    json!({"candidates":[{"content":{"parts":[{"text":ANSWER}]},"finishReason":"STOP"}]})
                ))),
        },
    ];

    for gateway in gateways {
        let dir = tempfile::tempdir().unwrap();
        let server = MockServer::start().await;
        gateway.request.expect(1..).mount(&server).await;
        let endpoint = format!("{}{}/", server.uri(), gateway.base_path);
        let prompt = dir.path().join("prompt.txt");
        std::fs::write(&prompt, "Give a concise answer.\n").unwrap();
        let mut command = bamboo(dir.path());
        gateway_environment(&mut command, gateway.provider, &endpoint);
        command
            .args(["-p", "-", "--timeout", "15"])
            .stdin(File::open(&prompt).unwrap());
        if gateway.provider == "gemini" {
            command.env("BAMBOO_PROVIDER", "gemini");
        } else {
            command.args(["--provider", gateway.provider]);
        }
        assert!(!dir.path().join("bamboo/config.json").exists());
        let output = run(&mut command).await;
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), ANSWER);
        if gateway.provider == "openai" {
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(stderr.contains("[thinking: "), "{stderr}");
            assert!(!stderr.contains(REASONING), "{stderr}");
            let output = run(command
                .arg("--stream-json")
                .stdin(File::open(&prompt).unwrap()))
            .await;
            let events: Vec<Value> = String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(events[0]["type"], "session_started");
            assert!(events.iter().any(|event| {
                event["type"] == "reasoning_token" && event["content"] == REASONING
            }));
            assert_eq!(events.last().unwrap()["result"], ANSWER);
            assert!(!String::from_utf8_lossy(&output.stderr).contains("[thinking: "));
        }
        let saved =
            bamboo_config::Config::from_data_dir_without_env(Some(dir.path().join("bamboo")));
        assert!(saved.provider_instances.values().all(|instance| {
            instance.base_url.is_none() && instance.model.is_none() && instance.api_key.is_empty()
        }));
        assert!(!saved
            .to_compatibility_value()
            .unwrap()
            .to_string()
            .contains("/gateway/"));
    }
}

#[tokio::test]
async fn doctor_accepts_runtime_credentials_and_model_without_a_config_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut command = bamboo(dir.path());
    gateway_environment(&mut command, "gemini", "http://127.0.0.1:1/v1beta");
    let output = run(command
        .env("BAMBOO_PROVIDER", "gemini")
        .env("BAMBOO_PORT", "0")
        .arg("doctor"))
    .await;
    assert!(String::from_utf8_lossy(&output.stdout).contains("default model = fixture-model"));
}

#[tokio::test]
async fn invalid_env_endpoint_is_redacted_and_prevents_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(openai_stream())
        .expect(0)
        .mount(&server)
        .await;
    let data = dir.path().join("bamboo");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(
        data.join("config.json"),
        json!({
            "default_provider_instance": "openai",
            "provider_instances": {"openai": {
                "provider_type": "openai", "api_key_from_env": true,
                "base_url": server.uri(), "model": MODEL,
            }},
        })
        .to_string(),
    )
    .unwrap();
    let secret = "fixture-url-secret";
    let endpoint = format!("http://gateway-user:{secret}@127.0.0.1:1/v1");
    let mut inspect = bamboo(dir.path());
    gateway_environment(&mut inspect, "openai", &endpoint);
    let output = run(inspect.arg("config")).await;
    assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));

    let mut command = bamboo(dir.path());
    gateway_environment(&mut command, "openai", &endpoint);
    let output = run_to_exit(command.env("RUST_LOG", "warn").args([
        "-p",
        "Give a concise answer.",
        "--provider",
        "openai",
        "--timeout",
        "15",
    ]))
    .await;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("BAMBOO_OPENAI_BASE_URL"));
    assert!(!stderr.contains(secret));
    assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
}

#[tokio::test]
async fn init_saves_an_endpoint_and_model_for_the_next_headless_run() {
    let _key = bamboo_config::encryption::set_test_encryption_key(ENCRYPTION_KEY);
    let dir = tempfile::tempdir().unwrap();
    let server = MockServer::start().await;
    let endpoint = format!("{}/custom/v1", server.uri());
    Mock::given(method("POST"))
        .and(path("/custom/v1/chat/completions"))
        .and(header("authorization", format!("Bearer {KEY}").as_str()))
        .and(body_partial_json(json!({"model": MODEL})))
        .respond_with(openai_stream())
        .expect(1..)
        .mount(&server)
        .await;
    run(bamboo(dir.path())
        .env("BAMBOO_OPENAI_MODEL", "runtime-override")
        .env("BAMBOO_OPENAI_BASE_URL", "http://127.0.0.1:1")
        .args([
            "init",
            "--non-interactive",
            "--provider",
            "openai",
            "--api-key",
            KEY,
            "--base-url",
            &endpoint,
            "--model",
            MODEL,
        ]))
    .await;
    let output =
        run(bamboo(dir.path()).args(["-p", "Give a concise answer.", "--timeout", "15"])).await;
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), ANSWER);
    let data_dir = dir.path().join("bamboo");
    let prior = bamboo_config::Config::from_data_dir_without_env(Some(data_dir.clone()));
    let reference = prior.provider_instances["openai"]
        .credential_ref
        .as_ref()
        .unwrap();
    let store = bamboo_config::CredentialStore::open(&data_dir);
    store.clear(reference, store.revision().unwrap()).unwrap();
    assert!(!store.status(reference).unwrap().configured);
    let updated = format!("{}/updated/v1", server.uri());
    Mock::given(method("POST"))
        .and(path("/updated/v1/chat/completions"))
        .and(header("authorization", format!("Bearer {KEY}").as_str()))
        .and(body_partial_json(json!({"model": "updated-model"})))
        .respond_with(openai_stream())
        .expect(1..)
        .mount(&server)
        .await;
    run(bamboo(dir.path()).args([
        "init",
        "--non-interactive",
        "--force",
        "--provider",
        "openai",
        "--api-key",
        KEY,
        "--base-url",
        &updated,
        "--model",
        "updated-model",
    ]))
    .await;
    let output =
        run(bamboo(dir.path()).args(["-p", "Give a concise answer.", "--timeout", "15"])).await;
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), ANSWER);
    let saved = bamboo_config::Config::from_data_dir_without_env(Some(data_dir));
    let instance = &saved.provider_instances["openai"];
    assert_eq!(instance.base_url.as_deref(), Some(updated.as_str()));
    assert_eq!(instance.model.as_deref(), Some("updated-model"));
    assert!(instance.credential_ref.is_some());
    assert_eq!(instance.api_key, KEY);
    assert!(!saved
        .to_compatibility_value()
        .unwrap()
        .to_string()
        .contains(KEY));
}

#[tokio::test]
async fn a_saved_bodhi_gateway_can_be_selected_by_instance_and_model() {
    let dir = tempfile::tempdir().unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/proxy/openai/v1/chat/completions"))
        .and(header("authorization", format!("Bearer {KEY}").as_str()))
        .and(body_partial_json(json!({"model": MODEL})))
        .respond_with(openai_stream())
        .expect(1..)
        .mount(&server)
        .await;
    let data = dir.path().join("bamboo");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(
        data.join("config.json"),
        json!({
            "default_provider_instance":"relay",
            "provider_instances":{"relay":{
                "provider_type":"bodhi","base_url":server.uri(),"target_provider":"openai",
                "api_key":KEY,"model":"saved-default","runtime_models":["saved-default",MODEL],
            }},
        })
        .to_string(),
    )
    .unwrap();
    let output = run(bamboo(dir.path()).args([
        "-p",
        "Give a concise answer.",
        "--model",
        "relay:fixture-model",
        "--timeout",
        "15",
    ]))
    .await;
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), ANSWER);
}

#[tokio::test]
async fn init_keeps_all_accounts_in_a_legacy_hybrid_config() {
    let _key = bamboo_config::encryption::set_test_encryption_key(ENCRYPTION_KEY);
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("bamboo");
    std::fs::create_dir(&data_dir).unwrap();
    std::fs::write(
        data_dir.join("config.json"),
        json!({
            "provider": "openai",
            "providers": {
                "openai": {"api_key": "fixture-openai", "model": "previous-openai", "base_url": "http://127.0.0.1:1/openai"},
                "anthropic": {"api_key": "fixture-anthropic", "model": "previous-anthropic"},
            },
            "provider_instances": {
                "work": {"provider_type": "openai", "api_key": "fixture-work", "model": "previous-work"},
            },
        }).to_string(),
    ).unwrap();
    run(bamboo(dir.path()).args([
        "init",
        "--non-interactive",
        "--provider",
        "gemini",
        "--api-key",
        KEY,
        "--model",
        MODEL,
    ]))
    .await;
    let saved = bamboo_config::Config::from_data_dir_without_env(Some(data_dir));
    assert_eq!(saved.effective_default_provider(), "gemini");
    for (id, model, key) in [
        ("openai", "previous-openai", "fixture-openai"),
        ("anthropic", "previous-anthropic", "fixture-anthropic"),
        ("work", "previous-work", "fixture-work"),
        ("gemini", MODEL, KEY),
    ] {
        let account = &saved.provider_instances[id];
        assert_eq!(account.model.as_deref(), Some(model), "{id}");
        assert_eq!(account.api_key, key, "{id}");
        assert!(account.credential_ref.is_some(), "{id}");
    }
    assert_eq!(
        saved.provider_instances["openai"].base_url.as_deref(),
        Some("http://127.0.0.1:1/openai"),
    );
}
