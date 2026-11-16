//! Exercise CLI setup and output against the real binary and loopback provider APIs.

use std::fs::File;
use std::path::Path;
use std::process::{Output, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::process::Command;
use wiremock::matchers::{body_partial_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const MODEL: &str = "fixture-model";
const KEY: &str = "synthetic-api-key";
const ANSWER: &str = "Fixture answer";
const REASONING: &str = "fixture private thought";

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
    let output = tokio::time::timeout(Duration::from_secs(35), command.output())
        .await
        .expect("Bamboo must exit")
        .expect("launch the Bamboo binary");
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
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
async fn init_saves_an_endpoint_and_model_for_the_next_headless_run() {
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
    let updated = format!("{}/updated/v1", server.uri());
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
    let saved = bamboo_config::Config::from_data_dir_without_env(Some(dir.path().join("bamboo")));
    let instance = &saved.provider_instances["openai"];
    assert_eq!(instance.base_url.as_deref(), Some(updated.as_str()));
    assert_eq!(instance.model.as_deref(), Some("updated-model"));
    assert!(instance.credential_ref.is_some());
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
