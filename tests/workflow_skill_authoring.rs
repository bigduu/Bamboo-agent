//! Exercise the packaged Workflow Skill through the real host and named Child.
#![cfg(any(target_os = "linux", target_os = "macos"))]

use reqwest::{Client, Response, StatusCode};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, Request, ResponseTemplate,
};

const SESSION: &str = "workflow-teaching-session";
const MODEL: &str = "workflow-teaching-fixture";

struct Host(Child);

impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_host(data: &Path, port: u16) -> Host {
    let log = std::fs::File::create(data.join("host.log")).unwrap();
    Host(
        Command::new(env!("CARGO_BIN_EXE_bamboo"))
            .args([
                "serve",
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--data-dir",
            ])
            .arg(data)
            .arg("--static-dir")
            .arg(data.join("frontend"))
            .current_dir(data)
            .env("HOME", data.join("home"))
            .env("BAMBOO_JIANDU_DATA_DIR", data.join("jiandu"))
            .env("RUST_LOG", "warn")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}

fn provider_response(request: &Request, files: &[PathBuf; 2]) -> ResponseTemplate {
    let body = String::from_utf8_lossy(&request.body);
    let content = if let Some(index) = ["DRAFT_FIXTURE_ONE", "DRAFT_FIXTURE_TWO"]
        .iter()
        .rposition(|marker| body.contains(marker))
    {
        let draft = json!({
            "decision":"draft", "description":"Read the project fixture",
            "definition":{
                "workflow_schema":1, "id":"read-project-fixture", "revision":index + 1,
                "steps":[{"id":"inspect","type":"tool","tool":"Read","capabilities":["read"],
                    "args":{"file_path":files[index]}}],
                "plan":{"type":"step","step":"inspect"}
            }
        });
        json!({"version":1,"outcome":"completed","summary":draft.to_string(),
            "reported_evidence":[],"reported_verification":[],"proposals":[],"blockers":[],"open_decisions":[]})
            .to_string()
    } else {
        "Workflow fixture".into()
    };
    let event = json!({
        "id":"workflow-teaching", "object":"chat.completion.chunk", "model":MODEL,
        "choices":[{"index":0,"delta":{"role":"assistant","content":content},"finish_reason":"stop"}],
        "usage":{"prompt_tokens":30,"completion_tokens":40,"total_tokens":70},
    });
    ResponseTemplate::new(200)
        .insert_header("Content-Type", "text/event-stream")
        .set_body_string(format!("data: {event}\n\ndata: [DONE]\n\n"))
}

async fn expect_json(response: Response, status: StatusCode) -> Value {
    let actual = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(actual, status, "{text}");
    serde_json::from_str(&text).unwrap()
}

async fn wait_finished(client: &Client, url: &str, data: &Path) -> Value {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let body = expect_json(client.get(url).send().await.unwrap(), StatusCode::OK).await;
            match body["status"].as_str() {
                Some("succeeded") => break body,
                Some("queued" | "running") => tokio::time::sleep(Duration::from_millis(50)).await,
                _ => panic!(
                    "{body}\n{}",
                    std::fs::read_to_string(data.join("host.log")).unwrap()
                ),
            }
        }
    })
    .await
    .expect("workflow must settle")
}

#[tokio::test]
async fn workflow_skill_teaching_runs_the_curator_and_publishes_only_accepted_revisions() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().canonicalize().unwrap();
    let workspace = data.join("source");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(data.join("home")).unwrap();
    std::fs::create_dir(data.join("frontend")).unwrap();
    std::fs::write(data.join("frontend/index.html"), "<!doctype html>").unwrap();
    let files = [workspace.join("first.txt"), workspace.join("second.txt")];
    for (index, file) in files.iter().enumerate() {
        std::fs::write(file, format!("fixture {index}")).unwrap();
    }
    let provider = MockServer::start().await;
    let provider_files = files.clone();
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(move |request: &Request| provider_response(request, &provider_files))
        .mount(&provider)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data":[{"id":MODEL}]})))
        .mount(&provider)
        .await;
    std::fs::write(data.join("config.json"), json!({
        "provider":"openai","features":{"provider_model_ref":true},
        "providers":{"openai":{"api_key":"fixture-key","base_url":format!("{}/v1",provider.uri()),"model":MODEL,"runtime_models":[MODEL]}},
        "defaults":{"chat":{"provider":"openai","model":MODEL}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1},
    }).to_string()).unwrap();
    let port = std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut host = start_host(&data, port);
    let origin = format!("http://127.0.0.1:{port}");
    let base = format!("{origin}/api/v1");
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "{}",
                std::fs::read_to_string(data.join("host.log")).unwrap()
            );
            if client
                .get(format!("{origin}/readyz"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Host readiness timed out\n{}",
            std::fs::read_to_string(data.join("host.log")).unwrap()
        )
    });
    let project = expect_json(
        client
            .post(format!("{base}/projects"))
            .json(&json!({"name":"Workflow fixture","project_path":workspace}))
            .send()
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    let project_id = project["id"].as_str().unwrap();
    expect_json(client.post(format!("{base}/chat")).json(&json!({
        "session_id":SESSION,"message":"Workflow authoring fixture","project_id":project_id,"workspace_path":workspace,
        "model":MODEL,"model_ref":{"provider":"openai","model":MODEL},"provider":"openai",
    })).send().await.unwrap(), StatusCode::CREATED).await;

    let proposals = format!("{base}/sessions/{SESSION}/workflow-proposals");
    let definitions = format!("{base}/bamboo/workflow-definitions");
    let runs = format!("{base}/sessions/{SESSION}/workflow-runs");
    let authored_url = format!("{definitions}/read-project-fixture");
    let read_url = format!("{authored_url}?target=project&session_id={SESSION}");
    let mut previous_run = None;
    for revision in 1..=2 {
        let teaching = if revision == 1 {
            json!({"instruction":"DRAFT_FIXTURE_ONE. Read the first fixture."})
        } else {
            json!({"instruction":"DRAFT_FIXTURE_TWO. Read the second fixture instead.","workflow_id":"read-project-fixture"})
        };
        let started = expect_json(
            client
                .post(&proposals)
                .json(&teaching)
                .send()
                .await
                .unwrap(),
            StatusCode::ACCEPTED,
        )
        .await;
        let proposal_url = format!("{proposals}/{}", started["run_id"].as_str().unwrap());
        let body = wait_finished(&client, &proposal_url, &data).await;
        let proposal = &body["result"];
        assert_eq!(proposal["decision"], "draft");
        assert_eq!(proposal["project_id"], project_id);
        assert_eq!(proposal["definition"]["revision"], revision);
        assert!(body.get("prompt").is_none());

        let mut publication = json!({"target":"project","session_id":SESSION,"expected_project_id":project_id,
            "definition":proposal["definition"]});
        let saved = if revision == 1 {
            assert_eq!(
                client.get(&read_url).send().await.unwrap().status(),
                StatusCode::NOT_FOUND
            );
            assert!(proposal["base"].is_null());
            publication["description"] = proposal["description"].clone();
            expect_json(
                client
                    .post(&definitions)
                    .json(&publication)
                    .send()
                    .await
                    .unwrap(),
                StatusCode::CREATED,
            )
            .await
        } else {
            let still_saved =
                expect_json(client.get(&read_url).send().await.unwrap(), StatusCode::OK).await;
            assert_eq!(still_saved["definition"]["revision"], 1);
            assert_eq!(proposal["base"]["definition"], still_saved["definition"]);
            publication["expected_revision"] = json!(1);
            publication["expected_digest"] = proposal["base"]["content_digest"].clone();
            publication["expected_skill_digest"] = proposal["base"]["skill_digest"].clone();
            let saved = expect_json(
                client
                    .put(&authored_url)
                    .json(&publication)
                    .send()
                    .await
                    .unwrap(),
                StatusCode::OK,
            )
            .await;
            assert_eq!(
                client
                    .put(&authored_url)
                    .json(&publication)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::CONFLICT
            );
            saved
        };
        assert_eq!(saved["catalog_available"], true);
        let run = expect_json(
            client
                .post(&runs)
                .json(&json!({
                    "workflow_id":"read-project-fixture", "revision":revision, "args":{},
                }))
                .send()
                .await
                .unwrap(),
            StatusCode::ACCEPTED,
        )
        .await;
        let url = format!("{runs}/{}", run["run_id"].as_str().unwrap());
        let finished = wait_finished(&client, &url, &data).await;
        assert_eq!(finished["workflow_revision"], revision);
        if let Some(previous) = previous_run {
            assert_eq!(
                expect_json(client.get(previous).send().await.unwrap(), StatusCode::OK).await
                    ["workflow_revision"],
                1
            );
        }
        previous_run = Some(url);
    }
}
