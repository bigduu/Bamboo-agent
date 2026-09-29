//! Real serve/current_exe Child question round trip. Only the provider is simulated.
#![cfg(unix)]

use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::{ParentQuestion, Role, SessionMessageEnvelope};
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

const ROOT_ID: &str = "parent-question-root";
const QUESTION: &str = "Which color should I use?";
const ANSWER: &str = "Blue";

struct Probe {
    data: PathBuf,
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    first_child_ready: AtomicBool,
    release_first_child: AtomicBool,
    second_child_ready: AtomicBool,
    release_second_child: AtomicBool,
    wake: tokio::sync::Notify,
}

fn tool_call(id: &str, args: Value) -> (Value, &'static str) {
    (
        json!({"tool_calls":[{"index":0,"id":id,"type":"function",
            "function":{"name":"SubAgent","arguments":args.to_string()}}]}),
        "tool_calls",
    )
}

fn question_in_parent(parent: &bamboo_domain::Session) -> Vec<ParentQuestion> {
    parent
        .messages
        .iter()
        .filter_map(|message| {
            let marker = message.metadata.as_ref()?.get("session_message")?;
            let envelope: SessionMessageEnvelope = serde_json::from_value(marker.clone()).ok()?;
            ParentQuestion::from_envelope(&envelope)
        })
        .collect()
}

async fn provider(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let body = body.into_inner();
    let (delta, finish) = if body["model"] == "question-child" {
        let round = probe.child_calls.fetch_add(1, Ordering::SeqCst);
        let tools = body["tools"].as_array().expect("Child has tools");
        assert!(tools
            .iter()
            .any(|tool| tool["function"]["name"] == "SubAgent"));
        match round {
            0 => {
                probe.first_child_ready.store(true, Ordering::SeqCst);
                tokio::time::timeout(Duration::from_secs(60), async {
                    while !probe.release_first_child.load(Ordering::SeqCst) {
                        probe.wake.notified().await;
                    }
                })
                .await
                .expect("Root wait precedes Child question");
                tool_call(
                    "child-ask-parent",
                    json!({"intent":"ask_parent","message":QUESTION}),
                )
            }
            1 => {
                let answers: Vec<_> = body["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|message| {
                        message["role"] == "tool"
                            && message["tool_call_id"] == "child-ask-parent"
                            && message["content"] == ANSWER
                    })
                    .collect();
                assert_eq!(answers.len(), 1, "Child sees one canonical answer");
                probe.second_child_ready.store(true, Ordering::SeqCst);
                tokio::time::timeout(Duration::from_secs(60), async {
                    while !probe.release_second_child.load(Ordering::SeqCst) {
                        probe.wake.notified().await;
                    }
                })
                .await
                .expect("Parent wait rearm precedes Child completion");
                (json!({"content":"CHILD_USED_BLUE"}), "stop")
            }
            _ => panic!("Child must resume once after the answer"),
        }
    } else if body["model"] == "question-root" && body["tools"].to_string().contains("SubAgent") {
        match probe.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => tool_call(
                "root-create-child",
                json!({"role":"worker","message":"Ask your direct parent which color to use, then return CHILD_USED_BLUE."}),
            ),
            1 => {
                assert!(body.to_string().contains("root-create-child"));
                (json!({"content":"ROOT_WAITING_FOR_CHILD"}), "stop")
            }
            2 => {
                let store = SessionStoreV2::new(probe.data.clone()).await.unwrap();
                let parent = store.load_session(ROOT_ID).await.unwrap().unwrap();
                let requests = question_in_parent(&parent);
                assert_eq!(requests.len(), 1, "one durable direct-parent request");
                assert_eq!(requests[0].question, QUESTION);
                assert!(
                    body.to_string().contains(requests[0].id.as_str()),
                    "model context includes the exact reply_to id"
                );
                tool_call(
                    "root-answer-child",
                    json!({"reply_to":requests[0].id.as_str(),"message":ANSWER}),
                )
            }
            3 => {
                assert!(body.to_string().contains("root-answer-child"));
                (json!({"content":"ROOT_REPLY_SENT"}), "stop")
            }
            4 => {
                assert!(body.to_string().contains("CHILD_USED_BLUE"));
                (json!({"content":"ROOT_SAW_CHILD_COMPLETION"}), "stop")
            }
            _ => panic!("Parent must answer once and receive one Child outcome"),
        }
    } else {
        (json!({"content":"auxiliary"}), "stop")
    };
    let event = json!({"id":"parent-question","object":"chat.completion.chunk",
        "choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}

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

#[actix_web::test]
async fn direct_parent_question_rearms_original_wait_and_resumes_same_child() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let data = root.join("host");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&workspace).unwrap();
    let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
    let project = projects
        .create_with_project_path("question", None, workspace.to_string_lossy(), vec![])
        .unwrap();
    let probe = web::Data::new(Probe {
        data: data.clone(),
        root_calls: AtomicUsize::new(0),
        child_calls: AtomicUsize::new(0),
        first_child_ready: AtomicBool::new(false),
        release_first_child: AtomicBool::new(false),
        second_child_ready: AtomicBool::new(false),
        release_second_child: AtomicBool::new(false),
        wake: tokio::sync::Notify::new(),
    });
    let server_probe = probe.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(server_probe.clone())
            .route("/v1/chat/completions", web::post().to(provider))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok()
                        .json(json!({"data":[{"id":"question-root"},{"id":"question-child"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let provider_url = format!("http://{}/v1", server.addrs()[0]);
    let running = server.run();
    let server_handle = running.handle();
    actix_web::rt::spawn(running);
    std::fs::write(data.join("config.json"), serde_json::to_vec(&json!({"provider":"openai","features":{"provider_model_ref":true},"providers":{"openai":{"api_key":"fixture","base_url":provider_url,"model":"question-root"}},"defaults":{"chat":{"provider":"openai","model":"question-root"},"subagent_models":{"worker":{"provider":"openai","model":"question-child"}}},"subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1}})).unwrap()).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut host = start_host(&data, port);
    let base = format!("http://127.0.0.1:{port}/api/v1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(host.0.try_wait().unwrap().is_none(), "Host exited");
            if client
                .get(format!("{base}/health"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    let chat = client
        .post(format!("{base}/chat"))
        .json(&json!({"session_id":ROOT_ID,"message":"Delegate the color task and wait for its result.","model":"question-root","provider":"openai","model_ref":{"provider":"openai","model":"question-root"},"thinking_mode":"ultra","permission_mode":"bypass","workspace_path":workspace,"project_id":project.id}))
        .send()
        .await
        .unwrap();
    assert!(
        chat.status().is_success(),
        "chat: {}",
        chat.text().await.unwrap()
    );
    assert!(client
        .post(format!("{base}/execute/{ROOT_ID}"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    let store = SessionStoreV2::new(data.clone()).await.unwrap();
    let first_cut = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let parent = store.load_session(ROOT_ID).await.unwrap().unwrap();
            if let Some(wait) = parent
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.waiting_for_children.as_ref())
            {
                if parent.last_run_status().as_deref() == Some("suspended")
                    && probe.first_child_ready.load(Ordering::SeqCst)
                {
                    assert_eq!(wait.child_session_ids.len(), 1);
                    let fresh = SessionStoreV2::new(data.clone()).await.unwrap();
                    let child = fresh
                        .load_session(&wait.child_session_ids[0])
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(wait.child_session_ids, [child.id.clone()]);
                    break (
                        child.id,
                        child.created_at,
                        serde_json::to_value(wait).unwrap(),
                    );
                }
            }
            assert_ne!(
                parent.last_run_status().as_deref(),
                Some("error"),
                "Root: {:?}",
                parent.last_run_error()
            );
            assert!(host.0.try_wait().unwrap().is_none(), "Host exited");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let (child_id, child_birth, original_wait) = match first_cut {
        Ok(cut) => cut,
        Err(_) => {
            let parent = store.load_session(ROOT_ID).await.unwrap().unwrap();
            let child_rows: Vec<_> = store
                .list_index_entries()
                .await
                .into_iter()
                .filter(|entry| entry.parent_session_id.as_deref() == Some(ROOT_ID))
                .map(|entry| (entry.id, entry.last_run_status))
                .collect();
            let log = std::fs::read_to_string(data.join("host.log")).unwrap_or_default();
            let tail = log.lines().rev().take(20).collect::<Vec<_>>();
            panic!(
                "Root wait cut: root_calls={} child_calls={} first_child_ready={} root_status={:?} wait={:?} children={child_rows:?} root_messages={:?} host_log_tail={tail:?}",
                probe.root_calls.load(Ordering::SeqCst),
                probe.child_calls.load(Ordering::SeqCst),
                probe.first_child_ready.load(Ordering::SeqCst),
                parent.last_run_status(),
                parent.agent_runtime_state.as_ref().and_then(|state| state.waiting_for_children.as_ref()),
                parent.messages.iter().map(|message| (&message.role, &message.content)).collect::<Vec<_>>(),
            );
        }
    };
    // The first Store was opened before the Child was created. Reopen its
    // index so subsequent Child reads use the published canonical path.
    let store = SessionStoreV2::new(data.clone()).await.unwrap();
    probe.release_first_child.store(true, Ordering::SeqCst);
    probe.wake.notify_waiters();
    let second_cut = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let child = store.load_session(&child_id).await.unwrap().unwrap();
            let parent = store.load_session(ROOT_ID).await.unwrap().unwrap();
            let requests = question_in_parent(&parent);
            assert!(requests.len() <= 1, "stable question delivery id");
            if probe.second_child_ready.load(Ordering::SeqCst)
                && parent.last_run_status().as_deref() == Some("suspended")
                && parent
                    .agent_runtime_state
                    .as_ref()
                    .and_then(|state| state.waiting_for_children.as_ref())
                    .is_some_and(|wait| {
                        let value = serde_json::to_value(wait).unwrap();
                        value == original_wait
                    })
            {
                assert_eq!(requests.len(), 1);
                assert_eq!(requests[0].child.session_id, child_id);
                assert_eq!(requests[0].question, QUESTION);
                assert_eq!(child.created_at, child_birth);
                assert!(child.pending_question.is_none());
                assert_eq!(child.last_run_status().as_deref(), Some("running"));
                assert!(child
                    .messages
                    .iter()
                    .any(|message| message.role == Role::Tool
                        && message.tool_call_id.as_deref() == Some("child-ask-parent")
                        && message.content == ANSWER));
                assert!(!child
                    .messages
                    .iter()
                    .any(|message| message.content == "CHILD_USED_BLUE"));
                break;
            }
            assert_ne!(
                child.last_run_status().as_deref(),
                Some("error"),
                "Child: {:?}",
                child.last_run_error()
            );
            assert_ne!(
                parent.last_run_status().as_deref(),
                Some("error"),
                "Root: {:?}",
                parent.last_run_error()
            );
            if let Some(exit) = host.0.try_wait().unwrap() {
                let log = std::fs::read_to_string(data.join("host.log")).unwrap_or_default();
                panic!(
                    "Host exited {exit}: root_calls={} child_calls={} child_status={:?} pending={:?} parent_status={:?} requests={} host_log_tail={:?}",
                    probe.root_calls.load(Ordering::SeqCst),
                    probe.child_calls.load(Ordering::SeqCst),
                    child.last_run_status(),
                    child.pending_question,
                    parent.last_run_status(),
                    requests.len(),
                    log.lines().rev().take(35).collect::<Vec<_>>(),
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if second_cut.is_err() {
        let child = store.load_session(&child_id).await.unwrap().unwrap();
        let parent = store.load_session(ROOT_ID).await.unwrap().unwrap();
        let log = std::fs::read_to_string(data.join("host.log")).unwrap_or_default();
        panic!(
            "question continuation cut: root_calls={} child_calls={} child_status={:?} pending={:?} resolution={:?} parent_status={:?} wait={:?} original_wait={original_wait:?} requests={:?} parent_messages={:?} host_log_tail={:?}",
            probe.root_calls.load(Ordering::SeqCst),
            probe.child_calls.load(Ordering::SeqCst),
            child.last_run_status(),
            child.pending_question,
            child.metadata.get(bamboo_domain::PARENT_QUESTION_RESOLUTION_KEY),
            parent.last_run_status(),
            parent.agent_runtime_state.as_ref().and_then(|state| state.waiting_for_children.as_ref()),
            question_in_parent(&parent).iter().map(|request| request.id.to_string()).collect::<Vec<_>>(),
            parent.messages.iter().filter(|message| message.role != Role::System).map(|message| (&message.role, &message.content)).collect::<Vec<_>>(),
            log.lines().rev().take(30).collect::<Vec<_>>(),
        );
    }
    probe.release_second_child.store(true, Ordering::SeqCst);
    probe.wake.notify_waiters();
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let child = store.load_session(&child_id).await.unwrap().unwrap();
            let parent = store.load_session(ROOT_ID).await.unwrap().unwrap();
            if child.last_run_status().as_deref() == Some("completed")
                && parent.last_run_status().as_deref() == Some("completed")
                && parent
                    .messages
                    .iter()
                    .any(|message| message.content == "ROOT_SAW_CHILD_COMPLETION")
            {
                assert_eq!(child.created_at, child_birth);
                assert!(parent
                    .agent_runtime_state
                    .as_ref()
                    .is_none_or(|state| state.waiting_for_children.is_none()));
                assert_eq!(probe.child_calls.load(Ordering::SeqCst), 2);
                assert_eq!(probe.root_calls.load(Ordering::SeqCst), 5);
                let answers: Vec<_> = parent
                    .messages
                    .iter()
                    .filter(|message| {
                        message.role == Role::Assistant
                            && message.tool_calls.as_ref().is_some_and(|calls| {
                                calls.iter().any(|call| call.id == "root-answer-child")
                            })
                    })
                    .collect();
                assert_eq!(answers.len(), 1, "one direct-parent reply tool call");
                break;
            }
            assert_ne!(
                child.last_run_status().as_deref(),
                Some("error"),
                "Child: {:?}",
                child.last_run_error()
            );
            assert_ne!(
                parent.last_run_status().as_deref(),
                Some("error"),
                "Root: {:?}",
                parent.last_run_error()
            );
            assert!(host.0.try_wait().unwrap().is_none(), "Host exited");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("same Child must complete and wake the original Parent wait once");
    drop(host);
    let cold = SessionStoreV2::new(data).await.unwrap();
    let child = cold.load_session(&child_id).await.unwrap().unwrap();
    assert_eq!(child.created_at, child_birth);
    assert!(child
        .messages
        .iter()
        .any(|message| message.content == "CHILD_USED_BLUE"));
    server_handle.stop(true).await;
}
