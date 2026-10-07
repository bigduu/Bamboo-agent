//! Real CLI Host → default current_exe worker. Only the model endpoint is fake.
#![cfg(unix)]
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::{Role, Session, SessionInboxLimits, SessionInboxPort};
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};

const TASK: &str = "  Inspect the compact-marker.txt file with Glob 🪷\n\nKeep this complete task, including the trailing spaces.  ";
const CORRECTION: &str = "  Keep the same logical Child; report COMPACT_CORRECTION_DONE.  ";
struct Probe {
    data: PathBuf,
    workspace: PathBuf,
    role: Option<&'static str>,
    fast_grandchild: bool,
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    grandchild_parent: Mutex<Option<String>>,
    requests: Mutex<Vec<Value>>,
}
fn call(id: &str, name: &str, args: Value) -> (Value, &'static str) {
    (
        json!({"tool_calls":[{"index":0,"id":id,"type":"function","function":{
        "name":name,"arguments":args.to_string()}}]}),
        "tool_calls",
    )
}
fn tool_content(body: &Value, id: &str) -> String {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "tool" && message["tool_call_id"] == id)
        .and_then(|message| message["content"].as_str())
        .unwrap()
        .into()
}
fn logical_child(body: &Value) -> String {
    let content = tool_content(body, "compact-create");
    let result: Value = serde_json::from_str(&content).unwrap_or_else(|error| {
        let preview: String = content.chars().take(512).collect();
        panic!("compact-create did not return JSON: {error}; actual tool result: {preview}");
    });
    result["actor_id"].as_str().unwrap().into()
}
fn logical_grandchild(body: &Value) -> String {
    let content = tool_content(body, "grandchild-create");
    let result: Value = serde_json::from_str(&content).unwrap_or_else(|error| {
        panic!("grandchild create did not return JSON: {error}; {content}")
    });
    result["child_session_id"].as_str().unwrap().into()
}
async fn response(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let body = body.into_inner();
    probe.requests.lock().unwrap().push(body.clone());
    let is_grandchild = body["messages"].as_array().is_some_and(|messages| {
        messages.iter().any(|message| {
            message["role"] == "user"
                && message["content"].as_str().is_some_and(|content| {
                    content.contains("Sub-session title: Nested running Grandchild")
                })
        })
    });
    let (delta, finish) = if probe.role.is_none() && is_grandchild {
        let parent_id = probe.grandchild_parent.lock().unwrap().clone().unwrap();
        let suspended = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let disk = SessionStoreV2::new(probe.data.clone()).await.unwrap();
                let parent = disk.load_session(&parent_id).await.unwrap().unwrap();
                let wait_registered = parent
                    .agent_runtime_state
                    .as_ref()
                    .and_then(|state| state.waiting_for_children.as_ref())
                    .is_some();
                if (probe.fast_grandchild && wait_registered)
                    || (!probe.fast_grandchild
                        && parent.last_run_status().as_deref() == Some("suspended"))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        if suspended.is_err() {
            let disk = SessionStoreV2::new(probe.data.clone()).await.unwrap();
            let parent = disk.load_session(&parent_id).await.unwrap().unwrap();
            let grandchildren = disk
                .list_index_entries()
                .await
                .into_iter()
                .filter(|entry| entry.parent_session_id.as_deref() == Some(parent_id.as_str()))
                .map(|entry| (entry.id, entry.last_run_status))
                .collect::<Vec<_>>();
            let log = std::fs::read_to_string(probe.data.join("host.log")).unwrap_or_default();
            panic!("canonical Child did not suspend: status={:?}, error={:?}, wait={:?}, reason={:?}, grandchildren={:?}, log_tail={}",
                parent.last_run_status(), parent.last_run_error(),
                parent.agent_runtime_state.as_ref().and_then(|state| state.waiting_for_children.as_ref()),
                parent.metadata.get("runtime.suspend_reason"),
                grandchildren,
                log.chars().rev().take(3500).collect::<String>().chars().rev().collect::<String>());
        }
        (json!({"content":"GRANDCHILD_DONE"}), "stop")
    } else if body["model"] == "compact-child" {
        let tool_names = body["tools"]
            .as_array()
            .expect("actual Child provider tools array")
            .iter()
            .map(|tool| {
                tool["function"]["name"]
                    .as_str()
                    .expect("registered Child provider execution name")
            })
            .collect::<std::collections::BTreeSet<_>>();
        if probe.role.is_none() {
            assert!(
                tool_names.contains("SubAgent"),
                "ordinary Child can delegate"
            );
        }
        for physical in ["ask_agent", "deploy_agent", "cluster"] {
            assert!(!tool_names.contains(physical), "Child model saw {physical}");
        }
        match (probe.role, probe.child_calls.fetch_add(1, Ordering::SeqCst)) {
            (None, 0) => {
                // The isolated Worker has no Root ancestor, so a successful
                // lineage-checked tree page must come from canonical Host storage.
                call("compact-tree", "SubAgent", json!({"intent":"inspect"}))
            }
            (None, 1) => {
                let tree_result = tool_content(&body, "compact-tree");
                let page: Value = serde_json::from_str(&tree_result)
                    .unwrap_or_else(|error| {
                        let log = std::fs::read_to_string(probe.data.join("host.log"))
                            .unwrap_or_default();
                        panic!("Child tree should return canonical Host JSON: {error}; actual={tree_result}; host_log={}",
                            log.chars().rev().take(4000).collect::<String>().chars().rev().collect::<String>())
                    });
                let actor_id = page["actor_id"].as_str().unwrap();
                *probe.grandchild_parent.lock().unwrap() = Some(actor_id.to_string());
                let nodes = page["nodes"].as_array().unwrap();
                assert!(nodes.iter().any(|node| node["actor_id"] == actor_id));
                assert!(!nodes.iter().any(|node| node["actor_id"] == "foreign-child"));
                let canonical = SessionStoreV2::new(probe.data.clone())
                    .await
                    .unwrap()
                    .load_session(actor_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(canonical.parent_session_id.as_deref(), Some("compact-root"));
                call(
                    "grandchild-create",
                    "SubAgent",
                    json!({
                        "action":"create", "title":"Nested canonical draft",
                        "responsibility":"Keep a bounded draft under the active Child",
                        "prompt":"Nested draft from the real Child",
                        "auto_run":false
                    }),
                )
            }
            (None, 2) => {
                let grandchild_id = logical_grandchild(&body);
                let page: Value =
                    serde_json::from_str(&tool_content(&body, "compact-tree")).unwrap();
                let parent_id = page["actor_id"].as_str().unwrap();
                let canonical = SessionStoreV2::new(probe.data.clone())
                    .await
                    .unwrap()
                    .load_session(&grandchild_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(canonical.parent_session_id.as_deref(), Some(parent_id));
                assert_eq!(canonical.root_session_id, "compact-root");
                call(
                    "grandchild-message",
                    "SubAgent",
                    json!({
                        "action":"send_message", "child_session_id":grandchild_id,
                        "message":"Correct this bounded draft in place", "auto_run":false
                    }),
                )
            }
            (None, 3) => {
                let grandchild_id = logical_grandchild(&body);
                let canonical = SessionStoreV2::new(probe.data.clone())
                    .await
                    .unwrap()
                    .load_session(&grandchild_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(canonical.messages.iter().any(|message| message
                    .content
                    .contains("Correct this bounded draft in place")));
                call(
                    "grandchild-get",
                    "SubAgent",
                    json!({
                        "action":"get", "child_session_id":grandchild_id,
                    }),
                )
            }
            (None, 4) => {
                let grandchild_id = logical_grandchild(&body);
                let overview: Value =
                    serde_json::from_str(&tool_content(&body, "grandchild-get")).unwrap();
                assert_eq!(overview["child_session_id"], grandchild_id);
                call(
                    "grandchild-cancel",
                    "SubAgent",
                    json!({
                        "action":"cancel", "child_session_id":grandchild_id,
                    }),
                )
            }
            (None, 5) => {
                let grandchild_id = logical_grandchild(&body);
                let canonical = SessionStoreV2::new(probe.data.clone())
                    .await
                    .unwrap()
                    .load_session(&grandchild_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(canonical.last_run_status().as_deref(), Some("cancelled"));
                call(
                    "compact-glob",
                    "Glob",
                    json!({"pattern":"compact-marker.txt","limit":1}),
                )
            }
            (Some(_), 0) => call(
                "compact-glob",
                "Glob",
                json!({"pattern":"compact-marker.txt","limit":1}),
            ),
            (None, 6) => {
                assert!(tool_content(&body, "compact-glob")
                    .contains(probe.workspace.join("compact-marker.txt").to_str().unwrap()));
                call(
                    "grandchild-auto-create",
                    "SubAgent",
                    json!({
                        "action":"create", "title":"Nested running Grandchild",
                        "responsibility":"Return one bounded result to the direct Child",
                        "prompt":"Nested running Grandchild", "auto_run":true
                    }),
                )
            }
            (None, 7) => {
                let created: Value =
                    serde_json::from_str(&tool_content(&body, "grandchild-auto-create")).unwrap();
                assert!(created["child_session_id"].as_str().is_some());
                if probe.fast_grandchild {
                    call(
                        "grandchild-fast-wait",
                        "SubAgent",
                        json!({
                            "action":"wait", "child_session_ids":[created["child_session_id"]]
                        }),
                    )
                } else {
                    (json!({"content":"CHILD_AWAITING_GRANDCHILD"}), "stop")
                }
            }
            (None, 8) => {
                assert!(
                    body.to_string().contains("GRANDCHILD_DONE"),
                    "resumed Child must receive canonical Grandchild outcome; messages={:?}",
                    body["messages"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|message| {
                            (
                                message["role"].as_str().unwrap_or(""),
                                message["content"]
                                    .as_str()
                                    .unwrap_or("")
                                    .chars()
                                    .take(300)
                                    .collect::<String>(),
                            )
                        })
                        .collect::<Vec<_>>()
                );
                (json!({"content":"COMPACT_CHILD_EVIDENCE"}), "stop")
            }
            (Some(_), 1) => {
                assert!(tool_content(&body, "compact-glob")
                    .contains(probe.workspace.join("compact-marker.txt").to_str().unwrap()));
                (json!({"content":"COMPACT_CHILD_EVIDENCE"}), "stop")
            }
            _ => {
                assert!(body.to_string().contains(CORRECTION));
                (json!({"content":"COMPACT_CORRECTION_DONE"}), "stop")
            }
        }
    } else if body["model"] == "compact-root" && body["tools"].to_string().contains("SubAgent") {
        match probe.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                let schema = body["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|tool| tool["function"]["name"] == "SubAgent")
                    .unwrap();
                assert_eq!(
                    schema["function"]["parameters"]["properties"]
                        .as_object()
                        .unwrap()
                        .len(),
                    5
                );
                let mut args = json!({"message":TASK});
                if let Some(role) = probe.role {
                    args["role"] = json!(role);
                }
                call("compact-create", "SubAgent", args)
            }
            1 => {
                let child_id = logical_child(&body);
                tokio::time::timeout(Duration::from_secs(60), async {
                    loop {
                        let disk = SessionStoreV2::new(probe.data.clone()).await.unwrap();
                        let child = disk.load_session(&child_id).await.unwrap().unwrap();
                        if child.last_run_status().as_deref() == Some("completed") {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await
                .unwrap();
                call(
                    "compact-result",
                    "SubAgent",
                    json!({"intent":"inspect","target":child_id,"message":"result"}),
                )
            }
            2 => {
                let result: Value =
                    serde_json::from_str(&tool_content(&body, "compact-result")).unwrap();
                if result["text"] != "COMPACT_CHILD_EVIDENCE" {
                    let child_id = logical_child(&body);
                    let child = SessionStoreV2::new(probe.data.clone())
                        .await
                        .unwrap()
                        .load_session(&child_id)
                        .await
                        .unwrap()
                        .unwrap();
                    eprintln!(
                        "root inspect mismatch: status={:?}, messages={:?}",
                        child.last_run_status(),
                        child
                            .messages
                            .iter()
                            .rev()
                            .take(6)
                            .map(|message| (
                                format!("{:?}", message.role),
                                message.content.chars().take(150).collect::<String>()
                            ))
                            .collect::<Vec<_>>()
                    );
                    let log =
                        std::fs::read_to_string(probe.data.join("host.log")).unwrap_or_default();
                    eprintln!(
                        "fixture host log tail: {}",
                        log.chars()
                            .rev()
                            .take(3500)
                            .collect::<String>()
                            .chars()
                            .rev()
                            .collect::<String>()
                    );
                }
                assert_eq!(result["text"], "COMPACT_CHILD_EVIDENCE");
                call(
                    "compact-correct",
                    "SubAgent",
                    json!({"target":logical_child(&body),"message":CORRECTION}),
                )
            }
            3 => call(
                "compact-foreign",
                "SubAgent",
                json!({"intent":"inspect","target":"foreign-child"}),
            ),
            _ => {
                assert!(
                    tool_content(&body, "compact-foreign").contains("SubAgent operation failed")
                );
                (json!({"content":"COMPACT_ROOT_DONE"}), "stop")
            }
        }
    } else {
        (json!({"content":"bounded auxiliary response"}), "stop")
    };
    let event = json!({"id":"compact-response","object":"chat.completion.chunk",
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
fn start(data: &Path, port: u16) -> Host {
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
            .env_remove("RUST_MIN_STACK")
            .env("RUST_LOG", "warn")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}

#[test]
fn compact_root_creates_inspects_and_corrects_one_actual_child() {
    // Only the debug provider fixture thread is larger. Actual serve/worker
    // processes use their default native stack and the same compiled artifact.
    std::thread::Builder::new()
        .name("compact-provider-fixture".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            actix_web::rt::System::new().block_on(async {
                for (role, fast_grandchild) in
                    [(None, false), (None, true), (Some("explorer"), false)]
                {
                    fixture(role, fast_grandchild).await;
                }
            })
        })
        .unwrap()
        .join()
        .unwrap();
}
async fn fixture(role: Option<&'static str>, fast_grandchild: bool) {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().canonicalize().unwrap();
    let workspace = data.join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::write(
        workspace.join("compact-marker.txt"),
        "real native tool evidence",
    )
    .unwrap();
    let disk = SessionStoreV2::new(data.clone()).await.unwrap();
    let foreign = Session::new("foreign-root", "compact-root");
    disk.save_session(&foreign).await.unwrap();
    let foreign_child =
        Session::new_child("foreign-child", foreign.id, "compact-child", "Not owned");
    disk.save_session(&foreign_child).await.unwrap();
    let probe = web::Data::new(Probe {
        data: data.clone(),
        workspace: workspace.clone(),
        role,
        fast_grandchild,
        root_calls: AtomicUsize::new(0),
        child_calls: AtomicUsize::new(0),
        grandchild_parent: Mutex::new(None),
        requests: Mutex::new(vec![]),
    });
    let provider_probe = probe.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(provider_probe.clone())
            .route("/v1/chat/completions", web::post().to(response))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok()
                        .json(json!({"data":[{"id":"compact-root"},{"id":"compact-child"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let url = format!("http://{}/v1", server.addrs()[0]);
    let running = server.run();
    let provider_handle = running.handle();
    actix_web::rt::spawn(running);
    // No worker_bin/args override: actual production default current_exe route.
    let config = json!({"provider":"openai","features":{"provider_model_ref":true},
        "providers":{"openai":{"api_key":"fixture-key","base_url":url,"model":"compact-root"}},
        "defaults":{"chat":{"provider":"openai","model":"compact-root"},
            "subagent_models":{"worker":{"provider":"openai","model":"compact-child"},
                "explorer":{"provider":"openai","model":"compact-child"}}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1}});
    std::fs::write(
        data.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut host = start(&data, port);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}/api/v1");
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "{}",
                std::fs::read_to_string(data.join("host.log")).unwrap()
            );
            if client
                .get(format!("{base}/health"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let created = client.post(format!("{base}/chat")).json(&json!({"session_id":"compact-root",
        "message":"Delegate the complete task and correct the same child", "model":"compact-root", "provider":"openai",
        "model_ref":{"provider":"openai","model":"compact-root"},"thinking_mode":"ultra",
        "permission_mode":"bypass","workspace_path":workspace})).send().await.unwrap();
    assert!(
        created.status().is_success(),
        "{}",
        created.text().await.unwrap()
    );
    assert!(client
        .post(format!("{base}/execute/compact-root"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    let parent = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "{}",
                std::fs::read_to_string(data.join("host.log")).unwrap()
            );
            let disk = SessionStoreV2::new(data.clone()).await.unwrap();
            let parent = disk.load_session("compact-root").await.unwrap().unwrap();
            if parent
                .messages
                .iter()
                .any(|m| m.content.contains("COMPACT_ROOT_DONE"))
            {
                break parent;
            }
            assert_ne!(
                parent.last_run_status().as_deref(),
                Some("error"),
                "{:?}",
                parent.last_run_error()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let cold = SessionStoreV2::new(data.clone()).await.unwrap();
    let entries: Vec<_> = cold
        .list_index_entries()
        .await
        .into_iter()
        .filter(|e| e.parent_session_id.as_deref() == Some("compact-root"))
        .collect();
    assert_eq!(entries.len(), 1);
    let corrected = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let child = cold.load_session(&entries[0].id).await.unwrap().unwrap();
            if child.messages.iter().any(|message| {
                message.role == Role::Assistant && message.content == "COMPACT_CORRECTION_DONE"
            }) {
                break child;
            }
            assert_ne!(
                child.last_run_status().as_deref(),
                Some("error"),
                "Child correction failed: {:?}",
                child.last_run_error()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let child = if let Ok(child) = corrected {
        child
    } else {
        let child = cold.load_session(&entries[0].id).await.unwrap().unwrap();
        panic!("Child correction did not finish: role={role:?}, fast={fast_grandchild}, status={:?}, error={:?}, messages={:?}",
            child.last_run_status(), child.last_run_error(),
            child.messages.iter().rev().take(5).map(|message|
                (format!("{:?}", message.role), message.content.chars().take(180).collect::<String>())
            ).collect::<Vec<_>>());
    };
    assert_eq!(child.metadata["assignment_prompt"], TASK);
    if let Some(role) = role {
        let binding: Value =
            serde_json::from_str(&child.metadata["child.named_profile.v1"]).unwrap();
        assert_eq!(binding["name"], role);
        assert!(child.agent_runtime_state.as_ref().unwrap().read_only);
        assert!(child.messages[0]
            .content
            .contains("Selected child role: explorer"));
    } else {
        assert_eq!(child.metadata["subagent_type"], "worker");
        assert!(!child.metadata.contains_key("child.named_profile.v1"));
    }
    assert!(child
        .messages
        .iter()
        .any(|m| m.role == Role::User && m.content.contains(CORRECTION)));
    assert!(child
        .messages
        .iter()
        .any(|m| m.role == Role::Assistant && m.content == "COMPACT_CORRECTION_DONE"));
    assert_eq!(child.last_run_status().as_deref(), Some("completed"));
    let inbox = FileSessionInbox::new(
        std::sync::Arc::new(SessionStoreV2::new(data.clone()).await.unwrap()),
        SessionInboxLimits::default(),
    );
    let inbox_state = inbox.inspect(&child.id).await.unwrap();
    assert_eq!((inbox_state.pending, inbox_state.claimed), (0, 0));
    assert!(probe.child_calls.load(Ordering::SeqCst) >= 3);
    let requests = probe.requests.lock().unwrap().clone();
    let matched = requests
        .iter()
        .filter(|r| r["model"] == "compact-child")
        .any(|r| {
            r["messages"].as_array().unwrap().iter().any(|m| {
                m["content"].as_str().is_some_and(|content| {
                    if role.is_some() {
                        content.matches("<task-brief>").count() == 1
                            && content.matches("</task-brief>").count() == 1
                            && content
                                .split_once("<task-brief>")
                                .and_then(|(_, tail)| tail.split_once("</task-brief>"))
                                .and_then(|(payload, _)| {
                                    serde_json::from_str::<Value>(payload).ok()
                                })
                                .is_some_and(|packet| {
                                    packet["objective"] == TASK && packet["task_brief"] == TASK
                                })
                    } else {
                        content.contains(TASK)
                    }
                })
            })
        });
    if !matched {
        // Only fixture-authored User bodies, never full requests/config/secrets.
        let mut users: Vec<_> = requests
            .iter()
            .filter(|r| r["model"] == "compact-child")
            .flat_map(|r| r["messages"].as_array().unwrap())
            .filter(|m| m["role"] == "user")
            .map(|m| {
                let shape = if m["content"].is_string() {
                    "string"
                } else {
                    "other"
                };
                let content = m["content"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| m["content"].to_string());
                let task =
                    content.contains("compact-marker.txt") || content.contains("<task-brief>");
                (task, shape, content)
            })
            .collect();
        users.sort_by_key(|(task, _, _)| !task);
        const HEADER: &str = "actual compact-child User wire (bounded): ";
        let mut diagnostic = String::new();
        for (task, shape, content) in users {
            let prefix: String = content.chars().take(if task { 4096 } else { 80 }).collect();
            let line = format!(
                "\nrole=user shape={shape} bytes={} content={prefix}",
                content.len()
            );
            for ch in line.chars() {
                if diagnostic.len() + ch.len_utf8() + HEADER.len() + 1 > 8192 {
                    break;
                }
                diagnostic.push(ch);
            }
        }
        eprintln!("{HEADER}{diagnostic}");
    }
    assert!(
        matched,
        "actual child provider must receive the complete task"
    );
    for message in parent
        .messages
        .iter()
        .filter(|m| m.role == Role::Tool && m.tool_success == Some(true))
    {
        if let Ok(value) = serde_json::from_str::<Value>(&message.content) {
            if value.get("actor_id").is_some() {
                assert_eq!(value["actor_id"], child.id);
                for forbidden in [
                    "runtime_kind",
                    "external_agent_id",
                    "external_protocol",
                    "a2a_context_id",
                    "child_session_id",
                ] {
                    assert!(value.get(forbidden).is_none());
                }
            }
        }
    }
    drop(host);
    provider_handle.stop(true).await;
}
