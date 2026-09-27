//! Real serve/SubAgent/Host/current_exe worker. Only the provider is simulated.
//! Explicit Ultra + a normally selected deny-all profile opts in; no manual claim.
#![cfg(unix)]
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::{
    ActorActivationStatus, ActorDirectoryPort, ActorSnapshotLimits, ActorSnapshotPort,
    ActorSnapshotPrincipal,
};
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};

struct Probe {
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    ready: AtomicBool,
    release: AtomicBool,
    wake: tokio::sync::Notify,
    requests: Mutex<Vec<Value>>,
    child_id: Mutex<Option<String>>,
    data: PathBuf,
    workspace: PathBuf,
    reasoning: bool,
    replay: bool,
    correction: bool,
}
fn call(args: Value) -> Value {
    json!({"tool_calls":[{"index":0,"id":format!("subagent-{}",args["action"].as_str().unwrap()),"type":"function","function":{"name":"SubAgent","arguments":args.to_string()}}]})
}
async fn provider(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let body = body.into_inner();
    probe.requests.lock().unwrap().push(body.clone());
    let (delta, finish) = if body["model"] == "plain-child" {
        let child_call = probe.child_calls.fetch_add(1, Ordering::SeqCst);
        probe.ready.store(true, Ordering::SeqCst);
        while !probe.release.load(Ordering::SeqCst) {
            let wake = probe.wake.notified();
            if probe.release.load(Ordering::SeqCst) {
                break;
            }
            wake.await;
        }
        if probe.correction && child_call == 1 {
            assert_eq!(
                body["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|message| message["content"] == "CORRECTION_FROM_ACTUAL_ROOT")
                    .count(),
                1
            );
            let id = probe.child_id.lock().unwrap().clone().unwrap();
            let store = std::sync::Arc::new(SessionStoreV2::new(probe.data.clone()).await.unwrap());
            let canonical = store.load_session(&id).await.unwrap().unwrap();
            let first = canonical
                .messages
                .iter()
                .position(|m| m.content == "INITIAL_BEFORE_CORRECTION")
                .unwrap();
            let message = canonical
                .messages
                .iter()
                .find(|m| m.content == "CORRECTION_FROM_ACTUAL_ROOT")
                .unwrap();
            assert_eq!(canonical.messages[first + 1].id, message.id);
            let envelope_id = bamboo_domain::SessionMessageId::parse(message.id.clone()).unwrap();
            assert!(
                canonical
                    .session_inbox_admission()
                    .unwrap()
                    .contains(&envelope_id),
                "Host first reply + correction checkpoint precede second provider admission"
            );
            let inbox = bamboo_storage::FileSessionInbox::new(
                store,
                bamboo_domain::SessionInboxLimits::default(),
            );
            tokio::time::timeout(Duration::from_secs(30), async {
                while !bamboo_domain::SessionInboxPort::was_admitted(&inbox, &id, &envelope_id)
                    .await
                    .unwrap()
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
        if probe.reasoning {
            (json!({"content":"UNSUPPORTED_REPLY"}), "stop")
        } else if probe.correction && child_call == 0 {
            (json!({"content":"INITIAL_BEFORE_CORRECTION"}), "stop")
        } else {
            (json!({"content":"FENCED_PLAIN_REPLY"}), "stop")
        }
    } else if body["model"] == "plain-root" && body["tools"].to_string().contains("SubAgent") {
        match probe.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => (
                call(
                    json!({"action":"create","title":"Zero-tool Child","responsibility":"Return exactly one plain answer; do not use tools","prompt":"Respond with a plain answer inside this task boundary","subagent_type":"plain-reply","workspace":probe.workspace,"auto_run":probe.correction}),
                ),
                "tool_calls",
            ),
            1 => {
                let content = body["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .rev()
                    .find(|message| {
                        message["role"] == "tool" && message["tool_call_id"] == "subagent-create"
                    })
                    .expect("actual Root SubAgent create tool result")["content"]
                    .as_str()
                    .unwrap();
                let diagnostic: String = content.chars().take(512).collect();
                let created: Value = serde_json::from_str(content).unwrap_or_else(|_| {
                    panic!("actual Root create did not return JSON: {diagnostic}")
                });
                assert_eq!(
                    created["status"],
                    if probe.correction {
                        "running_in_background"
                    } else {
                        "created"
                    },
                    "actual Root create failed: {diagnostic}"
                );
                let store = SessionStoreV2::new(probe.data.clone()).await.unwrap();
                let id = store
                    .list_index_entries()
                    .await
                    .into_iter()
                    .find(|row| row.parent_session_id.as_deref() == Some("plain-root"))
                    .unwrap()
                    .id;
                *probe.child_id.lock().unwrap() = Some(id.clone());
                if probe.correction {
                    tokio::time::timeout(Duration::from_secs(30), async {
                        while !probe.ready.load(Ordering::SeqCst) {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    })
                    .await
                    .unwrap();
                    (
                        call(json!({"action":"send_message","child_session_id":id,
                        "message":"CORRECTION_FROM_ACTUAL_ROOT","interrupt_running":false})),
                        "tool_calls",
                    )
                } else {
                    (
                        call(
                            json!({"action":"run","child_session_id":id,"reset_to_last_user":false}),
                        ),
                        "tool_calls",
                    )
                }
            }
            2 if probe.replay => (
                call(
                    json!({"action":"run","child_session_id":probe.child_id.lock().unwrap().clone().unwrap(),"reset_to_last_user":false}),
                ),
                "tool_calls",
            ),
            _ => (json!({"content":"ROOT_DONE"}), "stop"),
        }
    } else {
        (json!({"content":"auxiliary"}), "stop")
    };
    // Keep the published #1414 reasoning-only fixture shape when adding this
    // correction case: answer+reasoning in one delta is not a ReasoningToken.
    let reasoning_event = if body["model"] == "plain-child" && probe.reasoning {
        let reasoning = json!({"id":"plain-actor","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"reasoning_content":"UNSUPPORTED_NATIVE_REASONING"},"finish_reason":null}]});
        format!("data: {reasoning}\n\n")
    } else {
        String::new()
    };
    let event = json!({"id":"plain-actor","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!(
            "{reasoning_event}data: {event}\n\ndata: [DONE]\n\n"
        ))
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
            .env("RUST_LOG", "warn")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}
async fn fixture(ultra: bool, reasoning: bool, correction: bool) {
    let temp = tempfile::tempdir().unwrap();
    let temp_root = temp.path().canonicalize().unwrap();
    let data = temp_root.join("host");
    let workspace = temp_root.join("workspace");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&workspace).unwrap();
    let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
    let project = projects
        .create_with_project_path("plain", None, workspace.to_string_lossy(), vec![])
        .unwrap();
    let agents = projects.paths().project_home(&project.id).join("agents");
    std::fs::create_dir_all(&agents).unwrap();
    // This is the public named-profile selection path, not a test capability switch.
    std::fs::write(agents.join("plain-reply.md"), "---\nschema_version: 1\nname: plain-reply\ndescription: One bounded plain reply\nmodel_hint: openai:plain-child\ntools:\n  deny: [Bash, Read, Glob, Edit, Write]\n---\nDo not use tools; return one plain answer and stop.\n").unwrap();
    let probe = web::Data::new(Probe {
        root_calls: AtomicUsize::new(0),
        child_calls: AtomicUsize::new(0),
        ready: AtomicBool::new(false),
        release: AtomicBool::new(false),
        wake: Default::default(),
        requests: Mutex::new(vec![]),
        child_id: Mutex::new(None),
        data: data.clone(),
        workspace: workspace.clone(),
        reasoning,
        replay: ultra && !reasoning && !correction,
        correction,
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
                        .json(json!({"data":[{"id":"plain-root"},{"id":"plain-child"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let provider_url = format!("http://{}/v1", server.addrs()[0]);
    let running = server.run();
    let handle = running.handle();
    actix_web::rt::spawn(running);
    std::fs::write(data.join("config.json"),serde_json::to_vec(&json!({"provider":"openai","features":{"provider_model_ref":true},"providers":{"openai":{"api_key":"fixture","base_url":provider_url,"model":"plain-root"}},"defaults":{"chat":{"provider":"openai","model":"plain-root"}},"subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1}})).unwrap()).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut host = start(&data, port);
    let base = format!("http://127.0.0.1:{port}/api/v1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "Host exited: {}",
                std::fs::read_to_string(data.join("host.log")).unwrap()
            );
            if client
                .get(format!("{base}/health"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let response = client.post(format!("{base}/chat")).json(&json!({"session_id":"plain-root","message":"Delegate one plain answer","model":"plain-root","provider":"openai","model_ref":{"provider":"openai","model":"plain-root"},"thinking_mode":if ultra {"ultra"} else {"standard"},"permission_mode":"bypass","workspace_path":workspace,"project_id":project.id})).send().await.unwrap();
    assert!(
        response.status().is_success(),
        "chat: {}",
        response.text().await.unwrap()
    );
    assert!(client
        .post(format!("{base}/execute/plain-root"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    tokio::time::timeout(Duration::from_secs(60), async {
        while !probe.ready.load(Ordering::SeqCst) {
            assert!(host.0.try_wait().unwrap().is_none(), "actual Host exited");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let id = probe.child_id.lock().unwrap().clone().unwrap();
    let store = SessionStoreV2::new(data.clone()).await.unwrap();
    let before = store.load_session(&id).await.unwrap().unwrap();
    assert_eq!(before.root_session_id, "plain-root");
    assert_eq!(before.parent_session_id.as_deref(), Some("plain-root"));
    assert_eq!(before.spawn_depth, 1);
    let binding: Value = serde_json::from_str(&before.metadata["child.named_profile.v1"]).unwrap();
    assert_eq!(binding["tools"], json!([]));
    let requests = probe.requests.lock().unwrap().clone();
    let child_wire = requests
        .iter()
        .find(|body| body["model"] == "plain-child")
        .unwrap();
    assert!(
        child_wire["tools"].is_null() || child_wire["tools"].as_array().is_some_and(Vec::is_empty)
    );
    let mut original_activation = None;
    if ultra {
        let running = store.inspect_actor(&id).await.unwrap();
        assert_eq!(running.actor.actor_id, id);
        assert_eq!(running.actor.session_created_at, before.created_at);
        assert_eq!(
            running.activation.as_ref().unwrap().status,
            ActorActivationStatus::Running
        );
        assert_eq!(running.actor.current_attempt, 1);
        original_activation = running.activation;
    }
    if correction {
        let inbox = bamboo_storage::FileSessionInbox::new(
            std::sync::Arc::new(SessionStoreV2::new(data.clone()).await.unwrap()),
            bamboo_domain::SessionInboxLimits::default(),
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let backlog = bamboo_domain::SessionInboxPort::inspect(&inbox, &id)
                    .await
                    .unwrap();
                if backlog.activation_pending() {
                    let actual = store.load_session(&id).await.unwrap().unwrap();
                    assert!(!actual
                        .messages
                        .iter()
                        .any(|m| m.content == "CORRECTION_FROM_ACTUAL_ROOT"));
                    break;
                }
                assert!(
                    host.0.try_wait().unwrap().is_none(),
                    "actual Host exited before eligible correction delivery"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    probe.release.store(true, Ordering::SeqCst);
    probe.wake.notify_waiters();
    let completed = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let child = store.load_session(&id).await.unwrap().unwrap();
            let status = child.last_run_status();
            if matches!(status.as_deref(), Some("completed" | "error" | "cancelled")) {
                break child;
            }
            assert!(host.0.try_wait().unwrap().is_none(), "actual Host exited");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    if probe.replay {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let parent = store.load_session("plain-root").await.unwrap().unwrap();
                if parent
                    .messages
                    .iter()
                    .any(|message| message.content == "ROOT_DONE")
                {
                    break;
                }
                assert!(host.0.try_wait().unwrap().is_none(), "actual Host exited");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            probe.root_calls.load(Ordering::SeqCst) >= 4,
            "actual second SubAgent run was dispatched"
        );
    }
    let child_calls = probe.child_calls.load(Ordering::SeqCst);
    let expected_calls = if correction { 2 } else { 1 };
    if child_calls != expected_calls {
        let actor = store.inspect_actor(&id).await.map(|entry| {
            json!({"state":entry.actor.state,"attempt":entry.actor.current_attempt,
                "activation":entry.activation.map(|a| a.status)})
        });
        let inbox = bamboo_storage::FileSessionInbox::new(
            std::sync::Arc::new(SessionStoreV2::new(data.clone()).await.unwrap()),
            bamboo_domain::SessionInboxLimits::default(),
        );
        let backlog = bamboo_domain::SessionInboxPort::inspect(&inbox, &id).await;
        let mut diagnostic = json!({
            "status":completed.last_run_status(),
            "error":completed.last_run_error().map(|s| s.chars().take(384).collect::<String>()),
            "actor":actor.map_err(|e| e.to_string()),
            "inbox":backlog.map(|b| json!({"pending":b.pending,"claimed":b.claimed,
                "generation":b.generation,"eligible":b.activation_generation})).map_err(|e| e.to_string()),
            "tail":completed.messages.iter().rev().take(3).map(|m| json!({
                "id":m.id,"role":m.role,"content":m.content.chars().take(128).collect::<String>(),
                "owned_marker":m.metadata.as_ref().is_some_and(|v| v.get("_bamboo_owned_input_checkpoint").is_some())
            })).collect::<Vec<_>>()
        }).to_string();
        let mut end = diagnostic.len().min(2048);
        while !diagnostic.is_char_boundary(end) {
            end -= 1;
        }
        diagnostic.truncate(end);
        assert_eq!(
            child_calls, expected_calls,
            "actual Child phase: {diagnostic}"
        );
    }
    if ultra {
        let finished = store.inspect_actor(&id).await.unwrap();
        let finished_activation = finished.activation.unwrap();
        if correction {
            let original = original_activation.as_ref().unwrap();
            assert_eq!(
                finished_activation.lease_expires_at,
                original.lease_expires_at
            );
            assert_eq!(finished_activation.lease_owner, original.lease_owner);
            assert_eq!(finished_activation.lease_epoch, original.lease_epoch);
        }
        assert_eq!(
            finished_activation.status,
            if reasoning {
                ActorActivationStatus::Failed
            } else {
                ActorActivationStatus::Succeeded
            }
        );
    } else {
        let observed = store
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                "plain-root",
                &id,
                ActorSnapshotLimits::default(),
            )
            .await
            .unwrap();
        assert!(observed
            .nodes
            .iter()
            .find(|node| node.actor_id == id)
            .unwrap()
            .activation
            .is_none());
    }
    assert_eq!(
        completed
            .messages
            .iter()
            .filter(|message| message.content == "FENCED_PLAIN_REPLY")
            .count(),
        usize::from(!reasoning)
    );
    assert!(!completed
        .messages
        .iter()
        .any(|message| message.content.contains("UNSUPPORTED_REPLY")));
    // Genuine cold reopen, without a live worker/Host cache or a manual authority claim.
    drop(host);
    drop(store);
    let reopened = std::sync::Arc::new(SessionStoreV2::new(data).await.unwrap());
    let cold = reopened.load_session(&id).await.unwrap().unwrap();
    assert_eq!(cold.id, before.id);
    assert_eq!(cold.created_at, before.created_at);
    assert_eq!(cold.parent_session_id, before.parent_session_id);
    assert_eq!(
        serde_json::to_value(&cold.messages).unwrap(),
        serde_json::to_value(&completed.messages).unwrap()
    );
    if correction {
        assert!(
            bamboo_domain::PermissionAuditSnapshot::from_metadata(&cold.metadata)
                .unwrap()
                .audit_revision
                > bamboo_domain::PermissionAuditSnapshot::from_metadata(&before.metadata)
                    .unwrap()
                    .audit_revision,
            "the second Run must durably confirm a fresh Host permission audit"
        );
        let first = cold
            .messages
            .iter()
            .position(|m| m.content == "INITIAL_BEFORE_CORRECTION")
            .unwrap();
        assert_eq!(cold.messages[first].role, bamboo_domain::Role::Assistant);
        assert_eq!(cold.messages[first + 1].role, bamboo_domain::Role::User);
        assert_eq!(
            cold.messages[first + 1].content,
            "CORRECTION_FROM_ACTUAL_ROOT"
        );
        assert_eq!(
            cold.messages[first + 2].role,
            bamboo_domain::Role::Assistant
        );
        assert_eq!(cold.messages[first + 2].content, "FENCED_PLAIN_REPLY");
        assert_eq!(first + 3, cold.messages.len());
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m.metadata.as_ref().is_some_and(|metadata| metadata
                    .get("_bamboo_owned_input_checkpoint")
                    .is_some()))
                .count(),
            1
        );
        let message = cold
            .messages
            .iter()
            .find(|m| m.content == "CORRECTION_FROM_ACTUAL_ROOT")
            .unwrap();
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m.content == "CORRECTION_FROM_ACTUAL_ROOT")
                .count(),
            1
        );
        let envelope_id = bamboo_domain::SessionMessageId::parse(message.id.clone()).unwrap();
        assert!(cold
            .session_inbox_admission()
            .unwrap()
            .contains(&envelope_id));
        let inbox = bamboo_storage::FileSessionInbox::new(
            reopened.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        );
        assert!(
            bamboo_domain::SessionInboxPort::was_admitted(&inbox, &id, &envelope_id)
                .await
                .unwrap()
        );
        let backlog = bamboo_domain::SessionInboxPort::inspect(&inbox, &id)
            .await
            .unwrap();
        assert_eq!((backlog.pending, backlog.claimed), (0, 0));
    }
    if ultra {
        assert_eq!(
            reopened
                .inspect_actor(&id)
                .await
                .unwrap()
                .actor
                .current_attempt,
            1
        );
    }
    handle.stop(true).await;
}
#[actix_web::test]
async fn actual_zero_tool_child_uses_host_actor_commit_and_preserves_legacy() {
    for (ultra, reasoning) in [(true, false), (true, true), (false, false)] {
        Box::pin(fixture(ultra, reasoning, false)).await;
    }
}

#[actix_web::test]
async fn actual_owned_child_admits_root_correction_before_second_provider() {
    Box::pin(fixture(true, false, true)).await;
}
