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
}
fn call(args: Value) -> Value {
    json!({"tool_calls":[{"index":0,"id":format!("subagent-{}",args["action"]),"type":"function","function":{"name":"SubAgent","arguments":args.to_string()}}]})
}
async fn provider(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let body = body.into_inner();
    probe.requests.lock().unwrap().push(body.clone());
    let (delta, finish) = if body["model"] == "plain-child" {
        probe.child_calls.fetch_add(1, Ordering::SeqCst);
        probe.ready.store(true, Ordering::SeqCst);
        while !probe.release.load(Ordering::SeqCst) {
            let wake = probe.wake.notified();
            if probe.release.load(Ordering::SeqCst) {
                break;
            }
            wake.await;
        }
        if probe.reasoning {
            (
                json!({"reasoning_content":"UNSUPPORTED_NATIVE_REASONING","content":"UNSUPPORTED_REPLY"}),
                "stop",
            )
        } else {
            (json!({"content":"FENCED_PLAIN_REPLY"}), "stop")
        }
    } else if body["model"] == "plain-root" && body["tools"].to_string().contains("SubAgent") {
        match probe.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => (
                call(
                    json!({"action":"create","title":"Zero-tool Child","responsibility":"Return exactly one plain answer; do not use tools","prompt":"Respond with a plain answer inside this task boundary","subagent_type":"plain-reply","workspace":probe.workspace,"auto_run":false}),
                ),
                "tool_calls",
            ),
            1 => {
                let store = SessionStoreV2::new(probe.data.clone()).await.unwrap();
                let id = store
                    .list_index_entries()
                    .await
                    .into_iter()
                    .find(|row| row.parent_session_id.as_deref() == Some("plain-root"))
                    .unwrap()
                    .id;
                *probe.child_id.lock().unwrap() = Some(id.clone());
                (
                    call(json!({"action":"run","child_session_id":id,"reset_to_last_user":false})),
                    "tool_calls",
                )
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
    let event = json!({"id":"plain-actor","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
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
            .env("RUST_LOG", "warn")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}
async fn fixture(ultra: bool, reasoning: bool) {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("host");
    let workspace = temp.path().join("workspace");
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
        replay: ultra && !reasoning,
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
    if ultra {
        let running = store.inspect_actor(&id).await.unwrap();
        assert_eq!(running.actor.actor_id, id);
        assert_eq!(running.actor.session_created_at, before.created_at);
        assert_eq!(
            running.activation.as_ref().unwrap().status,
            ActorActivationStatus::Running
        );
        assert_eq!(running.actor.current_attempt, 1);
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
    assert_eq!(probe.child_calls.load(Ordering::SeqCst), 1);
    if ultra {
        let finished = store.inspect_actor(&id).await.unwrap();
        assert_eq!(
            finished.activation.unwrap().status,
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
    let reopened = SessionStoreV2::new(data).await.unwrap();
    let cold = reopened.load_session(&id).await.unwrap().unwrap();
    assert_eq!(cold.id, before.id);
    assert_eq!(cold.created_at, before.created_at);
    assert_eq!(cold.parent_session_id, before.parent_session_id);
    assert_eq!(cold.messages, completed.messages);
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
        Box::pin(fixture(ultra, reasoning)).await;
    }
}
