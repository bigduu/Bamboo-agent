//! Independent live Hosts replace expired zero-tool Child owners.
//! Fixtures pause an initial owner or delay its real pre-ACK release requests.
#![cfg(unix)]
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::{ActorActivationStatus, ActorDirectoryPort, Role, SessionInboxPort};
use bamboo_storage::SessionStoreV2;
use futures::StreamExt;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[path = "support/delayed_initial_release.rs"]
mod delayed_initial_release;
use delayed_initial_release::ReleaseProxy;

const INITIAL_REPLY: &str = "INITIAL_CHILD_REPLY_BEFORE_CHECKPOINT";
const ROOT: &str = "replacement-root";
const INPUT: &str = "REPLACEMENT_OWNED_INPUT";
const OLD_REPLY: &str = "STALE_CHILD_OUTPUT_MUST_NOT_COMMIT";
const NEW_REPLY: &str = "REPLACEMENT_CHILD_REPLY";
const WAIT: Duration = Duration::from_secs(30);

struct Probe {
    delayed_release: bool,
    data: PathBuf,
    workspace: PathBuf,
    stage: AtomicUsize,
    root_round: AtomicUsize,
    requests: Mutex<Vec<Value>>,
    child_calls: AtomicUsize,
    child_id: Mutex<Option<String>>,
    message_id: Mutex<Option<bamboo_domain::SessionMessageId>>,
    initial_open: AtomicBool,
    replacement_checked: AtomicBool,
    old_sent: AtomicBool,
    release_old: tokio::sync::watch::Sender<bool>,
}

fn chunk(delta: Value, finish: Option<&str>) -> web::Bytes {
    web::Bytes::from(format!(
        "data: {}\n\n",
        json!({"id":"child-replacement", "object":"chat.completion.chunk", "choices":[{"index":0,"delta":delta,"finish_reason":finish}]})
    ))
}
fn answer(delta: Value, finish: &str) -> HttpResponse {
    HttpResponse::Ok().content_type("text/event-stream").body(
        [
            chunk(delta, Some(finish)),
            web::Bytes::from_static(b"data: [DONE]\n\n"),
        ]
        .concat(),
    )
}
fn tool(id: &str, args: Value) -> HttpResponse {
    answer(
        json!({"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":"SubAgent","arguments":args.to_string()}}]}),
        "tool_calls",
    )
}

async fn provider(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let request = json!({
        "model":body["model"], "stage":probe.stage.load(Ordering::SeqCst),
        "round":probe.root_round.load(Ordering::SeqCst),
        "has_subagent":body["tools"].to_string().contains("SubAgent"),
        "messages":body["messages"].as_array().map(Vec::len),
    });
    {
        let mut requests = probe.requests.lock().unwrap();
        if requests.len() < 24 {
            requests.push(request);
        }
    }

    if body["model"] == "replacement-child" {
        let call = probe.child_calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            call < 2,
            "only original and replacement Child provider entries"
        );
        assert!(body["tools"].is_null() || body["tools"].as_array().is_some_and(Vec::is_empty));
        if call == 1 {
            let id = probe.child_id.lock().unwrap().clone().unwrap();
            let input = probe.message_id.lock().unwrap().clone().unwrap();
            let store = Arc::new(SessionStoreV2::new(probe.data.clone()).await.unwrap());
            let child = store.load_session(&id).await.unwrap().unwrap();
            let actor = store.inspect_actor(&id).await.unwrap();
            assert_eq!(actor.actor.current_attempt, 2);
            assert_eq!(
                actor.activation.as_ref().unwrap().status,
                ActorActivationStatus::Running
            );
            assert_eq!(
                child
                    .messages
                    .iter()
                    .filter(|m| m.id == input.as_str()
                        && m.role == Role::User
                        && m.content == INPUT)
                    .count(),
                1,
                "input checkpoint precedes provider admission"
            );
            assert!(child.session_inbox_admission().unwrap().contains(&input));
            let inbox = bamboo_storage::FileSessionInbox::new(
                store,
                bamboo_domain::SessionInboxLimits::default(),
            );
            assert!(
                inbox.was_admitted(&id, &input).await.unwrap(),
                "permanent Inbox ACK precedes provider admission"
            );
            assert_eq!(
                body["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|m| m["role"] == "user" && m["content"] == INPUT)
                    .count(),
                1
            );
            assert!(!body["messages"].to_string().contains(OLD_REPLY));
            if probe.delayed_release {
                assert_eq!(
                    body["messages"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|m| m["role"] == "assistant" && m["content"] == INITIAL_REPLY)
                        .count(),
                    1,
                    "replacement resumes the actual pre-ACK checkpoint"
                );
            }
            probe.replacement_checked.store(true, Ordering::SeqCst);
            return answer(json!({"content":NEW_REPLY}), "stop");
        }
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<web::Bytes, std::io::Error>>(4);
        tx.send(Ok(chunk(json!({"role":"assistant"}), None)))
            .await
            .unwrap();
        let held = probe.clone();
        actix_web::rt::spawn(async move {
            held.initial_open.store(true, Ordering::SeqCst);
            let mut release = held.release_old.subscribe();
            let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
            loop {
                if *release.borrow_and_update() {
                    break;
                }
                tokio::select! {
                    changed=release.changed()=> { if changed.is_err() {return;} }
                    _=heartbeat.tick()=> {if tx.send(Ok(web::Bytes::from_static(b": keepalive\n\n"))).await.is_err(){return;}}
                }
            }
            if tx
                .send(Ok(chunk(
                    json!({"content":if held.delayed_release {INITIAL_REPLY} else {OLD_REPLY}}),
                    Some("stop"),
                )))
                .await
                .is_ok()
                && tx
                    .send(Ok(web::Bytes::from_static(b"data: [DONE]\n\n")))
                    .await
                    .is_ok()
            {
                held.old_sent.store(true, Ordering::SeqCst);
            }
        });
        return HttpResponse::Ok()
            .content_type("text/event-stream")
            .streaming(tokio_stream::wrappers::ReceiverStream::new(rx));
    }
    if body["model"] != "replacement-root" || !body["tools"].to_string().contains("SubAgent") {
        return answer(json!({"content":"auxiliary"}), "stop");
    }
    let stage = probe.stage.load(Ordering::SeqCst);
    let round = probe.root_round.fetch_add(1, Ordering::SeqCst);
    eprintln!("Root provider stage={stage} round={round}");
    assert!(round < 5, "bounded Root tool exchange");
    if stage == 0 && round == 0 {
        return tool(
            "create-child",
            json!({"action":"create","title":"Surviving owner Child","responsibility":"Return one plain answer without tools","prompt":"Return one bounded plain answer","subagent_type":"plain-reply","workspace":probe.workspace,"auto_run":false}),
        );
    }
    if stage == 0 && round == 1 {
        let result = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .rev()
            .find(|m| m["role"] == "tool" && m["tool_call_id"] == "create-child")
            .unwrap()["content"]
            .as_str()
            .unwrap();
        let created: Value = serde_json::from_str(result).unwrap();
        assert_eq!(created["status"], "created", "{created}");
        let id = created["child_session_id"].as_str().unwrap().to_string();
        *probe.child_id.lock().unwrap() = Some(id.clone());
        // Normal cold-session watchdog configuration, before any Actor claim.
        let store = SessionStoreV2::new(probe.data.clone()).await.unwrap();
        let mut child = store.load_session(&id).await.unwrap().unwrap();
        child
            .metadata
            .insert("child_watchdog.max_total_secs".into(), "30".into());
        // Keep the expired old owner alive long enough to exercise its write fence,
        // rather than letting a resumed watchdog tick cancel it first.
        child
            .metadata
            .insert("child_watchdog.check_interval_secs".into(), "300".into());
        store.save_session(&child).await.unwrap();
        return tool(
            "initial-run",
            json!({"action":"run","child_session_id":id,"reset_to_last_user":false}),
        );
    }
    let id = probe.child_id.lock().unwrap().clone().unwrap();
    if stage == 1 && round == 0 {
        return tool(
            "queue-replacement-input",
            json!({"action":"send_message","child_session_id":id,"message":INPUT,"interrupt_running":false,"auto_run":false}),
        );
    }
    if stage == 2 && round == 0 {
        return tool(
            "replacement-run",
            json!({"action":"run","child_session_id":id,"reset_to_last_user":false}),
        );
    }
    answer(json!({"content":format!("ROOT_DONE_{stage}")}), "stop")
}

struct Host {
    child: Child,
    base: String,
    log: PathBuf,
    stopped: bool,
}

impl Host {
    fn start(data: &Path, name: &str) -> Self {
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        let path = data.join(format!("host-{name}.log"));
        let log = std::fs::File::create(&path).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_bamboo"))
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
            .unwrap();
        Self {
            child,
            base: format!("http://127.0.0.1:{port}/api/v1"),
            log: path,
            stopped: false,
        }
    }

    fn alive(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "fixture-owned Host is still alive: {}",
            self.tail()
        );
    }

    fn signal(&mut self, signal: &str) {
        self.alive();
        assert!(
            Command::new("kill")
                .args([signal, &self.child.id().to_string()])
                .status()
                .unwrap()
                .success(),
            "signal only the exact fixture-owned Host PID"
        );
        self.stopped = signal == "-STOP";
    }

    fn tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        text.lines()
            .rev()
            .take(12)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(|line| line.chars().take(768).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        if self.stopped {
            // Cleanup resumes only our owned child before killing/reaping it.
            let _ = Command::new("kill")
                .args(["-CONT", &self.child.id().to_string()])
                .status();
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct EventTap {
    events: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
}

impl EventTap {
    async fn open(client: &reqwest::Client, host: &Host, path: &str) -> Self {
        let response = client
            .get(format!("{}{path}", host.base))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = events.clone();
        let task = tokio::spawn(async move {
            let mut stream = response.bytes_stream();
            let mut buffer = String::new();
            while let Some(bytes) = stream.next().await {
                let bytes = match bytes {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        captured.lock().unwrap().push(
                            json!({"type":"fixture_transport_error","message":error.to_string()}),
                        );
                        break;
                    }
                };
                buffer.push_str(&String::from_utf8_lossy(&bytes));
                assert!(buffer.len() < 64 * 1024, "bounded dummy-data SSE frame");
                while let Some(end) = buffer.find('\n') {
                    let line = buffer[..end].trim_end_matches('\r').to_string();
                    buffer.drain(..=end);
                    if let Some(data) = line.strip_prefix("data:") {
                        if let Ok(event) = serde_json::from_str::<Value>(data.trim()) {
                            let mut events = captured.lock().unwrap();
                            if events.len() >= 512 {
                                return;
                            }
                            events.push(event);
                        }
                    }
                }
            }
        });
        Self { events, task }
    }
}

impl Drop for EventTap {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn healthy(client: &reqwest::Client, host: &mut Host) {
    tokio::time::timeout(WAIT, async {
        loop {
            host.alive();
            if client
                .get(format!("{}/health", host.base))
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Host health timeout: {}", host.tail()));
}
async fn wait_child_quiet(client: &reqwest::Client, host: &Host, id: &str) {
    tokio::time::timeout(WAIT, async {
        loop {
            let active: Value = client
                .get(format!("{}/runs/active", host.base))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if !active["sessions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["session_id"] == id)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Child {id} runner did not finish before canonical inspection: {}",
            host.tail()
        )
    });
}

async fn root_settled(
    client: &reqwest::Client,
    host: &Host,
    probe: &Probe,
    stage: usize,
) -> bamboo_domain::Session {
    let result = tokio::time::timeout(WAIT, async {
        loop {
            let store = SessionStoreV2::new(probe.data.clone()).await.unwrap();
            let root = store.load_session(ROOT).await.unwrap().unwrap();
            let rows: Value = client
                .get(format!("{}/sessions", host.base))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let finished = if stage == 0 {
                root.last_run_status().as_deref() == Some("suspended")
                    && root
                        .agent_runtime_state
                        .as_ref()
                        .is_some_and(|state| state.waiting_for_children.is_some())
                    && probe.initial_open.load(Ordering::SeqCst)
            } else {
                // A SubAgent run suspends the Root, and a correction can leave
                // that wait in place. The needed boundary is the real tool
                // receipt plus a settled Root runner, not an assistant marker.
                let call_id = if stage == 1 {
                    "queue-replacement-input"
                } else {
                    "replacement-run"
                };
                root.messages
                    .iter()
                    .any(|m| m.role == Role::Tool && m.tool_call_id.as_deref() == Some(call_id))
            };
            if finished
                && rows["sessions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|r| r["id"] == ROOT && r["is_running"] == false)
            {
                return root;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await;
    match result {
        Ok(root) => root,
        Err(_) => {
            let store = SessionStoreV2::new(probe.data.clone()).await.unwrap();
            let root = store.load_session(ROOT).await.unwrap().unwrap();
            let requests = probe.requests.lock().unwrap().clone();
            let messages: Vec<_> = root
                .messages
                .iter()
                .rev()
                .take(6)
                .map(|m| {
                    json!({
                        "role":m.role,"tool_call_id":m.tool_call_id,
                        "content":m.content.chars().take(512).collect::<String>(),
                    })
                })
                .collect();
            let wait = root
                .agent_runtime_state
                .as_ref()
                .and_then(|state| state.waiting_for_children.as_ref());
            panic!("Root stage {stage} did not settle; status={:?}; error={:?}; wait={wait:?}; messages={messages:?}; root_round={}; child_calls={}; requests={requests:?}; Host={}",root.last_run_status(),root.last_run_error(),probe.root_round.load(Ordering::SeqCst),probe.child_calls.load(Ordering::SeqCst),host.tail());
        }
    }
}
async fn chat(client: &reqwest::Client, host: &Host, body: Value) {
    let response = client
        .post(format!("{}/chat", host.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Root chat: {}",
        response.text().await.unwrap()
    );
}
fn root_result(root: &bamboo_domain::Session, id: &str) -> Value {
    let message = root
        .messages
        .iter()
        .find(|m| m.role == Role::Tool && m.tool_call_id.as_deref() == Some(id))
        .unwrap();
    serde_json::from_str(&message.content)
        .unwrap_or_else(|_| panic!("tool {id}: {}", message.content))
}

#[actix_web::test]
async fn expired_surviving_child_owner_yields_to_independent_host_and_queued_input() {
    replacement_fixture(false).await;
}

#[actix_web::test]
async fn delayed_pre_ack_release_from_surviving_host_cannot_release_replacement_input() {
    replacement_fixture(true).await;
}

async fn replacement_fixture(delayed_release: bool) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let data = root.join("host");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&workspace).unwrap();
    let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
    let project = projects
        .create_with_project_path("replacement", None, workspace.to_string_lossy(), vec![])
        .unwrap();
    let agents = projects.paths().project_home(&project.id).join("agents");
    std::fs::create_dir_all(&agents).unwrap();
    std::fs::write(agents.join("plain-reply.md"),"---\nschema_version: 1\nname: plain-reply\ndescription: One plain answer\nmodel_hint: openai:replacement-child\ntools:\n  deny: [Bash, Read, Glob, Edit, Write]\n---\nDo not use tools; answer once.\n").unwrap();
    let (release_old, _) = tokio::sync::watch::channel(false);
    let probe = web::Data::new(Probe {
        delayed_release,
        data: data.clone(),
        workspace: workspace.clone(),
        stage: AtomicUsize::new(0),
        root_round: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
        child_calls: AtomicUsize::new(0),
        child_id: Mutex::new(None),
        message_id: Mutex::new(None),
        initial_open: AtomicBool::new(false),
        replacement_checked: AtomicBool::new(false),
        old_sent: AtomicBool::new(false),
        release_old,
    });
    let captured = probe.clone();
    let server=HttpServer::new(move || App::new().app_data(captured.clone()).route("/v1/chat/completions",web::post().to(provider)).route("/v1/models",web::get().to(|| async {HttpResponse::Ok().json(json!({"data":[{"id":"replacement-root"},{"id":"replacement-child"},{"id":"replacement-auxiliary"}]}))}))).workers(1).bind(("127.0.0.1",0)).unwrap();
    let url = format!("http://{}/v1", server.addrs()[0]);
    let running = server.run();
    let provider_handle = running.handle();
    actix_web::rt::spawn(running);
    std::fs::write(data.join("config.json"),serde_json::to_vec(&json!({"provider":"openai","features":{"provider_model_ref":true},"providers":{"openai":{"api_key":"fixture","base_url":url,"model":"replacement-root","fast_model":"replacement-auxiliary"}},"defaults":{"chat":{"provider":"openai","model":"replacement-root"},"fast":{"provider":"openai","model":"replacement-auxiliary"}},"subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1,"fabric_dir":data.join("subagents")},"stream_timeout":{"transport_idle_timeout_secs":120,"first_semantic_timeout_secs":120,"semantic_idle_timeout_secs":120}})).unwrap()).unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(150))
        .build()
        .unwrap();
    // These brokers have distinct mailbox namespaces. Changing broker.json
    // between boots does not move A's already-connected runtime onto B's bus.
    let proxies = if delayed_release {
        Some((
            ReleaseProxy::start(&root.join("broker-a"), true).await,
            ReleaseProxy::start(&root.join("broker-b"), false).await,
        ))
    } else {
        None
    };
    if let Some((a_proxy, _)) = &proxies {
        a_proxy.configure_host(&data);
    }
    let mut a = Host::start(&data, "a");
    healthy(&client, &mut a).await;
    chat(&client,&a,json!({"session_id":ROOT,"message":"Create and run one plain Child","model":"replacement-root","provider":"openai","model_ref":{"provider":"openai","model":"replacement-root"},"thinking_mode":"ultra","permission_mode":"bypass","workspace_path":workspace,"project_id":project.id})).await;
    let response = client
        .post(format!("{}/execute/{ROOT}", a.base))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "initial execute: {}",
        response.text().await.unwrap()
    );
    root_settled(&client, &a, &probe, 0).await;
    tokio::time::timeout(WAIT, async {
        while !probe.initial_open.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("original Child provider stream established");
    let id = probe.child_id.lock().unwrap().clone().unwrap();
    let store = Arc::new(SessionStoreV2::new(data.clone()).await.unwrap());
    let before = store.load_session(&id).await.unwrap().unwrap();
    let old = store.inspect_actor(&id).await.unwrap();
    let old_activation = old.activation.as_ref().unwrap();
    assert_eq!(old_activation.status, ActorActivationStatus::Running);
    assert_eq!(old.actor.current_attempt, 1);
    let events = EventTap::open(&client, &a, &format!("/events/{id}")).await;
    probe.stage.store(1, Ordering::SeqCst);
    probe.root_round.store(0, Ordering::SeqCst);
    chat(&client,&a,json!({"session_id":ROOT,"message":"Queue one replacement input without interrupting the held Child","model":"replacement-root","provider":"openai"})).await;
    let root = root_settled(&client, &a, &probe, 1).await;
    let queued = root_result(&root, "queue-replacement-input");
    assert_eq!(queued["child_session_id"], id, "{queued}");
    let input =
        bamboo_domain::SessionMessageId::parse(queued["message_id"].as_str().unwrap()).unwrap();
    *probe.message_id.lock().unwrap() = Some(input.clone());
    let inbox = bamboo_storage::FileSessionInbox::new(
        store.clone(),
        bamboo_domain::SessionInboxLimits::default(),
    );
    let pending = inbox.inspect(&id).await.unwrap();
    assert_eq!((pending.pending, pending.claimed), (1, 0));
    assert!(!inbox.was_admitted(&id, &input).await.unwrap());
    assert_eq!(probe.child_calls.load(Ordering::SeqCst), 1);
    let old_request = if let Some((a_proxy, b_proxy)) = &proxies {
        // The continuation's worker release deadline remains the production
        // 60 seconds. Start it with only 30 seconds left on A's real lease.
        let release_at = old_activation.lease_expires_at - chrono::Duration::seconds(30);
        tokio::time::timeout(Duration::from_secs(65), async {
            while chrono::Utc::now() < release_at {
                a.alive();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("first provider release follows the actual actor lease clock");
        probe.release_old.send_replace(true);
        tokio::time::timeout(WAIT, async {
            while !a_proxy.is_held() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "actual continuation initial request was not captured: {}",
                a.tail()
            )
        });
        let requests = a_proxy.requests();
        assert_eq!(
            requests.len(),
            1,
            "only this actual continuation release request is delayed"
        );
        let request = requests[0].clone();
        assert_eq!(request.child_id, id);
        assert_eq!(request.envelope_id, input.as_str());
        assert_eq!(request.activation_run_id, old_activation.run_id);
        assert_eq!(request.created_at, before.created_at);
        assert_eq!(request.generation, pending.generation);
        let cut = store.load_session(&id).await.unwrap().unwrap();
        assert_eq!(
            cut.messages
                .iter()
                .filter(|m| m.content == INITIAL_REPLY)
                .count(),
            1
        );
        let checkpoint = cut
            .messages
            .iter()
            .find(|m| m.id == input.as_str())
            .unwrap();
        assert_eq!(checkpoint.content, INPUT);
        assert!(checkpoint
            .metadata
            .as_ref()
            .unwrap()
            .get("_bamboo_owned_input_checkpoint")
            .is_some());
        assert!(cut.session_inbox_admission().unwrap().contains(&input));
        assert!(
            !inbox.was_admitted(&id, &input).await.unwrap(),
            "Host has checkpointed but must not ACK before its real release request"
        );
        let backlog = inbox.inspect(&id).await.unwrap();
        assert_eq!((backlog.pending, backlog.claimed), (0, 1));
        let leases = inbox
            .inspect_owned_leases(&id, 2, chrono::Utc::now())
            .await
            .unwrap();
        assert_eq!(leases.len(), 1);
        assert!(!leases[0].expired);
        assert_eq!(leases[0].generation, pending.generation);
        assert!(a_proxy.releases().is_empty());
        assert_eq!(
            probe.child_calls.load(Ordering::SeqCst),
            1,
            "old continuation cannot enter provider before Host release"
        );
        b_proxy.configure_host(&data);
        Some(request)
    } else {
        None
    };
    let mut b = Host::start(&data, "b");
    healthy(&client, &mut b).await;
    // Child completion (including its error) is published to the Parent/account
    // feed, not the Child token stream. Keep that feed open across Root turns.
    let account = EventTap::open(&client, &a, "/stream").await;
    if !delayed_release {
        a.signal("-STOP");
    }
    let deadline = old_activation.lease_expires_at;
    eprintln!(
        "real Child lease expires at {deadline}; old Host PID={} stays alive",
        a.child.id()
    );
    tokio::time::timeout(Duration::from_secs(95), async {
        while chrono::Utc::now() < deadline {
            a.alive();
            b.alive();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("wait only for the actual lease deadline");
    assert_eq!(
        store
            .inspect_actor(&id)
            .await
            .unwrap()
            .activation
            .as_ref()
            .unwrap()
            .run_id,
        old_activation.run_id
    );
    probe.stage.store(2, Ordering::SeqCst);
    probe.root_round.store(0, Ordering::SeqCst);
    chat(&client,&b,json!({"session_id":ROOT,"message":"Recover the same Child with run(false)","model":"replacement-root","provider":"openai"})).await;
    let root = root_settled(&client, &b, &probe, 2).await;
    let replacement_result = root_result(&root, "replacement-run");
    eprintln!("real replacement SubAgent result: {replacement_result}");
    let admitted = tokio::time::timeout(WAIT, async {
        while !probe.replacement_checked.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    let child = store.load_session(&id).await.unwrap().unwrap();
    let authority = store.inspect_actor(&id).await.unwrap();
    assert!(admitted.is_ok(),"independent Host must admit the queued input before provider; result={replacement_result}; Child status={:?}; error={:?}; authority={authority:?}; B={}",child.last_run_status(),child.last_run_error(),b.tail());
    tokio::time::timeout(WAIT, async {
        loop {
            let child = store.load_session(&id).await.unwrap().unwrap();
            if child
                .messages
                .iter()
                .any(|m| m.role == Role::Assistant && m.content == NEW_REPLY)
                && store
                    .inspect_actor(&id)
                    .await
                    .unwrap()
                    .activation
                    .as_ref()
                    .unwrap()
                    .status
                    == ActorActivationStatus::Succeeded
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("replacement did not commit: {}", b.tail()));
    wait_child_quiet(&client, &b, &id).await;
    let replaced = store.load_session(&id).await.unwrap().unwrap();
    let replacement = store.inspect_actor(&id).await.unwrap();
    let new_activation = replacement.activation.as_ref().unwrap();
    assert_ne!(new_activation.run_id, old_activation.run_id);
    assert_ne!(new_activation.lease_owner, old_activation.lease_owner);
    assert!(new_activation.lease_epoch > old_activation.lease_epoch);
    assert_eq!(replaced.created_at, before.created_at);
    assert_eq!(replaced.parent_session_id, before.parent_session_id);
    assert_eq!(replaced.spawn_depth, before.spawn_depth);
    assert_eq!(replacement.actor.policy_revision, old.actor.policy_revision);
    assert_eq!(replacement.actor.project_id, old.actor.project_id);
    assert_eq!(probe.child_calls.load(Ordering::SeqCst), 2);
    if let Some((a_proxy, b_proxy)) = &proxies {
        let original = old_request.as_ref().unwrap();
        let replacement_requests = b_proxy.requests();
        assert_eq!(replacement_requests.len(), 1);
        let fresh = &replacement_requests[0];
        assert_eq!(fresh.child_id, original.child_id);
        assert_eq!(fresh.envelope_id, original.envelope_id);
        assert_eq!(fresh.created_at, original.created_at);
        assert_ne!(fresh.activation_run_id, original.activation_run_id);
        assert_ne!(fresh.nonce, original.nonce);
        let releases = b_proxy.releases();
        assert_eq!(releases.len(), 1);
        assert_eq!(
            &releases[0].request, fresh,
            "only the current B request gets its matching release"
        );
        assert!(a_proxy.releases().is_empty());
        a_proxy.release();
        tokio::time::timeout(WAIT, async {
            while !a_proxy.was_released() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("forward the original unmodified old request to its live Host");
    } else {
        probe.release_old.send_replace(true);
        tokio::time::timeout(WAIT, async {
            while !probe.old_sent.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("surviving old Child provider delivers its stale answer");
        // Event ingress may reject the old fence before final append.
        a.signal("-CONT");
    }
    let old_commit = tokio::time::timeout(WAIT, async {
        loop {
            let captured = account.events.lock().unwrap().clone();
            if captured.iter().any(|change| {
                let event = &change["event"];
                event["type"] == "sub_agent_completed"
                    && event["child_session_id"] == id
                    && event["status"] == "error"
                    && event["error"].as_str().is_some_and(|error| {
                        error.contains("actor activation fence is stale or expired")
                    })
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await;
    let observed_events = account.events.lock().unwrap().clone();
    assert!(
        old_commit.is_ok(),
        "old owner must reject its stale activation fence; A={}; events={observed_events:?}",
        a.tail()
    );
    wait_child_quiet(&client, &a, &id).await;
    if let Some((a_proxy, _)) = &proxies {
        assert!(
            a_proxy.releases().is_empty(),
            "expired old request must never receive a release for the replacement-owned input"
        );
        assert_eq!(
            probe.child_calls.load(Ordering::SeqCst),
            2,
            "no old continuation provider after delayed request delivery"
        );
        let backlog = inbox.inspect(&id).await.unwrap();
        assert_eq!((backlog.pending, backlog.claimed), (0, 0));
    }
    let observed_events = events.events.lock().unwrap().clone();
    assert!(
        !observed_events.iter().any(|event| event["type"] == "token"
            && event["content"]
                .as_str()
                .is_some_and(|text| text.contains(OLD_REPLY))),
        "stale Child output must be rejected before token publication"
    );
    let final_child = store.load_session(&id).await.unwrap().unwrap();
    assert_eq!(
        final_child.last_run_status(),
        replaced.last_run_status(),
        "old finalization must preserve replacement status"
    );
    assert_eq!(
        serde_json::to_value(&final_child.agent_runtime_state).unwrap(),
        serde_json::to_value(&replaced.agent_runtime_state).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&final_child.model_context_state).unwrap(),
        serde_json::to_value(&replaced.model_context_state).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&final_child.provider_transcript).unwrap(),
        serde_json::to_value(&replaced.provider_transcript).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&final_child.messages).unwrap(),
        serde_json::to_value(&replaced.messages).unwrap()
    );
    assert!(!final_child
        .messages
        .iter()
        .any(|m| m.content.contains(OLD_REPLY)));
    assert_eq!(store.inspect_actor(&id).await.unwrap(), replacement);
    assert!(inbox.was_admitted(&id, &input).await.unwrap());
    assert_eq!(
        final_child
            .messages
            .iter()
            .filter(|m| m.id == input.as_str())
            .count(),
        1
    );
    for (host, allow_conflict) in [(&a, true), (&b, false)] {
        let response = client
            .get(format!("{}/history/{id}", host.base))
            .send()
            .await
            .unwrap();
        if allow_conflict && response.status() == reqwest::StatusCode::CONFLICT {
            continue;
        }
        assert!(
            response.status().is_success(),
            "quiet Host history status {}",
            response.status()
        );
        let history: Value = response.json().await.unwrap();
        assert!(
            !history.to_string().contains(OLD_REPLY),
            "quiet Host must not serve stale cached output"
        );
        if !allow_conflict {
            assert!(history.to_string().contains(NEW_REPLY));
        }
    }
    a.alive();
    b.alive();
    drop(events);
    drop(b);
    drop(a);
    provider_handle.stop(true).await;
}
