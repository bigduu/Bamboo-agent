//! Real ordinary Root execution authority, independent Hosts, and a surviving
//! old provider stream. Only the provider and fixture-owned process scheduling
//! are controlled. The fixture never creates an Actor claim for either Host.
#![cfg(unix)]

use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::{storage::Storage, AgentEvent};
use bamboo_domain::{ActorActivationStatus, ActorDirectoryEntry, Role, Session};
use bamboo_storage::SessionStoreV2;
use chrono::Utc;
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

const ROOT: &str = "ordinary-root-actor-writer";
const INPUT: &str = "ROOT_WRITER_INPUT: return one plain answer, without tools.";
const OLD_REPLY: &str = "OLD_ROOT_PROVIDER_OUTPUT_MUST_NOT_COMMIT";
const NEW_REPLY: &str = "REPLACEMENT_ROOT_PROVIDER_OUTPUT_COMMITTED";
const WAIT: Duration = Duration::from_secs(30);

struct Probe {
    calls: AtomicUsize,
    a_stream_open: AtomicBool,
    a_response_sent: AtomicBool,
    release_a: tokio::sync::watch::Sender<bool>,
    requests: Mutex<Vec<Value>>,
}

fn chunk(content: Value, finish: Option<&str>) -> web::Bytes {
    web::Bytes::from(format!(
        "data: {}\n\n",
        json!({"id":"root-writer-fixture","object":"chat.completion.chunk",
            "choices":[{"index":0,"delta":content,"finish_reason":finish}]})
    ))
}

async fn provider(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let foreground = body["model"] == "writer-root";
    let tools: Vec<_> = body["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["function"]["name"].as_str())
        .take(32)
        .collect();
    let has_input = body["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|message| {
            message["role"] == "user"
                && message["content"]
                    .as_str()
                    .is_some_and(|text| text.contains(INPUT))
        });
    {
        let mut requests = probe.requests.lock().unwrap();
        assert!(requests.len() < 24, "bounded provider request inventory");
        requests.push(json!({"model":body["model"],"tools":tools,"has_input":has_input}));
    }
    if !foreground {
        return HttpResponse::Ok().content_type("text/event-stream").body(
            [
                chunk(json!({"content":"auxiliary"}), Some("stop")),
                web::Bytes::from_static(b"data: [DONE]\n\n"),
            ]
            .concat(),
        );
    }
    assert!(
        has_input,
        "foreground execution uses the durable User input"
    );
    let call = probe.calls.fetch_add(1, Ordering::SeqCst);
    assert!(call < 2, "only A and the legitimate replacement B execute");
    if call == 1 {
        return HttpResponse::Ok().content_type("text/event-stream").body(
            [
                chunk(json!({"content":NEW_REPLY}), Some("stop")),
                web::Bytes::from_static(b"data: [DONE]\n\n"),
            ]
            .concat(),
        );
    }

    // Establish the real SSE stream before holding its final answer. Keepalive
    // frames do not create semantic text, and the 120-second stream deadlines
    // are longer than the genuine 15-second production lease expiry.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<web::Bytes, std::io::Error>>(4);
    tx.send(Ok(chunk(json!({"role":"assistant"}), None)))
        .await
        .unwrap();
    let held = probe.clone();
    actix_web::rt::spawn(async move {
        held.a_stream_open.store(true, Ordering::SeqCst);
        let mut release = held.release_a.subscribe();
        let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
        loop {
            if *release.borrow_and_update() {
                break;
            }
            tokio::select! {
                changed = release.changed() => {
                    if changed.is_err() { return; }
                }
                _ = heartbeat.tick() => {
                    if tx.send(Ok(web::Bytes::from_static(b": fixture keepalive\n\n"))).await.is_err() {
                        return;
                    }
                }
            }
        }
        if tx
            .send(Ok(chunk(json!({"content":OLD_REPLY}), Some("stop"))))
            .await
            .is_ok()
            && tx
                .send(Ok(web::Bytes::from_static(b"data: [DONE]\n\n")))
                .await
                .is_ok()
        {
            held.a_response_sent.store(true, Ordering::SeqCst);
        }
    });
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(tokio_stream::wrappers::ReceiverStream::new(rx))
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
                            assert!(events.len() < 512, "bounded event inventory");
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

struct Fixture {
    data: PathBuf,
    a: Host,
    b: Host,
    client: reqwest::Client,
    store: SessionStoreV2,
    probe: web::Data<Probe>,
    provider: actix_web::dev::ServerHandle,
    // Reap the fixture Hosts before removing their disposable data directory.
    _temp: tempfile::TempDir,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.probe.release_a.send_replace(true);
    }
}

impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let data = root.join("host");
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        let (release_a, _) = tokio::sync::watch::channel(false);
        let probe = web::Data::new(Probe {
            calls: AtomicUsize::new(0),
            a_stream_open: AtomicBool::new(false),
            a_response_sent: AtomicBool::new(false),
            release_a,
            requests: Mutex::new(Vec::new()),
        });
        let captured = probe.clone();
        let server = HttpServer::new(move || {
            App::new()
                .app_data(captured.clone())
                .route("/v1/chat/completions", web::post().to(provider))
                .route(
                    "/v1/models",
                    web::get().to(|| async {
                        HttpResponse::Ok()
                            .json(json!({"data":[{"id":"writer-root"},{"id":"writer-auxiliary"}]}))
                    }),
                )
        })
        .workers(1)
        .bind(("127.0.0.1", 0))
        .unwrap();
        let url = format!("http://{}/v1", server.addrs()[0]);
        let running = server.run();
        let provider = running.handle();
        actix_web::rt::spawn(running);
        std::fs::write(data.join("config.json"),serde_json::to_vec(&json!({
            "provider":"openai","features":{"provider_model_ref":true},
            "providers":{"openai":{"api_key":"fixture","base_url":url,"model":"writer-root","fast_model":"writer-auxiliary"}},
            "defaults":{"chat":{"provider":"openai","model":"writer-root"},"fast":{"provider":"openai","model":"writer-auxiliary"}},
            "stream_timeout":{"transport_idle_timeout_secs":120,"first_semantic_timeout_secs":120,"semantic_idle_timeout_secs":120}
        })).unwrap()).unwrap();
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(150))
            .build()
            .unwrap();
        let mut a = Host::start(&data, "a");
        Self::healthy(&client, &mut a).await;
        let response = client
            .post(format!("{}/chat", a.base))
            .json(&json!({
                "session_id":ROOT,"message":INPUT,"model":"writer-root","provider":"openai",
                "model_ref":{"provider":"openai","model":"writer-root"},"thinking_mode":"standard",
                "permission_mode":"default","root_orchestration_only":false,"workspace_path":workspace
            }))
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "chat: {}",
            response.text().await.unwrap()
        );
        assert_eq!(
            probe.calls.load(Ordering::SeqCst),
            0,
            "first-create /chat must persist Root without automatically executing it"
        );
        // Both B and the read-only observer discover the persisted Root during
        // their normal initialization. B starts before A has any live attempt,
        // so B's startup cannot act as A's loss/recovery event.
        let mut b = Host::start(&data, "b");
        Self::healthy(&client, &mut b).await;
        let store = SessionStoreV2::new(data.clone()).await.unwrap();
        assert_eq!(
            probe.calls.load(Ordering::SeqCst),
            0,
            "Host startup must not execute the newly created Root before /execute"
        );
        let fixture = Self {
            _temp: temp,
            data,
            a,
            b,
            client,
            store,
            probe,
            provider,
        };
        let root = fixture.canonical().await;
        assert_eq!(root.kind, bamboo_domain::SessionKind::Root);
        assert!(
            !root.root_orchestration_only,
            "ordinary Root, not Supervisor"
        );
        assert_eq!(
            root.messages
                .iter()
                .filter(|message| message.role == Role::User && message.content == INPUT)
                .count(),
            1
        );
        fixture
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
        .expect("real independent Host becomes healthy");
    }

    async fn execute(&self, host: &Host) -> Value {
        let response = self
            .client
            .post(format!("{}/execute/{ROOT}", host.base))
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert!(
            status.is_success() || status == reqwest::StatusCode::CONFLICT,
            "execute {status}: {body}; {}",
            host.tail()
        );
        body
    }

    async fn canonical(&self) -> Session {
        self.store.load_session(ROOT).await.unwrap().unwrap()
    }

    fn account_terminal_history(&self) -> Vec<Value> {
        bamboo_engine::events::journal::read_since(&self.data.join("events"), 0)
            .unwrap()
            .into_iter()
            .filter(|frame| {
                frame.session_id.as_deref() == Some(ROOT)
                    && matches!(
                        frame.event,
                        AgentEvent::Complete { .. }
                            | AgentEvent::Cancelled { .. }
                            | AgentEvent::Error { .. }
                            | AgentEvent::SessionHistoryCommitted { .. }
                    )
            })
            .map(|frame| serde_json::to_value(frame).unwrap())
            .collect()
    }

    async fn history(&self, host: &Host) -> Option<Value> {
        let response = self
            .client
            .get(format!("{}/history/{ROOT}", host.base))
            .send()
            .await
            .unwrap();
        if response.status() == reqwest::StatusCode::CONFLICT {
            // A quiet stale Root may fail closed rather than serve its cache.
            return None;
        }
        assert!(response.status().is_success());
        Some(response.json().await.unwrap())
    }

    fn authority(&self) -> ActorDirectoryEntry {
        // inspect_actor calls ensure_actor in the current public port. Read its
        // canonical sidecar without initialization, repair, or any forged claim.
        let path = self
            .data
            .join("sessions")
            .join(ROOT)
            .join("actor-authority.json");
        let bytes = std::fs::read(&path).unwrap_or_else(|error| panic!(
            "production /execute must create Running Actor authority before the provider; {error}; requests={:?}; A log={}; B log={}",
            self.probe.requests.lock().unwrap(),self.a.tail(),self.b.tail()));
        let entry: ActorDirectoryEntry = serde_json::from_slice(&bytes).unwrap();
        entry.validate().unwrap();
        entry
    }

    async fn start_a(&self) -> ActorDirectoryEntry {
        let response = self.execute(&self.a).await;
        assert_eq!(
            response["status"], "started",
            "A must use the production execution path: {response}"
        );
        tokio::time::timeout(WAIT, async {
            while !self.probe.a_stream_open.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("A's actual provider stream reaches the body barrier");
        let entry = self.authority();
        let activation = entry
            .activation
            .as_ref()
            .expect("production Root must claim, without external fixture claims");
        assert_eq!(activation.status, ActorActivationStatus::Running);
        assert_eq!(activation.run_id, response["run_id"].as_str().unwrap());
        assert_eq!(entry.actor.actor_id, ROOT);
        assert_eq!(
            entry.actor.session_created_at,
            self.canonical().await.created_at
        );
        assert!(activation.lease_expires_at > Utc::now());
        assert!(!self.probe.a_response_sent.load(Ordering::SeqCst));
        entry
    }

    async fn wait_canonical(&self, reply: &str) -> Session {
        tokio::time::timeout(WAIT, async {
            loop {
                let root = self.canonical().await;
                if root
                    .messages
                    .iter()
                    .any(|message| message.role == Role::Assistant && message.content == reply)
                    && root.last_run_status().as_deref() == Some("completed")
                {
                    return root;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "canonical reply {reply} commits; authority={:?}; A={}; B={}",
                self.authority(),
                self.a.tail(),
                self.b.tail()
            )
        })
    }
}

#[actix_web::test]
async fn ordinary_root_execution_claims_and_finishes_its_production_actor() {
    let fixture = Fixture::new().await;
    let a = fixture.start_a().await;
    let before = fixture.canonical().await;
    let response = fixture.execute(&fixture.b).await;
    assert_ne!(
        response["status"], "started",
        "B cannot start while A owns a live Actor: {response}"
    );
    assert_eq!(
        fixture.authority().activation.unwrap().fence(),
        a.activation.as_ref().unwrap().fence()
    );
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), 1);
    let after_busy = fixture.canonical().await;
    assert_eq!(
        after_busy.last_run_status(),
        before.last_run_status(),
        "rejected B cannot change the legitimate A's runtime status"
    );
    assert_eq!(
        serde_json::to_value(&after_busy.messages).unwrap(),
        serde_json::to_value(&before.messages).unwrap()
    );
    fixture.probe.release_a.send_replace(true);
    fixture.wait_canonical(OLD_REPLY).await;
    tokio::time::timeout(WAIT, async {
        loop {
            let activation = fixture.authority().activation.unwrap();
            if activation.status == ActorActivationStatus::Succeeded {
                assert_eq!(activation.fence(), a.activation.as_ref().unwrap().fence());
                assert!(activation.finished_at.is_some());
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("a legitimate owner finishes its own activation");
    fixture.provider.stop(true).await;
}

#[actix_web::test]
async fn surviving_root_writer_rejects_old_output_after_real_host_lease_reclaim() {
    let mut fixture = Fixture::new().await;
    let a = fixture.start_a().await;
    let events = EventTap::open(&fixture.client, &fixture.a, &format!("/events/{ROOT}")).await;
    // The account feed stays open after the token stream's terminal event.
    let account = EventTap::open(&fixture.client, &fixture.a, "/stream").await;
    fixture.a.signal("-STOP");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status = Command::new("ps")
                .args(["-o", "stat=", "-p", &fixture.a.child.id().to_string()])
                .output()
                .unwrap();
            if String::from_utf8_lossy(&status.stdout)
                .trim()
                .starts_with('T')
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fixture-owned A is stopped, not exited or cancelled");
    // Actual wall time and the persisted lease decide expiry. No future `now`,
    // shortened sidecar, A finish, or external A claim substitutes for it.
    tokio::time::timeout(WAIT, async {
        loop {
            fixture.a.alive();
            let live = fixture.authority().activation.unwrap();
            assert_eq!(live.fence(), a.activation.as_ref().unwrap().fence());
            if live.lease_expires_at <= Utc::now() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("production Root lease expires within the finite pause budget");
    assert!(!fixture.probe.a_response_sent.load(Ordering::SeqCst));
    let response = fixture.execute(&fixture.b).await;
    assert_eq!(response["status"],"started","expired authority must permit the same durable User's real B execution; do not repair through chat: {response}");
    let committed = fixture.wait_canonical(NEW_REPLY).await;
    assert!(
        committed.model_context_state.is_some(),
        "B must establish an actual durable model context before testing stale replacement"
    );
    tokio::time::timeout(WAIT, async {
        loop {
            let activation = fixture.authority().activation.unwrap();
            assert_eq!(activation.run_id, response["run_id"].as_str().unwrap());
            if activation.status == ActorActivationStatus::Succeeded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("B confirms its durable history publication before A resumes");
    let b = fixture.authority().activation.unwrap();
    let old = a.activation.as_ref().unwrap();
    assert!(b.attempt > old.attempt && b.lease_epoch > old.lease_epoch);
    assert_ne!(b.lease_owner, old.lease_owner);
    assert_eq!(b.run_id, response["run_id"].as_str().unwrap());
    fixture.a.alive();
    assert!(!fixture.probe.a_response_sent.load(Ordering::SeqCst));
    events.events.lock().unwrap().clear();
    account.events.lock().unwrap().clear();
    // B's Succeeded activation follows its confirmed account publication. A
    // is still stopped and has received no answer, so these exact durable
    // frames belong to B. A's account sink may forward the shared journal's
    // remote delta after resume; clearing the tap does not drain that delta.
    let b_account_frames = fixture.account_terminal_history();
    assert_eq!(
        b_account_frames.len(),
        2,
        "only B completed and committed history while A was stopped: {b_account_frames:?}"
    );
    assert_eq!(b_account_frames[0]["event"]["type"], "complete");
    assert_eq!(
        b_account_frames[1]["event"]["type"],
        "session_history_committed"
    );
    fixture.a.signal("-CONT");
    fixture.probe.release_a.send_replace(true);
    tokio::time::timeout(WAIT,async {
        loop {
            fixture.a.alive();
            let log = std::fs::read_to_string(&fixture.a.log).unwrap_or_default();
            let observed_output = events.events.lock().unwrap().iter().any(|event|
                event["type"] == "token" && event["content"].as_str().is_some_and(|text|text.contains(OLD_REPLY)));
            let rejected_save = log.lines().any(|line| {
                let line = line.to_lowercase();
                line.contains("failed to save session") && (line.contains("fence") || line.contains("actor"))
                    && (line.contains("stale") || line.contains("expired"))
            });
            if fixture.probe.a_response_sent.load(Ordering::SeqCst) && observed_output && rejected_save { break; }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.unwrap_or_else(|_| panic!("A must consume the released output and reject its final authority save, not merely time out; events={:?}; A={}; B={}",events.events.lock().unwrap(),fixture.a.tail(),fixture.b.tail()));
    tokio::time::timeout(WAIT, async {
        loop {
            let active: Value = fixture
                .client
                .get(format!("{}/runs/active", fixture.a.base))
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
                .any(|entry| entry["session_id"] == ROOT)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("A's rejected execution finishes before inspecting its quiet cache path");
    let final_root = fixture.canonical().await;
    assert_eq!(
        final_root.last_run_status(),
        committed.last_run_status(),
        "old error/finalization cannot replace B's terminal status"
    );
    assert_eq!(
        serde_json::to_value(&final_root.agent_runtime_state).unwrap(),
        serde_json::to_value(&committed.agent_runtime_state).unwrap(),
        "old runtime checkpoint cannot overwrite B's concrete execution state"
    );
    assert_eq!(
        serde_json::to_value(&final_root.messages).unwrap(),
        serde_json::to_value(&committed.messages).unwrap(),
        "old writer cannot append or replace canonical transcript"
    );
    assert_eq!(
        serde_json::to_value(&final_root.model_context_state).unwrap(),
        serde_json::to_value(&committed.model_context_state).unwrap(),
        "old writer cannot replace B's durable model context"
    );
    assert_eq!(
        serde_json::to_value(&final_root.provider_transcript).unwrap(),
        serde_json::to_value(&committed.provider_transcript).unwrap()
    );
    assert_eq!(
        fixture.authority().activation.unwrap().fence(),
        b.fence(),
        "old finish/renew never changes B's replacement"
    );
    let observed = events.events.lock().unwrap().clone();
    assert!(
        !observed.iter().any(
            |event| event["type"] == "complete" || event["type"] == "session_history_committed"
        ),
        "stale A cannot publish terminal/history success: {:?}",
        observed
    );
    let account_events = account.events.lock().unwrap().clone();
    assert_eq!(
        fixture.account_terminal_history(),
        b_account_frames,
        "stale A cannot append any durable terminal/history result"
    );
    assert!(
        !account_events.iter().any(|frame| {
            matches!(
                frame["event"]["type"].as_str(),
                Some("complete" | "cancelled" | "error" | "session_history_committed")
            ) && !b_account_frames.contains(frame)
        }),
        "stale A cannot publish the durable account-feed barrier: {account_events:?}"
    );
    if let Some(history) = fixture.history(&fixture.a).await {
        assert!(
            !history.to_string().contains(OLD_REPLY),
            "quiet A's cache must not expose rejected provider output: {history}"
        );
    }
    let history = fixture
        .history(&fixture.b)
        .await
        .expect("current B history remains readable");
    assert!(history.to_string().contains(NEW_REPLY));
    assert!(!history.to_string().contains(OLD_REPLY));
    assert_eq!(fixture.probe.calls.load(Ordering::SeqCst), 2);
    fixture.provider.stop(true).await;
}
