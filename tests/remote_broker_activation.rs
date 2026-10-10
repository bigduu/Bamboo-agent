//! Actual Host/SubAgent → pinned strict WSS → same-source resident BambooRuntime.
#![cfg(unix)]
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_broker::{client_config_trusting_cert, BrokerClient};
use bamboo_domain::{
    ActorActivation, ActorActivationStatus, ActorDirectoryError, ActorDirectoryPort,
    HostRegistryError, Session, WorkerSlotLease,
};
use bamboo_storage::v2::{BrokerTerminalReceipt, FileHostRegistry};
use bamboo_storage::SessionStoreV2;
use bamboo_subagent::{
    provision::{ChildIdentity, ExecutorSpec, ModelRefSpec, ScopedCredential},
    AgentRef, BusEndpoint, ProvisionSpec,
};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime},
};
const HOST: &str = "remote-host-opaque-credential-000000001";
const WORKER: &str = "remote-worker-opaque-credential-0000001";
const OBSERVER: &str = "remote-observer-opaque-credential-00001";
const ROLE: &str = "legacy-remote-native";
fn diagnostic_text(value: &str) -> String {
    let mut value = value.to_owned();
    for credential in [HOST, WORKER, OBSERVER] {
        value = value.replace(credential, "[redacted fixture credential]");
    }
    value.chars().take(2048).collect()
}
fn diagnostic_tail(path: &Path) -> String {
    let read = || -> std::io::Result<String> {
        let mut file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        file.seek(SeekFrom::Start(len.saturating_sub(4096)))?;
        let mut bytes = Vec::new();
        file.take(4096).read_to_end(&mut bytes)?;
        let tail = if len > 4096 {
            bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(&[][..], |newline| &bytes[newline + 1..])
        } else {
            &bytes
        };
        Ok(diagnostic_text(&String::from_utf8_lossy(tail)))
    };
    match read() {
        Ok(tail) => tail,
        Err(error) => format!("unavailable: {error}"),
    }
}
struct Process(Child, PathBuf);
impl Process {
    fn diagnostic(&mut self) -> String {
        format!(
            "pid={} exit={:?} log={} stdout={:?} stderr={:?}",
            self.0.id(),
            self.0.try_wait(),
            self.1.display(),
            diagnostic_tail(&self.1),
            diagnostic_tail(&self.1.with_extension("stderr")),
        )
    }
    fn stop(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}
fn command(data: &Path) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_bamboo"));
    c.env_remove("RUST_MIN_STACK")
        .env_remove("BAMBOO_BROKER_TOKEN")
        .env("HOME", data.join("home"))
        .env("BAMBOO_DATA_DIR", data)
        .env("BAMBOO_JIANDU_DATA_DIR", data.join("jiandu"))
        .env("RUST_LOG", "warn");
    c
}
fn spawn(mut c: Command, input: Option<String>, log: &Path) -> Process {
    c.stdin(if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    })
    .stdout(Stdio::from(std::fs::File::create(log).unwrap()))
    .stderr(Stdio::from(
        std::fs::File::create(log.with_extension("stderr")).unwrap(),
    ));
    let mut child = c.spawn().unwrap();
    if let Some(input) = input {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    }
    Process(child, log.to_path_buf())
}
fn address() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}
struct Probe {
    data: PathBuf,
    ids: Mutex<Vec<String>>,
    calls: AtomicUsize,
    hold: AtomicBool,
    held_closed: Arc<AtomicUsize>,
    operation: AtomicUsize,
    turn: AtomicUsize,
    step: AtomicUsize,
    target: AtomicUsize,
}
struct HeldResponse(Arc<AtomicUsize>);
impl Drop for HeldResponse {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
const STALE_OUTPUT: &str = "CANCELLED_OWNER_MUST_NOT_REACH_SUCCESSOR";
async fn response(body: web::Json<Value>, p: web::Data<Probe>) -> HttpResponse {
    let body = body.into_inner();
    assert!(!body.to_string().contains(STALE_OUTPUT));
    let (delta, finish) = if body["model"] == "remote-child" {
        assert!(body["messages"].to_string().contains("REMOTE_NATIVE_TASK"));
        assert!(!body.to_string().contains(HOST));
        p.calls.fetch_add(1, Ordering::SeqCst);
        if p.hold.load(Ordering::SeqCst) {
            let event = json!({"choices":[{"index":0,"delta":{"role":"assistant","content":"INFLIGHT"},"finish_reason":null}]});
            let first = web::Bytes::from(format!("data: {event}\n\n"));
            let closed = HeldResponse(p.held_closed.clone());
            return HttpResponse::Ok()
                .content_type("text/event-stream")
                .streaming(async_stream::stream! {
                    let _closed = closed;
                    yield Ok::<_, std::io::Error>(first);
                    while p.hold.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        // Keep the transport observable while the provider stays held.
                        yield Ok(web::Bytes::from_static(b": held

"));
                    }
                    let end = json!({"choices":[{"index":0,"delta":{"content":"REMOTE_NATIVE_REPLY"},"finish_reason":"stop"}]});
                    yield Ok(web::Bytes::from(format!("data: {end}\n\ndata: [DONE]\n\n")));
                });
        }
        (json!({"content":"REMOTE_NATIVE_REPLY"}), "stop")
    } else if body["model"] == "remote-root" && body["tools"].to_string().contains("SubAgent") {
        let id = format!("remote-op-{}", p.turn.load(Ordering::SeqCst));
        if p.step.fetch_add(1, Ordering::SeqCst) == 0 {
            let op = p.operation.load(Ordering::SeqCst);
            let target = p
                .ids
                .lock()
                .unwrap()
                .get(p.target.load(Ordering::SeqCst))
                .cloned();
            let args = match op {
                0 => json!({"message":"REMOTE_NATIVE_TASK: return one plain reply", "role":ROLE}),
                1 => json!({"intent":"control","target":target.unwrap(),"message":"retry"}),
                2 => json!({"intent":"control","target":target.unwrap(),"message":"cancel"}),
                3 => json!({"intent":"inspect","target":target.unwrap(),"message":"result"}),
                4 => json!({"intent":"chat","target":target.unwrap(),
                    "message":"Repeat REMOTE_NATIVE_TASK: return one plain reply in this existing Actor session"}),
                _ => panic!("unknown remote SubAgent operation {op}"),
            };
            (
                json!({"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":"SubAgent","arguments":args.to_string()}}]}),
                "tool_calls",
            )
        } else {
            let content = body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .rev()
                .find(|m| m["role"] == "tool" && m["tool_call_id"] == id)
                .unwrap()["content"]
                .as_str()
                .unwrap();
            if p.operation.load(Ordering::SeqCst) == 3 {
                assert!(
                    content.contains("REMOTE_NATIVE_REPLY"),
                    "{}",
                    content.chars().take(512).collect::<String>()
                );
                assert!(!content.contains("remote-worker") && !content.contains(HOST));
            } else if p.operation.load(Ordering::SeqCst) != 0 {
                assert!(!content.contains("SubAgent operation failed"), "{content}");
            }
            (
                json!({"content":format!("REMOTE_ROOT_DONE_{}",p.turn.load(Ordering::SeqCst))}),
                "stop",
            )
        }
    } else {
        (json!({"content":"bounded auxiliary response"}), "stop")
    };
    let event = json!({"id":"remote-native","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}
async fn cold(data: &Path, id: &str) -> Session {
    SessionStoreV2::new(data.to_path_buf())
        .await
        .unwrap()
        .load_session(id)
        .await
        .unwrap()
        .unwrap()
}
async fn wait_child(data: &Path, id: &str, status: &str) -> Session {
    wait_child_after(data, id, status, SystemTime::UNIX_EPOCH).await
}
async fn wait_child_after(data: &Path, id: &str, status: &str, since: SystemTime) -> Session {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let reader = SessionStoreV2::new(data.to_path_buf()).await.unwrap();
            let runtime = data
                .join(reader.resolve_rel_path(id).await.unwrap())
                .join("runtime.json");
            let before = std::fs::metadata(&runtime).unwrap().modified().unwrap();
            let child = reader.load_session(id).await.unwrap().unwrap();
            if child.last_run_status().as_deref() == Some(status)
                && before >= since
                && std::fs::metadata(&runtime).unwrap().modified().unwrap() == before
            {
                break child;
            }
            if child.last_run_status().as_deref() == Some("error") && status != "error" {
                panic!(
                    "unexpected Child error: {}",
                    child
                        .last_run_error()
                        .unwrap_or_default()
                        .chars()
                        .take(384)
                        .collect::<String>()
                );
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("actual Child terminal")
}
async fn turn(client: &reqwest::Client, base: &str, p: &Probe, op: usize, target: usize) {
    let existing_wait = if op == 0 && p.hold.load(Ordering::SeqCst) {
        let reader = SessionStoreV2::new(p.data.clone()).await.unwrap();
        reader
            .load_session("remote-root")
            .await
            .unwrap()
            .and_then(|root| root.agent_runtime_state?.waiting_for_children)
    } else {
        None
    };
    let number = p.turn.fetch_add(1, Ordering::SeqCst) + 1;
    p.operation.store(op, Ordering::SeqCst);
    p.target.store(target, Ordering::SeqCst);
    p.step.store(0, Ordering::SeqCst);
    let reply=client.post(format!("{base}/chat")).json(&json!({"session_id":"remote-root","message":format!("Perform remote operation {number}"),
        "model":"remote-root","provider":"openai","model_ref":{"provider":"openai","model":"remote-root"},
        "permission_mode":"bypass","workspace_path":p.data.join("host-workspace")})).send().await.unwrap();
    assert!(
        reply.status().is_success(),
        "{}",
        reply.text().await.unwrap()
    );
    if number == 1 {
        assert!(client
            .post(format!("{base}/execute/remote-root"))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status()
            .is_success());
    }
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let root = cold(&p.data, "remote-root").await;
            let call_id = format!("remote-op-{number}");
            let result = matches!(op, 0 | 1 | 2 | 4)
                .then(|| {
                    root.messages
                        .iter()
                        .rev()
                        .find(|m| m.tool_call_id.as_deref() == Some(&call_id))
                        .map(|m| {
                            assert_eq!(
                                m.tool_success,
                                Some(true),
                                "durable SubAgent {call_id} failed: {}",
                                m.content
                            );
                            serde_json::from_str::<Value>(&m.content)
                                .unwrap_or_else(|error| panic!(
                                    "durable SubAgent {call_id} returned non-JSON content: {error}; {}",
                                    m.content
                                ))
                        })
                })
                .flatten();
            if let Some(result) = result {
                let actor = result["actor_id"].as_str().expect("actual logical ActorId");
                {
                let mut ids = p.ids.lock().unwrap();
                if op == 0 {
                    if !ids.iter().any(|id| id == actor) {
                        ids.push(actor.into());
                    }
                } else {
                    assert_eq!(actor, ids[target]);
                }
                // A successful cancel may suspend on a different held actor.
                // Observe the durable tool result and remaining wait together;
                // the fixture releases that provider only after both cancels.
                if op == 2
                    && result["observed_status"] == "cancelled"
                    && p.hold.load(Ordering::SeqCst)
                    && root.last_run_status().as_deref() == Some("suspended")
                    && root
                        .agent_runtime_state
                        .as_ref()
                        .and_then(|s| s.waiting_for_children.as_ref())
                        .is_some_and(|wait| {
                            !wait.child_session_ids.is_empty()
                                && !wait.child_session_ids.iter().any(|id| id == actor)
                                && wait.child_session_ids.iter().all(|id| ids.contains(id))
                        })
                {
                    break;
                }
                // Held spawn/retry/continuation may suspend with a merged,
                // untagged sibling wait.
                if matches!(op, 0 | 1 | 4)
                    && p.hold.load(Ordering::SeqCst)
                    && root.last_run_status().as_deref() == Some("suspended")
                    && root
                        .agent_runtime_state
                        .as_ref()
                        .and_then(|s| s.waiting_for_children.as_ref())
                        .is_some_and(|wait| {
                            wait.child_session_ids.iter().any(|id| id == actor)
                                && wait.child_session_ids.iter().all(|id| ids.contains(id))
                                && wait
                                    .registered_by_tool_call_id
                                    .as_ref()
                                    .is_none_or(|id| id == &call_id)
                        })
                {
                    break;
                }
                }
                if op == 0
                    && existing_wait.is_some()
                    && result["observed_status"] == "running_in_background"
                {
                    let child = cold(&p.data, actor).await;
                    if child.last_run_status().as_deref() == Some("running")
                        && root.last_run_status().as_deref() == Some("suspended")
                    {
                        assert_eq!(child.id, actor);
                        assert_eq!(child.parent_session_id.as_deref(), Some("remote-root"));
                        assert_eq!(child.root_session_id, "remote-root");
                        assert_eq!(
                            root.agent_runtime_state
                                .as_ref()
                                .and_then(|state| state.waiting_for_children.as_ref()),
                            existing_wait.as_ref()
                        );
                        break;
                    }
                }
            }
            if root
                .messages
                .iter()
                .any(|m| m.content == format!("REMOTE_ROOT_DONE_{number}"))
            {
                break;
            }
            assert_ne!(
                root.last_run_status().as_deref(),
                Some("error"),
                "{:?}",
                root.last_run_error()
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    // This fixture intentionally kills and replaces Hosts between operations.
    // A transcript checkpoint precedes actual Root owner finalization; wait
    // for that durable finish before killing this successful Host. Hard-kill
    // recovery across a still-live Root lease has its own ownership fixture.
    let root = SessionStoreV2::new(p.data.clone()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let current = bamboo_domain::ActorDirectoryPort::inspect_actor(&root, "remote-root")
                .await
                .unwrap();
            if current
                .activation
                .is_some_and(|activation| !activation.status.is_live())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("successful Root owner is durably finished before this fixture replaces its Host");
}
async fn wait_calls(p: &Probe, count: usize) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while p.calls.load(Ordering::SeqCst) != count {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actual resident provider admission");
}
async fn host(data: &Path, config: &Value, mode: &str) -> (Process, String) {
    let configured = serde_json::from_value::<bamboo_config::Config>(config.clone()).unwrap();
    let mut candidate = bamboo_config::Config::from_data_dir_without_env(Some(data.to_path_buf()));
    *candidate.subagents_mut() = configured.subagents().clone();
    candidate.save_to_dir(data.to_path_buf()).unwrap();
    let bind: std::net::SocketAddr = address().parse().unwrap();
    let base = format!("http://{bind}/api/v1");
    let mut c = command(data);
    c.args(["serve", "--bind"])
        .arg(bind.ip().to_string())
        .arg("--port")
        .arg(bind.port().to_string())
        .arg("--data-dir")
        .arg(data);
    if mode == "missing" {
        c.env_remove("BAMBOO_REMOTE_HOST_TOKEN");
    } else {
        c.env(
            "BAMBOO_REMOTE_HOST_TOKEN",
            if mode == "credential" { WORKER } else { HOST },
        );
    }
    let mut process = spawn(c, None, &data.join(format!("host-{mode}.log")));
    let client = reqwest::Client::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "{}",
                std::fs::read_to_string(data.join(format!("host-{mode}.stderr"))).unwrap()
            );
            if client
                .get(format!("{base}/health"))
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
    .unwrap();
    (process, base)
}
fn worker(data: &Path, spec: &ProvisionSpec, url: &str, cert: &Path, label: &str) -> Process {
    let mut c = command(data);
    c.args(["broker-agent", "serve", "--broker"])
        .arg(url)
        .args(["--id", "remote-worker", "--spec-stdin", "--tls-ca-cert"])
        .arg(cert)
        .env("BAMBOO_BROKER_TOKEN", WORKER);
    spawn(
        c,
        Some(spec.to_json().unwrap()),
        &data.join(format!("worker-{label}.log")),
    )
}
async fn wait_runs_settled(data: &Path) {
    let mailbox = data.join("broker/scoped-peers-v1/mailboxes/remote-worker");
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut runs = 0;
            for directory in ["cur", "new"] {
                for entry in std::fs::read_dir(mailbox.join(directory)).unwrap() {
                    match std::fs::read(entry.unwrap().path()) {
                        Ok(bytes) => {
                            let message: bamboo_subagent::InboxMessage =
                                serde_json::from_slice(&bytes).unwrap();
                            runs += usize::from(matches!(
                                message.kind,
                                bamboo_subagent::InboxKind::Run
                                    | bamboo_subagent::InboxKind::LeasedRun
                                    | bamboo_subagent::InboxKind::FencedRun
                            ));
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => panic!("physical Run observation: {error}"),
                    }
                }
            }
            if runs == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actual old Run completed and ACKed before operator replacement");
}
fn mailbox_messages(data: &Path, mailbox: &str) -> Vec<(PathBuf, bamboo_subagent::InboxMessage)> {
    let root = data.join("broker/scoped-peers-v1/mailboxes").join(mailbox);
    let mut messages = Vec::new();
    for directory in ["new", "cur"] {
        for entry in std::fs::read_dir(root.join(directory)).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            match std::fs::read(&path) {
                Ok(bytes) => messages.push((path, serde_json::from_slice(&bytes).unwrap())),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("physical broker mailbox: {error}"),
            }
        }
    }
    messages
}
async fn placement(data: &Path, id: &str) -> (ActorActivation, WorkerSlotLease) {
    let store = SessionStoreV2::new(data.to_path_buf()).await.unwrap();
    let activation = store.inspect_actor(id).await.unwrap().activation.unwrap();
    assert_eq!(activation.status, ActorActivationStatus::Running);
    let reference = activation.placement_ref.as_ref().unwrap();
    let registry = FileHostRegistry::new(data.to_path_buf()).await.unwrap();
    let lease = registry
        .inspect_slot_by_lease_id(&reference.lease_id, id, &activation.run_id, Utc::now())
        .await
        .unwrap();
    assert_eq!(reference.slot_epoch, Some(lease.epoch));
    registry.validate_slot(&lease, Utc::now()).await.unwrap();
    (activation, lease)
}
fn transcript_digest(session: &Session) -> String {
    let bytes = serde_json::to_vec(&serde_json::to_value(&session.messages).unwrap()).unwrap();
    let hash = Sha256::digest(bytes);
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut encoded = String::new();
    for chunk in hash.chunks(3) {
        let bits = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for shift in [18, 12, 6, 0].into_iter().take(chunk.len() + 1) {
            encoded.push(alphabet[((bits >> shift) & 63) as usize] as char);
        }
    }
    encoded
}
async fn terminal_ack(
    data: &Path,
    session: &Session,
    activation: &ActorActivation,
    status: &str,
) -> BrokerTerminalReceipt {
    let store = SessionStoreV2::new(data.to_path_buf()).await.unwrap();
    let path = data
        .join(store.resolve_rel_path(&session.id).await.unwrap())
        .join("broker-terminal-receipts.v1.json");
    let receipt = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                Err(error) => panic!("actual terminal receipt: {error}"),
            };
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            if let Ok(receipt) = serde_json::from_value::<BrokerTerminalReceipt>(
                value["acknowledged_anchor"].clone(),
            ) {
                if receipt.activation_run_id == activation.run_id {
                    assert_eq!(value["receipts"], json!([]));
                    break receipt;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actual Host terminal receipt confirmed ACK");
    assert_eq!(receipt.session_id, session.id);
    assert_eq!(receipt.created_at, session.created_at);
    assert_eq!(
        Some(receipt.parent_session_id.as_str()),
        session.parent_session_id.as_deref()
    );
    assert_eq!(receipt.root_session_id, session.root_session_id);
    assert_eq!(receipt.terminal_status, status);
    assert_eq!(receipt.message_count, session.messages.len());
    assert_eq!(receipt.messages_sha256, transcript_digest(session));
    assert!(!receipt.message_ids.is_empty());
    assert_eq!(
        receipt
            .message_ids
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        receipt.message_ids.len()
    );
    assert!(!receipt.broker_correlation_id.is_empty());
    assert_eq!(receipt.parent_mailbox, "remote-parent");
    match (
        &receipt.required_execution_epoch,
        &receipt.terminal_completeness,
    ) {
        (Some(epoch), Some(proof)) => {
            assert_eq!(proof.execution_epoch, *epoch);
            assert_eq!(proof.activation_run_id, activation.run_id);
            assert_eq!(proof.messages_sha256, receipt.messages_sha256);
            assert_eq!(proof.message_count, receipt.message_count);
        }
        (None, None) => {}
        _ => panic!("strict receipt requires matching Host completeness proof"),
    }
    assert!(mailbox_messages(data, &receipt.parent_mailbox)
        .iter()
        .all(|(_, msg)| !receipt.message_ids.contains(&msg.id.0)));
    receipt
}
struct RemoteProofContext<'a> {
    data: &'a PathBuf,
    child_id: &'a str,
    client: &'a reqwest::Client,
    base: &'a str,
    p: &'a Probe,
    resident: &'a mut Process,
    url: &'a str,
    cert: &'a Path,
}
async fn cancelled_slot_reuse(context: RemoteProofContext<'_>) {
    let RemoteProofContext {
        data,
        child_id,
        client,
        base,
        p,
        resident,
        url,
        cert,
    } = context;
    let resident_pid = resident.0.id();
    let (cancelled_activation, old_slot) = placement(data, child_id).await;
    let old_event = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(message) = mailbox_messages(data, "remote-parent")
                .into_iter()
                .map(|(_, message)| message)
                .find(|message| {
                    message.kind == bamboo_subagent::InboxKind::Event
                        && serde_json::from_value::<bamboo_subagent::ActorEventBatch>(
                            message.body.clone(),
                        )
                        .is_ok_and(|batch| batch.execution_epoch != 0)
                })
            {
                break message;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("capture actual held Run event route before cancellation");
    turn(client, base, p, 0, 0).await;
    let sibling = p.ids.lock().unwrap()[1].clone();
    turn(client, base, p, 2, 1).await;
    wait_child(data, &sibling, "cancelled").await;
    assert_eq!(
        p.calls.load(Ordering::SeqCst),
        2,
        "cancelled subscription waiter must not dispatch Run"
    );
    turn(client, base, p, 2, 0).await;
    let cancelled = wait_child(data, child_id, "cancelled").await;
    let registry = FileHostRegistry::new(data.clone()).await.unwrap();
    let authority = SessionStoreV2::new(data.clone()).await.unwrap();
    let finished = authority
        .inspect_actor(child_id)
        .await
        .unwrap()
        .activation
        .unwrap();
    assert_eq!(finished.fence(), cancelled_activation.fence());
    assert_eq!(finished.status, ActorActivationStatus::Cancelled);
    let cancelled_receipt =
        terminal_ack(data, &cancelled, &cancelled_activation, "cancelled").await;
    assert_eq!(
        old_event.correlation_id.as_ref().map(|id| id.0.as_str()),
        Some(cancelled_receipt.broker_correlation_id.as_str())
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while p.held_closed.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cancel closes actual held provider stream before fixture release or successor");
    assert!(p.hold.load(Ordering::SeqCst));
    wait_runs_settled(data).await;
    assert!(registry
        .inspect_host(&old_slot.host_ref)
        .await
        .unwrap()
        .unwrap()
        .slots
        .is_empty());
    assert!(resident.0.try_wait().unwrap().is_none());

    // Reuse the same live resident, before any operator replacement or lease expiry.
    turn(client, base, p, 1, 0).await;
    wait_calls(p, 3).await;
    let (successor, successor_slot) = placement(data, child_id).await;
    assert_eq!(resident.0.id(), resident_pid);
    assert!(resident.0.try_wait().unwrap().is_none());
    assert_eq!(successor_slot.host_ref, old_slot.host_ref);
    assert_eq!(
        successor_slot.connection_generation,
        old_slot.connection_generation
    );
    assert_eq!(successor_slot.slot, old_slot.slot);
    assert_ne!(successor_slot.lease_id, old_slot.lease_id);
    assert!(successor_slot.epoch > old_slot.epoch);
    assert_ne!(successor.fence(), cancelled_activation.fence());
    assert_ne!(successor.run_id, cancelled_activation.run_id);
    assert!(successor.lease_epoch > cancelled_activation.lease_epoch);
    assert!(matches!(
        registry.release_slot(&old_slot).await,
        Err(HostRegistryError::StaleLease)
    ));
    assert!(matches!(
        registry
            .renew_slot(
                &old_slot,
                Utc::now(),
                Utc::now() + ChronoDuration::seconds(80)
            )
            .await,
        Err(HostRegistryError::StaleLease)
    ));
    assert!(matches!(
        authority
            .renew_activation(
                &cancelled_activation.fence(),
                Utc::now(),
                Utc::now() + ChronoDuration::seconds(80)
            )
            .await,
        Err(ActorDirectoryError::StaleFence)
    ));
    assert!(matches!(
        authority
            .checkpoint_activation(
                &cancelled_activation.fence(),
                Utc::now(),
                cancelled_activation.checkpoint_revision
            )
            .await,
        Err(ActorDirectoryError::StaleFence)
    ));
    registry
        .validate_slot(&successor_slot, Utc::now())
        .await
        .unwrap();
    authority
        .validate_fence(&successor.fence(), Utc::now())
        .await
        .unwrap();

    // This authenticated publish-only connection does not replace the resident subscription.
    let mut publisher = BrokerClient::connect_with_tls(
        url,
        old_event.from.clone(),
        WORKER,
        Some(client_config_trusting_cert(cert).unwrap()),
    )
    .await
    .unwrap();
    let mut stale = old_event;
    let mut batch: bamboo_subagent::ActorEventBatch = serde_json::from_value(stale.body).unwrap();
    batch.first_seq = batch.last_seq.checked_add(1).unwrap();
    batch.last_seq = batch.first_seq;
    batch.qos = bamboo_subagent::ActorEventQos::Durable;
    batch.events = vec![json!({"type":"token","content":STALE_OUTPUT})];
    batch.validate().unwrap();
    stale.id = bamboo_subagent::MsgId::new();
    stale.created_at = Utc::now();
    stale.body = serde_json::to_value(batch).unwrap();
    let stale_id = stale.id.clone();
    assert_eq!(
        publisher
            .deliver(&cancelled_receipt.parent_mailbox, stale)
            .await
            .unwrap(),
        stale_id
    );
    drop(publisher);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if mailbox_messages(data, "remote-parent")
                .iter()
                .any(|(path, msg)| msg.id == stale_id && path.parent().unwrap().ends_with("cur"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actual stale output delivered to current Host, retained for old Run recovery");
    assert_eq!(
        authority
            .inspect_actor(child_id)
            .await
            .unwrap()
            .activation
            .unwrap()
            .fence(),
        successor.fence()
    );
    registry
        .validate_slot(&successor_slot, Utc::now())
        .await
        .unwrap();
    assert!(!serde_json::to_string(&cold(data, child_id).await.messages)
        .unwrap()
        .contains(STALE_OUTPUT));
    p.hold.store(false, Ordering::SeqCst);
    let reused = wait_child(data, child_id, "completed").await;
    let reused_receipt = terminal_ack(data, &reused, &successor, "completed").await;
    assert!(!reused_receipt.message_ids.contains(&stale_id.0));
    assert!(!serde_json::to_string(&reused.messages)
        .unwrap()
        .contains(STALE_OUTPUT));
    assert!(mailbox_messages(data, "remote-parent")
        .iter()
        .any(|(_, msg)| msg.id == stale_id));
    assert_eq!(p.calls.load(Ordering::SeqCst), 3);
    assert!(registry
        .inspect_host(&old_slot.host_ref)
        .await
        .unwrap()
        .unwrap()
        .slots
        .is_empty());
    assert_eq!(
        authority
            .inspect_actor(child_id)
            .await
            .unwrap()
            .activation
            .unwrap()
            .status,
        ActorActivationStatus::Succeeded
    );
    for entry in std::fs::read_dir(data.join("events")).unwrap() {
        assert!(!std::fs::read_to_string(entry.unwrap().path())
            .unwrap()
            .contains(STALE_OUTPUT));
    }
    eprintln!(
        "remote cancel slot proof: {}",
        json!({"resident_pid":resident_pid,
        "old_slot":old_slot,"successor_slot":successor_slot,"cancelled_fence":cancelled_activation.fence(),
        "successor_fence":successor.fence(),"cancelled_ack_count":cancelled_receipt.message_ids.len(),
        "successor_ack_count":reused_receipt.message_ids.len(),"required_execution_epoch":cancelled_receipt.required_execution_epoch,
        "held_provider_closed":p.held_closed.load(Ordering::SeqCst),"stale_delivered_retained_id":stale_id,
        "provider_admissions":p.calls.load(Ordering::SeqCst)})
    );
    wait_runs_settled(data).await;
}
#[actix_web::test]
async fn actual_host_pinned_remote_runs_cancels_and_explicitly_replaces_over_wss() {
    Box::pin(fixture()).await;
}
async fn fixture() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.keep().canonicalize().unwrap();
    eprintln!("remote broker native evidence: {}", data.display());
    let host_workspace = data.join("host-workspace");
    let worker_workspace = data.join("worker-workspace");
    let replacement_workspace = data.join("replacement-worker-workspace");
    assert!(Command::new("git")
        .args(["init", "-q"])
        .arg(&host_workspace)
        .status()
        .unwrap()
        .success());
    std::fs::write(host_workspace.join("README.md"), "same portable bytes\n").unwrap();
    assert!(Command::new("git")
        .arg("-C")
        .arg(&host_workspace)
        .args(["add", "README.md"])
        .status()
        .unwrap()
        .success());
    assert!(Command::new("git")
        .arg("-C")
        .arg(&host_workspace)
        .args([
            "-c",
            "user.name=Remote Test",
            "-c",
            "user.email=remote@test.invalid",
            "commit",
            "-qm",
            "snapshot"
        ])
        .status()
        .unwrap()
        .success());
    assert!(Command::new("git")
        .args(["clone", "-q", "--local"])
        .arg(&host_workspace)
        .arg(&worker_workspace)
        .status()
        .unwrap()
        .success());
    assert!(Command::new("git")
        .args(["clone", "-q", "--local"])
        .arg(&host_workspace)
        .arg(&replacement_workspace)
        .status()
        .unwrap()
        .success());
    assert_ne!(host_workspace, worker_workspace);
    assert_ne!(worker_workspace, replacement_workspace);
    std::fs::create_dir(data.join("home")).unwrap();
    let (cert, key) = (data.join("cert.pem"), data.join("key.pem"));
    assert!(Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-addext",
            "basicConstraints=critical,CA:FALSE"
        ])
        .arg("-keyout")
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .output()
        .expect("required openssl unavailable")
        .status
        .success());
    let bind = address();
    let url = format!("wss://{bind}");
    let expiry = Utc::now() + ChronoDuration::minutes(15);
    let policy = json!({"peers":[{"credential":HOST,"mailbox":"remote-parent","role":"host","host":"host-node","expires_at":expiry,
        "destinations":[{"mailbox":"remote-worker","kinds":["fenced_run","steer"]}],"cancel":["remote-worker"],"presence":["worker"]},
        {"credential":WORKER,"mailbox":"remote-worker","role":"worker","host":"worker-node","expires_at":expiry,
        "destinations":[{"mailbox":"remote-parent","kinds":["event","outcome","session_message_admitted","approval_request"]}],
        "max_slots":1,
        "host_capabilities":{"placement_class":"remote","project_ids":[],"allow_unscoped_project":true,
            "trust_zone":"trusted","workspace_labels":["clean-git"],"executors":["bamboo-runtime"],
            "tools":[],"network_zones":["internal"],"network_isolation":true}},
        {"credential":OBSERVER,"mailbox":"remote-observer","role":"observer","host":"observer-node","expires_at":expiry,
        "destinations":[],"presence":["worker"]}]});
    let mut c = command(&data);
    c.args(["broker", "serve", "--bind"])
        .arg(&bind)
        .arg("--root")
        .arg(data.join("broker"))
        .arg("--peer-policy-stdin")
        .arg("--cert")
        .arg(&cert)
        .arg("--key")
        .arg(&key);
    let mut _broker = spawn(c, Some(policy.to_string()), &data.join("broker.log"));
    let p = web::Data::new(Probe {
        data: data.clone(),
        ids: Mutex::new(vec![]),
        calls: AtomicUsize::new(0),
        hold: AtomicBool::new(false),
        held_closed: Arc::new(AtomicUsize::new(0)),
        operation: AtomicUsize::new(0),
        turn: AtomicUsize::new(0),
        step: AtomicUsize::new(0),
        target: AtomicUsize::new(0),
    });
    let clone = p.clone();
    let http = HttpServer::new(move || {
        App::new()
            .app_data(clone.clone())
            .route("/v1/chat/completions", web::post().to(response))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok()
                        .json(json!({"data":[{"id":"remote-root"},{"id":"remote-child"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let provider = format!("http://{}/v1", http.addrs()[0]);
    let running = http.run();
    let handle = running.handle();
    actix_web::rt::spawn(running);
    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: "remote-worker".into(),
            parent_id: Some("remote-root".into()),
            project_key: None,
            role: "worker".into(),
            depth: 1,
        },
        ExecutorSpec::BambooRuntime,
        data.join("fabric").to_string_lossy().into_owned(),
    );
    spec.storage_dir = Some(data.join("worker-cache").to_string_lossy().into_owned());
    spec.workspace = Some(worker_workspace.to_string_lossy().into_owned());
    spec.model = Some(ModelRefSpec {
        provider: "openai".into(),
        model: "remote-child".into(),
    });
    spec.capabilities.bypass = true;
    spec.capabilities.enforce_permissions = true;
    spec.capabilities.child_creation_identity = true;
    spec.secrets.provider_credentials.push(ScopedCredential {
        provider: "openai".into(),
        api_key: "fixture-key".into(),
        base_url: Some(provider.clone()),
        provider_type: None,
        credential_ref: None,
    });
    spec.bus = Some(BusEndpoint {
        endpoint: url.clone(),
        token: WORKER.into(),
    });
    // A replacement resident has its own checkout and private cache. The
    // broker mailbox and Host-owned Child identity remain stable across the
    // explicit handoff after the old Run has ACKed.
    let mut replacement_spec = spec.clone();
    replacement_spec.storage_dir = Some(
        data.join("replacement-worker-cache")
            .to_string_lossy()
            .into_owned(),
    );
    replacement_spec.workspace = Some(replacement_workspace.to_string_lossy().into_owned());
    let mut last_connect_error = None;
    let mut observer = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match BrokerClient::connect_with_tls(
                &url,
                AgentRef {
                    session_id: "remote-parent".into(),
                    role: Some("host".into()),
                },
                HOST,
                Some(client_config_trusting_cert(&cert).unwrap()),
            )
            .await
            {
                Ok(client) => break client,
                Err(error) => last_connect_error = Some(diagnostic_text(&error.to_string())),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "original broker TLS deadline: {error}; last_connect_error={last_connect_error:?}; {}",
            _broker.diagnostic()
        )
    });
    // A scoped old Worker can subscribe but has no lease capability. The Host
    // rejects it at placement, before it can receive a Run or call a provider.
    let mut legacy_worker = BrokerClient::connect_with_tls(
        &url,
        AgentRef {
            session_id: "remote-worker".into(),
            role: Some("worker".into()),
        },
        WORKER,
        Some(client_config_trusting_cert(&cert).unwrap()),
    )
    .await
    .unwrap();
    legacy_worker.subscribe().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if observer
                .observe_host("remote-worker", "worker")
                .await
                .unwrap()
                .is_some_and(|host| !host.environment_lease_v1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let unsupported = bamboo_broker::BrokerChildLink::connect_strict_with_tls_environment_lease(
        &url,
        AgentRef {
            session_id: "remote-parent".into(),
            role: Some("host".into()),
        },
        HOST,
        AgentRef {
            session_id: "remote-worker".into(),
            role: Some("worker".into()),
        },
        client_config_trusting_cert(&cert).unwrap(),
    )
    .await
    .err()
    .expect("old Worker must be refused before Run");
    assert!(unsupported
        .to_string()
        .contains("remote_environment_lease_unsupported"));
    assert_eq!(p.calls.load(Ordering::SeqCst), 0);
    drop(legacy_worker);
    let mut resident = worker(&data, &spec, &url, &cert, "first");
    tokio::time::timeout(Duration::from_secs(30), async {
        while !observer
            .observe_host("remote-worker", "worker")
            .await
            .unwrap()
            .is_some_and(|host| host.environment_lease_v1)
        {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    drop(observer); // Fixed parent identity cannot have concurrent subscribers.
    let config = json!({"provider":"openai","features":{"provider_model_ref":true},"providers":{"openai":{"api_key":"fixture-key","base_url":provider,"model":"remote-root"}},
        "defaults":{"chat":{"provider":"openai","model":"remote-root"},"subagent_models":{"worker":{"provider":"openai","model":"remote-child"}}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":2,"remote_placements":[{"role":ROLE,"endpoint":url,
            "token_env":"BAMBOO_REMOTE_HOST_TOKEN","ca_cert_file":cert,"broker_peer":{"parent_mailbox":"remote-parent","parent_role":"host","worker_mailbox":"remote-worker","worker_role":"worker"},
            "placement_requirements":{"trust_zone":"trusted","workspace_label":"clean-git","network_zone":"internal","require_network_isolation":true}}]}});
    // Initialize provider/defaults once; restarts update only the selected route.
    {
        let mut candidate =
            serde_json::from_value::<bamboo_config::Config>(config.clone()).unwrap();
        bamboo_config::persist_provider_credential_transaction(
            &data,
            &mut candidate,
            &std::collections::BTreeSet::from(["openai".to_owned()]),
        )
        .unwrap();
        candidate.save_to_dir(data.clone()).unwrap();
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let mut child_id = String::new();
    for mode in ["missing", "credential", "role", "ca"] {
        let mut configured = config.clone();
        if mode == "role" {
            configured["subagents"]["remote_placements"][0]["broker_peer"]["worker_role"] =
                json!("incorrect");
        }
        if mode == "ca" {
            configured["subagents"]["remote_placements"][0]["ca_cert_file"] =
                json!(data.join("absent-ca"));
        }
        let (mut h, base) = host(&data, &configured, mode).await;
        let requested_at = SystemTime::now();
        turn(
            &client,
            &base,
            &p,
            if child_id.is_empty() { 0 } else { 1 },
            0,
        )
        .await;
        child_id = p.ids.lock().unwrap()[0].clone();
        let failed = wait_child_after(&data, &child_id, "error", requested_at).await;
        assert_eq!(p.calls.load(Ordering::SeqCst), 0, "{mode}");
        assert!(!failed.last_run_error().unwrap_or_default().contains(HOST));
        h.stop();
    }
    let original = cold(&data, &child_id).await;
    let (mut h, base) = host(&data, &config, "valid").await;
    std::fs::write(worker_workspace.join("README.md"), "dirty worker bytes\n").unwrap();
    let dirty_requested_at = SystemTime::now();
    turn(&client, &base, &p, 1, 0).await;
    let dirty = wait_child_after(&data, &child_id, "error", dirty_requested_at).await;
    assert!(dirty
        .last_run_error()
        .unwrap_or_default()
        .contains("remote_environment_checkout_not_clean"));
    assert_eq!(p.calls.load(Ordering::SeqCst), 0);
    std::fs::write(worker_workspace.join("README.md"), "same portable bytes\n").unwrap();
    let public: Value = client
        .get(format!("{base}/bamboo/config"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let route = public["subagents"]["remote_placements"].to_string();
    for secret in [
        &url,
        "remote-parent",
        "remote-worker",
        "BAMBOO_REMOTE_HOST_TOKEN",
        cert.to_str().unwrap(),
        HOST,
    ] {
        assert!(!route.contains(secret));
    }
    for invalid in [Value::Null, json!(7)] {
        assert!(client
            .post(format!("{base}/bamboo/config"))
            .json(&json!({"subagents":invalid}))
            .send()
            .await
            .unwrap()
            .status()
            .is_client_error());
        let saved = bamboo_config::Config::from_data_dir_without_env(Some(data.clone()));
        assert_eq!(
            serde_json::to_value(&saved.subagents().remote_placements).unwrap(),
            config["subagents"]["remote_placements"]
        );
    }
    assert!(client
        .post(format!("{base}/bamboo/config"))
        .json(&public)
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    let saved = bamboo_config::Config::from_data_dir_without_env(Some(data.clone()));
    assert_eq!(
        serde_json::to_value(&saved.subagents().remote_placements).unwrap(),
        config["subagents"]["remote_placements"]
    );
    turn(&client, &base, &p, 1, 0).await;
    wait_calls(&p, 1).await;
    let completed = wait_child(&data, &child_id, "completed").await;
    assert_eq!(completed.created_at, original.created_at);
    assert_eq!(completed.parent_session_id.as_deref(), Some("remote-root"));
    assert_eq!(completed.root_session_id, "remote-root");
    assert!(completed.project_id_meta().is_none());
    assert_eq!(
        completed.metadata.get("placement").unwrap(),
        "{\"host\":\"remote\",\"kind\":\"remote\"}"
    );
    turn(&client, &base, &p, 3, 0).await;
    p.hold.store(true, Ordering::SeqCst);
    // A completed answer is protected by the broker ACK transcript anchor.
    // Continue this logical Actor with a new durable user turn; control retry
    // below still covers failed/cancelled Runs without rewriting that answer.
    turn(&client, &base, &p, 4, 0).await;
    wait_calls(&p, 2).await;
    Box::pin(cancelled_slot_reuse(RemoteProofContext {
        data: &data,
        child_id: &child_id,
        client: &client,
        base: &base,
        p: &p,
        resident: &mut resident,
        url: &url,
        cert: &cert,
    }))
    .await;
    resident.stop();
    assert!(resident.0.try_wait().unwrap().is_some());
    std::fs::write(
        replacement_workspace.join("README.md"),
        "different replacement bytes\n",
    )
    .unwrap();
    resident = worker(&data, &replacement_spec, &url, &cert, "replacement");
    let mut replacement_observer = BrokerClient::connect_with_tls(
        &url,
        AgentRef {
            session_id: "remote-observer".into(),
            role: Some("observer".into()),
        },
        OBSERVER,
        Some(client_config_trusting_cert(&cert).unwrap()),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !replacement_observer
            .list_connected("worker")
            .await
            .unwrap()
            .contains(&"remote-worker".to_string())
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    drop(replacement_observer);
    let mismatched_requested_at = SystemTime::now();
    // Same-resident reuse has completed and ACKed this answer. A new durable
    // turn exercises replacement admission; retry below still covers its failed Run.
    turn(&client, &base, &p, 4, 0).await;
    let mismatched = wait_child_after(&data, &child_id, "error", mismatched_requested_at).await;
    assert!(
        mismatched
            .last_run_error()
            .unwrap_or_default()
            .contains("remote_environment_checkout_not_clean"),
        "replacement checkout rejection: Child={} parent={:?} root={} generation={} run={:?} status={:?} error={:?} calls={} host={} worker={} broker={}",
        mismatched.id,
        mismatched.parent_session_id,
        mismatched.root_session_id,
        mismatched.child_launch_generation(),
        mismatched.agent_runtime_state.as_ref().map(|state| &state.run_id),
        mismatched.last_run_status(),
        diagnostic_text(&mismatched.last_run_error().unwrap_or_default()),
        p.calls.load(Ordering::SeqCst),
        h.diagnostic(),
        resident.diagnostic(),
        _broker.diagnostic(),
    );
    assert_eq!(
        p.calls.load(Ordering::SeqCst),
        3,
        "a replacement with a different checkout must not call the provider"
    );
    std::fs::write(
        replacement_workspace.join("README.md"),
        "same portable bytes\n",
    )
    .unwrap();
    turn(&client, &base, &p, 1, 0).await;
    wait_calls(&p, 4).await;
    wait_child(&data, &child_id, "completed").await;
    p.hold.store(true, Ordering::SeqCst);
    turn(&client, &base, &p, 4, 0).await;
    wait_calls(&p, 5).await;
    h.stop();
    // Disconnection is not worker Run anti-replay. Settle the actual old Run
    // before explicit quiescent replacement; stale parent frames remain queued.
    p.hold.store(false, Ordering::SeqCst);
    wait_runs_settled(&data).await;
    resident.stop();
    assert!(h.0.try_wait().unwrap().is_some() && resident.0.try_wait().unwrap().is_some());
    // SIGKILL left no Host-validated terminal checkpoint. The Worker's old
    // broker Outcome is still queued, but cannot alone retire the Actor fence.
    // Use the recorded lease deadline for safe cold takeover; the new link
    // must still ignore the stale Event/Outcome frames from that old Run.
    let orphaned = SessionStoreV2::new(data.to_path_buf())
        .await
        .unwrap()
        .inspect_actor(&child_id)
        .await
        .unwrap()
        .activation
        .expect("old Actor activation after Host SIGKILL");
    assert_eq!(orphaned.status, ActorActivationStatus::Running);
    let remaining = (orphaned.lease_expires_at - Utc::now())
        .to_std()
        .unwrap_or_default();
    assert!(
        remaining <= Duration::from_secs(90),
        "unexpectedly long orphaned Actor lease: {remaining:?}"
    );
    tokio::time::sleep(remaining + Duration::from_millis(200)).await;
    resident = worker(&data, &replacement_spec, &url, &cert, "cold");
    let (mut h, base) = host(&data, &config, "cold").await;
    turn(&client, &base, &p, 1, 0).await;
    wait_calls(&p, 6).await;
    let final_child = wait_child(&data, &child_id, "completed").await;
    h.stop();
    resident.stop();
    let cached = cold(&data.join("replacement-worker-cache"), &child_id).await;
    assert_eq!(cached.workspace.as_deref(), replacement_workspace.to_str());
    assert_ne!(cached.workspace.as_deref(), worker_workspace.to_str());
    assert_ne!(cached.workspace.as_deref(), host_workspace.to_str());
    for session in [&final_child, &cached] {
        assert_eq!(session.id, child_id);
        assert_eq!(session.created_at, original.created_at);
        assert_eq!(session.parent_session_id, original.parent_session_id);
        assert_eq!(session.root_session_id, original.root_session_id);
        assert_eq!(session.spawn_depth, original.spawn_depth);
        assert!(session.project_id_meta().is_none());
        assert!(session
            .messages
            .iter()
            .any(|m| m.content == "REMOTE_NATIVE_REPLY"));
    }
    assert_eq!(p.calls.load(Ordering::SeqCst), 6);
    handle.stop(true).await;
    drop(_broker);
    std::fs::remove_dir_all(&data).unwrap();
}
