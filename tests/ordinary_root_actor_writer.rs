//! Real ordinary Root execution authority, independent Hosts, and a surviving
//! old provider stream. Only the provider and fixture-owned process scheduling
//! are controlled. The fixture never creates an Actor claim for either Host.
#![cfg(unix)]

use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::{storage::Storage, AgentEvent};
use bamboo_domain::{
    ActorActivationStatus, ActorDirectoryEntry, ActorDirectoryError, ActorDirectoryPort, Role,
    Session, SessionActivationPolicy, SessionInboxLimits, SessionInboxPort, SessionMessageEnvelope,
};
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
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

// Nonblocking probes close each independent descriptor immediately. They
// never create lock files, retain authority, or change a production lease.
fn blocked_file_locks(locks: &[(PathBuf, bool)]) -> std::io::Result<Vec<PathBuf>> {
    let mut blocked = Vec::new();
    for (path, shared) in locks {
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let result = if *shared {
            file.try_lock_shared()
        } else {
            file.try_lock()
        };
        match result {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => blocked.push(path.clone()),
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
        }
    }
    Ok(blocked)
}

struct Host {
    child: Child,
    base: String,
    log: PathBuf,
    stopped: bool,
}

impl Host {
    fn start(data: &Path, name: &str) -> Self {
        Self::start_with_control(data, name, None)
    }

    fn start_with_control(data: &Path, name: &str, control: Option<&Path>) -> Self {
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        let path = data.join(format!("host-{name}.log"));
        let log = std::fs::File::create(&path).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_bamboo"));
        command
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
            .stderr(Stdio::from(log));
        if let Some(control) = control {
            command
                .args(["--parent-pid", &std::process::id().to_string()])
                .env("BAMBOO_MANAGED_SHUTDOWN_DIR", control);
        }
        let child = command.spawn().unwrap();
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

    async fn stop_clear_of_file_locks(
        &mut self,
        locks: &[(PathBuf, bool)],
        mut on_blocked: impl FnMut(&[PathBuf]),
    ) -> Result<usize, String> {
        let mut rejected = 0;
        let mut last_blocked = Vec::new();
        // A lease cannot release an OS file lock held by a SIGSTOPed process.
        // Pause only between the same physical boundaries B must cross. The
        // original total 2s stop budget includes every resumed retry.
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                self.signal("-STOP");
                loop {
                    let status = Command::new("ps")
                        .args(["-o", "stat=", "-p", &self.child.id().to_string()])
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
                last_blocked = blocked_file_locks(locks).map_err(|error| error.to_string())?;
                if last_blocked.is_empty() {
                    return Ok(rejected);
                }
                rejected += 1;
                on_blocked(&last_blocked);
                self.signal("-CONT");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if !matches!(result, Ok(Ok(_))) && self.stopped {
            self.signal("-CONT");
        }
        result.map_err(|_| {
            format!(
                "fixture-owned Host cannot pause clear of file locks within 2s: {last_blocked:?}"
            )
        })?
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

const UPDATE_MODEL: &str = "managed-update-root";
const UPDATE_PROMPTS: [&str; 2] = ["MANAGED_UPDATE_FIRST", "MANAGED_UPDATE_SECOND"];
const UPDATE_READBACK: &str = "durable result from the predecessor";
const UPDATE_QUEUED_INPUT: &str = "Durable follow-up queued before successor startup";

struct UpdateProbe {
    path: PathBuf,
    calls: [AtomicUsize; 2],
    release: tokio::sync::watch::Sender<bool>,
}

async fn update_provider(body: web::Json<Value>, probe: web::Data<UpdateProbe>) -> HttpResponse {
    let (delta, finish) = if body["model"] != UPDATE_MODEL {
        (json!({"content":"auxiliary"}), "stop")
    } else {
        let messages = body["messages"].as_array().unwrap();
        let index = UPDATE_PROMPTS
            .iter()
            .position(|prompt| {
                messages.iter().any(|message| {
                    message["role"] == "user" && message["content"].to_string().contains(prompt)
                })
            })
            .expect("the foreground call retains the original User input");
        let tool_id = format!("managed-update-read-{index}");
        match probe.calls[index].fetch_add(1, Ordering::SeqCst) {
            0 => {
                let mut release = probe.release.subscribe();
                release.wait_for(|ready| *ready).await.unwrap();
                (
                    json!({"role":"assistant","tool_calls":[{
                        "index":0,"id":tool_id,"type":"function","function":{
                            "name":"Read","arguments":json!({"file_path":probe.path}).to_string()
                        }
                    }]}),
                    "tool_calls",
                )
            }
            1 => {
                assert_eq!(
                    messages
                        .iter()
                        .filter(|message| {
                            message["role"] == "tool"
                                && message["tool_call_id"] == tool_id
                                && message["content"].to_string().contains(UPDATE_READBACK)
                        })
                        .count(),
                    1,
                    "the successor receives the one persisted tool result"
                );
                (
                    json!({"content":"Resumed after the planned update."}),
                    "stop",
                )
            }
            _ => panic!("recovery must not schedule the same continuation twice"),
        }
    };
    HttpResponse::Ok().content_type("text/event-stream").body(
        [
            chunk(delta, Some(finish)),
            web::Bytes::from_static(b"data: [DONE]\n\n"),
        ]
        .concat(),
    )
}

async fn send_update_command(
    control: &Path,
    host: &Host,
    request: uuid::Uuid,
    sequence: &mut u64,
    command: &str,
) -> Value {
    *sequence += 1;
    let file = tempfile::NamedTempFile::new_in(control).unwrap();
    serde_json::to_writer(
        file.as_file(),
        &json!({"version":1,"request_id":request,"sequence":*sequence,"command":command}),
    )
    .unwrap();
    file.persist(control.join("planned-update-command.json"))
        .unwrap();
    let reply = control.join("planned-update-result.json");
    tokio::time::timeout(WAIT, async {
        loop {
            match std::fs::read(&reply) {
                Ok(bytes) => {
                    let value: Value = serde_json::from_slice(&bytes).unwrap();
                    if value["sequence"] == *sequence {
                        assert_eq!(value["request_id"], request.to_string());
                        assert_eq!(value["version"], 1);
                        return value;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => panic!("read managed update response: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("managed update {command} timed out: {}", host.tail()))
}

async fn expect_graceful_exit(host: &mut Host) {
    tokio::time::timeout(WAIT, async {
        loop {
            if let Some(status) = host.child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "managed Host exit: {status}: {}",
                    host.tail()
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("managed Host is still running: {}", host.tail()));
}

#[actix_web::test]
async fn planned_update_commits_real_predecessor_and_resumes_roots_in_a_successor() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let data = root.join("profile");
    let workspace = root.join("workspace");
    std::fs::create_dir(&data).unwrap();
    std::fs::create_dir(&workspace).unwrap();
    let path = workspace.join("predecessor.txt");
    std::fs::write(&path, UPDATE_READBACK).unwrap();
    let (release, _) = tokio::sync::watch::channel(false);
    let probe = web::Data::new(UpdateProbe {
        path,
        calls: std::array::from_fn(|_| AtomicUsize::new(0)),
        release,
    });
    let captured = probe.clone();
    let gateway = HttpServer::new(move || {
        App::new()
            .app_data(captured.clone())
            .route("/v1/chat/completions", web::post().to(update_provider))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok()
                        .json(json!({"data":[{"id":UPDATE_MODEL},{"id":"update-auxiliary"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let url = format!("http://{}/v1", gateway.addrs()[0]);
    let gateway = gateway.run();
    let gateway_handle = gateway.handle();
    actix_web::rt::spawn(gateway);
    std::fs::write(
        data.join("config.json"),
        serde_json::to_vec(&json!({
            "provider":"openai","features":{"provider_model_ref":true,"ticket_mutation":false,"ticket_dispatch":false},
            "providers":{"openai":{"api_key":"fixture","base_url":url,"model":UPDATE_MODEL,"fast_model":"update-auxiliary"}},
            "defaults":{"chat":{"provider":"openai","model":UPDATE_MODEL},"fast":{"provider":"openai","model":"update-auxiliary"}},
            "memory":{"auto_dream_enabled":false},
            "stream_timeout":{"transport_idle_timeout_secs":120,"first_semantic_timeout_secs":120,"semantic_idle_timeout_secs":120}
        })).unwrap(),
    ).unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(WAIT)
        .build()
        .unwrap();
    let old_control = tempfile::tempdir_in(&root).unwrap();
    let new_control = tempfile::tempdir_in(&root).unwrap();
    let mut old = Host::start_with_control(&data, "managed-old", Some(old_control.path()));
    Fixture::healthy(&client, &mut old).await;
    assert!(old_control.path().join("planned-update-v1").is_file());

    let mut roots = Vec::new();
    for prompt in UPDATE_PROMPTS {
        let model = json!({"provider":"openai","model":UPDATE_MODEL});
        let created: Value = client
            .post(format!("{}/sessions", old.base))
            .json(&json!({
                "title":prompt,"provider":"openai","model":UPDATE_MODEL,"model_ref":model,
                "permission_mode":"default","workspace_path":workspace
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let id = created["session"]["id"].as_str().unwrap();
        client
            .post(format!("{}/chat", old.base))
            .json(&json!({
                "session_id":id,"message":prompt,"provider":"openai","model":UPDATE_MODEL,
                "model_ref":model,"permission_mode":"default","workspace_path":workspace
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        client
            .post(format!("{}/execute/{id}", old.base))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        roots.push(id.to_string());
    }
    tokio::time::timeout(WAIT, async {
        while probe
            .calls
            .iter()
            .any(|calls| calls.load(Ordering::SeqCst) == 0)
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("both ordinary Roots entered the live provider");

    let mut sequence = 0;
    let abandoned = uuid::Uuid::new_v4();
    let first = send_update_command(
        old_control.path(),
        &old,
        abandoned,
        &mut sequence,
        "prepare",
    )
    .await;
    assert_eq!(
        first["status"], "busy",
        "live ordinary Roots still own work: {first}"
    );
    let aborted =
        send_update_command(old_control.path(), &old, abandoned, &mut sequence, "abort").await;
    assert_eq!(aborted["status"], "aborted");
    let stale =
        send_update_command(old_control.path(), &old, abandoned, &mut sequence, "commit").await;
    assert_eq!(stale["status"], "revoked");
    old.alive();

    let request = uuid::Uuid::new_v4();
    let preparing =
        send_update_command(old_control.path(), &old, request, &mut sequence, "prepare").await;
    assert_eq!(
        preparing["status"], "busy",
        "new request can replace the aborted preparation: {preparing}"
    );
    probe.release.send_replace(true);
    tokio::time::timeout(WAIT, async {
        loop {
            let reply =
                send_update_command(old_control.path(), &old, request, &mut sequence, "prepare")
                    .await;
            match reply["status"].as_str() {
                Some("ready") => break,
                Some("busy") => (),
                _ => panic!("managed preparation failed: {reply}: {}", old.tail()),
            }
        }
    })
    .await
    .expect("both old Roots finish Read and park before the next provider call");
    assert!(probe
        .calls
        .iter()
        .all(|calls| calls.load(Ordering::SeqCst) == 1));

    let receipts = data.join("managed-update");
    let ready = receipts.join(format!("{request}.ready"));
    assert!(!ready.exists());
    tokio::time::timeout(WAIT, async {
        loop {
            let reply =
                send_update_command(old_control.path(), &old, request, &mut sequence, "commit")
                    .await;
            match reply["status"].as_str() {
                Some("committed") => break,
                Some("committing") => (),
                _ => panic!("managed commit failed: {reply}: {}", old.tail()),
            }
        }
    })
    .await
    .expect("the predecessor authorizes replacement after finalization");
    expect_graceful_exit(&mut old).await;
    let intent = std::fs::read(receipts.join(format!("{request}.json"))).unwrap();
    assert_eq!(
        std::fs::read_to_string(ready).unwrap(),
        bamboo_domain::session::model_context::sha256_hex(&intent)
    );
    assert!(!receipts.join(format!("{abandoned}.ready")).exists());
    let intent: Value = serde_json::from_slice(&intent).unwrap();
    assert_eq!(intent["request"], request.to_string());
    assert_eq!(intent["roots"].as_array().unwrap().len(), 2);
    let store = Arc::new(SessionStoreV2::new(data.clone()).await.unwrap());
    let mut predecessors = Vec::new();
    for id in &roots {
        let session = store.load_session(id).await.unwrap().unwrap();
        assert_eq!(session.last_run_status().as_deref(), Some("suspended"));
        assert!(session.is_parked_for_planned_update());
        let owner = store.inspect_actor(id).await.unwrap().activation.unwrap();
        assert_eq!(owner.status, ActorActivationStatus::Succeeded);
        predecessors.push((session, owner.fence()));
    }
    // Seed a genuine wake-eligible Inbox entry between the two processes.
    // Its durable delivery grants wake authority, with no transcript change.
    let inbox = FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    let queued = inbox
        .deliver_with_activation_intent(
            &SessionMessageEnvelope::user_input(&roots[0], UPDATE_QUEUED_INPUT),
            SessionActivationPolicy::InterruptSpecificWait,
            None,
        )
        .await
        .unwrap();
    let readiness = inbox
        .inspect_wake_readiness(&roots[0], Utc::now())
        .await
        .unwrap();
    assert_eq!(readiness.ready.unwrap().generation, queued.generation);

    let mut new = Host::start_with_control(&data, "managed-new", Some(new_control.path()));
    Fixture::healthy(&client, &mut new).await;
    assert_ne!(old.child.id(), new.child.id());
    // Startup has run its inline Inbox reconciliation by the time health is
    // served. A direct execute also cannot claim the parked Root or its input.
    let refused = client
        .post(format!("{}/execute/{}", new.base, roots[0]))
        .header("Bamboo-Input-Id", queued.id.as_str())
        .header("Bamboo-Ingress-Seq", queued.generation)
        .header(
            "Bamboo-Session-Created-At",
            predecessors[0].0.created_at.to_rfc3339(),
        )
        .header("Idempotency-Key", "planned-update-before-resume")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        refused.json::<Value>().await.unwrap()["error"]["code"],
        "planned_update_resume_pending"
    );
    for (id, (snapshot, fence)) in roots.iter().zip(&predecessors) {
        let parked = store.load_session(id).await.unwrap().unwrap();
        assert!(parked.is_parked_for_planned_update());
        assert_eq!(parked.last_run_status().as_deref(), Some("suspended"));
        assert_eq!(
            serde_json::to_value(&parked.messages).unwrap(),
            serde_json::to_value(&snapshot.messages).unwrap()
        );
        let owner = store.inspect_actor(id).await.unwrap().activation.unwrap();
        assert_eq!(&owner.fence(), fence);
        assert_eq!(owner.status, ActorActivationStatus::Succeeded);
    }
    // The physical admission guard also rejects an ordinary caller whose
    // observed candidate accidentally carries a different runtime view.
    let (snapshot, _) = &predecessors[1];
    let now = Utc::now();
    assert!(matches!(
        store
            .claim_root_activation_observed(
                &bamboo_domain::ActorActivationClaim {
                    actor_id: snapshot.id.clone(),
                    run_id: "unexpected-before-resume".into(),
                    lease_owner: "fixture-ordinary-recovery".into(),
                    lease_expires_at: now + chrono::Duration::seconds(15),
                    inbox_generation: 0,
                    placement_ref: None,
                    now,
                },
                snapshot,
            )
            .await,
        Err(ActorDirectoryError::Busy)
    ));
    let ready_after = inbox
        .inspect_wake_readiness(&roots[0], Utc::now())
        .await
        .unwrap();
    assert_eq!(ready_after.ready.unwrap().id, queued.id);
    assert!(probe
        .calls
        .iter()
        .all(|calls| calls.load(Ordering::SeqCst) == 1));
    let mut sequence = 0;
    let reply =
        send_update_command(new_control.path(), &new, request, &mut sequence, "resume").await;
    assert_eq!(reply["status"], "resumed", "successor recovery: {reply}");
    assert_eq!(reply["report"]["resumed"], 2, "{reply}");
    assert_eq!(reply["report"]["pending_sessions"], json!([]));
    for (id, (_, predecessor)) in roots.iter().zip(predecessors) {
        tokio::time::timeout(WAIT, async {
            loop {
                let session = store.load_session(id).await.unwrap().unwrap();
                if session.last_run_status().as_deref() == Some("completed") {
                    assert_eq!(
                        session.messages.last().unwrap().content,
                        "Resumed after the planned update."
                    );
                    let owner = store.inspect_actor(id).await.unwrap().activation.unwrap();
                    assert_eq!(owner.lease_epoch, predecessor.lease_epoch + 1);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("the successor finishes the same Root");
    }
    let repeated =
        send_update_command(new_control.path(), &new, request, &mut sequence, "resume").await;
    assert_eq!(
        repeated["report"],
        json!({"resumed":0,"advanced":2,"pending_sessions":[]})
    );
    assert!(probe
        .calls
        .iter()
        .all(|calls| calls.load(Ordering::SeqCst) == 2));
    std::fs::File::create_new(new_control.path().join("stop")).unwrap();
    expect_graceful_exit(&mut new).await;
    gateway_handle.stop(true).await;
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
    root: String,
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
        // A bare Root has no eligible input. Check real independent Host
        // startup before Native chat publishes its durable Inbox intent.
        let response = client
            .post(format!("{}/sessions", a.base))
            .json(&json!({
                "title":"ordinary-root-actor-writer","model":"writer-root",
                "provider":"openai","model_ref":{"provider":"openai","model":"writer-root"},
                "permission_mode":"default","workspace_path":workspace
            }))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let created: Value = response.json().await.unwrap();
        assert!(status.is_success(), "create bare Root: {status}: {created}");
        let root = created["session"]["id"].as_str().unwrap().to_string();
        let mut b = Host::start(&data, "b");
        Self::healthy(&client, &mut b).await;
        let store = SessionStoreV2::new(data.clone()).await.unwrap();
        assert_eq!(
            probe.calls.load(Ordering::SeqCst),
            0,
            "Host startup must not execute a newly created Root without eligible input"
        );
        assert!(
            probe
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|request| request["model"] != "writer-root"),
            "bare Root startup must not even attempt a foreground provider request"
        );
        let empty = store.load_session(&root).await.unwrap().unwrap();
        assert_eq!(empty.kind, bamboo_domain::SessionKind::Root);
        assert!(!empty.root_orchestration_only);
        assert_eq!(
            empty
                .messages
                .iter()
                .filter(|message| message.role == Role::User)
                .count(),
            0
        );
        let authority = data
            .join("sessions")
            .join(&root)
            .join("actor-authority.json");
        if authority.exists() {
            let entry: ActorDirectoryEntry =
                serde_json::from_slice(&std::fs::read(authority).unwrap()).unwrap();
            entry.validate().unwrap();
            assert!(
                entry.activation.is_none(),
                "bare Root startup cannot create an Actor activation: {entry:?}"
            );
        }
        let response = client
            .post(format!("{}/chat", a.base))
            .json(&json!({
                "session_id":root,"message":INPUT,"model":"writer-root","provider":"openai",
                "model_ref":{"provider":"openai","model":"writer-root"},
                "permission_mode":"default","workspace_path":workspace
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
            "Native /chat itself must not start a foreground execution"
        );
        // Both Hosts are already healthy. A takes the actual execution owner
        // below; Native's intent does not add a client-execute-only barrier.
        let fixture = Self {
            root,
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

    fn pause_boundary_locks(&self) -> Vec<(PathBuf, bool)> {
        let mut locks = vec![
            (self.data.join(".session-lifecycle.lock"), true),
            (self.data.join(".runtime-task-transactions.lock"), true),
            (self.data.join(".sessions-index.lock"), false),
            (self.data.join("events/.account-journal.lock"), false),
            (
                self.data
                    .join("sessions")
                    .join(&self.root)
                    .join("inbox/.session-inbox.lock"),
                false,
            ),
        ];
        for name in [".session-write-locks", ".actor-tree-locks"] {
            let entries = match std::fs::read_dir(self.data.join(name)) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => panic!("fixture lock inventory: {error}"),
            };
            for entry in entries {
                let entry = entry.unwrap();
                if entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "lock")
                {
                    assert!(entry.file_type().unwrap().is_file());
                    locks.push((entry.path(), false));
                }
            }
        }
        locks.sort();
        assert!(
            locks.len() <= 32,
            "bounded disposable fixture lock inventory"
        );
        locks
    }

    async fn execute(&self, host: &Host) -> Value {
        let response = self
            .client
            .post(format!("{}/execute/{}", host.base, self.root))
            .json(&json!({}))
            .send()
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "fixture execute request failed: {error:?}; target {}; A {}; B {}; blocked locks {:?}; authority {:?}; requests {:?}",
                    host.tail(),
                    self.a.tail(),
                    self.b.tail(),
                    blocked_file_locks(&self.pause_boundary_locks()),
                    self.authority(),
                    self.probe.requests.lock().unwrap()
                )
            });
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
        self.store.load_session(&self.root).await.unwrap().unwrap()
    }

    fn account_terminal_history(&self) -> Vec<Value> {
        bamboo_engine::events::journal::read_since(&self.data.join("events"), 0)
            .unwrap()
            .into_iter()
            .filter(|frame| {
                frame.session_id.as_deref() == Some(self.root.as_str())
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
            .get(format!("{}/history/{}", host.base, self.root))
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
            .join(&self.root)
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
        assert_eq!(entry.actor.actor_id, self.root);
        // Checked admission has checkpointed and ACKed the real User before
        // the foreground provider can reach its controlled stream barrier.
        let root = self.canonical().await;
        assert_eq!(
            root.messages
                .iter()
                .filter(|message| message.role == Role::User && message.content == INPUT)
                .count(),
            1
        );
        assert_eq!(entry.actor.session_created_at, root.created_at);
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
async fn pause_boundary_rejects_a_real_conflicting_file_lock() {
    let temp = tempfile::tempdir().unwrap();
    let lock_path = temp.path().join("fixture-writer.lock");
    let mut held = Some(std::fs::File::create(&lock_path).unwrap());
    held.as_ref().unwrap().lock().unwrap();
    let mut host = Host {
        child: Command::new("sleep").arg("30").spawn().unwrap(),
        base: String::new(),
        log: temp.path().join("unused-host.log"),
        stopped: false,
    };
    let rejected = host
        .stop_clear_of_file_locks(&[(lock_path.clone(), false)], |blocked| {
            assert_eq!(blocked, std::slice::from_ref(&lock_path));
            drop(held.take());
        })
        .await
        .unwrap();
    assert!(
        rejected > 0 && held.is_none(),
        "the pause must reject a conflicting physical writer before accepting STOP"
    );
    let replacement = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    replacement.try_lock().unwrap();
    assert!(host.stopped);
}

#[actix_web::test]
async fn surviving_root_writer_rejects_old_output_after_real_host_lease_reclaim() {
    let mut fixture = Fixture::new().await;
    let a = fixture.start_a().await;
    let events = EventTap::open(
        &fixture.client,
        &fixture.a,
        &format!("/events/{}", fixture.root),
    )
    .await;
    // The account feed stays open after the token stream's terminal event.
    let account = EventTap::open(&fixture.client, &fixture.a, "/stream").await;
    let locks = fixture.pause_boundary_locks();
    let rejected = fixture
        .a
        .stop_clear_of_file_locks(&locks, |_| {})
        .await
        .unwrap_or_else(|reason| {
            panic!(
                "{reason}; A {}; B {}; authority {:?}",
                fixture.a.tail(),
                fixture.b.tail(),
                fixture.authority()
            )
        });
    if rejected > 0 {
        eprintln!("fixture rejected {rejected} pauses inside physical writer boundaries");
    }
    // Actual wall time and the persisted lease decide expiry. No future `now`,
    // shortened sidecar, A finish, or external A claim substitutes for it. The
    // window follows the claim A actually made, so a lease-length change widens
    // this proof instead of truncating it.
    let claimed = fixture.authority().activation.unwrap();
    assert_eq!(claimed.fence(), a.activation.as_ref().unwrap().fence());
    let expiry_window = (claimed.lease_expires_at - Utc::now())
        .to_std()
        .unwrap_or_default()
        + WAIT;
    tokio::time::timeout(expiry_window, async {
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
    // Queue the held answer and DONE before A can resume and close its stream.
    // The flag proves mock SSE enqueue, not that A consumed or published text.
    fixture.probe.release_a.send_replace(true);
    tokio::time::timeout(WAIT, async {
        loop {
            fixture.a.alive();
            if fixture.probe.a_response_sent.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "mock must enqueue the old answer and DONE while A is paused; A={}; B={}",
            fixture.a.tail(),
            fixture.b.tail()
        )
    });
    fixture.a.signal("-CONT");
    tokio::time::timeout(WAIT, async {
        loop {
            fixture.a.alive();
            let log = std::fs::read_to_string(&fixture.a.log).unwrap_or_default();
            let (observed_output, authority_interrupted) = {
                let observed = events.events.lock().unwrap();
                (
                    observed.iter().any(|event| {
                        event["type"] == "token"
                            && event["content"].as_str().is_some_and(|text| text.contains(OLD_REPLY))
                    }),
                    observed.iter().any(|event| {
                        event["type"] == "error"
                            && event["message"].as_str().is_some_and(|text| text.contains("actor_authority_conflict"))
                    }),
                )
            };
            let rejected_save = log.lines().any(|line| {
                let line = line.to_lowercase();
                line.contains("failed to save session") && (line.contains("fence") || line.contains("actor"))
                    && (line.contains("stale") || line.contains("expired"))
            });
            // A guarded runtime event may fence and cancel A before the queued
            // answer becomes a live token. Both paths must reject its final save.
            if (observed_output || authority_interrupted) && rejected_save {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.unwrap_or_else(|_| panic!("A must reach old output or explicit authority interruption and reject its final authority save; events={:?}; A={}; B={}",events.events.lock().unwrap(),fixture.a.tail(),fixture.b.tail()));
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
                .any(|entry| entry["session_id"] == fixture.root)
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
