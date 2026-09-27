//! Actual CLI broker + compiled BambooRuntime worker/provider over trusted WSS.
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_broker::{client_config_trusting_cert, BrokerClient, BrokerStreamEvent};
use bamboo_storage::SessionStoreV2;
use bamboo_subagent::{
    proto::{ChildCreationIdentity, LogicalSessionIdentity, RunSpec, TerminalStatus},
    provision::{ChildIdentity, ExecutorSpec, ModelRefSpec, ScopedCredential},
    ActorEventBatch, AgentRef, BusEndpoint, ChildOutcome, InboxKind, InboxMessage, MsgId,
    ProvisionSpec,
};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::{json, Value};
use std::{
    io::Write,
    path::Path,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
const HOST: &str = "native-host-opaque-credential-000000001";
const WORKER: &str = "native-worker-opaque-credential-0000001";
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}
fn command(dir: &Path) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_bamboo"));
    c.env_remove("RUST_MIN_STACK")
        .env("BAMBOO_DATA_DIR", dir)
        .env("BAMBOO_JIANDU_DATA_DIR", dir.join("jiandu"));
    c
}
fn broker(dir: &Path, root: &Path, bind: &str) -> Command {
    let mut c = command(dir);
    c.args(["broker", "serve", "--bind", bind, "--root"])
        .arg(root);
    c
}
fn spawn(mut command: Command, input: Option<Vec<u8>>, log: &Path) -> Process {
    command
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::from(std::fs::File::create(log).unwrap()))
        .stderr(Stdio::from(
            std::fs::File::create(log.with_extension("stderr")).unwrap(),
        ));
    let mut child = command.spawn().unwrap();
    if let Some(bytes) = input {
        child.stdin.take().unwrap().write_all(&bytes).unwrap();
    }
    Process(child)
}
fn addr() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().to_string()
}
fn agent(id: &str, role: &str) -> AgentRef {
    AgentRef {
        session_id: id.into(),
        role: Some(role.into()),
    }
}
async fn connect(
    url: &str,
    cert: Option<&Path>,
    id: &str,
    role: &str,
    token: &str,
) -> BrokerClient {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(c) = BrokerClient::connect_with_tls(
                url,
                agent(id, role),
                token,
                cert.map(|p| client_config_trusting_cert(p).unwrap()),
            )
            .await
            {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real broker startup/credential admission")
}
async fn response(body: web::Json<Value>, calls: web::Data<AtomicUsize>) -> HttpResponse {
    assert_eq!(body["model"], "auth-native");
    assert!(body["messages"].to_string().contains("AUTH_NATIVE_TASK"));
    calls.fetch_add(1, Ordering::SeqCst);
    HttpResponse::Ok().content_type("text/event-stream").body(format!("data: {}\n\ndata: [DONE]\n\n",json!({"id":"auth-native","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"AUTH_NATIVE_REPLY"},"finish_reason":"stop"}]})))
}
#[test]
fn strict_cli_and_real_bamboo_runtime_worker_execute_over_wss() {
    // Host fixture stack only; real broker/worker subprocesses use native defaults.
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| actix_web::rt::System::new().block_on(fixture()))
        .unwrap()
        .join()
        .unwrap();
}
async fn fixture() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.keep().canonicalize().unwrap();
    eprintln!("scoped broker native evidence: {}", dir.display());
    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");
    assert!(Command::new("openssl")
        .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1"])
        .args(["-subj", "/CN=127.0.0.1"])
        .args(["-addext", "subjectAltName=IP:127.0.0.1"])
        .args(["-addext", "basicConstraints=critical,CA:FALSE"])
        .arg("-keyout")
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .output()
        .expect("required openssl unavailable")
        .status
        .success());
    let expires = Utc::now() + ChronoDuration::minutes(5);
    let policy = json!({"peers":[{"credential":HOST,"host":"native-host","mailbox":"host-native","role":"host","expires_at":expires,"destinations":[{"mailbox":"worker-native","kinds":["run"]}],"presence":["worker"]},
        {"credential":WORKER,"host":"native-worker-host","mailbox":"worker-native","role":"worker","expires_at":expires,"destinations":[{"mailbox":"host-native","kinds":["event","outcome","session_message_admitted"]}]}]});
    // Actual CLI early rejection before listener/core/GC, with static output.
    for (bind, args, input) in [
        ("0.0.0.0:0", vec![], policy.to_string().into_bytes()),
        (
            "127.0.0.1:0",
            vec!["--token", "legacy-flag"],
            policy.to_string().into_bytes(),
        ),
        (
            "127.0.0.1:0",
            vec![],
            b"{\"peers\":[],\"peers\":[]}".to_vec(),
        ),
        ("127.0.0.1:0", vec![], vec![b' '; 65537]),
    ] {
        let root = dir.join("rejected");
        let mut c = broker(&dir, &root, bind);
        c.arg("--peer-policy-stdin")
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = c.spawn().unwrap();
        child.stdin.take().unwrap().write_all(&input).ok();
        let out = child.wait_with_output().unwrap();
        assert!(!out.status.success());
        let output = String::from_utf8_lossy(&out.stderr);
        assert!(!output.contains(HOST) && !output.contains(WORKER));
        assert!(!root.exists());
    }
    let base = dir.join("broker");
    let bind = addr();
    let url = format!("wss://{bind}");
    let mut c = broker(&dir, &base, &bind);
    c.arg("--peer-policy-stdin")
        .arg("--cert")
        .arg(&cert)
        .arg("--key")
        .arg(&key);
    // Ambient legacy env is ignored, never a strict-mode fallback.
    c.env("BAMBOO_BROKER_TOKEN", "legacy-global-control");
    let _broker = spawn(
        c,
        Some(policy.to_string().into_bytes()),
        &dir.join("broker.log"),
    );
    let mut parent = connect(&url, Some(&cert), "host-native", "host", HOST).await;
    parent.subscribe().await.unwrap();
    for (id, role, token) in [
        ("worker-native", "worker", HOST),
        ("host-native", "host", "legacy-global-control"),
    ] {
        assert!(BrokerClient::connect_with_tls(
            &url,
            agent(id, role),
            token,
            Some(client_config_trusting_cert(&cert).unwrap())
        )
        .await
        .is_err());
    }
    let calls = web::Data::new(AtomicUsize::new(0));
    let data = calls.clone();
    let http = HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .route("/v1/chat/completions", web::post().to(response))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok().json(json!({"data":[{"id":"auth-native"}]}))
                }),
            )
    })
    .bind(("127.0.0.1", 0))
    .unwrap();
    let provider_url = format!("http://{}/v1", http.addrs()[0]);
    let running = http.run();
    let handle = running.handle();
    actix_web::rt::spawn(running);
    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: "worker-native".into(),
            parent_id: Some("native-root".into()),
            project_key: None,
            role: "worker".into(),
            depth: 1,
        },
        ExecutorSpec::BambooRuntime,
        dir.join("fabric").to_string_lossy().into_owned(),
    );
    let worker_cache = dir.join("worker-cache");
    spec.storage_dir = Some(worker_cache.to_string_lossy().into_owned());
    spec.capabilities.child_creation_identity = true;
    spec.capabilities.read_only = true;
    spec.capabilities.guardian_read_only = true;
    spec.capabilities.enforce_permissions = true;
    spec.disabled_tools = Some(
        bamboo_engine::runtime::guardian_state::read_only_child_disabled_tools()
            .into_iter()
            .collect(),
    );
    spec.workspace = Some(dir.to_string_lossy().into_owned());
    spec.model = Some(ModelRefSpec {
        provider: "openai".into(),
        model: "auth-native".into(),
    });
    spec.secrets.provider_credentials.push(ScopedCredential {
        provider: "openai".into(),
        api_key: "fixture-key".into(),
        base_url: Some(provider_url),
        provider_type: None,
        credential_ref: None,
    });
    spec.bus = Some(BusEndpoint {
        endpoint: url.clone(),
        token: WORKER.into(),
    });
    let mut c = command(&dir);
    c.args(["broker-agent", "serve", "--broker"])
        .arg(&url)
        .args(["--id", "worker-native", "--spec-stdin", "--tls-ca-cert"])
        .arg(&cert)
        .env("BAMBOO_BROKER_TOKEN", WORKER);
    let mut _worker = spawn(
        c,
        Some(spec.to_json().unwrap().into_bytes()),
        &dir.join("worker.log"),
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if parent.list_connected("worker").await.unwrap() == vec!["worker-native".to_owned()] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let identity = LogicalSessionIdentity {
        session_id: "native-child".into(),
        parent_session_id: Some("native-root".into()),
        root_session_id: "native-root".into(),
        creation: Some(ChildCreationIdentity {
            created_at: Utc::now(),
            spawn_depth: 1,
        }),
    };
    let run = RunSpec {
        assignment: "AUTH_NATIVE_TASK".into(),
        logical_session: Some(identity.clone()),
        project_id: None,
        reasoning_effort: None,
        permission_policy: None,
        messages: vec![],
        activation_run_id: Some("auth-native-run".into()),
        execution_epoch: 7,
        initial_session_messages: vec![],
        secrets: Default::default(),
    };
    let mut restricted = connect(&url, Some(&cert), "worker-native", "worker", WORKER).await;
    assert!(restricted
        .deliver(
            "worker-native",
            InboxMessage {
                id: MsgId::new(),
                from: agent("worker-native", "worker"),
                kind: InboxKind::Run,
                body: serde_json::to_value(&run).unwrap(),
                created_at: Utc::now(),
                correlation_id: None
            }
        )
        .await
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let id = MsgId::new();
    parent
        .deliver(
            "worker-native",
            InboxMessage {
                id: id.clone(),
                from: agent("host-native", "host"),
                kind: InboxKind::Run,
                body: serde_json::to_value(run).unwrap(),
                created_at: Utc::now(),
                correlation_id: None,
            },
        )
        .await
        .unwrap();
    let mut events = 0;
    let outcome = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match parent.next_message_or_event_batch().await {
                BrokerStreamEvent::Message(Some(m)) => {
                    assert_eq!(m.from, agent("worker-native", "worker"));
                    assert_eq!(m.correlation_id, Some(id.clone()));
                    let kind = m.kind;
                    parent.ack(m.id).await.unwrap();
                    if kind == InboxKind::Outcome {
                        break serde_json::from_value::<ChildOutcome>(m.body).unwrap();
                    }
                    assert_eq!(kind, InboxKind::Event);
                    let b = serde_json::from_value::<ActorEventBatch>(m.body).unwrap();
                    b.validate().unwrap();
                    assert_eq!(b.source_actor_id.as_deref(), Some("worker-native"));
                    assert_eq!(b.logical_session, Some(identity.clone()));
                    events += 1;
                }
                BrokerStreamEvent::EventBatch(Some(e)) => {
                    assert_eq!(e.correlation_id, id);
                    e.batch.validate().unwrap();
                    assert_eq!(e.batch.source_actor_id.as_deref(), Some("worker-native"));
                    events += 1;
                }
                _ => panic!("real worker closed before outcome"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        outcome.status,
        TerminalStatus::Completed,
        "native worker terminal error: {}",
        outcome
            .error
            .as_deref()
            .unwrap_or("")
            .chars()
            .take(384)
            .collect::<String>()
    );
    assert!(outcome
        .result
        .as_deref()
        .unwrap()
        .contains("AUTH_NATIVE_REPLY"));
    assert!(events > 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    _worker.0.kill().unwrap();
    _worker.0.wait().unwrap();
    drop(_worker);
    let cold = SessionStoreV2::new(worker_cache.clone()).await.unwrap();
    let cached = cold.load_session("native-child").await.unwrap().unwrap();
    assert_eq!(cached.id, identity.session_id);
    assert_eq!(
        cached.created_at,
        identity.creation.as_ref().unwrap().created_at
    );
    assert_eq!(
        cached.spawn_depth,
        identity.creation.as_ref().unwrap().spawn_depth
    );
    assert_eq!(cached.parent_session_id, identity.parent_session_id);
    assert_eq!(cached.root_session_id, identity.root_session_id);
    assert!(cached.project_id_meta().is_none());
    assert!(worker_cache.is_dir());
    assert!(base
        .join("scoped-peers-v1/mailboxes/worker-native")
        .exists());
    assert!(!base.join("mailboxes").exists());
    // Actual legacy CLI on the SAME operator base, separately rooted Ask control.
    let legacy_bind = addr();
    let legacy_url = format!("ws://{legacy_bind}");
    let mut c = broker(&dir, &base, &legacy_bind);
    c.env("BAMBOO_BROKER_TOKEN", "legacy-global-control");
    let _legacy = spawn(c, None, &dir.join("legacy.log"));
    let mut legacy_parent = connect(
        &legacy_url,
        None,
        "host-native",
        "host",
        "legacy-global-control",
    )
    .await;
    let mut legacy_child = connect(
        &legacy_url,
        None,
        "worker-native",
        "worker",
        "legacy-global-control",
    )
    .await;
    legacy_child.subscribe().await.unwrap();
    let ask = InboxMessage {
        id: MsgId::new(),
        from: agent("host-native", "host"),
        kind: InboxKind::Ask,
        body: json!({"question":"legacy control"}),
        created_at: Utc::now(),
        correlation_id: None,
    };
    legacy_parent
        .deliver("worker-native", ask.clone())
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(3), legacy_child.next_message())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, ask);
    legacy_child.ack(got.id).await.unwrap();
    assert!(base.join("mailboxes/worker-native").exists());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    for name in [
        "broker.stderr",
        "worker.stderr",
        "legacy.stderr",
        "broker.log",
        "worker.log",
        "legacy.log",
    ] {
        let text = std::fs::read_to_string(dir.join(name)).unwrap();
        assert!(!text.contains(HOST) && !text.contains(WORKER));
    }
    handle.stop(true).await;
}
