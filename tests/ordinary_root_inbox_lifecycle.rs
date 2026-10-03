//! Ordinary local Root input lifecycle through the real Host and Child processes.
//! Only the OpenAI-compatible provider is controlled; canonical V2 storage is read-only evidence.
#![cfg(unix)]

use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::{Role, Session, SessionInboxPort, SessionMessageEnvelope, SessionMessageId};
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
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

const ROOT_ID: &str = "ordinary-inbox-root";
const INPUT_ID: &str = "ordinary-durable-user-input";
const INPUT: &str =
    "ROOT_NEW_DURABLE_INPUT: acknowledge this input while the Child is still working.";
const INPUT_REPLY: &str = "ROOT_HANDLED_DURABLE_INPUT";
const CHILD_RESULT: &str = "ORIGINAL_CHILD_RESULT";
const COLLECTED: &str = "ROOT_COLLECTED_ORIGINAL_CHILD_RESULT";
const STALE_REPLY: &str = "CANCELLED_OWNER_MUST_NOT_COMMIT";
const SUCCESSOR_ID: &str = "ordinary-successor-input";
const SUCCESSOR: &str = "ROOT_SUCCESSOR_INPUT: replace the cancelled execution.";
const SUCCESSOR_REPLY: &str = "ROOT_SUCCESSOR_COMMITTED";
const WRITE_CALL: &str = "root-write-after-permission-tightening";
const RESTART_WAIT: &str = "ROOT_RESTART_WAIT: hold this provider boundary without delegating.";

#[derive(Clone, Copy)]
enum Case {
    Input,
    Cancel,
    Permission,
    PendingRestart,
}

struct Probe {
    case: Case,
    workspace: PathBuf,
    delegated: AtomicBool,
    child_calls: AtomicUsize,
    input_calls: AtomicUsize,
    child_ready: AtomicBool,
    root_ready: AtomicBool,
    late_response_emitted: AtomicBool,
    requests: Mutex<Vec<Value>>,
    release_child: tokio::sync::watch::Sender<bool>,
    release_root: tokio::sync::watch::Sender<bool>,
}

async fn released(sender: &tokio::sync::watch::Sender<bool>) {
    let mut receiver = sender.subscribe();
    tokio::time::timeout(Duration::from_secs(60), async {
        while !*receiver.borrow_and_update() {
            receiver.changed().await.unwrap();
        }
    })
    .await
    .expect("fixture releases the held provider boundary");
}

fn has_user(body: &Value, marker: &str) -> bool {
    body["messages"].as_array().unwrap().iter().any(|message| {
        message["role"] == "user"
            && message["content"]
                .as_str()
                .is_some_and(|text| text.contains(marker))
    })
}

fn diagnostic_prefix(text: &str, cap: usize) -> &str {
    let mut end = text.len().min(cap);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn tool_call(id: &str, name: &str, args: Value) -> (Value, &'static str) {
    (
        json!({"tool_calls":[{"index":0,"id":id,"type":"function",
            "function":{"name":name,"arguments":args.to_string()}}]}),
        "tool_calls",
    )
}

async fn provider(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let body = body.into_inner();
    let tools: Vec<_> = body["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["function"]["name"].as_str())
        .take(32)
        .collect();
    let diagnostic = json!({
        "model":body["model"],"tools":tools,
        "has_input":has_user(&body,INPUT),"has_successor":has_user(&body,SUCCESSOR),
        "matched_previous_root_classifier":body["model"] == "inbox-root" && tools.contains(&"SubAgent")
    });
    {
        let mut requests = probe.requests.lock().unwrap();
        if requests.len() < 24 {
            eprintln!("ordinary fixture provider request: {diagnostic}");
            requests.push(diagnostic);
        }
    }
    let (delta, finish) = if body["model"] == "inbox-child" {
        assert_eq!(
            probe.child_calls.fetch_add(1, Ordering::SeqCst),
            0,
            "one actual Child provider admission for the original assignment"
        );
        assert!(has_user(&body, CHILD_RESULT));
        probe.child_ready.store(true, Ordering::SeqCst);
        released(&probe.release_child).await;
        (json!({"content":CHILD_RESULT}), "stop")
    } else if body["model"] == "inbox-root" {
        // Foreground identity is the configured model. The runtime may project
        // the tool catalog after cancellation; that does not turn a Root
        // request into a background title or summary request.
        if matches!(probe.case, Case::PendingRestart) && !has_user(&body, INPUT) {
            probe.root_ready.store(true, Ordering::SeqCst);
            released(&probe.release_root).await;
            probe.late_response_emitted.store(true, Ordering::SeqCst);
            (json!({"content":STALE_REPLY}), "stop")
        } else if matches!(probe.case, Case::PendingRestart) {
            probe.input_calls.fetch_add(1, Ordering::SeqCst);
            (json!({"content":INPUT_REPLY}), "stop")
        } else if !probe.delegated.swap(true, Ordering::SeqCst) {
            tool_call(
                "root-original-child",
                "SubAgent",
                json!({"role":"worker","message":format!("Return only {CHILD_RESULT}; use no tools.")}),
            )
        } else if has_user(&body, SUCCESSOR) {
            (json!({"content":SUCCESSOR_REPLY}), "stop")
        } else if body["messages"].as_array().unwrap().iter().any(|message| {
            message["content"]
                .as_str()
                .is_some_and(|text| text.contains(CHILD_RESULT))
                && message["role"] == "user"
                && text_is_child_outcome(message)
        }) {
            (json!({"content":COLLECTED}), "stop")
        } else if has_user(&body, INPUT) {
            let call = probe.input_calls.fetch_add(1, Ordering::SeqCst);
            probe.root_ready.store(true, Ordering::SeqCst);
            match probe.case {
                Case::Input => (json!({"content":INPUT_REPLY}), "stop"),
                Case::Cancel if call == 0 => {
                    released(&probe.release_root).await;
                    probe.late_response_emitted.store(true, Ordering::SeqCst);
                    (json!({"content":STALE_REPLY}), "stop")
                }
                Case::Permission if call == 0 => {
                    released(&probe.release_root).await;
                    tool_call(
                        WRITE_CALL,
                        "Write",
                        json!({"file_path":probe.workspace.join("permission-boundary.txt"),"content":"must require approval"}),
                    )
                }
                _ => (json!({"content":INPUT_REPLY}), "stop"),
            }
        } else {
            (json!({"content":"ROOT_WAITING_FOR_ORIGINAL_CHILD"}), "stop")
        }
    } else {
        (json!({"content":"auxiliary"}), "stop")
    };
    let event = json!({"id":"ordinary-inbox-fixture","object":"chat.completion.chunk",
        "choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}

fn text_is_child_outcome(message: &Value) -> bool {
    // Provider metadata may be stripped; the coordinator's rendered result
    // carries the Child's terminal marker and is distinct from its assignment.
    message["content"].as_str().is_some_and(|text| {
        text.contains(CHILD_RESULT)
            && !text.contains("Return only")
            && !text.contains("use no tools")
    })
}

struct Host(Child);

impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_host(data: &Path, port: u16, generation: usize) -> Host {
    let log = std::fs::File::create(data.join(format!("host-{generation}.log"))).unwrap();
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
            .env("BAMBOO_JIANDU_DATA_DIR", data.join("jiandu"))
            .env("RUST_LOG", "warn")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}

struct Fixture {
    _temp: tempfile::TempDir,
    data: PathBuf,
    port: u16,
    base: String,
    client: reqwest::Client,
    host: Option<Host>,
    probe: web::Data<Probe>,
    provider_handle: actix_web::dev::ServerHandle,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.probe.release_child.send_replace(true);
        self.probe.release_root.send_replace(true);
        drop(self.host.take());
    }
}

impl Fixture {
    async fn new(case: Case) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let data = root.join("host");
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
        let project = projects
            .create_with_project_path(
                "ordinary-root-input",
                None,
                workspace.to_string_lossy(),
                vec![],
            )
            .unwrap();
        let agents = projects.paths().project_home(&project.id).join("agents");
        std::fs::create_dir_all(&agents).unwrap();
        std::fs::write(agents.join("worker.md"), "---\nschema_version: 1\nname: worker\ndescription: One held plain result\nmodel_hint: openai:inbox-child\ntools:\n  deny: [Bash, Read, Glob, Edit, Write]\n---\nReturn the exact assignment marker; use no tools.\n").unwrap();
        let (release_child, _) = tokio::sync::watch::channel(false);
        let (release_root, _) = tokio::sync::watch::channel(false);
        let probe = web::Data::new(Probe {
            case,
            workspace: workspace.clone(),
            delegated: AtomicBool::new(false),
            child_calls: AtomicUsize::new(0),
            input_calls: AtomicUsize::new(0),
            child_ready: AtomicBool::new(false),
            root_ready: AtomicBool::new(false),
            late_response_emitted: AtomicBool::new(false),
            requests: Mutex::new(Vec::new()),
            release_child,
            release_root,
        });
        let provider_probe = probe.clone();
        let server = HttpServer::new(move || {
            App::new()
                .app_data(provider_probe.clone())
                .route("/v1/chat/completions", web::post().to(provider))
                .route(
                    "/v1/models",
                    web::get().to(|| async {
                        HttpResponse::Ok()
                            .json(json!({"data":[{"id":"inbox-root"},{"id":"inbox-child"},{"id":"inbox-auxiliary"}]}))
                    }),
                )
        })
        .workers(1)
        .bind(("127.0.0.1", 0))
        .unwrap();
        let provider_url = format!("http://{}/v1", server.addrs()[0]);
        let running = server.run();
        let provider_handle = running.handle();
        actix_web::rt::spawn(running);
        std::fs::write(data.join("config.json"), serde_json::to_vec(&json!({
            "provider":"openai","features":{"provider_model_ref":true},
            "providers":{"openai":{"api_key":"fixture","base_url":provider_url,"model":"inbox-root","fast_model":"inbox-auxiliary"}},
            "defaults":{"chat":{"provider":"openai","model":"inbox-root"},"fast":{"provider":"openai","model":"inbox-auxiliary"}},
            "subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1}
        })).unwrap()).unwrap();
        if matches!(case, Case::Permission) {
            // The first-run document prompts only at High risk; Write is
            // Medium. Explicitly select a threshold at which Default asks,
            // while the initial session Bypass still permits the operation.
            let mut permissions = bamboo_tools::permission::default_permission_document();
            permissions.confirm_threshold = Some(bamboo_tools::permission::RiskLevel::Medium);
            std::fs::write(
                data.join("permissions.json"),
                serde_json::to_vec(&json!({"schema_version":1,"revision":1,"data":permissions}))
                    .unwrap(),
            )
            .unwrap();
        }
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();
        let mut fixture = Self {
            _temp: temp,
            data: data.clone(),
            port,
            base: format!("http://127.0.0.1:{port}/api/v1"),
            client,
            host: Some(start_host(&data, port, 1)),
            probe,
            provider_handle,
        };
        fixture.healthy().await;
        let chat = fixture.client.post(format!("{}/chat", fixture.base)).json(&json!({
            "session_id":ROOT_ID,"message":if matches!(case, Case::PendingRestart) {RESTART_WAIT} else {"Delegate one original Child and wait for its result."},
            "model":"inbox-root","provider":"openai","model_ref":{"provider":"openai","model":"inbox-root"},
            "thinking_mode":"standard","permission_mode":"bypass","workspace_path":workspace,"project_id":project.id
        })).send().await.unwrap();
        assert!(
            chat.status().is_success(),
            "chat: {}",
            chat.text().await.unwrap()
        );
        let execute = fixture
            .client
            .post(format!("{}/execute/{ROOT_ID}", fixture.base))
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert!(
            execute.status().is_success(),
            "execute: {}",
            execute.text().await.unwrap()
        );
        fixture
    }

    async fn healthy(&mut self) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                assert!(
                    self.host.as_mut().unwrap().0.try_wait().unwrap().is_none(),
                    "Host exited"
                );
                if self
                    .client
                    .get(format!("{}/health", self.base))
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
        .expect("real Host becomes healthy");
    }

    async fn canonical(&self) -> Session {
        SessionStoreV2::new(self.data.clone())
            .await
            .unwrap()
            .load_session(ROOT_ID)
            .await
            .unwrap()
            .unwrap()
    }

    async fn wait(&mut self, reason: &str, predicate: impl Fn(&Session) -> bool) -> Session {
        let result = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                assert!(
                    self.host.as_mut().unwrap().0.try_wait().unwrap().is_none(),
                    "Host exited at {reason}"
                );
                let session = self.canonical().await;
                assert_ne!(
                    session.last_run_status().as_deref(),
                    Some("error"),
                    "{reason}: {:?}",
                    session.last_run_error()
                );
                if predicate(&session) {
                    break session;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        match result {
            Ok(session) => session,
            Err(_) => {
                let session = self.canonical().await;
                let store = Arc::new(SessionStoreV2::new(self.data.clone()).await.unwrap());
                let inbox = FileSessionInbox::new(store, Default::default());
                let backlog = match inbox.inspect(ROOT_ID).await {
                    Ok(backlog) => {
                        json!({"pending":backlog.pending,"claimed":backlog.claimed,"generation":backlog.generation})
                    }
                    Err(error) => json!({"inspection_error":error.to_string()}),
                };
                let runtime = session.agent_runtime_state.as_ref().map(|runtime| {
                    json!({
                        "run_id":runtime.run_id,"status":runtime.status,
                        "waiting_for_children":runtime.waiting_for_children,
                        "waiting_for_bash":runtime.waiting_for_bash,"suspension":runtime.suspension
                    })
                });
                let messages: Vec<_> = session.messages.iter().rev().take(12).map(|message| json!({
                    "id":message.id,"role":message.role,"tool_call_id":message.tool_call_id,
                    "tool_success":message.tool_success,"content":diagnostic_prefix(&message.content,512)
                })).collect();
                let logs: Vec<_> = (1..=2)
                    .filter_map(|generation| {
                        let log = std::fs::read_to_string(
                            self.data.join(format!("host-{generation}.log")),
                        )
                        .ok()?;
                        Some(
                            json!({"generation":generation,"tail":log.lines().rev().take(12)
                        .map(|line| diagnostic_prefix(line,768)).collect::<Vec<_>>() }),
                        )
                    })
                    .collect();
                panic!(
                    "{reason}: timed out: {}",
                    json!({
                        "last_status":session.last_run_status(),"last_error":session.last_run_error(),
                        "pending_question":session.pending_question,"runtime":runtime,"inbox":backlog,
                        "admission":session.session_inbox_admission(),"messages_newest_first":messages,
                        "child_calls":self.probe.child_calls.load(Ordering::SeqCst),
                        "input_calls":self.probe.input_calls.load(Ordering::SeqCst),
                        "provider_requests":self.probe.requests.lock().unwrap().clone(),
                        "child_ready":self.probe.child_ready.load(Ordering::SeqCst),
                        "root_ready":self.probe.root_ready.load(Ordering::SeqCst),"host_logs":logs
                    })
                );
            }
        }
    }

    async fn held_child(&mut self) -> String {
        let probe = self.probe.clone();
        let session = self
            .wait("Root waits for one actual Child", move |session| {
                probe.child_ready.load(Ordering::SeqCst)
                    && session.last_run_status().as_deref() == Some("suspended")
                    && session
                        .agent_runtime_state
                        .as_ref()
                        .and_then(|runtime| runtime.waiting_for_children.as_ref())
                        .is_some_and(|wait| wait.child_session_ids.len() == 1)
            })
            .await;
        assert!(
            !session.root_orchestration_only_enabled(),
            "ordinary Root retains its native tool surface"
        );
        session
            .agent_runtime_state
            .unwrap()
            .waiting_for_children
            .unwrap()
            .child_session_ids[0]
            .clone()
    }

    async fn guidance(&self, id: &str, text: &str) {
        self.guidance_mode(id, text, "after_round").await;
    }

    async fn guidance_mode(&self, id: &str, text: &str, mode: &str) {
        let response = self
            .client
            .post(format!("{}/sessions/{ROOT_ID}/guidance", self.base))
            .json(&json!({"id":id,"text":text,"mode":mode}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::ACCEPTED,
            "guidance: {}",
            response.text().await.unwrap()
        );
    }

    async fn admitted_once(&self, id: &str, text: &str) {
        let store = Arc::new(SessionStoreV2::new(self.data.clone()).await.unwrap());
        let session = store.load_session(ROOT_ID).await.unwrap().unwrap();
        let matches: Vec<_> = session
            .messages
            .iter()
            .filter(|message| message.id == id)
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "stable input id has one canonical transcript entry"
        );
        assert_eq!(matches[0].role, Role::User);
        assert_eq!(matches[0].content, text);
        let envelope: SessionMessageEnvelope = serde_json::from_value(
            matches[0].metadata.as_ref().unwrap()["session_message"].clone(),
        )
        .unwrap();
        assert_eq!(envelope.id.as_str(), id);
        let id = SessionMessageId::parse(id).unwrap();
        assert!(session.session_inbox_admission().unwrap().contains(&id));
        let inbox = FileSessionInbox::new(store, Default::default());
        assert!(
            inbox.was_admitted(ROOT_ID, &id).await.unwrap(),
            "durable admission receipt agrees with transcript"
        );
        let backlog = inbox.inspect(ROOT_ID).await.unwrap();
        assert_eq!((backlog.pending, backlog.claimed), (0, 0));
    }

    async fn stop(&self) {
        let response = self
            .client
            .post(format!("{}/stop/{ROOT_ID}", self.base))
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "stop: {}",
            response.text().await.unwrap()
        );
    }

    async fn restart(&mut self) {
        drop(self.host.take());
        self.host = Some(start_host(&self.data, self.port, 2));
        self.healthy().await;
    }

    async fn finish(&mut self) {
        self.probe.release_child.send_replace(true);
        self.probe.release_root.send_replace(true);
        drop(self.host.take());
        self.provider_handle.stop(true).await;
    }
}

fn has_reply(session: &Session, text: &str) -> bool {
    session
        .messages
        .iter()
        .any(|message| message.role == Role::Assistant && message.content == text)
}

fn outcomes(session: &Session) -> Vec<bamboo_domain::SessionChildOutcome> {
    session
        .messages
        .iter()
        .filter_map(|message| {
            let envelope: SessionMessageEnvelope =
                serde_json::from_value(message.metadata.as_ref()?.get("session_message")?.clone())
                    .ok()?;
            match envelope.body {
                bamboo_domain::SessionMessageBody::ChildOutcome(outcome) => Some(outcome),
                _ => None,
            }
        })
        .collect()
}

#[actix_web::test]
async fn waiting_ordinary_root_handles_durable_input_retains_child_result_and_restart_dedupes() {
    let mut fixture = Fixture::new(Case::Input).await;
    let child_id = fixture.held_child().await;
    fixture.guidance(INPUT_ID, INPUT).await;
    fixture
        .wait(
            "durable input executes while original Child remains held",
            |session| has_reply(session, INPUT_REPLY),
        )
        .await;
    assert!(
        !*fixture.probe.release_child.borrow(),
        "Child is still held at input execution"
    );
    fixture.admitted_once(INPUT_ID, INPUT).await;
    fixture.probe.release_child.send_replace(true);
    let completed = fixture
        .wait("original Child result survives the new input", |session| {
            session.last_run_status().as_deref() == Some("completed")
                && has_reply(session, COLLECTED)
                && outcomes(session).len() == 1
        })
        .await;
    let result = &outcomes(&completed)[0];
    assert_eq!(result.child_session_id, child_id);
    assert_eq!(result.status, "completed");
    assert!(result
        .result
        .as_deref()
        .is_some_and(|text| text.contains(CHILD_RESULT)));
    fixture.admitted_once(INPUT_ID, INPUT).await;
    let calls = fixture.probe.input_calls.load(Ordering::SeqCst);

    // Restart after admission and Child finalization, keeping Child recovery
    // outside this Root-input slice. The same sender id remains deduplicated.
    fixture.restart().await;
    fixture.admitted_once(INPUT_ID, INPUT).await;
    fixture.guidance(INPUT_ID, INPUT).await;
    let replayed = fixture.canonical().await;
    fixture.admitted_once(INPUT_ID, INPUT).await;
    assert_eq!(outcomes(&replayed).len(), 1);
    assert_eq!(fixture.probe.child_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.probe.input_calls.load(Ordering::SeqCst), calls);
    fixture.finish().await;
}

#[actix_web::test]
async fn cancelled_root_provider_response_cannot_commit_over_successor_input() {
    let mut fixture = Fixture::new(Case::Cancel).await;
    fixture.held_child().await;
    let original_wait = serde_json::to_value(
        fixture
            .canonical()
            .await
            .agent_runtime_state
            .unwrap()
            .waiting_for_children
            .unwrap(),
    )
    .unwrap();
    fixture.guidance(INPUT_ID, INPUT).await;
    let probe = fixture.probe.clone();
    fixture
        .wait("Root provider holds the admitted input", move |_| {
            probe.root_ready.load(Ordering::SeqCst)
        })
        .await;
    fixture.admitted_once(INPUT_ID, INPUT).await;
    fixture.stop().await;
    fixture
        .wait("cancelled Root owner finalizes", |session| {
            session.last_run_status().as_deref() == Some("cancelled")
        })
        .await;
    fixture.guidance(SUCCESSOR_ID, SUCCESSOR).await;
    let successor = fixture
        .wait("successor input commits", |session| {
            has_reply(session, SUCCESSOR_REPLY)
                && session.last_run_status().as_deref() == Some("suspended")
        })
        .await;
    assert!(!*fixture.probe.release_child.borrow());
    assert_eq!(
        serde_json::to_value(
            successor
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .waiting_for_children
                .as_ref()
                .unwrap()
        )
        .unwrap(),
        original_wait,
        "successor reasoning preserves the original held Child wait lease"
    );
    fixture.probe.release_root.send_replace(true);
    let probe = fixture.probe.clone();
    fixture
        .wait("cancelled provider emits its late response", move |_| {
            probe.late_response_emitted.load(Ordering::SeqCst)
        })
        .await;
    fixture.probe.release_child.send_replace(true);
    fixture
        .wait("original Child reaches the successor", |session| {
            outcomes(session).len() == 1
                && session.last_run_status().as_deref() == Some("completed")
        })
        .await;
    let canonical = fixture.canonical().await;
    assert!(
        !has_reply(&canonical, STALE_REPLY),
        "cancelled owner cannot append a late provider answer"
    );
    assert!(has_reply(&canonical, SUCCESSOR_REPLY));
    fixture.admitted_once(INPUT_ID, INPUT).await;
    fixture.admitted_once(SUCCESSOR_ID, SUCCESSOR).await;
    fixture.finish().await;
}

#[actix_web::test]
async fn root_input_tool_boundary_observes_permission_tightening() {
    let mut fixture = Fixture::new(Case::Permission).await;
    fixture.held_child().await;
    fixture.guidance(INPUT_ID, INPUT).await;
    let probe = fixture.probe.clone();
    fixture
        .wait(
            "Root input provider holds the next tool boundary",
            move |_| probe.root_ready.load(Ordering::SeqCst),
        )
        .await;
    let version = fixture.canonical().await.metadata_version;
    let response = fixture
        .client
        .patch(format!("{}/sessions/{ROOT_ID}", fixture.base))
        .header("If-Match", version.to_string())
        .json(&json!({"permission_mode":"default"}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "permission PATCH: {}",
        response.text().await.unwrap()
    );
    fixture.probe.release_root.send_replace(true);
    let sentinel = fixture.probe.workspace.join("permission-boundary.txt");
    let parked = fixture
        .wait(
            "next Write asks for approval after bypass is removed",
            move |session| {
                assert!(
                    !sentinel.exists(),
                    "tightened permission must prevent the Write"
                );
                session
                    .pending_question
                    .as_ref()
                    .is_some_and(|question| question.tool_call_id == WRITE_CALL)
            },
        )
        .await;
    assert_eq!(
        parked
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .effective_permission_mode(),
        bamboo_domain::SessionPermissionMode::Default
    );
    let permission_result = parked
        .messages
        .iter()
        .find(|message| {
            message.role == Role::Tool && message.tool_call_id.as_deref() == Some(WRITE_CALL)
        })
        .expect("the attempted Write has a canonical permission result");
    let payload: Value = serde_json::from_str(&permission_result.content).unwrap();
    assert_eq!(payload["status"], "awaiting_permission_approval");
    fixture.admitted_once(INPUT_ID, INPUT).await;
    fixture.finish().await;
}

#[actix_web::test]
async fn pending_root_input_survives_host_restart_and_same_id_retry_admits_once() {
    let mut fixture = Fixture::new(Case::PendingRestart).await;
    let probe = fixture.probe.clone();
    fixture
        .wait("initial Root provider boundary is held", move |_| {
            probe.root_ready.load(Ordering::SeqCst)
        })
        .await;
    fixture.guidance_mode(INPUT_ID, INPUT, "after_run").await;
    let before = fixture.canonical().await;
    assert!(!before.messages.iter().any(|message| message.id == INPUT_ID));
    let store = Arc::new(SessionStoreV2::new(fixture.data.clone()).await.unwrap());
    let inbox = FileSessionInbox::new(store, Default::default());
    let backlog = inbox.inspect(ROOT_ID).await.unwrap();
    assert_eq!((backlog.pending, backlog.claimed), (1, 0));
    assert!(!inbox
        .was_admitted(ROOT_ID, &SessionMessageId::parse(INPUT_ID).unwrap())
        .await
        .unwrap());
    let pending: Value = fixture
        .client
        .get(format!("{}/sessions/{ROOT_ID}/guidance", fixture.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(pending["messages"].as_array().unwrap().len(), 1);
    assert_eq!(pending["messages"][0]["id"], INPUT_ID);
    assert_eq!(pending["messages"][0]["text"], INPUT);

    // The Host dies before this input reaches a transcript writer. Reopening
    // the same data directory and retrying the sender's existing id must
    // activate the recovered durable delivery without another Chat append.
    fixture.restart().await;
    fixture.guidance_mode(INPUT_ID, INPUT, "after_run").await;
    fixture
        .wait(
            "recovered pending input executes after Host restart",
            |session| {
                session.last_run_status().as_deref() == Some("completed")
                    && has_reply(session, INPUT_REPLY)
            },
        )
        .await;
    fixture.admitted_once(INPUT_ID, INPUT).await;
    assert_eq!(fixture.probe.input_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.probe.child_calls.load(Ordering::SeqCst), 0);
    fixture.probe.release_root.send_replace(true);
    let probe = fixture.probe.clone();
    fixture
        .wait(
            "pre-restart provider emits its obsolete response",
            move |_| probe.late_response_emitted.load(Ordering::SeqCst),
        )
        .await;
    assert!(!has_reply(&fixture.canonical().await, STALE_REPLY));
    fixture.admitted_once(INPUT_ID, INPUT).await;
    fixture.finish().await;
}
