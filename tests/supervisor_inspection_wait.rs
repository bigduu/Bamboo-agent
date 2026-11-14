//! Controlled provider + actual serve/current_exe workers, in disposable homes.
//! This proves runtime mechanisms, not a real model's choice of coordination tools.
#![cfg(unix)]

use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_domain::{
    ParentQuestion, Role, Session, SessionInboxPort, SessionMessageBody, SessionMessageEnvelope,
    Storage, DEFAULT_SUPERVISOR_SESSION_ID as SUPERVISOR,
};
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

const WORK: &str = "selected-work-root";
const ORIGINAL: &str = "Preserve this independent Root's original history and context.";
const FOLLOWUP: &str = "CONTINUE_ORIGINAL_ROOT: perform the same independent work.";
const STEER: &str = "NEW_SUPERVISOR_INPUT: acknowledge while the verifier is still running.";
const ANSWER: &str = "Use the original selected Root; do not create another worker.";
const DECISION: &str =
    "USER_DECISION: Use the original selected Root; do not create another worker.";
const NEEDS_DECISION: &str =
    "USER_DECISION_REQUIRED: the verifier needs a decision before continuing.";
const DONE: &str = "SUPERVISOR_COLLECTED_OUTCOMES";

#[derive(Clone, Copy, Debug)]
enum Case {
    Completed,
    Failed,
    Question,
    Concurrent,
}

struct Probe {
    data: PathBuf,
    case: Case,
    root_calls: AtomicUsize,
    inspections: AtomicUsize,
    work_calls: AtomicUsize,
    requests: Mutex<Vec<Value>>,
}

fn result(body: &Value, id: &str) -> Option<Value> {
    let message = body["messages"]
        .as_array()?
        .iter()
        .find(|m| m["role"] == "tool" && m["tool_call_id"] == id)?;
    Some(
        serde_json::from_str(message["content"].as_str()?)
            .unwrap_or_else(|_| panic!("{id}: {}", message["content"])),
    )
}
fn has(body: &Value, text: &str) -> bool {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["content"].as_str().is_some_and(|s| s.contains(text)))
}
fn has_tool_result(body: &Value, id: &str) -> bool {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|message| message["role"] == "tool" && message["tool_call_id"] == id)
}
fn call(id: &str, name: &str, args: Value) -> (Value, &'static str) {
    (
        json!({"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":name,"arguments":args.to_string()}}]}),
        "tool_calls",
    )
}
fn envelopes(session: &Session) -> Vec<SessionMessageEnvelope> {
    session
        .messages
        .iter()
        .filter_map(|m| {
            serde_json::from_value(m.metadata.as_ref()?.get("session_message")?.clone()).ok()
        })
        .collect()
}
fn outcomes(session: &Session) -> Vec<SessionMessageEnvelope> {
    envelopes(session)
        .into_iter()
        .filter(|e| matches!(&e.body, SessionMessageBody::ChildOutcome(_)))
        .collect()
}
fn report() -> Value {
    json!({"version":1,"outcome":"completed","summary":"AUXILIARY_VERIFICATION_DONE", "reported_evidence":[],"reported_verification":[],"proposals":[],"blockers":[],"open_decisions":[]})
}

async fn provider(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let body = body.into_inner();
    probe.requests.lock().unwrap().push(body.clone());
    let (delta, finish) = if body["model"] == "wait-child" {
        if !has_tool_result(&body, "verifier-command") {
            // Hold an actual foreground command, not only a provider response.
            // The test releases its file after proving the parent is quiescent.
            let index = usize::from(has(&body, "second non-overlapping acceptance"));
            let command = format!("touch verifier-started-{index}; while [ ! -f verifier-release ]; do sleep 0.05; done; printf 'verified\\n'");
            call(
                "verifier-command",
                "bash",
                json!({"command":command,"timeout":30}),
            )
        } else if matches!(probe.case, Case::Failed) {
            return HttpResponse::BadRequest().json(json!({"error":{"type":"invalid_request_error","message":"controlled verifier provider failure"}}));
        } else if matches!(probe.case, Case::Question)
            && !has_tool_result(&body, "verifier-question")
        {
            call(
                "verifier-question",
                "SubAgent",
                json!({"intent":"ask_parent","message":"Which original Root should I use?"}),
            )
        } else {
            if matches!(probe.case, Case::Question) {
                assert!(has(&body, ANSWER));
            }
            (json!({"content":report().to_string()}), "stop")
        }
    } else if body["model"] == "wait-work" {
        assert!(has(&body, ORIGINAL));
        assert!(has(&body, FOLLOWUP));
        probe.work_calls.fetch_add(1, Ordering::SeqCst);
        (json!({"content":"INDEPENDENT_WORK_DONE"}), "stop")
    } else if body["model"] == "wait-supervisor" {
        let round = probe.root_calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            round < 24,
            "controlled negative bound: inspect polling never parks"
        );
        let store = SessionStoreV2::new(probe.data.clone()).await.unwrap();
        let parent = store.load_session(SUPERVISOR).await.unwrap().unwrap();
        let terminal = outcomes(&parent);
        let wanted = if matches!(probe.case, Case::Concurrent) {
            2
        } else {
            1
        };
        if terminal.len() >= wanted {
            for envelope in terminal {
                if let SessionMessageBody::ChildOutcome(outcome) = envelope.body {
                    assert!(body.to_string().contains(&outcome.child_session_id));
                }
            }
            (json!({"content":DONE}), "stop")
        } else if !terminal.is_empty() {
            (
                json!({"content":"COLLECTED_ONE_CONCURRENT_OUTCOME"}),
                "stop",
            )
        } else if let Some(question) = envelopes(&parent)
            .iter()
            .find_map(ParentQuestion::from_envelope)
            .filter(|_| !has_tool_result(&body, "answer-verifier"))
        {
            assert!(body.to_string().contains(question.id.as_str()));
            if !has(&body, DECISION) {
                (json!({"content":NEEDS_DECISION}), "stop")
            } else {
                call(
                    "answer-verifier",
                    "SubAgent",
                    json!({"reply_to":question.id.as_str(),"message":ANSWER}),
                )
            }
        } else if has_tool_result(&body, "answer-verifier") {
            (
                json!({"content":"ANSWERED_VERIFIER_WITH_ORIGINAL_WAIT"}),
                "stop",
            )
        } else if has(&body, STEER) && !has(&body, "STEERING_HANDLED") {
            (json!({"content":"STEERING_HANDLED"}), "stop")
        } else if result(&body, "directory").is_none() {
            call(
                "directory",
                "session_control",
                json!({"action":"directory","query":WORK,"limit":2}),
            )
        } else if result(&body, "select-root").is_none() {
            let directory = result(&body, "directory").unwrap();
            assert_eq!(directory["entries"][0]["session_id"], WORK);
            call(
                "select-root",
                "session_history",
                json!({"action":"inspect","session_id":WORK}),
            )
        } else if result(&body, "follow-original").is_none() {
            let selected = result(&body, "select-root").unwrap();
            assert!(selected["current_user_target"].is_object(), "{selected}");
            call(
                "follow-original",
                "session_control",
                json!({"action":"followup","target_session_id":WORK,
                "target_ref":selected["current_user_target"],"operation_id":"24510000-0000-4000-8000-000000000001","message":FOLLOWUP}),
            )
        } else if result(&body, "follow-original-again").is_none() {
            let selected = result(&body, "select-root").unwrap();
            call(
                "follow-original-again",
                "session_control",
                json!({"action":"followup","target_session_id":WORK,
                "target_ref":selected["current_user_target"],"operation_id":"24510000-0000-4000-8000-000000000001","message":FOLLOWUP}),
            )
        } else if result(&body, "create-verifier-0").is_none() {
            assert_eq!(
                result(&body, "follow-original").unwrap()["target_session_id"],
                WORK
            );
            call(
                "create-verifier-0",
                "SubAgent",
                json!({"message":"Perform bounded auxiliary verification in the assigned workspace and return a v1 child report; do not implement the main task."}),
            )
        } else if wanted == 2 && result(&body, "create-verifier-1").is_none() {
            call(
                "create-verifier-1",
                "SubAgent",
                json!({"message":"Independently verify the second non-overlapping acceptance observation; do not implement the main task."}),
            )
        } else {
            tokio::time::timeout(Duration::from_secs(60), async {
                while !(0..wanted).all(|index| {
                    probe
                        .workspace
                        .join(format!("verifier-started-{index}"))
                        .exists()
                }) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            let batch = probe.inspections.fetch_add(wanted, Ordering::SeqCst) / wanted;
            let calls: Vec<_> = (0..wanted).map(|index| json!({"index":index,"id":format!("inspect-{batch}-{index}"),"type":"function","function":{
                "name":"SubAgent","arguments":json!({"intent":"inspect","target":result(&body,&format!("create-verifier-{index}")).unwrap()["actor_id"],
                "message":if matches!(probe.case,Case::Failed) {"diagnostics"} else {"overview"}}).to_string()}})).collect();
            (json!({"tool_calls":calls}), "tool_calls")
        }
    } else {
        (json!({"content":"bounded auxiliary response"}), "stop")
    };
    let chunk = json!({"id":"wait-response","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {chunk}\n\ndata: [DONE]\n\n"))
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
async fn canonical(data: &Path, id: &str) -> Session {
    SessionStoreV2::new(data.to_path_buf())
        .await
        .unwrap()
        .load_session(id)
        .await
        .unwrap()
        .unwrap()
}
async fn wait(data: &Path, id: &str, predicate: impl Fn(&Session) -> bool) -> Session {
    match tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let session = canonical(data, id).await;
            if predicate(&session) {
                break session;
            }
            assert_ne!(
                session.last_run_status().as_deref(),
                Some("error"),
                "{:?}",
                session.last_run_error()
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    {
        Ok(session) => session,
        Err(_) => panic!(
            "waiting for {id}: {:?}; log {}",
            canonical(data, id).await.agent_runtime_state,
            std::fs::read_to_string(data.join("host.log"))
                .unwrap()
                .lines()
                .rev()
                .take(20)
                .collect::<Vec<_>>()
                .join("\n")
        ),
    }
}
async fn post(client: &reqwest::Client, url: String, value: Value) {
    let response = client.post(url).json(&value).send().await.unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
}

#[test]
fn supervisor_inspection_wait_native_host_mechanism_and_directory_followup() {
    std::thread::Builder::new()
        .name("supervisor-wait-provider".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            actix_web::rt::System::new().block_on(async {
                for (case, ultra) in [
                    (Case::Completed, false),
                    (Case::Failed, true),
                    (Case::Question, false),
                    (Case::Concurrent, true),
                ] {
                    fixture(case, ultra).await;
                }
            })
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn fixture(case: Case, ultra: bool) {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().canonicalize().unwrap();
    let workspace = data.join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let work_workspace = data.join("independent-workspace");
    std::fs::create_dir(&work_workspace).unwrap();
    // Keep real permission checking enabled. The disposable fixture permits
    // its verifier command and followup; Supervisor execution is still denied
    // by its independent orchestration boundary. directory needs #2450.
    let policy = bamboo_tools::permission::PermissionConfig::new();
    policy.add_rule(bamboo_tools::permission::PermissionRule::new(
        bamboo_tools::permission::PermissionType::ExecuteCommand,
        "*",
        true,
    ));
    bamboo_tools::permission::storage::PermissionStorage::new(&data)
        .save(&policy)
        .await
        .unwrap();
    let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
    let project = projects
        .create_with_project_path("inspection-wait", None, workspace.to_string_lossy(), vec![])
        .unwrap();
    let work_project = projects
        .create_with_project_path(
            "independent-original",
            None,
            work_workspace.to_string_lossy(),
            vec![],
        )
        .unwrap();
    let probe = web::Data::new(Probe {
        data: data.clone(),
        case,
        root_calls: AtomicUsize::new(0),
        inspections: AtomicUsize::new(0),
        work_calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let provider_probe = probe.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(provider_probe.clone())
            .route("/v1/chat/completions", web::post().to(provider))
            .route("/v1/models",web::get().to(|| async {
                HttpResponse::Ok().json(json!({"data":[{"id":"wait-supervisor"},{"id":"wait-work"},{"id":"wait-child"},{"id":"wait-aux"}]}))
            }))
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let url = format!("http://{}/v1", server.addrs()[0]);
    let running = server.run();
    let provider_handle = running.handle();
    actix_web::rt::spawn(running);
    std::fs::write(data.join("config.json"),serde_json::to_vec(&json!({
        "provider":"openai","features":{"provider_model_ref":true},
        "providers":{"openai":{"api_key":"fixture-key","base_url":url,"model":"wait-supervisor","fast_model":"wait-aux","runtime_models":["wait-supervisor","wait-child","wait-work","wait-aux"]}},
        "defaults":{"chat":{"provider":"openai","model":"wait-supervisor"},"subagent_models":{"worker":{"provider":"openai","model":"wait-child"}}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":2}
    })).unwrap()).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut host = start(&data, port);
    let client = reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}/api/v1");
    // A fresh disposable Home performs the real durable config/Skill install.
    // This fixture budget is separate from every runner, wait and tool lease.
    let healthy = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            assert!(host.0.try_wait().unwrap().is_none());
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
    .await;
    assert!(
        healthy.is_ok(),
        "Host startup timed out: {}",
        std::fs::read_to_string(data.join("host.log")).unwrap()
    );
    post(&client,format!("{base}/chat"),json!({"session_id":WORK,"message":ORIGINAL,"provider":"openai","model":"wait-work",
        "model_ref":{"provider":"openai","model":"wait-work"},"thinking_mode":"standard","permission_mode":"auto","workspace_path":work_workspace,"project_id":work_project.id})).await;
    let original = canonical(&data, WORK).await;
    post(&client, format!("{base}/supervisor/default"), json!({})).await;
    // Bootstrap creates an existing, unassigned Supervisor. Bind its fixture
    // context through the same CAS endpoint as the UI, never by rewriting chat.
    let detail = client
        .get(format!("{base}/sessions/{SUPERVISOR}"))
        .send()
        .await
        .unwrap();
    assert!(detail.status().is_success());
    let etag = detail.headers().get(reqwest::header::ETAG).unwrap().clone();
    let binding = client
        .patch(format!("{base}/sessions/{SUPERVISOR}"))
        .header(reqwest::header::IF_MATCH, etag)
        .json(&json!({"project_id":project.id,"workspace_path":workspace}))
        .send()
        .await
        .unwrap();
    assert!(
        binding.status().is_success(),
        "{}",
        binding.text().await.unwrap()
    );
    if ultra {
        let detail: Value = client
            .get(format!("{base}/sessions/{SUPERVISOR}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let session = &detail["session"];
        let epoch = session["root_mode_transition_epoch"].as_u64().unwrap();
        let operation = format!("{epoch}:{}", uuid::Uuid::new_v4());
        post(&client,format!("{base}/sessions/{SUPERVISOR}/root-mode-operations/{operation}"),json!({
            "birth_token":session["root_mode_birth_token"],"expected_epoch":epoch,"thinking_mode":"ultra"
        })).await;
    }
    post(&client,format!("{base}/chat"),json!({"session_id":SUPERVISOR,"message":"Continue existing independent main work using the directory, then coordinate bounded auxiliary verification.",
        "provider":"openai","model":"wait-supervisor","model_ref":{"provider":"openai","model":"wait-supervisor"},
        "permission_mode":"default","workspace_path":workspace,"project_id":project.id})).await;
    assert_eq!(
        canonical(&data, SUPERVISOR).await.root_thinking_mode(),
        if ultra {
            bamboo_domain::RootThinkingMode::Ultra
        } else {
            bamboo_domain::RootThinkingMode::Standard
        }
    );
    post(&client, format!("{base}/execute/{SUPERVISOR}"), json!({})).await;
    let sleeping = wait(&data, SUPERVISOR, |s| {
        s.last_run_status().as_deref() == Some("suspended")
            && s.agent_runtime_state
                .as_ref()
                .is_some_and(|r| r.waiting_for_children.is_some())
    })
    .await;
    let original_wait = sleeping
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .clone()
        .unwrap();
    let wanted = if matches!(case, Case::Concurrent) {
        2
    } else {
        1
    };
    assert_eq!(original_wait.child_session_ids.len(), wanted);
    let inspect_count = probe.inspections.load(Ordering::SeqCst);
    assert_eq!(
        inspect_count,
        3 * wanted,
        "three unchanged complete inspection batches before waiting"
    );
    let calls = probe.root_calls.load(Ordering::SeqCst);
    if matches!(case, Case::Failed) {
        // The third result itself suspends the runner, so it is durable but
        // intentionally never sent in another provider request.
        let diagnostics: Vec<Value> = (0..3)
            .map(|batch| {
                let id = format!("inspect-{batch}-0");
                let message = sleeping
                    .messages
                    .iter()
                    .find(|message| {
                        message.role == Role::Tool
                            && message.tool_call_id.as_deref() == Some(id.as_str())
                    })
                    .unwrap_or_else(|| panic!("missing durable diagnostic {id}"));
                serde_json::from_str(&message.content).unwrap()
            })
            .collect();
        let revisions: std::collections::BTreeSet<_> = diagnostics
            .iter()
            .filter_map(|value| value["activation"]["directory_revision"].as_u64())
            .collect();
        for diagnostic in &diagnostics {
            assert_eq!(diagnostic["view"], "diagnostics");
            assert_eq!(diagnostic["actor_id"], original_wait.child_session_ids[0]);
            assert_eq!(diagnostic["observed_status"], "running");
            assert_eq!(diagnostic["activation"]["available"], true);
            assert!(diagnostic["activation"]["directory_revision"].is_u64());
        }
        // This ordinary local worker route need not renew an Actor lease.
        // Renewal churn is controlled explicitly in the engine regression;
        // these are three actual durable Host diagnostics, not invented ticks.
        eprintln!("2451 native diagnostics revisions={revisions:?} (observed only); inspect={inspect_count}");
    }
    // Outlast the audited 3–5 second polling interval while the real verifier
    // command is still held and no completion/input event has been sent.
    let quiet_started = std::time::Instant::now();
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        probe.root_calls.load(Ordering::SeqCst),
        calls,
        "waiting dispatches zero providers"
    );
    assert_eq!(probe.inspections.load(Ordering::SeqCst), inspect_count);
    let quiet_ms = quiet_started.elapsed().as_millis();
    assert!(!workspace.join("verifier-release").exists());
    for child in &original_wait.child_session_ids {
        let child = canonical(&data, child).await;
        assert_ne!(child.last_run_status().as_deref(), Some("cancelled"));
        assert!(child.messages.iter().any(|m| m
            .tool_calls
            .as_ref()
            .is_some_and(|calls| calls.iter().any(|c| c.function.name == "bash"))));
    }
    post(
        &client,
        format!("{base}/sessions/{SUPERVISOR}/guidance"),
        json!({"id":"2451-steering","text":STEER,"mode":"after_round"}),
    )
    .await;
    let steered = wait(&data, SUPERVISOR, |s| {
        s.messages
            .iter()
            .any(|m| m.role == Role::Assistant && m.content == "STEERING_HANDLED")
            && s.last_run_status().as_deref() == Some("suspended")
    })
    .await;
    assert_eq!(
        steered.agent_runtime_state.unwrap().waiting_for_children,
        Some(original_wait.clone())
    );
    let steered_calls = probe.root_calls.load(Ordering::SeqCst);
    post(
        &client,
        format!("{base}/sessions/{SUPERVISOR}/guidance"),
        json!({"id":"2451-steering","text":STEER,"mode":"after_round"}),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        probe.root_calls.load(Ordering::SeqCst),
        steered_calls,
        "duplicate steering cannot dispatch again"
    );
    std::fs::write(
        workspace.join("verifier-release"),
        "release only fixture commands",
    )
    .unwrap();
    if matches!(case, Case::Question) {
        let needs_input = wait(&data, SUPERVISOR, |session| {
            session.last_run_status().as_deref() == Some("suspended")
                && session.messages.iter().any(|message| {
                    message.role == Role::Assistant && message.content == NEEDS_DECISION
                })
        })
        .await;
        assert_eq!(
            needs_input
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .waiting_for_children
                .as_ref(),
            Some(&original_wait)
        );
        assert!(
            outcomes(&needs_input).is_empty(),
            "a question is nonterminal, not a completed Child"
        );
        assert_eq!(
            envelopes(&needs_input)
                .iter()
                .filter_map(ParentQuestion::from_envelope)
                .count(),
            1
        );
        post(
            &client,
            format!("{base}/sessions/{SUPERVISOR}/guidance"),
            json!({"id":"2451-decision","text":DECISION,"mode":"after_round"}),
        )
        .await;
    }
    let completed = wait(&data, SUPERVISOR, |s| {
        s.last_run_status().as_deref() == Some("completed")
            && s.messages
                .iter()
                .any(|m| m.role == Role::Assistant && m.content == DONE)
    })
    .await;
    let delivered = outcomes(&completed);
    assert_eq!(delivered.len(), wanted);
    assert_eq!(
        delivered
            .iter()
            .map(|e| e.id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        wanted
    );
    for envelope in &delivered {
        if let SessionMessageBody::ChildOutcome(outcome) = &envelope.body {
            assert_eq!(
                outcome.status,
                if matches!(case, Case::Failed) {
                    "error"
                } else {
                    "completed"
                }
            );
        }
    }
    assert_eq!(
        completed
            .messages
            .iter()
            .filter(|m| m.id == "2451-steering")
            .count(),
        1
    );
    if matches!(case, Case::Question) {
        assert_eq!(
            completed
                .messages
                .iter()
                .filter(|message| message.id == "2451-decision" && message.content == DECISION)
                .count(),
            1
        );
    }
    assert!(completed
        .agent_runtime_state
        .unwrap()
        .waiting_for_children
        .is_none());
    let work = wait(&data, WORK, |s| {
        s.last_run_status().as_deref() == Some("completed")
    })
    .await;
    assert_eq!(work.id, original.id);
    assert_eq!(work.created_at, original.created_at);
    assert_eq!(work.kind, original.kind);
    assert_eq!(work.root_thinking_mode(), original.root_thinking_mode());
    assert_eq!(work.root_session_id, original.root_session_id);
    assert_eq!(work.project_id_meta(), original.project_id_meta());
    assert_eq!(work.workspace_path_meta(), original.workspace_path_meta());
    assert_eq!(
        work.agent_runtime_state.as_ref().unwrap().permission_mode,
        original
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .permission_mode
    );
    for message in original.messages.iter().filter(|m| m.role == Role::User) {
        assert!(work
            .messages
            .iter()
            .any(|m| m.id == message.id && m.content == message.content));
    }
    assert!(work
        .messages
        .iter()
        .any(|m| m.role == Role::Assistant && m.content == "INDEPENDENT_WORK_DONE"));
    assert_eq!(
        probe.work_calls.load(Ordering::SeqCst),
        1,
        "one independent Root admission, no replacement dispatch"
    );
    let cold = Arc::new(SessionStoreV2::new(data.clone()).await.unwrap());
    assert_eq!(
        cold.list_index_entries()
            .await
            .iter()
            .filter(|e| e.parent_session_id.as_deref() == Some(SUPERVISOR))
            .count(),
        wanted
    );
    let inbox = bamboo_storage::FileSessionInbox::new(cold, Default::default());
    let generation = inbox.inspect(SUPERVISOR).await.unwrap().generation;
    let final_calls = probe.root_calls.load(Ordering::SeqCst);
    for envelope in &delivered {
        assert!(inbox.was_admitted(SUPERVISOR, &envelope.id).await.unwrap());
        inbox.deliver(envelope).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        inbox.inspect(SUPERVISOR).await.unwrap().generation,
        generation,
        "duplicate outcomes retain the admission watermark"
    );
    assert_eq!(
        probe.root_calls.load(Ordering::SeqCst),
        final_calls,
        "duplicate outcomes never dispatch another turn"
    );
    assert_eq!(outcomes(&canonical(&data, SUPERVISOR).await).len(), wanted);
    eprintln!("2451 controlled-provider/native-Host case={case:?} ultra={ultra}: before_wait provider={calls}, inspect={inspect_count}; quiet_ms={quiet_ms}, quiet_delta=0; outcomes={wanted}; final_provider={}; independent_root_calls=1",probe.root_calls.load(Ordering::SeqCst));
    drop(host);
    provider_handle.stop(true).await;
}
