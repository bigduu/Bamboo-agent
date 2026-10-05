//! Actual compiled `bamboo serve` -> default current_exe worker -> provider IR.
//! Only the remote model is fake. Negative budget/guidance cases explicitly
//! author the trusted host store between create(auto_run=false) and run; they
//! are fault fixtures, not a public in-place assignment or settings API.
#![cfg(unix)]
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::{ChildContextBinding, ChildContextPacket, Message, Role, TokenBudget};
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

#[derive(Clone, Copy, PartialEq, Debug)]
enum Case {
    Complete,
    TypedReport,
    LargeProse,
    TinyBudget,
    HugeGuidance,
    Overflow,
    UnsupportedStartup,
}
impl Case {
    fn normal_success(self) -> bool {
        matches!(self, Self::Complete | Self::TypedReport | Self::LargeProse)
    }
    fn creates_child(self) -> bool {
        !matches!(self, Self::Overflow | Self::UnsupportedStartup)
    }
}
struct Probe {
    case: Case,
    workspace: PathBuf,
    packet: ChildContextPacket,
    root_calls: AtomicUsize,
    requests: Mutex<Vec<Value>>,
}
fn tool_delta(id: &str, args: Value) -> Value {
    json!({"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":"SubAgent","arguments":args.to_string()}}]})
}
async fn response(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let body = body.into_inner();
    probe.requests.lock().unwrap().push(body.clone());
    let (delta, finish) = if body["model"] == "child-packet-test" {
        if probe.case == Case::HugeGuidance {
            let wire = body.to_string();
            eprintln!(
                "HugeGuidance child wire bytes={}, contains_guidance={}",
                wire.len(),
                wire.contains("Host guidance Host guidance")
            );
        }
        (
            json!({"content": match probe.case {
                Case::TypedReport => typed_report().to_string(),
                Case::LargeProse => large_prose_report(),
                _ => "REAL_CHILD_PACKET_EVIDENCE".into(),
            }}),
            "stop",
        )
    } else if body["model"] == "root-packet-test" && body["tools"].to_string().contains("SubAgent")
    {
        match probe.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                let mut packet = probe.packet.clone();
                let disk = SessionStoreV2::new(probe.workspace.clone()).await.unwrap();
                let parent = disk.load_session("packet-root").await.unwrap().unwrap();
                packet.source_user_message_ids = vec![parent
                    .messages
                    .iter()
                    .find(|message| message.role == Role::User)
                    .unwrap()
                    .id
                    .clone()];
                if probe.case == Case::Overflow {
                    packet.objective = "x\n".repeat(8193);
                }
                (
                    tool_delta(
                        "packet-create",
                        json!({"action":"create","title":"Bounded packet","responsibility":"Read the exact packet only",
                    "prompt":"Verify the bounded assignment 🪷","subagent_type":"packet-worker","workspace":probe.workspace,
                    "model":"openai:child-packet-test","auto_run":false,"context_packet":packet}),
                    ),
                    "tool_calls",
                )
            }
            1 if probe.case == Case::Complete => {
                let disk = SessionStoreV2::new(probe.workspace.clone()).await.unwrap();
                let child_id = disk
                    .list_index_entries()
                    .await
                    .into_iter()
                    .find(|entry| entry.parent_session_id.as_deref() == Some("packet-root"))
                    .unwrap()
                    .id;
                (
                    tool_delta(
                        "packet-reject-update",
                        json!({"action":"update","child_session_id":child_id,"prompt":"Replace the original goal"}),
                    ),
                    "tool_calls",
                )
            }
            round @ (1 | 2)
                if probe.case.creates_child() && (round == 1 || probe.case == Case::Complete) =>
            {
                // Each process/store owns an index cache; reopen the reader to
                // observe the actual host's newly persisted Child entry.
                let disk = SessionStoreV2::new(probe.workspace.clone()).await.unwrap();
                let entries = disk.list_index_entries().await;
                let child_id = entries
                    .iter()
                    .find(|entry| entry.parent_session_id.as_deref() == Some("packet-root"))
                    .expect("actual SubAgent.create persisted child")
                    .id
                    .clone();
                if !probe.case.normal_success() {
                    let mut child = disk.load_session(&child_id).await.unwrap().unwrap();
                    let mut binding = ChildContextBinding::from_session(&child).unwrap().unwrap();
                    let tiny = probe.case == Case::TinyBudget;
                    child.token_budget = Some(TokenBudget::with_safety_margin(
                        if tiny { 128 } else { 64_000 },
                        if tiny { 64 } else { 1024 },
                        Default::default(),
                        0,
                    ));
                    let old_id = binding.assignment_message().id;
                    binding.bind_host_budget(&child).unwrap();
                    binding.install(&mut child).unwrap();
                    let old_index = child
                        .messages
                        .iter()
                        .position(|message| message.id == old_id)
                        .unwrap();
                    child.messages[old_index] = binding.assignment_message();
                    child
                        .messages
                        .retain(|message| !message.id.starts_with("child-background-v1:"));
                    child.messages.extend(binding.background_messages());
                    if probe.case == Case::HugeGuidance {
                        child
                            .messages
                            .insert(0, Message::system("Host guidance ".repeat(100_000)));
                    }
                    child.updated_at = chrono::Utc::now();
                    disk.save_session(&child).await.unwrap();
                }
                (
                    tool_delta(
                        "packet-run",
                        json!({"action":"run","child_session_id":child_id,"reset_to_last_user":false}),
                    ),
                    "tool_calls",
                )
            }
            _ => (json!({"content":"REAL_ROOT_PACKET_FINISHED"}), "stop"),
        }
    } else {
        (json!({"content":"auxiliary response"}), "stop")
    };
    let event = json!({"id":"packet-response","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
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
fn start_host(data: &Path, port: u16) -> Host {
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
async fn await_host(host: &mut Host, client: &reqwest::Client, base: &str, data: &Path) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "real host exited: {}",
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
}
async fn fixture(case: Case) {
    eprintln!("actual CLI case {case:?}");
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().canonicalize().unwrap();
    let mut packet = ChildContextPacket {
        version: 1,
        objective: "Exact objective \"quoted\" \\ 🪷".into(),
        constraints: vec!["No commits, no task expansion".into()],
        acceptance: vec!["Return concrete evidence".into()],
        non_goals: vec!["No adjacent cleanup".into()],
        necessary_user_instructions: vec!["Preserve complete UTF-8 用户约束".into()],
        recorded_decisions: vec!["Use existing architecture".into()],
        source_user_message_ids: vec!["source-user".into()],
        background_message_ids: (0..10).map(|index| format!("background-{index}")).collect(),
    };
    if case == Case::TypedReport {
        packet.acceptance.push(format!(
            "Return exactly this JSON report shape, without prose or fences: {}",
            typed_report()
        ));
    }
    let probe = web::Data::new(Probe {
        case,
        workspace: data.clone(),
        packet,
        root_calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let provider_probe = probe.clone();
    let provider = HttpServer::new(move || {
        App::new()
            .app_data(provider_probe.clone())
            .route("/v1/chat/completions", web::post().to(response))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok().json(
                        json!({"data":[{"id":"root-packet-test"},{"id":"child-packet-test"}]}),
                    )
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let provider_url = format!("http://{}/v1", provider.addrs()[0]);
    let provider_running = provider.run();
    let provider_handle = provider_running.handle();
    actix_web::rt::spawn(provider_running);
    // No worker_bin/worker_args override: both host and worker are this Cargo
    // fixture's actual compiled artifact, using the production current_exe path.
    let mut config = json!({"provider":"openai",
        "features":{"provider_model_ref":true},"providers":{"openai":{"api_key":"fixture-key","base_url":provider_url,"model":"root-packet-test"}},
        "defaults":{"chat":{"provider":"openai","model":"root-packet-test"}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1}});
    if case == Case::UnsupportedStartup {
        config["subagents"]["worker_bin"] = json!("/bin/false");
        config["subagents"]["worker_args"] = json!(["subagent-worker"]);
    }
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
    let mut host = start_host(&data, port);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}/api/v1");
    await_host(&mut host, &client, &base, &data).await;
    let created = client.post(format!("{base}/chat")).json(&json!({"session_id":"packet-root","message":"ROOT_COMPLETE_INSTRUCTION 🪷",
        "model":"root-packet-test","provider":"openai","model_ref":{"provider":"openai","model":"root-packet-test"},
        "permission_mode":"bypass","workspace_path":data})).send().await.unwrap();
    assert!(
        created.status().is_success(),
        "chat: {}",
        created.text().await.unwrap()
    );
    // Stop the real host before fixture-only authoring so metadata refresh
    // cannot race this cold store's load/save. Reopen it after the write.
    drop(host);
    let store = SessionStoreV2::new(data.clone()).await.unwrap();
    let mut parent = store.load_session("packet-root").await.unwrap().unwrap();
    for index in 0..10 {
        let mut message = Message::assistant(format!("bounded background {index}"), None);
        message.id = format!("background-{index}");
        parent.messages.insert(1 + index, message);
    }
    parent.updated_at = chrono::Utc::now();
    store.save_session(&parent).await.unwrap();
    let mut host = start_host(&data, port);
    await_host(&mut host, &client, &base, &data).await;
    if case == Case::UnsupportedStartup {
        let changed = client.post(format!("http://127.0.0.1:{port}/v1/bamboo/config"))
            .json(&json!({"subagents":{"worker_bin":null,"worker_args":null,"executor":"bamboo_runtime"}})).send().await.unwrap();
        assert!(
            changed.status().is_success(),
            "live config flip: {}",
            changed.text().await.unwrap()
        );
    }
    let started = client
        .post(format!("{base}/execute/packet-root"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert!(started.status().is_success());
    let completed = match tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let parent = store.load_session("packet-root").await.unwrap().unwrap();
            if parent
                .messages
                .iter()
                .any(|message| message.content.contains("REAL_ROOT_PACKET_FINISHED"))
                && (!case.normal_success()
                    || parent.last_run_status().as_deref() == Some("completed"))
            {
                break parent;
            }
            if parent.last_run_status().as_deref() == Some("error") {
                panic!(
                    "Root failed: {:?}\n{}",
                    parent.last_run_error(),
                    std::fs::read_to_string(data.join("host.log")).unwrap()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    {
        Ok(parent) => parent,
        Err(error) => {
            let retained = temp.keep();
            panic!(
                "real host timed out: {error}; retained fixture {}\n{}",
                retained.display(),
                std::fs::read_to_string(data.join("host.log")).unwrap()
            );
        }
    };
    let current = SessionStoreV2::new(data.clone()).await.unwrap();
    let child_entries: Vec<_> = current
        .list_index_entries()
        .await
        .into_iter()
        .filter(|entry| entry.parent_session_id.as_deref() == Some("packet-root"))
        .collect();
    let requests = probe.requests.lock().unwrap().clone();
    let child_requests: Vec<_> = requests
        .iter()
        .filter(|request| request["model"] == "child-packet-test")
        .collect();
    if !case.creates_child() {
        assert!(child_entries.is_empty());
        assert!(child_requests.is_empty());
        let expected = if case == Case::Overflow {
            "context_budget_exceeded"
        } else {
            "required_child_context_unsupported"
        };
        assert!(completed
            .messages
            .iter()
            .any(|message| message.content.contains(expected)));
    } else {
        assert_eq!(
            child_entries.len(),
            1,
            "create results: {:?}; host: {}",
            completed
                .messages
                .iter()
                .filter(|message| message.role == Role::Tool)
                .map(|message| &message.content)
                .collect::<Vec<_>>(),
            std::fs::read_to_string(data.join("host.log")).unwrap()
        );
        let reopened = SessionStoreV2::new(data.clone()).await.unwrap();
        let child = reopened
            .load_session(&child_entries[0].id)
            .await
            .unwrap()
            .unwrap();
        let binding = ChildContextBinding::from_session(&child).unwrap().unwrap();
        assert_eq!(binding.payload.parent_created_at, parent.created_at);
        assert_eq!(binding.payload.background.len(), 8);
        assert_eq!(binding.payload.background_omitted, 2);
        assert_eq!(
            serde_json::to_value(&binding.payload.token_budget).unwrap(),
            serde_json::to_value(&child.token_budget).unwrap()
        );
        match case {
            Case::TinyBudget => {
                assert_eq!(child.token_budget.as_ref().unwrap().max_context_tokens, 128)
            }
            Case::HugeGuidance => assert_eq!(
                child.token_budget.as_ref().unwrap().max_context_tokens,
                64_000
            ),
            _ => assert!(child.token_budget.is_none()),
        }
        assert_eq!(
            child.root_thinking_mode(),
            bamboo_domain::RootThinkingMode::Standard
        );
        assert_eq!(child.agent_runtime_state.as_ref().unwrap().read_only, false);
        if case == Case::Complete {
            assert_eq!(
                child.metadata["assignment_prompt"],
                "Verify the bounded assignment 🪷"
            );
            assert!(completed.messages.iter().any(|message| message
                .content
                .contains("immutable assignment cannot be updated")));
            assert_eq!(child.last_run_status().as_deref(), Some("completed"));
            assert_eq!(child_requests.len(), 1);
            assert!(child_requests[0]["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| message["role"] == "user"
                    && message["content"] == binding.payload.required_assignment));
            assert!(binding
                .payload
                .required_assignment
                .contains("ROOT_COMPLETE_INSTRUCTION 🪷"));
            assert!(child
                .messages
                .iter()
                .any(|message| message.content.contains("REAL_CHILD_PACKET_EVIDENCE")));
            assert_unavailable_parent_resume(&requests, &completed, &child, "report_malformed");
            drop(host);
            cold_typed_inspection(
                &data,
                &child,
                &binding,
                "REAL_CHILD_PACKET_EVIDENCE",
                Some("report_malformed"),
            )
            .await;
        } else if case == Case::LargeProse {
            assert_eq!(child.last_run_status().as_deref(), Some("completed"));
            assert_eq!(child_requests.len(), 1);
            let stored = child
                .messages
                .iter()
                .rev()
                .find(|message| message.role == Role::Assistant)
                .unwrap();
            assert_eq!(stored.content, large_prose_report());
            assert!(stored.content.len() > 8192);
            assert_unavailable_parent_resume(
                &requests,
                &completed,
                &child,
                "result_budget_exceeded",
            );
            drop(host);
            cold_typed_inspection(
                &data,
                &child,
                &binding,
                &stored.content,
                Some("result_budget_exceeded"),
            )
            .await;
        } else if case == Case::TypedReport {
            assert_eq!(child.last_run_status().as_deref(), Some("completed"));
            assert_eq!(child_requests.len(), 1);
            assert!(binding
                .payload
                .required_assignment
                .contains("reported_verification"));
            assert!(
                completed
                    .messages
                    .iter()
                    .filter(|m| m.role == Role::Tool)
                    .any(|m| {
                        serde_json::from_str::<Value>(&m.content).is_ok_and(|value| {
                            value["context_packet"]["assignment_sha256"]
                                == binding.assignment_sha256
                                && value["context_packet"]["child_created_at"]
                                    == json!(child.created_at)
                        })
                    }),
                "create returns the actual reloaded Child selectors"
            );
            let stored = child
                .messages
                .iter()
                .rev()
                .find(|m| m.role == Role::Assistant)
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&stored.content).unwrap(),
                typed_report()
            );
            assert_typed_parent_resume(&requests, &completed, &child, &binding);
            // Stop the real host before observing through a cold adapter/tool.
            drop(host);
            cold_typed_inspection(&data, &child, &binding, &stored.content, None).await;
        } else {
            assert!(
                child_requests.is_empty(),
                "budget failure must precede any child provider call"
            );
            assert_eq!(child.last_run_status().as_deref(), Some("error"));
            assert!(child
                .last_run_error()
                .unwrap()
                .contains("context_budget_exceeded"));
        }
    }
    provider_handle.stop(false).await;
}

#[test]
fn real_cli_required_packet_success_and_preprovider_failure_boundaries() {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            actix_web::rt::System::new().block_on(async {
                for case in [
                    Case::Complete,
                    Case::TypedReport,
                    Case::LargeProse,
                    Case::TinyBudget,
                    Case::HugeGuidance,
                    Case::Overflow,
                    Case::UnsupportedStartup,
                ] {
                    fixture(case).await;
                }
            })
        })
        .unwrap()
        .join()
        .unwrap();
}

fn typed_report() -> Value {
    json!({"version":1,"outcome":"blocked","summary":"Need an explicit decision 🪷",
        "reported_evidence":[{"description":"A model claim, not verified","reference":"https://invalid.example/reported-only","sha256":null}],
        "reported_verification":[{"check":"fixture","reported_status":"not_run","details":""}],
        "proposals":[],"blockers":["Root must decide"],"open_decisions":[]})
}

fn large_prose_report() -> String {
    format!(
        "REAL_CHILD_PACKET_EVIDENCE {}",
        "quoted \"claim\" \\ 用户 🪷\n".repeat(1024)
    )
}

fn resume_typed_projection(content: &str) -> Option<Value> {
    // This is the host's explicit projection marker. Do not infer a report
    // shape from arbitrary child-authored text in the Parent request.
    let (_, projection) = content.split_once("Child typed result:\n")?;
    serde_json::from_str(projection.lines().next()?).ok()
}

fn assert_typed_parent_resume(
    requests: &[Value],
    parent: &bamboo_domain::Session,
    child: &bamboo_domain::Session,
    binding: &ChildContextBinding,
) {
    let terminal_source: Value = serde_json::from_str(
        child
            .metadata
            .get("runtime.child_completion_source_v1")
            .expect("the real terminal writer persisted its source seal"),
    )
    .unwrap();
    let provider_messages: Vec<_> = requests
        .iter()
        .filter(|request| request["model"] == "root-packet-test")
        .flat_map(|request| request["messages"].as_array().unwrap())
        .filter(|message| message["role"] == "user")
        .filter_map(|message| {
            let projection = resume_typed_projection(message["content"].as_str()?)?;
            Some((message, projection))
        })
        .collect();
    assert_eq!(
        provider_messages.len(),
        1,
        "the actual resumed Parent provider request must receive the typed result"
    );
    let (provider_message, projection) = &provider_messages[0];
    assert!(serde_json::to_vec(provider_message).unwrap().len() <= 8192);
    assert_eq!(projection["view"], "typed_result");
    assert_eq!(projection["available"], true);
    assert_eq!(projection["child_report"], typed_report());
    assert_eq!(
        projection["host_observation"]["kind"],
        "committed_terminal_source"
    );
    assert_eq!(
        projection["host_observation"]["terminal_source"],
        terminal_source
    );
    assert_eq!(
        projection["host_observation"]["assignment_sha256"],
        binding.assignment_sha256
    );
    assert_eq!(
        projection["host_observation"]["child_created_at"],
        json!(child.created_at)
    );
    assert_eq!(
        projection["host_observation"]["last_run_status"],
        "completed"
    );
    assert_eq!(terminal_source["status"], "completed");
    // Runtime completion is independently observed. The model's blocked
    // outcome and not_run verification remain its own unverified claims.
    assert_eq!(projection["child_report"]["outcome"], "blocked");
    assert_eq!(
        projection["child_report"]["reported_verification"][0]["reported_status"],
        "not_run"
    );
    let runtime_messages: Vec<_> = parent
        .messages
        .iter()
        .filter(|message| {
            message.role == Role::User
                && message.metadata.as_ref().is_some_and(|metadata| {
                    metadata["runtime_kind"] == "child_completion_resume"
                        && metadata["child_session_id"] == child.id
                })
        })
        .collect();
    assert_eq!(runtime_messages.len(), 1, "one durable completion resume");
    let runtime_message = runtime_messages[0];
    assert!(serde_json::to_vec(runtime_message).unwrap().len() <= 8192);
    assert_eq!(
        resume_typed_projection(&runtime_message.content).as_ref(),
        Some(projection)
    );
    assert_eq!(
        runtime_message.metadata.as_ref().unwrap()["child_typed_result_included"],
        true
    );
    assert!(runtime_message.content.contains("Resume the parent task"));
    assert!(runtime_message.content.contains("SubAgent"));
}

fn assert_unavailable_parent_resume(
    requests: &[Value],
    parent: &bamboo_domain::Session,
    child: &bamboo_domain::Session,
    reason: &str,
) {
    let source: Value =
        serde_json::from_str(&child.metadata["runtime.child_completion_source_v1"]).unwrap();
    assert_eq!(source["status"], "completed");
    let root_requests: Vec<_> = requests
        .iter()
        .filter(|request| request["model"] == "root-packet-test")
        .collect();
    assert!(root_requests
        .iter()
        .all(|request| !request.to_string().contains("REAL_CHILD_PACKET_EVIDENCE")));
    assert!(parent
        .messages
        .iter()
        .all(|message| !message.content.contains("REAL_CHILD_PACKET_EVIDENCE")));
    let provider_messages: Vec<_> = root_requests
        .iter()
        .flat_map(|request| request["messages"].as_array().unwrap())
        .filter(|message| message["role"] == "user")
        .filter_map(|message| {
            Some((
                message,
                resume_typed_projection(message["content"].as_str()?)?,
            ))
        })
        .collect();
    assert_eq!(
        provider_messages.len(),
        1,
        "one actual unavailable provider resume"
    );
    let (provider_message, projection) = &provider_messages[0];
    assert!(serde_json::to_vec(provider_message).unwrap().len() <= 8192);
    assert_eq!(
        projection,
        &json!({"view":"typed_result","version":1,"available":false,"reason":reason})
    );
    let runtime_messages: Vec<_> = parent
        .messages
        .iter()
        .filter(|message| {
            message.role == Role::User
                && message.metadata.as_ref().is_some_and(|metadata| {
                    metadata["runtime_kind"] == "child_completion_resume"
                        && metadata["child_session_id"] == child.id
                })
        })
        .collect();
    assert_eq!(runtime_messages.len(), 1, "one durable unavailable resume");
    let message = runtime_messages[0];
    assert!(serde_json::to_vec(message).unwrap().len() <= 8192);
    assert_eq!(
        provider_message["content"].as_str(),
        Some(message.content.as_str())
    );
    assert_eq!(
        resume_typed_projection(&message.content).as_ref(),
        Some(projection)
    );
    assert_eq!(
        message.metadata.as_ref().unwrap()["child_typed_result_included"],
        true
    );
    assert_eq!(
        message.metadata.as_ref().unwrap()["child_final_response_included"],
        false
    );
    assert!(message.content.contains("Resume the parent task"));
    assert!(message.content.contains(&format!(
        "SubAgent with {}",
        json!({"intent":"inspect","target":child.id,"message":"result"})
    )));
    assert!(message.content.contains("follow returned cursors"));
}

async fn cold_typed_inspection(
    data: &Path,
    child: &bamboo_domain::Session,
    binding: &ChildContextBinding,
    content: &str,
    unavailable_reason: Option<&str>,
) {
    use bamboo_agent::server::{
        app_state::AppState,
        tools::{ChildSessionAdapter, SubAgentTool},
    };
    use bamboo_agent_core::tools::{Tool, ToolExecutionContext, ToolOutcome};
    use bamboo_engine::execution::{ChildCompletion, ChildCompletionHandler};
    use sha2::{Digest, Sha256};
    let state = AppState::new(data.to_path_buf()).await.unwrap();
    let adapter = Arc::new(ChildSessionAdapter::new(
        state.session_store.clone(),
        state.storage.clone(),
        state.persistence.clone(),
        state.spawn_scheduler.clone(),
        Arc::default(),
        Arc::default(),
        Arc::default(),
        None,
        None,
        state.config.clone(),
    ));
    let tool = SubAgentTool::new(adapter.clone(), adapter);
    let files = [
        data.join("sessions/packet-root/session.json"),
        data.join("sessions/packet-root/runtime.json"),
        data.join("sessions/packet-root/children")
            .join(&child.id)
            .join("session.json"),
        data.join("sessions/packet-root/children")
            .join(&child.id)
            .join("runtime.json"),
    ];
    let before: Vec<_> = files.iter().map(|p| std::fs::read(p).unwrap()).collect();
    let backlog = state.session_inbox.inspect("packet-root").await.unwrap();
    assert_eq!(backlog.pending, 0);
    assert_eq!(backlog.claimed, 0);
    assert!(state.agent_runners.read().await.is_empty());
    for view in ["result_binding", "typed_result"] {
        let mut args = json!({"action":"get","child_session_id":child.id,"view":view});
        if view == "typed_result" {
            args["expected_child_created_at"] = json!(child.created_at);
            args["expected_assignment_sha256"] = json!(binding.assignment_sha256);
        }
        let ctx = ToolExecutionContext {
            session_id: Some("packet-root"),
            root_session_id: None,
            tool_call_id: "cold-report",
            executing_supervisor: None,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx();
        let ToolOutcome::Completed(result) = tool.invoke(args, ctx).await.unwrap() else {
            panic!("explicit read must complete");
        };
        assert!(serde_json::to_vec(&result).unwrap().len() <= 8192);
        let value: Value = serde_json::from_str(&result.result).unwrap();
        if view == "typed_result" {
            if let Some(reason) = unavailable_reason {
                assert_eq!(
                    value,
                    json!({"view":view,"version":1,"available":false,"reason":reason})
                );
                continue;
            }
        }
        assert_eq!(value["available"], true);
        if view == "typed_result" {
            assert_eq!(value["child_report"], typed_report());
            assert_eq!(value["host_observation"]["last_run_status"], "completed");
            assert_eq!(
                value["host_observation"]["content_sha256"],
                hex::encode(Sha256::digest(content.as_bytes()))
            );
            assert_eq!(
                value["host_observation"]["child_created_at"],
                json!(child.created_at)
            );
            assert_eq!(
                value["host_observation"]["assignment_sha256"],
                binding.assignment_sha256
            );
        } else {
            assert_eq!(value["assignment_sha256"], binding.assignment_sha256);
            assert_eq!(value["child_created_at"], json!(child.created_at));
        }
    }
    // A cold process can receive the original callback again, including the
    // same persisted-source replay used at the watchdog's lost-wake boundary.
    // The cleared durable wait must prevent another message or activation.
    let completion = ChildCompletion {
        parent_session_id: "packet-root".into(),
        child_session_id: child.id.clone(),
        status: "completed".into(),
        error: None,
        completed_at: child.updated_at,
        source: Some(
            serde_json::from_str(&child.metadata["runtime.child_completion_source_v1"]).unwrap(),
        ),
    };
    tokio::join!(
        state
            .child_completion_coordinator
            .on_child_completed(completion.clone()),
        state
            .child_completion_coordinator
            .on_child_completed(completion),
    );
    assert_eq!(
        state.session_inbox.inspect("packet-root").await.unwrap(),
        backlog,
        "cold duplicate/replay cannot admit or authorize another outcome"
    );
    assert!(state.agent_runners.read().await.is_empty());
    assert_eq!(
        files
            .iter()
            .map(|p| std::fs::read(p).unwrap())
            .collect::<Vec<_>>(),
        before
    );
}
