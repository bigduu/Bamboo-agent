//! Actual compiled `bamboo serve` -> default current_exe worker -> provider IR.
//! Only the remote model is fake. Negative budget/guidance cases explicitly
//! author the trusted host store between create(auto_run=false) and run; they
//! are fault fixtures, not a public in-place assignment or settings API.
#![cfg(unix)]
use std::{path::PathBuf, process::{Child, Command, Stdio}, sync::{atomic::{AtomicUsize, Ordering}, Mutex}, time::Duration};
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::{BudgetStrategy, ChildContextBinding, ChildContextPacket, Message, Role, Session, TokenBudget};
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};

#[derive(Clone, Copy, PartialEq)]
enum Case { Complete, TinyBudget, HugeGuidance, Overflow }
struct Probe {
    case: Case,
    store: std::sync::Arc<SessionStoreV2>,
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
        (json!({"content":"REAL_CHILD_PACKET_EVIDENCE"}), "stop")
    } else if body["model"] == "root-packet-test" && body["tools"].to_string().contains("SubAgent") {
        match probe.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                let mut packet = probe.packet.clone();
                if probe.case == Case::Overflow { packet.objective = "x".repeat(33 * 1024); }
                (tool_delta("packet-create", json!({"action":"create","title":"Bounded packet","responsibility":"Read the exact packet only",
                    "prompt":"Verify the bounded assignment 🪷","subagent_type":"packet-worker","workspace":probe.workspace,
                    "model":"openai:child-packet-test","auto_run":false,"context_packet":packet})), "tool_calls")
            }
            1 if probe.case != Case::Overflow => {
                let entries = probe.store.list_index_entries().await;
                let child_id = entries.iter().find(|entry| entry.parent_session_id.as_deref() == Some("packet-root"))
                    .expect("actual SubAgent.create persisted child").id.clone();
                if probe.case != Case::Complete {
                    let mut child = probe.store.load_session(&child_id).await.unwrap().unwrap();
                    let mut binding = ChildContextBinding::from_session(&child).unwrap().unwrap();
                    if probe.case == Case::TinyBudget {
                        child.token_budget = Some(TokenBudget::with_safety_margin(128, 64, BudgetStrategy::Hybrid, 0));
                        let old_id = binding.assignment_message().id;
                        binding.bind_host_budget(&child).unwrap();
                        binding.install(&mut child).unwrap();
                        let old_index = child.messages.iter().position(|message| message.id == old_id).unwrap();
                        child.messages[old_index] = binding.assignment_message();
                        child.messages.retain(|message| !message.id.starts_with("child-background-v1:"));
                        child.messages.extend(binding.background_messages());
                    } else {
                        child.messages.insert(0, Message::system("Host guidance ".repeat(100_000)));
                    }
                    child.updated_at = chrono::Utc::now();
                    probe.store.save_session(&child).await.unwrap();
                }
                (tool_delta("packet-run", json!({"action":"run","child_session_id":child_id,"reset_to_last_user":false})), "tool_calls")
            }
            _ => (json!({"content":"REAL_ROOT_PACKET_FINISHED"}), "stop"),
        }
    } else { (json!({"content":"auxiliary response"}), "stop") };
    let event = json!({"id":"packet-response","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok().content_type("text/event-stream").body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}
struct Host(Child);
impl Drop for Host { fn drop(&mut self) { let _ = self.0.kill(); let _ = self.0.wait(); } }
async fn fixture(case: Case) {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().canonicalize().unwrap();
    let store = std::sync::Arc::new(SessionStoreV2::new(data.clone()).await.unwrap());
    let packet = ChildContextPacket { version:1, objective:"Exact objective \"quoted\" \\ 🪷".into(),
        constraints:vec!["No commits, no task expansion".into()], acceptance:vec!["Return concrete evidence".into()],
        non_goals:vec!["No adjacent cleanup".into()], necessary_user_instructions:vec!["Preserve complete UTF-8 用户约束".into()],
        recorded_decisions:vec!["Use existing architecture".into()], source_user_message_ids:vec!["source-user".into()],
        background_message_ids:(0..10).map(|index| format!("background-{index}")).collect() };
    let probe = web::Data::new(Probe { case, store:store.clone(), workspace:data.clone(), packet,
        root_calls:AtomicUsize::new(0), requests:Mutex::new(Vec::new()) });
    let provider_probe = probe.clone();
    let provider = HttpServer::new(move || App::new().app_data(provider_probe.clone())
        .route("/v1/chat/completions", web::post().to(response))
        .route("/v1/models", web::get().to(|| async { HttpResponse::Ok().json(json!({"data":[{"id":"root-packet-test"},{"id":"child-packet-test"}]})) })))
        .workers(1).bind(("127.0.0.1",0)).unwrap();
    let provider_url = format!("http://{}/v1", provider.addrs()[0]);
    let provider_running = provider.run();
    let provider_handle = provider_running.handle();
    actix_web::rt::spawn(provider_running);
    // No worker_bin/worker_args override: both host and worker are this Cargo
    // fixture's actual compiled artifact, using the production current_exe path.
    std::fs::write(data.join("config.json"), serde_json::to_vec(&json!({"provider":"openai",
        "features":{"provider_model_ref":true},"providers":{"openai":{"api_key":"fixture-key","base_url":provider_url,"model":"root-packet-test"}},
        "defaults":{"chat":{"provider":"openai","model":"root-packet-test"}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1}})).unwrap()).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let log = std::fs::File::create(data.join("host.log")).unwrap();
    let mut host = Host(Command::new(env!("CARGO_BIN_EXE_bamboo")).args(["serve","--bind","127.0.0.1","--port",&port.to_string(),"--data-dir"])
        .arg(&data).current_dir(&data).env("HOME",data.join("home")).env("BAMBOO_JIANDU_DATA_DIR",data.join("jiandu"))
        .env("RUST_LOG","warn").stdout(Stdio::from(log.try_clone().unwrap())).stderr(Stdio::from(log)).spawn().unwrap());
    let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(15)).build().unwrap();
    let base = format!("http://127.0.0.1:{port}/api/v1");
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(host.0.try_wait().unwrap().is_none(), "real host exited: {}",std::fs::read_to_string(data.join("host.log")).unwrap());
            if client.get(format!("{base}/health")).send().await.is_ok_and(|response| response.status().is_success()) { break; }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.unwrap();
    let created = client.post(format!("{base}/chat")).json(&json!({"session_id":"packet-root","message":"ROOT_COMPLETE_INSTRUCTION 🪷",
        "model":"root-packet-test","provider":"openai","model_ref":{"provider":"openai","model":"root-packet-test"},
        "permission_mode":"bypass","workspace_path":data})).send().await.unwrap();
    assert!(created.status().is_success(),"chat: {}",created.text().await.unwrap());
    let mut parent = store.load_session("packet-root").await.unwrap().unwrap();
    parent.messages.iter_mut().find(|message| message.role == Role::User).unwrap().id = "source-user".into();
    for index in 0..10 { let mut message = Message::assistant(format!("bounded background {index}"),None); message.id=format!("background-{index}"); parent.add_message(message); }
    parent.token_budget = Some(TokenBudget::with_safety_margin(64_000, 1024, BudgetStrategy::Hybrid, 0));
    parent.updated_at = chrono::Utc::now();
    store.save_session(&parent).await.unwrap();
    let started = client.post(format!("{base}/execute/packet-root")).json(&json!({})).send().await.unwrap();
    assert!(started.status().is_success());
    let completed = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let parent = store.load_session("packet-root").await.unwrap().unwrap();
            if parent.messages.iter().any(|message| message.content.contains("REAL_ROOT_PACKET_FINISHED")) { break parent; }
            if parent.last_run_status() == Some("error") { panic!("Root failed: {:?}\n{}",parent.last_run_error(),std::fs::read_to_string(data.join("host.log")).unwrap()); }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.expect("real host must finish bounded child workflow");
    let child_entries: Vec<_> = store.list_index_entries().await.into_iter().filter(|entry| entry.parent_session_id.as_deref()==Some("packet-root")).collect();
    let requests = probe.requests.lock().unwrap().clone();
    let child_requests: Vec<_> = requests.iter().filter(|request| request["model"]=="child-packet-test").collect();
    if case == Case::Overflow {
        assert!(child_entries.is_empty());
        assert!(child_requests.is_empty());
        assert!(completed.messages.iter().any(|message| message.content.contains("context_budget_exceeded")));
    } else {
        assert_eq!(child_entries.len(),1);
        let reopened = SessionStoreV2::new(data.clone()).await.unwrap();
        let child = reopened.load_session(&child_entries[0].id).await.unwrap().unwrap();
        let binding = ChildContextBinding::from_session(&child).unwrap().unwrap();
        assert_eq!(binding.payload.parent_created_at,parent.created_at);
        assert_eq!(binding.payload.background.len(),8);
        assert_eq!(binding.payload.background_omitted,2);
        assert_eq!(binding.payload.token_budget.as_ref().unwrap().max_context_tokens,if case==Case::TinyBudget {128} else {64_000});
        assert_eq!(child.root_thinking_mode(),bamboo_domain::RootThinkingMode::Standard);
        assert_eq!(child.agent_runtime_state.as_ref().unwrap().read_only,false);
        if case == Case::Complete {
            assert_eq!(child.last_run_status(),Some("completed"));
            assert_eq!(child_requests.len(),1);
            assert!(child_requests[0]["messages"].as_array().unwrap().iter().any(|message| message["role"]=="user" && message["content"]==binding.payload.required_assignment));
            assert!(binding.payload.required_assignment.contains("ROOT_COMPLETE_INSTRUCTION 🪷"));
            assert!(child.messages.iter().any(|message| message.content.contains("REAL_CHILD_PACKET_EVIDENCE")));
        } else {
            assert!(child_requests.is_empty(),"budget failure must precede any child provider call");
            assert_eq!(child.last_run_status(),Some("error"));
            assert!(child.last_run_error().unwrap().contains("context_budget_exceeded"));
        }
    }
    provider_handle.stop(false).await;
}

#[test]
fn real_cli_required_packet_success_and_preprovider_failure_boundaries() {
    std::thread::Builder::new().stack_size(32*1024*1024).spawn(|| actix_web::rt::System::new().block_on(async {
        for case in [Case::Complete,Case::TinyBudget,Case::HugeGuidance,Case::Overflow] { fixture(case).await; }
    })).unwrap().join().unwrap();
}
