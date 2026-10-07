//! Ordinary Child effort crosses the production actor adapter and real worker.
//! Reused compiled workers are tested against actual loopback provider requests.
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex,
};
use std::time::Duration;

use actix_web::{test as actix_test, web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::{ReasoningEffort, RootThinkingMode};
use bamboo_server::app_state::{AppState, MemoryStore};
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use tokio::sync::Notify;

const REQUIRED: &str = "CHILD_EFFORT_REQUIRED: inspect only; preserve the user constraints";

#[derive(Default)]
struct ProviderProbe {
    requests: Mutex<Vec<Value>>,
    root_calls: AtomicUsize,
    release_child: AtomicBool,
    child_ready: Notify,
    workspace: Mutex<std::path::PathBuf>,
}

fn chunk(delta: Value, finish_reason: Option<&str>) -> String {
    format!(
        "data: {}\n\n",
        json!({
            "id": "ordinary-effort-response", "object": "chat.completion.chunk",
            "choices": [{"index":0, "delta": delta, "finish_reason":finish_reason}]
        })
    )
}

async fn model_response(body: web::Json<Value>, probe: web::Data<ProviderProbe>) -> HttpResponse {
    let body = body.into_inner();
    let rendered = body.to_string();
    probe.requests.lock().unwrap().push(body.clone());
    let (delta, finish) = if body["model"] == "planner-test" {
        // Hold the actual worker's provider call until the test observes the
        // durable parent wait. No runtime is allowed to invent child success.
        loop {
            let wake = probe.child_ready.notified();
            if probe.release_child.load(Ordering::SeqCst) {
                break;
            }
            wake.await;
        }
        (
            json!({"content":"PLAN_PROVIDER_EVIDENCE: verified the read-only scope"}),
            "stop",
        )
    } else if rendered.contains("Ultra Root thinking mode") {
        match probe.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => (
                json!({"tool_calls":[{"index":0,"id":"ordinary-plan","type":"function","function":{"name":"Plan","arguments":json!({"task":REQUIRED,"title":"Bounded Child effort plan","workspace":probe.workspace.lock().unwrap().clone()}).to_string()}}]}),
                "tool_calls",
            ),
            _ => (
                json!({"content":"ROOT_VERIFIED_PLAN: synthesized the delegated provider evidence"}),
                "stop",
            ),
        }
    } else {
        // Auxiliary/title requests have no Root execution authority.
        (json!({"content":"bounded auxiliary response"}), "stop")
    };
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("{}data: [DONE]\n\n", chunk(delta, Some(finish))))
}

#[test]
fn child_owned_effort_reaches_host_and_reused_compiled_worker_provider_wire() {
    // The full debug AppState/route fixture exceeds libtest's default stack.
    // Increase only the host test thread; the real child has its normal stack.
    std::thread::Builder::new()
        .name("child-effort-fixture".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| actix_web::rt::System::new().block_on(run_effort_fixture()))
        .unwrap()
        .join()
        .unwrap();
}

async fn run_effort_fixture() {
    let home = tempfile::tempdir().unwrap();
    let home_path = home.path().canonicalize().unwrap();
    let jiandu = home_path.join("jiandu");
    // This standalone test binary has one test. Scope the production worker's
    // independent Jiandu store to the same disposable fixture as the host.
    let old_jiandu = std::env::var_os("BAMBOO_JIANDU_DATA_DIR");
    struct RestoreJiandu(Option<std::ffi::OsString>);
    impl Drop for RestoreJiandu {
        fn drop(&mut self) {
            match &self.0 {
                Some(value) => std::env::set_var("BAMBOO_JIANDU_DATA_DIR", value),
                None => std::env::remove_var("BAMBOO_JIANDU_DATA_DIR"),
            }
        }
    }
    let _restore_jiandu = RestoreJiandu(old_jiandu);
    std::env::set_var("BAMBOO_JIANDU_DATA_DIR", &jiandu);
    let probe = web::Data::new(ProviderProbe::default());
    *probe.workspace.lock().unwrap() = home_path.clone();
    let server_probe = probe.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(server_probe.clone())
            .route("/v1/chat/completions", web::post().to(model_response))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok()
                        .json(json!({"data":[{"id":"root-test"},{"id":"planner-test"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let base_url = format!("http://{}/v1", server.addrs()[0]);
    let running = server.run();
    let server_handle = running.handle();
    actix_web::rt::spawn(running);
    let config = json!({
        "provider":"openai",
        "features":{"provider_model_ref":true},
        "providers":{"openai":{"api_key":"fixture-key","base_url":base_url,"model":"root-test"}},
        "defaults":{"chat":{"provider":"openai","model":"root-test","reasoning_effort":"high"},
                    "subagent_models":{"planner":{"provider":"openai","model":"planner-test","reasoning_effort":"low"}}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime", "worker_bin":env!("CARGO_BIN_EXE_bamboo"), "worker_args":["subagent-worker"], "fabric_dir":home_path.join("fabric"), "max_concurrent":1}
    });
    std::fs::write(
        home_path.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let state = web::Data::new(
        AppState::new_with_memory_store(home_path.clone(), MemoryStore::new(jiandu))
            .await
            .unwrap(),
    );
    {
        let live = state.config.read().await;
        let planner = &live.defaults.as_ref().unwrap().subagent_models["planner"];
        assert_eq!(planner.model, "planner-test");
        assert_eq!(planner.reasoning_effort, Some(ReasoningEffort::Low));
        assert_eq!(
            live.subagents().worker_bin.as_deref(),
            Some(env!("CARGO_BIN_EXE_bamboo"))
        );
    }
    let app = actix_test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::routes::configure_routes),
    )
    .await;
    let root_id = "child-effort-root";
    let response = actix_test::call_service(&app, actix_test::TestRequest::post().uri("/api/v1/chat").set_json(json!({
        "session_id":root_id,"message":REQUIRED,"model":"root-test","provider":"openai",
        "model_ref":{"provider":"openai","model":"root-test"},
        "thinking_mode":"ultra","reasoning_effort":"high","permission_mode":"bypass","workspace_path":home_path
    })).to_request()).await;
    assert_eq!(response.status(), actix_web::http::StatusCode::CREATED);
    let response = actix_test::call_service(
        &app,
        actix_test::TestRequest::post()
            .uri(&format!("/api/v1/execute/{root_id}"))
            .set_json(json!({}))
            .to_request(),
    )
    .await;
    assert!(response.status().is_success(), "execute must start");
    let (waiting_root, child_id) = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let root = state.storage.load_session(root_id).await.unwrap().unwrap();
            let wait = root
                .agent_runtime_state
                .as_ref()
                .and_then(|runtime| runtime.waiting_for_children.as_ref());
            let child_called_provider = probe
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|request| request["model"] == "planner-test");
            if child_called_provider {
                if let Some(wait) = wait {
                    let child_id = wait.child_session_ids[0].clone();
                    break (root, child_id);
                }
            }
            if root
                .messages
                .iter()
                .any(|message| message.content.contains("ROOT_VERIFIED_PLAN"))
            {
                let tool_results: Vec<_> = root
                    .messages
                    .iter()
                    .filter(|message| message.role == bamboo_domain::Role::Tool)
                    .take(4)
                    .map(|message| message.content.chars().take(512).collect::<String>())
                    .collect();
                panic!(
                    "Root finished before a real planner request; tool_results={tool_results:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("real Plan must wait for the real provider worker");
    assert_eq!(waiting_root.root_thinking_mode(), RootThinkingMode::Ultra);
    assert_eq!(waiting_root.reasoning_effort, Some(ReasoningEffort::High));
    let child = state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert!(child.agent_runtime_state.as_ref().unwrap().read_only);
    assert_eq!(child.root_thinking_mode(), RootThinkingMode::Standard);
    assert_eq!(child.reasoning_effort, Some(ReasoningEffort::Low));
    assert_eq!(
        child.metadata.get("external.protocol").map(String::as_str),
        Some("actor")
    );
    assert_eq!(
        probe.root_calls.load(Ordering::SeqCst),
        1,
        "Root has not synthesized an unfinished child"
    );
    probe.release_child.store(true, Ordering::SeqCst);
    probe.child_ready.notify_waiters();
    let completed = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let root = state.storage.load_session(root_id).await.unwrap().unwrap();
            if root
                .messages
                .iter()
                .any(|message| message.content.contains("ROOT_VERIFIED_PLAN"))
            {
                break root;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("Root must resume and synthesize actual child evidence");
    assert!(completed
        .messages
        .iter()
        .any(|message| message.content.contains("PLAN_PROVIDER_EVIDENCE")));
    let child = state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert!(child
        .messages
        .iter()
        .any(|message| message.content.contains("PLAN_PROVIDER_EVIDENCE")));
    let requests = probe.requests.lock().unwrap().clone();
    let root_requests: Vec<_> = requests
        .iter()
        .filter(|request| request.to_string().contains("Ultra Root thinking mode"))
        .collect();
    assert!(root_requests.len() >= 2);
    for request in root_requests {
        assert_eq!(request["reasoning_effort"], "high");
        assert!(request.to_string().contains(REQUIRED));
    }
    let child_request = requests
        .iter()
        .find(|request| request["model"] == "planner-test")
        .unwrap();
    assert_eq!(
        child_request["reasoning_effort"], "low",
        "own planner preference must reach the wire instead of Root High"
    );
    assert!(child_request.to_string().contains(REQUIRED));
    assert!(!child_request
        .to_string()
        .contains("Ultra Root thinking mode"));
    assert!(requests
        .iter()
        .all(|request| request["reasoning_effort"] != "ultra"
            && request.get("thinking_mode").is_none()));
    verify_reused_worker(&home_path, &base_url, &probe).await;
    server_handle.stop(false).await;
}

async fn verify_reused_worker(home: &std::path::Path, base_url: &str, probe: &ProviderProbe) {
    use bamboo_subagent::fleet::spawn_worker;
    use bamboo_subagent::proto::{
        ChildFrame, LogicalSessionIdentity, ParentFrame, RunSpec, TerminalStatus,
    };
    use bamboo_subagent::provision::{
        ChildIdentity, ExecutorSpec, ModelRefSpec, ProvisionSpec, ScopedCredential,
    };
    use bamboo_subagent::transport::ChildClient;

    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: "ordinary-warm-worker".into(),
            parent_id: Some("independent-parent".into()),
            project_key: None,
            role: "worker".into(),
            depth: 4,
        },
        ExecutorSpec::BambooRuntime,
        home.join("warm-fabric").to_string_lossy().into_owned(),
    );
    spec.reusable = true;
    spec.workspace = Some(home.to_string_lossy().into_owned());
    spec.storage_dir = Some(home.join("warm-store").to_string_lossy().into_owned());
    spec.model = Some(ModelRefSpec {
        provider: "openai".into(),
        model: "worker-test".into(),
    });
    spec.secrets.provider_credentials.push(ScopedCredential {
        provider: "openai".into(),
        api_key: "fixture-key".into(),
        base_url: Some(base_url.into()),
        provider_type: None,
        credential_ref: None,
    });
    let mut spawned = spawn_worker(
        std::path::Path::new(env!("CARGO_BIN_EXE_bamboo")),
        &["subagent-worker".into()],
        &spec,
        Duration::from_secs(25),
    )
    .await
    .expect("same-source real Bamboo runtime worker");
    let worker_pid = spawned.pid().expect("owned worker PID");
    let mut successful_requests = 0;
    for (index, effort) in [
        Some("low"),
        Some("high"),
        Some("none"),
        None,
        Some("ultra"),
        Some("unknown"),
    ]
    .into_iter()
    .enumerate()
    {
        assert!(spawned.is_alive(), "reuse must keep the same live worker");
        assert_eq!(spawned.pid(), Some(worker_pid));
        let before = probe
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["model"] == "worker-test")
            .count();
        let mut client = ChildClient::connect(&spawned.record.endpoint)
            .await
            .unwrap();
        client
            .send(ParentFrame::Run(RunSpec {
                assignment: format!("ORDINARY_WARM_RUN_{index}"),
                logical_session: Some(LogicalSessionIdentity {
                    creation: None,
                    // This is the supported pool reuse path: one live worker
                    // handles successive independent logical Child sessions.
                    session_id: format!("ordinary-warm-session-{index}"),
                    parent_session_id: Some("independent-parent".into()),
                    root_session_id: "independent-parent".into(),
                }),
                project_id: None,
                reasoning_effort: effort.map(str::to_owned),
                permission_policy: None,
                messages: vec![],
                activation_run_id: Some(format!("ordinary-run-{index}")),
                execution_epoch: index as u64 + 1,
                initial_session_messages: vec![],
                secrets: Default::default(),
            }))
            .await
            .unwrap();
        let (status, error) = tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                match client
                    .next_frame()
                    .await
                    .unwrap()
                    .expect("terminal from live real worker")
                {
                    ChildFrame::Terminal { status, error, .. } => break (status, error),
                    ChildFrame::EventBatch { batch } => batch.validate().unwrap(),
                    ChildFrame::ApprovalRequest { .. } => {
                        panic!("ordinary effort cannot request authority")
                    }
                    ChildFrame::OwnedTreeRequest { .. } => {
                        panic!("ordinary effort cannot request a Host tree")
                    }
                    ChildFrame::SubAgentRequest { .. } => {
                        panic!("ordinary effort cannot request a Host SubAgent operation")
                    }
                    ChildFrame::SessionMessageAdmitted { .. } => panic!("no initial deliveries"),
                    ChildFrame::Event { .. } => {}
                }
            }
        })
        .await
        .expect("bounded real worker terminal");
        client.close().await.unwrap();
        let requests = probe.requests.lock().unwrap();
        let requests: Vec<_> = requests
            .iter()
            .filter(|request| request["model"] == "worker-test")
            .collect();
        if index < 4 {
            assert_eq!(status, TerminalStatus::Completed, "{error:?}");
            assert_eq!(
                requests.len(),
                before + 1,
                "one actual execution call per Run"
            );
            let request = requests.last().unwrap();
            match effort {
                Some(effort) => assert_eq!(request["reasoning_effort"], effort),
                None => assert!(
                    request.get("reasoning_effort").is_none(),
                    "omission clears warm override and uses this isolated provider's unset default"
                ),
            }
            assert!(request
                .to_string()
                .contains(&format!("ORDINARY_WARM_RUN_{index}")));
            assert!(request.get("thinking_mode").is_none());
            successful_requests += 1;
        } else {
            assert_eq!(status, TerminalStatus::Error);
            assert_eq!(error.as_deref(), Some("invalid RunSpec reasoning_effort"));
            assert_eq!(
                requests.len(),
                before,
                "invalid effort fails before the provider"
            );
        }
    }
    assert_eq!(successful_requests, 4);
    let store = SessionStoreV2::new(home.join("warm-store"))
        .await
        .expect("independent terminal store handle");
    for index in 0..4 {
        let saved = store
            .load_session(&format!("ordinary-warm-session-{index}"))
            .await
            .unwrap()
            .expect("successful ordinary Child persisted");
        assert!(
            !saved
                .agent_runtime_state
                .as_ref()
                .expect("ordinary terminal persists typed runtime")
                .read_only,
            "ordinary Child cannot acquire read-only posture during startup"
        );
    }
    spawned.kill().await;
}
