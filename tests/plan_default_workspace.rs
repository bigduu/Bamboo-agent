//! Metadata-only first chat reaches Plan without an explicit workspace.
//! A real compiled worker runs pathless Glob and sends its result to the provider.
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex,
};
use std::time::Duration;

use actix_web::{test as actix_test, web, App, HttpResponse, HttpServer};
use bamboo_domain::RootThinkingMode;
use bamboo_server::app_state::{AppState, MemoryStore};
use serde_json::{json, Value};
use tokio::sync::Notify;

const MARKER: &str = "plan-default-1347.marker";
const REQUIRED: &str = "PLAN_WORKSPACE_REQUIRED: inspect only; preserve the user constraints";

#[derive(Default)]
struct ProviderProbe {
    requests: Mutex<Vec<Value>>,
    root_calls: AtomicUsize,
    planner_calls: AtomicUsize,
    workspace_tool_observed: AtomicBool,
    release_child: AtomicBool,
    child_ready: Notify,
    workspace: Mutex<std::path::PathBuf>,
}

fn chunk(delta: Value, finish_reason: Option<&str>) -> String {
    format!(
        "data: {}\n\n",
        json!({
            "id": "plan-workspace-response", "object": "chat.completion.chunk",
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
        if probe.planner_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            (
                json!({"tool_calls":[{"index":0,"id":"workspace-glob","type":"function","function":{"name":"Glob","arguments":json!({"pattern":MARKER,"limit":1}).to_string()}}]}),
                "tool_calls",
            )
        } else {
            // The argument names only a file: full-path Tool evidence proves
            // the worker used the selected workspace, not its process cwd.
            let marker = probe.workspace.lock().unwrap().join(MARKER);
            let observed = body["messages"].as_array().is_some_and(|messages| {
                messages.iter().any(|message| {
                    message["role"] == "tool"
                        && message["content"]
                            .as_str()
                            .is_some_and(|content| content.contains(marker.to_str().unwrap()))
                })
            });
            probe
                .workspace_tool_observed
                .store(observed, Ordering::SeqCst);
            (
                json!({"content":"PLAN_WORKSPACE_VERIFIED: completed read-only workspace inspection"}),
                "stop",
            )
        }
    } else if rendered.contains("Ultra Root thinking mode") {
        match probe.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => (
                json!({"tool_calls":[{"index":0,"id":"default-workspace-plan","type":"function","function":{"name":"Plan","arguments":json!({"task":REQUIRED,"title":"Inspect the default workspace"}).to_string()}}]}),
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
fn first_chat_metadata_workspace_reaches_plan_and_actual_worker_glob() {
    // The full debug AppState/route fixture exceeds libtest's default stack.
    // Increase only the host test thread; the real child has its normal stack.
    std::thread::Builder::new()
        .name("plan-workspace-fixture".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| actix_web::rt::System::new().block_on(run_workspace_fixture()))
        .unwrap()
        .join()
        .unwrap();
}

async fn run_workspace_fixture() {
    let home = tempfile::tempdir().unwrap();
    let home_path = home.path().canonicalize().unwrap();
    let workspace_path = home_path.join("user-workspace");
    std::fs::create_dir(&workspace_path).unwrap();
    std::fs::write(workspace_path.join(MARKER), "bounded read-only marker").unwrap();
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
    *probe.workspace.lock().unwrap() = workspace_path.clone();
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
        "defaults":{"chat":{"provider":"openai","model":"root-test"},
                    "subagent_models":{"planner":{"provider":"openai","model":"planner-test"}}},
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
    let root_id = "plan-default-workspace-root";
    let response = actix_test::call_service(
        &app,
        actix_test::TestRequest::post()
            .uri("/api/v1/chat")
            .set_json(json!({
                "session_id":root_id,"message":REQUIRED,"model":"root-test","provider":"openai",
                "model_ref":{"provider":"openai","model":"root-test"},
                "thinking_mode":"ultra","permission_mode":"bypass","workspace_path":workspace_path
            }))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), actix_web::http::StatusCode::CREATED);
    let initial = state.storage.load_session(root_id).await.unwrap().unwrap();
    assert!(
        initial.workspace.is_none(),
        "fixture must exercise the old metadata-only gap"
    );
    assert_eq!(
        initial.workspace_path_meta().as_deref(),
        workspace_path.to_str()
    );
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
    let child = state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert!(child.agent_runtime_state.as_ref().unwrap().read_only);
    assert_eq!(child.workspace.as_deref(), workspace_path.to_str());
    assert_eq!(
        child.workspace_path_meta().as_deref(),
        workspace_path.to_str()
    );
    assert_eq!(child.root_thinking_mode(), RootThinkingMode::Standard);
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
        .any(|message| message.content.contains("PLAN_WORKSPACE_VERIFIED")));
    let child = state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert!(child
        .messages
        .iter()
        .any(|message| message.content.contains("PLAN_WORKSPACE_VERIFIED")));
    let requests = probe.requests.lock().unwrap().clone();
    let child_request = requests
        .iter()
        .find(|request| request["model"] == "planner-test")
        .unwrap();
    assert!(child_request.to_string().contains(REQUIRED));
    assert!(!child_request
        .to_string()
        .contains("Ultra Root thinking mode"));
    assert_eq!(probe.planner_calls.load(Ordering::SeqCst), 2);
    assert!(
        probe.workspace_tool_observed.load(Ordering::SeqCst),
        "the second actual planner request must contain pathless Glob's full-path Tool result"
    );
    server_handle.stop(false).await;
}
