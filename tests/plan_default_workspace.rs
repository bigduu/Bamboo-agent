//! A read-only Child inherits the first chat’s metadata-only workspace.
//! A real compiled worker runs pathless Glob and sends its result to the provider.
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex,
};
use std::time::Duration;

use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_domain::RootThinkingMode;
use serde_json::{json, Value};
use tokio::sync::Notify;

#[path = "support/read_only_child_host.rs"]
mod read_only_child_host;
use read_only_child_host::{reported_result, Host};

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
                reported_result(
                    "PLAN_WORKSPACE_VERIFIED: completed read-only workspace inspection",
                ),
                "stop",
            )
        }
    } else if rendered.contains("Ultra Root thinking mode") {
        match probe.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => (
                json!({"tool_calls":[{"index":0,"id":"default-workspace-plan","type":"function","function":{"name":"SubAgent","arguments":json!({"role":"explorer","message":REQUIRED}).to_string()}}]}),
                "tool_calls",
            ),
            1 => (
                json!({"tool_calls":[{"index":0,"id":"default-workspace-plan-wait","type":"function","function":{"name":"SubAgent","arguments":json!({"intent":"wait"}).to_string()}}]}),
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
fn first_chat_metadata_workspace_reaches_explorer_and_actual_worker_glob() {
    // Run the loopback provider fixture on its own Actix system thread.
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
        "providers":{"openai":{"api_key":"fixture-key","base_url":base_url,"model":"root-test", "runtime_models":["root-test","planner-test"]}},
        "defaults":{"chat":{"provider":"openai","model":"root-test"},
                    "subagent_models":{"explorer":{"provider":"openai","model":"planner-test"}}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime", "fabric_dir":home_path.join("fabric"), "max_concurrent":1}
    });
    std::fs::write(
        home_path.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let host = Host::start(&home_path).await;
    let root_id = "plan-default-workspace-root";
    let response = host
        .post(
            "chat",
            json!({
                "session_id":root_id,"message":REQUIRED,"model":"root-test","provider":"openai",
                "model_ref":{"provider":"openai","model":"root-test"},
                "thinking_mode":"ultra","permission_mode":"bypass","workspace_path":workspace_path
            }),
        )
        .await;
    assert_eq!(response.status().as_u16(), 201);
    let initial = host.session(root_id).await;
    assert!(
        initial.workspace.is_none(),
        "fixture must exercise the old metadata-only gap"
    );
    assert_eq!(
        initial.workspace_path_meta().as_deref(),
        workspace_path.to_str()
    );
    let response = host.post(&format!("execute/{root_id}"), json!({})).await;
    assert!(response.status().is_success(), "execute must start");
    let (waiting_root, child_id) = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let root = host.session(root_id).await;
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
    .expect("the Root must wait for the real provider worker");
    assert_eq!(waiting_root.root_thinking_mode(), RootThinkingMode::Ultra);
    let child = host.session(&child_id).await;
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
        2,
        "Root has not synthesized an unfinished child"
    );
    probe.release_child.store(true, Ordering::SeqCst);
    probe.child_ready.notify_waiters();
    let completed = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let root = host.session(root_id).await;
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
    let child = host.session(&child_id).await;
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
