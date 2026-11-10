//! Ultra uses the production Root runner, SubAgent and a real BambooRuntime
//! worker. Only the remote model is replaced by a loopback SSE provider.
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex,
};
use std::time::Duration;

use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_domain::{ReasoningEffort, RootThinkingMode};
use serde_json::{json, Value};
use tokio::sync::Notify;

#[path = "support/read_only_child_host.rs"]
mod read_only_child_host;
use read_only_child_host::{reported_result, Host};

const REQUIRED: &str = "ULTRA_REQUIRED: inspect only; preserve the user constraints";

#[derive(Default)]
struct ProviderProbe {
    requests: Mutex<Vec<Value>>,
    root_calls: AtomicUsize,
    release_child: AtomicBool,
    child_ready: Notify,
    forbidden_path: Mutex<std::path::PathBuf>,
}

fn chunk(delta: Value, finish_reason: Option<&str>) -> String {
    format!(
        "data: {}\n\n",
        json!({
            "id": "ultra-test-response", "object": "chat.completion.chunk",
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
            reported_result("PLAN_PROVIDER_EVIDENCE: verified the read-only scope"),
            "stop",
        )
    } else if rendered.contains("Ultra Root thinking mode") {
        match probe.root_calls.fetch_add(1, Ordering::SeqCst) {
            0 => (
                json!({"tool_calls":[{"index":0,"id":"ultra-denied-write","type":"function","function":{"name":"Write","arguments":json!({"file_path":probe.forbidden_path.lock().unwrap().clone(),"content":"must not write"}).to_string()}}]}),
                "tool_calls",
            ),
            1 => (
                json!({"tool_calls":[{"index":0,"id":"ultra-plan","type":"function","function":{"name":"SubAgent","arguments":json!({"role":"explorer","message":REQUIRED}).to_string()}}]}),
                "tool_calls",
            ),
            2 => (
                json!({"tool_calls":[{"index":0,"id":"ultra-plan-wait","type":"function","function":{"name":"SubAgent","arguments":json!({"intent":"wait"}).to_string()}}]}),
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
fn ultra_root_delegates_to_a_real_read_only_provider_worker_and_resumes() {
    // Run the loopback provider fixture on its own Actix system thread.
    std::thread::Builder::new()
        .name("ultra-root-fixture".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| actix_web::rt::System::new().block_on(run_ultra_fixture()))
        .unwrap()
        .join()
        .unwrap();
}

async fn run_ultra_fixture() {
    let home = tempfile::tempdir().unwrap();
    let home_path = home.path().canonicalize().unwrap();
    let probe = web::Data::new(ProviderProbe::default());
    let forbidden_path = home_path.join("root-must-not-write.txt");
    *probe.forbidden_path.lock().unwrap() = forbidden_path.clone();
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
        "defaults":{"chat":{"provider":"openai","model":"root-test","reasoning_effort":"high"},
                    "subagent_models":{"explorer":{"provider":"openai","model":"planner-test"}}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime", "fabric_dir":home_path.join("fabric"), "max_concurrent":1}
    });
    std::fs::write(
        home_path.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let host = Host::start(&home_path).await;
    let root_id = "ultra-provider-root";
    let response = host.post("chat", json!({
        "session_id":root_id,"message":REQUIRED,"model":"root-test","provider":"openai",
        "model_ref":{"provider":"openai","model":"root-test"},
        "thinking_mode":"ultra","permission_mode":"bypass","reasoning_effort":"high","workspace_path":home_path
    })).await;
    assert_eq!(response.status().as_u16(), 201);
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
    assert_eq!(waiting_root.reasoning_effort, Some(ReasoningEffort::High));
    assert!(
        waiting_root.messages.iter().any(|message| message
            .content
            .contains("not callable at the current conversation position")
            && message.content.contains("Write")),
        "real dispatch must block direct Root implementation"
    );
    assert!(!forbidden_path.exists());
    let child = host.session(&child_id).await;
    assert!(child.agent_runtime_state.as_ref().unwrap().read_only);
    assert_eq!(child.root_thinking_mode(), RootThinkingMode::Standard);
    assert_eq!(child.reasoning_effort, None);
    assert_eq!(
        child.metadata.get("external.protocol").map(String::as_str),
        Some("actor")
    );
    assert_eq!(
        probe.root_calls.load(Ordering::SeqCst),
        3,
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
        .any(|message| message.content.contains("PLAN_PROVIDER_EVIDENCE")));
    let child = host.session(&child_id).await;
    assert!(child
        .messages
        .iter()
        .any(|message| message.content.contains("PLAN_PROVIDER_EVIDENCE")));
    let requests = probe.requests.lock().unwrap().clone();
    let root_requests: Vec<_> = requests
        .iter()
        .filter(|request| request.to_string().contains("Ultra Root thinking mode"))
        .collect();
    assert!(root_requests.len() >= 4);
    for request in root_requests {
        assert_eq!(request["reasoning_effort"], "high");
        assert!(request.to_string().contains(REQUIRED));
    }
    let child_request = requests
        .iter()
        .find(|request| request["model"] == "planner-test")
        .unwrap();
    assert!(
        child_request.get("reasoning_effort").is_none(),
        "planner uses its own ordinary provider default, not the Root's High override"
    );
    assert!(child_request.to_string().contains(REQUIRED));
    assert!(!child_request
        .to_string()
        .contains("Ultra Root thinking mode"));
    assert!(requests
        .iter()
        .all(|request| request["reasoning_effort"] != "ultra"
            && request.get("thinking_mode").is_none()));
    server_handle.stop(false).await;
}
