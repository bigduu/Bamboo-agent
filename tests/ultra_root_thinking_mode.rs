//! Ultra uses the production Root runner, SubAgent and real BambooRuntime
//! workers. Only the remote model is replaced by a loopback SSE provider.
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

const REQUIRED: &str =
    "ULTRA_REQUIRED: update the Root output and delegate independent read-only verification";
const CHECKS: [&str; 3] = ["CHECK_INPUTS", "CHECK_SCOPE", "CHECK_ACCEPTANCE"];
const EVIDENCE_PREFIX: &str = "PLAN_PROVIDER_EVIDENCE: ";

#[derive(Default)]
struct ProviderProbe {
    requests: Mutex<Vec<Value>>,
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    child_started: Notify,
    release_children: AtomicBool,
    children_released: Notify,
    root_output_path: Mutex<std::path::PathBuf>,
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

fn tool_response(id: &str, name: &str, arguments: Value) -> (Value, &'static str) {
    (
        json!({"tool_calls": [{
            "index": 0,
            "id": id,
            "type": "function",
            "function": {"name": name, "arguments": arguments.to_string()}
        }]}),
        "tool_calls",
    )
}

async fn model_response(body: web::Json<Value>, probe: web::Data<ProviderProbe>) -> HttpResponse {
    let body = body.into_inner();
    let rendered = body.to_string();
    probe.requests.lock().unwrap().push(body.clone());
    let (delta, finish) = if body["model"] == "planner-test" {
        let check = CHECKS
            .iter()
            .find(|check| rendered.contains(**check))
            .expect("planner receives a distinct assigned check");
        probe.child_calls.fetch_add(1, Ordering::SeqCst);
        probe.child_started.notify_waiters();
        // Hold each real worker in inference until the Root has done its work
        // and the test observes its durable wait.
        loop {
            let wake = probe.children_released.notified();
            if probe.release_children.load(Ordering::SeqCst) {
                break;
            }
            wake.await;
        }
        (
            reported_result(&format!("{EVIDENCE_PREFIX}{check}")),
            "stop",
        )
    } else if rendered.contains("Ultra Root thinking mode") {
        let round = probe.root_calls.fetch_add(1, Ordering::SeqCst);
        match round {
            0..=2 => tool_response(
                &format!("ultra-plan-{round}"),
                "SubAgent",
                json!({"role": "explorer", "message": format!("{}\n{REQUIRED}", CHECKS[round])}),
            ),
            3 => {
                loop {
                    let started = probe.child_started.notified();
                    if probe.child_calls.load(Ordering::SeqCst) >= CHECKS.len() {
                        break;
                    }
                    started.await;
                }
                tool_response(
                    "ultra-write",
                    "Write",
                    json!({"file_path": probe.root_output_path.lock().unwrap().clone(), "content": "root draft"}),
                )
            }
            4 => tool_response(
                "ultra-edit",
                "Edit",
                json!({"file_path": probe.root_output_path.lock().unwrap().clone(), "old_string": "root draft", "new_string": "root verified"}),
            ),
            _ if CHECKS
                .iter()
                .any(|check| !rendered.contains(&format!("{EVIDENCE_PREFIX}{check}"))) =>
            {
                tool_response(
                    &format!("ultra-plan-wait-{round}"),
                    "SubAgent",
                    json!({"intent": "wait"}),
                )
            }
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
fn ultra_root_works_while_parallel_read_only_children_run_and_resumes() {
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
    let root_output_path = home_path.join("root-output.txt");
    *probe.root_output_path.lock().unwrap() = root_output_path.clone();
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
        "subagents":{"runtime":"actor","executor":"bamboo_runtime", "fabric_dir":home_path.join("fabric"), "max_concurrent":CHECKS.len()}
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
    let (waiting_root, child_ids) = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let root = host.session(root_id).await;
            let wait = root
                .agent_runtime_state
                .as_ref()
                .and_then(|runtime| runtime.waiting_for_children.as_ref());
            if let Some(wait) = wait {
                let child_ids = wait.child_session_ids.clone();
                break (root, child_ids);
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
                    "Root finished before waiting for the children; tool_results={tool_results:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the Root must wait after running its own tools alongside three provider workers");
    assert_eq!(waiting_root.root_thinking_mode(), RootThinkingMode::Ultra);
    assert_eq!(waiting_root.reasoning_effort, Some(ReasoningEffort::High));
    assert_eq!(child_ids.len(), CHECKS.len());
    assert_eq!(probe.child_calls.load(Ordering::SeqCst), CHECKS.len());
    for call_id in ["ultra-write", "ultra-edit"] {
        assert!(
            waiting_root.messages.iter().any(|message| {
                message.tool_call_id.as_deref() == Some(call_id)
                    && message.tool_success == Some(true)
            }),
            "authorized Ultra call {call_id} must succeed in real dispatch"
        );
    }
    assert_eq!(
        std::fs::read_to_string(&root_output_path).unwrap(),
        "root verified"
    );
    for child_id in &child_ids {
        let child = host.session(child_id).await;
        assert!(child.agent_runtime_state.as_ref().unwrap().read_only);
        assert_eq!(child.root_thinking_mode(), RootThinkingMode::Standard);
        assert_eq!(child.reasoning_effort, None);
        assert_eq!(
            child.metadata.get("external.protocol").map(String::as_str),
            Some("actor")
        );
    }
    assert_eq!(
        probe.root_calls.load(Ordering::SeqCst),
        6,
        "Root has dispatched three children, written, edited and requested one wait"
    );
    probe.release_children.store(true, Ordering::SeqCst);
    probe.children_released.notify_waiters();
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
    .expect("Root must resume and synthesize all actual child evidence");
    let mut child_evidence = Vec::new();
    for child_id in &child_ids {
        let child = host.session(child_id).await;
        let report = child
            .messages
            .into_iter()
            .find(|message| message.content.contains(EVIDENCE_PREFIX))
            .expect("each Child persisted a provider report");
        child_evidence.push(report.content);
    }
    for check in CHECKS {
        let evidence = format!("{EVIDENCE_PREFIX}{check}");
        assert!(completed
            .messages
            .iter()
            .any(|message| message.content.contains(&evidence)));
        assert!(child_evidence
            .iter()
            .any(|report| report.contains(&evidence)));
    }
    let requests = probe.requests.lock().unwrap().clone();
    let root_requests: Vec<_> = requests
        .iter()
        .filter(|request| request.to_string().contains("Ultra Root thinking mode"))
        .collect();
    assert!(root_requests.len() >= 7);
    for request in root_requests {
        assert_eq!(request["reasoning_effort"], "high");
        assert!(request.to_string().contains(REQUIRED));
        assert!(request
            .to_string()
            .contains("Launch ready independent SubAgent assignments in parallel"));
    }
    let child_requests: Vec<_> = requests
        .iter()
        .filter(|request| request["model"] == "planner-test")
        .collect();
    assert_eq!(child_requests.len(), CHECKS.len());
    for request in child_requests {
        assert!(
            request.get("reasoning_effort").is_none(),
            "planner uses its own provider default"
        );
        assert!(request.to_string().contains(REQUIRED));
        assert!(!request.to_string().contains("Ultra Root thinking mode"));
    }
    assert!(requests
        .iter()
        .all(|request| request["reasoning_effort"] != "ultra"
            && request.get("thinking_mode").is_none()));
    server_handle.stop(false).await;
}
