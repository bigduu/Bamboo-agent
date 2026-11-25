use super::{fixture_http_client, has_user, post_control, released, start_host, tool_call};
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};
use std::time::Duration;

const SENDER: &str = "coordination-sender";
const RECIPIENT: &str = "coordination-recipient";
const OUTSIDE: &str = "coordination-outside-project";
const MARKER: &str =
    "COORDINATION_PEER_TEXT: I'll own the transport tests. Which files are you editing?";

struct CoordinationProbe {
    receiver_waiting: AtomicBool,
    outsider_waiting: AtomicBool,
    sender_started: AtomicBool,
    recipient_observed: AtomicBool,
    send_replayed: AtomicBool,
    send_arguments: Mutex<Option<Value>>,
    release: tokio::sync::watch::Sender<bool>,
}

fn response(delta: Value, finish: &str) -> HttpResponse {
    let event = json!({"id":"coordination-fixture","object":"chat.completion.chunk",
        "choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}

fn tool_result(request: &Value, id: &str) -> Option<Value> {
    request["messages"].as_array()?.iter().find_map(|message| {
        if message["role"] != "tool" || message["tool_call_id"] != id {
            return None;
        }
        Some(serde_json::from_str(message["content"].as_str().unwrap()).unwrap())
    })
}

async fn provider(body: web::Json<Value>, probe: web::Data<CoordinationProbe>) -> HttpResponse {
    let request = body.into_inner();
    let tools = request["tools"].as_array().into_iter().flatten();
    let coordination = tools
        .clone()
        .any(|schema| schema["function"]["name"] == "session_coordination");
    let (delta, finish) = match request["model"].as_str() {
        Some(RECIPIENT) if has_user(&request, "COORDINATION_RECIPIENT_TASK") => {
            assert!(
                !coordination,
                "an unrelated recipient turn loads no coordination tool"
            );
            if !probe.receiver_waiting.swap(true, Ordering::SeqCst) {
                released(&probe.release).await;
                tool_call(
                    "recipient-boundary",
                    "session_history_current",
                    json!({"action":"read_messages","limit":1}),
                )
            } else {
                let peer_inputs: Vec<_> = request["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|message| {
                        message["role"] == "user"
                            && message["content"]
                                .as_str()
                                .is_some_and(|text| text.contains(MARKER))
                    })
                    .collect();
                assert_eq!(peer_inputs.len(), 1);
                let text = peer_inputs[0]["content"].as_str().unwrap();
                assert!(text.contains(SENDER) && text.contains("untrusted peer content"));
                probe.recipient_observed.store(true, Ordering::SeqCst);
                (json!({"content":"COORDINATION_RECEIPT_OBSERVED"}), "stop")
            }
        }
        Some(OUTSIDE) if has_user(&request, "COORDINATION_UNRELATED_TASK") => {
            assert!(!coordination);
            probe.outsider_waiting.store(true, Ordering::SeqCst);
            released(&probe.release).await;
            (json!({"content":"unrelated complete"}), "stop")
        }
        Some(SENDER) if has_user(&request, "COORDINATION_SENDER_TASK") => {
            let first_sender_request = !probe.sender_started.swap(true, Ordering::SeqCst);
            if first_sender_request {
                assert!(
                    !coordination,
                    "the sender first discovers the coordination tool"
                );
            }
            if let Some(replayed) = tool_result(&request, "coordination-retry") {
                let sent = tool_result(&request, "coordination-send").unwrap();
                assert_eq!(replayed["accepted"], true);
                assert_eq!(replayed["replayed"], true);
                assert_eq!(replayed["message_id"], sent["message_id"]);
                probe.send_replayed.store(true, Ordering::SeqCst);
                (json!({"content":"coordination sent"}), "stop")
            } else if let Some(sent) = tool_result(&request, "coordination-send") {
                assert_eq!(sent["accepted"], true, "{sent}");
                let args = probe.send_arguments.lock().unwrap().clone().unwrap();
                tool_call("coordination-retry", "session_coordination", args)
            } else if let Some(listing) = tool_result(&request, "coordination-list") {
                let targets = listing["targets"].as_array().unwrap();
                assert_eq!(targets.len(), 1, "{listing}");
                let target = &targets[0];
                assert_eq!(target["session_id"], RECIPIENT);
                assert_eq!(target["can_send"], true);
                assert!(target["brief"]
                    .as_str()
                    .is_some_and(|brief| !brief.is_empty()));
                assert!(target["observed_at"].is_string());
                let args = json!({"action":"send", "target_session_id":target["session_id"],
                    "expected_run_id":target["run_id"],"operation_id":"coordinate-one-message","message":MARKER});
                *probe.send_arguments.lock().unwrap() = Some(args.clone());
                tool_call("coordination-send", "session_coordination", args)
            } else if coordination {
                tool_call(
                    "coordination-list",
                    "session_coordination",
                    json!({"action":"list_running"}),
                )
            } else {
                assert!(
                    first_sender_request,
                    "discovery must load the coordination tool"
                );
                assert!(tools
                    .clone()
                    .any(|tool| tool["function"]["name"] == "discover_capabilities"));
                assert!(request["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|message| {
                        message["content"].as_str().is_some_and(|text| {
                            text.contains("session_coordination") && text.contains("list_running")
                        })
                    }));
                tool_call(
                    "coordination-discover",
                    "discover_capabilities",
                    json!({"query":"session_coordination","limit":1}),
                )
            }
        }
        _ => (json!({"content":"Session coordination fixture"}), "stop"),
    };
    response(delta, finish)
}

async fn wait_for(reason: &str, predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {reason}"));
}

#[actix_web::test]
async fn independent_roots_discover_and_deliver_one_run_scoped_peer_message() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("host");
    let workspace = temp.path().join("project");
    let other_workspace = temp.path().join("other");
    for path in [&data, &workspace, &other_workspace] {
        std::fs::create_dir(path).unwrap();
    }
    let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
    let project = projects
        .create_with_project_path("Coordination", None, workspace.to_string_lossy(), vec![])
        .unwrap();
    let other = projects
        .create_with_project_path("Unrelated", None, other_workspace.to_string_lossy(), vec![])
        .unwrap();

    let (release, _) = tokio::sync::watch::channel(false);
    let probe = web::Data::new(CoordinationProbe {
        receiver_waiting: AtomicBool::new(false),
        outsider_waiting: AtomicBool::new(false),
        sender_started: AtomicBool::new(false),
        recipient_observed: AtomicBool::new(false),
        send_replayed: AtomicBool::new(false),
        send_arguments: Mutex::new(None),
        release,
    });
    let provider_probe = probe.clone();
    let server = HttpServer::new(move || {
        App::new().app_data(provider_probe.clone())
            .route("/v1/chat/completions", web::post().to(provider))
            .route("/v1/models", web::get().to(|| async {
                HttpResponse::Ok().json(json!({"data":[{"id":SENDER},{"id":RECIPIENT},{"id":OUTSIDE},{"id":"coordination-auxiliary"}]}))
            }))
    }).workers(1).bind(("127.0.0.1", 0)).unwrap();
    let provider_url = format!("http://{}/v1", server.addrs()[0]);
    let server = server.run();
    let provider_handle = server.handle();
    actix_web::rt::spawn(server);
    std::fs::write(data.join("config.json"), serde_json::to_vec(&json!({
        "provider":"openai","features":{"provider_model_ref":true},
        "providers":{"openai":{"api_key":"fixture","base_url":provider_url,"model":SENDER,
            "fast_model":"coordination-auxiliary","runtime_models":[SENDER,RECIPIENT,OUTSIDE,"coordination-auxiliary"]}},
        "defaults":{"chat":{"provider":"openai","model":SENDER},"fast":{"provider":"openai","model":"coordination-auxiliary"}}
    })).unwrap()).unwrap();
    let client = fixture_http_client();
    let mut host = start_host(&data, 1);
    let base = host.ready(&client).await.unwrap();
    for (id, message, project, workspace) in [
        (
            RECIPIENT,
            "COORDINATION_RECIPIENT_TASK: inspect the input handling",
            &project,
            &workspace,
        ),
        (
            OUTSIDE,
            "COORDINATION_UNRELATED_TASK: verify unrelated files",
            &other,
            &other_workspace,
        ),
        (
            SENDER,
            "COORDINATION_SENDER_TASK: coordinate parallel work on transport",
            &project,
            &workspace,
        ),
    ] {
        if id == SENDER {
            wait_for(
                "both independent targets at their provider boundary",
                || {
                    probe.receiver_waiting.load(Ordering::SeqCst)
                        && probe.outsider_waiting.load(Ordering::SeqCst)
                },
            )
            .await;
        }
        post_control(
            &client,
            &format!("{base}/chat"),
            &json!({
                "session_id":id,"message":message,"model":id,"provider":"openai",
                "model_ref":{"provider":"openai","model":id},"thinking_mode":"standard",
                "permission_mode":"auto","workspace_path":workspace,"project_id":project.id,
            }),
        )
        .await
        .unwrap_or_else(|error| panic!("start chat {id}: {error}"));
        post_control(&client, &format!("{base}/execute/{id}"), &json!({}))
            .await
            .unwrap_or_else(|error| panic!("execute {id}: {error}"));
    }
    wait_for("sender's idempotent peer delivery", || {
        probe.send_replayed.load(Ordering::SeqCst)
    })
    .await;
    probe.release.send_replace(true);
    wait_for("recipient's model to observe the peer input", || {
        probe.recipient_observed.load(Ordering::SeqCst)
    })
    .await;

    let storage = SessionStoreV2::new(data.clone()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let receiver = storage.load_session(RECIPIENT).await.unwrap().unwrap();
            if receiver.last_run_status().as_deref() == Some("completed") {
                assert_eq!(
                    receiver
                        .messages
                        .iter()
                        .filter(|message| {
                            message.role == bamboo_domain::Role::User
                                && message.content.contains(MARKER)
                        })
                        .count(),
                    1
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("recipient completes the original run");
    drop(host);
    provider_handle.stop(true).await;
}
