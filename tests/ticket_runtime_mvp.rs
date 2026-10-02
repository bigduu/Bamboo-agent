//! Real local Host/native Worker processes with a controlled HTTP provider.
//! This is Runtime acceptance; it makes no real-model semantic-quality claim.
#![cfg(unix)]
use actix_web::{web, App, HttpResponse, HttpServer};
use serde_json::{json, Value};
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

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
            .env("BAMBOO_JIANDU_DATA_DIR", data.join("jiandu"))
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}

async fn provider(body: web::Json<Value>, calls: web::Data<AtomicUsize>) -> HttpResponse {
    let task_only = body["tools"]
        .as_array()
        .is_some_and(|tools| tools.len() == 1 && tools[0]["function"]["name"] == "Task");
    let (delta, finish) = if task_only {
        calls.fetch_add(1, Ordering::SeqCst);
        let has_plan = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["role"] == "tool" && m["tool_call_id"] == "ticket-native-plan");
        if has_plan {
            (json!({"content":"TICKET_E2E_1481_DONE"}), "stop")
        } else {
            assert!(body["messages"].to_string().contains("TICKET_E2E_1481"));
            let args = json!({"tasks":[{"id":"own-step","content":"Own private plan","status":"pending"}]});
            (
                json!({"tool_calls":[{"index":0,"id":"ticket-native-plan","type":"function","function":{"name":"Task","arguments":args.to_string()}}]}),
                "tool_calls",
            )
        }
    } else {
        (json!({"content":"auxiliary"}), "stop")
    };
    let event = json!({"id":"ticket-runtime-mvp","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}

async fn get(client: &reqwest::Client, base: &str, path: &str) -> Value {
    let response = client.get(format!("{base}{path}")).send().await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "GET {path}: {status} {body}");
    serde_json::from_str(&body).unwrap()
}

async fn post(client: &reqwest::Client, base: &str, path: &str, request: &Value) -> Value {
    let response = client
        .post(format!("{base}{path}"))
        .json(request)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "POST {path}: {status} {body}");
    serde_json::from_str(&body).unwrap()
}

async fn command(client: &reqwest::Client, base: &str, id: &str, operations: Value) -> Value {
    let scope = get(client, base, "/tickets/scope").await;
    assert_eq!(scope["available"], true, "scope: {scope}");
    let snapshot = &scope["overview"]["snapshot"];
    json!({"operation_id":id,"binding":scope["binding"],"expected_seq":snapshot["seq"],"expected_epoch":snapshot["authority_epoch"],"operations":operations})
}

async fn ready(client: &reqwest::Client, base: &str, host: &mut Host, data: &Path) {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if let Some(status) = host.0.try_wait().unwrap() {
                panic!(
                    "actual Host exited {status}: {}",
                    std::fs::read_to_string(data.join("host.log")).unwrap()
                );
            }
            if client
                .get(format!("{base}/health"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("actual Host startup");
}

#[actix_web::test]
async fn actual_ticket_native_worker_create_plan_submit_accept_restart() {
    let temp = tempfile::Builder::new()
        .prefix("bamboo-1481-ticket-runtime-")
        .tempdir_in("/tmp")
        .unwrap()
        .keep();
    let data = temp.canonicalize().unwrap().join("host");
    eprintln!("Ticket fixture data: {}", data.display());
    std::fs::create_dir_all(&data).unwrap();
    let calls = web::Data::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(provider_calls.clone())
            .route("/v1/chat/completions", web::post().to(provider))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok().json(json!({"data":[{"id":"ticket-model"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let url = format!("http://{}/v1", server.addrs()[0]);
    let running = server.run();
    let provider_handle = running.handle();
    actix_web::rt::spawn(running);
    std::fs::write(
        data.join("config.json"),
        serde_json::to_vec(&json!({"provider":"openai",
        "features":{"provider_model_ref":true,"ticket_mutation":true,"ticket_dispatch":true},
        "providers":{"openai":{"api_key":"fixture","base_url":url,"model":"ticket-model"}},
        "defaults":{"chat":{"provider":"openai","model":"ticket-model"}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":5}}))
        .unwrap(),
    )
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let base = format!("http://127.0.0.1:{port}/api/v1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let mut host = start(&data, port);
    ready(&client, &base, &mut host, &data).await;
    let create = command(&client, &base, "create-one", json!([
        {"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[],
            "contract":{"title":"TICKET_E2E_1481","objective":"Return TICKET_E2E_1481_DONE and update only your private plan.","constraints":["Never alter the Supervisor plan"],"acceptance":["Exact output bytes"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
        {"op":"ready","work_id":"work"}])).await;
    let created = post(&client, &base, "/tickets/update", &create).await;
    let work = created["ids"]["work"].as_str().unwrap();
    let start_request = command(
        &client,
        &base,
        "start-one",
        json!([{"op":"start","work_id":work,"temp_id":"assignment","workspace":null}]),
    )
    .await;
    let started = post(&client, &base, "/tickets/dispatch", &start_request).await;
    eprintln!("Ticket dispatch observation: {}", started["runtime"]);
    assert_eq!(started["status"], "accepted_for_dispatch");
    assert_eq!(started["errors"], json!([]), "{started}");
    let key = started["runtime"][0]["dispatch_key"].as_str().unwrap();
    let inspect = json!({"ids":[work],"depth":0,"budget_bytes":65536,"fixed_commit":null});
    let view = tokio::time::timeout(Duration::from_secs(90), async {
        let mut last_seq = Value::Null;
        loop {
            let view = post(&client, &base, "/tickets/inspect", &inspect).await;
            if last_seq != view["snapshot"]["seq"] {
                eprintln!(
                    "Ticket seq {}; Work {}; Assignment {}; plan revision {}; provider calls {}",
                    view["snapshot"]["seq"],
                    view["data"][0]["ticket"]["state"],
                    view["data"][0]["assignments"][0]["state"],
                    view["data"][0]["assignments"][0]["plan"]["plan_revision"],
                    calls.load(Ordering::SeqCst)
                );
                last_seq = view["snapshot"]["seq"].clone();
            }
            if view["data"][0]["ticket"]["state"] == "submitted" {
                break view;
            }
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "Host exited during Worker run"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "actual Ticket Worker completion: {}",
            std::fs::read_to_string(data.join("host.log")).unwrap()
        )
    });
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "actual native Task callback then final output"
    );
    assert_eq!(
        view["data"][0]["assignments"][0]["plan"]["steps"][0]["id"],
        "own-step"
    );
    let submission = view["data"][0]["ticket"]["current_submission"]
        .as_str()
        .unwrap();
    let hash = view["data"][0]["submissions"][0]["artifacts"][0]["sha256"]
        .as_str()
        .unwrap();
    let output = client
        .get(format!("{base}/tickets/artifacts/{hash}"))
        .send()
        .await
        .unwrap();
    assert!(output.status().is_success());
    assert_eq!(output.text().await.unwrap(), "TICKET_E2E_1481_DONE");
    let child = get(&client, &base, &format!("/tickets/dispatch/{key}")).await;
    assert_eq!(child["status"], "terminal");
    let run = child["receipt"]["run_id"].clone();
    let accept = command(&client, &base, "accept-one", json!([{"op":"accept","work_id":work,"submission_id":submission,"evidence":["Verified exact Artifact output"]}])).await;
    post(&client, &base, "/tickets/update", &accept).await;
    assert_eq!(
        post(&client, &base, "/tickets/inspect", &inspect).await["data"][0]["ticket"]["state"],
        "accepted"
    );
    drop(host);
    let mut host = start(&data, port);
    ready(&client, &base, &mut host, &data).await;
    let replay = post(&client, &base, "/tickets/dispatch", &start_request).await;
    assert_eq!(replay["receipt"], started["receipt"]);
    assert_eq!(
        replay["runtime"][0]["observation"]["receipt"]["run_id"],
        run
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "restart does not reexecute a terminal dispatch"
    );
    assert_eq!(
        post(&client, &base, "/tickets/inspect", &inspect).await["data"][0]["ticket"]["state"],
        "accepted"
    );
    drop(host);
    provider_handle.stop(true).await;
    std::fs::remove_dir_all(temp).unwrap();
}
