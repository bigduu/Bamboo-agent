// Shared isolated process fixture; it never reads the user provider settings.
#![allow(dead_code)] // Integration targets use different subsets of this fixture.
use actix_web::{web, App, HttpResponse, HttpServer};
use serde_json::{json, Value};
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

pub struct Host(pub Child);
impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub fn start(data: &Path, port: u16) -> Host {
    start_with_fault(data, port, None)
}
pub fn start_with_fault(data: &Path, port: u16, fault: Option<(&str, &str)>) -> Host {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(data.join("host.log"))
        .unwrap();
    let mut process = Command::new(env!("CARGO_BIN_EXE_bamboo"));
    process
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
        .env_remove("BAMBOO_TICKET_FIXTURE_OPERATION_PREFIX")
        .env_remove("BAMBOO_TICKET_FIXTURE_BOUNDARY")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log));
    if let Some((prefix, boundary)) = fault {
        process
            .env("BAMBOO_TICKET_FIXTURE_OPERATION_PREFIX", prefix)
            .env("BAMBOO_TICKET_FIXTURE_BOUNDARY", boundary);
    }
    if let Ok(path) = std::env::var("BAMBOO_TICKET_FIXTURE_STATIC_DIR") {
        process.arg("--static-dir").arg(path);
    }
    Host(process.spawn().unwrap())
}

pub async fn get(client: &reqwest::Client, base: &str, path: &str) -> Value {
    let response = client.get(format!("{base}{path}")).send().await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "GET {path}: {status} {body}");
    serde_json::from_str(&body).unwrap()
}

pub async fn post(client: &reqwest::Client, base: &str, path: &str, request: &Value) -> Value {
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

pub async fn command(client: &reqwest::Client, base: &str, id: &str, operations: Value) -> Value {
    let scope = get(client, base, "/tickets/scope").await;
    assert_eq!(scope["available"], true, "scope: {scope}");
    let snapshot = &scope["overview"]["snapshot"];
    json!({"operation_id":id,"binding":scope["binding"],"expected_seq":snapshot["seq"],"expected_epoch":snapshot["authority_epoch"],"operations":operations})
}

pub async fn ready(client: &reqwest::Client, base: &str, host: &mut Host, data: &Path) {
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

#[derive(Default)]
pub struct Probe {
    pub coding_path: std::sync::Mutex<Option<(String, String)>>,
    pub hold_code: std::sync::atomic::AtomicBool,
    pub ui_fixture: std::sync::atomic::AtomicBool,
    pub calls: AtomicUsize,
    pub root_calls: AtomicUsize,
    pub held: AtomicUsize,
    pub input_checks: AtomicUsize,
    pub questions: AtomicUsize,
    pub answer_checks: AtomicUsize,
    pub release: tokio::sync::Notify,
}
async fn provider(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let task_only = body["tools"]
        .as_array()
        .is_some_and(|tools| tools.len() == 1 && tools[0]["function"]["name"] == "Task");
    let coding = body["tools"].as_array().is_some_and(|tools| {
        tools.len() == 3
            && tools.iter().all(|tool| {
                matches!(
                    tool["function"]["name"].as_str(),
                    Some("Task" | "Read" | "Write")
                )
            })
    });
    let (delta, finish) = if coding {
        probe.calls.fetch_add(1, Ordering::SeqCst);
        let (path, outside) = probe
            .coding_path
            .lock()
            .unwrap()
            .clone()
            .expect("explicit coding fixture");
        let messages = body["messages"].as_array().unwrap();
        let result = |id: &str| {
            messages
                .iter()
                .find(|m| m["role"] == "tool" && m["tool_call_id"] == id)
        };
        if result("code-read").is_some()
            && result("code-write").is_none()
            && probe.hold_code.swap(false, Ordering::SeqCst)
        {
            probe.held.fetch_add(1, Ordering::SeqCst);
            probe.release.notified().await;
        }
        let next = if result("code-read").is_none() {
            Some(("code-read", "Read", json!({"file_path":path})))
        } else if result("code-write").is_none() {
            Some((
                "code-write",
                "Write",
                json!({"file_path":path,"content":"pub fn answer() -> u8 { 42 }\n"}),
            ))
        } else if result("code-escape").is_none() {
            Some((
                "code-escape",
                "Write",
                json!({"file_path":outside,"content":"corrupt"}),
            ))
        } else if result("code-verify").is_none() {
            Some(("code-verify", "Read", json!({"file_path":path})))
        } else if result("code-plan").is_none() {
            assert!(result("code-escape").unwrap()["content"]
                .as_str()
                .unwrap()
                .contains("outside Assignment"));
            assert!(result("code-verify").unwrap()["content"]
                .as_str()
                .unwrap()
                .contains("42"));
            Some((
                "code-plan",
                "Task",
                json!({"tasks":[{"id":"code-step","content":"Verified isolated code artifact","status":"completed"}]}),
            ))
        } else {
            None
        };
        match next {
            Some((id, name, args)) => (
                json!({"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":name,"arguments":args.to_string()}}]}),
                "tool_calls",
            ),
            None => (json!({"content":"TICKET_CODE_DONE"}), "stop"),
        }
    } else if task_only {
        let messages = body["messages"].to_string();
        if messages.contains("TICKET_ACCEPTED_INPUT_E2E") {
            assert!(messages.contains("input_artifacts"));
            assert!(messages.contains("TICKET_E2E_1481_DONE"));
            probe.input_checks.fetch_add(1, Ordering::SeqCst);
        }
        probe.calls.fetch_add(1, Ordering::SeqCst);
        let content = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|m| m["content"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        if let Some(letter) = ["A", "B", "C", "D", "E"]
            .into_iter()
            .find(|l| content.contains(&format!("TICKET_QUESTION_E2E:{l}")))
        {
            assert!(
                body["tools"][0]["function"]["parameters"]["properties"]["question"].is_object()
            );
            if !content.contains(&format!("答案 {letter}")) {
                probe.questions.fetch_add(1, Ordering::SeqCst);
                let args = json!({"tasks":[{"id":"own-step","content":format!("Own private plan {letter}"),"status":"blocked"}],"question":{"prompt":format!("问题 {letter}：请给出专属答案")}});
                let delta = json!({"tool_calls":[{"index":0,"id":"ticket-native-question","type":"function","function":{"name":"Task","arguments":args.to_string()}}]});
                let event = json!({"id":"ticket-question","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":"tool_calls"}]});
                return HttpResponse::Ok()
                    .content_type("text/event-stream")
                    .body(format!("data: {event}\n\ndata: [DONE]\n\n"));
            }
            assert!(
                content.contains(&format!("答案 {letter}")),
                "versioned own answer missing"
            );
            for other in ["A", "B", "C", "D", "E"]
                .into_iter()
                .filter(|o| *o != letter)
            {
                assert!(
                    !content.contains(&format!("答案 {other}")),
                    "sibling answer leaked"
                );
            }
            probe.answer_checks.fetch_add(1, Ordering::SeqCst);
        }
        let has_plan = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["role"] == "tool" && m["tool_call_id"] == "ticket-native-plan");
        if has_plan {
            if body["messages"].to_string().contains("WAIT_FOR_CANCEL") {
                probe.held.fetch_add(1, Ordering::SeqCst);
                probe.release.notified().await;
            }
            (json!({"content":"TICKET_E2E_1481_DONE"}), "stop")
        } else {
            let args = json!({"tasks":[{"id":"own-step","content":"Own private plan","status":"pending"}]});
            (
                json!({"tool_calls":[{"index":0,"id":"ticket-native-plan","type":"function","function":{"name":"Task","arguments":args.to_string()}}]}),
                "tool_calls",
            )
        }
    } else if probe.ui_fixture.load(Ordering::SeqCst)
        && body["tools"].as_array().is_some_and(|tools| {
            tools
                .iter()
                .any(|t| t["function"]["name"] == "work_overview")
        })
    {
        // Controlled browser fixture only: the real single Supervisor reads
        // committed requests, proposes chatter once, then dispatches ready
        // question Workers through the existing tool/Runtime admission path.
        let last = body["messages"].as_array().unwrap().last().unwrap();
        let value = || {
            let raw: Value = serde_json::from_str(last["content"].as_str().unwrap()).unwrap();
            raw.get("result")
                .and_then(Value::as_str)
                .map(|s| serde_json::from_str(s).unwrap())
                .unwrap_or(raw)
        };
        let call = match last["tool_call_id"].as_str() {
            Some("ticket-ui-overview") => {
                let view = value();
                if view["pending_message"]["input"].is_object() {
                    Some((
                        "ticket-ui-resolve",
                        "work_update",
                        json!({"message_id":view["pending_message"]["input"]["message_id"],"proposal":{"groups":[]}}),
                    ))
                } else {
                    Some((
                        "ticket-ui-search",
                        "work_search",
                        json!({"filter":{"query":"TICKET_QUESTION_E2E","kind":"work","state":"ready","updated_after":null,"updated_before":null,"include_archived":false},"limit":100,"cursor":null,"fixed_commit":null}),
                    ))
                }
            }
            Some("ticket-ui-search") => {
                let search = value();
                let ops: Vec<_> = search["data"].as_array().expect("bounded ready Work search").iter().map(|row|json!({"op":"start","work_id":row["id"],"temp_id":format!("resume-{}",row["id"].as_str().unwrap()),"workspace":null})).collect();
                if ops.is_empty() {
                    None
                } else {
                    Some((
                        "ticket-ui-dispatch",
                        "work_dispatch",
                        json!({"operation_id":format!("ui-resume-{}",search["snapshot"]["seq"]),"expected_seq":search["snapshot"]["seq"],"expected_epoch":search["snapshot"]["authority_epoch"],"operations":ops}),
                    ))
                }
            }
            Some("ticket-ui-resolve") => Some(("ticket-ui-overview", "work_overview", json!({}))),
            Some("ticket-ui-dispatch") => {
                if value()["status_code"] == 409 {
                    Some(("ticket-ui-overview", "work_overview", json!({})))
                } else {
                    None
                }
            }
            _ => Some(("ticket-ui-overview", "work_overview", json!({}))),
        };
        match call {
            Some((id, name, args)) => (
                json!({"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":name,"arguments":args.to_string()}}]}),
                "tool_calls",
            ),
            None => (json!({"content":"UI_TICKET_SUPERVISOR_ACK"}), "stop"),
        }
    } else if body["tools"].as_array().is_some_and(|tools| {
        tools
            .iter()
            .any(|tool| tool["function"]["name"] == "work_overview")
    }) && body["messages"]
        .to_string()
        .contains("TICKET_SUPERVISOR_E2E")
    {
        let phase = probe.root_calls.fetch_add(1, Ordering::SeqCst);
        let result = |id: &str| -> Value {
            let message = body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| m["role"] == "tool" && m["tool_call_id"] == id)
                .expect("Host tool result in provider history");
            let value: Value = serde_json::from_str(message["content"].as_str().unwrap()).unwrap();
            if let Some(inner) = value.get("result").and_then(Value::as_str) {
                serde_json::from_str(inner).unwrap()
            } else {
                value
            }
        };
        let call = match phase {
            0 => Some(("ticket-root-overview", "work_overview", json!({}))),
            1 => {
                let overview = result("ticket-root-overview");
                let input = &overview["pending_message"]["input"];
                let proposal = if input["human"]["text"]
                    .as_str()
                    .unwrap()
                    .contains("TICKET_MULTI_E2E")
                {
                    let candidate = |title: &str| {
                        input["candidates"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|c| c["contract"]["title"] == title)
                            .unwrap()
                    };
                    let a = candidate("A");
                    let b = candidate("B");
                    let d = candidate("D");
                    let q = &a["requests"][0];
                    let mut contract = b["contract"].clone();
                    contract["objective"] = json!("英文报告B");
                    json!({"groups":[
                        {"group_id":"answer-A","item_ids":["answer"],"source_quote":"A使用绿色","clarification":null,"operations":[{"op":"answer","target":{"request_id":q["id"],"work_id":q["work_id"],"assignment_id":q["assignment_id"],"generation":q["generation"],"contract_revision":q["contract_revision"],"prompt_revision":q["prompt_revision"]},"answer":"绿色"}]},
                        {"group_id":"steer-B","item_ids":["steer"],"source_quote":"B改为英文","clarification":null,"operations":[{"op":"steer","target":b["target"],"contract":contract}]},
                        {"group_id":"new-C","item_ids":["create-start"],"source_quote":"新建并开始C","clarification":null,"operations":[
                            {"op":"create","temp_id":"C","kind":"work","parent":null,"depends_on":[],"contract":{"title":"C","objective":"WAIT_FOR_CANCEL TICKET_E2E_1481","constraints":[],"acceptance":["具体成果"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
                            {"op":"ready","target":{"ref":"temporary","id":"C"}},
                            {"op":"start","target":{"ref":"temporary","id":"C"},"temp_id":"assignment-C","workspace":null}]},
                        {"group_id":"cancel-D","item_ids":["cancel"],"source_quote":"取消D","clarification":null,"operations":[{"op":"cancel","target":d["target"]}]}
                    ]})
                } else {
                    json!({"groups":[{"group_id":"new-work","item_ids":["create-start"],"source_quote":input["human"]["text"],"clarification":null,"operations":[
                    {"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[], "contract":{"title":"TICKET_SUPERVISOR_E2E","objective":"WAIT_FOR_CANCEL TICKET_E2E_1481","constraints":["Own plan only"],"acceptance":["Exact output"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
                    {"op":"ready","target":{"ref":"temporary","id":"work"}},
                    {"op":"start","target":{"ref":"temporary","id":"work"},"temp_id":"assignment","workspace":null}]}]})
                };
                Some((
                    "ticket-root-create",
                    "work_update",
                    json!({"message_id":input["message_id"],"proposal":proposal}),
                ))
            }
            2 => {
                let created = result("ticket-root-create");
                assert_eq!(created["resolution"]["groups"][0]["status"], "committed");
                assert_eq!(created["dispatch"][0]["status"], "accepted_for_dispatch");
                None
            }
            3 => Some(("ticket-root-overview-next", "work_overview", json!({}))),
            4 => {
                let overview = result("ticket-root-overview-next");
                Some((
                    "ticket-root-noop",
                    "work_update",
                    json!({"message_id":overview["pending_message"]["input"]["message_id"],"proposal":{"groups":[]}}),
                ))
            }
            _ => None,
        };
        match call {
            Some((id, name, args)) => (
                json!({"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":name,"arguments":args.to_string()}}]}),
                "tool_calls",
            ),
            None => (json!({"content":"TICKET_SUPERVISOR_DISPATCHED"}), "stop"),
        }
    } else {
        (json!({"content":"auxiliary"}), "stop")
    };
    let event = json!({"id":"ticket-lifecycle","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}

pub struct Fixture {
    pub temp: std::path::PathBuf,
    pub data: std::path::PathBuf,
    pub port: u16,
    pub base: String,
    pub client: reqwest::Client,
    pub host: Option<Host>,
    pub probe: web::Data<Probe>,
    pub synthetic_config: Vec<u8>,
    provider: actix_web::dev::ServerHandle,
}
#[allow(dead_code)] // Shared by integration targets using different fixture cases.
impl Fixture {
    pub async fn new() -> Self {
        Self::with_fault(None).await
    }
    pub async fn with_fault(fault: Option<(&str, &str)>) -> Self {
        let temp = tempfile::Builder::new()
            .prefix("bamboo-1481-ticket-lifecycle-")
            .tempdir_in("/tmp")
            .unwrap()
            .keep();
        let data = temp.canonicalize().unwrap().join("host");
        eprintln!("Ticket fixture data: {}", data.display());
        std::fs::create_dir_all(&data).unwrap();
        let calls = web::Data::new(Probe::default());
        calls.ui_fixture.store(
            std::env::var_os("BAMBOO_TICKET_FIXTURE_STATIC_DIR").is_some(),
            Ordering::SeqCst,
        );
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
        let synthetic_config =
            serde_json::to_vec(&json!({"provider":"openai","setup":{"completed":true},
        "features":{"provider_model_ref":true,"ticket_mutation":true,"ticket_dispatch":true},
        "providers":{"openai":{"api_key":"fixture","base_url":url,"model":"ticket-model"}},
        "defaults":{"chat":{"provider":"openai","model":"ticket-model"}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":5}}))
            .unwrap();
        std::fs::write(data.join("config.json"), &synthetic_config).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let base = format!("http://127.0.0.1:{port}/api/v1");
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();
        let mut host = start_with_fault(&data, port, fault);
        ready(&client, &base, &mut host, &data).await;

        Self {
            temp,
            data,
            port,
            base,
            client,
            host: Some(host),
            probe: calls,
            synthetic_config,
            provider: provider_handle,
        }
    }
    pub async fn restart(&mut self) {
        drop(self.host.take());
        let mut host = start(&self.data, self.port);
        ready(&self.client, &self.base, &mut host, &self.data).await;
        self.host = Some(host);
    }
    pub async fn finish(mut self) {
        drop(self.host.take());
        self.probe.release.notify_waiters();
        self.provider.stop(true).await;
        std::fs::remove_dir_all(&self.temp).unwrap();
    }
}
