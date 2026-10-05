//! Opt-in real Supervisor/model and bounded native code Worker acceptance.
//! Coding capability/workspace is explicitly bound by User, never model-created.
#![cfg(unix)]
#[path = "support/ticket_live_provider.rs"]
mod live;
#[path = "support/ticket_runtime.rs"]
mod runtime;

use bamboo_domain::{Storage, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_engine::ticket_worker_plan::tickets::{content_hash, store::FileStore, *};
use bamboo_storage::SessionStoreV2;
use live::{LiveBridge, LiveConfig, HOST_CREDENTIAL};
use runtime::{command, get, post, Host};
use serde_json::{json, Value};
use std::{os::unix::fs::MetadataExt, path::PathBuf, process::Command, time::Duration};

const TITLE: &str = "单工单代码交付";
const CODE: &str = "pub fn answer() -> u8 { 42 }\n";
const ORIGINAL: &str = "pub fn answer() -> u8 { 0 }\n";

fn model_catalog_read(request: &Value) -> bool {
    request["role"] == "models"
        && request["path"] == "/v1/models"
        && request.get("model") == Some(&Value::Null)
        && request["status"] == 200
        && request["request_bytes"] == 0
        && request["request_sha256"] == content_hash(b"")
}

// Classify every actual attempt without retrying at the fixture layer. The
// production provider already permits at most four bounded transport attempts.
fn upstream_runs_finished(requests: &[Value], model: &str) -> bool {
    let mut runs = std::collections::BTreeMap::<(String, String), Vec<&Value>>::new();
    for request in requests {
        let path = request["path"].as_str().unwrap_or_default();
        if request["model"].is_null() {
            if !model_catalog_read(request) {
                return false;
            }
            continue;
        }
        let Some(hash) = request["request_sha256"].as_str() else {
            return false;
        };
        if request["model"] != model
            || !matches!(path, "/v1/responses" | "/v1/chat/completions")
            || hash.len() != 64
            || !hash.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return false;
        }
        runs.entry((path.to_owned(), hash.to_owned()))
            .or_default()
            .push(request);
    }
    !runs.is_empty()
        && runs.values().all(|attempts| {
            attempts.len() <= 4
                && attempts.last().unwrap()["status"] == 200
                && attempts[..attempts.len() - 1].iter().all(|r| {
                    r["status"]
                        .as_u64()
                        .is_some_and(|s| (500..600).contains(&s))
                        || (r["status"].is_null() && r["error"] == "upstream transport failure")
                })
        })
}

fn replay_keeps_model_execution(before: &[Value], after: &[Value], model: &str) -> bool {
    upstream_runs_finished(before, model)
        && upstream_runs_finished(after, model)
        && before
            .iter()
            .filter(|r| !model_catalog_read(r))
            .eq(after.iter().filter(|r| !model_catalog_read(r)))
}

#[test]
fn replay_allows_only_valid_catalog_reads_without_another_execution() {
    let execution = |hash: char, path: &str| {
        json!({"role":"supervisor","path":path,"model":"selected",
            "request_sha256":hash.to_string().repeat(64),"request_bytes":100,"status":200})
    };
    let catalog = json!({"role":"models","path":"/v1/models","model":null,
        "request_sha256":content_hash(b""),"request_bytes":0,"status":200});
    let before = vec![execution('a', "/v1/responses"), catalog.clone()];
    let mut refreshed = before.clone();
    refreshed.push(catalog.clone());
    assert_ne!(before, refreshed, "a browser refresh adds a catalog read");
    assert!(replay_keeps_model_execution(
        &before, &refreshed, "selected"
    ));
    for extra in [
        execution('b', "/v1/responses"),
        execution('b', "/v1/chat/completions"),
        execution('a', "/v1/responses"),
    ] {
        let mut changed = refreshed.clone();
        changed.push(extra);
        assert!(!replay_keeps_model_execution(&before, &changed, "selected"));
    }
    assert!(!replay_keeps_model_execution(&before, &[], "selected"));
    for (field, value) in [
        ("role", json!("supervisor")),
        ("path", json!("/v1/responses")),
        ("model", json!("selected")),
        ("status", json!(500)),
        ("request_bytes", json!(1)),
        ("request_sha256", json!("b".repeat(64))),
    ] {
        let mut invalid = catalog.clone();
        invalid[field] = value;
        let mut changed = before.clone();
        changed.push(invalid);
        assert!(!replay_keeps_model_execution(&before, &changed, "selected"));
        let mut missing = catalog.clone();
        missing.as_object_mut().unwrap().remove(field);
        let mut changed = before.clone();
        changed.push(missing);
        assert!(!replay_keeps_model_execution(&before, &changed, "selected"));
    }
}

#[test]
fn upstream_completion_requires_bounded_recovery_and_exact_model() {
    let attempt = |status: u16| {
        json!({"path":"/v1/responses","model":"selected",
        "request_sha256":"a".repeat(64),"status":status})
    };
    assert!(upstream_runs_finished(&[attempt(200)], "selected"));
    assert!(upstream_runs_finished(
        &[attempt(500), attempt(500), attempt(200)],
        "selected"
    ));
    for statuses in [
        vec![500],
        vec![401, 200],
        vec![200, 200],
        vec![500, 500, 500, 500, 200],
    ] {
        assert!(!upstream_runs_finished(
            &statuses.into_iter().map(attempt).collect::<Vec<_>>(),
            "selected"
        ));
    }
    assert!(!upstream_runs_finished(&[attempt(200)], "other"));
    let mut foreign = attempt(500);
    foreign["request_sha256"] = json!("b".repeat(64));
    assert!(!upstream_runs_finished(
        &[foreign, attempt(200)],
        "selected"
    ));
    let mut transport = attempt(500);
    transport["status"] = Value::Null;
    transport["error"] = json!("upstream transport failure");
    assert!(upstream_runs_finished(
        &[transport.clone(), attempt(200)],
        "selected"
    ));
    transport["error"] = json!("unknown failure");
    assert!(!upstream_runs_finished(
        &[transport, attempt(200)],
        "selected"
    ));
}

#[actix_web::test]
async fn native_bridge_requires_explicit_bounded_worker_catalog() {
    use actix_web::{web, App, HttpResponse, HttpServer};
    const BYTES: &str = "data: {\"actual\":\"transport-only\"}\n\ndata: [DONE]\n\n";
    let server = HttpServer::new(|| {
        App::new().route(
            "/v1/responses",
            web::post().to(|| async {
                HttpResponse::Ok()
                    .content_type("text/event-stream")
                    .body(BYTES)
            }),
        )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let endpoint = format!("http://{}/v1", server.addrs()[0]);
    let server = server.run();
    let handle = server.handle();
    actix_web::rt::spawn(server);
    let client = reqwest::Client::new();
    let ordinary = LiveBridge::start(LiveConfig::synthetic_native_transport_test(
        endpoint.clone(),
    ))
    .await;
    for (tools, expected) in [
        (vec!["Task", "Read", "Write"], 200),
        (vec!["Task", "Read", "Bash"], 403),
        (vec!["Task", "Read", "Write", "Bash"], 403),
        (vec!["Task", "Task", "Write"], 403),
        (vec!["work_overview", "work_update"], 200),
    ] {
        let response = client.post(format!("{}/responses", ordinary.base))
            .bearer_auth(HOST_CREDENTIAL)
            .json(&json!({"model":"transport-only","tools":tools.iter().map(|name|json!({"name":name})).collect::<Vec<_>>() }))
            .send().await.unwrap();
        assert_eq!(response.status().as_u16(), expected, "{tools:?}");
        if expected == 200 {
            assert_eq!(response.text().await.unwrap(), BYTES);
        }
    }
    let response = client
        .post(format!("{}/responses", ordinary.base))
        .bearer_auth(HOST_CREDENTIAL)
        .json(&json!({"model":"transport-only","tools":[
            {"name":"Task"},{"name":"Read"},{"name":"Write"},{"type":"web_search"}
        ]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403, "unnamed extra tools are forbidden");
    assert_eq!(
        ordinary.requests().len(),
        2,
        "rejected catalogs never reach upstream"
    );
    assert_eq!(ordinary.requests()[0]["role"], "native_worker");
    assert_eq!(ordinary.requests()[1]["role"], "supervisor");
    ordinary.finish().await;
    // The existing Human-only mode must still reject Worker traffic.
    let disabled = LiveBridge::start({
        let mut config = LiveConfig::synthetic_native_transport_test(endpoint);
        config.disable_native_worker_for_transport_test();
        config
    })
    .await;
    let response = client.post(format!("{}/responses", disabled.base))
        .bearer_auth(HOST_CREDENTIAL)
        .json(&json!({"model":"transport-only","tools":[{"name":"Task"},{"name":"Read"},{"name":"Write"}]}))
        .send().await.unwrap();
    assert_eq!(response.status(), 403);
    assert!(disabled.requests().is_empty());
    disabled.finish().await;
    handle.stop(true).await;
}

struct LiveWork {
    temp: PathBuf,
    data: PathBuf,
    evidence: PathBuf,
    base: String,
    port: u16,
    client: reqwest::Client,
    host: Option<Host>,
    bridge: LiveBridge,
    binding: ScopeBinding,
    worker_pid: Option<u32>,
}

impl LiveWork {
    async fn start() -> Self {
        let evidence = PathBuf::from(
            std::env::var("BAMBOO_TICKET_LIVE_WORKER_EVIDENCE")
                .expect("explicit fresh absolute evidence directory required"),
        );
        assert!(
            evidence.is_absolute() && !evidence.exists(),
            "never overwrite a prior live attempt"
        );
        std::fs::create_dir_all(&evidence).unwrap();
        let bridge = LiveBridge::start(LiveConfig::read_with_native_worker()).await;
        let temp = tempfile::Builder::new()
            .prefix("bamboo-1538-live-work-")
            .tempdir()
            .unwrap()
            .keep()
            .canonicalize()
            .unwrap();
        let data = temp.join("host");
        std::fs::create_dir(&data).unwrap();
        std::fs::write(data.join("config.json"), serde_json::to_vec(&json!({
            "provider":"openai","setup":{"completed":true},
            "features":{"provider_model_ref":true,"ticket_mutation":true,"ticket_dispatch":true},
            "providers":{"openai":{"api_key":HOST_CREDENTIAL,"base_url":bridge.base,
                "model":bridge.model,"responses_only_models":bridge.responses_only_models}},
            "defaults":{"chat":{"provider":"openai","model":bridge.model}},
            "subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1}
        })).unwrap()).unwrap();
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        let base = format!("http://127.0.0.1:{port}/api/v1");
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();
        let mut host = runtime::start(&data, port);
        runtime::ready(&client, &base, &mut host, &data).await;
        let scope = get(&client, &base, "/tickets/scope").await;
        assert_eq!(scope["available"], true);
        let f = Self {
            temp,
            data,
            evidence,
            base,
            port,
            client,
            host: Some(host),
            bridge,
            binding: serde_json::from_value(scope["binding"].clone()).unwrap(),
            worker_pid: None,
        };
        f.save(
            "runtime",
            &json!({"data":f.data,"origin":f.base.trim_end_matches("/api/v1"),
            "model":f.bridge.model,"host_pid":f.host.as_ref().unwrap().0.id(),"scope":scope,
            "coding_authority":"explicit User command; existing bounded tools/workspace only"}),
        );
        f
    }

    fn save(&self, name: &str, value: &Value) {
        std::fs::write(
            self.evidence.join(format!("{name}.json")),
            serde_json::to_vec_pretty(value).unwrap(),
        )
        .unwrap();
        self.bridge.verify_unchanged();
        if self.data.join("host.log").exists() {
            std::fs::copy(self.data.join("host.log"), self.evidence.join("host.log")).unwrap();
        }
    }

    async fn inspect(&self, work: &str) -> Value {
        post(&self.client,&self.base,"/tickets/inspect",&json!({"ids":[work],"depth":0,
            "sections":["assignments","requests","submissions"],"budget_bytes":65536,"fixed_commit":null})).await
    }

    async fn human(&self, id: &str, text: &str) -> (Value, Value) {
        let request = json!({"session_id":DEFAULT_SUPERVISOR_SESSION_ID,"message_id":id,
            "message":text,"model":self.bridge.model,"provider":"openai",
            "model_ref":{"provider":"openai","model":self.bridge.model}});
        self.save(&format!("{id}-request"), &request);
        let admitted = post(&self.client, &self.base, "/chat", &request).await;
        let execution = post(
            &self.client,
            &self.base,
            &format!("/execute/{DEFAULT_SUPERVISOR_SESSION_ID}"),
            &json!({}),
        )
        .await;
        let store = SessionStoreV2::new(self.data.clone()).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
        loop {
            let root = store
                .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
                .await
                .unwrap()
                .unwrap();
            let messages = json!(root.messages);
            let completed = current_turn_finished(&messages, id)
                && root.last_run_status().as_deref() == Some("completed");
            if completed
                || root.last_run_status().as_deref() == Some("error")
                || tokio::time::Instant::now() >= deadline
            {
                let run = json!({"admitted":admitted,"execution":execution,"status":root.last_run_status(),
                    "current_turn_finished":completed,"messages":messages,"requests":self.bridge.requests()});
                self.save(&format!("{id}-first-attempt"), &run);
                assert!(
                    completed,
                    "current Human turn failed; first attempt and owned Host data retained at {}",
                    self.evidence.display()
                );
                return (request, run);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn submitted(&mut self, work: &str) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
        loop {
            if self.worker_pid.is_none() {
                let host = self.host.as_ref().unwrap().0.id();
                let system = sysinfo::System::new_all();
                let pids: Vec<_> = system
                    .processes()
                    .iter()
                    .filter(|(_, p)| {
                        p.parent().is_some_and(|id| id.as_u32() == host)
                            && p.cmd().iter().any(|a| a == "subagent-worker")
                    })
                    .map(|(id, _)| id.as_u32())
                    .collect();
                if !pids.is_empty() {
                    self.save(
                        "native-worker-alive",
                        &json!({"host_pid":host,"owned_native_pids":pids}),
                    );
                    assert_eq!(pids.len(), 1);
                    assert_eq!(unsafe { libc::kill(pids[0] as i32, 0) }, 0);
                    self.worker_pid = Some(pids[0]);
                }
            }
            let view = self.inspect(work).await;
            let row = &view["data"][0];
            if row["ticket"]["state"] == "submitted"
                && row["assignments"][0]["process_stopped"] == true
            {
                self.save("submitted-first-attempt",&json!({"view":view,"requests":self.bridge.requests(),"worker_pid":self.worker_pid}));
                return view;
            }
            if tokio::time::Instant::now() >= deadline
                || matches!(
                    row["assignments"][0]["state"].as_str(),
                    Some("failed" | "outcome_unknown" | "cancelled")
                )
            {
                self.save("worker-failed-first-attempt",&json!({"view":view,"requests":self.bridge.requests(),"worker_pid":self.worker_pid}));
                panic!(
                    "native Work did not submit; retained {}",
                    self.evidence.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn browser_checkpoint(&self, phase: &str, view: &Value) {
        let Ok(path) = std::env::var("BAMBOO_TICKET_LIVE_WORKER_BROWSER_INFO") else {
            return;
        };
        let info = PathBuf::from(path);
        assert!(info.is_absolute());
        let done = info.with_extension(format!("{phase}.done"));
        assert!(!done.exists(), "fresh browser acknowledgement required");
        let value = json!({"phase":phase,"origin":self.base.trim_end_matches("/api/v1"),
            "session_id":DEFAULT_SUPERVISOR_SESSION_ID,"view":view,"done":done});
        let staged = info.with_extension("tmp");
        std::fs::write(&staged, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        std::fs::rename(staged, &info).unwrap();
        eprintln!("LIVE_WORKER_BROWSER_READY {phase} {}", info.display());
        tokio::time::timeout(Duration::from_secs(360), async {
            while !done.exists() {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .expect("bounded existing-Lotus browser acceptance");
        let receipt: Value = serde_json::from_slice(&std::fs::read(done).unwrap()).unwrap();
        self.save(&format!("browser-{phase}"), &receipt);
        assert_eq!(receipt["pass"], true);
        assert_eq!(receipt["phase"], phase);
    }

    fn stopped_snapshot(&mut self) -> Snapshot {
        drop(self.host.take());
        let store = FileStore::open(
            &self
                .data
                .join("tickets")
                .join(self.binding.scope_id.strip_prefix("supervisor/").unwrap()),
            self.binding.clone(),
        )
        .unwrap();
        assert_eq!(store.health, Health::Writable);
        store.published.as_ref().unwrap().1.clone()
    }

    async fn restart(&mut self) {
        assert!(self.host.is_none());
        let mut host = runtime::start(&self.data, self.port);
        runtime::ready(&self.client, &self.base, &mut host, &self.data).await;
        self.host = Some(host);
    }
}

fn current_turn_finished(messages: &Value, id: &str) -> bool {
    let Some(messages) = messages.as_array() else {
        return false;
    };
    let mut current_call = None;
    let mut result_seen = false;
    for message in messages {
        if let Some(calls) = message["tool_calls"].as_array() {
            for call in calls {
                if call["function"]["name"] == "work_update" {
                    let args: Value =
                        serde_json::from_str(call["function"]["arguments"].as_str().unwrap_or(""))
                            .unwrap_or(Value::Null);
                    if args["message_id"] == id {
                        current_call = call["id"].as_str();
                        result_seen = false;
                    }
                }
            }
        }
        if current_call.is_some()
            && message["role"] == "tool"
            && message["tool_call_id"].as_str() == current_call
        {
            result_seen = true;
        }
        if result_seen
            && message["role"] == "assistant"
            && message["content"]
                .as_str()
                .is_some_and(|s| !s.trim().is_empty())
            && message["tool_calls"]
                .as_array()
                .is_none_or(|c| c.is_empty())
        {
            return true;
        }
    }
    false
}

fn successful_file_reply(messages: &[Value], index: usize, call: &Value) -> Option<(usize, Value)> {
    let id = call["id"].as_str()?;
    let (index, result) = messages
        .iter()
        .enumerate()
        .skip(index + 1)
        .find(|(_, m)| m["role"] == "tool" && m["tool_call_id"].as_str() == Some(id))?;
    (result["tool_success"] == true).then(|| {
        serde_json::from_str(result["content"].as_str()?)
            .ok()
            .map(|v| (index, v))
    })?
}

fn verified_worker_file_io(messages: &Value, baseline: &str, file: &str) -> bool {
    let Some(messages) = messages.as_array() else {
        return false;
    };
    let calls: Vec<_> = messages
        .iter()
        .enumerate()
        .flat_map(|(index, m)| {
            m["tool_calls"]
                .as_array()
                .into_iter()
                .flatten()
                .map(move |call| (index, call))
        })
        .collect();
    let writes: Vec<_> = calls
        .iter()
        .filter(|(_, c)| c["function"]["name"] == "Write")
        .collect();
    let [write] = writes.as_slice() else {
        return false;
    };
    let args: Value = serde_json::from_str(write.1["function"]["arguments"].as_str().unwrap_or(""))
        .unwrap_or(Value::Null);
    if args["file_path"] != file || args["content"] != CODE {
        return false;
    }
    let Some((written, reply)) = successful_file_reply(messages, write.0, write.1) else {
        return false;
    };
    let hash = content_hash(CODE.as_bytes());
    if reply["sha256"] != hash || reply["artifact"]["sha256"] != hash {
        return false;
    }
    let read = |path: &str, content: &str, after: Option<usize>, before: Option<usize>| {
        calls.iter().any(|(index, call)| {
            let args: Value =
                serde_json::from_str(call["function"]["arguments"].as_str().unwrap_or(""))
                    .unwrap_or(Value::Null);
            if call["function"]["name"] != "Read"
                || args["file_path"] != path
                || after.is_some_and(|n| *index <= n)
            {
                return false;
            }
            successful_file_reply(messages, *index, call).is_some_and(|(index, reply)| {
                before.is_none_or(|n| index < n)
                    && reply["content"] == content
                    && reply["sha256"] == content_hash(content.as_bytes())
            })
        })
    };
    read(baseline, ORIGINAL, None, Some(write.0)) && read(file, CODE, Some(written), None)
}

#[test]
fn worker_verification_requires_successful_ordered_read_receipts() {
    let call = |id: &str, name: &str, args: Value| {
        json!({"role":"assistant","tool_calls":[
            {"id":id,"function":{"name":name,"arguments":args.to_string()}}
        ]})
    };
    let result = |id: &str, content: &str| {
        json!({"role":"tool","tool_call_id":id,"tool_success":true,
        "content":json!({"content":content,"sha256":content_hash(content.as_bytes()),"artifact":null}).to_string()})
    };
    let baseline = call("baseline", "Read", json!({"file_path":"/work/answer.rs"}));
    let write = call(
        "write",
        "Write",
        json!({"file_path":"/work/new.rs","content":CODE}),
    );
    let written = json!({"role":"tool","tool_call_id":"write","tool_success":true,
        "content":json!({"content":null,"sha256":content_hash(CODE.as_bytes()),
            "artifact":{"uri":"artifact:test","sha256":content_hash(CODE.as_bytes())}}).to_string()});
    let verify = call("verify", "Read", json!({"file_path":"/work/new.rs"}));
    let failed = json!([baseline, result("baseline", ORIGINAL), verify,
        {"role":"tool","tool_call_id":"verify","tool_success":false,"content":"Error: NotFound"}, write, written]);
    assert!(
        !verified_worker_file_io(&failed, "/work/answer.rs", "/work/new.rs"),
        "failed output Read before Write cannot verify the delivery"
    );
    let mut valid = vec![
        baseline,
        result("baseline", ORIGINAL),
        write,
        written,
        verify,
        result("verify", CODE),
    ];
    assert!(verified_worker_file_io(
        &json!(valid),
        "/work/answer.rs",
        "/work/new.rs"
    ));
    valid[5]["tool_call_id"] = json!("foreign");
    assert!(!verified_worker_file_io(
        &json!(valid),
        "/work/answer.rs",
        "/work/new.rs"
    ));
    valid[5] = result("verify", ORIGINAL);
    assert!(!verified_worker_file_io(
        &json!(valid),
        "/work/answer.rs",
        "/work/new.rs"
    ));
    valid[5] = result("verify", CODE);
    valid[5]["tool_success"] = json!(false);
    assert!(!verified_worker_file_io(
        &json!(valid),
        "/work/answer.rs",
        "/work/new.rs"
    ));
}

#[test]
fn current_human_completion_requires_its_own_receipt_and_final_reply() {
    let prior = json!([
        {"role":"assistant","tool_calls":[{"id":"old-call","function":{"name":"work_update","arguments":json!({"message_id":"old"}).to_string()}}]},
        {"role":"tool","tool_call_id":"old-call","content":"saved"},
        {"role":"assistant","content":"Previous turn completed"}
    ]);
    assert!(current_turn_finished(&prior, "old"));
    assert!(
        !current_turn_finished(&prior, "new"),
        "old completed status cannot complete new Human input"
    );
    let mut pending = prior.as_array().unwrap().clone();
    pending.push(json!({"role":"assistant","tool_calls":[{"id":"new-call","function":{"name":"work_update","arguments":json!({"message_id":"new"}).to_string()}}]}));
    assert!(!current_turn_finished(&json!(pending), "new"));
    pending.push(json!({"role":"tool","tool_call_id":"old-call","content":"late old result"}));
    assert!(
        !current_turn_finished(&json!(pending), "new"),
        "foreign receipt cannot complete current call"
    );
    pending.push(json!({"role":"tool","tool_call_id":"new-call","content":"saved"}));
    assert!(
        !current_turn_finished(&json!(pending), "new"),
        "receipt alone does not finish the Root reply"
    );
    pending.push(json!({"role":"assistant","content":"Current result saved"}));
    assert!(current_turn_finished(&json!(pending), "new"));
}

fn git(dir: &std::path::Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}

#[actix_web::test]
#[ignore = "explicit read-only live provider, fresh evidence directory and isolated native Worker"]
async fn real_model_single_work_code_delivery_acceptance_and_restart() {
    let mut f = LiveWork::start().await;
    let create_text=format!("创建一个名为{TITLE}的Work，目标是在稍后显式授权的隔离worktree中新建answer-created.rs，内容为pub fn answer() -> u8 {{ 42 }}并带一个末尾换行，保留answer.rs。现在只创建工单，不启动、不验收。交付需用户明确验收。");
    let (create_request, create_run) = f.human("live-work-create", &create_text).await;
    let search=post(&f.client,&f.base,"/tickets/search",&json!({"filter":{"query":TITLE,"kind":"work",
        "state":null,"updated_after":null,"updated_before":null,"include_archived":false},"limit":10,"cursor":null,"fixed_commit":null})).await;
    f.save(
        "created-first-attempt",
        &json!({"search":search,"run":create_run}),
    );
    assert_eq!(search["truncated"], false);
    assert_eq!(search["data"].as_array().unwrap().len(), 1);
    let work = search["data"][0]["id"].as_str().unwrap().to_owned();
    let created = f.inspect(&work).await;
    assert_eq!(created["data"][0]["ticket"]["state"], "draft");
    assert!(created["data"][0]["assignments"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        created["data"][0]["ticket"]["contract"]["allowed_tools"],
        json!(["Task"])
    );
    assert_eq!(
        created["data"][0]["ticket"]["contract"]["user_acceptance_required"],
        true
    );

    let repo = f.temp.join("repo");
    let tree = f.temp.join("assignment");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-b", "main"]);
    std::fs::write(repo.join("answer.rs"), ORIGINAL).unwrap();
    git(&repo, &["add", "answer.rs"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Ticket fixture",
            "-c",
            "user.email=ticket@fixture.invalid",
            "commit",
            "-m",
            "owned fixture baseline",
        ],
    );
    let base = git(&repo, &["rev-parse", "HEAD"]);
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "ticket-live-code",
            tree.to_str().unwrap(),
        ],
    );
    let tree = tree.canonicalize().unwrap();
    let file = tree.join("answer-created.rs");
    let mut contract = created["data"][0]["ticket"]["contract"].clone();
    contract["allowed_tools"] = json!(["Task", "Read", "Write"]);
    contract["objective"]=json!(format!("Use Task to maintain and complete your private plan. Read {}. Create the absent file {} using Write with exactly these UTF-8 bytes: {CODE:?}. Read the new file to verify it. Preserve answer.rs and every other file. Finish with a short nonempty actual result; do not ask for another permission or approval, commit, use shell, or dispatch other work.",tree.join("answer.rs").display(),file.display()));
    contract["constraints"] = json!([
        "Only this explicit isolated Assignment worktree",
        "No sibling repo, .git or .bamboo writes",
        "No external effects or shell"
    ]);
    contract["acceptance"] = json!([
        "Exact managed code artifact bytes/hash",
        "answer() returns 42",
        "Original/base file unchanged",
        "Explicit User acceptance for this Submission"
    ]);
    let grant = command(
        &f.client,
        &f.base,
        "live-code-user-grant",
        json!([{"op":"update_contract","work_id":work,"contract":contract}]),
    )
    .await;
    f.save("explicit-user-coding-grant-request", &grant);
    let granted = post(&f.client, &f.base, "/tickets/update", &grant).await;
    // The existing update_contract transition already makes a stopped draft
    // ready. A second Ready operation in this transaction would be invalid.
    let granted_view = f.inspect(&work).await;
    f.save(
        "explicit-user-coding-grant-result",
        &json!({"receipt":granted,"view":granted_view}),
    );
    assert_eq!(granted_view["data"][0]["ticket"]["state"], "ready");
    let granted_tools: std::collections::BTreeSet<String> = serde_json::from_value(
        granted_view["data"][0]["ticket"]["contract"]["allowed_tools"].clone(),
    )
    .unwrap();
    assert_eq!(
        granted_tools,
        ["Task", "Read", "Write"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    );
    assert!(granted_view["data"][0]["assignments"]
        .as_array()
        .unwrap()
        .is_empty());
    let workspace = json!({"repo":repo.canonicalize().unwrap(),"base_commit":base,"branch":"ticket-live-code",
        "worktree":tree,"write_roots":[tree],"claims":[format!("worktree:{}",tree.display())]});
    let dispatch = command(
        &f.client,
        &f.base,
        "live-code-user-start",
        json!([
            {"op":"start","work_id":work,"temp_id":"assignment","workspace":workspace}
        ]),
    )
    .await;
    f.save("explicit-user-coding-authority",&json!({"created":created,"grant_request":grant,"grant_receipt":granted,"dispatch_request":dispatch,
        "boundary":"User commands bind capabilities/workspace; the model created Task-only and did not grant itself file authority"}));
    let started = post(&f.client, &f.base, "/tickets/dispatch", &dispatch).await;
    f.save("dispatch-first-attempt", &started);
    assert_eq!(started["status"], "accepted_for_dispatch");
    assert_eq!(started["errors"], json!([]));
    let view = f.submitted(&work).await;
    let row = &view["data"][0];
    let a = &row["assignments"][0];
    let s = &row["submissions"][0];
    assert_eq!(row["assignments"].as_array().unwrap().len(), 1);
    assert_eq!(row["submissions"].as_array().unwrap().len(), 1);
    assert_eq!(row["ticket"]["accepted_submission"], Value::Null);
    assert_eq!(row["ticket"]["current_submission"], s["id"]);
    assert_eq!(s["stale"], false);
    assert_eq!(s["assignment_id"], a["id"]);
    assert_eq!(s["generation"], a["generation"]);
    assert_eq!(s["contract_revision"], row["ticket"]["contract_revision"]);
    assert_eq!(s["runtime"], a["runtime"]);
    let storage = SessionStoreV2::new(f.data.clone()).await.unwrap();
    let worker = storage
        .load_session(s["runtime"]["session_id"].as_str().unwrap())
        .await
        .unwrap()
        .expect("canonical native Worker transcript");
    f.save("native-worker-transcript", &json!(worker));
    let messages = json!(worker.messages);
    let calls: Vec<_> = messages
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|m| m["tool_calls"].as_array().into_iter().flatten())
        .collect();
    let named = |name: &str| {
        calls
            .iter()
            .filter(|c| c["function"]["name"] == name)
            .copied()
            .collect::<Vec<_>>()
    };
    assert!(
        !named("Task").is_empty(),
        "real model must update its private LocalPlan"
    );
    assert_eq!(
        named("Write").len(),
        1,
        "only the exact absent code path may be created"
    );
    let write: Value =
        serde_json::from_str(named("Write")[0]["function"]["arguments"].as_str().unwrap()).unwrap();
    assert_eq!(write["file_path"].as_str(), file.to_str());
    assert_eq!(write["content"], CODE);
    assert!(
        verified_worker_file_io(
            &messages,
            tree.join("answer.rs").to_str().unwrap(),
            file.to_str().unwrap()
        ),
        "successful baseline Read and exact created-code Read after successful Write required"
    );
    assert!(
        calls.iter().all(|c| matches!(
            c["function"]["name"].as_str(),
            Some("Task" | "Read" | "Write")
        )),
        "no extra native tools"
    );
    assert!(!a["plan"]["steps"].as_array().unwrap().is_empty());
    assert!(a["plan"]["steps"]
        .as_array()
        .unwrap()
        .iter()
        .all(|s| s["completed"] == true));
    let pid = f
        .worker_pid
        .expect("observe the actual owned native PID, not only a logical receipt");
    assert_ne!(
        unsafe { libc::kill(pid as i32, 0) },
        0,
        "native PID must actually be reaped"
    );
    assert_eq!(std::fs::read_to_string(&file).unwrap(), CODE);
    assert_eq!(
        std::fs::read_to_string(repo.join("answer.rs")).unwrap(),
        ORIGINAL
    );
    assert_eq!(
        std::fs::read_to_string(tree.join("answer.rs")).unwrap(),
        ORIGINAL
    );
    let hash = content_hash(CODE.as_bytes());
    assert!(s["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["sha256"] == hash));
    let artifact = f
        .client
        .get(format!("{}/tickets/artifacts/{hash}", f.base))
        .send()
        .await
        .unwrap();
    assert!(artifact.status().is_success());
    assert_eq!(artifact.bytes().await.unwrap().as_ref(), CODE.as_bytes());
    assert_eq!(s["effects"].as_object().unwrap().len(), 1);
    assert!(s["effects"]
        .as_object()
        .unwrap()
        .values()
        .all(|e| e["state"] == "succeeded" && e["artifact"]["sha256"] == hash));
    let verifier = f.temp.join("verify.rs");
    let binary = f.temp.join("verify-code");
    std::fs::write(
        &verifier,
        format!(
            "include!({:?});\n#[test] fn exact_answer() {{ assert_eq!(answer(),42); }}\n",
            file.to_str().unwrap()
        ),
    )
    .unwrap();
    let compiled = Command::new("rustc")
        .arg("--test")
        .arg(&verifier)
        .arg("-o")
        .arg(&binary)
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let checked = Command::new(&binary).output().unwrap();
    f.save("verified-artifact",&json!({"file":file,"sha256":hash,"inode":std::fs::metadata(&file).unwrap().ino(),
        "compile_pass":compiled.status.success(),"test_pass":checked.status.success(),"test_stdout":String::from_utf8_lossy(&checked.stdout),"native_pid":pid,"view":view}));
    assert!(checked.status.success());
    f.browser_checkpoint("submitted", &view).await;

    let submission = s["id"].as_str().unwrap();
    let accept_text = format!("验收通过 {TITLE} submission {submission} 文件SHA256 {hash}");
    let (accept_request, accept_run) = f.human("live-work-accept", &accept_text).await;
    let accepted = f.inspect(&work).await;
    f.save(
        "accepted-first-attempt",
        &json!({"view":accepted,"run":accept_run}),
    );
    assert_eq!(accepted["data"][0]["ticket"]["state"], "accepted");
    assert_eq!(
        accepted["data"][0]["ticket"]["accepted_submission"],
        s["id"]
    );
    f.browser_checkpoint("accepted", &accepted).await;
    let before = f.stopped_snapshot();
    f.save("canonical-before-restart", &json!(before));
    assert_eq!(before.tickets.len(), 1);
    assert_eq!(before.assignments.len(), 1);
    assert_eq!(before.submissions.len(), 1);
    for id in ["live-work-create", "live-work-accept"] {
        let resolution = &before.resolutions[id];
        assert!(resolution
            .proposal
            .as_ref()
            .is_some_and(|p| !p.groups.is_empty()));
        assert!(json!(resolution)["groups"]
            .as_array()
            .unwrap()
            .iter()
            .all(|g| g["status"] == "committed"));
    }
    let requests = f.bridge.requests();
    f.save("requests-before-restart", &json!(requests));
    assert!(requests.iter().any(|r| r["role"] == "native_worker"));
    assert!(requests.iter().any(|r| r["role"] == "supervisor"));
    assert!(upstream_runs_finished(&requests, &f.bridge.model),
        "each exact selected-model payload must finish successfully within the existing bounded retries; attempts are retained");
    let metadata = std::fs::metadata(&file).unwrap();
    f.restart().await;
    let create_replay = post(&f.client, &f.base, "/chat", &create_request).await;
    let accept_replay = post(&f.client, &f.base, "/chat", &accept_request).await;
    assert_eq!(
        create_replay["ingress_seq"],
        create_run["admitted"]["ingress_seq"]
    );
    assert_eq!(
        accept_replay["ingress_seq"],
        accept_run["admitted"]["ingress_seq"]
    );
    assert_eq!(
        post(&f.client, &f.base, "/tickets/update", &grant).await,
        granted
    );
    let dispatch_replay = post(&f.client, &f.base, "/tickets/dispatch", &dispatch).await;
    assert_eq!(dispatch_replay["receipt"], started["receipt"]);
    assert_eq!(
        dispatch_replay["runtime"][0]["observation"]["receipt"]["run_id"],
        s["runtime"]["run_id"]
    );
    let restarted = f.inspect(&work).await;
    f.browser_checkpoint("restarted", &restarted).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let after = f.stopped_snapshot();
    f.save("canonical-replay",&json!({"snapshot":after,"create":create_replay,"accept":accept_replay,"dispatch":dispatch_replay,"requests":f.bridge.requests()}));
    assert_eq!(json!(after.tickets), json!(before.tickets));
    assert_eq!(json!(after.assignments), json!(before.assignments));
    assert_eq!(json!(after.submissions), json!(before.submissions));
    assert_eq!(json!(after.resolutions), json!(before.resolutions));
    let replay_requests = f.bridge.requests();
    assert!(replay_keeps_model_execution(&requests, &replay_requests, &f.bridge.model),
        "replay must retain every model/Worker execution attempt; only validated catalog reads may be added");
    assert_eq!(std::fs::metadata(&file).unwrap().ino(), metadata.ino());
    assert_eq!(
        std::fs::metadata(&file).unwrap().mtime_nsec(),
        metadata.mtime_nsec()
    );
    assert_eq!(std::fs::read_to_string(&file).unwrap(), CODE);
    f.save("summary",&json!({"pass":true,"work_id":work,"submission_id":submission,"runtime":s["runtime"],
        "model":f.bridge.model,"transient_attempts":requests.iter().filter(|r| !r["model"].is_null() && r["status"] != 200).count(),
        "requests":requests,"replay_requests":replay_requests,"code_sha256":hash,"native_pid":pid,"native_pid_reaped":true,
        "explicit_user_coding_authority":true,"submitted_before_explicit_acceptance":true,"restart_exact_replay":true,
        "retained_owned_fixture":f.temp,"production_defaults_changed":false}));
    f.bridge.finish().await;
}
