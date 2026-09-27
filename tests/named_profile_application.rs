//! Real CLI Host and current_exe Bamboo worker; only the remote model is fake.
//! No fixture submits a resolved ceiling. Config and actual Project are inputs.
#![cfg(unix)]
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Case {
    Implementer,
    Explorer,
    Reviewer,
    Narrow,
    Unknown,
    ExplicitModel,
    Duplicate,
    Invalid,
    BuiltinImplementer,
    BuiltinExplorer,
    BuiltinReviewer,
}
struct Probe {
    case: Case,
    data: PathBuf,
    workspace: PathBuf,
    root: AtomicUsize,
    child: AtomicUsize,
    requests: Mutex<Vec<Value>>,
    profile_path: PathBuf,
    builtin_prompt: Option<String>,
    release_child: AtomicBool,
    child_ready: tokio::sync::Notify,
}
fn role(case: Case) -> &'static str {
    match case {
        Case::Explorer | Case::BuiltinExplorer => "explorer",
        Case::Reviewer | Case::BuiltinReviewer => "reviewer",
        Case::Unknown => "unknown-label",
        _ => "implementer",
    }
}
fn builtin(case: Case) -> bool {
    matches!(
        case,
        Case::BuiltinImplementer | Case::BuiltinExplorer | Case::BuiltinReviewer
    )
}
fn read_only(case: Case) -> bool {
    matches!(
        case,
        Case::Explorer | Case::Reviewer | Case::BuiltinExplorer | Case::BuiltinReviewer
    )
}
fn child_model(case: Case) -> &'static str {
    if case == Case::ExplicitModel {
        "explicit-child"
    } else {
        "native-child"
    }
}
fn definition(name: &str, body: &str, model: &str) -> String {
    format!("---\nschema_version: 1\nname: {name}\ndescription: A bounded test role\nmodel_hint: openai:{model}\ntools:\n  allow: [Read, Glob, Write]\n  deny: [Edit]\n---\n{body}\n")
}
fn call(name: &str, args: Value) -> (Value, &'static str) {
    (
        json!({"tool_calls":[{"index":0,"id":format!("call-{name}"),"type":"function","function":{"name":name,"arguments":args.to_string()}}]}),
        "tool_calls",
    )
}
async fn response(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let body = body.into_inner();
    probe.requests.lock().unwrap().push(body.clone());
    let (delta, finish) = if body["model"] == child_model(probe.case) {
        if !probe.release_child.load(Ordering::SeqCst) {
            loop {
                let wake = probe.child_ready.notified();
                if probe.release_child.load(Ordering::SeqCst) {
                    break;
                }
                wake.await;
            }
        }
        match probe.child.fetch_add(1, Ordering::SeqCst) {
            0 => call(
                "Write",
                json!({"file_path":probe.workspace.join("child-write.txt"),"content":"real native child"}),
            ),
            1 => call(
                "Read",
                json!({"file_path":probe.workspace.join("input.txt")}),
            ),
            2 => call("Glob", json!({"pattern":"*.txt","path":probe.workspace})),
            _ => (json!({"content":"NATIVE_CHILD_COMPLETED"}), "stop"),
        }
    } else if body["model"] == "native-root" && body["tools"].to_string().contains("SubAgent") {
        match probe.root.fetch_add(1, Ordering::SeqCst) {
            0 => call(
                "Write",
                json!({"file_path":probe.workspace.join("root-denied.txt"),"content":"forbidden root"}),
            ),
            1 => {
                let mut args = json!({"action":"create","title":"Profile child","responsibility":"Complete a bounded assignment with evidence","prompt":"Use the real file tools and report evidence","subagent_type":role(probe.case),"workspace":probe.workspace,"auto_run":false});
                if probe.case == Case::Unknown || builtin(probe.case) {
                    args["model"] = json!("openai:native-child");
                } else if probe.case == Case::ExplicitModel {
                    args["model"] = json!("openai:explicit-child");
                }
                call("SubAgent", args)
            }
            2 => {
                if matches!(probe.case, Case::Duplicate | Case::Invalid) {
                    return HttpResponse::Ok().content_type("text/event-stream").body(format!("data: {}\n\ndata: [DONE]\n\n", json!({"id":"profile-rejected","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"NATIVE_ROOT_COMPLETED"},"finish_reason":"stop"}]})));
                }
                let disk = SessionStoreV2::new(probe.data.clone()).await.unwrap();
                let child = disk
                    .list_index_entries()
                    .await
                    .into_iter()
                    .find(|entry| entry.parent_session_id.as_deref() == Some("native-root"))
                    .expect("real Child creation")
                    .id;
                if probe.case != Case::Unknown {
                    // Replace the actual source after durable creation, before launch.
                    std::fs::write(
                        &probe.profile_path,
                        definition(
                            role(probe.case),
                            "RELOADED_ROLE_MUST_NOT_APPLY",
                            "wrong-reloaded-model",
                        ),
                    )
                    .unwrap();
                    let selected = disk.load_session(&child).await.unwrap().unwrap();
                    let binding: Value =
                        serde_json::from_str(&selected.metadata["child.named_profile.v1"]).unwrap();
                    assert_eq!(
                        binding["source"],
                        if builtin(probe.case) {
                            "builtin"
                        } else {
                            "project"
                        }
                    );
                    if builtin(probe.case) {
                        assert!(binding["project_id"].is_null());
                        assert_eq!(
                            binding["scope_project_id"],
                            selected.project_id_meta().unwrap()
                        );
                        assert!(selected.messages[0]
                            .content
                            .contains(probe.builtin_prompt.as_ref().unwrap()));
                        assert!(!binding
                            .to_string()
                            .contains(probe.builtin_prompt.as_ref().unwrap()));
                    }
                    assert_eq!(binding["name"], role(probe.case));
                    assert_eq!(selected.model, child_model(probe.case));
                    assert_eq!(
                        selected.agent_runtime_state.as_ref().unwrap().read_only,
                        read_only(probe.case)
                    );
                    if !builtin(probe.case) {
                        assert!(selected.messages[0].content.contains("PROJECT_ROLE_V1"));
                    }
                    assert!(!binding.to_string().contains("PROJECT_ROLE_V1"));
                }
                call(
                    "SubAgent",
                    json!({"action":"run","child_session_id":child,"reset_to_last_user":false}),
                )
            }
            _ => (json!({"content":"NATIVE_ROOT_COMPLETED"}), "stop"),
        }
    } else {
        (json!({"content":"auxiliary response"}), "stop")
    };
    let event = json!({"id":"native-response","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}
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
            .env("HOME", data.join("home"))
            .env("BAMBOO_JIANDU_DATA_DIR", data.join("jiandu"))
            .env("RUST_LOG", "warn")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}
async fn fixture(case: Case) {
    eprintln!("actual named profile case {case:?}");
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let data = root.join("host");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("input.txt"), "ACTUAL_READ_EVIDENCE").unwrap();
    let skill = workspace.join(".bamboo/skills/ceiling-forbidden");
    std::fs::create_dir_all(&skill).unwrap();
    std::fs::write(skill.join("SKILL.md"), "---\nname: ceiling-forbidden\ndescription: Use the authorized native surface\n---\nWORKSPACE_SKILL_SHOULD_NOT_APPLY\n").unwrap();
    let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
    let project = projects
        .create_with_project_path("profile", None, workspace.to_string_lossy(), vec![])
        .unwrap();
    let global = data.join("agents");
    let local = projects.paths().project_home(&project.id).join("agents");
    std::fs::create_dir_all(&global).unwrap();
    std::fs::create_dir_all(&local).unwrap();
    let profile_path = local.join(format!("{}.md", role(case)));
    if case != Case::Unknown && !builtin(case) {
        std::fs::write(
            global.join(format!("{}.md", role(case))),
            definition(
                role(case),
                "SHADOWED_GLOBAL_MUST_NOT_APPLY",
                "wrong-global-model",
            ),
        )
        .unwrap();
        std::fs::write(&profile_path, definition(role(case), "PROJECT_ROLE_V1: Keep the custom base and report concrete evidence; stop at the assigned boundary.", "native-child")).unwrap();
        if case == Case::Duplicate {
            std::fs::copy(&profile_path, local.join("second.md")).unwrap();
        } else if case == Case::Invalid {
            std::fs::write(&profile_path, "---\nschema_version: 1\nname: implementer\nmalformed: true\n---\nINVALID_ROLE_BODY\n").unwrap();
        }
    }
    let builtin_prompt = if builtin(case) {
        use bamboo_skills::named_agents::{NamedAgentLimits, ScopedNamedAgentCatalog};
        let project_home = projects.paths().project_home(&project.id);
        let catalog = ScopedNamedAgentCatalog::discover_with_builtins(
            &data,
            Some((&project.id, &project_home)),
            NamedAgentLimits::default(),
        )
        .unwrap();
        let identity = catalog
            .metadata()
            .entries
            .iter()
            .find_map(|row| row.identity.as_ref().filter(|id| id.name == role(case)))
            .unwrap();
        Some(catalog.get(identity).unwrap().system_prompt().to_owned())
    } else {
        None
    };
    let probe = web::Data::new(Probe {
        case,
        data: data.clone(),
        workspace: workspace.clone(),
        root: AtomicUsize::new(0),
        child: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
        profile_path,
        builtin_prompt,
        release_child: AtomicBool::new(false),
        child_ready: Default::default(),
    });
    let server_probe = probe.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(server_probe.clone())
            .route("/v1/chat/completions", web::post().to(response))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok()
                        .json(json!({"data":[{"id":"native-root"},{"id":"native-child"},{"id":"explicit-child"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let url = format!("http://{}/v1", server.addrs()[0]);
    let running = server.run();
    let handle = running.handle();
    actix_web::rt::spawn(running);
    let mut config = json!({"provider":"openai","features":{"provider_model_ref":true},"providers":{"openai":{"api_key":"fixture-key","base_url":url,"model":"native-root"}},"defaults":{"chat":{"provider":"openai","model":"native-root"}},"subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1}});
    if case == Case::Narrow {
        config["tools"] = json!({"disabled":["Bash","Edit","Write"]});
    }
    std::fs::write(
        data.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut host = start(&data, port);
    let base = format!("http://127.0.0.1:{port}/api/v1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "actual Host exited: {}",
                std::fs::read_to_string(data.join("host.log")).unwrap()
            );
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
    .unwrap();
    let created = client.post(format!("{base}/chat")).json(&json!({"session_id":"native-root","message":"Delegate the authorized implementation","model":"native-root","provider":"openai","model_ref":{"provider":"openai","model":"native-root"},"thinking_mode":"ultra","permission_mode":"bypass","workspace_path":workspace,"project_id":project.id.clone()})).send().await.unwrap();
    assert!(
        created.status().is_success(),
        "chat: {}",
        created.text().await.unwrap()
    );
    let executed = client
        .post(format!("{base}/execute/native-root"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert!(executed.status().is_success());
    let store = SessionStoreV2::new(data.clone()).await.unwrap();
    let mut seen_wait = false;
    let parent = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "Host exited: {}",
                std::fs::read_to_string(data.join("host.log")).unwrap()
            );
            let parent = store.load_session("native-root").await.unwrap().unwrap();
            if parent
                .agent_runtime_state
                .as_ref()
                .is_some_and(|state| state.waiting_for_children.is_some())
            {
                seen_wait = true;
                probe.release_child.store(true, Ordering::SeqCst);
                probe.child_ready.notify_waiters();
            }
            if parent
                .messages
                .iter()
                .any(|m| m.content.contains("NATIVE_ROOT_COMPLETED"))
                || parent.last_run_status().as_deref() == Some("error")
            {
                break parent;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|e| {
        panic!(
            "{case:?}: {e}; calls root={}, child={}; {}",
            probe.root.load(Ordering::SeqCst),
            probe.child.load(Ordering::SeqCst),
            std::fs::read_to_string(data.join("host.log")).unwrap()
        )
    });
    assert!(!workspace.join("root-denied.txt").exists());
    {
        assert!(
            parent
                .messages
                .iter()
                .any(|m| m.content.contains("NATIVE_ROOT_COMPLETED")),
            "Root error: {:?}",
            parent.last_run_error()
        );
        let requests = probe.requests.lock().unwrap().clone();
        let child: Vec<_> = requests
            .iter()
            .filter(|r| r["model"] == child_model(case))
            .collect();
        if matches!(case, Case::Duplicate | Case::Invalid) {
            assert!(
                child.is_empty(),
                "invalid/duplicate Project profile cannot fall back to Global"
            );
            assert!(!workspace.join("child-write.txt").exists());
            let children = store.list_index_entries().await;
            assert!(!children
                .iter()
                .any(|entry| entry.parent_session_id.as_deref() == Some("native-root")));
            assert!(parent
                .messages
                .iter()
                .any(|message| message.content.contains("named_profile_")));
            drop(requests);
            drop(host);
            handle.stop(true).await;
            return;
        }
        assert!(child.len() >= 2);
        if case != Case::Unknown {
            for request in &child {
                let text = request["messages"].to_string();
                if builtin(case) {
                    assert!(
                        request["messages"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|m| m["content"]
                                .as_str()
                                .is_some_and(|content| content
                                    .contains(probe.builtin_prompt.as_ref().unwrap()))),
                        "complete builtin role must reach actual provider"
                    );
                } else {
                    assert!(
                        text.contains("PROJECT_ROLE_V1"),
                        "selected role must reach actual provider: {text}"
                    );
                }
                assert!(!text.contains("RELOADED_ROLE_MUST_NOT_APPLY"));
                assert!(!text.contains("SHADOWED_GLOBAL_MUST_NOT_APPLY"));
            }
        }
        if case != Case::Unknown {
            assert!(child
                .iter()
                .all(|r| !r.to_string().contains("WORKSPACE_SKILL_SHOULD_NOT_APPLY")));
        }
        let names: std::collections::BTreeSet<_> = child[0]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["function"]["name"].as_str().unwrap())
            .collect();
        assert!(
            seen_wait,
            "actual worker provider waited behind the durable Root wait"
        );
        let root_calls: Vec<_> = requests
            .iter()
            .filter(|r| r["model"] == "native-root" && r["tools"].to_string().contains("SubAgent"))
            .collect();
        assert!(root_calls.iter().all(|r| !r["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["function"]["name"] == "Write")));
        if case == Case::Narrow || read_only(case) {
            assert_eq!(names, std::collections::BTreeSet::from(["Glob", "Read"]));
            assert!(!workspace.join("child-write.txt").exists());
            let history = child.last().unwrap()["messages"].to_string();
            assert!(history.contains("ACTUAL_READ_EVIDENCE"));
            assert!(history.contains("input.txt"));
        } else {
            assert_eq!(
                std::fs::read_to_string(workspace.join("child-write.txt")).unwrap(),
                "real native child"
            );
            if matches!(case, Case::Implementer | Case::ExplicitModel) {
                assert_eq!(
                    names,
                    std::collections::BTreeSet::from(["Glob", "Read", "Write"])
                );
            } else if case == Case::BuiltinImplementer {
                assert_eq!(
                    names,
                    std::collections::BTreeSet::from(["Bash", "Edit", "Glob", "Read", "Write"])
                );
            } else {
                assert!(
                    names.contains("Grep"),
                    "unknown-name legacy surface unchanged"
                );
            }
        }
        assert!(parent
            .agent_runtime_state
            .as_ref()
            .is_none_or(|state| state.waiting_for_children.is_none()));
    }
    drop(host);
    handle.stop(true).await;
}
#[actix_web::test]
async fn actual_named_profiles_reach_provider_and_native_dispatch() {
    for case in [
        Case::Implementer,
        Case::Explorer,
        Case::Reviewer,
        Case::Narrow,
        Case::Unknown,
        Case::ExplicitModel,
        Case::Duplicate,
        Case::Invalid,
    ] {
        Box::pin(fixture(case)).await;
    }
}

#[actix_web::test]
async fn actual_builtin_roles_reach_provider_and_native_dispatch() {
    for case in [
        Case::BuiltinImplementer,
        Case::BuiltinExplorer,
        Case::BuiltinReviewer,
    ] {
        Box::pin(fixture(case)).await;
    }
}
// Deterministic provider script, not a claim of autonomous bug discovery.
const HANDOFF_ROOT: &str = "handoff-root";
const WRONG: &str = "answer=41\n";
const CORRECT: &str = "answer=42\n";
const CORRECTION: &str = "Change only answer.txt to exactly answer=42 followed by one newline. The independent reviewer read answer=41; do not change any other file. Report the actual correction as JSON.";
const IMPLEMENTER_PROMPT: &str = "Implement only answer.txt with answer=42 followed by one newline. Report concrete verification in the supplied version-1 JSON result format; do not modify any other file.";

#[derive(Default)]
struct HandoffState {
    implementer: String,
    reviewer: String,
    implementer_binding: Value,
    reviewer_binding: Value,
    frozen: Vec<(String, String)>,
    reports: Vec<Value>,
}
struct HandoffProbe {
    data: PathBuf,
    workspace: PathBuf,
    profiles: PathBuf,
    project_id: String,
    root: AtomicUsize,
    implementer: AtomicUsize,
    reviewer: AtomicUsize,
    state: Mutex<HandoffState>,
    requests: Mutex<Vec<Value>>,
}
fn handoff_call(id: &str, name: &str, args: Value) -> (Value, &'static str) {
    let (mut delta, finish) = call(name, args);
    delta["tool_calls"][0]["id"] = json!(id);
    (delta, finish)
}
fn handoff_report(outcome: &str, summary: &str, evidence: &str) -> Value {
    json!({"version":1,"outcome":outcome,"summary":summary,
        "reported_evidence":[{"description":evidence,"reference":"answer.txt","sha256":null}],
        "reported_verification":[{"check":"answer.txt equals answer=42 newline","reported_status":if outcome=="blocked" {"failed"} else {"passed"},"details":evidence}],
        "proposals":[],"blockers":if outcome=="blocked" {vec!["Parent must correct answer.txt"]} else {vec![]},"open_decisions":[]})
}
fn handoff_tool<'a>(body: &'a Value, id: &str) -> &'a str {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|m| m["role"] == "tool" && m["tool_call_id"] == id)
        .and_then(|m| m["content"].as_str())
        .unwrap_or_else(|| panic!("missing actual tool result {id}"))
}
fn handoff_public(value: &Value) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                assert!(
                    ![
                        "worker_id",
                        "host_id",
                        "mailbox",
                        "mailbox_id",
                        "endpoint",
                        "physical_child_id",
                        "placement_ref",
                        "fabric_dir",
                        "bus_token",
                    ]
                    .contains(&key.as_str()),
                    "physical key in public result: {key}"
                );
                handoff_public(value);
            }
        }
        Value::Array(values) => values.iter().for_each(handoff_public),
        _ => {}
    }
}
fn handoff_result(body: &Value, id: &str) -> Value {
    let text = handoff_tool(body, id);
    assert!(text.len() <= 8192, "bounded actual Root tool result");
    let value: Value = serde_json::from_str(text).unwrap_or_else(|error| {
        panic!(
            "{id}: {error}; {}",
            text.chars().take(512).collect::<String>()
        )
    });
    handoff_public(&value);
    assert!(
        !text.contains("required-worker-"),
        "physical worker alias leaked"
    );
    value
}
fn handoff_get(child: &str, binding: &Value) -> Value {
    json!({"action":"get","child_session_id":child,"view":"typed_result",
        "expected_child_created_at":binding["child_created_at"],
        "expected_assignment_sha256":binding["assignment_sha256"]})
}
async fn handoff_freeze(probe: &HandoffProbe, child: &str, role: &str) {
    let store = SessionStoreV2::new(probe.data.clone()).await.unwrap();
    let selected = store.load_session(child).await.unwrap().unwrap();
    assert_eq!(selected.parent_session_id.as_deref(), Some(HANDOFF_ROOT));
    assert_eq!(selected.project_id_meta(), Some(probe.project_id.clone()));
    assert_eq!(selected.model, format!("handoff-{role}"));
    assert_eq!(
        selected.agent_runtime_state.as_ref().unwrap().read_only,
        role == "reviewer"
    );
    let raw = selected.metadata["child.named_profile.v1"].clone();
    let binding: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(binding["source"], "project");
    assert_eq!(binding["name"], role);
    assert!(selected.messages[0]
        .content
        .contains(&format!("HANDOFF_{}_V1", role.to_uppercase())));
    probe.state.lock().unwrap().frozen.push((child.into(), raw));
    std::fs::write(
        probe.profiles.join(format!("{role}.md")),
        definition(
            role,
            "HANDOFF_RELOADED_MUST_NOT_APPLY",
            "wrong-reloaded-model",
        ),
    )
    .unwrap();
}
async fn handoff_response(body: web::Json<Value>, probe: web::Data<HandoffProbe>) -> HttpResponse {
    let body = body.into_inner();
    probe.requests.lock().unwrap().push(body.clone());
    let model = body["model"].as_str().unwrap();
    let (delta, finish) = match model {
        "handoff-implementer" => {
            let step = probe.implementer.fetch_add(1, Ordering::SeqCst);
            match step {
                0 => handoff_call(
                    "implementer-wrong",
                    "Write",
                    json!({"file_path":probe.workspace.join("answer.txt"),"content":WRONG}),
                ),
                1 => {
                    assert!(!handoff_tool(&body, "implementer-wrong").starts_with("Error:"));
                    assert_eq!(
                        std::fs::read_to_string(probe.workspace.join("answer.txt")).unwrap(),
                        WRONG
                    );
                    (
                        json!({"content":handoff_report("completed", "Implementation complete; all checks passed", "I claim answer.txt contains answer=42").to_string()}),
                        "stop",
                    )
                }
                2 => {
                    assert!(
                        body["messages"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|m| m["role"] == "user"
                                && m["content"].as_str().is_some_and(|s| s == CORRECTION)),
                        "explicit correction reaches the same Child unchanged"
                    );
                    handoff_call(
                        "implementer-correction-read",
                        "Read",
                        json!({"file_path":probe.workspace.join("answer.txt")}),
                    )
                }
                3 => {
                    let read = handoff_tool(&body, "implementer-correction-read");
                    assert!(
                        !read.starts_with("Error:") && read.contains("answer=41"),
                        "actual correction Read result: {}",
                        read.chars().take(512).collect::<String>()
                    );
                    handoff_call(
                        "implementer-correct",
                        "Write",
                        json!({"file_path":probe.workspace.join("answer.txt"),"content":CORRECT}),
                    )
                }
                4 => {
                    assert!(
                        !handoff_tool(&body, "implementer-correct").starts_with("Error:"),
                        "actual implementer-correct result: {}",
                        handoff_tool(&body, "implementer-correct")
                            .chars()
                            .take(512)
                            .collect::<String>()
                    );
                    (
                        json!({"content":handoff_report("completed", "Narrow correction applied", "answer.txt now contains answer=42").to_string()}),
                        "stop",
                    )
                }
                _ => panic!("unexpected implementer request {step}"),
            }
        }
        "handoff-reviewer" => {
            let step = probe.reviewer.fetch_add(1, Ordering::SeqCst);
            match step {
                0 | 4 => handoff_call(
                    if step == 0 {
                        "review-glob"
                    } else {
                        "recheck-glob"
                    },
                    "Glob",
                    json!({"path":probe.workspace,"pattern":"answer.txt"}),
                ),
                1 | 5 => {
                    assert!(handoff_tool(
                        &body,
                        if step == 1 {
                            "review-glob"
                        } else {
                            "recheck-glob"
                        }
                    )
                    .contains("answer.txt"));
                    handoff_call(
                        if step == 1 {
                            "review-read"
                        } else {
                            "recheck-read"
                        },
                        "Read",
                        json!({"file_path":probe.workspace.join("answer.txt")}),
                    )
                }
                2 => {
                    assert!(handoff_tool(&body, "review-read").contains("answer=41"));
                    handoff_call(
                        "review-write-denied",
                        "Write",
                        json!({"file_path":probe.workspace.join("answer.txt"),"content":"REVIEWER_MUST_NOT_WRITE"}),
                    )
                }
                3 => {
                    let rejected = handoff_tool(&body, "review-write-denied");
                    assert!(
                        rejected == "Error: Tool 'Write' is not callable at the current conversation position",
                        "actual Write result: {}",
                        rejected.chars().take(512).collect::<String>()
                    );
                    assert_eq!(
                        std::fs::read_to_string(probe.workspace.join("answer.txt")).unwrap(),
                        WRONG
                    );
                    (
                        json!({"content":handoff_report("blocked", "Implementer claim contradicted by actual file", "Glob found answer.txt; Read observed answer=41, not answer=42. Write was rejected: Tool 'Write' is not callable at the current conversation position.").to_string()}),
                        "stop",
                    )
                }
                6 => {
                    assert!(handoff_tool(&body, "recheck-read").contains("answer=42"));
                    (
                        json!({"content":handoff_report("completed", "Independent recheck passed", "Glob found answer.txt; Read observed answer=42 after the parent correction").to_string()}),
                        "stop",
                    )
                }
                _ => panic!("unexpected reviewer request {step}"),
            }
        }
        "handoff-root" if body["tools"].to_string().contains("SubAgent") => {
            let step = probe.root.fetch_add(1, Ordering::SeqCst);
            match step {
                0 => handoff_call(
                    "create-implementer",
                    "SubAgent",
                    json!({"action":"create","title":"Bounded implementer","responsibility":"Implement only answer.txt and report verification","prompt":format!("{IMPLEMENTER_PROMPT} Report JSON shape: {}", handoff_report("completed", "Actual summary", "Actual evidence")),"subagent_type":"implementer","workspace":probe.workspace,"auto_run":false}),
                ),
                1 => {
                    let value = handoff_result(&body, "create-implementer");
                    let child = value["child_session_id"].as_str().unwrap().to_owned();
                    probe.state.lock().unwrap().implementer = child.clone();
                    handoff_freeze(&probe, &child, "implementer").await;
                    handoff_call(
                        "run-implementer",
                        "SubAgent",
                        json!({"action":"run","child_session_id":child,"reset_to_last_user":false}),
                    )
                }
                2 | 6 => {
                    let state = probe.state.lock().unwrap();
                    let child = if step == 2 {
                        &state.implementer
                    } else {
                        &state.reviewer
                    };
                    handoff_call(
                        if step == 2 {
                            "binding-implementer"
                        } else {
                            "binding-reviewer"
                        },
                        "SubAgent",
                        json!({"action":"get","child_session_id":child,"view":"result_binding"}),
                    )
                }
                3 | 7 => {
                    let binding = handoff_result(
                        &body,
                        if step == 3 {
                            "binding-implementer"
                        } else {
                            "binding-reviewer"
                        },
                    );
                    assert_eq!(binding["available"], true);
                    let mut state = probe.state.lock().unwrap();
                    let args = if step == 3 {
                        state.implementer_binding = binding;
                        handoff_get(&state.implementer, &state.implementer_binding)
                    } else {
                        state.reviewer_binding = binding;
                        handoff_get(&state.reviewer, &state.reviewer_binding)
                    };
                    handoff_call(
                        if step == 3 {
                            "report-implementer"
                        } else {
                            "report-reviewer"
                        },
                        "SubAgent",
                        args,
                    )
                }
                4 => {
                    let value = handoff_result(&body, "report-implementer");
                    assert_eq!(value["available"], true);
                    assert_eq!(
                        value["child_report"]["summary"],
                        "Implementation complete; all checks passed"
                    );
                    assert_eq!(value["host_observation"]["last_run_status"], "completed");
                    probe.state.lock().unwrap().reports.push(value.clone());
                    let assignment = format!("Independently challenge this implementer claim: {}. Review only answer.txt against answer=42 newline. Use Read and Glob; report observed evidence, do not implement or trust the claim. Return exactly this JSON shape with your actual evidence: {}.", value["child_report"], handoff_report("blocked", "Actual review summary", "Actual evidence"));
                    handoff_call(
                        "create-reviewer",
                        "SubAgent",
                        json!({"action":"create","title":"Independent adversarial reviewer","responsibility":"Check only answer.txt without changing it","prompt":assignment,"subagent_type":"reviewer","workspace":probe.workspace,"auto_run":false}),
                    )
                }
                5 => {
                    let value = handoff_result(&body, "create-reviewer");
                    let child = value["child_session_id"].as_str().unwrap().to_owned();
                    {
                        let mut state = probe.state.lock().unwrap();
                        assert_ne!(child, state.implementer);
                        state.reviewer = child.clone();
                    }
                    handoff_freeze(&probe, &child, "reviewer").await;
                    handoff_call(
                        "run-reviewer",
                        "SubAgent",
                        json!({"action":"run","child_session_id":child,"reset_to_last_user":false}),
                    )
                }
                8 => {
                    let value = handoff_result(&body, "report-reviewer");
                    assert_eq!(value["available"], true);
                    assert_eq!(value["child_report"]["outcome"], "blocked");
                    assert!(value["child_report"]["reported_evidence"][0]["description"]
                        .as_str()
                        .unwrap()
                        .contains("Read observed answer=41"));
                    let mut state = probe.state.lock().unwrap();
                    state.reports.push(value);
                    handoff_call(
                        "correct-implementer",
                        "SubAgent",
                        json!({"intent":"chat","target":state.implementer,"message":CORRECTION}),
                    )
                }
                9 => {
                    let result = handoff_result(&body, "correct-implementer");
                    let state = probe.state.lock().unwrap();
                    assert_eq!(result["actor_id"], state.implementer);
                    handoff_call(
                        "corrected-report",
                        "SubAgent",
                        handoff_get(&state.implementer, &state.implementer_binding),
                    )
                }
                10 => {
                    let value = handoff_result(&body, "corrected-report");
                    assert_eq!(value["available"], true);
                    assert_eq!(
                        value["child_report"]["summary"],
                        "Narrow correction applied"
                    );
                    let mut state = probe.state.lock().unwrap();
                    state.reports.push(value);
                    handoff_call(
                        "recheck-reviewer",
                        "SubAgent",
                        json!({"intent":"chat","target":state.reviewer,"message":"Recheck only answer.txt using Read and Glob against answer=42 newline after the parent correction; do not change files. Return the version-1 JSON report."}),
                    )
                }
                11 => {
                    let result = handoff_result(&body, "recheck-reviewer");
                    let state = probe.state.lock().unwrap();
                    assert_eq!(result["actor_id"], state.reviewer);
                    handoff_call(
                        "final-review",
                        "SubAgent",
                        handoff_get(&state.reviewer, &state.reviewer_binding),
                    )
                }
                12 => {
                    let value = handoff_result(&body, "final-review");
                    assert_eq!(value["available"], true);
                    assert_eq!(
                        value["child_report"]["summary"],
                        "Independent recheck passed"
                    );
                    probe.state.lock().unwrap().reports.push(value);
                    (json!({"content":"HANDOFF_ROOT_COMPLETED"}), "stop")
                }
                _ => panic!("unexpected Root request {step}"),
            }
        }
        _ => (json!({"content":"auxiliary response"}), "stop"),
    };
    let event = json!({"id":"handoff-response","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}

async fn cold_handoff_report(data: &Path, child: &bamboo_domain::Session) {
    use bamboo_agent::server::{
        app_state::AppState,
        tools::{ChildSessionAdapter, SubAgentTool},
    };
    use bamboo_agent_core::tools::{Tool, ToolExecutionContext, ToolOutcome};
    use std::sync::Arc;
    let state = AppState::new(data.to_path_buf()).await.unwrap();
    let adapter = Arc::new(ChildSessionAdapter::new(
        state.session_store.clone(),
        state.storage.clone(),
        state.persistence.clone(),
        state.spawn_scheduler.clone(),
        Arc::default(),
        Arc::default(),
        Arc::default(),
        None,
        None,
        state.config.clone(),
    ));
    let tool = SubAgentTool::new(adapter.clone(), adapter);
    let binding = bamboo_domain::ChildContextBinding::from_session(child)
        .unwrap()
        .unwrap();
    let args = handoff_get(
        &child.id,
        &json!({"child_created_at":child.created_at,"assignment_sha256":binding.assignment_sha256}),
    );
    let ctx = ToolExecutionContext {
        session_id: Some(HANDOFF_ROOT),
        root_session_id: None,
        tool_call_id: "cold-handoff",
        executing_supervisor: None,
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        auto_approve_permissions: false,
        plan_read_only: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    }
    .to_tool_ctx();
    let ToolOutcome::Completed(result) = Box::pin(tool.invoke(args, ctx)).await.unwrap() else {
        panic!("cold typed report must complete")
    };
    assert!(
        serde_json::to_vec(&result).unwrap().len() <= 8192,
        "entire serialized actual ToolResult"
    );
    let value: Value = serde_json::from_str(&result.result).unwrap();
    handoff_public(&value);
    assert!(!result.result.contains("required-worker-"));
    assert_eq!(value["available"], true);
    assert_eq!(value["host_observation"]["child_session_id"], child.id);
    assert_eq!(
        value["host_observation"]["child_created_at"],
        json!(child.created_at)
    );
    assert_eq!(value["child_report"]["outcome"], "completed");
}

#[actix_web::test]
async fn actual_implementer_reviewer_bounded_handoff_and_correction() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let data = root.join("host");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(
        workspace.join("untouched.txt"),
        "OUTSIDE_CORRECTION_MUST_REMAIN",
    )
    .unwrap();
    let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
    let project = projects
        .create_with_project_path("handoff", None, workspace.to_string_lossy(), vec![])
        .unwrap();
    let profiles = projects.paths().project_home(&project.id).join("agents");
    std::fs::create_dir_all(&profiles).unwrap();
    std::fs::create_dir_all(data.join("agents")).unwrap();
    for role in ["implementer", "reviewer"] {
        std::fs::write(
            data.join("agents").join(format!("{role}.md")),
            definition(role, "HANDOFF_GLOBAL_MUST_NOT_APPLY", "wrong-global-model"),
        )
        .unwrap();
        std::fs::write(profiles.join(format!("{role}.md")),definition(role,
            &format!("HANDOFF_{}_V1: {}",role.to_uppercase(),if role=="reviewer" {
                "Challenge completion claims independently using actual Read and Glob. Do not change files or expand the assigned scope."
            } else {"Implement only the assigned file; preserve unrelated files and report concrete verification."}),
            &format!("handoff-{role}"))).unwrap();
    }
    let probe = web::Data::new(HandoffProbe {
        data: data.clone(),
        workspace: workspace.clone(),
        profiles,
        project_id: project.id.to_string(),
        root: AtomicUsize::new(0),
        implementer: AtomicUsize::new(0),
        reviewer: AtomicUsize::new(0),
        state: Mutex::default(),
        requests: Mutex::default(),
    });
    let server_probe = probe.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(server_probe.clone())
            .route("/v1/chat/completions", web::post().to(handoff_response))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok().json(json!({"data":[
            {"id":"handoff-root"},{"id":"handoff-implementer"},{"id":"handoff-reviewer"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let url = format!("http://{}/v1", server.addrs()[0]);
    let running = server.run();
    let handle = running.handle();
    actix_web::rt::spawn(running);
    let config = json!({"provider":"openai","features":{"provider_model_ref":true},
        "providers":{"openai":{"api_key":"fixture-key","base_url":url,"model":"handoff-root"}},
        "defaults":{"chat":{"provider":"openai","model":"handoff-root"}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1}});
    std::fs::write(
        data.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut host = start(&data, port);
    let base = format!("http://127.0.0.1:{port}/api/v1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "actual Host exited during startup"
            );
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
    .unwrap();
    let created=client.post(format!("{base}/chat")).json(&json!({"session_id":HANDOFF_ROOT,
        "message":"Implement answer.txt, independently review concrete evidence, and issue a narrow correction if the claim is wrong.",
        "model":"handoff-root","provider":"openai","model_ref":{"provider":"openai","model":"handoff-root"},
        "thinking_mode":"ultra","permission_mode":"bypass","workspace_path":workspace,"project_id":project.id.clone()}))
        .send().await.unwrap();
    assert!(
        created.status().is_success(),
        "chat: {}",
        created.text().await.unwrap()
    );
    let executed = client
        .post(format!("{base}/execute/{HANDOFF_ROOT}"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert!(
        executed.status().is_success(),
        "execute: {}",
        executed.text().await.unwrap()
    );
    let parent = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "actual Host exited during handoff"
            );
            // Fresh physical reader: the real Host owns index publication.
            let store = SessionStoreV2::new(data.clone()).await.unwrap();
            let parent = store.load_session(HANDOFF_ROOT).await.unwrap().unwrap();
            if parent
                .messages
                .iter()
                .any(|m| m.content == "HANDOFF_ROOT_COMPLETED")
                || parent.last_run_status().as_deref() == Some("error")
            {
                break parent;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "handoff timeout {error}; Root/implementer/reviewer={}/{}/{}; Host tail={}",
            probe.root.load(Ordering::SeqCst),
            probe.implementer.load(Ordering::SeqCst),
            probe.reviewer.load(Ordering::SeqCst),
            std::fs::read_to_string(data.join("host.log"))
                .unwrap()
                .chars()
                .rev()
                .take(2048)
                .collect::<String>()
                .chars()
                .rev()
                .collect::<String>()
        )
    });
    assert!(
        parent
            .messages
            .iter()
            .any(|m| m.content == "HANDOFF_ROOT_COMPLETED"),
        "Root error: {:?}",
        parent
            .last_run_error()
            .map(|s| s.chars().take(512).collect::<String>())
    );
    assert_eq!(probe.root.load(Ordering::SeqCst), 13);
    assert_eq!(probe.implementer.load(Ordering::SeqCst), 5);
    assert_eq!(probe.reviewer.load(Ordering::SeqCst), 7);
    assert_eq!(
        std::fs::read_to_string(workspace.join("answer.txt")).unwrap(),
        CORRECT
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join("untouched.txt")).unwrap(),
        "OUTSIDE_CORRECTION_MUST_REMAIN"
    );
    assert!(parent
        .agent_runtime_state
        .as_ref()
        .is_none_or(|r| r.waiting_for_children.is_none()));
    // Stop the real Host before cold storage and exact ToolResult readback.
    host.0.kill().unwrap();
    host.0.wait().unwrap();
    drop(host);
    let store = SessionStoreV2::new(data.clone()).await.unwrap();
    let entries = store.list_index_entries().await;
    let child_ids: Vec<_> = entries
        .iter()
        .filter(|e| e.parent_session_id.as_deref() == Some(HANDOFF_ROOT))
        .map(|e| e.id.clone())
        .collect();
    let (implementer, reviewer, frozen, reports) = {
        let state = probe.state.lock().unwrap();
        (
            state.implementer.clone(),
            state.reviewer.clone(),
            state.frozen.clone(),
            state.reports.clone(),
        )
    };
    assert_ne!(implementer, reviewer);
    assert_eq!(child_ids.len(), 2);
    assert!(child_ids.contains(&implementer) && child_ids.contains(&reviewer));
    assert_eq!(reports.len(), 4);
    assert_eq!(reports[0]["child_report"]["outcome"], "completed");
    assert_eq!(reports[1]["child_report"]["outcome"], "blocked");
    assert!(reports
        .iter()
        .all(
            |r| r["host_observation"]["current_project_id"] == json!(project.id)
                && r["host_observation"]["parent_session_id"] == HANDOFF_ROOT
                && r["host_observation"]["current_run_final_snapshot"] == true
        ));
    for (id, binding) in frozen {
        let child = store.load_session(&id).await.unwrap().unwrap();
        assert_eq!(child.parent_session_id.as_deref(), Some(HANDOFF_ROOT));
        assert_eq!(child.root_session_id, HANDOFF_ROOT);
        assert_eq!(child.spawn_depth, 1);
        assert_eq!(child.project_id_meta(), parent.project_id_meta());
        assert_eq!(
            child.metadata["child.named_profile.v1"], binding,
            "catalog reload cannot rebind a created Child"
        );
        assert_eq!(child.last_run_status().as_deref(), Some("completed"));
        assert_eq!(
            child.agent_runtime_state.as_ref().unwrap().read_only,
            id == reviewer
        );
        if id == implementer {
            assert!(child
                .messages
                .iter()
                .any(|m| m.role == bamboo_domain::Role::User && m.content == CORRECTION));
            assert!(child.messages.iter().any(|m| m
                .content
                .contains("Implementation complete; all checks passed")));
            assert!(child
                .messages
                .iter()
                .any(|m| m.tool_call_id.as_deref() == Some("implementer-wrong")));
            assert!(child
                .messages
                .iter()
                .any(|m| m.tool_call_id.as_deref() == Some("implementer-correct")));
        } else {
            let denied = child
                .messages
                .iter()
                .find(|m| m.tool_call_id.as_deref() == Some("review-write-denied"))
                .unwrap();
            assert_eq!(denied.role, bamboo_domain::Role::Tool);
            assert_eq!(denied.tool_success, Some(false));
            assert_eq!(
                denied.content,
                "Error: Tool 'Write' is not callable at the current conversation position"
            );
            assert!(child
                .messages
                .iter()
                .any(|m| m.tool_call_id.as_deref() == Some("review-read")
                    && m.content.contains("answer=41")));
            assert!(child
                .messages
                .iter()
                .any(|m| m.tool_call_id.as_deref() == Some("recheck-read")
                    && m.content.contains("answer=42")));
        }
        Box::pin(cold_handoff_report(&data, &child)).await;
    }
    let requests = probe.requests.lock().unwrap().clone();
    for (model, role) in [
        ("handoff-implementer", "implementer"),
        ("handoff-reviewer", "reviewer"),
    ] {
        let selected: Vec<_> = requests.iter().filter(|r| r["model"] == model).collect();
        assert!(!selected.is_empty());
        for request in selected {
            let text = request["messages"].to_string();
            assert!(text.contains(&format!("HANDOFF_{}_V1", role.to_uppercase())));
            assert!(!text.contains("HANDOFF_RELOADED_MUST_NOT_APPLY"));
            assert!(!text.contains("HANDOFF_GLOBAL_MUST_NOT_APPLY"));
            let names: std::collections::BTreeSet<_> = request["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["function"]["name"].as_str().unwrap())
                .collect();
            assert_eq!(
                names,
                if role == "reviewer" {
                    std::collections::BTreeSet::from(["Read", "Glob"])
                } else {
                    std::collections::BTreeSet::from(["Read", "Glob", "Write"])
                }
            );
        }
    }
    assert!(requests
        .iter()
        .filter(|r| r["model"] == "handoff-root" && r["tools"].is_array())
        .all(|r| r["tools"]
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["function"]["name"] != "Write")));
    handle.stop(true).await;
}
