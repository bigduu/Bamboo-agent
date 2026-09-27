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
}
struct Probe {
    case: Case,
    data: PathBuf,
    workspace: PathBuf,
    root: AtomicUsize,
    child: AtomicUsize,
    requests: Mutex<Vec<Value>>,
    profile_path: PathBuf,
    release_child: AtomicBool,
    child_ready: tokio::sync::Notify,
}
fn role(case: Case) -> &'static str {
    match case {
        Case::Explorer => "explorer",
        Case::Reviewer => "reviewer",
        Case::Unknown => "unknown-label",
        _ => "implementer",
    }
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
                if probe.case == Case::Unknown {
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
                    assert_eq!(binding["source"], "project");
                    assert_eq!(binding["name"], role(probe.case));
                    assert_eq!(selected.model, child_model(probe.case));
                    assert_eq!(
                        selected.agent_runtime_state.as_ref().unwrap().read_only,
                        matches!(probe.case, Case::Explorer | Case::Reviewer)
                    );
                    assert!(selected.messages[0].content.contains("PROJECT_ROLE_V1"));
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
    if case != Case::Unknown {
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
    let probe = web::Data::new(Probe {
        case,
        data: data.clone(),
        workspace: workspace.clone(),
        root: AtomicUsize::new(0),
        child: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
        profile_path,
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
                assert!(
                    text.contains("PROJECT_ROLE_V1"),
                    "selected role must reach actual provider: {text}"
                );
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
        if matches!(case, Case::Narrow | Case::Explorer | Case::Reviewer) {
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
