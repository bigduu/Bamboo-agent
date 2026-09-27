//! Real CLI Host and current_exe Bamboo worker; only the remote model is fake.
//! No fixture submits a resolved ceiling. Config and actual Project are inputs.
#![cfg(unix)]
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::{ChildContextPacket, Role};
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
    Write,
    Unassigned,
    Narrow,
    Legacy,
    Archived,
    ForcedApproval,
}
struct Probe {
    case: Case,
    data: PathBuf,
    workspace: PathBuf,
    root: AtomicUsize,
    child: AtomicUsize,
    requests: Mutex<Vec<Value>>,
    project: bamboo_domain::ProjectId,
    release_child: AtomicBool,
    child_ready: tokio::sync::Notify,
    reviews: AtomicUsize,
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
    let (delta, finish) = if body["model"] == "native-child" {
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
            1 if probe.case == Case::Narrow => call(
                "Read",
                json!({"file_path":probe.workspace.join("input.txt")}),
            ),
            2 if probe.case == Case::Narrow => {
                call("Glob", json!({"pattern":"*.txt","path":probe.workspace}))
            }
            _ => (json!({"content":"NATIVE_CHILD_COMPLETED"}), "stop"),
        }
    } else if body["model"] == "native-root" && body["tools"].to_string().contains("SubAgent") {
        match probe.root.fetch_add(1, Ordering::SeqCst) {
            0 => call(
                "Write",
                json!({"file_path":probe.workspace.join("root-denied.txt"),"content":"forbidden root"}),
            ),
            1 => {
                let disk = SessionStoreV2::new(probe.data.clone()).await.unwrap();
                let root = disk.load_session("native-root").await.unwrap().unwrap();
                let source = root
                    .messages
                    .iter()
                    .find(|message| message.role == Role::User)
                    .unwrap()
                    .id
                    .clone();
                let mut args = json!({"action":"create","title":"Native child","responsibility":"Implement authorized task","prompt":"Use the real file tools","subagent_type":"worker","workspace":probe.workspace,"model":"openai:native-child","auto_run":false});
                if probe.case != Case::Legacy {
                    args["context_packet"] = serde_json::to_value(ChildContextPacket {
                        version: 1,
                        objective: "Use the authorized native surface".into(),
                        constraints: vec!["Do not expand the task".into()],
                        acceptance: vec!["Return concrete evidence".into()],
                        non_goals: vec!["No composition or nesting".into()],
                        necessary_user_instructions: vec!["Preserve boundaries".into()],
                        recorded_decisions: vec!["Host configured tool names".into()],
                        source_user_message_ids: vec![source],
                        background_message_ids: vec![],
                    })
                    .unwrap();
                }
                call("SubAgent", args)
            }
            2 => {
                let disk = SessionStoreV2::new(probe.data.clone()).await.unwrap();
                let child = disk
                    .list_index_entries()
                    .await
                    .into_iter()
                    .find(|entry| entry.parent_session_id.as_deref() == Some("native-root"))
                    .expect("real Child creation")
                    .id;
                if probe.case == Case::Archived {
                    let projects = bamboo_projects::ProjectStore::open(&probe.data).unwrap();
                    let current = projects.get(&probe.project).unwrap();
                    projects.archive(&probe.project, current.revision).unwrap();
                }
                call(
                    "SubAgent",
                    json!({"action":"run","child_session_id":child,"reset_to_last_user":false}),
                )
            }
            _ => (json!({"content":"NATIVE_ROOT_COMPLETED"}), "stop"),
        }
    } else if probe.case == Case::ForcedApproval
        && body["model"] == "native-root"
        && body["messages"]
            .to_string()
            .contains("parent agent's security reviewer")
    {
        // This is the actual off-loop parent model call. RespectSpecificWait
        // leaves the request pending while its Child is still awaiting review.
        let path = probe.data.join("sessions/native-root/inbox/new");
        let files: Vec<_> = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        let request = files
            .iter()
            .find_map(|path| {
                let value: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
                let envelope = value.get("body")?;
                (envelope["body"]["instruction"] == "direct_parent_forced_permission_request_v1")
                    .then(|| envelope.clone())
            })
            .expect("actual durable parent request exists before reviewer provider call");
        assert_eq!(request["target_session_id"], "native-root");
        assert_eq!(request["body"]["data"]["reason"], "configured_always_ask");
        assert_eq!(request["body"]["data"]["parent_session_id"], "native-root");
        assert!(uuid::Uuid::parse_str(
            request["body"]["data"]["request_generation"]
                .as_str()
                .unwrap()
        )
        .is_ok());
        assert!(!probe.workspace.join("child-write.txt").exists());
        probe.reviews.fetch_add(1, Ordering::SeqCst);
        (json!({"content":"DENY"}), "stop")
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
    eprintln!("actual native tool ceiling case {case:?}");
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
    let project_workspace = if matches!(case, Case::Unassigned | Case::Legacy) {
        root.join("unrelated-project")
    } else {
        workspace.clone()
    };
    std::fs::create_dir_all(&project_workspace).unwrap();
    let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
    let project = projects
        .create_with_project_path("native", None, project_workspace.to_string_lossy(), vec![])
        .unwrap();
    let probe = web::Data::new(Probe {
        case,
        data: data.clone(),
        workspace: workspace.clone(),
        root: AtomicUsize::new(0),
        child: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
        project: project.id.clone(),
        release_child: AtomicBool::new(false),
        child_ready: Default::default(),
        reviews: AtomicUsize::new(0),
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
                        .json(json!({"data":[{"id":"native-root"},{"id":"native-child"}]}))
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
    if case == Case::ForcedApproval {
        let policy = bamboo_tools::permission::PermissionConfig::new();
        policy.set_ask_rules(["Write(*)".into()]);
        bamboo_tools::permission::PermissionStorage::new(&data)
            .save(&policy)
            .await
            .unwrap();
    }
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
    let created = client.post(format!("{base}/chat")).json(&json!({"session_id":"native-root","message":"Delegate the authorized implementation","model":"native-root","provider":"openai","model_ref":{"provider":"openai","model":"native-root"},"thinking_mode":"ultra","permission_mode":"bypass","workspace_path":workspace,"project_id": if matches!(case,Case::Unassigned|Case::Legacy) {None} else {Some(project.id.clone())}})).send().await.unwrap();
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
    if case == Case::Archived {
        assert!(probe.root.load(Ordering::SeqCst) >= 3);
        assert_eq!(
            projects.get(&project.id).unwrap().status,
            bamboo_domain::ProjectStatus::Archived
        );
        assert_eq!(
            probe.child.load(Ordering::SeqCst),
            0,
            "closed actual Project cannot launch provider"
        );
        assert!(!workspace.join("child-write.txt").exists());
    } else {
        assert!(
            parent
                .messages
                .iter()
                .any(|m| m.content.contains("NATIVE_ROOT_COMPLETED")),
            "Root error: {:?}",
            parent.last_run_error()
        );
        let requests = probe.requests.lock().unwrap();
        let child: Vec<_> = requests
            .iter()
            .filter(|r| r["model"] == "native-child")
            .collect();
        assert!(child.len() >= 2);
        if case != Case::Legacy {
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
        if case == Case::ForcedApproval {
            assert_eq!(probe.reviews.load(Ordering::SeqCst), 1);
            assert!(!workspace.join("child-write.txt").exists());
            let history = child.last().unwrap()["messages"].to_string();
            assert!(
                history.contains("Permission denied by host")
                    || history.contains("denied by parent agent review")
            );
        } else if case == Case::Narrow {
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
            if matches!(case, Case::Write | Case::Unassigned) {
                assert_eq!(
                    names,
                    std::collections::BTreeSet::from(["Bash", "Edit", "Glob", "Read", "Write"])
                );
            } else {
                assert!(names.contains("Grep"), "legacy surface unchanged");
            }
        }
        assert!(parent
            .agent_runtime_state
            .as_ref()
            .is_none_or(|state| state.waiting_for_children.is_none()));
    }
    drop(host);
    if case == Case::ForcedApproval {
        let cold = SessionStoreV2::new(data.clone()).await.unwrap();
        let parent = cold.load_session("native-root").await.unwrap().unwrap();
        assert!(parent.messages.iter().any(|message| message
            .content
            .contains("requests a forced permission decision")));
        assert!(!workspace.join("child-write.txt").exists());
    }
    handle.stop(true).await;
}
#[actix_web::test]
async fn actual_host_and_fresh_worker_native_ceiling_cases() {
    for case in [
        Case::Write,
        Case::Narrow,
        Case::Unassigned,
        Case::Legacy,
        Case::Archived,
    ] {
        Box::pin(fixture(case)).await;
    }
}

#[tokio::test]
async fn old_worker_capability_report_cannot_admit_native_ceiling() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let worker = temp.path().join("old-worker");
    let marker = temp.path().join("started");
    let fabric = temp.path().join("fabric");
    std::fs::write(&worker, "#!/bin/sh\nif [ \"$2\" = --print-capabilities ]; then\nprintf '%s\\n' '{\"provision_version\":2,\"capabilities\":[\"required_child_context_v1\",\"durable_child_creation_identity_v1\"]}'\nelse\n: > \"$1\"\ncat >/dev/null\nfi\n").unwrap();
    std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700)).unwrap();
    let spec = bamboo_subagent::ProvisionSpec::from_json(&json!({
        "version":2,"identity":{"child_id":"old-child","parent_id":"old-root","depth":1},
        "executor":{"kind":"bamboo_runtime"},"fabric_dir":fabric,
        "bus":{"endpoint":"ws://127.0.0.1:1","token":"fixture"},
        "capabilities":{"enforce_permissions":true,"required_child_context":true,
            "child_creation_identity":true,"native_tool_ceiling_required":true,
            "native_tool_ceiling":{"version":1,"child_session_id":"old-child",
                "parent_session_id":"old-root","root_session_id":"old-root",
                "created_at":chrono::Utc::now(),"spawn_depth":1,"project_id":null,"tools":["Read"]}}
    }).to_string()).unwrap();
    spec.to_json().unwrap(); // Valid strict spec, not an unrelated validation rejection.
    let args = vec![marker.to_str().unwrap().to_string()];
    for result in [
        bamboo_subagent::fleet::spawn_worker(&worker, &args, &spec, Duration::from_secs(1)).await,
        bamboo_subagent::fleet::spawn_worker_on_bus(&worker, &args, &spec).await,
    ] {
        assert!(
            matches!(result, Err(ref error) if error.to_string().contains("native_tool_ceiling_v1"))
        );
        assert!(!marker.exists() && !fabric.exists());
    }
}

#[actix_web::test]
async fn actual_child_forced_request_is_durable_before_parent_review() {
    Box::pin(fixture(Case::ForcedApproval)).await;
}
