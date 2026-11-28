//! Real Host, Inbox, Project Manager and Work Session with model responses at the HTTP boundary.
use super::*;
use bamboo_domain::{ProjectId, ProjectSidebar, ProjectSidebarSection, SessionKind};
use bamboo_llm::Config;
use bamboo_server::AppState;
use bamboo_tickets::{Authority, DraftBrief, DraftSource, TicketService};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, Request, ResponseTemplate,
};

const FAST: &str = "fixture-fast-scanner";
const CHAT: &str = "fixture-project-manager";
const TITLE: &str = "Installation documentation";
const OBJECTIVE: &str = "Draft a checklist for a fresh Bodhi desktop installation on macOS ARM64. Assume a signed drag-and-drop disk image containing Bodhi.app. Produce text only.";
const REASON: &str =
    "Ready with accepted dependencies, an allowed tool contract, and a prepared worktree.";
const MODEL_VETO: &str = "The card cannot start without seeing the file contents.";
const SUMMARY: &str = "Summarize the installer instructions for macOS users.";
const SOURCE_FILE: &str = "installer-notes.md";
const OTHER_CARD: &str = "Out-of-project fixture title";
const DRAFT_CARD: &str = "Local draft fixture title";
const SECTION: &str = "section:3017e5df-00b9-4a37-99a0-9fb43af12098";
const CHECKLIST: &str = concat!(
    "- Verify macOS ARM64 and obtain the signed Bodhi disk image.\n",
    "- Mount the disk image and drag Bodhi.app into Applications.\n",
    "- Eject the image, launch Bodhi, and verify the main window opens.",
);

fn stream(delta: Value, finish: &str) -> ResponseTemplate {
    let start = json!({"choices":[{"index":0,"delta":delta,"finish_reason":null}]});
    let stop = json!({"choices":[{"index":0,"delta":{},"finish_reason":finish}]});
    let usage = json!({"choices":[],"usage":{"prompt_tokens":101,"completion_tokens":23,"total_tokens":124}});
    ResponseTemplate::new(200).set_body_raw(
        format!("data: {start}\n\ndata: {stop}\n\ndata: {usage}\n\ndata: [DONE]\n\n"),
        "text/event-stream",
    )
}

fn tool(id: &str, name: &str, arguments: Value) -> ResponseTemplate {
    let id = format!("{id}-{}", uuid::Uuid::new_v4());
    stream(
        json!({"tool_calls":[{"index":0,"id":id,"type":"function","function":{
            "name":name,"arguments":arguments.to_string()
        }}]}),
        "tool_calls",
    )
}

fn tool_result<'a>(body: &'a Value, id: &str) -> Option<&'a Value> {
    let prefix = format!("{id}-");
    body["messages"]
        .as_array()?
        .iter()
        .rev()
        .take_while(|message| message["role"] != "user")
        .find(|message| {
            message["role"] == "tool"
                && message["tool_call_id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with(&prefix))
        })
}

fn scan_request(body: &Value) -> bool {
    body["messages"][0]["content"]
        .as_str()
        .is_some_and(|prompt| prompt.contains("Summarize the requested deliverable"))
}

fn assistant_text(text: &str) -> ResponseTemplate {
    stream(json!({"content":text}), "stop")
}

async fn host_for_scan(root: &std::path::Path, endpoint: &str) -> AppState {
    let mut config: Config = serde_json::from_value(json!({
        "provider":"fixture-account",
        "default_provider_instance":"fixture-account",
        "features":{"ticket_mutation":true,"ticket_dispatch":true},
        "provider_instances":{"fixture-account":{
            "provider_type":"openai","enabled":true,"api_key":"synthetic-http-fixture",
            "base_url":endpoint,"model":CHAT,"runtime_models":[CHAT,FAST]
        }},
        "defaults":{"chat":{"provider":"fixture-account","model":CHAT},
            "fast":{"provider":"fixture-account","model":FAST}},
        "run_budget":{"max_rounds":8}
    }))
    .unwrap();
    let memory = config.memory_mut().get_or_insert_with(Default::default);
    memory.auto_dream_enabled = false;
    memory.gardener_enabled = false;
    memory.dedup_gardener_enabled = false;
    memory.ledger_gardener_enabled = false;
    memory.ledger_distillation_enabled = false;
    memory.granularity_freshness_gardener_enabled = false;
    AppState::new_with_provider_and_memory_store(
        root.to_owned(),
        config,
        Arc::new(LocalProvider {
            requests: mpsc::unbounded_channel().0,
            calls: AtomicUsize::new(0),
            max_requests: 0,
        }),
        bamboo_memory::memory_store::MemoryStore::new(root.join("jiandu")),
    )
    .await
    .unwrap()
}

fn create_work(
    service: &TicketService,
    owner: &Authority,
    project: &ProjectId,
    title: &str,
    ready: bool,
    input: Option<&str>,
) -> String {
    let mut allowed_tools = BTreeSet::from(["Task".into()]);
    if input.is_some() {
        allowed_tools.insert("Read".into());
    }
    let mut operations = vec![
        Operation::Create {
            temp_id: "work".into(),
            kind: TicketKind::Work,
            parent: None,
            contract: Contract {
                title: title.into(),
                objective: OBJECTIVE.into(),
                constraints: vec!["Do not modify the repository.".into()],
                acceptance: vec!["Include prerequisites, installation, and launch checks.".into()],
                user_acceptance_required: true,
                allowed_tools,
            },
            depends_on: BTreeSet::new(),
        },
        Operation::AssociateProject {
            ticket_id: "work".into(),
            project_id: project.clone(),
        },
    ];
    if let Some(path) = input {
        operations.push(Operation::SetDraftBrief {
            work_id: "work".into(),
            brief: DraftBrief {
                source_session_id: "fixture".into(),
                facts: Vec::new(),
                decisions: Vec::new(),
                open_questions: Vec::new(),
                required_files: BTreeSet::from([path.into()]),
                sources: vec![DraftSource {
                    message_id: "source".into(),
                    content_sha256: content_hash(OBJECTIVE.as_bytes()),
                }],
            },
        });
    }
    if ready {
        operations.push(Operation::Ready {
            work_id: "work".into(),
        });
    }
    let command = service
        .prepare_command(owner, &uuid::Uuid::new_v4().to_string(), operations)
        .unwrap();
    service.work_update(owner, &command).unwrap().ids["work"].clone()
}

macro_rules! read_scan {
    ($app:ident, $route:ident) => {{
        let response = test::call_service(
            &$app,
            test::TestRequest::get()
                .uri(&$route)
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        test::read_body_json::<Value, _>(response).await
    }};
}

async fn wait_for_request(server: &MockServer, predicate: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(WAIT, async {
        loop {
            if let Some(body) = server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter_map(|request| request.body_json::<Value>().ok())
                .find(|body| predicate(body))
            {
                break body;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("a model request with the expected context")
}

#[actix_web::test]
async fn background_project_scan_isolated_restartable_and_routes_the_original_card() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let home = tempfile::tempdir().unwrap();
    let root = home.path().canonicalize().unwrap();
    let workspace = execution_workspace(&root);
    let server = MockServer::start().await;
    let (project_id, work, manager_id, other, draft) = {
        let state = web::Data::new(Box::pin(host_for_scan(&root, &server.uri())).await);
        let project = state
            .project_store
            .create_with_project_path(
                "Scan fixture",
                None,
                workspace.repo.clone(),
                vec![WorkspaceBinding {
                    path: workspace.worktree.clone(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .unwrap();
        let project = state
            .project_store
            .update(&project.id, project.revision, |manifest| {
                manifest.sidebar = Some(ProjectSidebar {
                    sections: vec![
                        ProjectSidebarSection {
                            id: "section:3d3af8c9-2f90-42e9-a03c-abca34574ea7".into(),
                            name: "Testing".into(),
                            description: None,
                        },
                        ProjectSidebarSection {
                            id: SECTION.into(),
                            name: "Documentation".into(),
                            description: None,
                        },
                    ],
                    ..Default::default()
                });
                Ok(())
            })
            .unwrap();
        let outside = state
            .project_store
            .create("Separate project", None)
            .unwrap();
        let (service, owner) = state
            .tickets
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await
            .unwrap();
        let work = create_work(
            &service,
            &owner,
            &project.id,
            TITLE,
            true,
            Some(SOURCE_FILE),
        );
        let draft = create_work(&service, &owner, &project.id, DRAFT_CARD, false, None);
        let other = create_work(&service, &owner, &outside.id, OTHER_CARD, true, None);
        let manager_route = format!("/api/v1/projects/{}/manager", project.id);
        let route = format!("/api/v1/projects/{}/scan", project.id);
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(bamboo_server::configure_routes),
        )
        .await;
        let manager = owner_post!(app, &manager_route, &json!({}), StatusCode::OK);
        assert_eq!(read_scan!(app, route)["mode"], "off");
        let manager_id = manager["session_id"].as_str().unwrap().to_owned();
        let work_for_provider = work.clone();
        let start_calls = Arc::new(AtomicUsize::new(0));
        let summary_calls = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST")).and(path("/chat/completions"))
            .respond_with(move |request: &Request| {
                let body: Value = request.body_json().unwrap();
                if scan_request(&body) {
                    let response = if summary_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        json!({"dispatchable":false,"reason":MODEL_VETO})
                    } else {
                        json!({"summary":SUMMARY})
                    };
                    return assistant_text(&response.to_string());
                }
                if body["model"] == FAST {
                    return if tool_result(&body, "fixture-worker-task").is_some() {
                        assistant_text(CHECKLIST)
                    } else {
                        tool("fixture-worker-task", "Task", json!({"tasks":[
                            {"id":"done","content":"Write installation checklist","status":"completed"}
                        ]}))
                    };
                }
                assert_eq!(body["model"], CHAT);
                if tool_result(&body, "fixture-scan-start").is_some() {
                    return assistant_text("Project Manager recorded the dispatch outcome.");
                }
                if let Some(result) = tool_result(&body, "fixture-scan-inspect") {
                    let details: Value = serde_json::from_str(result["content"].as_str().unwrap()).unwrap();
                    assert_eq!(details["ticket"]["contract"]["title"], TITLE);
                    let section = details["sections"].as_array().unwrap().iter()
                        .find(|section| section["name"] == "Documentation").unwrap();
                    let response = tool("fixture-scan-start", "project_manager_work", json!({
                        "action":"start","work_id":work_for_provider,
                        "section_id":section["id"],"operation_id":uuid::Uuid::new_v4()
                    }));
                    return if start_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        response.set_delay(Duration::from_secs(3))
                    } else {
                        response
                    };
                }
                if body["tools"].as_array().unwrap().iter().any(|item| item["function"]["name"] == "project_manager_work") {
                    return tool("fixture-scan-inspect", "project_manager_work",
                        json!({"action":"inspect","work_id":work_for_provider}));
                }
                tool("fixture-scan-discover", "discover_capabilities", json!({"query":"project_manager_work"}))
            })
            .mount(&server).await;
        let response = owner_post!(app, &route, &json!({"action":"enable"}), StatusCode::OK);
        assert_eq!(response["mode"], "enabled");
        let blocked = tokio::time::timeout(WAIT, async {
            loop {
                let scan = read_scan!(app, route);
                if !scan["last_check"]["blocker"].is_null() {
                    break scan;
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("the host reports the missing declared file");
        assert_eq!(blocked["last_check"]["work_id"], work);
        assert_eq!(
            blocked["last_check"]["blocker"],
            json!({"kind":"required_file_unavailable","path":SOURCE_FILE})
        );
        assert_eq!(blocked["usage"]["model_requests"], 0);
        assert!(server.received_requests().await.unwrap().is_empty());

        let revision = service.published().unwrap().1.tickets[&work].record_revision;
        std::fs::write(
            std::path::Path::new(&workspace.worktree).join(SOURCE_FILE),
            CHECKLIST,
        )
        .unwrap();
        assert_eq!(
            service.published().unwrap().1.tickets[&work].record_revision,
            revision
        );
        owner_post!(app, &route, &json!({"action":"pause"}), StatusCode::OK);
        owner_post!(app, &route, &json!({"action":"resume"}), StatusCode::OK);
        let scan = wait_for_request(&server, scan_request).await;
        assert_eq!(scan["model"], FAST);
        assert_eq!(scan["messages"].as_array().unwrap().len(), 2);
        assert!(scan["messages"][1]["content"]
            .as_str()
            .unwrap()
            .contains(TITLE));
        assert!(scan["tools"].is_null() || scan["tools"].as_array().is_some_and(Vec::is_empty));
        assert_eq!(scan["stream_options"]["include_usage"], true);
        assert!(!scan["messages"].to_string().contains(SOURCE_FILE));
        let intake = wait_for_request(&server, |body| {
            tool_result(body, "fixture-scan-inspect").is_some()
        })
        .await;
        assert_eq!(intake["model"], CHAT);
        let messages = intake["messages"].to_string();
        for expected in [&work, REASON] {
            assert!(messages.contains(expected));
        }
        for excluded in [
            OTHER_CARD,
            DRAFT_CARD,
            "Summarize the requested deliverable",
            MODEL_VETO,
            "paced_loop_iteration",
        ] {
            assert!(!messages.contains(excluded), "{excluded}");
        }
        let paused = owner_post!(app, &route, &json!({"action":"pause"}), StatusCode::OK);
        assert_eq!(paused["mode"], "paused");
        assert_eq!(paused["usage"]["model_requests"], 1);
        assert_eq!(paused["usage"]["unknown_requests"], 0);
        assert_eq!(paused["usage"]["tokens"]["total_tokens"], 124);
        let refusal = wait_for_request(&server, |body| {
            tool_result(body, "fixture-scan-start").is_some()
        })
        .await;
        assert!(refusal["messages"]
            .to_string()
            .contains("Project scan is paused"));
        assert_eq!(
            service.published().unwrap().1.tickets[&work].state,
            WorkState::Ready
        );
        drop(app);
        state.shutdown().await;
        (project.id, work, manager_id, other, draft)
    };

    let state = web::Data::new(Box::pin(host_for_scan(&root, &server.uri())).await);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let route = format!("/api/v1/projects/{project_id}/scan");
    let status = read_scan!(app, route);
    assert_eq!(status["mode"], "paused");
    assert_eq!(status["usage"]["tokens"]["total_tokens"], 124);
    assert_eq!(
        state
            .project_store
            .get(&project_id)
            .unwrap()
            .project_manager
            .unwrap()
            .session_id
            .to_string(),
        manager_id
    );
    let resumed = owner_post!(app, &route, &json!({"action":"resume"}), StatusCode::OK);
    assert_eq!(resumed["mode"], "enabled");
    let (service, owner) = state
        .tickets
        .authority(Principal::User {
            user_id: "host-owner".into(),
        })
        .await
        .unwrap();
    let submitted = tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = service.published().unwrap().1;
            if snapshot.tickets[&work].state == WorkState::Submitted {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("the original Work receives its submission after resuming");
    let submission = submitted.tickets[&work]
        .current_submission
        .as_ref()
        .unwrap();
    let facts = service
        .work_submission_facts(&owner, &work, submission, None)
        .unwrap()
        .data;
    assert_eq!(
        facts.submission.artifacts[0].sha256,
        content_hash(CHECKLIST.as_bytes())
    );
    assert_eq!(submitted.tickets[&other].state, WorkState::Ready);
    assert_eq!(submitted.tickets[&draft].state, WorkState::Draft);
    assert_eq!(submitted.assignments.len(), 1);
    let session_id = &facts.assignment.runtime.as_ref().unwrap().session_id;
    let worker = state
        .storage
        .load_session(session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(worker.kind, SessionKind::Root);
    assert_eq!(
        worker.project_id_meta().as_deref(),
        Some(project_id.as_str())
    );
    let manifest = state.project_store.get(&project_id).unwrap();
    assert_eq!(
        manifest.sidebar.unwrap().session_sections[session_id].as_deref(),
        Some(SECTION)
    );
    assert!(!worker
        .messages
        .iter()
        .any(|message| message.content.contains(REASON)));
    let scan = read_scan!(app, route);
    assert_eq!(scan["last_check"]["summary"], SUMMARY);
    let manager = state
        .storage
        .load_session(&manager_id)
        .await
        .unwrap()
        .unwrap();
    assert!(manager
        .messages
        .iter()
        .all(|msg| !msg.content.contains(SUMMARY)));
    let off = owner_post!(app, &route, &json!({"action":"disable"}), StatusCode::OK);
    assert_eq!(off["mode"], "off");
    assert_eq!(off["usage"]["model_requests"], 2);
    assert_eq!(off["usage"]["tokens"]["total_tokens"], 248);
    drop(app);
    state.shutdown().await;
}
