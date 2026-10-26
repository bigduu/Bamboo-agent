//! Reuse the required-packet fixture's actual compiled Host/current_exe worker.
//! Only model transport is synthetic; all creation, wait, publication and reads
//! travel through the production tool/runtime path. No Session is authored here.
use super::*;

struct PlannerProbe {
    data: PathBuf,
    crash_child: bool,
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    requests: Mutex<Vec<Value>>,
    read: Mutex<Option<Value>>,
    section: Mutex<Option<String>>,
}

fn delta(id: &str, name: &str, args: Value) -> Value {
    json!({"tool_calls":[{"index":0,"id":id,"type":"function",
        "function":{"name":name,"arguments":args.to_string()}}]})
}

fn tool_json(body: &Value, call_id: &str) -> Value {
    let message = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|m| m["role"] == "tool" && m["tool_call_id"] == call_id)
        .unwrap_or_else(|| panic!("missing actual tool result {call_id}"));
    let mut value: Value = serde_json::from_str(message["content"].as_str().unwrap()).unwrap();
    if let Some(inner) = value.get("result").and_then(Value::as_str) {
        value = serde_json::from_str(inner).unwrap();
    }
    value
}

async fn planner_response(body: web::Json<Value>, probe: web::Data<PlannerProbe>) -> HttpResponse {
    let body = body.into_inner();
    probe.requests.lock().unwrap().push(body.clone());
    assert!(
        probe.requests.lock().unwrap().len() <= 16,
        "bounded fixture request count"
    );
    let is_root = body["tools"]
        .as_array()
        .is_some_and(|tools| tools.iter().any(|t| t["function"]["name"] == "Plan"));
    let is_planner = body["model"] == "child-packet-test";
    if is_planner && probe.crash_child {
        assert_eq!(
            probe.child_calls.fetch_add(1, Ordering::SeqCst),
            0,
            "a lost planner must not be silently redispatched"
        );
        // The controller kills its own real Host while this real worker has a
        // pending model request. No response/report or completion seal exists.
        tokio::time::sleep(Duration::from_secs(600)).await;
        panic!("the isolated pending provider must be stopped by fixture teardown");
    }
    let (delta, finish) = if !is_root && !is_planner {
        (
            json!({"content":"isolated fixture auxiliary response"}),
            "stop",
        )
    } else if is_planner {
        let round = probe.child_calls.fetch_add(1, Ordering::SeqCst);
        match round {
            0 => (
                delta(
                    "planner-read-source",
                    "Read",
                    json!({"file_path":probe.data.join("source.txt")}),
                ),
                "tool_calls",
            ),
            1 => (
                delta(
                    "planner-denied-write",
                    "Write",
                    json!({"file_path":probe.data.join("must-not-exist.txt"),"content":"must be denied"}),
                ),
                "tool_calls",
            ),
            2 => {
                let wire = body.to_string();
                assert!(
                    wire.contains("NATIVE_READ_SOURCE"),
                    "real read result must reach worker"
                );
                assert!(!probe.data.join("must-not-exist.txt").exists());
                let mut report = typed_report();
                report["outcome"] = json!("completed");
                report["plan_markdown"] = json!(format!(
                    "# Migration\n## Chosen design\nSELECTED_PLAN_SECTION 用户🪷\n## Detailed appendix\n{}",
                    "EXCLUDED_PLAN_BODY ".repeat(1500)));
                (json!({"content":report.to_string()}), "stop")
            }
            _ => panic!("unexpected planner model request {round}"),
        }
    } else {
        let round = probe.root_calls.fetch_add(1, Ordering::SeqCst);
        if probe.crash_child && round == 1 {
            let projection = body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|m| m["content"].as_str())
                .find_map(resume_typed_projection)
                .expect(
                    "real boot recovery must resume the parent with a bounded unavailable outcome",
                );
            assert_eq!(projection["view"], "planner_artifact");
            assert_eq!(projection["available"], false);
            assert_eq!(projection["execution_approved"], false);
            assert!(projection["read"].is_null());
            let event = json!({"id":"planner-failure-response","object":"chat.completion.chunk",
                "choices":[{"index":0,"delta":{"content":"REAL_PLANNER_FAILURE_REPORTED"},"finish_reason":"stop"}]});
            return HttpResponse::Ok()
                .content_type("text/event-stream")
                .body(format!("data: {event}\n\ndata: [DONE]\n\n"));
        }
        match round {
            0 => {
                let disk = SessionStoreV2::new(probe.data.clone()).await.unwrap();
                let parent = disk.load_session("planner-root").await.unwrap().unwrap();
                let packet = json!({"version":1,"objective":"Plan a migration; do not implement",
                    "constraints":["Preserve the source"],"acceptance":["Return a versioned plan"],
                    "non_goals":["No implementation"],"necessary_user_instructions":[],
                    "recorded_decisions":[],"source_user_message_ids":[parent.messages.iter()
                        .find(|m|m.role == Role::User).unwrap().id],"background_message_ids":[]});
                (
                    delta(
                        "planner-create",
                        "Plan",
                        json!({"task":"Inspect source.txt and return a migration plan",
                    "context_packet":packet}),
                    ),
                    "tool_calls",
                )
            }
            1 => {
                let projection = body["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|m| m["content"].as_str())
                    .find_map(resume_typed_projection)
                    .expect("actual automatic parent resume");
                assert_eq!(projection["view"], "planner_artifact");
                assert_eq!(projection["available"], true);
                assert_eq!(projection["execution_approved"], false);
                assert!(!body.to_string().contains("EXCLUDED_PLAN_BODY"));
                let read = projection["read"].clone();
                *probe.read.lock().unwrap() = Some(read.clone());
                (delta("planner-index", "Plan", read), "tool_calls")
            }
            2 => {
                let index = tool_json(&body, "planner-index");
                let section = index["sections"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|s| s["heading"] == "Chosen design")
                    .unwrap()["id"]
                    .as_str()
                    .unwrap()
                    .to_string();
                *probe.section.lock().unwrap() = Some(section.clone());
                let mut read = probe.read.lock().unwrap().clone().unwrap();
                read["section_id"] = json!(section);
                read["max_bytes"] = json!(512);
                (delta("planner-section", "Plan", read), "tool_calls")
            }
            3 => {
                let section = tool_json(&body, "planner-section");
                assert!(section["text"]
                    .as_str()
                    .unwrap()
                    .contains("SELECTED_PLAN_SECTION"));
                assert!(!body.to_string().contains("EXCLUDED_PLAN_BODY"));
                (json!({"content":"REAL_PLANNER_ROOT_FINISHED"}), "stop")
            }
            4 => {
                let mut read = probe.read.lock().unwrap().clone().unwrap();
                read["section_id"] = json!(probe.section.lock().unwrap().clone().unwrap());
                (delta("planner-cold-section", "Plan", read), "tool_calls")
            }
            5 => {
                let section = tool_json(&body, "planner-cold-section");
                assert!(section["text"]
                    .as_str()
                    .unwrap()
                    .contains("SELECTED_PLAN_SECTION"));
                (json!({"content":"REAL_PLANNER_COLD_READ_FINISHED"}), "stop")
            }
            _ => panic!("unexpected parent model request {round}"),
        }
    };
    let event = json!({"id":"planner-response","object":"chat.completion.chunk",
        "choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}

async fn completed(data: &Path, marker: &str) -> bamboo_domain::Session {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let disk = SessionStoreV2::new(data.to_path_buf()).await.unwrap();
            let parent = disk.load_session("planner-root").await.unwrap().unwrap();
            assert_ne!(
                parent.last_run_status().as_deref(),
                Some("error"),
                "parent error: {:?}\n{}",
                parent.last_run_error(),
                std::fs::read_to_string(data.join("host.log")).unwrap()
            );
            if parent.last_run_status().as_deref() == Some("completed")
                && parent.messages.iter().any(|m| m.content == marker)
            {
                break parent;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|e| {
        panic!(
            "planner native timeout {e}: {}",
            std::fs::read_to_string(data.join("host.log")).unwrap()
        )
    })
}

async fn execute_only_legacy_chat(client: &reqwest::Client, base: &str, receipt: &Value) {
    assert_eq!(receipt["session_id"], "planner-root");
    if !receipt["message_id"].is_null() {
        // Current native Chat commits its own activation intent. This receipt
        // is data, not proof of execution: the persisted marker and bounded
        // provider-count assertions remain required below.
        return;
    }
    let response = client
        .post(format!("{base}/execute/planner-root"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
}

async fn planner_fixture(crash_child: bool) {
    let temp = tempfile::Builder::new()
        .prefix("bamboo-1059-native-plan-")
        .tempdir()
        .unwrap();
    let data = temp.path().canonicalize().unwrap();
    std::fs::write(data.join("source.txt"), "NATIVE_READ_SOURCE\n").unwrap();
    let probe = web::Data::new(PlannerProbe {
        data: data.clone(),
        crash_child,
        root_calls: AtomicUsize::new(0),
        child_calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
        read: Mutex::new(None),
        section: Mutex::new(None),
    });
    let copy = probe.clone();
    let provider = HttpServer::new(move || {
        App::new()
            .app_data(copy.clone())
            .route("/v1/chat/completions", web::post().to(planner_response))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok().json(
                        json!({"data":[{"id":"root-packet-test"},{"id":"child-packet-test"}]}),
                    )
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let url = format!("http://{}/v1", provider.addrs()[0]);
    let running = provider.run();
    let handle = running.handle();
    actix_web::rt::spawn(running);
    let config = json!({"provider":"openai","features":{"provider_model_ref":true},
        "providers":{"openai":{"api_key":"isolated-fixture","base_url":url,
            "model":"root-packet-test","runtime_models":["root-packet-test","child-packet-test"]}},
        "defaults":{"chat":{"provider":"openai","model":"root-packet-test"},
            "subagent_models":{"planner":{"provider":"openai","model":"child-packet-test"}}},
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
    let mut host = start_host(&data, port);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}/api/v1");
    await_host(&mut host, &client, &base, &data).await;
    let response = client
        .post(format!("{base}/chat"))
        .json(&json!({
        "session_id":"planner-root","message":"Prepare the migration plan without implementation",
        "model":"root-packet-test","model_ref":{"provider":"openai","model":"root-packet-test"},
        "permission_mode":"bypass","workspace_path":data}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let receipt: Value = response.json().await.unwrap();
    let disk = SessionStoreV2::new(data.clone()).await.unwrap();
    let original = disk.load_session("planner-root").await.unwrap().unwrap();
    execute_only_legacy_chat(&client, &base, &receipt).await;
    if crash_child {
        tokio::time::timeout(Duration::from_secs(30), async {
            while probe.child_calls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("the real worker must reach the pending provider before Host loss");
        let disk = SessionStoreV2::new(data.clone()).await.unwrap();
        let children: Vec<_> = disk
            .list_index_entries()
            .await
            .into_iter()
            .filter(|e| e.parent_session_id.as_deref() == Some("planner-root"))
            .collect();
        assert_eq!(children.len(), 1);
        let child_id = children[0].id.clone();
        assert_eq!(
            disk.load_session(&child_id)
                .await
                .unwrap()
                .unwrap()
                .last_run_status()
                .as_deref(),
            Some("running")
        );
        drop(host); // SIGKILL + wait, never a graceful cancellation or a forged terminal save.
        let mut restarted = start_host(&data, port);
        await_host(&mut restarted, &client, &base, &data).await;
        let parent = completed(&data, "REAL_PLANNER_FAILURE_REPORTED").await;
        assert_eq!(probe.root_calls.load(Ordering::SeqCst), 2);
        assert_eq!(probe.child_calls.load(Ordering::SeqCst), 1);
        assert!(parent
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .waiting_for_children
            .is_none());
        assert!(parent
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .plan_mode
            .is_none());
        assert_eq!(
            parent.agent_runtime_state.as_ref().unwrap().permission_mode,
            original
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .permission_mode
        );
        let disk = SessionStoreV2::new(data.clone()).await.unwrap();
        let child = disk.load_session(&child_id).await.unwrap().unwrap();
        assert_eq!(child.last_run_status().as_deref(), Some("error"));
        assert!(!child
            .metadata
            .contains_key("runtime.child_completion_source_v1"));
        assert!(!data.join("plan/delegated").exists());
        assert!(!data.join("must-not-exist.txt").exists());
        drop(restarted);
        handle.stop(false).await;
        eprintln!("actual delegated Plan crash: real Host SIGKILL/restart; pending worker; boot synthetic error; bounded unavailable parent resume; no sealed report/file; no redispatch; 0 paid calls");
        return;
    }
    let parent = completed(&data, "REAL_PLANNER_ROOT_FINISHED").await;
    assert_eq!(probe.root_calls.load(Ordering::SeqCst), 4);
    assert_eq!(probe.child_calls.load(Ordering::SeqCst), 3);
    assert_eq!(parent.kind, original.kind);
    assert_eq!(parent.root_thinking_mode(), original.root_thinking_mode());
    assert_eq!(
        parent.agent_runtime_state.as_ref().unwrap().permission_mode,
        original
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .permission_mode
    );
    assert!(parent
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .plan_mode
        .is_none());
    let disk = SessionStoreV2::new(data.clone()).await.unwrap();
    let children: Vec<_> = disk
        .list_index_entries()
        .await
        .into_iter()
        .filter(|e| e.parent_session_id.as_deref() == Some("planner-root"))
        .collect();
    assert_eq!(children.len(), 1);
    let child = disk.load_session(&children[0].id).await.unwrap().unwrap();
    assert!(child.agent_runtime_state.as_ref().unwrap().read_only);
    let denied = child
        .messages
        .iter()
        .find(|m| m.tool_call_id.as_deref() == Some("planner-denied-write"))
        .unwrap();
    assert_eq!(denied.tool_success, Some(false));
    assert_eq!(
        std::fs::read_to_string(data.join("source.txt")).unwrap(),
        "NATIVE_READ_SOURCE\n"
    );
    assert!(!data.join("must-not-exist.txt").exists());
    let revision = probe.read.lock().unwrap().as_ref().unwrap()["revision"]
        .as_str()
        .unwrap()
        .to_string();
    let file = data.join(format!("plan/delegated/{revision}.md"));
    let bytes = std::fs::read(&file).unwrap();
    assert!(bytes
        .windows("EXCLUDED_PLAN_BODY".len())
        .any(|s| s == b"EXCLUDED_PLAN_BODY"));
    drop(host);
    // Fresh real Host: only ordinary chat/execute APIs, no Session injection or
    // new Plan creation. The same source revision remains readable.
    let mut host = start_host(&data, port);
    await_host(&mut host, &client, &base, &data).await;
    let response = client
        .post(format!("{base}/chat"))
        .json(&json!({"session_id":"planner-root",
        "message":"Read the same completed plan section after restart"}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let receipt: Value = response.json().await.unwrap();
    execute_only_legacy_chat(&client, &base, &receipt).await;
    completed(&data, "REAL_PLANNER_COLD_READ_FINISHED").await;
    assert_eq!(probe.root_calls.load(Ordering::SeqCst), 6);
    assert_eq!(probe.child_calls.load(Ordering::SeqCst), 3);
    assert_eq!(std::fs::read(file).unwrap(), bytes);
    drop(host);
    handle.stop(false).await;
    eprintln!("actual delegated Plan: source-bound Host+worker; selected context; read-only denial; versioned artifact; automatic parent resume; partial and cold section reads; 6 Root + 3 Child primary requests; {} total bounded fake provider requests; 0 paid calls", probe.requests.lock().unwrap().len());
}

#[test]
fn real_cli_planner_publishes_versioned_artifact_and_resumes_with_partial_cold_reads() {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| actix_web::rt::System::new().block_on(planner_fixture(false)))
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn real_cli_planner_crash_recovery_resumes_without_publishing_unsealed_report() {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| actix_web::rt::System::new().block_on(planner_fixture(true)))
        .unwrap()
        .join()
        .unwrap();
}
