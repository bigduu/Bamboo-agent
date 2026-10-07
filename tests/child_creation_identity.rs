//! Real compiled builtin worker: A -> B -> warm B -> cold B in the same
//! default protocol cache, with exact host birth and durable admission proof.
use std::{path::Path, sync::Mutex, time::Duration};

use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::{
    Message, Session, SessionActivationPolicy, SessionInboxPort, SessionMessageEnvelope,
};
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
use bamboo_subagent::{
    fleet::spawn_worker,
    proto::{
        ChildCreationIdentity, ChildFrame, LogicalSessionIdentity, ParentFrame, RunSpec,
        SessionMessageDelivery, TerminalStatus,
    },
    provision::{ChildIdentity, ExecutorSpec, ModelRefSpec, ProvisionSpec, ScopedCredential},
    transport::ChildClient,
};
use serde_json::{json, Value};

const FOLLOWUP: &str = "ONE_CANONICAL_TYPED_INPUT: stay within this Child assignment";

async fn response(body: web::Json<Value>, requests: web::Data<Mutex<Vec<Value>>>) -> HttpResponse {
    requests.lock().unwrap().push(body.into_inner());
    HttpResponse::Ok().content_type("text/event-stream").body(format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"id":"birth-fixture", "object":"chat.completion.chunk", "choices":[{"index":0,"delta":{"content":"actual worker evidence"},"finish_reason":"stop"}]}),
    ))
}

#[test]
fn compiled_worker_preserves_child_birth_and_receipt_across_warm_and_cold_runs() {
    // Host libtest fixture only. The real subprocess keeps its native stack;
    // RUST_MIN_STACK is neither set nor supplied to the worker.
    std::thread::Builder::new()
        .name("child-birth-fixture".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| actix_web::rt::System::new().block_on(run_fixture()))
        .unwrap()
        .join()
        .unwrap();
}

fn provision(home: &Path, base_url: &str, physical_id: &str) -> ProvisionSpec {
    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: physical_id.into(),
            parent_id: Some("birth-parent".into()),
            project_key: None,
            role: "worker".into(),
            depth: 1,
        },
        ExecutorSpec::BambooRuntime,
        home.join("fabric").to_string_lossy().into_owned(),
    );
    spec.capabilities.child_creation_identity = true;
    spec.capabilities.read_only = true;
    spec.capabilities.guardian_read_only = true;
    spec.capabilities.enforce_permissions = true;
    spec.disabled_tools = Some(
        bamboo_engine::runtime::guardian_state::read_only_child_disabled_tools()
            .into_iter()
            .collect(),
    );
    spec.reusable = true;
    spec.workspace = Some(home.to_string_lossy().into_owned());
    spec.model = Some(ModelRefSpec {
        provider: "openai".into(),
        model: "birth-test".into(),
    });
    spec.secrets.provider_credentials.push(ScopedCredential {
        provider: "openai".into(),
        api_key: "fixture-key".into(),
        base_url: Some(base_url.into()),
        provider_type: None,
        credential_ref: None,
    });
    assert!(
        spec.storage_dir.is_none(),
        "test must exercise the real default namespace"
    );
    spec
}

async fn host_run(
    host: &SessionStoreV2,
    id: &str,
    epoch: u64,
    envelope: Option<&SessionMessageEnvelope>,
) -> RunSpec {
    let canonical = host.load_session(id).await.unwrap().unwrap();
    let run_id = format!("host-run-{epoch}");
    RunSpec {
        assignment: "bounded Child task".into(),
        logical_session: Some(LogicalSessionIdentity {
            session_id: canonical.id.clone(),
            parent_session_id: canonical.parent_session_id.clone(),
            root_session_id: canonical.root_session_id.clone(),
            creation: Some(ChildCreationIdentity {
                created_at: canonical.created_at,
                spawn_depth: canonical.spawn_depth,
            }),
        }),
        project_id: canonical
            .project_id_meta()
            .map(|id| bamboo_domain::ProjectId::parse(id).unwrap()),
        reasoning_effort: Some("low".into()),
        permission_policy: None,
        // Deliberately omit the previously admitted typed message, simulating
        // a lost host confirmation. Existing worker receipt reconciliation
        // must restore it before another provider dispatch, including cold B.
        messages: canonical
            .messages
            .iter()
            .map(|message| serde_json::to_value(message).unwrap())
            .collect(),
        activation_run_id: Some(run_id.clone()),
        execution_epoch: epoch,
        initial_session_messages: envelope
            .into_iter()
            .map(|envelope| SessionMessageDelivery {
                target_session_id: id.into(),
                envelope: envelope.clone(),
                canonical_claim_generation: 1,
                activation_run_id: run_id.clone(),
                activation_policy: SessionActivationPolicy::InterruptSpecificWait,
            })
            .collect(),
        secrets: Default::default(),
    }
}

async fn dispatch(endpoint: &str, run: RunSpec, expect_success: bool) {
    let expected_identity = run.logical_session.clone();
    let run_id = run.activation_run_id.clone();
    let deliveries = run.initial_session_messages.clone();
    let mut client = ChildClient::connect(endpoint).await.unwrap();
    client.send(ParentFrame::Run(run)).await.unwrap();
    let mut confirmations = 0;
    let (status, error) = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match client
                .next_frame()
                .await
                .unwrap()
                .expect("real worker terminal")
            {
                ChildFrame::EventBatch { batch } => {
                    batch.validate().unwrap();
                    assert_eq!(
                        batch.logical_session, expected_identity,
                        "event identity must equal the dispatched lifetime"
                    );
                    assert_eq!(batch.activation_id, run_id);
                }
                ChildFrame::SessionMessageAdmitted { confirmation } => {
                    confirmations += 1;
                    assert_eq!(
                        confirmation.activation_run_id.as_str(),
                        run_id.as_deref().unwrap()
                    );
                    assert_eq!(confirmation.envelope_id, deliveries[0].envelope.id.as_str());
                    assert_eq!(confirmation.canonical_claim_generation, 1);
                }
                ChildFrame::Terminal { status, error, .. } => break (status, error),
                ChildFrame::ApprovalRequest { .. } => {
                    panic!("identity test cannot request new authority")
                }
                ChildFrame::OwnedTreeRequest { .. } => {
                    panic!("identity test cannot request a Host tree")
                }
                ChildFrame::SubAgentRequest { .. } => {
                    panic!("identity test cannot request a Host SubAgent operation")
                }
                ChildFrame::Event { .. } => panic!("typed activation must retain event identity"),
            }
        }
    })
    .await
    .expect("bounded native worker execution");
    client.close().await.unwrap();
    assert_eq!(
        status,
        if expect_success {
            TerminalStatus::Completed
        } else {
            TerminalStatus::Error
        },
        "{error:?}"
    );
    assert_eq!(
        confirmations,
        if expect_success { deliveries.len() } else { 0 }
    );
}

async fn run_fixture() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().canonicalize().unwrap();
    // The independent Jiandu root is scoped to this disposable fixture.
    let previous = std::env::var_os("BAMBOO_JIANDU_DATA_DIR");
    struct Restore(Option<std::ffi::OsString>);
    impl Drop for Restore {
        fn drop(&mut self) {
            match &self.0 {
                Some(value) => std::env::set_var("BAMBOO_JIANDU_DATA_DIR", value),
                None => std::env::remove_var("BAMBOO_JIANDU_DATA_DIR"),
            }
        }
    }
    let _restore = Restore(previous);
    std::env::set_var("BAMBOO_JIANDU_DATA_DIR", path.join("jiandu"));
    let requests = web::Data::new(Mutex::new(Vec::<Value>::new()));
    let captured = requests.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(captured.clone())
            .route("/v1/chat/completions", web::post().to(response))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok().json(json!({"data":[{"id":"birth-test"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let base_url = format!("http://{}/v1", server.addrs()[0]);
    let server = server.run();
    let handle = server.handle();
    actix_web::rt::spawn(server);
    let host = SessionStoreV2::new(path.join("host-store")).await.unwrap();
    let parent = Session::new("birth-parent", "birth-test");
    host.save_session(&parent).await.unwrap();
    for id in ["logical-a", "logical-b"] {
        let mut child = Session::new_child_of(id, &parent, "birth-test", id);
        child.set_project_id_meta("birth-project");
        child.messages = vec![
            Message::system("bounded worker"),
            Message::user("bounded Child task"),
        ];
        host.save_session(&child).await.unwrap();
    }
    if let Some(image) = std::env::var_os("BAMBOO_1348_LEGACY_WORKER_IMAGE") {
        eprintln!(
            "executing real legacy worker capability gate: {}",
            Path::new(&image).display()
        );
        let legacy_spec = provision(&path, &base_url, "legacy-required-probe");
        let error = match spawn_worker(
            Path::new(&image),
            &["subagent-worker".into()],
            &legacy_spec,
            Duration::from_secs(25),
        )
        .await
        {
            Err(error) => error,
            Ok(worker) => {
                worker.kill().await;
                panic!("legacy worker must not receive a birth-required provision");
            }
        };
        assert!(
            error.to_string().contains(
                "does not acknowledge required capability 'durable_child_creation_identity_v1'"
            ),
            "{error}"
        );
        assert!(
            !path.join("fabric").exists(),
            "probe rejection precedes provision/fabric allocation"
        );
        assert!(
            requests.lock().unwrap().is_empty(),
            "legacy rejection precedes Run/provider"
        );
    } else {
        eprintln!(
            "real legacy image gate omitted; set BAMBOO_1348_LEGACY_WORKER_IMAGE to execute it"
        );
    }
    let spec = provision(&path, &base_url, "physical-a");
    let mut spawned = spawn_worker(
        Path::new(env!("CARGO_BIN_EXE_bamboo")),
        &["subagent-worker".into()],
        &spec,
        Duration::from_secs(25),
    )
    .await
    .unwrap();
    let warm_pid = spawned.pid().unwrap();
    dispatch(
        &spawned.record.endpoint,
        host_run(&host, "logical-a", 1, None).await,
        true,
    )
    .await;
    let mut envelope = SessionMessageEnvelope::user_input("logical-b", FOLLOWUP);
    envelope.id = bamboo_domain::SessionMessageId::parse("birth-typed-message").unwrap();
    for epoch in [2, 3] {
        assert!(spawned.is_alive());
        assert_eq!(spawned.pid(), Some(warm_pid));
        dispatch(
            &spawned.record.endpoint,
            host_run(&host, "logical-b", epoch, Some(&envelope)).await,
            true,
        )
        .await;
    }
    let namespace = path.join("fabric/bamboo-runtime-logical-v1");
    let cache = std::sync::Arc::new(SessionStoreV2::new(namespace.clone()).await.unwrap());
    let inbox = FileSessionInbox::new(cache.clone(), Default::default());
    assert!(inbox.was_admitted("logical-b", &envelope.id).await.unwrap());
    let before = cache.load_session("logical-b").await.unwrap().unwrap();
    let canonical = host.load_session("logical-b").await.unwrap().unwrap();
    assert_eq!(before.created_at, canonical.created_at);
    assert_eq!(before.spawn_depth, canonical.spawn_depth);
    assert!(
        before
            .agent_runtime_state
            .as_ref()
            .expect("warm terminal persists typed runtime")
            .read_only,
        "read-only posture survives warm startup and final save"
    );
    let cursor = before.session_inbox_admission().cloned();
    spawned.kill().await;

    let spec = provision(&path, &base_url, "physical-b");
    let mut cold = spawn_worker(
        Path::new(env!("CARGO_BIN_EXE_bamboo")),
        &["subagent-worker".into()],
        &spec,
        Duration::from_secs(25),
    )
    .await
    .unwrap();
    assert_ne!(
        cold.pid(),
        Some(warm_pid),
        "cold worker must be a new process"
    );
    assert!(namespace.is_dir());
    dispatch(
        &cold.record.endpoint,
        host_run(&host, "logical-b", 4, Some(&envelope)).await,
        true,
    )
    .await;
    assert_eq!(requests.lock().unwrap().len(), 4);
    for request in requests.lock().unwrap().iter() {
        let tools: Vec<_> = request["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|tool| tool["function"]["name"].as_str())
            .collect();
        assert!(tools.contains(&"Read") && tools.contains(&"Glob") && tools.contains(&"Grep"));
        for denied in bamboo_engine::runtime::guardian_state::read_only_child_disabled_tools() {
            assert!(
                !tools.contains(&denied.as_str()),
                "host read-only catalog cannot expose {denied}"
            );
        }
    }
    for request in requests.lock().unwrap().iter().skip(1) {
        assert_eq!(
            request["messages"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|message| message["content"]
                    .as_str()
                    .is_some_and(|content| content.contains(FOLLOWUP)))
                .count(),
            1,
            "warm and cold retry must each expose exactly one canonical typed input"
        );
        assert_eq!(request["reasoning_effort"], "low");
    }
    // New independent Store handle, proving the cold worker selected the same
    // namespace instead of hiding the receipt in a new physical-id directory.
    let restarted_cache = std::sync::Arc::new(SessionStoreV2::new(namespace).await.unwrap());
    let restarted_inbox = FileSessionInbox::new(restarted_cache.clone(), Default::default());
    let saved = restarted_cache
        .load_session("logical-b")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.created_at, canonical.created_at);
    assert_eq!(saved.parent_session_id, canonical.parent_session_id);
    assert_eq!(saved.root_session_id, canonical.root_session_id);
    assert!(
        saved
            .agent_runtime_state
            .as_ref()
            .expect("cold terminal persists typed runtime")
            .read_only,
        "read-only posture survives cold startup and final save"
    );
    assert_eq!(saved.session_inbox_admission(), cursor.as_ref());
    assert_eq!(
        saved
            .messages
            .iter()
            .filter(|message| bamboo_domain::is_matching_session_message(message, &envelope))
            .count(),
        1
    );
    assert!(restarted_inbox
        .was_admitted("logical-b", &envelope.id)
        .await
        .unwrap());
    let snapshot = serde_json::to_value(&saved).unwrap();
    for damage in 0..6 {
        let mut run = host_run(&host, "logical-b", 5 + damage, Some(&envelope)).await;
        let identity = run.logical_session.as_mut().unwrap();
        match damage {
            0 => identity.creation.as_mut().unwrap().created_at += chrono::Duration::nanoseconds(1),
            1 => identity.parent_session_id = Some("wrong-parent".into()),
            2 => identity.root_session_id = "wrong-root".into(),
            3 => identity.creation.as_mut().unwrap().spawn_depth += 1,
            4 => run.project_id = Some(bamboo_domain::ProjectId::parse("wrong-project").unwrap()),
            _ => identity.creation = None,
        }
        dispatch(&cold.record.endpoint, run, false).await;
        assert_eq!(
            requests.lock().unwrap().len(),
            4,
            "wrong lifetime must not reach provider"
        );
        assert_eq!(
            serde_json::to_value(
                restarted_cache
                    .load_session("logical-b")
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            snapshot
        );
        assert!(restarted_inbox
            .was_admitted("logical-b", &envelope.id)
            .await
            .unwrap());
    }
    assert!(cold.is_alive());
    cold.kill().await;
    handle.stop(false).await;
}
