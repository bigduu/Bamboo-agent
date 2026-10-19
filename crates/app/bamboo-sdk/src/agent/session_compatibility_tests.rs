//! Public SDK API regressions; no real provider or physical tool is used.
use super::*;

fn with_version(session: Session, version: Option<u32>) -> Session {
    let mut raw = serde_json::to_value(session).unwrap();
    match version {
        Some(version) => raw["session_version"] = json!(version),
        None => { raw.as_object_mut().unwrap().remove("session_version"); }
    }
    serde_json::from_value(raw).unwrap()
}

#[tokio::test]
async fn session_version_unknown_is_refused_before_sdk_append_or_provider() {
    for ultra in [false, true] {
        let fixture = Fixture::new().await;
        let mut session = Session::new(format!("future-sdk-{ultra}"), "test-model");
        session.set_root_orchestration_only(ultra).unwrap();
        session.add_message(Message::user("Existing history"));
        fixture.repo.save(&mut session).await.unwrap();
        let mut session = with_version(session, Some(99));
        let before = serde_json::to_value(&session).unwrap();
        let agent = fixture.agent(None, false, false, None);
        let result = agent.run(&mut session, "Must not append").await;
        let error = result.expect_err("unknown behavior cannot execute");
        assert!(error.to_string().contains("session_execution_incompatible"), "{error}");
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
        assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
        assert!(fixture.probe.calls.lock().unwrap().is_empty());
        assert!(!fixture.directory.path().join("actions.log").exists());
    }
}

#[tokio::test]
async fn session_version_unknown_preserves_legacy_replay_markers_without_action() {
    let fixture = Fixture::new().await;
    let (mut session, _) = parked("future-replay", false);
    fixture.repo.save(&mut session).await.unwrap();
    let agent = fixture.agent(None, false, false, None);
    let answered = agent.answer(&session.id, "Approve").await.unwrap().session;
    let mut session = with_version(answered, Some(99));
    let before = serde_json::to_value(&session).unwrap();
    let result = agent.run_session(&mut session).await;
    let error = result.expect_err("incompatible behavior cannot replay approved tools");
    assert!(error.to_string().contains("session_execution_incompatible"), "{error}");
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
    assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
    assert!(fixture.probe.calls.lock().unwrap().is_empty());
    assert!(!fixture.directory.path().join("actions.log").exists());
}

#[tokio::test]
async fn session_version_compatible_legacy_without_stamp_still_continues() {
    let fixture = Fixture::new().await;
    let mut session = with_version(Session::new("usable-legacy-sdk", "test-model"), None);
    session.add_message(Message::user("Existing history"));
    fixture.repo.save(&mut session).await.unwrap();
    let agent = fixture.agent(None, false, false, None);
    agent.run(&mut session, "Continue this compatible legacy session").await.unwrap();
    assert!(fixture.probe.provider_calls.load(Ordering::SeqCst) > 0);
    assert!(session.messages.iter().any(|message| message.content == "Existing history"));
    assert!(serde_json::to_value(&session).unwrap().get("session_version").is_none());
    let loaded = fixture.store.load_session(&session.id).await.unwrap().unwrap();
    assert!(serde_json::to_value(loaded).unwrap().get("session_version").is_none());
}

#[tokio::test]
async fn session_version_unsupported_retained_state_is_not_reset_before_dispatch() {
    for context_conflict in [false, true] {
        let fixture = Fixture::new().await;
        let mut session = with_version(Session::new("future-retained-state", "test-model"), None);
        session.add_message(Message::user("Existing preserved history"));
        fixture.repo.save(&mut session).await.unwrap();
        if context_conflict {
            session.model_context_state = Some(bamboo_domain::ModelContextState {
                schema_version: 99,
                state_revision: 7,
                ..Default::default()
            });
        } else {
            let mut runtime = AgentRuntimeState::new("retained-generation");
            runtime.version = 99;
            session.agent_runtime_state = Some(runtime);
        }
        let before = serde_json::to_value(&session).unwrap();
        let agent = fixture.agent(None, false, false, None);
        let error = agent.run(&mut session, "must not reset this retained schema").await
            .expect_err("unknown retained schema is a concrete conflict");
        let expected = if context_conflict {
            "unsupported_model_context_version"
        } else {
            "unsupported_runtime_state_version"
        };
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
        assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
        assert!(fixture.probe.calls.lock().unwrap().is_empty());
    }
}


async fn future_durable(fixture: &Fixture, id: &str, conflict: &str) -> [Vec<u8>; 2] {
    let directory = fixture.directory.path().join("sessions").join(id);
    let name = if conflict == "main" { "session.json" } else { "runtime.json" };
    let path = directory.join(name);
    let mut raw: Value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
    match conflict {
        "main" | "runtime" => raw["session_version"] = json!(99),
        "context" => raw["model_context_state"] = serde_json::to_value(bamboo_domain::ModelContextState {
            schema_version: 99, state_revision: 7, ..Default::default()
        }).unwrap(),
        _ => unreachable!(),
    }
    tokio::fs::write(path, serde_json::to_vec_pretty(&raw).unwrap()).await.unwrap();
    [tokio::fs::read(directory.join("session.json")).await.unwrap(),
     tokio::fs::read(directory.join("runtime.json")).await.unwrap()]
}

async fn durable_pair(fixture: &Fixture, id: &str) -> [Vec<u8>; 2] {
    let directory = fixture.directory.path().join("sessions").join(id);
    [tokio::fs::read(directory.join("session.json")).await.unwrap(),
     tokio::fs::read(directory.join("runtime.json")).await.unwrap()]
}

#[tokio::test]
async fn session_version_sdk_stale_approved_snapshot_refuses_durable_conflict() {
    for conflict in ["main", "runtime", "context"] {
        let fixture = Fixture::new().await;
        let (mut parked, request) = parked("stale-compatible-approval", true);
        fixture.repo.save(&mut parked).await.unwrap();
        let mut session = fixture.approve(&request).await;
        assert!(session.pending_question.is_none());
        let before_memory = serde_json::to_value(&session).unwrap();
        let before_disk = future_durable(&fixture, &session.id, conflict).await;
        let config = Arc::new(PermissionConfig::new());
        assert!(config.temporary_grants().is_empty());
        let agent = fixture.agent(Some(config.clone()), true, false, None);
        let result = agent.run_session(&mut session).await;
        let error = result.expect_err("old caller data cannot authorize replay over a future durable contract");
        assert!(error.to_string().contains("session_execution_incompatible"), "{error}");
        assert_eq!(serde_json::to_value(&session).unwrap(), before_memory);
        assert_eq!(durable_pair(&fixture, &session.id).await, before_disk);
        assert!(config.temporary_grants().is_empty());
        assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
        assert!(fixture.probe.calls.lock().unwrap().is_empty());
        assert!(!fixture.directory.path().join("actions.log").exists());
    }
}

struct CompatibilityHook(Arc<AtomicUsize>);

#[async_trait]
impl AgentHook for CompatibilityHook {
    fn point(&self) -> AgentHookPoint { AgentHookPoint::BeforeSessionSetup }
    async fn run(&self, _point: AgentHookPoint, _payload: &HookPayload, _session: &Session) -> HookResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        HookResult::Continue
    }
}

#[tokio::test]
async fn session_version_raw_engine_stale_snapshot_refuses_before_hooks_or_session_mutation() {
    for conflict in ["main", "runtime", "context"] {
        for explicit_inputs in [false, true] {
            let fixture = Fixture::new().await;
            let mut session = Session::new("stale-compatible-engine", "test-model");
            session.add_message(Message::user("Existing history"));
            session.metadata.insert(bamboo_engine::session_app::chat::SESSION_START_SOURCE_METADATA_KEY.into(), "resume".into());
            fixture.repo.save(&mut session).await.unwrap();
            let before_memory = serde_json::to_value(&session).unwrap();
            let before_disk = future_durable(&fixture, &session.id, conflict).await;
            let calls = Arc::new(AtomicUsize::new(0));
            let mut hooks = HookRunner::new();
            hooks.register(Arc::new(CompatibilityHook(calls.clone())));
            let metrics = bamboo_metrics::MetricsCollector::spawn(Arc::new(
                bamboo_metrics::SqliteMetricsStorage::new(fixture.directory.path().join("raw-metrics.db"))
            ), 7);
            let agent = RuntimeAgent::builder()
                .storage(fixture.store.clone())
                .persistence(fixture.persistence.clone())
                .attachment_reader(fixture.store.clone())
                .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
                .metrics_collector(metrics)
                .config(Arc::new(tokio::sync::RwLock::new(bamboo_llm::Config::default())))
                .provider(fixture.probe.clone())
                .default_tools(Arc::new(RecordingExecutor {
                    probe: fixture.probe.clone(), config: Arc::new(PermissionConfig::new()),
                    typed: false, second_context: false,
                    action_log: fixture.directory.path().join("actions.log"),
                }))
                .hook_runner(Arc::new(hooks)).build().unwrap();
            let (event_tx, _event_rx) = mpsc::channel(1024);
            let request = ExecuteRequestBuilder::new("must not execute", event_tx, CancellationToken::new())
                .model("test-model").build();
            let result = if explicit_inputs {
                agent.execute_with_inputs(&mut session, request, None).await
            } else {
                agent.execute(&mut session, request).await
            };
            let error = result.expect_err("raw execution must check canonical compatibility before hooks");
            assert!(error.to_string().contains("session_execution_incompatible"), "{error}");
            assert_eq!(serde_json::to_value(&session).unwrap(), before_memory);
            assert_eq!(durable_pair(&fixture, &session.id).await, before_disk);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
            assert!(fixture.probe.calls.lock().unwrap().is_empty());
        }
    }
}


#[tokio::test]
async fn session_version_sdk_string_input_is_not_appended_over_a_future_durable_contract() {
    for conflict in ["main", "runtime", "context"] {
        for cancellable in [false, true] {
            let fixture = Fixture::new().await;
            let mut session = Session::new("stale-compatible-string-input", "test-model");
            session.add_message(Message::user("Existing history"));
            fixture.repo.save(&mut session).await.unwrap();
            let before_memory = serde_json::to_value(&session).unwrap();
            let before_disk = future_durable(&fixture, &session.id, conflict).await;
            let agent = fixture.agent(None, false, false, None);
            let result = if cancellable {
                agent.run_with_cancel(&mut session, "must not append", CancellationToken::new()).await
            } else {
                agent.run(&mut session, "must not append").await
            };
            let error = result.expect_err("the retained durable contract must be checked before borrowed input changes");
            assert!(error.to_string().contains("session_execution_incompatible"), "{error}");
            assert_eq!(serde_json::to_value(&session).unwrap(), before_memory);
            assert_eq!(durable_pair(&fixture, &session.id).await, before_disk);
            assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
            assert!(fixture.probe.calls.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn session_version_sdk_stream_reports_refusal_instead_of_an_empty_close() {
    for durable_conflict in [false, true] {
        for append in [false, true] {
            let fixture = Fixture::new().await;
            let mut session = Session::new("future-stream", "test-model");
            session.add_message(Message::user("Existing history"));
            fixture.repo.save(&mut session).await.unwrap();
            let before_disk = if durable_conflict {
                future_durable(&fixture, &session.id, "main").await
            } else {
                session = with_version(session, Some(99));
                durable_pair(&fixture, &session.id).await
            };
            let agent = fixture.agent(None, false, false, None);
            let id = session.id.clone();
            let mut events = if append {
                agent.run_stream(session, "must not persist")
            } else {
                agent.run_stream_session(session)
            };
            let seen = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                let mut seen = Vec::new();
                while let Some(event) = events.recv().await { seen.push(event); }
                seen
            }).await.expect("refusal closes the stream without a runner");
            assert!(seen.iter().any(|event| matches!(event, AgentEvent::Error { message, .. }
                if message.contains("session_execution_incompatible"))), "{seen:?}");
            assert_eq!(durable_pair(&fixture, &id).await, before_disk);
            assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
            assert!(fixture.probe.calls.lock().unwrap().is_empty());
        }
    }
}
