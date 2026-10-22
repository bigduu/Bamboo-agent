//! Public SDK API regressions; no real provider or physical tool is used.
use super::*;

fn with_version(session: Session, version: Option<u32>) -> Session {
    let mut raw = serde_json::to_value(session).unwrap();
    match version {
        Some(version) => raw["session_version"] = json!(version),
        None => {
            raw.as_object_mut().unwrap().remove("session_version");
        }
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
        assert!(
            error.to_string().contains("session_execution_incompatible"),
            "{error}"
        );
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
    assert!(
        error.to_string().contains("session_execution_incompatible"),
        "{error}"
    );
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
    agent
        .run(&mut session, "Continue this compatible legacy session")
        .await
        .unwrap();
    assert!(fixture.probe.provider_calls.load(Ordering::SeqCst) > 0);
    assert!(session
        .messages
        .iter()
        .any(|message| message.content == "Existing history"));
    assert!(serde_json::to_value(&session)
        .unwrap()
        .get("session_version")
        .is_none());
    let loaded = fixture
        .store
        .load_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    assert!(serde_json::to_value(loaded)
        .unwrap()
        .get("session_version")
        .is_none());
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
        let error = agent
            .run(&mut session, "must not reset this retained schema")
            .await
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
    let name = if conflict == "main" {
        "session.json"
    } else {
        "runtime.json"
    };
    let path = directory.join(name);
    let mut raw: Value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
    if name == "session.json" {
        // Re-encode this owned fixture as valid flat legacy Main. A retained
        // compact header would no longer match its framing after JSON rewrite.
        raw.as_object_mut()
            .unwrap()
            .remove("_bamboo_main_authority");
    }
    match conflict {
        "main" | "runtime" => raw["session_version"] = json!(99),
        "agent" => {
            let mut runtime = AgentRuntimeState::new(id);
            runtime.version = 99;
            raw["agent_runtime_state"] = serde_json::to_value(runtime).unwrap();
        }
        "context" => {
            raw["model_context_state"] = serde_json::to_value(bamboo_domain::ModelContextState {
                schema_version: 99,
                state_revision: 7,
                ..Default::default()
            })
            .unwrap()
        }
        _ => unreachable!(),
    }
    tokio::fs::write(path, serde_json::to_vec_pretty(&raw).unwrap())
        .await
        .unwrap();
    assert!(fixture
        .store
        .load_execution_admission_snapshot(id)
        .await
        .expect("future fixture retains valid canonical storage framing")
        .is_some());
    [
        tokio::fs::read(directory.join("session.json"))
            .await
            .unwrap(),
        tokio::fs::read(directory.join("runtime.json"))
            .await
            .unwrap(),
    ]
}

async fn durable_pair(fixture: &Fixture, id: &str) -> [Vec<u8>; 2] {
    let directory = fixture.directory.path().join("sessions").join(id);
    [
        tokio::fs::read(directory.join("session.json"))
            .await
            .unwrap(),
        tokio::fs::read(directory.join("runtime.json"))
            .await
            .unwrap(),
    ]
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
        let error = result
            .expect_err("old caller data cannot authorize replay over a future durable contract");
        assert!(
            error.to_string().contains("session_execution_incompatible"),
            "{error}"
        );
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
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::AfterSessionSetup
    }
    async fn run(
        &self,
        _point: AgentHookPoint,
        _payload: &HookPayload,
        _session: &Session,
    ) -> HookResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        HookResult::Continue
    }
}

#[tokio::test]
async fn session_version_raw_engine_stale_snapshot_refuses_before_hooks_or_session_mutation() {
    for conflict in ["compatible", "main", "runtime", "context"] {
        for explicit_inputs in [false, true] {
            let fixture = Fixture::new().await;
            let mut session = Session::new("stale-compatible-engine", "test-model");
            session.add_message(Message::user("Existing history"));
            session.metadata.insert(
                bamboo_engine::session_app::chat::SESSION_START_SOURCE_METADATA_KEY.into(),
                "resume".into(),
            );
            fixture.repo.save(&mut session).await.unwrap();
            let before_memory = serde_json::to_value(&session).unwrap();
            let before_disk = if conflict == "compatible" {
                durable_pair(&fixture, &session.id).await
            } else {
                future_durable(&fixture, &session.id, conflict).await
            };
            let calls = Arc::new(AtomicUsize::new(0));
            let mut hooks = HookRunner::new();
            hooks.register(Arc::new(CompatibilityHook(calls.clone())));
            let metrics = bamboo_metrics::MetricsCollector::spawn(
                Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                    fixture.directory.path().join("raw-metrics.db"),
                )),
                7,
            );
            let agent = RuntimeAgent::builder()
                .storage(fixture.store.clone())
                .persistence(fixture.persistence.clone())
                .attachment_reader(fixture.store.clone())
                .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
                .metrics_collector(metrics)
                .config(Arc::new(tokio::sync::RwLock::new(
                    bamboo_llm::Config::default(),
                )))
                .provider(fixture.probe.clone())
                .default_tools(Arc::new(RecordingExecutor {
                    probe: fixture.probe.clone(),
                    config: Arc::new(PermissionConfig::new()),
                    typed: false,
                    second_context: false,
                    action_log: fixture.directory.path().join("actions.log"),
                }))
                .hook_runner(Arc::new(hooks))
                .build()
                .unwrap();
            let (event_tx, _event_rx) = mpsc::channel(1024);
            let request =
                ExecuteRequestBuilder::new("must not execute", event_tx, CancellationToken::new())
                    .model("test-model")
                    .build();
            let result = if explicit_inputs {
                agent.execute_with_inputs(&mut session, request, None).await
            } else {
                agent.execute(&mut session, request).await
            };
            if conflict == "compatible" {
                result.expect("compatible raw execution remains available");
                assert!(
                    calls.load(Ordering::SeqCst) > 0,
                    "the same native hook must be reachable"
                );
                assert!(fixture.probe.provider_calls.load(Ordering::SeqCst) > 0);
                continue;
            }
            let error =
                result.expect_err("raw execution must check canonical compatibility before hooks");
            assert!(
                error.to_string().contains("session_execution_incompatible"),
                "{error}"
            );
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
                agent
                    .run_with_cancel(&mut session, "must not append", CancellationToken::new())
                    .await
            } else {
                agent.run(&mut session, "must not append").await
            };
            let error = result.expect_err(
                "the retained durable contract must be checked before borrowed input changes",
            );
            assert!(
                error.to_string().contains("session_execution_incompatible"),
                "{error}"
            );
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
                while let Some(event) = events.recv().await {
                    seen.push(event);
                }
                seen
            })
            .await
            .expect("refusal closes the stream without a runner");
            assert!(
                seen.iter()
                    .any(|event| matches!(event, AgentEvent::Error { message, .. }
                if message.contains("session_execution_incompatible"))),
                "{seen:?}"
            );
            assert_eq!(durable_pair(&fixture, &id).await, before_disk);
            assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
            assert!(fixture.probe.calls.lock().unwrap().is_empty());
        }
    }
}

/// Publish a real retained pair at the second admission read: the first read
/// has already returned compatible, and the public SDK producer has appended.
/// No sleeps, fabricated admission result or private Engine bypass is used.
struct RacedSdkPersistence {
    inner: Arc<dyn RuntimeSessionPersistence>,
    directory: std::path::PathBuf,
    publication: [Vec<u8>; 2],
    fail_read: bool,
    reads: AtomicUsize,
    saves: AtomicUsize,
}

#[async_trait]
impl RuntimeSessionPersistence for RacedSdkPersistence {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        self.inner.save_runtime_session(session).await
    }

    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }

    async fn load_runtime_admission_snapshot(&self, id: &str) -> std::io::Result<Option<Session>> {
        if self.reads.fetch_add(1, Ordering::SeqCst) == 1 {
            if self.fail_read {
                return Err(std::io::Error::other(
                    "injected non-compatibility admission read failure",
                ));
            }
            for (name, bytes) in ["session.json", "runtime.json"]
                .into_iter()
                .zip(&self.publication)
            {
                tokio::fs::write(self.directory.join(name), bytes).await?;
            }
        }
        self.inner.load_runtime_admission_snapshot(id).await
    }
}

fn sdk_contract_files(
    directory: &std::path::Path,
) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(
        root: &std::path::Path,
        current: &std::path::Path,
        out: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for entry in std::fs::read_dir(current).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    visit(directory, directory, &mut out);
    out
}

async fn defaults_race_agent(fixture: &Fixture) -> Agent {
    std::fs::write(fixture.directory.path().join("config.json"), r#"{"provider":"anthropic","providers":{"anthropic":{"api_key":"fixture","model":"test-model"}}}"#).unwrap();
    Agent::builder()
        .provider(fixture.probe.clone())
        .model("test-model")
        .with_defaults_for_data_dir(fixture.directory.path().into())
        .await
        .unwrap()
        .build()
        .unwrap()
}

#[tokio::test]
async fn session_version_sdk_race_rejects_without_borrowed_input_or_checkpoint_side_effects() {
    let mut failures = Vec::new();
    for (flavor, replay) in [
        ("plain", false),
        ("defaults", false),
        ("typed", false),
        ("plain", true),
    ] {
        for cancellable in [false, true] {
            for conflict in ["main", "runtime", "agent", "context"] {
                let fixture = Fixture::new().await;
                let id = format!("sdk-race-{flavor}-{replay}-{cancellable}-{conflict}");
                if replay {
                    let (mut pending, request) = parked(&id, true);
                    fixture.repo.save(&mut pending).await.unwrap();
                    fixture.approve(&request).await;
                } else {
                    let mut fresh = Session::new(&id, "test-model");
                    fresh.add_message(Message::user("retained caller history"));
                    fixture.repo.save(&mut fresh).await.unwrap();
                }
                let config = Arc::new(PermissionConfig::new());
                let mut agent = if flavor == "plain" {
                    fixture.agent(Some(config.clone()), replay, false, None)
                } else {
                    defaults_race_agent(&fixture).await
                };
                let mut session = agent.storage().load_session(&id).await.unwrap().unwrap();
                let before_memory = serde_json::to_value(&session).unwrap();
                let before_pair = durable_pair(&fixture, &id).await;
                let publication = future_durable(&fixture, &id, conflict).await;
                let directory = fixture.directory.path().join("sessions").join(&id);
                let future_files = sdk_contract_files(&directory);
                let index = tokio::fs::read(fixture.store.index_path()).await.unwrap();
                for (name, bytes) in ["session.json", "runtime.json"]
                    .into_iter()
                    .zip(before_pair)
                {
                    tokio::fs::write(directory.join(name), bytes).await.unwrap();
                }
                assert!(
                    agent
                        .persistence()
                        .load_runtime_admission_snapshot(&id)
                        .await
                        .unwrap()
                        .unwrap()
                        .execution_compatibility()
                        .execution_allowed
                );
                let race = Arc::new(RacedSdkPersistence {
                    inner: agent.persistence().clone(),
                    directory: directory.clone(),
                    publication,
                    fail_read: false,
                    reads: AtomicUsize::new(0),
                    saves: AtomicUsize::new(0),
                });
                agent.inner = agent.inner.with_execution_persistence(race.clone());
                let error = if flavor == "typed" {
                    let input =
                        SdkSkillInput::new("new SDK input must not survive refusal", vec![]);
                    if cancellable {
                        agent
                            .run_with_skills_and_cancel(
                                &mut session,
                                input,
                                CancellationToken::new(),
                            )
                            .await
                            .map_err(|error| error.to_string())
                    } else {
                        agent
                            .run_with_skills(&mut session, input)
                            .await
                            .map_err(|error| error.to_string())
                    }
                } else if cancellable {
                    agent
                        .run_with_cancel(
                            &mut session,
                            "new SDK input must not survive refusal",
                            CancellationToken::new(),
                        )
                        .await
                        .map_err(|error| error.to_string())
                } else {
                    agent
                        .run(&mut session, "new SDK input must not survive refusal")
                        .await
                        .map_err(|error| error.to_string())
                }
                .expect_err("the real second canonical read observes the raced future writer");
                assert!(
                    error.contains("session_execution_incompatible"),
                    "{flavor}/{conflict}: {error}"
                );
                assert_eq!(race.reads.load(Ordering::SeqCst), 2);
                assert_eq!(race.saves.load(Ordering::SeqCst), 0);
                assert_eq!(sdk_contract_files(&directory), future_files);
                assert_eq!(
                    tokio::fs::read(fixture.store.index_path()).await.unwrap(),
                    index
                );
                assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
                assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
                assert!(fixture.probe.calls.lock().unwrap().is_empty());
                assert!(config.temporary_grants().is_empty());
                assert!(!fixture.directory.path().join("actions.log").exists());
                let cached = fixture
                    .repo
                    .cache()
                    .get(&id)
                    .expect("seeded repository cache");
                assert!(!cached.read().messages.iter().any(|message| message
                    .content
                    .contains("new SDK input must not survive refusal")));
                let memory_retained = serde_json::to_value(&session).unwrap() == before_memory;
                eprintln!("SDK race {flavor}/replay={replay}/cancel={cancellable}/{conflict}: memory_retained={memory_retained}; source_files_retained=true; provider=0; saves=0");
                if !memory_retained {
                    failures.push(format!("{flavor}/replay={replay}/cancel={cancellable}/{conflict}: borrowed Session changed despite admission refusal"));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn session_version_sdk_race_compatible_input_still_appends_and_runs_once() {
    for cancellable in [false, true] {
        let fixture = Fixture::new().await;
        let mut session = Session::new(format!("sdk-race-compatible-{cancellable}"), "test-model");
        session.add_message(Message::user("retained history"));
        fixture.repo.save(&mut session).await.unwrap();
        let race = Arc::new(RacedSdkPersistence {
            inner: fixture.persistence.clone(),
            directory: fixture.directory.path().join("sessions").join(&session.id),
            publication: durable_pair(&fixture, &session.id).await,
            fail_read: false,
            reads: AtomicUsize::new(0),
            saves: AtomicUsize::new(0),
        });
        let agent = fixture.agent(None, false, false, Some(race));
        if cancellable {
            agent
                .run_with_cancel(
                    &mut session,
                    "accepted raced compatible input",
                    CancellationToken::new(),
                )
                .await
                .unwrap();
        } else {
            agent
                .run(&mut session, "accepted raced compatible input")
                .await
                .unwrap();
        }
        assert_eq!(
            session
                .messages
                .iter()
                .filter(|message| message.content == "accepted raced compatible input")
                .count(),
            1
        );
        let durable = fixture
            .store
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            durable
                .messages
                .iter()
                .filter(|message| message.content == "accepted raced compatible input")
                .count(),
            1
        );
        assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn session_version_sdk_race_other_admission_errors_keep_original_append_semantics() {
    for cancellable in [false, true] {
        let fixture = Fixture::new().await;
        let mut session = Session::new(format!("sdk-race-other-error-{cancellable}"), "test-model");
        session.add_message(Message::user("retained history"));
        fixture.repo.save(&mut session).await.unwrap();
        let before_disk = durable_pair(&fixture, &session.id).await;
        let race = Arc::new(RacedSdkPersistence {
            inner: fixture.persistence.clone(),
            directory: fixture.directory.path().join("sessions").join(&session.id),
            publication: before_disk.clone(),
            fail_read: true,
            reads: AtomicUsize::new(0),
            saves: AtomicUsize::new(0),
        });
        let agent = fixture.agent(None, false, false, Some(race));
        let error = if cancellable {
            agent
                .run_with_cancel(
                    &mut session,
                    "legacy append on other admission error",
                    CancellationToken::new(),
                )
                .await
        } else {
            agent
                .run(&mut session, "legacy append on other admission error")
                .await
        }
        .unwrap_err();
        assert!(
            !error.to_string().contains("session_execution_incompatible"),
            "{error}"
        );
        assert_eq!(
            session
                .messages
                .iter()
                .filter(|message| message.content == "legacy append on other admission error")
                .count(),
            1
        );
        assert_eq!(durable_pair(&fixture, &session.id).await, before_disk);
        assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
    }
}
