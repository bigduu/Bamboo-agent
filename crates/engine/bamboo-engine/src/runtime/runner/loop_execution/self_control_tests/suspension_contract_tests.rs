//! Tests-only integration fragment for self_control_tests/suspension_contract_tests.rs.
//! Parent module adds only `mod suspension_contract_tests;`.
//! Reuses Fixture::run(Arc<dyn LLMProvider>, ..), managed_config and
//! PublicEntryFixture. No new production API or may_suspend override is used:
//! the unknown native control hook must retain the new default `true` policy.

use super::*;

const INSPECT_CALL: &str = "suspension-inspect";
const FOLLOWUP_INPUT: &str = "A new real user request after the refused managed Start.";

#[derive(Clone, Copy)]
enum SuspensionScript {
    StartThenInspect,
    InspectWithoutStart,
}

struct SuspensionProvider {
    script: SuspensionScript,
    storage: Arc<dyn Storage>,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
    start_observed_accepted: Mutex<Option<bool>>,
}

impl SuspensionProvider {
    fn new(script: SuspensionScript, storage: Arc<dyn Storage>) -> Self {
        Self {
            script,
            storage,
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            peak: AtomicUsize::new(0),
            start_observed_accepted: Mutex::new(None),
        }
    }
}

fn suspension_pair<'a>(messages: &'a [Message], id: &str) -> &'a Message {
    let mut pairs = messages.iter().filter(|message| {
        message.role == Role::Tool && message.tool_call_id.as_deref() == Some(id)
    });
    let pair = pairs.next().expect("one paired result");
    assert!(pairs.next().is_none(), "duplicate pair for {id}");
    pair
}

#[async_trait]
impl LLMProvider for SuspensionProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        use futures::StreamExt;

        let round = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        assert_eq!(active, 1);
        let guard = ActiveStream(self.active.clone());
        assert!(tools
            .iter()
            .any(|schema| schema.function.name == "loop_control"));
        let durable = self
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        let state = read_loop_state(&durable).unwrap();
        let chunk = match (self.script, round) {
            (SuspensionScript::StartThenInspect, 1) => {
                assert!(
                    state.is_none(),
                    "host opt-in alone cannot start a generation"
                );
                start_call(2, 6)
            }
            (SuspensionScript::StartThenInspect, 2) => {
                // Both branches are valid on the RED and GREEN sources. Never
                // use paired_result(), which assumes owner success and would
                // prevent the intended refusal from reaching the real hook.
                let start = suspension_pair(messages, "start");
                let accepted = start.tool_success.expect("truthful paired status");
                *self.start_observed_accepted.lock().unwrap() = Some(accepted);
                if accepted {
                    let receipt: LoopControlReceipt = serde_json::from_str(&start.content).unwrap();
                    let state = state.as_ref().expect("accepted start has durable state");
                    assert_eq!(receipt.generation, state.generation);
                    assert_eq!(
                        (state.iterations_started, state.iterations_completed),
                        (1, 0)
                    );
                } else {
                    assert!(state.is_none(), "refusal must not reserve a generation");
                    let body: Value = serde_json::from_str(&start.content).unwrap();
                    assert!(body["error"].as_str().is_some_and(|code| !code.is_empty()));
                    assert!(admitted_objectives(messages).is_empty());
                }
                control_call(INSPECT_CALL, json!({"action":"status"}))
            }
            (SuspensionScript::InspectWithoutStart, 1) => {
                assert!(state.is_none());
                control_call(INSPECT_CALL, json!({"action":"status"}))
            }
            _ => panic!("unexpected provider call {round}; the ordinary hook must suspend"),
        };
        let stream = futures::stream::iter([Ok(chunk), Ok(LLMChunk::Done)]).map(move |item| {
            let _keep_admitted = &guard;
            item
        });
        Ok(Box::pin(stream))
    }
}

struct UnknownNativeSuspensionHook {
    call_id: &'static str,
    control: LoopControlConfig,
    calls: AtomicUsize,
    witnessed: Mutex<Vec<(Option<LoopState>, Option<String>)>>,
}

#[async_trait]
impl bamboo_agent_core::AgentHook for UnknownNativeSuspensionHook {
    fn point(&self) -> bamboo_domain::AgentHookPoint {
        bamboo_domain::AgentHookPoint::AfterToolExecution
    }

    fn matches(&self, payload: &bamboo_domain::HookPayload) -> bool {
        matches!(payload, bamboo_domain::HookPayload::ToolResult { tool_call_id, .. }
            if tool_call_id == self.call_id)
    }

    // Do not implement may_suspend here. These tests compile before that API
    // exists, and an unknown native hook must remain conservatively classified.
    async fn run(
        &self,
        _: bamboo_domain::AgentHookPoint,
        _: &bamboo_domain::HookPayload,
        session: &Session,
    ) -> bamboo_domain::HookResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.witnessed.lock().unwrap().push((
            read_loop_state(session).unwrap(),
            self.control.generation().map(str::to_owned),
        ));
        bamboo_domain::HookResult::Suspend {
            reason: "ordinary native hook still owns its suspension decision".into(),
        }
    }
}

fn install_unknown_suspension_hook(
    fixture: &mut Fixture,
    call_id: &'static str,
) -> (LoopControlConfig, Arc<UnknownNativeSuspensionHook>) {
    let control = fixture
        .config
        .as_ref()
        .unwrap()
        .loop_control
        .as_ref()
        .unwrap()
        .clone();
    let hook = Arc::new(UnknownNativeSuspensionHook {
        call_id,
        control: control.clone(),
        calls: AtomicUsize::new(0),
        witnessed: Mutex::new(Vec::new()),
    });
    let mut runner = crate::runtime::HookRunner::new();
    runner.register(hook.clone());
    fixture.config.as_mut().unwrap().hook_runner = Arc::new(runner);
    (control, hook)
}

// The existing public fixture owns the real directory, Store and Router.
// Only the already-existing Fixture runner is used for the first activation.
async fn managed_fixture_on_public(public: &PublicEntryFixture) -> Fixture {
    let registration = public
        .router
        .register_run(SESSION_ID, "suspension-contract-owner")
        .await
        .unwrap();
    let config = managed_config(&public.session, &registration, public.storage.clone(), 2, 6);
    Fixture {
        // Fixture's normal TempDir field is not the Store owner in this bridge;
        // `public` remains alive throughout both real activations below.
        _dir: tempfile::tempdir().unwrap(),
        session: public.session.clone(),
        config: Some(config),
        storage: public.storage.clone(),
        registration: Some(registration),
    }
}

struct OrdinaryUserProvider {
    storage: Arc<dyn Storage>,
    calls: AtomicUsize,
}

#[async_trait]
impl LLMProvider for OrdinaryUserProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        assert_eq!(
            self.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "one ordinary user request needs only one answer"
        );
        let durable = self
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        assert!(read_loop_state(&durable).unwrap().is_none());
        assert!(messages.iter().any(|message| {
            message.role == Role::User
                && message.content == FOLLOWUP_INPUT
                && !bamboo_domain::session::is_system_resume_message(message)
        }));
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token(
                "The new ordinary user request is usable.".into(),
            )),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn verify_public_genuine_user_continues(
    mut fixture: Fixture,
    public: &mut PublicEntryFixture,
) {
    fixture.registration.take().unwrap().abandon().await;
    assert!(public.router.current_run_id(SESSION_ID).await.is_none());
    public.session = public
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(read_loop_state(&public.session).unwrap().is_none());
    // A real, canonical external User is persisted before execution. No loop
    // metadata, counters, pending markers or suspension decision is cleared.
    public.session.add_message(Message::user(FOLLOWUP_INPUT));
    public
        .persistence
        .checkpoint_runtime_session(&mut public.session)
        .await
        .unwrap();
    let provider = Arc::new(OrdinaryUserProvider {
        storage: public.storage.clone(),
        calls: AtomicUsize::new(0),
    });
    let (tx, mut rx) = mpsc::channel(512);
    let request =
        crate::runtime::ExecuteRequestBuilder::new(FOLLOWUP_INPUT, tx, CancellationToken::new())
            .model("synthetic-only")
            .provider_override(provider.clone())
            .app_data_dir(public.dir.path().join("app"))
            .run_budget(bamboo_config::RunBudgetConfig {
                max_rounds: Some(1),
                ..Default::default()
            })
            .build();
    assert!(request.loop_control.is_none());
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        public.agent.execute_direct(&mut public.session, request),
    )
    .await
    .expect("the ordinary public request must not hang on the old owner");
    assert!(
        result.is_ok(),
        "normal explicit user request must remain executable: {result:?}"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        public.provider.calls.load(Ordering::SeqCst),
        0,
        "never fall back to the fixture's unrelated provider script"
    );
    let mut completes = 0;
    while let Ok(event) = rx.try_recv() {
        completes += usize::from(matches!(event, AgentEvent::Complete { .. }));
    }
    assert_eq!(completes, 1);
    assert!(read_loop_state(&public.session).unwrap().is_none());
    let durable = public
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(read_loop_state(&durable).unwrap().is_none());
    assert!(durable
        .messages
        .iter()
        .any(|message| message.role == Role::User && message.content == FOLLOWUP_INPUT));
    assert!(public.router.current_run_id(SESSION_ID).await.is_none());
}

async fn reject_start_before_unknown_hook_reserves_generation(call_id: &'static str) {
    let mut public = PublicEntryFixture::new().await;
    let mut fixture = managed_fixture_on_public(&public).await;
    let (control, hook) = install_unknown_suspension_hook(&mut fixture, call_id);
    let provider = Arc::new(SuspensionProvider::new(
        SuspensionScript::StartThenInspect,
        fixture.storage.clone(),
    ));
    let (result, events) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        fixture.run(provider.clone(), CancellationToken::new()),
    )
    .await
    .expect("the original ordinary hook must suspend deterministically");
    assert!(
        result.is_ok(),
        "ordinary suspension retains the existing runner result: {result:?}"
    );
    // This fixture invokes the runner below Agent's final host save. Persist
    // its real suspended snapshot at that same existing boundary; do not
    // expect an earlier tool checkpoint to include a later native decision.
    public
        .persistence
        .checkpoint_runtime_session(&mut fixture.session)
        .await
        .unwrap();
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        if call_id == "start" { 1 } else { 2 }
    );
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        hook.calls.load(Ordering::SeqCst),
        1,
        "do not disable the original hook"
    );
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    let pair = suspension_pair(&durable.messages, "start");
    assert_eq!(
        pair.tool_success,
        Some(false),
        "Start must refuse before reserving a generation; observed state={:?}",
        read_loop_state(&durable).unwrap()
    );
    let refusal: Value = serde_json::from_str(&pair.content).expect("explicit structured refusal");
    assert!(refusal["error"]
        .as_str()
        .is_some_and(|code| !code.is_empty()));
    let mut published = events.iter().filter_map(|event| match event {
        AgentEvent::ToolComplete {
            tool_call_id,
            result,
        } if tool_call_id == "start" => Some(result),
        _ => None,
    });
    let published = {
        let result = published
            .next()
            .expect("publish one truthful Start refusal");
        assert!(published.next().is_none());
        result
    };
    assert!(!published.success);
    assert_eq!(published.result, pair.content);
    for session in [&fixture.session, &durable] {
        assert!(read_loop_state(session).unwrap().is_none());
        assert!(admitted_objectives(&session.messages).is_empty());
        assert_eq!(
            session.agent_runtime_state.as_ref().unwrap().status,
            bamboo_domain::AgentStatusState::Suspended
        );
        assert_eq!(
            session
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("hook_suspended")
        );
        assert!(crate::runtime::counted_loop::reject_recovery(session).is_ok());
    }
    assert!(control.generation().is_none());
    assert!(control.active_counted().is_none());
    let witnessed = hook.witnessed.lock().unwrap();
    assert_eq!(witnessed.len(), 1);
    assert!(
        witnessed[0].0.is_none(),
        "no managed state exists when the real hook runs"
    );
    assert!(
        witnessed[0].1.is_none(),
        "no active OnceLock generation was reserved"
    );
    drop(witnessed);
    if call_id == INSPECT_CALL {
        assert_eq!(
            *provider.start_observed_accepted.lock().unwrap(),
            Some(false)
        );
    }
    verify_public_genuine_user_continues(fixture, &mut public).await;
}

#[tokio::test]
async fn unknown_post_start_suspend_refuses_generation_before_zero_iteration_fence() {
    reject_start_before_unknown_hook_reserves_generation("start").await;
}

#[tokio::test]
async fn unknown_post_inspect_suspend_refuses_generation_before_pending_iteration_fence() {
    reject_start_before_unknown_hook_reserves_generation(INSPECT_CALL).await;
}

#[tokio::test]
async fn host_opt_in_without_start_preserves_ordinary_native_suspension() {
    let mut fixture = Fixture::new(2, 6).await;
    let (control, hook) = install_unknown_suspension_hook(&mut fixture, INSPECT_CALL);
    let provider = Arc::new(SuspensionProvider::new(
        SuspensionScript::InspectWithoutStart,
        fixture.storage.clone(),
    ));
    let persistence = fixture
        .config
        .as_ref()
        .unwrap()
        .persistence
        .clone()
        .unwrap();
    let (result, events) = fixture
        .run(provider.clone(), CancellationToken::new())
        .await;
    assert!(result.is_ok(), "{result:?}");
    // Mirror the existing host final save after this lower-level runner.
    persistence
        .checkpoint_runtime_session(&mut fixture.session)
        .await
        .unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert!(!events.iter().any(|event| matches!(event,
        AgentEvent::ToolStart { tool_call_id, .. } if tool_call_id == "start")));
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    for session in [&fixture.session, &durable] {
        assert!(read_loop_state(session).unwrap().is_none());
        assert!(admitted_objectives(&session.messages).is_empty());
        assert_eq!(
            session.agent_runtime_state.as_ref().unwrap().status,
            bamboo_domain::AgentStatusState::Suspended
        );
        assert!(crate::runtime::counted_loop::reject_recovery(session).is_ok());
    }
    assert!(control.generation().is_none());
    assert!(control.active_counted().is_none());
    assert_eq!(
        suspension_pair(&durable.messages, INSPECT_CALL).tool_success,
        Some(true)
    );
    fixture.close().await;
}
