//! Tests-only child of managed_running_terminal_tests.
//! Install as managed_running_terminal_tests/public_direct_running_tests.rs;
//! the sole integrator adds `mod public_direct_running_tests;` to that parent.
//! Uses only APIs already present on 8da/d391. No resource domain, stop proof,
//! terminal status, logical owner, process or background provider is fabricated.

use super::*;

const QUEUED_GENERATION: u64 = 17;
const LATER_GENERATION: u64 = 18;
const GENUINE_FOLLOWUP: &str = "A real user asks to continue after the failed custom tool.";

struct DirectRecordingSpawner {
    reservations: AtomicUsize,
    launches: Arc<AtomicUsize>,
}

#[async_trait]
impl crate::session_activation::SessionActivationSpawner for DirectRecordingSpawner {
    async fn reserve_activation(
        &self,
        target_session_id: &str,
        generation: u64,
    ) -> Result<
        crate::session_activation::SessionActivationReserveOutcome,
        bamboo_domain::SessionActivationError,
    > {
        assert_eq!(target_session_id, SESSION_ID);
        assert!(generation >= QUEUED_GENERATION);
        let ordinal = self.reservations.fetch_add(1, Ordering::SeqCst) + 1;
        let launches = self.launches.clone();
        Ok(
            crate::session_activation::SessionActivationReserveOutcome::Reserved(
                crate::session_activation::SessionActivationLaunch::new(
                    format!("forbidden-direct-running-successor-{ordinal}"),
                    move || {
                        launches.fetch_add(1, Ordering::SeqCst);
                    },
                ),
            ),
        )
    }
}

// Gate the real provider's first admission, before its first scripted response.
// The caller captures the owner minted by execute_direct and queues a wake while
// that owner is definitely active. This is not a synthesized owner or receipt.
struct FirstDirectProviderGate {
    inner: Arc<RunningProvider>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    gated: AtomicBool,
}

#[async_trait]
impl LLMProvider for FirstDirectProviderGate {
    async fn chat_stream(
        &self,
        messages: &[Message],
        schemas: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        if !self.gated.swap(true, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner
            .chat_stream(messages, schemas, max_output_tokens, model)
            .await
    }
}

async fn assert_public_direct_retains_unknown_running(sequence: RunningSequence) {
    let mut fixture = PublicEntryFixture::new().await;
    assert_eq!(fixture.session.kind, bamboo_domain::SessionKind::Root);
    let probe = Arc::new(RunningProbe::default());
    let tools = bamboo_tools::BuiltinToolExecutor::new();
    tools
        .registry()
        .register(IgnoresSuspensionRunningTool {
            flavor: CompletionFlavor::Detached,
            wrapping: ResultWrapping::Raw,
            expect_started: true,
            probe: probe.clone(),
            storage: fixture.storage.clone(),
        })
        .unwrap();
    tools
        .registry()
        .register(UnadmittedSiblingTool(probe.clone()))
        .unwrap();
    let provider = Arc::new(RunningProvider {
        sequence,
        with_sibling: true,
        probe: probe.clone(),
        storage: fixture.storage.clone(),
        calls: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
        peak: AtomicUsize::new(0),
        seen: Mutex::new(Vec::new()),
    });
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let gated = Arc::new(FirstDirectProviderGate {
        inner: provider.clone(),
        entered: entered.clone(),
        release: release.clone(),
        gated: AtomicBool::new(false),
    });
    let launches = Arc::new(AtomicUsize::new(0));
    let spawner = Arc::new(DirectRecordingSpawner {
        reservations: AtomicUsize::new(0),
        launches: launches.clone(),
    });
    fixture.router.set_spawner(spawner.clone()).await;

    let (tx, mut rx) = mpsc::channel(512);
    let mut request = fixture.request(tx);
    request.tools = Some(Arc::new(tools));
    request.provider_override = Some(gated);
    request.loop_control = Some(LoopControlConfig::new(2, 8));
    request.run_budget = Some(bamboo_config::RunBudgetConfig {
        max_rounds: Some(8),
        ..Default::default()
    });
    let agent = fixture.agent.clone();
    let router = fixture.router.clone();
    let storage = fixture.storage.clone();
    let (result, mut events, exact_owner) = {
        let run = agent.execute_direct(&mut fixture.session, request);
        tokio::pin!(run);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                result = &mut run => {
                    panic!("public direct fixture stopped before its first provider: {result:?}");
                }
                _ = entered.notified() => {}
            }
        })
        .await
        .expect("public direct must acquire its real owner before the provider");
        let exact_owner = router
            .current_run_id(SESSION_ID)
            .await
            .expect("execute_direct owns the exact registered activation");
        assert_eq!(
            bamboo_domain::SessionActivationPort::request_activation(
                router.as_ref(),
                SESSION_ID,
                QUEUED_GENERATION,
            )
            .await
            .unwrap(),
            bamboo_domain::SessionActivationDisposition::ActiveNotified
        );
        assert_eq!(spawner.reservations.load(Ordering::SeqCst), 0);
        assert_eq!(launches.load(Ordering::SeqCst), 0);
        release.notify_one();

        let mut events = Vec::new();
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    result = &mut run => break result,
                    event = rx.recv() => {
                        if let Some(event) = event {
                            record_running_publication(&probe, &event);
                            if matches!(&event, AgentEvent::ToolComplete { tool_call_id, result }
                                if tool_call_id == "start" && result.success)
                            {
                                Fixture::observe_event(storage.as_ref(), &event).await;
                            }
                            events.push(event);
                        }
                    }
                }
            }
        })
        .await
        .expect("unknown Running must return terminal failure finitely");
        (result, events, exact_owner)
    };
    while let Ok(event) = rx.try_recv() {
        record_running_publication(&probe, &event);
        if matches!(&event, AgentEvent::ToolComplete { tool_call_id, result }
            if tool_call_id == "start" && result.success)
        {
            Fixture::observe_event(storage.as_ref(), &event).await;
        }
        events.push(event);
    }

    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    // On 8da this actual witness is kill=0. Do not hide it behind an expected
    // Err, a new-token API, a fabricated resource scope, or a provider panic.
    assert_running_cleanup(probe.snapshot(), CompletionFlavor::Detached);
    assert!(result.is_err(), "{result:?}");
    let error = result.unwrap_err();
    assert!(
        !error.is_cancelled(),
        "the fixture never requested user cancellation"
    );
    assert!(!error.is_hook_suspended());
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::Complete { .. }))
            .count(),
        0
    );
    assert_eq!(probe.siblings.load(Ordering::SeqCst), 0);
    assert_eq!(running_pair(&fixture.session).tool_success, Some(false));
    assert_no_running_wait(&fixture.session);
    let (provider_calls, expected_counts) = match sequence {
        RunningSequence::SameBatchAsStart => (1, (0, 0, 1)),
        RunningSequence::AfterStart => (2, (1, 0, 2)),
        _ => unreachable!("this matrix covers only active management"),
    };
    assert_eq!(provider.calls.load(Ordering::SeqCst), provider_calls);
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 0);
    let state = read_loop_state(&fixture.session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Failed);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        expected_counts
    );
    assert_eq!(
        fixture.session.agent_runtime_state.as_ref().unwrap().status,
        bamboo_domain::AgentStatusState::Failed
    );
    let start_receipt = paired_result(&fixture.session.messages, "start");
    let durable = storage.load_session(SESSION_ID).await.unwrap().unwrap();
    assert_eq!(read_loop_state(&durable).unwrap(), Some(state.clone()));
    assert_eq!(paired_result(&durable.messages, "start"), start_receipt);

    // The public direct future has fully returned. These awaited APIs share
    // the real router lock; no yield/sleep guesses at detached cleanup timing.
    assert_eq!(
        router.current_run_id(SESSION_ID).await.as_deref(),
        Some(exact_owner.as_str()),
        "a cooperative kill callback has no physical-stop receipt"
    );
    assert_eq!(spawner.reservations.load(Ordering::SeqCst), 0);
    assert_eq!(launches.load(Ordering::SeqCst), 0);

    let genuine = Message::user(GENUINE_FOLLOWUP);
    assert!(!bamboo_domain::session::is_system_resume_message(&genuine));
    assert!(genuine
        .metadata
        .as_ref()
        .is_none_or(|meta| meta.get("runtime_kind").and_then(Value::as_str)
            != Some("counted_loop_iteration")));
    fixture.session.add_message(genuine);
    storage.save_session(&fixture.session).await.unwrap();
    let after_user = storage.load_session(SESSION_ID).await.unwrap().unwrap();
    assert_eq!(
        after_user.messages.last().unwrap().content,
        GENUINE_FOLLOWUP
    );
    assert_eq!(read_loop_state(&after_user).unwrap(), Some(state));
    assert_eq!(paired_result(&after_user.messages, "start"), start_receipt);

    let next = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        agent.begin_direct_execution(SESSION_ID),
    )
    .await
    .expect("an exact retained owner must reject immediately, not wait indefinitely");
    let refused = next.is_err();
    if let Ok(lease) = next {
        // Only release a new lease actually minted by a broken implementation.
        // Never release or manually finalize the unknown original execution.
        lease.abandon().await;
    }
    assert!(
        refused,
        "a genuine User is not evidence that detached work stopped"
    );
    assert_eq!(
        bamboo_domain::SessionActivationPort::request_activation(
            router.as_ref(),
            SESSION_ID,
            LATER_GENERATION,
        )
        .await
        .unwrap(),
        bamboo_domain::SessionActivationDisposition::ActivationCoalesced
    );
    assert_eq!(
        router.current_run_id(SESSION_ID).await.as_deref(),
        Some(exact_owner.as_str())
    );
    assert_eq!(spawner.reservations.load(Ordering::SeqCst), 0);
    assert_eq!(launches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn self_control_public_direct_unknown_running_retains_exact_owner_and_blocks_genuine_user_and_wakes(
) {
    for sequence in [
        RunningSequence::SameBatchAsStart,
        RunningSequence::AfterStart,
    ] {
        assert_public_direct_retains_unknown_running(sequence).await;
    }
}
