//! Old-API behavioral RED: a real direct registration owns every test run.
//! Attach as child of scoped_supervisor_runner_tests; no fake LoopState/writer.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Exit {
    NoStartComplete,
    NoStartBudget,
    LocalStop,
    Complete,
    Budget,
    HostStop,
    Cancel,
    ProviderError,
    Drop,
}

struct ExitProvider {
    exit: Exit,
    storage: Arc<dyn Storage>,
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
}

impl ExitProvider {
    fn gate_round(&self) -> usize {
        match self.exit {
            Exit::NoStartComplete | Exit::Drop => 1,
            Exit::Complete | Exit::Budget => 3,
            _ => 2,
        }
    }
    fn local_start(&self) -> bool {
        !matches!(
            self.exit,
            Exit::NoStartComplete | Exit::NoStartBudget | Exit::Drop
        )
    }
}

#[async_trait]
impl LLMProvider for ExitProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        let round = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let active_stream = ActiveStream(self.active.clone());
        if round == self.gate_round() {
            self.entered.notify_one();
            self.release.notified().await;
        }
        if self.exit == Exit::ProviderError && round >= self.gate_round() {
            return Err(bamboo_llm::provider::LLMError::Stream(
                "deterministic provider failure".into(),
            ));
        }
        let response = if round == 1 && self.local_start() {
            control_call(
                "local-retirement-start",
                json!({
                    "action":"start", "operation_id":"ad8160b1-5f2f-482f-a146-ae1ec4bf6155",
                    "expected_generation":null, "expected_revision":0,
                    "objective":"Local task whose origin is not the Supervisor.",
                    "max_iterations":2, "max_total_rounds":if self.exit == Exit::Budget { 3 } else { 6 },
                }),
            )
        } else if self.exit == Exit::LocalStop && round == 2 {
            let durable = self
                .storage
                .load_execution_admission_snapshot(TARGET_ID)
                .await
                .unwrap()
                .unwrap();
            let state = read_loop_state(&durable)
                .unwrap()
                .expect("real local Start succeeded");
            assert!(state.control.as_ref().unwrap().objective_origin.is_none());
            control_call(
                "local-retirement-stop",
                json!({
                    "action":"stop", "operation_id":"bbd1434d-ae80-4438-9c5d-f53b59582e94",
                    "generation":state.generation,
                    "expected_revision":state.control.unwrap().control_revision,
                }),
            )
        } else if matches!(self.exit, Exit::Budget | Exit::NoStartBudget) {
            control_call(
                &format!("retirement-budget-{round}"),
                json!({"action":"status"}),
            )
        } else {
            LLMChunk::Token("Actual bounded provider answer.".into())
        };
        use futures::StreamExt;
        Ok(Box::pin(
            futures::stream::iter([Ok(response), Ok(LLMChunk::Done)]).map(move |chunk| {
                let _keep_alive = &active_stream;
                chunk
            }),
        ))
    }
}

async fn run_exit(exit: Exit, native: bool) {
    let mut f = ScopedPublicFixture::new_with_root_actor_directory(native).await;
    let provider = Arc::new(ExitProvider {
        exit,
        storage: f.storage.clone(),
        entered: Notify::new(),
        release: Notify::new(),
        calls: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
        peak: AtomicUsize::new(0),
    });
    let (tx, mut rx) = mpsc::channel(1024);
    let mut request = f.request(tx);
    request.provider_override = Some(provider.clone());
    if matches!(exit, Exit::Budget | Exit::NoStartBudget) {
        request.run_budget.as_mut().unwrap().max_rounds =
            Some(if exit == Exit::Budget { 3 } else { 2 });
    }
    let cancel = request.cancel_token.clone();
    let policy = f.policy.clone();
    let lease = f.agent.begin_direct_execution(TARGET_ID).await.unwrap();
    let run_id = f.router.current_run_id(TARGET_ID).await.unwrap();
    let mut root = if native {
        f.repository
            .bind_root_actor_execution(&f.session, &run_id)
            .await
            .unwrap()
    } else {
        None
    };
    assert_eq!(root.is_some(), native);
    let agent = match root.as_ref() {
        Some(binding) => Arc::new(
            f.agent
                .with_execution_persistence(binding.persistence.clone()),
        ),
        None => f.agent.clone(),
    };
    let storage = f.storage.clone();
    let inbox = f.inbox.clone();
    let messenger = f.messenger.clone();
    let supervisor = f.supervisor.clone();
    let revision = f.link_revision;
    let mut events = Vec::new();
    let (result, queued, command) = {
        let run = agent.execute_direct_registered(&mut f.session, request, lease);
        tokio::pin!(run);
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                tokio::select! {
                    _ = provider.entered.notified() => break,
                    event = rx.recv() => if let Some(event) = event { events.push(event); },
                    result = &mut run => panic!("{exit:?}/{native}: actual run ended before controlled provider gate: {result:?}"),
                }
            }
        }).await.expect("real provider reached terminal gate");
        let durable = storage
            .load_execution_admission_snapshot(TARGET_ID)
            .await
            .unwrap()
            .unwrap();
        let command = match read_loop_state(&durable).unwrap() {
            Some(state) => LoopControlRequest::Update {
                operation_id: uuid::Uuid::new_v4().to_string(),
                generation: state.generation,
                expected_revision: state.control.unwrap().control_revision,
                objective: Some(
                    "Queued remote objective must never be admitted after this exit.".into(),
                ),
                max_iterations: None,
                max_total_rounds: None,
            },
            None => LoopControlRequest::Start {
                operation_id: uuid::Uuid::new_v4().to_string(),
                expected_generation: None,
                expected_revision: 0,
                objective: "Queued remote Start must not survive target termination.".into(),
                max_iterations: 2,
                max_total_rounds: 6,
            },
        };
        let queued = messenger
            .supervisor_loop_control(&supervisor, TARGET_ID, Some(revision), &command, None)
            .await
            .unwrap();
        assert_eq!(queued["admitted"], true);
        assert_eq!(queued["configured"], false);
        match exit {
            Exit::Cancel => cancel.cancel(),
            Exit::HostStop => policy.stop(),
            _ => {}
        }
        provider.release.notify_one();
        let result = tokio::time::timeout(std::time::Duration::from_secs(20), &mut run)
            .await
            .expect("bounded terminal execution");
        (result, queued, command)
    };
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    match exit {
        Exit::Cancel => assert!(
            result.as_ref().is_err_and(|error| error.is_cancelled()),
            "{result:?}"
        ),
        Exit::ProviderError => assert!(result.is_err(), "provider error must remain terminal"),
        _ => assert!(result.is_ok(), "{exit:?}/{native}: {result:?}"),
    }
    let id =
        bamboo_domain::SessionMessageId::parse(queued["receipt_id"].as_str().unwrap()).unwrap();
    let durable = storage
        .load_execution_admission_snapshot(TARGET_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(!durable
        .session_inbox_admission()
        .is_some_and(|cursor| cursor.contains(&id)));
    assert!(!scoped_loop_consumptions(&durable)
        .unwrap()
        .iter()
        .any(|record| record.input_id == id));
    assert!(!inbox.was_admitted(TARGET_ID, &id).await.unwrap());
    let backlog = inbox.inspect(TARGET_ID).await.unwrap();
    assert_eq!(
        (backlog.pending, backlog.claimed),
        (0, 0),
        "{exit:?}/{native}: a terminal owner must retire its exact-run tail"
    );
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::Complete { .. }))
            .count(),
        usize::from(result.is_ok())
    );
    assert!(f.router.current_run_id(TARGET_ID).await.is_none());
    assert!(
        messenger
            .supervisor_loop_control(&supervisor, TARGET_ID, Some(revision), &command, None)
            .await
            .is_err(),
        "retirement must not revive live capability or retry authority"
    );
    inbox
        .deliver(&bamboo_domain::SessionMessageEnvelope::user_input(
            TARGET_ID,
            "Independent explicit genuine User",
        ))
        .await
        .unwrap();
    f.assert_no_successor();
    if let Some(binding) = root.as_mut() {
        binding
            .directory
            .finish_activation(
                &binding.owner.fence,
                Utc::now(),
                bamboo_domain::ActorActivationFinish::Cancelled,
            )
            .await
            .unwrap();
        binding.disarm_abandonment();
    }
}

#[tokio::test]
async fn scoped_target_no_start_complete_and_budget_retire_exact_run_controls() {
    for native in [false, true] {
        for exit in [Exit::NoStartComplete, Exit::NoStartBudget] {
            run_exit(exit, native).await;
        }
    }
}

#[tokio::test]
async fn scoped_target_local_stop_complete_budget_cancel_and_error_retire_exact_run_controls() {
    for native in [false, true] {
        for exit in [
            Exit::LocalStop,
            Exit::Complete,
            Exit::Budget,
            Exit::HostStop,
            Exit::Cancel,
            Exit::ProviderError,
        ] {
            run_exit(exit, native).await;
        }
    }
}

#[tokio::test]
async fn scoped_target_future_drop_retires_before_abandoned_owner_releases() {
    for native in [false, true] {
        run_drop(native, false).await;
    }
}

#[tokio::test]
async fn scoped_target_native_terminal_before_drop_retires_without_live_root_writer() {
    run_drop(true, true).await;
}

async fn run_drop(native: bool, terminal_first: bool) {
    let f = ScopedPublicFixture::new_with_root_actor_directory(native).await;
    let provider = Arc::new(ExitProvider {
        exit: Exit::Drop,
        storage: f.storage.clone(),
        entered: Notify::new(),
        release: Notify::new(),
        calls: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
        peak: AtomicUsize::new(0),
    });
    let (tx, mut rx) = mpsc::channel(1024);
    let mut request = f.request(tx);
    request.provider_override = Some(provider.clone());
    let lease = f.agent.begin_direct_execution(TARGET_ID).await.unwrap();
    let run_id = f.router.current_run_id(TARGET_ID).await.unwrap();
    let mut root = if native {
        f.repository
            .bind_root_actor_execution(&f.session, &run_id)
            .await
            .unwrap()
    } else {
        None
    };
    assert_eq!(root.is_some(), native);
    let agent = match root.as_ref() {
        Some(binding) => Arc::new(
            f.agent
                .with_execution_persistence(binding.persistence.clone()),
        ),
        None => f.agent.clone(),
    };
    let mut session = f.session.clone();
    let task = tokio::spawn(async move {
        agent
            .execute_direct_registered(&mut session, request, lease)
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        provider.entered.notified(),
    )
    .await
    .unwrap();
    let command = LoopControlRequest::Start {
        operation_id: uuid::Uuid::new_v4().to_string(),
        expected_generation: None,
        expected_revision: 0,
        objective: "Never execute an abandoned target's command.".into(),
        max_iterations: 2,
        max_total_rounds: 6,
    };
    let queued = f
        .messenger
        .supervisor_loop_control(
            &f.supervisor,
            TARGET_ID,
            Some(f.link_revision),
            &command,
            None,
        )
        .await
        .unwrap();
    let id =
        bamboo_domain::SessionMessageId::parse(queued["receipt_id"].as_str().unwrap()).unwrap();
    if terminal_first {
        let binding = root.as_mut().expect("real native Root binding");
        binding
            .directory
            .finish_activation(
                &binding.owner.fence,
                Utc::now(),
                bamboo_domain::ActorActivationFinish::Cancelled,
            )
            .await
            .unwrap();
        binding.disarm_abandonment();
        assert_eq!(
            binding
                .directory
                .inspect_actor(TARGET_ID)
                .await
                .unwrap()
                .activation
                .unwrap()
                .status,
            bamboo_domain::ActorActivationStatus::Cancelled
        );
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        // Wait for the actual detached Drop cleanup's owner-release fact;
        // a single yield is never treated as cleanup completion evidence.
        while f.router.current_run_id(TARGET_ID).await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual registration Drop cleanup released its owner");
    let backlog = f.inbox.inspect(TARGET_ID).await.unwrap();
    assert_eq!(
        (backlog.pending, backlog.claimed),
        (0, 0),
        "{native}: Drop released owner before retiring tail"
    );
    let durable = f
        .storage
        .load_execution_admission_snapshot(TARGET_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(!durable
        .session_inbox_admission()
        .is_some_and(|cursor| cursor.contains(&id)));
    assert!(!scoped_loop_consumptions(&durable)
        .unwrap()
        .iter()
        .any(|record| record.input_id == id));
    assert!(!f.inbox.was_admitted(TARGET_ID, &id).await.unwrap());
    assert!(read_loop_state(&durable).unwrap().is_none());
    let mut complete = 0;
    while let Ok(event) = rx.try_recv() {
        complete += usize::from(matches!(event, AgentEvent::Complete { .. }));
    }
    assert_eq!(complete, 0);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    f.assert_no_successor();
    if let Some(binding) = root.as_mut().filter(|_| !terminal_first) {
        binding
            .directory
            .finish_activation(
                &binding.owner.fence,
                Utc::now(),
                bamboo_domain::ActorActivationFinish::Cancelled,
            )
            .await
            .unwrap();
        binding.disarm_abandonment();
    }
}

mod cleanup_failure_tests;
