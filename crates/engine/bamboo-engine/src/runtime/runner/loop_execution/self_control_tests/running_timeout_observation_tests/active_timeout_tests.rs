//! Child of the tests-only running_timeout_observation_tests module.
//! Install as self_control_tests/running_timeout_observation_tests/active_timeout_tests.rs
//! and let the sole integrator add `mod active_timeout_tests;` to that parent.
//! Reuses its real Fixture/Builtin pending Tool/control/runner/Probe helpers.
//! No new production API, owner, direct executor or terminal call is used.

use super::*;

struct ActiveTimeoutProvider {
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
}

#[async_trait]
impl LLMProvider for ActiveTimeoutProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        schemas: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        use futures::StreamExt;
        let round = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        assert_eq!(active, 1);
        let guard = ActiveStream(self.active.clone());
        assert!(schemas
            .iter()
            .any(|schema| schema.function.name == PENDING_TOOL));
        assert!(schemas
            .iter()
            .any(|schema| schema.function.name == CONTROL_TOOL));
        let chunk = match round {
            1 => start_call(2, 8),
            2 => LLMChunk::ToolCalls(vec![
                timeout_call(PENDING_CALL, PENDING_TOOL),
                timeout_call(CONTROL_CALL, CONTROL_TOOL),
            ]),
            // 8da still has a finite true-parallel timeout, then wrongly
            // continues. The script allows those calls to reach the intended
            // kill_count RED instead of panicking in the provider first.
            _ => LLMChunk::Token(format!(
                "Forbidden later provider answer after active Running timeout, round {round}."
            )),
        };
        Ok(Box::pin(
            futures::stream::iter([Ok(chunk), Ok(LLMChunk::Done)]).map(move |item| {
                let _keep_stream_admitted = &guard;
                item
            }),
        ))
    }
}

#[tokio::test]
async fn self_control_active_parallel_safe_running_timeout_is_terminal_before_sibling_or_provider()
{
    let mut fixture = Fixture::new(2, 8).await;
    let config = fixture.config.as_mut().unwrap();
    config.per_tool_timeout_secs = 1;
    config.parallel_batch_timeout_secs = 4;
    let control = config.loop_control.as_ref().unwrap().clone();
    let probe = Arc::new(TimeoutProbe::default());
    let tools = bamboo_tools::BuiltinToolExecutor::new();
    tools
        .registry()
        .register(PendingAfterCollapsedRunning(probe.clone()))
        .unwrap();
    tools
        .registry()
        .register(ParallelTimeoutControl(probe.clone()))
        .unwrap();
    let provider = Arc::new(ActiveTimeoutProvider {
        calls: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
        peak: AtomicUsize::new(0),
    });
    // The actual configured per-tool deadline is one second. Five seconds is
    // only the test's outer fail bound, not a substitute for that production
    // deadline. A forced-serial preview lacking timeout fails here finitely.
    let (result, events) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run_timeout_observation_fixture(&mut fixture, provider.clone(), Arc::new(tools)),
    )
    .await
    .expect("active serial dispatch must retain its former ParallelSafe one-second bound");
    assert_eq!(probe.pending_invoked.load(Ordering::SeqCst), 1);
    // Reliable first RED on 8da: its real parallel timeout finishes the run,
    // but the old public collapse does not invoke the supplied kill callback.
    assert_eq!(probe.kills.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.invoke_dropped.load(Ordering::SeqCst),
        1,
        "the configured timeout must cancel the pending dispatch/observer future"
    );
    let collapsed = probe.collapsed_ack.lock().unwrap().clone().unwrap();
    assert!(
        !collapsed.success,
        "an active host scope must reject the nested Running collapse"
    );
    assert!(collapsed.result.contains("stop unconfirmed"));
    assert_ne!(collapsed.result, INNER_ACK);
    assert_eq!(*probe.scope_allowed.lock().unwrap(), vec![false]);
    assert_eq!(
        probe.control_invoked.load(Ordering::SeqCst),
        0,
        "neither the remaining original parallel sibling nor its ToolStart may be admitted"
    );
    assert!(
        result.is_err(),
        "a timed-out disallowed Running is a terminal error: {result:?}"
    );
    let error = result.unwrap_err();
    assert!(!error.is_cancelled());
    assert!(!error.is_hook_suspended());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.peak.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    assert!(!events.iter().any(|event| matches!(event,
        AgentEvent::ToolStart { tool_call_id, .. } if tool_call_id == CONTROL_CALL
    )));
    assert!(!events.iter().any(|event| matches!(event,
        AgentEvent::ToolComplete { tool_call_id, result }
            if tool_call_id == CONTROL_CALL && result.success
    )));
    let failed_pair = timeout_pair(&fixture.session, PENDING_CALL);
    assert_eq!(failed_pair.tool_success, Some(false));
    assert_ne!(failed_pair.content, INNER_ACK);
    assert!(!failed_pair.content.is_empty());
    let failures = events.iter().filter(|event| matches!(event,
        AgentEvent::ToolError { tool_call_id, .. } | AgentEvent::ToolComplete { tool_call_id, .. }
            if tool_call_id == PENDING_CALL
    )).collect::<Vec<_>>();
    assert_eq!(
        failures.len(),
        1,
        "one real false pair/error, never a Running ack"
    );
    if let AgentEvent::ToolComplete { result, .. } = failures[0] {
        assert!(!result.success);
        assert_eq!(result.result, failed_pair.content);
    }
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::Complete { .. }))
            .count(),
        0
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
    let state = read_loop_state(&fixture.session).unwrap().unwrap();
    assert_eq!(state.status, LoopStatus::Failed);
    assert_eq!(
        (
            state.iterations_started,
            state.iterations_completed,
            state.rounds_started
        ),
        (1, 0, 2)
    );
    assert_eq!(control.generation(), Some(state.generation.as_str()));
    assert!(control.active_counted().is_some());
    assert_eq!(
        admitted_objectives(&fixture.session.messages),
        vec![(FIRST_OBJECTIVE, 1)]
    );
    let managed = state.control.as_ref().unwrap();
    assert_eq!(managed.operations.len(), 1);
    let start = timeout_pair(&fixture.session, "start");
    assert_eq!(start.tool_success, Some(true));
    let receipt: LoopControlReceipt = serde_json::from_str(&start.content).unwrap();
    assert_eq!(receipt, managed.operations[0].receipt);
    assert_eq!(start.content, serde_json::to_string(&receipt).unwrap());
    assert!(!fixture
        .session
        .messages
        .iter()
        .any(|message| message.role == Role::Assistant
            && message
                .content
                .starts_with("Forbidden later provider answer")));
    let durable = fixture
        .storage
        .load_session(SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_loop_state(&durable).unwrap().unwrap(), state);
    for session in [&fixture.session, &durable] {
        assert!(session.pending_question.is_none());
        assert!(!session.metadata.contains_key("runtime.suspend_reason"));
        assert!(
            crate::runtime::counted_loop::reject_recovery(session).is_err(),
            "retain the unfinished counted input fence"
        );
        let runtime = session.agent_runtime_state.as_ref().unwrap();
        assert_eq!(runtime.status, bamboo_domain::AgentStatusState::Failed);
        assert_eq!(
            runtime.execution_id.as_deref(),
            Some(state.owner_execution_id.as_str())
        );
        assert!(runtime.suspension.is_none());
        assert!(runtime.waiting_for_children.is_none());
        assert!(runtime.waiting_for_bash.is_none());
    }
    assert_eq!(
        timeout_pair(&durable, PENDING_CALL).tool_success,
        Some(false)
    );
    assert_eq!(
        timeout_pair(&durable, PENDING_CALL).content,
        failed_pair.content
    );
    assert_eq!(timeout_pair(&durable, "start").content, start.content);
    fixture.close().await;
}
