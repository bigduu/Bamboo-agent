use super::*;
use std::sync::atomic::AtomicBool;

struct AbortAfterPersistedStop(Arc<dyn Storage>);

#[async_trait]
impl AgentHook for AbortAfterPersistedStop {
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::AfterRound
    }
    async fn run(&self, _: AgentHookPoint, _: &HookPayload, session: &Session) -> HookResult {
        assert_eq!(
            read_loop_state(session).unwrap().unwrap().status,
            LoopStatus::Stopped
        );
        let durable = self.0.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            read_loop_state(&durable).unwrap().unwrap().status,
            LoopStatus::Stopped
        );
        HookResult::Abort {
            reason: "synthetic stopped post-round failure".into(),
        }
    }
}

#[tokio::test]
async fn counted_stopped_after_round_failure_persists_both_failure_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let mut session = Session::new("counted-fixture", "fake-only");
    let mut cfg = config(3, 3);
    let stop = cfg.counted_loop.clone().unwrap();
    let storage = add_persistence(dir.path(), &mut cfg, &session).await;
    let persistence = cfg.persistence.clone().unwrap();
    let mut hooks = crate::runtime::hooks::HookRunner::new();
    hooks.register(Arc::new(AbortAfterPersistedStop(storage.clone())));
    cfg.hook_runner = Arc::new(hooks);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let provider = Arc::new(FakeProvider {
        durable: Some(storage.clone()),
        barrier: Some((entered.clone(), release.clone())),
        ..Default::default()
    });
    let running_provider = provider.clone();
    let task = tokio::spawn(async move {
        let (result, events) = run(
            &mut session,
            cfg,
            running_provider,
            CancellationToken::new(),
        )
        .await;
        (session, result, events)
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    stop.stop();
    release.notify_one();
    let (mut session, result, events) =
        tokio::time::timeout(std::time::Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("synthetic stopped post-round failure"));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(complete_count(&events), 0);
    for snapshot in [
        &session,
        &storage.load_session(&session.id).await.unwrap().unwrap(),
    ] {
        let state = read_loop_state(snapshot).unwrap().unwrap();
        assert_eq!(state.status, LoopStatus::Failed);
        assert_eq!(
            (
                state.iterations_started,
                state.iterations_completed,
                state.rounds_started
            ),
            (1, 1, 1)
        );
        assert_eq!(
            snapshot.agent_runtime_state.as_ref().unwrap().status,
            bamboo_domain::AgentStatusState::Failed
        );
    }
    persistence
        .checkpoint_runtime_session(&mut session)
        .await
        .unwrap();
    let durable = storage.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(
        read_loop_state(&durable).unwrap(),
        read_loop_state(&session).unwrap()
    );
    assert_eq!(durable.agent_runtime_state, session.agent_runtime_state);
}

struct FailBoundedCheckpoint {
    inner: Arc<dyn RuntimeSessionPersistence>,
    boundary: LoopStatus,
    final_mirror: bool,
    commit_before_error: bool,
    fired: AtomicBool,
}

#[async_trait]
impl RuntimeSessionPersistence for FailBoundedCheckpoint {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }
    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }
    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        if read_loop_state(session)
            .unwrap()
            .is_some_and(|s| s.status == self.boundary)
            && (!self.final_mirror
                || session
                    .agent_runtime_state
                    .as_ref()
                    .is_some_and(|r| r.status == bamboo_domain::AgentStatusState::Completed))
            && !self.fired.swap(true, Ordering::SeqCst)
        {
            if self.commit_before_error {
                self.inner.checkpoint_runtime_session(session).await?;
            }
            return Err(std::io::Error::other(
                "synthetic bounded checkpoint failure",
            ));
        }
        self.inner.checkpoint_runtime_session(session).await
    }
}

#[tokio::test]
async fn counted_bounded_checkpoint_failure_matrix_never_publishes_success() {
    for (boundary, final_mirror) in [
        (LoopStatus::Stopped, false),
        (LoopStatus::Stopped, true),
        (LoopStatus::BudgetExhausted, false),
        (LoopStatus::BudgetExhausted, true),
        (LoopStatus::Completed, true),
    ] {
        for commit_before_error in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut session = Session::new("counted-fixture", "fake-only");
            let mut cfg = if boundary == LoopStatus::Stopped {
                config(2, 4)
            } else {
                config(1, 1)
            };
            if boundary == LoopStatus::Stopped {
                let mut hooks = crate::runtime::hooks::HookRunner::new();
                hooks.register(Arc::new(StopAfterRound(cfg.counted_loop.clone().unwrap())));
                cfg.hook_runner = Arc::new(hooks);
            }
            let storage = add_persistence(dir.path(), &mut cfg, &session).await;
            let persistence = Arc::new(FailBoundedCheckpoint {
                inner: cfg.persistence.take().unwrap(),
                boundary,
                final_mirror,
                commit_before_error,
                fired: AtomicBool::new(false),
            });
            cfg.persistence = Some(persistence.clone());
            let provider = Arc::new(FakeProvider {
                durable: Some(storage.clone()),
                tools_forever: boundary == LoopStatus::BudgetExhausted,
                ..Default::default()
            });
            let (result, events) = run(
                &mut session,
                cfg,
                provider.clone(),
                CancellationToken::new(),
            )
            .await;
            assert!(
                persistence.fired.load(Ordering::SeqCst),
                "{boundary:?}/final={final_mirror}/commit={commit_before_error}"
            );
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Session checkpoint failed; execution not admitted"));
            assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
            assert_eq!(complete_count(&events), 0);
            let state = read_loop_state(&session).unwrap().unwrap();
            assert_eq!(state.status, LoopStatus::Failed);
            assert_eq!(
                (
                    state.iterations_started,
                    state.iterations_completed,
                    state.rounds_started
                ),
                (1, u32::from(boundary != LoopStatus::BudgetExhausted), 1)
            );
            assert_eq!(
                session.agent_runtime_state.as_ref().unwrap().status,
                bamboo_domain::AgentStatusState::Failed
            );
            persistence
                .checkpoint_runtime_session(&mut session)
                .await
                .unwrap();
            let durable = storage.load_session(&session.id).await.unwrap().unwrap();
            assert_eq!(read_loop_state(&durable).unwrap(), Some(state));
            assert_eq!(durable.agent_runtime_state, session.agent_runtime_state);
        }
    }
}

#[tokio::test]
async fn counted_finish_outcome_matrix_preserves_counters_and_generation_fence() {
    // Current state, successful result, failed result, cancellation result.
    let cases = [
        (
            LoopStatus::Running,
            LoopStatus::Failed,
            LoopStatus::Failed,
            LoopStatus::Cancelled,
        ),
        (
            LoopStatus::Finalizing,
            LoopStatus::Completed,
            LoopStatus::Failed,
            LoopStatus::Cancelled,
        ),
        (
            LoopStatus::Completed,
            LoopStatus::Completed,
            LoopStatus::Failed,
            LoopStatus::Cancelled,
        ),
        (
            LoopStatus::Stopped,
            LoopStatus::Stopped,
            LoopStatus::Failed,
            LoopStatus::Cancelled,
        ),
        (
            LoopStatus::BudgetExhausted,
            LoopStatus::BudgetExhausted,
            LoopStatus::Failed,
            LoopStatus::Cancelled,
        ),
        (
            LoopStatus::Cancelled,
            LoopStatus::Cancelled,
            LoopStatus::Cancelled,
            LoopStatus::Cancelled,
        ),
        (
            LoopStatus::Failed,
            LoopStatus::Failed,
            LoopStatus::Failed,
            LoopStatus::Cancelled,
        ),
        (
            LoopStatus::RecoveryUnsupported,
            LoopStatus::RecoveryUnsupported,
            LoopStatus::RecoveryUnsupported,
            LoopStatus::Cancelled,
        ),
    ];
    for (status, ok, failed, cancelled) in cases {
        for (outcome, expected) in [("ok", ok), ("failed", failed), ("cancelled", cancelled)] {
            let dir = tempfile::tempdir().unwrap();
            let mut session = Session::new("counted-fixture", "fake-only");
            let mut state = interrupted_state();
            state.status = status;
            if matches!(status, LoopStatus::Finalizing | LoopStatus::Completed) {
                state.iterations_started = 3;
                state.iterations_completed = 3;
                state.rounds_started = 3;
            }
            session.metadata.insert(
                LOOP_STATE_METADATA_KEY.into(),
                serde_json::to_string(&state).unwrap(),
            );
            let mut runtime = bamboo_domain::AgentRuntimeState::new(&session.id);
            runtime.execution_id = Some(state.owner_execution_id.clone());
            runtime.status = bamboo_domain::AgentStatusState::Suspended;
            session.agent_runtime_state = Some(runtime.clone());
            let mut cfg = config(3, 6);
            let storage = add_persistence(dir.path(), &mut cfg, &session).await;
            let result = match outcome {
                "ok" => Ok(()),
                "failed" => Err(bamboo_agent_core::AgentError::LLM(
                    "synthetic finalizer failure".into(),
                )),
                _ => Err(bamboo_agent_core::AgentError::Cancelled),
            };
            // A live successful suspended execution resolves to the existing
            // recovery fence. Non-live statuses retain their prior semantics.
            let expected = if outcome == "ok"
                && matches!(status, LoopStatus::Running | LoopStatus::Finalizing)
            {
                LoopStatus::RecoveryUnsupported
            } else {
                expected
            };
            let before = serde_json::to_value(&session).unwrap();
            let wrong = crate::runtime::counted_loop::finish(
                &mut session,
                &cfg,
                Some("foreign-generation"),
                &result,
            )
            .await
            .unwrap_err();
            assert!(wrong.to_string().contains("generation changed"));
            assert_eq!(serde_json::to_value(&session).unwrap(), before);
            crate::runtime::counted_loop::finish(
                &mut session,
                &cfg,
                Some(&state.generation),
                &result,
            )
            .await
            .unwrap();
            let final_state = read_loop_state(&session).unwrap().unwrap();
            assert_eq!(final_state.status, expected, "{status:?}/{outcome}");
            assert_eq!(
                (
                    final_state.iterations_started,
                    final_state.iterations_completed,
                    final_state.rounds_started
                ),
                (
                    state.iterations_started,
                    state.iterations_completed,
                    state.rounds_started
                )
            );
            assert!(
                final_state.revision > state.revision,
                "final snapshot is checkpointed even for unchanged bounded/deny status"
            );
            assert_eq!(final_state.generation, state.generation);
            assert_eq!(session.agent_runtime_state, Some(runtime));
            let durable = storage.load_session(&session.id).await.unwrap().unwrap();
            assert_eq!(read_loop_state(&durable).unwrap(), Some(final_state));
            assert_eq!(durable.agent_runtime_state, session.agent_runtime_state);
        }
    }
}
