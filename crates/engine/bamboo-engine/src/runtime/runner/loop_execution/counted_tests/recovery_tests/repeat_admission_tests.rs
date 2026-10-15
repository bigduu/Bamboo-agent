use super::*;
use std::sync::atomic::AtomicBool;

struct RepeatCheckpointFailure {
    inner: Arc<dyn RuntimeSessionPersistence>,
    commit_before_error: bool,
    repeat_failed: AtomicBool,
    fail_terminal: AtomicBool,
}

#[async_trait]
impl RuntimeSessionPersistence for RepeatCheckpointFailure {
    async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        self.inner.save_runtime_session(session).await
    }

    async fn load_runtime_session(&self, id: &str) -> std::io::Result<Option<Session>> {
        self.inner.load_runtime_session(id).await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
        if let Some(state) = read_loop_state(session).unwrap() {
            if state.status == LoopStatus::Running
                && state.iterations_started == 3
                && state.iterations_completed == 2
                && !self.repeat_failed.swap(true, Ordering::SeqCst)
            {
                if self.commit_before_error {
                    self.inner.checkpoint_runtime_session(session).await?;
                }
                return Err(std::io::Error::other("synthetic repeat checkpoint failure"));
            }
            if state.status == LoopStatus::Failed && self.fail_terminal.load(Ordering::SeqCst) {
                return Err(std::io::Error::other(
                    "synthetic terminal checkpoint failure",
                ));
            }
        }
        self.inner.checkpoint_runtime_session(session).await
    }
}

fn repeat(state: &LoopState, iteration: u32) -> Message {
    let mut message = Message::user(&state.objective);
    message.metadata = Some(serde_json::json!({
        "runtime_kind":"counted_loop_iteration", "loop_generation":state.generation,
        "objective_revision":state.objective_revision, "iteration":iteration
    }));
    message
}

fn pending_repeat(status: LoopStatus) -> Session {
    let mut session = interrupted();
    let mut state = read_loop_state(&session).unwrap().unwrap();
    state.status = status;
    state.iterations_started = 3;
    state.iterations_completed = 2;
    state.rounds_started = 3;
    session.add_message(Message::assistant("first answer", None));
    session.add_message(repeat(&state, 2));
    session.add_message(Message::assistant("second answer", None));
    session.add_message(repeat(&state, 3));
    session.metadata.insert(
        LOOP_STATE_METADATA_KEY.into(),
        serde_json::to_string(&state).unwrap(),
    );
    session.agent_runtime_state.as_mut().unwrap().status = bamboo_domain::AgentStatusState::Failed;
    session
}

#[tokio::test]
async fn counted_repeat_checkpoint_failure_stages_input_and_fences_ambiguous_writes() {
    for (commit_before_error, fail_terminal) in [(false, false), (true, false), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(FakeProvider::default());
        let (agent, storage) = fixture(dir.path(), provider.clone()).await;
        let mut session = Session::new("counted-fixture", "fake-only");
        storage.save_session(&session).await.unwrap();
        let persistence = Arc::new(RepeatCheckpointFailure {
            inner: Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone())),
            commit_before_error,
            repeat_failed: AtomicBool::new(false),
            fail_terminal: AtomicBool::new(fail_terminal),
        });
        let mut cfg = config(3, 3);
        cfg.storage = Some(storage.clone());
        cfg.persistence = Some(persistence.clone());
        let (result, events) = run(
            &mut session,
            cfg,
            provider.clone(),
            CancellationToken::new(),
        )
        .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Session checkpoint failed; execution not admitted"));
        assert!(persistence.repeat_failed.load(Ordering::SeqCst));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(complete_count(&events), 0);
        let state = read_loop_state(&session).unwrap().unwrap();
        assert_eq!(state.status, LoopStatus::Failed);
        assert_eq!(
            (
                state.iterations_started,
                state.iterations_completed,
                state.rounds_started
            ),
            (3, 2, 3)
        );
        assert_eq!(
            session.agent_runtime_state.as_ref().unwrap().status,
            bamboo_domain::AgentStatusState::Failed
        );
        assert_eq!(
            session
                .messages
                .iter()
                .filter(|message| message.role == Role::User)
                .count(),
            if commit_before_error && !fail_terminal {
                3
            } else {
                2
            },
            "uncommitted repeat is absent; a durable fact can be adopted by successful finish"
        );

        // Include the ordinary host's subsequent error save, even when all
        // terminal saves fail and only disk knows about the admitted repeat.
        assert_eq!(
            persistence
                .checkpoint_runtime_session(&mut session)
                .await
                .is_err(),
            fail_terminal
        );
        let durable = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(
            durable
                .messages
                .iter()
                .filter(|message| message.role == Role::User)
                .count(),
            if commit_before_error { 3 } else { 2 }
        );
        if commit_before_error {
            for mut rejected in [session.clone(), durable.clone()] {
                let before = serde_json::to_value(&rejected).unwrap();
                let disk_before =
                    serde_json::to_value(storage.load_session(&session.id).await.unwrap()).unwrap();
                let (tx, mut rx) = mpsc::channel(512);
                let error = agent
                    .execute(
                        &mut rejected,
                        ExecuteRequestBuilder::new(PROMPT, tx, CancellationToken::new()).build(),
                    )
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("recovery unsupported"));
                assert!(rx.try_recv().is_err());
                assert_eq!(serde_json::to_value(&rejected).unwrap(), before);
                assert_eq!(
                    serde_json::to_value(storage.load_session(&session.id).await.unwrap()).unwrap(),
                    disk_before
                );
            }
            assert_eq!(
                provider.calls.load(Ordering::SeqCst),
                2,
                "raw public retries never dispatch"
            );
        } else {
            // The completed iteration-2 User is history, not the absent
            // iteration-3 token. A normal durable Failed result is inspectable.
            crate::runtime::counted_loop::reject_recovery(&session).unwrap();
            let lease = agent.begin_direct_execution(&session.id).await.unwrap();
            agent
                .check_direct_execution_admission(&mut session, &lease)
                .await
                .unwrap();
        }
        persistence.fail_terminal.store(false, Ordering::SeqCst);
        persistence
            .checkpoint_runtime_session(&mut session)
            .await
            .unwrap();
        let durable = storage.load_session(&session.id).await.unwrap().unwrap();
        assert_eq!(read_loop_state(&durable).unwrap(), Some(state));
        assert_eq!(
            durable.agent_runtime_state.as_ref().unwrap().status,
            bamboo_domain::AgentStatusState::Failed
        );
    }
}

#[tokio::test]
async fn counted_pending_repeat_requires_canonical_external_input() {
    for noise in ["none", "resume", "counted", "unsaved-user"] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(FakeProvider::default());
        let (agent, storage) = fixture(dir.path(), provider.clone()).await;
        let mut canonical = pending_repeat(LoopStatus::Failed);
        if noise == "resume" {
            let mut resume = Message::user("wake after child completion");
            resume.metadata = Some(serde_json::json!({"runtime_kind":"child_completion_resume"}));
            canonical.add_message(resume);
        } else if noise == "counted" {
            canonical.add_message(repeat(&read_loop_state(&canonical).unwrap().unwrap(), 4));
        }
        storage.save_session(&canonical).await.unwrap();
        let mut rejected = canonical.clone();
        if noise == "unsaved-user" {
            rejected.add_message(Message::user("real input not yet committed"));
        }
        let before = serde_json::to_value(&rejected).unwrap();
        let lease = agent.begin_direct_execution(&canonical.id).await.unwrap();
        let error = agent
            .check_direct_execution_admission(&mut rejected, &lease)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("recovery unsupported"),
            "{noise}"
        );
        assert_eq!(serde_json::to_value(&rejected).unwrap(), before);
        assert_eq!(
            serde_json::to_value(storage.load_session(&canonical.id).await.unwrap().unwrap())
                .unwrap(),
            serde_json::to_value(&canonical).unwrap()
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);

        // Persisting a genuine new external User is distinct from a typed
        // submission's preappend guard, which conveys no recovery capability.
        canonical.add_message(Message::user(
            "new explicit request after the failed repeat",
        ));
        storage.save_session(&canonical).await.unwrap();
        let mut reloaded = storage.load_session(&canonical.id).await.unwrap().unwrap();
        agent
            .check_direct_execution_admission(&mut reloaded, &lease)
            .await
            .unwrap();
        assert_eq!(
            read_loop_state(&reloaded).unwrap(),
            read_loop_state(&canonical).unwrap()
        );
    }
}

#[test]
fn counted_pending_repeat_fence_matches_exact_unfinished_token() {
    for status in [
        LoopStatus::Failed,
        LoopStatus::Cancelled,
        LoopStatus::Stopped,
        LoopStatus::BudgetExhausted,
    ] {
        for token in ["exact", "prior", "generation", "objective-revision"] {
            let mut session = pending_repeat(status);
            let message = session.messages.last_mut().unwrap();
            let metadata = message.metadata.as_mut().unwrap();
            match token {
                "prior" => metadata["iteration"] = serde_json::json!(2),
                "generation" => {
                    metadata["loop_generation"] =
                        serde_json::json!(uuid::Uuid::new_v4().to_string())
                }
                "objective-revision" => metadata["objective_revision"] = serde_json::json!(2),
                _ => {}
            }
            assert_eq!(
                crate::runtime::counted_loop::reject_recovery(&session).is_err(),
                token == "exact",
                "{status:?}/{token}"
            );
        }
    }
}

struct RecoverySnapshot {
    snapshot: Option<Session>,
    io_failure: bool,
}

#[async_trait]
impl RuntimeSessionPersistence for RecoverySnapshot {
    async fn save_runtime_session(&self, _: &mut Session) -> std::io::Result<()> {
        panic!("read-only recovery inspection must not save")
    }

    async fn load_runtime_session(&self, _: &str) -> std::io::Result<Option<Session>> {
        if self.io_failure {
            Err(std::io::Error::other("synthetic recovery read failure"))
        } else {
            Ok(self.snapshot.clone())
        }
    }
}

#[tokio::test]
async fn counted_raw_unfinished_terminal_read_fails_closed_without_writing() {
    let mut session = pending_repeat(LoopStatus::Failed);
    session.messages.pop(); // The iteration-3 User was never committed here.
    for mode in [
        "same",
        "missing",
        "io",
        "revision",
        "birth",
        "missing-state",
    ] {
        let mut durable = session.clone();
        match mode {
            "revision" => {
                let mut state = read_loop_state(&durable).unwrap().unwrap();
                state.revision += 1;
                durable.metadata.insert(
                    LOOP_STATE_METADATA_KEY.into(),
                    serde_json::to_string(&state).unwrap(),
                );
            }
            "birth" => durable.created_at -= chrono::Duration::seconds(1),
            "missing-state" => {
                durable.metadata.remove(LOOP_STATE_METADATA_KEY);
            }
            _ => {}
        }
        let persistence = RecoverySnapshot {
            snapshot: (mode != "missing").then_some(durable),
            io_failure: mode == "io",
        };
        let before = serde_json::to_value(&session).unwrap();
        assert_eq!(
            crate::runtime::counted_loop::reject_ambiguous_reentry(&session, &persistence)
                .await
                .is_ok(),
            mode == "same",
            "{mode}"
        );
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
    }
}

#[tokio::test]
async fn counted_admission_bypasses_stale_repository_cache_without_publishing() {
    for cached_state in ["pending", "missing", "unfinished-no-repeat"] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(FakeProvider::default());
        let cache: crate::SessionCache = Default::default();
        let (agent, storage) =
            fixture_with_cache(dir.path(), provider.clone(), cache.clone()).await;
        let mut canonical = pending_repeat(LoopStatus::Failed);
        let mut old = canonical.clone();
        match cached_state {
            "pending" => canonical.add_message(Message::user("persisted real new request")),
            "missing" => {
                old.metadata.remove(LOOP_STATE_METADATA_KEY);
                old.messages.pop();
            }
            "unfinished-no-repeat" => {
                old.messages.pop();
            }
            _ => unreachable!(),
        }
        cache.insert(
            old.id.clone(),
            Arc::new(crate::SessionSnapshot::new(old.clone())),
        );
        storage.save_session(&canonical).await.unwrap();
        let repository = crate::SessionRepository::new(
            cache.clone(),
            storage.clone(),
            Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone())),
        );
        // Demonstrate the actual runtime loader's stale hit; this is why the
        // execution admission needs its own durable, non-publishing read.
        assert_eq!(
            serde_json::to_value(repository.load_runtime_session(&old.id).await.unwrap()).unwrap(),
            serde_json::to_value(Some(&old)).unwrap()
        );
        let mut presented = if cached_state == "pending" {
            canonical.clone()
        } else {
            old.clone()
        };
        let before = serde_json::to_value(&presented).unwrap();
        let lease = agent.begin_direct_execution(&canonical.id).await.unwrap();
        assert_eq!(
            agent
                .check_direct_execution_admission(&mut presented, &lease)
                .await
                .is_ok(),
            cached_state == "pending",
            "{cached_state}"
        );
        assert_eq!(serde_json::to_value(&presented).unwrap(), before);
        if cached_state != "missing" {
            assert_eq!(
                crate::runtime::counted_loop::reject_ambiguous_reentry(&presented, &repository)
                    .await
                    .is_ok(),
                cached_state == "pending"
            );
        }
        assert_eq!(
            serde_json::to_value(storage.load_session(&canonical.id).await.unwrap()).unwrap(),
            serde_json::to_value(Some(&canonical)).unwrap()
        );
        assert_eq!(
            serde_json::to_value(repository.load_runtime_session(&old.id).await.unwrap()).unwrap(),
            serde_json::to_value(Some(&old)).unwrap(),
            "admission must not publish to the cache"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    }
}
