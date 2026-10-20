//! Real native Child publication and owned Inbox boundaries, independent of Worker IPC.
use super::default_actor_context_tests::DefaultWriteHook;
use super::*;
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorDirectoryPort, ChildActorRuntimeSave,
    ChildActorRuntimeWrite, Message, RootActorRuntimeWrite, RuntimeSessionPersistence,
    SessionActivationPolicy, SessionInboxConsumerId, SessionInboxLeaseRequest, SessionInboxLimits,
    SessionInboxOwnedClaim, SessionInboxPort, SessionMessageEnvelope, Storage,
};
use chrono::Duration as LeaseDuration;
use std::sync::Mutex as StdMutex;

const ROOT: &str = "native-child-root";
const CHILD: &str = "native-child";
const WAIT: Duration = Duration::from_secs(10);

struct Release(Arc<DefaultWriteHook>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct Fixture {
    a: Arc<SessionStoreV2>,
    b: Arc<SessionStoreV2>,
    raw: Arc<crate::FileSessionInbox>,
    bound: Arc<dyn SessionInboxPort>,
    base: Session,
    owner: ChildActorRuntimeWrite,
    seen: Arc<StdMutex<Vec<Session>>>,
    _home: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_duration(LeaseDuration::minutes(1)).await
    }
    async fn with_duration(duration: LeaseDuration) -> Self {
        let home = tempfile::tempdir().unwrap();
        let a = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let root = Session::new(ROOT, "model");
        a.save_session(&root).await.unwrap();
        let mut base = Session::new_child_of(CHILD, &root, "model", "native Child");
        base.add_message(Message::user("original Child assignment"));
        a.save_session(&base).await.unwrap();
        let b = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let now = Utc::now();
        let activation = a
            .claim_activation(&ActorActivationClaim {
                actor_id: CHILD.into(),
                run_id: "local-run-a".into(),
                lease_owner: "tokio-owner-a".into(),
                lease_expires_at: now + duration,
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
            .unwrap();
        a.start_activation(&activation.fence(), Utc::now())
            .await
            .unwrap();
        let owner = ChildActorRuntimeWrite::from_session(activation.fence(), &base).unwrap();
        let raw = Arc::new(crate::FileSessionInbox::new(
            a.clone(),
            SessionInboxLimits::default(),
        ));
        let bound = a.bind_child_actor_inbox(&owner, raw.clone()).unwrap();
        Self {
            a,
            b,
            raw,
            bound,
            base,
            owner,
            seen: Arc::default(),
            _home: home,
        }
    }
    fn directory(&self) -> PathBuf {
        self.a.sessions_dir.join(ROOT).join("children").join(CHILD)
    }
    fn publisher(&self) -> bamboo_domain::RootActorRuntimePublisher {
        let seen = self.seen.clone();
        Arc::new(move |saved| {
            seen.lock().unwrap().push(saved.clone());
        })
    }
    async fn save(&self, candidate: &mut Session, runtime_only: bool) -> io::Result<()> {
        self.a
            .save_child_actor_runtime(ChildActorRuntimeSave {
                owner: &self.owner,
                session: candidate,
                runtime_only,
                inherited: None,
                finalized_wait: None,
                input: None,
                publish: self.publisher(),
            })
            .await
    }
    async fn input(&self) -> (SessionMessageEnvelope, SessionInboxOwnedClaim) {
        let input = SessionMessageEnvelope::user_input(CHILD, "new typed Child input");
        self.raw
            .deliver_with_activation_intent(
                &input,
                SessionActivationPolicy::InterruptSpecificWait,
                None,
            )
            .await
            .unwrap();
        let claim = self
            .bound
            .claim_owned(
                CHILD,
                1,
                Some(&self.owner.fence.run_id),
                &SessionInboxLeaseRequest {
                    consumer: SessionInboxConsumerId::new(),
                    now: Utc::now(),
                    duration: LeaseDuration::seconds(5),
                },
            )
            .await
            .unwrap()
            .remove(0);
        (input, claim)
    }
    fn candidate(&self, claim: &SessionInboxOwnedClaim) -> Session {
        let mut session = self.base.clone();
        session.add_message(claim.claim.envelope.to_provider_message().unwrap());
        session
            .session_inbox_admission_mut()
            .record(claim.claim.envelope.id.clone(), claim.claim.generation);
        session
    }
    async fn checkpoint(&self, claim: &SessionInboxOwnedClaim) -> io::Result<()> {
        self.a
            .save_child_actor_runtime(ChildActorRuntimeSave {
                owner: &self.owner,
                session: &mut self.candidate(claim),
                runtime_only: false,
                inherited: None,
                finalized_wait: None,
                input: Some((self.bound.clone(), claim.clone())),
                publish: self.publisher(),
            })
            .await
    }
}

fn held(store: &SessionStoreV2, inbox: Option<&Path>) {
    let mut paths = vec![
        store.bamboo_home_dir.join(SESSION_LIFECYCLE_LOCK_FILE),
        store
            .bamboo_home_dir
            .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        store.session_write_lock_path(CHILD),
    ];
    if let Some(inbox) = inbox {
        paths.push(inbox.join("inbox/.session-inbox.lock"));
    }
    for path in paths {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let result = FileExt::try_lock_exclusive(&file);
        if result.is_ok() {
            FileExt::unlock(&file).unwrap();
        }
        assert_eq!(
            result.unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "physical guard released: {}",
            path.display()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_full_and_runtime_publish_exact_canonical_snapshot_with_held_guards() {
    for runtime_only in [false, true] {
        let f = Fixture::new().await;
        let mut candidate = f.base.clone();
        candidate.conversation_summary = Some(bamboo_domain::ConversationSummary::new(
            "owned summary",
            1,
            1,
        ));
        if !runtime_only {
            candidate.add_message(Message::assistant("native answer", None));
        }
        let store = f.a.clone();
        let seen = f.seen.clone();
        f.a.save_child_actor_runtime(ChildActorRuntimeSave {
            owner: &f.owner,
            session: &mut candidate,
            runtime_only,
            inherited: None,
            finalized_wait: None,
            input: None,
            publish: Arc::new(move |saved| {
                held(&store, None);
                seen.lock().unwrap().push(saved.clone());
            }),
        })
        .await
        .unwrap();
        let durable = f.b.load_session(CHILD).await.unwrap().unwrap();
        let published = f.seen.lock().unwrap()[0].clone();
        assert_eq!(
            serde_json::to_value(&durable).unwrap(),
            serde_json::to_value(&published).unwrap()
        );
        assert_eq!(
            durable.conversation_summary.unwrap().content,
            "owned summary"
        );
        assert_eq!(
            durable.messages.len(),
            f.base.messages.len() + usize::from(!runtime_only)
        );
    }
}

#[tokio::test]
async fn child_default_and_root_writers_remain_unprivileged() {
    let f = Fixture::new().await;
    let mut changed = f.base.clone();
    changed.add_message(Message::assistant("unfenced", None));
    assert!(f.a.save_session(&changed).await.is_err());
    changed.conversation_summary = Some(bamboo_domain::ConversationSummary::new("unfenced", 1, 1));
    assert!(f.a.save_runtime_state(&changed).await.is_err());
    let root_owner = RootActorRuntimeWrite {
        fence: f.owner.fence.clone(),
        created_at: f.base.created_at,
    };
    assert!(f
        .a
        .save_root_actor_runtime(&root_owner, &changed, false, f.publisher())
        .await
        .is_err());
    assert!(f
        .a
        .publish_root_actor_runtime_event(&root_owner, Box::new(|check| check()))
        .await
        .is_err());
    let root_bound =
        f.a.bind_root_actor_inbox(&root_owner, f.raw.clone())
            .unwrap();
    assert!(root_bound
        .claim_owned(
            CHILD,
            1,
            Some("local-run-a"),
            &SessionInboxLeaseRequest {
                consumer: SessionInboxConsumerId::new(),
                now: Utc::now(),
                duration: LeaseDuration::seconds(1),
            }
        )
        .await
        .is_err());
    assert!(f.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn child_capability_rejects_changed_birth_project_parent_root_and_depth() {
    let f = Fixture::new().await;
    let before = std::fs::read(f.directory().join("session.json")).unwrap();
    for change in 0..6 {
        let mut owner = f.owner.clone();
        match change {
            0 => owner.created_at += LeaseDuration::seconds(1),
            1 => owner.parent_created_at += LeaseDuration::seconds(1),
            2 => owner.parent_session_id = "unrelated-parent".into(),
            3 => owner.root_session_id = "unrelated-root".into(),
            4 => owner.spawn_depth += 1,
            _ => owner.project_id = Some("wrong-project".into()),
        }
        let mut candidate = f.base.clone();
        candidate.add_message(Message::assistant("rejected", None));
        assert!(f
            .a
            .save_child_actor_runtime(ChildActorRuntimeSave {
                owner: &owner,
                session: &mut candidate,
                runtime_only: false,
                inherited: None,
                finalized_wait: None,
                input: None,
                publish: f.publisher(),
            })
            .await
            .is_err());
    }
    assert_eq!(
        std::fs::read(f.directory().join("session.json")).unwrap(),
        before
    );
    assert!(f.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn child_owner_rejects_reborn_canonical_parent_without_repair() {
    let f = Fixture::new().await;
    let reborn_at = Utc::now() + LeaseDuration::seconds(1);
    for name in ["session.json", RUNTIME_SIDECAR_FILE] {
        let path = f.a.sessions_dir.join(ROOT).join(name);
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["created_at"] = serde_json::to_value(reborn_at).unwrap();
        std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    }
    let mut candidate = f.base.clone();
    candidate.add_message(Message::assistant("stale parent", None));
    assert!(f.save(&mut candidate, false).await.is_err());
    assert!(f
        .a
        .publish_child_actor_runtime_event(&f.owner, Box::new(|check| check()))
        .await
        .is_err());
    assert!(f.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn child_finished_generation_cannot_write_publish_or_claim_after_successor() {
    let f = Fixture::new().await;
    let locked = crate::LockedSessionStore::new(f.a.clone());
    let bound_writer = locked
        .bind_child_actor_writer(f.owner.clone(), f.a.clone(), f.publisher())
        .unwrap();
    assert!(bound_writer.load_runtime_session(ROOT).await.is_err());
    f.a.finish_activation(&f.owner.fence, Utc::now(), ActorActivationFinish::Cancelled)
        .await
        .unwrap();
    let now = Utc::now();
    let successor =
        f.b.claim_activation(&ActorActivationClaim {
            actor_id: CHILD.into(),
            run_id: "local-run-b".into(),
            lease_owner: "tokio-owner-b".into(),
            lease_expires_at: now + LeaseDuration::minutes(1),
            inbox_generation: 0,
            placement_ref: None,
            now,
        })
        .await
        .unwrap();
    f.b.start_activation(&successor.fence(), Utc::now())
        .await
        .unwrap();
    assert!(bound_writer.load_runtime_session(CHILD).await.is_err());
    let mut candidate = f.base.clone();
    candidate.add_message(Message::assistant("obsolete", None));
    assert!(f.save(&mut candidate, false).await.is_err());
    assert!(f
        .a
        .publish_child_actor_runtime_event(&f.owner, Box::new(|check| check()))
        .await
        .is_err());
    assert!(f
        .bound
        .claim_owned(
            CHILD,
            1,
            Some("local-run-a"),
            &SessionInboxLeaseRequest {
                consumer: SessionInboxConsumerId::new(),
                now: Utc::now(),
                duration: LeaseDuration::seconds(1),
            }
        )
        .await
        .is_err());
    assert_eq!(
        f.b.inspect_actor(CHILD)
            .await
            .unwrap()
            .activation
            .unwrap()
            .fence(),
        successor.fence()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_owned_input_checkpoint_appends_cursor_acks_and_reads_back_once() {
    let f = Fixture::new().await;
    let (input, claim) = f.input().await;
    f.checkpoint(&claim).await.unwrap();
    assert!(f.raw.was_admitted(CHILD, &input.id).await.unwrap());
    assert!(!f
        .directory()
        .join("inbox/cur")
        .join(&claim.claim.claim_id)
        .exists());
    let durable = f.b.load_session(CHILD).await.unwrap().unwrap();
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|m| bamboo_domain::is_matching_session_message(m, &input))
            .count(),
        1
    );
    assert!(durable
        .session_inbox_admission()
        .unwrap()
        .contains(&input.id));
    assert_eq!(
        durable
            .session_inbox_admission()
            .unwrap()
            .last_admitted_sequence,
        claim.claim.generation
    );
    assert_eq!(
        serde_json::to_value(&f.seen.lock().unwrap()[0]).unwrap(),
        serde_json::to_value(&durable).unwrap()
    );
    let replay = f
        .raw
        .deliver_with_activation_intent(
            &input,
            SessionActivationPolicy::InterruptSpecificWait,
            None,
        )
        .await
        .unwrap();
    assert_eq!(replay.generation, claim.claim.generation);
    assert!(f.bound.release_owned(CHILD, &claim).await.is_err());
}

#[tokio::test]
async fn child_owned_input_stale_lease_and_wrong_run_never_ack() {
    let f = Fixture::new().await;
    let (input, old) = f.input().await;
    assert!(f
        .bound
        .claim_owned(
            CHILD,
            1,
            Some("wrong-run"),
            &SessionInboxLeaseRequest {
                consumer: SessionInboxConsumerId::new(),
                now: Utc::now(),
                duration: LeaseDuration::seconds(1),
            }
        )
        .await
        .is_err());
    let current = f
        .bound
        .renew_owned(
            CHILD,
            &old,
            &SessionInboxLeaseRequest {
                consumer: old.lease.consumer.clone(),
                now: Utc::now(),
                duration: LeaseDuration::seconds(6),
            },
        )
        .await
        .unwrap();
    assert!(f.checkpoint(&old).await.is_err());
    assert!(f.bound.ack_owned(CHILD, &old, Utc::now()).await.is_err());
    assert!(!f.raw.was_admitted(CHILD, &input.id).await.unwrap());
    f.checkpoint(&current).await.unwrap();
    assert!(f.raw.was_admitted(CHILD, &input.id).await.unwrap());
}

#[tokio::test]
async fn bound_locked_child_persistence_consumes_owned_inbox_through_runtime_hook() {
    let f = Fixture::new().await;
    let locked = crate::LockedSessionStore::new(f.a.clone());
    let bound = locked
        .bind_child_actor_writer(f.owner.clone(), f.a.clone(), f.publisher())
        .unwrap();
    let input = SessionMessageEnvelope::user_input(CHILD, "runtime boundary input");
    f.raw
        .deliver_with_activation_intent(
            &input,
            SessionActivationPolicy::InterruptSpecificWait,
            None,
        )
        .await
        .unwrap();
    let mut session = f.base.clone();
    let admission = bound
        .admit_root_inbox(&mut session, f.raw.clone(), Some("local-run-a"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.merged, 1);
    assert!(
        admission.admission_error.is_none(),
        "{:?}",
        admission.admission_error
    );
    assert!(f.raw.was_admitted(CHILD, &input.id).await.unwrap());
    session.add_message(Message::assistant("runtime answer", None));
    bound
        .checkpoint_runtime_session(&mut session)
        .await
        .unwrap();
    let durable = f.b.load_session(CHILD).await.unwrap().unwrap();
    assert_eq!(durable.messages.last().unwrap().content, "runtime answer");
    let next = bound
        .admit_root_inbox(&mut session, f.raw.clone(), Some("local-run-a"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.merged, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_child_input_publisher_keeps_started_job_and_inbox_physically_locked() {
    let f = Fixture::new().await;
    let (input, claim) = f.input().await;
    let recovery_claim = claim.clone();
    let hook = DefaultWriteHook::install(
        &f.a,
        "session.json",
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = Release(hook.clone());
    let task = {
        let store = f.a.clone();
        let owner = f.owner.clone();
        let mut candidate = f.candidate(&claim);
        let inbox = f.bound.clone();
        tokio::spawn(async move {
            store
                .save_child_actor_runtime(ChildActorRuntimeSave {
                    owner: &owner,
                    session: &mut candidate,
                    runtime_only: false,
                    inherited: None,
                    finalized_wait: None,
                    input: Some((inbox, claim)),
                    publish: Arc::new(|_| {}),
                })
                .await
        })
    };
    hook.wait();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    held(&f.a, Some(&f.directory()));
    assert!(!f.raw.was_admitted(CHILD, &input.id).await.unwrap());
    hook.release();
    tokio::time::timeout(WAIT, async {
        loop {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(f.a.session_write_lock_path(CHILD))
                .unwrap();
            if FileExt::try_lock_exclusive(&file).is_ok() {
                FileExt::unlock(&file).unwrap();
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !f.raw.was_admitted(CHILD, &input.id).await.unwrap(),
        "aborted waiter cannot skip ACK protocol"
    );
    let durable = f.b.load_session(CHILD).await.unwrap().unwrap();
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|m| bamboo_domain::is_matching_session_message(m, &input))
            .count(),
        1
    );
    f.checkpoint(&recovery_claim).await.unwrap();
    assert!(f.raw.was_admitted(CHILD, &input.id).await.unwrap());
    let recovered = f.b.load_session(CHILD).await.unwrap().unwrap();
    assert_eq!(
        recovered
            .messages
            .iter()
            .filter(|m| bamboo_domain::is_matching_session_message(m, &input))
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_child_owner_is_rechecked_at_actual_filesystem_replace() {
    let f = Fixture::with_duration(LeaseDuration::seconds(2)).await;
    let before_main = std::fs::read(f.directory().join("session.json")).unwrap();
    let before_runtime = std::fs::read(f.directory().join(RUNTIME_SIDECAR_FILE)).unwrap();
    let deadline =
        f.a.inspect_actor(CHILD)
            .await
            .unwrap()
            .activation
            .unwrap()
            .lease_expires_at;
    let hook = DefaultWriteHook::install(
        &f.a,
        RUNTIME_SIDECAR_FILE,
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = Release(hook.clone());
    let task = {
        let store = f.a.clone();
        let owner = f.owner.clone();
        let mut candidate = f.base.clone();
        candidate.add_message(Message::assistant("expired response", None));
        let publish = f.publisher();
        tokio::spawn(async move {
            store
                .save_child_actor_runtime(ChildActorRuntimeSave {
                    owner: &owner,
                    session: &mut candidate,
                    runtime_only: false,
                    inherited: None,
                    finalized_wait: None,
                    input: None,
                    publish,
                })
                .await
        })
    };
    hook.wait();
    held(&f.a, None);
    tokio::time::timeout(WAIT, async {
        while Utc::now() <= deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    hook.release();
    assert!(task.await.unwrap().is_err());
    assert_eq!(
        std::fs::read(f.directory().join("session.json")).unwrap(),
        before_main
    );
    assert_eq!(
        std::fs::read(f.directory().join(RUNTIME_SIDECAR_FILE)).unwrap(),
        before_runtime
    );
    assert!(f.seen.lock().unwrap().is_empty());
}

fn native_question(f: &Fixture) -> Session {
    use bamboo_domain::{AgentRuntimeState, FunctionCall, PendingQuestionSource, ToolCall};
    let mut child = f.base.clone();
    child.agent_runtime_state = Some(AgentRuntimeState::new(&f.owner.fence.run_id));
    child.set_last_run_status("running");
    child.add_message(Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "native-question-call".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "AskUserQuestion".into(),
                arguments: "{}".into(),
            },
        }]),
    ));
    child.add_message(Message::tool_result_with_status(
        "native-question-call",
        "Clarification needed: Which option?",
        true,
    ));
    child.set_pending_question_with_source(
        "native-question-call".into(),
        "AskUserQuestion".into(),
        "Which option?".into(),
        vec!["A".into(), "B".into()],
        false,
        PendingQuestionSource::AgenticClarification,
    );
    child.metadata.insert(
        "runtime.suspend_reason".into(),
        "awaiting_clarification".into(),
    );
    child
}

#[tokio::test]
async fn native_child_question_full_checkpoint_normalizes_and_replays_exact_request() {
    use bamboo_domain::{ParentQuestion, PendingQuestionSource, PARENT_QUESTION_REQUEST_KEY};
    let f = Fixture::new().await;
    let parent = f.a.load_session(ROOT).await.unwrap().unwrap();
    let mut candidate = native_question(&f);
    let mut stale_agentic = candidate.clone();
    f.save(&mut candidate, false).await.unwrap();
    let canonical = f.b.load_session(CHILD).await.unwrap().unwrap();
    assert_eq!(
        candidate.pending_question.as_ref().unwrap().source,
        PendingQuestionSource::DirectParent
    );
    let request = ParentQuestion::for_pending(&parent, &canonical).unwrap();
    assert_eq!(request.parent.created_at, f.owner.parent_created_at);
    assert_eq!(request.child.created_at, f.owner.created_at);
    assert_eq!(
        request.tool_result_message_id,
        candidate.messages.last().unwrap().id
    );
    assert_eq!(
        candidate.metadata[PARENT_QUESTION_REQUEST_KEY],
        canonical.metadata[PARENT_QUESTION_REQUEST_KEY]
    );
    assert_eq!(
        ParentQuestion::for_pending(&parent, &candidate),
        Some(request.clone())
    );
    assert_eq!(
        ParentQuestion::for_pending(&parent, f.seen.lock().unwrap().last().unwrap()),
        Some(request.clone())
    );
    f.save(&mut stale_agentic, false).await.unwrap();
    assert_eq!(
        ParentQuestion::for_pending(&parent, &stale_agentic),
        Some(request.clone())
    );
    f.save(&mut candidate, true).await.unwrap();
    let replay = f.b.load_session(CHILD).await.unwrap().unwrap();
    assert_eq!(ParentQuestion::for_pending(&parent, &replay), Some(request));
    assert!(!replay
        .metadata
        .contains_key("runtime.actor_parent_question_handoff"));
}

#[tokio::test]
async fn native_child_question_rejects_wrong_run_and_invalid_paired_occurrences() {
    use bamboo_domain::PARENT_QUESTION_REQUEST_KEY;
    for case in [
        "run",
        "no_run",
        "no_call",
        "result_text",
        "failed_result",
        "duplicate_result",
    ] {
        let f = Fixture::new().await;
        let mut candidate = native_question(&f);
        match case {
            "run" => candidate.agent_runtime_state.as_mut().unwrap().run_id = "other-run".into(),
            "no_run" => candidate.agent_runtime_state = None,
            "no_call" => {
                candidate.messages.remove(1);
            }
            "result_text" => {
                candidate.messages.last_mut().unwrap().content = "unpaired text".into()
            }
            "failed_result" => candidate.messages.last_mut().unwrap().tool_success = Some(false),
            "duplicate_result" => candidate
                .messages
                .push(candidate.messages.last().unwrap().clone()),
            _ => unreachable!(),
        }
        let before = std::fs::read(f.directory().join("session.json")).unwrap();
        let before_runtime = std::fs::read(f.directory().join(RUNTIME_SIDECAR_FILE)).unwrap();
        assert!(f.save(&mut candidate, false).await.is_err(), "{case}");
        assert_eq!(
            std::fs::read(f.directory().join("session.json")).unwrap(),
            before,
            "{case}"
        );
        assert_eq!(
            std::fs::read(f.directory().join(RUNTIME_SIDECAR_FILE)).unwrap(),
            before_runtime,
            "{case}"
        );
        assert!(!candidate.metadata.contains_key(PARENT_QUESTION_REQUEST_KEY));
        assert!(f.seen.lock().unwrap().is_empty(), "{case}");
    }
}

#[tokio::test]
async fn native_child_question_runtime_only_requires_canonical_paired_history() {
    use bamboo_domain::{ParentQuestion, PARENT_QUESTION_REQUEST_KEY};
    let f = Fixture::new().await;
    let mut candidate = native_question(&f);
    assert!(f.save(&mut candidate, true).await.is_err());
    let unchanged = f.b.load_session(CHILD).await.unwrap().unwrap();
    assert!(unchanged.pending_question.is_none());
    assert!(!unchanged.metadata.contains_key(PARENT_QUESTION_REQUEST_KEY));

    // A data collector may already have checkpointed the exact call/result
    // before the suspension checkpoint. Only then may a runtime save issue it.
    let mut history = candidate.clone();
    history.pending_question = None;
    history.metadata.remove("runtime.suspend_reason");
    f.save(&mut history, false).await.unwrap();
    let main_before = std::fs::read(f.directory().join("session.json")).unwrap();
    f.save(&mut candidate, true).await.unwrap();
    assert_eq!(
        std::fs::read(f.directory().join("session.json")).unwrap(),
        main_before
    );
    let parent = f.a.load_session(ROOT).await.unwrap().unwrap();
    let canonical = f.b.load_session(CHILD).await.unwrap().unwrap();
    assert_eq!(
        ParentQuestion::for_pending(&parent, &candidate),
        ParentQuestion::for_pending(&parent, &canonical)
    );
    assert!(ParentQuestion::for_pending(&parent, &canonical).is_some());
}

#[tokio::test]
async fn native_child_question_default_writers_never_issue_parent_requests() {
    use bamboo_domain::{PendingQuestionSource, PARENT_QUESTION_REQUEST_KEY};
    let f = Fixture::new().await;
    let mut cold_child = native_question(&f);
    cold_child.id = "unbound-clarification".into();
    f.a.save_session(&cold_child).await.unwrap();
    let saved = f.a.load_session(&cold_child.id).await.unwrap().unwrap();
    assert_eq!(
        saved.pending_question.unwrap().source,
        PendingQuestionSource::AgenticClarification
    );
    assert!(!saved.metadata.contains_key(PARENT_QUESTION_REQUEST_KEY));

    let mut root = Session::new("unbound-clarification-root", "model");
    root.messages = cold_child.messages;
    root.pending_question = cold_child.pending_question;
    root.metadata = cold_child.metadata;
    f.a.save_session(&root).await.unwrap();
    let saved = f.a.load_session(&root.id).await.unwrap().unwrap();
    assert_eq!(
        saved.pending_question.unwrap().source,
        PendingQuestionSource::AgenticClarification
    );
    assert!(!saved.metadata.contains_key(PARENT_QUESTION_REQUEST_KEY));
}

#[tokio::test]
async fn native_child_question_fast_parent_answer_wins_over_stale_final_checkpoint() {
    use bamboo_domain::{ParentQuestion, ParentQuestionResolution, PARENT_QUESTION_RESOLUTION_KEY};
    let f = Fixture::new().await;
    let mut candidate = native_question(&f);
    let mut stale_agentic = candidate.clone();
    f.save(&mut candidate, false).await.unwrap();
    let mut parent = f.a.load_session(ROOT).await.unwrap().unwrap();
    let request = ParentQuestion::for_pending(&parent, &candidate).unwrap();
    parent.add_message(request.envelope().to_provider_message().unwrap());
    f.b.save_session(&parent).await.unwrap();
    let (answered, newly_answered) =
        f.b.answer_parent_question(&request, "A", |_| {})
            .await
            .unwrap()
            .expect("canonical parent answer must support an activated native Child");
    assert!(newly_answered);
    let resolution = ParentQuestionResolution::from_child(&parent, &answered, &request.id).unwrap();
    assert!(
        !f.b.answer_parent_question(&request, "A", |_| {})
            .await
            .unwrap()
            .unwrap()
            .1
    );
    f.save(&mut stale_agentic, false).await.unwrap();
    assert_eq!(stale_agentic.messages.last().unwrap().content, "A");
    assert_eq!(
        stale_agentic.metadata[PARENT_QUESTION_RESOLUTION_KEY],
        answered.metadata[PARENT_QUESTION_RESOLUTION_KEY]
    );
    assert_eq!(
        ParentQuestionResolution::from_child(&parent, &stale_agentic, &request.id),
        Some(resolution.clone())
    );
    f.save(&mut candidate, true).await.unwrap();
    let final_child = f.b.load_session(CHILD).await.unwrap().unwrap();
    assert_eq!(final_child.messages.last().unwrap().content, "A");
    assert_eq!(
        ParentQuestionResolution::from_child(&parent, &final_child, &request.id),
        Some(resolution)
    );
    // A different ask cannot silently reuse the old request's authority or
    // deadline. Request rollover remains outside the existing protocol.
    let mut later = native_question(&f);
    later.pending_question.as_mut().unwrap().tool_call_id = "later-question-call".into();
    later.messages[1].tool_calls.as_mut().unwrap()[0].id = "later-question-call".into();
    later.messages.last_mut().unwrap().tool_call_id = Some("later-question-call".into());
    let before = std::fs::read(f.directory().join("session.json")).unwrap();
    let before_runtime = std::fs::read(f.directory().join(RUNTIME_SIDECAR_FILE)).unwrap();
    assert!(f.save(&mut later, false).await.is_err());
    assert_eq!(
        std::fs::read(f.directory().join("session.json")).unwrap(),
        before
    );
    assert_eq!(
        std::fs::read(f.directory().join(RUNTIME_SIDECAR_FILE)).unwrap(),
        before_runtime
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_child_question_expired_owner_cannot_publish_issued_request() {
    use bamboo_domain::PARENT_QUESTION_REQUEST_KEY;
    let f = Fixture::with_duration(LeaseDuration::seconds(2)).await;
    let mut candidate = native_question(&f);
    let before = std::fs::read(f.directory().join("session.json")).unwrap();
    let before_runtime = std::fs::read(f.directory().join(RUNTIME_SIDECAR_FILE)).unwrap();
    let deadline =
        f.a.inspect_actor(CHILD)
            .await
            .unwrap()
            .activation
            .unwrap()
            .lease_expires_at;
    let hook = DefaultWriteHook::install(
        &f.a,
        RUNTIME_SIDECAR_FILE,
        DurableWritePhase::BeforeReplace,
        false,
    );
    let _release = Release(hook.clone());
    let store = f.a.clone();
    let owner = f.owner.clone();
    let publish = f.publisher();
    let job = tokio::spawn(async move {
        store
            .save_child_actor_runtime(ChildActorRuntimeSave {
                owner: &owner,
                session: &mut candidate,
                runtime_only: false,
                inherited: None,
                finalized_wait: None,
                input: None,
                publish,
            })
            .await
    });
    hook.wait();
    held(&f.a, None);
    while Utc::now() <= deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    hook.release();
    assert!(tokio::time::timeout(WAIT, job)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert_eq!(
        std::fs::read(f.directory().join("session.json")).unwrap(),
        before
    );
    assert_eq!(
        std::fs::read(f.directory().join(RUNTIME_SIDECAR_FILE)).unwrap(),
        before_runtime
    );
    assert!(f.seen.lock().unwrap().is_empty());
    let saved = f.b.load_session(CHILD).await.unwrap().unwrap();
    assert!(!saved.metadata.contains_key(PARENT_QUESTION_REQUEST_KEY));
}

struct PausedChildReadStorage {
    inner: Arc<SessionStoreV2>,
    reached: tokio::sync::Barrier,
    release: tokio::sync::Barrier,
    pause_after_read: bool,
}
#[async_trait::async_trait]
impl Storage for PausedChildReadStorage {
    fn supports_child_actor_runtime_write(&self) -> bool {
        true
    }
    async fn save_child_actor_runtime(&self, request: ChildActorRuntimeSave<'_>) -> io::Result<()> {
        self.inner.save_child_actor_runtime(request).await
    }
    async fn publish_child_actor_runtime_event(
        &self,
        owner: &ChildActorRuntimeWrite,
        publish: bamboo_domain::RootActorRuntimeEventPublisher,
    ) -> io::Result<()> {
        self.inner
            .publish_child_actor_runtime_event(owner, publish)
            .await
    }
    async fn save_session(&self, session: &Session) -> io::Result<()> {
        self.inner.save_session(session).await
    }
    async fn delete_session(&self, id: &str) -> io::Result<bool> {
        self.inner.delete_session(id).await
    }
    async fn load_session(&self, id: &str) -> io::Result<Option<Session>> {
        self.paused_load(id, false).await
    }
    async fn load_runtime_control_plane(&self, id: &str) -> io::Result<Option<Session>> {
        self.paused_load(id, true).await
    }
}
impl PausedChildReadStorage {
    async fn paused_load(&self, id: &str, control_plane: bool) -> io::Result<Option<Session>> {
        if !self.pause_after_read {
            self.reached.wait().await;
            self.release.wait().await;
        }
        let loaded = if control_plane {
            self.inner.load_runtime_control_plane(id).await?
        } else {
            self.inner.load_session(id).await?
        };
        if self.pause_after_read {
            self.reached.wait().await;
            self.release.wait().await;
        }
        Ok(loaded)
    }
}

#[derive(Clone, Copy)]
enum ReadSnapshot {
    Full,
    Control,
    Admission,
}

async fn paused_bound_read(
    f: &Fixture,
    pause_after_read: bool,
    kind: ReadSnapshot,
) -> (
    Arc<PausedChildReadStorage>,
    tokio::task::JoinHandle<io::Result<Option<Session>>>,
) {
    let paused = Arc::new(PausedChildReadStorage {
        inner: f.a.clone(),
        reached: tokio::sync::Barrier::new(2),
        release: tokio::sync::Barrier::new(2),
        pause_after_read,
    });
    let bound = crate::LockedSessionStore::new(paused.clone())
        .bind_child_actor_writer(f.owner.clone(), f.a.clone(), f.publisher())
        .unwrap();
    let read = tokio::spawn(async move {
        match kind {
            ReadSnapshot::Full => bound.load_runtime_session(CHILD).await,
            ReadSnapshot::Control => bound.load_runtime_control_plane(CHILD).await,
            ReadSnapshot::Admission => bound.load_runtime_admission_snapshot(CHILD).await,
        }
    });
    tokio::time::timeout(WAIT, paused.reached.wait())
        .await
        .unwrap();
    (paused, read)
}

#[tokio::test]
async fn child_bound_read_rejects_takeover_during_snapshot_materialization() {
    for (pause_after_read, kind) in [
        (false, ReadSnapshot::Full),
        (true, ReadSnapshot::Full),
        (false, ReadSnapshot::Control),
        (true, ReadSnapshot::Control),
        (false, ReadSnapshot::Admission),
        (true, ReadSnapshot::Admission),
    ] {
        let f = Fixture::new().await;
        let (paused, read) = paused_bound_read(&f, pause_after_read, kind).await;
        f.b.finish_activation(&f.owner.fence, Utc::now(), ActorActivationFinish::Cancelled)
            .await
            .unwrap();
        let now = Utc::now();
        let successor =
            f.b.claim_activation(&ActorActivationClaim {
                actor_id: CHILD.into(),
                run_id: "read-successor-run".into(),
                lease_owner: "read-successor-owner".into(),
                lease_expires_at: now + LeaseDuration::minutes(1),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
            .unwrap();
        f.b.start_activation(&successor.fence(), Utc::now())
            .await
            .unwrap();
        paused.release.wait().await;
        let error = tokio::time::timeout(WAIT, read)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error
            .get_ref()
            .is_some_and(|cause| cause.is::<bamboo_domain::SessionAuthorityConflict>()));
        assert_eq!(
            f.b.inspect_actor(CHILD)
                .await
                .unwrap()
                .activation
                .unwrap()
                .fence(),
            successor.fence()
        );
        assert!(f.seen.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn child_bound_read_rejects_delete_rebirth_during_snapshot_materialization() {
    for (pause_after_read, kind) in [
        (false, ReadSnapshot::Full),
        (true, ReadSnapshot::Full),
        (false, ReadSnapshot::Control),
        (true, ReadSnapshot::Control),
        (false, ReadSnapshot::Admission),
        (true, ReadSnapshot::Admission),
    ] {
        let f = Fixture::new().await;
        let (paused, read) = paused_bound_read(&f, pause_after_read, kind).await;
        assert!(f.b.delete_session(CHILD).await.unwrap());
        let parent = f.b.load_session(ROOT).await.unwrap().unwrap();
        let mut reborn = Session::new_child_of(CHILD, &parent, "new-model", "new Child birth");
        assert_ne!(reborn.created_at, f.owner.created_at);
        reborn.add_message(Message::user("successor-private transcript"));
        f.b.save_session(&reborn).await.unwrap();
        paused.release.wait().await;
        let error = tokio::time::timeout(WAIT, read)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error
            .get_ref()
            .is_some_and(|cause| cause.is::<bamboo_domain::SessionAuthorityConflict>()));
        let canonical = f.b.load_session(CHILD).await.unwrap().unwrap();
        assert_eq!(canonical.created_at, reborn.created_at);
        assert_eq!(
            canonical.messages.last().unwrap().content,
            "successor-private transcript"
        );
        assert!(
            !f.directory()
                .join("actor-authority.initialized.json")
                .exists(),
            "rejected read must not initialize new Actor authority"
        );
        assert!(f.seen.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn child_bound_control_plane_read_allows_only_own_child_and_shared_task_root() {
    let f = Fixture::new().await;
    let bound = crate::LockedSessionStore::new(f.a.clone())
        .bind_child_actor_writer(f.owner.clone(), f.a.clone(), f.publisher())
        .unwrap();
    let child = bound
        .load_runtime_control_plane(CHILD)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child.created_at, f.owner.created_at);
    assert!(child.messages.is_empty());
    let root = bound
        .load_runtime_control_plane(ROOT)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root.created_at, f.owner.parent_created_at);
    assert!(root.messages.is_empty());
    assert!(bound
        .load_runtime_control_plane("unrelated-session")
        .await
        .is_err());
    // Snapshot reads never inherit the Task-only Root exception.
    assert!(bound.load_runtime_session(ROOT).await.is_err());
    assert!(bound.load_runtime_admission_snapshot(ROOT).await.is_err());
    let admitted = bound
        .load_runtime_admission_snapshot(CHILD)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admitted.created_at, f.owner.created_at);
    assert_eq!(
        serde_json::to_value(&admitted.messages).unwrap(),
        serde_json::to_value(&f.base.messages).unwrap()
    );
    f.b.finish_activation(&f.owner.fence, Utc::now(), ActorActivationFinish::Cancelled)
        .await
        .unwrap();
    assert!(bound.load_runtime_control_plane(CHILD).await.is_err());
    assert!(bound.load_runtime_control_plane(ROOT).await.is_err());
    assert!(bound.load_runtime_admission_snapshot(CHILD).await.is_err());
}
