//! Actual repository-issued Root ownership through the generic forwarder.
//! These are publication-port tests; the two real Host/provider processes in
//! ordinary_root_actor_writer prove the production execution claim separately.

use std::{fs::File, path::Path, time::Duration};

use bamboo_agent_core::{storage::Storage, Message, Session};
use bamboo_domain::{ActorDirectoryPort, RootActorExecutionBinding, RuntimeSessionPersistence};
use bamboo_storage::{LockedSessionStore, SessionStoreV2};
use fs2::FileExt;
use tokio::sync::broadcast::error::TryRecvError;

use super::*;
use crate::{
    events::{journal, AccountEventSink, ChangeEvent, RootActorEventPublication},
    session_repository::SessionRepository,
};

const ROOT: &str = "generic-root-publication";
const WAIT: Duration = Duration::from_secs(8);

struct JournalClaim(File);

impl JournalClaim {
    fn acquire(events: &Path) -> Self {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(events.join(".account-journal.lock"))
            .unwrap();
        FileExt::lock_exclusive(&file).unwrap();
        Self(file)
    }
}

impl Drop for JournalClaim {
    fn drop(&mut self) {
        FileExt::unlock(&self.0).unwrap();
    }
}

struct Fixture {
    a: Arc<SessionStoreV2>,
    b: Arc<SessionStoreV2>,
    root: Session,
    sink: Arc<AccountEventSink>,
    _temp: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("bamboo");
        let a = Arc::new(SessionStoreV2::new(data.clone()).await.unwrap());
        let mut root = Session::new(ROOT, "controlled-model");
        root.add_message(Message::user("generic Root publication fixture"));
        a.save_session(&root).await.unwrap();
        let b = Arc::new(SessionStoreV2::new(data).await.unwrap());
        let sink = AccountEventSink::new(temp.path().join("events")).unwrap();
        Self {
            a,
            b,
            root,
            sink,
            _temp: temp,
        }
    }

    async fn bind(&self, store: Arc<SessionStoreV2>, run_id: &str) -> RootActorExecutionBinding {
        let repository = SessionRepository::new(
            Arc::default(),
            store.clone(),
            Arc::new(LockedSessionStore::new(store.clone())),
        )
        .with_root_actor_directory(store);
        tokio::time::timeout(
            WAIT,
            repository.bind_root_actor_execution(&self.root, run_id),
        )
        .await
        .expect("repository issues the actual Root binding without a journal deadlock")
        .unwrap()
        .expect("ordinary persisted Root receives its execution capability")
    }

    async fn expire(&self, binding: &RootActorExecutionBinding) {
        // Retain the repository-issued binding. This focused port test does not
        // start a runtime renewal task, forge a timestamp, or finish the owner.
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let activation = self
                    .a
                    .inspect_actor(ROOT)
                    .await
                    .unwrap()
                    .activation
                    .unwrap();
                assert_eq!(activation.fence(), binding.owner.fence);
                if activation.lease_expires_at <= Utc::now() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the repository's actual 15 second Root lease expires");
    }
}

struct Forwarder {
    input: mpsc::Sender<AgentEvent>,
    task: tokio::task::JoinHandle<()>,
    history: HistoryCommitBarrier,
    session: broadcast::Receiver<AgentEvent>,
    legacy: mpsc::Receiver<(Option<String>, AgentEvent)>,
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
}

impl Forwarder {
    fn new(
        fixture: &Fixture,
        store: Arc<SessionStoreV2>,
        binding: &RootActorExecutionBinding,
        run_id: &str,
    ) -> Self {
        let (event_sender, session) = broadcast::channel(32);
        let mut runner = AgentRunner::new();
        runner.status = super::super::runner_state::AgentStatus::Running;
        runner.event_sender = event_sender.clone();
        runner.run_id = run_id.into();
        let runners = Arc::new(RwLock::new(HashMap::from([(ROOT.into(), runner)])));
        let (legacy_tx, legacy) = mpsc::channel(32);
        let (input, task, history) = create_event_forwarder_with_root_actor(
            ROOT.into(),
            run_id.into(),
            event_sender,
            runners.clone(),
            Some(legacy_tx),
            Some(RootActorEventPublication::new(
                store,
                fixture.sink.clone(),
                binding.owner.clone(),
            )),
        );
        Self {
            input,
            task,
            history,
            session,
            legacy,
            runners,
        }
    }
}

fn clarification(question: &str) -> AgentEvent {
    AgentEvent::NeedClarification {
        question: question.into(),
        options: None,
        tool_call_id: Some("controlled-pause".into()),
        tool_name: Some("controlled-tool".into()),
        allow_custom: true,
        source: None,
    }
}

fn assert_frame(actual: &AgentEvent, expected: &AgentEvent) {
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
}

async fn session_event(receiver: &mut broadcast::Receiver<AgentEvent>) -> AgentEvent {
    tokio::time::timeout(WAIT, receiver.recv())
        .await
        .expect("actual per-session publication progresses")
        .unwrap()
}

async fn account_event(receiver: &mut broadcast::Receiver<Arc<ChangeEvent>>) -> Arc<ChangeEvent> {
    tokio::time::timeout(WAIT, receiver.recv())
        .await
        .expect("actual account append and live broadcast complete")
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_generic_started_critical_and_history_wait_for_actual_account_receipt() {
    let fixture = Fixture::new().await;
    let binding = fixture.bind(fixture.a.clone(), "run-current").await;
    let mut live = fixture.sink.subscribe();
    let mut forwarder = Forwarder::new(&fixture, fixture.a.clone(), &binding, "run-current");
    let started = session_event(&mut forwarder.session).await;
    assert!(
        matches!(&started, AgentEvent::ExecutionStarted { run_id, session_id, .. }
        if run_id == "run-current" && session_id == ROOT)
    );
    let started_live = account_event(&mut live).await;
    assert_frame(&started_live.event, &started);
    assert_eq!(started_live.seq, 1);

    let critical = clarification("current Root pause");
    forwarder.input.send(critical.clone()).await.unwrap();
    assert_frame(&session_event(&mut forwarder.session).await, &critical);
    let critical_live = account_event(&mut live).await;
    assert_frame(&critical_live.event, &critical);
    assert_eq!(critical_live.seq, 2);
    {
        let runners = forwarder.runners.read().await;
        let runner = runners.get(ROOT).unwrap();
        assert_eq!(runner.last_critical_events.len(), 1);
        assert_frame(&runner.last_critical_events[0], &critical);
        assert!(runner.last_event_at.is_some());
    }

    let claim = JournalClaim::acquire(fixture.sink.events_dir());
    let history_input = forwarder.input.clone();
    let mut history = forwarder.history.clone();
    let mut waiting =
        tokio::spawn(async move { history.send_and_wait(&history_input, ROOT.into()).await });
    let history_frame = session_event(&mut forwarder.session).await;
    assert!(matches!(
        history_frame,
        AgentEvent::SessionHistoryCommitted { .. }
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut waiting)
            .await
            .is_err(),
        "history is not acknowledged at per-session publication or account queue admission"
    );
    assert_eq!(
        journal::read_since(fixture.sink.events_dir(), 0)
            .unwrap()
            .len(),
        2
    );
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
    assert!(
        matches!(
            forwarder.legacy.try_recv(),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ),
        "a bound Root never falls back to the supplied legacy account inbox"
    );
    drop(claim);
    assert!(tokio::time::timeout(WAIT, waiting).await.unwrap().unwrap());
    let history_live = account_event(&mut live).await;
    assert_frame(&history_live.event, &history_frame);
    assert_eq!(history_live.seq, 3);
    let journal = journal::read_since(fixture.sink.events_dir(), 0).unwrap();
    assert_eq!(journal.len(), 3);
    for (stored, emitted) in journal
        .iter()
        .zip([started_live, critical_live, history_live])
    {
        assert_eq!(stored.seq, emitted.seq);
        assert_frame(&stored.event, &emitted.event);
    }
    drop(forwarder.input);
    tokio::time::timeout(WAIT, forwarder.task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaimed_generic_root_rejects_critical_cache_and_history_ack_without_legacy_fallback() {
    let fixture = Fixture::new().await;
    let a = fixture.bind(fixture.a.clone(), "run-a").await;
    let mut live = fixture.sink.subscribe();
    // Independent forwarders isolate the two rejection branches while using
    // the same real A capability; neither is a replacement production claim.
    let mut critical = Forwarder::new(&fixture, fixture.a.clone(), &a, "run-a");
    let mut history = Forwarder::new(&fixture, fixture.a.clone(), &a, "run-a");
    for stream in [&mut critical, &mut history] {
        assert!(matches!(
            session_event(&mut stream.session).await,
            AgentEvent::ExecutionStarted { .. }
        ));
    }
    for sequence in 1..=2 {
        let frame = account_event(&mut live).await;
        assert_eq!(frame.seq, sequence);
        assert!(matches!(frame.event, AgentEvent::ExecutionStarted { .. }));
    }
    fixture.expire(&a).await;
    let b = fixture.bind(fixture.b.clone(), "run-b").await;
    assert!(b.owner.fence.attempt > a.owner.fence.attempt);
    assert!(b.owner.fence.lease_epoch > a.owner.fence.lease_epoch);
    let mut successor = Forwarder::new(&fixture, fixture.b.clone(), &b, "run-b");
    assert!(matches!(session_event(&mut successor.session).await,
        AgentEvent::ExecutionStarted { run_id, .. } if run_id == "run-b"));
    assert_eq!(account_event(&mut live).await.seq, 3);

    critical
        .input
        .send(clarification("obsolete A pause"))
        .await
        .unwrap();
    tokio::time::timeout(WAIT, critical.task)
        .await
        .unwrap()
        .unwrap();
    assert_frame(
        &session_event(&mut critical.session).await,
        &AgentEvent::Error {
            message: ROOT_AUTHORITY_LOST.into(),
        },
    );
    assert!(matches!(
        critical.session.try_recv(),
        Err(TryRecvError::Empty)
    ));
    {
        let runners = critical.runners.read().await;
        let runner = runners.get(ROOT).unwrap();
        assert!(
            runner.last_critical_events.is_empty(),
            "old critical replay cache is fenced before mutation"
        );
        assert!(runner.last_event_at.is_none());
    }
    assert!(
        !tokio::time::timeout(
            WAIT,
            history.history.send_and_wait(&history.input, ROOT.into())
        )
        .await
        .unwrap(),
        "reclaimed A cannot acknowledge an uncommitted history event"
    );
    tokio::time::timeout(WAIT, history.task)
        .await
        .unwrap()
        .unwrap();
    assert_frame(
        &session_event(&mut history.session).await,
        &AgentEvent::Error {
            message: ROOT_AUTHORITY_LOST.into(),
        },
    );
    assert!(matches!(
        history.session.try_recv(),
        Err(TryRecvError::Empty)
    ));
    for legacy in [&mut critical.legacy, &mut history.legacy] {
        assert!(
            matches!(
                legacy.try_recv(),
                Err(mpsc::error::TryRecvError::Disconnected)
            ),
            "rejected bound Root does not enter a legacy queue"
        );
    }
    assert_eq!(
        journal::read_since(fixture.sink.events_dir(), 0)
            .unwrap()
            .len(),
        3
    );
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));

    let complete = AgentEvent::Complete {
        usage: Default::default(),
    };
    successor.input.send(complete.clone()).await.unwrap();
    assert_frame(&session_event(&mut successor.session).await, &complete);
    assert_frame(&account_event(&mut live).await.event, &complete);
    assert!(tokio::time::timeout(
        WAIT,
        successor
            .history
            .send_and_wait(&successor.input, ROOT.into())
    )
    .await
    .unwrap());
    assert!(matches!(
        session_event(&mut successor.session).await,
        AgentEvent::SessionHistoryCommitted { .. }
    ));
    assert!(matches!(
        account_event(&mut live).await.event,
        AgentEvent::SessionHistoryCommitted { .. }
    ));
    assert_eq!(fixture.sink.latest_seq(), 5);
    assert_eq!(
        journal::read_since(fixture.sink.events_dir(), 0)
            .unwrap()
            .len(),
        5
    );
    assert_eq!(
        fixture
            .b
            .inspect_actor(ROOT)
            .await
            .unwrap()
            .activation
            .unwrap()
            .fence(),
        b.owner.fence
    );
    drop(successor.input);
    tokio::time::timeout(WAIT, successor.task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_root_authority_interrupts_live_run_without_publishing_rejected_state() {
    let fixture = Fixture::new().await;
    let binding = fixture.bind(fixture.a.clone(), "lost-run").await;
    let mut live = fixture.sink.subscribe();
    let mut forwarder = Forwarder::new(&fixture, fixture.a.clone(), &binding, "lost-run");
    assert!(matches!(
        session_event(&mut forwarder.session).await,
        AgentEvent::ExecutionStarted { .. }
    ));
    assert_eq!(account_event(&mut live).await.seq, 1);
    let (cancel, publication, visible) = {
        let guard = forwarder.runners.read().await;
        let runner = guard.get(ROOT).unwrap();
        (
            runner.cancel_token.clone(),
            runner.event_publication.clone(),
            runner.visible_messages.clone(),
        )
    };
    let (mut message_events, _) = visible.subscribe_with_snapshot();
    // Revoke the real issued fence without a wall-clock lease-expiry race.
    fixture
        .a
        .finish_activation(
            &binding.owner.fence,
            Utc::now(),
            bamboo_domain::ActorActivationFinish::Failed,
        )
        .await
        .unwrap();
    let session_path = fixture
        ._temp
        .path()
        .join("bamboo/sessions")
        .join(ROOT)
        .join("session.json");
    let persisted = std::fs::read(&session_path).unwrap();
    forwarder
        .input
        .send(clarification("rejected private content"))
        .await
        .unwrap();
    assert_frame(
        &session_event(&mut forwarder.session).await,
        &AgentEvent::Error {
            message: ROOT_AUTHORITY_LOST.into(),
        },
    );
    tokio::time::timeout(WAIT, forwarder.task)
        .await
        .unwrap()
        .unwrap();
    assert!(cancel.is_cancelled());
    assert!(matches!(
        message_events.recv().await.unwrap().kind,
        super::super::visible_messages::VisibleMessageEventKind::Terminal { reason }
        if reason == "error"
    ));
    assert!(!publication.publish(|| panic!("obsolete token was published")));
    assert!(matches!(
        forwarder.session.try_recv(),
        Err(TryRecvError::Empty)
    ));
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(std::fs::read(session_path).unwrap(), persisted);
    assert_eq!(fixture.sink.latest_seq(), 1);
    assert!(forwarder.runners.read().await[ROOT]
        .last_critical_events
        .is_empty());
    assert!(
        !forwarder
            .history
            .send_and_wait(&forwarder.input, ROOT.into())
            .await
    );
}

#[tokio::test]
async fn obsolete_root_interruption_cannot_terminalize_local_successor_on_shared_channel() {
    let (sender, mut receiver) = broadcast::channel(16);
    let mut old = AgentRunner::new();
    old.event_sender = sender.clone();
    let old_id = old.run_id.clone();
    let old_cancel = old.cancel_token.clone();
    let old_publication = old.event_publication.clone();
    let old_visible = old.visible_messages.clone();
    old.status = super::super::runner_state::AgentStatus::Completed;
    let runners = Arc::new(RwLock::new(HashMap::from([(ROOT.into(), old)])));
    let senders = Arc::default();
    let successor =
        super::super::runner_lifecycle::try_reserve_runner(&runners, &senders, ROOT, &sender)
            .await
            .unwrap();
    let started = AgentEvent::ExecutionStarted {
        session_id: ROOT.into(),
        run_id: successor.run_id.clone(),
        started_at: Utc::now().to_rfc3339(),
    };
    sender.send(started.clone()).unwrap();
    let error = std::io::Error::other(bamboo_domain::SessionAuthorityConflict(
        "obsolete fence".into(),
    ));
    interrupt_root_on_authority_loss(
        &error,
        ROOT,
        &old_id,
        &runners,
        &old_publication,
        &old_visible,
        &old_cancel,
    )
    .await;
    assert!(old_cancel.is_cancelled());
    assert!(!successor.cancel_token.is_cancelled());
    assert_frame(&session_event(&mut receiver).await, &started);
    assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
    assert!(
        !super::super::runner_lifecycle::finalize_runner_exact(
            &runners,
            ROOT,
            &old_id,
            &Err(bamboo_agent_core::AgentError::Cancelled),
        )
        .await
    );
    let guard = runners.read().await;
    let current = &guard[ROOT];
    assert_eq!(current.run_id, successor.run_id);
    assert!(matches!(
        current.status,
        super::super::runner_state::AgentStatus::Running
    ));
    assert!(current.event_publication.publish(|| {}));
    assert_eq!(
        current
            .visible_messages
            .subscribe_with_snapshot()
            .1
            .terminal,
        None
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_root_authority_before_account_receipt_still_interrupts_live_transport() {
    let fixture = Fixture::new().await;
    let binding = fixture.bind(fixture.a.clone(), "queued-started").await;
    let claim = JournalClaim::acquire(fixture.sink.events_dir());
    let mut live = fixture.sink.subscribe();
    let mut forwarder = Forwarder::new(&fixture, fixture.a.clone(), &binding, "queued-started");
    // The per-session callback has run, but the account receipt is blocked by
    // the real journal claim. Its physical Root guards are already released.
    assert!(matches!(
        session_event(&mut forwarder.session).await,
        AgentEvent::ExecutionStarted { .. }
    ));
    let (cancel, visible) = {
        let guard = forwarder.runners.read().await;
        let runner = guard.get(ROOT).unwrap();
        (runner.cancel_token.clone(), runner.visible_messages.clone())
    };
    let (mut messages, _) = visible.subscribe_with_snapshot();
    fixture
        .a
        .finish_activation(
            &binding.owner.fence,
            Utc::now(),
            bamboo_domain::ActorActivationFinish::Failed,
        )
        .await
        .unwrap();
    drop(claim);
    assert_frame(
        &session_event(&mut forwarder.session).await,
        &AgentEvent::Error {
            message: ROOT_AUTHORITY_LOST.into(),
        },
    );
    tokio::time::timeout(WAIT, forwarder.task)
        .await
        .unwrap()
        .unwrap();
    assert!(cancel.is_cancelled());
    assert!(matches!(
        messages.recv().await.unwrap().kind,
        super::super::visible_messages::VisibleMessageEventKind::Terminal { reason }
        if reason == "error"
    ));
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
    assert!(journal::read_since(fixture.sink.events_dir(), 0)
        .unwrap()
        .is_empty());
    assert!(
        !forwarder
            .history
            .send_and_wait(&forwarder.input, ROOT.into())
            .await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_root_authority_before_started_still_closes_subscribed_transport() {
    let fixture = Fixture::new().await;
    let binding = fixture.bind(fixture.a.clone(), "not-started").await;
    fixture
        .a
        .finish_activation(
            &binding.owner.fence,
            Utc::now(),
            bamboo_domain::ActorActivationFinish::Failed,
        )
        .await
        .unwrap();
    let mut forwarder = Forwarder::new(&fixture, fixture.a.clone(), &binding, "not-started");
    assert_frame(
        &session_event(&mut forwarder.session).await,
        &AgentEvent::Error {
            message: ROOT_AUTHORITY_LOST.into(),
        },
    );
    tokio::time::timeout(WAIT, forwarder.task)
        .await
        .unwrap()
        .unwrap();
    assert!(forwarder.runners.read().await[ROOT]
        .cancel_token
        .is_cancelled());
    assert!(journal::read_since(fixture.sink.events_dir(), 0)
        .unwrap()
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mismatched_root_forwarder_handoff_rejects_started_before_all_publication() {
    let fixture = Fixture::new().await;
    let binding = fixture.bind(fixture.a.clone(), "actual-run").await;
    let mut live = fixture.sink.subscribe();
    let mut forwarder = Forwarder::new(&fixture, fixture.a.clone(), &binding, "different-run");
    tokio::time::timeout(WAIT, forwarder.task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        forwarder.session.try_recv(),
        Err(TryRecvError::Empty)
    ));
    assert!(matches!(
        forwarder.legacy.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
    assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));
    assert!(journal::read_since(fixture.sink.events_dir(), 0)
        .unwrap()
        .is_empty());
    let runners = forwarder.runners.read().await;
    assert!(runners.get(ROOT).unwrap().last_critical_events.is_empty());
}
