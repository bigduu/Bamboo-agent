//! Exercise the ordinary HTTP execute adapter's actual publication route.

use std::{fs::File, path::Path, sync::Arc, time::Duration};

use actix_web::web;
use bamboo_agent_core::{Message, Session};
use bamboo_domain::{ActorDirectoryPort, RootActorExecutionBinding};
use bamboo_engine::execution::{
    event_publication::EventPublication, reserve_runner_core, AgentRunner, AgentStatus,
    VisibleMessageEventKind, VisibleMessageStream,
};
use fs2::FileExt;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use super::{spawn_event_forwarder_with_root_actor, AgentEvent, AppState, HistoryCommitBarrier};

const ROOT: &str = "http-root-authority-loss";
const WAIT: Duration = Duration::from_secs(8);
const INTERRUPTION: &str = "This run was interrupted because its execution ownership was lost. Reload the conversation before retrying.";

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
    state: web::Data<AppState>,
    binding: RootActorExecutionBinding,
    run_id: String,
    sender: broadcast::Sender<AgentEvent>,
    receiver: broadcast::Receiver<AgentEvent>,
    cancel: CancellationToken,
    publication: Arc<EventPublication>,
    visible: Arc<VisibleMessageStream>,
    _directory: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(directory.path().to_path_buf()).await.unwrap());
        // Keep the real notification relay enabled, with no external delivery.
        {
            let mut config = state.config.write().await;
            config.notifications.desktop.enabled = Some(false);
            config.notifications.ntfy.enabled = false;
            config.notifications.bark.enabled = false;
            config.lifecycle_hooks.enabled = false;
        }
        let mut root = Session::new(ROOT, "controlled-model");
        root.add_message(Message::user("HTTP Root publication fixture"));
        state.storage.save_session(&root).await.unwrap();
        let sender = state.get_session_event_sender(ROOT).await;
        let receiver = sender.subscribe();
        reserve_runner_core(
            &state.agent_runners,
            &state.session_event_senders,
            ROOT,
            &sender,
        )
        .await;
        let (run_id, cancel, publication, visible) = {
            let mut runners = state.agent_runners.write().await;
            let runner = runners.get_mut(ROOT).unwrap();
            runner.status = AgentStatus::Running;
            (
                runner.run_id.clone(),
                runner.cancel_token.clone(),
                runner.event_publication.clone(),
                runner.visible_messages.clone(),
            )
        };
        let binding = state
            .agent
            .persistence()
            .bind_root_actor_execution(&root, &run_id)
            .await
            .unwrap()
            .expect("HTTP Root must obtain its real repository-issued capability");
        Self {
            state,
            binding,
            run_id,
            sender,
            receiver,
            cancel,
            publication,
            visible,
            _directory: directory,
        }
    }

    fn start(&self) -> (mpsc::Sender<AgentEvent>, HistoryCommitBarrier) {
        let (input, events) = mpsc::channel(8);
        let history = spawn_event_forwarder_with_root_actor(
            self.state.clone(),
            ROOT.into(),
            self.run_id.clone(),
            events,
            self.sender.clone(),
            None,
            Some(self.binding.owner.clone()),
        );
        (input, history)
    }

    async fn revoke(&self) {
        self.state
            .session_store
            .finish_activation(
                &self.binding.owner.fence,
                chrono::Utc::now(),
                bamboo_domain::ActorActivationFinish::Failed,
            )
            .await
            .unwrap();
    }

    async fn next(&mut self) -> AgentEvent {
        tokio::time::timeout(WAIT, self.receiver.recv())
            .await
            .expect("HTTP session transport must make progress")
            .unwrap()
    }

    fn persisted(&self) -> Vec<u8> {
        std::fs::read(
            self._directory
                .path()
                .join("sessions")
                .join(ROOT)
                .join("session.json"),
        )
        .unwrap()
    }

    async fn assert_interrupted(
        &mut self,
        input: &mpsc::Sender<AgentEvent>,
        history: &mut HistoryCommitBarrier,
        persisted: &[u8],
        account_sequence: u64,
    ) {
        assert!(matches!(self.next().await,
            AgentEvent::Error { message } if message == INTERRUPTION));
        // Wait for the actual asynchronous relay before checking that neither
        // the interruption nor its normal UI notification enters durable state.
        assert!(matches!(self.next().await,
            AgentEvent::Notification { category, body, .. }
            if category == "run_failed" && body == INTERRUPTION));
        tokio::time::timeout(WAIT, input.closed())
            .await
            .expect("rejected HTTP forwarder releases its input receiver");
        assert!(self.cancel.is_cancelled());
        assert!(!self
            .publication
            .publish(|| panic!("obsolete output was published")));
        let (_, snapshot) = self.visible.subscribe_with_snapshot();
        assert_eq!(snapshot.terminal.as_deref(), Some("error"));
        assert!(matches!(
            self.receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        assert_eq!(self.persisted(), persisted);
        assert_eq!(self.state.account_sink.latest_seq(), account_sequence);
        assert!(self.state.agent_runners.read().await[ROOT]
            .last_critical_events
            .is_empty());
        assert!(!history.send_and_wait(input, ROOT.into()).await);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_root_authority_loss_before_started_interrupts_only_live_transport() {
    let mut fixture = Fixture::new().await;
    fixture.revoke().await;
    let persisted = fixture.persisted();
    let (input, mut history) = fixture.start();
    fixture
        .assert_interrupted(&input, &mut history, &persisted, 0)
        .await;
}

async fn rejected_event_interrupts_http_run(event: AgentEvent) {
    let mut fixture = Fixture::new().await;
    let mut account = fixture.state.account_sink.subscribe();
    let (input, mut history) = fixture.start();
    assert!(matches!(
        fixture.next().await,
        AgentEvent::ExecutionStarted { .. }
    ));
    tokio::time::timeout(WAIT, account.recv())
        .await
        .unwrap()
        .unwrap();
    fixture.revoke().await;
    let persisted = fixture.persisted();
    let (mut visible_events, _) = fixture.visible.subscribe_with_snapshot();
    input.send(event).await.unwrap();
    fixture
        .assert_interrupted(&input, &mut history, &persisted, 1)
        .await;
    assert!(matches!(
        visible_events.recv().await.unwrap().kind,
        VisibleMessageEventKind::Terminal { reason } if reason == "error"
    ));
    assert!(account.try_recv().is_err());
    let journal =
        bamboo_engine::events::journal::read_since(fixture.state.account_sink.events_dir(), 0)
            .unwrap();
    assert_eq!(journal.len(), 1);
    assert!(matches!(
        journal[0].event,
        AgentEvent::ExecutionStarted { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_root_authority_loss_rejects_critical_state_before_replay() {
    rejected_event_interrupts_http_run(AgentEvent::NeedClarification {
        question: "rejected private content".into(),
        options: None,
        tool_call_id: Some("controlled-pause".into()),
        tool_name: Some("controlled-tool".into()),
        allow_custom: true,
        source: None,
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_root_authority_loss_rejects_terminal_success() {
    rejected_event_interrupts_http_run(AgentEvent::Complete {
        usage: Default::default(),
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_root_authority_loss_after_account_admission_preserves_successor() {
    let mut fixture = Fixture::new().await;
    let claim = JournalClaim::acquire(fixture.state.account_sink.events_dir());
    let (input, mut history) = fixture.start();
    assert!(matches!(
        fixture.next().await,
        AgentEvent::ExecutionStarted { .. }
    ));
    fixture.revoke().await;
    let persisted = fixture.persisted();
    let mut successor = AgentRunner::new();
    successor.event_sender = fixture.sender.clone();
    successor.status = AgentStatus::Running;
    let successor_id = successor.run_id.clone();
    let successor_cancel = successor.cancel_token.clone();
    let successor_publication = successor.event_publication.clone();
    let successor_visible = successor.visible_messages.clone();
    fixture
        .state
        .agent_runners
        .write()
        .await
        .insert(ROOT.into(), successor);
    fixture
        .sender
        .send(AgentEvent::ExecutionStarted {
            run_id: successor_id.clone(),
            session_id: ROOT.into(),
            started_at: chrono::Utc::now().to_rfc3339(),
        })
        .unwrap();
    drop(claim);
    tokio::time::timeout(WAIT, input.closed()).await.unwrap();
    assert!(fixture.cancel.is_cancelled());
    assert!(!fixture
        .publication
        .publish(|| panic!("obsolete output was published")));
    assert!(matches!(fixture.next().await,
        AgentEvent::ExecutionStarted { run_id, .. } if run_id == successor_id));
    assert!(fixture.receiver.try_recv().is_err());
    assert!(!successor_cancel.is_cancelled());
    assert!(successor_publication.publish(|| {}));
    assert_eq!(successor_visible.subscribe_with_snapshot().1.terminal, None);
    assert_eq!(
        fixture
            .visible
            .subscribe_with_snapshot()
            .1
            .terminal
            .as_deref(),
        Some("error")
    );
    assert_eq!(fixture.persisted(), persisted);
    assert_eq!(fixture.state.account_sink.latest_seq(), 0);
    assert!(
        bamboo_engine::events::journal::read_since(fixture.state.account_sink.events_dir(), 0,)
            .unwrap()
            .is_empty()
    );
    assert!(!history.send_and_wait(&input, ROOT.into()).await);
}
