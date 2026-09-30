//! Paces direct-parent clarification delivery and reconciles its durable
//! Child resolution into a parent terminal and one Child Inbox resume wake.

use std::{collections::HashMap, sync::Arc, time::Duration};

use bamboo_agent_core::storage::Storage;
use bamboo_domain::{
    ParentQuestion, ParentQuestionOutcome, ParentQuestionResolution, ProjectId, ProjectStatus,
    Session, SessionActivationPolicy, SessionKind, SessionMessageEnvelope, SessionMessageId,
    PARENT_QUESTION_REQUEST_KEY,
};
use chrono::{DateTime, Utc};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use super::parent_permission_request::LiveActorLineage;
use super::parent_question_outcome::{self as outcome, State};

const SCAN_INTERVAL: Duration = Duration::from_secs(5);
const MAX_CHILDREN_PER_TICK: usize = 16;
const RETRY_INTERVAL: chrono::Duration = chrono::Duration::seconds(5);

#[derive(Clone)]
pub(super) struct ParentQuestionCoordinator {
    store: Arc<bamboo_storage::SessionStoreV2>,
    sessions: bamboo_engine::SessionRepository,
    messenger: Arc<bamboo_engine::SessionMessenger>,
    projects: Arc<bamboo_projects::ProjectStore>,
    shutdown: CancellationToken,
}

impl ParentQuestionCoordinator {
    pub fn new(
        store: Arc<bamboo_storage::SessionStoreV2>,
        sessions: bamboo_engine::SessionRepository,
        messenger: Arc<bamboo_engine::SessionMessenger>,
        projects: Arc<bamboo_projects::ProjectStore>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            store,
            sessions,
            messenger,
            projects,
            shutdown,
        }
    }

    pub async fn resolve_answer(
        &self,
        caller_session_id: &str,
        request_id: &str,
        answer: &str,
    ) -> Result<bamboo_server_tools::ParentQuestionReplyReceipt, String> {
        use bamboo_server_tools::{ParentQuestionReplyReceipt, ParentRequestReplyState};
        let id = SessionMessageId::parse(request_id).map_err(|_| "Invalid ParentRequest id")?;
        let parent = self
            .store
            .load_session(caller_session_id)
            .await
            .map_err(|_| "Canonical parent Session is unavailable")?
            .ok_or("Canonical parent Session is unavailable")?;
        let envelope = request_envelope(&parent, &id)
            .ok_or("ParentQuestion is not in the direct parent Session")?;
        let question = ParentQuestion::from_envelope(&envelope)
            .ok_or("ParentQuestion canonical proof is invalid")?;
        if question.parent.session_id != caller_session_id {
            return Err("Only the canonical direct parent may answer this question".into());
        }
        let child = self
            .store
            .load_session(&question.child.session_id)
            .await
            .map_err(|_| "Canonical Child Session is unavailable")?
            .ok_or("Canonical Child Session is unavailable")?;
        if !matches!(
            self.question_lineage(&question).await,
            LiveActorLineage::Live(_)
        ) {
            return Err("ParentQuestion Actor lineage is unavailable or retired".into());
        }
        if child
            .metadata
            .get("runtime.parent_question.unavailable_at_deadline_v1")
            .is_some_and(|id| id == request_id)
        {
            return Err(
                "ParentQuestion Child is blocked after authority became unavailable".into(),
            );
        }
        if let Some(existing) = ParentQuestionResolution::from_child(&parent, &child, &id) {
            if existing.request != question {
                return Err("ParentQuestion terminal identity changed".into());
            }
            self.reconcile_after_reply(&child.id).await?;
            return match existing.outcome {
                ParentQuestionOutcome::Answer { text } => Ok(ParentQuestionReplyReceipt {
                    answer: text,
                    state: ParentRequestReplyState::AlreadyResolved,
                }),
                ParentQuestionOutcome::Expired => {
                    Err("ParentQuestion expired; Child is blocked awaiting input".into())
                }
            };
        }
        if ParentQuestion::inspect_direct_parent(&parent, &child, &id) != Some(question.clone())
            || !self.project_active(&question)
        {
            return Err("ParentQuestion direct-parent or Project fence changed".into());
        }
        question.validate_answer(answer).map_err(str::to_string)?;
        if Utc::now() >= question.deadline {
            let _ = outcome::expire(&self.sessions, &parent, &question).await;
            let _ = self.reconcile_after_reply(&child.id).await;
            return Err("ParentQuestion deadline elapsed".into());
        }
        let (state, recorded) =
            outcome::answer(&self.store, &self.sessions, &parent, &question, answer)
                .await
                .map_err(|_| "ParentQuestion answer CAS is unconfirmed")?;
        self.reconcile_after_reply(&child.id).await?;
        match state {
            State::Terminal(ParentQuestionResolution {
                outcome: ParentQuestionOutcome::Answer { text },
                ..
            }) => Ok(ParentQuestionReplyReceipt {
                answer: text,
                state: if recorded {
                    ParentRequestReplyState::Recorded
                } else {
                    ParentRequestReplyState::AlreadyResolved
                },
            }),
            State::Terminal(ParentQuestionResolution {
                outcome: ParentQuestionOutcome::Expired,
                ..
            }) => Err("ParentQuestion deadline won the resolution race".into()),
            State::Pending => Err("ParentQuestion terminal persistence is unconfirmed".into()),
        }
    }

    /// A model reply arrives inside the parent's already-deep tool/runner
    /// poll. Run its durable fanout as a fresh task so activation and the
    /// Child successor do not nest on that same Tokio worker stack. Awaiting
    /// retains the tool's confirmation/error boundary; a lost task is retried
    /// from the Child's durable resolution marker by the paced reconciler.
    async fn reconcile_after_reply(&self, child_id: &str) -> Result<ReconcileDisposition, String> {
        let this = self.clone();
        let child_id = child_id.to_owned();
        tokio::spawn(async move { this.reconcile_child(&child_id).await })
            .await
            .map_err(|_| "ParentQuestion fanout task stopped before confirmation".to_string())?
    }

    fn project_active(&self, question: &ParentQuestion) -> bool {
        match question.project_id.as_deref() {
            None => true,
            Some(id) => id
                .parse::<ProjectId>()
                .ok()
                .and_then(|id| self.projects.get(&id).ok())
                .is_some_and(|project| project.status == ProjectStatus::Active),
        }
    }

    async fn question_lineage(&self, question: &ParentQuestion) -> LiveActorLineage {
        match super::parent_permission_request::live_actor_lineage(
            &self.store,
            &question.child.session_id,
        )
        .await
        {
            LiveActorLineage::Live(lineage)
                if lineage.first().is_some_and(|child| {
                    child.actor_id == question.child.session_id
                        && child.session_created_at == question.child.created_at
                        && child.root_actor_id == question.root_session_id
                        && child.project_id == question.project_id
                }) && lineage.get(1).is_some_and(|parent| {
                    parent.actor_id == question.parent.session_id
                        && parent.session_created_at == question.parent.created_at
                }) =>
            {
                LiveActorLineage::Live(lineage)
            }
            LiveActorLineage::Retired => LiveActorLineage::Retired,
            _ => LiveActorLineage::Unavailable,
        }
    }

    /// One exact Child is loaded from canonical storage. The index and a
    /// callback, if added later, are scheduling hints only.
    pub async fn reconcile_child(&self, child_id: &str) -> Result<ReconcileDisposition, String> {
        let child = self
            .store
            .load_session(child_id)
            .await
            .map_err(|_| "Canonical Child Session is unavailable")?
            .ok_or("Canonical Child Session is unavailable")?;
        if child.kind != SessionKind::Child {
            return Ok(ReconcileDisposition::Done);
        }
        let Some(parent_id) = child.parent_session_id.as_deref() else {
            return Err("ParentQuestion Child has no direct parent".into());
        };
        let question: ParentQuestion = match child
            .metadata
            .get(PARENT_QUESTION_REQUEST_KEY)
            .and_then(|raw| serde_json::from_str(raw).ok())
        {
            Some(question) => question,
            None => return Ok(ReconcileDisposition::Done),
        };
        // A terminal Child no longer has an outbox obligation. A repeated
        // reply may read its retained resolution marker, but must not start a
        // second activation after the first fanout completed.
        if child.pending_question.is_none() {
            return Ok(ReconcileDisposition::Done);
        }
        let now = Utc::now();
        let parent = match self.store.load_session(parent_id).await {
            Ok(Some(parent)) => Some(parent),
            Ok(None) | Err(_) => None,
        };
        let lineage = self.question_lineage(&question).await;
        if parent.is_none()
            || !self.project_active(&question)
            || !matches!(lineage, LiveActorLineage::Live(_))
        {
            // A committed answer survives temporary storage/Project loss, but
            // a Retired ancestor cannot recover and cannot wake this Child.
            if ParentQuestionResolution::from_orphan_child(&child, &question.id).is_some_and(
                |resolution| {
                    resolution.request == question
                        && matches!(resolution.outcome, ParentQuestionOutcome::Answer { .. })
                },
            ) && lineage != LiveActorLineage::Retired
            {
                return Ok(ReconcileDisposition::RetryAt(now + RETRY_INTERVAL));
            }
            if now < question.deadline {
                return Ok(ReconcileDisposition::RetryAt(
                    (now + RETRY_INTERVAL).min(question.deadline),
                ));
            }
            if lineage == LiveActorLineage::Retired {
                outcome::block_retired_at_deadline(&self.sessions, &question)
                    .await
                    .map_err(|_| "ParentQuestion retired authority block is unconfirmed")?;
            } else {
                outcome::block_unavailable_at_deadline(&self.sessions, &question)
                    .await
                    .map_err(|_| "ParentQuestion unavailable authority block is unconfirmed")?;
            }
            return Ok(ReconcileDisposition::Done);
        }
        let parent = parent.expect("checked above");
        let mut terminal = ParentQuestionResolution::from_child(&parent, &child, &question.id);
        if terminal.is_none()
            && ParentQuestion::for_pending(&parent, &child) != Some(question.clone())
        {
            return Err("ParentQuestion Child pending proof changed".into());
        }
        if terminal.is_none() && Utc::now() >= question.deadline {
            let (state, _) = outcome::expire(&self.sessions, &parent, &question)
                .await
                .map_err(|_| "ParentQuestion expiration CAS is unconfirmed")?;
            terminal = match state {
                State::Terminal(resolution) => Some(resolution),
                State::Pending => None,
            };
        }
        let request = question.envelope();
        if request_envelope(&parent, &question.id).as_ref() != Some(&request) {
            // The stable Inbox id makes a lost ACK or crash after Child
            // checkpoint safe to retry. Parent-specific waits are interrupted
            // exactly as in forced permission review.
            let admission = self
                .messenger
                .admit_with_activation_intent(
                    request,
                    SessionActivationPolicy::InterruptSpecificWait,
                    None,
                )
                .await
                .map_err(|_| "ParentQuestion parent Inbox admission is unconfirmed")?;
            self.messenger
                .activate_prepared(&admission)
                .await
                .map_err(|_| "ParentQuestion parent activation is pending recovery")?;
            return Ok(ReconcileDisposition::RetryAt(Utc::now() + RETRY_INTERVAL));
        }
        let Some(resolution) = terminal else {
            return Ok(ReconcileDisposition::RetryAt(question.deadline));
        };
        match &resolution.outcome {
            ParentQuestionOutcome::Answer { .. } => {
                let wake = resolution
                    .answer_wake_envelope()
                    .ok_or("ParentQuestion Child wake is invalid")?;
                let admission = self
                    .messenger
                    .admit_with_activation_intent(
                        wake,
                        SessionActivationPolicy::InterruptSpecificWait,
                        None,
                    )
                    .await
                    .map_err(|_| "ParentQuestion Child wake admission is unconfirmed")?;
                outcome::append_terminal(&self.sessions, &resolution)
                    .await
                    .map_err(|_| "ParentQuestion parent terminal is unconfirmed")?;
                outcome::finish_answer(&self.sessions, &parent, &resolution)
                    .await
                    .map_err(|_| "ParentQuestion Child answer finalization is unconfirmed")?;
                self.messenger
                    .activate_prepared(&admission)
                    .await
                    .map_err(|_| {
                        "ParentQuestion Child activation is pending durable wake recovery"
                    })?;
            }
            ParentQuestionOutcome::Expired => {
                outcome::append_terminal(&self.sessions, &resolution)
                    .await
                    .map_err(|_| "ParentQuestion parent timeout terminal is unconfirmed")?;
                let wake = resolution
                    .expiry_parent_wake_envelope()
                    .ok_or("ParentQuestion parent timeout wake is invalid")?;
                let admission = self
                    .messenger
                    .admit_with_activation_intent(
                        wake,
                        SessionActivationPolicy::InterruptSpecificWait,
                        None,
                    )
                    .await
                    .map_err(|_| "ParentQuestion parent timeout wake admission is unconfirmed")?;
                outcome::finish_expired(&self.sessions, &parent, &resolution)
                    .await
                    .map_err(|_| "ParentQuestion Child blocked status is unconfirmed")?;
                self.messenger
                    .activate_prepared(&admission)
                    .await
                    .map_err(|_| {
                        "ParentQuestion parent timeout activation awaits durable wake recovery"
                    })?;
            }
        }
        Ok(ReconcileDisposition::Done)
    }

    pub fn spawn(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(SCAN_INTERVAL);
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut known_due: HashMap<String, (SessionMessageId, DateTime<Utc>)> = HashMap::new();
            loop {
                tokio::select! {
                    _ = this.shutdown.cancelled() => break,
                    _ = tick.tick() => {}
                }
                let now = Utc::now();
                let mut candidates = this
                    .store
                    .list_index_entries()
                    .await
                    .into_iter()
                    .filter(|entry| entry.kind == SessionKind::Child && entry.has_pending_question)
                    .map(|entry| entry.id)
                    .collect::<Vec<_>>();
                candidates.sort();
                for ids in candidates.chunks(MAX_CHILDREN_PER_TICK) {
                    if this.shutdown.is_cancelled() {
                        return;
                    }
                    for child_id in ids {
                        let child = match this.store.load_session(child_id).await {
                            Ok(Some(child)) => child,
                            _ => continue,
                        };
                        let Some(question) = child
                            .metadata
                            .get(PARENT_QUESTION_REQUEST_KEY)
                            .and_then(|raw| serde_json::from_str::<ParentQuestion>(raw).ok())
                        else {
                            continue;
                        };
                        if !child
                            .metadata
                            .contains_key(bamboo_domain::PARENT_QUESTION_RESOLUTION_KEY)
                            && known_due
                                .get(child_id)
                                .is_some_and(|(id, due)| id == &question.id && *due > now)
                        {
                            continue;
                        }
                        match this.reconcile_child(child_id).await {
                            Ok(ReconcileDisposition::Done) => {
                                known_due.remove(child_id);
                            }
                            Ok(ReconcileDisposition::RetryAt(due)) => {
                                known_due.insert(child_id.clone(), (question.id, due));
                            }
                            Err(error) => {
                                tracing::warn!(%child_id, %error,
                                    "ParentQuestion reconciliation unconfirmed; retrying");
                                known_due
                                    .insert(child_id.clone(), (question.id, now + RETRY_INTERVAL));
                            }
                        }
                    }
                    tokio::select! {
                        _ = this.shutdown.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReconcileDisposition {
    Done,
    RetryAt(DateTime<Utc>),
}

fn request_envelope(parent: &Session, id: &SessionMessageId) -> Option<SessionMessageEnvelope> {
    let mut found = None;
    for message in &parent.messages {
        if message.id != id.as_str() {
            continue;
        }
        if found.is_some() {
            return None;
        }
        let marker = message.metadata.as_ref()?.get("session_message")?;
        let envelope: SessionMessageEnvelope = serde_json::from_value(marker.clone()).ok()?;
        if ParentQuestion::from_envelope(&envelope).is_none()
            || !bamboo_domain::is_matching_session_message(message, &envelope)
            || envelope.to_provider_message().ok().is_none_or(|expected| {
                serde_json::to_value(message).ok() != serde_json::to_value(expected).ok()
            })
        {
            return None;
        }
        found = Some(envelope);
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bamboo_domain::{
        ActorDirectoryPort, FunctionCall, Message, PendingQuestionSource,
        SessionActivationDisposition, SessionActivationError, SessionActivationPort,
        SessionInboxPort, ToolCall,
    };

    struct Active;

    #[async_trait]
    impl SessionActivationPort for Active {
        async fn request_activation(
            &self,
            _: &str,
            _: u64,
        ) -> Result<SessionActivationDisposition, SessionActivationError> {
            Ok(SessionActivationDisposition::ActiveNotified)
        }
    }

    struct Fixture {
        home: tempfile::TempDir,
        store: Arc<bamboo_storage::SessionStoreV2>,
        inbox: Arc<bamboo_storage::FileSessionInbox>,
        coordinator: ParentQuestionCoordinator,
        question: ParentQuestion,
    }

    impl Fixture {
        async fn new(issued_at: DateTime<Utc>) -> Self {
            Self::with_options(issued_at, false, false).await
        }

        async fn with_missing_project(issued_at: DateTime<Utc>, missing_project: bool) -> Self {
            Self::with_options(issued_at, missing_project, false).await
        }

        async fn with_nested_parent(issued_at: DateTime<Utc>) -> Self {
            Self::with_options(issued_at, false, true).await
        }

        async fn with_options(
            issued_at: DateTime<Utc>,
            missing_project: bool,
            nested_parent: bool,
        ) -> Self {
            let home = tempfile::tempdir().unwrap();
            let store = Arc::new(
                bamboo_storage::SessionStoreV2::new(home.path().into())
                    .await
                    .unwrap(),
            );
            let mut parent = if nested_parent {
                let root = Session::new("ancestor-root", "test-model");
                store.save_session(&root).await.unwrap();
                Session::new_child_of("direct-parent", &root, "test-model", "Parent")
            } else {
                Session::new("direct-parent", "test-model")
            };
            if missing_project {
                parent.set_project_id_meta(ProjectId::new().to_string());
            }
            let mut child = Session::new_child_of("direct-child", &parent, "test-model", "Child");
            if let Some(project_id) = parent.project_id_meta() {
                child.set_project_id_meta(project_id);
            }
            child.messages.push(Message::assistant(
                "",
                Some(vec![ToolCall {
                    id: "question-call".into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "AskUserQuestion".into(),
                        arguments: "{}".into(),
                    },
                }]),
            ));
            child.messages.push(Message::tool_result_with_status(
                "question-call",
                "Clarification needed: Which option?",
                true,
            ));
            child.set_pending_question_with_source(
                "question-call".into(),
                "AskUserQuestion".into(),
                "Which option?".into(),
                vec!["A".into(), "B".into()],
                true,
                PendingQuestionSource::DirectParent,
            );
            child.metadata.insert(
                "runtime.suspend_reason".into(),
                "awaiting_clarification".into(),
            );
            let question = ParentQuestion::issue_at(&parent, &child, issued_at).unwrap();
            child.metadata.insert(
                PARENT_QUESTION_REQUEST_KEY.into(),
                serde_json::to_string(&question).unwrap(),
            );
            store.save_session(&parent).await.unwrap();
            store.save_session(&child).await.unwrap();
            let (inbox, coordinator) = Self::runtime(&home, store.clone());
            Self {
                home,
                store,
                inbox,
                coordinator,
                question,
            }
        }

        fn runtime(
            home: &tempfile::TempDir,
            store: Arc<bamboo_storage::SessionStoreV2>,
        ) -> (
            Arc<bamboo_storage::FileSessionInbox>,
            ParentQuestionCoordinator,
        ) {
            let sessions = bamboo_engine::SessionRepository::new(
                Default::default(),
                store.clone(),
                Arc::new(bamboo_storage::LockedSessionStore::new(store.clone())),
            );
            let inbox = Arc::new(bamboo_storage::FileSessionInbox::new(
                store.clone(),
                Default::default(),
            ));
            let messenger = Arc::new(bamboo_engine::SessionMessenger::new(
                store.clone(),
                inbox.clone(),
                Arc::new(Active),
            ));
            let coordinator = ParentQuestionCoordinator::new(
                store,
                sessions,
                messenger,
                Arc::new(bamboo_projects::ProjectStore::open(home.path()).unwrap()),
                CancellationToken::new(),
            );
            (inbox, coordinator)
        }

        async fn deliver_parent_request(&self) {
            let result = self
                .coordinator
                .reconcile_child("direct-child")
                .await
                .unwrap();
            assert!(matches!(result, ReconcileDisposition::RetryAt(_)));
            let claim = self
                .inbox
                .claim("direct-parent", 1)
                .await
                .unwrap()
                .remove(0);
            assert_eq!(claim.envelope.id, self.question.id);
            let mut parent = self
                .store
                .load_session("direct-parent")
                .await
                .unwrap()
                .unwrap();
            parent.add_message(claim.envelope.to_provider_message().unwrap());
            self.store.save_session(&parent).await.unwrap();
            self.inbox.ack("direct-parent", &claim).await.unwrap();
        }

        async fn commit_answer_before_expired_deadline(&self) -> Session {
            let mut parent = self
                .store
                .load_session("direct-parent")
                .await
                .unwrap()
                .unwrap();
            parent.add_message(self.question.envelope().to_provider_message().unwrap());
            self.store.save_session(&parent).await.unwrap();

            // Persist the exact Child state produced by an answer CAS that
            // committed just before the deadline. Its fanout is still pending.
            let mut child = self
                .store
                .load_session("direct-child")
                .await
                .unwrap()
                .unwrap();
            let resolution = ParentQuestionResolution::answered(
                &self.question,
                self.question.deadline - chrono::Duration::milliseconds(1),
                "A",
            )
            .unwrap();
            let paired = child
                .messages
                .iter_mut()
                .find(|message| message.id == self.question.tool_result_message_id)
                .unwrap();
            paired.content = "A".into();
            paired.tool_success = Some(true);
            child.metadata.insert(
                bamboo_domain::PARENT_QUESTION_RESOLUTION_KEY.into(),
                serde_json::to_string(&resolution).unwrap(),
            );
            self.store.save_session(&child).await.unwrap();
            assert_eq!(
                ParentQuestionResolution::from_orphan_child(&child, &self.question.id),
                Some(resolution)
            );
            parent
        }

        async fn assert_answer_fanout_once(&self) {
            assert_eq!(
                self.coordinator
                    .reconcile_child("direct-child")
                    .await
                    .unwrap(),
                ReconcileDisposition::Done
            );
            assert_eq!(
                self.coordinator
                    .reconcile_child("direct-child")
                    .await
                    .unwrap(),
                ReconcileDisposition::Done
            );
            let parent = self
                .store
                .load_session("direct-parent")
                .await
                .unwrap()
                .unwrap();
            let child = self
                .store
                .load_session("direct-child")
                .await
                .unwrap()
                .unwrap();
            let resolution =
                ParentQuestionResolution::from_child(&parent, &child, &self.question.id).unwrap();
            let terminal_id = resolution.terminal_envelope().unwrap().id;
            assert_eq!(
                parent
                    .messages
                    .iter()
                    .filter(|message| message.id == terminal_id.as_str())
                    .count(),
                1
            );
            assert!(child.pending_question.is_none());
            assert_eq!(
                child
                    .metadata
                    .get("clarification_resume_pending")
                    .map(String::as_str),
                Some("true")
            );
            let wake = self.inbox.claim("direct-child", 1).await.unwrap().remove(0);
            assert_eq!(
                wake.envelope.id,
                resolution.answer_wake_envelope().unwrap().id
            );
            self.inbox.ack("direct-child", &wake).await.unwrap();
            assert!(self
                .inbox
                .claim("direct-child", 1)
                .await
                .unwrap()
                .is_empty());
        }
    }

    #[tokio::test]
    async fn retired_direct_parent_cannot_answer_delivered_child_question() {
        let fixture = Fixture::new(Utc::now()).await;
        fixture.deliver_parent_request().await;
        fixture
            .store
            .retire_actor("direct-parent", Utc::now())
            .await
            .unwrap();
        assert!(fixture
            .coordinator
            .resolve_answer("direct-parent", fixture.question.id.as_str(), "A")
            .await
            .is_err());
        let child = fixture
            .store
            .load_session("direct-child")
            .await
            .unwrap()
            .unwrap();
        assert!(child.pending_question.is_some());
        assert!(
            ParentQuestionResolution::from_orphan_child(&child, &fixture.question.id).is_none(),
            "retired parent must not commit an answer behind an error response"
        );
    }

    #[tokio::test]
    async fn retired_root_blocks_grandchild_question_at_deadline_after_restart() {
        let fixture =
            Fixture::with_nested_parent(Utc::now() - chrono::Duration::seconds(241)).await;
        fixture
            .store
            .retire_actor("ancestor-root", Utc::now())
            .await
            .unwrap();
        let restarted_store = Arc::new(
            bamboo_storage::SessionStoreV2::new(fixture.home.path().into())
                .await
                .unwrap(),
        );
        let (restarted_inbox, restarted) = Fixture::runtime(&fixture.home, restarted_store.clone());
        assert_eq!(
            restarted.reconcile_child("direct-child").await.unwrap(),
            ReconcileDisposition::Done
        );
        let grandchild = restarted_store
            .load_session("direct-child")
            .await
            .unwrap()
            .unwrap();
        assert!(grandchild.pending_question.is_none());
        assert_eq!(
            grandchild
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("blocked_needs_input")
        );
        let parent = restarted_store
            .load_session("direct-parent")
            .await
            .unwrap()
            .unwrap();
        assert!(request_envelope(&parent, &fixture.question.id).is_none());
        assert!(restarted_inbox
            .claim("direct-parent", 1)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn retired_root_closes_committed_grandchild_answer_without_wake_after_restart() {
        let fixture =
            Fixture::with_nested_parent(Utc::now() - chrono::Duration::seconds(241)).await;
        fixture.commit_answer_before_expired_deadline().await;
        fixture
            .store
            .retire_actor("ancestor-root", Utc::now())
            .await
            .unwrap();
        let restarted_store = Arc::new(
            bamboo_storage::SessionStoreV2::new(fixture.home.path().into())
                .await
                .unwrap(),
        );
        let (restarted_inbox, restarted) = Fixture::runtime(&fixture.home, restarted_store.clone());
        assert_eq!(
            restarted.reconcile_child("direct-child").await.unwrap(),
            ReconcileDisposition::Done
        );
        let grandchild = restarted_store
            .load_session("direct-child")
            .await
            .unwrap()
            .unwrap();
        assert!(grandchild.pending_question.is_none());
        assert_eq!(
            grandchild
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("blocked_needs_input")
        );
        assert_eq!(
            grandchild
                .metadata
                .get("runtime.parent_question.retired_at_deadline_v1")
                .map(String::as_str),
            Some(fixture.question.id.as_str())
        );
        assert!(matches!(
            ParentQuestionResolution::from_orphan_child(&grandchild, &fixture.question.id)
                .map(|resolution| resolution.outcome),
            Some(ParentQuestionOutcome::Answer { .. })
        ));
        assert!(restarted_inbox
            .claim("direct-child", 1)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn checkpoint_marker_replays_stable_parent_inbox_request_after_restart() {
        let fixture = Fixture::new(Utc::now()).await;
        let request_id = fixture.question.id.clone();
        drop(fixture.coordinator);
        drop(fixture.inbox);
        drop(fixture.store);
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(fixture.home.path().into())
                .await
                .unwrap(),
        );
        let (inbox, coordinator) = Fixture::runtime(&fixture.home, store);
        assert!(matches!(
            coordinator.reconcile_child("direct-child").await.unwrap(),
            ReconcileDisposition::RetryAt(_)
        ));
        let claim = inbox.claim("direct-parent", 1).await.unwrap().remove(0);
        assert_eq!(claim.envelope.id, request_id);
        assert_eq!(
            ParentQuestion::from_envelope(&claim.envelope).unwrap().id,
            request_id
        );
    }

    #[tokio::test]
    async fn answered_child_cas_replays_parent_terminal_and_child_wake_after_restart() {
        let fixture = Fixture::new(Utc::now()).await;
        fixture.deliver_parent_request().await;
        let parent = fixture
            .store
            .load_session("direct-parent")
            .await
            .unwrap()
            .unwrap();
        let (state, recorded) = outcome::answer(
            &fixture.store,
            &fixture.coordinator.sessions,
            &parent,
            &fixture.question,
            "A",
        )
        .await
        .unwrap();
        assert!(recorded);
        assert!(matches!(state, State::Terminal(_)));
        drop(fixture.coordinator);
        drop(fixture.inbox);
        drop(fixture.store);
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(fixture.home.path().into())
                .await
                .unwrap(),
        );
        let (inbox, coordinator) = Fixture::runtime(&fixture.home, store.clone());
        assert_eq!(
            coordinator.reconcile_child("direct-child").await.unwrap(),
            ReconcileDisposition::Done
        );
        let child = store.load_session("direct-child").await.unwrap().unwrap();
        assert!(child.pending_question.is_none());
        assert!(child
            .messages
            .iter()
            .any(
                |message| message.tool_call_id.as_deref() == Some("question-call")
                    && message.content == "A"
            ));
        let parent = store.load_session("direct-parent").await.unwrap().unwrap();
        assert!(outcome::terminal_recorded(
            &parent,
            &ParentQuestionResolution::from_child(&parent, &child, &fixture.question.id).unwrap(),
        )
        .unwrap());
        let wake = inbox.claim("direct-child", 1).await.unwrap().remove(0);
        assert_eq!(wake.envelope.in_reply_to, Some(fixture.question.id.clone()));
        let repeated = coordinator
            .resolve_answer("direct-parent", fixture.question.id.as_str(), "B")
            .await
            .unwrap();
        assert_eq!(
            repeated.state,
            bamboo_server_tools::ParentRequestReplyState::AlreadyResolved
        );
        assert_eq!(repeated.answer, "A");
    }

    #[tokio::test]
    async fn expired_question_blocks_child_and_rejects_late_answer() {
        let fixture = Fixture::new(Utc::now() - chrono::Duration::seconds(241)).await;
        fixture.deliver_parent_request().await;
        assert_eq!(
            fixture
                .coordinator
                .reconcile_child("direct-child")
                .await
                .unwrap(),
            ReconcileDisposition::Done
        );
        let child = fixture
            .store
            .load_session("direct-child")
            .await
            .unwrap()
            .unwrap();
        assert!(child.pending_question.is_none());
        assert_eq!(
            child
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("blocked_needs_input")
        );
        assert!(fixture
            .coordinator
            .resolve_answer("direct-parent", fixture.question.id.as_str(), "A")
            .await
            .is_err());
        let wake = fixture
            .inbox
            .claim("direct-parent", 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            wake.activation_policy,
            SessionActivationPolicy::InterruptSpecificWait
        );
        assert_eq!(wake.envelope.in_reply_to, Some(fixture.question.id.clone()));
        assert_eq!(
            wake.envelope.id,
            ParentQuestionResolution::from_child(
                &fixture
                    .store
                    .load_session("direct-parent")
                    .await
                    .unwrap()
                    .unwrap(),
                &child,
                &fixture.question.id,
            )
            .unwrap()
            .expiry_parent_wake_envelope()
            .unwrap()
            .id
        );
    }

    #[tokio::test]
    async fn lost_parent_at_deadline_blocks_child_without_forging_parent_transcript() {
        let fixture = Fixture::new(Utc::now() - chrono::Duration::seconds(241)).await;
        let parent = fixture
            .store
            .load_session("direct-parent")
            .await
            .unwrap()
            .unwrap();
        let parent_file = fixture
            .home
            .path()
            .join("sessions/direct-parent/session.json");
        tokio::fs::remove_file(parent_file).await.unwrap();
        assert_eq!(
            fixture
                .coordinator
                .reconcile_child("direct-child")
                .await
                .unwrap(),
            ReconcileDisposition::Done
        );
        let child = fixture
            .store
            .load_session("direct-child")
            .await
            .unwrap()
            .unwrap();
        assert!(child.pending_question.is_none());
        assert_eq!(
            child
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("blocked_needs_input")
        );
        assert_eq!(
            child
                .metadata
                .get("runtime.parent_question.unavailable_at_deadline_v1")
                .map(String::as_str),
            Some(fixture.question.id.as_str())
        );
        assert!(matches!(
            ParentQuestionResolution::from_orphan_child(&child, &fixture.question.id)
                .map(|resolution| resolution.outcome),
            Some(ParentQuestionOutcome::Expired)
        ));
        fixture.store.save_session(&parent).await.unwrap();
        assert!(fixture
            .coordinator
            .resolve_answer("direct-parent", fixture.question.id.as_str(), "A",)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn missing_project_at_deadline_blocks_child_without_parent_delivery() {
        let fixture =
            Fixture::with_missing_project(Utc::now() - chrono::Duration::seconds(241), true).await;
        assert_eq!(
            fixture
                .coordinator
                .reconcile_child("direct-child")
                .await
                .unwrap(),
            ReconcileDisposition::Done
        );
        let child = fixture
            .store
            .load_session("direct-child")
            .await
            .unwrap()
            .unwrap();
        assert!(child.pending_question.is_none());
        assert_eq!(
            child
                .metadata
                .get("runtime.suspend_reason")
                .map(String::as_str),
            Some("blocked_needs_input")
        );
        let parent = fixture
            .store
            .load_session("direct-parent")
            .await
            .unwrap()
            .unwrap();
        assert!(request_envelope(&parent, &fixture.question.id).is_none());
    }

    #[tokio::test]
    async fn committed_answer_survives_lost_parent_after_deadline_and_fans_out_once() {
        let fixture = Fixture::new(Utc::now() - chrono::Duration::seconds(241)).await;
        let parent = fixture.commit_answer_before_expired_deadline().await;
        tokio::fs::remove_file(
            fixture
                .home
                .path()
                .join("sessions/direct-parent/session.json"),
        )
        .await
        .unwrap();

        assert!(matches!(
            fixture
                .coordinator
                .reconcile_child("direct-child")
                .await
                .unwrap(),
            ReconcileDisposition::RetryAt(_)
        ));
        assert!(outcome::block_unavailable_at_deadline(
            &fixture.coordinator.sessions,
            &fixture.question,
        )
        .await
        .is_err());
        let pending = fixture
            .store
            .load_session("direct-child")
            .await
            .unwrap()
            .unwrap();
        assert!(pending.pending_question.is_some());
        assert!(matches!(
            ParentQuestionResolution::from_orphan_child(&pending, &fixture.question.id)
                .map(|resolution| resolution.outcome),
            Some(ParentQuestionOutcome::Answer { .. })
        ));

        fixture.store.save_session(&parent).await.unwrap();
        fixture.assert_answer_fanout_once().await;
    }

    #[tokio::test]
    async fn committed_answer_survives_missing_project_after_deadline_and_fans_out_once() {
        let fixture =
            Fixture::with_missing_project(Utc::now() - chrono::Duration::seconds(241), true).await;
        fixture.commit_answer_before_expired_deadline().await;
        assert!(matches!(
            fixture
                .coordinator
                .reconcile_child("direct-child")
                .await
                .unwrap(),
            ReconcileDisposition::RetryAt(_)
        ));
        let pending = fixture
            .store
            .load_session("direct-child")
            .await
            .unwrap()
            .unwrap();
        assert!(pending.pending_question.is_some());
        assert!(pending
            .metadata
            .get("runtime.parent_question.unavailable_at_deadline_v1")
            .is_none());

        let project_id = fixture
            .question
            .project_id
            .as_ref()
            .unwrap()
            .parse()
            .unwrap();
        fixture
            .coordinator
            .projects
            .create_with_id(project_id, "Recovered Project", None)
            .unwrap();
        fixture.assert_answer_fanout_once().await;
    }

    #[tokio::test]
    async fn temporary_project_unavailability_retries_before_fixed_deadline() {
        let fixture = Fixture::with_missing_project(Utc::now(), true).await;
        let ReconcileDisposition::RetryAt(due) = fixture
            .coordinator
            .reconcile_child("direct-child")
            .await
            .unwrap()
        else {
            panic!("temporary Project unavailability must schedule a retry");
        };
        assert!(due < fixture.question.deadline);
        assert!(due <= Utc::now() + chrono::Duration::seconds(6));
        let parent = fixture
            .store
            .load_session("direct-parent")
            .await
            .unwrap()
            .unwrap();
        assert!(request_envelope(&parent, &fixture.question.id).is_none());
        let child = fixture
            .store
            .load_session("direct-child")
            .await
            .unwrap()
            .unwrap();
        assert!(child.pending_question.is_some());
    }
}
