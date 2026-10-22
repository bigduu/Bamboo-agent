//! Per-request native confirmation, fresh canonical checks, then exactly one CAS.
use super::{
    canonical::{Refused, ScopeStore},
    confirmation::{self, ConfirmationRequest, Decision},
    provider::ConfirmationProvider,
    state::{self, Confirmation, Reply, Session},
    wire::{AuthorityProfile, Outcome},
};
use bamboo_domain::SupervisorManagementRequest;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Closing and the submission boundary are serialized. The reader closes this
/// gate on EOF/protocol failure; an approval observed later cannot submit.
#[derive(Clone)]
pub(super) struct Connection {
    open: Arc<Mutex<bool>>,
    closed: CancellationToken,
}
impl Connection {
    pub(super) fn new() -> Self {
        Self {
            open: Arc::new(Mutex::new(true)),
            closed: CancellationToken::new(),
        }
    }
    pub(super) fn close(&self) {
        if let Ok(mut open) = self.open.lock() {
            *open = false;
        }
        self.closed.cancel();
    }
    pub(super) fn is_open(&self) -> bool {
        self.open.lock().is_ok_and(|open| *open)
    }
    pub(super) async fn closed(&self) {
        self.closed.cancelled().await;
    }
    fn while_open<T>(&self, action: impl FnOnce() -> T) -> Option<T> {
        let open = self.open.lock().ok()?;
        if !*open {
            return None;
        }
        Some(action())
    }
    async fn before_submission<F: Future>(&self, future: F) -> Option<F::Output> {
        tokio::select! {biased; _=self.closed()=>None, value=future=>Some(value)}
    }
}

pub(super) struct Processed {
    pub(super) outcome: Outcome,
    reply: Option<Reply<SupervisorManagementRequest>>,
}
pub(super) struct Adapter<S, P> {
    launch_id: Uuid,
    session: Session<SupervisorManagementRequest>,
    store: S,
    provider: P,
}
impl<S: ScopeStore, P: ConfirmationProvider> Adapter<S, P> {
    pub(super) fn new(launch_id: Uuid, store: S, provider: P) -> Result<Self, state::Refusal> {
        Ok(Self {
            launch_id,
            session: Session::new(launch_id.as_u128())?,
            store,
            provider,
        })
    }
    pub(super) fn transport_ready(&self) -> bool {
        self.session.transport_ready()
    }
    pub(super) fn profile(&self) -> AuthorityProfile {
        self.provider.profile()
    }
    pub(super) fn acknowledge(&mut self, launch_id: Uuid) -> Result<(), state::Refusal> {
        self.session.acknowledge(launch_id.as_u128())
    }
    pub(super) fn disconnect(&mut self) -> state::Disconnect {
        self.session.disconnect()
    }
    pub(super) fn flushed(&mut self, processed: Processed) -> Result<(), state::Refusal> {
        if let Some(reply) = processed.reply {
            self.session.reply_flushed(reply)?;
        }
        Ok(())
    }
    fn refuse(
        &mut self,
        token: Confirmation<SupervisorManagementRequest>,
        refused: Refused,
    ) -> Option<Processed> {
        self.session.cancel_confirmation(token).ok()?;
        Some(Processed {
            outcome: refused.outcome(),
            reply: None,
        })
    }
    pub(super) async fn apply(
        &mut self,
        launch_id: Uuid,
        request_id: Uuid,
        intent: SupervisorManagementRequest,
        busy_on_arrival: bool,
        connection: &Connection,
    ) -> Option<Processed> {
        if busy_on_arrival {
            let refusal = self
                .session
                .refuse_busy(launch_id.as_u128(), request_id.as_u128());
            return refusal_outcome(refusal).map(|outcome| Processed {
                outcome,
                reply: None,
            });
        }
        let token = match self.session.reserve_confirmation(
            launch_id.as_u128(),
            request_id.as_u128(),
            intent,
        ) {
            Ok(token) => token,
            Err(refusal) => {
                return refusal_outcome(refusal).map(|outcome| Processed {
                    outcome,
                    reply: None,
                })
            }
        };
        let before = match connection
            .before_submission(self.store.prepare(token.intent()))
            .await?
        {
            Ok(context) => context,
            Err(error) => return self.refuse(token, error),
        };
        if let Some(code) = self.provider.profile().unavailable_code() {
            return self.refuse(token, Refused::new(code, 503));
        }
        let request = ConfirmationRequest {
            version: 1,
            launch_id: self.launch_id,
            request_id,
            challenge_id: Uuid::new_v4(),
            data_directory: before.data_directory.clone(),
            intent: token.intent().clone(),
            previous_projects: before.previous_projects.clone(),
            selected_project_names: before.selected_project_names.clone(),
        };
        if let Err(code) = request.preview() {
            return self.refuse(token, Refused::new(code, 503));
        }
        let approval = match connection
            .before_submission(self.provider.confirm(&request))
            .await?
        {
            Decision::Approved(approval) => approval,
            Decision::Refused(code) => return self.refuse(token, Refused::new(code, 503)),
            Decision::Cancelled(code) => {
                self.session.cancel_confirmation(token).ok()?;
                return Some(Processed {
                    outcome: confirmation::cancelled(code),
                    reply: None,
                });
            }
        };
        let after = match connection
            .before_submission(self.store.prepare(token.intent()))
            .await?
        {
            Ok(context) => context,
            Err(error) => return self.refuse(token, error),
        };
        if before != after {
            return self.refuse(
                token,
                Refused::new("native_confirmation_context_changed", 409),
            );
        }
        // This is the one-way issuance boundary. From here, drain the existing
        // canonical CAS even if transport closes; never cancel/replay it as UI work.
        let submission = connection
            .while_open(|| self.session.submit_confirmation(token, &approval))?
            .ok()?;
        let outcome = self.store.apply(&submission).await;
        let reply = self.session.reply_ready(submission).ok()?;
        Some(Processed {
            outcome,
            reply: Some(reply),
        })
    }
}
fn refusal_outcome(refusal: state::Refusal) -> Option<Outcome> {
    let (code, status_code) = match refusal {
        state::Refusal::Replay => ("native_request_replay", 409),
        state::Refusal::Capacity => ("native_channel_exhausted", 503),
        state::Refusal::Busy => ("native_request_busy", 409),
        state::Refusal::ConsentDenied => ("native_confirmation_unavailable", 403),
        _ => return None,
    };
    Some(Outcome::Refused { code, status_code })
}

#[cfg(test)]
mod tests {
    use super::super::{canonical::ScopeContext, confirmation::mock_approval, state::Submission};
    use super::*;
    use async_trait::async_trait;
    use bamboo_domain::{
        ProjectId, SupervisorManagementMutation, SupervisorManagementReceipt, SupervisorReference,
        DEFAULT_SUPERVISOR_SESSION_ID,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Clone)]
    struct Store {
        context: Arc<Mutex<ScopeContext>>,
        revision: Arc<Mutex<u64>>,
        writes: Arc<AtomicUsize>,
        close_on_apply: Option<Connection>,
    }
    impl Store {
        fn new() -> Self {
            Self {
                context: Arc::new(Mutex::new(ScopeContext {
                    data_directory: "/isolated/fixture".into(),
                    previous_projects: Default::default(),
                    selected_project_names: [(
                        ProjectId::parse("p-a").unwrap(),
                        "Project A".into(),
                    )]
                    .into(),
                })),
                revision: Arc::new(Mutex::new(4)),
                writes: Arc::new(AtomicUsize::new(0)),
                close_on_apply: None,
            }
        }
    }
    #[async_trait]
    impl ScopeStore for Store {
        async fn prepare(
            &self,
            intent: &SupervisorManagementRequest,
        ) -> Result<ScopeContext, Refused> {
            if *self.revision.lock().unwrap() != intent.expected_state_revision {
                return Err(Refused::new("scope_revision_conflict", 409));
            }
            Ok(self.context.lock().unwrap().clone())
        }
        async fn apply(&self, submission: &Submission<SupervisorManagementRequest>) -> Outcome {
            self.writes.fetch_add(1, Ordering::SeqCst);
            if let Some(connection) = &self.close_on_apply {
                connection.close();
            }
            Outcome::Committed {
                receipt: SupervisorManagementReceipt {
                    supervisor: submission.intent().supervisor.clone(),
                    state_revision: 5,
                    changed: true,
                    link_revision: None,
                },
            }
        }
    }
    struct Mock {
        calls: Arc<AtomicUsize>,
        mode: u8,
        store: Store,
        link: Connection,
    }
    #[async_trait]
    impl ConfirmationProvider for Mock {
        fn profile(&self) -> AuthorityProfile {
            AuthorityProfile::new((self.mode == 0).then_some("native_confirmation_no_gui"))
        }
        async fn confirm(&self, request: &ConfirmationRequest) -> Decision {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                1 => Decision::Cancelled("native_confirmation_cancelled"),
                2 => Decision::Cancelled("native_confirmation_window_closed"),
                3 => Decision::Cancelled("native_confirmation_timed_out"),
                4 => {
                    *self.store.revision.lock().unwrap() = 5;
                    mock_approval(request)
                }
                5 => {
                    self.link.close();
                    mock_approval(request)
                }
                6 => {
                    self.store
                        .context
                        .lock()
                        .unwrap()
                        .selected_project_names
                        .values_mut()
                        .next()
                        .unwrap()
                        .push('x');
                    mock_approval(request)
                }
                _ => mock_approval(request),
            }
        }
    }
    fn intent() -> SupervisorManagementRequest {
        SupervisorManagementRequest {
            supervisor: SupervisorReference {
                session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
                incarnation_id: Uuid::new_v4(),
            },
            expected_state_revision: 4,
            mutation: SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: [ProjectId::parse("p-a").unwrap()].into(),
            },
        }
    }
    fn setup(
        mode: u8,
    ) -> (
        Adapter<Store, Mock>,
        Store,
        Connection,
        Uuid,
        Arc<AtomicUsize>,
    ) {
        let launch = Uuid::new_v4();
        let store = Store::new();
        let link = Connection::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = Mock {
            calls: calls.clone(),
            mode,
            store: store.clone(),
            link: link.clone(),
        };
        let mut adapter = Adapter::new(launch, store.clone(), provider).unwrap();
        adapter.acknowledge(launch).unwrap();
        (adapter, store, link, launch, calls)
    }
    fn json(outcome: &Outcome) -> serde_json::Value {
        serde_json::to_value(outcome).unwrap()
    }
    #[tokio::test]
    async fn no_gui_and_all_user_cancel_variants_never_submit_or_restore_id() {
        for mode in 0..=3 {
            let (mut adapter, store, link, launch, calls) = setup(mode);
            let id = Uuid::new_v4();
            let intent = intent();
            let processed = adapter
                .apply(launch, id, intent.clone(), false, &link)
                .await
                .unwrap();
            let value = json(&processed.outcome);
            assert_eq!(store.writes.load(Ordering::SeqCst), 0);
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(mode != 0));
            if mode != 0 {
                assert_eq!(value["kind"], "cancelled");
                assert_eq!(value["not_submitted"], true);
            }
            adapter.flushed(processed).unwrap();
            let replay = adapter
                .apply(launch, id, intent, false, &link)
                .await
                .unwrap();
            assert_eq!(json(&replay.outcome)["code"], "native_request_replay");
            assert_eq!(store.writes.load(Ordering::SeqCst), 0);
        }
    }
    #[tokio::test]
    async fn exact_confirmed_intent_submits_once_and_reply_flush_releases_slot() {
        let (mut adapter, store, link, launch, calls) = setup(7);
        let id = Uuid::new_v4();
        let original = intent();
        let result = adapter
            .apply(launch, id, original.clone(), false, &link)
            .await
            .unwrap();
        assert_eq!(json(&result.outcome)["kind"], "committed");
        assert_eq!(store.writes.load(Ordering::SeqCst), 1);
        adapter.flushed(result).unwrap();
        assert_eq!(adapter.disconnect(), state::Disconnect::Idle);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn revision_or_presented_context_change_after_confirmation_never_submit() {
        for mode in [4, 6] {
            let (mut adapter, store, link, launch, calls) = setup(mode);
            let result = adapter
                .apply(launch, Uuid::new_v4(), intent(), false, &link)
                .await
                .unwrap();
            assert_eq!(json(&result.outcome)["status_code"], 409);
            assert_eq!(store.writes.load(Ordering::SeqCst), 0);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }
    #[tokio::test]
    async fn disconnect_before_submission_wins_over_a_late_approval() {
        let (mut adapter, store, link, launch, _) = setup(5);
        let id = Uuid::new_v4();
        assert!(adapter
            .apply(launch, id, intent(), false, &link)
            .await
            .is_none());
        assert_eq!(store.writes.load(Ordering::SeqCst), 0);
        assert_eq!(
            adapter.disconnect(),
            state::Disconnect::NotSubmitted {
                request_id: id.as_u128()
            }
        );
    }
    #[tokio::test]
    async fn issued_cas_is_drained_and_lost_reply_remains_unknown() {
        let (mut adapter, mut store, link, launch, _) = setup(7);
        store.close_on_apply = Some(link.clone());
        adapter.store = store.clone();
        let id = Uuid::new_v4();
        let result = adapter
            .apply(launch, id, intent(), false, &link)
            .await
            .unwrap();
        assert_eq!(store.writes.load(Ordering::SeqCst), 1);
        assert!(!link.is_open());
        assert_eq!(json(&result.outcome)["kind"], "committed");
        let unknown = state::Disconnect::OutcomeUnconfirmed {
            request_id: id.as_u128(),
        };
        assert_eq!(adapter.disconnect(), unknown);
        assert_eq!(adapter.disconnect(), unknown);
        assert!(adapter.flushed(result).is_err());
    }
    #[tokio::test]
    async fn queued_busy_arrival_never_prompts_even_after_active_reply_flush() {
        let (mut adapter, store, link, launch, calls) = setup(7);
        let id = Uuid::new_v4();
        let result = adapter
            .apply(launch, id, intent(), true, &link)
            .await
            .unwrap();
        assert_eq!(json(&result.outcome)["code"], "native_request_busy");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.writes.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn restart_or_missing_handshake_cannot_reuse_the_parent_channel() {
        let (mut adapter, store, link, _, calls) = setup(7);
        assert!(adapter
            .apply(Uuid::new_v4(), Uuid::new_v4(), intent(), false, &link)
            .await
            .is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.writes.load(Ordering::SeqCst), 0);
    }
}
