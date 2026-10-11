//! Execution-private Child persistence, sharing the Host's existing write coordinator.
use super::*;
use bamboo_domain::{
    ActorActivationStatus, ActorDirectoryPort, ChildActorRuntimeSave, ChildActorRuntimeWrite,
    RootActorRuntimePublisher, RootInboxAdmission, SessionInboxConsumerId,
    SessionInboxLeaseRequest, SessionInboxOwnedClaim, SessionInboxPort,
};

struct PendingClaim {
    inbox: Arc<dyn SessionInboxPort>,
    target: String,
    claim: Option<SessionInboxOwnedClaim>,
}
impl Drop for PendingClaim {
    fn drop(&mut self) {
        let Some(claim) = self.claim.take() else {
            return;
        };
        let inbox = self.inbox.clone();
        let target = self.target.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = inbox.release_owned(&target, &claim).await;
            });
        }
    }
}

impl LockedSessionStore {
    /// Capture one already admitted Child's immutable execution capability.
    /// Reuses Host locks and never grants authority to the default writer.
    pub fn bind_child_actor_writer(
        &self,
        owner: ChildActorRuntimeWrite,
        directory: Arc<dyn ActorDirectoryPort>,
        publish: RootActorRuntimePublisher,
    ) -> std::io::Result<Self> {
        if !self.storage.supports_child_actor_runtime_write() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "storage does not support a Child Actor writer",
            ));
        }
        Ok(Self {
            storage: self.storage.clone(),
            inherited_child_wait: self.inherited_child_wait.clone(),
            root_actor_writer: None,
            child_actor_writer: Some((owner, directory, publish)),
            locks: self.locks.clone(),
            task_pair_transaction_lock: self.task_pair_transaction_lock.clone(),
        })
    }

    pub(super) async fn validate_child_runtime_read(
        &self,
        session_id: &str,
    ) -> std::io::Result<()> {
        let Some((owner, _, _)) = &self.child_actor_writer else {
            return Ok(());
        };
        let rejected = |reason: String| {
            std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                bamboo_domain::SessionAuthorityConflict(format!(
                    "Child Actor runtime read rejected: {reason}"
                )),
            )
        };
        if session_id != owner.fence.actor_id {
            return Err(rejected(
                "bound execution cannot load another Session".into(),
            ));
        }
        // The V2 proof is a pure read. ActorDirectory::validate_fence may
        // initialize an absent authority row before rejecting a stale fence;
        // an old reader must never create authority for a reborn Session.
        self.storage
            .publish_child_actor_runtime_event(owner, Box::new(|validate| validate()))
            .await
            .map_err(|error| rejected(error.to_string()))
    }

    pub(super) async fn validate_child_runtime_readback(
        &self,
        session_id: &str,
        session: Option<&Session>,
    ) -> std::io::Result<()> {
        let Some((owner, _, _)) = &self.child_actor_writer else {
            return Ok(());
        };
        if session.is_none_or(|session| {
            session.id != owner.fence.actor_id
                || session.kind != bamboo_domain::SessionKind::Child
                || !session.authority_identity.is_ordinary()
                || session.created_at != owner.created_at
                || session.parent_session_id.as_deref() != Some(owner.parent_session_id.as_str())
                || session.parent_created_at != Some(owner.parent_created_at)
                || session.root_session_id != owner.root_session_id
                || session.spawn_depth != owner.spawn_depth
                || session.project_id_meta() != owner.project_id
        }) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                bamboo_domain::SessionAuthorityConflict(
                    "Child Actor runtime readback birth or lineage changed".into(),
                ),
            ));
        }
        // A directory precheck alone does not cover the subsequent ordinary
        // storage read. Fences cannot revive after takeover, so checking again
        // after materializing this exact birth closes both read-side windows.
        self.validate_child_runtime_read(session_id).await
    }

    pub(super) async fn load_child_runtime_control_plane(
        &self,
        session_id: &str,
    ) -> std::io::Result<Option<Session>> {
        let Some((owner, _, _)) = &self.child_actor_writer else {
            return self.storage.load_runtime_control_plane(session_id).await;
        };
        // Task's existing shared-plan read is the one cross-Session exception.
        // It receives only the same canonical Root control plane it read before
        // Child binding; this does not widen any Task mutation authority.
        let task_root = session_id == owner.root_session_id;
        if session_id != owner.fence.actor_id && !task_root {
            return self
                .validate_child_runtime_read(session_id)
                .await
                .map(|_| None);
        }
        self.validate_child_runtime_read(&owner.fence.actor_id)
            .await?;
        let session = self.storage.load_runtime_control_plane(session_id).await?;
        if !task_root {
            self.validate_child_runtime_readback(session_id, session.as_ref())
                .await?;
        } else {
            if session.as_ref().is_none_or(|root| {
                root.id != owner.root_session_id
                    || root.kind != bamboo_domain::SessionKind::Root
                    || root.root_session_id != owner.root_session_id
                    || root.parent_session_id.is_some()
                    || root.spawn_depth != 0
                    || root.project_id_meta() != owner.project_id
                    || (owner.parent_session_id == owner.root_session_id
                        && root.created_at != owner.parent_created_at)
            }) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    bamboo_domain::SessionAuthorityConflict(
                        "Child shared Task Root control-plane identity changed".into(),
                    ),
                ));
            }
            // This also validates every recorded ancestor birth, including a
            // non-direct Root, after the shared control-plane read completes.
            self.validate_child_runtime_read(&owner.fence.actor_id)
                .await?;
        }
        Ok(session)
    }

    pub(super) async fn save_child_snapshot(
        &self,
        session: &mut Session,
        runtime_only: bool,
        input: Option<(Arc<dyn SessionInboxPort>, SessionInboxOwnedClaim)>,
        finalized_wait: Option<&bamboo_domain::session::runtime_state::WaitingForChildrenState>,
    ) -> std::io::Result<()> {
        let (owner, _, publish) = self
            .child_actor_writer
            .as_ref()
            .ok_or_else(|| std::io::Error::other("Child writer missing"))?;
        self.storage
            .save_child_actor_runtime(ChildActorRuntimeSave {
                owner,
                inherited: self.inherited_for_session(session),
                session,
                runtime_only,
                finalized_wait,
                input,
                publish: publish.clone(),
            })
            .await
    }

    async fn child_input_lease(
        directory: &dyn ActorDirectoryPort,
        owner: &ChildActorRuntimeWrite,
        consumer: &SessionInboxConsumerId,
    ) -> std::io::Result<SessionInboxLeaseRequest> {
        let activation = directory
            .inspect_actor(&owner.fence.actor_id)
            .await
            .map_err(std::io::Error::other)?
            .activation
            .ok_or_else(|| std::io::Error::other("Child activation missing"))?;
        let now = chrono::Utc::now();
        if activation.fence() != owner.fence
            || activation.status != ActorActivationStatus::Running
            || activation.lease_expires_at <= now
        {
            return Err(std::io::Error::other(
                "Child Inbox owner is stale or expired",
            ));
        }
        Ok(SessionInboxLeaseRequest {
            consumer: consumer.clone(),
            now,
            duration: (activation.lease_expires_at - now).min(chrono::Duration::seconds(5)),
        })
    }

    pub(super) async fn admit_owned_child_inbox(
        &self,
        session: &mut Session,
        inbox: Arc<dyn SessionInboxPort>,
        active_run_id: Option<&str>,
    ) -> std::io::Result<Option<RootInboxAdmission>> {
        let Some((owner, directory, _)) = &self.child_actor_writer else {
            return Ok(None);
        };
        if session.id != owner.fence.actor_id || active_run_id != Some(owner.fence.run_id.as_str())
        {
            return Err(std::io::Error::other(
                "Child Inbox execution identity changed",
            ));
        }
        // Unsupported or foreign queues fail before any claim or format upgrade.
        let inbox = self.storage.bind_child_actor_inbox(owner, inbox)?;
        let consumer = SessionInboxConsumerId::new();
        let mut admission = RootInboxAdmission::default();
        for _ in 0..128 {
            match self
                .admit_one_child_input(
                    session,
                    inbox.clone(),
                    active_run_id,
                    directory.as_ref(),
                    owner,
                    &consumer,
                )
                .await
            {
                Ok(None) => break,
                Ok(Some(None)) => {}
                Ok(Some(Some(message))) => {
                    admission.merged += 1;
                    admission.committed_messages.push(message);
                }
                Err(error) => {
                    admission.admission_error = Some(error.to_string());
                    break;
                }
            }
        }
        Ok(Some(admission))
    }

    async fn admit_one_child_input(
        &self,
        session: &mut Session,
        inbox: Arc<dyn SessionInboxPort>,
        active_run_id: Option<&str>,
        directory: &dyn ActorDirectoryPort,
        owner: &ChildActorRuntimeWrite,
        consumer: &SessionInboxConsumerId,
    ) -> std::io::Result<Option<Option<bamboo_domain::Message>>> {
        let request = Self::child_input_lease(directory, owner, consumer).await?;
        let Some(claim) = inbox
            .claim_owned(&session.id, 1, active_run_id, &request)
            .await
            .map_err(std::io::Error::other)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let mut pending = PendingClaim {
            inbox: inbox.clone(),
            target: session.id.clone(),
            claim: Some(claim),
        };
        let request = Self::child_input_lease(directory, owner, consumer).await?;
        let claim = inbox
            .renew_owned(
                &session.id,
                pending.claim.as_ref().expect("pending Child input"),
                &request,
            )
            .await
            .map_err(std::io::Error::other)?;
        pending.claim = Some(claim.clone());
        let envelope = &claim.claim.envelope;
        let matching = session
            .messages
            .iter()
            .any(|m| bamboo_domain::is_matching_session_message(m, envelope));
        if (session
            .messages
            .iter()
            .any(|m| m.id == envelope.id.as_str())
            || session
                .session_inbox_admission()
                .is_some_and(|cursor| cursor.contains(&envelope.id)))
            && !matching
        {
            return Err(std::io::Error::other(
                "Child Inbox id/cursor has no matching typed message",
            ));
        }
        let before = session.clone();
        if !matching {
            session.add_message(
                envelope
                    .to_provider_message()
                    .map_err(std::io::Error::other)?,
            );
        }
        session
            .session_inbox_admission_mut()
            .record(envelope.id.clone(), claim.claim.generation);
        session.updated_at = chrono::Utc::now();
        if let Err(error) = self.checkpoint_root_input(session, inbox, &claim).await {
            *session = before;
            return Err(error);
        }
        pending.claim = None;
        let message = if matching {
            None
        } else {
            Some(
                session
                    .messages
                    .iter()
                    .find(|m| bamboo_domain::is_matching_session_message(m, envelope))
                    .ok_or_else(|| std::io::Error::other("Child checkpoint lost typed input"))?
                    .clone(),
            )
        };
        Ok(Some(message))
    }
}
