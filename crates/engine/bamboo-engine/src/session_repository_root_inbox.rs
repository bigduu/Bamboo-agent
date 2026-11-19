//! A bounded provider-boundary consumer for ordinary Root inputs.
use super::*;
use bamboo_domain::{
    ActorActivationStatus, RootActorRuntimeWrite, RootInboxAdmission, SessionActivationPolicy,
    SessionInboxConsumerId, SessionInboxError, SessionInboxLeaseRequest, SessionInboxOwnedClaim,
    SessionInboxPort, SessionInboxReceipt, SessionRuntimeStartup,
};
use std::io;

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
                // One exact release, no retry and no consumer-failure report.
                // A lost/expired owner leaves the lease for normal recovery.
                if let Err(error) = inbox.release_owned(&target, &claim).await {
                    tracing::debug!(session_id = %target, %error, "Root Inbox claim remains recoverable");
                }
            });
        }
    }
}

pub(super) async fn lease_request(
    directory: &dyn bamboo_domain::ActorDirectoryPort,
    owner: &RootActorRuntimeWrite,
    consumer: &SessionInboxConsumerId,
) -> io::Result<SessionInboxLeaseRequest> {
    let current = directory
        .inspect_actor(&owner.fence.actor_id)
        .await
        .map_err(io::Error::other)?
        .activation
        .ok_or_else(|| io::Error::other("Root has no current activation"))?;
    let now = chrono::Utc::now();
    if current.fence() != owner.fence
        || current.status != ActorActivationStatus::Running
        || current.lease_expires_at <= now
    {
        return Err(io::Error::other("Root Inbox activation is lost or expired"));
    }
    Ok(SessionInboxLeaseRequest {
        consumer: consumer.clone(),
        now,
        duration: (current.lease_expires_at - now).min(chrono::Duration::seconds(5)),
    })
}

enum InputAdmission {
    Empty,
    Recovered,
    New(Box<bamboo_domain::Message>),
}

#[derive(Clone, Copy)]
enum RootInputSelection<'a> {
    Any,
    Bound(&'a RootInboxSelection),
    Work(&'a bamboo_domain::SessionMessageId),
    Runtime(&'a SessionInboxReceipt),
}

impl RootInputSelection<'_> {
    fn receipt(&self) -> Option<&SessionInboxReceipt> {
        match self {
            Self::Bound(selection) => Some(selection.receipt()),
            Self::Runtime(receipt) => Some(receipt),
            Self::Any | Self::Work(_) => None,
        }
    }
}

impl SessionRepository {
    pub(super) async fn admit_owned_root_inbox(
        &self,
        session: &mut Session,
        inbox: Arc<dyn SessionInboxPort>,
        active_run_id: Option<&str>,
        expected_work_input: Option<&bamboo_domain::SessionMessageId>,
    ) -> io::Result<Option<RootInboxAdmission>> {
        let Some((directory, owner)) = &self.root_actor_owner else {
            return Ok(None);
        };
        if session.id != owner.fence.actor_id
            || expected_work_input.is_some() && active_run_id != Some(owner.fence.run_id.as_str())
            || expected_work_input
                .zip(self.root_inbox_receipt.as_ref())
                .is_some_and(|(expected, selection)| expected != &selection.receipt().id)
        {
            return Err(io::Error::other("Root Inbox execution target mismatch"));
        }
        // Probe/bind before claim changes this queue's durable protocol version.
        let inbox = self.storage.bind_root_actor_inbox(owner, inbox)?;
        let consumer = SessionInboxConsumerId::new();
        let mut admission = RootInboxAdmission::default();
        let selection = match (self.root_inbox_receipt.as_ref(), expected_work_input) {
            (Some(selection), _) => RootInputSelection::Bound(selection),
            (None, Some(id)) => RootInputSelection::Work(id),
            (None, None) => RootInputSelection::Any,
        };
        let limit = if matches!(selection, RootInputSelection::Any) {
            128
        } else {
            1
        };
        for _ in 0..limit {
            match self
                .admit_one_root_input(
                    session,
                    inbox.clone(),
                    active_run_id,
                    directory.as_ref(),
                    owner,
                    &consumer,
                    selection,
                )
                .await
            {
                Ok(InputAdmission::Empty) => break,
                Ok(InputAdmission::Recovered) => {}
                Ok(InputAdmission::New(message)) => {
                    admission.merged += 1;
                    admission.committed_messages.push(*message);
                }
                Err(error) => {
                    admission.admission_error = Some(error.to_string());
                    break;
                }
            }
        }
        Ok(Some(admission))
    }
    pub(super) async fn admit_owned_root_runtime_startup(
        &self,
        session: &mut Session,
        inbox: Arc<dyn SessionInboxPort>,
        startup: &SessionRuntimeStartup,
        run_id: &str,
    ) -> io::Result<bamboo_domain::Message> {
        let (directory, owner) = self
            .root_actor_owner
            .as_ref()
            .ok_or_else(|| io::Error::other("Runtime Inbox startup requires an owned Root"))?;
        if self.root_inbox_receipt.is_some()
            || owner.fence.run_id != run_id
            || owner.fence.actor_id != session.id
            || startup.target_created_at != owner.created_at
            || session.created_at != owner.created_at
        {
            return Err(io::Error::other("Runtime Inbox startup target changed"));
        }
        let latest = self
            .storage
            .load_session(&session.id)
            .await?
            .ok_or_else(|| io::Error::other("Runtime Inbox startup target disappeared"))?;
        if latest.created_at != owner.created_at
            || session.check_conversation_generation(&latest).is_err()
            || latest.has_pending_question()
            || latest
                .metadata
                .contains_key(bamboo_domain::ONCE_RESPONSE_HANDOFF_KEY)
            || latest.has_runtime_wait()
        {
            return Err(io::Error::other(
                "Runtime Inbox startup target has a conflicting gate",
            ));
        }
        let inbox = self.storage.bind_root_actor_inbox(owner, inbox)?;
        let mut selected = latest;
        match self
            .admit_one_root_input(
                &mut selected,
                inbox,
                Some(run_id),
                directory.as_ref(),
                owner,
                &SessionInboxConsumerId::new(),
                RootInputSelection::Runtime(&startup.receipt),
            )
            .await?
        {
            InputAdmission::New(message) => {
                *session = selected;
                Ok(*message)
            }
            InputAdmission::Empty | InputAdmission::Recovered => Err(io::Error::other(
                "selected Runtime Inbox startup receipt is no longer pending",
            )),
        }
    }

    async fn admit_one_root_input(
        &self,
        session: &mut Session,
        inbox: Arc<dyn SessionInboxPort>,
        active_run_id: Option<&str>,
        directory: &dyn bamboo_domain::ActorDirectoryPort,
        owner: &RootActorRuntimeWrite,
        consumer: &SessionInboxConsumerId,
        selection: RootInputSelection<'_>,
    ) -> io::Result<InputAdmission> {
        let claim_once = async |request: &SessionInboxLeaseRequest| match selection.receipt() {
            Some(receipt) => {
                inbox
                    .claim_owned_receipt(&session.id, receipt, Some(&owner.fence.run_id), request)
                    .await
            }
            None => inbox
                .claim_owned(&session.id, 1, active_run_id, request)
                .await
                .map(|claims| claims.into_iter().next()),
        };
        let request = lease_request(directory, owner, consumer).await?;
        let claim = match claim_once(&request).await {
            // This error comes from before the queue was touched. Refresh the
            // request once, including a fresh check of the Root's authority.
            Err(SessionInboxError::ClaimLeaseExpired) => {
                let request = lease_request(directory, owner, consumer).await?;
                claim_once(&request).await
            }
            result => result,
        }
        .map_err(io::Error::other)?;
        let Some(claim) = claim else {
            return Ok(InputAdmission::Empty);
        };
        let mut pending = PendingClaim {
            inbox: inbox.clone(),
            target: session.id.clone(),
            claim: Some(claim),
        };
        let request = lease_request(directory, owner, consumer).await?;
        let claim = inbox
            .renew_owned(
                &session.id,
                pending.claim.as_ref().expect("pending claim"),
                &request,
            )
            .await
            .map_err(io::Error::other)?;
        pending.claim = Some(claim.clone());
        let envelope = &claim.claim.envelope;
        match selection {
            RootInputSelection::Work(expected) if &envelope.id != expected => {
                return Err(io::Error::other(
                    "Root input differs from the reserved Work dispatch",
                ));
            }
            RootInputSelection::Bound(RootInboxSelection::Human(_))
                if envelope.source != bamboo_domain::SessionMessageSource::User
                    || envelope.kind != bamboo_domain::SessionMessageKind::UserInput =>
            {
                return Err(io::Error::other(
                    "Selected Root receipt is not an ordinary User input",
                ));
            }
            RootInputSelection::Bound(RootInboxSelection::PacedLoop(_))
                if envelope.paced_loop_run_id() != Some(owner.fence.run_id.as_str()) =>
            {
                return Err(io::Error::other(
                    "Selected paced Loop receipt does not belong to this Root turn",
                ));
            }
            RootInputSelection::Runtime(_)
                if envelope.background_exec_completion_handle().is_none()
                    || claim.claim.activation_policy
                        != SessionActivationPolicy::RespectSpecificWait =>
            {
                return Err(io::Error::other(
                    "selected Runtime Inbox receipt is not an eligible command completion",
                ));
            }
            _ => {}
        }
        let matching = session
            .messages
            .iter()
            .any(|message| bamboo_domain::is_matching_session_message(message, envelope));
        if session
            .messages
            .iter()
            .any(|message| message.id == envelope.id.as_str())
            && !matching
        {
            return Err(io::Error::other(
                "Root Inbox stable id conflicts with typed transcript",
            ));
        }
        if session
            .session_inbox_admission()
            .is_some_and(|cursor| cursor.contains(&envelope.id))
            && !matching
        {
            return Err(io::Error::other(
                "Root Inbox cursor has no matching typed transcript",
            ));
        }
        let before = session.clone();
        if !matching {
            let message = envelope.to_provider_message().map_err(io::Error::other)?;
            session.add_message(message);
        }
        session
            .session_inbox_admission_mut()
            .record(envelope.id.clone(), claim.claim.generation);
        session.updated_at = chrono::Utc::now();
        // Same canonical Human UI engagement mark as the fallback admission
        // path in state_bridge: only interactive human input (plain
        // user/UserInput or the validated prompt-wrapped chat turn) stamps
        // it; peer/host dispatch never does. It joins the message in this
        // same atomic root-input checkpoint.
        if !matching && envelope.is_human_interactive_input() {
            let mark = bamboo_domain::HumanInputMark::user(envelope.created_at);
            match session.agent_runtime_state.as_mut() {
                Some(state) => state.set_last_human_input(mark),
                None => {
                    let mut state = bamboo_domain::AgentRuntimeState::new("");
                    state.set_last_human_input(mark);
                    session.agent_runtime_state = Some(state);
                }
            }
        }
        if let Err(error) = self
            .persistence
            .checkpoint_root_input(session, inbox.clone(), &claim)
            .await
        {
            *session = before;
            return Err(error);
        }
        pending.claim = None; // Main and exact ACK both confirmed.
        if !matching {
            let message = session
                .messages
                .iter()
                .find(|message| bamboo_domain::is_matching_session_message(message, envelope))
                .ok_or_else(|| io::Error::other("Root checkpoint lost its typed input"))?;
            return Ok(InputAdmission::New(Box::new(message.clone())));
        }
        Ok(InputAdmission::Recovered)
    }
}
