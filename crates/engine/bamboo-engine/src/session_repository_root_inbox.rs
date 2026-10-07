//! A bounded provider-boundary consumer for ordinary Root inputs.
use super::*;
use bamboo_domain::{
    ActorActivationStatus, RootActorRuntimeWrite, RootInboxAdmission, SessionInboxConsumerId,
    SessionInboxLeaseRequest, SessionInboxOwnedClaim, SessionInboxPort,
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

async fn lease_request(
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

impl SessionRepository {
    pub(super) async fn admit_owned_root_inbox(
        &self,
        session: &mut Session,
        inbox: Arc<dyn SessionInboxPort>,
        active_run_id: Option<&str>,
    ) -> io::Result<Option<RootInboxAdmission>> {
        let Some((directory, owner)) = &self.root_actor_owner else {
            return Ok(None);
        };
        if session.id != owner.fence.actor_id {
            return Err(io::Error::other("Root Inbox execution target mismatch"));
        }
        // Probe/bind before claim changes this queue's durable protocol version.
        let inbox = self.storage.bind_root_actor_inbox(owner, inbox)?;
        let consumer = SessionInboxConsumerId::new();
        let mut admission = RootInboxAdmission::default();
        for _ in 0..128 {
            match self
                .admit_one_root_input(
                    session,
                    inbox.clone(),
                    active_run_id,
                    directory.as_ref(),
                    owner,
                    &consumer,
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
    async fn admit_one_root_input(
        &self,
        session: &mut Session,
        inbox: Arc<dyn SessionInboxPort>,
        active_run_id: Option<&str>,
        directory: &dyn bamboo_domain::ActorDirectoryPort,
        owner: &RootActorRuntimeWrite,
        consumer: &SessionInboxConsumerId,
    ) -> io::Result<InputAdmission> {
        let request = lease_request(directory, owner, consumer).await?;
        let Some(claim) = inbox
            .claim_owned(&session.id, 1, active_run_id, &request)
            .await
            .map_err(io::Error::other)?
            .into_iter()
            .next()
        else {
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
