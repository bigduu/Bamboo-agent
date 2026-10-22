//! Host-installed complete-send capability over the canonical Session Inbox.
//!
//! A caller observation identifies one original execution; it grants nothing.
//! A Host's cross-Root adapter must retain its explicit scope and both Session
//! lifetimes through durable admission. A boolean preflight is insufficient.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::ToolCtx;
use bamboo_domain::{SessionMessageDeliveryIntent, SessionMessageId, Storage};
use bamboo_engine::{SessionMessenger, SessionMessengerError, SessionPeerDeliveryReceipt};

#[derive(Debug, Clone)]
pub struct ScopedSessionSendRequest {
    pub target_session_id: String,
    pub operation_id: String,
    pub message: String,
    /// None retains legacy intent only on the supported same-tree route.
    pub delivery_intent: Option<SessionMessageDeliveryIntent>,
}

impl ScopedSessionSendRequest {
    pub fn validate(&self) -> Result<(), ScopedSessionSendError> {
        if bamboo_domain::validate_supervisor_target_id(&self.target_session_id).is_err()
            || SessionMessageId::parse(&self.operation_id).is_err()
            || self.message.trim().is_empty()
            || self.message.len() > bamboo_engine::session_messaging::MAX_PEER_MESSAGE_BYTES
            || self.message.contains('\0')
        {
            return Err(ScopedSessionSendError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ScopedSessionSendError {
    InvalidRequest,
    AuthorityUnavailable,
    OriginalCallerChanged,
    PlanReadOnly,
    UnsupportedDeliveryIntent,
    /// No receipt is available to attest whether publication happened. This
    /// is neither admitted=false nor permission to choose a new operation ID.
    OutcomeUncertain,
    /// Activation/eligibility errors retain the canonical durable receipt.
    Delivery(SessionMessengerError),
}

impl From<SessionMessengerError> for ScopedSessionSendError {
    fn from(error: SessionMessengerError) -> Self {
        Self::Delivery(error)
    }
}

/// Implementations perform the complete operation, using the original caller
/// from ToolCtx and a trusted Host grant checked at the final Inbox boundary.
/// Model-supplied Project IDs, discovery observations, being managed by a
/// Supervisor and knowledge of its ID never grant cross-Root sending rights.
/// Unsupported/denied adapters must not fall back to another sender identity.
#[async_trait]
pub trait ScopedSessionSendPort: Send + Sync {
    async fn send(
        &self,
        original_ctx: &ToolCtx,
        request: ScopedSessionSendRequest,
    ) -> Result<SessionPeerDeliveryReceipt, ScopedSessionSendError>;
}

/// Prefer an explicitly installed Host scope for every caller role. Only a
/// never-installed namespace uses the existing same-tree or original Supervisor
/// followup writer. This composition never issues or widens a Host grant.
pub struct CanonicalSessionSendAdapter {
    storage: Arc<dyn Storage>,
    messenger: Arc<SessionMessenger>,
}

impl CanonicalSessionSendAdapter {
    pub fn new(storage: Arc<dyn Storage>, messenger: Arc<SessionMessenger>) -> Self {
        Self { storage, messenger }
    }
}

#[async_trait]
impl ScopedSessionSendPort for CanonicalSessionSendAdapter {
    async fn send(
        &self,
        ctx: &ToolCtx,
        request: ScopedSessionSendRequest,
    ) -> Result<SessionPeerDeliveryReceipt, ScopedSessionSendError> {
        request.validate()?;
        if ctx.plan_read_only {
            return Err(ScopedSessionSendError::PlanReadOnly);
        }
        let id = ctx
            .session_id()
            .ok_or(ScopedSessionSendError::AuthorityUnavailable)?;
        let original = ctx
            .executing_caller_for(id)
            .ok_or(ScopedSessionSendError::AuthorityUnavailable)?;
        // The installed Host namespace is selected before role compatibility.
        // A stale/revoked/unsupported namespace is an explicit refusal. Only
        // never-installed None can use the existing Supervisor/same-tree route.
        let token = self
            .storage
            .find_host_session_scope(original)
            .await
            .map_err(|_| ScopedSessionSendError::AuthorityUnavailable)?;
        if let Some(token) = token {
            let intent = request
                .delivery_intent
                .ok_or(ScopedSessionSendError::UnsupportedDeliveryIntent)?;
            return self
                .messenger
                .send_host_scoped_peer(
                    &token,
                    original,
                    &request.target_session_id,
                    &request.operation_id,
                    &request.message,
                    intent,
                )
                .await
                .map_err(Into::into);
        }
        let canonical = self
            .storage
            .load_execution_admission_snapshot(id)
            .await
            .map_err(|_| ScopedSessionSendError::AuthorityUnavailable)?
            .ok_or(ScopedSessionSendError::AuthorityUnavailable)?;
        canonical
            .check_execution_compatibility()
            .map_err(|_| ScopedSessionSendError::AuthorityUnavailable)?;
        if !original.matches_session(&canonical) {
            return Err(ScopedSessionSendError::OriginalCallerChanged);
        }
        if let Some(incarnation) = canonical.supervisor_incarnation_id() {
            let supervisor = ctx
                .executing_supervisor_for(id)
                .ok_or(ScopedSessionSendError::AuthorityUnavailable)?
                .supervisor_reference();
            if supervisor.incarnation_id != incarnation {
                return Err(ScopedSessionSendError::OriginalCallerChanged);
            }
            // The existing followup writer binds RespectSpecificWait activation.
            // It has no QueueOnly port. Never reinterpret a requested queue-only
            // operation as a wake-up, including on an exact retry.
            if request.delivery_intent != Some(SessionMessageDeliveryIntent::TriggerTurn) {
                return Err(ScopedSessionSendError::UnsupportedDeliveryIntent);
            }
            self.messenger
                .supervisor_followup(
                    &supervisor,
                    &request.target_session_id,
                    &request.operation_id,
                    &request.message,
                )
                .await
                .map(Into::into)
                .map_err(Into::into)
        } else {
            if ctx.executing_supervisor.is_some() {
                return Err(ScopedSessionSendError::AuthorityUnavailable);
            }
            // The observed helper keeps the original caller, then reuses the
            // same envelope and final lifetime/tree/Project Inbox guard. It
            // cannot deliver across Roots merely because a Project matches.
            self.messenger
                .send_peer_observed(
                    original,
                    &request.target_session_id,
                    &request.operation_id,
                    &request.message,
                    request.delivery_intent,
                )
                .await
                .map_err(Into::into)
        }
    }
}
