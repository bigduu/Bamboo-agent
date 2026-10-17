//! Internal logical-session messaging service.
//!
//! This module deliberately has no HTTP concepts. User transports, tools,
//! actor adapters, the SDK, and runtime coordinators all submit the same typed
//! envelope addressed by stable [`Session::id`](bamboo_domain::Session).

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bamboo_agent_core::storage::Storage;
use bamboo_domain::{
    ActorSession, Session, SessionActivationDisposition, SessionActivationError,
    SessionActivationPolicy, SessionActivationPort, SessionInboxError, SessionInboxPort,
    SessionInboxReceipt, SessionMessageBody, SessionMessageContent, SessionMessageDeliveryIntent,
    SessionMessageEnvelope, SessionMessageId, SessionMessageKind, SessionMessageSource,
    SupervisorReference,
};

/// A followup is one bounded peer instruction, not a context export or file transfer.
pub const MAX_SUPERVISOR_FOLLOWUP_BYTES: usize = 16 * 1024;

pub const MAX_PEER_MESSAGE_BYTES: usize = 16 * 1024;
pub const MAX_PEER_TARGETS: usize = 128;
pub const MAX_PEER_TARGET_RESULT_BYTES: usize = 32 * 1024;
const MAX_PEER_TARGET_TITLE_BYTES: usize = 256;
const MAX_PEER_TARGET_STATUS_BYTES: usize = 64;
const MAX_PEER_TARGET_SCAN: usize = 1024;

/// Logical discovery data only. A target observation is never delivery authority.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SessionPeerTarget {
    pub session_id: String,
    pub parent_session_id: Option<String>,
    pub kind: bamboo_domain::SessionKind,
    pub title: String,
    pub status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SessionPeerTargets {
    pub targets: Vec<SessionPeerTarget>,
    pub truncated: bool,
}

struct PeerTreePort<'a>(&'a dyn Storage);

#[async_trait::async_trait]
impl crate::session_app::child_session::owned_tree::OwnedTreePort for PeerTreePort<'_> {
    async fn load(
        &self,
        id: &str,
    ) -> Result<Session, crate::session_app::child_session::owned_tree::OwnedTreeError> {
        use crate::session_app::child_session::owned_tree::OwnedTreeError;
        bamboo_domain::validate_supervisor_target_id(id)
            .map_err(|_| OwnedTreeError::InvalidLineage)?;
        self.0
            .load_runtime_control_plane(id)
            .await
            .map_err(|_| OwnedTreeError::InvalidLineage)?
            .ok_or(OwnedTreeError::InvalidLineage)
    }

    async fn child_ids(
        &self,
        parent_id: &str,
    ) -> Result<Vec<String>, crate::session_app::child_session::owned_tree::OwnedTreeError> {
        use crate::session_app::child_session::owned_tree::OwnedTreeError;
        self.0
            .list_child_run_statuses(parent_id)
            .await
            .map(|children| children.into_iter().map(|(id, _)| id).collect())
            .map_err(|_| OwnedTreeError::InvalidLineage)
    }
}

/// Observable counters for the internal delivery plane.
#[derive(Debug, Default)]
pub struct SessionMessagingMetrics {
    delivered: AtomicU64,
    rejected: AtomicU64,
    invalid_envelope: AtomicU64,
    unauthorized: AtomicU64,
    payload_too_large: AtomicU64,
    backlog_full: AtomicU64,
    storage_failed: AtomicU64,
    activation_failed: AtomicU64,
    active_notified: AtomicU64,
    activation_reserved: AtomicU64,
    activation_coalesced: AtomicU64,
    delivery_latency_micros: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionMessagingMetricsSnapshot {
    pub delivered: u64,
    pub rejected: u64,
    pub invalid_envelope: u64,
    pub unauthorized: u64,
    pub payload_too_large: u64,
    pub backlog_full: u64,
    pub storage_failed: u64,
    pub activation_failed: u64,
    pub active_notified: u64,
    pub activation_reserved: u64,
    pub activation_coalesced: u64,
    pub delivery_latency_micros: u64,
}

impl SessionMessagingMetrics {
    pub fn snapshot(&self) -> SessionMessagingMetricsSnapshot {
        SessionMessagingMetricsSnapshot {
            delivered: self.delivered.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            invalid_envelope: self.invalid_envelope.load(Ordering::Relaxed),
            unauthorized: self.unauthorized.load(Ordering::Relaxed),
            payload_too_large: self.payload_too_large.load(Ordering::Relaxed),
            backlog_full: self.backlog_full.load(Ordering::Relaxed),
            storage_failed: self.storage_failed.load(Ordering::Relaxed),
            activation_failed: self.activation_failed.load(Ordering::Relaxed),
            active_notified: self.active_notified.load(Ordering::Relaxed),
            activation_reserved: self.activation_reserved.load(Ordering::Relaxed),
            activation_coalesced: self.activation_coalesced.load(Ordering::Relaxed),
            delivery_latency_micros: self.delivery_latency_micros.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMessengerReceipt {
    pub delivery: SessionInboxReceipt,
    pub activation: SessionActivationDisposition,
}

/// A durable peer delivery receipt. No field proves context consumption,
/// a terminal run, or an answer. None means activation was not requested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPeerDeliveryReceipt {
    pub delivery: SessionInboxReceipt,
    pub activation: Option<SessionActivationDisposition>,
    pub active_delivery_notified: bool,
}

impl From<SessionMessengerReceipt> for SessionPeerDeliveryReceipt {
    fn from(receipt: SessionMessengerReceipt) -> Self {
        Self {
            delivery: receipt.delivery,
            activation: Some(receipt.activation),
            active_delivery_notified: false,
        }
    }
}

impl SessionPeerDeliveryReceipt {
    fn into_legacy(self) -> SessionMessengerReceipt {
        SessionMessengerReceipt {
            delivery: self.delivery,
            activation: self
                .activation
                .expect("legacy peer delivery requests activation"),
        }
    }
}

/// Durable admission separated from activation so coordinators can commit
/// related control-plane state between those phases without opening a race.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMessengerAdmission {
    pub envelope_id: String,
    pub target_session_id: String,
    pub delivery: SessionInboxReceipt,
}

#[derive(Debug, thiserror::Error)]
pub enum SessionMessengerError {
    #[error("invalid session message: {0}")]
    InvalidEnvelope(String),
    #[error("session message source not found: {0}")]
    SourceNotFound(String),
    #[error("session message target not found: {0}")]
    TargetNotFound(String),
    #[error("session {source_session_id} is not authorized to message session {target}")]
    Unauthorized {
        source_session_id: String,
        target: String,
    },
    #[error(transparent)]
    Inbox(#[from] SessionInboxError),
    /// The enqueue is already durable when this error is returned. The receipt
    /// lets the caller distinguish it from a delivery failure and retry
    /// activation without inventing a second message id.
    #[error("message {receipt_id} was durably delivered but activation failed: {source}")]
    Activation {
        receipt_id: String,
        receipt: SessionInboxReceipt,
        #[source]
        source: SessionActivationError,
    },
    /// Staged delivery is durable, but its explicit activation watermark did
    /// not persist. Immediate delivery instead binds intent at publication.
    /// Startup cannot infer permission to run from admission alone; a caller
    /// must retry this exact envelope until the watermark is committed.
    #[error(
        "message {receipt_id} was delivered but activation eligibility was not persisted: {source}"
    )]
    ActivationEligibility {
        receipt_id: String,
        receipt: SessionInboxReceipt,
        #[source]
        source: SessionInboxError,
    },
    #[error("session store failure: {0}")]
    Storage(String),
}

/// Validates logical identity, atomically enqueues, then requests one runtime
/// activation. The service owns no process-global registry.
pub struct SessionMessenger {
    sessions: Arc<dyn Storage>,
    inbox: Arc<dyn SessionInboxPort>,
    activation: Arc<dyn SessionActivationPort>,
    metrics: Arc<SessionMessagingMetrics>,
}

impl SessionMessenger {
    pub fn new(
        sessions: Arc<dyn Storage>,
        inbox: Arc<dyn SessionInboxPort>,
        activation: Arc<dyn SessionActivationPort>,
    ) -> Self {
        Self {
            sessions,
            inbox,
            activation,
            metrics: Arc::new(SessionMessagingMetrics::default()),
        }
    }

    pub fn inbox(&self) -> &Arc<dyn SessionInboxPort> {
        &self.inbox
    }

    pub fn activation(&self) -> &Arc<dyn SessionActivationPort> {
        &self.activation
    }

    pub fn metrics(&self) -> &Arc<SessionMessagingMetrics> {
        &self.metrics
    }

    /// Discover bounded logical peers in the caller's current canonical tree.
    /// `caller_session_id` comes from the authenticated runtime context, never
    /// from model arguments. Every candidate is revalidated before disclosure.
    pub async fn list_peer_targets(
        &self,
        caller_session_id: &str,
    ) -> Result<SessionPeerTargets, SessionMessengerError> {
        let caller = self.peer_member(caller_session_id, true).await?;
        let mut pending = VecDeque::from([(caller.root_session_id.clone(), None)]);
        let mut seen = HashSet::new();
        let mut result = SessionPeerTargets {
            targets: Vec::new(),
            truncated: false,
        };
        // Account for JSON escaping, separators, and the complete wrapper,
        // rather than treating raw display-string lengths as an output bound.
        let mut result_bytes = serde_json::to_vec(&result)
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?
            .len();
        while let Some((id, indexed_parent)) = pending.pop_front() {
            if !seen.insert(id.clone()) {
                continue;
            }
            let current = match self.peer_member(&id, false).await {
                Ok(current)
                    if current.root_session_id == caller.root_session_id
                        && current.project_id_meta() == caller.project_id_meta()
                        && current.parent_session_id == indexed_parent =>
                {
                    current
                }
                Ok(_)
                | Err(SessionMessengerError::TargetNotFound(_))
                | Err(SessionMessengerError::Unauthorized { .. })
                | Err(SessionMessengerError::InvalidEnvelope(_)) => continue,
                Err(error) => return Err(error),
            };
            if id != caller_session_id {
                if result.targets.len() == MAX_PEER_TARGETS {
                    result.truncated = true;
                    break;
                }
                let target = SessionPeerTarget {
                    session_id: current.id.clone(),
                    parent_session_id: current.parent_session_id.clone(),
                    kind: current.kind,
                    title: Self::bounded_peer_label(&current.title, MAX_PEER_TARGET_TITLE_BYTES),
                    status: current.last_run_status().map(|status| {
                        Self::bounded_peer_label(&status, MAX_PEER_TARGET_STATUS_BYTES)
                    }),
                };
                let target_bytes = serde_json::to_vec(&target)
                    .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?
                    .len()
                    + usize::from(!result.targets.is_empty());
                if result_bytes.saturating_add(target_bytes) > MAX_PEER_TARGET_RESULT_BYTES {
                    result.truncated = true;
                    break;
                }
                result_bytes += target_bytes;
                result.targets.push(target);
            }
            let mut children = self
                .sessions
                .list_child_run_statuses(&id)
                .await
                .map_err(|error| SessionMessengerError::Storage(error.to_string()))?;
            children.sort_by(|left, right| left.0.cmp(&right.0));
            let budget = MAX_PEER_TARGET_SCAN.saturating_sub(seen.len() + pending.len());
            result.truncated |= children.len() > budget;
            pending.extend(
                children
                    .into_iter()
                    .take(budget)
                    .map(|(child, _)| (child, Some(id.clone()))),
            );
        }
        Ok(result)
    }

    /// Send an asynchronous peer instruction. Source identity and association
    /// fields are constructed by the runtime; callers provide no envelope.
    /// Reuse the same operation id and message after an uncertain result.
    pub async fn send_peer(
        &self,
        caller_session_id: &str,
        target_session_id: &str,
        operation_id: &str,
        message: &str,
    ) -> Result<SessionMessengerReceipt, SessionMessengerError> {
        self.send_peer_delivery(
            caller_session_id,
            target_session_id,
            operation_id,
            message,
            None,
        )
        .await
        .map(SessionPeerDeliveryReceipt::into_legacy)
    }

    /// Explicit scheduling over the same lifetime-fenced Inbox. Omission on
    /// the old send_peer API keeps legacy interrupt/activation behavior.
    pub async fn send_peer_with_intent(
        &self,
        caller_session_id: &str,
        target_session_id: &str,
        operation_id: &str,
        message: &str,
        intent: SessionMessageDeliveryIntent,
    ) -> Result<SessionPeerDeliveryReceipt, SessionMessengerError> {
        self.send_peer_delivery(
            caller_session_id,
            target_session_id,
            operation_id,
            message,
            Some(intent),
        )
        .await
    }

    /// Active-run-only peer input. The Inbox owns exact retry recovery and
    /// retains its original authority through the runtime's publication CAS.
    /// This never cancels a run, requests activation or reconstructs an owner.
    pub async fn steer_peer(
        &self,
        caller_session_id: &str,
        target_session_id: &str,
        operation_id: &str,
        message: &str,
        expected_run_id: &str,
    ) -> Result<bamboo_domain::SessionInboxSteerReceipt, SessionMessengerError> {
        Self::validate_peer_input(operation_id, message)?;
        if !bamboo_domain::valid_session_run_id(expected_run_id) {
            return Err(SessionMessengerError::InvalidEnvelope(
                "invalid expected run id".into(),
            ));
        }
        let (caller, target) = self.peer_pair(caller_session_id, target_session_id).await?;
        let id = Self::peer_message_id(&caller, &target, operation_id);
        let envelope = Self::peer_envelope(
            &caller,
            &target,
            operation_id,
            message,
            id.clone(),
            id.to_string(),
            None,
        );
        let source = ActorSession::from_session(&caller)
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?;
        let target = ActorSession::from_session(&target)
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?;
        let started = Instant::now();
        let receipt = self
            .inbox
            .deliver_peer_steer(
                &envelope,
                &source,
                &target,
                expected_run_id,
                self.activation.clone(),
            )
            .await
            .map_err(|error| {
                let error = SessionMessengerError::Inbox(error);
                self.record_rejection(&error);
                error
            })?;
        self.record_admission(envelope, receipt.delivery.clone(), started);
        Ok(receipt)
    }

    async fn send_peer_delivery(
        &self,
        caller_session_id: &str,
        target_session_id: &str,
        operation_id: &str,
        message: &str,
        intent: Option<SessionMessageDeliveryIntent>,
    ) -> Result<SessionPeerDeliveryReceipt, SessionMessengerError> {
        Self::validate_peer_input(operation_id, message)?;
        let (caller, target) = self.peer_pair(caller_session_id, target_session_id).await?;
        let id = Self::peer_message_id(&caller, &target, operation_id);
        let envelope = Self::peer_envelope(
            &caller,
            &target,
            operation_id,
            message,
            id.clone(),
            id.to_string(),
            None,
        );
        self.send_peer_envelope(envelope, &caller, &target, None, intent)
            .await
    }

    /// Reply only to a canonical received peer envelope in the caller's saved
    /// transcript. Recipient and thread edges are never supplied by the model.
    pub async fn reply_peer(
        &self,
        caller_session_id: &str,
        received_message_id: &str,
        operation_id: &str,
        message: &str,
    ) -> Result<SessionMessengerReceipt, SessionMessengerError> {
        self.reply_peer_delivery(
            caller_session_id,
            received_message_id,
            operation_id,
            message,
            None,
        )
        .await
        .map(SessionPeerDeliveryReceipt::into_legacy)
    }

    /// Derive recipient/thread from the canonical received peer message while
    /// opting into explicit scheduling. Reply observations never grant authority.
    pub async fn reply_peer_with_intent(
        &self,
        caller_session_id: &str,
        received_message_id: &str,
        operation_id: &str,
        message: &str,
        intent: SessionMessageDeliveryIntent,
    ) -> Result<SessionPeerDeliveryReceipt, SessionMessengerError> {
        self.reply_peer_delivery(
            caller_session_id,
            received_message_id,
            operation_id,
            message,
            Some(intent),
        )
        .await
    }

    async fn reply_peer_delivery(
        &self,
        caller_session_id: &str,
        received_message_id: &str,
        operation_id: &str,
        message: &str,
        intent: Option<SessionMessageDeliveryIntent>,
    ) -> Result<SessionPeerDeliveryReceipt, SessionMessengerError> {
        Self::validate_peer_input(operation_id, message)?;
        Self::validate_peer_session_id(caller_session_id)?;
        let received_id = SessionMessageId::parse(received_message_id)
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?;
        let received = self
            .load_session(caller_session_id)
            .await?
            .ok_or_else(|| SessionMessengerError::SourceNotFound(caller_session_id.into()))?;
        let original = received
            .messages
            .iter()
            .filter(|message| message.id == received_message_id)
            .find_map(|message| {
                let envelope: SessionMessageEnvelope = serde_json::from_value(
                    message.metadata.as_ref()?.get("session_message")?.clone(),
                )
                .ok()?;
                (envelope.id == received_id
                    && envelope.kind == SessionMessageKind::PeerMessage
                    && envelope.target_session_id == caller_session_id
                    && bamboo_domain::is_matching_session_message(message, &envelope))
                .then_some(envelope)
            })
            .ok_or_else(|| {
                SessionMessengerError::InvalidEnvelope(
                    "reply requires a canonical received peer message".into(),
                )
            })?;
        let SessionMessageSource::Session {
            session_id: target_id,
        } = &original.source
        else {
            return Err(SessionMessengerError::InvalidEnvelope(
                "reply has no logical sender".into(),
            ));
        };
        let (caller, target) = self.peer_pair(caller_session_id, target_id).await?;
        let thread = original.thread_id.clone().ok_or_else(|| {
            SessionMessengerError::InvalidEnvelope("reply requires a threaded peer message".into())
        })?;
        let original_operation = original.correlation_id.as_deref().ok_or_else(|| {
            SessionMessengerError::InvalidEnvelope(
                "reply requires a peer operation identity".into(),
            )
        })?;
        if original.id != Self::peer_message_id(&target, &caller, original_operation) {
            return Err(SessionMessengerError::InvalidEnvelope(
                "received peer message belongs to a different session lifetime".into(),
            ));
        }
        SessionMessageId::parse(&thread)
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?;
        let envelope = Self::peer_envelope(
            &caller,
            &target,
            operation_id,
            message,
            Self::peer_message_id(&caller, &target, operation_id),
            thread,
            Some(original.id.clone()),
        );
        self.send_peer_envelope(envelope, &caller, &target, Some(&original), intent)
            .await
    }

    fn validate_peer_input(operation_id: &str, message: &str) -> Result<(), SessionMessengerError> {
        SessionMessageId::parse(operation_id)
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?;
        if message.trim().is_empty()
            || message.len() > MAX_PEER_MESSAGE_BYTES
            || message.contains('\0')
        {
            return Err(SessionMessengerError::InvalidEnvelope(format!(
                "peer message must contain 1..={MAX_PEER_MESSAGE_BYTES} UTF-8 bytes without NUL"
            )));
        }
        Ok(())
    }

    fn validate_peer_session_id(id: &str) -> Result<(), SessionMessengerError> {
        bamboo_domain::validate_supervisor_target_id(id)
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.into()))?;
        SessionMessageId::parse(id)
            .map(|_| ())
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))
    }

    fn bounded_peer_label(value: &str, max_bytes: usize) -> String {
        let mut end = value.len().min(max_bytes);
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        value[..end].to_owned()
    }

    async fn peer_member(&self, id: &str, source: bool) -> Result<Session, SessionMessengerError> {
        Self::validate_peer_session_id(id)?;
        let session = self
            .sessions
            .load_runtime_control_plane(id)
            .await
            .map_err(|error| SessionMessengerError::Storage(error.to_string()))?
            .ok_or_else(|| {
                if source {
                    SessionMessengerError::SourceNotFound(id.into())
                } else {
                    SessionMessengerError::TargetNotFound(id.into())
                }
            })?;
        let unauthorized = || SessionMessengerError::Unauthorized {
            source_session_id: id.into(),
            target: session.root_session_id.clone(),
        };
        if session.id != id
            || session
                .runtime_metadata
                .as_ref()
                .and_then(|meta| meta.project_id.as_ref())
                .zip(session.metadata.get("project_id"))
                .is_some_and(|(typed, legacy)| typed != legacy)
            || bamboo_domain::ActorSession::from_session(&session).is_err()
        {
            return Err(unauthorized());
        }
        if session.kind == bamboo_domain::SessionKind::Root {
            return Ok(session);
        }
        crate::session_app::child_session::owned_tree::load_owned_descendant(
            &PeerTreePort(self.sessions.as_ref()),
            &session.root_session_id,
            id,
        )
        .await
        .map_err(|_| unauthorized())
    }

    async fn peer_pair(
        &self,
        caller_id: &str,
        target_id: &str,
    ) -> Result<(Session, Session), SessionMessengerError> {
        let caller = self.peer_member(caller_id, true).await?;
        let target = self.peer_member(target_id, false).await?;
        if caller_id == target_id
            || caller.root_session_id != target.root_session_id
            || caller.project_id_meta() != target.project_id_meta()
        {
            return Err(SessionMessengerError::Unauthorized {
                source_session_id: caller_id.into(),
                target: target_id.into(),
            });
        }
        Ok((caller, target))
    }

    fn peer_message_id(caller: &Session, target: &Session, operation_id: &str) -> SessionMessageId {
        SessionMessageId::stable(
            "session-peer-v1",
            &serde_json::json!({
                "source": caller.id, "source_birth": caller.created_at,
                "target": target.id, "target_birth": target.created_at,
                "operation_id": operation_id,
            }),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn peer_envelope(
        caller: &Session,
        target: &Session,
        operation_id: &str,
        message: &str,
        id: SessionMessageId,
        thread_id: String,
        in_reply_to: Option<SessionMessageId>,
    ) -> SessionMessageEnvelope {
        SessionMessageEnvelope {
            id,
            source: SessionMessageSource::Session {
                session_id: caller.id.clone(),
            },
            target_session_id: target.id.clone(),
            kind: SessionMessageKind::PeerMessage,
            body: SessionMessageBody::Content(SessionMessageContent::text(message)),
            created_at: chrono::Utc::now(),
            thread_id: Some(thread_id),
            in_reply_to,
            attempt: None,
            correlation_id: Some(operation_id.into()),
        }
    }

    async fn send_peer_envelope(
        &self,
        envelope: SessionMessageEnvelope,
        caller: &Session,
        target: &Session,
        reply_to: Option<&SessionMessageEnvelope>,
        intent: Option<SessionMessageDeliveryIntent>,
    ) -> Result<SessionPeerDeliveryReceipt, SessionMessengerError> {
        let started = Instant::now();
        let source = ActorSession::from_session(caller)
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?;
        let target = ActorSession::from_session(target)
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?;
        // These are the same births used for the stable message id. The inbox
        // must revalidate them under its retained lifecycle authority; a fresh
        // relationship read before generic delivery would still race rebirth.
        let result = match intent {
            Some(intent) => {
                self.inbox
                    .deliver_peer_message_with_intent(&envelope, &source, &target, reply_to, intent)
                    .await
            }
            None => {
                self.inbox
                    .deliver_peer_message(&envelope, &source, &target, reply_to)
                    .await
                    .map(|delivery| bamboo_domain::SessionInboxDeliveryReceipt {
                        delivery,
                        // Preserve the historical peer activation contract.
                        pending: true,
                    })
            }
        };
        let delivery = match result {
            Ok(receipt) => receipt,
            Err(error) => {
                let error = SessionMessengerError::Inbox(error);
                self.record_rejection(&error);
                return Err(error);
            }
        };
        let pending = delivery.pending;
        let admission = self.record_admission(envelope, delivery.delivery, started);
        // Exact explicit retries of ACKed/cancelled/dead deliveries cannot
        // revive input or request work from a fresh router. The Inbox observed
        // this state while retaining the original admission authority/locks.
        if !pending {
            return Ok(SessionPeerDeliveryReceipt {
                delivery: admission.delivery,
                activation: None,
                active_delivery_notified: false,
            });
        }
        if intent == Some(SessionMessageDeliveryIntent::QueueOnly) {
            let active_delivery_notified = self
                .activation
                .notify_active_delivery(&admission.target_session_id, admission.delivery.generation)
                .await;
            return Ok(SessionPeerDeliveryReceipt {
                delivery: admission.delivery,
                activation: None,
                active_delivery_notified,
            });
        }
        self.activate_prepared(&admission).await.map(Into::into)
    }

    /// Original executing Supervisor control over one separately host-granted
    /// live Root. Enqueue/notification is never a configuration application
    /// receipt. Reads use a fresh canonical target snapshot under both fences.
    pub async fn supervisor_loop_control(
        &self,
        supervisor: &SupervisorReference,
        target_session_id: &str,
        expected_link_revision: Option<u64>,
        command: &bamboo_domain::LoopControlRequest,
        receipt_operation_id: Option<&str>,
    ) -> Result<serde_json::Value, SessionMessengerError> {
        use bamboo_domain::{LoopControlAction, LoopControlRequest, ScopedLoopControlInput};
        let invalid = |message: String| SessionMessengerError::InvalidEnvelope(message);
        bamboo_domain::validate_supervisor_target_id(target_session_id)
            .map_err(|error| invalid(error.into()))?;
        bamboo_domain::validate_scoped_loop_control_command(command).map_err(invalid)?;
        let status = matches!(command, LoopControlRequest::Status);
        if expected_link_revision == Some(0)
            || (!status && expected_link_revision.is_none())
            || (!status && receipt_operation_id.is_some())
        {
            return Err(invalid("loop mutations require the exact nonzero link revision; receipt lookup is status-only".into()));
        }
        if let Some(operation) = receipt_operation_id {
            uuid::Uuid::parse_str(operation)
                .map_err(|_| invalid("receipt operation must be a UUID".into()))?;
        }
        let binding = self
            .activation
            .observe_supervisor_loop(
                supervisor,
                target_session_id,
                expected_link_revision,
                LoopControlAction::of(command),
            )
            .await?;
        binding.authorize(command).map_err(invalid)?;
        let canonical = self
            .sessions
            .load_scoped_loop_session(&binding, self.activation.clone())
            .await
            .map_err(|error| SessionMessengerError::Storage(error.to_string()))?
            .ok_or_else(|| SessionMessengerError::TargetNotFound(target_session_id.into()))?;
        if status {
            let mut snapshot = serde_json::to_value(
                crate::runtime::loop_control::inspect_loop(&canonical)
                    .map_err(|error| SessionMessengerError::Storage(error.to_string()))?,
            )
            .map_err(|error| SessionMessengerError::Storage(error.to_string()))?;
            // Bounded readback belongs to the initiating Supervisor's output
            // budget. The target's canonical objective bytes remain unchanged.
            for field in ["objective", "pending_objective"] {
                let text = snapshot[field].as_str().map(str::to_owned);
                if let Some(text) = text {
                    let mut end = text.len().min(1024);
                    while !text.is_char_boundary(end) {
                        end -= 1;
                    }
                    snapshot[field] = serde_json::Value::String(text[..end].into());
                    snapshot[format!("{field}_truncated")] =
                        serde_json::Value::Bool(end < text.len());
                }
            }
            let records = bamboo_domain::scoped_loop_consumptions(&canonical)
                .map_err(SessionMessengerError::Storage)?;
            let partial_checkpoint = records.iter().any(|record| {
                record.binding == binding
                    && receipt_operation_id.is_none_or(|id| id == record.operation_id)
                    && !canonical
                        .session_inbox_admission()
                        .is_some_and(|cursor| cursor.contains(&record.input_id))
            });
            let record = records.into_iter().rev().find(|record| {
                record.binding == binding
                    && receipt_operation_id.is_none_or(|id| id == record.operation_id)
                    && canonical
                        .session_inbox_admission()
                        .is_some_and(|cursor| cursor.contains(&record.input_id))
            });
            let state = crate::runtime::counted_loop::read_loop_state(&canonical)
                .map_err(|error| SessionMessengerError::Storage(error.to_string()))?;
            let objective_applied_actual = record
                .as_ref()
                .filter(|record| record.configured)
                .and_then(|record| record.receipt.as_ref())
                .zip(state.as_ref())
                .is_some_and(|(receipt, state)| {
                    receipt["generation"].as_str() == Some(state.generation.as_str())
                        && canonical.messages.iter().any(|message| {
                            message.role == bamboo_domain::Role::User
                                && message.metadata.as_ref().is_some_and(|metadata| {
                                    metadata["runtime_kind"].as_str()
                                        == Some("counted_loop_iteration")
                                        && metadata["loop_generation"] == receipt["generation"]
                                        && metadata["objective_revision"]
                                            == receipt["objective_revision"]
                                })
                        })
                        && state.control.as_ref().is_some_and(|control| {
                            receipt["accepted_revision"]
                                .as_u64()
                                .is_some_and(|revision| control.applied_revision >= revision)
                                && receipt["objective_revision"]
                                    .as_u64()
                                    .is_some_and(|revision| {
                                        revision > 0
                                            && control.iteration_objective_revision >= revision
                                    })
                        })
                });
            return Ok(serde_json::json!({
                "action":"loop", "command":"status", "target_session_id":target_session_id,
                "link_revision":binding.link_revision, "owner_run_id":binding.owner_run_id,
                "execution_id":binding.execution_id, "loop":snapshot,
                "host_max_iterations":binding.max_iterations, "host_max_total_rounds":binding.max_total_rounds,
                "configured":record.as_ref().is_some_and(|record|record.configured),
                "checkpoint_observation":if partial_checkpoint { "partial_or_unknown" } else if record.is_some() { "confirmed" } else { "not_observed" },
                "operation_receipt":record, "objective_applied_actual":objective_applied_actual,
                "note":"Configuration receipts are immutable target checkpoints; objective_applied_actual additionally requires its real iteration-admission checkpoint. Neither is a task result."
            }));
        }
        let input = ScopedLoopControlInput {
            binding,
            command: command.clone(),
        };
        let envelope = input.envelope().map_err(invalid)?;
        if let Some(record) = bamboo_domain::scoped_loop_consumptions(&canonical)
            .map_err(SessionMessengerError::Storage)?
            .into_iter()
            .find(|record| record.input_id == envelope.id)
        {
            if !bamboo_domain::has_scoped_loop_consumption(&canonical, &envelope) {
                return Err(invalid(
                    "operation payload conflicts with its immutable target receipt".into(),
                ));
            }
            if canonical
                .session_inbox_admission()
                .is_some_and(|cursor| cursor.contains(&record.input_id))
            {
                return Ok(serde_json::json!({
                    "action":"loop", "target_session_id":target_session_id,
                    "operation_id":input.operation_id(), "receipt_id":envelope.id,
                    "admitted":true, "replayed":true, "configured":record.configured,
                    "operation_receipt":record, "objective_applied_actual":false,
                    "note":"The original configuration receipt is replayed; use status for current objective application."
                }));
            }
            // A runtime sidecar without Main's typed cursor is a partial
            // publication, not a confirmed application. Exact delivery below
            // recovers its original queued receipt without claiming success.
        }
        let started = Instant::now();
        let receipt = self
            .inbox
            .deliver_supervisor_loop_control(&input.binding, &envelope, self.activation.clone())
            .await?;
        self.record_admission(envelope, receipt.delivery.clone(), started);
        Ok(serde_json::json!({
            "action":"loop", "target_session_id":target_session_id,
            "operation_id":input.operation_id(), "receipt_id":receipt.delivery.id,
            "generation":receipt.delivery.generation, "admitted":true,
            "replayed":receipt.replayed, "active_delivery_notified":receipt.active_delivery_notified,
            "configured":false, "target_application_observed":false,
            "objective_applied_actual":false, "operation_receipt":null,
            "note":"Transport admission is historical on replay; the input may no longer be pending. Target application remains unobserved here; query status with receipt_operation_id. This input never starts a successor or answers a wait."
        }))
    }

    /// Follow an already-attached Ordinary Root from the original executing
    /// Supervisor. The concrete inbox revalidates and retains all authority
    /// locks through durable admission; a prior inspect_link is never a grant.
    pub async fn supervisor_followup(
        &self,
        supervisor: &SupervisorReference,
        target_session_id: &str,
        operation_id: &str,
        message: &str,
    ) -> Result<SessionMessengerReceipt, SessionMessengerError> {
        bamboo_domain::validate_supervisor_target_id(target_session_id)
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?;
        SessionMessageId::parse(operation_id)
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?;
        if message.trim().is_empty() || message.len() > MAX_SUPERVISOR_FOLLOWUP_BYTES {
            return Err(SessionMessengerError::InvalidEnvelope(format!(
                "followup message must contain 1..={MAX_SUPERVISOR_FOLLOWUP_BYTES} UTF-8 bytes"
            )));
        }
        let envelope = SessionMessageEnvelope {
            id: SessionMessageId::stable(
                "supervisor_followup",
                &serde_json::json!({
                    "supervisor": supervisor,
                    "target_session_id": target_session_id,
                    "operation_id": operation_id,
                }),
            ),
            source: SessionMessageSource::Session {
                session_id: supervisor.session_id.clone(),
            },
            target_session_id: target_session_id.to_string(),
            kind: SessionMessageKind::PeerMessage,
            body: SessionMessageBody::Content(SessionMessageContent::text(message)),
            created_at: chrono::Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: Some(operation_id.to_string()),
        };
        let started = Instant::now();
        let delivery = self
            .inbox
            .deliver_supervisor_followup(supervisor, &envelope)
            .await
            .map_err(|error| {
                let error = SessionMessengerError::Inbox(error);
                self.record_rejection(&error);
                error
            })?;
        let admission = self.record_admission(envelope, delivery, started);
        // Its specific-message RespectSpecificWait intent was published while
        // holding Supervisor authority. This grants no staged sibling prefix.
        self.activate_prepared(&admission).await
    }

    async fn load_session(&self, id: &str) -> Result<Option<Session>, SessionMessengerError> {
        self.sessions
            .load_session(id)
            .await
            .map_err(|error| SessionMessengerError::Storage(error.to_string()))
    }

    fn logical_root(session: &Session) -> &str {
        if session.root_session_id.trim().is_empty() {
            &session.id
        } else {
            &session.root_session_id
        }
    }

    async fn validate_relationship(
        &self,
        envelope: &SessionMessageEnvelope,
    ) -> Result<(), SessionMessengerError> {
        envelope
            .validate()
            .map_err(|error| SessionMessengerError::InvalidEnvelope(error.to_string()))?;
        let target = self
            .load_session(&envelope.target_session_id)
            .await?
            .ok_or_else(|| {
                SessionMessengerError::TargetNotFound(envelope.target_session_id.clone())
            })?;

        let SessionMessageSource::Session { session_id } = &envelope.source else {
            return Ok(());
        };
        let source = self
            .load_session(session_id)
            .await?
            .ok_or_else(|| SessionMessengerError::SourceNotFound(session_id.clone()))?;

        let same_root = Self::logical_root(&source) == Self::logical_root(&target);
        let source_project = source.project_id_meta();
        let target_project = target.project_id_meta();
        let project_compatible = match (source_project.as_deref(), target_project.as_deref()) {
            (Some(left), Some(right)) => left == right,
            // Older sessions did not persist project_id. Same-root ancestry is
            // the compatibility authority until both sides have migrated.
            _ => true,
        };
        if !same_root || !project_compatible {
            return Err(SessionMessengerError::Unauthorized {
                source_session_id: source.id,
                target: target.id,
            });
        }
        Ok(())
    }

    fn record_rejection(&self, error: &SessionMessengerError) {
        self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
        match error {
            SessionMessengerError::InvalidEnvelope(_) => {
                self.metrics
                    .invalid_envelope
                    .fetch_add(1, Ordering::Relaxed);
            }
            SessionMessengerError::Unauthorized { .. }
            | SessionMessengerError::SourceNotFound(_)
            | SessionMessengerError::TargetNotFound(_) => {
                self.metrics.unauthorized.fetch_add(1, Ordering::Relaxed);
            }
            SessionMessengerError::Inbox(SessionInboxError::AdmissionCancelled)
            | SessionMessengerError::Inbox(SessionInboxError::ExpectedRunConflict(_)) => {}
            SessionMessengerError::Inbox(SessionInboxError::PayloadTooLarge { .. }) => {
                self.metrics
                    .payload_too_large
                    .fetch_add(1, Ordering::Relaxed);
            }
            SessionMessengerError::Inbox(SessionInboxError::BacklogFull { .. }) => {
                self.metrics.backlog_full.fetch_add(1, Ordering::Relaxed);
            }
            SessionMessengerError::Inbox(_) | SessionMessengerError::Storage(_) => {
                self.metrics.storage_failed.fetch_add(1, Ordering::Relaxed);
            }
            SessionMessengerError::Activation { .. }
            | SessionMessengerError::ActivationEligibility { .. } => {}
        }
    }

    pub async fn admit(
        &self,
        envelope: SessionMessageEnvelope,
    ) -> Result<SessionMessengerAdmission, SessionMessengerError> {
        self.admit_with_gate(envelope, None).await
    }

    pub async fn admit_with_gate(
        &self,
        envelope: SessionMessageEnvelope,
        gate: Option<&bamboo_domain::AdmissionGate>,
    ) -> Result<SessionMessengerAdmission, SessionMessengerError> {
        self.admit_with_delivery_intent(envelope, gate, None).await
    }

    /// Commit this specific message's immediate permission together with the
    /// envelope. Coordinators continue to use staged [`admit`](Self::admit).
    pub async fn admit_with_activation_intent(
        &self,
        envelope: SessionMessageEnvelope,
        policy: SessionActivationPolicy,
        gate: Option<&bamboo_domain::AdmissionGate>,
    ) -> Result<SessionMessengerAdmission, SessionMessengerError> {
        self.admit_with_delivery_intent(envelope, gate, Some(policy))
            .await
    }

    async fn admit_with_delivery_intent(
        &self,
        envelope: SessionMessageEnvelope,
        gate: Option<&bamboo_domain::AdmissionGate>,
        policy: Option<SessionActivationPolicy>,
    ) -> Result<SessionMessengerAdmission, SessionMessengerError> {
        let started = Instant::now();
        if let Err(error) = self.validate_relationship(&envelope).await {
            self.record_rejection(&error);
            tracing::warn!(
                message_id = %envelope.id,
                target_session_id = %envelope.target_session_id,
                error = %error,
                "session message rejected"
            );
            return Err(error);
        }

        let delivery = match (policy, gate) {
            (Some(policy), gate) => {
                self.inbox
                    .deliver_with_activation_intent(&envelope, policy, gate)
                    .await
            }
            (None, Some(gate)) => self.inbox.deliver_with_gate(&envelope, gate).await,
            (None, None) => self.inbox.deliver(&envelope).await,
        };
        let delivery = match delivery {
            Ok(receipt) => receipt,
            Err(error) => {
                let error = SessionMessengerError::Inbox(error);
                self.record_rejection(&error);
                return Err(error);
            }
        };
        Ok(self.record_admission(envelope, delivery, started))
    }

    fn record_admission(
        &self,
        envelope: SessionMessageEnvelope,
        delivery: SessionInboxReceipt,
        started: Instant,
    ) -> SessionMessengerAdmission {
        self.metrics.delivered.fetch_add(1, Ordering::Relaxed);
        self.metrics.delivery_latency_micros.fetch_add(
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        SessionMessengerAdmission {
            envelope_id: envelope.id.as_str().to_string(),
            target_session_id: envelope.target_session_id,
            delivery,
        }
    }

    pub async fn activate(
        &self,
        admission: &SessionMessengerAdmission,
    ) -> Result<SessionMessengerReceipt, SessionMessengerError> {
        self.activate_with_policy(admission, SessionActivationPolicy::InterruptSpecificWait)
            .await
    }

    pub async fn activate_with_policy(
        &self,
        admission: &SessionMessengerAdmission,
        policy: SessionActivationPolicy,
    ) -> Result<SessionMessengerReceipt, SessionMessengerError> {
        if let Err(error) = self
            .inbox
            .mark_activation_eligible(
                &admission.target_session_id,
                admission.delivery.generation,
                policy,
            )
            .await
        {
            self.metrics
                .activation_failed
                .fetch_add(1, Ordering::Relaxed);
            return Err(SessionMessengerError::ActivationEligibility {
                receipt_id: admission.delivery.id.to_string(),
                receipt: admission.delivery.clone(),
                source: error,
            });
        }
        self.activate_prepared(admission).await
    }

    /// Request execution for an admission whose activation policy was already
    /// durably recorded by a coordinator.
    pub async fn activate_prepared(
        &self,
        admission: &SessionMessengerAdmission,
    ) -> Result<SessionMessengerReceipt, SessionMessengerError> {
        let activation = match self
            .activation
            .request_activation(&admission.target_session_id, admission.delivery.generation)
            .await
        {
            Ok(disposition) => disposition,
            Err(source) => {
                self.metrics
                    .activation_failed
                    .fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    message_id = %admission.envelope_id,
                    target_session_id = %admission.target_session_id,
                    generation = admission.delivery.generation,
                    error = %source,
                    "session message durable but activation failed"
                );
                return Err(SessionMessengerError::Activation {
                    receipt_id: admission.delivery.id.to_string(),
                    receipt: admission.delivery.clone(),
                    source,
                });
            }
        };
        match activation {
            SessionActivationDisposition::ActiveNotified => {
                self.metrics.active_notified.fetch_add(1, Ordering::Relaxed);
            }
            SessionActivationDisposition::ActivationReserved => {
                self.metrics
                    .activation_reserved
                    .fetch_add(1, Ordering::Relaxed);
            }
            SessionActivationDisposition::ActivationCoalesced => {
                self.metrics
                    .activation_coalesced
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        tracing::info!(
            message_id = %admission.envelope_id,
            target_session_id = %admission.target_session_id,
            generation = admission.delivery.generation,
            ?activation,
            "session message durably delivered"
        );
        Ok(SessionMessengerReceipt {
            delivery: admission.delivery.clone(),
            activation,
        })
    }

    /// Durably authorize the admitted queue prefix before a related
    /// control-plane transition is committed.
    ///
    /// Coordinators call this while their specific wait is still armed, then
    /// checkpoint the wait clear, and finally call [`activate`](Self::activate).
    /// A crash in either gap is recoverable: startup sees the watermark, while
    /// the real reservation adapter refuses to run until the specific wait has
    /// been durably cleared.
    pub async fn prepare_activation(
        &self,
        admission: &SessionMessengerAdmission,
    ) -> Result<(), SessionMessengerError> {
        self.inbox
            .mark_activation_eligible(
                &admission.target_session_id,
                admission.delivery.generation,
                SessionActivationPolicy::RespectSpecificWait,
            )
            .await
            .map_err(SessionMessengerError::Inbox)
    }

    /// Authorize one staged delivery, preserving its immutable delivery
    /// intent and every sibling's existing execution permission.
    pub async fn prepare_message_activation(
        &self,
        admission: &SessionMessengerAdmission,
        policy: SessionActivationPolicy,
    ) -> Result<(), SessionMessengerError> {
        self.inbox
            .mark_message_activation_eligible(
                &admission.target_session_id,
                &admission.delivery,
                policy,
            )
            .await
            .map_err(SessionMessengerError::Inbox)
    }

    pub async fn send(
        &self,
        envelope: SessionMessageEnvelope,
    ) -> Result<SessionMessengerReceipt, SessionMessengerError> {
        self.send_with_gate(envelope, None).await
    }

    pub async fn send_with_gate(
        &self,
        envelope: SessionMessageEnvelope,
        gate: Option<&bamboo_domain::AdmissionGate>,
    ) -> Result<SessionMessengerReceipt, SessionMessengerError> {
        let admission = self
            .admit_with_activation_intent(
                envelope,
                SessionActivationPolicy::InterruptSpecificWait,
                gate,
            )
            .await?;
        self.activate_prepared(&admission).await
    }

    pub async fn send_with_policy(
        &self,
        envelope: SessionMessageEnvelope,
        policy: SessionActivationPolicy,
    ) -> Result<SessionMessengerReceipt, SessionMessengerError> {
        let admission = self
            .admit_with_activation_intent(envelope, policy, None)
            .await?;
        self.activate_prepared(&admission).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bamboo_domain::{
        SessionActivationDisposition, SessionInboxLimits, SessionMessageBody,
        SessionMessageContent, SessionMessageId, SessionMessageKind,
    };
    use bamboo_storage::{FileSessionInbox, SessionStoreV2};
    use tempfile::TempDir;

    struct RecordingActivation {
        calls: tokio::sync::Mutex<Vec<(String, u64)>>,
    }

    struct FailingActivation;

    #[async_trait]
    impl SessionActivationPort for FailingActivation {
        async fn request_activation(
            &self,
            _target_session_id: &str,
            _inbox_generation: u64,
        ) -> Result<SessionActivationDisposition, SessionActivationError> {
            Err(SessionActivationError::Internal(
                "injected wakeup failure".into(),
            ))
        }
    }

    #[tokio::test]
    async fn immediate_activation_failure_preserves_receipt_and_restart_permission() {
        let (temp, store, recording, _messenger) = fixture().await;
        store
            .save_session(&Session::new("root", "model"))
            .await
            .unwrap();
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let messenger = SessionMessenger::new(store, inbox, Arc::new(FailingActivation));
        let envelope = SessionMessageEnvelope::user_input("root", "steer");
        let gate = bamboo_domain::AdmissionGate::default();
        let receipt = match messenger
            .send_with_gate(envelope.clone(), Some(&gate))
            .await
            .unwrap_err()
        {
            SessionMessengerError::Activation { receipt, .. } => receipt,
            error => panic!("expected durable delivery with failed wakeup: {error}"),
        };
        assert!(gate.is_committed());
        gate.cancel_if_pending();
        let restarted_store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
        let restarted = Arc::new(FileSessionInbox::new(
            restarted_store.clone(),
            SessionInboxLimits::default(),
        ));
        let backlog = restarted.inspect("root").await.unwrap();
        assert!(backlog.activation_pending());
        assert_eq!(backlog.coordinator_generation, 0);
        // This is the startup reconciliation request: the durable generation
        // comes from this message's intent while the coordinator prefix is 0.
        recording
            .request_activation("root", backlog.activation_generation)
            .await
            .unwrap();
        let retry = SessionMessenger::new(restarted_store, restarted.clone(), recording.clone());
        assert_eq!(
            retry
                .send_with_gate(envelope, Some(&gate))
                .await
                .unwrap()
                .delivery,
            receipt
        );
        assert_eq!(restarted.inspect("root").await.unwrap().pending, 1);
        assert_eq!(messenger.metrics().snapshot().activation_failed, 1);
        let claims = restarted.claim("root", 128).await.unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].envelope.id, receipt.id);
    }

    struct FailEligibilityOnce {
        inner: Arc<FileSessionInbox>,
        fail_next: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl SessionInboxPort for FailEligibilityOnce {
        async fn deliver(
            &self,
            envelope: &SessionMessageEnvelope,
        ) -> Result<SessionInboxReceipt, SessionInboxError> {
            self.inner.deliver(envelope).await
        }

        async fn deliver_with_gate(
            &self,
            envelope: &SessionMessageEnvelope,
            gate: &bamboo_domain::AdmissionGate,
        ) -> Result<SessionInboxReceipt, SessionInboxError> {
            self.inner.deliver_with_gate(envelope, gate).await
        }

        async fn mark_activation_eligible(
            &self,
            target_session_id: &str,
            generation: u64,
            policy: SessionActivationPolicy,
        ) -> Result<(), SessionInboxError> {
            if self.fail_next.swap(false, Ordering::SeqCst) {
                return Err(SessionInboxError::Storage(
                    "injected activation watermark failure".into(),
                ));
            }
            self.inner
                .mark_activation_eligible(target_session_id, generation, policy)
                .await
        }

        async fn claim(
            &self,
            target_session_id: &str,
            limit: usize,
        ) -> Result<Vec<bamboo_domain::SessionInboxClaim>, SessionInboxError> {
            self.inner.claim(target_session_id, limit).await
        }

        async fn was_admitted(
            &self,
            target_session_id: &str,
            id: &SessionMessageId,
        ) -> Result<bool, SessionInboxError> {
            self.inner.was_admitted(target_session_id, id).await
        }

        async fn ack(
            &self,
            target_session_id: &str,
            claim: &bamboo_domain::SessionInboxClaim,
        ) -> Result<(), SessionInboxError> {
            self.inner.ack(target_session_id, claim).await
        }

        async fn inspect(
            &self,
            target_session_id: &str,
        ) -> Result<bamboo_domain::SessionInboxBacklog, SessionInboxError> {
            self.inner.inspect(target_session_id).await
        }
    }

    #[async_trait]
    impl SessionActivationPort for RecordingActivation {
        async fn request_activation(
            &self,
            target_session_id: &str,
            inbox_generation: u64,
        ) -> Result<SessionActivationDisposition, SessionActivationError> {
            self.calls
                .lock()
                .await
                .push((target_session_id.to_string(), inbox_generation));
            Ok(SessionActivationDisposition::ActivationReserved)
        }
    }

    async fn fixture() -> (
        TempDir,
        Arc<SessionStoreV2>,
        Arc<RecordingActivation>,
        SessionMessenger,
    ) {
        let temp = TempDir::new().unwrap();
        let store = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let activation = Arc::new(RecordingActivation {
            calls: tokio::sync::Mutex::new(Vec::new()),
        });
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let messenger = SessionMessenger::new(store.clone(), inbox, activation.clone());
        (temp, store, activation, messenger)
    }

    fn peer(source: &str, target: &str, id: &str) -> SessionMessageEnvelope {
        SessionMessageEnvelope {
            id: SessionMessageId::parse(id).unwrap(),
            source: SessionMessageSource::Session {
                session_id: source.to_string(),
            },
            target_session_id: target.to_string(),
            kind: SessionMessageKind::PeerMessage,
            body: SessionMessageBody::Content(SessionMessageContent::text("hello")),
            created_at: chrono::Utc::now(),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: None,
        }
    }

    #[derive(Default)]
    struct PeerMemoryStorage(tokio::sync::RwLock<std::collections::HashMap<String, Session>>);

    #[async_trait]
    impl Storage for PeerMemoryStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.0
                .write()
                .await
                .insert(session.id.clone(), session.clone());
            Ok(())
        }

        async fn load_session(&self, id: &str) -> std::io::Result<Option<Session>> {
            Ok(self.0.read().await.get(id).cloned())
        }

        async fn delete_session(&self, id: &str) -> std::io::Result<bool> {
            Ok(self.0.write().await.remove(id).is_some())
        }

        async fn list_child_run_statuses(
            &self,
            parent: &str,
        ) -> std::io::Result<Vec<(String, Option<String>)>> {
            Ok(self
                .0
                .read()
                .await
                .values()
                .filter(|session| session.parent_session_id.as_deref() == Some(parent))
                .map(|session| (session.id.clone(), session.last_run_status()))
                .collect())
        }
    }

    async fn seed_peer_tree(store: &dyn Storage) {
        let mut root = Session::new("peer-root", "model");
        root.title = "Logical root".into();
        root.set_project_id_meta("peer-project");
        let left = Session::new_child_of("peer-left", &root, "model", "Left");
        let right = Session::new_child_of("peer-right", &root, "model", "Right");
        let cousin_left = Session::new_child_of("peer-left-leaf", &left, "model", "Left leaf");
        let cousin_right = Session::new_child_of("peer-right-leaf", &right, "model", "Right leaf");
        for mut session in [root, left, right, cousin_left, cousin_right] {
            session.set_project_id_meta("peer-project");
            store.save_session(&session).await.unwrap();
        }
    }

    struct PausedPeerAdmission {
        inner: Arc<FileSessionInbox>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl SessionInboxPort for PausedPeerAdmission {
        async fn deliver(
            &self,
            envelope: &SessionMessageEnvelope,
        ) -> Result<SessionInboxReceipt, SessionInboxError> {
            self.inner.deliver(envelope).await
        }

        async fn deliver_peer_message(
            &self,
            envelope: &SessionMessageEnvelope,
            source: &ActorSession,
            target: &ActorSession,
            reply_to: Option<&SessionMessageEnvelope>,
        ) -> Result<SessionInboxReceipt, SessionInboxError> {
            // The engine has already captured both births and built its id;
            // actual filesystem admission has not yet acquired its authority.
            self.entered.notify_one();
            self.release.notified().await;
            self.inner
                .deliver_peer_message(envelope, source, target, reply_to)
                .await
        }

        async fn mark_activation_eligible(
            &self,
            target: &str,
            generation: u64,
            policy: SessionActivationPolicy,
        ) -> Result<(), SessionInboxError> {
            self.inner
                .mark_activation_eligible(target, generation, policy)
                .await
        }

        async fn claim(
            &self,
            target: &str,
            limit: usize,
        ) -> Result<Vec<bamboo_domain::SessionInboxClaim>, SessionInboxError> {
            self.inner.claim(target, limit).await
        }

        async fn was_admitted(
            &self,
            target: &str,
            id: &SessionMessageId,
        ) -> Result<bool, SessionInboxError> {
            self.inner.was_admitted(target, id).await
        }

        async fn ack(
            &self,
            target: &str,
            claim: &bamboo_domain::SessionInboxClaim,
        ) -> Result<(), SessionInboxError> {
            self.inner.ack(target, claim).await
        }

        async fn inspect(
            &self,
            target: &str,
        ) -> Result<bamboo_domain::SessionInboxBacklog, SessionInboxError> {
            self.inner.inspect(target).await
        }
    }

    #[tokio::test]
    async fn peer_send_and_reply_reject_either_lifetime_recreated_before_admission() {
        for reply in [false, true] {
            for replaced_id in ["peer-left", "peer-right"] {
                let (_temp, store, activation, messenger) = fixture().await;
                seed_peer_tree(store.as_ref()).await;
                let received_id = if reply {
                    let incoming = messenger
                        .send_peer("peer-left", "peer-right", "incoming", "question")
                        .await
                        .unwrap();
                    admit_peer_for_test(store.as_ref(), messenger.inbox().as_ref(), "peer-right")
                        .await;
                    Some(incoming.delivery.id.to_string())
                } else {
                    None
                };
                activation.calls.lock().await.clear();
                let (caller, target) = if reply {
                    ("peer-right", "peer-left")
                } else {
                    ("peer-left", "peer-right")
                };
                let entered = Arc::new(tokio::sync::Notify::new());
                let release = Arc::new(tokio::sync::Notify::new());
                let inbox = Arc::new(FileSessionInbox::new(
                    store.clone(),
                    SessionInboxLimits::default(),
                ));
                let paused = SessionMessenger::new(
                    store.clone(),
                    Arc::new(PausedPeerAdmission {
                        inner: inbox.clone(),
                        entered: entered.clone(),
                        release: release.clone(),
                    }),
                    activation.clone(),
                );
                let sending = tokio::spawn(async move {
                    if let Some(id) = received_id {
                        paused
                            .reply_peer(caller, &id, "racing", "old lifetime")
                            .await
                    } else {
                        paused
                            .send_peer(caller, target, "racing", "old lifetime")
                            .await
                    }
                });
                tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
                    .await
                    .unwrap();
                let old = store.load_session(replaced_id).await.unwrap().unwrap();
                assert!(store.delete_session(replaced_id).await.unwrap());
                let root = store.load_session("peer-root").await.unwrap().unwrap();
                let mut replacement =
                    Session::new_child_of(replaced_id, &root, "model", "Replacement");
                replacement.created_at = old.created_at + chrono::Duration::seconds(1);
                replacement.set_project_id_meta("peer-project");
                store.save_session(&replacement).await.unwrap();
                release.notify_one();
                let result = tokio::time::timeout(std::time::Duration::from_secs(5), sending)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(result.is_err(), "reply={reply}, replaced={replaced_id}");
                let backlog = inbox.inspect(target).await.unwrap();
                assert_eq!(backlog.generation, 0);
                assert_eq!(backlog.pending, 0);
                assert!(!backlog.activation_pending());
                assert!(activation.calls.lock().await.is_empty());
                // Same ids, tree and Project are legal for a fresh operation;
                // only the stale captured lifetime must fail closed.
                let current = messenger
                    .send_peer(caller, target, "current-lifetime", "current")
                    .await
                    .unwrap();
                assert_eq!(current.delivery.generation, 1);
                assert_eq!(activation.calls.lock().await.len(), 1);
            }
        }
    }

    #[tokio::test]
    async fn peer_reply_revalidates_received_proof_after_concurrent_clear_or_save() {
        for clear in [true, false] {
            let (_temp, store, activation, messenger) = fixture().await;
            seed_peer_tree(store.as_ref()).await;
            let incoming = messenger
                .send_peer("peer-left", "peer-right", "incoming", "question")
                .await
                .unwrap();
            admit_peer_for_test(store.as_ref(), messenger.inbox().as_ref(), "peer-right").await;
            activation.calls.lock().await.clear();
            let before = store.load_session("peer-right").await.unwrap().unwrap();
            let received_id = incoming.delivery.id.to_string();
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let inbox = Arc::new(FileSessionInbox::new(
                store.clone(),
                SessionInboxLimits::default(),
            ));
            let paused = SessionMessenger::new(
                store.clone(),
                Arc::new(PausedPeerAdmission {
                    inner: inbox.clone(),
                    entered: entered.clone(),
                    release: release.clone(),
                }),
                activation.clone(),
            );
            let old_id = received_id.clone();
            let replying = tokio::spawn(async move {
                paused
                    .reply_peer("peer-right", &old_id, "racing-proof", "reply")
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            if clear {
                assert!(store.clear_session("peer-right").await.unwrap());
            } else {
                let mut changed = before.clone();
                changed.messages.retain(|message| message.id != received_id);
                store.save_session(&changed).await.unwrap();
            }
            let after = store.load_session("peer-right").await.unwrap().unwrap();
            assert_eq!(before.created_at, after.created_at);
            assert_eq!(before.root_session_id, after.root_session_id);
            assert_eq!(before.project_id_meta(), after.project_id_meta());
            assert!(!after
                .messages
                .iter()
                .any(|message| message.id == received_id));
            release.notify_one();
            let result = tokio::time::timeout(std::time::Duration::from_secs(5), replying)
                .await
                .unwrap()
                .unwrap();
            assert!(
                result.is_err(),
                "clear={clear}: removed proof must not admit a reply"
            );
            let backlog = inbox.inspect("peer-left").await.unwrap();
            assert_eq!(backlog.generation, 0);
            assert_eq!(backlog.pending, 0);
            assert!(!backlog.activation_pending());
            assert!(activation.calls.lock().await.is_empty());

            let current = messenger
                .send_peer("peer-left", "peer-right", "current-question", "current")
                .await
                .unwrap();
            admit_peer_for_test(store.as_ref(), messenger.inbox().as_ref(), "peer-right").await;
            let reply = messenger
                .reply_peer(
                    "peer-right",
                    current.delivery.id.as_str(),
                    "racing-proof",
                    "reply",
                )
                .await
                .unwrap();
            let retry = messenger
                .reply_peer(
                    "peer-right",
                    current.delivery.id.as_str(),
                    "racing-proof",
                    "reply",
                )
                .await
                .unwrap();
            assert_eq!(reply.delivery.generation, 1);
            assert_eq!(retry.delivery.generation, reply.delivery.generation);
            assert_eq!(retry.delivery.id, reply.delivery.id);
            let backlog = inbox.inspect("peer-left").await.unwrap();
            assert_eq!(backlog.generation, 1);
            assert_eq!(backlog.pending, 1);
        }
    }

    #[tokio::test]
    async fn peer_admission_without_integrated_lifetime_fence_is_unsupported() {
        let (_temp, store, activation, _messenger) = fixture().await;
        seed_peer_tree(store.as_ref()).await;
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let messenger = SessionMessenger::new(
            store,
            Arc::new(FailEligibilityOnce {
                inner: inbox.clone(),
                fail_next: std::sync::atomic::AtomicBool::new(false),
            }),
            activation.clone(),
        );
        let error = messenger
            .send_peer("peer-left", "peer-right", "unsupported", "message")
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("lifetime-fenced peer SessionInbox admission is unsupported"));
        for intent in [
            SessionMessageDeliveryIntent::QueueOnly,
            SessionMessageDeliveryIntent::TriggerTurn,
        ] {
            assert!(messenger
                .send_peer_with_intent(
                    "peer-left",
                    "peer-right",
                    "unsupported-explicit",
                    "message",
                    intent
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("explicit peer delivery intent is unsupported"));
        }
        assert_eq!(inbox.inspect("peer-right").await.unwrap().pending, 0);
        assert!(activation.calls.lock().await.is_empty());
    }

    async fn admit_peer_for_test(
        store: &dyn Storage,
        inbox: &dyn SessionInboxPort,
        target: &str,
    ) -> SessionMessageEnvelope {
        let claims = inbox.claim(target, 16).await.unwrap();
        assert_eq!(claims.len(), 1);
        let claim = &claims[0];
        let mut session = store.load_session(target).await.unwrap().unwrap();
        session.add_message(claim.envelope.to_provider_message().unwrap());
        store.save_session(&session).await.unwrap();
        inbox.ack(target, claim).await.unwrap();
        claim.envelope.clone()
    }

    #[tokio::test]
    async fn peer_api_discovers_and_messages_parent_child_siblings_and_cousins() {
        let (_temp, store, activation, messenger) = fixture().await;
        seed_peer_tree(store.as_ref()).await;
        store
            .save_session(&Session::new("foreign-root", "model"))
            .await
            .unwrap();
        let targets = messenger.list_peer_targets("peer-left-leaf").await.unwrap();
        assert!(!targets.truncated);
        let ids: Vec<_> = targets
            .targets
            .iter()
            .map(|target| target.session_id.as_str())
            .collect();
        assert_eq!(
            ids,
            ["peer-root", "peer-left", "peer-right", "peer-right-leaf"]
        );
        assert_eq!(targets.targets[0].title, "Logical root");
        assert!(targets.targets[0].parent_session_id.is_none());
        assert!(!serde_json::to_string(&targets)
            .unwrap()
            .contains("foreign-root"));

        for (index, (source, target)) in [
            ("peer-root", "peer-left"),
            ("peer-left", "peer-root"),
            ("peer-left", "peer-right"),
            ("peer-left-leaf", "peer-right-leaf"),
        ]
        .into_iter()
        .enumerate()
        {
            let receipt = messenger
                .send_peer(
                    source,
                    target,
                    &format!("operation-{index}"),
                    "bounded peer instruction",
                )
                .await
                .unwrap();
            let backlog = messenger.inbox().inspect(target).await.unwrap();
            assert_eq!(
                backlog.coordinator_generation, 0,
                "peer permission must not release a prefix"
            );
            assert!(backlog.interrupt_pending());
            let envelope =
                admit_peer_for_test(store.as_ref(), messenger.inbox().as_ref(), target).await;
            assert_eq!(envelope.id, receipt.delivery.id);
            assert_eq!(
                envelope.source,
                SessionMessageSource::Session {
                    session_id: source.into()
                }
            );
            assert_eq!(envelope.thread_id.as_deref(), Some(envelope.id.as_str()));
            assert_eq!(
                envelope.correlation_id.as_deref(),
                Some(format!("operation-{index}").as_str())
            );
            assert!(envelope.in_reply_to.is_none());
        }
        assert_eq!(activation.calls.lock().await.len(), 4);
    }

    #[tokio::test]
    async fn peer_api_denies_unknown_deleted_cross_root_and_invalid_project_or_lineage() {
        let (_temp, store, activation, messenger) = fixture().await;
        seed_peer_tree(store.as_ref()).await;
        store
            .save_session(&Session::new("foreign-root", "model"))
            .await
            .unwrap();
        for (source, target) in [
            ("peer-left", "foreign-root"),
            ("unknown", "peer-left"),
            ("peer-left", "unknown"),
            ("peer-left", "peer-left"),
        ] {
            assert!(messenger
                .send_peer(source, target, "denied-operation", "instruction")
                .await
                .is_err());
        }
        store.delete_session("peer-right-leaf").await.unwrap();
        assert!(matches!(
            messenger
                .send_peer("peer-left", "peer-right-leaf", "deleted", "instruction")
                .await,
            Err(SessionMessengerError::TargetNotFound(_))
        ));
        assert!(matches!(
            messenger
                .send_peer(
                    "peer-right-leaf",
                    "peer-left",
                    "deleted-source",
                    "instruction"
                )
                .await,
            Err(SessionMessengerError::SourceNotFound(_))
        ));

        // Invalid canonical snapshots exercise the read boundary without
        // relaxing the production store's immutable lineage writer checks.
        let memory = Arc::new(PeerMemoryStorage::default());
        seed_peer_tree(memory.as_ref()).await;
        let strict = SessionMessenger::new(
            memory.clone(),
            messenger.inbox().clone(),
            activation.clone(),
        );
        let original = memory.load_session("peer-right").await.unwrap().unwrap();
        for defect in [
            "project",
            "missing_project",
            "project_mirror",
            "parent",
            "depth",
            "root",
            "cycle",
        ] {
            let mut invalid = original.clone();
            match defect {
                "project" => invalid.set_project_id_meta("other-project"),
                "missing_project" => {
                    invalid.metadata.remove("project_id");
                    if let Some(metadata) = invalid.runtime_metadata.as_mut() {
                        metadata.project_id = None;
                    }
                }
                "project_mirror" => {
                    invalid
                        .metadata
                        .insert("project_id".into(), "other-project".into());
                }
                "parent" => invalid.parent_session_id = Some("missing-parent".into()),
                "depth" => invalid.spawn_depth += 1,
                "root" => invalid.root_session_id = "foreign-root".into(),
                "cycle" => invalid.parent_session_id = Some("peer-right-leaf".into()),
                _ => unreachable!(),
            }
            memory.save_session(&invalid).await.unwrap();
            assert!(
                strict
                    .send_peer("peer-left", "peer-right", "invalid", "instruction")
                    .await
                    .is_err(),
                "{defect}"
            );
            let targets = strict.list_peer_targets("peer-left").await.unwrap();
            assert!(
                !targets
                    .targets
                    .iter()
                    .any(|target| target.session_id == "peer-right"),
                "{defect}"
            );
            memory.save_session(&original).await.unwrap();
        }
        assert!(activation.calls.lock().await.is_empty());
        assert_eq!(
            messenger
                .inbox()
                .inspect("peer-left")
                .await
                .unwrap()
                .generation,
            0
        );
    }

    #[tokio::test]
    async fn peer_api_discovery_is_bounded_and_sender_is_runtime_selected() {
        let (_temp, _store, activation, messenger) = fixture().await;
        let memory = Arc::new(PeerMemoryStorage::default());
        let root = Session::new("many-peers", "model");
        memory.save_session(&root).await.unwrap();
        for index in 0..MAX_PEER_TARGETS + 2 {
            let child = Session::new_child_of(format!("peer-{index:03}"), &root, "model", "Peer");
            memory.save_session(&child).await.unwrap();
        }
        let bounded = SessionMessenger::new(
            memory.clone(),
            messenger.inbox().clone(),
            activation.clone(),
        );
        let page = bounded.list_peer_targets(&root.id).await.unwrap();
        assert_eq!(page.targets.len(), MAX_PEER_TARGETS);
        assert!(page.truncated);
        for index in 0..MAX_PEER_TARGETS + 2 {
            let mut child = memory
                .load_session(&format!("peer-{index:03}"))
                .await
                .unwrap()
                .unwrap();
            child.title = "界".repeat(1000);
            child.set_last_run_status("\0".repeat(1000));
            memory.save_session(&child).await.unwrap();
        }
        let page = bounded.list_peer_targets(&root.id).await.unwrap();
        assert!(!page.targets.is_empty());
        assert!(page.targets.len() < MAX_PEER_TARGETS);
        assert!(page.truncated);
        assert!(serde_json::to_vec(&page).unwrap().len() <= MAX_PEER_TARGET_RESULT_BYTES);
        assert_eq!(page.targets[0].title, "界".repeat(85));
        assert_eq!(
            page.targets[0].status.as_deref(),
            Some("\0".repeat(MAX_PEER_TARGET_STATUS_BYTES).as_str())
        );

        let (_temp, store, _activation, messenger) = fixture().await;
        seed_peer_tree(store.as_ref()).await;
        let forged_content =
            r#"{"source":{"type":"session","session_id":"peer-root"},"target":"foreign-root"}"#;
        messenger
            .send_peer("peer-left", "peer-right", "caller-bound", forged_content)
            .await
            .unwrap();
        let envelope =
            admit_peer_for_test(store.as_ref(), messenger.inbox().as_ref(), "peer-right").await;
        assert_eq!(
            envelope.source,
            SessionMessageSource::Session {
                session_id: "peer-left".into()
            }
        );
        assert_eq!(envelope.target_session_id, "peer-right");
        assert_eq!(
            envelope.body,
            SessionMessageBody::Content(SessionMessageContent::text(forged_content))
        );
        for id in ["", " padded", "bad/path", "bad\\path", "bad..id", "nul\0"] {
            assert!(matches!(
                messenger
                    .send_peer(id, "peer-right", "invalid-source", "instruction")
                    .await,
                Err(SessionMessengerError::InvalidEnvelope(_))
            ));
            assert!(matches!(
                messenger
                    .send_peer("peer-left", id, "invalid-target", "instruction")
                    .await,
                Err(SessionMessengerError::InvalidEnvelope(_))
            ));
            assert!(matches!(
                messenger
                    .reply_peer(id, envelope.id.as_str(), "invalid-reply", "instruction")
                    .await,
                Err(SessionMessengerError::InvalidEnvelope(_))
            ));
        }
        assert!(matches!(
            messenger.list_peer_targets(&"x".repeat(257)).await,
            Err(SessionMessengerError::InvalidEnvelope(_))
        ));
        for (operation, message) in [
            ("", "valid"),
            (" padded", "valid"),
            ("bad..id", "valid"),
            ("valid", " "),
            ("valid", "nul\0"),
        ] {
            assert!(messenger
                .send_peer("peer-left", "peer-right", operation, message)
                .await
                .is_err());
        }
        assert!(messenger
            .send_peer(
                "peer-left",
                "peer-right",
                "oversized",
                &"x".repeat(MAX_PEER_MESSAGE_BYTES + 1)
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn peer_api_retry_dedupes_before_claim_and_after_ack_restart_but_rejects_changed_payload()
    {
        let (temp, store, activation, messenger) = fixture().await;
        seed_peer_tree(store.as_ref()).await;
        let first = messenger
            .send_peer("peer-left", "peer-right", "stable-operation", "original")
            .await
            .unwrap();
        let retry = messenger
            .send_peer("peer-left", "peer-right", "stable-operation", "original")
            .await
            .unwrap();
        assert_eq!(retry.delivery, first.delivery);
        assert!(matches!(
            messenger
                .send_peer("peer-left", "peer-right", "stable-operation", "changed")
                .await,
            Err(SessionMessengerError::Inbox(
                SessionInboxError::InvalidClaim(_)
            ))
        ));
        assert_eq!(
            messenger
                .inbox()
                .inspect("peer-right")
                .await
                .unwrap()
                .generation,
            1
        );
        admit_peer_for_test(store.as_ref(), messenger.inbox().as_ref(), "peer-right").await;
        drop(messenger);
        drop(store);
        let reopened = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
        let inbox = Arc::new(FileSessionInbox::new(
            reopened.clone(),
            SessionInboxLimits::default(),
        ));
        let messenger = SessionMessenger::new(reopened, inbox.clone(), activation);
        let after_ack = messenger
            .send_peer("peer-left", "peer-right", "stable-operation", "original")
            .await
            .unwrap();
        assert_eq!(after_ack.delivery, first.delivery);
        assert!(inbox.claim("peer-right", 16).await.unwrap().is_empty());
        assert!(matches!(
            messenger
                .send_peer("peer-left", "peer-right", "stable-operation", "changed")
                .await,
            Err(SessionMessengerError::Inbox(
                SessionInboxError::InvalidClaim(_)
            ))
        ));
        assert_eq!(inbox.inspect("peer-right").await.unwrap().generation, 1);
    }

    #[tokio::test]
    async fn peer_api_reply_recovers_durable_recipient_and_thread_after_restart() {
        let (temp, store, activation, messenger) = fixture().await;
        seed_peer_tree(store.as_ref()).await;
        let first = messenger
            .send_peer("peer-left-leaf", "peer-right-leaf", "initial", "question")
            .await
            .unwrap();
        let incoming = admit_peer_for_test(
            store.as_ref(),
            messenger.inbox().as_ref(),
            "peer-right-leaf",
        )
        .await;
        drop(messenger);
        drop(store);
        let store = Arc::new(SessionStoreV2::new(temp.path().into()).await.unwrap());
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let messenger = SessionMessenger::new(store.clone(), inbox.clone(), activation);
        let reply = messenger
            .reply_peer(
                "peer-right-leaf",
                first.delivery.id.as_str(),
                "answer",
                "answer text",
            )
            .await
            .unwrap();
        let retry = messenger
            .reply_peer(
                "peer-right-leaf",
                first.delivery.id.as_str(),
                "answer",
                "answer text",
            )
            .await
            .unwrap();
        assert_eq!(reply.delivery, retry.delivery);
        let response = admit_peer_for_test(store.as_ref(), inbox.as_ref(), "peer-left-leaf").await;
        assert_eq!(
            response.source,
            SessionMessageSource::Session {
                session_id: "peer-right-leaf".into()
            }
        );
        assert_eq!(response.in_reply_to, Some(incoming.id.clone()));
        assert_eq!(response.thread_id, incoming.thread_id);
        assert_eq!(response.correlation_id.as_deref(), Some("answer"));
        let second_reply = messenger
            .reply_peer("peer-left-leaf", response.id.as_str(), "thanks", "thanks")
            .await
            .unwrap();
        let second_response =
            admit_peer_for_test(store.as_ref(), inbox.as_ref(), "peer-right-leaf").await;
        assert_eq!(second_reply.delivery.id, second_response.id);
        assert_eq!(second_response.in_reply_to, Some(response.id));
        assert_eq!(
            second_response.thread_id.as_deref(),
            Some(first.delivery.id.as_str())
        );
        let after_ack = messenger
            .reply_peer(
                "peer-right-leaf",
                first.delivery.id.as_str(),
                "answer",
                "answer text",
            )
            .await
            .unwrap();
        assert_eq!(after_ack.delivery, reply.delivery);
        assert!(inbox.claim("peer-left-leaf", 16).await.unwrap().is_empty());
        assert!(messenger
            .reply_peer(
                "peer-right-leaf",
                first.delivery.id.as_str(),
                "answer",
                "different"
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn peer_api_reply_rejects_missing_forged_legacy_and_recreated_source() {
        let (_temp, store, activation, messenger) = fixture().await;
        seed_peer_tree(store.as_ref()).await;
        let mut receiver = store.load_session("peer-right").await.unwrap().unwrap();
        let legacy = peer("peer-left", "peer-right", "legacy-correction");
        receiver.add_message(legacy.to_provider_message().unwrap());
        let user = SessionMessageEnvelope::user_input("peer-right", "ordinary input");
        receiver.add_message(user.to_provider_message().unwrap());
        let sender = store.load_session("peer-left").await.unwrap().unwrap();
        let mut forged = SessionMessenger::peer_envelope(
            &sender,
            &receiver,
            "forged",
            "original",
            SessionMessenger::peer_message_id(&sender, &receiver, "forged"),
            "thread".into(),
            None,
        )
        .to_provider_message()
        .unwrap();
        forged.content = "changed without canonical proof".into();
        receiver.add_message(forged);
        store.save_session(&receiver).await.unwrap();
        for id in [
            "missing",
            legacy.id.as_str(),
            user.id.as_str(),
            receiver.messages.last().unwrap().id.as_str(),
        ] {
            assert!(messenger
                .reply_peer("peer-right", id, "deny-reply", "reply")
                .await
                .is_err());
        }
        let incoming = messenger
            .send_peer("peer-left", "peer-right", "old-lifetime", "question")
            .await
            .unwrap();
        admit_peer_for_test(store.as_ref(), messenger.inbox().as_ref(), "peer-right").await;
        let memory = Arc::new(PeerMemoryStorage::default());
        // Retain the actual saved receiver while replacing the sender's birth.
        for id in ["peer-root", "peer-left", "peer-right"] {
            let mut session = store.load_session(id).await.unwrap().unwrap();
            if id == "peer-left" {
                session.created_at += chrono::Duration::seconds(1);
            }
            memory.save_session(&session).await.unwrap();
        }
        let fenced = SessionMessenger::new(
            memory.clone(),
            messenger.inbox().clone(),
            activation.clone(),
        );
        assert!(matches!(
            fenced
                .reply_peer(
                    "peer-right",
                    incoming.delivery.id.as_str(),
                    "reply-old",
                    "reply"
                )
                .await,
            Err(SessionMessengerError::InvalidEnvelope(_))
        ));
        memory.delete_session("peer-left").await.unwrap();
        assert!(matches!(
            fenced
                .reply_peer(
                    "peer-right",
                    incoming.delivery.id.as_str(),
                    "reply-deleted",
                    "reply"
                )
                .await,
            Err(SessionMessengerError::TargetNotFound(_))
        ));
        assert_eq!(activation.calls.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn same_root_delivery_enqueues_then_requests_activation() {
        let (_temp, store, activation, messenger) = fixture().await;
        let root = Session::new("root", "model");
        let mut child = Session::new("child", "model");
        child.kind = bamboo_domain::SessionKind::Child;
        child.parent_session_id = Some("root".to_string());
        child.root_session_id = "root".to_string();
        store.save_session(&root).await.unwrap();
        store.save_session(&child).await.unwrap();

        let receipt = messenger
            .send(peer("root", "child", "msg-1"))
            .await
            .unwrap();
        assert_eq!(receipt.delivery.generation, 1);
        assert_eq!(
            activation.calls.lock().await.as_slice(),
            &[("child".to_string(), 1)]
        );
        assert_eq!(messenger.metrics().snapshot().delivered, 1);
    }

    #[tokio::test]
    async fn staged_delivery_without_watermark_reports_exact_retry_required() {
        let temp = TempDir::new().unwrap();
        let store = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let root = Session::new("root", "model");
        let mut child = Session::new("child", "model");
        child.kind = bamboo_domain::SessionKind::Child;
        child.parent_session_id = Some("root".to_string());
        child.root_session_id = "root".to_string();
        store.save_session(&root).await.unwrap();
        store.save_session(&child).await.unwrap();
        let inbox = Arc::new(FailEligibilityOnce {
            inner: Arc::new(FileSessionInbox::new(
                store.clone(),
                SessionInboxLimits::default(),
            )),
            fail_next: std::sync::atomic::AtomicBool::new(true),
        });
        let activation = Arc::new(RecordingActivation {
            calls: tokio::sync::Mutex::new(Vec::new()),
        });
        let messenger = SessionMessenger::new(store.clone(), inbox, activation.clone());
        let envelope = peer("root", "child", "watermark-failure");
        let gate = bamboo_domain::AdmissionGate::default();

        let admission = messenger
            .admit_with_gate(envelope.clone(), Some(&gate))
            .await
            .unwrap();
        let receipt = match messenger.activate(&admission).await.unwrap_err() {
            SessionMessengerError::ActivationEligibility { receipt, .. } => receipt,
            error => panic!("expected retry-required classification: {error}"),
        };
        assert!(gate.is_committed());
        assert!(activation.calls.lock().await.is_empty());
        let reopened = FileSessionInbox::new(store, SessionInboxLimits::default());
        let backlog = reopened.inspect("child").await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 1);
        assert_eq!(backlog.activation_generation, 0);
        assert_eq!(backlog.generation, receipt.generation);

        let retry = messenger
            .admit_with_gate(envelope, Some(&gate))
            .await
            .unwrap();
        let retried = messenger.activate(&retry).await.unwrap();
        assert_eq!(retried.delivery, receipt);
        assert_eq!(activation.calls.lock().await.len(), 1);
        let reopened = reopened.inspect("child").await.unwrap();
        assert_eq!(reopened.activation_generation, receipt.generation);
        assert_eq!(reopened.pending + reopened.claimed, 1);
    }

    #[tokio::test]
    async fn cross_root_peer_is_rejected_before_enqueue() {
        let (_temp, store, activation, messenger) = fixture().await;
        store
            .save_session(&Session::new("root-a", "model"))
            .await
            .unwrap();
        store
            .save_session(&Session::new("root-b", "model"))
            .await
            .unwrap();

        let error = messenger
            .send(peer("root-a", "root-b", "msg-2"))
            .await
            .unwrap_err();
        assert!(matches!(error, SessionMessengerError::Unauthorized { .. }));
        assert!(activation.calls.lock().await.is_empty());
        let metrics = messenger.metrics().snapshot();
        assert_eq!(metrics.rejected, 1);
        assert_eq!(metrics.unauthorized, 1);
        assert_eq!(metrics.payload_too_large, 0);
        assert_eq!(metrics.backlog_full, 0);
    }

    #[tokio::test]
    async fn same_root_different_project_peer_is_rejected_before_enqueue() {
        let (_temp, store, activation, messenger) = fixture().await;
        let mut source = Session::new("project-root", "model");
        source.set_project_id_meta("project-a");
        let mut target = Session::new("project-child", "model");
        target.kind = bamboo_domain::SessionKind::Child;
        target.parent_session_id = Some(source.id.clone());
        target.root_session_id = source.id.clone();
        target.set_project_id_meta("project-b");
        store.save_session(&source).await.unwrap();
        store.save_session(&target).await.unwrap();

        let error = messenger
            .send(peer(&source.id, &target.id, "different-project"))
            .await
            .unwrap_err();
        assert!(matches!(error, SessionMessengerError::Unauthorized { .. }));
        assert!(activation.calls.lock().await.is_empty());
        let metrics = messenger.metrics().snapshot();
        assert_eq!(metrics.delivered, 0);
        assert_eq!(metrics.rejected, 1);
        assert_eq!(metrics.unauthorized, 1);
    }

    #[tokio::test]
    async fn limit_rejections_have_distinct_metrics() {
        let temp = TempDir::new().unwrap();
        let store = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        store
            .save_session(&Session::new("target", "model"))
            .await
            .unwrap();
        let activation = Arc::new(RecordingActivation {
            calls: tokio::sync::Mutex::new(Vec::new()),
        });
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits {
                max_payload_bytes: 512,
                max_backlog: 1,
                max_claim_batch: 1,
            },
        ));
        let messenger = SessionMessenger::new(store, inbox, activation.clone());

        let oversized = SessionMessageEnvelope::user_input("target", "x".repeat(2048));
        assert!(matches!(
            messenger.send(oversized).await,
            Err(SessionMessengerError::Inbox(
                SessionInboxError::PayloadTooLarge { .. }
            ))
        ));
        messenger
            .send(SessionMessageEnvelope::user_input("target", "first"))
            .await
            .unwrap();
        assert!(matches!(
            messenger
                .send(SessionMessageEnvelope::user_input("target", "second"))
                .await,
            Err(SessionMessengerError::Inbox(
                SessionInboxError::BacklogFull { .. }
            ))
        ));

        let metrics = messenger.metrics().snapshot();
        assert_eq!(metrics.rejected, 2);
        assert_eq!(metrics.payload_too_large, 1);
        assert_eq!(metrics.backlog_full, 1);
        assert_eq!(activation.calls.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn explicit_peer_intent_keeps_legacy_compatibility_and_fences_changed_retry_after_ack() {
        let (_temp, store, activation, messenger) = fixture().await;
        seed_peer_tree(store.as_ref()).await;
        let receipt = messenger
            .send_peer_with_intent(
                "peer-left",
                "peer-right",
                "queue-operation",
                "input",
                SessionMessageDeliveryIntent::QueueOnly,
            )
            .await
            .unwrap();
        assert_eq!(receipt.activation, None);
        assert!(!receipt.active_delivery_notified);
        assert!(activation.calls.lock().await.is_empty());
        let original =
            admit_peer_for_test(store.as_ref(), messenger.inbox().as_ref(), "peer-right").await;
        assert_eq!(
            messenger
                .send_peer_with_intent(
                    "peer-left",
                    "peer-right",
                    "queue-operation",
                    "input",
                    SessionMessageDeliveryIntent::QueueOnly
                )
                .await
                .unwrap()
                .delivery,
            receipt.delivery
        );
        assert!(messenger
            .send_peer_with_intent(
                "peer-left",
                "peer-right",
                "queue-operation",
                "input",
                SessionMessageDeliveryIntent::TriggerTurn
            )
            .await
            .is_err());
        assert!(activation.calls.lock().await.is_empty());
        let reply = messenger
            .reply_peer_with_intent(
                "peer-right",
                original.id.as_str(),
                "reply-operation",
                "reply",
                SessionMessageDeliveryIntent::TriggerTurn,
            )
            .await
            .unwrap();
        // This fixture records activation requests as reservations. Active
        // owner delivery is covered by the real router's safe-point tests.
        assert_eq!(
            reply.activation,
            Some(SessionActivationDisposition::ActivationReserved)
        );
        assert_eq!(
            activation.calls.lock().await.as_slice(),
            &[("peer-left".to_string(), reply.delivery.generation)]
        );
        let claims = messenger.inbox().claim("peer-left", 8).await.unwrap();
        assert_eq!(claims[0].envelope.in_reply_to, Some(original.id));
        assert_eq!(
            claims[0].activation_policy,
            SessionActivationPolicy::RespectSpecificWait
        );
        messenger
            .send_peer("peer-left", "peer-right", "legacy-operation", "legacy")
            .await
            .unwrap();
        let claims = messenger.inbox().claim("peer-right", 8).await.unwrap();
        assert_eq!(
            claims[0].activation_policy,
            SessionActivationPolicy::InterruptSpecificWait
        );
    }

    #[tokio::test]
    async fn explicit_peer_intents_deny_foreign_roots_before_admission_or_wake() {
        let (_temp, store, activation, messenger) = fixture().await;
        seed_peer_tree(store.as_ref()).await;
        store
            .save_session(&Session::new("foreign-root", "model"))
            .await
            .unwrap();
        for intent in [
            SessionMessageDeliveryIntent::QueueOnly,
            SessionMessageDeliveryIntent::TriggerTurn,
        ] {
            assert!(matches!(
                messenger
                    .send_peer_with_intent("peer-left", "foreign-root", "denied", "input", intent)
                    .await,
                Err(SessionMessengerError::Unauthorized { .. })
            ));
        }
        assert_eq!(
            messenger
                .inbox()
                .inspect("foreign-root")
                .await
                .unwrap()
                .pending,
            0
        );
        assert!(activation.calls.lock().await.is_empty());
    }
}
