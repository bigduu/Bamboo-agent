//! Internal logical-session messaging service.
//!
//! This module deliberately has no HTTP concepts. User transports, tools,
//! actor adapters, the SDK, and runtime coordinators all submit the same typed
//! envelope addressed by stable [`Session::id`](bamboo_domain::Session).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bamboo_agent_core::storage::Storage;
use bamboo_domain::{
    Session, SessionActivationDisposition, SessionActivationError, SessionActivationPolicy,
    SessionActivationPort, SessionInboxError, SessionInboxPort, SessionInboxReceipt,
    SessionMessageEnvelope, SessionMessageSource,
};

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
            SessionMessengerError::Activation { .. } => {}
        }
    }

    pub async fn admit(
        &self,
        envelope: SessionMessageEnvelope,
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

        let delivery = match self.inbox.deliver(&envelope).await {
            Ok(receipt) => receipt,
            Err(error) => {
                let error = SessionMessengerError::Inbox(error);
                self.record_rejection(&error);
                return Err(error);
            }
        };
        self.metrics.delivered.fetch_add(1, Ordering::Relaxed);
        self.metrics.delivery_latency_micros.fetch_add(
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        Ok(SessionMessengerAdmission {
            envelope_id: envelope.id.as_str().to_string(),
            target_session_id: envelope.target_session_id,
            delivery,
        })
    }

    pub async fn activate(
        &self,
        admission: &SessionMessengerAdmission,
    ) -> Result<SessionMessengerReceipt, SessionMessengerError> {
        if let Err(error) = self
            .inbox
            .mark_activation_eligible(
                &admission.target_session_id,
                admission.delivery.generation,
                SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
        {
            self.metrics
                .activation_failed
                .fetch_add(1, Ordering::Relaxed);
            return Err(SessionMessengerError::Activation {
                receipt_id: admission.delivery.id.to_string(),
                receipt: admission.delivery.clone(),
                source: SessionActivationError::Internal(format!(
                    "persist activation watermark: {error}"
                )),
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

    pub async fn send(
        &self,
        envelope: SessionMessageEnvelope,
    ) -> Result<SessionMessengerReceipt, SessionMessengerError> {
        let admission = self.admit(envelope).await?;
        self.activate(&admission).await
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
}
