//! Bridge between structured [`AgentRuntimeState`] and session metadata.
//!
//! The read path prefers the structured `session.agent_runtime_state` field
//! and falls back to the legacy `session.metadata["agent.runtime.state"]` key.
//! The write path writes only to the structured field; the legacy metadata
//! mirror is no longer maintained.

use std::sync::Arc;

use bamboo_agent_core::Session;
use bamboo_domain::{
    AgentRuntimeState, SessionInboxPort, SessionMessageBody, SessionMessageContent,
    SessionMessageEnvelope, SessionMessageId, SessionMessageKind, SessionMessageSource,
    SessionRuntimeInstruction,
};

const METADATA_KEY: &str = "agent.runtime.state";
#[cfg(test)]
const PENDING_INJECTED_MESSAGES_KEY: &str = "pending_injected_messages";

/// Read `AgentRuntimeState` from session.
///
/// Tries the structured field first, falls back to the metadata key.
#[allow(dead_code)]
pub fn read_runtime_state(session: &Session) -> Option<AgentRuntimeState> {
    session.agent_runtime_state.clone().or_else(|| {
        session
            .metadata
            .get(METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<AgentRuntimeState>(raw).ok())
    })
}

/// Write `AgentRuntimeState` to the structured session field.
///
/// Only writes to `session.agent_runtime_state`. The legacy metadata mirror
/// was removed after the migration completed.
pub fn write_runtime_state(session: &mut Session, state: &AgentRuntimeState) {
    session.agent_runtime_state = Some(state.clone());
}

/// Sync runtime state fields from existing metadata keys.
///
/// This extracts values that are currently stored as individual metadata
/// entries into the structured runtime state.
pub fn sync_from_metadata(session: &Session, state: &mut AgentRuntimeState) {
    // LLM info
    if state.llm.model_name.is_none() {
        state.llm.model_name = Some(session.model.clone());
    }
    if state.llm.provider_name.is_none() {
        state.llm.provider_name = session.provider_name();
    }
    if state.llm.responses_previous_id.is_none() {
        state.llm.responses_previous_id = session
            .metadata
            .get("responses.previous_response_id")
            .cloned();
    }

    // Prompt info
    if state.prompt.composer_version.is_none() {
        state.prompt.composer_version = session
            .metadata
            .get("runtime_prompt_composer_version")
            .cloned();
    }
    if state.prompt.section_flags.is_none() {
        state.prompt.section_flags = session
            .metadata
            .get("runtime_prompt_component_flags")
            .cloned();
    }
    if state.prompt.section_lengths.is_none() {
        state.prompt.section_lengths = session
            .metadata
            .get("runtime_prompt_component_lengths")
            .cloned();
    }
    if state.prompt.section_layout.is_none() {
        state.prompt.section_layout = session
            .metadata
            .get("runtime_prompt_section_layout")
            .cloned();
    }
}

/// Result of a turn-boundary disk refresh: how many injected messages were
/// merged, and the live per-session permission mode as it stands on
/// disk (the authoritative writer is `PATCH /sessions`).
#[derive(Debug, Default, Clone, Copy)]
pub struct TurnBoundaryRefresh {
    pub merged: usize,
    /// `None` when there was no storage / no on-disk session to read.
    pub disk_permission_mode: Option<bamboo_domain::SessionPermissionMode>,
}

/// Result of migrating only the rolling-upgrade legacy ingress queue.
///
/// Terminal execution paths use this without claiming the typed inbox: every
/// returned generation still needs a successor reasoning turn.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LegacyPendingMigration {
    pub delivered: usize,
    pub source_cleared: bool,
    pub highest_generation: Option<u64>,
}

/// Merge any queued follow-up messages that were injected via `send_message`
/// while a session is running.
///
/// Thin wrapper over [`refresh_turn_boundary_from_disk`] kept for callers that
/// only need the merge count. Returns the number of messages merged.
#[cfg(test)]
pub async fn merge_pending_injected_messages(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
) -> usize {
    refresh_turn_boundary_from_disk(session, storage, persistence)
        .await
        .merged
}

fn legacy_envelope(
    session_id: &str,
    index: usize,
    value: &serde_json::Value,
) -> SessionMessageEnvelope {
    let content = value
        .get("content")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| value.to_string());
    SessionMessageEnvelope {
        id: SessionMessageId::legacy(session_id, index, value),
        source: SessionMessageSource::Runtime {
            subsystem: "legacy_pending_injected_messages".to_string(),
        },
        target_session_id: session_id.to_string(),
        kind: SessionMessageKind::RuntimeInstruction,
        body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
            instruction: "legacy_pending_injected_message".to_string(),
            content: Some(SessionMessageContent::text(content)),
            // Preserve the complete legacy value for audit/replay even when it
            // contained fields the old bridge ignored.
            data: Some(value.clone()),
            provider_message: None,
        }),
        created_at: chrono::Utc::now(),
        thread_id: None,
        in_reply_to: None,
        attempt: None,
        correlation_id: Some("legacy_pending_injected_messages".to_string()),
    }
}

async fn migrate_legacy_pending_messages(
    session_id: &str,
    messages: &[serde_json::Value],
    inbox: &Arc<dyn SessionInboxPort>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
) -> LegacyPendingMigration {
    if messages.is_empty() {
        return LegacyPendingMigration {
            source_cleared: true,
            ..LegacyPendingMigration::default()
        };
    }
    tracing::info!(
        telemetry_event = "session_inbox.legacy_pending_ingress",
        session_id,
        count = messages.len(),
        "observed rolling-upgrade legacy pending-message ingress"
    );
    let mut migration = LegacyPendingMigration::default();
    for (index, value) in messages.iter().enumerate() {
        let envelope = legacy_envelope(session_id, index, value);
        match inbox.deliver(&envelope).await {
            Ok(receipt) => {
                migration.delivered += 1;
                // The legacy source is the only restart signal until its
                // coordinated CAS-clear. Authorize every durable prefix before
                // that source can be removed, so a crash after clear can never
                // leave a pending typed message behind an activation watermark
                // of zero. Marking does not itself start another loop: the
                // live caller claims below, while terminal callers hand the
                // generation to the activation router.
                if let Err(error) = inbox
                    .mark_activation_eligible(
                        session_id,
                        receipt.generation,
                        bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
                    )
                    .await
                {
                    tracing::warn!(
                        session_id,
                        message_id = %envelope.id,
                        generation = receipt.generation,
                        %error,
                        "legacy pending message migration stopped before source cleanup because activation authorization failed"
                    );
                    return migration;
                }
                migration.highest_generation = Some(
                    migration
                        .highest_generation
                        .map_or(receipt.generation, |current| {
                            current.max(receipt.generation)
                        }),
                );
            }
            Err(error) => {
                // Some earlier entries may already be durable. Their
                // deterministic ids make the next bounded retry safe; never
                // clear the source queue unless every enqueue completed.
                tracing::warn!(
                    session_id,
                    message_id = %envelope.id,
                    %error,
                    "legacy pending message migration stopped before source cleanup"
                );
                return migration;
            }
        }
    }

    let Some(persistence) = persistence else {
        tracing::warn!(
            session_id,
            "legacy pending messages were delivered but cannot be cleared without coordinated persistence"
        );
        return migration;
    };
    match persistence
        .clear_legacy_pending_messages(session_id, messages)
        .await
    {
        Ok(true) => {
            tracing::info!(
                session_id,
                count = messages.len(),
                "migrated legacy pending messages into SessionInbox"
            );
            migration.source_cleared = true;
        }
        Ok(false) => {
            tracing::debug!(
                session_id,
                "legacy pending source changed during migration; leaving it for a deterministic retry"
            );
        }
        Err(error) => {
            tracing::warn!(
                session_id,
                %error,
                "legacy pending messages are durable but source cleanup failed"
            );
        }
    }
    migration
}

/// Terminal-window compatibility migration.
///
/// Loads the latest durable legacy queue, enqueues it into the typed inbox and
/// clears it with the same CAS protocol as a normal turn boundary, but never
/// claims or acknowledges typed work. The caller must hand
/// `highest_generation` to the activation router before finalization.
pub async fn migrate_legacy_pending_only(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
    inbox: Option<&Arc<dyn SessionInboxPort>>,
) -> LegacyPendingMigration {
    let (Some(storage), Some(inbox)) = (storage, inbox) else {
        return LegacyPendingMigration::default();
    };
    let latest = match storage.load_session(&session.id).await {
        Ok(Some(latest)) => latest,
        Ok(None) => return LegacyPendingMigration::default(),
        Err(error) => {
            tracing::warn!(
                session_id = %session.id,
                %error,
                "terminal legacy SessionInbox migration could not load session"
            );
            return LegacyPendingMigration::default();
        }
    };
    let Some(messages) = latest.pending_injected_messages() else {
        return LegacyPendingMigration::default();
    };
    let migration =
        migrate_legacy_pending_messages(&session.id, &messages, inbox, persistence).await;
    if migration.source_cleared
        && session.pending_injected_messages().as_deref() == Some(messages.as_slice())
    {
        session.clear_pending_injected_messages();
    }
    migration
}

async fn admit_session_inbox(
    session: &mut Session,
    inbox: &Arc<dyn SessionInboxPort>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
) -> usize {
    let Some(persistence) = persistence else {
        tracing::warn!(
            session_id = %session.id,
            "SessionInbox cannot admit without durable runtime persistence"
        );
        return 0;
    };
    let claims = match inbox.claim(&session.id, 128).await {
        Ok(claims) => claims,
        Err(error) => {
            tracing::warn!(session_id = %session.id, %error, "failed to claim SessionInbox");
            return 0;
        }
    };

    let mut admitted = 0usize;
    for claim in claims {
        let permanently_admitted = match inbox.was_admitted(&session.id, &claim.envelope.id).await {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    session_id = %session.id,
                    message_id = %claim.envelope.id,
                    %error,
                    "failed to inspect SessionInbox admitted receipt"
                );
                break;
            }
        };
        if permanently_admitted {
            let durable = match persistence.load_runtime_session(&session.id).await {
                Ok(Some(durable)) => durable,
                Ok(None) => {
                    tracing::error!(
                        session_id = %session.id,
                        message_id = %claim.envelope.id,
                        "permanent SessionInbox receipt has no durable transcript to verify; leaving claim recoverable"
                    );
                    break;
                }
                Err(error) => {
                    tracing::warn!(
                        session_id = %session.id,
                        message_id = %claim.envelope.id,
                        %error,
                        "failed to load durable transcript for permanent SessionInbox receipt"
                    );
                    break;
                }
            };
            let Some(message) = durable
                .messages
                .iter()
                .find(|message| {
                    bamboo_domain::is_matching_session_message(message, &claim.envelope)
                })
                .cloned()
            else {
                // Receipt-without-transcript is a damaged/incomplete commit.
                // Keep cur as the only recoverable copy. Cursor membership is
                // not required here because it is intentionally bounded.
                tracing::error!(
                    session_id = %session.id,
                    message_id = %claim.envelope.id,
                    "permanent SessionInbox receipt lacks matching typed transcript proof; leaving claim recoverable"
                );
                break;
            };
            if let Some(existing) = session
                .messages
                .iter_mut()
                .find(|existing| existing.id == message.id)
            {
                *existing = message;
            } else {
                session.add_message(message);
            }
            bamboo_domain::merge_session_inbox_admission(session, &durable);
            if let Err(error) = inbox.ack(&session.id, &claim).await {
                tracing::warn!(
                    session_id = %session.id,
                    message_id = %claim.envelope.id,
                    %error,
                    "failed to remove permanently admitted duplicate"
                );
                break;
            }
            continue;
        }

        let transcript_has_id = session
            .messages
            .iter()
            .any(|message| bamboo_domain::is_matching_session_message(message, &claim.envelope));
        let transcript_has_conflicting_id = session
            .messages
            .iter()
            .any(|message| message.id == claim.envelope.id.as_str())
            && !transcript_has_id;
        if transcript_has_conflicting_id {
            tracing::error!(
                session_id = %session.id,
                message_id = %claim.envelope.id,
                "SessionInbox id collides with a non-matching transcript message; leaving claim recoverable"
            );
            break;
        }
        let cursor_has_id = session
            .session_inbox_admission()
            .is_some_and(|state| state.contains(&claim.envelope.id));
        if cursor_has_id && !transcript_has_id {
            // Fail closed: an admitted cursor without its transcript message is
            // an invariant violation. Never establish a permanent tombstone
            // that could discard the only recoverable queue copy.
            tracing::error!(
                session_id = %session.id,
                message_id = %claim.envelope.id,
                "SessionInbox cursor exists without transcript message; leaving claim recoverable"
            );
            break;
        }

        let before = session.clone();
        if !transcript_has_id {
            match claim.envelope.to_provider_message() {
                Ok(message) => session.add_message(message),
                Err(error) => {
                    tracing::warn!(
                        session_id = %session.id,
                        message_id = %claim.envelope.id,
                        %error,
                        "typed SessionInbox envelope failed provider translation"
                    );
                    break;
                }
            }
        }
        session
            .session_inbox_admission_mut()
            .record(claim.envelope.id.clone(), claim.generation);
        session.updated_at = chrono::Utc::now();

        // The provider-valid Message and bounded cursor live in the same
        // session.json atomic checkpoint. Only after that succeeds may ack
        // create the permanent admitted-id receipt and remove the claim.
        if let Err(error) = persistence.checkpoint_runtime_session(session).await {
            *session = before;
            tracing::warn!(
                session_id = %session.id,
                message_id = %claim.envelope.id,
                %error,
                "SessionInbox checkpoint failed; claim remains recoverable"
            );
            break;
        }
        if !session
            .messages
            .iter()
            .any(|message| bamboo_domain::is_matching_session_message(message, &claim.envelope))
        {
            *session = before;
            tracing::error!(
                session_id = %session.id,
                message_id = %claim.envelope.id,
                "SessionInbox checkpoint lost typed transcript proof to a concurrent id collision; leaving claim recoverable"
            );
            break;
        }
        if let Err(error) = inbox.ack(&session.id, &claim).await {
            tracing::warn!(
                session_id = %session.id,
                message_id = %claim.envelope.id,
                %error,
                "SessionInbox checkpoint succeeded but ack failed; dedupe will recover"
            );
            break;
        }
        if !transcript_has_id {
            admitted += 1;
        }
    }
    admitted
}

/// Turn-boundary refresh from the on-disk session: a SINGLE load that both
/// merges queued `send_message` injections AND reads the live typed permission
/// mode.
///
/// The mode read is what makes a mid-run
/// `PATCH /sessions {permission_mode|bypass_permissions}` take effect on the
/// CURRENT run: the run owns a `Session` value taken at spawn and never
/// otherwise re-reads storage, so the caller applies the returned
/// `disk_permission_mode` to the live runtime state at each round boundary. The
/// posture is folded into the existing per-round load so large parent sessions
/// aren't deserialized twice.
#[cfg(test)]
pub async fn refresh_turn_boundary_from_disk(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
) -> TurnBoundaryRefresh {
    refresh_turn_boundary_with_inbox(session, storage, persistence, None).await
}

/// Turn-boundary refresh with the first-class durable SessionInbox enabled.
pub async fn refresh_turn_boundary_with_inbox(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
    inbox: Option<&Arc<dyn SessionInboxPort>>,
) -> TurnBoundaryRefresh {
    let latest = match storage {
        Some(storage) => match storage.load_session(&session.id).await {
            Ok(latest) => latest,
            Err(error) => {
                tracing::warn!(
                    session_id = %session.id,
                    %error,
                    "turn-boundary session refresh failed"
                );
                None
            }
        },
        None => None,
    };

    // A disk copy with no runtime state carries no authoritative permission
    // mode — report `None` (unknown) so the caller leaves the live posture
    // untouched rather than force-disabling a legitimately permissive run.
    // #540/#770.
    let disk_permission_posture = latest
        .as_ref()
        .and_then(|latest| {
            latest
                .agent_runtime_state
                .as_ref()
                .map(|state| (latest, state.effective_permission_mode()))
        })
        .and_then(|(latest, disk_mode)| {
            let current_mode = session
                .agent_runtime_state
                .as_ref()
                .map(|state| state.effective_permission_mode())
                .unwrap_or_default();
            bamboo_domain::fresher_disk_permission_audit(
                current_mode,
                &session.metadata,
                disk_mode,
                &latest.metadata,
            )
            .map(|audit| (disk_mode, audit))
        });
    let disk_permission_mode = disk_permission_posture
        .as_ref()
        .map(|(disk_mode, _)| *disk_mode);

    if let Some(latest) = latest.as_ref() {
        // Keep the live snapshot's bounded audit record in the same revision as
        // the typed mode returned below. Runtime persistence also adopts these
        // keys under its session lock, so both the current round and any later
        // checkpoint observe one coherent permission transition.
        if let Some((_, audit)) = disk_permission_posture.as_ref() {
            audit.write_to(&mut session.metadata);
        }
        // A previous activation may have checkpointed and acked the claim
        // before this live snapshot was loaded/refreshed. Carry its durable
        // cursor forward so finalization never mistakes an admitted duplicate
        // for work requiring a successor activation.
        bamboo_domain::merge_session_inbox_admission(session, latest);
    }

    if let (Some(inbox), Some(messages)) = (
        inbox,
        latest
            .as_ref()
            .and_then(|latest| latest.pending_injected_messages()),
    ) {
        let migration =
            migrate_legacy_pending_messages(&session.id, &messages, inbox, persistence).await;
        if let Some(generation) = migration.highest_generation {
            // Migration durably authorized this prefix before it was allowed
            // to CAS-clear the compatibility source. Carry the generation on
            // the live snapshot to the owning entry point's terminal
            // handshake if admission below cannot checkpoint it.
            session.session_inbox_admission_mut().observe(generation);
        }
        // The coordinated CAS cleared the latest durable source, but this
        // running snapshot may still carry the same compatibility queue. Clear
        // that exact stale copy before the inbox checkpoint, otherwise it would
        // resurrect the source queue in session.json/runtime sidecar.
        if migration.source_cleared
            && session.pending_injected_messages().as_deref() == Some(messages.as_slice())
        {
            session.clear_pending_injected_messages();
        }
    }

    if let Some(inbox) = inbox {
        let merged = admit_session_inbox(session, inbox, persistence).await;
        return TurnBoundaryRefresh {
            merged,
            disk_permission_mode,
        };
    }

    let Some(latest) = latest else {
        return TurnBoundaryRefresh {
            merged: 0,
            disk_permission_mode,
        };
    };

    // Read via the typed accessor (prefers `runtime_metadata`, falls back to the
    // legacy `pending_injected_messages` JSON string; defensive on malformed).
    let Some(messages) = latest.pending_injected_messages() else {
        return TurnBoundaryRefresh {
            merged: 0,
            disk_permission_mode,
        };
    };

    let mut merged = 0usize;
    for msg in messages {
        if let Some(content) = msg.get("content").and_then(|v| v.as_str()) {
            session.add_message(bamboo_agent_core::Message::user(content.to_string()));
            merged += 1;
        }
    }

    if merged > 0 {
        // Clear on both planes (typed field + legacy string mirror).
        session.clear_pending_injected_messages();
        session.updated_at = chrono::Utc::now();

        let mut saved = false;
        if let Some(persistence) = persistence {
            match persistence.save_runtime_session(session).await {
                Ok(()) => saved = true,
                Err(error) => tracing::warn!(
                    "[{}] Failed to persist pending injected message cleanup: {}",
                    session.id,
                    error
                ),
            }
        }

        tracing::info!(
            "[{}] Merged {} injected message(s) from queued send_message at turn boundary",
            session.id,
            merged
        );

        // When the cleanup save ran, the adopting `save_runtime_session` stamped
        // the freshest on-disk bypass onto `session.agent_runtime_state` (so a
        // `PATCH` that landed DURING the merge is already reflected there) — read
        // it back from memory instead of a third full-session deserialization on
        // this steering hot path. Without a save, `session` holds no disk-fresh
        // value, so keep the pre-merge snapshot. #540.
        let disk_permission_mode = if saved {
            session
                .agent_runtime_state
                .as_ref()
                .map(|rs| rs.effective_permission_mode())
                .or(disk_permission_mode)
        } else {
            disk_permission_mode
        };
        return TurnBoundaryRefresh {
            merged,
            disk_permission_mode,
        };
    }

    TurnBoundaryRefresh {
        merged,
        disk_permission_mode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::storage::Storage;
    use bamboo_domain::{
        AgentStatusState, SessionActivationDisposition, SessionActivationError,
        SessionActivationPort,
    };
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::RwLock;

    fn test_session() -> Session {
        Session::new("test-session", "test-model")
    }

    #[test]
    fn read_from_structured_field() {
        let mut session = test_session();
        let mut state = AgentRuntimeState::new("run-1");
        state.status = AgentStatusState::Running;
        session.agent_runtime_state = Some(state.clone());

        let read = read_runtime_state(&session).unwrap();
        assert_eq!(read.status, AgentStatusState::Running);
        assert_eq!(read.run_id, "run-1");
    }

    #[test]
    fn read_from_metadata_fallback() {
        let mut session = test_session();
        let state = AgentRuntimeState::new("run-2");
        session.metadata.insert(
            METADATA_KEY.to_string(),
            serde_json::to_string(&state).unwrap(),
        );

        let read = read_runtime_state(&session).unwrap();
        assert_eq!(read.run_id, "run-2");
    }

    #[test]
    fn structured_field_takes_priority() {
        let mut session = test_session();
        let mut state1 = AgentRuntimeState::new("from-field");
        state1.status = AgentStatusState::Running;
        session.agent_runtime_state = Some(state1);

        let mut state2 = AgentRuntimeState::new("from-metadata");
        state2.status = AgentStatusState::Completed;
        session.metadata.insert(
            METADATA_KEY.to_string(),
            serde_json::to_string(&state2).unwrap(),
        );

        let read = read_runtime_state(&session).unwrap();
        assert_eq!(read.run_id, "from-field");
        assert_eq!(read.status, AgentStatusState::Running);
    }

    #[test]
    fn read_returns_none_when_empty() {
        let session = test_session();
        assert!(read_runtime_state(&session).is_none());
    }

    #[test]
    fn write_only_structured_field() {
        let mut session = test_session();
        let state = AgentRuntimeState::new("run-3");

        write_runtime_state(&mut session, &state);

        assert!(session.agent_runtime_state.is_some());
        // Legacy metadata mirror is no longer written
        assert!(!session.metadata.contains_key(METADATA_KEY));
        assert_eq!(
            session.agent_runtime_state.as_ref().unwrap().run_id,
            "run-3"
        );
    }

    #[test]
    fn sync_extracts_model_name() {
        let mut session = test_session();
        session.model = "gpt-4o".to_string();
        session.metadata.insert(
            "responses.previous_response_id".to_string(),
            "resp-123".to_string(),
        );

        let mut state = AgentRuntimeState::new("run-4");
        sync_from_metadata(&session, &mut state);

        assert_eq!(state.llm.model_name, Some("gpt-4o".to_string()));
        assert_eq!(
            state.llm.responses_previous_id,
            Some("resp-123".to_string())
        );
    }

    // --- Pending injected message tests ---

    #[derive(Default)]
    struct TestStorage {
        sessions: RwLock<HashMap<String, Session>>,
    }

    #[async_trait::async_trait]
    impl Storage for TestStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.sessions
                .write()
                .await
                .insert(session.id.clone(), session.clone());
            Ok(())
        }

        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            Ok(self.sessions.read().await.get(session_id).cloned())
        }

        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            Ok(self.sessions.write().await.remove(session_id).is_some())
        }
    }

    struct TestPersistence(Arc<dyn Storage>);

    #[async_trait::async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for TestPersistence {
        async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            self.0.save_session(session).await
        }
    }

    struct FaultingPersistence {
        inner: Arc<bamboo_storage::LockedSessionStore>,
        fail_checkpoint: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for FaultingPersistence {
        async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            self.inner.merge_save_runtime(session).await
        }

        async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            if self
                .fail_checkpoint
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(std::io::Error::other("injected checkpoint failure"));
            }
            self.inner.checkpoint_runtime_session(session).await
        }

        async fn load_runtime_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            self.inner.storage().load_session(session_id).await
        }
    }

    struct AckAfterPersistErrorInbox {
        inner: Arc<dyn SessionInboxPort>,
        fail_before_once: std::sync::atomic::AtomicBool,
        fail_after_once: std::sync::atomic::AtomicBool,
    }

    struct ForcedPermanentReceiptInbox {
        inner: Arc<dyn SessionInboxPort>,
        ack_count: Arc<AtomicUsize>,
    }

    struct FailSecondDeliveryInbox {
        inner: Arc<dyn SessionInboxPort>,
        deliveries: AtomicUsize,
    }

    struct FailActivationInbox {
        inner: Arc<dyn SessionInboxPort>,
        marks: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl SessionInboxPort for FailActivationInbox {
        async fn deliver(
            &self,
            envelope: &SessionMessageEnvelope,
        ) -> Result<bamboo_domain::SessionInboxReceipt, bamboo_domain::SessionInboxError> {
            self.inner.deliver(envelope).await
        }

        async fn mark_activation_eligible(
            &self,
            _target_session_id: &str,
            _generation: u64,
            _policy: bamboo_domain::SessionActivationPolicy,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            self.marks.fetch_add(1, Ordering::SeqCst);
            Err(bamboo_domain::SessionInboxError::Storage(
                "injected activation watermark failure".to_string(),
            ))
        }

        async fn claim(
            &self,
            target_session_id: &str,
            limit: usize,
        ) -> Result<Vec<bamboo_domain::SessionInboxClaim>, bamboo_domain::SessionInboxError>
        {
            self.inner.claim(target_session_id, limit).await
        }

        async fn was_admitted(
            &self,
            target_session_id: &str,
            id: &SessionMessageId,
        ) -> Result<bool, bamboo_domain::SessionInboxError> {
            self.inner.was_admitted(target_session_id, id).await
        }

        async fn ack(
            &self,
            target_session_id: &str,
            claim: &bamboo_domain::SessionInboxClaim,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            self.inner.ack(target_session_id, claim).await
        }

        async fn inspect(
            &self,
            target_session_id: &str,
        ) -> Result<bamboo_domain::SessionInboxBacklog, bamboo_domain::SessionInboxError> {
            self.inner.inspect(target_session_id).await
        }
    }

    #[async_trait::async_trait]
    impl SessionInboxPort for FailSecondDeliveryInbox {
        async fn deliver(
            &self,
            envelope: &SessionMessageEnvelope,
        ) -> Result<bamboo_domain::SessionInboxReceipt, bamboo_domain::SessionInboxError> {
            if self.deliveries.fetch_add(1, Ordering::SeqCst) == 1 {
                return Err(bamboo_domain::SessionInboxError::Storage(
                    "injected second legacy delivery failure".to_string(),
                ));
            }
            self.inner.deliver(envelope).await
        }

        async fn mark_activation_eligible(
            &self,
            target_session_id: &str,
            generation: u64,
            policy: bamboo_domain::SessionActivationPolicy,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            self.inner
                .mark_activation_eligible(target_session_id, generation, policy)
                .await
        }

        async fn claim(
            &self,
            target_session_id: &str,
            limit: usize,
        ) -> Result<Vec<bamboo_domain::SessionInboxClaim>, bamboo_domain::SessionInboxError>
        {
            self.inner.claim(target_session_id, limit).await
        }

        async fn was_admitted(
            &self,
            target_session_id: &str,
            id: &SessionMessageId,
        ) -> Result<bool, bamboo_domain::SessionInboxError> {
            self.inner.was_admitted(target_session_id, id).await
        }

        async fn ack(
            &self,
            target_session_id: &str,
            claim: &bamboo_domain::SessionInboxClaim,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            self.inner.ack(target_session_id, claim).await
        }

        async fn inspect(
            &self,
            target_session_id: &str,
        ) -> Result<bamboo_domain::SessionInboxBacklog, bamboo_domain::SessionInboxError> {
            self.inner.inspect(target_session_id).await
        }
    }

    struct ActivationBeforeClearPersistence {
        inner: Arc<bamboo_storage::LockedSessionStore>,
        inbox: Arc<dyn SessionInboxPort>,
        fail_clear_once: std::sync::atomic::AtomicBool,
        clear_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for ActivationBeforeClearPersistence {
        async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            self.inner.merge_save_runtime(session).await
        }

        async fn checkpoint_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            self.inner.checkpoint_runtime_session(session).await
        }

        async fn load_runtime_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            self.inner.storage().load_session(session_id).await
        }

        async fn clear_legacy_pending_messages(
            &self,
            session_id: &str,
            expected: &[serde_json::Value],
        ) -> std::io::Result<bool> {
            let backlog = self
                .inbox
                .inspect(session_id)
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            if backlog.activation_generation < backlog.generation
                || backlog.interrupt_generation < backlog.generation
            {
                return Err(std::io::Error::other(format!(
                    "legacy source clear attempted before interrupt activation publish: {backlog:?}"
                )));
            }
            self.clear_calls.fetch_add(1, Ordering::SeqCst);
            if self
                .fail_clear_once
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(std::io::Error::other(
                    "injected crash after enqueue+mark and before source clear",
                ));
            }
            self.inner
                .clear_legacy_pending_messages(session_id, expected)
                .await
        }
    }

    #[async_trait::async_trait]
    impl SessionInboxPort for ForcedPermanentReceiptInbox {
        async fn deliver(
            &self,
            envelope: &SessionMessageEnvelope,
        ) -> Result<bamboo_domain::SessionInboxReceipt, bamboo_domain::SessionInboxError> {
            self.inner.deliver(envelope).await
        }

        async fn mark_activation_eligible(
            &self,
            target_session_id: &str,
            generation: u64,
            policy: bamboo_domain::SessionActivationPolicy,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            self.inner
                .mark_activation_eligible(target_session_id, generation, policy)
                .await
        }

        async fn claim(
            &self,
            target_session_id: &str,
            limit: usize,
        ) -> Result<Vec<bamboo_domain::SessionInboxClaim>, bamboo_domain::SessionInboxError>
        {
            self.inner.claim(target_session_id, limit).await
        }

        async fn was_admitted(
            &self,
            _target_session_id: &str,
            _id: &SessionMessageId,
        ) -> Result<bool, bamboo_domain::SessionInboxError> {
            Ok(true)
        }

        async fn ack(
            &self,
            target_session_id: &str,
            claim: &bamboo_domain::SessionInboxClaim,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            self.inner.ack(target_session_id, claim).await?;
            self.ack_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn inspect(
            &self,
            target_session_id: &str,
        ) -> Result<bamboo_domain::SessionInboxBacklog, bamboo_domain::SessionInboxError> {
            self.inner.inspect(target_session_id).await
        }
    }

    struct BacklogActivationSpawner {
        inbox: Arc<dyn SessionInboxPort>,
        reservations: Arc<AtomicUsize>,
        launches: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::SessionActivationSpawner for BacklogActivationSpawner {
        async fn reserve_activation(
            &self,
            target_session_id: &str,
            _inbox_generation: u64,
        ) -> Result<crate::SessionActivationReserveOutcome, SessionActivationError> {
            let backlog = self
                .inbox
                .inspect(target_session_id)
                .await
                .map_err(|error| SessionActivationError::Internal(error.to_string()))?;
            if !backlog.activation_pending() {
                return Ok(crate::SessionActivationReserveOutcome::NoWork);
            }
            let ordinal = self.reservations.fetch_add(1, Ordering::SeqCst) + 1;
            let launches = self.launches.clone();
            Ok(crate::SessionActivationReserveOutcome::Reserved(
                crate::SessionActivationLaunch::new(format!("successor-{ordinal}"), move || {
                    launches.fetch_add(1, Ordering::SeqCst);
                }),
            ))
        }
    }

    #[async_trait::async_trait]
    impl SessionInboxPort for AckAfterPersistErrorInbox {
        async fn deliver(
            &self,
            envelope: &SessionMessageEnvelope,
        ) -> Result<bamboo_domain::SessionInboxReceipt, bamboo_domain::SessionInboxError> {
            self.inner.deliver(envelope).await
        }

        async fn mark_activation_eligible(
            &self,
            target_session_id: &str,
            generation: u64,
            policy: bamboo_domain::SessionActivationPolicy,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            self.inner
                .mark_activation_eligible(target_session_id, generation, policy)
                .await
        }

        async fn claim(
            &self,
            target_session_id: &str,
            limit: usize,
        ) -> Result<Vec<bamboo_domain::SessionInboxClaim>, bamboo_domain::SessionInboxError>
        {
            self.inner.claim(target_session_id, limit).await
        }

        async fn was_admitted(
            &self,
            target_session_id: &str,
            id: &SessionMessageId,
        ) -> Result<bool, bamboo_domain::SessionInboxError> {
            self.inner.was_admitted(target_session_id, id).await
        }

        async fn ack(
            &self,
            target_session_id: &str,
            claim: &bamboo_domain::SessionInboxClaim,
        ) -> Result<(), bamboo_domain::SessionInboxError> {
            if self
                .fail_before_once
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(bamboo_domain::SessionInboxError::Storage(
                    "injected pre-receipt ack failure".to_string(),
                ));
            }
            self.inner.ack(target_session_id, claim).await?;
            if self
                .fail_after_once
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(bamboo_domain::SessionInboxError::Storage(
                    "injected post-receipt ack failure".to_string(),
                ));
            }
            Ok(())
        }

        async fn inspect(
            &self,
            target_session_id: &str,
        ) -> Result<bamboo_domain::SessionInboxBacklog, bamboo_domain::SessionInboxError> {
            self.inner.inspect(target_session_id).await
        }
    }

    async fn durable_inbox_fixture(
        id: &str,
    ) -> (
        tempfile::TempDir,
        Arc<bamboo_storage::SessionStoreV2>,
        Arc<bamboo_storage::LockedSessionStore>,
        Arc<dyn SessionInboxPort>,
        Session,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = store.clone();
        let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage));
        let inbox: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        let session = Session::new(id, "model");
        store.save_session(&session).await.unwrap();
        (temp, store, persistence, inbox, session)
    }

    async fn deliver_interrupt_eligible(
        inbox: &Arc<dyn SessionInboxPort>,
        envelope: &SessionMessageEnvelope,
    ) -> bamboo_domain::SessionInboxReceipt {
        let receipt = inbox.deliver(envelope).await.unwrap();
        inbox
            .mark_activation_eligible(
                &envelope.target_session_id,
                receipt.generation,
                bamboo_domain::SessionActivationPolicy::InterruptSpecificWait,
            )
            .await
            .unwrap();
        receipt
    }

    #[tokio::test]
    async fn merge_pending_injected_messages_merges_and_clears() {
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            Arc::new(TestPersistence(storage.clone()));
        let mut persisted = Session::new_child("child-merge", "parent", "model", "Child");
        persisted.add_message(bamboo_agent_core::Message::system("system"));
        persisted.add_message(bamboo_agent_core::Message::user("original task"));
        persisted.metadata.insert(
            PENDING_INJECTED_MESSAGES_KEY.to_string(),
            serde_json::json!([
                {
                    "content": "queued correction",
                    "created_at": chrono::Utc::now(),
                }
            ])
            .to_string(),
        );
        storage
            .save_session(&persisted)
            .await
            .expect("persisted child should be saved");

        let mut running = persisted.clone();
        running.metadata.remove(PENDING_INJECTED_MESSAGES_KEY);

        let count =
            merge_pending_injected_messages(&mut running, Some(&storage), Some(&persistence)).await;

        assert_eq!(count, 1);
        assert_eq!(
            running
                .messages
                .last()
                .map(|message| message.content.as_str()),
            Some("queued correction")
        );
        assert!(!running.metadata.contains_key(PENDING_INJECTED_MESSAGES_KEY));
        let saved = storage
            .load_session("child-merge")
            .await
            .expect("load should succeed")
            .expect("session should exist");
        assert!(!saved.metadata.contains_key(PENDING_INJECTED_MESSAGES_KEY));

        // Second merge is a no-op
        let count2 =
            merge_pending_injected_messages(&mut running, Some(&storage), Some(&persistence)).await;
        assert_eq!(count2, 0);
    }

    #[tokio::test]
    async fn merge_pending_injected_messages_returns_zero_without_storage() {
        let mut session = test_session();
        let count = merge_pending_injected_messages(&mut session, None, None).await;
        assert_eq!(count, 0);
    }

    // #540/#770: the turn-boundary refresh reports the live on-disk permission
    // mode so the pipeline can adopt a mid-run PATCH into the running loop.
    #[tokio::test]
    async fn refresh_turn_boundary_reports_disk_permission_mode() {
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());

        // Persist a session with bypass ON.
        let mut persisted = Session::new("bypass-live", "model");
        let mut state = AgentRuntimeState::new("run-x");
        state.set_permission_mode(bamboo_domain::SessionPermissionMode::Auto);
        persisted.agent_runtime_state = Some(state);
        for (key, value) in [
            ("permission.policy_revision", "17"),
            ("permission.requested_mode", "auto"),
            ("permission.effective_mode", "auto"),
            ("permission.executor_mapping", "bamboo_runtime:auto"),
            ("permission.transitioned_at", "2026-07-31T12:00:00Z"),
        ] {
            persisted
                .metadata
                .insert(key.to_string(), value.to_string());
        }
        storage.save_session(&persisted).await.unwrap();

        // A running loop holds a stale copy with bypass OFF.
        let mut running = Session::new("bypass-live", "model");
        running.agent_runtime_state = Some(AgentRuntimeState::new("run-x"));
        running.metadata.insert(
            "permission.executor_mapping".to_string(),
            "bamboo_runtime:default".to_string(),
        );

        let refresh = refresh_turn_boundary_from_disk(&mut running, Some(&storage), None).await;
        assert_eq!(
            refresh.disk_permission_mode,
            Some(bamboo_domain::SessionPermissionMode::Auto)
        );
        assert_eq!(refresh.merged, 0);
        for (key, value) in [
            ("permission.policy_revision", "17"),
            ("permission.requested_mode", "auto"),
            ("permission.effective_mode", "auto"),
            ("permission.executor_mapping", "bamboo_runtime:auto"),
            ("permission.transitioned_at", "2026-07-31T12:00:00Z"),
        ] {
            assert_eq!(running.metadata.get(key).map(String::as_str), Some(value));
        }
    }

    #[tokio::test]
    async fn refresh_turn_boundary_reports_none_without_storage() {
        let mut session = test_session();
        let refresh = refresh_turn_boundary_from_disk(&mut session, None, None).await;
        assert_eq!(refresh.disk_permission_mode, None);
        assert_eq!(refresh.merged, 0);
    }

    // #540 review: a disk copy with no agent_runtime_state reports None
    // (unknown), NOT Some(false) — so the pipeline won't force-disable a
    // legitimately bypassed run.
    #[tokio::test]
    async fn refresh_turn_boundary_reports_none_when_disk_has_no_runtime_state() {
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        let persisted = Session::new("no-rs", "model");
        assert!(persisted.agent_runtime_state.is_none());
        storage.save_session(&persisted).await.unwrap();

        let mut running = Session::new("no-rs", "model");
        let refresh = refresh_turn_boundary_from_disk(&mut running, Some(&storage), None).await;
        assert_eq!(refresh.disk_permission_mode, None);
    }

    #[tokio::test]
    async fn inbox_checkpoint_failure_rolls_back_memory_and_leaves_claim_recoverable() {
        let (_temp, store, locked, inbox, mut running) =
            durable_inbox_fixture("checkpoint-failure").await;
        let mut envelope = SessionMessageEnvelope::user_input("checkpoint-failure", "recover me");
        envelope.id = SessionMessageId::parse("checkpoint-failure-message").unwrap();
        deliver_interrupt_eligible(&inbox, &envelope).await;

        let fault = Arc::new(FaultingPersistence {
            inner: locked.clone(),
            fail_checkpoint: std::sync::atomic::AtomicBool::new(true),
        });
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = fault;
        let storage: Arc<dyn Storage> = store.clone();
        let refresh = refresh_turn_boundary_with_inbox(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(refresh.merged, 0);
        assert!(!running
            .messages
            .iter()
            .any(|message| message.id == envelope.id.as_str()));
        assert!(!running
            .session_inbox_admission()
            .is_some_and(|state| state.contains(&envelope.id)));
        let backlog = inbox.inspect(&running.id).await.unwrap();
        assert_eq!(backlog.claimed, 1);
        assert!(!inbox.was_admitted(&running.id, &envelope.id).await.unwrap());

        // Restart: recover the cur claim and admit it exactly once.
        let reopened: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        let normal: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let mut restarted = store.load_session(&running.id).await.unwrap().unwrap();
        let refresh = refresh_turn_boundary_with_inbox(
            &mut restarted,
            Some(&storage),
            Some(&normal),
            Some(&reopened),
        )
        .await;
        assert_eq!(refresh.merged, 1);
        assert_eq!(
            restarted
                .messages
                .iter()
                .filter(|message| message.id == envelope.id.as_str())
                .count(),
            1
        );
        assert!(reopened
            .was_admitted(&running.id, &envelope.id)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn non_typed_transcript_id_collision_never_acks_the_typed_claim() {
        let (_temp, store, locked, inbox, mut running) =
            durable_inbox_fixture("transcript-id-collision").await;
        let mut envelope =
            SessionMessageEnvelope::user_input("transcript-id-collision", "typed truth");
        envelope.id = SessionMessageId::parse("colliding-id").unwrap();
        deliver_interrupt_eligible(&inbox, &envelope).await;

        let mut forged = bamboo_agent_core::Message::user("unrelated transcript entry");
        forged.id = envelope.id.to_string();
        running.add_message(forged);
        let storage: Arc<dyn Storage> = store;
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let refresh = refresh_turn_boundary_with_inbox(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(refresh.merged, 0);
        assert!(!running
            .session_inbox_admission()
            .is_some_and(|state| state.contains(&envelope.id)));
        let backlog = inbox.inspect(&running.id).await.unwrap();
        assert_eq!(backlog.claimed, 1);
        assert!(!inbox.was_admitted(&running.id, &envelope.id).await.unwrap());
    }

    #[tokio::test]
    async fn concurrent_durable_id_collision_after_claim_never_acks() {
        let (_temp, store, locked, inbox, mut running) =
            durable_inbox_fixture("durable-id-collision").await;
        let mut envelope =
            SessionMessageEnvelope::user_input("durable-id-collision", "typed truth");
        envelope.id = SessionMessageId::parse("durable-colliding-id").unwrap();
        deliver_interrupt_eligible(&inbox, &envelope).await;

        // A concurrent writer lands after the running snapshot was loaded but
        // before checkpoint merge. LockedSessionStore preserves that durable
        // message, so post-checkpoint proof must catch the collision.
        let mut concurrent = store.load_session(&running.id).await.unwrap().unwrap();
        let mut forged = bamboo_agent_core::Message::user("concurrent unrelated message");
        forged.id = envelope.id.to_string();
        concurrent.add_message(forged);
        store.save_session(&concurrent).await.unwrap();

        let storage: Arc<dyn Storage> = store.clone();
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let refresh = refresh_turn_boundary_with_inbox(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(refresh.merged, 0);
        let backlog = inbox.inspect(&running.id).await.unwrap();
        assert_eq!(backlog.claimed, 1);
        assert!(!inbox.was_admitted(&running.id, &envelope.id).await.unwrap());
        let durable = store.load_session(&running.id).await.unwrap().unwrap();
        assert!(!durable
            .messages
            .iter()
            .any(|message| { bamboo_domain::is_matching_session_message(message, &envelope) }));
    }

    #[tokio::test]
    async fn concurrent_durable_typed_body_mismatch_after_claim_never_acks() {
        let (_temp, store, locked, inbox, mut running) =
            durable_inbox_fixture("durable-typed-body-collision").await;
        let mut envelope =
            SessionMessageEnvelope::user_input("durable-typed-body-collision", "typed truth");
        envelope.id = SessionMessageId::parse("durable-typed-colliding-id").unwrap();
        deliver_interrupt_eligible(&inbox, &envelope).await;

        let mut different = envelope.clone();
        different.body = bamboo_domain::SessionMessageBody::Content(
            bamboo_domain::SessionMessageContent::text("forged different body"),
        );
        let mut concurrent = store.load_session(&running.id).await.unwrap().unwrap();
        concurrent.add_message(different.to_provider_message().unwrap());
        store.save_session(&concurrent).await.unwrap();

        let storage: Arc<dyn Storage> = store.clone();
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let refresh = refresh_turn_boundary_with_inbox(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(refresh.merged, 0);
        assert_eq!(inbox.inspect(&running.id).await.unwrap().claimed, 1);
        assert!(!inbox.was_admitted(&running.id, &envelope.id).await.unwrap());
        let durable = store.load_session(&running.id).await.unwrap().unwrap();
        assert!(!durable
            .messages
            .iter()
            .any(|message| bamboo_domain::is_matching_session_message(message, &envelope)));
    }

    #[tokio::test]
    async fn permanent_receipt_without_typed_transcript_proof_leaves_cur_recoverable() {
        let (_temp, store, locked, real_inbox, mut running) =
            durable_inbox_fixture("permanent-without-proof").await;
        let mut envelope =
            SessionMessageEnvelope::user_input("permanent-without-proof", "recoverable");
        envelope.id = SessionMessageId::parse("receipt-without-proof").unwrap();
        deliver_interrupt_eligible(&real_inbox, &envelope).await;
        let ack_count = Arc::new(AtomicUsize::new(0));
        let inbox: Arc<dyn SessionInboxPort> = Arc::new(ForcedPermanentReceiptInbox {
            inner: real_inbox.clone(),
            ack_count: ack_count.clone(),
        });
        let storage: Arc<dyn Storage> = store;
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;

        let refresh = refresh_turn_boundary_with_inbox(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(refresh.merged, 0);
        assert_eq!(ack_count.load(Ordering::SeqCst), 0);
        assert_eq!(real_inbox.inspect(&running.id).await.unwrap().claimed, 1);
    }

    #[tokio::test]
    async fn permanent_receipt_uses_unbounded_typed_marker_after_cursor_eviction() {
        let (_temp, store, locked, real_inbox, mut running) =
            durable_inbox_fixture("permanent-evicted-cursor").await;
        let mut envelope =
            SessionMessageEnvelope::user_input("permanent-evicted-cursor", "old admitted message");
        envelope.id = SessionMessageId::parse("evicted-receipt-id").unwrap();
        running.add_message(envelope.to_provider_message().unwrap());
        running
            .session_inbox_admission_mut()
            .record(envelope.id.clone(), 1);
        for sequence in 2..=(bamboo_domain::SESSION_INBOX_ADMITTED_CAPACITY as u64 + 1) {
            running.session_inbox_admission_mut().record(
                SessionMessageId::parse(format!("later-{sequence}")).unwrap(),
                sequence,
            );
        }
        assert!(!running
            .session_inbox_admission()
            .unwrap()
            .contains(&envelope.id));
        store.save_session(&running).await.unwrap();
        deliver_interrupt_eligible(&real_inbox, &envelope).await;

        let ack_count = Arc::new(AtomicUsize::new(0));
        let inbox: Arc<dyn SessionInboxPort> = Arc::new(ForcedPermanentReceiptInbox {
            inner: real_inbox.clone(),
            ack_count: ack_count.clone(),
        });
        let storage: Arc<dyn Storage> = store;
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let refresh = refresh_turn_boundary_with_inbox(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(refresh.merged, 0);
        assert_eq!(ack_count.load(Ordering::SeqCst), 1);
        let backlog = real_inbox.inspect(&running.id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
        assert_eq!(
            running
                .messages
                .iter()
                .filter(|message| message.id == envelope.id.as_str())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn post_receipt_ack_error_restarts_without_duplicate_transcript() {
        let (_temp, store, locked, real_inbox, mut running) =
            durable_inbox_fixture("ack-after-receipt").await;
        let mut envelope = SessionMessageEnvelope::user_input("ack-after-receipt", "exactly once");
        envelope.id = SessionMessageId::parse("ack-after-receipt-message").unwrap();
        deliver_interrupt_eligible(&real_inbox, &envelope).await;

        let faulted: Arc<dyn SessionInboxPort> = Arc::new(AckAfterPersistErrorInbox {
            inner: real_inbox.clone(),
            fail_before_once: std::sync::atomic::AtomicBool::new(false),
            fail_after_once: std::sync::atomic::AtomicBool::new(true),
        });
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked.clone();
        let storage: Arc<dyn Storage> = store.clone();
        refresh_turn_boundary_with_inbox(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&faulted),
        )
        .await;

        let checkpointed = store.load_session(&running.id).await.unwrap().unwrap();
        assert_eq!(
            checkpointed
                .messages
                .iter()
                .filter(|message| message.id == envelope.id.as_str())
                .count(),
            1
        );
        assert!(checkpointed
            .session_inbox_admission()
            .is_some_and(|state| state.contains(&envelope.id)));
        assert!(real_inbox
            .was_admitted(&running.id, &envelope.id)
            .await
            .unwrap());

        let reopened: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        // A sender retry after restart observes the permanent receipt and does
        // not enqueue a second copy.
        reopened.deliver(&envelope).await.unwrap();
        assert_eq!(
            reopened.inspect(&running.id).await.unwrap().pending
                + reopened.inspect(&running.id).await.unwrap().claimed,
            0
        );
        let mut restarted = store.load_session(&running.id).await.unwrap().unwrap();
        refresh_turn_boundary_with_inbox(
            &mut restarted,
            Some(&storage),
            Some(&persistence),
            Some(&reopened),
        )
        .await;
        assert_eq!(
            restarted
                .messages
                .iter()
                .filter(|message| message.id == envelope.id.as_str())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn checkpoint_success_pre_ack_failure_recovers_cur_without_duplicate() {
        let (_temp, store, locked, real_inbox, mut running) =
            durable_inbox_fixture("pre-ack-failure").await;
        let mut envelope = SessionMessageEnvelope::user_input("pre-ack-failure", "recover claimed");
        envelope.id = SessionMessageId::parse("pre-ack-message").unwrap();
        deliver_interrupt_eligible(&real_inbox, &envelope).await;
        let faulted: Arc<dyn SessionInboxPort> = Arc::new(AckAfterPersistErrorInbox {
            inner: real_inbox.clone(),
            fail_before_once: std::sync::atomic::AtomicBool::new(true),
            fail_after_once: std::sync::atomic::AtomicBool::new(false),
        });
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked.clone();
        let storage: Arc<dyn Storage> = store.clone();
        refresh_turn_boundary_with_inbox(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&faulted),
        )
        .await;
        let checkpointed = store.load_session(&running.id).await.unwrap().unwrap();
        assert_eq!(
            checkpointed
                .messages
                .iter()
                .filter(|message| message.id == envelope.id.as_str())
                .count(),
            1
        );
        assert_eq!(real_inbox.inspect(&running.id).await.unwrap().claimed, 1);
        assert!(!real_inbox
            .was_admitted(&running.id, &envelope.id)
            .await
            .unwrap());

        let reopened: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        let mut restarted = checkpointed;
        refresh_turn_boundary_with_inbox(
            &mut restarted,
            Some(&storage),
            Some(&persistence),
            Some(&reopened),
        )
        .await;
        assert_eq!(
            restarted
                .messages
                .iter()
                .filter(|message| message.id == envelope.id.as_str())
                .count(),
            1
        );
        assert!(reopened
            .was_admitted(&running.id, &envelope.id)
            .await
            .unwrap());
        let backlog = reopened.inspect(&running.id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
    }

    #[tokio::test]
    async fn legacy_migration_clears_matching_running_snapshot_without_resurrection() {
        let (_temp, store, locked, inbox, mut running) =
            durable_inbox_fixture("legacy-running-snapshot").await;
        let legacy = vec![serde_json::json!({
            "content": "migrate me once",
            "created_at": "2026-07-25T00:00:00Z"
        })];
        running.set_pending_injected_messages(legacy.clone());
        store.save_session(&running).await.unwrap();

        let storage: Arc<dyn Storage> = store.clone();
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let refresh = refresh_turn_boundary_with_inbox(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(refresh.merged, 1);
        assert!(!running.has_pending_injected_messages());

        let message_id = SessionMessageId::legacy(&running.id, 0, &legacy[0]);
        let reloaded = store.load_session(&running.id).await.unwrap().unwrap();
        assert!(!reloaded.has_pending_injected_messages());
        assert_eq!(
            reloaded
                .messages
                .iter()
                .filter(|message| message.id == message_id.as_str())
                .count(),
            1
        );

        let mut restarted = reloaded;
        let refresh = refresh_turn_boundary_with_inbox(
            &mut restarted,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(refresh.merged, 0);
        assert!(!restarted.has_pending_injected_messages());
        assert_eq!(
            restarted
                .messages
                .iter()
                .filter(|message| message.id == message_id.as_str())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn legacy_migration_publishes_interrupt_activation_before_source_clear() {
        let (_temp, store, locked, inbox, mut running) =
            durable_inbox_fixture("legacy-mark-before-clear").await;
        let legacy = vec![serde_json::json!({
            "content": "survive clear crash",
            "nested": {"z": 1, "a": 2}
        })];
        let mut producer = store.load_session(&running.id).await.unwrap().unwrap();
        producer.set_pending_injected_messages(legacy.clone());
        store.save_session(&producer).await.unwrap();

        let storage: Arc<dyn Storage> = store.clone();
        let fault = Arc::new(ActivationBeforeClearPersistence {
            inner: locked.clone(),
            inbox: inbox.clone(),
            fail_clear_once: std::sync::atomic::AtomicBool::new(true),
            clear_calls: AtomicUsize::new(0),
        });
        let fault_persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = fault.clone();
        let crashed = migrate_legacy_pending_only(
            &mut running,
            Some(&storage),
            Some(&fault_persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(crashed.delivered, 1);
        assert_eq!(crashed.highest_generation, Some(1));
        assert!(!crashed.source_cleared);
        assert_eq!(fault.clear_calls.load(Ordering::SeqCst), 1);

        let after_crash = inbox.inspect(&running.id).await.unwrap();
        assert_eq!(after_crash.generation, 1);
        assert_eq!(after_crash.activation_generation, 1);
        assert_eq!(after_crash.interrupt_generation, 1);
        assert!(after_crash.activation_pending());
        assert!(store
            .load_session(&running.id)
            .await
            .unwrap()
            .unwrap()
            .has_pending_injected_messages());

        // Process restart: deterministic delivery reuses generation one, then
        // the already-published activation permits the source CAS-clear.
        let reopened: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            store.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        ));
        let normal: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let mut restarted = store.load_session(&running.id).await.unwrap().unwrap();
        let recovered = migrate_legacy_pending_only(
            &mut restarted,
            Some(&storage),
            Some(&normal),
            Some(&reopened),
        )
        .await;
        assert_eq!(recovered.delivered, 1);
        assert_eq!(recovered.highest_generation, Some(1));
        assert!(recovered.source_cleared);
        let backlog = reopened.inspect(&running.id).await.unwrap();
        assert_eq!(backlog.generation, 1);
        assert_eq!(backlog.activation_generation, 1);
        assert_eq!(backlog.interrupt_generation, 1);
        assert!(!store
            .load_session(&running.id)
            .await
            .unwrap()
            .unwrap()
            .has_pending_injected_messages());
    }

    #[tokio::test]
    async fn legacy_activation_publish_failure_never_invokes_source_clear() {
        let (_temp, store, locked, inbox, mut running) =
            durable_inbox_fixture("legacy-mark-failure").await;
        let legacy = vec![serde_json::json!({"content": "do not clear"})];
        let mut producer = store.load_session(&running.id).await.unwrap().unwrap();
        producer.set_pending_injected_messages(legacy);
        store.save_session(&producer).await.unwrap();
        let storage: Arc<dyn Storage> = store.clone();
        let failed_inbox = Arc::new(FailActivationInbox {
            inner: inbox.clone(),
            marks: AtomicUsize::new(0),
        });
        let failed_port: Arc<dyn SessionInboxPort> = failed_inbox.clone();
        let clear_probe = Arc::new(ActivationBeforeClearPersistence {
            inner: locked,
            inbox: inbox.clone(),
            fail_clear_once: std::sync::atomic::AtomicBool::new(false),
            clear_calls: AtomicUsize::new(0),
        });
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = clear_probe.clone();

        let migration = migrate_legacy_pending_only(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&failed_port),
        )
        .await;
        assert_eq!(migration.delivered, 1);
        assert_eq!(migration.highest_generation, None);
        assert!(!migration.source_cleared);
        assert_eq!(failed_inbox.marks.load(Ordering::SeqCst), 1);
        assert_eq!(
            clear_probe.clear_calls.load(Ordering::SeqCst),
            0,
            "source CAS-clear must be unreachable until activation publish succeeds"
        );
        let backlog = inbox.inspect(&running.id).await.unwrap();
        assert_eq!(backlog.generation, 1);
        assert_eq!(backlog.activation_generation, 0);
        assert!(store
            .load_session(&running.id)
            .await
            .unwrap()
            .unwrap()
            .has_pending_injected_messages());
    }

    #[tokio::test]
    async fn partial_legacy_migration_authorizes_prefix_and_never_clears_source() {
        let (_temp, store, locked, inbox, mut running) =
            durable_inbox_fixture("legacy-partial-prefix").await;
        let legacy = vec![
            serde_json::json!({"content": "first"}),
            serde_json::json!({"content": "second"}),
        ];
        let mut producer = store.load_session(&running.id).await.unwrap().unwrap();
        producer.set_pending_injected_messages(legacy.clone());
        store.save_session(&producer).await.unwrap();
        let storage: Arc<dyn Storage> = store.clone();
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked.clone();
        let faulted: Arc<dyn SessionInboxPort> = Arc::new(FailSecondDeliveryInbox {
            inner: inbox.clone(),
            deliveries: AtomicUsize::new(0),
        });

        let partial = migrate_legacy_pending_only(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&faulted),
        )
        .await;
        assert_eq!(partial.delivered, 1);
        assert_eq!(partial.highest_generation, Some(1));
        assert!(!partial.source_cleared);
        let prefix = inbox.inspect(&running.id).await.unwrap();
        assert_eq!(prefix.generation, 1);
        assert_eq!(prefix.activation_generation, 1);
        assert_eq!(prefix.interrupt_generation, 1);
        assert!(prefix.activation_pending());
        assert!(store
            .load_session(&running.id)
            .await
            .unwrap()
            .unwrap()
            .has_pending_injected_messages());

        // A retry sees the first envelope's permanent enqueue identity, adds
        // only the missing suffix, publishes generation two, then clears.
        let mut restarted = store.load_session(&running.id).await.unwrap().unwrap();
        let complete = migrate_legacy_pending_only(
            &mut restarted,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(complete.delivered, 2);
        assert_eq!(complete.highest_generation, Some(2));
        assert!(complete.source_cleared);
        let backlog = inbox.inspect(&running.id).await.unwrap();
        assert_eq!(backlog.generation, 2);
        assert_eq!(backlog.activation_generation, 2);
        assert_eq!(backlog.interrupt_generation, 2);
        assert_eq!(backlog.pending, 2);
        assert!(!store
            .load_session(&running.id)
            .await
            .unwrap()
            .unwrap()
            .has_pending_injected_messages());
    }

    #[tokio::test]
    async fn terminal_typed_delivery_stays_pending_for_exactly_one_successor_turn() {
        let (_temp, store, locked, inbox, mut first_run) =
            durable_inbox_fixture("terminal-typed-race").await;
        let storage: Arc<dyn Storage> = store.clone();
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let router = crate::SessionActivationRouter::new();
        let reservations = Arc::new(AtomicUsize::new(0));
        let launches = Arc::new(AtomicUsize::new(0));
        router
            .set_spawner(Arc::new(BacklogActivationSpawner {
                inbox: inbox.clone(),
                reservations: reservations.clone(),
                launches: launches.clone(),
            }))
            .await;
        let mut registration = router
            .register_run(&first_run.id, "first-run")
            .await
            .unwrap();

        // The provider has returned from its final round. A typed delivery now
        // lands, but terminal cleanup must not claim or ack it.
        let mut envelope = SessionMessageEnvelope::user_input(&first_run.id, "after final round");
        envelope.id = SessionMessageId::parse("terminal-race-message").unwrap();
        let receipt = deliver_interrupt_eligible(&inbox, &envelope).await;
        assert_eq!(
            router
                .request_activation(&first_run.id, receipt.generation)
                .await
                .unwrap(),
            SessionActivationDisposition::ActiveNotified
        );
        assert_eq!(inbox.inspect(&first_run.id).await.unwrap().pending, 1);
        assert!(!first_run
            .messages
            .iter()
            .any(|message| message.id == envelope.id.as_str()));

        registration.begin_finalization().await;
        assert_eq!(
            registration.finish(0).await.unwrap(),
            Some(SessionActivationDisposition::ActivationReserved)
        );
        assert_eq!(reservations.load(Ordering::SeqCst), 1);
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        assert_eq!(inbox.inspect(&first_run.id).await.unwrap().pending, 1);

        // The successor's first safe boundary checkpoints and acknowledges the
        // envelope exactly once before reasoning.
        let refresh = refresh_turn_boundary_with_inbox(
            &mut first_run,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(refresh.merged, 1);
        assert_eq!(
            first_run
                .messages
                .iter()
                .filter(|message| message.id == envelope.id.as_str())
                .count(),
            1
        );
        let backlog = inbox.inspect(&first_run.id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
    }

    #[tokio::test]
    async fn terminal_legacy_migration_enqueues_without_ack_and_hands_off_generation() {
        let (_temp, store, locked, inbox, mut first_run) =
            durable_inbox_fixture("terminal-legacy-race").await;
        let legacy = vec![serde_json::json!({
            "content": "legacy after final round",
            "created_at": "2026-07-25T00:00:00Z"
        })];
        let mut producer_snapshot = store.load_session(&first_run.id).await.unwrap().unwrap();
        producer_snapshot.set_pending_injected_messages(legacy.clone());
        store.save_session(&producer_snapshot).await.unwrap();

        let storage: Arc<dyn Storage> = store.clone();
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let migration = migrate_legacy_pending_only(
            &mut first_run,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(migration.delivered, 1);
        assert!(migration.source_cleared);
        let generation = migration.highest_generation.expect("durable generation");
        let message_id = SessionMessageId::legacy(&first_run.id, 0, &legacy[0]);
        assert_eq!(inbox.inspect(&first_run.id).await.unwrap().pending, 1);
        assert!(!inbox
            .was_admitted(&first_run.id, &message_id)
            .await
            .unwrap());
        assert!(!store
            .load_session(&first_run.id)
            .await
            .unwrap()
            .unwrap()
            .has_pending_injected_messages());

        let router = crate::SessionActivationRouter::new();
        let reservations = Arc::new(AtomicUsize::new(0));
        let launches = Arc::new(AtomicUsize::new(0));
        router
            .set_spawner(Arc::new(BacklogActivationSpawner {
                inbox: inbox.clone(),
                reservations: reservations.clone(),
                launches: launches.clone(),
            }))
            .await;
        let mut registration = router
            .register_run(&first_run.id, "first-run")
            .await
            .unwrap();
        assert_eq!(
            router
                .request_activation(&first_run.id, generation)
                .await
                .unwrap(),
            SessionActivationDisposition::ActiveNotified
        );
        registration.begin_finalization().await;
        assert_eq!(
            registration.finish(0).await.unwrap(),
            Some(SessionActivationDisposition::ActivationReserved)
        );
        assert_eq!(reservations.load(Ordering::SeqCst), 1);
        assert_eq!(launches.load(Ordering::SeqCst), 1);

        let refresh = refresh_turn_boundary_with_inbox(
            &mut first_run,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(refresh.merged, 1);
        assert_eq!(
            first_run
                .messages
                .iter()
                .filter(|message| message.id == message_id.as_str())
                .count(),
            1
        );
        let backlog = inbox.inspect(&first_run.id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
    }
}
