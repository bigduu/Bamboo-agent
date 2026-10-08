//! Bridge between structured [`AgentRuntimeState`] and session metadata.
//!
//! The read path prefers the structured `session.agent_runtime_state` field
//! and falls back to the legacy `session.metadata["agent.runtime.state"]` key.
//! The write path writes only to the structured field; the legacy metadata
//! mirror is no longer maintained.

use std::sync::Arc;

use bamboo_agent_core::{AgentError, Message, Session};
use bamboo_domain::{
    AgentRuntimeState, SessionInboxPort, SessionMessageBody, SessionMessageContent,
    SessionMessageEnvelope, SessionMessageId, SessionMessageKind, SessionMessageSource,
    SessionRuntimeInstruction,
};

const METADATA_KEY: &str = "agent.runtime.state";
const INBOX_ACK_UNRESOLVED: &str =
    "SessionInbox ACK unresolved; durable input is preserved; retry this activation";
const INBOX_CLAIM_UNRESOLVED: &str =
    "SessionInbox claim unresolved; durable input is preserved; retry with its current owner";
/// Original scalar data borrowed without Message, image or Source retention.
#[derive(Clone, Copy)]
pub struct BorrowedInputRequestRecord<'a> {
    pub input_id: &'a str,
    pub source: &'a SessionMessageSource,
    pub kind: SessionMessageKind,
    pub wrapper: Option<&'a str>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub request: Option<&'a bamboo_domain::SessionSkillRequest>,
}

// Private deterministic measurement, never a wire/persistence format or decoder.
#[derive(serde::Serialize)]
struct InputRequestMeasurement<'a, T> {
    session_id: &'a str,
    execution_id: &'a str,
    records: T,
}
struct InputRequestRecordsMeasurement<'a>(&'a [BorrowedInputRequestRecord<'a>]);
impl serde::Serialize for InputRequestRecordsMeasurement<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for record in self.0 {
            sequence.serialize_element(&(
                record.input_id,
                record.source,
                record.kind,
                record.wrapper,
                record.created_at,
                record.request,
            ))?;
        }
        sequence.end()
    }
}

/// Validate all borrowed data and charge the whole batch before any owned copy.
/// Success does not classify input as New, authorize it or publish anything.
pub fn project_input_request_batch(
    session_id: &str,
    execution_id: &str,
    records: &[BorrowedInputRequestRecord<'_>],
) -> crate::runtime::managers::lifecycle::InputRequestProjection {
    use crate::runtime::managers::lifecycle::{
        BoundedInputRequestBatch,
        InputRequestProjection::{Available, Unavailable},
        InputRequestUnavailable, ProjectedInputRequest,
    };
    const LIMIT: usize = 262144;
    if records.len() > 128 {
        return Unavailable(InputRequestUnavailable::RecordLimit {
            count: records.len(),
            limit: 128,
        });
    }
    for record in records {
        let id = record.input_id;
        if id.trim().is_empty()
            || id.trim() != id
            || id.len() > 256
            || id.contains('/')
            || id.contains('\\')
            || id.contains("..")
            || record
                .request
                .is_some_and(|request| request.validate().is_err())
        {
            return Unavailable(InputRequestUnavailable::InvalidData);
        }
    }
    let Some(compact_bytes) = measure_input_request_view(
        session_id,
        execution_id,
        InputRequestRecordsMeasurement(records),
    ) else {
        return Unavailable(InputRequestUnavailable::CompactLimit { limit: LIMIT });
    };
    let records = records
        .iter()
        .map(|record| ProjectedInputRequest {
            input_id: record.input_id.into(),
            source: match record.source {
                SessionMessageSource::User => SessionMessageSource::User,
                SessionMessageSource::Session { session_id } => SessionMessageSource::Session {
                    session_id: session_id.as_str().into(),
                },
                SessionMessageSource::Runtime { subsystem } => SessionMessageSource::Runtime {
                    subsystem: subsystem.as_str().into(),
                },
            },
            kind: record.kind,
            wrapper: record.wrapper.map(String::from),
            created_at: record.created_at,
            request: record
                .request
                .map(|request| bamboo_domain::SessionSkillRequest {
                    mode: request.mode.as_deref().map(String::from),
                    selections: request
                        .selections
                        .iter()
                        .map(|selection| bamboo_domain::SessionSkillSelection {
                            id: selection.id.as_str().into(),
                            source: selection.source.as_str().into(),
                            revision: selection.revision,
                            args: copy_input_request_value(&selection.args),
                        })
                        .collect::<Vec<_>>()
                        .into_boxed_slice()
                        .into_vec(),
                }),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Available(BoundedInputRequestBatch {
        session_id: session_id.into(),
        execution_id: execution_id.into(),
        records,
        compact_bytes,
    })
}

fn measure_input_request_view(
    session_id: &str,
    execution_id: &str,
    records: impl serde::Serialize,
) -> Option<usize> {
    struct CountingWrite(usize);
    impl std::io::Write for CountingWrite {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|n| *n <= 262144)
                .ok_or_else(|| std::io::Error::other("compact input byte limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = CountingWrite(0);
    serde_json::to_writer(
        &mut count,
        &InputRequestMeasurement {
            session_id,
            execution_id,
            records,
        },
    )
    .ok()?;
    Some(count.0)
}

// Borrow the canonical request representation, including absent/default fields.
// Serde counts the same keys/scalars as Q-B without copying any args tree.
#[derive(serde::Serialize)]
struct RawRequest<'a> {
    selections: RawSelections<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<&'a str>,
}
struct RawSelections<'a>(&'a [serde_json::Value]);
#[derive(serde::Serialize)]
struct RawSelection<'a> {
    id: &'a str,
    source: &'a str,
    revision: u64,
    args: &'a serde_json::Value,
}
fn raw_selection(value: &serde_json::Value) -> Option<RawSelection<'_>> {
    let fields = value.as_object()?;
    if fields
        .keys()
        .any(|k| !matches!(k.as_str(), "id" | "source" | "revision" | "args"))
    {
        return None;
    }
    Some(RawSelection {
        id: value.get("id")?.as_str()?,
        source: value.get("source")?.as_str()?,
        revision: value.get("revision")?.as_u64()?,
        args: value.get("args").unwrap_or(&serde_json::Value::Null),
    })
}
impl serde::Serialize for RawSelections<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            sequence.serialize_element(
                &raw_selection(value)
                    .ok_or_else(|| serde::ser::Error::custom("invalid checked selection"))?,
            )?;
        }
        sequence.end()
    }
}
fn raw_request(value: &serde_json::Value) -> Option<RawRequest<'_>> {
    let fields = value.as_object()?;
    if fields
        .keys()
        .any(|k| !matches!(k.as_str(), "selections" | "mode"))
    {
        return None;
    }
    let selections = value.get("selections")?.as_array()?;
    if selections.is_empty() || selections.len() > 32 {
        return None;
    }
    let mode = match value.get("mode") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => Some(value.as_str()?),
    };
    if mode.is_some_and(|mode| {
        mode.is_empty()
            || mode.len() > 64
            || !mode
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    }) {
        return None;
    }
    for (index, value) in selections.iter().enumerate() {
        let selection = raw_selection(value)?;
        bamboo_domain::SessionSkillSelection::validate_borrowed(
            selection.id,
            selection.source,
            selection.revision,
            selection.args,
        )
        .ok()?;
        if selections[..index]
            .iter()
            .any(|prior| prior.get("id").and_then(|v| v.as_str()) == Some(selection.id))
        {
            return None;
        }
    }
    Some(RawRequest {
        selections: RawSelections(selections),
        mode,
    })
}
#[derive(serde::Serialize)]
struct RawInputRecord<'a>(
    &'a str,
    &'a serde_json::Value,
    SessionMessageKind,
    Option<&'static str>,
    chrono::DateTime<chrono::Utc>,
    Option<RawRequest<'a>>,
);

// Private caller precondition: ONLY this completed boundary's New/ACKed prefix.
// Never call on transcript history or use this projection as a provenance test.
fn project_new_committed_inputs(
    session_id: &str,
    execution_id: &str,
    messages: &[Message],
) -> crate::runtime::managers::lifecycle::InputRequestProjection {
    use crate::runtime::managers::lifecycle::{
        BoundedInputRequestBatch,
        InputRequestProjection::{Available, Unavailable},
        InputRequestUnavailable, ProjectedInputRequest,
    };
    if messages.len() > 128 {
        return Unavailable(InputRequestUnavailable::RecordLimit {
            count: messages.len(),
            limit: 128,
        });
    }
    let parse = || -> Option<Vec<RawInputRecord<'_>>> {
        let mut records = Vec::with_capacity(messages.len());
        for message in messages {
            let proof = message.metadata.as_ref()?.get("session_message")?;
            let source = proof.get("source")?;
            let body = proof.get("body")?;
            let (kind, wrapper, content) =
                match (source.get("type")?.as_str()?, proof.get("kind")?.as_str()?) {
                    ("user", "user_input")
                        if source.as_object()?.len() == 1
                            && body.get("type")?.as_str()? == "content" =>
                    {
                        (SessionMessageKind::UserInput, None, body)
                    }
                    ("runtime", "runtime_instruction")
                        if source.as_object()?.len() == 2
                            && source.get("subsystem")?.as_str()? == "chat"
                            && body.get("type")?.as_str()? == "runtime_instruction"
                            && body.get("instruction")?.as_str()? == "root_chat_turn_v1" =>
                    {
                        (
                            SessionMessageKind::RuntimeInstruction,
                            Some("root_chat_turn_v1"),
                            body.get("content")?,
                        )
                    }
                    _ => return None,
                };
            let id = message.id.as_str();
            if proof.get("id")?.as_str()? != id
                || proof.get("target_session_id")?.as_str()? != session_id
                || id.is_empty()
                || id.len() > 256
                || id.trim() != id
                || id.contains('/')
                || id.contains('\\')
                || id.contains("..")
            {
                return None;
            }
            let time = proof
                .get("created_at")?
                .as_str()?
                .parse::<chrono::DateTime<chrono::Utc>>()
                .ok()?;
            if time != message.created_at {
                return None;
            }
            let request = match content.get("skill_request").filter(|v| !v.is_null()) {
                Some(value) => Some(raw_request(value)?),
                None => None,
            };
            records.push(RawInputRecord(id, source, kind, wrapper, time, request));
        }
        Some(records)
    };
    let Some(records) = parse() else {
        return Unavailable(InputRequestUnavailable::InvalidData);
    };
    let Some(compact_bytes) = measure_input_request_view(session_id, execution_id, &records) else {
        return Unavailable(InputRequestUnavailable::CompactLimit { limit: 262144 });
    };
    let records = records
        .iter()
        .map(|record| ProjectedInputRequest {
            input_id: record.0.into(),
            source: if record.2 == SessionMessageKind::UserInput {
                SessionMessageSource::User
            } else {
                SessionMessageSource::Runtime {
                    subsystem: "chat".into(),
                }
            },
            kind: record.2,
            wrapper: record.3.map(String::from),
            created_at: record.4,
            request: record
                .5
                .as_ref()
                .map(|request| bamboo_domain::SessionSkillRequest {
                    mode: request.mode.map(String::from),
                    selections: request
                        .selections
                        .0
                        .iter()
                        .map(|value| {
                            let selection =
                                raw_selection(value).expect("validated borrowed selection");
                            bamboo_domain::SessionSkillSelection {
                                id: selection.id.into(),
                                source: selection.source.into(),
                                revision: selection.revision,
                                args: copy_input_request_value(selection.args),
                            }
                        })
                        .collect::<Vec<_>>()
                        .into_boxed_slice()
                        .into_vec(),
                }),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Available(BoundedInputRequestBatch {
        session_id: session_id.into(),
        execution_id: execution_id.into(),
        records,
        compact_bytes,
    })
}

// Called only after accepted I-W depth64/node8192/byte validation and full fit.
// Copy lengths, including nested strings/keys/arrays, never source capacities.
fn copy_input_request_value(value: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::String(value) => Value::String(value.as_str().into()),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(copy_input_request_value)
                .collect::<Vec<_>>()
                .into_boxed_slice()
                .into_vec(),
        ),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.as_str().into(), copy_input_request_value(value)))
                .collect(),
        ),
        scalar => scalar.clone(),
    }
}

#[cfg(test)]
const PENDING_INJECTED_MESSAGES_KEY: &str = "pending_injected_messages";

fn adopt_root_tool_authority(session: &mut Session, latest: &Session) -> Result<(), AgentError> {
    session
        .adopt_root_tool_authority_from(latest)
        .map_err(|error| {
            AgentError::Tool(format!(
                "authoritative Root tool authority refresh failed closed: {error}"
            ))
        })
}

fn ordinary_root_uses_tool_authority_proof(session: &Session) -> bool {
    session.kind == bamboo_domain::SessionKind::Root
        && session.parent_session_id.is_none()
        && session.authority_identity.is_ordinary()
}

/// Publish a genuinely new SDK Root through the Storage creation contract
/// before the first provider round. Storage must reject an old snapshot from a
/// deleted lifetime; a subsequent strict read binds the running snapshot to
/// the durable authority. Server-created Roots are already present here.
pub async fn ensure_initial_root_tool_authority(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
) -> Result<(), AgentError> {
    if !ordinary_root_uses_tool_authority_proof(session) {
        return Ok(());
    }
    let Some(storage) = storage else {
        return Ok(());
    };
    let mut latest = storage
        .load_runtime_control_plane(&session.id)
        .await
        .map_err(|error| {
            AgentError::Tool(format!(
                "initial Root tool authority read failed closed: {error}"
            ))
        })?;
    if latest.is_none() {
        storage.save_session(session).await.map_err(|error| {
            AgentError::Tool(format!(
                "initial Root authority publication failed closed: {error}"
            ))
        })?;
        latest = storage
            .load_runtime_control_plane(&session.id)
            .await
            .map_err(|error| {
                AgentError::Tool(format!(
                    "initial Root tool authority read failed closed: {error}"
                ))
            })?;
    }
    let latest = latest.ok_or_else(|| {
        AgentError::Tool("initial Root tool authority read failed closed: session missing".into())
    })?;
    adopt_root_tool_authority(session, &latest)
}

/// Recheck a Root before constructing its next provider catalog. Tool-boundary
/// checks protect execution, while this bounded control-plane read also removes
/// tools that were revoked after the prior round's last call. A Supervisor uses
/// its existing strict management-proof read, not the ordinary Root path.
pub async fn refresh_round_root_tool_authority(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
) -> Result<(), AgentError> {
    if session.kind != bamboo_domain::SessionKind::Root || session.parent_session_id.is_some() {
        return Ok(());
    }
    let Some(storage) = storage else {
        return Ok(());
    };
    let latest_read = if session.authority_identity.is_ordinary() {
        storage.load_runtime_control_plane(&session.id).await
    } else {
        storage.load_root_authority(&session.id).await
    };
    let latest = latest_read.map_err(|error| {
        AgentError::Tool(format!(
            "authoritative Root tool authority refresh failed closed: {error}"
        ))
    })?;
    let latest = latest.ok_or_else(|| {
        AgentError::Tool(
            "authoritative Root tool authority refresh failed closed: session missing".to_string(),
        )
    })?;
    adopt_root_tool_authority(session, &latest)
}

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
/// merged, which newly appended messages have a durable transcript checkpoint,
/// and the live per-session permission mode as it stands on disk (the
/// authoritative writer is `PATCH /sessions`).
#[derive(Debug, Default, Clone)]
pub struct TurnBoundaryRefresh {
    pub merged: usize,
    /// Newly appended messages whose transcript checkpoint completed at this
    /// boundary. Callers may publish these only after this function returns;
    /// recovery of an already-committed message is intentionally excluded.
    pub committed_messages: Vec<Message>,
    /// An unresolved ACK stops this activation even if its transcript and
    /// permanent receipt are already durable. Neither is proof that ACK returned
    /// successfully; the next safe boundary can retry existing recovery.
    pub admission_error: Option<String>,
    /// `None` when there was no storage / no on-disk session to read.
    pub disk_permission_mode: Option<bamboo_domain::SessionPermissionMode>,
}

/// Refresh the authoritative permission posture and Root tool authority at a
/// tool-dispatch safe boundary.
///
/// Unlike [`refresh_turn_boundary_with_inbox`], this deliberately never merges
/// messages, SessionInbox claims, or any other control-plane field. Sequential
/// tool calls may have externally visible side effects, so a configured durable
/// store must be readable before the next call is admitted: keeping an old Auto
/// posture after an Auto -> Default transition would execute without the newly
/// required approval. SDK/in-memory runtimes with no store retain their current
/// posture.
pub async fn refresh_tool_boundary_authorities(
    session: &mut Session,
    runtime_state: &mut AgentRuntimeState,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
) -> Result<(), AgentError> {
    let Some(storage) = storage else {
        return Ok(());
    };
    let latest = storage
        .load_runtime_control_plane(&session.id)
        .await
        .map_err(|error| {
            AgentError::Tool(format!(
                "authoritative permission posture refresh failed closed: {error}"
            ))
        })?
        .ok_or_else(|| {
            AgentError::Tool(
                "authoritative permission posture refresh failed closed: session missing"
                    .to_string(),
            )
        })?;
    let disk_mode = latest
        .agent_runtime_state
        .as_ref()
        .map(AgentRuntimeState::effective_permission_mode)
        .ok_or_else(|| {
            AgentError::Tool(
                "authoritative permission posture refresh failed closed: typed mode missing"
                    .to_string(),
            )
        })?;
    let current_mode = session
        .agent_runtime_state
        .as_ref()
        .map(AgentRuntimeState::effective_permission_mode)
        .unwrap_or_else(|| runtime_state.effective_permission_mode());

    let should_adopt_audit = bamboo_domain::disk_permission_posture_is_fresher(
        current_mode,
        &session.metadata,
        disk_mode,
        &latest.metadata,
    );
    let fresher_audit = bamboo_domain::fresher_disk_permission_audit(
        current_mode,
        &session.metadata,
        disk_mode,
        &latest.metadata,
    );
    if should_adopt_audit && fresher_audit.is_none() {
        return Err(AgentError::Tool(
            "authoritative permission posture refresh failed closed: complete audit unavailable"
                .to_string(),
        ));
    }

    // Commit the coherent pair only after every fallible validation above. A
    // failed refresh leaves the owned snapshot untouched and dispatch never
    // enters the executor.
    adopt_root_tool_authority(session, &latest)?;
    if let Some(audit) = fresher_audit {
        audit.write_to(&mut session.metadata);
    }
    runtime_state.set_permission_mode(disk_mode);
    session
        .agent_runtime_state
        .get_or_insert_with(AgentRuntimeState::default)
        .set_permission_mode(disk_mode);
    Ok(())
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

#[derive(Debug, Default)]
struct InboxAdmission {
    observation_complete: bool,
    merged: usize,
    committed_messages: Vec<Message>,
    admission_error: Option<String>,
}

async fn admit_session_inbox(
    session: &mut Session,
    inbox: &Arc<dyn SessionInboxPort>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
    active_run_id: Option<&str>,
) -> InboxAdmission {
    let Some(persistence) = persistence else {
        tracing::warn!(
            session_id = %session.id,
            "SessionInbox cannot admit without durable runtime persistence"
        );
        return InboxAdmission {
            admission_error: Some(INBOX_CLAIM_UNRESOLVED.to_string()),
            ..Default::default()
        };
    };
    match persistence
        .admit_root_inbox(session, inbox.clone(), active_run_id)
        .await
    {
        Ok(Some(admission)) => {
            if let Some(error) = &admission.admission_error {
                tracing::warn!(session_id = %session.id, %error, "owned Root Inbox admission stopped");
            }
            return InboxAdmission {
                observation_complete: admission.admission_error.is_none(),
                merged: admission.merged,
                committed_messages: admission.committed_messages,
                admission_error: admission
                    .admission_error
                    .map(|_| INBOX_CLAIM_UNRESOLVED.to_string()),
            };
        }
        Err(error) => {
            tracing::warn!(session_id = %session.id, %error, "owned Root Inbox admission failed");
            return InboxAdmission {
                admission_error: Some(INBOX_CLAIM_UNRESOLVED.to_string()),
                ..Default::default()
            };
        }
        Ok(None) => {}
    }
    let claims = match inbox.claim_for_turn(&session.id, 128, active_run_id).await {
        Ok(claims) => claims,
        Err(error) => {
            tracing::warn!(session_id = %session.id, %error, "failed to claim SessionInbox");
            return InboxAdmission {
                admission_error: Some(INBOX_CLAIM_UNRESOLVED.to_string()),
                ..Default::default()
            };
        }
    };

    let count = claims.len();
    let mut admission = InboxAdmission {
        observation_complete: count == 0,
        ..Default::default()
    };
    for (index, claim) in claims.into_iter().enumerate() {
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
                admission.admission_error = Some(INBOX_ACK_UNRESOLVED.to_string());
                break;
            }
            admission.observation_complete = index + 1 == count;
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
        // Capture only the message appended by this checkpoint. A recovered
        // transcript entry is already represented by its original append and
        // must not mint a duplicate durable change-feed coordinate.
        if !transcript_has_id {
            if let Some(message) = session
                .messages
                .iter()
                .find(|message| {
                    bamboo_domain::is_matching_session_message(message, &claim.envelope)
                })
                .cloned()
            {
                admission.committed_messages.push(message);
            }
        }
        if let Err(error) = inbox.ack(&session.id, &claim).await {
            tracing::warn!(
                session_id = %session.id,
                message_id = %claim.envelope.id,
                %error,
                "SessionInbox checkpoint succeeded but ack failed; dedupe will recover"
            );
            admission.admission_error = Some(INBOX_ACK_UNRESOLVED.to_string());
            break;
        }
        if !transcript_has_id {
            admission.merged += 1;
        }
        admission.observation_complete = index + 1 == count;
    }
    admission
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
    refresh_turn_boundary_with_inbox_for_run(session, storage, persistence, inbox, None).await
}

pub(crate) async fn refresh_turn_boundary_with_inbox_for_run(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
    inbox: Option<&Arc<dyn SessionInboxPort>>,
    active_run_id: Option<&str>,
) -> TurnBoundaryRefresh {
    refresh_turn_boundary_inner(session, storage, persistence, inbox, active_run_id, None).await
}

pub(crate) async fn refresh_turn_boundary_with_observation(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
    inbox: Option<&Arc<dyn SessionInboxPort>>,
    active_run_id: Option<&str>,
    execution_id: &str,
) -> (
    TurnBoundaryRefresh,
    crate::runtime::managers::lifecycle::InputObservation,
) {
    let mut observation = crate::runtime::managers::lifecycle::InputObservation::default();
    let refresh = refresh_turn_boundary_inner(
        session,
        storage,
        persistence,
        inbox,
        active_run_id,
        Some((execution_id, &mut observation)),
    )
    .await;
    (refresh, observation)
}

async fn refresh_turn_boundary_inner(
    session: &mut Session,
    storage: Option<&Arc<dyn bamboo_agent_core::storage::Storage>>,
    persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
    inbox: Option<&Arc<dyn SessionInboxPort>>,
    active_run_id: Option<&str>,
    capture: Option<(
        &str,
        &mut crate::runtime::managers::lifecycle::InputObservation,
    )>,
) -> TurnBoundaryRefresh {
    let mut read_complete = true;
    let latest = match storage {
        Some(storage) => match storage.load_session(&session.id).await {
            Ok(latest) => latest,
            Err(error) => {
                tracing::warn!(
                    session_id = %session.id,
                    %error,
                    "turn-boundary session refresh failed"
                );
                read_complete = false;
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
        let admission = admit_session_inbox(session, inbox, persistence, active_run_id).await;
        if let Some((execution_id, observation)) = capture {
            if read_complete
                && admission.observation_complete
                && admission.admission_error.is_none()
            {
                *observation = crate::runtime::managers::lifecycle::InputObservation::projected(
                    project_new_committed_inputs(
                        &session.id,
                        execution_id,
                        &admission.committed_messages,
                    ),
                );
            }
        }
        return TurnBoundaryRefresh {
            merged: admission.merged,
            committed_messages: admission.committed_messages,
            admission_error: admission.admission_error,
            disk_permission_mode,
        };
    }

    let Some(latest) = latest else {
        return TurnBoundaryRefresh {
            merged: 0,
            committed_messages: Vec::new(),
            admission_error: None,
            disk_permission_mode,
        };
    };

    // Read via the typed accessor (prefers `runtime_metadata`, falls back to the
    // legacy `pending_injected_messages` JSON string; defensive on malformed).
    let Some(messages) = latest.pending_injected_messages() else {
        return TurnBoundaryRefresh {
            merged: 0,
            committed_messages: Vec::new(),
            admission_error: None,
            disk_permission_mode,
        };
    };

    let mut merged = 0usize;
    let mut appended_messages = Vec::new();
    for msg in messages {
        if let Some(content) = msg.get("content").and_then(|v| v.as_str()) {
            let message = bamboo_agent_core::Message::user(content.to_string());
            appended_messages.push(message.clone());
            session.add_message(message);
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
            committed_messages: if saved { appended_messages } else { Vec::new() },
            admission_error: None,
            disk_permission_mode,
        };
    }

    TurnBoundaryRefresh {
        merged,
        committed_messages: Vec::new(),
        admission_error: None,
        disk_permission_mode,
    }
}

#[cfg(test)]
mod tests {
    fn ql_envelope(id: &str, ordinal: usize, bytes: Option<usize>) -> SessionMessageEnvelope {
        let mut envelope = SessionMessageEnvelope::user_input(id, "unchanged provider text");
        envelope.id = SessionMessageId::parse(format!("ql-{ordinal}")).unwrap();
        envelope.created_at =
            chrono::DateTime::from_timestamp(1700000000 + ordinal as i64, 0).unwrap();
        if let (Some(bytes), SessionMessageBody::Content(content)) = (bytes, &mut envelope.body) {
            content.skill_request = Some(bamboo_domain::SessionSkillRequest {
                mode: Some("exact-mode".into()),
                selections: vec![bamboo_domain::SessionSkillSelection {
                    id: format!("selection-{ordinal}"),
                    source: "user".into(),
                    revision: u64::MAX,
                    args: serde_json::json!({"payload":"x".repeat(bytes), "float": 1.125, "null":null}),
                }],
            });
        }
        envelope
    }

    #[test]
    fn ql_raw_projection_matches_accepted_qb_cost_and_exact_numeric_data() {
        use crate::runtime::managers::lifecycle::InputRequestProjection;
        for root in [false, true] {
            for request in [None, Some(0), Some(7000)] {
                let envelope = ql_envelope("ql-raw", 1, request);
                let envelope = if root {
                    envelope.with_root_chat_prompt("system".into()).unwrap()
                } else {
                    envelope
                };
                let skill_request = match &envelope.body {
                    SessionMessageBody::Content(content) => content.skill_request.as_ref(),
                    SessionMessageBody::RuntimeInstruction(body) => {
                        body.content.as_ref().unwrap().skill_request.as_ref()
                    }
                };
                let records = [BorrowedInputRequestRecord {
                    input_id: envelope.id.as_str(),
                    source: &envelope.source,
                    kind: envelope.kind,
                    wrapper: root.then_some("root_chat_turn_v1"),
                    created_at: envelope.created_at,
                    request: skill_request,
                }];
                let expected = project_input_request_batch("ql-raw", "actual-execution", &records);
                let actual = project_new_committed_inputs(
                    "ql-raw",
                    "actual-execution",
                    &[envelope.to_provider_message().unwrap()],
                );
                assert_eq!(actual, expected);
                let InputRequestProjection::Available(batch) = actual else {
                    panic!("valid fit")
                };
                assert_eq!(batch.records()[0].request.as_ref(), skill_request);
            }
        }
    }

    #[test]
    fn ql_raw_projection_normalizes_null_mode_and_missing_args_without_dropping_data() {
        let envelope = ql_envelope("ql-null", 0, Some(0));
        let mut message = envelope.to_provider_message().unwrap();
        let proof = &mut message.metadata.as_mut().unwrap()["session_message"];
        let request = &mut proof["body"]["skill_request"];
        request["mode"] = serde_json::Value::Null;
        request["selections"][0]
            .as_object_mut()
            .unwrap()
            .remove("args");
        let expected = bamboo_domain::SessionSkillRequest {
            mode: None,
            selections: vec![bamboo_domain::SessionSkillSelection {
                id: "selection-0".into(),
                source: "user".into(),
                revision: u64::MAX,
                args: serde_json::Value::Null,
            }],
        };
        let records = [BorrowedInputRequestRecord {
            input_id: envelope.id.as_str(),
            source: &envelope.source,
            kind: envelope.kind,
            wrapper: None,
            created_at: envelope.created_at,
            request: Some(&expected),
        }];
        assert_eq!(
            project_new_committed_inputs("ql-null", "e", &[message]),
            project_input_request_batch("ql-null", "e", &records)
        );
    }

    #[test]
    fn ql_raw_projection_rejects_unknown_small_deep_and_wide_programmatic_data() {
        use crate::runtime::managers::lifecycle::InputRequestProjection::Unavailable;
        let message = ql_envelope("ql-shape", 0, Some(0))
            .to_provider_message()
            .unwrap();
        let mut deep = serde_json::Value::Null;
        for _ in 0..65 {
            deep = serde_json::json!([deep]);
        }
        for args in [
            deep,
            serde_json::json!((0..8192).map(|_| 0).collect::<Vec<_>>()),
        ] {
            let mut invalid = message.clone();
            invalid.metadata.as_mut().unwrap()["session_message"]["body"]["skill_request"]
                ["selections"][0]["args"] = args;
            assert!(matches!(
                project_new_committed_inputs("ql-shape", "e", &[invalid]),
                Unavailable(_)
            ));
        }
        let mut invalid = message.clone();
        invalid.metadata.as_mut().unwrap()["session_message"]["body"]["skill_request"]["extra"] =
            1.into();
        assert!(matches!(
            project_new_committed_inputs("ql-shape", "e", &[invalid]),
            Unavailable(_)
        ));
        let mut invalid = message;
        invalid.metadata.as_mut().unwrap()["session_message"]["id"] = "wrong-id".into();
        assert!(matches!(
            project_new_committed_inputs("ql-shape", "e", &[invalid]),
            Unavailable(_)
        ));
        assert!(matches!(
            project_new_committed_inputs("ql-shape", "e", &[Message::user("old native")]),
            Unavailable(_)
        ));
    }

    #[tokio::test]
    async fn ql_real_new_order_none_overflow_nonew_and_fitting_restore() {
        let (_home, store, locked, inbox, mut session) = durable_inbox_fixture("ql-sequence").await;
        let storage: Arc<dyn Storage> = store;
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let mut current = None;
        for (start, count, request, fitting) in [
            (0, 2, Some(7), true),
            (2, 1, None, true),
            (3, 40, Some(7000), false),
            (43, 0, None, false),
            (43, 1, Some(9), true),
        ] {
            for ordinal in start..start + count {
                deliver_interrupt_eligible(&inbox, &ql_envelope(&session.id, ordinal, request))
                    .await;
            }
            let (refresh, observation) = refresh_turn_boundary_with_observation(
                &mut session,
                Some(&storage),
                Some(&persistence),
                Some(&inbox),
                None,
                "actual-sequence-run",
            )
            .await;
            assert_eq!(refresh.merged, count);
            assert_eq!(refresh.committed_messages.len(), count);
            assert!(refresh.admission_error.is_none());
            if count == 0 {
                assert!(observation.is_successful_no_new_input());
            }
            if count == 40 {
                assert!(observation.is_unavailable());
            }
            let session_id = session.id.clone();
            observation.update_current(&mut current, &session_id, "actual-sequence-run");
            assert_eq!(current.is_some(), fitting);
            if let Some(batch) = &current {
                assert_eq!(
                    batch
                        .records()
                        .iter()
                        .map(|r| r.input_id.as_str())
                        .collect::<Vec<_>>(),
                    (start..start + count)
                        .map(|i| format!("ql-{i}"))
                        .collect::<Vec<_>>()
                );
                assert_eq!(batch.records()[0].request.is_some(), request.is_some());
                assert_eq!(batch.execution_id(), "actual-sequence-run");
            }
            let backlog = inbox.inspect(&session_id).await.unwrap();
            assert_eq!(backlog.pending + backlog.claimed, 0);
        }
    }

    #[tokio::test]
    async fn ql_original_checkpoint_failure_is_unavailable_and_cold_retry_is_new_once() {
        let (_home, store, locked, inbox, mut session) =
            durable_inbox_fixture("ql-checkpoint-failure").await;
        let storage: Arc<dyn Storage> = store;
        let faulted = Arc::new(FaultingPersistence {
            inner: locked,
            fail_checkpoint: std::sync::atomic::AtomicBool::new(true),
        });
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = faulted.clone();
        let envelope = ql_envelope(&session.id, 0, Some(9));
        deliver_interrupt_eligible(&inbox, &envelope).await;
        let (refresh, observation) = refresh_turn_boundary_with_observation(
            &mut session,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
            None,
            "e",
        )
        .await;
        assert!(
            refresh.admission_error.is_none(),
            "preserve original non-error checkpoint branch"
        );
        assert_eq!(refresh.merged, 0);
        assert!(refresh.committed_messages.is_empty());
        assert!(observation.is_unavailable());
        assert!(!session
            .messages
            .iter()
            .any(|m| m.id == envelope.id.as_str()));
        assert!(!inbox.was_admitted(&session.id, &envelope.id).await.unwrap());
        faulted.fail_checkpoint.store(false, Ordering::SeqCst);
        let (refresh, observation) = refresh_turn_boundary_with_observation(
            &mut session,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
            None,
            "e",
        )
        .await;
        assert_eq!(refresh.merged, 1);
        assert_eq!(
            observation.new_inputs().unwrap().records()[0].input_id,
            envelope.id.as_str()
        );
        let (_, observation) = refresh_turn_boundary_with_observation(
            &mut session,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
            None,
            "e",
        )
        .await;
        assert!(observation.is_successful_no_new_input());
    }

    #[tokio::test]
    async fn ql_unresolved_ack_preserves_prefix_events_but_never_new_and_recovery_is_nonew() {
        for permanent in [false, true] {
            let (_home, store, locked, real, mut session) =
                durable_inbox_fixture("ql-ack-failure").await;
            let storage: Arc<dyn Storage> = store;
            let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
            let inbox: Arc<dyn SessionInboxPort> = Arc::new(AckAfterPersistErrorInbox {
                inner: real.clone(),
                fail_before_once: std::sync::atomic::AtomicBool::new(!permanent),
                fail_after_once: std::sync::atomic::AtomicBool::new(permanent),
            });
            let envelope = ql_envelope(&session.id, 0, Some(9));
            deliver_interrupt_eligible(&inbox, &envelope).await;
            let (refresh, observation) = refresh_turn_boundary_with_observation(
                &mut session,
                Some(&storage),
                Some(&persistence),
                Some(&inbox),
                None,
                "e",
            )
            .await;
            assert_eq!(
                refresh.admission_error.as_deref(),
                Some(INBOX_ACK_UNRESOLVED)
            );
            assert_eq!(
                refresh.committed_messages.len(),
                1,
                "retain original pre-ACK durable prefix"
            );
            assert!(observation.is_unavailable());
            let mut cold = storage.load_session(&session.id).await.unwrap().unwrap();
            let (refresh, observation) = refresh_turn_boundary_with_observation(
                &mut cold,
                Some(&storage),
                Some(&persistence),
                Some(&inbox),
                None,
                "cold-new-run",
            )
            .await;
            assert_eq!(refresh.merged, 0);
            assert!(refresh.committed_messages.is_empty());
            assert!(observation.is_successful_no_new_input());
            assert_eq!(
                cold.messages
                    .iter()
                    .filter(|m| m.id == envelope.id.as_str())
                    .count(),
                1
            );
            assert_eq!(real.inspect(&session.id).await.unwrap().claimed, 0);
        }
    }

    #[tokio::test]
    async fn ql_owned_root_new_and_recovered_are_distinguished_after_exact_ack() {
        use bamboo_domain::RuntimeSessionPersistence;
        for recovered in [false, true] {
            let (_home, store, locked, inbox, mut root) =
                durable_inbox_fixture("ql-owned-root").await;
            let envelope = ql_envelope(&root.id, 0, Some(17))
                .with_root_chat_prompt("Root system".into())
                .unwrap();
            if recovered {
                root.add_message(envelope.to_provider_message().unwrap());
                store.save_session(&root).await.unwrap();
            }
            let repo = crate::SessionRepository::new(Arc::default(), store.clone(), locked)
                .with_root_actor_directory(store.clone());
            let binding = repo
                .bind_root_actor_execution(&root, "actual-owned-root-run")
                .await
                .unwrap()
                .unwrap();
            let storage: Arc<dyn Storage> = store;
            deliver_interrupt_eligible(&inbox, &envelope).await;
            let (refresh, observation) = refresh_turn_boundary_with_observation(
                &mut root,
                Some(&storage),
                Some(&binding.persistence),
                Some(&inbox),
                None,
                "actual-owned-root-run",
            )
            .await;
            assert!(refresh.admission_error.is_none());
            assert_eq!(refresh.merged, usize::from(!recovered));
            assert_eq!(observation.is_successful_no_new_input(), recovered);
            if let Some(batch) = observation.new_inputs() {
                let record = &batch.records()[0];
                assert_eq!(record.input_id, envelope.id.as_str());
                assert_eq!(
                    record.source,
                    SessionMessageSource::Runtime {
                        subsystem: "chat".into()
                    }
                );
                assert_eq!(record.kind, SessionMessageKind::RuntimeInstruction);
                assert_eq!(record.wrapper.as_deref(), Some("root_chat_turn_v1"));
                assert_eq!(record.created_at, envelope.created_at);
                assert_eq!(
                    record.request.as_ref().unwrap().selections[0].revision,
                    u64::MAX
                );
            }
            assert!(inbox.was_admitted(&root.id, &envelope.id).await.unwrap());
            assert_eq!(inbox.inspect(&root.id).await.unwrap().pending, 0);
            assert_eq!(
                root.messages
                    .iter()
                    .filter(|m| m.id == envelope.id.as_str())
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn ql_new_peer_user_presentation_is_unavailable_without_stale_fallback() {
        let (_home, store, locked, inbox, mut session) = durable_inbox_fixture("ql-peer").await;
        let storage: Arc<dyn Storage> = store;
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let user = ql_envelope(&session.id, 0, Some(8));
        deliver_interrupt_eligible(&inbox, &user).await;
        let (_, observation) = refresh_turn_boundary_with_observation(
            &mut session,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
            None,
            "e",
        )
        .await;
        let mut current = None;
        observation.update_current(&mut current, &session.id, "e");
        assert!(current.is_some());
        let mut peer = ql_envelope(&session.id, 1, None);
        peer.source = SessionMessageSource::Session {
            session_id: "real-peer".into(),
        };
        peer.kind = SessionMessageKind::PeerMessage;
        assert_eq!(
            peer.to_provider_message().unwrap().role,
            bamboo_domain::Role::User
        );
        deliver_interrupt_eligible(&inbox, &peer).await;
        let (refresh, observation) = refresh_turn_boundary_with_observation(
            &mut session,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
            None,
            "e",
        )
        .await;
        assert_eq!(refresh.merged, 1);
        assert!(observation.is_unavailable());
        observation.update_current(&mut current, &session.id, "e");
        assert!(current.is_none());
        assert!(inbox.was_admitted(&session.id, &peer.id).await.unwrap());
    }

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

    #[tokio::test]
    async fn tool_boundary_permission_refresh_preserves_legacy_same_mode_without_audit() {
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        let mut persisted = Session::new("legacy-same-mode", "model");
        persisted.agent_runtime_state = Some(AgentRuntimeState::new("run"));
        persisted.add_message(bamboo_agent_core::Message::user(
            "must stay on disk until the next turn boundary",
        ));
        storage.save_session(&persisted).await.unwrap();

        let mut running = persisted.clone();
        running.messages.clear();
        let mut runtime_state = AgentRuntimeState::new("run");
        refresh_tool_boundary_authorities(&mut running, &mut runtime_state, Some(&storage))
            .await
            .expect("same-mode legacy posture is not a transition to adopt");

        assert_eq!(
            runtime_state.effective_permission_mode(),
            bamboo_domain::SessionPermissionMode::Default
        );
        assert!(
            running.messages.is_empty(),
            "tool-boundary refresh must not merge durable messages mid-round"
        );
        assert!(bamboo_domain::PermissionAuditSnapshot::from_metadata(&running.metadata).is_none());
    }

    #[tokio::test]
    async fn first_unpersisted_root_is_published_before_provider_round() {
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        let mut fresh = Session::new("fresh-sdk-root", "model");
        fresh.agent_runtime_state = Some(AgentRuntimeState::new("run"));
        ensure_initial_root_tool_authority(&mut fresh, Some(&storage))
            .await
            .expect("new SDK Root is durably published first");
        refresh_round_root_tool_authority(&mut fresh, Some(&storage))
            .await
            .expect("first provider round reads the published Root");
        assert!(fresh.allows_model_tool_execution("Bash"));
        assert!(storage
            .load_runtime_control_plane(&fresh.id)
            .await
            .unwrap()
            .is_some());

        let mut runtime_state = AgentRuntimeState::new("run");
        refresh_tool_boundary_authorities(&mut fresh, &mut runtime_state, Some(&storage))
            .await
            .expect("the saved control plane admits ordinary execution");

        let mut selected = Session::new("selected-sdk-root", "model");
        selected.set_root_orchestration_only(true).unwrap();
        assert!(
            refresh_round_root_tool_authority(&mut selected, Some(&storage))
                .await
                .is_err()
        );

        let mut resumed = Session::new("resumed-sdk-root", "model");
        resumed.add_message(Message::assistant("prior round", None));
        assert!(
            refresh_round_root_tool_authority(&mut resumed, Some(&storage))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn revoked_early_root_cannot_be_recreated_by_initial_runtime_publication() {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(home.path().join("sessions"))
                .await
                .unwrap(),
        );
        let stale = Session::new("revoked-early-root", "model");
        store.save_session(&stale).await.unwrap();
        assert!(store.delete_session(&stale.id).await.unwrap());
        let storage: Arc<dyn Storage> = store;
        let mut running = stale;

        let error = ensure_initial_root_tool_authority(&mut running, Some(&storage))
            .await
            .expect_err("ordinary save must not revive a deleted Root lifetime");
        assert!(error.to_string().contains("failed closed"));
        assert!(storage
            .load_runtime_control_plane(&running.id)
            .await
            .unwrap()
            .is_none());
        assert!(
            refresh_round_root_tool_authority(&mut running, Some(&storage))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn old_sdk_root_cannot_adopt_a_recreated_root_lifetime() {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(home.path().join("sessions"))
                .await
                .unwrap(),
        );
        let mut old = Session::new("recreated-sdk-root", "model");
        old.agent_runtime_state = Some(AgentRuntimeState::new("old-run"));
        store.save_session(&old).await.unwrap();
        assert!(store.delete_session(&old.id).await.unwrap());

        let mut replacement = store
            .recreate_root_session(&old.id, "new-model")
            .await
            .unwrap();
        assert_ne!(replacement.created_at, old.created_at);
        replacement.agent_runtime_state = Some(AgentRuntimeState::new("new-run"));
        store.save_session(&replacement).await.unwrap();
        let storage: Arc<dyn Storage> = store;

        let old_birth = old.created_at;
        let mut running = old;
        let error = ensure_initial_root_tool_authority(&mut running, Some(&storage))
            .await
            .expect_err("pre-provider proof must reject the old SDK snapshot");
        assert!(error.to_string().contains("Root tool authority"));
        assert!(
            refresh_round_root_tool_authority(&mut running, Some(&storage))
                .await
                .is_err(),
            "subsequent catalog refresh must reject the same stale lifetime"
        );
        let mut runtime_state = AgentRuntimeState::new("old-run");
        let error =
            refresh_tool_boundary_authorities(&mut running, &mut runtime_state, Some(&storage))
                .await
                .expect_err("pre-dispatch proof must reject the old SDK snapshot");
        assert!(error.to_string().contains("Root tool authority"));
        assert_eq!(running.created_at, old_birth);
        assert_eq!(runtime_state.run_id, "old-run");
    }

    #[tokio::test]
    async fn active_supervisor_adopts_mode_before_next_provider_catalog() {
        let home = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(
            bamboo_storage::SessionStoreV2::new(home.path().join("sessions"))
                .await
                .unwrap(),
        );
        store
            .get_or_create_default_supervisor("model")
            .await
            .unwrap();
        let id = bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID;
        let mut running = store.load_root_authority(id).await.unwrap().unwrap();
        assert!(running.allows_model_tool_execution("Bash"));
        let operation = bamboo_domain::RootModeOperationRequest {
            session_id: id.to_string(),
            operation_id: format!("0:{}", uuid::Uuid::new_v4()),
            birth_token: running.root_mode_birth_token(),
            expected_epoch: 0,
            requested_enabled: true,
            action: bamboo_domain::RootModeOperationAction::Select,
        };
        assert!(matches!(
            store.root_mode_operation(&operation).await.unwrap(),
            bamboo_domain::RootModeOperationDecision::Terminal(_)
        ));

        let storage: Arc<dyn Storage> = store;
        refresh_round_root_tool_authority(&mut running, Some(&storage))
            .await
            .expect("strict Supervisor management proof remains valid");
        assert!(running.root_orchestration_only_enabled());
        assert!(!running.allows_model_tool_execution("Bash"));
        assert!(running.allows_model_tool_execution("SubAgent"));
        assert_eq!(running.root_mode_transition_epoch, 1);
        assert_eq!(running.root_mode_operations.len(), 1);
        running.add_message(Message::assistant("Supervisor result", None));
        let persistence =
            std::sync::Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
        let repository =
            crate::SessionRepository::new(std::sync::Arc::default(), storage.clone(), persistence);
        repository.save(&mut running).await.unwrap();
        let durable = storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(durable.root_mode_transition_epoch, 1);
        assert_eq!(
            durable.messages.last().unwrap().content,
            "Supervisor result"
        );
    }

    #[tokio::test]
    async fn ordinary_root_at_reserved_id_still_refreshes_before_provider() {
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        let mut running = Session::new(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID, "model");
        let mut latest = running.clone();
        latest.set_root_orchestration_only(true).unwrap();
        storage.save_session(&latest).await.unwrap();

        refresh_round_root_tool_authority(&mut running, Some(&storage))
            .await
            .expect("ordinary identity at the reserved ID still needs durable proof");
        assert!(running.root_orchestration_only_enabled());
        assert!(!running.allows_model_tool_execution("Bash"));
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
    async fn owned_inbox_claim_requires_current_owner_before_provider_admission() {
        use bamboo_domain::{SessionInboxConsumerId, SessionInboxLeaseRequest};

        let (_home, store, locked, inbox, mut running) =
            durable_inbox_fixture("owned-claim-boundary").await;
        let envelope = SessionMessageEnvelope::user_input(&running.id, "leased input");
        deliver_interrupt_eligible(&inbox, &envelope).await;
        let lease = SessionInboxLeaseRequest {
            consumer: SessionInboxConsumerId::new(),
            now: chrono::Utc::now(),
            duration: chrono::Duration::seconds(30),
        };
        let owned = inbox
            .claim_owned(&running.id, 1, None, &lease)
            .await
            .unwrap();
        assert_eq!(owned.len(), 1);

        let storage: Arc<dyn Storage> = store.clone();
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let refresh = refresh_turn_boundary_with_inbox(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
        )
        .await;
        assert_eq!(
            refresh.admission_error.as_deref(),
            Some(INBOX_CLAIM_UNRESOLVED)
        );
        assert_eq!(refresh.merged, 0);
        assert!(!running
            .messages
            .iter()
            .any(|message| message.id == envelope.id.as_str()));
        assert!(!inbox.was_admitted(&running.id, &envelope.id).await.unwrap());
        let cold = store.load_session(&running.id).await.unwrap().unwrap();
        assert!(!cold
            .messages
            .iter()
            .any(|message| message.id == envelope.id.as_str()));
        assert_eq!(
            inbox
                .inspect_owned_leases(&running.id, 1, lease.now)
                .await
                .unwrap()
                .len(),
            1
        );
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

    #[derive(Default)]
    struct AckBoundaryProvider(AtomicUsize);

    #[async_trait::async_trait]
    impl bamboo_llm::LLMProvider for AckBoundaryProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<bamboo_llm::LLMStream> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::iter(vec![
                Ok(bamboo_llm::LLMChunk::Token("done".into())),
                Ok(bamboo_llm::LLMChunk::Done),
            ])))
        }
    }

    async fn run_ack_boundary(
        session: &mut Session,
        storage: Arc<dyn Storage>,
        persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence>,
        inbox: Arc<dyn SessionInboxPort>,
        provider: Arc<AckBoundaryProvider>,
    ) -> (Result<(), AgentError>, Vec<bamboo_agent_core::AgentEvent>) {
        let (events, mut received) = tokio::sync::mpsc::channel(128);
        let result = crate::runtime::runner::run_agent_loop_with_config(
            session,
            "continue".into(),
            events,
            provider,
            Arc::new(bamboo_tools::BuiltinToolExecutorBuilder::new().build()),
            tokio_util::sync::CancellationToken::new(),
            crate::runtime::config::AgentLoopConfig {
                storage: Some(storage),
                persistence: Some(persistence),
                session_inbox: Some(inbox),
                system_prompt: Some("system".into()),
                model_name: Some("model".into()),
                run_budget: bamboo_config::RunBudgetConfig {
                    max_rounds: Some(1),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;
        let mut observed = Vec::new();
        while let Ok(event) = received.try_recv() {
            observed.push(event);
        }
        (result, observed)
    }

    #[tokio::test]
    async fn unresolved_ack_stops_shared_provider_round_and_cold_retry_deduplicates() {
        for permanent in [false, true] {
            for after_receipt in [false, true] {
                let id = format!("ack-stop-{permanent}-{after_receipt}");
                let (home, store, locked, real_inbox, mut running) =
                    durable_inbox_fixture(&id).await;
                let envelope = SessionMessageEnvelope::user_input(&id, "durable input");
                deliver_interrupt_eligible(&real_inbox, &envelope).await;
                if permanent {
                    // Model receipt publication followed by an interrupted cur
                    // removal using the actual claim bytes and permanent receipt.
                    let claim = real_inbox.claim(&id, 1).await.unwrap().remove(0);
                    let path = store
                        .bamboo_home_dir()
                        .join(store.resolve_rel_path(&id).await.unwrap())
                        .join("inbox/cur")
                        .join(&claim.claim_id);
                    let raw_claim = tokio::fs::read(&path).await.unwrap();
                    running.add_message(envelope.to_provider_message().unwrap());
                    running
                        .session_inbox_admission_mut()
                        .record(envelope.id.clone(), claim.generation);
                    locked
                        .checkpoint_runtime_session(&mut running)
                        .await
                        .unwrap();
                    real_inbox.ack(&id, &claim).await.unwrap();
                    tokio::fs::write(&path, raw_claim).await.unwrap();
                    assert!(real_inbox.was_admitted(&id, &envelope.id).await.unwrap());
                }
                let faulted: Arc<dyn SessionInboxPort> = Arc::new(AckAfterPersistErrorInbox {
                    inner: real_inbox.clone(),
                    fail_before_once: std::sync::atomic::AtomicBool::new(!after_receipt),
                    fail_after_once: std::sync::atomic::AtomicBool::new(after_receipt),
                });
                let provider = Arc::new(AckBoundaryProvider::default());
                let (result, events) = run_ack_boundary(
                    &mut running,
                    store.clone(),
                    locked,
                    faulted,
                    provider.clone(),
                )
                .await;
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("SessionInbox ACK unresolved"));
                assert_eq!(provider.0.load(Ordering::SeqCst), 0);
                assert_eq!(
                    events
                        .iter()
                        .filter(|event| matches!(event,
                    bamboo_agent_core::AgentEvent::MessageAppended { message_id, .. }
                    if message_id == envelope.id.as_str()))
                        .count(),
                    usize::from(!permanent)
                );
                assert_eq!(
                    real_inbox.was_admitted(&id, &envelope.id).await.unwrap(),
                    permanent || after_receipt
                );
                assert_eq!(
                    real_inbox.inspect(&id).await.unwrap().claimed,
                    usize::from(!after_receipt)
                );

                let reopened = Arc::new(
                    bamboo_storage::SessionStoreV2::new(home.path().to_path_buf())
                        .await
                        .unwrap(),
                );
                let mut retry = reopened.load_session(&id).await.unwrap().unwrap();
                assert_eq!(
                    retry
                        .messages
                        .iter()
                        .filter(|message| bamboo_domain::is_matching_session_message(
                            message, &envelope
                        ))
                        .count(),
                    1
                );
                assert!(retry
                    .session_inbox_admission()
                    .unwrap()
                    .contains(&envelope.id));
                let storage: Arc<dyn Storage> = reopened.clone();
                let persistence =
                    Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
                let inbox: Arc<dyn SessionInboxPort> =
                    Arc::new(bamboo_storage::FileSessionInbox::new(
                        reopened.clone(),
                        bamboo_domain::SessionInboxLimits::default(),
                    ));
                let (result, events) = run_ack_boundary(
                    &mut retry,
                    storage,
                    persistence,
                    inbox.clone(),
                    provider.clone(),
                )
                .await;
                result.unwrap();
                assert_eq!(provider.0.load(Ordering::SeqCst), 1);
                assert!(!events.iter().any(|event| matches!(event,
                    bamboo_agent_core::AgentEvent::MessageAppended { message_id, .. }
                    if message_id == envelope.id.as_str())));
                assert!(inbox.was_admitted(&id, &envelope.id).await.unwrap());
                let backlog = inbox.inspect(&id).await.unwrap();
                assert_eq!(backlog.pending + backlog.claimed, 0);
                let durable = reopened.load_session(&id).await.unwrap().unwrap();
                assert_eq!(
                    durable
                        .messages
                        .iter()
                        .filter(|message| bamboo_domain::is_matching_session_message(
                            message, &envelope
                        ))
                        .count(),
                    1
                );
            }
        }
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
    async fn task_end_guidance_survives_later_round_admission_and_launches_one_successor() {
        let (_temp, store, locked, inbox, mut running) =
            durable_inbox_fixture("guidance-modes").await;
        let storage: Arc<dyn Storage> = store;
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> = locked;
        let router = crate::SessionActivationRouter::new();
        router.set_inbox(inbox.clone());
        let reservations = Arc::new(AtomicUsize::new(0));
        let launches = Arc::new(AtomicUsize::new(0));
        router
            .set_spawner(Arc::new(BacklogActivationSpawner {
                inbox: inbox.clone(),
                reservations: reservations.clone(),
                launches: launches.clone(),
            }))
            .await;
        let mut registration = router.register_run(&running.id, "run-a").await.unwrap();
        let mut deferred = SessionMessageEnvelope::user_input(&running.id, "task end");
        deferred.correlation_id = Some("session-guidance-after-run:run-a".into());
        let mut immediate = SessionMessageEnvelope::user_input(&running.id, "round end");
        immediate.correlation_id = Some("session-guidance".into());
        for envelope in [&deferred, &immediate] {
            let receipt = deliver_interrupt_eligible(&inbox, envelope).await;
            router
                .request_activation(&running.id, receipt.generation)
                .await
                .unwrap();
        }
        let result = refresh_turn_boundary_with_inbox_for_run(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
            Some("run-a"),
        )
        .await;
        assert_eq!(result.merged, 1);
        assert_eq!(result.committed_messages.len(), 1);
        assert_eq!(result.committed_messages[0].id, immediate.id.as_str());
        assert!(running
            .messages
            .iter()
            .any(|message| message.id == immediate.id.as_str()));
        assert!(!running
            .messages
            .iter()
            .any(|message| message.id == deferred.id.as_str()));
        let cursor = running
            .session_inbox_admission()
            .unwrap()
            .last_admitted_sequence;
        assert_eq!(cursor, 2);
        assert_eq!(inbox.pending_guidance(&running.id).await.unwrap().len(), 1);
        registration.begin_finalization().await;
        assert_eq!(
            registration.finish(cursor).await.unwrap(),
            Some(SessionActivationDisposition::ActivationReserved)
        );
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        let mut next = router
            .register_run(&running.id, "successor-1")
            .await
            .unwrap();
        let result = refresh_turn_boundary_with_inbox_for_run(
            &mut running,
            Some(&storage),
            Some(&persistence),
            Some(&inbox),
            Some("successor-1"),
        )
        .await;
        assert_eq!(result.merged, 1);
        assert_eq!(result.committed_messages.len(), 1);
        assert_eq!(result.committed_messages[0].id, deferred.id.as_str());
        assert_eq!(
            running
                .messages
                .iter()
                .filter(|message| message.id == deferred.id.as_str())
                .count(),
            1
        );
        next.begin_finalization().await;
        assert_eq!(next.finish(cursor).await.unwrap(), None);
        assert_eq!(reservations.load(Ordering::SeqCst), 1);
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

#[cfg(test)]
mod input_request_projection_tests {
    use super::*;
    use crate::runtime::managers::lifecycle::{
        BoundedInputRequestBatch,
        InputRequestProjection::{Available, Unavailable},
        InputRequestUnavailable,
    };
    use bamboo_domain::{SessionSkillRequest, SessionSkillSelection};

    fn request() -> SessionSkillRequest {
        SessionSkillRequest {
            selections: vec![SessionSkillSelection {
                id: "skill".into(),
                source: "user".into(),
                revision: u64::MAX,
                args: serde_json::json!({"n": u64::MAX, "a": [1, 1.25, null]}),
            }],
            mode: None,
        }
    }
    fn record(request: Option<&SessionSkillRequest>) -> BorrowedInputRequestRecord<'_> {
        BorrowedInputRequestRecord {
            input_id: "u",
            source: &SessionMessageSource::User,
            kind: SessionMessageKind::UserInput,
            wrapper: None,
            created_at: chrono::DateTime::from_timestamp(1, 0).unwrap(),
            request,
        }
    }
    fn available(
        session: &str,
        execution: &str,
        records: &[BorrowedInputRequestRecord<'_>],
    ) -> BoundedInputRequestBatch {
        match project_input_request_batch(session, execution, records) {
            Available(batch) => batch,
            Unavailable(error) => panic!("unexpected bounded diagnostic {error:?}"),
        }
    }

    #[test]
    fn input_request_projection_charges_exact_private_framing_escaping_and_null() {
        let mut row = record(None);
        row.input_id = "u\"\t界";
        let rows = [row];
        let expected = r#"{"session_id":"s\n界","execution_id":"e\"","records":[["u\"\t界",{"type":"user"},"user_input",null,"1970-01-01T00:00:01Z",null]]}"#;
        let encoded = serde_json::to_vec(&InputRequestMeasurement {
            session_id: "s\n界",
            execution_id: "e\"",
            records: InputRequestRecordsMeasurement(&rows),
        })
        .unwrap();
        assert_eq!(encoded, expected.as_bytes());
        let batch = available("s\n界", "e\"", &rows);
        assert_eq!(batch.compact_bytes(), expected.len());
        assert_eq!(batch.session_id(), "s\n界");
        assert_eq!(batch.execution_id(), "e\"");
        assert_eq!(batch.records()[0].input_id, row.input_id);
        assert_eq!(batch.records()[0].request, None);
        let mut data = request();
        let without_mode = available("s", "e", &[record(Some(&data))]).compact_bytes();
        data.mode = Some("review".into());
        assert_eq!(
            available("s", "e", &[record(Some(&data))]).compact_bytes(),
            without_mode + r#","mode":"review""#.len()
        );
    }

    #[test]
    fn input_request_projection_accepts_exact_whole_limit_then_rejects_one_byte() {
        let mut data = SessionSkillRequest {
            selections: (0..32)
                .map(|i| SessionSkillSelection {
                    id: format!("{i:x}"),
                    source: "user".into(),
                    revision: 1,
                    args: serde_json::Value::String(String::new()),
                })
                .collect(),
            mode: None,
        };
        let baseline = serde_json::to_vec(&InputRequestMeasurement {
            session_id: "s",
            execution_id: "e",
            records: InputRequestRecordsMeasurement(&[record(Some(&data))]),
        })
        .unwrap()
        .len();
        let padding = 262144 - baseline;
        for (i, selection) in data.selections.iter_mut().enumerate() {
            selection.args =
                serde_json::Value::String("x".repeat(padding / 32 + usize::from(i < padding % 32)));
        }
        data.validate().unwrap();
        let batch = available("s", "e", &[record(Some(&data))]);
        assert_eq!(batch.compact_bytes(), 262144);
        assert_eq!(batch.records()[0].request.as_ref().unwrap(), &data);
        if let serde_json::Value::String(value) = &mut data.selections[0].args {
            value.push('x');
        }
        data.validate().unwrap(); // Individually valid; only whole-batch overhead overflows.
        assert_eq!(
            project_input_request_batch("s", "e", &[record(Some(&data))]),
            Unavailable(InputRequestUnavailable::CompactLimit { limit: 262144 })
        );
        let rows = [record(Some(&data)), record(Some(&data))];
        assert!(matches!(
            project_input_request_batch("s", "e", &rows),
            Unavailable(InputRequestUnavailable::CompactLimit { .. })
        ));
    }

    #[test]
    fn input_request_projection_preserves_order_and_charges_every_none_record() {
        let ids: Vec<_> = (0..129).map(|i| format!("u-{i}")).collect();
        let rows: Vec<_> = ids
            .iter()
            .map(|id| BorrowedInputRequestRecord {
                input_id: id,
                ..record(None)
            })
            .collect();
        let batch = available("s", "e", &rows[..128]);
        assert_eq!(batch.records().len(), 128);
        assert_eq!(
            batch
                .records()
                .iter()
                .map(|r| &r.input_id)
                .collect::<Vec<_>>(),
            ids[..128].iter().collect::<Vec<_>>()
        );
        assert!(batch.records().iter().all(|row| row.request.is_none()));
        let one = available("s", "e", &rows[..1]);
        assert!(batch.compact_bytes() > one.compact_bytes() * 50);
        assert_eq!(
            project_input_request_batch("s", "e", &rows),
            Unavailable(InputRequestUnavailable::RecordLimit {
                count: 129,
                limit: 128
            })
        );
        let empty = available("s", "e", &[]);
        assert!(empty.records().is_empty());
        assert_eq!(
            empty.compact_bytes(),
            r#"{"session_id":"s","execution_id":"e","records":[]}"#.len()
        );
    }

    #[test]
    fn input_request_projection_rejects_original_invalid_data_without_partial_output() {
        for id in ["", " ", " u", "u ", "a/b", "a\\b", "a..b", &"界".repeat(86)] {
            let rows = [
                record(None),
                BorrowedInputRequestRecord {
                    input_id: id,
                    ..record(None)
                },
            ];
            assert_eq!(
                project_input_request_batch("s", "e", &rows),
                Unavailable(InputRequestUnavailable::InvalidData)
            );
        }
        let canonical = format!("{}\t界", "x".repeat(252));
        assert_eq!(canonical.len(), 256);
        assert_eq!(
            available(
                "s",
                "e",
                &[BorrowedInputRequestRecord {
                    input_id: &canonical,
                    ..record(None)
                }]
            )
            .records()[0]
                .input_id,
            canonical
        );
        let mut data = request();
        for depth in [64, 65, 256] {
            data.selections[0].args = (0..depth).fold(serde_json::Value::Null, |value, _| {
                serde_json::Value::Array(vec![value])
            });
            let result =
                project_input_request_batch("s", "e", &[record(None), record(Some(&data))]);
            assert_eq!(matches!(result, Available(_)), depth == 64);
            if depth != 64 {
                assert_eq!(result, Unavailable(InputRequestUnavailable::InvalidData));
            }
        }
        data.selections[0].args = serde_json::Value::Array(vec![serde_json::Value::Null; 8192]);
        assert_eq!(
            project_input_request_batch("s", "e", &[record(Some(&data))]),
            Unavailable(InputRequestUnavailable::InvalidData)
        );
        data.selections[0].args = serde_json::Value::Null;
        for mode in ["", "UPPER", "a_b", &"x".repeat(65)] {
            data.mode = Some(mode.into());
            assert_eq!(
                project_input_request_batch("s", "e", &[record(Some(&data))]),
                Unavailable(InputRequestUnavailable::InvalidData)
            );
        }
        data.mode = Some("x".repeat(64));
        available("s", "e", &[record(Some(&data))]);
        data.selections.clear();
        assert_eq!(
            project_input_request_batch("s", "e", &[record(Some(&data))]),
            Unavailable(InputRequestUnavailable::InvalidData)
        );
    }

    #[test]
    fn input_request_projection_normalizes_nested_capacities_and_outlives_original_data() {
        fn excess(value: &str) -> String {
            let mut s = String::with_capacity(100000);
            s.push_str(value);
            s
        }
        fn nodes(value: &serde_json::Value) -> usize {
            1 + match value {
                serde_json::Value::Array(v) => v.iter().map(nodes).sum(),
                serde_json::Value::Object(v) => v.values().map(nodes).sum(),
                _ => 0,
            }
        }
        let mut data = request();
        data.mode = Some(excess("review"));
        let mut array = Vec::with_capacity(100000);
        array.push(serde_json::Value::String(excess("界\"\n")));
        let mut object = serde_json::Map::new();
        object.insert(excess("key"), serde_json::Value::String(excess("value")));
        array.push(serde_json::Value::Object(object));
        data.selections[0].args = serde_json::Value::Array(array);
        data.selections[0].id = excess("skill");
        data.selections[0].source = excess("user");
        data.selections.reserve(100000);
        let source = SessionMessageSource::Runtime {
            subsystem: excess("chat"),
        };
        let input_id = excess("u");
        let wrapper = excess("root_chat_turn_v1");
        let session = excess("s");
        let execution = excess("e");
        let batch = available(
            &session,
            &execution,
            &[BorrowedInputRequestRecord {
                input_id: &input_id,
                source: &source,
                kind: SessionMessageKind::RuntimeInstruction,
                wrapper: Some(&wrapper),
                ..record(Some(&data))
            }],
        );
        let row = &batch.records()[0];
        assert_eq!(row.request.as_ref().unwrap(), &data);
        assert_eq!(
            row.created_at,
            chrono::DateTime::from_timestamp(1, 0).unwrap()
        );
        assert_eq!(row.kind, SessionMessageKind::RuntimeInstruction);
        assert_eq!(row.wrapper.as_deref(), Some("root_chat_turn_v1"));
        assert_eq!(batch.session_id.capacity(), batch.session_id.len());
        assert_eq!(batch.execution_id.capacity(), batch.execution_id.len());
        assert_eq!(row.input_id.capacity(), row.input_id.len());
        assert_eq!(row.wrapper.as_ref().unwrap().capacity(), wrapper.len());
        let SessionMessageSource::Runtime { subsystem } = &row.source else {
            panic!()
        };
        assert_eq!(subsystem.capacity(), subsystem.len());
        let copied = row.request.as_ref().unwrap();
        assert_eq!(copied.selections.capacity(), copied.selections.len());
        assert_eq!(copied.mode.as_ref().unwrap().capacity(), 6);
        let selection = &copied.selections[0];
        assert_eq!(selection.id.capacity(), selection.id.len());
        assert_eq!(selection.source.capacity(), selection.source.len());
        let values = selection.args.as_array().unwrap();
        assert_eq!(values.capacity(), values.len());
        let serde_json::Value::String(s) = &values[0] else {
            panic!()
        };
        assert_eq!(s.capacity(), s.len());
        let (key, value) = values[1].as_object().unwrap().iter().next().unwrap();
        assert_eq!(key.capacity(), key.len());
        let serde_json::Value::String(s) = value else {
            panic!()
        };
        assert_eq!(s.capacity(), s.len());
        assert!(nodes(&selection.args) <= batch.compact_bytes());
        drop((data, source, input_id, wrapper, session, execution));
        assert_eq!(
            batch.records()[0].request.as_ref().unwrap().selections[0].revision,
            u64::MAX
        );
        let scalar = request();
        assert_eq!(
            available("s", "e", &[record(Some(&scalar))]).records()[0]
                .request
                .as_ref(),
            Some(&scalar)
        );
    }
    #[test]
    fn input_request_projection_request_golden_charges_keys_numbers_mode_and_provenance() {
        let data = SessionSkillRequest {
            mode: Some("review".into()),
            selections: vec![SessionSkillSelection {
                id: "skill".into(),
                source: "plugin".into(),
                revision: u64::MAX,
                args: serde_json::json!({"n": u64::MAX, "q": "界\"\n"}),
            }],
        };
        let source = SessionMessageSource::Runtime {
            subsystem: "chat".into(),
        };
        let rows = [BorrowedInputRequestRecord {
            source: &source,
            kind: SessionMessageKind::RuntimeInstruction,
            wrapper: Some("root_chat_turn_v1"),
            created_at: chrono::DateTime::from_timestamp(1, 123456789).unwrap(),
            ..record(Some(&data))
        }];
        let expected = r#"{"session_id":"s","execution_id":"e","records":[["u",{"type":"runtime","subsystem":"chat"},"runtime_instruction","root_chat_turn_v1","1970-01-01T00:00:01.123456789Z",{"selections":[{"id":"skill","source":"plugin","revision":18446744073709551615,"args":{"n":18446744073709551615,"q":"界\"\n"}}],"mode":"review"}]]}"#;
        assert_eq!(
            serde_json::to_vec(&InputRequestMeasurement {
                session_id: "s",
                execution_id: "e",
                records: InputRequestRecordsMeasurement(&rows)
            })
            .unwrap(),
            expected.as_bytes()
        );
        let batch = available("s", "e", &rows);
        assert_eq!(batch.compact_bytes(), expected.len());
        assert_eq!(batch.records()[0].request.as_ref(), Some(&data));
        assert_eq!(batch.records()[0].created_at, rows[0].created_at);
    }

    #[test]
    fn input_request_projection_reuses_all_original_selection_validation() {
        let mut invalid = Vec::new();
        for id in ["", " skill", "skill ", "bad\n", &"x".repeat(257)] {
            let mut value = request();
            value.selections[0].id = id.into();
            invalid.push(value);
        }
        for source in ["", "USER", "user ", "unknown"] {
            let mut value = request();
            value.selections[0].source = source.into();
            invalid.push(value);
        }
        let mut zero = request();
        zero.selections[0].revision = 0;
        invalid.push(zero);
        let mut duplicate = request();
        duplicate.selections.push(duplicate.selections[0].clone());
        duplicate.selections[1].source = "builtin".into();
        invalid.push(duplicate);
        let mut excessive = request();
        excessive.selections = (0..33)
            .map(|i| SessionSkillSelection {
                id: format!("s{i}"),
                ..request().selections.remove(0)
            })
            .collect();
        invalid.push(excessive);
        for value in invalid {
            assert_eq!(
                project_input_request_batch("s", "e", &[record(None), record(Some(&value))]),
                Unavailable(InputRequestUnavailable::InvalidData)
            );
        }
    }

    #[test]
    fn input_request_projection_retains_every_untrusted_source_and_kind_exactly() {
        let mut identity = String::with_capacity(100000);
        identity.push_str(" peer/\n界");
        let sources = [
            SessionMessageSource::User,
            SessionMessageSource::Session {
                session_id: identity,
            },
            SessionMessageSource::Runtime {
                subsystem: "arbitrary".into(),
            },
        ];
        for source in &sources {
            for kind in [
                SessionMessageKind::UserInput,
                SessionMessageKind::PeerMessage,
                SessionMessageKind::ChildOutcome,
                SessionMessageKind::RuntimeInstruction,
            ] {
                let row = BorrowedInputRequestRecord {
                    source,
                    kind,
                    ..record(None)
                };
                let batch = available("s", "e", &[row]);
                assert_eq!(&batch.records()[0].source, source);
                assert_eq!(batch.records()[0].kind, kind); // Data, never eligibility/currentness.
                if let SessionMessageSource::Session { session_id } = &batch.records()[0].source {
                    assert_eq!(session_id.capacity(), session_id.len());
                    assert_eq!(session_id, " peer/\n界");
                }
            }
        }
    }
}
