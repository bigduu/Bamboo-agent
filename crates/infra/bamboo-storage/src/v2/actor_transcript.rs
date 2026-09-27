//! Explicit initialized-Actor append. No legacy save, authority repair or cache fallback.
use super::*;
use bamboo_domain::session::provider_transcript::{
    ProviderTranscriptItem, ProviderTranscriptItemKind, ProviderTranscriptOrigin,
    ProviderTranscriptState,
};
use bamboo_domain::{
    ActorActivationFence, ActorDirectoryEntry, ActorDirectoryError, ActorSession, Message,
};
use serde::de::{MapAccess, Visitor};
use serde_json::value::RawValue;
use std::collections::BTreeMap;

/// Complete durable prefixes, not a count or a caller-selected partial window.
#[derive(Debug, Clone)]
pub struct ActorTranscriptAppend {
    pub fence: ActorActivationFence,
    pub expected_created_at: DateTime<Utc>,
    pub expected_messages: Vec<Message>,
    pub expected_provider_transcript: ProviderTranscriptState,
    pub messages: Vec<Message>,
    pub native_groups: Vec<ActorTranscriptGroupAppend>,
}

/// Sequences, epochs and routes are assigned by the existing durable lane.
#[derive(Debug, Clone)]
pub struct ActorTranscriptGroupAppend {
    pub anchor_message_id: String,
    pub id_hint: Option<String>,
    pub items: Vec<ProviderTranscriptItem>,
}

#[derive(Debug, thiserror::Error)]
pub enum ActorTranscriptAppendError {
    #[error("Actor transcript authority rejected: {0}")]
    Authority(#[from] ActorDirectoryError),
    #[error("Actor transcript source is missing, invalid or inconsistent")]
    InvalidSource,
    #[error("Actor transcript prefix changed; reload the durable snapshot")]
    PrefixConflict,
    #[error("Actor transcript append payload is unsupported or has colliding identities")]
    UnsupportedPayload,
    #[error("Actor transcript native counter cannot advance")]
    CounterOverflow,
    #[error("Actor transcript was not replaced: {0}")]
    BeforePublication(#[from] io::Error),
    #[error("Actor transcript outcome is unconfirmed; reload before retrying: {0}")]
    OutcomeUnconfirmed(io::Error),
}
type Result<T> = std::result::Result<T, ActorTranscriptAppendError>;

// Standard serde decoding retains borrowed input spans and rejects duplicate
// object keys instead of letting a Value/Map silently keep the last identity.
pub(super) struct Object<'a>(pub(super) BTreeMap<String, &'a RawValue>);
impl<'de> Deserialize<'de> for Object<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> std::result::Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = Object<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object with unique keys")
            }
            fn visit_map<M: MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                let mut fields = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, &'de RawValue>()? {
                    if fields.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate object key"));
                    }
                }
                Ok(Object(fields))
            }
        }
        decoder.deserialize_map(ObjectVisitor)
    }
}
pub(super) fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| ActorTranscriptAppendError::InvalidSource)
}
pub(super) fn object(raw: &str) -> Result<Object<'_>> {
    decode(raw.as_bytes())
}
pub(super) fn encoded<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|_| ActorTranscriptAppendError::UnsupportedPayload)
}
pub(super) fn regular_bytes(path: &Path) -> Result<Vec<u8>> {
    if !std::fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(ActorTranscriptAppendError::InvalidSource);
    }
    Ok(std::fs::read(path)?)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    schema_version: u32,
    actor_id: String,
    session_created_at: DateTime<Utc>,
}
pub(super) struct Source {
    pub(super) raw: String,
    pub(super) side_bytes: Vec<u8>,
    pub(super) record_bytes: Vec<u8>,
    pub(super) marker_bytes: Vec<u8>,
    pub(super) proof_bytes: Option<Vec<u8>>,
    pub(super) main: Session,
    pub(super) side: Session,
    pub(super) entry: ActorDirectoryEntry,
}
impl Source {
    // Pure initialized reader: no ensure/inspect/validate_fence transaction.
    pub(super) fn read(directory: &Path, id: &str, kind: SessionKind, root: &str) -> Result<Self> {
        Self::read_checked(directory, id, kind, root, |_, _| Ok(()))
    }
    // Opt-in preflight sees the exact buffers subsequently decoded below.
    pub(super) fn read_checked(
        directory: &Path,
        id: &str,
        kind: SessionKind,
        root: &str,
        preflight: impl FnOnce(&str, &[u8]) -> Result<()>,
    ) -> Result<Self> {
        let raw = String::from_utf8(regular_bytes(&directory.join("session.json"))?)
            .map_err(|_| ActorTranscriptAppendError::InvalidSource)?;
        let side_bytes = regular_bytes(&directory.join(RUNTIME_SIDECAR_FILE))?;
        let record_bytes = regular_bytes(&directory.join("actor-authority.json"))?;
        let marker_bytes = regular_bytes(&directory.join("actor-authority.initialized.json"))?;
        preflight(&raw, &side_bytes)?;
        compact_main::validate_full_main(raw.as_bytes())
            .map_err(|_| ActorTranscriptAppendError::InvalidSource)?;
        let mut main: Session = decode(raw.as_bytes())?;
        let mut side: Session = decode(&side_bytes)?;
        supervisor::validate_overlay(&main, Some(&side))?;
        object(&raw)?;
        object(
            std::str::from_utf8(&side_bytes)
                .map_err(|_| ActorTranscriptAppendError::InvalidSource)?,
        )?;
        // Only a Root at its canonical own path can use the legacy empty root id.
        for session in [&mut main, &mut side] {
            if kind == SessionKind::Root && session.root_session_id.is_empty() {
                session.root_session_id = id.into();
            }
            if session.id != id
                || session.kind != kind
                || session.root_session_id != root
                || !session.authority_identity.is_ordinary()
            {
                return Err(ActorTranscriptAppendError::InvalidSource);
            }
        }
        if main.created_at != side.created_at
            || main.parent_session_id != side.parent_session_id
            || main.spawn_depth != side.spawn_depth
            || main.authority_identity != side.authority_identity
        {
            return Err(ActorTranscriptAppendError::InvalidSource);
        }
        let entry: ActorDirectoryEntry = decode(&record_bytes)?;
        let marker: Marker = decode(&marker_bytes)?;
        entry.validate()?;
        let session = overlay_runtime_sidecar(main.clone(), Some(side.clone()));
        let identity = ActorSession::from_session(&session)?;
        if !entry.actor.matches_session(&main)
            || !entry.actor.matches_session(&side)
            || marker.schema_version != bamboo_domain::ACTOR_DIRECTORY_SCHEMA_VERSION
            || marker.actor_id != id
            || marker.session_created_at != main.created_at
            || entry.actor.project_id != identity.project_id
            || identity.observed_metadata_version < entry.actor.observed_metadata_version
            || identity.observed_metadata_version - entry.actor.observed_metadata_version >= 2
        {
            return Err(ActorTranscriptAppendError::InvalidSource);
        }
        let proof_bytes = if kind == SessionKind::Root {
            Some(regular_bytes(
                &directory.join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE),
            )?)
        } else {
            None
        };
        Ok(Self {
            raw,
            side_bytes,
            record_bytes,
            marker_bytes,
            proof_bytes,
            main,
            side,
            entry,
        })
    }
    pub(super) fn unchanged(&self, other: &Self) -> bool {
        self.raw == other.raw
            && self.side_bytes == other.side_bytes
            && self.record_bytes == other.record_bytes
            && self.marker_bytes == other.marker_bytes
            && self.proof_bytes == other.proof_bytes
    }
}

// v1 client-tool extension: exactly one read-only Glob pair and a final reply.
// Tool availability/permission is independently established by the Host caller.
fn validate_glob_suffix(messages: &[Message], main: &Session) -> Result<()> {
    let [call, result, final_reply] = messages else {
        return Err(ActorTranscriptAppendError::UnsupportedPayload);
    };
    let Some(calls) = &call.tool_calls else {
        return Err(ActorTranscriptAppendError::UnsupportedPayload);
    };
    let [tool] = calls.as_slice() else {
        return Err(ActorTranscriptAppendError::UnsupportedPayload);
    };
    if encoded(&messages)?.len() > 64 * 1024
        || call.role != Role::Assistant
        || call.phase != Some(bamboo_domain::MessagePhase::Commentary)
        || call.tool_call_id.is_some()
        || call.tool_success.is_some()
        || tool.id.trim().is_empty()
        || tool.tool_type != "function"
        || tool.function.name != "Glob"
        || serde_json::from_str::<serde_json::Value>(&tool.function.arguments)
            .map_or(true, |args| !args.is_object())
        || main.messages.iter().any(|m| {
            m.tool_call_id.as_deref() == Some(tool.id.as_str())
                || m.tool_calls
                    .as_ref()
                    .is_some_and(|calls| calls.iter().any(|c| c.id == tool.id))
        })
        || result.role != Role::Tool
        || result.phase.is_some()
        || result.tool_calls.is_some()
        || result.tool_call_id.as_deref() != Some(tool.id.as_str())
        || result.tool_success != Some(true)
        || final_reply.role != Role::Assistant
        || final_reply.phase != Some(bamboo_domain::MessagePhase::FinalAnswer)
        || final_reply.tool_calls.is_some()
        || final_reply.tool_call_id.is_some()
        || final_reply.tool_success.is_some()
        || final_reply.content.trim().is_empty()
    {
        return Err(ActorTranscriptAppendError::UnsupportedPayload);
    }
    Ok(())
}

// The normal tool dispatcher adds these five lifecycle observations to its
// actual Glob result. They do not grant authority or permit other metadata.
fn glob_lifecycle_metadata(value: &serde_json::Value) -> bool {
    value.as_object().is_some_and(|fields| {
        fields.len() == 5
            && value["elapsed_ms"].as_u64().is_some()
            && value["is_mutating"] == false
            && value["auto_approved"] == true
            && value["tool_name"] == "Glob"
            && value["success"] == true
    })
}

fn validate_suffix(request: &ActorTranscriptAppend, main: &Session) -> Result<Session> {
    if encoded(&request.expected_messages)? != encoded(&main.messages)?
        || request.expected_provider_transcript != main.provider_transcript
    {
        return Err(ActorTranscriptAppendError::PrefixConflict);
    }
    let mut ids = HashSet::new();
    for message in &main.messages {
        if message.id.trim().is_empty() || !ids.insert(message.id.as_str()) {
            return Err(ActorTranscriptAppendError::InvalidSource);
        }
    }
    if request.messages.is_empty() {
        return Err(ActorTranscriptAppendError::UnsupportedPayload);
    }
    let typed = request
        .messages
        .iter()
        .any(|m| m.tool_calls.is_some() || m.role == Role::Tool);
    if typed {
        if !request.native_groups.is_empty() {
            return Err(ActorTranscriptAppendError::UnsupportedPayload);
        }
        validate_glob_suffix(&request.messages, main)?;
    }
    for m in &request.messages {
        if m.id.trim().is_empty()
            || !ids.insert(m.id.as_str())
            || (!typed
                && (m.role != Role::Assistant
                    || m.tool_calls.is_some()
                    || m.tool_call_id.is_some()
                    || m.tool_success.is_some()))
            || m.reasoning.is_some()
            || m.reasoning_signature.is_some()
            || m.content_parts.is_some()
            || m.image_ocr.is_some()
            || m.metadata.as_ref().is_some_and(|metadata| {
                !typed || m.role != Role::Tool || !glob_lifecycle_metadata(metadata)
            })
            || m.compressed
            || m.compressed_by_event_id.is_some()
            || m.never_compress
            || m.compression_level != 0
        {
            return Err(ActorTranscriptAppendError::UnsupportedPayload);
        }
    }
    let mut candidate = main.clone();
    candidate.messages.extend(request.messages.clone());
    for group in &request.native_groups {
        if !request
            .messages
            .iter()
            .any(|m| m.id == group.anchor_message_id)
            || group.items.iter().any(|item| {
                item.origin() != ProviderTranscriptOrigin::Provider
                    || !matches!(
                        item.kind(),
                        ProviderTranscriptItemKind::OpenAiMessage
                            | ProviderTranscriptItemKind::OpenAiToolSearchCall
                            | ProviderTranscriptItemKind::OpenAiToolSearchOutput
                            | ProviderTranscriptItemKind::AnthropicText
                            | ProviderTranscriptItemKind::AnthropicServerToolUse
                            | ProviderTranscriptItemKind::AnthropicToolSearchToolResult
                    )
                    || (matches!(
                        item.kind(),
                        ProviderTranscriptItemKind::OpenAiToolSearchCall
                            | ProviderTranscriptItemKind::OpenAiToolSearchOutput
                    ) && item
                        .payload()
                        .get("execution")
                        .and_then(serde_json::Value::as_str)
                        != Some("server"))
            })
        {
            return Err(ActorTranscriptAppendError::UnsupportedPayload);
        }
        // Derive the legacy unbound sentinel through the existing pure Domain
        // primitive, rather than duplicating its private hash format.
        let mut unbound = ProviderTranscriptState::default();
        unbound
            .append_group(&group.anchor_message_id, None, group.items.clone())
            .map_err(|_| ActorTranscriptAppendError::UnsupportedPayload)?;
        let state = &candidate.provider_transcript;
        if state.active_family().is_none()
            || state.active_protocol().is_none()
            || state.active_provider_boundary_sha256().is_none()
            || state.active_provider_boundary_sha256() == unbound.active_provider_boundary_sha256()
        {
            return Err(ActorTranscriptAppendError::UnsupportedPayload);
        }
        let next_revision = state
            .state_revision()
            .checked_add(1)
            .ok_or(ActorTranscriptAppendError::CounterOverflow)?;
        let wire: serde_json::Value = decode(encoded(state)?.as_bytes())?;
        let next_sequence = wire["next_sequence"]
            .as_u64()
            .and_then(|n| n.checked_add(1))
            .ok_or(ActorTranscriptAppendError::CounterOverflow)?;
        let before = state.groups().len();
        candidate
            .append_provider_transcript_group(
                &group.anchor_message_id,
                group.id_hint.as_deref(),
                group.items.clone(),
            )
            .map_err(|_| ActorTranscriptAppendError::UnsupportedPayload)?;
        if candidate.provider_transcript.state_revision() != next_revision
            || candidate.provider_transcript.groups().len() != before + 1
            || serde_json::to_value(&candidate.provider_transcript)
                .map_err(|_| ActorTranscriptAppendError::InvalidSource)?["next_sequence"]
                != next_sequence
        {
            return Err(ActorTranscriptAppendError::UnsupportedPayload);
        }
    }
    // Existing strict Deserialize enforces epochs, sequences and all item/group validators.
    let _: ProviderTranscriptState = decode(encoded(&candidate.provider_transcript)?.as_bytes())?;
    Ok(candidate)
}

pub(super) fn appended_array<T: Serialize>(raw: &str, suffix: &[T]) -> Result<String> {
    let entries: Vec<&RawValue> = decode(raw.as_bytes())?;
    let mut result = raw[..raw.len() - 1].to_string(); // RawValue is exactly the JSON array span.
    for (index, item) in suffix.iter().enumerate() {
        if !entries.is_empty() || index != 0 {
            result.push(',');
        }
        result.push_str(&encoded(item)?);
    }
    result.push(']');
    Ok(result)
}
pub(super) fn splice(raw: &str, mut replacements: Vec<(&RawValue, String)>) -> Result<String> {
    replacements.sort_by_key(|(span, _)| std::cmp::Reverse(span.get().as_ptr() as usize));
    let mut output = raw.to_string();
    for (span, replacement) in replacements {
        let offset = (span.get().as_ptr() as usize)
            .checked_sub(raw.as_ptr() as usize)
            .ok_or(ActorTranscriptAppendError::InvalidSource)?;
        let end = offset
            .checked_add(span.get().len())
            .filter(|end| *end <= raw.len())
            .ok_or(ActorTranscriptAppendError::InvalidSource)?;
        output.replace_range(offset..end, &replacement);
    }
    Ok(output)
}
fn patched_main(raw: &str, request: &ActorTranscriptAppend, candidate: &Session) -> Result<String> {
    let fields = object(raw)?.0;
    let messages = *fields
        .get("messages")
        .ok_or(ActorTranscriptAppendError::InvalidSource)?;
    let entries: Vec<&RawValue> = decode(messages.get().as_bytes())?;
    for entry in entries {
        let message = object(entry.get())?.0;
        if !message.contains_key("id") || !message.contains_key("created_at") {
            return Err(ActorTranscriptAppendError::InvalidSource);
        }
    }
    let mut replacements = vec![(messages, appended_array(messages.get(), &request.messages)?)];
    if !request.native_groups.is_empty() {
        let native = *fields
            .get("provider_transcript")
            .ok_or(ActorTranscriptAppendError::InvalidSource)?;
        let native_fields = object(native.get())?.0;
        let candidate_fields = encoded(&candidate.provider_transcript)?;
        let new_fields = object(&candidate_fields)?.0;
        let all_groups = candidate.provider_transcript.groups();
        let suffix = &all_groups[all_groups.len() - request.native_groups.len()..];
        let mut native_replacements = Vec::new();
        for name in ["state_revision", "next_sequence"] {
            let old = *native_fields
                .get(name)
                .ok_or(ActorTranscriptAppendError::InvalidSource)?;
            native_replacements.push((old, new_fields[name].get().to_string()));
        }
        let mut native_raw = if let Some(groups) = native_fields.get("groups") {
            let old_groups: Vec<&RawValue> = decode(groups.get().as_bytes())?;
            for group in old_groups {
                object(group.get())?;
            }
            native_replacements.push((*groups, appended_array(groups.get(), suffix)?));
            splice(native.get(), native_replacements)?
        } else {
            let mut result = splice(native.get(), native_replacements)?;
            result.pop();
            result.push_str(&format!(",\"groups\":{} }}", encoded(&suffix)?));
            result
        };
        // Validate the complete raw lane too; unknown keys remain verbatim.
        let _: ProviderTranscriptState = decode(native_raw.as_bytes())?;
        replacements.push((native, std::mem::take(&mut native_raw)));
    }
    let output = splice(raw, replacements)?;
    compact_main::validate_full_main(output.as_bytes())
        .map_err(|_| ActorTranscriptAppendError::InvalidSource)?;
    let committed: Session = decode(output.as_bytes())?;
    if encoded(&committed.messages)? != encoded(&candidate.messages)?
        || committed.provider_transcript != candidate.provider_transcript
    {
        return Err(ActorTranscriptAppendError::InvalidSource);
    }
    Ok(output)
}

impl SessionStoreV2 {
    /// V2-only, caller opt-in append. Rejection never initializes or repairs
    /// authority. Success confirms the physical main replacement and readback.
    /// An unconfirmed outcome must be reloaded, never automatically replayed.
    pub async fn append_actor_transcript(&self, request: ActorTranscriptAppend) -> Result<Session> {
        let lifecycle = self.lock_session_lifecycle_shared().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let writer = self
            .acquire_session_maintenance_lock(&request.fence.actor_id)
            .await?;
        let guards = DefaultWriterGuards::shared(lifecycle, task, writer);
        let (rel, _) = self
            .actor_authority_location(&request.fence.actor_id)
            .await?;
        let directory = self.abs_path_from_rel(&rel);
        let (kind, root) = Self::copy_source_identity_from_rel(&request.fence.actor_id, &rel)?;
        let id = request.fence.actor_id.clone();
        let source_dir = directory.clone();
        let source_id = id.clone();
        let source_root = root.clone();
        let home = self.sessions_dir.clone();
        let read_home = home.clone();
        let (source, ancestors) = Self::default_writer_job(&guards, move || {
            Ok((|| -> Result<_> {
                let source = Source::read(&source_dir, &source_id, kind, &source_root)?;
                let ancestors = actor_checkpoint_lineage::capture(&read_home, &source.entry.actor)?;
                Ok((source, ancestors))
            })())
        })
        .await??;
        let lineage = self.validate_actor_lineage(&source.entry.actor).await?;
        actor_checkpoint_lineage::validate_recorded_observations(
            &source.entry.actor.ancestor_observations,
            &lineage,
        )?;
        if source.main.created_at != request.expected_created_at {
            return Err(ActorDirectoryError::InvalidIdentity.into());
        }
        if !self.session_lifetime_is_live(&source.main).await? {
            return Err(ActorDirectoryError::InvalidIdentity.into());
        }
        self.validate_root_tool_authority_overlay(&id, &source.main, Some(&source.side))
            .await?;
        actor_directory::current_live(&source.entry, &request.fence, Utc::now())?;
        let candidate = validate_suffix(&request, &source.main)?;
        let output = patched_main(&source.raw, &request, &candidate)?;
        #[cfg(test)]
        let hook = self.transcript_write_hook.lock().unwrap().clone();
        Self::default_writer_job(&guards, move || {
            let initial = (|| -> Result<()> {
                let initial = Source::read(&directory, &id, kind, &root)?;
                if !source.unchanged(&initial)
                    || !ancestors.matches_current(&home, &initial.entry.actor)?
                {
                    return Err(ActorTranscriptAppendError::InvalidSource);
                }
                actor_directory::current_live(&initial.entry, &request.fence, Utc::now())?;
                Ok(())
            })();
            if let Err(error) = initial {
                return Ok(Err(error));
            }
            let mut replaced = false;
            let mut rejection = None;
            let path = directory.join("session.json");
            let write = durable_atomic_write_blocking(&path, output.as_bytes(), |phase| {
                // Set before faults: after rename, failure is always ambiguous.
                if phase == DurableWritePhase::AfterReplace {
                    replaced = true;
                }
                #[cfg(test)]
                if let Some(hook) = &hook {
                    hook.visit(phase)?;
                }
                if phase == DurableWritePhase::BeforeReplace {
                    let check = (|| {
                        let current = Source::read(&directory, &id, kind, &root)?;
                        if !source.unchanged(&current)
                            || !ancestors.matches_current(&home, &current.entry.actor)?
                        {
                            return Err(ActorTranscriptAppendError::InvalidSource);
                        }
                        // This fresh host clock is deliberately AFTER the publication barrier.
                        actor_directory::current_live(&current.entry, &request.fence, Utc::now())?;
                        Ok(())
                    })();
                    if let Err(error) = check {
                        rejection = Some(error);
                        return Err(io::Error::other(
                            "Actor transcript final authority rejected",
                        ));
                    }
                }
                Ok(())
            });
            if let Err(error) = write {
                return Ok(Err(if replaced {
                    ActorTranscriptAppendError::OutcomeUnconfirmed(error)
                } else {
                    rejection.unwrap_or(ActorTranscriptAppendError::BeforePublication(error))
                }));
            }
            let readback = (|| {
                #[cfg(test)]
                if let Some(hook) = &hook {
                    hook.before_readback(&path)?;
                }
                let actual = regular_bytes(&path)?;
                if actual != output.as_bytes()
                    || regular_bytes(&directory.join(RUNTIME_SIDECAR_FILE))? != source.side_bytes
                {
                    return Err(ActorTranscriptAppendError::InvalidSource);
                }
                compact_main::validate_full_main(&actual)
                    .map_err(|_| ActorTranscriptAppendError::InvalidSource)?;
                Ok(overlay_runtime_sidecar(
                    decode(&actual)?,
                    Some(decode(&source.side_bytes)?),
                ))
            })();
            Ok(readback.map_err(|_| {
                ActorTranscriptAppendError::OutcomeUnconfirmed(io::Error::other(
                    "committed transcript readback unavailable",
                ))
            }))
        })
        .await
        .map_err(ActorTranscriptAppendError::OutcomeUnconfirmed)?
    }
}
