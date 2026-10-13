//! Explicit Main-only input checkpoint. No admission/release/ACK or cache caller.
use super::actor_transcript::{
    appended_array, decode, encoded, object, regular_bytes, splice, Source,
};
use super::*;
use crate::FileSessionInbox;
use bamboo_domain::{
    is_matching_session_message, ActorActivationFence, ActorActivationStatus, ActorDirectoryError,
    Message, SessionActivationPolicy, SessionInboxActivationIntent, SessionInboxAdmissionState,
    SessionInboxClaim, SessionInboxError, SessionInboxOwnedClaim, SessionMessageBody,
    SessionMessageEnvelope, SessionMessageKind,
};
use serde_json::value::RawValue;

const BOOKKEEPING_KEY: &str = "_bamboo_owned_input_checkpoint";

/// Complete current durable prefixes are required on New and Already alike.
#[derive(Debug, Clone)]
pub struct ActorInputCheckpoint {
    pub fence: ActorActivationFence,
    pub expected_created_at: DateTime<Utc>,
    pub claim: SessionInboxOwnedClaim,
    pub expected_messages: Vec<Message>,
    pub expected_provider_transcript:
        bamboo_domain::session::provider_transcript::ProviderTranscriptState,
    pub expected_admission: Option<SessionInboxAdmissionState>,
}

/// Host pre-dispatch seed for an unowned typed claim. The durable User message
/// is written under the current Actor fence, but admission and Inbox ACK wait
/// for the worker's exact confirmation at its reasoning boundary.
#[derive(Debug, Clone)]
pub struct ActorClaimContextSeed {
    pub fence: ActorActivationFence,
    pub expected_created_at: DateTime<Utc>,
    pub expected_messages: Vec<Message>,
    pub expected_provider_transcript:
        bamboo_domain::session::provider_transcript::ProviderTranscriptState,
    pub expected_admission: Option<SessionInboxAdmissionState>,
    pub claims: Vec<SessionInboxClaim>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorInputCheckpointStatus {
    NewCheckpoint,
    AlreadyCheckpointed,
}
/// Actual current readback, not a historical Session or execution capability.
#[derive(Debug)]
pub struct ActorInputCheckpointResult {
    pub session: Session,
    pub status: ActorInputCheckpointStatus,
    pub envelope: SessionMessageEnvelope,
    pub generation: u64,
}
#[derive(Debug, thiserror::Error)]
pub enum ActorInputCheckpointError {
    #[error("Actor input authority rejected: {0}")]
    Actor(#[from] ActorDirectoryError),
    #[error("Actor input canonical authority is invalid or unsupported: {0}")]
    InvalidAuthority(#[from] ActorTranscriptAppendError),
    #[error("Actor input physical claim rejected: {0}")]
    Inbox(#[from] SessionInboxError),
    #[error("Actor input prefix changed; reload the durable snapshot")]
    PrefixConflict,
    #[error("Actor input payload or durable dedupe evidence is unsupported or conflicting")]
    Unsupported,
    #[error("Actor input was not replaced: {0}")]
    BeforePublication(#[from] io::Error),
    #[error("Actor input outcome is unconfirmed; reload before retrying: {0}")]
    OutcomeUnconfirmed(io::Error),
}
type Result<T> = std::result::Result<T, ActorInputCheckpointError>;

// Constructed only after the actual L -> Task -> Session acquisition. Inbox
// acquisition and every started final job own this Arc through the real FD holder.
pub(crate) struct ActorInputGuards {
    _guards: Arc<DefaultWriterGuards>,
}
#[derive(Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Bookkeeping {
    version: u32,
    envelope_id: String,
    target_session_id: String,
    session_created_at: DateTime<Utc>,
    generation: u64,
    activation_intent: Option<SessionInboxActivationIntent>,
    activation_policy: SessionActivationPolicy,
}
fn admission(session: &Session) -> Option<SessionInboxAdmissionState> {
    session
        .runtime_metadata
        .as_ref()?
        .session_inbox_admission
        .clone()
}
fn running(source: &Source, request: &ActorInputCheckpoint) -> Result<()> {
    let activation = actor_directory::current_live(&source.entry, &request.fence, Utc::now())?;
    if activation.status != ActorActivationStatus::Running
        || activation.lease_owner != request.claim.lease.consumer.as_str()
        || source.main.created_at != request.expected_created_at
    {
        return Err(ActorDirectoryError::StaleFence.into());
    }
    Ok(())
}
fn current_input(
    inbox: &FileSessionInbox,
    dir: &Path,
    request: &ActorInputCheckpoint,
) -> Result<(SessionMessageEnvelope, Bookkeeping)> {
    let (envelope, intent) =
        inbox.locked_input_claim(dir, &request.fence.actor_id, &request.claim, Utc::now())?;
    if !matches!(
        envelope.kind,
        SessionMessageKind::UserInput | SessionMessageKind::PeerMessage
    ) || !matches!(&envelope.body, SessionMessageBody::Content(content) if content.parts.is_empty() && !content.text.trim().is_empty())
    {
        return Err(ActorInputCheckpointError::Unsupported);
    }
    let proof = Bookkeeping {
        version: 1,
        envelope_id: envelope.id.as_str().into(),
        target_session_id: envelope.target_session_id.clone(),
        session_created_at: request.expected_created_at,
        generation: request.claim.claim.generation,
        activation_intent: intent,
        activation_policy: request.claim.claim.activation_policy,
    };
    Ok((envelope, proof))
}
fn parse_bookkeeping(raw_message: &str) -> Result<Bookkeeping> {
    let message = object(raw_message)?.0;
    let metadata = object(
        message
            .get("metadata")
            .ok_or(ActorInputCheckpointError::Unsupported)?
            .get(),
    )?
    .0;
    let raw = metadata
        .get(BOOKKEEPING_KEY)
        .ok_or(ActorInputCheckpointError::Unsupported)?;
    let fields = object(raw.get())?.0;
    let names = [
        "version",
        "envelope_id",
        "target_session_id",
        "session_created_at",
        "generation",
        "activation_intent",
        "activation_policy",
    ];
    if fields.len() != names.len() || names.iter().any(|name| !fields.contains_key(*name)) {
        return Err(ActorInputCheckpointError::Unsupported);
    }
    let proof: Bookkeeping =
        serde_json::from_str(raw.get()).map_err(|_| ActorInputCheckpointError::Unsupported)?;
    if proof.version != 1 || proof.generation == 0 {
        return Err(ActorInputCheckpointError::Unsupported);
    }
    Ok(proof)
}
fn translated(envelope: &SessionMessageEnvelope, proof: &Bookkeeping) -> Result<Message> {
    let mut message = envelope
        .to_provider_message()
        .map_err(|_| ActorInputCheckpointError::Unsupported)?;
    message
        .metadata
        .as_mut()
        .and_then(serde_json::Value::as_object_mut)
        .ok_or(ActorInputCheckpointError::Unsupported)?
        .insert(
            BOOKKEEPING_KEY.into(),
            serde_json::to_value(proof).map_err(|_| ActorInputCheckpointError::Unsupported)?,
        );
    Ok(message)
}
fn already(
    source: &Source,
    request: &ActorInputCheckpoint,
    envelope: &SessionMessageEnvelope,
    proof: &Bookkeeping,
) -> Result<bool> {
    if encoded(&request.expected_messages)? != encoded(&source.main.messages)?
        || request.expected_provider_transcript != source.main.provider_transcript
        || encoded(&request.expected_admission)? != encoded(&admission(&source.main))?
    {
        return Err(ActorInputCheckpointError::PrefixConflict);
    }
    let fields = object(&source.raw)?.0;
    let raw_messages: Vec<&RawValue> = decode(
        fields
            .get("messages")
            .ok_or(ActorInputCheckpointError::Unsupported)?
            .get()
            .as_bytes(),
    )?;
    let mut ids = HashSet::new();
    let mut found = false;
    for (message, raw) in source.main.messages.iter().zip(raw_messages) {
        let fields = object(raw.get())?.0;
        if !fields.contains_key("id")
            || !fields.contains_key("created_at")
            || message.id.trim().is_empty()
            || !ids.insert(&message.id)
        {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        if message.id != envelope.id.as_str() {
            continue;
        }
        if parse_bookkeeping(raw.get()).map_err(|_| ActorInputCheckpointError::Unsupported)?
            != *proof
        {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        let mut plain = message.clone();
        plain
            .metadata
            .as_mut()
            .and_then(serde_json::Value::as_object_mut)
            .ok_or(ActorInputCheckpointError::Unsupported)?
            .remove(BOOKKEEPING_KEY);
        // The general Domain matcher stays unchanged; only our validated key is
        // stripped. V1 recovers the exact original physical envelope/presentation.
        if !is_matching_session_message(&plain, envelope) {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        found = true;
    }
    if !found && admission(&source.main).is_some_and(|cursor| cursor.contains(&envelope.id)) {
        return Err(ActorInputCheckpointError::Unsupported);
    }
    Ok(found)
}
fn insert_member(raw: &str, name: &str, value: &str) -> Result<String> {
    let empty = object(raw)?.0.is_empty();
    Ok(format!(
        "{}{}{}:{}}}",
        &raw[..raw.len() - 1],
        if empty { "" } else { "," },
        encoded(&name)?,
        value
    ))
}
fn patched_main(source: &Source, message: &Message, generation: u64) -> Result<String> {
    let fields = object(&source.raw)?.0;
    let messages = *fields
        .get("messages")
        .ok_or(ActorInputCheckpointError::Unsupported)?;
    let mut cursor = admission(&source.main).unwrap_or_default();
    if !cursor.record(
        bamboo_domain::SessionMessageId::parse(message.id.clone())
            .map_err(|_| ActorInputCheckpointError::Unsupported)?,
        generation,
    ) {
        return Err(ActorInputCheckpointError::Unsupported);
    }
    let cursor_raw = encoded(&cursor)?;
    let mut replacements = vec![(
        messages,
        appended_array(messages.get(), std::slice::from_ref(message))?,
    )];
    let runtime = match fields.get("runtime_metadata") {
        Some(raw) if raw.get() != "null" => {
            let fields = object(raw.get())?.0;
            if let Some(old) = fields.get("session_inbox_admission") {
                splice(raw.get(), vec![(*old, cursor_raw.clone())])?
            } else {
                insert_member(raw.get(), "session_inbox_admission", &cursor_raw)?
            }
        }
        _ => format!("{{\"session_inbox_admission\":{cursor_raw}}}"),
    };
    if let Some(raw) = fields.get("runtime_metadata") {
        replacements.push((*raw, runtime.clone()));
    }
    let mut output = splice(&source.raw, replacements)?;
    if !fields.contains_key("runtime_metadata") {
        output = insert_member(&output, "runtime_metadata", &runtime)?;
    }
    compact_main::validate_full_main(output.as_bytes())
        .map_err(|_| ActorInputCheckpointError::Unsupported)?;
    let committed: Session = decode(output.as_bytes())?;
    let mut wanted = source.main.messages.clone();
    wanted.push(message.clone());
    if encoded(&committed.messages)? != encoded(&wanted)?
        || encoded(&admission(&committed))? != encoded(&Some(cursor))?
        || committed.provider_transcript != source.main.provider_transcript
    {
        return Err(ActorInputCheckpointError::Unsupported);
    }
    Ok(output)
}

impl SessionStoreV2 {
    pub(crate) async fn checkpoint_owned_input(
        &self,
        inbox: FileSessionInbox,
        request: ActorInputCheckpoint,
    ) -> Result<ActorInputCheckpointResult> {
        let lifecycle = self.lock_session_lifecycle_shared().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let writer = self
            .acquire_session_maintenance_lock(&request.fence.actor_id)
            .await?;
        let guards = Arc::new(ActorInputGuards {
            _guards: DefaultWriterGuards::shared(lifecycle, task, writer),
        });
        let (rel, _) = self
            .actor_authority_location(&request.fence.actor_id)
            .await?;
        let directory = self.abs_path_from_rel(&rel);
        let (kind, root) = Self::copy_source_identity_from_rel(&request.fence.actor_id, &rel)?;
        let source_dir = directory.clone();
        let source_id = request.fence.actor_id.clone();
        let source_root = root.clone();
        let home = self.sessions_dir.clone();
        let read_home = home.clone();
        let (source, ancestors) = Self::default_writer_job(&guards._guards, move || {
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
        if admission(&source.side).is_some() {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        if !self.session_lifetime_is_live(&source.main).await? {
            return Err(ActorDirectoryError::InvalidIdentity.into());
        }
        self.validate_root_tool_authority_overlay(
            &request.fence.actor_id,
            &source.main,
            Some(&source.side),
        )
        .await?;
        let (inbox_dir, filesystem) = inbox
            .actor_input_filesystem(&request.fence.actor_id, guards)
            .await?;
        if inbox_dir != directory.join("inbox") {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        #[cfg(test)]
        let hook = self.transcript_write_hook.lock().unwrap().clone();
        filesystem
            .job(
                "actor_input_commit",
                &directory.join("session.json"),
                move |_, path| {
                    let operation = (|| {
                        let initial =
                            Source::read(&directory, &request.fence.actor_id, kind, &root)?;
                        if !source.unchanged(&initial)
                            || !ancestors.matches_current(&home, &initial.entry.actor)?
                        {
                            return Err(ActorInputCheckpointError::PrefixConflict);
                        }
                        running(&initial, &request)?;
                        let (envelope, proof) = current_input(&inbox, &inbox_dir, &request)?;
                        let present = already(&source, &request, &envelope, &proof)?;
                        let mut replaced = false;
                        let mut rejection = None;
                        let verify = || -> Result<()> {
                            let current =
                                Source::read(&directory, &request.fence.actor_id, kind, &root)?;
                            if !source.unchanged(&current)
                                || !ancestors.matches_current(&home, &current.entry.actor)?
                            {
                                return Err(ActorInputCheckpointError::PrefixConflict);
                            }
                            running(&current, &request)?;
                            let (current_envelope, current_proof) =
                                current_input(&inbox, &inbox_dir, &request)?;
                            if current_envelope != envelope || current_proof != proof {
                                return Err(ActorInputCheckpointError::Unsupported);
                            }
                            Ok(())
                        };
                        let output = if present {
                            None
                        } else {
                            Some(patched_main(
                                &source,
                                &translated(&envelope, &proof)?,
                                proof.generation,
                            )?)
                        };
                        if let Some(output) = &output {
                            let write =
                                durable_atomic_write_blocking(path, output.as_bytes(), |phase| {
                                    if phase == DurableWritePhase::AfterReplace {
                                        replaced = true;
                                    }
                                    #[cfg(test)]
                                    if let Some(hook) = &hook {
                                        hook.visit(phase)?;
                                    }
                                    if phase == DurableWritePhase::BeforeReplace {
                                        if let Err(error) = verify() {
                                            rejection = Some(error);
                                            return Err(io::Error::other(
                                                "Actor input final authority rejected",
                                            ));
                                        }
                                    }
                                    Ok(())
                                });
                            if let Err(error) = write {
                                return Err(if replaced {
                                    ActorInputCheckpointError::OutcomeUnconfirmed(error)
                                } else {
                                    rejection.unwrap_or(
                                        ActorInputCheckpointError::BeforePublication(error),
                                    )
                                });
                            }
                        } else {
                            verify()?;
                        }
                        let readback = (|| -> Result<Session> {
                            #[cfg(test)]
                            if let Some(hook) = &hook {
                                hook.before_readback(path)?;
                            }
                            let actual = regular_bytes(path)?;
                            if actual != output.as_ref().unwrap_or(&source.raw).as_bytes()
                                || regular_bytes(&directory.join(RUNTIME_SIDECAR_FILE))?
                                    != source.side_bytes
                            {
                                return Err(ActorInputCheckpointError::Unsupported);
                            }
                            Ok(overlay_runtime_sidecar(
                                decode(&actual)?,
                                Some(decode(&source.side_bytes)?),
                            ))
                        })();
                        let session = readback.map_err(|_| {
                            ActorInputCheckpointError::OutcomeUnconfirmed(io::Error::other(
                                "input readback unavailable",
                            ))
                        })?;
                        Ok(ActorInputCheckpointResult {
                            session,
                            status: if present {
                                ActorInputCheckpointStatus::AlreadyCheckpointed
                            } else {
                                ActorInputCheckpointStatus::NewCheckpoint
                            },
                            envelope,
                            generation: proof.generation,
                        })
                    })();
                    Ok(operation)
                },
            )
            .await
            .map_err(ActorInputCheckpointError::OutcomeUnconfirmed)?
    }
}

#[derive(Clone, Copy)]
enum UnownedClaimWrite {
    Seed,
    Confirm,
}

fn unowned_claim_output(
    source: &Source,
    request: &ActorClaimContextSeed,
    mode: UnownedClaimWrite,
) -> Result<Option<String>> {
    if request.claims.is_empty()
        || source.main.created_at != request.expected_created_at
        || encoded(&source.main.messages)? != encoded(&request.expected_messages)?
        || source.main.provider_transcript != request.expected_provider_transcript
        || encoded(&admission(&source.main))? != encoded(&request.expected_admission)?
    {
        return Err(ActorInputCheckpointError::PrefixConflict);
    }
    let mut seen = HashSet::new();
    let mut additions = Vec::new();
    if matches!(mode, UnownedClaimWrite::Confirm) && request.claims.len() != 1 {
        return Err(ActorInputCheckpointError::Unsupported);
    }
    for claim in &request.claims {
        if claim.generation == 0
            || claim.envelope.target_session_id != request.fence.actor_id
            || !seen.insert(claim.envelope.id.clone())
            || !matches!(
                claim.envelope.kind,
                SessionMessageKind::UserInput | SessionMessageKind::PeerMessage
            )
            || !matches!(&claim.envelope.body, SessionMessageBody::Content(content)
                if content.parts.is_empty() && !content.text.trim().is_empty())
        {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        let matching = source
            .main
            .messages
            .iter()
            .any(|message| is_matching_session_message(message, &claim.envelope));
        if source
            .main
            .messages
            .iter()
            .any(|message| message.id == claim.envelope.id.as_str() && !matching)
            || (!matching
                && admission(&source.main)
                    .is_some_and(|cursor| cursor.contains(&claim.envelope.id)))
        {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        if !matching && matches!(mode, UnownedClaimWrite::Confirm) {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        if !matching {
            additions.push(
                claim
                    .envelope
                    .to_provider_message()
                    .map_err(|_| ActorInputCheckpointError::Unsupported)?,
            );
        }
    }
    if matches!(mode, UnownedClaimWrite::Confirm) {
        let claim = &request.claims[0];
        if admission(&source.main).is_some_and(|cursor| cursor.contains(&claim.envelope.id)) {
            return Ok(None);
        }
        let mut cursor = admission(&source.main).unwrap_or_default();
        if !cursor.record(claim.envelope.id.clone(), claim.generation) {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        let fields = object(&source.raw)?.0;
        let cursor_raw = encoded(&cursor)?;
        let runtime = match fields.get("runtime_metadata") {
            Some(raw) if raw.get() != "null" => {
                let runtime_fields = object(raw.get())?.0;
                if let Some(old) = runtime_fields.get("session_inbox_admission") {
                    splice(raw.get(), vec![(*old, cursor_raw.clone())])?
                } else {
                    insert_member(raw.get(), "session_inbox_admission", &cursor_raw)?
                }
            }
            _ => format!("{{\"session_inbox_admission\":{cursor_raw}}}"),
        };
        let output = if let Some(raw) = fields.get("runtime_metadata") {
            splice(&source.raw, vec![(*raw, runtime)])?
        } else {
            insert_member(&source.raw, "runtime_metadata", &runtime)?
        };
        compact_main::validate_full_main(output.as_bytes())
            .map_err(|_| ActorInputCheckpointError::Unsupported)?;
        let committed: Session = decode(output.as_bytes())?;
        if encoded(&committed.messages)? != encoded(&source.main.messages)?
            || committed.provider_transcript != source.main.provider_transcript
            || encoded(&admission(&committed))? != encoded(&Some(cursor))?
        {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        return Ok(Some(output));
    }
    if additions.is_empty() {
        return Ok(None);
    }
    let fields = object(&source.raw)?.0;
    let messages = *fields
        .get("messages")
        .ok_or(ActorInputCheckpointError::Unsupported)?;
    let updated = *fields
        .get("updated_at")
        .ok_or(ActorInputCheckpointError::Unsupported)?;
    let output = splice(
        &source.raw,
        vec![
            (messages, appended_array(messages.get(), &additions)?),
            (updated, encoded(&Utc::now())?),
        ],
    )?;
    compact_main::validate_full_main(output.as_bytes())
        .map_err(|_| ActorInputCheckpointError::Unsupported)?;
    let committed: Session = decode(output.as_bytes())?;
    let mut wanted = source.main.messages.clone();
    wanted.extend(additions);
    if encoded(&committed.messages)? != encoded(&wanted)?
        || committed.provider_transcript != source.main.provider_transcript
        || encoded(&admission(&committed))? != encoded(&admission(&source.main))?
    {
        return Err(ActorInputCheckpointError::Unsupported);
    }
    Ok(Some(output))
}

impl SessionStoreV2 {
    /// Seed the protected canonical main from exact physical unowned claims.
    /// The Inbox operation lock stays held through publication, so a competing
    /// ACK cannot remove a claim between verification and the fenced write.
    /// No cursor or receipt is advanced by this operation.
    pub async fn seed_actor_claim_context(
        &self,
        inbox: FileSessionInbox,
        request: ActorClaimContextSeed,
    ) -> Result<Session> {
        self.checkpoint_unowned_actor_claim(inbox, request, UnownedClaimWrite::Seed)
            .await
    }

    /// Advance only the canonical admission cursor after the selected Worker
    /// confirms the exact physical claim. The caller ACKs that claim only
    /// after this method confirms the durable cursor readback.
    pub async fn confirm_actor_claim_context(
        &self,
        inbox: FileSessionInbox,
        request: ActorClaimContextSeed,
    ) -> Result<Session> {
        self.checkpoint_unowned_actor_claim(inbox, request, UnownedClaimWrite::Confirm)
            .await
    }

    async fn checkpoint_unowned_actor_claim(
        &self,
        inbox: FileSessionInbox,
        request: ActorClaimContextSeed,
        mode: UnownedClaimWrite,
    ) -> Result<Session> {
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
        let source_dir = directory.clone();
        let source_id = request.fence.actor_id.clone();
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
        if !self.session_lifetime_is_live(&source.main).await? {
            return Err(ActorDirectoryError::InvalidIdentity.into());
        }
        self.validate_root_tool_authority_overlay(
            &request.fence.actor_id,
            &source.main,
            Some(&source.side),
        )
        .await?;
        let claim_guard = inbox
            .lock_unowned_actor_claims(&request.fence.actor_id)
            .await?;
        if claim_guard.directory() != directory.join("inbox") {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        for claim in &request.claims {
            claim_guard.verify(claim)?;
        }
        let activation = actor_directory::current_live(&source.entry, &request.fence, Utc::now())?;
        if activation.status != ActorActivationStatus::Running {
            return Err(ActorInputCheckpointError::Unsupported);
        }
        let output = unowned_claim_output(&source, &request, mode)?;
        #[cfg(test)]
        let hook = self.transcript_write_hook.lock().unwrap().clone();
        Self::default_writer_job(&guards, move || {
            let operation = (|| -> Result<Session> {
                let verify = || -> Result<()> {
                    let current = Source::read(&directory, &request.fence.actor_id, kind, &root)?;
                    if !source.unchanged(&current)
                        || !ancestors.matches_current(&home, &current.entry.actor)?
                    {
                        return Err(ActorInputCheckpointError::PrefixConflict);
                    }
                    let activation =
                        actor_directory::current_live(&current.entry, &request.fence, Utc::now())?;
                    if activation.status != ActorActivationStatus::Running {
                        return Err(ActorInputCheckpointError::Unsupported);
                    }
                    for claim in &request.claims {
                        claim_guard.verify(claim)?;
                    }
                    Ok(())
                };
                verify()?;
                let path = directory.join("session.json");
                if let Some(output) = &output {
                    let mut replaced = false;
                    let mut rejection = None;
                    let write = durable_atomic_write_blocking(&path, output.as_bytes(), |phase| {
                        if phase == DurableWritePhase::AfterReplace {
                            replaced = true;
                        }
                        #[cfg(test)]
                        if let Some(hook) = &hook {
                            hook.visit(phase)?;
                        }
                        if phase == DurableWritePhase::BeforeReplace {
                            if let Err(error) = verify() {
                                rejection = Some(error);
                                return Err(io::Error::other(
                                    "Actor claim seed authority rejected",
                                ));
                            }
                        }
                        Ok(())
                    });
                    if let Err(error) = write {
                        return Err(if replaced {
                            ActorInputCheckpointError::OutcomeUnconfirmed(error)
                        } else {
                            rejection.unwrap_or(ActorInputCheckpointError::BeforePublication(error))
                        });
                    }
                }
                #[cfg(test)]
                if let Some(hook) = &hook {
                    hook.before_readback(&path)?;
                }
                let actual = regular_bytes(&path)?;
                if actual != output.as_ref().unwrap_or(&source.raw).as_bytes()
                    || regular_bytes(&directory.join(RUNTIME_SIDECAR_FILE))? != source.side_bytes
                {
                    return Err(ActorInputCheckpointError::OutcomeUnconfirmed(
                        io::Error::other("Actor claim seed readback unavailable"),
                    ));
                }
                Ok(overlay_runtime_sidecar(
                    decode(&actual)?,
                    Some(decode(&source.side_bytes)?),
                ))
            })();
            Ok(operation)
        })
        .await
        .map_err(ActorInputCheckpointError::OutcomeUnconfirmed)?
    }
}
