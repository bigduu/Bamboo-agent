//! Opt-in existing-ledger representation commit, not provider context authority.
use super::actor_transcript::{object, splice, Source};
use super::*;
use bamboo_domain::session::provider_transcript::{
    ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor, ProviderTranscriptItem,
    ProviderTranscriptOrigin, ProviderTranscriptState,
};
use bamboo_domain::{
    deterministic_model_context_event_id, model_context_block_sha256, removed_model_context_sha256,
    render_model_context_removal, render_model_context_snapshot, sha256_hex, ActorActivationFence,
    ActorActivationStatus, ActorDirectoryError, ContextBlock, ContextBlockBaseline,
    ContextBlockType, ModelContextEventKind, ModelContextState, MAX_MODEL_CONTEXT_EVENTS,
    MAX_MODEL_CONTEXT_RENDERED_BYTES,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

const LEDGER: &str = "model_context_state";
const LEAF_LIMIT: usize = 4 * 1024 * 1024;
const WITNESS_LIMIT: usize = 2 * 1024 * 1024;
#[derive(Debug, Clone)]
pub struct ActorModelContextCheckpoint {
    pub fence: ActorActivationFence,
    pub expected: Session,
    pub candidate: ModelContextState,
    /// Only pure digest/render witnesses for suffix Snapshots; no authority grant.
    pub snapshot_blocks: Vec<ContextBlock>,
}
#[derive(Debug)]
pub enum ActorModelContextOutcome {
    Committed(Session),
    Already(Session),
}
#[derive(Debug, thiserror::Error)]
pub enum ActorModelContextError {
    #[error("Actor model-context authority rejected: {0}")]
    Authority(#[from] ActorDirectoryError),
    #[error("Actor model-context source is invalid or unsupported")]
    InvalidSource,
    #[error("Actor model-context snapshot changed; reload the complete durable Session")]
    SnapshotConflict,
    #[error("Actor model-context transition or bound is unsupported")]
    Unsupported,
    #[error("Actor model-context was not replaced: {0}")]
    BeforePublication(#[from] io::Error),
    #[error("Actor model-context outcome is unconfirmed; reload before retrying: {0}")]
    OutcomeUnconfirmed(io::Error),
}
type Error = ActorModelContextError;
type Result<T> = std::result::Result<T, Error>;
impl From<ActorTranscriptAppendError> for ActorModelContextError {
    fn from(error: ActorTranscriptAppendError) -> Self {
        match error {
            ActorTranscriptAppendError::Authority(e) => Self::Authority(e),
            ActorTranscriptAppendError::BeforePublication(e) => Self::BeforePublication(e),
            _ => Self::InvalidSource,
        }
    }
}
fn unsupported<T>() -> Result<T> {
    Err(Error::Unsupported)
}
fn hex64(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn strict_fields(
    raw: &str,
    allowed: &[&str],
) -> std::result::Result<(), ActorTranscriptAppendError> {
    if object(raw)?
        .0
        .keys()
        .any(|k| !allowed.contains(&k.as_str()))
    {
        return Err(ActorTranscriptAppendError::InvalidSource);
    }
    Ok(())
}
fn strict_leaf(raw: &str) -> std::result::Result<(), ActorTranscriptAppendError> {
    if raw.len() > LEAF_LIMIT {
        return Err(ActorTranscriptAppendError::InvalidSource);
    }
    if raw == "null" {
        return Ok(());
    }
    strict_fields(
        raw,
        &[
            "schema_version",
            "state_revision",
            "prefix_epoch",
            "next_sequence",
            "baselines",
            "events",
            "cache_scope_sha256",
            "transcript_item_sha256",
            "last_reset_reason",
        ],
    )?;
    let fields = object(raw)?.0;
    if let Some(baselines) = fields.get("baselines") {
        for baseline in object(baselines.get())?.0.values() {
            strict_fields(baseline.get(), &["revision", "content_sha256"])?;
        }
    }
    if let Some(events) = fields.get("events") {
        let entries: Vec<&serde_json::value::RawValue> = serde_json::from_str(events.get())
            .map_err(|_| ActorTranscriptAppendError::InvalidSource)?;
        for event in entries {
            strict_fields(
                event.get(),
                &[
                    "id",
                    "epoch",
                    "sequence",
                    "anchor_message_id",
                    "block_type",
                    "revision",
                    "supersedes_revision",
                    "kind",
                    "content_sha256",
                    "rendered_text",
                ],
            )?;
        }
    }
    Ok(())
}
fn preflight(main: &str, side: &[u8]) -> std::result::Result<(), ActorTranscriptAppendError> {
    // Both raw spans are capped before Source performs any Session typed decode.
    let side = std::str::from_utf8(side).map_err(|_| ActorTranscriptAppendError::InvalidSource)?;
    for raw in [main, side] {
        if let Some(leaf) = object(raw)?.0.get(LEDGER) {
            strict_leaf(leaf.get())?;
        }
    }
    Ok(())
}
fn source(directory: &Path, id: &str, kind: SessionKind, root: &str) -> Result<Source> {
    Ok(Source::read_checked(directory, id, kind, root, preflight)?)
}
struct BoundedBytes {
    bytes: Vec<u8>,
    limit: usize,
    used: usize,
}
impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .used
            .checked_add(bytes.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "model-context JSON cap exceeded",
                )
            })?;
        self.bytes.extend_from_slice(bytes);
        self.used = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn bounded_json(state: &ModelContextState) -> Result<String> {
    let mut output = BoundedBytes {
        bytes: Vec::new(),
        limit: LEAF_LIMIT,
        used: 0,
    };
    serde_json::to_writer(&mut output, state).map_err(|_| Error::Unsupported)?;
    String::from_utf8(output.bytes).map_err(|_| Error::Unsupported)
}
fn anchor(index: usize, hash: &str) -> Result<String> {
    let mut bytes = b"bamboo/model-context-anchor/v1\0".to_vec();
    bytes.extend_from_slice(
        &u64::try_from(index)
            .map_err(|_| Error::Unsupported)?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(hash.as_bytes());
    Ok(format!("anchor_{}", sha256_hex(&bytes)))
}
fn validate_state(id: &str, state: &ModelContextState) -> Result<()> {
    if state.schema_version != 1
        || state.state_revision == 0
        || !state.cache_scope_sha256.as_deref().is_some_and(hex64)
        || state.events.len() > MAX_MODEL_CONTEXT_EVENTS
        || state.next_sequence
            != u64::try_from(state.events.len()).map_err(|_| Error::Unsupported)?
        || state.transcript_item_sha256.iter().any(|s| !hex64(s))
    {
        return unsupported();
    }
    let anchors = state
        .transcript_item_sha256
        .iter()
        .enumerate()
        .map(|(i, hash)| Ok((anchor(i, hash)?, i)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut baselines: BTreeMap<ContextBlockType, ContextBlockBaseline> = BTreeMap::new();
    let mut removed = BTreeSet::new();
    let mut ids = BTreeSet::new();
    let mut last_anchor = None;
    let mut rendered = 0usize;
    for (i, event) in state.events.iter().enumerate() {
        rendered = rendered
            .checked_add(event.rendered_text.len())
            .filter(|n| *n <= MAX_MODEL_CONTEXT_RENDERED_BYTES)
            .ok_or(Error::Unsupported)?;
        if event.epoch != state.prefix_epoch
            || event.sequence != u64::try_from(i).map_err(|_| Error::Unsupported)?
            || !hex64(&event.content_sha256)
            || !ids.insert(&event.id)
            || event.id
                != deterministic_model_context_event_id(
                    id,
                    state.prefix_epoch,
                    event.block_type,
                    event.revision,
                    &event.content_sha256,
                )
        {
            return unsupported();
        }
        match &event.anchor_message_id {
            None if last_anchor.is_none() => (),
            Some(value) => {
                let position = *anchors.get(value).ok_or(Error::Unsupported)?;
                if last_anchor.is_some_and(|old| old > position) {
                    return unsupported();
                }
                last_anchor = Some(position);
            }
            _ => return unsupported(),
        }
        let previous = baselines.get(&event.block_type);
        let revision = match previous {
            None => 1,
            Some(p) => p.revision.checked_add(1).ok_or(Error::Unsupported)?,
        };
        if event.revision != revision
            || event.supersedes_revision != previous.map(|p| p.revision)
            || previous.is_some_and(|p| p.content_sha256 == event.content_sha256)
        {
            return unsupported();
        }
        match event.kind {
            ModelContextEventKind::Snapshot => {
                removed.remove(&event.block_type);
            }
            ModelContextEventKind::Removed => {
                let p = previous.ok_or(Error::Unsupported)?;
                if !removed.insert(event.block_type)
                    || event.content_sha256 != removed_model_context_sha256(event.block_type)
                    || event.rendered_text
                        != render_model_context_removal(
                            &event.id,
                            event.epoch,
                            event.sequence,
                            event.block_type,
                            event.revision,
                            p.revision,
                        )
                {
                    return unsupported();
                }
            }
        }
        baselines.insert(
            event.block_type,
            ContextBlockBaseline {
                revision: event.revision,
                content_sha256: event.content_sha256.clone(),
            },
        );
    }
    if baselines != state.baselines {
        return unsupported();
    }
    Ok(())
}
fn bound_native(state: &ProviderTranscriptState) -> Result<()> {
    if state.active_family().is_none()
        || state.active_protocol().is_none()
        || state.active_provider_boundary_sha256().is_none()
    {
        return unsupported();
    }
    // Existing Domain append derives the private legacy sentinel without cloning its hash format.
    let mut unbound = ProviderTranscriptState::default();
    let items = [
        (ProviderTranscriptAuthor::Model, serde_json::json!({"type":"tool_search_call","id":"call","execution":"server","call_id":"search","status":"completed","arguments":{"query":"x"}})),
        (ProviderTranscriptAuthor::ToolResult, serde_json::json!({"type":"tool_search_output","id":"output","execution":"server","call_id":"search","status":"completed","tools":[{"type":"function","name":"x"}]})),
    ].into_iter().map(|(author, value)| ProviderTranscriptItem::try_from_payload(ProviderFamily::OpenAi,
        ProviderProtocol::OpenAiResponsesV1, ProviderTranscriptOrigin::Provider, author, value))
        .collect::<std::result::Result<Vec<_>, _>>().map_err(|_| Error::Unsupported)?;
    unbound
        .append_group("anchor", None, items)
        .map_err(|_| Error::Unsupported)?;
    if state.active_provider_boundary_sha256() == unbound.active_provider_boundary_sha256() {
        return unsupported();
    }
    Ok(())
}
fn visible(source: &Source) -> Session {
    let mut value = overlay_runtime_sidecar(source.main.clone(), Some(source.side.clone()));
    value.clear_stale_root_token_budget();
    value
}
fn validate_request(
    request: &ActorModelContextCheckpoint,
    source: &Source,
) -> Result<(Session, String, bool)> {
    if !source.side.messages.is_empty()
        || source.side.provider_transcript != ProviderTranscriptState::default()
        || source
            .side
            .runtime_metadata
            .as_ref()
            .is_some_and(|m| m.session_inbox_admission.is_some())
    {
        return Err(Error::InvalidSource);
    }
    bound_native(&source.main.provider_transcript)?;
    let current = visible(source);
    let mut expected = request.expected.clone();
    if expected.kind == SessionKind::Root && expected.root_session_id.is_empty() {
        expected.root_session_id = expected.id.clone();
    }
    expected.clear_stale_root_token_budget();
    if serde_json::to_value(&expected).map_err(|_| Error::Unsupported)?
        != serde_json::to_value(&current).map_err(|_| Error::InvalidSource)?
    {
        return Err(Error::SnapshotConflict);
    }
    let old = current
        .model_context_state
        .as_ref()
        .ok_or(Error::Unsupported)?;
    let next = &request.candidate;
    validate_state(&current.id, old)?;
    validate_state(&current.id, next)?;
    let encoded = bounded_json(next)?;
    // Cap original witness text before digest/render (which trim and allocate).
    let mut text_bytes = 0usize;
    if request.snapshot_blocks.len() > MAX_MODEL_CONTEXT_EVENTS {
        return unsupported();
    }
    let mut blocks = BTreeMap::new();
    for block in &request.snapshot_blocks {
        text_bytes = text_bytes
            .checked_add(block.title.len())
            .and_then(|n| n.checked_add(block.content.len()))
            .filter(|n| *n <= WITNESS_LIMIT)
            .ok_or(Error::Unsupported)?;
        if block.metadata.is_some() || blocks.insert(block.block_type, block).is_some() {
            return unsupported();
        }
    }
    if old == next {
        if !blocks.is_empty() {
            return unsupported();
        }
        return Ok((current, encoded, true));
    }
    if next.prefix_epoch != old.prefix_epoch
        || next.cache_scope_sha256 != old.cache_scope_sha256
        || next.transcript_item_sha256 != old.transcript_item_sha256
        || next.last_reset_reason != old.last_reset_reason
        || next.state_revision
            != old
                .state_revision
                .checked_add(1)
                .ok_or(Error::Unsupported)?
        || next.events.len() <= old.events.len()
        || !next.events.starts_with(&old.events)
    {
        return unsupported();
    }
    let suffix = &next.events[old.events.len()..];
    if next.next_sequence
        != old
            .next_sequence
            .checked_add(u64::try_from(suffix.len()).map_err(|_| Error::Unsupported)?)
            .ok_or(Error::Unsupported)?
    {
        return unsupported();
    }
    let last_boundary = old
        .transcript_item_sha256
        .last()
        .map(|hash| anchor(old.transcript_item_sha256.len() - 1, hash))
        .transpose()?;
    let mut previous_type = None;
    for event in suffix {
        if previous_type.is_some_and(|p| p >= event.block_type)
            || event.anchor_message_id != last_boundary
        {
            return unsupported();
        }
        previous_type = Some(event.block_type);
        if event.kind == ModelContextEventKind::Snapshot {
            let block = blocks.remove(&event.block_type).ok_or(Error::Unsupported)?;
            if model_context_block_sha256(block) != event.content_sha256
                || render_model_context_snapshot(
                    &event.id,
                    event.epoch,
                    event.sequence,
                    block,
                    event.revision,
                    event.supersedes_revision,
                ) != event.rendered_text
            {
                return unsupported();
            }
        }
    }
    if !blocks.is_empty() {
        return unsupported();
    }
    Ok((current, encoded, false))
}
fn running(source: &Source, fence: &ActorActivationFence) -> Result<()> {
    if actor_directory::current_live(&source.entry, fence, Utc::now())?.status
        != ActorActivationStatus::Running
    {
        return Err(ActorDirectoryError::StaleFence.into());
    }
    Ok(())
}
impl SessionStoreV2 {
    /// No generic Storage fallback or production provider admission. Only a
    /// complete current Actor may append a bounded existing representation.
    pub async fn checkpoint_actor_model_context(
        &self,
        request: ActorModelContextCheckpoint,
    ) -> Result<ActorModelContextOutcome> {
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
        let (read_dir, read_id, read_root) = (directory.clone(), id.clone(), root.clone());
        let home = self.sessions_dir.clone();
        let read_home = home.clone();
        let (original, ancestor_bytes) = Self::default_writer_job(&guards, move || {
            Ok((|| -> Result<_> {
                let source = source(&read_dir, &read_id, kind, &read_root)?;
                let bytes = actor_checkpoint_lineage::capture(&read_home, &source.entry.actor)?;
                Ok((source, bytes))
            })())
        })
        .await??;
        let lineage = self.validate_actor_lineage(&original.entry.actor).await?;
        actor_checkpoint_lineage::validate_recorded_observations(
            &original.entry.actor.ancestor_observations,
            &lineage,
        )?;
        if !self.session_lifetime_is_live(&original.main).await? {
            return Err(ActorDirectoryError::InvalidIdentity.into());
        }
        self.validate_root_tool_authority_overlay(&id, &original.main, Some(&original.side))
            .await?;
        running(&original, &request.fence)?;
        let (current, encoded, already) = validate_request(&request, &original)?;
        #[cfg(test)]
        let hook = self.transcript_write_hook.lock().unwrap().clone();
        Self::default_writer_job(&guards, move || {
            let recheck = || -> Result<Source> {
                let now = source(&directory, &id, kind, &root)?;
                if !original.unchanged(&now)
                    || !ancestor_bytes.matches_current(&home, &now.entry.actor)?
                {
                    return Err(Error::InvalidSource);
                }
                running(&now, &request.fence)?;
                Ok(now)
            };
            if let Err(error) = recheck() {
                return Ok(Err(error));
            }
            if already {
                return Ok(recheck().map(|s| ActorModelContextOutcome::Already(visible(&s))));
            }
            let side = std::str::from_utf8(&original.side_bytes)
                .map_err(|_| io::Error::other("invalid Runtime UTF-8"))?;
            let fields = object(side).map_err(io::Error::other)?.0;
            let leaf = *fields
                .get(LEDGER)
                .ok_or_else(|| io::Error::other("missing Runtime ledger"))?;
            let output = splice(side, vec![(leaf, encoded)]).map_err(io::Error::other)?;
            let mut replaced = false;
            let mut rejection = None;
            let path = directory.join(RUNTIME_SIDECAR_FILE);
            let write = durable_atomic_write_blocking(&path, output.as_bytes(), |phase| {
                if phase == DurableWritePhase::AfterReplace {
                    replaced = true;
                }
                #[cfg(test)]
                if let Some(hook) = &hook {
                    hook.visit(phase)?;
                }
                if phase == DurableWritePhase::BeforeReplace {
                    if let Err(error) = recheck() {
                        rejection = Some(error);
                        return Err(io::Error::other(
                            "Actor model-context final authority rejected",
                        ));
                    }
                }
                Ok(())
            });
            if let Err(error) = write {
                return Ok(Err(if replaced {
                    Error::OutcomeUnconfirmed(error)
                } else {
                    rejection.unwrap_or(Error::BeforePublication(error))
                }));
            }
            let readback = (|| -> Result<Session> {
                #[cfg(test)]
                if let Some(hook) = &hook {
                    hook.before_readback(&path)?;
                }
                let actual = source(&directory, &id, kind, &root)?;
                if actual.side_bytes != output.as_bytes()
                    || actual.raw != original.raw
                    || actual.record_bytes != original.record_bytes
                    || actual.marker_bytes != original.marker_bytes
                    || actual.proof_bytes != original.proof_bytes
                {
                    return Err(Error::InvalidSource);
                }
                let actual = visible(&actual);
                let mut expected = current;
                expected.model_context_state = Some(request.candidate);
                if serde_json::to_value(&actual).map_err(|_| Error::InvalidSource)?
                    != serde_json::to_value(expected).map_err(|_| Error::InvalidSource)?
                {
                    return Err(Error::InvalidSource);
                }
                Ok(actual)
            })();
            Ok(readback
                .map(ActorModelContextOutcome::Committed)
                .map_err(|_| {
                    Error::OutcomeUnconfirmed(io::Error::other(
                        "committed ledger readback unavailable",
                    ))
                }))
        })
        .await
        .map_err(Error::OutcomeUnconfirmed)?
    }
}

#[cfg(test)]
#[path = "actor_model_context_tests.rs"]
mod tests;
