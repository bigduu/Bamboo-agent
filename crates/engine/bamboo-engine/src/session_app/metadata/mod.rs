//! Authoritative writer for session metadata fields (`title`, `pinned`, …).
//!
//! All callers that mutate session metadata MUST go through this service.
//! Each method follows a fixed pipeline so that title/pinned writes never
//! diverge in subtle ways (load order, version bump, save semantics, event
//! shape):
//!
//! 1. Trim / validate input (fail-fast before acquiring the lock).
//! 2. `persistence.acquire_lock(session_id)` — serialise all writes for this
//!    session so that commit order == publish order.
//! 3. `storage.load_session(session_id)` — pick up the latest authoritative
//!    copy from disk (not a runner-held session that may have stale metadata).
//! 4. Re-check preconditions inside the lock (e.g. `title_generated` for
//!    `apply_generated_title` when not forced; equality short-circuit for
//!    setters that would be a no-op).
//! 5. Mutate the field, bump `title_version` (for title) and always bump
//!    `metadata_version`, set `updated_at`.
//! 6. Plain `storage.save_session(&session)` — no merge needed because we
//!    loaded the latest copy inside the lock and no other writer for this
//!    session could have interleaved.
//! 7. Refresh the in-memory cache (`state.sessions`).
//! 8. Build the corresponding [`AgentEvent`] from the **final persisted
//!    session** and publish via [`publish_replayable_session_event`].
//!
//! ## Authority rules
//!
//! - `set_title` / `apply_generated_title`: the only authoritative writers
//!   for `title`, `title_version`, and `title_generated`. The runtime engine,
//!   scheduler, and
//!   tool execution paths are non-authoritative and must stay on
//!   `merge_save_session` / `merge_save_runtime` without touching `title`
//!   directly.
//! - `set_pinned`: the only authoritative writer for `pinned`.
//! - Only these methods bump `metadata_version`.  Runtime paths never bump it,
//!   so `merge_save_session` / `merge_save_runtime` can use it as a staleness
//!   signal for the entire authoritative metadata group.

use bamboo_agent_core::{AgentEvent, Session, TitleSource};
use chrono::Utc;

use crate::app_context::AgentSessionContext;
use crate::events::publish_replayable_session_event;
use crate::model_config_helper::GOLD_CONFIG_METADATA_KEY;

/// Errors returned by [`SessionMetadataService`].
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("storage error: {0}")]
    Storage(String),
    /// The caller's `If-Match` precondition (expected `metadata_version`) did
    /// not match the current persisted version — a concurrent write won.
    #[error("version conflict: expected {expected}, current {current}")]
    VersionConflict { expected: u64, current: u64 },
}

/// Enforce an optional `If-Match` precondition against the freshly-loaded
/// session, inside the per-session lock (so it is race-free against concurrent
/// authoritative writes). The single `metadata_version` is the session ETag.
fn ensure_if_match(session: &Session, if_match: Option<u64>) -> Result<(), MetadataError> {
    if let Some(expected) = if_match {
        if session.metadata_version != expected {
            return Err(MetadataError::VersionConflict {
                expected,
                current: session.metadata_version,
            });
        }
    }
    Ok(())
}

/// Outcome of a metadata mutation.
///
/// `None` means the request was a no-op (the field already had the requested
/// value, or a guard rejected the change). `Some(applied)` means the change
/// was persisted and an event was published.
pub type MetadataChange<T> = Option<T>;

pub struct SessionMetadataService;

impl SessionMetadataService {
    /// Manual rename via PATCH. Always authoritative; always bumps
    /// `title_version` and `metadata_version`. Returns `Ok(None)` when the
    /// trimmed input equals the existing finalized title (no event emitted).
    /// Renaming a pending session to the same visible text still finalizes the
    /// lifecycle so an in-flight automatic request cannot overwrite it.
    pub async fn set_title(
        state: &dyn AgentSessionContext,
        session_id: &str,
        new_title: &str,
        if_match: Option<u64>,
    ) -> Result<MetadataChange<(String, u64)>, MetadataError> {
        let trimmed = new_title.trim();
        if trimmed.is_empty() {
            return Err(MetadataError::Storage("title cannot be empty".into()));
        }

        // Lock: serialise all writes for this session.
        let _guard = state.persistence().acquire_lock(session_id).await;

        let mut session = load_latest(state, session_id).await?;
        ensure_if_match(&session, if_match)?;
        if session.title == trimmed && session.title_generated {
            return Ok(None);
        }

        session.title = trimmed.to_string();
        session.title_generated = true;
        session.title_version = session.title_version.saturating_add(1);
        session.metadata_version = session.metadata_version.saturating_add(1);
        session.updated_at = Utc::now();

        state
            .persistence()
            .storage()
            .save_session(&session)
            .await
            .map_err(|e| MetadataError::Storage(format!("save_session: {e}")))?;
        refresh_in_memory_cache(state, session_id, session.clone()).await;

        let event = AgentEvent::SessionTitleUpdated {
            session_id: session.id.clone(),
            title: session.title.clone(),
            title_version: session.title_version,
            title_generated: session.title_generated,
            source: TitleSource::Manual,
            updated_at: session.updated_at,
        };
        publish_replayable_session_event(state, session_id, event).await;

        Ok(Some((session.title, session.title_version)))
    }

    /// Auto/fallback rename produced by the title generator. Aborts (returns
    /// `Ok(None)`) if the on-disk title lifecycle is already finalized and
    /// `force` is false — this guards against races where the user renames
    /// mid-LLM without inferring state from the visible title text.
    /// On success bumps `title_version` and `metadata_version`, emits with
    /// the supplied [`TitleSource`].
    pub async fn apply_generated_title(
        state: &dyn AgentSessionContext,
        session_id: &str,
        candidate: &str,
        source: TitleSource,
        force: bool,
    ) -> Result<MetadataChange<(String, u64)>, MetadataError> {
        let trimmed = candidate.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        // Lock: serialise with any concurrent manual rename.
        let _guard = state.persistence().acquire_lock(session_id).await;

        let mut session = load_latest(state, session_id).await?;
        if !force && session.title_generated {
            return Ok(None);
        }
        if session.title == trimmed && session.title_generated {
            return Ok(None);
        }

        session.title = trimmed.to_string();
        session.title_generated = true;
        session.title_version = session.title_version.saturating_add(1);
        session.metadata_version = session.metadata_version.saturating_add(1);
        session.updated_at = Utc::now();

        state
            .persistence()
            .storage()
            .save_session(&session)
            .await
            .map_err(|e| MetadataError::Storage(format!("save_session: {e}")))?;
        refresh_in_memory_cache(state, session_id, session.clone()).await;

        let event = AgentEvent::SessionTitleUpdated {
            session_id: session.id.clone(),
            title: session.title.clone(),
            title_version: session.title_version,
            title_generated: session.title_generated,
            source,
            updated_at: session.updated_at,
        };
        publish_replayable_session_event(state, session_id, event).await;

        Ok(Some((session.title, session.title_version)))
    }

    /// Toggle the `pinned` flag. Returns `Ok(None)` if the requested value
    /// matches the current state (no event emitted). Bumps `metadata_version`.
    pub async fn set_pinned(
        state: &dyn AgentSessionContext,
        session_id: &str,
        pinned: bool,
        if_match: Option<u64>,
    ) -> Result<MetadataChange<bool>, MetadataError> {
        // Lock: serialise with runtime saves and other metadata writes.
        let _guard = state.persistence().acquire_lock(session_id).await;

        let mut session = load_latest(state, session_id).await?;
        ensure_if_match(&session, if_match)?;
        if session.pinned == pinned {
            return Ok(None);
        }

        session.pinned = pinned;
        session.metadata_version = session.metadata_version.saturating_add(1);
        session.updated_at = Utc::now();

        state
            .persistence()
            .storage()
            .save_session(&session)
            .await
            .map_err(|e| MetadataError::Storage(format!("save_session: {e}")))?;
        refresh_in_memory_cache(state, session_id, session.clone()).await;

        let event = AgentEvent::SessionPinnedUpdated {
            session_id: session.id.clone(),
            pinned: session.pinned,
            updated_at: session.updated_at,
        };
        publish_replayable_session_event(state, session_id, event).await;

        Ok(Some(pinned))
    }

    /// Set or clear the session-level Gold configuration JSON.
    ///
    /// This is an authoritative session metadata write: it bumps
    /// `metadata_version` so runtime saves with stale session structs do not
    /// overwrite the user's current-session Gold settings.
    pub async fn set_gold_config_json(
        state: &dyn AgentSessionContext,
        session_id: &str,
        gold_config_json: Option<String>,
        if_match: Option<u64>,
    ) -> Result<MetadataChange<Option<String>>, MetadataError> {
        let normalized = gold_config_json.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        let _guard = state.persistence().acquire_lock(session_id).await;
        let mut session = load_latest(state, session_id).await?;
        ensure_if_match(&session, if_match)?;
        let current = session
            .metadata
            .get(GOLD_CONFIG_METADATA_KEY)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if current == normalized {
            return Ok(None);
        }

        if let Some(value) = normalized.as_ref() {
            session
                .metadata
                .insert(GOLD_CONFIG_METADATA_KEY.to_string(), value.clone());
        } else {
            session.metadata.remove(GOLD_CONFIG_METADATA_KEY);
        }
        session.metadata_version = session.metadata_version.saturating_add(1);
        session.updated_at = Utc::now();

        state
            .persistence()
            .storage()
            .save_session(&session)
            .await
            .map_err(|e| MetadataError::Storage(format!("save_session: {e}")))?;
        refresh_in_memory_cache(state, session_id, session).await;

        Ok(Some(normalized))
    }
}

/// Load the latest session from persistent storage (bypasses the in-memory
/// cache). Called while the per-session lock is held.
async fn load_latest(
    state: &dyn AgentSessionContext,
    session_id: &str,
) -> Result<Session, MetadataError> {
    state
        .persistence()
        .storage()
        .load_session(session_id)
        .await
        .map_err(|e| MetadataError::Storage(format!("load_session: {e}")))?
        .ok_or_else(|| MetadataError::NotFound(session_id.to_string()))
}

/// Replace the in-memory cache entry with the freshly persisted session.
async fn refresh_in_memory_cache(
    state: &dyn AgentSessionContext,
    session_id: &str,
    session: Session,
) {
    state.sessions().insert(
        session_id.to_string(),
        std::sync::Arc::new(parking_lot::RwLock::new(session)),
    );
}
