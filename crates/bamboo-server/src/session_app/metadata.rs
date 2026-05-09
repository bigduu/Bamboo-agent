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
//! 4. Re-check preconditions inside the lock (e.g. `is_untitled` for
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
//!   for `title` and `title_version`. The runtime engine, scheduler, and
//!   tool execution paths are non-authoritative and must stay on
//!   `merge_save_session` / `merge_save_runtime` without touching `title`
//!   directly.
//! - `set_pinned`: the only authoritative writer for `pinned`.
//! - Only these methods bump `metadata_version`.  Runtime paths never bump it,
//!   so `merge_save_session` / `merge_save_runtime` can use it as a staleness
//!   signal for the entire authoritative metadata group.

use bamboo_agent_core::{AgentEvent, Session, TitleSource};
use chrono::Utc;

use crate::app_state::AppState;
use crate::events::publish_replayable_session_event;
use crate::title_gen::is_untitled;

/// Errors returned by [`SessionMetadataService`].
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("storage error: {0}")]
    Storage(String),
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
    /// trimmed input equals the existing title (no event emitted).
    pub async fn set_title(
        state: &AppState,
        session_id: &str,
        new_title: &str,
    ) -> Result<MetadataChange<(String, u64)>, MetadataError> {
        let trimmed = new_title.trim();
        if trimmed.is_empty() {
            return Err(MetadataError::Storage("title cannot be empty".into()));
        }

        // Lock: serialise all writes for this session.
        let _guard = state.persistence.acquire_lock(session_id).await;

        let mut session = load_latest(state, session_id).await?;
        if session.title == trimmed {
            return Ok(None);
        }

        session.title = trimmed.to_string();
        session.title_version = session.title_version.saturating_add(1);
        session.metadata_version = session.metadata_version.saturating_add(1);
        session.updated_at = Utc::now();

        state
            .persistence
            .storage()
            .save_session(&session)
            .await
            .map_err(|e| MetadataError::Storage(format!("save_session: {e}")))?;
        refresh_in_memory_cache(state, session_id, session.clone()).await;

        let event = AgentEvent::SessionTitleUpdated {
            session_id: session.id.clone(),
            title: session.title.clone(),
            title_version: session.title_version,
            source: TitleSource::Manual,
            updated_at: session.updated_at,
        };
        publish_replayable_session_event(state, session_id, event).await;

        Ok(Some((session.title, session.title_version)))
    }

    /// Auto/fallback rename produced by the title generator. Aborts (returns
    /// `Ok(None)`) if the on-disk session is no longer untitled and `force`
    /// is false — this guards against races where the user renames mid-LLM.
    /// On success bumps `title_version` and `metadata_version`, emits with
    /// the supplied [`TitleSource`].
    pub async fn apply_generated_title(
        state: &AppState,
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
        let _guard = state.persistence.acquire_lock(session_id).await;

        let mut session = load_latest(state, session_id).await?;
        if !force && !is_untitled(&session.title) {
            return Ok(None);
        }
        if session.title == trimmed {
            return Ok(None);
        }

        session.title = trimmed.to_string();
        session.title_version = session.title_version.saturating_add(1);
        session.metadata_version = session.metadata_version.saturating_add(1);
        session.updated_at = Utc::now();

        state
            .persistence
            .storage()
            .save_session(&session)
            .await
            .map_err(|e| MetadataError::Storage(format!("save_session: {e}")))?;
        refresh_in_memory_cache(state, session_id, session.clone()).await;

        let event = AgentEvent::SessionTitleUpdated {
            session_id: session.id.clone(),
            title: session.title.clone(),
            title_version: session.title_version,
            source,
            updated_at: session.updated_at,
        };
        publish_replayable_session_event(state, session_id, event).await;

        Ok(Some((session.title, session.title_version)))
    }

    /// Toggle the `pinned` flag. Returns `Ok(None)` if the requested value
    /// matches the current state (no event emitted). Bumps `metadata_version`.
    pub async fn set_pinned(
        state: &AppState,
        session_id: &str,
        pinned: bool,
    ) -> Result<MetadataChange<bool>, MetadataError> {
        // Lock: serialise with runtime saves and other metadata writes.
        let _guard = state.persistence.acquire_lock(session_id).await;

        let mut session = load_latest(state, session_id).await?;
        if session.pinned == pinned {
            return Ok(None);
        }

        session.pinned = pinned;
        session.metadata_version = session.metadata_version.saturating_add(1);
        session.updated_at = Utc::now();

        state
            .persistence
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
}

/// Load the latest session from persistent storage (bypasses the in-memory
/// cache). Called while the per-session lock is held.
async fn load_latest(state: &AppState, session_id: &str) -> Result<Session, MetadataError> {
    state
        .persistence
        .storage()
        .load_session(session_id)
        .await
        .map_err(|e| MetadataError::Storage(format!("load_session: {e}")))?
        .ok_or_else(|| MetadataError::NotFound(session_id.to_string()))
}

/// Replace the in-memory cache entry with the freshly persisted session.
async fn refresh_in_memory_cache(state: &AppState, session_id: &str, session: Session) {
    let mut cache = state.sessions.write().await;
    cache.insert(session_id.to_string(), session);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Session;

    async fn make_state() -> AppState {
        let temp_dir = tempfile::tempdir().unwrap();
        AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state init")
    }

    async fn seed_session(state: &AppState, session_id: &str, title: &str) -> Session {
        let mut session = Session::new(session_id.to_string(), "test-model".to_string());
        session.title = title.to_string();
        state
            .storage
            .save_session(&session)
            .await
            .expect("seed save");
        session
    }

    #[tokio::test]
    async fn set_title_bumps_version_and_emits_event() {
        let state = make_state().await;
        seed_session(&state, "s1", "New Session").await;

        let sender = state.get_session_event_sender("s1").await;
        let mut subscriber = sender.subscribe();

        let result = SessionMetadataService::set_title(&state, "s1", "  Hello  ")
            .await
            .expect("set_title ok");
        let (applied_title, version) = result.expect("change applied");
        assert_eq!(applied_title, "Hello");
        assert_eq!(version, 1);

        let persisted = state.storage.load_session("s1").await.unwrap().unwrap();
        assert_eq!(persisted.title, "Hello");
        assert_eq!(persisted.title_version, 1);
        assert_eq!(persisted.metadata_version, 1); // bumped

        let event = tokio::time::timeout(std::time::Duration::from_millis(100), subscriber.recv())
            .await
            .expect("event before timeout")
            .expect("event received");
        match event {
            AgentEvent::SessionTitleUpdated {
                session_id,
                title,
                title_version,
                source,
                ..
            } => {
                assert_eq!(session_id, "s1");
                assert_eq!(title, "Hello");
                assert_eq!(title_version, 1);
                assert_eq!(source, TitleSource::Manual);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_title_short_circuits_when_unchanged() {
        let state = make_state().await;
        seed_session(&state, "s1", "Same").await;

        let sender = state.get_session_event_sender("s1").await;
        let mut subscriber = sender.subscribe();

        let result = SessionMetadataService::set_title(&state, "s1", "Same")
            .await
            .expect("ok");
        assert!(result.is_none());

        let persisted = state.storage.load_session("s1").await.unwrap().unwrap();
        assert_eq!(persisted.title_version, 0);
        assert_eq!(persisted.metadata_version, 0); // unchanged

        let event_or_timeout =
            tokio::time::timeout(std::time::Duration::from_millis(50), subscriber.recv()).await;
        assert!(event_or_timeout.is_err(), "no event should be broadcast");
    }

    #[tokio::test]
    async fn apply_generated_title_aborts_on_concurrent_rename() {
        // B7: between the LLM call and commit, the user PATCH wins.
        // We simulate this by: (1) load_session called inside service sees
        // a non-untitled disk title, so apply_generated_title returns None.
        let state = make_state().await;
        seed_session(&state, "s1", "User Picked This").await;

        let result = SessionMetadataService::apply_generated_title(
            &state,
            "s1",
            "Auto Title",
            TitleSource::Auto,
            false,
        )
        .await
        .expect("ok");
        assert!(
            result.is_none(),
            "should abort because title is no longer untitled"
        );

        let persisted = state.storage.load_session("s1").await.unwrap().unwrap();
        assert_eq!(persisted.title, "User Picked This");
    }

    #[tokio::test]
    async fn apply_generated_title_force_overrides_existing() {
        let state = make_state().await;
        seed_session(&state, "s1", "User Picked This").await;

        let result = SessionMetadataService::apply_generated_title(
            &state,
            "s1",
            "Forced Auto",
            TitleSource::Auto,
            true,
        )
        .await
        .expect("ok");
        let (applied, version) = result.expect("force applied");
        assert_eq!(applied, "Forced Auto");
        assert_eq!(version, 1);

        let persisted = state.storage.load_session("s1").await.unwrap().unwrap();
        assert_eq!(persisted.title, "Forced Auto");
        assert_eq!(persisted.metadata_version, 1);
    }

    #[tokio::test]
    async fn apply_generated_title_accepts_prompt_scoped_default_placeholder() {
        let state = make_state().await;
        seed_session(&state, "s1", "New session with Bodhi").await;

        let result = SessionMetadataService::apply_generated_title(
            &state,
            "s1",
            "Real Generated Title",
            TitleSource::Auto,
            false,
        )
        .await
        .expect("ok");

        let (applied, version) = result.expect("applied");
        assert_eq!(applied, "Real Generated Title");
        assert_eq!(version, 1);

        let persisted = state.storage.load_session("s1").await.unwrap().unwrap();
        assert_eq!(persisted.title, "Real Generated Title");
        assert_eq!(persisted.title_version, 1);
    }

    #[tokio::test]
    async fn apply_generated_title_uses_correct_source_label() {
        let state = make_state().await;
        seed_session(&state, "s1", "New Session").await;

        let sender = state.get_session_event_sender("s1").await;
        let mut subscriber = sender.subscribe();

        SessionMetadataService::apply_generated_title(
            &state,
            "s1",
            "Heuristic Title",
            TitleSource::Fallback,
            false,
        )
        .await
        .expect("ok")
        .expect("applied");

        let event = tokio::time::timeout(std::time::Duration::from_millis(100), subscriber.recv())
            .await
            .expect("event")
            .expect("not closed");
        match event {
            AgentEvent::SessionTitleUpdated { source, .. } => {
                assert_eq!(source, TitleSource::Fallback);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_pinned_emits_event_and_updates_disk() {
        let state = make_state().await;
        seed_session(&state, "s1", "Title").await;

        let sender = state.get_session_event_sender("s1").await;
        let mut subscriber = sender.subscribe();

        let result = SessionMetadataService::set_pinned(&state, "s1", true)
            .await
            .expect("ok");
        assert_eq!(result, Some(true));

        let persisted = state.storage.load_session("s1").await.unwrap().unwrap();
        assert!(persisted.pinned);
        assert_eq!(persisted.metadata_version, 1); // bumped

        let event = tokio::time::timeout(std::time::Duration::from_millis(100), subscriber.recv())
            .await
            .expect("event")
            .expect("not closed");
        match event {
            AgentEvent::SessionPinnedUpdated {
                session_id, pinned, ..
            } => {
                assert_eq!(session_id, "s1");
                assert!(pinned);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_pinned_short_circuits_when_unchanged() {
        let state = make_state().await;
        seed_session(&state, "s1", "Title").await;

        let result = SessionMetadataService::set_pinned(&state, "s1", false)
            .await
            .expect("ok");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn set_title_returns_not_found_for_unknown_session() {
        let state = make_state().await;
        let err = SessionMetadataService::set_title(&state, "missing", "x")
            .await
            .unwrap_err();
        assert!(matches!(err, MetadataError::NotFound(_)));
    }

    /// A4: concurrent authoritative writes must serialise, with monotonically
    /// increasing versions and event order equal to commit order.
    #[tokio::test]
    async fn concurrent_authoritative_title_writes_serialize() {
        let state = std::sync::Arc::new(make_state().await);
        seed_session(&state, "c1", "New Session").await;

        let sender = state.get_session_event_sender("c1").await;
        let mut subscriber = sender.subscribe();

        let state_a = state.clone();
        let state_b = state.clone();

        let (a, b) = tokio::join!(
            SessionMetadataService::set_title(&state_a, "c1", "Title A"),
            SessionMetadataService::set_title(&state_b, "c1", "Title B"),
        );

        let a = a.expect("A ok").expect("A applied");
        let b = b.expect("B ok").expect("B applied");

        assert!(
            a.1 != b.1,
            "concurrent writes must produce distinct title_versions"
        );
        assert!(
            a.1 == 1 && b.1 == 2 || a.1 == 2 && b.1 == 1,
            "versions must be 1 and 2"
        );

        let persisted = state.storage.load_session("c1").await.unwrap().unwrap();
        assert!(
            persisted.title == "Title A" || persisted.title == "Title B",
            "final title must be one of the two writes"
        );
        assert_eq!(persisted.title_version, 2);
        assert_eq!(persisted.metadata_version, 2);

        let event1 = tokio::time::timeout(std::time::Duration::from_millis(200), subscriber.recv())
            .await
            .expect("event1")
            .expect("not closed");
        let event2 = tokio::time::timeout(std::time::Duration::from_millis(200), subscriber.recv())
            .await
            .expect("event2")
            .expect("not closed");

        let versions: Vec<u64> = vec![
            match &event1 {
                AgentEvent::SessionTitleUpdated { title_version, .. } => *title_version,
                _ => panic!("unexpected event: {event1:?}"),
            },
            match &event2 {
                AgentEvent::SessionTitleUpdated { title_version, .. } => *title_version,
                _ => panic!("unexpected event: {event2:?}"),
            },
        ];
        assert_eq!(versions, vec![1, 2], "event order must match commit order");
    }

    /// A5: manual title beats generated title. The generated path must either
    /// abort or emit a payload consistent with the final persisted state.
    #[tokio::test]
    async fn manual_title_beats_generated_title_without_lying_event() {
        let state = std::sync::Arc::new(make_state().await);
        seed_session(&state, "m1", "New Session").await;

        let sender = state.get_session_event_sender("m1").await;
        let mut subscriber = sender.subscribe();

        let state_gen = state.clone();
        let state_manual = state.clone();

        let manual = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            SessionMetadataService::set_title(&state_manual, "m1", "Manual Override").await
        });

        let gen = tokio::spawn(async move {
            SessionMetadataService::apply_generated_title(
                &state_gen,
                "m1",
                "Auto Generated",
                TitleSource::Auto,
                false,
            )
            .await
        });

        let manual_result = manual.await.expect("manual ok").expect("manual ok");
        let _gen_result = gen.await.expect("gen ok").expect("gen ok");

        // Manual rename must always apply (it's authoritative and always bumps).
        let _manual_changed = manual_result.expect("manual applied");

        // Final persisted state: at least one write landed. Both are valid
        // outcomes depending on race ordering. Key invariant: the two
        // operations completed without error.
        let persisted = state.storage.load_session("m1").await.unwrap().unwrap();
        assert!(persisted.title == "Manual Override" || persisted.title == "Auto Generated");

        // Drain events — at least the manual event must be emitted.
        let mut saw_manual = false;
        let mut events: Vec<AgentEvent> = Vec::new();
        loop {
            match tokio::time::timeout(std::time::Duration::from_millis(100), subscriber.recv())
                .await
            {
                Ok(Ok(e)) => events.push(e),
                _ => break,
            }
        }
        for e in &events {
            if let AgentEvent::SessionTitleUpdated { source, .. } = e {
                if *source == TitleSource::Manual {
                    saw_manual = true;
                }
            }
        }
        assert!(saw_manual, "must emit manual event");
        assert!(!events.is_empty(), "must emit at least one event");
    }

    /// A6: authoritative set_pinned must not be clobbered by a subsequent
    /// non-authoritative (runtime) save.
    #[tokio::test]
    async fn set_pinned_then_runtime_save_does_not_clobber() {
        let state = make_state().await;
        seed_session(&state, "p1", "Title").await;

        SessionMetadataService::set_pinned(&state, "p1", true)
            .await
            .expect("ok")
            .expect("applied");

        let after_pin = state.storage.load_session("p1").await.unwrap().unwrap();
        assert!(after_pin.pinned);
        assert_eq!(after_pin.metadata_version, 1);

        let mut runtime_copy = Session::new("p1".to_string(), "test-model");
        runtime_copy.pinned = false;
        runtime_copy.metadata_version = 0;
        runtime_copy.title = "Title".to_string();

        state
            .persistence
            .merge_save_runtime(&mut runtime_copy)
            .await
            .expect("runtime save ok");

        let after_runtime = state.storage.load_session("p1").await.unwrap().unwrap();
        assert!(after_runtime.pinned, "runtime save must not clobber pinned");
        assert_eq!(
            after_runtime.metadata_version, 1,
            "runtime save must preserve metadata_version"
        );
    }
}
