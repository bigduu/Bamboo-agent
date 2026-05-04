//! Backend auto-title-generation service.
//!
//! Runs asynchronously after the execute handler accepts a request, derives a
//! concise title for the session via the configured fast model (with a heuristic
//! fallback), persists it via the merge-save helper so concurrent runtime saves
//! cannot clobber it, and emits [`AgentEvent::SessionTitleUpdated`].
//!
//! Public entry points:
//! - [`spawn_title_generation`] - skips work when the session already has a
//!   non-default title.
//! - [`spawn_title_generation_force`] - always regenerates (used by the
//!   `POST /sessions/{id}/regenerate-title` endpoint).
//!
//! Both share an in-flight guard keyed by session id to prevent duplicate work
//! when called repeatedly for the same session.

use std::sync::Arc;
use std::time::Duration;

use bamboo_agent_core::{Message, Role, Session, TitleSource};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::session_app::metadata::SessionMetadataService;

mod fallback;
mod prompt;
#[cfg(test)]
mod tests;

pub use fallback::heuristic_title;
pub use prompt::build_title_messages;

const TITLE_GEN_TIMEOUT_SECS: u64 = 30;
const MAX_TITLE_LEN: usize = 80;
const MAX_USER_TEXT_FOR_TITLE: usize = 2000;

/// Spawn a background task that generates an auto-title for a new (untitled) session.
/// Skips if the session already has a non-default title.
pub fn spawn_title_generation(state: Arc<crate::app_state::AppState>, session_id: String) {
    spawn_inner(state, session_id, false);
}

/// Same as [`spawn_title_generation`] but bypasses the "is untitled" check -
/// always regenerates. Used by `POST /sessions/{id}/regenerate-title`.
pub fn spawn_title_generation_force(state: Arc<crate::app_state::AppState>, session_id: String) {
    spawn_inner(state, session_id, true);
}

fn spawn_inner(state: Arc<crate::app_state::AppState>, session_id: String, force: bool) {
    if !state.title_gen_acquire(&session_id) {
        info!(session_id = %session_id, "title-gen: already in-flight, skipping");
        return;
    }
    let state_for_task = state.clone();
    let sid = session_id.clone();
    tokio::spawn(async move {
        let _guard = TitleGenGuard {
            state: state_for_task.clone(),
            session_id: sid.clone(),
        };
        if let Err(e) = run_title_generation(&state_for_task, &sid, force).await {
            warn!(session_id = %sid, "title-gen failed: {e}");
        }
    });
}

struct TitleGenGuard {
    state: Arc<crate::app_state::AppState>,
    session_id: String,
}

impl Drop for TitleGenGuard {
    fn drop(&mut self) {
        self.state.title_gen_release(&self.session_id);
    }
}

/// Returns true when a session title is "untitled" (empty / whitespace / "New Session").
pub fn is_untitled(title: &str) -> bool {
    let s = title.trim();
    s.is_empty() || s == "New Session"
}

async fn run_title_generation(
    state: &Arc<crate::app_state::AppState>,
    session_id: &str,
    force: bool,
) -> Result<(), String> {
    // 1. Reload session.
    let session = state
        .storage
        .load_session(session_id)
        .await
        .map_err(|e| format!("load_session: {e}"))?
        .ok_or_else(|| "session not found".to_string())?;

    // 2. Skip if already titled (unless forced).
    if !force && !is_untitled(&session.title) {
        debug!(session_id = %session_id, "title-gen: session already titled, skipping");
        return Ok(());
    }

    // 3. Find first user message text.
    let first_user = match first_user_text(&session) {
        Some(t) if !t.trim().is_empty() => t,
        _ => {
            debug!(session_id = %session_id, "title-gen: no user message text, skipping");
            return Ok(());
        }
    };
    let first_user_len = first_user.len();

    // 4. Try LLM, then heuristic fallback.
    let (raw_title, source) = match try_llm_title(state, &session, &first_user).await {
        Ok(t) if !t.trim().is_empty() => {
            info!(session_id = %session_id, prompt_len = first_user_len, "title-gen: LLM produced title");
            (t, TitleSource::Auto)
        }
        Ok(_) => {
            warn!(session_id = %session_id, "title-gen: LLM returned empty, using fallback");
            (heuristic_title(&first_user), TitleSource::Fallback)
        }
        Err(e) => {
            warn!(session_id = %session_id, "title-gen: LLM failed ({e}), using fallback");
            (heuristic_title(&first_user), TitleSource::Fallback)
        }
    };

    let title = sanitize(&raw_title);
    if title.is_empty() {
        debug!(session_id = %session_id, "title-gen: sanitized title empty, skipping");
        return Ok(());
    }

    // 5. Hand off to the authoritative metadata writer. It re-checks
    //    is_untitled (when force=false) inside a per-session lock so a
    //    concurrent user PATCH wins, bumps title_version and metadata_version,
    //    performs a plain save, refreshes the in-memory cache, and emits
    //    SessionTitleUpdated through the replayable helper.
    match SessionMetadataService::apply_generated_title(state, session_id, &title, source, force)
        .await
        .map_err(|e| format!("apply_generated_title: {e}"))?
    {
        Some((applied, title_version)) => {
            info!(
                session_id = %session_id,
                title_len = applied.len(),
                title_version,
                source = ?source,
                "title-gen: applied generated title"
            );
        }
        None => {
            debug!(
                session_id = %session_id,
                "title-gen: skipped (already titled or unchanged)"
            );
        }
    }
    Ok(())
}

/// Extract the first user message's text, preferring `Text` parts when
/// `content_parts` is set. Truncates to `MAX_USER_TEXT_FOR_TITLE` characters.
fn first_user_text(session: &Session) -> Option<String> {
    let msg = session.messages.iter().find(|m| matches!(m.role, Role::User))?;
    let raw = if let Some(parts) = msg.content_parts.as_ref() {
        let joined: String = parts
            .iter()
            .filter_map(|p| match p {
                bamboo_agent_core::MessagePart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        if joined.trim().is_empty() {
            msg.content.clone()
        } else {
            joined
        }
    } else {
        msg.content.clone()
    };
    Some(raw.chars().take(MAX_USER_TEXT_FOR_TITLE).collect())
}

/// Run the LLM call to produce a title. Wrapped in a 30s timeout; any error
/// (resolve, stream, consume, timeout) propagates back so the caller can fall
/// through to the heuristic.
async fn try_llm_title(
    state: &Arc<crate::app_state::AppState>,
    session: &Session,
    first_user_text: &str,
) -> Result<String, String> {
    // Determine provider name (session-aware, with global config fallback) and resolve fast model.
    let config_snapshot = { state.config.read().await.clone() };
    let provider_name = if let Some(ref m) = session.model_ref {
        m.provider.clone()
    } else if let Some(p) = session.metadata.get("provider_name") {
        p.clone()
    } else {
        config_snapshot.provider.clone()
    };

    let resolved = crate::model_config_helper::resolve_fast_model(
        &config_snapshot,
        &provider_name,
        &state.provider_registry,
    )
    .ok_or_else(|| "no fast model resolved".to_string())?;

    let messages: Vec<Message> = build_title_messages(first_user_text);
    let model_name = resolved.model_name.clone();
    let provider = resolved.provider;

    let fut = async move {
        let stream = provider
            .chat_stream(&messages, &[], Some(64), &model_name)
            .await
            .map_err(|e| format!("chat_stream: {e}"))?;
        let output = bamboo_engine::runtime::stream::handler::consume_llm_stream_silent(
            stream,
            &CancellationToken::new(),
            "title-gen",
        )
        .await
        .map_err(|e| format!("consume_llm_stream_silent: {e}"))?;
        Ok::<String, String>(output.content)
    };

    timeout(Duration::from_secs(TITLE_GEN_TIMEOUT_SECS), fut)
        .await
        .map_err(|_| "title-gen LLM timeout".to_string())?
}

/// Trim, take first line, strip leading/trailing quotes/punctuation,
/// cap at [`MAX_TITLE_LEN`] characters.
fn sanitize(input: &str) -> String {
    let first_line = input.lines().next().unwrap_or("").trim();
    let trim_chars: &[char] = &['"', '\'', '`', '.', ',', ';', ':', ' ', '\t'];
    let stripped = first_line.trim_matches(trim_chars).trim();
    let limit = stripped
        .char_indices()
        .nth(MAX_TITLE_LEN)
        .map(|(i, _)| i)
        .unwrap_or(stripped.len());
    stripped[..limit].trim().to_string()
}
