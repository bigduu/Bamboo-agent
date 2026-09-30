//! Single-winner Child Session CAS for typed direct-parent clarification.
//!
//! The Child resolution is authoritative. Parent terminal and Child wake are
//! idempotent projections performed after this commit and recoverable at boot.

use std::sync::atomic::{AtomicBool, Ordering};

use bamboo_domain::{
    is_matching_session_message, ParentQuestion, ParentQuestionOutcome, ParentQuestionResolution,
    PendingQuestionSource, Session, SessionMessageEnvelope, PARENT_QUESTION_RESOLUTION_KEY,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum State {
    Pending,
    Terminal(ParentQuestionResolution),
}

pub(super) fn state(
    parent: &Session,
    child: &Session,
    question: &ParentQuestion,
) -> Result<State, ()> {
    if let Some(resolution) = ParentQuestionResolution::from_child(parent, child, &question.id) {
        if resolution.request == *question {
            return Ok(State::Terminal(resolution));
        }
        return Err(());
    }
    if ParentQuestion::for_pending(parent, child).as_ref() == Some(question) {
        return Ok(State::Pending);
    }
    Err(())
}

pub(super) async fn answer(
    store: &bamboo_storage::SessionStoreV2,
    sessions: &bamboo_engine::SessionRepository,
    parent: &Session,
    question: &ParentQuestion,
    text: &str,
) -> Result<(State, bool), ()> {
    question.validate_answer(text).map_err(|_| ())?;
    let (saved, wrote) = store
        .answer_parent_question(question, text, |saved| {
            sessions.cache().insert(
                saved.id.clone(),
                std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(saved.clone())),
            );
        })
        .await
        .map_err(|_| ())?
        .ok_or(())?;
    Ok((state(parent, &saved, question)?, wrote))
}

pub(super) async fn expire(
    sessions: &bamboo_engine::SessionRepository,
    parent: &Session,
    question: &ParentQuestion,
) -> Result<(State, bool), ()> {
    let wrote = AtomicBool::new(false);
    let saved = sessions
        .persistence()
        .mutate_runtime_session_and_publish(
            &question.child.session_id,
            || None,
            |child| {
                match state(parent, child, question)? {
                    State::Terminal(_) => return Ok::<_, ()>(()),
                    State::Pending => {}
                }
                let resolution =
                    ParentQuestionResolution::expired(question, chrono::Utc::now()).ok_or(())?;
                child.metadata.insert(
                    PARENT_QUESTION_RESOLUTION_KEY.into(),
                    serde_json::to_string(&resolution).map_err(|_| ())?,
                );
                wrote.store(true, Ordering::Relaxed);
                Ok(())
            },
            |saved| {
                sessions.cache().insert(
                    saved.id.clone(),
                    std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(saved.clone())),
                );
            },
        )
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?
        .ok_or(())?;
    Ok((
        state(parent, &saved, question)?,
        wrote.load(Ordering::Relaxed),
    ))
}

/// Parent or Project authority cannot be loaded. At the fixed deadline the
/// Child still needs a durable fail-closed terminal. This CAS only blocks the
/// Child; it never creates a parent transcript entry or authorizes a reply.
pub(super) async fn block_unavailable_at_deadline(
    sessions: &bamboo_engine::SessionRepository,
    question: &ParentQuestion,
) -> Result<(), ()> {
    block_at_deadline(
        sessions,
        question,
        false,
        "runtime.parent_question.unavailable_at_deadline_v1",
    )
    .await
}

/// A Retired ancestor cannot recover. Once the business deadline passes,
/// close even a previously accepted answer's pending fanout without waking a
/// Child under a revoked lineage. Keep its typed answer in the Child audit.
pub(super) async fn block_retired_at_deadline(
    sessions: &bamboo_engine::SessionRepository,
    question: &ParentQuestion,
) -> Result<(), ()> {
    block_at_deadline(
        sessions,
        question,
        true,
        "runtime.parent_question.retired_at_deadline_v1",
    )
    .await
}

async fn block_at_deadline(
    sessions: &bamboo_engine::SessionRepository,
    question: &ParentQuestion,
    preserve_answer: bool,
    marker_key: &str,
) -> Result<(), ()> {
    sessions
        .persistence()
        .mutate_runtime_session_and_publish(
            &question.child.session_id,
            || None,
            |child| {
                if chrono::Utc::now() < question.deadline {
                    return Err(());
                }
                let terminal = ParentQuestionResolution::from_orphan_child(child, &question.id);
                if terminal
                    .as_ref()
                    .is_some_and(|resolution| resolution.request != *question)
                    || terminal.is_none()
                        && ParentQuestion::for_orphan_pending(child).as_ref() != Some(question)
                {
                    return Err(());
                }
                // Another process may have won the answer CAS after the
                // reconciler's read. Its pending flag is still the outbox
                // obligation for parent terminal and Child wake delivery.
                if !preserve_answer
                    && terminal.as_ref().is_some_and(|resolution| {
                        matches!(resolution.outcome, ParentQuestionOutcome::Answer { .. })
                    })
                {
                    return Err(());
                }
                let pending = child.pending_question.as_ref().ok_or(())?;
                if pending.source != PendingQuestionSource::DirectParent
                    || pending.tool_call_id != question.tool_call_id
                {
                    return Err(());
                }
                if terminal.is_none() {
                    let expired =
                        ParentQuestionResolution::expired(question, chrono::Utc::now()).ok_or(())?;
                    child.metadata.insert(
                        PARENT_QUESTION_RESOLUTION_KEY.into(),
                        serde_json::to_string(&expired).map_err(|_| ())?,
                    );
                }
                child.clear_pending_question();
                child.metadata.remove("clarification_resume_pending");
                child.metadata.insert(
                    "runtime.suspend_reason".into(),
                    "blocked_needs_input".into(),
                );
                child
                    .metadata
                    .insert(marker_key.into(), question.id.to_string());
                Ok::<_, ()>(())
            },
            |saved| {
                sessions.cache().insert(
                    saved.id.clone(),
                    std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(saved.clone())),
                );
            },
        )
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?
        .ok_or(())?;
    Ok(())
}

pub(super) fn terminal_recorded(
    parent: &Session,
    resolution: &ParentQuestionResolution,
) -> Result<bool, ()> {
    let request = resolution.request.envelope();
    let expected = resolution.terminal_envelope().ok_or(())?;
    let mut request_found = false;
    let mut terminal_found = false;
    for message in &parent.messages {
        if message.id == request.id.as_str() {
            if request_found || !canonical(message, &request) {
                return Err(());
            }
            request_found = true;
        }
        if message.id == expected.id.as_str() {
            if terminal_found {
                return Err(());
            }
            let marker = message
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("session_message"))
                .ok_or(())?;
            let envelope: SessionMessageEnvelope =
                serde_json::from_value(marker.clone()).map_err(|_| ())?;
            if !canonical(message, &envelope)
                || ParentQuestionResolution::from_terminal(&request, &envelope)
                    != Some(resolution.clone())
            {
                return Err(());
            }
            terminal_found = true;
        }
    }
    if !request_found {
        return Err(());
    }
    Ok(terminal_found)
}

fn canonical(message: &bamboo_domain::Message, envelope: &SessionMessageEnvelope) -> bool {
    is_matching_session_message(message, envelope)
        && envelope.to_provider_message().ok().is_some_and(|expected| {
            serde_json::to_value(message).ok() == serde_json::to_value(expected).ok()
        })
}

pub(super) async fn append_terminal(
    sessions: &bamboo_engine::SessionRepository,
    resolution: &ParentQuestionResolution,
) -> Result<(), ()> {
    let terminal = resolution.terminal_envelope().ok_or(())?;
    sessions
        .persistence()
        .mutate_runtime_session_and_publish(
            &resolution.request.parent.session_id,
            || None,
            |parent| {
                if !terminal_recorded(parent, resolution)? {
                    parent.add_message(terminal.to_provider_message().map_err(|_| ())?);
                }
                Ok::<_, ()>(())
            },
            |saved| {
                sessions.cache().insert(
                    saved.id.clone(),
                    std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(saved.clone())),
                );
            },
        )
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?
        .ok_or(())?;
    Ok(())
}

/// Called only after the answer wake has durable Inbox activation intent and
/// the parent terminal has been recorded. Until then the pending flag keeps
/// the exact Child visible to the startup outbox reconciler.
pub(super) async fn finish_answer(
    sessions: &bamboo_engine::SessionRepository,
    parent: &Session,
    resolution: &ParentQuestionResolution,
) -> Result<(), ()> {
    if !matches!(resolution.outcome, ParentQuestionOutcome::Answer { .. }) {
        return Err(());
    }
    finish(sessions, parent, resolution, true).await
}

pub(super) async fn finish_expired(
    sessions: &bamboo_engine::SessionRepository,
    parent: &Session,
    resolution: &ParentQuestionResolution,
) -> Result<(), ()> {
    if !matches!(resolution.outcome, ParentQuestionOutcome::Expired) {
        return Err(());
    }
    finish(sessions, parent, resolution, false).await
}

async fn finish(
    sessions: &bamboo_engine::SessionRepository,
    parent: &Session,
    resolution: &ParentQuestionResolution,
    answered: bool,
) -> Result<(), ()> {
    sessions
        .persistence()
        .mutate_runtime_session_and_publish(
            &resolution.request.child.session_id,
            || None,
            |child| {
                if ParentQuestionResolution::from_child(parent, child, &resolution.request.id)
                    != Some(resolution.clone())
                {
                    return Err(());
                }
                let Some(pending) = child.pending_question.as_ref() else {
                    let expected = if answered {
                        child
                            .metadata
                            .get("clarification_resume_pending")
                            .map(String::as_str)
                            == Some("true")
                    } else {
                        child
                            .metadata
                            .get("runtime.suspend_reason")
                            .map(String::as_str)
                            == Some("blocked_needs_input")
                    };
                    return expected.then_some(()).ok_or(());
                };
                if pending.source != PendingQuestionSource::DirectParent
                    || pending.tool_call_id != resolution.request.tool_call_id
                {
                    return Err(());
                }
                child.clear_pending_question();
                if answered {
                    child.metadata.remove("runtime.suspend_reason");
                    child
                        .metadata
                        .insert("clarification_resume_pending".into(), "true".into());
                    child.metadata.insert(
                        "execute.startup_handoff_at".into(),
                        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    );
                } else {
                    child.metadata.insert(
                        "runtime.suspend_reason".into(),
                        "blocked_needs_input".into(),
                    );
                }
                Ok::<_, ()>(())
            },
            |saved| {
                sessions.cache().insert(
                    saved.id.clone(),
                    std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(saved.clone())),
                );
            },
        )
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?
        .ok_or(())?;
    Ok(())
}
