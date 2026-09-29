//! Immutable request and terminal proofs in the canonical Parent transcript.
//! Neither record is a grant. The caller must still hold a current live scope.
use bamboo_domain::{
    is_matching_session_message, ParentRequest, ParentRequestActivation, ParentRequestActor,
    ParentRequestDelegation, ParentRequestKind, ParentRequestOption, ParentResolution, Session,
    SessionMessageBody, SessionMessageContent, SessionMessageEnvelope, SessionMessageId,
    SessionProviderMessage, PARENT_REQUEST_VERSION,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum State {
    Missing,
    Pending,
    Terminal(bool),
}

#[derive(Clone, Copy)]
enum CommitMode {
    Initialize,
    Resolve(bool),
    Expire,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Terminal {
    request: String,
    live: serde_json::Value,
    approved: bool,
    parent_resolution: ParentResolution,
}

fn bounded(envelope: &SessionMessageEnvelope) -> Option<()> {
    let message = envelope.to_provider_message().ok()?;
    (serde_json::to_vec(envelope).ok()?.len() <= 8192
        && serde_json::to_vec(&message).ok()?.len() <= 8192)
        .then_some(())
}

fn canonical(message: &bamboo_domain::Message, envelope: &SessionMessageEnvelope) -> bool {
    is_matching_session_message(message, envelope)
        && envelope.to_provider_message().ok().is_some_and(|expected| {
            serde_json::to_value(message).ok() == serde_json::to_value(expected).ok()
        })
}

pub(super) fn bind(
    envelope: &mut SessionMessageEnvelope,
    scope: &bamboo_engine::external_agents::actor_adapter::ChildApprovalScope,
    lineage: &[bamboo_domain::ActorSession],
) -> Option<()> {
    let SessionMessageBody::RuntimeInstruction(body) = &mut envelope.body else {
        return None;
    };
    let data = body.data.as_mut()?.as_object_mut()?;
    data.insert("live".into(), scope.stamp().clone());
    data.insert("lineage".into(), serde_json::to_value(lineage).ok()?);
    envelope.created_at = scope.deadline() - chrono::Duration::seconds(240);
    let child = lineage.first()?;
    let parent = lineage.get(1)?;
    let live = scope.stamp();
    let request = ParentRequest {
        version: PARENT_REQUEST_VERSION,
        id: envelope.id.clone(),
        generation: data.get("request_generation")?.as_str()?.into(),
        child: ParentRequestActor {
            session_id: child.actor_id.clone(),
            created_at: child.session_created_at,
        },
        parent: ParentRequestActor {
            session_id: parent.actor_id.clone(),
            created_at: parent.session_created_at,
        },
        root_session_id: child.root_actor_id.clone(),
        project_id: child.project_id.clone(),
        activation: ParentRequestActivation {
            host_scope: live.get("host_scope")?.as_str()?.into(),
            attempt: u32::try_from(live.get("attempt")?.as_u64()?).ok()?,
            run: live.get("run")?.as_str()?.into(),
            epoch: live.get("epoch")?.as_u64()?,
            reply: live.get("reply")?.as_str()?.into(),
        },
        deadline: scope.deadline(),
        kind: ParentRequestKind::ForcedPermission {
            operation_digest: data.get("operation_digest")?.as_str()?.into(),
            policy_revision: data.get("policy_revision")?.as_u64()?,
            maximum_delegation: ParentRequestDelegation::ExactOperationOnce,
            options: vec![ParentRequestOption::Deny, ParentRequestOption::ApproveOnce],
        },
    };
    data.insert("parent_request".into(), serde_json::to_value(request).ok()?);
    ParentRequest::from_forced_permission_envelope(envelope)?;
    bounded(envelope)
}

fn terminal_id(request: &SessionMessageEnvelope) -> SessionMessageId {
    SessionMessageId::stable(
        "direct-parent-forced-terminal-v1",
        &serde_json::json!(request.id),
    )
}

fn terminal(
    request: &SessionMessageEnvelope,
    approved: bool,
    resolved_at: chrono::DateTime<chrono::Utc>,
) -> Option<SessionMessageEnvelope> {
    let typed = ParentRequest::from_forced_permission_envelope(request)?;
    let SessionMessageBody::RuntimeInstruction(original) = &request.body else {
        return None;
    };
    let deadline: chrono::DateTime<chrono::Utc> = serde_json::from_value(
        original
            .data
            .as_ref()?
            .get("live")?
            .get("deadline")?
            .clone(),
    )
    .ok()?;
    let approved = approved && resolved_at < deadline;
    let resolution = ParentResolution::for_forced_permission(&typed, resolved_at, approved);
    let mut result = request.clone();
    result.id = terminal_id(request);
    result.created_at = resolved_at;
    result.in_reply_to = Some(request.id.clone());
    let SessionMessageBody::RuntimeInstruction(body) = &mut result.body else {
        return None;
    };
    body.instruction = "direct_parent_forced_permission_terminal_v1".into();
    let text = format!(
        "Live parent permission review recorded {}. This record is not a permission grant.",
        if approved { "Approved" } else { "Denied" }
    );
    body.content = Some(SessionMessageContent::text(text.clone()));
    body.provider_message = Some(SessionProviderMessage {
        content: SessionMessageContent::text(text),
        metadata: Default::default(),
        never_compress: true,
    });
    body.data = Some(
        serde_json::to_value(Terminal {
            request: request.id.to_string(),
            live: original.data.as_ref()?.get("live")?.clone(),
            approved,
            parent_resolution: resolution,
        })
        .ok()?,
    );
    bounded(&result)?;
    Some(result)
}

pub(super) fn state(session: &Session, request: &SessionMessageEnvelope) -> Result<State, ()> {
    let typed = ParentRequest::from_forced_permission_envelope(request).ok_or(())?;
    let SessionMessageBody::RuntimeInstruction(body) = &request.body else {
        return Err(());
    };
    let data = body.data.as_ref().ok_or(())?;
    if session.id != request.target_session_id
        || typed.parent.session_id != session.id
        || typed.parent.created_at != session.created_at
        || serde_json::to_value(session.created_at).map_err(|_| ())? != data["parent_created_at"]
        || serde_json::to_value(session.project_id_meta()).map_err(|_| ())? != data["project_id"]
    {
        return Err(());
    }
    let end = terminal_id(request).to_string();
    let mut found = State::Missing;
    let mut outcome = None;
    for message in &session.messages {
        let marker = message
            .metadata
            .as_ref()
            .and_then(|v| v.get("session_message"));
        if let Some(marker) = marker {
            let same_request = marker.pointer("/body/data/request_generation")
                == data.get("request_generation")
                || marker
                    .pointer("/body/data/request")
                    .and_then(serde_json::Value::as_str)
                    == Some(request.id.as_str());
            if same_request && message.id != request.id.as_str() && message.id != end {
                return Err(());
            }
        }
        if message.id == request.id.as_str() {
            if found != State::Missing || !canonical(message, request) {
                return Err(());
            }
            found = State::Pending;
        } else if message.id == end {
            if found != State::Pending || outcome.is_some() {
                return Err(());
            }
            let marker = message
                .metadata
                .as_ref()
                .and_then(|v| v.get("session_message"))
                .ok_or(())?;
            let envelope: SessionMessageEnvelope =
                serde_json::from_value(marker.clone()).map_err(|_| ())?;
            let resolved_at = envelope.created_at;
            let SessionMessageBody::RuntimeInstruction(body) = &envelope.body else {
                return Err(());
            };
            let value: Terminal =
                serde_json::from_value(body.data.clone().ok_or(())?).map_err(|_| ())?;
            let deadline: chrono::DateTime<chrono::Utc> =
                serde_json::from_value(value.live["deadline"].clone()).map_err(|_| ())?;
            if resolved_at < request.created_at || (value.approved && resolved_at >= deadline) {
                return Err(());
            }
            let expected = terminal(request, value.approved, resolved_at).ok_or(())?;
            if !canonical(message, &expected)
                || ParentResolution::from_forced_permission_terminal(request, &envelope).is_none()
            {
                return Err(());
            }
            outcome = Some(value.approved);
        }
    }
    if let Some(value) = outcome {
        if found != State::Pending {
            return Err(());
        }
        Ok(State::Terminal(value))
    } else {
        Ok(found)
    }
}

/// Only this new port may initialize, after a first physical Inbox delivery.
/// Finalization/replay never creates a missing request or replaces a winner.
pub(super) async fn commit(
    sessions: &bamboo_engine::SessionRepository,
    request: &SessionMessageEnvelope,
    decision: Option<bool>,
    initialize: bool,
) -> Result<State, ()> {
    let mode = match (decision, initialize) {
        (None, true) => CommitMode::Initialize,
        (Some(value), false) => CommitMode::Resolve(value),
        _ => return Err(()),
    };
    commit_mode(sessions, request, mode)
        .await
        .map(|(state, _)| state)
}

/// Return whether this exact caller appended the terminal under the parent
/// Session lock. A competing reviewer or deadline may have won before us.
pub(super) async fn resolve_with_receipt(
    sessions: &bamboo_engine::SessionRepository,
    request: &SessionMessageEnvelope,
    approved: bool,
) -> Result<(State, bool), ()> {
    commit_mode(sessions, request, CommitMode::Resolve(approved)).await
}

/// Reconcile only a request whose fixed deadline has elapsed. The deadline is
/// checked again while holding the canonical parent-session mutation lock;
/// a stale scan cannot create an early terminal or replace an existing winner.
pub(super) async fn expire(
    sessions: &bamboo_engine::SessionRepository,
    request: &SessionMessageEnvelope,
) -> Result<State, ()> {
    commit_mode(sessions, request, CommitMode::Expire)
        .await
        .map(|(state, _)| state)
}

async fn commit_mode(
    sessions: &bamboo_engine::SessionRepository,
    request: &SessionMessageEnvelope,
    mode: CommitMode,
) -> Result<(State, bool), ()> {
    let typed = ParentRequest::from_forced_permission_envelope(request).ok_or(())?;
    let wrote = AtomicBool::new(false);
    let result = sessions
        .persistence()
        .mutate_runtime_session_and_publish(
            &request.target_session_id,
            || None,
            |parent| {
                let current = state(parent, request)?;
                match (current, mode) {
                    (State::Missing, CommitMode::Initialize) => {
                        parent.add_message(request.to_provider_message().map_err(|_| ())?)
                    }
                    (State::Pending, CommitMode::Resolve(value)) => {
                        let resolved_at = chrono::Utc::now();
                        parent.add_message(
                            terminal(request, value, resolved_at)
                                .ok_or(())?
                                .to_provider_message()
                                .map_err(|_| ())?,
                        );
                        wrote.store(resolved_at < typed.deadline, Ordering::Relaxed);
                    }
                    (State::Pending, CommitMode::Expire)
                        if chrono::Utc::now() >= typed.deadline =>
                    {
                        parent.add_message(
                            terminal(request, false, chrono::Utc::now())
                                .ok_or(())?
                                .to_provider_message()
                                .map_err(|_| ())?,
                        )
                    }
                    (State::Missing, _) => return Err(()),
                    _ => {}
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
    Ok((state(&result, request)?, wrote.load(Ordering::Relaxed)))
}
