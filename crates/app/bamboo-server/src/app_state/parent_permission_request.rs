//! Durable audit of the existing forced-ask reviewer, not a permission grant or
//! a ParentRequest resolution/wait-cycle protocol.
use bamboo_agent_core::{storage::Storage, Session};
use bamboo_domain::{
    ActorDirectoryPort, ActorLogicalState, ActorSession, SessionKind, SessionMessageBody,
    SessionMessageContent, SessionMessageEnvelope, SessionMessageId, SessionMessageKind,
    SessionMessageSource, SessionProviderMessage, SessionRuntimeInstruction,
};
use bamboo_subagent::proto::LogicalSessionIdentity;
use bamboo_tools::permission::{PermissionReasonCode, PermissionRequest, PermissionType};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const MAX_REQUEST_BYTES: usize = 8 * 1024;
const MAX_PARENT_LINEAGE_ACTORS: usize = 16;

/// Session ancestry describes the requested relationship; ActorDirectory is
/// the durable lifecycle authority. A retained Session must not make a
/// retired ancestor available for another child decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LiveActorLineage {
    Live(Vec<ActorSession>),
    Retired,
    Unavailable,
}

pub(super) async fn live_actor_lineage(
    store: &bamboo_storage::SessionStoreV2,
    child_id: &str,
) -> LiveActorLineage {
    let mut current_id = child_id.to_owned();
    let mut previous: Option<ActorSession> = None;
    let mut observed = Vec::new();
    loop {
        if observed.len() >= MAX_PARENT_LINEAGE_ACTORS {
            return LiveActorLineage::Unavailable;
        }
        let session = match store.load_session(&current_id).await {
            Ok(Some(session)) => session,
            _ => return LiveActorLineage::Unavailable,
        };
        let projected = match ActorSession::from_session(&session) {
            Ok(actor) => actor,
            Err(_) => return LiveActorLineage::Unavailable,
        };
        let saved = match store.inspect_actor(&current_id).await {
            Ok(entry) => entry.actor,
            Err(_) => return LiveActorLineage::Unavailable,
        };
        if saved.state == ActorLogicalState::Retired {
            return LiveActorLineage::Retired;
        }
        if !saved.matches_session(&session)
            || saved.project_id != projected.project_id
            || previous.as_ref().is_some_and(|child| {
                child.parent_actor_id.as_deref() != Some(projected.actor_id.as_str())
                    || child.root_actor_id != projected.root_actor_id
                    || child.project_id != projected.project_id
                    || projected.spawn_depth.checked_add(1) != Some(child.spawn_depth)
                    || projected.session_created_at > child.session_created_at
            })
        {
            return LiveActorLineage::Unavailable;
        }
        let parent_id = projected.parent_actor_id.clone();
        previous = Some(projected.clone());
        observed.push(projected);
        match parent_id {
            Some(parent_id) => current_id = parent_id,
            None => {
                return if observed
                    .last()
                    .is_some_and(|root| root.actor_id == root.root_actor_id)
                {
                    LiveActorLineage::Live(observed)
                } else {
                    LiveActorLineage::Unavailable
                }
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovalRunIdentity {
    logical_session: LogicalSessionIdentity,
    project_id: Option<String>,
}

#[derive(Serialize)]
struct ParentPermissionRequest<'a> {
    version: u32,
    child_session_id: &'a str,
    child_created_at: chrono::DateTime<chrono::Utc>,
    parent_session_id: &'a str,
    parent_created_at: chrono::DateTime<chrono::Utc>,
    root_session_id: &'a str,
    project_id: Option<String>,
    request_id: &'a str,
    request_generation: &'a str,
    operation_digest: String,
    policy_revision: u64,
    reason: PermissionReasonCode,
    tool: String,
    permission: PermissionType,
    resource: String,
}

/// All snapshots come from the durable store, never the held parent cache.
/// Re-observation after delivery binds review to the same complete lineage.
pub(super) fn lineage<'a>(
    storage: &'a dyn Storage,
    projects: &'a bamboo_projects::ProjectStore,
    parent_id: &'a str,
    child_id: &'a str,
    body: &'a Value,
) -> futures::future::BoxFuture<'a, Option<(Session, PermissionRequest, Vec<ActorSession>)>> {
    Box::pin(async move {
        let request: PermissionRequest =
            serde_json::from_value(body.get("permission_request")?.clone()).ok()?;
        let identity_value = body.get("approval_identity")?;
        if !identity_value.as_object()?.contains_key("project_id") {
            return None;
        }
        let identity: ApprovalRunIdentity = serde_json::from_value(identity_value.clone()).ok()?;
        let logical = identity.logical_session;
        let creation = logical.creation?;
        if !matches!(
            request.reason_code,
            PermissionReasonCode::HardDangerous | PermissionReasonCode::ConfiguredAlwaysAsk
        ) || request.session_id != child_id
            || logical.session_id != child_id
            || logical.parent_session_id.as_deref() != Some(parent_id)
            || request.request_id.trim().is_empty()
            || request.request_id.len() > 256
            || uuid::Uuid::parse_str(&request.request_generation)
                .ok()?
                .to_string()
                != request.request_generation
            || body.get("tool_name")?.as_str()? != request.tool_name
            || body.get("resource")?.as_str()? != request.resource
            || body.get("permission")?.as_str()? != request.permission_type.description()
        {
            return None;
        }
        let child = storage.load_session(child_id).await.ok()??;
        if child.kind != SessionKind::Child
            || child.created_at != creation.created_at
            || child.spawn_depth != creation.spawn_depth
            || child.root_session_id != logical.root_session_id
            || child.project_id_meta() != identity.project_id
        {
            return None;
        }
        let mut current = ActorSession::from_session(&child).ok()?;
        if let Some(project_id) = current.project_id.as_deref() {
            let id = project_id.parse::<bamboo_domain::ProjectId>().ok()?;
            let project = projects.get(&id).ok()?;
            if project.status != bamboo_domain::ProjectStatus::Active {
                return None;
            }
        }
        if current.parent_actor_id.as_deref() != Some(parent_id) {
            return None;
        }
        let mut observations = vec![current.clone()];
        let mut direct_parent = None;
        while let Some(id) = current.parent_actor_id.as_deref() {
            if observations.len() >= MAX_PARENT_LINEAGE_ACTORS {
                return None;
            }
            let session = storage.load_session(id).await.ok()??;
            let parent = ActorSession::from_session(&session).ok()?;
            if parent.root_actor_id != current.root_actor_id
                || parent.project_id != current.project_id
                || parent.spawn_depth.checked_add(1) != Some(current.spawn_depth)
                || parent.session_created_at > current.session_created_at
            {
                return None;
            }
            if direct_parent.is_none() {
                direct_parent = Some(session);
            }
            observations.push(parent.clone());
            current = parent;
        }
        Some((direct_parent?, request, observations))
    })
}

pub(super) fn envelope(
    parent: &Session,
    child: &ActorSession,
    request: &PermissionRequest,
) -> Option<(SessionMessageEnvelope, String)> {
    let tool = super::parent_approval_reviewer::sanitize_untrusted(
        request.tool_name.rsplit("::").next()?,
        128,
    );
    // Commands, network credentials and private browser keyed resources are
    // authority-only in the audit. Do not persist raw summaries/matchers.
    let resource = if request.has_private_browser_resource()
        || tool.eq_ignore_ascii_case("Bash")
        || !matches!(
            request.permission_type,
            PermissionType::WriteFile | PermissionType::DeleteOperation
        )
        || request.resource.contains("://")
    {
        "[redacted]".into()
    } else {
        super::parent_approval_reviewer::sanitize_untrusted(&request.resource, 800)
    };
    let data = serde_json::to_value(ParentPermissionRequest {
        version: 1,
        child_session_id: &child.actor_id,
        child_created_at: child.session_created_at,
        parent_session_id: &parent.id,
        parent_created_at: parent.created_at,
        root_session_id: &child.root_actor_id,
        project_id: child.project_id.clone(),
        request_id: &request.request_id,
        request_generation: &request.request_generation,
        operation_digest: SessionMessageId::stable(
            "permission-operation",
            &serde_json::to_value(request).ok()?,
        )
        .to_string(),
        policy_revision: request.policy_revision,
        reason: request.reason_code,
        tool: tool.clone(),
        permission: request.permission_type,
        resource: resource.clone(),
    })
    .ok()?;
    let id = SessionMessageId::stable(
        "direct-parent-forced-approval-v1",
        &json!({
            "child": child.actor_id, "birth": child.session_created_at,
            "parent": parent.id, "parent_birth": parent.created_at,
            "generation": request.request_generation,
        }),
    );
    let display = format!("Child {} asks to use {tool} ({}) on {resource}. Direct parent: reply SubAgent(reply_to=\"{}\", message=\"approve_once\") or message=\"deny\" by deadline. No grant.", child.actor_id, request.permission_type.description(), id);
    let envelope = SessionMessageEnvelope {
        id,
        source: SessionMessageSource::Runtime {
            subsystem: "direct_parent_permission_review".into(),
        },
        target_session_id: parent.id.clone(),
        kind: SessionMessageKind::RuntimeInstruction,
        body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
            instruction: "direct_parent_forced_permission_request_v1".into(),
            content: Some(SessionMessageContent::text(display.clone())),
            data: Some(data),
            provider_message: Some(SessionProviderMessage {
                content: SessionMessageContent::text(display.clone()),
                metadata: Default::default(),
                never_compress: true,
            }),
        }),
        created_at: chrono::Utc::now(),
        thread_id: None,
        in_reply_to: None,
        attempt: None,
        correlation_id: None,
    };
    (serde_json::to_vec(&envelope).ok()?.len() <= MAX_REQUEST_BYTES).then_some((envelope, display))
}

/// Transient review input is separate from the redacted durable audit. The
/// worker's typed request carries resource/summary, not an action_details field.
/// Reject unavailable/private or over-budget information instead of asking a
/// model to decide from a tool name or a silently truncated command.
pub(super) fn reviewer_action(request: &PermissionRequest) -> Option<String> {
    if request.has_private_browser_resource()
        || request.permission_type == PermissionType::BrowserInteraction
        || request.resource.contains("://")
        || request.operation_summary.contains("://")
    {
        return None;
    }
    let field = |value: &str| {
        if value.trim().is_empty()
            || value.contains("[redacted]")
            || value.len() > MAX_REQUEST_BYTES
        {
            return None;
        }
        let sanitized =
            super::parent_approval_reviewer::sanitize_untrusted(value, MAX_REQUEST_BYTES);
        // A syntax-changing projection cannot represent the exact operation.
        (sanitized == value).then_some(sanitized)
    };
    let action = json!({
        "tool": field(request.tool_name.rsplit("::").next()?)?,
        "permission": request.permission_type,
        "resource": field(&request.resource)?,
        "operation_summary": field(&request.operation_summary)?,
    });
    let serialized = serde_json::to_string(&action).ok()?;
    (serialized.len() <= MAX_REQUEST_BYTES).then_some(serialized)
}
