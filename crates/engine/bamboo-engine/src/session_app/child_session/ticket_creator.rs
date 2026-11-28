//! The bounded input and sealed result seam for the built-in Ticket Creator.
use super::{named_profile, result_projection, ChildSessionError};
use crate::runtime::execution::{ChildCompletion, ChildCompletionSource};
use bamboo_domain::{ChildContextBinding, ChildContextPacket, Role, Session};
use serde_json::Value;

pub(super) fn draft_context(parent: &Session) -> Result<ChildContextPacket, ChildSessionError> {
    let mut recent = parent.messages.iter().rev().filter(|message| {
        matches!(message.role, Role::User | Role::Assistant)
            && !message.compressed
            && !message.content.trim().is_empty()
            && message.content_parts.as_ref().is_none_or(Vec::is_empty)
            && message.tool_calls.as_ref().is_none_or(Vec::is_empty)
            && message.metadata.as_ref().is_none_or(|metadata| {
                metadata.get("hidden_from_ui").is_none()
                    && metadata.get("child_session_id").is_none()
            })
    });
    let anchor = recent
        .find(|message| message.role == Role::User)
        .ok_or_else(|| {
            ChildSessionError::InvalidArguments(
                "Ticket Creator requires a source user message".into(),
            )
        })?;
    let mut background_message_ids: Vec<_> = recent
        .filter(|message| message.content.len() <= 1_024)
        .take(3)
        .map(|message| message.id.clone())
        .collect();
    background_message_ids.reverse();
    Ok(ChildContextPacket {
        version: 1,
        objective: "Draft the independently trackable Work described in the brief.".into(),
        constraints: Vec::new(),
        acceptance: vec!["Produce a structured draft grounded in the supplied messages.".into()],
        non_goals: Vec::new(),
        necessary_user_instructions: Vec::new(),
        recorded_decisions: Vec::new(),
        source_user_message_ids: vec![anchor.id.clone()],
        background_message_ids,
    })
}

#[async_trait::async_trait]
pub trait TicketDraftPublisher: Send + Sync {
    async fn publish_draft(
        &self,
        parent: &Session,
        child: &Session,
        context: &ChildContextBinding,
        report: Value,
    ) -> Result<Value, String>;
}

pub(crate) async fn publish_completion(
    publisher: Option<&dyn TicketDraftPublisher>,
    completion: &ChildCompletion,
    child: &Session,
    parent: &Session,
) -> Result<Option<Value>, String> {
    if !named_profile::is_ticket_creator(child) {
        return Ok(None);
    }
    let unavailable = |reason| {
        Ok(Some(result_projection::unavailable_child_result(
            "ticket_draft",
            reason,
        )))
    };
    let Some(publisher) = publisher else {
        return unavailable("ticket_host_unavailable");
    };
    let Some(source) = completion.source.as_ref().filter(|source| {
        source.status == "completed"
            && source.matches_completion(completion)
            && ChildCompletionSource::from_committed_session(child).as_ref() == Some(*source)
            && source.matches_parent(child, parent)
    }) else {
        return unavailable("unconfirmed_creator_result");
    };
    let Some(context) = ChildContextBinding::from_session(child).ok().flatten() else {
        return unavailable("unconfirmed_creator_context");
    };
    let Some(report) = child.messages.iter().find(|message| {
        Some(&message.id) == source.result_message_id.as_ref()
            && result_projection::plain_child_report_message(message)
            && message.content.len() <= result_projection::MAX_CHILD_RESULT_BYTES
    }) else {
        return unavailable("creator_report_missing");
    };
    let Ok(report) = result_projection::unique_json(&report.content) else {
        return unavailable("creator_report_invalid");
    };
    publisher
        .publish_draft(parent, child, &context, report)
        .await
        .map(Some)
}
