//! The bounded input and sealed result seam for the built-in Ticket Creator.
use super::{named_profile, result_projection, ChildSessionError};
use crate::runtime::execution::{ChildCompletion, ChildCompletionSource};
use bamboo_domain::session::history::is_hidden_from_ui;
use bamboo_domain::{ChildContextBinding, ChildContextPacket, Message, Role, Session};
use serde_json::Value;

fn has_plain_text(message: &Message) -> bool {
    !message.compressed
        && !message.content.trim().is_empty()
        && message.content_parts.as_ref().is_none_or(Vec::is_empty)
        && message.tool_calls.as_ref().is_none_or(Vec::is_empty)
}

pub(super) fn draft_context(parent: &Session) -> Result<ChildContextPacket, ChildSessionError> {
    let mut recent = parent.messages.iter().rev().filter(|message| {
        matches!(message.role, Role::User | Role::Assistant)
            && !is_hidden_from_ui(message)
            && message
                .metadata
                .as_ref()
                .is_none_or(|metadata| metadata.get("child_session_id").is_none())
    });
    let anchor = recent
        .find(|message| message.role == Role::User)
        .filter(|message| has_plain_text(message))
        .ok_or_else(|| {
            ChildSessionError::InvalidArguments(
                "Ticket Creator requires the current source message in plain text".into(),
            )
        })?;
    let mut background_message_ids: Vec<_> = recent
        .filter(|message| has_plain_text(message) && message.content.len() <= 1_024)
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

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::session::MessagePart;

    #[test]
    fn an_unsupported_current_source_does_not_fall_back_to_an_older_turn() {
        let mut parent = Session::new("project-session", "model");
        parent.add_message(Message::user("Older project request."));
        let mut message = Message::user("Use the attached material.");
        message.content_parts = Some(vec![MessagePart::Text {
            text: message.content.clone(),
        }]);
        parent.add_message(message);
        assert!(matches!(
            draft_context(&parent),
            Err(ChildSessionError::InvalidArguments(_))
        ));
    }
}
