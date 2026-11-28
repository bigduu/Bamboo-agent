//! Program-owned eligibility for Project Work and optional display summaries.

use bamboo_domain::{Message, ProjectId, TaskPriority};
use bamboo_tickets::{Snapshot, Ticket, TicketKind, WorkState};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const INPUT_TOKENS: u32 = 4096;
pub const OUTPUT_TOKENS: u32 = 256;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Blocker {
    OpenQuestions,
    Dependencies,
    ToolContract,
    WorkspaceUnavailable,
    RequiredFileUnavailable { path: String },
}

pub fn ready_card(snapshot: &Snapshot, project: &ProjectId, ticket: &Ticket) -> bool {
    snapshot.project.links.get(&ticket.id) == Some(project)
        && ticket.kind == TicketKind::Work
        && ticket.state == WorkState::Ready
        && !ticket.archived
        && !ticket.paused
        && ticket.blocked.is_none()
        && ticket.active_assignment.is_none()
}

pub fn blocker(snapshot: &Snapshot, ticket: &Ticket) -> Option<Blocker> {
    if ticket
        .draft_brief
        .as_ref()
        .is_some_and(|brief| !brief.open_questions.is_empty())
    {
        return Some(Blocker::OpenQuestions);
    }
    if !ticket.depends_on.iter().all(|id| {
        snapshot.tickets.get(id).is_some_and(|work| {
            work.state == WorkState::Accepted && work.accepted_submission.is_some()
        })
    }) {
        return Some(Blocker::Dependencies);
    }
    if !crate::managed_work::supports_contract(&ticket.contract)
        || (ticket
            .draft_brief
            .as_ref()
            .is_some_and(|brief| !brief.required_files.is_empty())
            && !ticket.contract.allowed_tools.contains("Read"))
    {
        return Some(Blocker::ToolContract);
    }
    None
}

pub fn eligible(snapshot: &Snapshot, project: &ProjectId, ticket: &Ticket) -> bool {
    ready_card(snapshot, project, ticket) && blocker(snapshot, ticket).is_none()
}

pub fn order(ticket: &Ticket) -> (u8, u64, &str) {
    let priority = match ticket.priority {
        TaskPriority::Critical => 0,
        TaskPriority::High => 1,
        TaskPriority::Medium => 2,
        TaskPriority::Low => 3,
    };
    (priority, ticket.updated_seq, &ticket.id)
}

pub fn summary_messages(ticket: &Ticket) -> Option<[Message; 2]> {
    let input = serde_json::to_string(&json!({
        "title": ticket.contract.title,
        "objective": ticket.contract.objective,
        "acceptance": ticket.contract.acceptance,
    }))
    .ok()?;
    (input.len() <= 3000).then(|| {
        [
            Message::system(concat!(
                "Summarize the requested deliverable in the language of this Work card. ",
                "Write one short plain sentence for the board."
            )),
            Message::user(input),
        ]
    })
}

pub fn parse_summary(text: &str) -> Option<String> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Reply {
        summary: String,
    }
    let text = text.trim();
    let summary = if text.starts_with('{') {
        serde_json::from_str::<Reply>(text).ok()?.summary
    } else {
        text.to_owned()
    };
    let summary = summary.trim();
    (!summary.is_empty()
        && summary.len() <= 400
        && !summary.starts_with(['{', '}', '[', '"', '`'])
        && !summary.chars().any(char::is_control))
    .then(|| summary.to_owned())
}

#[cfg(test)]
mod tests {
    use super::parse_summary;

    #[test]
    fn board_summary_accepts_plain_text_and_ignores_old_dispatch_decisions() {
        let summary = "核查 Playwright 用例中的项目切换与看板用量断言。";
        assert_eq!(parse_summary(summary).as_deref(), Some(summary));
        assert_eq!(
            parse_summary(r#"{"summary":"Write help."}"#).as_deref(),
            Some("Write help.")
        );
        assert_eq!(
            parse_summary(r#"{"dispatchable":false,"reason":"No."}"#),
            None
        );
        assert_eq!(parse_summary(r#""summary":"Write help."}"#), None);
        assert_eq!(parse_summary("Multiple\nlines"), None);
    }
}
