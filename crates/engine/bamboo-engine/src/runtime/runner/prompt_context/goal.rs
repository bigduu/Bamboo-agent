use bamboo_agent_core::{Message, Role, Session};

use super::system_sections::strip_existing_prompt_block;

const GOAL_START_MARKER: &str = "<!-- BAMBOO_GOAL_START -->";
const GOAL_END_MARKER: &str = "<!-- BAMBOO_GOAL_END -->";

/// Render the goal section body that gets wrapped into the system prompt.
fn render_goal_section(goal: &str) -> String {
    format!(
        "## Session Goal\nThe user has set an explicit goal for this session. Keep working autonomously toward it until it is fully achieved. Do not stop early or hand back control while concrete progress is still possible. Still ask a focused clarifying question when you are genuinely blocked or when proceeding would risk irreversible or clearly wrong work; otherwise keep going.\n\nGoal:\n{}",
        goal.trim()
    )
}

/// Inject (or refresh) the session goal block in the system message.
///
/// When `goal` is `None`/empty, any previously injected goal block is stripped.
/// The block is wrapped in stable markers so repeated injection across rounds
/// never duplicates it, mirroring the task-list injection pattern.
pub(super) fn inject_goal_into_system_message(session: &mut Session, goal: Option<&str>) {
    let goal = goal.map(str::trim).filter(|value| !value.is_empty());

    if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, Role::System))
    {
        let base_prompt = strip_existing_goal(&system_message.content);

        match goal {
            Some(goal) => {
                let section = render_goal_section(goal);
                if base_prompt.trim().is_empty() {
                    system_message.content =
                        format!("{GOAL_START_MARKER}\n{section}\n{GOAL_END_MARKER}");
                } else {
                    system_message.content = format!(
                        "{}\n\n{GOAL_START_MARKER}\n{section}\n{GOAL_END_MARKER}",
                        base_prompt.trim_end(),
                    );
                }
                tracing::debug!(
                    "Injected session goal into system message ({} chars)",
                    goal.len()
                );
            }
            None => {
                system_message.content = base_prompt;
            }
        }
    } else if let Some(goal) = goal {
        let section = render_goal_section(goal);
        session.messages.insert(
            0,
            Message::system(format!("{GOAL_START_MARKER}\n{section}\n{GOAL_END_MARKER}")),
        );
        tracing::debug!(
            "Created system message with session goal ({} chars)",
            goal.len()
        );
    }
}

pub(super) fn strip_existing_goal(prompt: &str) -> String {
    strip_existing_prompt_block(prompt, GOAL_START_MARKER, GOAL_END_MARKER)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Session;

    fn system_content(session: &Session) -> &str {
        session
            .messages
            .iter()
            .find(|m| matches!(m.role, Role::System))
            .map(|m| m.content.as_str())
            .unwrap_or("")
    }

    #[test]
    fn injects_goal_block_into_existing_system_message() {
        let mut session = Session::new("s1", "model");
        session.add_message(Message::system("Base prompt.".to_string()));

        inject_goal_into_system_message(&mut session, Some("Ship the feature"));

        let content = system_content(&session);
        assert!(content.contains("Base prompt."));
        assert!(content.contains(GOAL_START_MARKER));
        assert!(content.contains("Ship the feature"));
        assert!(content.contains(GOAL_END_MARKER));
    }

    #[test]
    fn reinjection_does_not_duplicate_goal_block() {
        let mut session = Session::new("s1", "model");
        session.add_message(Message::system("Base prompt.".to_string()));

        inject_goal_into_system_message(&mut session, Some("Goal A"));
        inject_goal_into_system_message(&mut session, Some("Goal B"));

        let content = system_content(&session);
        assert_eq!(content.matches(GOAL_START_MARKER).count(), 1);
        assert!(content.contains("Goal B"));
        assert!(!content.contains("Goal A"));
    }

    #[test]
    fn empty_goal_strips_existing_block() {
        let mut session = Session::new("s1", "model");
        session.add_message(Message::system("Base prompt.".to_string()));

        inject_goal_into_system_message(&mut session, Some("Goal A"));
        inject_goal_into_system_message(&mut session, None);

        let content = system_content(&session);
        assert!(content.contains("Base prompt."));
        assert!(!content.contains(GOAL_START_MARKER));
    }
}
