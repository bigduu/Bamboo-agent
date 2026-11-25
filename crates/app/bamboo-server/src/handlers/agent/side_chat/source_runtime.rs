use bamboo_domain::{session::TaskItemStatus, Session};
use bamboo_engine::execution::{
    runner_state::RootConversationObservation, VisibleAssistantMessage,
};

use crate::app_state::{AgentStatus, AppState};
use crate::handlers::agent::sessions::SessionActivity;

const MAX_LIVE_TEXT_BYTES: usize = 64 * 1024;
const MAX_LIVE_MESSAGES: usize = 128;

pub(super) struct ObservedSourceRuntime {
    conversation: Option<RootConversationObservation>,
    last_tool: Option<(String, &'static str)>,
    visible_messages: Vec<VisibleAssistantMessage>,
    visible_messages_truncated: bool,
}

pub(super) struct SourceRuntime {
    pub(super) facts: String,
    pub(super) visible_text: Vec<String>,
}

/// Read the current runner before acquiring the source storage lock. Its
/// conversation identity is checked against the durable source at admission.
pub(super) async fn observe_source_runtime(
    state: &AppState,
    session_id: &str,
) -> Option<ObservedSourceRuntime> {
    let runners = state.agent_runners.read().await;
    let runner = runners
        .get(session_id)
        .filter(|runner| matches!(runner.status, AgentStatus::Running))?;
    let conversation = runner.root_conversation_observation();
    let (last_tool, visible_messages, visible_messages_truncated) = if conversation.is_some() {
        let last_tool = runner
            .last_tool_name
            .as_deref()
            .filter(|name| {
                !name.is_empty()
                    && name.len() <= 128
                    && name.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':')
                    })
            })
            .and_then(|name| {
                let phase = match runner.last_tool_phase.as_deref() {
                    Some("begin") => "started",
                    Some("executing") => "executing",
                    Some("activity") => "activity",
                    Some("finished") => "finished",
                    Some("error") => "error",
                    Some("cancelled") => "cancelled",
                    _ => return None,
                };
                Some((name.to_owned(), phase))
            });
        let (visible_messages, truncated) = runner
            .visible_messages
            .pending_text_tail(MAX_LIVE_TEXT_BYTES, MAX_LIVE_MESSAGES);
        (last_tool, visible_messages, truncated)
    } else {
        (None, Vec::new(), false)
    };
    Some(ObservedSourceRuntime {
        conversation,
        last_tool,
        visible_messages,
        visible_messages_truncated,
    })
}

pub(super) fn source_runtime(
    source: &Session,
    observed: Option<&ObservedSourceRuntime>,
) -> SourceRuntime {
    let live = observed.filter(|run| {
        run.conversation
            .as_ref()
            .is_some_and(|conversation| conversation.matches(source))
    });
    let last_status = source.last_run_status();
    let (waiting_for_children, waiting_for_bash) = source.persisted_wait_presence();
    let activity = SessionActivity::derive(
        false,
        source.has_pending_question(),
        last_status.as_deref(),
        0,
        waiting_for_children,
        waiting_for_bash,
    );
    let activity_text = match activity {
        SessionActivity::Running => "running",
        SessionActivity::WaitingChildren => "waiting for delegated sessions",
        SessionActivity::WaitingBash => "waiting for a background shell command",
        SessionActivity::WaitingUser => "waiting for the user's answer",
        SessionActivity::Terminal => match last_status.as_deref() {
            Some("completed") => "last run completed",
            Some("error" | "failed") => "last run failed",
            Some("timeout") => "last run timed out",
            Some("cancelled") => "last run cancelled",
            Some("skipped") => "last run skipped",
            _ => "last run ended",
        },
        SessionActivity::Idle => "idle",
    };
    let runtime_status = if live.is_some() {
        Some("The main session was observed running when this Side Chat question was received.")
    } else if observed.is_some() || last_status.as_deref() == Some("running") {
        Some("Live main session state could not be verified.")
    } else {
        None
    };
    let mut facts = runtime_status
        .map(str::to_owned)
        .unwrap_or_else(|| format!("Main session state: {activity_text}."));
    if runtime_status.is_some() && activity != SessionActivity::Idle {
        facts.push_str(&format!("\nSaved main session state: {activity_text}."));
    }
    if let Some((name, phase)) = live.and_then(|run| run.last_tool.as_ref()) {
        facts.push_str(&format!("\nLatest observed tool: {name} ({phase})."));
    }
    if live.is_some_and(|run| run.visible_messages_truncated) {
        facts.push_str("\nEarlier live assistant text was omitted.");
    }

    if let Some(task_list) = source
        .task_list
        .as_ref()
        .filter(|list| list.session_id == source.id)
    {
        for item in task_list
            .items
            .iter()
            .filter(|item| {
                matches!(
                    item.status,
                    TaskItemStatus::InProgress | TaskItemStatus::Blocked
                )
            })
            .take(3)
        {
            let label = if item.status == TaskItemStatus::InProgress {
                "In-progress task"
            } else {
                "Blocked task"
            };
            let description = item.active_form.as_deref().unwrap_or(&item.description);
            facts.push_str(&format!("\n{label}: {}", truncate(description.trim(), 128)));
        }
    }
    let visible_text = live
        .into_iter()
        .flat_map(|run| &run.visible_messages)
        .filter(|message| !source.messages.iter().any(|saved| saved.id == message.id))
        .map(|message| message.content.trim())
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .collect();

    SourceRuntime {
        facts,
        visible_text,
    }
}

fn truncate(text: &str, max_bytes: usize) -> &str {
    &text[..text.floor_char_boundary(max_bytes)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_engine::execution::VisibleMessageStream;

    #[test]
    fn running_without_local_evidence_is_reported_as_unverified() {
        let mut source = Session::new("main-thread", "model");
        source.set_last_run_status("running");
        let runtime = source_runtime(&source, None);
        assert!(runtime.facts.contains("could not be verified"));
        assert!(!runtime.facts.contains("idle"));
    }

    #[test]
    fn waiting_for_user_excludes_private_question_and_unfenced_live_data() {
        let mut source = Session::new("main-thread", "model");
        source.set_pending_question(
            "internal-call-id".into(),
            "Ask".into(),
            "private pending input".into(),
            Vec::new(),
            true,
        );
        let stream = VisibleMessageStream::new();
        stream.append("obsolete assistant text".into());
        let observed = ObservedSourceRuntime {
            conversation: None,
            last_tool: Some(("obsolete-tool".into(), "started")),
            visible_messages: stream.pending_text_tail(4096, 8).0,
            visible_messages_truncated: true,
        };

        let runtime = source_runtime(&source, Some(&observed));
        assert!(runtime.facts.contains("waiting for the user's answer"));
        assert!(!runtime.facts.contains("private pending input"));
        assert!(!runtime.facts.contains("internal-call-id"));
        assert!(!runtime.facts.contains("obsolete-tool"));
        assert!(!runtime.facts.contains("omitted"));
        assert!(runtime.visible_text.is_empty());
    }
}
