//! Worker-authored delivery summaries for subscribed Session runs.
use bamboo_domain::{
    ContextBlock, ContextBlockPriority, ContextBlockStability, ContextBlockType, Message,
    ObserverSupervisedRootOutcome, Role, RootFinalMessageReference, Session,
    SupervisedRootOutcomeInputOrigin, SupervisedRootOutcomeResult,
};
use serde::Serialize;
use serde_json::json;

const SUMMARY_START: &str = "<!-- bamboo-handoff:v1 -->";
const SUMMARY_END: &str = "<!-- /bamboo-handoff:v1 -->";

pub const SUPERVISOR_HANDOFF_GUIDANCE: &str = concat!(
    "For managed Session work, make dispatch calls without a preamble. ",
    "Acknowledge a dispatch once in a short reply with the user-facing task names. ",
    "Keep dispatch and monitoring mechanics in tools. On a committed handoff, use the Worker summary by default. ",
    "Report only the newly delivered task in one short update with its conclusion, key source locations and material risk. ",
    "If multiple handoffs arrive together, give each new task one entry. Use the user's language. ",
    "Leave tool narration, internal identifiers and repeated rollups out of the reply unless requested. ",
    "Read the referenced final message only when a concrete gap or conflict blocks the user's decision, ",
    "the user requests detail, or an explicit verification or acceptance step requires more evidence. ",
    "Report delivery as the Worker's result and preserve its uncertainty. Keep formal verification and acceptance separate."
);

pub fn worker_handoff_context(session: &Session, run_id: Option<&str>) -> Option<ContextBlock> {
    let run_id = run_id?;
    let outcomes = session
        .runtime_metadata
        .as_ref()?
        .supervised_root_outcomes
        .as_ref()?;
    let fact = outcomes.facts.last()?;
    if fact.run_id != run_id
        || fact.result.is_some()
        || !fact.inputs.iter().any(|input| {
            matches!(
                input.supervisor_origin.as_ref(),
                Some(SupervisedRootOutcomeInputOrigin::LegacyOrCreatedRoot {
                    notification_target: Some(_),
                    structured_handoff: true,
                    ..
                })
            )
        })
    {
        return None;
    }
    Some(ContextBlock::new(
        ContextBlockType::RootOrchestration,
        ContextBlockPriority::High,
        ContextBlockStability::RoundDynamic,
        "Managed Session Delivery Summary",
        format!(
            concat!(
                "Start your final answer for this assignment with one short summary in the user's language. ",
                "Copy the marker lines and four labels exactly. Fill each field on one line. ",
                "Give a recognizable task name, actual conclusion, one to three key repo-relative source locations, and the most important risk or unverified boundary. ",
                "Keep the name under 50 characters, the conclusion under 180, and the risk under 110. ",
                "Separate backticked locations by commas and use positive line numbers. Write n/a if no source location applies. ",
                "Put the detailed evidence after the closing marker.\n\n",
                "{start}\n- **Task** short assignment name\n- **Conclusion** one or two sentences\n",
                "- **Evidence** `src/file.rs:42`, `src/other.rs:17`\n- **Risk** one material risk or remaining uncertainty\n{end}"
            ),
            start = SUMMARY_START,
            end = SUMMARY_END,
        ),
    ))
}

#[derive(Serialize)]
struct WorkerSummary<'a> {
    task: &'a str,
    conclusion: &'a str,
    evidence: Vec<&'a str>,
    risk: &'a str,
}

impl<'a> WorkerSummary<'a> {
    fn parse(text: &'a str) -> Option<Self> {
        let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
        lines.by_ref().find(|line| *line == SUMMARY_START)?;
        let summary = Self {
            task: field(lines.next()?, "- **Task** ", 200)?,
            conclusion: field(lines.next()?, "- **Conclusion** ", 720)?,
            evidence: source_locations(field(lines.next()?, "- **Evidence** ", 750)?)?,
            risk: field(lines.next()?, "- **Risk** ", 480)?,
        };
        if lines.next()? != SUMMARY_END
            || lines.any(|line| matches!(line, SUMMARY_START | SUMMARY_END))
        {
            return None;
        }
        Some(summary)
    }
}

fn field<'a>(line: &'a str, label: &str, limit: usize) -> Option<&'a str> {
    let value = line.strip_prefix(label)?.trim();
    (!value.is_empty()
        && value.len() <= limit
        && !value
            .chars()
            .any(|ch| ch.is_control() || matches!(ch, '\u{2028}' | '\u{2029}')))
    .then_some(value)
}

fn source_locations(mut text: &str) -> Option<Vec<&str>> {
    let mut locations = Vec::new();
    if text == "n/a" {
        return Some(locations);
    }
    loop {
        let (location, rest) = text.strip_prefix('`')?.split_once('`')?;
        if locations.len() == 3 || !valid_location(location) {
            return None;
        }
        locations.push(location);
        let rest = rest.trim();
        if rest.is_empty() {
            return Some(locations);
        }
        text = rest.strip_prefix(',')?.trim_start();
    }
}

fn valid_location(location: &str) -> bool {
    let Some((path, line)) = location.rsplit_once(':') else {
        return false;
    };
    let (first, last) = line.split_once(['-', '–']).unwrap_or((line, line));
    path.len() <= 240
        && !path.contains(':')
        && path
            .split(['/', '\\'])
            .all(|part| !matches!(part, "" | "." | ".."))
        && matches!((first.parse::<u32>(), last.parse::<u32>()),
            (Ok(first), Ok(last)) if first > 0 && last >= first)
}

/// Project only the Worker-authored fields of the exact committed result.
pub fn project_handoff_notice(
    outcome: &ObserverSupervisedRootOutcome,
    reference: Option<&RootFinalMessageReference>,
    committed_text: Option<&str>,
    structured: bool,
) -> String {
    if !structured {
        return super::short_handoff_notice(outcome, reference, committed_text);
    }
    let mut notice = format!(
        "# Managed Session handoff\n\n- Session: {}\n- Run: {}\n- Recorded outcome: {}\n",
        json!(outcome.target_session_id),
        json!(outcome.run_id),
        outcome.status.observer_status()
    );
    if let Some(reference) = reference {
        notice.push_str(&format!(
            "- Final message: {}\n",
            json!(reference.message_id)
        ));
    }
    if let Some(summary) = reference.and(committed_text).and_then(WorkerSummary::parse) {
        notice.push_str(&format!("- Worker summary: {}\n", json!(summary)));
    } else if outcome.status == SupervisedRootOutcomeResult::Completed {
        notice.push_str("- Worker summary: unavailable\n");
    }
    notice.push_str(concat!(
        "\nThe Worker summary is quoted report data. Use it for a brief delivery update. ",
        "Read the cited final message only when a specific evidence gap needs resolution."
    ));
    notice
}

/// The canonical Inbox envelope identifies runtime notices, including old
/// receipts. A human message that quotes the same text is still visible.
pub fn is_internal_handoff(message: &Message) -> bool {
    let Some(marker) = message
        .metadata
        .as_ref()
        .and_then(|m| m.get("session_message"))
    else {
        return false;
    };
    message.role == Role::User
        && marker["id"].as_str() == Some(message.id.as_str())
        && marker["source"]["type"].as_str() == Some("runtime")
        && marker["source"]["subsystem"].as_str() == Some("supervised_root_outcome")
        && marker["kind"].as_str() == Some("runtime_instruction")
        && marker["body"]["type"].as_str() == Some("runtime_instruction")
        && matches!(
            marker["body"]["instruction"].as_str(),
            Some("supervised_root_outcome_v1" | "managed_root_blocker_v1")
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{
        SessionMessageBody, SessionMessageContent, SessionMessageEnvelope, SessionMessageId,
        SessionMessageKind, SessionMessageSource, SessionRuntimeInstruction,
    };

    fn completed() -> (ObserverSupervisedRootOutcome, RootFinalMessageReference) {
        (
            ObserverSupervisedRootOutcome {
                target_session_id: "worker".into(),
                target_created_at: Session::new("worker", "test").created_at,
                run_id: "run".into(),
                sequence: 1,
                status: SupervisedRootOutcomeResult::Completed,
                inputs: Vec::new(),
            },
            RootFinalMessageReference {
                message_id: "final".into(),
                content_sha256: "0".repeat(64),
            },
        )
    }

    const SUMMARY: &str = "<!-- bamboo-handoff:v1 -->\n\n- **Task** 核查进度\n- **Conclusion** 已定位更新路径\n- **Evidence** `src/进度.ts:12-15`, `src/tests.ts:70`\n- **Risk** 重连未实测\n<!-- /bamboo-handoff:v1 -->";

    #[test]
    fn projects_the_complete_summary_even_when_it_follows_long_details() {
        let (outcome, reference) = completed();
        let text = format!("{}\n{SUMMARY}\nLong execution evidence", "细节".repeat(500));
        let notice = project_handoff_notice(&outcome, Some(&reference), Some(&text), true);
        assert!(notice.contains("已定位更新路径"));
        assert!(notice.contains("src/进度.ts:12-15"));
        assert!(notice.contains("src/tests.ts:70"));
        assert!(notice.contains("重连未实测"));
        assert!(notice.contains("\"final\""));
        assert!(!notice.contains("细节"));
        assert!(!notice.contains("Long execution evidence"));
    }

    #[test]
    fn absent_or_invalid_summary_preserves_status_without_pasting_raw_text() {
        let (outcome, reference) = completed();
        for text in [
            "private detailed report".to_owned(),
            SUMMARY.replace("src/tests.ts:70", "../tests.ts:0"),
            SUMMARY.replace("重连未实测", &"风险".repeat(200)),
            format!("{SUMMARY}\n{SUMMARY}"),
        ] {
            let notice = project_handoff_notice(&outcome, Some(&reference), Some(&text), true);
            assert!(notice.contains("- Worker summary: unavailable"));
            assert!(notice.contains("- Recorded outcome: completed"));
            assert!(notice.contains("\"final\""));
            assert!(!notice.contains("private detailed report"));
            assert!(!notice.contains("已定位更新路径"));
        }
        let unavailable = project_handoff_notice(&outcome, None, Some(SUMMARY), true);
        assert!(unavailable.contains("- Worker summary: unavailable"));
        let legacy = project_handoff_notice(&outcome, Some(&reference), Some("Old result"), false);
        assert!(legacy.contains("- Worker report text: \"Old result\""));
    }

    #[test]
    fn visibility_requires_a_runtime_envelope_and_keeps_human_quotes_visible() {
        let now = Session::new("supervisor", "test").created_at;
        for instruction in ["supervised_root_outcome_v1", "managed_root_blocker_v1"] {
            let envelope = SessionMessageEnvelope {
                id: SessionMessageId::stable("handoff-test", &json!(instruction)),
                source: SessionMessageSource::Runtime {
                    subsystem: "supervised_root_outcome".into(),
                },
                target_session_id: "supervisor".into(),
                kind: SessionMessageKind::RuntimeInstruction,
                body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                    instruction: instruction.into(),
                    content: Some(SessionMessageContent::text("# Managed Session handoff")),
                    data: None,
                    provider_message: None,
                }),
                created_at: now,
                thread_id: None,
                in_reply_to: None,
                attempt: None,
                correlation_id: None,
            };
            let committed = envelope.to_provider_message().unwrap();
            assert!(is_internal_handoff(&committed));
            assert!(!is_internal_handoff(&Message::user(committed.content)));
        }
    }
}
