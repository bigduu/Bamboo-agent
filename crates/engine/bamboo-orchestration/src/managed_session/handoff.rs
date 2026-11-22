//! Worker-authored delivery summaries for subscribed Session runs.
use bamboo_domain::{
    ContextBlock, ContextBlockPriority, ContextBlockStability, ContextBlockType,
    ManagedRootHandoffFormat, Message, ObserverSupervisedRootOutcome, Role,
    RootFinalMessageReference, Session, SessionMessageContent, SessionProviderMessage,
    SupervisedRootOutcomeFact, SupervisedRootOutcomeResult, SupervisorReference,
};
use serde::Serialize;
use serde_json::json;
use std::borrow::Cow;

const SUMMARY_START: &str = "<!-- bamboo-handoff:v1 -->";
const SUMMARY_END: &str = "<!-- /bamboo-handoff:v1 -->";
const DISPLAYED_SOURCES: usize = 3;

pub const DISPATCH_CONFIRMATION: &str = concat!(
    "The first input and delivery subscription are recorded. ",
    "After dispatching the ready work, confirm once by task name and end the turn. ",
    "The Inbox wakes this Supervisor for delivery or a blocker that needs attention."
);

pub const SUPERVISOR_HANDOFF_GUIDANCE: &str = concat!(
    "Dispatch directly with a short task title in the user's language or the exact title they supplied. Enable material notices on supported Session dispatches and followups. ",
    "After dispatching the ready work, acknowledge it once by task name and end the turn. Resume on delivery, an actionable blocker, or a new user request. ",
    "Use bounded progress reads for an explicit progress request or a specific evidence gap. A wait timeout leaves the outcome unknown. ",
    "Park Sessions pending approval. Let the card collect the decision and advance independent work in available Sessions.\n",
    "For each new handoff, give a brief update in the user's language with the Worker's conclusion, key sources and uncertainty. ",
    "Inspect the referenced result for a specific evidence gap, requested detail or acceptance. ",
    "For ongoing work, dispatch the next authorized step before reporting, including bounded reproduction or missing validation when needed.\n",
    "Verify artifacts and evidence against assigned acceptance criteria. Identify a failed criterion, reject the exact current submission when applicable and send a bounded correction. Verify the corrected result. ",
    "When user_acceptance_required is true, wait for explicit User acceptance of the exact current submission."
);

/// The canonical notice stays stable across retries. The provider presentation
/// can clarify what to do with a newly delivered result.
pub fn project_managed_notice(
    supervisor: &SupervisorReference,
    target: &Session,
    fact: &SupervisedRootOutcomeFact,
    canonical_handoff: Option<&str>,
) -> Option<SessionProviderMessage> {
    if let Some(approval) = super::approval::project_approval_notice(supervisor, target, fact) {
        return Some(approval);
    }
    let status = fact.result.as_ref()?;
    if *status != SupervisedRootOutcomeResult::Completed {
        return None;
    }
    let body = if let Some(body) = canonical_handoff {
        Cow::Borrowed(body)
    } else {
        // Current-user follow-ups retain the original status receipt.
        // Project their exact committed result only into presentation.
        let reference = fact.verified_final_message(target);
        let text = reference.and_then(|reference| {
            target
                .messages
                .iter()
                .find(|message| message.id == reference.message_id)
                .map(|message| message.content.as_str())
        });
        Cow::Owned(worker_handoff_text(
            &target.id,
            &fact.run_id,
            status,
            reference,
            text,
            false,
        ))
    };
    Some(SessionProviderMessage {
        content: SessionMessageContent::text(body),
        metadata: Default::default(),
        never_compress: false,
    })
}

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
        || !outcomes.material_inputs(fact).any(|input| {
            input
                .supervisor_origin
                .as_ref()
                .is_some_and(|origin| origin.material_notifications_requested())
                && fact.handoff_format(input) != Some(ManagedRootHandoffFormat::Excerpt)
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
                "Put the most useful source locations first, with positive line numbers. Separate backticked locations by commas. Write n/a if no source location applies. ",
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
    fn parse(text: &'a str, strict_sources: bool) -> Option<Self> {
        let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
        lines.by_ref().find(|line| *line == SUMMARY_START)?;
        let summary = Self {
            task: field(lines.next()?, "- **Task** ", 200)?,
            conclusion: field(lines.next()?, "- **Conclusion** ", 720)?,
            evidence: source_locations(
                field(lines.next()?, "- **Evidence** ", 750)?,
                strict_sources,
            )?,
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

fn source_locations(mut text: &str, strict_sources: bool) -> Option<Vec<&str>> {
    let mut locations = Vec::new();
    if text == "n/a" {
        return Some(locations);
    }
    loop {
        let (location, rest) = text.strip_prefix('`')?.split_once('`')?;
        if !valid_location(location) || (strict_sources && locations.len() == DISPLAYED_SOURCES) {
            return None;
        }
        if locations.len() < DISPLAYED_SOURCES {
            locations.push(location);
        }
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
    format: ManagedRootHandoffFormat,
) -> String {
    if format == ManagedRootHandoffFormat::Excerpt {
        return super::short_handoff_notice(outcome, reference, committed_text);
    }
    worker_handoff_text(
        &outcome.target_session_id,
        &outcome.run_id,
        &outcome.status,
        reference,
        committed_text,
        format == ManagedRootHandoffFormat::StrictSummary,
    )
}

fn worker_handoff_text(
    target_id: &str,
    run_id: &str,
    status: &SupervisedRootOutcomeResult,
    reference: Option<&RootFinalMessageReference>,
    committed_text: Option<&str>,
    strict: bool,
) -> String {
    let mut notice = format!(
        "# Managed Session handoff\n\n- Session: {}\n- Run: {}\n- Recorded outcome: {}\n",
        json!(target_id),
        json!(run_id),
        status.observer_status()
    );
    if let Some(reference) = reference {
        notice.push_str(&format!(
            "- Final message: {}\n",
            json!(reference.message_id)
        ));
    }
    if let Some(summary) = reference
        .and(committed_text)
        .and_then(|text| WorkerSummary::parse(text, strict))
    {
        notice.push_str(&format!("- Worker summary: {}\n", json!(summary)));
    } else if *status == SupervisedRootOutcomeResult::Completed {
        notice.push_str("- Worker summary: unavailable\n");
    }
    notice.push_str(if strict {
        concat!(
            "\nThe Worker summary is quoted report data. Use it for a brief delivery update. ",
            "Read the cited final message only when a specific evidence gap needs resolution."
        )
    } else {
        concat!(
            "\nUse the quoted Worker report for one brief user update about this task. ",
            "Give the conclusion, key source locations and material risk. ",
            "Leave Run IDs, tool details and prior task recaps out. ",
            "Read the referenced result only to resolve a concrete evidence gap. ",
            "If the Run is paused or failed, report the blocker and any action needed in one sentence."
        )
    });
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
        && marker["kind"].as_str() == Some("runtime_instruction")
        && marker["body"]["type"].as_str() == Some("runtime_instruction")
        && matches!(
            (
                marker["source"]["subsystem"].as_str(),
                marker["body"]["instruction"].as_str(),
            ),
            (
                Some("supervised_root_outcome"),
                Some("supervised_root_outcome_v1" | "managed_root_blocker_v1")
            ) | (
                Some(super::approval_handoff::SUBSYSTEM),
                Some(super::approval_handoff::INSTRUCTION)
            )
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
    fn supervisor_delivery_projection_contains_the_result_without_repeating_role_guidance() {
        let mut target = Session::new("worker", "test");
        let message = Message::assistant(SUMMARY, None);
        let reference = RootFinalMessageReference::capture(&message).unwrap();
        target.add_message(message);
        let fact = SupervisedRootOutcomeFact {
            sequence: 2,
            activation_id: "worker-activation".into(),
            attempt: 1,
            lease_epoch: 1,
            lease_owner_sha256: "0".repeat(64),
            run_id: "run".into(),
            conversation_reset_sha256: None,
            resumed_from: Some(1),
            inputs: Vec::new(),
            questions: Vec::new(),
            result: Some(SupervisedRootOutcomeResult::Completed),
            final_message: Some(reference.clone()),
        };
        let supervisor = SupervisorReference {
            session_id: "supervisor".into(),
            incarnation_id: "1b6b8356-52fd-4c4c-84d6-58faf97cb5fc".parse().unwrap(),
        };
        let (outcome, _) = completed();
        let canonical = project_handoff_notice(
            &outcome,
            Some(&reference),
            Some(SUMMARY),
            ManagedRootHandoffFormat::WorkerSummary,
        );
        for canonical_handoff in [Some(canonical.as_str()), None] {
            let notice = project_managed_notice(&supervisor, &target, &fact, canonical_handoff)
                .expect("completed managed handoff");
            assert_eq!(notice.content.text, canonical);
        }
        assert!(canonical.contains("Recorded outcome: completed"));
        assert!(canonical.contains(&reference.message_id));
        assert!(canonical.contains("重连未实测"));
    }

    #[test]
    fn projects_the_complete_summary_even_when_it_follows_long_details() {
        let (outcome, reference) = completed();
        let text = format!("{}\n{SUMMARY}\nLong execution evidence", "细节".repeat(500));
        let notice = project_handoff_notice(
            &outcome,
            Some(&reference),
            Some(&text),
            ManagedRootHandoffFormat::WorkerSummary,
        );
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
            let notice = project_handoff_notice(
                &outcome,
                Some(&reference),
                Some(&text),
                ManagedRootHandoffFormat::WorkerSummary,
            );
            assert!(notice.contains("- Worker summary: unavailable"));
            assert!(notice.contains("- Recorded outcome: completed"));
            assert!(notice.contains("\"final\""));
            assert!(!notice.contains("private detailed report"));
            assert!(!notice.contains("已定位更新路径"));
        }
        let unavailable = project_handoff_notice(
            &outcome,
            None,
            Some(SUMMARY),
            ManagedRootHandoffFormat::WorkerSummary,
        );
        assert!(unavailable.contains("- Worker summary: unavailable"));
        let legacy = project_handoff_notice(
            &outcome,
            Some(&reference),
            Some("Old result"),
            ManagedRootHandoffFormat::Excerpt,
        );
        assert!(legacy.contains("- Worker report text: \"Old result\""));
    }

    #[test]
    fn extra_source_locations_preserve_the_conclusion_risk_and_key_evidence() {
        let (outcome, reference) = completed();
        let text = SUMMARY.replace(
            "`src/tests.ts:70`",
            "`src/tests.ts:70`, `src/third.rs:5`, `src/fourth.rs:7`, `src/fifth.rs:9`",
        );
        let notice = project_handoff_notice(
            &outcome,
            Some(&reference),
            Some(&text),
            ManagedRootHandoffFormat::WorkerSummary,
        );
        for retained in [
            "已定位更新路径",
            "重连未实测",
            "src/进度.ts:12-15",
            "src/tests.ts:70",
            "src/third.rs:5",
        ] {
            assert!(notice.contains(retained), "{notice}");
        }
        assert!(!notice.contains("src/fourth.rs"));
        assert!(!notice.contains("src/fifth.rs"));

        // A receipt from the original strict format is replayed byte-for-byte.
        let old = project_handoff_notice(
            &outcome,
            Some(&reference),
            Some(&text),
            ManagedRootHandoffFormat::StrictSummary,
        );
        assert!(old.contains("- Worker summary: unavailable"));
        assert!(!old.contains("src/third.rs"));
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
