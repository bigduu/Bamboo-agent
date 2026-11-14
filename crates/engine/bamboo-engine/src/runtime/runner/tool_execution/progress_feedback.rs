//! Execution-local quiet command diagnostics and their recovery budget.
//!
//! No provider, tool, Session, permission or process operation lives here. The
//! caller supplies only real observations, re-arms capture after consuming quiet
//! facts, and stops before dispatch when Exhausted is returned.

use serde_json::{json, Value};

pub(crate) const REQUIRED_PROGRESS_FEEDBACK_PREFIX: &str = "[runtime_progress_feedback_v1]\n";

const EPISODE_LIMIT: usize = 3;
const RECOVERY_ROUND_LIMIT: usize = 4;
const MAX_ID_CHARS: usize = 128;
const MAX_FEEDBACK_BYTES: usize = 4096;

pub(crate) fn is_required_progress_feedback(text: &str) -> bool {
    text.starts_with(REQUIRED_PROGRESS_FEEDBACK_PREFIX)
}

#[derive(Debug, Clone)]
pub(super) struct QuietCommandEvidence {
    pub session_id: String,
    pub tool_call_id: String,
    pub elapsed_ms: u64,
    pub output_idle_ms: u64,
    pub consecutive_empty_readouts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FeedbackDecision {
    Continue(Option<String>),
    Exhausted { feedback: String, summary: String },
}

#[derive(Debug, Clone)]
struct BoundedEvidence {
    facts: QuietCommandEvidence,
    identifiers_shortened: bool,
}

#[derive(Debug, Clone)]
struct ActiveEpisode {
    evidence: BoundedEvidence,
    rounds_used: usize,
}

/// A single real execution owns this value. Global run budgets may be unlimited;
/// this separate diagnostic allowance is always finite and is never refunded by
/// a model claiming progress, a new fact packet, or a hidden wakeup.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProgressFeedbackBudget {
    episodes_started: usize,
    active: Option<ActiveEpisode>,
    exhausted: Option<(String, String)>,
}

impl ProgressFeedbackBudget {
    /// Call once at a real next-provider boundary, not on provider retry.
    ///
    /// made_progress must describe changed real tool evidence, not model text.
    /// It ends the active episode but does not refund its slot. If fresh
    /// stagnation evidence also arrives, that starts the next episode.
    /// None retains the last facts while active; it cannot bypass the limit.
    pub(super) fn next_round(
        &mut self,
        evidence: Option<QuietCommandEvidence>,
        made_progress: bool,
    ) -> FeedbackDecision {
        if let Some((feedback, summary)) = &self.exhausted {
            return FeedbackDecision::Exhausted {
                feedback: feedback.clone(),
                summary: summary.clone(),
            };
        }
        if made_progress {
            self.active = None;
        }
        let fresh_evidence = evidence.is_some();
        if let Some(evidence) = evidence {
            let evidence = BoundedEvidence::new(evidence);
            if let Some(active) = &mut self.active {
                active.evidence = evidence;
            } else {
                if self.episodes_started >= EPISODE_LIMIT {
                    return self.exhaust(evidence, 0, fresh_evidence, "episode_limit");
                }
                self.episodes_started += 1;
                self.active = Some(ActiveEpisode {
                    evidence,
                    rounds_used: 0,
                });
            }
        }
        let Some(mut active) = self.active.take() else {
            return FeedbackDecision::Continue(None);
        };
        if active.rounds_used >= RECOVERY_ROUND_LIMIT {
            return self.exhaust(
                active.evidence,
                active.rounds_used,
                fresh_evidence,
                "recovery_round_limit",
            );
        }
        active.rounds_used += 1;
        let feedback = render_feedback(
            &active.evidence,
            self.episodes_started,
            active.rounds_used,
            fresh_evidence,
            None,
        );
        self.active = Some(active);
        FeedbackDecision::Continue(Some(feedback))
    }

    /// A terminal Inbox result supersedes the last quiet observation for that
    /// exact exec, without refunding the episode or clearing other evidence.
    pub(super) fn observe_exec_completion(&mut self, handle: &str) {
        if self
            .active
            .as_ref()
            .is_some_and(|episode| episode.evidence.facts.session_id == handle)
        {
            self.active = None;
        }
    }

    /// Only newly admitted real Human input may call this. Hidden notifications,
    /// provider retries, model claims and changed observations must not call it.
    pub(crate) fn reset_for_human_input(&mut self) {
        *self = Self::default();
    }

    fn exhaust(
        &mut self,
        evidence: BoundedEvidence,
        rounds_used: usize,
        fresh_evidence: bool,
        reason: &'static str,
    ) -> FeedbackDecision {
        let feedback = render_feedback(
            &evidence,
            self.episodes_started,
            rounds_used,
            fresh_evidence,
            Some(reason),
        );
        let summary = "The task is not confirmed complete. The finite diagnostic budget is \
                 exhausted, so this execution stopped before another recovery-model request or tool \
                 round. The existing command was not cancelled by this progress guard. Its \
                 last observed status was running, with zero output bytes and no observed \
                 exit; silence does not prove completion, failure, or liveness. Check its \
                 real status and output or provide new Human direction."
            .to_owned();
        self.active = None;
        self.exhausted = Some((feedback.clone(), summary.clone()));
        FeedbackDecision::Exhausted { feedback, summary }
    }
}

impl BoundedEvidence {
    fn new(mut facts: QuietCommandEvidence) -> Self {
        let (session_id, session_shortened) = bounded_id(&facts.session_id);
        let (tool_call_id, call_shortened) = bounded_id(&facts.tool_call_id);
        facts.session_id = session_id;
        facts.tool_call_id = tool_call_id;
        Self {
            facts,
            identifiers_shortened: session_shortened || call_shortened,
        }
    }

    fn packet_evidence(&self, include_ids: bool) -> Value {
        let mut evidence = json!({
            "elapsed_ms": self.facts.elapsed_ms,
            "output_idle_ms": self.facts.output_idle_ms,
            "consecutive_empty_readouts": self.facts.consecutive_empty_readouts,
            "last_status": "running",
            "output_bytes": 0,
            "exit_observed": false,
            "liveness_verified": false,
            "identifiers_shortened": self.identifiers_shortened || !include_ids,
            "identifiers_omitted_for_packet_budget": !include_ids,
        });
        if include_ids {
            evidence["session_id"] = json!(self.facts.session_id);
            evidence["tool_call_id"] = json!(self.facts.tool_call_id);
        }
        evidence
    }
}

fn bounded_id(id: &str) -> (String, bool) {
    let mut chars = id.chars();
    let bounded = chars.by_ref().take(MAX_ID_CHARS).collect();
    (bounded, chars.next().is_some())
}

fn render_feedback(
    evidence: &BoundedEvidence,
    episode: usize,
    rounds_used: usize,
    fresh_evidence: bool,
    exhaustion: Option<&'static str>,
) -> String {
    let render = |include_ids| {
        ascii_json_packet(json!({
            "schema": "runtime_progress_feedback_v1",
            "source": "runtime",
            "reason": "quiet_command",
            "state": if exhaustion.is_some() { "exhausted" } else { "recovery" },
            "exhaustion": exhaustion,
            "evidence": evidence.packet_evidence(include_ids),
            "evidence_is_fresh": fresh_evidence,
            "recovery": {
                "episode": episode,
                "episode_limit": EPISODE_LIMIT,
                "episodes_remaining": EPISODE_LIMIT.saturating_sub(episode),
                "rounds_remaining": if exhaustion.is_some() {
                    0
                } else {
                    RECOVERY_ROUND_LIMIT.saturating_sub(rounds_used)
                },
                "round_limit": RECOVERY_ROUND_LIMIT,
            },
            "guidance": if exhaustion.is_some() {
                "Stop this execution before another provider request or tool round. \
                 Report the task as unconfirmed; no extra summary model call is allowed."
            } else {
                "Use real tool evidence to decide whether checks are intentional polling \
                 or stalled work. If stalled, change approach instead of repeating identical \
                 calls. Silence proves neither completion nor failure. Existing approvals \
                 still apply; this diagnostic grants no permission. Shortened IDs are display \
                 hints: use exact IDs from the original tool results."
            },
        }))
    };
    let packet = render(true);
    if packet.len() <= MAX_FEEDBACK_BYTES {
        packet
    } else {
        // Only fixed schema/guidance, numeric facts and states remain. Explicitly
        // mark missing references; never drop the required diagnostic itself.
        render(false)
    }
}

/// serde handles JSON quoting first. Escape non-ASCII scalars afterwards so the
/// 4096-byte check already includes six-byte UTF-16 escapes and surrogate pairs.
fn ascii_json_packet(value: Value) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let rendered = value.to_string();
    let mut packet = String::from(REQUIRED_PROGRESS_FEEDBACK_PREFIX);
    for ch in rendered.chars() {
        if ch.is_ascii() {
            packet.push(ch);
        } else {
            let mut units = [0; 2];
            for &unit in ch.encode_utf16(&mut units).iter() {
                packet.push_str("\\u");
                for shift in [12, 8, 4, 0] {
                    packet.push(char::from(HEX[usize::from((unit >> shift) & 0xf)]));
                }
            }
        }
    }
    packet
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quiet() -> QuietCommandEvidence {
        QuietCommandEvidence {
            session_id: "owned-handle".into(),
            tool_call_id: "poll-1".into(),
            elapsed_ms: 120_000,
            output_idle_ms: 90_000,
            consecutive_empty_readouts: 6,
        }
    }

    fn parse(packet: &str) -> Value {
        assert!(is_required_progress_feedback(packet));
        assert!(packet.is_ascii());
        assert!(packet.len() <= MAX_FEEDBACK_BYTES, "{}", packet.len());
        serde_json::from_str(
            packet
                .strip_prefix(REQUIRED_PROGRESS_FEEDBACK_PREFIX)
                .unwrap(),
        )
        .unwrap()
    }

    fn continued(decision: FeedbackDecision) -> Value {
        match decision {
            FeedbackDecision::Continue(Some(packet)) => parse(&packet),
            other => panic!("expected a recovery fact packet, got {other:?}"),
        }
    }

    fn exhausted(decision: FeedbackDecision) -> (Value, String) {
        match decision {
            FeedbackDecision::Exhausted { feedback, summary } => (parse(&feedback), summary),
            other => panic!("expected finite exhaustion, got {other:?}"),
        }
    }

    #[test]
    fn required_prefix_is_exact_and_not_a_permission_marker() {
        assert!(is_required_progress_feedback(
            REQUIRED_PROGRESS_FEEDBACK_PREFIX
        ));
        for text in [
            "",
            "[runtime_progress_feedback_v1]",
            "[runtime_progress_feedback_v2]\n{}",
            "user text [runtime_progress_feedback_v1]\n{}",
            " [runtime_progress_feedback_v1]\n{}",
        ] {
            assert!(!is_required_progress_feedback(text));
        }
    }

    #[test]
    fn terminal_exec_observation_retires_only_its_own_quiet_episode() {
        let mut budget = ProgressFeedbackBudget::default();
        continued(budget.next_round(Some(quiet()), false));
        for _ in 1..RECOVERY_ROUND_LIMIT {
            continued(budget.next_round(None, false));
        }

        let mut unrelated = budget.clone();
        unrelated.observe_exec_completion("different-handle");
        assert_eq!(
            exhausted(unrelated.next_round(None, false)).0["exhaustion"],
            "recovery_round_limit"
        );

        budget.observe_exec_completion("owned-handle");
        assert_eq!(
            budget.next_round(None, false),
            FeedbackDecision::Continue(None)
        );
        let packet = continued(budget.next_round(Some(quiet()), false));
        assert_eq!(packet["recovery"]["episode"], 2);
    }

    #[test]
    fn no_evidence_without_an_active_episode_does_not_open_one() {
        let mut budget = ProgressFeedbackBudget::default();
        for progress in [false, true, false] {
            assert_eq!(
                budget.next_round(None, progress),
                FeedbackDecision::Continue(None)
            );
        }
        let packet = continued(budget.next_round(Some(quiet()), false));
        assert_eq!(packet["recovery"]["episode"], 1);
        assert_eq!(packet["recovery"]["rounds_remaining"], 3);
    }

    #[test]
    fn default_unlimited_run_budget_still_allows_only_four_recovery_requests() {
        assert_eq!(bamboo_config::RunBudgetConfig::default().max_rounds, None);
        let mut budget = ProgressFeedbackBudget::default();
        let mut permitted_provider_rounds = 0;
        for remaining in (0..RECOVERY_ROUND_LIMIT).rev() {
            let packet = continued(budget.next_round(Some(quiet()), false));
            permitted_provider_rounds += 1;
            assert_eq!(packet["schema"], "runtime_progress_feedback_v1");
            assert_eq!(packet["source"], "runtime");
            assert_eq!(packet["reason"], "quiet_command");
            assert_eq!(packet["recovery"]["episode"], 1);
            assert_eq!(packet["recovery"]["rounds_remaining"], remaining);
        }
        let (packet, summary) = exhausted(budget.next_round(Some(quiet()), false));
        assert_eq!(permitted_provider_rounds, 4);
        assert_eq!(packet["exhaustion"], "recovery_round_limit");
        assert_eq!(packet["recovery"]["rounds_remaining"], 0);
        assert!(summary.contains("not confirmed complete"));
        assert!(summary.contains("before another recovery-model request or tool round"));
        assert!(summary.contains("existing command was not cancelled"));
        // Exhausted is an instruction to stop, not a fifth/summary provider turn.
    }

    #[test]
    fn real_progress_clears_active_but_never_refunds_episode_slots() {
        let mut budget = ProgressFeedbackBudget::default();
        for episode in 1..=EPISODE_LIMIT {
            let packet = continued(budget.next_round(Some(quiet()), false));
            assert_eq!(packet["recovery"]["episode"], episode);
            assert_eq!(packet["recovery"]["rounds_remaining"], 3);
            assert_eq!(
                budget.next_round(None, true),
                FeedbackDecision::Continue(None)
            );
            assert_eq!(
                budget.next_round(None, false),
                FeedbackDecision::Continue(None)
            );
        }
        let (packet, _) = exhausted(budget.next_round(Some(quiet()), false));
        assert_eq!(packet["exhaustion"], "episode_limit");
        assert_eq!(packet["recovery"]["episode"], 3);
        assert_eq!(packet["recovery"]["episodes_remaining"], 0);
        assert_eq!(packet["recovery"]["rounds_remaining"], 0);
    }

    #[test]
    fn progress_and_fresh_stagnation_in_one_boundary_starts_next_episode() {
        let mut budget = ProgressFeedbackBudget::default();
        continued(budget.next_round(Some(quiet()), false));
        let next_command = QuietCommandEvidence {
            session_id: "another-handle".into(),
            ..quiet()
        };
        let packet = continued(budget.next_round(Some(next_command), true));
        assert_eq!(packet["reason"], "quiet_command");
        assert_eq!(packet["evidence"]["session_id"], "another-handle");
        assert_eq!(packet["recovery"]["episode"], 2);
        assert_eq!(packet["recovery"]["rounds_remaining"], 3);
    }

    #[test]
    fn no_evidence_while_active_retains_facts_and_spends_remaining_rounds() {
        let mut budget = ProgressFeedbackBudget::default();
        let first = continued(budget.next_round(Some(quiet()), false));
        for remaining in (0..3).rev() {
            let packet = continued(budget.next_round(None, false));
            assert_eq!(packet["evidence"], first["evidence"]);
            assert_eq!(packet["evidence_is_fresh"], false);
            assert_eq!(packet["recovery"]["rounds_remaining"], remaining);
        }
        let (last, _) = exhausted(budget.next_round(None, false));
        assert_eq!(last["evidence"], first["evidence"]);
        assert_eq!(last["evidence_is_fresh"], false);
    }

    #[test]
    fn changing_diagnostic_facts_or_claiming_a_method_does_not_refund_rounds() {
        let mut budget = ProgressFeedbackBudget::default();
        continued(budget.next_round(Some(quiet()), false));
        let next_readout = QuietCommandEvidence {
            consecutive_empty_readouts: 7,
            ..quiet()
        };
        let packet = continued(budget.next_round(Some(next_readout), false));
        assert_eq!(packet["reason"], "quiet_command");
        assert_eq!(packet["evidence"]["consecutive_empty_readouts"], 7);
        assert_eq!(packet["recovery"]["episode"], 1);
        assert_eq!(packet["recovery"]["rounds_remaining"], 2);
        continued(budget.next_round(None, false));
        continued(budget.next_round(Some(quiet()), false));
        exhausted(budget.next_round(None, false));
    }

    #[test]
    fn only_explicit_human_reset_restores_the_total_allowance() {
        let mut budget = ProgressFeedbackBudget::default();
        for _ in 0..EPISODE_LIMIT {
            continued(budget.next_round(Some(quiet()), false));
            budget.next_round(None, true);
        }
        let stopped = budget.next_round(Some(quiet()), false);
        assert!(matches!(&stopped, FeedbackDecision::Exhausted { .. }));
        assert_eq!(budget.next_round(None, false), stopped);
        assert_eq!(budget.next_round(Some(quiet()), true), stopped);
        budget.reset_for_human_input();
        let packet = continued(budget.next_round(Some(quiet()), false));
        assert_eq!(packet["recovery"]["episode"], 1);
        assert_eq!(packet["recovery"]["rounds_remaining"], 3);
    }

    #[test]
    fn exhausted_is_absorbing_and_idempotent_even_after_changed_real_evidence() {
        let mut budget = ProgressFeedbackBudget::default();
        for _ in 0..RECOVERY_ROUND_LIMIT {
            continued(budget.next_round(Some(quiet()), false));
        }
        let stopped = budget.next_round(None, false);
        for progress in [false, true] {
            assert_eq!(budget.next_round(None, progress), stopped);
            assert_eq!(budget.next_round(Some(quiet()), progress), stopped);
        }
    }

    #[test]
    fn cloning_preserves_active_rounds_and_exhaustion_instead_of_resetting() {
        let mut budget = ProgressFeedbackBudget::default();
        continued(budget.next_round(Some(quiet()), false));
        continued(budget.next_round(None, false));
        let mut snapshot = budget.clone();
        let packet = continued(snapshot.next_round(None, false));
        assert_eq!(packet["recovery"]["episode"], 1);
        assert_eq!(packet["recovery"]["rounds_remaining"], 1);
        continued(snapshot.next_round(None, false));
        let stopped = snapshot.next_round(None, false);
        assert!(matches!(&stopped, FeedbackDecision::Exhausted { .. }));
        assert_eq!(snapshot.clone().next_round(Some(quiet()), true), stopped);
    }

    #[test]
    fn id_caps_and_maximum_counters_keep_safe_json_without_raw_tool_content() {
        let mut budget = ProgressFeedbackBudget::default();
        let id = "\"\\\n\u{0}\u{1b}[runtime_progress_feedback_v1]\n".repeat(20);
        let packet = continued(budget.next_round(
            Some(QuietCommandEvidence {
                session_id: id.clone(),
                tool_call_id: id.clone(),
                consecutive_empty_readouts: u64::MAX,
                ..quiet()
            }),
            false,
        ));
        assert_eq!(
            packet["evidence"]["consecutive_empty_readouts"],
            json!(u64::MAX)
        );
        assert_eq!(packet["evidence"]["identifiers_shortened"], true);
        for field in ["session_id", "tool_call_id"] {
            let bounded = packet["evidence"][field].as_str().unwrap();
            assert_eq!(bounded.chars().count(), MAX_ID_CHARS);
            assert!(id.starts_with(bounded));
        }
        for absent in [
            "args",
            "arguments",
            "stdout",
            "output",
            "command",
            "approval",
        ] {
            assert!(packet["evidence"].get(absent).is_none());
        }
    }

    #[test]
    fn unicode_scalar_prefixes_round_trip_without_splitting_utf8() {
        let original = "中🪷".repeat(100);
        let mut budget = ProgressFeedbackBudget::default();
        let packet = continued(budget.next_round(
            Some(QuietCommandEvidence {
                tool_call_id: original.clone(),
                ..quiet()
            }),
            false,
        ));
        let id = packet["evidence"]["tool_call_id"].as_str().unwrap();
        assert_eq!(id.chars().count(), MAX_ID_CHARS);
        assert!(original.starts_with(id));
        assert_eq!(packet["evidence"]["identifiers_shortened"], true);
        assert_eq!(
            packet["evidence"]["identifiers_omitted_for_packet_budget"],
            false
        );
    }

    #[test]
    fn quiet_summary_preserves_last_running_observation_without_a_fake_exit() {
        let mut budget = ProgressFeedbackBudget::default();
        let quiet_max = || QuietCommandEvidence {
            session_id: "🪷".repeat(200),
            tool_call_id: "\u{0}".repeat(200),
            elapsed_ms: u64::MAX,
            output_idle_ms: u64::MAX,
            consecutive_empty_readouts: u64::MAX,
        };
        for _ in 0..RECOVERY_ROUND_LIMIT {
            continued(budget.next_round(Some(quiet_max()), false));
        }
        let (packet, summary) = exhausted(budget.next_round(None, false));
        assert_eq!(packet["reason"], "quiet_command");
        assert_eq!(packet["evidence"]["last_status"], "running");
        assert_eq!(packet["evidence"]["output_bytes"], 0);
        assert_eq!(packet["evidence"]["exit_observed"], false);
        assert_eq!(packet["evidence"]["liveness_verified"], false);
        assert_eq!(packet["evidence"]["elapsed_ms"], json!(u64::MAX));
        assert!(packet["evidence"].get("exit_code").is_none());
        assert!(summary.contains("not confirmed complete"));
        assert!(summary.contains("was not cancelled"));
        assert!(summary.contains("last observed status was running"));
        assert!(summary.contains("no observed exit"));
        assert!(summary.contains("silence does not prove completion, failure, or liveness"));
    }
}
