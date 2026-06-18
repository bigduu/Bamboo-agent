//! Durable per-session guardian-review state for the adversarial completion gate.
//!
//! The guardian feature is a sibling of the goal loop ([`crate::runtime::goal_state`]):
//! when the main agent would otherwise complete the run, the runtime can spawn a
//! read-only **guardian child** to adversarially review the final diff against
//! the active task's completion criteria before the run is allowed to stop. This
//! module owns the durable record that ties a single review pass together.
//!
//! Like `goal_state`, it lives in `session.metadata` under
//! [`GUARDIAN_STATE_METADATA_KEY`] as a single JSON value (the established
//! Bamboo pattern for structured, session-scoped state), so it round-trips
//! through the normal session save/load path with no new storage entity.
//!
//! The review budget mirrors the goal loop's `continuation_count`: a count-based
//! cap ([`GuardianState::review_count`] checked against a configured max via
//! [`GuardianState::budget_exhausted`]) so a pathological review → fix → review
//! cycle cannot run unbounded. The cap value itself lives with the caller (the
//! terminal gate), mirroring how `continuation_count` is enforced in `gold.rs`
//! rather than in `goal_state.rs`.

use std::collections::BTreeSet;

use bamboo_agent_core::Session;
use chrono::Utc;
use serde::{Deserialize, Serialize};

/// Session metadata key holding the serialized [`GuardianState`] JSON blob.
pub const GUARDIAN_STATE_METADATA_KEY: &str = "guardian.state";

/// Upper bound on retained findings on a single verdict, so a runaway reviewer
/// cannot grow the persisted blob without limit. The newest findings are kept.
const MAX_FINDINGS: usize = 50;

/// Lifecycle phase of the current guardian review pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardianPhase {
    /// No review is outstanding for the current terminal point.
    None,
    /// A guardian child has been spawned and a verdict is awaited.
    Pending,
    /// A verdict has been recorded for the current terminal point.
    Reviewed,
}

impl GuardianPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Pending => "pending",
            Self::Reviewed => "reviewed",
        }
    }

    /// Whether a guardian child is currently in flight.
    pub fn is_pending(self) -> bool {
        matches!(self, Self::Pending)
    }

    /// Whether a verdict has already been recorded for this pass.
    pub fn is_reviewed(self) -> bool {
        matches!(self, Self::Reviewed)
    }
}

/// A single guardian review verdict, parsed from the reviewer child's final
/// message (see the G5 verdict parser). The JSON shape the reviewer is asked to
/// emit matches this struct's serde representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardianVerdict {
    /// Whether the guardian approves stopping the run.
    pub approve: bool,
    /// The reviewer's short rationale, if provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Concrete findings the guardian wants addressed before approval.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<String>,
}

impl GuardianVerdict {
    /// Build an approving verdict with no findings.
    pub fn approved() -> Self {
        Self {
            approve: true,
            summary: None,
            findings: Vec::new(),
        }
    }

    /// Build a rejecting verdict carrying the given findings, trimmed to the
    /// newest [`MAX_FINDINGS`].
    pub fn rejected(findings: Vec<String>) -> Self {
        Self {
            approve: false,
            summary: None,
            findings: trim_findings(findings),
        }
    }

    /// Attach a reviewer summary (builder style).
    pub fn with_summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = Some(summary.into());
        self
    }

    /// Normalize a freshly-parsed verdict: clamp findings to [`MAX_FINDINGS`].
    pub fn normalized(mut self) -> Self {
        self.findings = trim_findings(std::mem::take(&mut self.findings));
        self
    }
}

/// Keep only the newest [`MAX_FINDINGS`] findings.
fn trim_findings(mut findings: Vec<String>) -> Vec<String> {
    if findings.len() > MAX_FINDINGS {
        let overflow = findings.len() - MAX_FINDINGS;
        findings.drain(0..overflow);
    }
    findings
}

/// Durable guardian record persisted in `session.metadata`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardianState {
    /// Lifecycle phase of the current review pass.
    pub phase: GuardianPhase,
    /// The session id of the in-flight (or most recent) guardian child, if any.
    /// Used by the completion coordinator to correlate a completing child with
    /// the review this parent actually dispatched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guardian_child_id: Option<String>,
    /// The most recent guardian verdict, if a review has completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_verdict: Option<GuardianVerdict>,
    /// The round at which the last verdict was recorded. Observability/diagnostics
    /// only — NOT currently read by the gate.
    ///
    /// KNOWN v1 SCOPE: the gate does not yet re-review work produced *after* an
    /// approval (e.g. during a Gold goal-continuation) or across top-level user
    /// turns, and the review budget is session-cumulative. A robust staleness
    /// re-review needs a cross-turn-monotonic "new work" signal (the per-run
    /// round counter resets each turn) plus a per-turn budget reset; deferred
    /// until the feature is enabled in production.
    #[serde(default)]
    pub last_reviewed_at_round: u32,
    /// How many guardian review passes have fired this session (the budget).
    #[serde(default)]
    pub review_count: u32,
    pub created_at: String,
    pub updated_at: String,
}

impl GuardianState {
    fn new() -> Self {
        let now = Utc::now().to_rfc3339();
        Self {
            phase: GuardianPhase::None,
            guardian_child_id: None,
            last_verdict: None,
            last_reviewed_at_round: 0,
            review_count: 0,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    /// Record that a guardian child has been spawned (move to `Pending`), and
    /// charge the review budget.
    pub fn record_spawn(&mut self, child_id: impl Into<String>) {
        self.guardian_child_id = Some(child_id.into());
        self.phase = GuardianPhase::Pending;
        self.review_count = self.review_count.saturating_add(1);
    }

    /// Record a completed guardian verdict (move to `Reviewed`).
    pub fn record_verdict(&mut self, verdict: GuardianVerdict, round: u32) {
        self.last_verdict = Some(verdict.normalized());
        self.last_reviewed_at_round = round;
        self.phase = GuardianPhase::Reviewed;
    }

    /// Clear the in-flight review pass (after it has been acted upon), keeping
    /// the budget count and the last verdict as the durable record.
    pub fn clear(&mut self) {
        self.phase = GuardianPhase::None;
        self.guardian_child_id = None;
    }

    /// Whether the review budget is spent, mirroring the goal loop's
    /// `continuation_count >= max_auto_continuations` gate.
    pub fn budget_exhausted(&self, max_reviews: u32) -> bool {
        self.review_count >= max_reviews
    }

    /// Whether the most recent verdict approved stopping the run.
    pub fn last_approved(&self) -> bool {
        self.last_verdict
            .as_ref()
            .is_some_and(|verdict| verdict.approve)
    }
}

/// Read the persisted guardian state, if present and parseable.
pub fn read_guardian_state(session: &Session) -> Option<GuardianState> {
    let raw = session.metadata.get(GUARDIAN_STATE_METADATA_KEY)?;
    serde_json::from_str::<GuardianState>(raw).ok()
}

/// Persist the guardian state into `session.metadata` (touching `updated_at`).
pub fn write_guardian_state(session: &mut Session, mut state: GuardianState) {
    state.updated_at = Utc::now().to_rfc3339();
    match serde_json::to_string(&state) {
        Ok(json) => {
            session
                .metadata
                .insert(GUARDIAN_STATE_METADATA_KEY.to_string(), json);
        }
        Err(error) => {
            // Serializing a plain data struct effectively never fails, but if it
            // ever does, the on-disk guardian state would silently go stale —
            // log loudly rather than swallow it (mirrors write_goal_state).
            tracing::warn!(
                "failed to serialize guardian state for session {}: {error}",
                session.id
            );
        }
    }
}

/// Read the existing guardian state, or create a fresh one.
pub fn ensure_guardian_state(session: &Session) -> GuardianState {
    read_guardian_state(session).unwrap_or_else(GuardianState::new)
}

/// Session metadata key holding the run's serialized [`GuardianConfig`].
///
/// The terminal gate persists the config here at first spawn so the resumed run
/// — driven by the completion coordinator, which has no original request — can
/// re-inject the guardian config and keep the review → fix → re-review loop
/// active across the suspend/resume boundary.
pub const GUARDIAN_CONFIG_METADATA_KEY: &str = "guardian.config";

/// Persist the run's [`crate::runtime::config::GuardianConfig`] into the session.
pub fn write_guardian_config(
    session: &mut Session,
    config: &crate::runtime::config::GuardianConfig,
) {
    if let Ok(json) = serde_json::to_string(config) {
        session
            .metadata
            .insert(GUARDIAN_CONFIG_METADATA_KEY.to_string(), json);
    }
}

/// Read the run's persisted [`crate::runtime::config::GuardianConfig`], if any.
pub fn read_guardian_config(session: &Session) -> Option<crate::runtime::config::GuardianConfig> {
    let raw = session.metadata.get(GUARDIAN_CONFIG_METADATA_KEY)?;
    serde_json::from_str(raw).ok()
}

/// The adversarial-review rubric handed to the guardian child as its task brief.
///
/// The terminal gate appends the run's concrete completion criteria and goal
/// after this template (see the guardian gate in the runner). The reviewer runs
/// as a real read-only sub-agent: it fetches the diff and runs tests *itself*
/// via its Bash/Read/Grep tools (so the engine never needs an in-process git),
/// and emits a single JSON verdict as its final message.
pub const GUARDIAN_REVIEW_RUBRIC: &str = r#"You are an adversarial code reviewer (Guardian). Another agent claims its task is complete. Independently VERIFY the work and decide whether the run may stop.

Verify, do not trust:
- Run `git diff` and `git status` in the workspace to see exactly what changed.
- Read the changed files and the surrounding code to judge correctness.
- If the task implies behavior, run the relevant tests or build (e.g. `cargo test`, `npm test`) and confirm they pass.
- Check every completion criterion below against real evidence, not the agent's claims.

Be skeptical. Flag real bugs, missed requirements, broken or skipped tests, and unmet criteria. You are READ-ONLY: do not modify files.

Emit your verdict as your FINAL message and ONLY as a single JSON object (no prose around it):
{"approve": <true|false>, "summary": "<one-line rationale>", "findings": ["<concrete issue>", "..."]}
Set approve=true ONLY if the work is correct and every criterion is met; otherwise approve=false with concrete, actionable findings."#;

/// The denylist of tool names disabled for a read-only guardian reviewer.
///
/// A DENYLIST matched by EXACT `ToolSchema.function.name` (see the worker's
/// `tool_schemas` retain). The reviewer keeps read/search/shell tools (Read,
/// Grep, Glob, GetFileInfo, Bash + its companions) so it can fetch the diff and
/// run tests, but loses every file-mutating, escalation, web, and interaction
/// tool. Names not registered in a given build are simply never matched, so the
/// list is safe to keep conservative and forward-looking.
pub fn guardian_read_only_disabled_tools() -> BTreeSet<String> {
    [
        // File mutation.
        "Edit",
        "Write",
        "NotebookEdit",
        "apply_patch",
        "MultiEdit",
        // Escalation / spawning / persistent side effects.
        "Task",
        "SubAgent",
        "DeployAgent",
        "AskAgent",
        "scheduler",
        "sub_session_manager",
        "session_note",
        "memory_note",
        // Plan-mode / interaction / permissions.
        "EnterPlanMode",
        "ExitPlanMode",
        "request_permissions",
        "conclusion_with_options",
        // Arbitrary execution surfaces beyond Bash, and web.
        "SlashCommand",
        "js_repl",
        "Workspace",
        "WebFetch",
        "WebSearch",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

/// Parse a [`GuardianVerdict`] from the reviewer child's final message.
///
/// No schema is enforced on the child's output, so the reviewer is asked (by the
/// rubric) to emit a single JSON object. This is tolerant: it accepts a bare
/// object, a ```json fenced block, or an object embedded in surrounding prose
/// (first `{` … last `}`). Returns the normalized verdict, or an error string
/// when no parseable JSON object is found.
pub fn parse_guardian_verdict(text: &str) -> Result<GuardianVerdict, String> {
    let trimmed = text.trim();
    if let Ok(verdict) = serde_json::from_str::<GuardianVerdict>(trimmed) {
        return Ok(verdict.normalized());
    }
    let unfenced = strip_code_fence(trimmed);
    if let Ok(verdict) = serde_json::from_str::<GuardianVerdict>(unfenced.trim()) {
        return Ok(verdict.normalized());
    }
    // Try each balanced top-level `{...}` object, preferring the LAST one (the
    // verdict normally follows any prose / examples / config snippets the
    // reviewer printed). A string-aware, brace-balanced scan avoids the
    // first-`{`-to-last-`}` over-capture that turned an embedded valid verdict
    // into a parse error (and thus a wrongful synthetic reject).
    for candidate in balanced_json_objects(unfenced).into_iter().rev() {
        if let Ok(verdict) = serde_json::from_str::<GuardianVerdict>(candidate) {
            return Ok(verdict.normalized());
        }
    }
    Err(format!(
        "no parseable guardian verdict JSON in reviewer output ({} chars)",
        trimmed.len()
    ))
}

/// Strip a single leading ```/```json fence and its trailing ``` if present.
fn strip_code_fence(text: &str) -> &str {
    let trimmed = text.trim();
    let Some(rest) = trimmed.strip_prefix("```") else {
        return trimmed;
    };
    // Drop the rest of the fence line (e.g. ```json) up to the first newline.
    let after_lang = rest.find('\n').map_or("", |idx| &rest[idx + 1..]);
    after_lang
        .trim_end()
        .strip_suffix("```")
        .unwrap_or(after_lang)
}

/// All top-level brace-balanced `{...}` spans in `text`, in source order.
///
/// String-aware (braces inside JSON string literals don't affect depth) and
/// nesting-aware, so a reviewer message like `config {a:1}; verdict {"approve":
/// true}` yields TWO candidates rather than the single over-wide first-`{`..
/// last-`}` slice that fails to parse.
fn balanced_json_objects(text: &str) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut objects = Vec::new();
    let mut depth = 0usize;
    let mut start: Option<usize> = None;
    let mut in_string = false;
    let mut escaped = false;
    for (i, &b) in bytes.iter().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' => {
                if depth == 0 {
                    start = Some(i);
                }
                depth += 1;
            }
            b'}' if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    if let Some(s) = start.take() {
                        objects.push(&text[s..=i]);
                    }
                }
            }
            _ => {}
        }
    }
    objects
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Session;

    #[test]
    fn round_trips_through_metadata() {
        let mut session = Session::new("s1", "model");
        let mut state = GuardianState::new();
        state.record_spawn("guardian-child-1");
        state.record_verdict(
            GuardianVerdict::rejected(vec!["missing test".to_string()]).with_summary("one bug"),
            7,
        );

        write_guardian_state(&mut session, state);
        let loaded = read_guardian_state(&session).expect("state persists");

        assert_eq!(loaded.phase, GuardianPhase::Reviewed);
        assert_eq!(
            loaded.guardian_child_id.as_deref(),
            Some("guardian-child-1")
        );
        assert_eq!(loaded.review_count, 1);
        assert_eq!(loaded.last_reviewed_at_round, 7);
        let verdict = loaded.last_verdict.expect("verdict persisted");
        assert!(!verdict.approve);
        assert_eq!(verdict.summary.as_deref(), Some("one bug"));
        assert_eq!(verdict.findings, vec!["missing test".to_string()]);
    }

    #[test]
    fn ensure_creates_fresh_when_absent() {
        let session = Session::new("s1", "model");
        let state = ensure_guardian_state(&session);
        assert_eq!(state.phase, GuardianPhase::None);
        assert_eq!(state.review_count, 0);
        assert!(state.guardian_child_id.is_none());
    }

    #[test]
    fn budget_gate_mirrors_continuation_count() {
        let mut state = GuardianState::new();
        assert!(!state.budget_exhausted(2));
        state.record_spawn("c1"); // review_count = 1
        assert!(!state.budget_exhausted(2));
        state.clear();
        state.record_spawn("c2"); // review_count = 2
        assert!(state.budget_exhausted(2));
    }

    #[test]
    fn clear_keeps_budget_and_verdict() {
        let mut state = GuardianState::new();
        state.record_spawn("c1");
        state.record_verdict(GuardianVerdict::approved(), 3);
        state.clear();
        assert_eq!(state.phase, GuardianPhase::None);
        assert!(state.guardian_child_id.is_none());
        // Budget + last verdict survive the clear (they are the record).
        assert_eq!(state.review_count, 1);
        assert!(state.last_approved());
    }

    #[test]
    fn rejected_trims_findings_to_newest() {
        let findings: Vec<String> = (0..(MAX_FINDINGS + 10)).map(|i| format!("f{i}")).collect();
        let verdict = GuardianVerdict::rejected(findings);
        assert_eq!(verdict.findings.len(), MAX_FINDINGS);
        // Oldest dropped; newest kept.
        assert_eq!(
            verdict.findings.last().unwrap(),
            &format!("f{}", MAX_FINDINGS + 9)
        );
    }

    #[test]
    fn missing_optional_fields_parse() {
        // The reviewer may emit a minimal verdict; summary/findings default.
        let verdict: GuardianVerdict =
            serde_json::from_str(r#"{"approve": true}"#).expect("minimal verdict parses");
        assert!(verdict.approve);
        assert!(verdict.summary.is_none());
        assert!(verdict.findings.is_empty());
    }

    #[test]
    fn parse_verdict_bare_object() {
        let v =
            parse_guardian_verdict(r#"{"approve": false, "summary": "bug", "findings": ["x"]}"#)
                .expect("parses");
        assert!(!v.approve);
        assert_eq!(v.summary.as_deref(), Some("bug"));
        assert_eq!(v.findings, vec!["x".to_string()]);
    }

    #[test]
    fn parse_verdict_fenced_and_embedded() {
        let fenced = "```json\n{\"approve\": true}\n```";
        assert!(
            parse_guardian_verdict(fenced)
                .expect("fenced parses")
                .approve
        );
        let embedded =
            "Here is my verdict:\n{\"approve\": false, \"findings\": [\"nope\"]}\nThanks.";
        let v = parse_guardian_verdict(embedded).expect("embedded parses");
        assert!(!v.approve);
        assert_eq!(v.findings, vec!["nope".to_string()]);
    }

    #[test]
    fn parse_verdict_rejects_garbage() {
        assert!(parse_guardian_verdict("no json here at all").is_err());
    }

    #[test]
    fn parse_verdict_picks_trailing_object_after_prose_braces() {
        // A reviewer that prints a config/example object before the real verdict
        // must not be mis-parsed into a synthetic reject (the old first-{..last-}
        // slice failed here).
        let text = "I inspected config {timeout: 30} then ran the suite.\n\
                    Verdict: {\"approve\": true, \"summary\": \"ok\"}";
        let v = parse_guardian_verdict(text).expect("parses the trailing verdict");
        assert!(v.approve);
        assert_eq!(v.summary.as_deref(), Some("ok"));
    }

    #[test]
    fn parse_verdict_is_string_aware_for_braces_in_findings() {
        // Braces inside a JSON string must not break brace-balancing, and the
        // object must still be found when embedded in prose.
        let text = "note: {\"approve\": false, \"findings\": [\"foo() { x }\"]}";
        let v = parse_guardian_verdict(text).expect("parses despite braces in the string");
        assert!(!v.approve);
        assert_eq!(v.findings, vec!["foo() { x }".to_string()]);
    }

    #[test]
    fn read_only_denylist_blocks_mutation_keeps_read() {
        let denied = guardian_read_only_disabled_tools();
        for tool in ["Edit", "Write", "SubAgent", "WebFetch", "Task", "js_repl"] {
            assert!(denied.contains(tool), "{tool} should be denied");
        }
        for tool in ["Read", "Grep", "Bash", "Glob", "GetFileInfo"] {
            assert!(!denied.contains(tool), "{tool} should remain allowed");
        }
    }
}
