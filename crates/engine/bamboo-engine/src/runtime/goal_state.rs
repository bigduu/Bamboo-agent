//! Durable per-session goal state for the Codex-style goal loop.
//!
//! Bamboo's goal feature is a hybrid of two ideas:
//!
//! 1. The **main agent self-reports** completion via the `update_goal` tool
//!    (mirroring OpenAI Codex's `update_goal`), and the runtime relentlessly
//!    re-injects a rigorous completion-audit continuation prompt until the agent
//!    declares the goal complete (or blocked, or the continuation budget runs
//!    out).
//! 2. The existing **Gold evaluator** is kept as a *side-channel double-check*:
//!    at the terminal point — when the run is actually about to stop — it
//!    re-verifies achievement and can veto a premature completion.
//!
//! This module owns the durable record that ties the two together. It lives in
//! `session.metadata` under [`GOAL_STATE_METADATA_KEY`] as a single JSON value
//! (the established Bamboo pattern for structured, session-scoped state — see
//! `gold_config` and the `gold.*` keys), so it round-trips through the normal
//! session save/load path with no new storage entity.
//!
//! Crucially, the double-check verdicts are persisted into
//! [`GoalState::eval_history`] so the goal record carries its own evaluation
//! trail ("goal 持久化也要加入评测内容").

use bamboo_agent_core::Session;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::runtime::gold_evaluation::GoldEvaluationResult;

/// Session metadata key holding the serialized [`GoalState`] JSON blob.
pub const GOAL_STATE_METADATA_KEY: &str = "goal.state";

/// Upper bound on retained evaluation records, so a long autonomous run cannot
/// grow the persisted blob without limit. The newest records are kept.
const MAX_EVAL_HISTORY: usize = 50;

/// Runtime status of the active session goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalRuntimeStatus {
    /// The agent is actively pursuing the goal; the loop will keep continuing.
    Active,
    /// The goal has been achieved (agent declared + double-check confirmed, or
    /// the evaluator was confidently achieved).
    Complete,
    /// The agent explicitly gave up after the blocked discipline, or the
    /// evaluator reported a concrete blocker.
    Blocked,
    /// The evaluator reported that user input is the true next blocker.
    NeedInput,
    /// The continuation budget was exhausted before completion.
    BudgetLimited,
}

impl GoalRuntimeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Complete => "complete",
            Self::Blocked => "blocked",
            Self::NeedInput => "need_input",
            Self::BudgetLimited => "budget_limited",
        }
    }

    /// Whether the loop should keep working on this goal.
    pub fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// What the agent last declared via the `update_goal` tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalDeclaredStatus {
    Complete,
    Blocked,
}

impl GoalDeclaredStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Blocked => "blocked",
        }
    }
}

/// A single persisted double-check verdict from the Gold evaluator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoalEvalRecord {
    pub checkpoint: String,
    pub iteration: u32,
    pub decision: String,
    pub confidence: String,
    pub reasoning: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_information: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_action: Option<String>,
    pub recorded_at: String,
}

impl GoalEvalRecord {
    /// Build a record from a Gold double-check verdict.
    pub fn from_evaluation(result: &GoldEvaluationResult) -> Self {
        Self {
            checkpoint: result.checkpoint.as_str().to_string(),
            iteration: result.iteration,
            decision: result.decision.as_str().to_string(),
            confidence: result.confidence.as_str().to_string(),
            reasoning: result.reasoning.clone(),
            missing_information: result.missing_information.clone(),
            next_action: result.next_action.clone(),
            recorded_at: Utc::now().to_rfc3339(),
        }
    }
}

/// Durable goal record persisted in `session.metadata`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoalState {
    /// The user's objective text (copied from the active goal config).
    pub objective: String,
    pub status: GoalRuntimeStatus,
    /// The agent's most recent self-report, if any. Cleared once acted upon.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_status: Option<GoalDeclaredStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_at_round: Option<u32>,
    /// How many autonomous continuations have fired toward this goal.
    #[serde(default)]
    pub continuation_count: u32,
    /// Persisted double-check verdicts (the "评测内容").
    #[serde(default)]
    pub eval_history: Vec<GoalEvalRecord>,
    pub created_at: String,
    pub updated_at: String,
}

impl GoalState {
    fn new(objective: impl Into<String>) -> Self {
        let now = Utc::now().to_rfc3339();
        Self {
            objective: objective.into(),
            status: GoalRuntimeStatus::Active,
            declared_status: None,
            declared_at_round: None,
            continuation_count: 0,
            eval_history: Vec::new(),
            created_at: now.clone(),
            updated_at: now,
        }
    }

    /// Record the agent's `update_goal` self-report.
    pub fn declare(&mut self, status: GoalDeclaredStatus, round: u32) {
        self.declared_status = Some(status);
        self.declared_at_round = Some(round);
    }

    /// Clear any pending self-report (after it has been acted upon).
    pub fn clear_declaration(&mut self) {
        self.declared_status = None;
        self.declared_at_round = None;
    }

    /// Append a double-check verdict, trimming to [`MAX_EVAL_HISTORY`].
    pub fn push_eval(&mut self, record: GoalEvalRecord) {
        self.eval_history.push(record);
        if self.eval_history.len() > MAX_EVAL_HISTORY {
            let overflow = self.eval_history.len() - MAX_EVAL_HISTORY;
            self.eval_history.drain(0..overflow);
        }
    }
}

/// Read the persisted goal state, if present and parseable.
pub fn read_goal_state(session: &Session) -> Option<GoalState> {
    let raw = session.metadata.get(GOAL_STATE_METADATA_KEY)?;
    serde_json::from_str::<GoalState>(raw).ok()
}

/// Persist the goal state into `session.metadata` (touching `updated_at`).
pub fn write_goal_state(session: &mut Session, mut state: GoalState) {
    state.updated_at = Utc::now().to_rfc3339();
    match serde_json::to_string(&state) {
        Ok(json) => {
            session
                .metadata
                .insert(GOAL_STATE_METADATA_KEY.to_string(), json);
        }
        Err(error) => {
            // Serializing a plain data struct effectively never fails, but if it
            // ever does, the on-disk goal state silently goes stale — log loudly
            // rather than swallow it.
            tracing::warn!(
                "failed to serialize goal state for session {}: {error}",
                session.id
            );
        }
    }
}

/// Read the existing goal state, or create a fresh one bound to `objective`.
///
/// When a state already exists but the objective changed (the user edited the
/// goal), the objective is refreshed and the declaration cleared so a stale
/// "complete" cannot leak across objectives.
pub fn ensure_goal_state(session: &Session, objective: &str) -> GoalState {
    match read_goal_state(session) {
        Some(mut state) => {
            if state.objective != objective {
                // A new objective is a fresh goal: reset the whole runtime, not
                // just the status — the continuation budget and eval trail
                // belonged to the previous objective.
                state.objective = objective.to_string();
                state.status = GoalRuntimeStatus::Active;
                state.continuation_count = 0;
                state.eval_history.clear();
                state.clear_declaration();
            }
            state
        }
        None => GoalState::new(objective),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Session;

    #[test]
    fn round_trips_through_metadata() {
        let mut session = Session::new("s1", "model");
        let mut state = GoalState::new("ship the feature");
        state.declare(GoalDeclaredStatus::Complete, 4);
        state.continuation_count = 2;
        state.push_eval(GoalEvalRecord {
            checkpoint: "terminal".to_string(),
            iteration: 4,
            decision: "continue".to_string(),
            confidence: "high".to_string(),
            reasoning: "still missing tests".to_string(),
            missing_information: vec!["the e2e test".to_string()],
            next_action: Some("write the e2e test".to_string()),
            recorded_at: "2026-06-15T00:00:00Z".to_string(),
        });

        write_goal_state(&mut session, state);
        let loaded = read_goal_state(&session).expect("state persists");

        assert_eq!(loaded.objective, "ship the feature");
        assert_eq!(loaded.declared_status, Some(GoalDeclaredStatus::Complete));
        assert_eq!(loaded.declared_at_round, Some(4));
        assert_eq!(loaded.continuation_count, 2);
        assert_eq!(loaded.eval_history.len(), 1);
        assert_eq!(
            loaded.eval_history[0].next_action.as_deref(),
            Some("write the e2e test")
        );
    }

    #[test]
    fn ensure_resets_when_objective_changes() {
        let mut session = Session::new("s1", "model");
        let mut state = GoalState::new("old objective");
        state.declare(GoalDeclaredStatus::Complete, 1);
        state.status = GoalRuntimeStatus::Complete;
        state.continuation_count = 2;
        state.push_eval(GoalEvalRecord {
            checkpoint: "terminal".to_string(),
            iteration: 1,
            decision: "achieved".to_string(),
            confidence: "high".to_string(),
            reasoning: "old".to_string(),
            missing_information: Vec::new(),
            next_action: None,
            recorded_at: "t".to_string(),
        });
        write_goal_state(&mut session, state);

        let refreshed = ensure_goal_state(&session, "new objective");
        assert_eq!(refreshed.objective, "new objective");
        assert_eq!(refreshed.status, GoalRuntimeStatus::Active);
        assert_eq!(refreshed.declared_status, None);
        // A new objective also resets the budget + eval trail of the old one.
        assert_eq!(refreshed.continuation_count, 0);
        assert!(refreshed.eval_history.is_empty());
    }

    #[test]
    fn push_eval_trims_history() {
        let mut state = GoalState::new("obj");
        for i in 0..(MAX_EVAL_HISTORY + 10) {
            state.push_eval(GoalEvalRecord {
                checkpoint: "terminal".to_string(),
                iteration: i as u32,
                decision: "continue".to_string(),
                confidence: "low".to_string(),
                reasoning: format!("round {i}"),
                missing_information: Vec::new(),
                next_action: None,
                recorded_at: "t".to_string(),
            });
        }
        assert_eq!(state.eval_history.len(), MAX_EVAL_HISTORY);
        // Oldest entries were dropped; the last one is the newest.
        assert_eq!(
            state.eval_history.last().unwrap().reasoning,
            format!("round {}", MAX_EVAL_HISTORY + 9)
        );
    }
}
