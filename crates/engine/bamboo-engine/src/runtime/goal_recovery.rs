//! Bounded recovery of silent transport timeouts while a goal owner is running.
use bamboo_agent_core::{AgentError, Session};
use serde::{Deserialize, Serialize};

const KEY: &str = "goal.recovery";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct GoalRecoveryPolicy {
    /// Additional retries across this goal, beyond the ordinary per-turn retries.
    /// Zero disables recovery. The runtime caps this at ten.
    pub max_attempts: u32,
    /// Elapsed recovery window from the first extra retry. Capped at one hour.
    pub max_elapsed_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecoveryState {
    goal_created_at: String,
    objective: String,
    attempts: u32,
    started_at_ms: i64,
    next_attempt_at_ms: i64,
}

/// Persist a retry reservation before waiting. Corrupt records fail closed;
/// changing a goal's identity starts a fresh budget, while a restart does not.
pub fn reserve_retry(
    session: &mut Session,
    policy: &GoalRecoveryPolicy,
    error: &AgentError,
    now_ms: i64,
) -> Option<u64> {
    if !matches!(error, AgentError::StreamTimeout(timeout) if timeout.retry_safe()) {
        return None;
    }
    let goal = super::goal_state::read_goal_state(session)?;
    if !goal.status.is_active() || goal.declared_status.is_some() || policy.max_attempts == 0 {
        return None;
    }
    let mut record = match session.metadata.get(KEY) {
        Some(raw) => serde_json::from_str::<RecoveryState>(raw).ok()?,
        None => RecoveryState {
            goal_created_at: goal.created_at.clone(),
            objective: goal.objective.clone(),
            attempts: 0,
            started_at_ms: now_ms,
            next_attempt_at_ms: now_ms,
        },
    };
    if record.goal_created_at != goal.created_at || record.objective != goal.objective {
        record = RecoveryState {
            goal_created_at: goal.created_at,
            objective: goal.objective,
            attempts: 0,
            started_at_ms: now_ms,
            next_attempt_at_ms: now_ms,
        };
    }
    let delay_ms = 5_000u64
        .saturating_mul(1u64 << record.attempts.min(6))
        .min(60_000);
    let deadline = record
        .started_at_ms
        .saturating_add(policy.max_elapsed_seconds.min(3600).saturating_mul(1000) as i64);
    if record.attempts >= policy.max_attempts.min(10)
        || now_ms < record.started_at_ms
        || now_ms.saturating_add(delay_ms as i64) >= deadline
    {
        return None;
    }
    record.attempts += 1;
    record.next_attempt_at_ms = now_ms.saturating_add(delay_ms as i64);
    session
        .metadata
        .insert(KEY.into(), serde_json::to_string(&record).ok()?);
    Some(delay_ms)
}

/// Remaining persisted backoff, used when the same goal run is resumed.
pub fn pending_delay(session: &Session, now_ms: i64) -> Option<u64> {
    let goal = super::goal_state::read_goal_state(session)?;
    let record: RecoveryState = serde_json::from_str(session.metadata.get(KEY)?).ok()?;
    if !goal.status.is_active()
        || goal.created_at != record.goal_created_at
        || goal.objective != record.objective
        || now_ms < record.started_at_ms
    {
        return None;
    }
    let remaining = record.next_attempt_at_ms.saturating_sub(now_ms);
    (remaining > 0 && remaining <= 60_000).then_some(remaining as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::goal_state::{ensure_goal_state, write_goal_state};
    use bamboo_agent_core::{StreamTimeoutError, StreamTimeoutPhase};
    use std::time::Duration;
    fn timeout() -> AgentError {
        AgentError::StreamTimeout(StreamTimeoutError::new(
            StreamTimeoutPhase::FirstSemantic,
            Duration::from_secs(30),
            None,
            None,
            Duration::ZERO,
            None,
            true,
        ))
    }
    #[test]
    fn retries_have_a_durable_bound_and_require_structured_safe_timeouts() {
        let mut session = Session::new("s", "m");
        let goal = ensure_goal_state(&mut session, "finish");
        write_goal_state(&mut session, goal);
        let policy = GoalRecoveryPolicy {
            max_attempts: 2,
            max_elapsed_seconds: 120,
        };
        assert!(
            reserve_retry(&mut session, &policy, &AgentError::LLM("timeout".into()), 0).is_none()
        );
        assert_eq!(
            reserve_retry(&mut session, &policy, &timeout(), 0),
            Some(5000)
        );
        let mut reopened: Session =
            serde_json::from_str(&serde_json::to_string(&session).unwrap()).unwrap();
        assert_eq!(
            reserve_retry(&mut reopened, &policy, &timeout(), 5000),
            Some(10000)
        );
        assert_eq!(
            reserve_retry(&mut reopened, &policy, &timeout(), 15000),
            None
        );
        let unsafe_error = AgentError::StreamTimeout(StreamTimeoutError::new(
            StreamTimeoutPhase::SemanticIdle,
            Duration::from_secs(30),
            None,
            None,
            Duration::ZERO,
            Some(Duration::ZERO),
            true,
        ));
        assert!(reserve_retry(&mut session, &policy, &unsafe_error, 0).is_none());
    }
    #[test]
    fn deadline_and_corrupt_state_fail_closed() {
        let mut session = Session::new("s", "m");
        let goal = ensure_goal_state(&mut session, "finish");
        write_goal_state(&mut session, goal);
        let policy = GoalRecoveryPolicy {
            max_attempts: 3,
            max_elapsed_seconds: 5,
        };
        assert_eq!(reserve_retry(&mut session, &policy, &timeout(), 0), None);
        session.metadata.insert(KEY.into(), "broken".into());
        assert_eq!(
            reserve_retry(
                &mut session,
                &GoalRecoveryPolicy {
                    max_elapsed_seconds: 120,
                    ..policy
                },
                &timeout(),
                0
            ),
            None
        );
    }
}
