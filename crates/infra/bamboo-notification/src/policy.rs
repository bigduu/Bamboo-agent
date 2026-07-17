//! Pure classification of agent events into candidate notifications.
//!
//! [`classify`] is the single source of truth for *which* events are
//! notification-worthy, *how* they are categorised and prioritised, and *what*
//! their dedup key is. It is intentionally side-effect free: it reads an
//! [`AgentEvent`] and the user's [`NotificationPreferences`] and returns an
//! optional [`ClassifiedNotification`]. Dedup timing and id/timestamp minting
//! happen one layer up in [`crate::service`].

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use bamboo_agent_core::AgentEvent;

use crate::preferences::NotificationPreferences;

/// Maximum body length for a clarification notification, in characters.
const CLARIFICATION_BODY_MAX: usize = 120;

/// Maximum body length for a background-task-completed notification, in characters.
const BACKGROUND_TASK_BODY_MAX: usize = 120;

/// Maximum body length for a run-failed notification, in characters.
const RUN_FAILED_BODY_MAX: usize = 200;

/// The kind of user-facing notification a classified event maps to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationCategory {
    /// A tool is requesting explicit user approval before running.
    NeedsApproval,
    /// The agent is asking the user a free-form clarifying question.
    NeedsClarification,
    /// Context usage has reached a critical level.
    ContextCritical,
    /// A background sub-agent task has completed.
    SubagentCompleted,
    /// A background shell/command (Bash `run_in_background`) has finished.
    BackgroundTaskCompleted,
    /// A run finished successfully (`AgentEvent::Complete`).
    RunCompleted,
    /// A run failed (`AgentEvent::Error`).
    RunFailed,
    /// A per-run token/tool-call/subagent budget tripped and the run was
    /// gracefully stopped (`AgentEvent::BudgetExceeded`, issue #221).
    BudgetExceeded,
    /// A caller-supplied notification minted directly by the `notify` tool
    /// rather than derived from a specific `AgentEvent` variant.
    Custom,
}

impl NotificationCategory {
    /// Returns the stable wire string for this category.
    pub fn as_str(&self) -> &'static str {
        match self {
            NotificationCategory::NeedsApproval => "needs_approval",
            NotificationCategory::NeedsClarification => "needs_clarification",
            NotificationCategory::ContextCritical => "context_critical",
            NotificationCategory::SubagentCompleted => "subagent_completed",
            NotificationCategory::BackgroundTaskCompleted => "background_task_completed",
            NotificationCategory::RunCompleted => "run_completed",
            NotificationCategory::RunFailed => "run_failed",
            NotificationCategory::BudgetExceeded => "budget_exceeded",
            NotificationCategory::Custom => "custom",
        }
    }
}

/// The urgency of a notification, surfaced to the client for sorting/styling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationPriority {
    /// Needs prompt user attention (e.g. the agent is blocked on input).
    High,
    /// Informational but worth surfacing.
    Normal,
    /// Low-signal; can be quietly delivered.
    Low,
}

impl NotificationPriority {
    /// Returns the stable wire string for this priority.
    pub fn as_str(&self) -> &'static str {
        match self {
            NotificationPriority::High => "high",
            NotificationPriority::Normal => "normal",
            NotificationPriority::Low => "low",
        }
    }
}

/// A notification candidate produced by [`classify`].
///
/// This carries everything the policy decided; the service layer adds the
/// volatile bits (unique id, creation timestamp) when materialising the
/// outgoing [`AgentEvent::Notification`].
#[derive(Debug, Clone, PartialEq)]
pub struct ClassifiedNotification {
    /// Category the event was classified into.
    pub category: NotificationCategory,
    /// Priority assigned to the notification.
    pub priority: NotificationPriority,
    /// Short title line.
    pub title: String,
    /// Body text.
    pub body: String,
    /// Stable key used to coalesce duplicate notifications within a window.
    pub dedup_key: String,
}

/// Returns `true` when a clarification question is actually a permission prompt.
///
/// Permission prompts are produced by the permission gate / `request_permissions`
/// tool and carry one of these markers in their question text; we route them to
/// the `NeedsApproval` category rather than `NeedsClarification`.
fn is_permission_prompt(question: &str) -> bool {
    question.contains("Permission required") || question.contains("Permission Request")
}

/// Truncates `text` to at most `max` characters, appending an ellipsis when cut.
///
/// Operates on Unicode scalar values (chars), not bytes, so it never splits a
/// multi-byte character.
fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max).collect();
    out.push('…');
    out
}

/// Classifies an agent event into an optional notification candidate.
///
/// Returns `None` immediately when notifications are disabled, when the event is
/// not notification-worthy, or when the specific per-category preference is off.
/// The match uses a catch-all arm, so adding new [`AgentEvent`] variants never
/// breaks this function.
pub fn classify(
    session_id: &str,
    event: &AgentEvent,
    prefs: &NotificationPreferences,
) -> Option<ClassifiedNotification> {
    if !prefs.enabled {
        return None;
    }

    match event {
        AgentEvent::NeedClarification {
            question,
            tool_call_id,
            ..
        } => {
            let (category, title) = if is_permission_prompt(question) {
                if !prefs.on_tool_approval {
                    return None;
                }
                (NotificationCategory::NeedsApproval, "Approval needed")
            } else {
                if !prefs.on_clarification {
                    return None;
                }
                (
                    NotificationCategory::NeedsClarification,
                    "Your input needed",
                )
            };
            Some(ClassifiedNotification {
                category,
                priority: NotificationPriority::High,
                title: title.to_string(),
                body: truncate(question, CLARIFICATION_BODY_MAX),
                dedup_key: format!(
                    "clarification:{session_id}:{}",
                    tool_call_id.as_deref().unwrap_or("")
                ),
            })
        }

        AgentEvent::ContextPressureNotification { level, message, .. } => {
            if level != "critical" {
                return None;
            }
            if !prefs.on_context_pressure {
                return None;
            }
            Some(ClassifiedNotification {
                category: NotificationCategory::ContextCritical,
                priority: NotificationPriority::Normal,
                title: "Context almost full".to_string(),
                body: message.clone(),
                dedup_key: format!("context:{session_id}"),
            })
        }

        AgentEvent::SubAgentCompleted {
            child_session_id,
            status,
            ..
        } => {
            if status != "completed" {
                return None;
            }
            if !prefs.on_subagent_complete {
                return None;
            }
            Some(ClassifiedNotification {
                category: NotificationCategory::SubagentCompleted,
                priority: NotificationPriority::Normal,
                title: "Background task done".to_string(),
                body: format!("A background task finished ({}).", child_session_id),
                dedup_key: format!("subagent:{}", child_session_id),
            })
        }

        AgentEvent::BashCompleted {
            bash_id,
            command,
            exit_code,
            status,
        } => {
            // A killed shell was terminated by the user (KillShell) — they already
            // know; don't ping. Notify when a background command reaches a terminal
            // state on its own ("completed" — any exit code — or an internal
            // "error").
            if status == "killed" {
                return None;
            }
            if !prefs.on_background_task_complete {
                return None;
            }
            let exit = match exit_code {
                Some(code) => format!("exit {code}"),
                None => "no exit code".to_string(),
            };
            Some(ClassifiedNotification {
                category: NotificationCategory::BackgroundTaskCompleted,
                priority: NotificationPriority::Normal,
                title: "Background command finished".to_string(),
                // Dedup by the shell id: the live `BashCompleted` fires once, but
                // key on `bash_id` (not session) so distinct shells each notify.
                body: truncate(
                    &format!("{command} — {status}, {exit}"),
                    BACKGROUND_TASK_BODY_MAX,
                ),
                dedup_key: format!("bash:{bash_id}"),
            })
        }

        AgentEvent::Complete { usage } => {
            if !prefs.on_run_complete {
                return None;
            }
            Some(ClassifiedNotification {
                category: NotificationCategory::RunCompleted,
                priority: NotificationPriority::Normal,
                title: "Run complete".to_string(),
                body: format!(
                    "Session {session_id} finished ({} tokens).",
                    usage.total_tokens
                ),
                dedup_key: format!("run:{session_id}:done"),
            })
        }

        AgentEvent::Error { message } => {
            if !prefs.on_run_failed {
                return None;
            }
            Some(ClassifiedNotification {
                category: NotificationCategory::RunFailed,
                priority: NotificationPriority::High,
                title: "Run failed".to_string(),
                body: truncate(message, RUN_FAILED_BODY_MAX),
                dedup_key: format!("run:{session_id}:failed"),
            })
        }

        AgentEvent::BudgetExceeded {
            kind,
            limit,
            actual,
            ..
        } => {
            // No dedicated preference for this category yet; gated on
            // `on_run_failed` since a budget-exceeded stop is, like a failure,
            // an abnormal (non-`Complete`) run termination the user should
            // learn about promptly.
            if !prefs.on_run_failed {
                return None;
            }
            Some(ClassifiedNotification {
                category: NotificationCategory::BudgetExceeded,
                priority: NotificationPriority::High,
                title: "Run stopped: budget exceeded".to_string(),
                body: truncate(
                    &format!("{kind} reached {actual}/{limit}; the run was stopped."),
                    RUN_FAILED_BODY_MAX,
                ),
                dedup_key: format!("run:{session_id}:budget_exceeded"),
            })
        }

        _ => None,
    }
}

/// Classifies a caller-supplied notification (from the `notify` tool) into a
/// candidate notification.
///
/// Unlike [`classify`], this does not gate on a per-category preference —
/// there is no dedicated preference for arbitrary agent-chosen content, only
/// the master [`NotificationPreferences::enabled`] switch applies here. The
/// dedup key hashes `title` + `body` so an identical repeated call within the
/// dedup window is coalesced while distinct content from the same session
/// still notifies. Kept in `policy` (not `service`) to stay pure and
/// unit-testable without a service instance.
pub fn classify_custom(
    session_id: &str,
    title: &str,
    body: &str,
    priority: NotificationPriority,
    prefs: &NotificationPreferences,
) -> Option<ClassifiedNotification> {
    if !prefs.enabled {
        return None;
    }
    let mut hasher = DefaultHasher::new();
    title.hash(&mut hasher);
    body.hash(&mut hasher);
    let digest = hasher.finish();
    Some(ClassifiedNotification {
        category: NotificationCategory::Custom,
        priority,
        title: title.to_string(),
        body: body.to_string(),
        dedup_key: format!("custom:{session_id}:{digest:x}"),
    })
}

/// Classifies a schedule-run completion/failure into a notification.
///
/// Unlike [`classify`], `title`/`body` are caller-supplied rather than
/// derived from a raw `AgentEvent` — the schedule manager can name the
/// schedule and quote the run's final assistant message, which the generic
/// `AgentEvent::Complete`/`Error` path has no way to know. Everything else
/// (category, priority, prefs gate, and — critically — the dedup key) is
/// deliberately IDENTICAL to what [`classify`] derives for
/// `AgentEvent::Complete`/`Error`: `"run:{session_id}:done"` /
/// `"run:{session_id}:failed"`. A scheduled run's agent loop still emits that
/// raw event, which the always-on notification relay independently
/// classifies via [`classify`] — sharing the dedup key means whichever of the
/// two sources reaches [`crate::service::NotificationService`] first wins the
/// user-visible copy and the second is coalesced within
/// [`crate::service::NotificationService::DEDUP_WINDOW`], so a scheduled run
/// can never double-notify its owner. See
/// `bamboo_server::schedule_app::manager`'s completion hook for the call
/// site.
pub fn classify_schedule_run(
    session_id: &str,
    success: bool,
    title: String,
    body: String,
    prefs: &NotificationPreferences,
) -> Option<ClassifiedNotification> {
    if !prefs.enabled {
        return None;
    }
    if success {
        if !prefs.on_run_complete {
            return None;
        }
        Some(ClassifiedNotification {
            category: NotificationCategory::RunCompleted,
            priority: NotificationPriority::Normal,
            title,
            body,
            dedup_key: format!("run:{session_id}:done"),
        })
    } else {
        if !prefs.on_run_failed {
            return None;
        }
        Some(ClassifiedNotification {
            category: NotificationCategory::RunFailed,
            priority: NotificationPriority::High,
            title,
            body,
            dedup_key: format!("run:{session_id}:failed"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clarification(question: &str, tool_call_id: Option<&str>) -> AgentEvent {
        AgentEvent::NeedClarification {
            question: question.to_string(),
            options: None,
            tool_call_id: tool_call_id.map(str::to_string),
            tool_name: None,
            allow_custom: true,
        }
    }

    fn context(level: &str) -> AgentEvent {
        AgentEvent::ContextPressureNotification {
            percent: 95.0,
            level: level.to_string(),
            message: "Context is nearly full.".to_string(),
        }
    }

    fn subagent(status: &str) -> AgentEvent {
        AgentEvent::SubAgentCompleted {
            parent_session_id: "parent".to_string(),
            child_session_id: "child-1".to_string(),
            status: status.to_string(),
            error: None,
        }
    }

    fn bash(bash_id: &str, status: &str, exit_code: Option<i32>) -> AgentEvent {
        AgentEvent::BashCompleted {
            bash_id: bash_id.to_string(),
            command: "npm run build".to_string(),
            exit_code,
            status: status.to_string(),
        }
    }

    fn complete(total_tokens: u64) -> AgentEvent {
        AgentEvent::Complete {
            usage: bamboo_agent_core::TokenUsage {
                prompt_tokens: total_tokens / 2,
                completion_tokens: total_tokens - total_tokens / 2,
                total_tokens,
            },
        }
    }

    fn error(message: &str) -> AgentEvent {
        AgentEvent::Error {
            message: message.to_string(),
        }
    }

    #[test]
    fn plain_clarification_is_needs_clarification() {
        let prefs = NotificationPreferences::default();
        let event = clarification("Which file should I edit?", Some("tc-1"));
        let result = classify("sess", &event, &prefs).unwrap();
        assert_eq!(result.category, NotificationCategory::NeedsClarification);
        assert_eq!(result.priority, NotificationPriority::High);
        assert_eq!(result.title, "Your input needed");
        assert_eq!(result.dedup_key, "clarification:sess:tc-1");
    }

    #[test]
    fn permission_prompt_is_needs_approval() {
        let prefs = NotificationPreferences::default();
        let event = clarification("**Permission required** to run `rm -rf`", Some("tc-2"));
        let result = classify("sess", &event, &prefs).unwrap();
        assert_eq!(result.category, NotificationCategory::NeedsApproval);
        assert_eq!(result.title, "Approval needed");
    }

    #[test]
    fn permission_request_marker_also_matches() {
        let prefs = NotificationPreferences::default();
        let event = clarification("Permission Request: write to disk", None);
        let result = classify("sess", &event, &prefs).unwrap();
        assert_eq!(result.category, NotificationCategory::NeedsApproval);
        // No tool_call_id → empty trailing segment.
        assert_eq!(result.dedup_key, "clarification:sess:");
    }

    #[test]
    fn clarification_body_truncates_with_ellipsis() {
        let prefs = NotificationPreferences::default();
        let long = "x".repeat(200);
        let event = clarification(&long, None);
        let result = classify("sess", &event, &prefs).unwrap();
        assert_eq!(result.body.chars().count(), CLARIFICATION_BODY_MAX + 1);
        assert!(result.body.ends_with('…'));
    }

    #[test]
    fn clarification_gated_by_on_clarification() {
        let prefs = NotificationPreferences {
            on_clarification: false,
            ..NotificationPreferences::default()
        };
        let event = clarification("plain question", None);
        assert!(classify("sess", &event, &prefs).is_none());
    }

    #[test]
    fn approval_gated_by_on_tool_approval() {
        let prefs = NotificationPreferences {
            on_tool_approval: false,
            ..NotificationPreferences::default()
        };
        let event = clarification("Permission required: do thing", None);
        assert!(classify("sess", &event, &prefs).is_none());
    }

    #[test]
    fn context_critical_fires() {
        let prefs = NotificationPreferences::default();
        let result = classify("sess", &context("critical"), &prefs).unwrap();
        assert_eq!(result.category, NotificationCategory::ContextCritical);
        assert_eq!(result.priority, NotificationPriority::Normal);
        assert_eq!(result.title, "Context almost full");
        assert_eq!(result.body, "Context is nearly full.");
        assert_eq!(result.dedup_key, "context:sess");
    }

    #[test]
    fn context_warning_is_none() {
        let prefs = NotificationPreferences::default();
        assert!(classify("sess", &context("warning"), &prefs).is_none());
    }

    #[test]
    fn context_gated_by_on_context_pressure() {
        let prefs = NotificationPreferences {
            on_context_pressure: false,
            ..NotificationPreferences::default()
        };
        assert!(classify("sess", &context("critical"), &prefs).is_none());
    }

    #[test]
    fn subagent_completed_fires() {
        let prefs = NotificationPreferences::default();
        let result = classify("sess", &subagent("completed"), &prefs).unwrap();
        assert_eq!(result.category, NotificationCategory::SubagentCompleted);
        assert_eq!(result.priority, NotificationPriority::Normal);
        assert_eq!(result.title, "Background task done");
        assert_eq!(result.body, "A background task finished (child-1).");
        assert_eq!(result.dedup_key, "subagent:child-1");
    }

    #[test]
    fn subagent_error_is_none() {
        let prefs = NotificationPreferences::default();
        assert!(classify("sess", &subagent("error"), &prefs).is_none());
    }

    #[test]
    fn subagent_gated_by_on_subagent_complete() {
        let prefs = NotificationPreferences {
            on_subagent_complete: false,
            ..NotificationPreferences::default()
        };
        assert!(classify("sess", &subagent("completed"), &prefs).is_none());
    }

    #[test]
    fn bash_completed_fires_and_dedups_by_bash_id() {
        let prefs = NotificationPreferences::default();
        let result = classify("sess", &bash("sh-9", "completed", Some(0)), &prefs).unwrap();
        assert_eq!(
            result.category,
            NotificationCategory::BackgroundTaskCompleted
        );
        assert_eq!(result.priority, NotificationPriority::Normal);
        assert_eq!(result.title, "Background command finished");
        assert!(result.body.contains("npm run build"));
        assert!(result.body.contains("exit 0"));
        // Keyed on bash_id (not session), so distinct shells each notify.
        assert_eq!(result.dedup_key, "bash:sh-9");
    }

    #[test]
    fn bash_error_status_still_fires() {
        let prefs = NotificationPreferences::default();
        let result = classify("sess", &bash("sh-e", "error", None), &prefs).unwrap();
        assert_eq!(
            result.category,
            NotificationCategory::BackgroundTaskCompleted
        );
        assert!(result.body.contains("no exit code"));
    }

    #[test]
    fn bash_killed_is_none() {
        // A user-killed shell must not ping (the user initiated the kill).
        let prefs = NotificationPreferences::default();
        assert!(classify("sess", &bash("sh-k", "killed", None), &prefs).is_none());
    }

    #[test]
    fn bash_gated_by_on_background_task_complete() {
        let prefs = NotificationPreferences {
            on_background_task_complete: false,
            ..NotificationPreferences::default()
        };
        assert!(classify("sess", &bash("sh-1", "completed", Some(0)), &prefs).is_none());
    }

    #[test]
    fn master_switch_off_yields_none() {
        let prefs = NotificationPreferences {
            enabled: false,
            ..NotificationPreferences::default()
        };
        assert!(classify("sess", &context("critical"), &prefs).is_none());
        assert!(classify("sess", &subagent("completed"), &prefs).is_none());
        assert!(classify("sess", &clarification("q", None), &prefs).is_none());
        assert!(classify("sess", &bash("sh-1", "completed", Some(0)), &prefs).is_none());
        assert!(classify("sess", &complete(100), &prefs).is_none());
        assert!(classify("sess", &error("boom"), &prefs).is_none());
        assert!(classify_custom("sess", "t", "b", NotificationPriority::Low, &prefs).is_none());
    }

    #[test]
    fn unrelated_variant_is_none() {
        let prefs = NotificationPreferences::default();
        let event = AgentEvent::Token {
            content: "hello".to_string(),
        };
        assert!(classify("sess", &event, &prefs).is_none());
    }

    #[test]
    fn run_completed_fires_with_token_usage_in_body() {
        let prefs = NotificationPreferences::default();
        let result = classify("sess-1", &complete(1234), &prefs).unwrap();
        assert_eq!(result.category, NotificationCategory::RunCompleted);
        assert_eq!(result.priority, NotificationPriority::Normal);
        assert_eq!(result.title, "Run complete");
        assert!(result.body.contains("sess-1"));
        assert!(result.body.contains("1234"));
        assert_eq!(result.dedup_key, "run:sess-1:done");
    }

    #[test]
    fn run_completed_gated_by_on_run_complete() {
        let prefs = NotificationPreferences {
            on_run_complete: false,
            ..NotificationPreferences::default()
        };
        assert!(classify("sess", &complete(10), &prefs).is_none());
    }

    #[test]
    fn run_failed_fires_with_truncated_message() {
        let prefs = NotificationPreferences::default();
        let long_message = "x".repeat(300);
        let result = classify("sess-2", &error(&long_message), &prefs).unwrap();
        assert_eq!(result.category, NotificationCategory::RunFailed);
        assert_eq!(result.priority, NotificationPriority::High);
        assert_eq!(result.title, "Run failed");
        assert_eq!(result.body.chars().count(), RUN_FAILED_BODY_MAX + 1);
        assert!(result.body.ends_with('…'));
        assert_eq!(result.dedup_key, "run:sess-2:failed");
    }

    #[test]
    fn run_failed_gated_by_on_run_failed() {
        let prefs = NotificationPreferences {
            on_run_failed: false,
            ..NotificationPreferences::default()
        };
        assert!(classify("sess", &error("boom"), &prefs).is_none());
    }

    #[test]
    fn classify_custom_mints_custom_category_with_hashed_dedup_key() {
        let prefs = NotificationPreferences::default();
        let result =
            classify_custom("sess-3", "Title", "Body", NotificationPriority::Low, &prefs).unwrap();
        assert_eq!(result.category, NotificationCategory::Custom);
        assert_eq!(result.priority, NotificationPriority::Low);
        assert_eq!(result.title, "Title");
        assert_eq!(result.body, "Body");
        assert!(result.dedup_key.starts_with("custom:sess-3:"));
    }

    #[test]
    fn classify_custom_dedup_key_is_stable_for_identical_content() {
        let prefs = NotificationPreferences::default();
        let a =
            classify_custom("sess", "Title", "Body", NotificationPriority::Low, &prefs).unwrap();
        let b =
            classify_custom("sess", "Title", "Body", NotificationPriority::Low, &prefs).unwrap();
        assert_eq!(a.dedup_key, b.dedup_key);
    }

    #[test]
    fn classify_custom_dedup_key_differs_for_different_content() {
        let prefs = NotificationPreferences::default();
        let a =
            classify_custom("sess", "Title", "Body", NotificationPriority::Low, &prefs).unwrap();
        let b = classify_custom(
            "sess",
            "Title",
            "Different body",
            NotificationPriority::Low,
            &prefs,
        )
        .unwrap();
        assert_ne!(a.dedup_key, b.dedup_key);
    }

    #[test]
    fn classify_custom_ignores_per_category_prefs() {
        // classify_custom must not be gated by any per-category flag — only
        // the master `enabled` switch applies.
        let prefs = NotificationPreferences {
            on_clarification: false,
            on_tool_approval: false,
            on_context_pressure: false,
            on_subagent_complete: false,
            on_background_task_complete: false,
            on_run_complete: false,
            on_run_failed: false,
            ..NotificationPreferences::default()
        };
        assert!(classify_custom("sess", "t", "b", NotificationPriority::Low, &prefs).is_some());
    }

    #[test]
    fn classify_schedule_run_success_shares_dedup_key_with_classify_complete() {
        let prefs = NotificationPreferences::default();
        let scheduled = classify_schedule_run(
            "sess-1",
            true,
            "Schedule 'nightly' completed".to_string(),
            "All done.".to_string(),
            &prefs,
        )
        .unwrap();
        let raw = classify("sess-1", &complete(10), &prefs).unwrap();
        assert_eq!(scheduled.dedup_key, raw.dedup_key);
        assert_eq!(scheduled.category, NotificationCategory::RunCompleted);
        assert_eq!(scheduled.priority, NotificationPriority::Normal);
        assert_eq!(scheduled.title, "Schedule 'nightly' completed");
        assert_eq!(scheduled.body, "All done.");
    }

    #[test]
    fn classify_schedule_run_failure_shares_dedup_key_with_classify_error() {
        let prefs = NotificationPreferences::default();
        let scheduled = classify_schedule_run(
            "sess-2",
            false,
            "Schedule 'nightly' failed".to_string(),
            "boom".to_string(),
            &prefs,
        )
        .unwrap();
        let raw = classify("sess-2", &error("boom"), &prefs).unwrap();
        assert_eq!(scheduled.dedup_key, raw.dedup_key);
        assert_eq!(scheduled.category, NotificationCategory::RunFailed);
        assert_eq!(scheduled.priority, NotificationPriority::High);
    }

    #[test]
    fn classify_schedule_run_gated_by_on_run_complete_and_on_run_failed() {
        let complete_off = NotificationPreferences {
            on_run_complete: false,
            ..NotificationPreferences::default()
        };
        assert!(classify_schedule_run(
            "sess",
            true,
            "t".to_string(),
            "b".to_string(),
            &complete_off
        )
        .is_none());

        let failed_off = NotificationPreferences {
            on_run_failed: false,
            ..NotificationPreferences::default()
        };
        assert!(classify_schedule_run(
            "sess",
            false,
            "t".to_string(),
            "b".to_string(),
            &failed_off
        )
        .is_none());
    }

    #[test]
    fn classify_schedule_run_master_switch_off_yields_none() {
        let prefs = NotificationPreferences {
            enabled: false,
            ..NotificationPreferences::default()
        };
        assert!(
            classify_schedule_run("sess", true, "t".to_string(), "b".to_string(), &prefs).is_none()
        );
        assert!(
            classify_schedule_run("sess", false, "t".to_string(), "b".to_string(), &prefs)
                .is_none()
        );
    }
}
