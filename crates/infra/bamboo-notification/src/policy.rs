//! Pure classification of agent events into candidate notifications.
//!
//! [`classify`] is the single source of truth for *which* events are
//! notification-worthy, *how* they are categorised and prioritised, and *what*
//! their dedup key is. It is intentionally side-effect free: it reads an
//! [`AgentEvent`] and the user's [`NotificationPreferences`] and returns an
//! optional [`ClassifiedNotification`]. Dedup timing and id/timestamp minting
//! happen one layer up in [`crate::service`].

use bamboo_agent_core::AgentEvent;

use crate::preferences::NotificationPreferences;

/// Maximum body length for a clarification notification, in characters.
const CLARIFICATION_BODY_MAX: usize = 120;

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
}

impl NotificationCategory {
    /// Returns the stable wire string for this category.
    pub fn as_str(&self) -> &'static str {
        match self {
            NotificationCategory::NeedsApproval => "needs_approval",
            NotificationCategory::NeedsClarification => "needs_clarification",
            NotificationCategory::ContextCritical => "context_critical",
            NotificationCategory::SubagentCompleted => "subagent_completed",
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

        _ => None,
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
    fn master_switch_off_yields_none() {
        let prefs = NotificationPreferences {
            enabled: false,
            ..NotificationPreferences::default()
        };
        assert!(classify("sess", &context("critical"), &prefs).is_none());
        assert!(classify("sess", &subagent("completed"), &prefs).is_none());
        assert!(classify("sess", &clarification("q", None), &prefs).is_none());
    }

    #[test]
    fn unrelated_variant_is_none() {
        let prefs = NotificationPreferences::default();
        let event = AgentEvent::Token {
            content: "hello".to_string(),
        };
        assert!(classify("sess", &event, &prefs).is_none());
    }
}
