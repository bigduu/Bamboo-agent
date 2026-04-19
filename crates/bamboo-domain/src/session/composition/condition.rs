//! Condition predicates for workflow control flow.
//!
//! Pure data types — the `evaluate(&ToolResult)` method lives in
//! bamboo-application-agent's composition module since it depends on
//! the application-layer `ToolResult` type.

use regex::Regex;
use serde::{Deserialize, Serialize};

/// Condition for control flow in tool expressions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Condition {
    /// Check if the result was successful
    Success,
    /// Check if JSON path contains a specific value
    Contains {
        path: String,
        value: String,
    },
    /// Check if value at JSON path matches a regex pattern
    Matches {
        path: String,
        pattern: String,
    },
    /// All conditions must be true
    And {
        conditions: Vec<Condition>,
    },
    /// At least one condition must be true
    Or {
        conditions: Vec<Condition>,
    },
}

/// Evaluate condition against a (success, result_json) pair.
///
/// This is the domain-layer evaluation that doesn't depend on
/// application-layer types.
pub fn evaluate_condition(condition: &Condition, success: bool, result_json: &str) -> bool {
    match condition {
        Condition::Success => success,
        Condition::Contains { path, value } => evaluate_contains(result_json, path, value),
        Condition::Matches { path, pattern } => evaluate_matches(result_json, path, pattern),
        Condition::And { conditions } => conditions
            .iter()
            .all(|c| evaluate_condition(c, success, result_json)),
        Condition::Or { conditions } => conditions
            .iter()
            .any(|c| evaluate_condition(c, success, result_json)),
    }
}

fn extract_at_path(json_str: &str, path: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = &value;
    for part in parts {
        if let Ok(index) = part.parse::<usize>() {
            current = current.get(index)?;
        } else {
            current = current.get(part)?;
        }
    }
    Some(current.to_string().trim_matches('"').to_string())
}

fn evaluate_contains(json_str: &str, path: &str, expected: &str) -> bool {
    extract_at_path(json_str, path)
        .map(|v| v.contains(expected))
        .unwrap_or(false)
}

fn evaluate_matches(json_str: &str, path: &str, pattern: &str) -> bool {
    extract_at_path(json_str, path)
        .and_then(|v| Regex::new(pattern).ok().map(|re| re.is_match(&v)))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_success_condition() {
        assert!(evaluate_condition(&Condition::Success, true, "{}"));
        assert!(!evaluate_condition(&Condition::Success, false, "{}"));
    }

    #[test]
    fn test_contains_condition() {
        let json = r#"{"status": "completed", "data": {"name": "test"}}"#;
        let cond = Condition::Contains {
            path: "status".into(),
            value: "complete".into(),
        };
        assert!(evaluate_condition(&cond, true, json));

        let cond = Condition::Contains {
            path: "data.name".into(),
            value: "test".into(),
        };
        assert!(evaluate_condition(&cond, true, json));

        let cond = Condition::Contains {
            path: "status".into(),
            value: "failed".into(),
        };
        assert!(!evaluate_condition(&cond, true, json));
    }

    #[test]
    fn test_matches_condition() {
        let json = r#"{"email": "user@example.com"}"#;
        let cond = Condition::Matches {
            path: "email".into(),
            pattern: r"^\S+@\S+\.\S+$".into(),
        };
        assert!(evaluate_condition(&cond, true, json));
    }

    #[test]
    fn test_json_serialization() {
        let cond = Condition::And {
            conditions: vec![
                Condition::Success,
                Condition::Contains {
                    path: "status".into(),
                    value: "ok".into(),
                },
            ],
        };
        let json = serde_json::to_string(&cond).unwrap();
        assert!(json.contains("\"type\":\"and\"") || json.contains("\"type\": \"and\""));
        let deserialized: Condition = serde_json::from_str(&json).unwrap();
        assert_eq!(cond, deserialized);
    }
}
