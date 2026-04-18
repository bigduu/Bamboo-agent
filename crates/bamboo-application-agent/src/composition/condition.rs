//! Condition predicates for workflow control flow
//!
//! This module provides condition types for branching logic in tool compositions.

use crate::tools::ToolResult;
use regex::Regex;
use serde::{Deserialize, Serialize};

/// Condition for control flow in tool expressions
///
/// Conditions are used in `Choice` expressions to determine which branch to execute.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Condition {
    /// Check if the result was successful
    Success,
    /// Check if JSON path contains a specific value
    Contains {
        /// JSON path to check (dot notation, e.g., "data.status")
        path: String,
        /// Value to check for
        value: String,
    },
    /// Check if value at JSON path matches a regex pattern
    Matches {
        /// JSON path to check (dot notation)
        path: String,
        /// Regex pattern to match
        pattern: String,
    },
    /// All conditions must be true
    And {
        /// List of conditions (all must be true)
        conditions: Vec<Condition>,
    },
    /// At least one condition must be true
    Or {
        /// List of conditions (at least one must be true)
        conditions: Vec<Condition>,
    },
}

impl Condition {
    /// Evaluate the condition against a tool result
    pub fn evaluate(&self, result: &ToolResult) -> bool {
        match self {
            Condition::Success => result.success,
            Condition::Contains { path, value } => evaluate_contains(&result.result, path, value),
            Condition::Matches { path, pattern } => evaluate_matches(&result.result, path, pattern),
            Condition::And { conditions } => conditions.iter().all(|c| c.evaluate(result)),
            Condition::Or { conditions } => conditions.iter().any(|c| c.evaluate(result)),
        }
    }
}

/// Extract value at JSON path (simple dot notation)
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

/// Check if value at path contains the expected value
fn evaluate_contains(json_str: &str, path: &str, expected: &str) -> bool {
    if let Some(value) = extract_at_path(json_str, path) {
        value.contains(expected)
    } else {
        false
    }
}

/// Check if value at path matches the regex pattern
fn evaluate_matches(json_str: &str, path: &str, pattern: &str) -> bool {
    let value = match extract_at_path(json_str, path) {
        Some(v) => v,
        None => return false,
    };

    Regex::new(pattern)
        .map(|re| re.is_match(&value))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_result(result_str: &str, success: bool) -> ToolResult {
        ToolResult {
            success,
            result: result_str.to_string(),
            display_preference: None,
        }
    }

    #[test]
    fn test_success_condition() {
        let success_result = create_result("{}", true);
        let failure_result = create_result("{}", false);

        assert!(Condition::Success.evaluate(&success_result));
        assert!(!Condition::Success.evaluate(&failure_result));
    }

    #[test]
    fn test_contains_condition() {
        let result = create_result(r#"{"status": "completed", "data": {"name": "test"}}"#, true);

        let cond = Condition::Contains {
            path: "status".to_string(),
            value: "complete".to_string(),
        };
        assert!(cond.evaluate(&result));

        let cond = Condition::Contains {
            path: "data.name".to_string(),
            value: "test".to_string(),
        };
        assert!(cond.evaluate(&result));

        let cond = Condition::Contains {
            path: "status".to_string(),
            value: "failed".to_string(),
        };
        assert!(!cond.evaluate(&result));
    }

    #[test]
    fn test_matches_condition() {
        let result = create_result(r#"{"email": "user@example.com"}"#, true);

        let cond = Condition::Matches {
            path: "email".to_string(),
            pattern: r"^\S+@\S+\.\S+$".to_string(),
        };
        assert!(cond.evaluate(&result));

        let cond = Condition::Matches {
            path: "email".to_string(),
            pattern: r"^admin@".to_string(),
        };
        assert!(!cond.evaluate(&result));
    }

    #[test]
    fn test_and_condition() {
        let result = create_result(r#"{"status": "ok", "code": 200}"#, true);

        let cond = Condition::And {
            conditions: vec![
                Condition::Success,
                Condition::Contains {
                    path: "status".to_string(),
                    value: "ok".to_string(),
                },
            ],
        };
        assert!(cond.evaluate(&result));

        let cond = Condition::And {
            conditions: vec![
                Condition::Success,
                Condition::Contains {
                    path: "status".to_string(),
                    value: "error".to_string(),
                },
            ],
        };
        assert!(!cond.evaluate(&result));
    }

    #[test]
    fn test_or_condition() {
        let result = create_result(r#"{"status": "warning"}"#, true);

        let cond = Condition::Or {
            conditions: vec![
                Condition::Contains {
                    path: "status".to_string(),
                    value: "ok".to_string(),
                },
                Condition::Contains {
                    path: "status".to_string(),
                    value: "warning".to_string(),
                },
            ],
        };
        assert!(cond.evaluate(&result));
    }

    #[test]
    fn test_json_serialization() {
        let cond = Condition::And {
            conditions: vec![
                Condition::Success,
                Condition::Contains {
                    path: "status".to_string(),
                    value: "ok".to_string(),
                },
            ],
        };

        let json = serde_json::to_string(&cond).unwrap();
        assert!(json.contains("\"type\":\"and\"") || json.contains("\"type\": \"and\""));

        let deserialized: Condition = serde_json::from_str(&json).unwrap();
        assert_eq!(cond, deserialized);
    }

    #[test]
    fn test_condition_roundtrip() {
        let conditions = vec![
            Condition::Success,
            Condition::Contains {
                path: "status".to_string(),
                value: "ok".to_string(),
            },
            Condition::Matches {
                path: "email".to_string(),
                pattern: r"^\S+@\S+\.\S+$".to_string(),
            },
            Condition::And {
                conditions: vec![Condition::Success, Condition::Success],
            },
            Condition::Or {
                conditions: vec![Condition::Success],
            },
        ];

        for original in conditions {
            let json = serde_json::to_string(&original).unwrap();
            let deserialized: Condition = serde_json::from_str(&json).unwrap();
            assert_eq!(original, deserialized);
        }
    }
}
