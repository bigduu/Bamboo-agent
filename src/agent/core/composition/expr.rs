//! Tool Expression DSL - Serializable tool composition language
//!
//! This module provides a declarative DSL for composing tool calls that can be
//! serialized to/from YAML and JSON.

use crate::agent::core::tools::ToolError;
use serde::{Deserialize, Serialize};

use super::condition::Condition;
use super::parallel::ParallelWait;

/// Tool expression DSL for composing tool calls
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ToolExpr {
    /// Execute a single tool call
    Call {
        tool: String,
        args: serde_json::Value,
    },
    /// Execute a sequence of expressions
    Sequence {
        steps: Vec<ToolExpr>,
        #[serde(default = "default_fail_fast")]
        fail_fast: bool,
    },
    /// Execute branches in parallel
    Parallel {
        branches: Vec<ToolExpr>,
        #[serde(default)]
        wait: ParallelWait,
    },
    /// Conditional execution
    Choice {
        condition: Condition,
        then_branch: Box<ToolExpr>,
        else_branch: Option<Box<ToolExpr>>,
    },
    /// Retry with backoff
    Retry {
        expr: Box<ToolExpr>,
        #[serde(default = "default_max_attempts")]
        max_attempts: u32,
        #[serde(default = "default_delay_ms")]
        delay_ms: u64,
    },
    /// Variable binding
    Let {
        var: String,
        expr: Box<ToolExpr>,
        body: Box<ToolExpr>,
    },
    /// Variable reference
    Var(String),
}

fn default_fail_fast() -> bool {
    true
}

fn default_max_attempts() -> u32 {
    3
}

fn default_delay_ms() -> u64 {
    1000
}

impl ToolExpr {
    /// Create a simple tool call expression
    pub fn call(tool: impl Into<String>, args: serde_json::Value) -> Self {
        ToolExpr::Call {
            tool: tool.into(),
            args,
        }
    }

    /// Create a sequence expression with fail_fast=true
    pub fn sequence(steps: Vec<ToolExpr>) -> Self {
        ToolExpr::Sequence {
            steps,
            fail_fast: true,
        }
    }

    /// Create a sequence expression with custom fail_fast
    pub fn sequence_with_fail_fast(steps: Vec<ToolExpr>, fail_fast: bool) -> Self {
        ToolExpr::Sequence { steps, fail_fast }
    }

    /// Create a parallel expression
    pub fn parallel(branches: Vec<ToolExpr>) -> Self {
        ToolExpr::Parallel {
            branches,
            wait: ParallelWait::All,
        }
    }

    /// Create a parallel expression with custom wait strategy
    pub fn parallel_with_wait(branches: Vec<ToolExpr>, wait: ParallelWait) -> Self {
        ToolExpr::Parallel { branches, wait }
    }

    /// Create a conditional expression
    pub fn choice(condition: Condition, then_branch: ToolExpr) -> Self {
        ToolExpr::Choice {
            condition,
            then_branch: Box::new(then_branch),
            else_branch: None,
        }
    }

    /// Create a conditional expression with else branch
    pub fn choice_with_else(
        condition: Condition,
        then_branch: ToolExpr,
        else_branch: ToolExpr,
    ) -> Self {
        ToolExpr::Choice {
            condition,
            then_branch: Box::new(then_branch),
            else_branch: Some(Box::new(else_branch)),
        }
    }

    /// Create a retry expression with defaults
    pub fn retry(expr: ToolExpr) -> Self {
        ToolExpr::Retry {
            expr: Box::new(expr),
            max_attempts: 3,
            delay_ms: 1000,
        }
    }

    /// Create a retry expression with custom parameters
    pub fn retry_with_params(expr: ToolExpr, max_attempts: u32, delay_ms: u64) -> Self {
        ToolExpr::Retry {
            expr: Box::new(expr),
            max_attempts,
            delay_ms,
        }
    }

    /// Create a let binding expression
    pub fn let_binding(var: impl Into<String>, expr: ToolExpr, body: ToolExpr) -> Self {
        ToolExpr::Let {
            var: var.into(),
            expr: Box::new(expr),
            body: Box::new(body),
        }
    }

    /// Create a variable reference
    pub fn var(name: impl Into<String>) -> Self {
        ToolExpr::Var(name.into())
    }

    /// Serialize to YAML string
    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    /// Deserialize from YAML string
    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    /// Serialize to JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize from JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Composition error types
#[derive(Debug, Clone)]
pub enum CompositionError {
    ToolError(ToolError),
    VariableNotFound(String),
    InvalidExpression(String),
    MaxRetriesExceeded,
}

impl std::fmt::Display for CompositionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompositionError::ToolError(e) => write!(f, "Tool error: {}", e),
            CompositionError::VariableNotFound(v) => write!(f, "Variable not found: {}", v),
            CompositionError::InvalidExpression(e) => write!(f, "Invalid expression: {}", e),
            CompositionError::MaxRetriesExceeded => write!(f, "Maximum retry attempts exceeded"),
        }
    }
}

impl std::error::Error for CompositionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CompositionError::ToolError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ToolError> for CompositionError {
    fn from(e: ToolError) -> Self {
        CompositionError::ToolError(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_call_expr() {
        let expr = ToolExpr::call("read_file", json!({"path": "/tmp/test"}));

        match expr {
            ToolExpr::Call { tool, args } => {
                assert_eq!(tool, "read_file");
                assert_eq!(args["path"], "/tmp/test");
            }
            _ => panic!("Expected Call variant"),
        }
    }

    #[test]
    fn test_sequence_expr() {
        let steps = vec![
            ToolExpr::call("step1", json!({})),
            ToolExpr::call("step2", json!({})),
        ];
        let expr = ToolExpr::sequence(steps);

        match expr {
            ToolExpr::Sequence { steps, fail_fast } => {
                assert_eq!(steps.len(), 2);
                assert!(fail_fast);
            }
            _ => panic!("Expected Sequence variant"),
        }
    }

    #[test]
    fn test_parallel_expr() {
        let branches = vec![
            ToolExpr::call("branch1", json!({})),
            ToolExpr::call("branch2", json!({})),
        ];
        let expr = ToolExpr::parallel(branches);

        match expr {
            ToolExpr::Parallel { branches, wait } => {
                assert_eq!(branches.len(), 2);
                assert_eq!(wait, ParallelWait::All);
            }
            _ => panic!("Expected Parallel variant"),
        }
    }

    #[test]
    fn test_choice_expr() {
        let condition = Condition::Success;
        let then_branch = ToolExpr::call("success_handler", json!({}));
        let else_branch = ToolExpr::call("failure_handler", json!({}));

        let expr = ToolExpr::choice_with_else(condition, then_branch, else_branch);

        match expr {
            ToolExpr::Choice {
                condition: _,
                then_branch,
                else_branch,
            } => {
                assert!(else_branch.is_some());
                match *then_branch {
                    ToolExpr::Call { tool, .. } => assert_eq!(tool, "success_handler"),
                    _ => panic!("Expected Call in then_branch"),
                }
            }
            _ => panic!("Expected Choice variant"),
        }
    }

    #[test]
    fn test_retry_expr() {
        let inner = ToolExpr::call("risky_op", json!({}));
        let expr = ToolExpr::retry_with_params(inner, 5, 500);

        match expr {
            ToolExpr::Retry {
                expr: _,
                max_attempts,
                delay_ms,
            } => {
                assert_eq!(max_attempts, 5);
                assert_eq!(delay_ms, 500);
            }
            _ => panic!("Expected Retry variant"),
        }
    }

    #[test]
    fn test_let_expr() {
        let expr = ToolExpr::let_binding(
            "result",
            ToolExpr::call("fetch", json!({"url": "http://example.com"})),
            ToolExpr::call("process", json!({"data": "${result}"})),
        );

        match expr {
            ToolExpr::Let { var, expr, body } => {
                assert_eq!(var, "result");
                assert!(matches!(*expr, ToolExpr::Call { .. }));
                assert!(matches!(*body, ToolExpr::Call { .. }));
            }
            _ => panic!("Expected Let variant"),
        }
    }

    #[test]
    fn test_yaml_roundtrip() {
        let expr = ToolExpr::sequence(vec![
            ToolExpr::call("step1", json!({"arg": 1})),
            ToolExpr::call("step2", json!({"arg": 2})),
        ]);

        let yaml = expr.to_yaml().unwrap();
        let deserialized = ToolExpr::from_yaml(&yaml).unwrap();

        assert_eq!(expr, deserialized);
    }

    #[test]
    fn test_json_roundtrip() {
        let expr = ToolExpr::choice_with_else(
            Condition::Success,
            ToolExpr::call("on_success", json!({})),
            ToolExpr::call("on_failure", json!({})),
        );

        let json_str = expr.to_json().unwrap();
        let deserialized = ToolExpr::from_json(&json_str).unwrap();

        assert_eq!(expr, deserialized);
    }

    #[test]
    fn test_yaml_deserialization() {
        let yaml = r#"
type: sequence
steps:
  - type: call
    tool: read_file
    args:
      path: /tmp/test.txt
  - type: call
    tool: process
    args:
      data: "hello"
fail_fast: true
"#;

        let expr: ToolExpr = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(expr, ToolExpr::Sequence { .. }));
    }
}
