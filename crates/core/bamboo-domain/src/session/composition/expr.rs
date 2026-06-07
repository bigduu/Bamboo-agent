//! Tool expression AST types — pure data, no agent dependencies.

use serde::{Deserialize, Serialize};

use super::condition::Condition;
use super::parallel::ParallelWait;

/// Tool expression DSL for composing tool calls.
///
/// This enum represents the AST for the tool composition DSL.
/// Each variant represents a different composition operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ToolExpr {
    /// Execute a single tool call
    Call {
        /// Name of the tool to execute
        tool: String,
        /// Arguments to pass to the tool (JSON object)
        args: serde_json::Value,
    },
    /// Execute a sequence of expressions
    Sequence {
        /// Steps to execute in order
        steps: Vec<ToolExpr>,
        /// Whether to stop on first error (default: true)
        #[serde(default = "default_fail_fast")]
        fail_fast: bool,
    },
    /// Execute branches in parallel
    Parallel {
        /// Branches to execute concurrently
        branches: Vec<ToolExpr>,
        /// Wait strategy: All, First, or Any
        #[serde(default)]
        wait: ParallelWait,
    },
    /// Conditional execution
    Choice {
        /// Condition to evaluate
        condition: Condition,
        /// Expression to execute if condition is true
        then_branch: Box<ToolExpr>,
        /// Expression to execute if condition is false
        else_branch: Option<Box<ToolExpr>>,
    },
    /// Retry with backoff
    Retry {
        /// Expression to retry
        expr: Box<ToolExpr>,
        /// Maximum number of retry attempts (default: 3)
        #[serde(default = "default_max_attempts")]
        max_attempts: u32,
        /// Delay between retries in milliseconds (default: 1000)
        #[serde(default = "default_delay_ms")]
        delay_ms: u64,
    },
    /// Variable binding
    Let {
        /// Variable name
        var: String,
        /// Expression to bind
        expr: Box<ToolExpr>,
        /// Body expression that use the variable
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
}
