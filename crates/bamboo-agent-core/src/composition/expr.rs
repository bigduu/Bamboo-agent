//! Tool Expression DSL — Serializable tool composition language.
//!
//! `ToolExpr`, `Condition`, and `ParallelWait` are defined in
//! `bamboo_domain::composition` (pure domain types).
//! This file keeps `CompositionError` (which depends on `ToolError`)
//! and re-exports the domain types for backward compatibility.

use crate::tools::ToolError;

// Re-export domain types
pub use bamboo_domain::composition::{
    Condition, ParallelWait, ToolExpr,
};

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
    fn test_yaml_roundtrip() {
        let expr = ToolExpr::sequence(vec![
            ToolExpr::call("step1", json!({"arg": 1})),
            ToolExpr::call("step2", json!({"arg": 2})),
        ]);
        let yaml = expr.to_yaml().unwrap();
        let deserialized = ToolExpr::from_yaml(&yaml).unwrap();
        assert_eq!(expr, deserialized);
    }
}
