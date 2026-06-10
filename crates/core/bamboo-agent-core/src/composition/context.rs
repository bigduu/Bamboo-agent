//! Execution context for tool composition DSL
//!
//! This module provides the ExecutionContext type for tracking variables and state
//! during tool composition execution.

use crate::tools::ToolResult;
use serde_json::Value;
use std::collections::HashMap;

/// Context for tool execution, carrying variables and state
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Variable bindings (for new DSL)
    pub bindings: HashMap<String, ToolResult>,
    /// Execution history for debugging
    pub execution_log: Vec<ExecutionStep>,
    /// Parent context for nested scopes
    parent: Option<Box<ExecutionContext>>,
    /// Variables for old API compatibility
    pub variables: serde_json::Map<String, Value>,
    /// Last result for old API compatibility
    pub last_result: Option<ToolResult>,
}

/// A single step in the execution log
#[derive(Debug, Clone)]
pub struct ExecutionStep {
    /// Type of step (e.g., "call", "sequence", "parallel")
    pub step_type: String,
    /// Result of this execution step
    pub result: Result<ToolResult, crate::tools::ToolError>,
    /// When this step was executed
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl ExecutionContext {
    /// Create a new empty execution context
    pub fn new() -> Self {
        Self {
            bindings: HashMap::new(),
            execution_log: Vec::new(),
            parent: None,
            variables: serde_json::Map::new(),
            last_result: None,
        }
    }

    /// Create a nested scope with this context as parent
    pub fn nested_scope(&self) -> Self {
        Self {
            bindings: HashMap::new(),
            execution_log: Vec::new(),
            parent: Some(Box::new(self.clone())),
            variables: self.variables.clone(),
            last_result: self.last_result.clone(),
        }
    }

    // Old API compatibility methods

    /// Set a variable (old API)
    pub fn set_variable(&mut self, key: impl Into<String>, value: Value) {
        self.variables.insert(key.into(), value);
    }

    /// Get a variable (old API)
    pub fn get_variable(&self, key: &str) -> Option<&Value> {
        self.variables.get(key)
    }

    /// With variable builder pattern (old API)
    pub fn with_variable(mut self, key: impl Into<String>, value: Value) -> Self {
        self.variables.insert(key.into(), value);
        self
    }

    /// With last result (old API)
    pub fn with_last_result(mut self, result: ToolResult) -> Self {
        self.last_result = Some(result);
        self
    }

    /// Bind a variable to a value
    pub fn bind(&mut self, name: String, value: ToolResult) {
        self.bindings.insert(name, value);
    }

    /// Look up a variable, checking parent scopes if not found locally
    pub fn lookup(&self, name: &str) -> Option<&ToolResult> {
        self.bindings
            .get(name)
            .or_else(|| self.parent.as_ref().and_then(|p| p.lookup(name)))
    }

    /// Log an execution step
    pub fn log_step(
        &mut self,
        step_type: String,
        result: Result<ToolResult, crate::tools::ToolError>,
    ) {
        self.execution_log.push(ExecutionStep {
            step_type,
            result,
            timestamp: chrono::Utc::now(),
        });
    }

    /// Get all bindings including from parent scopes
    pub fn all_bindings(&self) -> HashMap<String, &ToolResult> {
        let mut result = HashMap::new();

        // Add parent bindings first (they can be overridden)
        if let Some(ref parent) = self.parent {
            for (k, v) in parent.all_bindings() {
                result.insert(k, v);
            }
        }

        // Add local bindings
        for (k, v) in &self.bindings {
            result.insert(k.clone(), v);
        }

        result
    }
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_bindings() {
        let mut ctx = ExecutionContext::new();
        let result = ToolResult {
            success: true,
            result: "test".to_string(),
            display_preference: None,
            images: Vec::new(),
        };

        ctx.bind("test_var".to_string(), result.clone());
        assert!(ctx.lookup("test_var").is_some());
        assert!(ctx.lookup("nonexistent").is_none());
    }

    #[test]
    fn test_nested_scope() {
        let mut parent = ExecutionContext::new();
        let result = ToolResult {
            success: true,
            result: "parent_value".to_string(),
            display_preference: None,
            images: Vec::new(),
        };
        parent.bind("shared".to_string(), result);

        let mut child = parent.nested_scope();
        let child_result = ToolResult {
            success: true,
            result: "child_value".to_string(),
            display_preference: None,
            images: Vec::new(),
        };
        child.bind("child_only".to_string(), child_result);

        // Child can see parent's bindings
        assert!(child.lookup("shared").is_some());
        // Parent cannot see child's bindings
        assert!(parent.lookup("child_only").is_none());
    }
}
