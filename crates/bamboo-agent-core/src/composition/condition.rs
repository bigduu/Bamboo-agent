//! Condition predicates — application-layer adapter.
//!
//! `Condition` and its evaluation logic live in `bamboo_domain::composition`.
//! This module provides an adapter that bridges `evaluate(&ToolResult)` to the
//! domain-layer `evaluate_condition()` function.

pub use bamboo_domain::composition::{evaluate_condition, Condition};

use crate::tools::ToolResult;

/// Evaluate a condition against a `ToolResult`.
pub fn evaluate_tool_result(condition: &Condition, result: &ToolResult) -> bool {
    evaluate_condition(condition, result.success, &result.result)
}
