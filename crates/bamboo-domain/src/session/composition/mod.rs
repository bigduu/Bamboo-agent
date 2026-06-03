//! Tool Expression DSL — Serializable tool composition language.
//!
//! Pure domain types for composing tool calls into workflows.
//! Moved here from bamboo-engine to fix the domain → application
//! dependency inversion in bamboo-domain-workflow.

mod condition;
mod expr;
mod parallel;

pub use condition::{evaluate_condition, Condition};
pub use expr::ToolExpr;
pub use parallel::ParallelWait;
