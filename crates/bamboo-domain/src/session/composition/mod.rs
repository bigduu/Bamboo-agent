//! Tool Expression DSL — Serializable tool composition language.
//!
//! Pure domain types for composing tool calls into workflows.
//! Moved here from bamboo-application-agent to fix the domain → application
//! dependency inversion in bamboo-domain-workflow.

mod condition;
mod expr;
mod parallel;

pub use condition::{Condition, evaluate_condition};
pub use expr::ToolExpr;
pub use parallel::ParallelWait;
