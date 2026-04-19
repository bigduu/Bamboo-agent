//! Bamboo workflow domain types.
//!
//! Defines the `WorkflowDefinition` entity and its validation rules,
//! independent of filesystem loading and server infrastructure.

pub mod definition;

pub use definition::{WorkflowDefinition, WorkflowLoadError, validate_expr, validate_required};
