//! Bamboo workflow domain types.
//!
//! Defines the `WorkflowDefinition` entity and its validation rules,
//! independent of filesystem loading and server infrastructure.

pub mod compiler;
pub mod definition;
pub mod run;
pub mod schema;

pub use compiler::{CompiledWorkflow, WorkflowCompileError};
pub use definition::{validate_expr, validate_required, WorkflowDefinition, WorkflowLoadError};
pub use run::*;
pub use schema::{validate_schema, validate_schema_shape};
