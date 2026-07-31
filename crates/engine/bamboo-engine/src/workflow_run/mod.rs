//! Durable, catalog-revision-pinned workflow execution.

mod executor;
mod repository;

pub use bamboo_domain::{CompiledWorkflow, WorkflowCompileError};
pub use executor::{
    AgentStepPort, AgentStepResult, NamedAgentSpec, PermissionDecision, WorkflowDefinitionPort,
    WorkflowPolicyPort, WorkflowPolicyTarget, WorkflowRunEngine, WorkflowRunError,
    WorkflowSecretMaterial, WorkflowSecretResolverPort, WorkflowSessionPermissionPort,
};
pub use repository::{FileWorkflowRunRepository, WorkflowRunRepository};

#[cfg(test)]
mod tests;
