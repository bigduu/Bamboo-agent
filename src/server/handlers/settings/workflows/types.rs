use serde::{Deserialize, Serialize};

/// Workflow list item for API responses.
#[derive(Serialize)]
pub(super) struct WorkflowListItem {
    /// Workflow name.
    pub(super) name: String,
    /// Filename (e.g., "myworkflow.md").
    pub(super) filename: String,
    /// File size in bytes.
    pub(super) size: u64,
    /// Last modified timestamp (currently not populated).
    pub(super) modified_at: Option<String>,
}

/// Full workflow data with content.
#[derive(Serialize)]
pub(super) struct WorkflowGetResponse {
    /// Workflow name.
    pub(super) name: String,
    /// Filename.
    pub(super) filename: String,
    /// Workflow markdown content.
    pub(super) content: String,
    /// File size in bytes.
    pub(super) size: u64,
    /// Last modified timestamp (currently not populated).
    pub(super) modified_at: Option<String>,
}

/// Request body for saving a workflow.
#[derive(Deserialize)]
pub struct SaveWorkflowRequest {
    /// Workflow name.
    pub name: String,
    /// Workflow markdown content.
    pub content: String,
}
