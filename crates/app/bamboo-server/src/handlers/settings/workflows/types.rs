use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CloneWorkflowTarget {
    Project,
    User,
}

/// Exact metadata-only selection for cloning one immutable builtin Workflow.
///
/// The request never accepts prompt/resource bytes or a caller-supplied path.
/// Project publication is derived from the durable Session identity.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloneWorkflowRequest {
    pub source: bamboo_skills::WorkflowSource,
    pub revision: u64,
    pub content_digest: String,
    pub target: CloneWorkflowTarget,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct CloneWorkflowResponse {
    pub workflow_id: String,
    pub target: CloneWorkflowTarget,
    pub source_preserved: bool,
    pub source_revision: u64,
    pub source_content_digest: String,
    pub published_source: bamboo_skills::WorkflowSource,
    pub published_revision: u64,
    pub published_content_digest: String,
    pub catalog_revision: u64,
}

#[derive(Debug, Default, Deserialize)]
pub struct WorkflowCatalogQuery {
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Request body for explicitly migrating a read-only legacy workflow into the
/// canonical user or assigned-Project Skill layer selected by the source.
#[derive(Debug, Default, Deserialize)]
pub struct MigrateWorkflowRequest {
    /// Trusted session used to resolve the Project/workspace publication scope.
    pub session_id: String,
    /// Optional replacement for the legacy workflow description. Supplying one
    /// enables automatic invocation; otherwise description-less sources remain
    /// manual-only after migration.
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MigrateWorkflowResponse {
    pub workflow_id: String,
    pub outcome: bamboo_skills::legacy::LegacyWorkflowMigrationOutcome,
    pub source_preserved: bool,
    pub catalog_revision: u64,
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_list_item_serialization() {
        let item = WorkflowListItem {
            name: "my-workflow".to_string(),
            filename: "myworkflow.md".to_string(),
            size: 1024,
            modified_at: Some("2024-01-01".to_string()),
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("my-workflow"));
        assert!(json.contains("myworkflow.md"));
        assert!(json.contains("1024"));
    }

    #[test]
    fn test_workflow_list_item_without_modified() {
        let item = WorkflowListItem {
            name: "test".to_string(),
            filename: "test.md".to_string(),
            size: 512,
            modified_at: None,
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("test"));
        assert!(json.contains("512"));
    }

    #[test]
    fn test_workflow_get_response_serialization() {
        let response = WorkflowGetResponse {
            name: "deploy".to_string(),
            filename: "deploy.md".to_string(),
            content: "# Deploy Workflow\n\nSteps...".to_string(),
            size: 2048,
            modified_at: None,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("deploy"));
        assert!(json.contains("deploy.md"));
        assert!(json.contains("Deploy Workflow"));
    }

    #[test]
    fn test_workflow_get_response_with_content() {
        let markdown = "# Test\n\n## Steps\n\n1. Step one\n2. Step two";
        let response = WorkflowGetResponse {
            name: "test".to_string(),
            filename: "test.md".to_string(),
            content: markdown.to_string(),
            size: markdown.len() as u64,
            modified_at: Some("2024-03-17".to_string()),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("Test"));
        assert!(json.contains("Step one"));
    }

    #[test]
    fn test_save_workflow_request_deserialization() {
        let content = "# My Workflow\n\nContent here";
        let json = serde_json::json!({
            "name": "my-workflow",
            "content": content
        })
        .to_string();
        let req: SaveWorkflowRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(req.name, "my-workflow");
        assert!(req.content.contains("My Workflow"));
    }

    #[test]
    fn test_save_workflow_request_with_markdown() {
        let content = "# Deploy\n\n## Steps\n\n1. Build\n2. Deploy";
        let json = serde_json::json!({
            "name": "deployment",
            "content": content
        })
        .to_string();
        let req: SaveWorkflowRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(req.name, "deployment");
        assert!(req.content.contains("Build"));
        assert!(req.content.contains("Deploy"));
    }

    #[test]
    fn test_save_workflow_request_empty_content() {
        let json = r#"{"name":"empty","content":""}"#;
        let req: SaveWorkflowRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.name, "empty");
        assert_eq!(req.content, "");
    }

    #[test]
    fn test_workflow_list_item_size() {
        let item = WorkflowListItem {
            name: "test".to_string(),
            filename: "test.md".to_string(),
            size: 0,
            modified_at: None,
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"size\":0"));
    }

    #[test]
    fn test_workflow_get_response_size() {
        let content = "x".repeat(100);
        let response = WorkflowGetResponse {
            name: "test".to_string(),
            filename: "test.md".to_string(),
            content: content.clone(),
            size: content.len() as u64,
            modified_at: None,
        };

        assert_eq!(response.size, 100);
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"size\":100"));
    }
}
