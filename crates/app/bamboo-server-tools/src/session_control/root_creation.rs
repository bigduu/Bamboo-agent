//! Optional Host port for creating a new, explicitly scoped Ordinary Root.

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolError, ToolResult};
use bamboo_domain::{ProjectId, SupervisorReference};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisedRootCreateRequest {
    pub operation_id: uuid::Uuid,
    pub expected_state_revision: u64,
    pub project_id: ProjectId,
    pub workspace_path: String,
    pub message: String,
    #[serde(default)]
    pub title: Option<String>,
}

impl SupervisedRootCreateRequest {
    pub fn validate(&self) -> Result<(), ToolError> {
        if self.operation_id.is_nil()
            || self.workspace_path.trim().is_empty()
            || self.workspace_path.len() > 4096
            || self.message.trim().is_empty()
            || self.message.len() > bamboo_engine::session_messaging::MAX_SUPERVISOR_FOLLOWUP_BYTES
            || self.title.as_ref().is_some_and(|title| {
                title.trim().is_empty() || title.chars().count() > 256 || title.len() > 1024
            })
        {
            return Err(ToolError::InvalidArguments(
                "create requires a non-nil operation UUID, explicit workspace (1..=4096 UTF-8 bytes), instruction (1..=16384 UTF-8 bytes), and optional non-empty title (at most 256 characters/1024 bytes)".into(),
            ));
        }
        Ok(())
    }
}

/// Only a Host with canonical Root creation, Project/workspace validation and
/// scoped management/Inbox writers installs this port. Arguments contain no
/// caller identity, pre-existing target, permissions, private prompt or mode.
#[async_trait]
pub trait SupervisedRootCreatePort: Send + Sync {
    async fn scope(&self, supervisor: &SupervisorReference) -> Result<ToolResult, ToolError>;
    async fn create(
        &self,
        supervisor: &SupervisorReference,
        request: SupervisedRootCreateRequest,
    ) -> Result<ToolResult, ToolError>;
}

pub(super) fn request_schema() -> Value {
    json!({
        "type":"object", "additionalProperties":false,
        "properties": {
            "operation_id":{"type":"string", "format":"uuid", "description":"Fresh non-nil UUID for one creation. Reuse exactly this UUID and every request field after an uncertain response; never allocate another operation to recover it."},
            "expected_state_revision":{"type":"integer", "minimum":0, "description":"Exact management state_revision returned by scope. A new operation must reload scope after a conflict; exact retries keep the original request."},
            "project_id":{"type":"string", "minLength":1, "description":"An explicit existing Project in the Supervisor's current Host-configured allowed_projects. Creation never grants another Project."},
            "workspace_path":{"type":"string", "minLength":1, "maxLength":4096, "description":"Explicit existing workspace already bound to this Project by the Host; no inheritance from the Supervisor, path registration, or worktree creation."},
            "message":{"type":"string", "minLength":1, "maxLength":16384, "description":"One explicit first instruction, at most 16 KiB UTF-8. No private Supervisor history is copied."},
            "title":{"type":["string","null"], "minLength":1, "maxLength":256}
        },
        "required":["operation_id","expected_state_revision","project_id","workspace_path","message"]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creation_request_rejects_authority_fields_empty_values_and_utf8_overflow() {
        let request = json!({
            "operation_id":uuid::Uuid::new_v4(), "expected_state_revision":1,
            "project_id":"explicit-project", "workspace_path":"/explicit/worker", "message":"inspect"
        });
        let parsed: SupervisedRootCreateRequest = serde_json::from_value(request.clone()).unwrap();
        parsed.validate().unwrap();
        for (field, value) in [
            ("operation_id", json!(uuid::Uuid::nil())),
            ("workspace_path", json!(" ")),
            ("message", json!(" ")),
            ("message", json!("界".repeat(5462))),
            ("title", json!("x".repeat(257))),
        ] {
            let mut invalid = request.clone();
            invalid[field] = value;
            let parsed: SupervisedRootCreateRequest = serde_json::from_value(invalid).unwrap();
            assert!(parsed.validate().is_err(), "{field}");
        }
        for field in [
            "supervisor",
            "target_session_id",
            "permission_mode",
            "root_orchestration_only",
            "system_prompt",
            "model_ref",
        ] {
            let mut invalid = request.clone();
            invalid[field] = json!("untrusted");
            assert!(
                serde_json::from_value::<SupervisedRootCreateRequest>(invalid).is_err(),
                "{field}"
            );
        }
    }
}
