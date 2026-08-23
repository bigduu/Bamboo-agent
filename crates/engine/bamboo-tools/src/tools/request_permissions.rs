use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde_json::json;

use crate::permission::PermissionType;
use crate::permission::MAX_PROACTIVE_PERMISSION_BATCH;

/// Proactively request one or more remembered permissions from the user.
///
/// The executor classifies every requested entry as a typed permission context
/// before this implementation runs. It can therefore pause/replay the same
/// tool-call through each batch item, and reaches `invoke` only after every
/// context was authorized at a semantically durable scope.
pub struct RequestPermissionsTool;

impl RequestPermissionsTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RequestPermissionsTool {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate a permission type string and return the matching PermissionType.
fn parse_permission_type(s: &str) -> Result<PermissionType, String> {
    match s {
        "write_file" | "WriteFile" => Ok(PermissionType::WriteFile),
        "execute_command" | "ExecuteCommand" => Ok(PermissionType::ExecuteCommand),
        "git_write" | "GitWrite" => Ok(PermissionType::GitWrite),
        "http_request" | "HttpRequest" => Ok(PermissionType::HttpRequest),
        "delete_operation" | "DeleteOperation" => Ok(PermissionType::DeleteOperation),
        "terminal_session" | "TerminalSession" => Ok(PermissionType::TerminalSession),
        other => Err(format!(
            "Unknown permission type '{}'. Valid types: write_file, execute_command, git_write, http_request, delete_operation, terminal_session",
            other
        )),
    }
}

#[async_trait]
impl Tool for RequestPermissionsTool {
    fn name(&self) -> &str {
        "request_permissions"
    }

    fn description(&self) -> &str {
        "Request one or more permissions through Bamboo's typed approval flow. Each item is reviewed independently; proactive requests support remembered scopes, while one-shot approval belongs to the actual target operation."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Clear explanation of why these permissions are needed"
                },
                "permissions": {
                    "type": "array",
                    "description": "List of permissions being requested",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "description": "Permission type: write_file, execute_command, git_write, http_request, delete_operation, terminal_session",
                                "enum": ["write_file", "execute_command", "git_write", "http_request", "delete_operation", "terminal_session"]
                            },
                            "resource": {
                                "type": "string",
                                "description": "The resource pattern (file path, URL pattern, command pattern, etc.)"
                            },
                            "description": {
                                "type": "string",
                                "description": "Optional human-readable description of this specific permission"
                            }
                        },
                        "required": ["type", "resource"]
                    },
                    "minItems": 1,
                    "maxItems": MAX_PROACTIVE_PERMISSION_BATCH
                }
            },
            "required": ["reason", "permissions"]
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        _ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let reason = args["reason"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'reason' parameter".to_string()))?
            .trim();

        if reason.is_empty() {
            return Err(ToolError::InvalidArguments(
                "'reason' cannot be empty".to_string(),
            ));
        }

        let permissions = args["permissions"].as_array().ok_or_else(|| {
            ToolError::InvalidArguments("Missing 'permissions' array parameter".to_string())
        })?;

        if permissions.is_empty() {
            return Err(ToolError::InvalidArguments(
                "'permissions' array must contain at least one item".to_string(),
            ));
        }
        if permissions.len() > MAX_PROACTIVE_PERMISSION_BATCH {
            return Err(ToolError::InvalidArguments(format!(
                "'permissions' array cannot contain more than {MAX_PROACTIVE_PERMISSION_BATCH} items"
            )));
        }

        // Validate each permission entry
        let mut validated_permissions = Vec::new();
        for (i, perm) in permissions.iter().enumerate() {
            let perm_type_str = perm["type"].as_str().ok_or_else(|| {
                ToolError::InvalidArguments(format!("permissions[{}]: missing 'type' field", i))
            })?;

            let perm_type = parse_permission_type(perm_type_str)
                .map_err(|e| ToolError::InvalidArguments(format!("permissions[{}]: {}", i, e)))?;

            let resource = perm["resource"].as_str().ok_or_else(|| {
                ToolError::InvalidArguments(format!("permissions[{}]: missing 'resource' field", i))
            })?;

            if resource.trim().is_empty() {
                return Err(ToolError::InvalidArguments(format!(
                    "permissions[{}]: 'resource' cannot be empty",
                    i
                )));
            }

            let description = perm["description"]
                .as_str()
                .unwrap_or_else(|| perm_type.description());
            validated_permissions.push(json!({
                "type": perm_type_str,
                "resource": resource.trim(),
                "description": description,
                "risk_level": perm_type.risk_level().label(),
            }));
        }

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: json!({
                "status": "permissions_authorized",
                "reason": reason,
                "permissions": validated_permissions,
                "message": "Every requested permission passed the typed policy gate. Target operations remain subject to the same policy."
            })
            .to_string(),
            // This is a terminal acknowledgement, not another human pause.
            display_preference: None,
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_name() {
        let tool = RequestPermissionsTool::new();
        assert_eq!(tool.name(), "request_permissions");
    }

    #[test]
    fn schema_caps_the_batch_at_the_replay_ledger_limit() {
        let schema = RequestPermissionsTool::new().parameters_schema();
        assert_eq!(
            schema["properties"]["permissions"]["maxItems"],
            MAX_PROACTIVE_PERMISSION_BATCH
        );
    }

    #[tokio::test]
    async fn invoke_rejects_a_batch_larger_than_the_replay_ledger() {
        let permissions = (0..=MAX_PROACTIVE_PERMISSION_BATCH)
            .map(|index| {
                json!({
                    "type": "write_file",
                    "resource": format!("/workspace/file-{index}")
                })
            })
            .collect::<Vec<_>>();
        let error = RequestPermissionsTool::new()
            .invoke(
                json!({"reason": "Prepare files", "permissions": permissions}),
                ToolCtx::none("t"),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("more than 64 items"));
    }

    #[tokio::test]
    async fn test_valid_single_permission_request() {
        let tool = RequestPermissionsTool::new();
        let outcome = tool
            .invoke(
                json!({
                    "reason": "Need to write deployment config",
                    "permissions": [{
                        "type": "write_file",
                        "resource": "/etc/nginx/conf.d/*"
                    }]
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = outcome else {
            panic!("terminal acknowledgement")
        };
        assert!(result.success);
        assert!(result.display_preference.is_none());
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["status"], "permissions_authorized");
        assert_eq!(payload["permissions"].as_array().unwrap().len(), 1);
        assert!(!result.result.contains("awaiting_permission_approval"));
    }

    #[tokio::test]
    async fn test_valid_multiple_permissions() {
        let tool = RequestPermissionsTool::new();
        let outcome = tool
            .invoke(
                json!({
                    "reason": "Need to deploy the application",
                    "permissions": [
                        {
                            "type": "execute_command",
                            "resource": "docker compose up -d",
                            "description": "Start Docker containers"
                        },
                        {
                            "type": "http_request",
                            "resource": "registry.example.com",
                            "description": "Pull container images"
                        }
                    ]
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = outcome else {
            panic!("terminal acknowledgement")
        };
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["status"], "permissions_authorized");
        assert_eq!(payload["permissions"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_missing_reason() {
        let tool = RequestPermissionsTool::new();
        let err = tool
            .invoke(
                json!({
                    "permissions": [{"type": "write_file", "resource": "/tmp/test"}]
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("reason")));
    }

    #[tokio::test]
    async fn test_empty_reason() {
        let tool = RequestPermissionsTool::new();
        let err = tool
            .invoke(
                json!({
                    "reason": "   ",
                    "permissions": [{"type": "write_file", "resource": "/tmp/test"}]
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("empty")));
    }

    #[tokio::test]
    async fn test_missing_permissions() {
        let tool = RequestPermissionsTool::new();
        let err = tool
            .invoke(
                json!({
                    "reason": "Need access"
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("permissions")));
    }

    #[tokio::test]
    async fn test_empty_permissions_array() {
        let tool = RequestPermissionsTool::new();
        let err = tool
            .invoke(
                json!({
                    "reason": "Need access",
                    "permissions": []
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("at least one")));
    }

    #[tokio::test]
    async fn test_invalid_permission_type() {
        let tool = RequestPermissionsTool::new();
        let err = tool
            .invoke(
                json!({
                    "reason": "Need access",
                    "permissions": [{"type": "invalid_type", "resource": "/tmp"}]
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, ToolError::InvalidArguments(msg) if msg.contains("Unknown permission type"))
        );
    }

    #[tokio::test]
    async fn test_missing_resource() {
        let tool = RequestPermissionsTool::new();
        let err = tool
            .invoke(
                json!({
                    "reason": "Need access",
                    "permissions": [{"type": "write_file"}]
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("resource")));
    }

    #[tokio::test]
    async fn test_all_permission_types() {
        let tool = RequestPermissionsTool::new();
        let types = [
            "write_file",
            "execute_command",
            "git_write",
            "http_request",
            "delete_operation",
            "terminal_session",
        ];

        for ptype in types {
            let result = tool
                .invoke(
                    json!({
                        "reason": format!("Test {}", ptype),
                        "permissions": [{"type": ptype, "resource": "/test"}]
                    }),
                    ToolCtx::none("t"),
                )
                .await;
            assert!(result.is_ok(), "{ptype}");
        }
    }

    #[tokio::test]
    async fn test_pascal_case_permission_types() {
        let tool = RequestPermissionsTool::new();
        let types = [
            "WriteFile",
            "ExecuteCommand",
            "GitWrite",
            "HttpRequest",
            "DeleteOperation",
            "TerminalSession",
        ];

        for ptype in types {
            let result = tool
                .invoke(
                    json!({
                        "reason": format!("Test {}", ptype),
                        "permissions": [{"type": ptype, "resource": "/test"}]
                    }),
                    ToolCtx::none("t"),
                )
                .await;
            assert!(result.is_ok(), "{ptype}");
        }
    }

    #[test]
    fn test_parse_permission_type() {
        assert_eq!(
            parse_permission_type("write_file").unwrap(),
            PermissionType::WriteFile
        );
        assert_eq!(
            parse_permission_type("WriteFile").unwrap(),
            PermissionType::WriteFile
        );
        assert!(parse_permission_type("unknown").is_err());
    }
}
