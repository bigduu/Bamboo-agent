use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolError, ToolResult};
use serde_json::json;

use crate::permission::PermissionType;

/// Tool for the LLM to proactively request additional permissions from the user.
///
/// Instead of failing when a permission is denied, the LLM can call this tool
/// to explain why it needs certain permissions and ask the user for approval.
///
/// The tool returns a payload that signals the agent loop to pause and ask
/// the user for permission approval (similar to how `conclusion_with_options` pauses for
/// user input).
///
/// Inspired by Codex's `request_permissions` tool which allows the model to
/// request filesystem and network permissions at runtime.
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
        "Request additional permissions from the user. Use this when you need to perform an operation that requires elevated permissions (e.g., writing to a specific directory, executing a dangerous command, making HTTP requests). The user will be prompted to approve or deny the request."
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
                    "minItems": 1
                }
            },
            "required": ["reason", "permissions"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
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

        // Build a human-readable question for the UI
        let mut question = format!("**Permission Request**\n\n{}\n\n", reason);
        question.push_str("**Requested permissions:**\n");
        for perm in &validated_permissions {
            let risk = perm["risk_level"].as_str().unwrap_or("Unknown");
            let desc = perm["description"].as_str().unwrap_or("");
            let resource = perm["resource"].as_str().unwrap_or("");
            let ptype = perm["type"].as_str().unwrap_or("");
            question.push_str(&format!(
                "- **[{}]** {} `{}` — {}\n",
                risk, ptype, resource, desc
            ));
        }

        let result_payload = json!({
            "status": "awaiting_permission_approval",
            "question": question,
            "reason": reason,
            "permissions": validated_permissions,
            "options": ["Approve", "Deny"],
            "allow_custom": false
        });

        Ok(ToolResult {
            success: true,
            result: result_payload.to_string(),
            display_preference: Some("request_permissions".to_string()),
        })
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

    #[tokio::test]
    async fn test_valid_single_permission_request() {
        let tool = RequestPermissionsTool::new();
        let result = tool
            .execute(json!({
                "reason": "Need to write deployment config",
                "permissions": [{
                    "type": "write_file",
                    "resource": "/etc/nginx/conf.d/*"
                }]
            }))
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(
            result.display_preference,
            Some("request_permissions".to_string())
        );

        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["status"], "awaiting_permission_approval");
        assert!(payload["question"]
            .as_str()
            .unwrap()
            .contains("deployment config"));
        assert_eq!(payload["permissions"].as_array().unwrap().len(), 1);
        assert_eq!(payload["options"], json!(["Approve", "Deny"]));
    }

    #[tokio::test]
    async fn test_valid_multiple_permissions() {
        let tool = RequestPermissionsTool::new();
        let result = tool
            .execute(json!({
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
            }))
            .await
            .unwrap();

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["permissions"].as_array().unwrap().len(), 2);
        assert_eq!(payload["permissions"][0]["risk_level"], "High Risk");
        assert_eq!(payload["permissions"][1]["risk_level"], "Medium Risk");
    }

    #[tokio::test]
    async fn test_missing_reason() {
        let tool = RequestPermissionsTool::new();
        let err = tool
            .execute(json!({
                "permissions": [{"type": "write_file", "resource": "/tmp/test"}]
            }))
            .await
            .unwrap_err();

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("reason")));
    }

    #[tokio::test]
    async fn test_empty_reason() {
        let tool = RequestPermissionsTool::new();
        let err = tool
            .execute(json!({
                "reason": "   ",
                "permissions": [{"type": "write_file", "resource": "/tmp/test"}]
            }))
            .await
            .unwrap_err();

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("empty")));
    }

    #[tokio::test]
    async fn test_missing_permissions() {
        let tool = RequestPermissionsTool::new();
        let err = tool
            .execute(json!({
                "reason": "Need access"
            }))
            .await
            .unwrap_err();

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("permissions")));
    }

    #[tokio::test]
    async fn test_empty_permissions_array() {
        let tool = RequestPermissionsTool::new();
        let err = tool
            .execute(json!({
                "reason": "Need access",
                "permissions": []
            }))
            .await
            .unwrap_err();

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("at least one")));
    }

    #[tokio::test]
    async fn test_invalid_permission_type() {
        let tool = RequestPermissionsTool::new();
        let err = tool
            .execute(json!({
                "reason": "Need access",
                "permissions": [{"type": "invalid_type", "resource": "/tmp"}]
            }))
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
            .execute(json!({
                "reason": "Need access",
                "permissions": [{"type": "write_file"}]
            }))
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
                .execute(json!({
                    "reason": format!("Test {}", ptype),
                    "permissions": [{"type": ptype, "resource": "/test"}]
                }))
                .await;
            assert!(
                result.is_ok(),
                "Permission type '{}' should be valid",
                ptype
            );
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
                .execute(json!({
                    "reason": format!("Test {}", ptype),
                    "permissions": [{"type": ptype, "resource": "/test"}]
                }))
                .await;
            assert!(
                result.is_ok(),
                "PascalCase permission type '{}' should be valid",
                ptype
            );
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
