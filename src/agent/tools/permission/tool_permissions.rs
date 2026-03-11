use serde_json::Value;

use crate::agent::tools::permission::{PermissionContext, PermissionError, PermissionType};

const DELETE_COMMANDS: [&str; 7] = ["rm", "rmdir", "del", "erase", "unlink", "rd", "remove-item"];

pub fn check_permissions(
    tool_name: &str,
    args: &Value,
) -> Result<Option<Vec<PermissionContext>>, PermissionError> {
    match tool_name {
        "Write" | "Edit" => {
            let path = required_string_arg(args, "file_path")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::WriteFile,
                path,
                format!("{} file: {}", tool_name, path),
            )]))
        }
        "NotebookEdit" => {
            let path = required_string_arg(args, "notebook_path")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::WriteFile,
                path,
                format!("Notebook edit: {}", path),
            )]))
        }
        "Bash" => {
            let command = required_string_arg(args, "command")?;
            let mut contexts = Vec::new();
            if is_delete_command(command) {
                contexts.push(PermissionContext::new(
                    PermissionType::DeleteOperation,
                    command,
                    format!("Delete operation via shell: {}", command),
                ));
            }
            contexts.push(PermissionContext::new(
                PermissionType::ExecuteCommand,
                command,
                format!("Execute command: {}", command),
            ));
            Ok(Some(contexts))
        }
        "BashOutput" => {
            let bash_id = required_string_arg(args, "bash_id")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::TerminalSession,
                bash_id,
                format!("Read shell output: {}", bash_id),
            )]))
        }
        "KillShell" => {
            let shell_id = required_string_arg(args, "shell_id")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::TerminalSession,
                shell_id,
                format!("Kill shell: {}", shell_id),
            )]))
        }
        "WebFetch" => {
            let url = required_string_arg(args, "url")?;
            let resource = extract_domain(url);
            Ok(Some(vec![PermissionContext::new(
                PermissionType::HttpRequest,
                resource,
                format!("Web fetch: {}", url),
            )]))
        }
        "WebSearch" => {
            let query = required_string_arg(args, "query")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::HttpRequest,
                "duckduckgo.com",
                format!("Web search query: {}", query),
            )]))
        }
        _ => Ok(None),
    }
}

fn extract_domain(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(|host| host.to_string()))
        .unwrap_or_else(|| url.to_string())
}

pub fn is_delete_command(command: &str) -> bool {
    let command_lower = command.to_ascii_lowercase();
    DELETE_COMMANDS.iter().any(|delete| {
        command_lower
            .split_whitespace()
            .any(|token| token == *delete)
            || command_lower.contains(delete)
    })
}

fn required_string_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, PermissionError> {
    args.get(key)
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            PermissionError::CheckFailed(format!("Missing or invalid '{}' parameter", key))
        })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn check_permissions_write() {
        let args = json!({"file_path": "/tmp/test.txt"});
        let contexts = check_permissions("Write", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].permission_type, PermissionType::WriteFile);
    }

    #[test]
    fn check_permissions_bash_delete() {
        let args = json!({"command": "rm -rf /tmp/a"});
        let contexts = check_permissions("Bash", &args).unwrap().unwrap();
        assert_eq!(contexts.len(), 2);
        assert!(contexts
            .iter()
            .any(|ctx| ctx.permission_type == PermissionType::DeleteOperation));
    }

    #[test]
    fn check_permissions_web_fetch() {
        let args = json!({"url": "https://example.com/path"});
        let contexts = check_permissions("WebFetch", &args).unwrap().unwrap();
        assert_eq!(contexts[0].permission_type, PermissionType::HttpRequest);
        assert_eq!(contexts[0].resource, "example.com");
    }
}
